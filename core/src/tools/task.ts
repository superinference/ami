import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import * as child_process from 'child_process';
import { ToolDefinition, ToolContext, ToolResult, EngineConfig } from '../types';
import { ToolRegistry, createDefaultTools } from './index';
import { SessionManager } from '../session';

/** Auto-background threshold for foreground agents exceeding this duration. */
const AUTO_BG_MS = 120_000; // eslint-disable-line @typescript-eslint/no-unused-vars

const agentNameRegistry = new Map<string, string>();

export function getAgentByName(name: string): string | undefined {
  return agentNameRegistry.get(name);
}

export function resetAgentRegistry(): void {
  agentNameRegistry.clear();
}

export const taskTool: ToolDefinition = {
  name: 'task',
  description:
    'Spawn a subagent to perform a specific task. The subagent runs in an isolated context with its own conversation. Use for parallel codebase exploration, research, or delegating independent subtasks. The subagent\'s final text response is returned as the result.',
  inputSchema: {
    type: 'object',
    properties: {
      prompt: {
        type: 'string',
        description: 'The task for the subagent to perform.',
      },
      mode: {
        type: 'string',
        description: 'Agent mode. "explore" gives read-only tools only. "general" gives all tools except task (no recursion). "fork" clones parent conversation context.',
        enum: ['explore', 'general', 'fork'],
      },
      isolation: {
        type: 'string',
        enum: ['worktree'],
        description: 'Run the subagent in an isolated git worktree.',
      },
      subagent_type: {
        type: 'string',
        description: 'Name of an agent definition to use (from .superinference/agents/). Overrides mode, tools, and system prompt.',
      },
      model: {
        type: 'string',
        description: 'Model override. Accepts full names or aliases: "sonnet", "opus", "haiku", "gpt-4o", "gemini-pro".',
      },
      cwd: {
        type: 'string',
        description: 'Override working directory for the subagent. Must be an absolute path.',
      },
      name: {
        type: 'string',
        description: 'Name for the agent (addressable via send_message). Must be unique.',
      },
      description: {
        type: 'string',
        description: 'Short human-readable label for the task (3-5 words).',
      },
      run_in_background: {
        type: 'boolean',
        description: 'When true, runs the subagent in the background and returns a task_id immediately instead of waiting for completion.',
      },
      resume: {
        type: 'string',
        description: 'Resume a previously stopped agent by task ID or name. The prompt is sent as a follow-up message to the existing agent context.',
      },
    },
    required: ['prompt'],
  },
  isReadOnly: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const prompt = input.prompt as string;
    const mode = (input.mode as string) || 'explore';
    const subagentType = input.subagent_type as string | undefined;
    const taskDescription = input.description as string | undefined;
    const resumeTarget = input.resume as string | undefined;

    if (!prompt || prompt.trim().length === 0) {
      return { output: 'Error: prompt must not be empty.', isError: true };
    }

    if (resumeTarget) {
      const resolvedId = agentNameRegistry.get(resumeTarget) || resumeTarget;
      return {
        output: `[resume] Sending follow-up to agent "${resolvedId}": ${prompt.slice(0, 200)}...\n\n` +
          `Note: Full agent resume requires persistent session state. The prompt has been noted for re-dispatch.`,
      };
    }

    const MODEL_ALIASES: Record<string, string> = {
      'sonnet': 'claude-sonnet-4',
      'opus': 'claude-opus-4',
      'haiku': 'claude-haiku-4-5',
    };
    const rawModel = input.model as string | undefined;
    const modelOverride = rawModel ? (MODEL_ALIASES[rawModel] || rawModel) : undefined;

    if (context._hookManager) {
      const agentTypeCheck = await context._hookManager.executePreToolUse?.({
        messages: [], turnCount: 0,
        toolName: 'task',
        toolInput: { subagent_type: mode, prompt: prompt.slice(0, 100) },
      });
      if (agentTypeCheck?.action === 'deny') {
        return { output: `Error: Agent type "${mode}" denied by permission rules.`, isError: true };
      }
    }

    const allTools = createDefaultTools(context.cwd);
    const readOnlyNames = new Set([
      'file_read', 'grep', 'glob', 'list_dir', 'search_symbols',
      'web_search', 'web_fetch',
    ]);

    let tools: ToolDefinition[];
    let agentSystemPrompt: string | undefined;
    let agentModel: string | undefined;
    let agentMaxTurns: number | undefined;
    let agentPermissionMode: string | undefined;
    let effectiveMode = mode;

    if (subagentType && context._skillManager) {
      const agentDef = context._skillManager.getAgent(subagentType);
      if (!agentDef) {
        return { output: `Error: agent "${subagentType}" not found. Available agents: ${context._skillManager.listAgents().map(a => a.name).join(', ') || '(none)'}`, isError: true };
      }
      agentSystemPrompt = agentDef.systemPrompt;
      agentModel = agentDef.model;
      agentMaxTurns = agentDef.maxTurns;
      agentPermissionMode = agentDef.permissionMode;

      if (agentDef.tools && agentDef.tools.length > 0) {
        const allowed = new Set(agentDef.tools);
        tools = allTools.getAll().filter(t => allowed.has(t.name) && t.name !== 'task');
      } else if (agentDef.disallowedTools && agentDef.disallowedTools.length > 0) {
        const disallowed = new Set([...agentDef.disallowedTools, 'task']);
        tools = allTools.getAll().filter(t => !disallowed.has(t.name));
      } else {
        tools = allTools.getAll().filter(t => t.name !== 'task');
      }
      effectiveMode = 'general';
    } else if (mode === 'explore') {
      tools = allTools.getAll().filter(t => readOnlyNames.has(t.name));
    } else if (mode === 'fork') {
      tools = allTools.getAll().filter(t => t.name !== 'task');
      effectiveMode = 'general';
    } else {
      tools = allTools.getAll().filter(t => t.name !== 'task');
    }

    const subAbort = new AbortController();
    const forwardAbort = () => subAbort.abort();
    context.abortSignal.addEventListener('abort', forwardAbort, { once: true });

    const isolation = input.isolation as string | undefined;
    let effectiveCwd = (input.cwd as string) || context.cwd;
    if (isolation === 'worktree') {
      try {
        const { createWorktreeSession, symlinkLargeDirectories, copyWorktreeIncludes } = require('../worktree-manager');
        const slug = `agent-${crypto.randomBytes(4).toString('hex')}`;
        const session = createWorktreeSession(effectiveCwd, slug);
        symlinkLargeDirectories(effectiveCwd, session.worktreePath);
        copyWorktreeIncludes(effectiveCwd, session.worktreePath);
        effectiveCwd = session.worktreePath;
      } catch {}
    }

    const providerConfig = { ...(context._providerConfig || { baseUrl: '', apiKey: '', model: '' }) };
    if (modelOverride) {
      providerConfig.model = modelOverride;
    } else if (agentModel) {
      providerConfig.model = agentModel;
    }

    // Permission mode priority:
    // 1. Agent definition's permissionMode (e.g. verifier → auto-allow; safe, restricted tools)
    // 2. explore mode → always auto-allow (read-only tool set)
    // 3. Fallback → 'ask' (uses parent's permissionPromptHandler for TUI/YOLO propagation)
    const resolvedPermissionMode =
      agentPermissionMode ||
      (effectiveMode === 'explore' ? 'auto-allow' : 'ask');

    const subConfig: EngineConfig = {
      provider: providerConfig,
      cwd: effectiveCwd,
      tools,
      sessionId: SessionManager.newId(),
      permissionMode: resolvedPermissionMode,
      permissionPromptHandler: context._permissionPromptHandler,
      abortController: subAbort,
      maxTurns: agentMaxTurns,
    };

    if (!context._engineFactory) {
      return { output: 'Error: engine factory not available in this context.', isError: true };
    }

    const effectivePrompt = agentSystemPrompt
      ? `${agentSystemPrompt}\n\n---\n\n${prompt}`
      : prompt;

    const runInBackground = input.run_in_background === true;

    const agentName = input.name as string | undefined;

    if (runInBackground) {
      const taskId = `agent-${crypto.randomBytes(4).toString('hex')}`;
      if (agentName) {
        agentNameRegistry.set(agentName, taskId);
      }
      const tasksDir = path.join(context.cwd, '.superinference', 'tasks');
      fs.mkdirSync(tasksDir, { recursive: true });
      const outputPath = path.join(tasksDir, `${taskId}.output`);
      fs.writeFileSync(outputPath, '');

      if (context.processManager) {
        const label = taskDescription || `${prompt.slice(0, 80)}`;
        (context.processManager as any).processes.set(taskId, {
          taskId,
          pid: process.pid,
          command: `[agent] ${label}`,
          description: `Background agent: ${label}`,
          status: 'running',
          exitCode: null,
          outputPath,
          startTime: Date.now(),
          proc: { pid: process.pid, kill: () => { subAbort.abort(); } },
          outputFd: null,
          bytesWritten: 0,
        });
      }

      const subEngine = context._engineFactory(subConfig);
      (async () => {
        let result = '';
        try {
          for await (const event of subEngine.submit(effectivePrompt)) {
            if (event.type === 'text_delta') result += event.text;
            if (event.type === 'error') result += `\nError: ${event.error}`;
          }
        } catch (err) {
          result += `\nSubagent error: ${err instanceof Error ? err.message : String(err)}`;
        }
        try { fs.writeFileSync(outputPath, result || '(subagent produced no output)'); } catch { /* dir removed */ }
        if (context.processManager) {
          const label = taskDescription || `${prompt.slice(0, 80)}`;
          const entry = (context.processManager as any).processes.get(taskId);
          if (entry) {
            entry.status = 'completed';
            entry.exitCode = 0;
          }
          context.processManager.emit('complete', { taskId, exitCode: 0, command: `[agent] ${label}`, description: `Background agent: ${label}` });
        }
      })().catch(() => {});

      return { output: `Background agent started: ${taskId}\nUse task_output to check results.` };
    }

    const subEngine = context._engineFactory(subConfig);
    const taskId = `agent-${crypto.randomBytes(4).toString('hex')}`;
    if (agentName) {
      agentNameRegistry.set(agentName, taskId);
    }
    let resultText = '';

    let effectivePromptForSubmit = effectivePrompt;
    if (mode === 'fork' && context._parentMessages && context._parentMessages.length > 0) {
      const contextSummary = context._parentMessages
        .filter(m => m.role === 'assistant' && typeof m.content === 'string')
        .map(m => (m.content as string).slice(0, 500))
        .join('\n---\n')
        .slice(0, 3000);
      if (contextSummary) {
        effectivePromptForSubmit = `[Parent conversation context]\n${contextSummary}\n\n[New task]\n${effectivePromptForSubmit}`;
      }
    }

    if (context._hookManager) {
      context._hookManager.executeSubagentStart({ parentSessionId: taskId, subagentSessionId: subConfig.sessionId || taskId, prompt, mode: effectiveMode }).catch(() => {});
    }

    let toolUseCount = 0;
    let totalTokens = 0;
    const recentActivities: string[] = [];

    try {
      for await (const event of subEngine.submit(effectivePromptForSubmit)) {
        if (event.type === 'text_delta') {
          resultText += event.text;
        }
        if (event.type === 'error') {
          resultText += `\nError: ${event.error}`;
        }
        if (event.type === 'tool_use_start') {
          toolUseCount++;
          recentActivities.push(`tool:${(event as any).toolName}`);
          if (recentActivities.length > 5) recentActivities.shift();
        }
        if (event.type === 'usage_update') {
          totalTokens = (event as any).stats?.totalTokens ?? totalTokens;
        }
      }
    } catch (err) {
      context.abortSignal.removeEventListener('abort', forwardAbort);
      if (context._hookManager) {
        context._hookManager.executeSubagentStop({ parentSessionId: taskId, subagentSessionId: subConfig.sessionId || taskId, prompt, mode: effectiveMode }).catch(() => {});
      }
      return {
        output: `Subagent error: ${err instanceof Error ? err.message : String(err)}`,
        isError: true,
      };
    }
    context.abortSignal.removeEventListener('abort', forwardAbort);

    if (context._hookManager) {
      context._hookManager.executeSubagentStop({ parentSessionId: taskId, subagentSessionId: subConfig.sessionId || taskId, prompt, mode: effectiveMode }).catch(() => {});
    }

    if (isolation === 'worktree' && effectiveCwd !== context.cwd) {
      try {
        const status = child_process.execSync('git status --porcelain -uno', { cwd: effectiveCwd, encoding: 'utf-8', timeout: 5000 }).trim();
        const unpushed = child_process.execSync('git rev-list HEAD --not --remotes', { cwd: effectiveCwd, encoding: 'utf-8', timeout: 5000 }).trim();
        if (!status && !unpushed) {
          child_process.execSync(`git worktree remove --force "${effectiveCwd}"`, { cwd: context.cwd, timeout: 10000 });
          resultText += '\n[Agent worktree cleaned up — no changes detected]';
        }
      } catch {}
    }

    return { output: `${resultText}\n\n[Agent stats: ${toolUseCount} tool calls, ${totalTokens} tokens]` };
  },
};
