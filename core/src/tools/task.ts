import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import * as child_process from 'child_process';
import { ToolDefinition, ToolContext, ToolResult, EngineConfig } from '../types';
import { createDefaultTools } from './index';
import { spillToolText } from './spill-output';
import { SessionManager } from '../session';

/** Auto-background threshold for foreground agents exceeding this duration. */
const AUTO_BG_MS = 120_000; // eslint-disable-line @typescript-eslint/no-unused-vars

const SUBAGENT_STALL_TIMEOUT_MS = 10 * 60 * 1000; // 10 minutes

const agentNameRegistry = new Map<string, string>();

export function getAgentByName(name: string): string | undefined {
  return agentNameRegistry.get(name);
}

export function getAgentNameById(taskId: string): string | undefined {
  for (const [name, id] of agentNameRegistry) {
    if (id === taskId) return name;
  }
  return undefined;
}

export function resetAgentRegistry(): void {
  agentNameRegistry.clear();
}

const DEFAULT_MAX_AGENT_DEPTH = 5;
export const DEFAULT_MAX_AGENTS_PER_SESSION = 50;
export const DEFAULT_MAX_CONCURRENT_AGENTS = 10;

/**
 * Check whether spawning a new agent is allowed under session and concurrency caps.
 * Returns null if allowed, or an error string if blocked.
 */
export function checkAgentCaps(context: {
  _agentSpawnCount?: { value: number };
  _maxAgentsPerSession?: number;
  _maxConcurrentAgents?: number;
  processManager?: any;
}): string | null {
  const sessionCap = context._maxAgentsPerSession ?? DEFAULT_MAX_AGENTS_PER_SESSION;
  const spawnCount = context._agentSpawnCount?.value ?? 0;
  if (spawnCount >= sessionCap) {
    return `Agent session cap reached (${sessionCap}). ${spawnCount} agents already spawned this session. Increase _maxAgentsPerSession to raise the limit.`;
  }

  const concurrentCap = context._maxConcurrentAgents ?? DEFAULT_MAX_CONCURRENT_AGENTS;
  if (context.processManager) {
    let running = 0;
    const processes = (context.processManager as any).processes;
    if (processes && typeof processes.values === 'function') {
      for (const entry of processes.values()) {
        if (entry.command?.startsWith('[agent]') && entry.status === 'running') {
          running++;
        }
      }
    }
    if (running >= concurrentCap) {
      return `Concurrent agent cap reached (${concurrentCap}). ${running} agents currently running. Wait for agents to complete or increase _maxConcurrentAgents.`;
    }
  }

  return null;
}

function incrementSpawnCount(context: { _agentSpawnCount?: { value: number } }): void {
  if (!context._agentSpawnCount) {
    context._agentSpawnCount = { value: 0 };
  }
  context._agentSpawnCount.value++;
}

export function shouldAllowNestedTask(currentDepth: number, maxDepth: number): boolean {
  return currentDepth < maxDepth;
}

export function getSubagentDepthContext(currentDepth: number, maxDepth: number): { _agentDepth: number; _maxAgentDepth: number } {
  return { _agentDepth: currentDepth + 1, _maxAgentDepth: maxDepth };
}

/**
 * Filter the tool set for an agent based on mode and nesting depth.
 * Exported for testing; used internally by the task tool execute method.
 */
export function filterToolsForAgent(
  allTools: ToolDefinition[],
  mode: string,
  agentDepth?: number,
  maxAgentDepth?: number,
): ToolDefinition[] {
  const readOnlyNames = new Set([
    'file_read', 'grep', 'glob', 'list_dir', 'search_symbols',
    'web_search', 'web_fetch',
  ]);

  if (mode === 'explore') {
    return allTools.filter(t => readOnlyNames.has(t.name));
  }

  const depth = agentDepth ?? 0;
  const maxDepth = maxAgentDepth ?? DEFAULT_MAX_AGENT_DEPTH;
  const allowNested = shouldAllowNestedTask(depth, maxDepth);

  return allTools.filter(t => allowNested || t.name !== 'task');
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
        description: 'Agent mode. "explore" gives read-only tools only. "general" gives all tools including nested task (up to max depth). "fork" clones parent conversation context.',
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
      effort: {
        type: 'string',
        description: 'Reasoning effort for this subagent. Controls thinking depth.',
        enum: ['low', 'medium', 'high', 'max'],
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
    const mode = (input.mode as string) || 'general';
    const subagentType = input.subagent_type as string | undefined;
    const taskDescription = input.description as string | undefined;
    const resumeTarget = input.resume as string | undefined;
    const effortOverride = input.effort as string | undefined;

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

    // Enforce session and concurrency caps before any spawn work
    const capError = checkAgentCaps(context);
    if (capError) {
      return { output: `Error: ${capError}`, isError: true };
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

    const currentDepth = context._agentDepth ?? 0;
    const maxDepth = context._maxAgentDepth ?? DEFAULT_MAX_AGENT_DEPTH;

    const allTools = createDefaultTools(context.cwd);

    let tools: ToolDefinition[];
    let agentSystemPrompt: string | undefined;
    let agentModel: string | undefined;
    let agentMaxTurns: number | undefined;
    let agentPermissionMode: string | undefined;
    let agentTokenBudget: number | undefined;
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
      agentTokenBudget = agentDef.autoCompactWindow;

      if (agentDef.tools && agentDef.tools.length > 0) {
        const allowed = new Set(agentDef.tools);
        tools = allTools.getAll().filter(t => allowed.has(t.name));
        if (!shouldAllowNestedTask(currentDepth, maxDepth)) {
          tools = tools.filter(t => t.name !== 'task');
        }
      } else if (agentDef.disallowedTools && agentDef.disallowedTools.length > 0) {
        const disallowed = new Set(agentDef.disallowedTools);
        tools = allTools.getAll().filter(t => !disallowed.has(t.name));
        if (!shouldAllowNestedTask(currentDepth, maxDepth)) {
          tools = tools.filter(t => t.name !== 'task');
        }
      } else {
        tools = filterToolsForAgent(allTools.getAll(), 'general', currentDepth, maxDepth);
      }
      effectiveMode = 'general';
    } else if (mode === 'explore') {
      tools = filterToolsForAgent(allTools.getAll(), 'explore', currentDepth, maxDepth);
    } else if (mode === 'fork') {
      tools = filterToolsForAgent(allTools.getAll(), 'general', currentDepth, maxDepth);
      effectiveMode = 'general';
    } else {
      tools = filterToolsForAgent(allTools.getAll(), 'general', currentDepth, maxDepth);
    }

    const subAbort = new AbortController();
    const runInBackground = input.run_in_background === true;
    const forwardAbort = () => subAbort.abort();
    if (!runInBackground) {
      context.abortSignal.addEventListener('abort', forwardAbort, { once: true });
    }

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
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        return { output: `Error: Failed to create worktree for isolated subagent: ${msg}`, isError: true };
      }
    }

    const providerConfig = { ...(context._providerConfig || { baseUrl: '', apiKey: '', model: '' }) };
    const envSubagentModel = process.env.SUPERINFERENCE_SUBAGENT_MODEL;
    if (modelOverride) {
      providerConfig.model = modelOverride;
    } else if (agentModel) {
      providerConfig.model = agentModel;
    } else if (envSubagentModel) {
      providerConfig.model = MODEL_ALIASES[envSubagentModel] || envSubagentModel;
    }

    // Permission mode priority:
    // 1. Agent definition's permissionMode (e.g. verifier → auto-allow; safe, restricted tools)
    // 2. explore mode → always auto-allow (read-only tool set)
    // 3. Fallback → 'ask' (uses parent's permissionPromptHandler for TUI/YOLO propagation)
    const resolvedPermissionMode =
      agentPermissionMode ||
      (effectiveMode === 'explore' ? 'auto-allow' : 'ask');

    const childDepth = getSubagentDepthContext(currentDepth, maxDepth);
    const subConfig: EngineConfig = {
      provider: providerConfig,
      cwd: effectiveCwd,
      tools,
      sessionId: SessionManager.newId(),
      permissionMode: resolvedPermissionMode,
      permissionPromptHandler: context._permissionPromptHandler,
      abortController: subAbort,
      maxTurns: agentMaxTurns,
      detachedMode: !!context.detachedMode,
      agentDepth: childDepth._agentDepth,
      maxAgentDepth: childDepth._maxAgentDepth,
      thinking: effortOverride
        ? { enabled: true, level: effortOverride as any }
        : context._parentThinking || undefined,
      // When a named agent type is used, its system prompt is the authoritative
      // identity — skip persona resolution (which would inject e.g. the code
      // persona bug-fixing workflow into a verification-only agent).
      systemPrompt: agentSystemPrompt || undefined,
      tokenBudget: agentTokenBudget || undefined,
    };

    if (!context._engineFactory) {
      return { output: 'Error: engine factory not available in this context.', isError: true };
    }

    // agentSystemPrompt is now in subConfig.systemPrompt — the user message is
    // just the task prompt. For mode-only tasks (no subagent_type), there is no
    // agentSystemPrompt and the engine uses its own persona-driven system prompt.
    const effectivePrompt = prompt;


    const agentName = input.name as string | undefined;

    if (runInBackground) {
      incrementSpawnCount(context);
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
        const fm = (context.processManager as any).flowManager;
        if (fm) {
          const flowId = fm.getOrCreateActiveFlow(label);
          fm.addAgent(flowId, taskId);
        }
      }

      const subEngine = context._engineFactory(subConfig);
      (async () => {
        let result = '';
        let sawError = false;
        const fd = fs.openSync(outputPath, 'w');
        const w = (s: string) => { try { fs.writeSync(fd, s); } catch { /* fd closed */ } };
        const bgStallMs = parseInt(process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS ?? '', 10) || SUBAGENT_STALL_TIMEOUT_MS;
        let bgStallTimer: ReturnType<typeof setTimeout> | null = null;
        let bgStalled = false;
        const resetBgStall = () => {
          if (bgStallTimer) clearTimeout(bgStallTimer);
          bgStallTimer = setTimeout(() => { bgStalled = true; (subEngine as any).shutdown?.(); }, bgStallMs);
        };
        const clearBgStall = () => { if (bgStallTimer) { clearTimeout(bgStallTimer); bgStallTimer = null; } };
        resetBgStall();
        try {
          for await (const event of subEngine.submit(effectivePrompt)) {
            resetBgStall();
            if (bgStalled) break;
            if (event.type === 'thinking_delta') {
              w(event.text);
            }
            if (event.type === 'text_delta') {
              result += event.text;
              w(event.text);
            }
            if (event.type === 'tool_use_start') {
              const summary = typeof (event as any).input?.command === 'string'
                ? (event as any).input.command
                : typeof (event as any).input?.file_path === 'string'
                  ? (event as any).input.file_path
                  : JSON.stringify((event as any).input || {}).slice(0, 120);
              w(`\n▸ ${(event as any).toolName} ${summary}\n`);
            }
            if (event.type === 'tool_use_progress') {
              w((event as any).data || '');
            }
            if (event.type === 'tool_use_result') {
              const output = (event as any).output || '';
              const lines = output.split('\n');
              const preview = lines.slice(0, 20).join('\n');
              const suffix = lines.length > 20 ? `\n  ... (${lines.length - 20} more lines)` : '';
              w(`${preview}${suffix}\n`);
            }
            if (event.type === 'error') {
              sawError = true;
              const errText = `\nError: ${event.error}`;
              result += errText;
              w(errText);
            }
          }
          if (bgStalled) {
            sawError = true;
            const msg = `\n[Subagent stalled — no events for ${Math.round(bgStallMs / 1000)}s, terminated]`;
            result += msg;
            w(msg);
          }
        } catch (err) {
          sawError = true;
          const errText = `\nSubagent error: ${err instanceof Error ? err.message : String(err)}`;
          result += errText;
          w(errText);
        } finally {
          clearBgStall();
          (subEngine as any).shutdown?.();
        }
        try { fs.closeSync(fd); } catch { /* already closed */ }
        if (!result) {
          try { fs.writeFileSync(outputPath, '(subagent produced no output)'); } catch { /* dir removed */ }
        }
        if (context.processManager) {
          const label = taskDescription || `${prompt.slice(0, 80)}`;
          const entry = (context.processManager as any).processes.get(taskId);
          if (entry) {
            entry.status = sawError ? 'failed' : 'completed';
            entry.exitCode = sawError ? 1 : 0;
          }
          context.processManager.emit('complete', { taskId, exitCode: sawError ? 1 : 0, command: `[agent] ${label}`, description: `Background agent: ${label}` });
        }
      })().catch(() => {});

      return { output: `Background agent started: ${taskId}\nUse task_output to check results.` };
    }

    incrementSpawnCount(context);
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

    const stallTimeoutMs = parseInt(process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS ?? '', 10) || SUBAGENT_STALL_TIMEOUT_MS;
    let stallTimer: ReturnType<typeof setTimeout> | null = null;
    let stalled = false;
    const resetStallTimer = () => {
      if (stallTimer) clearTimeout(stallTimer);
      stallTimer = setTimeout(() => {
        stalled = true;
        subAbort.abort('subagent stalled — no events for ' + Math.round(stallTimeoutMs / 1000) + 's');
      }, stallTimeoutMs);
    };
    const clearStallTimer = () => { if (stallTimer) { clearTimeout(stallTimer); stallTimer = null; } };
    resetStallTimer();

    try {
      for await (const event of subEngine.submit(effectivePromptForSubmit)) {
        resetStallTimer();
        if (stalled) break;
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
      if (stalled) {
        resultText += `\n[Subagent timed out — no activity for ${Math.round(stallTimeoutMs / 1000)}s]`;
      }
    } catch (err) {
      context.abortSignal.removeEventListener('abort', forwardAbort);
      if (context._hookManager) {
        context._hookManager.executeSubagentStop({ parentSessionId: taskId, subagentSessionId: subConfig.sessionId || taskId, prompt, mode: effectiveMode }).catch(() => {});
      }
      const errMsg = err instanceof Error ? err.message : String(err);
      const output = resultText.trim()
        ? `[Partial output before error]\n\n${resultText.trim()}\n\n[Error]\n${errMsg}`
        : `Subagent error: ${errMsg}`;
      return {
        output,
        isError: true,
      };
    } finally {
      clearStallTimer();
      (subEngine as any).shutdown?.();
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

    // Cap the subagent transcript to the configured budget (spills overflow to
    // disk), so a chatty subagent cannot blow up the parent's context.
    const taskMaxChars = context.outputBudget?.taskMaxChars;
    const cappedResult = taskMaxChars && taskMaxChars > 0
      ? spillToolText(context.cwd, 'task', resultText, taskMaxChars)
      : resultText;
    return { output: `${cappedResult}\n\n[Agent stats: ${toolUseCount} tool calls, ${totalTokens} tokens]` };
  },
};
