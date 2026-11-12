import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import * as child_process from 'child_process';
import { ToolDefinition, ToolContext, ToolResult, EngineConfig } from '../types';
import { createDefaultTools } from './index';
import { coerceBool } from '../utils/coerce';
import { spillToolText } from './spill-output';
import { mapChildToSubagentEvent } from './subagent-relay';
import { SessionManager } from '../session';

/** Auto-background threshold for foreground agents exceeding this duration. */
const AUTO_BG_MS = 120_000; // eslint-disable-line @typescript-eslint/no-unused-vars

const SUBAGENT_STALL_TIMEOUT_MS = 10 * 60 * 1000; // 10 minutes

/**
 * Framing prepended to a mode-only sub-agent's task (no subagent_type identity) so
 * it returns its result and STOPS instead of running the full interactive coding
 * loop. Mirrors how a workflow agent is told "your final message IS the result".
 */
export const SUBAGENT_RETURN_DIRECTIVE =
  'You are a sub-agent handling one delegated task. Do exactly what the task asks and nothing more. ' +
  'When you have the answer, give it directly as your final message and STOP — do not create or edit ' +
  'files, run commands, or add verification or extra work unless the task explicitly requires it.';

/**
 * Dedicated LEAN system prompt for a mode-only sub-agent (a `task` delegation with no
 * named `subagent_type`). It REPLACES the full interactive coding stance so a bounded
 * delegation returns its result and STOPS instead of over-working (the measured failure:
 * a trivial "reply X" delegation wrote files, ran commands, and looped without
 * terminating because the full coding system prompt — "write files, verify, never stop"
 * — won over a stapled user-message directive). Safety is unaffected: bash/permission
 * guards run at the tool/executor layer, not in the system prompt. ui#13.
 *
 * `fork` keeps the full stance (it is an explicit context-inheriting clone), and a named
 * `subagent_type` keeps its own authoritative identity — neither uses this.
 */
export const SUBAGENT_SYSTEM_PROMPT =
  `You are a focused sub-agent handling exactly one delegated task inside a larger session.

- Do precisely what the task asks — nothing more. Do not add verification, cleanup, refactors, or "while I'm here" work the task did not request.
- Use tools only when the task requires them; prefer answering directly. Do NOT create or edit files, run commands, or run tests unless the task explicitly requires it.
- When you have the result, give it directly and concisely as your final message, then STOP. Your final message IS the deliverable returned to the caller.
- Do not loop re-checking, re-running, or re-announcing completion. One correct answer, then stop.
- Read any error in full and fix the root cause; never fabricate results. If the task is impossible or underspecified, say so plainly and stop.`;

/** Lean system prompt for a read-only `explore`-mode sub-agent. ui#13. */
export const EXPLORE_SUBAGENT_SYSTEM_PROMPT =
  `You are a read-only exploration sub-agent handling one search/investigation task.

- Investigate using read and search tools only. Do NOT modify files, run mutating commands, or make any changes.
- Gather exactly what the task asks for, cite concrete locations (file:line) where relevant, then report your findings directly and concisely as your final message and STOP. Your final message IS the deliverable.
- Do not over-explore beyond what the task asks. One focused report, then stop.`;

const agentNameRegistry = new Map<string, string>();
// taskId -> the sub-engine sessionId it ran under. Lets `resume` recreate an
// agent on its PRIOR session (the Engine reloads saved messages when given a
// known sessionId), so a follow-up prompt continues the same context.
const agentSessionRegistry = new Map<string, string>();

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
  agentSessionRegistry.clear();
}

const DEFAULT_MAX_AGENT_DEPTH = 5;
export const DEFAULT_MAX_AGENTS_PER_SESSION = 50;
export const DEFAULT_MAX_CONCURRENT_AGENTS = 10;

/**
 * Decide whether an isolated agent worktree is safe to remove after the
 * foreground agent returns.
 *
 * Remove ONLY when all three hold:
 *  - `status` (git status --porcelain, including untracked) is empty — no
 *    uncommitted changes, so no new files a background agent may have written.
 *  - `unpushed` (git rev-list HEAD --not --remotes) is empty — no commits that
 *    would be lost.
 *  - `runningAgentCount === 0` — no background subagent is still live inside the
 *    worktree. A background subagent inherits the worktree cwd and runs detached
 *    from the parent abort, so removing the worktree while it is working would
 *    pull the rug out from under it (lost/partial writes, git errors).
 */
export function shouldRemoveWorktree(
  status: string,
  unpushed: string,
  runningAgentCount: number,
): boolean {
  return !status && !unpushed && runningAgentCount === 0;
}

/**
 * Check whether spawning a new agent is allowed under session and concurrency caps.
 * Returns null if allowed, or an error string if blocked.
 */
export function checkAgentCaps(context: {
  _agentSpawnCount?: { value: number };
  _maxAgentsPerSession?: number;
  _maxConcurrentAgents?: number;
  processManager?: import('../process-manager').ProcessManager;
}): string | null {
  const sessionCap = context._maxAgentsPerSession ?? DEFAULT_MAX_AGENTS_PER_SESSION;
  const spawnCount = context._agentSpawnCount?.value ?? 0;
  if (spawnCount >= sessionCap) {
    return `Agent session cap reached (${sessionCap}). ${spawnCount} agents already spawned this session. Increase _maxAgentsPerSession to raise the limit.`;
  }

  const concurrentCap = context._maxConcurrentAgents ?? DEFAULT_MAX_CONCURRENT_AGENTS;
  if (context.processManager) {
    const running = context.processManager.runningAgentCount();
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

  // plan_mode is an INTERACTIVE-session-only feature: "exit" submits the plan for
  // user approval, and a subagent has no user to approve it — so a subagent that
  // enters plan mode hangs forever "awaiting approval". Its state is also module-
  // global, so a subagent entering plan mode would corrupt the parent engine's
  // plan state. Autonomous subagents must never get it.
  return allTools.filter(t =>
    t.name !== 'plan_mode' && (allowNested || t.name !== 'task'));
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
        description: 'Name of an agent definition to use (from .ami/agents/). Overrides mode, tools, and system prompt.',
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

    // Resume: resolve the prior sub-engine sessionId so the spawn below reuses
    // it (the Engine reloads saved messages for a known sessionId), turning this
    // prompt into a follow-up on the existing agent context.
    let resumeSessionId: string | undefined;
    if (resumeTarget) {
      const resolvedId = agentNameRegistry.get(resumeTarget) || resumeTarget;
      resumeSessionId = agentSessionRegistry.get(resolvedId);
      if (!resumeSessionId) {
        const known = [...agentSessionRegistry.keys()];
        return {
          output: `Error: no resumable agent found for "${resumeTarget}". ` +
            `Known agent ids: ${known.length ? known.join(', ') : '(none in this session)'}.`,
          isError: true,
        };
      }
    }

    // Enforce session and concurrency caps before any spawn work
    const capError = checkAgentCaps(context);
    if (capError) {
      return { output: `Error: ${capError}`, isError: true };
    }

    // Enforce the per-session lifetime budget (env-configurable via
    // CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION). Depth and concurrency remain
    // governed by checkAgentCaps/shouldAllowNestedTask above — the limiter only
    // owns the lifetime cap here.
    if (context.subagentLimiter?.isLifetimeBudgetExhausted()) {
      return {
        output: `Error: Session subagent limit reached (${context.subagentLimiter.maxLifetime}). ` +
          'Use /clear to reset or set CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION to increase.',
        isError: true,
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

    // Universal guard: plan_mode is interactive-only (its "exit" awaits user
    // approval that never comes in a subagent, and its state is module-global).
    // Strip it unconditionally so an explicit agent-def `tools` list can't
    // reintroduce it. See filterToolsForAgent for the rationale.
    tools = tools.filter(t => t.name !== 'plan_mode');

    const subAbort = new AbortController();
    // Providers often serialize booleans as the STRING "true"/"false" in tool-call
    // args; coerceBool handles both (a strict `=== true` silently dropped
    // `run_in_background: "true"` to foreground, serializing a batch the model
    // meant to run concurrently). Shared with bash.ts for consistent coercion.
    const runInBackground = coerceBool(input.run_in_background);
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

    // ui#13: a mode-only sub-agent (no named subagent_type) runs on a dedicated LEAN
    // system prompt that REPLACES the full coding stance, so it returns its result and
    // STOPS instead of over-working. `fork` is exempt (an explicit context-inheriting
    // clone that should behave like the parent); a named agent keeps agentSystemPrompt.
    // Use the RAW `mode` (not effectiveMode, which fork reassigns to 'general'): fork is
    // the documented context-inheriting clone and keeps the full stance + directive staple.
    const modeOnlySystemPrompt = (!agentSystemPrompt && mode !== 'fork')
      ? (mode === 'explore' ? EXPLORE_SUBAGENT_SYSTEM_PROMPT : SUBAGENT_SYSTEM_PROMPT)
      : undefined;

    const childDepth = getSubagentDepthContext(currentDepth, maxDepth);
    const subConfig: EngineConfig = {
      provider: providerConfig,
      cwd: effectiveCwd,
      tools,
      sessionId: resumeSessionId ?? SessionManager.newId(),
      permissionMode: resolvedPermissionMode,
      permissionRules: context._parentPermissionRules,
      permissionPromptHandler: context._permissionPromptHandler,
      abortController: subAbort,
      maxTurns: agentMaxTurns,
      detachedMode: !!context.detachedMode,
      agentDepth: childDepth._agentDepth,
      maxAgentDepth: childDepth._maxAgentDepth,
      thinking: effortOverride
        ? { enabled: true, level: effortOverride as import('../model-capabilities').ThinkingLevel }
        : context._parentThinking || undefined,
      // When a named agent type is used, its system prompt is the authoritative
      // identity — skip stance-skill resolution (which would inject e.g. the
      // swebench stance workflow into a verification-only agent).
      systemPrompt: agentSystemPrompt || modeOnlySystemPrompt || undefined,
      tokenBudget: agentTokenBudget || undefined,
    };

    if (!context._engineFactory) {
      return { output: 'Error: engine factory not available in this context.', isError: true };
    }

    // agentSystemPrompt is now in subConfig.systemPrompt — the user message is
    // just the task prompt. ui#13: general/explore mode-only sub-agents now carry the
    // return-and-STOP framing in a dedicated LEAN *system* prompt (modeOnlySystemPrompt)
    // that replaces the full coding stance — so the user-message directive is redundant
    // for them and is dropped (the system role is authoritative; stapling it to a
    // user-message that the full stance overrode was the original, ineffective design).
    // Only `fork` (no agentSystemPrompt, no lean prompt — it inherits the full stance as
    // a clone) still needs the user-message directive to bound it. A named subagent_type
    // keeps its own authoritative identity (no directive).
    const effectivePrompt = (agentSystemPrompt || modeOnlySystemPrompt)
      ? prompt
      : `${SUBAGENT_RETURN_DIRECTIVE}\n\n---\n\n${prompt}`;


    const agentName = input.name as string | undefined;

    if (runInBackground) {
      incrementSpawnCount(context);
      context.subagentLimiter?.recordSpawn();
      const taskId = `agent-${crypto.randomBytes(4).toString('hex')}`;
      if (agentName) {
        agentNameRegistry.set(agentName, taskId);
      }
      if (subConfig.sessionId) agentSessionRegistry.set(taskId, subConfig.sessionId);
      const tasksDir = path.join(context.cwd, '.ami', 'tasks');
      fs.mkdirSync(tasksDir, { recursive: true });
      const outputPath = path.join(tasksDir, `${taskId}.output`);
      fs.writeFileSync(outputPath, '');

      if (context.processManager) {
        const label = taskDescription || `${prompt.slice(0, 80)}`;
        context.processManager.registerAgent(taskId, {
          command: `[agent] ${label}`,
          description: `Background agent: ${label}`,
          outputPath,
          abortFn: () => { subAbort.abort(); },
        });
        const fm = context.processManager.flowManager;
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
          bgStallTimer = setTimeout(() => { bgStalled = true; subEngine.shutdown?.(); }, bgStallMs);
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
              const summary = typeof event.input?.command === 'string'
                ? event.input.command
                : typeof event.input?.file_path === 'string'
                  ? (event.input.file_path as string)
                  : JSON.stringify(event.input || {}).slice(0, 120);
              w(`\n▸ ${event.toolName} ${summary}\n`);
            }
            if (event.type === 'tool_use_progress') {
              w(event.data || '');
            }
            if (event.type === 'tool_use_result') {
              const lines = event.output.split('\n');
              const preview = lines.slice(0, 20).join('\n');
              const suffix = lines.length > 20 ? `\n  ... (${lines.length - 20} more lines)` : '';
              w(`${preview}${suffix}\n`);
            }
            if (event.type === 'usage_update') {
              if (context.processManager) {
                context.processManager.updateEntry(taskId, {
                  tokenCount: event.stats?.totalTokens,
                });
              }
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
          subEngine.shutdown?.();
        }
        try { fs.closeSync(fd); } catch { /* already closed */ }
        if (!result) {
          try { fs.writeFileSync(outputPath, '(subagent produced no output)'); } catch { /* dir removed */ }
        }
        if (context.processManager) {
          const label = taskDescription || `${prompt.slice(0, 80)}`;
          context.processManager.updateEntry(taskId, {
            status: sawError ? 'failed' : 'completed',
            exitCode: sawError ? 1 : 0,
          });
          context.processManager.emit('complete', { taskId, exitCode: sawError ? 1 : 0, command: `[agent] ${label}`, description: `Background agent: ${label}` });
        }
      })().catch(() => {});

      return { output: `Background agent started: ${taskId}\nTo collect its result, call task_output({ task_id: "${taskId}", wait: true }) — this blocks until it finishes and returns in one call. Do NOT poll in a loop. When spawning several agents, issue all the task_output(wait) calls in a single turn so they run concurrently.` };
    }

    incrementSpawnCount(context);
    context.subagentLimiter?.recordSpawn();
    const subEngine = context._engineFactory(subConfig);
    const taskId = `agent-${crypto.randomBytes(4).toString('hex')}`;
    if (agentName) {
      agentNameRegistry.set(agentName, taskId);
    }
    if (subConfig.sessionId) agentSessionRegistry.set(taskId, subConfig.sessionId);
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

    // Surface this foreground subagent live to the parent engine's UI.
    const relayDepth = childDepth._agentDepth;
    context._emitChild?.({
      type: 'subagent_start', nodeId: taskId, depth: relayDepth,
      agentName: agentName || undefined, model: subConfig.provider?.model, prompt,
    });

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
        const relayed = mapChildToSubagentEvent(event, taskId, relayDepth);
        if (relayed) context._emitChild?.(relayed);
        if (event.type === 'text_delta') {
          resultText += event.text;
        }
        if (event.type === 'error') {
          resultText += `\nError: ${event.error}`;
        }
        if (event.type === 'tool_use_start') {
          toolUseCount++;
          recentActivities.push(`tool:${event.toolName}`);
          if (recentActivities.length > 5) recentActivities.shift();
        }
        if (event.type === 'usage_update') {
          totalTokens = event.stats?.totalTokens ?? totalTokens;
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
      context._emitChild?.({
        type: 'subagent_result', nodeId: taskId, depth: relayDepth,
        success: false, summary: errMsg.split('\n')[0].slice(0, 120), tokens: totalTokens,
      });
      return {
        output,
        isError: true,
      };
    } finally {
      clearStallTimer();
      subEngine.shutdown?.();
    }
    context.abortSignal.removeEventListener('abort', forwardAbort);

    if (context._hookManager) {
      context._hookManager.executeSubagentStop({ parentSessionId: taskId, subagentSessionId: subConfig.sessionId || taskId, prompt, mode: effectiveMode }).catch(() => {});
    }

    if (isolation === 'worktree' && effectiveCwd !== context.cwd) {
      try {
        const status = child_process.execSync('git status --porcelain', { cwd: effectiveCwd, encoding: 'utf-8', timeout: 5000 }).trim();
        const unpushed = child_process.execSync('git rev-list HEAD --not --remotes', { cwd: effectiveCwd, encoding: 'utf-8', timeout: 5000 }).trim();
        const runningAgents = context.processManager ? context.processManager.runningAgentCount() : 0;
        if (shouldRemoveWorktree(status, unpushed, runningAgents)) {
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
    context._emitChild?.({
      type: 'subagent_result', nodeId: taskId, depth: relayDepth,
      success: true, summary: (resultText.trim().split('\n')[0] || 'done').slice(0, 120), tokens: totalTokens,
    });
    return { output: `${cappedResult}\n\n[Agent stats: ${toolUseCount} tool calls, ${totalTokens} tokens]` };
  },
};
