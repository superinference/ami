import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { projectStateDir } from '../reference-journal';
import { executeWorkflow, parseWorkflowMeta, WorkflowResolver, DiskWorkflowCache, resolveToolScope, normalizeAgentModel, formatAgentCompletion, type WorkflowRuntime } from './workflow-runtime';
import { parseEffort } from '../skills';
import {
  SUBAGENT_RETURN_DIRECTIVE,
  generateTaskId,
  setupIsolatedWorktree,
  removeWorktreeIfClean,
  buildWorktreeNotice,
} from './task';
import { createDefaultTools } from './index';
import { normalizePermissionMode } from '../permissions';
import { isWorkflowScriptsEnabled, isBackgroundTasksDisabled, DISABLE_BACKGROUND_TASKS_ENV } from './tool-gates';
import { workflowSizeGuidelineText } from './workflow-size';
import { parseOptionalBool } from '../utils/coerce';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Write a workflow sub-agent's captured output WITHOUT following a symlink at
 * the destination path. The tasks directory is writable by sandboxed agents, so
 * a plain `fs.writeFileSync` would follow a planted symlink and clobber whatever
 * it points to on the host. Opening with `O_NOFOLLOW | O_EXCL`-equivalent flags
 * (fail on symlink → ELOOP) closes that vector — parity with the reference task
 * output writer, which opens every output file with O_NOFOLLOW for exactly this
 * reason. On platforms lacking O_NOFOLLOW (Windows) the constant is 0 and the
 * open degrades to a normal truncating create; the sandbox attack vector is
 * Unix-only. The file is truncated so a resumed/retried agent overwrites a prior
 * run's output.
 */
export function writeTaskOutputNoFollow(filePath: string, data: string): void {
  const O_NOFOLLOW = fs.constants.O_NOFOLLOW ?? 0;
  const flags = fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_TRUNC | O_NOFOLLOW;
  const fd = fs.openSync(filePath, flags, 0o600);
  try {
    fs.writeFileSync(fd, data);
  } finally {
    fs.closeSync(fd);
  }
}

/**
 * Upper bound on a workflow script's size. A workflow body is compiled with the
 * Function constructor and run in-process, so an unbounded script is a memory /
 * compile-time DoS vector. 512 KiB is far larger than any legitimate
 * orchestration script yet rejects pathological input before it is compiled.
 * (Parity hardening — the reference's Workflow tool guards script size.)
 */
export const MAX_WORKFLOW_SCRIPT_BYTES = 512 * 1024;

/**
 * Minimum gap between on-disk flushes of a running agent's partial output. The
 * first chunk is flushed immediately; subsequent flushes are throttled to this
 * interval so a chatty agent does not thrash the filesystem while still letting
 * `task_output` tail live progress (parity: the reference streams incremental
 * agent output rather than revealing it only on completion).
 */
export const PROGRESS_FLUSH_INTERVAL_MS = 500;

/** Reduce an opaque caller-supplied id to a single safe path segment so a
 *  runId / resumeKey can never escape the `.ami/workflows/` tree. Mirrors the
 *  sanitization already applied to the resume cache namespace. */
export function sanitizeWorkflowRunId(id: string): string {
  return id.replace(/[^A-Za-z0-9_-]/g, '_');
}

/** Persisted record for an opt-in, resumable workflow run (keyed by runId). */
export interface WorkflowRunRecord {
  /** The fully-resolved script source — persisted so a resume reproduces the
   *  exact body even if the original scriptPath is later moved or deleted. */
  script: string;
  /** The scriptPath the run was launched from, if any (informational). */
  scriptPath?: string;
  /** The saved-workflow name the run was launched from, if any (informational). */
  name?: string;
  meta: { name: string; description: string };
  createdAt: string;
}

function workflowRunsDir(cwd: string): string {
  return path.join(cwd, '.ami', 'workflows', '.runs');
}

/** Best-effort persistence of a run record so `resumeFromRunId` can reload it.
 *  Failures (read-only cwd, `.ami` is a file, …) are swallowed — persistence is
 *  an optimization, never a hard requirement for the run to complete. */
export function persistWorkflowRun(cwd: string, runId: string, record: WorkflowRunRecord): void {
  try {
    const dir = workflowRunsDir(cwd);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, `${runId}.json`), JSON.stringify(record));
  } catch { /* best-effort: a run must not fail because its record could not be saved */ }
}

/** Load a persisted run record by id, or null if absent / unreadable / malformed. */
export function loadWorkflowRun(cwd: string, runId: string): WorkflowRunRecord | null {
  try {
    const raw = fs.readFileSync(path.join(workflowRunsDir(cwd), `${runId}.json`), 'utf-8');
    const rec = JSON.parse(raw) as WorkflowRunRecord;
    return typeof rec?.script === 'string' ? rec : null;
  } catch {
    return null;
  }
}

/** A persisted workflow run record with its id attached (for enumeration). */
export interface WorkflowRunListing extends WorkflowRunRecord {
  runId: string;
}

/** Enumerate all persisted (resumable, runId-tagged) workflow runs, most-recent
 *  first. Powers `/flows` "recent/saved runs". Returns [] when the runs dir is
 *  absent / unreadable, and skips any malformed record (best-effort). */
export function listWorkflowRuns(cwd: string): WorkflowRunListing[] {
  let files: string[];
  try {
    files = fs.readdirSync(workflowRunsDir(cwd)).filter(f => f.endsWith('.json'));
  } catch {
    return [];
  }
  const runs: WorkflowRunListing[] = [];
  for (const f of files) {
    const runId = f.replace(/\.json$/, '');
    const rec = loadWorkflowRun(cwd, runId);
    if (rec) runs.push({ ...rec, runId });
  }
  // Newest first by createdAt (ISO strings sort lexicographically); ties keep order.
  runs.sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
  return runs;
}

/** Save a workflow SCRIPT to `.ami/workflows/<name>.js` so it can be relaunched by
 *  `name` later (the reference's "/flows to monitor and SAVE"). The name is reduced
 *  to a single safe path segment so it can never escape the workflows dir. Returns
 *  the absolute path written. Throws on an empty/invalid name. */
export function saveWorkflowScript(cwd: string, name: string, script: string): { path: string; name: string } {
  const safe = sanitizeWorkflowRunId(name).replace(/^_+|_+$/g, '');
  if (!safe) throw new Error('A non-empty workflow name is required.');
  const dir = path.join(cwd, '.ami', 'workflows');
  fs.mkdirSync(dir, { recursive: true });
  const dest = path.join(dir, `${safe}.js`);
  // O_NOFOLLOW-safe write (same hardening as task output) so a planted symlink at
  // the destination cannot redirect the write outside the workflows dir.
  writeTaskOutputNoFollow(dest, script);
  return { path: dest, name: safe };
}

export const workflowTool: ToolDefinition = {
  name: 'workflow',
  // PARITY (additive): declarative per-tool gate for the workflow-scripts subsystem.
  isEnabled: () => isWorkflowScriptsEnabled(),
  // Dynamic description (getter): the base contract + the current "Dynamic workflow
  // size" guideline, read at advertisement time so a /config size change is reflected
  // without a restart (parity: the reference surfaces the size guideline in the tool).
  get description(): string {
    return 'Execute a workflow script that orchestrates multiple subagents. ' +
      'Workflows run agent(), parallel(), pipeline(), phase(), and log() to coordinate multi-step work. ' +
      'Scripts must begin with `export const meta = { name, description }` followed by the workflow body. ' +
      'The run launches in the BACKGROUND by default and returns a task id immediately — monitor, stop, and save it with /flows. ' +
      workflowSizeGuidelineText();
  },
  inputSchema: {
    type: 'object',
    properties: {
      script: {
        type: 'string',
        description: 'Self-contained workflow script (JavaScript). Must start with export const meta = {...}.',
      },
      scriptPath: {
        type: 'string',
        description: 'Path to a workflow script file on disk. Takes precedence over script.',
      },
      name: {
        type: 'string',
        description: 'Name of a saved workflow to load from .ami/workflows/.',
      },
      args: {
        type: 'string',
        description: 'Optional input value exposed to the script as the global `args`. A JSON value (string, number, object, array, …) is passed through to the script unchanged.',
      },
      maxConcurrent: {
        type: 'number',
        description: 'Max agents running concurrently for this run (1-256). Lower for strict rate limits, higher for fast self-hosted providers. Defaults to a CPU-derived cap.',
      },
      resumeKey: {
        type: 'string',
        description: 'Opt-in resume: re-invoking with the same resumeKey reuses the cached results of agents that already completed (same prompt + opts), so a resumed/retried run skips finished work. Cached under .ami/workflows/.cache/.',
      },
      runId: {
        type: 'string',
        description: 'Opt-in: tag this run with a stable id so its script is persisted (under .ami/workflows/.runs/) and its agent results cached — enabling a later `resumeFromRunId` to replay it without re-supplying the script or repeating finished agents.',
      },
      resumeFromRunId: {
        type: 'string',
        description: 'Resume a previous run saved under this runId: reloads the persisted script (no script/scriptPath/name needed) and reuses that run\'s agent cache so completed work is skipped. Takes precedence over script/scriptPath/name.',
      },
      run_in_background: {
        type: 'boolean',
        description: 'Run the workflow DETACHED in the background (default: true). Returns immediately with a task id; monitor, stop, and save it with /flows, and collect its result with task_output. Set false to run inline and block until it finishes (the full result is returned in the tool output).',
      },
    },
  },
  isReadOnly: true,
  isConcurrencySafe: false,

  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    // Reject wrong-typed string inputs up front so a non-string can never reach
    // fs.*/path.join and fail obscurely (parity: argument-shape validation).
    for (const key of ['script', 'scriptPath', 'name', 'resumeKey', 'runId', 'resumeFromRunId'] as const) {
      const val = input[key];
      if (val !== undefined && typeof val !== 'string') {
        return { output: `Error: '${key}' must be a string.`, isError: true };
      }
    }

    let script: string;
    let scriptPathUsed: string | undefined;
    let nameUsed: string | undefined;

    const resumeFromRunId = typeof input.resumeFromRunId === 'string' && input.resumeFromRunId.trim()
      ? input.resumeFromRunId.trim()
      : undefined;

    if (resumeFromRunId) {
      // Resume takes precedence over script/scriptPath/name: reload the exact
      // persisted body so a retry replays the same workflow even if the original
      // source is gone.
      const record = loadWorkflowRun(context.cwd, sanitizeWorkflowRunId(resumeFromRunId));
      if (!record) {
        return { output: `Error: No saved workflow run found for runId '${resumeFromRunId}'.`, isError: true };
      }
      script = record.script;
      scriptPathUsed = record.scriptPath;
      nameUsed = record.name;
    } else if (input.scriptPath) {
      const scriptPath = input.scriptPath as string;
      if (!fs.existsSync(scriptPath)) {
        return { output: `Error: Script file not found: ${scriptPath}`, isError: true };
      }
      // existsSync → readFileSync is a TOCTOU window: the path may be unreadable
      // (EACCES), a directory (EISDIR), or deleted between the two calls. Convert
      // any such failure into a clean, caller-inspectable ToolResult rather than
      // letting the exception escape execute() as an unhandled rejection.
      try {
        script = fs.readFileSync(scriptPath, 'utf-8');
      } catch (err) {
        return { output: `Error: cannot read workflow script: ${err instanceof Error ? err.message : String(err)}`, isError: true };
      }
      scriptPathUsed = scriptPath;
    } else if (input.name) {
      const name = input.name as string;
      const searchPaths = [
        path.join(context.cwd, '.ami', 'workflows', `${name}.js`),
        path.join(context.cwd, '.ami', 'workflows', `${name}.mjs`),
      ];
      const found = searchPaths.find(p => fs.existsSync(p));
      if (!found) {
        return { output: `Error: Workflow '${name}' not found in .ami/workflows/`, isError: true };
      }
      // Same TOCTOU hardening as the scriptPath branch above.
      try {
        script = fs.readFileSync(found, 'utf-8');
      } catch (err) {
        return { output: `Error: cannot read workflow script: ${err instanceof Error ? err.message : String(err)}`, isError: true };
      }
      nameUsed = name;
    } else if (input.script) {
      script = input.script as string;
    } else {
      return { output: 'Error: One of script, scriptPath, or name is required (or resumeFromRunId to resume a saved run).', isError: true };
    }

    if (Buffer.byteLength(script, 'utf-8') > MAX_WORKFLOW_SCRIPT_BYTES) {
      return { output: `Error: Workflow script exceeds the maximum size of ${MAX_WORKFLOW_SCRIPT_BYTES} bytes.`, isError: true };
    }

    const meta = parseWorkflowMeta(script);
    if (!meta) {
      return { output: 'Error: Script must begin with `export const meta = { name, description }`', isError: true };
    }
    // the reference parity: meta must declare BOTH a name and a description.
    // parseWorkflowMeta coerces missing fields to '' rather than failing, so the
    // tool enforces the required-field contract here.
    if (!meta.name.trim() || !meta.description.trim()) {
      return { output: 'Error: Workflow meta must define a non-empty `name` and `description`.', isError: true };
    }

    // Opt-in run persistence (parity: resumable runs). A fresh run tagged with an
    // explicit runId saves its resolved script so `resumeFromRunId` can replay it.
    const runId = !resumeFromRunId && typeof input.runId === 'string' && input.runId.trim()
      ? sanitizeWorkflowRunId(input.runId.trim())
      : undefined;
    if (runId) {
      persistWorkflowRun(context.cwd, runId, {
        script,
        scriptPath: scriptPathUsed,
        name: nameUsed,
        meta: { name: meta.name, description: meta.description },
        createdAt: new Date().toISOString(),
      });
    }
    // Cache namespace drives resume: results are keyed under the run/resume id so
    // a resumed run reuses the finished agents of the original.
    const effectiveRunId = resumeFromRunId ? sanitizeWorkflowRunId(resumeFromRunId) : runId;

    // Recovery/continuation affordance (parity: the reference agent result surfaces
    // how to resume/continue). When this run opted into resumability, tell the
    // model exactly how to re-invoke and skip already-completed agents — surfaced
    // on BOTH the success and failure paths so a workflow that dies partway still
    // advertises how to pick up where it left off.
    const resumeKeyGiven = typeof input.resumeKey === 'string' && input.resumeKey.trim()
      ? input.resumeKey.trim()
      : undefined;
    const resumeHint = effectiveRunId
      ? `Re-run with resumeFromRunId '${effectiveRunId}' to replay this run and skip already-completed agents.`
      : resumeKeyGiven
        ? `Re-run with resumeKey '${resumeKeyGiven}' to skip already-completed agents.`
        : undefined;

    // Deploy mode: a workflow runs DETACHED in the background by DEFAULT (parity
    // with the reference's background Workflow launch — "Running in background ·
    // /flows to monitor and save"), returning a task id immediately. An explicit
    // run_in_background:false runs it inline and blocks for the full result (the
    // path the resume/parity/runtime tests exercise). Background can be globally
    // disabled via the shared background-tasks gate (no separate workflow knob).
    const ribParsed = parseOptionalBool(input.run_in_background, 'run_in_background', true);
    if ('error' in ribParsed) {
      return { output: `Error: ${ribParsed.error}`, isError: true };
    }
    // An EXPLICIT background request in an environment that forbids background work
    // is an error; a DEFAULTED request silently falls back to foreground.
    if (ribParsed.value && input.run_in_background !== undefined && isBackgroundTasksDisabled()) {
      return {
        output: `Error: background workflows are disabled (${DISABLE_BACKGROUND_TASKS_ENV} is set). ` +
          'Run the workflow inline by setting run_in_background:false, or unset the env knob.',
        isError: true,
      };
    }
    // Backgrounding is only meaningful when there is a process manager to TRACK,
    // monitor (/flows), and stop the detached run and to surface its completion
    // notification. Without one (bare/programmatic contexts), run inline — so the
    // tool still returns the full result and nothing is silently lost.
    const runInBackground = (isBackgroundTasksDisabled() ? false : ribParsed.value) && !!context.processManager;

    // One flow per workflow invocation so the inspector shows "1 flow, many
    // agents" — each agent()/parallel()/pipeline() call joins it. Guarded on
    // processManager so the workflow runs unchanged when there is none.
    const pm = context.processManager;
    const fm = pm?.flowManager;
    const workflowFlowId = fm ? fm.createFlow(meta.name) : undefined;

    // Run-level abort: a BACKGROUND run gets its OWN AbortController so it survives
    // this tool call returning (linked from the parent turn's signal below, so an
    // outer cancel / `/flows stop` still tears it down). A FOREGROUND run uses the
    // tool call's own abort signal directly. The agent handler and executeWorkflow
    // both key off `runAbortSignal`, so per-agent + whole-run cancellation work the
    // same in either mode.
    const workflowAbort = runInBackground ? new AbortController() : null;
    const runAbortSignal: AbortSignal = workflowAbort ? workflowAbort.signal : context.abortSignal;

    // The live runtime for THIS workflow run, captured via executeWorkflow's
    // onRuntime hook, so a per-agent task_kill retry/skip can recover one wedged
    // agent (parity: onRetryAgent / onSkipAgent) without aborting the whole run.
    let wfRuntime: WorkflowRuntime | undefined;

    // Aggregate usage across every agent this workflow spawns, plus the run's
    // wall-clock duration — reported on the completion output (parity: the
    // reference agent result surfaces tool uses, tokens, and duration).
    let totalAgentTokens = 0;
    let totalToolUses = 0;
    const workflowStartedAt = Date.now();

    const agentHandler = async (prompt: string, opts?: any, agentId?: number, report?: (s: { toolUseCount?: number; tokenCount?: number }) => void): Promise<string> => {
      if (!context._engineFactory) return `[Agent not available: no engine factory]`;

      // Per-agent specialized agent-type (parity: AgentTool `subagent_type`).
      // Resolve the requested agentType to its definition and layer the agent's
      // identity — system prompt, model, permission posture, and tool allowlist —
      // onto the spawned sub-engine, so `agent(p, { agentType: 'reviewer' })` runs
      // as that specialist instead of a generic engine. An explicit per-call opt
      // (model / permissionMode / tools) still wins over the agent's default.
      // Mirrors the resolution in task.ts so there is one agent-definition path.
      let agentSystemPrompt: string | undefined;
      let agentDefModel: string | undefined;
      let agentDefPermissionMode: string | undefined;
      let agentDefTools: ToolDefinition[] | undefined;
      let agentDefMaxTurns: number | undefined;
      let agentDefEffort: string | number | undefined;
      const agentType = typeof opts?.agentType === 'string' ? opts.agentType.trim() : '';
      if (agentType && context._skillManager) {
        const def = context._skillManager.getAgent(agentType);
        if (!def) {
          const available = context._skillManager.listAgents().map(a => a.name).join(', ') || '(none)';
          return `[Agent error: agentType "${agentType}" not found. Available agents: ${available}]`;
        }
        agentSystemPrompt = def.systemPrompt;
        // normalizeAgentModel maps the 'inherit' sentinel / blank value to undefined
        // (use the parent's model) — one shared normalization path.
        agentDefModel = normalizeAgentModel(def.model);
        agentDefPermissionMode = def.permissionMode;
        agentDefMaxTurns = def.maxTurns;
        agentDefEffort = def.effort;
        // Resolve the agentType's declared tool allow/deny scope via the shared
        // resolveToolScope (one code path for agentType + per-call scoping).
        if ((def.tools && def.tools.length > 0) || (def.disallowedTools && def.disallowedTools.length > 0)) {
          const everything = createDefaultTools(context.cwd).getAll();
          const { tools, invalid } = resolveToolScope(everything, def.tools, def.disallowedTools);
          if (invalid.length > 0) {
            return `[Agent error: agentType "${agentType}" lists unknown tool(s): ${invalid.join(', ')}]`;
          }
          agentDefTools = tools;
        }
      }

      // Per-agent cancellation (parity: AgentTool per-agent abort). Each agent
      // gets its OWN AbortController wired to the process-manager entry's abortFn,
      // so cancelling a single workflow agent (task_kill / UI) actually tears down
      // its sub-engine instead of leaving it running. The workflow-level
      // abortSignal is forwarded into it so an outer cancel still stops every
      // in-flight agent; the listener is removed once the agent settles.
      const ac = new AbortController();
      const forwardAbort = (): void => ac.abort();
      if (runAbortSignal.aborted) ac.abort();
      else runAbortSignal.addEventListener('abort', forwardAbort, { once: true });

      const label = String(opts?.label || prompt).slice(0, 80);
      let taskId: string | undefined;
      let outputPath: string | undefined;
      let result = '';
      if (pm) {
        // Self-describing, high-entropy agent id (parity: createAgentId's optional
        // label + 8-byte entropy). generateTaskId renders randomBytes(8) = 2^64
        // combinations (closing the brute-force symlink race the old randomBytes(4)
        // left open); the sanitized label is embedded as a prefix so a running
        // agent reads as `agent-<label>-<hex>` in task lists and on disk.
        const slug = sanitizeWorkflowRunId(label).slice(0, 24).replace(/^_+|_+$/g, '');
        taskId = generateTaskId(slug ? `agent-${slug}` : 'agent');
        outputPath = path.join(projectStateDir(context.cwd, 'tasks'), `${taskId}.out`);
        try { fs.mkdirSync(path.dirname(outputPath), { recursive: true }); } catch { /* ok */ }
        pm.registerAgent(taskId, {
          command: `[agent] ${label}`,
          description: `Workflow agent: ${label}`,
          outputPath,
          abortFn: forwardAbort,
          // Store the agent's abort signal for composite liveness (isActive).
          abortSignal: ac.signal,
          // Per-agent recovery (parity: onRetryAgent / onSkipAgent). task_kill
          // retry/skip of THIS agent drives the live runtime to re-run or settle
          // just this agent, then tears down its (stuck) sub-engine — the rest of
          // the workflow keeps running. Only wired when the runtime surfaced a
          // stable agent id for this call.
          ...(agentId !== undefined ? {
            retryFn: () => { wfRuntime?.retryAgent(agentId); ac.abort(); },
            skipFn: () => { wfRuntime?.skipAgent(agentId); ac.abort(); },
          } : {}),
        });
        if (fm && workflowFlowId) fm.addAgent(workflowFlowId, taskId);
      }
      // Per-agent worktree isolation (parity: AgentTool `isolation: 'worktree'`).
      // When requested, run this agent inside a fresh git worktree so parallel
      // workflow agents mutating the tree never race each other. Setup + teardown
      // go through the shared task.ts helpers (one worktree lifecycle path, no
      // duplicate). A clean (change-free) worktree is removed when the agent ends.
      let agentCwd = context.cwd;
      let isolatedWorktree: string | undefined;
      if (opts?.isolation === 'worktree') {
        try {
          agentCwd = setupIsolatedWorktree(context.cwd, taskId ?? generateTaskId('agent'));
          isolatedWorktree = agentCwd;
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          return `[Agent error: failed to create isolated worktree: ${msg}]`;
        }
      }
      const cleanupWorktree = async (): Promise<void> => {
        if (!isolatedWorktree || isolatedWorktree === context.cwd) return;
        const running = context.processManager?.runningAgentCount() ?? 0;
        await removeWorktreeIfClean(context.cwd, isolatedWorktree, running);
        isolatedWorktree = undefined;
      };
      // Per-agent model override (parity: AgentTool `model`). An explicit per-call
      // `model` wins; otherwise the resolved agentType's own model applies.
      // normalizeAgentModel maps a blank value or the `'inherit'` sentinel to
      // undefined (fall back to the parent/default model). The effective model is
      // layered onto a COPY of the parent provider config so the shared config
      // object is never mutated.
      const effectiveModel = normalizeAgentModel(opts?.model as string | undefined) ?? agentDefModel;
      const provider = effectiveModel
        ? { ...context._providerConfig!, model: effectiveModel }
        : context._providerConfig!;
      // Per-agent permission posture (parity: AgentTool `mode`). Precedence:
      // explicit per-call opt → the agentType's declared mode → 'auto-allow'. An
      // unrecognized value falls back rather than silently loosening or erroring
      // the whole workflow.
      const permissionMode = (opts?.permissionMode && normalizePermissionMode(String(opts.permissionMode)))
        || (agentDefPermissionMode && normalizePermissionMode(agentDefPermissionMode))
        || 'auto-allow';
      // Per-call tool allow/deny scope (parity: AgentTool `tools`/`disallowedTools`).
      // A per-call allow/deny list (tool NAMES) scopes THIS single agent and
      // overrides the agentType's resolved tool set. Resolved via the shared
      // resolveToolScope; an unknown allow name errors out rather than silently
      // dropping a typo'd tool.
      const perCallTools = Array.isArray(opts?.tools) ? (opts.tools as string[]) : undefined;
      const perCallDisallow = Array.isArray(opts?.disallowedTools) ? (opts.disallowedTools as string[]) : undefined;
      let effectiveTools = agentDefTools;
      if (perCallTools || perCallDisallow) {
        const everything = createDefaultTools(context.cwd).getAll();
        // An explicit per-call allowlist selects from the full set; a deny-only
        // scope narrows the agentType's set (or the full set when there is none).
        const base = perCallTools ? everything : (agentDefTools ?? everything);
        const { tools, invalid } = resolveToolScope(base, perCallTools, perCallDisallow);
        if (invalid.length > 0) {
          return `[Agent error: agent() lists unknown tool(s): ${invalid.join(', ')}]`;
        }
        effectiveTools = tools;
      }
      // Per-agent reasoning effort (parity: AgentTool `effort`). Precedence:
      // explicit per-call `effort` → the agentType's declared effort. parseEffort
      // validates/normalizes the value so an injected/typo'd effort never reaches
      // the sub-engine; when present it enables thinking at that level (mirrors
      // task.ts's effort→thinking mapping, one path).
      const effectiveEffort = parseEffort(opts?.effort) ?? (agentDefEffort !== undefined ? parseEffort(agentDefEffort) : undefined);
      const subEngine = context._engineFactory({
        provider,
        cwd: agentCwd,
        permissionMode,
        // Per-call scope wins; otherwise the agentType's tool allowlist.
        tools: effectiveTools,
        // The agentType's system prompt gives the sub-engine its specialist identity.
        systemPrompt: agentSystemPrompt,
        maxTurns: agentDefMaxTurns,
        ...(effectiveEffort !== undefined
          ? { thinking: { enabled: true, level: effectiveEffort as import('../model-capabilities').ThinkingLevel } }
          : {}),
        abortController: ac,
      });
      // Same return-and-stop framing as task sub-agents: a workflow agent should
      // complete its step and return, not run the full never-stop coding loop. When
      // running in an isolated worktree, prepend the shared worktree notice so the
      // child translates inherited paths to its worktree root.
      const framedPrompt = isolatedWorktree
        ? `${buildWorktreeNotice(context.cwd, agentCwd)}\n\n---\n\n${SUBAGENT_RETURN_DIRECTIVE}\n\n---\n\n${prompt}`
        : `${SUBAGENT_RETURN_DIRECTIVE}\n\n---\n\n${prompt}`;
      // Live progress: flush accumulated text to the on-disk output file as it
      // streams (first chunk immediately, then throttled) so `task_output` can
      // tail a running workflow agent rather than seeing nothing until it ends.
      let lastFlushAt = 0;
      const flushProgress = (): void => {
        if (!pm || !taskId || !outputPath) return;
        const now = Date.now();
        if (lastFlushAt !== 0 && now - lastFlushAt < PROGRESS_FLUSH_INTERVAL_MS) return;
        lastFlushAt = now;
        try { writeTaskOutputNoFollow(outputPath, result || '(no output)'); } catch { /* dir gone / symlink */ }
      };
      const finish = (status: 'completed' | 'failed', text: string, error?: string): void => {
        if (!pm || !taskId) return;
        try { if (outputPath) writeTaskOutputNoFollow(outputPath, text || '(no output)'); } catch { /* dir gone or symlink planted at the path */ }
        pm.updateEntry(taskId, {
          status,
          exitCode: status === 'failed' ? 1 : 0,
          // Final-result stats distinct from the live progress counters (parity:
          // AsyncAgentDetailDialog result-vs-progress split) — the authoritative
          // totals at the terminal transition, preferred by task_list for a
          // finished agent.
          resultToolCount: agentToolUses,
          resultTokenCount: agentTokens,
          // Surface the failure reason on the entry's dedicated error channel.
          ...(status === 'failed' && error ? { error } : {}),
        });
      };
      let agentTokens = 0;
      // Per-agent tool-use count (distinct from the run-wide totalToolUses) so a
      // finished workflow agent's resultToolCount reflects ITS OWN tool calls.
      let agentToolUses = 0;
      try {
        for await (const event of subEngine.submit(framedPrompt)) {
          if (event.type === 'text_delta') { result += event.text; flushProgress(); }
          else if (event.type === 'tool_use_start') {
            totalToolUses++;
            agentToolUses++;
            // Live tool count + current-activity summary on the task entry (parity:
            // task.progress.toolCount / progress.summary); stamps lastActivity.
            if (pm && taskId) {
              const act = typeof event.input?.command === 'string'
                ? event.input.command
                : event.toolName;
              pm.updateEntry(taskId, {
                toolCount: agentToolUses,
                progressSummary: `${event.toolName}: ${act}`.slice(0, 80),
              });
            }
          }
          else if (event.type === 'usage_update') {
            // usage_update carries the agent's cumulative token total; keep the
            // latest and mirror it onto the process entry as a live token count.
            agentTokens = event.stats?.totalTokens ?? agentTokens;
            if (pm && taskId) pm.updateEntry(taskId, { tokenCount: agentTokens });
          }
          // Report the latest per-agent counts to the runtime so its
          // `agent-complete` event carries the completion affordance (tool uses +
          // tokens); the runtime adds the measured duration.
          report?.({ toolUseCount: agentToolUses, tokenCount: agentTokens });
        }
      } catch (err) {
        runAbortSignal.removeEventListener('abort', forwardAbort);
        totalAgentTokens += agentTokens;
        finish('failed', result, err instanceof Error ? err.message : String(err));
        (subEngine as any).shutdown?.();
        await cleanupWorktree();
        throw err;
      }
      runAbortSignal.removeEventListener('abort', forwardAbort);
      totalAgentTokens += agentTokens;
      (subEngine as any).shutdown?.();
      finish('completed', result);
      await cleanupWorktree();
      return result || '[Agent produced no output]';
    };

    const workflowResolver: WorkflowResolver = (nameOrRef) => {
      let scriptContent: string | null = null;
      if (typeof nameOrRef === 'string') {
        const searchPaths = [
          path.join(context.cwd, '.ami', 'workflows', `${nameOrRef}.js`),
          path.join(context.cwd, '.ami', 'workflows', `${nameOrRef}.mjs`),
        ];
        const found = searchPaths.find(p => fs.existsSync(p));
        if (found) scriptContent = fs.readFileSync(found, 'utf-8');
      } else if (nameOrRef.scriptPath) {
        if (fs.existsSync(nameOrRef.scriptPath)) {
          scriptContent = fs.readFileSync(nameOrRef.scriptPath, 'utf-8');
        }
      }
      return scriptContent;
    };

    // Shared run body: execute the workflow and assemble the completion output
    // text. Used VERBATIM by BOTH the foreground (blocking) and background
    // (detached) paths so the result wording — agents, tokens, logs, result, resume
    // hint — is identical regardless of how the run was launched (no semantic
    // duplication between the two paths).
    const runWorkflow = async (progressTaskId?: string): Promise<string> => {
      // Opt-in resume cache (B2): keyed per run/resume id, else per resumeKey,
      // under a contained .cache subdir (segment sanitized to block traversal).
      const cacheNamespace = effectiveRunId
        ?? (typeof input.resumeKey === 'string' && input.resumeKey.trim()
          ? input.resumeKey.replace(/[^A-Za-z0-9_-]/g, '_')
          : undefined);
      const cache = cacheNamespace
        ? new DiskWorkflowCache(path.join(context.cwd, '.ami', 'workflows', '.cache', cacheNamespace))
        : undefined;

      // Per-agent completion affordances (parity: the reference agent result's
      // `Done (N tool uses · M tokens · Xs)`). The runtime's `agent-complete`
      // carries the tool-use/token counts (reported by the handler) + measured
      // duration; we format one summary line per completed agent and surface them
      // in the tool output so the TUI renders what each spawned agent did.
      const agentSummaries: string[] = [];
      // U2 live progress: for a BACKGROUND run, enrich the workflow's own tracked
      // entry with the current phase, running agent counts, and aggregate token/tool
      // totals as the runtime emits them — so /flows and the progress tree render
      // live progress. No new EngineEvent: the TUI polls this entry like any task
      // (reuses the existing progress-summary/token channels — no duplication).
      let agentsStarted = 0;
      let agentsCompleted = 0;
      let currentPhase: string | undefined;
      const liveSummary = (): string => {
        const phase = currentPhase ? `${currentPhase} · ` : '';
        return `${phase}${agentsCompleted}/${agentsStarted} agents`.slice(0, 80);
      };
      const pushLiveProgress = (): void => {
        if (progressTaskId && pm) {
          pm.updateEntry(progressTaskId, {
            progressSummary: liveSummary(),
            tokenCount: totalAgentTokens,
            toolCount: totalToolUses,
          });
        }
      };
      const result = await executeWorkflow(script, agentHandler, {
        args: input.args,
        abortSignal: runAbortSignal,
        workflowResolver,
        maxConcurrent: typeof input.maxConcurrent === 'number' ? input.maxConcurrent : undefined,
        cache,
        // Capture the live runtime so a per-agent task_kill retry/skip can recover
        // one wedged agent mid-run (parity: onRetryAgent / onSkipAgent), subscribe
        // to agent-complete for the per-agent completion summaries, and (background
        // run only) drive the run entry's live phase/agent-count progress.
        onRuntime: (rt) => {
          wfRuntime = rt;
          rt.on('agent-complete', (e: { label?: string; toolUseCount?: number; tokenCount?: number; durationMs?: number }) => {
            const prefix = e.label ? `${e.label}: ` : '';
            agentSummaries.push(`${prefix}${formatAgentCompletion(e)}`);
            agentsCompleted++;
            pushLiveProgress();
          });
          if (progressTaskId && pm) {
            rt.on('agent-start', () => { agentsStarted++; pushLiveProgress(); });
            rt.on('phase', (title: string) => { if (typeof title === 'string') currentPhase = title; pushLiveProgress(); });
          }
        },
      });

      const durationS = ((Date.now() - workflowStartedAt) / 1000).toFixed(1);
      return [
        `Workflow '${meta.name}' completed.`,
        `Agents spawned: ${result.agentCount}`,
        `Tokens: ${totalAgentTokens} · Tool uses: ${totalToolUses} · Duration: ${durationS}s`,
        effectiveRunId ? `Run id: ${effectiveRunId}` : '',
        agentSummaries.length > 0 ? `\nAgents:\n${agentSummaries.map(s => `  - ${s}`).join('\n')}` : '',
        result.logs.length > 0 ? `\nLogs:\n${result.logs.map(l => `  - ${l}`).join('\n')}` : '',
        result.result !== null ? `\nResult: ${JSON.stringify(result.result, null, 2)}` : '',
        resumeHint ? `\n${resumeHint}` : '',
      ].filter(Boolean).join('\n');
    };

    // --- Foreground (blocking) path: run inline, return the full result. ---
    if (!runInBackground) {
      try {
        const output = await runWorkflow();
        return { output, isError: false };
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        const output = resumeHint
          ? `Error executing workflow: ${message}\n${resumeHint}`
          : `Error executing workflow: ${message}`;
        return { output, isError: true };
      }
    }

    // --- Background (detached) path: register ONE tracked workflow task, kick the
    // run on a detached promise, and return a task id immediately (parity with the
    // task/bash run_in_background spawn-and-return pattern). The whole run is one
    // process-manager entry of kind 'workflow'; its per-agent children register
    // their own 'agent' entries + join the flow exactly as before. Completion
    // surfaces a <task-notification> FOR FREE via the SHARED emitCompletion →
    // drainCompletedTasks path — no separate workflow-completion bridge. ---
    const wfTaskId = generateTaskId('workflow');
    const wfOutputPath = path.join(projectStateDir(context.cwd, 'tasks'), `${wfTaskId}.output`);
    try { fs.mkdirSync(path.dirname(wfOutputPath), { recursive: true }); } catch { /* best-effort */ }
    try { writeTaskOutputNoFollow(wfOutputPath, `Workflow '${meta.name}' running in the background…\n`); } catch { /* ok */ }

    // Link the parent turn's abort to the detached run so an outer cancel still
    // stops it, while the run otherwise survives this tool call returning.
    const onParentAbort = (): void => { workflowAbort!.abort(); };
    if (context.abortSignal.aborted) workflowAbort!.abort();
    else context.abortSignal.addEventListener('abort', onParentAbort, { once: true });
    const unlinkParentAbort = (): void => {
      try { context.abortSignal.removeEventListener('abort', onParentAbort); } catch { /* ok */ }
    };

    if (pm) {
      pm.registerAgent(wfTaskId, {
        command: `[workflow] ${meta.name}`,
        description: `Workflow: ${meta.name}`,
        outputPath: wfOutputPath,
        abortFn: () => { workflowAbort!.abort(); },
        abortSignal: workflowAbort!.signal,
        kind: 'workflow',
      });
    }

    void (async () => {
      let sawError = false;
      let out = '';
      try {
        out = await runWorkflow(wfTaskId);
      } catch (err) {
        sawError = true;
        const message = err instanceof Error ? err.message : String(err);
        out = resumeHint
          ? `Error executing workflow: ${message}\n${resumeHint}`
          : `Error executing workflow: ${message}`;
      }
      // Classify the terminal state the same way the task background drain does: an
      // external abort (/flows stop, task_kill, parent cancel) is a KILL; a thrown
      // error is a FAILURE; otherwise COMPLETED.
      const killed = workflowAbort!.signal.aborted;
      const status: 'completed' | 'failed' | 'killed' = killed ? 'killed' : sawError ? 'failed' : 'completed';
      const exitCode = status === 'completed' ? 0 : 1;
      try { writeTaskOutputNoFollow(wfOutputPath, out || '(no output)'); } catch { /* dir gone / symlink */ }
      unlinkParentAbort();
      if (pm) {
        // Record final-result stats distinct from the live counters, then route the
        // completion through the DURABLE buffer (emitCompletion) so the engine's
        // per-turn drain surfaces a <task-notification> exactly once — the SAME path
        // a background agent/bash task uses (reused, not rebuilt).
        pm.updateEntry(wfTaskId, {
          status,
          exitCode,
          resultTokenCount: totalAgentTokens,
          resultToolCount: totalToolUses,
        });
        pm.emitCompletion({
          taskId: wfTaskId,
          exitCode,
          command: `[workflow] ${meta.name}`,
          description: `Workflow: ${meta.name}`,
          outputPath: wfOutputPath,
          status,
        });
      }
    })().catch(() => { /* detached: any error is already captured into the output file */ });

    return {
      output:
        `Workflow '${meta.name}' started in the background. Task id: ${wfTaskId}.\n` +
        `Monitor it live with /flows, or tail it with task_output({ task_id: "${wfTaskId}" }); ` +
        `task_output({ task_id: "${wfTaskId}", wait: true }) blocks until it finishes and returns the result. ` +
        `A completion notification surfaces automatically when the run ends.` +
        (effectiveRunId ? `\nRun id: ${effectiveRunId}` : '') +
        (resumeHint ? `\n${resumeHint}` : ''),
      isError: false,
    };
  },
};
