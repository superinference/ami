import { EventEmitter } from 'events';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as crypto from 'crypto';
import { parseEnvInt } from '../utils/env';
import { formatDuration } from '../conversation-summary';
import type { PermissionMode } from '../permissions';

/**
 * Opt-in cache of completed agent() results so a re-run/resume of a workflow
 * skips agents that already finished (B2). Keyed on a stable hash of the prompt
 * and the semantic agent options.
 */
export interface WorkflowAgentCache {
  get(key: string): unknown | undefined;
  set(key: string, value: unknown): void;
}

/** Stable cache key for an agent call — prompt + semantic opts (label/phase,
 *  which are display-only, are excluded so a relabel does not bust the cache).
 *  `maxTurns` is a semantic cap on the agent's work, so it participates in the
 *  key — two calls that differ only by their turn budget are not conflated. */
export function workflowAgentCacheKey(prompt: string, opts?: { schema?: unknown; model?: string; isolation?: string; agentType?: string; maxTurns?: number; permissionMode?: string; tools?: string[]; disallowedTools?: string[]; effort?: string | number }): string {
  const norm = {
    prompt,
    schema: opts?.schema ?? null,
    model: opts?.model ?? null,
    isolation: opts?.isolation ?? null,
    agentType: opts?.agentType ?? null,
    maxTurns: opts?.maxTurns ?? null,
    permissionMode: opts?.permissionMode ?? null,
    // The per-agent capability surface (tool allow/deny) and reasoning effort are
    // semantic inputs: two otherwise-identical calls scoped to different tools or
    // run at a different effort are NOT the same work, so they key separately.
    // Allow/deny lists are sorted so order-only differences do not bust the cache.
    tools: Array.isArray(opts?.tools) ? [...opts.tools].sort() : null,
    disallowedTools: Array.isArray(opts?.disallowedTools) ? [...opts.disallowedTools].sort() : null,
    effort: opts?.effort ?? null,
  };
  return crypto.createHash('sha256').update(JSON.stringify(norm)).digest('hex');
}

/** Disk-backed {@link WorkflowAgentCache}: one JSON file per key under `dir`. */
export class DiskWorkflowCache implements WorkflowAgentCache {
  constructor(private readonly dir: string) {}
  private file(key: string): string { return path.join(this.dir, `${key}.json`); }
  get(key: string): unknown | undefined {
    try { return JSON.parse(fs.readFileSync(this.file(key), 'utf8')).value; } catch { return undefined; }
  }
  set(key: string, value: unknown): void {
    try { fs.mkdirSync(this.dir, { recursive: true }); fs.writeFileSync(this.file(key), JSON.stringify({ value })); } catch { /* best-effort */ }
  }
}

/**
 * Mockable filesystem seam (ESM-safe). Tests replace this via
 * `mock.method(_wfHooks, 'readFileSync', ...)` to drive the cgroup v1/v2 CPU
 * detection branches without a real container. Behavior-preserving: defaults to
 * the real `fs.readFileSync`.
 */
export const _wfHooks = {
  readFileSync: fs.readFileSync as typeof fs.readFileSync,
  cpus: os.cpus as typeof os.cpus,
};

export interface WorkflowMeta {
  name: string;
  description: string;
  phases?: Array<{ title: string; detail?: string; model?: string }>;
  whenToUse?: string;
}

export interface AgentResult {
  text: string;
  toolUseCount: number;
  tokenCount: number;
}

/**
 * [FUNCTIONAL PARITY] Per-agent work stats a handler reports back to the runtime
 * so the `agent-complete` event can carry a completion affordance (parity with
 * the reference AgentTool UI `Done (N tool uses · M tokens · Xs)`). The handler
 * (which alone sees the sub-engine's token/tool-use stream) calls this with the
 * latest counts; the runtime adds the measured wall-clock duration itself.
 */
export type WorkflowAgentReporter = (stats: { toolUseCount?: number; tokenCount?: number }) => void;

/**
 * [FUNCTIONAL PARITY] Format a spawned agent's completion affordance the way the
 * reference AgentTool UI does: `Done (N tool uses · M tokens · Xs)`. Singular
 * "1 tool use", the token count rendered with thousands separators, and the
 * duration via the shared {@link formatDuration}. Omits a stat whose value is
 * unknown (undefined), so a cached short-circuit (no handler run) still yields a
 * well-formed message.
 */
export function formatAgentCompletion(stats: { toolUseCount?: number; tokenCount?: number; durationMs?: number }): string {
  const parts: string[] = [];
  if (typeof stats.toolUseCount === 'number') {
    parts.push(stats.toolUseCount === 1 ? '1 tool use' : `${stats.toolUseCount} tool uses`);
  }
  if (typeof stats.tokenCount === 'number') {
    parts.push(`${stats.tokenCount.toLocaleString('en-US')} tokens`);
  }
  if (typeof stats.durationMs === 'number') {
    parts.push(formatDuration(stats.durationMs));
  }
  return parts.length > 0 ? `Done (${parts.join(' · ')})` : 'Done';
}

/**
 * [FUNCTIONAL PARITY] Resolve a tool allow/deny scope by NAME against the full
 * tool set (parity with the reference agent `tools` / `disallowedTools` fields):
 *  - `allow` is an allowlist: only tools whose name is listed survive. `'*'` in
 *    the allowlist means "all tools" (no allow-filter). Any allow name not in
 *    `all` is collected in `invalid` so the caller can reject the scope rather
 *    than silently dropping a typo'd tool.
 *  - `disallow` is applied AFTER the allowlist and removes those tools by name
 *    (deny wins over allow, defense-in-depth).
 * A single shared resolver so the per-call `agent(p, { tools })` scope and a
 * named agentType's declared scope go through ONE code path (no duplication).
 */
export function resolveToolScope<T extends { name: string }>(
  all: T[],
  allow?: string[],
  disallow?: string[],
): { tools: T[]; invalid: string[] } {
  let tools = all;
  const invalid: string[] = [];
  if (allow && allow.length > 0 && !allow.includes('*')) {
    const known = new Set(all.map(t => t.name));
    for (const name of allow) if (!known.has(name)) invalid.push(name);
    const allowSet = new Set(allow);
    tools = all.filter(t => allowSet.has(t.name));
  }
  if (disallow && disallow.length > 0) {
    const denySet = new Set(disallow);
    tools = tools.filter(t => !denySet.has(t.name));
  }
  return { tools, invalid };
}

/**
 * [FUNCTIONAL PARITY] Normalize a per-agent `model` value (parity with the
 * reference loader's model transform + `'inherit'` sentinel). A blank/whitespace
 * value or the explicit case-insensitive `'inherit'` sentinel means "fall back
 * to the parent/default model" and returns `undefined`; any other value is
 * returned trimmed. So a workflow can say `agent(p, { model: 'inherit' })` to
 * intentionally inherit, and a stray-whitespace model never reaches the handler.
 */
export function normalizeAgentModel(model?: string): string | undefined {
  if (typeof model !== 'string') return undefined;
  const trimmed = model.trim();
  if (trimmed === '' || trimmed.toLowerCase() === 'inherit') return undefined;
  return trimmed;
}

/**
 * The injected per-agent worker. Receives the stable {@link WorkflowRuntime}
 * agent id (the same number surfaced on `agent-start`/`agent-complete` as
 * `agentNumber`) as a third argument so the consumer can correlate the spawned
 * work (e.g. a process-manager task) back to the live agent and drive per-agent
 * {@link WorkflowRuntime.retryAgent}/{@link WorkflowRuntime.skipAgent} recovery.
 */
export type AgentHandler = (prompt: string, opts?: AgentOptions, agentId?: number, report?: WorkflowAgentReporter) => Promise<unknown>;

export interface AgentOptions {
  label?: string;
  phase?: string;
  schema?: Record<string, unknown>;
  /**
   * [FUNCTIONAL PARITY] Per-agent model override (parity with the reference agent
   * `model`). An explicit `'inherit'` sentinel (or a blank value) means "use the
   * parent/default model" — see {@link normalizeAgentModel}. Part of
   * {@link workflowAgentCacheKey}.
   */
  model?: string;
  isolation?: 'worktree';
  agentType?: string;
  /**
   * [FUNCTIONAL PARITY] Per-agent tool allowlist by NAME (parity with the
   * reference agent `tools`). When set, this single `agent()` call is scoped to
   * only these tools (`'*'` = all). It overrides the resolved agentType's tool
   * set. Resolved via {@link resolveToolScope}; part of {@link workflowAgentCacheKey}.
   */
  tools?: string[];
  /**
   * [FUNCTIONAL PARITY] Per-agent tool denylist by NAME (parity with the
   * reference agent `disallowedTools`). Removed from the effective tool set AFTER
   * any allowlist (deny wins). Part of {@link workflowAgentCacheKey}.
   */
  disallowedTools?: string[];
  /**
   * [FUNCTIONAL PARITY] Per-agent reasoning-effort level (parity with the
   * reference agent `effort`): an {@link import('../skills').EffortLevel} name
   * ('low'|'medium'|'high'|'xhigh'|'max') or a non-negative integer. Threaded to
   * the agent handler to set the spawned sub-engine's thinking depth independently
   * of the model, and part of {@link workflowAgentCacheKey}.
   */
  effort?: string | number;
  /**
   * [FUNCTIONAL PARITY] Per-agent permission posture for this single `agent()`
   * call (parity with the reference AgentTool `mode`). When set to a recognized
   * {@link PermissionMode} it overrides the workflow's default `auto-allow`
   * posture for just this spawned agent (e.g. `'plan'` to require plan approval,
   * `'deny-all'` to tighten). It is part of {@link workflowAgentCacheKey} so two
   * otherwise-identical calls run under different postures are cached separately.
   */
  permissionMode?: PermissionMode;
  /**
   * [FUNCTIONAL PARITY] Cap the number of agentic turns this single `agent()`
   * call may take (parity with the reference per-agent `maxTurns`). It is passed
   * through to the injected agent handler, which enforces the bound, and it is
   * part of {@link workflowAgentCacheKey} so two otherwise-identical calls with a
   * different turn budget are cached separately.
   */
  maxTurns?: number;
}

export interface WorkflowBudget {
  total: number | null;
  spent(): number;
  remaining(): number;
}

export type WorkflowResolver = (nameOrRef: string | { scriptPath: string }) => string | null;

export interface WorkflowContext {
  agent: AgentHandler;
  parallel: <T>(thunks: Array<() => Promise<T>>) => Promise<Array<T | null>>;
  pipeline: <T, S1, S2 = S1, S3 = S2>(
    items: T[],
    ...stages: Array<(prev: any, item: T, index: number) => Promise<any>>
  ) => Promise<any[]>;
  phase: (title: string) => void;
  log: (message: string) => void;
  args: unknown;
  budget: WorkflowBudget;
  saveCheckpoint: (key: string, data: unknown) => void;
  loadCheckpoint: (key: string) => unknown | undefined;
  workflow: (nameOrRef: string | { scriptPath: string }, args?: unknown) => Promise<unknown>;
}

export function detectCpuCount(): number {
  const osCpus = _wfHooks.cpus()?.length ?? 8;
  try {
    const raw = _wfHooks.readFileSync('/sys/fs/cgroup/cpu.max', 'utf-8').trim();
    const [quotaStr, periodStr] = raw.split(/\s+/);
    if (quotaStr !== 'max') {
      const quota = parseInt(quotaStr!, 10);
      const period = parseInt(periodStr!, 10);
      if (quota > 0 && period > 0) return Math.max(1, Math.floor(quota / period));
    }
  } catch { /* not in a cgroup-v2 container */ }
  try {
    const quota = parseInt(_wfHooks.readFileSync('/sys/fs/cgroup/cpu/cpu.cfs_quota_us', 'utf-8').trim(), 10);
    const period = parseInt(_wfHooks.readFileSync('/sys/fs/cgroup/cpu/cpu.cfs_period_us', 'utf-8').trim(), 10);
    if (quota > 0 && period > 0) return Math.max(1, Math.floor(quota / period));
  } catch { /* not in a cgroup-v1 container */ }
  return osCpus;
}
export function getMaxConcurrent(): number {
  const n = parseEnvInt(process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS, 0);
  if (n >= 1 && n <= 256) return n;
  return Math.max(4, Math.min(16, detectCpuCount() - 2));
}
const DEFAULT_MAX_CONCURRENT = getMaxConcurrent();
const MAX_AGENTS = 1000;
const MAX_PIPELINE_ITEMS = 4096;

/**
 * Strip the `export const meta = { ... }` block from a workflow script using
 * brace-counting so nested objects (e.g. phases with detail objects) are handled
 * correctly, unlike the previous lazy regex approach.
 */
export function stripMeta(script: string): string {
  const match = script.match(/export\s+const\s+meta\s*=\s*\{/);
  if (!match) return script;
  // A non-global .match() always populates .index when it matches.
  const start = match.index!;
  let depth = 0;
  let inString = false;
  let stringChar = '';
  let end = start + match[0].length;
  for (let i = end - 1; i < script.length; i++) {
    const ch = script[i];
    if (inString) {
      if (ch === stringChar && script[i - 1] !== '\\') inString = false;
      continue;
    }
    if (ch === '"' || ch === "'" || ch === '`') { inString = true; stringChar = ch; continue; }
    if (ch === '{') depth++;
    if (ch === '}') { depth--; if (depth === 0) { end = i + 1; break; } }
  }
  return script.slice(0, start) + script.slice(end);
}

/**
 * [FUNCTIONAL PARITY] Sliding-window concurrency scheduler (parity with the
 * reference `all(generators, concurrencyCap)` in utils/generators): start at most
 * `cap` workers, and launch the next item ONLY when a running one finishes, so at
 * no point are more than `cap` promises materialized — regardless of what the
 * worker does. This bounds ALL work (not just `agent()` calls, which the separate
 * per-agent semaphore caps), so a large `parallel()`/`pipeline()` batch no longer
 * eagerly spins up thousands of simultaneous promises.
 *
 * Results are written back by index, so the returned array preserves input order
 * even though items complete out of order. `onLaunch` runs (awaited) just before
 * each worker is invoked, in ascending launch order, so a caller can gate/space
 * launches (e.g. the prompt-cache stagger).
 */
export async function mapWithConcurrency<T, R>(
  items: T[],
  cap: number,
  worker: (item: T, index: number) => Promise<R>,
  onLaunch?: (index: number) => void | Promise<void>,
): Promise<R[]> {
  const results = new Array<R>(items.length);
  let nextIndex = 0;
  const windowSize = Math.max(1, Math.min(cap, items.length));

  const runner = async (): Promise<void> => {
    for (;;) {
      const index = nextIndex++;
      if (index >= items.length) return;
      if (onLaunch) await onLaunch(index);
      results[index] = await worker(items[index]!, index);
    }
  };

  const runners: Array<Promise<void>> = [];
  for (let i = 0; i < windowSize; i++) runners.push(runner());
  await Promise.all(runners);
  return results;
}

export class WorkflowRuntime extends EventEmitter {
  private _currentPhase = '';
  private _agentCount = 0;
  private _activeSemaphore = 0;
  private _waitQueue: Array<() => void> = [];
  private _tokensSpent = 0;
  private _aborted = false;
  private readonly _agentHandler: AgentHandler;
  private readonly _budgetTotal: number | null;
  private readonly _maxConcurrent: number;
  private readonly _logs: string[] = [];
  private readonly _phaseAgents = new Map<string, number>();
  private checkpoints: Map<string, unknown> = new Map();
  private readonly _workflowResolver: WorkflowResolver | null;
  private readonly _nestingDepth: number;
  private readonly _cache: WorkflowAgentCache | null;
  /**
   * [FUNCTIONAL PARITY] Live controls for in-flight agents, keyed by the stable
   * per-agent id. Each entry lets a consumer recover a SINGLE wedged/failed agent
   * inside a still-running workflow without aborting the whole run — parity with
   * the reference workflow detail actions `onSkipAgent`/`onRetryAgent`, which call
   * `skipWorkflowAgent(workflowId, agentId)` / `retryWorkflowAgent(workflowId, agentId)`.
   * An agent is present here only while its handler call is in-flight (between
   * slot acquisition and completion), so {@link liveAgentIds} is the set of agents
   * eligible for retry/skip right now.
   */
  private readonly _agentControls = new Map<number, { agentId: number; label?: string; phase: string; skip: () => void; retry: () => void }>();

  constructor(options: {
    agentHandler: AgentHandler;
    budgetTotal?: number | null;
    workflowResolver?: WorkflowResolver | null;
    nestingDepth?: number;
    maxConcurrent?: number;
    cache?: WorkflowAgentCache | null;
  }) {
    super();
    // EventEmitter caps listeners at 10 and emits a MaxListenersExceededWarning
    // beyond that. A parent workflow registers one 'abort' listener per nested
    // workflow() call (see createWorkflowCaller), so a legitimate deep fan-out
    // exceeds 10. Disable the cap — parity with the reference, which calls
    // setMaxListeners(0) on its long-lived event emitters.
    this.setMaxListeners(0);
    this._agentHandler = options.agentHandler;
    this._budgetTotal = options.budgetTotal ?? null;
    this._workflowResolver = options.workflowResolver ?? null;
    this._nestingDepth = options.nestingDepth ?? 0;
    this._maxConcurrent = Math.max(1, Math.min(256, Math.floor(options.maxConcurrent ?? DEFAULT_MAX_CONCURRENT)));
    this._cache = options.cache ?? null;
  }

  get logs(): string[] { return [...this._logs]; }
  get agentCount(): number { return this._agentCount; }
  get currentPhase(): string { return this._currentPhase; }

  abort(): void {
    this._aborted = true;
    this.emit('abort');
  }

  /**
   * [FUNCTIONAL PARITY] Ids of the agents that are in-flight right now and thus
   * eligible for a targeted {@link retryAgent}/{@link skipAgent}. Mirrors the
   * reference workflow detail view, which lists the live agents a user may retry
   * or skip. An id is the same number reported as `agentNumber` on the
   * `agent-start` event.
   */
  liveAgentIds(): number[] {
    return [...this._agentControls.keys()];
  }

  /**
   * [FUNCTIONAL PARITY] Skip ONE wedged/failed agent in a still-running workflow
   * (parity with the reference `skipWorkflowAgent(workflowId, agentId)` /
   * `onSkipAgent`). The in-flight `agent()` call settles immediately as a skip
   * (resolving to `null`, the same value a failed agent yields) so the workflow
   * continues instead of being aborted wholesale. Emits `agent-skipped`. Returns
   * `false` when no agent with that id is currently in-flight.
   */
  skipAgent(agentId: number): boolean {
    const ctrl = this._agentControls.get(agentId);
    if (!ctrl) return false;
    ctrl.skip();
    return true;
  }

  /**
   * [FUNCTIONAL PARITY] Retry ONE wedged agent in a still-running workflow
   * (parity with the reference `retryWorkflowAgent(workflowId, agentId)` /
   * `onRetryAgent`). The current (stuck) attempt is abandoned and the agent
   * handler is re-invoked for the same agent, holding the same concurrency slot
   * and agent id. Emits `agent-retry`. Returns `false` when no agent with that id
   * is currently in-flight.
   */
  retryAgent(agentId: number): boolean {
    const ctrl = this._agentControls.get(agentId);
    if (!ctrl) return false;
    ctrl.retry();
    return true;
  }

  saveCheckpoint(key: string, data: unknown): void {
    this.checkpoints.set(key, data);
  }

  loadCheckpoint(key: string): unknown | undefined {
    return this.checkpoints.get(key);
  }

  createContext(args: unknown): WorkflowContext {
    return {
      agent: this.createAgent(),
      parallel: this.createParallel(),
      pipeline: this.createPipeline(),
      phase: (title: string) => this.setPhase(title),
      log: (message: string) => this.addLog(message),
      args,
      budget: {
        total: this._budgetTotal,
        spent: () => this._tokensSpent,
        remaining: () => this._budgetTotal !== null
          ? Math.max(0, this._budgetTotal - this._tokensSpent)
          : Infinity,
      },
      saveCheckpoint: (key: string, data: unknown) => this.saveCheckpoint(key, data),
      loadCheckpoint: (key: string) => this.loadCheckpoint(key),
      workflow: this.createWorkflowCaller(),
    };
  }

  private createWorkflowCaller(): (nameOrRef: string | { scriptPath: string }, args?: unknown) => Promise<unknown> {
    return async (nameOrRef, args) => {
      if (this._nestingDepth >= 1) {
        throw new Error('Nested workflow() calls are limited to one level of depth');
      }
      if (!this._workflowResolver) {
        throw new Error('workflow() is not available: no workflow resolver configured');
      }

      const script = this._workflowResolver(nameOrRef);
      if (!script) {
        const ref = typeof nameOrRef === 'string' ? nameOrRef : nameOrRef.scriptPath;
        throw new Error(`Workflow not found: ${ref}`);
      }

      const childRuntime = new WorkflowRuntime({
        maxConcurrent: this._maxConcurrent,
        agentHandler: this._agentHandler,
        budgetTotal: this._budgetTotal,
        workflowResolver: null,
        nestingDepth: this._nestingDepth + 1,
      });

      if (this._aborted) childRuntime.abort();
      this.on('abort', () => childRuntime.abort());

      const childCtx = childRuntime.createContext(args);
      const childBody = stripMeta(script);
      validateWorkflowScript(childBody);
      const wrappedScript = `
        return (async function(agent, parallel, pipeline, phase, log, args, budget, saveCheckpoint, loadCheckpoint, workflow) {
          ${childBody}
        })
      `;
      const factory = new Function(wrappedScript)(); // eslint-disable-line no-new-func
      const result = await factory(
        childCtx.agent, childCtx.parallel, childCtx.pipeline, childCtx.phase,
        childCtx.log, childCtx.args, childCtx.budget, childCtx.saveCheckpoint,
        childCtx.loadCheckpoint, childCtx.workflow,
      );

      this._agentCount += childRuntime.agentCount;
      this._tokensSpent += childRuntime['_tokensSpent'];
      for (const log of childRuntime.logs) this._logs.push(log);

      return result ?? null;
    };
  }

  private createAgent(): AgentHandler {
    return async (prompt: string, opts?: AgentOptions): Promise<unknown> => {
      if (this._aborted) throw new Error('Workflow aborted');
      if (this._agentCount >= MAX_AGENTS) {
        throw new Error(`Agent limit reached (${MAX_AGENTS})`);
      }
      if (this._budgetTotal !== null && this._tokensSpent >= this._budgetTotal) {
        throw new Error('Budget exhausted');
      }

      // [FUNCTIONAL PARITY] Measure this agent's wall-clock duration + collect the
      // tool-use/token counts the handler reports, so `agent-complete` carries a
      // completion affordance (parity with the reference `Done (N tool uses · M
      // tokens · Xs)`). The runtime owns the clock; the handler owns the counts.
      const startedAt = Date.now();
      const agentStats: { toolUseCount?: number; tokenCount?: number } = {};
      const report: WorkflowAgentReporter = (s) => {
        if (typeof s.toolUseCount === 'number') agentStats.toolUseCount = s.toolUseCount;
        if (typeof s.tokenCount === 'number') agentStats.tokenCount = s.tokenCount;
      };

      this._agentCount++;
      // Capture a STABLE per-agent id at spawn time. `this._agentCount` keeps
      // incrementing as concurrent agents spawn, so reading it later would
      // misattribute events/controls under parallelism — the captured id is the
      // single identifier used for this agent's events, cache, and live controls.
      const agentId = this._agentCount;
      const phase = opts?.phase ?? this._currentPhase;
      this._phaseAgents.set(phase, (this._phaseAgents.get(phase) ?? 0) + 1);

      // Resume short-circuit (B2): a cached result for this exact (prompt, opts)
      // skips the handler entirely, so a re-run/resume doesn't re-invoke an agent
      // that already completed. Only non-null results are cached (null = skip/error).
      const cacheKey = this._cache ? workflowAgentCacheKey(prompt, opts) : null;
      if (cacheKey && this._cache) {
        const cached = this._cache.get(cacheKey);
        if (cached !== undefined) {
          this.emit('agent-start', { prompt: prompt.slice(0, 100), label: opts?.label, phase, agentNumber: agentId, cached: true });
          // A cached short-circuit ran no handler, so tool-use/token counts are
          // unknown; only the (negligible) measured duration is surfaced.
          this.emit('agent-complete', { label: opts?.label, phase, agentNumber: agentId, cached: true, durationMs: Date.now() - startedAt });
          return cached;
        }
      }

      await this.acquireSlot();
      try {
        this.emit('agent-start', {
          prompt: prompt.slice(0, 100),
          label: opts?.label,
          phase,
          agentNumber: agentId,
        });

        // Produce the agent's result once. Factored out so the live-control race
        // ({@link runWithAgentControl}) can re-invoke it on a retry.
        const produce = async (): Promise<unknown> => {
          if (opts?.schema) {
            // [FUNCTIONAL PARITY] Schema-forced StructuredOutput: validate the
            // handler's output against opts.schema and retry on mismatch (parity
            // with the reference runtime, which re-asks the agent until it returns conforming
            // output or attempts are exhausted). A single agent() call still counts
            // as one agent — retries are internal and hold the same slot.
            const maxAttempts = schemaMaxAttempts();
            let validated: unknown | undefined;
            for (let attempt = 1; attempt <= maxAttempts; attempt++) {
              const raw = await this._agentHandler(prompt, opts, agentId, report);
              validated = validateStructuredOutput(raw, opts.schema);
              if (validated !== undefined) break;
              if (attempt < maxAttempts) {
                this.emit('agent-schema-retry', {
                  label: opts?.label,
                  phase,
                  attempt,
                  maxAttempts,
                  agentNumber: agentId,
                });
              }
            }
            if (validated === undefined) {
              throw new Error(`Workflow agent output failed schema validation after ${maxAttempts} attempt(s)`);
            }
            return validated;
          }
          return await this._agentHandler(prompt, opts, agentId, report);
        };

        const outcome = await this.runWithAgentControl(agentId, opts?.label, phase, produce);
        if (outcome.skipped) {
          this.emit('agent-skipped', { label: opts?.label, phase, agentNumber: agentId });
          return null;
        }
        const result = outcome.value;

        this.emit('agent-complete', {
          label: opts?.label,
          phase,
          agentNumber: agentId,
          // [FUNCTIONAL PARITY] Per-agent completion affordance: the tool-use/token
          // counts the handler reported plus the measured duration, so a consumer
          // can render `Done (N tool uses · M tokens · Xs)` (see formatAgentCompletion).
          toolUseCount: agentStats.toolUseCount,
          tokenCount: agentStats.tokenCount,
          durationMs: Date.now() - startedAt,
        });

        if (cacheKey && this._cache && result !== null && result !== undefined) {
          this._cache.set(cacheKey, result);
        }

        return result;
      } catch (err) {
        this.emit('agent-error', {
          label: opts?.label,
          phase,
          error: err instanceof Error ? err.message : String(err),
        });
        return null;
      } finally {
        this.releaseSlot();
      }
    };
  }

  /**
   * [FUNCTIONAL PARITY] Run the agent's `produce()` while a live control for it is
   * registered, racing the work against targeted skip/retry signals:
   *  - resolves `produce()` → the agent's result;
   *  - {@link skipAgent} → settle as skipped (`{ skipped: true }`);
   *  - {@link retryAgent} → abandon the current attempt and re-invoke `produce()`.
   * The abandoned attempt's eventual settle is swallowed (never an unhandled
   * rejection); the consumer wiring is responsible for tearing down the real work
   * (e.g. aborting a sub-engine) on retry/skip.
   */
  private async runWithAgentControl(
    agentId: number,
    label: string | undefined,
    phase: string,
    produce: () => Promise<unknown>,
  ): Promise<{ skipped: boolean; value?: unknown }> {
    for (;;) {
      let fired = false;
      let fire!: (signal: 'skip' | 'retry') => void;
      const control = new Promise<'skip' | 'retry'>((resolve) => {
        fire = (signal) => { if (!fired) { fired = true; resolve(signal); } };
      });
      this._agentControls.set(agentId, {
        agentId,
        label,
        phase,
        skip: () => fire('skip'),
        retry: () => fire('retry'),
      });
      let outcome:
        | { kind: 'done'; value: unknown }
        | { kind: 'error'; error: unknown }
        | { kind: 'skip' | 'retry' };
      try {
        const produced = produce().then(
          (value) => ({ kind: 'done' as const, value }),
          (error) => ({ kind: 'error' as const, error }),
        );
        outcome = await Promise.race([produced, control.then((signal) => ({ kind: signal }))]);
      } finally {
        this._agentControls.delete(agentId);
      }
      if (outcome.kind === 'done') return { skipped: false, value: outcome.value };
      if (outcome.kind === 'error') throw outcome.error;
      if (outcome.kind === 'skip') return { skipped: true };
      // retry: abandon the stuck attempt and re-invoke produce() on the next loop.
      this.emit('agent-retry', { label, phase, agentNumber: agentId });
    }
  }

  private createParallel(): <T>(thunks: Array<() => Promise<T>>) => Promise<Array<T | null>> {
    const STAGGER_DELAY = parseEnvInt(process.env.AMI_WORKFLOW_PREFIX_STAGGER_MS, 50);
    const cap = this._maxConcurrent;

    return async <T>(thunks: Array<() => Promise<T>>): Promise<Array<T | null>> => {
      if (thunks.length > MAX_PIPELINE_ITEMS) {
        throw new Error(`parallel() limit exceeded: ${thunks.length} > ${MAX_PIPELINE_ITEMS}`);
      }

      // Sliding-window scheduler (see mapWithConcurrency): at most `cap` thunks are
      // ever in flight, so even non-agent thunks (which never hit the agent
      // semaphore) are bounded and we never materialize thousands of promises at
      // once. A failed thunk resolves to null (same contract as before).
      //
      // Launch stagger: successive launches are spaced by STAGGER_DELAY so later
      // agents hit the prompt cache. The gate serializes launch times via a shared
      // `nextLaunchAt` watermark (assigned synchronously in ascending launch order)
      // so even when `cap` runners start together, their worker invocations are
      // still spaced — the first launch waits 0ms.
      let nextLaunchAt = 0;
      const stagger = STAGGER_DELAY > 0
        ? async (): Promise<void> => {
          const now = Date.now();
          const wait = Math.max(0, nextLaunchAt - now);
          nextLaunchAt = Math.max(now, nextLaunchAt) + STAGGER_DELAY;
          if (wait > 0) await new Promise(r => setTimeout(r, wait));
        }
        : undefined;
      return mapWithConcurrency(
        thunks,
        cap,
        async (thunk) => {
          try { return await thunk(); } catch { return null as T | null; }
        },
        stagger,
      );
    };
  }

  private createPipeline(): <T>(items: T[], ...stages: Array<(prev: any, item: T, idx: number) => Promise<any>>) => Promise<any[]> {
    const cap = this._maxConcurrent;
    return async <T>(items: T[], ...stages: Array<(prev: any, item: T, idx: number) => Promise<any>>): Promise<any[]> => {
      if (items.length > MAX_PIPELINE_ITEMS) {
        throw new Error(`pipeline() limit exceeded: ${items.length} > ${MAX_PIPELINE_ITEMS}`);
      }

      // Sliding-window scheduler bounds concurrent per-item stage chains at `cap`
      // (parity with the reference `all()`), so a pipeline whose stages do
      // non-agent work no longer runs every item at once with no bound.
      return mapWithConcurrency(items, cap, async (item, idx) => {
        let prev: any = item;
        for (const stage of stages) {
          try {
            prev = await stage(prev, item, idx);
          } catch {
            return null;
          }
        }
        return prev;
      });
    };
  }

  private setPhase(title: string): void {
    this._currentPhase = title;
    this.emit('phase', title);
  }

  private addLog(message: string): void {
    this._logs.push(message);
    this.emit('log', message);
  }

  addTokens(count: number): void {
    this._tokensSpent += count;
  }

  private async acquireSlot(): Promise<void> {
    if (this._activeSemaphore < this._maxConcurrent) {
      this._activeSemaphore++;
      return;
    }
    return new Promise<void>((resolve) => {
      this._waitQueue.push(() => {
        this._activeSemaphore++;
        resolve();
      });
    });
  }

  private releaseSlot(): void {
    this._activeSemaphore--;
    const next = this._waitQueue.shift();
    if (next) next();
  }
}

/**
 * Parse a restricted JavaScript *data* literal — object / array / string /
 * number / `true` / `false` / `null` — WITHOUT executing it. Throws on any
 * identifier, call, operator, template interpolation, or other non-data
 * construct, and on malformed input.
 *
 * Workflow metadata is untrusted content discovered on disk. The previous
 * `new Function('return (' + literal + ')')` approach EXECUTED that content at
 * mere discovery/listing time — before {@link validateWorkflowScript} ever ran —
 * so a crafted `meta` block could run arbitrary code just by being parsed. This
 * parser accepts only pure data (parity with the reference loader, which
 * validates parsed metadata as data and never evals it).
 */
export function parseDataLiteral(src: string): unknown {
  let i = 0;
  const n = src.length;
  const err = (msg: string): never => { throw new Error(`parseDataLiteral: ${msg} at ${i}`); };

  const skipWs = (): void => {
    while (i < n) {
      const c = src[i]!;
      if (c === ' ' || c === '\t' || c === '\n' || c === '\r' || c === '\f' || c === '\v') { i++; continue; }
      break;
    }
  };

  const parseString = (quote: string): string => {
    let out = '';
    while (i < n) {
      const c = src[i++]!;
      if (c === '\\') {
        const e = src[i++];
        if (e === undefined) return err('unterminated escape');
        const map: Record<string, string> = { n: '\n', t: '\t', r: '\r', b: '\b', f: '\f', v: '\v', '0': '\0' };
        out += map[e] ?? e;
        continue;
      }
      if (quote === '`' && c === '$' && src[i] === '{') return err('template interpolation not allowed');
      if (c === quote) return out;
      out += c;
    }
    return err('unterminated string');
  };

  const parseNumber = (): number => {
    const start = i;
    if (src[i] === '-') i++;
    while (i < n && src[i]! >= '0' && src[i]! <= '9') i++;
    if (src[i] === '.') { i++; while (i < n && src[i]! >= '0' && src[i]! <= '9') i++; }
    if (src[i] === 'e' || src[i] === 'E') {
      i++;
      if (src[i] === '+' || src[i] === '-') i++;
      while (i < n && src[i]! >= '0' && src[i]! <= '9') i++;
    }
    const text = src.slice(start, i);
    const num = Number(text);
    if (text === '' || text === '-' || !Number.isFinite(num)) return err('invalid number');
    return num;
  };

  const parseValue = (): unknown => {
    skipWs();
    const c = src[i];
    if (c === undefined) return err('unexpected end of input');
    if (c === '{') return parseObject();
    if (c === '[') return parseArray();
    if (c === '"' || c === "'" || c === '`') { i++; return parseString(c); }
    if (c === '-' || c === '.' || (c >= '0' && c <= '9')) return parseNumber();
    if (src.startsWith('true', i)) { i += 4; return true; }
    if (src.startsWith('false', i)) { i += 5; return false; }
    if (src.startsWith('null', i)) { i += 4; return null; }
    return err(`unexpected token '${c}'`);
  };

  const parseKey = (): string => {
    skipWs();
    const c = src[i];
    if (c === '"' || c === "'" || c === '`') { i++; return parseString(c); }
    const start = i;
    while (i < n && /[A-Za-z0-9_$]/.test(src[i]!)) i++;
    if (i === start) return err('expected object key');
    return src.slice(start, i);
  };

  const parseObject = (): Record<string, unknown> => {
    i++; // consume '{'
    const obj: Record<string, unknown> = {};
    for (;;) {
      skipWs();
      if (src[i] === '}') { i++; return obj; } // empty object or trailing comma
      const key = parseKey();
      skipWs();
      if (src[i] !== ':') return err("expected ':'");
      i++;
      obj[key] = parseValue();
      skipWs();
      const d = src[i];
      if (d === ',') { i++; continue; }
      if (d === '}') { i++; return obj; }
      return err("expected ',' or '}'");
    }
  };

  const parseArray = (): unknown[] => {
    i++; // consume '['
    const arr: unknown[] = [];
    for (;;) {
      skipWs();
      if (src[i] === ']') { i++; return arr; } // empty array or trailing comma
      arr.push(parseValue());
      skipWs();
      const d = src[i];
      if (d === ',') { i++; continue; }
      if (d === ']') { i++; return arr; }
      return err("expected ',' or ']'");
    }
  };

  const value = parseValue();
  skipWs();
  if (i !== n) return err('trailing characters after value');
  return value;
}

export function parseWorkflowMeta(script: string): WorkflowMeta | null {
  const headerMatch = script.match(/export\s+const\s+meta\s*=\s*\{/);
  if (!headerMatch) return null;

  // Extract the meta object literal using brace counting
  let depth = 0;
  let inString = false;
  let stringChar = '';
  // A non-global .match() always populates .index when it matches.
  const braceStart = headerMatch.index! + headerMatch[0].length - 1;
  let braceEnd = braceStart;
  for (let i = braceStart; i < script.length; i++) {
    const ch = script[i];
    if (inString) {
      if (ch === stringChar && script[i - 1] !== '\\') inString = false;
      continue;
    }
    if (ch === '"' || ch === "'" || ch === '`') { inString = true; stringChar = ch; continue; }
    if (ch === '{') depth++;
    if (ch === '}') { depth--; if (depth === 0) { braceEnd = i; break; } }
  }
  if (depth !== 0) return null;

  const metaLiteral = script.slice(braceStart, braceEnd + 1);

  try {
    // Parse the metadata as DATA — never execute it (see parseDataLiteral). A
    // literal that is not a plain object, or that contains any non-data
    // construct, is rejected.
    const parsed = parseDataLiteral(metaLiteral);
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
    const obj = parsed as Record<string, unknown>;
    return {
      name: String(obj.name ?? ''),
      description: String(obj.description ?? ''),
      phases: Array.isArray(obj.phases) ? (obj.phases as WorkflowMeta['phases']) : undefined,
      whenToUse: obj.whenToUse ? String(obj.whenToUse) : undefined,
    };
  } catch {
    return null;
  }
}

/**
 * [FUNCTIONAL PARITY] Validate a (possibly JSON-string) agent output against a
 * JSON-Schema-like descriptor — parity with the reference schema-forced
 * StructuredOutput: an `agent(prompt, { schema })` call must return output that
 * conforms to the schema. The runtime parses + validates it (and, in
 * {@link WorkflowRuntime}, retries the agent on mismatch).
 *
 * Returns the parsed/validated value on success, or `undefined` when the value
 * does not satisfy the schema (the signal to retry or fail). Validation is
 * intentionally shallow — type of the root plus `required` top-level keys —
 * matching the lightweight guarantees the workflow runtime makes without pulling
 * in a full JSON-Schema engine.
 */
export function validateStructuredOutput(value: unknown, schema: Record<string, unknown>): unknown | undefined {
  let parsed: unknown = value;
  if (typeof value === 'string') {
    // Best-effort parse: a string that isn't JSON stays the raw string, so a
    // `string`/absent-descriptor schema still accepts it while object/array/
    // number/boolean schemas reject it on the per-type check below.
    try { parsed = JSON.parse(value); } catch { parsed = value; }
  }
  const declaredType = typeof schema.type === 'string' ? (schema.type as string) : undefined;
  const expectsObject = declaredType === 'object' || (declaredType === undefined && (schema.properties !== undefined || schema.required !== undefined));
  if (expectsObject) {
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) return undefined;
    const obj = parsed as Record<string, unknown>;
    const required = Array.isArray(schema.required) ? (schema.required as unknown[]) : [];
    for (const key of required) {
      if (typeof key === 'string' && !(key in obj)) return undefined;
    }
    return parsed;
  }
  if (declaredType === 'array') {
    return Array.isArray(parsed) ? parsed : undefined;
  }
  if (declaredType === 'string') {
    // For a string schema the raw (unparsed) value is the structured output.
    return typeof value === 'string' ? value : undefined;
  }
  if (declaredType === 'number' || declaredType === 'integer') {
    return typeof parsed === 'number' && Number.isFinite(parsed) ? parsed : undefined;
  }
  if (declaredType === 'boolean') {
    return typeof parsed === 'boolean' ? parsed : undefined;
  }
  // Unknown/absent descriptor: accept the parsed value as-is.
  return parsed;
}

/** Max attempts for a schema-forced agent() call (1 try + retries). */
export function schemaMaxAttempts(): number {
  return Math.max(1, Math.min(10, parseEnvInt(process.env.AMI_WORKFLOW_SCHEMA_MAX_ATTEMPTS, 3)));
}

/**
 * Scan a workflow script body for sandbox-escaping constructs:
 * dynamic import(), require(), and process/child_process access.
 * Throws if any are found.
 */
export function validateWorkflowScript(scriptBody: string): void {
  // Strip string literals and comments to avoid false positives
  const stripped = scriptBody
    .replace(/\/\/[^\n]*/g, '')              // line comments
    .replace(/\/\*[\s\S]*?\*\//g, '')        // block comments
    .replace(/`(?:[^`\\]|\\.)*`/g, '""')     // template literals
    .replace(/"(?:[^"\\]|\\.)*"/g, '""')     // double-quoted strings
    .replace(/'(?:[^'\\]|\\.)*'/g, "''");    // single-quoted strings

  // Dynamic import() — matches import( but not import.meta or static import from
  if (/\bimport\s*\(/g.test(stripped)) {
    throw new Error('Workflow scripts cannot use dynamic import() — this would escape the workflow sandbox');
  }
  // require() calls
  if (/\brequire\s*\(/g.test(stripped)) {
    throw new Error('Workflow scripts cannot use require() — this would escape the workflow sandbox');
  }
  // Direct process access for spawning — dot OR bracket notation
  // (process.exit, process['kill'], process["env"]).
  if (/\bprocess\s*(?:\.\s*(?:exit|kill|env)\b|\[)/g.test(stripped)) {
    throw new Error('Workflow scripts cannot access process.exit/kill/env — this would escape the workflow sandbox');
  }
  // globalThis / global are escape hatches back to Node built-ins
  // (globalThis.process, globalThis['require'], global.process, …).
  if (/\b(?:globalThis|global)\s*[.[]/g.test(stripped)) {
    throw new Error('Workflow scripts cannot access globalThis/global — this would escape the workflow sandbox');
  }
  // The Function constructor (Function('…'), or the classic
  // ([]).constructor.constructor('return process')() escape) rebuilds code in
  // the global scope, defeating every check above.
  if (/\bFunction\s*\(/g.test(stripped) || /\.\s*constructor\b/g.test(stripped)) {
    throw new Error('Workflow scripts cannot use the Function constructor — this would escape the workflow sandbox');
  }
  // [FUNCTIONAL PARITY] Ban non-deterministic global sources. A workflow must be
  // reproducible/resumable: a resumed run (and the agent-result cache keyed on
  // the prompt) assumes identical inputs produce identical work. Date.now() and
  // Math.random() make that false, so the reference workflow runtime forbids
  // them in script bodies. Matched in dot OR bracket notation
  // (Date.now / Date['now'], Math.random / Math['random']).
  // After literal-stripping, `Date['now']`/`Date["now"]`/`Date[`now`]` all become
  // `Date['']` or `Date[""]`, so one pattern catches dot + bracket forms.
  if (/\bDate\s*(?:\.\s*now\b|\[\s*(?:""|'')\s*\])/g.test(stripped)) {
    throw new Error('Workflow scripts cannot use Date.now() — it is non-deterministic and breaks resumable/reproducible execution');
  }
  if (/\bMath\s*(?:\.\s*random\b|\[\s*(?:""|'')\s*\])/g.test(stripped)) {
    throw new Error('Workflow scripts cannot use Math.random() — it is non-deterministic and breaks resumable/reproducible execution');
  }
}

export async function executeWorkflow(
  script: string,
  agentHandler: AgentHandler,
  options: {
    args?: unknown;
    budgetTotal?: number | null;
    abortSignal?: AbortSignal;
    workflowResolver?: WorkflowResolver | null;
    maxConcurrent?: number;
    cache?: WorkflowAgentCache | null;
    /**
     * [FUNCTIONAL PARITY] Surface the live {@link WorkflowRuntime} to the caller
     * the instant it is created (before the script runs) so a consumer can drive
     * per-agent recovery — {@link WorkflowRuntime.retryAgent} /
     * {@link WorkflowRuntime.skipAgent} — on a wedged agent mid-run, parity with
     * the reference workflow detail `onRetryAgent`/`onSkipAgent` actions.
     */
    onRuntime?: (runtime: WorkflowRuntime) => void;
  } = {},
): Promise<{ result: unknown; logs: string[]; agentCount: number }> {
  const scriptBody = stripMeta(script);
  validateWorkflowScript(scriptBody);

  const runtime = new WorkflowRuntime({
    agentHandler,
    budgetTotal: options.budgetTotal,
    workflowResolver: options.workflowResolver,
    maxConcurrent: options.maxConcurrent,
    cache: options.cache,
  });
  options.onRuntime?.(runtime);

  if (options.abortSignal) {
    options.abortSignal.addEventListener('abort', () => runtime.abort(), { once: true });
  }

  const ctx = runtime.createContext(options.args);

  const wrappedScript = `
    return (async function(agent, parallel, pipeline, phase, log, args, budget, saveCheckpoint, loadCheckpoint, workflow) {
      ${scriptBody}
    })
  `;

  const factory = new Function(wrappedScript)(); // eslint-disable-line no-new-func
  const result = await factory(
    ctx.agent,
    ctx.parallel,
    ctx.pipeline,
    ctx.phase,
    ctx.log,
    ctx.args,
    ctx.budget,
    ctx.saveCheckpoint,
    ctx.loadCheckpoint,
    ctx.workflow,
  );

  return {
    result: result ?? null,
    logs: runtime.logs,
    agentCount: runtime.agentCount,
  };
}
