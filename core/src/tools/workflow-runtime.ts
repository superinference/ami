import { EventEmitter } from 'events';
import * as fs from 'fs';
import * as os from 'os';
import 'path';
import 'crypto';
import { parseEnvInt } from '../utils/env';
import '../conversation-summary';
import type { PermissionMode } from '../permissions';
import { planWaves, computeUnitSignature, computeRunId, computeResumePlan } from '../task';
import { MAX_AGENTS, MAX_PIPELINE_ITEMS, mapWithConcurrency, stripMeta, workflowAgentCacheKey } from './workflow-runtime-parse-data-literal';
import type { AgentHandler, WaveManifest, WaveUnit, WorkflowAgentCache, WorkflowAgentReporter, WorkflowContext, WorkflowResolver } from './workflow-runtime-parse-data-literal';
import { schemaMaxAttempts, validateStructuredOutput } from './workflow-runtime-validate-structured-output';
export { DiskWorkflowCache, formatAgentCompletion, mapWithConcurrency, normalizeAgentModel, parseDataLiteral, resolveToolScope, stripMeta, workflowAgentCacheKey } from './workflow-runtime-parse-data-literal';
export type { AgentHandler, AgentResult, WaveUnit, WorkflowAgentCache, WorkflowAgentReporter, WorkflowBudget, WorkflowContext, WorkflowMeta, WorkflowResolver } from './workflow-runtime-parse-data-literal';
export { parseWorkflowMeta, schemaMaxAttempts, validateStructuredOutput } from './workflow-runtime-validate-structured-output';

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
      wave: this.createWave(),
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
        return (async function(agent, parallel, pipeline, wave, phase, log, args, budget, saveCheckpoint, loadCheckpoint, workflow) {
          ${childBody}
        })
      `;
      const factory = new Function(wrappedScript)(); // eslint-disable-line no-new-func
      const result = await factory(
        childCtx.agent, childCtx.parallel, childCtx.pipeline, childCtx.wave, childCtx.phase,
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

  /**
   * Build the `wave()` capability: file-disjoint wave planning + serial
   * integration + prefix-resume from the configured cache. See
   * {@link WorkflowContext.wave}.
   */
  private createWave(): <T>(units: WaveUnit[], run: (unit: WaveUnit) => Promise<T>) => Promise<Array<T | null>> {
    const cache = this._cache;
    const cap = this._maxConcurrent;
    return async <T>(units: WaveUnit[], run: (unit: WaveUnit) => Promise<T>): Promise<Array<T | null>> => {
      if (units.length > MAX_PIPELINE_ITEMS) {
        throw new Error(`wave() limit exceeded: ${units.length} > ${MAX_PIPELINE_ITEMS}`);
      }

      // Stable per-unit signatures + run id drive the resume comparison.
      const sigById = new Map<string, string>();
      for (const u of units) sigById.set(u.id, computeUnitSignature(u));
      const resumeUnits = units.map((u) => ({ id: u.id, signature: sigById.get(u.id)! }));
      const runId = computeRunId(units);
      const manifestKey = `wave:${runId}`;

      // Longest unchanged prefix from a prior run is replayed from cache.
      const prior = cache ? (cache.get(manifestKey) as WaveManifest | undefined) : undefined;
      const plan = computeResumePlan(prior ?? null, resumeUnits);

      const resultById = new Map<string, unknown>();
      const completed: Record<string, string> = {};
      const results: Record<string, unknown> = {};
      for (const id of plan.replay) {
        // plan.replay is non-empty only when `prior` matched this run id.
        const value = prior!.results[id];
        resultById.set(id, value);
        completed[id] = sigById.get(id)!;
        results[id] = value;
      }
      const persist = (): void => {
        if (cache) cache.set(manifestKey, { runId, completed: { ...completed }, results: { ...results } });
      };

      const rerunSet = new Set(plan.rerun);
      const rerunUnits = units.filter((u) => rerunSet.has(u.id));
      this.emit('wave-start', { runId, total: units.length, replayed: plan.replay.length, rerun: rerunUnits.length });
      this.addLog(`wave ${runId.slice(0, 8)}: ${units.length} units (${plan.replay.length} replayed, ${rerunUnits.length} to run)`);

      const wavePlan = planWaves(rerunUnits);
      const byId = new Map(units.map((u) => [u.id, u]));

      let waveIndex = 0;
      for (const laneIds of wavePlan.waves) {
        if (this._aborted) throw new Error('Workflow aborted');
        const lanes = laneIds.map((id) => byId.get(id)!);
        const settled = await mapWithConcurrency(lanes, cap, async (unit) => {
          try { return { id: unit.id, ok: true as const, value: await run(unit) }; }
          catch (error) { return { id: unit.id, ok: false as const, error }; }
        });
        const failed: string[] = [];
        for (const s of settled) {
          if (s.ok) {
            resultById.set(s.id, s.value);
            completed[s.id] = sigById.get(s.id)!;
            results[s.id] = s.value;
          } else {
            failed.push(s.id);
          }
        }
        // Serial integration point: persist progress after each wave so a crash
        // between waves resumes from exactly here on the next run.
        persist();
        this.emit('wave-complete', { runId, wave: waveIndex, lanes: laneIds.length, failed: failed.length });
        waveIndex++;
        if (failed.length > 0) {
          throw new Error(`wave() failed in wave ${waveIndex - 1}: ${failed.join(', ')}`);
        }
      }

      return units.map((u) => (resultById.has(u.id) ? (resultById.get(u.id) as T) : null));
    };
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
    return (async function(agent, parallel, pipeline, wave, phase, log, args, budget, saveCheckpoint, loadCheckpoint, workflow) {
      ${scriptBody}
    })
  `;

  const factory = new Function(wrappedScript)(); // eslint-disable-line no-new-func
  const result = await factory(
    ctx.agent,
    ctx.parallel,
    ctx.pipeline,
    ctx.wave,
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
