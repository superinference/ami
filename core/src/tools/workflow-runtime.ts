import { EventEmitter } from 'events';

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

export type AgentHandler = (prompt: string, opts?: AgentOptions) => Promise<unknown>;

export interface AgentOptions {
  label?: string;
  phase?: string;
  schema?: Record<string, unknown>;
  model?: string;
  isolation?: 'worktree';
  agentType?: string;
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

const MAX_CONCURRENT = Math.max(4, Math.min(16, (require('os').cpus()?.length ?? 8) - 2));
const MAX_AGENTS = 1000;
const MAX_PIPELINE_ITEMS = 4096;

export class WorkflowRuntime extends EventEmitter {
  private _currentPhase = '';
  private _agentCount = 0;
  private _activeSemaphore = 0;
  private _waitQueue: Array<() => void> = [];
  private _tokensSpent = 0;
  private _aborted = false;
  private readonly _agentHandler: AgentHandler;
  private readonly _budgetTotal: number | null;
  private readonly _logs: string[] = [];
  private readonly _phaseAgents = new Map<string, number>();
  private checkpoints: Map<string, unknown> = new Map();
  private readonly _workflowResolver: WorkflowResolver | null;
  private readonly _nestingDepth: number;

  constructor(options: {
    agentHandler: AgentHandler;
    budgetTotal?: number | null;
    workflowResolver?: WorkflowResolver | null;
    nestingDepth?: number;
  }) {
    super();
    this._agentHandler = options.agentHandler;
    this._budgetTotal = options.budgetTotal ?? null;
    this._workflowResolver = options.workflowResolver ?? null;
    this._nestingDepth = options.nestingDepth ?? 0;
  }

  get logs(): string[] { return [...this._logs]; }
  get agentCount(): number { return this._agentCount; }
  get currentPhase(): string { return this._currentPhase; }

  abort(): void {
    this._aborted = true;
    this.emit('abort');
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
        agentHandler: this._agentHandler,
        budgetTotal: this._budgetTotal,
        workflowResolver: null,
        nestingDepth: this._nestingDepth + 1,
      });

      if (this._aborted) childRuntime.abort();
      this.on('abort', () => childRuntime.abort());

      const childCtx = childRuntime.createContext(args);
      const wrappedScript = `
        return (async function(agent, parallel, pipeline, phase, log, args, budget, saveCheckpoint, loadCheckpoint, workflow) {
          ${script.replace(/export\s+const\s+meta\s*=\s*\{[\s\S]*?\n\}/, '')}
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

      this._agentCount++;
      const phase = opts?.phase ?? this._currentPhase;
      this._phaseAgents.set(phase, (this._phaseAgents.get(phase) ?? 0) + 1);

      await this.acquireSlot();
      try {
        this.emit('agent-start', {
          prompt: prompt.slice(0, 100),
          label: opts?.label,
          phase,
          agentNumber: this._agentCount,
        });

        const result = await this._agentHandler(prompt, opts);

        this.emit('agent-complete', {
          label: opts?.label,
          phase,
          agentNumber: this._agentCount,
        });

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

  private createParallel(): <T>(thunks: Array<() => Promise<T>>) => Promise<Array<T | null>> {
    return async <T>(thunks: Array<() => Promise<T>>): Promise<Array<T | null>> => {
      if (thunks.length > MAX_PIPELINE_ITEMS) {
        throw new Error(`parallel() limit exceeded: ${thunks.length} > ${MAX_PIPELINE_ITEMS}`);
      }

      const results = await Promise.all(
        thunks.map(async (thunk) => {
          try {
            return await thunk();
          } catch {
            return null;
          }
        }),
      );
      return results;
    };
  }

  private createPipeline(): <T>(items: T[], ...stages: Array<(prev: any, item: T, idx: number) => Promise<any>>) => Promise<any[]> {
    return async <T>(items: T[], ...stages: Array<(prev: any, item: T, idx: number) => Promise<any>>): Promise<any[]> => {
      if (items.length > MAX_PIPELINE_ITEMS) {
        throw new Error(`pipeline() limit exceeded: ${items.length} > ${MAX_PIPELINE_ITEMS}`);
      }

      const results = await Promise.all(
        items.map(async (item, idx) => {
          let prev: any = item;
          for (const stage of stages) {
            try {
              prev = await stage(prev, item, idx);
            } catch {
              return null;
            }
          }
          return prev;
        }),
      );
      return results;
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
    if (this._activeSemaphore < MAX_CONCURRENT) {
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

export function parseWorkflowMeta(script: string): WorkflowMeta | null {
  const metaMatch = script.match(/export\s+const\s+meta\s*=\s*(\{[\s\S]*?\n\})/);
  if (!metaMatch) return null;

  try {
    const factory = new Function(`return (${metaMatch[1]})`); // eslint-disable-line no-new-func
    const obj = factory();
    if (!obj || typeof obj !== 'object') return null;
    return {
      name: String(obj.name ?? ''),
      description: String(obj.description ?? ''),
      phases: Array.isArray(obj.phases) ? obj.phases : undefined,
      whenToUse: obj.whenToUse ? String(obj.whenToUse) : undefined,
    };
  } catch {
    return null;
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
  } = {},
): Promise<{ result: unknown; logs: string[]; agentCount: number }> {
  const runtime = new WorkflowRuntime({
    agentHandler,
    budgetTotal: options.budgetTotal,
    workflowResolver: options.workflowResolver,
  });

  if (options.abortSignal) {
    options.abortSignal.addEventListener('abort', () => runtime.abort(), { once: true });
  }

  const ctx = runtime.createContext(options.args);

  const wrappedScript = `
    return (async function(agent, parallel, pipeline, phase, log, args, budget, saveCheckpoint, loadCheckpoint, workflow) {
      ${script.replace(/export\s+const\s+meta\s*=\s*\{[\s\S]*?\n\}/, '')}
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
