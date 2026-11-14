import * as crypto from 'crypto';

export interface FlowInfo {
  flowId: string;
  description: string;
  agentIds: string[];
  status: 'active' | 'completed' | 'partial' | 'failed';
  createdAt: number;
  /** Wall-clock time the flow reached a terminal status (completed/partial/failed).
   *  Undefined while active. Drives duration display and terminal-flow eviction
   *  order (evict by when work FINISHED, not when it started). */
  completedAt?: number;
}

/**
 * Groups spawned agents into "flows" (a flow = one or many agents working toward
 * one piece of work) for display in the TUI inspector.
 *
 * Concurrency: a flow is keyed by an opaque `flowKey` so SEVERAL flows can be
 * active at once (e.g. two independent background batches, or a workflow run that
 * owns its own flow). Within one key, spawns that land inside `windowMs` of each
 * other join the same active flow (a burst of parallel background agents); a gap
 * larger than the window starts a fresh flow. Callers that omit `flowKey` share a
 * single default key — identical to the original single-active-flow behavior.
 */
export class FlowManager {
  private flows = new Map<string, FlowInfo>();
  // One active flow per key → supports multiple concurrent flows.
  private activeByKey = new Map<string, { flowId: string; lastSpawnTime: number }>();

  private static readonly DEFAULT_KEY = '__default__';
  /** Cap on retained terminal (non-active) flows so they don't accumulate across a long session. */
  private static readonly MAX_RETAINED_TERMINAL = 20;

  getOrCreateActiveFlow(description: string, windowMs = 5000, flowKey: string = FlowManager.DEFAULT_KEY): string {
    const now = Date.now();
    const active = this.activeByKey.get(flowKey);
    if (active && this.flows.has(active.flowId) && (now - active.lastSpawnTime) < windowMs) {
      active.lastSpawnTime = now;
      return active.flowId;
    }
    return this._create(description, flowKey, now);
  }

  /**
   * Explicitly create a standalone flow (e.g. a workflow run or a user-spawned
   * flow). When `flowKey` is given the flow becomes the active flow for that key
   * so subsequent windowed spawns join it; otherwise it is a keyless flow that no
   * windowed spawn will ever reuse.
   */
  createFlow(description: string, flowKey?: string): string {
    return this._create(description, flowKey, Date.now());
  }

  private _create(description: string, flowKey: string | undefined, now: number): string {
    const flowId = `flow-${crypto.randomBytes(4).toString('hex')}`;
    this.flows.set(flowId, {
      flowId,
      description,
      agentIds: [],
      status: 'active',
      createdAt: now,
    });
    if (flowKey !== undefined) {
      this.activeByKey.set(flowKey, { flowId, lastSpawnTime: now });
    }
    this.pruneTerminal();
    return flowId;
  }

  addAgent(flowId: string, agentId: string): boolean {
    const flow = this.flows.get(flowId);
    if (!flow) return false;
    flow.agentIds.push(agentId);
    return true;
  }

  getFlow(flowId: string): FlowInfo | undefined {
    return this.flows.get(flowId);
  }

  /** All flows, oldest-first (stable ordering for the inspector). */
  listFlows(): FlowInfo[] {
    return Array.from(this.flows.values()).sort((a, b) => a.createdAt - b.createdAt);
  }

  updateFlowStatus(flowId: string, getAgentStatus: (agentId: string) => string | undefined): void {
    const flow = this.flows.get(flowId);
    if (!flow || flow.agentIds.length === 0) return;
    const statuses = flow.agentIds.map(id => getAgentStatus(id) || 'unknown');
    const allDone = statuses.every(s => s !== 'running');
    if (!allDone) return;
    const isFail = (s: string) => s === 'failed' || s === 'killed';
    const allFailed = statuses.every(isFail);
    const anyFailed = statuses.some(isFail);
    flow.status = allFailed ? 'failed' : anyFailed ? 'partial' : 'completed';
    flow.completedAt = Date.now();
    // A terminal flow stops being the active target for every key that points at it.
    for (const [key, active] of this.activeByKey) {
      if (active.flowId === flowId) this.activeByKey.delete(key);
    }
  }

  reset(): void {
    this.flows.clear();
    this.activeByKey.clear();
  }

  /** Evict terminal (non-active) flows beyond the retention cap, oldest-FINISHED
   *  first. Ordering by completion time (falling back to createdAt) keeps the
   *  most recently finished flows visible longest. */
  private pruneTerminal(): void {
    const terminal = Array.from(this.flows.values())
      .filter(f => f.status !== 'active')
      .sort((a, b) => (a.completedAt ?? a.createdAt) - (b.completedAt ?? b.createdAt));
    const excess = terminal.length - FlowManager.MAX_RETAINED_TERMINAL;
    for (let i = 0; i < excess; i++) {
      this.flows.delete(terminal[i].flowId);
    }
  }
}
