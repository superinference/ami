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
  /** Whether this flow's completion has already been surfaced to the user. Lets a
   *  consumer notify on terminal transition exactly once without tracking seen-state
   *  externally (set via markNotified()). Undefined/false = not yet surfaced. */
  notified?: boolean;
  /** Wall-clock grace deadline: a terminal flow is retained (lingers in the inspector)
   *  until this time, then becomes eligible for time-based eviction (pruneExpired()).
   *  Mirrors Claude's per-task `evictAfter` (completion + PANEL_GRACE_MS) so a just-
   *  finished row stays readable briefly before vanishing. Set on terminalization and
   *  re-settable via scheduleEviction() (e.g. 0 = immediate dismiss). Undefined while
   *  active. */
  evictAfter?: number;
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
  // Flows the inspector is currently displaying — retained from eviction (see pinFlow).
  private pinnedFlowIds = new Set<string>();
  // The single flow the inspector is CURRENTLY drilled into (viewed), or undefined.
  // Managed via setViewedFlow() so enter/exit transitions pin + linger exactly once.
  private viewedFlowId?: string;

  private static readonly DEFAULT_KEY = '__default__';
  /** Cap on retained terminal (non-active) flows so they don't accumulate across a long session. */
  private static readonly MAX_RETAINED_TERMINAL = 20;
  /** Grace window (ms) a terminal flow lingers before it is eligible for time-based
   *  eviction. Mirrors Claude's framework `PANEL_GRACE_MS` so a freshly-finished row
   *  stays readable briefly instead of disappearing the instant it completes. */
  static readonly TERMINAL_GRACE_MS = 30_000;

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

  /** All flows, oldest-first (stable ordering for the inspector). Reading also sweeps
   *  terminal flows whose grace window has elapsed, so lingering rows disappear once
   *  the inspector next renders them (end-to-end wiring for the grace window). */
  listFlows(): FlowInfo[] {
    this.pruneExpired();
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
    // Start the linger grace window: the row stays readable until this deadline,
    // then pruneExpired()/listFlows() may sweep it (mirrors Claude's evictAfter).
    flow.evictAfter = flow.completedAt + FlowManager.TERMINAL_GRACE_MS;
    // A terminal flow stops being the active target for every key that points at it.
    for (const [key, active] of this.activeByKey) {
      if (active.flowId === flowId) this.activeByKey.delete(key);
    }
  }

  /** FUNCTIONAL: Pin a flow so pruneTerminal() never evicts it — e.g. the TUI
   *  inspector is currently displaying it, so it must not vanish out from under the
   *  view. Pinning wins over the retention cap: a pinned terminal flow is retained
   *  even beyond MAX_RETAINED_TERMINAL. */
  pinFlow(flowId: string): void {
    this.pinnedFlowIds.add(flowId);
  }

  /** Release a pin so the flow becomes eligible for eviction again. */
  unpinFlow(flowId: string): void {
    this.pinnedFlowIds.delete(flowId);
  }

  /** FUNCTIONAL: Declare which flow the TUI inspector is CURRENTLY drilled into (viewed),
   *  or `undefined` when no flow view is open. This is the single enter/exit entry point
   *  the inspector calls on every drill change; it mirrors Claude's
   *  enterTeammateView/exitTeammateView retain+evictAfter special-casing:
   *   - Entering a flow PINS it (pruneTerminal/pruneExpired can never delete it out from
   *     under the view) and CLEARS any pending linger deadline (`evictAfter`) so a flow
   *     that terminated while being inspected won't be swept.
   *   - Moving the view away (to another flow or to `undefined`) RELEASES the previously
   *     viewed flow: it is unpinned and, if terminal, given a fresh grace window via
   *     scheduleEviction() so the row LINGERS briefly instead of vanishing immediately.
   *     (scheduleEviction() no-ops on active/unknown flows, so a still-active viewed flow
   *     is simply unpinned.)
   *  Idempotent: re-declaring the same viewed flow is a no-op. Returns the id of the flow
   *  released by this transition (if any), else undefined. */
  setViewedFlow(flowId: string | undefined): string | undefined {
    if (flowId === this.viewedFlowId) return undefined;
    const released = this.viewedFlowId;
    if (released !== undefined) {
      this.unpinFlow(released);
      // Terminal → linger briefly before eviction; active/unknown → no-op.
      this.scheduleEviction(released);
    }
    if (flowId !== undefined) {
      this.pinFlow(flowId);
      // Entering a view blocks eviction: drop any linger deadline already scheduled.
      const flow = this.flows.get(flowId);
      if (flow) flow.evictAfter = undefined;
    }
    this.viewedFlowId = flowId;
    return released;
  }

  /** Whether a flow is currently pinned (retained from eviction). */
  isPinned(flowId: string): boolean {
    return this.pinnedFlowIds.has(flowId);
  }

  /** FUNCTIONAL: Mark a flow's completion as surfaced to the user. Returns true
   *  only on the FIRST transition to notified so a consumer can notify exactly once
   *  (dedupe); subsequent calls — and unknown flows — return false. */
  markNotified(flowId: string): boolean {
    const flow = this.flows.get(flowId);
    if (!flow || flow.notified) return false;
    flow.notified = true;
    return true;
  }

  /** Terminal flows whose completion has not yet been surfaced, oldest-FINISHED
   *  first. A consumer lists these, notifies each, then markNotified()s it. */
  listUnnotifiedTerminalFlows(): FlowInfo[] {
    return Array.from(this.flows.values())
      .filter(f => f.status !== 'active' && !f.notified)
      .sort((a, b) => (a.completedAt ?? a.createdAt) - (b.completedAt ?? b.createdAt));
  }

  /** FUNCTIONAL: (Re)set a terminal flow's linger grace deadline. `graceMs` ms from
   *  now (default TERMINAL_GRACE_MS); pass 0 to dismiss immediately (mirrors Claude's
   *  stopOrDismissAgent evictAfter=0). Only terminal flows can be scheduled — active
   *  and unknown flows return false and are untouched. */
  scheduleEviction(flowId: string, graceMs: number = FlowManager.TERMINAL_GRACE_MS): boolean {
    const flow = this.flows.get(flowId);
    if (!flow || flow.status === 'active') return false;
    flow.evictAfter = Date.now() + Math.max(0, graceMs);
    return true;
  }

  /** FUNCTIONAL: Evict terminal, unpinned flows whose grace deadline has passed
   *  (`evictAfter <= now`). Flows still within their grace window, pinned flows (the
   *  inspector is viewing them), active flows, and terminal flows without a deadline
   *  are all retained. Returns the ids actually evicted. Invoked on every listFlows()
   *  and callable directly on a timer tick. */
  pruneExpired(now: number = Date.now()): string[] {
    const evicted: string[] = [];
    for (const flow of this.flows.values()) {
      if (flow.status === 'active') continue;
      if (flow.evictAfter === undefined || flow.evictAfter > now) continue;
      if (this.pinnedFlowIds.has(flow.flowId)) continue;
      this.flows.delete(flow.flowId);
      evicted.push(flow.flowId);
    }
    return evicted;
  }

  reset(): void {
    this.flows.clear();
    this.activeByKey.clear();
    this.pinnedFlowIds.clear();
    this.viewedFlowId = undefined;
  }

  /** Evict terminal (non-active) flows beyond the retention cap, oldest-FINISHED
   *  first. Ordering by completion time (falling back to createdAt) keeps the
   *  most recently finished flows visible longest. Pinned flows (the inspector is
   *  viewing them) are skipped and retained even past the cap. */
  private pruneTerminal(): void {
    const terminal = Array.from(this.flows.values())
      .filter(f => f.status !== 'active')
      .sort((a, b) => (a.completedAt ?? a.createdAt) - (b.completedAt ?? b.createdAt));
    let excess = terminal.length - FlowManager.MAX_RETAINED_TERMINAL;
    if (excess <= 0) return;
    for (const f of terminal) {
      if (excess <= 0) break;
      if (this.pinnedFlowIds.has(f.flowId)) continue;
      this.flows.delete(f.flowId);
      excess--;
    }
  }
}
