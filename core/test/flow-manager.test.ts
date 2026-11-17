import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { FlowManager, FlowInfo } from '../src/flow-manager';

describe('FlowManager', () => {
  let fm: FlowManager;

  beforeEach(() => {
    fm = new FlowManager();
  });

  describe('getOrCreateActiveFlow', () => {
    it('creates a new flow with flow- prefix', () => {
      const flowId = fm.getOrCreateActiveFlow('test flow');
      assert.ok(flowId.startsWith('flow-'), 'flow ID must start with flow-');
    });

    it('returns same flow within time window', () => {
      const id1 = fm.getOrCreateActiveFlow('first');
      const id2 = fm.getOrCreateActiveFlow('second');
      assert.equal(id1, id2, 'must return same flow within window');
    });

    it('creates new flow after window expires', async () => {
      const id1 = fm.getOrCreateActiveFlow('first', 10);
      await new Promise(r => setTimeout(r, 20));
      const id2 = fm.getOrCreateActiveFlow('second', 10);
      assert.notEqual(id1, id2, 'must create new flow after window');
    });

    it('stores description from first call', () => {
      const flowId = fm.getOrCreateActiveFlow('my description');
      const flow = fm.getFlow(flowId);
      assert.equal(flow?.description, 'my description');
    });

    it('sets status to active on creation', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      const flow = fm.getFlow(flowId);
      assert.equal(flow?.status, 'active');
    });

    it('sets createdAt timestamp', () => {
      const before = Date.now();
      const flowId = fm.getOrCreateActiveFlow('test');
      const after = Date.now();
      const flow = fm.getFlow(flowId);
      assert.ok(flow!.createdAt >= before && flow!.createdAt <= after);
    });

    it('initializes empty agentIds array', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      const flow = fm.getFlow(flowId);
      assert.deepEqual(flow?.agentIds, []);
    });
  });

  describe('addAgent', () => {
    it('adds agent to existing flow', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      const result = fm.addAgent(flowId, 'agent-abc');
      assert.equal(result, true);
      assert.deepEqual(fm.getFlow(flowId)?.agentIds, ['agent-abc']);
    });

    it('adds multiple agents', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      fm.addAgent(flowId, 'agent-1');
      fm.addAgent(flowId, 'agent-2');
      fm.addAgent(flowId, 'agent-3');
      assert.deepEqual(fm.getFlow(flowId)?.agentIds, ['agent-1', 'agent-2', 'agent-3']);
    });

    it('returns false for non-existent flow', () => {
      const result = fm.addAgent('flow-nonexistent', 'agent-abc');
      assert.equal(result, false);
    });

    it('rejects a non-agent-id string (teammate name / free text) without mutating the flow', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      // A space-containing teammate name is NOT a well-formed agent id (toAgentId → null).
      const result = fm.addAgent(flowId, 'my teammate');
      assert.equal(result, false, 'addAgent must reject a non-agent-id string');
      assert.deepEqual(fm.getFlow(flowId)?.agentIds, [], 'rejected id must not be tracked');
    });

    it('rejects empty and leading-dash ids (malformed agent ids)', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      assert.equal(fm.addAgent(flowId, ''), false, 'empty id rejected');
      assert.equal(fm.addAgent(flowId, '-leading-dash'), false, 'leading-dash id rejected');
      assert.deepEqual(fm.getFlow(flowId)?.agentIds, []);
    });

    it('accepts the agent-<label>-<hex> ids real spawns produce', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      assert.equal(fm.addAgent(flowId, 'agent-worker-0123456789abcdef'), true);
      assert.equal(fm.addAgent(flowId, 'bg-0123456789abcdef'), true);
      assert.deepEqual(
        fm.getFlow(flowId)?.agentIds,
        ['agent-worker-0123456789abcdef', 'bg-0123456789abcdef'],
      );
    });
  });

  describe('getFlow', () => {
    it('returns flow info for existing flow', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      const flow = fm.getFlow(flowId);
      assert.ok(flow);
      assert.equal(flow.flowId, flowId);
    });

    it('returns undefined for non-existent flow', () => {
      assert.equal(fm.getFlow('flow-nonexistent'), undefined);
    });
  });

  describe('listFlows', () => {
    it('returns empty array initially', () => {
      assert.deepEqual(fm.listFlows(), []);
    });

    it('returns all flows', async () => {
      fm.getOrCreateActiveFlow('flow-a', 10);
      await new Promise(r => setTimeout(r, 20));
      fm.getOrCreateActiveFlow('flow-b', 10);
      const flows = fm.listFlows();
      assert.equal(flows.length, 2);
    });

    it('returns copies that include all fields', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      fm.addAgent(flowId, 'agent-1');
      const flows = fm.listFlows();
      assert.equal(flows.length, 1);
      const flow = flows[0];
      assert.equal(flow.flowId, flowId);
      assert.equal(flow.description, 'test');
      assert.deepEqual(flow.agentIds, ['agent-1']);
      assert.equal(flow.status, 'active');
      assert.ok(typeof flow.createdAt === 'number');
    });
  });

  describe('updateFlowStatus', () => {
    it('sets completed when all agents done successfully', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      fm.addAgent(flowId, 'agent-1');
      fm.addAgent(flowId, 'agent-2');
      fm.updateFlowStatus(flowId, (id) => 'completed');
      assert.equal(fm.getFlow(flowId)?.status, 'completed');
    });

    it('records completedAt when a flow reaches a terminal status', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      fm.addAgent(flowId, 'agent-1');
      assert.equal(fm.getFlow(flowId)?.completedAt, undefined, 'active flow has no completedAt');
      const before = Date.now();
      fm.updateFlowStatus(flowId, () => 'completed');
      const completedAt = fm.getFlow(flowId)?.completedAt;
      assert.ok(typeof completedAt === 'number' && completedAt >= before, 'completedAt set on terminal');
    });

    it('sets partial when any agent failed', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      fm.addAgent(flowId, 'agent-1');
      fm.addAgent(flowId, 'agent-2');
      const statuses: Record<string, string> = { 'agent-1': 'completed', 'agent-2': 'failed' };
      fm.updateFlowStatus(flowId, (id) => statuses[id]);
      assert.equal(fm.getFlow(flowId)?.status, 'partial');
    });

    it('sets partial when any agent killed', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      fm.addAgent(flowId, 'agent-1');
      fm.addAgent(flowId, 'agent-2');
      const statuses: Record<string, string> = { 'agent-1': 'completed', 'agent-2': 'killed' };
      fm.updateFlowStatus(flowId, (id) => statuses[id]);
      assert.equal(fm.getFlow(flowId)?.status, 'partial');
    });

    it('stays active when any agent still running', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      fm.addAgent(flowId, 'agent-1');
      fm.addAgent(flowId, 'agent-2');
      const statuses: Record<string, string> = { 'agent-1': 'completed', 'agent-2': 'running' };
      fm.updateFlowStatus(flowId, (id) => statuses[id]);
      assert.equal(fm.getFlow(flowId)?.status, 'active');
    });

    it('clears activeFlowId when flow completes', async () => {
      const flowId = fm.getOrCreateActiveFlow('test', 10);
      fm.addAgent(flowId, 'agent-1');
      fm.updateFlowStatus(flowId, () => 'completed');
      await new Promise(r => setTimeout(r, 20));
      const newFlowId = fm.getOrCreateActiveFlow('new', 10);
      assert.notEqual(flowId, newFlowId, 'completed flow must not be reused');
    });

    it('no-ops for non-existent flow', () => {
      fm.updateFlowStatus('flow-nonexistent', () => 'completed');
    });

    it('no-ops for flow with no agents', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      fm.updateFlowStatus(flowId, () => 'completed');
      assert.equal(fm.getFlow(flowId)?.status, 'active');
    });

    it('keeps the flow ACTIVE while an agent is unresolved (unknown), never prematurely sealed', () => {
      // An unresolved lookup (evicted/orphaned → undefined ⇒ 'unknown') is NON-terminal:
      // Claude's isTerminalTaskStatus treats ONLY completed/failed/killed as terminal, so
      // an unknown agent must not fold the flow into 'completed'. It stays active and can
      // reopen once the agent resolves.
      const flowId = fm.getOrCreateActiveFlow('test', 10);
      fm.addAgent(flowId, 'agent-1');
      fm.updateFlowStatus(flowId, () => undefined);
      assert.equal(fm.getFlow(flowId)?.status, 'active', 'unknown keeps the flow active');
      assert.equal(fm.getFlow(flowId)?.completedAt, undefined, 'no terminal timestamp recorded');
    });

    it('keeps the flow ACTIVE while an agent is still PENDING (spawned, not yet started)', () => {
      // 'pending' is active (Claude's isBackgroundTask counts running AND pending as
      // in-flight); a pending agent must not resolve its group early.
      const flowId = fm.getOrCreateActiveFlow('test', 10);
      fm.addAgent(flowId, 'agent-1');
      fm.addAgent(flowId, 'agent-2');
      const statuses: Record<string, string> = { 'agent-1': 'completed', 'agent-2': 'pending' };
      fm.updateFlowStatus(flowId, id => statuses[id]);
      assert.equal(fm.getFlow(flowId)?.status, 'active', 'pending agent keeps the flow active');
    });

    it('a pending agent that later resolves lets the flow seal (reopened, not permanently sealed)', () => {
      const flowId = fm.getOrCreateActiveFlow('test', 10);
      fm.addAgent(flowId, 'agent-1');
      const live: Record<string, string> = { 'agent-1': 'pending' };
      fm.updateFlowStatus(flowId, id => live[id]);
      assert.equal(fm.getFlow(flowId)?.status, 'active', 'still pending → active');
      live['agent-1'] = 'completed';
      fm.updateFlowStatus(flowId, id => live[id]);
      assert.equal(fm.getFlow(flowId)?.status, 'completed', 'seals once the pending agent finishes');
    });

    it('seals only when EVERY agent is terminal (completed/failed/killed)', () => {
      const flowId = fm.getOrCreateActiveFlow('test', 10);
      fm.addAgent(flowId, 'agent-1');
      fm.addAgent(flowId, 'agent-2');
      fm.addAgent(flowId, 'agent-3');
      const statuses: Record<string, string> = { 'agent-1': 'completed', 'agent-2': 'failed', 'agent-3': 'killed' };
      fm.updateFlowStatus(flowId, id => statuses[id]);
      assert.equal(fm.getFlow(flowId)?.status, 'partial', 'all-terminal mix resolves (partial here)');
    });
  });

  describe('reset', () => {
    it('clears all flows', () => {
      fm.getOrCreateActiveFlow('test');
      fm.reset();
      assert.deepEqual(fm.listFlows(), []);
    });

    it('clears active flow so next call creates new', () => {
      const id1 = fm.getOrCreateActiveFlow('test');
      fm.reset();
      const id2 = fm.getOrCreateActiveFlow('test');
      assert.notEqual(id1, id2);
    });

    it('resets lastSpawnTime', async () => {
      fm.getOrCreateActiveFlow('test', 100000);
      fm.reset();
      await new Promise(r => setTimeout(r, 5));
      const id = fm.getOrCreateActiveFlow('new', 1);
      assert.ok(fm.getFlow(id), 'must create new flow after reset');
    });
  });

  describe('concurrent flows (per-key)', () => {
    it('keeps two flows active at once under distinct keys', () => {
      const a = fm.getOrCreateActiveFlow('batch A', 5000, 'turn-1');
      const b = fm.getOrCreateActiveFlow('batch B', 5000, 'turn-2');
      assert.notEqual(a, b, 'distinct keys must not collapse into one flow');
      // Both stay independently active (each key reuses its own flow within window).
      assert.equal(fm.getOrCreateActiveFlow('batch A', 5000, 'turn-1'), a);
      assert.equal(fm.getOrCreateActiveFlow('batch B', 5000, 'turn-2'), b);
      assert.equal(fm.listFlows().length, 2);
    });

    it('same key within window groups; omitted key shares the default key', () => {
      const a1 = fm.getOrCreateActiveFlow('x', 5000, 'k');
      const a2 = fm.getOrCreateActiveFlow('x', 5000, 'k');
      assert.equal(a1, a2);
      const d1 = fm.getOrCreateActiveFlow('d');
      const d2 = fm.getOrCreateActiveFlow('d');
      assert.equal(d1, d2, 'default-key spawns group together (back-compat)');
      assert.notEqual(a1, d1, 'default key is distinct from an explicit key');
    });
  });

  describe('createFlow (explicit)', () => {
    it('creates a keyless flow that no windowed spawn reuses', () => {
      const explicit = fm.createFlow('workflow run');
      const windowed = fm.getOrCreateActiveFlow('other');
      assert.notEqual(explicit, windowed, 'keyless explicit flow is never the active target');
      assert.equal(fm.getFlow(explicit)?.status, 'active');
      assert.equal(fm.getFlow(explicit)?.description, 'workflow run');
    });

    it('a keyed createFlow becomes the active flow for that key', () => {
      const created = fm.createFlow('named', 'wf-1');
      const reused = fm.getOrCreateActiveFlow('named', 5000, 'wf-1');
      assert.equal(created, reused);
    });
  });

  describe('updateFlowStatus — failed is reachable', () => {
    it('sets failed when ALL agents failed or were killed', () => {
      const flowId = fm.getOrCreateActiveFlow('doomed');
      fm.addAgent(flowId, 'agent-1');
      fm.addAgent(flowId, 'agent-2');
      const statuses: Record<string, string> = { 'agent-1': 'failed', 'agent-2': 'killed' };
      fm.updateFlowStatus(flowId, id => statuses[id]);
      assert.equal(fm.getFlow(flowId)?.status, 'failed');
    });

    it('a completed flow stops being reused for ITS key only', () => {
      const a = fm.getOrCreateActiveFlow('a', 5000, 'k1');
      const b = fm.getOrCreateActiveFlow('b', 5000, 'k2');
      fm.addAgent(a, 'x');
      fm.updateFlowStatus(a, () => 'completed');
      // k1 reuse must create a new flow; k2 is untouched.
      assert.notEqual(fm.getOrCreateActiveFlow('a2', 5000, 'k1'), a);
      assert.equal(fm.getOrCreateActiveFlow('b2', 5000, 'k2'), b);
    });
  });

  describe('listFlows ordering + retention', () => {
    it('returns flows oldest-first', () => {
      const ids: string[] = [];
      for (let i = 0; i < 3; i++) ids.push(fm.createFlow(`f${i}`));
      const listed = fm.listFlows().map(f => f.flowId);
      assert.deepEqual(listed, ids, 'stable createdAt ordering');
    });

    it('prunes oldest terminal flows beyond the retention cap but keeps active ones', () => {
      const keptActive = fm.createFlow('still-active'); // never terminalized
      for (let i = 0; i < 30; i++) {
        const id = fm.createFlow(`term-${i}`);
        fm.addAgent(id, `a${i}`);
        fm.updateFlowStatus(id, () => 'completed');
      }
      const flows = fm.listFlows();
      const terminal = flows.filter(f => f.status !== 'active');
      assert.ok(terminal.length <= 21, `terminal flows bounded, got ${terminal.length}`);
      assert.ok(flows.some(f => f.flowId === keptActive), 'active flow is never pruned');
      assert.ok(flows.length < 31, 'pruning actually happened');
    });

    it('evicts the oldest-FINISHED terminal flow (by completedAt, not createdAt)', () => {
      const ids: string[] = [];
      for (let i = 0; i < 21; i++) {
        const id = fm.createFlow(`t${i}`);
        fm.addAgent(id, `a${i}`);
        fm.updateFlowStatus(id, () => 'completed');
        ids.push(id);
      }
      // Give every flow a distinct completion time; make ids[5] finish FIRST even
      // though it was created in the middle (so createdAt != completedAt order).
      ids.forEach((id, i) => { const f = fm.getFlow(id); if (f) f.completedAt = 1000 + i; });
      const victim = fm.getFlow(ids[5])!;
      victim.completedAt = 1;
      // 21 terminal, cap 20 → one eviction, which must be the oldest-finished.
      (fm as unknown as { pruneTerminal(): void }).pruneTerminal();
      assert.equal(fm.getFlow(ids[5]), undefined, 'oldest-finished flow evicted');
      assert.ok(fm.getFlow(ids[0]), 'a later-finished (older created) flow survives');
      assert.ok(fm.getFlow(ids[20]), 'newest-finished flow survives');
    });
  });

  describe('pin/retain (eviction special-casing)', () => {
    it('pinFlow retains a terminal flow that would otherwise be evicted', () => {
      const ids: string[] = [];
      for (let i = 0; i < 21; i++) {
        const id = fm.createFlow(`t${i}`);
        fm.addAgent(id, `a${i}`);
        fm.updateFlowStatus(id, () => 'completed');
        ids.push(id);
      }
      // Give distinct completion times; make ids[0] the oldest-finished (first victim).
      ids.forEach((id, i) => { const f = fm.getFlow(id); if (f) f.completedAt = 1000 + i; });
      // Pin the would-be victim so it survives; the next-oldest is evicted instead.
      fm.pinFlow(ids[0]);
      assert.equal(fm.isPinned(ids[0]), true);
      (fm as unknown as { pruneTerminal(): void }).pruneTerminal();
      assert.ok(fm.getFlow(ids[0]), 'pinned oldest-finished flow is retained');
      assert.equal(fm.getFlow(ids[1]), undefined, 'next-oldest unpinned flow evicted instead');
    });

    it('retention wins over the cap: all-pinned terminal flows are kept beyond MAX', () => {
      const ids: string[] = [];
      for (let i = 0; i < 25; i++) {
        const id = fm.createFlow(`p${i}`);
        fm.addAgent(id, `a${i}`);
        fm.updateFlowStatus(id, () => 'completed');
        fm.pinFlow(id);
        ids.push(id);
      }
      (fm as unknown as { pruneTerminal(): void }).pruneTerminal();
      // Every flow is pinned → none evictable → all retained even past the cap of 20.
      assert.equal(fm.listFlows().length, 25, 'pinned flows retained beyond the cap');
    });

    it('unpinFlow re-enables eviction for a previously pinned flow', () => {
      const ids: string[] = [];
      for (let i = 0; i < 21; i++) {
        const id = fm.createFlow(`u${i}`);
        fm.addAgent(id, `a${i}`);
        fm.updateFlowStatus(id, () => 'completed');
        ids.push(id);
      }
      ids.forEach((id, i) => { const f = fm.getFlow(id); if (f) f.completedAt = 1000 + i; });
      fm.pinFlow(ids[0]);
      fm.unpinFlow(ids[0]);
      assert.equal(fm.isPinned(ids[0]), false);
      (fm as unknown as { pruneTerminal(): void }).pruneTerminal();
      assert.equal(fm.getFlow(ids[0]), undefined, 'unpinned oldest-finished flow evicted again');
    });

    it('reset clears pins', () => {
      const id = fm.createFlow('x');
      fm.pinFlow(id);
      fm.reset();
      assert.equal(fm.isPinned(id), false, 'pins cleared on reset');
    });

    it('isPinned is false for never-pinned flows', () => {
      const id = fm.createFlow('x');
      assert.equal(fm.isPinned(id), false);
    });
  });

  describe('notified flag (completion-surfaced dedupe)', () => {
    it('active flows have no notified flag', () => {
      const id = fm.getOrCreateActiveFlow('test');
      assert.equal(fm.getFlow(id)?.notified, undefined);
    });

    it('markNotified returns true once then false (dedupe)', () => {
      const id = fm.createFlow('t');
      fm.addAgent(id, 'a');
      fm.updateFlowStatus(id, () => 'completed');
      assert.equal(fm.markNotified(id), true, 'first mark reports a new transition');
      assert.equal(fm.getFlow(id)?.notified, true);
      assert.equal(fm.markNotified(id), false, 'second mark is deduped');
    });

    it('markNotified returns false for unknown flows', () => {
      assert.equal(fm.markNotified('flow-nope'), false);
    });

    it('listUnnotifiedTerminalFlows returns terminal + unnotified, oldest-finished first', () => {
      const a = fm.createFlow('a'); fm.addAgent(a, 'a1'); fm.updateFlowStatus(a, () => 'completed');
      const b = fm.createFlow('b'); fm.addAgent(b, 'b1'); fm.updateFlowStatus(b, () => 'completed');
      const active = fm.createFlow('still-active'); // excluded (active)
      fm.getFlow(a)!.completedAt = 200;
      fm.getFlow(b)!.completedAt = 100;
      let pending = fm.listUnnotifiedTerminalFlows().map(f => f.flowId);
      assert.deepEqual(pending, [b, a], 'oldest-finished first, active excluded');
      assert.ok(!pending.includes(active));
      fm.markNotified(b);
      pending = fm.listUnnotifiedTerminalFlows().map(f => f.flowId);
      assert.deepEqual(pending, [a], 'notified flow dropped from the pending set');
    });
  });

  describe('grace-window eviction (evictAfter / linger)', () => {
    it('terminalizing a flow sets evictAfter to completedAt + grace window', () => {
      const id = fm.createFlow('t');
      fm.addAgent(id, 'a');
      assert.equal(fm.getFlow(id)?.evictAfter, undefined, 'active flow has no evictAfter');
      fm.updateFlowStatus(id, () => 'completed');
      const flow = fm.getFlow(id)!;
      assert.ok(typeof flow.completedAt === 'number');
      assert.equal(flow.evictAfter, flow.completedAt! + FlowManager.TERMINAL_GRACE_MS,
        'evictAfter is the completion time plus the grace window');
    });

    it('pruneExpired evicts terminal flows past their grace deadline, keeps those still within it', () => {
      const expired = fm.createFlow('expired');
      fm.addAgent(expired, 'a'); fm.updateFlowStatus(expired, () => 'completed');
      const lingering = fm.createFlow('lingering');
      fm.addAgent(lingering, 'b'); fm.updateFlowStatus(lingering, () => 'completed');
      // Force the first one's deadline into the past; leave the second in its window.
      fm.getFlow(expired)!.evictAfter = 100;
      const evicted = fm.pruneExpired(1000);
      assert.deepEqual(evicted, [expired], 'returns the ids it evicted');
      assert.equal(fm.getFlow(expired), undefined, 'expired flow removed');
      assert.ok(fm.getFlow(lingering), 'within-grace flow lingers');
    });

    it('pruneExpired never evicts a pinned flow even past its deadline', () => {
      const id = fm.createFlow('pinned');
      fm.addAgent(id, 'a'); fm.updateFlowStatus(id, () => 'completed');
      fm.getFlow(id)!.evictAfter = 1;
      fm.pinFlow(id);
      const evicted = fm.pruneExpired(1000);
      assert.deepEqual(evicted, [], 'pinned flow is retained');
      assert.ok(fm.getFlow(id), 'pinned flow survives grace expiry');
    });

    it('pruneExpired ignores active flows and flows with no deadline', () => {
      const active = fm.createFlow('active');
      const terminalNoDeadline = fm.createFlow('term');
      fm.addAgent(terminalNoDeadline, 'a'); fm.updateFlowStatus(terminalNoDeadline, () => 'completed');
      fm.getFlow(terminalNoDeadline)!.evictAfter = undefined;
      const evicted = fm.pruneExpired(Number.MAX_SAFE_INTEGER);
      assert.deepEqual(evicted, [], 'nothing without a passed deadline is evicted');
      assert.ok(fm.getFlow(active));
      assert.ok(fm.getFlow(terminalNoDeadline));
    });

    it('scheduleEviction(id, 0) makes a terminal flow immediately evictable', () => {
      const id = fm.createFlow('dismiss');
      fm.addAgent(id, 'a'); fm.updateFlowStatus(id, () => 'completed');
      const now = Date.now();
      assert.equal(fm.scheduleEviction(id, 0), true, 'returns true for a terminal flow');
      assert.ok(fm.getFlow(id)!.evictAfter! <= now, 'deadline is now (immediate dismiss)');
      assert.deepEqual(fm.pruneExpired(now + 1), [id], 'immediately eligible for eviction');
    });

    it('scheduleEviction defaults to the grace window and rejects active/unknown flows', () => {
      const active = fm.createFlow('active');
      assert.equal(fm.scheduleEviction(active), false, 'cannot schedule an active flow');
      assert.equal(fm.scheduleEviction('flow-nope'), false, 'unknown flow rejected');
      const id = fm.createFlow('t');
      fm.addAgent(id, 'a'); fm.updateFlowStatus(id, () => 'completed');
      const before = Date.now();
      assert.equal(fm.scheduleEviction(id), true);
      assert.ok(fm.getFlow(id)!.evictAfter! >= before + FlowManager.TERMINAL_GRACE_MS,
        'default grace applied');
    });

    it('listFlows auto-sweeps grace-expired terminal flows (end-to-end wiring)', () => {
      const gone = fm.createFlow('gone');
      fm.addAgent(gone, 'a'); fm.updateFlowStatus(gone, () => 'completed');
      const kept = fm.createFlow('kept');
      fm.getFlow(gone)!.evictAfter = 1; // deadline in the distant past
      const listed = fm.listFlows().map(f => f.flowId);
      assert.ok(!listed.includes(gone), 'expired terminal flow swept on list');
      assert.ok(listed.includes(kept), 'active flow retained');
      assert.equal(fm.getFlow(gone), undefined, 'actually removed from the store');
    });
  });

  describe('integration: time-window grouping', () => {
    it('groups rapid spawns into one flow', () => {
      const flowId = fm.getOrCreateActiveFlow('batch');
      fm.addAgent(flowId, 'agent-1');
      const sameFlowId = fm.getOrCreateActiveFlow('batch');
      fm.addAgent(sameFlowId, 'agent-2');
      assert.equal(flowId, sameFlowId);
      assert.deepEqual(fm.getFlow(flowId)?.agentIds, ['agent-1', 'agent-2']);
    });

    it('separate flows for separate batches', async () => {
      const id1 = fm.getOrCreateActiveFlow('batch-1', 10);
      fm.addAgent(id1, 'agent-1');
      await new Promise(r => setTimeout(r, 20));
      const id2 = fm.getOrCreateActiveFlow('batch-2', 10);
      fm.addAgent(id2, 'agent-2');
      assert.notEqual(id1, id2);
      assert.deepEqual(fm.getFlow(id1)?.agentIds, ['agent-1']);
      assert.deepEqual(fm.getFlow(id2)?.agentIds, ['agent-2']);
    });

    it('flow lifecycle: active → completed', () => {
      const flowId = fm.getOrCreateActiveFlow('lifecycle');
      fm.addAgent(flowId, 'agent-1');
      fm.addAgent(flowId, 'agent-2');
      assert.equal(fm.getFlow(flowId)?.status, 'active');
      fm.updateFlowStatus(flowId, (id) => id === 'agent-1' ? 'completed' : 'running');
      assert.equal(fm.getFlow(flowId)?.status, 'active');
      fm.updateFlowStatus(flowId, () => 'completed');
      assert.equal(fm.getFlow(flowId)?.status, 'completed');
    });
  });

  describe('setViewedFlow (inspector view retain + linger)', () => {
    const terminate = (id: string) => { fm.addAgent(id, 'a-' + id); fm.updateFlowStatus(id, () => 'completed'); };

    it('entering a flow view pins it and clears any pending linger deadline', () => {
      const id = fm.createFlow('v');
      terminate(id);
      assert.ok(fm.getFlow(id)!.evictAfter !== undefined, 'terminalized flow has a grace deadline');
      const released = fm.setViewedFlow(id);
      assert.equal(released, undefined, 'nothing was previously viewed');
      assert.equal(fm.isPinned(id), true, 'viewed flow is pinned');
      assert.equal(fm.getFlow(id)!.evictAfter, undefined, 'linger deadline cleared while viewed');
    });

    it('a pinned viewed terminal flow survives pruneExpired even past a deadline', () => {
      const id = fm.createFlow('v');
      terminate(id);
      fm.setViewedFlow(id);
      // Force a stale deadline; it must still be retained because it is the viewed flow.
      fm.getFlow(id)!.evictAfter = 1;
      assert.deepEqual(fm.pruneExpired(Number.MAX_SAFE_INTEGER), [], 'viewed flow not evicted');
      assert.ok(fm.getFlow(id), 'viewed flow retained');
    });

    it('moving the view away releases the previous terminal flow and lets it linger', () => {
      const a = fm.createFlow('a'); terminate(a);
      fm.setViewedFlow(a);
      const before = Date.now();
      const released = fm.setViewedFlow(undefined);
      assert.equal(released, a, 'returns the released flow id');
      assert.equal(fm.isPinned(a), false, 'released flow is unpinned');
      assert.ok(fm.getFlow(a)!.evictAfter! >= before + FlowManager.TERMINAL_GRACE_MS,
        'released terminal flow gets a fresh grace window (lingers)');
      // After the grace window it becomes evictable.
      assert.deepEqual(fm.pruneExpired(fm.getFlow(a)!.evictAfter! + 1), [a], 'evicted once grace elapses');
    });

    it('switching between two flows releases the first and pins the second', () => {
      const a = fm.createFlow('a'); terminate(a);
      const b = fm.createFlow('b'); terminate(b);
      fm.setViewedFlow(a);
      const released = fm.setViewedFlow(b);
      assert.equal(released, a);
      assert.equal(fm.isPinned(a), false, 'first flow released');
      assert.equal(fm.isPinned(b), true, 'second flow pinned');
      assert.equal(fm.getFlow(b)!.evictAfter, undefined, 'second flow linger cleared');
    });

    it('re-viewing the same flow is an idempotent no-op', () => {
      const a = fm.createFlow('a'); terminate(a);
      fm.setViewedFlow(a);
      fm.getFlow(a)!.evictAfter = 12345; // would normally be cleared on enter
      assert.equal(fm.setViewedFlow(a), undefined, 'no release on re-view');
      assert.equal(fm.getFlow(a)!.evictAfter, 12345, 'no-op: deadline untouched');
      assert.equal(fm.isPinned(a), true);
    });

    it('releasing an active viewed flow just unpins it (no eviction scheduled)', () => {
      const a = fm.createFlow('a'); // still active (no agents terminalized)
      fm.setViewedFlow(a);
      assert.equal(fm.isPinned(a), true);
      fm.setViewedFlow(undefined);
      assert.equal(fm.isPinned(a), false, 'unpinned');
      assert.equal(fm.getFlow(a)!.evictAfter, undefined, 'active flow gets no grace deadline');
    });

    it('setViewedFlow(undefined) with nothing viewed is a no-op', () => {
      assert.equal(fm.setViewedFlow(undefined), undefined);
    });

    it('reset clears the viewed flow so a later release targets nothing', () => {
      const a = fm.createFlow('a'); terminate(a);
      fm.setViewedFlow(a);
      fm.reset();
      // After reset there is no viewed flow; declaring one does not try to release 'a'.
      const b = fm.createFlow('b'); terminate(b);
      assert.equal(fm.setViewedFlow(b), undefined, 'no stale previous view after reset');
    });
  });

  describe('summarize (centralized count-aware, failure-aware flow label)', () => {
    it('reports completed/total for a clean (no-failure) flow', () => {
      assert.equal(FlowManager.summarize(['completed', 'running', 'running']), '1/3');
      assert.equal(FlowManager.summarize(['completed', 'completed']), '2/2');
    });

    it('appends a · K failed segment when any agent failed or was killed', () => {
      assert.equal(FlowManager.summarize(['completed', 'failed']), '1/2 · 1 failed');
      assert.equal(FlowManager.summarize(['completed', 'failed', 'killed']), '1/3 · 2 failed');
    });

    it('omits the failed segment entirely at zero failures (no look change for clean flows)', () => {
      const label = FlowManager.summarize(['completed', 'running']);
      assert.ok(!label.includes('failed'), 'clean flow keeps the plain fraction');
    });

    it('counts unknown/undefined toward the total but neither completed nor failed', () => {
      assert.equal(FlowManager.summarize(['completed', undefined, 'unknown']), '1/3');
    });

    it('handles the empty set', () => {
      assert.equal(FlowManager.summarize([]), '0/0');
    });
  });

  describe('needsAttention (flow CTA / attention affordance)', () => {
    it('is true when an agent awaits input (needs_input) or a plan (plan_ready)', () => {
      assert.equal(FlowManager.needsAttention(['working', 'needs_input']), true);
      assert.equal(FlowManager.needsAttention(['working', 'plan_ready']), true);
    });

    it('is false when every agent is merely working (parity: pillNeedsCta skips working)', () => {
      assert.equal(FlowManager.needsAttention(['working', 'working']), false);
    });

    it('ignores undefined/unknown attention states', () => {
      assert.equal(FlowManager.needsAttention([undefined, 'working', 'completed']), false);
    });

    it('is false for an empty flow', () => {
      assert.equal(FlowManager.needsAttention([]), false);
    });
  });
});
