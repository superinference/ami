import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  type AgentAttentionRequest,
  type AgentLivenessEvidence,
  AGENT_LIVENESS_STATES,
  AGENT_FINISH_DROP_CAUSES,
  DEFAULT_AGENT_STALE_AFTER_MS,
  isLivenessFresh,
  resolveDecayedLiveness,
  resolveAgentLiveness,
  resolveAgentAttention,
  isCoordinatorOnlyNews,
  shouldAdmitTurnBoundary,
} from '../src/attention-policy';

const base = (o: Partial<AgentAttentionRequest> = {}): AgentAttentionRequest => ({
  workspaceId: 'w1',
  subjectKey: 'a1',
  settlesTurn: true,
  hasFreshActivityEvidence: true,
  hasLiveSession: true,
  isMuted: false,
  isViewed: false,
  reason: 'agent-completion',
  ...o,
});

test('liveness vocabulary is exactly the three honest values', () => {
  assert.deepEqual([...AGENT_LIVENESS_STATES], ['live', 'unverifiable', 'settled']);
});

test('isLivenessFresh compares against the stale window', () => {
  assert.ok(isLivenessFresh(1000, 1000 + DEFAULT_AGENT_STALE_AFTER_MS - 1));
  assert.ok(!isLivenessFresh(1000, 1000 + DEFAULT_AGENT_STALE_AFTER_MS));
});

test('a fresh live-claim stays live', () => {
  const ev: AgentLivenessEvidence = { state: 'working', evidenceObservedAt: 1000, hasLiveSession: true };
  assert.deepEqual(resolveAgentLiveness(ev, 1500), { liveness: 'live', status: 'working' });
});

test('a stale live-claim with a live session decays to unverifiable, never a lying spinner', () => {
  const ev: AgentLivenessEvidence = { state: 'working', evidenceObservedAt: 0, hasLiveSession: true };
  const v = resolveAgentLiveness(ev, DEFAULT_AGENT_STALE_AFTER_MS + 1);
  assert.deepEqual(v, { liveness: 'unverifiable', status: 'unverifiable' });
});

test('a stale live-claim with no live session settles to idle', () => {
  const ev: AgentLivenessEvidence = { state: 'blocked', evidenceObservedAt: 0, hasLiveSession: false };
  const v = resolveAgentLiveness(ev, DEFAULT_AGENT_STALE_AFTER_MS + 1);
  assert.deepEqual(v, { liveness: 'settled', status: 'idle' });
});

test('a restored seed never claims unverifiable (no silence to report)', () => {
  const ev: AgentLivenessEvidence = {
    state: 'working',
    evidenceObservedAt: 0,
    hasLiveSession: true,
    restoredUnconfirmed: true,
  };
  assert.deepEqual(resolveAgentLiveness(ev, DEFAULT_AGENT_STALE_AFTER_MS + 1), {
    liveness: 'settled',
    status: 'idle',
  });
  assert.deepEqual(resolveDecayedLiveness({ state: 'working', restoredUnconfirmed: true }, true), {
    liveness: 'settled',
    status: 'idle',
  });
});

test('terminal outcomes are settled regardless of age', () => {
  for (const state of ['done', 'failed', 'interrupted'] as const) {
    const ev: AgentLivenessEvidence = { state, evidenceObservedAt: 0, hasLiveSession: true };
    assert.deepEqual(resolveAgentLiveness(ev, 10 ** 9), { liveness: 'settled', status: state });
  }
});

test('an already-ambiguous status stays unverifiable; idle stays settled', () => {
  assert.equal(resolveAgentLiveness({ state: 'unconfirmed', evidenceObservedAt: 0, hasLiveSession: false }, 1).liveness, 'unverifiable');
  assert.equal(resolveAgentLiveness({ state: 'idle', evidenceObservedAt: 0, hasLiveSession: false }, 1).liveness, 'settled');
});

test('attention: muted workspace drops with muted-workspace', () => {
  assert.deepEqual(resolveAgentAttention(base({ isMuted: true })), {
    admitted: false,
    cause: 'muted-workspace',
  });
});

test('attention: coordinator-only-news drops a dispatched worker', () => {
  assert.deepEqual(resolveAgentAttention(base({ coordinatorOnlyNews: true })), {
    admitted: false,
    cause: 'coordinator-only-news',
  });
});

test('attention: no live session and no fresh evidence drops', () => {
  assert.deepEqual(
    resolveAgentAttention(base({ hasLiveSession: false, hasFreshActivityEvidence: false })),
    { admitted: false, cause: 'no-live-session' },
  );
});

test('attention: fresh evidence admits even without a live session', () => {
  const d = resolveAgentAttention(base({ hasLiveSession: false, hasFreshActivityEvidence: true }));
  assert.ok(d.admitted);
});

test('attention: an unviewed turn-settle writes an unread marker', () => {
  const d = resolveAgentAttention(base());
  assert.ok(d.admitted);
  if (d.admitted) {
    assert.deepEqual(d.unread, { workspaceId: 'w1', subjectKey: 'a1', reason: 'agent-completion' });
    assert.deepEqual(d.delivery, { workspaceId: 'w1', subjectKey: 'a1' });
  }
});

test('attention: a viewed turn-settle delivers but writes no unread', () => {
  const d = resolveAgentAttention(base({ isViewed: true }));
  assert.ok(d.admitted);
  if (d.admitted) assert.equal(d.unread, null);
});

test('attention: a non-settling signal delivers but never marks unread', () => {
  const d = resolveAgentAttention(base({ settlesTurn: false }));
  assert.ok(d.admitted);
  if (d.admitted) assert.equal(d.unread, null);
});

test('isCoordinatorOnlyNews is true only below the top level', () => {
  assert.ok(!isCoordinatorOnlyNews(0));
  assert.ok(isCoordinatorOnlyNews(1));
  assert.ok(isCoordinatorOnlyNews(5));
});

test('shouldAdmitTurnBoundary admits a fresh marker and rejects a replay/blank', () => {
  const seen = new Set<string>(['t1']);
  assert.ok(shouldAdmitTurnBoundary('t2', seen));
  assert.ok(!shouldAdmitTurnBoundary('t1', seen));
  assert.ok(!shouldAdmitTurnBoundary('', seen));
});

test('finish-drop causes catalog includes the Orca causes plus AMI additions', () => {
  for (const c of ['coordinator-only-news', 'muted-workspace', 'no-live-session', 'viewed', 'host-replay-skipped']) {
    assert.ok(AGENT_FINISH_DROP_CAUSES.includes(c as never), c);
  }
  assert.ok(AGENT_FINISH_DROP_CAUSES.includes('replayed-turn'));
  assert.equal(new Set(AGENT_FINISH_DROP_CAUSES).size, AGENT_FINISH_DROP_CAUSES.length);
});
