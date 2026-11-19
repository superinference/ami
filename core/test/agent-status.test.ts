import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  type AgentStatus,
  AGENT_STATUS_RANK,
  AGENT_STATUS_ORDER,
  AGENT_STATUS_BUCKET,
  AGENT_STATUS_GLYPH,
  AGENT_ATTENTION_BUCKETS,
  agentStatusBucket,
  agentStatusGlyph,
  agentStatusNeedsYou,
  compareAgentStatus,
  assertUniqueAgentStatusRanks,
  subagentStatusToAgentStatus,
} from '../src/agent-status';

const ALL: AgentStatus[] = Object.keys(AGENT_STATUS_RANK) as AgentStatus[];

test('every status has a unique rank (total order)', () => {
  const ranks = ALL.map((s) => AGENT_STATUS_RANK[s]);
  assert.equal(new Set(ranks).size, ranks.length);
  assert.doesNotThrow(() => assertUniqueAgentStatusRanks());
});

test('assertUniqueAgentStatusRanks throws on a duplicated rank', () => {
  // Simulate a dev introducing a colliding rank by mutating a frozen-like copy via the checker.
  const original = AGENT_STATUS_RANK.idle;
  (AGENT_STATUS_RANK as Record<AgentStatus, number>).idle = AGENT_STATUS_RANK.done;
  try {
    assert.throws(() => assertUniqueAgentStatusRanks(), /duplicate rank/);
  } finally {
    (AGENT_STATUS_RANK as Record<AgentStatus, number>).idle = original;
  }
});

test('AGENT_STATUS_ORDER is sorted by rank, loudest first', () => {
  assert.deepEqual([...AGENT_STATUS_ORDER], [...ALL].sort((a, b) => AGENT_STATUS_RANK[a] - AGENT_STATUS_RANK[b]));
  assert.equal(AGENT_STATUS_ORDER[0], 'waiting');
  assert.equal(AGENT_STATUS_ORDER[AGENT_STATUS_ORDER.length - 1], 'idle');
});

test('compareAgentStatus implements the rank total order', () => {
  assert.ok(compareAgentStatus('waiting', 'idle') < 0);
  assert.ok(compareAgentStatus('done', 'working') > 0);
  assert.equal(compareAgentStatus('failed', 'failed'), 0);
  // sorting a shuffled list yields the canonical order
  const shuffled: AgentStatus[] = ['done', 'waiting', 'working', 'failed', 'idle'];
  shuffled.sort(compareAgentStatus);
  assert.deepEqual(shuffled, ['waiting', 'failed', 'working', 'done', 'idle']);
});

test('every status maps to exactly one of the four buckets', () => {
  for (const s of ALL) {
    assert.ok(AGENT_ATTENTION_BUCKETS.includes(agentStatusBucket(s)), `${s} -> ${agentStatusBucket(s)}`);
    assert.equal(agentStatusBucket(s), AGENT_STATUS_BUCKET[s]);
  }
});

test('bucket mapping matches Orca crewStatusGroup semantics', () => {
  assert.equal(agentStatusBucket('permission'), 'needsYou');
  assert.equal(agentStatusBucket('blocked'), 'needsYou');
  assert.equal(agentStatusBucket('waiting'), 'needsYou');
  assert.equal(agentStatusBucket('working'), 'working');
  assert.equal(agentStatusBucket('monitoring'), 'working');
  assert.equal(agentStatusBucket('failed'), 'failed');
  assert.equal(agentStatusBucket('done'), 'idle');
  assert.equal(agentStatusBucket('interrupted'), 'idle');
  assert.equal(agentStatusBucket('unverifiable'), 'idle');
  assert.equal(agentStatusBucket('unconfirmed'), 'idle');
});

test('agentStatusNeedsYou flags only the loudest bucket', () => {
  assert.ok(agentStatusNeedsYou('permission'));
  assert.ok(!agentStatusNeedsYou('working'));
  assert.ok(!agentStatusNeedsYou('done'));
});

test('missing-evidence states never borrow the done glyph', () => {
  const done = agentStatusGlyph('done');
  assert.equal(done.glyph, '✓');
  for (const s of ['unconfirmed', 'unverifiable'] as AgentStatus[]) {
    assert.notEqual(agentStatusGlyph(s).glyph, done.glyph);
    assert.equal(agentStatusGlyph(s).tone, 'warning');
  }
  // every status has a glyph + a valid tone
  for (const s of ALL) {
    const g = AGENT_STATUS_GLYPH[s];
    assert.ok(g.glyph.length >= 1);
    assert.ok(['attention', 'active', 'error', 'warning', 'muted'].includes(g.tone));
  }
});

test('subagentStatusToAgentStatus lifts the cli roster lifecycle with the right precedence', () => {
  // error outranks a nominally-successful terminal
  assert.equal(subagentStatusToAgentStatus('done', { hasError: true }), 'failed');
  assert.equal(subagentStatusToAgentStatus('failed'), 'failed');
  assert.equal(subagentStatusToAgentStatus('killed'), 'interrupted');
  assert.equal(subagentStatusToAgentStatus('done'), 'done');
  // live states + flags
  assert.equal(subagentStatusToAgentStatus('running', { awaitingApproval: true }), 'permission');
  assert.equal(subagentStatusToAgentStatus('running', { awaitingPlanApproval: true }), 'permission');
  assert.equal(subagentStatusToAgentStatus('running', { shutdownRequested: true }), 'monitoring');
  assert.equal(subagentStatusToAgentStatus('running', { isIdle: true }), 'idle');
  assert.equal(subagentStatusToAgentStatus('running'), 'working');
  assert.equal(subagentStatusToAgentStatus('pending'), 'working');
  // precedence: approval wait beats idle
  assert.equal(subagentStatusToAgentStatus('running', { awaitingApproval: true, isIdle: true }), 'permission');
});
