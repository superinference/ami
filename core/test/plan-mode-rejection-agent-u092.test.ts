/**
 * U092 — plan-mode parity primitives:
 *  1. `planFileAgentSuffix(agentId?)` — per-subagent plan file path suffix
 *     (parity: the reference's getPlanFilePath(agentId) → `{slug}-agent-{agentId}.md`,
 *     utils/plans.ts L113-129).
 *  2. `PlanRejectionTracker` — rejectCount / skip-already-rejected state
 *     (parity: ExitPlanModeScanner, utils/ultraplan/ccrSession.ts L80-181).
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  planFileAgentSuffix,
  PlanRejectionTracker,
} from '../src/tools/plan-mode';

describe('planFileAgentSuffix — per-subagent plan file path', () => {
  it('returns an empty suffix for the main conversation (no agentId)', () => {
    assert.equal(planFileAgentSuffix(), '');
    assert.equal(planFileAgentSuffix(undefined), '');
    assert.equal(planFileAgentSuffix(null), '');
    assert.equal(planFileAgentSuffix(''), '');
  });

  it('derives a distinct `-agent-<id>` suffix for a subagent', () => {
    assert.equal(planFileAgentSuffix('abc123'), '-agent-abc123');
    assert.equal(planFileAgentSuffix('A1_b-2'), '-agent-A1_b-2');
  });

  it('two distinct agents get distinct plan file names for the same slug', () => {
    const slug = 'brave-river-moon';
    const a = `${slug}${planFileAgentSuffix('agent-1')}.md`;
    const b = `${slug}${planFileAgentSuffix('agent-2')}.md`;
    const main = `${slug}${planFileAgentSuffix()}.md`;
    assert.notEqual(a, b);
    assert.notEqual(a, main);
    assert.equal(main, 'brave-river-moon.md');
  });

  it('sanitizes a crafted id so it cannot inject a path separator or traversal', () => {
    assert.equal(planFileAgentSuffix('../../etc/passwd'), '-agent-------etc-passwd');
    assert.equal(planFileAgentSuffix('a/b\\c'), '-agent-a-b-c');
    // The suffix never contains a path separator or `..` segment.
    const suffix = planFileAgentSuffix('..\\../x');
    assert.ok(!suffix.includes('/'));
    assert.ok(!suffix.includes('\\'));
  });
});

describe('PlanRejectionTracker — rejectCount + skip-already-rejected', () => {
  it('starts at zero rejections', () => {
    const t = new PlanRejectionTracker();
    assert.equal(t.rejectCount, 0);
    assert.equal(t.wasRejected('anything'), false);
  });

  it('increments rejectCount on each recorded rejection', () => {
    const t = new PlanRejectionTracker();
    t.recordRejection('plan A');
    assert.equal(t.rejectCount, 1);
    t.recordRejection('plan B');
    assert.equal(t.rejectCount, 2);
  });

  it('detects a plan resubmitted unchanged after a rejection', () => {
    const t = new PlanRejectionTracker();
    assert.equal(t.wasRejected('do X then Y'), false);
    t.recordRejection('do X then Y');
    assert.equal(t.wasRejected('do X then Y'), true);
    // Trailing/leading whitespace is normalized away — same plan.
    assert.equal(t.wasRejected('  do X then Y\n'), true);
    // A genuinely different plan is not flagged.
    assert.equal(t.wasRejected('do X then Z'), false);
  });

  it('still counts a re-rejected identical plan (iterate-until-approved count)', () => {
    const t = new PlanRejectionTracker();
    t.recordRejection('same plan');
    t.recordRejection('same plan');
    assert.equal(t.rejectCount, 2);
  });

  it('normalizes null/undefined/empty plan content to one key', () => {
    const t = new PlanRejectionTracker();
    t.recordRejection(null);
    assert.equal(t.wasRejected(undefined), true);
    assert.equal(t.wasRejected(''), true);
    assert.equal(t.wasRejected('   '), true);
  });

  it('reset() clears both the count and the rejected set', () => {
    const t = new PlanRejectionTracker();
    t.recordRejection('plan A');
    t.recordRejection('plan B');
    t.reset();
    assert.equal(t.rejectCount, 0);
    assert.equal(t.wasRejected('plan A'), false);
    assert.equal(t.wasRejected('plan B'), false);
  });
});
