/**
 * U006 (w96) — core/core/src/types.ts parity additions (building-block layer).
 *
 * Each block covers one confirmed parity gap targeting types.ts, as a self-contained
 * typed schema field + a pure (non-display) helper/resolver. Mirrors the established
 * U006 pattern (types-u006-w88.test.ts). Display/rendering stays in the cli layer;
 * these only derive/validate data.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { join } from 'node:path';
import {
  isInferenceConfigImmediate,
  resolveDiffPreviewWidth,
  VERBOSE_DIFF_PREVIEW_WIDTH,
  COMPACT_DIFF_PREVIEW_WIDTH,
  describeSymlinkEdit,
  shouldShowWorkerBadge,
  nextTaskBudgetRemaining,
  resolveNotificationTimeout,
  planNotificationQueue,
  DEFAULT_NOTIFICATION_TIMEOUT_MS,
} from '../src/types';
import type {
  InferenceConfigImmediacy,
  SymlinkEditDisclosure,
  WorkerAttribution,
  TaskBudget,
  ReplNotification,
  NotificationPriority,
} from '../src/types';

// ---------------------------------------------------------------------------
// Gap 1: immediate inference-config command gating
// (parity: utils/immediateCommand.ts shouldInferenceConfigCommandBeImmediate)
// ---------------------------------------------------------------------------
describe('isInferenceConfigImmediate', () => {
  it('defaults to false with no inputs', () => {
    assert.equal(isInferenceConfigImmediate(), false);
    assert.equal(isInferenceConfigImmediate({}), false);
  });
  it('a privileged user forces it on regardless of config', () => {
    assert.equal(isInferenceConfigImmediate({ privilegedUser: true, configEnabled: false }), true);
  });
  it('an enabled experiment forces it on regardless of config', () => {
    assert.equal(isInferenceConfigImmediate({ experimentEnabled: true, configEnabled: false }), true);
  });
  it('otherwise the persisted config toggle decides', () => {
    assert.equal(isInferenceConfigImmediate({ configEnabled: true }), true);
    assert.equal(isInferenceConfigImmediate({ configEnabled: false }), false);
  });
  it('only strict true enables (no truthy coercion)', () => {
    const r = { privilegedUser: 1 as unknown as boolean };
    assert.equal(isInferenceConfigImmediate(r as InferenceConfigImmediacy), false);
  });
});

// ---------------------------------------------------------------------------
// Gap 7: diff-preview width knob
// (parity: NotebookEditPermissionRequest `verbose ? 120 : 80`)
// ---------------------------------------------------------------------------
describe('resolveDiffPreviewWidth', () => {
  it('derives width from the verbose flag when unconfigured', () => {
    assert.equal(resolveDiffPreviewWidth(true), VERBOSE_DIFF_PREVIEW_WIDTH);
    assert.equal(resolveDiffPreviewWidth(false), COMPACT_DIFF_PREVIEW_WIDTH);
    assert.equal(VERBOSE_DIFF_PREVIEW_WIDTH, 120);
    assert.equal(COMPACT_DIFF_PREVIEW_WIDTH, 80);
  });
  it('an explicit positive config override wins over the verbose default', () => {
    assert.equal(resolveDiffPreviewWidth(false, { diffPreviewWidth: 200 }), 200);
    assert.equal(resolveDiffPreviewWidth(true, { diffPreviewWidth: 60 }), 60);
  });
  it('ignores a non-finite or non-positive override and falls back to verbose default', () => {
    assert.equal(resolveDiffPreviewWidth(true, { diffPreviewWidth: 0 }), VERBOSE_DIFF_PREVIEW_WIDTH);
    assert.equal(resolveDiffPreviewWidth(false, { diffPreviewWidth: -5 }), COMPACT_DIFF_PREVIEW_WIDTH);
    assert.equal(resolveDiffPreviewWidth(false, { diffPreviewWidth: NaN }), COMPACT_DIFF_PREVIEW_WIDTH);
  });
});

// ---------------------------------------------------------------------------
// Gap 10: symlink-target disclosure in the edit-approval prompt
// (parity: ShowInIDEPrompt.tsx symlinkTarget note)
// ---------------------------------------------------------------------------
describe('describeSymlinkEdit', () => {
  const cwd = '/home/u/project';
  it('returns undefined when there is no symlink target', () => {
    assert.equal(describeSymlinkEdit(join(cwd, 'a.ts'), undefined, cwd), undefined);
    assert.equal(describeSymlinkEdit(join(cwd, 'a.ts'), null, cwd), undefined);
    assert.equal(describeSymlinkEdit(join(cwd, 'a.ts'), '', cwd), undefined);
  });
  it('returns undefined when the target equals the edit path (not redirecting)', () => {
    const p = join(cwd, 'a.ts');
    assert.equal(describeSymlinkEdit(p, p, cwd), undefined);
  });
  it('discloses an in-workspace symlink target with the plain note', () => {
    const d = describeSymlinkEdit(join(cwd, 'link.ts'), join(cwd, 'src', 'real.ts'), cwd);
    assert.ok(d);
    const disc: SymlinkEditDisclosure = d;
    assert.equal(disc.outsideWorkspace, false);
    assert.equal(disc.target, join(cwd, 'src', 'real.ts'));
    assert.equal(disc.note, `Symlink target: ${join(cwd, 'src', 'real.ts')}`);
  });
  it('flags an out-of-workspace symlink target with the stronger note', () => {
    const target = '/etc/passwd';
    const d = describeSymlinkEdit(join(cwd, 'link.ts'), target, cwd);
    assert.ok(d);
    assert.equal(d!.outsideWorkspace, true);
    assert.equal(d!.note, `This will modify ${target} (outside working directory) via a symlink`);
  });
});

// ---------------------------------------------------------------------------
// Gap 8: worker/subagent attribution badge gate
// (parity: WorkerBadge.tsx — shown only for non-primary swarm workers)
// ---------------------------------------------------------------------------
describe('shouldShowWorkerBadge', () => {
  const agent: WorkerAttribution = { name: 'explorer', color: 'cyan' };
  it('shows the badge for a named non-primary subagent', () => {
    assert.equal(shouldShowWorkerBadge(agent, false), true);
  });
  it('never badges the primary session', () => {
    assert.equal(shouldShowWorkerBadge(agent, true), false);
  });
  it('does not show without a requesting agent or with a blank name', () => {
    assert.equal(shouldShowWorkerBadge(undefined, false), false);
    assert.equal(shouldShowWorkerBadge({ name: '   ' }, false), false);
  });
});

// ---------------------------------------------------------------------------
// Gap 19: per-turn taskBudget output-token countdown across compaction
// (parity: query.ts taskBudget remaining carryover)
// ---------------------------------------------------------------------------
describe('nextTaskBudgetRemaining', () => {
  const budget: TaskBudget = { total: 10_000 };
  it('subtracts from total on the first compaction', () => {
    assert.equal(nextTaskBudgetRemaining(budget, 3_000), 7_000);
  });
  it('subtracts from the prior remaining on subsequent compactions', () => {
    const r1 = nextTaskBudgetRemaining(budget, 3_000);
    assert.equal(nextTaskBudgetRemaining(budget, 2_000, r1), 5_000);
  });
  it('never goes negative', () => {
    assert.equal(nextTaskBudgetRemaining(budget, 50_000), 0);
    assert.equal(nextTaskBudgetRemaining(budget, 9_000, 1_000), 0);
  });
  it('clamps a negative pre-compact token count to zero (no budget refund)', () => {
    assert.equal(nextTaskBudgetRemaining(budget, -500, 4_000), 4_000);
  });
});

// ---------------------------------------------------------------------------
// Gap 14: in-REPL startup notification schema + queue planning
// (parity: context/notifications.tsx)
// ---------------------------------------------------------------------------
describe('ReplNotification queue', () => {
  const mk = (key: string, priority: NotificationPriority, extra: Partial<ReplNotification> = {}): ReplNotification => ({
    key, text: key, priority, ...extra,
  });
  it('resolves the timeout, defaulting to 8000ms', () => {
    assert.equal(DEFAULT_NOTIFICATION_TIMEOUT_MS, 8000);
    assert.equal(resolveNotificationTimeout({}), 8000);
    assert.equal(resolveNotificationTimeout({ timeoutMs: 1234 }), 1234);
  });
  it('appends a non-immediate notification to the back', () => {
    const q = planNotificationQueue([mk('a', 'low')], mk('b', 'medium'));
    assert.deepEqual(q.map((n) => n.key), ['a', 'b']);
  });
  it('an immediate notification jumps to the front', () => {
    const q = planNotificationQueue([mk('a', 'low')], mk('b', 'immediate'));
    assert.deepEqual(q.map((n) => n.key), ['b', 'a']);
  });
  it('folds (replaces) an existing entry with the same key', () => {
    const q = planNotificationQueue([mk('a', 'low', { text: 'old' })], mk('a', 'high', { text: 'new' }));
    assert.equal(q.length, 1);
    assert.equal(q[0].text, 'new');
    assert.equal(q[0].priority, 'high');
  });
  it('drops invalidated keys', () => {
    const q = planNotificationQueue(
      [mk('a', 'low'), mk('b', 'low'), mk('c', 'low')],
      mk('d', 'medium', { invalidates: ['a', 'c'] }),
    );
    assert.deepEqual(q.map((n) => n.key), ['b', 'd']);
  });
  it('does not mutate the existing queue', () => {
    const existing = [mk('a', 'low')];
    const copy = [...existing];
    planNotificationQueue(existing, mk('b', 'immediate', { invalidates: ['a'] }));
    assert.deepEqual(existing, copy);
  });
});
