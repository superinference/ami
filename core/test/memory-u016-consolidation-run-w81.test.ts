import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  CONSOLIDATION_TASK_TYPE,
  CONSOLIDATION_DEBUG_COMPONENT,
  consolidationLockPath,
  tryAcquireConsolidationLock,
  rollbackConsolidationLock,
  recordConsolidation,
  readLastConsolidatedAt,
  beginConsolidationRun,
  abortConsolidationRun,
  isDreamTask,
  type ConsolidationRunParams,
} from '../src/memory-consolidation';

/**
 * U239 / wave-81 — background memory-consolidation ("dream") RUN descriptor and
 * namespaced debug introspection. Parity with the reference's
 * tasks/DreamTask/DreamTask.ts (registerDreamTask opts:
 * sessionsReviewing / priorMtime / abortController; type:'dream' discriminator;
 * isDreamTask guard) and services/autoDream/consolidationLock.ts ([autoDream]-
 * namespaced human-readable skip/delay diagnostics).
 */

let tmpDir: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mem-w81-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('ConsolidationRunParams + begin/abort lifecycle (gap: DreamTask opts schema)', () => {
  it('beginConsolidationRun on a fresh dir returns a typed dream descriptor', () => {
    const run = beginConsolidationRun({ lockDir: tmpDir, sessionsReviewing: 7 });
    assert.ok(run, 'expected a run descriptor on a free lock');
    assert.equal(run.type, CONSOLIDATION_TASK_TYPE);
    assert.equal(run.type, 'dream');
    assert.equal(run.sessionsReviewing, 7);
    assert.equal(run.priorMtime, 0, 'no prior lock → priorMtime 0');
    assert.ok(run.abortController instanceof AbortController);
    assert.equal(run.abortController.signal.aborted, false);
    // Lock was actually acquired (PID written).
    assert.equal(fs.readFileSync(consolidationLockPath(tmpDir), 'utf-8').trim(), String(process.pid));
  });

  it('clamps sessionsReviewing to a non-negative integer', () => {
    const neg = beginConsolidationRun({ lockDir: path.join(tmpDir, 'a'), sessionsReviewing: -3 });
    assert.equal(neg?.sessionsReviewing, 0);
    const frac = beginConsolidationRun({ lockDir: path.join(tmpDir, 'b'), sessionsReviewing: 4.9 });
    assert.equal(frac?.sessionsReviewing, 4);
  });

  it('uses a caller-supplied AbortController when provided', () => {
    const ac = new AbortController();
    const run = beginConsolidationRun({ lockDir: tmpDir, sessionsReviewing: 1, abortController: ac });
    assert.equal(run?.abortController, ac);
  });

  it('returns null when the lock is held by a live, fresh holder', () => {
    // Seed a lock owned by THIS (live) process with a fresh mtime.
    fs.writeFileSync(consolidationLockPath(tmpDir), String(process.pid));
    const run = beginConsolidationRun({ lockDir: tmpDir, sessionsReviewing: 2 });
    assert.equal(run, null, 'must not double-launch while a live holder holds the lock');
  });

  it('abortConsolidationRun aborts the controller and rewinds the lock (priorMtime 0 → unlink)', () => {
    const run = beginConsolidationRun({ lockDir: tmpDir, sessionsReviewing: 3 });
    assert.ok(run);
    assert.equal(run.priorMtime, 0);
    assert.equal(fs.existsSync(consolidationLockPath(tmpDir)), true);
    abortConsolidationRun(tmpDir, run);
    assert.equal(run.abortController.signal.aborted, true, 'controller must be aborted');
    assert.equal(fs.existsSync(consolidationLockPath(tmpDir)), false, 'priorMtime 0 → lock removed');
  });

  it('abortConsolidationRun rewinds mtime to the pre-acquire value for an existing lock', () => {
    // Seed an OLD, dead-holder lock so it is reclaimable and priorMtime is non-zero.
    const p = consolidationLockPath(tmpDir);
    fs.writeFileSync(p, '999999999'); // non-live PID → reclaimable
    const oldMs = Date.now() - 30 * 60 * 1000; // 30m ago (< HOLDER_STALE_MS so freshness matters, but dead PID → reclaim)
    fs.utimesSync(p, oldMs / 1000, oldMs / 1000);
    const priorMtimeBefore = readLastConsolidatedAt(tmpDir);

    const run = beginConsolidationRun({ lockDir: tmpDir, sessionsReviewing: 1 });
    assert.ok(run);
    assert.ok(Math.abs(run.priorMtime - priorMtimeBefore) < 1000, 'priorMtime should capture the pre-acquire mtime');

    abortConsolidationRun(tmpDir, run);
    const afterAbort = readLastConsolidatedAt(tmpDir);
    assert.ok(
      Math.abs(afterAbort - run.priorMtime) < 1000,
      `expected mtime rewound to priorMtime (~${run.priorMtime}), got ${afterAbort}`,
    );
  });
});

describe('isDreamTask guard + task-type discriminator (gap: dream TaskType)', () => {
  it('exposes the canonical dream task-type and debug component names', () => {
    assert.equal(CONSOLIDATION_TASK_TYPE, 'dream');
    assert.equal(CONSOLIDATION_DEBUG_COMPONENT, 'autoDream');
  });

  it('isDreamTask accepts a run descriptor and rejects everything else', () => {
    const run = beginConsolidationRun({ lockDir: tmpDir, sessionsReviewing: 0 });
    assert.equal(isDreamTask(run), true);
    assert.equal(isDreamTask(null), false);
    assert.equal(isDreamTask(undefined), false);
    assert.equal(isDreamTask({}), false);
    assert.equal(isDreamTask({ type: 'fork' }), false);
    assert.equal(isDreamTask('dream'), false);
    assert.equal(isDreamTask({ type: 'dream', sessionsReviewing: 1 } as ConsolidationRunParams), true);
  });
});

describe('Namespaced [autoDream] debug introspection (gap: skip/delay diagnostics)', () => {
  it('tryAcquireConsolidationLock reports a live-holder block with mtime age', () => {
    const p = consolidationLockPath(tmpDir);
    fs.writeFileSync(p, String(process.pid));
    const tenSecAgo = Date.now() - 10_000;
    fs.utimesSync(p, tenSecAgo / 1000, tenSecAgo / 1000);

    const lines: string[] = [];
    const result = tryAcquireConsolidationLock(tmpDir, Date.now(), (m) => lines.push(m));
    assert.equal(result, null);
    assert.equal(lines.length, 1);
    assert.match(lines[0], /^lock held by live PID \d+ \(mtime \d+s ago\)$/);
    assert.match(lines[0], new RegExp(`PID ${process.pid}\\b`));
  });

  it('rollbackConsolidationLock reports a failure with the delay note', () => {
    const lines: string[] = [];
    // priorMtime 0 → unlink; the lock file does not exist → ENOENT → failure path.
    rollbackConsolidationLock(path.join(tmpDir, 'missing'), 0, (m) => lines.push(m));
    assert.equal(lines.length, 1);
    assert.match(lines[0], /^rollback failed: .+ — next trigger delayed to minHours$/);
  });

  it('recordConsolidation reports a write failure', () => {
    // Make the memory-dir parent a FILE so mkdir/write fails (ENOTDIR).
    const asFile = path.join(tmpDir, 'notadir');
    fs.writeFileSync(asFile, 'x');
    const lines: string[] = [];
    recordConsolidation(path.join(asFile, 'sub'), (m) => lines.push(m));
    assert.equal(lines.length, 1);
    assert.match(lines[0], /^recordConsolidation write failed: /);
  });

  it('defaults to the shared namespaced logger when no sink is passed (no throw)', () => {
    // Exercises the defaultDebug branch end-to-end through the real logger.
    fs.writeFileSync(consolidationLockPath(tmpDir), String(process.pid));
    assert.doesNotThrow(() => {
      const r = tryAcquireConsolidationLock(tmpDir);
      assert.equal(r, null);
    });
    assert.doesNotThrow(() => recordConsolidation(path.join(tmpDir, 'nd-is-file', 'x')));
  });
});
