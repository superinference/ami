/**
 * U253-w112 — scan-throttle backoff for the background consolidation scheduler.
 *
 * Parity: Claude Code's `autoDream` runner guards its (expensive) per-turn session
 * scan behind a closure-scoped `lastSessionScanAt` + `SESSION_SCAN_INTERVAL_MS`
 * (10 min). Because the lock mtime only advances once a consolidation actually
 * fires, a perpetually-open time-gate would otherwise re-scan the transcript
 * directory on every turn. These tests cover `createConsolidationScheduler`:
 *   - gaps "Scan-throttle backoff so a passed time-gate doesn't re-run work every
 *     turn" and "Scan-throttle backoff to avoid re-scanning every turn".
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  createConsolidationScheduler,
  consolidationLockPath,
  SESSION_SCAN_INTERVAL_MS,
  AUTO_DREAM_DEFAULTS,
} from '../src/memory-consolidation';

const NOW = 1_600_000_000_000; // fixed clock anchor (ms)

let tmp: string;
beforeEach(() => {
  tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'scan-throttle-'));
});
afterEach(() => {
  fs.rmSync(tmp, { recursive: true, force: true });
});

/** Stamp the lock so `readLastConsolidatedAt` sees a chosen last-consolidated time. */
function stampLock(mtimeMs: number): void {
  const p = consolidationLockPath(tmp);
  fs.writeFileSync(p, String(process.pid));
  const secs = mtimeMs / 1000;
  fs.utimesSync(p, secs, secs);
}

describe('U253-w112 — createConsolidationScheduler scan throttle', () => {
  it('exposes the 10-minute interval constant mirroring the reference', () => {
    assert.equal(SESSION_SCAN_INTERVAL_MS, 10 * 60 * 1000);
  });

  it('closes at the time-gate WITHOUT scanning (scan callback never runs)', () => {
    stampLock(NOW); // consolidated "now" → 0 hours elapsed
    let scans = 0;
    const sched = createConsolidationScheduler({ debug: () => {} });
    const d = sched.evaluate({ lockDir: tmp, scanSessions: () => { scans++; return 99; }, now: NOW });
    assert.equal(d.fire, false);
    assert.equal(d.reason, 'time-gate');
    assert.equal(d.sessionCount, null);
    assert.equal(scans, 0, 'the expensive scan must not run when the time-gate is closed');
    assert.equal(sched.lastSessionScanAt(), 0, 'a time-gate-closed turn never consumes the throttle window');
  });

  it('fires when the time-gate opens and enough sessions accumulated', () => {
    // never consolidated → Infinity hours → time-gate open
    let scans = 0;
    const sched = createConsolidationScheduler({ debug: () => {} });
    const d = sched.evaluate({ lockDir: tmp, scanSessions: () => { scans++; return 5; }, now: NOW });
    assert.equal(d.fire, true);
    assert.equal(d.reason, 'due');
    assert.equal(d.sessionCount, 5);
    assert.equal(scans, 1);
    assert.equal(sched.lastSessionScanAt(), NOW, 'the scan timestamp advances only after a real scan');
  });

  it('throttles the second turn — same-window re-eval does NOT re-scan', () => {
    let scans = 0;
    const sched = createConsolidationScheduler({ debug: () => {} });
    // First turn: time-gate open, session-gate NOT met → scan runs, lock mtime stays put.
    const first = sched.evaluate({ lockDir: tmp, scanSessions: () => { scans++; return 1; }, now: NOW });
    assert.equal(first.reason, 'session-gate');
    assert.equal(first.sessionCount, 1);
    assert.equal(scans, 1);
    // Second turn, 30s later (< 10 min) with now MANY sessions available: throttle
    // suppresses the scan entirely — proving it is the per-turn de-bounce.
    const second = sched.evaluate({ lockDir: tmp, scanSessions: () => { scans++; return 50; }, now: NOW + 30_000 });
    assert.equal(second.fire, false);
    assert.equal(second.reason, 'scan-throttled');
    assert.equal(second.sessionCount, null);
    assert.equal(scans, 1, 'scan must NOT run again within the throttle interval');
    assert.equal(sched.lastSessionScanAt(), NOW, 'the throttled turn does not move the scan timestamp');
  });

  it('re-scans once the throttle interval lapses (and fires)', () => {
    let scans = 0;
    const sched = createConsolidationScheduler({ debug: () => {} });
    sched.evaluate({ lockDir: tmp, scanSessions: () => { scans++; return 1; }, now: NOW });
    assert.equal(scans, 1);
    // Exactly at the interval boundary it is still throttled (strict <), one ms past it opens.
    const atBoundary = sched.evaluate({ lockDir: tmp, scanSessions: () => { scans++; return 5; }, now: NOW + SESSION_SCAN_INTERVAL_MS - 1 });
    assert.equal(atBoundary.reason, 'scan-throttled');
    assert.equal(scans, 1);
    const past = sched.evaluate({ lockDir: tmp, scanSessions: () => { scans++; return 5; }, now: NOW + SESSION_SCAN_INTERVAL_MS });
    assert.equal(past.fire, true);
    assert.equal(past.reason, 'due');
    assert.equal(scans, 2, 'scan runs again once the interval has elapsed');
    assert.equal(sched.lastSessionScanAt(), NOW + SESSION_SCAN_INTERVAL_MS);
  });

  it('doubles as post-failure backoff: a lock rewind re-opens the time-gate but the scan stays throttled', () => {
    // Simulate: a run fired at NOW (scan ran), then the launch failed and the lock
    // mtime was rewound (time-gate re-opens). Within the interval the scheduler must
    // NOT immediately re-scan — the scan throttle IS the retry backoff.
    let scans = 0;
    const sched = createConsolidationScheduler({ debug: () => {} });
    sched.evaluate({ lockDir: tmp, scanSessions: () => { scans++; return 5; }, now: NOW }); // due, scan #1
    // lock stays absent (rewound to "never") → time-gate still open
    const retry = sched.evaluate({ lockDir: tmp, scanSessions: () => { scans++; return 5; }, now: NOW + 60_000 });
    assert.equal(retry.reason, 'scan-throttled');
    assert.equal(scans, 1, 'retry is backed off by the scan throttle, not re-attempted every turn');
  });

  it('clamps the scanned session count to a non-negative integer', () => {
    const sched = createConsolidationScheduler({ debug: () => {} });
    const neg = sched.evaluate({ lockDir: tmp, scanSessions: () => -3, now: NOW });
    assert.equal(neg.sessionCount, 0);
    assert.equal(neg.reason, 'session-gate');
    const frac = createConsolidationScheduler({ debug: () => {} })
      .evaluate({ lockDir: tmp, scanSessions: () => 5.9, now: NOW });
    assert.equal(frac.sessionCount, 5);
    assert.equal(frac.fire, true);
  });

  it('honors validated rawConfig overrides for both gates', () => {
    stampLock(NOW - 2 * 60 * 60 * 1000); // 2h ago
    const sched = createConsolidationScheduler({ debug: () => {} });
    // minHours=1 opens the time-gate at 2h; minSessions=2 is met by 3.
    const d = sched.evaluate({
      lockDir: tmp,
      scanSessions: () => 3,
      rawConfig: { minHours: 1, minSessions: 2 },
      now: NOW,
    });
    assert.equal(d.fire, true);
    assert.equal(d.config.minHours, 1);
    assert.equal(d.config.minSessions, 2);
    // A garbage rawConfig falls back to defaults (24h) → time-gate closed at 2h.
    const bad = createConsolidationScheduler({ debug: () => {} }).evaluate({
      lockDir: tmp,
      scanSessions: () => 3,
      rawConfig: { minHours: -1, minSessions: 'x' },
      now: NOW,
    });
    assert.equal(bad.reason, 'time-gate');
    assert.equal(bad.config.minHours, AUTO_DREAM_DEFAULTS.minHours);
  });

  it('keeps throttle state independent per scheduler instance', () => {
    const a = createConsolidationScheduler({ debug: () => {} });
    const b = createConsolidationScheduler({ debug: () => {} });
    a.evaluate({ lockDir: tmp, scanSessions: () => 1, now: NOW });
    assert.equal(a.lastSessionScanAt(), NOW);
    assert.equal(b.lastSessionScanAt(), 0, 'a second scheduler has its own fresh throttle');
  });

  it('emits namespaced diagnostics for throttle / skip / due', () => {
    const lines: string[] = [];
    const sched = createConsolidationScheduler({ debug: (m) => lines.push(m) });
    sched.evaluate({ lockDir: tmp, scanSessions: () => 1, now: NOW }); // session-gate (skip)
    sched.evaluate({ lockDir: tmp, scanSessions: () => 1, now: NOW + 1000 }); // throttled
    createConsolidationScheduler({ debug: (m) => lines.push(m) })
      .evaluate({ lockDir: tmp, scanSessions: () => 9, now: NOW }); // due
    assert.ok(lines.some((l) => /need 5/.test(l)), 'session-gate skip is explained');
    assert.ok(lines.some((l) => /scan throttle/.test(l)), 'throttle is explained');
    assert.ok(lines.some((l) => /due —/.test(l) && /9 sessions/.test(l)), 'a firing decision is announced');
  });

  it('uses a custom scanIntervalMs when supplied', () => {
    let scans = 0;
    const sched = createConsolidationScheduler({ scanIntervalMs: 1000, debug: () => {} });
    sched.evaluate({ lockDir: tmp, scanSessions: () => { scans++; return 1; }, now: NOW });
    // 1.5s later, past the 1s custom interval → scans again.
    sched.evaluate({ lockDir: tmp, scanSessions: () => { scans++; return 1; }, now: NOW + 1500 });
    assert.equal(scans, 2);
  });
});
