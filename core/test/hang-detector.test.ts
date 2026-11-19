/**
 * Unit tests for the pure hang detector (core). Behaviour-level: every test
 * drives the state machine through an injected clock and asserts on the callbacks
 * it fires — never on source text.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  createHangWatchdogDetectionLoop,
  tierForSilence,
  recoveredTier,
  isWallClockDriftSuspend,
} from '../src/hang-detector';

const TIMEOUT_MS = 45_000;
const CHECK_INTERVAL_MS = 5_000;

function counter() {
  const calls: unknown[][] = [];
  const fn = (...args: unknown[]) => {
    calls.push(args);
  };
  Object.defineProperty(fn, 'count', { get: () => calls.length });
  (fn as unknown as { calls: unknown[][] }).calls = calls;
  return fn as typeof fn & { count: number; calls: unknown[][] };
}

function loopWithClock(startAt = 0, stallMs?: number) {
  let now = startAt;
  const onHangDetected = counter();
  const onHangResolved = counter();
  const onStallDetected = counter();
  const onStallMissed = counter();
  const loop = createHangWatchdogDetectionLoop({
    timeoutMs: TIMEOUT_MS,
    checkIntervalMs: CHECK_INTERVAL_MS,
    stallMs,
    now: () => now,
    onHangDetected,
    onHangResolved,
    onStallDetected: (ms) => {
      onStallDetected(ms);
      return true;
    },
    onStallMissed,
  });
  return {
    loop,
    onHangDetected,
    onHangResolved,
    onStallDetected,
    onStallMissed,
    advance: (ms: number) => (now += ms),
  };
}

describe('createHangWatchdogDetectionLoop', () => {
  it('treats a large tick gap as system sleep and restarts the wait', () => {
    const { loop, onHangDetected, advance } = loopWithClock();
    loop.recordHeartbeat();
    advance(TIMEOUT_MS * 4);
    loop.tick();
    assert.equal(onHangDetected.count, 0);
    for (let i = 0; i < 9; i++) {
      advance(CHECK_INTERVAL_MS);
      loop.tick();
    }
    assert.equal(onHangDetected.count, 0);
    for (let i = 0; i < 3; i++) {
      advance(CHECK_INTERVAL_MS);
      loop.tick();
    }
    assert.equal(onHangDetected.count, 1);
  });

  it('can detect a second hang after the first one resolved', () => {
    const { loop, onHangDetected, onHangResolved, advance } = loopWithClock();
    loop.recordHeartbeat();
    for (let i = 0; i < 12; i++) {
      advance(CHECK_INTERVAL_MS);
      loop.tick();
    }
    advance(CHECK_INTERVAL_MS);
    loop.recordHeartbeat();
    assert.equal(onHangResolved.count, 1);
    for (let i = 0; i < 12; i++) {
      advance(CHECK_INTERVAL_MS);
      loop.tick();
    }
    assert.equal(onHangDetected.count, 2);
  });

  it('reports a stall that recovers before the hang timeout, then resolves it', () => {
    const { loop, onStallDetected, onHangDetected, onHangResolved, advance } = loopWithClock(
      0,
      8_000,
    );
    loop.recordHeartbeat();
    advance(5_000);
    loop.tick();
    advance(5_000);
    loop.tick();
    assert.equal(onStallDetected.count, 1);
    assert.deepEqual(onStallDetected.calls[0], [10_000]);
    advance(3_000);
    loop.recordHeartbeat();
    assert.equal(onHangResolved.count, 1);
    assert.deepEqual(onHangResolved.calls[0], [13_000]);
    assert.equal(onHangDetected.count, 0);
  });

  it('escalates the same episode from stall to hang', () => {
    const { loop, onStallDetected, onHangDetected, onHangResolved, advance } = loopWithClock(
      0,
      8_000,
    );
    loop.recordHeartbeat();
    for (let i = 0; i < 10; i++) {
      advance(CHECK_INTERVAL_MS);
      loop.tick();
    }
    assert.equal(onStallDetected.count, 1);
    assert.equal(onHangDetected.count, 1);
    assert.deepEqual(onHangDetected.calls[0], [50_000]);
    loop.recordHeartbeat();
    assert.equal(onHangResolved.count, 1);
  });

  it('records a stall no tick saw once heartbeats resume', () => {
    const { loop, onStallDetected, onStallMissed, advance } = loopWithClock(0, 8_000);
    loop.recordHeartbeat();
    loop.tick();
    advance(4_000);
    loop.tick();
    advance(5_000);
    loop.recordHeartbeat();
    assert.equal(onStallDetected.count, 0);
    assert.equal(onStallMissed.count, 1);
    assert.deepEqual(onStallMissed.calls[0], [9_000, 0]);
  });

  it('does not treat a heartbeat after system sleep as a missed stall', () => {
    const { loop, onStallMissed, advance } = loopWithClock(0, 8_000);
    loop.recordHeartbeat();
    loop.tick();
    advance(20_000);
    loop.recordHeartbeat();
    assert.equal(onStallMissed.count, 0);
  });

  it('ends a detected stall at a sleep gap instead of growing it into a hang', () => {
    let now = 0;
    const onHangDetected = counter();
    const onHangResolved = counter();
    const onStallEndedBySleep = counter();
    const loop = createHangWatchdogDetectionLoop({
      timeoutMs: TIMEOUT_MS,
      checkIntervalMs: CHECK_INTERVAL_MS,
      stallMs: 8_000,
      now: () => now,
      onHangDetected,
      onHangResolved,
      onStallDetected: () => true,
      onStallEndedBySleep,
    });
    loop.recordHeartbeat();
    now += 5_000;
    loop.tick();
    now += 5_000;
    loop.tick();
    now += 3_600_000;
    loop.tick();
    assert.equal(onStallEndedBySleep.count, 1);
    now += 5_000;
    loop.tick();
    assert.equal(onHangDetected.count, 0);
    loop.recordHeartbeat();
    assert.equal(onHangResolved.count, 0);
  });

  it('ends a stall woken before the overdue tick without reporting the sleep as stall time', () => {
    let now = 0;
    const order: string[] = [];
    const onHangResolved = counter();
    const loop = createHangWatchdogDetectionLoop({
      timeoutMs: TIMEOUT_MS,
      checkIntervalMs: CHECK_INTERVAL_MS,
      stallMs: 8_000,
      now: () => now,
      onHangDetected: () => {},
      onHangResolved,
      onStallDetected: () => true,
      onStallEndedBySleep: () => order.push('ended'),
      onWatchdogGap: () => order.push('gap'),
    });
    loop.recordHeartbeat();
    now += 5_000;
    loop.tick();
    now += 5_000;
    loop.tick();
    now += 3_600_000;
    loop.recordHeartbeat();
    assert.equal(onHangResolved.count, 0);
    assert.deepEqual(order, ['gap', 'ended']);
    now += CHECK_INTERVAL_MS;
    loop.tick();
    assert.deepEqual(order, ['gap', 'ended']);
  });

  it('restarts the stall clock when a detected silence was a suspend (onStallDetected=false)', () => {
    let now = 0;
    const stalls: number[] = [];
    let suspended = true;
    const loop = createHangWatchdogDetectionLoop({
      timeoutMs: TIMEOUT_MS,
      checkIntervalMs: CHECK_INTERVAL_MS,
      stallMs: 8_000,
      now: () => now,
      onHangDetected: () => {},
      onHangResolved: () => {},
      onStallDetected: (ms) => {
        stalls.push(ms);
        const accepted = !suspended;
        suspended = false;
        return accepted;
      },
    });
    loop.recordHeartbeat();
    loop.tick();
    now += 10_000;
    loop.tick();
    for (let i = 0; i < 2; i++) {
      now += 5_000;
      loop.tick();
    }
    assert.deepEqual(stalls, [10_000, 10_000]);
  });

  it('keeps a wake heartbeat that arrived before the overdue tick, timing the next stall from it', () => {
    const { loop, onStallMissed, advance } = loopWithClock(0, 8_000);
    loop.recordHeartbeat();
    loop.tick();
    advance(60_000);
    loop.recordHeartbeat();
    advance(1_000);
    loop.tick();
    advance(7_500);
    loop.recordHeartbeat();
    assert.equal(onStallMissed.count, 1);
    assert.deepEqual(onStallMissed.calls[0], [8_500, 60_000]);
  });

  it('does not count a sleep as silence when the overdue tick wakes first after a pre-sleep heartbeat', () => {
    const { loop, onHangDetected, onStallMissed, advance } = loopWithClock(0, 8_000);
    loop.recordHeartbeat();
    loop.tick();
    advance(1_000);
    loop.recordHeartbeat();
    advance(120_000);
    loop.tick();
    advance(5_000);
    loop.tick();
    assert.equal(onHangDetected.count, 0);
    advance(2_000);
    loop.recordHeartbeat();
    assert.equal(onStallMissed.count, 0);
  });

  it('keeps the heartbeat that resolved a hang on wake, timing the next stall from it', () => {
    const { loop, onHangResolved, onStallMissed, advance } = loopWithClock(0, 8_000);
    loop.recordHeartbeat();
    for (let i = 0; i < 10; i++) {
      advance(CHECK_INTERVAL_MS);
      loop.tick();
    }
    advance(70_000);
    loop.recordHeartbeat();
    assert.equal(onHangResolved.count, 1);
    advance(1_000);
    loop.tick();
    advance(5_000);
    loop.tick();
    advance(2_500);
    loop.recordHeartbeat();
    assert.equal(onStallMissed.count, 1);
    assert.deepEqual(onStallMissed.calls[0], [8_500, 120_000]);
  });

  it('drops a wake heartbeat when the machine slept again before the overdue tick', () => {
    const { loop, onHangDetected, advance } = loopWithClock(0, 8_000);
    loop.recordHeartbeat();
    loop.tick();
    advance(60_000);
    loop.recordHeartbeat();
    advance(60_000);
    loop.tick();
    advance(5_000);
    loop.tick();
    assert.equal(onHangDetected.count, 0);
  });

  it('detects hangs only when no stall tier is configured', () => {
    const { loop, onHangDetected, onStallDetected, advance } = loopWithClock();
    loop.recordHeartbeat();
    for (let i = 0; i < 10; i++) {
      advance(CHECK_INTERVAL_MS);
      loop.tick();
    }
    assert.equal(onStallDetected.count, 0);
    assert.equal(onHangDetected.count, 1);
  });

  it('ignores a stall threshold not below the hang timeout', () => {
    const { loop, onStallDetected, onHangDetected, advance } = loopWithClock(0, TIMEOUT_MS);
    loop.recordHeartbeat();
    for (let i = 0; i < 10; i++) {
      advance(CHECK_INTERVAL_MS);
      loop.tick();
    }
    assert.equal(onStallDetected.count, 0);
    assert.equal(onHangDetected.count, 1);
  });

  it('a tick during a detected hang is a no-op', () => {
    const { loop, onHangDetected, advance } = loopWithClock();
    loop.recordHeartbeat();
    for (let i = 0; i < 12; i++) {
      advance(CHECK_INTERVAL_MS);
      loop.tick();
    }
    assert.equal(onHangDetected.count, 1);
    advance(CHECK_INTERVAL_MS);
    loop.tick();
    assert.equal(onHangDetected.count, 1);
  });

  it('works without any optional callbacks provided', () => {
    let now = 0;
    const loop = createHangWatchdogDetectionLoop({
      timeoutMs: TIMEOUT_MS,
      checkIntervalMs: CHECK_INTERVAL_MS,
      stallMs: 8_000,
      now: () => now,
      onHangDetected: () => {},
      onHangResolved: () => {},
    });
    loop.recordHeartbeat();
    // stall threshold crossed with no onStallDetected provided: still escalates, no throw.
    for (let i = 0; i < 10; i++) {
      now += CHECK_INTERVAL_MS;
      loop.tick();
    }
    // missed-stall path with no onStallMissed: no throw.
    now += 10_000;
    loop.recordHeartbeat();
    assert.ok(true);
  });
});

describe('tierForSilence / recoveredTier', () => {
  it('tiers by the hang timeout boundary', () => {
    assert.equal(tierForSilence(10_000, 45_000), 'stall');
    assert.equal(tierForSilence(45_000, 45_000), 'stall');
    assert.equal(tierForSilence(45_001, 45_000), 'hang');
  });

  it('never demotes an episode that already escalated to hang', () => {
    assert.equal(recoveredTier('hang', 1_000, 45_000), 'hang');
    assert.equal(recoveredTier('stall', 50_000, 45_000), 'hang');
    assert.equal(recoveredTier('stall', 10_000, 45_000), 'stall');
    assert.equal(recoveredTier(undefined, 10_000, 45_000), 'stall');
  });
});

describe('isWallClockDriftSuspend', () => {
  it('flags a wall-clock jump far beyond monotonic elapsed as a suspend', () => {
    // 1h wall jump, only 10s monotonic -> suspend.
    assert.equal(isWallClockDriftSuspend(3_600_000, 10_000, 2_000), true);
  });

  it('does not flag normal jitter within tolerance', () => {
    assert.equal(isWallClockDriftSuspend(10_500, 10_000, 2_000), false);
    assert.equal(isWallClockDriftSuspend(10_000, 10_000, 2_000), false);
  });

  it('treats a negative tolerance as zero', () => {
    assert.equal(isWallClockDriftSuspend(10_001, 10_000, -5), true);
  });

  it('returns false for non-finite deltas', () => {
    assert.equal(isWallClockDriftSuspend(Number.NaN, 10_000, 2_000), false);
    assert.equal(isWallClockDriftSuspend(10_000, Number.POSITIVE_INFINITY, 2_000), false);
  });
});
