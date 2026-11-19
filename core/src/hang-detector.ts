/**
 * Runtime hang detector — PURE building block (core layer).
 *
 * A heartbeat/tick state machine that tells you WHERE the single engine thread
 * wedged. The engine loop records a heartbeat on every iteration; an independent
 * timer calls {@link HangWatchdogDetectionLoop.tick} on a fixed cadence. When
 * heartbeats go silent while ticks keep arriving, the loop reports a STALL (a
 * recoverable beachball) and, past the hang timeout, escalates to a HANG.
 *
 * The hard part this solves is telling a genuine stall apart from the MACHINE's
 * own silence (laptop suspend / process suspension / timer starvation): when the
 * watchdog's OWN ticks were overdue by more than {@link sleepGapMs}, the silence
 * belongs to the machine, not to the engine, so the wait is restarted rather than
 * reported as a hang. This mirrors the reference watchdog subsystem one-to-one.
 *
 * This module is pure (no timers, no I/O, no process access); the clock is
 * injected via `now`. The wiring that drives it from a real timer and captures
 * stack evidence to disk lives in `common/src/hang-watchdog.ts`. Per the layer
 * rules, core NEVER imports from common/.
 */

/** The two escalation tiers an unresponsive episode can reach. */
export type HangTier = 'stall' | 'hang';

export type HangWatchdogDetectionLoopConfig = {
  /** Silence beyond this (and ticks still arriving) is a HANG. */
  timeoutMs: number;
  /** The cadence the external timer calls {@link HangWatchdogDetectionLoop.tick}. */
  checkIntervalMs: number;
  /**
   * Earlier tier for beachballs that recover before `timeoutMs`; omit to detect
   * hangs only. Ignored when `>= timeoutMs` (a stall can never outlast a hang).
   */
  stallMs?: number;
  /** Injected monotonic-ish clock in ms. */
  now: () => number;
  /** A full hang was detected after `unresponsiveMs` of heartbeat silence. */
  onHangDetected: (unresponsiveMs: number) => void;
  /**
   * A stall was detected. Return `false` when the silence was the machine's (a
   * suspend, e.g. detected via wall-clock drift) — that restarts the stall clock
   * rather than letting the episode escalate into a hang.
   */
  onStallDetected?: (unresponsiveMs: number) => boolean;
  /** Heartbeats resumed after a detected stall or hang — the thread stalled, not deadlocked. */
  onHangResolved: (unresponsiveMs: number) => void;
  /** Heartbeats resumed after a stall no tick saw; its evidence can only be gathered afterwards. */
  onStallMissed?: (unresponsiveMs: number, startedAt: number) => void;
  /** A sleep interrupted a detected stall; its silence after this point belongs to the machine. */
  onStallEndedBySleep?: () => void;
  /** The watchdog's own timer was starved for `gapMs` (suspend / GC / overload). */
  onWatchdogGap?: (gapMs: number, startedAt: number) => void;
};

export type HangWatchdogDetectionLoop = {
  /** Call from the engine loop each iteration to prove liveness. */
  recordHeartbeat: () => void;
  /** Call from the independent timer on the configured cadence. */
  tick: () => void;
};

/**
 * Build the pure hang-detection state machine. Faithful port of the reference
 * watchdog detection loop; every branch is exercised by the unit tests.
 */
export function createHangWatchdogDetectionLoop(
  config: HangWatchdogDetectionLoopConfig,
): HangWatchdogDetectionLoop {
  const sleepGapMs = config.checkIntervalMs * 3;
  const stallMs =
    config.stallMs !== undefined && config.stallMs < config.timeoutMs ? config.stallMs : null;
  let lastHeartbeatAt = config.now();
  let lastTickAt = config.now();
  let detected: HangTier | null = null;
  // A heartbeat received while the tick was overdue came after the wake; the overdue tick keeps it.
  let wokeHeartbeat = false;
  return {
    recordHeartbeat: () => {
      const now = config.now();
      const silentMs = now - lastHeartbeatAt;
      if (detected === 'stall' && now - lastTickAt > sleepGapMs) {
        // Woke before the overdue tick: report and consume its gap first, in the
        // tick's order, so it attaches to this stall.
        const tickGap = now - lastTickAt;
        if (tickGap > config.timeoutMs) {
          config.onWatchdogGap?.(tickGap, lastTickAt);
        }
        lastTickAt = now;
        detected = null;
        config.onStallEndedBySleep?.();
      } else if (detected) {
        detected = null;
        config.onHangResolved(silentMs);
      } else if (stallMs !== null && silentMs > stallMs && now - lastTickAt <= sleepGapMs) {
        // A tick-gap reset already moved lastHeartbeatAt past any sleep, so this silence was the loop's.
        config.onStallMissed?.(silentMs, lastHeartbeatAt);
      }
      wokeHeartbeat ||= now - lastTickAt > sleepGapMs;
      lastHeartbeatAt = now;
    },
    tick: () => {
      const now = config.now();
      const tickGap = now - lastTickAt;
      // Advance while hung so recovery cannot be misclassified as a long system-sleep gap.
      lastTickAt = now;
      // Sleep, process suspension and observer starvation are indistinguishable here.
      if (tickGap > sleepGapMs) {
        if (tickGap > config.timeoutMs) {
          config.onWatchdogGap?.(tickGap, now - tickGap);
        }
        if (detected === 'stall') {
          detected = null;
          config.onStallEndedBySleep?.();
        }
        // A wake heartbeat older than a sleep gap predates another sleep; it no longer marks the start.
        if (!detected && !(wokeHeartbeat && now - lastHeartbeatAt <= sleepGapMs)) {
          lastHeartbeatAt = now;
        }
        wokeHeartbeat = false;
        return;
      }
      if (detected === 'hang') {
        return;
      }
      const unresponsiveMs = now - lastHeartbeatAt;
      if (unresponsiveMs > config.timeoutMs) {
        detected = 'hang';
        config.onHangDetected(unresponsiveMs);
      } else if (!detected && stallMs !== null && unresponsiveMs > stallMs) {
        detected = 'stall';
        if (config.onStallDetected?.(unresponsiveMs) === false) {
          detected = null;
          lastHeartbeatAt = now;
        }
      }
    },
  };
}

/**
 * Past the hang timeout an episode is a hang however it was observed; the loop's
 * `>` can let one recover as a stall. Pure.
 */
export function tierForSilence(unresponsiveMs: number, timeoutMs: number): HangTier {
  return unresponsiveMs > timeoutMs ? 'hang' : 'stall';
}

/**
 * The tier a resolved episode should be recorded under, given the stall tier it
 * started at and the total unresponsive time: a `stall` never demotes an episode
 * that already escalated to `hang`. Pure.
 */
export function recoveredTier(
  startedTier: HangTier | undefined,
  unresponsiveMs: number,
  timeoutMs: number,
): HangTier {
  return startedTier === 'hang' ? 'hang' : tierForSilence(unresponsiveMs, timeoutMs);
}

/**
 * Discriminate a real engine stall from a machine suspend via WALL-CLOCK DRIFT.
 * Between two watchdog ticks the monotonic clock measures elapsed CPU-scheduled
 * time; the wall clock (`Date.now()`) also advances across a suspend. When the
 * wall advanced substantially more than the monotonic delta, the process was
 * suspended — the silence was the machine's, not a real stall. Pure.
 *
 * @param wallDeltaMs      `Date.now()` delta across the interval.
 * @param monotonicDeltaMs monotonic-clock delta across the same interval.
 * @param toleranceMs      drift allowed before calling it a suspend (jitter headroom).
 */
export function isWallClockDriftSuspend(
  wallDeltaMs: number,
  monotonicDeltaMs: number,
  toleranceMs: number,
): boolean {
  if (!Number.isFinite(wallDeltaMs) || !Number.isFinite(monotonicDeltaMs)) return false;
  return wallDeltaMs - monotonicDeltaMs > Math.max(0, toleranceMs);
}
