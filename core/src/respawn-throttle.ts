/**
 * Clock-injectable sliding-window crash-loop respawn throttle.
 *
 * AMI auto-restarts things that can crash in a tight loop: MCP servers it
 * supervises, detached runs, and spawn-capable bash trees driven by an
 * agent-in-a-loop. A thing that crashes the instant it starts, if respawned
 * unconditionally, becomes a busy-loop that burns CPU and floods logs without
 * ever making progress.
 *
 * This throttle admits at most {@link RespawnThrottleOptions.maxAttempts}
 * respawns inside a rolling {@link RespawnThrottleOptions.windowMs}. Once that
 * budget is spent it refuses with `crash_loop` and reports how long until the
 * oldest attempt ages out of the window (so the caller can schedule a retry
 * instead of spinning). The clock is injectable so the behaviour is
 * deterministically testable without real time.
 *
 * Recovery is NOT automatic on a single successful start — a fork can succeed
 * and then crash immediately, so clearing the window on `admit()` would defeat
 * the throttle. {@link RespawnThrottle.reset} is called ONLY after proven
 * recovery (the restarted process has stayed healthy) or an explicit
 * user-initiated restart.
 *
 * Building block (core): the MCP restart path and the detached runner consult
 * an instance of this; {@link ToolCallGuardrailController} in common holds one
 * for the bash-spawn crash-loop case.
 */

/** The outcome of asking the throttle whether a respawn may proceed. */
export type RespawnAdmission =
  | { allowed: true }
  | {
      allowed: false;
      reason: 'crash_loop';
      /** How many attempts are currently inside the window. */
      attemptsInWindow: number;
      /** Milliseconds until the oldest attempt ages out and a slot frees up. */
      retryAfterMs: number;
    };

export interface RespawnThrottleOptions {
  /** Attempts admitted per window before `crash_loop` is returned. */
  maxAttempts?: number;
  /** Width of the sliding window, in milliseconds. */
  windowMs?: number;
  /** Clock seam; defaults to {@link Date.now}. */
  now?: () => number;
}

/** Allow occasional restarts while bounding repeated launch attempts. */
export const DEFAULT_RESPAWN_MAX_ATTEMPTS = 5;
export const DEFAULT_RESPAWN_WINDOW_MS = 60_000;

export class RespawnThrottle {
  private readonly maxAttempts: number;
  private readonly windowMs: number;
  private readonly now: () => number;
  private attempts: number[] = [];

  constructor(options: RespawnThrottleOptions = {}) {
    const maxAttempts = options.maxAttempts ?? DEFAULT_RESPAWN_MAX_ATTEMPTS;
    const windowMs = options.windowMs ?? DEFAULT_RESPAWN_WINDOW_MS;
    if (!Number.isInteger(maxAttempts) || maxAttempts <= 0) {
      throw new RangeError(`RespawnThrottle maxAttempts must be a positive integer, got ${maxAttempts}`);
    }
    if (!(windowMs > 0)) {
      throw new RangeError(`RespawnThrottle windowMs must be > 0, got ${windowMs}`);
    }
    this.maxAttempts = maxAttempts;
    this.windowMs = windowMs;
    this.now = options.now ?? Date.now;
  }

  /**
   * Ask whether a respawn may proceed. Evicts attempts older than the window,
   * then either records this attempt and allows it, or refuses with the time
   * until the window frees a slot. A refusal does NOT record an attempt, so the
   * window drains purely by the passage of time.
   */
  admit(): RespawnAdmission {
    const now = this.now();
    this.attempts = this.attempts.filter((at) => now - at < this.windowMs);
    if (this.attempts.length >= this.maxAttempts) {
      const oldest = this.attempts[0] as number;
      return {
        allowed: false,
        reason: 'crash_loop',
        attemptsInWindow: this.attempts.length,
        retryAfterMs: Math.max(0, this.windowMs - (now - oldest)),
      };
    }
    this.attempts.push(now);
    return { allowed: true };
  }

  /**
   * Clear the window. A successful fork can still crash immediately, so reset
   * ONLY after proven recovery (the restart stayed healthy) or an explicit
   * restart — never on a bare successful `admit()`.
   */
  reset(): void {
    this.attempts = [];
  }

  /** Attempts currently inside the window (observability / tests). */
  attemptsInWindow(): number {
    const now = this.now();
    this.attempts = this.attempts.filter((at) => now - at < this.windowMs);
    return this.attempts.length;
  }
}

/** Thrown (or surfaced as a reason) when the throttle refuses a respawn. */
export class CrashLoopError extends Error {
  readonly code = 'crash_loop';
  readonly attemptsInWindow: number;
  readonly retryAfterMs: number;
  constructor(admission: Extract<RespawnAdmission, { allowed: false }>, subject = 'process') {
    super(
      `The ${subject} has failed to start ${admission.attemptsInWindow} times in a row; ` +
        `refusing to respawn it for another ${Math.ceil(admission.retryAfterMs / 1000)}s. ` +
        'It will not restart until the underlying failure is fixed.',
    );
    this.name = 'CrashLoopError';
    this.attemptsInWindow = admission.attemptsInWindow;
    this.retryAfterMs = admission.retryAfterMs;
  }
}
