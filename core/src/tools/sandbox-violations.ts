/**
 * Sandbox violation store — a recorded, queryable surface of operations the
 * sandbox boundary blocked.
 *
 * PARITY (Claude's `SandboxManager.getSandboxViolationStore()` /
 * `SandboxViolationStore` + `SandboxViolationExpandedView`): AMI's container
 * sandbox (see {@link import('./docker-sandbox')}) otherwise blocks operations
 * SILENTLY — a dropped network call or an unmounted-path access just fails with a
 * raw error, giving the user no way to tell "the sandbox blocked it" from "the
 * command itself failed". This module records each detected violation so the user
 * can see what was blocked (the `/sandbox` status surfaces the running total + the
 * last-N), satisfying AMI's introspection invariant.
 *
 * It also implements Claude's `sandbox.ignoreViolations` (a `Record<command,
 * string[]>` of per-command violation strings to tolerate): a recorded violation
 * whose detail matches an allowlisted entry for its command is suppressed, letting
 * operators whitelist known-benign denials without disabling the sandbox wholesale.
 */

/** The class of sandbox boundary that blocked an operation. */
export type SandboxViolationKind = 'network' | 'filesystem';

/** A single blocked-by-sandbox operation. */
export interface SandboxViolationEvent {
  /** Which sandbox boundary blocked the operation. */
  kind: SandboxViolationKind;
  /** The bash command whose execution triggered the violation — also the key used
   *  to match against the {@link SandboxViolationStore} ignore map. */
  command: string;
  /** Human-readable description of the specific blocked operation. */
  detail: string;
  /** Epoch-ms timestamp when the violation was recorded. */
  timestamp: number;
}

/** A listener invoked (with the full, newest-last violation list) on every record. */
export type SandboxViolationListener = (violations: readonly SandboxViolationEvent[]) => void;

/** Default cap on retained violation events (oldest dropped past this). */
export const SANDBOX_VIOLATION_CAP = 500;

/** Clock seam so TTL/timestamp behavior is testable without real wall-clock. */
export const _sandboxViolationHooks = { now: (): number => Date.now() };

/**
 * In-memory store of sandbox violations with a running total, a bounded recent
 * buffer, a subscribe/unsubscribe feed (so a UI can react), and an
 * `ignoreViolations` suppression map (Claude parity). `getTotalCount()` counts
 * every RECORDED (non-suppressed) violation even after old events age out of the
 * bounded buffer.
 */
export class SandboxViolationStore {
  private readonly events: SandboxViolationEvent[] = [];
  private total = 0;
  private readonly listeners = new Set<SandboxViolationListener>();
  private ignore: Record<string, string[]> = {};
  private readonly cap: number;

  constructor(cap: number = SANDBOX_VIOLATION_CAP) {
    this.cap = cap > 0 ? cap : SANDBOX_VIOLATION_CAP;
  }

  /**
   * Install the per-command allowlist of violation substrings to tolerate
   * (Claude's `sandbox.ignoreViolations`). A later {@link record} whose detail
   * matches an allowlisted entry for its command (or for the `'*'` wildcard
   * command) is suppressed. Passing `undefined` clears the map.
   */
  setIgnoreViolations(map: Record<string, string[]> | undefined): void {
    this.ignore = map ?? {};
  }

  /** True if `detail` is allowlisted for `command` (or the `'*'` wildcard key).
   *  An allowlist entry of `'*'` tolerates every violation for that command;
   *  otherwise an entry matches when it is a substring of the detail. */
  private isIgnored(command: string, detail: string): boolean {
    const match = (entries: string[] | undefined): boolean =>
      Array.isArray(entries)
      && entries.some((e) => e === '*' || (e.length > 0 && detail.includes(e)));
    return match(this.ignore[command]) || match(this.ignore['*']);
  }

  /**
   * Record a blocked operation. Returns `true` when it was stored (and listeners
   * notified), or `false` when it was suppressed by the ignore map. The caller
   * supplies `kind`/`command`/`detail`; the timestamp is stamped here.
   */
  record(v: { kind: SandboxViolationKind; command: string; detail: string }): boolean {
    if (this.isIgnored(v.command, v.detail)) return false;
    const event: SandboxViolationEvent = {
      kind: v.kind,
      command: v.command,
      detail: v.detail,
      timestamp: _sandboxViolationHooks.now(),
    };
    this.events.push(event);
    this.total += 1;
    // Keep the buffer bounded — drop the oldest beyond the cap (total is preserved).
    if (this.events.length > this.cap) this.events.splice(0, this.events.length - this.cap);
    for (const fn of this.listeners) fn(this.events);
    return true;
  }

  /** The retained violations, oldest first. */
  getAll(): readonly SandboxViolationEvent[] {
    return this.events;
  }

  /** The most recent `n` violations, oldest first. */
  getRecent(n = 10): readonly SandboxViolationEvent[] {
    if (n <= 0) return [];
    return this.events.slice(-n);
  }

  /** Total number of violations ever recorded (survives buffer eviction). */
  getTotalCount(): number {
    return this.total;
  }

  /** Subscribe to records; returns an unsubscribe function (idempotent). */
  subscribe(fn: SandboxViolationListener): () => void {
    this.listeners.add(fn);
    return () => {
      this.listeners.delete(fn);
    };
  }

  /** Drop all recorded violations and reset the total (e.g. on `/clear`). The
   *  ignore map and subscribers are retained. */
  clear(): void {
    this.events.length = 0;
    this.total = 0;
    for (const fn of this.listeners) fn(this.events);
  }
}

/**
 * Process-wide singleton used by the sandbox backends to record violations and by
 * the `/sandbox` introspection surface to read them. A single store keeps the
 * running total/recent feed coherent across all sandboxed commands in a session.
 */
export const sandboxViolationStore = new SandboxViolationStore();
