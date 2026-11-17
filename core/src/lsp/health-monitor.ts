/**
 * LSP health-poll state machine.
 *
 * Mirrors Claude Code's `useLspInitializationNotification` polling loop
 * (hooks/notifs/useLspInitializationNotification.tsx): on a fixed interval it
 * re-checks the client's initialization status so a language server that enters
 * an error state AFTER a successful launch is still caught, dedupes each error
 * per source so it is surfaced once (never spammed), and STOPS polling once the
 * manager has DEFINITIVELY failed — a bounded give-up boundary. While the client
 * is still initializing (`pending`/`not-started`) it keeps polling; once
 * initialized it scans every server for a post-startup error and keeps polling
 * to catch future crashes.
 *
 * The lazy, on-demand LSP client only ever (re)checks a server when a tool
 * touches a matching file; without this loop a long-lived server that crashes
 * between tool calls would stay silently dead. The loop is pure logic plus an
 * injectable scheduler so it is fully testable without real timers.
 */
import type { LspInitializationStatus, LspServerStatus } from './client';
import { logError } from '../logger';

/**
 * Health-poll interval in milliseconds. Parity with Claude Code's
 * `LSP_POLL_INTERVAL_MS = 5000` driving `useInterval(poll, shouldPoll ? … : null)`.
 */
export const LSP_POLL_INTERVAL_MS = 5000;

/** A single post-startup LSP error observed by a poll tick. */
export interface LspHealthError {
  /** `lsp-manager` for a client-wide init failure, else the server's language. */
  source: string;
  message: string;
}

/**
 * The user-facing diagnostic command an LSP failure toast points the user at.
 * AMI surfaces LSP health failures through the `/doctor` diagnostic surface
 * (health errors route to the shared logger ring `/doctor` reads), so the
 * affordance hint references it — the AMI analogue of Claude Code's
 * `· /plugin for details` hint on its LSP-failure notification.
 */
export const LSP_HEALTH_HINT = '/doctor for details';

/**
 * Format an {@link LspHealthError} into the user-facing failure toast surfaced in
 * the TUI. Mirrors Claude Code's `useLspInitializationNotification` notification
 * (`<Text color="error">LSP for {displayName} failed</Text>` +
 * `<Text dimColor> · /plugin for details</Text>`): a bare `plugin:` prefix is
 * stripped from the source so a plugin-contributed server shows a clean name, and
 * an affordance hint points the user at the diagnostic command. Returned as data
 * (message + hint) so the CLI renders it through AMI's own flash chrome rather
 * than any hard-coded styling — this is a pure string builder, no UI rendering.
 */
export function formatLspHealthError(error: LspHealthError): { message: string; hint: string } {
  const displayName = error.source.startsWith('plugin:')
    ? error.source.split(':')[1] ?? error.source
    : error.source;
  return { message: `LSP for ${displayName} failed`, hint: LSP_HEALTH_HINT };
}

/**
 * Classify one poll tick against the client's current initialization status and
 * per-server statuses, returning the errors observed right now and whether
 * polling should STOP. The branch structure mirrors Claude's `poll()` exactly:
 *  - `failed`                     → report the manager error AND stop (terminal);
 *  - `pending` / `not-started`    → nothing to report, keep polling (initializing);
 *  - otherwise (success)          → scan servers for `error` states, keep polling.
 *
 * Deduping is the caller's responsibility (so the same error is reported once).
 */
export function collectLspHealthErrors(
  status: LspInitializationStatus,
  servers: LspServerStatus[],
): { errors: LspHealthError[]; shouldStop: boolean } {
  if (status.status === 'failed') {
    return { errors: [{ source: 'lsp-manager', message: status.error }], shouldStop: true };
  }
  if (status.status === 'pending' || status.status === 'not-started') {
    return { errors: [], shouldStop: false };
  }
  const errors: LspHealthError[] = [];
  for (const server of servers) {
    if (server.state === 'error' && server.lastError) {
      errors.push({ source: server.language, message: server.lastError });
    }
  }
  return { errors, shouldStop: false };
}

/**
 * Minimal timer interface so tests can drive polling synchronously (no real
 * wall-clock wait) by injecting a scheduler that captures the tick callback.
 */
export interface HealthScheduler {
  setInterval: (fn: () => void, ms: number) => unknown;
  clearInterval: (handle: unknown) => void;
}

/**
 * Default scheduler: a real `setInterval` that is `unref`'d so the poll loop
 * NEVER keeps the process alive on its own (a background health check must not
 * block a clean exit).
 */
const DEFAULT_SCHEDULER: HealthScheduler = {
  setInterval: (fn, ms) => {
    const timer = setInterval(fn, ms);
    if (typeof (timer as { unref?: () => void }).unref === 'function') {
      (timer as { unref: () => void }).unref();
    }
    return timer;
  },
  clearInterval: (handle) => clearInterval(handle as ReturnType<typeof setInterval>),
};

export interface LspHealthMonitorOptions {
  getStatus: () => LspInitializationStatus;
  getServers: () => LspServerStatus[];
  /** Sink for a newly-observed error. Defaults to the shared error logger. */
  onError?: (error: LspHealthError) => void;
  /** Poll interval; defaults to {@link LSP_POLL_INTERVAL_MS}. */
  intervalMs?: number;
  /** Timer source; defaults to an unref'd real `setInterval`. */
  scheduler?: HealthScheduler;
}

/**
 * Drives {@link collectLspHealthErrors} on a fixed interval, deduping errors per
 * `source:message` and latching OFF once the manager has definitively failed.
 */
export class LspHealthMonitor {
  private handle: unknown = undefined;
  private readonly notified = new Set<string>();
  private readonly getStatus: () => LspInitializationStatus;
  private readonly getServers: () => LspServerStatus[];
  private readonly onError: (error: LspHealthError) => void;
  private readonly intervalMs: number;
  private readonly scheduler: HealthScheduler;

  constructor(opts: LspHealthMonitorOptions) {
    this.getStatus = opts.getStatus;
    this.getServers = opts.getServers;
    this.onError =
      opts.onError ??
      ((error) => logError('lsp', `LSP for ${error.source} failed: ${error.message}`));
    this.intervalMs = opts.intervalMs ?? LSP_POLL_INTERVAL_MS;
    this.scheduler = opts.scheduler ?? DEFAULT_SCHEDULER;
  }

  /** True while the poll loop is scheduled. */
  isRunning(): boolean {
    return this.handle !== undefined;
  }

  /** Begin polling. Idempotent — a second call while running is a no-op. */
  start(): void {
    if (this.handle !== undefined) return;
    this.handle = this.scheduler.setInterval(() => this.poll(), this.intervalMs);
  }

  /** Stop polling. Idempotent. */
  stop(): void {
    if (this.handle === undefined) return;
    this.scheduler.clearInterval(this.handle);
    this.handle = undefined;
  }

  /**
   * One poll tick: report every newly-observed (deduped) error and stop polling
   * when the manager has terminally failed. Public so a consumer (or a test) can
   * trigger a check on demand without waiting for the interval.
   */
  poll(): void {
    const { errors, shouldStop } = collectLspHealthErrors(this.getStatus(), this.getServers());
    for (const error of errors) {
      const key = `${error.source}:${error.message}`;
      if (this.notified.has(key)) continue;
      this.notified.add(key);
      this.onError(error);
    }
    if (shouldStop) this.stop();
  }
}
