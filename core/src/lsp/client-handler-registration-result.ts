// Split from client.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as child_process from 'child_process';

/** LSP transient error: server state changed mid-request (e.g. still indexing). */
export const LSP_CONTENT_MODIFIED = -32801;

/** Default per-request timeout. */
export const DEFAULT_REQUEST_TIMEOUT_MS = 15000;

/** Default bounded wait for diagnostics to settle after open/change. */
export const DEFAULT_DIAGNOSTICS_TIMEOUT_MS = 2000;

/** Default crash-recovery cap (matches the reference's maxRestarts default). */
export const DEFAULT_MAX_RESTARTS = 3;

/**
 * Bounded wait for a server's reply to the graceful `shutdown` request before we
 * send `exit` and kill anyway. Teardown must never hang on an unresponsive server.
 */
export const DEFAULT_SHUTDOWN_TIMEOUT_MS = 1000;

/**
 * Grace window after the `exit` notification before we force-kill a server that
 * has not shut itself down. Keeps teardown a clean LSP shutdown in the common case.
 */
export const EXIT_GRACE_MS = 50;

/**
 * Consecutive diagnostic-handling failures for one server before we emit a loud
 * warning. Mirrors the reference's passiveFeedback threshold (warn after >= 3) so a
 * persistently misbehaving (or malicious) language server whose diagnostics keep
 * failing to process is surfaced instead of being silently swallowed forever.
 */
export const DIAGNOSTIC_FAILURE_WARN_THRESHOLD = 3;

/**
 * Bytes of a language server's stderr retained for crash classification. The
 * failure reason a crashing server prints (missing module, disabled API, …) lands
 * at the END of its stderr, so only the most recent slice is kept — enough to carry
 * the signature {@link classifySubprocessError} matches on, bounded so a chatty
 * server can never grow the buffer without limit.
 */
export const LSP_STDERR_TAIL_BYTES = 4096;

/**
 * The BCP-47 locale AMI advertises to a language server in the `initialize`
 * handshake, derived from the process environment (LC_ALL > LC_MESSAGES > LANG),
 * stripped of any `.encoding`/`@modifier` suffix (`en_US.UTF-8` → `en_US`).
 * Falls back to `en` when unset. Mirrors the reference's `locale` InitializeParam,
 * which lets a server localize its diagnostic messages.
 */
export function lspLocale(env: NodeJS.ProcessEnv = process.env): string {
  const raw = env.LC_ALL || env.LC_MESSAGES || env.LANG || '';
  const base = raw.split('.')[0].split('@')[0].trim();
  return base.length > 0 && base !== 'C' && base !== 'POSIX' ? base : 'en';
}

/**
 * Lifecycle state of a single language server, mirroring the reference's
 * `LspServerState` (services/lsp/LSPServerInstance.ts). `error` is retained after
 * a crash so the failure reason stays attributable once the live process is gone.
 */
export const LSP_SERVER_STATES = ['stopped', 'starting', 'running', 'stopping', 'error'] as const;

export type LspServerState = (typeof LSP_SERVER_STATES)[number];

/**
 * Introspection snapshot of one language server's lifecycle — the AMI analogue of
 * the reference's `LSPServerInstance` readonly getters (`state`, `startTime`, `lastError`,
 * `restartCount`) plus the `isHealthy` predicate and the last `exitCode`. Surfaced
 * by `getServerStatus`/`getAllServerStatus` and rendered in `/doctor`.
 */
export interface LspServerStatus {
  language: string;
  state: LspServerState;
  startTime?: number;
  /** Message of the last error this server hit (crash, spawn failure, …). */
  lastError?: string;
  /** Number of times this language's server has been (re)spawned after a crash. */
  restartCount: number;
  /** Process exit code of the last exit, when it exited (null = killed by signal). */
  exitCode?: number | null;
  /** True only when the server is running AND its handshake completed. */
  isHealthy: boolean;
  /**
   * Number of CONSECUTIVE `publishDiagnostics` deliveries from this server that
   * failed to process (reset to 0 on the next successful delivery). Mirrors
   * the reference's passiveFeedback per-server `diagnosticFailures` counter; once it
   * reaches `DIAGNOSTIC_FAILURE_WARN_THRESHOLD` a warning is logged. Surfaced by
   * `getServerStatus`/`getAllServerStatus` so `/doctor` can flag a flapping server.
   */
  consecutiveDiagnosticFailures: number;
}

/**
 * Client-wide initialization status, mirroring the reference's manager-level
 * `getInitializationStatus()` discriminated union — lets a UI/tool report WHY LSP
 * is (un)available instead of a bare boolean.
 */
export type LspInitializationStatus =
  | { status: 'not-started' }
  | { status: 'pending' }
  | { status: 'success' }
  | { status: 'failed'; error: string };

/**
 * Aggregate outcome of wiring diagnostic-notification handling across every
 * language server, mirroring the reference's passiveFeedback `HandlerRegistrationResult`
 * (services/lsp/passiveFeedback.ts L104-L114). the reference registers a
 * `textDocument/publishDiagnostics` handler on each `LSPServerInstance` in a loop,
 * isolates per-server failures, and returns this typed object for monitoring/testing.
 *
 * AMI dispatches `publishDiagnostics` CENTRALLY (one client-level handler fan-out in
 * {@link LSPClient.deliverDiagnostics}), so there is no per-instance handler to
 * register; instead {@link LSPClient.registerDiagnosticHandlers} enumerates the
 * servers attempted this session, VALIDATES each can actually deliver diagnostics
 * (live, initialized, readable stdout), isolates a dead/half-open server as a
 * per-server `registrationError`, and returns this SAME typed shape — consolidating
 * the already-tracked per-server lifecycle + diagnostic-failure state into the reference's
 * monitoring schema rather than adding a parallel tracker.
 */
export interface HandlerRegistrationResult {
  /** Total number of servers attempted this session (the enumeration universe). */
  totalServers: number;
  /** Number of servers ready to deliver diagnostics (running + initialized + readable). */
  successCount: number;
  /** Per-server readiness failures, isolated so one bad server never masks the rest. */
  registrationErrors: Array<{ serverName: string; error: string }>;
  /**
   * Snapshot copy of the consecutive `publishDiagnostics` processing-failure counts
   * per server (the reference's `diagnosticFailures` map). A copy, so a caller inspecting
   * the monitoring result cannot mutate the client's live failure tracker.
   */
  diagnosticFailures: Map<string, { count: number; lastError: string }>;
}

export type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (reason: Error) => void;
  timer: ReturnType<typeof setTimeout>;
};

export interface ServerConn {
  /** The language this connection serves — carried for log/error context. */
  language: string;
  proc: child_process.ChildProcess;
  buffer: Buffer;
  nextId: number;
  pending: Map<number, PendingRequest>;
  capabilities: unknown;
  initialized: boolean;
}

export type NotificationHandler = (params: unknown) => void;

export type RequestHandler = (params: unknown) => unknown | Promise<unknown>;
