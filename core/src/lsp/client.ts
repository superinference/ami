/**
 * LSP client — a real JSON-RPC request/response client over stdio.
 *
 * Mirrors Claude Code's LSPClient/LSPServerInstance behavior without pulling in
 * vscode-jsonrpc (core/ stays dependency-light): it frames outbound messages
 * with Content-Length headers, PARSES the Content-Length-framed stream the
 * server writes back, correlates responses to pending requests by id, and
 * dispatches server-pushed notifications (notably `textDocument/publishDiagnostics`)
 * to registered handlers. On top of the raw channel it provides:
 *   - a proper initialize handshake (await the result, capture capabilities,
 *     send the mandatory `initialized` notification, THEN mark ready);
 *   - per-request timeouts + error propagation + ContentModified (-32801)
 *     retry with exponential backoff (reusing the shared retry helper);
 *   - crash-recovery caps so a persistently crashing server can't respawn
 *     unbounded;
 *   - captured diagnostics + a baseline/diff tracker for post-edit reporting;
 *   - a scrubbed subprocess environment and a supply-chain guard against npx
 *     auto-fetching servers from the registry.
 */
import * as child_process from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { pathToFileURL } from 'url';
import { sanitizeEnvForSubprocess } from '../tools/bash';
import { isBinaryInstalled } from '../utils/which';
import { retryWithBackoff } from '../mcp/oauth';
import { log, logError } from '../logger';
import { AMI_CLIENT_INFO } from '../version';
import {
  assertNoAutoFetch,
  getAllLspServers,
  languageIdForExtension,
  resolveLanguageForExtension,
  type LspServerConfig,
} from './config';
import {
  convertPublishDiagnostics,
  DiagnosticTracker,
  lspFileSizeBytes,
  normalizeDiagnosticPath,
  uriToPath,
  MAX_LSP_FILE_SIZE_BYTES,
  type DiagnosticFile,
  type DiagnosticSource,
  type PublishDiagnosticsParams,
} from './diagnostic-tracking';
import {
  LspHealthMonitor,
  type HealthScheduler,
  type LspHealthError,
} from './health-monitor';

/** LSP transient error: server state changed mid-request (e.g. still indexing). */
const LSP_CONTENT_MODIFIED = -32801;
/** Default per-request timeout. */
const DEFAULT_REQUEST_TIMEOUT_MS = 15000;
/** Default bounded wait for diagnostics to settle after open/change. */
const DEFAULT_DIAGNOSTICS_TIMEOUT_MS = 2000;
/** Default crash-recovery cap (matches Claude's maxRestarts default). */
const DEFAULT_MAX_RESTARTS = 3;
/**
 * Bounded wait for a server's reply to the graceful `shutdown` request before we
 * send `exit` and kill anyway. Teardown must never hang on an unresponsive server.
 */
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 1000;
/**
 * Grace window after the `exit` notification before we force-kill a server that
 * has not shut itself down. Keeps teardown a clean LSP shutdown in the common case.
 */
const EXIT_GRACE_MS = 50;
/**
 * Consecutive diagnostic-handling failures for one server before we emit a loud
 * warning. Mirrors Claude Code's passiveFeedback threshold (warn after >= 3) so a
 * persistently misbehaving (or malicious) language server whose diagnostics keep
 * failing to process is surfaced instead of being silently swallowed forever.
 */
const DIAGNOSTIC_FAILURE_WARN_THRESHOLD = 3;

/**
 * The BCP-47 locale AMI advertises to a language server in the `initialize`
 * handshake, derived from the process environment (LC_ALL > LC_MESSAGES > LANG),
 * stripped of any `.encoding`/`@modifier` suffix (`en_US.UTF-8` → `en_US`).
 * Falls back to `en` when unset. Mirrors Claude Code's `locale` InitializeParam,
 * which lets a server localize its diagnostic messages.
 */
export function lspLocale(env: NodeJS.ProcessEnv = process.env): string {
  const raw = env.LC_ALL || env.LC_MESSAGES || env.LANG || '';
  const base = raw.split('.')[0].split('@')[0].trim();
  return base.length > 0 && base !== 'C' && base !== 'POSIX' ? base : 'en';
}

/**
 * Lifecycle state of a single language server, mirroring Claude Code's
 * `LspServerState` (services/lsp/LSPServerInstance.ts). `error` is retained after
 * a crash so the failure reason stays attributable once the live process is gone.
 */
export type LspServerState = 'stopped' | 'starting' | 'running' | 'stopping' | 'error';

/**
 * Introspection snapshot of one language server's lifecycle — the AMI analogue of
 * Claude's `LSPServerInstance` readonly getters (`state`, `startTime`, `lastError`,
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
   * Claude Code's passiveFeedback per-server `diagnosticFailures` counter; once it
   * reaches `DIAGNOSTIC_FAILURE_WARN_THRESHOLD` a warning is logged. Surfaced by
   * `getServerStatus`/`getAllServerStatus` so `/doctor` can flag a flapping server.
   */
  consecutiveDiagnosticFailures: number;
}

/**
 * Client-wide initialization status, mirroring Claude Code's manager-level
 * `getInitializationStatus()` discriminated union — lets a UI/tool report WHY LSP
 * is (un)available instead of a bare boolean.
 */
export type LspInitializationStatus =
  | { status: 'not-started' }
  | { status: 'pending' }
  | { status: 'success' }
  | { status: 'failed'; error: string };

type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (reason: Error) => void;
  timer: ReturnType<typeof setTimeout>;
};

interface ServerConn {
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

export class LSPClient implements DiagnosticSource {
  private servers: Map<string, ServerConn> = new Map();
  private openedDocuments: Set<string> = new Set();
  /** Monotonic per-document version counter (LSP requires strictly increasing
   * versions per `didChange`; keyed by the canonical document URI). */
  private documentVersions: Map<string, number> = new Map();
  /** Per-language crash-recovery counter (reset to 0 on a successful init). */
  private crashCounts: Map<string, number> = new Map();
  /**
   * In-flight `initialize` handshakes keyed by language. A server is added to the
   * `servers` map BEFORE its handshake completes (so `onData` can route the
   * server's `initialize` reply), which means a second concurrent caller would
   * otherwise observe a half-open entry and fail in `sendRequest` with
   * "server is starting". Tracking the handshake promise lets every concurrent
   * `ensureServer`/`request` for the same language await the SAME result — the
   * AMI analogue of Claude Code's generation-guarded init + `waitForInitialization`.
   */
  private handshakes: Map<string, Promise<boolean>> = new Map();
  /**
   * Monotonic initialization generation, bumped by {@link shutdown}. A `startServer`
   * handshake captures the generation at its start and re-checks it after every
   * `await`; if `shutdown()` (or any teardown) bumped it while the handshake was in
   * flight, the now-STALE handshake is abandoned instead of resolving into — and
   * clobbering — the newer client state (e.g. re-recording a server as `error`
   * after shutdown already cleared it). This is the AMI analogue of Claude Code's
   * `initializationGeneration` guard (services/lsp/manager.ts): the stale promise's
   * resolution becomes a no-op. Without it, a shutdown mid-handshake leaves a
   * phantom `failed` server in `getInitializationStatus()`/`getServerStatus()`.
   */
  private generation = 0;
  /**
   * Retained per-language lifecycle status. Unlike the live `servers` map (which a
   * crash removes), this survives a crash so the failure state + reason stay
   * attributable — the AMI analogue of Claude's per-`LSPServerInstance` state.
   */
  private serverStatus: Map<string, LspServerStatus> = new Map();
  /** Number of ensureServer handshakes currently in flight (drives init 'pending'). */
  private pendingInits = 0;
  /** Latest diagnostics per normalized path, from publishDiagnostics. */
  private diagnosticsByUri: Map<string, DiagnosticFile> = new Map();
  /** One-shot waiters resolved when the next publishDiagnostics for a path arrives. */
  private diagnosticWaiters: Map<string, Array<() => void>> = new Map();
  /** Client-level notification handlers (apply to every server; survive respawns). */
  private notificationHandlers: Map<string, NotificationHandler[]> = new Map();
  /** Client-level server→client request handlers. */
  private requestHandlers: Map<string, RequestHandler> = new Map();
  /**
   * Per-language consecutive `publishDiagnostics` processing-failure tracker
   * ({count,lastError}). Incremented when delivering a diagnostics notification
   * throws, reset on the next success — the AMI analogue of Claude's
   * passiveFeedback `diagnosticFailures` map. Surfaced in the per-server status.
   */
  private diagnosticFailures: Map<string, { count: number; lastError: string }> = new Map();
  /**
   * Background health-poll loop (lazily created on the first successful init). The
   * lazy LSP client only (re)checks a server when a tool touches a matching file;
   * this loop re-checks on a fixed interval so a server that crashes BETWEEN tool
   * calls — i.e. after a successful launch — is still surfaced, and gives up once
   * the client has definitively failed. Parity with Claude Code's
   * useLspInitializationNotification polling. See {@link startHealthMonitoring}.
   */
  private healthMonitor?: LspHealthMonitor;

  /** Baseline/diff tracker reading diagnostics from this client. */
  readonly diagnostics = new DiagnosticTracker(this);

  constructor() {
    // Default responder for the server→client `workspace/configuration` request.
    // We advertise `workspace.configuration: false` so conformant servers never
    // ask, but some (e.g. older gopls / misconfigured servers) request it anyway
    // and will HANG waiting for a reply. Return `null` per requested item (the
    // LSP-sanctioned "no configuration" answer) so the server proceeds instead of
    // stalling. A caller's explicit `onRequest('workspace/configuration', …)`
    // overrides this default.
    this.requestHandlers.set('workspace/configuration', (params) => {
      const items = (params as { items?: unknown[] } | undefined)?.items;
      return Array.isArray(items) ? items.map(() => null) : [];
    });
  }

  // Process-spawn hook (overridable in tests). Production behavior is identical
  // to calling child_process directly.
  _spawn: typeof child_process.spawn = child_process.spawn;
  /**
   * Binary-presence check (overridable in tests). Delegates by default to the
   * ONE canonical cross-platform, session-cached `isBinaryInstalled` — this is a
   * thin DI seam, NOT a second implementation of `which`/`where`. Tests inject a
   * fixed boolean so the spawn path can be driven without a real server on PATH.
   */
  _isBinaryInstalled: (command: string) => boolean = isBinaryInstalled;
  /** Overridable sleep for backoff (tests make retries instant). */
  _sleep: (ms: number) => Promise<void> = (ms) =>
    new Promise((r) => {
      const t = setTimeout(r, ms);
      if (typeof (t as { unref?: () => void }).unref === 'function') (t as { unref: () => void }).unref();
    });

  /** Register a handler for a server-pushed notification method. */
  onNotification(method: string, handler: NotificationHandler): void {
    const list = this.notificationHandlers.get(method) ?? [];
    list.push(handler);
    this.notificationHandlers.set(method, list);
    this.logLazyHandler('notification', method);
  }

  /** Register a handler for a server→client request method. */
  onRequest(method: string, handler: RequestHandler): void {
    this.requestHandlers.set(method, handler);
    this.logLazyHandler('request', method);
  }

  /**
   * Lazy-handler affordance (parity with Claude Code's LSPClient "Queued … handler
   * (connection not ready)" / "Applied queued … handler" debug hints): AMI applies
   * client-level handlers at dispatch time rather than queuing them per connection,
   * so a handler registered BEFORE any server is connected is not lost — but the
   * ordering was previously invisible. Emit a debug line noting the handler will
   * apply once a server initializes, making the lazy-init flow observable.
   */
  private logLazyHandler(kind: 'notification' | 'request', method: string): void {
    if (this.isConnected()) return;
    log(
      'lsp',
      `[LSP] ${kind} handler "${method}" registered before any server is connected; it will apply once a server initializes`,
      undefined,
      'debug',
    );
  }

  private configFor(language: string): LspServerConfig | undefined {
    return getAllLspServers()[language];
  }

  /**
   * Canonical `file://` URI for a filesystem path. Uses `pathToFileURL` so the
   * path is resolved absolutely and percent-encoded (spaces, unicode, `#`, `?`,
   * …) — raw `file://` + path interpolation produces malformed URIs that servers
   * like gopls/pyright reject or mis-key. Inverse is `uriToPath`.
   */
  private fileUri(filePath: string): string {
    return pathToFileURL(filePath).href;
  }

  /**
   * Resolve once the child has successfully spawned; reject if it fails to spawn.
   * Listeners are one-shot and mutually cleaned up so neither leaks. A child that
   * exits immediately after a successful spawn still resolves here — the `spawn`
   * event always precedes `exit`, and the crash is handled by the `exit`/`error`
   * handlers wired by the caller.
   */
  private awaitSpawn(proc: child_process.ChildProcess): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const cleanup = (): void => {
        proc.removeListener('spawn', onSpawn);
        proc.removeListener('error', onError);
      };
      const onSpawn = (): void => { cleanup(); resolve(); };
      const onError = (err: Error): void => { cleanup(); reject(err); };
      proc.once('spawn', onSpawn);
      proc.once('error', onError);
    });
  }

  /**
   * Ensure the language server for `language` is connected and its initialize
   * handshake has completed, returning `true` only once it is genuinely ready.
   *
   * Concurrency: a fully-initialized server is reused immediately; otherwise
   * callers that arrive while a handshake is still in flight await the SAME
   * handshake promise (no duplicate spawn, no half-open "starting" race). This is
   * the init-pending await Claude's manager performs before answering a request.
   */
  async ensureServer(language: string, cwd: string): Promise<boolean> {
    if (this.servers.get(language)?.initialized) return true;
    const inflight = this.handshakes.get(language);
    if (inflight) return inflight;
    const started = this.startServer(language, cwd);
    this.handshakes.set(language, started);
    try {
      return await started;
    } finally {
      this.handshakes.delete(language);
    }
  }

  /**
   * Spawn + handshake a single language server. Only ever invoked (and serialized
   * per language) through {@link ensureServer}, which de-duplicates concurrent
   * callers via the `handshakes` map; callers must not invoke this directly.
   */
  private async startServer(language: string, cwd: string): Promise<boolean> {
    const config = this.configFor(language);
    if (!config) return false;

    // Capture the init generation so every `await` below can detect a `shutdown()`
    // that raced this handshake and abandon the now-stale start (see `generation`).
    const generation = this.generation;

    // Only stdio transport is implemented. `socket` is a valid config value
    // (Claude-Code schema parity) but we refuse to spawn rather than silently
    // treat it as stdio, so a misconfiguration fails loudly and visibly.
    if (config.transport && config.transport !== 'stdio') {
      this.recordError(
        language,
        `LSP server "${language}" transport "${config.transport}" is not supported (only "stdio")`,
      );
      return false;
    }

    // Supply-chain guard: never let npx auto-fetch a server from the registry.
    // A violating config is a MISCONFIGURATION the operator must see — record it
    // on the server status AND surface it through the shared error logger (read by
    // `/doctor`) instead of swallowing it silently. Parity with Claude Code's LSP
    // discovery, which never throws (LSP is optional) but logs every failure "to
    // improve the feature" rather than failing invisibly.
    try {
      assertNoAutoFetch(config);
    } catch (e) {
      this.recordError(language, (e as Error).message);
      return false;
    }

    // Crash-recovery cap: refuse to respawn a persistently crashing server. The
    // crash counter doubles as Claude's `restartCount` — each respawn after a
    // crash bumps it, and exceeding `maxRestarts` leaves the server in `error`.
    const maxRestarts = config.maxRestarts ?? DEFAULT_MAX_RESTARTS;
    const restartCount = this.crashCounts.get(language) ?? 0;
    // Honor `restartOnCrash: false` — a server that has crashed at least once
    // stays down for the rest of the session instead of being respawned on the
    // next incidental file event (parity with Claude's `restartOnCrash` knob).
    if (restartCount > 0 && config.restartOnCrash === false) {
      this.recordError(
        language,
        `LSP server "${language}" crashed and restartOnCrash is disabled; not respawning`,
      );
      return false;
    }
    if (restartCount > maxRestarts) {
      this.recordError(
        language,
        `LSP server "${language}" exceeded max crash recovery attempts (${maxRestarts})`,
      );
      return false;
    }

    this.pendingInits++;
    this.setStatus(language, { state: 'starting', startTime: Date.now(), restartCount });
    try {
      // Cross-platform, session-cached binary-presence check (`which` on POSIX,
      // `where.exe` on Windows). An empty/whitespace command resolves to false
      // without a filesystem probe. Shared with every other binary check in core.
      if (!this._isBinaryInstalled(config.command)) {
        this.recordError(language, `LSP server "${language}" command not found: ${config.command}`);
        return false;
      }

      // Lifecycle introspection: a "starting" debug line so the on-demand launch
      // of a language server is observable (which binary, with which args) — the
      // AMI analogue of Claude Code's per-server load logging. Pairs with the
      // `initialized` line below and the `command not found` error above so a
      // started / missing / failed server is distinguishable in the debug log.
      log('lsp', `[LSP ${language}] starting server (command=${config.command} ${(config.args ?? []).join(' ')})`.trimEnd(), undefined, 'debug');

      const proc = this._spawn(config.command, config.args ?? [], {
        cwd: config.workspaceFolder ?? cwd,
        stdio: ['pipe', 'pipe', 'pipe'],
        // No console window for the child on Windows (headless subprocess).
        windowsHide: true,
        // Scrub credential-bearing env from the untrusted language-server child,
        // then layer any per-server env. Reuses the canonical bash scrub helper.
        env: { ...sanitizeEnvForSubprocess({ ...process.env }), ...config.env },
      });

      // Exit handler: distinguish an intentional stop (gracefulStop removes these
      // listeners first) from a real crash. On a non-zero/non-null exit code we
      // build a `crashed with exit code N` error, log it, and retain it as the
      // server's lastError so the crash reason survives the connection teardown.
      const onExit = (code: number | null) => {
        this.crashCounts.set(language, (this.crashCounts.get(language) ?? 0) + 1);
        const crashed = code !== 0 && code !== null;
        const message = crashed
          ? `LSP server "${language}" crashed with exit code ${code}`
          : `LSP server "${language}" exited`;
        this.recordError(language, message, code);
        this.clearServer(language);
      };
      // Spawn-time failure (ENOENT/EACCES surfacing after spawn) — record as the
      // start failure, not a crash. `awaitSpawn` also rejects so the handshake bails.
      const onError = (err: Error) => {
        this.crashCounts.set(language, (this.crashCounts.get(language) ?? 0) + 1);
        this.recordError(language, `LSP server "${language}" failed to start: ${err.message}`);
        this.clearServer(language);
      };
      proc.on('error', onError);
      proc.on('exit', onExit);
      proc.stdin?.on('error', () => {});
      // Surface the language-server's own stderr as a named diagnostic log line
      // (`[LSP SERVER <language>] …`) instead of swallowing it — a misbehaving
      // server's error text (missing deps, config errors) becomes diagnosable.
      proc.stderr?.on('data', (data: Buffer) => {
        const output = data.toString().trim();
        if (output) log('lsp', `[LSP SERVER ${language}] ${output}`);
      });
      if (!proc.pid) {
        this.recordError(language, `LSP server "${language}" failed to spawn`);
        return false;
      }

      const conn: ServerConn = {
        language,
        proc,
        buffer: Buffer.alloc(0),
        nextId: 0,
        pending: new Map(),
        capabilities: undefined,
        initialized: false,
      };
      proc.stdout?.on('data', (chunk: Buffer) => this.onData(language, chunk));
      this.servers.set(language, conn);

      // Await the child's `spawn` event (racing it against the async `error`
      // event) BEFORE writing to its stdio. `_spawn` returns synchronously, but a
      // spawn failure (ENOENT for a missing binary, EACCES, …) surfaces later on
      // the `error` event; writing the initialize frame before spawn is confirmed
      // races that failure and produces writes to a stream that is about to error.
      // Waiting here deterministically catches spawn failure as a start error.
      await this.awaitSpawn(proc);

      // A `shutdown()` may have raced the spawn — if so this handshake is stale;
      // abandon it without recording a spurious error over the cleared state.
      if (this.isStale(generation)) {
        this.abandonStale(language);
        return false;
      }

      // The child may have died the instant after it spawned (a server that
      // crashes on startup). Writing the initialize frame into a dead stream would
      // just block until the request timeout, so bail out fast instead. `onGone`
      // (exit/error) has already bumped the crash counter and dropped the server.
      if (this.servers.get(language) !== conn || proc.exitCode !== null || proc.signalCode !== null) {
        this.recordError(language, `LSP server "${language}" exited during startup`);
        this.clearServer(language);
        return false;
      }

      // Handshake: await initialize, capture capabilities, send `initialized`,
      // THEN flip initialized. Matches the LSP spec ordering so servers like
      // gopls/rust-analyzer accept subsequent didOpen/didChange.
      const workspaceFolder = config.workspaceFolder ?? cwd;
      // Bound the initialize handshake by the server's configured startupTimeout
      // (falling back to the default request timeout) so a slow/hanging server
      // start fails within the operator-configured window.
      const result = await this.rawRequest(
        conn,
        'initialize',
        this.initializeParams(workspaceFolder, config),
        config.startupTimeout ?? DEFAULT_REQUEST_TIMEOUT_MS,
      );
      // A `shutdown()` while the initialize was in flight makes this result stale;
      // discard it so it cannot flip a torn-down server back to `running`.
      if (this.isStale(generation)) {
        this.abandonStale(language);
        return false;
      }
      conn.capabilities = (result as { capabilities?: unknown })?.capabilities;
      this.rawNotify(conn, 'initialized', {});
      conn.initialized = true;
      // Push per-server settings via workspace/didChangeConfiguration once the
      // handshake is complete (LSP requires the server be initialized first).
      // Mirrors Claude Code's per-server `settings` knob.
      if (config.settings !== undefined) {
        this.rawNotify(conn, 'workspace/didChangeConfiguration', { settings: config.settings });
      }
      this.crashCounts.set(language, 0);
      // Successful handshake: reset restartCount and mark the server running.
      this.setStatus(language, { state: 'running', startTime: Date.now(), restartCount: 0, lastError: undefined, exitCode: undefined });
      log('lsp', `LSP server "${language}" initialized`);
      // A server is now live — start the background health-poll loop (idempotent)
      // so a post-startup crash between tool calls is caught instead of leaving a
      // silently-dead server. `shutdown()` stops it.
      this.startHealthMonitoring();
      return true;
    } catch (e) {
      // A `shutdown()` that killed the server mid-handshake surfaces here as a
      // rejected initialize. That is an EXPECTED consequence of teardown, not a
      // server failure — abandon the stale handshake silently rather than writing
      // a phantom `error` status over state shutdown already cleared.
      if (this.isStale(generation)) {
        this.abandonStale(language);
        return false;
      }
      // Don't clobber a more specific error already recorded by the exit handler
      // (e.g. "crashed with exit code N") — the generic init-failure wrapper here
      // fires second when a crash rejected the in-flight initialize request.
      if (this.serverStatus.get(language)?.state !== 'error') {
        this.recordError(language, `LSP server "${language}" initialize failed: ${(e as Error).message}`);
      }
      this.clearServer(language);
      return false;
    } finally {
      this.pendingInits = Math.max(0, this.pendingInits - 1);
    }
  }

  /**
   * True when the init generation captured at the start of a handshake no longer
   * matches the current one — i.e. a `shutdown()` raced the handshake and its
   * result/rejection must be treated as a no-op (see {@link generation}).
   */
  private isStale(generation: number): boolean {
    return this.generation !== generation;
  }

  /**
   * Tear down the residue of a stale handshake (one invalidated by a concurrent
   * `shutdown()`): drop the half-open connection AND any per-language bookkeeping
   * the aborted start had begun to write (status/crash/diagnostic-failure), so the
   * cleared client state is not left with a phantom entry for a server that was
   * shut down. Idempotent and safe when the entries are already gone.
   */
  private abandonStale(language: string): void {
    this.clearServer(language);
    this.serverStatus.delete(language);
    this.crashCounts.delete(language);
    this.diagnosticFailures.delete(language);
  }

  /** Merge a partial status update into the retained per-language status record. */
  private setStatus(language: string, patch: Partial<LspServerStatus>): void {
    const prev = this.serverStatus.get(language);
    const next: LspServerStatus = {
      language,
      state: patch.state ?? prev?.state ?? 'stopped',
      startTime: 'startTime' in patch ? patch.startTime : prev?.startTime,
      lastError: 'lastError' in patch ? patch.lastError : prev?.lastError,
      restartCount: patch.restartCount ?? prev?.restartCount ?? 0,
      exitCode: 'exitCode' in patch ? patch.exitCode : prev?.exitCode,
      isHealthy: false,
      consecutiveDiagnosticFailures: this.diagnosticFailures.get(language)?.count ?? 0,
    };
    next.isHealthy = next.state === 'running' && this.servers.get(language)?.initialized === true;
    this.serverStatus.set(language, next);
  }

  /**
   * Record a server error: flip the retained status to `error`, keep the message
   * as `lastError`, and surface it through the shared logger (which also captures
   * it in the in-memory error ring that `/doctor` reads).
   */
  private recordError(language: string, message: string, exitCode?: number | null): void {
    this.setStatus(language, { state: 'error', lastError: message, exitCode });
    logError('lsp', message);
  }

  private initializeParams(workspaceFolder: string, config: LspServerConfig): unknown {
    const workspaceUri = this.fileUri(workspaceFolder);
    return {
      processId: process.pid,
      // Identify AMI (name + real version) to the server, matching the full LSP
      // InitializeParams Claude sends. Reuses the single version source of truth
      // (also used for the MCP handshake) — no second hardcoded version string.
      clientInfo: { ...AMI_CLIENT_INFO },
      // BCP-47 locale so a server can localize its diagnostic messages.
      locale: lspLocale(),
      // We do not consume `$/logTrace`, so request no protocol tracing.
      trace: 'off',
      // Required by vue-language-server, harmless default elsewhere.
      initializationOptions: config.initializationOptions ?? {},
      // LSP 3.16+ — required by Pyright/gopls.
      workspaceFolders: [{ uri: workspaceUri, name: path.basename(workspaceFolder) }],
      // Deprecated but still needed by some servers for URI resolution.
      rootPath: workspaceFolder,
      rootUri: workspaceUri,
      capabilities: {
        workspace: { configuration: false, workspaceFolders: false },
        textDocument: {
          synchronization: {
            dynamicRegistration: false,
            willSave: false,
            willSaveWaitUntil: false,
            didSave: true,
          },
          publishDiagnostics: {
            relatedInformation: true,
            tagSupport: { valueSet: [1, 2] },
            versionSupport: false,
            codeDescriptionSupport: true,
            dataSupport: false,
          },
          hover: { dynamicRegistration: false, contentFormat: ['markdown', 'plaintext'] },
          definition: { dynamicRegistration: false, linkSupport: true },
          references: { dynamicRegistration: false },
          implementation: { dynamicRegistration: false, linkSupport: true },
          documentSymbol: { dynamicRegistration: false, hierarchicalDocumentSymbolSupport: true },
          callHierarchy: { dynamicRegistration: false },
        },
        general: { positionEncodings: ['utf-16'] },
      },
    };
  }

  /** Server capabilities captured from the initialize result (undefined until ready). */
  getCapabilities(language: string): unknown {
    return this.servers.get(language)?.capabilities;
  }

  /** True only after a successful initialize + `initialized` for the language. */
  isInitialized(language: string): boolean {
    return this.servers.get(language)?.initialized === true;
  }

  /**
   * Health predicate backing tool affordances: true when at least one language
   * server is connected and initialized. Mirrors Claude's `isLspConnected()`.
   */
  isConnected(): boolean {
    for (const conn of this.servers.values()) {
      if (conn.initialized) return true;
    }
    return false;
  }

  /**
   * Health predicate for a single language server, mirroring Claude's
   * `LSPServerInstance.isHealthy()`: true only when the server is running AND its
   * initialize handshake completed. Used to gate requests with a clear error.
   */
  isHealthy(language: string): boolean {
    const status = this.serverStatus.get(language);
    return status?.state === 'running' && this.servers.get(language)?.initialized === true;
  }

  /**
   * Introspection snapshot for one language server, or `undefined` if no server
   * for that language has ever been attempted. Mirrors Claude's per-instance
   * readonly getters (state/startTime/lastError/restartCount) + isHealthy.
   */
  getServerStatus(language: string): LspServerStatus | undefined {
    const status = this.serverStatus.get(language);
    if (!status) return undefined;
    // `isHealthy` and the diagnostic-failure streak are derived live so they
    // reflect the current connection, not a snapshot taken when the status was
    // last written.
    return {
      ...status,
      isHealthy: this.isHealthy(language),
      consecutiveDiagnosticFailures: this.diagnosticFailures.get(language)?.count ?? 0,
    };
  }

  /** Introspection snapshot for every language server attempted this session. */
  getAllServerStatus(): LspServerStatus[] {
    return [...this.serverStatus.keys()].map((lang) => this.getServerStatus(lang)!);
  }

  /**
   * Client-wide initialization status, mirroring Claude's manager-level
   * `getInitializationStatus()`: `pending` while a handshake is in flight,
   * `success` once any server is connected, `failed` (with the captured error)
   * when the last attempt(s) failed and none is connected, else `not-started`.
   */
  getInitializationStatus(): LspInitializationStatus {
    if (this.isConnected()) return { status: 'success' };
    if (this.pendingInits > 0) return { status: 'pending' };
    let error: string | undefined;
    for (const status of this.serverStatus.values()) {
      if (status.state === 'error' && status.lastError) error = status.lastError;
    }
    if (error) return { status: 'failed', error };
    return { status: 'not-started' };
  }

  /**
   * Start the background LSP health-poll loop (idempotent). Driven by this
   * client's own {@link getInitializationStatus}/{@link getAllServerStatus}, it
   * re-checks health on a fixed interval ({@link LspHealthMonitor}) so a server
   * that crashes AFTER a successful launch is still surfaced, deduped per source,
   * and gives up once the client has definitively failed. Started automatically on
   * the first successful handshake; callers (and tests) may also start it with a
   * custom `onError` sink and/or an injectable scheduler. The default sink routes
   * to the shared error logger (read by `/doctor`).
   */
  startHealthMonitoring(onError?: (error: LspHealthError) => void, scheduler?: HealthScheduler): void {
    if (!this.healthMonitor) {
      this.healthMonitor = new LspHealthMonitor({
        getStatus: () => this.getInitializationStatus(),
        getServers: () => this.getAllServerStatus(),
        onError,
        scheduler,
      });
    }
    this.healthMonitor.start();
  }

  /** Stop the background health-poll loop (idempotent). */
  stopHealthMonitoring(): void {
    this.healthMonitor?.stop();
  }

  /** True while the background health-poll loop is active. Introspection seam. */
  isHealthMonitoringActive(): boolean {
    return this.healthMonitor?.isRunning() ?? false;
  }

  // ---- inbound message framing / dispatch -------------------------------

  private onData(language: string, chunk: Buffer): void {
    const conn = this.servers.get(language);
    if (!conn) return;
    conn.buffer = Buffer.concat([conn.buffer, chunk]);
    // Drain as many complete Content-Length framed messages as are buffered.
    for (;;) {
      const headerEnd = conn.buffer.indexOf('\r\n\r\n');
      if (headerEnd === -1) return;
      const header = conn.buffer.subarray(0, headerEnd).toString('utf-8');
      const match = /Content-Length:\s*(\d+)/i.exec(header);
      if (!match) {
        // Unparseable header — drop up to the separator and resync.
        conn.buffer = conn.buffer.subarray(headerEnd + 4);
        continue;
      }
      const length = Number(match[1]);
      const bodyStart = headerEnd + 4;
      if (conn.buffer.length < bodyStart + length) return; // wait for more data
      const body = conn.buffer.subarray(bodyStart, bodyStart + length).toString('utf-8');
      conn.buffer = conn.buffer.subarray(bodyStart + length);
      let message: unknown;
      try {
        message = JSON.parse(body);
      } catch {
        continue; // isolate a bad frame; keep the loop alive
      }
      try {
        this.dispatch(language, conn, message as Record<string, unknown>);
      } catch {
        // Isolate handler errors to this message — never break the read loop.
      }
    }
  }

  private dispatch(language: string, conn: ServerConn, message: Record<string, unknown>): void {
    const hasId = 'id' in message && message.id !== null && message.id !== undefined;
    const hasMethod = typeof message.method === 'string';

    if (hasId && !hasMethod) {
      // Response to one of our requests.
      const id = message.id as number;
      const entry = conn.pending.get(id);
      if (!entry) return;
      conn.pending.delete(id);
      clearTimeout(entry.timer);
      if (message.error) {
        const err = message.error as { code?: number; message?: string };
        const e = new Error(err.message ?? 'LSP request failed') as Error & { code?: number };
        e.code = err.code;
        entry.reject(e);
      } else {
        entry.resolve(message.result);
      }
      return;
    }

    if (hasMethod && hasId) {
      // Server→client request — respond so the server isn't left waiting.
      this.handleServerRequest(conn, message);
      return;
    }

    if (hasMethod) {
      // Notification.
      const method = message.method as string;
      const params = message.params;
      if (method === 'textDocument/publishDiagnostics') {
        // Diagnostics delivery (storage + registered handlers) runs under the
        // per-server consecutive-failure tracker, so a server whose diagnostics
        // keep failing to process is surfaced instead of silently swallowed.
        this.deliverDiagnostics(language, params);
        return;
      }
      for (const handler of this.notificationHandlers.get(method) ?? []) {
        try { handler(params); } catch { /* isolate handler errors */ }
      }
    }
  }

  /**
   * Deliver a `publishDiagnostics` notification: store the diagnostics and fan
   * out to any registered handlers, tracking CONSECUTIVE per-server failures.
   * A successful delivery resets the server's failure streak; a throwing handler
   * (or storage error) increments it and, past the threshold, logs a warning —
   * mirroring Claude Code's passiveFeedback diagnostic-failure circuit breaker.
   * Errors never break the read loop (the caller's try/catch is a second net).
   */
  private deliverDiagnostics(language: string, params: unknown): void {
    try {
      this.onPublishDiagnostics(language, params);
      for (const handler of this.notificationHandlers.get('textDocument/publishDiagnostics') ?? []) {
        handler(params);
      }
      // Success — clear the consecutive-failure streak for this server.
      this.diagnosticFailures.delete(language);
    } catch (e) {
      this.recordDiagnosticFailure(language, (e as Error).message);
    }
  }

  /**
   * Increment a server's consecutive diagnostic-failure count, retain the last
   * error, and emit a loud warning once the count reaches
   * `DIAGNOSTIC_FAILURE_WARN_THRESHOLD` (parity with Claude's warn-after-3). The
   * warning routes through the shared logger (the in-memory ring `/doctor` reads).
   */
  private recordDiagnosticFailure(language: string, message: string): void {
    const prev = this.diagnosticFailures.get(language) ?? { count: 0, lastError: '' };
    const next = { count: prev.count + 1, lastError: message };
    this.diagnosticFailures.set(language, next);
    logError('lsp', `LSP server "${language}" diagnostic delivery failed: ${message}`);
    if (next.count >= DIAGNOSTIC_FAILURE_WARN_THRESHOLD) {
      logError(
        'lsp',
        `WARNING: LSP diagnostic handler for "${language}" has failed ${next.count} times consecutively. ` +
          `Last error: ${next.lastError}. This may indicate a problem with the LSP server or diagnostic processing.`,
      );
    }
  }

  private handleServerRequest(conn: ServerConn, message: Record<string, unknown>): void {
    const method = message.method as string;
    const id = message.id;
    const handler = this.requestHandlers.get(method);
    const respond = (payload: Record<string, unknown>) => {
      try {
        const body = JSON.stringify({ jsonrpc: '2.0', id, ...payload });
        conn.proc.stdin?.write(`Content-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`);
      } catch { /* server gone */ }
    };
    if (!handler) {
      respond({ error: { code: -32601, message: `Method not found: ${method}` } });
      return;
    }
    Promise.resolve()
      .then(() => handler(message.params))
      .then(
        (result) => respond({ result: result ?? null }),
        (err: Error) => respond({ error: { code: -32603, message: err.message } }),
      );
  }

  /**
   * Handle a `textDocument/publishDiagnostics` notification. A JSON-RPC
   * notification from an LSP subprocess is UNTRUSTED input, so the params are
   * structurally validated before use: a non-null object carrying both a string
   * `uri` AND an array `diagnostics`. Malformed payloads are logged (with the
   * server's language for attribution) and dropped rather than being cast blindly
   * — parity with Claude Code's passiveFeedback validation. An empty diagnostics
   * array is still stored: it is how a server clears a file's previous
   * diagnostics, which the baseline/diff tracker depends on.
   */
  private onPublishDiagnostics(language: string, params: unknown): void {
    if (
      !params ||
      typeof params !== 'object' ||
      typeof (params as { uri?: unknown }).uri !== 'string' ||
      !Array.isArray((params as { diagnostics?: unknown }).diagnostics)
    ) {
      logError('lsp', `LSP server "${language}" sent invalid diagnostic params (missing uri or diagnostics)`);
      return;
    }
    const valid = params as PublishDiagnosticsParams;
    const key = normalizeDiagnosticPath(valid.uri);
    this.diagnosticsByUri.set(key, {
      uri: valid.uri,
      diagnostics: convertPublishDiagnostics(valid),
    });
    const waiters = this.diagnosticWaiters.get(key);
    if (waiters) {
      this.diagnosticWaiters.delete(key);
      for (const w of waiters) w();
    }
  }

  // ---- outbound request / notification ----------------------------------

  /** Low-level framed request against an already-connected server. */
  private rawRequest<T = unknown>(conn: ServerConn, method: string, params: unknown, timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS): Promise<T> {
    const id = ++conn.nextId;
    const body = JSON.stringify({ jsonrpc: '2.0', id, method, params });
    // JSON-RPC protocol trace (verbose; filtered out by default), mirroring
    // Claude's `[LSP PROTOCOL <name>]` tracing hook for deep debugging.
    log('lsp', `[LSP PROTOCOL ${conn.language}] → request #${id} ${method}`, undefined, 'verbose');
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        conn.pending.delete(id);
        reject(new Error(`LSP request "${method}" timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      if (typeof (timer as { unref?: () => void }).unref === 'function') (timer as { unref: () => void }).unref();
      conn.pending.set(id, { resolve: resolve as (v: unknown) => void, reject, timer });
      try {
        conn.proc.stdin?.write(`Content-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`);
      } catch (e) {
        conn.pending.delete(id);
        clearTimeout(timer);
        reject(e as Error);
      }
    });
  }

  /**
   * Fire-and-forget framed notification. Returns `true` on a successful stdin
   * write, `false` when the write throws (server gone / broken pipe). Lifecycle
   * notifications (initialized/exit/didClose/didChangeConfiguration) ignore the
   * result; document-sync notifications route through `syncNotify` so the failure
   * is propagated to the caller instead of silently dropped.
   */
  private rawNotify(conn: ServerConn, method: string, params: unknown): boolean {
    const body = JSON.stringify({ jsonrpc: '2.0', method, params });
    log('lsp', `[LSP PROTOCOL ${conn.language}] → notify ${method}`, undefined, 'verbose');
    try {
      conn.proc.stdin?.write(`Content-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`);
      return true;
    } catch (e) {
      // A write failure (server gone mid-sync) is worth a contextual debug line
      // instead of a silent swallow; the boolean lets sync callers propagate it.
      log('lsp', `LSP server "${conn.language}" notification "${method}" failed: ${(e as Error).message}`);
      return false;
    }
  }

  /**
   * Document-sync notification (didOpen/didChange/didSave) whose write failure
   * MUST NOT be silently swallowed: on a failed stdin write we record a contextual
   * error on the server's status (so `/doctor` surfaces the desync) and throw so
   * the caller learns the document is no longer in sync — parity with Claude's
   * LSPServerManager, which logs `Failed to sync file <action> <path>` and
   * re-throws rather than leaving AMI believing the document is synced.
   */
  private syncNotify(conn: ServerConn, method: string, params: unknown, action: string, filePath: string): void {
    if (this.rawNotify(conn, method, params)) return;
    const message = `Failed to sync file ${action} ${filePath}: LSP server "${conn.language}" write failed`;
    this.recordError(conn.language, message);
    throw new Error(message);
  }

  /**
   * Send an LSP request to the server for `language`, returning the server's
   * actual result. Rejects on server error or timeout. Transient
   * ContentModified (-32801) errors are retried with exponential backoff.
   */
  async sendRequest<T = unknown>(language: string, method: string, params: unknown, timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS): Promise<T> {
    const conn = this.servers.get(language);
    if (!conn || !conn.initialized) {
      // Contextual, Claude-parity wording: name the server, its current lifecycle
      // state, and its retained lastError so the failure is attributable.
      const status = this.serverStatus.get(language);
      const state = status?.state ?? 'stopped';
      const lastError = status?.lastError ? `, last error: ${status.lastError}` : '';
      const message = `Cannot send request to LSP server "${language}": server is ${state}${lastError}`;
      logError('lsp', message);
      throw new Error(message);
    }
    try {
      return await retryWithBackoff<T>(() => this.rawRequest<T>(conn, method, params, timeoutMs), {
        retries: 3,
        baseMs: 500,
        sleep: this._sleep,
        isRetryable: (e) => (e as { code?: number }).code === LSP_CONTENT_MODIFIED,
        // Progress/affordance hint for a transient ContentModified retry: the
        // server's state changed mid-request (e.g. still indexing). Surface the
        // attempt counter + backoff delay so a stalled request is diagnosable
        // instead of silently waiting. Mirrors Claude's retry progress logging.
        onRetry: ({ attempt, delayMs }) =>
          log('lsp', `LSP server "${language}" request "${method}" content modified; retry ${attempt}/3 in ${delayMs}ms`),
      });
    } catch (e) {
      // Wrap the server/timeout error with the server name + method for context,
      // matching Claude's `LSP server <name> request <method> failed: <msg>`.
      const message = `LSP server "${language}" request "${method}" failed: ${(e as Error).message}`;
      logError('lsp', message);
      throw new Error(message);
    }
  }

  /**
   * Position-addressed request keyed by file path: resolves the language,
   * ensures the server is up and the document is open, then forwards the
   * request. Returns `undefined` (not a throw) when no server handles the file
   * type — matching Claude's "no LSP server available" fallback.
   */
  async request<T = unknown>(filePath: string, cwd: string, method: string, params: unknown): Promise<T | undefined> {
    const lang = resolveLanguageForExtension(path.extname(filePath));
    if (!lang) return undefined;
    const ok = await this.ensureServer(lang, cwd);
    if (!ok) return undefined;
    const conn = this.servers.get(lang)!;
    this.ensureDidOpen(conn, filePath);
    return this.sendRequest<T>(lang, method, params);
  }

  // ---- document sync -----------------------------------------------------

  private ensureDidOpen(conn: ServerConn, filePath: string, content?: string): void {
    const uri = this.fileUri(filePath);
    if (this.openedDocuments.has(uri)) {
      // Lifecycle introspection: the didOpen short-circuit is otherwise invisible,
      // so a developer cannot tell a skipped-because-already-open from a never-sent
      // didOpen. Mirrors Claude's "File already open, skipping didOpen" debug line.
      log('lsp', `[LSP ${conn.language}] file already open, skipping didOpen: ${filePath}`, undefined, 'debug');
      return;
    }
    const languageId = languageIdForExtension(path.extname(filePath));
    let text = content;
    if (text === undefined) {
      // File-size boundary: never stream a huge file into the server (OOM/wedge
      // guard, parity with Claude's MAX_LSP_FILE_SIZE_BYTES). Open it as empty so
      // the document is still registered but no multi-hundred-MB buffer is read.
      const size = lspFileSizeBytes(filePath);
      if (size !== undefined && size > MAX_LSP_FILE_SIZE_BYTES) {
        text = '';
      } else {
        try { text = fs.readFileSync(filePath, 'utf-8'); } catch { text = ''; }
      }
    }
    this.rawNotify(conn, 'textDocument/didOpen', {
      textDocument: { uri, languageId, version: 1, text },
    });
    this.openedDocuments.add(uri);
    this.documentVersions.set(uri, 1);
    // Lifecycle introspection: record which document was opened and with which
    // languageId, so a "server saw the file but produced no diagnostics" case is
    // distinguishable from "the file was never synced" (parity with Claude's
    // per-sync debug logging in LSPServerManager).
    log('lsp', `[LSP ${conn.language}] didOpen ${filePath} (languageId=${languageId})`, undefined, 'debug');
  }

  async notifyDidChange(filePath: string, content: string, cwd: string): Promise<void> {
    const lang = resolveLanguageForExtension(path.extname(filePath));
    if (!lang) return;
    const ok = await this.ensureServer(lang, cwd);
    if (!ok) return;
    const conn = this.servers.get(lang)!;
    this.ensureDidOpen(conn, filePath, content);
    const uri = this.fileUri(filePath);
    // LSP requires strictly increasing versions per document; a monotonic
    // counter avoids the collisions a same-millisecond `Date.now()` can produce
    // (which some servers silently drop).
    const version = (this.documentVersions.get(uri) ?? 1) + 1;
    this.documentVersions.set(uri, version);
    this.syncNotify(conn, 'textDocument/didChange', {
      textDocument: { uri, version },
      contentChanges: [{ text: content }],
    }, 'change', filePath);
    log('lsp', `[LSP ${lang}] didChange ${filePath} (version=${version})`, undefined, 'debug');
  }

  async notifyDidSave(filePath: string, cwd: string): Promise<void> {
    const lang = resolveLanguageForExtension(path.extname(filePath));
    if (!lang) return;
    const ok = await this.ensureServer(lang, cwd);
    if (!ok) return;
    const conn = this.servers.get(lang)!;
    this.ensureDidOpen(conn, filePath);
    this.syncNotify(conn, 'textDocument/didSave', { textDocument: { uri: this.fileUri(filePath) } }, 'save', filePath);
    log('lsp', `[LSP ${lang}] didSave ${filePath}`, undefined, 'debug');
  }

  // ---- diagnostics (DiagnosticSource) -----------------------------------

  /**
   * Return the current diagnostics for a file, waiting up to `timeoutMs` for the
   * server to publish them when none have arrived yet. Returns `null` when no
   * server handles the file type. Implements DiagnosticSource for the tracker.
   */
  async getDiagnostics(filePath: string, timeoutMs = DEFAULT_DIAGNOSTICS_TIMEOUT_MS): Promise<DiagnosticFile | null> {
    const lang = resolveLanguageForExtension(path.extname(filePath));
    if (!lang) return null;
    const ok = await this.ensureServer(lang, filePath ? path.dirname(path.resolve(filePath)) : '.');
    if (!ok) return null;
    const conn = this.servers.get(lang)!;
    this.ensureDidOpen(conn, filePath);
    const key = normalizeDiagnosticPath(filePath);
    if (!this.diagnosticsByUri.has(key)) {
      await this.waitForDiagnostics(key, timeoutMs);
    }
    return this.diagnosticsByUri.get(key) ?? { uri: this.fileUri(filePath), diagnostics: [] };
  }

  /** Snapshot of all files that currently have at least one diagnostic. */
  getAllDiagnostics(): DiagnosticFile[] {
    const out: DiagnosticFile[] = [];
    for (const file of this.diagnosticsByUri.values()) {
      if (file.diagnostics.length > 0) out.push(file);
    }
    return out;
  }

  /**
   * Number of files with an in-flight `getDiagnostics` wait — i.e. documents the
   * client has synced and is still awaiting a `publishDiagnostics` for. The AMI
   * analogue of Claude Code's `getPendingLSPDiagnosticCount()` monitoring accessor
   * (LSPDiagnosticRegistry): a non-zero count means diagnostics are still settling,
   * which a `/doctor`-style introspection surface can report instead of guessing
   * whether "no diagnostics" means clean or not-yet-arrived.
   */
  getPendingDiagnosticCount(): number {
    let count = 0;
    for (const waiters of this.diagnosticWaiters.values()) {
      if (waiters.length > 0) count++;
    }
    return count;
  }

  private waitForDiagnostics(key: string, timeoutMs: number): Promise<void> {
    return new Promise<void>((resolve) => {
      const list = this.diagnosticWaiters.get(key) ?? [];
      const timer = setTimeout(() => {
        const current = this.diagnosticWaiters.get(key);
        if (current) {
          const idx = current.indexOf(onPublish);
          if (idx !== -1) current.splice(idx, 1);
          // Drop the key once its last waiter times out so the pending count (and
          // the map) reflects only files genuinely still awaiting diagnostics — a
          // settled timeout is no longer "pending".
          if (current.length === 0) this.diagnosticWaiters.delete(key);
        }
        resolve();
      }, timeoutMs);
      if (typeof (timer as { unref?: () => void }).unref === 'function') (timer as { unref: () => void }).unref();
      const onPublish = () => { clearTimeout(timer); resolve(); };
      list.push(onPublish);
      this.diagnosticWaiters.set(key, list);
    });
  }

  // ---- lifecycle ---------------------------------------------------------

  private clearServer(language: string): void {
    const conn = this.servers.get(language);
    if (conn) {
      for (const [, entry] of conn.pending) {
        clearTimeout(entry.timer);
        entry.reject(new Error(`LSP server for "${language}" exited`));
      }
      conn.pending.clear();
      // Kill the child so a server that failed to initialize (e.g. handshake
      // timeout) does not leak as an orphaned process. Harmless if already gone.
      try { conn.proc.kill(); } catch { /* already dead */ }
    }
    this.servers.delete(language);
    this.clearOpenedForLanguage(language);
  }

  private clearOpenedForLanguage(language: string): void {
    for (const uri of this.openedDocuments) {
      const uriPath = uriToPath(uri);
      if (resolveLanguageForExtension(path.extname(uriPath)) === language) {
        this.openedDocuments.delete(uri);
        this.documentVersions.delete(uri);
      }
    }
  }

  shutdown(): void {
    // Stop the background health-poll loop first — teardown must not keep polling
    // the state it is about to clear (and the loop must never outlive the client).
    this.stopHealthMonitoring();
    // Bump the init generation FIRST so any handshake still in flight observes the
    // change on its next `await` and abandons itself (via `abandonStale`) instead
    // of resolving into the state we are about to clear.
    this.generation++;
    // Snapshot the open documents before we clear them — gracefulStop needs them
    // to emit `textDocument/didClose` for each server it is tearing down.
    const openUris = [...this.openedDocuments];
    // Lifecycle introspection: a one-line teardown summary so a developer can see
    // the subsystem shutting down and how many servers it is stopping.
    if (this.servers.size > 0) {
      log('lsp', `[LSP] shutting down ${this.servers.size} language server(s): ${[...this.servers.keys()].join(', ')}`, undefined, 'debug');
    }
    // Collect each server's graceful-stop promise so their outcomes can be
    // aggregated. AMI's teardown stays NON-blocking (the client-level state is
    // cleared synchronously below, a superset of Claude Code's awaited-throwing
    // `LSPServerManager.shutdown`), but — rather than swallowing a stuck/zombie
    // server the way the old per-process `try { kill() } catch {}` did — every
    // per-server stop is settled via `Promise.allSettled` and any failures are
    // aggregated into ONE reported error ("Failed to stop N LSP server(s): …").
    const stops = [...this.servers].map(([language, conn]) => ({
      language,
      promise: this.gracefulStop(language, conn, openUris),
    }));
    if (stops.length > 0) {
      void Promise.allSettled(stops.map((s) => s.promise)).then((results) => {
        const failures = results.flatMap((r, i) =>
          r.status === 'rejected' ? [`${stops[i].language}: ${(r.reason as Error)?.message ?? String(r.reason)}`] : [],
        );
        if (failures.length > 0) {
          logError('lsp', `Failed to stop ${failures.length} LSP server(s): ${failures.join('; ')}`);
        }
      });
    }
    // Client-level state clears synchronously so `isConnected()` et al. reflect the
    // stop immediately; each server entry is removed as its graceful stop settles.
    this.openedDocuments.clear();
    this.documentVersions.clear();
    this.crashCounts.clear();
    this.handshakes.clear();
    this.serverStatus.clear();
    this.pendingInits = 0;
    this.diagnosticsByUri.clear();
    this.diagnosticWaiters.clear();
    this.diagnosticFailures.clear();
    this.diagnostics.reset();
  }

  /**
   * Gracefully stop one server: for an initialized connection, send a `didClose`
   * for each of its open documents (so a later session can cleanly reopen them),
   * then run the LSP shutdown handshake — a bounded `shutdown` request followed by
   * the `exit` notification — before killing the process and removing listeners.
   * Teardown never blocks on the server: the map state is cleared by the caller
   * synchronously, and the handshake proceeds to `exit`+kill whether the `shutdown`
   * request succeeds, errors, or times out. A never-initialized server (handshake
   * never completed) is just killed.
   *
   * Returns a promise that RESOLVES once the process has been (force-)killed and
   * REJECTS if the kill itself throws — so {@link shutdown} can aggregate which
   * servers failed to stop (parity with Claude Code's per-server stop outcomes).
   */
  private gracefulStop(language: string, conn: ServerConn, openUris: string[]): Promise<void> {
    return new Promise<void>((resolve, reject) => {
    // Flip initialized off first so `isConnected()` is false immediately and no
    // new request races the teardown.
    conn.initialized = false;
    // Mark the lifecycle `stopping`; `detach()` flips it to `stopped`. An intentional
    // stop must never land in `error` state — the crash listeners are removed below.
    this.setStatus(language, { state: 'stopping' });

    const detach = (): void => {
      this.servers.delete(language);
      this.setStatus(language, { state: 'stopped' });
      // Reject (not just cancel the timer of) every still-pending request so an
      // in-flight handshake/request racing this stop settles IMMEDIATELY instead
      // of hanging until its (unref'd) timeout — which may never fire if the loop
      // is otherwise idle after teardown. The generation guard then treats the
      // rejection as a stale no-op.
      for (const [, entry] of conn.pending) {
        clearTimeout(entry.timer);
        entry.reject(new Error(`LSP server for "${language}" stopped`));
      }
      conn.pending.clear();
      // Drop the crash listeners so the server's OWN graceful exit (prompted by the
      // `exit` notification) is not mis-counted as a crash.
      if (typeof conn.proc.removeAllListeners === 'function') {
        conn.proc.removeAllListeners('exit');
        conn.proc.removeAllListeners('error');
      }
    };

    if (!conn.proc.pid) {
      detach();
      try { conn.proc.kill(); resolve(); } catch (e) { reject(e as Error); }
      return;
    }

    // didClose each open document that belongs to this language (reopen tracking).
    for (const uri of openUris) {
      if (resolveLanguageForExtension(path.extname(uriToPath(uri))) === language) {
        this.rawNotify(conn, 'textDocument/didClose', { textDocument: { uri } });
      }
    }

    // Graceful handshake: bounded `shutdown` request, then `exit`, then let the
    // server shut itself down — force-killing only as a safety net after a short
    // grace window (kill happens regardless of whether the server answered the
    // shutdown request). The grace lets the `exit` notification flush and be acted
    // on before SIGTERM, so teardown is a clean LSP shutdown, not an abrupt kill.
    const finish = (): void => {
      detach();
      this.rawNotify(conn, 'exit', null);
      const timer = setTimeout(() => {
        try { conn.proc.kill(); resolve(); } catch (e) { reject(e as Error); }
      }, EXIT_GRACE_MS);
      if (typeof (timer as { unref?: () => void }).unref === 'function') (timer as { unref: () => void }).unref();
    };
    const shutdownTimeout = this.configFor(language)?.shutdownTimeout ?? DEFAULT_SHUTDOWN_TIMEOUT_MS;
    this.rawRequest(conn, 'shutdown', null, shutdownTimeout).then(finish, finish);
    });
  }
}

let _lspClient: LSPClient | null = null;
export function getLSPClient(): LSPClient {
  if (!_lspClient) _lspClient = new LSPClient();
  return _lspClient;
}

/**
 * Non-creating accessor for the singleton: returns the live client if one has
 * already been instantiated, else `null`. Unlike {@link getLSPClient} this never
 * constructs a client, so a cheap health probe (e.g. the LSP tools' `isEnabled`
 * gate checking whether every started server has errored) can inspect server
 * state WITHOUT eagerly spinning up the subsystem just to ask the question.
 */
export function peekLSPClient(): LSPClient | null {
  return _lspClient;
}
