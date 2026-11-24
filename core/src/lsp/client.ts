/**
 * LSP client — a real JSON-RPC request/response client over stdio.
 *
 * Mirrors the reference's LSPClient/LSPServerInstance behavior without pulling in
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
import { isBinaryInstalled, firstAvailableBinary } from '../utils/which';
import { pythonBootstrapHint } from '../utils/package-manager';
import { retryWithBackoff } from '../mcp/oauth';
import { classifySubprocessError } from '../error-classifier';
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
import { getLSPDiagnosticRegistry } from './diagnostic-registry';
import {
  LspHealthMonitor,
  type HealthScheduler,
  type LspHealthError,
} from './health-monitor';
import { DEFAULT_DIAGNOSTICS_TIMEOUT_MS, DEFAULT_MAX_RESTARTS, DEFAULT_REQUEST_TIMEOUT_MS, DEFAULT_SHUTDOWN_TIMEOUT_MS, DIAGNOSTIC_FAILURE_WARN_THRESHOLD, EXIT_GRACE_MS, LSP_CONTENT_MODIFIED, LSP_STDERR_TAIL_BYTES, lspLocale } from './client-handler-registration-result';
import type { HandlerRegistrationResult, LspInitializationStatus, LspServerStatus, NotificationHandler, RequestHandler, ServerConn } from './client-handler-registration-result';
export { LSP_SERVER_STATES, lspLocale } from './client-handler-registration-result';
export type { HandlerRegistrationResult, LspInitializationStatus, LspServerState, LspServerStatus, NotificationHandler, RequestHandler } from './client-handler-registration-result';

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
   * AMI analogue of the reference's generation-guarded init + `waitForInitialization`.
   */
  private handshakes: Map<string, Promise<boolean>> = new Map();
  /**
   * Monotonic initialization generation, bumped by {@link shutdown}. A `startServer`
   * handshake captures the generation at its start and re-checks it after every
   * `await`; if `shutdown()` (or any teardown) bumped it while the handshake was in
   * flight, the now-STALE handshake is abandoned instead of resolving into — and
   * clobbering — the newer client state (e.g. re-recording a server as `error`
   * after shutdown already cleared it). This is the AMI analogue of the reference's
   * `initializationGeneration` guard (services/lsp/manager.ts): the stale promise's
   * resolution becomes a no-op. Without it, a shutdown mid-handshake leaves a
   * phantom `failed` server in `getInitializationStatus()`/`getServerStatus()`.
   */
  private generation = 0;
  /**
   * Retained per-language lifecycle status. Unlike the live `servers` map (which a
   * crash removes), this survives a crash so the failure state + reason stay
   * attributable — the AMI analogue of the reference's per-`LSPServerInstance` state.
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
   * throws, reset on the next success — the AMI analogue of the reference's
   * passiveFeedback `diagnosticFailures` map. Surfaced in the per-server status.
   */
  private diagnosticFailures: Map<string, { count: number; lastError: string }> = new Map();
  /**
   * Background health-poll loop (lazily created on the first successful init). The
   * lazy LSP client only (re)checks a server when a tool touches a matching file;
   * this loop re-checks on a fixed interval so a server that crashes BETWEEN tool
   * calls — i.e. after a successful launch — is still surfaced, and gives up once
   * the client has definitively failed. Parity with the reference's
   * useLspInitializationNotification polling. See {@link startHealthMonitoring}.
   */
  private healthMonitor?: LspHealthMonitor;
  /**
   * User-facing health-error listeners (e.g. the CLI's transient-flash sink). Every
   * server failure routes through {@link recordError}, which emits to these so a
   * spawn/init failure or a post-startup crash is SURFACED to the user instead of
   * only being logged — the AMI analogue of the reference's
   * `useLspInitializationNotification` toast. Deduped via {@link notifiedHealthErrors}
   * so the same failure is shown at most once per session (never spammed).
   */
  private healthErrorListeners: Array<(error: LspHealthError) => void> = [];
  /** Deduper for {@link emitHealthError}, keyed `source:message` (parity with
   * the reference's `notifiedErrorsRef` set and the health monitor's own per-source dedup). */
  private notifiedHealthErrors = new Set<string>();

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
   * Lazy-handler affordance (parity with the reference's LSPClient "Queued … handler
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
   * the init-pending await the reference's manager performs before answering a request.
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
    // (the reference schema parity) but we refuse to spawn rather than silently
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
    // `/doctor`) instead of swallowing it silently. Parity with the reference's LSP
    // discovery, which never throws (LSP is optional) but logs every failure "to
    // improve the feature" rather than failing invisibly.
    try {
      assertNoAutoFetch(config);
    } catch (e) {
      this.recordError(language, (e as Error).message);
      return false;
    }

    // Crash-recovery cap: refuse to respawn a persistently crashing server. The
    // crash counter doubles as the reference's `restartCount` — each respawn after a
    // crash bumps it, and exceeding `maxRestarts` leaves the server in `error`.
    const maxRestarts = config.maxRestarts ?? DEFAULT_MAX_RESTARTS;
    const restartCount = this.crashCounts.get(language) ?? 0;
    // Honor `restartOnCrash: false` — a server that has crashed at least once
    // stays down for the rest of the session instead of being respawned on the
    // next incidental file event (parity with the reference's `restartOnCrash` knob).
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
      // Ordered-candidate fallback: try `command` then each `commandAlternatives`
      // entry in preference order and spawn the FIRST installed one, so a server
      // published under several interchangeable binary names still launches when
      // the canonical one is absent (parity with the reference's alternate-binary
      // fallback). The shared `firstAvailableBinary` resolver runs through this
      // client's `_isBinaryInstalled` seam so tests drive it deterministically.
      const candidates = [config.command, ...(config.commandAlternatives ?? [])];
      const resolvedCommand = firstAvailableBinary(candidates, this._isBinaryInstalled);
      if (!resolvedCommand) {
        // Bootstrap affordance: for a Python-installable server (config declares a
        // `pythonPackage`) append the exact install command for the best available
        // Python package manager (uv/pipx/pip, pip3 collapsed to pip). AMI never
        // auto-installs — the hint is surfaced in lastError (→ /doctor) for the user
        // to run. Shares the client's `_isBinaryInstalled` seam so the package-manager
        // probe is deterministic under test.
        const hint = config.pythonPackage
          ? ` ${pythonBootstrapHint(config.pythonPackage, this._isBinaryInstalled)}`
          : '';
        this.recordError(language, `LSP server "${language}" command not found: ${candidates.join(', ')}${hint}`);
        return false;
      }

      // Lifecycle introspection: a "starting" debug line so the on-demand launch
      // of a language server is observable (which binary, with which args) — the
      // AMI analogue of the reference's per-server load logging. Pairs with the
      // `initialized` line below and the `command not found` error above so a
      // started / missing / failed server is distinguishable in the debug log.
      log('lsp', `[LSP ${language}] starting server (command=${resolvedCommand} ${(config.args ?? []).join(' ')})`.trimEnd(), undefined, 'debug');

      const proc = this._spawn(resolvedCommand, config.args ?? [], {
        cwd: config.workspaceFolder ?? cwd,
        stdio: ['pipe', 'pipe', 'pipe'],
        // No console window for the child on Windows (headless subprocess).
        windowsHide: true,
        // Scrub credential-bearing env from the untrusted language-server child,
        // then layer any per-server env. Reuses the canonical bash scrub helper.
        env: { ...sanitizeEnvForSubprocess({ ...process.env }), ...config.env },
      });

      // Bounded tail of the server's own stderr, retained so a CRASH exit can be
      // classified into an actionable recovery path (missing dependency, disabled
      // API, …) instead of only "crashed with exit code N". Capped so a chatty
      // server can't grow it unbounded; only the most recent bytes carry the
      // failure reason.
      let recentStderr = '';

      // Exit handler: distinguish an intentional stop (gracefulStop removes these
      // listeners first) from a real crash. On a non-zero/non-null exit code we
      // build a `crashed with exit code N` error, log it, and retain it as the
      // server's lastError so the crash reason survives the connection teardown.
      // When the retained stderr matches a known-recoverable signature, the typed
      // remediation steps are appended so `/doctor` tells the user HOW to recover
      // (parity: the reference's verifyIt2Setup stderr → remediation classification).
      const onExit = (code: number | null) => {
        this.crashCounts.set(language, (this.crashCounts.get(language) ?? 0) + 1);
        const crashed = code !== 0 && code !== null;
        let message = crashed
          ? `LSP server "${language}" crashed with exit code ${code}`
          : `LSP server "${language}" exited`;
        if (crashed) {
          const classification = classifySubprocessError(recentStderr);
          if (classification.recoverable) {
            message += ` — ${classification.remediation.join(' ')}`;
          }
        }
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
        const chunk = data.toString();
        // Retain a bounded tail for crash classification (keep the most recent
        // LSP_STDERR_TAIL_BYTES — the failure reason is always at the end).
        recentStderr = (recentStderr + chunk).slice(-LSP_STDERR_TAIL_BYTES);
        const output = chunk.trim();
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
      // Mirrors the reference's per-server `settings` knob.
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
   * Register a user-facing health-error listener (e.g. the CLI's transient-flash
   * sink). The listener fires once per distinct failure (deduped per
   * `source:message`), isolating a throwing listener so one bad sink never breaks
   * the rest. This is the subscription a frontend uses to turn a silent LSP
   * spawn/init failure into a visible toast — parity with the reference's
   * `useLspInitializationNotification` `addNotification`.
   */
  onHealthError(listener: (error: LspHealthError) => void): void {
    this.healthErrorListeners.push(listener);
  }

  /**
   * Emit a health error to every registered listener exactly once per distinct
   * `source:message` (so a repeatedly-failing server is surfaced once, never
   * spammed). Listener exceptions are isolated. The shared dedup set also covers
   * the background health monitor (whose auto-start sink routes here), so a
   * poll-detected error never double-fires an already-surfaced failure.
   */
  private emitHealthError(error: LspHealthError): void {
    const key = `${error.source}:${error.message}`;
    if (this.notifiedHealthErrors.has(key)) return;
    this.notifiedHealthErrors.add(key);
    for (const listener of this.healthErrorListeners) {
      try { listener(error); } catch { /* isolate a bad sink */ }
    }
  }

  /**
   * Record a server error: flip the retained status to `error`, keep the message
   * as `lastError`, surface it through the shared logger (which also captures it in
   * the in-memory error ring that `/doctor` reads), AND notify any user-facing
   * health-error listeners so a spawn/init failure or crash becomes a visible
   * toast instead of a silent log line (deduped — see {@link emitHealthError}).
   */
  private recordError(language: string, message: string, exitCode?: number | null): void {
    this.setStatus(language, { state: 'error', lastError: message, exitCode });
    logError('lsp', message);
    this.emitHealthError({ source: language, message });
  }

  private initializeParams(workspaceFolder: string, config: LspServerConfig): unknown {
    const workspaceUri = this.fileUri(workspaceFolder);
    return {
      processId: process.pid,
      // Identify AMI (name + real version) to the server, matching the full LSP
      // InitializeParams the reference sends. Reuses the single version source of truth
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
   * server is connected and initialized. Mirrors the reference's `isLspConnected()`.
   */
  isConnected(): boolean {
    for (const conn of this.servers.values()) {
      if (conn.initialized) return true;
    }
    return false;
  }

  /**
   * Health predicate for a single language server, mirroring the reference's
   * `LSPServerInstance.isHealthy()`: true only when the server is running AND its
   * initialize handshake completed. Used to gate requests with a clear error.
   */
  isHealthy(language: string): boolean {
    const status = this.serverStatus.get(language);
    return status?.state === 'running' && this.servers.get(language)?.initialized === true;
  }

  /**
   * Introspection snapshot for one language server, or `undefined` if no server
   * for that language has ever been attempted. Mirrors the reference's per-instance
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
   * Wire diagnostic-notification handling across every language server and report
   * the aggregate outcome, mirroring the reference's
   * `registerLSPNotificationHandlers` (services/lsp/passiveFeedback.ts L124-L324).
   *
   * the reference loops over each server instance, registers a
   * `textDocument/publishDiagnostics` handler, isolates per-server failures, and
   * returns a {@link HandlerRegistrationResult}. AMI dispatches publishDiagnostics
   * CENTRALLY, so an optional `observer` is attached ONCE at the client level (its
   * fan-out already reaches every server); the per-server loop instead VALIDATES
   * that each attempted server can actually deliver diagnostics — a live,
   * initialized connection with a readable stdout channel — and isolates a
   * crashed/half-open/errored server as a per-server `registrationError` so one bad
   * server never masks the readiness of the rest. The aggregate "N/M ready" status
   * is logged (parity with the reference's aggregate report) and the typed result is
   * returned for monitoring/introspection (surfaced by `/doctor`) and testing. No
   * second diagnostic tracker is created: `diagnosticFailures` is a snapshot of the
   * existing per-server consecutive-failure state.
   */
  registerDiagnosticHandlers(observer?: NotificationHandler): HandlerRegistrationResult {
    // Attach the optional extra observer once; the central fan-out delivers to it
    // for every server (no per-server re-registration, which would double-fire it).
    if (observer) this.onNotification('textDocument/publishDiagnostics', observer);

    const registrationErrors: Array<{ serverName: string; error: string }> = [];
    let successCount = 0;
    // Enumeration universe: every server attempted this session. The retained
    // status map survives a crash (unlike the live `servers` map), so a server that
    // failed to start is still counted and reported — the AMI analogue of the reference's
    // `manager.getAllServers()`.
    const languages = [...this.serverStatus.keys()];
    for (const language of languages) {
      const conn = this.servers.get(language);
      // Validate the server can DELIVER diagnostics: a live, initialized connection
      // (the "has onNotification" analogue — a torn-down/crashed server has no live
      // channel). Isolate a not-ready server as a per-server error and continue.
      if (!conn || !conn.initialized) {
        const status = this.serverStatus.get(language);
        registrationErrors.push({
          serverName: language,
          error: status?.lastError ?? `server is ${status?.state ?? 'stopped'}`,
        });
        continue;
      }
      const stdout = conn.proc.stdout;
      if (!stdout || stdout.destroyed || conn.proc.exitCode !== null || conn.proc.signalCode !== null) {
        registrationErrors.push({
          serverName: language,
          error: 'server connection is not readable (cannot deliver diagnostics)',
        });
        continue;
      }
      successCount++;
    }

    const totalServers = languages.length;
    // Aggregate status report (parity with the reference's "N/M succeeded, failed …").
    if (registrationErrors.length > 0) {
      const failed = registrationErrors.map((e) => `${e.serverName} (${e.error})`).join(', ');
      logError(
        'lsp',
        `LSP diagnostic handler registration: ${successCount}/${totalServers} server(s) ready; not ready: ${failed}`,
      );
    } else if (totalServers > 0) {
      log('lsp', `LSP diagnostic handlers ready for all ${totalServers} server(s)`);
    }

    return {
      totalServers,
      successCount,
      registrationErrors,
      // Deep-copy the live failure tracker so the monitoring result is inert.
      diagnosticFailures: new Map(
        [...this.diagnosticFailures].map(([lang, v]) => [lang, { ...v }]),
      ),
    };
  }

  /**
   * Client-wide initialization status, mirroring the reference's manager-level
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
    // Fast-path the common fully-drained case (buffer empty): adopt the chunk
    // directly instead of allocating a fresh buffer and copying the chunk for no
    // reason. Node hands us a fresh Buffer per read, so there is no aliasing hazard,
    // and the drain loop below uses subarray views, so framing is unchanged.
    conn.buffer = conn.buffer.length === 0 ? chunk : Buffer.concat([conn.buffer, chunk]);
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
   * mirroring the reference's passiveFeedback diagnostic-failure circuit breaker.
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
   * `DIAGNOSTIC_FAILURE_WARN_THRESHOLD` (parity with the reference's warn-after-3). The
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
   * — parity with the reference's passiveFeedback validation. An empty diagnostics
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
    const converted: DiagnosticFile = {
      uri: valid.uri,
      diagnostics: convertPublishDiagnostics(valid),
    };
    this.diagnosticsByUri.set(key, converted);
    // Feed the async delivery registry so a drain (the `diagnostics` tool's
    // `only_new` mode) can deliver each distinct diagnostic to the conversation at
    // most once per session. An empty publish (a server CLEARING a file) carries no
    // diagnostics to deliver, so skip registering it.
    if (converted.diagnostics.length > 0) {
      getLSPDiagnosticRegistry().registerPendingLSPDiagnostic({
        serverName: language,
        files: [converted],
      });
    }
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
    // the reference's `[LSP PROTOCOL <name>]` tracing hook for deep debugging.
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
   * the caller learns the document is no longer in sync — parity with the reference's
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
      // Contextual, the reference-parity wording: name the server, its current lifecycle
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
        // instead of silently waiting. Mirrors the reference's retry progress logging.
        onRetry: ({ attempt, delayMs }) =>
          log('lsp', `LSP server "${language}" request "${method}" content modified; retry ${attempt}/3 in ${delayMs}ms`),
      });
    } catch (e) {
      // Wrap the server/timeout error with the server name + method for context,
      // matching the reference's `LSP server <name> request <method> failed: <msg>`.
      const message = `LSP server "${language}" request "${method}" failed: ${(e as Error).message}`;
      logError('lsp', message);
      throw new Error(message);
    }
  }

  /**
   * Position-addressed request keyed by file path: resolves the language,
   * ensures the server is up and the document is open, then forwards the
   * request. Returns `undefined` (not a throw) when no server handles the file
   * type — matching the reference's "no LSP server available" fallback.
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
      // didOpen. Mirrors the reference's "File already open, skipping didOpen" debug line.
      log('lsp', `[LSP ${conn.language}] file already open, skipping didOpen: ${filePath}`, undefined, 'debug');
      return;
    }
    const languageId = languageIdForExtension(path.extname(filePath));
    let text = content;
    if (text === undefined) {
      // File-size boundary: never stream a huge file into the server (OOM/wedge
      // guard, parity with the reference's MAX_LSP_FILE_SIZE_BYTES). Open it as empty so
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
    // distinguishable from "the file was never synced" (parity with the reference's
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
   * analogue of the reference's `getPendingLSPDiagnosticCount()` monitoring accessor
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
    // cleared synchronously below, a superset of the reference's awaited-throwing
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
    // Reset the health-error deduper so a later session re-surfaces a recurring
    // failure (parity with resetAllLSPDiagnosticState clearing cross-turn state).
    // The listener subscriptions themselves persist — a frontend registers its
    // flash sink once per process, not per session.
    this.notifiedHealthErrors.clear();
    this.diagnostics.reset();
    // Drop the async registry's pending + cross-turn delivered state so a later
    // session starts clean (parity: resetAllLSPDiagnosticState on session reset).
    getLSPDiagnosticRegistry().resetAllLSPDiagnosticState();
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
   * servers failed to stop (parity with the reference's per-server stop outcomes).
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
