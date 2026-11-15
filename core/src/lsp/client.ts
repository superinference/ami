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
import { sanitizeEnvForSubprocess } from '../tools/bash';
import { retryWithBackoff } from '../mcp/oauth';
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
  normalizeDiagnosticPath,
  type DiagnosticFile,
  type DiagnosticSource,
  type PublishDiagnosticsParams,
} from './diagnostic-tracking';

/** LSP transient error: server state changed mid-request (e.g. still indexing). */
const LSP_CONTENT_MODIFIED = -32801;
/** Default per-request timeout. */
const DEFAULT_REQUEST_TIMEOUT_MS = 15000;
/** Default bounded wait for diagnostics to settle after open/change. */
const DEFAULT_DIAGNOSTICS_TIMEOUT_MS = 2000;
/** Default crash-recovery cap (matches Claude's maxRestarts default). */
const DEFAULT_MAX_RESTARTS = 3;

type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (reason: Error) => void;
  timer: ReturnType<typeof setTimeout>;
};

interface ServerConn {
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
  /** Per-language crash-recovery counter (reset to 0 on a successful init). */
  private crashCounts: Map<string, number> = new Map();
  /** Latest diagnostics per normalized path, from publishDiagnostics. */
  private diagnosticsByUri: Map<string, DiagnosticFile> = new Map();
  /** One-shot waiters resolved when the next publishDiagnostics for a path arrives. */
  private diagnosticWaiters: Map<string, Array<() => void>> = new Map();
  /** Client-level notification handlers (apply to every server; survive respawns). */
  private notificationHandlers: Map<string, NotificationHandler[]> = new Map();
  /** Client-level server→client request handlers. */
  private requestHandlers: Map<string, RequestHandler> = new Map();

  /** Baseline/diff tracker reading diagnostics from this client. */
  readonly diagnostics = new DiagnosticTracker(this);

  // Process-spawn hooks (overridable in tests). Production behavior is identical
  // to calling child_process directly.
  _spawnSync: typeof child_process.spawnSync = child_process.spawnSync;
  _spawn: typeof child_process.spawn = child_process.spawn;
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
  }

  /** Register a handler for a server→client request method. */
  onRequest(method: string, handler: RequestHandler): void {
    this.requestHandlers.set(method, handler);
  }

  private configFor(language: string): LspServerConfig | undefined {
    return getAllLspServers()[language];
  }

  async ensureServer(language: string, cwd: string): Promise<boolean> {
    if (this.servers.has(language)) return true;
    const config = this.configFor(language);
    if (!config) return false;

    // Supply-chain guard: never let npx auto-fetch a server from the registry.
    try {
      assertNoAutoFetch(config);
    } catch {
      return false;
    }

    // Crash-recovery cap: refuse to respawn a persistently crashing server.
    const maxRestarts = config.maxRestarts ?? DEFAULT_MAX_RESTARTS;
    if ((this.crashCounts.get(language) ?? 0) > maxRestarts) return false;

    try {
      const which = this._spawnSync('which', [config.command], { timeout: 2000, stdio: 'pipe' });
      if (which.status !== 0) return false;

      const proc = this._spawn(config.command, config.args ?? [], {
        cwd: config.workspaceFolder ?? cwd,
        stdio: ['pipe', 'pipe', 'pipe'],
        // Scrub credential-bearing env from the untrusted language-server child,
        // then layer any per-server env. Reuses the canonical bash scrub helper.
        env: { ...sanitizeEnvForSubprocess({ ...process.env }), ...config.env },
      });

      const onGone = () => {
        this.crashCounts.set(language, (this.crashCounts.get(language) ?? 0) + 1);
        this.clearServer(language);
      };
      proc.on('error', onGone);
      proc.on('exit', onGone);
      proc.stdin?.on('error', () => {});
      proc.stderr?.on('data', () => {});
      if (!proc.pid) return false;

      const conn: ServerConn = {
        proc,
        buffer: Buffer.alloc(0),
        nextId: 0,
        pending: new Map(),
        capabilities: undefined,
        initialized: false,
      };
      proc.stdout?.on('data', (chunk: Buffer) => this.onData(language, chunk));
      this.servers.set(language, conn);

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
      conn.capabilities = (result as { capabilities?: unknown })?.capabilities;
      this.rawNotify(conn, 'initialized', {});
      conn.initialized = true;
      this.crashCounts.set(language, 0);
      return true;
    } catch {
      this.clearServer(language);
      return false;
    }
  }

  private initializeParams(workspaceFolder: string, config: LspServerConfig): unknown {
    const workspaceUri = `file://${workspaceFolder}`;
    return {
      processId: process.pid,
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
        this.onPublishDiagnostics(language, params as PublishDiagnosticsParams);
      }
      for (const handler of this.notificationHandlers.get(method) ?? []) {
        try { handler(params); } catch { /* isolate handler errors */ }
      }
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

  private onPublishDiagnostics(_language: string, params: PublishDiagnosticsParams): void {
    if (!params || typeof params.uri !== 'string') return;
    const key = normalizeDiagnosticPath(params.uri);
    this.diagnosticsByUri.set(key, {
      uri: params.uri,
      diagnostics: convertPublishDiagnostics(params),
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

  private rawNotify(conn: ServerConn, method: string, params: unknown): void {
    const body = JSON.stringify({ jsonrpc: '2.0', method, params });
    try {
      conn.proc.stdin?.write(`Content-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`);
    } catch { /* server gone */ }
  }

  /**
   * Send an LSP request to the server for `language`, returning the server's
   * actual result. Rejects on server error or timeout. Transient
   * ContentModified (-32801) errors are retried with exponential backoff.
   */
  async sendRequest<T = unknown>(language: string, method: string, params: unknown, timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS): Promise<T> {
    const conn = this.servers.get(language);
    if (!conn || !conn.initialized) {
      throw new Error(`LSP server for "${language}" is not initialized`);
    }
    return retryWithBackoff<T>(() => this.rawRequest<T>(conn, method, params, timeoutMs), {
      retries: 3,
      baseMs: 500,
      sleep: this._sleep,
      isRetryable: (e) => (e as { code?: number }).code === LSP_CONTENT_MODIFIED,
    });
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
    const uri = `file://${filePath}`;
    if (this.openedDocuments.has(uri)) return;
    const languageId = languageIdForExtension(path.extname(filePath));
    let text = content;
    if (text === undefined) {
      try { text = fs.readFileSync(filePath, 'utf-8'); } catch { text = ''; }
    }
    this.rawNotify(conn, 'textDocument/didOpen', {
      textDocument: { uri, languageId, version: 1, text },
    });
    this.openedDocuments.add(uri);
  }

  async notifyDidChange(filePath: string, content: string, cwd: string): Promise<void> {
    const lang = resolveLanguageForExtension(path.extname(filePath));
    if (!lang) return;
    const ok = await this.ensureServer(lang, cwd);
    if (!ok) return;
    const conn = this.servers.get(lang)!;
    this.ensureDidOpen(conn, filePath, content);
    this.rawNotify(conn, 'textDocument/didChange', {
      textDocument: { uri: `file://${filePath}`, version: Date.now() },
      contentChanges: [{ text: content }],
    });
  }

  async notifyDidSave(filePath: string, cwd: string): Promise<void> {
    const lang = resolveLanguageForExtension(path.extname(filePath));
    if (!lang) return;
    const ok = await this.ensureServer(lang, cwd);
    if (!ok) return;
    const conn = this.servers.get(lang)!;
    this.ensureDidOpen(conn, filePath);
    this.rawNotify(conn, 'textDocument/didSave', { textDocument: { uri: `file://${filePath}` } });
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
    return this.diagnosticsByUri.get(key) ?? { uri: `file://${path.resolve(filePath)}`, diagnostics: [] };
  }

  /** Snapshot of all files that currently have at least one diagnostic. */
  getAllDiagnostics(): DiagnosticFile[] {
    const out: DiagnosticFile[] = [];
    for (const file of this.diagnosticsByUri.values()) {
      if (file.diagnostics.length > 0) out.push(file);
    }
    return out;
  }

  private waitForDiagnostics(key: string, timeoutMs: number): Promise<void> {
    return new Promise<void>((resolve) => {
      const list = this.diagnosticWaiters.get(key) ?? [];
      const timer = setTimeout(() => {
        const current = this.diagnosticWaiters.get(key);
        if (current) {
          const idx = current.indexOf(onPublish);
          if (idx !== -1) current.splice(idx, 1);
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
      const uriPath = uri.replace('file://', '');
      if (resolveLanguageForExtension(path.extname(uriPath)) === language) {
        this.openedDocuments.delete(uri);
      }
    }
  }

  shutdown(): void {
    for (const [language] of this.servers) {
      const conn = this.servers.get(language);
      try { conn?.proc.kill(); } catch { /* already dead */ }
    }
    this.servers.clear();
    this.openedDocuments.clear();
    this.crashCounts.clear();
    this.diagnosticsByUri.clear();
    this.diagnosticWaiters.clear();
    this.diagnostics.reset();
  }
}

let _lspClient: LSPClient | null = null;
export function getLSPClient(): LSPClient {
  if (!_lspClient) _lspClient = new LSPClient();
  return _lspClient;
}
