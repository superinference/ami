import { spawn, ChildProcess } from 'child_process';
import { EventEmitter } from 'events';
import { AMI_CLIENT_INFO } from '../version';
import type { McpOAuthProvider } from './oauth';

export const MCP_TRANSPORTS = ['stdio', 'sse', 'http'] as const;
export type McpTransport = (typeof MCP_TRANSPORTS)[number];

export const DEFAULT_MCP_MAX_RESPONSE_BYTES = 10 * 1024 * 1024; // 10 MB

/**
 * Latest MCP protocol version we request in `initialize`. Servers that only
 * support an older revision reply with their own version; we store the
 * negotiated value and proceed, since our wire usage (initialize / tools.list /
 * tools.call / notifications.initialized) is identical across these revisions.
 * Single source of truth for every transport's initialize handshake.
 */
export const MCP_PROTOCOL_VERSION = '2025-06-18';

const MIN_MCP_BUFFER = 1024;            // 1 KB
const MAX_MCP_BUFFER = 100 * 1024 * 1024; // 100 MB

/**
 * Hard cap on the stdio read buffer (L31). A stdio MCP server streams
 * newline-delimited JSON over stdout and we accumulate bytes in `this.buffer`
 * until a newline completes a message. A malicious/buggy server that never
 * sends a newline (or emits one gigantic line) would otherwise grow this buffer
 * without bound → memory-exhaustion DoS. When the un-terminated remainder left
 * after extracting complete messages exceeds this cap, the buffer is dropped and
 * a `protocol-error` is emitted; complete messages parsed on the same pass are
 * unaffected, so normal framing keeps working.
 */
export const MCP_MAX_STDIO_BUFFER = 16 * 1024 * 1024; // 16 MiB

/** Hard cap on cursor-pagination pages per list call — a runaway-server backstop. */
export const MCP_MAX_PAGES = 1000;

export function boundMcpResponse(
  content: string,
  maxBytes: number = DEFAULT_MCP_MAX_RESPONSE_BYTES,
): { content: string; truncated: boolean; originalBytes: number } {
  const originalBytes = Buffer.byteLength(content, 'utf-8');
  if (originalBytes <= maxBytes) {
    return { content, truncated: false, originalBytes };
  }
  const buf = Buffer.from(content, 'utf-8');
  let end = maxBytes;
  // Walk back to avoid splitting a multi-byte UTF-8 character
  while (end > 0 && (buf[end] & 0xc0) === 0x80) end--;
  const truncated = buf.subarray(0, end).toString('utf-8');
  return {
    content: `${truncated}\n\n[Response truncated: ${originalBytes} bytes → ${maxBytes} bytes. Set MCP_MAX_RESPONSE_BYTES to increase limit.]`,
    truncated: true,
    originalBytes,
  };
}

export function validateMcpBufferSize(size: unknown): number {
  if (size === undefined || size === null) return DEFAULT_MCP_MAX_RESPONSE_BYTES;
  const n = typeof size === 'string' ? Number(size) : Number(size);
  if (!Number.isFinite(n)) return DEFAULT_MCP_MAX_RESPONSE_BYTES;
  if (n < MIN_MCP_BUFFER) return MIN_MCP_BUFFER;
  if (n > MAX_MCP_BUFFER) return MAX_MCP_BUFFER;
  return n;
}

export interface McpToolAnnotations {
  title?: string;
  readOnlyHint?: boolean;
  destructiveHint?: boolean;
  openWorldHint?: boolean;
  idempotentHint?: boolean;
}

export interface McpToolSchema {
  name: string;
  /** Human-display name (2025-06-18); falls back to `name` when absent. */
  title?: string;
  description: string;
  inputSchema: Record<string, unknown>;
  /** JSON Schema for the tool's structured result (2025-06-18). */
  outputSchema?: Record<string, unknown>;
  annotations?: McpToolAnnotations;
  _meta?: Record<string, unknown>;
}

export interface McpResource {
  uri: string;
  name: string;
  /** Human-display name (2025-06-18). */
  title?: string;
  mimeType?: string;
}

/** A parameterized resource URI (RFC 6570 template), from resources/templates/list. */
export interface McpResourceTemplate {
  uriTemplate: string;
  name: string;
  title?: string;
  description?: string;
  mimeType?: string;
}

export interface McpPrompt {
  name: string;
  title?: string;
  description?: string;
  arguments?: Array<{ name: string; description?: string; required?: boolean }>;
}

// ---------------------------------------------------------------------------
// 2025-06-18 client-capability request/response shapes
// ---------------------------------------------------------------------------

/**
 * Elicitation (2025-06-18): a server asks the user, via the client, for
 * structured input mid-operation. The client MUST reply with exactly one of
 * three actions and MUST NOT block indefinitely — absent a handler, or on a
 * handler error, it declines/cancels so the server can proceed.
 */
export const MCP_ELICITATION_ACTIONS = ['accept', 'decline', 'cancel'] as const;
export type McpElicitationAction = (typeof MCP_ELICITATION_ACTIONS)[number];

export interface McpElicitationRequest {
  /** Human-readable prompt the server wants shown to the user. */
  message: string;
  /** Flat JSON Schema of primitive fields the server is requesting. */
  requestedSchema: Record<string, unknown>;
}

export interface McpElicitationResult {
  action: McpElicitationAction;
  /** Present only when action === 'accept'. */
  content?: Record<string, unknown>;
}

/**
 * Sampling (`sampling/createMessage`): a server asks the client to run an LLM
 * turn on its behalf. AMI is uniquely able to satisfy this because it owns the
 * engine. Absent a handler the request is refused with a JSON-RPC error.
 */
export interface McpSamplingMessage {
  role: 'user' | 'assistant';
  content: { type: string; text?: string; data?: string; mimeType?: string; [key: string]: unknown };
}

export interface McpSamplingRequest {
  messages: McpSamplingMessage[];
  systemPrompt?: string;
  maxTokens?: number;
  temperature?: number;
  stopSequences?: string[];
  modelPreferences?: Record<string, unknown>;
  includeContext?: 'none' | 'thisServer' | 'allServers';
  metadata?: Record<string, unknown>;
}

export interface McpSamplingResult {
  role: 'assistant';
  content: { type: 'text'; text: string } | { type: string; [key: string]: unknown };
  model: string;
  stopReason?: string;
}

/** MCP log levels (RFC 5424 severities) for logging/setLevel. */
export const MCP_LOG_LEVELS = ['debug', 'info', 'notice', 'warning', 'error', 'critical', 'alert', 'emergency'] as const;
export type McpLogLevel = (typeof MCP_LOG_LEVELS)[number];

export interface McpLogMessage {
  level: McpLogLevel;
  logger?: string;
  data: unknown;
}

/** completion/complete — argument autocompletion for prompts & resource templates. */
export interface McpCompletionRef {
  type: 'ref/prompt' | 'ref/resource';
  name?: string;
  uri?: string;
}

export interface McpCompletionResult {
  values: string[];
  total?: number;
  hasMore?: boolean;
}

export interface McpPromptMessage {
  role: 'user' | 'assistant';
  content: { type: string; text?: string; [key: string]: unknown };
}

interface JsonRpcRequest {
  jsonrpc: '2.0';
  id: number;
  method: string;
  params?: Record<string, unknown>;
}

interface JsonRpcResponse {
  jsonrpc: '2.0';
  id: number;
  result?: unknown;
  error?: { code: number; message: string; data?: unknown };
}

interface JsonRpcNotification {
  jsonrpc: '2.0';
  method: string;
  params?: Record<string, unknown>;
}

export const MCP_CLIENT_STATES = ['disconnected', 'connecting', 'ready', 'error'] as const;
export type McpClientState = (typeof MCP_CLIENT_STATES)[number];

// Only codes classifyError() can actually PRODUCE are advertised here (audit #8: the old
// 'SESSION_EXPIRED' was never produced — no MCP transport emitted it — so it was removed to
// keep the --introspect manifest honest. Re-add it only alongside code that produces it.)
export const MCP_TERMINAL_ERROR_CODES = ['ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'EHOSTUNREACH', 'ECONNREFUSED', 'ESRCH', 'SPAWN_ERROR', 'UNKNOWN'] as const;
export type McpTerminalErrorCode = (typeof MCP_TERMINAL_ERROR_CODES)[number];

export interface McpTerminalError {
  code: McpTerminalErrorCode;
  message: string;
  original?: Error;
}

export class McpSessionExpiredError extends Error {
  constructor(message: string = 'MCP session expired') {
    super(message);
    this.name = 'McpSessionExpiredError';
  }
}

const TERMINAL_ERROR_CODES = new Set([
  'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'EHOSTUNREACH', 'ECONNREFUSED', 'ESRCH',
]);

const SHUTDOWN_SIGINT_MS = 100;
const SHUTDOWN_SIGTERM_MS = 400;
const SHUTDOWN_TOTAL_MS = 500;
const SHUTDOWN_POLL_MS = 50;

/** Backoff bounds for re-opening the Streamable-HTTP server→client GET stream. */
const HTTP_STREAM_RECONNECT_BASE_MS = 1000;
const HTTP_STREAM_RECONNECT_MAX_MS = 30000;

export class McpClient extends EventEmitter {
  private process: ChildProcess | null = null;
  private processPid: number | null = null;
  private requestId = 0;
  private pendingRequests = new Map<number, {
    resolve: (value: unknown) => void;
    reject: (error: Error) => void;
    timer: ReturnType<typeof setTimeout>;
    abortHandler?: () => void;
  }>();
  private buffer = '';
  private _state: McpClientState = 'disconnected';
  private _serverInfo: { name: string; version: string } | null = null;
  private _serverCapabilities: Record<string, unknown> = {};
  private _protocolVersion: string | null = null;
  private _tools: McpToolSchema[] = [];
  private _resources: McpResource[] = [];
  private _prompts: McpPrompt[] = [];
  private transport: McpTransport = 'stdio';
  private sseUrl: string | null = null;
  private readonly command: string;
  private readonly args: string[];
  private readonly env: Record<string, string>;
  private readonly requestTimeout: number;
  private readonly connectTimeout: number;
  private readonly rootPaths: string[];
  private readonly headers: Record<string, string>;
  /** Optional OAuth 2.1 layer for the HTTP transport (bearer token + 401 re-auth). */
  private readonly oauth?: McpOAuthProvider;

  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;
  private hasTriggeredClose = false;
  private connectionStartTime: number | null = null;

  private progressCallbacks = new Map<string, (progress: { percent?: number; message?: string }) => void>();
  /** URIs this client has an active resources/subscribe for. */
  private subscribedResources = new Set<string>();

  // --- Streamable-HTTP server→client GET SSE stream state -------------------
  /** Session id returned by the server (Mcp-Session-Id), echoed on later calls. */
  private httpSessionId: string | null = null;
  /** The in-flight GET stream request, so disconnect can tear it down. */
  private httpStreamReq: { destroy: () => void } | null = null;
  /** Partial SSE buffer for the GET stream (events split on a blank line). */
  private httpStreamBuffer = '';
  /** Set when the stream was closed intentionally (disconnect) — stops reconnect. */
  private httpStreamClosed = false;
  private httpStreamReconnectAttempts = 0;

  /** Injected by the manager/engine: resolve a server elicitation request. */
  onElicitation?: (request: McpElicitationRequest) => Promise<McpElicitationResult>;
  /** Injected by the manager/engine: run an LLM turn for a sampling request. */
  onSampling?: (request: McpSamplingRequest) => Promise<McpSamplingResult>;

  constructor(options: {
    command: string;
    args?: string[];
    env?: Record<string, string>;
    headers?: Record<string, string>;
    requestTimeout?: number;
    connectTimeout?: number;
    rootPaths?: string[];
    oauth?: McpOAuthProvider;
  }) {
    super();
    this.command = options.command;
    this.args = options.args ?? [];
    this.env = options.env ?? {};
    this.headers = options.headers ?? {};
    this.requestTimeout = options.requestTimeout ?? 30000;
    this.connectTimeout = options.connectTimeout ?? 10000;
    this.rootPaths = options.rootPaths ?? [];
    this.oauth = options.oauth;
  }

  get state(): McpClientState { return this._state; }
  get serverInfo() { return this._serverInfo; }
  get serverCapabilities(): Record<string, unknown> { return { ...this._serverCapabilities }; }
  /** The protocol version negotiated with the server (null until connected). */
  get protocolVersion(): string | null { return this._protocolVersion; }
  get tools(): McpToolSchema[] { return [...this._tools]; }
  get resources(): McpResource[] { return [...this._resources]; }
  get prompts(): McpPrompt[] { return [...this._prompts]; }
  get uptime(): number | null {
    return this.connectionStartTime ? Date.now() - this.connectionStartTime : null;
  }

  async connect(): Promise<void> {
    if (this._state === 'ready') return;
    this._state = 'connecting';
    this.hasTriggeredClose = false;

    this.process = spawn(this.command, this.args, {
      stdio: ['pipe', 'pipe', 'pipe'],
      env: { ...process.env, ...this.env },
    });
    this.processPid = this.process.pid ?? null;

    this.process.stdin?.on('error', (err: NodeJS.ErrnoException) => {
      if (err.code !== 'EPIPE') {
        this.emit('stderr', `MCP stdin error: ${err.message}`);
      }
    });

    this.process.stdout!.on('data', (chunk: Buffer) => {
      this.buffer += chunk.toString();
      this.processBuffer();
    });

    this.process.stderr!.on('data', (chunk: Buffer) => {
      this.emit('stderr', chunk.toString());
    });

    this.process.on('error', (err) => {
      const classified = this.classifyError(err);
      this._state = 'error';
      this.rejectAllPending(err);
      this.emit('error', err);
      this.emit('terminal-error', classified);
    });

    this.process.on('close', (code) => {
      if (this.hasTriggeredClose) return;
      this.hasTriggeredClose = true;
      const uptimeMs = this.uptime;
      this._state = 'disconnected';
      this.rejectAllPending(new Error(`MCP server exited with code ${code}`));
      this.emit('close', code, uptimeMs);
    });

    const connectPromise = this.performInitialize();
    const timeoutPromise = new Promise<never>((_, reject) => {
      const t = setTimeout(() => {
        reject(new Error(`MCP connection timed out after ${this.connectTimeout}ms`));
      }, this.connectTimeout);
      t.unref();
    });

    try {
      await Promise.race([connectPromise, timeoutPromise]);
    } catch (err) {
      this._state = 'error';
      this.disconnect();
      throw err;
    }
  }

  /**
   * The client capabilities AMI advertises at `initialize`. Single source of
   * truth for all three transports so a server sees identical support
   * regardless of how it was launched: elicitation + sampling always (AMI can
   * fulfill both, gracefully declining/erroring when no handler is wired), and
   * roots (with listChanged) only when root paths were configured.
   */
  private buildClientCapabilities(): Record<string, unknown> {
    const caps: Record<string, unknown> = { elicitation: {}, sampling: {} };
    if (this.rootPaths.length > 0) {
      caps.roots = { listChanged: true };
    }
    return caps;
  }

  private async performInitialize(): Promise<void> {
    const initResult = await this.sendRequest('initialize', {
      protocolVersion: MCP_PROTOCOL_VERSION,
      capabilities: this.buildClientCapabilities(),
      clientInfo: AMI_CLIENT_INFO,
    }) as { serverInfo?: { name: string; version: string }; capabilities?: Record<string, unknown>; protocolVersion?: string };

    this._serverInfo = initResult.serverInfo ?? { name: 'unknown', version: '0.0.0' };
    this._serverCapabilities = initResult.capabilities ?? {};
    this._protocolVersion = initResult.protocolVersion ?? MCP_PROTOCOL_VERSION;

    await this.sendNotification('notifications/initialized', {});

    this.connectionStartTime = Date.now();
    this._state = 'ready';
    this.emit('ready');
  }

  async connectSSE(url: string): Promise<void> {
    if (this._state === 'ready') return;
    this._state = 'connecting';
    this.transport = 'sse';
    this.sseUrl = url;
    this.hasTriggeredClose = false;

    const http = url.startsWith('https') ? require('https') : require('http');
    const urlObj = new URL(url);

    const initBody = JSON.stringify({
      jsonrpc: '2.0',
      id: 1,
      method: 'initialize',
      params: {
        protocolVersion: MCP_PROTOCOL_VERSION,
        capabilities: this.buildClientCapabilities(),
        clientInfo: AMI_CLIENT_INFO,
      },
    });

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        req.destroy();
        this._state = 'error';
        reject(new Error(`MCP SSE connection timed out after ${this.connectTimeout}ms`));
      }, this.connectTimeout);
      timer.unref?.();

      const req = http.request(urlObj, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json, text/event-stream',
          ...this.headers,
        },
        timeout: this.requestTimeout,
      }, (res: any) => {
        const statusCode = res.statusCode;
        let body = '';
        res.on('data', (d: Buffer) => body += d);
        res.on('end', () => {
          clearTimeout(timer);
          if (statusCode === 404 && this.isSessionExpiredBody(body)) {
            this._state = 'error';
            reject(new McpSessionExpiredError());
            return;
          }
          try {
            const result = JSON.parse(body);
            if (result.error) {
              if (result.error.code === -32001) {
                this._state = 'error';
                reject(new McpSessionExpiredError());
                return;
              }
              this._state = 'error';
              reject(new Error(`MCP SSE init error ${result.error.code}: ${result.error.message}`));
              return;
            }
            this._serverInfo = result.result?.serverInfo ?? { name: 'unknown', version: '0.0.0' };
            this._serverCapabilities = result.result?.capabilities ?? {};
            this._protocolVersion = result.result?.protocolVersion ?? MCP_PROTOCOL_VERSION;
            this.connectionStartTime = Date.now();
            this._state = 'ready';
            this.emit('ready');
            resolve();
          } catch {
            this._state = 'error';
            reject(new Error('Invalid SSE init response'));
          }
        });
      });
      req.on('error', (err: Error) => {
        clearTimeout(timer);
        const classified = this.classifyError(err);
        this._state = 'error';
        this.emit('terminal-error', classified);
        reject(err);
      });
      req.write(initBody);
      req.end();
    });
  }

  async connectHTTP(url: string): Promise<void> {
    if (this._state === 'ready') return;
    this._state = 'connecting';
    this.transport = 'http';
    this.sseUrl = url;
    this.hasTriggeredClose = false;

    const initResult = await this.sendHttpRequest('initialize', {
      protocolVersion: MCP_PROTOCOL_VERSION,
      capabilities: this.buildClientCapabilities(),
      clientInfo: AMI_CLIENT_INFO,
    }) as { serverInfo?: { name: string; version: string }; capabilities?: Record<string, unknown>; protocolVersion?: string };

    this._serverInfo = initResult?.serverInfo ?? { name: 'unknown', version: '0.0.0' };
    this._serverCapabilities = initResult?.capabilities ?? {};
    this._protocolVersion = initResult?.protocolVersion ?? MCP_PROTOCOL_VERSION;

    await this.sendHttpRequest('notifications/initialized', {});

    this.connectionStartTime = Date.now();
    this._state = 'ready';
    this.emit('ready');

    // Streamable-HTTP: open the server→client GET SSE stream so the server can
    // push notifications and requests (sampling, elicitation, progress, roots).
    // Best-effort — a server that declines (405) leaves us on the POST-only path.
    this.httpStreamClosed = false;
    this.httpStreamReconnectAttempts = 0;
    this.openHttpStream().catch(() => { /* POST path remains fully usable */ });
  }

  private async sendWithRetry<T>(method: string, params: Record<string, unknown>, maxAttempts: number = 3): Promise<T> {
    const { isTransientConnectError } = require('./manager');
    let lastErr: unknown;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        return await this.sendRequestAuto(method, params) as T;
      } catch (err) {
        lastErr = err;
        if (err instanceof McpSessionExpiredError || !isTransientConnectError(err)) throw err;
        if (attempt < maxAttempts - 1) {
          await new Promise(r => setTimeout(r, 1000 * (attempt + 1)));
        }
      }
    }
    throw lastErr;
  }

  /**
   * Drive a cursor-paginated list method to completion (2025-06-18). Follows
   * `nextCursor` until the server stops returning one, concatenating the array
   * under `key`. Bounded by MCP_MAX_PAGES so a server that never drops the
   * cursor cannot loop forever (it logs and stops rather than hanging).
   */
  private async collectPaginated<T>(method: string, key: string): Promise<T[]> {
    const items: T[] = [];
    let cursor: string | undefined;
    for (let page = 0; page < MCP_MAX_PAGES; page++) {
      const params: Record<string, unknown> = cursor !== undefined ? { cursor } : {};
      const result = await this.sendWithRetry<Record<string, unknown>>(method, params);
      const batch = (result?.[key] as T[] | undefined) ?? [];
      items.push(...batch);
      const next = result?.nextCursor;
      if (typeof next !== 'string' || next.length === 0) return items;
      cursor = next;
    }
    this.emit('pagination-capped', method, items.length);
    return items;
  }

  async listTools(): Promise<McpToolSchema[]> {
    this.assertReady();
    this._tools = await this.collectPaginated<McpToolSchema>('tools/list', 'tools');
    return this._tools;
  }

  async callTool(name: string, args: Record<string, unknown> = {}, options?: { signal?: AbortSignal }): Promise<unknown> {
    this.assertReady();
    if (options?.signal?.aborted) {
      throw new Error('MCP tool call aborted');
    }
    const result = await this.sendRequestAuto('tools/call', { name, arguments: args }, options?.signal);
    return result;
  }

  async listResources(): Promise<McpResource[]> {
    this.assertReady();
    this._resources = await this.collectPaginated<McpResource>('resources/list', 'resources');
    return this._resources;
  }

  /** resources/templates/list — parameterized resource URIs (2025-06-18). */
  async listResourceTemplates(): Promise<McpResourceTemplate[]> {
    this.assertReady();
    return this.collectPaginated<McpResourceTemplate>('resources/templates/list', 'resourceTemplates');
  }

  async readResource(uri: string, signal?: AbortSignal): Promise<unknown> {
    this.assertReady();
    if (signal?.aborted) {
      throw new Error('MCP request aborted');
    }
    return await this.sendRequestAuto('resources/read', { uri }, signal);
  }

  /** resources/subscribe — ask the server to emit notifications/resources/updated for a URI. */
  async subscribeResource(uri: string): Promise<void> {
    this.assertReady();
    await this.sendRequestAuto('resources/subscribe', { uri });
    this.subscribedResources.add(uri);
  }

  async unsubscribeResource(uri: string): Promise<void> {
    this.assertReady();
    await this.sendRequestAuto('resources/unsubscribe', { uri });
    this.subscribedResources.delete(uri);
  }

  async listPrompts(): Promise<McpPrompt[]> {
    this.assertReady();
    this._prompts = await this.collectPaginated<McpPrompt>('prompts/list', 'prompts');
    return this._prompts;
  }

  async getPrompt(name: string, args: Record<string, string> = {}): Promise<{ description?: string; messages: McpPromptMessage[] }> {
    this.assertReady();
    const result = await this.sendRequestAuto('prompts/get', { name, arguments: args }) as {
      description?: string;
      messages: McpPromptMessage[];
    };
    return result;
  }

  /** logging/setLevel — set the minimum severity the server should emit. */
  async setLogLevel(level: McpLogLevel): Promise<void> {
    this.assertReady();
    await this.sendRequestAuto('logging/setLevel', { level });
  }

  /** completion/complete — argument autocompletion for a prompt or resource template. */
  async complete(
    ref: McpCompletionRef,
    argument: { name: string; value: string },
    context?: { arguments?: Record<string, string> },
  ): Promise<McpCompletionResult> {
    this.assertReady();
    const params: Record<string, unknown> = { ref, argument };
    if (context) params.context = context;
    const result = await this.sendRequestAuto('completion/complete', params) as { completion?: McpCompletionResult };
    return result?.completion ?? { values: [] };
  }

  async ping(): Promise<void> {
    this.assertReady();
    await this.sendRequestAuto('ping', {});
  }

  async reconnect(): Promise<void> {
    this.disconnect();
    while (this.reconnectAttempts < this.maxReconnectAttempts) {
      const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
      const jitter = Math.floor(Math.random() * delay * 0.3);
      await new Promise(r => setTimeout(r, delay + jitter));
      try {
        if (this.transport === 'sse' && this.sseUrl) {
          await this.connectSSE(this.sseUrl);
        } else if (this.transport === 'http' && this.sseUrl) {
          // HTTP transport must re-establish over HTTP; falling through to
          // connect() would spawn an (empty) stdio command and never reconnect.
          await this.connectHTTP(this.sseUrl);
        } else {
          await this.connect();
        }
        this.reconnectAttempts = 0;
        return;
      } catch {
        this.reconnectAttempts++;
      }
    }
    throw new Error(`MCP reconnection failed after ${this.maxReconnectAttempts} attempts`);
  }

  onToolProgress(callId: string, callback: (progress: { percent?: number; message?: string }) => void): void {
    this.progressCallbacks.set(callId, callback);
  }

  hasCapability(name: string): boolean {
    return name in this._serverCapabilities;
  }

  getRoots(): Array<{ uri: string; name?: string }> {
    return this.rootPaths.map(p => ({
      uri: `file://${p}`,
      name: p.split('/').pop() || p,
    }));
  }

  disconnect(): void {
    if (this.hasTriggeredClose && !this.process) return;
    const wasConnected = this._state === 'ready' || this._state === 'connecting';
    this.hasTriggeredClose = true;
    this.closeHttpStream();

    const proc = this.process;
    const pid = this.processPid;
    const uptimeMs = this.uptime;
    this.process = null;
    this.processPid = null;
    this.connectionStartTime = null;

    if (proc) {
      this.gracefulShutdown(proc, pid);
    }

    this._state = 'disconnected';
    this.rejectAllPending(new Error('Client disconnected'));

    if (wasConnected) {
      this.emit('close', null, uptimeMs);
    }
  }

  private gracefulShutdown(proc: ChildProcess, pid: number | null): void {
    if (!pid) {
      try { proc.kill('SIGKILL'); } catch {}
      return;
    }

    const processExists = (): boolean => {
      try { process.kill(pid, 0); return true; } catch { return false; }
    };

    if (!processExists()) return;

    try { proc.kill('SIGINT'); } catch {}

    const checkAfterSigint = setTimeout(() => {
      if (!processExists()) return;
      try { proc.kill('SIGTERM'); } catch {}

      const checkAfterSigterm = setTimeout(() => {
        if (!processExists()) return;
        try { process.kill(pid, 'SIGKILL'); } catch {}
      }, SHUTDOWN_SIGTERM_MS);
      checkAfterSigterm.unref();
    }, SHUTDOWN_SIGINT_MS);
    checkAfterSigint.unref();

    const failsafe = setTimeout(() => {
      if (processExists()) {
        try { process.kill(pid, 'SIGKILL'); } catch {}
      }
    }, SHUTDOWN_TOTAL_MS);
    failsafe.unref();
  }

  classifyError(err: Error): McpTerminalError {
    const errWithCode = err as NodeJS.ErrnoException;
    const code = errWithCode.code;
    const msg = err.message?.toLowerCase() ?? '';

    if (code && TERMINAL_ERROR_CODES.has(code)) {
      return { code: code as McpTerminalErrorCode, message: this.errorContext(code), original: err };
    }
    if (msg.includes('spawn')) {
      return { code: 'SPAWN_ERROR', message: 'Failed to spawn MCP server process', original: err };
    }
    if (msg.includes('terminated') || msg.includes('killed')) {
      return { code: 'ESRCH', message: 'MCP server process terminated', original: err };
    }
    return { code: 'UNKNOWN', message: err.message, original: err };
  }

  private errorContext(code: string): string {
    switch (code) {
      case 'ECONNRESET': return 'Connection reset by peer — server crashed or restarted';
      case 'ETIMEDOUT': return 'Connection timed out — network issue or server unresponsive';
      case 'EPIPE': return 'Broken pipe — server closed unexpectedly';
      case 'EHOSTUNREACH': return 'Host unreachable — network connectivity issue';
      case 'ECONNREFUSED': return 'Connection refused — server is down';
      case 'ESRCH': return 'Process not found — server terminated';
      default: return `System error: ${code}`;
    }
  }

  private isSessionExpiredBody(body: string): boolean {
    return body.includes('"code":-32001') || body.includes('"code": -32001');
  }

  private assertReady(): void {
    if (this._state !== 'ready') {
      throw new Error(`MCP client not ready (state: ${this._state})`);
    }
  }

  private async sendRequestAuto(method: string, params: Record<string, unknown>, signal?: AbortSignal): Promise<unknown> {
    if ((this.transport === 'sse' || this.transport === 'http') && this.sseUrl) {
      return this.sendHttpRequest(method, params, signal);
    }
    return this.sendRequest(method, params, signal);
  }

  private async sendHttpRequest(method: string, params: Record<string, unknown>, signal?: AbortSignal, retried = false): Promise<unknown> {
    const isNotification = method.startsWith('notifications/');
    const msg: Record<string, unknown> = { jsonrpc: '2.0', method, params };
    if (!isNotification) {
      msg.id = ++this.requestId;
    }
    const body = JSON.stringify(msg);
    const http = this.sseUrl!.startsWith('https') ? require('https') : require('http');
    // OAuth 2.1 (2025-06-18): attach the current bearer token, if any, before the request.
    const authHeader = this.oauth ? await this.oauth.authorizationHeader() : null;
    return new Promise((resolve, reject) => {
      if (signal?.aborted) {
        reject(new Error('MCP request aborted'));
        return;
      }

      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
        'Accept': 'application/json, text/event-stream',
        ...this.headers,
      };
      // 2025-06-18: every HTTP request after the initialize handshake MUST carry
      // the negotiated protocol version so the server pins compatible framing.
      if (method !== 'initialize' && this._protocolVersion) {
        headers['MCP-Protocol-Version'] = this._protocolVersion;
      }
      // Echo the server-assigned session id (if any) on every subsequent request.
      if (this.httpSessionId) {
        headers['Mcp-Session-Id'] = this.httpSessionId;
      }
      if (authHeader && !headers.Authorization && !headers.authorization) {
        headers.Authorization = authHeader;
      }
      const req = http.request(new URL(this.sseUrl!), {
        method: 'POST',
        headers,
        timeout: this.requestTimeout,
      }, (res: any) => {
        const statusCode = res.statusCode;
        // Capture the session id the server assigns at initialize (2025 spec).
        const sid = res.headers?.['mcp-session-id'];
        if (typeof sid === 'string' && sid) this.httpSessionId = sid;
        let data = '';
        res.on('data', (d: Buffer) => data += d);
        res.on('end', () => {
          // Notifications: server responds with 202 Accepted (empty body)
          if (isNotification) {
            resolve(undefined);
            return;
          }
          // OAuth 2.1: a 401 triggers (re)authorization; on success retry once.
          if (statusCode === 401 && this.oauth && !retried) {
            const www = (res.headers?.['www-authenticate'] as string | undefined) ?? null;
            this.oauth.onUnauthorized(www).then((ok: boolean) => {
              if (ok) {
                this.sendHttpRequest(method, params, signal, true).then(resolve, reject);
              } else {
                reject(new Error('MCP HTTP 401 Unauthorized (authorization failed)'));
              }
            }).catch(reject);
            return;
          }
          if (statusCode === 404 && this.isSessionExpiredBody(data)) {
            this._state = 'error';
            this.emit('session-expired');
            reject(new McpSessionExpiredError());
            return;
          }
          try {
            // Try direct JSON parse first; if that fails, extract from
            // SSE framing (text/event-stream: "event: message\ndata: {…}")
            let jsonStr = data;
            try {
              JSON.parse(jsonStr);
            } catch {
              const dataLine = data.split('\n').find(l => l.startsWith('data: '));
              if (dataLine) {
                jsonStr = dataLine.slice(6);
              }
            }
            const parsed = JSON.parse(jsonStr);
            if (parsed.error) {
              if (parsed.error.code === -32001) {
                this._state = 'error';
                this.emit('session-expired');
                reject(new McpSessionExpiredError());
                return;
              }
              reject(new Error(`MCP error ${parsed.error.code}: ${parsed.error.message}`));
            } else {
              resolve(parsed.result);
            }
          } catch {
            reject(new Error('Invalid JSON response from MCP SSE server'));
          }
        });
      });

      const abortHandler = () => {
        req.destroy();
        reject(new Error('MCP request aborted'));
      };
      if (signal) {
        signal.addEventListener('abort', abortHandler, { once: true });
        req.on('close', () => signal.removeEventListener('abort', abortHandler));
      }

      req.on('error', (err: Error) => {
        const classified = this.classifyError(err);
        if (classified.code !== 'UNKNOWN') {
          this.emit('terminal-error', classified);
        }
        reject(err);
      });
      req.on('timeout', () => { req.destroy(); reject(new Error(`MCP SSE request '${method}' timed out`)); });
      req.write(body);
      req.end();
    });
  }

  /**
   * Streamable-HTTP server→client channel (MCP spec): after initialization open a
   * long-lived GET with `Accept: text/event-stream` (carrying the negotiated
   * protocol version + any `Mcp-Session-Id`) so the server can push JSON-RPC
   * notifications and requests (sampling, elicitation, progress, roots). Incoming
   * messages route through the SAME dispatcher the stdio/sse transports use, and
   * server→client requests are answered via respondToServerRequest. A server that
   * does not offer the stream replies 405 (Method Not Allowed) — we then degrade
   * to POST-only without erroring. An unexpected close reconnects with bounded
   * exponential backoff (mirroring the client's reconnect policy).
   */
  private async openHttpStream(): Promise<void> {
    if (this.transport !== 'http' || !this.sseUrl || this.httpStreamClosed) return;
    const http = this.sseUrl.startsWith('https') ? require('https') : require('http');
    const headers = await this.buildStreamHeaders('text/event-stream');
    if (this.httpStreamClosed) return;
    this.httpStreamBuffer = '';
    const req = http.request(new URL(this.sseUrl), { method: 'GET', headers }, (res: any) => {
      const statusCode = res.statusCode;
      // 405 / 404 / 501 → the server does not offer the GET stream: POST-only.
      if (statusCode === 405 || statusCode === 404 || statusCode === 501) {
        res.resume();
        this.httpStreamReq = null;
        this.emit('http-stream-unsupported', statusCode);
        return;
      }
      if (statusCode !== 200) {
        res.resume();
        this.httpStreamReq = null;
        this.scheduleHttpStreamReconnect();
        return;
      }
      this.httpStreamReconnectAttempts = 0;
      res.setEncoding('utf-8');
      res.on('data', (chunk: string) => {
        this.httpStreamBuffer += chunk;
        this.processHttpStreamBuffer();
      });
      res.on('end', () => { this.httpStreamReq = null; this.scheduleHttpStreamReconnect(); });
      res.on('error', () => { this.httpStreamReq = null; this.scheduleHttpStreamReconnect(); });
    });
    req.on('error', () => { this.httpStreamReq = null; this.scheduleHttpStreamReconnect(); });
    this.httpStreamReq = req;
    req.end();
  }

  /** Shared header builder for the GET stream + POSTed server-request responses. */
  private async buildStreamHeaders(accept: string): Promise<Record<string, string>> {
    const authHeader = this.oauth ? await this.oauth.authorizationHeader() : null;
    const headers: Record<string, string> = { Accept: accept, ...this.headers };
    if (this._protocolVersion) headers['MCP-Protocol-Version'] = this._protocolVersion;
    if (this.httpSessionId) headers['Mcp-Session-Id'] = this.httpSessionId;
    if (authHeader && !headers.Authorization && !headers.authorization) headers.Authorization = authHeader;
    return headers;
  }

  /** Drain every complete SSE event from the GET stream buffer. */
  private processHttpStreamBuffer(): void {
    this.httpStreamBuffer = this.httpStreamBuffer.replace(/\r\n/g, '\n');
    let sep: number;
    while ((sep = this.httpStreamBuffer.indexOf('\n\n')) !== -1) {
      const rawEvent = this.httpStreamBuffer.slice(0, sep);
      this.httpStreamBuffer = this.httpStreamBuffer.slice(sep + 2);
      this.handleSseStreamEvent(rawEvent);
    }
  }

  /** Parse one SSE event's `data:` payload(s) and route the JSON-RPC message. */
  private handleSseStreamEvent(rawEvent: string): void {
    const dataParts: string[] = [];
    for (const line of rawEvent.split('\n')) {
      if (line.startsWith('data:')) {
        dataParts.push(line.slice(line.startsWith('data: ') ? 6 : 5));
      }
    }
    if (dataParts.length === 0) return;
    const payload = dataParts.join('\n');
    try {
      const parsed = JSON.parse(payload);
      if (Array.isArray(parsed)) {
        for (const m of parsed) this.routeMessage(m as JsonRpcResponse | JsonRpcNotification);
      } else {
        this.routeMessage(parsed as JsonRpcResponse | JsonRpcNotification);
      }
    } catch {
      this.emit('parse-error', payload);
    }
  }

  private scheduleHttpStreamReconnect(): void {
    if (this.httpStreamClosed || this._state !== 'ready') return;
    if (this.httpStreamReconnectAttempts >= this.maxReconnectAttempts) {
      this.emit('http-stream-closed');
      return;
    }
    const attempt = this.httpStreamReconnectAttempts++;
    const delay = Math.min(HTTP_STREAM_RECONNECT_BASE_MS * Math.pow(2, attempt), HTTP_STREAM_RECONNECT_MAX_MS);
    const timer = setTimeout(() => {
      if (this.httpStreamClosed || this._state !== 'ready') return;
      this.openHttpStream().catch(() => { /* a later close retries, bounded */ });
    }, delay);
    timer.unref?.();
  }

  /** Tear down the GET stream and stop any reconnect (called from disconnect). */
  private closeHttpStream(): void {
    this.httpStreamClosed = true;
    const req = this.httpStreamReq;
    this.httpStreamReq = null;
    if (req) {
      try { req.destroy(); } catch { /* already gone */ }
    }
  }

  /** POST a JSON-RPC response back to the HTTP endpoint for a server→client request. */
  private postServerResponse(id: number, payload: { result: unknown } | { error: { code: number; message: string } }): void {
    const url = this.sseUrl;
    if (!url) return;
    const http = url.startsWith('https') ? require('https') : require('http');
    this.buildStreamHeaders('application/json, text/event-stream').then((streamHeaders) => {
      const headers = { 'Content-Type': 'application/json', ...streamHeaders };
      const req = http.request(new URL(url), { method: 'POST', headers, timeout: this.requestTimeout }, (res: any) => res.resume());
      req.on('error', () => { /* best-effort: the server times out its own request */ });
      req.on('timeout', () => { try { req.destroy(); } catch { /* ignore */ } });
      req.write(JSON.stringify({ jsonrpc: '2.0', id, ...payload }));
      req.end();
    }).catch(() => { /* best-effort */ });
  }

  private sendRequest(method: string, params: Record<string, unknown>, signal?: AbortSignal): Promise<unknown> {
    return new Promise((resolve, reject) => {
      if (signal?.aborted) {
        reject(new Error('MCP request aborted'));
        return;
      }

      const id = ++this.requestId;
      const request: JsonRpcRequest = {
        jsonrpc: '2.0',
        id,
        method,
        params,
      };

      const timer = setTimeout(() => {
        this.pendingRequests.delete(id);
        reject(new Error(`MCP request '${method}' timed out after ${this.requestTimeout}ms`));
      }, this.requestTimeout);

      let abortHandler: (() => void) | undefined;
      if (signal) {
        abortHandler = () => {
          clearTimeout(timer);
          this.pendingRequests.delete(id);
          this.sendCancelled(id, 'client aborted');
          reject(new Error('MCP request aborted'));
        };
        signal.addEventListener('abort', abortHandler, { once: true });
      }

      this.pendingRequests.set(id, { resolve, reject, timer, abortHandler });
      this.writeMessage(request);
    });
  }

  /** notifications/cancelled — tell the server we abandoned an in-flight request. */
  private sendCancelled(requestId: number, reason: string): void {
    try {
      if (this.process?.stdin?.writable) {
        this.writeMessage({ jsonrpc: '2.0', method: 'notifications/cancelled', params: { requestId, reason } });
      }
    } catch {
      /* best-effort: the connection may already be gone */
    }
  }

  private sendNotification(method: string, params: Record<string, unknown>): Promise<void> {
    const notification: JsonRpcNotification = {
      jsonrpc: '2.0',
      method,
      params,
    };
    this.writeMessage(notification);
    return Promise.resolve();
  }

  private writeMessage(msg: JsonRpcRequest | JsonRpcNotification | JsonRpcResponse): void {
    if (!this.process?.stdin?.writable) {
      throw new Error('MCP server stdin not writable');
    }
    const json = JSON.stringify(msg);
    this.process.stdin.write(json + '\n');
  }

  /**
   * Reply to a server→client JSON-RPC request. Used by the ping/roots/
   * elicitation/sampling handlers so no server request is ever left unanswered.
   * Stdio writes the response to the child's stdin; the Streamable-HTTP transport
   * (whose server→client requests arrive on the GET SSE stream) POSTs it back to
   * the same endpoint. With neither available this is a safe no-op.
   */
  private respondToServerRequest(id: number, payload: { result: unknown } | { error: { code: number; message: string } }): void {
    if (this.process?.stdin?.writable) {
      this.writeMessage({ jsonrpc: '2.0', id, ...payload } as JsonRpcResponse);
      return;
    }
    if ((this.transport === 'http' || this.transport === 'sse') && this.sseUrl) {
      this.postServerResponse(id, payload);
    }
  }

  private processBuffer(): void {
    const lines = this.buffer.split('\n');
    this.buffer = lines.pop() ?? '';

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        const msg = JSON.parse(trimmed) as JsonRpcResponse | JsonRpcNotification;
        this.routeMessage(msg);
      } catch {
        this.emit('parse-error', trimmed);
      }
    }

    // L31: bound the accumulated read buffer. Every complete (newline-delimited)
    // message above has already been routed; what remains is a single
    // not-yet-terminated line. If that remainder exceeds the cap, a server is
    // either never sending a newline or streaming one oversized line — drop it
    // and signal the overflow rather than letting the buffer grow without bound.
    if (this.buffer.length > MCP_MAX_STDIO_BUFFER) {
      const droppedBytes = this.buffer.length;
      this.buffer = '';
      this.emit(
        'protocol-error',
        `MCP stdio read buffer exceeded ${MCP_MAX_STDIO_BUFFER} bytes without a complete message; dropped ${droppedBytes} buffered bytes`,
      );
    }
  }

  /**
   * Dispatch a parsed JSON-RPC message to the right handler. Shared by the stdio
   * read loop (processBuffer) and the Streamable-HTTP GET stream so server→client
   * requests and notifications are handled identically across transports.
   */
  private routeMessage(msg: JsonRpcResponse | JsonRpcNotification): void {
    if ('id' in msg && msg.id !== undefined && 'method' in msg) {
      this.handleNotification(msg as JsonRpcNotification);
    } else if ('id' in msg && msg.id !== undefined) {
      this.handleResponse(msg as JsonRpcResponse);
    } else if ('method' in msg) {
      this.handleNotification(msg as JsonRpcNotification);
    }
  }

  private handleResponse(response: JsonRpcResponse): void {
    const pending = this.pendingRequests.get(response.id);
    if (!pending) return;

    clearTimeout(pending.timer);
    if (pending.abortHandler) {
      // Can't remove from signal directly, but the once:true handles cleanup
    }
    this.pendingRequests.delete(response.id);

    if (response.error) {
      if (response.error.code === -32001) {
        this.emit('session-expired');
        pending.reject(new McpSessionExpiredError());
        return;
      }
      pending.reject(new Error(`MCP error ${response.error.code}: ${response.error.message}`));
    } else {
      pending.resolve(response.result);
    }
  }

  private handleNotification(notification: JsonRpcNotification): void {
    const id = (notification as JsonRpcNotification & { id?: number }).id;

    // --- Server→client REQUESTS (carry an id; MUST be answered) -------------
    if (notification.method === 'elicitation/create' || notification.method === 'notifications/elicitation') {
      void this.handleElicitation(notification as JsonRpcNotification & { id?: number });
    }

    if (notification.method === 'sampling/createMessage') {
      void this.handleSampling(notification as JsonRpcNotification & { id?: number });
    }

    if (notification.method === 'ping' && id !== undefined) {
      this.respondToServerRequest(id, { result: {} });
    }

    if (notification.method === 'roots/list' && id !== undefined) {
      this.respondToServerRequest(id, { result: { roots: this.getRoots() } });
    }

    // --- Server→client NOTIFICATIONS (no reply) ----------------------------
    if (notification.method === 'notifications/tools/list_changed') {
      this._tools = [];
      this.emit('tools-changed');
    }

    if (notification.method === 'notifications/resources/list_changed') {
      this._resources = [];
      this.emit('resources-changed');
    }

    if (notification.method === 'notifications/resources/updated') {
      const params = notification.params as { uri?: string } | undefined;
      if (params?.uri) this.emit('resource-updated', params.uri);
    }

    if (notification.method === 'notifications/prompts/list_changed') {
      this._prompts = [];
      this.emit('prompts-changed');
    }

    if (notification.method === 'notifications/message') {
      const params = notification.params as Partial<McpLogMessage> | undefined;
      if (params) {
        this.emit('log', { level: params.level ?? 'info', logger: params.logger, data: params.data } as McpLogMessage);
      }
    }

    if (notification.method === 'notifications/progress') {
      const params = notification.params as { progressToken?: string; progress?: number; total?: number; message?: string } | undefined;
      if (params?.progressToken) {
        const cb = this.progressCallbacks.get(String(params.progressToken));
        if (cb) {
          const percent = params.progress != null && params.total
            ? Math.round((params.progress / params.total) * 100)
            : undefined;
          cb({ percent, message: params.message });
        }
      }
    }

    this.emit('notification', notification.method, notification.params);
  }

  /**
   * Resolve a server elicitation request: ask the user via the injected handler,
   * then reply with the chosen action. Never blocks the read loop and never
   * leaves the request unanswered — absent a handler it declines, on a handler
   * error it cancels, so the server can always proceed.
   */
  private async handleElicitation(notification: JsonRpcNotification & { id?: number }): Promise<void> {
    const raw = (notification.params ?? {}) as Partial<McpElicitationRequest>;
    const request: McpElicitationRequest = {
      message: typeof raw.message === 'string' ? raw.message : '',
      requestedSchema:
        raw.requestedSchema && typeof raw.requestedSchema === 'object'
          ? (raw.requestedSchema as Record<string, unknown>)
          : {},
    };
    let result: McpElicitationResult;
    if (this.onElicitation) {
      try {
        result = await this.onElicitation(request);
      } catch {
        result = { action: 'cancel' };
      }
    } else {
      result = { action: 'decline' };
    }
    if (notification.id !== undefined && notification.id !== null) {
      this.respondToServerRequest(notification.id, { result });
    }
  }

  /**
   * Fulfill a server sampling request by running an LLM turn through the injected
   * handler, then reply with the completion. Absent a handler, or on error, it
   * replies with a JSON-RPC error (−32601 / −32603) so the server is not left
   * hanging and can fall back.
   */
  private async handleSampling(notification: JsonRpcNotification & { id?: number }): Promise<void> {
    const id = notification.id;
    if (id === undefined || id === null) return;
    if (!this.onSampling) {
      this.respondToServerRequest(id, { error: { code: -32601, message: 'sampling not supported by this client' } });
      return;
    }
    try {
      const result = await this.onSampling((notification.params ?? {}) as unknown as McpSamplingRequest);
      this.respondToServerRequest(id, { result });
    } catch (err) {
      this.respondToServerRequest(id, { error: { code: -32603, message: err instanceof Error ? err.message : 'sampling failed' } });
    }
  }

  private rejectAllPending(error: Error): void {
    for (const [id, pending] of this.pendingRequests) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pendingRequests.clear();
  }
}
