import { spawn, ChildProcess } from 'child_process';
import { EventEmitter } from 'events';

export interface McpToolAnnotations {
  title?: string;
  readOnlyHint?: boolean;
  destructiveHint?: boolean;
  openWorldHint?: boolean;
  idempotentHint?: boolean;
}

export interface McpToolSchema {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
  annotations?: McpToolAnnotations;
}

export interface McpResource {
  uri: string;
  name: string;
  mimeType?: string;
}

export interface McpPrompt {
  name: string;
  description?: string;
  arguments?: Array<{ name: string; description?: string; required?: boolean }>;
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

export type McpClientState = 'disconnected' | 'connecting' | 'ready' | 'error';

export type McpTerminalErrorCode =
  | 'ECONNRESET'
  | 'ETIMEDOUT'
  | 'EPIPE'
  | 'EHOSTUNREACH'
  | 'ECONNREFUSED'
  | 'ESRCH'
  | 'SPAWN_ERROR'
  | 'SESSION_EXPIRED'
  | 'UNKNOWN';

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
  private _tools: McpToolSchema[] = [];
  private _resources: McpResource[] = [];
  private _prompts: McpPrompt[] = [];
  private transport: 'stdio' | 'sse' | 'http' = 'stdio';
  private sseUrl: string | null = null;
  private readonly command: string;
  private readonly args: string[];
  private readonly env: Record<string, string>;
  private readonly requestTimeout: number;
  private readonly connectTimeout: number;
  private readonly rootPaths: string[];

  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;
  private hasTriggeredClose = false;
  private connectionStartTime: number | null = null;

  private progressCallbacks = new Map<string, (progress: { percent?: number; message?: string }) => void>();

  onElicitation?: (params: unknown) => void;

  constructor(options: {
    command: string;
    args?: string[];
    env?: Record<string, string>;
    requestTimeout?: number;
    connectTimeout?: number;
    rootPaths?: string[];
  }) {
    super();
    this.command = options.command;
    this.args = options.args ?? [];
    this.env = options.env ?? {};
    this.requestTimeout = options.requestTimeout ?? 30000;
    this.connectTimeout = options.connectTimeout ?? 10000;
    this.rootPaths = options.rootPaths ?? [];
  }

  get state(): McpClientState { return this._state; }
  get serverInfo() { return this._serverInfo; }
  get serverCapabilities(): Record<string, unknown> { return { ...this._serverCapabilities }; }
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

  private async performInitialize(): Promise<void> {
    const clientCapabilities: Record<string, unknown> = {};
    if (this.rootPaths.length > 0) {
      clientCapabilities.roots = {};
    }

    const initResult = await this.sendRequest('initialize', {
      protocolVersion: '2024-11-05',
      capabilities: clientCapabilities,
      clientInfo: { name: 'ami', version: '1.0.0' },
    }) as { serverInfo?: { name: string; version: string }; capabilities?: Record<string, unknown> };

    this._serverInfo = initResult.serverInfo ?? { name: 'unknown', version: '0.0.0' };
    this._serverCapabilities = initResult.capabilities ?? {};

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

    const clientCapabilities: Record<string, unknown> = {};
    if (this.rootPaths.length > 0) {
      clientCapabilities.roots = {};
    }

    const initBody = JSON.stringify({
      jsonrpc: '2.0',
      id: 1,
      method: 'initialize',
      params: {
        protocolVersion: '2024-11-05',
        capabilities: clientCapabilities,
        clientInfo: { name: 'ami', version: '1.0.0' },
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
      protocolVersion: '2024-11-05',
      capabilities: { roots: { listChanged: true } },
      clientInfo: { name: 'ami', version: '1.0.0' },
    }) as { serverInfo?: { name: string; version: string }; capabilities?: Record<string, unknown> };

    this._serverInfo = initResult?.serverInfo ?? { name: 'unknown', version: '0.0.0' };
    this._serverCapabilities = initResult?.capabilities ?? {};

    await this.sendHttpRequest('notifications/initialized', {});

    this.connectionStartTime = Date.now();
    this._state = 'ready';
    this.emit('ready');
  }

  async listTools(): Promise<McpToolSchema[]> {
    this.assertReady();
    const result = await this.sendRequestAuto('tools/list', {}) as { tools: McpToolSchema[] };
    this._tools = result.tools ?? [];
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
    const result = await this.sendRequestAuto('resources/list', {}) as { resources: McpResource[] };
    this._resources = result.resources ?? [];
    return this._resources;
  }

  async readResource(uri: string): Promise<unknown> {
    this.assertReady();
    return await this.sendRequestAuto('resources/read', { uri });
  }

  async listPrompts(): Promise<McpPrompt[]> {
    this.assertReady();
    const result = await this.sendRequestAuto('prompts/list', {}) as { prompts: McpPrompt[] };
    this._prompts = result.prompts ?? [];
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

  private sendHttpRequest(method: string, params: Record<string, unknown>, signal?: AbortSignal): Promise<unknown> {
    const id = ++this.requestId;
    const body = JSON.stringify({ jsonrpc: '2.0', id, method, params });
    const http = this.sseUrl!.startsWith('https') ? require('https') : require('http');
    return new Promise((resolve, reject) => {
      if (signal?.aborted) {
        reject(new Error('MCP request aborted'));
        return;
      }

      const req = http.request(new URL(this.sseUrl!), {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json, text/event-stream',
        },
        timeout: this.requestTimeout,
      }, (res: any) => {
        const statusCode = res.statusCode;
        let data = '';
        res.on('data', (d: Buffer) => data += d);
        res.on('end', () => {
          if (statusCode === 404 && this.isSessionExpiredBody(data)) {
            this._state = 'error';
            this.emit('session-expired');
            reject(new McpSessionExpiredError());
            return;
          }
          try {
            const parsed = JSON.parse(data);
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
          reject(new Error('MCP request aborted'));
        };
        signal.addEventListener('abort', abortHandler, { once: true });
      }

      this.pendingRequests.set(id, { resolve, reject, timer, abortHandler });
      this.writeMessage(request);
    });
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

  private writeMessage(msg: JsonRpcRequest | JsonRpcNotification): void {
    if (!this.process?.stdin?.writable) {
      throw new Error('MCP server stdin not writable');
    }
    const json = JSON.stringify(msg);
    this.process.stdin.write(json + '\n');
  }

  private processBuffer(): void {
    const lines = this.buffer.split('\n');
    this.buffer = lines.pop() ?? '';

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        const msg = JSON.parse(trimmed) as JsonRpcResponse | JsonRpcNotification;
        if ('id' in msg && msg.id !== undefined && 'method' in msg) {
          this.handleNotification(msg as JsonRpcNotification);
        } else if ('id' in msg && msg.id !== undefined) {
          this.handleResponse(msg as JsonRpcResponse);
        } else if ('method' in msg) {
          this.handleNotification(msg as JsonRpcNotification);
        }
      } catch {
        this.emit('parse-error', trimmed);
      }
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
    if (notification.method === 'elicitation/create' || notification.method === 'notifications/elicitation') {
      this.onElicitation?.(notification.params);
    }

    if (notification.method === 'notifications/tools/list_changed') {
      this._tools = [];
      this.emit('tools-changed');
    }

    if (notification.method === 'notifications/resources/list_changed') {
      this._resources = [];
      this.emit('resources-changed');
    }

    if (notification.method === 'notifications/prompts/list_changed') {
      this._prompts = [];
      this.emit('prompts-changed');
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

    if (notification.method === 'roots/list') {
      const roots = this.getRoots();
      if (this.process?.stdin?.writable) {
        const response: JsonRpcResponse = {
          jsonrpc: '2.0',
          id: (notification as any).id ?? 0,
          result: { roots },
        };
        this.writeMessage(response as any);
      }
    }

    this.emit('notification', notification.method, notification.params);
  }

  private rejectAllPending(error: Error): void {
    for (const [id, pending] of this.pendingRequests) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pendingRequests.clear();
  }
}
