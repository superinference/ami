import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  McpManager,
  isValidCallbackPort,
  resolveMcpOAuthRedirect,
  type McpServerConfig,
} from '../src/mcp/manager';
import {
  sanitizeMcpLogSegment,
  mcpLogDateStamp,
  getMcpServerLogPath,
  logMcpError,
  logMcpDebug,
  type McpErrorLogRecord,
  type McpDebugLogRecord,
} from '../src/mcp/log-sink';

// ---------------------------------------------------------------------------
// U018-w70 — additive MCP manager parity (Claude Code):
//   * Per-MCP-server structured log sink (getMCPLogsPath + logMCPError/logMCPDebug):
//     each server gets its own dated JSONL file with distinct error/debug record
//     shapes, credential-redacted, wired into the manager's client-event handlers.
//   * Per-server OAuth `callbackPort` config knob (mcp add --callback-port): a fixed
//     loopback redirect port per server, taking precedence over the global env knob.
// ---------------------------------------------------------------------------

/** Minimal fake MCP client the factory seam returns; event-driven so the manager's
 *  per-server logging handlers can be exercised without a real server. */
class FakeClient extends EventEmitter {
  state = 'idle';
  serverInfo: unknown = { name: 'fake', version: '1' };
  instructions: string | null = null;
  uptime: number | null = 1;
  tools: Array<{ name: string; description?: string }> = [];
  resources: unknown[] = [];
  prompts: unknown[] = [];
  onElicitation: unknown;
  onSampling: unknown;
  connectImpl: () => Promise<void> = async () => { this.state = 'ready'; };
  async connect(): Promise<void> { await this.connectImpl(); }
  async connectHTTP(): Promise<void> { await this.connectImpl(); }
  async connectSSE(): Promise<void> { await this.connectImpl(); }
  async listTools(): Promise<Array<{ name: string; description?: string }>> { return this.tools; }
  async listResources(): Promise<unknown[]> { return this.resources; }
  async reconnect(): Promise<void> {}
  disconnect(): void { this.state = 'stopped'; }
  setHeaders(): void {}
  async revokeAuth(): Promise<void> {}
}

const tmpDirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-logsink-'));
  tmpDirs.push(d);
  return d;
}
after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch {} }
});

// ===========================================================================
// sanitizeMcpLogSegment
// ===========================================================================
describe('sanitizeMcpLogSegment', () => {
  it('keeps a safe name unchanged', () => {
    assert.equal(sanitizeMcpLogSegment('github.server-1'), 'github.server-1');
  });
  it('replaces path separators, colons, and spaces with underscores', () => {
    assert.equal(sanitizeMcpLogSegment('a/b:c d'), 'a_b_c_d');
  });
  it('collapses an all-dot (traversal) name to a single underscore', () => {
    assert.equal(sanitizeMcpLogSegment('..'), '_');
  });
  it('collapses an empty / fully-stripped name to a single underscore', () => {
    assert.equal(sanitizeMcpLogSegment(''), '_');
    assert.equal(sanitizeMcpLogSegment('***'), '___'); // each char -> underscore, not empty
  });
});

// ===========================================================================
// mcpLogDateStamp + getMcpServerLogPath
// ===========================================================================
describe('mcpLogDateStamp / getMcpServerLogPath', () => {
  it('produces a UTC YYYY-MM-DD stamp for an injected clock', () => {
    assert.equal(mcpLogDateStamp(new Date('2026-11-17T23:59:59Z')), '2026-11-17');
  });
  it('builds a per-server dated JSONL path under the config dir', () => {
    const p = getMcpServerLogPath('my:server', { configDir: '/cfg', now: new Date('2026-01-02T00:00:00Z') });
    assert.equal(p, path.join('/cfg', 'mcp-logs-my_server', '2026-01-02.jsonl'));
  });
});

// ===========================================================================
// logMcpError / logMcpDebug (real on-disk JSONL)
// ===========================================================================
describe('logMcpError / logMcpDebug', () => {
  it('writes a credential-redacted error record (Error -> stack/message)', () => {
    const dir = mkTmp();
    const now = new Date('2026-03-04T00:00:00Z');
    logMcpError('srv', new Error('connect failed: Authorization: Bearer sk-abcdef0123456789ABCDEF0123'), {
      configDir: dir, now, cwd: '/work', sessionId: 'sess-1',
    });
    const file = getMcpServerLogPath('srv', { configDir: dir, now });
    const line = fs.readFileSync(file, 'utf-8').trim();
    const rec = JSON.parse(line) as McpErrorLogRecord;
    assert.equal(rec.cwd, '/work');
    assert.equal(rec.sessionId, 'sess-1');
    assert.equal(rec.timestamp, now.toISOString());
    assert.ok(rec.error.includes('[REDACTED]'), 'secret must be redacted');
    assert.ok(!rec.error.includes('sk-abcdef0123456789ABCDEF0123'), 'raw secret must not be persisted');
  });

  it('stringifies a non-Error value and omits sessionId when unset', () => {
    const dir = mkTmp();
    const now = new Date('2026-03-05T00:00:00Z');
    logMcpError('srv', { code: 'EBOOM' }, { configDir: dir, now, cwd: '/w' });
    const rec = JSON.parse(fs.readFileSync(getMcpServerLogPath('srv', { configDir: dir, now }), 'utf-8').trim()) as McpErrorLogRecord;
    assert.equal(rec.error, '[object Object]');
    assert.equal(rec.sessionId, undefined);
  });

  it('appends a debug record and keeps multiple entries one-per-line', () => {
    const dir = mkTmp();
    const now = new Date('2026-03-06T00:00:00Z');
    logMcpDebug('srv', 'first', { configDir: dir, now });
    logMcpDebug('srv', 'second', { configDir: dir, now });
    const lines = fs.readFileSync(getMcpServerLogPath('srv', { configDir: dir, now }), 'utf-8').trim().split('\n');
    assert.equal(lines.length, 2);
    assert.equal((JSON.parse(lines[0]) as McpDebugLogRecord).debug, 'first');
    assert.equal((JSON.parse(lines[1]) as McpDebugLogRecord).debug, 'second');
  });

  it('is best-effort: a write to an impossible path never throws', () => {
    const dir = mkTmp();
    // Create a FILE where the per-server log dir would be, so mkdirSync fails (ENOTDIR).
    const blocker = path.join(dir, 'mcp-logs-srv');
    fs.writeFileSync(blocker, 'x');
    assert.doesNotThrow(() => logMcpError('srv', new Error('boom'), { configDir: dir, now: new Date('2026-03-07T00:00:00Z') }));
  });

  it('defaults the clock/cwd when no seams are supplied', () => {
    const dir = mkTmp();
    logMcpDebug('srv', 'defaulted', { configDir: dir });
    // The file name uses today's UTC date; just assert exactly one file was written.
    const serverDir = path.join(dir, 'mcp-logs-srv');
    const files = fs.readdirSync(serverDir);
    assert.equal(files.length, 1);
    const rec = JSON.parse(fs.readFileSync(path.join(serverDir, files[0]), 'utf-8').trim()) as McpDebugLogRecord;
    assert.equal(rec.cwd, process.cwd());
    assert.ok(typeof rec.timestamp === 'string' && rec.timestamp.length > 0);
  });
});

// ===========================================================================
// Manager wiring: client events route to the per-server log sink
// ===========================================================================
describe('McpManager per-server logging wiring', () => {
  interface LogCall { kind: 'error' | 'debug'; server: string; payload: unknown; sessionId?: string }
  function managerWithCapturedSink(): { mgr: McpManager; client: FakeClient; calls: LogCall[] } {
    const mgr = new McpManager();
    const client = new FakeClient();
    const calls: LogCall[] = [];
    mgr._createClient = (() => client) as never;
    mgr._logMcpError = (server, payload, opts) => calls.push({ kind: 'error', server, payload, sessionId: opts?.sessionId });
    mgr._logMcpDebug = (server, payload, opts) => calls.push({ kind: 'debug', server, payload, sessionId: opts?.sessionId });
    return { mgr, client, calls };
  }

  it("routes a client 'error' event to the per-server error log AND still emits server-error", () => {
    const { mgr, client, calls } = managerWithCapturedSink();
    let emitted = false;
    mgr.on('server-error', () => { emitted = true; });
    mgr.addServer('srv', { command: 'node' });
    const err = new Error('kaboom');
    client.emit('error', err);
    assert.deepEqual(calls, [{ kind: 'error', server: 'srv', payload: err, sessionId: undefined }]);
    assert.equal(emitted, true);
    mgr.disconnectAll();
  });

  it("routes a terminal-error to the per-server error log with code+message", () => {
    const { mgr, client, calls } = managerWithCapturedSink();
    mgr.addServer('srv', { command: 'node' });
    client.emit('terminal-error', { code: 'spawn-failed', message: 'nope' });
    assert.equal(calls.length, 1);
    assert.equal(calls[0].kind, 'error');
    assert.equal(calls[0].payload, '[spawn-failed] nope');
    mgr.disconnectAll();
  });

  it("routes an error-level 'log' to error and other levels to debug", () => {
    const { mgr, client, calls } = managerWithCapturedSink();
    mgr.addServer('srv', { command: 'node' });
    client.emit('log', { level: 'error', data: 'bad thing' });
    client.emit('log', { level: 'info', data: { msg: 'hi' } });
    assert.equal(calls[0].kind, 'error');
    assert.equal(calls[0].payload, 'bad thing');
    assert.equal(calls[1].kind, 'debug');
    assert.equal(calls[1].payload, JSON.stringify({ msg: 'hi' }));
    mgr.disconnectAll();
  });

  it('stamps the configured session id into log-sink options', () => {
    const { mgr, client, calls } = managerWithCapturedSink();
    mgr.setMcpLogSessionId('S-42');
    mgr.addServer('srv', { command: 'node' });
    client.emit('error', new Error('x'));
    assert.equal(calls[0].sessionId, 'S-42');
    mgr.disconnectAll();
  });

  it('logs the final error when a connect attempt exhausts retries', async () => {
    const { mgr, client, calls } = managerWithCapturedSink();
    client.connectImpl = async () => { throw new Error('exited with code 1'); }; // handshake failure -> no retry
    mgr.addServer('srv', { command: 'node' });
    await assert.rejects(() => mgr.connectServer('srv'));
    const errCalls = calls.filter((c) => c.kind === 'error');
    assert.equal(errCalls.length, 1);
    assert.ok(String((errCalls[0].payload as Error).message).includes('handshake'));
    mgr.disconnectAll();
  });

  it('getServerLogPath exposes the dated per-server log path', () => {
    const mgr = new McpManager();
    const p = mgr.getServerLogPath('weird name');
    assert.ok(p.includes(`mcp-logs-${sanitizeMcpLogSegment('weird name')}`));
    assert.ok(p.endsWith('.jsonl'));
  });
});

// ===========================================================================
// End-to-end: a real (default) sink writes a readable file on a real error event
// ===========================================================================
describe('McpManager per-server logging — real on-disk E2E', () => {
  it('writes a server error to its dedicated JSONL file via the default sink', () => {
    const dir = mkTmp();
    const prev = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = dir;
    try {
      const mgr = new McpManager();
      const client = new FakeClient();
      mgr._createClient = (() => client) as never;
      mgr.setMcpLogSessionId('e2e');
      mgr.addServer('realsrv', { command: 'node' });
      client.emit('error', new Error('disk-level failure'));
      const logPath = mgr.getServerLogPath('realsrv');
      const rec = JSON.parse(fs.readFileSync(logPath, 'utf-8').trim()) as McpErrorLogRecord;
      assert.ok(rec.error.includes('disk-level failure'));
      assert.equal(rec.sessionId, 'e2e');
      mgr.disconnectAll();
    } finally {
      if (prev === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
      else process.env.SUPERINFERENCE_DATA_DIR = prev;
    }
  });
});

// ===========================================================================
// isValidCallbackPort
// ===========================================================================
describe('isValidCallbackPort', () => {
  it('accepts an in-range positive integer', () => {
    assert.equal(isValidCallbackPort(8080), true);
    assert.equal(isValidCallbackPort(1), true);
    assert.equal(isValidCallbackPort(65535), true);
  });
  it('rejects undefined, zero, negative, non-integer, and out-of-range', () => {
    assert.equal(isValidCallbackPort(undefined), false);
    assert.equal(isValidCallbackPort(0), false);
    assert.equal(isValidCallbackPort(-1), false);
    assert.equal(isValidCallbackPort(1.5), false);
    assert.equal(isValidCallbackPort(70000), false);
    assert.equal(isValidCallbackPort(NaN), false);
  });
});

// ===========================================================================
// resolveMcpOAuthRedirect — precedence
// ===========================================================================
describe('resolveMcpOAuthRedirect', () => {
  it('an explicit redirectUri wins over everything (no dynamic lookup)', () => {
    const r = resolveMcpOAuthRedirect({ redirectUri: 'http://host/cb', callbackPort: 9000 }, 7000);
    assert.deepEqual(r, { redirectUri: 'http://host/cb', dynamic: false });
  });
  it('a valid per-server callbackPort wins over the env port', () => {
    const r = resolveMcpOAuthRedirect({ callbackPort: 9000 }, 7000);
    assert.equal(r.redirectUri, 'http://127.0.0.1:9000/oauth/callback');
    assert.equal(r.dynamic, false);
  });
  it('falls back to the env port when no per-server port', () => {
    const r = resolveMcpOAuthRedirect({}, 7000);
    assert.equal(r.redirectUri, 'http://127.0.0.1:7000/oauth/callback');
    assert.equal(r.dynamic, false);
  });
  it('an invalid callbackPort falls through to the env port', () => {
    const r = resolveMcpOAuthRedirect({ callbackPort: 0 }, 7000);
    assert.equal(r.redirectUri, 'http://127.0.0.1:7000/oauth/callback');
    assert.equal(r.dynamic, false);
  });
  it('is dynamic (free-port discovery) when neither per-server nor env port is set', () => {
    const r = resolveMcpOAuthRedirect({}, undefined);
    // default-port seed, but dynamic resolver is requested.
    assert.equal(r.dynamic, true);
    assert.ok(r.redirectUri.startsWith('http://127.0.0.1:'));
  });
});

// ===========================================================================
// addServer wires the per-server callbackPort into the OAuth session
// ===========================================================================
describe('McpManager addServer — per-server OAuth callbackPort', () => {
  it('creates an OAuth provider for an http server configured with a callbackPort', () => {
    const mgr = new McpManager();
    let capturedOAuth: unknown;
    mgr._createClient = ((opts: { oauth?: unknown }) => {
      capturedOAuth = opts.oauth;
      return new FakeClient() as never;
    }) as never;
    const config: McpServerConfig = {
      command: '',
      transport: 'http',
      url: 'https://api.example.com/mcp',
      oauth: { callbackPort: 9123 },
    };
    mgr.addServer('oauth-srv', config);
    assert.ok(capturedOAuth, 'an OAuth provider must be wired when oauth config is present');
    mgr.disconnectAll();
  });
});
