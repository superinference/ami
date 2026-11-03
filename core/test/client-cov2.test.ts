/**
 * client-cov2.test.ts — meaningful branch-coverage tests for src/mcp/client.ts.
 *
 * ESM-SAFE by construction:
 *   - No source-text reads (no readFileSync of .ts, no "source.includes(...)").
 *   - No monkeypatching of imported namespaces (no `fs.x = ...`). We use real
 *     spawned processes (`node -e <script>`), real http servers bound to port 0,
 *     node:test `mock.method`, and dependency injection via instance casts.
 *   - No __dirname/import.meta.url fixture coupling.
 *
 * Every test asserts observable behavior (return values, emitted events, thrown
 * errors) so it fails if the corresponding branch regresses.
 */

import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'node:http';
import type { AddressInfo } from 'node:net';
import {
  McpClient,
  McpSessionExpiredError,
  boundMcpResponse,
  validateMcpBufferSize,
  DEFAULT_MCP_MAX_RESPONSE_BYTES,
  MCP_MAX_PAGES,
} from '../src/mcp/client';

// NOTE: deliberately no `process.exit` in an after() hook — that would kill the
// shared coverage harness (which runs all test files in one process) before
// other files finish. All spawned children are killed in afterEach so the
// event loop drains and Node exits on its own.

// --- shared cleanup bookkeeping ------------------------------------------
const liveClients: McpClient[] = [];
const liveServers: http.Server[] = [];

afterEach(() => {
  while (liveClients.length) {
    try { liveClients.pop()!.disconnect(); } catch { /* ignore */ }
  }
  while (liveServers.length) {
    try { liveServers.pop()!.close(); } catch { /* ignore */ }
  }
});

function track(c: McpClient): McpClient { liveClients.push(c); c.on('error', () => {}); return c; }

// A minimal stdio MCP server that answers the standard methods. `extra` lets a
// test inject additional per-line behavior. Returns args for `node -e`.
function stdioArgs(extra = ''): string[] {
  const script = `
    const readline = require('readline');
    const rl = readline.createInterface({ input: process.stdin });
    rl.on('line', (line) => {
      try {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{
            protocolVersion:'2024-11-05',
            serverInfo:{ name:'test-server', version:'1.0' },
            capabilities:{ tools:{}, resources:{}, prompts:{} } } }) + '\\n');
        } else if (msg.method === 'tools/call') {
          process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{ content:[{type:'text',text:'ok'}] } }) + '\\n');
        } else if (msg.method === 'prompts/get') {
          process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{ description:'d', messages:[{role:'user',content:{type:'text',text:'hi'}}] } }) + '\\n');
        } else if (msg.method === 'ping') {
          process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{} }) + '\\n');
        }
        ${extra}
      } catch {}
    });
    setInterval(() => {}, 60000);
  `;
  return ['-e', script];
}

function startHttpServer(handler: http.RequestListener): Promise<{ url: string; server: http.Server }> {
  return new Promise((resolve) => {
    const server = http.createServer(handler);
    liveServers.push(server);
    server.listen(0, '127.0.0.1', () => {
      const port = (server.address() as AddressInfo).port;
      resolve({ url: `http://127.0.0.1:${port}/mcp`, server });
    });
  });
}

async function connectedStdio(extra = '', opts: Record<string, unknown> = {}): Promise<McpClient> {
  const client = track(new McpClient({ command: 'node', args: stdioArgs(extra), requestTimeout: 5000, connectTimeout: 5000, ...opts }));
  await client.connect();
  assert.equal(client.state, 'ready');
  return client;
}

// =========================================================================
// Pure helpers
// =========================================================================
describe('validateMcpBufferSize', () => {
  it('returns default for undefined and null', () => {
    assert.equal(validateMcpBufferSize(undefined), DEFAULT_MCP_MAX_RESPONSE_BYTES);
    assert.equal(validateMcpBufferSize(null), DEFAULT_MCP_MAX_RESPONSE_BYTES);
  });
  it('returns default for non-finite values', () => {
    assert.equal(validateMcpBufferSize('not-a-number'), DEFAULT_MCP_MAX_RESPONSE_BYTES);
    assert.equal(validateMcpBufferSize(Infinity), DEFAULT_MCP_MAX_RESPONSE_BYTES);
  });
  it('clamps below minimum up to 1 KB', () => {
    assert.equal(validateMcpBufferSize(10), 1024);
  });
  it('clamps above maximum down to 100 MB', () => {
    assert.equal(validateMcpBufferSize(999 * 1024 * 1024), 100 * 1024 * 1024);
  });
  it('accepts an in-range numeric string', () => {
    assert.equal(validateMcpBufferSize('2048'), 2048);
  });
});

describe('boundMcpResponse', () => {
  it('passes through content under the limit untruncated', () => {
    const r = boundMcpResponse('hello', 100);
    assert.equal(r.truncated, false);
    assert.equal(r.content, 'hello');
    assert.equal(r.originalBytes, 5);
  });
  it('truncates and reports original size when over the limit', () => {
    const r = boundMcpResponse('x'.repeat(50), 10);
    assert.equal(r.truncated, true);
    assert.equal(r.originalBytes, 50);
    assert.match(r.content, /Response truncated: 50 bytes/);
  });
  it('walks back to avoid splitting a multi-byte UTF-8 char', () => {
    // "€" is 3 bytes (E2 82 AC). Limit mid-char must not emit a broken byte.
    const s = '€€€€';
    const r = boundMcpResponse(s, 4); // cut inside the 2nd euro sign
    assert.equal(r.truncated, true);
    // The visible prefix must be a clean decode (only whole euro signs).
    const prefix = r.content.split('\n\n[Response truncated')[0];
    assert.ok(!prefix.includes('�'), 'no replacement char from a split sequence');
    assert.equal(prefix, '€');
  });
});

// =========================================================================
// Getters / lifecycle basics
// =========================================================================
describe('McpClient getters', () => {
  it('uptime is null before connect and a number after', async () => {
    const pre = track(new McpClient({ command: 'node', args: stdioArgs() }));
    assert.equal(pre.uptime, null);
    assert.equal(pre.serverInfo, null);

    const c = await connectedStdio();
    assert.equal(typeof c.uptime, 'number');
    assert.ok((c.uptime as number) >= 0);
    assert.equal(c.serverInfo?.name, 'test-server');
    assert.deepEqual(c.tools, []);
    assert.deepEqual(c.resources, []);
    assert.deepEqual(c.prompts, []);
  });

  it('hasCapability reflects server-advertised capabilities', async () => {
    const c = await connectedStdio();
    assert.equal(c.hasCapability('tools'), true);
    assert.equal(c.hasCapability('nonexistent'), false);
    // serverCapabilities getter returns a copy, not the internal object.
    const caps = c.serverCapabilities;
    caps.injected = true;
    assert.equal(c.hasCapability('injected'), false);
  });

  it('getRoots maps paths and falls back to the full path for root "/"', () => {
    const c = track(new McpClient({ command: 'node', args: [], rootPaths: ['/home/u/proj', '/'] }));
    assert.deepEqual(c.getRoots(), [
      { uri: 'file:///home/u/proj', name: 'proj' },
      { uri: 'file:///', name: '/' },
    ]);
  });
});

// =========================================================================
// assertReady guard
// =========================================================================
describe('assertReady guard', () => {
  it('rejects tool/resource/prompt ops before connect', async () => {
    const c = track(new McpClient({ command: 'node', args: [] }));
    await assert.rejects(() => c.listTools(), /not ready \(state: disconnected\)/);
    await assert.rejects(() => c.callTool('x'), /not ready/);
    await assert.rejects(() => c.listResources(), /not ready/);
    await assert.rejects(() => c.readResource('file:///a'), /not ready/);
    await assert.rejects(() => c.listPrompts(), /not ready/);
    await assert.rejects(() => c.getPrompt('p'), /not ready/);
    await assert.rejects(() => c.ping(), /not ready/);
  });
});

// =========================================================================
// stdio request/response paths
// =========================================================================
describe('stdio requests', () => {
  it('callTool, getPrompt and ping round-trip through the server', async () => {
    const c = await connectedStdio();
    assert.deepEqual(await c.callTool('t', { a: 1 }), { content: [{ type: 'text', text: 'ok' }] });
    const p = await c.getPrompt('p', { x: 'y' });
    assert.equal(p.messages[0].content.text, 'hi');
    await assert.doesNotReject(() => c.ping());
  });

  it('callTool rejects immediately when the signal is already aborted', async () => {
    const c = await connectedStdio();
    await assert.rejects(() => c.callTool('t', {}, { signal: AbortSignal.abort() }), /aborted/);
  });

  it('sendRequest honors an abort that fires while the request is pending', async () => {
    const c = await connectedStdio();
    const ctrl = new AbortController();
    const p = (c as unknown as { sendRequest: (m: string, pa: object, s: AbortSignal) => Promise<unknown> })
      .sendRequest('unanswered/method', {}, ctrl.signal);
    setTimeout(() => ctrl.abort(), 20);
    await assert.rejects(() => p, /MCP request aborted/);
  });

  it('sendRequest rejects before writing when signal is pre-aborted', async () => {
    const c = await connectedStdio();
    const p = (c as unknown as { sendRequest: (m: string, pa: object, s: AbortSignal) => Promise<unknown> })
      .sendRequest('x', {}, AbortSignal.abort());
    await assert.rejects(() => p, /MCP request aborted/);
  });

  it('sendRequest times out when the server never answers', async () => {
    const c = await connectedStdio('', { requestTimeout: 150 });
    await assert.rejects(
      () => (c as unknown as { sendRequest: (m: string, p: object) => Promise<unknown> }).sendRequest('never', {}),
      /timed out after 150ms/,
    );
  });
});

// =========================================================================
// writeMessage guard
// =========================================================================
describe('writeMessage', () => {
  it('throws when the child process stdin is not writable', () => {
    const c = track(new McpClient({ command: 'node', args: [] }));
    (c as unknown as { process: unknown }).process = { stdin: { writable: false } };
    assert.throws(
      () => (c as unknown as { writeMessage: (m: object) => void }).writeMessage({ jsonrpc: '2.0', method: 'x' }),
      /stdin not writable/,
    );
  });
  it('throws when there is no process at all', () => {
    const c = track(new McpClient({ command: 'node', args: [] }));
    assert.throws(
      () => (c as unknown as { writeMessage: (m: object) => void }).writeMessage({ jsonrpc: '2.0', method: 'x' }),
      /stdin not writable/,
    );
  });
});

// =========================================================================
// processBuffer + handleResponse / handleNotification, driven via stdout
// =========================================================================
function feed(c: McpClient, obj: unknown): void {
  const proc = (c as unknown as { process: { stdout: NodeJS.EventEmitter } }).process;
  proc.stdout.emit('data', Buffer.from(JSON.stringify(obj) + '\n'));
}
function feedRaw(c: McpClient, raw: string): void {
  const proc = (c as unknown as { process: { stdout: NodeJS.EventEmitter } }).process;
  proc.stdout.emit('data', Buffer.from(raw + '\n'));
}
function currentId(c: McpClient): number {
  return (c as unknown as { requestId: number }).requestId;
}
function rawSend(c: McpClient, method: string): Promise<unknown> {
  return (c as unknown as { sendRequest: (m: string, p: object) => Promise<unknown> }).sendRequest(method, {});
}

describe('processBuffer + message routing', () => {
  it('emits parse-error on malformed JSON lines', async () => {
    const c = await connectedStdio();
    const errs: string[] = [];
    c.on('parse-error', (l: string) => errs.push(l));
    feedRaw(c, '{not valid json');
    await new Promise((r) => setTimeout(r, 20));
    assert.deepEqual(errs, ['{not valid json']);
  });

  it('routes a message with both id and method to notification handling', async () => {
    const c = await connectedStdio();
    const notes: Array<[string, unknown]> = [];
    c.on('notification', (m: string, p: unknown) => notes.push([m, p]));
    feed(c, { jsonrpc: '2.0', id: 42, method: 'custom/evt', params: { k: 1 } });
    await new Promise((r) => setTimeout(r, 20));
    assert.ok(notes.some(([m]) => m === 'custom/evt'));
  });

  it('ignores a response whose id has no pending request', async () => {
    const c = await connectedStdio();
    feed(c, { jsonrpc: '2.0', id: 99999, result: { ok: true } });
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(c.state, 'ready'); // no crash, still usable
  });

  it('resolves a pending request from a matching response', async () => {
    const c = await connectedStdio();
    const p = rawSend(c, 'custom/req');
    feed(c, { jsonrpc: '2.0', id: currentId(c), result: { value: 7 } });
    assert.deepEqual(await p, { value: 7 });
  });

  it('resolves a pending request that was issued with an abort signal', async () => {
    // Exercises handleResponse when the pending entry carries an abortHandler.
    const c = await connectedStdio();
    const ctrl = new AbortController();
    const p = (c as unknown as { sendRequest: (m: string, pa: object, s: AbortSignal) => Promise<unknown> })
      .sendRequest('custom/req', {}, ctrl.signal);
    feed(c, { jsonrpc: '2.0', id: currentId(c), result: { done: true } });
    assert.deepEqual(await p, { done: true });
  });

  it('rejects a pending request with a generic JSON-RPC error', async () => {
    const c = await connectedStdio();
    const p = rawSend(c, 'custom/req');
    feed(c, { jsonrpc: '2.0', id: currentId(c), error: { code: -32000, message: 'boom' } });
    await assert.rejects(() => p, /MCP error -32000: boom/);
  });

  it('maps a -32001 response error to McpSessionExpiredError and emits session-expired', async () => {
    const c = await connectedStdio();
    let expired = false;
    c.on('session-expired', () => { expired = true; });
    const p = rawSend(c, 'custom/req');
    feed(c, { jsonrpc: '2.0', id: currentId(c), error: { code: -32001, message: 'gone' } });
    await assert.rejects(() => p, McpSessionExpiredError);
    assert.equal(expired, true);
  });
});

describe('handleNotification branches', () => {
  it('invokes onElicitation for elicitation/create and notifications/elicitation', async () => {
    const c = await connectedStdio();
    const got: unknown[] = [];
    c.onElicitation = async (request) => { got.push(request); return { action: 'decline' as const }; };
    feed(c, { jsonrpc: '2.0', method: 'elicitation/create', params: { message: 'm', requestedSchema: { type: 'object' } } });
    feed(c, { jsonrpc: '2.0', method: 'notifications/elicitation', params: { message: 'n', requestedSchema: {} } });
    await new Promise((r) => setTimeout(r, 20));
    assert.deepEqual(got, [
      { message: 'm', requestedSchema: { type: 'object' } },
      { message: 'n', requestedSchema: {} },
    ]);
  });

  it('resets caches and emits *-changed for list_changed notifications', async () => {
    const c = await connectedStdio();
    (c as unknown as { _tools: unknown[] })._tools = [{ name: 't' }];
    (c as unknown as { _resources: unknown[] })._resources = [{ uri: 'u' }];
    (c as unknown as { _prompts: unknown[] })._prompts = [{ name: 'p' }];
    const events: string[] = [];
    c.on('tools-changed', () => events.push('tools'));
    c.on('resources-changed', () => events.push('resources'));
    c.on('prompts-changed', () => events.push('prompts'));
    feed(c, { jsonrpc: '2.0', method: 'notifications/tools/list_changed' });
    feed(c, { jsonrpc: '2.0', method: 'notifications/resources/list_changed' });
    feed(c, { jsonrpc: '2.0', method: 'notifications/prompts/list_changed' });
    await new Promise((r) => setTimeout(r, 20));
    assert.deepEqual(events.sort(), ['prompts', 'resources', 'tools']);
    assert.deepEqual(c.tools, []);
    assert.deepEqual(c.resources, []);
    assert.deepEqual(c.prompts, []);
  });

  it('computes percent from progress/total and leaves it undefined otherwise', async () => {
    const c = await connectedStdio();
    const updates: Array<{ percent?: number; message?: string }> = [];
    c.onToolProgress('tok-1', (u) => updates.push(u));

    feed(c, { jsonrpc: '2.0', method: 'notifications/progress', params: { progressToken: 'tok-1', progress: 1, total: 4, message: 'quarter' } });
    feed(c, { jsonrpc: '2.0', method: 'notifications/progress', params: { progressToken: 'tok-1', progress: 2, message: 'no-total' } });
    await new Promise((r) => setTimeout(r, 20));

    assert.deepEqual(updates[0], { percent: 25, message: 'quarter' });
    assert.deepEqual(updates[1], { percent: undefined, message: 'no-total' });
  });

  it('ignores progress with no registered callback or no token', async () => {
    const c = await connectedStdio();
    // These must not throw and must be no-ops.
    feed(c, { jsonrpc: '2.0', method: 'notifications/progress', params: { progressToken: 'unknown', progress: 1, total: 2 } });
    feed(c, { jsonrpc: '2.0', method: 'notifications/progress', params: { progress: 1, total: 2 } });
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(c.state, 'ready');
  });

  it('answers a roots/list request by writing a roots response to stdin', async () => {
    const c = await connectedStdio('', { rootPaths: ['/tmp/work'] });
    const writeSpy = mock.method(
      (c as unknown as { writeMessage: (m: unknown) => void }),
      'writeMessage',
    );
    feed(c, { jsonrpc: '2.0', id: 3, method: 'roots/list' });
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(writeSpy.mock.calls.length, 1);
    const written = writeSpy.mock.calls[0].arguments[0] as { result: { roots: Array<{ uri: string }> } };
    assert.equal(written.result.roots[0].uri, 'file:///tmp/work');
  });
});

// =========================================================================
// classifyError / errorContext / isSessionExpiredBody
// =========================================================================
describe('classifyError', () => {
  const c = track(new McpClient({ command: 'node', args: [] }));
  it('maps a known terminal errno code with context', () => {
    const e = Object.assign(new Error('reset'), { code: 'ECONNRESET' });
    const r = c.classifyError(e);
    assert.equal(r.code, 'ECONNRESET');
    assert.match(r.message, /Connection reset by peer/);
  });
  it('classifies spawn failures', () => {
    assert.equal(c.classifyError(new Error('spawn node ENOENT')).code, 'SPAWN_ERROR');
  });
  it('classifies terminated/killed as ESRCH', () => {
    assert.equal(c.classifyError(new Error('process terminated')).code, 'ESRCH');
    assert.equal(c.classifyError(new Error('was killed')).code, 'ESRCH');
  });
  it('falls back to UNKNOWN for anything else', () => {
    const r = c.classifyError(new Error('weird failure'));
    assert.equal(r.code, 'UNKNOWN');
    assert.equal(r.message, 'weird failure');
  });
  it('errorContext covers every terminal code and the default arm', () => {
    const ec = (code: string) => (c as unknown as { errorContext: (x: string) => string }).errorContext(code);
    assert.match(ec('ETIMEDOUT'), /timed out/);
    assert.match(ec('EPIPE'), /Broken pipe/);
    assert.match(ec('EHOSTUNREACH'), /Host unreachable/);
    assert.match(ec('ECONNREFUSED'), /server is down/);
    assert.match(ec('ESRCH'), /Process not found/);
    assert.equal(ec('ENODEV'), 'System error: ENODEV');
  });
});

describe('isSessionExpiredBody', () => {
  const c = track(new McpClient({ command: 'node', args: [] }));
  const check = (b: string) => (c as unknown as { isSessionExpiredBody: (x: string) => boolean }).isSessionExpiredBody(b);
  it('detects -32001 in both spacing variants', () => {
    assert.equal(check('{"error":{"code":-32001}}'), true);
    assert.equal(check('{"error":{"code": -32001}}'), true);
  });
  it('returns false for unrelated bodies', () => {
    assert.equal(check('{"ok":true}'), false);
  });
});

// =========================================================================
// sendWithRetry — retry/backoff semantics (mock.method, no network)
// =========================================================================
describe('sendWithRetry', () => {
  function stubAuto(c: McpClient, impl: () => Promise<unknown>) {
    return mock.method(c as unknown as { sendRequestAuto: () => Promise<unknown> }, 'sendRequestAuto', impl);
  }
  const retry = (c: McpClient, attempts: number) =>
    (c as unknown as { sendWithRetry: (m: string, p: object, n: number) => Promise<unknown> })
      .sendWithRetry('tools/list', {}, attempts);

  it('returns the first successful result without retrying', async () => {
    const c = track(new McpClient({ command: 'node', args: [] }));
    const spy = stubAuto(c, async () => ({ tools: [] }));
    assert.deepEqual(await retry(c, 3), { tools: [] });
    assert.equal(spy.mock.calls.length, 1);
  });

  it('does not retry on a non-transient error', async () => {
    const c = track(new McpClient({ command: 'node', args: [] }));
    const spy = stubAuto(c, async () => { throw new Error('bad request'); });
    await assert.rejects(() => retry(c, 3), /bad request/);
    assert.equal(spy.mock.calls.length, 1);
  });

  it('does not retry on a session-expired error', async () => {
    const c = track(new McpClient({ command: 'node', args: [] }));
    const spy = stubAuto(c, async () => { throw new McpSessionExpiredError(); });
    await assert.rejects(() => retry(c, 3), McpSessionExpiredError);
    assert.equal(spy.mock.calls.length, 1);
  });

  it('retries a transient error and then succeeds', async () => {
    const c = track(new McpClient({ command: 'node', args: [] }));
    let n = 0;
    const spy = stubAuto(c, async () => {
      n++;
      if (n === 1) throw new Error('read ECONNRESET');
      return { tools: [{ name: 'ok' }] };
    });
    assert.deepEqual(await retry(c, 2), { tools: [{ name: 'ok' }] });
    assert.equal(spy.mock.calls.length, 2);
  });
});

// =========================================================================
// HTTP transport (connectHTTP + sendHttpRequest), real http server, port 0
// =========================================================================
function jsonRpcServer(resultFor: (method: string) => unknown): http.RequestListener {
  return (req, res) => {
    let body = '';
    req.on('data', (d) => body += d);
    req.on('end', () => {
      const msg = JSON.parse(body);
      if (String(msg.method).startsWith('notifications/')) { res.statusCode = 202; res.end(); return; }
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: resultFor(msg.method) }));
    });
  };
}

describe('connectHTTP / sendHttpRequest', () => {
  it('connects, reads server info, and lists tools over HTTP', async () => {
    const { url } = await startHttpServer(jsonRpcServer((m) => {
      if (m === 'initialize') return { serverInfo: { name: 'http-srv', version: '2.0' }, capabilities: { tools: {} } };
      if (m === 'tools/list') return { tools: [{ name: 'ht', description: '', inputSchema: {} }] };
      return {};
    }));
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 }));
    await c.connectHTTP(url);
    assert.equal(c.state, 'ready');
    assert.equal(c.serverInfo?.name, 'http-srv');
    const tools = await c.listTools();
    assert.equal(tools[0].name, 'ht');
  });

  it('connectHTTP is a no-op when already ready', async () => {
    const c = await connectedStdio();
    await c.connectHTTP('http://127.0.0.1:1/mcp'); // must not throw / not change state
    assert.equal(c.state, 'ready');
  });

  it('applies defaults when the HTTP server omits serverInfo/capabilities', async () => {
    const { url } = await startHttpServer(jsonRpcServer(() => ({})));
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000 }));
    await c.connectHTTP(url);
    assert.deepEqual(c.serverInfo, { name: 'unknown', version: '0.0.0' });
  });

  it('parses an SSE-framed (event: message / data:) response body', async () => {
    const { url } = await startHttpServer((req, res) => {
      let body = ''; req.on('data', (d) => body += d);
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'initialize') {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { serverInfo: { name: 's', version: '1' } } }));
          return;
        }
        if (String(msg.method).startsWith('notifications/')) { res.statusCode = 202; res.end(); return; }
        res.setHeader('Content-Type', 'text/event-stream');
        res.end(`event: message\ndata: ${JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { tools: [{ name: 'framed', description: '', inputSchema: {} }] } })}\n\n`);
      });
    });
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 }));
    await c.connectHTTP(url);
    const tools = await c.listTools();
    assert.equal(tools[0].name, 'framed');
  });

  it('rejects a JSON-RPC error response over HTTP', async () => {
    const { url } = await startHttpServer((req, res) => {
      let body = ''; req.on('data', (d) => body += d);
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'initialize') { res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} })); return; }
        if (String(msg.method).startsWith('notifications/')) { res.statusCode = 202; res.end(); return; }
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, error: { code: -32000, message: 'nope' } }));
      });
    });
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 }));
    await c.connectHTTP(url);
    await assert.rejects(() => c.callTool('t'), /MCP error -32000: nope/);
  });

  it('maps a -32001 HTTP error to session-expired', async () => {
    let expired = false;
    const { url } = await startHttpServer((req, res) => {
      let body = ''; req.on('data', (d) => body += d);
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'initialize') { res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} })); return; }
        if (String(msg.method).startsWith('notifications/')) { res.statusCode = 202; res.end(); return; }
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, error: { code: -32001, message: 'expired' } }));
      });
    });
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 }));
    c.on('session-expired', () => { expired = true; });
    await c.connectHTTP(url);
    await assert.rejects(() => c.readResource('file:///a'), McpSessionExpiredError);
    assert.equal(expired, true);
  });

  it('maps a 404 session-expired body to McpSessionExpiredError', async () => {
    const { url } = await startHttpServer((req, res) => {
      let body = ''; req.on('data', (d) => body += d);
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'initialize') { res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} })); return; }
        if (String(msg.method).startsWith('notifications/')) { res.statusCode = 202; res.end(); return; }
        res.statusCode = 404;
        res.end(JSON.stringify({ error: { code: -32001 } }));
      });
    });
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 }));
    await c.connectHTTP(url);
    await assert.rejects(() => c.ping(), McpSessionExpiredError);
  });

  it('rejects an invalid (non-JSON) HTTP response body', async () => {
    const { url } = await startHttpServer((req, res) => {
      let body = ''; req.on('data', (d) => body += d);
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'initialize') { res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} })); return; }
        if (String(msg.method).startsWith('notifications/')) { res.statusCode = 202; res.end(); return; }
        res.end('<<not json at all>>');
      });
    });
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 }));
    await c.connectHTTP(url);
    await assert.rejects(() => c.ping(), /Invalid JSON response from MCP SSE server/);
  });

  it('rejects immediately when the abort signal is already set', async () => {
    const { url } = await startHttpServer(jsonRpcServer((m) => (m === 'initialize' ? { serverInfo: { name: 's', version: '1' } } : {})));
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 }));
    await c.connectHTTP(url);
    await assert.rejects(() => c.callTool('t', {}, { signal: AbortSignal.abort() }), /aborted/);
  });

  it('sendHttpRequest rejects synchronously for a pre-aborted signal', async () => {
    // callTool pre-checks the signal, so drive sendHttpRequest directly to cover
    // its own aborted-before-dispatch guard.
    const { url } = await startHttpServer(jsonRpcServer((m) => (m === 'initialize' ? { serverInfo: { name: 's', version: '1' } } : {})));
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 }));
    await c.connectHTTP(url);
    await assert.rejects(
      () => (c as unknown as { sendHttpRequest: (m: string, p: object, s: AbortSignal) => Promise<unknown> })
        .sendHttpRequest('tools/call', {}, AbortSignal.abort()),
      /MCP request aborted/,
    );
  });

  it('aborts an in-flight HTTP request', async () => {
    const { url } = await startHttpServer((req, res) => {
      let body = ''; req.on('data', (d) => body += d);
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'initialize') { res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} })); return; }
        if (String(msg.method).startsWith('notifications/')) { res.statusCode = 202; res.end(); return; }
        // Never respond to the tool call → the client must abort it.
      });
    });
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 }));
    await c.connectHTTP(url);
    const ctrl = new AbortController();
    const p = c.callTool('t', {}, { signal: ctrl.signal });
    setTimeout(() => ctrl.abort(), 30);
    await assert.rejects(() => p, /MCP request aborted/);
  });

  it('times out an HTTP request when the server stalls', async () => {
    const { url } = await startHttpServer((req, res) => {
      let body = ''; req.on('data', (d) => body += d);
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'initialize') { res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} })); return; }
        if (String(msg.method).startsWith('notifications/')) { res.statusCode = 202; res.end(); return; }
        // Stall the tool call.
      });
    });
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 150 }));
    await c.connectHTTP(url);
    await assert.rejects(() => c.ping(), /timed out/);
  });

  it('classifies and emits a terminal-error when the HTTP endpoint refuses', async () => {
    // Point a ready HTTP-transport client at a closed port → ECONNREFUSED.
    const c = track(new McpClient({ command: 'x', args: [], requestTimeout: 1000 }));
    (c as unknown as { _state: string })._state = 'ready';
    (c as unknown as { transport: string }).transport = 'http';
    (c as unknown as { sseUrl: string }).sseUrl = 'http://127.0.0.1:1/mcp';
    const terminal: Array<{ code: string }> = [];
    c.on('terminal-error', (e: { code: string }) => terminal.push(e));
    await assert.rejects(() => c.ping());
    assert.ok(terminal.some((t) => t.code === 'ECONNREFUSED'), `expected ECONNREFUSED, got ${JSON.stringify(terminal)}`);
  });

  it('selects the https module for https URLs (surfaces a connection error)', async () => {
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 800, requestTimeout: 800 }));
    // Nothing is listening on this port with TLS → connect must reject, proving
    // the https branch of require() was taken without crashing on require.
    await assert.rejects(() => c.connectHTTP('https://127.0.0.1:1/mcp'));
  });
});

describe('list fallbacks + readResource over HTTP', () => {
  it('falls back to empty arrays when the server omits list keys', async () => {
    const { url } = await startHttpServer(jsonRpcServer((m) => {
      if (m === 'initialize') return { serverInfo: { name: 's', version: '1' }, capabilities: {} };
      return {}; // tools/list, resources/list, prompts/list → no arrays
    }));
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 }));
    await c.connectHTTP(url);
    assert.deepEqual(await c.listTools(), []);
    assert.deepEqual(await c.listResources(), []);
    assert.deepEqual(await c.listPrompts(), []);
  });

  it('populates and returns resources and reads a resource', async () => {
    const { url } = await startHttpServer(jsonRpcServer((m) => {
      if (m === 'initialize') return { serverInfo: { name: 's', version: '1' }, capabilities: {} };
      if (m === 'resources/list') return { resources: [{ uri: 'file:///a.txt', name: 'a.txt', mimeType: 'text/plain' }] };
      if (m === 'resources/read') return { contents: [{ uri: 'file:///a.txt', text: 'hi' }] };
      if (m === 'prompts/list') return { prompts: [{ name: 'p' }] };
      return {};
    }));
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 }));
    await c.connectHTTP(url);
    const res = await c.listResources();
    assert.equal(res[0].uri, 'file:///a.txt');
    assert.deepEqual(c.resources, res);
    const read = await c.readResource('file:///a.txt') as { contents: Array<{ text: string }> };
    assert.equal(read.contents[0].text, 'hi');
    const prompts = await c.listPrompts();
    assert.equal(prompts[0].name, 'p');
    assert.deepEqual(c.prompts, prompts);
  });
});

// =========================================================================
// connectSSE error handler + session-expired paths
// =========================================================================
describe('connectSSE', () => {
  it('is a no-op when already ready', async () => {
    const c = await connectedStdio();
    await c.connectSSE('http://127.0.0.1:1/mcp');
    assert.equal(c.state, 'ready');
  });

  it('classifies and emits terminal-error on a refused connection', async () => {
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 1000, requestTimeout: 1000 }));
    const terminal: Array<{ code: string }> = [];
    c.on('terminal-error', (e: { code: string }) => terminal.push(e));
    await assert.rejects(() => c.connectSSE('http://127.0.0.1:1/mcp'));
    assert.equal(c.state, 'error');
    assert.ok(terminal.length >= 1);
  });

  it('connects successfully and exposes server info', async () => {
    const { url } = await startHttpServer((req, res) => {
      let body = ''; req.on('data', (d) => body += d);
      req.on('end', () => {
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ jsonrpc: '2.0', id: 1, result: { serverInfo: { name: 'sse-srv', version: '3.0' }, capabilities: { prompts: {} } } }));
      });
    });
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000 }));
    await c.connectSSE(url);
    assert.equal(c.state, 'ready');
    assert.equal(c.serverInfo?.name, 'sse-srv');
  });

  it('rejects a JSON-RPC error during SSE init', async () => {
    const { url } = await startHttpServer((req, res) => {
      let body = ''; req.on('data', (d) => body += d);
      req.on('end', () => res.end(JSON.stringify({ jsonrpc: '2.0', id: 1, error: { code: -32000, message: 'init-fail' } })));
    });
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000 }));
    await assert.rejects(() => c.connectSSE(url), /MCP SSE init error -32000: init-fail/);
  });

  it('maps a -32001 SSE init error to session-expired', async () => {
    const { url } = await startHttpServer((req, res) => {
      let body = ''; req.on('data', (d) => body += d);
      req.on('end', () => res.end(JSON.stringify({ jsonrpc: '2.0', id: 1, error: { code: -32001, message: 'x' } })));
    });
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000 }));
    await assert.rejects(() => c.connectSSE(url), McpSessionExpiredError);
  });

  it('maps a 404 session-expired body during SSE init', async () => {
    const { url } = await startHttpServer((req, res) => {
      let body = ''; req.on('data', (d) => body += d);
      req.on('end', () => { res.statusCode = 404; res.end('{"error":{"code":-32001}}'); });
    });
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000 }));
    await assert.rejects(() => c.connectSSE(url), McpSessionExpiredError);
  });

  it('rejects an invalid SSE init body', async () => {
    const { url } = await startHttpServer((req, res) => {
      let body = ''; req.on('data', (d) => body += d);
      req.on('end', () => res.end('garbage-not-json'));
    });
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000 }));
    await assert.rejects(() => c.connectSSE(url), /Invalid SSE init response/);
  });

  it('times out an SSE connection that never responds', async () => {
    const { url } = await startHttpServer((req) => { req.on('data', () => {}); /* never end response */ });
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 150, requestTimeout: 150 }));
    await assert.rejects(() => c.connectSSE(url), /SSE connection timed out/);
  });
});

// =========================================================================
// disconnect / gracefulShutdown
// =========================================================================
describe('disconnect / gracefulShutdown', () => {
  it('emits close with uptime when disconnecting a connected client', async () => {
    const c = await connectedStdio();
    const closes: Array<[unknown, unknown]> = [];
    c.on('close', (code: unknown, up: unknown) => closes.push([code, up]));
    c.disconnect();
    assert.equal(c.state, 'disconnected');
    assert.equal(closes.length, 1);
    assert.equal(closes[0][0], null);
  });

  it('is idempotent: a second disconnect after close is a no-op', async () => {
    const c = await connectedStdio();
    c.disconnect();
    const closes: unknown[] = [];
    c.on('close', () => closes.push(1));
    c.disconnect(); // hasTriggeredClose && !process → early return
    assert.equal(closes.length, 0);
  });

  it('SIGKILLs immediately when the child has no pid (injected)', () => {
    const c = track(new McpClient({ command: 'x', args: [] }));
    const signals: string[] = [];
    (c as unknown as { process: unknown }).process = { kill: (s: string) => { signals.push(s); return true; } };
    (c as unknown as { processPid: number | null }).processPid = null;
    (c as unknown as { _state: string })._state = 'ready';
    c.disconnect();
    assert.deepEqual(signals, ['SIGKILL']);
  });

  it('escalates SIGINT→SIGTERM for a child that ignores SIGINT', async () => {
    const c = track(new McpClient({
      command: 'node',
      // Ignore SIGINT so the shutdown ladder has to escalate.
      args: ['-e', 'process.on("SIGINT",()=>{}); setInterval(()=>{},1000);'],
      connectTimeout: 300,
    }));
    // We don't need a full MCP handshake; just a live child to shut down.
    await assert.rejects(() => c.connect(), /timed out/); // no initialize reply
    const pid = (c as unknown as { processPid: number | null }).processPid
      ?? (c as unknown as { process: { pid: number } | null }).process?.pid ?? null;
    // connect()'s catch already called disconnect(); start a fresh child to drive gracefulShutdown directly.
    assert.ok(pid === null || typeof pid === 'number');
    // Drive gracefulShutdown on a known-live child and let the SIGTERM timer fire.
    const { spawn } = await import('node:child_process');
    const child = spawn('node', ['-e', 'process.on("SIGINT",()=>{}); setInterval(()=>{},1000);']);
    await new Promise((r) => setTimeout(r, 50));
    (c as unknown as { gracefulShutdown: (p: unknown, pid: number) => void }).gracefulShutdown(child, child.pid!);
    await new Promise((r) => setTimeout(r, 300));
    // After escalation the child must be gone.
    let alive = true;
    try { process.kill(child.pid!, 0); } catch { alive = false; }
    try { child.kill('SIGKILL'); } catch { /* already dead */ }
    assert.equal(alive, false);
  });
});

// =========================================================================
// reconnect
// =========================================================================
describe('reconnect', () => {
  it('reconnects a stdio client after disconnect', async () => {
    const c = await connectedStdio();
    c.disconnect();
    await c.reconnect();
    assert.equal(c.state, 'ready');
  });

  it('gives up after maxReconnectAttempts when the command keeps failing', async () => {
    const c = track(new McpClient({ command: 'node', args: ['-e', 'process.exit(1)'], connectTimeout: 150, requestTimeout: 150 }));
    (c as unknown as { maxReconnectAttempts: number }).maxReconnectAttempts = 1;
    await assert.rejects(() => c.reconnect(), /reconnection failed after 1 attempts/);
  });

  it('uses the SSE path when transport is sse', async () => {
    const { url } = await startHttpServer((req, res) => {
      let body = ''; req.on('data', (d) => body += d);
      req.on('end', () => res.end(JSON.stringify({ jsonrpc: '2.0', id: 1, result: { serverInfo: { name: 'r', version: '1' } } })));
    });
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000 }));
    await c.connectSSE(url);
    c.disconnect();
    await c.reconnect(); // transport === 'sse' && sseUrl → connectSSE
    assert.equal(c.state, 'ready');
  });
});

// =========================================================================
// 2025-06-18 capability surface
// =========================================================================

// A stdio server that answers the paginated list / template / subscribe /
// logging / completion methods, layered on top of the base handshake server.
const FEATURE_EXTRA = `
  if (msg.method === 'tools/list') {
    if (msg.params && msg.params.cursor === 'p2') {
      process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{ tools:[{name:'t2',description:'',inputSchema:{}}] } }) + '\\n');
    } else {
      process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{ tools:[{name:'t1',description:'',inputSchema:{}}], nextCursor:'p2' } }) + '\\n');
    }
  }
  if (msg.method === 'resources/templates/list') {
    process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{ resourceTemplates:[{uriTemplate:'x://{id}',name:'tmpl'}] } }) + '\\n');
  }
  if (msg.method === 'resources/subscribe' || msg.method === 'resources/unsubscribe' || msg.method === 'logging/setLevel') {
    process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{} }) + '\\n');
  }
  if (msg.method === 'completion/complete') {
    process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{ completion:{ values:['alpha','beta'], total:2, hasMore:false } } }) + '\\n');
  }
`;

function writeSpy(c: McpClient) {
  return mock.method((c as unknown as { writeMessage: (m: unknown) => void }), 'writeMessage');
}
function lastResponses(spy: ReturnType<typeof writeSpy>): Array<Record<string, any>> {
  return spy.mock.calls.map((c) => c.arguments[0] as Record<string, any>);
}

describe('buildClientCapabilities', () => {
  it('always advertises elicitation and sampling; roots only with rootPaths', () => {
    const bare = track(new McpClient({ command: 'node', args: [] }));
    assert.deepEqual(
      (bare as unknown as { buildClientCapabilities: () => unknown }).buildClientCapabilities(),
      { elicitation: {}, sampling: {} },
    );
    const withRoots = track(new McpClient({ command: 'node', args: [], rootPaths: ['/w'] }));
    assert.deepEqual(
      (withRoots as unknown as { buildClientCapabilities: () => unknown }).buildClientCapabilities(),
      { elicitation: {}, sampling: {}, roots: { listChanged: true } },
    );
  });
});

describe('elicitation request/response', () => {
  it('accepts: replies with the handler action + content', async () => {
    const c = await connectedStdio();
    c.onElicitation = async () => ({ action: 'accept', content: { name: 'Ada' } });
    const spy = writeSpy(c);
    feed(c, { jsonrpc: '2.0', id: 11, method: 'elicitation/create', params: { message: 'Your name?', requestedSchema: { type: 'object' } } });
    await new Promise((r) => setTimeout(r, 20));
    const resp = lastResponses(spy).find((m) => m.id === 11);
    assert.deepEqual(resp?.result, { action: 'accept', content: { name: 'Ada' } });
  });

  it('declines when no handler is wired (never hangs)', async () => {
    const c = await connectedStdio();
    const spy = writeSpy(c);
    feed(c, { jsonrpc: '2.0', id: 12, method: 'elicitation/create', params: { message: 'm', requestedSchema: {} } });
    await new Promise((r) => setTimeout(r, 20));
    assert.deepEqual(lastResponses(spy).find((m) => m.id === 12)?.result, { action: 'decline' });
  });

  it('cancels when the handler throws', async () => {
    const c = await connectedStdio();
    c.onElicitation = async () => { throw new Error('ui closed'); };
    const spy = writeSpy(c);
    feed(c, { jsonrpc: '2.0', id: 13, method: 'elicitation/create', params: {} });
    await new Promise((r) => setTimeout(r, 20));
    assert.deepEqual(lastResponses(spy).find((m) => m.id === 13)?.result, { action: 'cancel' });
  });

  it('normalizes a malformed request and does not reply to the no-id notification form', async () => {
    const c = await connectedStdio();
    const seen: unknown[] = [];
    c.onElicitation = async (req) => { seen.push(req); return { action: 'decline' as const }; };
    const spy = writeSpy(c);
    feed(c, { jsonrpc: '2.0', method: 'notifications/elicitation', params: { message: 5, requestedSchema: 'nope' } });
    await new Promise((r) => setTimeout(r, 20));
    assert.deepEqual(seen, [{ message: '', requestedSchema: {} }]);
    assert.equal(lastResponses(spy).length, 0); // no id → no response written
  });
});

describe('sampling request/response', () => {
  it('fulfills with the handler result', async () => {
    const c = await connectedStdio();
    c.onSampling = async () => ({ role: 'assistant', content: { type: 'text', text: 'hi' }, model: 'm', stopReason: 'endTurn' });
    const spy = writeSpy(c);
    feed(c, { jsonrpc: '2.0', id: 21, method: 'sampling/createMessage', params: { messages: [] } });
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(lastResponses(spy).find((m) => m.id === 21)?.result.content.text, 'hi');
  });

  it('errors -32601 when no handler is wired', async () => {
    const c = await connectedStdio();
    const spy = writeSpy(c);
    feed(c, { jsonrpc: '2.0', id: 22, method: 'sampling/createMessage', params: { messages: [] } });
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(lastResponses(spy).find((m) => m.id === 22)?.error.code, -32601);
  });

  it('errors -32603 when the handler throws', async () => {
    const c = await connectedStdio();
    c.onSampling = async () => { throw new Error('model down'); };
    const spy = writeSpy(c);
    feed(c, { jsonrpc: '2.0', id: 23, method: 'sampling/createMessage', params: { messages: [] } });
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(lastResponses(spy).find((m) => m.id === 23)?.error.message, 'model down');
  });

  it('ignores a sampling request with no id', async () => {
    const c = await connectedStdio();
    c.onSampling = async () => ({ role: 'assistant', content: { type: 'text', text: 'x' }, model: 'm' });
    const spy = writeSpy(c);
    feed(c, { jsonrpc: '2.0', method: 'sampling/createMessage', params: { messages: [] } });
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(lastResponses(spy).length, 0);
  });
});

describe('ping responder + roots no-id guard', () => {
  it('answers a server ping with an empty result', async () => {
    const c = await connectedStdio();
    const spy = writeSpy(c);
    feed(c, { jsonrpc: '2.0', id: 31, method: 'ping' });
    await new Promise((r) => setTimeout(r, 20));
    assert.deepEqual(lastResponses(spy).find((m) => m.id === 31)?.result, {});
  });

  it('does not answer a roots/list that arrives without an id', async () => {
    const c = await connectedStdio('', { rootPaths: ['/w'] });
    const spy = writeSpy(c);
    feed(c, { jsonrpc: '2.0', method: 'roots/list' });
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(lastResponses(spy).length, 0);
  });

  it('respondToServerRequest is a safe no-op with no writable stdin', () => {
    const c = track(new McpClient({ command: 'node', args: [] })); // never connected
    assert.doesNotThrow(() =>
      (c as unknown as { respondToServerRequest: (id: number, p: object) => void })
        .respondToServerRequest(1, { result: {} }),
    );
  });
});

describe('logging + resource-updated notifications', () => {
  it('emits a normalized log event (default level info)', async () => {
    const c = await connectedStdio();
    const logs: any[] = [];
    c.on('log', (m) => logs.push(m));
    feed(c, { jsonrpc: '2.0', method: 'notifications/message', params: { level: 'error', logger: 'db', data: 'boom' } });
    feed(c, { jsonrpc: '2.0', method: 'notifications/message', params: { data: 'no-level' } });
    await new Promise((r) => setTimeout(r, 20));
    assert.deepEqual(logs[0], { level: 'error', logger: 'db', data: 'boom' });
    assert.equal(logs[1].level, 'info');
  });

  it('emits resource-updated only when a uri is present', async () => {
    const c = await connectedStdio();
    const uris: string[] = [];
    c.on('resource-updated', (u: string) => uris.push(u));
    feed(c, { jsonrpc: '2.0', method: 'notifications/resources/updated', params: { uri: 'file:///a' } });
    feed(c, { jsonrpc: '2.0', method: 'notifications/resources/updated', params: {} });
    await new Promise((r) => setTimeout(r, 20));
    assert.deepEqual(uris, ['file:///a']);
  });
});

describe('cursor pagination', () => {
  it('follows nextCursor across pages', async () => {
    const c = await connectedStdio(FEATURE_EXTRA);
    const tools = await c.listTools();
    assert.deepEqual(tools.map((t) => t.name), ['t1', 't2']);
  });

  it('stops at MCP_MAX_PAGES and emits pagination-capped', async () => {
    const c = await connectedStdio();
    mock.method(
      (c as unknown as { sendWithRetry: (m: string, p: object) => Promise<unknown> }),
      'sendWithRetry',
      async () => ({ tools: [{ name: 'x', description: '', inputSchema: {} }], nextCursor: 'always' }),
    );
    let capped: [string, number] | null = null;
    c.on('pagination-capped', (method: string, count: number) => { capped = [method, count]; });
    const tools = await c.listTools();
    assert.equal(tools.length, MCP_MAX_PAGES);
    assert.deepEqual(capped, ['tools/list', MCP_MAX_PAGES]);
  });
});

describe('resource templates / subscribe / logging level / completion', () => {
  it('lists resource templates', async () => {
    const c = await connectedStdio(FEATURE_EXTRA);
    const tmpls = await c.listResourceTemplates();
    assert.equal(tmpls[0].uriTemplate, 'x://{id}');
  });

  it('subscribes and unsubscribes, tracking active URIs', async () => {
    const c = await connectedStdio(FEATURE_EXTRA);
    await c.subscribeResource('file:///a');
    const set = (c as unknown as { subscribedResources: Set<string> }).subscribedResources;
    assert.ok(set.has('file:///a'));
    await c.unsubscribeResource('file:///a');
    assert.ok(!set.has('file:///a'));
  });

  it('sets the log level', async () => {
    const c = await connectedStdio(FEATURE_EXTRA);
    await assert.doesNotReject(() => c.setLogLevel('warning'));
  });

  it('returns completion values, and falls back to empty on a bare result', async () => {
    const c = await connectedStdio(FEATURE_EXTRA);
    const res = await c.complete({ type: 'ref/prompt', name: 'p' }, { name: 'arg', value: 'al' }, { arguments: { other: 'x' } });
    assert.deepEqual(res.values, ['alpha', 'beta']);

    mock.method(
      (c as unknown as { sendRequestAuto: (m: string, p: object) => Promise<unknown> }),
      'sendRequestAuto',
      async () => ({}),
    );
    assert.deepEqual((await c.complete({ type: 'ref/resource', uri: 'x://1' }, { name: 'a', value: '' })).values, []);
  });
});

describe('cancellation notification', () => {
  it('writes notifications/cancelled when an in-flight request is aborted', async () => {
    const c = await connectedStdio();
    const spy = writeSpy(c);
    const ctrl = new AbortController();
    const p = (c as unknown as { sendRequest: (m: string, pa: object, s: AbortSignal) => Promise<unknown> })
      .sendRequest('slow/op', {}, ctrl.signal);
    ctrl.abort();
    await assert.rejects(() => p, /aborted/);
    const cancelled = lastResponses(spy).find((m) => m.method === 'notifications/cancelled');
    assert.ok(cancelled, 'a cancelled notification was sent');
    assert.equal(typeof cancelled!.params.requestId, 'number');
  });
});

describe('MCP-Protocol-Version header (HTTP)', () => {
  it('omits the header on initialize and includes it on later requests', async () => {
    const seen: Array<string | undefined> = [];
    const { url } = await startHttpServer((req, res) => {
      let body = ''; req.on('data', (d) => body += d);
      req.on('end', () => {
        const msg = JSON.parse(body || '{}');
        if (msg.method === 'initialize') {
          seen.push(req.headers['mcp-protocol-version'] as string | undefined);
          res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2025-06-18', serverInfo: { name: 's', version: '1' }, capabilities: {} } }));
        } else if (msg.method && msg.method.startsWith('notifications/')) {
          res.statusCode = 202; res.end();
        } else {
          seen.push(req.headers['mcp-protocol-version'] as string | undefined);
          res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} }));
        }
      });
    });
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 }));
    await c.connectHTTP(url);
    await c.ping();
    assert.equal(seen[0], undefined);          // initialize: no version header yet
    assert.equal(seen[1], '2025-06-18');       // ping: negotiated version header present
  });
});

describe('OAuth 2.1 HTTP integration', () => {
  function initServer(onRequest: (msg: any, req: http.IncomingMessage, res: http.ServerResponse) => void) {
    return startHttpServer((req, res) => {
      let body = ''; req.on('data', (d) => body += d);
      req.on('end', () => {
        const msg = JSON.parse(body || '{}');
        if (msg.method === 'initialize') {
          res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2025-06-18', serverInfo: { name: 's', version: '1' }, capabilities: {} } }));
          return;
        }
        if (msg.method && msg.method.startsWith('notifications/')) { res.statusCode = 202; res.end(); return; }
        onRequest(msg, req, res);
      });
    });
  }

  it('attaches the bearer token from the OAuth provider', async () => {
    let seenAuth: string | undefined;
    const { url } = await initServer((msg, req, res) => {
      seenAuth = req.headers.authorization as string | undefined;
      res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} }));
    });
    const oauth = { authorizationHeader: async () => 'Bearer tok-123', onUnauthorized: async () => false };
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000, oauth }));
    await c.connectHTTP(url);
    await c.ping();
    assert.equal(seenAuth, 'Bearer tok-123');
  });

  it('re-authorizes on 401 and retries once on success', async () => {
    let calls = 0; let sawWww: string | null = null;
    const { url } = await initServer((msg, _req, res) => {
      calls++;
      if (calls === 1) {
        res.statusCode = 401;
        res.setHeader('WWW-Authenticate', 'Bearer resource_metadata="https://rs/.well-known/oauth-protected-resource"');
        res.end(JSON.stringify({ error: 'unauthorized' }));
      } else {
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { ok: true } }));
      }
    });
    const oauth = { authorizationHeader: async () => null, onUnauthorized: async (www: string | null) => { sawWww = www; return true; } };
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000, oauth }));
    await c.connectHTTP(url);
    const result = await (c as unknown as { sendHttpRequest: (m: string, p: object) => Promise<unknown> }).sendHttpRequest('tools/list', {});
    assert.deepEqual(result, { ok: true });
    assert.match(sawWww!, /resource_metadata/);
    assert.equal(calls, 2);
  });

  it('rejects when re-authorization fails on 401', async () => {
    const { url } = await initServer((_msg, _req, res) => { res.statusCode = 401; res.end(JSON.stringify({ error: 'nope' })); });
    const oauth = { authorizationHeader: async () => null, onUnauthorized: async () => false };
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000, oauth }));
    await c.connectHTTP(url);
    await assert.rejects(
      () => (c as unknown as { sendHttpRequest: (m: string, p: object) => Promise<unknown> }).sendHttpRequest('tools/list', {}),
      /401 Unauthorized/,
    );
  });

  it('does not re-authorize twice (the retried request is not re-triggered)', async () => {
    let calls = 0;
    const { url } = await initServer((_msg, _req, res) => { calls++; res.statusCode = 401; res.end(JSON.stringify({ error: 'again' })); });
    const oauth = { authorizationHeader: async () => null, onUnauthorized: async () => true };
    const c = track(new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000, oauth }));
    await c.connectHTTP(url);
    await assert.rejects(() => (c as unknown as { sendHttpRequest: (m: string, p: object) => Promise<unknown> }).sendHttpRequest('tools/list', {}));
    assert.equal(calls, 2); // original + one retry, then no further re-auth
  });
});
