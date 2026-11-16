import { describe, it, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import {
  McpClient,
  McpRpcError,
  McpAuthError,
  MCP_URL_ELICITATION_REQUIRED_CODE,
  MAX_MCP_URL_ELICITATION_RETRIES,
  extractUrlElicitations,
  type McpElicitationRequest,
  type McpElicitationResult,
} from '../src/mcp/client';
import { McpManager } from '../src/mcp/manager';

after(() => { setTimeout(() => process.exit(0), 200); });

// A minimal Streamable-HTTP MCP server: answers `initialize`, ACKs notifications,
// returns an empty tool list, degrades the server→client GET stream to POST-only
// (405), and delegates every other POST to `onRequest`.
type ReqHandler = (msg: { id: number; method: string; params?: unknown }, res: http.ServerResponse) => void;
function startMcpHttpServer(onRequest: ReqHandler): Promise<{ url: string; close: () => void }> {
  return new Promise(resolve => {
    const server = http.createServer((req, res) => {
      if (req.method === 'GET') { res.statusCode = 405; res.end(); return; }
      let body = '';
      req.on('data', d => (body += d));
      req.on('end', () => {
        const msg = JSON.parse(body || '{}');
        if (msg.method === 'initialize') {
          res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2025-06-18', serverInfo: { name: 's', version: '1' }, capabilities: {} } }));
          return;
        }
        if (typeof msg.method === 'string' && msg.method.startsWith('notifications/')) { res.statusCode = 202; res.end(); return; }
        if (msg.method === 'tools/list') { res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { tools: [] } })); return; }
        onRequest(msg, res);
      });
    });
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address() as { port: number };
      resolve({ url: `http://127.0.0.1:${addr.port}/mcp`, close: () => server.close() });
    });
  });
}

const rpcError = (res: http.ServerResponse, id: number, code: number, message: string, data?: unknown) =>
  res.end(JSON.stringify({ jsonrpc: '2.0', id, error: { code, message, data } }));

// ---------------------------------------------------------------------------
// GAP U041-w67 (McpRpcError): a JSON-RPC error preserves code + data so callers
// can branch on well-known codes (parity with Claude Code's code-based checks).
// ---------------------------------------------------------------------------
describe('McpRpcError preserves code + data', () => {
  const servers: Array<() => void> = [];
  afterEach(() => { servers.splice(0).forEach(c => c()); });

  it('constructs with the "MCP error <code>: <message>" message and exposes code/data', () => {
    const e = new McpRpcError(-32042, 'elicitation required', { elicitations: [] });
    assert.equal(e.name, 'McpRpcError');
    assert.equal(e.code, -32042);
    assert.deepEqual(e.data, { elicitations: [] });
    assert.equal(e.message, 'MCP error -32042: elicitation required');
    assert.ok(e instanceof Error);
  });

  it('a tool-call error from the HTTP transport is an McpRpcError carrying code + data', async () => {
    const srv = await startMcpHttpServer((msg, res) => {
      rpcError(res, msg.id, -32010, 'boom', { hint: 'nope' });
    });
    servers.push(srv.close);
    const c = new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 });
    await c.connectHTTP(srv.url);
    const err = await c.callTool('t', {}).then(() => null, e => e);
    assert.ok(err instanceof McpRpcError);
    assert.equal(err.code, -32010);
    assert.deepEqual(err.data, { hint: 'nope' });
    c.disconnect();
  });
});

// ---------------------------------------------------------------------------
// GAP U041-w67: extractUrlElicitations — runtime filter of url-mode elicitations
// embedded in a -32042 error's data (parity with ElicitRequestURLParams filter).
// ---------------------------------------------------------------------------
describe('extractUrlElicitations', () => {
  it('keeps only well-formed url-mode entries', () => {
    const data = {
      elicitations: [
        { mode: 'url', url: 'https://a', elicitationId: 'e1', message: 'open a' },
        { mode: 'form', url: 'https://b', elicitationId: 'e2', message: 'not url' },
        { mode: 'url', url: 'https://c', message: 'no id' },
        { mode: 'url', elicitationId: 'e4', message: 'no url' },
        null,
        'garbage',
      ],
    };
    const out = extractUrlElicitations(data);
    assert.equal(out.length, 1);
    assert.deepEqual(out[0], { mode: 'url', url: 'https://a', elicitationId: 'e1', message: 'open a' });
  });

  it('returns [] for non-object / missing / non-array elicitations', () => {
    assert.deepEqual(extractUrlElicitations(null), []);
    assert.deepEqual(extractUrlElicitations('x'), []);
    assert.deepEqual(extractUrlElicitations({}), []);
    assert.deepEqual(extractUrlElicitations({ elicitations: 'nope' }), []);
  });
});

// ---------------------------------------------------------------------------
// GAP U041-w67: URL-elicitation-required (-32042) retry loop on tool calls.
// ---------------------------------------------------------------------------
describe('callTool URL-elicitation (-32042) retry loop', () => {
  const servers: Array<() => void> = [];
  afterEach(() => { servers.splice(0).forEach(c => c()); });

  const urlElicitationData = (url: string) => ({
    elicitations: [{ mode: 'url', url, elicitationId: 'e1', message: `open ${url}` }],
  });

  it('runs the elicitation then retries the call, succeeding on accept', async () => {
    let calls = 0;
    const srv = await startMcpHttpServer((msg, res) => {
      if (msg.method === 'tools/call') {
        calls++;
        if (calls === 1) { rpcError(res, msg.id, MCP_URL_ELICITATION_REQUIRED_CODE, 'url elicitation required', urlElicitationData('https://auth.example/go')); return; }
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { content: [{ type: 'text', text: 'done' }] } }));
        return;
      }
      rpcError(res, msg.id, -1, 'unexpected');
    });
    servers.push(srv.close);
    const c = new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 });
    const seen: McpElicitationRequest[] = [];
    c.onElicitation = async (req): Promise<McpElicitationResult> => { seen.push(req); return { action: 'accept' }; };
    await c.connectHTTP(srv.url);
    const result = await c.callTool('t', {}) as { content: Array<{ text: string }> };
    assert.equal(calls, 2);
    assert.equal(result.content[0].text, 'done');
    assert.equal(seen.length, 1);
    assert.equal(seen[0].mode, 'url');
    assert.equal(seen[0].url, 'https://auth.example/go');
    assert.equal(seen[0].elicitationId, 'e1');
    c.disconnect();
  });

  it('returns an informative (non-throwing) result when the user declines', async () => {
    const srv = await startMcpHttpServer((msg, res) => {
      rpcError(res, msg.id, MCP_URL_ELICITATION_REQUIRED_CODE, 'url elicitation required', urlElicitationData('https://auth.example/go'));
    });
    servers.push(srv.close);
    const c = new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 });
    c.onElicitation = async (): Promise<McpElicitationResult> => ({ action: 'decline' });
    await c.connectHTTP(srv.url);
    const result = await c.callTool('t', {}) as { content: Array<{ text: string }>; isError: boolean };
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /declined/);
    assert.match(result.content[0].text, /https:\/\/auth\.example\/go/);
    c.disconnect();
  });

  it('rethrows the original -32042 when no valid elicitations are present', async () => {
    const srv = await startMcpHttpServer((msg, res) => {
      rpcError(res, msg.id, MCP_URL_ELICITATION_REQUIRED_CODE, 'bad', { elicitations: [{ mode: 'form' }] });
    });
    servers.push(srv.close);
    const c = new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 });
    c.onElicitation = async (): Promise<McpElicitationResult> => ({ action: 'accept' });
    await c.connectHTTP(srv.url);
    const err = await c.callTool('t', {}).then(() => null, e => e);
    assert.ok(err instanceof McpRpcError);
    assert.equal(err.code, MCP_URL_ELICITATION_REQUIRED_CODE);
    c.disconnect();
  });

  it('gives up after MAX_MCP_URL_ELICITATION_RETRIES when the server keeps requesting', async () => {
    let calls = 0;
    const srv = await startMcpHttpServer((msg, res) => {
      if (msg.method === 'tools/call') { calls++; rpcError(res, msg.id, MCP_URL_ELICITATION_REQUIRED_CODE, 'again', urlElicitationData('https://auth.example/go')); return; }
      rpcError(res, msg.id, -1, 'unexpected');
    });
    servers.push(srv.close);
    const c = new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 });
    let accepts = 0;
    c.onElicitation = async (): Promise<McpElicitationResult> => { accepts++; return { action: 'accept' }; };
    await c.connectHTTP(srv.url);
    const err = await c.callTool('t', {}).then(() => null, e => e);
    assert.ok(err instanceof McpRpcError);
    assert.equal(err.code, MCP_URL_ELICITATION_REQUIRED_CODE);
    // 1 initial + MAX retries = MAX+1 tool calls; one elicitation accepted per attempt except the last give-up.
    assert.equal(calls, MAX_MCP_URL_ELICITATION_RETRIES + 1);
    assert.equal(accepts, MAX_MCP_URL_ELICITATION_RETRIES);
    c.disconnect();
  });

  it('rethrows -32042 unchanged when no elicitation handler is wired', async () => {
    const srv = await startMcpHttpServer((msg, res) => {
      rpcError(res, msg.id, MCP_URL_ELICITATION_REQUIRED_CODE, 'need url', urlElicitationData('https://auth.example/go'));
    });
    servers.push(srv.close);
    const c = new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000 });
    await c.connectHTTP(srv.url);
    const err = await c.callTool('t', {}).then(() => null, e => e);
    assert.ok(err instanceof McpRpcError);
    assert.equal(err.code, MCP_URL_ELICITATION_REQUIRED_CODE);
    c.disconnect();
  });
});

// ---------------------------------------------------------------------------
// GAP U041-w67: an unrecoverable auth error on a live tool call flips the client
// to `needs-auth` (parity with Claude Code flipping a client on McpAuthError).
// ---------------------------------------------------------------------------
describe('needs-auth on a tool-call auth failure', () => {
  const servers: Array<() => void> = [];
  afterEach(() => { servers.splice(0).forEach(c => c()); });

  it('flips client state to needs-auth, emits needs-auth, and throws McpAuthError (401)', async () => {
    const srv = await startMcpHttpServer((msg, res) => {
      if (msg.method === 'tools/call') { res.statusCode = 401; res.end(JSON.stringify({ error: 'nope' })); return; }
      rpcError(res, msg.id, -1, 'unexpected');
    });
    servers.push(srv.close);
    const oauth = { authorizationHeader: async () => null, onUnauthorized: async () => false };
    const c = new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000, oauth });
    let emitted = false;
    c.on('needs-auth', () => { emitted = true; });
    await c.connectHTTP(srv.url);
    assert.equal(c.state, 'ready');
    const err = await c.callTool('t', {}).then(() => null, e => e);
    assert.ok(err instanceof McpAuthError);
    assert.match(err.message, /401 Unauthorized/);
    assert.equal(c.state, 'needs-auth');
    assert.equal(emitted, true);
    c.disconnect();
  });

  it('flips to needs-auth on a 403 whose body marks the token revoked', async () => {
    const srv = await startMcpHttpServer((msg, res) => {
      if (msg.method === 'tools/call') { res.statusCode = 403; res.end('OAuth token has been revoked'); return; }
      rpcError(res, msg.id, -1, 'unexpected');
    });
    servers.push(srv.close);
    const oauth = { authorizationHeader: async () => null, onUnauthorized: async () => false };
    const c = new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000, oauth });
    await c.connectHTTP(srv.url);
    const err = await c.callTool('t', {}).then(() => null, e => e);
    assert.ok(err instanceof McpAuthError);
    assert.match(err.message, /token revoked/);
    assert.equal(c.state, 'needs-auth');
    c.disconnect();
  });

  it('a successful reconnect clears the needs-auth state back to ready', async () => {
    let revoked = true;
    const srv = await startMcpHttpServer((msg, res) => {
      if (msg.method === 'tools/call') {
        if (revoked) { res.statusCode = 401; res.end('nope'); return; }
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { ok: true } }));
        return;
      }
      rpcError(res, msg.id, -1, 'unexpected');
    });
    servers.push(srv.close);
    const oauth = { authorizationHeader: async () => null, onUnauthorized: async () => false };
    const c = new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000, oauth });
    await c.connectHTTP(srv.url);
    await c.callTool('t', {}).catch(() => {});
    assert.equal(c.state, 'needs-auth');
    revoked = false;
    await c.reconnect();
    assert.equal(c.state, 'ready');
    c.disconnect();
  });
});

// ---------------------------------------------------------------------------
// GAP U041-w67 (integration): the manager surfaces the needs-auth state end to
// end — getServerState + listAllServers report `needs-auth` / needsAuth after a
// tool call hits an unrecoverable auth error.
// ---------------------------------------------------------------------------
describe('McpManager surfaces tool-call needs-auth end to end', () => {
  const servers: Array<() => void> = [];
  afterEach(() => { servers.splice(0).forEach(c => c()); });

  it('getServerState and listAllServers report needs-auth after a 401 tool call', async () => {
    const srv = await startMcpHttpServer((msg, res) => {
      if (msg.method === 'tools/call') { res.statusCode = 401; res.end('nope'); return; }
      rpcError(res, msg.id, -1, 'unexpected');
    });
    servers.push(srv.close);
    const oauth = { authorizationHeader: async () => null, onUnauthorized: async () => false };
    const client = new McpClient({ command: 'x', args: [], connectTimeout: 3000, requestTimeout: 3000, oauth });
    const mgr = new McpManager();
    mgr._createClient = (() => client) as unknown as typeof mgr._createClient;
    const stateEvents: Array<[string, string]> = [];
    mgr.on('server-state-changed', (name: string, state: string) => stateEvents.push([name, state]));
    mgr.addServer('remote', { command: '', transport: 'http', url: srv.url });
    await mgr.connectServer('remote');
    assert.equal(mgr.getServerState('remote'), 'ready');

    const err = await mgr.callTool('remote:t', {}).then(() => null, e => e);
    assert.ok(err instanceof McpAuthError);
    assert.equal(mgr.getServerState('remote'), 'needs-auth');
    const row = mgr.listAllServers().find(s => s.name === 'remote')!;
    assert.equal(row.state, 'needs-auth');
    assert.equal(row.needsAuth, true);
    assert.equal(row.isAuthenticated, false);
    assert.ok(stateEvents.some(([n, s]) => n === 'remote' && s === 'needs-auth'));
    mgr.removeServer('remote');
  });
});
