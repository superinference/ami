import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  McpManager,
  resolveMcpServerHeaders,
  validateMcpServerConfigShape,
} from '../src/mcp/manager';
import { McpClient } from '../src/mcp/client';

after(() => { setTimeout(() => process.exit(0), 200); });

// ---------------------------------------------------------------------------
// U018 / w72 — additive MCP manager + client parity
//
//  Gap 52: validateMcpServerConfigShape rejects a malformed `oauth` sub-object
//          (authServerMetadataUrl must be https; callbackPort must be a valid port).
//  Gap 48: resolveMcpServerHeaders emits progress/introspection log hooks.
//  Gap 55: McpClient exposes a public outbound notify() API, wired through
//          McpManager.setRootPaths → client.setRoots (roots/list_changed).
//  Gap 42: setAllowManagedServersOnly blocks every non-enterprise MCP server at load.
// ---------------------------------------------------------------------------

describe('Gap 52 — oauth config validation (validateMcpServerConfigShape)', () => {
  it('accepts a well-formed oauth block (https metadata URL + valid port)', () => {
    const issues = validateMcpServerConfigShape({
      url: 'https://api.example.com',
      transport: 'http',
      oauth: { authServerMetadataUrl: 'https://as.example.com/.well-known/oauth', callbackPort: 33418 },
    });
    assert.deepEqual(issues, []);
  });

  it('rejects a plaintext http authServerMetadataUrl', () => {
    const issues = validateMcpServerConfigShape({
      url: 'https://api.example.com',
      oauth: { authServerMetadataUrl: 'http://as.example.com/meta' },
    });
    assert.ok(issues.some((i) => i.includes('authServerMetadataUrl')), `expected https issue, got ${JSON.stringify(issues)}`);
  });

  it('rejects a non-URL authServerMetadataUrl', () => {
    const issues = validateMcpServerConfigShape({ url: 'https://x', oauth: { authServerMetadataUrl: 'not a url' } });
    assert.ok(issues.some((i) => i.includes('authServerMetadataUrl')));
  });

  it('rejects a non-string authServerMetadataUrl', () => {
    const issues = validateMcpServerConfigShape({ url: 'https://x', oauth: { authServerMetadataUrl: 123 } });
    assert.ok(issues.some((i) => i.includes('authServerMetadataUrl')));
  });

  it('rejects a non-integer / out-of-range callbackPort', () => {
    for (const bad of [0, -1, 70000, 1.5]) {
      const issues = validateMcpServerConfigShape({ url: 'https://x', oauth: { callbackPort: bad } });
      assert.ok(issues.some((i) => i.includes('callbackPort')), `port ${bad} should be rejected`);
    }
  });

  it('rejects an oauth that is not an object', () => {
    const issues = validateMcpServerConfigShape({ url: 'https://x', oauth: 'yes' });
    assert.ok(issues.some((i) => i.includes('`oauth` must be an object')));
  });

  it('leaves a config with no oauth block untouched', () => {
    assert.deepEqual(validateMcpServerConfigShape({ command: 'echo' }), []);
  });
});

describe('Gap 48 — resolveMcpServerHeaders introspection log hooks', () => {
  it('emits Executing + Successfully-retrieved debug lines around a successful helper', async () => {
    const debug: string[] = [];
    const errors: unknown[] = [];
    const out = await resolveMcpServerHeaders(
      's',
      { command: '', url: 'https://x', headersHelper: 'run' },
      async () => JSON.stringify({ A: '1', B: '2' }),
      { onDebug: (m) => debug.push(m), onError: (e) => errors.push(e) },
    );
    assert.deepEqual(out, { A: '1', B: '2' });
    assert.equal(debug[0], 'Executing headersHelper to get dynamic headers');
    assert.equal(debug[1], 'Successfully retrieved 2 headers from headersHelper');
    assert.equal(errors.length, 0);
  });

  it('routes a failing helper to onError (and still falls back to static headers)', async () => {
    const debug: string[] = [];
    const errors: unknown[] = [];
    const out = await resolveMcpServerHeaders(
      's',
      { command: '', url: 'https://x', headers: { Static: 'keep' }, headersHelper: 'run' },
      async () => { throw new Error('boom'); },
      { onDebug: (m) => debug.push(m), onError: (e) => errors.push(e) },
    );
    assert.deepEqual(out, { Static: 'keep' });
    assert.equal(debug[0], 'Executing headersHelper to get dynamic headers');
    assert.equal(errors.length, 1);
    assert.ok(errors[0] instanceof Error);
  });

  it('is a no-op when no hooks are supplied (backward-compatible signature)', async () => {
    const out = await resolveMcpServerHeaders(
      's',
      { command: '', url: 'https://x', headersHelper: 'run' },
      async () => JSON.stringify({ A: '1' }),
    );
    assert.deepEqual(out, { A: '1' });
  });
});

function createNotifyCapturingServer(captured: Array<Record<string, unknown>>): http.Server {
  return http.createServer((req, res) => {
    if (req.method === 'GET') { res.writeHead(405); res.end(); return; }
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', () => {
      const msg = JSON.parse(body);
      captured.push(msg);
      if (msg.method === 'initialize') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05', serverInfo: { name: 'm', version: '1' }, capabilities: { tools: {} } } }));
        return;
      }
      if (msg.id === undefined) { res.writeHead(202); res.end(); return; }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} }));
    });
  });
}

function listen(server: http.Server): Promise<number> {
  return new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve((server.address() as { port: number }).port)));
}

describe('Gap 55 — public outbound notification API', () => {
  it('notify() posts a JSON-RPC notification (no id) over HTTP and resolves', async () => {
    const captured: Array<Record<string, unknown>> = [];
    const server = createNotifyCapturingServer(captured);
    const port = await listen(server);
    const client = new McpClient({ url: `http://127.0.0.1:${port}/`, transport: 'http' });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    await client.notify('file_updated', { filePath: '/a.txt', oldContent: 'x', newContent: 'y' });
    const notif = captured.find((m) => m.method === 'file_updated');
    assert.ok(notif, 'server should have received the file_updated notification');
    assert.equal(notif!.id, undefined, 'a notification must carry no id');
    assert.deepEqual((notif!.params as Record<string, unknown>).filePath, '/a.txt');
    await client.disconnect();
    server.close();
  });

  it('notify() rejects an empty method name', async () => {
    const client = new McpClient({ command: 'echo' });
    await assert.rejects(() => client.notify('  '), /non-empty method/);
  });

  it('notify() rejects when the client is not ready', async () => {
    const client = new McpClient({ command: 'echo' });
    await assert.rejects(() => client.notify('ping'), /not ready/);
  });

  it('setRootPaths propagates roots/list_changed to a connected client', async () => {
    const captured: Array<Record<string, unknown>> = [];
    const server = createNotifyCapturingServer(captured);
    const port = await listen(server);
    const mgr = new McpManager();
    const client = new McpClient({ url: `http://127.0.0.1:${port}/`, transport: 'http' });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    // register the live client on the manager so setRootPaths can reach it
    (mgr as unknown as { clients: Map<string, McpClient> }).clients.set('s', client);
    mgr.setRootPaths(['/workspace/one']);
    // allow the fire-and-forget notify to flush
    await new Promise((r) => setTimeout(r, 50));
    assert.ok(captured.some((m) => m.method === 'notifications/roots/list_changed'), 'expected a roots/list_changed push');
    await client.disconnect();
    server.close();
  });

  it('setRootPaths is a no-op (no push) when roots are unchanged', async () => {
    const captured: Array<Record<string, unknown>> = [];
    const server = createNotifyCapturingServer(captured);
    const port = await listen(server);
    const mgr = new McpManager();
    const client = new McpClient({ url: `http://127.0.0.1:${port}/`, transport: 'http' });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    mgr.setRootPaths(['/w']);
    (mgr as unknown as { clients: Map<string, McpClient> }).clients.set('s', client);
    mgr.setRootPaths(['/w']); // identical → no notification
    await new Promise((r) => setTimeout(r, 30));
    assert.equal(captured.filter((m) => m.method === 'notifications/roots/list_changed').length, 0);
    await client.disconnect();
    server.close();
  });
});

function writeUserConfig(servers: Record<string, unknown>): { dir: string; file: string } {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u018w72-'));
  const file = path.join(dir, 'mcp.json');
  fs.writeFileSync(file, JSON.stringify({ mcpServers: servers }));
  return { dir, file };
}

describe('Gap 42 — allowManagedMcpServersOnly exclusive control', () => {
  it('blocks a non-enterprise (user) server at load when enabled', () => {
    const { dir, file } = writeUserConfig({ userserver: { command: 'echo', args: ['hi'] } });
    try {
      const mgr = new McpManager();
      mgr.setAllowManagedServersOnly(true);
      assert.equal(mgr.isManagedServersOnly(), true);
      const blocked: string[] = [];
      mgr.on('config-blocked', (name: string, reason: string) => {
        if (reason === 'allowManagedMcpServersOnly') blocked.push(name);
      });
      mgr.loadFromConfig(file, { scope: 'user' });
      assert.deepEqual(blocked, ['userserver']);
      assert.equal(mgr.listServers().length, 0, 'no non-managed server should be registered');
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('still loads enterprise/managed servers when enabled', () => {
    const mgr = new McpManager();
    mgr.setAllowManagedServersOnly(true);
    mgr.loadManagedServers({ corp: { command: 'echo', args: ['hi'] } });
    assert.ok(mgr.listServers().some((s) => s.name === 'corp'), 'managed server must still load');
  });

  it('loads a user-scope server when the flag is off (default)', () => {
    const { dir, file } = writeUserConfig({ userserver: { command: 'echo' } });
    try {
      const mgr = new McpManager();
      assert.equal(mgr.isManagedServersOnly(), false);
      mgr.loadFromConfig(file, { scope: 'user' });
      assert.ok(mgr.listServers().some((s) => s.name === 'userserver'));
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
