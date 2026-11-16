import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { McpManager } from '../src/mcp/manager';

// ===========================================================================
// U018 (w67) — MCP manager parity additions:
//  - reloadFromConfig: config-hash diff / stale-client detection on reload
//    (useManageMCPConnections excludeStalePluginClients)
//  - listResources: ensure-connected (reconnect-then-refetch) within the call
//    (ListMcpResourcesTool ensureConnectedClient)
// ===========================================================================

// Minimal fake MCP client (mirrors the ones used by the other mcp-*.test.ts).
// `listResources` throws when the client is not `ready`, emulating the real
// client's assertReady(), so the reconnect-before-fetch path is observable.
class FakeClient extends EventEmitter {
  onElicitation: unknown;
  onSampling: unknown;
  state = 'ready';
  serverInfo = { name: 'fake', version: '1' };
  tools: unknown[] = [];
  resources: Array<{ uri: string; name: string }> = [{ uri: 'file:///r', name: 'r' }];
  prompts: unknown[] = [];
  uptime: number | null = 0;
  reconnectCalls = 0;
  setHeaders(): void {}
  async connect(): Promise<void> {}
  async connectHTTP(): Promise<void> {}
  async connectSSE(): Promise<void> {}
  async listTools(): Promise<unknown[]> { return []; }
  async listResources(): Promise<Array<{ uri: string; name: string }>> {
    if (this.state !== 'ready') throw new Error('client is not ready');
    return this.resources;
  }
  async reconnect(): Promise<void> { this.reconnectCalls++; this.state = 'ready'; }
  async revokeAuth(): Promise<void> {}
  disconnect(): void {}
}

const dirs: string[] = [];
function tmpConfig(obj: unknown): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-u018-w67-'));
  dirs.push(d);
  const p = path.join(d, 'mcp.json');
  fs.writeFileSync(p, JSON.stringify(obj));
  return p;
}
function rewrite(p: string, obj: unknown): void {
  fs.writeFileSync(p, JSON.stringify(obj));
}
afterEach(() => {
  for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
});

describe('McpManager.reloadFromConfig — config-hash diff / stale detection', () => {
  function newManager(): { mgr: McpManager } {
    const mgr = new McpManager();
    mgr._createClient = (() => new FakeClient()) as never;
    return { mgr };
  }

  it('leaves an unchanged server untouched, re-registers a changed one, removes an absent one, adds a new one', () => {
    const { mgr } = newManager();
    const p = tmpConfig({ mcpServers: { a: { command: 'a1' }, b: { command: 'b1' } } });
    mgr.loadFromConfig(p);
    assert.equal(mgr.getServerConfig('a')?.command, 'a1');
    assert.equal(mgr.getServerConfig('b')?.command, 'b1');
    const bClientBefore = (mgr as unknown as { clients: Map<string, unknown> }).clients.get('b');
    assert.ok(bClientBefore);

    // Edit: change a's command, drop b, add c. Reload b stays untouched? No — b is
    // removed. a changed, c added.
    rewrite(p, { mcpServers: { a: { command: 'a2' }, c: { command: 'c1' } } });
    const diff = mgr.reloadFromConfig(p);
    assert.deepEqual(diff.changed, ['a']);
    assert.deepEqual(diff.added, ['c']);
    assert.deepEqual(diff.removed, ['b']);
    assert.deepEqual(diff.unchanged, []);

    assert.equal(mgr.getServerConfig('a')?.command, 'a2', 'changed server re-registered with fresh config');
    assert.equal(mgr.getServerConfig('b'), undefined, 'absent server removed');
    assert.equal(mgr.getServerConfig('c')?.command, 'c1', 'new server added');
  });

  it('a second reload of an unchanged file touches nothing (same client instance, no churn)', () => {
    const { mgr } = newManager();
    const p = tmpConfig({ mcpServers: { a: { command: 'a1' } } });
    mgr.loadFromConfig(p);
    const aClient = (mgr as unknown as { clients: Map<string, unknown> }).clients.get('a');

    const diff = mgr.reloadFromConfig(p);
    assert.deepEqual(diff, { added: [], changed: [], removed: [], unchanged: ['a'] });
    assert.strictEqual(
      (mgr as unknown as { clients: Map<string, unknown> }).clients.get('a'),
      aClient,
      'unchanged server is the same client instance (not torn down/respawned)',
    );
  });

  it('treats a key-order-only difference as unchanged (stable hash)', () => {
    const { mgr } = newManager();
    const p = tmpConfig({ mcpServers: { a: { command: 'x', args: ['1'], env: { A: '1', B: '2' } } } });
    mgr.loadFromConfig(p);
    // Same logical config, different key order within the entry + env.
    rewrite(p, { mcpServers: { a: { env: { B: '2', A: '1' }, args: ['1'], command: 'x' } } });
    const diff = mgr.reloadFromConfig(p);
    assert.deepEqual(diff.unchanged, ['a']);
    assert.deepEqual(diff.changed, []);
  });

  it('removes every server from a file that has become missing/unreadable', () => {
    const { mgr } = newManager();
    const p = tmpConfig({ mcpServers: { a: { command: 'a1' }, b: { command: 'b1' } } });
    mgr.loadFromConfig(p);
    fs.rmSync(p);
    const diff = mgr.reloadFromConfig(p);
    assert.deepEqual(diff.removed.sort(), ['a', 'b']);
    assert.deepEqual(diff.added, []);
    assert.deepEqual(diff.changed, []);
    assert.equal(mgr.getServerConfig('a'), undefined);
    assert.equal(mgr.getServerConfig('b'), undefined);
  });

  it('emits config-reloaded with the diff', () => {
    const { mgr } = newManager();
    const p = tmpConfig({ mcpServers: { a: { command: 'a1' } } });
    mgr.loadFromConfig(p);
    let seen: { path: string; diff: unknown } | null = null;
    mgr.on('config-reloaded', (cp: string, diff: unknown) => { seen = { path: cp, diff }; });
    rewrite(p, { mcpServers: { a: { command: 'a2' } } });
    mgr.reloadFromConfig(p);
    assert.ok(seen);
    assert.equal(seen!.path, p);
    assert.deepEqual(seen!.diff, { added: [], changed: ['a'], removed: [], unchanged: [] });
  });
});

describe('McpManager.listResources — ensure-connected (reconnect-then-refetch)', () => {
  it('reconnects a dropped (not-ready) server before fetching so its resources still appear', async () => {
    const mgr = new McpManager();
    const fake = new FakeClient();
    mgr._createClient = (() => fake) as never;
    mgr.addServer('s', { command: 'x' });
    // Simulate a server dropped by onclose: the client is no longer ready.
    fake.state = 'disconnected';

    const res = await mgr.listResources();
    assert.equal(res.length, 1, 'resources returned after an in-call reconnect');
    assert.equal(res[0].server, 's');
    assert.equal(fake.reconnectCalls, 1, 'listResources reconnected the dropped client once');
    assert.equal(fake.state, 'ready', 'client is ready again after the in-call reconnect');
  });

  it('does not reconnect a server that is already ready', async () => {
    const mgr = new McpManager();
    const fake = new FakeClient();
    mgr._createClient = (() => fake) as never;
    mgr.addServer('s', { command: 'x' });
    const res = await mgr.listResources();
    assert.equal(res.length, 1);
    assert.equal(fake.reconnectCalls, 0, 'a ready client is fetched directly, no reconnect');
  });
});
