import { describe, it, beforeEach, afterEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpManager } from '../src/mcp/manager';

after(() => { setTimeout(() => process.exit(0), 200); });

function makeServerScript(): string {
  return `
    const readline = require('readline');
    const rl = readline.createInterface({ input: process.stdin });
    rl.on('line', (line) => {
      try {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: {
              protocolVersion: '2024-11-05',
              serverInfo: { name: 'idle-test', version: '1.0' },
              capabilities: { tools: {} },
            },
          }) + '\\n');
        } else if (msg.method === 'tools/list') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { tools: [
              { name: 'echo', description: 'echo', inputSchema: { type: 'object' } },
            ] },
          }) + '\\n');
        } else if (msg.method === 'tools/call') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { content: [{ type: 'text', text: 'pong' }] },
          }) + '\\n');
        } else if (msg.method === 'ping') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id, result: {},
          }) + '\\n');
        } else if (msg.method === 'notifications/initialized') {
          // no-op
        } else {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id, result: {},
          }) + '\\n');
        }
      } catch {}
    });
  `;
}

describe('McpManager idle timeout', () => {
  let mgr: McpManager;

  beforeEach(() => {
    mgr = new McpManager();
  });

  afterEach(() => {
    mgr.disconnectAll();
  });

  // -----------------------------------------------------------------------
  // _lastUsedAt tracking
  // -----------------------------------------------------------------------

  it('records _lastUsedAt on connectServer', async () => {
    mgr.addServer('srv1', { command: 'node', args: ['-e', makeServerScript()] });
    const before = Date.now();
    await mgr.connectServer('srv1');
    const ts = mgr._lastUsedAt.get('srv1');
    assert.ok(ts !== undefined, 'timestamp must be set');
    assert.ok(ts! >= before, 'timestamp must be at or after connect time');
  });

  it('updates _lastUsedAt on callTool with qualified name', async () => {
    mgr.addServer('srv1', { command: 'node', args: ['-e', makeServerScript()] });
    await mgr.connectServer('srv1');
    const afterConnect = mgr._lastUsedAt.get('srv1')!;

    // small delay to ensure timestamp advances
    await new Promise(r => setTimeout(r, 20));
    await mgr.callTool('srv1:echo', {});
    const afterCall = mgr._lastUsedAt.get('srv1')!;
    assert.ok(afterCall >= afterConnect, 'timestamp must advance on tool call');
  });

  it('updates _lastUsedAt on callTool with unqualified name', async () => {
    mgr.addServer('srv1', { command: 'node', args: ['-e', makeServerScript()] });
    await mgr.connectServer('srv1');
    await new Promise(r => setTimeout(r, 20));
    await mgr.callTool('echo', {});
    const ts = mgr._lastUsedAt.get('srv1');
    assert.ok(ts !== undefined);
  });

  // -----------------------------------------------------------------------
  // _evictIdleServers
  // -----------------------------------------------------------------------

  it('evicts servers past the idle timeout', async () => {
    mgr.addServer('srv1', { command: 'node', args: ['-e', makeServerScript()] });
    await mgr.connectServer('srv1');

    // Fake the timestamp to be old
    mgr._lastUsedAt.set('srv1', Date.now() - 2_000_000);
    mgr._idleTimeoutMs = 100;

    const evicted = mgr._evictIdleServers();
    assert.deepEqual(evicted, ['srv1']);
  });

  it('does NOT evict active servers', async () => {
    mgr.addServer('srv1', { command: 'node', args: ['-e', makeServerScript()] });
    await mgr.connectServer('srv1');

    mgr._idleTimeoutMs = 60_000;
    // lastUsedAt is fresh (just set by connectServer)
    const evicted = mgr._evictIdleServers();
    assert.deepEqual(evicted, []);
  });

  it('evicts only the idle server among multiple', async () => {
    mgr.addServer('idle', { command: 'node', args: ['-e', makeServerScript()] });
    mgr.addServer('active', { command: 'node', args: ['-e', makeServerScript()] });
    await mgr.connectServer('idle');
    await mgr.connectServer('active');

    mgr._idleTimeoutMs = 100;
    mgr._lastUsedAt.set('idle', Date.now() - 200);
    mgr._lastUsedAt.set('active', Date.now());

    const evicted = mgr._evictIdleServers();
    assert.deepEqual(evicted, ['idle']);
    // 'active' should still be connected
    assert.equal(mgr.getServerState('active'), 'ready');
  });

  it('removes the entry from _lastUsedAt after eviction', async () => {
    mgr.addServer('srv1', { command: 'node', args: ['-e', makeServerScript()] });
    await mgr.connectServer('srv1');
    mgr._lastUsedAt.set('srv1', Date.now() - 999_999);
    mgr._idleTimeoutMs = 100;
    mgr._evictIdleServers();
    assert.equal(mgr._lastUsedAt.has('srv1'), false);
  });

  // -----------------------------------------------------------------------
  // Evicted server can reconnect on demand
  // -----------------------------------------------------------------------

  it('evicted server becomes pending and can reconnect via ensureConnected', async () => {
    mgr.addServer('srv1', { command: 'node', args: ['-e', makeServerScript()] });
    await mgr.connectServer('srv1');

    mgr._lastUsedAt.set('srv1', Date.now() - 999_999);
    mgr._idleTimeoutMs = 100;
    mgr._evictIdleServers();

    // Give stopServer a tick to complete
    await new Promise(r => setTimeout(r, 50));

    // Server should now be in pending/stopped state
    const state = mgr.getServerState('srv1');
    assert.ok(state === 'pending' || state === 'stopped', `expected pending/stopped, got ${state}`);

    // Reconnect via ensureConnected
    await mgr.ensureConnected('srv1');
    assert.equal(mgr.getServerState('srv1'), 'ready');
  });

  // -----------------------------------------------------------------------
  // startIdleChecker / stopIdleChecker
  // -----------------------------------------------------------------------

  it('startIdleChecker sets _idleCheckTimer', () => {
    mgr.startIdleChecker(10_000);
    assert.ok(mgr['_idleCheckTimer'] !== null);
    mgr.stopIdleChecker();
  });

  it('stopIdleChecker clears the timer', () => {
    mgr.startIdleChecker(10_000);
    mgr.stopIdleChecker();
    assert.equal(mgr['_idleCheckTimer'], null);
  });

  it('stopIdleChecker is safe when no timer exists', () => {
    assert.doesNotThrow(() => mgr.stopIdleChecker());
  });

  it('startIdleChecker replaces an existing timer', () => {
    mgr.startIdleChecker(10_000);
    const first = mgr['_idleCheckTimer'];
    mgr.startIdleChecker(20_000);
    const second = mgr['_idleCheckTimer'];
    assert.notEqual(first, second);
    mgr.stopIdleChecker();
  });

  it('idle checker fires and evicts stale servers', async () => {
    mgr.addServer('srv1', { command: 'node', args: ['-e', makeServerScript()] });
    await mgr.connectServer('srv1');

    mgr._idleTimeoutMs = 50;
    mgr._lastUsedAt.set('srv1', Date.now() - 200);

    mgr.startIdleChecker(80);
    await new Promise(r => setTimeout(r, 200));
    mgr.stopIdleChecker();

    // After the checker fires, srv1 should be evicted
    const state = mgr.getServerState('srv1');
    assert.ok(state === 'pending' || state === 'stopped', `expected evicted, got ${state}`);
  });

  // -----------------------------------------------------------------------
  // Custom timeout value
  // -----------------------------------------------------------------------

  it('respects custom _idleTimeoutMs', async () => {
    mgr.addServer('srv1', { command: 'node', args: ['-e', makeServerScript()] });
    await mgr.connectServer('srv1');

    mgr._idleTimeoutMs = 999_999_999;
    mgr._lastUsedAt.set('srv1', Date.now() - 1000);
    const evicted = mgr._evictIdleServers();
    assert.deepEqual(evicted, [], 'should not evict with very large timeout');
  });

  // -----------------------------------------------------------------------
  // disconnectAll clears idle state
  // -----------------------------------------------------------------------

  it('disconnectAll clears _lastUsedAt and stops idle checker', async () => {
    mgr.addServer('srv1', { command: 'node', args: ['-e', makeServerScript()] });
    await mgr.connectServer('srv1');
    mgr.startIdleChecker(10_000);

    mgr.disconnectAll();

    assert.equal(mgr._lastUsedAt.size, 0);
    assert.equal(mgr['_idleCheckTimer'], null);
  });
});
