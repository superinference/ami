import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { McpManager, type McpServerConfig } from '../src/mcp/manager';
import { resolveConfigDir } from '../src/reference-journal';
import {
  MCP_NEEDS_AUTH_CACHE_TTL_MS,
  isNeedsAuthCached,
  setNeedsAuthEntry,
  clearNeedsAuthEntry,
  pruneNeedsAuthCache,
  readNeedsAuthCache,
  writeNeedsAuthCache,
  type NeedsAuthCache,
} from '../src/mcp/needs-auth-cache';

// ---------------------------------------------------------------------------
// U018 — needs-auth skip cache (the reference parity, services/mcp/client.ts:
//   mcp-needs-auth-cache.json, MCP_AUTH_CACHE_TTL_MS, isMcpAuthCached /
//   hasMcpDiscoveryButNoToken short-circuit): a remote MCP server that returns
//   401 within the TTL is a guaranteed-401, so the next connectAll pass SKIPS it
//   (no network round-trip + OAuth discovery) and short-circuits to needs-auth.
// ---------------------------------------------------------------------------

/** A fake client whose connect method either succeeds or rejects with a chosen error. */
class FakeClient extends EventEmitter {
  state = 'idle';
  serverInfo: unknown = { name: 'fake', version: '1' };
  instructions: string | null = null;
  uptime: number | null = 1;
  tools: Array<{ name: string; description?: string }> = [];
  resources: unknown[] = [];
  prompts: unknown[] = [];
  serverCapabilities: Record<string, unknown> = {};
  onElicitation: unknown;
  onSampling: unknown;
  connectCalls: string[] = [];
  /** When set, every remote connect rejects with this error. */
  failWith: Error | null = null;
  private failRemote(kind: string): void {
    this.connectCalls.push(kind);
    // Mirror the real McpClient: a 401 parks the client in `needs-auth` (not a hard
    // error) so the manager surfaces the re-auth affordance.
    if (this.failWith) { this.state = 'needs-auth'; throw this.failWith; }
    this.state = 'ready';
  }
  async connect(): Promise<void> { this.connectCalls.push('stdio'); this.state = 'ready'; }
  async connectHTTP(): Promise<void> { this.failRemote('http'); }
  async connectSSE(): Promise<void> { this.failRemote('sse'); }
  async connectWebSocket(): Promise<void> { this.failRemote('ws'); }
  async listTools(): Promise<Array<{ name: string; description?: string }>> { return this.tools; }
  async listResources(): Promise<unknown[]> { return this.resources; }
  async reconnect(): Promise<void> {}
  disconnect(): void { this.state = 'stopped'; }
  setHeaders(): void {}
  async revokeAuth(): Promise<void> {}
}

function newManagerWithFake(): { mgr: McpManager; created: FakeClient[] } {
  const mgr = new McpManager();
  const created: FakeClient[] = [];
  (mgr as unknown as { _createClient: (o: unknown) => FakeClient })._createClient = () => {
    const c = new FakeClient();
    created.push(c);
    return c;
  };
  return { mgr, created };
}

const REMOTE_CFG: McpServerConfig = { command: '', transport: 'http', url: 'https://host/mcp' };

// ===========================================================================
// Pure cache primitives
// ===========================================================================
describe('needs-auth-cache pure helpers', () => {
  it('isNeedsAuthCached honours the TTL window', () => {
    const now = 1_000_000;
    const cache: NeedsAuthCache = { a: { ts: now } };
    assert.equal(isNeedsAuthCached(cache, 'a', now, 1000), true);
    assert.equal(isNeedsAuthCached(cache, 'a', now + 999, 1000), true);
    assert.equal(isNeedsAuthCached(cache, 'a', now + 1000, 1000), false, 'expired at exactly TTL');
    assert.equal(isNeedsAuthCached(cache, 'missing', now, 1000), false);
  });

  it('setNeedsAuthEntry / clearNeedsAuthEntry are immutable + identity-stable on no-op', () => {
    const base: NeedsAuthCache = {};
    const withA = setNeedsAuthEntry(base, 'a', 5);
    assert.deepEqual(withA, { a: { ts: 5 } });
    assert.deepEqual(base, {}, 'original untouched');
    assert.equal(clearNeedsAuthEntry(withA, 'absent'), withA, 'no-op returns same ref');
    assert.deepEqual(clearNeedsAuthEntry(withA, 'a'), {});
  });

  it('pruneNeedsAuthCache drops expired entries (same ref when nothing pruned)', () => {
    const now = 10_000;
    const cache: NeedsAuthCache = { fresh: { ts: now }, stale: { ts: now - 2000 } };
    assert.deepEqual(pruneNeedsAuthCache(cache, now, 1000), { fresh: { ts: now } });
    const allFresh: NeedsAuthCache = { a: { ts: now } };
    assert.equal(pruneNeedsAuthCache(allFresh, now, 1000), allFresh);
  });

  it('read/write round-trips and tolerates missing/corrupt/malformed files', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nac-'));
    try {
      const file = path.join(dir, 'sub', 'cache.json');
      assert.deepEqual(readNeedsAuthCache(file), {}, 'missing file → empty');
      writeNeedsAuthCache(file, { a: { ts: 7 } });
      assert.deepEqual(readNeedsAuthCache(file), { a: { ts: 7 } });
      fs.writeFileSync(file, 'not json');
      assert.deepEqual(readNeedsAuthCache(file), {}, 'corrupt → empty');
      fs.writeFileSync(file, JSON.stringify({ ok: { ts: 3 }, bad: { ts: 'x' }, arr: [1] }));
      assert.deepEqual(readNeedsAuthCache(file), { ok: { ts: 3 } }, 'drops non-numeric ts');
      fs.writeFileSync(file, JSON.stringify([1, 2, 3]));
      assert.deepEqual(readNeedsAuthCache(file), {}, 'top-level array → empty');
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('exposes the reference-parity 15-minute TTL default', () => {
    assert.equal(MCP_NEEDS_AUTH_CACHE_TTL_MS, 15 * 60 * 1000);
  });
});

// ===========================================================================
// Manager integration — record, skip, persist, clear
// ===========================================================================
describe('McpManager needs-auth skip cache', () => {
  let tmp: string;
  let prevDataDir: string | undefined;
  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-nac-'));
    prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = tmp;
  });
  afterEach(() => {
    if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
    else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  const cacheFile = () => path.join(resolveConfigDir(), 'mcp-needs-auth-cache.json');

  it('records a needs-auth entry when a remote connect returns 401, and persists it', async () => {
    const { mgr, created } = newManagerWithFake();
    mgr.addServer('remote', REMOTE_CFG);
    created[0].failWith = new Error('HTTP 401 Unauthorized');

    const results = await mgr.connectAll();
    assert.ok(results.get('remote') instanceof Error, 'first pass surfaces the 401 error');
    assert.equal(mgr.isNeedsAuthCached('remote'), true);
    assert.deepEqual(Object.keys(readNeedsAuthCache(cacheFile())), ['remote'], 'persisted to disk');
    // State is surfaced as needs-auth for the /mcp view.
    const status = mgr.listAllServers().find(s => s.name === 'remote')!;
    assert.equal(status.state, 'needs-auth');
    assert.equal(status.needsAuth, true);
  });

  it('skips the connect on a later pass while cached (no network probe)', async () => {
    // First manager records the 401.
    const first = newManagerWithFake();
    first.mgr.addServer('remote', REMOTE_CFG);
    first.created[0].failWith = new Error('401 unauthorized');
    await first.mgr.connectAll();
    assert.equal(first.created[0].connectCalls.length, 1, 'first pass did probe');

    // A fresh manager (same data dir) reads the persisted cache and short-circuits.
    const second = newManagerWithFake();
    second.mgr.addServer('remote', REMOTE_CFG);
    const results = await second.mgr.connectAll();
    assert.equal(second.created[0].connectCalls.length, 0, 'cached server is NOT probed');
    assert.equal(results.get('remote'), null, 'skip is not reported as an error');
    assert.equal(second.mgr.getServerState('remote'), 'needs-auth');
  });

  it('re-probes once the TTL has expired', async () => {
    // Seed a stale (expired) entry directly on disk.
    writeNeedsAuthCache(cacheFile(), { remote: { ts: Date.now() - MCP_NEEDS_AUTH_CACHE_TTL_MS - 1000 } });
    const { mgr, created } = newManagerWithFake();
    mgr.addServer('remote', REMOTE_CFG);
    assert.equal(mgr.isNeedsAuthCached('remote'), false, 'expired entry is not cached');
    await mgr.connectAll();
    assert.equal(created[0].connectCalls.length, 1, 'expired → re-probed');
  });

  it('clears the entry on a successful connect', async () => {
    writeNeedsAuthCache(cacheFile(), { remote: { ts: Date.now() - MCP_NEEDS_AUTH_CACHE_TTL_MS - 1000 } });
    const { mgr } = newManagerWithFake();
    mgr.addServer('remote', REMOTE_CFG);
    await mgr.connectAll(); // succeeds (failWith unset)
    assert.equal(mgr.getServerState('remote'), 'ready');
    assert.deepEqual(readNeedsAuthCache(cacheFile()), {}, 'success purged the stale entry');
  });

  it('reconnect() clears a cached needs-auth marker so a re-auth re-probes', async () => {
    const { mgr, created } = newManagerWithFake();
    mgr.addServer('remote', REMOTE_CFG);
    created[0].failWith = new Error('got a 401 back');
    await mgr.connectAll();
    assert.equal(mgr.isNeedsAuthCached('remote'), true);

    // User re-authenticates: the next connect should succeed now.
    created[0].failWith = null;
    await mgr.reconnect('remote');
    assert.equal(mgr.isNeedsAuthCached('remote'), false, 'reconnect cleared the cache');
    assert.equal(mgr.getServerState('remote'), 'ready');
  });

  it('does NOT cache a stdio server whose connect fails (remote-only concern)', async () => {
    const { mgr, created } = newManagerWithFake();
    mgr.addServer('local', { command: 'node', args: ['server.js'] });
    created[0].failWith = new Error('401'); // only remote connect paths fail; stdio uses connect()
    // connect() does not throw in the fake, but assert the cache stays empty regardless.
    await mgr.connectAll();
    assert.equal(mgr.isNeedsAuthCached('local'), false);
    assert.deepEqual(readNeedsAuthCache(cacheFile()), {});
  });

  it('clearNeedsAuthCache() with no name wipes the whole cache', async () => {
    const { mgr, created } = newManagerWithFake();
    mgr.addServer('a', { command: '', transport: 'http', url: 'https://a/mcp' });
    mgr.addServer('b', { command: '', transport: 'sse', url: 'https://b/mcp' });
    created[0].failWith = new Error('401'); created[1].failWith = new Error('401');
    await mgr.connectAll();
    assert.equal(mgr.isNeedsAuthCached('a'), true);
    assert.equal(mgr.isNeedsAuthCached('b'), true);
    mgr.clearNeedsAuthCache();
    assert.deepEqual(readNeedsAuthCache(cacheFile()), {});
    assert.equal(mgr.isNeedsAuthCached('a'), false);
  });
});
