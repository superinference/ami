// Unit U018 (wave w80) — core/core/src/mcp/manager.ts
//
// Two additive the reference-parity improvements:
//
//  * substitutePluginMcpVars Windows backslash normalization — the two plugin-PATH
//    substitutions (${CLAUDE_PLUGIN_ROOT} / ${CLAUDE_PLUGIN_DATA}) have backslashes
//    normalized to forward slashes on win32 so a `C:\…\plugin` path dropped into a
//    shell command/arg is not mangled by escape interpretation (the reference-parity
//    `substitutePluginVariables`, pluginOptionsStorage.ts:L312-345). user_config
//    values are opaque user data and are NOT normalized.
//
//  * getServerStateSummary / formatMcpStateSummary — a compact four-way
//    {connected, pending, needsAuth, failed} tally for a single-row status-pane
//    summary, excluding the reserved `ide` pseudo-client and counting intentional
//    disabled/stopped/disconnected states in NO bucket (the reference-parity
//    `buildMcpProperties`, status.tsx:L89-115).
//
// Pure/in-memory — no spawn, no fs, no network. Run from core/core/:
//   node --import tsx --test test/mcp-manager-u018-w80.test.ts
import { describe, it, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';
import {
  McpManager,
  substitutePluginMcpVars,
  formatMcpStateSummary,
  type McpServerStateSummary,
} from '../src/mcp/manager';

after(() => { setTimeout(() => process.exit(0), 100); });

// ---------------------------------------------------------------------------
// substitutePluginMcpVars — Windows backslash normalization
// ---------------------------------------------------------------------------
describe('substitutePluginMcpVars Windows path normalization (U018 w80)', () => {
  const realPlatform = process.platform;
  afterEach(() => {
    Object.defineProperty(process, 'platform', { value: realPlatform, configurable: true });
  });
  const setPlatform = (p: NodeJS.Platform): void => {
    Object.defineProperty(process, 'platform', { value: p, configurable: true });
  };

  it('normalizes backslashes to forward slashes in ROOT/DATA paths on win32', () => {
    setPlatform('win32');
    const out = substitutePluginMcpVars('${CLAUDE_PLUGIN_ROOT}/bin and ${CLAUDE_PLUGIN_DATA}', {
      pluginRoot: 'C:\\Users\\me\\plugin',
      pluginDataDir: 'C:\\Data\\plugin',
    });
    assert.equal(out, 'C:/Users/me/plugin/bin and C:/Data/plugin');
  });

  it('leaves backslashes untouched on non-win32 (POSIX paths have no escape issue)', () => {
    setPlatform('linux');
    const out = substitutePluginMcpVars('${CLAUDE_PLUGIN_ROOT}', { pluginRoot: '/home/me/a\\b' });
    assert.equal(out, '/home/me/a\\b', 'POSIX path with a literal backslash is preserved verbatim');
  });

  it('does NOT normalize user_config values even on win32 (opaque user data)', () => {
    setPlatform('win32');
    const out = substitutePluginMcpVars('${user_config.token}', { userConfig: { token: 'a\\b\\c' } });
    assert.equal(out, 'a\\b\\c', 'user_config is not a plugin path — backslashes stay literal');
  });

  it('missing ROOT/DATA collapse to empty string (unchanged) and never crash', () => {
    setPlatform('win32');
    assert.equal(substitutePluginMcpVars('${CLAUDE_PLUGIN_ROOT}${CLAUDE_PLUGIN_DATA}', {}), '');
  });
});

// ---------------------------------------------------------------------------
// getServerStateSummary / formatMcpStateSummary
// ---------------------------------------------------------------------------
class FakeClient extends EventEmitter {
  state = 'idle';
  serverInfo: unknown = { name: 'fake', version: '1' };
  instructions: string | null = null;
  uptime: number | null = 1;
  tools: Array<{ name: string }> = [];
  resources: unknown[] = [];
  prompts: unknown[] = [];
  serverCapabilities: Record<string, unknown> = {};
  connectCalls: string[] = [];
  async connect(): Promise<void> { this.connectCalls.push('stdio'); this.state = 'ready'; }
  async connectHTTP(): Promise<void> { this.connectCalls.push('http'); this.state = 'ready'; }
  async connectSSE(): Promise<void> { this.connectCalls.push('sse'); this.state = 'ready'; }
  async connectWebSocket(): Promise<void> { this.connectCalls.push('ws'); this.state = 'ready'; }
  async listTools(): Promise<Array<{ name: string }>> { return this.tools; }
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

describe('getServerStateSummary (U018 w80 — four-way state tally)', () => {
  it('buckets ready→connected, connecting→pending, needs-auth→needsAuth, error→failed', () => {
    const { mgr, created } = newManagerWithFake();
    for (const n of ['a-ready', 'b-ready', 'c-conn', 'd-auth', 'e-err']) mgr.addServer(n, { command: 'node' });
    created[0].state = 'ready';
    created[1].state = 'ready';
    created[2].state = 'connecting';
    created[3].state = 'needs-auth';
    created[4].state = 'error';
    assert.deepEqual(mgr.getServerStateSummary(), { connected: 2, pending: 1, needsAuth: 1, failed: 1 });
  });

  it('excludes the reserved `ide` pseudo-client from every bucket', () => {
    const { mgr, created } = newManagerWithFake();
    mgr.addServer('ide', { command: 'node' });   // the editor bridge — surfaced as its own badge
    mgr.addServer('real', { command: 'node' });
    created[0].state = 'ready'; // ide
    created[1].state = 'ready'; // real
    assert.deepEqual(mgr.getServerStateSummary(), { connected: 1, pending: 0, needsAuth: 0, failed: 0 });
  });

  it('counts intentional disabled/stopped/disconnected states in NO bucket (not failures)', () => {
    const { mgr, created } = newManagerWithFake();
    for (const n of ['x-stop', 'y-disc', 'z-ok']) mgr.addServer(n, { command: 'node' });
    created[0].state = 'stopped';
    created[1].state = 'disconnected';
    created[2].state = 'ready';
    assert.deepEqual(mgr.getServerStateSummary(), { connected: 1, pending: 0, needsAuth: 0, failed: 0 });
  });

  it('returns an all-zero summary when no servers are registered', () => {
    const { mgr } = newManagerWithFake();
    assert.deepEqual(mgr.getServerStateSummary(), { connected: 0, pending: 0, needsAuth: 0, failed: 0 });
  });
});

describe('formatMcpStateSummary (U018 w80 — ordered, severity-labeled parts)', () => {
  it('emits non-zero buckets in the fixed order connected→needsAuth→pending→failed', () => {
    const summary: McpServerStateSummary = { connected: 3, pending: 2, needsAuth: 1, failed: 4 };
    assert.deepEqual(formatMcpStateSummary(summary), [
      { state: 'connected', count: 3, label: '3 connected', severity: 'success' },
      { state: 'needsAuth', count: 1, label: '1 need auth', severity: 'warning' },
      { state: 'pending', count: 2, label: '2 pending', severity: 'inactive' },
      { state: 'failed', count: 4, label: '4 failed', severity: 'error' },
    ]);
  });

  it('omits zero buckets', () => {
    assert.deepEqual(formatMcpStateSummary({ connected: 1, pending: 0, needsAuth: 0, failed: 2 }), [
      { state: 'connected', count: 1, label: '1 connected', severity: 'success' },
      { state: 'failed', count: 2, label: '2 failed', severity: 'error' },
    ]);
  });

  it('returns [] when every bucket is zero (render nothing)', () => {
    assert.deepEqual(formatMcpStateSummary({ connected: 0, pending: 0, needsAuth: 0, failed: 0 }), []);
  });
});
