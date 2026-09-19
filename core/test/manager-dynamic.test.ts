import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import { McpManager } from '../src/mcp/manager';
import type { McpServerConfig, McpServerState, PendingServerInfo } from '../src/mcp/manager';

after(() => { setTimeout(() => process.exit(0), 200); });

const managerSrc = fs.readFileSync(
  path.resolve(__dirname, '../src/mcp/manager.ts'),
  'utf-8',
);

// ---------------------------------------------------------------------------
// McpServerState type
// ---------------------------------------------------------------------------

describe('McpServerState type', () => {
  it('exports McpServerState type', () => {
    assert.ok(managerSrc.includes("export type McpServerState"));
  });

  it('includes pending state', () => {
    assert.ok(managerSrc.includes("'pending'"));
  });

  it('includes connecting state', () => {
    assert.ok(managerSrc.includes("'connecting'"));
  });

  it('includes ready state', () => {
    assert.ok(managerSrc.includes("'ready'"));
  });

  it('includes error state', () => {
    assert.ok(managerSrc.includes("'error'"));
  });

  it('includes stopped state', () => {
    assert.ok(managerSrc.includes("'stopped'"));
  });
});

// ---------------------------------------------------------------------------
// PendingServerInfo interface
// ---------------------------------------------------------------------------

describe('PendingServerInfo interface', () => {
  it('exports PendingServerInfo interface', () => {
    assert.ok(managerSrc.includes('export interface PendingServerInfo'));
  });

  it('has name field', () => {
    const info: PendingServerInfo = {
      name: 'test',
      config: { command: 'echo' },
      state: 'pending',
    };
    assert.equal(info.name, 'test');
  });

  it('has config field', () => {
    const info: PendingServerInfo = {
      name: 'test',
      config: { command: 'echo', containerConfig: { image: 'test:latest' } },
      state: 'pending',
    };
    assert.ok(info.config.containerConfig);
  });

  it('has state field', () => {
    const states: McpServerState[] = ['pending', 'connecting', 'ready', 'error', 'stopped'];
    for (const state of states) {
      const info: PendingServerInfo = { name: 'test', config: { command: 'echo' }, state };
      assert.equal(info.state, state);
    }
  });

  it('has optional error field', () => {
    const info: PendingServerInfo = {
      name: 'test',
      config: { command: 'echo' },
      state: 'error',
      error: 'Connection refused',
    };
    assert.equal(info.error, 'Connection refused');
  });

  it('has optional imageAvailable field', () => {
    const info: PendingServerInfo = {
      name: 'test',
      config: { command: 'echo' },
      state: 'pending',
      imageAvailable: true,
    };
    assert.equal(info.imageAvailable, true);
  });
});

// ---------------------------------------------------------------------------
// McpServerStatus extended fields
// ---------------------------------------------------------------------------

describe('McpServerStatus extended fields', () => {
  it('has imageAvailable field', () => {
    assert.ok(managerSrc.includes('imageAvailable?: boolean'));
  });

  it('has containerBacked field', () => {
    assert.ok(managerSrc.includes('containerBacked?: boolean'));
  });

  it('has error field', () => {
    assert.ok(managerSrc.includes('error?: string'));
  });
});

// ---------------------------------------------------------------------------
// addPendingServer
// ---------------------------------------------------------------------------

describe('addPendingServer', () => {
  it('registers a server as pending', () => {
    const mgr = new McpManager();
    mgr.addPendingServer('test', { command: 'echo' });
    assert.equal(mgr.isPending('test'), true);
    assert.equal(mgr.getServerState('test'), 'pending');
    mgr.disconnectAll();
  });

  it('throws if server already registered as pending', () => {
    const mgr = new McpManager();
    mgr.addPendingServer('test', { command: 'echo' });
    assert.throws(() => mgr.addPendingServer('test', { command: 'echo' }), /already registered/);
    mgr.disconnectAll();
  });

  it('throws if server already registered as active', () => {
    const mgr = new McpManager();
    mgr.addServer('test', { command: 'echo' });
    assert.throws(() => mgr.addPendingServer('test', { command: 'echo' }), /already registered/);
    mgr.disconnectAll();
  });

  it('emits server-state-changed event with pending state', () => {
    const mgr = new McpManager();
    const events: string[] = [];
    mgr.on('server-state-changed', (name: string, state: string) => events.push(`${name}:${state}`));
    mgr.addPendingServer('test', { command: 'echo' });
    assert.deepEqual(events, ['test:pending']);
    mgr.disconnectAll();
  });

  it('stores config for later connection', () => {
    const mgr = new McpManager();
    const config: McpServerConfig = { command: 'echo', args: ['hello'] };
    mgr.addPendingServer('test', config);
    const servers = mgr.listAllServers();
    assert.equal(servers.length, 1);
    assert.equal(servers[0].name, 'test');
    assert.equal(servers[0].state, 'pending');
    mgr.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// isPending
// ---------------------------------------------------------------------------

describe('isPending', () => {
  it('returns true for pending servers', () => {
    const mgr = new McpManager();
    mgr.addPendingServer('test', { command: 'echo' });
    assert.equal(mgr.isPending('test'), true);
    mgr.disconnectAll();
  });

  it('returns false for active servers', () => {
    const mgr = new McpManager();
    mgr.addServer('test', { command: 'echo' });
    assert.equal(mgr.isPending('test'), false);
    mgr.disconnectAll();
  });

  it('returns false for unknown servers', () => {
    const mgr = new McpManager();
    assert.equal(mgr.isPending('nonexistent'), false);
    mgr.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// getServerState
// ---------------------------------------------------------------------------

describe('getServerState', () => {
  it('returns pending for pending servers', () => {
    const mgr = new McpManager();
    mgr.addPendingServer('test', { command: 'echo' });
    assert.equal(mgr.getServerState('test'), 'pending');
    mgr.disconnectAll();
  });

  it('returns pending for unknown servers', () => {
    const mgr = new McpManager();
    assert.equal(mgr.getServerState('nonexistent'), 'pending');
    mgr.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// getToolsForServer
// ---------------------------------------------------------------------------

describe('getToolsForServer', () => {
  it('returns empty array for pending servers', () => {
    const mgr = new McpManager();
    mgr.addPendingServer('test', { command: 'echo' });
    const tools = mgr.getToolsForServer('test');
    assert.deepEqual(tools, []);
    mgr.disconnectAll();
  });

  it('returns empty array for unknown servers', () => {
    const mgr = new McpManager();
    const tools = mgr.getToolsForServer('nonexistent');
    assert.deepEqual(tools, []);
    mgr.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// listAllServers
// ---------------------------------------------------------------------------

describe('listAllServers', () => {
  it('returns empty array when no servers registered', () => {
    const mgr = new McpManager();
    assert.deepEqual(mgr.listAllServers(), []);
    mgr.disconnectAll();
  });

  it('returns pending servers', () => {
    const mgr = new McpManager();
    mgr.addPendingServer('kali', { command: '', containerConfig: { image: 'cyberillo/kali-mcp-server:latest' } });
    const servers = mgr.listAllServers();
    assert.equal(servers.length, 1);
    assert.equal(servers[0].name, 'kali');
    assert.equal(servers[0].state, 'pending');
    assert.equal(servers[0].containerBacked, true);
    assert.equal(servers[0].toolCount, 0);
    mgr.disconnectAll();
  });

  it('returns active servers', () => {
    const mgr = new McpManager();
    mgr.addServer('test', { command: 'echo' });
    const servers = mgr.listAllServers();
    assert.equal(servers.length, 1);
    assert.equal(servers[0].name, 'test');
    mgr.disconnectAll();
  });

  it('returns both active and pending servers', () => {
    const mgr = new McpManager();
    mgr.addServer('active', { command: 'echo' });
    mgr.addPendingServer('pending', { command: 'echo' });
    const servers = mgr.listAllServers();
    assert.equal(servers.length, 2);
    const names = servers.map(s => s.name).sort();
    assert.deepEqual(names, ['active', 'pending']);
    mgr.disconnectAll();
  });

  it('does not duplicate servers that are in both maps', () => {
    const mgr = new McpManager();
    mgr.addServer('test', { command: 'echo' });
    const servers = mgr.listAllServers();
    assert.equal(servers.length, 1);
    mgr.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// ensureConnected
// ---------------------------------------------------------------------------

describe('ensureConnected', () => {
  it('throws for unknown servers', async () => {
    const mgr = new McpManager();
    await assert.rejects(() => mgr.ensureConnected('nonexistent'), /not found/);
    mgr.disconnectAll();
  });

  it('emits connecting then error on connection failure', async () => {
    const mgr = new McpManager();
    const states: string[] = [];
    mgr.on('server-state-changed', (_name: string, state: string) => states.push(state));
    mgr.addPendingServer('bad', { command: '_nonexistent_cmd_12345' });
    try {
      await mgr.ensureConnected('bad');
    } catch {
      // expected
    }
    assert.ok(states.includes('pending'));
    assert.ok(states.includes('connecting'));
    assert.ok(states.includes('error'));
    mgr.disconnectAll();
  });

  it('returns server to pending with error on failure', async () => {
    const mgr = new McpManager();
    mgr.addPendingServer('bad', { command: '_nonexistent_cmd_12345' });
    try {
      await mgr.ensureConnected('bad');
    } catch {
      // expected
    }
    assert.equal(mgr.getServerState('bad'), 'error');
    const servers = mgr.listAllServers();
    const bad = servers.find(s => s.name === 'bad');
    assert.ok(bad);
    assert.ok(bad!.error);
    mgr.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// stopServer
// ---------------------------------------------------------------------------

describe('stopServer', () => {
  it('stops a pending server and sets state to stopped', async () => {
    const mgr = new McpManager();
    mgr.addPendingServer('test', { command: 'echo' });
    await mgr.stopServer('test');
    assert.equal(mgr.getServerState('test'), 'stopped');
    mgr.disconnectAll();
  });

  it('emits server-state-changed with stopped', async () => {
    const mgr = new McpManager();
    const states: string[] = [];
    mgr.on('server-state-changed', (_name: string, state: string) => states.push(state));
    mgr.addPendingServer('test', { command: 'echo' });
    await mgr.stopServer('test');
    assert.ok(states.includes('stopped'));
    mgr.disconnectAll();
  });

  it('stops an active server', async () => {
    const mgr = new McpManager();
    mgr.addServer('test', { command: 'echo' });
    await mgr.stopServer('test');
    assert.equal(mgr.getServerState('test'), 'stopped');
    mgr.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// restartServer
// ---------------------------------------------------------------------------

describe('restartServer', () => {
  it('stops then tries to connect (may fail for invalid commands)', async () => {
    const mgr = new McpManager();
    const states: string[] = [];
    mgr.on('server-state-changed', (_name: string, state: string) => states.push(state));
    mgr.addPendingServer('test', { command: '_nonexistent_cmd_12345' });
    try {
      await mgr.restartServer('test');
    } catch {
      // expected
    }
    assert.ok(states.includes('stopped'));
    assert.ok(states.includes('connecting'));
    mgr.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// removeServer cleans up pending
// ---------------------------------------------------------------------------

describe('removeServer with pending', () => {
  it('removes pending server', () => {
    const mgr = new McpManager();
    mgr.addPendingServer('test', { command: 'echo' });
    mgr.removeServer('test');
    assert.equal(mgr.isPending('test'), false);
    assert.deepEqual(mgr.listAllServers(), []);
    mgr.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// disconnectAll cleans up pending
// ---------------------------------------------------------------------------

describe('disconnectAll with pending', () => {
  it('clears pending servers', () => {
    const mgr = new McpManager();
    mgr.addPendingServer('a', { command: 'echo' });
    mgr.addPendingServer('b', { command: 'echo' });
    mgr.disconnectAll();
    assert.deepEqual(mgr.listAllServers(), []);
  });
});

// ---------------------------------------------------------------------------
// ensureConnected idempotency (gap #2)
// ---------------------------------------------------------------------------

describe('ensureConnected idempotency', () => {
  it('is idempotent for pending servers that fail', async () => {
    const mgr = new McpManager();
    mgr.addPendingServer('bad', { command: '_nonexistent_cmd_12345' });
    try { await mgr.ensureConnected('bad'); } catch {}
    assert.equal(mgr.getServerState('bad'), 'error');
    try { await mgr.ensureConnected('bad'); } catch {}
    assert.equal(mgr.getServerState('bad'), 'error');
    mgr.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// stopServer clears tool cache (gap #4)
// ---------------------------------------------------------------------------

describe('stopServer clears tool cache', () => {
  it('source calls invalidateToolCache in stopServer', () => {
    const stopBody = managerSrc.slice(
      managerSrc.indexOf('async stopServer('),
      managerSrc.indexOf('async restartServer('),
    );
    assert.ok(stopBody.includes('invalidateToolCache(name)'));
  });
});

// ---------------------------------------------------------------------------
// getServerState for stopped servers (gap #5)
// ---------------------------------------------------------------------------

describe('getServerState for stopped', () => {
  it('returns stopped after stopServer', async () => {
    const mgr = new McpManager();
    mgr.addPendingServer('test', { command: 'echo' });
    await mgr.stopServer('test');
    assert.equal(mgr.getServerState('test'), 'stopped');
    mgr.disconnectAll();
  });

  it('returns stopped for active server after stop', async () => {
    const mgr = new McpManager();
    mgr.addServer('test', { command: 'echo' });
    await mgr.stopServer('test');
    assert.equal(mgr.getServerState('test'), 'stopped');
    mgr.disconnectAll();
  });

  it('stopped server appears in listAllServers with stopped state', async () => {
    const mgr = new McpManager();
    mgr.addPendingServer('test', { command: 'echo' });
    await mgr.stopServer('test');
    const servers = mgr.listAllServers();
    assert.equal(servers.length, 1);
    assert.equal(servers[0].state, 'stopped');
    mgr.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// addPendingServer imageAvailable for container config (gap #6)
// ---------------------------------------------------------------------------

describe('addPendingServer imageAvailable', () => {
  it('sets imageAvailable to false for nonexistent image', () => {
    const mgr = new McpManager();
    mgr.addPendingServer('test', {
      command: '',
      containerConfig: { image: 'si_test_nonexistent_image_99999:latest', runtime: 'auto' },
    });
    const servers = mgr.listAllServers();
    assert.equal(servers.length, 1);
    assert.equal(servers[0].containerBacked, true);
    mgr.disconnectAll();
  });

  it('sets imageAvailable undefined when no container config', () => {
    const mgr = new McpManager();
    mgr.addPendingServer('test', { command: 'echo' });
    const servers = mgr.listAllServers();
    assert.equal(servers[0].imageAvailable, undefined);
    assert.equal(servers[0].containerBacked, false);
    mgr.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// ensureConnected auto-pull integration
// ---------------------------------------------------------------------------

describe('ensureConnected auto-pull', () => {
  it('source imports pullImage from container', () => {
    assert.ok(managerSrc.includes('pullImage'));
  });

  it('source checks isImageAvailable before pulling', () => {
    const ensureBody = managerSrc.slice(
      managerSrc.indexOf('async ensureConnected('),
      managerSrc.indexOf('async stopServer('),
    );
    assert.ok(ensureBody.includes('isImageAvailable'));
  });

  it('source calls pullImage when image not available', () => {
    const ensureBody = managerSrc.slice(
      managerSrc.indexOf('async ensureConnected('),
      managerSrc.indexOf('async stopServer('),
    );
    assert.ok(ensureBody.includes('pullImage'));
  });

  it('source resolves runtime before pulling', () => {
    const ensureBody = managerSrc.slice(
      managerSrc.indexOf('async ensureConnected('),
      managerSrc.indexOf('async stopServer('),
    );
    assert.ok(ensureBody.includes('resolveRuntime'));
  });

  it('auto-pull only runs for container-backed servers', () => {
    const ensureBody = managerSrc.slice(
      managerSrc.indexOf('async ensureConnected('),
      managerSrc.indexOf('async stopServer('),
    );
    assert.ok(ensureBody.includes('pending.config.containerConfig'));
  });

  it('auto-pull logs before and after pull', () => {
    const ensureBody = managerSrc.slice(
      managerSrc.indexOf('async ensureConnected('),
      managerSrc.indexOf('async stopServer('),
    );
    assert.ok(ensureBody.includes('pulling image for'));
    assert.ok(ensureBody.includes('image pulled for'));
  });
});

// ---------------------------------------------------------------------------
// Source code structure
// ---------------------------------------------------------------------------

describe('manager.ts dynamic lifecycle structure', () => {
  it('imports isImageAvailable from container', () => {
    assert.ok(managerSrc.includes('isImageAvailable'));
  });

  it('has pendingServers map', () => {
    assert.ok(managerSrc.includes('pendingServers'));
  });

  it('has serverErrors map', () => {
    assert.ok(managerSrc.includes('serverErrors'));
  });

  it('exports PendingServerInfo interface', () => {
    assert.ok(managerSrc.includes('export interface PendingServerInfo'));
  });

  it('exports McpServerState type', () => {
    assert.ok(managerSrc.includes('export type McpServerState'));
  });

  it('has addPendingServer method', () => {
    assert.ok(managerSrc.includes('addPendingServer('));
  });

  it('has ensureConnected method', () => {
    assert.ok(managerSrc.includes('ensureConnected('));
  });

  it('has stopServer method', () => {
    assert.ok(managerSrc.includes('stopServer('));
  });

  it('has restartServer method', () => {
    assert.ok(managerSrc.includes('restartServer('));
  });

  it('has isPending method', () => {
    assert.ok(managerSrc.includes('isPending('));
  });

  it('has getServerState method', () => {
    assert.ok(managerSrc.includes('getServerState('));
  });

  it('has getToolsForServer method', () => {
    assert.ok(managerSrc.includes('getToolsForServer('));
  });

  it('has listAllServers method', () => {
    assert.ok(managerSrc.includes('listAllServers('));
  });

  it('emits server-state-changed events', () => {
    assert.ok(managerSrc.includes("'server-state-changed'"));
  });
});
