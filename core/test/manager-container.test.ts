import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import { McpManager } from '../src/mcp/manager';

after(() => { setTimeout(() => process.exit(0), 200); });

const managerSrc = fs.readFileSync(
  path.resolve(__dirname, '../src/mcp/manager.ts'),
  'utf-8',
);

// ---------------------------------------------------------------------------
// McpServerConfig interface
// ---------------------------------------------------------------------------

describe('McpServerConfig — containerConfig field', () => {
  it('declares containerConfig field typed as ContainerConfig', () => {
    assert.match(managerSrc, /containerConfig\??\s*:\s*ContainerConfig/);
  });

  it('containerConfig is optional', () => {
    assert.match(managerSrc, /containerConfig\?\s*:/);
  });
});

// ---------------------------------------------------------------------------
// Import structure
// ---------------------------------------------------------------------------

describe('McpManager imports', () => {
  it('imports resolveRuntime from container module', () => {
    assert.ok(managerSrc.includes('resolveRuntime'));
    assert.ok(managerSrc.includes("from './container'"));
  });

  it('imports buildContainerArgs from container module', () => {
    assert.ok(managerSrc.includes('buildContainerArgs'));
  });

  it('imports ContainerConfig type from container module', () => {
    assert.ok(managerSrc.includes('type ContainerConfig'));
  });

  it('does not import bridge script', () => {
    assert.ok(!managerSrc.includes('kali-bridge-script'));
  });
});

// ---------------------------------------------------------------------------
// addServer with containerConfig
// ---------------------------------------------------------------------------

describe('addServer with containerConfig', () => {
  it('checks for containerConfig presence', () => {
    assert.match(managerSrc, /if\s*\(\s*config\.containerConfig\s*\)/);
  });

  it('calls resolveRuntime with containerConfig.runtime', () => {
    assert.ok(managerSrc.includes('resolveRuntime(config.containerConfig.runtime)'));
  });

  it('calls buildContainerArgs to generate docker run args', () => {
    assert.ok(managerSrc.includes('buildContainerArgs(config.containerConfig)'));
  });

  it('sets resolvedCommand to the detected runtime', () => {
    const addServerBody = managerSrc.slice(
      managerSrc.indexOf('addServer('),
      managerSrc.indexOf('this.configs.set(name, config)'),
    );
    assert.ok(addServerBody.includes('resolvedCommand = runtime'));
  });

  it('sets resolvedArgs to buildContainerArgs result', () => {
    const addServerBody = managerSrc.slice(
      managerSrc.indexOf('addServer('),
      managerSrc.indexOf('this.configs.set(name, config)'),
    );
    assert.ok(addServerBody.includes('resolvedArgs = buildContainerArgs'));
  });

  it('uses higher default requestTimeout (60s) for container servers', () => {
    assert.match(
      managerSrc,
      /requestTimeout\s*:\s*config\.requestTimeout\s*\?\?\s*\(\s*config\.containerConfig\s*\?\s*60000\s*:\s*30000\s*\)/,
    );
  });

  it('uses higher default connectTimeout (120s) for container servers', () => {
    assert.match(
      managerSrc,
      /connectTimeout\s*:\s*config\.connectTimeout\s*\?\?\s*\(\s*config\.containerConfig\s*\?\s*120000\s*:\s*10000\s*\)/,
    );
  });

  it('does not use bridge script', () => {
    assert.ok(!managerSrc.includes('writeBridgeScript'));
    assert.ok(!managerSrc.includes('process.execPath'));
    assert.ok(!managerSrc.includes('bridgeScripts'));
  });
});

// ---------------------------------------------------------------------------
// Cleanup — no bridge scripts to clean up
// ---------------------------------------------------------------------------

describe('cleanup — no bridge script management', () => {
  it('removeServer does not reference bridgeScripts', () => {
    const removeIdx = managerSrc.indexOf('removeServer(');
    assert.ok(removeIdx !== -1, 'removeServer method must exist');
    const afterRemove = managerSrc.slice(removeIdx, removeIdx + 500);
    assert.ok(!afterRemove.includes('bridgeScript'), 'no bridge script cleanup in removeServer');
  });

  it('disconnectAll does not reference bridgeScripts', () => {
    const disconnectIdx = managerSrc.indexOf('disconnectAll()');
    assert.ok(disconnectIdx !== -1, 'disconnectAll method must exist');
    const afterDisconnect = managerSrc.slice(disconnectIdx, disconnectIdx + 500);
    assert.ok(!afterDisconnect.includes('bridgeScript'), 'no bridge script cleanup in disconnectAll');
  });

  it('no cleanupBridgeScript method exists', () => {
    assert.ok(!managerSrc.includes('cleanupBridgeScript'));
  });
});

// ---------------------------------------------------------------------------
// Integration: register container-backed server
// ---------------------------------------------------------------------------

describe('McpManager container server registration', () => {
  it('registers a container-backed server via addServer', () => {
    const manager = new McpManager();
    manager.addServer('kali', {
      command: '',
      containerConfig: {
        image: 'cyberillo/kali-mcp-server:latest',
        name: 'si-kali-mcp',
        runtime: 'auto',
      },
    });
    const servers = manager.listServers();
    assert.ok(servers.some(s => s.name === 'kali'));
    manager.disconnectAll();
  });

  it('registers multiple servers including container-backed', () => {
    const manager = new McpManager();
    manager.addServer('echo', { command: 'echo', args: ['test'] });
    manager.addServer('kali', {
      command: '',
      containerConfig: {
        image: 'cyberillo/kali-mcp-server:latest',
        name: 'si-kali-mcp',
        runtime: 'auto',
      },
    });
    const servers = manager.listServers();
    assert.equal(servers.length, 2);
    assert.ok(servers.some(s => s.name === 'echo'));
    assert.ok(servers.some(s => s.name === 'kali'));
    manager.disconnectAll();
  });

  it('throws on duplicate container server registration', () => {
    const manager = new McpManager();
    manager.addServer('kali', {
      command: '',
      containerConfig: { image: 'test-image' },
    });
    assert.throws(() => {
      manager.addServer('kali', {
        command: '',
        containerConfig: { image: 'test-image-2' },
      });
    }, /already registered/);
    manager.disconnectAll();
  });

  it('removeServer cleans up container server', () => {
    const manager = new McpManager();
    manager.addServer('kali', {
      command: '',
      containerConfig: { image: 'test-image' },
    });
    manager.removeServer('kali');
    const servers = manager.listServers();
    assert.equal(servers.length, 0);
  });
});
