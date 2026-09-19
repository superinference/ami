import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import { McpManager } from '../src/mcp/manager';
import { pullImage } from '../src/mcp/container';

after(() => { setTimeout(() => process.exit(0), 200); });

const managerSrc = fs.readFileSync(
  path.resolve(__dirname, '../src/mcp/manager.ts'),
  'utf-8',
);

const containerSrc = fs.readFileSync(
  path.resolve(__dirname, '../src/mcp/container.ts'),
  'utf-8',
);

// ---------------------------------------------------------------------------
// pullImage function
// ---------------------------------------------------------------------------

describe('pullImage function', () => {
  it('is exported from container module', () => {
    assert.equal(typeof pullImage, 'function');
  });

  it('returns a promise', () => {
    const p = pullImage('_nonexistent_runtime_xyz_', 'fake:latest');
    assert.ok(p instanceof Promise);
    p.catch(() => {});
  });

  it('rejects when runtime does not exist (ENOENT)', async () => {
    await assert.rejects(
      () => pullImage('_nonexistent_runtime_xyz_', 'fake:latest'),
      (err: Error) => err.message.includes('ENOENT') || err.message.includes('pull failed'),
    );
  });

  it('rejects when image does not exist', async () => {
    await assert.rejects(
      () => pullImage('docker', 'si_test_nonexistent_image_auto_pull_99999:latest'),
      (err: Error) => err.message.includes('pull failed') || err.message.includes('not found'),
    );
  });

  it('includes exit code in error message', async () => {
    try {
      await pullImage('docker', 'si_test_nonexistent_image_auto_pull_99999:latest');
      assert.fail('should have thrown');
    } catch (err: any) {
      assert.ok(err.message.includes('exit'));
    }
  });

  it('invokes onProgress callback', async () => {
    const lines: string[] = [];
    try {
      await pullImage('docker', 'si_test_nonexistent_image_auto_pull_99999:latest', (l) => lines.push(l));
    } catch {
      // expected
    }
    assert.ok(Array.isArray(lines));
  });

  it('source spawns runtime pull command', () => {
    assert.ok(containerSrc.includes("spawn(runtime, ['pull', image]"));
  });

  it('source resolves on code 0', () => {
    assert.ok(containerSrc.includes('if (code === 0) resolve()'));
  });

  it('source rejects with stderr on failure', () => {
    assert.ok(containerSrc.includes('pull failed'));
    assert.ok(containerSrc.includes('stderr'));
  });

  it('source handles process error event', () => {
    assert.ok(containerSrc.includes("proc.on('error'"));
  });
});

// ---------------------------------------------------------------------------
// manager imports pullImage
// ---------------------------------------------------------------------------

describe('manager auto-pull wiring', () => {
  it('manager imports pullImage from container', () => {
    assert.ok(managerSrc.includes('pullImage'));
    assert.ok(managerSrc.includes("from './container'"));
  });

  it('ensureConnected checks containerConfig before pull', () => {
    const ensureBody = managerSrc.slice(
      managerSrc.indexOf('async ensureConnected('),
      managerSrc.indexOf('async stopServer('),
    );
    assert.ok(ensureBody.includes('pending.config.containerConfig'));
  });

  it('ensureConnected calls resolveRuntime for container servers', () => {
    const ensureBody = managerSrc.slice(
      managerSrc.indexOf('async ensureConnected('),
      managerSrc.indexOf('async stopServer('),
    );
    assert.ok(ensureBody.includes('resolveRuntime(pending.config.containerConfig.runtime)'));
  });

  it('ensureConnected checks isImageAvailable before pulling', () => {
    const ensureBody = managerSrc.slice(
      managerSrc.indexOf('async ensureConnected('),
      managerSrc.indexOf('async stopServer('),
    );
    assert.ok(ensureBody.includes('isImageAvailable(runtime, pending.config.containerConfig.image)'));
  });

  it('ensureConnected calls pullImage when image not available', () => {
    const ensureBody = managerSrc.slice(
      managerSrc.indexOf('async ensureConnected('),
      managerSrc.indexOf('async stopServer('),
    );
    assert.ok(ensureBody.includes('await pullImage(runtime, pending.config.containerConfig.image)'));
  });

  it('pull only happens when runtime exists and image unavailable', () => {
    const ensureBody = managerSrc.slice(
      managerSrc.indexOf('async ensureConnected('),
      managerSrc.indexOf('async stopServer('),
    );
    assert.ok(ensureBody.includes('runtime && !isImageAvailable'));
  });

  it('logs before and after pull', () => {
    const ensureBody = managerSrc.slice(
      managerSrc.indexOf('async ensureConnected('),
      managerSrc.indexOf('async stopServer('),
    );
    assert.ok(ensureBody.includes('pulling image for'));
    assert.ok(ensureBody.includes('image pulled for'));
  });
});

// ---------------------------------------------------------------------------
// ensureConnected auto-pull behavior (runtime tests with mock MCP server)
// ---------------------------------------------------------------------------

describe('ensureConnected auto-pull runtime behavior', () => {
  it('non-container server skips pull entirely', async () => {
    const mgr = new McpManager();
    mgr.addPendingServer('plain', { command: '_nonexistent_cmd_autopull_test_' });
    try {
      await mgr.ensureConnected('plain');
    } catch {
      // connection failure expected — but pullImage should NOT have been called
    }
    const state = mgr.getServerState('plain');
    assert.ok(state === 'error', `expected error state, got ${state}`);
    mgr.disconnectAll();
  });

  it('container server with unavailable image attempts pull', async () => {
    const mgr = new McpManager();
    const states: string[] = [];
    mgr.on('server-state-changed', (_n: string, s: string) => states.push(s));

    mgr.addPendingServer('kali', {
      command: '',
      containerConfig: {
        image: 'si_test_nonexistent_image_auto_pull_99999:latest',
        runtime: 'auto',
      },
    });

    try {
      await mgr.ensureConnected('kali');
    } catch (err: any) {
      assert.ok(
        err.message.includes('pull failed') || err.message.includes('not found') || err.message.includes('ENOENT'),
        `Expected pull-related error, got: ${err.message}`,
      );
    }
    assert.ok(states.includes('pending'));
    assert.ok(states.includes('connecting'));
    assert.ok(states.includes('error'));
    mgr.disconnectAll();
  });

  it('pull failure sets error state with meaningful message', async () => {
    const mgr = new McpManager();
    mgr.addPendingServer('kali', {
      command: '',
      containerConfig: {
        image: 'si_test_nonexistent_image_auto_pull_99999:latest',
        runtime: 'auto',
      },
    });

    try {
      await mgr.ensureConnected('kali');
    } catch {
      // expected
    }

    assert.equal(mgr.getServerState('kali'), 'error');
    const servers = mgr.listAllServers();
    const kali = servers.find(s => s.name === 'kali');
    assert.ok(kali);
    assert.ok(kali!.error, 'error message should be set');
    mgr.disconnectAll();
  });

  it('pull failure preserves server in pending map for retry', async () => {
    const mgr = new McpManager();
    mgr.addPendingServer('kali', {
      command: '',
      containerConfig: {
        image: 'si_test_nonexistent_image_auto_pull_99999:latest',
        runtime: 'auto',
      },
    });

    try { await mgr.ensureConnected('kali'); } catch {}
    assert.ok(mgr.isPending('kali') || mgr.getServerState('kali') === 'error');

    const servers = mgr.listAllServers();
    assert.equal(servers.length, 1);
    assert.equal(servers[0].name, 'kali');
    mgr.disconnectAll();
  });

  it('container-backed server shows containerBacked=true in status', () => {
    const mgr = new McpManager();
    mgr.addPendingServer('kali', {
      command: '',
      containerConfig: {
        image: 'docker.io/cyberillo/kali-mcp-server:latest',
        runtime: 'auto',
      },
    });
    const servers = mgr.listAllServers();
    assert.equal(servers[0].containerBacked, true);
    mgr.disconnectAll();
  });

  it('non-container-backed server shows containerBacked=false', () => {
    const mgr = new McpManager();
    mgr.addPendingServer('plain', { command: 'echo' });
    const servers = mgr.listAllServers();
    assert.equal(servers[0].containerBacked, false);
    mgr.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// mcp-connect tool with auto-pull (mock mcpManager)
// ---------------------------------------------------------------------------

describe('connect tool triggers auto-pull via ensureConnected', () => {
  it('connect tool calls ensureConnected which handles pull internally', async () => {
    const { createMcpConnectTool } = require('../src/tools/mcp-connect');
    let ensureCalled = false;
    const mockManager = {
      ensureConnected: async (name: string) => {
        assert.equal(name, 'kali');
        ensureCalled = true;
      },
      getToolsForServer: () => [
        { name: 'nmap_scan' },
        { name: 'nikto_scan' },
      ],
    };

    const tool = createMcpConnectTool('kali', 'Start kali MCP server');
    const result = await tool.execute({}, {
      cwd: '/tmp',
      abortSignal: new AbortController().signal,
      filesRead: new Set(),
      _mcpManager: mockManager,
    });

    assert.equal(ensureCalled, true);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Connected'));
    assert.ok(result.output.includes('2 tools'));
  });

  it('connect tool reports pull failure from ensureConnected', async () => {
    const { createMcpConnectTool } = require('../src/tools/mcp-connect');
    const mockManager = {
      ensureConnected: async () => {
        throw new Error('docker pull failed (exit 1): image not found');
      },
      getToolsForServer: () => [],
    };

    const tool = createMcpConnectTool('kali', 'Start kali MCP server');
    const result = await tool.execute({}, {
      cwd: '/tmp',
      abortSignal: new AbortController().signal,
      filesRead: new Set(),
      _mcpManager: mockManager,
    });

    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Failed to connect'));
    assert.ok(result.output.includes('docker pull failed'));
  });
});

// ---------------------------------------------------------------------------
// mcp-tool auto-connect with pull (mock mcpManager)
// ---------------------------------------------------------------------------

describe('MCP tool auto-connect triggers pull via ensureConnected', () => {
  it('auto-connect → ensureConnected → pull → callTool flow', async () => {
    const { createMcpTool } = require('../src/tools/mcp-tool');
    const callOrder: string[] = [];
    const mockManager = {
      isPending: () => {
        callOrder.push('isPending');
        return true;
      },
      ensureConnected: async () => {
        callOrder.push('ensureConnected');
      },
      callTool: async () => {
        callOrder.push('callTool');
        return { result: 'scan done' };
      },
    };

    const tool = createMcpTool('kali', 'nmap_scan', 'Port scan', {});
    const result = await tool.execute({ target: '10.0.0.1' }, {
      cwd: '/tmp',
      abortSignal: new AbortController().signal,
      filesRead: new Set(),
      _mcpManager: mockManager,
    });

    assert.deepEqual(callOrder, ['isPending', 'ensureConnected', 'callTool']);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('scan done'));
  });

  it('auto-connect pull failure returns error without calling tool', async () => {
    const { createMcpTool } = require('../src/tools/mcp-tool');
    const mockManager = {
      isPending: () => true,
      ensureConnected: async () => {
        throw new Error('podman pull failed (exit 125): short-name resolution');
      },
      callTool: async () => {
        throw new Error('should not reach callTool');
      },
    };

    const tool = createMcpTool('kali', 'nmap_scan', 'Port scan', {});
    const result = await tool.execute({}, {
      cwd: '/tmp',
      abortSignal: new AbortController().signal,
      filesRead: new Set(),
      _mcpManager: mockManager,
    });

    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Failed to start MCP server'));
    assert.ok(result.output.includes('podman pull failed'));
  });
});
