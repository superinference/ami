import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { McpManager } from '../src/mcp/manager';
import { pullImage } from '../src/mcp/container';
import { createMcpConnectTool } from '../src/tools/mcp-connect';
import { createMcpTool } from '../src/tools/mcp-tool';

after(() => { setTimeout(() => process.exit(0), 200); });

// ---------------------------------------------------------------------------
// Fixture runtimes: tiny executable scripts that stand in for docker/podman so
// pullImage's spawn/resolve/reject/stderr/onProgress paths can be driven
// deterministically without a real container runtime.
// ---------------------------------------------------------------------------
const fixtureDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-pull-'));

function writeRuntime(name: string, body: string): string {
  const p = path.join(fixtureDir, name);
  fs.writeFileSync(p, `#!/bin/sh\n${body}\n`);
  fs.chmodSync(p, 0o755);
  return p;
}

// Echoes its argv to stdout and exits 0 — lets us observe the exact command.
const okRuntime = writeRuntime('fake-ok', 'echo "argv:$*"\necho "pull-complete"\nexit 0');
// Writes diagnostics to stderr and exits non-zero — the failure path.
const failRuntime = writeRuntime('fake-fail', 'echo "no such image: $2" >&2\nexit 7');

after(() => { fs.rmSync(fixtureDir, { recursive: true, force: true }); });

// ---------------------------------------------------------------------------
// pullImage function — real behavior
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

  it('rejects via the process error event when the runtime binary is missing (ENOENT)', async () => {
    await assert.rejects(
      () => pullImage('_nonexistent_runtime_xyz_', 'fake:latest'),
      (err: Error) => /ENOENT/.test(err.message),
    );
  });

  it('spawns the runtime with ["pull", image] and resolves on exit code 0', async () => {
    const seen: string[] = [];
    await assert.doesNotReject(() =>
      pullImage(okRuntime, 'myrepo/img:1', (line) => seen.push(line)),
    );
    // argv observed by the fixture proves spawn(runtime, ['pull', image]).
    assert.ok(seen.some((l) => l === 'argv:pull myrepo/img:1'), seen.join('|'));
  });

  it('streams stdout lines to onProgress', async () => {
    const seen: string[] = [];
    await pullImage(okRuntime, 'img:2', (line) => seen.push(line));
    assert.ok(seen.includes('pull-complete'));
  });

  it('rejects with exit code and stderr on non-zero exit', async () => {
    await assert.rejects(
      () => pullImage(failRuntime, 'ghost:latest'),
      (err: Error) =>
        err.message.includes('pull failed') &&
        err.message.includes('exit 7') &&
        err.message.includes('no such image: ghost:latest'),
    );
  });

  it('forwards stderr lines to onProgress even on failure', async () => {
    const seen: string[] = [];
    try {
      await pullImage(failRuntime, 'ghost:latest', (line) => seen.push(line));
    } catch {
      // expected
    }
    assert.ok(seen.some((l) => l.includes('no such image')));
  });
});

// ---------------------------------------------------------------------------
// ensureConnected auto-pull decision path — driven via injected runtime hooks.
// This replaces the former source-text assertions with real behavior: the exact
// sequence of resolveRuntime / isImageAvailable / pullImage calls is observed.
// ---------------------------------------------------------------------------

interface HookCalls {
  resolve: (string | undefined)[];
  avail: [string, string][];
  pull: [string, string][];
}

function makeMgr(opts: { runtime: string | null; available: boolean; pullError?: string }) {
  const mgr = new McpManager();
  const calls: HookCalls = { resolve: [], avail: [], pull: [] };
  mgr._resolveRuntime = (pref?: 'docker' | 'podman' | 'auto') => {
    calls.resolve.push(pref);
    return opts.runtime;
  };
  mgr._isImageAvailable = (rt: string, img: string) => {
    calls.avail.push([rt, img]);
    return opts.available;
  };
  mgr._pullImage = async (rt: string, img: string) => {
    calls.pull.push([rt, img]);
    if (opts.pullError) throw new Error(opts.pullError);
  };
  return { mgr, calls };
}

describe('ensureConnected auto-pull decision path', () => {
  it('resolves runtime, checks availability, and pulls when image is unavailable', async () => {
    const { mgr, calls } = makeMgr({ runtime: '_fake_rt_di_', available: false });
    mgr.addPendingServer('kali', {
      command: '',
      containerConfig: { image: 'kali:latest', runtime: 'auto' },
    });
    try { await mgr.ensureConnected('kali'); } catch { /* connection fails after pull */ }

    assert.ok(calls.resolve.includes('auto'));
    assert.deepEqual(calls.avail[0], ['_fake_rt_di_', 'kali:latest']);
    assert.deepEqual(calls.pull[0], ['_fake_rt_di_', 'kali:latest']);
    mgr.disconnectAll();
  });

  it('skips the pull when the image is already available', async () => {
    const { mgr, calls } = makeMgr({ runtime: '_fake_rt_di_', available: true });
    mgr.addPendingServer('kali', {
      command: '',
      containerConfig: { image: 'kali:latest', runtime: 'auto' },
    });
    try { await mgr.ensureConnected('kali'); } catch {}

    assert.ok(calls.avail.length >= 1);
    assert.equal(calls.pull.length, 0, 'must not pull an already-present image');
    mgr.disconnectAll();
  });

  it('skips the availability check and pull when no runtime is resolved', async () => {
    const { mgr, calls } = makeMgr({ runtime: null, available: false });
    mgr.addPendingServer('kali', {
      command: '',
      containerConfig: { image: 'kali:latest', runtime: 'auto' },
    });
    try { await mgr.ensureConnected('kali'); } catch {}

    assert.ok(calls.resolve.length >= 1);
    assert.equal(calls.avail.length, 0, 'runtime falsy must short-circuit before availability check');
    assert.equal(calls.pull.length, 0);
    mgr.disconnectAll();
  });

  it('never touches the runtime hooks for a non-container server', async () => {
    const { mgr, calls } = makeMgr({ runtime: '_fake_rt_di_', available: false });
    mgr.addPendingServer('plain', { command: '_nonexistent_cmd_autopull_test_' });
    try { await mgr.ensureConnected('plain'); } catch {}

    assert.equal(calls.resolve.length, 0);
    assert.equal(calls.avail.length, 0);
    assert.equal(calls.pull.length, 0);
    assert.equal(mgr.getServerState('plain'), 'error');
    mgr.disconnectAll();
  });

  it('surfaces a pull failure as an error state carrying the pull message', async () => {
    const { mgr } = makeMgr({
      runtime: '_fake_rt_di_',
      available: false,
      pullError: '_fake_rt_di_ pull failed (exit 1): image not found',
    });
    const states: string[] = [];
    mgr.on('server-state-changed', (_n: string, s: string) => states.push(s));
    mgr.addPendingServer('kali', {
      command: '',
      containerConfig: { image: 'kali:latest', runtime: 'auto' },
    });

    await assert.rejects(
      () => mgr.ensureConnected('kali'),
      (err: Error) => err.message.includes('pull failed') && err.message.includes('image not found'),
    );
    assert.ok(states.includes('pending'));
    assert.ok(states.includes('connecting'));
    assert.ok(states.includes('error'));
    assert.equal(mgr.getServerState('kali'), 'error');
    const kali = mgr.listAllServers().find(s => s.name === 'kali');
    assert.ok(kali?.error, 'error message should be set');
    mgr.disconnectAll();
  });

  it('preserves the failed container server in the registry for retry', async () => {
    const { mgr } = makeMgr({
      runtime: '_fake_rt_di_',
      available: false,
      pullError: 'pull failed (exit 1)',
    });
    mgr.addPendingServer('kali', {
      command: '',
      containerConfig: { image: 'kali:latest', runtime: 'auto' },
    });
    try { await mgr.ensureConnected('kali'); } catch {}

    assert.ok(mgr.isPending('kali') || mgr.getServerState('kali') === 'error');
    const servers = mgr.listAllServers();
    assert.equal(servers.length, 1);
    assert.equal(servers[0].name, 'kali');
    mgr.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// containerBacked status reporting (real manager, no hooks needed)
// ---------------------------------------------------------------------------

describe('container-backed status reporting', () => {
  it('container-backed server shows containerBacked=true in status', () => {
    const mgr = new McpManager();
    mgr.addPendingServer('kali', {
      command: '',
      containerConfig: { image: 'docker.io/cyberillo/kali-mcp-server:latest', runtime: 'auto' },
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
// connect tool triggers auto-pull via ensureConnected (mock mcpManager)
// ---------------------------------------------------------------------------

describe('connect tool triggers auto-pull via ensureConnected', () => {
  it('connect tool calls ensureConnected which handles pull internally', async () => {
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
    } as any);

    assert.equal(ensureCalled, true);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Connected'));
    assert.ok(result.output.includes('2 tools'));
  });

  it('connect tool reports pull failure from ensureConnected', async () => {
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
    } as any);

    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Failed to connect'));
    assert.ok(result.output.includes('docker pull failed'));
  });
});

// ---------------------------------------------------------------------------
// MCP tool auto-connect with pull (mock mcpManager)
// ---------------------------------------------------------------------------

describe('MCP tool auto-connect triggers pull via ensureConnected', () => {
  it('auto-connect → ensureConnected → pull → callTool flow', async () => {
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
    } as any);

    assert.deepEqual(callOrder, ['isPending', 'ensureConnected', 'callTool']);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('scan done'));
  });

  it('auto-connect pull failure returns error without calling tool', async () => {
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
    } as any);

    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Failed to start MCP server'));
    assert.ok(result.output.includes('podman pull failed'));
  });
});
