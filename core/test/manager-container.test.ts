import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpManager, type McpClientFactory } from '../src/mcp/manager';
import { buildContainerArgs } from '../src/mcp/container';

after(() => { setTimeout(() => process.exit(0), 200); });

// ---------------------------------------------------------------------------
// Test seam: a manager whose client factory records the options the manager
// resolves (command, args, timeouts) and whose runtime detection is forced, so
// the container-wiring BEHAVIOR can be observed deterministically without a real
// docker/podman on the host. The factory delegates to the real default so the
// manager still gets a genuine McpClient (its event wiring stays exercised).
// ---------------------------------------------------------------------------

type ClientOptions = Parameters<McpClientFactory>[0];

interface Instrumented {
  manager: McpManager;
  /** Options passed to the client factory, in call order. */
  clientOptions: ClientOptions[];
  /** Runtime preferences passed to resolveRuntime, in call order. */
  runtimeRequests: Array<string | undefined>;
}

function instrument(runtime: string | null): Instrumented {
  const manager = new McpManager();
  const clientOptions: ClientOptions[] = [];
  const runtimeRequests: Array<string | undefined> = [];

  const realFactory = manager._createClient;
  manager._createClient = (opts) => {
    clientOptions.push(opts);
    return realFactory(opts);
  };
  manager._resolveRuntime = (pref) => {
    runtimeRequests.push(pref);
    return runtime;
  };

  return { manager, clientOptions, runtimeRequests };
}

// ---------------------------------------------------------------------------
// containerConfig field — accepted and optional
// ---------------------------------------------------------------------------

describe('McpServerConfig — containerConfig field', () => {
  it('accepts a server configured with a containerConfig', () => {
    const { manager } = instrument('podman');
    manager.addServer('kali', {
      command: '',
      containerConfig: { image: 'cyberillo/kali-mcp-server:latest', name: 'si-kali-mcp', runtime: 'auto' },
    });
    assert.ok(manager.listServers().some(s => s.name === 'kali'));
    manager.disconnectAll();
  });

  it('containerConfig is optional — a plain command server still registers', () => {
    const { manager, runtimeRequests } = instrument('podman');
    manager.addServer('echo', { command: 'echo', args: ['test'] });
    assert.ok(manager.listServers().some(s => s.name === 'echo'));
    // No containerConfig => runtime detection is never consulted.
    assert.deepEqual(runtimeRequests, []);
    manager.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// Container wiring: resolveRuntime + buildContainerArgs feed the client
// ---------------------------------------------------------------------------

describe('addServer with containerConfig', () => {
  it('resolves the runtime using containerConfig.runtime as the preference', () => {
    const { manager, runtimeRequests } = instrument('podman');
    manager.addServer('kali', { command: '', containerConfig: { image: 'img', runtime: 'docker' } });
    assert.deepEqual(runtimeRequests, ['docker']);
    manager.disconnectAll();
  });

  it('sets the resolved command to the detected runtime', () => {
    const { manager, clientOptions } = instrument('podman');
    manager.addServer('kali', { command: '', containerConfig: { image: 'img', runtime: 'auto' } });
    assert.equal(clientOptions[0].command, 'podman');
    manager.disconnectAll();
  });

  it('sets the resolved args to buildContainerArgs(containerConfig)', () => {
    const { manager, clientOptions } = instrument('docker');
    const containerConfig = { image: 'cyberillo/kali-mcp-server:latest', name: 'si-kali-mcp', runtime: 'auto' as const };
    manager.addServer('kali', { command: '', containerConfig });
    assert.deepEqual(clientOptions[0].args, buildContainerArgs(containerConfig));
    // Sanity: buildContainerArgs produces a `run -i --rm` invocation naming the image.
    assert.deepEqual(clientOptions[0].args, [
      'run', '-i', '--rm', '--name', 'si-kali-mcp', 'cyberillo/kali-mcp-server:latest',
    ]);
    manager.disconnectAll();
  });

  it('falls back to the plain command when no runtime is found', () => {
    const { manager, clientOptions } = instrument(null);
    manager.addServer('kali', { command: '', args: ['fallback'], containerConfig: { image: 'img' } });
    // No runtime => command stays the (empty) config command, args untouched.
    assert.equal(clientOptions[0].command, '');
    assert.deepEqual(clientOptions[0].args, ['fallback']);
    manager.disconnectAll();
  });

  it('uses a higher default requestTimeout (60s) for container servers', () => {
    const { manager, clientOptions } = instrument('podman');
    manager.addServer('kali', { command: '', containerConfig: { image: 'img' } });
    assert.equal(clientOptions[0].requestTimeout, 60000);
    manager.disconnectAll();
  });

  it('uses a higher default connectTimeout (120s) for container servers', () => {
    const { manager, clientOptions } = instrument('podman');
    manager.addServer('kali', { command: '', containerConfig: { image: 'img' } });
    assert.equal(clientOptions[0].connectTimeout, 120000);
    manager.disconnectAll();
  });

  it('uses the standard default timeouts (30s/10s) for non-container servers', () => {
    const { manager, clientOptions } = instrument('podman');
    manager.addServer('echo', { command: 'echo' });
    assert.equal(clientOptions[0].requestTimeout, 30000);
    assert.equal(clientOptions[0].connectTimeout, 10000);
    manager.disconnectAll();
  });

  it('honours explicit timeouts over the container defaults', () => {
    const { manager, clientOptions } = instrument('podman');
    manager.addServer('kali', {
      command: '',
      requestTimeout: 5000,
      connectTimeout: 7000,
      containerConfig: { image: 'img' },
    });
    assert.equal(clientOptions[0].requestTimeout, 5000);
    assert.equal(clientOptions[0].connectTimeout, 7000);
    manager.disconnectAll();
  });

  it('launches the runtime directly — no bridge-script indirection', () => {
    const { manager, clientOptions } = instrument('podman');
    manager.addServer('kali', { command: '', containerConfig: { image: 'img' } });
    // The resolved command is the container runtime itself, not node/process.execPath
    // running a generated bridge script.
    assert.equal(clientOptions[0].command, 'podman');
    assert.notEqual(clientOptions[0].command, process.execPath);
    manager.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// Cleanup — no bridge script machinery exists on the manager
// ---------------------------------------------------------------------------

describe('cleanup — no bridge script management', () => {
  it('exposes no cleanupBridgeScript method', () => {
    const manager = new McpManager();
    assert.equal((manager as unknown as Record<string, unknown>).cleanupBridgeScript, undefined);
  });

  it('removeServer tears a container server down without errors', () => {
    const { manager } = instrument('podman');
    manager.addServer('kali', { command: '', containerConfig: { image: 'img' } });
    assert.doesNotThrow(() => manager.removeServer('kali'));
    assert.equal(manager.listServers().length, 0);
  });

  it('disconnectAll disconnects container-backed clients without errors', () => {
    const { manager, clientOptions } = instrument('podman');
    manager.addServer('kali', { command: '', containerConfig: { image: 'img' } });
    assert.equal(clientOptions.length, 1);
    assert.doesNotThrow(() => manager.disconnectAll());
  });
});

// ---------------------------------------------------------------------------
// Integration: register container-backed servers through the real client path
// ---------------------------------------------------------------------------

describe('McpManager container server registration', () => {
  it('registers a container-backed server via addServer', () => {
    const manager = new McpManager();
    manager.addServer('kali', {
      command: '',
      containerConfig: { image: 'cyberillo/kali-mcp-server:latest', name: 'si-kali-mcp', runtime: 'auto' },
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
      containerConfig: { image: 'cyberillo/kali-mcp-server:latest', name: 'si-kali-mcp', runtime: 'auto' },
    });
    const servers = manager.listServers();
    assert.equal(servers.length, 2);
    assert.ok(servers.some(s => s.name === 'echo'));
    assert.ok(servers.some(s => s.name === 'kali'));
    manager.disconnectAll();
  });

  it('throws on duplicate container server registration', () => {
    const manager = new McpManager();
    manager.addServer('kali', { command: '', containerConfig: { image: 'test-image' } });
    assert.throws(() => {
      manager.addServer('kali', { command: '', containerConfig: { image: 'test-image-2' } });
    }, /already registered/);
    manager.disconnectAll();
  });

  it('removeServer cleans up container server', () => {
    const manager = new McpManager();
    manager.addServer('kali', { command: '', containerConfig: { image: 'test-image' } });
    manager.removeServer('kali');
    const servers = manager.listServers();
    assert.equal(servers.length, 0);
  });
});
