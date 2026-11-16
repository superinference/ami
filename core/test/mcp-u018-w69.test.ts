import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  McpManager,
  resolveMcpServerNameCollision,
  resolveDesktopMcpConfigPath,
  readDesktopMcpServers,
  type DesktopImportEnv,
  MCP_BYPASS_PERMISSION_ENV,
  MCP_BYPASS_PERMISSION_VALUE,
} from '../src/mcp/manager';

// ---------------------------------------------------------------------------
// U018-w69 — additive MCP manager parity (Claude Code):
//   * desktop-config import (readClaudeDesktopMcpServers + WSL path resolution)
//   * numbered-suffix server-name collision resolution
//   * bypass/auto-allow permission env propagation into stdio MCP servers
//   * persisted "ever connected" set to gate startup connectivity notifications
//   * original-vs-normalized MCP tool-name introspection mapping
// ---------------------------------------------------------------------------

/** A minimal fake MCP client the factory seam returns, so connect/ready/tools
 *  behavior is driveable without spawning a real server. */
class FakeClient extends EventEmitter {
  state = 'idle';
  serverInfo: unknown = { name: 'fake', version: '1' };
  instructions: string | null = null;
  uptime: number | null = 1;
  tools: Array<{ name: string; description?: string }> = [];
  resources: unknown[] = [];
  prompts: unknown[] = [];
  onElicitation: unknown;
  onSampling: unknown;
  readonly env: NodeJS.ProcessEnv | undefined;
  constructor(opts?: { env?: NodeJS.ProcessEnv }) {
    super();
    this.env = opts?.env;
  }
  async connect(): Promise<void> { this.state = 'ready'; }
  async connectHTTP(): Promise<void> { this.state = 'ready'; }
  async connectSSE(): Promise<void> { this.state = 'ready'; }
  async listTools(): Promise<Array<{ name: string; description?: string }>> { return this.tools; }
  async listResources(): Promise<unknown[]> { return this.resources; }
  async listPrompts(): Promise<unknown[]> { return this.prompts; }
  async callTool(): Promise<unknown> { return { ok: true }; }
  async reconnect(): Promise<void> {}
  disconnect(): void { this.state = 'stopped'; }
  setHeaders(): void {}
  async revokeAuth(): Promise<void> {}
}

// ===========================================================================
// resolveMcpServerNameCollision
// ===========================================================================
describe('resolveMcpServerNameCollision', () => {
  it('returns the base name when its normalized form is unused', () => {
    const used = new Set<string>();
    const r = resolveMcpServerNameCollision('github', used);
    assert.equal(r.name, 'github');
    assert.equal(r.normalized, 'github');
  });

  it('appends a numbered (2) suffix on a normalized collision', () => {
    const used = new Set<string>(['github']);
    const r = resolveMcpServerNameCollision('github', used);
    assert.equal(r.name, 'github (2)');
    // normalization replaces the space + parens, collapsing to single underscores.
    assert.equal(r.normalized, 'github_2');
  });

  it('keeps incrementing until the FINAL normalized name is unused', () => {
    // Both the base and the (2) form are already taken.
    const used = new Set<string>(['github', 'github_2']);
    const r = resolveMcpServerNameCollision('github', used);
    assert.equal(r.name, 'github (3)');
    assert.equal(r.normalized, 'github_3');
  });

  it('catches the suffixed-name-collides-with-another-base edge case', () => {
    // A distinct display base whose (2) form normalizes to the same thing as an
    // already-used server's base name is still caught (checks final normalized).
    const used = new Set<string>(['srv', 'srv_2']);
    const r = resolveMcpServerNameCollision('srv!', used); // 'srv!' normalizes to 'srv'
    assert.equal(r.name, 'srv! (3)');
  });

  it('does not mutate the used-names set', () => {
    const used = new Set<string>(['github']);
    resolveMcpServerNameCollision('github', used);
    assert.deepEqual([...used], ['github']);
  });
});

// ===========================================================================
// resolveDesktopMcpConfigPath
// ===========================================================================
describe('resolveDesktopMcpConfigPath', () => {
  const base = (over: Partial<DesktopImportEnv>): DesktopImportEnv => ({
    platform: 'linux',
    isWsl: false,
    env: {},
    homedir: '/home/u',
    exists: () => false,
    readdir: () => [],
    ...over,
  });

  it('resolves the macOS Application Support path', () => {
    const p = resolveDesktopMcpConfigPath(base({ platform: 'darwin', homedir: '/Users/me' }));
    assert.equal(p, '/Users/me/Library/Application Support/Claude/claude_desktop_config.json');
  });

  it('resolves the WSL USERPROFILE path when it exists (drive-letter stripped)', () => {
    const p = resolveDesktopMcpConfigPath(base({
      platform: 'linux',
      isWsl: true,
      env: { USERPROFILE: 'C:\\Users\\Alice' },
      exists: (x) => x === '/mnt/c/Users/Alice/AppData/Roaming/Claude/claude_desktop_config.json',
    }));
    assert.equal(p, '/mnt/c/Users/Alice/AppData/Roaming/Claude/claude_desktop_config.json');
  });

  it('falls back to scanning /mnt/c/Users, skipping system profiles', () => {
    const p = resolveDesktopMcpConfigPath(base({
      platform: 'linux',
      isWsl: true,
      env: {}, // no USERPROFILE → straight to the scan
      readdir: (d) => d === '/mnt/c/Users' ? ['Public', 'Default', 'Bob'] : [],
      exists: (x) => x === '/mnt/c/Users/Bob/AppData/Roaming/Claude/claude_desktop_config.json',
    }));
    assert.equal(p, '/mnt/c/Users/Bob/AppData/Roaming/Claude/claude_desktop_config.json');
  });

  it('returns null on WSL when neither USERPROFILE nor a scanned profile has a config', () => {
    const p = resolveDesktopMcpConfigPath(base({
      platform: 'linux',
      isWsl: true,
      env: { USERPROFILE: 'C:\\Users\\Ghost' },
      readdir: () => { throw new Error('no /mnt/c'); }, // readdir throws → treated as empty
    }));
    assert.equal(p, null);
  });

  it('returns null on an unsupported platform (plain linux / win32)', () => {
    assert.equal(resolveDesktopMcpConfigPath(base({ platform: 'linux', isWsl: false })), null);
    assert.equal(resolveDesktopMcpConfigPath(base({ platform: 'win32' })), null);
  });
});

// ===========================================================================
// readDesktopMcpServers
// ===========================================================================
describe('readDesktopMcpServers', () => {
  const macEnv = (home: string, exists: (p: string) => boolean): DesktopImportEnv => ({
    platform: 'darwin', isWsl: false, env: {}, homedir: home, exists, readdir: () => [],
  });

  it('reads and validates stdio servers from a desktop config file', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-desktop-'));
    try {
      const cfgPath = '/Users/me/Library/Application Support/Claude/claude_desktop_config.json';
      const file = path.join(dir, 'cfg.json');
      fs.writeFileSync(file, JSON.stringify({
        mcpServers: {
          good: { command: 'npx', args: ['-y', 'good-mcp'] },
          remoteNoCmd: { url: 'https://x/mcp' }, // no command → not desktop-importable
          malformed: { command: 123 },           // wrong-typed → skipped
        },
      }));
      const servers = readDesktopMcpServers({
        env: macEnv('/Users/me', (p) => p === cfgPath),
        readFile: () => fs.readFileSync(file, 'utf-8'),
      });
      assert.deepEqual(Object.keys(servers), ['good']);
      assert.deepEqual(servers.good, { command: 'npx', args: ['-y', 'good-mcp'] });
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('returns {} on an unsupported platform (no desktop app)', () => {
    const servers = readDesktopMcpServers({
      env: { platform: 'linux', isWsl: false, env: {}, homedir: '/h', exists: () => true, readdir: () => [] },
    });
    assert.deepEqual(servers, {});
  });

  it('returns {} for a missing / unreadable file', () => {
    const servers = readDesktopMcpServers({
      env: macEnv('/Users/me', () => true),
      readFile: () => { throw Object.assign(new Error('nope'), { code: 'ENOENT' }); },
    });
    assert.deepEqual(servers, {});
  });

  it('returns {} for malformed JSON and for a config with no mcpServers block', () => {
    const malformed = readDesktopMcpServers({ env: macEnv('/Users/me', () => true), readFile: () => '{ not json' });
    assert.deepEqual(malformed, {});
    const noBlock = readDesktopMcpServers({ env: macEnv('/Users/me', () => true), readFile: () => JSON.stringify({ other: 1 }) });
    assert.deepEqual(noBlock, {});
  });
});

// ===========================================================================
// McpManager.importDesktopServers (end-to-end wiring)
// ===========================================================================
describe('McpManager.importDesktopServers', () => {
  const macEnv: DesktopImportEnv = {
    platform: 'darwin', isWsl: false, env: {}, homedir: '/Users/me',
    exists: () => true, readdir: () => [],
  };

  it('registers imported stdio servers under the user scope and returns their names', () => {
    const mgr = new McpManager();
    const imported = mgr.importDesktopServers({
      env: macEnv,
      readFile: () => JSON.stringify({ mcpServers: { files: { command: 'file-mcp' }, git: { command: 'git-mcp' } } }),
    });
    assert.deepEqual(imported.sort(), ['files', 'git']);
    assert.equal(mgr.getServerScope('files'), 'user');
    assert.ok(mgr.listServers().some((s) => s.name === 'git'));
    mgr.disconnectAll();
  });

  it('resolves a name collision with an already-registered server via a (2) suffix', () => {
    const mgr = new McpManager();
    mgr.addServer('files', { command: 'existing' });
    const imported = mgr.importDesktopServers({
      env: macEnv,
      readFile: () => JSON.stringify({ mcpServers: { files: { command: 'desktop-file-mcp' } } }),
    });
    assert.deepEqual(imported, ['files (2)']);
    // The original is untouched; the import registered under the suffixed name.
    assert.equal(mgr.getServerConfig('files')?.command, 'existing');
    assert.equal(mgr.getServerConfig('files (2)')?.command, 'desktop-file-mcp');
    mgr.disconnectAll();
  });

  it('is a no-op returning [] when there is nothing to import', () => {
    const mgr = new McpManager();
    const imported = mgr.importDesktopServers({ env: macEnv, readFile: () => JSON.stringify({ mcpServers: {} }) });
    assert.deepEqual(imported, []);
    mgr.disconnectAll();
  });
});

// ===========================================================================
// Bypass / auto-allow permission env propagation into stdio MCP servers
// ===========================================================================
describe('bypass permission env propagation', () => {
  function instrument(): { mgr: McpManager; envs: Array<NodeJS.ProcessEnv | undefined> } {
    const mgr = new McpManager();
    const envs: Array<NodeJS.ProcessEnv | undefined> = [];
    mgr._createClient = (opts) => {
      envs.push(opts.env);
      return new FakeClient({ env: opts.env }) as never;
    };
    return { mgr, envs };
  }

  it('injects the bypass env into a spawned stdio MCP server when enabled', () => {
    const { mgr, envs } = instrument();
    mgr.setBypassPermissions(true);
    assert.equal(mgr.getBypassPermissions(), true);
    mgr.addServer('local', { command: 'node', args: ['s.js'] });
    assert.equal(envs[0]?.[MCP_BYPASS_PERMISSION_ENV], MCP_BYPASS_PERMISSION_VALUE);
    mgr.disconnectAll();
  });

  it('does NOT inject the bypass env into a remote (http/sse) server', () => {
    const { mgr, envs } = instrument();
    mgr.setBypassPermissions(true);
    mgr.addServer('remote', { command: '', transport: 'http', url: 'https://x/mcp' });
    assert.equal(envs[0]?.[MCP_BYPASS_PERMISSION_ENV], undefined);
    mgr.disconnectAll();
  });

  it('does not inject the bypass env when disabled (the default)', () => {
    const { mgr, envs } = instrument();
    mgr.addServer('local', { command: 'node' });
    assert.equal(envs[0]?.[MCP_BYPASS_PERMISSION_ENV], undefined);
    mgr.disconnectAll();
  });
});

// ===========================================================================
// Ever-connected persistence (gates startup connectivity notifications)
// ===========================================================================
describe('ever-connected persistence', () => {
  function instrument(): { mgr: McpManager; client: FakeClient } {
    const mgr = new McpManager();
    const client = new FakeClient();
    mgr._createClient = (() => client) as never;
    return { mgr, client };
  }

  it('marks a server ever-connected on its first ready event and emits once', () => {
    const { mgr, client } = instrument();
    const marked: string[] = [];
    mgr.on('ever-connected', (name: string) => marked.push(name));
    mgr.addServer('srv', { command: 'node' });
    assert.equal(mgr.hasEverConnected('srv'), false);
    client.emit('ready');
    assert.equal(mgr.hasEverConnected('srv'), true);
    // A repeat ready (e.g. after reconnect) does not re-emit.
    client.emit('ready');
    assert.deepEqual(marked, ['srv']);
    mgr.disconnectAll();
  });

  it('seeds the ever-connected set from persistence and reports it back', () => {
    const mgr = new McpManager();
    mgr.setEverConnectedServers(['a', 'b']);
    assert.equal(mgr.hasEverConnected('a'), true);
    assert.equal(mgr.hasEverConnected('c'), false);
    assert.deepEqual(mgr.getEverConnectedServers().sort(), ['a', 'b']);
  });

  it('does NOT clear the ever-connected history on disconnectAll (install-scoped)', () => {
    const { mgr, client } = instrument();
    mgr.addServer('srv', { command: 'node' });
    client.emit('ready');
    mgr.disconnectAll();
    assert.equal(mgr.hasEverConnected('srv'), true);
  });
});

// ===========================================================================
// Original-vs-normalized MCP tool-name introspection mapping
// ===========================================================================
describe('tool-name introspection mapping', () => {
  async function withTool(server: string, toolName: string): Promise<McpManager> {
    const mgr = new McpManager();
    const client = new FakeClient();
    client.tools = [{ name: toolName, description: 'd' }];
    mgr._createClient = (() => client) as never;
    mgr.addServer(server, { command: 'node' });
    await mgr.connectServer(server);
    return mgr;
  }

  it('maps a model-facing normalized name back to the original server-side tool', async () => {
    const mgr = await withTool('my-server', 'search repos');
    const mapping = mgr.getToolNameMapping();
    assert.equal(mapping.length, 1);
    assert.equal(mapping[0].server, 'my-server');
    assert.equal(mapping[0].originalName, 'search repos');
    // hyphen preserved, space → underscore.
    assert.equal(mapping[0].normalizedName, 'mcp__my-server__search_repos');
    mgr.disconnectAll();
  });

  it('reverse-looks-up the original tool name from the normalized name', async () => {
    const mgr = await withTool('srv', 'do thing');
    const found = mgr.getOriginalToolName('mcp__srv__do_thing');
    assert.deepEqual(found, { server: 'srv', originalName: 'do thing' });
    assert.equal(mgr.getOriginalToolName('mcp__srv__unknown'), undefined);
    mgr.disconnectAll();
  });

  it('returns an empty mapping before any tool has been discovered', () => {
    const mgr = new McpManager();
    assert.deepEqual(mgr.getToolNameMapping(), []);
    mgr.disconnectAll();
  });
});
