import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  McpManager,
  findMcpConfigPaths,
  findMcpConfigScopes,
  mcpScopeLabel,
  MCP_SCOPE_ORDER,
  type McpServerScope,
} from '../src/mcp/manager';

// ---------------------------------------------------------------------------
// MCP config-source SCOPE model (Claude-parity MCPListPanel ConfigScope):
// each live server carries the scope (enterprise|user|project|local|dynamic)
// and the config file it was loaded from, and the listing groups + orders by
// scope with alphabetical ordering within each group.
// ---------------------------------------------------------------------------

const dirs: string[] = [];
function tmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-scope-'));
  dirs.push(d);
  return d;
}
afterEach(() => {
  for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
});

describe('MCP scope — labels + order', () => {
  it('orders scopes project→local→user→enterprise→dynamic', () => {
    assert.deepEqual([...MCP_SCOPE_ORDER], ['project', 'local', 'user', 'enterprise', 'dynamic']);
  });
  it('gives each scope a Claude-parity heading label', () => {
    assert.equal(mcpScopeLabel('project'), 'Project MCPs');
    assert.equal(mcpScopeLabel('local'), 'Local MCPs');
    assert.equal(mcpScopeLabel('user'), 'User MCPs');
    assert.equal(mcpScopeLabel('enterprise'), 'Enterprise MCPs');
    assert.equal(mcpScopeLabel('dynamic'), 'Built-in MCPs');
  });
});

describe('findMcpConfigScopes', () => {
  it('tags .ami/mcp.json and .mcp.json under cwd as project, ~/.ami/mcp.json as user', () => {
    const home = tmp();
    const cwd = fs.mkdtempSync(path.join(home, 'proj-'));
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(cwd, '.ami', 'mcp.json'), '{"mcpServers":{}}');
    fs.writeFileSync(path.join(cwd, '.mcp.json'), '{"mcpServers":{}}');
    fs.mkdirSync(path.join(home, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(home, '.ami', 'mcp.json'), '{"mcpServers":{}}');

    const prevHome = process.env.HOME;
    process.env.HOME = home;
    try {
      const scoped = findMcpConfigScopes(cwd);
      const byPath = new Map(scoped.map((e) => [e.path, e.scope] as const));
      assert.equal(byPath.get(path.join(cwd, '.ami', 'mcp.json')), 'project');
      assert.equal(byPath.get(path.join(cwd, '.mcp.json')), 'project');
      assert.equal(byPath.get(path.join(home, '.ami', 'mcp.json')), 'user');
      // findMcpConfigPaths stays the paths-only projection of the scoped list.
      assert.deepEqual(findMcpConfigPaths(cwd), scoped.map((e) => e.path));
    } finally {
      if (prevHome === undefined) delete process.env.HOME; else process.env.HOME = prevHome;
    }
  });
});

describe('McpManager scope tracking', () => {
  it('records the scope + source path a server was loaded from', () => {
    const d = tmp();
    const cfg = path.join(d, 'mcp.json');
    fs.writeFileSync(cfg, JSON.stringify({ mcpServers: { srv: { command: 'run' } } }));
    const mgr = new McpManager();
    mgr.loadFromConfig(cfg, { scope: 'user' });
    assert.equal(mgr.getServerScope('srv'), 'user');
    const status = mgr.getServerStatus('srv');
    assert.equal(status?.scope, 'user');
    assert.equal(status?.sourcePath, cfg);
  });

  it('defaults a programmatically-added server to the dynamic (built-in) scope', () => {
    const mgr = new McpManager();
    mgr.addServer('builtin', { command: 'run' });
    assert.equal(mgr.getServerScope('builtin'), 'dynamic');
    assert.equal(mgr.getServerStatus('builtin')?.scope, 'dynamic');
  });

  it('derives project scope from the projectScoped flag when no explicit scope', () => {
    const d = tmp();
    const cfg = path.join(d, 'mcp.json');
    fs.writeFileSync(cfg, JSON.stringify({ mcpServers: { psrv: { command: 'run' } } }));
    const mgr = new McpManager();
    mgr.loadFromConfig(cfg, { projectScoped: true });
    assert.equal(mgr.getServerScope('psrv'), 'project');
    // A pending (needs-approval) project server still reports its scope.
    const listed = mgr.listAllServers().find((s) => s.name === 'psrv');
    assert.equal(listed?.scope, 'project');
  });

  it('loads managed servers as the enterprise scope', () => {
    const mgr = new McpManager();
    mgr.loadManagedServers({ corp: { command: 'corp-mcp' } });
    assert.equal(mgr.getServerScope('corp'), 'enterprise');
    assert.equal(mgr.getServerStatus('corp')?.scope, 'enterprise');
  });

  it('clears scope + source tracking on removeServer and disconnectAll', () => {
    const mgr = new McpManager();
    mgr.addServer('a', { command: 'run' });
    mgr.removeServer('a');
    assert.equal(mgr.getServerScope('a'), undefined);
    mgr.addServer('b', { command: 'run' });
    mgr.disconnectAll();
    assert.equal(mgr.getServerScope('b'), undefined);
  });
});

describe('listServersByScope grouping', () => {
  it('groups by scope in MCP_SCOPE_ORDER, alphabetical within each group', () => {
    const mgr = new McpManager();
    const d = tmp();
    const userCfg = path.join(d, 'user.json');
    const projCfg = path.join(d, 'proj.json');
    fs.writeFileSync(userCfg, JSON.stringify({ mcpServers: { zed: { command: 'r' }, alpha: { command: 'r' } } }));
    fs.writeFileSync(projCfg, JSON.stringify({ mcpServers: { beta: { command: 'r' } } }));
    mgr.loadFromConfig(userCfg, { scope: 'user' });
    mgr.loadFromConfig(projCfg, { scope: 'project' });
    mgr.loadManagedServers({ corp: { command: 'r' } }, { sourcePath: '/etc/managed.json' });

    const groups = mgr.listServersByScope();
    assert.deepEqual(groups.map((g) => g.scope), ['project', 'user', 'enterprise']);
    const proj = groups.find((g) => g.scope === 'project')!;
    assert.equal(proj.label, 'Project MCPs');
    assert.equal(proj.sourcePath, projCfg);
    assert.deepEqual(proj.servers.map((s) => s.name), ['beta']);
    const user = groups.find((g) => g.scope === 'user')!;
    assert.deepEqual(user.servers.map((s) => s.name), ['alpha', 'zed']); // alphabetical
    const ent = groups.find((g) => g.scope === 'enterprise')!;
    assert.equal(ent.sourcePath, '/etc/managed.json');
    assert.deepEqual(ent.servers.map((s) => s.name), ['corp']);
  });

  it('returns no empty groups when a scope has no servers', () => {
    const mgr = new McpManager();
    mgr.addServer('only', { command: 'r' });
    const groups = mgr.listServersByScope();
    assert.deepEqual(groups.map((g) => g.scope), ['dynamic']);
  });
});

// Type-only reference so the exported alias is exercised by the suite too.
const _scope: McpServerScope = 'project';
void _scope;
