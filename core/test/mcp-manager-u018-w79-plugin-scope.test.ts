// Unit U018 (wave w79) — core/core/src/mcp/manager.ts
//
// Gap: per-plugin MCP server name scoping to avoid cross-plugin collisions
//   (the reference-parity `addPluginScopeToServers`, mcpPluginIntegration.ts:L341-360).
//   Two plugins can both ship a server named e.g. `github`; scoping renames each to
//   `plugin:<pluginName>:<server>` + tags `pluginSource`, and the companion
//   `remapMcpAutoAllowPatterns` carries the plugin's auto-allow globs onto the
//   scoped tool names.
//
// Pure functions — no spawn, no fs, no network. Run from core/core/:
//   node --import tsx --test test/mcp-manager-u018-w79-plugin-scope.test.ts
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  scopePluginMcpServers,
  remapMcpAutoAllowPatterns,
  type McpServerConfig,
} from '../src/mcp/manager';

after(() => { setTimeout(() => process.exit(0), 100); });

describe('scopePluginMcpServers (U018 w79 — per-plugin name scoping)', () => {
  it('renames each server to plugin:<name>:<server> and tags pluginSource', () => {
    const servers: Record<string, McpServerConfig> = {
      github: { command: 'gh-mcp', env: { TOKEN: 'x' } },
      jira: { command: '', url: 'https://jira.example/mcp', transport: 'http' },
    };
    const { scoped, nameMap } = scopePluginMcpServers(servers, 'acme', 'acme@marketplace');

    assert.deepEqual(Object.keys(scoped).sort(), ['plugin:acme:github', 'plugin:acme:jira']);
    assert.equal(scoped['plugin:acme:github'].command, 'gh-mcp');
    assert.deepEqual(scoped['plugin:acme:github'].env, { TOKEN: 'x' });
    assert.equal(scoped['plugin:acme:github'].pluginSource, 'acme@marketplace');
    assert.equal(scoped['plugin:acme:jira'].pluginSource, 'acme@marketplace');
    assert.deepEqual(nameMap, { github: 'plugin:acme:github', jira: 'plugin:acme:jira' });
  });

  it('does NOT mutate the input configs (shallow clone)', () => {
    const original: McpServerConfig = { command: 'x' };
    const servers = { s: original };
    const { scoped } = scopePluginMcpServers(servers, 'p', 'src');
    assert.equal(original.pluginSource, undefined, 'the caller-owned config is untouched');
    assert.notEqual(scoped['plugin:p:s'], original, 'the scoped entry is a fresh object');
  });

  it('two plugins that both ship `github` scope to distinct names (no collision)', () => {
    const a = scopePluginMcpServers({ github: { command: 'a' } }, 'alpha', 'alpha');
    const b = scopePluginMcpServers({ github: { command: 'b' } }, 'beta', 'beta');
    const merged = { ...a.scoped, ...b.scoped };
    assert.equal(Object.keys(merged).length, 2, 'both survive — neither clobbers the other');
    assert.equal(merged['plugin:alpha:github'].command, 'a');
    assert.equal(merged['plugin:beta:github'].command, 'b');
  });

  it('empty input yields empty output', () => {
    const { scoped, nameMap } = scopePluginMcpServers({}, 'p', 's');
    assert.deepEqual(scoped, {});
    assert.deepEqual(nameMap, {});
  });
});

describe('remapMcpAutoAllowPatterns (U018 w79 — auto-allow follows the scoped name)', () => {
  const nameMap = { github: 'plugin:acme:github' };

  it('rewrites a server-qualified tool pattern onto the normalized scoped prefix', () => {
    // plugin:acme:github normalizes to plugin_acme_github (colons → underscores).
    assert.deepEqual(
      remapMcpAutoAllowPatterns(['mcp__github__create_issue'], nameMap),
      ['mcp__plugin_acme_github__create_issue'],
    );
  });

  it('rewrites a wildcard tool pattern', () => {
    assert.deepEqual(
      remapMcpAutoAllowPatterns(['mcp__github__*'], nameMap),
      ['mcp__plugin_acme_github__*'],
    );
  });

  it('rewrites a bare server-granularity pattern (exact and star)', () => {
    assert.deepEqual(remapMcpAutoAllowPatterns(['mcp__github'], nameMap), ['mcp__plugin_acme_github']);
    assert.deepEqual(remapMcpAutoAllowPatterns(['mcp__github*'], nameMap), ['mcp__plugin_acme_github*']);
  });

  it('leaves a pattern for an unrelated server untouched', () => {
    assert.deepEqual(
      remapMcpAutoAllowPatterns(['mcp__other__tool', 'bash'], nameMap),
      ['mcp__other__tool', 'bash'],
    );
  });

  it('does not mis-match a server whose name is a prefix of another (boundary)', () => {
    // `git` must NOT be rewritten by a `github` entry.
    assert.deepEqual(
      remapMcpAutoAllowPatterns(['mcp__git__status'], { github: 'plugin:acme:github' }),
      ['mcp__git__status'],
    );
  });
});
