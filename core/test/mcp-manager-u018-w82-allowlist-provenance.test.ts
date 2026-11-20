import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpManager } from '../src/mcp/manager';

// ---------------------------------------------------------------------------
// U018 (w82) — provenance-scoped MCP allowlist (Claude-parity channelAllowlist:
// services/mcp/channelAllowlist.ts). Claude's allowlist keys approval to a
// server's PROVENANCE ({marketplace, plugin}) rather than its arbitrary local
// name, so "only servers from this plugin are trusted" is expressible. AMI's
// provenance tag is `McpServerConfig.pluginSource` (stamped by
// scopePluginMcpServers). The allowlist now accepts provenance entries —
// `{ pluginSource }` (AMI-native) and the reference-shaped `{ marketplace, plugin }`
// (composed to the `<plugin>@<marketplace>` pluginSource convention) — alongside
// the existing name / command / URL entries, enforced through the SAME
// enforceAllowlist path addServer already runs.
// ---------------------------------------------------------------------------

describe('McpManager allowlist — provenance-scoped approval', () => {
  it('a { pluginSource } entry admits a server tagged with that provenance and rejects others', () => {
    const mgr = new McpManager();
    mgr.setAllowedServers([{ pluginSource: 'acme-bundle' }]);

    // Same name, different provenance: provenance — not name — decides.
    assert.doesNotThrow(() =>
      mgr.addServer('plugin:acme:github', { command: 'gh-mcp', pluginSource: 'acme-bundle' }),
    );
    assert.throws(
      () => mgr.addServer('plugin:evil:github', { command: 'gh-mcp', pluginSource: 'evil-bundle' }),
      /not in the allowed servers list/,
    );
    // A server with NO provenance is not covered by a provenance-only allowlist.
    assert.throws(
      () => mgr.addServer('bare', { command: 'x' }),
      /not in the allowed servers list/,
    );
  });

  it('isServerAllowed reports provenance membership without mutating state', () => {
    const mgr = new McpManager();
    mgr.setAllowedServers([{ pluginSource: 'acme-bundle' }]);
    assert.equal(mgr.isServerAllowed('anything', { pluginSource: 'acme-bundle' }), true);
    assert.equal(mgr.isServerAllowed('anything', { pluginSource: 'other' }), false);
    assert.equal(mgr.isServerAllowed('anything'), false);
  });

  it('a { pluginSource } wildcard matches a family of provenances', () => {
    const mgr = new McpManager();
    mgr.setAllowedServers([{ pluginSource: 'acme-*' }]);
    assert.equal(mgr.isServerAllowed('s', { pluginSource: 'acme-github' }), true);
    assert.equal(mgr.isServerAllowed('s', { pluginSource: 'acme-jira' }), true);
    assert.equal(mgr.isServerAllowed('s', { pluginSource: 'beta-github' }), false);
  });

  it('a { marketplace, plugin } entry matches the <plugin>@<marketplace> provenance convention', () => {
    const mgr = new McpManager();
    mgr.setAllowedServers([{ marketplace: 'official', plugin: 'acme' }]);
    // scopePluginMcpServers tags pluginSource as `<plugin>@<marketplace>`.
    assert.equal(mgr.isServerAllowed('s', { pluginSource: 'acme@official' }), true);
    // Also matches the marketplace-agnostic bare plugin name.
    assert.equal(mgr.isServerAllowed('s', { pluginSource: 'acme' }), true);
    // A different marketplace for the same plugin is NOT admitted.
    assert.equal(mgr.isServerAllowed('s', { pluginSource: 'acme@thirdparty' }), false);
    assert.equal(mgr.isServerAllowed('s', { pluginSource: 'other@official' }), false);
  });

  it('provenance entries compose with the existing name/url/command entries (unioned)', () => {
    const mgr = new McpManager();
    mgr.setAllowedServers(['by-name', { pluginSource: 'trusted' }]);
    // name entry still works
    assert.doesNotThrow(() => mgr.addServer('by-name', { command: 'x' }));
    // provenance entry works
    assert.doesNotThrow(() => mgr.addServer('whatever', { command: 'y', pluginSource: 'trusted' }));
    // neither → rejected
    assert.throws(() => mgr.addServer('nope', { command: 'z' }), /not in the allowed servers list/);
  });

  it('the denylist also understands provenance (deny by pluginSource regardless of name)', () => {
    const mgr = new McpManager();
    mgr.setDeniedServerList([{ pluginSource: 'banned' }]);
    assert.equal(mgr.isServerDenied('any-name', { pluginSource: 'banned' }), true);
    assert.equal(mgr.isServerDenied('any-name', { pluginSource: 'ok' }), false);
    assert.throws(
      () => mgr.addServer('x', { command: 'c', pluginSource: 'banned' }),
      /blocked by the denied-servers list/,
    );
  });
});
