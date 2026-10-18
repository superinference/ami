/**
 * Tests for MCP allowlist configuration (Gap 9).
 *
 * Verifies that:
 * - McpManager.setAllowedServers() restricts which servers can be added
 * - McpManager.isServerAllowed() returns false for unlisted servers
 * - McpManager.addServer() throws for unlisted servers
 * - With no allowlist set, all servers are allowed
 * - The ProjectConfig type includes allowedMcpServers and managedMcpServers
 * - mergeConfigs handles the new fields and managed config is authoritative
 */

import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpManager } from '../src/mcp/manager';
import { mergeConfigs, type ProjectConfig } from '../src/config';

after(() => { setTimeout(() => process.exit(0), 200); });

// ---------------------------------------------------------------------------
// McpManager allowlist
// ---------------------------------------------------------------------------

describe('McpManager – allowlist', () => {
  it('all servers are allowed when no allowlist is set', () => {
    const mgr = new McpManager();
    assert.equal(mgr.isServerAllowed('any-server'), true);
    assert.equal(mgr.isServerAllowed('another'), true);
  });

  it('setAllowedServers restricts which servers are allowed', () => {
    const mgr = new McpManager();
    mgr.setAllowedServers(['alpha', 'beta']);
    assert.equal(mgr.isServerAllowed('alpha'), true);
    assert.equal(mgr.isServerAllowed('beta'), true);
    assert.equal(mgr.isServerAllowed('gamma'), false);
    assert.equal(mgr.isServerAllowed('unknown'), false);
  });

  it('addServer throws for an unlisted server when allowlist is set', () => {
    const mgr = new McpManager();
    mgr.setAllowedServers(['allowed-server']);
    assert.throws(
      () => mgr.addServer('forbidden-server', { command: 'echo' }),
      (err: Error) => {
        assert.ok(err.message.includes('not in the allowed servers list'));
        return true;
      },
    );
  });

  it('addServer succeeds for a listed server when allowlist is set', () => {
    const mgr = new McpManager();
    mgr.setAllowedServers(['my-server']);
    // Should not throw
    assert.doesNotThrow(() => mgr.addServer('my-server', { command: 'echo hello' }));
    mgr.disconnectAll();
  });

  it('allowlist blocks server addition at addServer time', () => {
    const mgr = new McpManager();
    mgr.setAllowedServers(['keep']);
    mgr.addServer('keep', { command: 'echo keep' });
    assert.throws(
      () => mgr.addServer('remove', { command: 'echo remove' }),
      (err: Error) => err.message.includes('not in the allowed servers list'),
    );

    const statuses = mgr.listAllServers();
    const names = statuses.map(s => s.name);
    assert.ok(names.includes('keep'));
    assert.ok(!names.includes('remove'));

    const denied = mgr.getDeniedServers();
    assert.ok(denied.includes('remove'));
    mgr.disconnectAll();
  });

  it('empty allowlist blocks all servers', () => {
    const mgr = new McpManager();
    mgr.setAllowedServers([]);
    assert.equal(mgr.isServerAllowed('any'), false);
    assert.throws(
      () => mgr.addServer('any', { command: 'echo' }),
      (err: Error) => err.message.includes('not in the allowed servers list'),
    );
  });
});

// ---------------------------------------------------------------------------
// ProjectConfig type — allowedMcpServers / managedMcpServers
// ---------------------------------------------------------------------------

describe('ProjectConfig – MCP allowlist fields', () => {
  it('allowedMcpServers is accepted in ProjectConfig', () => {
    const config: ProjectConfig = {
      allowedMcpServers: ['server-a', 'server-b'],
    };
    assert.deepEqual(config.allowedMcpServers, ['server-a', 'server-b']);
  });

  it('managedMcpServers is accepted in ProjectConfig', () => {
    const config: ProjectConfig = {
      managedMcpServers: {
        'corp-server': { command: 'corp-mcp', args: ['--mode', 'prod'] },
      },
    };
    assert.ok(config.managedMcpServers);
    assert.ok('corp-server' in config.managedMcpServers);
  });
});

// ---------------------------------------------------------------------------
// mergeConfigs — new fields
// ---------------------------------------------------------------------------

describe('mergeConfigs – MCP fields', () => {
  it('merges allowedMcpServers from project config', () => {
    const result = mergeConfigs(
      {},
      {},
      { allowedMcpServers: ['a', 'b'] },
      null,
    );
    assert.deepEqual(result.allowedMcpServers, ['a', 'b']);
  });

  it('merges managedMcpServers from project config', () => {
    const result = mergeConfigs(
      {},
      {},
      { managedMcpServers: { s1: { command: 'echo' } } },
      null,
    );
    assert.deepEqual(result.managedMcpServers, { s1: { command: 'echo' } });
  });

  it('managed config allowedMcpServers overrides project config', () => {
    const managed: ProjectConfig = { allowedMcpServers: ['managed-only'] };
    const result = mergeConfigs(
      { allowedMcpServers: ['cli-override'] },
      {},
      { allowedMcpServers: ['project-servers'] },
      null,
      null,
      managed,
    );
    // Managed is authoritative — cli/project cannot override
    assert.deepEqual(result.allowedMcpServers, ['managed-only']);
  });

  it('managed config managedMcpServers overrides project config', () => {
    const managed: ProjectConfig = {
      managedMcpServers: { enterprise: { command: 'enterprise-mcp' } },
    };
    const result = mergeConfigs(
      {},
      {},
      { managedMcpServers: { local: { command: 'local-mcp' } } },
      null,
      null,
      managed,
    );
    // Managed is authoritative
    assert.deepEqual(result.managedMcpServers, { enterprise: { command: 'enterprise-mcp' } });
  });
});
