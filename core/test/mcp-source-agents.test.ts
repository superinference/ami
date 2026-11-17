/**
 * Behavior tests for MCP server source-agent provenance (U018 parity:
 * `AgentMcpServerInfo.sourceAgents`, MCPAgentServerMenu.tsx:L139-142 /
 * MCPListPanel.tsx agent grouping).
 *
 * Covers the reverse mapping of which skill/stance (agent) declared a given MCP
 * server: McpManager.addServerSourceAgent/getServerSourceAgents, the
 * `sourceAgents` field surfaced on McpServerStatus, teardown cleanup, and the
 * pure `groupMcpServersBySourceAgent` grouping helper.
 *
 * ESM-SAFE: no spawned processes, no source-text reads, no monkeypatching — only
 * the public McpManager API (pending servers never connect) and the exported
 * helper.
 */

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  McpManager,
  groupMcpServersBySourceAgent,
  type McpServerStatus,
} from '../src/mcp/manager';

function serverConfig(): { command: string } {
  return { command: 'node -e "process.exit(0)"' };
}

describe('McpManager source-agent provenance', () => {
  let mgr: McpManager;
  beforeEach(() => { mgr = new McpManager(); });

  it('records and returns de-duplicated, sorted source agents', () => {
    mgr.addPendingServer('github', serverConfig());
    assert.equal(mgr.getServerSourceAgents('github'), undefined);

    mgr.addServerSourceAgent('github', 'reviewer');
    mgr.addServerSourceAgent('github', 'architect');
    mgr.addServerSourceAgent('github', 'reviewer'); // duplicate — idempotent

    assert.deepEqual(mgr.getServerSourceAgents('github'), ['architect', 'reviewer']);
  });

  it('ignores a blank/whitespace agent name', () => {
    mgr.addPendingServer('srv', serverConfig());
    mgr.addServerSourceAgent('srv', '   ');
    mgr.addServerSourceAgent('srv', '');
    assert.equal(mgr.getServerSourceAgents('srv'), undefined);
  });

  it('trims surrounding whitespace on a recorded agent name', () => {
    mgr.addPendingServer('srv', serverConfig());
    mgr.addServerSourceAgent('srv', '  pentest  ');
    assert.deepEqual(mgr.getServerSourceAgents('srv'), ['pentest']);
  });

  it('surfaces sourceAgents on the server status (pending server, listAllServers)', () => {
    mgr.addPendingServer('kali', serverConfig());
    mgr.addServerSourceAgent('kali', 'pentest');
    const status = mgr.listAllServers().find((s) => s.name === 'kali');
    assert.ok(status);
    assert.deepEqual(status.sourceAgents, ['pentest']);
  });

  it('omits sourceAgents from status when no stance declared the server', () => {
    mgr.addPendingServer('plain', serverConfig());
    const status = mgr.listAllServers().find((s) => s.name === 'plain');
    assert.ok(status);
    assert.ok(!('sourceAgents' in status) || status.sourceAgents === undefined);
  });

  it('drops the provenance entry when the server is removed', () => {
    mgr.addPendingServer('gone', serverConfig());
    mgr.addServerSourceAgent('gone', 'stance');
    mgr.removeServer('gone');
    assert.equal(mgr.getServerSourceAgents('gone'), undefined);
  });
});

describe('groupMcpServersBySourceAgent', () => {
  const mk = (name: string, sourceAgents?: string[]): McpServerStatus => ({
    name,
    state: 'ready',
    serverInfo: null,
    toolCount: 0,
    resourceCount: 0,
    uptime: null,
    ...(sourceAgents ? { sourceAgents } : {}),
  });

  it('groups servers under every declaring agent, sorted and de-duplicated', () => {
    const groups = groupMcpServersBySourceAgent([
      mk('zeta', ['reviewer']),
      mk('alpha', ['reviewer', 'architect']),
      mk('beta', ['architect']),
      mk('orphan'),
    ]);
    assert.deepEqual(groups, [
      { agent: 'architect', servers: [mk('alpha', ['reviewer', 'architect']), mk('beta', ['architect'])] },
      { agent: 'reviewer', servers: [mk('alpha', ['reviewer', 'architect']), mk('zeta', ['reviewer'])] },
    ]);
  });

  it('returns an empty array when no server carries source agents', () => {
    assert.deepEqual(groupMcpServersBySourceAgent([mk('a'), mk('b')]), []);
  });
});
