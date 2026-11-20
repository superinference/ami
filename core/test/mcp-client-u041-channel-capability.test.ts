/**
 * mcp-client-u041-channel-capability.test.ts — parity for the MCP experimental
 * capability opt-in `claude/channel` + `claude/channel/permission`
 * (the reference-parity channelPermissions.ts:L177-194).
 *
 * A server opts IN to relaying permission prompts over a message channel by
 * declaring BOTH experimental capabilities under `capabilities.experimental` at
 * `initialize`. AMI inspects them via McpClient.hasExperimentalCapability /
 * supportsChannelPermissionRelay, and surfaces the opt-in through
 * McpServerStatus.channelPermissionRelay (the `/mcp` diagnostics surface).
 *
 * ESM-SAFE by construction: real spawned stdio servers (`node -e`), no
 * source-text reads, no namespace monkeypatching. Every assertion checks
 * observable behavior so a regression in either direction fails the test.
 */

import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  McpClient,
  MCP_CHANNEL_CAPABILITY,
  MCP_CHANNEL_PERMISSION_CAPABILITY,
} from '../src/mcp/client';
import { McpManager } from '../src/mcp/manager';

const liveClients: McpClient[] = [];
const liveManagers: McpManager[] = [];

afterEach(() => {
  while (liveClients.length) { try { liveClients.pop()!.disconnect(); } catch { /* ignore */ } }
  while (liveManagers.length) { try { liveManagers.pop()!.disconnectAll(); } catch { /* ignore */ } }
});

// A minimal stdio MCP server that advertises an arbitrary capabilities object
// (JSON-embedded) on `initialize`, so a test can declare experimental caps.
function stdioArgs(capsJson: string): string[] {
  const script = `
    const readline = require('readline');
    const rl = readline.createInterface({ input: process.stdin });
    const caps = ${capsJson};
    rl.on('line', (line) => {
      try {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{
            protocolVersion:'2024-11-05',
            serverInfo:{ name:'chan-server', version:'1.0' },
            capabilities: caps } }) + '\\n');
        } else if (msg.method === 'tools/list') {
          process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{ tools:[] } }) + '\\n');
        } else if (msg.method === 'resources/list') {
          process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{ resources:[] } }) + '\\n');
        } else if (msg.method === 'prompts/list') {
          process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{ prompts:[] } }) + '\\n');
        } else if (msg.method === 'ping') {
          process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{} }) + '\\n');
        }
      } catch {}
    });
    setInterval(() => {}, 60000);
  `;
  return ['-e', script];
}

async function connect(capsJson: string): Promise<McpClient> {
  const client = new McpClient({ command: 'node', args: stdioArgs(capsJson), requestTimeout: 5000, connectTimeout: 5000 });
  client.on('error', () => {});
  liveClients.push(client);
  await client.connect();
  assert.equal(client.state, 'ready');
  return client;
}

describe('McpClient.hasExperimentalCapability', () => {
  it('reports true only for a declared experimental capability', async () => {
    const c = await connect(`{ tools:{}, experimental:{ '${MCP_CHANNEL_CAPABILITY}': {} } }`);
    assert.equal(c.hasExperimentalCapability(MCP_CHANNEL_CAPABILITY), true);
    assert.equal(c.hasExperimentalCapability(MCP_CHANNEL_PERMISSION_CAPABILITY), false);
    // A standard (non-experimental) capability is NOT an experimental one.
    assert.equal(c.hasExperimentalCapability('tools'), false);
  });

  it('treats a key explicitly mapped to undefined as absent (the reference parity)', async () => {
    // JSON can't carry `undefined`; emit it from the server script directly.
    const c = await connect(`{ experimental:{ '${MCP_CHANNEL_CAPABILITY}': undefined } }`);
    assert.equal(c.hasExperimentalCapability(MCP_CHANNEL_CAPABILITY), false);
  });

  it('returns false when the server declares no experimental object', async () => {
    const c = await connect(`{ tools:{}, resources:{} }`);
    assert.equal(c.hasExperimentalCapability(MCP_CHANNEL_CAPABILITY), false);
  });

  it('returns false when experimental is a non-object (malformed server)', async () => {
    const c = await connect(`{ experimental: 'nope' }`);
    assert.equal(c.hasExperimentalCapability(MCP_CHANNEL_CAPABILITY), false);
  });
});

describe('McpClient.supportsChannelPermissionRelay', () => {
  it('is true only when BOTH experimental capabilities are declared', async () => {
    const c = await connect(`{ experimental:{ '${MCP_CHANNEL_CAPABILITY}': {}, '${MCP_CHANNEL_PERMISSION_CAPABILITY}': {} } }`);
    assert.equal(c.supportsChannelPermissionRelay(), true);
  });

  it('is false when only the channel capability is declared (no accidental permission surface)', async () => {
    const c = await connect(`{ experimental:{ '${MCP_CHANNEL_CAPABILITY}': {} } }`);
    assert.equal(c.supportsChannelPermissionRelay(), false);
  });

  it('is false when only the permission capability is declared', async () => {
    const c = await connect(`{ experimental:{ '${MCP_CHANNEL_PERMISSION_CAPABILITY}': {} } }`);
    assert.equal(c.supportsChannelPermissionRelay(), false);
  });

  it('is false when neither is declared', async () => {
    const c = await connect(`{ tools:{} }`);
    assert.equal(c.supportsChannelPermissionRelay(), false);
  });
});

describe('McpManager server status — channelPermissionRelay (end-to-end)', () => {
  async function managerWith(capsJson: string): Promise<McpManager> {
    const mgr = new McpManager();
    liveManagers.push(mgr);
    mgr.addServer('chan', { command: 'node', args: stdioArgs(capsJson) });
    await mgr.connectServer('chan');
    assert.equal(mgr.getServerStatus('chan')?.state, 'ready');
    return mgr;
  }

  it('surfaces channelPermissionRelay=true for a server that opts in via both caps', async () => {
    const mgr = await managerWith(`{ experimental:{ '${MCP_CHANNEL_CAPABILITY}': {}, '${MCP_CHANNEL_PERMISSION_CAPABILITY}': {} } }`);
    assert.equal(mgr.getServerStatus('chan')?.channelPermissionRelay, true);
    assert.equal(mgr.listServers().find((s) => s.name === 'chan')?.channelPermissionRelay, true);
  });

  it('surfaces channelPermissionRelay=false for a server that declares only one cap', async () => {
    const mgr = await managerWith(`{ experimental:{ '${MCP_CHANNEL_CAPABILITY}': {} } }`);
    assert.equal(mgr.getServerStatus('chan')?.channelPermissionRelay, false);
  });

  it('surfaces channelPermissionRelay=false for a plain server (no experimental caps)', async () => {
    const mgr = await managerWith(`{ tools:{} }`);
    assert.equal(mgr.getServerStatus('chan')?.channelPermissionRelay, false);
  });
});
