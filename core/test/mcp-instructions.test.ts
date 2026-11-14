import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpClient } from '../src/mcp/client';
import { McpManager } from '../src/mcp/manager';

// ---------------------------------------------------------------------------
// H32/H62/H63/H64: capture server-authored InitializeResult.instructions on the
// client, and surface them (truncated) through the manager for the prompt.
// ---------------------------------------------------------------------------

function serverScript(instructions: string | null): string {
  const instrJson = instructions === null ? 'undefined' : JSON.stringify(instructions);
  return `
    const readline = require('readline');
    const rl = readline.createInterface({ input: process.stdin });
    rl.on('line', (line) => {
      try {
        const msg = JSON.parse(line);
        const send = (result) => process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result }) + '\\n');
        if (msg.method === 'initialize') {
          const r = { protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' }, capabilities: {} };
          const instr = ${instrJson};
          if (instr !== undefined) r.instructions = instr;
          send(r);
        } else if (msg.method === 'tools/list') {
          send({ tools: [] });
        }
      } catch {}
    });
  `;
}

afterEach(() => { delete process.env.SUPERINFERENCE_MAX_MCP_DESCRIPTION_LENGTH; });

describe('MCP server instructions (H32/H62/H63/H64)', () => {
  it('captures instructions from the initialize result', async () => {
    const client = new McpClient({ command: 'node', args: ['-e', serverScript('Use tool X for Y.')], requestTimeout: 5000 });
    try {
      await client.connect();
      assert.equal(client.instructions, 'Use tool X for Y.');
    } finally {
      client.disconnect();
    }
  });

  it('is null when the server supplies no instructions', async () => {
    const client = new McpClient({ command: 'node', args: ['-e', serverScript(null)], requestTimeout: 5000 });
    try {
      await client.connect();
      assert.equal(client.instructions, null);
    } finally {
      client.disconnect();
    }
  });

  it('manager surfaces per-server instruction blocks for ready servers', async () => {
    const mgr = new McpManager();
    mgr.addServer('alpha', { command: 'node', args: ['-e', serverScript('alpha help')], requestTimeout: 5000 });
    mgr.addServer('beta', { command: 'node', args: ['-e', serverScript(null)], requestTimeout: 5000 });
    try {
      await mgr.connectServer('alpha');
      await mgr.connectServer('beta');
      const got = mgr.getServerInstructions();
      assert.deepEqual(got, [{ name: 'alpha', instructions: 'alpha help' }]);
    } finally {
      await mgr.disconnectAll();
    }
  });

  it('manager truncates overlong instructions with "… [truncated]"', async () => {
    process.env.SUPERINFERENCE_MAX_MCP_DESCRIPTION_LENGTH = '10';
    const mgr = new McpManager();
    mgr.addServer('alpha', { command: 'node', args: ['-e', serverScript('0123456789ABCDEFG')], requestTimeout: 5000 });
    try {
      await mgr.connectServer('alpha');
      const got = mgr.getServerInstructions();
      assert.equal(got[0].instructions, '0123456789… [truncated]');
    } finally {
      await mgr.disconnectAll();
    }
  });
});
