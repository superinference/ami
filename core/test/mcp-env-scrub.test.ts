import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpClient } from '../src/mcp/client';

// ---------------------------------------------------------------------------
// H31: the environment handed to a spawned stdio MCP server must have inherited
// secrets scrubbed, while the server's own explicitly-configured env survives.
// The server echoes selected env vars back through a tool call so the test can
// assert exactly what the child process received.
// ---------------------------------------------------------------------------

const serverScript = `
  const readline = require('readline');
  const rl = readline.createInterface({ input: process.stdin });
  rl.on('line', (line) => {
    try {
      const msg = JSON.parse(line);
      const send = (result) => process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result }) + '\\n');
      if (msg.method === 'initialize') {
        send({ protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' }, capabilities: {} });
      } else if (msg.method === 'tools/call') {
        send({ content: [{ type: 'text', text: JSON.stringify({
          secret: process.env.ANTHROPIC_API_KEY ?? null,
          token: process.env.GITHUB_TOKEN ?? null,
          explicit: process.env.MCP_SERVER_EXPLICIT ?? null,
          path: process.env.PATH ? 'present' : 'absent',
        }) }] });
      }
    } catch {}
  });
`;

const savedKey = process.env.ANTHROPIC_API_KEY;
const savedTok = process.env.GITHUB_TOKEN;

afterEach(() => {
  if (savedKey === undefined) delete process.env.ANTHROPIC_API_KEY; else process.env.ANTHROPIC_API_KEY = savedKey;
  if (savedTok === undefined) delete process.env.GITHUB_TOKEN; else process.env.GITHUB_TOKEN = savedTok;
});

describe('stdio MCP server env scrubbing (H31)', () => {
  it('strips inherited secrets but keeps explicit env and PATH', async () => {
    process.env.ANTHROPIC_API_KEY = 'sk-should-not-leak';
    process.env.GITHUB_TOKEN = 'ghp-should-not-leak';
    const client = new McpClient({
      command: 'node',
      args: ['-e', serverScript],
      env: { MCP_SERVER_EXPLICIT: 'kept' },
      requestTimeout: 5000,
    });
    try {
      await client.connect();
      const res = await client.callTool('echo', {}) as any;
      const seen = JSON.parse(res.content[0].text);
      assert.equal(seen.secret, null, 'ANTHROPIC_API_KEY must be scrubbed');
      assert.equal(seen.token, null, 'GITHUB_TOKEN must be scrubbed');
      assert.equal(seen.explicit, 'kept', 'explicit server env survives');
      assert.equal(seen.path, 'present', 'non-sensitive PATH is still inherited');
    } finally {
      client.disconnect();
    }
  });
});
