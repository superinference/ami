import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpClient } from '../src/mcp/client';

// ---------------------------------------------------------------------------
// H26: untrusted MCP server data (tool/resource/prompt metadata + tool results)
// must have invisible/smuggling Unicode stripped before it reaches the model.
// Drives a real stdio server whose payloads carry a zero-width space (U+200B)
// and a bidi override (U+202E), then asserts the client scrubs them.
// ---------------------------------------------------------------------------

// zero-width space + right-to-left override, embedded in server payloads.
const ZW = '\\u200b';
const BIDI = '\\u202e';

const serverScript = `
  const readline = require('readline');
  const rl = readline.createInterface({ input: process.stdin });
  const ZW = '${ZW}';
  const BIDI = '${BIDI}';
  rl.on('line', (line) => {
    try {
      const msg = JSON.parse(line);
      const send = (result) => process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result }) + '\\n');
      if (msg.method === 'initialize') {
        send({ protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' }, capabilities: {} });
      } else if (msg.method === 'tools/list') {
        send({ tools: [{ name: 'do'+ZW+'thing', description: 'a'+ZW+'desc', inputSchema: { type: 'object', properties: { q: { type: 'string', description: 'f'+ZW+'help' } } } }] });
      } else if (msg.method === 'tools/call') {
        send({ content: [{ type: 'text', text: 'res'+BIDI+'ult' }] });
      } else if (msg.method === 'resources/list') {
        send({ resources: [{ uri: 'test://x', name: 'n'+ZW+'ame', mimeType: 'text/plain' }] });
      } else if (msg.method === 'resources/read') {
        send({ contents: [{ uri: msg.params.uri, text: 'bo'+ZW+'dy' }] });
      } else if (msg.method === 'prompts/list') {
        send({ prompts: [{ name: 'p'+ZW+'r', description: 'd'+ZW+'esc' }] });
      } else if (msg.method === 'prompts/get') {
        send({ messages: [{ role: 'user', content: { type: 'text', text: 'he'+ZW+'llo' } }] });
      }
    } catch {}
  });
`;

describe('McpClient unicode sanitization of untrusted server data (H26)', () => {
  it('scrubs tool metadata, results, resources, and prompts', async () => {
    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000 });
    try {
      await client.connect();

      const tools = await client.listTools();
      assert.equal(tools[0].name, 'dothing');
      assert.equal(tools[0].description, 'adesc');
      assert.equal((tools[0].inputSchema as any).properties.q.description, 'fhelp');

      const result = await client.callTool('dothing', {}) as any;
      assert.equal(result.content[0].text, 'result');

      const resources = await client.listResources();
      assert.equal(resources[0].name, 'name');

      const read = await client.readResource('test://x') as any;
      assert.equal(read.contents[0].text, 'body');

      const prompts = await client.listPrompts();
      assert.equal(prompts[0].name, 'pr');
      assert.equal(prompts[0].description, 'desc');

      const got = await client.getPrompt('pr') as any;
      assert.equal(got.messages[0].content.text, 'hello');
    } finally {
      client.disconnect();
    }
  });
});
