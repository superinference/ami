import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpClient } from '../src/mcp/client';
import { McpManager, findMcpConfigPaths } from '../src/mcp/manager';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

describe('McpClient', () => {
  it('starts in disconnected state', () => {
    const client = new McpClient({ command: 'echo', args: ['test'] });
    assert.equal(client.state, 'disconnected');
    assert.equal(client.serverInfo, null);
    assert.deepEqual(client.tools, []);
    assert.deepEqual(client.resources, []);
  });

  it('rejects operations when not ready', async () => {
    const client = new McpClient({ command: 'echo' });
    await assert.rejects(() => client.listTools(), /not ready/);
    await assert.rejects(() => client.callTool('test'), /not ready/);
    await assert.rejects(() => client.listResources(), /not ready/);
    await assert.rejects(() => client.readResource('test://uri'), /not ready/);
  });

  it('disconnect is safe to call when not connected', () => {
    const client = new McpClient({ command: 'echo' });
    client.disconnect();
    assert.equal(client.state, 'disconnected');
  });

  it('connects to a working MCP server via stdio', async () => {
    const serverScript = `
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin });
      rl.on('line', (line) => {
        try {
          const msg = JSON.parse(line);
          if (msg.method === 'initialize') {
            process.stdout.write(JSON.stringify({
              jsonrpc: '2.0',
              id: msg.id,
              result: {
                protocolVersion: '2024-11-05',
                serverInfo: { name: 'test-server', version: '1.0.0' },
                capabilities: {},
              },
            }) + '\\n');
          } else if (msg.method === 'tools/list') {
            process.stdout.write(JSON.stringify({
              jsonrpc: '2.0',
              id: msg.id,
              result: {
                tools: [
                  { name: 'greet', description: 'Greet someone', inputSchema: { type: 'object', properties: { name: { type: 'string' } } } },
                ],
              },
            }) + '\\n');
          } else if (msg.method === 'tools/call') {
            const name = msg.params?.arguments?.name || 'World';
            process.stdout.write(JSON.stringify({
              jsonrpc: '2.0',
              id: msg.id,
              result: { content: [{ type: 'text', text: 'Hello, ' + name + '!' }] },
            }) + '\\n');
          } else if (msg.method === 'resources/list') {
            process.stdout.write(JSON.stringify({
              jsonrpc: '2.0',
              id: msg.id,
              result: { resources: [{ uri: 'test://doc', name: 'doc', mimeType: 'text/plain' }] },
            }) + '\\n');
          } else if (msg.method === 'resources/read') {
            process.stdout.write(JSON.stringify({
              jsonrpc: '2.0',
              id: msg.id,
              result: { contents: [{ uri: msg.params.uri, text: 'content here' }] },
            }) + '\\n');
          }
        } catch {}
      });
    `;

    const client = new McpClient({
      command: 'node',
      args: ['-e', serverScript],
      requestTimeout: 5000,
    });

    try {
      await client.connect();
      assert.equal(client.state, 'ready');
      assert.deepEqual(client.serverInfo, { name: 'test-server', version: '1.0.0' });

      const tools = await client.listTools();
      assert.equal(tools.length, 1);
      assert.equal(tools[0].name, 'greet');

      const result = await client.callTool('greet', { name: 'Alice' }) as any;
      assert.ok(result.content[0].text.includes('Alice'));

      const resources = await client.listResources();
      assert.equal(resources.length, 1);
      assert.equal(resources[0].uri, 'test://doc');

      const resourceContent = await client.readResource('test://doc') as any;
      assert.ok(resourceContent.contents[0].text === 'content here');
    } finally {
      client.disconnect();
    }
  });

  it('handles server errors in responses', async () => {
    const serverScript = `
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin });
      rl.on('line', (line) => {
        try {
          const msg = JSON.parse(line);
          if (msg.method === 'initialize') {
            process.stdout.write(JSON.stringify({
              jsonrpc: '2.0', id: msg.id,
              result: { protocolVersion: '2024-11-05', serverInfo: { name: 'err-server', version: '1.0' }, capabilities: {} },
            }) + '\\n');
          } else if (msg.method === 'tools/list') {
            process.stdout.write(JSON.stringify({
              jsonrpc: '2.0', id: msg.id,
              error: { code: -32601, message: 'Method not found' },
            }) + '\\n');
          }
        } catch {}
      });
    `;

    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000 });
    try {
      await client.connect();
      await assert.rejects(() => client.listTools(), /Method not found/);
    } finally {
      client.disconnect();
    }
  });

  it('times out on unresponsive server', async () => {
    const serverScript = `
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin });
      rl.on('line', () => { /* ignore everything */ });
    `;

    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 500 });
    await assert.rejects(() => client.connect(), /timed out/);
    client.disconnect();
  });

  it('emits events on server lifecycle', async () => {
    const serverScript = `
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin });
      rl.on('line', (line) => {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { protocolVersion: '2024-11-05', serverInfo: { name: 'evt-server', version: '1.0' }, capabilities: {} },
          }) + '\\n');
        }
      });
    `;

    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000 });
    const events: string[] = [];
    client.on('ready', () => events.push('ready'));
    client.on('close', () => events.push('close'));

    await client.connect();
    assert.ok(events.includes('ready'));

    client.disconnect();
    await new Promise(r => setTimeout(r, 100));
    assert.ok(events.includes('close'));
  });
});

describe('McpManager', () => {
  let manager: McpManager;

  beforeEach(() => {
    manager = new McpManager();
  });

  afterEach(() => {
    manager.disconnectAll();
  });

  it('adds and removes servers', () => {
    manager.addServer('test', { command: 'echo' });
    assert.equal(manager.listServers().length, 1);
    assert.equal(manager.listServers()[0].name, 'test');

    manager.removeServer('test');
    assert.equal(manager.listServers().length, 0);
  });

  it('rejects duplicate server names', () => {
    manager.addServer('test', { command: 'echo' });
    assert.throws(() => manager.addServer('test', { command: 'echo' }), /already registered/);
  });

  it('getServerStatus returns null for unknown server', () => {
    assert.equal(manager.getServerStatus('nope'), null);
  });

  it('connectServer rejects unknown server', async () => {
    await assert.rejects(() => manager.connectServer('nope'), /not found/);
  });

  it('callTool rejects unknown tool', async () => {
    await assert.rejects(() => manager.callTool('nope'), /not found/);
  });

  it('callTool rejects unknown server in qualified name', async () => {
    await assert.rejects(() => manager.callTool('nope:tool'), /not found/);
  });

  it('connects to server and discovers tools', async () => {
    const serverScript = `
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin });
      rl.on('line', (line) => {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { protocolVersion: '2024-11-05', serverInfo: { name: 'mgr-server', version: '1.0' }, capabilities: {} },
          }) + '\\n');
        } else if (msg.method === 'tools/list') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { tools: [
              { name: 'add', description: 'Add numbers', inputSchema: { type: 'object' } },
              { name: 'sub', description: 'Subtract numbers', inputSchema: { type: 'object' } },
            ] },
          }) + '\\n');
        } else if (msg.method === 'tools/call') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { content: [{ type: 'text', text: 'result: 42' }] },
          }) + '\\n');
        }
      });
    `;

    manager.addServer('math', { command: 'node', args: ['-e', serverScript] });
    await manager.connectServer('math');

    const tools = manager.getAllTools();
    assert.equal(tools.length, 2);
    assert.equal(tools[0].serverName, 'math');

    const status = manager.getServerStatus('math');
    assert.ok(status);
    assert.equal(status.state, 'ready');
    assert.equal(status.toolCount, 2);

    const result = await manager.callTool('math:add', { a: 1, b: 2 }) as any;
    assert.ok(result.content[0].text.includes('42'));

    const result2 = await manager.callTool('add', { a: 1, b: 2 }) as any;
    assert.ok(result2.content[0].text.includes('42'));
  });

  it('connectAll handles mixed success/failure', async () => {
    const serverScript = `
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin });
      rl.on('line', (line) => {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' }, capabilities: {} },
          }) + '\\n');
        } else if (msg.method === 'tools/list') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { tools: [] },
          }) + '\\n');
        }
      });
    `;

    manager.addServer('good', { command: 'node', args: ['-e', serverScript] });
    manager.addServer('bad', { command: 'this-command-does-not-exist-xyz' });

    const results = await manager.connectAll();
    assert.equal(results.get('good'), null);
    assert.ok(results.get('bad') instanceof Error);
  });
});

describe('McpManager config loading', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-test-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('loadFromConfig reads server configs', () => {
    const configDir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(configDir, { recursive: true });
    const configPath = path.join(configDir, 'mcp.json');
    fs.writeFileSync(configPath, JSON.stringify({
      'test-server': {
        command: 'echo',
        args: ['test'],
        env: { FOO: 'bar' },
      },
    }));

    const manager = new McpManager();
    manager.loadFromConfig(configPath);
    assert.equal(manager.listServers().length, 1);
    assert.equal(manager.listServers()[0].name, 'test-server');
    manager.disconnectAll();
  });

  it('loadFromConfig handles mcpServers key', () => {
    const configPath = path.join(tmpDir, 'mcp.json');
    fs.writeFileSync(configPath, JSON.stringify({
      mcpServers: {
        'server-a': { command: 'node', args: ['script.js'] },
        'server-b': { command: 'python', args: ['-m', 'server'] },
      },
    }));

    const manager = new McpManager();
    manager.loadFromConfig(configPath);
    assert.equal(manager.listServers().length, 2);
    manager.disconnectAll();
  });

  it('loadFromConfig ignores missing file', () => {
    const manager = new McpManager();
    manager.loadFromConfig('/nonexistent/path/mcp.json');
    assert.equal(manager.listServers().length, 0);
  });

  it('loadFromConfig ignores invalid JSON', () => {
    const configPath = path.join(tmpDir, 'bad.json');
    fs.writeFileSync(configPath, 'not json');
    const manager = new McpManager();
    manager.loadFromConfig(configPath);
    assert.equal(manager.listServers().length, 0);
  });

  it('loadFromConfig skips entries without command', () => {
    const configPath = path.join(tmpDir, 'mcp.json');
    fs.writeFileSync(configPath, JSON.stringify({
      'no-cmd': { args: ['test'] },
      'has-cmd': { command: 'echo' },
    }));

    const manager = new McpManager();
    manager.loadFromConfig(configPath);
    assert.equal(manager.listServers().length, 1);
    assert.equal(manager.listServers()[0].name, 'has-cmd');
    manager.disconnectAll();
  });
});

describe('findMcpConfigPaths', () => {
  it('returns empty array when no configs exist', () => {
    const paths = findMcpConfigPaths('/tmp/nonexistent-dir-' + Date.now());
    assert.ok(Array.isArray(paths));
  });
});
