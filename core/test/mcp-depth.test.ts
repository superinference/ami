import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpClient, McpSessionExpiredError, McpTerminalError, McpTerminalErrorCode } from '../src/mcp/client';
import { McpManager, expandEnvVars } from '../src/mcp/manager';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

// ---------------------------------------------------------------------------
// Graceful shutdown escalation (SIGINT → SIGTERM → SIGKILL)
// ---------------------------------------------------------------------------

describe('McpClient — graceful shutdown', () => {
  it('disconnect sets state to disconnected', () => {
    const client = new McpClient({ command: 'echo' });
    client.disconnect();
    assert.equal(client.state, 'disconnected');
  });

  it('double disconnect is safe (re-entry guard)', () => {
    const client = new McpClient({ command: 'echo' });
    client.disconnect();
    client.disconnect();
    assert.equal(client.state, 'disconnected');
  });

  it('graceful shutdown kills a running process', async () => {
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
        }
      });
      // Keep alive
      setInterval(() => {}, 10000);
    `;

    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000 });
    await client.connect();
    assert.equal(client.state, 'ready');

    const closeEvents: any[] = [];
    client.on('close', (...args) => closeEvents.push(args));

    client.disconnect();
    assert.equal(client.state, 'disconnected');

    // Close event should be emitted
    assert.ok(closeEvents.length >= 1);
  });
});

// ---------------------------------------------------------------------------
// Terminal error classification
// ---------------------------------------------------------------------------

describe('McpClient — terminal error classification', () => {
  it('classifies ECONNRESET', () => {
    const client = new McpClient({ command: 'echo' });
    const err = new Error('read ECONNRESET') as NodeJS.ErrnoException;
    err.code = 'ECONNRESET';
    const classified = client.classifyError(err);
    assert.equal(classified.code, 'ECONNRESET');
    assert.ok(classified.message.includes('reset'));
  });

  it('classifies ETIMEDOUT', () => {
    const client = new McpClient({ command: 'echo' });
    const err = new Error('connect ETIMEDOUT') as NodeJS.ErrnoException;
    err.code = 'ETIMEDOUT';
    const classified = client.classifyError(err);
    assert.equal(classified.code, 'ETIMEDOUT');
    assert.ok(classified.message.includes('timed out'));
  });

  it('classifies EPIPE', () => {
    const client = new McpClient({ command: 'echo' });
    const err = new Error('write EPIPE') as NodeJS.ErrnoException;
    err.code = 'EPIPE';
    const classified = client.classifyError(err);
    assert.equal(classified.code, 'EPIPE');
    assert.ok(classified.message.includes('pipe'));
  });

  it('classifies EHOSTUNREACH', () => {
    const client = new McpClient({ command: 'echo' });
    const err = new Error('connect EHOSTUNREACH') as NodeJS.ErrnoException;
    err.code = 'EHOSTUNREACH';
    const classified = client.classifyError(err);
    assert.equal(classified.code, 'EHOSTUNREACH');
    assert.ok(classified.message.includes('unreachable'));
  });

  it('classifies ECONNREFUSED', () => {
    const client = new McpClient({ command: 'echo' });
    const err = new Error('connect ECONNREFUSED') as NodeJS.ErrnoException;
    err.code = 'ECONNREFUSED';
    const classified = client.classifyError(err);
    assert.equal(classified.code, 'ECONNREFUSED');
    assert.ok(classified.message.includes('refused'));
  });

  it('classifies ESRCH', () => {
    const client = new McpClient({ command: 'echo' });
    const err = new Error('kill ESRCH') as NodeJS.ErrnoException;
    err.code = 'ESRCH';
    const classified = client.classifyError(err);
    assert.equal(classified.code, 'ESRCH');
    assert.ok(classified.message.includes('not found'));
  });

  it('classifies spawn errors', () => {
    const client = new McpClient({ command: 'echo' });
    const err = new Error('spawn ENOENT');
    const classified = client.classifyError(err);
    assert.equal(classified.code, 'SPAWN_ERROR');
  });

  it('classifies terminated processes', () => {
    const client = new McpClient({ command: 'echo' });
    const err = new Error('Process was terminated');
    const classified = client.classifyError(err);
    assert.equal(classified.code, 'ESRCH');
  });

  it('classifies unknown errors', () => {
    const client = new McpClient({ command: 'echo' });
    const err = new Error('Something went wrong');
    const classified = client.classifyError(err);
    assert.equal(classified.code, 'UNKNOWN');
    assert.equal(classified.original, err);
  });

  it('emits terminal-error on process spawn failure', async () => {
    const client = new McpClient({
      command: 'nonexistent-command-xyz',
      connectTimeout: 2000,
      requestTimeout: 1000,
    });

    const errors: McpTerminalError[] = [];
    client.on('terminal-error', (e: McpTerminalError) => errors.push(e));
    client.on('error', () => {}); // suppress unhandled

    await assert.rejects(() => client.connect());
    // Wait for async spawn error event
    await new Promise(r => setTimeout(r, 100));
    client.disconnect();

    assert.ok(errors.length >= 1);
    assert.equal(errors[0].code, 'SPAWN_ERROR');
  });
});

// ---------------------------------------------------------------------------
// McpSessionExpiredError
// ---------------------------------------------------------------------------

describe('McpSessionExpiredError', () => {
  it('has correct name and message', () => {
    const err = new McpSessionExpiredError();
    assert.equal(err.name, 'McpSessionExpiredError');
    assert.equal(err.message, 'MCP session expired');
  });

  it('accepts custom message', () => {
    const err = new McpSessionExpiredError('Custom expiry');
    assert.equal(err.message, 'Custom expiry');
  });

  it('is an instance of Error', () => {
    const err = new McpSessionExpiredError();
    assert.ok(err instanceof Error);
    assert.ok(err instanceof McpSessionExpiredError);
  });
});

// ---------------------------------------------------------------------------
// Session expiry detection via JSON-RPC -32001
// ---------------------------------------------------------------------------

describe('McpClient — session expiry detection', () => {
  it('detects -32001 error in stdio response and emits session-expired', async () => {
    const serverScript = `
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin });
      let initialized = false;
      rl.on('line', (line) => {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' }, capabilities: {} },
          }) + '\\n');
          initialized = true;
        } else if (msg.method === 'tools/list' && initialized) {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            error: { code: -32001, message: 'Session not found' },
          }) + '\\n');
        }
      });
    `;

    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000 });
    const sessionEvents: string[] = [];
    client.on('session-expired', () => sessionEvents.push('expired'));

    await client.connect();
    await assert.rejects(() => client.listTools(), /McpSessionExpiredError|session expired/i);
    assert.ok(sessionEvents.includes('expired'));
    client.disconnect();
  });
});

// ---------------------------------------------------------------------------
// Tool cancellation via AbortSignal
// ---------------------------------------------------------------------------

describe('McpClient — tool cancellation via AbortSignal', () => {
  it('rejects immediately when signal is already aborted', async () => {
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
        }
      });
    `;

    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000 });
    await client.connect();

    const controller = new AbortController();
    controller.abort();

    await assert.rejects(
      () => client.callTool('test', {}, { signal: controller.signal }),
      /aborted/
    );
    client.disconnect();
  });

  it('aborts an in-flight tool call when signal fires', async () => {
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
        }
        // tools/call never responds — simulates a slow server
      });
    `;

    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 30000 });
    await client.connect();

    const controller = new AbortController();
    const callPromise = client.callTool('slow-tool', {}, { signal: controller.signal });

    setTimeout(() => controller.abort(), 100);

    await assert.rejects(callPromise, /aborted/);
    client.disconnect();
  });
});

// ---------------------------------------------------------------------------
// MCP Roots protocol
// ---------------------------------------------------------------------------

describe('McpClient — Roots protocol', () => {
  it('declares roots capability when rootPaths provided', async () => {
    const serverScript = `
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin });
      rl.on('line', (line) => {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          // Verify client capabilities include roots
          const hasRoots = msg.params?.capabilities?.roots !== undefined;
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: {
              protocolVersion: '2024-11-05',
              serverInfo: { name: 'roots-server', version: '1' },
              capabilities: { rootsReceived: hasRoots },
            },
          }) + '\\n');
        }
      });
    `;

    const client = new McpClient({
      command: 'node',
      args: ['-e', serverScript],
      requestTimeout: 5000,
      rootPaths: ['/home/user/project'],
    });

    await client.connect();
    assert.ok(client.serverCapabilities.rootsReceived);
    client.disconnect();
  });

  it('does not declare roots capability when no rootPaths', async () => {
    const serverScript = `
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin });
      rl.on('line', (line) => {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          const hasRoots = msg.params?.capabilities?.roots !== undefined;
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: {
              protocolVersion: '2024-11-05',
              serverInfo: { name: 'no-roots', version: '1' },
              capabilities: { rootsReceived: hasRoots },
            },
          }) + '\\n');
        }
      });
    `;

    const client = new McpClient({
      command: 'node',
      args: ['-e', serverScript],
      requestTimeout: 5000,
    });

    await client.connect();
    assert.equal(client.serverCapabilities.rootsReceived, false);
    client.disconnect();
  });

  it('getRoots returns file URIs for rootPaths', () => {
    const client = new McpClient({
      command: 'echo',
      rootPaths: ['/home/user/project', '/home/user/other'],
    });

    const roots = client.getRoots();
    assert.equal(roots.length, 2);
    assert.equal(roots[0].uri, 'file:///home/user/project');
    assert.equal(roots[0].name, 'project');
    assert.equal(roots[1].uri, 'file:///home/user/other');
    assert.equal(roots[1].name, 'other');
  });

  it('getRoots returns empty array when no rootPaths', () => {
    const client = new McpClient({ command: 'echo' });
    assert.deepEqual(client.getRoots(), []);
  });
});

// ---------------------------------------------------------------------------
// Connection timeout (Promise.race)
// ---------------------------------------------------------------------------

describe('McpClient — connection timeout', () => {
  it('times out when server is unresponsive during connect', async () => {
    const serverScript = `
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin });
      rl.on('line', () => { /* never respond */ });
    `;

    const client = new McpClient({
      command: 'node',
      args: ['-e', serverScript],
      connectTimeout: 500,
      requestTimeout: 10000,
    });

    await assert.rejects(() => client.connect(), /timed out/);
    client.disconnect();
  });

  it('uses separate connect and request timeouts', () => {
    const client = new McpClient({
      command: 'echo',
      connectTimeout: 3000,
      requestTimeout: 15000,
    });
    assert.equal(client.state, 'disconnected');
  });
});

// ---------------------------------------------------------------------------
// Connection uptime tracking
// ---------------------------------------------------------------------------

describe('McpClient — uptime tracking', () => {
  it('uptime is null when disconnected', () => {
    const client = new McpClient({ command: 'echo' });
    assert.equal(client.uptime, null);
  });

  it('uptime is positive after connection', async () => {
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
        }
      });
    `;

    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000 });
    await client.connect();

    assert.ok(client.uptime !== null);
    assert.ok(client.uptime! >= 0);

    client.disconnect();
    assert.equal(client.uptime, null);
  });
});

// ---------------------------------------------------------------------------
// Server capability negotiation
// ---------------------------------------------------------------------------

describe('McpClient — capability negotiation', () => {
  it('stores server capabilities from initialize response', async () => {
    const serverScript = `
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin });
      rl.on('line', (line) => {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: {
              protocolVersion: '2024-11-05',
              serverInfo: { name: 'cap-server', version: '2.0' },
              capabilities: {
                tools: {},
                resources: { subscribe: true },
                prompts: {},
              },
            },
          }) + '\\n');
        } else if (msg.method === 'tools/list') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { tools: [] },
          }) + '\\n');
        } else if (msg.method === 'resources/list') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { resources: [] },
          }) + '\\n');
        }
      });
    `;

    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000 });
    await client.connect();

    assert.ok(client.hasCapability('tools'));
    assert.ok(client.hasCapability('resources'));
    assert.ok(client.hasCapability('prompts'));
    assert.ok(!client.hasCapability('sampling'));

    const caps = client.serverCapabilities;
    assert.deepEqual(caps.resources, { subscribe: true });

    client.disconnect();
  });

  it('returns empty capabilities when server sends none', async () => {
    const serverScript = `
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin });
      rl.on('line', (line) => {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' } },
          }) + '\\n');
        }
      });
    `;

    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000 });
    await client.connect();

    assert.deepEqual(client.serverCapabilities, {});
    assert.ok(!client.hasCapability('tools'));

    client.disconnect();
  });
});

// ---------------------------------------------------------------------------
// Tool list changed notification
// ---------------------------------------------------------------------------

describe('McpClient — notification handling', () => {
  it('emits tools-changed on notifications/tools/list_changed', async () => {
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
          // After init, send a tool list changed notification
          setTimeout(() => {
            process.stdout.write(JSON.stringify({
              jsonrpc: '2.0',
              method: 'notifications/tools/list_changed',
              params: {},
            }) + '\\n');
          }, 50);
        }
      });
    `;

    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000 });
    const events: string[] = [];
    client.on('tools-changed', () => events.push('tools-changed'));

    await client.connect();
    await new Promise(r => setTimeout(r, 200));

    assert.ok(events.includes('tools-changed'));
    client.disconnect();
  });

  it('emits resources-changed on notifications/resources/list_changed', async () => {
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
          setTimeout(() => {
            process.stdout.write(JSON.stringify({
              jsonrpc: '2.0',
              method: 'notifications/resources/list_changed',
              params: {},
            }) + '\\n');
          }, 50);
        }
      });
    `;

    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000 });
    const events: string[] = [];
    client.on('resources-changed', () => events.push('resources-changed'));

    await client.connect();
    await new Promise(r => setTimeout(r, 200));

    assert.ok(events.includes('resources-changed'));
    client.disconnect();
  });
});

// ---------------------------------------------------------------------------
// EPIPE handling on stdin writes
// ---------------------------------------------------------------------------

describe('McpClient — EPIPE handling', () => {
  it('handles EPIPE on stdin without crashing', async () => {
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
          // Exit immediately after init to cause EPIPE on next write
          setTimeout(() => process.exit(0), 50);
        }
      });
    `;

    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000 });
    await client.connect();
    await new Promise(r => setTimeout(r, 200));

    // The process has exited; subsequent operations should fail gracefully
    client.disconnect();
    assert.equal(client.state, 'disconnected');
  });
});

// ---------------------------------------------------------------------------
// McpManager — AbortSignal passthrough
// ---------------------------------------------------------------------------

describe('McpManager — callTool with AbortSignal', () => {
  let manager: McpManager;

  beforeEach(() => {
    manager = new McpManager();
  });

  afterEach(() => {
    manager.disconnectAll();
  });

  it('passes AbortSignal through to client callTool', async () => {
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
            result: { tools: [{ name: 'slow', description: 'Slow tool', inputSchema: { type: 'object' } }] },
          }) + '\\n');
        }
        // tools/call never responds
      });
    `;

    manager.addServer('test', { command: 'node', args: ['-e', serverScript] });
    await manager.connectServer('test');

    const controller = new AbortController();
    controller.abort();

    await assert.rejects(
      () => manager.callTool('test:slow', {}, { signal: controller.signal }),
      /aborted/
    );
  });
});

// ---------------------------------------------------------------------------
// McpManager — root paths
// ---------------------------------------------------------------------------

describe('McpManager — root paths', () => {
  it('passes rootPaths to clients', () => {
    const manager = new McpManager();
    manager.setRootPaths(['/project']);
    manager.addServer('test', { command: 'echo' });

    const status = manager.getServerStatus('test');
    assert.ok(status);
    manager.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// McpManager — tool annotations (extended)
// ---------------------------------------------------------------------------

describe('McpManager — tool annotations (extended)', () => {
  let manager: McpManager;

  beforeEach(() => {
    manager = new McpManager();
  });

  it('detects fetch tools as openWorld', () => {
    const ann = manager.getToolAnnotations('fetchPage');
    assert.equal(ann.openWorldHint, true);
  });

  it('detects query tools as readOnly', () => {
    const ann = manager.getToolAnnotations('queryDatabase');
    assert.equal(ann.readOnlyHint, true);
  });

  it('detects truncate as destructive', () => {
    const ann = manager.getToolAnnotations('truncateTable');
    assert.equal(ann.destructiveHint, true);
  });

  it('detects web tools as openWorld', () => {
    const ann = manager.getToolAnnotations('webSearch');
    assert.equal(ann.openWorldHint, true);
  });

  it('prefers server-declared annotations over name heuristics', () => {
    manager.addServer('test', { command: 'echo' });
    (manager as any)._allTools.set('test:deleteSafely', {
      serverName: 'test',
      schema: {
        name: 'deleteSafely',
        description: 'Safe delete',
        inputSchema: {},
        annotations: { readOnlyHint: true, destructiveHint: false },
      },
    });
    const ann = manager.getToolAnnotations('test:deleteSafely');
    assert.equal(ann.readOnlyHint, true);
    assert.equal(ann.destructiveHint, false);
    manager.removeServer('test');
  });
});

// ---------------------------------------------------------------------------
// McpManager — server status includes uptime
// ---------------------------------------------------------------------------

describe('McpManager — server status uptime', () => {
  let manager: McpManager;

  afterEach(() => {
    manager?.disconnectAll();
  });

  it('reports uptime after connection', async () => {
    manager = new McpManager();
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

    manager.addServer('test', { command: 'node', args: ['-e', serverScript] });
    await manager.connectServer('test');

    const status = manager.getServerStatus('test');
    assert.ok(status);
    assert.ok(status.uptime !== null);
    assert.ok(status.uptime! >= 0);
  });
});

// ---------------------------------------------------------------------------
// McpManager — tool cache invalidation on tools-changed
// ---------------------------------------------------------------------------

describe('McpManager — tool cache invalidation', () => {
  let manager: McpManager;

  afterEach(() => {
    manager?.disconnectAll();
  });

  it('invalidates tool cache when tools-changed fires', async () => {
    manager = new McpManager();

    const serverScript = `
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin });
      let callCount = 0;
      rl.on('line', (line) => {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' }, capabilities: {} },
          }) + '\\n');
        } else if (msg.method === 'tools/list') {
          callCount++;
          const tools = callCount === 1
            ? [{ name: 'toolA', description: 'A', inputSchema: { type: 'object' } }]
            : [
                { name: 'toolA', description: 'A', inputSchema: { type: 'object' } },
                { name: 'toolB', description: 'B', inputSchema: { type: 'object' } },
              ];
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { tools },
          }) + '\\n');
          // After first list, send tool list changed notification
          if (callCount === 1) {
            setTimeout(() => {
              process.stdout.write(JSON.stringify({
                jsonrpc: '2.0',
                method: 'notifications/tools/list_changed',
                params: {},
              }) + '\\n');
            }, 50);
          }
        }
      });
    `;

    manager.addServer('dyn', { command: 'node', args: ['-e', serverScript] });
    await manager.connectServer('dyn');

    assert.equal(manager.getAllTools().length, 1);
    assert.equal(manager.getAllTools()[0].schema.name, 'toolA');

    // Wait for tools-changed + refresh
    await new Promise(r => setTimeout(r, 500));

    assert.equal(manager.getAllTools().length, 2);
  });
});

// ---------------------------------------------------------------------------
// Environment variable expansion
// ---------------------------------------------------------------------------

describe('expandEnvVars', () => {
  it('expands ${VAR} syntax', () => {
    process.env.__TEST_MCP_VAR = 'hello';
    assert.equal(expandEnvVars('prefix-${__TEST_MCP_VAR}-suffix'), 'prefix-hello-suffix');
    delete process.env.__TEST_MCP_VAR;
  });

  it('expands $VAR syntax', () => {
    process.env.__TEST_MCP_BARE = 'world';
    assert.equal(expandEnvVars('$__TEST_MCP_BARE'), 'world');
    delete process.env.__TEST_MCP_BARE;
  });

  it('replaces undefined vars with empty string', () => {
    delete process.env.__UNDEFINED_MCP_VAR;
    assert.equal(expandEnvVars('${__UNDEFINED_MCP_VAR}'), '');
  });

  it('handles multiple vars in one string', () => {
    process.env.__TEST_A = 'foo';
    process.env.__TEST_B = 'bar';
    assert.equal(expandEnvVars('${__TEST_A}/${__TEST_B}'), 'foo/bar');
    delete process.env.__TEST_A;
    delete process.env.__TEST_B;
  });

  it('leaves strings without vars unchanged', () => {
    assert.equal(expandEnvVars('no vars here'), 'no vars here');
  });

  it('handles empty string', () => {
    assert.equal(expandEnvVars(''), '');
  });
});

// ---------------------------------------------------------------------------
// McpManager — loadFromConfig with env expansion
// ---------------------------------------------------------------------------

describe('McpManager — config env expansion', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-env-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('expands env vars in command and args', () => {
    process.env.__MCP_TEST_CMD = 'python3';
    process.env.__MCP_TEST_ARG = 'server.py';

    const configPath = path.join(tmpDir, 'mcp.json');
    fs.writeFileSync(configPath, JSON.stringify({
      'env-server': {
        command: '${__MCP_TEST_CMD}',
        args: ['${__MCP_TEST_ARG}', '--port', '8080'],
      },
    }));

    const manager = new McpManager();
    manager.loadFromConfig(configPath);

    const servers = manager.listServers();
    assert.equal(servers.length, 1);
    assert.equal(servers[0].name, 'env-server');

    manager.disconnectAll();
    delete process.env.__MCP_TEST_CMD;
    delete process.env.__MCP_TEST_ARG;
  });
});

// ---------------------------------------------------------------------------
// McpManager — reconnect scheduling
// ---------------------------------------------------------------------------

describe('McpManager — reconnect timer cleanup', () => {
  it('cleans up reconnect timers on removeServer', () => {
    const manager = new McpManager();
    manager.addServer('test', { command: 'echo' });
    manager.removeServer('test');
    assert.equal(manager.listServers().length, 0);
  });

  it('cleans up reconnect timers on disconnectAll', () => {
    const manager = new McpManager();
    manager.addServer('a', { command: 'echo' });
    manager.addServer('b', { command: 'echo' });
    manager.disconnectAll();
    // disconnectAll disconnects clients and clears tool cache, but doesn't remove server registrations
    // Verify no crash and all tools are cleared
    assert.equal(manager.getAllTools().length, 0);
  });
});

// ---------------------------------------------------------------------------
// McpClient — prompts/list and prompts/get
// ---------------------------------------------------------------------------

describe('McpClient — prompts protocol', () => {
  it('listPrompts returns prompt definitions', async () => {
    const serverScript = `
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin });
      rl.on('line', (line) => {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' }, capabilities: { prompts: {} } },
          }) + '\\n');
        } else if (msg.method === 'prompts/list') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { prompts: [
              { name: 'summarize', description: 'Summarize text', arguments: [{ name: 'text', required: true }] },
              { name: 'translate', description: 'Translate text' },
            ] },
          }) + '\\n');
        }
      });
    `;

    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000 });
    await client.connect();

    const prompts = await client.listPrompts();
    assert.equal(prompts.length, 2);
    assert.equal(prompts[0].name, 'summarize');
    assert.equal(prompts[0].description, 'Summarize text');
    assert.ok(prompts[0].arguments);
    assert.equal(prompts[0].arguments![0].name, 'text');
    assert.equal(prompts[1].name, 'translate');

    assert.deepEqual(client.prompts, prompts);
    client.disconnect();
  });

  it('getPrompt returns prompt messages', async () => {
    const serverScript = `
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin });
      rl.on('line', (line) => {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' }, capabilities: { prompts: {} } },
          }) + '\\n');
        } else if (msg.method === 'prompts/get') {
          const name = msg.params.name;
          const lang = msg.params.arguments?.language || 'en';
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: {
              description: 'Translate prompt',
              messages: [
                { role: 'user', content: { type: 'text', text: 'Translate "hello" to ' + lang } },
              ],
            },
          }) + '\\n');
        }
      });
    `;

    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000 });
    await client.connect();

    const result = await client.getPrompt('translate', { language: 'es' });
    assert.equal(result.description, 'Translate prompt');
    assert.equal(result.messages.length, 1);
    assert.equal(result.messages[0].role, 'user');
    assert.ok((result.messages[0].content as any).text.includes('es'));
    client.disconnect();
  });

  it('prompts getter returns empty before listing', () => {
    const client = new McpClient({ command: 'echo' });
    assert.deepEqual(client.prompts, []);
  });

  it('clears prompts cache on list_changed notification', async () => {
    const serverScript = `
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin });
      let listCount = 0;
      rl.on('line', (line) => {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' }, capabilities: { prompts: {} } },
          }) + '\\n');
        } else if (msg.method === 'prompts/list') {
          listCount++;
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { prompts: [{ name: 'p' + listCount, description: 'Prompt ' + listCount }] },
          }) + '\\n');
          if (listCount === 1) {
            setTimeout(() => {
              process.stdout.write(JSON.stringify({
                jsonrpc: '2.0', method: 'notifications/prompts/list_changed', params: {},
              }) + '\\n');
            }, 50);
          }
        }
      });
    `;

    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000 });
    await client.connect();

    const first = await client.listPrompts();
    assert.equal(first[0].name, 'p1');

    // Wait for notification to clear cache
    await new Promise(r => setTimeout(r, 200));

    const second = await client.listPrompts();
    assert.equal(second[0].name, 'p2');

    client.disconnect();
  });
});
