/**
 * Branch-coverage tests for src/mcp/manager.ts.
 *
 * ESM-SAFE: these tests never read source files as text, never monkeypatch
 * namespace bindings, and never couple fixtures to __dirname/import.meta.url.
 * Connect-dependent behavior is exercised against REAL spawned stdio MCP
 * servers (node -e scripts); filesystem behavior uses fresh fs.mkdtempSync
 * temp dirs. Everything asserts observable behavior / return values / errors,
 * so each test is red-green meaningful.
 */

import { describe, it, beforeEach, afterEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { once } from 'node:events';
import {
  McpManager,
  isTransientConnectError,
  validateMcpToolSchema,
  sanitizeMcpToolSchema,
  expandEnvVars,
  findMcpConfigPaths,
  MAX_MCP_TOOLS_PER_SERVER,
  MAX_MCP_TOOL_NAME_LENGTH,
} from '../src/mcp/manager';
import { McpSessionExpiredError } from '../src/mcp/client';

after(() => { setTimeout(() => process.exit(0), 200).unref(); });

const tmpDirs: string[] = [];
function tmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-mgr-'));
  tmpDirs.push(d);
  return d;
}

// A real, configurable stdio MCP server run as `node -e <script>`.
function serverScript(cfg: Record<string, unknown> = {}): string[] {
  const C = JSON.stringify({
    toolName: cfg.toolName ?? 'getThing',
    toolDesc: cfg.toolDesc ?? 'A tool',
    annotations: cfg.annotations ?? null,
    extraTool: cfg.extraTool ?? null,
    errorOn: cfg.errorOn ?? [],
    pingError: cfg.pingError ?? false,
    notify: cfg.notify ?? false,
  });
  const script = `
    const C = ${C};
    const readline = require('readline');
    const rl = readline.createInterface({ input: process.stdin });
    const send = (o) => process.stdout.write(JSON.stringify(o) + '\\n');
    const err = (id) => send({ jsonrpc: '2.0', id, error: { code: -32000, message: 'boom' } });
    rl.on('line', (line) => {
      let msg; try { msg = JSON.parse(line); } catch { return; }
      const m = msg.method;
      if (C.errorOn.includes(m)) { err(msg.id); return; }
      if (m === 'initialize') {
        send({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05',
          serverInfo: { name: 'srv', version: '1.0' },
          capabilities: { tools: {}, resources: {}, prompts: {} } } });
        if (C.notify) setTimeout(() => {
          send({ jsonrpc: '2.0', method: 'notifications/tools/list_changed' });
          send({ jsonrpc: '2.0', method: 'notifications/resources/list_changed' });
          send({ jsonrpc: '2.0', method: 'notifications/prompts/list_changed' });
        }, 15);
      } else if (m === 'tools/list') {
        const tools = [{ name: C.toolName, description: C.toolDesc, inputSchema: { type: 'object' } }];
        if (C.annotations) tools[0].annotations = C.annotations;
        if (C.extraTool) tools.push({ name: C.extraTool, description: 'extra', inputSchema: { type: 'object' } });
        send({ jsonrpc: '2.0', id: msg.id, result: { tools } });
      } else if (m === 'tools/call') {
        send({ jsonrpc: '2.0', id: msg.id, result: { content: [{ type: 'text', text: 'called' }] } });
      } else if (m === 'resources/list') {
        send({ jsonrpc: '2.0', id: msg.id, result: { resources: [{ uri: 'file:///a.txt', name: 'a.txt', mimeType: 'text/plain' }] } });
      } else if (m === 'resources/read') {
        send({ jsonrpc: '2.0', id: msg.id, result: { contents: [{ uri: msg.params.uri, text: 'hi' }] } });
      } else if (m === 'prompts/list') {
        send({ jsonrpc: '2.0', id: msg.id, result: { prompts: [{ name: 'p1', description: 'prompt one' }] } });
      } else if (m === 'prompts/get') {
        send({ jsonrpc: '2.0', id: msg.id, result: { description: 'd', messages: [{ role: 'user', content: { type: 'text', text: 'hi' } }] } });
      } else if (m === 'ping') {
        if (C.pingError) err(msg.id); else send({ jsonrpc: '2.0', id: msg.id, result: {} });
      } else if (msg.id !== undefined) {
        send({ jsonrpc: '2.0', id: msg.id, result: {} });
      }
    });
  `;
  return ['-e', script];
}

function serverConfig(cfg: Record<string, unknown> = {}) {
  return { command: process.execPath, args: serverScript(cfg) };
}

let mgr: McpManager;
beforeEach(() => { mgr = new McpManager(); });
afterEach(() => { mgr.disconnectAll(); });
after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch {} }
});

// ---------------------------------------------------------------------------
// isTransientConnectError
// ---------------------------------------------------------------------------
describe('isTransientConnectError', () => {
  it('returns false for McpSessionExpiredError (never transient)', () => {
    assert.equal(isTransientConnectError(new McpSessionExpiredError()), false);
  });
  it('returns true for a known transient error code', () => {
    assert.equal(isTransientConnectError(Object.assign(new Error('x'), { code: 'ECONNREFUSED' })), true);
  });
  it('returns true when the message matches the transient regex', () => {
    assert.equal(isTransientConnectError(new Error('socket hang up')), true);
  });
  it('returns false for a non-transient code and non-matching message', () => {
    assert.equal(isTransientConnectError(Object.assign(new Error('nope'), { code: 'EACCES' })), false);
  });
  it('returns false for a plain non-error value', () => {
    assert.equal(isTransientConnectError('just a string'), false);
  });
});

// ---------------------------------------------------------------------------
// validateMcpToolSchema — guard + every rejection branch
// ---------------------------------------------------------------------------
describe('validateMcpToolSchema', () => {
  it('rejects null and undefined', () => {
    assert.equal(validateMcpToolSchema(null).valid, false);
    assert.equal(validateMcpToolSchema(undefined).valid, false);
  });
  it('rejects arrays and non-objects', () => {
    assert.equal(validateMcpToolSchema([]).valid, false);
    assert.equal(validateMcpToolSchema('str').valid, false);
  });
  it('rejects a root type that is not object', () => {
    const r = validateMcpToolSchema({ type: 'string' });
    assert.equal(r.valid, false);
    assert.match(r.error!, /root type must be 'object'/);
  });
  it('rejects non-object properties', () => {
    assert.equal(validateMcpToolSchema({ type: 'object', properties: [] }).valid, false);
    assert.equal(validateMcpToolSchema({ type: 'object', properties: null }).valid, false);
  });
  it('rejects non-array required', () => {
    assert.equal(validateMcpToolSchema({ type: 'object', required: 'x' }).valid, false);
  });
  it('rejects non-string required items', () => {
    const r = validateMcpToolSchema({ type: 'object', required: [1] });
    assert.equal(r.valid, false);
    assert.match(r.error!, /required items must be strings/);
  });
  it('rejects a required key missing from properties', () => {
    const r = validateMcpToolSchema({ type: 'object', properties: { a: {} }, required: ['b'] });
    assert.equal(r.valid, false);
    assert.match(r.error!, /not defined in properties/);
  });
  it('accepts required present with no properties block (skips the in-props check)', () => {
    assert.equal(validateMcpToolSchema({ type: 'object', required: ['anything'] }).valid, true);
  });
  it('accepts a fully valid schema', () => {
    assert.equal(validateMcpToolSchema({ type: 'object', properties: { a: {} }, required: ['a'] }).valid, true);
  });
});

// ---------------------------------------------------------------------------
// sanitizeMcpToolSchema — guard vs bypass
// ---------------------------------------------------------------------------
describe('sanitizeMcpToolSchema', () => {
  it('returns a safe empty object for an invalid schema (guard)', () => {
    assert.deepEqual(sanitizeMcpToolSchema({ type: 'notobject' }), { type: 'object', properties: {} });
  });
  it('strips disallowed keys but keeps allowlisted keys (bypass variant)', () => {
    const out = sanitizeMcpToolSchema({
      type: 'object',
      properties: { a: {} },
      required: ['a'],
      $evil: 'x',
      __proto__key: 'y',
      description: 'ok',
    }) as Record<string, unknown>;
    assert.deepEqual(Object.keys(out).sort(), ['description', 'properties', 'required', 'type']);
    assert.equal(out.$evil, undefined);
  });
});

// ---------------------------------------------------------------------------
// expandEnvVars
// ---------------------------------------------------------------------------
describe('expandEnvVars', () => {
  it('expands braced, bare, and missing vars', () => {
    process.env.MCP_T_A = 'alpha';
    process.env.MCP_T_B = 'beta';
    try {
      assert.equal(expandEnvVars('${MCP_T_A}-$MCP_T_B-${MCP_T_MISSING}'), 'alpha-beta-');
    } finally {
      delete process.env.MCP_T_A; delete process.env.MCP_T_B;
    }
  });
});

// ---------------------------------------------------------------------------
// truncateDescription + env override (getMcpMaxDescriptionLength)
// ---------------------------------------------------------------------------
describe('truncateDescription env override', () => {
  afterEach(() => { delete process.env.SUPERINFERENCE_MAX_MCP_DESCRIPTION_LENGTH; });
  it('uses a valid positive env limit', () => {
    process.env.SUPERINFERENCE_MAX_MCP_DESCRIPTION_LENGTH = '10';
    const out = mgr.truncateDescription('a'.repeat(50));
    assert.equal(out.length, 10);
    assert.ok(out.endsWith('...'));
  });
  it('ignores a non-numeric env value and falls back to default', () => {
    process.env.SUPERINFERENCE_MAX_MCP_DESCRIPTION_LENGTH = 'notanumber';
    assert.equal(mgr.truncateDescription('a'.repeat(50)), 'a'.repeat(50));
  });
  it('ignores a non-positive env value and falls back to default', () => {
    process.env.SUPERINFERENCE_MAX_MCP_DESCRIPTION_LENGTH = '0';
    assert.equal(mgr.truncateDescription('short'), 'short');
  });
});

// ---------------------------------------------------------------------------
// allowlist: setAllowedServers / isServerAllowed / enforceAllowlist / denied
// ---------------------------------------------------------------------------
describe('allowlist enforcement', () => {
  it('allows everything when no allowlist is set', () => {
    assert.equal(mgr.isServerAllowed('anything'), true);
    assert.deepEqual(mgr.getDeniedServers(), []);
  });
  it('blocks addServer for a non-allowed name and records it once', () => {
    mgr.setAllowedServers(['ok']);
    assert.equal(mgr.isServerAllowed('bad'), false);
    assert.throws(() => mgr.addServer('bad', serverConfig()), /not in the allowed servers list/);
    assert.throws(() => mgr.addServer('bad', serverConfig()), /not in the allowed servers list/);
    assert.deepEqual(mgr.getDeniedServers(), ['bad']); // deduped
  });
  it('blocks addPendingServer for a non-allowed name', () => {
    mgr.setAllowedServers(['ok']);
    assert.throws(() => mgr.addPendingServer('bad', serverConfig()), /not in the allowed servers list/);
  });
  it('permits an allowed name', () => {
    mgr.setAllowedServers(['ok']);
    assert.doesNotThrow(() => mgr.addServer('ok', serverConfig()));
  });
});

// ---------------------------------------------------------------------------
// addServer / addPendingServer registration guards + container branch
// ---------------------------------------------------------------------------
describe('addServer / addPendingServer', () => {
  it('throws when a server is already registered', () => {
    mgr.addServer('dup', serverConfig());
    assert.throws(() => mgr.addServer('dup', serverConfig()), /already registered/);
  });
  it('rejects a duplicate pending registration (client or pending exists)', () => {
    mgr.addServer('a', serverConfig());
    assert.throws(() => mgr.addPendingServer('a', serverConfig()), /already registered/);
    mgr.addPendingServer('b', serverConfig());
    assert.throws(() => mgr.addPendingServer('b', serverConfig()), /already registered/);
  });
  it('applies rootPaths enrichment without error', () => {
    mgr.setRootPaths(['/tmp/projroot']);
    assert.doesNotThrow(() => mgr.addServer('withroot', serverConfig()));
    assert.equal(mgr.getServerState('withroot'), 'stopped'); // disconnected client
  });
  it('builds a container-backed server with a real runtime', () => {
    // docker/podman are present in this environment -> runtime resolves.
    mgr.addServer('cont', { command: '', containerConfig: { image: 'example.invalid/none:latest', runtime: 'auto' } });
    const status = mgr.listAllServers().find(s => s.name === 'cont');
    assert.equal(status?.containerBacked, true);
  });
  it('registers a pending container server and probes image availability', () => {
    mgr.addPendingServer('pc', { command: '', containerConfig: { image: 'example.invalid/none:latest', runtime: 'auto' } });
    const info = mgr.listAllServers().find(s => s.name === 'pc');
    assert.equal(info?.state, 'pending');
    assert.equal(info?.imageAvailable, false); // image not pulled locally
    assert.equal(info?.containerBacked, true);
  });
});

// ---------------------------------------------------------------------------
// ensureConnected lifecycle + connect/connectAll
// ---------------------------------------------------------------------------
describe('ensureConnected / connect lifecycle', () => {
  it('connects a plain (non-pending) server and caches tools', async () => {
    mgr.addServer('s1', serverConfig({ toolDesc: 'd'.repeat(5000) }));
    await mgr.ensureConnected('s1');
    assert.equal(mgr.getServerState('s1'), 'ready');
    const tools = mgr.getToolsForServer('s1');
    assert.equal(tools.length, 1);
    // description truncated by connectServer -> truncateDescription
    assert.ok(tools[0].description.length <= 2048);
    assert.ok(tools[0].description.endsWith('...'));
  });
  it('returns immediately when the client is already ready', async () => {
    mgr.addServer('s1', serverConfig());
    await mgr.ensureConnected('s1');
    await mgr.ensureConnected('s1'); // second call hits the ready short-circuit
    assert.equal(mgr.getServerState('s1'), 'ready');
  });
  it('throws for an unknown server', async () => {
    await assert.rejects(() => mgr.ensureConnected('ghost'), /not found/);
  });
  it('throws for a disabled pending server', async () => {
    mgr.addPendingServer('p1', serverConfig());
    await mgr.stopServer('p1', true); // disable
    await assert.rejects(() => mgr.ensureConnected('p1'), /is disabled/);
  });
  it('connects a pending server on demand and emits ready', async () => {
    mgr.addPendingServer('p2', serverConfig());
    const ready = once(mgr, 'server-ready');
    await mgr.ensureConnected('p2');
    await ready;
    assert.equal(mgr.getServerState('p2'), 'ready');
  });
  it('marks a pending server errored when the process fails to initialize', async () => {
    mgr.addPendingServer('pbad', { command: process.execPath, args: ['-e', 'process.exit(3)'] });
    await assert.rejects(() => mgr.ensureConnected('pbad'));
    assert.equal(mgr.getServerState('pbad'), 'error');
    const info = mgr.listAllServers().find(s => s.name === 'pbad');
    assert.ok(info?.error);
  });
  it('connectAll reports null for success and an Error for failure', async () => {
    mgr.addServer('good', serverConfig());
    mgr.addServer('bad', { command: process.execPath, args: ['-e', 'process.exit(1)'] });
    const results = await mgr.connectAll();
    assert.equal(results.get('good'), null);
    assert.ok(results.get('bad') instanceof Error);
  });
  it('connectServer throws for a missing client', async () => {
    await assert.rejects(() => mgr.connectServer('nope'), /not found/);
  });
});

// ---------------------------------------------------------------------------
// stop / restart / enable / state predicates
// ---------------------------------------------------------------------------
describe('stop / restart / enable / predicates', () => {
  it('stopServer (stopped) and stopServer (disabled) set distinct states', async () => {
    mgr.addServer('a', serverConfig());
    await mgr.ensureConnected('a');
    await mgr.stopServer('a');
    assert.equal(mgr.getServerState('a'), 'stopped');
    assert.equal(mgr.isPending('a'), true);
    await mgr.stopServer('a', true);
    assert.equal(mgr.getServerState('a'), 'disabled');
    assert.equal(mgr.isDisabled('a'), true);
    assert.equal(mgr.isPending('a'), false);
  });
  it('enableServer flips a disabled server back to stopped and is a no-op otherwise', async () => {
    mgr.addPendingServer('e1', serverConfig());
    await mgr.stopServer('e1', true);
    mgr.enableServer('e1');
    assert.equal(mgr.getServerState('e1'), 'stopped');
    // no-op path: not disabled
    mgr.enableServer('e1');
    assert.equal(mgr.getServerState('e1'), 'stopped');
    // no-op path: unknown server
    assert.doesNotThrow(() => mgr.enableServer('unknown'));
  });
  it('restartServer stops then reconnects', async () => {
    mgr.addServer('r1', serverConfig());
    await mgr.ensureConnected('r1');
    await mgr.restartServer('r1');
    assert.equal(mgr.getServerState('r1'), 'ready');
  });
  it('isPending false for an unregistered name; getServerState pending for unknown', () => {
    assert.equal(mgr.isPending('nobody'), false);
    assert.equal(mgr.isDisabled('nobody'), false);
    assert.equal(mgr.getServerState('nobody'), 'pending');
  });
});

// ---------------------------------------------------------------------------
// status listings
// ---------------------------------------------------------------------------
describe('status listings', () => {
  it('getServerStatus returns null for no client and a status object when connected', async () => {
    assert.equal(mgr.getServerStatus('x'), null);
    mgr.addServer('x', serverConfig());
    await mgr.ensureConnected('x');
    const st = mgr.getServerStatus('x');
    assert.equal(st?.state, 'ready');
    assert.equal(st?.toolCount, 1);
  });
  it('listServers reflects connected clients', async () => {
    mgr.addServer('x', serverConfig());
    await mgr.ensureConnected('x');
    assert.equal(mgr.listServers().length, 1);
  });
  it('listAllServers merges connected clients and pending servers without duplicates', async () => {
    mgr.addServer('conn', serverConfig());
    await mgr.ensureConnected('conn');
    mgr.addPendingServer('pend', serverConfig());
    const all = mgr.listAllServers();
    const names = all.map(s => s.name).sort();
    assert.deepEqual(names, ['conn', 'pend']);
    assert.equal(all.find(s => s.name === 'conn')!.state, 'ready');
    assert.equal(all.find(s => s.name === 'pend')!.state, 'pending');
  });
  it('listAllServers does not double-list a name present as both client and pending', async () => {
    mgr.addServer('both', serverConfig());
    await mgr.ensureConnected('both');
    await mgr.stopServer('both'); // creates a pending entry too... but client removed
    mgr.addServer('both', serverConfig()); // re-add client
    const all = mgr.listAllServers().filter(s => s.name === 'both');
    assert.equal(all.length, 1);
  });
});

// ---------------------------------------------------------------------------
// tool calls + resources + prompts
// ---------------------------------------------------------------------------
describe('callTool / resources / prompts', () => {
  it('calls a qualified tool name', async () => {
    mgr.addServer('s', serverConfig());
    await mgr.ensureConnected('s');
    const res = await mgr.callTool('s:getThing', {}) as { content: unknown[] };
    assert.ok(Array.isArray(res.content));
  });
  it('calls an unqualified tool name via the name index', async () => {
    mgr.addServer('s', serverConfig());
    await mgr.ensureConnected('s');
    const res = await mgr.callTool('getThing') as { content: unknown[] };
    assert.ok(Array.isArray(res.content));
  });
  it('throws for an unqualified tool that is not found', async () => {
    await assert.rejects(() => mgr.callTool('nonexistentTool'), /not found/);
  });
  it('throws for a qualified tool whose server is not registered', async () => {
    await assert.rejects(() => mgr.callTool('ghost:tool'), /not found/);
  });
  it('lists resources across servers with a filter', async () => {
    mgr.addServer('r', serverConfig());
    await mgr.ensureConnected('r');
    const all = await mgr.listResources();
    assert.equal(all.length, 1);
    assert.equal(all[0].server, 'r');
    assert.deepEqual(await mgr.listResources('does-not-match'), []);
  });
  it('swallows a non-session-expired error during listResources', async () => {
    mgr.addServer('re', serverConfig({ errorOn: ['resources/list'] }));
    await mgr.ensureConnected('re');
    assert.deepEqual(await mgr.listResources(), []); // error caught, no throw
  });
  it('reads a resource; throws when the server is not connected', async () => {
    await assert.rejects(() => mgr.readResource('absent', 'file:///a.txt'), /not connected/);
    mgr.addServer('r', serverConfig());
    await mgr.ensureConnected('r');
    const out = await mgr.readResource('r', 'file:///a.txt') as { contents: unknown[] };
    assert.ok(Array.isArray(out.contents));
  });
  it('propagates a read error from the client', async () => {
    mgr.addServer('re2', serverConfig({ errorOn: ['resources/read'] }));
    await mgr.ensureConnected('re2');
    await assert.rejects(() => mgr.readResource('re2', 'file:///a.txt'));
  });
  it('lists prompts only from ready servers and skips erroring ones', async () => {
    mgr.addServer('p', serverConfig());
    await mgr.ensureConnected('p');
    const prompts = await mgr.listPrompts();
    assert.equal(prompts.length, 1);
    assert.equal(prompts[0].server, 'p');
    assert.deepEqual(await mgr.listPrompts('other'), []);

    // not-ready server is skipped (client exists but never connected)
    mgr.addServer('notready', serverConfig());
    assert.equal((await mgr.listPrompts()).length, 1);

    // erroring prompts/list is swallowed
    mgr.addServer('pe', serverConfig({ errorOn: ['prompts/list'] }));
    await mgr.ensureConnected('pe');
    const after = await mgr.listPrompts('pe');
    assert.deepEqual(after, []);
  });
  it('gets a prompt; throws when the server is not connected', async () => {
    await assert.rejects(() => mgr.getPrompt('absent', 'p1'), /not connected/);
    mgr.addServer('p', serverConfig());
    await mgr.ensureConnected('p');
    const got = await mgr.getPrompt('p', 'p1');
    assert.ok(Array.isArray(got.messages));
  });
});

// ---------------------------------------------------------------------------
// tool inventory + conflicts + annotations
// ---------------------------------------------------------------------------
describe('tool inventory, conflicts, annotations', () => {
  it('getAllTools and getToolConflicts detect a cross-server name clash', async () => {
    mgr.addServer('s1', serverConfig({ toolName: 'shared' }));
    mgr.addServer('s2', serverConfig({ toolName: 'shared' }));
    await mgr.ensureConnected('s1');
    await mgr.ensureConnected('s2');
    assert.equal(mgr.getAllTools().length, 2);
    const conflicts = mgr.getToolConflicts();
    assert.equal(conflicts.length, 1);
    assert.equal(conflicts[0].toolName, 'shared');
    assert.deepEqual(conflicts[0].servers.sort(), ['s1', 's2']);
  });
  it('reports no conflicts for distinct tool names', async () => {
    mgr.addServer('s1', serverConfig({ toolName: 'alpha' }));
    await mgr.ensureConnected('s1');
    assert.deepEqual(mgr.getToolConflicts(), []);
  });
  it('uses server-provided annotations when present', async () => {
    mgr.addServer('s', serverConfig({ toolName: 'doThing', annotations: { readOnlyHint: true, destructiveHint: false } }));
    await mgr.ensureConnected('s');
    const a = mgr.getToolAnnotations('s:doThing');
    assert.equal(a.readOnlyHint, true);
    assert.equal(a.destructiveHint, false);
  });
  it('infers read-only, destructive, and open-world hints from name prefixes', () => {
    assert.equal(mgr.getToolAnnotations('getUser').readOnlyHint, true);
    assert.equal(mgr.getToolAnnotations('srv:deleteUser').destructiveHint, true);
    assert.equal(mgr.getToolAnnotations('webSearch').openWorldHint, true);
  });
  it('returns all-undefined hints for a neutral name', () => {
    assert.deepEqual(mgr.getToolAnnotations('frobnicate'), {
      readOnlyHint: undefined, destructiveHint: undefined, openWorldHint: undefined,
    });
  });
});

// ---------------------------------------------------------------------------
// tools-changed / resources-changed / prompts-changed refresh wiring
// ---------------------------------------------------------------------------
describe('change-notification wiring', () => {
  it('refreshes tools and re-emits on a tools/list_changed notification', async () => {
    mgr.addServer('n', serverConfig({ notify: true }));
    const changed = once(mgr, 'tools-changed');
    const resChanged = once(mgr, 'resources-changed');
    const promptChanged = once(mgr, 'prompts-changed');
    await mgr.ensureConnected('n');
    await Promise.all([changed, resChanged, promptChanged]);
    // refreshToolsForServer runs async after the emit; let it settle
    await new Promise(r => setTimeout(r, 50));
    // after refresh the tool is still present
    assert.equal(mgr.getToolsForServer('n').length, 1);
  });
});

// ---------------------------------------------------------------------------
// health checks
// ---------------------------------------------------------------------------
describe('health checks', () => {
  it('start is idempotent and stop clears the interval', () => {
    mgr.startHealthChecks(10000);
    mgr.startHealthChecks(10000); // early-return second call
    assert.doesNotThrow(() => mgr.stopHealthChecks());
    assert.doesNotThrow(() => mgr.stopHealthChecks()); // no-op when already stopped
  });
  it('pings ready servers, skips non-ready, and schedules reconnect on failure', async () => {
    mgr.addServer('ready', serverConfig());          // ping ok
    await mgr.ensureConnected('ready');
    mgr.addServer('idle', serverConfig());           // disconnected -> skipped
    mgr.addServer('pf', serverConfig({ pingError: true }));
    await mgr.ensureConnected('pf');                 // ready but ping errors
    mgr.startHealthChecks(10);
    await new Promise(r => setTimeout(r, 80));
    mgr.stopHealthChecks();
    // the failing server triggered scheduleReconnect; removeServer clears its timer
    assert.doesNotThrow(() => mgr.removeServer('pf'));
  });
});

// ---------------------------------------------------------------------------
// removeServer / disconnectAll
// ---------------------------------------------------------------------------
describe('removeServer / disconnectAll', () => {
  it('removes a connected server and its tools', async () => {
    mgr.addServer('x', serverConfig());
    await mgr.ensureConnected('x');
    assert.equal(mgr.getToolsForServer('x').length, 1);
    mgr.removeServer('x');
    assert.equal(mgr.getServerStatus('x'), null);
    assert.equal(mgr.getToolsForServer('x').length, 0);
  });
  it('removeServer is safe for an unknown name', () => {
    assert.doesNotThrow(() => mgr.removeServer('never-existed'));
  });
  it('disconnectAll clears clients, tools, pending, and denied list', async () => {
    mgr.setAllowedServers(['x', 'y']);
    mgr.addServer('x', serverConfig());
    await mgr.ensureConnected('x');
    mgr.addPendingServer('y', serverConfig());
    assert.throws(() => mgr.addServer('denied', serverConfig()));
    assert.deepEqual(mgr.getDeniedServers(), ['denied']);
    assert.ok(mgr.getAllTools().length >= 1);
    mgr.disconnectAll();
    // disconnectAll clears tool cache, pending servers, and the denied list
    assert.deepEqual(mgr.getAllTools(), []);
    assert.deepEqual(mgr.getDeniedServers(), []);
    assert.equal(mgr.listAllServers().some(s => s.name === 'y'), false); // pending cleared
  });
});

// ---------------------------------------------------------------------------
// idle eviction
// ---------------------------------------------------------------------------
describe('idle eviction', () => {
  it('startIdleChecker replaces an existing timer and stopIdleChecker clears it', () => {
    mgr.startIdleChecker(10000);
    mgr.startIdleChecker(10000); // stops the previous one first
    assert.doesNotThrow(() => mgr.stopIdleChecker());
    assert.doesNotThrow(() => mgr.stopIdleChecker()); // no-op when null
  });
  it('_evictIdleServers stops only servers idle past the timeout', async () => {
    mgr.addServer('old', serverConfig());
    await mgr.ensureConnected('old');
    mgr.addServer('fresh', serverConfig());
    await mgr.ensureConnected('fresh');
    // 'old' is stale, 'fresh' just used, and a never-used server is skipped
    mgr._lastUsedAt.set('old', Date.now() - 10 * 60 * 1000);
    mgr._lastUsedAt.set('fresh', Date.now());
    mgr.addServer('nouse', serverConfig());
    mgr._lastUsedAt.delete('nouse'); // no timestamp -> skipped
    mgr._idleTimeoutMs = 60 * 1000;
    const evicted = mgr._evictIdleServers();
    assert.deepEqual(evicted, ['old']);
    assert.equal(mgr.getServerState('fresh'), 'ready');
  });
});

// ---------------------------------------------------------------------------
// loadFromConfig / expandConfigEnv / findMcpConfigPaths
// ---------------------------------------------------------------------------
describe('loadFromConfig', () => {
  it('returns silently when the path does not exist', () => {
    assert.doesNotThrow(() => mgr.loadFromConfig(path.join(tmp(), 'nope.json')));
    assert.deepEqual(mgr.listAllServers(), []);
  });
  it('loads mcpServers, strips comments, expands env, and skips entries with no command/url', () => {
    process.env.MCP_CFG_CMD = process.execPath;
    const dir = tmp();
    const file = path.join(dir, 'mcp.json');
    fs.writeFileSync(file, [
      '// leading comment line',
      '{',
      '  "mcpServers": {',
      '    "good": { "command": "${MCP_CFG_CMD}", "args": ["--x"], "url": "http://${MCP_CFG_CMD}", "env": { "K": "${MCP_CFG_CMD}" } },',
      '    "skipme": { "autoConnect": true }',
      '  }',
      '}',
    ].join('\n'));
    try {
      mgr.loadFromConfig(file);
      const names = mgr.listAllServers().map(s => s.name);
      assert.deepEqual(names, ['good']); // skipme omitted (no command/url)
      // re-adding proves it was registered via loadFromConfig
      assert.throws(() => mgr.addServer('good', serverConfig()), /already registered/);
    } finally {
      delete process.env.MCP_CFG_CMD;
    }
  });
  it('accepts a top-level server map without an mcpServers wrapper', () => {
    const file = path.join(tmp(), 'mcp.json');
    fs.writeFileSync(file, JSON.stringify({ direct: { command: process.execPath } }));
    mgr.loadFromConfig(file);
    assert.deepEqual(mgr.listAllServers().map(s => s.name), ['direct']);
  });
  it('returns when the servers value is not an object', () => {
    const file = path.join(tmp(), 'mcp.json');
    fs.writeFileSync(file, JSON.stringify({ mcpServers: 'not-an-object' }));
    mgr.loadFromConfig(file);
    assert.deepEqual(mgr.listAllServers(), []);
  });
  it('swallows invalid JSON', () => {
    const file = path.join(tmp(), 'mcp.json');
    fs.writeFileSync(file, '{ this is not json ');
    assert.doesNotThrow(() => mgr.loadFromConfig(file));
    assert.deepEqual(mgr.listAllServers(), []);
  });
});

describe('findMcpConfigPaths', () => {
  it('returns project and global config paths when both exist', () => {
    const cwd = tmp();
    const home = tmp();
    fs.mkdirSync(path.join(cwd, '.superinference'), { recursive: true });
    fs.writeFileSync(path.join(cwd, '.superinference', 'mcp.json'), '{}');
    fs.mkdirSync(path.join(home, '.superinference'), { recursive: true });
    fs.writeFileSync(path.join(home, '.superinference', 'mcp.json'), '{}');
    const prevHome = process.env.HOME;
    const prevUser = process.env.USERPROFILE;
    process.env.HOME = home;
    delete process.env.USERPROFILE;
    try {
      const paths = findMcpConfigPaths(cwd);
      assert.equal(paths.length, 2);
      assert.ok(paths[0].startsWith(cwd));
      assert.ok(paths[1].startsWith(home));
    } finally {
      if (prevHome === undefined) delete process.env.HOME; else process.env.HOME = prevHome;
      if (prevUser !== undefined) process.env.USERPROFILE = prevUser;
    }
  });
  it('returns an empty array when no config files exist', () => {
    const cwd = tmp();
    const home = tmp();
    const prevHome = process.env.HOME;
    const prevUser = process.env.USERPROFILE;
    process.env.HOME = home;
    delete process.env.USERPROFILE;
    try {
      assert.deepEqual(findMcpConfigPaths(cwd), []);
    } finally {
      if (prevHome === undefined) delete process.env.HOME; else process.env.HOME = prevHome;
      if (prevUser !== undefined) process.env.USERPROFILE = prevUser;
    }
  });
});

// ---------------------------------------------------------------------------
// exported constants
// ---------------------------------------------------------------------------
describe('exported limits', () => {
  it('exposes tool-count and name-length ceilings', () => {
    assert.equal(MAX_MCP_TOOLS_PER_SERVER, 200);
    assert.equal(MAX_MCP_TOOL_NAME_LENGTH, 128);
  });
});
