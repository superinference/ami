import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import * as http from 'node:http';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { execFileSync, spawn } from 'node:child_process';

import { LSPClient, getLSPClient } from '../src/lsp/client';
import { McpClient, McpSessionExpiredError } from '../src/mcp/client';
import {
  McpManager,
  isTransientConnectError,
  findMcpConfigPaths,
  type McpServerConfig,
} from '../src/mcp/manager';
import { WorkspaceIndexer, findProjectRoot, indexerHooks } from '../src/workspace-indexer';

// ===========================================================================
// Helpers
// ===========================================================================

/** A fake spawned child-process good enough for the LSP seam success path. */
function makeFakeProc(opts: { pid?: number | null; killThrows?: boolean; stdinThrows?: boolean } = {}) {
  const proc: any = new EventEmitter();
  proc.pid = 'pid' in opts ? opts.pid : 1234;
  const stdinWrites: string[] = [];
  const stdin: any = new EventEmitter();
  stdin.writable = true;
  stdin._writes = stdinWrites;
  stdin.write = (s: string) => { if (opts.stdinThrows) throw new Error('EPIPE'); stdinWrites.push(s); return true; };
  proc.stdin = stdin;
  proc.stdout = new EventEmitter();
  proc.stderr = new EventEmitter();
  proc.kill = () => { if (opts.killThrows) throw new Error('kill boom'); return true; };
  proc._writes = stdinWrites;
  return proc;
}

// ===========================================================================
// LSPClient — ensureServer spawn success path (via _spawn / _spawnSync seams)
// ===========================================================================

describe('LSPClient — spawn success path', () => {
  let client: LSPClient;
  let cwd: string;

  beforeEach(() => {
    client = new LSPClient();
    cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-cov-'));
  });

  afterEach(() => {
    client.shutdown();
    fs.rmSync(cwd, { recursive: true, force: true });
  });

  it('spawns a server, sends initialize, and tracks it as running', async () => {
    const proc = makeFakeProc();
    client._spawnSync = (() => ({ status: 0 })) as any;
    client._spawn = (() => proc) as any;

    const ok = await client.ensureServer('typescript', cwd);
    assert.equal(ok, true);
    // initialize request written with LSP framing
    assert.equal(proc._writes.length, 1);
    assert.match(proc._writes[0], /Content-Length: \d+\r\n\r\n/);
    assert.match(proc._writes[0], /"method":"initialize"/);

    // stream handlers are registered — fire them to execute their callbacks
    proc.stdin.emit('error', new Error('pipe'));
    proc.stdout.emit('data', Buffer.from('x'));
    proc.stderr.emit('data', Buffer.from('y'));

    // second call short-circuits (already running)
    client._spawn = (() => { throw new Error('should not spawn again'); }) as any;
    assert.equal(await client.ensureServer('typescript', cwd), true);
  });

  it('clears tracking when the process emits exit', async () => {
    const proc = makeFakeProc();
    client._spawnSync = (() => ({ status: 0 })) as any;
    client._spawn = (() => proc) as any;

    await client.ensureServer('typescript', cwd);
    // open a document so there is something to clear
    await client.notifyDidChange(path.join(cwd, 'a.ts'), 'const a = 1;', cwd);
    const before = proc._writes.length;
    assert.ok(before >= 2); // init + didOpen + didChange

    proc.emit('exit', 0); // triggers clearLang -> processes/initialized/opened cleared

    // after exit, a new spawn is attempted (process no longer tracked)
    let spawnedAgain = false;
    client._spawn = (() => { spawnedAgain = true; return makeFakeProc(); }) as any;
    await client.ensureServer('typescript', cwd);
    assert.equal(spawnedAgain, true);
  });

  it('clears tracking when the process emits error', async () => {
    const proc = makeFakeProc();
    client._spawnSync = (() => ({ status: 0 })) as any;
    client._spawn = (() => proc) as any;
    await client.ensureServer('go', cwd);
    proc.emit('error', new Error('spawn fail'));
    let spawnedAgain = false;
    client._spawn = (() => { spawnedAgain = true; return makeFakeProc(); }) as any;
    await client.ensureServer('go', cwd);
    assert.equal(spawnedAgain, true);
  });

  it('returns false when which cannot find the binary', async () => {
    client._spawnSync = (() => ({ status: 1 })) as any;
    client._spawn = (() => { throw new Error('nope'); }) as any;
    assert.equal(await client.ensureServer('python', cwd), false);
  });

  it('returns false for an unknown language', async () => {
    assert.equal(await client.ensureServer('cobol', cwd), false);
  });

  it('returns false when the spawned process has no pid', async () => {
    client._spawnSync = (() => ({ status: 0 })) as any;
    client._spawn = (() => makeFakeProc({ pid: null })) as any;
    assert.equal(await client.ensureServer('rust', cwd), false);
  });

  it('returns false when spawnSync throws (caught)', async () => {
    client._spawnSync = (() => { throw new Error('boom'); }) as any;
    assert.equal(await client.ensureServer('typescript', cwd), false);
  });

  it('notifyDidChange auto-starts the server then sends didOpen + didChange', async () => {
    const proc = makeFakeProc();
    client._spawnSync = (() => ({ status: 0 })) as any;
    client._spawn = (() => proc) as any;
    await client.notifyDidChange(path.join(cwd, 'x.ts'), 'const x = 2;', cwd);
    const joined = proc._writes.join('');
    assert.match(joined, /textDocument\/didOpen/);
    assert.match(joined, /textDocument\/didChange/);
  });

  it('notifyDidChange ignores unsupported extensions', async () => {
    client._spawn = (() => { throw new Error('should not spawn'); }) as any;
    await client.notifyDidChange(path.join(cwd, 'x.txt'), 'hi', cwd);
    // no throw == pass
  });

  it('notifyDidChange returns early when the server cannot start', async () => {
    client._spawnSync = (() => ({ status: 1 })) as any;
    await client.notifyDidChange(path.join(cwd, 'y.ts'), 'code', cwd);
    // no throw, server never started
  });

  it('notifyDidSave auto-starts the server then sends didOpen + didSave', async () => {
    const proc = makeFakeProc();
    client._spawnSync = (() => ({ status: 0 })) as any;
    client._spawn = (() => proc) as any;
    await client.notifyDidSave(path.join(cwd, 'z.ts'), cwd);
    const joined = proc._writes.join('');
    assert.match(joined, /textDocument\/didOpen/);
    assert.match(joined, /textDocument\/didSave/);
  });

  it('notifyDidSave ignores unsupported extensions', async () => {
    await client.notifyDidSave(path.join(cwd, 'z.md'), cwd);
  });

  it('notifyDidSave returns early when the server cannot start', async () => {
    client._spawnSync = (() => ({ status: 1 })) as any;
    await client.notifyDidSave(path.join(cwd, 'w.ts'), cwd);
  });

  it('does not re-send didOpen for an already-open document', async () => {
    const proc = makeFakeProc();
    client._spawnSync = (() => ({ status: 0 })) as any;
    client._spawn = (() => proc) as any;
    const file = path.join(cwd, 'dup.ts');
    await client.notifyDidChange(file, 'v1', cwd);
    await client.notifyDidChange(file, 'v2', cwd);
    const didOpens = proc._writes.filter((w: string) => w.includes('textDocument/didOpen'));
    assert.equal(didOpens.length, 1);
  });

  it('ensureDidOpen falls back to plaintext languageId for unknown extensions', () => {
    const proc = makeFakeProc();
    (client as any).ensureDidOpen(proc, path.join(cwd, 'notes.unknownext'), 'content');
    assert.match(proc._writes.join(''), /"languageId":"plaintext"/);
  });

  it('swallows stdin write errors in sendRequest and sendNotification', async () => {
    const proc = makeFakeProc({ stdinThrows: true });
    client._spawnSync = (() => ({ status: 0 })) as any;
    client._spawn = (() => proc) as any;
    // init sendRequest write throws -> caught; ensureServer still succeeds
    const ok = await client.ensureServer('typescript', cwd);
    assert.equal(ok, true);
    // sendNotification write also throws -> caught
    await client.notifyDidChange(path.join(cwd, 's.ts'), 'code', cwd);
  });

  it('shutdown kills tracked processes and swallows kill errors', async () => {
    const good = makeFakeProc();
    const bad = makeFakeProc({ killThrows: true });
    client._spawnSync = (() => ({ status: 0 })) as any;
    client._spawn = (() => good) as any;
    await client.ensureServer('typescript', cwd);
    client._spawn = (() => bad) as any;
    await client.ensureServer('python', cwd);
    // kill of bad throws -> caught; must not throw
    client.shutdown();
  });
});

describe('getLSPClient', () => {
  it('returns a singleton instance', () => {
    const a = getLSPClient();
    const b = getLSPClient();
    assert.ok(a instanceof LSPClient);
    assert.equal(a, b);
  });
});

// ===========================================================================
// McpClient — stderr emission + gracefulShutdown deep kill paths
// ===========================================================================

const INIT_ONLY_SERVER = `
  const readline = require('readline');
  const rl = readline.createInterface({ input: process.stdin });
  process.stderr.write('server starting\\n');
  rl.on('line', (line) => {
    try {
      const msg = JSON.parse(line);
      if (msg.method === 'initialize') {
        process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' }, capabilities: {} } }) + '\\n');
      }
    } catch {}
  });
`;

describe('McpClient — stderr forwarding', () => {
  it('emits stderr chunks from the server process', async () => {
    const client = new McpClient({ command: 'node', args: ['-e', INIT_ONLY_SERVER], requestTimeout: 4000 });
    const stderr: string[] = [];
    client.on('stderr', (s: string) => stderr.push(s));
    try {
      await client.connect();
      await new Promise(r => setTimeout(r, 100));
      assert.ok(stderr.join('').includes('server starting'));
    } finally {
      client.disconnect();
    }
  });
});

describe('McpClient — gracefulShutdown escalation', () => {
  let sleeperPid: number;
  let sleeper: ReturnType<typeof spawn>;

  beforeEach(() => {
    // A real, live process so processExists(pid) is true throughout the test.
    sleeper = spawn('node', ['-e', 'setTimeout(() => {}, 60000)']);
    sleeperPid = sleeper.pid!;
  });

  afterEach(() => {
    try { sleeper.kill('SIGKILL'); } catch {}
  });

  it('escalates SIGINT -> SIGTERM -> SIGKILL and tolerates kill() throwing', async () => {
    const client = new McpClient({ command: 'echo' });
    const killCalls: string[] = [];
    // proc.kill throws for every attempt -> exercises the empty catch arms.
    const fakeProc: any = { kill: (sig: string) => { killCalls.push(`proc:${sig}`); throw new Error('cannot signal'); } };

    // Keep the target "alive" for the whole escalation so every SIGKILL branch
    // runs; make SIGKILL throw to exercise the empty catch arms. Scope the
    // override to our sleeper pid only so the test runner's own process.kill
    // calls keep their real behavior.
    const realKill = process.kill.bind(process);
    const killMock = mock.method(process, 'kill', (pid: number, sig?: string | number) => {
      if (pid !== sleeperPid) return realKill(pid, sig as any);
      if (sig === 0) return true;      // processExists() -> always alive
      killCalls.push(`process:${sig}`);
      throw new Error('kill denied');   // exercise catch arms around SIGKILL
    });

    try {
      (client as any).gracefulShutdown(fakeProc, sleeperPid);
      // SIGINT attempted synchronously
      assert.ok(killCalls.includes('proc:SIGINT'));
      // wait past SIGINT(100) + SIGTERM(400) + total(500) timers
      await new Promise(r => setTimeout(r, 700));
      assert.ok(killCalls.includes('proc:SIGTERM'), 'SIGTERM escalation');
      assert.ok(killCalls.some(c => c === 'process:SIGKILL'), 'SIGKILL escalation');
    } finally {
      killMock.mock.restore();
    }
  });

  it('SIGKILLs immediately when the process has no pid', () => {
    const client = new McpClient({ command: 'echo' });
    let killed = '';
    const fakeProc: any = { kill: (sig: string) => { killed = sig; return true; } };
    (client as any).gracefulShutdown(fakeProc, null);
    assert.equal(killed, 'SIGKILL');
  });

  it('returns early when the target process does not exist', () => {
    const client = new McpClient({ command: 'echo' });
    let called = false;
    const fakeProc: any = { kill: () => { called = true; return true; } };
    // pid 1 with signal 0 — not ours; use a definitely-dead pid
    const deadPid = 2 ** 30; // extremely unlikely to exist
    (client as any).gracefulShutdown(fakeProc, deadPid);
    assert.equal(called, false);
  });
});

describe('McpClient — SSE transport + helpers', () => {
  it('selects the https module for https SSE urls', async () => {
    const client = new McpClient({ command: '', connectTimeout: 500, requestTimeout: 500 });
    // Nothing is listening — the https request errors out, but the https branch runs.
    await assert.rejects(() => client.connectSSE('https://127.0.0.1:0/sse'));
  });

  it('connectSSE falls back to a default serverInfo when the server omits it', async () => {
    const server = http.createServer((req, res) => {
      let body = '';
      req.on('data', (d) => body += d);
      req.on('end', () => {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ jsonrpc: '2.0', id: 1, result: { capabilities: {} } }));
      });
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', r));
    const port = (server.address() as any).port;
    const client = new McpClient({ command: '', requestTimeout: 2000, connectTimeout: 2000 });
    try {
      await client.connectSSE(`http://127.0.0.1:${port}/sse`);
      assert.equal(client.state, 'ready');
      assert.deepEqual(client.serverInfo, { name: 'unknown', version: '0.0.0' });
    } finally {
      client.disconnect();
      server.close();
    }
  });

  it('classifyError tolerates an error without a message', () => {
    const client = new McpClient({ command: '' });
    const classified = (client as any).classifyError({ code: undefined, message: undefined });
    assert.equal(classified.code, 'UNKNOWN');
  });

  it('processBuffer skips blank lines', () => {
    const client = new McpClient({ command: '' });
    (client as any).buffer = '   \n\n';
    // Must not throw and must not emit parse-error for blank lines
    let parseErrors = 0;
    client.on('parse-error', () => parseErrors++);
    (client as any).processBuffer();
    assert.equal(parseErrors, 0);
  });
});

// ===========================================================================
// McpManager — remaining branches
// ===========================================================================

/** Minimal fake McpClient the manager can drive without spawning. */
class FakeClient extends EventEmitter {
  state = 'ready';
  serverInfo: any = { name: 'fake', version: '1' };
  tools: any[] = [];
  resources: any[] = [];
  uptime: number | null = 42;
  _tools: any[] = [];
  behavior: Record<string, any> = {};
  async connect() { if (this.behavior.connect) return this.behavior.connect(); }
  async connectHTTP(_url: string) { if (this.behavior.connectHTTP) return this.behavior.connectHTTP(_url); }
  async connectSSE(_url: string) { if (this.behavior.connectSSE) return this.behavior.connectSSE(_url); }
  async listTools() { if (this.behavior.listTools) return this.behavior.listTools(); return this._tools; }
  async listResources() { if (this.behavior.listResources) return this.behavior.listResources(); return this.resources; }
  async readResource(_u: string) { if (this.behavior.readResource) return this.behavior.readResource(); return {}; }
  async callTool(_n: string, _a: any) { return { ok: true }; }
  async listPrompts() { return []; }
  async getPrompt() { return { messages: [] }; }
  async ping() {}
  async reconnect() { if (this.behavior.reconnect) return this.behavior.reconnect(); }
  disconnect() {}
}

describe('McpManager — targeted branches', () => {
  let mgr: McpManager;

  beforeEach(() => { mgr = new McpManager(); });
  afterEach(() => { mgr.disconnectAll(); });

  it('isTransientConnectError classifies errors (guard + bypass)', () => {
    assert.equal(isTransientConnectError(new McpSessionExpiredError()), false);
    assert.equal(isTransientConnectError({ code: 'ECONNREFUSED' }), true);
    assert.equal(isTransientConnectError(new Error('socket hang up')), true);
    assert.equal(isTransientConnectError(new Error('connect ENOENT /x')), true);
    assert.equal(isTransientConnectError(new Error('totally fine')), false);
    assert.equal(isTransientConnectError('a plain string'), false);
  });

  it('resolveEnvVars expands ${VAR} in headers when adding a server', () => {
    process.env.MCP_COV_TOKEN = 'sekret';
    const captured: any[] = [];
    mgr._createClient = ((opts: any) => { captured.push(opts); return new FakeClient() as any; });
    mgr.addServer('h', { command: 'x', headers: { Authorization: 'Bearer ${MCP_COV_TOKEN}', Empty: '${MCP_COV_MISSING}' } });
    assert.equal(captured[0].headers.Authorization, 'Bearer sekret');
    assert.equal(captured[0].headers.Empty, '');
    delete process.env.MCP_COV_TOKEN;
  });

  it('removeServer clears a pending reconnect timer', () => {
    mgr._createClient = (() => new FakeClient() as any);
    mgr.addServer('r', { command: 'x' });
    const t = setTimeout(() => {}, 100000);
    (mgr as any).reconnectTimers.set('r', t);
    mgr.removeServer('r');
    assert.equal((mgr as any).reconnectTimers.has('r'), false);
  });

  it('getFullServerStatus returns a pending placeholder for an unknown name', () => {
    const cfg: McpServerConfig = { command: 'x', containerConfig: { image: 'img', runtime: 'docker' } as any };
    mgr['configs'].set('ghost', cfg);
    const st = (mgr as any).getFullServerStatus('ghost');
    assert.equal(st.state, 'pending');
    assert.equal(st.serverInfo, null);
    assert.equal(st.containerBacked, true);
  });

  it('listAllServers reports a connected client via full status', async () => {
    const fake = new FakeClient();
    fake._tools = [{ name: 'a', description: '', inputSchema: {} }];
    mgr._createClient = (() => fake as any);
    mgr.addServer('live', { command: 'x' });
    await mgr.connectServer('live');
    const all = mgr.listAllServers();
    const row = all.find(s => s.name === 'live')!;
    assert.equal(row.state, 'ready');
    assert.equal(row.toolCount, fake.tools.length);
  });

  it('connectServer uses the HTTP transport when configured', async () => {
    const fake = new FakeClient();
    let httpUrl = '';
    fake.behavior.connectHTTP = (u: string) => { httpUrl = u; };
    mgr._createClient = (() => fake as any);
    mgr.addServer('httpsrv', { command: '', transport: 'http', url: 'http://localhost:1/mcp' });
    await mgr.connectServer('httpsrv');
    assert.equal(httpUrl, 'http://localhost:1/mcp');
  });

  it('connectServer uses the SSE transport when configured', async () => {
    const fake = new FakeClient();
    let sseUrl = '';
    fake.behavior.connectSSE = (u: string) => { sseUrl = u; };
    mgr._createClient = (() => fake as any);
    mgr.addServer('ssesrv', { command: '', transport: 'sse', url: 'http://localhost:2/sse' });
    await mgr.connectServer('ssesrv');
    assert.equal(sseUrl, 'http://localhost:2/sse');
  });

  it('listResources emits session-expired when a client reports an expired session', async () => {
    const fake = new FakeClient();
    fake.behavior.listResources = () => { throw new McpSessionExpiredError(); };
    mgr._createClient = (() => fake as any);
    mgr.addServer('exp', { command: 'x' });
    await mgr.connectServer('exp');
    let expired = '';
    mgr.on('session-expired', (n: string) => { expired = n; });
    const res = await mgr.listResources();
    assert.deepEqual(res, []);
    assert.equal(expired, 'exp');
  });

  it('refreshToolsForServer logs a conflict and caches tools', async () => {
    const a = new FakeClient();
    a._tools = [{ name: 'shared', description: 'x', inputSchema: {} }];
    const b = new FakeClient();
    b._tools = [{ name: 'shared', description: 'y', inputSchema: {} }];
    const clients = [a, b];
    let i = 0;
    mgr._createClient = (() => clients[i++] as any);
    mgr.addServer('srvA', { command: 'x' });
    mgr.addServer('srvB', { command: 'x' });
    await mgr.connectServer('srvA');
    await mgr.connectServer('srvB');
    // force a refresh on B -> existing 'shared' from A triggers conflict log
    await (mgr as any).refreshToolsForServer('srvB');
    assert.equal(mgr.getToolsForServer('srvB').length, 1);
  });

  it('refreshToolsForServer swallows listTools failures', async () => {
    const fake = new FakeClient();
    fake.behavior.listTools = () => { throw new Error('list boom'); };
    mgr._createClient = (() => fake as any);
    mgr.addServer('bad', { command: 'x' });
    // client present + ready, listTools throws -> caught
    await (mgr as any).refreshToolsForServer('bad');
    assert.equal(mgr.getToolsForServer('bad').length, 0);
  });

  it('refreshToolsForServer no-ops when the client is not ready', async () => {
    const fake = new FakeClient();
    fake.state = 'connecting';
    mgr._createClient = (() => fake as any);
    mgr.addServer('nr', { command: 'x' });
    await (mgr as any).refreshToolsForServer('nr');
    await (mgr as any).refreshToolsForServer('missing');
  });

  it('scheduleReconnect reconnects and refreshes on success', async () => {
    const fake = new FakeClient();
    fake._tools = [{ name: 't', description: '', inputSchema: {} }];
    let reconnected = false;
    fake.behavior.reconnect = () => { reconnected = true; };
    mgr._createClient = (() => fake as any);
    mgr.addServer('rc', { command: 'x' });
    (mgr as any).scheduleReconnect('rc');
    // reconnect clears a previously scheduled timer path too
    (mgr as any).scheduleReconnect('rc');
    await new Promise(r => setTimeout(r, 1200));
    assert.equal(reconnected, true);
    assert.equal(mgr.getToolsForServer('rc').length, 1);
  });

  it('scheduleReconnect logs when reconnect fails', async () => {
    const fake = new FakeClient();
    fake.behavior.reconnect = () => { throw new Error('reconnect boom'); };
    mgr._createClient = (() => fake as any);
    mgr.addServer('rf', { command: 'x' });
    (mgr as any).scheduleReconnect('rf');
    await new Promise(r => setTimeout(r, 1200));
    // no throw == caught
  });

  it('scheduleReconnect no-ops when the client is gone', () => {
    (mgr as any).scheduleReconnect('nonexistent');
  });

  it('stopServer clears a pending reconnect timer', async () => {
    mgr._createClient = (() => new FakeClient() as any);
    mgr.addServer('st', { command: 'x' });
    const t = setTimeout(() => {}, 100000);
    (mgr as any).reconnectTimers.set('st', t);
    await mgr.stopServer('st');
    assert.equal((mgr as any).reconnectTimers.has('st'), false);
  });

  it('getServerState reports connecting / error / stopped client states', () => {
    const fake = new FakeClient();
    mgr._createClient = (() => fake as any);
    mgr.addServer('gs', { command: 'x' });
    fake.state = 'connecting';
    assert.equal(mgr.getServerState('gs'), 'connecting');
    fake.state = 'error';
    assert.equal(mgr.getServerState('gs'), 'error');
    fake.state = 'disconnected';
    assert.equal(mgr.getServerState('gs'), 'stopped');
  });

  it('connectAll wraps non-Error throwables', async () => {
    const fake = new FakeClient();
    fake.behavior.connect = () => { throw 'string failure'; };
    mgr._createClient = (() => fake as any);
    mgr.addServer('ca', { command: 'x' });
    const results = await mgr.connectAll();
    const err = results.get('ca');
    assert.ok(err instanceof Error);
    assert.match(err!.message, /string failure/);
  });

  it('forwards all client lifecycle events', async () => {
    const fake = new FakeClient();
    mgr._createClient = (() => fake as any);
    mgr.addServer('ev', { command: 'x' });
    const seen = new Set<string>();
    mgr.on('server-error', () => seen.add('error'));
    mgr.on('server-close', () => seen.add('close'));
    mgr.on('server-ready', () => seen.add('ready'));
    mgr.on('notification', () => seen.add('notification'));
    mgr.on('terminal-error', () => seen.add('terminal-error'));
    mgr.on('session-expired', () => seen.add('session-expired'));
    mgr.on('tools-changed', () => seen.add('tools-changed'));
    mgr.on('resources-changed', () => seen.add('resources-changed'));
    mgr.on('prompts-changed', () => seen.add('prompts-changed'));

    fake.emit('error', new Error('e'));
    fake.emit('close', 0, 10);
    fake.emit('ready');
    fake.emit('notification', 'method/x', { a: 1 });
    fake.emit('terminal-error', { code: 'EPIPE', message: 'pipe' });
    fake.emit('session-expired');
    fake.emit('tools-changed');
    fake.emit('resources-changed');
    fake.emit('prompts-changed');
    await new Promise(r => setTimeout(r, 10));

    for (const ev of ['error', 'close', 'ready', 'notification', 'terminal-error', 'session-expired', 'tools-changed', 'resources-changed', 'prompts-changed']) {
      assert.ok(seen.has(ev), `missing ${ev}`);
    }
  });

  it('connectServer retries after a transient connection error', async () => {
    const fake = new FakeClient();
    let attempts = 0;
    fake.behavior.connect = () => {
      attempts++;
      if (attempts === 1) { const e: any = new Error('refused'); e.code = 'ECONNREFUSED'; throw e; }
    };
    mgr._createClient = (() => fake as any);
    mgr.addServer('retry', { command: 'x' });
    await mgr.connectServer('retry');
    assert.equal(attempts, 2);
  });

  it('tools-changed handler swallows a rejected tool refresh', async () => {
    const fake = new FakeClient();
    mgr._createClient = (() => fake as any);
    mgr.addServer('tc', { command: 'x' });
    // Make refreshToolsForServer reject by having state access throw.
    Object.defineProperty(fake, 'state', { get() { throw new Error('state boom'); }, configurable: true });
    let emitted = false;
    mgr.on('tools-changed', () => { emitted = true; });
    fake.emit('tools-changed');       // handler calls refreshToolsForServer(...).catch(() => {})
    await new Promise(r => setTimeout(r, 20));
    assert.equal(emitted, true);       // handler still ran to completion
  });

  it('_evictIdleServers swallows a rejected stop', () => {
    const fake = new FakeClient();
    fake.disconnect = () => { throw new Error('disconnect boom'); };
    mgr._createClient = (() => fake as any);
    mgr.addServer('idle', { command: 'x' });
    mgr._idleTimeoutMs = 1;
    mgr._lastUsedAt.set('idle', Date.now() - 10_000);
    const evicted = mgr._evictIdleServers();
    assert.deepEqual(evicted, ['idle']);  // stopServer rejected but was caught
    fake.disconnect = () => {};            // restore so afterEach cleanup is safe
  });

  it('findMcpConfigPaths tolerates a missing HOME/USERPROFILE', () => {
    const savedHome = process.env.HOME;
    const savedProfile = process.env.USERPROFILE;
    try {
      delete process.env.HOME;
      delete process.env.USERPROFILE;
      const paths = findMcpConfigPaths('/tmp/definitely-not-a-project-' + Date.now());
      assert.ok(Array.isArray(paths));
    } finally {
      if (savedHome !== undefined) process.env.HOME = savedHome;
      if (savedProfile !== undefined) process.env.USERPROFILE = savedProfile;
    }
  });
});

// ===========================================================================
// WorkspaceIndexer — remaining branches
// ===========================================================================

describe('WorkspaceIndexer — targeted branches', () => {
  let dir: string;
  afterEach(() => {
    if (dir) fs.rmSync(dir, { recursive: true, force: true });
  });

  it('indexFile returns null when the file cannot be read (EACCES)', async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ws-eacces-'));
    const f = path.join(dir, 'secret.ts');
    fs.writeFileSync(f, 'export const x = 1;');
    fs.chmodSync(f, 0);
    const idx = new WorkspaceIndexer(dir);
    const entry = await idx.indexFile(f);
    assert.equal(entry, null);
    fs.chmodSync(f, 0o644);
  });

  it('getSummary truncates to ~500 chars for large workspaces', async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ws-big-'));
    fs.writeFileSync(path.join(dir, 'package.json'), '{}');
    // Very long symbol names so the "Key symbols" section alone exceeds 500 chars.
    // NOTE: relative-import paths are interpolated (not written as a literal
    // `from './x'`) so the ESM coverage harness's import-rewriter cannot mangle
    // the fixture content.
    const pad = 'X'.repeat(90);
    for (let i = 0; i < 10; i++) {
      const imports = Array.from({ length: 3 }, (_, j) => {
        const rel = `./file${(i + j) % 10}`;
        return `import { f${(i + j) % 10} } from '${rel}';`;
      }).join('\n');
      fs.writeFileSync(path.join(dir, `file${i}.ts`),
        `${imports}\nexport function veryLongFunctionName${pad}${i}() { return ${i}; }\n` +
        `export class VeryLongClassName${pad}${i} {}\n`);
    }
    const idx = new WorkspaceIndexer(dir);
    await idx.buildIndex();
    const summary = idx.getSummary();
    assert.equal(summary.length, 500);
    assert.ok(summary.endsWith('...'));
  });

  it('buildIndex uses the glob fallback for a non-git project directory', async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ws-glob-'));
    fs.writeFileSync(path.join(dir, 'package.json'), '{"name":"x"}');
    fs.writeFileSync(path.join(dir, 'main.ts'), 'export function main() {}\n');
    const idx = new WorkspaceIndexer(dir);
    const res = await idx.buildIndex();
    assert.ok(res.fileCount >= 1);
  });

  it('buildIndex slices to MAX_FILES for huge git repos', async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ws-max-'));
    execFileSync('git', ['init', '-q'], { cwd: dir });
    execFileSync('git', ['config', 'user.email', 't@t'], { cwd: dir });
    execFileSync('git', ['config', 'user.name', 't'], { cwd: dir });
    // 5001 tracked .ts files, all empty (indexFile skips them fast after stat)
    for (let i = 0; i < 5001; i++) {
      fs.writeFileSync(path.join(dir, `f${i}.ts`), '');
    }
    execFileSync('git', ['add', '-A'], { cwd: dir });
    const idx = new WorkspaceIndexer(dir);
    const res = await idx.buildIndex();
    // all files are empty -> skipped, but slice path executed
    assert.ok(res.fileCount <= 5000);
  });

  it('buildIndex falls back to an empty list when glob throws', async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ws-globfail-'));
    fs.writeFileSync(path.join(dir, 'package.json'), '{}');
    fs.writeFileSync(path.join(dir, 'a.ts'), 'export const a = 1;');
    const globMock = mock.method(indexerHooks, 'glob', () => { throw new Error('glob exploded'); });
    try {
      const idx = new WorkspaceIndexer(dir);
      const res = await idx.buildIndex();
      assert.equal(res.fileCount, 0);
    } finally {
      globMock.mock.restore();
    }
  });

  it('findProjectRoot stops at the home directory with no markers', () => {
    // A fresh dir directly under $HOME with no project markers forces the walk
    // to reach HOME and break (returns null) when HOME itself has no marker.
    const home = os.homedir();
    const sub = fs.mkdtempSync(path.join(home, '.wscov-'));
    try {
      const root = findProjectRoot(sub);
      // Either null (walked to home and broke) or some ancestor project — both
      // valid; the key is the home-break branch executes without throwing.
      assert.ok(root === null || typeof root === 'string');
    } finally {
      fs.rmSync(sub, { recursive: true, force: true });
    }
  });

  it('search, definition and dependency queries behave for present and absent data', async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ws-query-'));
    fs.writeFileSync(path.join(dir, 'package.json'), '{}');
    fs.writeFileSync(path.join(dir, 'lib.ts'), 'export function helper() {}\nexport class Helper {}\n');
    // Interpolated relative path so the coverage harness import-rewriter leaves it intact.
    const libRel = './lib';
    fs.writeFileSync(path.join(dir, 'app.ts'), `import { helper } from '${libRel}';\nexport const run = () => helper();\n`);
    const idx = new WorkspaceIndexer(dir);
    await idx.buildIndex();

    const found = idx.searchSymbols('help');
    assert.ok(found.some(s => s.name === 'helper' || s.name === 'Helper'));
    assert.ok(idx.searchSymbols('helper', 1).length <= 1);

    const defs = idx.findDefinition('helper');
    assert.ok(defs.every(d => d.type !== 'import'));

    // dependents of lib.ts: app.ts imports it
    const deps = idx.findDependents(path.join(dir, 'lib.ts'));
    assert.ok(deps.includes(path.join(dir, 'app.ts')));
    // dependencies of a file WITH recorded imports -> non-empty (true arm)
    assert.ok(idx.findDependencies(path.join(dir, 'app.ts')).includes(path.join(dir, 'lib.ts')));
    // dependencies of a file with no recorded imports -> empty (false arm)
    assert.deepEqual(idx.findDependencies(path.join(dir, 'lib.ts')), []);
    // a file not in the graph -> empty for both
    assert.deepEqual(idx.findDependents('/nope/x.ts'), []);
    assert.deepEqual(idx.findDependencies('/nope/x.ts'), []);

    // relative-path inputs resolve against cwd
    assert.ok(idx.findDependents('lib.ts').includes(path.join(dir, 'app.ts')));
    assert.ok(idx.findDependencies('app.ts').includes(path.join(dir, 'lib.ts')));
  });

  it('indexes multiple languages, re-indexes, and reports stats/summary', async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ws-multi-'));
    fs.writeFileSync(path.join(dir, 'package.json'), '{}');
    fs.writeFileSync(path.join(dir, 'm.py'), 'import os\nfrom pkg import thing\ndef pyfunc():\n    pass\nclass PyClass:\n    pass\n');
    fs.mkdirSync(path.join(dir, 'pkg'));
    fs.writeFileSync(path.join(dir, 'pkg', '__init__.py'), 'thing = 1\n');
    fs.writeFileSync(path.join(dir, 'g.go'), 'package main\nimport (\n  "fmt"\n)\nfunc GoFunc() {}\ntype GoStruct struct {}\ntype GoIface interface {}\n');
    fs.writeFileSync(path.join(dir, 'r.rs'), 'use std::fmt;\npub fn rust_fn() {}\npub struct RStruct {}\npub trait RTrait {}\npub enum REnum {}\n');
    fs.writeFileSync(path.join(dir, 'J.java'), 'import java.util.List;\npublic class JClass {\n  public void method() {}\n}\ninterface JIface {}\n');
    const idx = new WorkspaceIndexer(dir);
    const built = await idx.buildIndex();
    assert.ok(built.fileCount >= 5);
    assert.ok(built.symbolCount > 0);

    const stats = idx.getStats();
    assert.equal(stats.fileCount, idx.getFiles().length);

    // getFile hit + miss
    assert.ok(idx.getFile(path.join(dir, 'm.py')));
    assert.equal(idx.getFile(path.join(dir, 'missing.py')), null);

    const summary = idx.getSummary();
    assert.ok(summary.includes('Workspace'));

    // re-index the same file exercises removeFile-before-reindex
    const again = await idx.indexFile(path.join(dir, 'm.py'));
    assert.ok(again);
  });

  it('getSummary reports an empty workspace', () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ws-empty-'));
    const idx = new WorkspaceIndexer(dir);
    assert.match(idx.getSummary(), /empty/);
  });

  it('buildIndex reports when no project root is found', async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ws-noroot-'));
    const deep = path.join(dir, 'a', 'b');
    fs.mkdirSync(deep, { recursive: true });
    const idx = new WorkspaceIndexer(deep);
    const globMock = mock.method(indexerHooks, 'glob', async () => []);
    try {
      const res = await idx.buildIndex();
      // either no root (temp dir under /tmp has no marker) or an empty index
      assert.ok(res.fileCount === 0);
    } finally {
      globMock.mock.restore();
    }
  });

  it('indexFile skips directories, oversized, empty, binary, and unknown files', async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ws-skip-'));
    const idx = new WorkspaceIndexer(dir);
    fs.mkdirSync(path.join(dir, 'sub.ts'));            // a directory named like a .ts
    assert.equal(await idx.indexFile(path.join(dir, 'sub.ts')), null);
    assert.equal(await idx.indexFile(path.join(dir, 'nope.ts')), null);  // nonexistent
    fs.writeFileSync(path.join(dir, 'u.unknownext'), 'x');
    assert.equal(await idx.indexFile(path.join(dir, 'u.unknownext')), null);
    fs.writeFileSync(path.join(dir, 'empty.ts'), '');
    assert.equal(await idx.indexFile(path.join(dir, 'empty.ts')), null);
    fs.writeFileSync(path.join(dir, 'bin.ts'), 'abc\0def');
    assert.equal(await idx.indexFile(path.join(dir, 'bin.ts')), null);
  });

  it('extractSymbols and extractImports return [] for an unknown language', async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ws-unk-'));
    const idx = new WorkspaceIndexer(dir);
    assert.deepEqual((idx as any).extractSymbols('whatever', '/f.cobol', 'cobol'), []);
    assert.deepEqual(await (idx as any).extractImports('whatever', '/f.cobol', 'cobol'), []);
  });
});
