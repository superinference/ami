import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { LSPClient } from '../src/lsp/client';
import { registerLspServers, resetLspServers } from '../src/lsp/config';

/**
 * A minimal LSP server over stdio (Content-Length framed JSON-RPC). Spawned as a
 * real child process so these are genuine end-to-end tests of the client's
 * framing parser, id correlation, handshake, diagnostics capture, retry, and
 * server→client request handling — not unit stubs.
 */
const MOCK_SERVER = String.raw`
let buf = Buffer.alloc(0);
let cmCount = 0;
const changeVersions = []; // textDocument.version seen on each didChange
const pendingPing = new Map(); // pingId -> originalRequestId
function send(msg) {
  const b = JSON.stringify(msg);
  process.stdout.write('Content-Length: ' + Buffer.byteLength(b) + '\r\n\r\n' + b);
}
function diagsFor(text) {
  return text && text.includes('ERR')
    ? [{ message: 'bad thing', severity: 1, source: 'mock', code: 42, range: { start: { line: 0, character: 0 }, end: { line: 0, character: 3 } } }]
    : [];
}
function handle(msg) {
  // Response to a server-initiated request (our ping).
  if (msg.id !== undefined && msg.method === undefined && (msg.result !== undefined || msg.error !== undefined)) {
    const orig = pendingPing.get(msg.id);
    if (orig !== undefined) {
      pendingPing.delete(msg.id);
      send({ jsonrpc: '2.0', id: orig, result: msg.result });
    }
    return;
  }
  const m = msg.method;
  if (m === 'initialize') { send({ jsonrpc: '2.0', id: msg.id, result: { capabilities: { hoverProvider: true, definitionProvider: true, callHierarchyProvider: true } } }); return; }
  if (m === 'initialized' || m === 'textDocument/didSave') return;
  if (m === 'textDocument/didOpen') { if ((msg.params.textDocument.text || '').includes('NOPUB')) return; send({ jsonrpc: '2.0', method: 'textDocument/publishDiagnostics', params: { uri: msg.params.textDocument.uri, diagnostics: diagsFor(msg.params.textDocument.text) } }); return; }
  if (m === 'textDocument/didChange') { changeVersions.push(msg.params.textDocument.version); send({ jsonrpc: '2.0', method: 'textDocument/publishDiagnostics', params: { uri: msg.params.textDocument.uri, diagnostics: diagsFor(msg.params.contentChanges[0].text) } }); return; }
  if (m === 'test/versions') { send({ jsonrpc: '2.0', id: msg.id, result: { versions: changeVersions } }); return; }
  if (m === 'textDocument/definition') { send({ jsonrpc: '2.0', id: msg.id, result: [{ uri: msg.params.textDocument.uri, range: { start: { line: 5, character: 2 }, end: { line: 5, character: 8 } } }] }); return; }
  if (m === 'textDocument/hover') { send({ jsonrpc: '2.0', id: msg.id, result: { contents: { kind: 'markdown', value: 'THE HOVER' } } }); return; }
  if (m === 'textDocument/references') { send({ jsonrpc: '2.0', id: msg.id, result: [{ uri: msg.params.textDocument.uri, range: { start: { line: 1, character: 0 }, end: { line: 1, character: 2 } } }, { uri: msg.params.textDocument.uri, range: { start: { line: 9, character: 0 }, end: { line: 9, character: 2 } } }] }); return; }
  if (m === 'textDocument/implementation') { send({ jsonrpc: '2.0', id: msg.id, result: [{ targetUri: msg.params.textDocument.uri, targetRange: { start: { line: 2, character: 0 }, end: { line: 2, character: 4 } } }] }); return; }
  if (m === 'textDocument/documentSymbol') { send({ jsonrpc: '2.0', id: msg.id, result: [{ name: 'Outer', kind: 5, range: { start: { line: 0, character: 0 }, end: { line: 9, character: 0 } }, children: [{ name: 'inner', kind: 6, range: { start: { line: 1, character: 2 }, end: { line: 1, character: 8 } } }] }] }); return; }
  if (m === 'workspace/symbol') { send({ jsonrpc: '2.0', id: msg.id, result: [{ name: 'GlobalThing', kind: 5, location: { uri: msg.params ? 'file:///w/x.mock' : '', range: { start: { line: 3, character: 0 }, end: { line: 3, character: 5 } } } }] }); return; }
  if (m === 'textDocument/prepareCallHierarchy') { send({ jsonrpc: '2.0', id: msg.id, result: [{ name: 'target', uri: msg.params.textDocument.uri, range: { start: { line: 1, character: 0 }, end: { line: 1, character: 6 } } }] }); return; }
  if (m === 'callHierarchy/incomingCalls') { send({ jsonrpc: '2.0', id: msg.id, result: [{ from: { name: 'caller', uri: msg.params.item.uri, range: { start: { line: 8, character: 0 }, end: { line: 8, character: 6 } } } }] }); return; }
  if (m === 'callHierarchy/outgoingCalls') { send({ jsonrpc: '2.0', id: msg.id, result: [{ to: { name: 'callee', uri: msg.params.item.uri, range: { start: { line: 12, character: 0 }, end: { line: 12, character: 6 } } } }] }); return; }
  if (m === 'test/contentModified') { cmCount++; if (cmCount < 3) send({ jsonrpc: '2.0', id: msg.id, error: { code: -32801, message: 'content modified' } }); else send({ jsonrpc: '2.0', id: msg.id, result: { ok: true, attempts: cmCount } }); return; }
  if (m === 'test/neverResponds') return;
  if (m === 'test/stderr') { process.stderr.write('server log line\n'); send({ jsonrpc: '2.0', id: msg.id, result: { logged: true } }); return; }
  if (m === 'test/slow') { setTimeout(() => send({ jsonrpc: '2.0', id: msg.id, result: { late: true } }), 150); return; }
  if (m === 'test/resync') { process.stdout.write('Oops-no-content-length\r\n\r\n'); const bad = '{bad json'; process.stdout.write('Content-Length: ' + Buffer.byteLength(bad) + '\r\n\r\n' + bad); send({ jsonrpc: '2.0', id: msg.id, result: { resynced: true } }); return; }
  if (m === 'test/serverError') { send({ jsonrpc: '2.0', id: msg.id, error: { code: -32603, message: 'boom' } }); return; }
  if (m === 'env/get') { const values = {}; for (const k of msg.params.keys) values[k] = process.env[k] === undefined ? null : process.env[k]; send({ jsonrpc: '2.0', id: msg.id, result: { values } }); return; }
  if (m === 'test/askClient') { const pingId = 'ping-' + msg.id; pendingPing.set(pingId, msg.id); send({ jsonrpc: '2.0', id: pingId, method: 'mock/ping', params: {} }); return; }
  if (msg.id !== undefined) send({ jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: 'method not found' } });
}
process.stdin.on('data', (c) => {
  buf = Buffer.concat([buf, c]);
  for (;;) {
    const he = buf.indexOf('\r\n\r\n');
    if (he === -1) return;
    const mm = /Content-Length:\s*(\d+)/i.exec(buf.subarray(0, he).toString());
    if (!mm) { buf = buf.subarray(he + 4); continue; }
    const len = Number(mm[1]);
    const bs = he + 4;
    if (buf.length < bs + len) return;
    const body = buf.subarray(bs, bs + len).toString();
    buf = buf.subarray(bs + len);
    try { handle(JSON.parse(body)); } catch {}
  }
});
`;

const CRASH_SERVER = 'process.exit(1);\n';

// Spawns and stays alive but NEVER answers the initialize request — used to prove
// the configured startupTimeout bounds the handshake.
const SILENT_SERVER = 'process.stdin.resume(); process.stdin.on("data", () => {});\n';

let dir: string;
let mockScript: string;
let crashScript: string;
let silentScript: string;
let mockFile: string;

/** A client wired to always pass the `which` check and never actually sleep. */
function makeClient(): LSPClient {
  const client = new LSPClient();
  // The which-check is not the thing under test; force it to succeed so the real
  // _spawn drives the real mock server subprocess.
  client._spawnSync = (() => ({ status: 0 })) as unknown as typeof client._spawnSync;
  client._sleep = async () => {};
  return client;
}

before(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-e2e-'));
  mockScript = path.join(dir, 'mock-server.cjs');
  crashScript = path.join(dir, 'crash-server.cjs');
  silentScript = path.join(dir, 'silent-server.cjs');
  mockFile = path.join(dir, 'sample.mock');
  fs.writeFileSync(mockScript, MOCK_SERVER);
  fs.writeFileSync(crashScript, CRASH_SERVER);
  fs.writeFileSync(silentScript, SILENT_SERVER);
  fs.writeFileSync(mockFile, 'clean content\n');
  registerLspServers({
    mocklang: { command: process.execPath, args: [mockScript], extensionToLanguage: { '.mock': 'mocklang' } },
    envlang: { command: process.execPath, args: [mockScript], env: { MY_CUSTOM: 'custom-val' }, extensionToLanguage: { '.env1': 'envlang' } },
    crashlang: { command: process.execPath, args: [crashScript], maxRestarts: 1, extensionToLanguage: { '.crash': 'crashlang' } },
    silentlang: { command: process.execPath, args: [silentScript], startupTimeout: 200, extensionToLanguage: { '.silent': 'silentlang' } },
  });
});

after(() => {
  resetLspServers();
  if (dir) fs.rmSync(dir, { recursive: true, force: true });
});

describe('LSPClient e2e: initialize handshake + request/response round-trip', () => {
  let client: LSPClient;
  afterEach(() => client?.shutdown());

  it('initializes (awaits result, captures capabilities, flips initialized)', async () => {
    client = makeClient();
    const ok = await client.ensureServer('mocklang', dir);
    assert.equal(ok, true);
    assert.equal(client.isInitialized('mocklang'), true);
    assert.deepEqual(client.getCapabilities('mocklang'), { hoverProvider: true, definitionProvider: true, callHierarchyProvider: true });
  });

  it('bounds the initialize handshake by the configured startupTimeout', async () => {
    client = makeClient();
    const t0 = Date.now();
    const ok = await client.ensureServer('silentlang', dir);
    const elapsed = Date.now() - t0;
    // The silent server never answers initialize; the 200ms startupTimeout must
    // bound the handshake, so ensureServer fails well within the 15s default.
    assert.equal(ok, false);
    assert.equal(client.isInitialized('silentlang'), false);
    assert.ok(elapsed < 5000, `expected failure within the startupTimeout window, took ${elapsed}ms`);
  });

  it('round-trips a request and correlates the result by id', async () => {
    client = makeClient();
    const result = await client.request<Array<{ uri: string }>>(mockFile, dir, 'textDocument/definition', {
      textDocument: { uri: `file://${mockFile}` }, position: { line: 4, character: 1 },
    });
    assert.ok(Array.isArray(result));
    assert.equal(result![0].range.start.line, 5);
  });

  it('correlates concurrent requests independently', async () => {
    client = makeClient();
    await client.ensureServer('mocklang', dir);
    const [def, hover, refs] = await Promise.all([
      client.sendRequest('mocklang', 'textDocument/definition', { textDocument: { uri: `file://${mockFile}` }, position: { line: 0, character: 0 } }),
      client.sendRequest<{ contents: { value: string } }>('mocklang', 'textDocument/hover', { textDocument: { uri: `file://${mockFile}` }, position: { line: 0, character: 0 } }),
      client.sendRequest<unknown[]>('mocklang', 'textDocument/references', { textDocument: { uri: `file://${mockFile}` }, position: { line: 0, character: 0 } }),
    ]);
    assert.ok(Array.isArray(def));
    assert.equal(hover.contents.value, 'THE HOVER');
    assert.equal(refs.length, 2);
  });

  it('returns undefined (not a throw) for an unsupported file type', async () => {
    client = makeClient();
    const r = await client.request('/tmp/x.unknownlang', dir, 'textDocument/hover', {});
    assert.equal(r, undefined);
  });

  it('propagates a server error', async () => {
    client = makeClient();
    await client.ensureServer('mocklang', dir);
    await assert.rejects(() => client.sendRequest('mocklang', 'test/serverError', {}), /boom/);
  });

  it('times out a request that never gets a response', async () => {
    client = makeClient();
    await client.ensureServer('mocklang', dir);
    await assert.rejects(() => client.sendRequest('mocklang', 'test/neverResponds', {}, 300), /timed out/);
  });

  it('throws when the language server is not initialized', async () => {
    client = makeClient();
    await assert.rejects(() => client.sendRequest('mocklang', 'textDocument/hover', {}), /not initialized/);
  });
});

describe('LSPClient e2e: ContentModified retry', () => {
  let client: LSPClient;
  afterEach(() => client?.shutdown());

  it('retries -32801 with backoff then resolves', async () => {
    client = makeClient();
    await client.ensureServer('mocklang', dir);
    const res = await client.sendRequest<{ ok: boolean; attempts: number }>('mocklang', 'test/contentModified', {});
    assert.equal(res.ok, true);
    assert.equal(res.attempts, 3);
  });
});

describe('LSPClient e2e: publishDiagnostics capture + baseline diff', () => {
  let client: LSPClient;
  afterEach(() => client?.shutdown());

  it('captures server-pushed diagnostics and returns them', async () => {
    client = makeClient();
    const f = path.join(dir, 'errs.mock');
    fs.writeFileSync(f, 'ERR here\n');
    const got = await client.getDiagnostics(f);
    assert.ok(got);
    assert.equal(got!.diagnostics.length, 1);
    assert.equal(got!.diagnostics[0].severity, 'Error');
    assert.equal(got!.diagnostics[0].code, '42'); // numeric code coerced to string
    assert.equal(got!.diagnostics[0].source, 'mock');
  });

  it('getDiagnostics returns an empty set for a clean file', async () => {
    client = makeClient();
    const got = await client.getDiagnostics(mockFile);
    assert.ok(got);
    assert.equal(got!.diagnostics.length, 0);
  });

  it('opens a file over the LSP size limit without streaming its content', async () => {
    client = makeClient();
    const f = path.join(dir, 'huge.mock');
    // Defense-in-depth: the didOpen path must not readFileSync a multi-hundred-MB
    // file. Truncate past the 10MB limit so the size guard sends empty text; the
    // document still registers and the server reports no diagnostics for it.
    fs.writeFileSync(f, 'ERR in a huge file\n');
    fs.truncateSync(f, 10_000_001);
    const got = await client.getDiagnostics(f);
    assert.ok(got);
    assert.equal(got!.diagnostics.length, 0);
  });

  it('getAllDiagnostics lists only files with issues', async () => {
    client = makeClient();
    const f = path.join(dir, 'errs2.mock');
    fs.writeFileSync(f, 'ERR x\n');
    await client.getDiagnostics(f);
    await client.getDiagnostics(mockFile); // clean — should not appear
    const all = client.getAllDiagnostics();
    assert.ok(all.some((x) => x.uri.includes('errs2.mock')));
    assert.ok(!all.some((x) => x.uri.includes('sample.mock')));
  });

  it('new-vs-baseline diff: only edit-introduced diagnostics are surfaced', async () => {
    client = makeClient();
    const f = path.join(dir, 'delta.mock');
    fs.writeFileSync(f, 'clean\n');
    // Baseline captured on a clean file.
    await client.diagnostics.beforeFileEdited(f);
    assert.ok(client.diagnostics.hasBaseline(f));
    // The "edit" introduces an error; notify the server of the new content.
    fs.writeFileSync(f, 'ERR now\n');
    await client.notifyDidChange(f, 'ERR now\n', dir);
    // Diagnostics are pushed asynchronously; let the publishDiagnostics arrive.
    await new Promise((r) => setTimeout(r, 150));
    const fresh = await client.diagnostics.getNewDiagnostics([f]);
    assert.equal(fresh.length, 1);
    assert.equal(fresh[0].diagnostics[0].message, 'bad thing');
  });
});

describe('LSPClient e2e: server→client request handling', () => {
  let client: LSPClient;
  afterEach(() => client?.shutdown());

  it('answers a server-initiated request via a registered handler', async () => {
    client = makeClient();
    client.onRequest('mock/ping', () => ({ pong: true }));
    await client.ensureServer('mocklang', dir);
    const res = await client.sendRequest<{ pong: boolean }>('mocklang', 'test/askClient', {});
    assert.deepEqual(res, { pong: true });
  });

  it('dispatches a notification to a registered handler', async () => {
    client = makeClient();
    let seen: unknown;
    client.onNotification('textDocument/publishDiagnostics', (p) => { seen = p; });
    const f = path.join(dir, 'notif.mock');
    fs.writeFileSync(f, 'ERR notify\n');
    await client.getDiagnostics(f);
    assert.ok(seen);
  });
});

describe('LSPClient e2e: env scrub + per-server env', () => {
  let client: LSPClient;
  afterEach(() => {
    client?.shutdown();
    delete process.env.ANTHROPIC_API_KEY;
  });

  it('strips credential env from the server subprocess but keeps safe + per-server env', async () => {
    process.env.ANTHROPIC_API_KEY = 'super-secret';
    client = makeClient();
    await client.ensureServer('envlang', dir);
    const res = await client.sendRequest<{ values: Record<string, string | null> }>('envlang', 'env/get', {
      keys: ['ANTHROPIC_API_KEY', 'MY_CUSTOM', 'PATH'],
    });
    assert.equal(res.values.ANTHROPIC_API_KEY, null); // scrubbed
    assert.equal(res.values.MY_CUSTOM, 'custom-val'); // per-server env applied
    assert.ok(res.values.PATH); // safe var preserved
  });
});

describe('LSPClient: supply-chain guard + crash cap', () => {
  afterEach(() => resetLspServersKeepTestServers());

  function resetLspServersKeepTestServers(): void {
    // Re-register the test servers that the suite's before() set up (a prior
    // test may have registered a conflicting one).
    registerLspServers({
      mocklang: { command: process.execPath, args: [mockScript], extensionToLanguage: { '.mock': 'mocklang' } },
    });
  }

  it('refuses to spawn a server whose npx command would auto-fetch', async () => {
    registerLspServers({ badnpx: { command: 'npx', args: ['some-langserver', '--stdio'], extensionToLanguage: { '.badnpx': 'badnpx' } } });
    const client = makeClient();
    const ok = await client.ensureServer('badnpx', dir);
    assert.equal(ok, false);
    client.shutdown();
  });

  it('refuses to spawn when the server binary is not on PATH', async () => {
    const client = new LSPClient();
    client._spawnSync = (() => ({ status: 1 })) as unknown as typeof client._spawnSync; // which fails
    const ok = await client.ensureServer('mocklang', dir);
    assert.equal(ok, false);
    client.shutdown();
  });

  it('caps crash recovery so a crashing server is not respawned unbounded', async () => {
    const client = makeClient();
    let spawnCount = 0;
    const realSpawn = client._spawn;
    client._spawn = ((...args: Parameters<typeof realSpawn>) => { spawnCount++; return realSpawn(...args); }) as typeof realSpawn;

    assert.equal(await client.ensureServer('crashlang', dir), false);
    await new Promise((r) => setTimeout(r, 60));
    assert.equal(await client.ensureServer('crashlang', dir), false);
    await new Promise((r) => setTimeout(r, 60));
    // crashCount (2) now exceeds maxRestarts (1): the next attempt must NOT spawn.
    assert.equal(await client.ensureServer('crashlang', dir), false);
    assert.equal(spawnCount, 2);
    client.shutdown();
  });
});

describe('LSPClient e2e: framing + dispatch edge cases', () => {
  let client: LSPClient;
  afterEach(() => client?.shutdown());

  it('resyncs past a header without Content-Length and isolates a bad-JSON frame', async () => {
    client = makeClient();
    await client.ensureServer('mocklang', dir);
    const res = await client.sendRequest<{ resynced: boolean }>('mocklang', 'test/resync', {});
    assert.deepEqual(res, { resynced: true });
  });

  it('ignores a late response whose request already timed out', async () => {
    client = makeClient();
    await client.ensureServer('mocklang', dir);
    await assert.rejects(() => client.sendRequest('mocklang', 'test/slow', {}, 50), /timed out/);
    // The real response arrives ~150ms later and must be dropped without crashing.
    await new Promise((r) => setTimeout(r, 200));
    // The connection is still usable afterwards.
    const hover = await client.sendRequest<{ contents: { value: string } }>('mocklang', 'textDocument/hover', { textDocument: { uri: `file://${mockFile}` }, position: { line: 0, character: 0 } });
    assert.equal(hover.contents.value, 'THE HOVER');
  });

  it('answers a server request with method-not-found when no handler is registered', async () => {
    client = makeClient();
    await client.ensureServer('mocklang', dir);
    // No onRequest('mock/ping') handler: the client replies with an error, which
    // the mock forwards back as the (undefined) result of test/askClient.
    const res = await client.sendRequest('mocklang', 'test/askClient', {});
    assert.equal(res, undefined);
  });

  it('answers a server request with an error when the handler throws', async () => {
    client = makeClient();
    client.onRequest('mock/ping', () => { throw new Error('handler boom'); });
    await client.ensureServer('mocklang', dir);
    const res = await client.sendRequest('mocklang', 'test/askClient', {});
    assert.equal(res, undefined);
  });
});

describe('LSPClient: document sync + accessors', () => {
  let client: LSPClient;
  afterEach(() => client?.shutdown());

  it('getDiagnostics returns an empty fallback when the server never publishes', async () => {
    client = makeClient();
    const f = path.join(dir, 'nopub.mock');
    fs.writeFileSync(f, 'NOPUB content\n');
    const got = await client.getDiagnostics(f, 150);
    assert.ok(got);
    assert.equal(got!.diagnostics.length, 0);
  });

  it('notifyDidChange / notifyDidSave are no-ops for an unknown file type', async () => {
    client = makeClient();
    await client.notifyDidChange('/tmp/x.unknownlang', 'x', dir);
    await client.notifyDidSave('/tmp/x.unknownlang', dir);
    assert.equal(client.getAllDiagnostics().length, 0);
  });

  it('notifyDidSave opens + saves a known file', async () => {
    client = makeClient();
    await client.notifyDidSave(mockFile, dir);
    assert.equal(client.isInitialized('mocklang'), true);
  });

  it('getCapabilities / isInitialized are empty for a server that was never started', () => {
    client = makeClient();
    assert.equal(client.getCapabilities('mocklang'), undefined);
    assert.equal(client.isInitialized('mocklang'), false);
  });

  it('isConnected reflects whether any server is initialized', async () => {
    client = makeClient();
    assert.equal(client.isConnected(), false);
    await client.ensureServer('mocklang', dir);
    assert.equal(client.isConnected(), true);
    client.shutdown();
    assert.equal(client.isConnected(), false);
  });

  it('ensureServer returns false for an unknown language', async () => {
    client = makeClient();
    assert.equal(await client.ensureServer('nosuchlang', dir), false);
  });

  it('request returns undefined when the server cannot start', async () => {
    client = makeClient();
    const f = path.join(dir, 'x.crash');
    fs.writeFileSync(f, 'boom');
    const r = await client.request(f, dir, 'textDocument/hover', {});
    assert.equal(r, undefined);
  });

  it('shutdown is safe with no servers', () => {
    client = makeClient();
    assert.doesNotThrow(() => client.shutdown());
  });

  it('tolerates a server that writes to stderr', async () => {
    client = makeClient();
    await client.ensureServer('mocklang', dir);
    const res = await client.sendRequest<{ logged: boolean }>('mocklang', 'test/stderr', {});
    assert.equal(res.logged, true);
    // Give the stderr 'data' handler a tick to fire.
    await new Promise((r) => setTimeout(r, 30));
  });

  it('the default (non-injected) sleep resolves', async () => {
    const real = new LSPClient();
    await real._sleep(1); // exercise the production backoff sleep
  });
});

describe('LSPClient: canonical URI construction + document versioning', () => {
  let client: LSPClient;
  afterEach(() => client?.shutdown());

  it('percent-encodes file URIs for paths with spaces (pathToFileURL)', async () => {
    client = makeClient();
    // A directory whose name contains a space would, with raw `file://${path}`
    // interpolation, produce a malformed URI (literal space) that servers reject.
    const spaceDir = path.join(dir, 'has space');
    fs.mkdirSync(spaceDir, { recursive: true });
    const f = path.join(spaceDir, 'err.mock');
    fs.writeFileSync(f, 'ERR here\n');
    const got = await client.getDiagnostics(f);
    assert.ok(got);
    // The server echoes back the exact URI the client sent. A canonical URI is
    // percent-encoded (%20), not a raw literal space.
    assert.ok(got!.uri.includes('%20'), `expected percent-encoded URI, got ${got!.uri}`);
    assert.ok(!/ /.test(new URL(got!.uri).pathname), 'URI path must not contain a raw space');
    // Diagnostics must still resolve correctly through the normalized key.
    assert.equal(got!.diagnostics.length, 1);
    assert.equal(got!.diagnostics[0].message, 'bad thing');
  });

  it('sends strictly increasing monotonic didChange versions (not Date.now())', async () => {
    client = makeClient();
    const f = path.join(dir, 'versioned.mock');
    fs.writeFileSync(f, 'clean\n');
    await client.notifyDidChange(f, 'one\n', dir);
    await client.notifyDidChange(f, 'two\n', dir);
    await client.notifyDidChange(f, 'three\n', dir);
    const res = await client.sendRequest<{ versions: number[] }>('mocklang', 'test/versions', {});
    // didOpen seeds version 1, so the three changes are 2, 3, 4 — small sequential
    // integers, NOT large Date.now() timestamps.
    assert.deepEqual(res.versions, [2, 3, 4]);
  });

  it('spawns the language server with windowsHide set', async () => {
    client = makeClient();
    let seenOpts: { windowsHide?: boolean } | undefined;
    const realSpawn = client._spawn;
    client._spawn = ((cmd: string, args: string[], opts: { windowsHide?: boolean }) => {
      seenOpts = opts;
      return realSpawn(cmd, args, opts as Parameters<typeof realSpawn>[2]);
    }) as typeof realSpawn;
    await client.ensureServer('mocklang', dir);
    assert.equal(seenOpts?.windowsHide, true);
  });
});
