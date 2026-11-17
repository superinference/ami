import { describe, it, before, after, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { LSPClient } from '../src/lsp/client';
import {
  registerLspServers,
  resetLspServers,
  resolveLanguageForExtension,
  languageIdForExtension,
  validateLspServerConfig,
} from '../src/lsp/config';
import { fsHooks } from '../src/logger';

/**
 * U027 — three additive parity improvements to the LSP client/config:
 *   1. In-flight handshake de-duplication (init-pending await): concurrent
 *      ensureServer/request callers for one language await the SAME handshake
 *      instead of racing a half-open "starting" server.
 *   2. Lifecycle debug/introspection logging on didOpen/didChange/didSave +
 *      the "already open" short-circuit + a shutdown summary.
 *   3. Priority ordering when more than one configured server claims the same
 *      file extension.
 */

// A minimal Content-Length-framed JSON-RPC server. `initialize` is answered on a
// macrotask (setTimeout 0) so there is a real async window during which a second
// concurrent ensureServer could observe a half-open server.
const MOCK_SERVER = String.raw`
let buf = Buffer.alloc(0);
function send(msg) { const b = JSON.stringify(msg); process.stdout.write('Content-Length: ' + Buffer.byteLength(b) + '\r\n\r\n' + b); }
function handle(msg) {
  const m = msg.method;
  if (m === 'initialize') { setTimeout(() => send({ jsonrpc: '2.0', id: msg.id, result: { capabilities: {} } }), 0); return; }
  if (m === 'initialized' || m === 'textDocument/didSave' || m === 'exit') return;
  if (m === 'textDocument/didOpen') { send({ jsonrpc: '2.0', method: 'textDocument/publishDiagnostics', params: { uri: msg.params.textDocument.uri, diagnostics: [] } }); return; }
  if (m === 'textDocument/didChange') { send({ jsonrpc: '2.0', method: 'textDocument/publishDiagnostics', params: { uri: msg.params.textDocument.uri, diagnostics: [] } }); return; }
  if (m === 'textDocument/hover') { send({ jsonrpc: '2.0', id: msg.id, result: { contents: 'ok' } }); return; }
  if (m === 'shutdown') { send({ jsonrpc: '2.0', id: msg.id, result: null }); return; }
  if (msg.id !== undefined) send({ jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: 'nope' } });
}
process.stdin.on('data', (c) => {
  buf = Buffer.concat([buf, c]);
  for (;;) {
    const he = buf.indexOf('\r\n\r\n');
    if (he === -1) return;
    const mm = /Content-Length:\s*(\d+)/i.exec(buf.subarray(0, he).toString());
    if (!mm) { buf = buf.subarray(he + 4); continue; }
    const len = Number(mm[1]); const bs = he + 4;
    if (buf.length < bs + len) return;
    const body = buf.subarray(bs, bs + len).toString();
    buf = buf.subarray(bs + len);
    try { handle(JSON.parse(body)); } catch {}
  }
});
`;

let dir: string;
let mockScript: string;
let mockFile: string;

function makeClient(): LSPClient {
  const client = new LSPClient();
  client._isBinaryInstalled = () => true;
  client._sleep = async () => {};
  return client;
}

before(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-u027-'));
  mockScript = path.join(dir, 'mock.cjs');
  mockFile = path.join(dir, 'sample.u27');
  fs.writeFileSync(mockScript, MOCK_SERVER);
  fs.writeFileSync(mockFile, 'clean\n');
  registerLspServers({
    u27lang: { command: process.execPath, args: [mockScript], extensionToLanguage: { '.u27': 'u27lang' } },
  });
});

after(() => {
  resetLspServers();
  if (dir) fs.rmSync(dir, { recursive: true, force: true });
});

describe('U027 LSPClient: in-flight handshake de-duplication', () => {
  let client: LSPClient;
  afterEach(() => client?.shutdown());

  it('two concurrent ensureServer calls share ONE handshake and both see it initialized', async () => {
    client = makeClient();
    let spawnCount = 0;
    const realSpawn = client._spawn;
    client._spawn = ((...args: Parameters<typeof realSpawn>) => { spawnCount++; return realSpawn(...args); }) as typeof realSpawn;

    const [a, b] = await Promise.all([
      client.ensureServer('u27lang', dir),
      client.ensureServer('u27lang', dir),
    ]);
    assert.equal(a, true);
    assert.equal(b, true);
    // The whole point: only one process is spawned for the two concurrent callers.
    assert.equal(spawnCount, 1);
    assert.equal(client.isInitialized('u27lang'), true);
  });

  it('concurrent requests issued during startup all succeed (no half-open "starting" race)', async () => {
    client = makeClient();
    let spawnCount = 0;
    const realSpawn = client._spawn;
    client._spawn = ((...args: Parameters<typeof realSpawn>) => { spawnCount++; return realSpawn(...args); }) as typeof realSpawn;

    // Both requests race the single in-flight handshake. Before the fix the second
    // caller observed a half-open server and sendRequest threw "server is starting".
    const [r1, r2] = await Promise.all([
      client.request(mockFile, dir, 'textDocument/hover', { textDocument: { uri: `file://${mockFile}` }, position: { line: 0, character: 0 } }),
      client.request(mockFile, dir, 'textDocument/hover', { textDocument: { uri: `file://${mockFile}` }, position: { line: 0, character: 0 } }),
    ]);
    assert.deepEqual(r1, { contents: 'ok' });
    assert.deepEqual(r2, { contents: 'ok' });
    assert.equal(spawnCount, 1);
  });

  it('a settled handshake is not reused — the next ensureServer returns the live server', async () => {
    client = makeClient();
    assert.equal(await client.ensureServer('u27lang', dir), true);
    // Already initialized → fast path, no new handshake entry leaked.
    assert.equal(await client.ensureServer('u27lang', dir), true);
    assert.equal(client.isInitialized('u27lang'), true);
  });
});

describe('U027 LSPClient: lifecycle debug/introspection logging', () => {
  let client: LSPClient;
  afterEach(() => { client?.shutdown(); mock.restoreAll(); });

  it('logs didOpen (with languageId), the already-open skip, didChange, didSave, and a shutdown summary', async () => {
    const lines: string[] = [];
    mock.method(fsHooks, 'appendFileSync', (_f: string, line: string) => { lines.push(line); });
    client = makeClient();

    // didOpen (first touch) + publishDiagnostics capture.
    await client.getDiagnostics(mockFile);
    // Second touch → "already open, skipping didOpen".
    await client.getDiagnostics(mockFile);
    // didChange + didSave.
    await client.notifyDidChange(mockFile, 'changed\n', dir);
    await client.notifyDidSave(mockFile, dir);
    client.shutdown();

    assert.ok(lines.some((l) => /didOpen .*sample\.u27 \(languageId=u27lang\)/.test(l)), 'expected a didOpen log with languageId');
    assert.ok(lines.some((l) => /file already open, skipping didOpen/.test(l)), 'expected the already-open skip log');
    assert.ok(lines.some((l) => /didChange .*sample\.u27 \(version=2\)/.test(l)), 'expected a didChange log with the bumped version');
    assert.ok(lines.some((l) => /didSave .*sample\.u27/.test(l)), 'expected a didSave log');
    assert.ok(lines.some((l) => /shutting down 1 language server\(s\): u27lang/.test(l)), 'expected a shutdown summary log');
  });
});

describe('U027 config: priority ordering across servers sharing one extension', () => {
  afterEach(() => {
    resetLspServers();
    // Restore the suite-wide default server for other describe blocks' afterEach.
    registerLspServers({
      u27lang: { command: process.execPath, args: [mockScript], extensionToLanguage: { '.u27': 'u27lang' } },
    });
  });

  it('resolves and reports the languageId of the HIGHEST-priority server for an extension', () => {
    registerLspServers({
      lowpri: { command: 'a', extensionToLanguage: { '.shared': 'low-lang' }, priority: 1 },
      highpri: { command: 'b', extensionToLanguage: { '.shared': 'high-lang' }, priority: 5 },
    });
    assert.equal(resolveLanguageForExtension('.shared'), 'highpri');
    // languageId MUST come from the same winner, never a lower-priority server.
    assert.equal(languageIdForExtension('.shared'), 'high-lang');
  });

  it('breaks ties by stable insertion order (built-in default wins over an equal-priority registered key)', () => {
    // typescript is a built-in default owning .ts; a registered server under a
    // DIFFERENT key with equal (default) priority 0 must NOT steal .ts on a tie —
    // built-in defaults come first in iteration order. Overriding needs priority.
    registerLspServers({
      myts: { command: 'c', extensionToLanguage: { '.ts': 'typescript' } },
    });
    assert.equal(resolveLanguageForExtension('.ts'), 'typescript');
  });

  it('higher priority overrides a built-in default regardless of insertion order', () => {
    registerLspServers({
      superts: { command: 'd', extensionToLanguage: { '.ts': 'typescript' }, priority: 10 },
    });
    assert.equal(resolveLanguageForExtension('.ts'), 'superts');
  });

  it('returns null / plaintext for an unhandled extension', () => {
    assert.equal(resolveLanguageForExtension('.nopeext'), null);
    assert.equal(languageIdForExtension('.nopeext'), 'plaintext');
  });

  it('validateLspServerConfig accepts a numeric priority and ignores a non-number', () => {
    assert.equal(validateLspServerConfig('x', { command: 'c', priority: 7 }).priority, 7);
    assert.equal(validateLspServerConfig('x', { command: 'c', priority: 'high' }).priority, undefined);
  });
});
