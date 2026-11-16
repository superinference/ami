import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { LSPClient } from '../src/lsp/client';
import { registerLspServers, resetLspServers } from '../src/lsp/config';
import { __resetLoggerForTest } from '../src/logger';

/**
 * E2E coverage for the LSP client's per-server lifecycle introspection surface,
 * crash detection, retained error state, contextual error wording, and the
 * stderr-as-named-log channel — exercised against REAL child-process language
 * servers (Content-Length framed JSON-RPC over stdio), not unit stubs.
 */
const MOCK_SERVER = String.raw`
let buf = Buffer.alloc(0);
function send(msg) { const b = JSON.stringify(msg); process.stdout.write('Content-Length: ' + Buffer.byteLength(b) + '\r\n\r\n' + b); }
function handle(msg) {
  const m = msg.method;
  if (m === 'initialize') { send({ jsonrpc: '2.0', id: msg.id, result: { capabilities: { hoverProvider: true } } }); return; }
  if (m === 'initialized' || m === 'textDocument/didOpen' || m === 'textDocument/didChange' || m === 'textDocument/didSave' || m === 'textDocument/didClose' || m === 'exit') return;
  if (m === 'shutdown') { send({ jsonrpc: '2.0', id: msg.id, result: null }); return; }
  if (m === 'test/stderr') { process.stderr.write('server diagnostic text\n'); send({ jsonrpc: '2.0', id: msg.id, result: { ok: true } }); return; }
  if (m === 'textDocument/hover') { send({ jsonrpc: '2.0', id: msg.id, result: { contents: 'hi' } }); return; }
  if (msg.id !== undefined) send({ jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: 'method not found' } });
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

const CRASH_SERVER = 'process.exit(1);\n';

let dir: string;
let mockScript: string;
let crashScript: string;
let mockFile: string;

function makeClient(): LSPClient {
  const client = new LSPClient();
  client._spawnSync = (() => ({ status: 0 })) as unknown as typeof client._spawnSync;
  client._sleep = async () => {};
  return client;
}

before(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-introspect-'));
  mockScript = path.join(dir, 'mock-server.cjs');
  crashScript = path.join(dir, 'crash-server.cjs');
  mockFile = path.join(dir, 'sample.mock');
  fs.writeFileSync(mockScript, MOCK_SERVER);
  fs.writeFileSync(crashScript, CRASH_SERVER);
  fs.writeFileSync(mockFile, 'content\n');
});

after(() => {
  resetLspServers();
  if (dir) fs.rmSync(dir, { recursive: true, force: true });
});

afterEach(() => {
  registerLspServers({
    mocklang: { command: process.execPath, args: [mockScript], extensionToLanguage: { '.mock': 'mocklang' } },
  });
});

describe('LSPClient: initialization-status machine + per-server introspection', () => {
  it('reports not-started with no attempts, then success + a running server after init', async () => {
    const client = makeClient();
    registerLspServers({ mocklang: { command: process.execPath, args: [mockScript], extensionToLanguage: { '.mock': 'mocklang' } } });

    assert.deepEqual(client.getInitializationStatus(), { status: 'not-started' });
    assert.equal(client.getServerStatus('mocklang'), undefined);
    assert.deepEqual(client.getAllServerStatus(), []);
    assert.equal(client.isHealthy('mocklang'), false);

    const ok = await client.ensureServer('mocklang', dir);
    assert.equal(ok, true);

    assert.deepEqual(client.getInitializationStatus(), { status: 'success' });
    const status = client.getServerStatus('mocklang');
    assert.ok(status);
    assert.equal(status!.state, 'running');
    assert.equal(status!.isHealthy, true);
    assert.equal(status!.restartCount, 0);
    assert.equal(typeof status!.startTime, 'number');
    assert.equal(client.isHealthy('mocklang'), true);
    assert.equal(client.getAllServerStatus().length, 1);
    client.shutdown();
  });

  it('flips a running server to stopped (not error) after a graceful shutdown', async () => {
    const client = makeClient();
    registerLspServers({ mocklang: { command: process.execPath, args: [mockScript], extensionToLanguage: { '.mock': 'mocklang' } } });
    await client.ensureServer('mocklang', dir);
    assert.equal(client.isHealthy('mocklang'), true);

    client.shutdown();
    // shutdown() clears retained status synchronously; a later attempt is clean.
    assert.deepEqual(client.getInitializationStatus(), { status: 'not-started' });
    assert.equal(client.isHealthy('mocklang'), false);
  });
});

describe('LSPClient: crash detection + retained error state', () => {
  it('records an error status with "crashed with exit code N" when a server crashes', async () => {
    const client = makeClient();
    registerLspServers({ crashlang: { command: process.execPath, args: [crashScript], maxRestarts: 1, extensionToLanguage: { '.crash': 'crashlang' } } });

    const ok = await client.ensureServer('crashlang', dir);
    assert.equal(ok, false);
    // Give the exit handler a tick to record the crash status.
    await new Promise((r) => setTimeout(r, 60));

    const status = client.getServerStatus('crashlang');
    assert.ok(status, 'crash status must be retained after the connection is gone');
    assert.equal(status!.state, 'error');
    assert.match(status!.lastError ?? '', /crashed with exit code 1/);
    assert.equal(status!.isHealthy, false);

    const init = client.getInitializationStatus();
    assert.equal(init.status, 'failed');
    assert.match((init as { error: string }).error, /crashed with exit code 1/);
    client.shutdown();
  });

  it('records a command-not-found error status when the binary is missing', async () => {
    const client = new LSPClient();
    client._spawnSync = (() => ({ status: 1 })) as unknown as typeof client._spawnSync; // which fails
    registerLspServers({ mocklang: { command: process.execPath, args: [mockScript], extensionToLanguage: { '.mock': 'mocklang' } } });

    const ok = await client.ensureServer('mocklang', dir);
    assert.equal(ok, false);
    const status = client.getServerStatus('mocklang');
    assert.ok(status);
    assert.equal(status!.state, 'error');
    assert.match(status!.lastError ?? '', /command not found/);
    client.shutdown();
  });
});

describe('LSPClient: contextual request errors naming server + state + lastError', () => {
  it('names the stopped state when requesting a never-started server', async () => {
    const client = makeClient();
    await assert.rejects(
      () => client.sendRequest('mocklang', 'textDocument/hover', {}),
      /Cannot send request to LSP server "mocklang": server is stopped/,
    );
    client.shutdown();
  });

  it('includes the retained lastError after a crash', async () => {
    const client = makeClient();
    registerLspServers({ crashlang: { command: process.execPath, args: [crashScript], maxRestarts: 1, extensionToLanguage: { '.crash': 'crashlang' } } });
    await client.ensureServer('crashlang', dir);
    await new Promise((r) => setTimeout(r, 60));
    await assert.rejects(
      () => client.sendRequest('crashlang', 'textDocument/hover', {}),
      /server is error, last error: .*crashed with exit code 1/,
    );
    client.shutdown();
  });
});

describe('LSPClient: server stderr surfaced as a named diagnostic log line', () => {
  it('writes "[LSP SERVER <language>] <text>" to the debug log', async () => {
    const logFile = path.join(dir, `lsp-stderr-${Date.now()}.log`);
    const prev = process.env.AMI_DEBUG_FILE;
    process.env.AMI_DEBUG_FILE = logFile;
    __resetLoggerForTest();
    try {
      const client = makeClient();
      registerLspServers({ mocklang: { command: process.execPath, args: [mockScript], extensionToLanguage: { '.mock': 'mocklang' } } });
      await client.ensureServer('mocklang', dir);
      // Trigger the mock server to write to its stderr.
      await client.sendRequest('mocklang', 'test/stderr', {});
      await new Promise((r) => setTimeout(r, 80));
      client.shutdown();

      const contents = fs.existsSync(logFile) ? fs.readFileSync(logFile, 'utf-8') : '';
      assert.match(contents, /\[LSP SERVER mocklang\] server diagnostic text/);
    } finally {
      if (prev === undefined) delete process.env.AMI_DEBUG_FILE; else process.env.AMI_DEBUG_FILE = prev;
      __resetLoggerForTest();
    }
  });
});
