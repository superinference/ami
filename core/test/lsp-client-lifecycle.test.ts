import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { EventEmitter } from 'node:events';

import { LSPClient } from '../src/lsp/client';
import { registerLspServers, resetLspServers } from '../src/lsp/config';

/**
 * End-to-end lifecycle coverage for the LSP client's graceful-teardown handshake,
 * `didClose` reopen-tracking notifications, the server→client `workspace/configuration`
 * responder, and the `spawn`-event await before stdio use. Servers are spawned as
 * real child processes (Content-Length framed JSON-RPC over stdio) so these exercise
 * the real framing/dispatch paths, not stubs.
 */

// A server that records every lifecycle method it receives (one per line) into the
// file named by LSP_RECORD, replies to `shutdown`, and exits on `exit`.
const LIFECYCLE_SERVER = String.raw`
const fs = require('fs');
const REC = process.env.LSP_RECORD;
function record(m) { try { fs.appendFileSync(REC, m + '\n'); } catch {} }
let buf = Buffer.alloc(0);
function send(msg) { const b = JSON.stringify(msg); process.stdout.write('Content-Length: ' + Buffer.byteLength(b) + '\r\n\r\n' + b); }
function handle(msg) {
  const m = msg.method;
  if (m === 'initialize') { send({ jsonrpc: '2.0', id: msg.id, result: { capabilities: {} } }); return; }
  if (m === 'initialized' || m === 'textDocument/didOpen' || m === 'textDocument/didChange' || m === 'textDocument/didSave') return;
  if (m === 'textDocument/didClose') { record('didClose'); return; }
  if (m === 'shutdown') { record('shutdown'); send({ jsonrpc: '2.0', id: msg.id, result: null }); return; }
  if (m === 'exit') { record('exit'); setTimeout(() => process.exit(0), 5); return; }
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
    const body = buf.subarray(bs, bs + len).toString(); buf = buf.subarray(bs + len);
    try { handle(JSON.parse(body)); } catch {}
  }
});
`;

// A server that, on `test/askConfig`, issues a server→client `workspace/configuration`
// request and echoes the client's answer back as the request result.
const CONFIG_SERVER = String.raw`
let buf = Buffer.alloc(0);
const pending = new Map();
function send(msg) { const b = JSON.stringify(msg); process.stdout.write('Content-Length: ' + Buffer.byteLength(b) + '\r\n\r\n' + b); }
function handle(msg) {
  if (msg.id !== undefined && msg.method === undefined && (msg.result !== undefined || msg.error !== undefined)) {
    const orig = pending.get(msg.id);
    if (orig !== undefined) { pending.delete(msg.id); send({ jsonrpc: '2.0', id: orig, result: msg.result }); }
    return;
  }
  const m = msg.method;
  if (m === 'initialize') { send({ jsonrpc: '2.0', id: msg.id, result: { capabilities: {} } }); return; }
  if (m === 'initialized' || m === 'textDocument/didOpen') return;
  if (m === 'test/askConfig') {
    const cfgId = 'cfg-' + msg.id; pending.set(cfgId, msg.id);
    send({ jsonrpc: '2.0', id: cfgId, method: 'workspace/configuration', params: { items: [{ section: 'editor' }, { section: 'files' }] } });
    return;
  }
  if (m === 'shutdown') { send({ jsonrpc: '2.0', id: msg.id, result: null }); return; }
  if (m === 'exit') { setTimeout(() => process.exit(0), 5); return; }
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
    const body = buf.subarray(bs, bs + len).toString(); buf = buf.subarray(bs + len);
    try { handle(JSON.parse(body)); } catch {}
  }
});
`;

let dir: string;
let lifeScript: string;
let cfgScript: string;
let recordSeq = 0;

/** Register `lifelang` with a fresh, unique record file and return its path. */
function freshLifeServer(): string {
  const recordFile = path.join(dir, `record-${recordSeq++}.log`);
  fs.writeFileSync(recordFile, '');
  registerLspServers({
    lifelang: { command: process.execPath, args: [lifeScript], env: { LSP_RECORD: recordFile }, extensionToLanguage: { '.life': 'lifelang' } },
  });
  return recordFile;
}

function makeClient(): LSPClient {
  const client = new LSPClient();
  client._isBinaryInstalled = () => true;
  client._sleep = async () => {};
  return client;
}

async function waitFor(pred: () => boolean, ms = 2000): Promise<void> {
  const start = Date.now();
  while (!pred() && Date.now() - start < ms) await new Promise((r) => setTimeout(r, 10));
}

before(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-life-'));
  lifeScript = path.join(dir, 'life-server.cjs');
  cfgScript = path.join(dir, 'cfg-server.cjs');
  recordFile = path.join(dir, 'record.log');
  fs.writeFileSync(lifeScript, LIFECYCLE_SERVER);
  fs.writeFileSync(cfgScript, CONFIG_SERVER);
  registerLspServers({
    cfglang: { command: process.execPath, args: [cfgScript], extensionToLanguage: { '.cfg': 'cfglang' } },
  });
});

after(() => {
  resetLspServers();
  if (dir) fs.rmSync(dir, { recursive: true, force: true });
});

describe('LSPClient: graceful shutdown handshake + didClose', () => {
  it('sends didClose for open docs, then shutdown, then exit, before killing', async () => {
    const recordFile = freshLifeServer();
    const client = makeClient();
    const f = path.join(dir, 'sample.life');
    fs.writeFileSync(f, 'content\n');
    // Open the document so there is a didClose to send on teardown.
    await client.notifyDidSave(f, dir);
    assert.equal(client.isInitialized('lifelang'), true);

    client.shutdown();
    // shutdown() flips connection state synchronously.
    assert.equal(client.isConnected(), false);

    await waitFor(() => fs.readFileSync(recordFile, 'utf-8').includes('exit'));
    const seq = fs.readFileSync(recordFile, 'utf-8').trim().split('\n');
    const iClose = seq.indexOf('didClose');
    const iShut = seq.indexOf('shutdown');
    const iExit = seq.indexOf('exit');
    assert.ok(iClose !== -1, `expected didClose in ${JSON.stringify(seq)}`);
    assert.ok(iShut !== -1, `expected shutdown in ${JSON.stringify(seq)}`);
    assert.ok(iExit !== -1, `expected exit in ${JSON.stringify(seq)}`);
    // Ordering: didClose before shutdown, shutdown before exit.
    assert.ok(iClose < iShut && iShut < iExit, `bad order ${JSON.stringify(seq)}`);
  });

  it('shutdown with a never-opened server still runs without error', async () => {
    const recordFile = freshLifeServer();
    const client = makeClient();
    await client.ensureServer('lifelang', dir);
    assert.doesNotThrow(() => client.shutdown());
    await waitFor(() => fs.readFileSync(recordFile, 'utf-8').includes('exit'));
    const seq = fs.readFileSync(recordFile, 'utf-8');
    // No document was opened, so no didClose — but the handshake still happened.
    assert.ok(!seq.includes('didClose'));
    assert.ok(seq.includes('shutdown') && seq.includes('exit'));
  });
});

describe('LSPClient: server→client workspace/configuration responder', () => {
  let client: LSPClient;
  afterEach(() => client?.shutdown());

  it('answers a workspace/configuration request with null per requested item', async () => {
    client = makeClient();
    await client.ensureServer('cfglang', dir);
    const res = await client.sendRequest<unknown[]>('cfglang', 'test/askConfig', {});
    assert.deepEqual(res, [null, null]);
  });

  it('a caller-registered handler overrides the default workspace/configuration responder', async () => {
    client = makeClient();
    client.onRequest('workspace/configuration', () => [{ custom: true }, { custom: true }]);
    await client.ensureServer('cfglang', dir);
    const res = await client.sendRequest<unknown[]>('cfglang', 'test/askConfig', {});
    assert.deepEqual(res, [{ custom: true }, { custom: true }]);
  });
});

describe('LSPClient: await spawn before writing to stdio', () => {
  // A fake child that never auto-emits 'spawn'; the test controls when spawn fires
  // and records every stdin write so we can prove the client waits.
  class FakeChild extends EventEmitter {
    pid = 4242;
    exitCode: number | null = null;
    signalCode: string | null = null;
    writes: string[] = [];
    stdout = new EventEmitter();
    stderr = new EventEmitter();
    stdin = {
      write: (s: string) => { this.writes.push(s); return true; },
      on: () => {},
    };
    kill() { return true; }
  }

  it('does NOT write the initialize frame until the spawn event fires', async () => {
    const client = makeClient();
    const fake = new FakeChild();
    client._spawn = (() => fake) as unknown as typeof client._spawn;

    const started = client.ensureServer('cfglang', dir);
    // Let microtasks drain. Without the spawn-await, the initialize frame would
    // already have been written here; with it, nothing is written yet.
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(fake.writes.length, 0, 'must not write before spawn event');

    // Fire spawn; now the handshake proceeds and writes initialize.
    fake.emit('spawn');
    await new Promise((r) => setTimeout(r, 20));
    assert.ok(
      fake.writes.some((w) => w.includes('"initialize"')),
      'initialize must be written after spawn',
    );

    // Complete the handshake so ensureServer resolves: reply to the initialize id.
    const frame = fake.writes.find((w) => w.includes('"initialize"'))!;
    const id = JSON.parse(frame.slice(frame.indexOf('{'))).id as number;
    const reply = JSON.stringify({ jsonrpc: '2.0', id, result: { capabilities: {} } });
    fake.stdout.emit('data', Buffer.from(`Content-Length: ${Buffer.byteLength(reply)}\r\n\r\n${reply}`));
    assert.equal(await started, true);
    client.shutdown();
  });

  it('rejects the start (returns false) when the child emits error instead of spawn', async () => {
    const client = makeClient();
    const fake = new FakeChild();
    client._spawn = (() => fake) as unknown as typeof client._spawn;
    const started = client.ensureServer('cfglang', dir);
    await new Promise((r) => setTimeout(r, 10));
    fake.emit('error', new Error('ENOENT'));
    assert.equal(await started, false);
  });
});
