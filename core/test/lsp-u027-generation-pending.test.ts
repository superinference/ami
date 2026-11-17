import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { LSPClient } from '../src/lsp/client';
import { registerLspServers, resetLspServers } from '../src/lsp/config';

/**
 * U027 — two genuinely-missing client.ts capabilities, driven end-to-end against
 * real stdio language-server subprocesses:
 *   1. Generation-counter stale-promise invalidation: a `shutdown()` that races an
 *      in-flight `initialize` handshake must NOT leave a phantom server behind.
 *   2. `getPendingDiagnosticCount()` introspection: files awaiting a
 *      `publishDiagnostics` are counted while pending and drop to zero once the
 *      wait settles (including the timeout path).
 */

// A server that answers `initialize` but NEVER publishes diagnostics — a client
// that opens a document gets no publishDiagnostics, so its getDiagnostics stays
// pending until the bounded wait times out.
const NOPUB_SERVER = String.raw`
let buf = Buffer.alloc(0);
function send(msg) {
  const b = JSON.stringify(msg);
  process.stdout.write('Content-Length: ' + Buffer.byteLength(b) + '\r\n\r\n' + b);
}
function handle(msg) {
  if (msg.method === 'initialize') { send({ jsonrpc: '2.0', id: msg.id, result: { capabilities: {} } }); return; }
  if (msg.method === 'shutdown') { send({ jsonrpc: '2.0', id: msg.id, result: null }); return; }
  // Everything else (initialized, didOpen, didChange, exit, …) is ignored: no
  // publishDiagnostics is ever emitted.
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

// A server that spawns and stays alive but NEVER answers `initialize` — its
// handshake only ever ends via the configured startupTimeout.
const SILENT_SERVER = 'process.stdin.resume(); process.stdin.on("data", () => {});\n';

let dir: string;
let nopubScript: string;
let silentScript: string;

function makeClient(): LSPClient {
  const client = new LSPClient();
  client._isBinaryInstalled = () => true;
  client._sleep = async () => {};
  return client;
}

async function waitFor(pred: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  for (;;) {
    if (pred()) return;
    if (Date.now() - start > timeoutMs) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 5));
  }
}

before(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-u027-'));
  nopubScript = path.join(dir, 'nopub-server.cjs');
  silentScript = path.join(dir, 'silent-server.cjs');
  fs.writeFileSync(nopubScript, NOPUB_SERVER);
  fs.writeFileSync(silentScript, SILENT_SERVER);
  registerLspServers({
    pendlang: { command: process.execPath, args: [nopubScript], extensionToLanguage: { '.pend': 'pendlang' } },
    stalelang: {
      command: process.execPath,
      args: [silentScript],
      startupTimeout: 150,
      shutdownTimeout: 50,
      extensionToLanguage: { '.stale': 'stalelang' },
    },
  });
});

after(() => {
  resetLspServers();
  if (dir) fs.rmSync(dir, { recursive: true, force: true });
});

describe('LSPClient U027: generation-guarded stale-handshake invalidation', () => {
  let client: LSPClient;
  afterEach(() => client?.shutdown());

  it('shutdown() during an in-flight handshake leaves NO phantom server state', async () => {
    client = makeClient();
    // Start a handshake that will only settle via startupTimeout (150ms); the
    // synchronous part of ensureServer has already spawned + registered the conn.
    const pending = client.ensureServer('stalelang', dir);
    // Let the spawn + initialize send happen, then tear down mid-handshake.
    await new Promise((r) => setTimeout(r, 30));
    client.shutdown();
    const ok = await pending;

    // The stale handshake must resolve false and must not have re-recorded the
    // server as `error` over the state shutdown() cleared.
    assert.equal(ok, false);
    assert.equal(client.getServerStatus('stalelang'), undefined);
    assert.deepEqual(client.getInitializationStatus(), { status: 'not-started' });
    assert.equal(client.isConnected(), false);
  });

  it('a normal (un-raced) handshake failure still records an error status', async () => {
    // Control: without a racing shutdown, the SAME timeout path DOES surface an
    // error — proving the guard suppresses ONLY stale resolutions, not real ones.
    client = makeClient();
    const ok = await client.ensureServer('stalelang', dir);
    assert.equal(ok, false);
    const status = client.getServerStatus('stalelang');
    assert.equal(status?.state, 'error');
    assert.match(status?.lastError ?? '', /timed out|initialize failed/);
    assert.deepEqual(client.getInitializationStatus(), { status: 'failed', error: status!.lastError! });
  });
});

describe('LSPClient U027: getPendingDiagnosticCount introspection', () => {
  let client: LSPClient;
  afterEach(() => client?.shutdown());

  it('counts files awaiting diagnostics and clears them once the wait settles', async () => {
    client = makeClient();
    const file = path.join(dir, 'sample.pend');
    fs.writeFileSync(file, 'content\n');

    assert.equal(client.getPendingDiagnosticCount(), 0);

    // The NOPUB server never publishes, so getDiagnostics registers a waiter and
    // resolves only after the (short) bounded wait.
    const got = client.getDiagnostics(file, 120);
    await waitFor(() => client.getPendingDiagnosticCount() === 1);
    assert.equal(client.getPendingDiagnosticCount(), 1);

    const result = await got;
    // No diagnostics were published → an empty set for the file.
    assert.deepEqual(result?.diagnostics, []);
    // The timed-out waiter is cleaned up, so nothing is left "pending".
    assert.equal(client.getPendingDiagnosticCount(), 0);
  });
});
