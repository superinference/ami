import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import type { ChildProcess } from 'node:child_process';

import { LSPClient } from '../src/lsp/client';
import { registerLspServers, resetLspServers } from '../src/lsp/config';

/**
 * End-to-end coverage for the per-server config knobs honored by the client at
 * runtime (Claude-Code `ScopedLspServerConfig` parity): `settings` pushed via
 * `workspace/didChangeConfiguration`, `restartOnCrash: false`, unsupported
 * `transport`, `shutdownTimeout`, plus propagation of document-sync write
 * failures. Servers are spawned as real child processes framing JSON-RPC over
 * stdio, so these drive the real spawn/handshake/teardown paths.
 */

// Records every workspace/didChangeConfiguration it receives (settings JSON) to
// the file named by LSP_REC, then behaves as a normal initialize-able server.
const SETTINGS_SERVER = String.raw`
const fs = require('fs');
const REC = process.env.LSP_REC;
let buf = Buffer.alloc(0);
function send(msg) { const b = JSON.stringify(msg); process.stdout.write('Content-Length: ' + Buffer.byteLength(b) + '\r\n\r\n' + b); }
function handle(msg) {
  const m = msg.method;
  if (m === 'initialize') { send({ jsonrpc: '2.0', id: msg.id, result: { capabilities: {} } }); return; }
  if (m === 'workspace/didChangeConfiguration') { try { fs.appendFileSync(REC, JSON.stringify(msg.params) + '\n'); } catch {} return; }
  if (m === 'shutdown') { send({ jsonrpc: '2.0', id: msg.id, result: null }); return; }
  if (m === 'exit') { setTimeout(() => process.exit(0), 5); return; }
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

// Initializes normally, but NEVER replies to `shutdown`; exits promptly on the
// `exit` notification. Used to prove the configured shutdownTimeout bounds the
// graceful teardown (short timeout → fast kill, not the 1000ms default).
const HANG_SHUTDOWN_SERVER = String.raw`
let buf = Buffer.alloc(0);
function send(msg) { const b = JSON.stringify(msg); process.stdout.write('Content-Length: ' + Buffer.byteLength(b) + '\r\n\r\n' + b); }
function handle(msg) {
  const m = msg.method;
  if (m === 'initialize') { send({ jsonrpc: '2.0', id: msg.id, result: { capabilities: {} } }); return; }
  if (m === 'exit') { process.exit(0); return; }
  // shutdown: intentionally no reply.
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

// A plain initialize-able server for the sync-propagation test.
const BASIC_SERVER = String.raw`
let buf = Buffer.alloc(0);
function send(msg) { const b = JSON.stringify(msg); process.stdout.write('Content-Length: ' + Buffer.byteLength(b) + '\r\n\r\n' + b); }
function handle(msg) {
  const m = msg.method;
  if (m === 'initialize') { send({ jsonrpc: '2.0', id: msg.id, result: { capabilities: {} } }); return; }
  if (m === 'shutdown') { send({ jsonrpc: '2.0', id: msg.id, result: null }); return; }
  if (m === 'exit') { setTimeout(() => process.exit(0), 5); return; }
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

const CRASH_SERVER = 'process.exit(1);\n';

let dir: string;
let settingsScript: string;
let hangScript: string;
let basicScript: string;
let crashScript: string;
let recSeq = 0;

function makeClient(): LSPClient {
  const client = new LSPClient();
  client._spawnSync = (() => ({ status: 0 })) as unknown as typeof client._spawnSync;
  client._sleep = async () => {};
  return client;
}

/** Wrap `_spawn` to capture the spawned child process handle. */
function captureSpawn(client: LSPClient): { get(): ChildProcess | undefined } {
  let proc: ChildProcess | undefined;
  const real = client._spawn;
  client._spawn = ((cmd: string, args: string[], opts: unknown) => {
    proc = real(cmd, args, opts as Parameters<typeof real>[2]);
    return proc;
  }) as typeof client._spawn;
  return { get: () => proc };
}

async function waitFor(pred: () => boolean, ms = 2000): Promise<void> {
  const start = Date.now();
  while (!pred() && Date.now() - start < ms) await new Promise((r) => setTimeout(r, 10));
}

before(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-knobs-'));
  settingsScript = path.join(dir, 'settings-server.cjs');
  hangScript = path.join(dir, 'hang-server.cjs');
  basicScript = path.join(dir, 'basic-server.cjs');
  crashScript = path.join(dir, 'crash-server.cjs');
  fs.writeFileSync(settingsScript, SETTINGS_SERVER);
  fs.writeFileSync(hangScript, HANG_SHUTDOWN_SERVER);
  fs.writeFileSync(basicScript, BASIC_SERVER);
  fs.writeFileSync(crashScript, CRASH_SERVER);
});

after(() => {
  resetLspServers();
  if (dir) fs.rmSync(dir, { recursive: true, force: true });
});

afterEach(() => resetLspServers());

describe('LSPClient per-server config knobs', () => {
  it('pushes config.settings via workspace/didChangeConfiguration after init', async () => {
    const rec = path.join(dir, `settings-rec-${recSeq++}.log`);
    fs.writeFileSync(rec, '');
    registerLspServers({
      setlang: {
        command: process.execPath,
        args: [settingsScript],
        env: { LSP_REC: rec },
        settings: { typescript: { format: { indentSize: 2 } } },
        extensionToLanguage: { '.set': 'setlang' },
      },
    });
    const client = makeClient();
    try {
      const ok = await client.ensureServer('setlang', dir);
      assert.equal(ok, true);
      await waitFor(() => fs.readFileSync(rec, 'utf-8').includes('indentSize'));
      const lines = fs.readFileSync(rec, 'utf-8').trim().split('\n').filter(Boolean);
      assert.equal(lines.length, 1);
      assert.deepEqual(JSON.parse(lines[0]), { settings: { typescript: { format: { indentSize: 2 } } } });
    } finally {
      client.shutdown();
    }
  });

  it('does NOT send didChangeConfiguration when no settings are configured', async () => {
    const rec = path.join(dir, `settings-rec-${recSeq++}.log`);
    fs.writeFileSync(rec, '');
    registerLspServers({
      nosetlang: { command: process.execPath, args: [settingsScript], env: { LSP_REC: rec }, extensionToLanguage: { '.nos': 'nosetlang' } },
    });
    const client = makeClient();
    try {
      await client.ensureServer('nosetlang', dir);
      await new Promise((r) => setTimeout(r, 80));
      assert.equal(fs.readFileSync(rec, 'utf-8').trim(), '');
    } finally {
      client.shutdown();
    }
  });

  it('refuses to spawn a server whose transport is not stdio', async () => {
    registerLspServers({
      socklang: { command: process.execPath, args: [basicScript], transport: 'socket', extensionToLanguage: { '.sock': 'socklang' } },
    });
    const client = makeClient();
    try {
      const ok = await client.ensureServer('socklang', dir);
      assert.equal(ok, false);
      const status = client.getServerStatus('socklang');
      assert.equal(status?.state, 'error');
      assert.match(status?.lastError ?? '', /transport "socket" is not supported/);
    } finally {
      client.shutdown();
    }
  });

  it('honors restartOnCrash:false — does not respawn after the first crash', async () => {
    registerLspServers({
      norestartlang: { command: process.execPath, args: [crashScript], restartOnCrash: false, maxRestarts: 5, extensionToLanguage: { '.nr': 'norestartlang' } },
    });
    const client = makeClient();
    try {
      const first = await client.ensureServer('norestartlang', dir);
      assert.equal(first, false);
      // Let the process `exit` event fire so the crash counter is bumped before the
      // second attempt (the restartOnCrash gate keys off a prior crash).
      await waitFor(() => client.getServerStatus('norestartlang')?.state === 'error');
      await new Promise((r) => setTimeout(r, 60));
      const second = await client.ensureServer('norestartlang', dir);
      assert.equal(second, false);
      const status = client.getServerStatus('norestartlang');
      assert.match(status?.lastError ?? '', /restartOnCrash is disabled; not respawning/);
    } finally {
      client.shutdown();
    }
  });

  it('bounds graceful teardown by the configured shutdownTimeout', async () => {
    registerLspServers({
      hanglang: { command: process.execPath, args: [hangScript], shutdownTimeout: 50, extensionToLanguage: { '.hang': 'hanglang' } },
    });
    const client = makeClient();
    const captured = captureSpawn(client);
    const ok = await client.ensureServer('hanglang', dir);
    assert.equal(ok, true);
    const proc = captured.get()!;
    const t0 = Date.now();
    client.shutdown();
    // The server never replies to `shutdown`; the 50ms shutdownTimeout must fire,
    // the `exit` notification goes out, and the server exits — well under the
    // 1000ms default that would otherwise bound the handshake.
    await waitFor(() => proc.exitCode !== null || proc.signalCode !== null, 900);
    const elapsed = Date.now() - t0;
    assert.ok(proc.exitCode !== null || proc.signalCode !== null, 'server should have exited');
    assert.ok(elapsed < 600, `expected teardown within the configured timeout window, took ${elapsed}ms`);
  });

  it('propagates a document-sync write failure from notifyDidChange', async () => {
    registerLspServers({
      synclang: { command: process.execPath, args: [basicScript], extensionToLanguage: { '.sync': 'synclang' } },
    });
    const client = makeClient();
    const captured = captureSpawn(client);
    try {
      const f = path.join(dir, 'doc.sync');
      fs.writeFileSync(f, 'clean\n');
      await client.ensureServer('synclang', dir);
      // Break the pipe so every subsequent stdin write throws.
      const proc = captured.get()!;
      (proc.stdin as unknown as { write: () => never }).write = () => { throw new Error('EPIPE broken pipe'); };
      await assert.rejects(
        () => client.notifyDidChange(f, 'changed\n', dir),
        /Failed to sync file change .*doc\.sync: LSP server "synclang" write failed/,
      );
      const status = client.getServerStatus('synclang');
      assert.equal(status?.state, 'error');
      assert.match(status?.lastError ?? '', /Failed to sync file change/);
    } finally {
      client.shutdown();
    }
  });

  it('propagates a document-sync write failure from notifyDidSave', async () => {
    registerLspServers({
      savelang: { command: process.execPath, args: [basicScript], extensionToLanguage: { '.save': 'savelang' } },
    });
    const client = makeClient();
    const captured = captureSpawn(client);
    try {
      const f = path.join(dir, 'doc.save');
      fs.writeFileSync(f, 'clean\n');
      await client.ensureServer('savelang', dir);
      const proc = captured.get()!;
      (proc.stdin as unknown as { write: () => never }).write = () => { throw new Error('EPIPE broken pipe'); };
      await assert.rejects(
        () => client.notifyDidSave(f, dir),
        /Failed to sync file save .*doc\.save: LSP server "savelang" write failed/,
      );
    } finally {
      client.shutdown();
    }
  });
});
