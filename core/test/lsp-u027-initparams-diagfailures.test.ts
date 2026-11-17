/**
 * U027 parity (w70): two additive LSP client capabilities.
 *
 *  1. Full InitializeParams — the client now sends `clientInfo` (name+real
 *     version, from the single version source of truth), a `locale` derived from
 *     the environment, and `trace: 'off'` in the `initialize` handshake, matching
 *     Claude Code's complete InitializeParams (services/lsp/LSPClient.ts). Proven
 *     end-to-end by a mock server that records the exact params it received.
 *
 *  2. Per-server consecutive diagnostic-failure tracking — when delivering a
 *     `publishDiagnostics` notification (storage + registered handlers) throws,
 *     the client increments a per-language streak, logs a warning once it reaches
 *     3 (parity with Claude's passiveFeedback warn-after-3), and resets the streak
 *     on the next successful delivery. The count is surfaced on the per-server
 *     introspection status (`getServerStatus().consecutiveDiagnosticFailures`).
 */
import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { LSPClient, lspLocale } from '../src/lsp/client';
import { registerLspServers, resetLspServers } from '../src/lsp/config';
import { AMI_CLIENT_INFO } from '../src/version';
import { getInMemoryErrors, __resetLoggerForTest } from '../src/logger';

// Records the `initialize` params JSON (one line) to LSP_INIT_RECORD, replies to
// initialize, and — on each didChange — publishes a diagnostic so the failure
// tracker can be driven by repeated notifications.
const RECORD_SERVER = String.raw`
const fs = require('fs');
const REC = process.env.LSP_INIT_RECORD;
let buf = Buffer.alloc(0);
function send(msg) { const b = JSON.stringify(msg); process.stdout.write('Content-Length: ' + Buffer.byteLength(b) + '\r\n\r\n' + b); }
function handle(msg) {
  const m = msg.method;
  if (m === 'initialize') { try { fs.writeFileSync(REC, JSON.stringify(msg.params)); } catch {} send({ jsonrpc: '2.0', id: msg.id, result: { capabilities: {} } }); return; }
  if (m === 'initialized' || m === 'textDocument/didOpen' || m === 'textDocument/didSave') return;
  if (m === 'textDocument/didChange') { send({ jsonrpc: '2.0', method: 'textDocument/publishDiagnostics', params: { uri: msg.params.textDocument.uri, diagnostics: [] } }); return; }
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
let serverScript: string;
let recordFile: string;
let mockFile: string;

function makeClient(): LSPClient {
  const client = new LSPClient();
  client._isBinaryInstalled = () => true;
  client._sleep = async () => {};
  return client;
}

async function waitFor(pred: () => boolean, ms = 2000): Promise<void> {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > ms) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 10));
  }
}

before(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-u027-w70-'));
  serverScript = path.join(dir, 'record-server.cjs');
  recordFile = path.join(dir, 'init-params.json');
  mockFile = path.join(dir, 'sample.rmock');
  fs.writeFileSync(serverScript, RECORD_SERVER);
  fs.writeFileSync(mockFile, 'clean\n');
  registerLspServers({
    rmock: {
      command: process.execPath,
      args: [serverScript],
      env: { LSP_INIT_RECORD: recordFile },
      extensionToLanguage: { '.rmock': 'rmock' },
    },
  });
});

after(() => {
  resetLspServers();
  try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* best effort */ }
});

describe('lspLocale: BCP-47 locale derived from the environment', () => {
  it('prefers LC_ALL, strips encoding/modifier suffixes', () => {
    assert.equal(lspLocale({ LC_ALL: 'de_DE.UTF-8', LANG: 'en_US' }), 'de_DE');
    assert.equal(lspLocale({ LC_MESSAGES: 'fr_FR@euro' }), 'fr_FR');
    assert.equal(lspLocale({ LANG: 'ja_JP.eucJP' }), 'ja_JP');
  });

  it('falls back to "en" for unset / C / POSIX locales', () => {
    assert.equal(lspLocale({}), 'en');
    assert.equal(lspLocale({ LANG: 'C' }), 'en');
    assert.equal(lspLocale({ LC_ALL: 'POSIX' }), 'en');
    assert.equal(lspLocale({ LANG: '' }), 'en');
  });
});

describe('LSPClient e2e: full InitializeParams (clientInfo + locale + trace)', () => {
  let client: LSPClient;
  afterEach(() => client?.shutdown());

  it('sends clientInfo (name+version), locale, and trace alongside the existing params', async () => {
    client = makeClient();
    const ok = await client.ensureServer('rmock', dir);
    assert.equal(ok, true);
    const params = JSON.parse(fs.readFileSync(recordFile, 'utf-8')) as Record<string, unknown>;

    // New full-InitializeParams fields.
    assert.deepEqual(params.clientInfo, { name: AMI_CLIENT_INFO.name, version: AMI_CLIENT_INFO.version });
    assert.equal(typeof params.locale, 'string');
    assert.ok((params.locale as string).length > 0);
    assert.equal(params.trace, 'off');

    // Pre-existing params still present (no regression).
    assert.equal(params.processId, process.pid);
    assert.ok(Array.isArray(params.workspaceFolders));
    assert.ok(params.capabilities);
  });
});

describe('LSPClient e2e: consecutive diagnostic-failure tracking (warn after 3)', () => {
  let client: LSPClient;
  afterEach(() => { client?.shutdown(); __resetLoggerForTest(); });

  it('increments per-server failure streak, warns at 3, resets on success', async () => {
    __resetLoggerForTest();
    client = makeClient();
    // A diagnostics handler that throws for the first three deliveries, then
    // succeeds on the fourth — driving the streak up and then resetting it.
    let deliveries = 0;
    client.onNotification('textDocument/publishDiagnostics', () => {
      deliveries++;
      if (deliveries <= 3) throw new Error(`handler boom #${deliveries}`);
    });

    const ok = await client.ensureServer('rmock', dir);
    assert.equal(ok, true);

    // Each didChange makes the mock server publish diagnostics → one delivery.
    for (let i = 1; i <= 3; i++) {
      await client.notifyDidChange(mockFile, `content ${i}`, dir);
      await waitFor(() => deliveries >= i);
      await waitFor(() => (client.getServerStatus('rmock')?.consecutiveDiagnosticFailures ?? 0) >= i);
      assert.equal(client.getServerStatus('rmock')!.consecutiveDiagnosticFailures, i);
    }

    // At 3 consecutive failures a warning is logged to the in-memory ring.
    const warned = getInMemoryErrors().some(
      (e) => e.error.includes('failed 3 times consecutively') && e.error.includes('rmock'),
    );
    assert.ok(warned, 'expected a warn-after-3 diagnostic-failure log');

    // The fourth delivery succeeds → streak resets to 0.
    await client.notifyDidChange(mockFile, 'content 4', dir);
    await waitFor(() => deliveries >= 4);
    await waitFor(() => (client.getServerStatus('rmock')?.consecutiveDiagnosticFailures ?? 1) === 0);
    assert.equal(client.getServerStatus('rmock')!.consecutiveDiagnosticFailures, 0);
  });

  it('defaults consecutiveDiagnosticFailures to 0 for a healthy server', async () => {
    client = makeClient();
    await client.ensureServer('rmock', dir);
    assert.equal(client.getServerStatus('rmock')!.consecutiveDiagnosticFailures, 0);
  });
});
