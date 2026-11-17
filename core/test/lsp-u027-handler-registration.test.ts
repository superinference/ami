import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { LSPClient } from '../src/lsp/client';
import type { HandlerRegistrationResult } from '../src/lsp/client';
import { registerLspServers, resetLspServers } from '../src/lsp/config';
import { __resetLoggerForTest, getInMemoryErrors } from '../src/logger';

/**
 * E2E coverage for {@link LSPClient.registerDiagnosticHandlers} — AMI's analogue of
 * Claude Code's `registerLSPNotificationHandlers` (services/lsp/passiveFeedback.ts):
 * it enumerates every server attempted this session, validates each can deliver
 * diagnostics, isolates per-server readiness failures (one bad server never masks
 * the rest), optionally attaches a central publishDiagnostics observer, logs the
 * aggregate "N/M ready" status, and returns the typed `HandlerRegistrationResult`
 * monitoring shape. Exercised against REAL child-process language servers over
 * Content-Length-framed JSON-RPC, not unit stubs.
 */

// A server that replies to initialize and, on each didChange, publishes one
// diagnostic so a registered observer can be driven.
const DIAG_SERVER = String.raw`
let buf = Buffer.alloc(0);
function send(msg) { const b = JSON.stringify(msg); process.stdout.write('Content-Length: ' + Buffer.byteLength(b) + '\r\n\r\n' + b); }
function handle(msg) {
  const m = msg.method;
  if (m === 'initialize') { send({ jsonrpc: '2.0', id: msg.id, result: { capabilities: {} } }); return; }
  if (m === 'initialized' || m === 'textDocument/didOpen' || m === 'textDocument/didSave') return;
  if (m === 'textDocument/didChange') { send({ jsonrpc: '2.0', method: 'textDocument/publishDiagnostics', params: { uri: msg.params.textDocument.uri, diagnostics: [{ range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } }, severity: 1, message: 'boom' }] } }); return; }
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

const CRASH_SERVER = 'process.exit(1);\n';

let dir: string;
let diagScript: string;
let crashScript: string;
let diagFile: string;

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
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-u027-reg-'));
  diagScript = path.join(dir, 'diag-server.cjs');
  crashScript = path.join(dir, 'crash-server.cjs');
  diagFile = path.join(dir, 'sample.dmock');
  fs.writeFileSync(diagScript, DIAG_SERVER);
  fs.writeFileSync(crashScript, CRASH_SERVER);
  fs.writeFileSync(diagFile, 'content\n');
});

after(() => {
  resetLspServers();
  try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* best effort */ }
});

afterEach(() => {
  __resetLoggerForTest();
  registerLspServers({
    dmock: { command: process.execPath, args: [diagScript], extensionToLanguage: { '.dmock': 'dmock' } },
  });
});

describe('LSPClient.registerDiagnosticHandlers — aggregate registration result', () => {
  it('returns an all-zero result when no server has been attempted', () => {
    const client = makeClient();
    const reg: HandlerRegistrationResult = client.registerDiagnosticHandlers();
    assert.equal(reg.totalServers, 0);
    assert.equal(reg.successCount, 0);
    assert.deepEqual(reg.registrationErrors, []);
    assert.equal(reg.diagnosticFailures.size, 0);
    client.shutdown();
  });

  it('counts a live, initialized server as ready and logs the aggregate line', async () => {
    const client = makeClient();
    registerLspServers({ dmock: { command: process.execPath, args: [diagScript], extensionToLanguage: { '.dmock': 'dmock' } } });
    const ok = await client.ensureServer('dmock', dir);
    assert.equal(ok, true);

    const reg = client.registerDiagnosticHandlers();
    assert.equal(reg.totalServers, 1);
    assert.equal(reg.successCount, 1);
    assert.deepEqual(reg.registrationErrors, []);
    client.shutdown();
  });

  it('isolates a crashed server as a per-server registrationError, continuing past it', async () => {
    const client = makeClient();
    registerLspServers({
      dmock: { command: process.execPath, args: [diagScript], extensionToLanguage: { '.dmock': 'dmock' } },
      crashlang: { command: process.execPath, args: [crashScript], maxRestarts: 1, extensionToLanguage: { '.crash': 'crashlang' } },
    });
    assert.equal(await client.ensureServer('dmock', dir), true);
    assert.equal(await client.ensureServer('crashlang', dir), false);
    await waitFor(() => client.getServerStatus('crashlang')?.state === 'error');

    const reg = client.registerDiagnosticHandlers();
    assert.equal(reg.totalServers, 2);
    assert.equal(reg.successCount, 1, 'only the live server is ready');
    assert.equal(reg.registrationErrors.length, 1);
    assert.equal(reg.registrationErrors[0].serverName, 'crashlang');
    assert.match(reg.registrationErrors[0].error, /crashed with exit code 1/);
    // Aggregate failure line is logged (parity with Claude's "N/M succeeded").
    assert.ok(
      getInMemoryErrors().some((e) => e.error.includes('1/2 server(s) ready') && e.error.includes('crashlang')),
      'expected an aggregate registration-status log line',
    );
    client.shutdown();
  });

  it('attaches an observer that receives publishDiagnostics via the central fan-out', async () => {
    const client = makeClient();
    registerLspServers({ dmock: { command: process.execPath, args: [diagScript], extensionToLanguage: { '.dmock': 'dmock' } } });
    assert.equal(await client.ensureServer('dmock', dir), true);

    let received: unknown = null;
    const reg = client.registerDiagnosticHandlers((params) => { received = params; });
    assert.equal(reg.successCount, 1);

    await client.notifyDidChange(diagFile, 'changed', dir);
    await waitFor(() => received !== null);
    assert.ok(received && typeof received === 'object');
    assert.equal(typeof (received as { uri: string }).uri, 'string');
    assert.ok(Array.isArray((received as { diagnostics: unknown[] }).diagnostics));
    client.shutdown();
  });

  it('surfaces the per-server diagnostic-failure streak in the result as an inert copy', async () => {
    const client = makeClient();
    registerLspServers({ dmock: { command: process.execPath, args: [diagScript], extensionToLanguage: { '.dmock': 'dmock' } } });
    // An observer that throws drives the consecutive-failure tracker.
    let deliveries = 0;
    client.onNotification('textDocument/publishDiagnostics', () => { deliveries++; throw new Error('observer boom'); });
    assert.equal(await client.ensureServer('dmock', dir), true);
    await client.notifyDidChange(diagFile, 'changed', dir);
    await waitFor(() => deliveries >= 1);
    await waitFor(() => (client.getServerStatus('dmock')?.consecutiveDiagnosticFailures ?? 0) >= 1);

    const reg = client.registerDiagnosticHandlers();
    const failure = reg.diagnosticFailures.get('dmock');
    assert.ok(failure, 'failure streak must be surfaced');
    assert.ok(failure!.count >= 1);
    assert.match(failure!.lastError, /observer boom/);

    // The returned map is a copy — mutating it must not affect a later snapshot.
    reg.diagnosticFailures.set('dmock', { count: 999, lastError: 'tampered' });
    const reg2 = client.registerDiagnosticHandlers();
    assert.notEqual(reg2.diagnosticFailures.get('dmock')!.count, 999);
    client.shutdown();
  });
});
