import { describe, it, before, after, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { LSPClient } from '../src/lsp/client';
import { registerLspServers, resetLspServers } from '../src/lsp/config';
import { getLSPDiagnosticRegistry, resetLSPDiagnosticRegistry } from '../src/lsp/diagnostic-registry';
import { lspDiagnosticsTool, _lspDiagHooks, type LspDiagClient } from '../src/tools/lsp-diagnostics';
import { captureLspBaseline, lspHooks } from '../src/tools/file-edit';
import type { ToolContext } from '../src/types';

/**
 * E2E (red-green) for U143: a real LSPClient subprocess pushes
 * `textDocument/publishDiagnostics`; the client's onPublishDiagnostics must FEED
 * the async {@link getLSPDiagnosticRegistry} singleton, and the `diagnostics` tool's
 * `only_new` mode must DRAIN it with cross-turn deduplication.
 *
 * Reverting either the client ingestion hook (client.ts onPublishDiagnostics) or
 * the tool drain (`only_new` branch) turns this suite red.
 */
const MOCK_SERVER = String.raw`
let buf = Buffer.alloc(0);
function send(msg) { const b = JSON.stringify(msg); process.stdout.write('Content-Length: ' + Buffer.byteLength(b) + '\r\n\r\n' + b); }
function diagsFor(text) {
  return text && text.includes('ERR')
    ? [{ message: 'bad thing', severity: 1, source: 'mock', code: 42, range: { start: { line: 0, character: 0 }, end: { line: 0, character: 3 } } }]
    : [];
}
function handle(msg) {
  const m = msg.method;
  if (m === 'initialize') { send({ jsonrpc: '2.0', id: msg.id, result: { capabilities: {} } }); return; }
  if (m === 'initialized' || m === 'textDocument/didSave') return;
  if (m === 'textDocument/didOpen') { send({ jsonrpc: '2.0', method: 'textDocument/publishDiagnostics', params: { uri: msg.params.textDocument.uri, diagnostics: diagsFor(msg.params.textDocument.text) } }); return; }
  if (m === 'textDocument/didChange') { send({ jsonrpc: '2.0', method: 'textDocument/publishDiagnostics', params: { uri: msg.params.textDocument.uri, diagnostics: diagsFor(msg.params.contentChanges[0].text) } }); return; }
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

function makeClient(): LSPClient {
  const client = new LSPClient();
  client._isBinaryInstalled = () => true;
  client._sleep = async () => {};
  return client;
}

function ctx(): ToolContext {
  return { cwd: dir } as unknown as ToolContext;
}

describe('U143 e2e: publishDiagnostics → registry → diagnostics tool only_new drain', () => {
  let client: LSPClient;

  before(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-reg-e2e-'));
    const mockScript = path.join(dir, 'mock-server.cjs');
    fs.writeFileSync(mockScript, MOCK_SERVER);
    registerLspServers({
      mocklang: { command: process.execPath, args: [mockScript], extensionToLanguage: { '.mock': 'mocklang' } },
    });
  });

  after(() => {
    resetLspServers();
    if (dir) fs.rmSync(dir, { recursive: true, force: true });
    resetLSPDiagnosticRegistry();
  });

  afterEach(() => {
    mock.restoreAll();
    client?.shutdown();
    resetLSPDiagnosticRegistry();
  });

  it('client ingestion registers pending; tool drain delivers then cross-turn-dedupes', async () => {
    resetLSPDiagnosticRegistry();
    client = makeClient();
    const f = path.join(dir, 'errs.mock');
    fs.writeFileSync(f, 'ERR here\n');

    // Drive a real publishDiagnostics via the client.
    const got = await client.getDiagnostics(f);
    assert.ok(got && got.diagnostics.length === 1);

    // WIRING UNDER TEST #1: the publish fed the async registry.
    assert.ok(getLSPDiagnosticRegistry().getPendingLSPDiagnosticCount() >= 1, 'client ingestion must register pending diagnostics');

    // Point the tool at a connected client stub (only_new path uses the registry, not the client).
    mock.method(_lspDiagHooks, 'getLSPClient', () => ({
      getDiagnostics: async () => null,
      getAllDiagnostics: () => [],
      isConnected: () => true,
      diagnostics: { hasBaseline: () => false, getNewDiagnostics: async () => [] },
    } as unknown as LspDiagClient));

    // WIRING UNDER TEST #2: only_new drains the registry.
    const first = await lspDiagnosticsTool.execute({ only_new: true }, ctx());
    assert.match(first.output, /new diagnostic issue/);
    assert.match(first.output, /bad thing/);

    // Cross-turn dedup: a second drain has nothing new.
    const second = await lspDiagnosticsTool.execute({ only_new: true }, ctx());
    assert.match(second.output, /No new diagnostics since the last check\./);
  });

  it('only_new rejects a file_path combination and a non-boolean value', async () => {
    mock.method(_lspDiagHooks, 'getLSPClient', () => ({
      getDiagnostics: async () => null,
      getAllDiagnostics: () => [],
      isConnected: () => true,
      diagnostics: { hasBaseline: () => false, getNewDiagnostics: async () => [] },
    } as unknown as LspDiagClient));
    const combo = await lspDiagnosticsTool.execute({ only_new: true, file_path: '/x.ts' }, ctx());
    assert.equal(combo.isError, true);
    assert.match(combo.output, /only_new applies to workspace scope/);
    const bad = await lspDiagnosticsTool.execute({ only_new: 'yes' }, ctx());
    assert.equal(bad.isError, true);
    assert.match(bad.output, /Invalid only_new/);
  });

  it('captureLspBaseline (file edit) clears the delivered set so the file re-delivers', () => {
    resetLSPDiagnosticRegistry();
    const reg = getLSPDiagnosticRegistry();
    const target = path.join(dir, 'edited.ts');
    reg.registerPendingLSPDiagnostic({
      serverName: 'ts',
      files: [{ uri: `file://${target}`, diagnostics: [{ message: 'e', severity: 'Error', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } }] }],
    });
    reg.checkForLSPDiagnostics();
    assert.equal(reg.deliveredFileCount, 1);
    // getClient would throw without a real client; the hook swallows it, and the
    // clearDelivered wiring (under test) still runs.
    mock.method(lspHooks, 'getClient', () => { throw new Error('no client in test'); });
    captureLspBaseline(target, { cwd: dir, detachedMode: false } as unknown as ToolContext);
    assert.equal(reg.deliveredFileCount, 0, 'editing the file must clear its delivered diagnostics');
  });

  it('client.shutdown resets the async registry state', async () => {
    resetLSPDiagnosticRegistry();
    client = makeClient();
    const f = path.join(dir, 'errs2.mock');
    fs.writeFileSync(f, 'ERR x\n');
    await client.getDiagnostics(f);
    assert.ok(getLSPDiagnosticRegistry().getPendingLSPDiagnosticCount() >= 1);
    client.shutdown();
    assert.equal(getLSPDiagnosticRegistry().getPendingLSPDiagnosticCount(), 0, 'shutdown clears pending');
  });
});
