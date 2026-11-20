import { describe, it, afterEach, before, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { LSPClient } from '../src/lsp/client';
import { registerLspServers, resetLspServers } from '../src/lsp/config';
import { getInMemoryErrors, __resetLoggerForTest, fsHooks } from '../src/logger';

/**
 * U027 — visible error reporting + lifecycle/affordance introspection logging on
 * the LSP client (parity with the reference's LSP discovery, which "doesn't throw
 * (LSP is optional) but needs visibility into why loading fails", plus its
 * per-server start logging and lazy-handler affordance hints):
 *   1. A supply-chain-guard rejection (npx without --no-install) is RECORDED on
 *      the server status + surfaced through the error logger, not swallowed.
 *   2. A "starting server" debug line makes the on-demand launch observable.
 *   3. Registering a handler before any server is connected logs an affordance hint.
 */

const INIT_ONLY_SERVER = String.raw`
let buf = Buffer.alloc(0);
function send(m){const b=JSON.stringify(m);process.stdout.write('Content-Length: '+Buffer.byteLength(b)+'\r\n\r\n'+b);}
process.stdin.on('data',(c)=>{buf=Buffer.concat([buf,c]);for(;;){const he=buf.indexOf('\r\n\r\n');if(he===-1)return;const mm=/Content-Length:\s*(\d+)/i.exec(buf.subarray(0,he).toString());if(!mm){buf=buf.subarray(he+4);continue;}const len=Number(mm[1]);const bs=he+4;if(buf.length<bs+len)return;const body=buf.subarray(bs,bs+len).toString();buf=buf.subarray(bs+len);let msg;try{msg=JSON.parse(body);}catch{continue;}if(msg.method==='initialize')send({jsonrpc:'2.0',id:msg.id,result:{capabilities:{}}});else if(msg.method==='shutdown')send({jsonrpc:'2.0',id:msg.id,result:null});}});
`;

let dir: string;
let script: string;

function makeClient(): LSPClient {
  const client = new LSPClient();
  client._isBinaryInstalled = () => true;
  client._sleep = async () => {};
  return client;
}

before(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-u027-disc-'));
  script = path.join(dir, 'init-only.cjs');
  fs.writeFileSync(script, INIT_ONLY_SERVER);
  registerLspServers({
    // An npx server WITHOUT --no-install — the supply-chain guard rejects it.
    autofetch: { command: 'npx', args: ['some-language-server', '--stdio'], extensionToLanguage: { '.af': 'autofetch' } },
    disclang: { command: process.execPath, args: [script], extensionToLanguage: { '.disc': 'disclang' } },
  });
});

after(() => {
  resetLspServers();
  if (dir) fs.rmSync(dir, { recursive: true, force: true });
});

describe('LSPClient U027: visible supply-chain-guard failure', () => {
  let client: LSPClient;
  afterEach(() => { client?.shutdown(); __resetLoggerForTest(); });

  it('records + logs the auto-fetch rejection instead of silently returning false', async () => {
    client = makeClient();
    __resetLoggerForTest();

    const ok = await client.ensureServer('autofetch', dir);
    assert.equal(ok, false);

    // Status now carries the failure (was previously left `undefined`/silent).
    const status = client.getServerStatus('autofetch');
    assert.equal(status?.state, 'error');
    assert.match(status?.lastError ?? '', /auto-fetch|--no-install/);
    assert.deepEqual(client.getInitializationStatus(), { status: 'failed', error: status!.lastError! });

    // And it is visible through the shared error logger (read by /doctor).
    assert.ok(
      getInMemoryErrors().some((e) => /auto-fetch|--no-install/.test(e.error)),
      'auto-fetch rejection surfaced through the error logger',
    );
  });
});

describe('LSPClient U027: lifecycle + affordance introspection logging', () => {
  let client: LSPClient;
  afterEach(() => { client?.shutdown(); mock.restoreAll(); });

  it('emits a "starting server" debug line on the on-demand launch', async () => {
    const lines: string[] = [];
    mock.method(fsHooks, 'appendFileSync', (_f: string, line: string) => { lines.push(line); });
    client = makeClient();

    const ok = await client.ensureServer('disclang', dir);
    assert.equal(ok, true);

    assert.ok(
      lines.some((l) => /\[LSP disclang\] starting server \(command=/.test(l)),
      'a starting-server debug line was written',
    );
  });

  it('logs an affordance hint when a handler is registered before any server connects', () => {
    const lines: string[] = [];
    mock.method(fsHooks, 'appendFileSync', (_f: string, line: string) => { lines.push(line); });
    client = makeClient();

    client.onNotification('window/showMessage', () => {});
    client.onRequest('window/workDoneProgress/create', () => null);

    assert.ok(
      lines.some((l) => /notification handler "window\/showMessage" registered before any server is connected/.test(l)),
      'notification-handler affordance hint logged',
    );
    assert.ok(
      lines.some((l) => /request handler "window\/workDoneProgress\/create" registered before any server is connected/.test(l)),
      'request-handler affordance hint logged',
    );
  });
});
