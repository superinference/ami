import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { LSPClient } from '../src/lsp/client';
import {
  LspHealthMonitor,
  collectLspHealthErrors,
  LSP_POLL_INTERVAL_MS,
  type HealthScheduler,
  type LspHealthError,
} from '../src/lsp/health-monitor';
import type { LspInitializationStatus, LspServerStatus } from '../src/lsp/client';
import { registerLspServers, resetLspServers } from '../src/lsp/config';

/**
 * U027 — LSP health-poll state machine (parity with the reference's
 * useLspInitializationNotification polling loop). Covers:
 *   1. `collectLspHealthErrors` branch structure: failed→report+stop,
 *      pending/not-started→continue-silent, success→scan-servers+continue.
 *   2. `LspHealthMonitor` dedup per source, terminal stop latch, injectable
 *      scheduler + onError sink.
 *   3. `LSP_POLL_INTERVAL_MS` default (5000ms).
 *   4. E2E: LSPClient auto-starts the loop on the first successful handshake and
 *      `shutdown()` stops it.
 */

const status = (s: LspInitializationStatus): LspInitializationStatus => s;
function serverStatus(partial: Partial<LspServerStatus> & { language: string }): LspServerStatus {
  return {
    state: 'running',
    restartCount: 0,
    isHealthy: true,
    consecutiveDiagnosticFailures: 0,
    ...partial,
  };
}

/** A scheduler whose single tick callback the test drives synchronously. */
function manualScheduler(): HealthScheduler & { tick(): void; cleared: boolean } {
  let fn: (() => void) | undefined;
  let cleared = false;
  return {
    setInterval(f) { fn = f; return 1; },
    clearInterval() { cleared = true; fn = undefined; },
    tick() { fn?.(); },
    get cleared() { return cleared; },
  } as HealthScheduler & { tick(): void; cleared: boolean };
}

describe('collectLspHealthErrors: poll-tick classification', () => {
  it('failed manager → reports the manager error AND stops polling (terminal)', () => {
    const r = collectLspHealthErrors(status({ status: 'failed', error: 'boom' }), []);
    assert.deepEqual(r.errors, [{ source: 'lsp-manager', message: 'boom' }]);
    assert.equal(r.shouldStop, true);
  });

  it('pending / not-started → no errors, keep polling (still initializing)', () => {
    for (const s of ['pending', 'not-started'] as const) {
      const r = collectLspHealthErrors(status({ status: s }), [
        serverStatus({ language: 'go', state: 'error', lastError: 'ignored while initializing' }),
      ]);
      assert.deepEqual(r.errors, []);
      assert.equal(r.shouldStop, false);
    }
  });

  it('success → scans servers for post-startup errors and keeps polling', () => {
    const r = collectLspHealthErrors(status({ status: 'success' }), [
      serverStatus({ language: 'ts', state: 'running' }),
      serverStatus({ language: 'py', state: 'error', lastError: 'crashed with exit code 1' }),
      // An error server with no lastError message is not reportable.
      serverStatus({ language: 'rs', state: 'error' }),
    ]);
    assert.deepEqual(r.errors, [{ source: 'py', message: 'crashed with exit code 1' }]);
    assert.equal(r.shouldStop, false);
  });
});

describe('LspHealthMonitor: dedup, terminal stop, scheduler', () => {
  it('reports each error once (deduped per source:message) across ticks', () => {
    const errs: LspHealthError[] = [];
    const sched = manualScheduler();
    const monitor = new LspHealthMonitor({
      getStatus: () => ({ status: 'success' }),
      getServers: () => [serverStatus({ language: 'py', state: 'error', lastError: 'E1' })],
      onError: (e) => errs.push(e),
      scheduler: sched,
    });
    monitor.start();
    sched.tick();
    sched.tick();
    sched.tick();
    assert.deepEqual(errs, [{ source: 'py', message: 'E1' }]);
    assert.equal(monitor.isRunning(), true);
    monitor.stop();
    assert.equal(monitor.isRunning(), false);
  });

  it('stops polling after a terminal manager failure (give-up boundary)', () => {
    const errs: LspHealthError[] = [];
    const sched = manualScheduler();
    const monitor = new LspHealthMonitor({
      getStatus: () => ({ status: 'failed', error: 'dead' }),
      getServers: () => [],
      onError: (e) => errs.push(e),
      scheduler: sched,
    });
    monitor.start();
    assert.equal(monitor.isRunning(), true);
    sched.tick();
    assert.equal(monitor.isRunning(), false, 'latched off after failed');
    assert.equal(sched.cleared, true);
    assert.deepEqual(errs, [{ source: 'lsp-manager', message: 'dead' }]);
  });

  it('start() is idempotent and the default interval is LSP_POLL_INTERVAL_MS', () => {
    assert.equal(LSP_POLL_INTERVAL_MS, 5000);
    let intervals = 0;
    let lastMs = 0;
    const sched: HealthScheduler = {
      setInterval: (_f, ms) => { intervals++; lastMs = ms; return intervals; },
      clearInterval: () => {},
    };
    const monitor = new LspHealthMonitor({
      getStatus: () => ({ status: 'not-started' }),
      getServers: () => [],
      scheduler: sched,
    });
    monitor.start();
    monitor.start();
    assert.equal(intervals, 1, 'second start is a no-op');
    assert.equal(lastMs, LSP_POLL_INTERVAL_MS);
  });
});

// ---- E2E: client auto-starts / stops the loop --------------------------------

const INIT_ONLY_SERVER = String.raw`
let buf = Buffer.alloc(0);
function send(m){const b=JSON.stringify(m);process.stdout.write('Content-Length: '+Buffer.byteLength(b)+'\r\n\r\n'+b);}
process.stdin.on('data',(c)=>{buf=Buffer.concat([buf,c]);for(;;){const he=buf.indexOf('\r\n\r\n');if(he===-1)return;const mm=/Content-Length:\s*(\d+)/i.exec(buf.subarray(0,he).toString());if(!mm){buf=buf.subarray(he+4);continue;}const len=Number(mm[1]);const bs=he+4;if(buf.length<bs+len)return;const body=buf.subarray(bs,bs+len).toString();buf=buf.subarray(bs+len);let msg;try{msg=JSON.parse(body);}catch{continue;}if(msg.method==='initialize')send({jsonrpc:'2.0',id:msg.id,result:{capabilities:{}}});else if(msg.method==='shutdown')send({jsonrpc:'2.0',id:msg.id,result:null});}});
`;

describe('LSPClient U027: health monitor auto-start / stop (E2E)', () => {
  let dir: string;
  let client: LSPClient;

  before(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-u027-hm-'));
    const script = path.join(dir, 'init-only.cjs');
    fs.writeFileSync(script, INIT_ONLY_SERVER);
    registerLspServers({
      hmlang: { command: process.execPath, args: [script], extensionToLanguage: { '.hm': 'hmlang' } },
      hmbad: { command: 'HMBADCMD', extensionToLanguage: { '.hmb': 'hmbad' } },
    });
  });

  after(() => {
    resetLspServers();
    if (dir) fs.rmSync(dir, { recursive: true, force: true });
  });

  afterEach(() => client?.shutdown());

  it('starts the background health loop on the first successful init and stops it on shutdown', async () => {
    client = new LSPClient();
    client._isBinaryInstalled = () => true;
    client._sleep = async () => {};

    assert.equal(client.isHealthMonitoringActive(), false, 'not polling before any server');
    const ok = await client.ensureServer('hmlang', dir);
    assert.equal(ok, true);
    assert.equal(client.isHealthMonitoringActive(), true, 'auto-started after successful init');

    client.shutdown();
    assert.equal(client.isHealthMonitoringActive(), false, 'stopped on shutdown');
  });

  it('a loop bound to the live client surfaces a per-server error once, with one server healthy', async () => {
    client = new LSPClient();
    // Only the good command resolves; the bad one is "not found" so its server
    // lands in an `error` state WHILE the good server stays connected (success).
    client._isBinaryInstalled = (cmd) => cmd !== 'HMBADCMD';
    client._sleep = async () => {};

    assert.equal(await client.ensureServer('hmlang', dir), true);
    assert.equal(await client.ensureServer('hmbad', dir), false);

    // Client-wide status is `success` (hmlang connected) yet hmbad is errored —
    // exactly the "scan for post-startup errors while initialized" branch.
    assert.equal(client.getInitializationStatus().status, 'success');

    const errs: LspHealthError[] = [];
    const sched = manualScheduler();
    const monitor = new LspHealthMonitor({
      getStatus: () => client.getInitializationStatus(),
      getServers: () => client.getAllServerStatus(),
      onError: (e) => errs.push(e),
      scheduler: sched,
    });
    monitor.start();
    sched.tick();
    sched.tick(); // deduped — still reported once

    assert.equal(errs.length, 1);
    assert.equal(errs[0].source, 'hmbad');
    assert.match(errs[0].message, /command not found/);
    monitor.stop();
  });
});
