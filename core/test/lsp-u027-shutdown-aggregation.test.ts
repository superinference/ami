import { describe, it, afterEach, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import type * as child_process from 'node:child_process';

import { LSPClient } from '../src/lsp/client';
import { registerLspServers, resetLspServers } from '../src/lsp/config';
import { getInMemoryErrors, __resetLoggerForTest } from '../src/logger';

/**
 * U027 — graceful aggregated shutdown with per-server failure reporting (parity
 * with Claude Code's `LSPServerManager.shutdown`, which settles every server stop
 * via Promise.allSettled and aggregates the rejections into ONE error). AMI's
 * `shutdown()` stays non-blocking (synchronous state clear — a superset) but now
 * reports, rather than swallows, which servers failed to stop.
 *
 * Driven with a fake child process so a `kill()` that throws is deterministic.
 */

/**
 * Minimal fake LSP server process: emits `spawn`, answers `initialize` + `shutdown`
 * synchronously on stdin writes, and optionally throws from `kill()` to model a
 * stuck/zombie server that cannot be stopped.
 */
function makeFakeProc(opts: { killThrows?: boolean }): child_process.ChildProcess {
  const proc = new EventEmitter() as unknown as child_process.ChildProcess & { stdout: EventEmitter; stderr: EventEmitter };
  (proc as unknown as { pid: number }).pid = 4242;
  (proc as unknown as { exitCode: number | null }).exitCode = null;
  (proc as unknown as { signalCode: string | null }).signalCode = null;
  const stdout = new EventEmitter();
  const stderr = new EventEmitter();
  (proc as unknown as { stdout: EventEmitter }).stdout = stdout;
  (proc as unknown as { stderr: EventEmitter }).stderr = stderr;
  const reply = (obj: unknown): void => {
    const body = JSON.stringify(obj);
    stdout.emit('data', Buffer.from(`Content-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`));
  };
  (proc as unknown as { stdin: unknown }).stdin = {
    write(frame: string): boolean {
      const m = /\r\n\r\n([\s\S]*)$/.exec(frame);
      if (!m) return true;
      let msg: { id?: number; method?: string };
      try { msg = JSON.parse(m[1]); } catch { return true; }
      if (msg.method === 'initialize') reply({ jsonrpc: '2.0', id: msg.id, result: { capabilities: {} } });
      else if (msg.method === 'shutdown') reply({ jsonrpc: '2.0', id: msg.id, result: null });
      return true;
    },
    on(): void { /* no-op */ },
  };
  (proc as unknown as { kill: () => boolean }).kill = () => {
    if (opts.killThrows) throw new Error('kill boom');
    return true;
  };
  // `spawn` must arrive asynchronously so `awaitSpawn` has a listener attached.
  setImmediate(() => proc.emit('spawn'));
  return proc;
}

function makeClient(spawnFor: (cmd: string) => child_process.ChildProcess): LSPClient {
  const client = new LSPClient();
  client._isBinaryInstalled = () => true;
  client._sleep = async () => {};
  client._spawn = ((cmd: string) => spawnFor(cmd)) as typeof client._spawn;
  return client;
}

async function waitFor(pred: () => boolean, timeoutMs = 1500): Promise<boolean> {
  const start = Date.now();
  for (;;) {
    if (pred()) return true;
    if (Date.now() - start > timeoutMs) return false;
    await new Promise((r) => setTimeout(r, 5));
  }
}

describe('LSPClient U027: graceful shutdown aggregates per-server stop failures', () => {
  before(() => {
    registerLspServers({
      sdok: { command: 'SDOK', extensionToLanguage: { '.sdok': 'sdok' } },
      sdbad: { command: 'SDBAD', shutdownTimeout: 30, extensionToLanguage: { '.sdbad': 'sdbad' } },
    });
  });
  after(() => resetLspServers());
  afterEach(() => __resetLoggerForTest());

  it('reports an aggregated error naming each server whose kill() failed', async () => {
    const client = makeClient((cmd) => makeFakeProc({ killThrows: cmd === 'SDBAD' }));
    assert.equal(await client.ensureServer('sdok', '.'), true);
    assert.equal(await client.ensureServer('sdbad', '.'), true);
    __resetLoggerForTest();

    client.shutdown();

    const found = await waitFor(() =>
      getInMemoryErrors().some((e) => /Failed to stop 1 LSP server\(s\): sdbad: kill boom/.test(e.error)),
    );
    assert.ok(found, 'aggregated stop-failure error was reported');
    // The healthy server's stop is NOT reported as a failure.
    assert.ok(
      !getInMemoryErrors().some((e) => /sdok/.test(e.error)),
      'successfully-stopped server is not in the failure aggregate',
    );
  });

  it('a clean shutdown (all servers stop) reports NO aggregated failure', async () => {
    const client = makeClient(() => makeFakeProc({ killThrows: false }));
    assert.equal(await client.ensureServer('sdok', '.'), true);
    __resetLoggerForTest();

    client.shutdown();

    // Give the graceful handshake + exit-grace window time to settle, then assert
    // nothing was aggregated as a failure.
    await new Promise((r) => setTimeout(r, 120));
    assert.ok(
      !getInMemoryErrors().some((e) => /Failed to stop/.test(e.error)),
      'no stop-failure error on a clean shutdown',
    );
  });
});
