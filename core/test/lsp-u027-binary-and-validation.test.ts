/**
 * U027 parity: retry progress hook, untrusted publishDiagnostics validation, and
 * the empty-command guard on LSP server start. (The cross-platform, cached
 * binary-presence check `isBinaryInstalled` lives in utils/which.ts and is tested
 * in which.test.ts — this unit wires it into the LSP server launch.)
 *
 * Covers:
 *  - retryWithBackoff `onRetry` — fires once per scheduled retry with the
 *    attempt counter + backoff delay, never on success or the final failure.
 *  - LSPClient.onPublishDiagnostics — structural validation of untrusted
 *    JSON-RPC notification params (Claude's passiveFeedback.ts).
 *  - LSPClient.ensureServer — empty/whitespace server command resolves to
 *    "command not found" without a filesystem probe.
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import { retryWithBackoff } from '../src/mcp/oauth';
import { LSPClient } from '../src/lsp/client';
import { registerLspServers, resetLspServers } from '../src/lsp/config';
import { getInMemoryErrors, __resetLoggerForTest } from '../src/logger';

describe('retryWithBackoff: onRetry progress hook', () => {
  const sleep = async (): Promise<void> => {};

  it('fires once per scheduled retry with the attempt counter + backoff delay', async () => {
    const seen: Array<{ attempt: number; delayMs: number; msg: string }> = [];
    let n = 0;
    const result = await retryWithBackoff(
      async () => {
        n++;
        if (n < 3) throw new Error(`fail ${n}`);
        return 'ok';
      },
      {
        retries: 5,
        baseMs: 500,
        sleep,
        onRetry: ({ attempt, delayMs, error }) =>
          seen.push({ attempt, delayMs, msg: (error as Error).message }),
      },
    );
    assert.equal(result, 'ok');
    // Two failures → two retries scheduled; the successful third call fires none.
    assert.deepEqual(seen, [
      { attempt: 1, delayMs: 500, msg: 'fail 1' },
      { attempt: 2, delayMs: 1000, msg: 'fail 2' },
    ]);
  });

  it('does NOT fire on the final (exhausted) failure', async () => {
    let fired = 0;
    await assert.rejects(
      retryWithBackoff(async () => { throw new Error('always'); }, {
        retries: 2,
        baseMs: 10,
        sleep,
        onRetry: () => { fired++; },
      }),
    );
    // retries=2 → attempts at 0 and 1 schedule a retry (2 fires); attempt 2 is final.
    assert.equal(fired, 2);
  });

  it('does NOT fire when the first attempt succeeds', async () => {
    let fired = 0;
    const r = await retryWithBackoff(async () => 'first', { onRetry: () => { fired++; } });
    assert.equal(r, 'first');
    assert.equal(fired, 0);
  });
});

describe('LSPClient.onPublishDiagnostics: structural validation of untrusted params', () => {
  afterEach(() => __resetLoggerForTest());

  const call = (client: LSPClient, params: unknown): void =>
    (client as unknown as { onPublishDiagnostics(lang: string, p: unknown): void }).onPublishDiagnostics('go', params);

  it('drops malformed params and records a contextual error', () => {
    for (const bad of [
      null,
      undefined,
      'a string',
      42,
      {},
      { uri: 'file:///a.go' }, // missing diagnostics
      { diagnostics: [] }, // missing uri
      { uri: 123, diagnostics: [] }, // non-string uri
      { uri: 'file:///a.go', diagnostics: 'oops' }, // non-array diagnostics
    ]) {
      __resetLoggerForTest();
      const client = new LSPClient();
      call(client, bad);
      assert.equal(client.getAllDiagnostics().length, 0, `must not store: ${JSON.stringify(bad)}`);
      const errs = getInMemoryErrors();
      assert.ok(
        errs.some((e) => /invalid diagnostic params \(missing uri or diagnostics\)/.test(e.error)),
        `must log for: ${JSON.stringify(bad)}`,
      );
    }
  });

  it('stores well-formed diagnostics', () => {
    const client = new LSPClient();
    call(client, {
      uri: 'file:///proj/main.go',
      diagnostics: [{ message: 'oops', severity: 1, range: { start: { line: 1, character: 0 }, end: { line: 1, character: 4 } } }],
    });
    const all = client.getAllDiagnostics();
    assert.equal(all.length, 1);
    assert.equal(all[0].diagnostics.length, 1);
  });

  it('accepts an empty diagnostics array (clear) without logging an error', () => {
    const client = new LSPClient();
    // Seed a non-empty set, then clear it via an empty publish.
    call(client, { uri: 'file:///proj/x.go', diagnostics: [{ message: 'e', severity: 1, range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } }] });
    assert.equal(client.getAllDiagnostics().length, 1);
    call(client, { uri: 'file:///proj/x.go', diagnostics: [] });
    assert.equal(client.getAllDiagnostics().length, 0, 'empty publish clears the file');
    assert.equal(getInMemoryErrors().length, 0, 'empty is valid, not an error');
  });
});

describe('LSPClient.ensureServer: empty-command guard (#49)', () => {
  afterEach(() => { resetLspServers(); __resetLoggerForTest(); });

  it('treats a whitespace-only server command as not found, with no spawn', async () => {
    registerLspServers({ emptylang: { command: '   ', args: [], extensionToLanguage: { '.empty': 'emptylang' } } });
    const client = new LSPClient();
    let spawned = false;
    client._spawn = (() => { spawned = true; throw new Error('must not spawn'); }) as unknown as typeof client._spawn;
    const ok = await client.ensureServer('emptylang', '/tmp');
    assert.equal(ok, false);
    assert.equal(spawned, false);
    const status = client.getServerStatus('emptylang');
    assert.equal(status?.state, 'error');
    assert.match(status?.lastError ?? '', /command not found/);
  });
});
