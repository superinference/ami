import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { LSPClient } from '../src/lsp/client';
import {
  formatLspHealthError,
  LSP_HEALTH_HINT,
  type LspHealthError,
} from '../src/lsp/health-monitor';
import { registerLspServers, resetLspServers } from '../src/lsp/config';

/**
 * U027 — user-facing LSP failure notification (parity with Claude Code's
 * useLspInitializationNotification toast). Covers:
 *   1. `formatLspHealthError`: the "LSP for <displayName> failed" message with the
 *      `plugin:` prefix stripped and the `/doctor` affordance hint.
 *   2. `LSPClient.onHealthError`: every server failure routed through `recordError`
 *      is emitted to registered listeners, deduped per source:message, with a
 *      throwing sink isolated; `shutdown()` resets the deduper so a later session
 *      re-surfaces a recurring failure.
 */

describe('formatLspHealthError: user-facing toast formatting', () => {
  it('builds "LSP for <source> failed" + the /doctor affordance hint', () => {
    assert.deepEqual(formatLspHealthError({ source: 'typescript', message: 'boom' }), {
      message: 'LSP for typescript failed',
      hint: '/doctor for details',
    });
    assert.equal(LSP_HEALTH_HINT, '/doctor for details');
  });

  it('reports the manager-level aggregate source verbatim', () => {
    assert.equal(
      formatLspHealthError({ source: 'lsp-manager', message: 'x' }).message,
      'LSP for lsp-manager failed',
    );
  });

  it('strips a `plugin:` prefix so a plugin-contributed server shows a clean name', () => {
    assert.equal(
      formatLspHealthError({ source: 'plugin:vue', message: 'x' }).message,
      'LSP for vue failed',
    );
    // Multi-segment: only the first `:`-segment after the prefix is the name.
    assert.equal(
      formatLspHealthError({ source: 'plugin:gopls:extra', message: 'x' }).message,
      'LSP for gopls failed',
    );
    // Degenerate `plugin:` with no name → empty display name (matches the reference
    // `source.split(':')[1] ?? source` edge behavior).
    assert.equal(
      formatLspHealthError({ source: 'plugin:', message: 'x' }).message,
      'LSP for  failed',
    );
  });
});

// ---- E2E: client surfaces real spawn/init failures to listeners --------------

describe('LSPClient U027: onHealthError surfacing (E2E)', () => {
  let dir: string;
  let client: LSPClient;

  before(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-u027-notif-'));
    registerLspServers({
      nbad: { command: 'U027BADCMD', extensionToLanguage: { '.nb': 'nbad' } },
      obad: { command: 'U027OTHERBAD', extensionToLanguage: { '.ob': 'obad' } },
    });
  });

  after(() => {
    resetLspServers();
    if (dir) fs.rmSync(dir, { recursive: true, force: true });
  });

  afterEach(() => client?.shutdown());

  it('emits a spawn/init failure to a registered listener as {source,message}', async () => {
    client = new LSPClient();
    client._isBinaryInstalled = () => false; // every command "not found"
    const got: LspHealthError[] = [];
    client.onHealthError((e) => got.push(e));

    const ok = await client.ensureServer('nbad', dir);
    assert.equal(ok, false, 'spawn failed (command not found)');
    assert.equal(got.length, 1, 'the silent failure was surfaced to the listener');
    assert.equal(got[0].source, 'nbad');
    assert.match(got[0].message, /command not found/);

    // The listener payload formats into the user-facing toast.
    const toast = formatLspHealthError(got[0]);
    assert.equal(toast.message, 'LSP for nbad failed');
    assert.equal(toast.hint, '/doctor for details');
  });

  it('dedupes the SAME failure across retries but surfaces a DISTINCT server', async () => {
    client = new LSPClient();
    client._isBinaryInstalled = () => false;
    const got: LspHealthError[] = [];
    client.onHealthError((e) => got.push(e));

    await client.ensureServer('nbad', dir);
    await client.ensureServer('nbad', dir); // same source:message → deduped
    assert.equal(got.length, 1, 'repeated identical failure shown at most once');

    await client.ensureServer('obad', dir); // different source → new toast
    assert.equal(got.length, 2);
    assert.deepEqual(got.map((e) => e.source).sort(), ['nbad', 'obad']);
  });

  it('isolates a throwing listener so other sinks still fire', async () => {
    client = new LSPClient();
    client._isBinaryInstalled = () => false;
    const got: LspHealthError[] = [];
    client.onHealthError(() => { throw new Error('bad sink'); });
    client.onHealthError((e) => got.push(e));

    await assert.doesNotReject(client.ensureServer('nbad', dir));
    assert.equal(got.length, 1, 'the healthy sink still received the error');
  });

  it('shutdown() resets the deduper so a later session re-surfaces the failure', async () => {
    client = new LSPClient();
    client._isBinaryInstalled = () => false;
    const got: LspHealthError[] = [];
    client.onHealthError((e) => got.push(e));

    await client.ensureServer('nbad', dir);
    assert.equal(got.length, 1);

    client.shutdown(); // clears notifiedHealthErrors, KEEPS the listener
    await client.ensureServer('nbad', dir);
    assert.equal(got.length, 2, 'the same failure surfaces again in the new session');
  });
});
