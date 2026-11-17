/**
 * U027 parity: a Python-installable LSP server whose binary is missing surfaces
 * an actionable bootstrap hint in its "command not found" diagnostic (naming the
 * exact `uv/pipx/pip install` command), instead of only "not found". AMI never
 * auto-installs — the hint is surfaced via lastError (→ /doctor) for the user.
 *
 * Covers:
 *  - validateLspServerConfig — accepts a valid `pythonPackage`, rejects a
 *    non-string / empty-string one.
 *  - LSPClient.ensureServer — when a server with a `pythonPackage` is not on PATH,
 *    lastError includes the install command for the best available Python PM;
 *    an npm-based server (no `pythonPackage`) gets no python hint.
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import { LSPClient } from '../src/lsp/client';
import { registerLspServers, resetLspServers, validateLspServerConfig } from '../src/lsp/config';

describe('validateLspServerConfig: pythonPackage field', () => {
  it('accepts a valid non-empty pythonPackage', () => {
    const c = validateLspServerConfig('pylsp', {
      command: 'pylsp',
      pythonPackage: 'python-lsp-server',
    });
    assert.equal(c.pythonPackage, 'python-lsp-server');
  });

  it('rejects a non-string pythonPackage', () => {
    assert.throws(
      () => validateLspServerConfig('pylsp', { command: 'pylsp', pythonPackage: 42 }),
      /"pythonPackage" must be a non-empty string/,
    );
  });

  it('rejects an empty pythonPackage', () => {
    assert.throws(
      () => validateLspServerConfig('pylsp', { command: 'pylsp', pythonPackage: '' }),
      /"pythonPackage" must be a non-empty string/,
    );
  });

  it('omits pythonPackage when absent', () => {
    const c = validateLspServerConfig('pylsp', { command: 'pylsp' });
    assert.equal(c.pythonPackage, undefined);
  });
});

describe('LSPClient.ensureServer: Python bootstrap hint on missing server', () => {
  let client: LSPClient;
  afterEach(() => {
    client?.shutdown();
    resetLspServers();
  });

  it('appends the install command for the best available Python package manager', async () => {
    registerLspServers({
      pylsp: {
        command: 'pylsp',
        pythonPackage: 'python-lsp-server',
        extensionToLanguage: { '.py': 'pylsp' },
      },
    });
    client = new LSPClient();
    // pylsp itself is absent; the user HAS pipx available.
    client._isBinaryInstalled = (c) => c === 'pipx';
    let spawnedAny = false;
    client._spawn = (() => { spawnedAny = true; throw new Error('must not spawn'); }) as unknown as typeof client._spawn;

    const ok = await client.ensureServer('pylsp', process.cwd());
    assert.equal(ok, false);
    assert.equal(spawnedAny, false);
    const status = client.getServerStatus('pylsp');
    assert.match(status?.lastError ?? '', /command not found: pylsp/);
    assert.match(status?.lastError ?? '', /Install it with: pipx install python-lsp-server/);
  });

  it('reports "no Python package manager" when none is available', async () => {
    registerLspServers({
      ruffserver: {
        command: 'ruff',
        pythonPackage: 'ruff',
        extensionToLanguage: { '.py': 'ruffserver' },
      },
    });
    client = new LSPClient();
    client._isBinaryInstalled = () => false;
    client._spawn = (() => { throw new Error('must not spawn'); }) as unknown as typeof client._spawn;

    const ok = await client.ensureServer('ruffserver', process.cwd());
    assert.equal(ok, false);
    const status = client.getServerStatus('ruffserver');
    assert.match(status?.lastError ?? '', /No Python package manager \(uv\/pipx\/pip\) found to install "ruff"/);
  });

  it('gives NO python hint for an npm-based server without pythonPackage', async () => {
    registerLspServers({
      npmserver: {
        command: 'definitely-not-installed-xyz',
        extensionToLanguage: { '.zz': 'npmserver' },
      },
    });
    client = new LSPClient();
    client._isBinaryInstalled = () => false;
    client._spawn = (() => { throw new Error('must not spawn'); }) as unknown as typeof client._spawn;

    const ok = await client.ensureServer('npmserver', process.cwd());
    assert.equal(ok, false);
    const status = client.getServerStatus('npmserver');
    assert.match(status?.lastError ?? '', /command not found: definitely-not-installed-xyz$/);
    assert.doesNotMatch(status?.lastError ?? '', /Install it with|No Python package manager/);
  });
});
