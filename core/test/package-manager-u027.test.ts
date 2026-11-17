/**
 * U027 parity: ordered Python package-manager detection + install-command
 * construction (Claude Code's detectPythonPackageManager / installIt2,
 * utils/swarm/backends/it2Setup.ts:L40-72).
 *
 * Covers:
 *  - detectPythonPackageManager — probes uv → pipx → pip → pip3 in preference
 *    order, collapses pip3 to the logical 'pip' kind, returns null when none are
 *    installed, and uses the shared session-cached probe by default.
 *  - pythonInstallCommand — the spawn-ready install table for each logical kind.
 *  - pythonBootstrapHint — the human-readable affordance (install command, or a
 *    "none found" message).
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  detectPythonPackageManager,
  pythonInstallCommand,
  pythonBootstrapHint,
} from '../src/utils/package-manager';
import { clearBinaryCache } from '../src/utils/which';

const probe = (installed: string[]) => (c: string) => installed.includes(c);

describe('detectPythonPackageManager: ordered preference + alias collapse', () => {
  it('prefers uv (reported as the "uvx" logical kind) over everything else', () => {
    assert.equal(detectPythonPackageManager(probe(['uv', 'pipx', 'pip', 'pip3'])), 'uvx');
  });

  it('falls back to pipx when uv is absent', () => {
    assert.equal(detectPythonPackageManager(probe(['pipx', 'pip', 'pip3'])), 'pipx');
  });

  it('falls back to pip when only pip is present', () => {
    assert.equal(detectPythonPackageManager(probe(['pip'])), 'pip');
  });

  it('collapses a pip3-only system to the logical "pip" kind', () => {
    assert.equal(detectPythonPackageManager(probe(['pip3'])), 'pip');
  });

  it('returns null when no Python package manager is installed', () => {
    assert.equal(detectPythonPackageManager(probe([])), null);
  });

  it('uses the shared isBinaryInstalled probe by default (empty PATH ⇒ null)', () => {
    const saved = process.env.PATH;
    const empty = fs.mkdtempSync(path.join(os.tmpdir(), 'pm-empty-'));
    try {
      process.env.PATH = empty; // no uv/pipx/pip/pip3 reachable here
      clearBinaryCache();
      assert.equal(detectPythonPackageManager(), null);
    } finally {
      process.env.PATH = saved;
      clearBinaryCache();
      fs.rmSync(empty, { recursive: true, force: true });
    }
  });
});

describe('pythonInstallCommand: spawn-ready install table', () => {
  it('uv tool install for the uvx kind', () => {
    assert.deepEqual(pythonInstallCommand('ruff', 'uvx'), { cmd: 'uv', args: ['tool', 'install', 'ruff'] });
  });
  it('pipx install for the pipx kind', () => {
    assert.deepEqual(pythonInstallCommand('ruff', 'pipx'), { cmd: 'pipx', args: ['install', 'ruff'] });
  });
  it('pip install --user for the pip kind', () => {
    assert.deepEqual(pythonInstallCommand('ruff', 'pip'), { cmd: 'pip', args: ['install', '--user', 'ruff'] });
  });
});

describe('pythonBootstrapHint: human-readable affordance', () => {
  it('names the exact install command for the best available manager', () => {
    assert.equal(
      pythonBootstrapHint('python-lsp-server', probe(['pipx', 'pip'])),
      'Install it with: pipx install python-lsp-server',
    );
  });

  it('reports when no Python package manager is available', () => {
    assert.equal(
      pythonBootstrapHint('ruff', probe([])),
      'No Python package manager (uv/pipx/pip) found to install "ruff".',
    );
  });

  it('uses the shared probe by default (empty PATH ⇒ none-found message)', () => {
    const saved = process.env.PATH;
    const empty = fs.mkdtempSync(path.join(os.tmpdir(), 'pm-empty2-'));
    try {
      process.env.PATH = empty;
      clearBinaryCache();
      assert.equal(
        pythonBootstrapHint('ruff'),
        'No Python package manager (uv/pipx/pip) found to install "ruff".',
      );
    } finally {
      process.env.PATH = saved;
      clearBinaryCache();
      fs.rmSync(empty, { recursive: true, force: true });
    }
  });

  afterEach(() => clearBinaryCache());
});
