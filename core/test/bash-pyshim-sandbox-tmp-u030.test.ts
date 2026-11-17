/**
 * U030 — sandbox-writable tmpdir fallback for the python shim aux file.
 *
 * Under a restrictive sandbox the global `os.tmpdir()` is NOT writable — only the
 * sandbox-pinned tmp dir (exported via `AMI_TMPDIR`/`CLAUDE_CODE_TMPDIR`) is. The
 * python→python3 shim must be materialized there, so `ensurePythonShimDir` now
 * routes its aux-file write through `resolveTmpBase()` instead of `os.tmpdir()`.
 *
 * RED on revert: switching the shim dir base back to `os.tmpdir()` makes the
 * "under AMI_TMPDIR" assertion fail (the shim lands in the system /tmp instead).
 *
 * Run: cd core/core && node --import tsx --test test/bash-pyshim-sandbox-tmp-u030.test.ts
 */
import { describe, it, afterEach, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { ensurePythonShimDir, _resetPythonShimCache } from '../src/tools/bash';

const tmpdirs: string[] = [];
let savedAmiTmp: string | undefined;
let savedCcTmp: string | undefined;

function binDir(tools: string[]): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'pyshim-sbx-fixture-'));
  tmpdirs.push(d);
  for (const t of tools) {
    const p = path.join(d, t);
    fs.writeFileSync(p, `#!/bin/sh\necho ${t} "$@"\n`, { mode: 0o755 });
    fs.chmodSync(p, 0o755);
  }
  return d;
}

beforeEach(() => {
  _resetPythonShimCache();
  savedAmiTmp = process.env.AMI_TMPDIR;
  savedCcTmp = process.env.CLAUDE_CODE_TMPDIR;
});
afterEach(() => {
  _resetPythonShimCache();
  if (savedAmiTmp === undefined) delete process.env.AMI_TMPDIR; else process.env.AMI_TMPDIR = savedAmiTmp;
  if (savedCcTmp === undefined) delete process.env.CLAUDE_CODE_TMPDIR; else process.env.CLAUDE_CODE_TMPDIR = savedCcTmp;
  for (const d of tmpdirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
});

describe('ensurePythonShimDir sandbox-writable tmp fallback (U030)', () => {
  it('materializes the shim under AMI_TMPDIR when the sandbox tmp override is set', (t) => {
    if (process.platform === 'win32') return t.skip('POSIX-only shim');
    const sandboxTmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-sandbox-tmp-'));
    tmpdirs.push(sandboxTmp);
    process.env.AMI_TMPDIR = sandboxTmp;
    delete process.env.CLAUDE_CODE_TMPDIR;

    const bin = binDir(['python3']); // python absent, python3 present → shim is created
    const shimDir = ensurePythonShimDir(bin);
    assert.ok(shimDir, 'a shim dir should be returned');
    tmpdirs.push(shimDir!);
    const realBase = fs.realpathSync(sandboxTmp);
    const realShim = fs.realpathSync(shimDir!);
    assert.ok(
      realShim.startsWith(realBase + path.sep),
      `shim dir ${realShim} must be nested under the sandbox tmp base ${realBase}`,
    );
    assert.ok(fs.existsSync(path.join(shimDir!, 'python')), 'python shim file exists');
  });

  it('honors CLAUDE_CODE_TMPDIR as the sandbox tmp base too', (t) => {
    if (process.platform === 'win32') return t.skip('POSIX-only shim');
    const sandboxTmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cc-sandbox-tmp-'));
    tmpdirs.push(sandboxTmp);
    delete process.env.AMI_TMPDIR;
    process.env.CLAUDE_CODE_TMPDIR = sandboxTmp;

    const bin = binDir(['python3']);
    const shimDir = ensurePythonShimDir(bin);
    assert.ok(shimDir, 'a shim dir should be returned');
    tmpdirs.push(shimDir!);
    const realBase = fs.realpathSync(sandboxTmp);
    const realShim = fs.realpathSync(shimDir!);
    assert.ok(
      realShim.startsWith(realBase + path.sep),
      `shim dir ${realShim} must be nested under the sandbox tmp base ${realBase}`,
    );
  });

  it('falls back to the system tmpdir when no sandbox override is set', (t) => {
    if (process.platform === 'win32') return t.skip('POSIX-only shim');
    delete process.env.AMI_TMPDIR;
    delete process.env.CLAUDE_CODE_TMPDIR;
    const bin = binDir(['python3']);
    const shimDir = ensurePythonShimDir(bin);
    assert.ok(shimDir, 'a shim dir should be returned');
    tmpdirs.push(shimDir!);
    const realShim = fs.realpathSync(shimDir!);
    const realSys = fs.realpathSync(os.tmpdir());
    assert.ok(
      realShim.startsWith(realSys + path.sep),
      `shim dir ${realShim} must fall back under the system tmpdir ${realSys}`,
    );
  });
});
