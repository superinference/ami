import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import type { ToolContext } from '../src/types';

/**
 * U039/w99 — file_edit routes its filesystem read failures through the shared
 * typed errno helpers (core/core/src/utils/errno.ts: getErrnoCode / getErrnoPath
 * / isENOENT / isFsInaccessible), so a grouped "path missing / inaccessible /
 * unreachable" errno (ENOTDIR / ELOOP / EACCES / EPERM — the configured
 * FS_INACCESSIBLE_CODES set, excluding ENOENT which has its own not-found
 * recovery) surfaces a distinct, actionable diagnostic instead of the generic
 * "Error reading file" / silent new-file fallback.
 */

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: tmpDir, abortSignal: new AbortController().signal, filesRead: new Set<string>(), ...overrides };
}

beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fe-w99-')); });
afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

describe('file_edit fs-inaccessible errno routing (U039/w99)', () => {
  it('single-replace: a non-ENOENT inaccessible read (ENOTDIR) yields the targeted "inaccessible, or unreachable" diagnostic', async () => {
    // A regular file used as a directory component makes readFile throw ENOTDIR,
    // which isFsInaccessible classifies as inaccessible — deterministic without
    // relying on chmod/root.
    fs.writeFileSync(path.join(tmpDir, 'afile'), 'x');
    const target = path.join(tmpDir, 'afile', 'child.txt');
    const result = await fileEditTool.execute(
      { file_path: target, old_string: 'a', new_string: 'b' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /ENOTDIR/);
    assert.match(result.output, /missing, inaccessible, or unreachable/);
    // It is NOT the generic fallback nor the ENOENT "File not found" recovery.
    assert.doesNotMatch(result.output, /^Error reading file:/);
    assert.doesNotMatch(result.output, /File not found/);
  });

  it('single-replace: a genuinely absent file (ENOENT) keeps the "File not found" recovery, not the inaccessible message', async () => {
    const target = path.join(tmpDir, 'nope.txt');
    const result = await fileEditTool.execute(
      { file_path: target, old_string: 'a', new_string: 'b' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /File not found/);
    assert.doesNotMatch(result.output, /missing, inaccessible, or unreachable/);
  });

  it('single-replace: an EACCES-unreadable existing file surfaces the inaccessible diagnostic (skipped as root)', async () => {
    const secret = path.join(tmpDir, 'secret.txt');
    fs.writeFileSync(secret, 'secret\n');
    fs.chmodSync(secret, 0o000);
    let readable = true;
    try { fs.readFileSync(secret); } catch { readable = false; }
    if (readable) { fs.chmodSync(secret, 0o644); return; } // root — EACCES not enforced

    const result = await fileEditTool.execute(
      { file_path: secret, old_string: 's', new_string: 'x' },
      ctx({ filesRead: new Set([secret]) }),
    );
    fs.chmodSync(secret, 0o644);
    assert.equal(result.isError, true);
    assert.match(result.output, /EACCES/);
    assert.match(result.output, /missing, inaccessible, or unreachable/);
  });

  it('overwrite: an EACCES-unreadable existing file is not silently clobbered and reports the inaccessible hint (skipped as root)', async () => {
    const secret = path.join(tmpDir, 'keep.txt');
    fs.writeFileSync(secret, 'must survive\n');
    fs.chmodSync(secret, 0o000);
    let readable = true;
    try { fs.readFileSync(secret); } catch { readable = false; }
    if (readable) { fs.chmodSync(secret, 0o644); return; } // root — EACCES not enforced

    const result = await fileEditTool.execute(
      { file_path: secret, new_string: 'overwrite' },
      ctx({ filesRead: new Set([secret]) }),
    );
    fs.chmodSync(secret, 0o644);
    assert.equal(result.isError, true);
    assert.match(result.output, /cannot read existing file/);
    assert.match(result.output, /missing, inaccessible, or unreachable/);
    assert.equal(fs.readFileSync(secret, 'utf-8'), 'must survive\n');
  });
});
