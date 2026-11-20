/**
 * U058 — file_edit emits a `structuredPatchTruncated` signal on the diff-cost-cap
 * success paths (file exceeds the 10 MiB diff-scan cap). This is the machine-readable
 * analogue of the reference's readEditContext `truncated: true`: the edit landed but its
 * per-hunk location could not be rendered, so a replay/introspection surface (the session
 * journal) can say "edit location not shown — file too large to scan" instead of mistaking
 * the empty `structuredPatch: []` for "no change".
 *
 * RED before the fix: the capped paths returned `structuredPatch: []` with NO truncated
 * flag, so the two cases were indistinguishable.
 *
 * Run: cd core/core && node --import tsx --test test/parity-file-edit-structured-patch-truncated-u058.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { fileEditTool } from '../src/tools/file-edit';
import type { ToolContext } from '../src/types';

let tmpDir: string;
function ctx(): ToolContext {
  return { cwd: tmpDir, abortSignal: new AbortController().signal };
}

beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fe-u058-')); });
afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

// Pushes either side of the edit past the 10 MiB diff-scan cap.
const BIG_FILLER = 'x'.repeat(11 * 1024 * 1024);

describe('file_edit structuredPatchTruncated signal', () => {
  it('single-replace on a >10 MiB file sets structuredPatchTruncated with an empty patch', async () => {
    const file = path.join(tmpDir, 'big.txt');
    fs.writeFileSync(file, `NEEDLE = 0;\n${BIG_FILLER}\n`);
    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'NEEDLE = 0;', new_string: 'NEEDLE = 1;' },
      ctx(),
    );
    assert.deepEqual(res.structuredPatch, []);
    assert.equal(res.structuredPatchTruncated, true);
  });

  it('replace_all on a >10 MiB file sets structuredPatchTruncated', async () => {
    const file = path.join(tmpDir, 'big-all.txt');
    fs.writeFileSync(file, `DUP\nDUP\n${BIG_FILLER}\n`);
    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'DUP', new_string: 'REPL', replace_all: true },
      ctx(),
    );
    assert.deepEqual(res.structuredPatch, []);
    assert.equal(res.structuredPatchTruncated, true);
  });

  it('a normal small edit carries a populated structuredPatch and NO truncated flag', async () => {
    const file = path.join(tmpDir, 'small.txt');
    fs.writeFileSync(file, 'alpha\nbeta\ngamma\n');
    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'beta', new_string: 'BETA' },
      ctx(),
    );
    assert.ok(Array.isArray(res.structuredPatch) && res.structuredPatch.length > 0,
      'a small edit must produce a real per-hunk patch');
    assert.equal(res.structuredPatchTruncated, undefined,
      'a non-capped edit must NOT set the truncation signal');
  });

  it('a brand-new file write produces a synthetic all-additions hunk (not truncated)', async () => {
    const file = path.join(tmpDir, 'created.txt');
    const res = await fileEditTool.execute(
      { file_path: file, content: 'line one\nline two\n' },
      ctx(),
    );
    assert.ok(Array.isArray(res.structuredPatch) && res.structuredPatch.length > 0,
      'a new file must still get a structured patch (synthetic hunk)');
    assert.equal(res.structuredPatchTruncated, undefined);
  });
});
