// Unit U039 (wave w70) — parity gaps for core/core/src/tools/file-edit.ts:
//  - Atomicity: a synchronous staleness RE-CHECK runs immediately before the
//    disk write, closing the race window opened by the async pre-write
//    file-history backup (parity: Claude keeps "no async operations between the
//    staleness check and writing to disk"). Covers all write sites:
//    writeEntireFile, applySequentialEdits, single-replace, and replace_all.
//  - `mode` option: create/overwrite with explicit octal permission bits
//    (parseFileMode + atomic create-with-mode via writeFileAtomicSync).
//  - Overwrite diff degrades to a size-only summary past EDIT_DIFF_SCAN_MAX_BYTES
//    (parity: Claude's MAX_SCAN_BYTES fallback to the plain "create" view).
//  - Large-needle short-circuit: an old_string longer than the whole file can
//    never match, so the full-file scan is skipped with a precise diagnostic
//    (parity: Claude's CHUNK_SIZE large-old_string fast path).
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool, fileHistoryHooks } from '../src/tools/file-edit';
import { parseFileMode, writeFileAtomicSync } from '../src/tools/tool-utils';
import { getFileCache } from '../src/file-cache';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    detachedMode: true,
    ...overrides,
  };
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u039w70-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// parseFileMode — octal permission-bits parser
// ---------------------------------------------------------------------------

describe('parseFileMode helper', () => {
  it('accepts octal strings with and without a leading zero / 0o', () => {
    assert.equal(parseFileMode('600'), 0o600);
    assert.equal(parseFileMode('0600'), 0o600);
    assert.equal(parseFileMode('0o755'), 0o755);
    assert.equal(parseFileMode(' 644 '), 0o644);
  });

  it('accepts an in-range number verbatim', () => {
    assert.equal(parseFileMode(0o700), 0o700);
    assert.equal(parseFileMode(0), 0);
  });

  it('rejects out-of-range / non-octal / wrong-typed values as undefined', () => {
    assert.equal(parseFileMode('999'), undefined); // 9 is not an octal digit
    assert.equal(parseFileMode('xyz'), undefined);
    assert.equal(parseFileMode('12'), undefined); // too short (<3 digits)
    assert.equal(parseFileMode('17777'), undefined); // exceeds 0o7777 after parse width? reject by width/range
    assert.equal(parseFileMode(-1), undefined);
    assert.equal(parseFileMode(1.5), undefined);
    assert.equal(parseFileMode(0o10000), undefined); // > 0o7777
    assert.equal(parseFileMode(undefined), undefined);
    assert.equal(parseFileMode(null), undefined);
    assert.equal(parseFileMode({}), undefined);
  });
});

describe('writeFileAtomicSync — explicit mode option', () => {
  it('applies an explicit mode on create and overrides preservation on rewrite', () => {
    const f = path.join(tmpDir, 'perms.txt');
    writeFileAtomicSync(f, 'a', { mode: 0o600 });
    assert.equal(fs.statSync(f).mode & 0o777, 0o600);
    // rewrite with a new explicit mode overrides the preserved bits
    writeFileAtomicSync(f, 'b', { mode: 0o640 });
    assert.equal(fs.statSync(f).mode & 0o777, 0o640);
  });
});

// ---------------------------------------------------------------------------
// file_edit `mode` option (gap: file/dir creation mode permission bits)
// ---------------------------------------------------------------------------

describe('file_edit mode option', () => {
  it('creates a private file (600) with exact permission bits', async () => {
    const f = path.join(tmpDir, 'secret.env');
    const r = await fileEditTool.execute({ file_path: f, content: 'TOKEN=x\n', mode: '600' }, ctx());
    assert.equal(r.isError, undefined);
    assert.equal(fs.statSync(f).mode & 0o777, 0o600);
  });

  it('creates an executable script (755) with exact permission bits', async () => {
    const f = path.join(tmpDir, 'run.sh');
    const r = await fileEditTool.execute({ file_path: f, content: '#!/bin/sh\necho hi\n', mode: '0755' }, ctx());
    assert.equal(r.isError, undefined);
    assert.equal(fs.statSync(f).mode & 0o777, 0o755);
  });

  it('ignores an invalid mode value (creates with default perms, no error)', async () => {
    const f = path.join(tmpDir, 'plain.txt');
    const r = await fileEditTool.execute({ file_path: f, content: 'hi\n', mode: 'not-a-mode' }, ctx());
    assert.equal(r.isError, undefined);
    assert.notEqual(fs.statSync(f).mode & 0o777, 0o600);
  });

  it('preserves existing permissions on an in-place replace (mode is a write-mode knob)', async () => {
    const f = path.join(tmpDir, 'keep.txt');
    fs.writeFileSync(f, 'alpha beta\n');
    fs.chmodSync(f, 0o600);
    const r = await fileEditTool.execute({ file_path: f, old_string: 'alpha', new_string: 'gamma' }, ctx());
    assert.equal(r.isError, undefined);
    assert.equal(fs.statSync(f).mode & 0o777, 0o600);
  });

  it('accepts `mode` as a known parameter (not rejected by the strict schema)', async () => {
    const f = path.join(tmpDir, 'schema.txt');
    const r = await fileEditTool.execute({ file_path: f, content: 'x\n', mode: '644' }, ctx());
    assert.equal(r.isError, undefined);
    assert.ok(!/Unknown parameter/.test(r.output));
  });
});

// ---------------------------------------------------------------------------
// Overwrite diff degrades past EDIT_DIFF_SCAN_MAX_BYTES (OOM/cost boundary)
// ---------------------------------------------------------------------------

describe('file_edit overwrite diff degradation (large existing file)', () => {
  it('emits a size-only summary (no @@ diff, empty structuredPatch) when the existing file exceeds the diff cap', async () => {
    const f = path.join(tmpDir, 'big.log');
    // 11 MiB existing file (> the 10 MiB EDIT_DIFF_SCAN_MAX_BYTES cap).
    fs.writeFileSync(f, 'a'.repeat(11 * 1024 * 1024));
    const r = await fileEditTool.execute({ file_path: f, content: 'small\n' }, ctx());
    assert.equal(r.isError, undefined);
    assert.match(r.output, /diff omitted/);
    assert.match(r.output, /diff cap/);
    assert.ok(!r.output.includes('@@'), 'no unified-diff hunk header for a degraded large write');
    assert.deepEqual(r.structuredPatch, []);
    assert.equal(fs.readFileSync(f, 'utf-8'), 'small\n');
  });

  it('still emits a real diff for a normal-sized overwrite', async () => {
    const f = path.join(tmpDir, 'small.txt');
    fs.writeFileSync(f, 'one\ntwo\n');
    const r = await fileEditTool.execute({ file_path: f, content: 'one\nTWO\n' }, ctx());
    assert.equal(r.isError, undefined);
    assert.ok(!/diff omitted/.test(r.output));
    assert.ok(r.structuredPatch && r.structuredPatch.length > 0);
  });
});

// ---------------------------------------------------------------------------
// Large-needle short-circuit (old_string longer than the whole file)
// ---------------------------------------------------------------------------

describe('file_edit large-needle short-circuit', () => {
  it('rejects an old_string larger than the entire file with a precise diagnostic', async () => {
    const f = path.join(tmpDir, 'tiny.txt');
    fs.writeFileSync(f, 'abc\n');
    const huge = 'x'.repeat(10_000);
    const r = await fileEditTool.execute({ file_path: f, old_string: huge, new_string: 'y' }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /larger than the entire file/);
    // original file is untouched
    assert.equal(fs.readFileSync(f, 'utf-8'), 'abc\n');
  });
});

// ---------------------------------------------------------------------------
// Atomicity: synchronous staleness re-check immediately before the write.
// The pre-write backup (fileHistoryHooks.track) is mocked to inject a concurrent
// on-disk modification DURING its await; the re-check must catch it and block the
// clobber. (Red-green: without the re-check the write silently clobbers.)
// ---------------------------------------------------------------------------

function seedRead(file: string, content: string): Set<string> {
  // Simulate a prior file_read: mark the file read and cache its content so the
  // staleness machinery has a baseline to compare the concurrent change against.
  const set = new Set<string>([file]);
  getFileCache(tmpDir).setWritten(file, content);
  return set;
}

describe('file_edit atomicity — staleness re-check before write', () => {
  it('single-replace: blocks a clobber when the file changes during the backup await', async (t) => {
    const f = path.join(tmpDir, 'race1.txt');
    fs.writeFileSync(f, 'hello world\n');
    const filesRead = seedRead(f, 'hello world\n');
    t.mock.method(fileHistoryHooks, 'track', async () => {
      fs.writeFileSync(f, 'CONCURRENT EDIT\n'); // lands inside the critical window
    });
    const r = await fileEditTool.execute(
      { file_path: f, old_string: 'world', new_string: 'there' },
      ctx({ filesRead }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /modified since you last read it/);
    // the concurrent write was NOT clobbered
    assert.equal(fs.readFileSync(f, 'utf-8'), 'CONCURRENT EDIT\n');
  });

  it('replace_all: blocks a clobber when the file changes during the backup await', async (t) => {
    const f = path.join(tmpDir, 'race2.txt');
    fs.writeFileSync(f, 'x x x\n');
    const filesRead = seedRead(f, 'x x x\n');
    t.mock.method(fileHistoryHooks, 'track', async () => {
      fs.writeFileSync(f, 'CONCURRENT\n');
    });
    const r = await fileEditTool.execute(
      { file_path: f, old_string: 'x', new_string: 'y', replace_all: true },
      ctx({ filesRead }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /modified since you last read it/);
    assert.equal(fs.readFileSync(f, 'utf-8'), 'CONCURRENT\n');
  });

  it('overwrite (write mode): blocks a clobber when the file changes during the backup await', async (t) => {
    const f = path.join(tmpDir, 'race3.txt');
    fs.writeFileSync(f, 'original\n');
    const filesRead = seedRead(f, 'original\n');
    t.mock.method(fileHistoryHooks, 'track', async () => {
      fs.writeFileSync(f, 'CONCURRENT\n');
    });
    const r = await fileEditTool.execute(
      { file_path: f, content: 'brand new contents\n' },
      ctx({ filesRead }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /modified since you last read it/);
    assert.equal(fs.readFileSync(f, 'utf-8'), 'CONCURRENT\n');
  });

  it('sequential edits: blocks a clobber when the file changes during the backup await', async (t) => {
    const f = path.join(tmpDir, 'race4.txt');
    fs.writeFileSync(f, 'alpha\nbeta\n');
    const filesRead = seedRead(f, 'alpha\nbeta\n');
    t.mock.method(fileHistoryHooks, 'track', async () => {
      fs.writeFileSync(f, 'CONCURRENT\n');
    });
    const r = await fileEditTool.execute(
      { file_path: f, edits: [{ old_string: 'alpha', new_string: 'ALPHA' }] },
      ctx({ filesRead }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /modified since you last read it/);
    assert.equal(fs.readFileSync(f, 'utf-8'), 'CONCURRENT\n');
  });

  it('does NOT block when nothing changes during the backup (happy path still writes)', async (t) => {
    const f = path.join(tmpDir, 'nochange.txt');
    fs.writeFileSync(f, 'keep me\n');
    const filesRead = seedRead(f, 'keep me\n');
    let backedUp = false;
    t.mock.method(fileHistoryHooks, 'track', async () => { backedUp = true; });
    const r = await fileEditTool.execute(
      { file_path: f, old_string: 'keep', new_string: 'KEEP' },
      ctx({ filesRead }),
    );
    assert.equal(r.isError, undefined);
    assert.ok(backedUp, 'backup hook still invoked');
    assert.equal(fs.readFileSync(f, 'utf-8'), 'KEEP me\n');
  });
});
