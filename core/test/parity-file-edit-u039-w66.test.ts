// Unit U039 (wave w66) — parity gaps for core/core/src/tools/file-edit.ts and the
// shared diff machinery in tool-utils.ts:
//   - multi-hunk unified diff (separate @@ hunks per non-contiguous change region)
//   - multi-hunk Myers/LCS for disjoint changes with per-region context
//   - bounded multi-hunk diff (no unbounded single hunk on scattered edits)
//   - caller-tunable contextLines (default 3)
//   - singleHunk flag (context 100000)
//   - ignoreWhitespace comparison option
//   - byte cap with a '... [N lines truncated] ...' marker
//   - countLinesChanged (additions vs removals, new-file all-added)
//   - diff-computation guard + graceful bounded fallback on degenerate inputs
//   - multi-edit substring guard (old_string ⊂ a prior edit's new_string)
//   - unified diff appended to multi-edit (edits:[]) success output
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import { buildUnifiedDiff, countLinesChanged, DIFF_SINGLE_HUNK_CONTEXT } from '../src/tools/tool-utils';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u039w66-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function hunkCount(diff: string): number {
  return diff.split('\n').filter(l => l.startsWith('@@ ')).length;
}

// ---------------------------------------------------------------------------
// buildUnifiedDiff — multi-hunk output
// ---------------------------------------------------------------------------

describe('buildUnifiedDiff — multi-hunk', () => {
  const base = Array.from({ length: 30 }, (_, i) => `line${i}`).join('\n');

  it('emits SEPARATE @@ hunks for two far-apart change regions', () => {
    const lines = base.split('\n');
    lines[2] = 'CHANGED_TOP';
    lines[25] = 'CHANGED_BOTTOM';
    const diff = buildUnifiedDiff(base, lines.join('\n'), '/f.txt');
    assert.equal(hunkCount(diff), 2, diff);
    assert.ok(diff.includes('-line2'));
    assert.ok(diff.includes('+CHANGED_TOP'));
    assert.ok(diff.includes('-line25'));
    assert.ok(diff.includes('+CHANGED_BOTTOM'));
    // The unrelated middle lines must NOT appear inside either hunk.
    assert.ok(!diff.includes(' line14'), diff);
  });

  it('merges two changes closer than 2*context into ONE hunk', () => {
    const lines = base.split('\n');
    lines[10] = 'A';
    lines[13] = 'B'; // gap of 2 unchanged lines (< 2*3)
    const diff = buildUnifiedDiff(base, lines.join('\n'), '/f.txt');
    assert.equal(hunkCount(diff), 1, diff);
  });

  it('shows a modified line as a removal + an addition, not whole-file', () => {
    const diff = buildUnifiedDiff('a\nb\nc\n', 'a\nB\nc\n', '/f.txt');
    assert.ok(diff.includes('-b'));
    assert.ok(diff.includes('+B'));
    assert.ok(diff.includes(' a'));
    assert.ok(diff.includes(' c'));
    assert.equal(hunkCount(diff), 1);
  });

  it('returns the no-changes sentinel for identical content', () => {
    assert.equal(buildUnifiedDiff('x\ny\n', 'x\ny\n', '/f.txt'), '(no changes detected)');
  });
});

// ---------------------------------------------------------------------------
// buildUnifiedDiff — options: contextLines / singleHunk / ignoreWhitespace / maxBytes
// ---------------------------------------------------------------------------

describe('buildUnifiedDiff — options', () => {
  const base = Array.from({ length: 30 }, (_, i) => `line${i}`).join('\n');

  it('contextLines widens the emitted context window', () => {
    const lines = base.split('\n');
    lines[15] = 'X';
    const narrow = buildUnifiedDiff(base, lines.join('\n'), '/f.txt', { contextLines: 1 });
    const wide = buildUnifiedDiff(base, lines.join('\n'), '/f.txt', { contextLines: 5 });
    const ctxNarrow = narrow.split('\n').filter(l => l.startsWith(' ')).length;
    const ctxWide = wide.split('\n').filter(l => l.startsWith(' ')).length;
    assert.ok(ctxWide > ctxNarrow, `${ctxWide} !> ${ctxNarrow}`);
    assert.equal(ctxNarrow, 2); // 1 line above + 1 below
  });

  it('singleHunk folds two far-apart changes into one hunk', () => {
    const lines = base.split('\n');
    lines[2] = 'TOP';
    lines[25] = 'BOTTOM';
    const diff = buildUnifiedDiff(base, lines.join('\n'), '/f.txt', { singleHunk: true });
    assert.equal(hunkCount(diff), 1, diff);
    assert.ok(DIFF_SINGLE_HUNK_CONTEXT >= 100000);
  });

  it('ignoreWhitespace treats a whitespace-only change as no change', () => {
    const diff = buildUnifiedDiff('foo( a, b )\n', 'foo(  a,   b )\n', '/f.txt', { ignoreWhitespace: true });
    assert.equal(diff, '(no changes detected)', diff);
    // Without the option, the same inputs DO diff.
    const strict = buildUnifiedDiff('foo( a, b )\n', 'foo(  a,   b )\n', '/f.txt');
    assert.notEqual(strict, '(no changes detected)');
  });

  it('maxBytes truncates at a line boundary with a [N lines truncated] marker', () => {
    const big = Array.from({ length: 400 }, (_, i) => `old${i}`).join('\n');
    const bigNew = Array.from({ length: 400 }, (_, i) => `new${i}`).join('\n');
    const diff = buildUnifiedDiff(big, bigNew, '/f.txt', { maxBytes: 512 });
    assert.ok(Buffer.byteLength(diff, 'utf8') <= 512 + 64, `len ${diff.length}`);
    assert.match(diff, /\.\.\. \[\d+ lines truncated\] \.\.\./);
  });

  it('does not add a truncation marker when under the cap', () => {
    const diff = buildUnifiedDiff('a\nb\n', 'a\nB\n', '/f.txt', { maxBytes: 9999 });
    assert.ok(!diff.includes('lines truncated'), diff);
  });
});

// ---------------------------------------------------------------------------
// buildUnifiedDiff — degenerate-input guard (timeout-equivalent fallback)
// ---------------------------------------------------------------------------

describe('buildUnifiedDiff — degenerate-input guard', () => {
  it('still produces a bounded diff promptly for very large distinct inputs', () => {
    const n = 2500; // 2500 * 2500 = 6.25M cells > MAX_DIFF_CELLS (4M) → fast fallback
    const a = Array.from({ length: n }, (_, i) => `a${i}`).join('\n');
    const b = Array.from({ length: n }, (_, i) => `b${i}`).join('\n');
    const start = Date.now();
    const diff = buildUnifiedDiff(a, b, '/big.txt');
    const elapsed = Date.now() - start;
    assert.ok(elapsed < 2000, `fallback took ${elapsed}ms`);
    assert.ok(diff.startsWith('--- /big.txt'), diff.slice(0, 40));
    assert.ok(diff.includes('@@ '));
  });
});

// ---------------------------------------------------------------------------
// countLinesChanged
// ---------------------------------------------------------------------------

describe('countLinesChanged', () => {
  it('counts a modified line as one addition + one removal', () => {
    assert.deepEqual(countLinesChanged('a\nb\nc\n', 'a\nB\nc\n'), { additions: 1, removals: 1 });
  });

  it('counts a pure insertion as additions only', () => {
    assert.deepEqual(countLinesChanged('a\nc\n', 'a\nb\nc\n'), { additions: 1, removals: 0 });
  });

  it('counts a pure deletion as removals only', () => {
    assert.deepEqual(countLinesChanged('a\nb\nc\n', 'a\nc\n'), { additions: 0, removals: 1 });
  });

  it('treats an empty old file as all-added (new file)', () => {
    assert.deepEqual(countLinesChanged('', 'x\ny\nz'), { additions: 3, removals: 0 });
  });

  it('reports zero for empty→empty', () => {
    assert.deepEqual(countLinesChanged('', ''), { additions: 0, removals: 0 });
  });

  it('respects ignoreWhitespace', () => {
    assert.deepEqual(
      countLinesChanged('a( x )\n', 'a(  x  )\n', { ignoreWhitespace: true }),
      { additions: 0, removals: 0 },
    );
  });
});

// ---------------------------------------------------------------------------
// fileEditTool — multi-edit substring guard
// ---------------------------------------------------------------------------

describe('fileEditTool – multi-edit substring guard', () => {
  it('rejects an edit whose old_string is a substring of a prior edit new_string', async () => {
    const file = path.join(tmpDir, 'g.txt');
    fs.writeFileSync(file, 'alpha\nkeep\n');
    const res = await fileEditTool.execute(
      {
        file_path: file,
        edits: [
          { old_string: 'alpha', new_string: 'INSERTED_TOKEN' },
          // old_string "TOKEN" is contained in the prior new_string → re-editing
          // just-written text; must be rejected, not silently applied.
          { old_string: 'TOKEN', new_string: 'oops' },
        ],
      },
      ctx(),
    );
    // Edit 1 applies, edit 2 is rejected as a substring-of-prior-new_string.
    assert.equal(res.isError, false, res.output);
    assert.match(res.output, /substring of a new_string from a previous edit/);
    const written = fs.readFileSync(file, 'utf-8');
    assert.ok(written.includes('INSERTED_TOKEN'), written);
    assert.ok(!written.includes('oops'), written);
  });

  it('allows independent sequential edits that do not overlap prior inserts', async () => {
    const file = path.join(tmpDir, 'ok.txt');
    fs.writeFileSync(file, 'one\ntwo\n');
    const res = await fileEditTool.execute(
      {
        file_path: file,
        edits: [
          { old_string: 'one', new_string: 'ONE' },
          { old_string: 'two', new_string: 'TWO' },
        ],
      },
      ctx(),
    );
    assert.equal(res.isError, false, res.output);
    assert.ok(!res.output.includes('substring'), res.output);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'ONE\nTWO\n');
  });
});

// ---------------------------------------------------------------------------
// fileEditTool — multi-edit success now renders a unified diff + change counts
// ---------------------------------------------------------------------------

describe('fileEditTool – multi-edit diff + counts', () => {
  it('appends a unified diff and +A -B counts to the multi-edit success output', async () => {
    const file = path.join(tmpDir, 'd.txt');
    fs.writeFileSync(file, 'one\ntwo\nthree\n');
    const res = await fileEditTool.execute(
      {
        file_path: file,
        edits: [
          { old_string: 'one', new_string: 'ONE' },
          { old_string: 'three', new_string: 'THREE' },
        ],
      },
      ctx(),
    );
    assert.equal(res.isError, false, res.output);
    assert.match(res.output, /\(\+2 -2\)/, res.output); // two modified lines
    assert.ok(res.output.includes('@@ '), res.output);
    assert.ok(res.output.includes('-one'));
    assert.ok(res.output.includes('+ONE'));
  });
});

// ---------------------------------------------------------------------------
// fileEditTool — single-replace patch info reports +additions/-removals
// ---------------------------------------------------------------------------

describe('fileEditTool – single-replace change counts', () => {
  it('reports +additions -removals in the [edit:] patch info line', async () => {
    const file = path.join(tmpDir, 's.txt');
    fs.writeFileSync(file, 'hello world\n');
    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'hello world', new_string: 'goodbye world' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    // The patch-info tag now also carries the 1-based edit location (`:1` — the
    // first changed line) alongside the +additions/-removals counts.
    assert.match(res.output, /\[edit: s\.txt:1, \+1 -1\]/, res.output);
  });
});
