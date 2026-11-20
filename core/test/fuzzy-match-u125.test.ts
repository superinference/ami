// Parity unit U125 — fuzzy-match.ts quote-style preservation + post-apply no-op
// identity check (vs Claude Code's FileEditTool/utils.ts).
//
// Two behavior gaps are closed here, each as a RED-GREEN reproducer:
//
//   1. preserveQuoteStyle previously chose open vs close curly quotes by an
//      even/odd PARITY counter. the reference instead walks each character and
//      decides from the PREVIOUS character's CONTEXT (whitespace / start / an
//      opening bracket / em-or-en dash), and special-cases a contraction
//      apostrophe (a `'` flanked by two letters -> always a RIGHT single curly
//      quote). The parity counter mis-flips every quote after a skipped or
//      contraction apostrophe and mis-opens a quote after a bracket/dash.
//      RED: revert preserveQuoteStyle to the parity logic -> these assertions
//      flip to the wrong glyphs.
//
//   2. A located edit whose replacement is byte-identical to the matched region
//      (normalization/reindent/quote-restyling erased the change) was reported
//      as a SUCCESSFUL edit. Claude Code's post-apply identity check rejects it.
//      RED: remove the `newContent === content` check in fuzzyFindAndReplace ->
//      the no-op is reported as a success instead of NOOP_EDIT_ERROR.
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { fuzzyFindAndReplace, NOOP_EDIT_ERROR } from '../src/tools/fuzzy-match';
import { applyEditsToContent } from '../src/tools/file-edit-equivalence';
import { fileEditTool } from '../src/tools/file-edit';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// 1a. Contraction-aware single quotes + opening-context after a bracket.
// ---------------------------------------------------------------------------
describe('preserveQuoteStyle — contraction + opening-context single quotes', () => {
  it('keeps a contraction apostrophe as a right curly quote (not an opener)', () => {
    const r = fuzzyFindAndReplace('note: ‘x’', '‘x’', "don't go");
    assert.equal(r.error, null);
    // Parity counter would have produced "don‘t go" (left curly on the first ').
    assert.equal(r.newContent, 'note: don’t go');
  });

  it('opens a quote after "(" and treats a contraction between the quotes correctly', () => {
    // "it's ('v')" exercises: a contraction apostrophe, an opener preceded by '(',
    // and a closer preceded by a letter — all three diverge from parity counting.
    const r = fuzzyFindAndReplace('q: ‘x’', '‘x’', "it's ('v')");
    assert.equal(r.error, null);
    assert.equal(r.newContent, 'q: it’s (‘v’)');
  });

  it('treats an en-dash as an opening context for single quotes', () => {
    const r = fuzzyFindAndReplace('m ‘n’', '‘n’', "c'–'d'");
    assert.equal(r.error, null);
    // parity would give "c‘–’d‘"; context gives closer, en-dash opener, closer.
    assert.equal(r.newContent, 'm c’–‘d’');
  });
});

// ---------------------------------------------------------------------------
// 1b. Opening-context double quotes (bracket + em-dash contexts).
// ---------------------------------------------------------------------------
describe('preserveQuoteStyle — context-based double quotes', () => {
  it('chooses open/close from the previous character, not parity', () => {
    // 'end" ("x")' : closer after 'd', opener after '(', closer after 'x'.
    const r = fuzzyFindAndReplace('say “y”', '“y”', 'end" ("x")');
    assert.equal(r.error, null);
    assert.equal(r.newContent, 'say end” (“x”)');
  });

  it('treats an em-dash as an opening context for double quotes', () => {
    const r = fuzzyFindAndReplace('w “k”', '“k”', 'z" —"q"');
    assert.equal(r.error, null);
    assert.equal(r.newContent, 'w z” —“q”');
  });

  it('leaves a replacement that already uses curly double quotes untouched', () => {
    const r = fuzzyFindAndReplace('x “a”', '“a”', '“kept" more');
    assert.equal(r.error, null);
    assert.equal(r.newContent, 'x “kept" more');
  });
});

// ---------------------------------------------------------------------------
// 2. Post-apply no-op identity check.
// ---------------------------------------------------------------------------
describe('fuzzyFindAndReplace — post-apply identity check rejects no-op edits', () => {
  it('rejects an edit whose quote-restyled replacement equals the region', () => {
    // '‘b’' -> "'b'" is restyled back to '‘b’', so the file does not change.
    const r = fuzzyFindAndReplace('a‘b’c', '‘b’', "'b'");
    assert.equal(r.newContent, null);
    assert.equal(r.matchCount, 1);
    assert.equal(r.error, NOOP_EDIT_ERROR);
  });

  it('rejects a normalization-equivalent no-op (replacement reproduces the region)', () => {
    // 'a b' matches 'a  b' only via whitespace_normalized; the replacement 'a  b'
    // reproduces the matched region byte-for-byte, so the file does not change
    // even though old_string !== new_string.
    const r = fuzzyFindAndReplace('a  b\n', 'a b', 'a  b');
    assert.equal(r.newContent, null);
    assert.equal(r.error, NOOP_EDIT_ERROR);
  });

  it('still succeeds when the restyle produces a genuinely different result', () => {
    const r = fuzzyFindAndReplace('note: ‘x’', '‘x’', "don't go");
    assert.equal(r.error, null);
    assert.notEqual(r.newContent, 'note: ‘x’');
  });
});

// ---------------------------------------------------------------------------
// Consumer wiring: the equivalence engine treats a no-op as content-unchanged.
// ---------------------------------------------------------------------------
describe('applyEditsToContent — a no-op edit carries content forward unchanged', () => {
  it('does not throw and leaves the content identical', () => {
    const out = applyEditsToContent('a‘b’c', [{ old_string: '‘b’', new_string: "'b'" }]);
    assert.equal(out, 'a‘b’c');
  });
});

// ---------------------------------------------------------------------------
// E2E through the real file_edit tool.
// ---------------------------------------------------------------------------
describe('file_edit tool — U125 behaviors end to end', () => {
  let tmpDir: string;
  const ctx = (): ToolContext => ({ cwd: tmpDir, abortSignal: new AbortController().signal });

  beforeEach(() => {
    tmpDir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'si-fe-u125-')));
  });
  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('rejects a no-op edit with a clear "produces no change" error', async () => {
    const file = path.join(tmpDir, 'quote.txt');
    fs.writeFileSync(file, 'a‘b’c\n');
    const res = await fileEditTool.execute(
      { file_path: file, old_string: '‘b’', new_string: "'b'" },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /produces no change/);
    // The file is left exactly as it was.
    assert.equal(fs.readFileSync(file, 'utf-8'), 'a‘b’c\n');
  });

  it('writes contraction-correct typography when preserving curly quotes', async () => {
    const file = path.join(tmpDir, 'note.txt');
    fs.writeFileSync(file, 'note: ‘x’\n');
    const res = await fileEditTool.execute(
      { file_path: file, old_string: '‘x’', new_string: "don't go" },
      ctx(),
    );
    assert.equal(res.isError, undefined);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'note: don’t go\n');
  });
});
