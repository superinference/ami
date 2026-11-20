/**
 * U027 parity: position → symbol extraction (the reference's getSymbolAtPosition,
 * tools/LSPTool/symbolContext.ts) plus its wiring into the diagnostic-detail
 * renderer so a diagnostic line shows a readable `symbol: foo` label instead of
 * only a bare `line:char`.
 *
 * Covers:
 *  - identifier / operator / lifetime extraction at a 0-indexed (line, char);
 *  - out-of-range line, out-of-range character, empty line, whitespace gap → null;
 *  - the 30-char length cap (ellipsis-truncated);
 *  - the 64KB bounded read + mid-window-truncation guard (filled window, last
 *    split line → null);
 *  - ENOENT / unreadable file → null (graceful fallback);
 *  - formatDiagnosticsDetail annotates with the resolved symbol, omits it when
 *    the file is missing/unknown, and the location always survives.
 */
import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { pathToFileURL } from 'url';

import {
  getSymbolAtPosition,
  MAX_READ_BYTES,
  MAX_SYMBOL_LENGTH,
} from '../src/lsp/symbol-context';
import { formatDiagnosticsDetail, type DiagnosticFile } from '../src/lsp/diagnostic-tracking';

let dir: string;
let sample: string;
let bigOneLine: string;

before(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-symctx-'));
  sample = path.join(dir, 'sample.ts');
  // Line 0: const fooBar = 1 + baz;   Line 1: (empty)   Line 2:   indented
  fs.writeFileSync(sample, 'const fooBar = 1 + baz;\n\n  spaced;\n');
  bigOneLine = path.join(dir, 'big.txt');
  // One giant line with no newline, larger than the 64KB read window.
  fs.writeFileSync(bigOneLine, 'x'.repeat(MAX_READ_BYTES + 5000));
});

after(() => {
  if (dir) fs.rmSync(dir, { recursive: true, force: true });
});

describe('getSymbolAtPosition', () => {
  it('extracts the identifier under the cursor (0-indexed)', () => {
    // "const fooBar = 1 + baz;" — 'fooBar' starts at column 6.
    assert.equal(getSymbolAtPosition(sample, 0, 6), 'fooBar');
    assert.equal(getSymbolAtPosition(sample, 0, 9), 'fooBar');
    // 'baz' starts at column 19.
    assert.equal(getSymbolAtPosition(sample, 0, 19), 'baz');
  });

  it('extracts an operator run', () => {
    // '+' sits at column 17 ("… 1 + baz").
    assert.equal(getSymbolAtPosition(sample, 0, 17), '+');
  });

  it('returns null for an out-of-range line', () => {
    assert.equal(getSymbolAtPosition(sample, 99, 0), null);
    assert.equal(getSymbolAtPosition(sample, -1, 0), null);
  });

  it('returns null for an out-of-range or negative character', () => {
    assert.equal(getSymbolAtPosition(sample, 0, 999), null);
    assert.equal(getSymbolAtPosition(sample, 0, -1), null);
  });

  it('returns null for an empty line', () => {
    assert.equal(getSymbolAtPosition(sample, 1, 0), null);
  });

  it('returns null when the position lands on whitespace (no token)', () => {
    // Line 2 is "  spaced;" — column 0 is a leading space.
    assert.equal(getSymbolAtPosition(sample, 2, 0), null);
    // column 2 is the 's' of 'spaced'.
    assert.equal(getSymbolAtPosition(sample, 2, 2), 'spaced');
  });

  it('truncates an over-long symbol to the length cap with an ellipsis', () => {
    const longName = 'a'.repeat(40);
    const f = path.join(dir, 'long.ts');
    fs.writeFileSync(f, `const ${longName} = 1;\n`);
    const sym = getSymbolAtPosition(f, 0, 6)!;
    assert.equal(sym.length, MAX_SYMBOL_LENGTH);
    assert.ok(sym.endsWith('…'));
    assert.ok(sym.startsWith('aaaa'));
  });

  it('returns null at the last split line when the 64KB window is filled (mid-line truncation guard)', () => {
    // The whole window is one un-terminated line, so lines.length === 1 and line 0
    // is the last (possibly truncated) split element — must refuse to extract.
    assert.equal(getSymbolAtPosition(bigOneLine, 0, 10), null);
  });

  it('returns null for a missing file (ENOENT) rather than throwing', () => {
    assert.equal(getSymbolAtPosition(path.join(dir, 'nope.ts'), 0, 0), null);
  });
});

describe('formatDiagnosticsDetail: symbol annotation wiring', () => {
  function fileEntry(p: string, line: number, character: number): DiagnosticFile {
    return {
      uri: pathToFileURL(p).href,
      diagnostics: [
        {
          range: { start: { line, character }, end: { line, character: character + 1 } },
          severity: 'Error',
          message: 'boom',
        },
      ],
    };
  }

  it('shows "symbol: <name>" for a resolvable position', () => {
    const out = formatDiagnosticsDetail([fileEntry(sample, 0, 6)]);
    assert.match(out, /\[Line 1:7\] symbol: fooBar boom/);
  });

  it('omits the symbol label but keeps the location when the symbol cannot be resolved', () => {
    // Column 0 of line 1 (empty line) has no token.
    const out = formatDiagnosticsDetail([fileEntry(sample, 1, 0)]);
    assert.match(out, /\[Line 2:1\] boom/);
    assert.doesNotMatch(out, /symbol:/);
  });

  it('omits the symbol label for a missing file (no throw)', () => {
    const out = formatDiagnosticsDetail([fileEntry(path.join(dir, 'ghost.ts'), 0, 0)]);
    assert.match(out, /\[Line 1:1\] boom/);
    assert.doesNotMatch(out, /symbol:/);
  });

  it('omits the symbol label for an unknown (empty-uri) location', () => {
    const out = formatDiagnosticsDetail([
      { uri: '', diagnostics: [{ range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } }, severity: 'Warning', message: 'x' }] },
    ]);
    assert.match(out, /<unknown location>/);
    assert.doesNotMatch(out, /symbol:/);
  });
});
