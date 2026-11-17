// U089 — column-aware tab expansion in core (parity: Claude Code ink/tabstops.ts
// `expandTabs`, DEFAULT_TAB_INTERVAL = 8). The canonical implementation lives in
// core/tools/input-sanitizer.ts and is reused by the CLI renderer and the
// permission-prompt command normalizer (No-Duplication Rule).

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  expandTabs,
  DEFAULT_TAB_INTERVAL,
  normalizeCommandForDisplay,
} from '../src/tools/input-sanitizer';

describe('expandTabs (column-aware)', () => {
  it('defaults to the POSIX 8-column interval', () => {
    assert.equal(DEFAULT_TAB_INTERVAL, 8);
  });

  it('advances a leading tab to the first 8-column stop', () => {
    // column 0 → a full interval of spaces (8 - (0 % 8) = 8).
    assert.equal(expandTabs('\tx'), '        x');
  });

  it('expands a tab to the NEXT stop, not a fixed run', () => {
    // 'a' is column 1 → 8 - (1 % 8) = 7 spaces (NOT 8, NOT a fixed 4/8).
    assert.equal(expandTabs('a\tb'), 'a       b');
    // 'abc' is column 3 → 8 - (3 % 8) = 5 spaces.
    assert.equal(expandTabs('abc\tcol'), 'abc     col');
  });

  it('on an exact stop boundary advances a full interval', () => {
    // '12345678' is column 8 → 8 - (8 % 8) = 8 spaces.
    assert.equal(expandTabs('12345678\tx'), '12345678        x');
  });

  it('resets the column counter at every newline', () => {
    assert.equal(expandTabs('a\tb\nabc\tcol'), 'a       b\nabc     col');
  });

  it('honours a custom interval', () => {
    assert.equal(expandTabs('a\tb', 4), 'a   b');
    assert.equal(expandTabs('a\tb', 2), 'a b');
  });

  it('expands multiple tabs on one line cumulatively', () => {
    // col0 tab → 8 spaces (col 8); 'xy' → col 10; tab → 8 - (10 % 8) = 6 spaces.
    assert.equal(expandTabs('\txy\tz'), '        xy      z');
  });

  it('fast-returns the original string unchanged when there is no tab', () => {
    const s = 'plain text with spaces';
    assert.equal(expandTabs(s), s);
  });

  it('is the engine behind normalizeCommandForDisplay (column-aware)', () => {
    // 'rm' is column 2 → 8 - (2 % 8) = 6 spaces.
    assert.equal(normalizeCommandForDisplay('rm\t-rf'), 'rm      -rf');
  });
});
