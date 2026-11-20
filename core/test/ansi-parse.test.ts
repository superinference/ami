/**
 * Unit tests for core/core/src/utils/ansi-parse — the defensive inbound ANSI /
 * SGR parser. Covers parity with the reference's parseAnsi/get256Color plus the
 * graceful-degradation token model (unknown-sequence fallback, bell, reset) and
 * the malformed/partial/out-of-range edge cases that must never throw.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  parseAnsi,
  tokenizeAnsi,
  stripAnsi,
  get256Color,
  ANSI_COLORS,
  DEFAULT_FG,
  DEFAULT_BG,
  type AnsiColor,
} from '../src/utils/ansi-parse';

describe('ansi-parse: parseAnsi (styled spans)', () => {
  it('splits lines and strips carriage returns', () => {
    const lines = parseAnsi('ab\r\ncd');
    assert.equal(lines.length, 2);
    assert.equal(lines[0]!.map((s) => s.text).join(''), 'ab');
    assert.equal(lines[1]!.map((s) => s.text).join(''), 'cd');
  });

  it('parses standard SGR foreground + reset', () => {
    const spans = parseAnsi('\x1b[31mred\x1b[0mplain')[0]!;
    assert.deepEqual(spans.find((s) => s.text === 'red')!.color, { r: 205, g: 49, b: 49 });
    assert.deepEqual(spans.find((s) => s.text === 'plain')!.color, DEFAULT_FG);
  });

  it('treats empty SGR (ESC[m) as a reset', () => {
    const spans = parseAnsi('\x1b[31mred\x1b[mplain')[0]!;
    assert.deepEqual(spans.find((s) => s.text === 'plain')!.color, DEFAULT_FG);
  });

  it('parses bold on (1) and off (22)', () => {
    const spans = parseAnsi('\x1b[1mB\x1b[22mn')[0]!;
    assert.equal(spans.find((s) => s.text === 'B')!.bold, true);
    assert.equal(spans.find((s) => s.text === 'n')!.bold, false);
  });

  it('honors default-fg code 39', () => {
    const spans = parseAnsi('\x1b[31mr\x1b[39md')[0]!;
    assert.deepEqual(spans.find((s) => s.text === 'd')!.color, DEFAULT_FG);
  });

  it('parses bright, 256-color and truecolor foregrounds', () => {
    assert.deepEqual(parseAnsi('\x1b[91mx')[0]![0]!.color, { r: 241, g: 76, b: 76 });
    assert.deepEqual(parseAnsi('\x1b[38;5;196my')[0]![0]!.color, { r: 255, g: 0, b: 0 });
    assert.deepEqual(parseAnsi('\x1b[38;5;244mg')[0]![0]!.color, { r: 128, g: 128, b: 128 });
    assert.deepEqual(parseAnsi('\x1b[38;2;10;20;30mz')[0]![0]!.color, { r: 10, g: 20, b: 30 });
  });

  it('respects a custom defaultFg', () => {
    const fg: AnsiColor = { r: 1, g: 2, b: 3 };
    assert.deepEqual(parseAnsi('plain', fg)[0]![0]!.color, fg);
    // reset returns to the custom default, not the module default
    assert.deepEqual(parseAnsi('\x1b[31mr\x1b[0md', fg)[0]!.find((s) => s.text === 'd')!.color, fg);
  });

  it('drops non-SGR CSI and unknown escapes without leaking text', () => {
    assert.equal(parseAnsi('a\x1b[2Jb\x1bXc')[0]!.map((s) => s.text).join(''), 'abc');
  });

  it('preserves empty lines as empty span lists', () => {
    const lines = parseAnsi('a\n\nb');
    assert.equal(lines.length, 3);
    assert.deepEqual(lines[1], []);
  });

  it('interprets ESC c (reset) as clearing style mid-stream', () => {
    const spans = parseAnsi('\x1b[1mB\x1bcn')[0]!;
    assert.equal(spans.find((s) => s.text === 'B')!.bold, true);
    assert.equal(spans.find((s) => s.text === 'n')!.bold, false);
  });
});

describe('ansi-parse: defensive degradation (never throws / no NaN)', () => {
  it('tolerates a truncated 256-color sequence', () => {
    assert.doesNotThrow(() => parseAnsi('\x1b[38;5mx'));
    // index missing → code 38 ignored, color stays default
    assert.deepEqual(parseAnsi('\x1b[38;5mx')[0]![0]!.color, DEFAULT_FG);
  });

  it('tolerates a truncated truecolor sequence without producing NaN', () => {
    const span = parseAnsi('\x1b[38;2;10;20mx')[0]![0]!;
    assert.deepEqual(span.color, DEFAULT_FG);
    assert.ok(!Number.isNaN(span.color.r));
  });

  it('ignores non-numeric SGR params instead of throwing', () => {
    assert.doesNotThrow(() => parseAnsi('\x1b[zzmx'));
    assert.deepEqual(parseAnsi('\x1b[zzmx')[0]![0]!.color, DEFAULT_FG);
  });

  it('handles an unterminated CSI at end of input', () => {
    assert.doesNotThrow(() => parseAnsi('text\x1b[38;5;'));
    assert.equal(parseAnsi('text\x1b[38;5;')[0]!.map((s) => s.text).join(''), 'text');
  });

  it('handles a dangling ESC at end of input', () => {
    assert.doesNotThrow(() => parseAnsi('text\x1b'));
    assert.equal(parseAnsi('text\x1b')[0]!.map((s) => s.text).join(''), 'text');
  });

  it('handles empty input', () => {
    assert.deepEqual(parseAnsi(''), [[]]);
  });
});

describe('ansi-parse: tokenizeAnsi (semantic action model)', () => {
  it('splits text and sgr tokens', () => {
    assert.deepEqual(tokenizeAnsi('a\x1b[31mb'), [
      { type: 'text', text: 'a' },
      { type: 'sgr', params: '31' },
      { type: 'text', text: 'b' },
    ]);
  });

  it('emits a bell token for BEL', () => {
    assert.deepEqual(tokenizeAnsi('a\x07b'), [
      { type: 'text', text: 'a' },
      { type: 'bell' },
      { type: 'text', text: 'b' },
    ]);
  });

  it('emits a reset token for ESC c', () => {
    assert.deepEqual(tokenizeAnsi('\x1bcx'), [
      { type: 'reset' },
      { type: 'text', text: 'x' },
    ]);
  });

  it('preserves an unknown non-SGR CSI sequence verbatim', () => {
    assert.deepEqual(tokenizeAnsi('\x1b[2J'), [{ type: 'unknown', sequence: '\x1b[2J' }]);
  });

  it('preserves an unknown two-byte escape verbatim', () => {
    assert.deepEqual(tokenizeAnsi('\x1bX'), [{ type: 'unknown', sequence: '\x1bX' }]);
  });

  it('preserves an unterminated CSI verbatim', () => {
    assert.deepEqual(tokenizeAnsi('ab\x1b[38;5'), [
      { type: 'text', text: 'ab' },
      { type: 'unknown', sequence: '\x1b[38;5' },
    ]);
  });

  it('preserves a dangling ESC verbatim', () => {
    assert.deepEqual(tokenizeAnsi('a\x1b'), [
      { type: 'text', text: 'a' },
      { type: 'unknown', sequence: '\x1b' },
    ]);
  });
});

describe('ansi-parse: get256Color', () => {
  it('resolves the dim legacy 16-color cube (distinct from SGR theme)', () => {
    assert.deepEqual(get256Color(0), { r: 0, g: 0, b: 0 });
    assert.deepEqual(get256Color(1), { r: 128, g: 0, b: 0 }); // dim red, not SGR red
    assert.deepEqual(get256Color(15), { r: 255, g: 255, b: 255 });
  });

  it('resolves the 6x6x6 color cube', () => {
    assert.deepEqual(get256Color(16), { r: 0, g: 0, b: 0 });
    assert.deepEqual(get256Color(196), { r: 255, g: 0, b: 0 });
    assert.deepEqual(get256Color(231), { r: 255, g: 255, b: 255 });
  });

  it('resolves the grayscale ramp', () => {
    assert.deepEqual(get256Color(232), { r: 8, g: 8, b: 8 });
    assert.deepEqual(get256Color(255), { r: 238, g: 238, b: 238 });
  });

  it('falls back to DEFAULT_FG for out-of-range / non-integer indices', () => {
    assert.deepEqual(get256Color(-1), DEFAULT_FG);
    assert.deepEqual(get256Color(256), DEFAULT_FG);
    assert.deepEqual(get256Color(1.5), DEFAULT_FG);
    assert.deepEqual(get256Color(NaN), DEFAULT_FG);
  });
});

describe('ansi-parse: stripAnsi', () => {
  it('removes SGR/CSI sequences but keeps visible text and newlines', () => {
    assert.equal(stripAnsi('\x1b[31mred\x1b[0m\nplain'), 'red\nplain');
  });

  it('removes bell, reset, unknown and unterminated sequences', () => {
    // '\x1bc' is ESC c (reset) and consumes the following 'c' as the final byte.
    assert.equal(stripAnsi('a\x07b\x1bcc\x1b[2Jd\x1b[38;5'), 'abcd');
  });

  it('leaves plain text untouched', () => {
    assert.equal(stripAnsi('hello world'), 'hello world');
  });
});

describe('ansi-parse: palette exports', () => {
  it('exposes the 16-color SGR palette and defaults', () => {
    assert.equal(ANSI_COLORS.length, 16);
    assert.deepEqual(ANSI_COLORS[1], { r: 205, g: 49, b: 49 });
    assert.deepEqual(DEFAULT_FG, { r: 229, g: 229, b: 229 });
    assert.deepEqual(DEFAULT_BG, { r: 30, g: 30, b: 30 });
  });
});
