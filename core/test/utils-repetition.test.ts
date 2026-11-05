import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { detectRepetition } from '../src/utils/repetition';

// Helper: pad text to exceed the 200-char minimum using unique lines
let _padCounter = 0;
function padLines(prefix: string[], repeated: string, count: number, suffix: string[] = []): string {
  const lines = [...prefix];
  for (let i = 0; i < count; i++) lines.push(repeated);
  lines.push(...suffix);
  while (lines.join('\n').length < 250) {
    lines.unshift(`unique padding line number ${++_padCounter} for test`);
  }
  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// No repetition
// ---------------------------------------------------------------------------

describe('detectRepetition – no repetition', () => {
  it('returns false for short text (< 200 chars)', () => {
    const text = 'line1\nline2\nline3';
    const result = detectRepetition(text);
    assert.equal(result.isRepetitive, false);
    assert.equal(result.truncateAt, -1);
  });

  it('returns false for text with no consecutive duplicates', () => {
    const lines: string[] = [];
    for (let i = 0; i < 20; i++) {
      lines.push(`unique line ${i}: content varies here with value ${i * 31 + 7}`);
    }
    const result = detectRepetition(lines.join('\n'));
    assert.equal(result.isRepetitive, false);
    assert.equal(result.truncateAt, -1);
  });

  it('returns false for text with a few consecutive duplicates (< 10)', () => {
    const text = padLines(
      ['line 0', 'line 1', 'line 2', 'line 3', 'line 4'],
      'repeated line content here',
      8, // 8 < 10
      ['other 0', 'other 1', 'other 2', 'other 3', 'other 4'],
    );
    const result = detectRepetition(text);
    assert.equal(result.isRepetitive, false);
  });

  it('returns false for consecutive empty lines', () => {
    const lines: string[] = [];
    for (let i = 0; i < 10; i++) lines.push(`line ${i}: unique text block number ${i * 13}`);
    for (let i = 0; i < 15; i++) lines.push('');
    for (let i = 0; i < 10; i++) lines.push(`other ${i}: different text block number ${i * 17}`);
    const result = detectRepetition(lines.join('\n'));
    assert.equal(result.isRepetitive, false);
  });

  it('returns false for consecutive whitespace-only lines', () => {
    const lines: string[] = [];
    for (let i = 0; i < 10; i++) lines.push(`line ${i}: unique text block number ${i * 19}`);
    for (let i = 0; i < 15; i++) lines.push('   ');
    for (let i = 0; i < 10; i++) lines.push(`other ${i}: different text block number ${i * 23}`);
    const result = detectRepetition(lines.join('\n'));
    assert.equal(result.isRepetitive, false);
  });
});

// ---------------------------------------------------------------------------
// Repetition detected — line-level
// ---------------------------------------------------------------------------

describe('detectRepetition – line-level repetition', () => {
  it('detects 10+ consecutive duplicate lines', () => {
    const text = padLines(
      ['line 0', 'line 1', 'line 2', 'line 3', 'line 4'],
      'REPEATED LINE CONTENT THAT IS LONG ENOUGH',
      12,
    );
    const result = detectRepetition(text);
    assert.equal(result.isRepetitive, true);
    assert.ok(result.truncateAt > 0);
  });

  it('detects exactly 10 consecutive duplicate lines', () => {
    const text = padLines(
      ['line 0', 'line 1', 'line 2', 'line 3', 'line 4'],
      'REPEATED LINE CONTENT THAT IS LONG ENOUGH',
      10,
    );
    const result = detectRepetition(text);
    assert.equal(result.isRepetitive, true);
  });

  it('truncateAt points to a valid position in the text', () => {
    const text = padLines(
      ['prefix line zero', 'prefix line one', 'prefix line two', 'prefix line three', 'prefix line four'],
      'REP content that repeats many times in sequence',
      15,
    );
    const result = detectRepetition(text);
    assert.equal(result.isRepetitive, true);
    assert.ok(result.truncateAt > 0);
    assert.ok(result.truncateAt <= text.length);
  });

  it('handles repetition at the start of text (after padding)', () => {
    const text = padLines([], 'SAME LINE REPEATED OVER AND OVER AGAIN AND AGAIN', 12);
    const result = detectRepetition(text);
    assert.equal(result.isRepetitive, true);
  });

  it('detects repetition after different lines', () => {
    const text = padLines(
      ['a unique line', 'b unique line', 'c unique line', 'd unique line', 'e unique line'],
      'xxx repeated content that goes on and on and on',
      15,
      ['f unique line', 'g unique line'],
    );
    const result = detectRepetition(text);
    assert.equal(result.isRepetitive, true);
  });
});

// ---------------------------------------------------------------------------
// Repetition detected — phrase-level (new strategy)
// ---------------------------------------------------------------------------

describe('detectRepetition – phrase-level repetition', () => {
  it('detects repeated phrases on the same line', () => {
    const phrase = 'I have made a mistake. ';
    const text = 'Some valid intro text that sets the stage. ' + phrase.repeat(20);
    const result = detectRepetition(text);
    assert.equal(result.isRepetitive, true);
    assert.ok(result.truncateAt > 0);
  });

  it('detects repeated long phrases', () => {
    const phrase = 'This is a repeated sentence that keeps going. ';
    const text = 'Introduction paragraph. ' + phrase.repeat(10);
    const result = detectRepetition(text);
    assert.equal(result.isRepetitive, true);
  });

  it('does not false-positive on similar but different phrases', () => {
    const lines: string[] = [];
    for (let i = 0; i < 20; i++) {
      lines.push(`Line ${i}: some content that varies each time with number ${i * 17}`);
    }
    const result = detectRepetition(lines.join('\n'));
    assert.equal(result.isRepetitive, false);
  });
});

// ---------------------------------------------------------------------------
// False positive guards
// ---------------------------------------------------------------------------

describe('detectRepetition – false positive guards', () => {
  it('does not false-positive on box-drawing border lines (─────)', () => {
    const border = '─'.repeat(49);
    const text = [
      'Some valid introduction text that is long enough to pass the 200-char minimum.',
      '',
      `★ Insight ${border}`,
      '- Point one about the implementation',
      '- Point two about the trade-offs',
      border,
      '',
      'More text after the insight callout that continues the explanation.',
      '',
      `★ Insight ${border}`,
      '- Another insight about a different topic',
      '- With multiple teaching points here',
      border,
    ].join('\n');
    const result = detectRepetition(text);
    assert.equal(result.isRepetitive, false,
      'box-drawing borders must not trigger phrase repetition (low-entropy skip)');
  });

  it('does not false-positive on horizontal rules (═════)', () => {
    const rule = '═'.repeat(50);
    const lines = [`First section of content with enough text. ${rule}`];
    for (let i = 0; i < 6; i++) lines.push(`Section ${i}: unique content number ${i * 31}. ${rule}`);
    const result = detectRepetition(lines.join('\n'));
    assert.equal(result.isRepetitive, false,
      'repeated horizontal rules must not trigger phrase repetition');
  });

  it('does not false-positive on tool output footer borders', () => {
    const footer = '─'.repeat(60);
    const text = [
      'Some long intro text that is unique and not repetitive at all.',
      '',
      '▶ Ran bash(ls -la)',
      '  file1.txt  file2.txt  file3.txt',
      footer,
      '',
      '▶ Ran bash(cat file1.txt)',
      '  contents of file1',
      footer,
      '',
      '▶ Ran bash(cat file2.txt)',
      '  contents of file2',
      footer,
      '',
      '▶ Ran bash(cat file3.txt)',
      '  contents of file3',
      footer,
      '',
      '▶ Ran bash(wc -l *.txt)',
      '  4 total',
      footer,
    ].join('\n');
    const result = detectRepetition(text);
    assert.equal(result.isRepetitive, false,
      'tool output footer borders (─) must not trigger phrase repetition');
  });

  it('still detects genuinely repetitive high-entropy phrases', () => {
    const phrase = 'The quick brown fox jumps. ';
    const text = 'Some valid intro text that sets the stage. ' + phrase.repeat(20);
    const result = detectRepetition(text);
    assert.equal(result.isRepetitive, true,
      'high-entropy phrases repeated many times must still be detected');
    assert.ok(result.truncateAt > 0);
  });
});

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

describe('detectRepetition – edge cases', () => {
  it('handles empty text', () => {
    const result = detectRepetition('');
    assert.equal(result.isRepetitive, false);
    assert.equal(result.truncateAt, -1);
  });

  it('handles single line', () => {
    const result = detectRepetition('single line');
    assert.equal(result.isRepetitive, false);
    assert.equal(result.truncateAt, -1);
  });

  it('handles text just under 200 chars with repetition', () => {
    // Under the 200-char minimum — should not detect
    const text = 'x'.repeat(199);
    const result = detectRepetition(text);
    assert.equal(result.isRepetitive, false);
  });

  it('handles text at exactly 200 chars of single char (low-entropy skip)', () => {
    const text = 'a'.repeat(200);
    // 200 chars of 'a' — low-entropy phrase (Set size ≤ 2) is skipped by Strategy 2
    // so this is NOT detected as repetitive (it's a decorative/padding pattern)
    const result = detectRepetition(text);
    assert.equal(result.isRepetitive, false);
  });

  it('resets count when different line breaks the sequence', () => {
    const lines: string[] = [];
    for (let i = 0; i < 5; i++) lines.push(`first block unique line ${i * 29}`);
    for (let i = 0; i < 5; i++) lines.push('AAA repeated content line');
    lines.push('BBB completely different line breaks it here');
    for (let i = 0; i < 5; i++) lines.push('CCC another repeated line');
    for (let i = 0; i < 5; i++) lines.push(`last block unique line ${i * 37}`);
    const result = detectRepetition(lines.join('\n'));
    // Neither AAA nor CCC reaches 10 consecutive
    assert.equal(result.isRepetitive, false);
  });
});
