import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  detectRepetition,
  ToolCallRepetitionTracker,
  toolCallSignature,
} from '../src/utils/repetition';

// ---------------------------------------------------------------------------
// FIX 1: configurable thresholds via RepetitionOptions (additive).
// A lower `maxConsecutive` catches shorter line runs that the default (10)
// deliberately ignores; the default must still return false for the same text.
// RED on revert: the options arg is ignored → the custom-threshold call returns
// false → the `true` assertion fails.
// ---------------------------------------------------------------------------

describe('detectRepetition – configurable thresholds (parity)', () => {
  function textWithRun(runLength: number): string {
    const lines: string[] = [];
    for (let i = 0; i < 12; i++) lines.push(`unique padding alpha ${i} beta gamma ${i * 7}`);
    const rep = 'REPEATED HIGH ENTROPY LINE ALPHA';
    for (let i = 0; i < runLength; i++) lines.push(rep);
    return lines.join('\n');
  }

  it('default thresholds do NOT flag a 4-line run', () => {
    const text = textWithRun(4);
    const result = detectRepetition(text);
    assert.equal(result.isRepetitive, false);
  });

  it('a custom lower maxConsecutive flags the same 4-line run', () => {
    const text = textWithRun(4);
    const result = detectRepetition(text, { maxConsecutive: 3 });
    assert.equal(result.isRepetitive, true,
      'maxConsecutive option must lower the consecutive-line threshold');
    assert.ok(result.truncateAt > 0);
  });
});

// ---------------------------------------------------------------------------
// FIX 2: whitespace normalization before line comparison (additive, opt-in).
// Lines differing only by trailing whitespace are a real agent output loop that
// exact comparison misses. With strategy-2 disabled (minPhraseRepeats huge),
// only normalizeWhitespace can make the run collapse into a detected loop.
// RED on revert: options ignored → default strategy-2 sees the identical
// high-entropy content and flags BOTH calls true → the `false` assertion fails.
// ---------------------------------------------------------------------------

describe('detectRepetition – whitespace normalization (parity)', () => {
  function textWithTrailingWsRun(): string {
    const lines: string[] = [];
    for (let i = 0; i < 14; i++) {
      // Same content, different trailing whitespace each line.
      lines.push('duplicate alpha bravo charlie line' + ' '.repeat(i % 3));
    }
    return lines.join('\n');
  }

  it('exact comparison does NOT flag trailing-whitespace-only variation', () => {
    const text = textWithTrailingWsRun();
    const result = detectRepetition(text, { minPhraseRepeats: 9999 });
    assert.equal(result.isRepetitive, false,
      'exact line comparison treats differing trailing whitespace as distinct');
  });

  it('normalizeWhitespace collapses trailing-whitespace variation into a loop', () => {
    const text = textWithTrailingWsRun();
    const result = detectRepetition(text, { minPhraseRepeats: 9999, normalizeWhitespace: true });
    assert.equal(result.isRepetitive, true,
      'normalizeWhitespace must trim trailing whitespace before comparing lines');
  });
});

// ---------------------------------------------------------------------------
// FIX 3: tunable low-entropy skip (maxEntropyForSkip) (additive).
// A repeating 3-distinct-char pattern is flagged by default (entropy 3 > 2) but
// can be suppressed by raising the skip ceiling to 3.
// RED on revert: option ignored → both calls use default → both true → the
// `false` assertion fails.
// ---------------------------------------------------------------------------

describe('detectRepetition – tunable low-entropy skip (parity)', () => {
  const text = 'xyz'.repeat(100); // 300 chars, 3 distinct chars, one line

  it('default flags a repeating 3-distinct-char pattern', () => {
    assert.equal(detectRepetition(text).isRepetitive, true);
  });

  it('raising maxEntropyForSkip to 3 suppresses it', () => {
    assert.equal(detectRepetition(text, { maxEntropyForSkip: 3 }).isRepetitive, false,
      'maxEntropyForSkip must make 3-distinct-char phrases count as decoration');
  });
});

// ---------------------------------------------------------------------------
// FIX 4: ToolCallRepetitionTracker — anti-infinite-loop guardrail for repeated
// identical tool calls (hash-based, reset-on-progress). Entirely new symbol.
// RED on revert: the export does not exist → construction throws → every test
// here fails.
// ---------------------------------------------------------------------------

describe('ToolCallRepetitionTracker – repeated identical tool calls (parity)', () => {
  it('flags once the consecutive identical count reaches the threshold', () => {
    const t = new ToolCallRepetitionTracker(); // default threshold 3
    const r1 = t.record('bash', { command: 'ls' });
    const r2 = t.record('bash', { command: 'ls' });
    const r3 = t.record('bash', { command: 'ls' });
    assert.equal(r1.isLooping, false);
    assert.equal(r1.count, 1);
    assert.equal(r2.isLooping, false);
    assert.equal(r3.isLooping, true);
    assert.equal(r3.count, 3);
    assert.equal(r3.threshold, 3);
  });

  it('resets the consecutive count when a different call interleaves (progress)', () => {
    const t = new ToolCallRepetitionTracker({ threshold: 3 });
    t.record('bash', { command: 'ls' });
    t.record('bash', { command: 'ls' });
    const diff = t.record('bash', { command: 'pwd' }); // progress — resets
    assert.equal(diff.count, 1);
    assert.equal(diff.isLooping, false);
    const again = t.record('bash', { command: 'ls' });
    assert.equal(again.count, 1, 'count must reset after an intervening different call');
    assert.equal(again.isLooping, false);
  });

  it('treats structurally-equal but key-reordered inputs as identical', () => {
    const t = new ToolCallRepetitionTracker({ threshold: 2 });
    const r1 = t.record('file_edit', { path: '/a', content: 'x' });
    const r2 = t.record('file_edit', { content: 'x', path: '/a' }); // reordered keys
    assert.equal(r1.signature, r2.signature, 'key order must not change the signature');
    assert.equal(r2.isLooping, true);
    assert.equal(r2.count, 2);
  });

  it('distinguishes different tools and different inputs', () => {
    const t = new ToolCallRepetitionTracker({ threshold: 2 });
    t.record('bash', { command: 'ls' });
    const other = t.record('grep', { command: 'ls' }); // different tool name
    assert.equal(other.count, 1);
    assert.equal(other.isLooping, false);
  });

  it('window mode counts non-consecutive repeats within the window', () => {
    const t = new ToolCallRepetitionTracker({ threshold: 3, windowSize: 5 });
    t.record('bash', { c: 'a' });
    t.record('bash', { c: 'b' });
    t.record('bash', { c: 'a' });
    t.record('bash', { c: 'b' });
    const r = t.record('bash', { c: 'a' }); // 'a' appears 3x within last 5
    assert.equal(r.count, 3);
    assert.equal(r.isLooping, true);
  });

  it('window mode evicts entries beyond the window size', () => {
    const t = new ToolCallRepetitionTracker({ threshold: 3, windowSize: 2 });
    t.record('bash', { c: 'a' });
    t.record('bash', { c: 'a' });
    const r = t.record('bash', { c: 'a' }); // window holds only last 2 → count 2
    assert.equal(r.count, 2);
    assert.equal(r.isLooping, false);
  });

  it('reset() clears all tracking state', () => {
    const t = new ToolCallRepetitionTracker({ threshold: 2 });
    t.record('bash', { command: 'ls' });
    assert.equal(t.consecutiveCount, 1);
    t.reset();
    assert.equal(t.consecutiveCount, 0);
    const r = t.record('bash', { command: 'ls' });
    assert.equal(r.count, 1, 'after reset the first call starts a fresh run');
    assert.equal(r.isLooping, false);
  });

  it('clamps invalid threshold/window options to safe values', () => {
    const t = new ToolCallRepetitionTracker({ threshold: 0, windowSize: -5 });
    const r = t.record('bash', { command: 'ls' });
    assert.equal(r.threshold, 1, 'threshold < 1 is clamped to 1');
    assert.equal(r.isLooping, true, 'with threshold 1 the first call already loops');
  });

  it('handles null / undefined / primitive inputs without throwing', () => {
    const t = new ToolCallRepetitionTracker({ threshold: 2 });
    assert.doesNotThrow(() => {
      t.record('bash', null);
      t.record('bash', undefined);
      t.record('bash', 42);
      t.record('bash', 'str');
      t.record('bash', [1, 2, 3]);
    });
  });
});

describe('toolCallSignature (parity)', () => {
  it('is deterministic and order-insensitive for object keys', () => {
    const a = toolCallSignature('bash', { x: 1, y: 2 });
    const b = toolCallSignature('bash', { y: 2, x: 1 });
    assert.equal(a, b);
  });

  it('preserves array order (semantically significant)', () => {
    const a = toolCallSignature('bash', { args: [1, 2] });
    const b = toolCallSignature('bash', { args: [2, 1] });
    assert.notEqual(a, b);
  });

  it('differs by tool name', () => {
    assert.notEqual(
      toolCallSignature('bash', { command: 'ls' }),
      toolCallSignature('grep', { command: 'ls' }),
    );
  });
});

// ---------------------------------------------------------------------------
// Backward-compatibility: calling detectRepetition with no options behaves
// exactly as before (guards against accidental default changes).
// ---------------------------------------------------------------------------

describe('detectRepetition – backward compatibility (parity)', () => {
  it('no-options call still flags 10 consecutive identical lines', () => {
    const lines: string[] = [];
    for (let i = 0; i < 5; i++) lines.push(`unique prefix line ${i} with padding content here`);
    for (let i = 0; i < 12; i++) lines.push('REPEATED LINE CONTENT THAT IS LONG ENOUGH');
    const result = detectRepetition(lines.join('\n'));
    assert.equal(result.isRepetitive, true);
  });

  it('no-options call still returns false for short text', () => {
    assert.deepEqual(detectRepetition('short'), { isRepetitive: false, truncateAt: -1 });
  });
});
