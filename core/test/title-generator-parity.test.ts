import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { generateTitle, sanitizeTitle, truncateTitle } from '../src/title-generator';
import type { ProviderConfig } from '../src/types';
import type { streamChatCompletion } from '../src/provider';

// ---------------------------------------------------------------------------
// Parity / robustness hardening for title-generator.ts (ADDITIVE SUPERSET).
//
// Each block below targets one real gap in the pre-hardening implementation and
// is written to be RED on the old code and GREEN on the new:
//   1. ANSI/VT escape sequences survived -> corrupted the single-line bar.
//   2. C0/C1 control chars + DEL survived -> corrupted the terminal.
//   3. Invisible/zero-width + bidi-override Unicode survived -> spoof/noise.
//   4. Hard mid-word slice(0,50) truncation -> severed-word titles.
//   5. Only a single straight leading/trailing quote stripped -> smart/nested
//      quotes and `"<space>` wrappers leaked into the title.
//
// All invisible/control chars are written as \u escapes so the source stays
// unambiguous ASCII.
// ---------------------------------------------------------------------------

const provider: ProviderConfig = {
  baseUrl: 'http://127.0.0.1:1/v1',
  apiKey: 'k',
  model: 'm',
  maxTokens: 50,
};

/** Build a test-seam streamer that emits the given text as one content_delta. */
function seam(text: string): typeof streamChatCompletion {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (async function* () {
    yield { type: 'content_delta', text } as any;
    yield { type: 'done' } as any;
  }) as unknown as typeof streamChatCompletion;
}

describe('sanitizeTitle — gap 1: ANSI/VT escape sequences', () => {
  it('strips CSI colour/cursor sequences', () => {
    assert.equal(sanitizeTitle('\u001b[31mRed\u001b[0m Title'), 'Red Title');
  });
  it('strips a clear-screen CSI code that would blank the bar', () => {
    assert.equal(sanitizeTitle('Before\u001b[2JAfter'), 'BeforeAfter');
  });
  it('strips an OSC set-window-title sequence', () => {
    assert.equal(sanitizeTitle('\u001b]0;pwned\u0007Clean'), 'Clean');
  });
});

describe('sanitizeTitle — gap 2: C0/C1 control chars + DEL', () => {
  it('removes NUL and BEL but keeps the visible text', () => {
    assert.equal(sanitizeTitle('Ti\u0000tle\u0007here'), 'Titlehere');
  });
  it('removes DEL and C1 control chars', () => {
    assert.equal(sanitizeTitle('A\u007fB\u009fC'), 'ABC');
  });
  it('still collapses the kept \\t/\\n/\\r whitespace to single spaces', () => {
    assert.equal(sanitizeTitle('a\tb\nc\rd'), 'a b c d');
  });
});

describe('sanitizeTitle — gap 3: invisible / bidi Unicode', () => {
  it('removes zero-width space, joiner and BOM', () => {
    assert.equal(sanitizeTitle('a​b‍c﻿d'), 'abcd');
  });
  it('removes bidi override (Trojan-source RLO) and isolates', () => {
    assert.equal(sanitizeTitle('a‮b⁦c⁩d'), 'abcd');
  });
  it('collapses line/paragraph separators to a space (not deletion)', () => {
    assert.equal(sanitizeTitle('a b c'), 'a b c');
  });
});

describe('truncateTitle — gap 4: word-boundary truncation', () => {
  it('breaks on a word boundary instead of mid-word and appends an ellipsis', () => {
    const out = truncateTitle('the quick brown fox jumps over the lazy dog again now');
    assert.ok(out.length <= 50);
    // One budget char reserved for '…', so the word boundary lands earlier than a
    // bare 50-char cut; the ellipsis signals the title continued.
    assert.equal(out, 'the quick brown fox jumps over the lazy dog…');
  });
  it('hard-cuts a single long word with an ellipsis affordance', () => {
    // No usable boundary -> 49 chars + '…' == 50 total (never exceeds max).
    assert.equal(truncateTitle('X'.repeat(200)), 'X'.repeat(49) + '…');
  });
  it('leaves a short title untouched (no ellipsis)', () => {
    assert.equal(truncateTitle('short title'), 'short title');
  });
  it('respects a custom max and breaks on the boundary within it, ellipsis appended', () => {
    // max 12 -> budget 11 -> clip "alpha beta "; last space at 10 (>= 60% of 12) -> "alpha beta…".
    assert.equal(truncateTitle('alpha beta gamma', 12), 'alpha beta…');
  });
});

describe('generateTitle — gap 5: smart / nested surrounding quotes', () => {
  it('strips typographic (curly) surrounding quotes', async () => {
    const title = await generateTitle('x', provider, undefined, seam('“Smart Title”'));
    assert.equal(title, 'Smart Title');
  });
  it('strips guillemets and repeated/nested quotes', async () => {
    const title = await generateTitle('x', provider, undefined, seam('«\'Nested\'»'));
    assert.equal(title, 'Nested');
  });
  it('strips a straight quote even when a trailing space follows it', async () => {
    const title = await generateTitle('x', provider, undefined, seam('"Spaced" '));
    assert.equal(title, 'Spaced');
  });
});

describe('generateTitle — end-to-end hardening through the engine seam', () => {
  it('flattens ANSI + control + bidi + markdown from model output', async () => {
    const dirty = '\u001b[32m# ‮Title\u0007​ with\nnoise\u001b[0m';
    const title = await generateTitle('x', provider, undefined, seam(dirty));
    assert.ok(!/[\u0000-\u001f\u007f-\u009f]/.test(title), 'no control chars');
    assert.ok(!title.includes('\n'), 'single line');
    assert.ok(!title.startsWith('#'), 'heading stripped');
  });
  it('word-boundary-truncates an over-long model title', async () => {
    const title = await generateTitle('x', provider, undefined,
      seam('alpha beta gamma delta epsilon zeta eta theta iota kappa lambda'));
    assert.ok(title.length <= 50);
    assert.ok(!title.endsWith(' '));
  });
  it('falls back to the sanitised user message when the model yields nothing', async () => {
    const title = await generateTitle('fallback used here', provider, undefined, seam('   '));
    assert.equal(title, 'fallback used here');
  });
  it('word-boundary-truncates the fallback user message too', async () => {
    const msg = 'alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu';
    const title = await generateTitle(msg, provider, undefined, seam(''));
    assert.ok(title.length <= 50);
    assert.ok(!title.endsWith(' '));
    // Truncation affordance: the cut title ends with '…'; its body (sans ellipsis)
    // is still a clean word-boundary prefix of the original message.
    assert.ok(title.endsWith('…'));
    assert.ok(msg.startsWith(title.slice(0, -1)));
  });
});
