import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  generateTitle,
  selectTitleSource,
  isHumanAuthoredUserMessage,
  extractConversationText,
  capTitleInput,
  MAX_CONVERSATION_TEXT,
} from '../src/title-generator';
import { stripDisplayTagsAllowEmpty } from '../src/utils/display-tags';
import type { Message } from '../src/types';
import type { ProviderConfig } from '../src/types';
import type { streamChatCompletion } from '../src/provider';

// ---------------------------------------------------------------------------
// U198 (wave 69) — remaining title-generator parity gaps:
//   [30] empty / whitespace / pure-injected-tag input short-circuits BEFORE the
//        model call (sessionTitle.ts:83-84 + stripDisplayTagsAllowEmpty).
//   [8]/[29]/[32] recency-biased TAIL slice of the title input, capped at
//        MAX_CONVERSATION_TEXT=1000 (sessionTitle.ts:24,52-53) — both in
//        extractConversationText and in the generateTitle prompt input.
//   [26] stripDisplayTagsAllowEmpty discriminates command-only / pure-tag turns so
//        they are skipped as a title source (displayTags.ts:31-39).
//   [13] truncation affordance: ellipsis on an over-length generated title
//        (initReplBridge.ts:566-567).
// ---------------------------------------------------------------------------

const provider: ProviderConfig = {
  baseUrl: 'http://127.0.0.1:1/v1',
  apiKey: 'k',
  model: 'm',
  maxTokens: 50,
};

/** Seam emitting `text` as one content_delta. */
function seam(text: string): typeof streamChatCompletion {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (async function* () {
    yield { type: 'content_delta', text } as any;
    yield { type: 'done' } as any;
  }) as unknown as typeof streamChatCompletion;
}

/** Seam that records the messages it was handed, then emits `text`. The title
 *  request is split into a system (instruction) + user (conversation) message for
 *  prompt-injection isolation, so capture the JOINED content of every message. */
function capturingSeam(text: string, sink: { prompt?: string }): typeof streamChatCompletion {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (async function* (_p: any, messages: any[]) {
    sink.prompt = messages.map((m) => m?.content as string).join('\n');
    yield { type: 'content_delta', text } as any;
    yield { type: 'done' } as any;
  }) as unknown as typeof streamChatCompletion;
}

/** Seam that FAILS the test if the model stream is ever opened. */
function neverCalledSeam(): typeof streamChatCompletion {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (async function* () {
    assert.fail('streamChatCompletion must not be called for empty / pure-tag input');
    yield { type: 'done' } as any;
  }) as unknown as typeof streamChatCompletion;
}

// ---------------------------------------------------------------------------
// [30] empty / whitespace / pure-tag short-circuit (no model round-trip)
// ---------------------------------------------------------------------------
describe('generateTitle — empty/whitespace/pure-tag short-circuit (gap 30/26)', () => {
  it('returns "" for an empty string WITHOUT opening the model stream', async () => {
    assert.equal(await generateTitle('', provider, undefined, neverCalledSeam()), '');
  });
  it('returns "" for whitespace-only input WITHOUT opening the model stream', async () => {
    assert.equal(await generateTitle('   \n\t ', provider, undefined, neverCalledSeam()), '');
  });
  it('returns "" for a pure-injected-tag turn WITHOUT opening the model stream', async () => {
    const injected = '<session-start-hook>\ndoing setup\n</session-start-hook>\n';
    assert.equal(await generateTitle(injected, provider, undefined, neverCalledSeam()), '');
  });
  it('still titles a message that mixes real prose with injected tags', async () => {
    const mixed = '<ide_opened_file>a.ts</ide_opened_file>\nFix the login bug';
    const out = await generateTitle(mixed, provider, undefined, seam('{"title": "Fix login bug"}'));
    assert.equal(out, 'Fix login bug');
  });
});

// ---------------------------------------------------------------------------
// [26] stripDisplayTagsAllowEmpty + title-source discrimination
// ---------------------------------------------------------------------------
describe('stripDisplayTagsAllowEmpty (gap 26)', () => {
  it('returns "" when a message is entirely injected tags', () => {
    assert.equal(stripDisplayTagsAllowEmpty('<task-notification>ping</task-notification>\n'), '');
  });
  it('returns the surviving human prose when tags are mixed in', () => {
    assert.equal(
      stripDisplayTagsAllowEmpty('<ide_selection>x</ide_selection>\nadd a test'),
      'add a test',
    );
  });
  it('differs from stripDisplayTags, which returns the original on an all-tags input', () => {
    // stripDisplayTags preserves the original ("show something"); the allow-empty
    // variant must NOT — that is the whole point of the discriminator.
    const allTags = '<hook>out</hook>';
    assert.equal(stripDisplayTagsAllowEmpty(allTags), '');
  });
});

describe('isHumanAuthoredUserMessage / selectTitleSource skip pure-tag turns (gap 26/15)', () => {
  it('rejects a user message that is entirely injected wrapper tags', () => {
    assert.equal(
      isHumanAuthoredUserMessage({ role: 'user', content: '<session-start-hook>s</session-start-hook>' }),
      false,
    );
  });
  it('falls through to the most recent REAL human message as the title source', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'fix the parser' },
      { role: 'user', content: '<task-notification>done</task-notification>' },
    ];
    assert.equal(selectTitleSource(msgs), 'fix the parser');
  });
});

// ---------------------------------------------------------------------------
// [8]/[29]/[32] recency-biased tail slice
// ---------------------------------------------------------------------------
describe('capTitleInput — recency-biased tail slice (gap 8/29/32)', () => {
  it('exports MAX_CONVERSATION_TEXT = 1000', () => {
    assert.equal(MAX_CONVERSATION_TEXT, 1000);
  });
  it('returns input unchanged when within the cap', () => {
    assert.equal(capTitleInput('short'), 'short');
  });
  it('keeps the LAST 1000 chars (tail), not the head, when over the cap', () => {
    const text = 'HEAD'.padEnd(1200, '.') + 'THE-RECENT-ASK';
    const out = capTitleInput(text);
    assert.equal(out.length, MAX_CONVERSATION_TEXT);
    assert.ok(out.endsWith('THE-RECENT-ASK'));
    assert.ok(!out.startsWith('HEAD'));
  });
});

describe('extractConversationText tail-slices to recent context (gap 8/11/12/29/32)', () => {
  it('keeps the most recent turns when the conversation exceeds the cap', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'X'.repeat(1500) },
      { role: 'user', content: 'the latest concrete ask' },
    ];
    const out = extractConversationText(msgs);
    assert.equal(out.length, MAX_CONVERSATION_TEXT);
    assert.ok(out.endsWith('the latest concrete ask'));
  });
});

describe('generateTitle feeds the TAIL of a long input to the model (gap 32)', () => {
  it('sends the last 1000 chars (recent ask), not the opening preamble', async () => {
    const sink: { prompt?: string } = {};
    const long = 'OPENING-BOILERPLATE'.padEnd(1300, '.') + ' the real recent request';
    await generateTitle(long, provider, undefined, capturingSeam('{"title":"ok"}', sink));
    assert.ok(sink.prompt!.includes('the real recent request'));
    assert.ok(!sink.prompt!.includes('OPENING-BOILERPLATE'));
  });
});

// ---------------------------------------------------------------------------
// [13] truncation affordance through generateTitle
// ---------------------------------------------------------------------------
describe('generateTitle appends an ellipsis on an over-length generated title (gap 13)', () => {
  it('an over-length model title is cut on a boundary and ends with "…"', async () => {
    const long = 'Investigate and fix the issue where the login button does not respond on mobile';
    const out = await generateTitle('q', provider, undefined, seam(long));
    assert.ok(out.length <= 50);
    assert.ok(out.endsWith('…'), `expected trailing ellipsis, got: ${out}`);
  });
});
