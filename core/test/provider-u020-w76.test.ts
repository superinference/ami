import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  streamChatCompletion,
  extractRedactedThinkingData,
  REDACTED_THINKING_PLACEHOLDER,
  isEmptyStreamError,
} from '../src/provider';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

// ---------------------------------------------------------------------------
// U020 / w76 — Redacted (encrypted) thinking placeholder.
//
// The `@ai-sdk/anthropic` provider maps a `redacted_thinking` content block to a
// `reasoning-start` fullStream part carrying
// `providerMetadata.anthropic.redactedData` and emits NO `reasoning-delta` text.
// Previously AMI's stream switch ignored `reasoning-start` entirely (default
// branch), so a redacted block surfaced nothing — the user saw a silent gap where
// the model actually reasoned. Parity with the reference's
// AssistantRedactedThinkingMessage ("✻ Thinking…"): emit a placeholder once per
// redacted block through the already-wired `thinking_delta` render path.
// ---------------------------------------------------------------------------

const CFG: ProviderConfig = {
  baseUrl: 'http://127.0.0.1:1',
  apiKey: 'test-key',
  model: 'claude-sonnet-4-6',
};
const MSG: Message[] = [{ role: 'user', content: 'hi' }];

async function collect(gen: AsyncGenerator<StreamChunk>): Promise<StreamChunk[]> {
  const out: StreamChunk[] = [];
  for await (const c of gen) out.push(c);
  return out;
}

// Fake AI-SDK streamText that yields the given fullStream parts.
function fakeStreamText(parts: Array<Record<string, unknown>>): unknown {
  return () => ({
    stream: (async function* () {
      for (const p of parts) yield p;
    })(),
  });
}

const redactedStart = (id: string, data = 'ENCRYPTED_BLOB') => ({
  type: 'reasoning-start',
  id,
  providerMetadata: { anthropic: { redactedData: data } },
});
const finishStep = {
  type: 'finish-step',
  finishReason: 'stop',
  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
};

describe('extractRedactedThinkingData', () => {
  it('returns the redacted blob for a redacted reasoning-start part', () => {
    assert.equal(extractRedactedThinkingData(redactedStart('0', 'abc')), 'abc');
  });

  it('returns undefined for a normal reasoning-start (no providerMetadata)', () => {
    assert.equal(extractRedactedThinkingData({ type: 'reasoning-start', id: '0' }), undefined);
  });

  it('returns undefined when the metadata lacks an anthropic.redactedData string', () => {
    assert.equal(extractRedactedThinkingData({ providerMetadata: { anthropic: {} } }), undefined);
    assert.equal(extractRedactedThinkingData({ providerMetadata: {} }), undefined);
    assert.equal(extractRedactedThinkingData({ providerMetadata: { anthropic: { redactedData: '' } } }), undefined);
    assert.equal(extractRedactedThinkingData({ providerMetadata: { anthropic: { redactedData: 42 } } }), undefined);
    assert.equal(extractRedactedThinkingData({ providerMetadata: 'nope' }), undefined);
  });

  it('returns undefined for non-object inputs', () => {
    assert.equal(extractRedactedThinkingData(undefined), undefined);
    assert.equal(extractRedactedThinkingData(null), undefined);
    assert.equal(extractRedactedThinkingData('x'), undefined);
  });
});

describe('streamChatCompletion - redacted thinking placeholder', () => {
  it('emits a placeholder thinking_delta for a redacted reasoning-start block', async () => {
    // RED (pre-fix): reasoning-start fell into the default (ignore) branch, so a
    // redacted block produced ZERO thinking_delta chunks.
    // GREEN (post-fix): one placeholder thinking_delta carrying the redacted marker.
    const chunks = await collect(
      streamChatCompletion(CFG, MSG, [], new AbortController().signal, {
        streamTextImpl: fakeStreamText([redactedStart('0'), finishStep]) as never,
      }),
    );
    const thinking = chunks.filter((c) => c.type === 'thinking_delta');
    assert.equal(thinking.length, 1);
    assert.ok(thinking[0].text!.includes(REDACTED_THINKING_PLACEHOLDER));
    assert.equal(chunks.some((c) => c.type === 'done'), true);
  });

  it('does NOT emit a placeholder for a normal (readable) reasoning block', async () => {
    // A normal thinking block: reasoning-start with no redactedData, then its text
    // via reasoning-delta. Only the real reasoning text must surface — no placeholder.
    const chunks = await collect(
      streamChatCompletion(CFG, MSG, [], new AbortController().signal, {
        streamTextImpl: fakeStreamText([
          { type: 'reasoning-start', id: '0' },
          { type: 'reasoning-delta', text: 'pondering' },
          finishStep,
        ]) as never,
      }),
    );
    const thinking = chunks.filter((c) => c.type === 'thinking_delta');
    assert.equal(thinking.length, 1);
    assert.equal(thinking[0].text, 'pondering');
    assert.equal(thinking.some((c) => c.text!.includes(REDACTED_THINKING_PLACEHOLDER)), false);
  });

  it('counts a redacted-only stream as output (no empty-stream error)', async () => {
    // A stream whose ONLY content is a redacted thinking block (no finish-step) must
    // NOT be flagged as a degenerate/empty stream — the model did produce output,
    // it was just redacted. Previously (ignored) this would trip EMPTY_STREAM_ERROR.
    const chunks = await collect(
      streamChatCompletion(CFG, MSG, [], new AbortController().signal, {
        streamTextImpl: fakeStreamText([redactedStart('0')]) as never,
      }),
    );
    assert.equal(chunks.some((c) => isEmptyStreamError(c.error)), false);
    assert.equal(chunks.filter((c) => c.type === 'thinking_delta').length, 1);
    assert.equal(chunks.at(-1)!.type, 'done');
  });

  it('emits one placeholder per redacted block when several arrive', async () => {
    const chunks = await collect(
      streamChatCompletion(CFG, MSG, [], new AbortController().signal, {
        streamTextImpl: fakeStreamText([redactedStart('0'), redactedStart('1'), finishStep]) as never,
      }),
    );
    assert.equal(chunks.filter((c) => c.type === 'thinking_delta').length, 2);
  });
});
