import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  compactionFailureMessage,
  parseMockRateLimit,
  MOCK_RATE_LIMIT_ENV,
  DEFAULT_MOCK_RATE_LIMIT_RETRY_AFTER_MS,
  makeAbortError,
} from '../src/error-classifier';
import {
  streamChatCompletionWithRetry,
} from '../src/provider';
import type { StreamChunk, Message, ToolDefinition, ProviderConfig } from '../src/types';

// ---------------------------------------------------------------------------
// U073 / w82 — actionable auto-compaction failure wording.
// Gap: when background (auto) compaction fails or is interrupted, AMI gave the
// user no guidance (the manual /compact path had an error line; the auto path was
// opaque). compactionFailureMessage() classifies the failure and returns copy that
// says what happened + how to recover. Parity: the reference's ERROR_MESSAGE_* compaction
// strings (services/compact/compact.ts).
// ---------------------------------------------------------------------------
describe('compactionFailureMessage', () => {
  it('gives an interruption/retry hint for an aborted compaction', () => {
    const msg = compactionFailureMessage(makeAbortError());
    assert.match(msg, /interrupted/i);
    assert.match(msg, /\/compact/);
  });

  it('gives a network-retry hint for a network failure', () => {
    const msg = compactionFailureMessage(new Error('fetch failed: ECONNRESET'));
    assert.match(msg, /network/i);
    assert.match(msg, /try again/i);
    assert.match(msg, /\/compact/);
  });

  it('gives a manual-trim hint when the conversation still overflows', () => {
    const msg = compactionFailureMessage(new Error('API Error: prompt is too long: 215000 tokens > 200000 maximum'));
    assert.match(msg, /could not shrink/i);
    assert.match(msg, /\/compact/);
    assert.match(msg, /\/clear/);
  });

  it('gives a rate-limit hint for a 429 during compaction', () => {
    const msg = compactionFailureMessage(new Error('API Error: HTTP 429 rate_limit_error'));
    assert.match(msg, /rate limited/i);
    assert.match(msg, /\/compact/);
  });

  it('gives a generic recovery hint for an unknown failure', () => {
    const msg = compactionFailureMessage(new Error('something unexpected happened'));
    assert.match(msg, /compaction failed/i);
    assert.match(msg, /\/compact/);
    assert.match(msg, /\/clear/);
  });

  it('accepts a raw string error too (not just Error objects)', () => {
    const msg = compactionFailureMessage('plain string failure');
    assert.ok(msg.length > 0);
    assert.match(msg, /\/compact/);
  });
});

// ---------------------------------------------------------------------------
// U073 / w82 — AMI_MOCK_RATE_LIMIT knob parsing.
// Gap: AMI had robust rate-limit handling but no knob to SIMULATE a 429, so the
// rate-limit UX could only be exercised against a real provider throttle. Parity:
// Claude's shouldProcessMockLimits()/`/mock-limits`.
// ---------------------------------------------------------------------------
describe('parseMockRateLimit', () => {
  it('returns null when unset', () => {
    assert.equal(parseMockRateLimit(undefined), null);
    assert.equal(parseMockRateLimit(''), null);
  });

  it('returns null for explicit-off tokens', () => {
    for (const off of ['0', 'false', 'off', 'no', 'FALSE', ' Off ']) {
      assert.equal(parseMockRateLimit(off), null, `expected ${off} → null`);
    }
  });

  it('returns null for a non-numeric, non-boolean token (fail-safe)', () => {
    assert.equal(parseMockRateLimit('banana'), null);
    assert.equal(parseMockRateLimit('yesish'), null);
  });

  it('returns null for a zero / negative count', () => {
    assert.equal(parseMockRateLimit('0:500'), null);
    assert.equal(parseMockRateLimit('-3'), null);
  });

  it('treats truthy booleans as one synthetic 429 with the default delay', () => {
    for (const on of ['true', 'on', 'yes', '1', 'TRUE']) {
      assert.deepEqual(parseMockRateLimit(on), {
        count: 1,
        retryAfterMs: DEFAULT_MOCK_RATE_LIMIT_RETRY_AFTER_MS,
        status: 429,
      }, `expected ${on} → one default 429`);
    }
  });

  it('parses a bare positive integer count', () => {
    assert.deepEqual(parseMockRateLimit('3'), {
      count: 3,
      retryAfterMs: DEFAULT_MOCK_RATE_LIMIT_RETRY_AFTER_MS,
      status: 429,
    });
  });

  it('parses the N:MS form (count + retry-after ms)', () => {
    assert.deepEqual(parseMockRateLimit('2:500'), { count: 2, retryAfterMs: 500, status: 429 });
    assert.deepEqual(parseMockRateLimit('1:0'), { count: 1, retryAfterMs: 0, status: 429 });
  });

  it('falls back to the default delay when the ms part is malformed', () => {
    assert.deepEqual(parseMockRateLimit('2:abc'), {
      count: 2,
      retryAfterMs: DEFAULT_MOCK_RATE_LIMIT_RETRY_AFTER_MS,
      status: 429,
    });
  });
});

// ---------------------------------------------------------------------------
// U073 / w82 — E2E: the knob drives the REAL retry/backoff machinery.
// With AMI_MOCK_RATE_LIMIT set, streamChatCompletionWithRetry must inject the
// configured number of synthetic 429s (classified + backed off + onRetry fired)
// before the real stream runs, exercising the whole rate-limit path in-process.
// ---------------------------------------------------------------------------
describe('streamChatCompletionWithRetry + AMI_MOCK_RATE_LIMIT (E2E)', () => {
  const prevEnv = process.env[MOCK_RATE_LIMIT_ENV];
  afterEach(() => {
    if (prevEnv === undefined) delete process.env[MOCK_RATE_LIMIT_ENV];
    else process.env[MOCK_RATE_LIMIT_ENV] = prevEnv;
  });

  const config = { provider: 'openai', model: 'gpt-x', apiKey: 'k', baseUrl: 'http://x' } as unknown as ProviderConfig;
  const messages: Message[] = [{ role: 'user', content: 'hi' }] as unknown as Message[];
  const tools: ToolDefinition[] = [];

  async function* okStream(): AsyncGenerator<StreamChunk> {
    yield { type: 'content_delta', text: 'ok' };
    yield { type: 'done', finishReason: 'stop' };
  }

  async function drain(knob: string, maxRetries = 3): Promise<{ chunks: StreamChunk[]; retries: number[] }> {
    process.env[MOCK_RATE_LIMIT_ENV] = knob;
    const retries: number[] = [];
    const chunks: StreamChunk[] = [];
    const gen = streamChatCompletionWithRetry(config, messages, tools, new AbortController().signal, {
      streamImpl: okStream as never,
      maxRetries,
      baseDelayMs: 0,
      onRetry: (_a, _m, _d, status) => { retries.push(status ?? -1); },
    });
    for await (const c of gen) chunks.push(c);
    return { chunks, retries };
  }

  it('injects one synthetic 429 before the real stream succeeds', async () => {
    const { chunks, retries } = await drain('1:0');
    // The real stream's content eventually comes through.
    assert.ok(chunks.some(c => c.type === 'content_delta' && c.text === 'ok'), 'real content delivered');
    assert.ok(chunks.some(c => c.type === 'done'), 'stream completed');
    // Exactly one retry, classified as a 429 rate-limit.
    assert.deepEqual(retries, [429]);
    // The synthetic error was NOT surfaced to the caller (it was retried away).
    assert.ok(!chunks.some(c => c.type === 'error'), 'no error chunk leaked');
  });

  it('injects N synthetic 429s for a count knob', async () => {
    const { retries } = await drain('2:0');
    assert.deepEqual(retries, [429, 429]);
  });

  it('is a no-op when the knob is unset (real stream on first attempt)', async () => {
    delete process.env[MOCK_RATE_LIMIT_ENV];
    const retries: number[] = [];
    const chunks: StreamChunk[] = [];
    const gen = streamChatCompletionWithRetry(config, messages, tools, new AbortController().signal, {
      streamImpl: okStream as never,
      maxRetries: 3,
      baseDelayMs: 0,
      onRetry: (_a, _m, _d, status) => { retries.push(status ?? -1); },
    });
    for await (const c of gen) chunks.push(c);
    assert.deepEqual(retries, [], 'no synthetic retries when unset');
    assert.ok(chunks.some(c => c.type === 'content_delta'));
  });
});
