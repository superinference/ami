import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  SessionMemoryExtractor,
  sanitizeMemoryEntry,
  redactSecrets,
  DEFAULT_SESSION_MEMORY_CONFIG,
  type SessionFact,
} from '../src/session-memory';
import { streamChatCompletionWithRetry } from '../src/provider';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

const provider: ProviderConfig = { model: 'test', apiKey: 'x', baseUrl: 'http://localhost' };

function factGen(chunks: StreamChunk[]): SessionMemoryExtractor['_streamFn'] {
  return (async function* () {
    for (const c of chunks) yield c;
  }) as unknown as SessionMemoryExtractor['_streamFn'];
}

// ---------------------------------------------------------------------------
// Gap: threshold/cadence gate before extraction (shouldExtractFacts)
// ---------------------------------------------------------------------------
describe('SessionMemoryExtractor.shouldExtractFacts — dual threshold + natural break', () => {
  const ex = new SessionMemoryExtractor(provider);

  it('is false when neither threshold is reached and no natural break', () => {
    assert.equal(ex.shouldExtractFacts({ tokensSinceLast: 10, toolCallsSinceLast: 1 }), false);
  });

  it('is true once the token threshold is crossed', () => {
    assert.equal(
      ex.shouldExtractFacts({ tokensSinceLast: DEFAULT_SESSION_MEMORY_CONFIG.extractTokenThreshold, toolCallsSinceLast: 0 }),
      true,
    );
  });

  it('is true once the tool-call threshold is crossed', () => {
    assert.equal(
      ex.shouldExtractFacts({ tokensSinceLast: 0, toolCallsSinceLast: DEFAULT_SESSION_MEMORY_CONFIG.extractToolCallThreshold }),
      true,
    );
  });

  it('natural break extracts any pending work below the thresholds', () => {
    assert.equal(ex.shouldExtractFacts({ tokensSinceLast: 5, toolCallsSinceLast: 0, naturalBreak: true }), true);
  });

  it('natural break with nothing pending does not extract', () => {
    assert.equal(ex.shouldExtractFacts({ tokensSinceLast: 0, toolCallsSinceLast: 0, naturalBreak: true }), false);
  });

  it('respects a custom tool-call threshold from config', () => {
    const tuned = new SessionMemoryExtractor(provider, undefined, { extractToolCallThreshold: 2 });
    assert.equal(tuned.shouldExtractFacts({ tokensSinceLast: 0, toolCallsSinceLast: 2 }), true);
    assert.equal(ex.shouldExtractFacts({ tokensSinceLast: 0, toolCallsSinceLast: 2 }), false);
  });
});

// ---------------------------------------------------------------------------
// Gap: secret/credential redaction before persisting / sending memory content
// ---------------------------------------------------------------------------
describe('redactSecrets + sanitizeMemoryEntry — credential scrubbing', () => {
  it('redactSecrets masks common credential shapes', () => {
    const out = redactSecrets('key sk-ant-abcd1234EFGH5678 and AKIAIOSFODNN7EXAMPLE token');
    assert.ok(!out.includes('sk-ant-abcd1234EFGH5678'));
    assert.ok(!out.includes('AKIAIOSFODNN7EXAMPLE'));
    assert.ok(out.includes('[REDACTED]'));
  });

  it('returns credential-free text unchanged', () => {
    assert.equal(redactSecrets('plain fact about tabs'), 'plain fact about tabs');
  });

  it('sanitizeMemoryEntry redacts a secret embedded in a fact', () => {
    const cleaned = sanitizeMemoryEntry('The API key is sk-ant-SECRETsecret1234tokenvalue please remember');
    assert.ok(!cleaned.includes('sk-ant-SECRETsecret1234tokenvalue'));
    assert.ok(cleaned.includes('[REDACTED]'));
  });

  it('redacts secrets in tool output before sending them to the extraction model', async () => {
    const ex = new SessionMemoryExtractor(provider);
    let sentUserContent = '';
    ex._streamFn = (async function* (_cfg: ProviderConfig, msgs: Message[]) {
      sentUserContent = String(msgs[1]?.content ?? '');
      yield { type: 'content_delta', text: '[]' } as StreamChunk;
    }) as unknown as SessionMemoryExtractor['_streamFn'];

    await ex.extractFacts(
      [{ toolName: 'bash', output: 'export AWS_SECRET_KEY=supersecretvalue1234', isError: false }],
      [{ role: 'user', content: 'Authorization: Bearer abcdef0123456789token' }],
    );

    assert.ok(sentUserContent.length > 0);
    assert.ok(!sentUserContent.includes('supersecretvalue1234'), 'tool-result secret must be redacted');
    assert.ok(!sentUserContent.includes('abcdef0123456789token'), 'message bearer token must be redacted');
    assert.ok(sentUserContent.includes('[REDACTED]'));
  });
});

// ---------------------------------------------------------------------------
// Gap: caller-supplied abort signal threaded into extraction
// ---------------------------------------------------------------------------
describe('SessionMemoryExtractor.extractFacts — caller abort signal', () => {
  it('an external abort cancels the extraction stream before the watchdog', async () => {
    const ex = new SessionMemoryExtractor(provider);
    ex._timeoutMs = 10_000; // watchdog long enough that ONLY the external signal can abort
    const ac = new AbortController();
    let sawAbort: boolean | undefined;
    ex._streamFn = (async function* (_cfg: ProviderConfig, _m: Message[], _t: unknown, sig: AbortSignal) {
      ac.abort(); // user interrupt
      await new Promise(r => setTimeout(r, 5));
      sawAbort = sig.aborted;
      yield { type: 'content_delta', text: '[]' } as StreamChunk;
    }) as unknown as SessionMemoryExtractor['_streamFn'];

    await ex.extractFacts([{ toolName: 'bash', output: 'ok', isError: false }], [{ role: 'user', content: 'hi' }], ac.signal);
    assert.equal(sawAbort, true);
  });

  it('a pre-aborted signal aborts the stream immediately', async () => {
    const ex = new SessionMemoryExtractor(provider);
    ex._timeoutMs = 10_000;
    const ac = new AbortController();
    ac.abort();
    let abortedAtStart: boolean | undefined;
    ex._streamFn = (async function* (_cfg: ProviderConfig, _m: Message[], _t: unknown, sig: AbortSignal) {
      abortedAtStart = sig.aborted;
      yield { type: 'content_delta', text: '[]' } as StreamChunk;
    }) as unknown as SessionMemoryExtractor['_streamFn'];

    await ex.extractFacts([{ toolName: 'bash', output: 'ok', isError: false }], [{ role: 'user', content: 'hi' }], ac.signal);
    assert.equal(abortedAtStart, true);
  });
});

// ---------------------------------------------------------------------------
// Gap: transient-error retry + model fallback for background LLM calls
// ---------------------------------------------------------------------------
describe('SessionMemoryExtractor — default stream uses the retry wrapper', () => {
  it('_streamFn defaults to streamChatCompletionWithRetry', () => {
    const ex = new SessionMemoryExtractor(provider);
    assert.equal(ex._streamFn, streamChatCompletionWithRetry);
  });
});

// ---------------------------------------------------------------------------
// Gap: semantic consolidation / near-duplicate merging of retained facts
// ---------------------------------------------------------------------------
describe('SessionMemoryExtractor.mergeFacts — near-duplicate collapse', () => {
  it('collapses facts that differ only in punctuation (not lowercase-equal)', () => {
    const ex = new SessionMemoryExtractor(provider);
    const existing: SessionFact[] = [{ fact: 'Use PostgreSQL.', category: 'decision', confidence: 0.8 }];
    const incoming: SessionFact[] = [{ fact: 'Use PostgreSQL', category: 'decision', confidence: 0.9 }];
    const merged = ex.mergeFacts(existing, incoming);
    assert.equal(merged.length, 1);
    assert.equal(merged[0].confidence, 0.9); // higher-confidence variant wins
  });

  it('collapses a paraphrase above the similarity threshold', () => {
    const ex = new SessionMemoryExtractor(provider);
    const existing: SessionFact[] = [
      { fact: 'chose to use postgres for storage', category: 'decision', confidence: 0.8 },
    ];
    const incoming: SessionFact[] = [
      { fact: 'chose to use postgres for the storage', category: 'decision', confidence: 0.75 },
    ];
    const merged = ex.mergeFacts(existing, incoming);
    assert.equal(merged.length, 1);
    assert.equal(merged[0].confidence, 0.8); // existing kept (incoming lower)
  });

  it('keeps genuinely distinct facts separate', () => {
    const ex = new SessionMemoryExtractor(provider);
    const merged = ex.mergeFacts(
      [{ fact: 'chose postgres', category: 'decision', confidence: 0.8 }],
      [{ fact: 'chose redis', category: 'decision', confidence: 0.8 }],
    );
    assert.equal(merged.length, 2);
  });
});

// ---------------------------------------------------------------------------
// Gap: runtime-configurable thresholds (config schema + override API)
// ---------------------------------------------------------------------------
describe('SessionMemoryConfig — runtime-overridable knobs', () => {
  it('exposes documented defaults', () => {
    assert.equal(DEFAULT_SESSION_MEMORY_CONFIG.maxEntries, 100);
    assert.equal(DEFAULT_SESSION_MEMORY_CONFIG.confidenceFloor, 0.7);
    assert.equal(DEFAULT_SESSION_MEMORY_CONFIG.extractionTimeoutMs, 30_000);
    assert.equal(DEFAULT_SESSION_MEMORY_CONFIG.contextWindowMessages, 4);
  });

  it('a constructor override lowers the entry cap', () => {
    const ex = new SessionMemoryExtractor(provider, undefined, { maxEntries: 2, recencyProtectedCount: 1 });
    const facts: SessionFact[] = Array.from({ length: 5 }, (_, i) => ({
      fact: `distinct fact number ${i}`,
      category: 'decision' as const,
      confidence: 0.7 + i / 100,
    }));
    const merged = ex.mergeFacts([], facts);
    assert.equal(merged.length, 2);
  });

  it('a constructor override seeds the extraction timeout', () => {
    const ex = new SessionMemoryExtractor(provider, undefined, { extractionTimeoutMs: 1234 });
    assert.equal(ex._timeoutMs, 1234);
    assert.equal(new SessionMemoryExtractor(provider)._timeoutMs, 30_000);
  });

  it('a custom confidence floor filters extracted facts', async () => {
    const ex = new SessionMemoryExtractor(provider, undefined, { confidenceFloor: 0.9 });
    ex._streamFn = factGen([
      { type: 'content_delta', text: JSON.stringify([{ fact: 'mid confidence fact', category: 'decision', confidence: 0.8 }]) } as StreamChunk,
    ]);
    const facts = await ex.extractFacts([{ toolName: 'bash', output: 'ok', isError: false }], [{ role: 'user', content: 'hi' }]);
    assert.deepEqual(facts, []); // 0.8 < custom floor 0.9

    const ex2 = new SessionMemoryExtractor(provider); // default floor 0.7
    ex2._streamFn = factGen([
      { type: 'content_delta', text: JSON.stringify([{ fact: 'mid confidence fact', category: 'decision', confidence: 0.8 }]) } as StreamChunk,
    ]);
    const facts2 = await ex2.extractFacts([{ toolName: 'bash', output: 'ok', isError: false }], [{ role: 'user', content: 'hi' }]);
    assert.equal(facts2.length, 1);
  });

  it('a custom context window changes how many messages are summarized', async () => {
    const ex = new SessionMemoryExtractor(provider, undefined, { contextWindowMessages: 1 });
    let sent = '';
    ex._streamFn = (async function* (_cfg: ProviderConfig, msgs: Message[]) {
      sent = String(msgs[1]?.content ?? '');
      yield { type: 'content_delta', text: '[]' } as StreamChunk;
    }) as unknown as SessionMemoryExtractor['_streamFn'];
    await ex.extractFacts(
      [{ toolName: 'bash', output: 'ok', isError: false }],
      [
        { role: 'user', content: 'FIRST-MESSAGE-MARKER' },
        { role: 'assistant', content: 'SECOND-MESSAGE-MARKER' },
      ],
    );
    assert.ok(sent.includes('SECOND-MESSAGE-MARKER'));
    assert.ok(!sent.includes('FIRST-MESSAGE-MARKER')); // only the last 1 message used
  });
});

// ---------------------------------------------------------------------------
// Gap: eviction that preserves recency, not confidence-only truncation
// ---------------------------------------------------------------------------
describe('SessionMemoryExtractor.mergeFacts — recency-protected eviction', () => {
  it('protects the newest facts from eviction even when their confidence is lower', () => {
    const ex = new SessionMemoryExtractor(provider);
    const existing: SessionFact[] = Array.from({ length: 100 }, (_, i) => ({
      fact: `established fact ${i}`,
      category: 'decision' as const,
      confidence: 0.95,
    }));
    const fresh: SessionFact[] = [{ fact: 'current work in progress', category: 'decision', confidence: 0.71 }];
    const merged = ex.mergeFacts(existing, fresh);
    assert.equal(merged.length, 100);
    // Confidence-only truncation would drop the 0.71 newcomer; recency protection keeps it.
    assert.ok(merged.some(f => f.fact === 'current work in progress'));
  });
});
