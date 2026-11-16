import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  SessionMemoryExtractor,
  normalizeRelativeDates,
  DEFAULT_SESSION_MEMORY_CONFIG,
  type SessionFact,
} from '../src/session-memory';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

const provider: ProviderConfig = { model: 'test', apiKey: 'x', baseUrl: 'http://localhost' };

/** A streamFn that yields the given chunks and records the messages it was called with. */
function recordingStreamFn(
  chunks: StreamChunk[],
  sink: { messages?: Message[] },
): SessionMemoryExtractor['_streamFn'] {
  return (async function* (_config: unknown, messages: Message[]) {
    sink.messages = messages;
    for (const c of chunks) yield c;
  }) as unknown as SessionMemoryExtractor['_streamFn'];
}

function contentChunk(text: string): StreamChunk {
  return { type: 'content_delta', text } as unknown as StreamChunk;
}

// ---------------------------------------------------------------------------
// Gap: relative-to-absolute date normalization in extracted facts
// ---------------------------------------------------------------------------
describe('normalizeRelativeDates', () => {
  const now = new Date(2026, 10, 16); // 2026-11-16 (local)

  it('rewrites today/yesterday/tomorrow to absolute YYYY-MM-DD', () => {
    assert.equal(normalizeRelativeDates('ship it today', now), 'ship it 2026-11-16');
    assert.equal(normalizeRelativeDates('fixed yesterday', now), 'fixed 2026-11-15');
    assert.equal(normalizeRelativeDates('freeze merges tomorrow', now), 'freeze merges 2026-11-17');
  });

  it('is case-insensitive and respects word boundaries', () => {
    assert.equal(normalizeRelativeDates('Today we start', now), '2026-11-16 we start');
    // "todays" must NOT be rewritten — no word boundary after "today".
    assert.equal(normalizeRelativeDates('todays build', now), 'todays build');
  });

  it('leaves text without relative terms unchanged', () => {
    assert.equal(normalizeRelativeDates('no dates here', now), 'no dates here');
  });

  it('defaults to the current date when now is omitted', () => {
    const out = normalizeRelativeDates('due today');
    assert.match(out, /^due \d{4}-\d{2}-\d{2}$/);
  });
});

describe('extractFacts — normalizes relative dates in model output', () => {
  it('converts a relative date in an extracted fact to an absolute one', async () => {
    const ex = new SessionMemoryExtractor(provider);
    ex._now = () => new Date(2026, 10, 16);
    const sink: { messages?: Message[] } = {};
    ex._streamFn = recordingStreamFn(
      [contentChunk(JSON.stringify([
        { fact: 'Merge freeze begins tomorrow for the release cut', category: 'decision', confidence: 0.9 },
      ]))],
      sink,
    );

    const facts = await ex.extractFacts([{ toolName: 'bash', output: 'ok', isError: false }], []);
    assert.equal(facts.length, 1);
    assert.equal(facts[0].fact, 'Merge freeze begins 2026-11-17 for the release cut');
  });
});

// ---------------------------------------------------------------------------
// Gap: pre-inject existing-memory manifest into the fact-extraction prompt
// ---------------------------------------------------------------------------
describe('extractFacts — existing-memory manifest pre-injection', () => {
  it('injects existing facts into the user prompt so the model can dedupe', async () => {
    const ex = new SessionMemoryExtractor(provider);
    const sink: { messages?: Message[] } = {};
    ex._streamFn = recordingStreamFn([contentChunk('[]')], sink);

    const existing: SessionFact[] = [
      { fact: 'Project uses pnpm', category: 'convention', confidence: 0.95 },
    ];
    await ex.extractFacts([{ toolName: 'bash', output: 'x', isError: false }], [], undefined, existing);

    const userMsg = sink.messages?.find(m => m.role === 'user');
    assert.ok(userMsg, 'expected a user message');
    const content = typeof userMsg!.content === 'string' ? userMsg!.content : '';
    assert.match(content, /Existing Memory/);
    assert.match(content, /\[convention\] Project uses pnpm/);
  });

  it('omits the manifest section when no existing facts are supplied', async () => {
    const ex = new SessionMemoryExtractor(provider);
    const sink: { messages?: Message[] } = {};
    ex._streamFn = recordingStreamFn([contentChunk('[]')], sink);

    await ex.extractFacts([{ toolName: 'bash', output: 'x', isError: false }], []);
    const userMsg = sink.messages?.find(m => m.role === 'user');
    const content = typeof userMsg!.content === 'string' ? userMsg!.content : '';
    assert.doesNotMatch(content, /Existing Memory/);
  });
});

// ---------------------------------------------------------------------------
// Gap: eviction preserves recency/criticality, not confidence-only truncation
// ---------------------------------------------------------------------------
describe('mergeFacts — criticality-aware eviction', () => {
  it('keeps a lower-confidence critical fact over a higher-confidence non-critical one', () => {
    // maxEntries=2, no recency protection, so eviction is purely the score-based pick.
    const ex = new SessionMemoryExtractor(provider, undefined, {
      maxEntries: 2,
      recencyProtectedCount: 0,
    });
    const facts: SessionFact[] = [
      { fact: 'A high-confidence convention', category: 'convention', confidence: 0.95 },
      { fact: 'B high-confidence convention', category: 'convention', confidence: 0.92 },
      // lower confidence but critical category -> boosted above both conventions.
      { fact: 'C critical error fix', category: 'error_fix', confidence: 0.75 },
    ];
    const merged = ex.mergeFacts([], facts);
    assert.equal(merged.length, 2);
    const kept = merged.map(f => f.fact);
    assert.ok(kept.includes('C critical error fix'), 'critical fact must survive eviction');
    assert.ok(kept.includes('A high-confidence convention'));
    assert.ok(!kept.includes('B high-confidence convention'), 'lowest-scoring non-critical fact is evicted');
  });

  it('defaults mark error_fix/decision/user_preference critical with a 0.5 boost', () => {
    assert.deepEqual(DEFAULT_SESSION_MEMORY_CONFIG.criticalCategories, [
      'error_fix',
      'decision',
      'user_preference',
    ]);
    assert.equal(DEFAULT_SESSION_MEMORY_CONFIG.criticalityBoost, 0.5);
  });

  it('confidence-only eviction (zero boost) would drop the critical fact — discriminates the fix', () => {
    const ex = new SessionMemoryExtractor(provider, undefined, {
      maxEntries: 2,
      recencyProtectedCount: 0,
      criticalityBoost: 0, // simulate the old confidence-only behavior
    });
    const facts: SessionFact[] = [
      { fact: 'A high-confidence convention', category: 'convention', confidence: 0.95 },
      { fact: 'B high-confidence convention', category: 'convention', confidence: 0.92 },
      { fact: 'C critical error fix', category: 'error_fix', confidence: 0.75 },
    ];
    const merged = ex.mergeFacts([], facts);
    assert.ok(!merged.map(f => f.fact).includes('C critical error fix'));
  });
});
