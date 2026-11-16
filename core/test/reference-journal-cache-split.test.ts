/**
 * buildUsageFields per-TTL cache-creation split red-green: the journal usage
 * schema carries cache_creation.{ephemeral_5m_input_tokens, ephemeral_1h_input_tokens}
 * so 1-hour vs 5-minute prompt-cache writes are priced/surfaced independently
 * (parity: Claude's emptyUsage). Previously the builder hardcoded
 * ephemeral_1h_input_tokens:0 and attributed ALL cache-creation to the 5m bucket,
 * so the 1h tier the schema models could never be observed. buildUsageFields must
 * now honor an explicit split supplied by the provider layer while preserving the
 * all-to-5m fallback when no split is present.
 *
 * Run: node --import tsx --test test/reference-journal-cache-split.test.ts
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { buildUsageFields, buildAssistantEntry } from '../src/reference-journal';
import { extractCacheCreationSplit, extractCacheCreationTokens } from '../src/provider';

describe('buildUsageFields cache-creation TTL split', () => {
  it('populates both the 5m and 1h buckets from an explicit split', () => {
    const u = buildUsageFields({
      promptTokens: 1000,
      completionTokens: 50,
      cachedPromptTokens: 200,
      cacheCreationTokens: 300,
      cacheCreation5mTokens: 180,
      cacheCreation1hTokens: 120,
    });
    assert.equal(u.cache_creation_input_tokens, 300);
    assert.equal(u.cache_creation.ephemeral_5m_input_tokens, 180);
    assert.equal(u.cache_creation.ephemeral_1h_input_tokens, 120);
  });

  it('honors a 1h-only split (5m arm defaults to 0, not the aggregate)', () => {
    const u = buildUsageFields({
      promptTokens: 400,
      completionTokens: 10,
      cacheCreationTokens: 150,
      cacheCreation1hTokens: 150,
    });
    assert.equal(u.cache_creation.ephemeral_1h_input_tokens, 150);
    assert.equal(u.cache_creation.ephemeral_5m_input_tokens, 0);
  });

  it('floors negative split arms to 0', () => {
    const u = buildUsageFields({
      promptTokens: 10,
      completionTokens: 0,
      cacheCreationTokens: 10,
      cacheCreation5mTokens: -5,
      cacheCreation1hTokens: -1,
    });
    assert.equal(u.cache_creation.ephemeral_5m_input_tokens, 0);
    assert.equal(u.cache_creation.ephemeral_1h_input_tokens, 0);
  });

  it('falls back to all-to-5m when no split is supplied (unchanged behavior)', () => {
    const u = buildUsageFields({ promptTokens: 1000, completionTokens: 200, cacheCreationTokens: 100 });
    assert.equal(u.cache_creation.ephemeral_5m_input_tokens, 100);
    assert.equal(u.cache_creation.ephemeral_1h_input_tokens, 0);
  });

  // End-to-end: a provider finish-step `part` (the shape the AI SDK yields)
  // flows through the extractors into the usage input the engine hands to
  // buildAssistantEntry, and the persisted journal line carries the TTL split.
  it('round-trips a provider part into a journal line with the TTL split preserved', () => {
    const part = {
      usage: { inputTokens: 1000, outputTokens: 40, cacheCreationInputTokens: 300 },
      providerMetadata: {
        anthropic: { cache_creation: { ephemeral_5m_input_tokens: 220, ephemeral_1h_input_tokens: 80 } },
      },
    };
    const split = extractCacheCreationSplit(part);
    const entry = buildAssistantEntry({
      uuid: 'u1', parentUuid: null, timestamp: '2026-01-01T00:00:00.000Z',
      sessionId: 's1', cwd: '/x', gitBranch: 'main', isSidechain: false,
      messageId: 's1:1', model: 'claude-opus-4-8', stopReason: 'end_turn',
      usage: {
        promptTokens: 1000,
        completionTokens: 40,
        cacheCreationTokens: extractCacheCreationTokens(part),
        cacheCreation5mTokens: split.ephemeral5m,
        cacheCreation1hTokens: split.ephemeral1h,
      },
      content: [{ type: 'text', text: 'ok' }],
    });
    assert.equal(entry.message.usage.cache_creation_input_tokens, 300);
    assert.equal(entry.message.usage.cache_creation.ephemeral_5m_input_tokens, 220);
    assert.equal(entry.message.usage.cache_creation.ephemeral_1h_input_tokens, 80);
  });
});
