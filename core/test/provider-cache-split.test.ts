/**
 * extractCacheCreationSplit red-green: Anthropic bills 1-hour and 5-minute
 * prompt-cache WRITES at different rates, so the per-TTL split carried in
 * providerMetadata.anthropic.cache_creation must be surfaced independently
 * (parity: the reference's emptyUsage cache_creation {ephemeral_1h_input_tokens,
 * ephemeral_5m_input_tokens}). When no split is present the whole aggregate is a
 * 5m write.
 *
 * Run: node --import tsx --test test/provider-cache-split.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { extractCacheCreationSplit } from '../src/provider';

describe('extractCacheCreationSplit', () => {
  it('reads an explicit snake_case per-TTL split', () => {
    const part = {
      usage: { cacheCreationInputTokens: 300 },
      providerMetadata: {
        anthropic: {
          cache_creation: { ephemeral_5m_input_tokens: 200, ephemeral_1h_input_tokens: 100 },
        },
      },
    };
    assert.deepEqual(extractCacheCreationSplit(part), { ephemeral5m: 200, ephemeral1h: 100 });
  });

  it('reads an explicit camelCase per-TTL split', () => {
    const part = {
      providerMetadata: {
        anthropic: {
          cacheCreation: { ephemeral5mInputTokens: 10, ephemeral1hInputTokens: 40 },
        },
      },
    };
    assert.deepEqual(extractCacheCreationSplit(part), { ephemeral5m: 10, ephemeral1h: 40 });
  });

  it('attributes the whole aggregate to 5m when no split is surfaced', () => {
    const part = { usage: { cacheCreationInputTokens: 123 } };
    assert.deepEqual(extractCacheCreationSplit(part), { ephemeral5m: 123, ephemeral1h: 0 });
  });

  it('defaults a missing arm to 0 and floors non-numeric / negative values', () => {
    const part = {
      providerMetadata: { anthropic: { cache_creation: { ephemeral_1h_input_tokens: 50, ephemeral_5m_input_tokens: -5 } } },
    };
    // 5m is non-numeric-after-floor (negative rejected → undefined → 0), 1h honored.
    assert.deepEqual(extractCacheCreationSplit(part), { ephemeral5m: 0, ephemeral1h: 50 });
  });

  it('returns zeros for an empty / nullish part', () => {
    assert.deepEqual(extractCacheCreationSplit(undefined), { ephemeral5m: 0, ephemeral1h: 0 });
    assert.deepEqual(extractCacheCreationSplit({}), { ephemeral5m: 0, ephemeral1h: 0 });
  });
});
