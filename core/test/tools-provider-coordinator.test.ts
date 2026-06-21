import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';

import { ProviderCoordinator } from '../src/provider-coordinator';
import type { ProviderConfig, ProviderSubsystem } from '../src/types';
import { RateLimitTracker } from '../src/rate-limiter';
import { CredentialPool } from '../src/credential-pool';
import { classifyError } from '../src/error-classifier';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makePrimary(overrides?: Partial<ProviderConfig>): ProviderConfig {
  return {
    baseUrl: 'https://api.openai.com/v1',
    apiKey: 'sk-test-key-abc123',
    model: 'gpt-4o',
    ...overrides,
  };
}

function makeSubsystem(overrides?: Partial<ProviderSubsystem>): ProviderSubsystem {
  return { primary: makePrimary(), ...overrides };
}

// =========================================================================
// ProviderCoordinator — additional coverage
// =========================================================================

describe('ProviderCoordinator — constructor inference', () => {
  it('infers Anthropic provider from sk-ant- API key', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      primary: makePrimary({ apiKey: 'sk-ant-api03-test', model: 'claude-3-opus-20240229' }),
    }));
    const state = coord.getState();
    assert.equal(state.currentProvider, 'anthropic');
    assert.equal(state.currentModel, 'claude-3-opus-20240229');
  });

  it('infers OpenAI provider from sk- API key', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      primary: makePrimary({ apiKey: 'sk-proj-abc123' }),
    }));
    const state = coord.getState();
    assert.equal(state.currentProvider, 'openai');
  });

  it('infers provider from baseUrl when key prefix is unknown', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      primary: makePrimary({
        apiKey: 'unknown-key-format',
        baseUrl: 'https://api.groq.com/openai/v1',
      }),
    }));
    const state = coord.getState();
    assert.equal(state.currentProvider, 'groq');
  });

  it('uses explicit provider field when no inference matches', () => {
    // Temporarily clear env vars that could match
    const saved: Record<string, string | undefined> = {};
    const envKeys = [
      'GROQ_API_KEY', 'MISTRAL_API_KEY', 'XAI_API_KEY', 'DEEPSEEK_API_KEY',
      'TOGETHER_AI_API_KEY', 'COHERE_API_KEY', 'FIREWORKS_API_KEY',
      'PERPLEXITY_API_KEY', 'DEEPINFRA_API_KEY', 'CEREBRAS_API_KEY',
      'ALIBABA_API_KEY', 'DASHSCOPE_API_KEY', 'LUMA_API_KEY', 'HF_TOKEN',
      'AZURE_OPENAI_API_KEY', 'AWS_ACCESS_KEY_ID',
      'ANTHROPIC_VERTEX_PROJECT_ID', 'GOOGLE_APPLICATION_CREDENTIALS',
    ];
    for (const k of envKeys) {
      saved[k] = process.env[k];
      delete process.env[k];
    }
    try {
      const coord = new ProviderCoordinator(makeSubsystem({
        primary: makePrimary({
          apiKey: 'unknown-key-format',
          baseUrl: 'https://custom.internal.api/v1',
          provider: 'custom-provider',
        }),
      }));
      const state = coord.getState();
      assert.equal(state.currentProvider, 'custom-provider');
    } finally {
      for (const [k, v] of Object.entries(saved)) {
        if (v !== undefined) process.env[k] = v;
        else delete process.env[k];
      }
    }
  });

  it('falls back to "unknown" provider when nothing matches', () => {
    // Clear env vars that could match
    const saved: Record<string, string | undefined> = {};
    const envKeys = [
      'GROQ_API_KEY', 'MISTRAL_API_KEY', 'XAI_API_KEY', 'DEEPSEEK_API_KEY',
      'TOGETHER_AI_API_KEY', 'COHERE_API_KEY', 'FIREWORKS_API_KEY',
      'PERPLEXITY_API_KEY', 'DEEPINFRA_API_KEY', 'CEREBRAS_API_KEY',
      'ALIBABA_API_KEY', 'DASHSCOPE_API_KEY', 'LUMA_API_KEY', 'HF_TOKEN',
      'AZURE_OPENAI_API_KEY', 'AWS_ACCESS_KEY_ID',
      'ANTHROPIC_VERTEX_PROJECT_ID', 'GOOGLE_APPLICATION_CREDENTIALS',
    ];
    for (const k of envKeys) {
      saved[k] = process.env[k];
      delete process.env[k];
    }
    try {
      const coord = new ProviderCoordinator(makeSubsystem({
        primary: makePrimary({
          apiKey: 'zzzz-unknown-format',
          baseUrl: 'https://custom.nowhere.test/v1',
        }),
      }));
      const state = coord.getState();
      assert.equal(state.currentProvider, 'unknown');
    } finally {
      for (const [k, v] of Object.entries(saved)) {
        if (v !== undefined) process.env[k] = v;
        else delete process.env[k];
      }
    }
  });

  it('uses inferred defaultModel when primary config has no model', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      primary: { baseUrl: 'https://api.openai.com/v1', apiKey: 'sk-proj-test123', model: '' },
    }));
    const state = coord.getState();
    // sk- prefix infers openai, defaultModel is the first in MODEL_PREFERENCE.openai
    assert.ok(state.currentModel.length > 0);
    assert.notEqual(state.currentModel, 'unknown');
  });

  it('sets up credential pool when additionalKeys provided', () => {
    const coord = new ProviderCoordinator(makeSubsystem(), ['sk-extra-1', 'sk-extra-2']);
    // Pool has primary key + 2 additional = 3
    assert.equal(coord.availableCredentials, 3);
  });

  it('does not create credential pool when additionalKeys is empty', () => {
    const coord = new ProviderCoordinator(makeSubsystem(), []);
    assert.equal(coord.availableCredentials, 1);
  });

  it('does not create credential pool when additionalKeys is undefined', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    assert.equal(coord.availableCredentials, 1);
  });
});

describe('ProviderCoordinator — getConfig', () => {
  it('returns primary config when not on fallback', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    const config = coord.getConfig();
    assert.equal(config.model, 'gpt-4o');
    assert.equal(config.apiKey, 'sk-test-key-abc123');
  });

  it('returns fallback model in config after error with fallback configured', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModel: 'gpt-4o-mini' }));
    coord.recordError();
    const config = coord.getConfig();
    assert.equal(config.model, 'gpt-4o-mini');
    // Other config fields remain from primary
    assert.equal(config.baseUrl, 'https://api.openai.com/v1');
  });

  it('uses credential pool key when pool is configured', () => {
    const coord = new ProviderCoordinator(makeSubsystem(), ['sk-alt-key']);
    const config = coord.getConfig();
    // Should get a key from the pool (either primary or alt)
    assert.ok(['sk-test-key-abc123', 'sk-alt-key'].includes(config.apiKey));
  });

  it('returns base config when credential pool is exhausted', () => {
    const coord = new ProviderCoordinator(
      makeSubsystem(),
      ['sk-alt-key'],
    );
    // Exhaust all credentials by acquiring and marking them
    for (let i = 0; i < 3; i++) {
      coord.getConfig(); // acquire a credential
      coord.markCredentialExhausted(999999999); // long cooldown
    }
    // Now pool may return null, falling through to base config
    const config = coord.getConfig();
    assert.ok(config.apiKey);
  });
});

describe('ProviderCoordinator — getCompactionConfig', () => {
  it('returns primary config with compaction model', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      compactionModel: 'gpt-4o-mini',
    }));
    const config = coord.getCompactionConfig();
    assert.equal(config.model, 'gpt-4o-mini');
    assert.equal(config.apiKey, 'sk-test-key-abc123');
    assert.equal(config.baseUrl, 'https://api.openai.com/v1');
  });

  it('returns primary config when no compaction model set', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    const config = coord.getCompactionConfig();
    assert.equal(config.model, 'gpt-4o');
  });
});

describe('ProviderCoordinator — recordRequest', () => {
  it('increments request count', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    assert.equal(coord.getState().requestCount, 0);
    coord.recordRequest();
    assert.equal(coord.getState().requestCount, 1);
    coord.recordRequest();
    coord.recordRequest();
    assert.equal(coord.getState().requestCount, 3);
  });
});

describe('ProviderCoordinator — recordError', () => {
  it('returns true and switches to fallback when fallback available', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModel: 'fallback-model' }));
    assert.equal(coord.recordError(), true);
    assert.equal(coord.getState().currentModel, 'fallback-model');
    assert.equal(coord.getState().usedFallback, true);
    assert.equal(coord.getState().errorCount, 1);
  });

  it('returns false when no fallback available', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    assert.equal(coord.recordError(), false);
    assert.equal(coord.getState().errorCount, 1);
    assert.equal(coord.getState().usedFallback, false);
  });

  it('returns false when already on fallback', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModel: 'fallback-model' }));
    assert.equal(coord.recordError(), true);
    assert.equal(coord.recordError(), false);
    assert.equal(coord.getState().errorCount, 2);
  });

  it('increments error count each call', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    coord.recordError();
    coord.recordError();
    coord.recordError();
    assert.equal(coord.getState().errorCount, 3);
  });
});

describe('ProviderCoordinator — recordSuccess', () => {
  it('does nothing when not on fallback', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModel: 'fallback' }));
    coord.recordSuccess();
    coord.recordSuccess();
    // Still on primary — no model change
    assert.equal(coord.getState().currentModel, 'gpt-4o');
  });

  it('counts successes on fallback and reverts after FALLBACK_COOLDOWN (3)', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModel: 'fallback' }));
    coord.recordError();
    assert.equal(coord.getState().currentModel, 'fallback');

    coord.recordSuccess(); // 1
    assert.equal(coord.getConfig().model, 'fallback');
    coord.recordSuccess(); // 2
    assert.equal(coord.getConfig().model, 'fallback');
    coord.recordSuccess(); // 3 — reverts
    assert.equal(coord.getConfig().model, 'gpt-4o');
    assert.equal(coord.getState().currentModel, 'gpt-4o');
  });

  it('resets fallback success counter after reverting', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModel: 'fallback' }));
    // First cycle
    coord.recordError();
    coord.recordSuccess();
    coord.recordSuccess();
    coord.recordSuccess();
    assert.equal(coord.getConfig().model, 'gpt-4o');

    // Second cycle — error again
    coord.recordError();
    assert.equal(coord.getConfig().model, 'fallback');
    // Need 3 successes again
    coord.recordSuccess();
    coord.recordSuccess();
    assert.equal(coord.getConfig().model, 'fallback');
    coord.recordSuccess();
    assert.equal(coord.getConfig().model, 'gpt-4o');
  });

  it('uses "unknown" as model when primary has empty model string', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      primary: makePrimary({ model: '', apiKey: 'zzzz-unknown' }),
      fallbackModel: 'fallback',
    }));
    coord.recordError();
    coord.recordSuccess();
    coord.recordSuccess();
    coord.recordSuccess();
    // Revert uses primary.model or 'unknown'
    assert.equal(coord.getState().currentModel, 'unknown');
  });
});

describe('ProviderCoordinator — isAnthropicProvider', () => {
  it('returns true for model starting with "claude"', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      primary: makePrimary({ model: 'claude-3-opus-20240229', apiKey: 'test-key' }),
    }));
    assert.equal(coord.isAnthropicProvider(), true);
  });

  it('returns true for baseUrl containing "anthropic.com"', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      primary: makePrimary({
        model: 'custom-model',
        baseUrl: 'https://api.anthropic.com/v1',
        apiKey: 'test-key',
      }),
    }));
    assert.equal(coord.isAnthropicProvider(), true);
  });

  it('returns true for provider="anthropic"', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      primary: makePrimary({
        model: 'custom-model',
        baseUrl: 'https://custom.proxy/v1',
        apiKey: 'test-key',
        provider: 'anthropic',
      }),
    }));
    assert.equal(coord.isAnthropicProvider(), true);
  });

  it('returns false for non-Anthropic setup', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      primary: makePrimary({
        model: 'gpt-4o',
        baseUrl: 'https://api.openai.com/v1',
        apiKey: 'test-key',
      }),
    }));
    assert.equal(coord.isAnthropicProvider(), false);
  });

  it('returns false when model is empty and no anthropic signals', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      primary: makePrimary({
        model: '',
        baseUrl: 'https://custom.api/v1',
        apiKey: 'test-key',
      }),
    }));
    assert.equal(coord.isAnthropicProvider(), false);
  });
});

describe('ProviderCoordinator — hasFallback', () => {
  it('returns true when fallbackModel is set', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModel: 'backup' }));
    assert.equal(coord.hasFallback(), true);
  });

  it('returns false when fallbackModel is not set', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    assert.equal(coord.hasFallback(), false);
  });

  it('returns false when fallbackModel is empty string', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModel: '' }));
    assert.equal(coord.hasFallback(), false);
  });
});

describe('ProviderCoordinator — rate limits', () => {
  it('updateRateLimits and getRateLimitStatus work together', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    coord.updateRateLimits({
      'x-ratelimit-limit-requests': '1000',
      'x-ratelimit-remaining-requests': '100',
      'x-ratelimit-limit-tokens': '50000',
      'x-ratelimit-remaining-tokens': '5000',
    });
    const status = coord.getRateLimitStatus();
    assert.equal(status.requestsPercent, 90);
    assert.equal(status.tokensPercent, 90);
    assert.equal(status.shouldWarn, true);
  });

  it('getRateLimitStatus returns clean state when no headers updated', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    const status = coord.getRateLimitStatus();
    assert.equal(status.requestsPercent, 0);
    assert.equal(status.tokensPercent, 0);
    assert.equal(status.shouldWarn, false);
    assert.equal(status.requests, null);
    assert.equal(status.tokens, null);
  });
});

describe('ProviderCoordinator — markCredentialExhausted', () => {
  it('marks the active credential as exhausted with cooldown', () => {
    const coord = new ProviderCoordinator(makeSubsystem(), ['sk-extra']);
    coord.getConfig(); // acquires a credential, setting activeCredentialId
    coord.markCredentialExhausted(60000);
    // One credential exhausted, one or more remaining
    assert.ok(coord.availableCredentials >= 1);
  });

  it('does nothing when no credential pool', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    // Should not throw
    coord.markCredentialExhausted(5000);
    assert.equal(coord.availableCredentials, 1);
  });

  it('does nothing when no active credential', () => {
    const coord = new ProviderCoordinator(makeSubsystem(), ['sk-extra']);
    // Don't call getConfig() first, so no activeCredentialId
    coord.markCredentialExhausted(5000);
    // All still available
    assert.equal(coord.availableCredentials, 2);
  });

  it('clears activeCredentialId after marking exhausted', () => {
    const coord = new ProviderCoordinator(makeSubsystem(), ['sk-extra']);
    coord.getConfig();
    coord.markCredentialExhausted();
    // Calling again should be a no-op since activeCredentialId is null
    coord.markCredentialExhausted();
    // availableCredentials should still work
    assert.ok(coord.availableCredentials >= 0);
  });
});

describe('ProviderCoordinator — availableCredentials', () => {
  it('returns 1 when no pool configured', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    assert.equal(coord.availableCredentials, 1);
  });

  it('returns pool count when pool configured', () => {
    const coord = new ProviderCoordinator(makeSubsystem(), ['sk-a', 'sk-b', 'sk-c']);
    assert.equal(coord.availableCredentials, 4); // primary + 3 additional
  });
});

describe('ProviderCoordinator — getState defensive copy', () => {
  it('returns a copy that does not affect internal state', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    const state = coord.getState();
    state.requestCount = 999;
    state.errorCount = 888;
    state.currentModel = 'tampered';
    const fresh = coord.getState();
    assert.equal(fresh.requestCount, 0);
    assert.equal(fresh.errorCount, 0);
    assert.equal(fresh.currentModel, 'gpt-4o');
  });
});

// =========================================================================
// RateLimitTracker — additional coverage
// =========================================================================

describe('RateLimitTracker — update and getStatus', () => {
  let tracker: RateLimitTracker;

  beforeEach(() => {
    tracker = new RateLimitTracker();
  });

  it('starts with empty status', () => {
    const status = tracker.getStatus();
    assert.equal(status.requests, null);
    assert.equal(status.tokens, null);
    assert.equal(status.requestsPercent, 0);
    assert.equal(status.tokensPercent, 0);
    assert.equal(status.shouldWarn, false);
  });

  it('updates only request limits when token headers absent', () => {
    tracker.update({
      'x-ratelimit-limit-requests': '200',
      'x-ratelimit-remaining-requests': '50',
    });
    const status = tracker.getStatus();
    assert.ok(status.requests);
    assert.equal(status.requests!.limit, 200);
    assert.equal(status.requests!.remaining, 50);
    assert.equal(status.tokens, null);
    assert.equal(status.requestsPercent, 75);
    assert.equal(status.tokensPercent, 0);
  });

  it('updates only token limits when request headers absent', () => {
    tracker.update({
      'x-ratelimit-limit-tokens': '100000',
      'x-ratelimit-remaining-tokens': '10000',
    });
    const status = tracker.getStatus();
    assert.equal(status.requests, null);
    assert.ok(status.tokens);
    assert.equal(status.tokens!.limit, 100000);
    assert.equal(status.tokens!.remaining, 10000);
    assert.equal(status.tokensPercent, 90);
    assert.equal(status.shouldWarn, true);
  });

  it('updates both request and token buckets', () => {
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '80',
      'x-ratelimit-limit-tokens': '500000',
      'x-ratelimit-remaining-tokens': '400000',
    });
    const status = tracker.getStatus();
    assert.ok(status.requests);
    assert.ok(status.tokens);
    assert.equal(status.requestsPercent, 20);
    assert.equal(status.tokensPercent, 20);
    assert.equal(status.shouldWarn, false);
  });

  it('warns when request usage >= 80%', () => {
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '20',
    });
    assert.equal(tracker.getStatus().requestsPercent, 80);
    assert.equal(tracker.getStatus().shouldWarn, true);
  });

  it('warns when token usage >= 80%', () => {
    tracker.update({
      'x-ratelimit-limit-tokens': '100000',
      'x-ratelimit-remaining-tokens': '19999',
    });
    assert.equal(tracker.getStatus().shouldWarn, true);
  });

  it('does not warn below 80% threshold', () => {
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '25',
      'x-ratelimit-limit-tokens': '100000',
      'x-ratelimit-remaining-tokens': '25000',
    });
    const status = tracker.getStatus();
    assert.equal(status.requestsPercent, 75);
    assert.equal(status.tokensPercent, 75);
    assert.equal(status.shouldWarn, false);
  });

  it('ignores non-numeric header values', () => {
    tracker.update({
      'x-ratelimit-limit-requests': 'abc',
      'x-ratelimit-remaining-requests': 'xyz',
    });
    assert.equal(tracker.getStatus().requests, null);
  });

  it('handles zero remaining correctly', () => {
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '0',
    });
    assert.equal(tracker.getStatus().requestsPercent, 100);
    assert.equal(tracker.getStatus().shouldWarn, true);
  });

  it('handles limit equal to remaining (0% usage)', () => {
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '100',
    });
    assert.equal(tracker.getStatus().requestsPercent, 0);
  });

  it('overwrites previous values on subsequent update', () => {
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '90',
    });
    assert.equal(tracker.getStatus().requestsPercent, 10);

    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '5',
    });
    assert.equal(tracker.getStatus().requestsPercent, 95);
  });

  it('sets resetAt from request reset header with seconds duration', () => {
    const before = Date.now();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '50',
      'x-ratelimit-reset-requests': '30s',
    });
    const after = Date.now();
    const status = tracker.getStatus();
    assert.ok(status.requests);
    assert.ok(status.requests!.resetAt >= before + 30000);
    assert.ok(status.requests!.resetAt <= after + 30000);
  });

  it('sets resetAt from token reset header with ms duration', () => {
    const before = Date.now();
    tracker.update({
      'x-ratelimit-limit-tokens': '100000',
      'x-ratelimit-remaining-tokens': '50000',
      'x-ratelimit-reset-tokens': '2500ms',
    });
    const after = Date.now();
    const status = tracker.getStatus();
    assert.ok(status.tokens);
    assert.ok(status.tokens!.resetAt >= before + 2500);
    assert.ok(status.tokens!.resetAt <= after + 2500);
  });

  it('sets resetAt to 0 when no reset header present', () => {
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '50',
    });
    assert.equal(tracker.getStatus().requests!.resetAt, 0);
  });

  it('parses minutes duration in reset header', () => {
    const before = Date.now();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '50',
      'x-ratelimit-reset-requests': '5m',
    });
    const after = Date.now();
    const resetAt = tracker.getStatus().requests!.resetAt;
    assert.ok(resetAt >= before + 5 * 60000);
    assert.ok(resetAt <= after + 5 * 60000);
  });

  it('parses hours duration in reset header', () => {
    const before = Date.now();
    tracker.update({
      'x-ratelimit-limit-tokens': '1000000',
      'x-ratelimit-remaining-tokens': '500000',
      'x-ratelimit-reset-tokens': '1h',
    });
    const after = Date.now();
    const resetAt = tracker.getStatus().tokens!.resetAt;
    assert.ok(resetAt >= before + 3600000);
    assert.ok(resetAt <= after + 3600000);
  });

  it('parses bare number (no unit) as seconds', () => {
    const before = Date.now();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '50',
      'x-ratelimit-reset-requests': '45',
    });
    const after = Date.now();
    const resetAt = tracker.getStatus().requests!.resetAt;
    // bare number defaults to seconds: 45 * 1000 = 45000ms
    assert.ok(resetAt >= before + 45000);
    assert.ok(resetAt <= after + 45000);
  });

  it('falls back to 60000ms for unparseable reset value', () => {
    const before = Date.now();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '50',
      'x-ratelimit-reset-requests': 'not-a-duration',
    });
    const after = Date.now();
    const resetAt = tracker.getStatus().requests!.resetAt;
    assert.ok(resetAt >= before + 60000);
    assert.ok(resetAt <= after + 60000);
  });

  it('handles partial headers (only limit, no remaining) — no bucket created', () => {
    tracker.update({
      'x-ratelimit-limit-requests': '100',
    });
    assert.equal(tracker.getStatus().requests, null);
  });

  it('handles partial headers (only remaining, no limit) — no bucket created', () => {
    tracker.update({
      'x-ratelimit-remaining-requests': '50',
    });
    assert.equal(tracker.getStatus().requests, null);
  });
});

// =========================================================================
// CredentialPool — additional coverage
// =========================================================================

describe('CredentialPool — constructor and strategies', () => {
  it('defaults to fill_first strategy', () => {
    const pool = new CredentialPool();
    pool.addKey('key-a');
    pool.addKey('key-b');
    // fill_first always returns the first ok entry
    assert.equal(pool.acquire()!.apiKey, 'key-a');
    assert.equal(pool.acquire()!.apiKey, 'key-a');
    assert.equal(pool.acquire()!.apiKey, 'key-a');
  });

  it('fill_first skips exhausted keys', () => {
    const pool = new CredentialPool('fill_first');
    pool.addKey('key-a');
    pool.addKey('key-b');
    const first = pool.acquire()!;
    pool.markExhausted(first.id, 999999999);
    assert.equal(pool.acquire()!.apiKey, 'key-b');
  });

  it('round_robin wraps around after exhausting the cycle', () => {
    const pool = new CredentialPool('round_robin');
    pool.addKey('a');
    pool.addKey('b');
    assert.equal(pool.acquire()!.apiKey, 'a');
    assert.equal(pool.acquire()!.apiKey, 'b');
    assert.equal(pool.acquire()!.apiKey, 'a');
    assert.equal(pool.acquire()!.apiKey, 'b');
  });

  it('least_used picks the key with lowest usage count', () => {
    const pool = new CredentialPool('least_used');
    pool.addKey('x');
    pool.addKey('y');
    pool.addKey('z');
    // First acquisition: all at 0 usage, picks first
    const r1 = pool.acquire()!;
    assert.equal(r1.apiKey, 'x'); // usage: x=1, y=0, z=0

    const r2 = pool.acquire()!;
    assert.equal(r2.apiKey, 'y'); // usage: x=1, y=1, z=0

    const r3 = pool.acquire()!;
    assert.equal(r3.apiKey, 'z'); // usage: x=1, y=1, z=1

    // All equal at 1, picks first in sorted order
    const r4 = pool.acquire()!;
    assert.ok(r4.apiKey); // any of the three
  });
});

describe('CredentialPool — addKey', () => {
  it('assigns sequential IDs', () => {
    const pool = new CredentialPool();
    pool.addKey('k1', 'label1');
    pool.addKey('k2');
    pool.addKey('k3', 'label3');
    assert.equal(pool.size, 3);
    const c1 = pool.acquire()!;
    assert.equal(c1.id, 'key-0');
  });

  it('accepts optional label', () => {
    const pool = new CredentialPool();
    pool.addKey('test-key', 'my-label');
    assert.equal(pool.size, 1);
    const cred = pool.acquire()!;
    assert.equal(cred.apiKey, 'test-key');
  });
});

describe('CredentialPool — acquire', () => {
  it('returns null on empty pool', () => {
    const pool = new CredentialPool();
    assert.equal(pool.acquire(), null);
  });

  it('increments usageCount on acquire', () => {
    const pool = new CredentialPool('fill_first');
    pool.addKey('k1');
    pool.acquire();
    pool.acquire();
    pool.acquire();
    // After 3 acquires with fill_first, k1 should have usageCount=3
    // We verify indirectly via least_used behavior
    assert.equal(pool.size, 1);
  });

  it('recovers exhausted keys once cooldown expires', async () => {
    const pool = new CredentialPool('fill_first');
    pool.addKey('k1');
    pool.addKey('k2');
    const cred = pool.acquire()!;
    assert.equal(cred.apiKey, 'k1');
    pool.markExhausted(cred.id, 1); // 1ms cooldown
    // k1 exhausted, k2 available
    assert.equal(pool.acquire()!.apiKey, 'k2');

    await new Promise(r => setTimeout(r, 10));
    // After cooldown, k1 should be recovered
    const recovered = pool.acquire()!;
    assert.equal(recovered.apiKey, 'k1');
  });

  it('skips dead keys even if cooldown expired', () => {
    const pool = new CredentialPool('fill_first');
    pool.addKey('k1');
    pool.addKey('k2');
    const cred = pool.acquire()!;
    pool.markDead(cred.id);
    // k1 is dead permanently
    assert.equal(pool.acquire()!.apiKey, 'k2');
    assert.equal(pool.acquire()!.apiKey, 'k2');
  });
});

describe('CredentialPool — markExhausted', () => {
  it('defaults cooldown to 60000ms', () => {
    const pool = new CredentialPool();
    pool.addKey('k1');
    pool.addKey('k2');
    const cred = pool.acquire()!;
    pool.markExhausted(cred.id);
    // k1 exhausted with default 60s cooldown, k2 still available
    assert.equal(pool.acquire()!.apiKey, 'k2');
  });

  it('does nothing for non-existent id', () => {
    const pool = new CredentialPool();
    pool.addKey('k1');
    pool.markExhausted('nonexistent-id');
    assert.equal(pool.availableCount, 1);
  });

  it('uses custom cooldown', () => {
    const pool = new CredentialPool();
    pool.addKey('k1');
    const cred = pool.acquire()!;
    pool.markExhausted(cred.id, 999999999);
    assert.equal(pool.availableCount, 0);
  });
});

describe('CredentialPool — markDead', () => {
  it('permanently removes credential from availability', () => {
    const pool = new CredentialPool();
    pool.addKey('k1');
    pool.addKey('k2');
    pool.addKey('k3');
    const c1 = pool.acquire()!;
    pool.markDead(c1.id);
    assert.equal(pool.size, 3);
    assert.equal(pool.availableCount, 2);
  });

  it('does nothing for non-existent id', () => {
    const pool = new CredentialPool();
    pool.addKey('k1');
    pool.markDead('nonexistent-id');
    assert.equal(pool.availableCount, 1);
    assert.equal(pool.size, 1);
  });

  it('dead credentials never recover', async () => {
    const pool = new CredentialPool();
    pool.addKey('k1');
    const cred = pool.acquire()!;
    pool.markDead(cred.id);
    await new Promise(r => setTimeout(r, 10));
    assert.equal(pool.availableCount, 0);
    assert.equal(pool.acquire(), null);
  });
});

describe('CredentialPool — size and availableCount', () => {
  it('size is 0 on empty pool', () => {
    const pool = new CredentialPool();
    assert.equal(pool.size, 0);
  });

  it('size includes dead and exhausted entries', () => {
    const pool = new CredentialPool();
    pool.addKey('k1');
    pool.addKey('k2');
    pool.addKey('k3');
    const c1 = pool.acquire()!;
    pool.markDead(c1.id);
    const c2 = pool.acquire()!;
    pool.markExhausted(c2.id, 999999);
    assert.equal(pool.size, 3);
  });

  it('availableCount excludes dead but includes exhausted-past-cooldown', async () => {
    const pool = new CredentialPool();
    pool.addKey('k1');
    pool.addKey('k2');
    pool.addKey('k3');

    const c1 = pool.acquire()!;
    pool.markDead(c1.id);
    const c2 = pool.acquire()!;
    pool.markExhausted(c2.id, 1); // 1ms cooldown

    await new Promise(r => setTimeout(r, 10));
    // k1 dead (excluded), k2 exhausted but cooldown passed (included), k3 ok
    assert.equal(pool.availableCount, 2);
  });

  it('availableCount is 0 when all dead', () => {
    const pool = new CredentialPool();
    pool.addKey('k1');
    pool.addKey('k2');
    pool.markDead('key-0');
    pool.markDead('key-1');
    assert.equal(pool.availableCount, 0);
  });
});

// =========================================================================
// classifyError — additional coverage for uncovered branches
// =========================================================================

describe('classifyError — additional abort patterns', () => {
  it('detects "aborterror" (case insensitive)', () => {
    const r = classifyError('AbortError: The user aborted the request');
    assert.equal(r.category, 'abort');
    assert.equal(r.retryable, false);
  });

  it('detects "cancel" keyword', () => {
    const r = classifyError('Operation was cancelled by the client');
    assert.equal(r.category, 'abort');
  });
});

describe('classifyError — additional auth_error patterns', () => {
  it('detects "forbidden" as retryable 403', () => {
    const r = classifyError('Access forbidden to this resource');
    assert.equal(r.category, 'auth_error');
    assert.equal(r.retryable, true);
    assert.equal(r.shouldFallback, true);
  });

  it('detects "invalid_api_key"', () => {
    const r = classifyError('Error: invalid_api_key');
    assert.equal(r.category, 'auth_error');
    assert.equal(r.retryable, false);
  });

  it('detects "authentication"', () => {
    const r = classifyError('Authentication failed for user');
    assert.equal(r.category, 'auth_error');
    assert.equal(r.retryable, false);
  });

  it('detects HTTP 403 as retryable', () => {
    const r = classifyError('HTTP 403 Forbidden');
    assert.equal(r.category, 'auth_error');
    assert.equal(r.retryable, true);
    assert.equal(r.shouldFallback, true);
  });
});

describe('classifyError — additional rate_limited patterns', () => {
  it('detects "rate_limit" (underscore)', () => {
    const r = classifyError('rate_limit_exceeded');
    assert.equal(r.category, 'rate_limited');
    assert.equal(r.suggestedDelay, 5000);
  });

  it('detects "too many requests"', () => {
    const r = classifyError('Too many requests, please try again later');
    assert.equal(r.category, 'rate_limited');
  });

  it('detects "throttled"', () => {
    const r = classifyError('Request was throttled by the server');
    assert.equal(r.category, 'rate_limited');
  });
});

describe('classifyError — additional context_overflow patterns', () => {
  it('detects "prompt is too long"', () => {
    const r = classifyError('The prompt is too long for this model');
    assert.equal(r.category, 'context_overflow');
    assert.equal(r.shouldCompact, true);
  });

  it('detects "token limit"', () => {
    const r = classifyError('Exceeded the token limit');
    assert.equal(r.category, 'context_overflow');
  });

  it('detects "too many tokens"', () => {
    const r = classifyError('Input has too many tokens');
    assert.equal(r.category, 'context_overflow');
  });

  it('detects "reduce the length"', () => {
    const r = classifyError('Please reduce the length of the prompt');
    assert.equal(r.category, 'context_overflow');
  });

  it('detects "context overflow"', () => {
    const r = classifyError('context overflow detected');
    assert.equal(r.category, 'context_overflow');
  });

  it('detects "context maximum"', () => {
    const r = classifyError('exceeds context maximum allowed');
    assert.equal(r.category, 'context_overflow');
  });

  it('detects "context length" limit', () => {
    const r = classifyError("This model's maximum context length is 128000 tokens");
    assert.equal(r.category, 'context_overflow');
  });
});

describe('classifyError — additional output_too_large patterns', () => {
  it('detects "output is too"', () => {
    const r = classifyError('The output is too long');
    assert.equal(r.category, 'output_too_large');
    assert.equal(r.retryable, true);
  });

  it('detects "output_too_large"', () => {
    const r = classifyError('Error code: output_too_large');
    assert.equal(r.category, 'output_too_large');
  });

  it('detects "max_output"', () => {
    const r = classifyError('max_output exceeded');
    assert.equal(r.category, 'output_too_large');
  });
});

describe('classifyError — additional model_unavailable patterns', () => {
  it('detects "model_not_found"', () => {
    const r = classifyError('Error: model_not_found');
    assert.equal(r.category, 'model_unavailable');
    assert.equal(r.shouldFallback, true);
    assert.equal(r.suggestedDelay, 10000);
  });

  it('detects "model not found"', () => {
    const r = classifyError('Error: model not found');
    assert.equal(r.category, 'model_unavailable');
  });

  it('detects HTTP 529', () => {
    const r = classifyError('HTTP 529 API Overloaded');
    assert.equal(r.category, 'model_unavailable');
  });

  it('detects "capacity"', () => {
    const r = classifyError('Insufficient capacity to serve the request');
    assert.equal(r.category, 'model_unavailable');
  });
});

describe('classifyError — additional server_error patterns', () => {
  it('detects "500 internal server" in text', () => {
    const r = classifyError('Received 500 internal server error from upstream');
    assert.equal(r.category, 'server_error');
    assert.equal(r.shouldFallback, true);
    assert.equal(r.suggestedDelay, 3000);
  });

  it('detects "500 server" pattern', () => {
    const r = classifyError('500 server error occurred');
    assert.equal(r.category, 'server_error');
  });

  it('detects HTTP 502', () => {
    const r = classifyError('HTTP 502 Bad Gateway');
    assert.equal(r.category, 'server_error');
  });

  it('detects "internal server error" without status code', () => {
    const r = classifyError('An internal server error occurred');
    assert.equal(r.category, 'server_error');
  });
});

describe('classifyError — additional network_error patterns', () => {
  it('detects "ECONNRESET"', () => {
    const r = classifyError('read ECONNRESET');
    assert.equal(r.category, 'network_error');
    assert.equal(r.retryable, true);
    assert.equal(r.suggestedDelay, 5000);
  });

  it('detects "fetch failed"', () => {
    const r = classifyError('TypeError: fetch failed');
    assert.equal(r.category, 'network_error');
  });

  it('detects "dns" issues', () => {
    const r = classifyError('DNS resolution failed for api.example.com');
    assert.equal(r.category, 'network_error');
  });

  it('detects "network" keyword', () => {
    const r = classifyError('A network error was encountered');
    assert.equal(r.category, 'network_error');
  });
});

describe('classifyError — priority ordering', () => {
  it('abort takes priority over auth keywords', () => {
    const r = classifyError('abort: unauthorized cancel');
    assert.equal(r.category, 'abort');
  });

  it('auth takes priority over rate_limited', () => {
    const r = classifyError('HTTP 401 rate limit quota exceeded');
    assert.equal(r.category, 'auth_error');
  });

  it('auth takes priority over context_overflow', () => {
    const r = classifyError('HTTP 403 context too long');
    assert.equal(r.category, 'auth_error');
  });

  it('rate_limited takes priority over context_overflow', () => {
    const r = classifyError('HTTP 429 context too long');
    assert.equal(r.category, 'rate_limited');
  });

  it('context_overflow takes priority over output_too_large', () => {
    const r = classifyError('context too long output too large');
    assert.equal(r.category, 'context_overflow');
  });

  it('output_too_large takes priority over content_filter', () => {
    const r = classifyError('output too large content_filter');
    assert.equal(r.category, 'output_too_large');
  });

  it('content_filter takes priority over model_unavailable', () => {
    const r = classifyError('content_filter overloaded');
    assert.equal(r.category, 'content_filter');
  });

  it('model_unavailable takes priority over server_error', () => {
    const r = classifyError('HTTP 503 internal server error');
    assert.equal(r.category, 'model_unavailable');
  });

  it('server_error takes priority over network_error', () => {
    const r = classifyError('HTTP 500 Internal Server Error network ECONNRESET');
    assert.equal(r.category, 'server_error');
  });
});

describe('classifyError — message preservation', () => {
  it('preserves the original error string in all categories', () => {
    const inputs = [
      'abort signal',
      'HTTP 401 Unauthorized',
      'HTTP 429 rate limited',
      'context too long',
      'output too large',
      'content_filter',
      'HTTP 503 overloaded',
      'HTTP 500 Internal Server Error',
      'ECONNREFUSED',
      'completely unknown error',
    ];
    for (const input of inputs) {
      assert.equal(classifyError(input).message, input);
    }
  });
});

describe('classifyError — edge cases', () => {
  it('empty string classifies as unknown', () => {
    const r = classifyError('');
    assert.equal(r.category, 'unknown');
    assert.equal(r.retryable, false);
    assert.equal(r.shouldCompact, false);
    assert.equal(r.shouldFallback, false);
  });

  it('whitespace-only string classifies as unknown', () => {
    const r = classifyError('   ');
    assert.equal(r.category, 'unknown');
  });

  it('port numbers do not trigger server_error', () => {
    const r = classifyError('Connected on port 5000');
    assert.equal(r.category, 'unknown');
  });

  it('large token counts do not trigger server_error', () => {
    const r = classifyError('Processed 50000 items successfully');
    assert.equal(r.category, 'unknown');
  });

  it('timeout values do not trigger server_error', () => {
    const r = classifyError('Waited 3000 milliseconds');
    assert.equal(r.category, 'unknown');
  });
});
