import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { ProviderCoordinator } from '../src/provider-coordinator';
import type { ProviderConfig, ProviderSubsystem } from '../src/types';

function makeSubsystem(overrides?: Partial<ProviderSubsystem>): ProviderSubsystem {
  const primary: ProviderConfig = {
    baseUrl: 'https://api.openai.com/v1',
    apiKey: 'sk-test-key',
    model: 'gpt-4o',
  };
  return { primary, ...overrides };
}

describe('ProviderCoordinator', () => {
  it('returns primary config by default', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    const config = coord.getConfig();
    assert.equal(config.model, 'gpt-4o');
  });

  it('tracks request count', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    coord.recordRequest();
    coord.recordRequest();
    assert.equal(coord.getState().requestCount, 2);
  });

  it('switches to fallback on error', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      fallbackModel: 'gpt-3.5-turbo',
    }));

    const switched = coord.recordError();
    assert.equal(switched, true);
    assert.equal(coord.getConfig().model, 'gpt-3.5-turbo');
    assert.equal(coord.getState().usedFallback, true);
  });

  it('returns false when no fallback available', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    const switched = coord.recordError();
    assert.equal(switched, false);
  });

  it('recovers to primary after cooldown period on fallback', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      fallbackModel: 'gpt-3.5-turbo',
    }));

    coord.recordError();
    assert.equal(coord.getConfig().model, 'gpt-3.5-turbo');

    // First two successes keep us on fallback (cooldown = 3)
    coord.recordSuccess();
    assert.equal(coord.getConfig().model, 'gpt-3.5-turbo');
    coord.recordSuccess();
    assert.equal(coord.getConfig().model, 'gpt-3.5-turbo');

    // Third success reverts to primary
    coord.recordSuccess();
    assert.equal(coord.getConfig().model, 'gpt-4o');
  });

  it('returns compaction config with compaction model', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      compactionModel: 'gpt-4o-mini',
    }));
    const config = coord.getCompactionConfig();
    assert.equal(config.model, 'gpt-4o-mini');
    assert.equal(config.apiKey, 'sk-test-key');
  });

  it('returns primary as compaction config when no compaction model', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    const config = coord.getCompactionConfig();
    assert.equal(config.model, 'gpt-4o');
  });

  it('detects Anthropic provider from model name', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      primary: {
        baseUrl: 'https://api.anthropic.com/v1',
        apiKey: 'sk-ant-test',
        model: 'claude-3-opus-20240229',
      },
    }));
    assert.equal(coord.isAnthropicProvider(), true);
  });

  it('detects non-Anthropic provider', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    assert.equal(coord.isAnthropicProvider(), false);
  });

  it('hasFallback returns true when configured', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      fallbackModel: 'backup',
    }));
    assert.equal(coord.hasFallback(), true);
  });

  it('hasFallback returns false when not configured', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    assert.equal(coord.hasFallback(), false);
  });

  it('tracks error count', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      fallbackModel: 'backup',
    }));
    coord.recordError();
    coord.recordError();
    assert.equal(coord.getState().errorCount, 2);
  });

  it('getState returns defensive copy', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    const s1 = coord.getState();
    s1.requestCount = 999;
    assert.equal(coord.getState().requestCount, 0);
  });

  it('initializes state with model info', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    const state = coord.getState();
    assert.equal(state.currentModel, 'gpt-4o');
    assert.equal(state.usedFallback, false);
    assert.equal(state.requestCount, 0);
    assert.equal(state.errorCount, 0);
  });

  it('handles fallback -> primary -> fallback re-entry cycle', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      fallbackModel: 'gpt-3.5-turbo',
    }));

    coord.recordError();
    assert.equal(coord.getConfig().model, 'gpt-3.5-turbo');

    coord.recordSuccess();
    coord.recordSuccess();
    coord.recordSuccess();
    assert.equal(coord.getConfig().model, 'gpt-4o');

    coord.recordError();
    assert.equal(coord.getConfig().model, 'gpt-3.5-turbo');
    assert.equal(coord.getState().errorCount, 2);
  });

  it('recordError returns false when already on fallback', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      fallbackModel: 'gpt-3.5-turbo',
    }));
    assert.equal(coord.recordError(), true);
    assert.equal(coord.recordError(), false);
  });

  it('tracks rate limits from response headers', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    coord.updateRateLimits({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '10',
      'x-ratelimit-limit-tokens': '100000',
      'x-ratelimit-remaining-tokens': '50000',
    });
    const status = coord.getRateLimitStatus();
    assert.equal(status.requestsPercent, 90);
    assert.equal(status.tokensPercent, 50);
    assert.equal(status.shouldWarn, true);
  });

  it('rate limit warns at 80% threshold', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    coord.updateRateLimits({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '25',
    });
    const status = coord.getRateLimitStatus();
    assert.equal(status.requestsPercent, 75);
    assert.equal(status.shouldWarn, false);
  });

  it('rotates credentials from pool when configured', () => {
    const coord = new ProviderCoordinator(makeSubsystem(), ['sk-extra-1', 'sk-extra-2']);
    const keys = new Set<string>();
    for (let i = 0; i < 3; i++) {
      keys.add(coord.getConfig().apiKey);
    }
    assert.ok(keys.size > 1, 'Expected multiple keys from pool rotation');
  });

  it('marks credential exhausted', () => {
    const coord = new ProviderCoordinator(makeSubsystem(), ['sk-extra-1']);
    coord.getConfig();
    coord.markCredentialExhausted();
    assert.equal(coord.availableCredentials, 1);
  });

  it('returns 1 available credential when no pool configured', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    assert.equal(coord.availableCredentials, 1);
  });
});
