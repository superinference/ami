import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  ESSENTIAL_TRAFFIC_DENY_ON_MISS,
  isNonessentialFeatureAllowedOnMiss,
} from '../src/privacy-level';

const ON = { SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC: '1' } as NodeJS.ProcessEnv;
const CC_ON = { CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: '1' } as NodeJS.ProcessEnv;
const OFF = {} as NodeJS.ProcessEnv;

describe('ESSENTIAL_TRAFFIC_DENY_ON_MISS catalog', () => {
  it('lists the nonessential egress features that fail closed on a cache miss', () => {
    assert.ok(ESSENTIAL_TRAFFIC_DENY_ON_MISS.has('skill-catalog'));
    assert.ok(ESSENTIAL_TRAFFIC_DENY_ON_MISS.has('live-pricing'));
    assert.ok(ESSENTIAL_TRAFFIC_DENY_ON_MISS.has('model-capabilities'));
    assert.ok(ESSENTIAL_TRAFFIC_DENY_ON_MISS.has('auto-update'));
    assert.ok(ESSENTIAL_TRAFFIC_DENY_ON_MISS.has('product-feedback'));
  });

  it('is re-exported through ./analytics (single source of truth)', async () => {
    const analytics = await import('../src/analytics');
    assert.equal(analytics.ESSENTIAL_TRAFFIC_DENY_ON_MISS, ESSENTIAL_TRAFFIC_DENY_ON_MISS);
    assert.equal(analytics.isNonessentialFeatureAllowedOnMiss, isNonessentialFeatureAllowedOnMiss);
  });

  it('is re-exported through the package index', async () => {
    const index = await import('../src/index');
    assert.equal(index.ESSENTIAL_TRAFFIC_DENY_ON_MISS, ESSENTIAL_TRAFFIC_DENY_ON_MISS);
    assert.equal(index.isNonessentialFeatureAllowedOnMiss, isNonessentialFeatureAllowedOnMiss);
  });
});

describe('isNonessentialFeatureAllowedOnMiss', () => {
  it('fails OPEN for every feature when essential-traffic-only mode is off', () => {
    for (const feature of ESSENTIAL_TRAFFIC_DENY_ON_MISS) {
      assert.equal(isNonessentialFeatureAllowedOnMiss(feature, OFF), true, feature);
    }
  });

  it('fails CLOSED for deny-on-miss features under essential-traffic-only (SUPERINFERENCE var)', () => {
    for (const feature of ESSENTIAL_TRAFFIC_DENY_ON_MISS) {
      assert.equal(isNonessentialFeatureAllowedOnMiss(feature, ON), false, feature);
    }
  });

  it('fails CLOSED under the Claude Code-compatible env var too', () => {
    assert.equal(isNonessentialFeatureAllowedOnMiss('skill-catalog', CC_ON), false);
  });

  it('fails OPEN for a feature NOT in the deny-on-miss set even under essential-traffic-only', () => {
    // A feature outside the catalog keeps Claude's default fail-open behavior.
    assert.equal(isNonessentialFeatureAllowedOnMiss('core-inference', ON), true);
    assert.equal(isNonessentialFeatureAllowedOnMiss('unknown-feature', ON), true);
  });

  it('defaults to process.env when no env override is supplied', () => {
    const prev = process.env.SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC;
    try {
      delete process.env.SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC;
      assert.equal(isNonessentialFeatureAllowedOnMiss('skill-catalog'), true);
      process.env.SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC = '1';
      assert.equal(isNonessentialFeatureAllowedOnMiss('skill-catalog'), false);
    } finally {
      if (prev === undefined) delete process.env.SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC;
      else process.env.SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC = prev;
    }
  });
});
