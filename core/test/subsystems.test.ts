import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { buildSubsystems } from '../src/types';
import type { EngineConfig, ProviderConfig } from '../src/types';
import { ProviderCoordinator } from '../src/provider-coordinator';

function minimalConfig(overrides?: Partial<EngineConfig>): EngineConfig {
  const provider: ProviderConfig = {
    baseUrl: 'https://api.example.com',
    apiKey: 'test-key',
    model: 'test-model',
  };
  return {
    provider,
    cwd: '/tmp',
    ...overrides,
  };
}

describe('buildSubsystems', () => {
  it('extracts provider subsystem from config', () => {
    const config = minimalConfig({
      fallbackModel: 'fallback-1',
      compactionModel: 'compact-1',
    });
    const subs = buildSubsystems(config);

    assert.equal(subs.provider.primary.model, 'test-model');
    assert.equal(subs.provider.fallbackModel, 'fallback-1');
    assert.equal(subs.provider.compactionModel, 'compact-1');
  });

  it('extracts permission subsystem with defaults', () => {
    const subs = buildSubsystems(minimalConfig());
    assert.equal(subs.permissions.mode, 'ask');
    assert.deepEqual(subs.permissions.rules, []);
    assert.equal(subs.permissions.promptHandler, undefined);
  });

  it('extracts permission subsystem with overrides', () => {
    const config = minimalConfig({
      permissionMode: 'auto-allow',
      permissionRules: [{ tool: 'bash', action: 'allow' }],
    });
    const subs = buildSubsystems(config);
    assert.equal(subs.permissions.mode, 'auto-allow');
    assert.equal(subs.permissions.rules.length, 1);
  });

  it('extracts session subsystem with defaults', () => {
    const subs = buildSubsystems(minimalConfig());
    assert.equal(subs.session.tokenBudget, 100_000);
    assert.equal(subs.session.maxTurns, 100);
    assert.equal(subs.session.sessionId, undefined);
  });

  it('extracts session subsystem with overrides', () => {
    const config = minimalConfig({
      tokenBudget: 50_000,
      maxTurns: 20,
      sessionId: 'sess-123',
      sessionDir: '/tmp/sessions',
      maxSteps: 10,
    });
    const subs = buildSubsystems(config);
    assert.equal(subs.session.tokenBudget, 50_000);
    assert.equal(subs.session.maxTurns, 20);
    assert.equal(subs.session.sessionId, 'sess-123');
    assert.equal(subs.session.sessionDir, '/tmp/sessions');
    assert.equal(subs.session.maxSteps, 10);
  });

  it('includes thinking config in provider subsystem', () => {
    const config = minimalConfig({
      thinking: { enabled: true, level: 'high' },
    });
    const subs = buildSubsystems(config);
    assert.equal(subs.provider.thinking?.enabled, true);
    assert.equal(subs.provider.thinking?.level, 'high');
  });

  it('returns independent objects from config', () => {
    const config = minimalConfig();
    const subs1 = buildSubsystems(config);
    const subs2 = buildSubsystems(config);
    assert.notEqual(subs1.provider, subs2.provider);
    assert.notEqual(subs1.permissions, subs2.permissions);
    assert.notEqual(subs1.session, subs2.session);
  });

  it('provider subsystem is accepted by ProviderCoordinator', () => {
    const config = minimalConfig({ fallbackModel: 'fallback-model' });
    const subs = buildSubsystems(config);
    const coordinator = new ProviderCoordinator(subs.provider);
    const providerConfig = coordinator.getConfig();
    assert.equal(providerConfig.model, 'test-model');
    assert.equal(providerConfig.apiKey, 'test-key');
  });

  it('fallbackModel propagates through coordinator', () => {
    const config = minimalConfig({ fallbackModel: 'fallback-model' });
    const subs = buildSubsystems(config);
    const coordinator = new ProviderCoordinator(subs.provider);
    coordinator.recordError();
    coordinator.recordError();
    coordinator.recordError();
    const providerConfig = coordinator.getConfig();
    assert.equal(providerConfig.model, 'fallback-model');
  });
});
