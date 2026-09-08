import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { buildSubsystems } from '../src/types';
import type { EngineConfig } from '../src/types';

function cfg(overrides: Partial<EngineConfig> = {}): EngineConfig {
  return {
    provider: { apiKey: 'k', baseUrl: 'http://localhost', model: 'm' },
    cwd: '/tmp',
    ...overrides,
  };
}

describe('buildSubsystems', () => {
  it('maps maxTurns 0 to the finite default instead of Infinity', () => {
    const { session } = buildSubsystems(cfg({ maxTurns: 0 }));
    assert.equal(session.maxTurns, 100);
    assert.ok(Number.isFinite(session.maxTurns));
  });

  it('maps non-finite maxTurns to the finite default', () => {
    const { session } = buildSubsystems(cfg({ maxTurns: Number.POSITIVE_INFINITY }));
    assert.equal(session.maxTurns, 100);
  });

  it('preserves a positive maxTurns', () => {
    const { session } = buildSubsystems(cfg({ maxTurns: 42 }));
    assert.equal(session.maxTurns, 42);
  });
});
