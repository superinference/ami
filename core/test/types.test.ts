import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { buildSubsystems, EXIT_REASONS, STREAM_CHUNK_TYPES, OUTPUT_STYLES, ENGINE_EVENT_TYPES, OUTPUT_FORMATS, INPUT_FORMATS, CHAT_MODES, THEMES } from '../src/types';
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

  it('passes architectModel through to provider subsystem', () => {
    const { provider } = buildSubsystems(cfg({ architectModel: 'claude-opus-4' }));
    assert.equal(provider.architectModel, 'claude-opus-4');
  });

  it('leaves architectModel undefined when not provided', () => {
    const { provider } = buildSubsystems(cfg({}));
    assert.equal(provider.architectModel, undefined);
  });

  it('passes compactionModel through to provider subsystem', () => {
    const { provider } = buildSubsystems(cfg({ compactionModel: 'gpt-4o-mini' }));
    assert.equal(provider.compactionModel, 'gpt-4o-mini');
  });
});

describe('EXIT_REASONS', () => {
  it('contains all 15 exit reasons', () => {
    assert.equal(EXIT_REASONS.length, 15);
    for (const reason of ['completed', 'max_turns', 'error', 'abort', 'session_timeout',
      'max_steps', 'budget_exceeded', 'hook_denied', 'superinference_stop',
      'consecutive_errors', 'escalation_terminate', 'no_edit_stall',
      'keepalive_exhausted', 'context_too_small', 'usage_limit_watchdog'] as const) {
      assert.ok(EXIT_REASONS.includes(reason), `missing exit reason: ${reason}`);
    }
  });
});

describe('STREAM_CHUNK_TYPES', () => {
  it('contains all 5 stream chunk types', () => {
    assert.equal(STREAM_CHUNK_TYPES.length, 5);
    for (const type of ['content_delta', 'tool_call_delta', 'thinking_delta', 'done', 'error'] as const) {
      assert.ok(STREAM_CHUNK_TYPES.includes(type), `missing chunk type: ${type}`);
    }
  });
});

describe('OUTPUT_STYLES', () => {
  it('contains all 3 output styles', () => {
    assert.equal(OUTPUT_STYLES.length, 3);
    assert.ok(OUTPUT_STYLES.includes('default'));
    assert.ok(OUTPUT_STYLES.includes('concise'));
    assert.ok(OUTPUT_STYLES.includes('explanatory'));
  });
});

describe('runtime const arrays are non-empty', () => {
  it('ENGINE_EVENT_TYPES has 22+ events', () => {
    assert.ok(ENGINE_EVENT_TYPES.length >= 22);
  });
  it('OUTPUT_FORMATS has 3 values', () => {
    assert.equal(OUTPUT_FORMATS.length, 3);
  });
  it('INPUT_FORMATS has 2 values', () => {
    assert.equal(INPUT_FORMATS.length, 2);
  });
  it('CHAT_MODES has 3 values', () => {
    assert.equal(CHAT_MODES.length, 3);
  });
  it('THEMES has 3 values', () => {
    assert.equal(THEMES.length, 3);
  });
});
