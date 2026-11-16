// ===========================================================================
// model-max-effort-u051.test.ts
//
// Unit U051 — per-model 'max' reasoning-effort tier gating + max→high downgrade.
//
// Mirrors Claude Code's modelSupportsMaxEffort + resolveAppliedEffort
// (references/claude/claude-code-main/src/utils/effort.ts:L50-65, L162-165) and
// the ModelPicker displayEffort clamp (components/ModelPicker.tsx:L147-170):
// the top 'max' effort tier is Opus-4.6+-class only; the API rejects
// effort:'max' on every other model (e.g. sonnet-4-6), so it is downgraded to
// 'high' before the parameter is ever sent.
//
// Gaps covered (all suggestedAmiTarget core/core/src/model-capabilities.ts):
//  - utils/effort.ts "per model max effort support gate max high downgrade clamp"
//  - utils/effort.ts "downgrade max effort to high when the model does not support"
//  - utils/effort.ts "per model max effort gating with max high downgrade"
//  - components/ModelPicker.tsx "per model max reasoning effort tier gating"
//  - components/ModelPicker.tsx "per model max effort gating with downgrade"
//
// The buildThinkingOptions cases are the red-green discriminator: before the
// wiring in provider.ts, sonnet-4-6 + level 'max' sent effort:'max'; after, it
// sends effort:'high'.
// ===========================================================================
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  modelSupportsMaxEffort,
  resolveMaxEffortLevel,
} from '../src/model-capabilities';
import { buildThinkingOptions } from '../src/provider';

const THIRD_PARTY_ENV = [
  'OPENAI_BASE_URL',
  'ANTHROPIC_BASE_URL',
  'ANTHROPIC_DEFAULT_OPUS_MODEL',
  'ANTHROPIC_DEFAULT_OPUS_MODEL_SUPPORTED_CAPABILITIES',
  'ANTHROPIC_DEFAULT_SONNET_MODEL',
  'ANTHROPIC_DEFAULT_SONNET_MODEL_SUPPORTED_CAPABILITIES',
  'ANTHROPIC_DEFAULT_HAIKU_MODEL',
  'ANTHROPIC_DEFAULT_HAIKU_MODEL_SUPPORTED_CAPABILITIES',
];

afterEach(() => {
  for (const key of THIRD_PARTY_ENV) delete process.env[key];
});

// Read the anthropic adaptive effort out of buildThinkingOptions' nested shape.
function anthropicEffort(opts: Record<string, unknown>): unknown {
  const providerOptions = opts.providerOptions as Record<string, unknown> | undefined;
  const anthropic = providerOptions?.anthropic as Record<string, unknown> | undefined;
  return anthropic?.effort;
}

describe('modelSupportsMaxEffort — per-model max-effort gate (U051)', () => {
  it('is true for the Opus 4.6+ class (and the 5.x line)', () => {
    assert.equal(modelSupportsMaxEffort('claude-opus-4-6'), true);
    assert.equal(modelSupportsMaxEffort('claude-opus-4-6-20251101'), true);
    assert.equal(modelSupportsMaxEffort('claude-opus-4-7'), true);
    assert.equal(modelSupportsMaxEffort('claude-opus-4-8'), true);
    assert.equal(modelSupportsMaxEffort('claude-opus-5'), true);
    assert.equal(modelSupportsMaxEffort('claude-opus-5-5'), true);
    // unanchored — gateway prefixes still resolve the family/version
    assert.equal(modelSupportsMaxEffort('openrouter/anthropic/claude-opus-4-6'), true);
  });

  it('is false for every non-Opus-4.6 model (API rejects effort:max there)', () => {
    assert.equal(modelSupportsMaxEffort('claude-opus-4-5'), false);
    assert.equal(modelSupportsMaxEffort('claude-opus-4-1'), false);
    assert.equal(modelSupportsMaxEffort('claude-opus-4'), false);
    assert.equal(modelSupportsMaxEffort('claude-sonnet-4-6'), false);
    assert.equal(modelSupportsMaxEffort('claude-sonnet-5'), false);
    assert.equal(modelSupportsMaxEffort('claude-haiku-4-5'), false);
    assert.equal(modelSupportsMaxEffort('gpt-6'), false);
    assert.equal(modelSupportsMaxEffort('o3'), false);
  });

  it('returns false for an empty / non-string id', () => {
    assert.equal(modelSupportsMaxEffort(''), false);
    assert.equal(modelSupportsMaxEffort(undefined as unknown as string), false);
    assert.equal(modelSupportsMaxEffort(null as unknown as string), false);
  });

  it('honors a 3P max_effort capability override on a gateway endpoint', () => {
    // A custom base URL makes this a non-first-party (gateway) endpoint, so the
    // pinned-tier capability override applies.
    process.env.OPENAI_BASE_URL = 'https://gateway.example/v1';
    process.env.ANTHROPIC_DEFAULT_SONNET_MODEL = 'gw-reasoner-x';
    process.env.ANTHROPIC_DEFAULT_SONNET_MODEL_SUPPORTED_CAPABILITIES = 'effort,max_effort';
    assert.equal(modelSupportsMaxEffort('gw-reasoner-x'), true);
  });

  it('3P override of false overrides Opus auto-detection (declared no max_effort)', () => {
    process.env.ANTHROPIC_BASE_URL = 'https://gateway.example/v1';
    process.env.ANTHROPIC_DEFAULT_OPUS_MODEL = 'claude-opus-4-6';
    // capability list present but WITHOUT max_effort → explicit false, wins over
    // the opus-4-6 auto-detection.
    process.env.ANTHROPIC_DEFAULT_OPUS_MODEL_SUPPORTED_CAPABILITIES = 'effort,thinking';
    assert.equal(modelSupportsMaxEffort('claude-opus-4-6'), false);
  });

  it('ignores the override on a first-party endpoint (no custom base URL)', () => {
    // No *_BASE_URL set → first-party → override ignored, auto-detection wins.
    process.env.ANTHROPIC_DEFAULT_SONNET_MODEL = 'claude-sonnet-4-6';
    process.env.ANTHROPIC_DEFAULT_SONNET_MODEL_SUPPORTED_CAPABILITIES = 'max_effort';
    assert.equal(modelSupportsMaxEffort('claude-sonnet-4-6'), false);
  });
});

describe('resolveMaxEffortLevel — max→high downgrade (U051)', () => {
  it('downgrades max→high on a model without max-effort support', () => {
    assert.equal(resolveMaxEffortLevel('max', 'claude-sonnet-4-6'), 'high');
    assert.equal(resolveMaxEffortLevel('max', 'claude-opus-4-5'), 'high');
    assert.equal(resolveMaxEffortLevel('max', 'gpt-6'), 'high');
  });

  it('keeps max on a max-effort-capable model', () => {
    assert.equal(resolveMaxEffortLevel('max', 'claude-opus-4-6'), 'max');
    assert.equal(resolveMaxEffortLevel('max', 'claude-opus-5'), 'max');
  });

  it('passes every non-max level through unchanged', () => {
    for (const level of ['off', 'low', 'medium', 'high'] as const) {
      assert.equal(resolveMaxEffortLevel(level, 'claude-sonnet-4-6'), level);
      assert.equal(resolveMaxEffortLevel(level, 'claude-opus-4-6'), level);
    }
  });
});

describe('buildThinkingOptions — end-to-end per-model max downgrade (U051)', () => {
  it('downgrades max→high effort for an adaptive model without max support', () => {
    // sonnet-4-6 is adaptive-thinking-capable but NOT max-effort-capable: the
    // API would 400 on effort:max, so the sent effort must be 'high'.
    const opts = buildThinkingOptions('claude-sonnet-4-6', { enabled: true, level: 'max' });
    assert.equal(anthropicEffort(opts), 'high');
  });

  it('sends max effort unchanged for a max-effort-capable model', () => {
    const opts = buildThinkingOptions('claude-opus-4-6', { enabled: true, level: 'max' });
    assert.equal(anthropicEffort(opts), 'max');
  });

  it('leaves non-max levels untouched (regression lock)', () => {
    assert.equal(
      anthropicEffort(buildThinkingOptions('claude-sonnet-4-6', { enabled: true, level: 'high' })),
      'high',
    );
    assert.equal(
      anthropicEffort(buildThinkingOptions('claude-sonnet-4-6', { enabled: true, level: 'medium' })),
      'medium',
    );
    assert.equal(
      anthropicEffort(buildThinkingOptions('claude-opus-4-6', { enabled: true, level: 'low' })),
      'low',
    );
  });
});
