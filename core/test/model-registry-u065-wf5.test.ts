/**
 * U065 (wf5) — model-registry additive parity:
 *  1. detectProvider — explicit CLAUDE_CODE_USE_FOUNDRY / SUPERINFERENCE_USE_FOUNDRY
 *     opt-in toggle routing to the azure-openai (Foundry) provider, independent of
 *     credential sniffing (parity: the reference getAPIProvider, utils/model/providers.ts).
 *  2. resolveOverriddenModel — reverse of applyModelOverride: map a provider-specific
 *     override string (e.g. a Bedrock inference-profile ARN) back to its canonical id
 *     (parity: the reference's resolveOverriddenModel, utils/model/modelStrings.ts:L78-L100),
 *     and the display helpers (getModelDisplayName / getModelMetadata) honoring it so
 *     an opaque override surfaces its clean family name.
 *  3. MODEL_ENV_TOGGLES — introspection surface for the new Foundry env knobs.
 *
 * Run from core/core: node --import tsx --test test/model-registry-u065-wf5.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  detectProvider,
  resolveOverriddenModel,
  getModelDisplayName,
  getModelMetadata,
  MODEL_ENV_TOGGLES,
} from '../src/model-registry';
import type { ProviderConfig } from '../src/types';

const ENV_KEYS = [
  'CLAUDE_CODE_USE_FOUNDRY',
  'SUPERINFERENCE_USE_FOUNDRY',
  'CLAUDE_CODE_USE_BEDROCK',
  'SUPERINFERENCE_USE_BEDROCK',
  'CLAUDE_CODE_USE_VERTEX',
  'ANTHROPIC_VERTEX_PROJECT_ID',
];
// Snapshot + clear any ambient routing vars so a developer/CI env with a Vertex
// or Bedrock toggle exported cannot shadow the Foundry assertions; restore after.
const savedEnv: Array<[string, string | undefined]> = [];
beforeEach(() => {
  for (const k of ENV_KEYS) { savedEnv.push([k, process.env[k]]); delete process.env[k]; }
});
afterEach(() => {
  for (const [k, v] of savedEnv.splice(0)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
});

describe('detectProvider — explicit Foundry opt-in toggle', () => {
  it('routes to azure-openai from CLAUDE_CODE_USE_FOUNDRY regardless of credentials', () => {
    process.env.CLAUDE_CODE_USE_FOUNDRY = '1';
    // An OpenAI-looking key would normally be sniffed as openai; the toggle wins.
    assert.equal(detectProvider({ apiKey: 'sk-openai', baseUrl: '' } as ProviderConfig), 'azure-openai');
  });
  it('routes to azure-openai from the SUPERINFERENCE_USE_FOUNDRY alias', () => {
    process.env.SUPERINFERENCE_USE_FOUNDRY = 'true';
    assert.equal(detectProvider({ apiKey: '', baseUrl: '' } as ProviderConfig), 'azure-openai');
  });
  it('does NOT route to foundry without the toggle (credential sniffing stands)', () => {
    assert.equal(detectProvider({ apiKey: 'sk-ant-xyz', baseUrl: '' } as ProviderConfig), 'anthropic');
    assert.equal(detectProvider({ apiKey: 'sk-openai', baseUrl: '' } as ProviderConfig), 'openai');
  });
  it('an explicit config.provider still wins over the toggle', () => {
    process.env.CLAUDE_CODE_USE_FOUNDRY = '1';
    assert.equal(detectProvider({ provider: 'anthropic', apiKey: '', baseUrl: '' } as ProviderConfig), 'anthropic');
  });
  it('the bedrock toggle still wins ordering-wise (foundry does not shadow it)', () => {
    process.env.CLAUDE_CODE_USE_BEDROCK = '1';
    process.env.CLAUDE_CODE_USE_FOUNDRY = '1';
    assert.equal(detectProvider({ apiKey: '', baseUrl: '' } as ProviderConfig), 'amazon-bedrock');
  });
});

describe('resolveOverriddenModel — reverse of applyModelOverride', () => {
  // An OPAQUE deployment id that carries no recognizable family substring, so the
  // reverse map is the ONLY way to recover the canonical id (discriminating).
  const OPAQUE = 'deploy-alpha-001';
  const overrides = { 'claude-opus-4-6': OPAQUE };

  it('maps an override value back to its canonical id', () => {
    assert.equal(resolveOverriddenModel(OPAQUE, overrides), 'claude-opus-4-6');
  });
  it('returns the input unchanged when no value matches', () => {
    assert.equal(resolveOverriddenModel('claude-sonnet-4-6', overrides), 'claude-sonnet-4-6');
  });
  it('returns the input unchanged when there is no override map', () => {
    assert.equal(resolveOverriddenModel(OPAQUE, undefined), OPAQUE);
  });
  it('returns the input unchanged for an empty override map', () => {
    assert.equal(resolveOverriddenModel(OPAQUE, {}), OPAQUE);
  });
});

describe('display helpers honor modelOverrides reverse resolution', () => {
  // Opaque override target → no family substring, so the raw string has no friendly
  // name UNTIL the reverse map recovers the canonical id.
  const OPAQUE = 'deploy-beta-002';
  const overrides = { 'claude-sonnet-4-6': OPAQUE };

  it('getModelDisplayName resolves an opaque override to its clean family name with overrides', () => {
    // Without the overrides map the raw opaque id is echoed back (no pattern match).
    assert.equal(getModelDisplayName(OPAQUE), OPAQUE);
    assert.equal(getModelDisplayName(OPAQUE, overrides), 'Sonnet 4.6');
  });
  it('getModelMetadata resolves description + displayName through overrides', () => {
    const meta = getModelMetadata(OPAQUE, overrides);
    assert.equal(meta.displayName, 'Sonnet 4.6');
    assert.ok(meta.description && /Balanced/.test(meta.description));
    // Without the map, no friendly label and no canned description.
    const bare = getModelMetadata(OPAQUE);
    assert.equal(bare.displayName, OPAQUE);
    assert.equal(bare.description, undefined);
  });
  it('getModelMetadata leaves a non-override id untouched', () => {
    const meta = getModelMetadata('claude-opus-4-6', overrides);
    assert.equal(meta.displayName, 'Opus 4.6');
  });
});

describe('MODEL_ENV_TOGGLES — Foundry introspection surface', () => {
  it('advertises both Foundry routing toggles', () => {
    const envs = MODEL_ENV_TOGGLES.map(t => t.env);
    assert.ok(envs.includes('CLAUDE_CODE_USE_FOUNDRY'));
    assert.ok(envs.includes('SUPERINFERENCE_USE_FOUNDRY'));
    for (const t of MODEL_ENV_TOGGLES) {
      assert.ok(t.env && t.affects && t.description, 'each toggle carries env/affects/description');
    }
  });
});
