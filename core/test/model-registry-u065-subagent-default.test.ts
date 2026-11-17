/**
 * U065 — provider-aware default subagent/teammate model.
 *
 * Gap: Claude Code's getHardcodedTeammateModelFallback (utils/swarm/teammateModel.ts:L1-L10)
 * returns a PROVIDER-CORRECT model id for a teammate the user never configured
 * (firstParty/bedrock/vertex/foundry). AMI's task tool, when NO model is resolvable
 * from any source, previously could dispatch an empty model id. These tests pin the
 * new provider-aware default:
 *  - getDefaultSubagentModel(config) → fast canonical default on first-party,
 *  - routed through the SINGLE canonical Bedrock resolver (resolveBedrockModelId) on
 *    Amazon Bedrock so a declared inference profile / cross-region prefix wins,
 *  - defaultSubagentModelFallback keeps a non-empty model (AMI's inherit perk) and
 *    only substitutes the default for the genuinely-unset case.
 *
 * Run: cd core/core && node --import tsx --test test/model-registry-u065-subagent-default.test.ts
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import { getDefaultSubagentModel } from '../src/model-registry';
import { defaultSubagentModelFallback } from '../src/tools/task';
import { BEDROCK_INFERENCE_PROFILES_ENV, BEDROCK_REGION_PREFIX_ENV } from '../src/bedrock';
import type { ProviderConfig } from '../src/types';

const base: ProviderConfig = { baseUrl: '', apiKey: '', model: '' };

afterEach(() => {
  delete process.env[BEDROCK_INFERENCE_PROFILES_ENV];
  delete process.env[BEDROCK_REGION_PREFIX_ENV];
});

describe('getDefaultSubagentModel — provider-aware fast default (U065)', () => {
  it('returns the canonical fast default on a first-party provider', () => {
    assert.equal(
      getDefaultSubagentModel({ ...base, provider: 'anthropic' }),
      'claude-haiku-4-5',
    );
  });

  it('returns the canonical fast default on vertex / foundry (central send-path remap, no per-provider table)', () => {
    assert.equal(
      getDefaultSubagentModel({ ...base, provider: 'anthropic-vertex' }),
      'claude-haiku-4-5',
    );
    assert.equal(
      getDefaultSubagentModel({ ...base, provider: 'azure-openai' }),
      'claude-haiku-4-5',
    );
  });

  it('resolves the default to a declared Bedrock inference profile when provisioned', () => {
    process.env[BEDROCK_INFERENCE_PROFILES_ENV] =
      'eu.anthropic.claude-haiku-4-5-20251001-v1:0, us.anthropic.claude-opus-4-6-v1:0';
    assert.equal(
      getDefaultSubagentModel({ ...base, provider: 'amazon-bedrock' }),
      'eu.anthropic.claude-haiku-4-5-20251001-v1:0',
    );
  });

  it('falls back to the canonical id on Bedrock when no profile matches', () => {
    // No profiles, no region prefix → the bare canonical id (which the provider
    // send-path will still attempt to resolve) rather than a mangled id.
    assert.equal(
      getDefaultSubagentModel({ ...base, provider: 'amazon-bedrock' }),
      'claude-haiku-4-5',
    );
  });
});

describe('defaultSubagentModelFallback — additive, keeps inherit perk (U065)', () => {
  it('passes a configured/inherited model through untouched', () => {
    assert.equal(
      defaultSubagentModelFallback('claude-opus-4-6', { ...base, provider: 'anthropic' }),
      'claude-opus-4-6',
    );
  });

  it('substitutes the provider-aware default when the model is empty', () => {
    assert.equal(
      defaultSubagentModelFallback('', { ...base, provider: 'anthropic' }),
      'claude-haiku-4-5',
    );
  });

  it('substitutes the default when the model is undefined or whitespace', () => {
    assert.equal(
      defaultSubagentModelFallback(undefined, { ...base, provider: 'anthropic' }),
      'claude-haiku-4-5',
    );
    assert.equal(
      defaultSubagentModelFallback('   ', { ...base, provider: 'anthropic' }),
      'claude-haiku-4-5',
    );
  });

  it('substitutes a Bedrock-provisioned profile when empty on a Bedrock run', () => {
    process.env[BEDROCK_INFERENCE_PROFILES_ENV] = 'us.anthropic.claude-haiku-4-5-20251001-v1:0';
    assert.equal(
      defaultSubagentModelFallback('', { ...base, provider: 'amazon-bedrock' }),
      'us.anthropic.claude-haiku-4-5-20251001-v1:0',
    );
  });
});
