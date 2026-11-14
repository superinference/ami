import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  getBedrockInferenceProfiles,
  resolveBedrockModelId,
  BEDROCK_INFERENCE_PROFILES_ENV,
} from '../src/bedrock';

describe('getBedrockInferenceProfiles', () => {
  it('returns [] when the env var is unset', () => {
    assert.deepEqual(getBedrockInferenceProfiles({} as NodeJS.ProcessEnv), []);
  });

  it('comma-splits, trims, and drops empties', () => {
    const env = {
      [BEDROCK_INFERENCE_PROFILES_ENV]: ' eu.anthropic.claude-opus-4-6-v1:0 , , us.anthropic.claude-sonnet-4-5-v1:0 ,',
    } as unknown as NodeJS.ProcessEnv;
    assert.deepEqual(getBedrockInferenceProfiles(env), [
      'eu.anthropic.claude-opus-4-6-v1:0',
      'us.anthropic.claude-sonnet-4-5-v1:0',
    ]);
  });
});

describe('resolveBedrockModelId', () => {
  const profiles = [
    'eu.anthropic.claude-opus-4-6-v1:0',
    'us.anthropic.claude-sonnet-4-5-v1:0',
  ];

  it('maps a canonical id to the region-scoped profile that contains it', () => {
    assert.equal(resolveBedrockModelId('claude-opus-4-6', profiles), 'eu.anthropic.claude-opus-4-6-v1:0');
    assert.equal(resolveBedrockModelId('claude-sonnet-4-5', profiles), 'us.anthropic.claude-sonnet-4-5-v1:0');
  });

  it('is case-insensitive', () => {
    assert.equal(resolveBedrockModelId('CLAUDE-OPUS-4-6', profiles), 'eu.anthropic.claude-opus-4-6-v1:0');
  });

  it('falls back to the canonical id when no profile matches', () => {
    assert.equal(resolveBedrockModelId('claude-haiku-4-5', profiles), 'claude-haiku-4-5');
  });

  it('falls back when there are no profiles', () => {
    assert.equal(resolveBedrockModelId('claude-opus-4-6', []), 'claude-opus-4-6');
  });

  it('returns an empty canonical id unchanged', () => {
    assert.equal(resolveBedrockModelId('', profiles), '');
  });
});
