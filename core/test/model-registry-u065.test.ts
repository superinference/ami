/**
 * U065 — model-registry human-readable metadata parity additions:
 *  - getModelDisplayName: friendly display name for a model id + raw-id fallback
 *    (marketing label for a raw id). Ref utils/model/model.ts:L344-L415,
 *    utils/model/modelOptions.ts:L431-L459.
 *  - getModelDescription: per-model picker description. Ref
 *    utils/model/modelOptions.ts:L96-L209, utils/model/antModels.ts:L4-L16.
 *  - getModelMetadata: combined { displayName, description } option annotation.
 *    Ref components/ModelPicker.tsx:L88-L124.
 *
 * Run: cd core/core && node --import tsx --test test/model-registry-u065.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  getModelDisplayName,
  getModelDescription,
  getModelMetadata,
} from '../src/model-registry';

describe('getModelDisplayName — friendly names (U065)', () => {
  it('maps versioned Claude ids to friendly names', () => {
    assert.equal(getModelDisplayName('claude-opus-4-6'), 'Opus 4.6');
    assert.equal(getModelDisplayName('claude-sonnet-4-5-20251022'), 'Sonnet 4.5');
    assert.equal(getModelDisplayName('claude-haiku-4-5-20251001'), 'Haiku 4.5');
    assert.equal(getModelDisplayName('claude-opus-4-1'), 'Opus 4.1');
    assert.equal(getModelDisplayName('claude-opus-4'), 'Opus 4');
  });

  it('recognizes legacy Claude 3.x ids', () => {
    assert.equal(getModelDisplayName('claude-3-opus-20240229'), 'Claude 3 Opus');
    assert.equal(getModelDisplayName('claude-3-7-sonnet'), 'Sonnet 3.7');
    assert.equal(getModelDisplayName('claude-3-5-haiku-latest'), 'Haiku 3.5');
  });

  it('matches through gateway prefixes (OpenRouter)', () => {
    assert.equal(getModelDisplayName('openrouter/anthropic/claude-sonnet-4.6'), 'Sonnet 4.6');
  });

  it('appends the 1M-context suffix label', () => {
    assert.equal(getModelDisplayName('claude-sonnet-4-6[1m]'), 'Sonnet 4.6 (1M context)');
    assert.equal(getModelDisplayName('claude-opus-4-6[1m]'), 'Opus 4.6 (1M context)');
  });

  it('labels bare family aliases', () => {
    assert.equal(getModelDisplayName('opus'), 'Opus');
    assert.equal(getModelDisplayName('sonnet'), 'Sonnet');
    assert.equal(getModelDisplayName('haiku'), 'Haiku');
  });

  it('labels OpenAI and Gemini ids', () => {
    assert.equal(getModelDisplayName('gpt-4o'), 'GPT-4o');
    assert.equal(getModelDisplayName('gpt-4o-mini'), 'GPT-4o mini');
    assert.equal(getModelDisplayName('gemini-2.5-pro'), 'Gemini 2.5 Pro');
    assert.equal(getModelDisplayName('gemini-3-pro-preview'), 'Gemini 3');
  });

  it('falls back to the raw id for unknown/custom models', () => {
    assert.equal(getModelDisplayName('my-private-finetune-v2'), 'my-private-finetune-v2');
    assert.equal(getModelDisplayName('  spaced-model  '), 'spaced-model');
  });

  it('returns an empty string for an empty/whitespace id', () => {
    assert.equal(getModelDisplayName(''), '');
    assert.equal(getModelDisplayName('   '), '');
  });
});

describe('getModelDescription — picker descriptions (U065)', () => {
  it('returns a family description for known models', () => {
    assert.match(getModelDescription('claude-opus-4-6') ?? '', /complex/i);
    assert.match(getModelDescription('claude-sonnet-4-6') ?? '', /everyday/i);
    assert.match(getModelDescription('claude-haiku-4-5') ?? '', /fast/i);
    assert.match(getModelDescription('gpt-4o') ?? '', /OpenAI/i);
    assert.match(getModelDescription('gemini-2.5-flash') ?? '', /Gemini/i);
  });

  it('returns undefined for unknown models', () => {
    assert.equal(getModelDescription('my-private-finetune-v2'), undefined);
    assert.equal(getModelDescription(''), undefined);
  });
});

describe('getModelMetadata — combined option annotation (U065)', () => {
  it('bundles a display name and description for a known model', () => {
    const meta = getModelMetadata('claude-opus-4-6');
    assert.equal(meta.displayName, 'Opus 4.6');
    assert.match(meta.description ?? '', /complex/i);
  });

  it('keeps the raw id and omits description for a custom model', () => {
    const meta = getModelMetadata('acme/custom-llm');
    assert.equal(meta.displayName, 'acme/custom-llm');
    assert.equal(meta.description, undefined);
  });
});
