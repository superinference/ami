/**
 * Shared classifier infrastructure (parity: the reference's
 * utils/permissions/classifierShared.ts — extractToolUseBlock + parseClassifierResponse).
 *
 * RED on revert: core/src/permissions/classifier-shared.ts does not exist, so the import
 * fails and every test errors.
 * Run from core/core: node --import tsx --test test/classifier-shared.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { z } from 'zod/v4';
import {
  extractToolCallBlock,
  parseClassifierResponse,
  extractAndParseClassifierResponse,
} from '../src/permissions/classifier-shared';
import type { ToolCallContent } from '../src/types';

// A representative classifier output schema — mirrors a semantic Bash-match / YOLO
// security decision the reference classifiers return.
const decisionSchema = z.object({
  decision: z.enum(['allow', 'deny', 'ask']),
  reason: z.string(),
});

function toolCall(name: string, args: unknown, id = 'tc_1'): ToolCallContent {
  return {
    id,
    type: 'function',
    function: { name, arguments: typeof args === 'string' ? args : JSON.stringify(args) },
  };
}

describe('extractToolCallBlock', () => {
  it('returns the first tool call matching the name', () => {
    const calls = [
      toolCall('other', { x: 1 }, 'a'),
      toolCall('classify', { decision: 'allow', reason: 'ok' }, 'b'),
      toolCall('classify', { decision: 'deny', reason: 'no' }, 'c'),
    ];
    const block = extractToolCallBlock(calls, 'classify');
    assert.equal(block?.id, 'b');
  });

  it('returns null when no tool call has the name', () => {
    const calls = [toolCall('other', { x: 1 })];
    assert.equal(extractToolCallBlock(calls, 'classify'), null);
  });

  it('returns null for an empty tool-call list', () => {
    assert.equal(extractToolCallBlock([], 'classify'), null);
  });
});

describe('parseClassifierResponse', () => {
  it('returns typed data when arguments satisfy the schema', () => {
    const block = toolCall('classify', { decision: 'deny', reason: 'rm -rf' });
    const parsed = parseClassifierResponse(block, decisionSchema);
    assert.deepEqual(parsed, { decision: 'deny', reason: 'rm -rf' });
  });

  it('treats empty-string arguments as {} (fails the schema here → null)', () => {
    const block = toolCall('classify', '');
    assert.equal(parseClassifierResponse(block, decisionSchema), null);
  });

  it('accepts empty-string arguments when the schema permits an empty object', () => {
    const block = toolCall('noop', '');
    const parsed = parseClassifierResponse(block, z.object({}).loose());
    assert.deepEqual(parsed, {});
  });

  it('returns null on malformed (non-JSON) arguments', () => {
    const block = toolCall('classify', '{not valid json');
    assert.equal(parseClassifierResponse(block, decisionSchema), null);
  });

  it('returns null when arguments are valid JSON but miss the schema', () => {
    const block = toolCall('classify', { decision: 'maybe', reason: 'x' });
    assert.equal(parseClassifierResponse(block, decisionSchema), null);
  });

  it('returns null when a required field is absent', () => {
    const block = toolCall('classify', { decision: 'allow' });
    assert.equal(parseClassifierResponse(block, decisionSchema), null);
  });
});

describe('extractAndParseClassifierResponse (end-to-end classifier parse)', () => {
  it('extracts the named tool call and validates it in one step', () => {
    // Simulates the realistic consumer flow: a provider turn returns several tool
    // calls; the classifier pulls its own by name and trusts only schema-valid output.
    const providerToolCalls = [
      toolCall('thinking', { note: 'ignore me' }, 'a'),
      toolCall('bash_classify', { decision: 'ask', reason: 'ambiguous deletion' }, 'b'),
    ];
    const result = extractAndParseClassifierResponse(
      providerToolCalls,
      'bash_classify',
      decisionSchema,
    );
    assert.deepEqual(result, { decision: 'ask', reason: 'ambiguous deletion' });
  });

  it('returns null when the classifier tool call is absent', () => {
    const result = extractAndParseClassifierResponse(
      [toolCall('other', { x: 1 })],
      'bash_classify',
      decisionSchema,
    );
    assert.equal(result, null);
  });

  it('returns null when the present tool call fails validation', () => {
    const result = extractAndParseClassifierResponse(
      [toolCall('bash_classify', { decision: 'nope', reason: 1 })],
      'bash_classify',
      decisionSchema,
    );
    assert.equal(result, null);
  });

  it('returns null when the present tool call has malformed JSON arguments', () => {
    const result = extractAndParseClassifierResponse(
      [toolCall('bash_classify', 'definitely-not-json')],
      'bash_classify',
      decisionSchema,
    );
    assert.equal(result, null);
  });
});
