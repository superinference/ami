/**
 * U006 — native `document` (PDF) content block parity.
 *
 * Covers the three building blocks that let a PDF enter the model's context as a
 * native `document` block (parity: Claude's Read-tool PDF path emitting
 * `{ type: 'document', source: { type:'base64', media_type:'application/pdf', data } }`):
 *   1. the {@link DocumentContent} type carried in a user message's content union,
 *   2. {@link modelSupportsDocuments} — the capability gate (distinct from vision), and
 *   3. {@link convertMessages} serializing a document block to an AI-SDK `file` part.
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import { convertMessages } from '../src/provider';
import {
  modelSupportsDocuments,
  modelSupportsVision,
  registerModelCapabilities,
  clearDynamicCapabilities,
} from '../src/model-capabilities';
import type { DocumentContent, Message } from '../src/types';

const PDF_B64 = 'JVBERi0xLjQKJ=='; // minimal base64-ish stand-in for PDF bytes

function doc(): DocumentContent {
  return { type: 'document', source: { type: 'base64', media_type: 'application/pdf', data: PDF_B64 } };
}

describe('modelSupportsDocuments', () => {
  afterEach(() => clearDynamicCapabilities());

  it('is true for Claude and Gemini document-capable families', () => {
    assert.equal(modelSupportsDocuments('claude-sonnet-5'), true);
    assert.equal(modelSupportsDocuments('claude-3-5-sonnet'), true);
    assert.equal(modelSupportsDocuments('claude-opus-5'), true);
    assert.equal(modelSupportsDocuments('gemini-2.0-flash'), true);
    assert.equal(modelSupportsDocuments('gemini-1.5-pro'), true);
  });

  it('is false for vision-capable but non-document models and unknown ids', () => {
    // gpt-4o accepts images but is NOT in the document-capable table.
    assert.equal(modelSupportsVision('gpt-4o'), true);
    assert.equal(modelSupportsDocuments('gpt-4o'), false);
    assert.equal(modelSupportsDocuments('grok-4'), false);
    assert.equal(modelSupportsDocuments('test-model'), false);
    assert.equal(modelSupportsDocuments(''), false);
    // @ts-expect-error — non-string input is handled defensively.
    assert.equal(modelSupportsDocuments(undefined), false);
  });

  it('resolves a provider-prefixed id via its suffix', () => {
    assert.equal(modelSupportsDocuments('openrouter/anthropic/claude-sonnet-5'), true);
    assert.equal(modelSupportsDocuments('openai/gpt-4o'), false);
  });

  it('honors an explicitly registered supportsDocuments capability over the prefix table', () => {
    // Force-ON a model the prefix table would reject.
    registerModelCapabilities('vendor-doc', {
      reasoning: false, defaultThinkingLevel: 'off', supportsAdaptiveThinking: false,
      requiresTemperatureOne: false, temperatureMustBeUnset: false, maxThinkingBudget: 0,
      supportsDocuments: true,
    });
    assert.equal(modelSupportsDocuments('vendor-doc'), true);
    // Force-OFF a model the prefix table would accept.
    registerModelCapabilities('claude-opus-5', {
      reasoning: false, defaultThinkingLevel: 'off', supportsAdaptiveThinking: false,
      requiresTemperatureOne: false, temperatureMustBeUnset: false, maxThinkingBudget: 0,
      supportsDocuments: false,
    });
    assert.equal(modelSupportsDocuments('claude-opus-5'), false);
  });
});

describe('convertMessages — document content block', () => {
  it('serializes a document block to an AI-SDK file part', () => {
    const messages: Message[] = [
      { role: 'user', content: [doc()] },
    ];
    const out = convertMessages(messages);
    assert.equal(out.length, 1);
    assert.equal(out[0].role, 'user');
    const parts = out[0].content as Array<Record<string, unknown>>;
    assert.ok(Array.isArray(parts));
    assert.equal(parts.length, 1);
    assert.deepEqual(parts[0], { type: 'file', data: PDF_B64, mediaType: 'application/pdf' });
  });

  it('keeps text, image and document blocks distinct in a mixed message', () => {
    const messages: Message[] = [
      {
        role: 'user',
        content: [
          { type: 'text', text: 'read this' },
          { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'IMG' } },
          doc(),
        ],
      },
    ];
    const parts = convertMessages(messages)[0].content as Array<Record<string, unknown>>;
    assert.deepEqual(parts[0], { type: 'text', text: 'read this' });
    assert.deepEqual(parts[1], { type: 'image', image: 'IMG', mediaType: 'image/png' });
    assert.deepEqual(parts[2], { type: 'file', data: PDF_B64, mediaType: 'application/pdf' });
  });
});
