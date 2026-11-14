import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { estimateTokens, estimateToolCallTokens, estimateMessageTokens, estimateConversationTokens, estimateBlockTokens, estimateContentTokens, estimateTokensForFileType, charsPerTokenForFileType, IMAGE_DOCUMENT_TOKEN_ESTIMATE } from '../src/token-estimator';

describe('estimateTokens', () => {
  it('returns 0 for empty string', () => {
    assert.equal(estimateTokens(''), 0);
  });

  it('estimates tokens for short text', () => {
    const estimate = estimateTokens('Hello, world!');
    assert.ok(estimate > 0);
    assert.ok(estimate < 10);
  });

  it('estimates tokens for longer text proportionally', () => {
    const short = estimateTokens('hello');
    const long = estimateTokens('hello '.repeat(100));
    assert.ok(long > short * 10, 'longer text should produce more tokens');
  });

  it('handles code content', () => {
    const code = 'function foo(bar: string): number { return parseInt(bar, 10); }';
    const estimate = estimateTokens(code);
    assert.ok(estimate > 5 && estimate < 50);
  });

  it('handles unicode content', () => {
    const unicode = '你好世界 🌍 こんにちは';
    const estimate = estimateTokens(unicode);
    assert.ok(estimate > 0);
  });
});

describe('estimateToolCallTokens', () => {
  it('estimates tokens for a simple tool call', () => {
    const estimate = estimateToolCallTokens('file_read', { file_path: '/tmp/test.ts' });
    assert.ok(estimate > 5);
    assert.ok(estimate < 50);
  });

  it('larger input produces more tokens', () => {
    const small = estimateToolCallTokens('bash', { command: 'ls' });
    const large = estimateToolCallTokens('bash', { command: 'find . -name "*.ts" -type f -not -path "*/node_modules/*" | head -100' });
    assert.ok(large > small);
  });

  it('serialized JSON input is charged at the dense (~2 char/token) ratio, not 4', () => {
    const input = { a: 'x'.repeat(400) };
    const serialized = JSON.stringify(input);
    const got = estimateToolCallTokens('t', input);
    // JSON ratio (2/token) must exceed what the generic 4/token estimate would give.
    const genericBody = estimateTokens(serialized);
    assert.ok(got - estimateTokens('t') - 10 > genericBody,
      'dense-JSON estimate must exceed the generic 4-char/token estimate');
    // And it matches the JSON-aware helper exactly.
    assert.equal(got, estimateTokens('t') + estimateTokensForFileType(serialized, 'json') + 10);
  });
});

describe('estimateMessageTokens', () => {
  it('estimates tokens for a user message', () => {
    const estimate = estimateMessageTokens({ role: 'user', content: 'What is TypeScript?' });
    assert.ok(estimate > 3);
  });

  it('estimates tokens for a system message', () => {
    const estimate = estimateMessageTokens({ role: 'system', content: 'You are a helpful assistant.' });
    assert.ok(estimate > 3);
  });

  it('handles null content', () => {
    const estimate = estimateMessageTokens({ role: 'assistant', content: null });
    assert.equal(estimate, 4); // role overhead only
  });

  it('handles array content', () => {
    const estimate = estimateMessageTokens({
      role: 'user',
      content: [{ type: 'text', text: 'Check this file' }],
    } as any);
    assert.ok(estimate > 3);
  });

  it('handles undefined content', () => {
    const estimate = estimateMessageTokens({ role: 'assistant', content: undefined });
    assert.equal(estimate, 4);
  });

  it('handles array content with non-text parts', () => {
    const estimate = estimateMessageTokens({
      role: 'user',
      content: [
        { type: 'image', source: { data: 'base64data' } },
        { type: 'text', text: 'Describe this image' },
      ],
    } as any);
    // image (2000) + the text + overhead
    assert.ok(estimate > IMAGE_DOCUMENT_TOKEN_ESTIMATE);
  });

  it('counts an image block as the fixed conservative estimate, not zero', () => {
    const estimate = estimateMessageTokens({
      role: 'user',
      content: [{ type: 'image', source: { data: 'base64data' } }],
    } as any);
    assert.equal(estimate, IMAGE_DOCUMENT_TOKEN_ESTIMATE + 4);
  });
});

describe('estimateBlockTokens (per-block-type dispatch)', () => {
  it('counts a tool_use block by name + serialized input, not zero', () => {
    const block = { type: 'tool_use', name: 'bash', input: { command: 'find / -name "*.ts"'.repeat(20) } };
    const est = estimateBlockTokens(block);
    assert.ok(est > 20, 'large tool_use input must contribute many tokens');
  });

  it('counts a thinking block by its thinking text', () => {
    const est = estimateBlockTokens({ type: 'thinking', thinking: 'x'.repeat(400) });
    assert.ok(est >= 100);
  });

  it('counts a redacted_thinking block by its data payload', () => {
    const est = estimateBlockTokens({ type: 'redacted_thinking', data: 'y'.repeat(400) });
    assert.ok(est >= 100);
  });

  it('gives image and document a fixed conservative estimate (not char-counted)', () => {
    const hugePdf = { type: 'document', source: { data: 'A'.repeat(1_330_000) } };
    assert.equal(estimateBlockTokens(hugePdf), IMAGE_DOCUMENT_TOKEN_ESTIMATE);
    assert.equal(estimateBlockTokens({ type: 'image', source: { data: 'A'.repeat(1_330_000) } }), IMAGE_DOCUMENT_TOKEN_ESTIMATE);
  });

  it('recurses into tool_result content (string and nested blocks)', () => {
    assert.equal(estimateBlockTokens({ type: 'tool_result', content: 'hello' }), estimateTokens('hello'));
    const nested = { type: 'tool_result', content: [{ type: 'text', text: 'abcd' }, { type: 'image', source: {} }] };
    assert.equal(estimateBlockTokens(nested), estimateTokens('abcd') + IMAGE_DOCUMENT_TOKEN_ESTIMATE);
  });

  it('counts a raw string block directly', () => {
    assert.equal(estimateBlockTokens('abcd'), estimateTokens('abcd'));
  });

  it('serializes unknown block types instead of dropping them', () => {
    const est = estimateBlockTokens({ type: 'server_tool_use', foo: 'z'.repeat(80) });
    assert.ok(est > 0);
  });

  it('returns 0 for a non-object, non-string block', () => {
    assert.equal(estimateBlockTokens(42 as any), 0);
    assert.equal(estimateBlockTokens(null as any), 0);
  });

  it('text block with non-string text counts as 0', () => {
    assert.equal(estimateBlockTokens({ type: 'text', text: 123 }), 0);
  });

  it('thinking/redacted block with missing payload counts as 0', () => {
    assert.equal(estimateBlockTokens({ type: 'thinking' }), 0);
    assert.equal(estimateBlockTokens({ type: 'redacted_thinking' }), 0);
  });
});

describe('estimateContentTokens', () => {
  it('handles nullish, string, array and object content', () => {
    assert.equal(estimateContentTokens(null), 0);
    assert.equal(estimateContentTokens(undefined), 0);
    assert.equal(estimateContentTokens('abcd'), estimateTokens('abcd'));
    assert.equal(estimateContentTokens([{ type: 'text', text: 'ab' }]), estimateTokens('ab'));
    assert.ok(estimateContentTokens({ nested: 'x'.repeat(40) }) > 0);
  });
});

describe('charsPerTokenForFileType / estimateTokensForFileType', () => {
  it('uses a denser 2 chars/token ratio for JSON family', () => {
    assert.equal(charsPerTokenForFileType('json'), 2);
    assert.equal(charsPerTokenForFileType('.jsonl'), 2);
    assert.equal(charsPerTokenForFileType('JSONC'), 2);
    assert.equal(charsPerTokenForFileType('ts'), 4);
  });

  it('estimates dense JSON at roughly double the generic estimate', () => {
    const json = '{"a":1,"b":2,"c":3}'.repeat(10);
    assert.equal(estimateTokensForFileType(json, 'json'), Math.ceil(json.length / 2));
    assert.ok(estimateTokensForFileType(json, 'json') > estimateTokens(json));
    assert.equal(estimateTokensForFileType('', 'json'), 0);
  });
});

describe('estimateConversationTokens', () => {
  it('returns framing overhead for empty conversation', () => {
    const estimate = estimateConversationTokens([]);
    assert.equal(estimate, 3);
  });

  it('sums message tokens plus framing', () => {
    const messages = [
      { role: 'user', content: 'Hello' },
      { role: 'assistant', content: 'Hi there' },
    ];
    const estimate = estimateConversationTokens(messages);
    const msg1 = estimateMessageTokens(messages[0]);
    const msg2 = estimateMessageTokens(messages[1]);
    assert.equal(estimate, 3 + msg1 + msg2);
  });

  it('handles multiple messages with tool calls', () => {
    const messages = [
      { role: 'user', content: 'Fix the bug' },
      { role: 'assistant', content: null },
      { role: 'tool', content: 'output' },
      { role: 'assistant', content: 'Done' },
    ];
    const estimate = estimateConversationTokens(messages);
    assert.ok(estimate > 3 + 4 * 4);
  });
});
