import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { estimateTokens, estimateToolCallTokens, estimateMessageTokens } from '../src/token-estimator';

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
});
