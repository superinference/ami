import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  estimateTokens,
  estimateToolSchemaTokens,
  truncateToTokenLimit,
} from '../src/utils/tokens';

// ---------------------------------------------------------------------------
// estimateTokens
// ---------------------------------------------------------------------------

describe('estimateTokens – unit', () => {
  it('returns 0 for empty string', () => {
    assert.equal(estimateTokens(''), 0);
  });

  it('returns 1 for a single character', () => {
    assert.equal(estimateTokens('a'), 1);
  });

  it('returns ceil(length / 4)', () => {
    assert.equal(estimateTokens('1234'), 1);   // 4/4 = 1
    assert.equal(estimateTokens('12345'), 2);  // ceil(5/4) = 2
    assert.equal(estimateTokens('12345678'), 2); // 8/4 = 2
    assert.equal(estimateTokens('123456789'), 3); // ceil(9/4) = 3
  });

  it('handles long text', () => {
    const text = 'a'.repeat(1000);
    assert.equal(estimateTokens(text), 250);
  });

  it('handles text with special characters', () => {
    const text = '!@#$%^&*()';
    assert.equal(estimateTokens(text), Math.ceil(10 / 4));
  });

  it('handles multi-byte characters', () => {
    // Emoji and CJK characters — each counts as 1 char in JS
    const text = 'abc';
    assert.equal(estimateTokens(text), 1); // 3 chars / 4 = ceil(0.75) = 1
  });
});

// ---------------------------------------------------------------------------
// estimateToolSchemaTokens
// ---------------------------------------------------------------------------

describe('estimateToolSchemaTokens', () => {
  it('returns 0 for empty tools array', () => {
    assert.equal(estimateToolSchemaTokens([]), 0);
  });

  it('estimates tokens for a single tool', () => {
    const tools = [{
      name: 'file_read',
      description: 'Read a file',
      inputSchema: { type: 'object', properties: { path: { type: 'string' } } },
    }];
    const tokens = estimateToolSchemaTokens(tools);
    assert.ok(tokens > 0);
    // Should be roughly (name + description + JSON.stringify(schema)).length / 4
    const serialized = tools[0].name + tools[0].description + JSON.stringify(tools[0].inputSchema);
    assert.equal(tokens, Math.ceil(serialized.length / 4));
  });

  it('sums tokens across multiple tools', () => {
    const tools = [
      { name: 'a', description: 'desc A', inputSchema: {} },
      { name: 'b', description: 'desc B', inputSchema: {} },
    ];
    const total = estimateToolSchemaTokens(tools);
    const individual = tools.map(t =>
      estimateTokens(t.name + t.description + JSON.stringify(t.inputSchema))
    );
    assert.equal(total, individual[0] + individual[1]);
  });

  it('handles tool with complex schema', () => {
    const tools = [{
      name: 'multi_edit',
      description: 'Apply multiple edits',
      inputSchema: {
        type: 'object',
        properties: {
          file_path: { type: 'string' },
          edits: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                old_string: { type: 'string' },
                new_string: { type: 'string' },
              },
            },
          },
        },
      },
    }];
    const tokens = estimateToolSchemaTokens(tools);
    assert.ok(tokens > 10, 'Complex schema should have substantial tokens');
  });
});

// ---------------------------------------------------------------------------
// truncateToTokenLimit
// ---------------------------------------------------------------------------

describe('truncateToTokenLimit – unit', () => {
  it('returns text unchanged when within limit', () => {
    assert.equal(truncateToTokenLimit('hello', 100), 'hello');
  });

  it('returns [truncated] for maxTokens = 0', () => {
    assert.equal(truncateToTokenLimit('any', 0), '[truncated]');
  });

  it('returns [truncated] for maxTokens < 0', () => {
    assert.equal(truncateToTokenLimit('any', -5), '[truncated]');
  });

  it('truncates text exceeding the limit', () => {
    const text = 'x'.repeat(1000);
    const result = truncateToTokenLimit(text, 10);
    assert.ok(result.length < text.length);
    assert.ok(result.endsWith('[truncated]'));
  });

  it('preserves the start of the text', () => {
    const text = 'START' + 'x'.repeat(1000);
    const result = truncateToTokenLimit(text, 20);
    assert.ok(result.startsWith('START'));
  });

  it('returns empty string unchanged for any positive limit', () => {
    assert.equal(truncateToTokenLimit('', 1), '');
  });

  it('does not truncate text exactly at the limit', () => {
    const text = 'a'.repeat(40); // 40 chars = 10 tokens
    assert.equal(truncateToTokenLimit(text, 10), text);
  });

  it('includes [truncated] marker in output', () => {
    const text = 'a'.repeat(200);
    const result = truncateToTokenLimit(text, 5);
    assert.ok(result.includes('[truncated]'));
  });
});
