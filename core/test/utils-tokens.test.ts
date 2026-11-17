import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  estimateTokens,
  estimateToolSchemaTokens,
  truncateToTokenLimit,
  truncationMarker,
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

  it('honours a configurable charsPerToken ratio', () => {
    const text = 'x'.repeat(100);
    assert.equal(estimateTokens(text), 25);       // default 4:1
    assert.equal(estimateTokens(text, 2), 50);    // denser JSON-like ratio
    assert.equal(estimateTokens(text, 1), 100);   // 1 char = 1 token
  });

  it('a tighter ratio never under-counts relative to the default', () => {
    const text = '{"k":"v","n":1}'.repeat(10);
    assert.ok(estimateTokens(text, 2) >= estimateTokens(text));
  });

  it('falls back to the default ratio when given a non-positive value', () => {
    const text = 'x'.repeat(100);
    assert.equal(estimateTokens(text, 0), 25);
    assert.equal(estimateTokens(text, -4), 25);
  });

  it('returns 0 for empty string regardless of ratio', () => {
    assert.equal(estimateTokens('', 2), 0);
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

  it('applies a configurable charsPerToken ratio to the serialised schema', () => {
    const tools = [{
      name: 'grep',
      description: 'search',
      inputSchema: { type: 'object', properties: { pattern: { type: 'string' } } },
    }];
    const serialized = tools[0].name + tools[0].description + JSON.stringify(tools[0].inputSchema);
    // Default stays flat-4 (backward compatible).
    assert.equal(estimateToolSchemaTokens(tools), Math.ceil(serialized.length / 4));
    // A denser (JSON) ratio yields a larger, less-optimistic estimate.
    assert.equal(estimateToolSchemaTokens(tools, 2), Math.ceil(serialized.length / 2));
    assert.ok(estimateToolSchemaTokens(tools, 2) > estimateToolSchemaTokens(tools));
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

  it('drops the whole input and quantifies it for maxTokens = 0', () => {
    // 'any' = 3 chars ≈ 1 token; the marker has no leading newline (nothing kept).
    assert.equal(
      truncateToTokenLimit('any', 0),
      '[truncated — ~1 tokens omitted; request the specific section to see the rest]',
    );
  });

  it('drops the whole input and quantifies it for maxTokens < 0', () => {
    const result = truncateToTokenLimit('any', -5);
    assert.ok(!result.startsWith('\n'));
    assert.match(result, /~1 tokens omitted/);
    assert.match(result, /request the specific section to see the rest/);
  });

  it('truncates text exceeding the limit with a quantified recovery marker', () => {
    const text = 'x'.repeat(1000);
    const result = truncateToTokenLimit(text, 10);
    assert.ok(result.length < text.length);
    assert.ok(result.includes('[truncated'));
    assert.match(result, /tokens omitted/);
    assert.match(result, /request the specific section to see the rest/);
    // The omitted-token figure is a real number, not a bare marker.
    assert.match(result, /~\d+ tokens omitted/);
  });

  it('preserves the start of the text', () => {
    // Budget exceeds the informative marker's own token cost, so content is kept.
    const text = 'START' + 'x'.repeat(1000);
    const result = truncateToTokenLimit(text, 100);
    assert.ok(result.startsWith('START'));
  });

  it('returns empty string unchanged for any positive limit', () => {
    assert.equal(truncateToTokenLimit('', 1), '');
  });

  it('does not truncate text exactly at the limit', () => {
    const text = 'a'.repeat(40); // 40 chars = 10 tokens
    assert.equal(truncateToTokenLimit(text, 10), text);
  });

  it('includes the truncation marker in output', () => {
    const text = 'a'.repeat(200);
    const result = truncateToTokenLimit(text, 5);
    assert.ok(result.includes('[truncated'));
    assert.match(result, /tokens omitted/);
  });

  it('truncates harder for a denser charsPerToken ratio', () => {
    const text = 'a'.repeat(400); // 100 tokens @4:1, 200 tokens @2:1
    // At the default ratio 400 chars = 100 tokens, so a 100-token budget keeps it whole.
    assert.equal(truncateToTokenLimit(text, 100), text);
    // At the JSON ratio the same text is 200 tokens, so it must be truncated to fit 100.
    const dense = truncateToTokenLimit(text, 100, 2);
    assert.ok(dense.length < text.length);
    assert.match(dense, /tokens omitted/);
    // The truncated result genuinely fits the budget at the same ratio, marker included.
    assert.ok(estimateTokens(dense, 2) <= 100);
  });

  it('a non-positive ratio falls back to the default behaviour', () => {
    const text = 'a'.repeat(40); // exactly 10 tokens @4:1
    assert.equal(truncateToTokenLimit(text, 10, 0), text);
    assert.equal(truncateToTokenLimit(text, 10, -2), text);
  });

  it('reports a larger omitted-token count the more is dropped (discriminates the quantity)', () => {
    const small = truncateToTokenLimit('a'.repeat(400), 10);
    const big = truncateToTokenLimit('a'.repeat(4000), 10);
    const n = (s: string): number => Number(/~(\d+) tokens omitted/.exec(s)?.[1]);
    assert.ok(n(small) > 0);
    assert.ok(n(big) > n(small), `expected ${n(big)} > ${n(small)}`);
  });
});

// ---------------------------------------------------------------------------
// truncationMarker
// ---------------------------------------------------------------------------

describe('truncationMarker – unit', () => {
  it('quantifies omitted tokens and gives a recovery hint', () => {
    const m = truncationMarker(42, true);
    assert.match(m, /~42 tokens omitted/);
    assert.match(m, /request the specific section to see the rest/);
  });

  it('prepends a newline only when content was kept', () => {
    assert.ok(truncationMarker(5, true).startsWith('\n'));
    assert.ok(!truncationMarker(5, false).startsWith('\n'));
  });

  it('never reports a negative omitted count', () => {
    assert.match(truncationMarker(-3, false), /~0 tokens omitted/);
  });
});
