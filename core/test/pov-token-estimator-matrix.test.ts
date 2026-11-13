import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  estimateTokens,
  estimateToolCallTokens,
  estimateMessageTokens,
  estimateConversationTokens,
} from '../src/token-estimator';
import {
  estimateTokens as estimateTokensUtil,
  estimateToolSchemaTokens,
  truncateToTokenLimit,
} from '../src/utils/tokens';
import type { Message } from '../src/types';

// The contract under test is the ~4-chars/token heuristic. We recompute the
// EXPECTED values from that same formula (Math.ceil(len/4)) rather than reading
// source text, and assert the real functions agree.
const CHARS_PER_TOKEN = 4;
const expectTok = (s: string) => (s.length === 0 ? 0 : Math.ceil(s.length / CHARS_PER_TOKEN));
const OVERHEAD_PER_MESSAGE = 4;
const CONVO_FRAMING = 3;

describe('pov/token-estimator matrix: estimateTokens base contract', () => {
  it('empty string estimates to exactly 0', () => {
    assert.equal(estimateTokens(''), 0);
  });

  it('re-export from token-estimator is the same function as utils/tokens', () => {
    assert.equal(estimateTokens, estimateTokensUtil);
  });

  it('matches ceil(len/4) across a length sweep', () => {
    for (let len = 0; len <= 400; len++) {
      const s = 'a'.repeat(len);
      assert.equal(estimateTokens(s), expectTok(s), `len=${len}`);
    }
  });

  it('boundary lengths 1..8 produce the ceil-rounded counts', () => {
    const expected = [0, 1, 1, 1, 1, 2, 2, 2, 2];
    for (let len = 0; len <= 8; len++) {
      assert.equal(estimateTokens('x'.repeat(len)), expected[len], `len=${len}`);
    }
  });

  it('exactly-divisible lengths do not round up', () => {
    assert.equal(estimateTokens('abcd'), 1);
    assert.equal(estimateTokens('abcdefgh'), 2);
    assert.equal(estimateTokens('a'.repeat(400)), 100);
  });
});

describe('pov/token-estimator matrix: monotonicity property', () => {
  it('is non-decreasing (monotonic) as text grows one char at a time', () => {
    let prev = estimateTokens('');
    for (let len = 1; len <= 500; len++) {
      const cur = estimateTokens('z'.repeat(len));
      assert.ok(cur >= prev, `monotonic break at len=${len}: ${cur} < ${prev}`);
      prev = cur;
    }
  });

  it('a strict superset string never estimates fewer tokens', () => {
    const base = 'the quick brown fox';
    for (const suffix of ['', '!', ' jumps', ' over the lazy dog', '.'.repeat(50)]) {
      assert.ok(
        estimateTokens(base + suffix) >= estimateTokens(base),
        `suffix=${JSON.stringify(suffix)}`,
      );
    }
  });

  it('doubling the text at least doubles minus rounding slack', () => {
    const unit = 'hello world ';
    const single = estimateTokens(unit);
    const hundred = estimateTokens(unit.repeat(100));
    assert.ok(hundred >= single * 99, `${hundred} vs ${single}`);
    assert.equal(hundred, expectTok(unit.repeat(100)));
  });
});

describe('pov/token-estimator matrix: degenerate + unicode inputs', () => {
  it('counts by UTF-16 code units (.length), astral chars count as 2', () => {
    // '😀' has .length === 2 in JS.
    const emoji = '😀';
    assert.equal(emoji.length, 2);
    assert.equal(estimateTokens(emoji), expectTok(emoji));
    const many = '😀'.repeat(10); // length 20 => ceil(20/4)=5
    assert.equal(estimateTokens(many), 5);
  });

  it('whitespace-only and newline strings still count their length', () => {
    assert.equal(estimateTokens('    '), 1);
    assert.equal(estimateTokens('\n\n\n\n\n\n\n\n'), 2);
  });

  it('handles a very large string without error and matches the formula', () => {
    const huge = 'x'.repeat(1_000_003);
    assert.equal(estimateTokens(huge), Math.ceil(1_000_003 / 4));
  });

  it('combining-mark unicode is counted by code units', () => {
    const s = 'é'.repeat(5); // 'é' as e + combining acute, length 10
    assert.equal(s.length, 10);
    assert.equal(estimateTokens(s), 3);
  });
});

describe('pov/token-estimator matrix: estimateMessageTokens', () => {
  it('string content = estimateTokens(text) + per-message overhead', () => {
    const msg: Message = { role: 'user', content: 'hello world' };
    assert.equal(
      estimateMessageTokens(msg),
      estimateTokens('hello world') + OVERHEAD_PER_MESSAGE,
    );
  });

  it('empty string content still charges only the overhead', () => {
    const msg: Message = { role: 'user', content: '' };
    assert.equal(estimateMessageTokens(msg), OVERHEAD_PER_MESSAGE);
  });

  it('null content (assistant tool-call turn) returns exactly the overhead', () => {
    const msg: Message = { role: 'assistant', content: null };
    assert.equal(estimateMessageTokens(msg), OVERHEAD_PER_MESSAGE);
  });

  it('undefined content returns exactly the overhead', () => {
    assert.equal(
      estimateMessageTokens({ role: 'user', content: undefined as unknown as string }),
      OVERHEAD_PER_MESSAGE,
    );
  });

  it('array content sums text parts and ignores non-text parts', () => {
    const msg = {
      role: 'user',
      content: [
        { type: 'text', text: 'foo bar' },
        { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'AAAA' } },
        { type: 'text', text: 'baz' },
      ],
    };
    assert.equal(
      estimateMessageTokens(msg as unknown as Message),
      estimateTokens('foo bar' + 'baz') + OVERHEAD_PER_MESSAGE,
    );
  });

  it('empty array content charges only overhead', () => {
    assert.equal(
      estimateMessageTokens({ role: 'user', content: [] as unknown as string }),
      OVERHEAD_PER_MESSAGE,
    );
  });

  it('array with malformed/non-text entries (null, number, missing text) is robust', () => {
    const msg = {
      role: 'user',
      content: [
        null,
        42,
        { type: 'text' }, // no text field
        { text: 123 }, // non-string text
        { type: 'text', text: 'real' },
      ],
    };
    assert.equal(
      estimateMessageTokens(msg as unknown as Message),
      estimateTokens('real') + OVERHEAD_PER_MESSAGE,
    );
  });

  it('message cost grows with content length (monotone in text)', () => {
    const short = estimateMessageTokens({ role: 'user', content: 'hi' });
    const long = estimateMessageTokens({ role: 'user', content: 'hi '.repeat(200) });
    assert.ok(long > short);
  });
});

describe('pov/token-estimator matrix: estimateConversationTokens', () => {
  it('empty conversation = framing constant only', () => {
    assert.equal(estimateConversationTokens([]), CONVO_FRAMING);
  });

  it('equals framing + sum of per-message estimates', () => {
    const msgs: Message[] = [
      { role: 'system', content: 'you are a helpful assistant' },
      { role: 'user', content: 'what is 2+2?' },
      { role: 'assistant', content: 'It is 4.' },
    ];
    const expected =
      CONVO_FRAMING + msgs.reduce((acc, m) => acc + estimateMessageTokens(m), 0);
    assert.equal(estimateConversationTokens(msgs), expected);
  });

  it('adding a message never decreases the total (monotone)', () => {
    const base: Message[] = [{ role: 'user', content: 'a' }];
    const before = estimateConversationTokens(base);
    const after = estimateConversationTokens([
      ...base,
      { role: 'assistant', content: 'a much longer reply here' },
    ]);
    assert.ok(after > before);
  });

  it('a conversation of null-content turns is framing + overhead*n', () => {
    const msgs: Message[] = [
      { role: 'assistant', content: null },
      { role: 'assistant', content: null },
    ];
    assert.equal(
      estimateConversationTokens(msgs),
      CONVO_FRAMING + 2 * OVERHEAD_PER_MESSAGE,
    );
  });
});

describe('pov/token-estimator matrix: estimateToolCallTokens', () => {
  it('= tokens(name) + tokens(JSON(input)) + framing(10)', () => {
    const name = 'bash';
    const input = { command: 'ls -la', timeout: 5000 };
    const expected =
      estimateTokens(name) + estimateTokens(JSON.stringify(input)) + 10;
    assert.equal(estimateToolCallTokens(name, input), expected);
  });

  it('empty input object still charges name + "{}" + framing', () => {
    const expected = estimateTokens('x') + estimateTokens('{}') + 10;
    assert.equal(estimateToolCallTokens('x', {}), expected);
  });

  it('empty name + empty input is exactly the framing constant', () => {
    // estimateTokens('')=0, estimateTokens('{}')=1 -> 11
    assert.equal(estimateToolCallTokens('', {}), 0 + estimateTokens('{}') + 10);
  });

  it('larger inputs produce strictly more tokens (monotone in payload)', () => {
    const small = estimateToolCallTokens('tool', { a: 1 });
    const large = estimateToolCallTokens('tool', { a: 'x'.repeat(400) });
    assert.ok(large > small);
  });

  it('is always at least the framing floor of 10', () => {
    assert.ok(estimateToolCallTokens('', {}) >= 10);
  });

  it('handles nested/unicode input via JSON serialization', () => {
    const input = { nested: { deep: ['😀', 'café'] } };
    assert.equal(
      estimateToolCallTokens('t', input),
      estimateTokens('t') + estimateTokens(JSON.stringify(input)) + 10,
    );
  });
});

describe('pov/token-estimator matrix: estimateToolSchemaTokens', () => {
  it('empty tool list estimates to 0', () => {
    assert.equal(estimateToolSchemaTokens([]), 0);
  });

  it('single tool = tokens(name + description + JSON(inputSchema))', () => {
    const tool = {
      name: 'grep',
      description: 'search files',
      inputSchema: { type: 'object', properties: { pattern: { type: 'string' } } },
    };
    const expected = estimateTokens(
      tool.name + tool.description + JSON.stringify(tool.inputSchema),
    );
    assert.equal(estimateToolSchemaTokens([tool]), expected);
  });

  it('sums across multiple tools', () => {
    const tools = [
      { name: 'a', description: 'alpha', inputSchema: {} },
      { name: 'b', description: 'beta', inputSchema: { x: 1 } },
      { name: 'c', description: 'gamma', inputSchema: { y: [1, 2, 3] } },
    ];
    const expected = tools.reduce(
      (acc, t) =>
        acc + estimateTokens(t.name + t.description + JSON.stringify(t.inputSchema)),
      0,
    );
    assert.equal(estimateToolSchemaTokens(tools), expected);
  });

  it('grows as the schema grows (monotone in schema size)', () => {
    const small = estimateToolSchemaTokens([
      { name: 'n', description: 'd', inputSchema: {} },
    ]);
    const big = estimateToolSchemaTokens([
      {
        name: 'n',
        description: 'd'.repeat(200),
        inputSchema: { huge: 'y'.repeat(400) },
      },
    ]);
    assert.ok(big > small);
  });

  it('adding more tools never decreases the total', () => {
    const one = estimateToolSchemaTokens([
      { name: 'one', description: 'first', inputSchema: {} },
    ]);
    const two = estimateToolSchemaTokens([
      { name: 'one', description: 'first', inputSchema: {} },
      { name: 'two', description: 'second', inputSchema: {} },
    ]);
    assert.ok(two >= one);
  });

  it('null inputSchema serializes to "null" and is counted', () => {
    const expected = estimateTokens('n' + 'd' + JSON.stringify(null));
    assert.equal(
      estimateToolSchemaTokens([{ name: 'n', description: 'd', inputSchema: null }]),
      expected,
    );
  });
});

describe('pov/token-estimator matrix: truncateToTokenLimit (companion of tokens.ts)', () => {
  it('returns text unchanged when already within limit', () => {
    const text = 'short text';
    assert.equal(truncateToTokenLimit(text, 1000), text);
  });

  it('maxTokens <= 0 returns the bare marker', () => {
    assert.equal(truncateToTokenLimit('anything', 0), '[truncated]');
    assert.equal(truncateToTokenLimit('anything', -5), '[truncated]');
  });

  it('truncates long text and appends the marker', () => {
    const text = 'a'.repeat(1000);
    const out = truncateToTokenLimit(text, 10);
    assert.ok(out.endsWith('\n[truncated]'));
    assert.ok(out.length < text.length);
    // Result fits within the requested budget.
    assert.ok(estimateTokens(out) <= 10);
  });

  it('text exactly at the limit is returned unchanged', () => {
    const text = 'abcd'.repeat(10); // 40 chars => 10 tokens
    assert.equal(estimateTokens(text), 10);
    assert.equal(truncateToTokenLimit(text, 10), text);
  });
});
