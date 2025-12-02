import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { applyCacheControl, sanitizeToolCallIds } from '../src/provider-transform';
import type { Message } from '../src/types';

// ---------------------------------------------------------------------------
// applyCacheControl
// ---------------------------------------------------------------------------
describe('applyCacheControl', () => {
  it('does nothing for non-anthropic providers', () => {
    const msgs = [
      { role: 'system', content: 'You are helpful.' },
      { role: 'user', content: 'hi' },
    ];
    applyCacheControl(msgs, 'openai');
    assert.equal((msgs[0] as any).providerOptions, undefined);
    assert.equal((msgs[1] as any).providerOptions, undefined);
  });

  it('marks system messages for caching (anthropic)', () => {
    const msgs = [
      { role: 'system', content: 'prompt 1' },
      { role: 'user', content: 'hello' },
    ];
    applyCacheControl(msgs, 'anthropic');
    assert.deepEqual((msgs[0] as any).providerOptions, {
      anthropic: { cacheControl: { type: 'ephemeral' } },
    });
  });

  it('marks at most 2 system messages', () => {
    const msgs = [
      { role: 'system', content: 's1' },
      { role: 'system', content: 's2' },
      { role: 'system', content: 's3' },
      { role: 'user', content: 'u1' },
    ];
    applyCacheControl(msgs, 'anthropic');
    assert.ok((msgs[0] as any).providerOptions);
    assert.ok((msgs[1] as any).providerOptions);
    // Third system message should NOT have been marked (only the user messages get marked after)
    // Actually s3 might get marked if it also matches user. Let's check:
    // The function only marks system messages with systemCount < 2, so s3 should not be marked by the system loop.
    // But s3 has role=system, not role=user, so the user loop won't mark it either.
    // Let's verify:
    // After the system loop, s3 has no providerOptions from that loop.
    // The user loop only looks for role=user.
    assert.equal((msgs[2] as any).providerOptions, undefined);
  });

  it('marks last 2 user messages for caching', () => {
    const msgs = [
      { role: 'system', content: 'sys' },
      { role: 'user', content: 'u1' },
      { role: 'assistant', content: 'a1' },
      { role: 'user', content: 'u2' },
      { role: 'assistant', content: 'a2' },
      { role: 'user', content: 'u3' },
    ];
    applyCacheControl(msgs, 'anthropic');
    // u3 (index 5) and u2 (index 3) should be marked
    assert.deepEqual((msgs[5] as any).providerOptions, {
      anthropic: { cacheControl: { type: 'ephemeral' } },
    });
    assert.deepEqual((msgs[3] as any).providerOptions, {
      anthropic: { cacheControl: { type: 'ephemeral' } },
    });
    // u1 (index 1) should NOT be marked
    assert.equal((msgs[1] as any).providerOptions, undefined);
  });

  it('handles single user message', () => {
    const msgs = [
      { role: 'user', content: 'only one' },
    ];
    applyCacheControl(msgs, 'anthropic');
    assert.deepEqual((msgs[0] as any).providerOptions, {
      anthropic: { cacheControl: { type: 'ephemeral' } },
    });
  });

  it('handles no user messages', () => {
    const msgs = [
      { role: 'system', content: 'sys' },
      { role: 'assistant', content: 'hello' },
    ];
    // Should not throw
    applyCacheControl(msgs, 'anthropic');
    assert.ok((msgs[0] as any).providerOptions); // system still marked
  });

  it('handles empty messages array', () => {
    const msgs: { role: string; content: unknown }[] = [];
    applyCacheControl(msgs, 'anthropic');
    assert.equal(msgs.length, 0);
  });
});

// ---------------------------------------------------------------------------
// sanitizeToolCallIds
// ---------------------------------------------------------------------------
describe('sanitizeToolCallIds', () => {
  it('returns messages unchanged for non-anthropic providers', () => {
    const msgs: Message[] = [
      { role: 'assistant', content: 'hi', tool_calls: [{ id: 'call@123!', type: 'function', function: { name: 'test', arguments: '{}' } }] },
    ];
    const result = sanitizeToolCallIds(msgs, 'openai');
    assert.equal(result[0], msgs[0]); // same reference
  });

  it('strips non-alphanumeric chars from tool call IDs (anthropic)', () => {
    const msgs: Message[] = [
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          { id: 'call@foo#bar$baz', type: 'function', function: { name: 'readFile', arguments: '{}' } },
        ],
      },
    ];
    const result = sanitizeToolCallIds(msgs, 'anthropic');
    assert.equal(result[0].role, 'assistant');
    const assistant = result[0] as { role: 'assistant'; tool_calls?: Array<{ id: string }> };
    assert.equal(assistant.tool_calls![0].id, 'callfoobarbaz');
  });

  it('strips non-alphanumeric chars from tool message tool_call_id', () => {
    const msgs: Message[] = [
      { role: 'tool', tool_call_id: 'call@123!abc', content: 'result' },
    ];
    const result = sanitizeToolCallIds(msgs, 'anthropic');
    const tool = result[0] as { role: 'tool'; tool_call_id: string };
    assert.equal(tool.tool_call_id, 'call123abc');
  });

  it('preserves hyphens and underscores in IDs', () => {
    const msgs: Message[] = [
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          { id: 'call_foo-bar_123', type: 'function', function: { name: 'test', arguments: '{}' } },
        ],
      },
    ];
    const result = sanitizeToolCallIds(msgs, 'anthropic');
    const assistant = result[0] as { role: 'assistant'; tool_calls?: Array<{ id: string }> };
    assert.equal(assistant.tool_calls![0].id, 'call_foo-bar_123');
  });

  it('passes through user messages unchanged', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'hello' },
    ];
    const result = sanitizeToolCallIds(msgs, 'anthropic');
    assert.deepEqual(result[0], msgs[0]);
  });

  it('passes through system messages unchanged', () => {
    const msgs: Message[] = [
      { role: 'system', content: 'system prompt' },
    ];
    const result = sanitizeToolCallIds(msgs, 'anthropic');
    assert.deepEqual(result[0], msgs[0]);
  });

  it('handles assistant messages without tool_calls', () => {
    const msgs: Message[] = [
      { role: 'assistant', content: 'just text' },
    ];
    const result = sanitizeToolCallIds(msgs, 'anthropic');
    assert.deepEqual(result[0], msgs[0]);
  });

  it('handles mixed message types', () => {
    const msgs: Message[] = [
      { role: 'system', content: 'sys' },
      { role: 'user', content: 'u1' },
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          { id: 'tc!1', type: 'function', function: { name: 'bash', arguments: '{}' } },
          { id: 'tc@2', type: 'function', function: { name: 'read', arguments: '{}' } },
        ],
      },
      { role: 'tool', tool_call_id: 'tc!1', content: 'output1' },
      { role: 'tool', tool_call_id: 'tc@2', content: 'output2' },
    ];
    const result = sanitizeToolCallIds(msgs, 'anthropic');
    assert.equal(result.length, 5);
    const assistant = result[2] as { role: 'assistant'; tool_calls?: Array<{ id: string }> };
    assert.equal(assistant.tool_calls![0].id, 'tc1');
    assert.equal(assistant.tool_calls![1].id, 'tc2');
    const tool1 = result[3] as { role: 'tool'; tool_call_id: string };
    assert.equal(tool1.tool_call_id, 'tc1');
    const tool2 = result[4] as { role: 'tool'; tool_call_id: string };
    assert.equal(tool2.tool_call_id, 'tc2');
  });

  it('handles empty messages array', () => {
    const result = sanitizeToolCallIds([], 'anthropic');
    assert.deepEqual(result, []);
  });
});
