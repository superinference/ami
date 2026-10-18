import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { sanitizeToolCallIds, buildConversationCachePoints, buildToolCacheBreakpoints, healOrphanedToolCalls, stripEmptyTextBlocks } from '../src/provider-transform';
import type { Message, ToolDefinition } from '../src/types';

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

  it('produces non-empty fallback ID when all chars stripped', () => {
    const msgs: Message[] = [
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          { id: '@@##$$', type: 'function', function: { name: 'test', arguments: '{}' } },
        ],
      },
    ];
    const result = sanitizeToolCallIds(msgs, 'anthropic');
    const assistant = result[0] as { role: 'assistant'; tool_calls?: Array<{ id: string }> };
    assert.ok(assistant.tool_calls![0].id.length > 0, 'ID must not be empty');
  });

  it('produces non-empty fallback for tool_call_id when all chars stripped', () => {
    const msgs: Message[] = [
      { role: 'tool', tool_call_id: '!!!', content: 'result' },
    ];
    const result = sanitizeToolCallIds(msgs, 'anthropic');
    const tool = result[0] as { role: 'tool'; tool_call_id: string };
    assert.ok(tool.tool_call_id.length > 0, 'tool_call_id must not be empty');
  });
});

// ---------------------------------------------------------------------------
// buildConversationCachePoints
// ---------------------------------------------------------------------------
describe('buildConversationCachePoints', () => {
  it('returns empty object for empty messages', () => {
    assert.deepEqual(buildConversationCachePoints([]), {});
  });

  it('marks the last non-tool message', () => {
    const msgs = [
      { role: 'user' },
      { role: 'assistant' },
    ];
    const points = buildConversationCachePoints(msgs);
    assert.deepEqual(points[1], { type: 'ephemeral' });
  });

  it('marks last two non-tool messages', () => {
    const msgs = [
      { role: 'user' },
      { role: 'assistant' },
      { role: 'user' },
    ];
    const points = buildConversationCachePoints(msgs);
    assert.deepEqual(points[1], { type: 'ephemeral' });
    assert.deepEqual(points[2], { type: 'ephemeral' });
  });

  it('skips tool messages when finding cache points', () => {
    const msgs = [
      { role: 'user' },
      { role: 'assistant' },
      { role: 'tool' },
      { role: 'tool' },
    ];
    const points = buildConversationCachePoints(msgs);
    assert.deepEqual(points[0], { type: 'ephemeral' });
    assert.deepEqual(points[1], { type: 'ephemeral' });
    assert.equal(points[2], undefined);
    assert.equal(points[3], undefined);
  });

  it('handles single non-tool message', () => {
    const msgs = [{ role: 'user' }];
    const points = buildConversationCachePoints(msgs);
    assert.deepEqual(points[0], { type: 'ephemeral' });
    assert.equal(Object.keys(points).length, 1);
  });

  it('handles all tool messages', () => {
    const msgs = [{ role: 'tool' }, { role: 'tool' }];
    const points = buildConversationCachePoints(msgs);
    assert.deepEqual(points, {});
  });

  it('marks assistant messages not just user messages', () => {
    const msgs = [
      { role: 'user' },
      { role: 'assistant' },
      { role: 'tool' },
    ];
    const points = buildConversationCachePoints(msgs);
    assert.deepEqual(points[1], { type: 'ephemeral' });
    assert.deepEqual(points[0], { type: 'ephemeral' });
  });
});

// ---------------------------------------------------------------------------
// buildToolCacheBreakpoints
// ---------------------------------------------------------------------------
describe('buildToolCacheBreakpoints', () => {
  const stubTool = (name: string): ToolDefinition => ({
    name,
    description: 'stub',
    inputSchema: { type: 'object', properties: {} },
    isReadOnly: true,
    execute: async () => ({ output: '' }),
  });

  it('returns empty object for empty tools array', () => {
    assert.deepEqual(buildToolCacheBreakpoints([]), {});
  });

  it('marks the last tool with ephemeral cache', () => {
    const tools = [stubTool('bash'), stubTool('read'), stubTool('edit')];
    const points = buildToolCacheBreakpoints(tools);
    assert.deepEqual(points[2], { type: 'ephemeral' });
    assert.equal(points[0], undefined);
    assert.equal(points[1], undefined);
  });

  it('marks index 0 for single tool', () => {
    const points = buildToolCacheBreakpoints([stubTool('bash')]);
    assert.deepEqual(points[0], { type: 'ephemeral' });
  });

  it('returns exactly one entry', () => {
    const tools = [stubTool('a'), stubTool('b'), stubTool('c'), stubTool('d')];
    const points = buildToolCacheBreakpoints(tools);
    assert.equal(Object.keys(points).length, 1);
  });
});

// ---------------------------------------------------------------------------
// healOrphanedToolCalls
// ---------------------------------------------------------------------------
describe('healOrphanedToolCalls', () => {
  it('returns empty array for empty messages', () => {
    assert.deepEqual(healOrphanedToolCalls([]), []);
  });

  it('passes through messages without tool calls', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'hello' },
      { role: 'assistant', content: 'hi' },
    ];
    const result = healOrphanedToolCalls(msgs);
    assert.equal(result.length, 2);
  });

  it('passes through when all tool calls have matching results', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'test' },
      {
        role: 'assistant',
        content: null,
        tool_calls: [{ id: 'tc1', type: 'function', function: { name: 'bash', arguments: '{}' } }],
      },
      { role: 'tool', tool_call_id: 'tc1', content: 'done' },
    ];
    const result = healOrphanedToolCalls(msgs);
    assert.equal(result.length, 3);
  });

  it('drops an orphaned tool_result whose tool_call_id has no matching assistant call', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'test' },
      // orphaned tool_result (its assistant tool_call was truncated/compacted away)
      { role: 'tool', tool_call_id: 'ghost', content: 'stale result' },
      { role: 'assistant', content: 'ok' },
    ];
    const result = healOrphanedToolCalls(msgs);
    assert.equal(result.find(m => m.role === 'tool'), undefined, 'orphaned tool_result must be dropped');
    assert.equal(result.length, 2);
  });

  it('keeps a valid tool_result but drops an orphaned one in the same list', () => {
    const msgs: Message[] = [
      { role: 'assistant', content: null, tool_calls: [{ id: 'tc1', type: 'function', function: { name: 'bash', arguments: '{}' } }] },
      { role: 'tool', tool_call_id: 'tc1', content: 'valid' },
      { role: 'tool', tool_call_id: 'ghost', content: 'orphan' },
    ];
    const result = healOrphanedToolCalls(msgs);
    const toolMsgs = result.filter(m => m.role === 'tool') as Array<{ tool_call_id: string }>;
    assert.deepEqual(toolMsgs.map(m => m.tool_call_id), ['tc1']);
  });

  it('injects synthetic result for orphaned tool call', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'test' },
      {
        role: 'assistant',
        content: null,
        tool_calls: [{ id: 'orphan1', type: 'function', function: { name: 'bash', arguments: '{}' } }],
      },
    ];
    const result = healOrphanedToolCalls(msgs);
    assert.equal(result.length, 3);
    assert.equal(result[2].role, 'tool');
    const toolMsg = result[2] as { role: 'tool'; tool_call_id: string; content: string };
    assert.equal(toolMsg.tool_call_id, 'orphan1');
    assert.ok(toolMsg.content.includes('interrupted'));
  });

  it('heals multiple orphaned tool calls in one message', () => {
    const msgs: Message[] = [
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          { id: 'tc1', type: 'function', function: { name: 'bash', arguments: '{}' } },
          { id: 'tc2', type: 'function', function: { name: 'read', arguments: '{}' } },
        ],
      },
    ];
    const result = healOrphanedToolCalls(msgs);
    assert.equal(result.length, 3);
    assert.equal((result[1] as { tool_call_id: string }).tool_call_id, 'tc1');
    assert.equal((result[2] as { tool_call_id: string }).tool_call_id, 'tc2');
  });

  it('only heals orphans, not answered calls', () => {
    const msgs: Message[] = [
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          { id: 'answered', type: 'function', function: { name: 'bash', arguments: '{}' } },
          { id: 'orphan', type: 'function', function: { name: 'read', arguments: '{}' } },
        ],
      },
      { role: 'tool', tool_call_id: 'answered', content: 'result' },
    ];
    const result = healOrphanedToolCalls(msgs);
    assert.equal(result.length, 3);
    const toolIds = result.filter(m => m.role === 'tool').map(m => (m as { tool_call_id: string }).tool_call_id);
    assert.ok(toolIds.includes('answered'));
    assert.ok(toolIds.includes('orphan'));
  });

  it('does not add duplicates for already-answered calls', () => {
    const msgs: Message[] = [
      {
        role: 'assistant',
        content: null,
        tool_calls: [{ id: 'tc1', type: 'function', function: { name: 'bash', arguments: '{}' } }],
      },
      { role: 'tool', tool_call_id: 'tc1', content: 'ok' },
    ];
    const result = healOrphanedToolCalls(msgs);
    const toolMsgs = result.filter(m => m.role === 'tool');
    assert.equal(toolMsgs.length, 1);
  });
});

// ---------------------------------------------------------------------------
// stripEmptyTextBlocks
// ---------------------------------------------------------------------------
describe('stripEmptyTextBlocks', () => {
  it('removes text blocks with empty string text', () => {
    const msgs: Array<Record<string, unknown>> = [
      { role: 'assistant', content: [{ type: 'text', text: '' }, { type: 'text', text: 'hello' }] },
    ];
    stripEmptyTextBlocks(msgs);
    assert.equal((msgs[0].content as any[]).length, 1);
    assert.equal((msgs[0].content as any[])[0].text, 'hello');
  });

  it('removes text blocks with whitespace-only text', () => {
    const msgs: Array<Record<string, unknown>> = [
      { role: 'assistant', content: [{ type: 'text', text: '   \n\t  ' }, { type: 'tool_use', id: 'x' }] },
    ];
    stripEmptyTextBlocks(msgs);
    assert.equal((msgs[0].content as any[]).length, 1);
    assert.equal((msgs[0].content as any[])[0].type, 'tool_use');
  });

  it('removes text blocks with non-string text field', () => {
    const msgs: Array<Record<string, unknown>> = [
      { role: 'assistant', content: [{ type: 'text' }, { type: 'text', text: 'ok' }] },
    ];
    stripEmptyTextBlocks(msgs);
    assert.equal((msgs[0].content as any[]).length, 1);
    assert.equal((msgs[0].content as any[])[0].text, 'ok');
  });

  it('replaces empty assistant content array with (continued)', () => {
    const msgs: Array<Record<string, unknown>> = [
      { role: 'assistant', content: [{ type: 'text', text: '' }] },
    ];
    stripEmptyTextBlocks(msgs);
    assert.deepEqual(msgs[0].content, [{ type: 'text', text: '(continued)' }]);
  });

  it('does not replace empty user content array', () => {
    const msgs: Array<Record<string, unknown>> = [
      { role: 'user', content: [{ type: 'text', text: '' }] },
    ];
    stripEmptyTextBlocks(msgs);
    assert.equal((msgs[0].content as any[]).length, 0);
  });

  it('replaces empty string assistant content with (continued)', () => {
    const msgs: Array<Record<string, unknown>> = [
      { role: 'assistant', content: '' },
    ];
    stripEmptyTextBlocks(msgs);
    assert.equal(msgs[0].content, '(continued)');
  });

  it('replaces whitespace-only string assistant content with (continued)', () => {
    const msgs: Array<Record<string, unknown>> = [
      { role: 'assistant', content: '  \n  ' },
    ];
    stripEmptyTextBlocks(msgs);
    assert.equal(msgs[0].content, '(continued)');
  });

  it('does not modify non-empty string content', () => {
    const msgs: Array<Record<string, unknown>> = [
      { role: 'assistant', content: 'real text' },
    ];
    stripEmptyTextBlocks(msgs);
    assert.equal(msgs[0].content, 'real text');
  });

  it('does not replace empty string user content', () => {
    const msgs: Array<Record<string, unknown>> = [
      { role: 'user', content: '' },
    ];
    stripEmptyTextBlocks(msgs);
    assert.equal(msgs[0].content, '');
  });

  it('preserves non-text content blocks', () => {
    const msgs: Array<Record<string, unknown>> = [
      {
        role: 'assistant',
        content: [
          { type: 'tool_use', id: 'tc1', name: 'bash', input: {} },
          { type: 'text', text: 'done' },
        ],
      },
    ];
    stripEmptyTextBlocks(msgs);
    assert.equal((msgs[0].content as any[]).length, 2);
  });

  it('handles mixed messages in a single call', () => {
    const msgs: Array<Record<string, unknown>> = [
      { role: 'user', content: 'hi' },
      { role: 'assistant', content: [{ type: 'text', text: '' }] },
      { role: 'assistant', content: [{ type: 'text', text: 'ok' }] },
    ];
    stripEmptyTextBlocks(msgs);
    assert.deepEqual(msgs[1].content, [{ type: 'text', text: '(continued)' }]);
    assert.equal((msgs[2].content as any[])[0].text, 'ok');
  });
});
