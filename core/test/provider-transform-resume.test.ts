import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { healOrphanedToolCalls } from '../src/provider-transform';
import type { Message } from '../src/types';

const ORPHAN_PLACEHOLDER = '[Orphaned tool result removed due to conversation resume]';

describe('healOrphanedToolCalls — resume-safe leading orphan', () => {
  it('replaces a transcript that would start with an assistant turn with a user placeholder', () => {
    // On resume mid-turn the user turn + its assistant tool_call were dropped by
    // compaction, leaving a leading orphaned tool_result followed by an assistant.
    const messages: Message[] = [
      { role: 'tool', tool_call_id: 'gone', content: 'result for a dropped call' },
      { role: 'assistant', content: 'continuing…' },
      { role: 'user', content: 'next' },
    ];
    const healed = healOrphanedToolCalls(messages);
    assert.equal(healed[0].role, 'user', 'payload must start with a user turn');
    assert.equal((healed[0] as { content: string }).content, ORPHAN_PLACEHOLDER);
    // The orphaned tool_result is gone.
    assert.ok(!healed.some(m => m.role === 'tool' && m.tool_call_id === 'gone'));
  });

  it('does not add a placeholder when the transcript already leads with a user turn', () => {
    const messages: Message[] = [
      { role: 'tool', tool_call_id: 'gone', content: 'orphan' },
      { role: 'user', content: 'hello' },
      { role: 'assistant', content: 'hi' },
    ];
    const healed = healOrphanedToolCalls(messages);
    assert.equal(healed[0].role, 'user');
    assert.equal((healed[0] as { content: string }).content, 'hello');
    assert.ok(!healed.some(m => (m as { content?: unknown }).content === ORPHAN_PLACEHOLDER));
  });

  it('leaves a well-formed transcript untouched', () => {
    const messages: Message[] = [
      { role: 'user', content: 'go' },
      { role: 'assistant', content: null, tool_calls: [{ id: 'a', type: 'function', function: { name: 'x', arguments: '{}' } }] },
      { role: 'tool', tool_call_id: 'a', content: 'ok' },
    ];
    const healed = healOrphanedToolCalls(messages);
    assert.equal(healed.length, 3);
    assert.equal(healed[0].role, 'user');
    assert.ok(!healed.some(m => (m as { content?: unknown }).content === ORPHAN_PLACEHOLDER));
  });
});
