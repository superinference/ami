import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { mapChildToSubagentEvent } from '../src/tools/subagent-relay';
import type { EngineEvent } from '../src/types';

const map = (e: EngineEvent) => mapChildToSubagentEvent(e, 'agent-1', 2);

describe('mapChildToSubagentEvent', () => {
  it('maps text_delta → subagent_progress(text)', () => {
    const r = map({ type: 'text_delta', text: 'hello' });
    assert.deepEqual(r, { type: 'subagent_progress', nodeId: 'agent-1', depth: 2, activity: 'text', detail: 'hello' });
  });

  it('maps thinking_delta → subagent_progress(thinking)', () => {
    const r = map({ type: 'thinking_delta', text: 'hmm' });
    assert.equal(r?.type === 'subagent_progress' && r.activity, 'thinking');
  });

  it('maps tool_use_start → subagent_progress(tool) with the tool name', () => {
    const r = map({ type: 'tool_use_start', toolName: 'grep', toolCallId: 'c1', input: {} });
    assert.deepEqual(r, { type: 'subagent_progress', nodeId: 'agent-1', depth: 2, activity: 'tool', detail: 'grep' });
  });

  it('maps tool_use_result → subagent_progress(tool) with a status glyph', () => {
    assert.equal(map({ type: 'tool_use_result', toolCallId: 'c', toolName: 'bash', output: 'ok', isError: false })?.type === 'subagent_progress'
      && (map({ type: 'tool_use_result', toolCallId: 'c', toolName: 'bash', output: 'ok', isError: false }) as any).detail, 'bash ✓');
    assert.equal((map({ type: 'tool_use_result', toolCallId: 'c', toolName: 'bash', output: 'x', isError: true }) as any).detail, 'bash ✗');
  });

  it('drops empty text/thinking deltas', () => {
    assert.equal(map({ type: 'text_delta', text: '' }), null);
    assert.equal(map({ type: 'thinking_delta', text: '' }), null);
  });

  it('does not relay unrelated events', () => {
    assert.equal(map({ type: 'usage_update', stats: {} as any }), null);
    assert.equal(map({ type: 'done', totalTurns: 1, exitReason: 'completed' }), null);
    assert.equal(map({ type: 'tool_use_progress', toolCallId: 'c', data: 'x' }), null);
  });

  it('tags with the provided nodeId and depth', () => {
    const r = mapChildToSubagentEvent({ type: 'text_delta', text: 'hi' }, 'node-9', 3);
    assert.equal(r?.type === 'subagent_progress' && r.nodeId, 'node-9');
    assert.equal(r?.type === 'subagent_progress' && r.depth, 3);
  });
});
