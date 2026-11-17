import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { mapChildToSubagentEvent, toolActivitySummary, toolVerb } from '../src/tools/subagent-relay';
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

  it('maps tool_use_start (no salient input) → verb-only detail + structured lastToolName', () => {
    const r = map({ type: 'tool_use_start', toolName: 'grep', toolCallId: 'c1', input: {} });
    assert.deepEqual(r, { type: 'subagent_progress', nodeId: 'agent-1', depth: 2, activity: 'tool', detail: 'Searching', lastToolName: 'grep' });
  });

  it('maps tool_use_start → human-readable "verb + target" detail (parity: toolSummary)', () => {
    assert.equal((map({ type: 'tool_use_start', toolName: 'bash', toolCallId: 'c', input: { command: 'npm test' } }) as any).detail, 'Running npm test');
    assert.equal((map({ type: 'tool_use_start', toolName: 'file_read', toolCallId: 'c', input: { file_path: 'src/x.ts' } }) as any).detail, 'Reading src/x.ts');
    assert.equal((map({ type: 'tool_use_start', toolName: 'grep', toolCallId: 'c', input: { pattern: 'TODO' } }) as any).detail, 'Searching TODO');
    assert.equal((map({ type: 'tool_use_start', toolName: 'web_fetch', toolCallId: 'c', input: { url: 'https://x.dev' } }) as any).detail, 'Fetching https://x.dev');
  });

  it('maps tool_use_start for an unmapped tool → bare tool name as the verb', () => {
    assert.equal((map({ type: 'tool_use_start', toolName: 'config', toolCallId: 'c', input: {} }) as any).detail, 'config');
    assert.equal((map({ type: 'tool_use_start', toolName: 'config', toolCallId: 'c', input: { command: 'get model' } }) as any).detail, 'config get model');
  });

  it('maps tool_use_result → subagent_progress(tool) with the verb + status glyph', () => {
    assert.equal((map({ type: 'tool_use_result', toolCallId: 'c', toolName: 'bash', output: 'ok', isError: false }) as any).detail, 'Running ✓');
    // On failure the bounded failure reason (first output line) is appended after the glyph.
    assert.equal((map({ type: 'tool_use_result', toolCallId: 'c', toolName: 'bash', output: 'x', isError: true }) as any).detail, 'Running ✗ — x');
    const r = map({ type: 'tool_use_result', toolCallId: 'c', toolName: 'grep', output: 'ok', isError: false });
    assert.equal(r?.type === 'subagent_progress' && r.lastToolName, 'grep');
  });

  it('toolActivitySummary / toolVerb are pure building blocks', () => {
    assert.equal(toolVerb('bash'), 'Running');
    assert.equal(toolVerb('notebook_edit'), 'Editing notebook');
    assert.equal(toolVerb('nope'), 'nope');
    assert.equal(toolActivitySummary('bash', { command: 'ls -la' }), 'Running ls -la');
    assert.equal(toolActivitySummary('grep', {}), 'Searching');
    // long targets truncate to the summarizeToolInput cap (keeps the roster compact)
    assert.ok(toolActivitySummary('bash', { command: 'x'.repeat(200) }).endsWith('...'));
  });

  it('drops empty text/thinking deltas', () => {
    assert.equal(map({ type: 'text_delta', text: '' }), null);
    assert.equal(map({ type: 'thinking_delta', text: '' }), null);
  });

  it('does not relay unrelated events', () => {
    assert.equal(map({ type: 'usage_update', stats: {} as any }), null);
    assert.equal(map({ type: 'done', totalTurns: 1, exitReason: 'completed' }), null);
    // tool_use_progress IS relayed now (see the dedicated live-progress test).
    assert.equal(map({ type: 'tool_use_progress', toolCallId: 'c', data: '   ' }), null);
  });

  it('tags with the provided nodeId and depth', () => {
    const r = mapChildToSubagentEvent({ type: 'text_delta', text: 'hi' }, 'node-9', 3);
    assert.equal(r?.type === 'subagent_progress' && r.nodeId, 'node-9');
    assert.equal(r?.type === 'subagent_progress' && r.depth, 3);
  });
});
