import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import type { Message } from '../src/types';

import {
  extractFilesModified,
  estimateCompactionMessageTokens,
  dropOrphanedToolResults,
  selectRecentWithinBudget,
  buildTruncationMarker,
} from '../src/conversation-summary';

// ---------------------------------------------------------------------------
// GAP 1 (functional fix): extractFilesModified must resolve the mutated path
// from `notebook_path` / `path`, not only `file_path`. The real notebook_edit
// tool writes `notebook_path`. RED on old code: the file was never recorded.
// ---------------------------------------------------------------------------
describe('extractFilesModified — notebook_path / path resolution', () => {
  it('records notebook_edit files that use notebook_path (not file_path)', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'notebook_edit', arguments: '{"notebook_path":"/tmp/real.ipynb","new_source":"x"}' } },
      ] },
    ];
    const files = extractFilesModified(messages);
    // RED on old implementation (only read file_path): size 0, no entry.
    assert.ok(files.has('/tmp/real.ipynb'));
    assert.equal(files.size, 1);
  });

  it('records a mutating tool that uses the `path` key', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'file_write', arguments: '{"path":"/tmp/via-path.ts"}' } },
      ] },
    ];
    const files = extractFilesModified(messages);
    assert.ok(files.has('/tmp/via-path.ts'));
  });

  it('still prefers file_path when present (superset preserved)', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'file_edit', arguments: '{"file_path":"/tmp/a.ts","path":"/tmp/other.ts"}' } },
      ] },
    ];
    const files = extractFilesModified(messages);
    assert.ok(files.has('/tmp/a.ts'));
    assert.ok(!files.has('/tmp/other.ts'));
  });

  it('ignores a non-string path value', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'file_edit', arguments: '{"file_path":123}' } },
      ] },
    ];
    assert.equal(extractFilesModified(messages).size, 0);
  });
});

// ---------------------------------------------------------------------------
// GAP 2: estimateMessageTokens — per-message token cost for budgeting.
// ---------------------------------------------------------------------------
describe('estimateCompactionMessageTokens', () => {
  it('estimates a non-trivial cost from string content', () => {
    const t = estimateCompactionMessageTokens({ role: 'user', content: 'a'.repeat(40) });
    assert.ok(t >= 10); // ~10 content tokens + per-message overhead
  });

  it('adds tool-call cost on top of content for assistant messages', () => {
    const noCall = estimateCompactionMessageTokens({ role: 'assistant', content: 'hi' });
    const withCall = estimateCompactionMessageTokens({ role: 'assistant', content: 'hi', tool_calls: [
      { id: '1', type: 'function', function: { name: 'bash', arguments: '{"command":"ls -la /very/long/path"}' } },
    ] });
    // RED on the canonical estimateMessageTokens (ignores tool_calls): equal.
    assert.ok(withCall > noCall);
  });

  it('prices a tool-heavy turn with null content above bare overhead', () => {
    const bare = estimateCompactionMessageTokens({ role: 'assistant', content: null });
    const toolTurn = estimateCompactionMessageTokens({ role: 'assistant', content: null, tool_calls: [
      { id: '1', type: 'function', function: { name: 'file_edit', arguments: '{"file_path":"/x","new_string":"' + 'y'.repeat(80) + '"}' } },
    ] });
    assert.ok(toolTurn > bare);
  });

  it('falls back to raw length when tool arguments are not valid JSON', () => {
    const t = estimateCompactionMessageTokens({ role: 'assistant', content: null, tool_calls: [
      { id: '1', type: 'function', function: { name: 'bash', arguments: 'not json at all' } },
    ] });
    assert.ok(t > estimateCompactionMessageTokens({ role: 'assistant', content: null }));
  });
});

// ---------------------------------------------------------------------------
// GAP 3: dropOrphanedToolResults — remove tool results whose assistant call
// is absent from the window (provider-breaking after truncation).
// ---------------------------------------------------------------------------
describe('dropOrphanedToolResults', () => {
  it('drops a tool result with no matching assistant tool_call', () => {
    const messages: Message[] = [
      { role: 'tool', tool_call_id: 'gone', content: 'orphan' },
      { role: 'assistant', content: 'later' },
    ];
    const repaired = dropOrphanedToolResults(messages);
    assert.equal(repaired.length, 1);
    assert.equal(repaired[0].role, 'assistant');
  });

  it('keeps a tool result paired with its assistant call', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: 'c1', type: 'function', function: { name: 'bash', arguments: '{}' } },
      ] },
      { role: 'tool', tool_call_id: 'c1', content: 'ok' },
    ];
    assert.equal(dropOrphanedToolResults(messages).length, 2);
  });

  it('does not mutate the input array', () => {
    const messages: Message[] = [{ role: 'tool', tool_call_id: 'x', content: 'o' }];
    dropOrphanedToolResults(messages);
    assert.equal(messages.length, 1);
  });
});

// ---------------------------------------------------------------------------
// GAP 4: selectRecentWithinBudget — recent-tail selection under a token budget,
// never empty, pinned turns, tool-pair repair at the boundary.
// ---------------------------------------------------------------------------
describe('selectRecentWithinBudget', () => {
  it('returns empty selection for empty input', () => {
    const r = selectRecentWithinBudget([], 100);
    assert.deepEqual(r.kept, []);
    assert.equal(r.droppedCount, 0);
  });

  it('always keeps the most recent message even when it exceeds the budget', () => {
    const messages: Message[] = [
      { role: 'user', content: 'a'.repeat(400) },
    ];
    const r = selectRecentWithinBudget(messages, 0);
    assert.equal(r.kept.length, 1);
    assert.equal(r.droppedCount, 0);
  });

  it('keeps a contiguous recent window within budget and drops older ones', () => {
    // each message ~14 tokens (40 chars + overhead). budget 30 → keep last 2.
    const messages: Message[] = [
      { role: 'user', content: 'a'.repeat(40) },
      { role: 'assistant', content: 'b'.repeat(40) },
      { role: 'user', content: 'c'.repeat(40) },
    ];
    const r = selectRecentWithinBudget(messages, 30);
    assert.equal(r.kept.length, 2);
    assert.equal(r.droppedCount, 1);
    assert.equal((r.kept[0] as any).content, 'b'.repeat(40));
    assert.equal((r.kept[1] as any).content, 'c'.repeat(40));
  });

  it('repairs orphaned tool results exposed at the truncation boundary', () => {
    // budget keeps only the last 2 messages, which would start with a tool
    // result whose assistant call (index 0) is dropped → must be repaired.
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: 'c1', type: 'function', function: { name: 'bash', arguments: '{"command":"' + 'x'.repeat(60) + '"}' } },
      ] },
      { role: 'tool', tool_call_id: 'c1', content: 'r'.repeat(40) },
      { role: 'assistant', content: 'final answer' },
    ];
    // Budget 30 includes the tool result + final answer but NOT the large
    // assistant tool_call turn, so the tool result is exposed as an orphan and
    // must be repaired out. RED without dropOrphanedToolResults: it stays.
    const r = selectRecentWithinBudget(messages, 30);
    // The orphaned tool result must NOT be in the kept window.
    assert.ok(!r.kept.some((m) => m.role === 'tool'));
    assert.ok(r.kept.some((m) => m.role === 'assistant' && (m as any).content === 'final answer'));
  });

  it('keeps pinned messages outside the recent window', () => {
    const messages: Message[] = [
      { role: 'user', content: 'p'.repeat(40) },   // pinned, index 0
      { role: 'assistant', content: 'm'.repeat(40) },
      { role: 'user', content: 'n'.repeat(40) },
    ];
    const r = selectRecentWithinBudget(messages, 15, { pin: (_m, i) => i === 0 });
    assert.ok(r.kept.some((m) => (m as any).content === 'p'.repeat(40)));
  });
});

// ---------------------------------------------------------------------------
// GAP 5: buildTruncationMarker — marker string for elided messages.
// ---------------------------------------------------------------------------
describe('buildTruncationMarker', () => {
  it('returns null when nothing was dropped', () => {
    assert.equal(buildTruncationMarker(0), null);
    assert.equal(buildTruncationMarker(-3), null);
  });

  it('uses singular for one message', () => {
    assert.equal(buildTruncationMarker(1), '[1 earlier message omitted during context compaction]');
  });

  it('uses plural for multiple messages', () => {
    assert.equal(buildTruncationMarker(5), '[5 earlier messages omitted during context compaction]');
  });
});
