import { test } from 'node:test';
import assert from 'node:assert/strict';

import { TurnMetrics } from '../src/turn-metrics';
import type { TurnSummary } from '../src/turn-metrics';
import { ToolAnalytics } from '../src/tool-analytics';
import type { ToolStats } from '../src/tool-analytics';
import {
  countToolUses,
  extractFilesModified,
  extractConversationMeta,
  getConversationDuration,
} from '../src/conversation-summary';
import type { Message, AssistantMessage, ToolCallContent } from '../src/types';

// ---------------------------------------------------------------------------
// Helpers to craft real, type-correct message arrays.
// ---------------------------------------------------------------------------

function toolCall(id: string, name: string, args: Record<string, unknown> = {}): ToolCallContent {
  return { id, type: 'function', function: { name, arguments: JSON.stringify(args) } };
}

function assistant(content: string | null, tool_calls?: ToolCallContent[]): AssistantMessage {
  return tool_calls ? { role: 'assistant', content, tool_calls } : { role: 'assistant', content };
}

// ===========================================================================
// TurnMetrics
// ===========================================================================

test('TurnMetrics: fresh instance reports zeroed, guard-safe summary', () => {
  const m = new TurnMetrics();
  const s: TurnSummary = m.getSummary();
  assert.deepEqual(s, {
    turnCount: 0,
    totalToolCalls: 0,
    totalTokens: 0,
    totalDurationMs: 0,
    avgToolCallsPerTurn: 0,
    avgTokensPerTurn: 0,
    tokensPerSecond: 0,
    longestTurnMs: 0,
  });
  // Getters agree with the summary.
  assert.equal(m.turnCount, 0);
  assert.equal(m.totalToolCalls, 0);
  assert.equal(m.totalTokens, 0);
  assert.equal(m.totalDurationMs, 0);
  assert.equal(m.longestTurnMs, 0);
  assert.equal(m.avgToolCallsPerTurn, 0);
  assert.equal(m.avgTokensPerTurn, 0);
  assert.equal(m.tokensPerSecond, 0);
});

test('TurnMetrics: single turn computes averages and rate from observed accumulation', () => {
  const m = new TurnMetrics();
  m.recordTurn({ toolCalls: 3, tokensUsed: 2000, durationMs: 1000 });
  assert.equal(m.turnCount, 1);
  assert.equal(m.totalToolCalls, 3);
  assert.equal(m.totalTokens, 2000);
  assert.equal(m.totalDurationMs, 1000);
  assert.equal(m.avgToolCallsPerTurn, 3);
  assert.equal(m.avgTokensPerTurn, 2000);
  // 2000 tokens over 1000ms => 2000 tokens/sec.
  assert.equal(m.tokensPerSecond, 2000);
  assert.equal(m.longestTurnMs, 1000);
});

test('TurnMetrics: multiple turns aggregate totals and averages correctly', () => {
  const m = new TurnMetrics();
  m.recordTurn({ toolCalls: 2, tokensUsed: 100, durationMs: 400 });
  m.recordTurn({ toolCalls: 4, tokensUsed: 300, durationMs: 600 });
  assert.equal(m.turnCount, 2);
  assert.equal(m.totalToolCalls, 6);
  assert.equal(m.totalTokens, 400);
  assert.equal(m.totalDurationMs, 1000);
  assert.equal(m.avgToolCallsPerTurn, 3); // 6/2
  assert.equal(m.avgTokensPerTurn, 200); // 400/2
  // 400 tokens over 1000ms => 400 tokens/sec.
  assert.equal(m.tokensPerSecond, 400);
});

test('TurnMetrics: longestTurnMs tracks the max, not the last', () => {
  const m = new TurnMetrics();
  m.recordTurn({ toolCalls: 0, tokensUsed: 0, durationMs: 500 });
  m.recordTurn({ toolCalls: 0, tokensUsed: 0, durationMs: 1500 });
  m.recordTurn({ toolCalls: 0, tokensUsed: 0, durationMs: 800 });
  assert.equal(m.longestTurnMs, 1500);
});

test('TurnMetrics: zero-duration turns keep tokensPerSecond at 0 (no divide-by-zero)', () => {
  const m = new TurnMetrics();
  m.recordTurn({ toolCalls: 1, tokensUsed: 500, durationMs: 0 });
  assert.equal(m.totalDurationMs, 0);
  assert.equal(m.tokensPerSecond, 0);
  assert.equal(m.longestTurnMs, 0);
});

test('TurnMetrics: reset returns to pristine empty state', () => {
  const m = new TurnMetrics();
  m.recordTurn({ toolCalls: 5, tokensUsed: 9000, durationMs: 3000 });
  m.reset();
  assert.deepEqual(m.getSummary(), new TurnMetrics().getSummary());
  assert.equal(m.turnCount, 0);
  assert.equal(m.longestTurnMs, 0);
  // Recording after reset starts from zero again.
  m.recordTurn({ toolCalls: 1, tokensUsed: 10, durationMs: 100 });
  assert.equal(m.turnCount, 1);
  assert.equal(m.totalTokens, 10);
});

test('TurnMetrics: getSummary snapshot is immutable to later records', () => {
  const m = new TurnMetrics();
  m.recordTurn({ toolCalls: 1, tokensUsed: 10, durationMs: 100 });
  const snap = m.getSummary();
  m.recordTurn({ toolCalls: 1, tokensUsed: 10, durationMs: 100 });
  // Earlier snapshot reflects state at capture time.
  assert.equal(snap.turnCount, 1);
  assert.equal(m.turnCount, 2);
});

// ===========================================================================
// ToolAnalytics
// ===========================================================================

test('ToolAnalytics: unknown tool returns zeroed stats keyed by name', () => {
  const a = new ToolAnalytics();
  const s: ToolStats = a.getStats('nope');
  assert.deepEqual(s, {
    tool: 'nope',
    callCount: 0,
    errorCount: 0,
    totalDurationMs: 0,
    avgDurationMs: 0,
    minDurationMs: 0,
    maxDurationMs: 0,
    errorRate: 0,
  });
});

test('ToolAnalytics: records counts, durations, min/max, and error rate', () => {
  const a = new ToolAnalytics();
  a.record('bash', 100, false);
  a.record('bash', 300, true);
  a.record('bash', 200, false);
  const s = a.getStats('bash');
  assert.equal(s.callCount, 3);
  assert.equal(s.errorCount, 1);
  assert.equal(s.totalDurationMs, 600);
  assert.equal(s.avgDurationMs, 200);
  assert.equal(s.minDurationMs, 100);
  assert.equal(s.maxDurationMs, 300);
  assert.equal(s.errorRate, 1 / 3);
});

test('ToolAnalytics: single successful call yields 0 error rate and equal min/max', () => {
  const a = new ToolAnalytics();
  a.record('grep', 42, false);
  const s = a.getStats('grep');
  assert.equal(s.callCount, 1);
  assert.equal(s.errorCount, 0);
  assert.equal(s.errorRate, 0);
  assert.equal(s.minDurationMs, 42);
  assert.equal(s.maxDurationMs, 42);
  assert.equal(s.avgDurationMs, 42);
});

test('ToolAnalytics: all-error tool reports errorRate 1', () => {
  const a = new ToolAnalytics();
  a.record('web_fetch', 10, true);
  a.record('web_fetch', 30, true);
  const s = a.getStats('web_fetch');
  assert.equal(s.errorRate, 1);
  assert.equal(s.errorCount, 2);
});

test('ToolAnalytics: zero-duration call keeps minDurationMs at 0, not Infinity', () => {
  const a = new ToolAnalytics();
  a.record('file_edit', 0, false);
  const s = a.getStats('file_edit');
  assert.equal(s.minDurationMs, 0);
  assert.equal(s.maxDurationMs, 0);
  assert.equal(s.totalDurationMs, 0);
  assert.equal(s.avgDurationMs, 0);
});

test('ToolAnalytics: getSummary sorts tools by descending call count', () => {
  const a = new ToolAnalytics();
  a.record('rare', 1, false);
  a.record('common', 1, false);
  a.record('common', 1, false);
  a.record('common', 1, false);
  a.record('mid', 1, false);
  a.record('mid', 1, false);
  const summary = a.getSummary();
  assert.deepEqual(summary.map(s => s.tool), ['common', 'mid', 'rare']);
  assert.deepEqual(summary.map(s => s.callCount), [3, 2, 1]);
});

test('ToolAnalytics: totalCalls and totalErrors aggregate across tools', () => {
  const a = new ToolAnalytics();
  a.record('bash', 10, false);
  a.record('bash', 10, true);
  a.record('grep', 10, false);
  a.record('grep', 10, true);
  a.record('grep', 10, true);
  assert.equal(a.totalCalls, 5);
  assert.equal(a.totalErrors, 3);
});

test('ToolAnalytics: empty instance has zero totals and empty summary', () => {
  const a = new ToolAnalytics();
  assert.equal(a.totalCalls, 0);
  assert.equal(a.totalErrors, 0);
  assert.deepEqual(a.getSummary(), []);
});

test('ToolAnalytics: reset clears all recorded entries', () => {
  const a = new ToolAnalytics();
  a.record('bash', 50, true);
  a.reset();
  assert.equal(a.totalCalls, 0);
  assert.equal(a.totalErrors, 0);
  assert.deepEqual(a.getSummary(), []);
  assert.equal(a.getStats('bash').callCount, 0);
});

test('ToolAnalytics: min tracks the smallest of out-of-order durations', () => {
  const a = new ToolAnalytics();
  a.record('t', 500, false);
  a.record('t', 50, false);
  a.record('t', 900, false);
  const s = a.getStats('t');
  assert.equal(s.minDurationMs, 50);
  assert.equal(s.maxDurationMs, 900);
});

// ===========================================================================
// conversation-summary
// ===========================================================================

test('countToolUses: tallies tool_calls across assistant messages', () => {
  const msgs: Message[] = [
    { role: 'user', content: 'hi' },
    assistant(null, [toolCall('1', 'bash'), toolCall('2', 'grep')]),
    assistant(null, [toolCall('3', 'bash')]),
    assistant('done'),
  ];
  const counts = countToolUses(msgs);
  assert.equal(counts.get('bash'), 2);
  assert.equal(counts.get('grep'), 1);
  assert.equal(counts.size, 2);
});

test('countToolUses: empty and no-tool conversations return empty map', () => {
  assert.equal(countToolUses([]).size, 0);
  const noTools: Message[] = [
    { role: 'user', content: 'hey' },
    assistant('plain text, no tools'),
    { role: 'system', content: 'sys' },
  ];
  assert.equal(countToolUses(noTools).size, 0);
});

test('extractFilesModified: collects file_path only from mutating tools', () => {
  const msgs: Message[] = [
    assistant(null, [
      toolCall('1', 'file_edit', { file_path: '/a.ts' }),
      toolCall('2', 'file_write', { file_path: '/b.ts' }),
      toolCall('3', 'notebook_edit', { file_path: '/n.ipynb' }),
      toolCall('4', 'multi_edit', { file_path: '/c.ts' }),
      // Non-mutating tool with a file_path must be ignored.
      toolCall('5', 'grep', { file_path: '/ignored.ts' }),
      // Bash reading a file is not a mutation record.
      toolCall('6', 'bash', { command: 'cat /also-ignored.ts' }),
    ]),
  ];
  const files = extractFilesModified(msgs);
  assert.deepEqual([...files].sort(), ['/a.ts', '/b.ts', '/c.ts', '/n.ipynb']);
});

test('extractFilesModified: dedupes repeated paths and skips missing/invalid args', () => {
  const msgs: Message[] = [
    assistant(null, [
      toolCall('1', 'file_edit', { file_path: '/same.ts' }),
      toolCall('2', 'file_edit', { file_path: '/same.ts' }),
      // Missing file_path on a mutating tool => contributes nothing.
      toolCall('3', 'file_write', { content: 'x' }),
    ]),
    // Malformed JSON arguments must be swallowed, not thrown.
    { role: 'assistant', content: null, tool_calls: [
      { id: '4', type: 'function', function: { name: 'file_edit', arguments: '{not json' } },
    ] },
  ];
  const files = extractFilesModified(msgs);
  assert.deepEqual([...files], ['/same.ts']);
});

test('extractFilesModified: empty on no mutating activity', () => {
  assert.equal(extractFilesModified([]).size, 0);
  assert.equal(extractFilesModified([assistant('hello')]).size, 0);
});

test('extractConversationMeta: aggregates turn counts, tool totals, files, and uses', () => {
  const msgs: Message[] = [
    { role: 'user', content: 'q1' },
    assistant(null, [toolCall('1', 'bash'), toolCall('2', 'file_edit', { file_path: '/x.ts' })]),
    { role: 'tool', tool_call_id: '1', content: 'ok' },
    { role: 'user', content: 'q2' },
    assistant('final answer'),
    { role: 'system', content: 'ignored for turn counts' },
  ];
  const meta = extractConversationMeta(msgs);
  assert.equal(meta.userTurnCount, 2);
  assert.equal(meta.assistantTurnCount, 2);
  assert.equal(meta.totalToolCalls, 2);
  assert.equal(meta.toolUseCounts.get('bash'), 1);
  assert.equal(meta.toolUseCounts.get('file_edit'), 1);
  assert.deepEqual([...meta.filesModified], ['/x.ts']);
});

test('extractConversationMeta: degenerate empty conversation', () => {
  const meta = extractConversationMeta([]);
  assert.equal(meta.userTurnCount, 0);
  assert.equal(meta.assistantTurnCount, 0);
  assert.equal(meta.totalToolCalls, 0);
  assert.equal(meta.toolUseCounts.size, 0);
  assert.equal(meta.filesModified.size, 0);
});

test('extractConversationMeta: totalToolCalls counts calls, toolUseCounts groups by name', () => {
  const msgs: Message[] = [
    assistant(null, [toolCall('1', 'bash'), toolCall('2', 'bash'), toolCall('3', 'grep')]),
  ];
  const meta = extractConversationMeta(msgs);
  assert.equal(meta.totalToolCalls, 3);
  assert.equal(meta.toolUseCounts.get('bash'), 2);
  assert.equal(meta.toolUseCounts.get('grep'), 1);
});

test('getConversationDuration: returns delta between first and last _timestamp', () => {
  const first: any = { role: 'user', content: 'a', _timestamp: 1000 };
  const last: any = { role: 'assistant', content: 'b', _timestamp: 5200 };
  const mid: any = { role: 'assistant', content: 'm', _timestamp: 3000 };
  assert.equal(getConversationDuration([first, mid, last]), 4200);
});

test('getConversationDuration: 0 for fewer than two messages', () => {
  assert.equal(getConversationDuration([]), 0);
  assert.equal(getConversationDuration([{ role: 'user', content: 'solo' } as Message]), 0);
});

test('getConversationDuration: 0 when timestamps are absent', () => {
  const msgs: Message[] = [
    { role: 'user', content: 'a' },
    { role: 'assistant', content: 'b' },
  ];
  assert.equal(getConversationDuration(msgs), 0);
});

test('getConversationDuration: 0 when only one endpoint has a timestamp', () => {
  const withFirst: Message[] = [
    { role: 'user', content: 'a', _timestamp: 1000 } as any,
    { role: 'assistant', content: 'b' },
  ];
  const withLast: Message[] = [
    { role: 'user', content: 'a' },
    { role: 'assistant', content: 'b', _timestamp: 9000 } as any,
  ];
  assert.equal(getConversationDuration(withFirst), 0);
  assert.equal(getConversationDuration(withLast), 0);
});
