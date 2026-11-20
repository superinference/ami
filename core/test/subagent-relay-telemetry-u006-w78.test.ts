import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  mapChildToSubagentEvent,
  attachSubagentUsage,
} from '../src/tools/subagent-relay';
import type { EngineEvent, TaskUsageSnapshot } from '../src/types';

/**
 * U006 (w78) — live sub-agent progress telemetry.
 *
 * The parent's live roster/inspector reads the structured `lastToolName` and the
 * cumulative `usage` snapshot off each relayed `subagent_progress` event
 * (cli/src/subagent-roster.ts). Before this unit the relay never populated either,
 * so the roster had to re-parse the free-text detail and could never show a running
 * sub-agent's live token spend or wall-clock. These tests lock the emitter side.
 */
describe('mapChildToSubagentEvent — structured lastToolName (parity: last_tool_name)', () => {
  it('stamps lastToolName on a tool_use_start activity', () => {
    const r = mapChildToSubagentEvent(
      { type: 'tool_use_start', toolName: 'file_read', toolCallId: 'c1', input: { file_path: 'a.ts' } },
      'agent-1',
      2,
    );
    assert.equal(r?.type === 'subagent_progress' && r.lastToolName, 'file_read');
    // The structured name is authoritative over the free-text detail, which carries a
    // human-readable "verb + target" line (parity: the reference's toolSummary: Read->Reading).
    assert.equal(r?.type === 'subagent_progress' && r.detail, 'Reading a.ts');
  });

  it('stamps lastToolName on a tool_use_result activity', () => {
    const r = mapChildToSubagentEvent(
      { type: 'tool_use_result', toolCallId: 'c', toolName: 'bash', output: 'ok', isError: false },
      'agent-1',
      2,
    );
    assert.equal(r?.type === 'subagent_progress' && r.lastToolName, 'bash');
  });

  it('does NOT add lastToolName to non-tool (text/thinking) activities', () => {
    const t = mapChildToSubagentEvent({ type: 'text_delta', text: 'hi' }, 'agent-1', 2);
    assert.equal(t?.type === 'subagent_progress' && t.lastToolName, undefined);
    const k = mapChildToSubagentEvent({ type: 'thinking_delta', text: 'hmm' }, 'agent-1', 2);
    assert.equal(k?.type === 'subagent_progress' && k.lastToolName, undefined);
  });

  it('suppresses the internal synthetic-output tool (no event, so no lastToolName)', () => {
    assert.equal(
      mapChildToSubagentEvent(
        { type: 'tool_use_start', toolName: 'structured_output', toolCallId: 'c', input: {} },
        'agent-1',
        2,
      ),
      null,
    );
  });
});

describe('attachSubagentUsage — live cumulative usage snapshot (parity: task_progress usage)', () => {
  const usage: TaskUsageSnapshot = { totalTokens: 1234, toolUses: 3, durationMs: 5000 };

  it('stamps usage onto a subagent_progress event without mutating the input', () => {
    const base = mapChildToSubagentEvent(
      { type: 'tool_use_start', toolName: 'grep', toolCallId: 'c', input: {} },
      'n',
      1,
    );
    const out = attachSubagentUsage(base, usage);
    assert.equal(out?.type === 'subagent_progress' && out.usage?.totalTokens, 1234);
    assert.equal(out?.type === 'subagent_progress' && out.usage?.toolUses, 3);
    assert.equal(out?.type === 'subagent_progress' && out.usage?.durationMs, 5000);
    // purity: original untouched + a new object returned
    assert.equal(base?.type === 'subagent_progress' && base.usage, undefined);
    assert.notEqual(out, base);
    // the structured lastToolName survives the stamp
    assert.equal(out?.type === 'subagent_progress' && out.lastToolName, 'grep');
  });

  it('passes a null (dropped) event straight through', () => {
    assert.equal(attachSubagentUsage(null, usage), null);
  });

  it('leaves a non-progress event untouched (identity passthrough)', () => {
    const done: EngineEvent = { type: 'done', totalTurns: 1, exitReason: 'completed' };
    assert.equal(attachSubagentUsage(done, usage), done);
  });
});
