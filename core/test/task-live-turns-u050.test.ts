import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { taskTool, resetAgentRegistry } from '../src/tools/task';
import type { ToolContext, EngineConfig, EngineEvent } from '../src/types';

/**
 * U050 — END-TO-END: a subagent's live-display state (PARITY: the reference's DreamTask
 * phase + bounded turn buffer) is tracked through the REAL task tool event loop and
 * surfaced in the terminated/capped completion introspection footer. Drives the
 * actual taskTool.execute → foreground subagent loop with a fake sub-engine.
 *
 * RED/GREEN: these assert the `[phase: ...]` / `[last turn: ...]` footer that only
 * exists once background-live-turns is wired into task.ts — reverting the wiring
 * drops the footer and fails every case here.
 */
describe('U050 task tool — live-turn phase + retention footer (E2E)', () => {
  beforeEach(() => resetAgentRegistry());

  const savedRet = process.env.AMI_LIVE_TURN_RETENTION;
  afterEach(() => {
    if (savedRet === undefined) delete process.env.AMI_LIVE_TURN_RETENTION;
    else process.env.AMI_LIVE_TURN_RETENTION = savedRet;
  });

  async function run(events: EngineEvent[]): Promise<string> {
    async function* fakeSubmit(_p: string) {
      for (const e of events) yield e as never;
    }
    const res = await taskTool.execute(
      { prompt: 'go', description: 'do it' },
      {
        cwd: process.cwd(),
        abortSignal: new AbortController().signal,
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
        _emitChild: () => {},
        _engineFactory: (_cfg: EngineConfig) => ({ submit: fakeSubmit, shutdown: () => {} }),
      } as unknown as ToolContext,
    );
    return res.output;
  }

  it('reports phase "updating" when the agent ran a file-mutating tool before being capped', async () => {
    const output = await run([
      { type: 'tool_use_start', toolName: 'file_edit', toolCallId: 'c1', input: { file_path: 'a.ts' } },
      { type: 'tool_use_result', toolCallId: 'c1', toolName: 'file_edit', output: 'ok', isError: false },
      { type: 'text_delta', text: 'editing done' },
      { type: 'turn_complete', content: 'editing done', toolCalls: [] },
      { type: 'done', totalTurns: 1, exitReason: 'max_turns' },
    ] as unknown as EngineEvent[]);
    assert.match(output, /reached max turns limit/);
    assert.match(output, /\[phase: updating, \d+\/\d+ turns retained\]/);
    assert.match(output, /\[last turn: "editing done"\]/);
  });

  it('reports phase "starting" when the agent only read before being capped', async () => {
    const output = await run([
      { type: 'tool_use_start', toolName: 'file_read', toolCallId: 'c1', input: { file_path: 'a.ts' } },
      { type: 'tool_use_result', toolCallId: 'c1', toolName: 'file_read', output: 'contents', isError: false },
      { type: 'text_delta', text: 'just looking' },
      { type: 'turn_complete', content: 'just looking', toolCalls: [] },
      { type: 'done', totalTurns: 1, exitReason: 'max_turns' },
    ] as unknown as EngineEvent[]);
    assert.match(output, /\[phase: starting, \d+\/\d+ turns retained\]/);
  });

  it('bounds the retained-turn count to the configured retention cap (ring buffer)', async () => {
    process.env.AMI_LIVE_TURN_RETENTION = '2';
    const events: EngineEvent[] = [];
    for (let i = 1; i <= 5; i++) {
      events.push({ type: 'text_delta', text: `t${i}` } as unknown as EngineEvent);
      events.push({ type: 'turn_complete', content: `turn ${i}`, toolCalls: [] } as unknown as EngineEvent);
    }
    events.push({ type: 'done', totalTurns: 5, exitReason: 'max_turns' } as unknown as EngineEvent);
    const output = await run(events);
    // 5 turns were committed but only 2 retained — the footer count is capped at 2/2.
    assert.match(output, /\[phase: starting, 2\/2 turns retained\]/);
    // The most recent retained turn is the last one.
    assert.match(output, /\[last turn: "turn 5"\]/);
  });

  it('omits the live-state footer on a clean (non-capped) completion', async () => {
    const output = await run([
      { type: 'text_delta', text: 'all good' },
      { type: 'turn_complete', content: 'all good', toolCalls: [] },
      { type: 'done', totalTurns: 1, exitReason: 'end_turn' },
    ] as unknown as EngineEvent[]);
    assert.ok(!output.includes('turns retained'), 'no live-state footer on a clean finish');
    assert.ok(!output.includes('reached max turns'));
  });
});
