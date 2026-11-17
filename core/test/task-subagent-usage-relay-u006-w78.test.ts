import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { taskTool, resetAgentRegistry } from '../src/tools/task';
import type { ToolContext, EngineConfig, EngineEvent } from '../src/types';

/**
 * U006 (w78) — END-TO-END: a running sub-agent's relayed `subagent_progress`
 * events carry live cumulative usage (tokens / tool-uses / elapsed ms) AND the
 * structured `lastToolName`, driven through the real task tool → subagent-relay
 * → _emitChild path (parity: Claude's live task_progress usage).
 *
 * This is the integration proof that the task.ts wiring (not just the pure helper)
 * actually stamps the snapshot onto emitted events the parent roster consumes.
 */
describe('task tool — live usage + lastToolName on relayed subagent_progress (E2E)', () => {
  beforeEach(() => resetAgentRegistry());

  async function run(): Promise<EngineEvent[]> {
    const emitted: EngineEvent[] = [];
    async function* fakeSubmit(_p: string) {
      // A usage_update lands first so the relay can stamp a non-zero token total
      // onto the subsequent tool activity.
      yield { type: 'usage_update', stats: { totalTokens: 777 } } as never;
      yield { type: 'tool_use_start', toolName: 'bash', toolCallId: 'c1', input: { command: 'ls' } } as never;
      yield { type: 'tool_use_result', toolCallId: 'c1', toolName: 'bash', output: 'ok', isError: false } as never;
      yield { type: 'text_delta', text: 'done' } as never;
      yield { type: 'turn_complete', content: 'done' } as never;
      yield { type: 'done', exitReason: 'end_turn' } as never;
    }
    await taskTool.execute(
      { prompt: 'go', description: 'do it' },
      {
        cwd: process.cwd(),
        abortSignal: new AbortController().signal,
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
        _emitChild: (e: EngineEvent) => emitted.push(e),
        _engineFactory: (_cfg: EngineConfig) => ({ submit: fakeSubmit, shutdown: () => {} }),
      } as unknown as ToolContext,
    );
    return emitted;
  }

  it('stamps a live usage snapshot on every relayed progress event', async () => {
    const emitted = await run();
    const progress = emitted.filter((e) => e.type === 'subagent_progress');
    assert.ok(progress.length > 0, 'expected at least one relayed subagent_progress');
    for (const p of progress) {
      assert.ok(p.type === 'subagent_progress' && p.usage, 'every progress carries a usage snapshot');
      assert.equal(typeof (p.type === 'subagent_progress' && p.usage!.totalTokens), 'number');
      assert.equal(typeof (p.type === 'subagent_progress' && p.usage!.toolUses), 'number');
      assert.ok(p.type === 'subagent_progress' && p.usage!.durationMs >= 0);
    }
  });

  it('reflects the latest token total and structured lastToolName on the tool activity', async () => {
    const emitted = await run();
    const toolStart = emitted.find(
      (e) => e.type === 'subagent_progress' && e.activity === 'tool' && e.lastToolName === 'bash',
    );
    assert.ok(toolStart && toolStart.type === 'subagent_progress', 'bash tool activity relayed');
    // usage_update(777) was processed before the tool_use_start relay.
    assert.equal(toolStart.usage!.totalTokens, 777);
    assert.equal(toolStart.lastToolName, 'bash');
  });
});
