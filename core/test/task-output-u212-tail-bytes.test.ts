import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { taskOutputTool } from '../src/tools/task-output';
import type { ToolContext } from '../src/types';

/**
 * U212 (parity, additive): a per-call `tail_bytes` input lets the model request a
 * byte-bounded tail read of a task's on-disk log (mirrors Claude Code's
 * ShellDetailDialog reading only the last N bytes / DEFAULT_MAX_READ_BYTES),
 * instead of the fixed internal window. The value is floored to an int and clamped
 * to [1, MAX_READ_BYTES]; a missing/garbage value uses the default window.
 */

function ctx(overrides: Record<string, unknown>): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    processManager: overrides,
  } as unknown as ToolContext;
}

function capture(): { ctx: ToolContext; seen: () => unknown } {
  let seenMaxBytes: unknown = 'unset';
  const c = ctx({
    getOutput: (_id: string, _tail: number, _filter: unknown, maxBytes?: number) => {
      seenMaxBytes = maxBytes;
      return { status: 'completed', exitCode: 0, elapsedMs: 10, output: 'done' };
    },
  });
  return { ctx: c, seen: () => seenMaxBytes };
}

describe('task_output U212: tail_bytes byte window override', () => {
  it('advertises the tail_bytes input in the schema', () => {
    const props = taskOutputTool.inputSchema.properties as Record<string, { type?: string; maximum?: number }>;
    assert.ok(props.tail_bytes, 'tail_bytes is advertised');
    assert.equal(props.tail_bytes.type, 'number');
    assert.equal(typeof props.tail_bytes.maximum, 'number', 'a byte ceiling is advertised');
  });

  it('passes a supplied tail_bytes as the byte ceiling to getOutput', async () => {
    const { ctx: c, seen } = capture();
    const res = await taskOutputTool.execute!({ task_id: 't', tail_bytes: 8192 }, c);
    assert.ok(!res.isError);
    assert.equal(seen(), 8192, 'the requested byte window is honored');
  });

  it('uses the default window (not the requested value) when tail_bytes is absent', async () => {
    const { ctx: c, seen } = capture();
    await taskOutputTool.execute!({ task_id: 't' }, c);
    assert.equal(typeof seen(), 'number');
    assert.notEqual(seen(), 8192, 'absent tail_bytes does not leak a requested value');
    assert.ok((seen() as number) > 0, 'default window is positive');
  });

  it('clamps a tail_bytes above the ceiling down to the max', async () => {
    const { ctx: c, seen } = capture();
    const props = taskOutputTool.inputSchema.properties as Record<string, { maximum: number }>;
    const max = props.tail_bytes.maximum;
    await taskOutputTool.execute!({ task_id: 't', tail_bytes: max * 100 }, c);
    assert.equal(seen(), max, 'over-ceiling request is clamped to the max');
  });

  it('clamps a zero/negative tail_bytes up to at least 1 byte', async () => {
    const { ctx: c, seen } = capture();
    await taskOutputTool.execute!({ task_id: 't', tail_bytes: 0 }, c);
    assert.equal(seen(), 1, 'zero is clamped up to the 1-byte floor');
  });

  it('floors a fractional tail_bytes to an integer', async () => {
    const { ctx: c, seen } = capture();
    await taskOutputTool.execute!({ task_id: 't', tail_bytes: 4096.9 }, c);
    assert.equal(seen(), 4096, 'fractional byte count is truncated to an int');
  });

  it('accepts a provider-serialized numeric string', async () => {
    const { ctx: c, seen } = capture();
    await taskOutputTool.execute!({ task_id: 't', tail_bytes: '2048' }, c);
    assert.equal(seen(), 2048, 'a quoted numeric string is coerced');
  });

  it('also applies the override on the blocking-wait re-read', async () => {
    const seen: number[] = [];
    let calls = 0;
    const c = ctx({
      getOutput: (_id: string, _tail: number, _filter: unknown, maxBytes?: number) => {
        seen.push(maxBytes as number);
        calls += 1;
        // first read running, second read terminal -> exercises the wait re-read
        return calls < 2
          ? { status: 'running', exitCode: null, elapsedMs: 5, output: '' }
          : { status: 'completed', exitCode: 0, elapsedMs: 20, output: 'done' };
      },
    });
    await taskOutputTool.execute!({ task_id: 't', tail_bytes: 1024, timeout_ms: 2000 }, c);
    assert.ok(calls >= 2, 'the wait loop re-read');
    assert.ok(seen.every((b) => b === 1024), 'every read used the requested byte window');
  });
});
