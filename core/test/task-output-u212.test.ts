import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { taskOutputTool, getAgentOutputTaskId } from '../src/tools/task-output';
import { formatSize } from '../src/tools/spill-output';
import type { ToolContext } from '../src/types';

/**
 * U212 parity (additive superset) for task_output:
 *   - timeout_ms schema advertises Claude's min(0)/max(600000) bound
 *   - a wait interrupted by the abort signal returns a DISTINCT "wait aborted"
 *     retrieval status (not the timeout status, not a silent partial)
 *   - a tail that is only a window into a larger buffer appends a quantitative
 *     "Showing N lines of <total>, <omitted> omitted" indicator + full-file pointer
 */

function ctx(overrides: Record<string, unknown>, signal?: AbortSignal): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: signal ?? new AbortController().signal,
    processManager: overrides,
  } as unknown as ToolContext;
}

describe('task_output U212: timeout_ms schema bound', () => {
  it('advertises minimum 0 and maximum 600000 (mirrors Claude min/max)', () => {
    const props = (taskOutputTool.inputSchema as { properties: Record<string, { minimum?: number; maximum?: number }> }).properties;
    assert.equal(props.timeout_ms.minimum, 0, 'schema advertises a lower bound of 0');
    assert.equal(props.timeout_ms.maximum, 600000, 'schema advertises the 600000ms ceiling');
  });
});

describe('task_output U212: explicit abort signalling', () => {
  it('returns a distinct "wait aborted" status when the wait is interrupted, not the timeout status', async () => {
    const controller = new AbortController();
    controller.abort();
    let calls = 0;
    const c = ctx({
      getOutput: () => { calls++; return { status: 'running', exitCode: null, elapsedMs: 1200, output: 'partial so far' }; },
    }, controller.signal);
    const res = await taskOutputTool.execute!({ task_id: 't', wait: true }, c);
    assert.ok(!res.isError);
    assert.ok(res.output.includes('wait aborted'), 'distinct aborted retrieval status');
    assert.ok(!res.output.includes('wait timed out'), 'NOT reported as a timeout');
    assert.ok(res.output.includes('partial so far'), 'still surfaces the partial snapshot');
    // Aborted before any poll sleep, so getOutput is queried exactly once (the
    // initial read); the wait loop breaks on the first abort check.
    assert.equal(calls, 1, 'abort short-circuits the poll loop immediately');
  });

  it('a plain wait timeout (no abort) still reports the timeout status, not aborted', async () => {
    const c = ctx({
      getOutput: () => ({ status: 'running', exitCode: null, elapsedMs: 10, output: 'x' }),
    });
    // timeout_ms:1 → deadline passes after the first poll, loop exits via deadline
    const res = await taskOutputTool.execute!({ task_id: 't', wait: true, timeout_ms: 1 }, c);
    assert.ok(res.output.includes('wait timed out'), 'timeout path preserved');
    assert.ok(!res.output.includes('wait aborted'), 'not misreported as aborted');
  });
});

describe('task_output U212: truncation / total-size indicator', () => {
  it('appends "Showing N lines of <total>" + omitted bytes + full-file pointer when the tail is a window', async () => {
    const body = 'line48\nline49\nline50';
    const total = 40_000; // far larger than the shown tail
    const c = ctx({
      getOutput: () => ({ status: 'running', exitCode: null, elapsedMs: 100, output: body }),
      getOutputSize: () => total,
      get: () => ({ outputPath: '/var/tasks/t.output' }),
    });
    const res = await taskOutputTool.execute!({ task_id: 't' }, c);
    assert.ok(res.output.includes('Showing 3 lines'), 'reports the shown line count');
    assert.ok(res.output.includes(`of ${formatSize(total)} total`), 'reports the total size');
    assert.ok(res.output.includes('earlier output omitted'), 'quantifies the omission');
    assert.ok(res.output.includes('/var/tasks/t.output'), 'points at the full output file');
  });

  it('does NOT append the indicator when the tail already covers the whole buffer', async () => {
    const body = 'all\nof\nit';
    const c = ctx({
      getOutput: () => ({ status: 'completed', exitCode: 0, elapsedMs: 5, output: body }),
      getOutputSize: () => Buffer.byteLength(body, 'utf-8'),
      get: () => ({ outputPath: '/var/tasks/t.output' }),
    });
    const res = await taskOutputTool.execute!({ task_id: 't' }, c);
    assert.ok(!res.output.includes('earlier output omitted'), 'complete output carries no truncation notice');
  });

  it('does NOT append the indicator when a filter is active (subset, not truncation)', async () => {
    const c = ctx({
      getOutput: () => ({ status: 'running', exitCode: null, elapsedMs: 5, output: 'ERROR one\nERROR two' }),
      getOutputSize: () => 99_999,
      get: () => ({ outputPath: '/var/tasks/t.output' }),
    });
    const res = await taskOutputTool.execute!({ task_id: 't', filter: 'ERROR' }, c);
    assert.ok(!res.output.includes('earlier output omitted'), 'filtered subset is not a truncation');
  });

  it('is silent when the process manager cannot introspect size', async () => {
    const c = ctx({
      getOutput: () => ({ status: 'running', exitCode: null, elapsedMs: 5, output: 'tiny' }),
    });
    const res = await taskOutputTool.execute!({ task_id: 't' }, c);
    assert.ok(!res.output.includes('earlier output omitted'), 'no size introspection -> no indicator');
  });
});

describe('getAgentOutputTaskId (parity: Claude Code FileReadTool/UI getAgentOutputTaskId)', () => {
  it('extracts the task id from a …/tasks/<id>.output path', () => {
    assert.equal(getAgentOutputTaskId('/home/u/.ami/projects/p/tasks/bg-123.output'), 'bg-123');
    assert.equal(getAgentOutputTaskId('tasks/abc.output'), 'abc');
    // Windows-style separators normalize to the same recognition.
    assert.equal(getAgentOutputTaskId('C:\\state\\tasks\\agent_7.output'), 'agent_7');
  });

  it('rejects paths not shaped like an agent output file', () => {
    assert.equal(getAgentOutputTaskId('/var/log/tasks/x.txt'), null, 'wrong suffix');
    assert.equal(getAgentOutputTaskId('/var/other/x.output'), null, 'not under a tasks/ dir');
    assert.equal(getAgentOutputTaskId('x.output'), null, 'no directory at all');
    assert.equal(getAgentOutputTaskId('/t/tasks/.output'), null, 'empty id');
  });

  it('rejects ids that fail the length/charset validator', () => {
    assert.equal(getAgentOutputTaskId('/t/tasks/' + 'a'.repeat(21) + '.output'), null, 'id too long (>20)');
    assert.equal(getAgentOutputTaskId('/t/tasks/bad id.output'), null, 'id has a space');
    assert.equal(getAgentOutputTaskId('/t/tasks/bad.id.output'), null, 'id has a dot');
  });

  it('is null-safe for non-string input', () => {
    assert.equal(getAgentOutputTaskId(undefined), null);
    assert.equal(getAgentOutputTaskId(42), null);
    assert.equal(getAgentOutputTaskId(null), null);
  });
});
