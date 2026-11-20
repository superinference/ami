import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { taskOutputTool } from '../src/tools/task-output';
import type { ToolContext } from '../src/types';

/**
 * NOTE: task_output BLOCKS by default (parity with the reference's `block:true`), so the
 * non-blocking read-formatting cases below pass `wait: false` to take a one-shot
 * snapshot of a still-running task instead of blocking for the (30s) timeout.
 *
 * Parity (additive superset) for task_output vs Claude Code BashOutput retrieval:
 *   - filter (regex) line selection, passed through to the process manager
 *   - invalid-filter clear error (not silent empty)
 *   - tail validation (NaN / string / negative) -> safe default / clamp
 *   - incremental since_offset read (cursor-style, reports Next offset + reset)
 *   - explicit success/failure verdict for a terminal task
 */

function ctxWithGetOutput(fn: (id: string, tail: number, filter?: RegExp) => unknown): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    processManager: { getOutput: fn },
  } as unknown as ToolContext;
}

describe('task_output parity: filter regex', () => {
  it('passes a valid filter RegExp through to getOutput and labels the result', async () => {
    let seenFilter: RegExp | undefined;
    const ctx = ctxWithGetOutput((_id, _tail, filter) => {
      seenFilter = filter;
      return { status: 'running', exitCode: null, elapsedMs: 1000, output: 'ERROR boom' };
    });
    const res = await taskOutputTool.execute!({ task_id: 't', filter: 'ERROR', wait: false }, ctx);
    assert.ok(!res.isError);
    assert.ok(seenFilter instanceof RegExp, 'getOutput received a compiled RegExp');
    assert.equal(seenFilter!.source, 'ERROR');
    assert.ok(res.output.includes('Filter:'), 'labels that a filter is applied');
  });

  it('empty-filter / no-match reports a filter-specific empty message', async () => {
    const ctx = ctxWithGetOutput(() => ({ status: 'running', exitCode: null, elapsedMs: 10, output: '' }));
    const res = await taskOutputTool.execute!({ task_id: 't', filter: 'ZZZ', wait: false }, ctx);
    assert.ok(res.output.includes('no output matching filter'), 'filter-aware empty message');
  });
});

describe('task_output parity: invalid filter', () => {
  it('returns a clear error for an unparseable regex instead of silently empty', async () => {
    let getOutputCalled = false;
    const ctx = ctxWithGetOutput(() => { getOutputCalled = true; return { status: 'running', exitCode: null, elapsedMs: 1, output: '' }; });
    const res = await taskOutputTool.execute!({ task_id: 't', filter: '(' }, ctx);
    assert.ok(res.isError, 'invalid regex is an error');
    assert.ok(res.output.includes('invalid filter regex'), 'names the problem');
    assert.ok(!getOutputCalled, 'does not even query output for a bad pattern');
  });

  it('rejects a non-string filter', async () => {
    const ctx = ctxWithGetOutput(() => ({ status: 'running', exitCode: null, elapsedMs: 1, output: '' }));
    const res = await taskOutputTool.execute!({ task_id: 't', filter: 123 as unknown as string }, ctx);
    assert.ok(res.isError);
    assert.ok(res.output.includes('filter must be'));
  });
});

describe('task_output parity: tail validation', () => {
  it('a NaN / non-numeric tail falls back to the default (50), not "NaN lines"', async () => {
    let seenTail = -1;
    const ctx = ctxWithGetOutput((_id, tail) => {
      seenTail = tail;
      return { status: 'running', exitCode: null, elapsedMs: 1, output: 'x' };
    });
    const res = await taskOutputTool.execute!({ task_id: 't', tail: 'oops' as unknown as number, wait: false }, ctx);
    assert.equal(seenTail, 50, 'invalid tail coerced to default');
    assert.ok(!res.output.includes('NaN'), 'no NaN leaks into the label');
    assert.ok(res.output.includes('last 50 lines'));
  });

  it('tail:0 means all buffered lines', async () => {
    let seenTail = -1;
    const ctx = ctxWithGetOutput((_id, tail) => {
      seenTail = tail;
      return { status: 'running', exitCode: null, elapsedMs: 1, output: 'x' };
    });
    const res = await taskOutputTool.execute!({ task_id: 't', tail: 0, wait: false }, ctx);
    assert.equal(seenTail, 0);
    assert.ok(res.output.includes('all lines'));
  });

  it('a negative tail clamps to 0 (all), never a broken negative slice', async () => {
    let seenTail = -99;
    const ctx = ctxWithGetOutput((_id, tail) => { seenTail = tail; return { status: 'running', exitCode: null, elapsedMs: 1, output: 'x' }; });
    await taskOutputTool.execute!({ task_id: 't', tail: -5, wait: false }, ctx);
    assert.equal(seenTail, 0, 'negative tail clamped to 0');
  });
});

describe('task_output parity: incremental since_offset read', () => {
  function ctxIncremental(tailImpl: (id: string, from: number) => unknown): ToolContext {
    return {
      cwd: '/tmp',
      abortSignal: new AbortController().signal,
      processManager: {
        getOutput: () => ({ status: 'running', exitCode: null, elapsedMs: 1, output: 'WHOLE BUFFER' }),
        getOutputTail: tailImpl,
      },
    } as unknown as ToolContext;
  }

  it('returns only bytes since the offset and reports the next offset', async () => {
    let seenFrom = -1;
    const ctx = ctxIncremental((_id, from) => {
      seenFrom = from;
      return { output: 'new line A\nnew line B\n', offset: 4242, reset: false };
    });
    const res = await taskOutputTool.execute!({ task_id: 't', since_offset: 100, wait: false }, ctx);
    assert.equal(seenFrom, 100, 'passed the caller offset to getOutputTail');
    assert.ok(res.output.includes('Next offset: 4242'), 'reports the cursor for the next call');
    assert.ok(res.output.includes('new line A'), 'shows the incremental chunk');
    assert.ok(!res.output.includes('WHOLE BUFFER'), 'does NOT fall back to the whole buffer');
    assert.ok(res.output.includes('New output'), 'labels it as incremental');
  });

  it('flags a reset when the output rotated/shrank', async () => {
    const ctx = ctxIncremental(() => ({ output: 'rebuilt', offset: 7, reset: true }));
    const res = await taskOutputTool.execute!({ task_id: 't', since_offset: 999, wait: false }, ctx);
    assert.ok(res.output.includes('offset reset'), 'surfaces the reset');
  });

  it('reports no-new-output when nothing was appended', async () => {
    const ctx = ctxIncremental(() => ({ output: '', offset: 100, reset: false }));
    const res = await taskOutputTool.execute!({ task_id: 't', since_offset: 100, wait: false }, ctx);
    assert.ok(res.output.includes('no new output since last read'));
  });

  it('applies the filter to the incremental chunk too', async () => {
    const ctx = ctxIncremental(() => ({ output: 'noise\nERROR here\nmore noise\n', offset: 50, reset: false }));
    const res = await taskOutputTool.execute!({ task_id: 't', since_offset: 10, filter: 'ERROR', wait: false }, ctx);
    assert.ok(res.output.includes('ERROR here'));
    assert.ok(!res.output.includes('more noise'), 'non-matching incremental lines filtered out');
  });
});

describe('task_output parity: exit-status verdict', () => {
  it('a zero exit on a terminal task reads as success', async () => {
    const ctx = ctxWithGetOutput(() => ({ status: 'completed', exitCode: 0, elapsedMs: 1000, output: 'ok' }));
    const res = await taskOutputTool.execute!({ task_id: 't' }, ctx);
    assert.ok(res.output.includes('Result: success'));
  });

  it('a non-zero exit on a terminal task reads as failure with the code', async () => {
    const ctx = ctxWithGetOutput(() => ({ status: 'failed', exitCode: 2, elapsedMs: 1000, output: 'bad' }));
    const res = await taskOutputTool.execute!({ task_id: 't' }, ctx);
    assert.ok(res.output.includes('Result: failure (exit code 2)'));
  });

  it('a running task gets no verdict', async () => {
    const ctx = ctxWithGetOutput(() => ({ status: 'running', exitCode: null, elapsedMs: 1000, output: 'x' }));
    const res = await taskOutputTool.execute!({ task_id: 't', wait: false }, ctx);
    assert.ok(!res.output.includes('Result:'));
  });
});
