import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { sleepTool } from '../src/tools/sleep';
import type { ToolContext } from '../src/types';

const ctx = (over: Partial<ToolContext> = {}) =>
  ({ cwd: '/tmp', abortSignal: new AbortController().signal, ...over } as unknown as ToolContext);

describe('sleep tool — early-wake on queued input', () => {
  it('advertises the early-wake affordance in its description', () => {
    assert.match(sleepTool.description, /wakes early/i);
    assert.match(sleepTool.description, /queued/i);
  });

  it('wakes immediately when input is already queued at start', async () => {
    let queued = true;
    const r = await sleepTool.execute!(
      { duration_ms: 500_000, reason: 'poll' },
      ctx({ hasQueuedInput: () => queued }),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /woke early/i);
    assert.match(r.output, /new input was queued/i);
    assert.match(r.output, /Reason: poll/);
    assert.equal(r.metadata?.wokeForInput, true);
    assert.equal(r.metadata?.interrupted, false);
    // Elapsed should be tiny, nowhere near the requested 500s.
    assert.ok((r.metadata?.sleptMs as number) < 1000, 'woke promptly');
    queued = false;
  });

  it('wakes early (≤ ~1s) when input becomes queued mid-sleep', async () => {
    let queued = false;
    const p = sleepTool.execute!(
      { duration_ms: 500_000 },
      ctx({ hasQueuedInput: () => queued }),
    );
    setTimeout(() => {
      queued = true;
    }, 15);
    const r = await p;
    assert.match(r.output, /woke early/i);
    assert.equal(r.metadata?.wokeForInput, true);
    assert.ok((r.metadata?.sleptMs as number) < 2000, 'woke within the poll window');
  });

  it('completes normally (no early wake) when nothing is queued', async () => {
    const r = await sleepTool.execute!(
      { duration_ms: 20 },
      ctx({ hasQueuedInput: () => false }),
    );
    assert.match(r.output, /Slept for/);
    assert.equal(r.metadata?.wokeForInput, false);
    assert.equal(r.metadata?.interrupted, false);
  });

  it('abort still wins over the poll loop', async () => {
    const ac = new AbortController();
    const p = sleepTool.execute!(
      { duration_ms: 500_000 },
      ctx({ abortSignal: ac.signal, hasQueuedInput: () => false }),
    );
    setTimeout(() => ac.abort(), 15);
    const r = await p;
    assert.match(r.output, /interrupted/i);
    assert.equal(r.metadata?.interrupted, true);
    assert.equal(r.metadata?.wokeForInput, false);
  });

  it('behaves unchanged when no hasQueuedInput hook is supplied', async () => {
    const r = await sleepTool.execute!({ duration_ms: 15 }, ctx());
    assert.match(r.output, /Slept for/);
    assert.equal(r.metadata?.wokeForInput, false);
  });
});
