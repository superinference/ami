import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { sleepTool } from '../src/tools/sleep';
import type { ToolContext } from '../src/types';

const ctx = (signal?: AbortSignal) => ({ cwd: '/tmp', abortSignal: signal } as unknown as ToolContext);

describe('sleep tool', () => {
  it('has the expected name and is read-only / concurrency-safe', () => {
    assert.equal(sleepTool.name, 'sleep');
    assert.equal(sleepTool.isReadOnly, true);
    assert.equal(sleepTool.isConcurrencySafe, true);
  });

  it('description carries the interrupt, concurrency, cost, and prefer-over-bash affordances', () => {
    const d = sleepTool.description;
    assert.match(d, /interrupt/i);
    assert.match(d, /concurrently/);
    assert.match(d, /API call/);
    assert.match(d, /prompt cache/);
    assert.match(d, /Bash\(sleep/);
    assert.match(d, /rest/);
  });

  it('rejects a non-positive / non-numeric duration', async () => {
    for (const bad of [0, -5, NaN, 'x' as unknown as number]) {
      const r = await sleepTool.execute!({ duration_ms: bad }, ctx(new AbortController().signal));
      assert.equal(r.isError, true);
      assert.match(r.output, /positive number/);
    }
  });

  it('sleeps for a short duration and reports elapsed time + metadata', async () => {
    const r = await sleepTool.execute!({ duration_ms: 20 }, ctx(new AbortController().signal));
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Slept for/);
    assert.equal(r.metadata?.interrupted, false);
    assert.equal(r.metadata?.requestedMs, 20);
  });

  it('includes the reason when provided', async () => {
    const r = await sleepTool.execute!({ duration_ms: 10, reason: 'poll CI' }, ctx(new AbortController().signal));
    assert.match(r.output, /Reason: poll CI/);
  });

  it('returns immediately interrupted when the signal is already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    const r = await sleepTool.execute!({ duration_ms: 500000 }, ctx(ac.signal));
    assert.match(r.output, /interrupted/i);
    assert.equal(r.metadata?.interrupted, true);
  });

  it('wakes early and reports interruption when aborted mid-sleep', async () => {
    const ac = new AbortController();
    const p = sleepTool.execute!({ duration_ms: 500000, reason: 'long wait' }, ctx(ac.signal));
    setTimeout(() => ac.abort(), 15);
    const r = await p;
    assert.match(r.output, /Sleep interrupted after/);
    assert.match(r.output, /Reason: long wait/);
    assert.equal(r.metadata?.interrupted, true);
  });

  it('works without an abort signal (completes normally)', async () => {
    const r = await sleepTool.execute!({ duration_ms: 10 }, ctx(undefined));
    assert.match(r.output, /Slept for/);
    assert.equal(r.metadata?.interrupted, false);
  });

  it('caps the duration at the 10-minute maximum', async () => {
    const ac = new AbortController();
    const p = sleepTool.execute!({ duration_ms: 99_999_999 }, ctx(ac.signal));
    // Abort quickly so the test does not actually wait; verify the requested cap.
    setTimeout(() => ac.abort(), 10);
    const r = await p;
    assert.equal(r.metadata?.requestedMs, 600_000);
  });
});
