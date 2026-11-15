import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { sleepTool } from '../src/tools/sleep';
import type { ToolContext } from '../src/types';

const ctx = (signal?: AbortSignal) =>
  ({ cwd: '/tmp', abortSignal: signal } as unknown as ToolContext);

describe('sleep tool — parity (additive superset)', () => {
  // Fix 1: non-finite durations are rejected rather than silently clamped to MAX
  // and slept in full. We pass an ALREADY-ABORTED signal so that on the RED path
  // (old guard `isNaN(...) || <= 0`) Infinity passes validation, is clamped to
  // MAX, and the aborted signal makes it return promptly as `interrupted`
  // (isError undefined) — a fast, clean failure instead of a 600s hang. On the
  // GREEN path it errors at validation before ever touching the signal.
  const aborted = () => {
    const ac = new AbortController();
    ac.abort();
    return ac.signal;
  };

  it('rejects a non-finite (Infinity) duration as an error', async () => {
    const r = await sleepTool.execute!({ duration_ms: Infinity }, ctx(aborted()));
    assert.equal(r.isError, true);
    assert.match(r.output, /finite/i);
  });

  it('rejects -Infinity as an error', async () => {
    const r = await sleepTool.execute!({ duration_ms: -Infinity }, ctx(aborted()));
    assert.equal(r.isError, true);
  });

  // Guard: a valid finite duration still works (green-both-ways check for Fix 1
  // discrimination — the Infinity case must behave DIFFERENTLY from this one).
  it('still accepts a normal finite duration', async () => {
    const r = await sleepTool.execute!({ duration_ms: 5 }, ctx(new AbortController().signal));
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Slept for/);
  });

  // Fix 2: sub-millisecond durations clamp to a minimum of 1ms instead of
  // rounding down to a 0ms no-op. RED (old `Math.round(0.4)` === 0): requestedMs
  // is 0. GREEN (`Math.max(1, ...)`): requestedMs is 1.
  it('clamps a sub-millisecond duration up to 1ms (no 0ms no-op)', async () => {
    const r = await sleepTool.execute!({ duration_ms: 0.4 }, ctx(new AbortController().signal));
    assert.equal(r.isError, undefined);
    assert.equal(r.metadata?.requestedMs, 1);
  });

  // Fix 3: exceeding the 10-minute cap is surfaced to the model (metadata + text)
  // instead of being silently truncated. RED (old impl): metadata.capped is
  // undefined and the output has no cap notice. GREEN: capped === true, the
  // original request is preserved, and the message states it was capped.
  it('surfaces that an over-cap request was capped (metadata + message)', async () => {
    const ac = new AbortController();
    const p = sleepTool.execute!({ duration_ms: 99_999_999 }, ctx(ac.signal));
    setTimeout(() => ac.abort(), 10); // abort so the test does not actually wait
    const r = await p;
    assert.equal(r.metadata?.capped, true);
    assert.equal(r.metadata?.requestedMs, 600_000);
    assert.equal(r.metadata?.originalRequestedMs, 99_999_999);
    assert.match(r.output, /capped/i);
  });

  // Guard: an under-cap request is NOT flagged as capped (discriminates Fix 3 —
  // the flag must be false here and true above, never constant).
  it('does not flag an under-cap request as capped', async () => {
    const r = await sleepTool.execute!({ duration_ms: 10 }, ctx(new AbortController().signal));
    assert.equal(r.metadata?.capped, false);
    assert.doesNotMatch(r.output, /capped/i);
    assert.equal(r.metadata?.originalRequestedMs, 10);
  });
});
