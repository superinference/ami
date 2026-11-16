import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { sleepTool, parseSleepDuration } from '../src/tools/sleep';
import type { ToolContext } from '../src/types';

const ctx = (signal?: AbortSignal) =>
  ({ cwd: '/tmp', abortSignal: signal } as unknown as ToolContext);

const aborted = () => {
  const ac = new AbortController();
  ac.abort();
  return ac.signal;
};

describe('parseSleepDuration — human-friendly duration tokens', () => {
  it('treats a bare number as seconds', () => {
    assert.equal(parseSleepDuration('5'), 5_000);
    assert.equal(parseSleepDuration('0.5'), 500);
  });

  it('parses the s / m / h / ms unit suffixes', () => {
    assert.equal(parseSleepDuration('5s'), 5_000);
    assert.equal(parseSleepDuration('2m'), 120_000);
    assert.equal(parseSleepDuration('1h'), 3_600_000);
    assert.equal(parseSleepDuration('500ms'), 500);
    assert.equal(parseSleepDuration('1.5h'), 5_400_000);
  });

  it('is case-insensitive and tolerates whitespace around the token', () => {
    assert.equal(parseSleepDuration('  2M  '), 120_000);
    assert.equal(parseSleepDuration('30 S'), 30_000);
  });

  it('returns null for non-duration tokens', () => {
    for (const bad of ['', 'abc', '5x', '$VAR', '1h30m', 'sleep 5']) {
      assert.equal(parseSleepDuration(bad), null);
    }
  });
});

describe('sleep tool — duration string field (parity, additive)', () => {
  it('accepts a human-friendly duration string instead of duration_ms', async () => {
    const r = await sleepTool.execute!({ duration: '5ms' }, ctx(new AbortController().signal));
    assert.equal(r.isError, undefined);
    assert.equal(r.metadata?.requestedMs, 5);
  });

  it('normalizes a seconds-with-suffix value, then applies the 10-minute cap', async () => {
    // 20m = 1_200_000ms → capped to 600_000ms. Abort immediately so we do not wait.
    const r = await sleepTool.execute!({ duration: '20m' }, ctx(aborted()));
    assert.equal(r.metadata?.capped, true);
    assert.equal(r.metadata?.requestedMs, 600_000);
    assert.equal(r.metadata?.originalRequestedMs, 1_200_000);
  });

  it('prefers duration_ms when both fields are supplied', async () => {
    const r = await sleepTool.execute!(
      { duration_ms: 7, duration: '9s' },
      ctx(new AbortController().signal),
    );
    assert.equal(r.metadata?.requestedMs, 7);
  });

  it('errors on an unparseable duration string', async () => {
    const r = await sleepTool.execute!({ duration: 'soon' }, ctx(new AbortController().signal));
    assert.equal(r.isError, true);
    assert.match(r.output, /could not parse duration/);
  });

  it('errors when neither duration_ms nor duration is provided', async () => {
    const r = await sleepTool.execute!({}, ctx(new AbortController().signal));
    assert.equal(r.isError, true);
    assert.match(r.output, /positive number/);
  });
});

describe('sleep tool — description affordances (parity, additive)', () => {
  it('advertises the human-friendly duration suffix format', () => {
    assert.match(sleepTool.inputSchema.properties!.duration!.description!, /5s, 2m, 1h/);
  });

  it('carries the anti-poll / run_in_background guidance', () => {
    const d = sleepTool.description;
    assert.match(d, /run_in_background/);
    assert.match(d, /do not poll/i);
    assert.match(d, /sleep loop/i);
  });

  it('surfaces the periodic check-in affordance', () => {
    assert.match(sleepTool.description, /check-in/i);
    assert.match(sleepTool.description, /useful work/i);
  });
});
