import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { sleepTool } from '../src/tools/sleep';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

describe('sleepTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(sleepTool.name, 'sleep');
  });

  it('is read-only and concurrency-safe', () => {
    assert.equal(sleepTool.isReadOnly, true);
    assert.equal(sleepTool.isConcurrencySafe, true);
  });

  it('requires duration_ms', () => {
    assert.ok(sleepTool.inputSchema.required?.includes('duration_ms'));
  });

  it('has reason as optional', () => {
    assert.ok(!sleepTool.inputSchema.required?.includes('reason'));
  });
});

describe('sleepTool – validation', () => {
  it('rejects zero duration', async () => {
    const result = await sleepTool.execute({ duration_ms: 0 }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('positive'));
  });

  it('rejects negative duration', async () => {
    const result = await sleepTool.execute({ duration_ms: -100 }, ctx());
    assert.equal(result.isError, true);
  });

  it('rejects NaN duration', async () => {
    const result = await sleepTool.execute({ duration_ms: NaN }, ctx());
    assert.equal(result.isError, true);
  });
});

describe('sleepTool – execution', () => {
  it('sleeps for a short duration', async () => {
    const start = Date.now();
    const result = await sleepTool.execute({ duration_ms: 50 }, ctx());
    const elapsed = Date.now() - start;
    assert.ok(!result.isError);
    assert.ok(elapsed >= 40); // allow some timer jitter
    assert.ok(result.output.includes('Slept'));
  });

  it('includes reason in output when provided', async () => {
    const result = await sleepTool.execute(
      { duration_ms: 10, reason: 'waiting for CI' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('waiting for CI'));
  });

  it('caps at MAX_SLEEP_MS (600000)', async () => {
    const ac = new AbortController();
    setTimeout(() => ac.abort(), 50);
    const result = await sleepTool.execute(
      { duration_ms: 999999 },
      ctx({ abortSignal: ac.signal }),
    );
    assert.ok(result.output.includes('600000.0') || result.output.includes('600.0'));
  });

  it('respects abort signal', async () => {
    const ac = new AbortController();
    const start = Date.now();
    setTimeout(() => ac.abort(), 30);
    const result = await sleepTool.execute(
      { duration_ms: 10000 },
      ctx({ abortSignal: ac.signal }),
    );
    const elapsed = Date.now() - start;
    assert.ok(elapsed < 5000);
    assert.ok(!result.isError);
  });

  it('handles pre-aborted signal', async () => {
    const ac = new AbortController();
    ac.abort();
    const start = Date.now();
    const result = await sleepTool.execute(
      { duration_ms: 5000 },
      ctx({ abortSignal: ac.signal }),
    );
    const elapsed = Date.now() - start;
    assert.ok(elapsed < 1000);
    assert.ok(!result.isError);
  });
});
