import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import { taskOutputTool, maxOutputChars } from '../src/tools/task-output';
import type { ToolContext } from '../src/types';

/**
 * U212 (parity, additive): the hard character cap on returned task output is
 * operator-tunable via the `AMI_TASK_MAX_OUTPUT_CHARS` env var, validated through
 * the canonical bounded-int env validator — mirroring the reference's
 * getMaxTaskOutputLength()/validateBoundedIntEnvVar around TASK_MAX_OUTPUT_LENGTH.
 * Absent/garbage → default; below the floor → default; above the ceiling → clamped.
 * The cap can never be disabled or set to an unbounded value.
 */

const ENV = 'AMI_TASK_MAX_OUTPUT_CHARS';

function ctx(overrides: Record<string, unknown>): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    processManager: overrides,
  } as unknown as ToolContext;
}

describe('task_output U212: bounded env-configurable char cap', () => {
  afterEach(() => { delete process.env[ENV]; });

  it('defaults to 100000 when the env var is unset', () => {
    delete process.env[ENV];
    assert.equal(maxOutputChars(), 100_000);
  });

  it('honours a valid in-bounds override', () => {
    process.env[ENV] = '5000';
    assert.equal(maxOutputChars(), 5000);
  });

  it('falls back to the default on a garbage value', () => {
    process.env[ENV] = 'not-a-number';
    assert.equal(maxOutputChars(), 100_000);
  });

  it('falls back to the default when set below the floor', () => {
    process.env[ENV] = '10';
    assert.equal(maxOutputChars(), 100_000);
  });

  it('clamps a value above the ceiling', () => {
    process.env[ENV] = '99999999';
    assert.equal(maxOutputChars(), 1_000_000);
  });

  it('applies the overridden cap to the returned output tail', async () => {
    process.env[ENV] = '5000';
    const body = 'x'.repeat(6000);
    const c = ctx({
      getOutput: () => ({ status: 'completed', exitCode: 0, elapsedMs: 10, output: body, bytesOmitted: 1 }),
    });
    const res = await taskOutputTool.execute!({ task_id: 't', wait: false }, c);
    assert.ok(!res.isError);
    assert.ok(res.output.includes('truncated to the last 5000 chars'), 'marker reports the overridden cap');
    // The emitted x-run is bounded by the overridden cap (5000), not the 6000 raw length.
    assert.ok(res.output.includes('x'.repeat(5000)), 'tail retains exactly the overridden cap of chars');
    assert.ok(!res.output.includes('x'.repeat(5001)), 'tail does not exceed the overridden cap');
  });

  it('does not truncate when the body is within the default cap', async () => {
    delete process.env[ENV];
    const body = 'y'.repeat(6000);
    const c = ctx({
      getOutput: () => ({ status: 'completed', exitCode: 0, elapsedMs: 10, output: body, bytesOmitted: 1 }),
    });
    const res = await taskOutputTool.execute!({ task_id: 't', wait: false }, c);
    assert.ok(!res.output.includes('truncated to the last'), 'no truncation under the default cap');
    assert.ok(res.output.includes('y'.repeat(6000)), 'full body retained');
  });
});
