/**
 * UNS005 — soft-fail timed tool-list / tool-description enumeration.
 *
 * Covers the reusable primitive added to tool-gates.ts that mirrors the reference
 * Code's `tryGetInstalledAppNames` / `APP_ENUM_TIMEOUT_MS` soft-fail pattern:
 * a configurable timeout knob plus a race-with-timeout helper that returns a
 * fallback on timeout OR rejection OR synchronous throw and swallows a late
 * settle. ESM-safe: asserts observable return values / timing only.
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  DEFAULT_TOOL_ENUM_TIMEOUT_MS,
  TOOL_ENUM_TIMEOUT_ENV,
  getToolEnumerationTimeoutMs,
  softFailTimedEnumeration,
} from '../src/tools/tool-gates';

describe('getToolEnumerationTimeoutMs', () => {
  it('returns the default when the env var is unset / empty / whitespace', () => {
    assert.equal(getToolEnumerationTimeoutMs({}), DEFAULT_TOOL_ENUM_TIMEOUT_MS);
    assert.equal(getToolEnumerationTimeoutMs({ [TOOL_ENUM_TIMEOUT_ENV]: '' }), DEFAULT_TOOL_ENUM_TIMEOUT_MS);
    assert.equal(getToolEnumerationTimeoutMs({ [TOOL_ENUM_TIMEOUT_ENV]: '   ' }), DEFAULT_TOOL_ENUM_TIMEOUT_MS);
  });

  it('returns the default for non-numeric / non-positive values', () => {
    for (const v of ['abc', '0', '-5', 'NaN', 'Infinity']) {
      assert.equal(
        getToolEnumerationTimeoutMs({ [TOOL_ENUM_TIMEOUT_ENV]: v }),
        DEFAULT_TOOL_ENUM_TIMEOUT_MS,
        `expected default for ${v}`,
      );
    }
  });

  it('honors a valid positive numeric override', () => {
    assert.equal(getToolEnumerationTimeoutMs({ [TOOL_ENUM_TIMEOUT_ENV]: '250' }), 250);
    assert.equal(getToolEnumerationTimeoutMs({ [TOOL_ENUM_TIMEOUT_ENV]: '1.5' }), 1.5);
  });

  it('reads process.env by default', () => {
    const saved = process.env[TOOL_ENUM_TIMEOUT_ENV];
    try {
      process.env[TOOL_ENUM_TIMEOUT_ENV] = '321';
      assert.equal(getToolEnumerationTimeoutMs(), 321);
    } finally {
      if (saved === undefined) delete process.env[TOOL_ENUM_TIMEOUT_ENV];
      else process.env[TOOL_ENUM_TIMEOUT_ENV] = saved;
    }
  });
});

describe('softFailTimedEnumeration', () => {
  it('returns the producer value when it resolves before the timeout', async () => {
    const out = await softFailTimedEnumeration(async () => [1, 2, 3], [], 1000);
    assert.deepEqual(out, [1, 2, 3]);
  });

  it('returns the fallback when the producer exceeds the timeout', async () => {
    let timedOut = false;
    const out = await softFailTimedEnumeration(
      () => new Promise<number[]>((resolve) => setTimeout(() => resolve([9]), 200)),
      null,
      20,
      () => { timedOut = true; },
    );
    assert.equal(out, null);
    assert.equal(timedOut, true, 'onTimeout should fire on the timeout branch');
  });

  it('returns the fallback when the producer rejects before the timeout', async () => {
    let timedOut = false;
    const out = await softFailTimedEnumeration(
      async () => { throw new Error('boom'); },
      'fallback',
      1000,
      () => { timedOut = true; },
    );
    assert.equal(out, 'fallback');
    assert.equal(timedOut, false, 'onTimeout must NOT fire on the rejection branch');
  });

  it('returns the fallback when the producer throws synchronously', async () => {
    const out = await softFailTimedEnumeration<string>(
      () => { throw new Error('sync boom'); },
      'sync-fallback',
      1000,
    );
    assert.equal(out, 'sync-fallback');
  });

  it('swallows a late rejection after a timeout (no unhandled rejection)', async () => {
    const seen: unknown[] = [];
    const onUnhandled = (reason: unknown): void => { seen.push(reason); };
    process.on('unhandledRejection', onUnhandled);
    try {
      const out = await softFailTimedEnumeration(
        () => new Promise<number>((_, reject) => setTimeout(() => reject(new Error('late')), 30)),
        -1,
        5,
      );
      assert.equal(out, -1);
      // Let the late rejection settle; it must have been swallowed.
      await new Promise((r) => setTimeout(r, 60));
      assert.equal(seen.length, 0, 'late rejection should be swallowed');
    } finally {
      process.off('unhandledRejection', onUnhandled);
    }
  });

  it('uses the env-resolved default timeout when none is passed', async () => {
    const saved = process.env[TOOL_ENUM_TIMEOUT_ENV];
    try {
      process.env[TOOL_ENUM_TIMEOUT_ENV] = '15';
      const start = Date.now();
      const out = await softFailTimedEnumeration(
        () => new Promise<number>((resolve) => setTimeout(() => resolve(1), 500)),
        0,
      );
      assert.equal(out, 0);
      assert.ok(Date.now() - start < 300, 'should time out at the env-resolved 15ms, well before 500ms');
    } finally {
      if (saved === undefined) delete process.env[TOOL_ENUM_TIMEOUT_ENV];
      else process.env[TOOL_ENUM_TIMEOUT_ENV] = saved;
    }
  });

  it('treats a negative timeout as an immediate (0ms) soft-fail', async () => {
    const out = await softFailTimedEnumeration(
      () => new Promise<number>((resolve) => setTimeout(() => resolve(1), 100)),
      42,
      -10,
    );
    assert.equal(out, 42);
  });
});

afterEach(() => { /* no shared state to reset */ });
