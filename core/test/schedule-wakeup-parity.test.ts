import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  scheduleWakeupTool,
  clampWakeupDelay,
  resetLoopPacingState,
  LOOP_CONTINUE_SENTINEL,
  NOOP_STREAK_WARN_THRESHOLD,
  WAKEUP_MIN_DELAY_SECONDS,
  WAKEUP_MAX_DELAY_SECONDS,
} from '../src/tools/schedule-wakeup';
import { getScheduler, resetScheduler } from '../src/cron';
import type { ToolContext } from '../src/types';

// Parity tests for the ScheduleWakeup (/loop pacing) tool as an additive superset
// of Claude Code's behaviour: delay clamp/rounding, stop short-circuit, no-op
// streak semantics, sentinel prompt passthrough, and error surfacing.
describe('scheduleWakeupTool — parity', () => {
  let tmpDir: string;
  let ctx: ToolContext;

  beforeEach(() => {
    // getScheduler() is a process-global singleton (ignores cwd after the first
    // call) — reset it so each test gets a scheduler bound to its own tmpDir and
    // the 50-job saturation in the error test can't leak forward.
    resetScheduler();
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'wakeup-parity-'));
    ctx = { cwd: tmpDir } as ToolContext;
    resetLoopPacingState();
  });

  afterEach(() => {
    resetScheduler();
    resetLoopPacingState();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  // --- FIX 1: integer rounding + non-finite clamp -------------------------
  describe('delay clamp + rounding', () => {
    it('clampWakeupDelay rounds fractional seconds to nearest integer', () => {
      // GAP: pre-fix passed `delaySeconds` straight through (no Math.round), so a
      // fractional value rendered as "120.9s".
      assert.equal(clampWakeupDelay(120.9), 121);
      assert.equal(clampWakeupDelay(120.4), 120);
    });

    it('clampWakeupDelay collapses non-finite / junk to the minimum', () => {
      assert.equal(clampWakeupDelay(NaN), WAKEUP_MIN_DELAY_SECONDS);
      assert.equal(clampWakeupDelay(Infinity), WAKEUP_MIN_DELAY_SECONDS); // non-finite collapses to the floor
      assert.equal(clampWakeupDelay('not-a-number'), WAKEUP_MIN_DELAY_SECONDS);
      assert.equal(clampWakeupDelay(undefined), WAKEUP_MIN_DELAY_SECONDS);
      assert.equal(clampWakeupDelay(-5), WAKEUP_MIN_DELAY_SECONDS);
    });

    it('clampWakeupDelay honours the [60,3600] window', () => {
      assert.equal(clampWakeupDelay(10), 60);
      assert.equal(clampWakeupDelay(9999), 3600);
      assert.equal(clampWakeupDelay(600), 600);
    });

    it('execute renders a rounded integer delay in the output (discriminator)', async () => {
      const result = await scheduleWakeupTool.execute!(
        { delaySeconds: 120.9, prompt: 'x', reason: 'frac' },
        ctx,
      );
      assert.ok(!result.isError);
      // RED (pre-fix): output contained "120.9s".
      assert.ok(result.output.includes('121s'), result.output);
      assert.ok(!result.output.includes('120.9s'), result.output);
      assert.equal((result.metadata as { delaySeconds: number }).delaySeconds, 121);
    });
  });

  // --- FIX 2: stop short-circuit -----------------------------------------
  describe('stop short-circuit', () => {
    it('stop:true schedules NO job and ignores delaySeconds/prompt', async () => {
      const before = getScheduler(tmpDir).list().length;
      const result = await scheduleWakeupTool.execute!(
        { delaySeconds: 120, prompt: 'should be ignored', reason: 'done', stop: true },
        ctx,
      );
      assert.ok(!result.isError);
      // RED (pre-fix): no stop handling → a job WAS created and output said
      // "Wakeup scheduled".
      assert.equal(getScheduler(tmpDir).list().length, before, 'no job must be scheduled on stop');
      assert.ok(!result.output.includes('Wakeup scheduled'), result.output);
      assert.ok(result.output.toLowerCase().includes('stopped'), result.output);
      assert.equal((result.metadata as { stopped: boolean }).stopped, true);
    });

    it('stop:true resets an accumulated no-op streak', async () => {
      await scheduleWakeupTool.execute!({ delaySeconds: 120, reason: 'n', noop: true }, ctx);
      await scheduleWakeupTool.execute!({ delaySeconds: 120, reason: 'n', noop: true }, ctx);
      await scheduleWakeupTool.execute!({ delaySeconds: 120, reason: 'done', stop: true }, ctx);
      // After stop, a fresh no-op should start the streak at 1 again.
      const r = await scheduleWakeupTool.execute!({ delaySeconds: 120, reason: 'n', noop: true }, ctx);
      assert.equal((r.metadata as { noopStreak: number }).noopStreak, 1);
    });
  });

  // --- FIX 3: no-op streak semantics -------------------------------------
  describe('no-op streak semantics', () => {
    it('increments the streak across consecutive no-op wakeups', async () => {
      const r1 = await scheduleWakeupTool.execute!({ delaySeconds: 120, reason: 'n', noop: true }, ctx);
      const r2 = await scheduleWakeupTool.execute!({ delaySeconds: 120, reason: 'n', noop: true }, ctx);
      // RED (pre-fix): `noop` was ignored; metadata.noopStreak did not exist.
      assert.equal((r1.metadata as { noopStreak: number }).noopStreak, 1);
      assert.equal((r2.metadata as { noopStreak: number }).noopStreak, 2);
      assert.ok(r2.output.includes('No-op streak: 2'), r2.output);
    });

    it('surfaces an escalation hint once the streak reaches the threshold', async () => {
      let result!: Awaited<ReturnType<NonNullable<typeof scheduleWakeupTool.execute>>>;
      for (let i = 0; i < NOOP_STREAK_WARN_THRESHOLD; i++) {
        result = await scheduleWakeupTool.execute!({ delaySeconds: 120, reason: 'n', noop: true }, ctx);
      }
      assert.equal((result.metadata as { noopStreak: number }).noopStreak, NOOP_STREAK_WARN_THRESHOLD);
      // RED (pre-fix): no escalation text existed.
      assert.ok(result.output.includes('consecutive no-op'), result.output);
      assert.ok(result.output.toLowerCase().includes('stop:true'), result.output);
    });

    it('a real (non-noop) wakeup resets the streak to zero', async () => {
      await scheduleWakeupTool.execute!({ delaySeconds: 120, reason: 'n', noop: true }, ctx);
      await scheduleWakeupTool.execute!({ delaySeconds: 120, reason: 'n', noop: true }, ctx);
      const real = await scheduleWakeupTool.execute!({ delaySeconds: 120, prompt: 'do work', reason: 'r' }, ctx);
      assert.equal((real.metadata as { noopStreak: number }).noopStreak, 0);
      assert.ok(!real.output.includes('No-op streak'), real.output);
      // And the next no-op begins a fresh streak at 1.
      const again = await scheduleWakeupTool.execute!({ delaySeconds: 120, reason: 'n', noop: true }, ctx);
      assert.equal((again.metadata as { noopStreak: number }).noopStreak, 1);
    });

    it('still schedules a wakeup for a no-op turn (loop keeps pacing)', async () => {
      const before = getScheduler(tmpDir).list().length;
      await scheduleWakeupTool.execute!({ delaySeconds: 120, reason: 'n', noop: true }, ctx);
      assert.equal(getScheduler(tmpDir).list().length, before + 1);
    });
  });

  // --- FIX 4: sentinel prompt passthrough --------------------------------
  describe('sentinel continuation prompt', () => {
    it('fires the exported sentinel verbatim when the prompt is omitted', async () => {
      const result = await scheduleWakeupTool.execute!(
        { delaySeconds: 120, reason: 'loop continues' },
        ctx,
      );
      assert.ok(!result.isError);
      const jobs = getScheduler(tmpDir).list();
      const last = jobs[jobs.length - 1];
      assert.equal(last.prompt, LOOP_CONTINUE_SENTINEL);
      // RED (pre-fix): no autoContinuation metadata flag existed.
      assert.equal((result.metadata as { autoContinuation: boolean }).autoContinuation, true);
    });

    it('marks an explicit prompt as NOT an auto-continuation', async () => {
      const result = await scheduleWakeupTool.execute!(
        { delaySeconds: 120, prompt: 'real work', reason: 'r' },
        ctx,
      );
      const jobs = getScheduler(tmpDir).list();
      assert.equal(jobs[jobs.length - 1].prompt, 'real work');
      assert.equal((result.metadata as { autoContinuation: boolean }).autoContinuation, false);
    });

    it('treats whitespace-only prompt as a sentinel continuation', async () => {
      const result = await scheduleWakeupTool.execute!(
        { delaySeconds: 120, prompt: '   ', reason: 'r' },
        ctx,
      );
      assert.equal((result.metadata as { autoContinuation: boolean }).autoContinuation, true);
      assert.ok(result.output.includes('fallback'), result.output);
    });
  });

  // --- FIX 5: scheduler error surfacing ----------------------------------
  describe('error surfacing', () => {
    it('returns isError (not a rejection) when the scheduler throws', async () => {
      const sched = getScheduler(tmpDir);
      // Saturate the scheduler up to its job cap so the next create() throws.
      for (let i = 0; i < 50; i++) {
        sched.create({ cron: '0 0 1 1 *', prompt: 'p', recurring: false, durable: false });
      }
      // RED (pre-fix): no try/catch → execute() rejects instead of resolving.
      let result!: Awaited<ReturnType<NonNullable<typeof scheduleWakeupTool.execute>>>;
      await assert.doesNotReject(async () => {
        result = await scheduleWakeupTool.execute!(
          { delaySeconds: 120, prompt: 'x', reason: 'overflow' },
          ctx,
        );
      });
      assert.equal(result.isError, true);
      assert.ok(result.output.includes('Failed to schedule wakeup'), result.output);
      assert.ok((result.metadata as { error: string }).error.includes('Maximum'), result.output);
    });
  });

  // --- schema additivity (non-regression) --------------------------------
  it('adds stop/noop to the schema without changing required fields', () => {
    const props = scheduleWakeupTool.inputSchema.properties as Record<string, unknown>;
    assert.ok(props.stop);
    assert.ok(props.noop);
    assert.deepEqual(scheduleWakeupTool.inputSchema.required, ['delaySeconds', 'reason']);
  });

  it('resetLoopPacingState() with no cwd clears all streaks', async () => {
    await scheduleWakeupTool.execute!({ delaySeconds: 120, reason: 'n', noop: true }, ctx);
    resetLoopPacingState();
    const r = await scheduleWakeupTool.execute!({ delaySeconds: 120, reason: 'n', noop: true }, ctx);
    assert.equal((r.metadata as { noopStreak: number }).noopStreak, 1);
  });
});
