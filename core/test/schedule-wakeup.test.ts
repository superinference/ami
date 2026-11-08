import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { scheduleWakeupTool } from '../src/tools/schedule-wakeup';
import { getScheduler } from '../src/cron';
import type { ToolContext } from '../src/types';

describe('scheduleWakeupTool', () => {
  let tmpDir: string;
  let ctx: ToolContext;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'wakeup-test-'));
    ctx = { cwd: tmpDir } as ToolContext;
  });

  after(() => { fs.rmSync(tmpDir, { recursive: true }); });

  it('has correct name and schema', () => {
    assert.equal(scheduleWakeupTool.name, 'schedule_wakeup');
    assert.equal(scheduleWakeupTool.isReadOnly, false);
    assert.deepEqual(scheduleWakeupTool.inputSchema.required, ['delaySeconds', 'reason']);
  });

  it('schedules a wakeup and returns job id', async () => {
    const result = await scheduleWakeupTool.execute!(
      { delaySeconds: 120, prompt: 'check status', reason: 'waiting for CI' },
      ctx,
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Wakeup scheduled'));
    assert.ok(result.output.includes('120s'));
    assert.ok(result.output.includes('waiting for CI'));
  });

  it('clamps delay to minimum 60s', async () => {
    const result = await scheduleWakeupTool.execute!(
      { delaySeconds: 10, prompt: 'test', reason: 'short' },
      ctx,
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('60s'));
  });

  it('clamps delay to maximum 3600s', async () => {
    const result = await scheduleWakeupTool.execute!(
      { delaySeconds: 9999, prompt: 'test', reason: 'long' },
      ctx,
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('3600s'));
  });

  // B3: a missing/empty/whitespace prompt must NOT end a self-paced loop — it
  // schedules a fallback continuation wakeup instead of erroring.
  it('schedules a fallback wakeup when prompt is empty', async () => {
    const before = getScheduler(tmpDir).list().length;
    const result = await scheduleWakeupTool.execute!(
      { delaySeconds: 120, prompt: '', reason: 'test' },
      ctx,
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Wakeup scheduled'));
    assert.ok(result.output.includes('fallback'));
    assert.equal(getScheduler(tmpDir).list().length, before + 1);
  });

  it('schedules a fallback wakeup when prompt is whitespace-only', async () => {
    const result = await scheduleWakeupTool.execute!(
      { delaySeconds: 120, prompt: '   ', reason: 'test' },
      ctx,
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('fallback'));
  });

  it('schedules a fallback wakeup when prompt is omitted entirely', async () => {
    const before = getScheduler(tmpDir).list().length;
    const result = await scheduleWakeupTool.execute!(
      { delaySeconds: 120, reason: 'loop continues' },
      ctx,
    );
    assert.ok(!result.isError);
    assert.equal(getScheduler(tmpDir).list().length, before + 1);
  });

  it('uses a non-empty prompt verbatim (no fallback note)', async () => {
    const result = await scheduleWakeupTool.execute!(
      { delaySeconds: 120, prompt: 'do the real thing', reason: 'r' },
      ctx,
    );
    assert.ok(!result.isError);
    assert.ok(!result.output.includes('fallback'));
  });

  it('defaults delay to 60 when not provided', async () => {
    const result = await scheduleWakeupTool.execute!(
      { prompt: 'test', reason: 'default' },
      ctx,
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('60s'));
  });
});
