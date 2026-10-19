import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { cronDeleteTool } from '../src/tools/cron-delete';
import { cronListTool } from '../src/tools/cron-list';
import { getScheduler } from '../src/cron';
import type { ToolContext } from '../src/types';

describe('cronDeleteTool', () => {
  let tmpDir: string;
  let ctx: ToolContext;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-delete-test-'));
    ctx = { cwd: tmpDir } as ToolContext;
  });

  after(() => { fs.rmSync(tmpDir, { recursive: true }); });

  it('has correct name and schema', () => {
    assert.equal(cronDeleteTool.name, 'cron_delete');
    assert.equal(cronDeleteTool.isReadOnly, false);
    assert.deepEqual(cronDeleteTool.inputSchema.required, ['id']);
  });

  it('returns error when id is empty', async () => {
    const result = await cronDeleteTool.execute!({ id: '' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('required'));
  });

  it('returns error when job not found', async () => {
    const result = await cronDeleteTool.execute!({ id: 'nonexistent-job' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('not found'));
  });

  it('deletes an existing job', async () => {
    const sched = getScheduler(tmpDir);
    const job = sched.create({
      prompt: 'test prompt',
      cron: '* * * * *',
      recurring: false,
      durable: false,
    });
    const result = await cronDeleteTool.execute!({ id: job.id }, ctx);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Deleted'));
    assert.ok(result.output.includes(job.id));
  });
});

describe('cronListTool', () => {
  let tmpDir: string;
  let ctx: ToolContext;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-list-test-'));
    ctx = { cwd: tmpDir } as ToolContext;
  });

  after(() => { fs.rmSync(tmpDir, { recursive: true }); });

  it('has correct name and schema', () => {
    assert.equal(cronListTool.name, 'cron_list');
    assert.ok(cronListTool.isReadOnly);
  });

  it('returns "no jobs" when empty', async () => {
    const result = await cronListTool.execute!({}, ctx);
    assert.ok(result.output.includes('No scheduled jobs'));
  });

  it('lists created jobs', async () => {
    const sched = getScheduler(tmpDir);
    sched.create({
      prompt: 'check status',
      cron: '*/5 * * * *',
      recurring: true,
      durable: false,
    });
    const result = await cronListTool.execute!({}, ctx);
    assert.ok(result.output.includes('1 scheduled job'));
    assert.ok(result.output.includes('check status'));
    assert.ok(result.output.includes('recurring'));
  });
});
