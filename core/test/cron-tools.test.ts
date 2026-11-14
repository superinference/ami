import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { cronDeleteTool } from '../src/tools/cron-delete';
import { cronListTool } from '../src/tools/cron-list';
import { cronCreateTool } from '../src/tools/cron-create';
import { getScheduler, resetScheduler } from '../src/cron';
import type { ToolContext } from '../src/types';

const resetSched = () => resetScheduler();

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

// ---------------------------------------------------------------------------
// Parity enhancements for the cron tool wrappers
// ---------------------------------------------------------------------------

describe('cron tool wrappers — parity enhancements', () => {
  let dir: string;
  const ctxFor = (over: Partial<ToolContext> = {}) => ({ cwd: dir, ...over } as ToolContext);

  before(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-parity-tools-')); });
  after(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('cron_create rejects unknown parameters', async () => {
    resetSched();
    const r = await cronCreateTool.execute!({ cron: '* * * * *', prompt: 'p', bogus: 1 }, ctxFor());
    assert.equal(r.isError, true);
    assert.match(r.output, /unknown parameter/);
  });

  it('cron_create coerces string boolean flags', async () => {
    resetSched();
    const r = await cronCreateTool.execute!(
      { cron: '*/5 * * * *', prompt: 'p', recurring: 'false', durable: 'true' },
      ctxFor(),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /one-shot/);
    assert.match(r.output, /Durable: true/);
  });

  it('cron_create rejects an impossible calendar cron', async () => {
    resetSched();
    const r = await cronCreateTool.execute!({ cron: '0 0 30 2 *', prompt: 'p' }, ctxFor());
    assert.equal(r.isError, true);
    assert.match(r.output, /never matches/);
  });

  it('cron_create is blocked by the kill-switch', async () => {
    resetSched();
    process.env.AMI_DISABLE_CRON = '1';
    try {
      const r = await cronCreateTool.execute!({ cron: '* * * * *', prompt: 'p' }, ctxFor());
      assert.equal(r.isError, true);
      assert.match(r.output, /disabled by operator/);
    } finally {
      delete process.env.AMI_DISABLE_CRON;
    }
  });

  it('cron_create degrades a durable request to session-only when durable persistence is gated off', async () => {
    resetSched();
    process.env.AMI_DISABLE_DURABLE_CRON = '1';
    try {
      const r = await cronCreateTool.execute!({ cron: '0 9 * * *', prompt: 'durable-degrade-test', durable: true }, ctxFor());
      assert.equal(r.isError, undefined);
      assert.match(r.output, /Durable: false/);
      assert.match(r.output, /disabled by operator policy/);
      const job = getScheduler(dir).list().find(j => j.prompt === 'durable-degrade-test')!;
      assert.equal(job.durable, false);
    } finally {
      delete process.env.AMI_DISABLE_DURABLE_CRON;
    }
  });

  it('cron_create tags a job with the calling agentId', async () => {
    resetSched();
    await cronCreateTool.execute!({ cron: '0 9 * * *', prompt: 'mine-tag' }, ctxFor({ agentId: 'a1' }));
    const job = getScheduler(dir).list().find(j => j.prompt === 'mine-tag')!;
    assert.equal(job.agentId, 'a1');
  });

  it('cron_list scopes to the calling agent', async () => {
    resetSched();
    const s = getScheduler(dir);
    s.create({ cron: '0 9 * * *', prompt: 'global' });
    s.create({ cron: '0 9 * * *', prompt: 'a1', agentId: 'a1' });
    s.create({ cron: '0 9 * * *', prompt: 'a2', agentId: 'a2' });
    const r = await cronListTool.execute!({}, ctxFor({ agentId: 'a1' }));
    assert.match(r.output, /Prompt: a1/);
    assert.match(r.output, /Prompt: global/);
    assert.doesNotMatch(r.output, /Prompt: a2/);
  });

  it('cron_list rejects unknown parameters', async () => {
    resetSched();
    const r = await cronListTool.execute!({ nope: 1 } as Record<string, unknown>, ctxFor());
    assert.equal(r.isError, true);
    assert.match(r.output, /takes no parameters/);
  });

  it('cron_delete refuses to delete another agent\'s job', async () => {
    resetSched();
    const s = getScheduler(dir);
    const j = s.create({ cron: '0 9 * * *', prompt: 'a2', agentId: 'a2' });
    const r = await cronDeleteTool.execute!({ id: j.id }, ctxFor({ agentId: 'a1' }));
    assert.equal(r.isError, true);
    assert.equal(r.metadata?.errorCode, 2);
  });

  it('cron_delete validates the id format', async () => {
    resetSched();
    const r = await cronDeleteTool.execute!({ id: 'bad id!' }, ctxFor());
    assert.equal(r.isError, true);
    assert.equal(r.metadata?.errorCode, 1);
  });

  it('cron_list truncates a multi-line prompt to a single line', async () => {
    resetSched();
    getScheduler(dir).create({ cron: '0 9 * * *', prompt: 'line one\nline two\nline three' });
    const r = await cronListTool.execute!({}, ctxFor());
    assert.match(r.output, /line one …/);
    assert.doesNotMatch(r.output, /line two/);
  });
});
