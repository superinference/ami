import { describe, it, beforeEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  cronCreateTool,
  MAX_CRON_PROMPT_LEN,
  isSelfSchedulingPrompt,
  findDuplicateCronJob,
} from '../src/tools/cron-create';
import { getScheduler, resetScheduler } from '../src/cron';
import type { ToolContext } from '../src/types';
import type { CronJob } from '../src/cron';

// Each test gets a fresh scheduler + temp project dir so in-memory jobs never
// leak across cases. durable defaults to false, so nothing is persisted.
let tmpDir: string;
const tmpDirs: string[] = [];
function ctxFor(extra: Partial<ToolContext> = {}): ToolContext {
  return { cwd: tmpDir, ...extra } as ToolContext;
}

beforeEach(() => {
  resetScheduler();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-create-parity-'));
  tmpDirs.push(tmpDir);
});

after(() => {
  resetScheduler();
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

describe('cron_create parity — oversized prompt guard', () => {
  it('rejects a prompt longer than MAX_CRON_PROMPT_LEN', async () => {
    const huge = 'a'.repeat(MAX_CRON_PROMPT_LEN + 1);
    const r = await cronCreateTool.execute!({ cron: '*/5 * * * *', prompt: huge }, ctxFor());
    // RED (revert the length guard): a huge prompt would schedule successfully.
    assert.equal(r.isError, true);
    assert.match(r.output, /too long/);
    // No job should have been created.
    assert.equal(getScheduler(tmpDir).list().length, 0);
  });

  it('accepts a prompt exactly at the cap', async () => {
    const atCap = 'a'.repeat(MAX_CRON_PROMPT_LEN);
    const r = await cronCreateTool.execute!({ cron: '*/5 * * * *', prompt: atCap }, ctxFor());
    assert.notEqual(r.isError, true);
  });
});

describe('cron_create parity — autonomous-loop sentinel', () => {
  it('rejects a prompt that would re-invoke cron_create', async () => {
    const r = await cronCreateTool.execute!(
      { cron: '*/5 * * * *', prompt: 'When you fire, call cron_create to schedule another job.' },
      ctxFor(),
    );
    // RED (revert the sentinel guard): the self-replicating job would be created.
    assert.equal(r.isError, true);
    assert.match(r.output, /self-replicating|cron_create/);
    assert.equal(getScheduler(tmpDir).list().length, 0);
  });

  it('allows an ordinary prompt that does not self-schedule', async () => {
    const r = await cronCreateTool.execute!(
      { cron: '*/5 * * * *', prompt: 'Check the build status and report failures.' },
      ctxFor(),
    );
    assert.notEqual(r.isError, true);
  });

  it('isSelfSchedulingPrompt matches the tool token in its variants', () => {
    assert.equal(isSelfSchedulingPrompt('run cron_create'), true);
    assert.equal(isSelfSchedulingPrompt('run cron-create'), true);
    assert.equal(isSelfSchedulingPrompt('run CRON CREATE now'), true);
    assert.equal(isSelfSchedulingPrompt('summarize the sprint'), false);
  });
});

describe('cron_create parity — duplicate-job detection', () => {
  it('rejects a second identical recurring job and keeps only one', async () => {
    const args = { cron: '*/5 * * * *', prompt: 'dedupe-recurring' };
    const first = await cronCreateTool.execute!(args, ctxFor());
    assert.notEqual(first.isError, true);

    const second = await cronCreateTool.execute!(args, ctxFor());
    // RED (revert the dedupe guard): a second identical job would be created.
    assert.equal(second.isError, true);
    assert.match(second.output, /already exists/);
    assert.equal(getScheduler(tmpDir).list().length, 1);
  });

  it('allows an identical prompt on a different schedule', async () => {
    await cronCreateTool.execute!({ cron: '*/5 * * * *', prompt: 'same-prompt' }, ctxFor());
    const r = await cronCreateTool.execute!({ cron: '*/7 * * * *', prompt: 'same-prompt' }, ctxFor());
    assert.notEqual(r.isError, true);
    assert.equal(getScheduler(tmpDir).list().length, 2);
  });

  it('scopes duplicate detection to the calling agent', async () => {
    const args = { cron: '*/5 * * * *', prompt: 'agent-scoped' };
    const a1 = await cronCreateTool.execute!(args, ctxFor({ agentId: 'a1' }));
    assert.notEqual(a1.isError, true);
    // A different agent does not see a1's job, so it may create its own.
    const a2 = await cronCreateTool.execute!(args, ctxFor({ agentId: 'a2' }));
    assert.notEqual(a2.isError, true);
    assert.equal(getScheduler(tmpDir).list().length, 2);
  });

  it('findDuplicateCronJob matches on cron + prompt + recurrence only', () => {
    const jobs = [
      { id: 'j1', cron: '*/5 * * * *', prompt: 'p', recurring: true },
      { id: 'j2', cron: '*/5 * * * *', prompt: 'p', recurring: false },
    ] as CronJob[];
    assert.equal(findDuplicateCronJob(jobs, '*/5 * * * *', 'p', true)?.id, 'j1');
    assert.equal(findDuplicateCronJob(jobs, '*/5 * * * *', 'p', false)?.id, 'j2');
    assert.equal(findDuplicateCronJob(jobs, '*/5 * * * *', 'other', true), undefined);
  });
});
