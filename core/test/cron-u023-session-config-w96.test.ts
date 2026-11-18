// U023-w96 — per-job session-config knobs on a scheduled cron job: a `model`
// override and an `allowedTools` whitelist (parity with Claude Code's scheduled-job
// `session_context.model` + `allowed_tools`, scheduleRemoteAgents.ts:L204-L211).
// AMI's cron only stored a bare prompt enqueued into the current session; a job can
// now carry its own model + toolset, recorded on the CronJob and surfaced when it fires.

import { describe, it, beforeEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { cronCreateTool } from '../src/tools/cron-create';
import { getScheduler, resetScheduler } from '../src/cron';
import type { ToolContext } from '../src/types';

let tmpDir: string;
const tmpDirs: string[] = [];
function ctxFor(extra: Partial<ToolContext> = {}): ToolContext {
  return { cwd: tmpDir, ...extra } as ToolContext;
}

beforeEach(() => {
  resetScheduler();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-u023-w96-'));
  tmpDirs.push(tmpDir);
});

after(() => {
  resetScheduler();
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

describe('cron_create U023/w96 — per-job model + allowedTools', () => {
  it('stores a per-job model override and allowed-tools whitelist on the CronJob', async () => {
    const r = await cronCreateTool.execute(
      { cron: '*/5 * * * *', prompt: 'run a health check', model: 'claude-haiku-4', allowedTools: ['bash', 'file_read'] },
      ctxFor(),
    );
    assert.equal(r.isError, undefined);
    const jobs = getScheduler(tmpDir).list();
    assert.equal(jobs.length, 1);
    assert.equal(jobs[0].model, 'claude-haiku-4');
    assert.deepEqual(jobs[0].allowedTools, ['bash', 'file_read']);
    // The overrides are echoed in the confirmation output.
    assert.match(r.output, /Model override: claude-haiku-4/);
    assert.match(r.output, /Allowed tools: bash, file_read/);
  });

  it('omits both fields when not supplied (inherit the session defaults)', async () => {
    const r = await cronCreateTool.execute(
      { cron: '*/5 * * * *', prompt: 'plain job' },
      ctxFor(),
    );
    assert.equal(r.isError, undefined);
    const job = getScheduler(tmpDir).list()[0];
    assert.equal(job.model, undefined);
    assert.equal(job.allowedTools, undefined);
    assert.ok(!/Model override/.test(r.output));
    assert.ok(!/Allowed tools/.test(r.output));
  });

  it('rejects a blank model override', async () => {
    const r = await cronCreateTool.execute(
      { cron: '*/5 * * * *', prompt: 'p', model: '   ' },
      ctxFor(),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /model must be a non-empty string/);
  });

  it('rejects a non-array allowedTools', async () => {
    const r = await cronCreateTool.execute(
      { cron: '*/5 * * * *', prompt: 'p', allowedTools: 'bash' },
      ctxFor(),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /allowedTools must be an array/);
  });

  it('rejects an allowedTools array with a blank entry', async () => {
    const r = await cronCreateTool.execute(
      { cron: '*/5 * * * *', prompt: 'p', allowedTools: ['bash', ''] },
      ctxFor(),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /allowedTools must be an array/);
  });

  it('rejects an empty allowedTools array (deny-everything is almost always a mistake)', async () => {
    const r = await cronCreateTool.execute(
      { cron: '*/5 * * * *', prompt: 'p', allowedTools: [] },
      ctxFor(),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /at least one tool/);
  });
});
