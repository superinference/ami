import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  cronListTool,
  isActiveJob,
  sortJobsForDisplay,
  CRON_LIST_STATUSES,
} from '../src/tools/cron-list';
import { getScheduler, resetScheduler } from '../src/cron';
import type { CronJob } from '../src/cron';
import type { ToolContext } from '../src/types';

// Each test uses a fresh tmp dir + a fresh scheduler singleton so there is no
// cross-test leakage through the on-disk store or the module-level _scheduler.
let tmpDir: string;
const ctx = (agentId?: string): ToolContext => ({ cwd: tmpDir, ...(agentId ? { agentId } : {}) } as ToolContext);

beforeEach(() => {
  resetScheduler();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-list-parity-'));
});
afterEach(() => {
  resetScheduler();
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

const mk = (cron: string, prompt: string, recurring = true): CronJob =>
  getScheduler(tmpDir).create({ cron, prompt, recurring, durable: false });

describe('cron-list parity: deterministic next-fire ordering', () => {
  // GAP: the list rendered jobs in Map insertion order, which is meaningless to a
  // user. Parity: soonest upcoming fire is shown first.
  it('orders jobs by soonest nextRun, not insertion order', async () => {
    const first = mk('*/5 * * * *', 'created first');   // inserted first
    const second = mk('*/5 * * * *', 'created second');  // inserted second
    // Force the SECOND-inserted job to fire sooner than the first.
    first.nextRun = Date.now() + 10_000_000;
    second.nextRun = Date.now() + 1_000;

    const out = (await cronListTool.execute({}, ctx())).output;
    const idxFirst = out.indexOf(first.id);
    const idxSecond = out.indexOf(second.id);
    assert.ok(idxFirst >= 0 && idxSecond >= 0, 'both jobs listed');
    // With sorting, the sooner-firing (second-inserted) job comes first.
    // On revert (insertion order), idxFirst < idxSecond and this fails.
    assert.ok(idxSecond < idxFirst, 'soonest-firing job is listed first');
  });

  it('sinks inactive (no nextRun) jobs below active ones', async () => {
    const dead = mk('*/5 * * * *', 'inactive one');     // inserted first
    const live = mk('*/5 * * * *', 'active one');        // inserted second
    dead.nextRun = undefined;                            // inactive
    live.nextRun = Date.now() + 1_000;                   // active

    const out = (await cronListTool.execute({}, ctx())).output;
    // Active job (inserted second) must appear before the inactive (inserted first).
    // On revert, insertion order keeps the inactive job first → fails.
    assert.ok(out.indexOf(live.id) < out.indexOf(dead.id), 'active before inactive');
  });

  it('sortJobsForDisplay is a stable, pure helper', () => {
    const a = { id: 'cron_2_a', nextRun: 200 } as CronJob;
    const b = { id: 'cron_1_b', nextRun: 100 } as CronJob;
    const c = { id: 'cron_3_c' } as CronJob; // inactive
    const sorted = sortJobsForDisplay([a, b, c]);
    assert.deepEqual(sorted.map(j => j.id), ['cron_1_b', 'cron_2_a', 'cron_3_c']);
    // Pure: input array not mutated.
    assert.equal([a, b, c][0], a);
  });
});

describe('cron-list parity: last-run surfacing', () => {
  // GAP: CronJob tracked lastRun but the list never showed it, so a user could
  // not tell a job that has fired from one that never has.
  it('shows a "Last:" line once a job has fired', async () => {
    const job = mk('*/5 * * * *', 'periodic job');
    const before = (await cronListTool.execute({}, ctx())).output;
    assert.ok(!before.includes('Last:'), 'no Last line before first fire');

    getScheduler(tmpDir).markRun(job.id); // records lastRun
    const after = (await cronListTool.execute({}, ctx())).output;
    // On revert, "Last:" is never emitted even after a run → fails here.
    assert.ok(after.includes('Last:'), 'Last-run line surfaced after a fire');
  });
});

describe('cron-list parity: inactive job labelling', () => {
  // GAP: a job with no upcoming fire rendered "Next: N/A" but carried no explicit
  // status flag, so it looked identical to a healthy job in the flags column.
  it('labels a job with no nextRun as "inactive" in its flags', async () => {
    const job = mk('*/5 * * * *', 'stalled job');
    job.nextRun = undefined;
    const out = (await cronListTool.execute({}, ctx())).output;
    assert.ok(out.includes('N/A'), 'Next shows N/A');
    // On revert, the "inactive" flag is never added → fails.
    assert.ok(/\(.*inactive.*\)/.test(out), 'flags include inactive marker');
  });

  it('isActiveJob reflects presence of nextRun', () => {
    assert.equal(isActiveJob({ nextRun: 123 } as CronJob), true);
    assert.equal(isActiveJob({} as CronJob), false);
  });
});

describe('cron-list parity: status filter', () => {
  // GAP: the tool took no args, so a user could not narrow a long list to just
  // the live (or dead) jobs. Parity: optional status=all|active|inactive.
  it('status=active returns only jobs with an upcoming fire', async () => {
    const live = mk('*/5 * * * *', 'live job');
    const dead = mk('*/5 * * * *', 'dead job');
    live.nextRun = Date.now() + 1_000;
    dead.nextRun = undefined;

    const res = await cronListTool.execute({ status: 'active' }, ctx());
    // On revert, status is an unknown param → isError, output lacks live.id → fails.
    assert.ok(!res.isError, 'status filter accepted');
    assert.ok(res.output.includes(live.id), 'active job present');
    assert.ok(!res.output.includes(dead.id), 'inactive job excluded');
  });

  it('status=inactive returns only jobs without an upcoming fire', async () => {
    const live = mk('*/5 * * * *', 'live job');
    const dead = mk('*/5 * * * *', 'dead job');
    live.nextRun = Date.now() + 1_000;
    dead.nextRun = undefined;

    const res = await cronListTool.execute({ status: 'inactive' }, ctx());
    assert.ok(!res.isError);
    assert.ok(res.output.includes(dead.id), 'inactive job present');
    assert.ok(!res.output.includes(live.id), 'active job excluded');
  });

  it('empty filter result reflects the filter in the message', async () => {
    mk('*/5 * * * *', 'only an active job'); // active by default (has nextRun)
    const res = await cronListTool.execute({ status: 'inactive' }, ctx());
    // On revert, status is rejected → isError and message is "takes no parameters" → fails.
    assert.ok(!res.isError, 'filter accepted');
    assert.ok(res.output.includes('No inactive scheduled jobs'), 'filter echoed in empty-state');
  });

  it('rejects an invalid status value WITH the valid options', async () => {
    const res = await cronListTool.execute({ status: 'bogus' }, ctx());
    assert.ok(res.isError, 'invalid status is an error');
    // On revert, the error is "takes no parameters" and does NOT list the statuses → fails.
    for (const s of CRON_LIST_STATUSES) {
      assert.ok(res.output.includes(s), `error lists valid status "${s}"`);
    }
  });

  it('still rejects genuinely unknown parameters', async () => {
    const res = await cronListTool.execute({ nope: 1 } as Record<string, unknown>, ctx());
    assert.ok(res.isError, 'unknown param rejected');
    assert.ok(res.output.toLowerCase().includes('unknown'), 'names the offending param class');
  });

  it('status=all (default) lists everything, unchanged happy path', async () => {
    mk('*/5 * * * *', 'alpha');
    const res = await cronListTool.execute({ status: 'all' }, ctx());
    assert.ok(!res.isError);
    assert.ok(res.output.includes('1 scheduled job'));
    assert.ok(res.output.includes('alpha'));
  });
});
