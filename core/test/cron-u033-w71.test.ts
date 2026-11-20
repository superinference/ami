/**
 * U033 (w71) — core/core/src/cron.ts additive parity improvements.
 *
 * Gaps implemented (plannings/parity/gaps.jsonl):
 *  - UTC-aware cron definition + display (`utc` flag on a job; computeNextRun reads
 *    fields in UTC; formatSchedule renders a UTC schedule in local time with a TZ
 *    label). upstreamRef: src/utils/cron.ts:L183-307 (cronToHuman `{ utc }` option,
 *    formatUtcTimeAsLocal, UTC dayIndex recomputation) + the UTC next-run path.
 *  - assistant-mode bypass of the busy/idle gate (setAssistantMode / isAssistantMode
 *    + AMI_CRON_ASSISTANT_MODE env). upstreamRef: src/hooks/useScheduledTasks.ts:L18-28
 *    (assistantMode knob that bypasses the isLoading gate).
 *
 * The UTC assertions are timezone-robust: they assert the absolute instant's UTC
 * fields match the cron, so they pass whatever timezone the host/CI runs in.
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { CronScheduler, formatClockTime, to12Hour, type CronJob } from '../src/cron';
import { cronCreateTool, resetCronScheduler } from '../src/tools/cron-create';
import { cronListTool } from '../src/tools/cron-list';
import type { ToolContext } from '../src/types';

const ctx = (cwd: string): ToolContext => ({ cwd, abortSignal: new AbortController().signal });

let dir: string;
const mk = () => fs.mkdtempSync(path.join(os.tmpdir(), 'cron-u033-w71-'));
const tick = (s: CronScheduler, cb: (j: CronJob) => void) =>
  (s as unknown as { tick: (f: (j: CronJob) => void) => void }).tick(cb);

beforeEach(() => { dir = mk(); });
afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
  delete process.env.AMI_CRON_ASSISTANT_MODE;
});

// ---------------------------------------------------------------------------
// UTC-aware next-run computation
// ---------------------------------------------------------------------------

describe('computeNextRun — utc interpretation', () => {
  it('a utc daily cron fires at the matching UTC wall-clock instant', () => {
    const s = new CronScheduler(dir, 's');
    const from = Date.UTC(2026, 0, 1, 0, 0, 0); // Jan 1 2026, 00:00 UTC
    const next = s.computeNextRun('30 14 * * *', from, true);
    assert.ok(next !== null);
    const d = new Date(next as number);
    assert.equal(d.getUTCHours(), 14, 'fires at 14:00 UTC');
    assert.equal(d.getUTCMinutes(), 30);
  });

  it('local vs utc interpretation address different clocks', () => {
    const s = new CronScheduler(dir, 's');
    const from = Date.UTC(2026, 0, 1, 0, 0, 0);
    const local = s.computeNextRun('30 14 * * *', from, false);
    const utc = s.computeNextRun('30 14 * * *', from, true);
    assert.ok(local !== null && utc !== null);
    // The local result matches the LOCAL clock; the utc result matches the UTC clock.
    assert.equal(new Date(local as number).getHours(), 14);
    assert.equal(new Date(local as number).getMinutes(), 30);
    assert.equal(new Date(utc as number).getUTCHours(), 14);
    assert.equal(new Date(utc as number).getUTCMinutes(), 30);
  });

  it('utc skip-ahead resolves a pinned future UTC date (month/day/hour/minute jumps)', () => {
    const s = new CronScheduler(dir, 's');
    const from = Date.UTC(2026, 0, 1, 0, 0, 0); // Jan 1
    const next = s.computeNextRun('30 14 15 6 *', from, true); // 14:30 UTC, Jun 15
    assert.ok(next !== null);
    const d = new Date(next as number);
    assert.equal(d.getUTCMonth(), 5, 'June (0-indexed 5)');
    assert.equal(d.getUTCDate(), 15);
    assert.equal(d.getUTCHours(), 14);
    assert.equal(d.getUTCMinutes(), 30);
  });

  it('utc weekday cron fires on the matching UTC weekday', () => {
    const s = new CronScheduler(dir, 's');
    const from = Date.UTC(2026, 0, 1, 0, 0, 0);
    const next = s.computeNextRun('0 9 * * 1', from, true); // Monday 09:00 UTC
    assert.ok(next !== null);
    const d = new Date(next as number);
    assert.equal(d.getUTCDay(), 1, 'UTC Monday');
    assert.equal(d.getUTCHours(), 9);
  });
});

// ---------------------------------------------------------------------------
// UTC flag plumbed through create / persist / reload
// ---------------------------------------------------------------------------

describe('CronScheduler.create — utc flag', () => {
  it('stores the utc flag and computes nextRun in UTC', () => {
    const s = new CronScheduler(dir, 's');
    const job = s.create({ cron: '30 14 * * *', prompt: 'p', utc: true });
    assert.equal(job.utc, true);
    assert.ok(typeof job.nextRun === 'number');
    assert.equal(new Date(job.nextRun as number).getUTCHours(), 14);
    assert.equal(new Date(job.nextRun as number).getUTCMinutes(), 30);
  });

  it('omits the utc flag entirely for a local job', () => {
    const s = new CronScheduler(dir, 's');
    const job = s.create({ cron: '0 9 * * *', prompt: 'p' });
    assert.equal(job.utc, undefined);
  });

  it('persists and reloads the utc flag for a durable job', () => {
    const s1 = new CronScheduler(dir, 's');
    const job = s1.create({ cron: '30 14 * * *', prompt: 'p', durable: true, utc: true });
    // Fresh scheduler re-reads the persisted file.
    const s2 = new CronScheduler(dir, 's2');
    const reloaded = s2.get(job.id);
    assert.ok(reloaded, 'durable utc job reloaded');
    assert.equal(reloaded?.utc, true);
  });

  it('a corrupt non-boolean utc field makes a persisted entry invalid', () => {
    const file = path.join(dir, 'scheduled_tasks.json');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(file, JSON.stringify([
      { id: 'cron_1_x', cron: '0 9 * * *', prompt: 'p', recurring: true, durable: true, createdAt: Date.now(), utc: 'yes' },
    ]));
    const s = new CronScheduler(dir, 's');
    assert.equal(s.list().length, 0, 'non-boolean utc entry dropped on load');
  });
});

// ---------------------------------------------------------------------------
// UTC-aware display
// ---------------------------------------------------------------------------

describe('formatClockTime', () => {
  it('local mode renders AMI wall-clock style', () => {
    assert.equal(formatClockTime(9, 5, false), to12Hour(9, 5));
    assert.equal(formatClockTime(9, 5, false), '9:05 am');
  });

  it('utc mode renders a clock time with lowercase am/pm and a TZ label', () => {
    const out = formatClockTime(14, 30, true);
    assert.match(out, /\d:\d{2}/, 'has a clock time');
    assert.match(out, /\b(am|pm)\b/, 'lowercase meridiem');
    // The TZ label (timeZoneName: short) adds letters beyond the clock+meridiem.
    assert.ok(/[A-Za-z]/.test(out.replace(/\b(am|pm)\b/, '')), 'has a timezone label');
  });
});

describe('formatSchedule — utc option', () => {
  it('renders a utc daily schedule in local time with a TZ label', () => {
    const s = new CronScheduler(dir, 's');
    const out = s.formatSchedule('30 14 * * *', { utc: true });
    assert.match(out, /^every day at /);
    // Distinct from the local rendering (which has no TZ label).
    const local = s.formatSchedule('30 14 * * *');
    assert.notEqual(out, local);
  });

  it('renders a utc weekday schedule, resolving the local weekday name', () => {
    const s = new CronScheduler(dir, 's');
    const out = s.formatSchedule('0 9 * * 1', { utc: true });
    assert.match(out, /^every (Sun|Mon|Tue|Wed|Thu|Fri|Sat) at /, 'names a local weekday');
  });

  it('local rendering is unchanged (no opts / utc:false)', () => {
    const s = new CronScheduler(dir, 's');
    assert.equal(s.formatSchedule('0 9 * * *'), 'every day at 9:00 am');
    assert.equal(s.formatSchedule('0 9 * * *', { utc: false }), 'every day at 9:00 am');
  });
});

// ---------------------------------------------------------------------------
// assistant-mode bypass of the busy/idle gate
// ---------------------------------------------------------------------------

describe('CronScheduler — assistant-mode bypass', () => {
  function dueJob(s: CronScheduler): CronJob {
    const j = s.create({ cron: '* * * * *', prompt: 'p' });
    j.nextRun = Date.now() - 30 * 60_000; // well past any jitter window
    return j;
  }

  it('defers a due job while busy when assistant mode is OFF', () => {
    const s = new CronScheduler(dir, 's');
    const j = dueJob(s);
    s.setBusyCheck(() => true);
    const fired: string[] = [];
    tick(s, (job) => fired.push(job.id));
    assert.deepEqual(fired, [], 'deferred while busy');
    assert.equal(s.isAssistantMode(), false);
    void j;
  });

  it('fires a due job while busy when assistant mode is ON (setter)', () => {
    const s = new CronScheduler(dir, 's');
    const j = dueJob(s);
    s.setBusyCheck(() => true);
    s.setAssistantMode(true);
    assert.equal(s.isAssistantMode(), true);
    const fired: string[] = [];
    tick(s, (job) => fired.push(job.id));
    assert.deepEqual(fired, [j.id], 'assistant mode bypasses the busy gate');
  });

  it('fires a due job while busy when forced on by the env var', () => {
    const s = new CronScheduler(dir, 's');
    const j = dueJob(s);
    s.setBusyCheck(() => true);
    process.env.AMI_CRON_ASSISTANT_MODE = '1';
    assert.equal(s.isAssistantMode(), true);
    const fired: string[] = [];
    tick(s, (job) => fired.push(job.id));
    assert.deepEqual(fired, [j.id], 'env override bypasses the busy gate');
  });

  it('env var "true" also enables the bypass', () => {
    const s = new CronScheduler(dir, 's');
    process.env.AMI_CRON_ASSISTANT_MODE = 'true';
    assert.equal(s.isAssistantMode(), true);
  });
});

// ---------------------------------------------------------------------------
// End-to-end: UTC flag through the cron_create + cron_list tools
// ---------------------------------------------------------------------------

describe('cron tools — utc flag end-to-end', () => {
  beforeEach(() => resetCronScheduler());
  afterEach(() => resetCronScheduler());

  it('cron_create accepts utc and reports a UTC-defined schedule', async () => {
    const res = await cronCreateTool.execute(
      { cron: '30 14 * * *', prompt: 'nightly report', recurring: true, utc: true },
      ctx(dir),
    );
    assert.equal(res.isError, undefined);
    assert.match(res.output, /defined in UTC, shown in local time/);
  });

  it('cron_create defaults to local time (no UTC note)', async () => {
    const res = await cronCreateTool.execute(
      { cron: '0 9 * * *', prompt: 'morning', recurring: true },
      ctx(dir),
    );
    assert.match(res.output, /Schedule: .*\(local time\)/);
    assert.doesNotMatch(res.output, /defined in UTC/);
  });

  it('cron_list surfaces the UTC flag on a UTC job', async () => {
    await cronCreateTool.execute(
      { cron: '30 14 * * *', prompt: 'nightly', recurring: true, utc: true },
      ctx(dir),
    );
    const res = await cronListTool.execute({}, ctx(dir));
    assert.match(res.output, /UTC/, 'UTC flag shown in the list');
  });

  it('cron_list omits the UTC flag for a local job', async () => {
    await cronCreateTool.execute(
      { cron: '0 9 * * *', prompt: 'morning', recurring: true },
      ctx(dir),
    );
    const res = await cronListTool.execute({}, ctx(dir));
    assert.doesNotMatch(res.output, /\bUTC\b/);
  });
});
