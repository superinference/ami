import { describe, it, before, after, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  CronScheduler,
  DEFAULT_CRON_JITTER,
  MAX_AGE_DAYS,
  validateJitterConfig,
  resetScheduler,
  getScheduler,
  type CronJitterConfig,
} from '../src/cron';
import { cronCreateTool } from '../src/tools/cron-create';
import { cronListTool } from '../src/tools/cron-list';
import type { ToolContext } from '../src/types';

// U033 / w67 — genuinely-missing cron parity sub-capabilities:
//  1. One-shot jitter FLOOR knob (CronJitterConfig.oneShotFloorMs) + cross-field
//     refine (validateJitterConfig), mirroring the reference's cronJitterConfig refine.
//  2. Per-job expiry window (maxAgeDays, 0=never) surfaced through the cron_create
//     tool schema/output and the cron_list flags, with MAX_AGE_DAYS interpolated
//     from the single source constant.

describe('CronJitterConfig — oneShotFloorMs + validateJitterConfig (refine)', () => {
  it('DEFAULT_CRON_JITTER carries a (0) one-shot floor and passes validation', () => {
    assert.equal(DEFAULT_CRON_JITTER.oneShotFloorMs, 0);
    assert.doesNotThrow(() => validateJitterConfig(DEFAULT_CRON_JITTER));
  });

  it('rejects a floor greater than the cap (inverted one-shot window)', () => {
    const bad: CronJitterConfig = { ...DEFAULT_CRON_JITTER, oneShotFloorMs: 40_000, oneShotMaxMs: 30_000 };
    assert.throws(() => validateJitterConfig(bad), /oneShotFloorMs.*must not exceed/);
  });

  it('rejects negative / NaN knobs and a sub-1 minute-mod', () => {
    assert.throws(() => validateJitterConfig({ ...DEFAULT_CRON_JITTER, recurringFrac: -1 }), /recurringFrac/);
    assert.throws(() => validateJitterConfig({ ...DEFAULT_CRON_JITTER, oneShotMaxMs: NaN }), /oneShotMaxMs/);
    assert.throws(() => validateJitterConfig({ ...DEFAULT_CRON_JITTER, oneShotMinuteMod: 0 }), /oneShotMinuteMod/);
  });

  it('a floor == cap gives an exactly-floor early fire (deterministic)', () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-floor-'));
    try {
      // floor == cap collapses the hash span to 0, so EVERY non-pinned one-shot
      // fires exactly `floor` ms early regardless of its id hash.
      const cfg: CronJitterConfig = { ...DEFAULT_CRON_JITTER, oneShotFloorMs: 120_000, oneShotMaxMs: 120_000, oneShotMinuteMod: 97 };
      const s = new CronScheduler(tmp, 'floor-eq-cap', cfg);
      const job = s.create({ cron: '* * * * *', prompt: 'p', recurring: false });
      // A fire ~9s in the future, nudged off a pinned minute (mod 97 pins only :00).
      let nextRun = Date.now() + 9_000;
      while (new Date(nextRun).getMinutes() % cfg.oneShotMinuteMod === 0) nextRun += 60_000;
      job.nextRun = nextRun;
      assert.ok(s.getDueJobs().some(j => j.id === job.id), 'a floor wider than the lead time makes the one-shot due early');
      s.stopTicker();
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('with no floor (floor=max=0) the same future one-shot is NOT yet due', () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-nofloor-'));
    try {
      const cfg: CronJitterConfig = { ...DEFAULT_CRON_JITTER, oneShotFloorMs: 0, oneShotMaxMs: 0, oneShotMinuteMod: 97 };
      const s = new CronScheduler(tmp, 'no-floor', cfg);
      const job = s.create({ cron: '* * * * *', prompt: 'p', recurring: false });
      let nextRun = Date.now() + 9_000;
      while (new Date(nextRun).getMinutes() % cfg.oneShotMinuteMod === 0) nextRun += 60_000;
      job.nextRun = nextRun;
      assert.ok(!s.getDueJobs().some(j => j.id === job.id), 'without a floor the one-shot waits for its nominal time');
      s.stopTicker();
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('constructor rejects an invalid jitter config (fail fast)', () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-ctor-'));
    try {
      assert.throws(
        () => new CronScheduler(tmp, 'bad', { ...DEFAULT_CRON_JITTER, oneShotFloorMs: 99, oneShotMaxMs: 1 }),
        /oneShotFloorMs/,
      );
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('AMI_CRON_ONESHOT_FLOOR_MS env knob is applied live and clamped to the cap', () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-env-floor-'));
    const prev = process.env.AMI_CRON_ONESHOT_FLOOR_MS;
    // Synchronous tick seam (same pattern as the live-retuning suite) so
    // resolveRuntimeJitter() re-reads the env deterministically, not on a timer.
    const tick = (s: CronScheduler) =>
      (s as unknown as { tick: (cb: () => void) => void }).tick(() => {});
    try {
      const s = new CronScheduler(tmp, 'env-floor', { ...DEFAULT_CRON_JITTER, oneShotMaxMs: 30_000 });
      // Valid in-range floor is applied.
      process.env.AMI_CRON_ONESHOT_FLOOR_MS = '5000';
      tick(s);
      assert.equal(s.getJitterConfig().oneShotFloorMs, 5_000, 'in-range env floor applied');
      // A floor above the cap is clamped to the cap, never inverting the window.
      process.env.AMI_CRON_ONESHOT_FLOOR_MS = '999999';
      tick(s);
      assert.equal(s.getJitterConfig().oneShotFloorMs, 30_000, 'env floor clamped to cap');
      // Cleared → reverts to the construction baseline (0).
      delete process.env.AMI_CRON_ONESHOT_FLOOR_MS;
      tick(s);
      assert.equal(s.getJitterConfig().oneShotFloorMs, 0, 'reverts to baseline when cleared');
      s.stopTicker();
    } finally {
      if (prev === undefined) delete process.env.AMI_CRON_ONESHOT_FLOOR_MS;
      else process.env.AMI_CRON_ONESHOT_FLOOR_MS = prev;
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});

describe('cron_create — maxAgeDays surfaced in schema + output', () => {
  let dir: string;
  const ctx = () => ({ cwd: dir } as ToolContext);

  before(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-u033w67-create-')); });
  after(() => { fs.rmSync(dir, { recursive: true, force: true }); });
  beforeEach(() => resetScheduler());

  it('advertises maxAgeDays in the input schema', () => {
    assert.ok(cronCreateTool.inputSchema.properties?.maxAgeDays, 'maxAgeDays property present');
  });

  it('interpolates MAX_AGE_DAYS (not a stale literal) into the description', () => {
    assert.ok(cronCreateTool.description.includes(`${MAX_AGE_DAYS} days`), 'description cites the constant');
  });

  it('accepts a custom maxAgeDays and reflects the window in the output', async () => {
    const r = await cronCreateTool.execute!({ cron: '7 9 * * *', prompt: 'p', maxAgeDays: 3 }, ctx());
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Auto-expires in 3 days/);
    const job = getScheduler(dir).list()[0];
    assert.equal(job.maxAgeDays, 3);
  });

  it('maxAgeDays:0 reports a never-expiring job', async () => {
    const r = await cronCreateTool.execute!({ cron: '7 9 * * *', prompt: 'p', maxAgeDays: 0 }, ctx());
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Never auto-expires/);
  });

  it('default recurring job cites the MAX_AGE_DAYS default window', async () => {
    const r = await cronCreateTool.execute!({ cron: '7 9 * * *', prompt: 'p' }, ctx());
    assert.match(r.output, new RegExp(`Auto-expires in ${MAX_AGE_DAYS} days`));
  });

  it('rejects a fractional / negative maxAgeDays with a clean error', async () => {
    const bad = await cronCreateTool.execute!({ cron: '7 9 * * *', prompt: 'p', maxAgeDays: -1 }, ctx());
    assert.equal(bad.isError, true);
    assert.match(bad.output, /maxAgeDays/);
    const frac = await cronCreateTool.execute!({ cron: '7 9 * * *', prompt: 'p', maxAgeDays: 1.5 }, ctx());
    assert.equal(frac.isError, true);
  });
});

describe('cron_list — surfaces a custom expiry window flag', () => {
  let dir: string;
  const ctx = () => ({ cwd: dir } as ToolContext);

  before(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-u033w67-list-')); });
  after(() => { fs.rmSync(dir, { recursive: true, force: true }); });
  beforeEach(() => resetScheduler());

  it('shows "expires in Nd" for a recurring job with a custom window', async () => {
    getScheduler(dir).create({ cron: '7 9 * * *', prompt: 'alpha', recurring: true, maxAgeDays: 2 });
    const r = await cronListTool.execute!({}, ctx());
    assert.match(r.output, /expires in 2d/);
  });

  it('shows "never-expires" for a recurring maxAgeDays:0 job', async () => {
    getScheduler(dir).create({ cron: '7 9 * * *', prompt: 'beta', recurring: true, maxAgeDays: 0 });
    const r = await cronListTool.execute!({}, ctx());
    assert.match(r.output, /never-expires/);
  });
});
