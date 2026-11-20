/**
 * U033 (w96) — core/core/src/cron.ts + cron tools additive parity improvements.
 *
 * Gaps implemented (plannings/parity/wave/unit-35.jsonl):
 *  - G2: human-readable `name` field on a scheduled job (CronJob.name + create()
 *    validation + persistence + findByName lookup; surfaced in cron_create /
 *    cron_list / cron_get output and usable as a selector in cron_get / cron_delete).
 *    upstreamRef: src/skills/bundled/scheduleRemoteAgents.ts:L250 (required trigger name).
 *  - G3: run-now — the new cron_run tool wires the previously-unwired
 *    CronScheduler.runNow(id) so a job can be fired on demand.
 *    upstreamRef: src/skills/bundled/scheduleRemoteAgents.ts:L191 ({action:"run"}).
 *  - G4: user-visible recurring-job auto-expiry — startTicker's onExpire callback +
 *    the effective-expiry flag now shown for DEFAULT-window recurring jobs in
 *    cron_list. upstreamRef: src/tools/ScheduleCronTool/prompt.ts:116-118.
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  CronScheduler,
  resetScheduler,
  getScheduler,
  validateCronName,
  MAX_CRON_NAME_LEN,
  MAX_AGE_DAYS,
  type CronJob,
} from '../src/cron';
import { cronCreateTool } from '../src/tools/cron-create';
import { cronListTool } from '../src/tools/cron-list';
import { cronGetTool } from '../src/tools/cron-get';
import { cronDeleteTool } from '../src/tools/cron-delete';
import { cronRunTool } from '../src/tools/cron-run';
import type { ToolContext } from '../src/types';

let dir: string;
const ctx = (agentId?: string): ToolContext => ({ cwd: dir, agentId } as ToolContext);

beforeEach(() => {
  resetScheduler();
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-u033-w96-'));
});
afterEach(() => {
  resetScheduler();
  fs.rmSync(dir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// G2 — name validation helper
// ---------------------------------------------------------------------------

describe('validateCronName', () => {
  it('accepts undefined (optional field)', () => {
    assert.equal(validateCronName(undefined), null);
  });
  it('accepts a normal label', () => {
    assert.equal(validateCronName('nightly backup'), null);
  });
  it('rejects a blank / whitespace-only name', () => {
    assert.match(validateCronName('   ') as string, /non-empty/);
    assert.match(validateCronName('') as string, /non-empty/);
  });
  it('rejects an oversized name', () => {
    assert.match(validateCronName('x'.repeat(MAX_CRON_NAME_LEN + 1)) as string, /too long/);
  });
});

// ---------------------------------------------------------------------------
// G2 — scheduler stores + validates + looks up by name
// ---------------------------------------------------------------------------

describe('CronScheduler name field', () => {
  it('stores a trimmed name on create and exposes it on the job', () => {
    const s = new CronScheduler(dir);
    const j = s.create({ cron: '7 9 * * *', prompt: 'p', name: '  nightly  ' });
    assert.equal(j.name, 'nightly');
  });

  it('omits name when not provided', () => {
    const s = new CronScheduler(dir);
    const j = s.create({ cron: '7 9 * * *', prompt: 'p' });
    assert.equal(j.name, undefined);
  });

  it('throws on an invalid name at the engine boundary', () => {
    const s = new CronScheduler(dir);
    assert.throws(() => s.create({ cron: '7 9 * * *', prompt: 'p', name: '   ' }), /Invalid cron name/);
  });

  it('findByName returns every exact match, scoped by agent', () => {
    const s = new CronScheduler(dir);
    s.create({ cron: '7 9 * * *', prompt: 'a', name: 'dup' });
    s.create({ cron: '8 9 * * *', prompt: 'b', name: 'dup' });
    s.create({ cron: '9 9 * * *', prompt: 'c', name: 'solo', agentId: 'teamA' });
    assert.equal(s.findByName('dup').length, 2);
    assert.equal(s.findByName('missing').length, 0);
    // A different teammate cannot see teamA's named job.
    assert.equal(s.findByName('solo', 'teamB').length, 0);
    assert.equal(s.findByName('solo', 'teamA').length, 1);
  });

  it('persists a durable job name across reload and rejects a corrupt name', () => {
    const s = new CronScheduler(dir, 'sess');
    s.create({ cron: '7 9 * * *', prompt: 'p', name: 'persisted', durable: true });
    const persistPath = (s as unknown as { persistPath: string }).persistPath;
    // Fresh scheduler re-reads from disk.
    const s2 = new CronScheduler(dir, 'sess2');
    assert.equal(s2.findByName('persisted').length, 1);
    // Corrupt the stored name → the whole entry is dropped on reload.
    const raw = JSON.parse(fs.readFileSync(persistPath, 'utf-8'));
    raw[0].name = 42;
    fs.writeFileSync(persistPath, JSON.stringify(raw));
    const s3 = new CronScheduler(dir, 'sess3');
    assert.equal(s3.list().length, 0);
  });
});

// ---------------------------------------------------------------------------
// G2 — tools surface + accept the name
// ---------------------------------------------------------------------------

describe('cron tools — name field', () => {
  it('cron_create accepts, validates and echoes the name', async () => {
    const r = await cronCreateTool.execute!({ cron: '7 9 * * *', prompt: 'p', name: 'weekly report' }, ctx());
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Name: weekly report/);
  });

  it('cron_create rejects a blank name cleanly', async () => {
    const r = await cronCreateTool.execute!({ cron: '7 9 * * *', prompt: 'p', name: '   ' }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /non-empty/);
  });

  it('cron_list surfaces the name alongside the id', async () => {
    await cronCreateTool.execute!({ cron: '7 9 * * *', prompt: 'p', name: 'labelled' }, ctx());
    const r = await cronListTool.execute!({}, ctx());
    assert.match(r.output, /"labelled"/);
  });

  it('cron_get looks up by name and surfaces it', async () => {
    const c = await cronCreateTool.execute!({ cron: '7 9 * * *', prompt: 'the full prompt', name: 'byname' }, ctx());
    assert.equal(c.isError, undefined);
    const r = await cronGetTool.execute!({ name: 'byname' }, ctx());
    assert.equal(r.isError, undefined);
    assert.match(r.output, /"byname"/);
    assert.match(r.output, /the full prompt/);
    assert.equal(r.metadata?.name, 'byname');
  });

  it('cron_get rejects an ambiguous name', async () => {
    await cronCreateTool.execute!({ cron: '7 9 * * *', prompt: 'a', name: 'dup' }, ctx());
    await cronCreateTool.execute!({ cron: '8 9 * * *', prompt: 'b', name: 'dup' }, ctx());
    const r = await cronGetTool.execute!({ name: 'dup' }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /ambiguous/);
  });

  it('cron_get requires exactly one of id or name', async () => {
    const none = await cronGetTool.execute!({}, ctx());
    assert.equal(none.isError, true);
    assert.match(none.output, /exactly one/);
    const both = await cronGetTool.execute!({ id: 'cron_1_x', name: 'y' }, ctx());
    assert.equal(both.isError, true);
    assert.match(both.output, /exactly one/);
  });

  it('cron_delete cancels by name', async () => {
    await cronCreateTool.execute!({ cron: '7 9 * * *', prompt: 'p', name: 'killme' }, ctx());
    const r = await cronDeleteTool.execute!({ name: 'killme' }, ctx());
    assert.equal(r.isError, undefined);
    assert.equal((r.metadata as { deleted: number }).deleted, 1);
    assert.equal(getScheduler(dir).findByName('killme').length, 0);
  });

  it('cron_delete rejects an ambiguous name and honors ignoreMissing', async () => {
    await cronCreateTool.execute!({ cron: '7 9 * * *', prompt: 'a', name: 'dup' }, ctx());
    await cronCreateTool.execute!({ cron: '8 9 * * *', prompt: 'b', name: 'dup' }, ctx());
    const amb = await cronDeleteTool.execute!({ name: 'dup' }, ctx());
    assert.equal(amb.isError, true);
    assert.match(amb.output, /ambiguous/);
    // Unknown name: error normally, no-op with ignoreMissing.
    const miss = await cronDeleteTool.execute!({ name: 'ghost' }, ctx());
    assert.equal(miss.isError, true);
    const ok = await cronDeleteTool.execute!({ name: 'ghost', ignoreMissing: true }, ctx());
    assert.equal(ok.isError, undefined);
    assert.equal((ok.metadata as { alreadyDeleted?: boolean }).alreadyDeleted, true);
  });
});

// ---------------------------------------------------------------------------
// G3 — cron_run (run-now) tool wires CronScheduler.runNow
// ---------------------------------------------------------------------------

describe('cron_run tool', () => {
  it('is registered and gated like the other cron tools', () => {
    assert.equal(cronRunTool.name, 'cron_run');
    assert.equal(cronRunTool.isReadOnly, false);
    assert.equal(typeof cronRunTool.isEnabled, 'function');
  });

  it('fires a recurring job now, surfaces its fenced prompt, and advances the schedule', async () => {
    const c = await cronCreateTool.execute!({ cron: '7 9 * * *', prompt: 'do the thing', name: 'nightly' }, ctx());
    const id = /job (cron_\S+)/.exec(c.output)![1];
    assert.equal(getScheduler(dir).get(id)!.lastRun, undefined);
    const r = await cronRunTool.execute!({ id }, ctx());
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Running scheduled task now \(on-demand\)/);
    assert.match(r.output, /do the thing/);
    // Recurring job still present with its lifecycle advanced (lastRun stamped, and a
    // future nextRun still scheduled since it recurs).
    const after = getScheduler(dir).get(id)!;
    assert.ok(after.lastRun, 'lastRun stamped by runNow');
    assert.equal(typeof after.nextRun, 'number');
    assert.equal(r.metadata?.id, id);
  });

  it('fires a one-shot by name then removes it', async () => {
    await cronCreateTool.execute!({ cron: '30 14 25 12 *', prompt: 'xmas', name: 'oneshot', recurring: false }, ctx());
    const r = await cronRunTool.execute!({ name: 'oneshot' }, ctx());
    assert.equal(r.isError, undefined);
    assert.match(r.output, /xmas/);
    assert.equal(getScheduler(dir).findByName('oneshot').length, 0, 'one-shot removed after run-now');
  });

  it('reports a not-found id and a not-found name', async () => {
    const byId = await cronRunTool.execute!({ id: 'cron_9_zzz' }, ctx());
    assert.equal(byId.isError, true);
    assert.match(byId.output, /not found/);
    const byName = await cronRunTool.execute!({ name: 'nope' }, ctx());
    assert.equal(byName.isError, true);
    assert.match(byName.output, /not found/);
  });

  it('refuses to run another agent\'s job (authorization)', async () => {
    await cronCreateTool.execute!({ cron: '7 9 * * *', prompt: 'p' }, ctx('owner'));
    const id = getScheduler(dir).list('owner')[0].id;
    const r = await cronRunTool.execute!({ id }, ctx('intruder'));
    assert.equal(r.isError, true);
    assert.equal((r.metadata as { errorCode?: number }).errorCode, 2);
  });

  it('requires exactly one of id or name and rejects unknown params', async () => {
    const none = await cronRunTool.execute!({}, ctx());
    assert.equal(none.isError, true);
    assert.match(none.output, /exactly one/);
    const unk = await cronRunTool.execute!({ id: 'cron_1_x', bogus: 1 }, ctx());
    assert.equal(unk.isError, true);
    assert.match(unk.output, /unknown/);
  });
});

// ---------------------------------------------------------------------------
// G4 — user-visible recurring auto-expiry
// ---------------------------------------------------------------------------

describe('CronScheduler auto-expiry notice (G4)', () => {
  it('cron_list shows the DEFAULT expiry window for a recurring job', async () => {
    await cronCreateTool.execute!({ cron: '7 9 * * *', prompt: 'p', recurring: true }, ctx());
    const r = await cronListTool.execute!({}, ctx());
    assert.match(r.output, new RegExp(`expires in ${MAX_AGE_DAYS}d`));
  });

  it('startTicker onExpire fires once with the aged recurring job', () => {
    const s = new CronScheduler(dir);
    const j = s.create({ cron: '*/5 * * * *', prompt: 'old', name: 'ageing', recurring: true, maxAgeDays: 1 });
    // Backdate creation past the 1-day window.
    s.get(j.id)!.createdAt = Date.now() - 2 * 86400000;
    const expired: CronJob[] = [];
    s.startTicker(() => {}, (job) => expired.push(job));
    (s as unknown as { tick: (d: (x: CronJob) => void) => void }).tick(() => {});
    s.stopTicker();
    assert.equal(expired.length, 1);
    assert.equal(expired[0].id, j.id);
    assert.equal(expired[0].name, 'ageing');
    // And the job is gone.
    assert.equal(s.get(j.id), undefined);
  });

  it('does not fire onExpire for a permanent recurring job', () => {
    const s = new CronScheduler(dir);
    const j = s.create({ cron: '*/5 * * * *', prompt: 'keep', recurring: true, permanent: true });
    s.get(j.id)!.createdAt = Date.now() - 400 * 86400000;
    let expired = 0;
    s.startTicker(() => {}, () => { expired++; });
    (s as unknown as { tick: (d: (x: CronJob) => void) => void }).tick(() => {});
    s.stopTicker();
    assert.equal(expired, 0);
    assert.ok(s.get(j.id), 'permanent job survives');
  });
});
