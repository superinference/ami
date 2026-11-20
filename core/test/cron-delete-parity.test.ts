import { describe, it, beforeEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { cronDeleteTool } from '../src/tools/cron-delete';
import { getScheduler, resetScheduler } from '../src/cron';
import type { ToolContext } from '../src/types';

// Each test gets a fresh scheduler + temp project dir so job ids don't leak.
const tmpDirs: string[] = [];
function freshCtx(agentId?: string): ToolContext {
  resetScheduler();
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-delete-parity-'));
  tmpDirs.push(dir);
  // Bind the singleton scheduler to this dir.
  getScheduler(dir);
  return { cwd: dir, ...(agentId ? { agentId } : {}) } as ToolContext;
}

after(() => {
  resetScheduler();
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

function makeJob(cwd: string, prompt: string, agentId?: string) {
  return getScheduler(cwd).create({ cron: '0 9 * * *', prompt, recurring: false, durable: false, agentId });
}

describe('cron-delete parity — additive superset', () => {
  // ---- Regression: legacy single-id behavior must be preserved ----
  it('single id: deletes an owned job (legacy path)', async () => {
    const ctx = freshCtx();
    const j = makeJob(ctx.cwd, 'p');
    const r = await cronDeleteTool.execute!({ id: j.id }, ctx);
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Cancelled job/);
    assert.ok(r.output.includes(j.id));
    assert.deepEqual(r.metadata?.deletedIds, [j.id]);
  });

  it('single id: not found is an error with errorCode 1 (legacy path)', async () => {
    const ctx = freshCtx();
    const r = await cronDeleteTool.execute!({ id: 'missing-job' }, ctx);
    assert.equal(r.isError, true);
    assert.match(r.output, /no scheduled job with id "missing-job"/);
    assert.match(r.output, /not found/);
    assert.equal(r.metadata?.errorCode, 1);
  });

  // ---- FIX 1: bulk delete via ids:[...] ----
  it('FIX bulk: deletes several jobs in one call via ids', async () => {
    const ctx = freshCtx();
    const a = makeJob(ctx.cwd, 'a');
    const b = makeJob(ctx.cwd, 'b');
    const r = await cronDeleteTool.execute!({ ids: [a.id, b.id] }, ctx);
    assert.notEqual(r.isError, true);                 // RED on old: "unknown parameter(s): ids"
    assert.equal(r.metadata?.deleted, 2);
    assert.match(r.output, /Cancelled 2 job\(s\)/);
    assert.deepEqual((r.metadata?.deletedIds as string[]).sort(), [a.id, b.id].sort());
    assert.equal(getScheduler(ctx.cwd).list().length, 0);
  });

  // ---- FIX 2: all:true deletes every owned job ----
  it('FIX all: all:true deletes every job the agent owns', async () => {
    const ctx = freshCtx();
    makeJob(ctx.cwd, 'x');
    makeJob(ctx.cwd, 'y');
    makeJob(ctx.cwd, 'z');
    const r = await cronDeleteTool.execute!({ all: true }, ctx);
    assert.notEqual(r.isError, true);                 // RED on old: "unknown parameter(s): all"
    assert.equal(r.metadata?.deleted, 3);
    assert.equal(getScheduler(ctx.cwd).list().length, 0);
  });

  it('FIX all: all:true on an empty scheduler is an idempotent no-op (not an error)', async () => {
    const ctx = freshCtx();
    const r = await cronDeleteTool.execute!({ all: true }, ctx);
    assert.notEqual(r.isError, true);                 // RED on old: "unknown parameter(s): all"
    assert.equal(r.metadata?.deleted, 0);
    assert.match(r.output, /No scheduled jobs to delete/);
  });

  // ---- FIX 3: ignoreMissing idempotency (single) ----
  it('FIX idempotency: ignoreMissing makes a not-found single id a success no-op', async () => {
    const ctx = freshCtx();
    const r = await cronDeleteTool.execute!({ id: 'ghost', ignoreMissing: true }, ctx);
    assert.notEqual(r.isError, true);                 // RED on old: not-found error / unknown-param error
    assert.equal(r.metadata?.alreadyDeleted, true);
    assert.match(r.output, /already deleted/);
  });

  // ---- FIX 4: not-found surfacing in bulk (no ignoreMissing) ----
  it('FIX surfacing: bulk reports not-found ids and still deletes the found ones', async () => {
    const ctx = freshCtx();
    const a = makeJob(ctx.cwd, 'a');
    const r = await cronDeleteTool.execute!({ ids: [a.id, 'nope'] }, ctx);
    assert.equal(r.isError, true);                    // RED on old: "unknown parameter(s): ids"
    assert.equal(r.metadata?.errorCode, 1);
    assert.match(r.output, /Not found: nope/);
    assert.match(r.output, /Cancelled 1 job\(s\)/);   // the found one is still removed
    assert.equal(r.metadata?.deleted, 1);
    assert.deepEqual(r.metadata?.deletedIds, [a.id]);
  });

  it('FIX surfacing: bulk with ignoreMissing skips missing ids without error', async () => {
    const ctx = freshCtx();
    const a = makeJob(ctx.cwd, 'a');
    const r = await cronDeleteTool.execute!({ ids: [a.id, 'nope'], ignoreMissing: true }, ctx);
    assert.notEqual(r.isError, true);                 // RED on old: unknown-param error
    assert.equal(r.metadata?.deleted, 1);
    assert.match(r.output, /Skipped 1 already-deleted/);
  });

  // ---- FIX 5: bulk authorization aggregation (errorCode 2) ----
  it('FIX auth: bulk refuses a teammate job, aggregates, errorCode 2', async () => {
    const ctx = freshCtx('a1');
    const mine = makeJob(ctx.cwd, 'mine', 'a1');
    const theirs = makeJob(ctx.cwd, 'theirs', 'a2');
    const r = await cronDeleteTool.execute!({ ids: [mine.id, theirs.id] }, ctx);
    assert.equal(r.isError, true);                    // RED on old: "unknown parameter(s): ids"
    assert.equal(r.metadata?.errorCode, 2);           // auth outranks not-found
    assert.match(r.output, /Not authorized to delete:/);
    assert.ok(r.output.includes(theirs.id));
    // the owned job is still deleted despite the teammate failure
    assert.equal(r.metadata?.deleted, 1);
  });

  // ---- FIX 6: mutual exclusivity of selectors ----
  it('FIX exclusivity: id + ids together is rejected as mutually exclusive', async () => {
    const ctx = freshCtx();
    const a = makeJob(ctx.cwd, 'a');
    const r = await cronDeleteTool.execute!({ id: a.id, ids: [a.id] }, ctx);
    assert.equal(r.isError, true);
    assert.match(r.output, /mutually exclusive/);      // RED on old: "unknown parameter(s): ids"
    assert.equal(r.metadata?.errorCode, 1);
    // nothing should have been deleted on a rejected request
    assert.equal(getScheduler(ctx.cwd).list().length, 1);
  });

  // ---- FIX 7: no selector given shows the valid options ----
  it('FIX no-selector: empty input shows the id/ids/all options', async () => {
    const ctx = freshCtx();
    const r = await cronDeleteTool.execute!({}, ctx);
    assert.equal(r.isError, true);
    assert.match(r.output, /provide one of id, ids/);  // RED on old: "id is required and must match"
    assert.equal(r.metadata?.errorCode, 1);
  });

  // ---- FIX 8: duplicate-id guard in a bulk request ----
  it('FIX dedupe: duplicate ids in a bulk request are rejected', async () => {
    const ctx = freshCtx();
    const a = makeJob(ctx.cwd, 'a');
    const r = await cronDeleteTool.execute!({ ids: [a.id, a.id] }, ctx);
    assert.equal(r.isError, true);
    assert.match(r.output, /duplicate id/);            // RED on old: "unknown parameter(s): ids"
    // the job must NOT have been deleted by a rejected request
    assert.equal(getScheduler(ctx.cwd).list().length, 1);
  });

  // ---- additive guards: ids shape validation ----
  it('bulk: ids must be an array', async () => {
    const ctx = freshCtx();
    const r = await cronDeleteTool.execute!({ ids: 'not-an-array' } as Record<string, unknown>, ctx);
    assert.equal(r.isError, true);
    assert.match(r.output, /ids must be an array/);
  });

  it('bulk: empty ids array is rejected', async () => {
    const ctx = freshCtx();
    const r = await cronDeleteTool.execute!({ ids: [] }, ctx);
    assert.equal(r.isError, true);
    assert.match(r.output, /ids must not be empty/);
  });

  it('bulk: a malformed id in the list is rejected by format check', async () => {
    const ctx = freshCtx();
    const r = await cronDeleteTool.execute!({ ids: ['ok-id', 'bad id!'] }, ctx);
    assert.equal(r.isError, true);
    assert.equal(r.metadata?.errorCode, 1);
    assert.match(r.output, /must match/);
  });

  it('still rejects unknown parameters', async () => {
    const ctx = freshCtx();
    const r = await cronDeleteTool.execute!({ id: 'x', bogus: 1 } as Record<string, unknown>, ctx);
    assert.equal(r.isError, true);
    assert.match(r.output, /unknown parameter/);
  });

  // ---- PARITY: separate rich prompt vs terse description ----
  it('declares a long-form prompt alongside the description (durable file vs session store + cron_create)', () => {
    const p = cronDeleteTool.prompt;
    assert.equal(typeof p, 'string');
    // mentions where the job is removed from (parity: the reference buildCronDeletePrompt)
    assert.match(p!, /cron_create/);
    assert.match(p!, /scheduled_tasks\.json/);
    assert.match(p!, /session/i);
    // the prompt is distinct from the terse description, not a copy of it
    assert.notEqual(p, cronDeleteTool.description);
  });

  // ---- PARITY: declared structured output schema ----
  it('declares an object-rooted outputSchema carrying the deleted count/ids', () => {
    const os = cronDeleteTool.outputSchema;
    assert.ok(os, 'outputSchema must be declared');
    assert.equal(os!.type, 'object');            // object-root => advertised by provider
    assert.ok(os!.properties?.deleted, 'deleted field declared');
    assert.ok(os!.properties?.deletedIds, 'deletedIds field declared');
    assert.deepEqual(os!.required, ['deleted']);
  });

  it('structured result: single delete carries deletedIds; all:true empty carries []', async () => {
    const ctx = freshCtx();
    const j = makeJob(ctx.cwd, 'p');
    const r = await cronDeleteTool.execute!({ id: j.id }, ctx);
    assert.deepEqual(r.metadata?.deletedIds, [j.id]);

    const empty = await cronDeleteTool.execute!({ all: true }, ctx);
    assert.deepEqual(empty.metadata?.deletedIds, []);
  });

  it('ignoreMissing single no-op reports deleted:0 and empty deletedIds', async () => {
    const ctx = freshCtx();
    const r = await cronDeleteTool.execute!({ id: 'ghost', ignoreMissing: true }, ctx);
    assert.equal(r.metadata?.deleted, 0);
    assert.deepEqual(r.metadata?.deletedIds, []);
  });
});
