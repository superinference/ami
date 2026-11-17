import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { cronDeleteTool } from '../src/tools/cron-delete';
import { getScheduler, resetScheduler } from '../src/cron';
import type { ToolContext } from '../src/types';

// U232 — cancel drains already-enqueued/in-flight work, not just future scheduling.
// A tick dispatches a due job's run (the engine registers a drain via setCancelDrain);
// deleting that job must also abort the pending run and report how many were aborted.
const tmpDirs: string[] = [];
function freshCtx(agentId?: string): ToolContext {
  resetScheduler();
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-delete-u232-'));
  tmpDirs.push(dir);
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

describe('cron_delete — in-flight run drain (U232 gap: cancellation drains enqueued work)', () => {
  it('single id: aborts the already-dispatched run via the registered drain and reports it', async () => {
    const ctx = freshCtx();
    const sched = getScheduler(ctx.cwd);
    const j = makeJob(ctx.cwd, 'p');
    const drained: string[] = [];
    // Simulate the engine: a tick already enqueued ONE run for this id.
    sched.setCancelDrain((id) => { drained.push(id); return 1; });

    const r = await cronDeleteTool.execute!({ id: j.id }, ctx);
    assert.equal(r.isError, undefined);
    assert.deepEqual(drained, [j.id], 'drain must be invoked for the deleted id');
    assert.equal(r.metadata?.inFlightCancelled, 1);
    assert.match(r.output, /Also aborted 1 already-dispatched run\(s\)\./);
  });

  it('no drain registered: inFlightCancelled is 0 and no abort suffix is added', async () => {
    const ctx = freshCtx();
    const j = makeJob(ctx.cwd, 'p');
    const r = await cronDeleteTool.execute!({ id: j.id }, ctx);
    assert.equal(r.metadata?.inFlightCancelled, 0);
    assert.doesNotMatch(r.output, /already-dispatched/);
    assert.equal(r.output, `Cancelled job ${j.id}.`);
  });

  it('bulk ids: aggregates the aborted-run count across deleted jobs', async () => {
    const ctx = freshCtx();
    const sched = getScheduler(ctx.cwd);
    const a = makeJob(ctx.cwd, 'a');
    const b = makeJob(ctx.cwd, 'b');
    // Each surviving delete drains one in-flight run.
    sched.setCancelDrain(() => 1);
    const r = await cronDeleteTool.execute!({ ids: [a.id, b.id] }, ctx);
    assert.notEqual(r.isError, true);
    assert.equal(r.metadata?.deleted, 2);
    assert.equal(r.metadata?.inFlightCancelled, 2);
    assert.match(r.output, /Also aborted 2 already-dispatched run\(s\)\./);
  });

  it('missing id is never drained (only an existing job can have a pending run)', async () => {
    const ctx = freshCtx();
    const sched = getScheduler(ctx.cwd);
    let calls = 0;
    sched.setCancelDrain(() => { calls++; return 5; });
    const r = await cronDeleteTool.execute!({ id: 'missing-job', ignoreMissing: true }, ctx);
    assert.equal(calls, 0, 'drain must not run for a job that did not exist');
    assert.equal(r.metadata?.inFlightCancelled, undefined); // no-op path carries alreadyDeleted, not a count
  });

  it('cancelPending clamps a bogus drain return (NaN / negative) to 0', () => {
    const ctx = freshCtx();
    const sched = getScheduler(ctx.cwd);
    sched.setCancelDrain(() => NaN);
    assert.equal(sched.cancelPending('x'), 0);
    sched.setCancelDrain(() => -3);
    assert.equal(sched.cancelPending('x'), 0);
    sched.setCancelDrain(null);
    assert.equal(sched.cancelPending('x'), 0, 'no drain registered → 0');
  });
});

// Guard the already-shipped parity gaps on this file so a regression reopens them.
describe('cron_delete — parity gaps already present (regression guards)', () => {
  it('exposes BOTH a terse description and a richer prompt mentioning durable vs session store', () => {
    assert.match(cronDeleteTool.description, /Cancel scheduled cron job/);
    assert.ok(cronDeleteTool.prompt, 'a distinct model-facing prompt must exist');
    assert.match(cronDeleteTool.prompt!, /scheduled_tasks\.json/);
    assert.match(cronDeleteTool.prompt!, /in-memory session store/);
    assert.match(cronDeleteTool.prompt!, /cron_create/);
  });

  it('declares an object-rooted outputSchema carrying the deleted ids/count', () => {
    assert.equal(cronDeleteTool.outputSchema?.type, 'object');
    assert.ok(cronDeleteTool.outputSchema?.properties?.deleted);
    assert.ok(cronDeleteTool.outputSchema?.properties?.deletedIds);
    assert.ok(cronDeleteTool.outputSchema?.properties?.inFlightCancelled, 'U232 adds inFlightCancelled to the schema');
  });

  it('uses the "Cancelled" verb and names the entity on not-found', async () => {
    const ctx = freshCtx();
    const j = makeJob(ctx.cwd, 'p');
    const ok = await cronDeleteTool.execute!({ id: j.id }, ctx);
    assert.match(ok.output, /^Cancelled job /);
    const nf = await cronDeleteTool.execute!({ id: 'nope' }, ctx);
    assert.match(nf.output, /no scheduled job with id "nope"/);
  });
});
