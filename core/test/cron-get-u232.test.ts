import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { cronGetTool } from '../src/tools/cron-get';
import { getScheduler, resetScheduler } from '../src/cron';
import type { ToolContext } from '../src/types';

// U232 — cron_get: fetch a single scheduled job's FULL, untruncated detail by id
// (parity with the reference's RemoteTrigger `get` action). cron_list clips each prompt;
// cron_get returns the whole thing + a machine-readable outputSchema.
const tmpDirs: string[] = [];
function freshCtx(agentId?: string): ToolContext {
  resetScheduler();
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-get-u232-'));
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

const LONG_PROMPT = 'x'.repeat(240) + ' END-OF-PROMPT-SENTINEL';

function makeJob(cwd: string, prompt: string, agentId?: string) {
  return getScheduler(cwd).create({ cron: '0 9 * * *', prompt, recurring: true, durable: false, agentId });
}

describe('cron_get tool', () => {
  it('returns the FULL untruncated prompt for an existing job (unlike cron_list)', async () => {
    const ctx = freshCtx();
    const j = makeJob(ctx.cwd, LONG_PROMPT);
    const r = await cronGetTool.execute!({ id: j.id }, ctx);
    assert.equal(r.isError, undefined);
    assert.ok(r.output.includes('END-OF-PROMPT-SENTINEL'), 'prompt must not be truncated');
    assert.ok(!r.output.includes('…'), 'no ellipsis truncation marker');
    assert.equal(r.metadata?.id, j.id);
    assert.equal(r.metadata?.prompt, LONG_PROMPT);
    assert.equal(r.metadata?.cron, '0 9 * * *');
    assert.equal(r.metadata?.recurring, true);
    assert.equal(r.metadata?.durable, false);
    assert.equal(r.metadata?.active, true);
    assert.equal(typeof r.metadata?.nextRun, 'number');
  });

  it('errors with errorCode 1 when the id is unknown', async () => {
    const ctx = freshCtx();
    const r = await cronGetTool.execute!({ id: 'nope' }, ctx);
    assert.equal(r.isError, true);
    assert.match(r.output, /no scheduled job with id "nope"/);
    assert.equal(r.metadata?.errorCode, 1);
  });

  it('does not leak another teammate\'s job (scoped visibility → not found)', async () => {
    const ctx = freshCtx('agent-A');
    const j = makeJob(ctx.cwd, 'secret plan', 'agent-B');
    // A different teammate asking for agent-B's job sees "not found".
    const asOther = await cronGetTool.execute!({ id: j.id }, { ...ctx, agentId: 'agent-A' } as ToolContext);
    assert.equal(asOther.isError, true);
    assert.match(asOther.output, /not found/);
    // The owner can read it.
    const asOwner = await cronGetTool.execute!({ id: j.id }, { ...ctx, agentId: 'agent-B' } as ToolContext);
    assert.equal(asOwner.isError, undefined);
    assert.equal(asOwner.metadata?.prompt, 'secret plan');
  });

  it('an unscoped (team-lead) caller can read any job', async () => {
    const ctx = freshCtx();
    const j = makeJob(ctx.cwd, 'lead-visible', 'agent-B');
    const r = await cronGetTool.execute!({ id: j.id }, ctx); // no agentId
    assert.equal(r.isError, undefined);
    assert.equal(r.metadata?.prompt, 'lead-visible');
  });

  it('rejects unknown parameters and a malformed id', async () => {
    const ctx = freshCtx();
    const bad = await cronGetTool.execute!({ id: 'ok', extra: 1 } as Record<string, unknown>, ctx);
    assert.equal(bad.isError, true);
    assert.match(bad.output, /unknown parameter/);
    const malformed = await cronGetTool.execute!({ id: 'has space' }, ctx);
    assert.equal(malformed.isError, true);
    assert.match(malformed.output, /must match/);
  });

  it('declares its read-only/concurrency-safe contract and an outputSchema', () => {
    assert.equal(cronGetTool.isReadOnly, true);
    assert.equal(cronGetTool.isConcurrencySafe, true);
    assert.equal(cronGetTool.outputSchema?.type, 'object');
    assert.ok(cronGetTool.outputSchema?.properties?.prompt);
  });
});
