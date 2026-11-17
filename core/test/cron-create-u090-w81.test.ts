import { describe, it, beforeEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { cronCreateTool } from '../src/tools/cron-create';
import { resetScheduler } from '../src/cron';
import type { ToolContext } from '../src/types';

// Fresh scheduler + temp project dir per test; durable defaults to false so
// nothing is persisted across cases.
let tmpDir: string;
const tmpDirs: string[] = [];
function ctxFor(extra: Partial<ToolContext> = {}): ToolContext {
  return { cwd: tmpDir, ...extra } as ToolContext;
}

beforeEach(() => {
  resetScheduler();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-create-u090-'));
  tmpDirs.push(tmpDir);
});

after(() => {
  resetScheduler();
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

// Gap: success output lacks affordance / next-step hints (one-shot lifecycle).
describe('cron_create U090 — one-shot fire-once affordance', () => {
  it('tells the model a one-shot job fires once then auto-deletes', async () => {
    const r = await cronCreateTool.execute!(
      { cron: '30 14 28 2 *', prompt: 'remind me', recurring: false },
      ctxFor(),
    );
    assert.notEqual(r.isError, true);
    // RED (revert the one-shot expiry branch back to ''): this note is absent.
    assert.match(r.output, /Fires once then auto-deletes\./);
    assert.match(r.output, /one-shot/);
    // Cancel affordance is still surfaced.
    assert.match(r.output, /Cancel with cron_delete\./);
  });

  it('does not show the fire-once note for a recurring job', async () => {
    const r = await cronCreateTool.execute!(
      { cron: '*/5 * * * *', prompt: 'recurring task', recurring: true },
      ctxFor(),
    );
    assert.notEqual(r.isError, true);
    assert.doesNotMatch(r.output, /Fires once then auto-deletes/);
    // Recurring jobs keep their auto-expiry affordance.
    assert.match(r.output, /Auto-expires in/);
  });
});

// Gap: validation error messages are generic and don't echo the offending value.
describe('cron_create U090 — validation errors echo the offending expression', () => {
  it('includes the bad cron string and the field legend in the error', async () => {
    const r = await cronCreateTool.execute!(
      { cron: '99 88 77 66 55', prompt: 'p' },
      ctxFor(),
    );
    assert.equal(r.isError, true);
    // RED (revert the echo change): the raw value is not quoted in the message.
    assert.match(r.output, /"99 88 77 66 55"/);
    assert.match(r.output, /minute hour day-of-month month day-of-week/);
  });

  it('echoes a garbage token so the model can self-correct', async () => {
    const r = await cronCreateTool.execute!(
      { cron: 'abc * * * *', prompt: 'p' },
      ctxFor(),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /"abc \* \* \* \*"/);
  });
});

// Gap: input-schema field descriptions are thinner (no one-shot example,
// no when-to-use guidance for recurring/durable).
describe('cron_create U090 — enriched inputSchema field descriptions', () => {
  const props = (cronCreateTool.inputSchema as {
    properties: Record<string, { description?: string }>;
  }).properties;

  it('cron description gives a one-shot worked example', () => {
    const d = props.cron.description ?? '';
    assert.match(d, /local time/i);
    // RED (revert the enriched cron description): the one-shot example is gone.
    assert.match(d, /Feb 28/);
    assert.match(d, /once/);
  });

  it('recurring description tells the model when to pick false', () => {
    const d = props.recurring.description ?? '';
    assert.match(d, /remind me at X/);
    assert.match(d, /one-shot/);
  });

  it('durable description tells the model when to pick true', () => {
    const d = props.durable.description ?? '';
    assert.match(d, /survive across sessions/);
  });
});
