import { describe, it, beforeEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  cronCreateTool,
  CRON_EXAMPLE,
  CRON_ANNOTATED_EXAMPLES,
} from '../src/tools/cron-create';
import { cronListTool } from '../src/tools/cron-list';
import {
  resetScheduler,
  cronRuntimeBehaviorNote,
  localTimeZoneLabel,
  DEFAULT_CRON_JITTER,
} from '../src/cron';
import type { ToolContext } from '../src/types';

let tmpDir: string;
const tmpDirs: string[] = [];
function ctxFor(extra: Partial<ToolContext> = {}): ToolContext {
  return { cwd: tmpDir, ...extra } as ToolContext;
}

beforeEach(() => {
  resetScheduler();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-u090-w88-'));
  tmpDirs.push(tmpDir);
});

after(() => {
  resetScheduler();
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

// Gap 1 (medium): Runtime-behavior/jitter explanation in the cron prompt.
describe('cron_create U090/w88 — runtime-behavior (jitter + idle) note', () => {
  it('cronRuntimeBehaviorNote derives its numbers from the jitter config', () => {
    const note = cronRuntimeBehaviorNote();
    // idle-gating affordance
    assert.match(note, /idle/i);
    assert.match(note, /never mid-query/i);
    // defaults: 10% late, max 15 min, one-shot up to 30s early
    assert.match(note, /up to 10% of their period late/);
    assert.match(note, /max 15 min/);
    assert.match(note, /up to 30s early/);
    assert.match(note, /:00 or :30/);
  });

  it('scales the numbers with a custom jitter config (no hardcoding/drift)', () => {
    const note = cronRuntimeBehaviorNote({
      ...DEFAULT_CRON_JITTER,
      recurringFrac: 0.25,
      recurringCapMs: 20 * 60 * 1000,
      oneShotMaxMs: 90 * 1000,
    });
    assert.match(note, /up to 25% of their period late/);
    assert.match(note, /max 20 min/);
    assert.match(note, /up to 90s early/);
  });

  it('surfaces the runtime-behavior note in the tool description', () => {
    // RED (revert the cronRuntimeBehaviorNote() insertion): the description never
    // mentions jitter or idle-only firing.
    assert.ok(cronCreateTool.description.includes(cronRuntimeBehaviorNote()));
  });
});

// Gap 2 (low): Annotated cron examples + self-correction affordance in the error.
describe('cron_create U090/w88 — annotated examples + self-correcting error', () => {
  it('ships at least five annotated examples with human-readable meanings', () => {
    assert.ok(CRON_ANNOTATED_EXAMPLES.length >= 5);
    for (const [expr, meaning] of CRON_ANNOTATED_EXAMPLES) {
      assert.equal(expr.trim().split(/\s+/).length, 5, `example "${expr}" is 5-field`);
      assert.ok(meaning.length > 0);
      // every annotated example must appear verbatim in the description
      assert.ok(cronCreateTool.description.includes(`"${expr}" ${meaning}`));
    }
  });

  it('echoes a valid, copy-pasteable example in the invalid-cron error', async () => {
    const r = await cronCreateTool.execute!(
      { cron: 'not a cron', prompt: 'x' },
      ctxFor(),
    );
    assert.equal(r.isError, true);
    // RED (revert the ` — e.g. "${CRON_EXAMPLE}" ...` suffix): no copyable example.
    assert.ok(r.output.includes(CRON_EXAMPLE), 'error echoes a valid example');
    assert.equal(CRON_EXAMPLE.trim().split(/\s+/).length, 5);
  });
});

// Gap 3 (low): Timezone labeling in next-run output + scheduling guidance.
describe('cron_create/cron_list U090/w88 — timezone labeling', () => {
  it('localTimeZoneLabel returns a non-empty label', () => {
    assert.ok(localTimeZoneLabel().length > 0);
  });

  it('labels the next-run instant with the host timezone on create', async () => {
    const tz = localTimeZoneLabel();
    const r = await cronCreateTool.execute!(
      { cron: '*/5 * * * *', prompt: 'ping' },
      ctxFor(),
    );
    assert.notEqual(r.isError, true);
    // RED (revert the `(${tz})` suffix on nextRun): the next-run line has no tz.
    assert.ok(r.output.includes(`(${tz})`), 'next-run line carries the tz label');
  });

  it('mentions the local timezone in the tool description', () => {
    assert.match(cronCreateTool.description, /timezone/i);
    assert.ok(cronCreateTool.description.includes(localTimeZoneLabel()));
  });

  it('labels the next-fire instant with the host timezone in cron_list', async () => {
    const tz = localTimeZoneLabel();
    await cronCreateTool.execute!({ cron: '*/5 * * * *', prompt: 'listme' }, ctxFor());
    const r = await cronListTool.execute!({}, ctxFor());
    assert.notEqual(r.isError, true);
    assert.match(r.output, /Next:/);
    assert.ok(r.output.includes(`(local, ${tz})`), 'cron_list next-fire carries the tz label');
  });
});
