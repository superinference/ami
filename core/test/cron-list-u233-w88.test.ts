import { describe, it, before, after, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { cronListTool } from '../src/tools/cron-list';
import { getScheduler, resetScheduler } from '../src/cron';
import { getTimeZone } from '../src/rate-limit-parser';
import type { ToolContext } from '../src/types';

/**
 * U233 / w88 — additive parity improvements to the cron_list tool
 * (core/core/src/tools/cron-list.ts):
 *   - grapheme-safe single-line prompt truncation (no lone surrogate on a cut),
 *   - declared maxResultSizeChars bound (honored by the tool-executor spill),
 *   - a rich tool `prompt` naming the durable persistence path,
 *   - the host timezone name on the Next/Last fire-time lines.
 */
describe('cron_list U233 parity', () => {
  let tmpDir: string;
  const ctx = (): ToolContext => ({ cwd: tmpDir } as ToolContext);

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-list-u233-'));
  });
  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
  beforeEach(() => resetScheduler());

  it('truncates a wide (emoji) prompt without emitting a lone surrogate', async () => {
    // 150 astral-plane graphemes → 300 UTF-16 code units. A code-unit slice at
    // 99 lands inside a surrogate pair; a grapheme-aware cut must not.
    getScheduler(tmpDir).create({ cron: '0 9 * * *', prompt: '\u{1F600}'.repeat(150) });
    const out = (await cronListTool.execute!({}, ctx())).output;

    // Strip valid surrogate pairs; any surrogate code unit left is a broken cut.
    const stripped = out.replace(/[\uD800-\uDBFF][\uDC00-\uDFFF]/g, '');
    assert.ok(
      !/[\uD800-\uDFFF]/.test(stripped),
      'truncated prompt must not contain a lone (unpaired) surrogate',
    );
    assert.ok(out.includes('\u2026'), 'truncation marker present');
  });

  it('still collapses a multi-line prompt to its first line with an ellipsis', async () => {
    getScheduler(tmpDir).create({ cron: '0 9 * * *', prompt: 'line one\nline two\nline three' });
    const out = (await cronListTool.execute!({}, ctx())).output;
    assert.match(out, /line one \u2026/);
    assert.doesNotMatch(out, /line two/);
  });

  it('declares a maxResultSizeChars bound (parity with Claude 100_000)', () => {
    assert.equal(cronListTool.maxResultSizeChars, 100_000);
  });

  it('exposes a tool prompt that names the durable persistence path', () => {
    assert.ok(cronListTool.prompt, 'cron_list declares a rich tool prompt');
    assert.match(cronListTool.prompt as string, /scheduled_tasks\.json/);
  });

  it('labels Next/Last fire times with the host timezone name', async () => {
    const tz = getTimeZone();
    assert.notEqual(tz, 'local', 'test host resolves a real IANA timezone');
    const sched = getScheduler(tmpDir);
    const job = sched.create({ cron: '*/5 * * * *', prompt: 'ping', recurring: true });
    job.lastRun = Date.now();
    const out = (await cronListTool.execute!({}, ctx())).output;
    // Fire-time lines carry the host timezone as "(local, <tz>)" (consolidated with
    // the U090 schedule labelling so Next/Last read identically); not a bare "(local)".
    assert.match(out, new RegExp(`Next: .+ \\(local, ${tz.replace(/[/.]/g, '\\$&')}\\)`));
    assert.match(out, new RegExp(`Last: .+ \\(local, ${tz.replace(/[/.]/g, '\\$&')}\\)`));
    assert.doesNotMatch(out, /\(local\)/);
  });
});
