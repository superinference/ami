import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as nodePath from 'path';
import { CronScheduler } from '../src/cron';
import { webFetchTool } from '../src/tools/web-fetch';
import { resolveCommitAttribution, CO_AUTHOR_TRAILER } from '../src/tools/git-commit';
import type { ToolContext } from '../src/types';

/**
 * U228 — env-toggle consolidation onto the shared tolerant {@link parseEnvBool}.
 *
 * Gaps: "Lenient/robust env-var boolean parsing (isEnvTruthy / isEnvDefinedFalsy)"
 * and "Centralized, normalization-tolerant env-var truthy/falsy parsing". The
 * brittle `process.env.X === '1'` / `=== 'true'` string compares scattered across
 * cron.ts, web-fetch.ts and git-commit.ts silently ignored every other reasonable
 * spelling (`yes`, `on`, `TRUE`, ` 1 `, …). These were routed through the shared
 * `parseEnvBool` token sets so the full case/whitespace-insensitive vocabulary is
 * honoured — an ADDITIVE superset (the old `'1'`/`'true'` cases still work).
 *
 * RED discriminator: each case below uses a tolerant spelling (`yes`/`on`/`TRUE`/
 * `enable`/` 1 `) that the pre-consolidation exact-string checks did NOT match, so
 * reverting any refactor makes the matching assertion fail.
 */

// Tolerant spellings NONE of the old exact-string checks (`=== '1'`/`=== 'true'`)
// recognised — but parseEnvBool does.
const TOLERANT_TRUE = ['yes', 'on', 'TRUE', 'True', ' 1 ', 'enable', 'y'];

function snapshot(keys: string[]): Record<string, string | undefined> {
  const saved: Record<string, string | undefined> = {};
  for (const k of keys) saved[k] = process.env[k];
  return saved;
}
function restore(saved: Record<string, string | undefined>): void {
  for (const [k, v] of Object.entries(saved)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
}

describe('U228 cron kill-switch — tolerant env parsing', () => {
  const KEYS = [
    'AMI_DISABLE_CRON',
    'CLAUDE_CODE_DISABLE_CRON',
    'AMI_DISABLE_DURABLE_CRON',
    'CLAUDE_CODE_DISABLE_DURABLE_CRON',
    'AMI_CRON_ASSISTANT_MODE',
  ];
  let saved: Record<string, string | undefined>;
  let dir: string;
  beforeEach(() => {
    saved = snapshot(KEYS);
    for (const k of KEYS) delete process.env[k];
    dir = fs.mkdtempSync(nodePath.join(os.tmpdir(), 'u228-cron-'));
  });
  afterEach(() => {
    restore(saved);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('isDisabled honours every tolerant truthy spelling of AMI_DISABLE_CRON', () => {
    assert.equal(CronScheduler.isDisabled(), false, 'unset → enabled');
    for (const v of TOLERANT_TRUE) {
      process.env.AMI_DISABLE_CRON = v;
      assert.equal(CronScheduler.isDisabled(), true, `AMI_DISABLE_CRON=${JSON.stringify(v)} should disable`);
      delete process.env.AMI_DISABLE_CRON;
    }
  });

  it('isDisabled still honours the Claude-compat alias with a tolerant value', () => {
    process.env.CLAUDE_CODE_DISABLE_CRON = 'on';
    assert.equal(CronScheduler.isDisabled(), true);
  });

  it('isDisabled leaves an unrecognised value as not-disabled (falsy default)', () => {
    process.env.AMI_DISABLE_CRON = 'maybe';
    assert.equal(CronScheduler.isDisabled(), false);
  });

  it('isDurableEnabled is disabled by a tolerant truthy AMI_DISABLE_DURABLE_CRON', () => {
    assert.equal(CronScheduler.isDurableEnabled(), true, 'unset → durable on');
    process.env.AMI_DISABLE_DURABLE_CRON = 'yes';
    assert.equal(CronScheduler.isDurableEnabled(), false);
  });

  it('isAssistantMode honours a tolerant truthy AMI_CRON_ASSISTANT_MODE', () => {
    const s = new CronScheduler(dir, 's');
    assert.equal(s.isAssistantMode(), false, 'unset → off');
    process.env.AMI_CRON_ASSISTANT_MODE = 'ON';
    assert.equal(s.isAssistantMode(), true);
  });
});

describe('U228 web_fetch kill-switch — tolerant env parsing', () => {
  const KEYS = ['CLAUDE_CODE_DISABLE_WEB_FETCH', 'SUPERINFERENCE_DISABLE_WEB_FETCH'];
  let saved: Record<string, string | undefined>;
  beforeEach(() => {
    saved = snapshot(KEYS);
    for (const k of KEYS) delete process.env[k];
  });
  afterEach(() => restore(saved));

  const ctx = (): ToolContext => ({ cwd: process.cwd(), abortSignal: new AbortController().signal });

  it('disables web_fetch for a tolerant truthy CLAUDE_CODE_DISABLE_WEB_FETCH', async () => {
    process.env.CLAUDE_CODE_DISABLE_WEB_FETCH = 'yes';
    const res = await webFetchTool.execute({ url: 'https://example.com' }, ctx());
    assert.equal(res.isError, true);
    assert.match(res.output, /disabled/i);
  });

  it('disables web_fetch for a tolerant truthy SUPERINFERENCE_DISABLE_WEB_FETCH', async () => {
    process.env.SUPERINFERENCE_DISABLE_WEB_FETCH = 'TRUE';
    const res = await webFetchTool.execute({ url: 'https://example.com' }, ctx());
    assert.equal(res.isError, true);
    assert.match(res.output, /disabled/i);
  });
});

describe('U228 git-commit co-author opt-out — tolerant env parsing', () => {
  const KEY = 'SUPERINFERENCE_DISABLE_CO_AUTHOR';
  let saved: Record<string, string | undefined>;
  let dir: string;
  beforeEach(() => {
    saved = snapshot([KEY]);
    delete process.env[KEY];
    // An isolated, config-free cwd so resolveCommitAttribution falls through to
    // the env opt-out (no attribution.commit / attribution.auto in play).
    dir = fs.mkdtempSync(nodePath.join(os.tmpdir(), 'u228-gc-'));
  });
  afterEach(() => {
    restore(saved);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('suppresses the co-author trailer for a tolerant truthy opt-out', () => {
    // Baseline (unset) keeps the default trailer, proving the env toggle is what
    // flips the result (so the tolerant-parse branch is genuinely exercised).
    assert.equal(resolveCommitAttribution(dir), CO_AUTHOR_TRAILER);
    process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR = 'yes';
    assert.equal(resolveCommitAttribution(dir), null);
  });

  it('an unrecognised value does NOT suppress the trailer', () => {
    process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR = 'nope';
    assert.equal(resolveCommitAttribution(dir), CO_AUTHOR_TRAILER);
  });
});
