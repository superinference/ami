import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'node:os';
import { sanitizeAnalyticsValue, sanitizeAnalyticsData } from '../src/analytics';

// INTEGRATION (gap U103): the analytics JSONL sink must not leak the local
// username / machine layout via a free-text string value (e.g. an inline error
// message that embedded an absolute path). redactString now runs redactPaths on
// top of the existing credential scrub. Revert that and the raw cwd/home path
// survives in the sanitized value → these go RED.
describe('analytics free-text path redaction (U103)', () => {
  it('normalizes the current working directory to [CWD] in a string value', () => {
    const raw = `failed to open ${process.cwd()}/src/config.ts`;
    const out = sanitizeAnalyticsValue(raw) as string;
    assert.ok(!out.includes(process.cwd()), 'cwd must not leak into analytics');
    assert.ok(out.includes('[CWD]'));
  });

  it('normalizes the home directory to [HOME] in a nested analytics payload', () => {
    const home = os.homedir();
    // Use a home-relative path that is NOT under cwd so it collapses to [HOME].
    const raw = `${home}/.ssh/known_hosts missing`;
    const out = sanitizeAnalyticsData({ error: raw }, { env: {} });
    const text = String(out.error);
    assert.ok(!text.includes(home), 'home dir must not leak into analytics');
    assert.ok(text.includes('[HOME]') || text.includes('[CWD]'));
  });

  it('still scrubs credentials alongside paths (no regression)', () => {
    const out = sanitizeAnalyticsValue('key sk-ant-api03-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdef') as string;
    assert.ok(!out.includes('ABCDEFGHIJKLMNOPQRSTUVWXYZ'));
  });
});
