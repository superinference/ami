import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  getConfigValidationTip,
  validateProjectConfigValues,
  loadConfigFileChecked,
  type ProjectConfig,
} from '../src/config';

// ---------------------------------------------------------------------------
// UNIT U010 — core/core/src/config.ts (wave 107)
//   Gap 41 (ami-partial) — Actionable, SECURITY/semantics-aware validation feedback
//     for invalid config VALUES. AMI's getConfigValidationTip previously returned
//     only a GENERIC per-kind tip ("Valid values: …", "Must be a positive, finite
//     number.", etc.). Claude Code's getValidationTip is FIELD-SPECIFIC: it explains
//     what each permission mode does, what cleanupPeriodDays=0 means, how the default
//     shell routes `!` input, etc. This adds field-specific OVERRIDES on top of the
//     generic fallback (purely additive — the generic path is preserved for every
//     field without a specific override).
//   Gap 42 (config part) — Transcript-retention limit with VALIDATED BOUNDS.
//     cleanupPeriodDays was only base-type-checked (must be a number); a negative or
//     fractional value slipped through unvalidated. It is now range-validated
//     (non-negative integer; 0 disables cleanup) in validateProjectConfigValues, so an
//     out-of-range retention window surfaces as a loader invalid-value diagnostic.
// Each assertion discriminates the NEW behavior from the pre-change behavior.
// ---------------------------------------------------------------------------

function mkTemp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-w107-'));
}

describe('U010 w107 Gap 41 — field-specific validation tips', () => {
  const tmpDirs: string[] = [];
  afterEach(() => {
    while (tmpDirs.length) {
      try { fs.rmSync(tmpDirs.pop()!, { recursive: true, force: true }); } catch { /* best effort */ }
    }
  });

  it('permissionMode enum tip explains what each mode does (and keeps the "Valid values:" prefix)', () => {
    const tip = getConfigValidationTip('permissionMode', { kind: 'enum', allowed: ['ask', 'auto-allow', 'deny-all'] })!;
    // Regression lock: the generic enum formatting is still present (other callers rely on it).
    assert.match(tip, /Valid values: "ask", "auto-allow", "deny-all"\./);
    // NEW: field-specific semantics — mode-by-mode explanation.
    assert.match(tip, /auto-allow/);
    assert.match(tip, /deny-all/);
    assert.match(tip, /prompt|approv|block/i);
  });

  it('defaultShell enum tip explains input-box `!` routing', () => {
    const tip = getConfigValidationTip('defaultShell', { kind: 'enum', allowed: ['bash', 'powershell'] })!;
    assert.match(tip, /Valid values: "bash", "powershell"\./);
    // NEW override wording — distinctive from the bare enum list.
    assert.match(tip, /input|routes|!/i);
  });

  it('diffTool enum tip explains the renderer choice', () => {
    const tip = getConfigValidationTip('diffTool', { kind: 'enum', allowed: ['terminal', 'auto'] })!;
    assert.match(tip, /Valid values: "terminal", "auto"\./);
    assert.match(tip, /diff/i);
  });

  it('cleanupPeriodDays tip explains retention semantics and the 0-disables rule', () => {
    const tip = getConfigValidationTip('cleanupPeriodDays', { kind: 'range' })!;
    assert.match(tip, /day/i);
    assert.match(tip, /0/);
    assert.match(tip, /disabl/i);
    // It must NOT be the generic range tip.
    assert.notEqual(tip, 'Must be a positive, finite number.');
  });

  it('preserves the GENERIC per-kind tips for fields without a specific override (regression lock)', () => {
    // These exact strings are asserted by existing tests — field-specific overrides
    // must not leak into unrelated fields.
    assert.equal(getConfigValidationTip('maxBudgetUsd', { kind: 'range' }), 'Must be a positive, finite number.');
    assert.equal(getConfigValidationTip('safeMode', { kind: 'type', type: 'boolean' }), 'Use true or false without quotes.');
    assert.equal(getConfigValidationTip('maxTurns', { kind: 'type', type: 'number' }), 'Provide a number without quotes, e.g. 10.');
    assert.equal(getConfigValidationTip('model', { kind: 'type', type: 'string' }), 'Must be a quoted string, e.g. "value".');
    // env.* override still wins over any kind.
    assert.match(getConfigValidationTip('env.DEBUG', { kind: 'type', type: 'string' })!, /wrap numbers and booleans in quotes/);
  });
});

describe('U010 w107 Gap 42 — cleanupPeriodDays validated bounds', () => {
  const tmpDirs: string[] = [];
  afterEach(() => {
    while (tmpDirs.length) {
      try { fs.rmSync(tmpDirs.pop()!, { recursive: true, force: true }); } catch { /* best effort */ }
    }
  });

  it('flags a NEGATIVE retention window with a field-specific tip', () => {
    const issues = validateProjectConfigValues({ cleanupPeriodDays: -5 });
    const issue = issues.find(i => i.field === 'cleanupPeriodDays');
    assert.ok(issue, 'expected a cleanupPeriodDays issue for a negative value');
    assert.equal(issue!.value, -5);
    assert.match(issue!.message, /non-negative|days|0 disables/i);
    assert.match(issue!.tip!, /day/i);
  });

  it('flags a FRACTIONAL retention window (days must be whole)', () => {
    const issues = validateProjectConfigValues({ cleanupPeriodDays: 1.5 });
    assert.ok(issues.some(i => i.field === 'cleanupPeriodDays'), 'expected an issue for a fractional value');
  });

  it('ACCEPTS 0 (disables cleanup) and a positive integer', () => {
    assert.equal(validateProjectConfigValues({ cleanupPeriodDays: 0 }).some(i => i.field === 'cleanupPeriodDays'), false);
    assert.equal(validateProjectConfigValues({ cleanupPeriodDays: 30 }).some(i => i.field === 'cleanupPeriodDays'), false);
  });

  it('does not reach the range check when the base type is wrong (no double-report)', () => {
    const issues = validateProjectConfigValues({ cleanupPeriodDays: 'soon' as unknown as number });
    const cp = issues.filter(i => i.field === 'cleanupPeriodDays');
    assert.equal(cp.length, 1, 'a non-number cleanupPeriodDays reports exactly one (type) issue');
    assert.match(cp[0].message, /must be a number/i);
  });

  it('END-TO-END: a negative cleanupPeriodDays surfaces as a loader invalid-value diagnostic carrying the tip', () => {
    const dir = mkTemp();
    tmpDirs.push(dir);
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p, JSON.stringify({ cleanupPeriodDays: -1 }), 'utf-8');
    const r = loadConfigFileChecked(p);
    assert.equal(r.diagnostic!.kind, 'invalid-value');
    assert.match(r.diagnostic!.message, /cleanupPeriodDays/);
    // The actionable tip is threaded all the way into the surfaced diagnostic body.
    assert.match(r.diagnostic!.message, /day/i);
    // The valid-shaped parts of the config still load (validation is advisory).
    assert.equal((r.config as ProjectConfig).cleanupPeriodDays, -1);
  });
});
