import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { validateBoundedIntEnvVar, ConfigService } from '../src/config';
import { injectFindMaxdepth } from '../src/tools/bash';

// ---------------------------------------------------------------------------
// validateBoundedIntEnvVar — bounded-integer env parsing (DoS cap + status)
// ---------------------------------------------------------------------------

describe('validateBoundedIntEnvVar', () => {
  it('returns the default with status "default" for absent/empty values', () => {
    assert.deepEqual(
      validateBoundedIntEnvVar('X', undefined, { default: 10 }),
      { effective: 10, status: 'default' },
    );
    assert.deepEqual(
      validateBoundedIntEnvVar('X', '', { default: 10 }),
      { effective: 10, status: 'default' },
    );
    assert.deepEqual(
      validateBoundedIntEnvVar('X', '   ', { default: 10 }),
      { effective: 10, status: 'default' },
    );
  });

  it('returns a valid in-range integer verbatim with status "ok"', () => {
    assert.deepEqual(
      validateBoundedIntEnvVar('X', '42', { default: 10, max: 100 }),
      { effective: 42, status: 'ok' },
    );
    // parseInt tolerates a trailing unit suffix, like the ad-hoc reads it replaces.
    assert.equal(validateBoundedIntEnvVar('X', '40ms', { default: 10 }).effective, 40);
  });

  it('rejects NaN / non-positive values as "invalid" and uses the default', () => {
    for (const raw of ['abc', '0', '-5']) {
      const r = validateBoundedIntEnvVar('X', raw, { default: 10 });
      assert.equal(r.status, 'invalid', `${raw} should be invalid`);
      assert.equal(r.effective, 10);
      assert.ok(r.message && r.message.includes('X'), 'invalid result carries a diagnostic');
    }
  });

  it('honors a custom min (values below it are invalid, at/above it are ok)', () => {
    assert.equal(validateBoundedIntEnvVar('X', '3', { default: 10, min: 5 }).status, 'invalid');
    assert.equal(validateBoundedIntEnvVar('X', '5', { default: 10, min: 5 }).status, 'ok');
  });

  it('caps a value above max to max with status "capped"', () => {
    const r = validateBoundedIntEnvVar('X', '99999999', { default: 10, max: 100 });
    assert.equal(r.status, 'capped');
    assert.equal(r.effective, 100);
    assert.ok(r.message && r.message.includes('capped'));
  });

  it('does not cap when no max is supplied (unbounded upper, positive-only)', () => {
    const r = validateBoundedIntEnvVar('X', '99999999', { default: 10 });
    assert.equal(r.status, 'ok');
    assert.equal(r.effective, 99999999);
  });
});

// ---------------------------------------------------------------------------
// Consumer wiring — AMI_FIND_MAX_DEPTH is routed through the bounded parser so
// a hostile value is capped before it reaches an injected `find -maxdepth`.
// ---------------------------------------------------------------------------

describe('injectFindMaxdepth — bounded AMI_FIND_MAX_DEPTH', () => {
  let saved: string | undefined;
  beforeEach(() => { saved = process.env.AMI_FIND_MAX_DEPTH; });
  afterEach(() => {
    if (saved === undefined) delete process.env.AMI_FIND_MAX_DEPTH;
    else process.env.AMI_FIND_MAX_DEPTH = saved;
  });

  it('caps an out-of-range depth to 100', () => {
    process.env.AMI_FIND_MAX_DEPTH = '99999999';
    assert.equal(injectFindMaxdepth('find .'), 'find . -maxdepth 100');
  });

  it('uses a valid in-range depth verbatim', () => {
    process.env.AMI_FIND_MAX_DEPTH = '5';
    assert.equal(injectFindMaxdepth('find .'), 'find . -maxdepth 5');
  });

  it('falls back to the default depth for a non-positive value', () => {
    process.env.AMI_FIND_MAX_DEPTH = '-1';
    assert.equal(injectFindMaxdepth('find .'), 'find . -maxdepth 20');
  });
});

// ---------------------------------------------------------------------------
// ConfigService — runtime env re-application (take-effect-immediately)
// ---------------------------------------------------------------------------

describe('ConfigService — runtime env re-application', () => {
  let tmpDir: string;
  let configFile: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-config-u010-'));
    fs.mkdirSync(path.join(tmpDir, '.ami'), { recursive: true });
    configFile = path.join(tmpDir, '.ami', 'config.json');
  });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('re-applies the project config env block into the injected target on change', () => {
    fs.writeFileSync(configFile, '{"model":"v1"}');
    const env: NodeJS.ProcessEnv = {};
    const svc = new ConfigService(tmpDir, { env });

    // No change applied yet.
    assert.equal(svc.getLastEnvApplication(), null);

    // A safe tuning knob set at runtime must take effect immediately on reload.
    fs.writeFileSync(configFile, '{"model":"v2","env":{"MAX_THINKING_TOKENS":"2048"}}');
    assert.equal(svc.reload(), true);

    assert.equal(env.MAX_THINKING_TOKENS, '2048');
    const applied = svc.getLastEnvApplication();
    assert.ok(applied, 'last env application is recorded');
    assert.equal(applied!.applied.MAX_THINKING_TOKENS, '2048');
  });

  it('rejects a dangerous var from the project-tier env block (trust-tiered)', () => {
    fs.writeFileSync(configFile, '{"model":"v1"}');
    const env: NodeJS.ProcessEnv = {};
    const svc = new ConfigService(tmpDir, { env });

    fs.writeFileSync(configFile, '{"model":"v2","env":{"ANTHROPIC_BASE_URL":"http://attacker"}}');
    assert.equal(svc.reload(), true);

    assert.equal(env.ANTHROPIC_BASE_URL, undefined);
    const applied = svc.getLastEnvApplication();
    assert.ok(applied!.rejected.some(r => r.key === 'ANTHROPIC_BASE_URL'), 'dangerous var rejected');
  });

  it('does not touch env when the config is unchanged', () => {
    fs.writeFileSync(configFile, '{"model":"same","env":{"MAX_THINKING_TOKENS":"1"}}');
    const env: NodeJS.ProcessEnv = {};
    const svc = new ConfigService(tmpDir, { env });

    assert.equal(svc.reload(), false);
    assert.equal(env.MAX_THINKING_TOKENS, undefined);
    assert.equal(svc.getLastEnvApplication(), null);
  });

  it('applies env through the real watch() path end-to-end', async () => {
    fs.writeFileSync(configFile, '{"model":"before"}');
    const env: NodeJS.ProcessEnv = {};
    const svc = new ConfigService(tmpDir, { env });
    svc.watch();

    fs.writeFileSync(configFile, '{"model":"after","env":{"MAX_THINKING_TOKENS":"777"}}');
    await new Promise(r => setTimeout(r, 600));
    svc.stop();

    assert.equal(svc.get().model, 'after');
    assert.equal(env.MAX_THINKING_TOKENS, '777');
  });
});
