import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  salvageManagedConfig,
  suggestKnownConfigKey,
  loadManagedConfig,
  loadConfigFileChecked,
  collectConfigDiagnostics,
  getManagedConfigPath,
  ManagedConfigError,
  JSON_SYNTAX_RECOVERY_HINT,
  type ProjectConfig,
} from '../src/config';

// ---------------------------------------------------------------------------
// UNIT U010 — wave 66: config.ts parity gaps
//   A — managed/enterprise config per-field VALUE validation + SALVAGE: drop an
//       invalid field, keep the valid remainder (forwards-compatible degradation),
//       instead of trusting a bad value or rejecting the whole policy.
//   B — "did you mean X?" near-match suggestion for an unknown config-file key.
//   C — fail-open VISIBILITY: a broken managed policy surfaces in
//       collectConfigDiagnostics (/doctor) across every source, not just user/local.
//   D — managed JSON syntax error carries the actionable recovery hint + a
//       non-object managed payload fails closed.
// Each assertion discriminates the NEW behavior from the pre-fix behavior.
// ---------------------------------------------------------------------------

function mkTemp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-u010-w66-'));
}

// ===========================================================================
// Gap A — managed per-field value salvage
// ===========================================================================
describe('U010/w66 Gap A — managed config per-field value salvage', () => {
  const saved = process.env.SUPERINFERENCE_MANAGED_CONFIG;
  afterEach(() => {
    if (saved === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = saved;
  });

  it('salvageManagedConfig drops an invalid-enum field and keeps the valid rest', () => {
    const cfg = {
      model: 'm',
      permissionMode: 'yolo', // invalid enum
    } as unknown as ProjectConfig;
    const out = salvageManagedConfig(cfg, '/etc/x');
    // RED (pre-fix): the whole object was cast with no value validation, so the
    // bogus permissionMode flowed straight through.
    assert.equal(out.permissionMode, undefined, 'invalid enum field dropped');
    assert.equal(out.model, 'm', 'valid sibling field retained');
  });

  it('drops an invalid-typed field but preserves every other key', () => {
    const cfg = {
      maxTurns: 'lots', // must be a number
      allowedMcpServers: ['srv'],
    } as unknown as ProjectConfig;
    const out = salvageManagedConfig(cfg, '/etc/x');
    assert.equal(out.maxTurns, undefined);
    assert.deepEqual(out.allowedMcpServers, ['srv']);
  });

  it('drops ONLY the invalid nested superinference flag, not the whole object', () => {
    const cfg = {
      superinference: { enforcing: 'true', useLLMCritic: true },
    } as unknown as ProjectConfig;
    const out = salvageManagedConfig(cfg, '/etc/x');
    assert.deepEqual(out.superinference, { useLLMCritic: true });
  });

  it('preserves unknown/future keys untouched (forwards-compatible)', () => {
    const cfg = { model: 'm', brandNewPolicyKey: 42 } as unknown as ProjectConfig;
    const out = salvageManagedConfig(cfg, '/etc/x') as Record<string, unknown>;
    assert.equal(out.brandNewPolicyKey, 42);
    assert.equal(out.model, 'm');
  });

  it('returns the same object when nothing is invalid (no needless clone)', () => {
    const cfg: ProjectConfig = { model: 'm', maxTurns: 10 };
    assert.equal(salvageManagedConfig(cfg, '/etc/x'), cfg);
  });

  it('loadManagedConfig salvages a present file: bad field dropped, good field kept', () => {
    const dir = mkTemp();
    const p = path.join(dir, 'managed.json');
    fs.writeFileSync(p, JSON.stringify({ model: 'm', cacheTtl: 'forever' }), 'utf-8');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    const cfg = loadManagedConfig()!;
    // RED (pre-fix): loadManagedConfig cast the raw object — cacheTtl 'forever' was trusted.
    assert.equal(cfg.cacheTtl, undefined);
    assert.equal(cfg.model, 'm');
  });
});

// ===========================================================================
// Gap B — "did you mean" for unknown config keys
// ===========================================================================
describe('U010/w66 Gap B — unknown config-key suggestion', () => {
  it('suggests the nearest known key for a single-char typo', () => {
    assert.equal(suggestKnownConfigKey('modle'), 'model');
    assert.equal(suggestKnownConfigKey('provder'), 'provider');
  });

  it('matches case-insensitively', () => {
    assert.equal(suggestKnownConfigKey('StatusLine'), 'statusLine');
  });

  it('returns undefined for an exact known key (no self-suggestion)', () => {
    assert.equal(suggestKnownConfigKey('model'), undefined);
  });

  it('returns undefined for an unrelated key (nothing close enough)', () => {
    assert.equal(suggestKnownConfigKey('completelyUnrelatedThing'), undefined);
  });

  it('surfaces the "did you mean" hint in the loader unknown-key diagnostic', () => {
    const dir = mkTemp();
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p, JSON.stringify({ modle: 'gpt-4o' }), 'utf-8');
    const r = loadConfigFileChecked(p);
    assert.equal(r.diagnostic!.kind, 'unknown-key');
    // RED (pre-fix): the message listed "modle" with no suggestion.
    assert.match(r.diagnostic!.message, /modle \(did you mean "model"\?\)/);
  });
});

// ===========================================================================
// Gap C — fail-open visibility of a broken managed policy in /doctor
// ===========================================================================
describe('U010/w66 Gap C — managed policy visibility in collectConfigDiagnostics', () => {
  const saved = process.env.SUPERINFERENCE_MANAGED_CONFIG;
  afterEach(() => {
    if (saved === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = saved;
  });

  it('surfaces a world-writable (untrusted) managed policy as a diagnostic', () => {
    const cwd = mkTemp();
    const managed = path.join(mkTemp(), 'managed.json');
    fs.writeFileSync(managed, JSON.stringify({ model: 'evil' }), 'utf-8');
    fs.chmodSync(managed, 0o666);
    process.env.SUPERINFERENCE_MANAGED_CONFIG = managed;
    const diags = collectConfigDiagnostics(cwd);
    // RED (pre-fix): the managed tier was never inspected by collectConfigDiagnostics.
    const d = diags.find(x => x.path === managed);
    assert.ok(d, 'expected a diagnostic for the broken managed policy');
    assert.equal(d!.kind, 'unreadable');
    assert.match(d!.message, /world-writable/);
  });

  it('surfaces an unparseable managed policy as a diagnostic', () => {
    const cwd = mkTemp();
    const managed = path.join(mkTemp(), 'managed.json');
    fs.writeFileSync(managed, '{ not json', 'utf-8');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = managed;
    const diags = collectConfigDiagnostics(cwd);
    const d = diags.find(x => x.path === managed);
    assert.ok(d);
    assert.match(d!.message, /failed to parse/);
  });

  it('adds NO diagnostic when the managed policy is absent (clean path)', () => {
    const cwd = mkTemp();
    const missing = path.join(mkTemp(), 'does-not-exist.json');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = missing;
    const diags = collectConfigDiagnostics(cwd);
    // Only assert the managed tier is clean — global/project tiers may carry
    // unrelated user-config noise (unknown keys, etc.) that must not fail this check.
    assert.equal(diags.filter((d) => d.path === missing || d.source === 'managed').length, 0);
  });
});

// ===========================================================================
// Gap D — managed JSON syntax hint + non-object fail-closed
// ===========================================================================
describe('U010/w66 Gap D — managed parse-error affordances', () => {
  const saved = process.env.SUPERINFERENCE_MANAGED_CONFIG;
  afterEach(() => {
    if (saved === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = saved;
  });

  it('a malformed managed JSON error carries the shared recovery hint', () => {
    const dir = mkTemp();
    const p = path.join(dir, 'managed.json');
    fs.writeFileSync(p, '{ "model": ', 'utf-8');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    // RED (pre-fix): the ManagedConfigError carried only a raw parser message.
    assert.throws(() => loadManagedConfig(), (err: unknown) => {
      assert.ok(err instanceof ManagedConfigError);
      assert.ok((err as Error).message.includes(JSON_SYNTAX_RECOVERY_HINT));
      return true;
    });
  });

  it('a non-object managed payload (array) fails closed', () => {
    const dir = mkTemp();
    const p = path.join(dir, 'managed.json');
    fs.writeFileSync(p, JSON.stringify(['a', 'b']), 'utf-8');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    // RED (pre-fix): an array was blindly cast to ProjectConfig.
    assert.throws(() => loadManagedConfig(), (err: unknown) => {
      assert.ok(err instanceof ManagedConfigError);
      assert.match((err as Error).message, /not a JSON object \(got array\)/);
      return true;
    });
  });

  it('getManagedConfigPath honors the env override (sanity for the above)', () => {
    process.env.SUPERINFERENCE_MANAGED_CONFIG = '/tmp/x/managed.json';
    assert.equal(getManagedConfigPath(), '/tmp/x/managed.json');
  });
});
