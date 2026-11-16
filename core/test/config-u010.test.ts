import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  mergeConfigs,
  validateProjectConfigValues,
  getConfigValidationTip,
  isWorldWritable,
  loadConfigFileChecked,
  loadManagedConfig,
  ManagedConfigError,
  KNOWN_CONFIG_KEYS,
  type ProjectConfig,
} from '../src/config';

// ---------------------------------------------------------------------------
// UNIT U010 — core/core/src/config.ts parity gaps
//   Gap A — deniedMcpServers config-file knob (merge + managed-authoritative,
//           deny-wins accumulation) feeding the engine's existing denylist.
//   Gap B — actionable validation-tip surface (getValidationTip parity): each
//           invalid-value issue carries a `tip`, surfaced in the diagnostic.
//   Gap C — tamper-resistant managed-policy source: a symlinked or group/world-
//           writable managed config/drop-in is refused fail-closed.
// Each assertion discriminates the NEW behavior from the pre-fix behavior.
// ---------------------------------------------------------------------------

function mkTemp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-u010-'));
}

// ===========================================================================
// Gap A — deniedMcpServers
// ===========================================================================
describe('U010 Gap A — deniedMcpServers config knob', () => {
  it('is a recognized config key (KNOWN_CONFIG_KEYS, so no unknown-key warning)', () => {
    assert.ok(KNOWN_CONFIG_KEYS.has('deniedMcpServers'));
  });

  it('a non-array deniedMcpServers is flagged as string[] (value validation)', () => {
    // RED (pre-fix): deniedMcpServers was not a known field → silently accepted.
    const issues = validateProjectConfigValues({ deniedMcpServers: 'srv' as unknown as string[] });
    const issue = issues.find(i => i.field === 'deniedMcpServers');
    assert.ok(issue, 'expected a deniedMcpServers type issue');
    assert.match(issue!.message, /must be a string\[\]/);
  });

  it('accumulates across layers (deny-wins, dedup, first-seen order)', () => {
    const merged = mergeConfigs(
      {}, {},
      { deniedMcpServers: ['proj-srv', 'shared'] },   // project
      { deniedMcpServers: ['glob-srv', 'shared'] },   // global
      { deniedMcpServers: ['local-srv'] },            // local
    );
    // global applied first, then project, then local — all accumulate, dedup 'shared'.
    assert.deepEqual(merged.deniedMcpServers, ['glob-srv', 'shared', 'proj-srv', 'local-srv']);
  });

  it('managed denies are authoritative AND still accumulate lower-tier denies', () => {
    const merged = mergeConfigs(
      {}, {},
      { deniedMcpServers: ['proj-evil'] },             // project
      null,                                            // global
      null,                                            // local
      { deniedMcpServers: ['managed-blocked'] },       // managed
    );
    // managed first (can never be dropped), then the project's additional deny.
    assert.deepEqual(merged.deniedMcpServers, ['managed-blocked', 'proj-evil']);
  });

  it('absent deniedMcpServers stays undefined (default-open)', () => {
    const merged = mergeConfigs({}, {}, { model: 'm' }, null);
    assert.equal(merged.deniedMcpServers, undefined);
  });
});

// ===========================================================================
// Gap B — validation tips
// ===========================================================================
describe('U010 Gap B — actionable validation tips', () => {
  it('maps each failure kind to an actionable suggestion', () => {
    assert.equal(getConfigValidationTip('safeMode', { kind: 'type', type: 'boolean' }), 'Use true or false without quotes.');
    assert.equal(getConfigValidationTip('maxTurns', { kind: 'type', type: 'number' }), 'Provide a number without quotes, e.g. 10.');
    assert.equal(getConfigValidationTip('allowedMcpServers', { kind: 'type', type: 'string[]' }), 'Must be an array of strings, e.g. ["a", "b"].');
    assert.equal(getConfigValidationTip('superinference', { kind: 'type', type: 'object' }), 'Must be a JSON object, e.g. { "key": "value" }.');
    assert.equal(getConfigValidationTip('model', { kind: 'type', type: 'string' }), 'Must be a quoted string, e.g. "value".');
    assert.equal(getConfigValidationTip('maxBudgetUsd', { kind: 'range' }), 'Must be a positive, finite number.');
    assert.match(getConfigValidationTip('permissionMode', { kind: 'enum', allowed: ['ask', 'auto-allow'] })!, /Valid values: "ask", "auto-allow"\./);
  });

  it('env.* fields get the environment-variable tip regardless of kind', () => {
    assert.match(getConfigValidationTip('env.DEBUG', { kind: 'type', type: 'string' })!, /wrap numbers and booleans in quotes/);
    assert.match(getConfigValidationTip('env', { kind: 'type', type: 'object' })!, /Environment variables must be strings/);
  });

  it('attaches a tip to each invalid-value issue', () => {
    const issues = validateProjectConfigValues({
      safeMode: 'yes' as unknown as boolean,
      permissionMode: 'nope' as unknown as ProjectConfig['permissionMode'],
    });
    const sm = issues.find(i => i.field === 'safeMode');
    const pm = issues.find(i => i.field === 'permissionMode');
    assert.equal(sm!.tip, 'Use true or false without quotes.');
    assert.match(pm!.tip!, /Valid values:/);
  });

  it('surfaces the tip in the loader invalid-value diagnostic message', () => {
    const dir = mkTemp();
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p, JSON.stringify({ safeMode: 'yes' }), 'utf-8');
    const r = loadConfigFileChecked(p);
    assert.equal(r.diagnostic!.kind, 'invalid-value');
    // RED (pre-fix): the diagnostic message carried no tip.
    assert.match(r.diagnostic!.message, /Use true or false without quotes/);
  });
});

// ===========================================================================
// Gap C — tamper-resistant managed-policy source
// ===========================================================================
describe('U010 Gap C — managed-config tamper resistance', () => {
  const saved = process.env.SUPERINFERENCE_MANAGED_CONFIG;
  afterEach(() => {
    if (saved === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = saved;
  });

  it('isWorldWritable flags the world (other) write bit only, not group', () => {
    assert.equal(isWorldWritable(0o644), false);
    assert.equal(isWorldWritable(0o600), false);
    assert.equal(isWorldWritable(0o664), false); // group-writable is tolerated (umask 002)
    assert.equal(isWorldWritable(0o646), true);  // world-writable
    assert.equal(isWorldWritable(0o666), true);
    assert.equal(isWorldWritable(0o777), true);
  });

  it('accepts a normal (non-world-writable) managed file (regression: no false positive)', () => {
    const dir = mkTemp();
    const p = path.join(dir, 'managed.json');
    fs.writeFileSync(p, JSON.stringify({ model: 'm' }), 'utf-8');
    fs.chmodSync(p, 0o664); // group-writable (umask-002 default) must still be accepted
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    assert.equal(loadManagedConfig()!.model, 'm');
  });

  it('FAILS CLOSED on a symlinked managed file (redirect attack)', () => {
    const dir = mkTemp();
    const real = path.join(dir, 'real.json');
    const link = path.join(dir, 'managed.json');
    fs.writeFileSync(real, JSON.stringify({ model: 'evil' }), 'utf-8');
    fs.symlinkSync(real, link);
    process.env.SUPERINFERENCE_MANAGED_CONFIG = link;
    // RED (pre-fix): the symlink was followed and 'evil' loaded silently.
    assert.throws(() => loadManagedConfig(), ManagedConfigError);
  });

  it('FAILS CLOSED on a world-writable managed file', () => {
    const dir = mkTemp();
    const p = path.join(dir, 'managed.json');
    fs.writeFileSync(p, JSON.stringify({ model: 'm' }), 'utf-8');
    fs.chmodSync(p, 0o666);
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    // RED (pre-fix): a world-writable policy file was trusted.
    assert.throws(() => loadManagedConfig(), ManagedConfigError);
  });

  it('FAILS CLOSED on a world-writable drop-in file', () => {
    const dir = mkTemp();
    const base = path.join(dir, 'config.json');
    fs.writeFileSync(base, JSON.stringify({ model: 'base' }), 'utf-8');
    fs.chmodSync(base, 0o664);
    const dropInDir = path.join(dir, 'config.d');
    fs.mkdirSync(dropInDir);
    const drop = path.join(dropInDir, '10-evil.json');
    fs.writeFileSync(drop, JSON.stringify({ model: 'evil' }), 'utf-8');
    fs.chmodSync(drop, 0o662);
    process.env.SUPERINFERENCE_MANAGED_CONFIG = base;
    assert.throws(() => loadManagedConfig(), ManagedConfigError);
  });
});
