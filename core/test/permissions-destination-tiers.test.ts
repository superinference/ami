import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  PermissionManager,
  permissionDestinationPath,
  PERMISSION_UPDATE_DESTINATIONS,
} from '../src/permissions';

test('PERMISSION_UPDATE_DESTINATIONS lists the five tiers', () => {
  assert.deepEqual([...PERMISSION_UPDATE_DESTINATIONS],
    ['userSettings', 'projectSettings', 'localSettings', 'session', 'cliArg']);
});

test('permissionDestinationPath maps each tier to its file (or null for in-memory)', () => {
  const cwd = '/repo';
  assert.equal(permissionDestinationPath(cwd, 'projectSettings'), path.join('/repo', '.ami', 'permissions.json'));
  assert.equal(permissionDestinationPath(cwd, 'localSettings'), path.join('/repo', '.ami', 'permissions.local.json'));
  assert.equal(permissionDestinationPath(cwd, 'userSettings'), path.join(os.homedir(), '.ami', 'permissions.json'));
  assert.equal(permissionDestinationPath(cwd, 'session'), null);
  assert.equal(permissionDestinationPath(cwd, 'cliArg'), null);
});

test('loadRules tags project rules with source=projectSettings', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-tier-'));
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'permissions.json'),
    JSON.stringify({ rules: [{ tool: 'bash', pattern: 'ls', action: 'allow' }] }));
  const pm = new PermissionManager();
  pm.loadRules(dir);
  assert.equal(pm.getRules()[0]?.source, 'projectSettings');
  fs.rmSync(dir, { recursive: true, force: true });
});

test('loadTieredRules merges tiers most-specific-first (local before project)', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-tier-'));
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'permissions.json'),
    JSON.stringify({ rules: [{ tool: 'bash', pattern: 'npm *', action: 'ask', source: 'projectSettings' }] }));
  fs.writeFileSync(path.join(dir, '.ami', 'permissions.local.json'),
    JSON.stringify({ rules: [{ tool: 'bash', pattern: 'npm *', action: 'allow' }] }));
  const pm = new PermissionManager('ask');
  pm.loadTieredRules(dir);
  const rules = pm.getRules();
  // local rule comes first (most specific wins at first-match)
  assert.equal(rules[0]?.source, 'localSettings');
  assert.equal(rules[0]?.action, 'allow');
  assert.ok(rules.some(r => r.source === 'projectSettings'));
  fs.rmSync(dir, { recursive: true, force: true });
});

test('saveRulesToDestination writes only that tier and is a no-op for in-memory tiers', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-tier-'));
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'ls', action: 'allow', source: 'localSettings' },
    { tool: 'web_fetch', action: 'deny', source: 'projectSettings' },
  ]);
  const r = pm.saveRulesToDestination(dir, 'localSettings');
  assert.equal(r.ok, true);
  const written = JSON.parse(fs.readFileSync(path.join(dir, '.ami', 'permissions.local.json'), 'utf-8'));
  assert.equal(written.rules.length, 1);
  assert.equal(written.rules[0].tool, 'bash');
  // session is in-memory → no file, ok:true
  assert.equal(pm.saveRulesToDestination(dir, 'session').ok, true);
  fs.rmSync(dir, { recursive: true, force: true });
});
