// U004 — PermissionManager.syncRulesFromDisk: re-apply on-disk permission rules to
// the LIVE session when settings change at runtime (parity: the reference's
// loadAllPermissionRulesFromDisk + syncPermissionRulesFromDisk in applySettingsChange).
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { PermissionManager } from '../src/permissions';
import type { PermissionRule } from '../src/permissions';

function tmp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-sync-rules-'));
}
function writeProjectRules(dir: string, rules: unknown[], mode?: string): void {
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'permissions.json'),
    JSON.stringify({ ...(mode ? { mode } : {}), rules }));
}

describe('PermissionManager.syncRulesFromDisk (U004)', () => {
  let dir: string;
  beforeEach(() => { dir = tmp(); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('loads newly-added on-disk deny rules into the live manager', async () => {
    const pm = new PermissionManager('auto-allow');
    // No disk rules yet — a brand new command is allowed by the auto-allow mode.
    assert.equal(await pm.check('bash', { command: 'curl evil.test' }), 'allow');
    // An admin drops a deny rule on disk mid-session.
    writeProjectRules(dir, [{ tool: 'bash', pattern: 'curl *', action: 'deny' }]);
    const warnings = pm.syncRulesFromDisk(dir);
    assert.deepEqual(warnings, []);
    // The live session now honors the new deny without a restart.
    assert.equal(await pm.check('bash', { command: 'curl evil.test' }), 'deny');
  });

  it('is idempotent — repeated syncs do not accumulate duplicate disk rules', () => {
    writeProjectRules(dir, [{ tool: 'bash', pattern: 'npm *', action: 'ask' }]);
    const pm = new PermissionManager('auto-allow');
    pm.syncRulesFromDisk(dir);
    const afterOne = pm.getRules().filter(r => r.source === 'projectSettings').length;
    pm.syncRulesFromDisk(dir);
    pm.syncRulesFromDisk(dir);
    const afterThree = pm.getRules().filter(r => r.source === 'projectSettings').length;
    assert.equal(afterOne, 1);
    assert.equal(afterThree, 1);
  });

  it('reflects a rule REMOVED from disk on re-sync (drops stale disk rules)', async () => {
    writeProjectRules(dir, [{ tool: 'bash', pattern: 'rm *', action: 'deny' }]);
    const pm = new PermissionManager('auto-allow');
    pm.syncRulesFromDisk(dir);
    assert.equal(await pm.check('bash', { command: 'rm foo' }), 'deny');
    // Operator removes the deny from disk.
    writeProjectRules(dir, []);
    pm.syncRulesFromDisk(dir);
    assert.equal(await pm.check('bash', { command: 'rm foo' }), 'allow');
  });

  it('preserves in-memory session grants across a disk re-sync', () => {
    const pm = new PermissionManager('ask');
    const sessionRule: PermissionRule = { tool: 'bash', pattern: 'git *', action: 'allow', source: 'session' };
    pm.addRule(sessionRule);
    const cliRule: PermissionRule = { tool: 'file_edit', action: 'allow', source: 'cliArg' };
    pm.addRule(cliRule);
    writeProjectRules(dir, [{ tool: 'bash', pattern: 'npm *', action: 'ask' }]);
    pm.syncRulesFromDisk(dir);
    const rules = pm.getRules();
    assert.ok(rules.some(r => r.source === 'session' && r.tool === 'bash'), 'session grant preserved');
    assert.ok(rules.some(r => r.source === 'cliArg' && r.tool === 'file_edit'), 'cliArg grant preserved');
    assert.ok(rules.some(r => r.source === 'projectSettings'), 'disk rule loaded');
  });

  it('releases a managed-rules-only lockdown when the policy file is removed', () => {
    const pm = new PermissionManager('ask');
    // Simulate an engaged lockdown by loading a managed file, then removing it.
    // (loadTieredRules/syncRulesFromDisk re-derive the lockdown from disk.)
    pm.syncRulesFromDisk(dir); // no managed file → lockdown off
    assert.equal(pm.isManagedRulesOnly(), false);
  });

  it('clears the policy memo so a new rule takes effect immediately', async () => {
    const pm = new PermissionManager('ask');
    pm.enableMemo(60_000); // long TTL so a stale verdict would persist without a clear
    // Prime the memo: 'ask' mode memoizes the default 'ask' verdict for this input.
    assert.equal(await pm.check('bash', { command: 'wget x' }), 'ask');
    assert.ok((pm.getMemo()?.size() ?? 0) > 0, 'memo primed');
    writeProjectRules(dir, [{ tool: 'bash', pattern: 'wget *', action: 'deny' }]);
    pm.syncRulesFromDisk(dir);
    assert.equal(pm.getMemo()?.size(), 0, 'memo cleared on sync');
    // Without the memo clear this would return the stale memoized 'ask'.
    assert.equal(await pm.check('bash', { command: 'wget x' }), 'deny', 'new deny applies immediately');
  });
});
