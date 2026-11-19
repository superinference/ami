// UNIT U-perm2 (w128)
//  gap — Re-run the plan/auto-mode transition (transitionPlanAutoMode) when the
//    permission context is rebuilt from an on-disk settings change. Parity: Claude's
//    applySettingsChange running transitionPlanAutoMode(newContext) after
//    syncPermissionRulesFromDisk (utils/settings/applySettingsChange.ts:L68 +
//    utils/permissions/permissionSetup.ts transitionPlanAutoMode).
//
//  AMI's engine settings-change watcher already calls
//  PermissionManager.syncRulesFromDisk(cwd) (common/src/engine.ts). These tests drive
//  that exact real entrypoint: write/edit the on-disk settings, call syncRulesFromDisk,
//  and assert the plan/auto reconciliation (activate/deactivate, strip/restore dangerous
//  code-exec rules, screened autonomy during plan) took effect on the LIVE manager —
//  the full disk → load → reconcile → check path the engine uses.
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { PermissionManager } from '../src/permissions';

function tmp(): string {
  return fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-perm2-w128-')));
}
function writeProject(dir: string, data: Record<string, unknown>): void {
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'permissions.json'), JSON.stringify(data));
}
function writeLocal(dir: string, data: Record<string, unknown>): void {
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'permissions.local.json'), JSON.stringify(data));
}
// Build a manager in plan mode entered from a non-dangerous prior mode, so prePlanMode
// is 'ask' (not 'bypassPermissions') — transitionPlanAutoMode may activate.
function planManager(): PermissionManager {
  const pm = new PermissionManager('ask');
  pm.setMode('plan');
  return pm;
}

describe('PermissionManager plan/auto reconciliation on settings sync (U-perm2 w128)', () => {
  let dir: string;
  beforeEach(() => { dir = tmp(); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('activates auto-mode-during-plan when useAutoModeDuringPlan is toggled on mid-plan', async () => {
    const pm = planManager();
    // Baseline plan mode: a non-read-only tool is denied (read-only allowlist only).
    assert.equal(await pm.check('bash', { command: 'ls' }), 'deny');
    assert.equal(pm.isAutoModeActiveDuringPlan(), false);
    assert.equal(pm.getUseAutoModeDuringPlan(), false);

    // Operator enables useAutoModeDuringPlan on disk; the engine watcher re-syncs.
    writeProject(dir, { useAutoModeDuringPlan: true, rules: [] });
    pm.syncRulesFromDisk(dir);

    // Reconciliation activated auto mode: the setting and realized state both flip,
    // and a safe non-read-only call is now screened-and-allowed instead of denied.
    assert.equal(pm.getUseAutoModeDuringPlan(), true);
    assert.equal(pm.isAutoModeActiveDuringPlan(), true);
    assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
  });

  it('keeps read-only allowed and still blocks HIGH-risk calls while auto-during-plan is active', async () => {
    const pm = planManager();
    writeProject(dir, { useAutoModeDuringPlan: true, rules: [] });
    pm.syncRulesFromDisk(dir);
    assert.equal(pm.isAutoModeActiveDuringPlan(), true);
    // Read-only tools: always allowed in plan mode.
    assert.equal(await pm.check('file_read', { file_path: path.join(dir, 'x.txt') }), 'allow');
    // A recoverable mutation is screened through: allowed.
    assert.equal(await pm.check('file_edit', { file_path: path.join(dir, 'y.txt'), old_string: 'a', new_string: 'b' }), 'allow');
    // A destructive command is screened and blocked.
    assert.equal(await pm.check('bash', { command: 'rm -rf /' }), 'deny');
  });

  it('deactivates auto-mode-during-plan when the setting is toggled back off mid-plan', async () => {
    const pm = planManager();
    writeProject(dir, { useAutoModeDuringPlan: true, rules: [] });
    pm.syncRulesFromDisk(dir);
    assert.equal(pm.isAutoModeActiveDuringPlan(), true);
    assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');

    // Operator disables it again; re-sync reconciles back to the read-only allowlist.
    writeProject(dir, { useAutoModeDuringPlan: false, rules: [] });
    pm.syncRulesFromDisk(dir);
    assert.equal(pm.getUseAutoModeDuringPlan(), false);
    assert.equal(pm.isAutoModeActiveDuringPlan(), false);
    assert.equal(await pm.check('bash', { command: 'ls' }), 'deny');
  });

  it('is a no-op when NOT in plan mode (reconciliation only applies while planning)', async () => {
    const pm = new PermissionManager('ask'); // not plan
    writeProject(dir, { useAutoModeDuringPlan: true, rules: [] });
    pm.syncRulesFromDisk(dir);
    // The desire is read from disk, but auto-during-plan is never REALIZED outside plan.
    assert.equal(pm.getUseAutoModeDuringPlan(), true);
    assert.equal(pm.isAutoModeActiveDuringPlan(), false);
    assert.deepEqual(pm.transitionPlanAutoMode(), {});
  });

  it('never activates when plan mode was entered from bypassPermissions', () => {
    const pm = new PermissionManager('bypassPermissions');
    pm.setMode('plan'); // prePlanMode === 'bypassPermissions'
    assert.equal(pm.getPrePlanMode(), 'bypassPermissions');
    writeProject(dir, { useAutoModeDuringPlan: true, rules: [] });
    pm.syncRulesFromDisk(dir);
    assert.equal(pm.getUseAutoModeDuringPlan(), true);
    // Excluded: a dangerous entry mode must not silently gain the classifier.
    assert.equal(pm.isAutoModeActiveDuringPlan(), false);
    assert.deepEqual(pm.transitionPlanAutoMode(), {});
  });

  it('strips dangerous code-exec allow rules on activation and restores them on deactivation', () => {
    const pm = planManager();
    const dangerous = { tool: 'bash', pattern: 'python:*', action: 'allow' };
    // Activate with a dangerous prefix-allow rule present on disk.
    writeProject(dir, { useAutoModeDuringPlan: true, rules: [dangerous] });
    pm.syncRulesFromDisk(dir);
    assert.equal(pm.isAutoModeActiveDuringPlan(), true);
    // The dangerous rule was stripped so it cannot bypass the classifier screen.
    assert.ok(!pm.getRules().some(r => r.tool === 'bash' && r.pattern === 'python:*' && r.action === 'allow'),
      'dangerous python:* allow should be stripped while auto-during-plan is active');

    // Toggle off: the stripped rule is restored verbatim.
    writeProject(dir, { useAutoModeDuringPlan: false, rules: [dangerous] });
    pm.syncRulesFromDisk(dir);
    assert.equal(pm.isAutoModeActiveDuringPlan(), false);
    assert.ok(pm.getRules().some(r => r.tool === 'bash' && r.pattern === 'python:*' && r.action === 'allow'),
      'dangerous python:* allow should be restored after deactivation');
  });

  it('re-strips disk-reintroduced dangerous rules on each sync while auto stays active', () => {
    const pm = planManager();
    writeProject(dir, { useAutoModeDuringPlan: true, rules: [] });
    pm.syncRulesFromDisk(dir);
    assert.equal(pm.isAutoModeActiveDuringPlan(), true);
    // A dangerous rule is later added to disk; a re-sync re-adds then re-strips it
    // (the want && have "restripped" path).
    const dangerous = { tool: 'bash', pattern: 'node:*', action: 'allow' };
    writeProject(dir, { useAutoModeDuringPlan: true, rules: [dangerous] });
    assert.deepEqual(pm.syncRulesFromDisk(dir), []);
    assert.ok(!pm.getRules().some(r => r.tool === 'bash' && r.pattern === 'node:*' && r.action === 'allow'),
      'disk-reintroduced dangerous rule must be re-stripped while auto stays active');
    assert.deepEqual(pm.transitionPlanAutoMode(), { restripped: true });
  });

  it('deactivates and restores stripped rules when plan mode is left', async () => {
    const pm = planManager();
    const dangerous = { tool: 'bash', pattern: 'python:*', action: 'allow' };
    writeProject(dir, { useAutoModeDuringPlan: true, rules: [dangerous] });
    pm.syncRulesFromDisk(dir);
    assert.equal(pm.isAutoModeActiveDuringPlan(), true);
    assert.ok(!pm.getRules().some(r => r.pattern === 'python:*'));

    // Leaving plan mode tears down auto-during-plan and restores the stripped rule.
    pm.exitPlanMode(); // restores prior mode 'ask'
    assert.equal(pm.getMode(), 'ask');
    assert.equal(pm.isAutoModeActiveDuringPlan(), false);
    assert.ok(pm.getRules().some(r => r.tool === 'bash' && r.pattern === 'python:*' && r.action === 'allow'),
      'leaving plan mode must restore the auto-mode-stripped rule');
  });

  it('honors the highest-precedence tier for useAutoModeDuringPlan (local overrides project)', () => {
    const pm = planManager();
    // local=false (highest precedence) must win over project=true.
    writeLocal(dir, { useAutoModeDuringPlan: false, rules: [] });
    writeProject(dir, { useAutoModeDuringPlan: true, rules: [] });
    pm.syncRulesFromDisk(dir);
    assert.equal(pm.getUseAutoModeDuringPlan(), false);
    assert.equal(pm.isAutoModeActiveDuringPlan(), false);
  });

  it('transitionPlanAutoMode reports the structured transition outcome', () => {
    const pm = planManager();
    writeProject(dir, { useAutoModeDuringPlan: true, rules: [] });
    // First sync activates.
    pm.syncRulesFromDisk(dir);
    // A direct follow-up call with unchanged desire + active state re-strips.
    assert.deepEqual(pm.transitionPlanAutoMode(), { restripped: true });
    // Flip the in-memory desire off and reconcile → deactivated.
    writeProject(dir, { useAutoModeDuringPlan: false, rules: [] });
    pm.syncRulesFromDisk(dir); // loads desire=false and deactivates
    assert.equal(pm.isAutoModeActiveDuringPlan(), false);
    // Now both desire and state are off → no-op.
    assert.deepEqual(pm.transitionPlanAutoMode(), {});
  });
});
