/**
 * U006 / w94 — Single observable choke point for permission-mode changes.
 *
 * Parity: the reference's `notifyPermissionModeChanged` (state/onChangeAppState.ts,
 * utils/sessionState.ts). EVERY mutation path (setMode, exitPlanMode fallback,
 * deserialize/loadRules, constructor) routes through `applyMode`, so a single
 * registered listener observes every real privilege transition — no path can
 * silently escalate into bypassPermissions/auto-allow. A no-op change and a
 * policy-blocked change do NOT notify.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { PermissionManager, type PermissionModeChange } from '../src/permissions';

describe('PermissionManager mode-change choke point (U006/w94)', () => {
  it('notifies the listener on a real setMode transition with full payload', () => {
    const pm = new PermissionManager('ask');
    const seen: PermissionModeChange[] = [];
    pm.setModeChangeListener((c) => seen.push(c));

    assert.equal(pm.setMode('bypassPermissions'), true);
    assert.equal(seen.length, 1);
    assert.deepEqual(seen[0], {
      previousMode: 'ask',
      mode: 'bypassPermissions',
      previousExternalMode: 'default',
      externalMode: 'bypassPermissions',
      reason: 'setMode',
    });
    assert.equal(pm.getMode(), 'bypassPermissions');
  });

  it('does NOT notify when setMode is a no-op (same mode)', () => {
    const pm = new PermissionManager('acceptEdits');
    let count = 0;
    pm.setModeChangeListener(() => { count++; });
    assert.equal(pm.setMode('acceptEdits'), true); // returns true but no transition
    assert.equal(count, 0);
  });

  it('does NOT notify when a bypass transition is blocked by managed policy', () => {
    const pm = new PermissionManager('ask');
    pm.setDisableBypass(true);
    let count = 0;
    pm.setModeChangeListener(() => { count++; });
    assert.equal(pm.setMode('bypassPermissions'), false);
    assert.equal(pm.setMode('auto-allow'), false);
    assert.equal(count, 0);
    assert.equal(pm.getMode(), 'ask');
  });

  it('notifies once for enter-plan then once for exitPlanMode restore', () => {
    const pm = new PermissionManager('acceptEdits');
    const seen: PermissionModeChange[] = [];
    pm.setModeChangeListener((c) => seen.push(c));
    assert.equal(pm.setMode('plan'), true);
    assert.equal(pm.exitPlanMode(), 'acceptEdits');
    assert.equal(seen.length, 2);
    assert.equal(seen[0].mode, 'plan');
    assert.equal(seen[1].previousMode, 'plan');
    assert.equal(seen[1].mode, 'acceptEdits');
    assert.equal(seen[1].reason, 'setMode'); // restore routes through setMode
  });

  it('notifies via the deserialize path (loadRules) with reason "deserialize"', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-mode-'));
    try {
      fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
      fs.writeFileSync(
        path.join(dir, '.ami', 'permissions.json'),
        JSON.stringify({ mode: 'acceptEdits', rules: [] }),
      );
      const pm = new PermissionManager('ask');
      const seen: PermissionModeChange[] = [];
      pm.setModeChangeListener((c) => seen.push(c));
      pm.loadRules(dir);
      assert.equal(seen.length, 1);
      assert.equal(seen[0].reason, 'deserialize');
      assert.equal(seen[0].previousMode, 'ask');
      assert.equal(seen[0].mode, 'acceptEdits');
      assert.equal(pm.getMode(), 'acceptEdits');
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('clears the listener with setModeChangeListener(undefined)', () => {
    const pm = new PermissionManager('ask');
    let count = 0;
    pm.setModeChangeListener(() => { count++; });
    pm.setMode('plan');
    pm.setModeChangeListener(undefined);
    pm.setMode('ask');
    assert.equal(count, 1);
  });

  it('a throwing listener never aborts the transition', () => {
    const pm = new PermissionManager('ask');
    pm.setModeChangeListener(() => { throw new Error('listener boom'); });
    assert.doesNotThrow(() => pm.setMode('acceptEdits'));
    assert.equal(pm.getMode(), 'acceptEdits');
  });

  it('a listener registered after construction is not retro-fired for the constructor seed', () => {
    const pm = new PermissionManager('bypassPermissions');
    let count = 0;
    pm.setModeChangeListener(() => { count++; });
    assert.equal(count, 0); // constructor routed through applyMode before any listener
    assert.equal(pm.getMode(), 'bypassPermissions');
  });
});
