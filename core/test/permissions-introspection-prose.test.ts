import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  getRuleBehaviorDescription,
  getAutoAllowedCommands,
  getDefaultPermissionPatterns,
  createPermissionRequestMessage,
  isAcceptEditsFilesystemMutation,
  PERMISSION_MODES,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 — getRuleBehaviorDescription (behavior enum → grammatical past-tense prose)
// Parity: Claude's utils/permissions/PermissionResult.ts getRuleBehaviorDescription.
// ---------------------------------------------------------------------------
describe('getRuleBehaviorDescription', () => {
  it('maps allow → "allowed"', () => {
    assert.equal(getRuleBehaviorDescription('allow'), 'allowed');
  });
  it('maps deny → "denied"', () => {
    assert.equal(getRuleBehaviorDescription('deny'), 'denied');
  });
  it('maps ask → "asked for confirmation for" (the default prose)', () => {
    assert.equal(getRuleBehaviorDescription('ask'), 'asked for confirmation for');
  });
});

// ---------------------------------------------------------------------------
// U003 — hook decisionReason narrates the hook's actual behavior via the prose
// helper, falling back to the generic phrasing when no behavior is attached.
// ---------------------------------------------------------------------------
describe('createPermissionRequestMessage — hook behavior prose', () => {
  it('renders an allow hook decision with "allowed this action"', () => {
    assert.equal(
      createPermissionRequestMessage({ type: 'hook', hookName: 'PreToolUse', behavior: 'allow' }),
      'Hook "PreToolUse" allowed this action',
    );
  });
  it('renders a deny hook decision with "denied this action"', () => {
    assert.equal(
      createPermissionRequestMessage({ type: 'hook', hookName: 'PreToolUse', behavior: 'deny' }),
      'Hook "PreToolUse" denied this action',
    );
  });
  it('renders an ask hook decision with "asked for confirmation for this action"', () => {
    assert.equal(
      createPermissionRequestMessage({ type: 'hook', hookName: 'guard', behavior: 'ask' }),
      'Hook "guard" asked for confirmation for this action',
    );
  });
  it('appends the hook reason when present', () => {
    assert.equal(
      createPermissionRequestMessage({ type: 'hook', hookName: 'guard', behavior: 'deny', reason: 'blocked path' }),
      'Hook "guard" denied this action: blocked path',
    );
  });
  it('falls back to the generic phrasing when no behavior is attached', () => {
    assert.equal(
      createPermissionRequestMessage({ type: 'hook', hookName: 'guard' }),
      'Hook "guard" requires approval for this action',
    );
  });
});

// ---------------------------------------------------------------------------
// U003 — getAutoAllowedCommands(mode) introspection accessor.
// Parity: Claude's BashTool/modeValidation.ts getAutoAllowedCommands.
// ---------------------------------------------------------------------------
describe('getAutoAllowedCommands', () => {
  it('acceptEdits returns the sorted edit-adjacent fs command set', () => {
    assert.deepEqual([...getAutoAllowedCommands('acceptEdits')], ['cp', 'mkdir', 'mv', 'sed', 'touch']);
  });

  it('every mode other than acceptEdits auto-allows no fixed command set', () => {
    for (const mode of PERMISSION_MODES) {
      if (mode === 'acceptEdits') continue;
      assert.deepEqual([...getAutoAllowedCommands(mode)], [], `mode ${mode} should return []`);
    }
  });

  it('is the single source of truth shared with the acceptEdits runtime gate', () => {
    // Every command the accessor reports must actually be auto-allowed by the
    // runtime acceptEdits filesystem-mutation gate (no drift between the two).
    for (const cmd of getAutoAllowedCommands('acceptEdits')) {
      assert.equal(isAcceptEditsFilesystemMutation(`${cmd} foo`), true, `${cmd} should be an acceptEdits fs mutation`);
    }
    // AMI's superset divergence: rm stays gated even though Claude auto-allows it.
    assert.equal(getAutoAllowedCommands('acceptEdits').includes('rm'), false);
  });
});

// ---------------------------------------------------------------------------
// U003 — the accessor's data flows out through the default-patterns introspection
// surface (consumed by `/permissions defaults`).
// ---------------------------------------------------------------------------
describe('getDefaultPermissionPatterns — acceptEditsAutoAllowedCommands', () => {
  it('exposes the acceptEdits auto-allowed commands matching the accessor', () => {
    const defaults = getDefaultPermissionPatterns();
    assert.deepEqual(defaults.acceptEditsAutoAllowedCommands, [...getAutoAllowedCommands('acceptEdits')]);
  });
});
