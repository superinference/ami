import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  shouldEnableSafeMode,
  SafeModeGuard,
} from '../src/safe-mode';

// Wave 1 coverage: pin remaining uncovered branches in safe-mode.ts.
// Each test asserts real guard behavior so a deleted/weakened guard would fail it.

describe('shouldEnableSafeMode — branch variants', () => {
  // untrustedProject true must win regardless of other (falsy) indicators.
  it('untrustedProject true short-circuits before clone/override checks', () => {
    assert.equal(shouldEnableSafeMode('/repo', {
      untrustedProject: true,
      clonedFromUnknown: false,
      hasPromptOverride: false,
      hasUntrustedMcp: false,
    }), true);
  });

  // untrustedProject false, clonedFromUnknown true → second guard fires.
  it('clonedFromUnknown true fires when untrustedProject is false', () => {
    assert.equal(shouldEnableSafeMode('/repo', {
      untrustedProject: false,
      clonedFromUnknown: true,
    }), true);
  });

  // hasPromptOverride true but hasUntrustedMcp explicitly undefined → && right
  // operand is evaluated and falsy → no safe mode. (override alone is not enough)
  it('hasPromptOverride true with hasUntrustedMcp undefined does NOT enable', () => {
    assert.equal(shouldEnableSafeMode('/repo', {
      hasPromptOverride: true,
      hasUntrustedMcp: undefined,
    }), false);
  });

  // hasPromptOverride undefined (falsy) short-circuits && before mcp is read.
  it('hasPromptOverride undefined short-circuits even with untrusted mcp', () => {
    assert.equal(shouldEnableSafeMode('/repo', {
      hasPromptOverride: undefined,
      hasUntrustedMcp: true,
    }), false);
  });

  // Both true → enable via the compound guard (right operand truthy).
  it('both prompt override and untrusted mcp true enables safe mode', () => {
    assert.equal(shouldEnableSafeMode('/repo', {
      hasPromptOverride: true,
      hasUntrustedMcp: true,
    }), true);
  });
});

describe('SafeModeGuard — reason nullish branches', () => {
  // config.reason present-but-empty-string is NOT nullish → kept as ''.
  it('empty-string reason is preserved (not replaced by default)', () => {
    const guard = new SafeModeGuard({ enabled: true, reason: '' });
    assert.equal(guard.getReason(), '');
  });

  // config.reason null → nullish → default applies.
  it('null reason falls back to default', () => {
    const guard = new SafeModeGuard({ enabled: true, reason: null as unknown as undefined });
    assert.equal(guard.getReason(), 'safe mode active');
  });
});

describe('SafeModeGuard — check guard integrity', () => {
  // Disabled guard records nothing even for a would-be-blocked resource.
  it('disabled guard does not record blocked attempts', () => {
    const guard = new SafeModeGuard({ enabled: false });
    assert.equal(guard.check('plugins'), true);
    assert.equal(guard.getBlockedCount(), 0);
  });

  // Enabled guard allows partially-blocked project_config without recording it.
  it('enabled guard allows project_config and records nothing', () => {
    const guard = new SafeModeGuard({ enabled: true });
    assert.equal(guard.check('project_config', '/p/.superinference/config.yaml'), true);
    assert.equal(guard.getBlockedCount(), 0);
  });

  // Every fully-blocked resource is actually blocked + recorded with its path.
  it('blocks and records every fully-blocked resource with path', () => {
    const guard = new SafeModeGuard({ enabled: true });
    const resources = guard.getBlockedResources();
    for (const r of resources) {
      assert.equal(guard.check(r, `/p/${r}`), false);
    }
    assert.equal(guard.getBlockedCount(), resources.length);
    const blocked = guard.getBlocked();
    for (let i = 0; i < resources.length; i++) {
      assert.equal(blocked[i].resource, resources[i]);
      assert.equal(blocked[i].path, `/p/${resources[i]}`);
    }
  });
});
