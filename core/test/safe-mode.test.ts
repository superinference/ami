import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  isBlockedInSafeMode,
  isPartiallyBlocked,
  shouldEnableSafeMode,
  SafeModeGuard,
} from '../src/safe-mode';
import type { BlockedResource } from '../src/safe-mode';

describe('isBlockedInSafeMode', () => {
  it('returns true for prompt_override', () => {
    assert.equal(isBlockedInSafeMode('prompt_override'), true);
  });

  it('returns true for project_memory', () => {
    assert.equal(isBlockedInSafeMode('project_memory'), true);
  });

  it('returns true for project_skills', () => {
    assert.equal(isBlockedInSafeMode('project_skills'), true);
  });

  it('returns true for mcp_servers', () => {
    assert.equal(isBlockedInSafeMode('mcp_servers'), true);
  });

  it('returns true for hooks', () => {
    assert.equal(isBlockedInSafeMode('hooks'), true);
  });

  it('returns true for plugins', () => {
    assert.equal(isBlockedInSafeMode('plugins'), true);
  });

  it('returns false for project_config (partially blocked)', () => {
    assert.equal(isBlockedInSafeMode('project_config'), false);
  });
});

describe('isPartiallyBlocked', () => {
  it('returns true for project_config', () => {
    assert.equal(isPartiallyBlocked('project_config'), true);
  });

  it('returns false for prompt_override', () => {
    assert.equal(isPartiallyBlocked('prompt_override'), false);
  });

  it('returns false for plugins', () => {
    assert.equal(isPartiallyBlocked('plugins'), false);
  });
});

describe('shouldEnableSafeMode', () => {
  it('returns true for untrusted project', () => {
    assert.equal(shouldEnableSafeMode('/tmp', { untrustedProject: true }), true);
  });

  it('returns true for unknown clone', () => {
    assert.equal(shouldEnableSafeMode('/tmp', { clonedFromUnknown: true }), true);
  });

  it('returns false with no indicators', () => {
    assert.equal(shouldEnableSafeMode('/tmp', {}), false);
  });

  it('returns false with all indicators false', () => {
    assert.equal(shouldEnableSafeMode('/tmp', {
      untrustedProject: false,
      clonedFromUnknown: false,
      hasPromptOverride: false,
      hasUntrustedMcp: false,
    }), false);
  });

  it('returns true for combined override + untrusted mcp', () => {
    assert.equal(shouldEnableSafeMode('/tmp', {
      hasPromptOverride: true,
      hasUntrustedMcp: true,
    }), true);
  });

  it('returns false for override alone', () => {
    assert.equal(shouldEnableSafeMode('/tmp', { hasPromptOverride: true }), false);
  });

  it('returns false for untrusted mcp alone', () => {
    assert.equal(shouldEnableSafeMode('/tmp', { hasUntrustedMcp: true }), false);
  });
});

describe('SafeModeGuard', () => {
  it('isEnabled returns true when enabled', () => {
    const guard = new SafeModeGuard({ enabled: true });
    assert.equal(guard.isEnabled(), true);
  });

  it('isEnabled returns false when disabled', () => {
    const guard = new SafeModeGuard({ enabled: false });
    assert.equal(guard.isEnabled(), false);
  });

  it('getReason returns default reason', () => {
    const guard = new SafeModeGuard({ enabled: true });
    assert.equal(guard.getReason(), 'safe mode active');
  });

  it('getReason returns custom reason', () => {
    const guard = new SafeModeGuard({ enabled: true, reason: 'untrusted repo' });
    assert.equal(guard.getReason(), 'untrusted repo');
  });

  it('check blocks resources in safe mode', () => {
    const guard = new SafeModeGuard({ enabled: true });
    assert.equal(guard.check('prompt_override'), false);
    assert.equal(guard.check('hooks'), false);
  });

  it('check allows resources when disabled', () => {
    const guard = new SafeModeGuard({ enabled: false });
    assert.equal(guard.check('prompt_override'), true);
    assert.equal(guard.check('hooks'), true);
  });

  it('check allows project_config (not fully blocked)', () => {
    const guard = new SafeModeGuard({ enabled: true });
    assert.equal(guard.check('project_config'), true);
  });

  it('check records path when blocking', () => {
    const guard = new SafeModeGuard({ enabled: true });
    guard.check('prompt_override', '/project/.superinference/override.yaml');
    const blocked = guard.getBlocked();
    assert.equal(blocked.length, 1);
    assert.equal(blocked[0].resource, 'prompt_override');
    assert.equal(blocked[0].path, '/project/.superinference/override.yaml');
    assert.ok(blocked[0].timestamp > 0);
  });

  it('getBlocked tracks blocked attempts', () => {
    const guard = new SafeModeGuard({ enabled: true });
    guard.check('prompt_override');
    guard.check('hooks');
    guard.check('plugins');
    assert.equal(guard.getBlocked().length, 3);
  });

  it('getBlockedCount returns correct count', () => {
    const guard = new SafeModeGuard({ enabled: true });
    guard.check('mcp_servers');
    guard.check('project_skills');
    assert.equal(guard.getBlockedCount(), 2);
  });

  it('enable activates safe mode', () => {
    const guard = new SafeModeGuard({ enabled: false });
    assert.equal(guard.isEnabled(), false);
    guard.enable('detected threat');
    assert.equal(guard.isEnabled(), true);
    assert.equal(guard.getReason(), 'detected threat');
  });

  it('disable deactivates safe mode', () => {
    const guard = new SafeModeGuard({ enabled: true, reason: 'test' });
    guard.disable();
    assert.equal(guard.isEnabled(), false);
    assert.equal(guard.getReason(), '');
  });

  it('getBlockedResources returns all blocked types', () => {
    const guard = new SafeModeGuard({ enabled: true });
    const resources = guard.getBlockedResources();
    assert.ok(resources.includes('prompt_override'));
    assert.ok(resources.includes('project_memory'));
    assert.ok(resources.includes('project_skills'));
    assert.ok(resources.includes('mcp_servers'));
    assert.ok(resources.includes('hooks'));
    assert.ok(resources.includes('plugins'));
    assert.ok(!resources.includes('project_config'));
  });

  it('clear resets blocked list', () => {
    const guard = new SafeModeGuard({ enabled: true });
    guard.check('hooks');
    guard.check('plugins');
    assert.equal(guard.getBlockedCount(), 2);
    guard.clear();
    assert.equal(guard.getBlockedCount(), 0);
    assert.equal(guard.getBlocked().length, 0);
  });

  it('does not block when disabled even after enable/disable cycle', () => {
    const guard = new SafeModeGuard({ enabled: true });
    assert.equal(guard.check('hooks'), false);
    guard.disable();
    assert.equal(guard.check('hooks'), true);
  });
});
