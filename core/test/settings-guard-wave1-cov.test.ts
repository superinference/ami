import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  canOverride,
  getEffectiveTier,
  filterManagedKeys,
  SettingsGuard,
  MANAGED_ONLY_KEYS,
  TIER_LOCKED_KEYS,
} from '../src/settings-guard';

// Wave-1 coverage: pins branch behavior that existing tests leave uncovered.
// Each test discriminates real guard behavior (would fail if the guard logic
// were removed/inverted), not just touches a line.

describe('getEffectiveTier — normal key, later source has LOWER priority', () => {
  // Exercises the `>` false short-circuit on the normal-key reduction loop:
  // when a subsequent source ranks below the current best it must NOT replace it.
  it('keeps the earlier higher-priority source when a lower one follows', () => {
    // cli (5) first, then managed (0): managed is NOT > cli, so cli wins.
    assert.equal(
      getEffectiveTier('customKey', [{ tier: 'cli' }, { tier: 'managed' }]),
      'cli',
    );
  });

  it('keeps cli over a trailing project for a normal key', () => {
    // env (4) then local (3) then project (2): all later ones rank lower,
    // so the first (env) stays the effective tier.
    assert.equal(
      getEffectiveTier('customKey', [
        { tier: 'env' },
        { tier: 'local' },
        { tier: 'project' },
      ]),
      'env',
    );
  });
});

describe('getEffectiveTier — tier-locked key, later source has HIGHER priority number', () => {
  // Exercises the `<` false short-circuit on the tier-locked reduction loop:
  // a later source with a worse (higher) priority number must NOT win.
  it('keeps the lowest-priority-number source when a higher-number one follows', () => {
    // managed (0) first, then cli (5): cli is NOT < managed, managed wins.
    assert.equal(
      getEffectiveTier('permissionMode', [{ tier: 'managed' }, { tier: 'cli' }]),
      'managed',
    );
  });
});

describe('getEffectiveTier — single managed-only source present among non-managed', () => {
  it('returns managed only when a managed source exists', () => {
    assert.equal(
      getEffectiveTier('processWrapper', [{ tier: 'local' }, { tier: 'managed' }]),
      'managed',
    );
    assert.equal(
      getEffectiveTier('processWrapper', [{ tier: 'local' }, { tier: 'cli' }]),
      null,
    );
  });
});

describe('canOverride — guard discrimination', () => {
  it('blocks a managed-only key even when overriding the lowest tier', () => {
    // If MANAGED_ONLY guard were removed this would wrongly return true.
    assert.equal(canOverride('processWrapper', 'cli', 'cli'), false);
  });

  it('allows a managed-only key set by managed over any existing tier', () => {
    assert.equal(canOverride('processWrapper', 'managed', 'cli'), true);
  });

  it('allows tier-locked downward (lower priority number overriding higher)', () => {
    // managed (0) overriding an existing local (3): 0 > 3 is false -> allowed.
    assert.equal(canOverride('maxTurns', 'managed', 'local'), true);
  });

  it('blocks tier-locked upward (higher priority number vs lower)', () => {
    // local (3) overriding existing global (1): 3 > 1 -> blocked.
    assert.equal(canOverride('maxTurns', 'local', 'global'), false);
  });
});

describe('filterManagedKeys — rejection vs passthrough', () => {
  it('rejects every managed-only key for a non-managed tier and keeps the rest', () => {
    const config = {
      allowManagedModsOnly: true,
      allowedMcpServers: ['a'],
      theme: 'dark',
      maxTurns: 10,
    };
    const { filtered, rejected } = filterManagedKeys(config, 'global');
    // theme and maxTurns are NOT managed-only, so they survive.
    assert.deepEqual(filtered, { theme: 'dark', maxTurns: 10 });
    assert.deepEqual(rejected.sort(), ['allowManagedModsOnly', 'allowedMcpServers']);
  });

  it('passes all keys through for the managed tier', () => {
    const config = { allowManagedModsOnly: true, theme: 'dark' };
    const { filtered, rejected } = filterManagedKeys(config, 'managed');
    assert.deepEqual(filtered, config);
    assert.deepEqual(rejected, []);
  });

  it('returns empty structures for empty config', () => {
    const { filtered, rejected } = filterManagedKeys({}, 'project');
    assert.deepEqual(filtered, {});
    assert.deepEqual(rejected, []);
  });
});

describe('SettingsGuard — blocked-reason wording and accumulation', () => {
  it('uses the managed-config reason for managed-only keys', () => {
    const guard = new SettingsGuard();
    assert.equal(guard.checkOverride('processWrapper', 'project', 'managed'), false);
    const [b] = guard.getBlockedOverrides();
    assert.equal(b.reason, '"processWrapper" can only be set by managed config');
  });

  it('uses the tier-locked reason for tier-locked keys and names both tiers', () => {
    const guard = new SettingsGuard();
    assert.equal(guard.checkOverride('maxSessionMs', 'cli', 'global'), false);
    const [b] = guard.getBlockedOverrides();
    assert.equal(
      b.reason,
      '"maxSessionMs" is locked at tier "global" and cannot be overridden by "cli"',
    );
  });

  it('does not record a blocked entry for an allowed override', () => {
    const guard = new SettingsGuard();
    assert.equal(guard.checkOverride('theme', 'cli', 'managed'), true);
    assert.equal(guard.getBlockedCount(), 0);
    assert.deepEqual(guard.getBlockedOverrides(), []);
  });
});

describe('constant sets are frozen contracts', () => {
  it('managed-only and tier-locked sets are disjoint', () => {
    for (const k of MANAGED_ONLY_KEYS) {
      assert.ok(!TIER_LOCKED_KEYS.has(k), `${k} must not be in both sets`);
    }
  });
});
