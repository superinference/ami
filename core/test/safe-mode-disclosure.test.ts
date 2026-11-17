import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  BLOCKED_RESOURCES,
  isBlockedInSafeMode,
  isPartiallyBlocked,
  resourceTier,
  describeBlocked,
  RESOURCE_DESCRIPTIONS,
  SAFE_MODE_GUIDANCE,
  SafeModeGuard,
} from '../src/safe-mode';
import type { BlockedResource } from '../src/safe-mode';

// Unit — U122 safe-mode disclosure/enforcement parity additions.

describe('safe-mode :: dangerous_env_vars resource category', () => {
  it('is a declared blocked resource, fully blocked in safe mode', () => {
    assert.ok((BLOCKED_RESOURCES as readonly string[]).includes('dangerous_env_vars'));
    assert.equal(isBlockedInSafeMode('dangerous_env_vars'), true);
    assert.equal(isPartiallyBlocked('dangerous_env_vars'), false);
  });

  it('keeps the full/partial partition invariant', () => {
    for (const r of BLOCKED_RESOURCES) {
      const full = isBlockedInSafeMode(r);
      const partial = isPartiallyBlocked(r);
      assert.equal(full && partial, false, `${r} cannot be both`);
      assert.equal(full || partial, true, `${r} must be one`);
    }
  });
});

describe('safe-mode :: resourceTier', () => {
  it('classifies fully-blocked resources as blocked', () => {
    assert.equal(resourceTier('hooks'), 'blocked');
    assert.equal(resourceTier('mcp_servers'), 'blocked');
    assert.equal(resourceTier('dangerous_env_vars'), 'blocked');
  });
  it('classifies partially-blocked resources as restricted', () => {
    assert.equal(resourceTier('project_config'), 'restricted');
  });
  it('agrees with the isBlocked/isPartial classifiers for every resource', () => {
    for (const r of BLOCKED_RESOURCES) {
      const tier = resourceTier(r);
      if (isBlockedInSafeMode(r)) assert.equal(tier, 'blocked');
      else if (isPartiallyBlocked(r)) assert.equal(tier, 'restricted');
      else assert.equal(tier, 'allowed');
    }
  });
});

describe('safe-mode :: descriptions + guidance', () => {
  it('has a non-empty human-readable description for every resource', () => {
    for (const r of BLOCKED_RESOURCES) {
      assert.ok(RESOURCE_DESCRIPTIONS[r] && RESOURCE_DESCRIPTIONS[r].length > 0, `missing description for ${r}`);
    }
  });

  it('describeBlocked returns one labelled, tiered row per resource', () => {
    const rows = describeBlocked();
    assert.equal(rows.length, BLOCKED_RESOURCES.length);
    for (const row of rows) {
      assert.ok((BLOCKED_RESOURCES as readonly string[]).includes(row.resource));
      assert.equal(row.tier, resourceTier(row.resource));
      assert.equal(row.label, RESOURCE_DESCRIPTIONS[row.resource]);
      // U122: each row carries a concrete risk reason distinct from its label.
      assert.ok(row.why && row.why.length > 0, `missing why for ${row.resource}`);
      assert.notEqual(row.why, row.label);
    }
    // project_config surfaces as restricted, not blocked
    assert.equal(rows.find((r) => r.resource === 'project_config')?.tier, 'restricted');
  });

  it('SAFE_MODE_GUIDANCE is an actionable next step mentioning review', () => {
    assert.ok(SAFE_MODE_GUIDANCE.length > 0);
    assert.match(SAFE_MODE_GUIDANCE, /review/i);
    assert.match(SAFE_MODE_GUIDANCE, /safeMode/);
  });
});

describe('safe-mode :: SafeModeGuard partial-tier enforcement', () => {
  it('tierFor returns allowed for everything when disabled', () => {
    const g = new SafeModeGuard({ enabled: false });
    assert.equal(g.tierFor('hooks'), 'allowed');
    assert.equal(g.tierFor('project_config'), 'allowed');
  });

  it('checkTier enforces the restricted tier distinctly from blocked', () => {
    const g = new SafeModeGuard({ enabled: true });
    assert.equal(g.checkTier('hooks', '/h'), 'blocked');
    assert.equal(g.checkTier('project_config', '/c'), 'restricted');
    assert.equal(g.getBlockedCount(), 1);
    assert.equal(g.getRestrictedCount(), 1);
    assert.equal(g.getBlocked()[0]!.resource, 'hooks');
    assert.equal(g.getRestricted()[0]!.resource, 'project_config');
    assert.equal(g.getRestricted()[0]!.path, '/c');
  });

  it('check() stays a boolean full-block gate backed by the same tier logic', () => {
    const g = new SafeModeGuard({ enabled: true });
    assert.equal(g.check('hooks'), false);           // fully blocked
    assert.equal(g.check('project_config'), true);   // restricted still allowed via boolean gate
    assert.equal(g.getBlockedCount(), 1);
    assert.equal(g.getRestrictedCount(), 0);          // boolean check() does not record restricted
  });

  it('clear() resets both blocked and restricted logs', () => {
    const g = new SafeModeGuard({ enabled: true });
    g.checkTier('hooks');
    g.checkTier('project_config');
    g.clear();
    assert.equal(g.getBlockedCount(), 0);
    assert.equal(g.getRestrictedCount(), 0);
  });

  it('dangerous_env_vars is blocked by the guard when enabled', () => {
    const g = new SafeModeGuard({ enabled: true });
    const r: BlockedResource = 'dangerous_env_vars';
    assert.equal(g.check(r), false);
    assert.equal(g.checkTier(r), 'blocked');
  });
});
