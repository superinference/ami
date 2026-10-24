import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  PROTECTED_EVENTS,
  PASSTHROUGH_EVENTS,
  isProtectedEvent,
  canHookEvent,
  sortHooksByTier,
  isStricterVerdict,
  HookChainGuard,
  type HookTier,
  type HookRegistration,
} from '../src/hook-protection';

const ALL_TIERS: HookTier[] = ['builtin', 'prepend', 'user', 'append', 'core'];

describe('canHookEvent — protected-event guard is exact-match (no bypass via case/whitespace)', () => {
  it('blocks ONLY the user tier for every protected event; all other tiers pass', () => {
    for (const event of PROTECTED_EVENTS) {
      for (const tier of ALL_TIERS) {
        const expected = tier !== 'user';
        assert.equal(
          canHookEvent(event, tier),
          expected,
          `tier=${tier} on protected event=${event} should be ${expected}`,
        );
      }
    }
  });

  it('case-variation does NOT make a protected name count as protected (exact match), so a user hook on the mutated string is NOT the real protected event', () => {
    // SECURITY: 'Session.Compact' / 'SESSION.COMPACT' are distinct strings. The guard
    // must treat them as unprotected custom events — they are NOT aliases that let a
    // user-tier hook intercept the real 'session.compact' dispatch.
    for (const mutated of ['Session.Compact', 'SESSION.COMPACT', 'session.Compact']) {
      assert.equal(isProtectedEvent(mutated), false, `${mutated} must not be protected`);
      assert.equal(canHookEvent(mutated, 'user'), true, `${mutated} is a different event`);
    }
    // The real, exactly-cased event stays locked for user tier.
    assert.equal(canHookEvent('session.compact', 'user'), false);
  });

  it('whitespace/padding variants of a protected name are NOT protected (exact match)', () => {
    for (const mutated of [' session.compact', 'session.compact ', 'session.compact\t', '\nsession.compact']) {
      assert.equal(isProtectedEvent(mutated), false, `"${mutated}" must not match`);
      assert.equal(canHookEvent(mutated, 'user'), true);
    }
    assert.equal(canHookEvent('session.compact', 'user'), false);
  });

  it('a passthrough event with a protected-looking suffix is not confused for protected', () => {
    // substring / prefix tricks must not trip the Set membership check
    assert.equal(isProtectedEvent('xsession.compact'), false);
    assert.equal(isProtectedEvent('session.compact.extra'), false);
    assert.equal(canHookEvent('session.compact.extra', 'user'), true);
  });

  it('PROTECTED_EVENTS and PASSTHROUGH_EVENTS are disjoint', () => {
    for (const e of PASSTHROUGH_EVENTS) {
      assert.equal(PROTECTED_EVENTS.has(e), false, `${e} must not be in both sets`);
      assert.equal(canHookEvent(e, 'user'), true, `passthrough ${e} must allow user tier`);
    }
  });
});

describe('sortHooksByTier — full 5-tier ordering incl. append/core and edge cases', () => {
  it('orders all five tiers builtin<prepend<user<append<core regardless of input order', () => {
    const hooks: HookRegistration[] = [
      { name: 'core', event: 'e', tier: 'core', source: 's', priority: 0 },
      { name: 'append', event: 'e', tier: 'append', source: 's', priority: 0 },
      { name: 'user', event: 'e', tier: 'user', source: 's', priority: 0 },
      { name: 'prepend', event: 'e', tier: 'prepend', source: 's', priority: 0 },
      { name: 'builtin', event: 'e', tier: 'builtin', source: 's', priority: 0 },
    ];
    const sorted = sortHooksByTier(hooks).map((h) => h.tier);
    assert.deepEqual(sorted, ['builtin', 'prepend', 'user', 'append', 'core']);
  });

  it('tie-break uses priority ascending within the same tier, incl. negative priorities', () => {
    const hooks: HookRegistration[] = [
      { name: 'p-hi', event: 'e', tier: 'core', source: 's', priority: 100 },
      { name: 'p-neg', event: 'e', tier: 'core', source: 's', priority: -5 },
      { name: 'p-zero', event: 'e', tier: 'core', source: 's', priority: 0 },
    ];
    const sorted = sortHooksByTier(hooks).map((h) => h.name);
    assert.deepEqual(sorted, ['p-neg', 'p-zero', 'p-hi']);
  });

  it('empty array returns a new empty array (comparator never runs)', () => {
    const input: HookRegistration[] = [];
    const out = sortHooksByTier(input);
    assert.deepEqual(out, []);
    assert.notEqual(out, input, 'must be a fresh array, not the same reference');
  });

  it('single-element array returns a copy (comparator never runs)', () => {
    const input: HookRegistration[] = [
      { name: 'only', event: 'e', tier: 'user', source: 's', priority: 3 },
    ];
    const out = sortHooksByTier(input);
    assert.deepEqual(out, input);
    assert.notEqual(out, input);
  });

  it('does not mutate the caller array even when reordering is required', () => {
    const hooks: HookRegistration[] = [
      { name: 'core', event: 'e', tier: 'core', source: 's', priority: 0 },
      { name: 'builtin', event: 'e', tier: 'builtin', source: 's', priority: 0 },
    ];
    const snapshot = hooks.map((h) => h.name);
    sortHooksByTier(hooks);
    assert.deepEqual(hooks.map((h) => h.name), snapshot);
  });
});

describe('isStricterVerdict — complete 3x3 matrix', () => {
  const rank: Record<'allow' | 'deny' | 'ask', number> = { allow: 0, ask: 1, deny: 2 };
  const verdicts = ['allow', 'ask', 'deny'] as const;
  it('returns true iff current outranks previous for every pair', () => {
    for (const current of verdicts) {
      for (const previous of verdicts) {
        const expected = rank[current] > rank[previous];
        assert.equal(
          isStricterVerdict(current, previous),
          expected,
          `isStricterVerdict(${current}, ${previous}) should be ${expected}`,
        );
      }
    }
  });
});

describe('HookChainGuard — accumulation, snapshot, and reason integrity', () => {
  it('accumulates one entry per blocked protected event with correct event/tier/reason', () => {
    const guard = new HookChainGuard();
    for (const event of PROTECTED_EVENTS) {
      assert.equal(guard.checkRegistration(event, 'user'), false);
    }
    const blocked = guard.getBlocked();
    assert.equal(blocked.length, PROTECTED_EVENTS.size);
    for (const entry of blocked) {
      assert.equal(entry.tier, 'user');
      assert.ok(PROTECTED_EVENTS.has(entry.event));
      assert.ok(entry.reason.includes(entry.event), 'reason must name the event');
      assert.ok(entry.reason.includes('User-tier'), 'reason must identify user tier');
    }
    assert.equal(guard.getBlockedCount(), PROTECTED_EVENTS.size);
  });

  it('allowed registrations across all non-user tiers are never recorded', () => {
    const guard = new HookChainGuard();
    for (const event of PROTECTED_EVENTS) {
      for (const tier of ALL_TIERS) {
        if (tier === 'user') continue;
        assert.equal(guard.checkRegistration(event, tier), true);
      }
    }
    assert.equal(guard.getBlockedCount(), 0);
    assert.deepEqual(guard.getBlocked(), []);
  });

  it('interleaving allowed and blocked registrations only records the blocked ones', () => {
    const guard = new HookChainGuard();
    assert.equal(guard.checkRegistration('tool.call', 'user'), true); // passthrough, allowed
    assert.equal(guard.checkRegistration('session.compact', 'user'), false); // blocked
    assert.equal(guard.checkRegistration('prompt.compose', 'builtin'), true); // protected but builtin
    assert.equal(guard.checkRegistration('settings.read', 'user'), false); // blocked
    const blocked = guard.getBlocked();
    assert.deepEqual(blocked.map((b) => b.event), ['session.compact', 'settings.read']);
  });

  it('clear() fully resets and the guard is reusable afterward', () => {
    const guard = new HookChainGuard();
    guard.checkRegistration('session.compact', 'user');
    guard.clear();
    assert.equal(guard.getBlockedCount(), 0);
    // reusable: a subsequent block is recorded normally
    guard.checkRegistration('prompt.context', 'user');
    assert.equal(guard.getBlockedCount(), 1);
    assert.equal(guard.getBlocked()[0].event, 'prompt.context');
  });

  it('getBlocked snapshot grows as more violations are recorded (live view of internal state)', () => {
    const guard = new HookChainGuard();
    guard.checkRegistration('session.compact', 'user');
    const first = guard.getBlocked();
    assert.equal(first.length, 1);
    guard.checkRegistration('prompt.compose', 'user');
    assert.equal(guard.getBlocked().length, 2);
  });
});
