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
  type HookRegistration,
} from '../src/hook-protection';

describe('PROTECTED_EVENTS', () => {
  it('contains session.compact', () => {
    assert.ok(PROTECTED_EVENTS.has('session.compact'));
  });
  it('contains prompt.compose', () => {
    assert.ok(PROTECTED_EVENTS.has('prompt.compose'));
  });
  it('contains prompt.section', () => {
    assert.ok(PROTECTED_EVENTS.has('prompt.section'));
  });
  it('contains prompt.context', () => {
    assert.ok(PROTECTED_EVENTS.has('prompt.context'));
  });
  it('contains prompt.attachment', () => {
    assert.ok(PROTECTED_EVENTS.has('prompt.attachment'));
  });
  it('contains settings.read', () => {
    assert.ok(PROTECTED_EVENTS.has('settings.read'));
  });
});

describe('PASSTHROUGH_EVENTS', () => {
  it('contains prompt.submit', () => {
    assert.ok(PASSTHROUGH_EVENTS.has('prompt.submit'));
  });
  it('contains tool.call', () => {
    assert.ok(PASSTHROUGH_EVENTS.has('tool.call'));
  });
  it('contains turn.start', () => {
    assert.ok(PASSTHROUGH_EVENTS.has('turn.start'));
  });
  it('contains command.run', () => {
    assert.ok(PASSTHROUGH_EVENTS.has('command.run'));
  });
});

describe('isProtectedEvent', () => {
  it('returns true for protected events', () => {
    assert.equal(isProtectedEvent('session.compact'), true);
    assert.equal(isProtectedEvent('prompt.compose'), true);
    assert.equal(isProtectedEvent('settings.read'), true);
  });
  it('returns false for passthrough events', () => {
    assert.equal(isProtectedEvent('prompt.submit'), false);
    assert.equal(isProtectedEvent('tool.call'), false);
  });
  it('returns false for unknown events', () => {
    assert.equal(isProtectedEvent('custom.event'), false);
    assert.equal(isProtectedEvent(''), false);
  });
});

describe('canHookEvent', () => {
  it('allows builtin on protected events', () => {
    assert.equal(canHookEvent('session.compact', 'builtin'), true);
  });
  it('allows prepend on protected events', () => {
    assert.equal(canHookEvent('session.compact', 'prepend'), true);
  });
  it('denies user on protected events', () => {
    assert.equal(canHookEvent('session.compact', 'user'), false);
    assert.equal(canHookEvent('prompt.compose', 'user'), false);
    assert.equal(canHookEvent('settings.read', 'user'), false);
  });
  it('allows user on passthrough events', () => {
    assert.equal(canHookEvent('prompt.submit', 'user'), true);
    assert.equal(canHookEvent('tool.call', 'user'), true);
  });
  it('allows append on protected events', () => {
    assert.equal(canHookEvent('session.compact', 'append'), true);
  });
  it('allows core on protected events', () => {
    assert.equal(canHookEvent('prompt.compose', 'core'), true);
  });
  it('allows all tiers on unknown events', () => {
    assert.equal(canHookEvent('custom.event', 'user'), true);
    assert.equal(canHookEvent('custom.event', 'builtin'), true);
  });
});

describe('sortHooksByTier', () => {
  it('orders by tier priority', () => {
    const hooks: HookRegistration[] = [
      { name: 'c', event: 'e', tier: 'user', source: 's', priority: 0 },
      { name: 'a', event: 'e', tier: 'builtin', source: 's', priority: 0 },
      { name: 'b', event: 'e', tier: 'prepend', source: 's', priority: 0 },
    ];
    const sorted = sortHooksByTier(hooks);
    assert.equal(sorted[0].tier, 'builtin');
    assert.equal(sorted[1].tier, 'prepend');
    assert.equal(sorted[2].tier, 'user');
  });
  it('breaks ties by priority number', () => {
    const hooks: HookRegistration[] = [
      { name: 'b', event: 'e', tier: 'user', source: 's', priority: 5 },
      { name: 'a', event: 'e', tier: 'user', source: 's', priority: 1 },
    ];
    const sorted = sortHooksByTier(hooks);
    assert.equal(sorted[0].name, 'a');
    assert.equal(sorted[1].name, 'b');
  });
  it('does not mutate original array', () => {
    const hooks: HookRegistration[] = [
      { name: 'b', event: 'e', tier: 'append', source: 's', priority: 0 },
      { name: 'a', event: 'e', tier: 'builtin', source: 's', priority: 0 },
    ];
    sortHooksByTier(hooks);
    assert.equal(hooks[0].name, 'b');
  });
});

describe('isStricterVerdict', () => {
  it('deny is stricter than ask', () => {
    assert.equal(isStricterVerdict('deny', 'ask'), true);
  });
  it('ask is stricter than allow', () => {
    assert.equal(isStricterVerdict('ask', 'allow'), true);
  });
  it('deny is stricter than allow', () => {
    assert.equal(isStricterVerdict('deny', 'allow'), true);
  });
  it('allow is not stricter than deny', () => {
    assert.equal(isStricterVerdict('allow', 'deny'), false);
  });
  it('allow is not stricter than ask', () => {
    assert.equal(isStricterVerdict('allow', 'ask'), false);
  });
  it('same verdict is not stricter', () => {
    assert.equal(isStricterVerdict('deny', 'deny'), false);
    assert.equal(isStricterVerdict('allow', 'allow'), false);
  });
});

describe('HookChainGuard', () => {
  it('blocks user on protected event', () => {
    const guard = new HookChainGuard();
    assert.equal(guard.checkRegistration('session.compact', 'user'), false);
  });
  it('allows builtin on protected event', () => {
    const guard = new HookChainGuard();
    assert.equal(guard.checkRegistration('session.compact', 'builtin'), true);
  });
  it('allows prepend on protected event', () => {
    const guard = new HookChainGuard();
    assert.equal(guard.checkRegistration('prompt.compose', 'prepend'), true);
  });
  it('allows user on passthrough event', () => {
    const guard = new HookChainGuard();
    assert.equal(guard.checkRegistration('tool.call', 'user'), true);
  });
  it('tracks blocked count', () => {
    const guard = new HookChainGuard();
    guard.checkRegistration('session.compact', 'user');
    guard.checkRegistration('prompt.compose', 'user');
    assert.equal(guard.getBlockedCount(), 2);
  });
  it('getBlocked returns details', () => {
    const guard = new HookChainGuard();
    guard.checkRegistration('settings.read', 'user');
    const blocked = guard.getBlocked();
    assert.equal(blocked.length, 1);
    assert.equal(blocked[0].event, 'settings.read');
    assert.equal(blocked[0].tier, 'user');
    assert.ok(blocked[0].reason.includes('settings.read'));
  });
  it('clear resets blocked list', () => {
    const guard = new HookChainGuard();
    guard.checkRegistration('session.compact', 'user');
    assert.equal(guard.getBlockedCount(), 1);
    guard.clear();
    assert.equal(guard.getBlockedCount(), 0);
    assert.deepEqual(guard.getBlocked(), []);
  });
  it('does not track allowed registrations', () => {
    const guard = new HookChainGuard();
    guard.checkRegistration('tool.call', 'user');
    guard.checkRegistration('session.compact', 'builtin');
    assert.equal(guard.getBlockedCount(), 0);
  });
});
