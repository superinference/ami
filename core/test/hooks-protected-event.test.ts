import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { canHookEvent, isProtectedEvent, PROTECTED_EVENTS } from '../src/hook-protection';

describe('hooks-protected-event: Gap 15 — block user-tier hooks on protected events', () => {
  it('PROTECTED_EVENTS is a non-empty set', () => {
    assert.ok(PROTECTED_EVENTS.size > 0, 'PROTECTED_EVENTS should not be empty');
  });

  it('session.compact is a protected event', () => {
    assert.ok(isProtectedEvent('session.compact'));
  });

  it('prompt.compose is a protected event', () => {
    assert.ok(isProtectedEvent('prompt.compose'));
  });

  it('settings.read is a protected event', () => {
    assert.ok(isProtectedEvent('settings.read'));
  });

  it('user-tier CANNOT hook session.compact', () => {
    assert.equal(canHookEvent('session.compact', 'user'), false);
  });

  it('user-tier CANNOT hook prompt.compose', () => {
    assert.equal(canHookEvent('prompt.compose', 'user'), false);
  });

  it('user-tier CANNOT hook settings.read', () => {
    assert.equal(canHookEvent('settings.read', 'user'), false);
  });

  it('builtin-tier CAN hook session.compact', () => {
    assert.equal(canHookEvent('session.compact', 'builtin'), true);
  });

  it('prepend-tier CAN hook session.compact', () => {
    assert.equal(canHookEvent('session.compact', 'prepend'), true);
  });

  it('builtin-tier CAN hook all protected events', () => {
    for (const event of PROTECTED_EVENTS) {
      assert.equal(canHookEvent(event, 'builtin'), true, `builtin should be able to hook ${event}`);
    }
  });

  it('user-tier CAN hook non-protected passthrough events', () => {
    assert.equal(canHookEvent('prompt.submit', 'user'), true);
    assert.equal(canHookEvent('turn.start', 'user'), true);
    assert.equal(canHookEvent('turn.end', 'user'), true);
    assert.equal(canHookEvent('tool.call', 'user'), true);
    assert.equal(canHookEvent('command.run', 'user'), true);
  });

  it('non-protected events are allowed for any tier', () => {
    const tiers = ['user', 'builtin', 'prepend', 'append', 'core'] as const;
    for (const tier of tiers) {
      assert.equal(
        canHookEvent('turn.start', tier), true,
        `${tier} should be able to hook turn.start`,
      );
    }
  });

  it('isProtectedEvent returns false for non-protected events', () => {
    assert.equal(isProtectedEvent('turn.start'), false);
    assert.equal(isProtectedEvent('tool.call'), false);
    assert.equal(isProtectedEvent('command.run'), false);
    assert.equal(isProtectedEvent('random.event'), false);
  });
});
