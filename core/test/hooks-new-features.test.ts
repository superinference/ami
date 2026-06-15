/**
 * Tests for hooks new features:
 * - EventEmitter-based HookManager
 * - Workspace trust gating
 * - registerCallback (returns unsubscribe fn)
 * - getSummary (Record<string, number>)
 * - HookDecisionOutput type
 */

import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { HookManager } from '../src/hooks';

let hookManager: HookManager;

beforeEach(() => {
  hookManager = new HookManager();
});

// ---------------------------------------------------------------------------
// EventEmitter integration
// ---------------------------------------------------------------------------

describe('HookManager — EventEmitter', () => {
  it('is an instance of EventEmitter', () => {
    assert.ok(typeof hookManager.on === 'function');
    assert.ok(typeof hookManager.emit === 'function');
    assert.ok(typeof hookManager.removeAllListeners === 'function');
  });

  it('can register and fire events', () => {
    let fired = false;
    hookManager.on('test-event', () => { fired = true; });
    hookManager.emit('test-event');
    assert.equal(fired, true);
  });
});

// ---------------------------------------------------------------------------
// Workspace trust gating
// ---------------------------------------------------------------------------

describe('HookManager — workspace trust', () => {
  it('defaults to untrusted', () => {
    assert.equal(hookManager.isWorkspaceTrusted(), false);
  });

  it('can set workspace trusted', () => {
    hookManager.setWorkspaceTrusted(true);
    assert.equal(hookManager.isWorkspaceTrusted(), true);
  });

  it('can revoke trust', () => {
    hookManager.setWorkspaceTrusted(true);
    hookManager.setWorkspaceTrusted(false);
    assert.equal(hookManager.isWorkspaceTrusted(), false);
  });
});

// ---------------------------------------------------------------------------
// Session ID
// ---------------------------------------------------------------------------

describe('HookManager — session ID', () => {
  it('can set session ID without error', () => {
    hookManager.setSessionId('test-session-123');
    assert.ok(true);
  });
});

// ---------------------------------------------------------------------------
// Callback hooks
// ---------------------------------------------------------------------------

describe('HookManager — registerCallback', () => {
  it('registers a callback and it appears in summary', () => {
    hookManager.registerCallback('testHook', async () => undefined);
    const summary = hookManager.getSummary();
    assert.equal(summary.callbackHooks, 1);
  });

  it('returns an unsubscribe function', () => {
    const unsub = hookManager.registerCallback('hookA', async () => undefined);
    assert.equal(typeof unsub, 'function');
    assert.equal(hookManager.getSummary().callbackHooks, 1);
    unsub();
    assert.equal(hookManager.getSummary().callbackHooks, 0);
  });

  it('can register multiple callbacks on different events', () => {
    hookManager.registerCallback('hookA', async () => undefined);
    hookManager.registerCallback('hookB', async () => undefined);
    assert.equal(hookManager.getSummary().callbackHooks, 2);
  });

  it('can register multiple callbacks on the same event', () => {
    hookManager.registerCallback('hookA', async () => undefined);
    hookManager.registerCallback('hookA', async () => undefined);
    assert.equal(hookManager.getSummary().callbackHooks, 2);
  });
});

// ---------------------------------------------------------------------------
// getSummary
// ---------------------------------------------------------------------------

describe('HookManager — getSummary', () => {
  it('returns a Record<string, number>', () => {
    const summary = hookManager.getSummary();
    assert.equal(typeof summary, 'object');
    assert.equal(typeof summary.preToolUse, 'number');
    assert.equal(typeof summary.postToolUse, 'number');
    assert.equal(typeof summary.callbackHooks, 'number');
  });

  it('all counts start at zero', () => {
    const summary = hookManager.getSummary();
    for (const [key, value] of Object.entries(summary)) {
      assert.equal(value, 0, `${key} should be 0`);
    }
  });
});
