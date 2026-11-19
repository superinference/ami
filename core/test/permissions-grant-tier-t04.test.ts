/**
 * Unit tests for the permission GRANT-TIER classifier (core/core/src/permissions.ts):
 * `permissionGrantTier` + `isSessionOnlyGrant`.
 *
 * These are the single canonical source distinguishing a SESSION-ONLY grant
 * (`allow_session`, Claude Code's `accept-session` / the `s` / confirm:cycleMode
 * keybinding) from a PERSISTED-ALWAYS grant (`allow_pattern`). The tool-executor uses
 * `isSessionOnlyGrant` to decide whether to flush a standing rule to disk, so the
 * mapping below is load-bearing — a regression here silently changes whether a scoped
 * session accept persists to the user's settings file.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { permissionGrantTier, isSessionOnlyGrant } from '../src/permissions';

describe('permissionGrantTier — session-only vs persisted-always', () => {
  // REGRESSION LOCK: if `allow_session` maps to anything but 'session' (e.g.
  // 'always'), the scoped session accept would start persisting grants to disk;
  // if `allow_pattern` stops mapping to 'always', persisted grants stop being saved.
  it('maps each action to its tier', () => {
    assert.equal(permissionGrantTier('allow_once'), 'once');
    assert.equal(permissionGrantTier('allow_session'), 'session');
    assert.equal(permissionGrantTier('allow_pattern'), 'always');
    assert.equal(permissionGrantTier('deny'), 'deny');
    assert.equal(permissionGrantTier('cancelled'), 'cancelled');
  });

  it('session and always are DISTINCT tiers (the whole point of the feature)', () => {
    assert.notEqual(permissionGrantTier('allow_session'), permissionGrantTier('allow_pattern'));
  });
});

describe('isSessionOnlyGrant — the "never persist to disk" discriminator', () => {
  it('is true ONLY for allow_session', () => {
    assert.equal(isSessionOnlyGrant('allow_session'), true);
  });

  it('is false for every other action (persisted / once / deny / cancelled)', () => {
    for (const a of ['allow_once', 'allow_pattern', 'deny', 'cancelled'] as const) {
      assert.equal(isSessionOnlyGrant(a), false, `${a} must not be session-only`);
    }
  });
});
