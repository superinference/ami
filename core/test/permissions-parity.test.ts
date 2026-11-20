import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  normalizePermissionMode,
  PERMISSION_MODE_ALIASES,
  PERMISSION_MODE_CYCLE,
  cyclePermissionMode,
  PERMISSION_MODES,
  type PermissionMode,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// Fix 1: normalizePermissionMode accepts Claude Code's `default` wire name and
//        other non-canonical spellings (additive acceptance).
//
// RED (revert fix): drop the alias lookup in normalizePermissionMode (restore
//   `if (m === 'dontAsk') return 'deny-all'; return includes(m) ? m : null;`).
//   `normalizePermissionMode('default')` then returns null → these assertions fail.
// GREEN: with the alias table it returns 'ask'.
// ---------------------------------------------------------------------------
describe('normalizePermissionMode — the reference wire-name + alias parity', () => {
  it("maps Claude Code's `default` mode to AMI's `ask` (not null)", () => {
    // Discriminator: the OLD code returned null here; the fix returns 'ask'.
    assert.equal(normalizePermissionMode('default'), 'ask');
  });

  it('accepts case-insensitive / kebab spellings of the mixed-naming modes', () => {
    assert.equal(normalizePermissionMode('acceptedits'), 'acceptEdits');
    assert.equal(normalizePermissionMode('accept-edits'), 'acceptEdits');
    assert.equal(normalizePermissionMode('bypass-permissions'), 'bypassPermissions');
    assert.equal(normalizePermissionMode('BYPASSPERMISSIONS'), 'bypassPermissions');
    assert.equal(normalizePermissionMode('denyall'), 'deny-all');
    assert.equal(normalizePermissionMode('autoallow'), 'auto-allow');
    assert.equal(normalizePermissionMode('plan-mode'), 'plan');
    assert.equal(normalizePermissionMode('normal'), 'ask');
    assert.equal(normalizePermissionMode('interactive'), 'ask');
  });

  it('trims whitespace before matching aliases', () => {
    assert.equal(normalizePermissionMode('  default  '), 'ask');
  });

  it('still accepts every canonical mode unchanged (no regression)', () => {
    for (const m of PERMISSION_MODES) assert.equal(normalizePermissionMode(m), m);
  });

  it('preserves the legacy dontAsk → deny-all mapping (exact camel spelling)', () => {
    assert.equal(normalizePermissionMode('dontAsk'), 'deny-all');
  });

  it('still rejects genuinely unknown strings (no silent widening)', () => {
    assert.equal(normalizePermissionMode('bogus'), null);
    assert.equal(normalizePermissionMode(''), null);
    assert.equal(normalizePermissionMode('allow-everything'), null);
  });

  it('every alias target is a real canonical mode (table integrity)', () => {
    for (const target of Object.values(PERMISSION_MODE_ALIASES)) {
      assert.ok(
        (PERMISSION_MODES as readonly string[]).includes(target),
        `alias target ${target} must be a canonical mode`,
      );
    }
  });

  it('no alias key collides with a canonical mode name (canonical wins first)', () => {
    for (const key of Object.keys(PERMISSION_MODE_ALIASES)) {
      assert.ok(
        !(PERMISSION_MODES as readonly string[]).includes(key),
        `alias key ${key} must not shadow a canonical mode`,
      );
    }
  });
});

// ---------------------------------------------------------------------------
// Fix 2: cyclePermissionMode — the reference shift-tab mode cycle parity.
//
// RED (revert fix): delete cyclePermissionMode / PERMISSION_MODE_CYCLE. The
//   import fails to resolve and the whole suite errors (red). Alternatively,
//   break the ordering (e.g. return current) and the order assertions fail.
// GREEN: the cycle advances default → acceptEdits → plan → default.
// ---------------------------------------------------------------------------
describe('cyclePermissionMode — interactive mode cycle parity', () => {
  it('cycles forward default → acceptEdits → plan → default (CC shift-tab order)', () => {
    assert.equal(cyclePermissionMode('ask'), 'acceptEdits');
    assert.equal(cyclePermissionMode('acceptEdits'), 'plan');
    assert.equal(cyclePermissionMode('plan'), 'ask');
  });

  it('cycles backward as the exact inverse of forward', () => {
    assert.equal(cyclePermissionMode('ask', 'backward'), 'plan');
    assert.equal(cyclePermissionMode('plan', 'backward'), 'acceptEdits');
    assert.equal(cyclePermissionMode('acceptEdits', 'backward'), 'ask');
  });

  it('forward then backward returns to the starting mode (round-trip)', () => {
    for (const m of PERMISSION_MODE_CYCLE) {
      assert.equal(cyclePermissionMode(cyclePermissionMode(m), 'backward'), m);
    }
  });

  it('excludes the blanket modes from the keypress cycle (safety)', () => {
    // A blanket allow/deny mode must never be reachable by an accidental cycle.
    assert.ok(!PERMISSION_MODE_CYCLE.includes('deny-all' as PermissionMode));
    assert.ok(!PERMISSION_MODE_CYCLE.includes('auto-allow' as PermissionMode));
    assert.ok(!PERMISSION_MODE_CYCLE.includes('bypassPermissions' as PermissionMode));
  });

  it('a blanket mode re-enters the cycle at a RULE-EVALUATED mode, never widens', () => {
    // From a flag-only blanket mode, forward lands on the safe interactive default
    // (ask) and backward on the last cycle entry (plan) — both rule-evaluated, so a
    // user-initiated cycle can never silently flip a blanket deny into a blanket allow.
    assert.equal(cyclePermissionMode('deny-all'), 'ask');
    assert.equal(cyclePermissionMode('auto-allow'), 'ask');
    assert.equal(cyclePermissionMode('bypassPermissions'), 'ask');
    assert.equal(cyclePermissionMode('deny-all', 'backward'), 'plan');
    // the landing modes are never blanket-permissive
    for (const blanket of ['deny-all', 'auto-allow', 'bypassPermissions'] as PermissionMode[]) {
      const fwd = cyclePermissionMode(blanket);
      const bwd = cyclePermissionMode(blanket, 'backward');
      assert.ok(PERMISSION_MODE_CYCLE.includes(fwd));
      assert.ok(PERMISSION_MODE_CYCLE.includes(bwd));
    }
  });

  it('every cycle entry is a canonical mode', () => {
    for (const m of PERMISSION_MODE_CYCLE) {
      assert.ok((PERMISSION_MODES as readonly string[]).includes(m));
    }
  });
});
