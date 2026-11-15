import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  canHookEvent,
  isStricterVerdict,
  shouldSkipHookDueToTrust,
  HOOK_TIERS,
  PROTECTED_EVENTS,
  type HookTier,
} from '../src/hook-protection';

// Gap F0170/18255 — fail-safe handling of an unrecognized tier in the protection gate.
// Before the fix canHookEvent used `tier !== 'user'`, so ANY tier string that was not
// literally 'user' (a casted/unknown/malformed value) bypassed the protected-event guard.
describe('canHookEvent — fail-closed on unrecognized tier (18255)', () => {
  it('denies an unknown tier on every protected event (fail-closed, not fail-open)', () => {
    for (const event of PROTECTED_EVENTS) {
      for (const bogus of ['User', 'plugin', 'managed', '', 'USER', 'core ']) {
        assert.equal(
          canHookEvent(event, bogus as HookTier),
          false,
          `unknown tier "${bogus}" must NOT pass the guard for protected event "${event}"`,
        );
      }
    }
  });

  it('still allows every recognized non-user tier on protected events', () => {
    for (const event of PROTECTED_EVENTS) {
      for (const tier of HOOK_TIERS) {
        assert.equal(canHookEvent(event, tier), tier !== 'user');
      }
    }
  });

  it('an unknown tier on a non-protected (passthrough/unknown) event is still allowed', () => {
    // The gate only guards protected events; unknown tiers on unprotected events pass.
    assert.equal(canHookEvent('tool.call', 'bogus' as HookTier), true);
    assert.equal(canHookEvent('custom.event', 'bogus' as HookTier), true);
  });
});

// Gap F0170/8298 — strict rejection of unknown hook verdict values (fail-safe boundary).
describe('isStricterVerdict — throws on unrecognized verdict values (8298)', () => {
  it('throws when the current verdict is unknown', () => {
    assert.throws(() => isStricterVerdict('bogus', 'allow'), /Unknown hook verdict "bogus"/);
  });

  it('throws when the previous verdict is unknown', () => {
    assert.throws(() => isStricterVerdict('deny', 'garbage'), /Unknown hook verdict "garbage"/);
  });

  it('throws on empty-string verdicts', () => {
    assert.throws(() => isStricterVerdict('', 'allow'), /Unknown hook verdict/);
    assert.throws(() => isStricterVerdict('deny', ''), /Unknown hook verdict/);
  });

  it('does NOT silently drop a real deny against a garbage previous value', () => {
    // The old fail-open behavior returned false here (rank[garbage] === undefined),
    // which would let a stricter deny be dropped. Now it refuses to proceed.
    assert.throws(() => isStricterVerdict('deny', 'nonsense'));
  });

  it('still computes the 3-valued matrix correctly for valid verdicts', () => {
    assert.equal(isStricterVerdict('deny', 'ask'), true);
    assert.equal(isStricterVerdict('ask', 'allow'), true);
    assert.equal(isStricterVerdict('deny', 'allow'), true);
    assert.equal(isStricterVerdict('allow', 'deny'), false);
    assert.equal(isStricterVerdict('deny', 'deny'), false);
  });
});

// Gap F0170/8299 — 4-valued precedence lattice with a neutral `passthrough` no-op.
describe('isStricterVerdict — passthrough is a neutral no-op (8299)', () => {
  it('passthrough never overrides a real verdict', () => {
    assert.equal(isStricterVerdict('passthrough', 'allow'), false);
    assert.equal(isStricterVerdict('passthrough', 'ask'), false);
    assert.equal(isStricterVerdict('passthrough', 'deny'), false);
  });

  it('any real verdict overrides a neutral passthrough (nothing set yet)', () => {
    assert.equal(isStricterVerdict('allow', 'passthrough'), true);
    assert.equal(isStricterVerdict('ask', 'passthrough'), true);
    assert.equal(isStricterVerdict('deny', 'passthrough'), true);
  });

  it('passthrough vs passthrough is a no-op (neither stricter)', () => {
    assert.equal(isStricterVerdict('passthrough', 'passthrough'), false);
  });

  it('a strictest-wins merge seeded with passthrough lets the first real verdict take, then only stricter ones replace it', () => {
    let current = 'passthrough';
    for (const incoming of ['passthrough', 'allow', 'passthrough', 'ask', 'allow', 'deny', 'passthrough']) {
      if (isStricterVerdict(incoming, current)) current = incoming;
    }
    assert.equal(current, 'deny');
  });
});

// Gap F0170/956 — workspace-trust gate (already implemented); pin its behavior.
describe('shouldSkipHookDueToTrust — defense-in-depth trust gate (956)', () => {
  it('skips ALL hooks in an interactive session when the workspace is untrusted', () => {
    assert.equal(shouldSkipHookDueToTrust(false, true), true);
  });

  it('runs hooks in an interactive session once the workspace is trusted', () => {
    assert.equal(shouldSkipHookDueToTrust(true, true), false);
  });

  it('never skips in non-interactive / SDK mode (implicit trust), trusted or not', () => {
    assert.equal(shouldSkipHookDueToTrust(false, false), false);
    assert.equal(shouldSkipHookDueToTrust(true, false), false);
  });
});
