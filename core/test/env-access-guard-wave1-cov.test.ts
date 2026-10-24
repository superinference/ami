import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  isSensitiveEnvVar,
  canAccessEnv,
  EnvAccessGuard,
} from '../src/env-access-guard';

// Wave-1 coverage top-up for env-access-guard.
// Targets the uncovered short-circuit branch in canAccessEnv: a get by a
// 'user' caller where readerTier is DEFINED but is neither 'prepend' nor
// 'append'. The existing suite only covers readerTier undefined, 'prepend',
// and 'append', so the `=== 'prepend' || === 'append'` disjunction is never
// exercised with both arms false. These tests also pin the real security
// contract: the org-plugin read block fires ONLY for prepend/append readers,
// and must not over-block other tiers.

describe('canAccessEnv — readerTier neither prepend nor append (||-false arm)', () => {
  // If the guard were broken to block ALL user gets with any readerTier,
  // these would flip to false — so each is red-green meaningful.
  it('allows user get when readerTier is engine (not an org-plugin tier)', () => {
    assert.equal(canAccessEnv('MY_VAR', 'get', 'user', 'engine'), true);
  });

  it('allows user get when readerTier is builtin', () => {
    assert.equal(canAccessEnv('MY_VAR', 'get', 'user', 'builtin'), true);
  });

  it('allows user get when readerTier is also user', () => {
    assert.equal(canAccessEnv('MY_VAR', 'get', 'user', 'user'), true);
  });

  it('blocks ONLY prepend/append readers, not adjacent tiers', () => {
    // Contrast rows prove the disjunction discriminates correctly.
    assert.equal(canAccessEnv('X', 'get', 'user', 'prepend'), false);
    assert.equal(canAccessEnv('X', 'get', 'user', 'append'), false);
    assert.equal(canAccessEnv('X', 'get', 'user', 'engine'), true);
    assert.equal(canAccessEnv('X', 'get', 'user', 'builtin'), true);
  });

  it('does not block non-user callers even with org-plugin readerTier', () => {
    // callerTier !== 'user' short-circuits the final && — both arms of that
    // guard are thus pinned (user=blocked above, non-user=allowed here).
    assert.equal(canAccessEnv('X', 'get', 'append', 'append'), true);
    assert.equal(canAccessEnv('X', 'get', 'builtin', 'prepend'), true);
  });

  it('set/delete by user are never blocked even with org-plugin readerTier', () => {
    // operation !== 'get' short-circuits the first guard entirely.
    assert.equal(canAccessEnv('X', 'set', 'user', 'prepend'), true);
    assert.equal(canAccessEnv('X', 'delete', 'user', 'append'), true);
  });
});

describe('EnvAccessGuard.safeGetEnv — redaction branch arms', () => {
  it('returns the real value (not redacted) for a present non-sensitive var', () => {
    const name = `WAVE1_PLAIN_${process.pid}`;
    const original = process.env[name];
    process.env[name] = 'plain-value-123';
    try {
      const guard = new EnvAccessGuard();
      const result = guard.safeGetEnv(name);
      assert.equal(result.redacted, false);
      assert.equal(result.value, 'plain-value-123');
    } finally {
      if (original === undefined) delete process.env[name];
      else process.env[name] = original;
    }
  });

  it('returns undefined (not redacted) for an absent non-sensitive var', () => {
    const name = `WAVE1_ABSENT_${process.pid}`;
    delete process.env[name];
    const guard = new EnvAccessGuard();
    const result = guard.safeGetEnv(name);
    assert.equal(result.redacted, false);
    assert.equal(result.value, undefined);
  });

  it('redacts a present sensitive var without leaking its value', () => {
    const name = `WAVE1_SECRET_PASSWORD_${process.pid}`;
    const original = process.env[name];
    process.env[name] = 'hunter2-should-not-leak';
    try {
      const guard = new EnvAccessGuard();
      const result = guard.safeGetEnv(name);
      assert.equal(result.redacted, true);
      assert.equal(result.value, '[REDACTED]');
      assert.ok(!String(result.value).includes('hunter2'), 'secret must not leak');
    } finally {
      if (original === undefined) delete process.env[name];
      else process.env[name] = original;
    }
  });
});

describe('isSensitiveEnvVar — substring/positional variants', () => {
  it('matches when sensitive token is a prefix, infix, or suffix', () => {
    assert.equal(isSensitiveEnvVar('API_KEY_ROTATION'), true); // prefix
    assert.equal(isSensitiveEnvVar('X_PASSWORD_Y'), true);     // infix
    assert.equal(isSensitiveEnvVar('OPENAI_API_KEY'), true);   // suffix
  });

  it('matches regardless of case folding', () => {
    assert.equal(isSensitiveEnvVar('github_auth_token'), true);
    assert.equal(isSensitiveEnvVar('CrEdEnTiAl'), true);
  });

  it('does not flag an unrelated var', () => {
    assert.equal(isSensitiveEnvVar('NODE_ENV'), false);
  });
});
