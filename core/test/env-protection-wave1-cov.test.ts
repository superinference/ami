import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { EnvProtectionGuard } from '../src/env-protection';

// ---------------------------------------------------------------------------
// Wave 1 coverage: the _maxAttempts (1000) overflow cap on the blocked-attempt
// log. This is the one remaining uncovered branch — the FALSE path of
// `if (this._blockedAttempts.length < this._maxAttempts)` in checkSet.
//
// Security intent: a malicious actor spamming blocked env-set attempts must not
// be able to grow the in-memory log without bound (memory-exhaustion DoS). Once
// the cap is hit, further attempts are still *reported* (attempt object
// returned, so the caller can still block the set) but are NOT stored.
// ---------------------------------------------------------------------------

describe('EnvProtectionGuard — _maxAttempts overflow cap', () => {
  it('stops growing the log at the 1000-attempt cap but keeps reporting', () => {
    const guard = new EnvProtectionGuard();

    // Fill the log exactly to the cap.
    for (let i = 0; i < 1000; i++) {
      const attempt = guard.checkSet('LD_PRELOAD', `src-${i}`);
      assert.ok(attempt !== null);
    }
    assert.equal(guard.getBlockedCount(), 1000);

    // The 1001st blocked attempt: still a pinned var, so the attempt object is
    // returned (caller can still deny the set) ...
    const overflow = guard.checkSet('NODE_OPTIONS', 'overflow-src');
    assert.ok(overflow !== null, 'overflow attempt must still be reported');
    assert.equal(overflow!.variable, 'NODE_OPTIONS');
    assert.equal(overflow!.category, 'code_loading');
    assert.equal(overflow!.source, 'overflow-src');

    // ... but it must NOT be stored — the count is frozen at the cap. If the
    // cap branch were removed, this would read 1001 and fail.
    assert.equal(guard.getBlockedCount(), 1000);

    // The last stored entry is still the pre-overflow LD_PRELOAD, never the
    // dropped NODE_OPTIONS overflow entry.
    const attempts = guard.getBlockedAttempts();
    assert.equal(attempts.length, 1000);
    assert.equal(attempts[999].variable, 'LD_PRELOAD');
    assert.equal(attempts[999].source, 'src-999');

    // Additional overflow attempts also drop silently from storage.
    guard.checkSet('BASH_ENV', 'overflow-2');
    guard.checkSet('PATH', 'overflow-3');
    assert.equal(guard.getBlockedCount(), 1000);
  });

  it('clear() lets the log accept new attempts again after overflow', () => {
    const guard = new EnvProtectionGuard();
    for (let i = 0; i < 1001; i++) guard.checkSet('LD_PRELOAD', `s-${i}`);
    assert.equal(guard.getBlockedCount(), 1000);

    guard.clear();
    assert.equal(guard.getBlockedCount(), 0);

    const attempt = guard.checkSet('DISABLE_TELEMETRY', 'post-clear');
    assert.ok(attempt !== null);
    assert.equal(guard.getBlockedCount(), 1);
    assert.equal(guard.getBlockedAttempts()[0].variable, 'DISABLE_TELEMETRY');
  });

  it('checkAndBlock also respects the overflow cap (goes through checkSet)', () => {
    const guard = new EnvProtectionGuard();
    for (let i = 0; i < 1000; i++) guard.checkAndBlock('LD_PRELOAD', `s-${i}`);
    assert.equal(guard.getBlockedCount(), 1000);

    // Over the cap, the result is still blocked:true (guard still protects),
    // but the attempt is not appended to the log.
    const result = guard.checkAndBlock('NODE_OPTIONS', 'over');
    assert.equal(result.blocked, true);
    assert.equal(result.category, 'code_loading');
    assert.equal(guard.getBlockedCount(), 1000);
  });
});
