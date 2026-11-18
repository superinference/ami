/**
 * Unit + integration tests for the speculative bash-classifier (the interactive
 * ASK-path grace-race optimization) and its PermissionManager wiring.
 *
 * Parity target: Claude's speculative classifier (bashPermissions.ts peek/consume
 * + the Promise.race grace window in useCanUseTool.tsx). A high-confidence match
 * that settles inside the grace window turns a would-be prompt into an allow; a
 * miss / low-confidence / timeout / rejection falls through to the prompt.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  SpeculativeClassifierCache,
  raceWithGracePeriod,
  isHighConfidenceMatch,
  DEFAULT_SPECULATIVE_GRACE_MS,
  DEFAULT_SPECULATIVE_TTL_MS,
  type SpeculativeClassifierResult,
} from '../src/speculative-classifier';
import { PermissionManager } from '../src/permissions';

const HIGH: SpeculativeClassifierResult = { matches: true, confidence: 'high', matchedDescription: 'git status is read-only' };
const LOW: SpeculativeClassifierResult = { matches: true, confidence: 'low', matchedDescription: 'maybe safe' };
const MISS: SpeculativeClassifierResult = { matches: false, confidence: 'high' };

const never = <T>(): Promise<T> => new Promise<T>(() => {});

describe('isHighConfidenceMatch', () => {
  it('is true only for a definitive high-confidence match', () => {
    assert.equal(isHighConfidenceMatch(HIGH), true);
  });
  it('is false for a low-confidence match', () => {
    assert.equal(isHighConfidenceMatch(LOW), false);
  });
  it('is false for a high-confidence non-match', () => {
    assert.equal(isHighConfidenceMatch(MISS), false);
  });
  it('is false for null/undefined', () => {
    assert.equal(isHighConfidenceMatch(null), false);
    assert.equal(isHighConfidenceMatch(undefined), false);
  });
});

describe('raceWithGracePeriod', () => {
  it('resolves with the value when the promise settles first', async () => {
    const outcome = await raceWithGracePeriod(Promise.resolve(HIGH), 1000);
    assert.deepEqual(outcome, { type: 'result', result: HIGH });
  });

  it('resolves to a timeout when the grace window elapses first', async () => {
    const outcome = await raceWithGracePeriod(never<SpeculativeClassifierResult>(), 5);
    assert.deepEqual(outcome, { type: 'timeout' });
  });

  it('clamps a negative grace to 0 and still times out', async () => {
    const outcome = await raceWithGracePeriod(never<number>(), -100);
    assert.deepEqual(outcome, { type: 'timeout' });
  });

  it('propagates a rejection of the raced promise', async () => {
    await assert.rejects(
      raceWithGracePeriod(Promise.reject(new Error('boom')), 1000),
      /boom/,
    );
  });
});

describe('SpeculativeClassifierCache', () => {
  it('primes one in-flight check per command (dedup)', () => {
    let calls = 0;
    const cache = new SpeculativeClassifierCache({
      classifier: async (cmd) => { calls++; void cmd; return HIGH; },
    });
    cache.prime('git status');
    cache.prime('git status');
    assert.equal(calls, 1, 'second prime reuses the live check');
    assert.equal(cache.size(), 1);
    cache.prime('git log');
    assert.equal(calls, 2);
    assert.equal(cache.size(), 2);
  });

  it('peek returns the primed promise and consume drops it', async () => {
    const cache = new SpeculativeClassifierCache({ classifier: async () => HIGH });
    cache.prime('ls');
    const p = cache.peek('ls');
    assert.ok(p, 'peek returns the primed promise');
    assert.equal(await p, HIGH);
    cache.consume('ls');
    assert.equal(cache.peek('ls'), undefined);
    assert.equal(cache.size(), 0);
  });

  it('peek returns undefined for a command that was never primed', () => {
    const cache = new SpeculativeClassifierCache({ classifier: async () => HIGH });
    assert.equal(cache.peek('nope'), undefined);
  });

  it('clear() drops every cached check', () => {
    const cache = new SpeculativeClassifierCache({ classifier: async () => HIGH });
    cache.prime('a');
    cache.prime('b');
    assert.equal(cache.size(), 2);
    cache.clear();
    assert.equal(cache.size(), 0);
  });

  it('expires a primed-but-unconsumed entry after its TTL', () => {
    let clock = 1000;
    const cache = new SpeculativeClassifierCache({
      classifier: async () => HIGH,
      ttlMs: 100,
      now: () => clock,
    });
    cache.prime('slow');
    assert.equal(cache.size(), 1);
    clock += 101; // past the TTL
    assert.equal(cache.peek('slow'), undefined, 'expired entry is purged on peek');
    assert.equal(cache.size(), 0);
  });

  it('uses the default TTL when none is supplied', () => {
    // A primed entry is still live well within the (large) default TTL window.
    const cache = new SpeculativeClassifierCache({ classifier: async () => HIGH });
    assert.ok(DEFAULT_SPECULATIVE_TTL_MS > 0);
    cache.prime('x');
    assert.equal(cache.size(), 1);
  });

  describe('resolveApproval', () => {
    it('auto-approves (returns the description, consumes) on a high-confidence match', async () => {
      const cache = new SpeculativeClassifierCache({ classifier: async () => HIGH });
      cache.prime('git status');
      const reason = await cache.resolveApproval('git status', 1000);
      assert.equal(reason, 'git status is read-only');
      assert.equal(cache.peek('git status'), undefined, 'consumed after auto-approve');
    });

    it('returns "" (still auto-approves) for a high-confidence match with no description', async () => {
      const cache = new SpeculativeClassifierCache({
        classifier: async () => ({ matches: true, confidence: 'high' }),
      });
      cache.prime('pwd');
      const reason = await cache.resolveApproval('pwd', 1000);
      assert.equal(reason, '', 'empty description is preserved, not coerced to null');
    });

    it('returns null (fall through) when the command was never primed', async () => {
      const cache = new SpeculativeClassifierCache({ classifier: async () => HIGH });
      assert.equal(await cache.resolveApproval('unprimed', 1000), null);
    });

    it('returns null and does NOT consume on a low-confidence match', async () => {
      const cache = new SpeculativeClassifierCache({ classifier: async () => LOW });
      cache.prime('rm x');
      assert.equal(await cache.resolveApproval('rm x', 1000), null);
      assert.ok(cache.peek('rm x'), 'low-confidence result is left to expire, not consumed');
    });

    it('returns null on a high-confidence non-match', async () => {
      const cache = new SpeculativeClassifierCache({ classifier: async () => MISS });
      cache.prime('curl evil');
      assert.equal(await cache.resolveApproval('curl evil', 1000), null);
    });

    it('returns null when the grace window elapses before the check settles', async () => {
      const cache = new SpeculativeClassifierCache({ classifier: () => never<SpeculativeClassifierResult>() });
      cache.prime('slow-cmd');
      assert.equal(await cache.resolveApproval('slow-cmd', 5), null);
    });

    it('returns null when the classifier rejects within the grace window', async () => {
      const cache = new SpeculativeClassifierCache({ classifier: async () => { throw new Error('classifier down'); } });
      cache.prime('boom');
      assert.equal(await cache.resolveApproval('boom', 1000), null);
    });

    it('uses the default grace period when none is passed', async () => {
      assert.equal(DEFAULT_SPECULATIVE_GRACE_MS, 200);
      const cache = new SpeculativeClassifierCache({ classifier: async () => HIGH });
      cache.prime('echo hi');
      const reason = await cache.resolveApproval('echo hi');
      assert.equal(reason, 'git status is read-only');
    });
  });
});

describe('PermissionManager speculative bash classifier wiring', () => {
  it('is inert by default: no classifier installed, resolve returns null', async () => {
    const pm = new PermissionManager('ask');
    assert.equal(pm.hasSpeculativeBashClassifier(), false);
    pm.primeSpeculativeBashCheck('git status'); // no-op, must not throw
    assert.equal(await pm.resolveSpeculativeBashApproval('git status'), null);
    pm.clearSpeculativeBashChecks(); // no-op, must not throw
  });

  it('prime + resolve auto-approves a high-confidence command once installed', async () => {
    const pm = new PermissionManager('ask');
    let seen = '';
    pm.setSpeculativeBashClassifier(async (cmd) => {
      seen = cmd;
      return { matches: true, confidence: 'high', matchedDescription: 'allowlisted: git status' };
    });
    assert.equal(pm.hasSpeculativeBashClassifier(), true);
    pm.primeSpeculativeBashCheck('git status');
    const reason = await pm.resolveSpeculativeBashApproval('git status', 1000);
    assert.equal(reason, 'allowlisted: git status');
    assert.equal(seen, 'git status', 'classifier received the command');
    // Consumed — a second resolve (not re-primed) falls through.
    assert.equal(await pm.resolveSpeculativeBashApproval('git status', 1000), null);
  });

  it('falls through (null) for a low-confidence command', async () => {
    const pm = new PermissionManager('ask');
    pm.setSpeculativeBashClassifier(async () => ({ matches: true, confidence: 'low' }));
    pm.primeSpeculativeBashCheck('rm -rf build');
    assert.equal(await pm.resolveSpeculativeBashApproval('rm -rf build', 1000), null);
  });

  it('clearing the classifier (undefined) makes it inert again', async () => {
    const pm = new PermissionManager('ask');
    pm.setSpeculativeBashClassifier(async () => HIGH);
    pm.primeSpeculativeBashCheck('ls');
    pm.setSpeculativeBashClassifier(undefined);
    assert.equal(pm.hasSpeculativeBashClassifier(), false);
    assert.equal(await pm.resolveSpeculativeBashApproval('ls', 1000), null);
  });

  it('clearSpeculativeBashChecks drops primed entries so they no longer auto-approve', async () => {
    const pm = new PermissionManager('ask');
    pm.setSpeculativeBashClassifier(async () => HIGH);
    pm.primeSpeculativeBashCheck('git status');
    pm.clearSpeculativeBashChecks();
    assert.equal(await pm.resolveSpeculativeBashApproval('git status', 1000), null);
  });

  it('honors a custom TTL on the installed cache', async () => {
    const pm = new PermissionManager('ask');
    pm.setSpeculativeBashClassifier(async () => HIGH, { ttlMs: 0 });
    pm.primeSpeculativeBashCheck('git status');
    // ttlMs:0 → the entry expires immediately (expiresAt <= now), so nothing to resolve.
    assert.equal(await pm.resolveSpeculativeBashApproval('git status', 1000), null);
  });
});
