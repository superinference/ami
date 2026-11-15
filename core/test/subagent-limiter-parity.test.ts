import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { SubagentLimiter } from '../src/subagent-limiter';

// Parity/hardening tests for the concurrent-subagent cap enforcer. Each block
// documents the real gap it closes and is RED-GREEN validated against the
// pre-fix implementation (see the lane report for the per-fix revert results).

describe('SubagentLimiter — parity: queue-drain slot leak on lifetime exhaustion', () => {
  const saved: Record<string, string | undefined> = {};
  beforeEach(() => {
    for (const k of [
      'CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS',
      'CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH',
      'CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION',
    ]) {
      saved[k] = process.env[k];
      delete process.env[k];
    }
  });
  afterEach(() => {
    for (const [k, v] of Object.entries(saved)) {
      if (v !== undefined) process.env[k] = v;
      else delete process.env[k];
    }
  });

  // GAP A: when a queued waiter is rejected at drain time because the lifetime
  // budget is exhausted, the freed concurrency slot must be returned to the pool
  // (activeCount -> 0) AND remaining waiters must not hang. The pre-fix release()
  // left `running` pinned (leak) and stopped draining (hang).
  it('frees the slot and rejects remaining waiters when lifetime runs out mid-queue', async () => {
    const limiter = new SubagentLimiter(1, 0, 2); // 1 concurrent, 2 lifetime
    await limiter.acquire(); // task1: running=1, lifetime=1

    // Two waiters queued behind the single slot.
    const w2 = limiter.acquire();
    let w3Rejected = false;
    const w3 = limiter.acquire().then(
      () => { /* unexpected resolve */ },
      () => { w3Rejected = true; },
    );
    assert.equal(limiter.queuedCount, 2);

    limiter.release(); // task1 done -> hands slot to w2 (lifetime now 2)
    await w2;
    assert.equal(limiter.lifetimeCount, 2);

    limiter.release(); // task2 done -> w3 cannot run (lifetime exhausted)
    await w3;

    assert.equal(w3Rejected, true, 'exhausted waiter must reject, not hang');
    assert.equal(limiter.queuedCount, 0, 'queue must be fully drained');
    // DISCRIMINATOR: pre-fix left running=1 (leaked slot); fixed returns it.
    assert.equal(limiter.activeCount, 0, 'freed slot must return to the pool');
    assert.equal(limiter.availableSlots, 1);
  });

  // GAP A (multi-waiter drain): several waiters queued while under lifetime; once
  // the budget is consumed mid-drain, the remaining ones all reject (none hang)
  // and the slot is returned exactly once.
  it('drains every remaining waiter when the budget is consumed partway through', async () => {
    const limiter = new SubagentLimiter(1, 0, 2); // 1 concurrent, 2 lifetime
    await limiter.acquire(); // task1: running=1, lifetime=1 (queuing still allowed)

    let resolvedCount = 0;
    let rejectedCount = 0;
    const settle = (p: Promise<void>) => p.then(
      () => { resolvedCount++; },
      () => { rejectedCount++; },
    );
    const w2 = settle(limiter.acquire());
    const w3 = settle(limiter.acquire());
    const w4 = settle(limiter.acquire());
    assert.equal(limiter.queuedCount, 3);

    limiter.release(); // -> w2 runs (lifetime=2)
    limiter.release(); // w2 done -> w3,w4 exhausted, both rejected, slot freed
    await Promise.all([w2, w3, w4]);

    assert.equal(resolvedCount, 1, 'exactly one waiter got the budgeted slot');
    assert.equal(rejectedCount, 2, 'exhausted waiters reject rather than hang');
    assert.equal(limiter.queuedCount, 0);
    assert.equal(limiter.activeCount, 0, 'slot returns to pool exactly once');
  });
});

describe('SubagentLimiter — parity: reset must not strand queued acquisitions', () => {
  // GAP B: reset() previously truncated the wait queue in place, leaving every
  // pending `await acquire()` unsettled forever. It must reject them.
  it('rejects pending queued acquisitions on reset instead of hanging them', async () => {
    const limiter = new SubagentLimiter(1, 0, 200);
    await limiter.acquire(); // fills the only slot

    let settled: 'pending' | 'resolved' | 'rejected' = 'pending';
    let msg = '';
    const queued = limiter.acquire().then(
      () => { settled = 'resolved'; },
      (e: Error) => { settled = 'rejected'; msg = e.message; },
    );
    assert.equal(limiter.queuedCount, 1);

    limiter.reset();
    await queued; // must settle (pre-fix: never settles)

    // DISCRIMINATOR: pre-fix leaves settled==='pending'; fixed rejects.
    assert.equal(settled, 'rejected');
    assert.match(msg, /reset/i);
    assert.equal(limiter.queuedCount, 0);
    assert.equal(limiter.activeCount, 0);
    assert.equal(limiter.lifetimeCount, 0);
  });

  it('reset with no queued waiters is a no-op reject-wise and clears counters', async () => {
    const limiter = new SubagentLimiter(2, 0, 5);
    await limiter.acquire();
    await limiter.acquire();
    limiter.reset();
    assert.equal(limiter.activeCount, 0);
    assert.equal(limiter.queuedCount, 0);
    assert.equal(limiter.lifetimeCount, 0);
    // usable again after reset
    assert.equal(limiter.tryAcquire(), true);
  });
});

describe('SubagentLimiter — parity: tryAcquire (non-blocking over-limit rejection)', () => {
  // GAP C: additive non-blocking acquire that surfaces over-limit rejection
  // without queueing.
  it('reserves a slot and returns true when capacity is available', () => {
    const limiter = new SubagentLimiter(2, 0, 200);
    assert.equal(limiter.tryAcquire(), true);
    assert.equal(limiter.activeCount, 1);
    assert.equal(limiter.lifetimeCount, 1);
    assert.equal(limiter.tryAcquire(), true);
    assert.equal(limiter.activeCount, 2);
  });

  it('returns false WITHOUT queueing when at the concurrency cap', () => {
    const limiter = new SubagentLimiter(1, 0, 200);
    assert.equal(limiter.tryAcquire(), true);
    assert.equal(limiter.tryAcquire(), false);
    assert.equal(limiter.queuedCount, 0, 'tryAcquire must never queue');
    assert.equal(limiter.activeCount, 1);
    // a real slot can still be released and re-taken
    limiter.release();
    assert.equal(limiter.tryAcquire(), true);
  });

  it('returns false when the lifetime budget is exhausted', () => {
    const limiter = new SubagentLimiter(5, 0, 1);
    assert.equal(limiter.tryAcquire(), true); // lifetime=1
    limiter.release();
    assert.equal(limiter.isLifetimeBudgetExhausted(), true);
    assert.equal(limiter.tryAcquire(), false);
    assert.equal(limiter.activeCount, 0);
  });
});

describe('SubagentLimiter — parity: availableSlots introspection', () => {
  it('reports free slots and never goes negative', async () => {
    const limiter = new SubagentLimiter(3, 0, 200);
    assert.equal(limiter.availableSlots, 3);
    await limiter.acquire();
    await limiter.acquire();
    assert.equal(limiter.availableSlots, 1);
    await limiter.acquire();
    assert.equal(limiter.availableSlots, 0);
    limiter.release();
    assert.equal(limiter.availableSlots, 1);
  });
});

describe('SubagentLimiter — parity: existing behavior preserved', () => {
  it('normal queue handoff still works (no lifetime pressure)', async () => {
    const limiter = new SubagentLimiter(1, 0, 200);
    await limiter.acquire();
    let resolved = false;
    const pending = limiter.acquire().then(() => { resolved = true; });
    assert.equal(limiter.queuedCount, 1);
    limiter.release();
    await pending;
    assert.equal(resolved, true);
    assert.equal(limiter.activeCount, 1);
    assert.equal(limiter.queuedCount, 0);
  });

  it('release below zero is still clamped', () => {
    const limiter = new SubagentLimiter(2, 0, 200);
    limiter.release();
    assert.equal(limiter.activeCount, 0);
    assert.equal(limiter.availableSlots, 2);
  });
});
