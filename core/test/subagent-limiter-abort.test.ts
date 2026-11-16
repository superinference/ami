import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { SubagentLimiter } from '../src/subagent-limiter';

// Coverage for two additive parity capabilities on the concurrent-subagent cap
// enforcer:
//   1. Abort/cancellation of queued (not-yet-started) acquisitions via AbortSignal
//      (Claude threads toolUseContext.abortController.signal through the fan-out).
//   2. Runtime-reconfigurable limits — getters re-read process.env every call so a
//      long-lived limiter honours live env/config changes (Claude re-parses the
//      concurrency cap per batch via getMaxToolUseConcurrency()).

const ENV_KEYS = [
  'CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS',
  'CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH',
  'CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION',
] as const;

function withCleanEnv() {
  const saved: Record<string, string | undefined> = {};
  beforeEach(() => {
    for (const k of ENV_KEYS) {
      saved[k] = process.env[k];
      delete process.env[k];
    }
  });
  afterEach(() => {
    for (const k of ENV_KEYS) {
      if (saved[k] !== undefined) process.env[k] = saved[k];
      else delete process.env[k];
    }
  });
}

describe('SubagentLimiter — abort of queued acquisitions', () => {
  withCleanEnv();

  it('acquire(signal) rejects immediately when the signal is already aborted', async () => {
    const limiter = new SubagentLimiter(2, 0, 200);
    const ac = new AbortController();
    ac.abort();
    await assert.rejects(limiter.acquire(ac.signal), { name: 'AbortError' });
    // No concurrency slot or lifetime budget was consumed by the aborted call.
    assert.equal(limiter.activeCount, 0);
    assert.equal(limiter.lifetimeCount, 0);
    assert.equal(limiter.queuedCount, 0);
  });

  it('a queued waiter aborted while parked rejects and is spliced out, freeing the slot cleanly', async () => {
    const limiter = new SubagentLimiter(1, 0, 200);
    await limiter.acquire(); // fills the only slot (running=1)

    const ac = new AbortController();
    let settled: 'pending' | 'resolved' | 'rejected' = 'pending';
    const queued = limiter.acquire(ac.signal).then(
      () => { settled = 'resolved'; },
      () => { settled = 'rejected'; },
    );
    assert.equal(limiter.queuedCount, 1);

    ac.abort();
    await queued;

    assert.equal(settled, 'rejected');
    // DISCRIMINATOR: abort-blind limiter leaves the entry parked forever.
    assert.equal(limiter.queuedCount, 0, 'aborted waiter must be removed from the queue');
    // The running holder still owns its slot; aborting a queued waiter must not
    // touch it, and release must return it to the pool (not hand it to the dead waiter).
    assert.equal(limiter.activeCount, 1);
    limiter.release();
    assert.equal(limiter.activeCount, 0);
    assert.equal(limiter.availableSlots, 1);
  });

  it('aborting one queued waiter leaves the others to receive slots on release', async () => {
    const limiter = new SubagentLimiter(1, 0, 200);
    await limiter.acquire(); // running=1

    const ac = new AbortController();
    let w2Rejected = false;
    const w2 = limiter.acquire(ac.signal).then(() => {}, () => { w2Rejected = true; });
    let w3Resolved = false;
    const w3 = limiter.acquire().then(() => { w3Resolved = true; });
    assert.equal(limiter.queuedCount, 2);

    ac.abort(); // cancel only the first waiter
    await w2;
    assert.equal(w2Rejected, true);
    assert.equal(limiter.queuedCount, 1, 'only the aborted waiter is removed');

    limiter.release(); // slot must now go to the still-live w3
    await w3;
    assert.equal(w3Resolved, true);
    assert.equal(limiter.queuedCount, 0);
  });

  it('run(fn, signal) aborted while queued never invokes fn and rejects', async () => {
    const limiter = new SubagentLimiter(1, 0, 200);
    await limiter.acquire(); // running=1

    const ac = new AbortController();
    let fnCalled = false;
    const p = limiter.run(async () => { fnCalled = true; return 'x'; }, ac.signal);
    assert.equal(limiter.queuedCount, 1);

    ac.abort();
    await assert.rejects(p, { name: 'AbortError' });
    assert.equal(fnCalled, false, 'fn must not run for an aborted-while-queued acquisition');
    assert.equal(limiter.queuedCount, 0);
    // The queued run never held a slot, so nothing was leaked.
    limiter.release();
    assert.equal(limiter.activeCount, 0);
  });

  it('a non-aborting signal does not interfere with a normal queue handoff', async () => {
    const limiter = new SubagentLimiter(1, 0, 200);
    await limiter.acquire();
    const ac = new AbortController();
    let resolved = false;
    const pending = limiter.acquire(ac.signal).then(() => { resolved = true; });
    assert.equal(limiter.queuedCount, 1);
    limiter.release(); // hands the slot to the waiter normally
    await pending;
    assert.equal(resolved, true);
    // Aborting AFTER the waiter has already been resolved must be a harmless no-op.
    ac.abort();
    assert.equal(limiter.activeCount, 1);
    assert.equal(limiter.queuedCount, 0);
  });

  it('run(fn, signal) with a non-aborting signal executes and releases normally', async () => {
    const limiter = new SubagentLimiter(2, 0, 200);
    const ac = new AbortController();
    const result = await limiter.run(async () => 'ok', ac.signal);
    assert.equal(result, 'ok');
    assert.equal(limiter.activeCount, 0);
  });
});

describe('SubagentLimiter — runtime-reconfigurable limits (getters re-read env)', () => {
  withCleanEnv();

  it('honours a concurrency limit raised at runtime for new acquisitions', async () => {
    process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS = '2';
    const limiter = new SubagentLimiter(); // no override -> env-driven
    assert.equal(limiter.maxConcurrent, 2);
    await limiter.acquire();
    await limiter.acquire();
    assert.equal(limiter.availableSlots, 0);

    // Live reconfiguration: raise the cap without recreating the limiter.
    process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS = '3';
    assert.equal(limiter.maxConcurrent, 3, 'getter must re-read env');
    assert.equal(limiter.availableSlots, 1);
    // DISCRIMINATOR: a frozen-at-construction field would still report 2 and queue.
    await limiter.acquire();
    assert.equal(limiter.activeCount, 3);
    assert.equal(limiter.queuedCount, 0);
  });

  it('honours a lifetime budget lowered at runtime', async () => {
    process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION = '100';
    const limiter = new SubagentLimiter();
    assert.equal(limiter.maxLifetime, 100);
    process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION = '1';
    assert.equal(limiter.maxLifetime, 1);
    await limiter.acquire();
    limiter.release();
    assert.equal(limiter.isLifetimeBudgetExhausted(), true);
  });

  it('honours a spawn depth changed at runtime', () => {
    process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH = '1';
    const limiter = new SubagentLimiter();
    assert.equal(limiter.canSpawnAtDepth(1), true);
    process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH = '3';
    assert.equal(limiter.maxDepth, 3);
    assert.equal(limiter.canSpawnAtDepth(3), true);
  });

  it('explicit constructor overrides take precedence over later env changes', () => {
    const limiter = new SubagentLimiter(3, 2, 50);
    process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS = '99';
    process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH = '99';
    process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION = '99';
    assert.equal(limiter.maxConcurrent, 3);
    assert.equal(limiter.maxDepth, 2);
    assert.equal(limiter.maxLifetime, 50);
  });
});

describe('SubagentLimiter — slot reclamation when lifetime is exhausted via recordSpawn mid-queue', () => {
  withCleanEnv();

  // Confirms the release() drain path reclaims the slot and drains all waiters
  // when the lifetime budget is consumed by recordSpawn() (not acquire) while a
  // caller is parked — the race called out in the "robust slot reclamation" gap.
  it('frees the slot and rejects the parked waiter when recordSpawn exhausts the budget', async () => {
    const limiter = new SubagentLimiter(1, 0, 2); // 1 concurrent, 2 lifetime
    await limiter.acquire(); // task1: running=1, lifetime=1

    let rejected = false;
    const queued = limiter.acquire().then(() => {}, () => { rejected = true; });
    assert.equal(limiter.queuedCount, 1);

    // A sibling spawn tracked lifetime-only pushes totalSpawned to the cap while
    // the waiter is still parked.
    limiter.recordSpawn(); // lifetime=2 (exhausted)
    assert.equal(limiter.isLifetimeBudgetExhausted(), true);

    limiter.release(); // waiter cannot run -> reject AND reclaim the slot
    await queued;

    assert.equal(rejected, true, 'exhausted waiter must reject, not hang');
    assert.equal(limiter.queuedCount, 0);
    assert.equal(limiter.activeCount, 0, 'the freed slot must return to the pool');
    assert.equal(limiter.availableSlots, 1);
  });
});
