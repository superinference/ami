import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { SubagentLimiter } from '../src/subagent-limiter';

describe('SubagentLimiter', () => {
  let savedMax: string | undefined;
  let savedDepth: string | undefined;
  let savedLifetime: string | undefined;

  beforeEach(() => {
    savedMax = process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS;
    savedDepth = process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH;
    savedLifetime = process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION;
    delete process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS;
    delete process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH;
    delete process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION;
  });

  afterEach(() => {
    if (savedMax !== undefined) process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS = savedMax;
    else delete process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS;
    if (savedDepth !== undefined) process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH = savedDepth;
    else delete process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH;
    if (savedLifetime !== undefined) process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION = savedLifetime;
    else delete process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION;
  });

  it('defaults to maxConcurrent=20', () => {
    const limiter = new SubagentLimiter();
    assert.equal(limiter.maxConcurrent, 20);
  });

  it('accepts explicit maxConcurrent', () => {
    const limiter = new SubagentLimiter(5);
    assert.equal(limiter.maxConcurrent, 5);
  });

  it('reads maxConcurrent from env var', () => {
    process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS = '10';
    const limiter = new SubagentLimiter();
    assert.equal(limiter.maxConcurrent, 10);
  });

  it('explicit maxConcurrent overrides env var', () => {
    process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS = '10';
    const limiter = new SubagentLimiter(3);
    assert.equal(limiter.maxConcurrent, 3);
  });

  it('starts with zero active and queued', () => {
    const limiter = new SubagentLimiter(5);
    assert.equal(limiter.activeCount, 0);
    assert.equal(limiter.queuedCount, 0);
  });

  it('acquire increments activeCount', async () => {
    const limiter = new SubagentLimiter(5);
    await limiter.acquire();
    assert.equal(limiter.activeCount, 1);
    await limiter.acquire();
    assert.equal(limiter.activeCount, 2);
  });

  it('release decrements activeCount', async () => {
    const limiter = new SubagentLimiter(5);
    await limiter.acquire();
    await limiter.acquire();
    limiter.release();
    assert.equal(limiter.activeCount, 1);
  });

  it('release does not go below zero', () => {
    const limiter = new SubagentLimiter(5);
    limiter.release();
    assert.equal(limiter.activeCount, 0);
  });

  it('queues excess acquire requests', async () => {
    const limiter = new SubagentLimiter(2);
    await limiter.acquire();
    await limiter.acquire();
    assert.equal(limiter.activeCount, 2);

    let resolved = false;
    const pending = limiter.acquire().then(() => { resolved = true; });
    assert.equal(limiter.queuedCount, 1);
    assert.equal(resolved, false);

    limiter.release();
    await pending;
    assert.equal(resolved, true);
    assert.equal(limiter.queuedCount, 0);
    assert.equal(limiter.activeCount, 2);
  });

  it('run() acquires and releases on success', async () => {
    const limiter = new SubagentLimiter(2);
    const result = await limiter.run(async () => 'ok');
    assert.equal(result, 'ok');
    assert.equal(limiter.activeCount, 0);
  });

  it('run() releases on error', async () => {
    const limiter = new SubagentLimiter(2);
    await assert.rejects(
      limiter.run(async () => { throw new Error('boom'); }),
      { message: 'boom' }
    );
    assert.equal(limiter.activeCount, 0);
  });

  it('run() respects concurrency limit', async () => {
    const limiter = new SubagentLimiter(2);
    let maxConcurrent = 0;
    let current = 0;

    const task = async () => {
      current++;
      if (current > maxConcurrent) maxConcurrent = current;
      await new Promise(r => setTimeout(r, 10));
      current--;
    };

    await Promise.all([
      limiter.run(task),
      limiter.run(task),
      limiter.run(task),
      limiter.run(task),
    ]);

    assert.equal(maxConcurrent, 2);
    assert.equal(limiter.activeCount, 0);
  });

  it('reset clears all state', async () => {
    const limiter = new SubagentLimiter(2);
    await limiter.acquire();
    await limiter.acquire();
    limiter.reset();
    assert.equal(limiter.activeCount, 0);
    assert.equal(limiter.queuedCount, 0);
  });
});

describe('SubagentLimiter — depth control', () => {
  let savedDepth: string | undefined;

  beforeEach(() => {
    savedDepth = process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH;
    delete process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH;
  });

  afterEach(() => {
    if (savedDepth !== undefined) process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH = savedDepth;
    else delete process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH;
  });

  it('defaults to maxDepth=0 (no nested subagents)', () => {
    const limiter = new SubagentLimiter();
    assert.equal(limiter.maxDepth, 0);
  });

  it('canSpawnAtDepth allows depth 0', () => {
    const limiter = new SubagentLimiter();
    assert.equal(limiter.canSpawnAtDepth(0), true);
  });

  it('canSpawnAtDepth blocks depth 1 by default', () => {
    const limiter = new SubagentLimiter();
    assert.equal(limiter.canSpawnAtDepth(1), false);
  });

  it('accepts explicit maxDepth', () => {
    const limiter = new SubagentLimiter(20, 3);
    assert.equal(limiter.maxDepth, 3);
    assert.equal(limiter.canSpawnAtDepth(3), true);
    assert.equal(limiter.canSpawnAtDepth(4), false);
  });

  it('reads maxDepth from env var', () => {
    process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH = '2';
    const limiter = new SubagentLimiter();
    assert.equal(limiter.maxDepth, 2);
    assert.equal(limiter.canSpawnAtDepth(2), true);
    assert.equal(limiter.canSpawnAtDepth(3), false);
  });
});

describe('SubagentLimiter — lifetime budget', () => {
  let savedLifetime: string | undefined;

  beforeEach(() => {
    savedLifetime = process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION;
    delete process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION;
  });

  afterEach(() => {
    if (savedLifetime !== undefined) process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION = savedLifetime;
    else delete process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION;
  });

  it('defaults to maxLifetime=200', () => {
    const limiter = new SubagentLimiter();
    assert.equal(limiter.maxLifetime, 200);
  });

  it('accepts explicit maxLifetime', () => {
    const limiter = new SubagentLimiter(20, 0, 50);
    assert.equal(limiter.maxLifetime, 50);
  });

  it('reads maxLifetime from env var', () => {
    process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION = '100';
    const limiter = new SubagentLimiter();
    assert.equal(limiter.maxLifetime, 100);
  });

  it('lifetimeCount starts at zero', () => {
    const limiter = new SubagentLimiter();
    assert.equal(limiter.lifetimeCount, 0);
  });

  it('acquire increments lifetimeCount', async () => {
    const limiter = new SubagentLimiter(20, 0, 200);
    await limiter.acquire();
    assert.equal(limiter.lifetimeCount, 1);
    await limiter.acquire();
    assert.equal(limiter.lifetimeCount, 2);
    limiter.release();
    limiter.release();
  });

  it('lifetimeCount is not decremented by release', async () => {
    const limiter = new SubagentLimiter(20, 0, 200);
    await limiter.acquire();
    await limiter.acquire();
    limiter.release();
    limiter.release();
    assert.equal(limiter.lifetimeCount, 2);
    assert.equal(limiter.activeCount, 0);
  });

  it('isLifetimeBudgetExhausted returns false when under limit', async () => {
    const limiter = new SubagentLimiter(20, 0, 3);
    assert.equal(limiter.isLifetimeBudgetExhausted(), false);
    await limiter.acquire();
    limiter.release();
    assert.equal(limiter.isLifetimeBudgetExhausted(), false);
  });

  it('isLifetimeBudgetExhausted returns true when at limit', async () => {
    const limiter = new SubagentLimiter(20, 0, 2);
    await limiter.acquire();
    limiter.release();
    await limiter.acquire();
    limiter.release();
    assert.equal(limiter.isLifetimeBudgetExhausted(), true);
  });

  it('acquire throws when lifetime budget is exhausted', async () => {
    const limiter = new SubagentLimiter(20, 0, 2);
    await limiter.acquire();
    limiter.release();
    await limiter.acquire();
    limiter.release();
    await assert.rejects(
      limiter.acquire(),
      { message: /Session subagent limit reached/ },
    );
  });

  it('run() respects lifetime budget', async () => {
    const limiter = new SubagentLimiter(20, 0, 2);
    await limiter.run(async () => 'first');
    await limiter.run(async () => 'second');
    await assert.rejects(
      limiter.run(async () => 'third'),
      { message: /Session subagent limit reached/ },
    );
  });

  it('reset clears lifetime count', async () => {
    const limiter = new SubagentLimiter(20, 0, 2);
    await limiter.acquire();
    limiter.release();
    await limiter.acquire();
    limiter.release();
    assert.equal(limiter.isLifetimeBudgetExhausted(), true);
    limiter.reset();
    assert.equal(limiter.lifetimeCount, 0);
    assert.equal(limiter.isLifetimeBudgetExhausted(), false);
    await limiter.acquire();
    limiter.release();
  });

  it('queued acquires also check lifetime budget', async () => {
    const limiter = new SubagentLimiter(1, 0, 2);
    await limiter.acquire();
    assert.equal(limiter.lifetimeCount, 1);

    const second = limiter.acquire();
    assert.equal(limiter.queuedCount, 1);

    limiter.release();
    await second;
    assert.equal(limiter.lifetimeCount, 2);
    limiter.release();

    await assert.rejects(
      limiter.acquire(),
      { message: /Session subagent limit reached/ },
    );
  });
});
