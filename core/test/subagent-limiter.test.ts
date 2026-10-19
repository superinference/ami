import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { SubagentLimiter } from '../src/subagent-limiter';

describe('SubagentLimiter', () => {
  let savedMax: string | undefined;
  let savedDepth: string | undefined;

  beforeEach(() => {
    savedMax = process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS;
    savedDepth = process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH;
    delete process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS;
    delete process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH;
  });

  afterEach(() => {
    if (savedMax !== undefined) process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS = savedMax;
    else delete process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS;
    if (savedDepth !== undefined) process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH = savedDepth;
    else delete process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH;
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
