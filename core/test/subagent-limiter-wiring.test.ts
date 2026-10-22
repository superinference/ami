import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { SubagentLimiter } from '../src/subagent-limiter';
import { taskTool } from '../src/tools/task';
import type { ToolContext } from '../src/types';

describe('SubagentLimiter.recordSpawn (lifetime-only increment)', () => {
  let saved: string | undefined;
  beforeEach(() => {
    saved = process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION;
    delete process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION;
  });
  afterEach(() => {
    if (saved !== undefined) process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION = saved;
    else delete process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION;
  });

  it('increments lifetimeCount without touching concurrency', () => {
    const limiter = new SubagentLimiter(20, 0, 2);
    assert.equal(limiter.lifetimeCount, 0);
    limiter.recordSpawn();
    assert.equal(limiter.lifetimeCount, 1);
    assert.equal(limiter.activeCount, 0);
    assert.equal(limiter.isLifetimeBudgetExhausted(), false);
    limiter.recordSpawn();
    assert.equal(limiter.lifetimeCount, 2);
    assert.equal(limiter.activeCount, 0);
    assert.equal(limiter.isLifetimeBudgetExhausted(), true);
  });
});

describe('task tool honors ToolContext.subagentLimiter lifetime budget', () => {
  function makeContext(limiter: SubagentLimiter): ToolContext {
    return {
      cwd: process.cwd(),
      abortSignal: new AbortController().signal,
      subagentLimiter: limiter,
    } as ToolContext;
  }

  it('blocks a spawn when the lifetime budget is exhausted', async () => {
    const limiter = new SubagentLimiter(20, 0, 1);
    limiter.recordSpawn(); // exhaust the single-spawn lifetime budget
    assert.equal(limiter.isLifetimeBudgetExhausted(), true);

    const result = await taskTool.execute({ prompt: 'investigate something' }, makeContext(limiter));

    assert.equal(result.isError, true);
    // Discriminating assertion: without the wiring, execute falls through to the
    // "engine factory not available" error instead of the lifetime-cap message.
    assert.match(result.output, /Session subagent limit reached \(1\)/);
    assert.match(result.output, /CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION/);
  });

  it('does not block when the lifetime budget still has room', async () => {
    const limiter = new SubagentLimiter(20, 0, 5);
    limiter.recordSpawn(); // 1 of 5 used — still under budget

    const result = await taskTool.execute({ prompt: 'investigate something' }, makeContext(limiter));

    // No engine factory in this bare context, so the spawn path stops there —
    // proving the lifetime check did NOT short-circuit the call.
    assert.equal(result.isError, true);
    assert.match(result.output, /engine factory not available/);
  });
});
