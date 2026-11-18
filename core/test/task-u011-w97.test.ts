import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  reserveAgentSlot,
  checkAgentCaps,
  taskTool,
  resetAgentRegistry,
  DEFAULT_MAX_AGENTS_PER_SESSION,
  DEFAULT_MAX_CONCURRENT_AGENTS,
} from '../src/tools/task';
import type { ToolContext } from '../src/types';

/**
 * U011 / w97 — atomic agent-spawn slot reservation (reserveAgentSlot).
 *
 * Parity target: Claude Code's serialized teammate-spawn critical section
 * (swarm/backends TmuxBackend `paneCreationLock` / ITermBackend
 * `acquirePaneCreationLock`). AMI spawns sub-engines in-process, so the shared
 * state to protect against concurrent mutation is the spawn counter, not a pane
 * split target. reserveAgentSlot does the cap check + the counter increment as ONE
 * synchronous step, closing the TOCTOU overshoot where two parallel `task` calls
 * both pass the early checkAgentCaps and both spawn.
 */
describe('reserveAgentSlot — happy path', () => {
  it('reserves (increments) and returns null when under the session cap', () => {
    const ctx = { _agentSpawnCount: { value: 5 } };
    assert.equal(reserveAgentSlot(ctx), null);
    assert.equal(ctx._agentSpawnCount.value, 6); // slot consumed
  });

  it('creates the counter when none exists and consumes a slot', () => {
    const ctx: { _agentSpawnCount?: { value: number } } = {};
    assert.equal(reserveAgentSlot(ctx), null);
    assert.equal(ctx._agentSpawnCount?.value, 1);
  });
});

describe('reserveAgentSlot — session cap', () => {
  it('rejects at the session cap and does NOT consume a slot', () => {
    const ctx = { _agentSpawnCount: { value: DEFAULT_MAX_AGENTS_PER_SESSION } };
    const err = reserveAgentSlot(ctx);
    assert.ok(err !== null);
    assert.ok(err!.includes('session cap'));
    // The counter is untouched when the reservation is refused.
    assert.equal(ctx._agentSpawnCount.value, DEFAULT_MAX_AGENTS_PER_SESSION);
  });

  it('honors a custom session cap', () => {
    const ctx = { _agentSpawnCount: { value: 2 }, _maxAgentsPerSession: 3 };
    assert.equal(reserveAgentSlot(ctx), null); // -> 3
    assert.equal(ctx._agentSpawnCount.value, 3);
    const err = reserveAgentSlot(ctx); // at cap now
    assert.ok(err !== null);
    assert.ok(err!.includes('3'));
    assert.equal(ctx._agentSpawnCount.value, 3); // not incremented past the cap
  });
});

describe('reserveAgentSlot — concurrent cap', () => {
  function makeProcessManager(runningCount: number) {
    return {
      runningAgentCount() {
        return runningCount;
      },
    } as unknown as import('../src/process-manager').ProcessManager;
  }

  it('rejects at the concurrent cap without consuming a session slot', () => {
    const ctx = {
      _agentSpawnCount: { value: 0 },
      processManager: makeProcessManager(DEFAULT_MAX_CONCURRENT_AGENTS),
    };
    const err = reserveAgentSlot(ctx);
    assert.ok(err !== null);
    assert.ok(err!.includes('Concurrent'));
    assert.equal(ctx._agentSpawnCount.value, 0);
  });

  it('reserves when under the concurrent cap', () => {
    const ctx = {
      _agentSpawnCount: { value: 0 },
      processManager: makeProcessManager(DEFAULT_MAX_CONCURRENT_AGENTS - 1),
    };
    assert.equal(reserveAgentSlot(ctx), null);
    assert.equal(ctx._agentSpawnCount.value, 1);
  });
});

describe('reserveAgentSlot — closes the TOCTOU overshoot', () => {
  it('two reservations at the boundary: first succeeds, second is refused', () => {
    // One slot left before the cap.
    const cap = 10;
    const ctx = { _agentSpawnCount: { value: cap - 1 }, _maxAgentsPerSession: cap };

    const first = reserveAgentSlot(ctx);
    const second = reserveAgentSlot(ctx);

    assert.equal(first, null); // consumed the last slot (-> cap)
    assert.ok(second !== null); // the second cannot overshoot
    assert.ok(second!.includes('session cap'));
    // Exactly the cap was reserved — never cap + 1.
    assert.equal(ctx._agentSpawnCount.value, cap);
  });

  it('the bare check-then-act pattern it replaces WOULD overshoot', () => {
    // Demonstrates the race reserveAgentSlot closes: if a caller checks the cap and
    // only later increments (as the task handler did across its awaits), two parallel
    // spawns both observe "under cap" and both commit — cap + 1.
    const cap = 10;
    const ctx = { _agentSpawnCount: { value: cap - 1 }, _maxAgentsPerSession: cap };

    // Both calls check BEFORE either increments (the pre-fix interleaving).
    const checkA = checkAgentCaps(ctx);
    const checkB = checkAgentCaps(ctx);
    assert.equal(checkA, null);
    assert.equal(checkB, null); // both pass — the TOCTOU hole
    // ...then both commit, overshooting.
    ctx._agentSpawnCount.value += 2;
    assert.equal(ctx._agentSpawnCount.value, cap + 1); // overshoot the fix prevents
  });
});

// ===========================================================================
// E2E / integration red-green: drive the REAL task handler twice concurrently
// through one shared ToolContext at a session cap of 1. The handler's early
// checkAgentCaps runs before its pre-tool-use hook await; a yielding hook lets
// both spawns pass that early check and then race to the spawn commit. With the
// atomic reserveAgentSlot at the commit, exactly ONE spawns and the other is
// refused with a cap error. (RED: revert the two reserveAgentSlot commits back
// to bare incrementSpawnCount and BOTH succeed — this test then fails.)
// ===========================================================================

function e2eCtx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

function eventGen(events: unknown[]) {
  return async function* (_p: string) {
    for (const e of events) yield e;
  };
}

describe('taskTool — concurrent spawns cannot overshoot the session cap', () => {
  beforeEach(() => resetAgentRegistry());

  it('two parallel foreground spawns at cap=1 → one spawns, one is refused', async () => {
    let spawns = 0;
    // A hook whose async executePreToolUse yields a microtask, forcing the two
    // concurrent handler invocations to both clear the early cap check before
    // either reaches the spawn commit — reproducing the parallel-tool_call race.
    const hookManager = {
      executePreToolUse: async () => {
        await Promise.resolve();
        return { action: 'allow' };
      },
      executeSubagentStart: async () => undefined,
      executeSubagentStop: async () => undefined,
    };
    const shared = e2eCtx({
      _agentSpawnCount: { value: 0 },
      _maxAgentsPerSession: 1,
      _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
      _hookManager: hookManager as unknown as ToolContext['_hookManager'],
      _engineFactory: ((_c: unknown) => {
        spawns++;
        return { submit: eventGen([{ type: 'text_delta', text: 'ok' }]) };
      }) as unknown as ToolContext['_engineFactory'],
    });

    const [a, b] = await Promise.all([
      taskTool.execute({ prompt: 'first task' }, shared),
      taskTool.execute({ prompt: 'second task' }, shared),
    ]);

    const results = [a, b];
    const errors = results.filter(r => r.isError);
    const ok = results.filter(r => !r.isError);

    assert.equal(ok.length, 1, 'exactly one spawn is allowed at cap=1');
    assert.equal(errors.length, 1, 'the second concurrent spawn is refused');
    assert.ok(
      errors[0].output.includes('session cap'),
      `refusal cites the session cap, got: ${errors[0].output}`,
    );
    // The engine factory was invoked for the allowed spawn only — the refused one
    // never created a sub-engine (the reservation failed before factory use).
    assert.equal(spawns, 1, 'no sub-engine is created for the refused spawn');
    // The counter landed exactly at the cap, never cap + 1.
    assert.equal((shared as { _agentSpawnCount?: { value: number } })._agentSpawnCount?.value, 1);
  });
});
