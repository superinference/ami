import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { taskKillTool } from '../src/tools/task-kill';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U011 — task_kill `prune_idle` bulk mode (parity: the reference's TeamsDialog 'p'
// prune-idle handler, TeamsDialog.tsx L207-217): stop every idle teammate at once,
// with NO task_id, leaving busy teammates untouched. Red-green: reverting the
// prune_idle handler makes `{ prune_idle: true }` fall through to the task_id
// requirement and fail these cases.
// ---------------------------------------------------------------------------

function ctxWithPruned(pruned: Array<{ taskId: string }>): ToolContext {
  const calls = { prune: 0 };
  return {
    cwd: '/tmp',
    processManager: {
      pruneIdleAgents: () => { calls.prune++; return pruned; },
      // present but must NOT be consulted on the bulk path:
      get: () => { throw new Error('get() should not be called on the prune path'); },
      kill: () => false,
    },
    // expose the counter for assertions
    _pruneCalls: calls,
  } as unknown as ToolContext;
}

describe('taskKillTool prune_idle', () => {
  it('advertises the prune_idle parameter in the schema', () => {
    const props = taskKillTool.inputSchema.properties as Record<string, unknown>;
    assert.ok(props.prune_idle, 'prune_idle property present');
  });

  it('bulk-stops every idle agent and reports the count + ids (no task_id required)', async () => {
    const ctx = ctxWithPruned([{ taskId: 'a1' }, { taskId: 'a2' }]);
    const result = await taskKillTool.execute!({ prune_idle: true }, ctx);
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('Pruned 2 idle agents'));
    assert.ok(result.output.includes('a1'));
    assert.ok(result.output.includes('a2'));
    assert.equal(result.metadata?.reason, 'pruned_idle');
    assert.deepEqual(result.metadata?.prunedTaskIds, ['a1', 'a2']);
    assert.equal(result.metadata?.prunedCount, 2);
    assert.equal((ctx as unknown as { _pruneCalls: { prune: number } })._pruneCalls.prune, 1);
  });

  it('uses singular wording for exactly one pruned agent', async () => {
    const ctx = ctxWithPruned([{ taskId: 'solo' }]);
    const result = await taskKillTool.execute!({ prune_idle: true }, ctx);
    assert.ok(result.output.includes('Pruned 1 idle agent:'));
    assert.ok(!result.output.includes('idle agents'));
  });

  it('reports "no idle agents" when nothing was idle (still a success)', async () => {
    const ctx = ctxWithPruned([]);
    const result = await taskKillTool.execute!({ prune_idle: true }, ctx);
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('No idle agents to prune'));
    assert.equal(result.metadata?.reason, 'pruned_idle');
    assert.equal(result.metadata?.prunedCount, 0);
  });

  it('rejects a non-boolean prune_idle', async () => {
    const ctx = ctxWithPruned([]);
    const result = await taskKillTool.execute!({ prune_idle: 'yes' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('"prune_idle" must be a boolean'));
  });

  it('refuses prune_idle combined with a per-agent action', async () => {
    const ctx = ctxWithPruned([{ taskId: 'a1' }]);
    const result = await taskKillTool.execute!({ prune_idle: true, graceful: true }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('cannot be combined'));
    assert.equal(result.metadata?.reason, 'invalid_input');
  });

  it('errors (manager_unavailable) before pruning when no process manager', async () => {
    const result = await taskKillTool.execute!({ prune_idle: true }, { cwd: '/tmp' } as ToolContext);
    assert.ok(result.isError);
    assert.ok(result.output.includes('not available'));
  });

  it('lists prune_idle in the unknown-key allow-list message', async () => {
    const ctx = ctxWithPruned([]);
    const result = await taskKillTool.execute!({ bogus: 1 }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('prune_idle'));
  });
});
