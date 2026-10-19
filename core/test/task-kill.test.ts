import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { taskKillTool } from '../src/tools/task-kill';
import type { ToolContext } from '../src/types';

describe('taskKillTool', () => {
  it('has correct name and schema', () => {
    assert.equal(taskKillTool.name, 'task_kill');
    assert.equal(taskKillTool.isReadOnly, false);
    assert.deepEqual(taskKillTool.inputSchema.required, ['task_id']);
  });

  it('returns error when processManager is not available', async () => {
    const ctx = { cwd: '/tmp' } as ToolContext;
    const result = await taskKillTool.execute!({ task_id: 'abc' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('not available'));
  });

  it('returns error when task not found', async () => {
    const ctx = {
      cwd: '/tmp',
      processManager: {
        get: () => undefined,
        kill: () => false,
      },
    } as unknown as ToolContext;
    const result = await taskKillTool.execute!({ task_id: 'missing-id' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('No background task'));
    assert.ok(result.output.includes('missing-id'));
  });

  it('reports already completed task', async () => {
    const ctx = {
      cwd: '/tmp',
      processManager: {
        get: () => ({ status: 'completed', exitCode: 0, pid: 1234 }),
        kill: () => false,
      },
    } as unknown as ToolContext;
    const result = await taskKillTool.execute!({ task_id: 'done-task' }, ctx);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('already completed'));
  });

  it('kills running task successfully', async () => {
    const ctx = {
      cwd: '/tmp',
      processManager: {
        get: () => ({ status: 'running', pid: 5678 }),
        kill: () => true,
      },
    } as unknown as ToolContext;
    const result = await taskKillTool.execute!({ task_id: 'active-task' }, ctx);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('killed successfully'));
    assert.ok(result.output.includes('5678'));
  });

  it('reports failure when kill returns false', async () => {
    const ctx = {
      cwd: '/tmp',
      processManager: {
        get: () => ({ status: 'running', pid: 9999 }),
        kill: () => false,
      },
    } as unknown as ToolContext;
    const result = await taskKillTool.execute!({ task_id: 'unkillable' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('Failed to kill'));
  });
});
