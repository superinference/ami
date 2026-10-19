import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { taskCompleteTool } from '../src/tools/task-complete';

describe('taskCompleteTool', () => {
  it('has correct name and metadata', () => {
    assert.equal(taskCompleteTool.name, 'task_complete');
    assert.ok(taskCompleteTool.isReadOnly);
    assert.deepEqual(taskCompleteTool.inputSchema.required, ['summary']);
  });

  it('returns summary in output', async () => {
    const result = await taskCompleteTool.execute!({ summary: 'Fixed the bug' }, {} as any);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Fixed the bug'));
    assert.ok(result.output.includes('Task complete'));
  });

  it('uses default summary when empty string', async () => {
    const result = await taskCompleteTool.execute!({ summary: '' }, {} as any);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Task complete'));
  });

  it('instructs no more tool calls', async () => {
    const result = await taskCompleteTool.execute!({ summary: 'Done' }, {} as any);
    assert.ok(result.output.includes('Do not make any more tool calls'));
  });

  it('isError is false', async () => {
    const result = await taskCompleteTool.execute!({ summary: 'test' }, {} as any);
    assert.equal(result.isError, false);
  });
});
