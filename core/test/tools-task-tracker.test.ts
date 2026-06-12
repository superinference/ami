import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { taskTrackerTool, resetTaskState } from '../src/tools/task-tracker';
import type { ToolContext } from '../src/types';

function ctx(): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
  };
}

// Reset state between every test
beforeEach(() => {
  resetTaskState();
});

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('taskTrackerTool — definition', () => {
  it('has the correct name', () => {
    assert.equal(taskTrackerTool.name, 'task_tracker');
  });

  it('has a description', () => {
    assert.ok(taskTrackerTool.description.length > 0);
  });

  it('is not read-only', () => {
    assert.equal(taskTrackerTool.isReadOnly, false);
  });

  it('schema requires "action"', () => {
    assert.ok(taskTrackerTool.inputSchema.required?.includes('action'));
  });

  it('schema defines all action-related properties', () => {
    const props = taskTrackerTool.inputSchema.properties;
    assert.ok('action' in props);
    assert.ok('subject' in props);
    assert.ok('description' in props);
    assert.ok('task_id' in props);
    assert.ok('status' in props);
  });
});

// ---------------------------------------------------------------------------
// Create action
// ---------------------------------------------------------------------------

describe('taskTrackerTool — create', () => {
  it('creates a task and returns its ID', async () => {
    const result = await taskTrackerTool.execute(
      { action: 'create', subject: 'Fix the bug', description: 'It crashes on null input' },
      ctx(),
    );
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('Task #1'));
    assert.ok(result.output.includes('Fix the bug'));
    assert.ok(result.output.includes('[pending]'));
  });

  it('auto-increments task IDs', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'First' }, ctx());
    const result = await taskTrackerTool.execute({ action: 'create', subject: 'Second' }, ctx());
    assert.ok(result.output.includes('Task #2'));
  });

  it('rejects empty subject', async () => {
    const result = await taskTrackerTool.execute({ action: 'create', subject: '' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('subject is required'));
  });

  it('rejects missing subject', async () => {
    const result = await taskTrackerTool.execute({ action: 'create' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('subject is required'));
  });

  it('trims whitespace from subject', async () => {
    const result = await taskTrackerTool.execute(
      { action: 'create', subject: '  Trimmed  ' },
      ctx(),
    );
    assert.ok(result.output.includes('Trimmed'));
    assert.ok(!result.output.includes('  Trimmed'));
  });
});

// ---------------------------------------------------------------------------
// List action
// ---------------------------------------------------------------------------

describe('taskTrackerTool — list', () => {
  it('returns "No tasks" when empty', async () => {
    const result = await taskTrackerTool.execute({ action: 'list' }, ctx());
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('No tasks'));
  });

  it('lists all tasks with status icons', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Task A' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'Task B' }, ctx());
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'in_progress' },
      ctx(),
    );

    const result = await taskTrackerTool.execute({ action: 'list' }, ctx());
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('[>] Task #1'));
    assert.ok(result.output.includes('[ ] Task #2'));
  });

  it('lists tasks in ID order', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'First' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'Second' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'Third' }, ctx());

    const result = await taskTrackerTool.execute({ action: 'list' }, ctx());
    const lines = result.output.split('\n').filter(l => l.includes('Task #'));
    assert.ok(lines[0].includes('#1'));
    assert.ok(lines[1].includes('#2'));
    assert.ok(lines[2].includes('#3'));
  });
});

// ---------------------------------------------------------------------------
// Get action
// ---------------------------------------------------------------------------

describe('taskTrackerTool — get', () => {
  it('returns a single task', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'My Task', description: 'Do this thing' },
      ctx(),
    );
    const result = await taskTrackerTool.execute({ action: 'get', task_id: '1' }, ctx());
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('My Task'));
    assert.ok(result.output.includes('Do this thing'));
  });

  it('returns error for non-existent task', async () => {
    const result = await taskTrackerTool.execute({ action: 'get', task_id: '99' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not found'));
  });

  it('returns error for missing task_id', async () => {
    const result = await taskTrackerTool.execute({ action: 'get' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('task_id is required'));
  });

  it('returns error for non-numeric task_id', async () => {
    const result = await taskTrackerTool.execute({ action: 'get', task_id: 'abc' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('task_id is required'));
  });
});

// ---------------------------------------------------------------------------
// Update action
// ---------------------------------------------------------------------------

describe('taskTrackerTool — update', () => {
  it('updates task status', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Test' }, ctx());
    const result = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'in_progress' },
      ctx(),
    );
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('[>]'));
    assert.ok(result.output.includes('[in_progress]'));
  });

  it('updates task subject', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Old Name' }, ctx());
    const result = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', subject: 'New Name' },
      ctx(),
    );
    assert.ok(result.output.includes('New Name'));
  });

  it('updates task description', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'Test', description: 'Old desc' },
      ctx(),
    );
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', description: 'New desc' },
      ctx(),
    );
    const result = await taskTrackerTool.execute({ action: 'get', task_id: '1' }, ctx());
    assert.ok(result.output.includes('New desc'));
  });

  it('supports full status lifecycle: pending → in_progress → completed', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Lifecycle' }, ctx());

    let result = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'in_progress' },
      ctx(),
    );
    assert.ok(result.output.includes('[in_progress]'));

    result = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'completed' },
      ctx(),
    );
    assert.ok(result.output.includes('[x]'));
    assert.ok(result.output.includes('[completed]'));
  });

  it('returns error for non-existent task', async () => {
    const result = await taskTrackerTool.execute(
      { action: 'update', task_id: '99', status: 'completed' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not found'));
  });

  it('returns error for invalid status', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Test' }, ctx());
    const result = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'invalid_status' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('invalid status'));
  });

  it('returns error for missing task_id', async () => {
    const result = await taskTrackerTool.execute(
      { action: 'update', status: 'completed' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('task_id is required'));
  });
});

// ---------------------------------------------------------------------------
// Delete action
// ---------------------------------------------------------------------------

describe('taskTrackerTool — delete', () => {
  it('deletes an existing task', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'To Delete' }, ctx());
    const result = await taskTrackerTool.execute({ action: 'delete', task_id: '1' }, ctx());
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('deleted'));

    const listResult = await taskTrackerTool.execute({ action: 'list' }, ctx());
    assert.ok(listResult.output.includes('No tasks'));
  });

  it('returns error for non-existent task', async () => {
    const result = await taskTrackerTool.execute({ action: 'delete', task_id: '99' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not found'));
  });

  it('returns error for missing task_id', async () => {
    const result = await taskTrackerTool.execute({ action: 'delete' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('task_id is required'));
  });
});

// ---------------------------------------------------------------------------
// Unknown action
// ---------------------------------------------------------------------------

describe('taskTrackerTool — unknown action', () => {
  it('returns error for unknown action', async () => {
    const result = await taskTrackerTool.execute({ action: 'unknown' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('unknown action'));
  });
});

// ---------------------------------------------------------------------------
// State management
// ---------------------------------------------------------------------------

describe('taskTrackerTool — state management', () => {
  it('resetTaskState clears all tasks and resets ID counter', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'First' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'Second' }, ctx());
    resetTaskState();

    const listResult = await taskTrackerTool.execute({ action: 'list' }, ctx());
    assert.ok(listResult.output.includes('No tasks'));

    const result = await taskTrackerTool.execute({ action: 'create', subject: 'After Reset' }, ctx());
    assert.ok(result.output.includes('Task #1'));
  });
});

// ---------------------------------------------------------------------------
// Integration — tool registry
// ---------------------------------------------------------------------------

describe('taskTrackerTool — integration', () => {
  it('is registered in createDefaultTools', () => {
    const { createDefaultTools } = require('../src/tools/index');
    const registry = createDefaultTools('/tmp');
    const tool = registry.get('task_tracker');
    assert.ok(tool, 'task_tracker should be in the default registry');
    assert.equal(tool.name, 'task_tracker');
    assert.equal(tool.isReadOnly, false);
  });

  it('is included in OpenAI format with correct schema', () => {
    const { createDefaultTools } = require('../src/tools/index');
    const registry = createDefaultTools('/tmp');
    const formatted = registry.toOpenAIFormat();
    const entry = formatted.find((e: any) => e.function.name === 'task_tracker');
    assert.ok(entry);
    assert.equal(entry.type, 'function');
    assert.ok(entry.function.parameters.required?.includes('action'));
  });
});
