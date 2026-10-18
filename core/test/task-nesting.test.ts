import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

// We test the exported helper that decides whether to include the 'task' tool
// and the depth-propagation logic.
import { shouldAllowNestedTask, getSubagentDepthContext } from '../src/tools/task';

// ---------------------------------------------------------------------------
// shouldAllowNestedTask — gate on depth
// ---------------------------------------------------------------------------

describe('shouldAllowNestedTask', () => {
  it('allows nesting at depth 0 (default)', () => {
    assert.equal(shouldAllowNestedTask(0, 5), true);
  });

  it('allows nesting at depth 4 (under default max 5)', () => {
    assert.equal(shouldAllowNestedTask(4, 5), true);
  });

  it('blocks nesting at depth 5 (equals default max)', () => {
    assert.equal(shouldAllowNestedTask(5, 5), false);
  });

  it('blocks nesting at depth 6 (over default max)', () => {
    assert.equal(shouldAllowNestedTask(6, 5), false);
  });

  it('allows nesting at depth 2 with custom maxDepth 3', () => {
    assert.equal(shouldAllowNestedTask(2, 3), true);
  });

  it('blocks nesting at depth 3 with custom maxDepth 3', () => {
    assert.equal(shouldAllowNestedTask(3, 3), false);
  });

  it('blocks all recursion when maxDepth is 0', () => {
    assert.equal(shouldAllowNestedTask(0, 0), false);
  });

  it('allows single level with maxDepth 1', () => {
    assert.equal(shouldAllowNestedTask(0, 1), true);
  });

  it('blocks at depth 1 with maxDepth 1', () => {
    assert.equal(shouldAllowNestedTask(1, 1), false);
  });
});

// ---------------------------------------------------------------------------
// getSubagentDepthContext — propagate depth to child
// ---------------------------------------------------------------------------

describe('getSubagentDepthContext', () => {
  it('returns depth 1 when parent is at depth 0', () => {
    const result = getSubagentDepthContext(0, 5);
    assert.equal(result._agentDepth, 1);
    assert.equal(result._maxAgentDepth, 5);
  });

  it('increments depth by 1', () => {
    const result = getSubagentDepthContext(3, 5);
    assert.equal(result._agentDepth, 4);
  });

  it('preserves maxAgentDepth', () => {
    const result = getSubagentDepthContext(0, 10);
    assert.equal(result._maxAgentDepth, 10);
  });

  it('works with custom maxDepth', () => {
    const result = getSubagentDepthContext(2, 3);
    assert.equal(result._agentDepth, 3);
    assert.equal(result._maxAgentDepth, 3);
  });
});

// ---------------------------------------------------------------------------
// Integration: tool filtering uses depth from context
// ---------------------------------------------------------------------------

describe('task tool filtering respects agent depth', () => {
  it('includes task tool when context has no depth (default depth 0)', async () => {
    // Import the tool and simulate filtering
    const { filterToolsForAgent } = await import('../src/tools/task');
    const fakeTools = [
      { name: 'bash' },
      { name: 'file_read' },
      { name: 'task' },
    ] as any[];
    const result = filterToolsForAgent(fakeTools, 'general', undefined, undefined);
    assert.ok(result.some(t => t.name === 'task'), 'task tool should be included at default depth');
  });

  it('includes task tool at depth 4 (under default max 5)', async () => {
    const { filterToolsForAgent } = await import('../src/tools/task');
    const fakeTools = [
      { name: 'bash' },
      { name: 'task' },
    ] as any[];
    const result = filterToolsForAgent(fakeTools, 'general', 4, 5);
    assert.ok(result.some(t => t.name === 'task'), 'task tool should be included at depth 4');
  });

  it('excludes task tool at depth 5 (equals max)', async () => {
    const { filterToolsForAgent } = await import('../src/tools/task');
    const fakeTools = [
      { name: 'bash' },
      { name: 'task' },
    ] as any[];
    const result = filterToolsForAgent(fakeTools, 'general', 5, 5);
    assert.ok(!result.some(t => t.name === 'task'), 'task tool should be excluded at max depth');
  });

  it('excludes task tool in explore mode regardless of depth', async () => {
    const { filterToolsForAgent } = await import('../src/tools/task');
    const fakeTools = [
      { name: 'bash' },
      { name: 'file_read' },
      { name: 'task' },
    ] as any[];
    const result = filterToolsForAgent(fakeTools, 'explore', 0, 5);
    assert.ok(!result.some(t => t.name === 'task'), 'explore mode never includes task');
  });

  it('excludes task tool when maxDepth is 0', async () => {
    const { filterToolsForAgent } = await import('../src/tools/task');
    const fakeTools = [
      { name: 'bash' },
      { name: 'task' },
    ] as any[];
    const result = filterToolsForAgent(fakeTools, 'general', 0, 0);
    assert.ok(!result.some(t => t.name === 'task'), 'task blocked when maxDepth=0');
  });
});

// ---------------------------------------------------------------------------
// Task tool input schema — effort parameter
// ---------------------------------------------------------------------------

describe('task tool inputSchema — effort parameter', () => {
  it('effort enum includes low, medium, high, max', async () => {
    const { taskTool } = await import('../src/tools/task');
    const props = taskTool.inputSchema.properties as Record<string, any>;
    assert.ok(props.effort, 'effort property must exist');
    assert.deepEqual(props.effort.enum, ['low', 'medium', 'high', 'max']);
    assert.equal(props.effort.type, 'string');
  });

  it('effort is optional (not required)', async () => {
    const { taskTool } = await import('../src/tools/task');
    const required = taskTool.inputSchema.required as string[];
    assert.ok(!required.includes('effort'), 'effort must not be required');
  });
});
