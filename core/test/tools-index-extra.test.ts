import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';

import { createDefaultTools, ToolRegistry } from '../src/tools/index';

// ---------------------------------------------------------------------------
// ToolRegistry
// ---------------------------------------------------------------------------
describe('ToolRegistry', () => {
  it('register and get work together', () => {
    const registry = new ToolRegistry();
    const tool = {
      name: 'test_tool',
      description: 'A test tool',
      inputSchema: { type: 'object' as const, properties: {} },
      isReadOnly: true,
      async execute() { return { output: 'ok' }; },
    };
    registry.register(tool);
    assert.equal(registry.get('test_tool'), tool);
  });

  it('get returns undefined for unknown tool', () => {
    const registry = new ToolRegistry();
    assert.equal(registry.get('nonexistent'), undefined);
  });

  it('getAll returns all registered tools', () => {
    const registry = new ToolRegistry();
    const tool1 = {
      name: 'tool_a',
      description: 'Tool A',
      inputSchema: { type: 'object' as const, properties: {} },
      isReadOnly: true,
      async execute() { return { output: 'a' }; },
    };
    const tool2 = {
      name: 'tool_b',
      description: 'Tool B',
      inputSchema: { type: 'object' as const, properties: {} },
      isReadOnly: false,
      async execute() { return { output: 'b' }; },
    };
    registry.register(tool1);
    registry.register(tool2);
    assert.equal(registry.getAll().length, 2);
  });

  it('toOpenAIFormat returns correct structure', () => {
    const registry = new ToolRegistry();
    registry.register({
      name: 'my_tool',
      description: 'My tool',
      inputSchema: { type: 'object' as const, properties: { x: { type: 'string', description: 'x' } } },
      isReadOnly: true,
      async execute() { return { output: 'ok' }; },
    });
    const result = registry.toOpenAIFormat();
    assert.equal(result.length, 1);
    assert.equal(result[0].type, 'function');
    assert.equal(result[0].function.name, 'my_tool');
    assert.equal(result[0].function.description, 'My tool');
    assert.deepEqual(result[0].function.parameters, {
      type: 'object',
      properties: { x: { type: 'string', description: 'x' } },
    });
  });
});

// ---------------------------------------------------------------------------
// createDefaultTools — coverage for lines 52-74 (all register calls)
// ---------------------------------------------------------------------------
describe('createDefaultTools', () => {
  it('returns a ToolRegistry with all default tools registered', () => {
    const registry = createDefaultTools(os.tmpdir());
    const tools = registry.getAll();

    // All expected default tools
    const expectedNames = [
      'bash', 'file_read', 'file_write', 'file_edit',
      'grep', 'glob', 'list_dir', 'web_fetch', 'web_search',
      'notebook_edit', 'search_symbols', 'multi_edit',
      'task', 'tool_search', 'AskUserQuestion', 'git_commit',
      'task_tracker', 'plan_mode',
    ];

    for (const name of expectedNames) {
      assert.ok(registry.get(name), `Tool "${name}" should be registered`);
    }

    assert.ok(tools.length >= expectedNames.length, `Should have at least ${expectedNames.length} tools, got ${tools.length}`);
  });

  it('each tool has required properties', () => {
    const registry = createDefaultTools(os.tmpdir());
    for (const tool of registry.getAll()) {
      assert.ok(tool.name, 'Tool should have a name');
      assert.ok(tool.description, 'Tool should have a description');
      assert.ok(tool.inputSchema, 'Tool should have an input schema');
      assert.equal(typeof tool.execute, 'function', 'Tool should have an execute function');
      assert.equal(typeof tool.isReadOnly, 'boolean', 'Tool should have isReadOnly');
    }
  });
});
