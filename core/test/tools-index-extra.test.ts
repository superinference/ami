import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';

import {
  createDefaultTools, ToolRegistry,
  // Re-exported tools
  taskCompleteTool, taskOutputTool, taskKillTool, taskListTool,
  skillTool, cronCreateTool, cronDeleteTool, cronListTool,
  scheduleWakeupTool, enterWorktreeTool, exitWorktreeTool,
  sendMessageTool, sleepTool, workflowTool, briefTool,
  teamCreateTool, teamDeleteTool, todoWriteTool, configTool,
  syntheticOutputTool, lspDiagnosticsTool, lspReferencesTool,
  agenticFetchTool, scoutTool, webFetchTool, webSearchTool,
  // Re-exported functions
  resetTaskState, getMailbox, clearMailbox, resetAllMailboxes,
  getCronScheduler, resetCronScheduler,
  getCurrentTeam, resetTeam,
  resetTodos, getTodos,
  parseBashAST, extractCommands, extractCommandsFromString,
  shouldUseSandbox, getSandboxStatus, resetSandboxCache,
  isReadOnlyBashCommand, BashResultCache,
  normalizeCommand, deobfuscateEnvExpansion, collapseWhitespace,
  sanitizeToolInput, validateRequiredFields, detectPromptInjection,
  levenshteinDistance, suggestSimilarPaths,
  detectToolConflict, isReservedToolName, ToolRegistryGuard, RESERVED_TOOL_PREFIXES,
  parseWorkflowMeta,
  getWorkspaceIndexer,
} from '../src/tools/index';

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
      'bash', 'file_read', 'file_edit',
      'grep', 'glob', 'list_dir', 'web_fetch', 'web_search',
      'notebook_edit', 'search_symbols',
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

  it('getConflicts returns empty array for fresh registry', () => {
    const registry = new ToolRegistry();
    assert.deepEqual(registry.getConflicts(), []);
  });
});

// ---------------------------------------------------------------------------
// Re-exported tools are importable and have correct shape
// ---------------------------------------------------------------------------
describe('re-exported tool definitions via index', () => {
  const tools = [
    taskCompleteTool, taskOutputTool, taskKillTool, taskListTool,
    skillTool, cronCreateTool, cronDeleteTool, cronListTool,
    scheduleWakeupTool, enterWorktreeTool, exitWorktreeTool,
    sendMessageTool, sleepTool, workflowTool, briefTool,
    teamCreateTool, teamDeleteTool, todoWriteTool, configTool,
    syntheticOutputTool, lspDiagnosticsTool, lspReferencesTool,
    agenticFetchTool, scoutTool, webFetchTool, webSearchTool,
  ];

  it('all re-exported tools have name, description, execute, inputSchema', () => {
    for (const tool of tools) {
      assert.ok(tool.name, `tool missing name`);
      assert.ok(tool.description, `${tool.name} missing description`);
      assert.equal(typeof tool.execute, 'function', `${tool.name} missing execute`);
      assert.ok(tool.inputSchema, `${tool.name} missing inputSchema`);
    }
  });
});

// ---------------------------------------------------------------------------
// Re-exported functions via index
// ---------------------------------------------------------------------------
describe('re-exported functions via index', () => {
  it('resetTaskState is callable', () => {
    assert.equal(typeof resetTaskState, 'function');
    resetTaskState();
  });

  it('getMailbox, clearMailbox, resetAllMailboxes are callable', () => {
    assert.equal(typeof getMailbox, 'function');
    assert.equal(typeof clearMailbox, 'function');
    assert.equal(typeof resetAllMailboxes, 'function');
    resetAllMailboxes();
    const mailbox = getMailbox('test-agent');
    assert.ok(Array.isArray(mailbox));
    clearMailbox('test-agent');
  });

  it('getCronScheduler and resetCronScheduler are callable', () => {
    assert.equal(typeof getCronScheduler, 'function');
    assert.equal(typeof resetCronScheduler, 'function');
    resetCronScheduler();
  });

  it('getCurrentTeam and resetTeam are callable', () => {
    assert.equal(typeof getCurrentTeam, 'function');
    assert.equal(typeof resetTeam, 'function');
    resetTeam();
    const team = getCurrentTeam();
    assert.equal(team, null);
  });

  it('resetTodos and getTodos are callable', () => {
    assert.equal(typeof resetTodos, 'function');
    assert.equal(typeof getTodos, 'function');
    resetTodos();
    const todos = getTodos();
    assert.ok(Array.isArray(todos));
    assert.equal(todos.length, 0);
  });

  it('parseBashAST, extractCommands, extractCommandsFromString are callable', () => {
    assert.equal(typeof parseBashAST, 'function');
    assert.equal(typeof extractCommands, 'function');
    assert.equal(typeof extractCommandsFromString, 'function');
    const cmds = extractCommandsFromString('ls -la');
    assert.ok(Array.isArray(cmds));
  });

  it('shouldUseSandbox, getSandboxStatus, resetSandboxCache are callable', () => {
    assert.equal(typeof shouldUseSandbox, 'function');
    assert.equal(typeof getSandboxStatus, 'function');
    assert.equal(typeof resetSandboxCache, 'function');
    resetSandboxCache();
  });

  it('isReadOnlyBashCommand is callable', () => {
    assert.equal(typeof isReadOnlyBashCommand, 'function');
    assert.equal(isReadOnlyBashCommand('ls'), true);
    assert.equal(isReadOnlyBashCommand('rm -rf /'), false);
  });

  it('BashResultCache is constructable', () => {
    assert.equal(typeof BashResultCache, 'function');
    const cache = new BashResultCache();
    assert.ok(cache);
  });

  it('normalizeCommand, deobfuscateEnvExpansion, collapseWhitespace are callable', () => {
    assert.equal(typeof normalizeCommand, 'function');
    assert.equal(typeof deobfuscateEnvExpansion, 'function');
    assert.equal(typeof collapseWhitespace, 'function');
    assert.equal(normalizeCommand('ls   -la'), 'ls -la');
  });

  it('sanitizeToolInput, validateRequiredFields, detectPromptInjection are callable', () => {
    assert.equal(typeof sanitizeToolInput, 'function');
    assert.equal(typeof validateRequiredFields, 'function');
    assert.equal(typeof detectPromptInjection, 'function');
  });

  it('levenshteinDistance, suggestSimilarPaths are callable', () => {
    assert.equal(typeof levenshteinDistance, 'function');
    assert.equal(typeof suggestSimilarPaths, 'function');
    assert.equal(levenshteinDistance('cat', 'hat'), 1);
  });

  it('detectToolConflict, isReservedToolName are callable', () => {
    assert.equal(typeof detectToolConflict, 'function');
    assert.equal(typeof isReservedToolName, 'function');
    assert.equal(isReservedToolName('bash'), true);
    assert.equal(isReservedToolName('my_custom_tool'), false);
  });

  it('ToolRegistryGuard is constructable', () => {
    assert.equal(typeof ToolRegistryGuard, 'function');
    const guard = new ToolRegistryGuard();
    assert.ok(guard);
  });

  it('RESERVED_TOOL_PREFIXES is an array', () => {
    assert.ok(Array.isArray(RESERVED_TOOL_PREFIXES));
    assert.ok(RESERVED_TOOL_PREFIXES.length > 0);
  });

  it('parseWorkflowMeta is callable', () => {
    assert.equal(typeof parseWorkflowMeta, 'function');
  });

  it('getWorkspaceIndexer is callable', () => {
    assert.equal(typeof getWorkspaceIndexer, 'function');
  });
});
