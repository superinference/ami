import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  ToolRegistry, createDefaultTools,
  bashTool, fileReadTool, fileWriteTool, fileEditTool,
  grepTool, globTool, listDirTool,
  notebookEditTool, searchSymbolsTool, multiEditTool,
  taskTool, toolSearchTool, askUserQuestionTool,
  gitCommitTool,
  taskTrackerTool,
  planModeTool,
} from '../src/tools/index';

describe('ToolRegistry', () => {
  it('register and get a tool', () => {
    const registry = new ToolRegistry();
    const tool = {
      name: 'test_tool',
      description: 'A test tool',
      inputSchema: { type: 'object' as const, properties: {}, required: [] },
      isReadOnly: true,
      isConcurrencySafe: true,
      execute: async () => ({ output: 'ok', isError: false }),
    };
    registry.register(tool);
    assert.equal(registry.get('test_tool'), tool);
  });

  it('get returns undefined for unknown tool', () => {
    const registry = new ToolRegistry();
    assert.equal(registry.get('unknown'), undefined);
  });

  it('getAll returns all registered tools', () => {
    const registry = new ToolRegistry();
    const tool1 = {
      name: 'tool1',
      description: 'Tool 1',
      inputSchema: { type: 'object' as const, properties: {}, required: [] },
      isReadOnly: true,
      isConcurrencySafe: true,
      execute: async () => ({ output: 'ok', isError: false }),
    };
    const tool2 = {
      name: 'tool2',
      description: 'Tool 2',
      inputSchema: { type: 'object' as const, properties: {}, required: [] },
      isReadOnly: false,
      isConcurrencySafe: false,
      execute: async () => ({ output: 'ok', isError: false }),
    };
    registry.register(tool1);
    registry.register(tool2);
    const all = registry.getAll();
    assert.equal(all.length, 2);
    assert.ok(all.includes(tool1));
    assert.ok(all.includes(tool2));
  });

  it('toOpenAIFormat returns correct shape', () => {
    const registry = new ToolRegistry();
    const tool = {
      name: 'my_tool',
      description: 'My tool description',
      inputSchema: { type: 'object' as const, properties: { arg: { type: 'string' } }, required: ['arg'] },
      isReadOnly: true,
      isConcurrencySafe: true,
      execute: async () => ({ output: 'ok', isError: false }),
    };
    registry.register(tool);
    const formatted = registry.toOpenAIFormat();
    assert.equal(formatted.length, 1);
    assert.equal(formatted[0].type, 'function');
    assert.equal(formatted[0].function.name, 'my_tool');
    assert.equal(formatted[0].function.description, 'My tool description');
    assert.deepEqual(formatted[0].function.parameters, tool.inputSchema);
  });

  it('register overwrites existing tool with same name', () => {
    const registry = new ToolRegistry();
    const tool1 = {
      name: 'dup',
      description: 'First',
      inputSchema: { type: 'object' as const, properties: {}, required: [] },
      isReadOnly: true,
      isConcurrencySafe: true,
      execute: async () => ({ output: 'v1', isError: false }),
    };
    const tool2 = {
      name: 'dup',
      description: 'Second',
      inputSchema: { type: 'object' as const, properties: {}, required: [] },
      isReadOnly: true,
      isConcurrencySafe: true,
      execute: async () => ({ output: 'v2', isError: false }),
    };
    registry.register(tool1);
    registry.register(tool2);
    assert.equal(registry.get('dup')!.description, 'Second');
    assert.equal(registry.getAll().length, 1);
  });
});

describe('createDefaultTools', () => {
  it('creates a registry with all default tools', () => {
    const registry = createDefaultTools('/tmp');
    const all = registry.getAll();
    assert.ok(all.length >= 15, `Expected at least 15 tools, got ${all.length}`);
  });

  it('includes core tools by name', () => {
    const registry = createDefaultTools('/tmp');
    const names = registry.getAll().map(t => t.name);
    for (const expected of ['bash', 'file_read', 'file_write', 'file_edit', 'grep', 'glob', 'list_dir', 'web_fetch', 'web_search', 'notebook_edit', 'search_symbols', 'multi_edit', 'task', 'tool_search', 'AskUserQuestion', 'git_commit', 'task_tracker', 'plan_mode']) {
      assert.ok(names.includes(expected), `Missing tool: ${expected}`);
    }
  });

  it('tools are retrievable by name', () => {
    const registry = createDefaultTools('/tmp');
    assert.ok(registry.get('bash'));
    assert.ok(registry.get('file_read'));
    assert.ok(registry.get('file_edit'));
  });

  it('toOpenAIFormat returns all tools', () => {
    const registry = createDefaultTools('/tmp');
    const formatted = registry.toOpenAIFormat();
    assert.ok(formatted.length >= 15);
    for (const entry of formatted) {
      assert.equal(entry.type, 'function');
      assert.ok(entry.function.name);
      assert.ok(entry.function.description);
    }
  });

  it('tools are callable through registry', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-tools-idx-'));
    try {
      const registry = createDefaultTools(tmpDir);
      const ctx = { cwd: tmpDir, abortSignal: new AbortController().signal };

      const listDir = registry.get('list_dir')!;
      const result = await listDir.execute({ path: tmpDir }, ctx);
      assert.ok(!result.isError);

      const glob = registry.get('glob')!;
      const globResult = await glob.execute({ pattern: '*.ts' }, ctx);
      assert.ok(!globResult.isError);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

describe('re-exported tools are accessible', () => {
  it('all re-exported tools have correct types', () => {
    for (const tool of [bashTool, fileReadTool, fileWriteTool, fileEditTool, grepTool, globTool, listDirTool, notebookEditTool, searchSymbolsTool, multiEditTool, taskTool, toolSearchTool, askUserQuestionTool, gitCommitTool, taskTrackerTool, planModeTool]) {
      assert.equal(typeof tool.name, 'string');
      assert.equal(typeof tool.description, 'string');
      assert.equal(typeof tool.execute, 'function');
      assert.ok(tool.inputSchema);
    }
  });

  it('re-exported tools can execute validation', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-tools-reexport-'));
    const ctx = { cwd: tmpDir, abortSignal: new AbortController().signal };
    try {
      const r1 = await fileReadTool.execute({ file_path: '' }, ctx);
      assert.equal(r1.isError, true);

      const r2 = await fileEditTool.execute({ file_path: '', old_string: 'a', new_string: 'b' }, ctx);
      assert.equal(r2.isError, true);

      const r3 = await fileWriteTool.execute({ file_path: '', content: 'x' }, ctx);
      assert.equal(r3.isError, true);

      const r4 = await grepTool.execute({ pattern: '' }, ctx);
      assert.equal(r4.isError, true);

      const r5 = await globTool.execute({ pattern: '*.ts' }, ctx);
      assert.ok(!r5.isError);

      const r6 = await listDirTool.execute({ path: tmpDir }, ctx);
      assert.ok(!r6.isError);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
