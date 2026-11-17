import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  getToolCategory,
  TOOL_CATEGORY_LABELS,
  isToolConcurrencySafe,
  isToolReadOnlyForInput,
  bashTool,
  fileEditTool,
  notebookEditTool,
  fileReadTool,
  grepTool,
  globTool,
  toolSearchTool,
  type ToolCategory,
  type CategorizableTool,
} from '../src/tools/index';

describe('tool-categories — getToolCategory', () => {
  it('classifies execution tools', () => {
    assert.equal(getToolCategory(bashTool), 'execution');
  });

  it('classifies edit tools (content mutation)', () => {
    assert.equal(getToolCategory(fileEditTool), 'edit');
    assert.equal(getToolCategory(notebookEditTool), 'edit');
  });

  it('classifies read-only tools via their isReadOnly metadata', () => {
    assert.equal(getToolCategory(fileReadTool), 'read-only');
    assert.equal(getToolCategory(grepTool), 'read-only');
    assert.equal(getToolCategory(globTool), 'read-only');
    assert.equal(getToolCategory(toolSearchTool), 'read-only');
  });

  it('classifies MCP tools by the isMcp flag', () => {
    assert.equal(
      getToolCategory({ name: 'some_mcp_tool', isReadOnly: true, isMcp: true }),
      'mcp',
    );
  });

  it('classifies MCP tools by the mcp__ name prefix', () => {
    assert.equal(getToolCategory({ name: 'mcp__server__do', isReadOnly: false }), 'mcp');
  });

  it('falls back to other for a non-read-only, non-execution, non-edit tool', () => {
    assert.equal(getToolCategory({ name: 'mystery', isReadOnly: false }), 'other');
  });

  it('MCP classification wins over execution/read-only ordering', () => {
    // A tool named like bash but flagged MCP should still be MCP (first match wins).
    assert.equal(getToolCategory({ name: 'bash', isReadOnly: false, isMcp: true }), 'mcp');
  });

  it('every category has a human-readable label', () => {
    const cats: ToolCategory[] = ['read-only', 'edit', 'execution', 'mcp', 'other'];
    for (const c of cats) {
      assert.equal(typeof TOOL_CATEGORY_LABELS[c], 'string');
      assert.ok(TOOL_CATEGORY_LABELS[c].length > 0);
    }
    assert.equal(TOOL_CATEGORY_LABELS['read-only'], 'Read-only tools');
    assert.equal(TOOL_CATEGORY_LABELS.execution, 'Execution tools');
  });
});

describe('tool-categories — isToolConcurrencySafe (input-aware predicate)', () => {
  it('returns a static boolean flag verbatim (true)', () => {
    const tool: CategorizableTool = { name: 't', isReadOnly: false, isConcurrencySafe: true };
    assert.equal(isToolConcurrencySafe(tool, {}), true);
  });

  it('returns a static boolean flag verbatim (false) even for a read-only tool', () => {
    const tool: CategorizableTool = { name: 't', isReadOnly: true, isConcurrencySafe: false };
    assert.equal(isToolConcurrencySafe(tool, {}), false);
  });

  it('evaluates a per-invocation predicate against the call input', () => {
    const tool: CategorizableTool = {
      name: 't',
      isReadOnly: true,
      isConcurrencySafe: (input) => input.parallel === true,
    };
    assert.equal(isToolConcurrencySafe(tool, { parallel: true }), true);
    assert.equal(isToolConcurrencySafe(tool, { parallel: false }), false);
  });

  it('defaults to the per-invocation read-only verdict when the field is absent', () => {
    const readOnly: CategorizableTool = { name: 'r', isReadOnly: true };
    const writable: CategorizableTool = { name: 'w', isReadOnly: false };
    assert.equal(isToolConcurrencySafe(readOnly, {}), true);
    assert.equal(isToolConcurrencySafe(writable, {}), false);
    // Default tracks isReadOnlyForInput exactly.
    const dynamic: CategorizableTool = {
      name: 'd',
      isReadOnly: false,
      isReadOnlyForInput: (input) => input.op === 'get',
    };
    assert.equal(isToolConcurrencySafe(dynamic, { op: 'get' }), true);
    assert.equal(isToolConcurrencySafe(dynamic, { op: 'set' }), false);
    assert.equal(isToolConcurrencySafe(dynamic, { op: 'get' }), isToolReadOnlyForInput(dynamic, { op: 'get' }));
  });
});
