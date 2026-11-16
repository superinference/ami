import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  getToolCategory,
  TOOL_CATEGORY_LABELS,
  bashTool,
  fileEditTool,
  notebookEditTool,
  fileReadTool,
  grepTool,
  globTool,
  toolSearchTool,
  type ToolCategory,
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
