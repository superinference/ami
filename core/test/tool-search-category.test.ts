import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { toolSearchTool, setSearchableTools } from '../src/tools/tool-search';
import type { ToolDefinition, ToolContext } from '../src/types';

const mockTools: ToolDefinition[] = [
  { name: 'file_read', description: 'Read file contents from the filesystem', inputSchema: { type: 'object', properties: { file_path: { type: 'string' } }, required: ['file_path'] }, isReadOnly: true, execute: async () => ({ output: '', isError: false }) },
  { name: 'bash', description: 'Execute shell commands', inputSchema: { type: 'object', properties: { command: { type: 'string' } }, required: ['command'] }, isReadOnly: false, execute: async () => ({ output: '', isError: false }) },
  { name: 'file_edit', description: 'Edit a file', inputSchema: { type: 'object', properties: { file_path: { type: 'string' } }, required: ['file_path'] }, isReadOnly: false, execute: async () => ({ output: '', isError: false }) },
];

const ctx: ToolContext = { cwd: '/tmp', abortSignal: new AbortController().signal };

describe('tool_search surfaces each tool category (e2e: discovery → payload)', () => {
  beforeEach(() => { setSearchableTools(mockTools); });

  it('tags an execution tool with its human-readable category', async () => {
    const result = await toolSearchTool.execute({ query: 'bash' }, ctx);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    // RED on revert: toolPayload had no `category` field before this change.
    assert.equal(parsed.category, 'Execution tools');
  });

  it('tags a read-only tool', async () => {
    const result = await toolSearchTool.execute({ query: 'file_read' }, ctx);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.category, 'Read-only tools');
  });

  it('tags an edit tool', async () => {
    const result = await toolSearchTool.execute({ query: 'file_edit' }, ctx);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.category, 'Edit tools');
  });
});
