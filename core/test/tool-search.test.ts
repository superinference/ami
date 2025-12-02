import { describe, it, before } from 'node:test';
import * as assert from 'node:assert/strict';
import { toolSearchTool, setSearchableTools } from '../src/tools/tool-search';
import type { ToolDefinition } from '../src/types';

const mockTools: ToolDefinition[] = [
  { name: 'file_read', description: 'Read file contents', inputSchema: { type: 'object', properties: { file_path: { type: 'string' } }, required: ['file_path'] }, isReadOnly: true, execute: async () => ({ output: '', isError: false }) },
  { name: 'bash', description: 'Execute shell commands', inputSchema: { type: 'object', properties: { command: { type: 'string' } }, required: ['command'] }, isReadOnly: false, execute: async () => ({ output: '', isError: false }) },
  { name: 'grep', description: 'Search for patterns in files', inputSchema: { type: 'object', properties: { pattern: { type: 'string' } }, required: ['pattern'] }, isReadOnly: true, execute: async () => ({ output: '', isError: false }) },
];

const ctx = { cwd: '/tmp', abortSignal: new AbortController().signal };

describe('tool_search', () => {
  before(() => { setSearchableTools(mockTools); });

  it('finds tools by keyword', async () => {
    const result = await toolSearchTool.execute({ query: 'file' }, ctx);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('file_read'));
  });

  it('returns full schema with select:', async () => {
    const result = await toolSearchTool.execute({ query: 'select:bash' }, ctx);
    assert.ok(!result.isError);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.name, 'bash');
    assert.ok(parsed.inputSchema);
  });

  it('handles not found with select:', async () => {
    const result = await toolSearchTool.execute({ query: 'select:nonexistent' }, ctx);
    assert.ok(result.isError);
  });

  it('handles no matches', async () => {
    const result = await toolSearchTool.execute({ query: 'zzzznotool' }, ctx);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('No tools matched'));
  });

  it('rejects empty query', async () => {
    const result = await toolSearchTool.execute({ query: '' }, ctx);
    assert.ok(result.isError);
  });

  it('ranks by relevance', async () => {
    const result = await toolSearchTool.execute({ query: 'search pattern' }, ctx);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('grep'));
  });
});
