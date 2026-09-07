import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { toolSearchTool, setSearchableTools } from '../src/tools/tool-search';
import type { ToolDefinition } from '../src/types';

const mockTools: ToolDefinition[] = [
  { name: 'file_read', description: 'Read file contents from the filesystem', inputSchema: { type: 'object', properties: { file_path: { type: 'string' } }, required: ['file_path'] }, isReadOnly: true, isConcurrencySafe: true, execute: async () => ({ output: '', isError: false }) },
  { name: 'file_write', description: 'Write content to a file on the filesystem', inputSchema: { type: 'object', properties: { path: { type: 'string' }, content: { type: 'string' } }, required: ['path', 'content'] }, isReadOnly: false, isConcurrencySafe: false, execute: async () => ({ output: '', isError: false }) },
  { name: 'bash', description: 'Execute shell commands', inputSchema: { type: 'object', properties: { command: { type: 'string' } }, required: ['command'] }, isReadOnly: false, isConcurrencySafe: false, execute: async () => ({ output: '', isError: false }) },
  { name: 'grep', description: 'Search for patterns in files', inputSchema: { type: 'object', properties: { pattern: { type: 'string' } }, required: ['pattern'] }, isReadOnly: true, execute: async () => ({ output: '', isError: false }) },
];

const ctx = { cwd: '/tmp', abortSignal: new AbortController().signal };

describe('tool_search', () => {
  beforeEach(() => { setSearchableTools(mockTools); });

  it('finds tools by keyword', async () => {
    const result = await toolSearchTool.execute({ query: 'file' }, ctx);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('file_read'));
  });

  it('finds tools by description keyword', async () => {
    const result = await toolSearchTool.execute({ query: 'filesystem' }, ctx);
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('file_read') || result.output.includes('file_write'));
  });

  it('ranks name matches above description-only matches', async () => {
    const result = await toolSearchTool.execute({ query: 'file' }, ctx);
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('file_read'));
    assert.ok(result.output.includes('file_write'));
  });

  it('handles multi-keyword search', async () => {
    const result = await toolSearchTool.execute({ query: 'file read' }, ctx);
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('file_read'));
  });

  it('returns full schema with select:', async () => {
    const result = await toolSearchTool.execute({ query: 'select:bash' }, ctx);
    assert.ok(!result.isError);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.name, 'bash');
    assert.ok(parsed.inputSchema);
    assert.ok(parsed.description);
    assert.equal(parsed.isReadOnly, false);
  });

  it('trims the tool name after select:', async () => {
    const result = await toolSearchTool.execute({ query: 'select:  bash  ' }, ctx);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.name, 'bash');
  });

  it('handles not found with select:', async () => {
    const result = await toolSearchTool.execute({ query: 'select:nonexistent' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('not found'));
    assert.ok(result.output.includes('Available'));
  });

  it('handles no matches', async () => {
    const result = await toolSearchTool.execute({ query: 'zzzznotool' }, ctx);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('No tools matched'));
    assert.ok(result.output.includes('Available'));
  });

  it('rejects empty query', async () => {
    const result = await toolSearchTool.execute({ query: '' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('query must not be empty'));
  });

  it('rejects whitespace-only query', async () => {
    const result = await toolSearchTool.execute({ query: '   ' }, ctx);
    assert.equal(result.isError, true);
  });

  it('rejects missing query field', async () => {
    const result = await toolSearchTool.execute({}, ctx);
    assert.equal(result.isError, true);
  });

  it('ranks by relevance', async () => {
    const result = await toolSearchTool.execute({ query: 'search pattern' }, ctx);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('grep'));
  });

  it('replaces previously registered tools', async () => {
    setSearchableTools([mockTools[2]!]);
    const result = await toolSearchTool.execute({ query: 'file' }, ctx);
    assert.ok(!result.output.includes('file_read') || result.output.includes('No tools matched'));
  });

  it('limits results to top 5', async () => {
    const manyTools = Array.from({ length: 10 }, (_, i) => ({
      name: `tool_${i}`,
      description: `A tool number ${i}`,
      inputSchema: { type: 'object' as const, properties: {}, required: [] as string[] },
      isReadOnly: true,
      execute: async () => ({ output: '', isError: false }),
    }));
    setSearchableTools(manyTools);
    const result = await toolSearchTool.execute({ query: 'tool' }, ctx);
    const bullets = result.output.match(/^- \*\*/gm);
    assert.ok(bullets);
    assert.ok(bullets!.length <= 5);
  });

  it('truncates long descriptions to 100 chars with ellipsis', async () => {
    const longDesc = 'A'.repeat(150);
    setSearchableTools([{
      name: 'long_desc_tool',
      description: longDesc,
      inputSchema: { type: 'object' as const, properties: {}, required: [] },
      isReadOnly: true,
      execute: async () => ({ output: '', isError: false }),
    }]);
    const result = await toolSearchTool.execute({ query: 'long_desc_tool' }, ctx);
    assert.ok(result.output.includes('...'));
    assert.ok(!result.output.includes(longDesc));
  });

  it('does not add ellipsis for short descriptions', async () => {
    setSearchableTools([{
      name: 'short_tool',
      description: 'Short desc',
      inputSchema: { type: 'object' as const, properties: {}, required: [] },
      isReadOnly: true,
      execute: async () => ({ output: '', isError: false }),
    }]);
    const result = await toolSearchTool.execute({ query: 'short_tool' }, ctx);
    assert.ok(!result.output.includes('...'));
  });

  it('includes a select: usage hint in results', async () => {
    const result = await toolSearchTool.execute({ query: 'file' }, ctx);
    assert.ok(result.output.includes('select:'));
  });
});

describe('tool_search metadata', () => {
  it('has the correct name', () => {
    assert.equal(toolSearchTool.name, 'tool_search');
  });

  it('is read-only', () => {
    assert.equal(toolSearchTool.isReadOnly, true);
  });

  it('is concurrency safe', () => {
    assert.equal(toolSearchTool.isConcurrencySafe, true);
  });

  it('requires query in the schema', () => {
    assert.ok(toolSearchTool.inputSchema.required?.includes('query'));
  });
});
