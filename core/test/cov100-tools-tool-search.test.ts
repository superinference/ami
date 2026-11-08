import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { toolSearchTool, setSearchableTools } from '../src/tools/tool-search';
import type { ToolDefinition, ToolContext } from '../src/types';

// Minimal searchable tool set used to drive the select: path.
const tools: ToolDefinition[] = [
  {
    name: 'bash',
    description: 'Execute shell commands',
    inputSchema: { type: 'object', properties: { command: { type: 'string' } }, required: ['command'] },
    isReadOnly: false,
    isConcurrencySafe: false,
    execute: async () => ({ output: '', isError: false }),
  },
];

describe('tool_search — _loadDeferredTool defined branch (branch coverage)', () => {
  beforeEach(() => { setSearchableTools(tools); });

  it('invokes context._loadDeferredTool with the resolved tool name on select: hit', async () => {
    // The existing suite only exercises the short-circuit (undefined) arm of
    // `context._loadDeferredTool?.(name)`. Here we supply the callback so the
    // defined arm runs and we can assert it is called correctly.
    const loaded: string[] = [];
    const ctx: ToolContext = {
      cwd: '/tmp',
      abortSignal: new AbortController().signal,
      _loadDeferredTool: (name: string) => { loaded.push(name); },
    };

    const result = await toolSearchTool.execute({ query: 'select:bash' }, ctx);

    // The deferred-tool callback must have been invoked exactly once with the
    // exact tool name (not the raw query, not a trimmed-with-prefix value).
    assert.deepEqual(loaded, ['bash']);

    // And the normal schema payload is still returned unchanged.
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.name, 'bash');
    assert.ok(parsed.inputSchema);
    assert.equal(parsed.isReadOnly, false);
  });

  it('passes the trimmed name (not surrounding whitespace) to _loadDeferredTool', async () => {
    const loaded: string[] = [];
    const ctx: ToolContext = {
      cwd: '/tmp',
      abortSignal: new AbortController().signal,
      _loadDeferredTool: (name: string) => { loaded.push(name); },
    };

    const result = await toolSearchTool.execute({ query: 'select:   bash   ' }, ctx);

    assert.deepEqual(loaded, ['bash']);
    assert.equal(result.isError, false);
  });

  it('does NOT invoke _loadDeferredTool when the select: target is not a tool', async () => {
    // Guards the ordering: the callback must only fire inside the tool-found
    // branch, never for an unknown name.
    const loaded: string[] = [];
    const ctx: ToolContext = {
      cwd: '/tmp',
      abortSignal: new AbortController().signal,
      _loadDeferredTool: (name: string) => { loaded.push(name); },
    };

    const result = await toolSearchTool.execute({ query: 'select:nope' }, ctx);

    assert.deepEqual(loaded, []);
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not found'));
  });
});
