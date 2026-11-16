// U004 — formatDeferredToolLine: render one deferred tool as a roster line so the
// model knows the exact name to pass to tool_search select:<name> (parity:
// Claude's formatDeferredToolLine, tools/ToolSearchTool/prompt.ts L115).
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { formatDeferredToolLine } from '../src/tools/tool-search';
import type { ToolDefinition } from '../src/types';

function tool(name: string): ToolDefinition {
  return {
    name,
    description: `desc for ${name}`,
    inputSchema: { type: 'object', properties: {} },
    handler: async () => ({ output: '' }),
  } as ToolDefinition;
}

describe('formatDeferredToolLine (U004)', () => {
  it('renders the bare tool name (no search hints)', () => {
    assert.equal(formatDeferredToolLine(tool('web_fetch')), 'web_fetch');
    assert.equal(formatDeferredToolLine(tool('notebook_edit')), 'notebook_edit');
  });

  it('produces a stable sortable roster for a deferred set', () => {
    const names = [tool('web_search'), tool('cron_create'), tool('web_fetch')]
      .map(formatDeferredToolLine)
      .sort();
    assert.deepEqual(names, ['cron_create', 'web_fetch', 'web_search']);
  });
});
