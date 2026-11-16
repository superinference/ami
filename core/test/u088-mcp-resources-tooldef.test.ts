// Unit U088 — MCP resource tool-definition parity: searchHint (findability via
// tool_search) and maxResultSizeChars (a declared, consumed result-size knob)
// mirroring Claude's ListMcpResourcesTool / ReadMcpResourceTool definitions.
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { listMcpResourcesTool, readMcpResourceTool } from '../src/tools/mcp-resources';
import { toolSearchTool, setSearchableTools } from '../src/tools/tool-search';
import type { ToolContext } from '../src/types';
import { projectStateDir } from '../src/reference-journal';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: '/tmp', abortSignal: new AbortController().signal, ...overrides } as ToolContext;
}
function fakeManager(impl: Record<string, unknown>): ToolContext {
  return ctx({ _mcpManager: impl as unknown as ToolContext['_mcpManager'] });
}

describe('U088 — MCP resource tools expose a searchHint consumed by tool_search', () => {
  it('declares a non-empty searchHint on both resource tools', () => {
    assert.ok(listMcpResourcesTool.searchHint && listMcpResourcesTool.searchHint.length > 0);
    assert.ok(readMcpResourceTool.searchHint && readMcpResourceTool.searchHint.length > 0);
  });

  it('finds ReadMcpResource via a synonym that only lives in its searchHint', async () => {
    setSearchableTools([listMcpResourcesTool, readMcpResourceTool]);
    // "download" is NOT in ReadMcpResource's name or description — only its searchHint.
    assert.ok(!/download/i.test(`${readMcpResourceTool.name} ${readMcpResourceTool.description}`));
    const res = await toolSearchTool.execute!({ query: 'download' }, ctx());
    assert.equal(res.isError, false);
    assert.match(res.output, /ReadMcpResource/);
  });

  it('finds ListMcpResources via a synonym that only lives in its searchHint', async () => {
    setSearchableTools([listMcpResourcesTool, readMcpResourceTool]);
    // "inventory" is NOT in ListMcpResources' name or description — only its searchHint.
    assert.ok(!/inventory/i.test(`${listMcpResourcesTool.name} ${listMcpResourcesTool.description}`));
    const res = await toolSearchTool.execute!({ query: 'inventory' }, ctx());
    assert.equal(res.isError, false);
    assert.match(res.output, /ListMcpResources/);
  });
});

describe('U088 — maxResultSizeChars is a declared knob that drives the spill cap', () => {
  it('declares maxResultSizeChars: 100_000 on both resource tools (Claude parity)', () => {
    assert.equal(listMcpResourcesTool.maxResultSizeChars, 100_000);
    assert.equal(readMcpResourceTool.maxResultSizeChars, 100_000);
  });

  it('honors the list tool declared cap when spilling (consumed, not hardcoded)', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'u088-list-'));
    const orig = listMcpResourcesTool.maxResultSizeChars;
    try {
      // Shrink the declared cap far below 100k; a small catalog must now spill.
      listMcpResourcesTool.maxResultSizeChars = 40;
      const resources = Array.from({ length: 20 }, (_, i) => ({
        uri: `res://${i}`, name: `name-${i}`, mimeType: 'text/plain',
      }));
      const context = fakeManager({ listResources: async () => resources });
      const res = await listMcpResourcesTool.execute!(
        {},
        ctx({ cwd: tmp, _mcpManager: (context as ToolContext)._mcpManager }),
      );
      // If the cap were the hardcoded 100_000, a sub-100k listing would NOT spill.
      assert.match(res.output, /persisted to/);
      const spillDir = projectStateDir(tmp, 'tool-results');
      assert.ok(fs.readdirSync(spillDir).length >= 1, 'a spill file should exist');
    } finally {
      listMcpResourcesTool.maxResultSizeChars = orig;
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('honors the read tool declared cap when spilling (consumed, not hardcoded)', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'u088-read-'));
    const orig = readMcpResourceTool.maxResultSizeChars;
    try {
      readMcpResourceTool.maxResultSizeChars = 30;
      const context = fakeManager({ readResource: async () => 'x'.repeat(200) });
      const res = await readMcpResourceTool.execute!(
        { server: 's', uri: 'u' },
        ctx({ cwd: tmp, _mcpManager: (context as ToolContext)._mcpManager }),
      );
      assert.match(res.output, /persisted to/);
    } finally {
      readMcpResourceTool.maxResultSizeChars = orig;
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});
