import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { createMcpTool } from '../src/tools/mcp-tool';
import { webFetchTool } from '../src/tools/web-fetch';
import { webSearchTool } from '../src/tools/web-search';
import { fileReadTool } from '../src/tools/file-read';

// ---------------------------------------------------------------------------
// U006 — `ToolDefinition.isOpenWorld` (the reference parity, MCPTool.ts:L30-32).
// Marks tools whose effects reach OUTSIDE the local env / are non-deterministic.
// Distinct from `isReadOnly`: a read-only tool (remote fetch / web search) can
// still be open-world. Sourced from an MCP server's `annotations.openWorldHint`
// and set statically on the network-touching native tools.
// ---------------------------------------------------------------------------

describe('ToolDefinition.isOpenWorld — MCP annotations', () => {
  it('sets isOpenWorld when the server declares openWorldHint: true', () => {
    const tool = createMcpTool('gh', 'create_issue', 'desc', {}, { openWorldHint: true });
    assert.equal(tool.isOpenWorld, true);
  });

  it('leaves isOpenWorld undefined when openWorldHint is false', () => {
    const tool = createMcpTool('gh', 'closed', 'desc', {}, { openWorldHint: false });
    assert.equal(tool.isOpenWorld, undefined);
  });

  it('leaves isOpenWorld undefined when openWorldHint is absent', () => {
    const tool = createMcpTool('gh', 'plain', 'desc', {});
    assert.equal(tool.isOpenWorld, undefined);
  });

  it('is independent of readOnlyHint — a read-only tool can be open-world', () => {
    const tool = createMcpTool('gh', 'remote_read', 'desc', {}, {
      readOnlyHint: true,
      openWorldHint: true,
    });
    assert.equal(tool.isReadOnly, true);
    assert.equal(tool.isOpenWorld, true);
  });

  it('only sets isOpenWorld for a strict boolean true (not a truthy string)', () => {
    const tool = createMcpTool('gh', 'typo', 'desc', {}, {
      openWorldHint: 'true' as unknown as boolean,
    });
    assert.equal(tool.isOpenWorld, undefined);
  });
});

describe('ToolDefinition.isOpenWorld — native tools', () => {
  it('web_fetch is read-only yet open-world', () => {
    assert.equal(webFetchTool.isReadOnly, true);
    assert.equal(webFetchTool.isOpenWorld, true);
  });

  it('web_search is read-only yet open-world', () => {
    assert.equal(webSearchTool.isReadOnly, true);
    assert.equal(webSearchTool.isOpenWorld, true);
  });

  it('a local read-only tool (file_read) is NOT open-world', () => {
    assert.equal(fileReadTool.isReadOnly, true);
    assert.equal(fileReadTool.isOpenWorld, undefined);
  });
});
