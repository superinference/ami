import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { createMcpTool, readMcpToolMeta } from '../src/tools/mcp-tool';

// ---------------------------------------------------------------------------
// U071 — per-MCP-tool `_meta` discovery hints (Claude Code parity,
// services/mcp/client.ts:L1779-L1785):
//   * anthropic/searchHint  -> ToolDefinition.searchHint (whitespace-collapsed)
//   * anthropic/alwaysLoad  -> ToolDefinition.alwaysLoad (strict === true)
// These wire an MCP server's self-declared discovery hints into AMI's
// tool_search weighting + schema-size deferral opt-out.
// ---------------------------------------------------------------------------

describe('readMcpToolMeta', () => {
  it('collapses internal whitespace and trims the searchHint', () => {
    const { searchHint } = readMcpToolMeta({
      'anthropic/searchHint': '  find   repos\n and  issues \t',
    });
    assert.equal(searchHint, 'find repos and issues');
  });

  it('treats a whitespace-only searchHint as undefined', () => {
    assert.equal(readMcpToolMeta({ 'anthropic/searchHint': '   \n\t ' }).searchHint, undefined);
  });

  it('ignores a non-string searchHint', () => {
    assert.equal(readMcpToolMeta({ 'anthropic/searchHint': 42 as unknown as string }).searchHint, undefined);
  });

  it('returns undefined searchHint and false alwaysLoad for absent meta', () => {
    assert.deepEqual(readMcpToolMeta(undefined), { searchHint: undefined, alwaysLoad: false });
    assert.deepEqual(readMcpToolMeta({}), { searchHint: undefined, alwaysLoad: false });
  });

  it('sets alwaysLoad only for a strict boolean true', () => {
    assert.equal(readMcpToolMeta({ 'anthropic/alwaysLoad': true }).alwaysLoad, true);
    assert.equal(readMcpToolMeta({ 'anthropic/alwaysLoad': 'true' as unknown as boolean }).alwaysLoad, false);
    assert.equal(readMcpToolMeta({ 'anthropic/alwaysLoad': 1 as unknown as boolean }).alwaysLoad, false);
  });
});

describe('createMcpTool _meta discovery hints', () => {
  it('surfaces a searchHint from _meta on the tool definition', () => {
    const tool = createMcpTool('gh', 'search_repos', 'desc', {}, undefined, {
      'anthropic/searchHint': 'find   repositories',
    });
    assert.equal(tool.searchHint, 'find repositories');
  });

  it('strips invisible unicode smuggled into the searchHint', () => {
    const tool = createMcpTool('gh', 'search_repos', 'desc', {}, undefined, {
      // U+200B zero-width space between words must not survive into the hint.
      'anthropic/searchHint': 'find​repos',
    });
    assert.equal(tool.searchHint, 'findrepos');
  });

  it('marks alwaysLoad when _meta opts out of deferral', () => {
    const tool = createMcpTool('gh', 'critical', 'desc', {}, undefined, {
      'anthropic/alwaysLoad': true,
    });
    assert.equal(tool.alwaysLoad, true);
  });

  it('omits searchHint/alwaysLoad entirely when _meta carries neither', () => {
    const tool = createMcpTool('gh', 'plain', 'desc', {});
    assert.equal(tool.searchHint, undefined);
    assert.equal(tool.alwaysLoad, undefined);
    assert.ok(!('searchHint' in tool), 'searchHint key should be absent');
    assert.ok(!('alwaysLoad' in tool), 'alwaysLoad key should be absent');
  });
});
