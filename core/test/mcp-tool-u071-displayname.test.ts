import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { getMcpToolDisplayName } from '../src/tools/mcp-tool';

// U071 (continuation) — user-facing MCP tool display name from the `title`
// annotation, mirroring the reference's userFacingName (client.ts:L1972-1976):
// `<server> - <title||name> (MCP)`.
describe('getMcpToolDisplayName', () => {
  it('prefers the annotation title over the raw tool name', () => {
    assert.equal(
      getMcpToolDisplayName('github', 'create_issue', { title: 'Create Issue' }),
      'github - Create Issue (MCP)',
    );
  });

  it('falls back to the raw tool name when no annotations are given', () => {
    assert.equal(
      getMcpToolDisplayName('github', 'create_issue'),
      'github - create_issue (MCP)',
    );
  });

  it('falls back to the raw tool name when the title is missing or blank', () => {
    assert.equal(
      getMcpToolDisplayName('srv', 'get_x', {}),
      'srv - get_x (MCP)',
    );
    assert.equal(
      getMcpToolDisplayName('srv', 'get_x', { title: '   ' }),
      'srv - get_x (MCP)',
    );
  });

  it('trims surrounding whitespace from a real title', () => {
    assert.equal(
      getMcpToolDisplayName('linear', 'list_issues', { title: '  My Issues  ' }),
      'linear - My Issues (MCP)',
    );
  });
});
