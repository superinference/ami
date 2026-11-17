import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { toolSearchTool, setSearchableTools } from '../src/tools/tool-search';
import type { ToolDefinition, ToolContext } from '../src/types';

function mk(name: string, description: string, extra: Partial<ToolDefinition> = {}): ToolDefinition {
  return {
    name,
    description,
    inputSchema: { type: 'object', properties: {} },
    execute: async () => ({ output: '', isError: false }),
    ...extra,
  };
}

const mockTools: ToolDefinition[] = [
  mk('file_read', 'Read deploy notes from the filesystem', { isReadOnly: true }),
  mk('bash', 'Execute deploy shell commands'),
  mk('mcp__slack__post_deploy', 'Post a deploy message to slack', { isMcp: true, isReadOnly: false }),
  mk('mcp__github__deploy_pr', 'Open a deploy pull request on github', { isMcp: true, isReadOnly: false }),
  mk('mcp__slack__dm_deploy', 'Direct-message a deploy update on slack', { isMcp: true, isReadOnly: false }),
];

const ctx: ToolContext = { cwd: '/tmp', abortSignal: new AbortController().signal };

describe('tool_search surfaces MCP-server + category buckets in metadata (U011 gap10)', () => {
  beforeEach(() => { setSearchableTools(mockTools); });

  it('groups matched MCP tools by server (sorted) in mcp_servers metadata', async () => {
    const result = await toolSearchTool.execute({ query: 'deploy', max_results: 50 }, ctx);
    assert.equal(result.isError, false);
    // RED on revert: mcp_servers did not exist in the keyword-search metadata before U011.
    const servers = (result.metadata as any)?.mcp_servers as Array<{ server: string; tools: string[] }>;
    assert.ok(Array.isArray(servers), 'mcp_servers should be present when MCP tools match');
    assert.deepEqual(servers.map(s => s.server), ['github', 'slack']); // localeCompare sorted
    const slack = servers.find(s => s.server === 'slack')!;
    assert.deepEqual(slack.tools, ['mcp__slack__dm_deploy', 'mcp__slack__post_deploy']); // tool-name sorted
  });

  it('reports capability-class counts of matched tools in tool_categories metadata', async () => {
    const result = await toolSearchTool.execute({ query: 'deploy', max_results: 50 }, ctx);
    const cats = (result.metadata as any)?.tool_categories as Record<string, number>;
    assert.ok(cats, 'tool_categories should be present');
    assert.equal(cats.mcp, 3);
    assert.equal(cats['read-only'], 1);
    assert.equal(cats.execution, 1);
  });

  it('omits mcp_servers when no MCP tool matches', async () => {
    const result = await toolSearchTool.execute({ query: 'read', max_results: 50 }, ctx);
    assert.equal((result.metadata as any)?.mcp_servers, undefined);
  });
});
