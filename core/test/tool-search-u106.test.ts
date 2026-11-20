import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { toolSearchTool, setSearchableTools } from '../src/tools/tool-search';
import type { ToolDefinition, ToolContext } from '../src/types';
import type { SkillDefinition, SkillManagerLike } from '../src/skills';

// Unit U106 — parity gaps on core/core/src/tools/tool-search.ts:
//  A) a `select:` that resolves nothing while MCP servers are still connecting
//     is NOT a hard error (Claude's buildSearchResult returns data, not an
//     error, so the model retries instead of treating it as fatal).
//  B) a required `+term` matches against the description / searchHint / whenToUse,
//     not only the name (Claude pre-filters on name-parts OR description OR hint).
//  D) a term that matches a tool's curated `searchHint` ranks ABOVE a term that
//     only matches a description (the reference weights hint +4 vs description +2).

const base: ToolDefinition[] = [
  { name: 'bash', description: 'Execute shell commands', inputSchema: { type: 'object', properties: {}, required: [] }, isReadOnly: false, execute: async () => ({ output: '', isError: false }) },
];

const ctx: ToolContext = { cwd: '/tmp', abortSignal: new AbortController().signal };

const pendingMgr = (servers: Array<{ name: string; state: string }>) =>
  ({ listAllServers: () => servers }) as any;

function mkSkillMgr(skills: SkillDefinition[]): SkillManagerLike {
  return {
    getSkill(name: string) { return skills.find(s => s.name === name) || null; },
    getSkillContent() { return null; },
    listSkills() { return skills; },
    getAgent() { return null; },
    listAgents() { return []; },
  };
}

describe('U106: select-miss while MCP servers pending is not an error', () => {
  beforeEach(() => { setSearchableTools(base); });

  it('single-name select-miss returns isError:false when a server is pending', async () => {
    const result = await toolSearchTool.execute(
      { query: 'select:nope' },
      { ...ctx, _mcpManager: pendingMgr([{ name: 'kali', state: 'connecting' }]) },
    );
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('kali'));
    assert.deepEqual(result.metadata?.pending_mcp_servers, ['kali']);
  });

  it('multi-name all-missing select returns isError:false when a server is pending', async () => {
    const result = await toolSearchTool.execute(
      { query: 'select:nope1, nope2' },
      { ...ctx, _mcpManager: pendingMgr([{ name: 'slack', state: 'pending' }]) },
    );
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('slack'));
    assert.deepEqual(result.metadata?.pending_mcp_servers, ['slack']);
  });

  it('still reports isError:true for a select-miss when NOTHING is pending', async () => {
    const result = await toolSearchTool.execute(
      { query: 'select:nope' },
      { ...ctx, _mcpManager: pendingMgr([{ name: 'github', state: 'ready' }]) },
    );
    assert.equal(result.isError, true);
  });

  it('still reports isError:true for a select-miss with no MCP manager', async () => {
    const result = await toolSearchTool.execute({ query: 'select:nope' }, ctx);
    assert.equal(result.isError, true);
  });
});

describe('U106: required +term matches description / searchHint / whenToUse', () => {
  it('a +term found only in the DESCRIPTION still matches', async () => {
    setSearchableTools([
      { name: 'bash', description: 'Execute shell commands', inputSchema: { type: 'object', properties: {}, required: [] }, isReadOnly: false, execute: async () => ({ output: '', isError: false }) },
    ]);
    // "shell" is in the description but NOT in the name "bash".
    const result = await toolSearchTool.execute({ query: '+shell' }, ctx);
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('Tools matching'), `expected a match, got:\n${result.output}`);
    assert.ok(result.output.includes('bash'));
  });

  it('a +term found only in the searchHint still matches', async () => {
    setSearchableTools([
      { name: 'notebook_edit', description: 'Edit a cell', searchHint: 'jupyter ipynb kernel', inputSchema: { type: 'object', properties: {}, required: [] }, isReadOnly: false, execute: async () => ({ output: '', isError: false }) },
    ]);
    const result = await toolSearchTool.execute({ query: '+jupyter' }, ctx);
    assert.equal(result.isError, false);
    assert.ok(!result.output.includes('No tools or skills matched'), `expected a real match, got:\n${result.output}`);
    assert.ok(result.output.includes('Tools matching'), `expected a match section, got:\n${result.output}`);
    assert.ok(result.output.includes('notebook_edit'));
  });

  it('a +term found only in a skill whenToUse still matches', async () => {
    setSearchableTools(base);
    const skills: SkillDefinition[] = [
      { name: 'research', description: 'Deep analysis stance', content: 'x', filePath: '/s/research/SKILL.md', whenToUse: 'When the user asks for investigation work' },
    ];
    const result = await toolSearchTool.execute(
      { query: '+investigation' },
      { ...ctx, _skillManager: mkSkillMgr(skills) },
    );
    assert.equal(result.isError, false);
    assert.ok(!result.output.includes('No tools or skills matched'), `expected a real match, got:\n${result.output}`);
    assert.ok(result.output.includes('Skills matching'), `expected a skill match section, got:\n${result.output}`);
    assert.ok(result.output.includes('research'));
  });

  it('a +term in NONE of name/description/hint is still filtered out', async () => {
    setSearchableTools([
      { name: 'bash', description: 'Execute shell commands', inputSchema: { type: 'object', properties: {}, required: [] }, isReadOnly: false, execute: async () => ({ output: '', isError: false }) },
    ]);
    const result = await toolSearchTool.execute({ query: '+kubernetes' }, ctx);
    assert.ok(result.output.includes('No tools or skills matched'));
  });
});

describe('U106: whole-word boundary matching (both edges) avoids prefix false-positives', () => {
  // Claude compiles `\bterm\b` (BOTH boundaries); a leading-only `\bterm` still
  // spuriously matches a longer word that merely STARTS with the term
  // (`cat` → `category`, `shell` → `shellac`). Parity requires both edges.
  it('a term does not match a longer description word it only prefixes', async () => {
    setSearchableTools([
      { name: 'browse', description: 'open the category index', inputSchema: { type: 'object', properties: {}, required: [] }, isReadOnly: true, execute: async () => ({ output: '', isError: false }) },
    ]);
    // "cat" prefixes "category" but is not a whole word there, and the name
    // "browse" has no "cat" either — so there must be NO match.
    const result = await toolSearchTool.execute({ query: 'cat' }, ctx);
    assert.ok(
      result.output.includes('No tools or skills matched'),
      `expected no match for prefix-only term, got:\n${result.output}`,
    );
  });

  it('a required +term does not match a searchHint word it only prefixes', async () => {
    setSearchableTools([
      { name: 'paint', description: 'draw things', searchHint: 'shellac varnish finish', inputSchema: { type: 'object', properties: {}, required: [] }, isReadOnly: true, execute: async () => ({ output: '', isError: false }) },
    ]);
    // "+shell" prefixes "shellac" in the hint but is not a whole word → filtered out.
    const result = await toolSearchTool.execute({ query: '+shell' }, ctx);
    assert.ok(
      result.output.includes('No tools or skills matched'),
      `expected +shell to NOT gate a hint containing only "shellac", got:\n${result.output}`,
    );
  });

  it('a whole-word term still matches normally', async () => {
    setSearchableTools([
      { name: 'browse', description: 'open the cat index', inputSchema: { type: 'object', properties: {}, required: [] }, isReadOnly: true, execute: async () => ({ output: '', isError: false }) },
    ]);
    const result = await toolSearchTool.execute({ query: 'cat' }, ctx);
    assert.ok(result.output.includes('Tools matching'), `expected a whole-word match, got:\n${result.output}`);
    assert.ok(result.output.includes('browse'));
  });
});

describe('U106: searchHint ranks above description', () => {
  it('a hint-only match outranks a description-only match for the same term', async () => {
    // descTool has "deploy" in its DESCRIPTION; hintTool has "deploy" only in
    // its searchHint. descTool is listed FIRST, so a stable sort would keep it
    // first on a tie — the hint tool can only come first if the hint is weighted
    // strictly higher than the description.
    setSearchableTools([
      { name: 'alpha_tool', description: 'helper that can deploy things', inputSchema: { type: 'object', properties: {}, required: [] }, isReadOnly: true, execute: async () => ({ output: '', isError: false }) },
      { name: 'beta_tool', description: 'a generic helper', searchHint: 'deploy release ship', inputSchema: { type: 'object', properties: {}, required: [] }, isReadOnly: true, execute: async () => ({ output: '', isError: false }) },
    ]);
    const result = await toolSearchTool.execute({ query: 'deploy' }, ctx);
    assert.equal(result.isError, false);
    assert.ok(
      result.output.indexOf('beta_tool') < result.output.indexOf('alpha_tool'),
      `expected searchHint tool (beta_tool) ranked above description-only tool (alpha_tool):\n${result.output}`,
    );
  });
});
