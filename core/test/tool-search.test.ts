import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { toolSearchTool, setSearchableTools } from '../src/tools/tool-search';
import type { ToolDefinition, ToolContext } from '../src/types';
import type { SkillDefinition, SkillManagerLike } from '../src/skills';

const mockTools: ToolDefinition[] = [
  { name: 'file_read', description: 'Read file contents from the filesystem', inputSchema: { type: 'object', properties: { file_path: { type: 'string' } }, required: ['file_path'] }, isReadOnly: true, isConcurrencySafe: true, execute: async () => ({ output: '', isError: false }) },
  { name: 'file_write', description: 'Write content to a file on the filesystem', inputSchema: { type: 'object', properties: { path: { type: 'string' }, content: { type: 'string' } }, required: ['path', 'content'] }, isReadOnly: false, isConcurrencySafe: false, execute: async () => ({ output: '', isError: false }) },
  { name: 'bash', description: 'Execute shell commands', inputSchema: { type: 'object', properties: { command: { type: 'string' } }, required: ['command'] }, isReadOnly: false, isConcurrencySafe: false, execute: async () => ({ output: '', isError: false }) },
  { name: 'grep', description: 'Search for patterns in files', inputSchema: { type: 'object', properties: { pattern: { type: 'string' } }, required: ['pattern'] }, isReadOnly: true, execute: async () => ({ output: '', isError: false }) },
];

const mockSkills: SkillDefinition[] = [
  { name: 'research', description: 'Deep research and analysis stance with web search capabilities', content: 'Research instructions', filePath: '/skills/research/SKILL.md', whenToUse: 'When the user asks for research or investigation', autoAllowPatterns: ['curl *'], defaultThinkingLevel: 'high' as any },
  { name: 'pentest', description: 'Penetration testing stance with Kali MCP server', content: 'Pentest instructions', filePath: '/skills/pentest/SKILL.md', mcpServers: { kali: { command: 'kali-mcp', args: [] } as any } },
  { name: 'code-review', description: 'Review code for bugs and improvements', content: 'Code review instructions', filePath: '/skills/code-review/SKILL.md' },
  { name: 'explain', description: 'Explain code or concepts clearly', content: 'Explain instructions', filePath: '/skills/explain/SKILL.md', whenToUse: 'When the user asks to explain something' },
];

function createMockSkillManager(skills: SkillDefinition[] = mockSkills): SkillManagerLike {
  return {
    getSkill(name: string) { return skills.find(s => s.name === name) || null; },
    getSkillContent(name: string) { const s = skills.find(sk => sk.name === name); return s ? s.content : null; },
    listSkills() { return skills; },
    getAgent() { return null; },
    listAgents() { return []; },
  };
}

const ctx: ToolContext = { cwd: '/tmp', abortSignal: new AbortController().signal };
const ctxWithSkills: ToolContext = { ...ctx, _skillManager: createMockSkillManager() };

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
  });

  it('handles no matches', async () => {
    const result = await toolSearchTool.execute({ query: 'zzzznotool' }, ctx);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('No tools'));
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
    assert.ok(!result.output.includes('file_read') || result.output.includes('No tools'));
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
    const result = await toolSearchTool.execute({ query: 'long_desc' }, ctx);
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
    const result = await toolSearchTool.execute({ query: 'short' }, ctx);
    assert.ok(!result.output.includes('...'));
  });

  it('includes a select: usage hint in results', async () => {
    const result = await toolSearchTool.execute({ query: 'file' }, ctx);
    assert.ok(result.output.includes('select:'));
  });
});

describe('tool_search with skills', () => {
  beforeEach(() => { setSearchableTools(mockTools); });

  it('finds skills by keyword alongside tools', async () => {
    const result = await toolSearchTool.execute({ query: 'research' }, ctxWithSkills);
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('Skills matching'));
    assert.ok(result.output.includes('research'));
  });

  it('returns skill details with select: when tool not found', async () => {
    const result = await toolSearchTool.execute({ query: 'select:research' }, ctxWithSkills);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.type, 'skill');
    assert.equal(parsed.name, 'research');
    assert.equal(parsed.isStance, true);
    assert.ok(parsed.activation.includes('skill({skill: "research"})'));
  });

  it('prefers tool over skill for select: when both match', async () => {
    const skillManager = createMockSkillManager([
      { name: 'bash', description: 'A skill also named bash', content: '', filePath: 'x' },
    ]);
    const result = await toolSearchTool.execute(
      { query: 'select:bash' },
      { ...ctx, _skillManager: skillManager },
    );
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.ok(!parsed.type);
    assert.equal(parsed.name, 'bash');
    assert.ok(parsed.inputSchema);
  });

  it('shows stance tag for stance skills in keyword search', async () => {
    const result = await toolSearchTool.execute({ query: 'research' }, ctxWithSkills);
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('[stance]'));
    assert.ok(result.output.includes('Activate stance'));
  });

  it('shows regular activation for non-stance skills', async () => {
    const result = await toolSearchTool.execute({ query: 'explain' }, ctxWithSkills);
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('Run with: skill({skill: "explain"})'));
    assert.ok(!result.output.includes('[stance]'));
  });

  it('falls back to tool-only search when no _skillManager on context', async () => {
    const result = await toolSearchTool.execute({ query: 'research' }, ctx);
    assert.equal(result.isError, false);
    assert.ok(!result.output.includes('Skills matching'));
  });

  it('shows both tools and skills sections when both match', async () => {
    const skillManager = createMockSkillManager([
      { name: 'search-helper', description: 'Search and find things', content: '', filePath: 'x' },
    ]);
    const result = await toolSearchTool.execute(
      { query: 'search' },
      { ...ctx, _skillManager: skillManager },
    );
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('Tools matching'));
    assert.ok(result.output.includes('Skills matching'));
  });

  it('shows skill names in not-found error for select:', async () => {
    const result = await toolSearchTool.execute({ query: 'select:nonexistent' }, ctxWithSkills);
    assert.ok(result.isError);
    assert.ok(result.output.includes('Skills:'));
    assert.ok(result.output.includes('research'));
  });

  it('shows skill names in no-match message', async () => {
    const result = await toolSearchTool.execute({ query: 'zzzznothing' }, ctxWithSkills);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Skills:'));
    assert.ok(result.output.includes('research'));
  });

  it('matches skills by whenToUse field', async () => {
    const result = await toolSearchTool.execute({ query: 'investigation' }, ctxWithSkills);
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('research'));
  });

  it('limits skill results to top 5', async () => {
    const manySkills = Array.from({ length: 10 }, (_, i) => ({
      name: `skill_${i}`,
      description: `A skill about testing number ${i}`,
      content: '',
      filePath: `x${i}`,
    }));
    const result = await toolSearchTool.execute(
      { query: 'testing' },
      { ...ctx, _skillManager: createMockSkillManager(manySkills) },
    );
    const skillBullets = (result.output.match(/^- \*\*skill_/gm) || []);
    assert.ok(skillBullets.length <= 5);
  });

  it('truncates long skill descriptions with ellipsis', async () => {
    const longDesc = 'B'.repeat(150);
    const result = await toolSearchTool.execute(
      { query: 'long_skill' },
      { ...ctx, _skillManager: createMockSkillManager([{ name: 'long_skill', description: longDesc, content: '', filePath: 'x' }]) },
    );
    assert.ok(result.output.includes('...'));
    assert.ok(!result.output.includes(longDesc));
  });

  it('handles empty skill manager gracefully', async () => {
    const result = await toolSearchTool.execute(
      { query: 'file' },
      { ...ctx, _skillManager: createMockSkillManager([]) },
    );
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('file_read'));
  });

  it('select: returns whenToUse for skills that have it', async () => {
    const result = await toolSearchTool.execute({ query: 'select:explain' }, ctxWithSkills);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.whenToUse, 'When the user asks to explain something');
  });

  it('select: returns null whenToUse for skills without it', async () => {
    const result = await toolSearchTool.execute({ query: 'select:code-review' }, ctxWithSkills);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.whenToUse, null);
  });

  it('select: shows isStance false for non-stance skills', async () => {
    const result = await toolSearchTool.execute({ query: 'select:code-review' }, ctxWithSkills);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.isStance, false);
    assert.ok(parsed.activation.includes('Run with'));
  });

  it('select: shows isStance true for pentest (has mcpServers)', async () => {
    const result = await toolSearchTool.execute({ query: 'select:pentest' }, ctxWithSkills);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.isStance, true);
    assert.ok(parsed.activation.includes('Activate stance'));
  });

  it('shows (none) for skills when no skill manager and select: fails', async () => {
    const result = await toolSearchTool.execute({ query: 'select:nonexistent' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('Skills: (none)'));
  });
});

describe('tool_search — select: multi-name (M4a)', () => {
  beforeEach(() => { setSearchableTools(mockTools); });

  it('loads multiple tools with select:A,B', async () => {
    const loaded: string[] = [];
    const c: ToolContext = { ...ctx, _loadDeferredTool: (n: string) => { loaded.push(n); } };
    const result = await toolSearchTool.execute({ query: 'select:bash,grep' }, c);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.ok(Array.isArray(parsed.tools));
    assert.equal(parsed.tools.length, 2);
    assert.deepEqual(parsed.tools.map((t: any) => t.name), ['bash', 'grep']);
    assert.deepEqual(parsed.notFound, []);
    // Every matched tool must be loaded as a deferred tool.
    assert.deepEqual(loaded, ['bash', 'grep']);
  });

  it('still loads a single tool with select:A (unchanged shape)', async () => {
    const result = await toolSearchTool.execute({ query: 'select:bash' }, ctx);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    // Single select returns a bare payload, NOT the {tools,skills,notFound} wrapper.
    assert.equal(parsed.name, 'bash');
    assert.ok(parsed.inputSchema);
    assert.equal(parsed.tools, undefined);
  });

  it('trims whitespace and ignores empty names in a multi-select', async () => {
    const result = await toolSearchTool.execute({ query: 'select: bash , , grep , ' }, ctx);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.deepEqual(parsed.tools.map((t: any) => t.name), ['bash', 'grep']);
    assert.deepEqual(parsed.notFound, []);
  });

  it('dedupes repeated names (two uniques stay multi)', async () => {
    const result = await toolSearchTool.execute({ query: 'select:bash,bash,grep' }, ctx);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.tools.length, 2);
    assert.deepEqual(parsed.tools.map((t: any) => t.name), ['bash', 'grep']);
  });

  it('collapses to the single-name path when dupes reduce to one', async () => {
    const result = await toolSearchTool.execute({ query: 'select:bash,bash' }, ctx);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.name, 'bash');
    assert.equal(parsed.tools, undefined);
  });

  it('reports unknown names in notFound while still loading the known ones', async () => {
    const loaded: string[] = [];
    const c: ToolContext = { ...ctx, _loadDeferredTool: (n: string) => { loaded.push(n); } };
    const result = await toolSearchTool.execute({ query: 'select:bash,nope' }, c);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.deepEqual(parsed.tools.map((t: any) => t.name), ['bash']);
    assert.deepEqual(parsed.notFound, ['nope']);
    assert.deepEqual(loaded, ['bash']);
  });

  it('errors gracefully when every name in a multi-select is unknown', async () => {
    const result = await toolSearchTool.execute({ query: 'select:nope,nada' }, ctx);
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not found'));
    assert.ok(result.output.includes('"nope"'));
    assert.ok(result.output.includes('"nada"'));
  });

  it('resolves skills alongside tools in a multi-select', async () => {
    const result = await toolSearchTool.execute({ query: 'select:bash,research' }, ctxWithSkills);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.deepEqual(parsed.tools.map((t: any) => t.name), ['bash']);
    assert.equal(parsed.skills.length, 1);
    assert.equal(parsed.skills[0].name, 'research');
    assert.equal(parsed.skills[0].type, 'skill');
  });

  it('treats a bare select: (all-empty) as a single not-found', async () => {
    const result = await toolSearchTool.execute({ query: 'select:' }, ctx);
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not found'));
  });
});

describe('tool_search — +required keyword terms (M4b)', () => {
  beforeEach(() => { setSearchableTools(mockTools); });

  it('restricts matches to tools whose name contains the +term, ranked by the rest', async () => {
    const result = await toolSearchTool.execute({ query: '+file read' }, ctx);
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('file_read'));
    assert.ok(result.output.includes('file_write'));
    // bash/grep names lack "file" so they are excluded even if desc/rank match.
    assert.ok(!result.output.includes('**bash**'));
    assert.ok(!result.output.includes('**grep**'));
    // "read" ranks file_read above file_write.
    assert.ok(result.output.indexOf('file_read') < result.output.indexOf('file_write'));
  });

  it('returns no matches when no tool name contains the +term', async () => {
    const result = await toolSearchTool.execute({ query: '+zzznotaname read' }, ctx);
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('No tools or skills matched'));
  });

  it('surfaces name matches even with no plain ranking terms (+term only)', async () => {
    setSearchableTools([
      { name: 'slack_send', description: 'send a slack message', inputSchema: { type: 'object', properties: {}, required: [] }, isReadOnly: false, execute: async () => ({ output: '', isError: false }) },
      { name: 'email_send', description: 'send an email', inputSchema: { type: 'object', properties: {}, required: [] }, isReadOnly: false, execute: async () => ({ output: '', isError: false }) },
    ]);
    const result = await toolSearchTool.execute({ query: '+slack' }, ctx);
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('slack_send'));
    assert.ok(!result.output.includes('email_send'));
  });

  it('requires ALL +terms to appear in the name (multiple + terms)', async () => {
    setSearchableTools([
      { name: 'slack_send', description: 'send to slack', inputSchema: { type: 'object', properties: {}, required: [] }, isReadOnly: false, execute: async () => ({ output: '', isError: false }) },
      { name: 'slack_read', description: 'read from slack', inputSchema: { type: 'object', properties: {}, required: [] }, isReadOnly: true, execute: async () => ({ output: '', isError: false }) },
      { name: 'email_send', description: 'send an email', inputSchema: { type: 'object', properties: {}, required: [] }, isReadOnly: false, execute: async () => ({ output: '', isError: false }) },
    ]);
    const result = await toolSearchTool.execute({ query: '+slack +send' }, ctx);
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('slack_send'));
    assert.ok(!result.output.includes('slack_read'));
    assert.ok(!result.output.includes('email_send'));
  });

  it('applies the +term gate to skills by name too', async () => {
    const result = await toolSearchTool.execute({ query: '+research stance' }, ctxWithSkills);
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('research'));
    // "explain"/"pentest" names lack "research" so they are gated out.
    assert.ok(!result.output.includes('**explain**'));
    assert.ok(!result.output.includes('**pentest**'));
  });

  it('ignores a lone "+" term and ranks by the remaining plain terms', async () => {
    const result = await toolSearchTool.execute({ query: '+ file' }, ctx);
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('file_read'));
    assert.ok(result.output.includes('file_write'));
  });

  it('leaves plain (no-+) keyword ranking unchanged', async () => {
    const result = await toolSearchTool.execute({ query: 'file' }, ctx);
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('file_read'));
    assert.ok(result.output.includes('file_write'));
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

  it('description mentions skills', () => {
    assert.ok(toolSearchTool.description.includes('skill'));
  });
});

describe('tool_search — parity enhancements', () => {
  beforeEach(() => { setSearchableTools(mockTools); });

  it('documents every query form and max_results', () => {
    const d = toolSearchTool.description;
    assert.ok(d.includes('select:'));
    assert.ok(d.includes('+term'));
    assert.ok(d.includes('max_results'));
    assert.ok((toolSearchTool.inputSchema.properties as any).max_results);
  });

  it('exact bare tool name is a fast path that loads and returns the schema', async () => {
    const loaded: string[] = [];
    const c: ToolContext = { ...ctx, _loadDeferredTool: (n) => loaded.push(n) };
    const result = await toolSearchTool.execute({ query: 'bash' }, c);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.name, 'bash');
    assert.ok(parsed.inputSchema);
    assert.deepEqual(loaded, ['bash']);
  });

  it('a bare keyword that is not an exact tool name still does ranked search', async () => {
    const result = await toolSearchTool.execute({ query: 'file' }, ctx);
    assert.ok(result.output.includes('Tools matching'));
    assert.ok(result.output.includes('file_read'));
  });

  it('max_results bounds the number of matches', async () => {
    const many = Array.from({ length: 8 }, (_, i) => ({
      name: `widget_${i}`, description: 'a widget tool',
      inputSchema: { type: 'object' as const, properties: {}, required: [] as string[] },
      isReadOnly: true, execute: async () => ({ output: '', isError: false }),
    }));
    setSearchableTools(many);
    const result = await toolSearchTool.execute({ query: 'widget', max_results: 2 }, ctx);
    const bullets = result.output.match(/^- \*\*/gm) || [];
    assert.equal(bullets.length, 2);
  });

  it('invalid max_results falls back to the default of 5', async () => {
    const many = Array.from({ length: 8 }, (_, i) => ({
      name: `gadget_${i}`, description: 'a gadget tool',
      inputSchema: { type: 'object' as const, properties: {}, required: [] as string[] },
      isReadOnly: true, execute: async () => ({ output: '', isError: false }),
    }));
    setSearchableTools(many);
    const result = await toolSearchTool.execute({ query: 'gadget', max_results: -3 }, ctx);
    const bullets = result.output.match(/^- \*\*/gm) || [];
    assert.equal(bullets.length, 5);
  });

  it('graded scoring ranks an exact name-part above a partial match', async () => {
    setSearchableTools([
      // 'msg' is an exact name PART here (+3)
      { name: 'msg_send', description: 'deliver', inputSchema: { type: 'object', properties: {}, required: [] }, isReadOnly: false, execute: async () => ({ output: '', isError: false }) },
      // 'msg' only appears inside a larger part here (+2)
      { name: 'msgbox_open', description: 'open a box', inputSchema: { type: 'object', properties: {}, required: [] }, isReadOnly: false, execute: async () => ({ output: '', isError: false }) },
    ]);
    const result = await toolSearchTool.execute({ query: 'msg' }, ctx);
    assert.ok(result.output.indexOf('msg_send') < result.output.indexOf('msgbox_open'));
  });

  it('returns structured metadata on a successful search', async () => {
    const result = await toolSearchTool.execute({ query: 'file' }, ctx);
    assert.equal(result.metadata?.query, 'file');
    assert.ok(Array.isArray(result.metadata?.matches));
    assert.equal(result.metadata?.total_deferred_tools, mockTools.length);
  });

  it('empty results include a pending-MCP hint when an MCP manager is present', async () => {
    const result = await toolSearchTool.execute(
      { query: 'zzznotathing' },
      { ...ctx, _mcpManager: {} as any },
    );
    assert.ok(result.output.includes('MCP servers may still be connecting'));
    assert.deepEqual(result.metadata?.matches, []);
  });

  it('empty results omit the MCP hint without an MCP manager', async () => {
    const result = await toolSearchTool.execute({ query: 'zzznotathing' }, ctx);
    assert.ok(!result.output.includes('MCP servers may still be connecting'));
  });

  it('word-boundary matching does not match mid-word substrings', async () => {
    setSearchableTools([
      { name: 'alpha', description: 'already configured', inputSchema: { type: 'object', properties: {}, required: [] }, isReadOnly: true, execute: async () => ({ output: '', isError: false }) },
    ]);
    // "read" should NOT match "already" via word boundary; name has no match either.
    const result = await toolSearchTool.execute({ query: 'read' }, ctx);
    assert.ok(result.output.includes('No tools or skills matched'));
  });
});
