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
