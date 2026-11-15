import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  toolSearchTool,
  setSearchableTools,
  resolveSearchableTool,
  resolveSearchableSkill,
} from '../src/tools/tool-search';
import type { ToolDefinition, ToolContext } from '../src/types';
import type { SkillDefinition, SkillManagerLike } from '../src/skills';

const mockTools: ToolDefinition[] = [
  { name: 'file_read', description: 'Read file contents from the filesystem', inputSchema: { type: 'object', properties: { file_path: { type: 'string' } }, required: ['file_path'] }, isReadOnly: true, isConcurrencySafe: true, execute: async () => ({ output: '', isError: false }) },
  { name: 'bash', description: 'Execute shell commands', inputSchema: { type: 'object', properties: { command: { type: 'string' } }, required: ['command'] }, isReadOnly: false, isConcurrencySafe: false, execute: async () => ({ output: '', isError: false }) },
  { name: 'grep', description: 'Search for patterns in files', inputSchema: { type: 'object', properties: { pattern: { type: 'string' } }, required: ['pattern'] }, isReadOnly: true, execute: async () => ({ output: '', isError: false }) },
];

const mockSkills: SkillDefinition[] = [
  { name: 'research', description: 'Deep research stance', content: 'x', filePath: '/skills/research/SKILL.md', whenToUse: 'investigate', autoAllowPatterns: ['curl *'] },
  { name: 'explain', description: 'Explain code', content: 'x', filePath: '/skills/explain/SKILL.md' },
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

describe('tool_search parity — case-insensitive name resolution (cross-framework)', () => {
  beforeEach(() => { setSearchableTools(mockTools); });

  // --- bare fast-path (FIX: resolveSearchableTool in the bare fast path) ---
  it('bare PascalCase name loads the snake_case tool (RED without CI fallback)', async () => {
    const loaded: string[] = [];
    const c: ToolContext = { ...ctx, _loadDeferredTool: (n) => loaded.push(n) };
    const result = await toolSearchTool.execute({ query: 'Bash' }, c);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.name, 'bash');
    assert.ok(parsed.inputSchema);
    // FIX: loaded under the CANONICAL registered name, not the user's casing.
    assert.deepEqual(loaded, ['bash']);
  });

  it('bare lowercase mixed-case name (fIlE_rEaD) still resolves', async () => {
    const result = await toolSearchTool.execute({ query: 'fIlE_rEaD' }, ctx);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.name, 'file_read');
  });

  it('exact-case bare name still wins (unchanged happy path)', async () => {
    const loaded: string[] = [];
    const c: ToolContext = { ...ctx, _loadDeferredTool: (n) => loaded.push(n) };
    const result = await toolSearchTool.execute({ query: 'bash' }, c);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.name, 'bash');
    assert.deepEqual(loaded, ['bash']);
  });

  // --- select: single (FIX: resolveSearchableTool in single-select) ---
  it('select:Bash (uppercase) returns the bash schema and loads canonical name', async () => {
    const loaded: string[] = [];
    const c: ToolContext = { ...ctx, _loadDeferredTool: (n) => loaded.push(n) };
    const result = await toolSearchTool.execute({ query: 'select:Bash' }, c);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.name, 'bash');
    assert.deepEqual(loaded, ['bash']);
  });

  it('select:RESEARCH (uppercase) resolves the research skill', async () => {
    const result = await toolSearchTool.execute({ query: 'select:RESEARCH' }, ctxWithSkills);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.type, 'skill');
    assert.equal(parsed.name, 'research');
  });

  // --- select: multi (FIX: resolveSearchableTool in multi-select) ---
  it('select:Bash,GREP loads both tools under canonical names', async () => {
    const loaded: string[] = [];
    const c: ToolContext = { ...ctx, _loadDeferredTool: (n) => loaded.push(n) };
    const result = await toolSearchTool.execute({ query: 'select:Bash,GREP' }, c);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.deepEqual(parsed.tools.map((t: any) => t.name), ['bash', 'grep']);
    assert.deepEqual(parsed.notFound, []);
    assert.deepEqual(loaded, ['bash', 'grep']);
  });

  it('select: resolves a mixed-case skill alongside a mixed-case tool', async () => {
    const result = await toolSearchTool.execute({ query: 'select:BASH,ExPlAiN' }, ctxWithSkills);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.deepEqual(parsed.tools.map((t: any) => t.name), ['bash']);
    assert.equal(parsed.skills.length, 1);
    assert.equal(parsed.skills[0].name, 'explain');
  });

  // --- case-insensitive dedupe (FIX: lower-cased seen-set) ---
  it('select:bash,BASH collapses to a single tool (no double-load)', async () => {
    const loaded: string[] = [];
    const c: ToolContext = { ...ctx, _loadDeferredTool: (n) => loaded.push(n) };
    const result = await toolSearchTool.execute({ query: 'select:bash,BASH' }, c);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    // Collapses to the single-name path (bare payload, not the wrapper).
    assert.equal(parsed.name, 'bash');
    assert.equal(parsed.tools, undefined);
    // Loaded exactly once despite the case-variant duplicate.
    assert.deepEqual(loaded, ['bash']);
  });

  it('select:bash,BASH,grep dedupes the case-variant pair but keeps grep (multi)', async () => {
    const loaded: string[] = [];
    const c: ToolContext = { ...ctx, _loadDeferredTool: (n) => loaded.push(n) };
    const result = await toolSearchTool.execute({ query: 'select:bash,BASH,grep' }, c);
    assert.equal(result.isError, false);
    const parsed = JSON.parse(result.output);
    assert.deepEqual(parsed.tools.map((t: any) => t.name), ['bash', 'grep']);
    assert.deepEqual(loaded, ['bash', 'grep']);
  });

  // --- unknown names still error (guards the fallback stays narrow) ---
  it('an entirely unknown name still reports not found', async () => {
    const result = await toolSearchTool.execute({ query: 'select:Nonexistent' }, ctx);
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not found'));
  });

  // --- exported resolver helpers are directly usable ---
  it('resolveSearchableTool: exact match, CI fallback, and miss', () => {
    assert.equal(resolveSearchableTool('bash')?.name, 'bash');
    assert.equal(resolveSearchableTool('BASH')?.name, 'bash');
    assert.equal(resolveSearchableTool('nope'), undefined);
  });

  it('resolveSearchableSkill: exact, CI fallback, miss, and no-manager', () => {
    assert.equal(resolveSearchableSkill(ctxWithSkills, 'research')?.name, 'research');
    assert.equal(resolveSearchableSkill(ctxWithSkills, 'RESEARCH')?.name, 'research');
    assert.equal(resolveSearchableSkill(ctxWithSkills, 'nope'), null);
    assert.equal(resolveSearchableSkill(ctx, 'research'), null);
  });

  // --- feature is documented in the tool description ---
  it('description documents case-insensitive name matching', () => {
    assert.match(toolSearchTool.description, /case-insensitive/i);
  });
});
