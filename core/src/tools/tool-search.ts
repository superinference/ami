import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { type SkillDefinition, isStanceSkill } from '../skills';

let _allTools: ToolDefinition[] = [];

export function setSearchableTools(tools: ToolDefinition[]): void {
  _allTools = tools;
}

/**
 * Resolve a tool by name for the `select:`/bare fast-path loaders. Tries an
 * exact match first, then a case-insensitive fallback. The fallback is a
 * cross-framework parity superset: models trained on Claude Code conventions
 * emit PascalCase names (`select:Bash`, bare `Read`) while AMI registers tools
 * in snake_case (`bash`, `file_read`) — resolving them case-insensitively lets
 * those loads succeed instead of erroring as "not found". Callers MUST use the
 * returned definition's canonical `.name` when loading the deferred tool.
 */
export function resolveSearchableTool(name: string): ToolDefinition | undefined {
  const exact = _allTools.find(t => t.name === name);
  if (exact) return exact;
  const lower = name.toLowerCase();
  return _allTools.find(t => t.name.toLowerCase() === lower);
}

/**
 * Resolve a skill by name with the same exact-then-case-insensitive strategy as
 * {@link resolveSearchableTool}, so `select:RESEARCH` resolves the `research`
 * skill. Returns `null` when no skill manager is present or nothing matches.
 */
export function resolveSearchableSkill(
  context: ToolContext,
  name: string,
): SkillDefinition | null {
  const mgr = context._skillManager;
  if (!mgr) return null;
  const direct = mgr.getSkill(name);
  if (direct) return direct;
  const lower = name.toLowerCase();
  return mgr.listSkills().find(s => s.name.toLowerCase() === lower) ?? null;
}

function formatSkillActivation(skill: SkillDefinition): string {
  if (isStanceSkill(skill)) {
    return `Activate stance with: skill({skill: "${skill.name}"})`;
  }
  return `Run with: skill({skill: "${skill.name}"})`;
}

function toolPayload(tool: ToolDefinition): Record<string, unknown> {
  return {
    name: tool.name,
    description: tool.description,
    inputSchema: tool.inputSchema,
    isReadOnly: tool.isReadOnly,
  };
}

function skillPayload(skill: SkillDefinition): Record<string, unknown> {
  return {
    type: 'skill',
    name: skill.name,
    description: skill.description,
    whenToUse: skill.whenToUse || null,
    isStance: isStanceSkill(skill),
    activation: formatSkillActivation(skill),
  };
}

/** Hard cap (chars) on a tool_search result before it is truncated, mirroring
 *  Claude's `ToolSearchTool.maxResultSizeChars` (100k). A `select:` multi-load
 *  can emit many full JSON schemas and a keyword hit can list pathologically
 *  long (MCP) tool names, so the output is bounded so a single search can never
 *  blow up the model's context. Enforced uniformly at the tool's single exit. */
export const TOOL_SEARCH_MAX_RESULT_CHARS = 100_000;

/** Clamp any tool_search result output to {@link TOOL_SEARCH_MAX_RESULT_CHARS}
 *  with an explicit truncation marker (parity with Claude's result-size bound). */
function boundResult(result: ToolResult): ToolResult {
  if (result.output.length <= TOOL_SEARCH_MAX_RESULT_CHARS) return result;
  const kept = result.output.slice(0, TOOL_SEARCH_MAX_RESULT_CHARS);
  return {
    ...result,
    output: `${kept}\n... [tool_search result truncated at ${TOOL_SEARCH_MAX_RESULT_CHARS} chars]`,
  };
}

/**
 * Names of MCP servers that are still connecting (`pending`/`connecting`),
 * sorted and de-duplicated. MCP servers connect asynchronously, so a tool the
 * model expected may not be searchable yet — surfacing the specific server
 * names tells the model exactly which integration to retry once it is ready
 * (parity with Claude's `pending_mcp_servers`). Returns `[]` when there is no
 * MCP manager, it lacks `listAllServers`, or the call throws.
 */
function getPendingMcpServers(context: ToolContext): string[] {
  const mgr = context._mcpManager;
  if (!mgr || typeof mgr.listAllServers !== 'function') return [];
  try {
    const servers = mgr.listAllServers() as Array<{ name: string; state: string }>;
    const names = servers
      .filter(s => s && (s.state === 'pending' || s.state === 'connecting'))
      .map(s => s.name);
    return [...new Set(names)].sort((a, b) => a.localeCompare(b));
  } catch {
    return [];
  }
}

/**
 * Build the "MCP servers may still be connecting" retry affordance appended to
 * empty/not-found results. When specific servers are pending their names are
 * listed; otherwise (manager present but nothing pending) the generic hint is
 * kept so the model still knows to retry.
 */
function pendingMcpHint(context: ToolContext, pending: string[]): string {
  if (!context._mcpManager) return '';
  if (pending.length > 0) {
    return `\nMCP servers may still be connecting (${pending.join(', ')}) — their tools can appear shortly, so try again once they are ready.`;
  }
  return '\nMCP servers may still be connecting — their tools can appear shortly, so try again if you expected one.';
}

function notFoundMessage(
  names: string[],
  skills: SkillDefinition[],
  context: ToolContext,
  pending: string[],
): string {
  const availableTools = _allTools.map(t => t.name);
  const availableSkills = skills.map(s => s.name);
  const label = names.map(n => `"${n}"`).join(', ');
  return `${label} not found.\nTools: ${availableTools.join(', ')}\nSkills: ${availableSkills.length > 0 ? availableSkills.join(', ') : '(none)'}${pendingMcpHint(context, pending)}`;
}

/** Metadata attached to a not-found/no-match result, including the pending MCP
 *  servers when any are connecting (parity with Claude's output schema). */
function missMetadata(query: string, pending: string[]): Record<string, unknown> {
  return {
    query,
    matches: [],
    total_deferred_tools: _allTools.length,
    ...(pending.length > 0 ? { pending_mcp_servers: pending } : {}),
  };
}

/** Word-boundary match of a single term against text (so "read" matches
 *  "read file" but not "already"). */
function matchesTerm(text: string, term: string): boolean {
  const escaped = term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`\\b${escaped}`, 'i').test(text);
}

/**
 * Tokenize a tool/skill name into searchable parts. MCP tool names
 * (`mcp__server__action`) are parsed structurally — the `mcp__` prefix is
 * stripped and the remainder split on `__`/`_` — so a query like "slack"
 * matches `mcp__slack__send_message` by its server/action parts without the
 * literal "mcp" prefix becoming a spurious token. Regular names split on the
 * usual separators. Parity with Claude's `parseToolName`.
 */
function parseName(name: string): { parts: string[]; full: string; isMcp: boolean } {
  if (name.startsWith('mcp__')) {
    const withoutPrefix = name.slice('mcp__'.length).toLowerCase();
    const parts = withoutPrefix.split('__').flatMap(p => p.split('_')).filter(Boolean);
    return { parts, full: parts.join(' '), isMcp: true };
  }
  const lower = name.toLowerCase();
  return { parts: lower.split(/[_\-/.]+/).filter(Boolean), full: lower, isMcp: false };
}

/** Graded score of ranking terms against a tool/skill name + text, using
 *  part-based tokenization (exact part > partial part > full-name fallback).
 *  MCP tool-name parts score higher than regular ones (an MCP server/action
 *  name is a strong intent signal), mirroring Claude's MCP weighting. */
function gradedScore(name: string, text: string, rankTerms: string[]): number {
  const { parts, full, isMcp } = parseName(name);
  // MCP server/action name parts are a strong intent signal, so they outrank an
  // equivalent regular tool. The MCP margin is +2 (not +1) over the regular
  // tiers so it is not cancelled by the +1 word-boundary text bonus a regular
  // tool earns when its name part sits at a word boundary. Parity with Claude's
  // MCP weighting (12 vs 10 exact / 6 vs 5 partial).
  const exactWeight = isMcp ? 5 : 3;
  const partialWeight = isMcp ? 4 : 2;
  let score = 0;
  for (const kw of rankTerms) {
    const k = kw.toLowerCase();
    if (parts.includes(k)) score += exactWeight;            // exact name-part match
    else if (parts.some(p => p.includes(k))) score += partialWeight; // partial name-part
    else if (full.includes(k)) score += 1;                  // full-name fallback
    if (matchesTerm(text, kw)) score += 1;                  // word-boundary in text
  }
  return score;
}

async function runToolSearch(
  input: Record<string, unknown>,
  context: ToolContext,
): Promise<ToolResult> {
  const query = (input.query as string || '').trim();

  if (!query) {
    return { output: 'Error: query must not be empty.', isError: true };
  }

  const skills = context._skillManager?.listSkills() ?? [];

  if (query.startsWith('select:')) {
    // Parse one or more comma-separated names: trim each, drop empties, dedupe.
    // Dedupe is CASE-INSENSITIVE (keeping the first-seen casing) so that
    // `select:bash,BASH` collapses to one entry instead of loading the same
    // tool twice — consistent with the case-insensitive resolver below.
    const rawNames = query
      .slice('select:'.length)
      .split(',')
      .map(n => n.trim())
      .filter(n => n.length > 0);
    const seen = new Set<string>();
    const names: string[] = [];
    for (const n of rawNames) {
      const key = n.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      names.push(n);
    }

    // Single-name select: preserve the original behavior — a bare JSON payload
    // for the matched tool/skill, or a not-found error listing what's available.
    if (names.length <= 1) {
      const name = names[0] ?? '';
      const tool = resolveSearchableTool(name);
      if (tool) {
        // Load under the CANONICAL name so a case-variant input still targets
        // the registered deferred tool.
        context._loadDeferredTool?.(tool.name);
        return { output: JSON.stringify(toolPayload(tool), null, 2), isError: false };
      }
      const skill = resolveSearchableSkill(context, name);
      if (skill) {
        return { output: JSON.stringify(skillPayload(skill), null, 2), isError: false };
      }
      const pending = getPendingMcpServers(context);
      return {
        output: notFoundMessage([name], skills, context, pending),
        isError: true,
        metadata: missMetadata(query, pending),
      };
    }

    // Multi-name select: resolve every listed name, loading all matched tools.
    const toolsOut: Record<string, unknown>[] = [];
    const skillsOut: Record<string, unknown>[] = [];
    const notFound: string[] = [];
    for (const name of names) {
      const tool = resolveSearchableTool(name);
      if (tool) {
        context._loadDeferredTool?.(tool.name);
        toolsOut.push(toolPayload(tool));
        continue;
      }
      const skill = resolveSearchableSkill(context, name);
      if (skill) {
        skillsOut.push(skillPayload(skill));
        continue;
      }
      notFound.push(name);
    }
    if (toolsOut.length === 0 && skillsOut.length === 0) {
      const pending = getPendingMcpServers(context);
      return {
        output: notFoundMessage(notFound, skills, context, pending),
        isError: true,
        metadata: missMetadata(query, pending),
      };
    }
    return {
      output: JSON.stringify({ tools: toolsOut, skills: skillsOut, notFound }, null, 2),
      isError: false,
    };
  }

  // Exact bare-name fast path: a single-token query that exactly matches a
  // deferred TOOL name resolves and loads it directly, no "select:" prefix
  // needed. (Skills are not fast-pathed — a keyword can legitimately equal a
  // skill name, so skills go through ranked search.)
  if (!/\s/.test(query) && !query.startsWith('+')) {
    // Case-insensitive resolution (see resolveSearchableTool) so a bare
    // PascalCase name like `Bash` loads the registered `bash` tool.
    const exactTool = resolveSearchableTool(query);
    if (exactTool) {
      context._loadDeferredTool?.(exactTool.name);
      return { output: JSON.stringify(toolPayload(exactTool), null, 2), isError: false };
    }
  }

  const maxResults = (() => {
    const raw = Number(input.max_results);
    if (!Number.isFinite(raw) || raw <= 0) return 5;
    return Math.min(Math.floor(raw), 50);
  })();

  // Split terms into `+`-prefixed required terms and plain ranking terms. A
  // required term MUST appear in the item (name or description); plain terms
  // only affect ranking. With no `+` terms this reduces to pure ranking.
  const rawTerms = query.toLowerCase().split(/\s+/).filter(Boolean);
  const requiredTerms: string[] = [];
  const rankTerms: string[] = [];
  for (const term of rawTerms) {
    if (term.startsWith('+') && term.length > 1) requiredTerms.push(term.slice(1));
    else if (!term.startsWith('+')) rankTerms.push(term);
  }

  const scoredTools = _allTools.map(tool => {
    const nameLower = tool.name.toLowerCase();
    // A +term must appear in the NAME (deliberate: keeps the gate precise).
    if (!requiredTerms.every(rt => nameLower.includes(rt))) return { tool, score: 0 };
    const text = `${tool.name} ${tool.description}`.toLowerCase();
    const score = requiredTerms.length * 2 + gradedScore(tool.name, text, rankTerms);
    return { tool, score };
  })
  .filter(s => s.score > 0)
  .sort((a, b) => b.score - a.score)
  .slice(0, maxResults);

  const scoredSkills = skills.map(skill => {
    const nameLower = skill.name.toLowerCase();
    if (!requiredTerms.every(rt => nameLower.includes(rt))) return { skill, score: 0 };
    const text = `${skill.name} ${skill.description || ''} ${skill.whenToUse || ''}`.toLowerCase();
    const score = requiredTerms.length * 2 + gradedScore(skill.name, text, rankTerms);
    return { skill, score };
  })
  .filter(s => s.score > 0)
  .sort((a, b) => b.score - a.score)
  .slice(0, maxResults);

  if (scoredTools.length === 0 && scoredSkills.length === 0) {
    const availableTools = _allTools.map(t => t.name);
    const availableSkills = skills.map(s => s.name);
    // MCP servers connect asynchronously — their tools may not be searchable
    // yet, so a miss is not necessarily final. Name the specific pending
    // servers when any are connecting so the model knows what to retry.
    const pending = getPendingMcpServers(context);
    return {
      output: `No tools or skills matched "${query}".\nTools: ${availableTools.join(', ')}\nSkills: ${availableSkills.length > 0 ? availableSkills.join(', ') : '(none)'}${pendingMcpHint(context, pending)}`,
      isError: false,
      metadata: missMetadata(query, pending),
    };
  }

  const sections: string[] = [];

  if (scoredTools.length > 0) {
    const toolResults = scoredTools.map(s =>
      `- **${s.tool.name}** — ${s.tool.description.slice(0, 100)}${s.tool.description.length > 100 ? '...' : ''}`
    ).join('\n');
    sections.push(`Tools matching "${query}":\n\n${toolResults}`);
  }

  if (scoredSkills.length > 0) {
    const skillResults = scoredSkills.map(s => {
      const desc = (s.skill.description || '').slice(0, 100);
      const ellipsis = (s.skill.description || '').length > 100 ? '...' : '';
      const tag = isStanceSkill(s.skill) ? ' [stance]' : '';
      return `- **${s.skill.name}**${tag} — ${desc}${ellipsis} | ${formatSkillActivation(s.skill)}`;
    }).join('\n');
    sections.push(`Skills matching "${query}":\n\n${skillResults}`);
  }

  sections.push('Use tool_search with "select:name" to get details for a specific tool or skill.');

  return {
    output: sections.join('\n\n'),
    isError: false,
    metadata: {
      query,
      matches: [...scoredTools.map(s => s.tool.name), ...scoredSkills.map(s => s.skill.name)],
      total_deferred_tools: _allTools.length,
    },
  };
}

export const toolSearchTool: ToolDefinition = {
  name: 'tool_search',
  description:
    'Search for available tools and skills, or load a deferred tool. Query forms:\n' +
    '  - "select:name" — load the full schema for a specific tool (or skill details). ' +
    'Comma-separate names ("select:a,b,c") to load several at once.\n' +
    '  - a bare exact tool/skill name — same as select: (fast path, resolves and loads it directly).\n' +
    '  Name matching for select:/bare-name is case-insensitive (e.g. "Bash" resolves "bash").\n' +
    '  - keywords — ranked fuzzy search over names + descriptions; "+term" REQUIRES that term ' +
    'to appear in the match NAME, other terms only affect ranking.\n' +
    'Results are a ranked bullet list (top max_results). Use when you need a tool/skill not in your current set.',
  inputSchema: {
    type: 'object',
    properties: {
      query: {
        type: 'string',
        description: 'Keywords, "+term" to require a term, a bare exact name, or "select:name[,name2]" to load specific tools/skills.',
      },
      max_results: {
        type: 'number',
        description: 'Maximum number of matches to return (default 5).',
        default: 5,
      },
    },
    required: ['query'],
  },
  isReadOnly: true,
  isConcurrencySafe: true,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    // Single exit: bound every result to the max size (parity with Claude's
    // maxResultSizeChars) so no search can overflow the model's context.
    return boundResult(await runToolSearch(input, context));
  },
};
