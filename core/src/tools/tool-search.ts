import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { type SkillDefinition, isStanceSkill } from '../skills';

let _allTools: ToolDefinition[] = [];

export function setSearchableTools(tools: ToolDefinition[]): void {
  _allTools = tools;
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

function notFoundMessage(names: string[], skills: SkillDefinition[]): string {
  const availableTools = _allTools.map(t => t.name);
  const availableSkills = skills.map(s => s.name);
  const label = names.map(n => `"${n}"`).join(', ');
  return `${label} not found.\nTools: ${availableTools.join(', ')}\nSkills: ${availableSkills.length > 0 ? availableSkills.join(', ') : '(none)'}`;
}

/** Word-boundary match of a single term against text (so "read" matches
 *  "read file" but not "already"). */
function matchesTerm(text: string, term: string): boolean {
  const escaped = term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`\\b${escaped}`, 'i').test(text);
}

/** Graded score of ranking terms against a tool/skill name + text, using
 *  part-based tokenization (exact part > partial part > full-name fallback). */
function gradedScore(name: string, text: string, rankTerms: string[]): number {
  const parts = name.toLowerCase().split(/[_\-/.]+/).filter(Boolean);
  let score = 0;
  for (const kw of rankTerms) {
    const k = kw.toLowerCase();
    if (parts.includes(k)) score += 3;            // exact name-part match
    else if (parts.some(p => p.includes(k))) score += 2; // partial name-part
    else if (name.toLowerCase().includes(k)) score += 1; // full-name fallback
    if (matchesTerm(text, kw)) score += 1;        // word-boundary in text
  }
  return score;
}

export const toolSearchTool: ToolDefinition = {
  name: 'tool_search',
  description:
    'Search for available tools and skills, or load a deferred tool. Query forms:\n' +
    '  - "select:name" — load the full schema for a specific tool (or skill details). ' +
    'Comma-separate names ("select:a,b,c") to load several at once.\n' +
    '  - a bare exact tool/skill name — same as select: (fast path, resolves and loads it directly).\n' +
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
    const query = (input.query as string || '').trim();

    if (!query) {
      return { output: 'Error: query must not be empty.', isError: true };
    }

    const skills = context._skillManager?.listSkills() ?? [];

    if (query.startsWith('select:')) {
      // Parse one or more comma-separated names: trim each, drop empties, dedupe.
      const rawNames = query
        .slice('select:'.length)
        .split(',')
        .map(n => n.trim())
        .filter(n => n.length > 0);
      const names = [...new Set(rawNames)];

      // Single-name select: preserve the original behavior — a bare JSON payload
      // for the matched tool/skill, or a not-found error listing what's available.
      if (names.length <= 1) {
        const name = names[0] ?? '';
        const tool = _allTools.find(t => t.name === name);
        if (tool) {
          context._loadDeferredTool?.(name);
          return { output: JSON.stringify(toolPayload(tool), null, 2), isError: false };
        }
        const skill = context._skillManager?.getSkill(name);
        if (skill) {
          return { output: JSON.stringify(skillPayload(skill), null, 2), isError: false };
        }
        return { output: notFoundMessage([name], skills), isError: true };
      }

      // Multi-name select: resolve every listed name, loading all matched tools.
      const toolsOut: Record<string, unknown>[] = [];
      const skillsOut: Record<string, unknown>[] = [];
      const notFound: string[] = [];
      for (const name of names) {
        const tool = _allTools.find(t => t.name === name);
        if (tool) {
          context._loadDeferredTool?.(name);
          toolsOut.push(toolPayload(tool));
          continue;
        }
        const skill = context._skillManager?.getSkill(name);
        if (skill) {
          skillsOut.push(skillPayload(skill));
          continue;
        }
        notFound.push(name);
      }
      if (toolsOut.length === 0 && skillsOut.length === 0) {
        return { output: notFoundMessage(notFound, skills), isError: true };
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
      const exactTool = _allTools.find(t => t.name === query);
      if (exactTool) {
        context._loadDeferredTool?.(query);
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
      // yet, so a miss is not necessarily final.
      const pendingHint = context._mcpManager
        ? '\nMCP servers may still be connecting — their tools can appear shortly, so try again if you expected one.'
        : '';
      return {
        output: `No tools or skills matched "${query}".\nTools: ${availableTools.join(', ')}\nSkills: ${availableSkills.length > 0 ? availableSkills.join(', ') : '(none)'}${pendingHint}`,
        isError: false,
        metadata: { query, matches: [], total_deferred_tools: _allTools.length },
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
  },
};
