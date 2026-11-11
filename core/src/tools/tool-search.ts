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

export const toolSearchTool: ToolDefinition = {
  name: 'tool_search',
  description:
    'Search for available tools and skills by keyword. Use when you need a tool or skill that is not in your current set. ' +
    'Use "select:tool_name" to get the full schema for a specific tool, or "select:skill_name" for skill details.',
  inputSchema: {
    type: 'object',
    properties: {
      query: {
        type: 'string',
        description: 'Keywords describing the tool or skill you need, or "select:name" to load details for a specific tool or skill.',
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

    // Split terms into `+`-prefixed required-in-name terms and plain ranking
    // terms. A required term MUST appear in the item's name; plain terms only
    // affect ranking. With no `+` terms this reduces to the original behavior.
    const rawTerms = query.toLowerCase().split(/\s+/).filter(Boolean);
    const requiredTerms: string[] = [];
    const rankTerms: string[] = [];
    for (const term of rawTerms) {
      if (term.startsWith('+') && term.length > 1) requiredTerms.push(term.slice(1));
      else if (!term.startsWith('+')) rankTerms.push(term);
    }

    const scoredTools = _allTools.map(tool => {
      const nameLower = tool.name.toLowerCase();
      if (!requiredTerms.every(rt => nameLower.includes(rt))) return { tool, score: 0 };
      let score = requiredTerms.length * 2;
      const text = `${tool.name} ${tool.description}`.toLowerCase();
      for (const kw of rankTerms) {
        if (text.includes(kw)) score++;
        if (tool.name.includes(kw)) score += 2;
      }
      return { tool, score };
    })
    .filter(s => s.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 5);

    const scoredSkills = skills.map(skill => {
      const nameLower = skill.name.toLowerCase();
      if (!requiredTerms.every(rt => nameLower.includes(rt))) return { skill, score: 0 };
      let score = requiredTerms.length * 2;
      const text = `${skill.name} ${skill.description || ''} ${skill.whenToUse || ''}`.toLowerCase();
      for (const kw of rankTerms) {
        if (text.includes(kw)) score++;
        if (skill.name.includes(kw)) score += 2;
      }
      return { skill, score };
    })
    .filter(s => s.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 5);

    if (scoredTools.length === 0 && scoredSkills.length === 0) {
      const availableTools = _allTools.map(t => t.name);
      const availableSkills = skills.map(s => s.name);
      return {
        output: `No tools or skills matched "${query}".\nTools: ${availableTools.join(', ')}\nSkills: ${availableSkills.length > 0 ? availableSkills.join(', ') : '(none)'}`,
        isError: false,
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
    };
  },
};
