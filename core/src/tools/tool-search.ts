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
      const name = query.slice('select:'.length).trim();
      const tool = _allTools.find(t => t.name === name);
      if (tool) {
        context._loadDeferredTool?.(name);
        return {
          output: JSON.stringify({
            name: tool.name,
            description: tool.description,
            inputSchema: tool.inputSchema,
            isReadOnly: tool.isReadOnly,
          }, null, 2),
          isError: false,
        };
      }
      const skill = context._skillManager?.getSkill(name);
      if (skill) {
        return {
          output: JSON.stringify({
            type: 'skill',
            name: skill.name,
            description: skill.description,
            whenToUse: skill.whenToUse || null,
            isStance: isStanceSkill(skill),
            activation: formatSkillActivation(skill),
          }, null, 2),
          isError: false,
        };
      }
      const availableTools = _allTools.map(t => t.name);
      const availableSkills = skills.map(s => s.name);
      return {
        output: `"${name}" not found.\nTools: ${availableTools.join(', ')}\nSkills: ${availableSkills.length > 0 ? availableSkills.join(', ') : '(none)'}`,
        isError: true,
      };
    }

    const keywords = query.toLowerCase().split(/\s+/);
    const scoredTools = _allTools.map(tool => {
      let score = 0;
      const text = `${tool.name} ${tool.description}`.toLowerCase();
      for (const kw of keywords) {
        if (text.includes(kw)) score++;
        if (tool.name.includes(kw)) score += 2;
      }
      return { tool, score };
    })
    .filter(s => s.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 5);

    const scoredSkills = skills.map(skill => {
      let score = 0;
      const text = `${skill.name} ${skill.description || ''} ${skill.whenToUse || ''}`.toLowerCase();
      for (const kw of keywords) {
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
