import { ToolDefinition, ToolContext, ToolResult, EngineConfig } from '../types';
import { createDefaultTools } from './index';
import { SessionManager } from '../session';

export const skillTool: ToolDefinition = {
  name: 'skill',
  description:
    'Invoke a skill by name. The skill\'s instructions are loaded and executed in a subagent context. ' +
    'Use this when a skill matches the user\'s request. Available skills are listed in the system prompt.',
  inputSchema: {
    type: 'object',
    properties: {
      skill: {
        type: 'string',
        description: 'The name of the skill to invoke (e.g. "code-review", "explain").',
      },
      args: {
        type: 'string',
        description: 'Optional arguments passed to the skill (available as $ARGUMENTS in the skill template).',
      },
    },
    required: ['skill'],
  },
  isReadOnly: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const skillName = input.skill as string;
    const args = input.args as string | undefined;

    if (!skillName || skillName.trim().length === 0) {
      return { output: 'Error: skill name must not be empty.', isError: true };
    }

    if (!context._skillManager) {
      return { output: 'Error: skill manager not available in this context.', isError: true };
    }

    const skillDef = context._skillManager.getSkill(skillName);
    if (skillDef?.disableModelInvocation) {
      return { output: `Error: Skill "${skillName}" has disableModelInvocation set and cannot be invoked by the model.`, isError: true };
    }

    const skillContent = context._skillManager.getSkillContent(skillName, args ? { ARGUMENTS: args } : undefined);

    if (context._mcpManager && !skillContent) {
      try {
        const mcpPrompts = await context._mcpManager.listPrompts?.();
        const mcpSkill = mcpPrompts?.find((p: any) => p.name === skillName);
        if (mcpSkill) {
          const promptResult = await context._mcpManager.getPrompt?.(mcpSkill.server, skillName, args ? { ARGUMENTS: args } : {});
          if (promptResult) {
            return { output: `[MCP Skill: ${skillName}]\n${JSON.stringify(promptResult)}` };
          }
        }
      } catch {}
    }

    if (!skillContent) {
      const available = context._skillManager.listSkills().map(s => s.name).join(', ');
      return { output: `Error: skill "${skillName}" not found. Available: ${available || '(none)'}`, isError: true };
    }

    const skill = skillDef;
    const isInline = skill?.context === 'inline' || !skill?.context;

    if (isInline && context._engineAddSystemReminder) {
      context._engineAddSystemReminder(`[Skill: ${skillName}]\n${skillContent}`);
      return { output: `Skill "${skillName}" activated inline. Following its instructions.` };
    }
    const allTools = createDefaultTools(context.cwd);
    let tools: ToolDefinition[];

    if (skill?.allowedTools && skill.allowedTools.length > 0) {
      const allowed = new Set(skill.allowedTools);
      tools = allTools.getAll().filter(t => allowed.has(t.name));
    } else {
      tools = allTools.getAll().filter(t => t.name !== 'skill');
    }

    const subAbort = new AbortController();
    const forwardAbort = () => subAbort.abort();
    context.abortSignal.addEventListener('abort', forwardAbort, { once: true });

    const providerConfig = { ...(context._providerConfig || { baseUrl: '', apiKey: '', model: '' }) };
    if (skill?.model) {
      providerConfig.model = skill.model;
    }

    const subConfig: EngineConfig = {
      provider: providerConfig,
      cwd: context.cwd,
      tools,
      sessionId: SessionManager.newId(),
      permissionMode: 'auto-allow',
      permissionPromptHandler: context._permissionPromptHandler,
      abortController: subAbort,
    };

    if (skillDef?.effort) {
      subConfig.thinking = { enabled: true, level: skillDef.effort as any };
    }

    if (!context._engineFactory) {
      return { output: 'Error: engine factory not available in this context.', isError: true };
    }
    const subEngine = context._engineFactory(subConfig);
    let result = '';

    try {
      for await (const event of subEngine.submit(skillContent)) {
        if (event.type === 'text_delta') {
          result += event.text;
        }
        if (event.type === 'error') {
          result += `\nError: ${event.error}`;
        }
      }
    } catch (err) {
      return {
        output: `Skill execution error: ${err instanceof Error ? err.message : String(err)}`,
        isError: true,
      };
    } finally {
      context.abortSignal.removeEventListener('abort', forwardAbort);
    }

    return { output: result || '(skill produced no output)', isError: false };
  },
};
