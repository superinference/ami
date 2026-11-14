import { ToolDefinition, ToolContext, ToolResult, EngineConfig } from '../types';
import { createDefaultTools } from './index';
import { SessionManager } from '../session';
import { type SkillDefinition, isStanceSkill } from '../skills';

/**
 * Filter tools for a skill based on its allowedTools and disallowedTools settings.
 * Exported for testability.
 *
 * Logic:
 * 1. If allowedTools is set, whitelist to only those tools.
 * 2. Otherwise, include all tools minus 'skill'.
 * 3. If disallowedTools is set, remove those tools from the result.
 */
export function filterToolsForSkill(
  allTools: ToolDefinition[],
  skill: SkillDefinition | null | undefined,
): ToolDefinition[] {
  let tools: ToolDefinition[];

  if (skill?.allowedTools && skill.allowedTools.length > 0) {
    const allowed = new Set(skill.allowedTools);
    tools = allTools.filter(t => allowed.has(t.name));
  } else {
    tools = allTools.filter(t => t.name !== 'skill');
  }

  if (skill?.disallowedTools && skill.disallowedTools.length > 0) {
    const disallowed = new Set(skill.disallowedTools);
    tools = tools.filter(t => !disallowed.has(t.name));
  }

  return tools;
}

export const skillTool: ToolDefinition = {
  name: 'skill',
  description:
    'Invoke a skill by name. The skill\'s instructions are loaded and executed (inline or in a subagent). ' +
    'Use this when a skill matches the user\'s request. Available skills are listed in the system prompt. ' +
    'A user-typed slash command (e.g. /commit) IS a skill invocation — pass the bare name ("commit"). ' +
    'A leading slash is tolerated and stripped. Do NOT use this tool for built-in CLI commands ' +
    '(/help, /clear, /model, etc.) — those are handled by the CLI, not skills. ' +
    'With no skill name it reports the available skills.',
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
      clear: {
        type: 'boolean',
        description: 'Set to true to deactivate the current inline skill when its task is complete and the conversation moves to a different topic.',
      },
    },
    required: [],
  },
  isReadOnly: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    if (input.clear === true) {
      context._engineSetActiveSkill?.(undefined);
      return { output: 'Inline skill deactivated. Returning to core mode.' };
    }

    // Accept common aliases for the skill name + args. Models frequently call
    // this with {name|skill_name|skillName, prompt} (skills are listed by name)
    // and would otherwise waste a turn on an "empty skill name" error before
    // retrying with a different key. Normalize all the variants they try.
    const rawName = (input.skill ?? input.name ?? input.skill_name ?? input.skillName) as string;
    const args = (input.args ?? input.prompt ?? input.arguments) as string | undefined;

    if (!rawName || rawName.trim().length === 0) {
      return { output: 'Error: skill name must not be empty. Pass the bare skill name, e.g. {"skill":"code-review"}.', isError: true };
    }
    // Tolerate a leading slash (models often pass a slash-command form "/commit").
    const skillName = rawName.trim().replace(/^\/+/, '');

    if (!context._skillManager) {
      return { output: 'Error: skill manager not available in this context.', isError: true };
    }

    const skillDef = context._skillManager.getSkill(skillName);
    if (skillDef && isStanceSkill(skillDef)) {
      // Stance skills are activated by switching the session stance, which the
      // engine special-cases BEFORE this tool runs (it registers the stance's
      // MCP servers, permissions, and overlay). Reaching this tool means no
      // engine interception happened (e.g. a standalone/sub-engine context) —
      // never run a stance as a one-shot sub-skill; report how it is activated.
      return {
        output: `Skill "${skillName}" is a stance skill. It is activated by switching the session stance (which enables its tools, permissions, and MCP servers), not by running it as a one-shot skill. Pin it at launch with --skill ${skillName}, or it is activated automatically when invoked in an interactive/detached session.`,
        isError: false,
      };
    }
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
      context._engineSetActiveSkill?.(skillName);
      return {
        output: `Skill "${skillName}" activated inline. Following its instructions.`,
        metadata: { mode: 'inline', skill: skillName, model: skill?.model },
      };
    }
    const allTools = createDefaultTools(context.cwd);
    const tools = filterToolsForSkill(allTools.getAll(), skill);

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
      // Forward the parent session's permission rules so inherited deny rules
      // still apply inside the forked skill (least-privilege / policy honored).
      permissionRules: context._parentPermissionRules,
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
        // Surface the forked skill's nested activity (tool use/results/text) to
        // the parent stream so its progress is visible live, not just at the end.
        context._emitChild?.(event);
      }
    } catch (err) {
      // Preserve whatever partial progress was produced before the failure.
      const partial = result ? `\n\n--- Partial output before error ---\n${capSkillOutput(result)}` : '';
      return {
        output: `Skill execution error: ${err instanceof Error ? err.message : String(err)}${partial}`,
        isError: true,
        metadata: { mode: 'forked', skill: skillName, model: providerConfig.model, toolCount: tools.length },
      };
    } finally {
      context.abortSignal.removeEventListener('abort', forwardAbort);
      (subEngine as any).shutdown?.();
    }

    const header = `[Skill: ${skillName} · ${tools.length} tools · model ${providerConfig.model || 'default'}]`;
    return {
      output: `${header}\n${capSkillOutput(result) || '(skill produced no output)'}`,
      isError: false,
      metadata: { mode: 'forked', skill: skillName, model: providerConfig.model, toolCount: tools.length },
    };
  },
};

/** Hard cap on how much forked-skill output is echoed back into the context. */
export const SKILL_MAX_RESULT_CHARS = 100_000;

function capSkillOutput(text: string): string {
  if (text.length <= SKILL_MAX_RESULT_CHARS) return text;
  return text.slice(0, SKILL_MAX_RESULT_CHARS) + '\n[... skill output truncated ...]';
}
