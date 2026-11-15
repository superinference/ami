import { ToolDefinition, ToolContext, ToolResult, EngineConfig } from '../types';
import { createDefaultTools } from './index';
import { SessionManager } from '../session';
import { type SkillDefinition, type SkillManagerLike, isStanceSkill, sanitizeSkillArgs } from '../skills';

/**
 * The outcome of resolving a requested skill name against the manager.
 * - `skill`/`canonicalName` identify the resolved skill (the name the manager
 *   recognizes — pass it to `getSkillContent`).
 * - `ambiguous` lists the qualified candidates when an UNqualified request
 *   matches several directory/plugin-scoped skills and cannot be disambiguated.
 */
export interface SkillResolution {
  skill: SkillDefinition | null;
  canonicalName: string;
  ambiguous?: string[];
}

/**
 * Resolve a requested skill name to a registered skill, mirroring Claude Code's
 * Skill-tool name resolution as an additive superset:
 *
 * 1. **Exact match** — handles unscoped names and fully-qualified `plugin:skill`
 *    / directory-scoped `dir:name` forms that the manager stores verbatim.
 * 2. **Qualifier fallback** — a qualified request (`X:Y`) whose prefix is not
 *    registered falls back to the bare skill segment `Y` (so `web:deploy`
 *    resolves `deploy` when only the unscoped skill exists).
 * 3. **Most-specific-wins** — an UNqualified request `Y` resolves to a unique
 *    scoped variant `*:Y` when exactly one exists; when several scoped skills
 *    match it is reported as ambiguous so the caller can qualify it rather than
 *    silently guessing.
 *
 * Pure + exported for testability. Takes only the subset of the manager it needs.
 */
export function resolveSkillTarget(
  requested: string,
  manager: Pick<SkillManagerLike, 'getSkill' | 'listSkills'>,
): SkillResolution {
  const name = requested.trim().replace(/^\/+/, '');

  const exact = manager.getSkill(name);
  if (exact) return { skill: exact, canonicalName: name };

  if (name.includes(':')) {
    const bare = name.slice(name.lastIndexOf(':') + 1);
    if (bare && bare !== name) {
      const bareMatch = manager.getSkill(bare);
      if (bareMatch) return { skill: bareMatch, canonicalName: bare };
    }
    return { skill: null, canonicalName: name };
  }

  const suffix = `:${name}`;
  const scoped = manager.listSkills().filter(s => s.name.endsWith(suffix));
  if (scoped.length === 1) return { skill: scoped[0], canonicalName: scoped[0].name };
  if (scoped.length > 1) {
    return { skill: null, canonicalName: name, ambiguous: scoped.map(s => s.name) };
  }
  return { skill: null, canonicalName: name };
}

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
    'When a skill applies, you MUST invoke it with this tool BEFORE writing your answer — do the invocation first, then respond using what it returns. ' +
    'NEVER merely mention or describe a skill (or tell the user to run it) without actually calling this tool. ' +
    'Do NOT re-invoke a skill that is already active/loaded in this conversation — invoke each applicable skill once. ' +
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

    // FUNCTIONAL (parity/arg-safety): bound the ARGUMENTS passthrough before it
    // reaches the skill template. Previously the raw args string flowed unbounded
    // into getSkillContent (and MCP getPrompt); an oversized value could blow up
    // the context. sanitizeSkillArgs caps the value to MAX_SKILL_ARG_VALUE_LENGTH,
    // matching the arg-safety limits Claude Code enforces on skill invocation.
    const safeArgs = args ? sanitizeSkillArgs({ ARGUMENTS: args }) : undefined;

    if (!rawName || rawName.trim().length === 0) {
      return { output: 'Error: skill name must not be empty. Pass the bare skill name, e.g. {"skill":"code-review"}.', isError: true };
    }
    // Tolerate a leading slash (models often pass a slash-command form "/commit").
    const skillName = rawName.trim().replace(/^\/+/, '');

    if (!context._skillManager) {
      return { output: 'Error: skill manager not available in this context.', isError: true };
    }

    // FUNCTIONAL (parity/name-resolution): resolve plugin-qualified (`plugin:skill`)
    // and directory-scoped (`dir:name`) names with most-specific-wins semantics
    // instead of a single verbatim getSkill() lookup. See resolveSkillTarget.
    const resolution = resolveSkillTarget(skillName, context._skillManager);
    if (resolution.ambiguous) {
      return {
        output: `Error: skill "${skillName}" is ambiguous — it matches multiple scoped skills: ${resolution.ambiguous.join(', ')}. Qualify it, e.g. {"skill":"${resolution.ambiguous[0]}"}.`,
        isError: true,
      };
    }
    const skillDef = resolution.skill;
    const resolvedName = resolution.canonicalName;
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

    const skillContent = context._skillManager.getSkillContent(resolvedName, safeArgs);

    if (context._mcpManager && !skillContent) {
      try {
        const mcpPrompts = await context._mcpManager.listPrompts?.();
        const mcpSkill = mcpPrompts?.find((p: any) => p.name === skillName);
        if (mcpSkill) {
          const promptResult = await context._mcpManager.getPrompt?.(mcpSkill.server, skillName, safeArgs ?? {});
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
      context._engineAddSystemReminder(`[Skill: ${resolvedName}]\n${skillContent}`);
      context._engineSetActiveSkill?.(resolvedName);
      return {
        output: `Skill "${resolvedName}" activated inline. Following its instructions.`,
        metadata: { mode: 'inline', skill: resolvedName, model: skill?.model },
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
        metadata: { mode: 'forked', skill: resolvedName, model: providerConfig.model, toolCount: tools.length },
      };
    } finally {
      context.abortSignal.removeEventListener('abort', forwardAbort);
      (subEngine as any).shutdown?.();
    }

    const header = `[Skill: ${resolvedName} · ${tools.length} tools · model ${providerConfig.model || 'default'}]`;
    return {
      output: `${header}\n${capSkillOutput(result) || '(skill produced no output)'}`,
      isError: false,
      metadata: { mode: 'forked', skill: resolvedName, model: providerConfig.model, toolCount: tools.length },
    };
  },
};

/** Hard cap on how much forked-skill output is echoed back into the context. */
export const SKILL_MAX_RESULT_CHARS = 100_000;

function capSkillOutput(text: string): string {
  if (text.length <= SKILL_MAX_RESULT_CHARS) return text;
  return text.slice(0, SKILL_MAX_RESULT_CHARS) + '\n[... skill output truncated ...]';
}
