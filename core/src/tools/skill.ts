import { ToolDefinition, ToolContext, ToolResult, EngineConfig } from '../types';
import { createDefaultTools } from './index';
import { SessionManager } from '../session';
import { type SkillDefinition, type SkillManagerLike, isStanceSkill, sanitizeSkillArgs } from '../skills';
import { normalizeToolName, wildcardMatch } from '../permissions';
import { resolveSkillModelOverride } from '../model-capabilities';

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

/** A permission rule keyed by tool name (the subset the skill gate consults). */
type SkillPermissionRule = { tool: string; pattern?: string; action: 'allow' | 'deny' | 'ask' };

/** The per-skill permission decision: an EXPLICIT operator rule matched the skill
 *  name, or `undefined` when no `Skill(...)` rule applies (fall through to the
 *  generic skill-tool permission gate, which still enforces the default-ask). */
export type SkillPermissionDecision = 'allow' | 'deny' | 'ask';

/** Does a `Skill(...)` rule's pattern match this skill name? A rule with no
 *  pattern (`{tool:'skill', action:'deny'}`) matches EVERY skill; otherwise the
 *  pattern is a glob so exact names and `name:*` prefix scopes both work. Reuses
 *  the canonical {@link wildcardMatch} — no local matcher. */
function skillRuleMatches(pattern: string | undefined, skillName: string): boolean {
  if (pattern === undefined || pattern === '') return true;
  return wildcardMatch(pattern, skillName);
}

/**
 * Evaluate per-skill permission rules keyed by skill NAME, mirroring Claude Code's
 * SkillTool.checkPermissions (deny rules first, then allow rules, with exact and
 * `name:*` prefix matching). This is the per-skill deny/allow/ask surface AMI
 * previously lacked — an operator can now `deny: ["Skill(dangerous)"]` or scope
 * `allow: ["Skill(project:*)"]` which model-invokable skills may auto-run.
 *
 * Returns `undefined` when NO `Skill(...)` rule matches, so the default behavior is
 * unchanged: the generic skill-tool permission gate (the `skill` tool is
 * non-read-only) still provides Claude's default-ask. Only an explicit matching
 * rule overrides that. Pure + exported for testability.
 */
export function evaluateSkillPermission(
  skillName: string,
  rules: ReadonlyArray<SkillPermissionRule> | undefined,
): SkillPermissionDecision | undefined {
  if (!rules || rules.length === 0) return undefined;
  const skillRules = rules.filter(r => normalizeToolName(r.tool) === 'skill');
  if (skillRules.length === 0) return undefined;
  // Deny wins over allow wins over ask (defense-in-depth, Claude's order).
  for (const order of ['deny', 'allow', 'ask'] as const) {
    for (const r of skillRules) {
      if (r.action === order && skillRuleMatches(r.pattern, skillName)) return order;
    }
  }
  return undefined;
}

/**
 * A diagnostic produced while resolving a skill's tool restriction list — surfaced
 * so a misconfigured `allowed-tools`/`disallowed-tools` (a typo, a disconnected MCP
 * tool, or a whitelist that resolves to nothing) is VISIBLE instead of silently
 * degrading the subagent's toolset. Mirrors Claude Code's validation/selected-count
 * affordance (ToolSelector drops unresolved names but still surfaces the count).
 */
export interface SkillToolFilterWarning {
  kind: 'unknown-tool' | 'empty-toolset';
  /** The offending allow/deny entry (verbatim), when `kind === 'unknown-tool'`. */
  entry?: string;
  /** Which restriction list the entry came from. */
  list?: 'allowed' | 'disallowed';
  message: string;
}

/**
 * Normalize a single `allowed-tools`/`disallowed-tools` entry to an AMI tool name.
 *
 * Accepts Claude Code's authoring forms and maps them onto AMI's canonical tool
 * names so a skill/command imported verbatim resolves to the right tools instead
 * of silently matching nothing:
 * - **Scoped command patterns** `Tool(scope:*)` → the tool-name portion (`Tool`).
 *   The scope is a permission concern handled by the permission engine, not the
 *   tool-list filter, so only the name is used for inclusion here.
 * - **Claude-cased / aliased names** (`Bash`, `Read`, `Edit`, `Grep`, `LS`, …) →
 *   AMI names (`bash`, `file_read`, `file_edit`, `grep`, `list_dir`, …) via the
 *   shared `normalizeToolName` alias map (single source of truth — no local copy).
 */
function normalizeSkillToolEntry(entry: string): string {
  const paren = entry.indexOf('(');
  const base = (paren >= 0 ? entry.slice(0, paren) : entry).trim();
  return normalizeToolName(base);
}

/**
 * Filter tools for a skill based on its allowedTools and disallowedTools settings.
 * Exported for testability.
 *
 * Logic:
 * 1. A list containing the `*` wildcard means "all tools" — skip the allowlist
 *    filter (Claude Code's `tools: ['*']` sentinel; AMI's convention of an empty
 *    list already means "all", so `*` is treated identically).
 * 2. If allowedTools is set, whitelist to only those tools (entries normalized via
 *    {@link normalizeSkillToolEntry}, so scoped `Tool(scope:*)` and Claude-cased
 *    names resolve to the correct AMI tool).
 * 3. Otherwise, include all tools minus 'skill'.
 * 4. If disallowedTools is set, remove those tools from the result.
 *
 * `onWarning` (optional) receives a diagnostic for every allow/deny entry that
 * resolves to no real tool, and one when the effective toolset ends up empty, so
 * a misconfigured restriction is surfaced rather than silently applied.
 */
export function filterToolsForSkill(
  allTools: ToolDefinition[],
  skill: SkillDefinition | null | undefined,
  onWarning?: (warning: SkillToolFilterWarning) => void,
): ToolDefinition[] {
  let tools: ToolDefinition[];

  const realNames = new Set(allTools.map(t => t.name));
  const allowedEntries = (skill?.allowedTools ?? []).filter(e => e && e.trim().length > 0);
  const hasWildcard = allowedEntries.some(e => normalizeSkillToolEntry(e) === '*');

  if (allowedEntries.length > 0 && !hasWildcard) {
    const allowed = new Set<string>();
    for (const entry of allowedEntries) {
      const name = normalizeSkillToolEntry(entry);
      allowed.add(name);
      if (!realNames.has(name)) {
        onWarning?.({
          kind: 'unknown-tool',
          entry,
          list: 'allowed',
          message: `allowed-tools entry "${entry}" matches no available tool`,
        });
      }
    }
    tools = allTools.filter(t => allowed.has(t.name));
  } else {
    tools = allTools.filter(t => t.name !== 'skill');
  }

  const disallowedEntries = (skill?.disallowedTools ?? []).filter(e => e && e.trim().length > 0);
  if (disallowedEntries.length > 0) {
    const disallowed = new Set<string>();
    for (const entry of disallowedEntries) {
      const name = normalizeSkillToolEntry(entry);
      disallowed.add(name);
      if (!realNames.has(name)) {
        onWarning?.({
          kind: 'unknown-tool',
          entry,
          list: 'disallowed',
          message: `disallowed-tools entry "${entry}" matches no available tool`,
        });
      }
    }
    tools = tools.filter(t => !disallowed.has(t.name));
  }

  if (tools.length === 0) {
    onWarning?.({
      kind: 'empty-toolset',
      message: 'skill tool restrictions resolved to zero tools — the subagent will have no tools to act with',
    });
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

    // FUNCTIONAL (parity/security): per-skill permission gating keyed by skill NAME.
    // An operator `deny: ["Skill(name)"]` / `allow: ["Skill(scope:*)"]` rule decides
    // which model-invokable skills may auto-run BEFORE the skill executes (inline or
    // forked). No matching rule → undefined → the generic skill-tool gate still
    // applies the default-ask. Mirrors Claude's SkillTool.checkPermissions.
    const permDecision = evaluateSkillPermission(resolvedName, context._parentPermissionRules);
    if (permDecision === 'deny') {
      return {
        output: `Error: skill "${resolvedName}" is blocked by a permission rule (deny). An operator has denied this skill's invocation.`,
        isError: true,
      };
    }
    if (permDecision === 'ask') {
      const handler = context._permissionPromptHandler;
      if (handler) {
        const verdict = await handler.showPrompt(
          'skill',
          `skill: ${resolvedName}${safeArgs?.ARGUMENTS ? ` ${safeArgs.ARGUMENTS}` : ''}`,
          undefined,
          context.abortSignal,
        );
        if (!verdict.action.startsWith('allow')) {
          return {
            output: `Skill "${resolvedName}" was not approved${verdict.feedback ? `: ${verdict.feedback}` : '.'}`,
            isError: true,
          };
        }
      }
    }

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
    const toolWarnings: SkillToolFilterWarning[] = [];
    const tools = filterToolsForSkill(allTools.getAll(), skill, w => toolWarnings.push(w));
    const warningText =
      toolWarnings.length > 0
        ? `\n[Skill tool-restriction warnings: ${toolWarnings.map(w => w.message).join('; ')}]`
        : '';

    const subAbort = new AbortController();
    const forwardAbort = () => subAbort.abort();
    context.abortSignal.addEventListener('abort', forwardAbort, { once: true });

    const providerConfig = { ...(context._providerConfig || { baseUrl: '', apiKey: '', model: '' }) };
    if (skill?.model) {
      // Carry the session's [1m] 1M-context marker across the skill model override
      // when the target family supports it — a raw overwrite would silently drop an
      // `opus[1m]` session to the 200k window and trip autocompact. See
      // resolveSkillModelOverride.
      providerConfig.model = resolveSkillModelOverride(skill.model, providerConfig.model || '');
    }

    const subConfig: EngineConfig = {
      provider: providerConfig,
      cwd: context.cwd,
      tools,
      sessionId: SessionManager.newId(),
      // FUNCTIONAL (parity/security): do NOT hard-code 'auto-allow' for the forked
      // sub-engine — that suppressed every permission prompt and let a lower-trust
      // forked skill run arbitrary tools (privilege escalation). Default to 'ask'
      // (parity with task.ts's sub-agent fallback) so forked-skill tool calls stay
      // gated by the parent's deny/ask rules + forwarded prompt handler (YOLO/TUI
      // propagation flows through the handler). A trusted skill may still opt into
      // 'auto-allow' explicitly via its `permissionMode`.
      permissionMode: skill?.permissionMode ?? 'ask',
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

    const header = `[Skill: ${resolvedName} · ${tools.length} tools · model ${providerConfig.model || 'default'}]${warningText}`;
    return {
      output: `${header}\n${capSkillOutput(result) || '(skill produced no output)'}`,
      isError: false,
      metadata: {
        mode: 'forked',
        skill: resolvedName,
        model: providerConfig.model,
        toolCount: tools.length,
        ...(toolWarnings.length > 0 ? { toolWarnings } : {}),
      },
    };
  },
};

/** Hard cap on how much forked-skill output is echoed back into the context. */
export const SKILL_MAX_RESULT_CHARS = 100_000;

function capSkillOutput(text: string): string {
  if (text.length <= SKILL_MAX_RESULT_CHARS) return text;
  return text.slice(0, SKILL_MAX_RESULT_CHARS) + '\n[... skill output truncated ...]';
}
