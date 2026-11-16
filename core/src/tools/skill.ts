import { dirname } from 'node:path';
import { ToolDefinition, ToolContext, ToolResult, EngineConfig } from '../types';
import { createDefaultTools } from './index';
import { SessionManager } from '../session';
import { type SkillDefinition, type SkillManagerLike, type AgentDefinition, isStanceSkill, sanitizeSkillArgs, missingSkillArgumentMessage, redactSkillArgs, isSkillAvailableNonInteractive } from '../skills';
import { normalizeToolName, wildcardMatch } from '../permissions';
import { resolveSkillModelOverride } from '../model-capabilities';
import { parseMcpToolName } from './mcp-tool';

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
 * Allowlist of {@link SkillDefinition} property keys that are REVIEWED-SAFE — a
 * skill carrying only these may auto-run without an extra approval. Mirrors Claude
 * Code's `SAFE_SKILL_PROPERTIES` fail-closed design (SkillTool.ts:875-908): any
 * property NOT on this list that carries a meaningful value forces the skill
 * through the permission gate, so new/injected skill metadata (e.g. an
 * unreviewed field from an installed URL catalog or plugin bundle that tries to
 * influence model/effort/tools) can never silently gain auto-run privileges —
 * it defaults to requiring explicit permission until the property is reviewed and
 * added here. Kept in sync with the fields declared on {@link SkillDefinition}.
 */
export const SAFE_SKILL_PROPERTIES: ReadonlySet<string> = new Set<string>([
  'name',
  'description',
  'content',
  'filePath',
  'version',
  'whenToUse',
  'paths',
  'model',
  'userInvocable',
  'allowedTools',
  'disallowedTools',
  'argumentHint',
  'argumentNames',
  'context',
  'disableModelInvocation',
  'effort',
  'agent',
  'permissionMode',
  'autoAllowPatterns',
  'mcpServers',
  'mcpAutoAllowPatterns',
  'mcpToolGuidance',
  'defaultThinkingLevel',
  // U023 skill-metadata properties (parity): secondary-name aliases, a
  // runtime availability predicate, and required-argument declarations. These
  // are declarative metadata consumed by the loader/skill tool, not code the
  // catalog can inject to auto-run — safe to allow without explicit approval.
  'aliases',
  'isEnabled',
  'requiresArgument',
  'missingArgumentMessage',
  // U023-w67 skill-metadata flags (parity): sensitive-arg masking, help-listing
  // hiding, a display-name override, and non-interactive gating — declarative
  // metadata the loader/skill tool consumes, not injectable auto-run code.
  'isSensitive',
  'isHidden',
  'userFacingName',
  'disableNonInteractive',
  'supportsNonInteractive',
]);

/**
 * Return the skill's property keys that are OUTSIDE {@link SAFE_SKILL_PROPERTIES}
 * AND carry a meaningful value (not undefined/null, not an empty array/object).
 * An empty result means the skill uses only reviewed-safe properties and may
 * auto-run. Mirrors Claude Code's `skillHasOnlySafeProperties` value check.
 * Pure + exported for testability.
 */
export function unsafeSkillProperties(skill: SkillDefinition): string[] {
  const offending: string[] = [];
  for (const key of Object.keys(skill)) {
    if (SAFE_SKILL_PROPERTIES.has(key)) continue;
    const value = (skill as unknown as Record<string, unknown>)[key];
    if (value === undefined || value === null) continue;
    if (Array.isArray(value) && value.length === 0) continue;
    if (typeof value === 'object' && !Array.isArray(value) && Object.keys(value).length === 0) continue;
    offending.push(key);
  }
  return offending;
}

/** True when the skill carries only reviewed-safe properties (auto-run eligible). */
export function skillHasOnlySafeProperties(skill: SkillDefinition): boolean {
  return unsafeSkillProperties(skill).length === 0;
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
 * Coarse tool-class taxonomy (parity with Claude Code's ToolSelector getToolBuckets:
 * Read-only / Edit / Execution / MCP / Other). Lets a skill/agent allowlist be
 * authored and reasoned about by tool CLASS ("allow all read-only + edit") instead
 * of enumerating every tool name. Derived from the tool's own `isMcp`/`isReadOnly`
 * flags plus a small name set for the edit/execution classes (mutating file tools
 * and shell/process tools are never read-only, so they can't be auto-classified by
 * the flags alone) — mirroring Claude's hard-coded bucket membership.
 */
export type ToolCategory = 'read-only' | 'edit' | 'execution' | 'mcp' | 'other';

/** Tools that MUTATE files on disk — the "edit" bucket. */
const EDIT_TOOL_NAMES: ReadonlySet<string> = new Set(['file_edit', 'file_write', 'notebook_edit']);
/** Tools that EXECUTE shell / processes — the "execution" bucket. */
const EXECUTION_TOOL_NAMES: ReadonlySet<string> = new Set(['bash', 'bash_output', 'kill_shell']);

/** The category-token spellings accepted in an `allowed-tools`/`disallowed-tools`
 *  list (case-insensitive). An entry matching one of these selects the whole class
 *  rather than a single tool by name. */
export const TOOL_CATEGORY_TOKENS: ReadonlySet<string> = new Set<ToolCategory>([
  'read-only',
  'edit',
  'execution',
  'mcp',
  'other',
]);

/** Classify a tool into its taxonomy bucket (parity with getToolBuckets). */
export function toolCategory(tool: ToolDefinition): ToolCategory {
  if (tool.isMcp) return 'mcp';
  if (EDIT_TOOL_NAMES.has(tool.name)) return 'edit';
  if (EXECUTION_TOOL_NAMES.has(tool.name)) return 'execution';
  if (tool.isReadOnly) return 'read-only';
  return 'other';
}

/** The parsed form of a single allow/deny list entry: either a tool-CLASS token
 *  (expands to every tool in that category) or a single normalized tool NAME. */
type ParsedSkillToolEntry = { category: ToolCategory } | { name: string };

/**
 * Parse a single `allowed-tools`/`disallowed-tools` entry.
 *
 * Accepts Claude Code's authoring forms and maps them onto AMI concepts so a
 * skill/command imported verbatim resolves to the right tools instead of silently
 * matching nothing:
 * - **Category tokens** (`read-only`, `edit`, `execution`, `mcp`, `other`) → the
 *   whole tool class (see {@link toolCategory}).
 * - **Scoped command patterns** `Tool(scope:*)` → the tool-name portion (`Tool`).
 *   The scope is a permission concern handled by the permission engine, not the
 *   tool-list filter, so only the name is used for inclusion here.
 * - **Claude-cased / aliased names** (`Bash`, `Read`, `Edit`, `Grep`, `LS`, …) →
 *   AMI names (`bash`, `file_read`, `file_edit`, `grep`, `list_dir`, …) via the
 *   shared `normalizeToolName` alias map (single source of truth — no local copy).
 */
function parseSkillToolEntry(entry: string): ParsedSkillToolEntry {
  const paren = entry.indexOf('(');
  const base = (paren >= 0 ? entry.slice(0, paren) : entry).trim();
  const lower = base.toLowerCase();
  if (TOOL_CATEGORY_TOKENS.has(lower)) return { category: lower as ToolCategory };
  return { name: normalizeToolName(base) };
}

/** Sentinel `filePath` values that are NOT real on-disk locations (built-ins and
 *  in-memory/test skills), so no base-directory pointer can be derived from them. */
function isRealSkillPath(filePath: string | undefined): filePath is string {
  return !!filePath && !filePath.startsWith('<') && filePath.includes('/');
}

/**
 * The directory a disk-based skill lives in — where its sibling reference files
 * (scripts, templates, docs the SKILL.md refers to) sit — or `null` for a built-in
 * or in-memory skill that ships no on-disk tree. Parity with Claude's automatic
 * "Base directory for this skill" pointer. Pure + exported for testability.
 */
export function skillBaseDir(skill: SkillDefinition | null | undefined): string | null {
  const fp = skill?.filePath;
  if (!isRealSkillPath(fp)) return null;
  const dir = dirname(fp);
  return dir && dir !== '.' ? dir : null;
}

/**
 * Prepend a "Base directory for this skill: <dir>" pointer to a disk-based skill's
 * content so the model can Read/Grep the skill's sibling reference files on demand
 * without the author having to template `${SKILL_DIR}` by hand (parity with Claude
 * Code's bundled/disk skill base-dir injection). No-op for built-in/in-memory
 * skills and idempotent (never double-prepends). Pure + exported for testability.
 */
export function withSkillBaseDir(content: string, skill: SkillDefinition | null | undefined): string {
  const dir = skillBaseDir(skill);
  if (!dir || content.includes('Base directory for this skill:')) return content;
  return `Base directory for this skill: ${dir}\n\n${content}`;
}

/**
 * Build the skill-activation byline, surfacing the sandboxing a skill applies at the
 * moment it activates (parity with Claude's renderToolResultMessage byline): the
 * number of tools the skill is scoped to (when it restricts its toolset) and the
 * model it is pinned to (when non-default). A `·`-joined one-liner; the base message
 * is always first. Pure + exported for testability.
 */
export function skillActivationByline(
  base: string,
  allowedToolCount: number | undefined,
  model: string | undefined,
): string {
  const parts = [base];
  if (allowedToolCount !== undefined) {
    parts.push(`${allowedToolCount} tool${allowedToolCount === 1 ? '' : 's'} allowed`);
  }
  if (model) parts.push(`model ${model}`);
  return parts.join(' · ');
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
 *    {@link parseSkillToolEntry}, so scoped `Tool(scope:*)` and Claude-cased
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
  const isWildcard = (e: string): boolean => {
    const p = parseSkillToolEntry(e);
    return 'name' in p && p.name === '*';
  };
  // Resolve one allow/deny entry to concrete tool names: a category token expands
  // to every tool in that class; a name resolves to itself (warning if it matches
  // no real tool). Category tokens are valid taxonomy and never warn.
  const resolveEntry = (entry: string, list: 'allowed' | 'disallowed', into: Set<string>): void => {
    const parsed = parseSkillToolEntry(entry);
    if ('category' in parsed) {
      for (const t of allTools) if (toolCategory(t) === parsed.category) into.add(t.name);
      return;
    }
    into.add(parsed.name);
    if (!realNames.has(parsed.name)) {
      onWarning?.({
        kind: 'unknown-tool',
        entry,
        list,
        message: `${list}-tools entry "${entry}" matches no available tool`,
      });
    }
  };

  const allowedEntries = (skill?.allowedTools ?? []).filter(e => e && e.trim().length > 0);
  const hasWildcard = allowedEntries.some(isWildcard);

  if (allowedEntries.length > 0 && !hasWildcard) {
    const allowed = new Set<string>();
    for (const entry of allowedEntries) resolveEntry(entry, 'allowed', allowed);
    tools = allTools.filter(t => allowed.has(t.name));
  } else {
    tools = allTools.filter(t => t.name !== 'skill');
  }

  const disallowedEntries = (skill?.disallowedTools ?? []).filter(e => e && e.trim().length > 0);
  if (disallowedEntries.length > 0) {
    const disallowed = new Set<string>();
    for (const entry of disallowedEntries) resolveEntry(entry, 'disallowed', disallowed);
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

    // FUNCTIONAL (parity/security): fail-closed safe-property allowlist. A skill
    // carrying an UNreviewed property (outside SAFE_SKILL_PROPERTIES) with a
    // meaningful value must not auto-run — it requires explicit approval unless an
    // operator `allow` rule already cleared it. Mirrors Claude's
    // skillHasOnlySafeProperties gate so crafted/injected skill metadata (e.g.
    // from an installed catalog or plugin bundle) defaults to ask, not run.
    const skillPreCleared = permDecision === 'allow' || !skillDef || skillHasOnlySafeProperties(skillDef);
    const unreviewedProps = skillPreCleared ? [] : unsafeSkillProperties(skillDef!);
    const hasUnreviewedProps = unreviewedProps.length > 0;

    if (permDecision === 'ask' || hasUnreviewedProps) {
      const handler = context._permissionPromptHandler;
      if (handler) {
        // FUNCTIONAL (parity/security): a skill flagged `isSensitive` must NOT leak
        // its arguments into the echoed permission summary — mask them as `***`
        // (Claude's isSensitive arg masking). redactSkillArgs is a no-op for a
        // non-sensitive skill or an empty argument.
        const echoedArgs = skillDef && safeArgs?.ARGUMENTS
          ? redactSkillArgs(skillDef, safeArgs.ARGUMENTS)
          : safeArgs?.ARGUMENTS;
        const summary = hasUnreviewedProps
          ? `skill: ${resolvedName} (unreviewed properties: ${unreviewedProps.join(', ')})`
          : `skill: ${resolvedName}${echoedArgs ? ` ${echoedArgs}` : ''}`;
        const verdict = await handler.showPrompt('skill', summary, undefined, context.abortSignal);
        if (!verdict.action.startsWith('allow')) {
          return {
            output: `Skill "${resolvedName}" was not approved${verdict.feedback ? `: ${verdict.feedback}` : '.'}`,
            isError: true,
          };
        }
      } else if (hasUnreviewedProps) {
        // Fail-closed: no approver available to clear the unreviewed metadata.
        return {
          output: `Error: skill "${resolvedName}" carries unreviewed properties (${unreviewedProps.join(', ')}) and requires explicit approval, but no permission handler is available to grant it.`,
          isError: true,
        };
      }
      // An operator `ask` rule with no handler proceeds (the generic skill-tool
      // gate already applied the default-ask) — behavior unchanged.
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

    // FUNCTIONAL (parity): a skill may be gated OUT of non-interactive (detached /
    // --prompt) runs via `disableNonInteractive` / `supportsNonInteractive`
    // (Claude's per-command non-interactive gate). Refuse it in detached mode
    // rather than running a skill that assumes an interactive session.
    if (skillDef && !isSkillAvailableNonInteractive(skillDef, context.detachedMode === true)) {
      return {
        output: `Error: skill "${resolvedName}" is not available in non-interactive (detached) mode.`,
        isError: true,
      };
    }

    // FUNCTIONAL (parity): a skill that declares `requires-argument` must not run
    // with an empty argument — return its custom help/examples message (or a
    // generated usage line) instead of invoking an argument-less body. Mirrors
    // Claude's required-argument skills (e.g. /batch's MISSING_INSTRUCTION_MESSAGE).
    if (skillDef?.requiresArgument && !(safeArgs?.ARGUMENTS && safeArgs.ARGUMENTS.trim().length > 0)) {
      return { output: missingSkillArgumentMessage(skillDef), isError: true };
    }

    const skillContent = context._skillManager.getSkillContent(resolvedName, safeArgs);

    if (context._mcpManager && !skillContent) {
      // FUNCTIONAL (parity/security): an MCP prompt is reachable via the skill tool
      // ONLY through its discoverable namespaced identifier `mcp__<server>__<prompt>`
      // (the same canonical name MCP tools carry) — NOT by guessing a bare prompt
      // name against every server. This closes the "technically reachable but not
      // discoverable" hole Claude closed in getAllCommands (only loadedFrom==='mcp'
      // prompt commands are resolvable), so a non-skill MCP prompt can no longer be
      // invoked by a guessed bare name.
      const parsed = parseMcpToolName(skillName);
      if (parsed?.toolName) {
        try {
          const mcpPrompts = await context._mcpManager.listPrompts?.();
          const mcpSkill = mcpPrompts?.find(
            (p: any) => p.server === parsed.serverName && p.name === parsed.toolName,
          );
          if (mcpSkill) {
            const promptResult = await context._mcpManager.getPrompt?.(parsed.serverName, parsed.toolName, safeArgs ?? {});
            if (promptResult) {
              return { output: `[MCP Skill: ${skillName}]\n${JSON.stringify(promptResult)}` };
            }
          }
        } catch (err) {
          // FUNCTIONAL (parity/errors): surface a transient MCP failure instead of
          // silently reporting "not found" — a server/list error is distinct from a
          // genuinely-missing skill and gives the model an actionable signal.
          // Mirrors Claude's `Failed to load remote skill <slug>: <msg>`.
          return {
            output: `Error: failed to load MCP skill "${skillName}": ${err instanceof Error ? err.message : String(err)}`,
            isError: true,
          };
        }
      }
    }

    if (!skillContent) {
      const available = context._skillManager.listSkills().map(s => s.name).join(', ');
      return { output: `Error: skill "${skillName}" not found. Available: ${available || '(none)'}`, isError: true };
    }

    const skill = skillDef;
    const isInline = skill?.context === 'inline' || !skill?.context;

    if (isInline && context._engineAddSystemReminder) {
      context._engineAddSystemReminder(`[Skill: ${resolvedName}]\n${withSkillBaseDir(skillContent, skill)}`);
      context._engineSetActiveSkill?.(resolvedName);
      // Report the skill's effective sandboxing (tool-restriction count + model
      // override) at activation, instead of a bare "activated inline" sentence, so
      // the user sees the skill scoped the toolset/model (parity with Claude's
      // skill-load byline). The count is only computed when the skill declares a
      // restriction; an unrestricted skill keeps the plain message.
      const restricts = (skill?.allowedTools?.length ?? 0) > 0 || (skill?.disallowedTools?.length ?? 0) > 0;
      const inlineToolCount = restricts
        ? filterToolsForSkill(createDefaultTools(context.cwd).getAll(), skill).length
        : undefined;
      return {
        output: skillActivationByline(
          `Skill "${resolvedName}" activated inline. Following its instructions.`,
          inlineToolCount,
          skill?.model,
        ),
        metadata: {
          mode: 'inline',
          skill: resolvedName,
          model: skill?.model,
          ...(inlineToolCount !== undefined ? { toolCount: inlineToolCount } : {}),
        },
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

    // FUNCTIONAL (parity): a forked skill may target a NAMED agent type
    // (SkillDefinition.agent, parity with Claude's PromptCommand.agent). When set,
    // the sub-engine is bound to that agent's system prompt (identity/persona) and
    // its model/maxTurns are used as a FALLBACK when the skill does not override
    // them — instead of a generic, agent-agnostic sub-engine. An unknown agent name
    // is a hard error so a typo never silently degrades to the default persona.
    let agentDef: AgentDefinition | null = null;
    if (skill?.agent) {
      agentDef = context._skillManager.getAgent?.(skill.agent) ?? null;
      if (!agentDef) {
        const available = context._skillManager.listAgents?.().map(a => a.name).join(', ') || '(none)';
        return {
          output: `Error: skill "${resolvedName}" targets agent "${skill.agent}", which is not registered. Available agents: ${available}.`,
          isError: true,
        };
      }
    }

    const providerConfig = { ...(context._providerConfig || { baseUrl: '', apiKey: '', model: '' }) };
    if (skill?.model) {
      // Carry the session's [1m] 1M-context marker across the skill model override
      // when the target family supports it — a raw overwrite would silently drop an
      // `opus[1m]` session to the 200k window and trip autocompact. See
      // resolveSkillModelOverride.
      providerConfig.model = resolveSkillModelOverride(skill.model, providerConfig.model || '');
    } else if (agentDef?.model && agentDef.model !== 'inherit') {
      // The skill did not pin a model → fall back to the target agent's model.
      providerConfig.model = resolveSkillModelOverride(agentDef.model, providerConfig.model || '');
    }

    const subConfig: EngineConfig = {
      provider: providerConfig,
      cwd: context.cwd,
      tools,
      // Bind the sub-engine to the target agent's system prompt (its persona), so a
      // fork carries the agent's identity rather than the generic coding stance.
      ...(agentDef?.systemPrompt ? { systemPrompt: agentDef.systemPrompt } : {}),
      ...(agentDef?.maxTurns ? { maxTurns: agentDef.maxTurns } : {}),
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
      for await (const event of subEngine.submit(withSkillBaseDir(skillContent, skill))) {
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
        metadata: { mode: 'forked', skill: resolvedName, model: providerConfig.model, toolCount: tools.length, ...(agentDef ? { agent: agentDef.name } : {}) },
      };
    } finally {
      context.abortSignal.removeEventListener('abort', forwardAbort);
      (subEngine as any).shutdown?.();
    }

    const agentNote = agentDef ? ` · agent ${agentDef.name}` : '';
    const header = `[Skill: ${resolvedName} · ${tools.length} tools · model ${providerConfig.model || 'default'}${agentNote}]${warningText}`;

    // FUNCTIONAL (parity/errors): a sub-skill that was aborted mid-run (parent
    // cancel forwarded into subAbort, or the parent signal itself aborted) ended
    // the for-await loop with only PARTIAL output. Report it as an ERROR, not a
    // spurious success — otherwise the model treats truncated/cancelled work as a
    // completed result. Mirrors Claude's runAgent post-drain abort check.
    if (subAbort.signal.aborted || context.abortSignal.aborted) {
      return {
        output: `${header}\nSkill "${resolvedName}" was cancelled before completion.${result ? `\n\n--- Partial output ---\n${capSkillOutput(result)}` : ''}`,
        isError: true,
        metadata: {
          mode: 'forked',
          skill: resolvedName,
          model: providerConfig.model,
          toolCount: tools.length,
          cancelled: true,
          ...(agentDef ? { agent: agentDef.name } : {}),
          ...(toolWarnings.length > 0 ? { toolWarnings } : {}),
        },
      };
    }

    return {
      output: `${header}\n${capSkillOutput(result) || '(skill produced no output)'}`,
      isError: false,
      metadata: {
        mode: 'forked',
        skill: resolvedName,
        model: providerConfig.model,
        toolCount: tools.length,
        ...(agentDef ? { agent: agentDef.name } : {}),
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
