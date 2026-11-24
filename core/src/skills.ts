// Skill/agent TYPES + argument-safety helpers — the bare-minimum skills
// building blocks that belong in core/ (a public, dependency-free library).
//
// The skills-framework MANAGEMENT layer (discovery, loading, catalog assembly,
// content resolution — the SkillManager) lives in common/src/skill-manager.ts,
// NOT here: it is orchestration glue that wires skills into the product, so it
// sits next to the engine and system-prompt builders that consume it. core/
// stays a bare library of parts.

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

import './skill-catalog';
import './utils/tokens';
import { escapeRegExp } from './utils/string';
import { AGENT_COLORS, AGENT_ISOLATION_MODES, AGENT_MEMORY_SCOPES, EFFORT_LEVELS, SYSTEM_PROMPT_MODES, isValidEffort, parseAgentColor, parseBooleanFlag, parseIsolationMode, parseMemoryScope } from './skills-validate-skill-args';
import type { AgentMemoryScope, AgentSource } from './skills-validate-skill-args';
import { AGENT_PERMISSION_MODES, BUBBLE_PERMISSION_MODE, parseSystemPromptMode, validateAgentType } from './skills-agent-definition';
import type { AgentDefinition, AgentValidationResult } from './skills-agent-definition';
import { agentMemoryScopeDisplay } from './skills-resolve-skill-name';
import { mcpToolNamespacePrefix } from './skills-classify-agent-mcp-server-specs';
export { AGENT_COLORS, AGENT_ISOLATION_MODES, AGENT_MEMORY_SCOPES, AGENT_SOURCES, EFFORT_LEVELS, MAX_SKILL_ARG_NAMES, MAX_SKILL_ARG_NAME_LENGTH, MAX_SKILL_ARG_TOTAL_CHARS, MAX_SKILL_ARG_VALUE_LENGTH, SKILL_KINDS, SYSTEM_PROMPT_MODES, coerceDescriptionToString, getAgentSourceDisplayName, isValidEffort, parseAgentColor, parseAgentSkills, parseAllowedAgentTypes, parseBackgroundFlag, parseBooleanFlag, parseEffort, parseIsolationMode, parseMemoryScope, sanitizeSkillArgs, validateSkillArgs } from './skills-validate-skill-args';
export type { AgentColorName, AgentIsolationMode, AgentMemoryScope, AgentSource, AgentSourceSelector, EffortLevel, SkillKind, SystemPromptMode } from './skills-validate-skill-args';
export { AGENT_PERMISSION_MODES, BUBBLE_PERMISSION_MODE, DEFAULT_SKILL_PROGRESS_MESSAGE, REDACTED_SKILL_ARGS, SKILL_LOADING_PROGRESS_MESSAGE, agentPluginRoot, canSpawnAgentType, defaultProgressMessage, filterAgentsByMcpRequirements, hasRequiredMcpServers, parseArgumentNames, parseSkillAliases, parseSystemPromptMode, requiredEnvSatisfied, resolveAgentSystemPrompt, validateAgentType, validateSkillName } from './skills-agent-definition';
export type { AgentDefinition, AgentValidationResult } from './skills-agent-definition';
export { FRONTMATTER_SHELLS, PRELOADED_SKILLS_HEADING, formatSkillUpdates, getSkillFile, isSkillEnabled, isStanceSkill, isValidSkillImprovement, missingSkillArgumentMessage, parseSkillUpdates, progressMessageForKind, skillProgressMessage } from './skills-skill-definition';
export type { FrontmatterShell, PreloadedSkillsResult, SkillDefinition, SkillImprovementSuggestion, SkillManagerLike, SkillUpdate } from './skills-skill-definition';
export { DEFAULT_SHELL, agentFileNameMismatch, agentMemoryScopeDisplay, applySkillTemplateVariables, buildPreloadedSkillsBlock, estimateSkillFrontmatterTokens, formatAgentMemoryDimension, formatToolNameList, isBuiltInAgent, isSkillAvailableNonInteractive, isSkillHidden, isSkillImmediate, parseShellFrontmatter, progressiveArgumentHint, redactSkillArgs, resolveSkillName, resolveSkillPrompt, shellForExecution, skillKind, skillPluginRoot, skillUserFacingName } from './skills-resolve-skill-name';
export { allowlistGrantsBash, buildInlineReference, classifyAgentMcpServerSpecs, formatAgentSkillsList, getFilesForLanguage, isBashToolAllowRule, mcpToolNamespacePrefix, requiredMcpServerReadiness, resolveAgentModelDisplay, scopedToolAllowRules } from './skills-classify-agent-mcp-server-specs';
export type { AgentMcpServerSpec, ClassifiedAgentMcpServers, McpServerReadiness, RawAgentMcpServers } from './skills-classify-agent-mcp-server-specs';
export { isMcpToolOfServer } from './skills-is-mcp-tool-of-server';

/**
 * The property names a skill/command definition may carry that are known to be
 * SAFE — i.e. they do not themselves grant a capability that should require user
 * consent (parity with the reference's `SAFE_SKILL_PROPERTIES`). Any property
 * OUTSIDE this allowlist that carries a meaningful (non-empty) value means the
 * skill is doing something new/unknown and must not be auto-allowed. This is
 * forward-compatible by construction: a newly-added-but-unlisted property forces
 * the permission path rather than silently auto-allowing.
 */
export const SAFE_SKILL_PROPERTIES: ReadonlySet<string> = new Set([
  // Prompt/command properties
  'type', 'progressMessage', 'contentLength', 'content', 'argNames',
  'argumentNames', 'model', 'effort', 'source', 'pluginInfo',
  'disableNonInteractive', 'supportsNonInteractive', 'skillRoot', 'context',
  'agent', 'getPromptForCommand', 'frontmatterKeys', 'files',
  // Base command/skill properties
  'name', 'description', 'hasUserSpecifiedDescription', 'isEnabled', 'isHidden',
  'requiredEnv',
  'aliases', 'isMcp', 'argumentHint', 'whenToUse', 'paths', 'version',
  'disableModelInvocation', 'userInvocable', 'loadedFrom', 'filePath',
  'immediate', 'userFacingName', 'requiresArgument', 'missingArgumentMessage',
  'requiresGitRepo', 'sandboxReadOnlyBash', 'sandboxWriteRoots',
  'isSensitive', 'disableNonInteractive', 'supportsNonInteractive',
  'shell', 'templateVars', 'kind',
  // Stance/tool-restriction properties (reviewed-safe declarative metadata the
  // loader + skill tool consume to SCOPE a skill's toolset/permissions — they
  // narrow or configure, they are not injectable auto-run code). This block makes
  // the CANONICAL set the single source of truth for the skill-tool permission
  // gate too (core/core/src/tools/skill.ts imports it), replacing a diverged
  // second copy that omitted shell/templateVars/immediate/skillRoot/files/
  // getPromptForCommand/progressMessage — which made the gate wrongly route a
  // skill legitimately using those safe fields through the permission prompt.
  'allowedTools', 'disallowedTools', 'permissionMode', 'autoAllowPatterns',
  'mcpServers', 'mcpAutoAllowPatterns', 'mcpToolGuidance', 'defaultThinkingLevel',
  // Invocation-scoped frontmatter hooks (declarative config the SkillManager parses
  // + the sub-engine registers; trust-gated at parse/register time, not auto-run here).
  'hooks',
]);

/**
 * True when a skill/command object carries ONLY {@link SAFE_SKILL_PROPERTIES}
 * (parity with the reference's `skillHasOnlySafeProperties`). A property outside
 * the allowlist is tolerated only when it has no meaningful value (`undefined`,
 * `null`, an empty array, or an empty object); any meaningful unlisted value
 * returns `false` so the caller routes the skill through the permission gate.
 */
export function skillHasOnlySafeProperties(command: Record<string, unknown>): boolean {
  for (const key of Object.keys(command)) {
    if (SAFE_SKILL_PROPERTIES.has(key)) continue;
    const value = command[key];
    if (value === undefined || value === null) continue;
    if (Array.isArray(value) && value.length === 0) continue;
    if (
      typeof value === 'object' &&
      !Array.isArray(value) &&
      Object.keys(value as object).length === 0
    ) {
      continue;
    }
    return false;
  }
  return true;
}

/** The permission modes an agent/skill FILE frontmatter may legally DECLARE: every
 *  real engine mode ({@link AGENT_PERMISSION_MODES}) PLUS the {@link BUBBLE_PERMISSION_MODE}
 *  sentinel. This is the validation set for a declared `permission-mode:` (distinct
 *  from {@link AGENT_PERMISSION_MODES}, which stays the engine-mode lockstep set), so a
 *  definition declaring `permission-mode: bubble` validates instead of warning. */
export const AGENT_DECLARABLE_PERMISSION_MODES = [
  ...AGENT_PERMISSION_MODES, BUBBLE_PERMISSION_MODE,
] as const;

/**
 * Validate a (possibly partial / untrusted) agent definition, surfacing
 * structured errors + warnings instead of silently dropping or mis-loading it.
 *
 * Hard errors (→ `valid: false`): a missing/empty `name`, or a missing/empty
 * `systemPrompt` body.
 *
 * Warnings (non-fatal): an invalid `permissionMode`; a `model` not in
 * `opts.knownModels` (when provided); and any entry of `tools` / `disallowedTools`
 * not in `opts.knownTools` (when provided) — a typo'd tool name would otherwise
 * silently widen or misconfigure the allowlist that constrains what the subagent
 * may execute. Tool/model allowlists are optional so this stays a pure core
 * building block: the caller (common/) supplies the live tool + model sets.
 */
export function validateAgentDefinition(
  raw: Partial<AgentDefinition>,
  opts?: {
    knownTools?: readonly string[];
    knownModels?: readonly string[];
    knownSkills?: readonly string[];
    knownAgentTypes?: readonly string[];
  },
): AgentValidationResult {
  const errors: string[] = [];
  const warnings: string[] = [];

  if (typeof raw.name !== 'string' || raw.name.trim().length === 0) {
    errors.push('Agent definition is missing a name');
  } else {
    // Present but malformed name (bad charset / too short / too long): a non-fatal
    // warning so an on-disk agent still loads, while the author sees the problem.
    // A `/agents create` flow uses validateAgentType directly to reject it inline.
    const nameError = validateAgentType(raw.name.trim());
    if (nameError) warnings.push(nameError);
  }
  if (typeof raw.systemPrompt !== 'string' || raw.systemPrompt.trim().length === 0) {
    errors.push('Agent definition has an empty body (system prompt)');
  }

  if (
    raw.permissionMode !== undefined &&
    !(AGENT_DECLARABLE_PERMISSION_MODES as readonly string[]).includes(raw.permissionMode)
  ) {
    warnings.push(
      `Unknown permission mode "${raw.permissionMode}" (valid: ${AGENT_DECLARABLE_PERMISSION_MODES.join(', ')})`,
    );
  }

  if (
    opts?.knownModels &&
    raw.model !== undefined &&
    raw.model !== 'inherit' &&
    !opts.knownModels.includes(raw.model)
  ) {
    warnings.push(`Unknown model "${raw.model}"`);
  }

  if (opts?.knownTools) {
    const known = new Set(opts.knownTools);
    for (const tool of raw.tools ?? []) {
      if (!known.has(tool)) warnings.push(`Unknown tool in allowlist: "${tool}"`);
    }
    for (const tool of raw.disallowedTools ?? []) {
      if (!known.has(tool)) warnings.push(`Unknown tool in disallowed list: "${tool}"`);
    }
  }

  if (raw.color !== undefined && parseAgentColor(raw.color) === undefined) {
    warnings.push(`Unknown color "${raw.color}" (valid: ${AGENT_COLORS.join(', ')}, or "automatic")`);
  }
  if (raw.memory !== undefined && parseMemoryScope(raw.memory) === undefined) {
    warnings.push(`Unknown memory scope "${raw.memory}" (valid: ${AGENT_MEMORY_SCOPES.join(', ')})`);
  }
  if (raw.isolation !== undefined && parseIsolationMode(raw.isolation) === undefined) {
    warnings.push(`Unknown isolation mode "${raw.isolation}" (valid: ${AGENT_ISOLATION_MODES.join(', ')})`);
  }
  if (raw.effort !== undefined && !isValidEffort(raw.effort)) {
    warnings.push(`Invalid effort "${raw.effort}" (valid: ${EFFORT_LEVELS.join(', ')}, or a non-negative integer)`);
  }
  if (raw.systemPromptMode !== undefined && parseSystemPromptMode(raw.systemPromptMode) === undefined) {
    warnings.push(`Unknown systemPromptMode "${raw.systemPromptMode}" (valid: ${SYSTEM_PROMPT_MODES.join(', ')})`);
  }
  if (raw.descriptionColor !== undefined && parseAgentColor(raw.descriptionColor) === undefined) {
    warnings.push(`Unknown descriptionColor "${raw.descriptionColor}" (valid: ${AGENT_COLORS.join(', ')}, or "automatic")`);
  }
  if (raw.omitClaudeMd !== undefined && parseBooleanFlag(raw.omitClaudeMd).invalid) {
    warnings.push(`Invalid omitClaudeMd "${raw.omitClaudeMd}" (expected true/false)`);
  }
  if (raw.planModeRequired !== undefined && parseBooleanFlag(raw.planModeRequired).invalid) {
    warnings.push(`Invalid planModeRequired "${raw.planModeRequired}" (expected true/false)`);
  }
  if (raw.hooks !== undefined && !Array.isArray(raw.hooks)) {
    warnings.push('Invalid hooks (expected an array of hook config entries)');
  }

  if (opts?.knownSkills && raw.skills) {
    const known = new Set(opts.knownSkills);
    for (const skill of raw.skills) {
      if (!known.has(skill)) warnings.push(`Unknown preload skill: "${skill}"`);
    }
  }
  if (opts?.knownAgentTypes && raw.allowedAgentTypes) {
    const known = new Set(opts.knownAgentTypes);
    for (const type of raw.allowedAgentTypes) {
      if (!known.has(type)) warnings.push(`Unknown allowed agent type: "${type}"`);
    }
  }

  return { valid: errors.length === 0, errors, warnings };
}

/**
 * Namespace a server's advertised BARE tool names into the model-facing
 * `mcp__<server>__<tool>` allowlist entries (parity with the reference's
 * claude-in-chrome skill: `allowedTools = BROWSER_TOOLS.map(t =>
 * `mcp__<server>__${t.name}`)`). Lets a skill derive its allowlist from whatever
 * tools an MCP server currently exposes instead of hard-coding them. A blank/
 * non-string server name, or no tools, yields `[]`; blank/non-string tool names
 * are dropped and the result is de-duplicated in input order.
 */
export function deriveMcpToolAllowlist(
  serverName: string,
  toolNames: readonly string[] | undefined,
): string[] {
  if (typeof serverName !== 'string') return [];
  const server = serverName.trim();
  if (server.length === 0 || !toolNames || toolNames.length === 0) return [];
  const prefix = mcpToolNamespacePrefix(server);
  const seen = new Set<string>();
  const out: string[] = [];
  for (const raw of toolNames) {
    if (typeof raw !== 'string') continue;
    const tool = raw.trim();
    if (tool.length === 0) continue;
    const qualified = `${prefix}${tool}`;
    if (seen.has(qualified)) continue;
    seen.add(qualified);
    out.push(qualified);
  }
  return out;
}

/**
 * Parse a skill/command `allowedTools`/`disallowedTools` entry of the SERVER-
 * WILDCARD form `mcp__<server>__*` into the bare `<server>` name, or `null` when
 * the entry is not a server wildcard. Used by {@link filterToolsForSkill} to
 * expand such an entry against the LIVE advertised MCP tool set (via
 * {@link isMcpToolOfServer}), so a skill can allow/deny an entire MCP server's
 * tools without enumerating them. A blank/non-string entry, or one whose server
 * segment is empty, yields `null`.
 */
export function parseMcpServerWildcard(entry: string): string | null {
  if (typeof entry !== 'string') return null;
  const m = /^mcp__(.+)__\*$/.exec(entry.trim());
  const server = m?.[1]?.trim();
  return server && server.length > 0 ? server : null;
}

/**
 * True when `content` references the argument placeholder `name` in EITHER the
 * braced `${name}` or the bare `$name` form (the same two forms
 * {@link substituteNamedArgPlaceholders} fills). The bare form uses the reference's
 * word-boundary guard so `$name` inside `$namexyz` / `$name[0]` does not count.
 * Shared by the content path's "did the template already consume the args?" check
 * so the two placeholder forms never diverge from what substitution actually binds.
 */
export function contentReferencesArgName(content: string, name: string): boolean {
  if (typeof content !== 'string' || typeof name !== 'string' || name.length === 0) {
    return false;
  }
  const esc = escapeRegExp(name);
  return (
    content.includes(`\${${name}}`) ||
    new RegExp(`\\$${esc}(?![\\[\\w])`).test(content)
  );
}

/**
 * Substitute a skill/command's declared positional-argument NAMES into its body
 * (parity with the reference's `substituteArguments`). For each declared name at
 * index `i`, both the braced `${name}` and the bare `$name` placeholder are
 * replaced with `tokens[i]` (or `''` when no token was supplied, so a placeholder
 * is never left literal). The bare form uses the reference's word-boundary guard
 * `\$name(?![\[\w])` so `$name` inside `$namexyz` or `$name[0]` is NOT touched. A
 * name listed in `skipNames` (an argument the caller binds explicitly elsewhere)
 * is left untouched here. Names are escaped so a regex metacharacter is literal.
 */
export function substituteNamedArgPlaceholders(
  content: string,
  argumentNames: readonly string[] | undefined,
  tokens: readonly string[],
  skipNames: readonly string[] = [],
): string {
  if (!argumentNames || argumentNames.length === 0 || typeof content !== 'string') {
    return content;
  }
  const skip = new Set(skipNames);
  let out = content;
  argumentNames.forEach((argName, i) => {
    if (skip.has(argName)) return;
    const value = tokens[i] ?? '';
    const esc = escapeRegExp(argName);
    // Braced ${name}
    out = out.replace(new RegExp(`\\$\\{${esc}\\}`, 'g'), () => value);
    // Bare $name — not followed by a word char or `[` (so $name[0] / $namexyz stay)
    out = out.replace(new RegExp(`\\$${esc}(?![\\[\\w])`, 'g'), () => value);
  });
  return out;
}

// ---------------------------------------------------------------------------
// Agent source-based editability + composite identity, and location-aware
// agent-memory-scope recommendation — additive core building blocks mirroring
// the reference capabilities the skills library still lacked:
//   - source-based editability special-casing (AgentsMenu `isEditable`: a built-in,
//     plugin, or CLI-flag-injected agent is read-only; a user/project/local/managed
//     agent is editable).
//   - composite (agentType, source) agent identity (AgentsMenu dedup + selection
//     key, so two same-named agents from different sources stay distinct).
//   - the wizard's location-aware recommended memory scope + ordered scope options
//     (MemoryStep: a user-scoped agent recommends user memory, otherwise project).
// Pure predicates/formatters; the `/agents` listing/editor + agent-creation flow
// wire them into the real paths. They reuse {@link AGENT_MEMORY_SCOPES} /
// {@link agentMemoryScopeDisplay} so there is no second scope/label source.
// ---------------------------------------------------------------------------

/**
 * Agent provenance tokens whose definitions are READ-ONLY (not user-editable) in
 * the `/agents` editor (parity with the reference's AgentsMenu `isEditable`, which
 * excludes `built-in`, `plugin`, the policy-managed source, and the CLI-flag-injected
 * source). Kept as a STRING set — not {@link AgentSource}-typed — so a forward-added
 * non-editable tier stays non-editable; a `user`/`project`/`local` agent is editable
 * (an author-owned file), while a `managed` (enterprise/policy) or `flag` (per-run CLI)
 * agent is not.
 */
export const AGENT_NONEDITABLE_SOURCES: ReadonlySet<string> = new Set([
  'built-in', 'plugin', 'flag', 'managed',
]);

/**
 * True when an agent definition may be EDITED through the `/agents` editor (parity
 * with the reference's AgentsMenu `isEditable = source !== 'built-in' && source !==
 * 'plugin' && source !== 'flagSettings'`). An agent with no (or an unknown-blank)
 * source is treated as non-editable so an unprovenanced definition is never offered
 * for in-place editing; every known source outside {@link AGENT_NONEDITABLE_SOURCES}
 * is editable.
 */
export function isAgentEditable(agent: Pick<AgentDefinition, 'source'>): boolean {
  const src = agent.source;
  if (typeof src !== 'string' || src.length === 0) return false;
  return !AGENT_NONEDITABLE_SOURCES.has(src);
}

/**
 * The COMPOSITE identity of an agent — its canonical {@link AgentDefinition.name}
 * AND its {@link AgentDefinition.source} (parity with the reference's AgentsMenu,
 * which keys dedup + selection by `(agentType, source)` rather than name alone, so
 * two same-named agents from different sources stay distinct instead of one
 * silently shadowing the other in a selection/dedup set). An absent source
 * defaults to `'built-in'`. The two parts are joined with a NUL so no name/source
 * value can forge a collision.
 */
export function agentIdentityKey(
  agent: Pick<AgentDefinition, 'name' | 'source'>,
): string {
  const source = typeof agent.source === 'string' && agent.source.length > 0
    ? agent.source
    : 'built-in';
  return `${agent.name}\u0000${source}`;
}

/**
 * The recommended persistent-memory {@link AgentMemoryScope} for an agent given
 * WHERE it lives (parity with the reference's MemoryStep, which recommends the scope
 * matching the agent's location). A `'user'`-sourced agent recommends `'user'`
 * memory; a `'local'`-sourced (project-local, non-shared) agent recommends `'local'`
 * memory so its persistence stays as unshared as the agent itself; every other source
 * (project/managed/plugin/built-in/flag/absent) recommends `'project'` memory.
 */
export function recommendedAgentMemoryScope(
  source: AgentSource | undefined,
): AgentMemoryScope {
  if (source === 'user') return 'user';
  if (source === 'local') return 'local';
  return 'project';
}

/** One selectable agent-memory-scope option for the creation wizard: the scope
 *  value (or the `'none'` opt-out), its descriptive label, and whether it is the
 *  recommended default. */
export interface AgentMemoryScopeOption {
  value: AgentMemoryScope | 'none';
  label: string;
  recommended: boolean;
}

/**
 * The ordered agent-memory-scope options for the creation wizard, given the agent's
 * source/location (parity with the reference's MemoryStep option list): the
 * location-matched {@link recommendedAgentMemoryScope} FIRST — flagged recommended
 * and labelled `… (Recommended)` — then the `'none'` (no persistent memory)
 * opt-out, then every remaining {@link AGENT_MEMORY_SCOPES} entry in declaration
 * order. Each scope's descriptive label is sourced from the single
 * {@link agentMemoryScopeDisplay} map so wording never diverges.
 */
export function agentMemoryScopeOptions(
  source: AgentSource | undefined,
): AgentMemoryScopeOption[] {
  const recommended = recommendedAgentMemoryScope(source);
  const label = (scope: AgentMemoryScope): string => agentMemoryScopeDisplay(scope) ?? scope;
  const options: AgentMemoryScopeOption[] = [
    { value: recommended, label: `${label(recommended)} (Recommended)`, recommended: true },
    { value: 'none', label: 'None (no persistent memory)', recommended: false },
  ];
  for (const scope of AGENT_MEMORY_SCOPES) {
    if (scope === recommended) continue;
    options.push({ value: scope, label: label(scope), recommended: false });
  }
  return options;
}

/**
 * Agent-creation methods (parity with the reference's new-agent wizard MethodStep):
 * how a new agent definition is authored. `'generate'` has the model author the
 * agent from a natural-language description (the recommended default, listed
 * FIRST); `'manual'` hand-configures every field. Mirrors the
 * {@link AGENT_MEMORY_SCOPES} building-block pattern.
 */
export const AGENT_CREATION_METHODS = ['generate', 'manual'] as const;
export type AgentCreationMethod = (typeof AGENT_CREATION_METHODS)[number];

/**
 * Validate + normalize a raw creation-method value against the allowlist.
 * Case-insensitive; returns `undefined` for an unknown/non-string value so an
 * arbitrary method can never silently propagate. Mirrors {@link parseMemoryScope}.
 */
export function parseAgentCreationMethod(value: unknown): AgentCreationMethod | undefined {
  if (typeof value !== 'string') return undefined;
  const t = value.trim().toLowerCase();
  return (AGENT_CREATION_METHODS as readonly string[]).includes(t)
    ? (t as AgentCreationMethod)
    : undefined;
}

/**
 * A single selectable creation-method option for the new-agent flow (parity with
 * the reference's MethodStep `methodOptions`). `recommended` marks the default; `next`
 * names the step the flow branches to after this method is chosen (generate → the
 * generation step; manual → the manual-configuration step — mirroring the reference's
 * `goNext()` vs `goToStep(3)` branch), and `wasGenerated` is the provenance flag
 * stamped onto the resulting {@link AgentDefinition.wasGenerated}.
 */
export interface AgentCreationMethodOption {
  value: AgentCreationMethod;
  label: string;
  description: string;
  recommended: boolean;
  next: 'generate' | 'configure';
  wasGenerated: boolean;
}

/**
 * The ordered creation-method options for the new-agent flow — `generate`
 * (recommended, FIRST) then `manual` (parity with the reference's MethodStep ordering and
 * default). The caller (the `/agents create` slash command) renders these; the
 * `next`/`wasGenerated` metadata drives the post-selection branch.
 */
export function agentCreationMethodOptions(): AgentCreationMethodOption[] {
  return [
    {
      value: 'generate',
      label: 'Generate with AMI (Recommended)',
      description: 'Describe the agent in natural language and have the model author its definition.',
      recommended: true,
      next: 'generate',
      wasGenerated: true,
    },
    {
      value: 'manual',
      label: 'Manual configuration',
      description: 'Hand-configure the agent name, prompt, model, and tools yourself.',
      recommended: false,
      next: 'configure',
      wasGenerated: false,
    },
  ];
}
