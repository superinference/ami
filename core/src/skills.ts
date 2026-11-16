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

import type { McpServerConfig } from './mcp/manager';
import type { ThinkingLevel } from './model-capabilities';
import { isSafeRelativePath } from './skill-catalog';
import { estimateTokens } from './utils/tokens';

export interface SkillDefinition {
  name: string;
  description: string;
  content: string;        // Markdown body (the skill instructions)
  filePath: string;       // Where the skill file lives ('<builtin>' for built-ins)
  version?: string;       // Author-declared version string (frontmatter `version`), surfaced in /skills
  whenToUse?: string;     // Hint for auto-activation
  paths?: string[];       // File glob patterns that auto-activate this skill
  model?: string;         // Override model for this skill
  userInvocable?: boolean; // Can user invoke via /skill-name (default true)
  allowedTools?: string[]; // Restrict which tools this skill can use
  disallowedTools?: string[]; // Tools to exclude (blacklist, applied after allowedTools)
  argumentHint?: string;  // Hint text for the argument (e.g., "<file-path>")
  argumentNames?: string[]; // Declared positional argument names (frontmatter `arguments`), bound to $name placeholders
  /** Alternate names that ALSO resolve to this skill (parity with Claude's bundled
   *  skill `aliases`). Registered as secondary lookup keys so `getSkill`/`getSkillContent`
   *  and the skill tool accept any alias without a second definition. Never shadows a
   *  real skill name. Parsed via {@link parseSkillAliases}. */
  aliases?: string[];
  /** Runtime availability predicate (parity with Claude's bundled skill `isEnabled`).
   *  When present and it returns `false`, the skill is hidden from listings/context and
   *  cannot be invoked — letting a dependency/feature-gated skill ship but stay dormant
   *  until its precondition holds. Absent ⇒ always enabled. Evaluated via
   *  {@link isSkillEnabled} (a throwing predicate is treated as disabled). */
  isEnabled?: () => boolean;
  /** When true, invoking this skill with no argument is an error that returns
   *  {@link missingArgumentMessage} (or a generated help line) instead of running an
   *  argument-less skill body (parity with Claude's required-argument skills, e.g.
   *  `/batch`'s MISSING_INSTRUCTION_MESSAGE). */
  requiresArgument?: boolean;
  /** Custom help/examples message returned when a {@link requiresArgument} skill is
   *  invoked without an argument. Blank/absent ⇒ a generated usage line. */
  missingArgumentMessage?: string;
  /** Programmatic dynamic-prompt builder (parity with Claude's bundled-skill
   *  `getPromptForCommand(args)`, e.g. `/batch`, `/claude-api`). When present, the
   *  skill body is COMPUTED per invocation from the raw argument string (plus any
   *  runtime state the builder closes over) instead of the static {@link content}
   *  markdown — letting a skill validate args, lazy-load large reference material,
   *  or branch on the environment. May be sync or async. The content resolver
   *  prefers it over `content`; resolve via {@link resolveSkillPrompt}. */
  getPromptForCommand?: (args: string) => string | Promise<string>;
  /** Supporting resource files bundled WITH the skill (parity with Claude's bundled
   *  `SKILL_FILES`: a map of skill-relative path → inlined file content). The skill
   *  body may reference these companion docs/examples; read one safely (with
   *  path-traversal rejection) via {@link getSkillFile}. */
  files?: Record<string, string>;
  /** Affordance text shown while the skill is executing (parity with Claude's
   *  command/skill `progressMessage`, which defaults to `'running'`). Resolved —
   *  with the default fallback — via {@link skillProgressMessage}. */
  progressMessage?: string;
  /** When true, the skill's invocation ARGUMENTS are sensitive and must be masked
   *  (as `***`) wherever the invocation is echoed — the permission-prompt summary,
   *  transcript, and conversation history (parity with Claude's command
   *  `isSensitive`). Resolve a display-safe argument string via
   *  {@link redactSkillArgs}. */
  isSensitive?: boolean;
  /** Hide the skill from user-facing help/typeahead listings (parity with Claude's
   *  command `isHidden`). A hidden skill is STILL invocable by its exact name; it is
   *  only omitted from the `/skills` catalog. Checked via {@link isSkillHidden}. */
  isHidden?: boolean;
  /** Overrides the name shown for the skill in listings (parity with Claude's
   *  command `userFacingName()`) WITHOUT changing the canonical lookup `name`.
   *  Resolved — with the `name` fallback — via {@link skillUserFacingName}. */
  userFacingName?: string;
  /** When true, the skill is UNAVAILABLE in a non-interactive (detached / `--prompt`)
   *  run (parity with Claude's command `disableNonInteractive`). An explicit
   *  {@link supportsNonInteractive} overrides it. Gated via
   *  {@link isSkillAvailableNonInteractive}. */
  disableNonInteractive?: boolean;
  /** Tri-state non-interactive opt-in (parity with Claude's command
   *  `supportsNonInteractive`). `false` ⇒ unavailable in a non-interactive run;
   *  `true` ⇒ always available regardless of {@link disableNonInteractive}; absent ⇒
   *  defer to {@link disableNonInteractive}. Gated via
   *  {@link isSkillAvailableNonInteractive}. */
  supportsNonInteractive?: boolean;
  context?: 'inline' | 'fork'; // Execution mode: inline injects as system message, fork spawns subagent
  /** Named agent type a FORKED skill runs as (parity with Claude's PromptCommand
   *  `agent?: string`). When set, the forked sub-engine is bound to this agent's
   *  system prompt (and its model/maxTurns as a fallback when the skill does not
   *  override them) instead of a generic agent-agnostic sub-engine — so a fork can
   *  carry a specific persona/budget. Resolved via {@link SkillManagerLike.getAgent};
   *  an unknown agent name is a hard error at invocation. Ignored for inline skills. */
  agent?: string;
  disableModelInvocation?: boolean; // If true, LLM cannot invoke this skill via the skill tool
  effort?: string;         // Thinking effort level for sub-engine (e.g., "low", "medium", "high")
  /** Permission mode for this skill's FORKED sub-engine. Absent → the forked run
   *  defaults to 'ask' (parity with task.ts's sub-agent fallback) so forked-skill
   *  tool calls stay gated by the parent's deny/ask rules + prompt handler instead
   *  of the previous hard-coded 'auto-allow' bypass. A trusted skill may opt into
   *  'auto-allow' explicitly (mirrors AgentDefinition.permissionMode). */
  permissionMode?: 'auto-allow' | 'ask' | 'deny-all';

  // --- Stance capabilities (used when the skill is pinned as the session/run
  // stance via --skill) — a skill then governs the run the way a persona used to:
  // its body is the system-prompt overlay, plus the fields below. ---
  autoAllowPatterns?: string[];       // bash command globs auto-allowed while pinned
  mcpServers?: Record<string, McpServerConfig>; // MCP servers launched when pinned (e.g. kali)
  mcpAutoAllowPatterns?: string[];    // MCP tool-name globs auto-allowed while pinned
  mcpToolGuidance?: string;           // extra system-prompt guidance for the pinned MCP tools
  defaultThinkingLevel?: ThinkingLevel; // thinking level applied when pinned (unless user overrides)
}

/**
 * A skill is a *stance* when it carries stance capabilities — fields that change
 * the session's tools, permissions, MCP servers, or thinking level when the skill
 * is in effect (as opposed to a plain inline/fork skill or a workflow-doc skill
 * that only contributes instructions). Stance skills are the ones that can be
 * pinned via `--skill` OR dynamically activated by the model mid-session: when the
 * user's request matches a stance (e.g. "run the kali mcp to portscan" → pentest),
 * the engine switches into it, enabling its tooling (e.g. the Kali MCP server).
 *
 * This is the single discriminator that separates pinnable/activatable stances
 * (pentest, sre, research, swebench) from `disable-model-invocation` workflow docs
 * (review-changes-workflow, migration-workflow, …) that must NOT be activated as a
 * stance.
 */
export function isStanceSkill(skill: SkillDefinition): boolean {
  return !!(
    (skill.autoAllowPatterns && skill.autoAllowPatterns.length > 0) ||
    (skill.mcpServers && Object.keys(skill.mcpServers).length > 0) ||
    (skill.mcpAutoAllowPatterns && skill.mcpAutoAllowPatterns.length > 0) ||
    skill.defaultThinkingLevel
  );
}

/**
 * Where an agent definition was loaded from, in ascending priority order. A
 * higher-priority same-named agent SHADOWS a lower one (project > user > plugin >
 * built-in). Mirrors Claude Code's agent source groups so `/agents` can group and
 * flag shadowed entries instead of silently dropping the loser.
 */
export const AGENT_SOURCES = ['built-in', 'plugin', 'user', 'project'] as const;
export type AgentSource = (typeof AGENT_SOURCES)[number];

export interface AgentDefinition {
  name: string;
  description: string;
  systemPrompt: string;   // The agent's system prompt (markdown body)
  filePath: string;
  model?: string;         // Model override; the literal 'inherit' means "use the parent's model"
  maxTurns?: number;
  /** Allowed tool names. `undefined` ⇒ ALL tools (no restriction); a present but
   *  EMPTY array ⇒ NO tools (an explicit deny-everything allowlist) — the two are
   *  distinct, matching Claude Code's agent `tools` semantics (ConfirmStep). An
   *  empty allowlist resolves to a zero-tool set, which the task tool's zero-tool
   *  boundary guard then rejects with an actionable error rather than silently
   *  granting every tool. */
  tools?: string[];
  disallowedTools?: string[];
  /** Permission mode override for sub-agent invocation. */
  permissionMode?: 'auto-allow' | 'ask' | 'deny-all';
  /** Token budget for auto-compaction inside this subagent's context window. */
  autoCompactWindow?: number;
  /** Which tier this agent was loaded from (built-in | plugin | user | project). */
  source?: AgentSource;
  /** When a higher-priority same-named agent shadows this one, the winner's
   *  source. Undefined on the active (winning) agent. */
  overriddenBy?: AgentSource;
  /** Orchestrator delegation hint — the "when should the parent pick this agent"
   *  cue, distinct from `description` (parity with Claude's agent `whenToUse`).
   *  Parsed from a `when-to-use`/`when_to_use` frontmatter key. */
  whenToUse?: string;
  /** Per-agent display color used to tint the subagent's badge/label in listings
   *  (parity with Claude's agent `color`). `undefined` ⇒ the UI picks a default. */
  color?: AgentColorName;
  /** Persistent-memory storage scope for the agent (parity with Claude's agent
   *  `memory`): user / project / local. */
  memory?: AgentMemoryScope;
  /** Run the subagent in an isolated git worktree by default (parity with Claude's
   *  agent `isolation`). */
  isolation?: AgentIsolationMode;
  /** When true, spawning this agent defaults to background/detached execution
   *  unless the caller explicitly overrides `run_in_background` (parity with
   *  Claude's agent `background`). */
  background?: boolean;
  /** MCP server names this agent REQUIRES to be available before it can run
   *  (parity with Claude's agent `requiredMcpServers`). Each entry is matched
   *  case-insensitively as a substring against the live server names; when any
   *  requirement is unmet the agent is filtered out of the spawnable set. See
   *  {@link hasRequiredMcpServers} / {@link filterAgentsByMcpRequirements}. */
  requiredMcpServers?: string[];
  /** Per-subagent MCP servers to launch for this agent's run, additive to the
   *  parent's servers (parity with Claude's per-agent `mcpServers`). Mirrors the
   *  stance `mcpServers` shape on {@link SkillDefinition}. */
  mcpServers?: Record<string, McpServerConfig>;
  /** Names of skills to PRELOAD into this agent's context when it is spawned
   *  (parity with Claude's agent `skills` allowlist). Normalized via
   *  {@link parseAgentSkills}. */
  skills?: string[];
  /** Restrict which sub-agent *types* this agent may itself spawn via the task
   *  tool (parity with Claude's `allowedAgentTypes` metadata). `undefined`/empty
   *  ⇒ no restriction. Checked with {@link canSpawnAgentType}. */
  allowedAgentTypes?: string[];
  /** Default thinking/reasoning-effort budget for this subagent's run (parity with
   *  Claude's agent `effort`). A named level (`low`|`medium`|`high`|`xhigh`|`max`)
   *  OR the decimal string of a non-negative integer (raw token budget) — normalized
   *  at load time via {@link parseEffort}. Applied as the sub-engine's thinking level
   *  UNLESS the spawning task call passes an explicit per-call `effort` override. */
  effort?: string;
  /** Text auto-prepended to the agent's FIRST user turn when it is spawned (parity
   *  with Claude's agent `initialPrompt`). Lets an agent definition seed its run with
   *  a standing instruction ahead of the caller's task prompt. Only applied on the
   *  initial spawn, never when resuming an existing agent session. */
  initialPrompt?: string;
  /** Dynamic/computed system-prompt builder (parity with Claude's agent
   *  `getSystemPrompt({ toolUseContext })`). When present, the agent's system prompt
   *  is COMPUTED at spawn time from runtime context (e.g. the live set of custom
   *  skills/commands/agents/MCP servers) instead of the static {@link systemPrompt}
   *  string. May be sync or async; resolve via {@link resolveAgentSystemPrompt},
   *  which falls back to `systemPrompt` when the builder is absent. */
  getSystemPrompt?: (ctx?: Record<string, unknown>) => string | Promise<string>;
  /** Provenance flag recording that this agent definition was AUTO-GENERATED (e.g.
   *  by a creation wizard / model) rather than hand-authored (parity with Claude's
   *  agent `wasGenerated`). Surfaced in `/agents` so a user can tell generated
   *  definitions apart; parsed via {@link parseBooleanFlag}. */
  wasGenerated?: boolean;
  /** How this agent's {@link systemPrompt} combines with the base subagent stance
   *  when the agent is spawned (parity with Claude's swarm teammate
   *  `systemPromptMode`). `'append'` layers the agent's prompt AFTER the default
   *  subagent stance (the agent EXTENDS the base guidance); `'replace'`/`'default'`
   *  (and absent) make the agent's prompt the authoritative identity that fully
   *  REPLACES the base stance (AMI's existing behavior). Validated/normalized via
   *  {@link parseSystemPromptMode}; honored in the task tool's spawn path. */
  systemPromptMode?: SystemPromptMode;
}

/**
 * The named display colors an agent may declare (parity with Claude Code's
 * `AGENT_COLORS` / `AgentColorName`). Purely presentational metadata: it tints the
 * subagent's badge/label so concurrent agents are visually distinguishable. The
 * sentinel `'automatic'` (and any unrecognized value) maps to `undefined` so the
 * UI falls back to its default tinting — see {@link parseAgentColor}.
 */
export const AGENT_COLORS = [
  'red', 'blue', 'green', 'yellow', 'purple', 'orange', 'pink', 'cyan',
] as const;
export type AgentColorName = (typeof AGENT_COLORS)[number];

/**
 * Normalize a raw frontmatter `color` value to a known {@link AgentColorName}.
 * Case-insensitive; the `'automatic'` sentinel and any unrecognized value (or
 * non-string) return `undefined` (UI default). Mirrors Claude Code's
 * ColorPicker, where `'automatic'` is an explicit option that clears the color.
 */
export function parseAgentColor(value: unknown): AgentColorName | undefined {
  if (typeof value !== 'string') return undefined;
  const t = value.trim().toLowerCase();
  return (AGENT_COLORS as readonly string[]).includes(t) ? (t as AgentColorName) : undefined;
}

/**
 * The persistent-memory storage scopes an agent may declare (parity with Claude
 * Code's `AgentMemoryScope`): `user` (home), `project` (committed), or `local`
 * (project-local, non-shared).
 */
export const AGENT_MEMORY_SCOPES = ['user', 'project', 'local'] as const;
export type AgentMemoryScope = (typeof AGENT_MEMORY_SCOPES)[number];

/**
 * Validate + normalize a raw frontmatter `memory` value against the scope
 * allowlist. Case-insensitive; returns `undefined` for an unknown/non-string
 * value so an arbitrary scope can never silently propagate.
 */
export function parseMemoryScope(value: unknown): AgentMemoryScope | undefined {
  if (typeof value !== 'string') return undefined;
  const t = value.trim().toLowerCase();
  return (AGENT_MEMORY_SCOPES as readonly string[]).includes(t) ? (t as AgentMemoryScope) : undefined;
}

/**
 * The isolation modes an agent may declare (parity with Claude Code's agent
 * `isolation`). AMI supports `worktree` (run the subagent in an isolated git
 * worktree). Claude's ant-only `remote` mode has no AMI analogue and is rejected.
 */
export const AGENT_ISOLATION_MODES = ['worktree'] as const;
export type AgentIsolationMode = (typeof AGENT_ISOLATION_MODES)[number];

/**
 * Validate + normalize a raw frontmatter `isolation` value. Case-insensitive;
 * returns `undefined` for an unknown/non-string value.
 */
export function parseIsolationMode(value: unknown): AgentIsolationMode | undefined {
  if (typeof value !== 'string') return undefined;
  const t = value.trim().toLowerCase();
  return (AGENT_ISOLATION_MODES as readonly string[]).includes(t) ? (t as AgentIsolationMode) : undefined;
}

/**
 * Parse a raw frontmatter `background` value (parity with Claude Code's agent
 * `background` parsing). Accepts the string/boolean forms `'true'`/`true` →
 * `true` and `'false'`/`false`/omitted → `undefined`; ANY other value is
 * `invalid` (the caller surfaces a load warning) and yields `undefined` so a
 * garbled flag never silently forces background execution.
 */
export function parseBackgroundFlag(value: unknown): { value: boolean | undefined; invalid: boolean } {
  if (value === undefined) return { value: undefined, invalid: false };
  if (value === true || value === 'true') return { value: true, invalid: false };
  if (value === false || value === 'false') return { value: undefined, invalid: false };
  return { value: undefined, invalid: true };
}

/**
 * Parse a generic TRI-STATE boolean frontmatter flag (parity with Claude Code's
 * boolean agent/command knobs such as `wasGenerated`). Unlike
 * {@link parseBackgroundFlag} — whose `false` collapses to `undefined` because a
 * default-off background flag has no "explicitly off" state — this PRESERVES an
 * explicit `false` so a caller can distinguish "absent" (`undefined`) from
 * "declared false". Accepts the boolean/string forms `true`/`'true'` → `true` and
 * `false`/`'false'` → `false`; omitted → `undefined`; ANY other value is `invalid`
 * (the caller surfaces a load warning) and yields `undefined`.
 */
export function parseBooleanFlag(value: unknown): { value: boolean | undefined; invalid: boolean } {
  if (value === undefined) return { value: undefined, invalid: false };
  if (value === true || value === 'true') return { value: true, invalid: false };
  if (value === false || value === 'false') return { value: false, invalid: false };
  return { value: undefined, invalid: true };
}

/**
 * Normalize a raw frontmatter list value (a comma/whitespace-delimited string OR
 * a string array) into a clean, de-duplicated, length- and count-capped list of
 * names. Shared by {@link parseAgentSkills} and {@link parseAllowedAgentTypes} so
 * the two never drift apart. Unlike {@link parseArgumentNames} it does NOT drop
 * numeric-only entries (skill/agent-type names are free-form). Any non-string /
 * non-array value yields `[]`.
 */
function normalizeNameList(value: unknown): string[] {
  let raw: string[];
  if (typeof value === 'string') {
    raw = value.split(/[,\s]+/);
  } else if (Array.isArray(value)) {
    raw = value.map((v) => (typeof v === 'string' ? v : String(v)));
  } else {
    return [];
  }

  const seen = new Set<string>();
  const names: string[] = [];
  for (const entry of raw) {
    const name = entry.trim();
    if (name.length === 0) continue;
    if (name.length > MAX_SKILL_ARG_NAME_LENGTH) continue;
    if (seen.has(name)) continue;
    seen.add(name);
    names.push(name);
    if (names.length >= MAX_SKILL_ARG_NAMES) break;
  }
  return names;
}

/**
 * Parse a frontmatter `skills` field into the ordered list of skill names to
 * preload into a spawned agent's context (parity with Claude's agent `skills`).
 * Accepts a comma/whitespace string or a string array; see
 * {@link normalizeNameList} for the normalization rules.
 */
export function parseAgentSkills(value: unknown): string[] {
  return normalizeNameList(value);
}

/**
 * Parse a frontmatter `allowedAgentTypes` field into the ordered list of
 * sub-agent types this agent may itself spawn (parity with Claude's
 * `allowedAgentTypes` metadata, which splits a comma-delimited rule string).
 * Accepts a comma/whitespace string or a string array.
 */
export function parseAllowedAgentTypes(value: unknown): string[] {
  return normalizeNameList(value);
}

/**
 * True when `agent` is permitted to spawn a sub-agent of type `type`. An agent
 * with no `allowedAgentTypes` (or an empty list) is unrestricted and may spawn
 * any type; otherwise the type must appear in the allowlist (case-sensitive,
 * matching how agent types are registered by name).
 */
export function canSpawnAgentType(agent: Pick<AgentDefinition, 'allowedAgentTypes'>, type: string): boolean {
  const allowed = agent.allowedAgentTypes;
  if (!allowed || allowed.length === 0) return true;
  return allowed.includes(type);
}

/**
 * True when every MCP server the agent REQUIRES is available (parity with Claude
 * Code's `hasRequiredMcpServers`). With no requirements the agent is always
 * available. Each required pattern must match at least one available server name
 * as a case-insensitive substring.
 */
export function hasRequiredMcpServers(
  agent: Pick<AgentDefinition, 'requiredMcpServers'>,
  availableServers: readonly string[],
): boolean {
  if (!agent.requiredMcpServers || agent.requiredMcpServers.length === 0) {
    return true;
  }
  return agent.requiredMcpServers.every((pattern) =>
    availableServers.some((server) => server.toLowerCase().includes(pattern.toLowerCase())),
  );
}

/**
 * Filter a list of agents down to those whose required MCP servers are all
 * available (parity with Claude Code's `filterAgentsByMcpRequirements`). Agents
 * with unmet requirements are dropped from the spawnable/listable set.
 */
export function filterAgentsByMcpRequirements<T extends Pick<AgentDefinition, 'requiredMcpServers'>>(
  agents: readonly T[],
  availableServers: readonly string[],
): T[] {
  return agents.filter((agent) => hasRequiredMcpServers(agent, availableServers));
}

/**
 * The property names a skill/command definition may carry that are known to be
 * SAFE — i.e. they do not themselves grant a capability that should require user
 * consent (parity with Claude Code's `SAFE_SKILL_PROPERTIES`). Any property
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
  'aliases', 'isMcp', 'argumentHint', 'whenToUse', 'paths', 'version',
  'disableModelInvocation', 'userInvocable', 'loadedFrom', 'filePath',
  'immediate', 'userFacingName', 'requiresArgument', 'missingArgumentMessage',
  'isSensitive', 'disableNonInteractive', 'supportsNonInteractive',
]);

/**
 * True when a skill/command object carries ONLY {@link SAFE_SKILL_PROPERTIES}
 * (parity with Claude Code's `skillHasOnlySafeProperties`). A property outside
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

/**
 * Structural view of the SkillManager (which lives in common/). The core skill
 * and task tools reach their manager through `ToolContext._skillManager`, so core
 * depends only on this shape — never on the common/ implementation. This keeps
 * the dependency direction (common → core) intact.
 */
export interface SkillManagerLike {
  getSkill(name: string): SkillDefinition | null;
  getSkillContent(name: string, args?: Record<string, string>): string | null;
  listSkills(): SkillDefinition[];
  getAgent(name: string): AgentDefinition | null;
  listAgents(): AgentDefinition[];
  findMatchingSkills?(filePaths: string[]): SkillDefinition[];
}

// ---------------------------------------------------------------------------
// Skill argument safety — prevent memory exhaustion from oversized args
// ---------------------------------------------------------------------------

export const MAX_SKILL_ARG_NAMES = 64;
export const MAX_SKILL_ARG_NAME_LENGTH = 64;
export const MAX_SKILL_ARG_VALUE_LENGTH = 10_000;
export const MAX_SKILL_ARG_TOTAL_CHARS = 100_000;

export function sanitizeSkillArgs(args: Record<string, string>): Record<string, string> {
  const keys = Object.keys(args);
  const limitedKeys = keys.slice(0, MAX_SKILL_ARG_NAMES);
  const result: Record<string, string> = {};
  let totalChars = 0;

  for (const rawKey of limitedKeys) {
    const key = rawKey.length > MAX_SKILL_ARG_NAME_LENGTH
      ? rawKey.slice(0, MAX_SKILL_ARG_NAME_LENGTH)
      : rawKey;
    let value = typeof args[rawKey] === 'string' ? args[rawKey] : String(args[rawKey]);
    if (value.length > MAX_SKILL_ARG_VALUE_LENGTH) {
      value = value.slice(0, MAX_SKILL_ARG_VALUE_LENGTH);
    }
    if (totalChars + key.length + value.length > MAX_SKILL_ARG_TOTAL_CHARS) {
      break;
    }
    totalChars += key.length + value.length;
    result[key] = value;
  }

  return result;
}

export function validateSkillArgs(args: Record<string, string>): { valid: boolean; warnings: string[] } {
  const warnings: string[] = [];
  const keys = Object.keys(args);

  if (keys.length > MAX_SKILL_ARG_NAMES) {
    warnings.push(`Too many arguments: ${keys.length} (max ${MAX_SKILL_ARG_NAMES})`);
  }

  let totalChars = 0;
  for (const key of keys) {
    if (key.length > MAX_SKILL_ARG_NAME_LENGTH) {
      warnings.push(`Argument name too long: "${key.slice(0, 20)}..." (${key.length} chars, max ${MAX_SKILL_ARG_NAME_LENGTH})`);
    }
    const value = typeof args[key] === 'string' ? args[key] : String(args[key]);
    if (value.length > MAX_SKILL_ARG_VALUE_LENGTH) {
      warnings.push(`Argument value too long for "${key.slice(0, 20)}": ${value.length} chars (max ${MAX_SKILL_ARG_VALUE_LENGTH})`);
    }
    totalChars += key.length + value.length;
  }

  if (totalChars > MAX_SKILL_ARG_TOTAL_CHARS) {
    warnings.push(`Total argument size ${totalChars} exceeds limit ${MAX_SKILL_ARG_TOTAL_CHARS}`);
  }

  return { valid: true, warnings };
}

// ---------------------------------------------------------------------------
// Frontmatter value coercion & validation — hardening helpers that keep a
// malformed/injected SKILL.md or agent .md from crashing the loader or silently
// widening a tool allowlist. These are pure building blocks; the SkillManager
// (common/) wires them into parseSkillFile / parseAgentFile. They mirror the
// arg-safety helpers above: validate at the boundary, never trust YAML shape.
// ---------------------------------------------------------------------------

/**
 * Coerce a raw frontmatter `description` value into a safe, trimmed string.
 *
 * A YAML `description:` can parse to anything — a string, a number, a boolean,
 * or (for malformed/injected frontmatter) an array or object. Consumers that
 * call `.trim()` directly throw on a non-string value. This returns:
 *   - the trimmed string for a non-empty string,
 *   - `String(value)` for a finite number or a boolean,
 *   - `null` for everything else (empty/whitespace string, array, object,
 *     `null`/`undefined`, `NaN`/`Infinity`) so the caller can warn + skip.
 */
export function coerceDescriptionToString(value: unknown): string | null {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? String(value) : null;
  }
  if (typeof value === 'boolean') {
    return String(value);
  }
  return null;
}

/**
 * The named thinking-effort levels a skill/agent may request for its sub-engine,
 * in ascending order. A bare non-negative integer is also accepted (raw token
 * budget) — see {@link parseEffort}.
 */
export const EFFORT_LEVELS = ['low', 'medium', 'high', 'xhigh', 'max'] as const;
export type EffortLevel = (typeof EFFORT_LEVELS)[number];

/**
 * Validate + normalize a raw frontmatter `effort` value against the allowlist.
 * Returns the canonical lower-cased level name, or the decimal string of a
 * non-negative integer, or `undefined` when the value is unrecognized — so an
 * arbitrary/injected effort can never silently propagate into sub-engine config.
 */
export function parseEffort(value: unknown): string | undefined {
  if (typeof value === 'number') {
    return Number.isInteger(value) && value >= 0 ? String(value) : undefined;
  }
  if (typeof value === 'string') {
    const t = value.trim().toLowerCase();
    if ((EFFORT_LEVELS as readonly string[]).includes(t)) return t;
    if (/^\d+$/.test(t)) return t;
    return undefined;
  }
  return undefined;
}

/** True when `value` is an accepted effort level or non-negative integer. */
export function isValidEffort(value: unknown): boolean {
  return parseEffort(value) !== undefined;
}

/**
 * The ways a spawned agent's system prompt combines with the base subagent stance
 * (parity with Claude's swarm teammate `systemPromptMode`). `'append'` extends the
 * base stance; `'replace'`/`'default'` substitute it. Shared by the per-spawn
 * `system_prompt_mode` task-tool input AND {@link AgentDefinition.systemPromptMode}
 * so the two never drift apart.
 */
export const SYSTEM_PROMPT_MODES = ['default', 'replace', 'append'] as const;
export type SystemPromptMode = (typeof SYSTEM_PROMPT_MODES)[number];

/**
 * Validate + normalize a raw frontmatter `systemPromptMode`/`system-prompt-mode`
 * value against {@link SYSTEM_PROMPT_MODES}. Case-insensitive; returns `undefined`
 * for an unknown/non-string value so an arbitrary mode can never silently reshape
 * how a subagent's prompt is assembled.
 */
export function parseSystemPromptMode(value: unknown): SystemPromptMode | undefined {
  if (typeof value !== 'string') return undefined;
  const t = value.trim().toLowerCase();
  return (SYSTEM_PROMPT_MODES as readonly string[]).includes(t) ? (t as SystemPromptMode) : undefined;
}

/**
 * Validate a skill/agent *identifier* against the shared name rule (parity with
 * Claude Code's `validateAgentType`): a non-empty token that starts and ends with
 * an alphanumeric, contains only letters/numbers/hyphens, and is 3–50 characters.
 * Returns a human-readable error message for the first rule violated, or `null`
 * when the name is valid — so a creation flow can surface the problem inline. The
 * checks are ordered exactly like Claude's (required → charset → min → max).
 */
function validateIdentifierName(kind: string, value: string): string | null {
  if (!value) return `${kind} is required`;
  if (!/^[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]$/.test(value)) {
    return `${kind} must start and end with alphanumeric characters and contain only letters, numbers, and hyphens`;
  }
  if (value.length < 3) return `${kind} must be at least 3 characters long`;
  if (value.length > 50) return `${kind} must be less than 50 characters`;
  return null;
}

/**
 * Validate an agent *type* identifier (parity with Claude's `validateAgentType`).
 * Returns the human-readable error message for the first failed rule, or `null`
 * when valid. Consumed by {@link validateAgentDefinition} (as a load-time warning)
 * and by any agent-creation flow that wants to reject a bad name inline.
 */
export function validateAgentType(agentType: string): string | null {
  return validateIdentifierName('Agent type', agentType);
}

/**
 * Validate a skill *name* identifier (same rule as {@link validateAgentType}).
 * Returns the human-readable error message for the first failed rule, or `null`
 * when valid, for a `/skill create`-style flow to surface inline.
 */
export function validateSkillName(name: string): string | null {
  return validateIdentifierName('Skill name', name);
}

/**
 * Parse a frontmatter `arguments` field into an ordered list of positional
 * argument names (bound to bare `$name` placeholders by the content resolver).
 *
 * Accepts a single string (split on commas/whitespace) or a string array.
 * Empty, whitespace-only, and numeric-only names are dropped (numeric names
 * would collide with `$1`/`$2` positional tokens); names are de-duplicated,
 * length-capped at {@link MAX_SKILL_ARG_NAME_LENGTH}, and count-capped at
 * {@link MAX_SKILL_ARG_NAMES}. Any other value type yields `[]`.
 */
export function parseArgumentNames(value: unknown): string[] {
  let raw: string[];
  if (typeof value === 'string') {
    raw = value.split(/[,\s]+/);
  } else if (Array.isArray(value)) {
    raw = value.map((v) => (typeof v === 'string' ? v : String(v)));
  } else {
    return [];
  }

  const seen = new Set<string>();
  const names: string[] = [];
  for (const entry of raw) {
    const name = entry.trim();
    if (name.length === 0) continue;       // skip empty / whitespace-only
    if (/^\d+$/.test(name)) continue;      // skip numeric-only (collides with $1,$2…)
    if (name.length > MAX_SKILL_ARG_NAME_LENGTH) continue;
    if (seen.has(name)) continue;          // de-duplicate
    seen.add(name);
    names.push(name);
    if (names.length >= MAX_SKILL_ARG_NAMES) break;
  }
  return names;
}

/**
 * Parse a frontmatter `aliases` field into the alternate lookup names a skill may
 * be invoked by (parity with Claude's bundled skill `aliases`). Accepts a
 * comma/whitespace string or a string array; de-duplicated, length- and
 * count-capped via {@link normalizeNameList}. Any other value yields `[]`.
 */
export function parseSkillAliases(value: unknown): string[] {
  return normalizeNameList(value);
}

/**
 * True when a skill is currently available (parity with Claude's bundled skill
 * `isEnabled`). A skill with no predicate is always enabled; otherwise the
 * predicate decides. A predicate that THROWS is treated as DISABLED — a broken
 * availability gate hides the skill rather than crashing the loader/listing.
 */
export function isSkillEnabled(skill: Pick<SkillDefinition, 'isEnabled'>): boolean {
  if (!skill.isEnabled) return true;
  try {
    return skill.isEnabled() === true;
  } catch {
    return false;
  }
}

/**
 * Build the error message returned when a skill that declares
 * {@link SkillDefinition.requiresArgument} is invoked with no argument. Returns the
 * author's {@link SkillDefinition.missingArgumentMessage} verbatim when it is a
 * non-empty string; otherwise a generated usage line that includes the skill's
 * `argumentHint` when present (parity with Claude's required-argument skills, which
 * return a custom help/examples message, e.g. `/batch`).
 */
export function missingSkillArgumentMessage(
  skill: Pick<SkillDefinition, 'name' | 'argumentHint' | 'missingArgumentMessage'>,
): string {
  if (typeof skill.missingArgumentMessage === 'string' && skill.missingArgumentMessage.trim().length > 0) {
    return skill.missingArgumentMessage;
  }
  const hint = skill.argumentHint && skill.argumentHint.trim().length > 0
    ? ` (expected: ${skill.argumentHint.trim()})`
    : '';
  return `Skill "${skill.name}" requires an argument${hint}. Provide one and try again.`;
}

/** The permission-mode values an agent definition may declare. */
export const AGENT_PERMISSION_MODES = ['auto-allow', 'ask', 'deny-all'] as const;

/** Structured outcome of validating an agent definition. */
export interface AgentValidationResult {
  valid: boolean;      // no hard errors (the definition is loadable)
  errors: string[];    // reasons the definition must be rejected
  warnings: string[];  // non-fatal issues the author should see
}

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
    !(AGENT_PERMISSION_MODES as readonly string[]).includes(raw.permissionMode)
  ) {
    warnings.push(
      `Unknown permission mode "${raw.permissionMode}" (valid: ${AGENT_PERMISSION_MODES.join(', ')})`,
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

// ---------------------------------------------------------------------------
// Dynamic prompt / companion-file / progress-message resolution — building
// blocks that let a skill compute its body at invocation time, ship supporting
// resource files, and advertise an in-progress affordance. Pure core helpers;
// the SkillManager (common/) and the skill tool wire them into the content path.
// ---------------------------------------------------------------------------

/**
 * Resolve a skill's prompt/body for a given argument string (parity with Claude's
 * bundled-skill `getPromptForCommand(args)`). When the skill declares a
 * {@link SkillDefinition.getPromptForCommand} builder, it is invoked (awaited) with
 * the raw `args` string and its return value becomes the body — allowing per-call
 * validation, lazy content loading, or environment branching. A builder that
 * returns a non-string (malformed) yields `''`. With no builder, the static
 * {@link SkillDefinition.content} is returned unchanged.
 */
export async function resolveSkillPrompt(
  skill: Pick<SkillDefinition, 'content' | 'getPromptForCommand'>,
  args: string = '',
): Promise<string> {
  if (typeof skill.getPromptForCommand === 'function') {
    const out = await skill.getPromptForCommand(args);
    return typeof out === 'string' ? out : '';
  }
  return skill.content;
}

/**
 * Safely read a companion file bundled with a skill (parity with Claude's bundled
 * `SKILL_FILES` lookup). Returns the inlined content for `relPath` when it exists
 * in {@link SkillDefinition.files}, or `null` when there are no bundled files, the
 * path is not a string, the path is unsafe (absolute or containing `..`
 * traversal — rejected via {@link isSafeRelativePath}), or no such entry exists.
 * Never throws and never resolves outside the declared bundle.
 */
export function getSkillFile(
  skill: Pick<SkillDefinition, 'files'>,
  relPath: string,
): string | null {
  if (!skill.files || typeof relPath !== 'string') return null;
  const normalized = relPath.replace(/\\/g, '/').replace(/^\.\//, '');
  if (!isSafeRelativePath(normalized)) return null;
  const content = skill.files[normalized];
  return typeof content === 'string' ? content : null;
}

/** The affordance shown while a skill runs when it declares no custom message
 *  (parity with Claude's default command `progressMessage` of `'running'`). */
export const DEFAULT_SKILL_PROGRESS_MESSAGE = 'running';

/**
 * The progress/affordance message to show while a skill executes (parity with
 * Claude's command `progressMessage`). Returns the skill's own trimmed
 * {@link SkillDefinition.progressMessage} when it is a non-empty string, otherwise
 * {@link DEFAULT_SKILL_PROGRESS_MESSAGE}.
 */
export function skillProgressMessage(
  skill: Pick<SkillDefinition, 'progressMessage'>,
): string {
  if (typeof skill.progressMessage === 'string' && skill.progressMessage.trim().length > 0) {
    return skill.progressMessage.trim();
  }
  return DEFAULT_SKILL_PROGRESS_MESSAGE;
}

// ---------------------------------------------------------------------------
// Skill-metadata flags — declarative command/skill knobs (parity with Claude's
// command `isSensitive` / `isHidden` / `userFacingName()` /
// `disableNonInteractive` / `supportsNonInteractive`). Pure predicates; the skill
// tool (core) and the `/skills` listing (common) wire them into the real echo /
// catalog / non-interactive-gate paths.
// ---------------------------------------------------------------------------

/** Masked placeholder shown in place of a sensitive skill's arguments. */
export const REDACTED_SKILL_ARGS = '***';

/**
 * Display-safe form of a skill's raw argument string. When the skill declares
 * {@link SkillDefinition.isSensitive} and `rawArgs` is a non-empty string, returns
 * {@link REDACTED_SKILL_ARGS} so the arguments are never echoed into a permission
 * prompt, transcript, or history (parity with Claude's `isSensitive` arg masking).
 * A non-sensitive skill — or an empty/whitespace argument — returns `rawArgs`
 * unchanged.
 */
export function redactSkillArgs(
  skill: Pick<SkillDefinition, 'isSensitive'>,
  rawArgs: string,
): string {
  if (skill.isSensitive && typeof rawArgs === 'string' && rawArgs.trim().length > 0) {
    return REDACTED_SKILL_ARGS;
  }
  return rawArgs;
}

/**
 * True when a skill is HIDDEN from user-facing help/typeahead listings (parity
 * with Claude's command `isHidden`). A hidden skill stays invocable by its exact
 * name; it is only omitted from catalog listings such as `/skills`.
 */
export function isSkillHidden(skill: Pick<SkillDefinition, 'isHidden'>): boolean {
  return skill.isHidden === true;
}

/**
 * The name to DISPLAY for a skill in listings (parity with Claude's command
 * `userFacingName()`): the trimmed {@link SkillDefinition.userFacingName} when it
 * is a non-empty string, otherwise the canonical {@link SkillDefinition.name}.
 */
export function skillUserFacingName(
  skill: Pick<SkillDefinition, 'name' | 'userFacingName'>,
): string {
  if (typeof skill.userFacingName === 'string' && skill.userFacingName.trim().length > 0) {
    return skill.userFacingName.trim();
  }
  return skill.name;
}

/**
 * Whether a skill may be invoked in the CURRENT interactivity mode (parity with
 * Claude's command `disableNonInteractive` / `supportsNonInteractive` gating). In
 * an interactive run every skill is available. In a non-interactive (detached /
 * `--prompt`) run a skill is unavailable when it declares
 * `supportsNonInteractive === false`, OR it declares `disableNonInteractive` and
 * does not explicitly opt back in via `supportsNonInteractive === true`.
 */
export function isSkillAvailableNonInteractive(
  skill: Pick<SkillDefinition, 'disableNonInteractive' | 'supportsNonInteractive'>,
  nonInteractive: boolean,
): boolean {
  if (!nonInteractive) return true;
  if (skill.supportsNonInteractive === true) return true;
  if (skill.supportsNonInteractive === false) return false;
  return skill.disableNonInteractive !== true;
}

/**
 * Estimate the context-budget cost (in tokens) of a skill's FRONTMATTER only —
 * name + description + whenToUse (parity with Claude's
 * `estimateSkillFrontmatterTokens`). The full body is loaded only on invocation,
 * so the listing cost is just the always-present metadata. Reuses the canonical
 * {@link estimateTokens} heuristic so there is a single token estimator.
 */
export function estimateSkillFrontmatterTokens(
  skill: Pick<SkillDefinition, 'name' | 'description' | 'whenToUse'>,
): number {
  const text = [skill.name, skill.description, skill.whenToUse]
    .filter((v): v is string => typeof v === 'string' && v.length > 0)
    .join(' ');
  return estimateTokens(text);
}

/**
 * Resolve an agent's system prompt for a given runtime context (parity with
 * Claude's agent `getSystemPrompt({ toolUseContext })`). When the agent declares a
 * {@link AgentDefinition.getSystemPrompt} builder it is invoked (awaited) with
 * `ctx` and its return value becomes the prompt; a non-string return yields `''`.
 * With no builder, the static {@link AgentDefinition.systemPrompt} is returned.
 */
export async function resolveAgentSystemPrompt(
  agent: Pick<AgentDefinition, 'systemPrompt' | 'getSystemPrompt'>,
  ctx?: Record<string, unknown>,
): Promise<string> {
  if (typeof agent.getSystemPrompt === 'function') {
    const out = await agent.getSystemPrompt(ctx);
    return typeof out === 'string' ? out : '';
  }
  return agent.systemPrompt;
}

// ---------------------------------------------------------------------------
// Agent skill preloading — assemble the system-prompt block for the skills an
// agent definition declares via its `skills` allowlist (parity with Claude's
// runAgent skill preloading: inline each named skill's body into the spawned
// agent's context AND report which skills loaded vs. which names could not be
// resolved). Pure core helper; the task tool (core) wires it into the spawn path.
// ---------------------------------------------------------------------------

/** Heading introducing the inlined bodies of an agent's preloaded skills. */
export const PRELOADED_SKILLS_HEADING = '# Preloaded Skills';

/** Outcome of {@link buildPreloadedSkillsBlock}. */
export interface PreloadedSkillsResult {
  /** The assembled system-prompt block to append to the agent's prompt, or `''`
   *  when no requested skill resolved to enabled, non-empty content. */
  block: string;
  /** Canonical names of the skills that were preloaded, in request order (the
   *  "which skill is loading" metadata). */
  loaded: string[];
  /** Requested skill names that did NOT resolve to an enabled, non-empty skill
   *  (missing, disabled via {@link isSkillEnabled}, or blank-bodied) — surfaced as
   *  a load/spawn warning instead of silently dropped. */
  missing: string[];
}

/**
 * Build the preloaded-skills prompt block for an agent's `skills` allowlist
 * (parity with Claude's runAgent skill preloading). For each requested name,
 * `resolve` is consulted (the SkillManager's name/alias lookup); a skill that
 * resolves, is {@link isSkillEnabled}, and has a non-empty body is inlined under a
 * `## <name>` sub-heading, and its canonical name recorded in `loaded`. Any name
 * that does not resolve to such a skill is recorded in `missing`. The returned
 * `block` is empty when nothing loaded so the caller appends nothing. Duplicate
 * requested names resolve once each (the caller's allowlist is already de-duped by
 * {@link parseAgentSkills}); a skill is never inlined twice for the same request.
 */
export function buildPreloadedSkillsBlock(
  skillNames: readonly string[],
  resolve: (name: string) => SkillDefinition | null,
): PreloadedSkillsResult {
  const loaded: string[] = [];
  const missing: string[] = [];
  const parts: string[] = [];
  const seen = new Set<string>();
  for (const name of skillNames) {
    const skill = resolve(name);
    const body = typeof skill?.content === 'string' ? skill.content.trim() : '';
    if (skill && isSkillEnabled(skill) && body.length > 0) {
      if (seen.has(skill.name)) continue; // never inline the same skill twice
      seen.add(skill.name);
      loaded.push(skill.name);
      parts.push(`## ${skill.name}\n\n${body}`);
    } else {
      missing.push(name);
    }
  }
  const block = parts.length > 0 ? `${PRELOADED_SKILLS_HEADING}\n\n${parts.join('\n\n')}` : '';
  return { block, loaded, missing };
}

// ---------------------------------------------------------------------------
// Skill-improvement suggestions — a structured descriptor for applying concrete,
// section-scoped edits to a named skill (parity with Claude's
// SkillImprovementSuggestion / SkillUpdate schema). Pure data + validation; the
// apply/survey flow (model call + file write) lives in the upper layers.
// ---------------------------------------------------------------------------

/**
 * One concrete, section-scoped edit to apply to a skill definition (parity with
 * Claude Code's `SkillUpdate`). `section` names the part of the skill to change,
 * `change` is the edit to integrate, and `reason` is the (optional) rationale.
 */
export interface SkillUpdate {
  section: string;
  change: string;
  reason: string;
}

/**
 * A structured suggestion to improve a named skill (parity with Claude Code's
 * `SkillImprovementSuggestion`): the target skill plus the list of edits to apply.
 */
export interface SkillImprovementSuggestion {
  skillName: string;
  updates: SkillUpdate[];
}

/**
 * Parse/normalize a raw (possibly model-produced or injected) value into a clean
 * list of {@link SkillUpdate}s. Accepts only an array of objects; each entry must
 * carry a non-empty `section` AND `change` (both trimmed) to be kept — entries
 * missing either are dropped so a malformed suggestion can never produce a
 * no-op/garbage edit. `reason` is optional and defaults to `''`.
 */
export function parseSkillUpdates(value: unknown): SkillUpdate[] {
  if (!Array.isArray(value)) return [];
  const out: SkillUpdate[] = [];
  for (const entry of value) {
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) continue;
    const e = entry as Record<string, unknown>;
    const section = typeof e.section === 'string' ? e.section.trim() : '';
    const change = typeof e.change === 'string' ? e.change.trim() : '';
    if (section.length === 0 || change.length === 0) continue;
    const reason = typeof e.reason === 'string' ? e.reason.trim() : '';
    out.push({ section, change, reason });
  }
  return out;
}

/**
 * True when `value` is a well-formed {@link SkillImprovementSuggestion}: a
 * non-empty `skillName` and at least one valid {@link SkillUpdate} (as decided by
 * {@link parseSkillUpdates}). Used to gate whether an improvement survey/apply
 * flow should run at all.
 */
export function isValidSkillImprovement(value: unknown): value is SkillImprovementSuggestion {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const v = value as Record<string, unknown>;
  if (typeof v.skillName !== 'string' || v.skillName.trim().length === 0) return false;
  return parseSkillUpdates(v.updates).length > 0;
}

/**
 * Render a list of {@link SkillUpdate}s into the `- <section>: <change>` bullet
 * list Claude feeds the editor model when applying an improvement
 * (`applySkillImprovement`). Entries are normalized via {@link parseSkillUpdates}
 * first so invalid updates are excluded.
 */
export function formatSkillUpdates(updates: SkillUpdate[]): string {
  return parseSkillUpdates(updates)
    .map((u) => `- ${u.section}: ${u.change}`)
    .join('\n');
}
