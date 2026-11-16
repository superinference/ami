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
  context?: 'inline' | 'fork'; // Execution mode: inline injects as system message, fork spawns subagent
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
  tools?: string[];       // Allowed tool names (empty = all)
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
  'agent', 'getPromptForCommand', 'frontmatterKeys',
  // Base command/skill properties
  'name', 'description', 'hasUserSpecifiedDescription', 'isEnabled', 'isHidden',
  'aliases', 'isMcp', 'argumentHint', 'whenToUse', 'paths', 'version',
  'disableModelInvocation', 'userInvocable', 'loadedFrom', 'filePath',
  'immediate', 'userFacingName',
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
