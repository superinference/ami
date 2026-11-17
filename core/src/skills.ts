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

import type { HookConfigEntry } from './hooks';
import type { McpServerConfig } from './mcp/manager';
import type { ThinkingLevel } from './model-capabilities';
import { isSafeRelativePath } from './skill-catalog';
import { estimateTokens } from './utils/tokens';

/** The classification tokens a command/skill may declare (parity with Claude's
 *  command `kind`). `'workflow'` badges a workflow-backed command in help/typeahead;
 *  a plain skill declares none. */
export const SKILL_KINDS = ['workflow'] as const;
export type SkillKind = (typeof SKILL_KINDS)[number];

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
  /** When true, invoking this skill/command JUMPS the message queue and executes
   *  at once instead of waiting for the next stop point (parity with Claude's
   *  command `immediate`). Checked via {@link isSkillImmediate}. */
  immediate?: boolean;
  /** The shell a skill/command's inline `!`-blocks run under (parity with Claude's
   *  `shell:` frontmatter). Absent ⇒ the executor defaults to bash — resolve with
   *  the fallback via {@link shellForExecution}. Parsed/validated via
   *  {@link parseShellFrontmatter}. */
  shell?: FrontmatterShell;
  /** Author-defined named template variables substituted into the skill body:
   *  every `{{key}}` token in {@link content} is replaced with `templateVars[key]`
   *  (parity with Claude's bundled-skill `SKILL_MODEL_VARS`) so centrally-maintained
   *  wording updates in one place. An unknown token is left intact. Applied via
   *  {@link applySkillTemplateVariables}. */
  templateVars?: Record<string, string>;
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
  /** Base directory exported to the skill's `!`-blocks / hooks as
   *  `CLAUDE_PLUGIN_ROOT` (and the `AMI_PLUGIN_ROOT` alias) — parity with Claude's
   *  PromptCommand `skillRoot`. Lets a plugin/disk skill reference its bundle files
   *  by an absolute `${CLAUDE_PLUGIN_ROOT}/...` path regardless of the session cwd.
   *  Resolved — with a fallback to the skill's own on-disk directory — via
   *  {@link skillPluginRoot}; the substitution itself is done by
   *  `substitutePluginRoot` (hooks.ts). Already a {@link SAFE_SKILL_PROPERTIES}
   *  member (declarative metadata, not injectable auto-run code). */
  skillRoot?: string;
  /** Discriminator badging a workflow-backed command/skill in help/typeahead
   *  (parity with Claude's command `kind: 'workflow'`). Purely a classification
   *  surfaced by the listing; absent ⇒ a plain skill. Read via {@link skillKind}. */
  kind?: SkillKind;
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
  /** Invocation-scoped lifecycle hooks declared in the skill's frontmatter (parity
   *  with Claude Code's PromptCommand `hooks` + registerFrontmatterHooks). When a
   *  FORKED skill carries these, they are registered on its sub-engine for the
   *  duration of the invocation and torn down when the fork finishes — letting an
   *  individual skill contribute e.g. a preToolUse guard scoped to its own run
   *  instead of the global/plugin-only hooks AMI previously supported. Parsed from
   *  SKILL.md frontmatter via {@link parseFrontmatterHooks} (hooks.ts) and stripped
   *  for untrusted (installed/plugin) tiers by the SkillManager, mirroring how a
   *  self-granted `auto-allow` permissionMode is refused. */
  hooks?: HookConfigEntry[];
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
  /** Display color for the agent's DESCRIPTION badge, distinct from {@link color}
   *  (which tints the agent-type badge) — parity with Claude's AgentProgressLine
   *  `descriptionColor`. Normalized/validated via {@link parseAgentColor}. */
  descriptionColor?: AgentColorName;
  /** Drop the project CLAUDE.md/AGENTS.md context from this subagent's prompt to
   *  save tokens (parity with Claude's agent `omitClaudeMd`, e.g. the read-only
   *  Plan agent which can still Read it on demand). An invalid (non-boolean) value
   *  surfaces a load warning via {@link validateAgentDefinition}. */
  omitClaudeMd?: boolean;
  /** Require plan-mode approval before this delegated agent may implement/mutate
   *  (parity with Claude's swarm teammate `planModeRequired`). An invalid
   *  (non-boolean) value surfaces a load warning via {@link validateAgentDefinition}. */
  planModeRequired?: boolean;
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
  /** Agent-lifecycle hooks declared in the agent's frontmatter (parity with Claude
   *  Code's agent `hooks` field + registerFrontmatterHooks with `isAgent=true`).
   *  When a NAMED subagent carries these, they are registered on its spawned
   *  sub-engine for the duration of that agent's run and torn down when it finishes
   *  — so an individual agent definition can declare its own scoped hooks instead of
   *  relying on the global/plugin-only hooks surface. Parsed from frontmatter via
   *  {@link parseFrontmatterHooks} (hooks.ts); REFUSED for plugin-sourced agents
   *  (a privilege-escalation vector) alongside permission-mode / mcp-servers. */
  hooks?: HookConfigEntry[];
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
  /** Short, strongly-flagged hard constraint reinforced separately from
   *  {@link systemPrompt} (parity with Claude's `criticalSystemReminder_EXPERIMENTAL`).
   *  A must-hold rule — e.g. "no project writes", "you MUST end with VERDICT:" — is
   *  appended to the agent's authoritative system prompt as a conspicuous reminder so
   *  it does not drift into the surrounding prose. Parsed from a
   *  `critical-system-reminder`/`criticalSystemReminder` frontmatter key; honored in
   *  the task tool's spawn path via {@link buildCriticalReminderBlock}. */
  criticalSystemReminder?: string;
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

/**
 * Boundary-validate a skill's invocation arguments against the hard caps
 * ({@link MAX_SKILL_ARG_NAMES} / {@link MAX_SKILL_ARG_NAME_LENGTH} /
 * {@link MAX_SKILL_ARG_VALUE_LENGTH} / {@link MAX_SKILL_ARG_TOTAL_CHARS}) and
 * return an ACTIONABLE verdict (parity with Claude Code's skill/agent input
 * validators, which return a concrete negative result + a warning on invalid
 * input so a caller can degrade gracefully). `valid` is `false` whenever ANY hard
 * cap is exceeded — previously it was hard-coded `true`, so a caller trusting
 * `.valid` to gate oversized/abusive input could never reject. Each breach also
 * records a human-readable `warnings` entry. Unlike {@link sanitizeSkillArgs}
 * (which silently truncates), this is the report-and-let-the-caller-log path.
 */
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

  return { valid: warnings.length === 0, warnings };
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
  skill: Pick<SkillDefinition, 'content' | 'getPromptForCommand' | 'templateVars'>,
  args: string = '',
): Promise<string> {
  if (typeof skill.getPromptForCommand === 'function') {
    const out = await skill.getPromptForCommand(args);
    return applySkillTemplateVariables(typeof out === 'string' ? out : '', skill.templateVars);
  }
  return applySkillTemplateVariables(skill.content, skill.templateVars);
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
 * Resolve the base directory a skill exports to its `!`-blocks / hooks as
 * `CLAUDE_PLUGIN_ROOT` (parity with Claude's PromptCommand `skillRoot`). Returns
 * the skill's explicit {@link SkillDefinition.skillRoot} when it is a non-empty
 * string; otherwise falls back to the directory of its real on-disk
 * {@link SkillDefinition.filePath} (a `<builtin>`/in-memory sentinel or a bare
 * filename yields no directory). Returns `undefined` when neither is available, so
 * a caller can skip the `${CLAUDE_PLUGIN_ROOT}` substitution. Never throws.
 */
export function skillPluginRoot(
  skill: Pick<SkillDefinition, 'skillRoot' | 'filePath'>,
): string | undefined {
  if (typeof skill.skillRoot === 'string' && skill.skillRoot.trim().length > 0) {
    return skill.skillRoot.trim();
  }
  const fp = skill.filePath;
  if (typeof fp !== 'string' || fp.startsWith('<') || !fp.includes('/')) return undefined;
  const slash = fp.lastIndexOf('/');
  const dir = fp.slice(0, slash);
  return dir.length > 0 ? dir : undefined;
}

/**
 * The classification of a command/skill for help/typeahead badging (parity with
 * Claude's command `kind`). Returns the declared {@link SkillDefinition.kind}
 * (currently only `'workflow'`) or `undefined` for a plain skill.
 */
export function skillKind(skill: Pick<SkillDefinition, 'kind'>): SkillKind | undefined {
  return skill.kind;
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

// ---------------------------------------------------------------------------
// Additive skill/agent config knobs + display formatters — pure core building
// blocks mirroring Claude Code capabilities AMI's skills library was missing:
// immediate-queue flag, per-file shell selection, a human tool-name joiner, a
// progressive argument hint, an admin-trusted source gate, author template
// variables, and the built-in-agent discriminator. The SkillManager (common/),
// task tool, and `/skills`/`/agents` listings wire these into the real paths.
// ---------------------------------------------------------------------------

/**
 * True when a skill/command declares {@link SkillDefinition.immediate} (parity
 * with Claude's command `immediate`): it should bypass the message queue and run
 * at once rather than waiting for the next stop point. Only an explicit `true`
 * counts — a truthy-but-not-`true` value never silently jumps the queue.
 */
export function isSkillImmediate(skill: Pick<SkillDefinition, 'immediate'>): boolean {
  return skill.immediate === true;
}

/**
 * The shells accepted in a skill/command's `shell:` frontmatter for `!`-block
 * execution (parity with Claude's `FrontmatterShell`).
 */
export const FRONTMATTER_SHELLS = ['bash', 'powershell'] as const;
export type FrontmatterShell = (typeof FRONTMATTER_SHELLS)[number];

/** The shell used when a skill/command declares none (parity with Claude, which
 *  defaults `!`-block execution to bash). */
export const DEFAULT_SHELL: FrontmatterShell = 'bash';

/**
 * Parse + validate a raw frontmatter `shell:` value (parity with Claude's
 * `parseShellFrontmatter`). Absent/null/empty ⇒ `undefined` (the caller defaults
 * to bash via {@link shellForExecution}); a recognized value (case-insensitive,
 * trimmed) ⇒ that shell; an unrecognized value ⇒ `undefined` so a garbled shell
 * silently falls back to bash rather than failing the skill load.
 */
export function parseShellFrontmatter(value: unknown): FrontmatterShell | undefined {
  if (value === undefined || value === null) return undefined;
  const normalized = String(value).trim().toLowerCase();
  if (normalized === '') return undefined;
  return (FRONTMATTER_SHELLS as readonly string[]).includes(normalized)
    ? (normalized as FrontmatterShell)
    : undefined;
}

/**
 * Resolve the shell a skill/command's `!`-blocks execute under, applying the
 * {@link DEFAULT_SHELL} fallback when none was declared (parity with Claude's
 * caller-side bash default).
 */
export function shellForExecution(shell: FrontmatterShell | undefined): FrontmatterShell {
  return shell ?? DEFAULT_SHELL;
}

/**
 * Format a tool-name list for human display (parity with Claude Code's
 * `getToolsDisplay`, ConfirmStep): `undefined` ⇒ `'All tools'` (unset means no
 * restriction), `[]` ⇒ `'None'`, one name ⇒ that name (or `'None'` when blank), a
 * pair ⇒ `'a and b'`, and three+ ⇒ an Oxford-comma list `'a, b, and c'`.
 */
export function formatToolNameList(toolNames: readonly string[] | undefined): string {
  if (toolNames === undefined) return 'All tools';
  if (toolNames.length === 0) return 'None';
  if (toolNames.length === 1) return toolNames[0] || 'None';
  if (toolNames.length === 2) return toolNames.join(' and ');
  return `${toolNames.slice(0, -1).join(', ')}, and ${toolNames[toolNames.length - 1]}`;
}

/**
 * Compute a progressive argument hint showing only the arg names NOT yet filled
 * (parity with Claude's `generateProgressiveArgumentHint`): for `argNames` and
 * the args typed so far, returns e.g. `'[arg2] [arg3]'`, or `undefined` once
 * every positional arg is filled (or over-filled) so the hint disappears.
 */
export function progressiveArgumentHint(
  argNames: readonly string[],
  typedArgs: readonly string[],
): string | undefined {
  const remaining = argNames.slice(typedArgs.length);
  if (remaining.length === 0) return undefined;
  return remaining.map((name) => `[${name}]`).join(' ');
}

/**
 * Substitute author-defined `{{key}}` template variables into a skill body
 * (parity with Claude's bundled-skill `SKILL_MODEL_VARS` substitution). Every
 * `{{key}}` whose `key` is present in `vars` is replaced with its value (an
 * empty-string mapping is honored); an unknown token is left intact. With no
 * `vars` the content is returned unchanged. This is DISTINCT from the invocation
 * `$name`/`$ARGUMENTS` positional substitution (owned by the SkillManager): it is
 * a skill-owned, centrally-maintained wording map, not user-supplied args.
 */
export function applySkillTemplateVariables(
  content: string,
  vars: Record<string, string> | undefined,
): string {
  if (!vars || typeof content !== 'string') return content;
  return content.replace(/\{\{(\w+)\}\}/g, (match, key: string) =>
    Object.prototype.hasOwnProperty.call(vars, key) ? vars[key] : match,
  );
}

/**
 * True when an agent definition is a BUILT-IN agent (parity with Claude Code's
 * `isBuiltIn = source === 'built-in'`). The discriminator that gates built-in
 * vs. custom treatment — e.g. editability of the definition and the extra
 * custom-agent tool-denylist that applies only to non-built-in agents.
 */
export function isBuiltInAgent(agent: Pick<AgentDefinition, 'source'>): boolean {
  return agent.source === 'built-in';
}

// ---------------------------------------------------------------------------
// Agent-listing display affordances — surface a sub-agent's persistent-memory
// scope and its scoped skill allowlist in the `/agents` listing (parity with
// Claude Code's `formatAgent` memory dimension + AgentDetail `getMemoryScopeDisplay`
// / skills-list collapse). Pure core formatters; the `/agents` slash-command data
// already carries `memory` + `skills`, and the CLI listing renderer wires these in.
// ---------------------------------------------------------------------------

/**
 * Human-readable label for an agent's persistent-memory {@link AgentMemoryScope}
 * (parity with Claude Code's `getMemoryScopeDisplay`): `user` / `project` / `local`
 * each map to a descriptive, location-aware phrase. Returns `undefined` for an
 * unset/unknown scope so a memory-less agent surfaces no memory affordance.
 */
export function agentMemoryScopeDisplay(scope: AgentMemoryScope | undefined): string | undefined {
  switch (scope) {
    case 'user': return 'User (shared across all projects)';
    case 'project': return 'Project (shared with your team)';
    case 'local': return 'Local (this project only, not shared)';
    default: return undefined;
  }
}

/**
 * The short memory dimension appended to an agent's listing line (parity with
 * Claude Code's `formatAgent`, which appends `${agent.memory} memory` only when the
 * agent declares a memory scope). Returns e.g. `'project memory'`, or `''` when the
 * agent declares no (or an unrecognized) memory scope so no dangling affordance is
 * shown.
 */
export function formatAgentMemoryDimension(agent: Pick<AgentDefinition, 'memory'>): string {
  const scope = parseMemoryScope(agent.memory);
  return scope ? `${scope} memory` : '';
}

/**
 * Collapse an agent's scoped-skills allowlist for display (parity with Claude
 * Code's AgentDetail skills list: `skills.length > 10 ? '${n} skills' :
 * skills.join(', ')`). An agent that declares many skills shows a `'N skills'`
 * count instead of an overwhelming comma list; `undefined`/empty yields `''`.
 */
export function formatAgentSkillsList(skills: readonly string[] | undefined): string {
  if (!skills || skills.length === 0) return '';
  if (skills.length > 10) return `${skills.length} skills`;
  return skills.join(', ');
}

// ---------------------------------------------------------------------------
// Relevance-filtered bundled-doc inlining — select only the companion docs that
// match a detected language (plus shared docs) and splice them into a skill's
// context as deterministically-ordered `<doc path="…">…</doc>` blocks (parity
// with Claude Code's bundled-skill `getFilesForLanguage` + `buildInlineReference`,
// e.g. the claude-api skill). Pure core building blocks over {@link
// SkillDefinition.files}; avoids injecting every bundled doc verbatim.
// ---------------------------------------------------------------------------

/**
 * Select the subset of a skill's bundled {@link SkillDefinition.files} keys that
 * are relevant to a detected language (parity with Claude Code's
 * `getFilesForLanguage`): a file is relevant when its key is under `${lang}/` or
 * the always-shared `shared/` prefix. Returns `[]` for no files or a blank
 * language. Order follows the files map's own key order (callers sort when
 * inlining via {@link buildInlineReference}).
 */
export function getFilesForLanguage(
  files: Record<string, string> | undefined,
  lang: string,
): string[] {
  if (!files || typeof lang !== 'string' || lang.trim().length === 0) return [];
  const prefix = `${lang.trim()}/`;
  return Object.keys(files).filter((p) => p.startsWith(prefix) || p.startsWith('shared/'));
}

/** Strip HTML comments from markdown, nesting-safe (parity with Claude Code's
 *  `processContent` comment pass). Looped so a `<!-- a <!-- b --> c -->` resolves. */
function stripHtmlComments(md: string): string {
  let out = md;
  let prev: string;
  do {
    prev = out;
    out = out.replace(/<!--[\s\S]*?-->\n?/g, '');
  } while (out !== prev);
  return out;
}

/**
 * Assemble the inline-reference block for a set of a skill's bundled-doc keys
 * (parity with Claude Code's `buildInlineReference`). Keys are sorted
 * deterministically; each existing file's content is comment-stripped, has its
 * `{{key}}` author template variables applied (via
 * {@link applySkillTemplateVariables}), trimmed, and wrapped in a
 * `<doc path="…">…</doc>` tag. A requested key with no matching file is skipped
 * (never emits an empty tag). Returns `''` when nothing resolved.
 */
export function buildInlineReference(
  files: Record<string, string> | undefined,
  filePaths: readonly string[],
  templateVars?: Record<string, string>,
): string {
  if (!files) return '';
  const sections: string[] = [];
  for (const filePath of [...filePaths].sort()) {
    const md = files[filePath];
    if (typeof md !== 'string') continue; // skip missing keys
    const processed = applySkillTemplateVariables(stripHtmlComments(md), templateVars).trim();
    sections.push(`<doc path="${filePath}">\n${processed}\n</doc>`);
  }
  return sections.join('\n\n');
}

// ---------------------------------------------------------------------------
// Required-MCP-server readiness — the pure decision a subagent-spawn precondition
// loop needs (parity with Claude Code's AgentTool requiredMcpServers gate, which
// polls pending servers, early-exits on a failed required server, and errors when
// a required server is missing). The polling/timeout loop lives in the task tool;
// this classifies the CURRENT server-state snapshot so the loop can decide to
// proceed, keep waiting, or abort.
// ---------------------------------------------------------------------------

/** One required-MCP-server readiness classification over a state snapshot. */
export interface McpServerReadiness {
  /** True when every required server is connected (safe to spawn). */
  ready: boolean;
  /** Required patterns whose matching server(s) are still connecting — the loop
   *  should keep polling while these remain. */
  pending: string[];
  /** Required patterns whose every matching server has failed — the loop should
   *  early-exit (no point waiting). */
  failed: string[];
  /** Required patterns with no matching server at all — an actionable "missing
   *  required MCP server" error. */
  missing: string[];
}

/**
 * Classify a required-MCP-server precondition against a snapshot of live server
 * states (parity with Claude Code's AgentTool requiredMcpServers gate). Each
 * required entry is matched case-insensitively as a substring against the server
 * names (consistent with {@link hasRequiredMcpServers}); a requirement is
 * satisfied when ANY matching server is `'connected'`, `failed` when EVERY match
 * has `'failed'`, `missing` when no server matches, otherwise `pending`. With no
 * requirements the result is immediately {@link McpServerReadiness.ready}.
 */
export function requiredMcpServerReadiness(
  required: readonly string[] | undefined,
  serverStates: Readonly<Record<string, string>>,
): McpServerReadiness {
  const pending: string[] = [];
  const failed: string[] = [];
  const missing: string[] = [];
  if (!required || required.length === 0) {
    return { ready: true, pending, failed, missing };
  }
  const entries = Object.entries(serverStates);
  for (const pattern of required) {
    const pat = pattern.toLowerCase();
    const matches = entries.filter(([name]) => name.toLowerCase().includes(pat));
    if (matches.length === 0) { missing.push(pattern); continue; }
    if (matches.some(([, state]) => state === 'connected')) continue; // satisfied
    if (matches.every(([, state]) => state === 'failed')) { failed.push(pattern); continue; }
    pending.push(pattern); // at least one still connecting
  }
  const ready = pending.length === 0 && failed.length === 0 && missing.length === 0;
  return { ready, pending, failed, missing };
}

// ---------------------------------------------------------------------------
// Named positional-argument substitution — bind a skill/command's declared
// `arguments` names to their positional token values in the body (parity with
// Claude Code's `substituteArguments`, which replaces BARE `$name` as well as
// braced `${name}`). AMI previously bound only `${name}`; a bare `$foo` was left
// literal. Single canonical substitution shared by the SkillManager content path.
// ---------------------------------------------------------------------------

/** Escape a string for literal use inside a RegExp (so an arg name carrying a
 *  regex metacharacter matches literally instead of throwing / mis-matching). */
function escapeForRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// ---------------------------------------------------------------------------
// Path/argument-scoped tool allowlist → permission ALLOW rules — the
// least-privilege half AMI's name-only skill/agent tool filter was missing
// (parity with Claude Code's path-scoped allowlists, e.g. a /statusline-spawned
// subagent bound to `allowedTools: ['Read(~/**)', 'Edit(~/.claude/settings.json)']`
// so it can only read the home tree and edit that single file). `filterToolsForSkill`
// (core/core/src/tools/skill.ts) already enforces WHICH tools a forked skill gets by
// NAME; this derives the WHICH-ARGUMENTS half as additive permission rules the forked
// sub-engine pre-grants. Pure core building block; the skill tool wires it into the
// spawned sub-engine's `permissionRules`.
// ---------------------------------------------------------------------------

/**
 * Parse a skill/command `allowedTools` list into the ARGUMENT-scoped permission
 * ALLOW rules it implies (parity with Claude Code's path-scoped tool allowlists).
 *
 * For every entry of the form `Tool(scope)` whose scope is a REAL restriction, an
 * `{ tool, pattern, action: 'allow' }` rule is produced so the forked subagent can
 * pre-grant exactly that scoped operation (e.g. `Edit(~/.claude/settings.json)` →
 * allow the file-edit tool only for that path). Entries that are NOT an argument
 * scope yield no rule — the name-only filter alone governs them:
 * - a bare name (`Edit`, no parens),
 * - a category token (`read-only`, `edit`, … — these never carry parens),
 * - an empty or bare-`*` scope (`Edit()`, `Read(*)`) — unrestricted,
 * - a non-string or malformed (unterminated-paren) entry.
 *
 * `normalize` maps the authored tool name to the canonical runtime tool name — the
 * caller passes the shared `normalizeToolName` alias map so `Read`/`Edit` resolve to
 * `file_read`/`file_edit`; it defaults to identity to keep this a dependency-free
 * core helper. Results are de-duplicated by tool+scope and preserve input order.
 *
 * These are ADDITIVE allow rules only: a matching DENY unconditionally wins in the
 * permission engine, so prepending them ahead of inherited parent rules safely scopes
 * the subagent UP to the author's allowlist without ever relaxing a parent deny.
 */
export function scopedToolAllowRules(
  allowedTools: readonly string[] | undefined,
  normalize: (name: string) => string = (n) => n,
): Array<{ tool: string; pattern: string; action: 'allow' }> {
  if (!allowedTools || allowedTools.length === 0) return [];
  const rules: Array<{ tool: string; pattern: string; action: 'allow' }> = [];
  const seen = new Set<string>();
  for (const raw of allowedTools) {
    if (typeof raw !== 'string') continue;
    const entry = raw.trim();
    const open = entry.indexOf('(');
    if (open <= 0 || !entry.endsWith(')')) continue; // no scope (or no tool name) → name filter governs
    const base = entry.slice(0, open).trim();
    if (base.length === 0) continue;
    const scope = entry.slice(open + 1, -1).trim();
    if (scope.length === 0 || scope === '*') continue; // unrestricted scope → no rule
    const tool = normalize(base);
    const key = `${tool}\u0000${scope}`;
    if (seen.has(key)) continue;
    seen.add(key);
    rules.push({ tool, pattern: scope, action: 'allow' });
  }
  return rules;
}

/**
 * True when `content` references the argument placeholder `name` in EITHER the
 * braced `${name}` or the bare `$name` form (the same two forms
 * {@link substituteNamedArgPlaceholders} fills). The bare form uses Claude's
 * word-boundary guard so `$name` inside `$namexyz` / `$name[0]` does not count.
 * Shared by the content path's "did the template already consume the args?" check
 * so the two placeholder forms never diverge from what substitution actually binds.
 */
export function contentReferencesArgName(content: string, name: string): boolean {
  if (typeof content !== 'string' || typeof name !== 'string' || name.length === 0) {
    return false;
  }
  const esc = escapeForRegExp(name);
  return (
    content.includes(`\${${name}}`) ||
    new RegExp(`\\$${esc}(?![\\[\\w])`).test(content)
  );
}

/**
 * Substitute a skill/command's declared positional-argument NAMES into its body
 * (parity with Claude Code's `substituteArguments`). For each declared name at
 * index `i`, both the braced `${name}` and the bare `$name` placeholder are
 * replaced with `tokens[i]` (or `''` when no token was supplied, so a placeholder
 * is never left literal). The bare form uses Claude's word-boundary guard
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
    const esc = escapeForRegExp(argName);
    // Braced ${name}
    out = out.replace(new RegExp(`\\$\\{${esc}\\}`, 'g'), () => value);
    // Bare $name — not followed by a word char or `[` (so $name[0] / $namexyz stay)
    out = out.replace(new RegExp(`\\$${esc}(?![\\[\\w])`, 'g'), () => value);
  });
  return out;
}
