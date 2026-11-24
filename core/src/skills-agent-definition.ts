// Split from skills.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { HookConfigEntry } from './hooks';
import type { McpServerConfig } from './mcp/manager';
import { MAX_SKILL_ARG_NAMES, MAX_SKILL_ARG_NAME_LENGTH, SYSTEM_PROMPT_MODES, normalizeNameList } from './skills-validate-skill-args';
import type { AgentColorName, AgentIsolationMode, AgentMemoryScope, AgentSource, SystemPromptMode } from './skills-validate-skill-args';

export interface AgentDefinition {
  name: string;
  description: string;
  systemPrompt: string;   // The agent's system prompt (markdown body)
  filePath: string;
  /** The agent definition's on-disk FILE basename (with extension), tracked
   *  SEPARATELY from the resolved {@link name} (parity with the reference's agentFileUtils,
   *  which distinguishes the file name from the frontmatter agent type). When a file
   *  declares a `name:` frontmatter that differs from its filename, the frontmatter
   *  wins for the canonical {@link name} but the filename is retained here so a
   *  mismatch can be surfaced (as a load warning) instead of silently masked. Absent
   *  for in-memory/built-in agents. Compared via {@link agentFileNameMismatch}. */
  fileName?: string;
  model?: string;         // Model override; the literal 'inherit' means "use the parent's model"
  maxTurns?: number;
  /** Allowed tool names. `undefined` ⇒ ALL tools (no restriction); a present but
   *  EMPTY array ⇒ NO tools (an explicit deny-everything allowlist) — the two are
   *  distinct, matching the reference's agent `tools` semantics (ConfirmStep). An
   *  empty allowlist resolves to a zero-tool set, which the task tool's zero-tool
   *  boundary guard then rejects with an actionable error rather than silently
   *  granting every tool. */
  tools?: string[];
  disallowedTools?: string[];
  /** Permission mode override for sub-agent invocation. `'plan'` runs the sub-agent
   *  in read-only plan mode (reads/searches allowed, mutations denied). `'bubble'`
   *  is the sentinel (parity with the reference's async/background-agent `bubble`
   *  permission mode): instead of pinning a fixed mode, the spawned agent runs with
   *  the PARENT's live permission stance so every permission decision bubbles up to
   *  the parent terminal rather than auto-denying — resolved at spawn time by
   *  {@link BUBBLE_PERMISSION_MODE}/`resolveBubblePermissionMode` in task.ts. The
   *  valid declarable set is {@link AGENT_DECLARABLE_PERMISSION_MODES}. */
  permissionMode?: 'auto-allow' | 'ask' | 'deny-all' | 'plan' | 'bubble';
  /** Token budget for auto-compaction inside this subagent's context window. */
  autoCompactWindow?: number;
  /** Which tier this agent was loaded from (built-in | plugin | user | project). */
  source?: AgentSource;
  /** When a higher-priority same-named agent shadows this one, the winner's
   *  source. Undefined on the active (winning) agent. */
  overriddenBy?: AgentSource;
  /** The agent bundle's root directory — its on-disk provenance base (parity with
   *  the reference's agent `baseDir`). For a PLUGIN agent this is the plugin bundle
   *  root so the agent's bundled assets resolve under a stable path regardless of
   *  the session cwd; for a disk agent it is the directory the `.md` lives in. It is
   *  exported to the agent's frontmatter {@link hooks} as `CLAUDE_PLUGIN_ROOT` (and
   *  the `AMI_PLUGIN_ROOT` alias) so a hook command can reference
   *  `${CLAUDE_PLUGIN_ROOT}/scripts/x.sh`. Resolved — with a fallback to the agent's
   *  own on-disk directory — via {@link agentPluginRoot}; mirrors the skill
   *  {@link SkillDefinition.skillRoot} / {@link skillPluginRoot} pair. */
  baseDir?: string;
  /** Orchestrator delegation hint — the "when should the parent pick this agent"
   *  cue, distinct from `description` (parity with the reference's agent `whenToUse`).
   *  Parsed from a `when-to-use`/`when_to_use` frontmatter key. */
  whenToUse?: string;
  /** Per-agent display color used to tint the subagent's badge/label in listings
   *  (parity with the reference's agent `color`). `undefined` ⇒ the UI picks a default. */
  color?: AgentColorName;
  /** Display color for the agent's DESCRIPTION badge, distinct from {@link color}
   *  (which tints the agent-type badge) — parity with the reference's AgentProgressLine
   *  `descriptionColor`. Normalized/validated via {@link parseAgentColor}. */
  descriptionColor?: AgentColorName;
  /** Project-context knob (parity with the reference's agent `omitClaudeMd`). AMI subagents
   *  run on a REPLACED lean/identity prompt that already carries NEITHER the project
   *  CLAUDE.md/AGENTS.md NOR the session-start git status — a token-saving superset
   *  perk, stronger than the reference's per-agent `omitClaudeMd` (which only lets
   *  read-only agents DROP a context they otherwise get). So AMI inverts the knob into
   *  an OPT-IN: an explicit `omit-claude-md: false` requests the project instruction
   *  context back (consumed by the task tool's {@link resolveAgentProjectContext});
   *  absent/`true` keeps the lean default. An invalid (non-boolean) value surfaces a
   *  load warning via {@link validateAgentDefinition}. */
  omitClaudeMd?: boolean;
  /** Require plan-mode approval before this delegated agent may implement/mutate
   *  (parity with the reference's swarm teammate `planModeRequired`). An invalid
   *  (non-boolean) value surfaces a load warning via {@link validateAgentDefinition}. */
  planModeRequired?: boolean;
  /** Persistent-memory storage scope for the agent (parity with the reference's agent
   *  `memory`): user / project / local. */
  memory?: AgentMemoryScope;
  /** Run the subagent in an isolated git worktree by default (parity with the reference's
   *  agent `isolation`). */
  isolation?: AgentIsolationMode;
  /** When true, spawning this agent defaults to background/detached execution
   *  unless the caller explicitly overrides `run_in_background` (parity with
   *  the reference's agent `background`). */
  background?: boolean;
  /** MCP server names this agent REQUIRES to be available before it can run
   *  (parity with the reference's agent `requiredMcpServers`). Each entry is matched
   *  case-insensitively as a substring against the live server names; when any
   *  requirement is unmet the agent is filtered out of the spawnable set. See
   *  {@link hasRequiredMcpServers} / {@link filterAgentsByMcpRequirements}. */
  requiredMcpServers?: string[];
  /** Per-subagent MCP servers to launch for this agent's run, additive to the
   *  parent's servers (parity with the reference's per-agent `mcpServers`). Mirrors the
   *  stance `mcpServers` shape on {@link SkillDefinition}. */
  mcpServers?: Record<string, McpServerConfig>;
  /** Names of skills to PRELOAD into this agent's context when it is spawned
   *  (parity with the reference's agent `skills` allowlist). Normalized via
   *  {@link parseAgentSkills}. */
  skills?: string[];
  /** Agent-lifecycle hooks declared in the agent's frontmatter (parity with the reference
   *  Code's agent `hooks` field + registerFrontmatterHooks with `isAgent=true`).
   *  When a NAMED subagent carries these, they are registered on its spawned
   *  sub-engine for the duration of that agent's run and torn down when it finishes
   *  — so an individual agent definition can declare its own scoped hooks instead of
   *  relying on the global/plugin-only hooks surface. Parsed from frontmatter via
   *  {@link parseFrontmatterHooks} (hooks.ts); REFUSED for plugin-sourced agents
   *  (a privilege-escalation vector) alongside permission-mode / mcp-servers. */
  hooks?: HookConfigEntry[];
  /** Restrict which sub-agent *types* this agent may itself spawn via the task
   *  tool (parity with the reference's `allowedAgentTypes` metadata). `undefined`/empty
   *  ⇒ no restriction. Checked with {@link canSpawnAgentType}. */
  allowedAgentTypes?: string[];
  /** Default thinking/reasoning-effort budget for this subagent's run (parity with
   *  the reference's agent `effort`). A named level (`low`|`medium`|`high`|`xhigh`|`max`)
   *  OR the decimal string of a non-negative integer (raw token budget) — normalized
   *  at load time via {@link parseEffort}. Applied as the sub-engine's thinking level
   *  UNLESS the spawning task call passes an explicit per-call `effort` override. */
  effort?: string;
  /** Text auto-prepended to the agent's FIRST user turn when it is spawned (parity
   *  with the reference's agent `initialPrompt`). Lets an agent definition seed its run with
   *  a standing instruction ahead of the caller's task prompt. Only applied on the
   *  initial spawn, never when resuming an existing agent session. */
  initialPrompt?: string;
  /** Dynamic/computed system-prompt builder (parity with the reference's agent
   *  `getSystemPrompt({ toolUseContext })`). When present, the agent's system prompt
   *  is COMPUTED at spawn time from runtime context (e.g. the live set of custom
   *  skills/commands/agents/MCP servers) instead of the static {@link systemPrompt}
   *  string. May be sync or async; resolve via {@link resolveAgentSystemPrompt},
   *  which falls back to `systemPrompt` when the builder is absent. */
  getSystemPrompt?: (ctx?: Record<string, unknown>) => string | Promise<string>;
  /** Provenance flag recording that this agent definition was AUTO-GENERATED (e.g.
   *  by a creation wizard / model) rather than hand-authored (parity with the reference's
   *  agent `wasGenerated`). Surfaced in `/agents` so a user can tell generated
   *  definitions apart; parsed via {@link parseBooleanFlag}. */
  wasGenerated?: boolean;
  /** How this agent's {@link systemPrompt} combines with the base subagent stance
   *  when the agent is spawned (parity with the reference's swarm teammate
   *  `systemPromptMode`). `'append'` layers the agent's prompt AFTER the default
   *  subagent stance (the agent EXTENDS the base guidance); `'replace'`/`'default'`
   *  (and absent) make the agent's prompt the authoritative identity that fully
   *  REPLACES the base stance (AMI's existing behavior). Validated/normalized via
   *  {@link parseSystemPromptMode}; honored in the task tool's spawn path. */
  systemPromptMode?: SystemPromptMode;
  /** Short, strongly-flagged hard constraint reinforced separately from
   *  {@link systemPrompt} (parity with the reference's `criticalSystemReminder_EXPERIMENTAL`).
   *  A must-hold rule — e.g. "no project writes", "you MUST end with VERDICT:" — is
   *  appended to the agent's authoritative system prompt as a conspicuous reminder so
   *  it does not drift into the surrounding prose. Parsed from a
   *  `critical-system-reminder`/`criticalSystemReminder` frontmatter key; honored in
   *  the task tool's spawn path via {@link buildCriticalReminderBlock}. */
  criticalSystemReminder?: string;
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
 * True when every MCP server the agent REQUIRES is available (parity with the reference
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
 * available (parity with the reference's `filterAgentsByMcpRequirements`). Agents
 * with unmet requirements are dropped from the spawnable/listable set.
 */
export function filterAgentsByMcpRequirements<T extends Pick<AgentDefinition, 'requiredMcpServers'>>(
  agents: readonly T[],
  availableServers: readonly string[],
): T[] {
  return agents.filter((agent) => hasRequiredMcpServers(agent, availableServers));
}

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
 * the reference's `validateAgentType`): a non-empty token that starts and ends with
 * an alphanumeric, contains only letters/numbers/hyphens, and is 3–50 characters.
 * Returns a human-readable error message for the first rule violated, or `null`
 * when the name is valid — so a creation flow can surface the problem inline. The
 * checks are ordered exactly like the reference's (required → charset → min → max).
 */
export function validateIdentifierName(kind: string, value: string): string | null {
  if (!value) return `${kind} is required`;
  if (!/^[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]$/.test(value)) {
    return `${kind} must start and end with alphanumeric characters and contain only letters, numbers, and hyphens`;
  }
  if (value.length < 3) return `${kind} must be at least 3 characters long`;
  if (value.length > 50) return `${kind} must be less than 50 characters`;
  return null;
}

/**
 * Validate an agent *type* identifier (parity with the reference's `validateAgentType`).
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
 * be invoked by (parity with the reference's bundled skill `aliases`). Accepts a
 * comma/whitespace string or a string array; de-duplicated, length- and
 * count-capped via {@link normalizeNameList}. Any other value yields `[]`.
 */
export function parseSkillAliases(value: unknown): string[] {
  return normalizeNameList(value);
}

/**
 * True when EVERY environment variable named in `required` is set to a non-empty
 * value in `env` (parity with the reference's env/feature-flag-gated bundled skills). An
 * absent/empty requirement list is trivially satisfied; a variable that is unset,
 * `undefined`, or an empty/whitespace-only string fails the gate. A non-string
 * entry is ignored (treated as "no requirement" for that slot) so a malformed
 * frontmatter list can never crash the gate. Pure: the caller supplies `env`.
 */
export function requiredEnvSatisfied(
  required: readonly string[] | undefined,
  env: Record<string, string | undefined>,
): boolean {
  if (!required || required.length === 0) return true;
  for (const name of required) {
    if (typeof name !== 'string' || name.length === 0) continue;
    const value = env[name];
    if (value === undefined || value.trim().length === 0) return false;
  }
  return true;
}

/** The permission-mode values an agent/skill definition may declare. `'plan'` runs
 *  the sub-agent in read-only PLAN mode (parity with the reference's `plan` agent
 *  permission mode): the permission engine auto-allows read/search tools and DENIES
 *  every mutating call, so a planning subagent can investigate and propose without
 *  touching the workspace. It is strictly MORE restrictive than `ask` (see task.ts's
 *  PERMISSION_MODE_RANK), so {@link clampPermissionMode} never lets it escalate a
 *  parent's stance. Kept in lockstep with the engine's `PERMISSION_MODES` (which
 *  already accepts `plan`) so the two never diverge. */
export const AGENT_PERMISSION_MODES = ['auto-allow', 'ask', 'deny-all', 'plan'] as const;

/** The `'bubble'` permission-mode SENTINEL an agent definition may declare (parity
 *  with the reference's async/background-agent `bubble` permission mode,
 *  runAgent.ts:L440-L463). It is NOT one of {@link AGENT_PERMISSION_MODES} (the real
 *  engine modes kept in lockstep with the engine's `PERMISSION_MODES`) — it means
 *  "do not pin a mode of my own; run with the parent's LIVE stance so every
 *  permission decision bubbles up to the parent terminal instead of auto-denying".
 *  Resolved to the parent's concrete mode at spawn time by `resolveBubblePermissionMode`
 *  (task.ts) BEFORE the engine's enum validation, so it never reaches the
 *  PermissionManager as a literal. Re-exported as `FORK_BUBBLE_PERMISSION_MODE` from
 *  task.ts (single source — no second literal). */
export const BUBBLE_PERMISSION_MODE = 'bubble';

/** Structured outcome of validating an agent definition. */
export interface AgentValidationResult {
  valid: boolean;      // no hard errors (the definition is loadable)
  errors: string[];    // reasons the definition must be rejected
  warnings: string[];  // non-fatal issues the author should see
}

/** The affordance shown while a COMMAND runs when it declares no custom message
 *  (parity with the reference's default command `progressMessage` of `'running'`). */
export const DEFAULT_SKILL_PROGRESS_MESSAGE = 'running';

/** The affordance shown while a SKILL loads when it declares no custom message
 *  (parity with the reference's loadPluginCommands, which defaults a skill-sourced
 *  entry's `progressMessage` to `'loading'` rather than a command's `'running'`). */
export const SKILL_LOADING_PROGRESS_MESSAGE = 'loading';

/**
 * The kind-aware DEFAULT progress affordance (parity with the reference's
 * loadPluginCommands `progressMessage: isSkill ? 'loading' : 'running'`): a
 * skill-sourced entry defaults to {@link SKILL_LOADING_PROGRESS_MESSAGE}
 * (`'loading'`), a command-sourced entry to {@link DEFAULT_SKILL_PROGRESS_MESSAGE}
 * (`'running'`) — so a listing/affordance distinguishes a skill loading from a
 * command running even when neither declares a custom message.
 */
export function defaultProgressMessage(isSkill: boolean): string {
  return isSkill ? SKILL_LOADING_PROGRESS_MESSAGE : DEFAULT_SKILL_PROGRESS_MESSAGE;
}

// ---------------------------------------------------------------------------
// Skill-metadata flags — declarative command/skill knobs (parity with the reference's
// command `isSensitive` / `isHidden` / `userFacingName()` /
// `disableNonInteractive` / `supportsNonInteractive`). Pure predicates; the skill
// tool (core) and the `/skills` listing (common) wire them into the real echo /
// catalog / non-interactive-gate paths.
// ---------------------------------------------------------------------------

/** Masked placeholder shown in place of a sensitive skill's arguments. */
export const REDACTED_SKILL_ARGS = '***';

/**
 * Resolve the base directory an AGENT exports to its frontmatter `!`-blocks /
 * hooks as `CLAUDE_PLUGIN_ROOT` (parity with the reference's agent `baseDir`). The
 * agent analogue of {@link skillPluginRoot}: returns the agent's explicit
 * {@link AgentDefinition.baseDir} when it is a non-empty string; otherwise falls
 * back to the directory of its real on-disk {@link AgentDefinition.filePath} (a
 * `<builtin>`/in-memory sentinel or a bare filename yields no directory). Returns
 * `undefined` when neither is available, so a caller can skip the
 * `${CLAUDE_PLUGIN_ROOT}` substitution. Never throws.
 */
export function agentPluginRoot(
  agent: Pick<AgentDefinition, 'baseDir' | 'filePath'>,
): string | undefined {
  if (typeof agent.baseDir === 'string' && agent.baseDir.trim().length > 0) {
    return agent.baseDir.trim();
  }
  const fp = agent.filePath;
  if (typeof fp !== 'string' || fp.startsWith('<') || !fp.includes('/')) return undefined;
  const slash = fp.lastIndexOf('/');
  const dir = fp.slice(0, slash);
  return dir.length > 0 ? dir : undefined;
}

/**
 * Resolve an agent's system prompt for a given runtime context (parity with
 * the reference's agent `getSystemPrompt({ toolUseContext })`). When the agent declares a
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
