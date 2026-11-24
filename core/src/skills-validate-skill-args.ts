// Split from skills.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).

/** The classification tokens a command/skill may declare (parity with the reference's
 *  command `kind`). `'workflow'` badges a workflow-backed command in help/typeahead;
 *  a plain skill declares none. */
export const SKILL_KINDS = ['workflow'] as const;

export type SkillKind = (typeof SKILL_KINDS)[number];

/**
 * Where an agent definition was loaded from (parity with the reference's agent source
 * tiers — `built-in`, `plugin`, `policySettings`, `userSettings`, `localSettings`,
 * `projectSettings`, `flagSettings`). A higher-priority same-named agent SHADOWS a
 * lower one; the load order in `SkillManager.loadAll` realizes the precedence. The
 * AMI tiers:
 *   - `built-in` — the bundled baseline agents.
 *   - `plugin`   — contributed by an installed plugin bundle.
 *   - `managed`  — enterprise/policy-managed (the reference's `policySettings`): admin-provided
 *                  and READ-ONLY (see {@link AGENT_NONEDITABLE_SOURCES}).
 *   - `user`     — the user's home-level agents (`~/.claude`/`~/.ami`), shared across projects.
 *   - `local`    — project-LOCAL, non-shared agents (the reference's `localSettings`): editable,
 *                  override `user`, and are not committed to the team's shared config.
 *   - `project`  — the project's committed/shared agents (`.claude`/`.ami`), the team default.
 *   - `flag`     — injected via a CLI flag for a single run (the reference's `flagSettings`):
 *                  highest-priority and READ-ONLY (a per-run override, not a persisted file).
 * Mirrors the reference's agent source groups so `/agents` can group and flag shadowed
 * entries instead of silently dropping the loser.
 */
export const AGENT_SOURCES = [
  'built-in', 'plugin', 'managed', 'user', 'local', 'project', 'flag',
] as const;

export type AgentSource = (typeof AGENT_SOURCES)[number];

/**
 * A source *selector* for an agent listing: a concrete {@link AgentSource} or the
 * aggregate `'all'` (every source). Mirrors the reference's rich agent-source
 * parameter (`'all' | 'built-in' | 'plugin' | SettingSource`), collapsed onto
 * AMI's four provenance tiers plus the aggregate.
 */
export type AgentSourceSelector = AgentSource | 'all';

/**
 * Human-readable display name for an agent source selector (parity with the reference
 * Code's `getAgentSourceDisplayName`, components/agents/utils.ts). Turns a raw
 * provenance token ('built-in', 'plugin', 'user', 'project') — or the aggregate
 * 'all' — into the short prose label an `/agents` listing groups under, so a user
 * reads WHERE each agent came from instead of an opaque token:
 *   - `'all'`      → `'All sources'`  (the aggregate selector)
 *   - `'built-in'` → `'Built-in'`
 *   - `'plugin'`   → `'Plugin'`
 *   - `'managed'`  → `'Managed'`      (enterprise/policy-managed, read-only)
 *   - `'user'`     → `'User'`
 *   - `'local'`    → `'Local'`        (project-local, not shared)
 *   - `'project'`  → `'Project'`
 *   - `'flag'`     → `'Flag'`         (CLI-flag-injected, read-only)
 * An unknown/absent source yields `'Unknown'` so a malformed provenance never
 * surfaces as a blank label. This is the ONE canonical agent-source label map:
 * the SkillManager (common/) derives its `AGENT_SOURCE_LABELS` + grouped
 * `AGENT_SOURCE_GROUPS` headers (`<label> agents`) from it rather than keeping a
 * second literal copy. Pure (no I/O) so every listing surface shares it.
 */
export function getAgentSourceDisplayName(source: AgentSourceSelector | undefined): string {
  switch (source) {
    case 'all': return 'All sources';
    case 'built-in': return 'Built-in';
    case 'plugin': return 'Plugin';
    case 'managed': return 'Managed';
    case 'user': return 'User';
    case 'local': return 'Local';
    case 'project': return 'Project';
    case 'flag': return 'Flag';
    default: return 'Unknown';
  }
}

/**
 * The named display colors an agent may declare (parity with the reference's
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
 * non-string) return `undefined` (UI default). Mirrors the reference's
 * ColorPicker, where `'automatic'` is an explicit option that clears the color.
 */
export function parseAgentColor(value: unknown): AgentColorName | undefined {
  if (typeof value !== 'string') return undefined;
  const t = value.trim().toLowerCase();
  return (AGENT_COLORS as readonly string[]).includes(t) ? (t as AgentColorName) : undefined;
}

/**
 * The persistent-memory storage scopes an agent may declare (parity with the reference
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
 * The isolation modes an agent may declare (parity with the reference's agent
 * `isolation`). AMI supports `worktree` (run the subagent in an isolated git
 * worktree). the reference's ant-only `remote` mode has no AMI analogue and is rejected.
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
 * Parse a raw frontmatter `background` value (parity with the reference's agent
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
 * Parse a generic TRI-STATE boolean frontmatter flag (parity with the reference's
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
export function normalizeNameList(value: unknown): string[] {
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
 * preload into a spawned agent's context (parity with the reference's agent `skills`).
 * Accepts a comma/whitespace string or a string array; see
 * {@link normalizeNameList} for the normalization rules.
 */
export function parseAgentSkills(value: unknown): string[] {
  return normalizeNameList(value);
}

/**
 * Parse a frontmatter `allowedAgentTypes` field into the ordered list of
 * sub-agent types this agent may itself spawn (parity with the reference's
 * `allowedAgentTypes` metadata, which splits a comma-delimited rule string).
 * Accepts a comma/whitespace string or a string array.
 */
export function parseAllowedAgentTypes(value: unknown): string[] {
  return normalizeNameList(value);
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
 * return an ACTIONABLE verdict (parity with the reference's skill/agent input
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
 * (parity with the reference's swarm teammate `systemPromptMode`). `'append'` extends the
 * base stance; `'replace'`/`'default'` substitute it. Shared by the per-spawn
 * `system_prompt_mode` task-tool input AND {@link AgentDefinition.systemPromptMode}
 * so the two never drift apart.
 */
export const SYSTEM_PROMPT_MODES = ['default', 'replace', 'append'] as const;

export type SystemPromptMode = (typeof SYSTEM_PROMPT_MODES)[number];
