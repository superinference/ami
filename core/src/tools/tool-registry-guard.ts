export interface ToolConflict {
  toolName: string;
  existingSource: string;
  conflictingSource: string;
  reason: string;
}

export const RESERVED_TOOL_PREFIXES = [
  'file_read', 'file_write', 'file_edit',
  'bash', 'grep', 'glob', 'list_dir',
  'web_fetch', 'web_search',
  'notebook_edit',
  'task_', 'agent_',
] as const;

// PARITY (additive): Anthropic/OpenAI function-tool name constraint. A provider
// rejects the ENTIRE request if any advertised tool name is empty, too long, or
// contains characters outside this set — so an ill-named plugin tool would take the
// whole session down. The registry guard enforces the constraint at registration
// time, turning a provider-side 400 into a clean, local ToolConflict.
export const TOOL_NAME_PATTERN = /^[a-zA-Z0-9_-]+$/;
export const MAX_TOOL_NAME_LENGTH = 64;

/**
 * PARITY (additive): validate a tool name against the provider constraint.
 * Returns a human-readable reason string when invalid, or `null` when the name is
 * acceptable (mirrors the `string | null` convention used by the rest of this module).
 */
export function validateToolName(name: string): string | null {
  if (typeof name !== 'string' || name.length === 0) {
    return 'Tool name must be a non-empty string';
  }
  if (name.length > MAX_TOOL_NAME_LENGTH) {
    return `Tool name "${name}" exceeds the ${MAX_TOOL_NAME_LENGTH}-character limit`;
  }
  if (!TOOL_NAME_PATTERN.test(name)) {
    return `Tool name "${name}" contains invalid characters (allowed: letters, digits, "_", "-")`;
  }
  return null;
}

/**
 * PARITY (additive): canonical form of a tool name used for reserved-prefix and
 * cross-source collision matching. Mirrors the intent of the reference's
 * `normalizeNameForMCP` (services/mcp/normalization.ts): because
 * {@link validateToolName} has already rejected any out-of-charset name by the
 * time matching runs, canonicalization only has to fold the two spoof vectors a
 * crafted non-core name could use to dodge an exact/`startsWith` reserved check —
 * letter case (`Bash` vs `bash`) and doubled / leading underscores (`__bash`,
 * `task__create`) that forge or pad the `__`/`_` delimiter.
 *
 * Unlike `normalizeNameForMCP`, AMI deliberately does NOT strip a TRAILING
 * underscore: {@link RESERVED_TOOL_PREFIXES} uses a trailing `_` as a namespace
 * boundary marker (`task_`, `agent_`), so a trailing `_` is significant and the
 * prefix-boundary check (not canonicalization) is what matches `bash_`.
 */
export function canonicalToolName(name: string): string {
  return name.toLowerCase().replace(/_+/g, '_').replace(/^_/, '');
}

/** Minimal structural shape a tool definition must satisfy to be registrable.
 *  Kept local (not importing the full `ToolDefinition`) so this guard stays a
 *  standalone building block. */
export interface RegistrableToolLike {
  name: string;
  inputSchema?: {
    type?: unknown;
    properties?: unknown;
    required?: unknown;
  } | null;
  aliases?: readonly string[];
}

/**
 * PARITY (additive): structural pre-registration check of a tool definition.
 * A tool missing a `name`, or whose `inputSchema` is absent / not an
 * `{ type: 'object', properties: {...} }` JSON-Schema object, cannot be advertised
 * to a provider — this surfaces the problem locally instead of at request time.
 * Returns a reason string when invalid, else `null`.
 */
export function validateToolDefinition(tool: RegistrableToolLike): string | null {
  const nameReason = validateToolName(tool?.name);
  if (nameReason) return nameReason;
  const schema = tool.inputSchema;
  if (!schema || typeof schema !== 'object') {
    return `Tool "${tool.name}" is missing an inputSchema`;
  }
  if (schema.type !== 'object') {
    return `Tool "${tool.name}" inputSchema.type must be "object"`;
  }
  if (!schema.properties || typeof schema.properties !== 'object') {
    return `Tool "${tool.name}" inputSchema.properties must be an object`;
  }
  return null;
}

/**
 * PARITY (additive): operator-configurable knobs for the registration gate. An
 * operator can RESERVE extra prefixes so plugin/MCP tools cannot shadow an
 * org-specific built-in namespace, and TRUST extra sources so a verified
 * first-party plugin may claim a reserved prefix exactly like `core`/`builtin`.
 * Both are off by default (empty), preserving AMI's existing behavior. Grounded in
 * the reference's config-driven tool filtering (tools.ts) — surfaced here as
 * registration-gate configuration rather than a fixed-at-compile-time set.
 */
export interface ToolRegistryGuardOptions {
  /** Extra reserved prefixes (beyond {@link RESERVED_TOOL_PREFIXES}) that only a
   *  trusted source may claim. Matched on the canonical form like the built-ins;
   *  blank entries are ignored. */
  extraReservedPrefixes?: readonly string[];
  /** Sources that — like the implicit `core`/`builtin` — may register a
   *  reserved-prefix name without triggering a conflict. */
  trustedSources?: ReadonlySet<string> | readonly string[];
}

/** The two sources that are ALWAYS trusted to claim a reserved prefix. */
const BUILTIN_TRUSTED_SOURCES: ReadonlySet<string> = new Set(['core', 'builtin']);

/** PARITY (additive): is `source` allowed to register a reserved-prefix name — one of
 *  the always-trusted built-in sources, or an operator-configured trusted source? */
function isTrustedSource(
  source: string,
  trusted?: ReadonlySet<string> | readonly string[],
): boolean {
  if (BUILTIN_TRUSTED_SOURCES.has(source)) return true;
  if (!trusted) return false;
  const set = trusted instanceof Set ? trusted : new Set(trusted);
  return set.has(source);
}

function matchesReservedPrefix(name: string, extraPrefixes?: readonly string[]): string | null {
  // PARITY (additive): match on the CANONICAL form so a non-core source cannot slip
  // a tool past the reserved guard with a case variant (`Bash`) or a doubled/leading
  // underscore (`__bash`, `task__create`) that is equivalent after normalization.
  const canonical = canonicalToolName(name);
  const prefixes: readonly string[] =
    extraPrefixes && extraPrefixes.length
      ? [...RESERVED_TOOL_PREFIXES, ...extraPrefixes]
      : RESERVED_TOOL_PREFIXES;
  for (const prefix of prefixes) {
    // Canonicalize the prefix too (folds case / underscore runs) so an operator-supplied
    // extra prefix is matched on the same normalized footing as a crafted tool name.
    const canonicalPrefix = canonicalToolName(prefix);
    if (canonicalPrefix.length === 0) continue; // ignore blank/underscore-only extras
    const boundary = canonicalPrefix.endsWith('_') ? canonicalPrefix : canonicalPrefix + '_';
    if (canonical === canonicalPrefix || canonical.startsWith(boundary)) return prefix;
  }
  return null;
}

export function detectToolConflict(
  toolName: string,
  source: string,
  existingTools: Map<string, string>,
  options?: ToolRegistryGuardOptions,
): ToolConflict | null {
  // PARITY (additive): reject structurally invalid names/sources BEFORE the
  // duplicate/prefix checks — an unnameable tool can never be registered under any
  // source, and an attribution-less registration cannot be tracked for conflicts.
  const nameReason = validateToolName(toolName);
  if (nameReason) {
    return {
      toolName,
      existingSource: '',
      conflictingSource: source,
      reason: nameReason,
    };
  }
  if (typeof source !== 'string' || source.length === 0) {
    return {
      toolName,
      existingSource: '',
      conflictingSource: source,
      reason: `Tool "${toolName}" must be registered with a non-empty source`,
    };
  }

  const existingSource = existingTools.get(toolName);
  if (existingSource && existingSource !== source) {
    return {
      toolName,
      existingSource,
      conflictingSource: source,
      // PARITY (additive): state the OUTCOME + REMEDY, not just the condition —
      // the incumbent stays active and the newcomer is ignored; renaming resolves it.
      reason: `Tool "${toolName}" already registered by "${existingSource}" — the existing tool stays active and this one is ignored; rename the conflicting tool (e.g. prefix it with the plugin/server name) to register it alongside.`,
    };
  }

  if (!isTrustedSource(source, options?.trustedSources)) {
    const prefix = matchesReservedPrefix(toolName, options?.extraReservedPrefixes);
    if (prefix) {
      return {
        toolName,
        existingSource: 'core',
        conflictingSource: source,
        // PARITY (additive): outcome + remedy wording (see above).
        reason: `Tool "${toolName}" conflicts with reserved prefix "${prefix}" — reserved for built-in tools, so this one is ignored; rename it (e.g. prefix it with the plugin/server name) so it is not dropped.`,
      };
    }
  }

  // PARITY (additive): canonical cross-source collision. Two textually-different
  // names from DIFFERENT sources that collapse to the same canonical form (e.g. a
  // plugin's `My_Tool` vs a core `my_tool`, or `__fetch` vs `fetch`) would collide
  // once the provider normalizes them, silently shadowing one another. Catch it at
  // the single registration chokepoint instead of at request time.
  const canonical = canonicalToolName(toolName);
  for (const [existingName, owner] of existingTools) {
    if (
      owner !== source &&
      existingName !== toolName &&
      canonicalToolName(existingName) === canonical
    ) {
      return {
        toolName,
        existingSource: owner,
        conflictingSource: source,
        reason: `Tool "${toolName}" collides with "${existingName}" (registered by "${owner}") after name canonicalization — the provider would treat them as the same tool; rename one so their canonical names differ.`,
      };
    }
  }

  return null;
}

export function isReservedToolName(name: string): boolean {
  return matchesReservedPrefix(name) !== null;
}

export class ToolRegistryGuard {
  private _registry = new Map<string, string>();
  // PARITY (additive): alias -> canonical tool name, for alias-aware dispatch and
  // collision detection (grounded in ToolDefinition.aliases).
  private _aliases = new Map<string, string>();
  private _conflicts: ToolConflict[] = [];

  // PARITY (additive): operator-configurable registration-gate knobs (extra reserved
  // prefixes / trusted sources). Empty by default — behavior is unchanged unless set.
  constructor(private readonly _options: ToolRegistryGuardOptions = {}) {}

  register(toolName: string, source: string, aliases?: readonly string[]): ToolConflict | null {
    const conflict = detectToolConflict(toolName, source, this._registry, this._options);
    if (conflict) {
      this._conflicts.push(conflict);
      return conflict;
    }

    // PARITY (additive): validate every alias before committing anything, so a bad
    // alias neither half-registers the tool nor leaks a partial alias mapping.
    const aliasList = aliases ?? [];
    for (const alias of aliasList) {
      const aliasConflict = this._detectAliasConflict(alias, toolName, source);
      if (aliasConflict) {
        this._conflicts.push(aliasConflict);
        return aliasConflict;
      }
    }

    this._registry.set(toolName, source);
    for (const alias of aliasList) {
      if (alias !== toolName) this._aliases.set(alias, toolName);
    }
    return null;
  }

  private _detectAliasConflict(
    alias: string,
    toolName: string,
    source: string,
  ): ToolConflict | null {
    const nameReason = validateToolName(alias);
    if (nameReason) {
      return {
        toolName: alias,
        existingSource: '',
        conflictingSource: source,
        reason: `Alias of "${toolName}" is invalid: ${nameReason}`,
      };
    }
    // Aliasing yourself is a no-op, not a conflict.
    if (alias === toolName) return null;

    const existingTool = this._registry.get(alias);
    if (existingTool !== undefined) {
      return {
        toolName: alias,
        existingSource: existingTool,
        conflictingSource: source,
        reason: `Alias "${alias}" (of "${toolName}") shadows a registered tool owned by "${existingTool}"`,
      };
    }

    const existingAliasTarget = this._aliases.get(alias);
    if (existingAliasTarget !== undefined && existingAliasTarget !== toolName) {
      return {
        toolName: alias,
        existingSource: existingAliasTarget,
        conflictingSource: source,
        reason: `Alias "${alias}" is already an alias of "${existingAliasTarget}"`,
      };
    }

    if (!isTrustedSource(source, this._options.trustedSources)) {
      const prefix = matchesReservedPrefix(alias, this._options.extraReservedPrefixes);
      if (prefix) {
        return {
          toolName: alias,
          existingSource: 'core',
          conflictingSource: source,
          reason: `Alias "${alias}" (of "${toolName}") conflicts with reserved prefix "${prefix}"`,
        };
      }
    }

    return null;
  }

  isRegistered(toolName: string): boolean {
    return this._registry.has(toolName);
  }

  getSource(toolName: string): string | undefined {
    return this._registry.get(toolName);
  }

  /**
   * PARITY (additive): resolve a name (possibly an alias) to the canonical registered
   * tool name, or `undefined` when nothing answers to it. Mirrors the reference's
   * alias-aware tool dispatch and gives callers a single unknown-tool check.
   */
  resolveToolName(name: string): string | undefined {
    if (this._registry.has(name)) return name;
    return this._aliases.get(name);
  }

  /** PARITY (additive): true when a name resolves to a registered tool directly or via an alias. */
  isAvailable(name: string): boolean {
    return this.resolveToolName(name) !== undefined;
  }

  /** PARITY (additive): read-only view of the alias -> canonical map. */
  getAliases(): Map<string, string> {
    return new Map(this._aliases);
  }

  getConflicts(): readonly ToolConflict[] {
    return this._conflicts;
  }

  getRegisteredTools(): Map<string, string> {
    return new Map(this._registry);
  }

  clear(): void {
    this._registry.clear();
    this._aliases.clear();
    this._conflicts = [];
  }
}

/**
 * PARITY (additive): availability gating for the advertised tool set.
 *
 * Drops any tool whose `isEnabled(context)` returns false (feature-flag / env
 * condition — grounded in ToolDefinition.isEnabled) while ALWAYS keeping any tool
 * whose name is in `alwaysAvailable`. This is the mechanism by which the
 * ESSENTIAL + ORCHESTRATION tiers stay present in every mode: the caller passes
 * those names as the protected set so they can never be gated out, letting a stance
 * be activated from any tier. Tools without an `isEnabled` predicate are always kept.
 */
/** Minimal structural shape needed to match a tool by name or alias. */
export interface NameableToolLike {
  name: string;
  aliases?: readonly string[];
}

/**
 * PARITY (additive): does `tool` answer to `name` — by its primary name or any
 * declared `aliases` entry? Mirrors the reference's `toolMatchesName`, giving renamed
 * tools a back-compat dispatch path (e.g. a model/old transcript calling the
 * deprecated `KillShell` for `task_kill`).
 */
export function toolMatchesName(tool: NameableToolLike, name: string): boolean {
  return tool.name === name || (tool.aliases?.includes(name) ?? false);
}

/**
 * PARITY (additive): resolve a tool from a set by primary name or alias. An exact
 * (name-or-alias) match wins; a case-insensitive (name-or-alias) match is the
 * fallback so AMI keeps its existing case-insensitive tolerance. Mirrors the reference's
 * `findToolByName`. Returns `undefined` when nothing answers to `name`.
 */
export function findToolByName<T extends NameableToolLike>(
  tools: Iterable<T>,
  name: string,
): T | undefined {
  const list = Array.isArray(tools) ? (tools as T[]) : Array.from(tools);
  const exact = list.find((t) => toolMatchesName(t, name));
  if (exact) return exact;
  const lower = name.toLowerCase();
  return list.find(
    (t) =>
      t.name.toLowerCase() === lower ||
      (t.aliases?.some((a) => a.toLowerCase() === lower) ?? false),
  );
}

export function filterAvailableTools<
  T extends { name: string; isEnabled?: (context?: C) => boolean },
  C = unknown,
>(
  tools: readonly T[],
  context?: C,
  alwaysAvailable?: ReadonlySet<string> | readonly string[],
  denied?: (name: string) => boolean,
): T[] {
  const protectedSet =
    alwaysAvailable instanceof Set
      ? alwaysAvailable
      : new Set<string>(alwaysAvailable ?? []);
  return tools.filter((tool) => {
    // Protected (ESSENTIAL + ORCHESTRATION tier) tools can never be gated out — this
    // precedence preserves the TUI/CLI parity invariant that a stance can be activated
    // from any tier, so an operator `deny` knob applies only to the non-protected pool.
    if (protectedSet.has(tool.name)) return true;
    // PARITY (additive): operator-configurable blanket deny of a tool/source from the
    // advertised pool, mirroring the reference's `filterToolsByDenyRules` (tools.ts) —
    // a denied tool is stripped before the model ever sees it.
    if (denied && denied(tool.name)) return false;
    if (typeof tool.isEnabled !== 'function') return true;
    return tool.isEnabled(context) === true;
  });
}
