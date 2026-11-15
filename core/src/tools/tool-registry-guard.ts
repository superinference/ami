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

function matchesReservedPrefix(name: string): string | null {
  for (const prefix of RESERVED_TOOL_PREFIXES) {
    const boundary = prefix.endsWith('_') ? prefix : prefix + '_';
    if (name === prefix || name.startsWith(boundary)) return prefix;
  }
  return null;
}

export function detectToolConflict(
  toolName: string,
  source: string,
  existingTools: Map<string, string>,
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
      reason: `Tool "${toolName}" already registered by "${existingSource}"`,
    };
  }

  if (source !== 'core' && source !== 'builtin') {
    const prefix = matchesReservedPrefix(toolName);
    if (prefix) {
      return {
        toolName,
        existingSource: 'core',
        conflictingSource: source,
        reason: `Tool "${toolName}" conflicts with reserved prefix "${prefix}"`,
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

  register(toolName: string, source: string, aliases?: readonly string[]): ToolConflict | null {
    const conflict = detectToolConflict(toolName, source, this._registry);
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

    if (source !== 'core' && source !== 'builtin') {
      const prefix = matchesReservedPrefix(alias);
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
   * tool name, or `undefined` when nothing answers to it. Mirrors Claude Code's
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
export function filterAvailableTools<
  T extends { name: string; isEnabled?: (context?: C) => boolean },
  C = unknown,
>(
  tools: readonly T[],
  context?: C,
  alwaysAvailable?: ReadonlySet<string> | readonly string[],
): T[] {
  const protectedSet =
    alwaysAvailable instanceof Set
      ? alwaysAvailable
      : new Set<string>(alwaysAvailable ?? []);
  return tools.filter((tool) => {
    if (protectedSet.has(tool.name)) return true;
    if (typeof tool.isEnabled !== 'function') return true;
    return tool.isEnabled(context) === true;
  });
}
