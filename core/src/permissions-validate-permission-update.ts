// Split from permissions.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';
import * as os from 'os';
import { expandPath } from './utils/path';
import { detectUnsafeHelpCommand } from './tools/bash-security';
import { PERMISSION_MODES, PERMISSION_UPDATE_DESTINATIONS } from './permissions-permission-mode-aliases';
import type { PermissionMode, PermissionRuleSource, PermissionUpdateDestination } from './permissions-permission-mode-aliases';

/**
 * Structured description of an active permission rule that is effectively
 * equivalent to YOLO / bypass-permissions, for a UI/`/doctor` warning. Mirrors
 * the reference's `DangerousPermissionInfo` (utils/permissions/permissionSetup.ts): a
 * display string for the rule and a human-readable string for where it came from,
 * so the warning can say exactly which rule from which source grants full
 * autonomy. `ruleValue`/`source` are kept for callers that want to remediate
 * (remove/narrow) the rule, not just display it.
 */
export interface DangerousPermissionInfo {
  /** The parsed rule value (`{toolName, ruleContent?}`) for programmatic removal. */
  ruleValue: PermissionRuleValue;
  /** The rule's origin tier (or `'cliArg'` for a `--allow` flag spec). */
  source: PermissionRuleSource;
  /** Display form of the rule, e.g. `bash(*)`. */
  ruleDisplay: string;
  /** Human-readable origin, e.g. `project settings` or `--allow`. */
  sourceDisplay: string;
}

/**
 * Derive a narrow, scoped "allow always" prefix for a bash command, or null if
 * none is safe to suggest (the caller then falls back to the exact command).
 * Prefers the two-word `command subcommand` form; falls back to the first word
 * ONLY when that root is neither subcommand-aware nor a shell/interpreter/
 * wrapper. This is the single canonical prefix-derivation helper — it replaces
 * ad-hoc "first N words" heuristics and enforces the bare-root + NEVER_SUGGEST
 * guards in one place. Output redirections are stripped first so a redirect
 * target filename never becomes the suggested rule prefix.
 */
/**
 * Is `command` a plain `--help` invocation (`python --help`, `git commit --help`)
 * that is read-only and safe to allow AS-IS? Such a command should be allowed by
 * its FULL text rather than collapsed to a broad prefix (`python:*`), which would
 * grant arbitrary code execution through the interpreter. True only when:
 * - the command ends with `--help`;
 * - it carries no quotes (a quote could smuggle an argument past the check);
 * - `--help` is the ONLY flag; and
 * - every non-flag token is a bare alphanumeric identifier (no paths, operators,
 *   substitutions or redirections).
 *
 * `--help` forms that route through an unsafe help backend (`man -l`,
 * `git help --web`, …) are excluded — those are not simple `--help` at all and
 * are caught here via {@link detectUnsafeHelpCommand}. Mirrors the reference's
 * `isHelpCommand` (utils/bash/commands.ts), which bypasses prefix extraction so a
 * help command is allowed in full instead of as a too-broad prefix.
 */
export function isHelpCommand(command: string): boolean {
  const trimmed = command.trim();
  if (!trimmed.endsWith('--help')) return false;
  // Quotes could hide an injected argument — reject outright.
  if (trimmed.includes('"') || trimmed.includes("'")) return false;
  // A help form that reaches an unsafe backend is never a "simple" help command.
  if (detectUnsafeHelpCommand(trimmed)) return false;
  const words = trimmed.split(/\s+/).filter(Boolean);
  let foundHelp = false;
  const alphanumeric = /^[a-zA-Z0-9]+$/;
  for (const word of words) {
    if (word.startsWith('-')) {
      if (word === '--help') foundHelp = true;
      else return false; // any other flag disqualifies it
    } else if (!alphanumeric.test(word)) {
      return false; // paths, operators, substitutions, etc.
    }
  }
  return foundHelp;
}

/**
 * Per-subcommand scoped "allow always" prefixes for a (possibly compound)
 * command. A single command returns `[prefix]` (or `[]` when no safe prefix can
 * be derived). A compound command (`cd src && git status && npm test`) is split
 * into subcommands; each subcommand the caller marks via `excludeSubcommand`
 * (typically read-only / already-auto-allowed commands such as `cd`/`git
 * status`) is DROPPED, and a scoped prefix is derived for each remaining one.
 * Prefixes sharing a root word are collapsed via word-aligned LCP so a chain of
 * `git fetch && git worktree add` yields a single `git` rule rather than two.
 *
 * This is the compound-aware counterpart to {@link suggestAllowPrefix}: callers
 * seeding "allow always" rules for a compound command MUST use this so a rule is
 * scoped to each mutating subcommand instead of the misleading two-word prefix of
 * the whole chain (e.g. `cd src`). Mirrors the reference's
 * `getCompoundCommandPrefixesStatic` (utils/bash/prefix.ts) + per-subcommand
 * rule derivation (BashPermissionRequest `subcommandResults`).
 */
/**
 * Upper bound on how many per-subcommand "allow always" rules a single compound
 * command's permission prompt may suggest. A wide chain (`a && b && c && … && j`)
 * otherwise produces a 10+ rule dump the user has to read through; capping keeps
 * the prompt legible — the user can still allow the rest by re-running or granting
 * a broader rule. Mirrors the reference's `MAX_SUGGESTED_RULES_FOR_COMPOUND` (default 5,
 * bashPermissions.ts). Applied in {@link buildPermissionSuggestions}.
 */
export const MAX_SUGGESTED_RULES_FOR_COMPOUND = 5;

// ---------------------------------------------------------------------------
// Permission-rule string format — `Tool` / `Tool(specifier)`
// ---------------------------------------------------------------------------

/** Parsed form of a canonical permission-rule string. */
export interface PermissionRuleValue {
  toolName: string;
  ruleContent?: string;
}

/**
 * Canonical the reference wire name for the subagent/delegation tool (its current
 * tool name is `Agent`). AMI's own tool is `task`; a permission rule, hook, or
 * imported the reference settings file that names `Agent(...)` must therefore resolve to
 * AMI's `task` tool. Mirrors the reference's `AGENT_TOOL_NAME` (AgentTool/constants.ts).
 */
export const AGENT_TOOL_NAME = 'Agent';

/**
 * Legacy the reference wire name for the same tool, kept so a rule written against
 * the OLD spelling still matches (parity: the reference's `LEGACY_AGENT_TOOL_NAME = 'Task'`,
 * retained "for backward compat (permission rules, hooks, resumed sessions)").
 */
export const LEGACY_AGENT_TOOL_NAME = 'Task';

/** The pattern-syntax class a tool's permission-rule content is written in:
 *  `file` tools take filesystem GLOBS (`*.ts`, `src/**`); `bash` tools take a
 *  command string with free `*` wildcards plus the legacy `command:*` prefix
 *  form. Mirrors the reference's toolValidationConfig split (filePatternTools vs
 *  bashPrefixTools) that drives which pattern syntax is legal per tool. */
export const TOOL_PATTERN_CLASSES = ['file', 'bash'] as const;

export type ToolPatternClass = (typeof TOOL_PATTERN_CLASSES)[number];

/** Canonical-tool-name → pattern-class map (single source of truth for
 *  {@link isFilePatternTool}/{@link isBashPrefixTool}). Keyed by AMI canonical
 *  names; callers pass raw rule tool names through {@link normalizeToolName}
 *  first, so a reference-spelled `Edit`/`Glob` classifies the same as `file_edit`/
 *  `glob`. Tools absent from the map (`web_fetch`, `web_search`, MCP, `grep`,
 *  …) use their own content validators, not glob/bash-wildcard syntax. Exposed
 *  for the permission introspection surface. */
export const TOOL_PATTERN_CLASS: Readonly<Record<string, ToolPatternClass>> = {
  // File-glob tools (parity: Read/Write/Edit/Glob/NotebookRead/NotebookEdit).
  file_read: 'file', file_edit: 'file', glob: 'file', notebook_edit: 'file',
  // Bash wildcard + legacy `:*` prefix tool.
  bash: 'bash',
};

/** True if `str[index]` is escaped (preceded by an odd number of backslashes). */
export function isEscapedAt(str: string, index: number): boolean {
  let backslashCount = 0;
  let j = index - 1;
  while (j >= 0 && str[j] === '\\') { backslashCount++; j--; }
  return backslashCount % 2 !== 0;
}

export function findFirstUnescapedChar(str: string, char: string): number {
  for (let i = 0; i < str.length; i++) {
    if (str[i] === char && !isEscapedAt(str, i)) return i;
  }
  return -1;
}

export function findLastUnescapedChar(str: string, char: string): number {
  for (let i = str.length - 1; i >= 0; i--) {
    if (str[i] === char && !isEscapedAt(str, i)) return i;
  }
  return -1;
}

/** Escape backslashes then parentheses so rule content survives round-tripping. */
export function escapeRuleContent(content: string): string {
  return content.replace(/\\/g, '\\\\').replace(/\(/g, '\\(').replace(/\)/g, '\\)');
}

/** Inverse of {@link escapeRuleContent} (parens first, then backslashes). */
export function unescapeRuleContent(content: string): string {
  return content.replace(/\\\(/g, '(').replace(/\\\)/g, ')').replace(/\\\\/g, '\\');
}

/** Serialize a {@link PermissionRuleValue} back to its canonical string form. */
export function permissionRuleValueToString(ruleValue: PermissionRuleValue): string {
  if (!ruleValue.ruleContent) return ruleValue.toolName;
  return `${ruleValue.toolName}(${escapeRuleContent(ruleValue.ruleContent)})`;
}

/**
 * Parse an `mcp__server[__tool]` permission-rule tool name into its parts, or
 * `null` when the name is not an MCP rule. Mirrors the reference's `mcpInfoFromString`
 * (services/mcp/mcpStringUtils.ts): the first two `__`-delimited segments are
 * the `mcp` marker and the server name; everything after is re-joined so a tool
 * name containing `__` survives (`mcp__srv__a__b` → tool `a__b`). A missing
 * server segment (`mcp__`, `mcp`) is not a valid MCP rule and returns `null`.
 */
export function mcpInfoFromString(
  toolString: string,
): { serverName: string; toolName: string | undefined } | null {
  const parts = toolString.split('__');
  const [mcpPart, serverName, ...toolNameParts] = parts;
  if (mcpPart !== 'mcp' || !serverName) return null;
  return {
    serverName,
    toolName: toolNameParts.length > 0 ? toolNameParts.join('__') : undefined,
  };
}

/** Count unescaped occurrences of `char` in `str` (odd preceding backslash run
 *  = escaped). Shared by the MCP rule validator so `mcp__srv\(x\)` is inspected
 *  as a literal, not a pattern. Mirrors the reference's `countUnescapedChar`. */
export function countUnescapedChar(str: string, char: string): number {
  let count = 0;
  for (let i = 0; i < str.length; i++) {
    if (str[i] === char && !isEscapedAt(str, i)) count++;
  }
  return count;
}

/** The three behaviors a permission rule can carry. Mirrors the reference's
 *  `permissionBehavior` enum; identical to {@link PermissionRule.action}. */
export const PERMISSION_BEHAVIORS = ['allow', 'deny', 'ask'] as const;

export type PermissionBehavior = (typeof PERMISSION_BEHAVIORS)[number];

/**
 * A structured, declarative mutation of a {@link PermissionManager}'s live rule
 * set / mode / workspace boundary — the canonical representation of "what to
 * change" that a permission prompt, slash command, or hook can hand back as a
 * one-click SUGGESTION (or a batch of them). Mirrors the reference's
 * `PermissionUpdate` discriminated union (utils/permissions/PermissionUpdate.ts
 * + PermissionUpdateSchema.ts):
 *  - `addRules`        — append allow/deny/ask rules to a destination tier;
 *  - `replaceRules`    — atomically swap ALL rules of a (behavior, destination);
 *  - `removeRules`     — drop rules (normalized parse→serialize match, so a
 *                         `Bash(*)` suggestion removes a stored tool-wide `Bash`);
 *  - `setMode`         — change the permission mode;
 *  - `addDirectories`/`removeDirectories` — widen/shrink the read-access boundary.
 *
 * `destination` tags which trust tier the change belongs to (the rule `source`,
 * or the directory grant's origin). It is a parity-faithful subset of
 * {@link PERMISSION_UPDATE_DESTINATIONS}. Applying an update is purely in-memory
 * (matching the reference's context transform); persistence is a separate step
 * ({@link PermissionManager.saveRulesToDestination}).
 */
export type PermissionUpdate =
  | { type: 'addRules'; rules: PermissionRuleValue[]; behavior: PermissionBehavior; destination?: PermissionUpdateDestination }
  | { type: 'replaceRules'; rules: PermissionRuleValue[]; behavior: PermissionBehavior; destination?: PermissionUpdateDestination }
  | { type: 'removeRules'; rules: PermissionRuleValue[]; behavior: PermissionBehavior; destination?: PermissionUpdateDestination }
  | { type: 'setMode'; mode: PermissionMode; destination?: PermissionUpdateDestination }
  | { type: 'addDirectories'; directories: string[]; destination?: PermissionUpdateDestination }
  | { type: 'removeDirectories'; directories: string[]; destination?: PermissionUpdateDestination };

/**
 * Validate an UNTRUSTED {@link PermissionUpdate} at a trust boundary before it is
 * applied. Permission updates can arrive from less-trusted surfaces — a delegated
 * permission-prompt tool's JSON reply (an MCP server), a hook process's
 * `updatedPermissions`, or a settings file — where TypeScript's compile-time union
 * does NOT hold at runtime. A malformed mutation (unknown `type`, a missing/invalid
 * `behavior`, a non-array `rules`, a rule whose `ruleContent` is not a string, an
 * empty tool name, a `destination` outside the writable tiers, a bogus `setMode`
 * mode, or a non-string directory) must be REJECTED whole rather than partially
 * applied — a single acceptance boundary mirroring the reference's
 * `PermissionUpdateSchema` Zod `discriminatedUnion` with its enum `behavior` /
 * `destination` and typed rule/directory arrays
 * (references/claude/claude-code-main/src/utils/permissions/PermissionUpdateSchema.ts
 * L27-L77).
 *
 * Returns `{ valid: true }` for a well-formed update, else `{ valid: false, error }`
 * naming the first problem. Pure + synchronous; the enforcement point is
 * {@link PermissionManager.applyUpdate}, which drops an invalid update fail-closed
 * (grants nothing) instead of partially applying it or throwing mid-turn.
 */
export function validatePermissionUpdate(update: unknown): { valid: boolean; error?: string } {
  if (update === null || typeof update !== 'object' || Array.isArray(update)) {
    return { valid: false, error: 'permission update must be an object' };
  }
  const u = update as Record<string, unknown>;
  // `destination` is optional, but when present it is a hard boundary: a rule or
  // directory grant may only be routed to a WRITABLE tier (read-only sources like
  // policySettings/flagSettings/command can never be a persistence target).
  const dest = u.destination;
  if (dest !== undefined &&
      !(typeof dest === 'string' && (PERMISSION_UPDATE_DESTINATIONS as readonly string[]).includes(dest))) {
    return { valid: false, error: `invalid destination: ${JSON.stringify(dest)}` };
  }
  switch (u.type) {
    case 'addRules':
    case 'replaceRules':
    case 'removeRules': {
      if (u.behavior !== 'allow' && u.behavior !== 'deny' && u.behavior !== 'ask') {
        return { valid: false, error: `${u.type} requires behavior "allow" | "deny" | "ask"` };
      }
      if (!Array.isArray(u.rules)) {
        return { valid: false, error: `${u.type}.rules must be an array` };
      }
      for (const rv of u.rules) {
        if (rv === null || typeof rv !== 'object' || Array.isArray(rv)) {
          return { valid: false, error: `${u.type}.rules entries must be objects` };
        }
        const r = rv as Record<string, unknown>;
        if (typeof r.toolName !== 'string' || r.toolName === '') {
          return { valid: false, error: `${u.type}.rules entries require a non-empty string toolName` };
        }
        if (r.ruleContent !== undefined && typeof r.ruleContent !== 'string') {
          return { valid: false, error: `${u.type}.rules ruleContent must be a string when present` };
        }
      }
      return { valid: true };
    }
    case 'setMode': {
      if (typeof u.mode !== 'string' || !(PERMISSION_MODES as readonly string[]).includes(u.mode)) {
        return { valid: false, error: `setMode requires a valid mode (${PERMISSION_MODES.join(', ')})` };
      }
      return { valid: true };
    }
    case 'addDirectories':
    case 'removeDirectories': {
      if (!Array.isArray(u.directories)) {
        return { valid: false, error: `${u.type}.directories must be an array` };
      }
      for (const d of u.directories) {
        if (typeof d !== 'string' || d === '') {
          return { valid: false, error: `${u.type}.directories entries must be non-empty strings` };
        }
      }
      return { valid: true };
    }
    default:
      return { valid: false, error: `unknown permission update type: ${JSON.stringify(u.type)}` };
  }
}

/**
 * The kind of filesystem access a tool call represents — mirrors the reference's
 * `FileOperationType` (components/permissions/FilePermissionDialog/permissionOptions.tsx):
 * a read-only access, an edit of pre-existing content (`write`), or the creation of
 * a brand-new file (`create`). It drives OPERATION-AWARE permission affordances: a
 * `read` widens the read boundary by directory, a `write`/`create` offers a path
 * allow rule, and only a non-`read` op targeting the agent's own config folder gets
 * the "allow editing its own settings this session" grant.
 */
export const FILE_OPERATION_TYPES = ['read', 'write', 'create'] as const;

export type FileOperationType = (typeof FILE_OPERATION_TYPES)[number];

/**
 * Which of the agent's own config folders a path lives in: the PROJECT config
 * folder (`<cwd>/.ami` | `.ami-plugin` | `.claude`) or the GLOBAL one
 * (`~/.ami` | `~/.ami-plugin` | `~/.claude`). Mirrors the reference's `isInClaudeFolder` /
 * `isInGlobalClaudeFolder` scope distinction.
 */
export const CONFIG_FOLDER_SCOPES = ['project-config-folder', 'global-config-folder'] as const;

export type ConfigFolderScope = (typeof CONFIG_FOLDER_SCOPES)[number];

/** Directory names that are a genuine AMI *settings root* the agent may self-grant
 *  (NOT the IDE/git dirs in {@link AGENT_CONFIG_DIR_SEGMENTS}, which are not AMI's
 *  own config to offer a self-edit grant for). */
export const CONFIG_FOLDER_NAMES = ['.ami', '.ami-plugin', '.claude'] as const;

/** True when the already-absolute `child` is `root` itself or nested within it,
 *  case-folded on win32 (case-insensitive FS) like the reference's normalizeCaseForComparison. */
export function pathIsWithinDir(child: string, root: string): boolean {
  const fold = (p: string) => (process.platform === 'win32' ? p.toLowerCase() : p);
  const c = fold(path.normalize(child));
  const r = fold(path.normalize(root));
  return c === r || c.startsWith(r.endsWith(path.sep) ? r : r + path.sep);
}

/**
 * Resolve the config-folder {@link ConfigFolderScope} + matched root dir for
 * `filePath`, or `null` when it is outside every config folder. The PROJECT folder
 * wins when the two overlap (cwd under $HOME). Shared matcher behind
 * {@link resolveConfigFolderScope} and the config-folder permission suggestion — one
 * matching implementation, no per-caller duplication.
 */
export function matchConfigFolderRoot(
  filePath: string,
  cwd: string,
): { scope: ConfigFolderScope; root: string } | null {
  if (!filePath) return null;
  const abs = path.resolve(cwd, expandPath(filePath));
  for (const name of CONFIG_FOLDER_NAMES) {
    const root = path.resolve(cwd, name);
    if (pathIsWithinDir(abs, root)) return { scope: 'project-config-folder', root };
  }
  const home = os.homedir();
  for (const name of CONFIG_FOLDER_NAMES) {
    const root = path.join(home, name);
    if (pathIsWithinDir(abs, root)) return { scope: 'global-config-folder', root };
  }
  return null;
}

/**
 * Resolve whether `filePath` lives inside the agent's own config folder (project or
 * global), returning the {@link ConfigFolderScope} or `null`. Mirrors the reference's
 * `isInClaudeFolder` / `isInGlobalClaudeFolder`; used to offer a config-folder-scoped
 * "allow editing own settings this session" grant at permission-decision time.
 */
export function resolveConfigFolderScope(
  filePath: string,
  cwd: string = process.cwd(),
): ConfigFolderScope | null {
  return matchConfigFolderRoot(filePath, cwd)?.scope ?? null;
}

/** Config folders that host a `skills/` subdir. `.ami-plugin` is excluded — plugin
 *  bundles are not user-authored skills, so they get no skill-scoped grant. */
export const SKILL_CONFIG_FOLDER_NAMES = ['.ami', '.claude'] as const;
