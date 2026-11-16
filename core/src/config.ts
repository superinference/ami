import * as fs from 'fs';
import * as path from 'path';
import { log as coreLog } from './logger';
import { filterSettingsEnv, isHostManagedProvider, type EnvTrustTier, type EnvRejectReason } from './env-protection';
import { validateEngineConfig, type EngineConfig } from './types';
import { withFileLockSync, type FileLockSyncOptions } from './utils/file-lock';
import { levenshteinDistance } from './tools/path-suggest';

/**
 * Project-level configuration that can be stored in
 * `.ami/config.json` (project) or `~/.ami/config.json`
 * (global).  The file format is JSONC — single-line (//) and block comments
 * are stripped before parsing.
 *
 * Priority chain (highest to lowest):
 *   CLI args  >  env vars  >  project config  >  global config
 */
export interface ProjectConfig {
  model?: string;
  provider?: string;
  baseUrl?: string;
  permissionMode?: 'ask' | 'auto-allow' | 'deny-all';
  thinkingLevel?: string;
  maxEffortLevel?: string;
  fallbackModel?: string;
  compactionModel?: string;
  maxTurns?: number;
  tokenBudget?: number;
  /** Hard USD spend cap; session stops when estimated cost exceeds it. */
  maxBudgetUsd?: number;
  /** Opt-in SuperInference reflection gate (both default off). */
  superinference?: { enforcing?: boolean; useLLMCritic?: boolean };
  skill?: string;
  /** Managed policy: when non-empty, only these models are selectable. */
  enforceAvailableModels?: string[];
  /** Cap (chars) on inline bash stdout before it is spilled to a file. */
  bashOutputMaxChars?: number;
  /** Cap (chars) on inline background task output before it is spilled to a file. */
  taskOutputMaxChars?: number;
  /** Anthropic prompt-cache TTL ('5m' default or '1h' extended-cache beta). */
  cacheTtl?: '5m' | '1h';
  /** Safe mode: skip untrusted project-local hooks/MCP/memory/skills. */
  safeMode?: boolean;
  /** Session-wide cap on web_search tool calls. */
  maxWebSearchesPerSession?: number;
  /**
   * Retention window (days) for persisted session transcripts. On startup, sessions
   * whose last on-disk activity is older than this are deleted
   * ({@link SessionManager.cleanupOldSessions}). A missing/0/non-positive value DISABLES
   * cleanup (keep everything). Parity: Claude Code's `cleanupPeriodDays`.
   */
  cleanupPeriodDays?: number;
  /** Defer long-tail tools from the per-turn catalog until tool_search loads them. */
  deferTools?: boolean;
  /** Extra tool names to always keep loaded when deferTools is on. */
  alwaysLoadTools?: string[];
  /** Ordered fallback model chain stepped through on provider errors. */
  fallbackModels?: string[];
  /**
   * Managed killswitch: when true, the bypass/auto-allow ("YOLO") permission modes
   * are forbidden — a requested `--yolo` / `bypassPermissions` / `auto-allow` is
   * force-downgraded to the safe default and the Shift+Tab cycle skips them. A
   * {@link MANAGED_ONLY_KEYS} entry, so only a managed/enterprise config tier can
   * set it; project/local/env/cli can never flip it. Parity: Claude Code's
   * `permissions.disableBypassPermissionsMode === 'disable'`.
   */
  disableBypassPermissionsMode?: boolean;
  /**
   * Managed hook kill-switch: when true, ALL hook execution is disabled across
   * every event (preToolUse/postToolUse/sessionStart/sessionEnd/subagentStart/…
   * including status-line and file-suggestion hooks). A {@link MANAGED_ONLY_KEYS}
   * entry — only a managed/enterprise config tier can set it; project/local/env/cli
   * can never flip it. Parity: Claude Code's `disableAllHooks` policy setting.
   */
  disableAllHooks?: boolean;
  /**
   * Managed hook lockdown: when true, ONLY admin/managed-sourced hooks execute —
   * user, project, and plugin hooks are blocked (managed > non-managed privilege
   * boundary). A {@link MANAGED_ONLY_KEYS} entry — managed tier is the sole source.
   * Parity: Claude Code's `allowManagedHooksOnly` policy setting.
   */
  allowManagedHooksOnly?: boolean;
  /**
   * Managed plugin policy map (Claude parity: `enabledPlugins`). Keyed by plugin id
   * (`name@marketplace` or a bare name for direct installs). A `false` value FORCE-
   * DISABLES that plugin at every scope and chokepoint — install, enable, and load —
   * independent of {@link allowManagedModsOnly}; a `true`/`string[]` value vouches for
   * it under allow-only mode. A {@link MANAGED_ONLY_KEYS} entry — only a managed/
   * enterprise config tier can set it; project/local/env/cli can never flip it.
   */
  enabledPlugins?: Record<string, boolean | string[]>;
  /** Managed allow-only mode for plugins/mods (Claude parity: load only vouched mods). */
  allowManagedModsOnly?: boolean;
  /** MCP allowlist: when non-empty, only these MCP servers may be connected. */
  allowedMcpServers?: string[];
  /**
   * MCP denylist: servers named here (by name, case-insensitive, or normalized
   * upstream URL) are blocked across every scope. The denylist takes PRECEDENCE
   * over {@link allowedMcpServers} — a server on both lists is denied. Default-open:
   * an empty/absent list blocks nothing. A managed-tier value is authoritative (a
   * project/local config can only ADD denies via array accumulation, never remove a
   * managed deny). Parity: Claude Code's enterprise `deniedMcpServers` setting.
   */
  deniedMcpServers?: string[];
  /** Managed MCP server definitions pushed by enterprise policy. */
  managedMcpServers?: Record<string, unknown>;
  /** SL1: shell command whose stdout is rendered as a custom status line. */
  statusLine?: string;
  /** Bash commands exempted from the container sandbox (audit #4: settable + consumed,
   *  now also typed + merged across config layers). */
  sandboxExcludedCommands?: string[];
  /**
   * Environment variables to apply to the process from settings. Applied with
   * trust tiering by {@link applyConfigEnv}: project-scoped sources only apply
   * the SAFE_ENV_VARS allowlist, so a committed project file cannot inject a
   * dangerous var (ANTHROPIC_BASE_URL, LD_PRELOAD, …) into the agent.
   */
  env?: Record<string, string>;
  /** When false, the rotating discoverability tips are suppressed entirely. */
  tipsEnabled?: boolean;
  /** Override the discoverability-tip surface: inject custom tips / drop the built-ins. */
  tipsOverride?: { tips?: string[]; excludeDefault?: boolean };
  /** When false, the learn-once composer hints are suppressed entirely. */
  composerHintsEnabled?: boolean;
  /**
   * Internal, non-user-facing counter stamped by the versioned startup migration-set
   * runner (reference-journal `runStartupMigrations` / `CURRENT_MIGRATION_VERSION`): the
   * ordered one-time migration batch runs only while this stored value lags the current
   * generation, then stamps it, so bumping the code constant re-runs the whole set once.
   * Parity: Claude Code's persisted `migrationVersion`.
   */
  migrationVersion?: number;
  /** Override the composer-hint surface: inject custom hints / drop the built-ins. */
  composerHintsOverride?: { hints?: Array<{ key: string; label: string; learnedAfter: number }>; excludeDefault?: boolean };
  /**
   * Worktree behavior knobs (parity: Claude Code's `settings.worktree`).
   * `symlinkDirectories` is an opt-in list of directories to symlink from the
   * source checkout into a freshly-created worktree (avoids disk bloat for large,
   * reproducible dirs like `node_modules`, `vendor`, `.venv`). When this key is
   * CONFIGURED (including as an empty array → symlink nothing), it is honored
   * exactly; when ABSENT, AMI keeps its default of symlinking `node_modules`.
   * `sparsePaths` is an opt-in cone-mode sparse-checkout allow-list: when a
   * non-empty list is configured, a freshly-created worktree is added with
   * `--no-checkout`, its sparse-checkout cone is set to exactly those paths, and
   * `HEAD` is checked out — so only the listed subtrees materialise on disk (fast
   * worktree creation for huge monorepos). When ABSENT or empty, the full tree is
   * checked out as usual.
   */
  worktree?: { symlinkDirectories?: string[]; sparsePaths?: string[] };
}

// ---------------------------------------------------------------------------
// JSONC helpers
// ---------------------------------------------------------------------------

/**
 * Strip a leading UTF-8 byte-order mark (U+FEFF) so `JSON.parse` does not throw a
 * cryptic "Unexpected token" on a BOM-prefixed file. PowerShell 5.x (Out-File /
 * Set-Content), Notepad and various Windows editors write config/settings JSON
 * WITH a BOM; without this the whole file reads as corrupt. Shared by EVERY path
 * that parses AMI's own JSON config (user, local, global, AND managed) so the
 * robustness is one source. Parity: Claude Code's `stripBOM` leaf.
 */
export function stripBom(input: string): string {
  return input.charCodeAt(0) === 0xfeff ? input.slice(1) : input;
}

/**
 * Strip single-line (//) and block comments from a JSONC string so that
 * it can be parsed by the standard JSON.parse.
 *
 * Uses a context-aware parser to avoid corrupting URLs or other content
 * inside JSON string literals.
 */
export function stripJsoncComments(input: string): string {
  let result = '';
  let i = 0;
  let inString = false;
  let escape = false;
  while (i < input.length) {
    const ch = input[i];
    if (escape) { result += ch; escape = false; i++; continue; }
    if (inString) {
      if (ch === '\\') { escape = true; result += ch; i++; continue; }
      if (ch === '"') { inString = false; }
      result += ch; i++; continue;
    }
    if (ch === '"') { inString = true; result += ch; i++; continue; }
    if (ch === '/' && i + 1 < input.length) {
      if (input[i + 1] === '/') {
        // Skip to end of line
        while (i < input.length && input[i] !== '\n') i++;
        continue;
      }
      if (input[i + 1] === '*') {
        i += 2;
        while (i < input.length - 1 && !(input[i] === '*' && input[i + 1] === '/')) i++;
        i += 2;
        continue;
      }
    }
    result += ch; i++;
  }
  return result;
}

/**
 * Remove a JSON5/JSONC-style TRAILING comma — a `,` that directly precedes a
 * closing `}` or `]` (ignoring intervening whitespace) — so a hand-edited config
 * with `{ "a": 1, }` or `["x", ]` parses instead of throwing a cryptic
 * "Unexpected token" on the dangling comma. String-aware (a comma inside a JSON
 * string literal is never touched), so it composes safely with
 * {@link stripJsoncComments}. Parity: Claude Code's JSONC parser
 * (`safeParseJSONC` / `parseJsonc`) tolerates trailing commas; AMI previously only
 * stripped comments + BOM, so a trailing comma still read the whole file as corrupt.
 */
export function stripJsoncTrailingCommas(input: string): string {
  let result = '';
  let inString = false;
  let escape = false;
  for (let i = 0; i < input.length; i++) {
    const ch = input[i]!;
    if (escape) { result += ch; escape = false; continue; }
    if (inString) {
      if (ch === '\\') { escape = true; result += ch; continue; }
      if (ch === '"') inString = false;
      result += ch; continue;
    }
    if (ch === '"') { inString = true; result += ch; continue; }
    if (ch === ',') {
      let j = i + 1;
      while (j < input.length && /\s/.test(input[j]!)) j++;
      if (j < input.length && (input[j] === '}' || input[j] === ']')) {
        // Drop the trailing comma; the whitespace up to the bracket is re-emitted
        // as those characters are copied on subsequent iterations.
        continue;
      }
    }
    result += ch;
  }
  return result;
}

/** LRU cap (entries) on {@link parseJsonc}'s content-keyed parse memo. Parity: Claude's safeParseJSON LRU(50). */
const JSONC_PARSE_CACHE_MAX_ENTRIES = 50;
/** Only memoize small payloads — a huge config is cheaper to re-parse than to retain + clone. */
const JSONC_PARSE_CACHE_MAX_BYTES = 100_000;
/** prepared-JSONC-string → the parsed value (pristine; callers get a clone). */
const jsoncParseCache = new Map<string, unknown>();

/**
 * Canonical JSONC parse used by EVERY AMI config loader: strip a leading BOM, strip
 * `//` + block comments, tolerate a trailing comma, then `JSON.parse`. The parse is
 * MEMOIZED by the prepared content (bounded LRU, small inputs only) so re-reading an
 * unchanged file across the project/local/global waterfall (and repeated `/doctor`
 * diagnostic sweeps) parses it once instead of on every pass. The cache always holds
 * the pristine parsed tree and every caller receives a `structuredClone`, so a caller
 * mutating its config (e.g. {@link salvageManagedConfig}) can never corrupt a shared
 * cached value. Routes through {@link jsonHooks.parse} so the test parse-seam still
 * applies. Parity: Claude Code's `parseJsonc` + `safeParseJSON` LRU memoization.
 */
export function parseJsonc(raw: string): unknown {
  const prepared = stripJsoncTrailingCommas(stripJsoncComments(stripBom(raw)));
  if (prepared.length > JSONC_PARSE_CACHE_MAX_BYTES) {
    return jsonHooks.parse(prepared);
  }
  if (jsoncParseCache.has(prepared)) {
    const cached = jsoncParseCache.get(prepared);
    // LRU bump: re-insert so the most-recently-used key is evicted last.
    jsoncParseCache.delete(prepared);
    jsoncParseCache.set(prepared, cached);
    return structuredClone(cached);
  }
  const parsed = jsonHooks.parse(prepared);
  jsoncParseCache.set(prepared, parsed);
  if (jsoncParseCache.size > JSONC_PARSE_CACHE_MAX_ENTRIES) {
    const oldest = jsoncParseCache.keys().next().value as string;
    jsoncParseCache.delete(oldest);
  }
  return structuredClone(parsed);
}

// ---------------------------------------------------------------------------
// Loaders
// ---------------------------------------------------------------------------

/**
 * Load a project-level config from `<cwd>/.ami/config.json`.
 * Returns `null` when the file does not exist.
 */
export function loadProjectConfig(cwd: string): ProjectConfig | null {
  const configPath = path.join(cwd, '.ami', 'config.json');
  return loadConfigFile(configPath);
}

/**
 * Load a local config from `<cwd>/.ami/config.local.json`.
 * This file is typically gitignored for user-specific overrides.
 */
export function loadLocalConfig(cwd: string): ProjectConfig | null {
  return loadConfigFile(path.join(cwd, '.ami', 'config.local.json'));
}

/**
 * Load the global config from `~/.ami/config.json`.
 * Returns `null` when the file does not exist.
 */
export function loadGlobalConfig(): ProjectConfig | null {
  const configPath = getGlobalConfigPath();
  if (!configPath) return null;
  return loadConfigFile(configPath);
}

/**
 * Resolve the absolute path to the global config file (`~/.ami/config.json`),
 * or `null` when no home directory can be determined. Single source of truth so
 * the config tool's `scope:'global'` writes land in the same file the config
 * waterfall reads via {@link loadGlobalConfig}.
 */
export function getGlobalConfigPath(): string | null {
  const home = process.env.HOME || process.env.USERPROFILE || '';
  if (!home) return null;
  return path.join(home, '.ami', 'config.json');
}

/**
 * Load a managed/system-wide config from `/etc/superinference/config.json`.
 * Used by organizations to set defaults across all users on a machine.
 */
/** Thrown when a present managed-config file fails to parse (fail-closed). */
export class ManagedConfigError extends Error {}

/** The managed-config base file path (env override or the /etc default). */
export function getManagedConfigPath(): string {
  return process.env.SUPERINFERENCE_MANAGED_CONFIG || '/etc/superinference/config.json';
}

/**
 * The managed drop-in directory (`<managedBase>/config.d`). Admins can layer
 * additional managed policy as individual `*.json` files here instead of editing
 * one monolithic file (parity: Claude's managed-settings.d/ drop-ins).
 */
export function getManagedDropInDir(): string {
  return path.join(path.dirname(getManagedConfigPath()), 'config.d');
}

/**
 * True when a POSIX file mode grants WRITE to OTHER (world — the `0o002` bit). A
 * managed-policy file ANY user on the machine can rewrite is not a trust anchor —
 * the point of managed/enterprise policy is that only an administrator can change
 * it. World-writable is the unambiguous "not a trust anchor" signal (group-writable
 * is intentionally NOT flagged: the common umask 002 creates 0664 files and the
 * group may legitimately be an admin group). Pure + exported for direct unit coverage.
 */
export function isWorldWritable(mode: number): boolean {
  return (mode & 0o002) !== 0;
}

/**
 * Verify that a managed-policy file is a trustworthy source before it is read, and
 * throw {@link ManagedConfigError} (fail-closed) otherwise. This is AMI's analog of
 * Claude Code sourcing managed policy only from OS-protected, admin-only locations
 * (registry HKLM / /Library/Managed Preferences) that an end user cannot redirect:
 *
 *  - a SYMLINK is refused outright — otherwise an unprivileged user could redirect
 *    the authoritative policy path at a file they control;
 *  - on POSIX, a WORLD-WRITABLE regular file is refused — a policy file any user on
 *    the machine can rewrite is not a trust anchor.
 *
 * Windows (no `process.getuid`) carries no meaningful POSIX mode bits, so only the
 * symlink check applies there.
 */
function assertTrustedManagedFile(configPath: string): void {
  let stats: fs.Stats;
  try {
    stats = fsHooks.lstatSync(configPath);
  } catch (err) {
    throw new ManagedConfigError(`Managed config at ${configPath} could not be inspected: ${err instanceof Error ? err.message : String(err)}`);
  }
  if (stats.isSymbolicLink()) {
    throw new ManagedConfigError(`Managed config at ${configPath} is a symlink; refusing to follow a redirected managed-policy source.`);
  }
  // POSIX mode bits are only meaningful where the platform models uid/gid.
  if (typeof process.getuid === 'function' && isWorldWritable(stats.mode)) {
    throw new ManagedConfigError(`Managed config at ${configPath} is world-writable; refusing an untrusted managed-policy source (chmod o-w it).`);
  }
}

/**
 * Read + JSON-parse a managed file, failing closed on an untrusted source, an
 * unreadable file, a JSON syntax error, or a non-object payload. A present-but-
 * valid file is value-SALVAGED ({@link salvageManagedConfig}): each individual
 * field whose value is invalid is dropped (logged) while the valid remainder is
 * kept, so one bad enum/typed field never rejects the whole enterprise policy.
 */
function parseManagedFile(configPath: string): ProjectConfig {
  // Tamper-resistance: refuse a symlinked or group/world-writable managed-policy
  // file before trusting its contents (fail-closed on an untrusted source).
  assertTrustedManagedFile(configPath);
  let raw: string;
  try {
    raw = fsHooks.readFileSync(configPath, 'utf-8') as string;
  } catch (err) {
    // An unreadable present file (EACCES/EIO) must fail closed, not be ignored.
    throw new ManagedConfigError(`Managed config at ${configPath} could not be read: ${err instanceof Error ? err.message : String(err)}`);
  }
  let parsed: unknown;
  try {
    parsed = parseJsonc(raw);
  } catch (err) {
    // A broken enterprise policy must fail closed, with the SAME actionable JSON
    // syntax-recovery hint AMI surfaces for user/project config (one shared source)
    // instead of a bare parser message. Parity: Claude's root-level JSON syntax tip.
    throw new ManagedConfigError(`Managed config at ${configPath} failed to parse: ${err instanceof Error ? err.message : String(err)}. ${JSON_SYNTAX_RECOVERY_HINT}`);
  }
  // Managed policy must be a JSON object — an array/primitive at the root is a
  // malformed policy, not an empty one; fail closed rather than cast it blindly.
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new ManagedConfigError(`Managed config at ${configPath} is not a JSON object (got ${describeJsonType(parsed)}).`);
  }
  // Per-field salvage: drop invalid individual values, keep the valid remainder.
  return salvageManagedConfig(parsed as ProjectConfig, configPath);
}

/** Merge one managed drop-in over an accumulated base (later wins; env key-merges). */
function mergeManagedLayer(base: ProjectConfig, next: ProjectConfig): ProjectConfig {
  const merged: ProjectConfig = { ...base, ...next };
  if (base.env || next.env) merged.env = { ...base.env, ...next.env };
  return merged;
}

export function loadManagedConfig(): ProjectConfig | null {
  const configPath = getManagedConfigPath();
  let merged: ProjectConfig | null = fsHooks.existsSync(configPath) ? parseManagedFile(configPath) : null;

  // Layer `<managedBase>/config.d/*.json` drop-ins alphabetically (last file
  // wins) over the base. A broken drop-in fails closed, like the base file.
  const dropInDir = getManagedDropInDir();
  let entries: string[] = [];
  try {
    if (fsHooks.existsSync(dropInDir)) {
      entries = (fsHooks.readdirSync(dropInDir) as string[]).filter(e => e.endsWith('.json')).sort();
    }
  } catch {
    entries = [];
  }
  for (const name of entries) {
    const dropIn = parseManagedFile(path.join(dropInDir, name));
    merged = merged === null ? dropIn : mergeManagedLayer(merged, dropIn);
  }
  // Success breadcrumb: unlike the per-tier loadConfigFile ("loaded <path>"),
  // loadManagedConfig was previously silent on success, so an operator had no way to
  // confirm an enterprise policy was actually picked up or which keys it contributed.
  // Log the path + contributing key count (no values — the policy may be sensitive)
  // so a managed deployment is diagnosable. Parity: Claude's "MDM settings found: …".
  if (merged !== null) {
    coreLog('config', `loaded managed config ${configPath} (${Object.keys(merged).length} key(s)${entries.length > 0 ? `, ${entries.length} drop-in(s)` : ''})`);
  }
  return merged;
}

/** A single field-level value-validation problem in an otherwise-parseable config. */
export interface ConfigValueIssue {
  /** The offending config key (e.g. "permissionMode", "maxTurns"). */
  field: string;
  /** Human-readable reason the value is invalid. */
  message: string;
  /** The offending value, so a caller can surface "you supplied X" to the user
   *  (parity: Claude's ValidationError.invalidValue). */
  value?: unknown;
  /**
   * A short, actionable fix suggestion for THIS field+kind (parity: Claude's
   * `getValidationTip`). Tells the user HOW to correct the value (e.g. "Use true
   * or false without quotes", "Valid values: ..."), not just what is wrong. Absent
   * when no specific tip applies. Surfaced alongside {@link message} by the loader
   * diagnostic + `/doctor`. */
  tip?: string;
}

/**
 * Describe the JSON runtime kind of a value for a diagnostic message, distinguishing
 * `null` and `array` from a plain `object` (mirroring JSON's own value types). Shared
 * by the "not an object" and per-field type diagnostics so the wording stays one source.
 */
function describeJsonType(value: unknown): string {
  return value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value;
}

/**
 * Recovery guidance appended to every JSON syntax-error message AMI surfaces for a
 * config file, so the user is told HOW to fix a malformed file rather than just that
 * it is malformed. One source, reused by {@link loadConfigFileChecked} (the loader
 * diagnostic) and {@link validateConfigJson} (the stale-tmp / config-tool writer path)
 * so the wording never drifts. Parity: Claude Code's root-level JSON syntax-error tip
 * ("Check for missing commas, unmatched brackets, or trailing commas…").
 */
export const JSON_SYNTAX_RECOVERY_HINT =
  'Check for missing commas, unmatched brackets, or trailing commas. Use a JSON validator to identify the exact syntax error.';

/**
 * The config LAYER a value/diagnostic originates from, in merge-priority order
 * (`cli` highest, `managed` lowest). Mirrors the {@link mergeConfigs} layer stack and
 * the {@link CONFIG_ENV_TIERS} env-trust tiers, so every provenance-aware surface names
 * the same set of sources. Parity: Claude Code's `SettingSource` enum.
 */
export type ConfigSource = 'cli' | 'env' | 'local' | 'project' | 'global' | 'managed';

/**
 * Human-readable provenance name for each {@link ConfigSource}, so a diagnostic / `/doctor`
 * line reads "Invalid project config …" or "Invalid managed config …" instead of leaving
 * the user to infer which file a bare path belongs to (`.ami/config.local.json` vs
 * `~/.ami/config.json` vs `/etc/...`). An introspection surface over the config waterfall.
 * Parity: Claude Code's `getSettingSourceName` (e.g. `localSettings` → "project, gitignored").
 */
export const CONFIG_SOURCE_NAMES: Readonly<Record<ConfigSource, string>> = {
  cli: 'command-line flag',
  env: 'environment variable',
  local: 'project, gitignored',
  project: 'project',
  global: 'user',
  managed: 'managed',
};

/** Resolve the human-readable provenance name for a {@link ConfigSource}. */
export function getConfigSourceName(source: ConfigSource): string {
  return CONFIG_SOURCE_NAMES[source];
}

/** A problem found while loading a config file (distinct from "file absent"). */
export interface ConfigDiagnostic {
  path: string;
  kind: 'parse-error' | 'not-object' | 'invalid-value' | 'unknown-key' | 'unreadable';
  message: string;
  /** Which config layer this file belongs to (merge-priority order). */
  source?: ConfigSource;
  /** Human-readable provenance name for {@link source} ({@link getConfigSourceName}). */
  sourceName?: string;
  /** Where the corrupt file was backed up, or the most recent known-good backup. */
  backupPath?: string;
  /** For `invalid-value`: the per-field issues (grouped by this file). */
  issues?: ConfigValueIssue[];
  /** For `unknown-key`: the unrecognized top-level keys (likely typos). */
  unknownKeys?: string[];
}

/** The ProjectConfig enum fields and their allowed values. */
const PROJECT_CONFIG_ENUMS: ReadonlyArray<readonly [keyof ProjectConfig, readonly string[]]> = [
  ['permissionMode', ['ask', 'auto-allow', 'deny-all']],
  ['cacheTtl', ['5m', '1h']],
];

/** The expected runtime base-type of each typed ProjectConfig field. */
type ConfigFieldType = 'string' | 'number' | 'boolean' | 'string[]' | 'object';

/**
 * Expected runtime base-type of each typed ProjectConfig field. Enum fields live in
 * {@link PROJECT_CONFIG_ENUMS}; numeric RANGE is delegated to {@link validateEngineConfig}.
 * This table only guards the base runtime type so an untrusted JSON value of the wrong
 * type (a string where a number/boolean/array is expected) is surfaced instead of
 * silently flowing into runtime behavior — e.g. a string `maxBudgetUsd`, whose
 * `"ten" <= 0` comparison is `false`, would otherwise slip past the spend-cap range
 * check. Parity: Claude's per-field settings-schema validation.
 */
const PROJECT_CONFIG_FIELD_TYPES: ReadonlyArray<readonly [keyof ProjectConfig, ConfigFieldType]> = [
  ['model', 'string'], ['provider', 'string'], ['baseUrl', 'string'],
  ['thinkingLevel', 'string'], ['maxEffortLevel', 'string'], ['fallbackModel', 'string'],
  ['compactionModel', 'string'], ['skill', 'string'], ['statusLine', 'string'],
  ['maxTurns', 'number'], ['tokenBudget', 'number'], ['maxBudgetUsd', 'number'],
  ['bashOutputMaxChars', 'number'], ['taskOutputMaxChars', 'number'],
  ['maxWebSearchesPerSession', 'number'], ['migrationVersion', 'number'],
  ['cleanupPeriodDays', 'number'],
  ['safeMode', 'boolean'], ['deferTools', 'boolean'], ['tipsEnabled', 'boolean'],
  ['composerHintsEnabled', 'boolean'], ['disableBypassPermissionsMode', 'boolean'],
  ['disableAllHooks', 'boolean'], ['allowManagedHooksOnly', 'boolean'],
  ['allowManagedModsOnly', 'boolean'],
  ['enforceAvailableModels', 'string[]'], ['allowedMcpServers', 'string[]'],
  ['deniedMcpServers', 'string[]'],
  ['fallbackModels', 'string[]'], ['alwaysLoadTools', 'string[]'],
  ['sandboxExcludedCommands', 'string[]'],
  ['superinference', 'object'], ['managedMcpServers', 'object'],
  ['enabledPlugins', 'object'], ['env', 'object'],
  ['tipsOverride', 'object'], ['composerHintsOverride', 'object'],
  ['worktree', 'object'],
];

/**
 * The complete set of recognized top-level `ProjectConfig` keys, derived from the two
 * validation tables above (their union is exactly the `ProjectConfig` interface, so this
 * self-updates whenever a new field is added for validation — no hand-maintained list to
 * drift). Used by {@link findUnknownConfigKeys} to surface typos. Parity: Claude Code
 * warns on unrecognized settings keys rather than silently ignoring them.
 */
export const KNOWN_CONFIG_KEYS: ReadonlySet<string> = new Set<string>([
  ...PROJECT_CONFIG_FIELD_TYPES.map(([k]) => k as string),
  ...PROJECT_CONFIG_ENUMS.map(([k]) => k as string),
]);

/**
 * Return the top-level keys of `config` that are NOT recognized {@link KNOWN_CONFIG_KEYS}.
 * A non-empty result signals a likely typo (e.g. `"modle"` for `"model"`) or a stale key;
 * the loader surfaces these as an advisory `unknown-key` diagnostic (the config still
 * loads — unknown keys are ignored, never fatal). Parity: Claude Code surfaces
 * unrecognized settings so misspellings don't silently take no effect.
 */
export function findUnknownConfigKeys(config: ProjectConfig): string[] {
  return Object.keys(config).filter(k => !KNOWN_CONFIG_KEYS.has(k));
}

/**
 * Suggest the nearest recognized {@link KNOWN_CONFIG_KEYS} entry for a likely-typo
 * `key`, or `undefined` when nothing is close enough. Reuses the ONE canonical
 * edit-distance primitive ({@link levenshteinDistance}) — no parallel fuzzy matcher.
 * An exact known key gets no suggestion. Otherwise a suggestion is offered only for
 * a genuine near-miss: a case-insensitive edit distance within `min(3, max(1,
 * ceil(len/3)))` — tolerant enough for a transposition (`"modle"`→`"model"`, dist 2)
 * or a wrong case (`"StatusLine"`→`"statusLine"`, dist 0) yet capped at 3 so an
 * unrelated long key never matches. Parity: Claude Code's `ValidationErrorsList`
 * "did you mean" hint for an unrecognized settings key — AMI previously surfaced
 * this only for slash-command / `/config` key names, never for config-file keys.
 */
export function suggestKnownConfigKey(key: string): string | undefined {
  if (KNOWN_CONFIG_KEYS.has(key)) return undefined;
  const lower = key.toLowerCase();
  let best: string | undefined;
  let bestDist = Infinity;
  for (const known of KNOWN_CONFIG_KEYS) {
    const d = levenshteinDistance(lower, known.toLowerCase());
    if (d < bestDist) { bestDist = d; best = known; }
  }
  const threshold = Math.min(3, Math.max(1, Math.ceil(key.length / 3)));
  return best !== undefined && bestDist <= threshold ? best : undefined;
}

/**
 * True when `value` matches the expected {@link ConfigFieldType}. Numbers must be
 * finite (an out-of-range literal is not a usable knob); arrays must be homogeneously
 * `string[]`; objects exclude `null` and arrays.
 */
function matchesConfigFieldType(value: unknown, type: ConfigFieldType): boolean {
  switch (type) {
    case 'string': return typeof value === 'string';
    case 'number': return typeof value === 'number' && Number.isFinite(value);
    case 'boolean': return typeof value === 'boolean';
    case 'string[]': return Array.isArray(value) && value.every(v => typeof v === 'string');
    case 'object': return value !== null && typeof value === 'object' && !Array.isArray(value);
  }
}

/** The kind of validation failure a tip is being generated for. */
export type ConfigTipKind =
  | { kind: 'type'; type: ConfigFieldType }
  | { kind: 'enum'; allowed: readonly string[] }
  | { kind: 'range' };

/**
 * Map a per-field validation failure to a short, actionable fix suggestion
 * (parity: Claude Code's `getValidationTip`). Returns `undefined` when no specific
 * tip applies (the generic `message` already suffices). Keeping this as ONE table
 * (rather than inline strings at each push site) means the loader diagnostic,
 * `/doctor`, and any future renderer all surface identical guidance.
 */
export function getConfigValidationTip(field: string, ctx: ConfigTipKind): string | undefined {
  // Field-specific overrides first (most actionable), then generic per-kind tips.
  if (field.startsWith('env.') || field === 'env') {
    return 'Environment variables must be strings — wrap numbers and booleans in quotes, e.g. "DEBUG": "true".';
  }
  switch (ctx.kind) {
    case 'enum':
      return `Valid values: ${ctx.allowed.map(v => `"${v}"`).join(', ')}.`;
    case 'range':
      return 'Must be a positive, finite number.';
    case 'type':
      switch (ctx.type) {
        case 'boolean': return 'Use true or false without quotes.';
        case 'number': return 'Provide a number without quotes, e.g. 10.';
        case 'string[]': return 'Must be an array of strings, e.g. ["a", "b"].';
        case 'object': return 'Must be a JSON object, e.g. { "key": "value" }.';
        case 'string': return 'Must be a quoted string, e.g. "value".';
      }
  }
}

/**
 * Validate the VALUES of a parsed config (the file parsed as a JSON object, but a
 * field may still hold the wrong type, an out-of-range number, or an invalid enum
 * string — the TypeScript type gives no runtime guarantee). Returns a field-level
 * issue list (each carrying the offending `value`) so the loader can surface "your
 * config.json has a bad value" instead of silently accepting it (parity: Claude
 * surfaces per-field settings-validation errors).
 *
 * Numeric RANGE reuses the canonical {@link validateEngineConfig} (no duplicate range
 * logic); this adds the ProjectConfig-specific base-type and enum checks.
 */
export function validateProjectConfigValues(config: ProjectConfig): ConfigValueIssue[] {
  const issues: ConfigValueIssue[] = [];
  // 1. Base runtime-type guard. Untrusted JSON gives no runtime guarantee for a field
  //    the type calls a number/boolean/array; surface a wrong base type rather than
  //    letting it silently flow past the enum/range checks below.
  const typeInvalid = new Set<string>();
  for (const [field, type] of PROJECT_CONFIG_FIELD_TYPES) {
    const value = config[field];
    if (value === undefined) continue;
    if (!matchesConfigFieldType(value, type)) {
      issues.push({ field: field as string, message: `must be a ${type} (got ${describeJsonType(value)})`, value, tip: getConfigValidationTip(field as string, { kind: 'type', type }) });
      typeInvalid.add(field as string);
    }
  }
  // 2. Enum fields (permissionMode/cacheTtl are not in the type table above, so a
  //    non-string enum value is reported here as an out-of-set value).
  for (const [field, allowed] of PROJECT_CONFIG_ENUMS) {
    const value = config[field];
    if (value !== undefined && !allowed.includes(value as string)) {
      issues.push({ field: field as string, message: `must be one of ${allowed.join(', ')} (got ${JSON.stringify(value)})`, value, tip: getConfigValidationTip(field as string, { kind: 'enum', allowed }) });
    }
  }
  // 3. Numeric RANGE — delegate to the single canonical EngineConfig validator, one
  //    field at a time so each invalid value is attributed to its own field. Skip a
  //    field already flagged in step 1 so a non-number can't reach the range logic.
  for (const field of ['maxTurns', 'tokenBudget', 'maxBudgetUsd'] as const) {
    if (config[field] === undefined || typeInvalid.has(field)) continue;
    const r = validateEngineConfig({ [field]: config[field] } as Partial<EngineConfig>);
    if (!r.valid) issues.push({ field, message: r.warnings.join('; '), value: config[field], tip: getConfigValidationTip(field, { kind: 'range' }) });
  }
  // 4. Nested `superinference` flags must be booleans when present. Step 1 only guards
  //    that `superinference` is an object; a non-boolean nested flag (e.g.
  //    `{ enforcing: "true" }`) would otherwise flow TRUTHILY into the reflection gate
  //    and silently force-enable it. Skip when the outer object type was already flagged.
  //    Parity: Claude validates nested settings-schema value types, not just the top level.
  if (config.superinference !== undefined && !typeInvalid.has('superinference')) {
    const si = config.superinference as Record<string, unknown>;
    for (const flag of ['enforcing', 'useLLMCritic'] as const) {
      if (si[flag] !== undefined && typeof si[flag] !== 'boolean') {
        issues.push({ field: `superinference.${flag}`, message: `must be a boolean (got ${describeJsonType(si[flag])})`, value: si[flag], tip: getConfigValidationTip(`superinference.${flag}`, { kind: 'type', type: 'boolean' }) });
      }
    }
  }
  // 5. Nested `worktree.symlinkDirectories` must be a string[] when present. Step 1
  //    only guards that `worktree` is an object; a non-array (or non-string element)
  //    value would otherwise flow into the worktree symlink step and either no-op or
  //    throw at symlink time. Skip when the outer object type was already flagged.
  if (config.worktree !== undefined && !typeInvalid.has('worktree')) {
    const wt = config.worktree as Record<string, unknown>;
    if (wt.symlinkDirectories !== undefined && !matchesConfigFieldType(wt.symlinkDirectories, 'string[]')) {
      issues.push({ field: 'worktree.symlinkDirectories', message: `must be a string[] (got ${describeJsonType(wt.symlinkDirectories)})`, value: wt.symlinkDirectories, tip: getConfigValidationTip('worktree.symlinkDirectories', { kind: 'type', type: 'string[]' }) });
    }
    if (wt.sparsePaths !== undefined && !matchesConfigFieldType(wt.sparsePaths, 'string[]')) {
      issues.push({ field: 'worktree.sparsePaths', message: `must be a string[] (got ${describeJsonType(wt.sparsePaths)})`, value: wt.sparsePaths, tip: getConfigValidationTip('worktree.sparsePaths', { kind: 'type', type: 'string[]' }) });
    }
  }
  return issues;
}

/**
 * Validate the VALUES of a managed/enterprise config and SALVAGE it: drop each
 * field whose value fails {@link validateProjectConfigValues} (logging a per-field,
 * source-tagged warning) while keeping the valid remainder, instead of either
 * trusting a bad value or rejecting the entire managed policy. A nested flag issue
 * (e.g. `superinference.enforcing`) drops only that flag, not the whole object.
 * Unknown/future top-level keys are left untouched (ignored on merge) so a newer
 * client's policy key never degrades the file — forwards-compatible fail-safe
 * degradation: an invalid single field degrades to "not applied" (less locked),
 * never nulls the whole file.
 *
 * Parity: Claude's `parseCommandOutputAsSettings` — `filterInvalidPermissionRules`
 * keeps the valid remainder of a managed policy, per-field `.catch(undefined)`
 * degrades one bad value to undefined, and `formatZodError` tags each message with
 * its source path. Reuses the ONE canonical value validator (no parallel schema).
 */
export function salvageManagedConfig(config: ProjectConfig, sourcePath: string): ProjectConfig {
  const issues = validateProjectConfigValues(config);
  if (issues.length === 0) return config;
  const salvaged: ProjectConfig = { ...config };
  for (const issue of issues) {
    const dot = issue.field.indexOf('.');
    if (dot > 0) {
      const top = issue.field.slice(0, dot) as keyof ProjectConfig;
      const nested = issue.field.slice(dot + 1);
      const obj = salvaged[top];
      if (obj !== null && typeof obj === 'object' && !Array.isArray(obj)) {
        // Clone the nested object before mutating so the caller's parsed value is
        // not aliased, then drop just the invalid nested flag.
        const copy = { ...(obj as Record<string, unknown>) };
        delete copy[nested];
        (salvaged as Record<string, unknown>)[top as string] = copy;
      }
    } else {
      delete salvaged[issue.field as keyof ProjectConfig];
    }
    coreLog('config', `managed config at ${sourcePath}: dropped invalid ${issue.field} (${issue.message})`);
  }
  return salvaged;
}

/**
 * Back up a corrupt config file so the user's content is never silently lost.
 * Returns the backup path, or null if the backup itself failed.
 */
export function backupCorruptConfig(configPath: string, content: string): string | null {
  try {
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const backupPath = `${configPath}.corrupt-${stamp}`;
    fsHooks.writeFileSync(backupPath, content, 'utf-8');
    return backupPath;
  } catch {
    return null;
  }
}

/**
 * Find the most recent `<config>.corrupt-*` backup next to `configPath`, or null.
 * Timestamps are ISO-8601 (`-`-escaped), so lexical sort is chronological.
 */
export function findMostRecentBackup(configPath: string): string | null {
  try {
    const dir = path.dirname(configPath);
    const base = path.basename(configPath);
    const backups = (fsHooks.readdirSync(dir) as string[])
      .filter(e => e.startsWith(`${base}.corrupt-`))
      .sort();
    if (backups.length === 0) return null;
    return path.join(dir, backups[backups.length - 1]!);
  } catch {
    return null;
  }
}

/**
 * True for a Windows UNC path (`\\server\share` or `//server/share`). Reading such a
 * path triggers a network (DNS/SMB) round-trip during what should be a cheap local
 * config probe — a hang/SSRF-adjacent hazard for an attacker-influenced path. A config
 * file is always expected on the local filesystem, so a UNC source is treated as
 * "no config here". Parity: Claude Code's `safeResolvePath` blocks UNC before any I/O.
 */
export function isUncConfigPath(configPath: string): boolean {
  return configPath.startsWith('//') || configPath.startsWith('\\\\');
}

/**
 * True when `stats` describes a non-regular SPECIAL file — a FIFO/named pipe, a
 * socket, or a character/block device. `readFileSync` on a FIFO BLOCKS indefinitely
 * waiting for a writer (a trivial DoS if a config path can be pointed at one), and a
 * device/socket is never a valid config file. The loader skips such a source as
 * "absent" rather than hanging on it. Parity: Claude Code's `safeResolvePath`
 * special-file guard before `realpathSync`.
 */
export function isSpecialFileStats(stats: fs.Stats): boolean {
  return stats.isFIFO() || stats.isSocket() || stats.isCharacterDevice() || stats.isBlockDevice();
}

/**
 * Load a config file, distinguishing three outcomes:
 *  - file absent / unreadable → `{ config: null }` (no diagnostic);
 *  - present but corrupt (bad JSON / not an object) → `{ config: null, diagnostic }`,
 *    and the corrupt file is backed up so it is not lost;
 *  - valid → `{ config }`.
 */
export function loadConfigFileChecked(configPath: string): { config: ProjectConfig | null; diagnostic?: ConfigDiagnostic } {
  let raw: string;
  // Path-safety guard (parity: Claude's safeResolvePath): refuse a UNC path before any
  // filesystem access (it would force a network round-trip), and skip a special file
  // (FIFO/socket/device) whose `readFileSync` could BLOCK forever on a FIFO. Both are
  // treated as "no config here" — a config is always a regular local file.
  if (isUncConfigPath(configPath)) return { config: null };
  try {
    const st = fsHooks.statSync(configPath);
    if (isSpecialFileStats(st)) return { config: null };
  } catch {
    // Absent / broken symlink / not inspectable — fall through to the normal
    // existsSync+read path below, which resolves it to a plain absent/unreadable result.
  }
  try {
    if (!fsHooks.existsSync(configPath)) return { config: null };
    raw = fsHooks.readFileSync(configPath, 'utf-8') as string;
  } catch (err) {
    // ENOENT (missing file / broken symlink) and ENOTDIR (a path component is not a
    // directory) genuinely mean "no config here" — expected, not a problem, so they
    // resolve to a plain absent result with no diagnostic. Any OTHER read error
    // (EACCES permissions, EIO) means the file is PRESENT but could not be read;
    // surfacing that as an `unreadable` diagnostic stops a real-but-unreadable config
    // from being silently mistaken for "no config" (which would let a lower/empty
    // layer quietly take effect). Parity: Claude's handleFileSystemError — ENOENT is
    // a debug-level non-event, every other code is a tracked error.
    const code = err && typeof err === 'object' && 'code' in err ? String((err as NodeJS.ErrnoException).code) : '';
    if (code === 'ENOENT' || code === 'ENOTDIR') return { config: null };
    const message = err instanceof Error ? err.message : String(err);
    coreLog('config', `unreadable config at ${configPath}: ${message}`);
    return { config: null, diagnostic: { path: configPath, kind: 'unreadable', message } };
  }
  // Parse via the canonical JSONC pipeline (BOM strip + comment strip + trailing-comma
  // tolerance + memoized JSON.parse). The ORIGINAL bytes are retained in `raw` so a
  // corrupt-file backup preserves exactly what was on disk.
  let parsed: unknown;
  try {
    parsed = parseJsonc(raw);
  } catch (err) {
    const backupPath = backupCorruptConfig(configPath, raw) ?? undefined;
    // Append actionable recovery guidance so the user learns HOW to fix the malformed
    // file, not merely that the parse failed (parity: Claude's JSON syntax-error tip).
    const message = `${err instanceof Error ? err.message : String(err)}. ${JSON_SYNTAX_RECOVERY_HINT}`;
    coreLog('config', `corrupt config at ${configPath}: ${message}${backupPath ? ` (backed up to ${backupPath})` : ''}`);
    return { config: null, diagnostic: { path: configPath, kind: 'parse-error', message, backupPath } };
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    const message = `Expected a JSON object, got ${describeJsonType(parsed)}`;
    coreLog('config', `invalid config at ${configPath}: ${message}`);
    return { config: null, diagnostic: { path: configPath, kind: 'not-object', message } };
  }
  const config = parsed as ProjectConfig;
  // The file is a valid JSON object, but individual VALUES may still be invalid
  // (bad enum, out-of-range number). Surface those per-field rather than silently
  // accepting them — the valid parts still load (validation is advisory).
  const issues = validateProjectConfigValues(config);
  if (issues.length > 0) {
    const message = `${issues.length} invalid config value(s): ${issues.map(i => `${i.field}: ${i.message}${i.tip ? ` (${i.tip})` : ''}`).join('; ')}`;
    coreLog('config', `invalid value(s) in ${configPath}: ${message}`);
    return { config, diagnostic: { path: configPath, kind: 'invalid-value', message, issues } };
  }
  // The file is a valid JSON object with valid values, but may still carry
  // unrecognized top-level keys (a typo like "modle", or a stale key). Surface those
  // as an advisory `unknown-key` diagnostic so misspellings don't silently take no
  // effect — the config still loads (unknown keys are ignored, never fatal). Parity:
  // Claude Code warns on unrecognized settings keys.
  const unknownKeys = findUnknownConfigKeys(config);
  if (unknownKeys.length > 0) {
    // Attach a "did you mean X?" near-match suggestion per unknown key so a typo
    // (e.g. "modle") is self-correctable, not merely flagged. Parity: Claude's
    // ValidationErrorsList actionable-fix suggestion for an unrecognized key.
    const described = unknownKeys.map(k => {
      const suggestion = suggestKnownConfigKey(k);
      return suggestion ? `${k} (did you mean "${suggestion}"?)` : k;
    });
    const message = `${unknownKeys.length} unknown config key(s): ${described.join(', ')}`;
    coreLog('config', `unknown key(s) in ${configPath}: ${message}`);
    return { config, diagnostic: { path: configPath, kind: 'unknown-key', message, unknownKeys } };
  }
  coreLog('config', `loaded ${configPath}`);
  return { config };
}

function loadConfigFile(configPath: string): ProjectConfig | null {
  return loadConfigFileChecked(configPath).config;
}

/**
 * Collect config diagnostics across every config tier under `cwd` (project,
 * local, global). A missing file is not a diagnostic; a corrupt one is, with a
 * pointer to its backup (fresh or most-recent) so the user can recover it.
 */
export function collectConfigDiagnostics(cwd: string): ConfigDiagnostic[] {
  const home = process.env.HOME || process.env.USERPROFILE || '';
  // Each probed path is tagged with its config LAYER so a diagnostic names its
  // provenance ("project" / "project, gitignored" / "user") rather than leaving the
  // user to infer it from a bare path. Parity: Claude's per-source settings validation.
  const paths: Array<{ path: string; source: ConfigSource }> = [
    { path: path.join(cwd, '.ami', 'config.json'), source: 'project' },
    { path: path.join(cwd, '.ami', 'config.local.json'), source: 'local' },
    ...(home ? [{ path: path.join(home, '.ami', 'config.json'), source: 'global' as const }] : []),
  ];
  const diagnostics: ConfigDiagnostic[] = [];
  for (const { path: p, source } of paths) {
    const { diagnostic } = loadConfigFileChecked(p);
    if (diagnostic) {
      diagnostics.push({ ...diagnostic, source, sourceName: getConfigSourceName(source), backupPath: diagnostic.backupPath ?? findMostRecentBackup(p) ?? undefined });
    }
  }
  // Managed/enterprise tier (fail-open VISIBILITY): a broken managed policy fails
  // CLOSED at load (an untrusted source — symlink redirect / world-writable file —
  // an unreadable file, a JSON syntax error, or a non-object payload all throw
  // {@link ManagedConfigError}), so it is normally invisible to /doctor. Surface it
  // HERE alongside the user/project/local diagnostics so an invalid security-bearing
  // policy file is reported across EVERY source, never silently ignored. Parity:
  // Claude's buildInstallationHealthDiagnostics aggregating invalid settings across
  // managed + user + project + local. A clean/absent managed policy adds nothing.
  try {
    loadManagedConfig();
  } catch (err) {
    if (err instanceof ManagedConfigError) {
      diagnostics.push({ path: getManagedConfigPath(), kind: 'unreadable', message: err.message, source: 'managed', sourceName: getConfigSourceName('managed') });
    }
  }
  return diagnostics;
}

// ---------------------------------------------------------------------------
// Internal-write suppression (parity: Claude's changeDetector.ts
// markInternalWrite / consumeInternalWrite). When AMI writes a config file
// itself (a `/config` change, an atomic-write helper), the directory watcher
// observes that write and would otherwise reload + fire a config-change
// notification/hook — a self-triggered feedback loop. Every AMI-originated write
// records its path+timestamp; a watcher that fires within INTERNAL_WRITE_WINDOW_MS
// consumes the mark and swallows the event, so AMI's own writes never re-trigger.
// ---------------------------------------------------------------------------

/** Window (ms) during which a watcher event is attributed to AMI's own write. */
export const INTERNAL_WRITE_WINDOW_MS = 5000;

/** path.resolve(file) → timestamp of the last AMI-originated write. */
const internalWrites = new Map<string, number>();

/** Record that AMI itself is about to write `filePath` (suppresses the echo). */
export function markInternalWrite(filePath: string, now: number = Date.now()): void {
  internalWrites.set(path.resolve(filePath), now);
}

/**
 * True when `filePath` has a pending internal-write mark within `windowMs`,
 * consuming it so the watcher event that triggered it is swallowed exactly once.
 * Stale marks (older than the window) are dropped and return false.
 */
export function consumeInternalWrite(
  filePath: string,
  windowMs: number = INTERNAL_WRITE_WINDOW_MS,
  now: number = Date.now(),
): boolean {
  const key = path.resolve(filePath);
  const marked = internalWrites.get(key);
  if (marked === undefined) return false;
  internalWrites.delete(key);
  return now - marked <= windowMs;
}

// ---------------------------------------------------------------------------
// Merge
// ---------------------------------------------------------------------------

/**
 * Concatenate a lower-layer array with a higher-layer one, de-duplicating while
 * preserving first-seen order. Parity: Claude's `settingsMergeCustomizer`
 * (`uniq([...target, ...source])`) so list-valued settings ACCUMULATE across
 * config layers instead of a higher layer clobbering a lower one. `existing`
 * (the already-accumulated lower-priority values) comes first so its ordering is
 * stable and higher layers only append genuinely-new entries.
 */
function mergeConfigArray(existing: string[] | undefined, incoming: string[]): string[] {
  return [...new Set([...(existing ?? []), ...incoming])];
}

/** A plain (non-null, non-array) object — the shape a nested config block always has. */
function isPlainObject(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

/**
 * Deep-merge one config layer's nested object over the accumulated lower-layer one so
 * a higher layer AUGMENTS rather than CLOBBERS a nested block: a global
 * `superinference: { enforcing: true }` plus a project `superinference: { useLLMCritic:
 * true }` yields BOTH flags, instead of the project wiping the global's `enforcing`.
 * Recurses into nested objects; string arrays accumulate (concat + dedupe, like
 * {@link mergeConfigArray}); any other scalar/array is replaced (higher wins). Parity:
 * Claude Code's `settingsMergeCustomizer` / lodash `mergeWith` deep-merge of nested
 * settings objects across layers (AMI previously replaced each nested block wholesale).
 */
function deepMergeConfigObjects(
  base: Record<string, unknown> | undefined,
  next: Record<string, unknown>,
): Record<string, unknown> {
  if (base === undefined) return { ...next };
  const out: Record<string, unknown> = { ...base };
  for (const [key, value] of Object.entries(next)) {
    const prev = out[key];
    if (
      Array.isArray(value) && Array.isArray(prev) &&
      value.every(x => typeof x === 'string') && prev.every(x => typeof x === 'string')
    ) {
      out[key] = [...new Set([...(prev as string[]), ...(value as string[])])];
    } else if (isPlainObject(value) && isPlainObject(prev)) {
      out[key] = deepMergeConfigObjects(prev, value);
    } else {
      out[key] = value;
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// ConfigService — hot-reload config watcher
// ---------------------------------------------------------------------------

export type ConfigChangeListener = (config: ProjectConfig, prev: ProjectConfig) => void;

/**
 * Debounce (ms) collapsing a burst of watcher events for the same file into one
 * reload. Kept small so a genuine edit takes effect promptly.
 */
export const CONFIG_WATCH_DEBOUNCE_MS = 100;

/**
 * Write-stability window (ms): when a watched config file is observed PRESENT but
 * momentarily unparseable (the classic half-written / truncate-then-append save),
 * the reload is retried once after this delay instead of silently dropping the
 * change. Parity: Claude's changeDetector `FILE_STABILITY_THRESHOLD_MS` /
 * chokidar `awaitWriteFinish` stabilityThreshold.
 */
export const CONFIG_FILE_STABILITY_THRESHOLD_MS = 1000;

/**
 * Deletion grace window (ms): a vanished config file is not acted on immediately —
 * the common atomic delete-then-recreate pattern (another session's tmp+rename,
 * an auto-updater) is coalesced into a single change when the file reappears within
 * this window; only a deletion that OUTLASTS it reverts the in-memory project layer
 * to defaults. Parity: Claude's `DELETION_GRACE_MS`.
 */
export const CONFIG_DELETION_GRACE_MS = 1000;

/**
 * Fallback reconcile-poll interval (ms). `fs.watch` is known to silently miss
 * events (network filesystems, some Linux configurations, editors that save via
 * atomic rename), so a low-frequency unref'd poll re-reads the file to pick up a
 * dropped change event. Parity: Claude's `FALLBACK_POLL_MS`.
 */
export const CONFIG_FALLBACK_POLL_MS = 5000;

/**
 * Retry interval (ms) for (re-)establishing the watcher when the config directory
 * does not exist yet or the underlying watcher errored — instead of giving up
 * permanently, so a project whose `.ami/` appears mid-session (or a transient
 * inotify error) still ends up with live config reload. Parity: Claude's `#rewatch`.
 */
export const CONFIG_WATCH_RETRY_MS = 1000;

export interface ConfigServiceOptions {
  /**
   * Target env object the project config's `env` block is re-applied to when the
   * watched file changes at runtime (defaults to `process.env`). Injectable so a
   * test can observe the take-effect-immediately behavior without mutating the
   * real process environment.
   */
  env?: NodeJS.ProcessEnv;
  /** Override the watcher debounce window (ms). Test seam. */
  debounceMs?: number;
  /** Override the write-stability retry window (ms). Test seam. */
  stabilityThresholdMs?: number;
  /** Override the deletion grace window (ms). Test seam. */
  deletionGraceMs?: number;
  /** Override the fallback reconcile-poll interval (ms); `<= 0` disables it. Test seam. */
  fallbackPollMs?: number;
  /** Override the watcher (re-)establish retry interval (ms). Test seam. */
  watchRetryMs?: number;
}

export class ConfigService {
  private config: ProjectConfig;
  private configPath: string;
  private watcher: fs.FSWatcher | null = null;
  private listeners: ConfigChangeListener[] = [];
  private debounceTimer: ReturnType<typeof setTimeout> | null = null;
  private stabilityTimer: ReturnType<typeof setTimeout> | null = null;
  private deletionTimer: ReturnType<typeof setTimeout> | null = null;
  private retryTimer: ReturnType<typeof setTimeout> | null = null;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private started = false;
  private onConfigChangeCallbacks: Array<(source: string, path: string) => void> = [];
  private envTarget: NodeJS.ProcessEnv;
  private lastEnvApplication: ApplyConfigEnvResult | null = null;
  private readonly debounceMs: number;
  private readonly stabilityThresholdMs: number;
  private readonly deletionGraceMs: number;
  private readonly fallbackPollMs: number;
  private readonly watchRetryMs: number;

  constructor(cwd: string, opts: ConfigServiceOptions = {}) {
    this.configPath = path.join(cwd, '.ami', 'config.json');
    this.config = loadConfigFile(this.configPath) ?? {};
    this.envTarget = opts.env ?? process.env;
    this.debounceMs = opts.debounceMs ?? CONFIG_WATCH_DEBOUNCE_MS;
    this.stabilityThresholdMs = opts.stabilityThresholdMs ?? CONFIG_FILE_STABILITY_THRESHOLD_MS;
    this.deletionGraceMs = opts.deletionGraceMs ?? CONFIG_DELETION_GRACE_MS;
    this.fallbackPollMs = opts.fallbackPollMs ?? CONFIG_FALLBACK_POLL_MS;
    this.watchRetryMs = opts.watchRetryMs ?? CONFIG_WATCH_RETRY_MS;
  }

  /**
   * The env application performed by the most recent change-triggering reload, or
   * null when no runtime change has re-applied env yet. Introspection surface for
   * the take-effect-immediately env behavior.
   */
  getLastEnvApplication(): ApplyConfigEnvResult | null {
    return this.lastEnvApplication;
  }

  get(): ProjectConfig {
    return { ...this.config };
  }

  onChange(listener: ConfigChangeListener): () => void {
    this.listeners.push(listener);
    return () => {
      this.listeners = this.listeners.filter(l => l !== listener);
    };
  }

  onConfigChange(cb: (source: string, path: string) => void): void {
    this.onConfigChangeCallbacks.push(cb);
  }

  watch(): void {
    if (this.started) return;
    this.started = true;
    this.ensureWatch();
    this.startFallbackPoll();
  }

  /**
   * Establish the directory watcher, or — when the directory does not exist yet /
   * `fs.watch` throws (e.g. inotify exhaustion) — schedule a retry instead of giving
   * up permanently, so a `.ami/` created after startup (or a transient error) still
   * ends up watched. Parity: Claude's `#rewatch` retry-even-on-same-dir.
   */
  private ensureWatch(): void {
    if (this.watcher) return;
    const dir = path.dirname(this.configPath);
    if (!fsHooks.existsSync(dir)) {
      coreLog('config', `config dir ${dir} absent; scheduling watch retry in ${this.watchRetryMs}ms`);
      this.scheduleWatchRetry();
      return;
    }
    try {
      this.watcher = fsHooks.watch(dir, (_eventType, filename) => this.onWatchEvent(filename));
      // unref so the watcher doesn't prevent process exit in test environments
      this.watcher.unref();
      this.watcher.on('error', () => this.onWatcherError());
      coreLog('config', `watching ${this.configPath} for changes`);
    } catch {
      // watch() can throw (ENOSPC on inotify watchers, a transient FS error) — leave
      // the watcher null and re-attempt rather than permanently disabling hot-reload.
      this.scheduleWatchRetry();
    }
  }

  /** (Re-)establish the watcher after a delay; no-op once a watcher exists or stopped. */
  private scheduleWatchRetry(): void {
    if (this.retryTimer || this.watcher || !this.started) return;
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      if (this.started) this.ensureWatch();
    }, this.watchRetryMs);
    this.retryTimer.unref();
  }

  /** A watcher `error` event closes the dead watcher and re-establishes a fresh one. */
  private onWatcherError(): void {
    coreLog('config', 'config watcher error; re-establishing watch');
    if (this.watcher) {
      try { this.watcher.close(); } catch { /* already dead */ }
      this.watcher = null;
    }
    this.scheduleWatchRetry();
  }

  /** Debounce a config.json event into a single stability-checked reload. */
  private onWatchEvent(filename: string | null): void {
    if (filename !== 'config.json') return;

    // Swallow the echo of AMI's own write so it never self-triggers a reload
    // + config-change notification (parity: changeDetector consumeInternalWrite).
    if (consumeInternalWrite(this.configPath)) return;

    // A recreate within the deletion grace window cancels the pending deletion so the
    // common atomic delete-then-recreate save is coalesced into a single change rather
    // than flapping through a spurious "deleted" state.
    if (this.deletionTimer && fsHooks.existsSync(this.configPath)) {
      clearTimeout(this.deletionTimer);
      this.deletionTimer = null;
    }

    if (this.debounceTimer) clearTimeout(this.debounceTimer);
    this.debounceTimer = setTimeout(() => {
      this.debounceTimer = null;
      this.processWatchChange();
    }, this.debounceMs);
    this.debounceTimer.unref();
  }

  /**
   * Apply a debounced watcher change: a missing file enters the deletion-grace path;
   * a present-but-unparseable file (a half-written save caught mid-write) is retried
   * once after the stability window instead of being silently dropped; otherwise the
   * change is reloaded immediately.
   */
  private processWatchChange(): void {
    if (!fsHooks.existsSync(this.configPath)) {
      this.handlePossibleDeletion();
      return;
    }
    const checked = loadConfigFileChecked(this.configPath);
    if (checked.config === null && checked.diagnostic && checked.diagnostic.kind === 'parse-error') {
      this.scheduleStabilityRetry();
      return;
    }
    this.reload();
  }

  /** Retry a reload once after the write-stability window (partial-write tolerance). */
  private scheduleStabilityRetry(): void {
    if (this.stabilityTimer) return;
    coreLog('config', `config at ${this.configPath} not yet stable; retrying reload in ${this.stabilityThresholdMs}ms`);
    this.stabilityTimer = setTimeout(() => {
      this.stabilityTimer = null;
      // By now the write should have finished. A still-unparseable file is a REAL
      // syntax error and reload() correctly declines to clobber the in-memory config.
      this.reload();
    }, this.stabilityThresholdMs);
    this.stabilityTimer.unref();
  }

  /** Buffer a vanished file for the grace window before treating it as a deletion. */
  private handlePossibleDeletion(): void {
    if (this.deletionTimer) return;
    this.deletionTimer = setTimeout(() => {
      this.deletionTimer = null;
      if (fsHooks.existsSync(this.configPath)) {
        // Recreated during the grace window but the add event was missed — reload it.
        this.reload();
      } else {
        this.applyDeletion();
      }
    }, this.deletionGraceMs);
    this.deletionTimer.unref();
  }

  /**
   * A grace-expired deletion reverts the in-memory project layer to empty so the
   * session falls back to the lower merge layers (global/managed/env) instead of
   * staying pinned to the deleted file's values. Listeners + onConfigChange fire with
   * the empty config, mirroring a normal change. Parity: Claude's change-detector
   * treating a (grace-expired) deletion as a settings change.
   */
  private applyDeletion(): void {
    const prev = this.config;
    if (JSON.stringify(prev) === '{}') return;
    this.config = {};
    coreLog('config', `config ${this.configPath} deleted; reverting project layer to defaults`);
    for (const listener of this.listeners) {
      try { listener({}, prev); } catch (err) { coreLog('config', 'config listener error', { error: String(err) }); }
    }
    for (const cb of this.onConfigChangeCallbacks) {
      try { cb('project', this.configPath); } catch (err) { coreLog('config', 'onConfigChange callback error', { error: String(err) }); }
    }
  }

  /**
   * Low-frequency unref'd reconcile poll that re-reads the file so a change event
   * silently dropped by `fs.watch` (network FS, atomic-rename save) is still picked
   * up within one interval. Parity: Claude's FALLBACK_POLL_MS safety net.
   */
  private startFallbackPoll(): void {
    if (this.pollTimer || this.fallbackPollMs <= 0) return;
    this.pollTimer = setInterval(() => { this.reload(); }, this.fallbackPollMs);
    this.pollTimer.unref();
  }

  reload(): boolean {
    const prev = this.config;
    const next = loadConfigFile(this.configPath);
    if (!next) return false;

    const changed = JSON.stringify(prev) !== JSON.stringify(next);
    if (changed) {
      this.config = next;
      // Take-effect-immediately: re-apply the watched project config's `env`
      // block to the process environment so a mid-session edit (a changed tuning
      // knob / model-name default) applies without a restart, instead of being
      // silently ignored until the next launch. Trust-tiered via applyConfigEnv —
      // a project-scoped file still only applies the SAFE_ENV_VARS allowlist, so
      // a committed config can never inject a dangerous var at runtime. Parity:
      // Claude's onChangeAppState re-applying config-driven env on a settings change.
      try {
        this.lastEnvApplication = applyConfigEnv({ project: next }, this.envTarget);
      } catch (err) {
        coreLog('config', 'runtime env re-application failed', { error: String(err) });
      }
      for (const listener of this.listeners) {
        try { listener(next, prev); } catch (err) { coreLog('config', 'config listener error', { error: String(err) }); }
      }
      for (const cb of this.onConfigChangeCallbacks) {
        try { cb('project', this.configPath); } catch (err) { coreLog('config', 'onConfigChange callback error', { error: String(err) }); }
      }
    }
    return changed;
  }

  stop(): void {
    if (this.debounceTimer) { clearTimeout(this.debounceTimer); this.debounceTimer = null; }
    if (this.stabilityTimer) { clearTimeout(this.stabilityTimer); this.stabilityTimer = null; }
    if (this.deletionTimer) { clearTimeout(this.deletionTimer); this.deletionTimer = null; }
    if (this.retryTimer) { clearTimeout(this.retryTimer); this.retryTimer = null; }
    if (this.pollTimer) { clearInterval(this.pollTimer); this.pollTimer = null; }
    if (this.watcher) {
      this.watcher.close();
      this.watcher = null;
    }
    this.started = false;
  }
}

/**
 * Merge configuration sources in priority order.
 *
 * Priority (highest first):
 *   1. `cliArgs`       — flags passed on the command line
 *   2. `envVars`       — values derived from environment variables
 *   3. `localConfig`   — `.ami/config.local.json` (gitignored)
 *   4. `projectConfig` — `.ami/config.json` in the project
 *   5. `globalConfig`  — `~/.ami/config.json`
 *   6. `managedConfig` — `/etc/superinference/config.json`
 *
 * Only defined (non-undefined) values participate in the merge.
 */
export function mergeConfigs(
  cliArgs: Partial<ProjectConfig>,
  envVars: Partial<ProjectConfig>,
  projectConfig: ProjectConfig | null,
  globalConfig: ProjectConfig | null,
  localConfig?: ProjectConfig | null,
  managedConfig?: ProjectConfig | null,
): ProjectConfig {
  const layers: Array<Partial<ProjectConfig>> = [
    managedConfig ?? {},
    globalConfig ?? {},
    projectConfig ?? {},
    localConfig ?? {},
    envVars,
    cliArgs,
  ];

  const merged: ProjectConfig = {};

  for (const layer of layers) {
    if (layer.model !== undefined) merged.model = layer.model;
    if (layer.provider !== undefined) merged.provider = layer.provider;
    if (layer.baseUrl !== undefined) merged.baseUrl = layer.baseUrl;
    if (layer.permissionMode !== undefined) merged.permissionMode = layer.permissionMode;
    if (layer.thinkingLevel !== undefined) merged.thinkingLevel = layer.thinkingLevel;
    if (layer.maxEffortLevel !== undefined) merged.maxEffortLevel = layer.maxEffortLevel;
    if (layer.fallbackModel !== undefined) merged.fallbackModel = layer.fallbackModel;
    if (layer.compactionModel !== undefined) merged.compactionModel = layer.compactionModel;
    if (layer.maxTurns !== undefined) merged.maxTurns = layer.maxTurns;
    if (layer.tokenBudget !== undefined) merged.tokenBudget = layer.tokenBudget;
    if (layer.maxBudgetUsd !== undefined) merged.maxBudgetUsd = layer.maxBudgetUsd;
    if (layer.superinference !== undefined) merged.superinference = deepMergeConfigObjects(merged.superinference as Record<string, unknown> | undefined, layer.superinference as Record<string, unknown>) as ProjectConfig['superinference'];
    if (layer.skill !== undefined) merged.skill = layer.skill;
    if (layer.bashOutputMaxChars !== undefined) merged.bashOutputMaxChars = layer.bashOutputMaxChars;
    if (layer.taskOutputMaxChars !== undefined) merged.taskOutputMaxChars = layer.taskOutputMaxChars;
    if (layer.cacheTtl !== undefined) merged.cacheTtl = layer.cacheTtl;
    if (layer.safeMode !== undefined) merged.safeMode = layer.safeMode;
    if (layer.maxWebSearchesPerSession !== undefined) merged.maxWebSearchesPerSession = layer.maxWebSearchesPerSession;
    if (layer.cleanupPeriodDays !== undefined) merged.cleanupPeriodDays = layer.cleanupPeriodDays;
    if (layer.deferTools !== undefined) merged.deferTools = layer.deferTools;
    // Array-valued settings ACCUMULATE across layers (concat + dedupe), so a
    // higher layer augments rather than clobbers a lower one — parity with
    // Claude's settingsMergeCustomizer. Managed-authoritative arrays
    // (allowedMcpServers / sandboxExcludedCommands when managed owns them) are
    // still replaced wholesale by the managed override below (AMI superset).
    if (layer.alwaysLoadTools !== undefined) merged.alwaysLoadTools = mergeConfigArray(merged.alwaysLoadTools, layer.alwaysLoadTools);
    if (layer.fallbackModels !== undefined) merged.fallbackModels = mergeConfigArray(merged.fallbackModels, layer.fallbackModels);
    if (layer.allowedMcpServers !== undefined) merged.allowedMcpServers = mergeConfigArray(merged.allowedMcpServers, layer.allowedMcpServers);
    // Denylist ACCUMULATES across layers like the allowlist — a lower layer's
    // deny can never be removed by a higher one, only added to (deny-wins).
    if (layer.deniedMcpServers !== undefined) merged.deniedMcpServers = mergeConfigArray(merged.deniedMcpServers, layer.deniedMcpServers);
    if (layer.managedMcpServers !== undefined) merged.managedMcpServers = layer.managedMcpServers;
    if (layer.statusLine !== undefined) merged.statusLine = layer.statusLine;
    if (layer.sandboxExcludedCommands !== undefined) merged.sandboxExcludedCommands = mergeConfigArray(merged.sandboxExcludedCommands, layer.sandboxExcludedCommands);
    // env merges key-wise across layers (higher-priority layers override single
    // keys rather than replacing the whole block).
    if (layer.env !== undefined) merged.env = { ...merged.env, ...layer.env };
    if (layer.tipsEnabled !== undefined) merged.tipsEnabled = layer.tipsEnabled;
    if (layer.tipsOverride !== undefined) merged.tipsOverride = deepMergeConfigObjects(merged.tipsOverride as Record<string, unknown> | undefined, layer.tipsOverride as Record<string, unknown>) as ProjectConfig['tipsOverride'];
    if (layer.composerHintsEnabled !== undefined) merged.composerHintsEnabled = layer.composerHintsEnabled;
    if (layer.composerHintsOverride !== undefined) merged.composerHintsOverride = deepMergeConfigObjects(merged.composerHintsOverride as Record<string, unknown> | undefined, layer.composerHintsOverride as Record<string, unknown>) as ProjectConfig['composerHintsOverride'];
  }

  // Managed allowlist is authoritative and MUST NOT be overridable by lower
  // layers (project/local/env/cli) — otherwise managed enforcement is defeated.
  if (managedConfig?.enforceAvailableModels) {
    merged.enforceAvailableModels = managedConfig.enforceAvailableModels;
  }
  // The bypass/auto-allow killswitch is a MANAGED_ONLY_KEY: it is honored ONLY
  // from the managed tier. Lower layers never contribute it (the per-field loop
  // above deliberately omits it), so a project/local/env/cli config can neither
  // enable nor disable the killswitch — managed policy is the sole source.
  if (managedConfig?.disableBypassPermissionsMode) {
    merged.disableBypassPermissionsMode = true;
  }
  // Managed hook kill-switches are MANAGED_ONLY_KEYS: honored ONLY from the managed
  // tier (the per-field loop above deliberately omits them), so a project/local/env/
  // cli config can neither disable all hooks nor relax the managed-only lockdown —
  // managed policy is the sole source. Parity: Claude Code's disableAllHooks /
  // allowManagedHooksOnly policy settings.
  if (managedConfig?.disableAllHooks) {
    merged.disableAllHooks = true;
  }
  if (managedConfig?.allowManagedHooksOnly) {
    merged.allowManagedHooksOnly = true;
  }
  // Managed plugin policy is MANAGED-authoritative: the per-field loop above
  // deliberately omits enabledPlugins/allowManagedModsOnly, so a project/local/env/
  // cli config can neither force-disable a plugin nor relax the allow-only mode —
  // managed policy is the sole source. Parity: Claude Code's enabledPlugins policy.
  if (managedConfig?.enabledPlugins) {
    merged.enabledPlugins = managedConfig.enabledPlugins;
  }
  if (managedConfig?.allowManagedModsOnly) {
    merged.allowManagedModsOnly = true;
  }
  // Managed MCP allowlist is also authoritative.
  if (managedConfig?.allowedMcpServers) {
    merged.allowedMcpServers = managedConfig.allowedMcpServers;
  }
  // Managed MCP denylist is authoritative AND still accumulates the lower-tier
  // denies on top (deny-wins): managed denies can never be dropped by a lower
  // layer, but a project/local layer may add MORE blocked servers. Parity with
  // Claude's deniedMcpServers precedence (a server denied at any tier stays denied).
  if (managedConfig?.deniedMcpServers) {
    merged.deniedMcpServers = mergeConfigArray(managedConfig.deniedMcpServers, merged.deniedMcpServers ?? []);
  }
  if (managedConfig?.managedMcpServers) {
    merged.managedMcpServers = managedConfig.managedMcpServers;
  }
  // Sandbox exemptions weaken the container sandbox, so when managed policy owns
  // them a lower (project/local) layer must not be able to add its own — parity:
  // Claude's areSandboxSettingsLockedByPolicy refuses local sandbox changes once a
  // higher-priority config owns the setting. (When managed does NOT set it, a
  // project may still exempt commands, so this only overrides when present.)
  if (managedConfig?.sandboxExcludedCommands) {
    merged.sandboxExcludedCommands = managedConfig.sandboxExcludedCommands;
  }

  return merged;
}

// ---------------------------------------------------------------------------
// Settings-sourced env application (trust-tiered)
// ---------------------------------------------------------------------------

/** The trust tier of each config source for env-application purposes. */
export const CONFIG_ENV_TIERS: ReadonlyArray<readonly [keyof ConfigEnvLayers, EnvTrustTier]> = [
  ['managed', 'trusted'],
  ['global', 'trusted'],
  ['project', 'project'],
  ['local', 'project'],
];

export interface ConfigEnvLayers {
  managed?: ProjectConfig | null;
  global?: ProjectConfig | null;
  project?: ProjectConfig | null;
  local?: ProjectConfig | null;
}

export interface ApplyConfigEnvResult {
  applied: Record<string, string>;
  rejected: Array<{ key: string; reason: EnvRejectReason; tier: EnvTrustTier }>;
}

/**
 * Apply the `env` block of each config source to `target` (process.env by
 * default), honoring the trust model:
 *  - managed/global are trusted → apply any non-pinned var;
 *  - project/local are untrusted → apply only the SAFE_ENV_VARS allowlist;
 *  - when the host owns inference routing, provider/credential vars are stripped
 *    from every source.
 *
 * Applied in ascending-trust order (local → project → global → managed) so a
 * trusted source wins on key collisions, mirroring {@link mergeConfigs} where
 * managed is authoritative. Returns the applied/rejected vars for diagnostics.
 */
export function applyConfigEnv(
  layers: ConfigEnvLayers,
  target: NodeJS.ProcessEnv = process.env,
): ApplyConfigEnvResult {
  const hostManagedProvider = isHostManagedProvider(target);
  const applied: Record<string, string> = {};
  const rejected: Array<{ key: string; reason: EnvRejectReason; tier: EnvTrustTier }> = [];
  // Reverse so higher-trust sources are applied last and win collisions.
  for (const [source, tier] of [...CONFIG_ENV_TIERS].reverse()) {
    const env = layers[source]?.env;
    const result = filterSettingsEnv(env, { tier, hostManagedProvider });
    for (const [key, value] of Object.entries(result.applied)) {
      target[key] = value;
      applied[key] = value;
    }
    for (const r of result.rejected) {
      rejected.push({ key: r.key, reason: r.reason, tier });
    }
  }
  return { applied, rejected };
}

// ---------------------------------------------------------------------------
// Bounded-integer env-var validation (DoS-cap + structured status)
// ---------------------------------------------------------------------------

/** Outcome kind of a {@link validateBoundedIntEnvVar} parse. */
export type BoundedIntStatus =
  /** A valid in-range integer was supplied and used verbatim. */
  | 'ok'
  /** No value (absent/empty) was supplied; the default was used. */
  | 'default'
  /** The value exceeded `max` and was clamped down to it. */
  | 'capped'
  /** The value was non-numeric or below `min`; the default was used. */
  | 'invalid';

/** Structured result of parsing a bounded-integer env var. */
export interface BoundedIntResult {
  /** The value to actually use (always within [min, max]). */
  effective: number;
  status: BoundedIntStatus;
  /** A human/debug diagnostic for the `capped`/`invalid` cases (else undefined). */
  message?: string;
}

/** Tuning for {@link validateBoundedIntEnvVar}. `min` defaults to 1 (positive). */
export interface BoundedIntOptions {
  /** Fallback used when the value is absent or invalid. */
  default: number;
  /** Lowest accepted value (inclusive). Below it the value is rejected. Default 1. */
  min?: number;
  /** Highest accepted value (inclusive). Above it the value is CAPPED to this. */
  max?: number;
}

/**
 * Parse a numeric environment variable into a bounded, trustworthy integer —
 * the single canonical replacement for the ad-hoc `parseInt(env) || DEFAULT` /
 * `Number(env) || DEFAULT` reads scattered across the codebase, which silently
 * coerce NaN/0/negative to the default but apply NO upper ceiling, so a mistaken
 * or hostile `AMI_FIND_MAX_DEPTH=99999999` / `SI_MODEL_LIST_TIMEOUT_MS=huge`
 * flows through verbatim and enables resource-exhaustion / hang (DoS).
 *
 * Returns a structured `{ effective, status, message }` so a caller (and an
 * operator, via the debug log) learns WHEN a value was capped or rejected
 * rather than it failing silently:
 *  - absent/empty            → `{ default }` (no diagnostic);
 *  - NaN or `< min`          → `{ invalid }`, `effective = default`, logged;
 *  - `> max`                 → `{ capped }`,  `effective = max`,     logged;
 *  - otherwise               → `{ ok }`,      `effective = value`.
 *
 * Parity: Claude Code's `validateBoundedIntEnvVar` (envValidation.ts) upper-limit
 * cap + invalid/capped status reporting. When `name` is empty the debug log is
 * suppressed (the thin {@link parsePositiveIntEnv} wrapper reuses this without a
 * name), so there is exactly one bounded-int implementation.
 */
export function validateBoundedIntEnvVar(
  name: string,
  raw: string | undefined,
  opts: BoundedIntOptions,
): BoundedIntResult {
  const min = opts.min ?? 1;
  if (raw === undefined || raw.trim() === '') {
    return { effective: opts.default, status: 'default' };
  }
  const n = parseInt(raw, 10);
  if (!Number.isFinite(n) || n < min) {
    const message = `${name || 'bounded int'}="${raw}" is not an integer >= ${min}; using default ${opts.default}`;
    if (name) coreLog('config', message);
    return { effective: opts.default, status: 'invalid', message };
  }
  if (opts.max !== undefined && n > opts.max) {
    const message = `${name || 'bounded int'}=${n} exceeds the maximum ${opts.max}; capped to ${opts.max}`;
    if (name) coreLog('config', message);
    return { effective: opts.max, status: 'capped', message };
  }
  return { effective: n, status: 'ok' };
}

// ---------------------------------------------------------------------------
// Config file safety — atomic writes, stale .tmp cleanup, JSON validation
// ---------------------------------------------------------------------------

export const CONFIG_WRITE_TIMEOUT_MS = 5000;

/**
 * Mockable filesystem seam (ESM-safe) for the atomic-write helpers below.
 * Tests swap individual methods via `mock.method(fsHooks, 'name', ...)` to
 * exercise the rename-fallback and error branches deterministically. Defaults
 * to the real `fs`; production behavior is unchanged.
 */
export const fsHooks = {
  existsSync: fs.existsSync as typeof fs.existsSync,
  readFileSync: fs.readFileSync as typeof fs.readFileSync,
  writeFileSync: fs.writeFileSync as typeof fs.writeFileSync,
  renameSync: fs.renameSync as typeof fs.renameSync,
  unlinkSync: fs.unlinkSync as typeof fs.unlinkSync,
  mkdirSync: fs.mkdirSync as typeof fs.mkdirSync,
  watch: fs.watch as typeof fs.watch,
  readdirSync: fs.readdirSync as typeof fs.readdirSync,
  lstatSync: fs.lstatSync as typeof fs.lstatSync,
  readlinkSync: fs.readlinkSync as typeof fs.readlinkSync,
  statSync: fs.statSync as typeof fs.statSync,
  chmodSync: fs.chmodSync as typeof fs.chmodSync,
};

/**
 * Default POSIX mode for a NEWLY-created config file: owner read/write only (0o600).
 * A config file can hold a provider base-URL override, an `env` block, MCP/credential-
 * adjacent knobs; an unprivileged co-user on the same host must not be able to read it.
 * An EXISTING file's mode is preserved on rewrite (only new files get this default), so
 * a user who deliberately widened permissions is not fought. Parity: Claude Code writes
 * its config with `mode: 0o600`.
 */
export const CONFIG_FILE_MODE = 0o600;

/**
 * Mockable JSON seam (ESM-safe). Tests swap `jsonHooks.parse` via `mock.method`
 * to exercise the non-Error failure arm below. Defaults to the global JSON.parse.
 */
export const jsonHooks = { parse: JSON.parse as (text: string) => unknown };

export function validateConfigJson(content: string): { valid: boolean; error?: string } {
  let parsed: unknown;
  try {
    // Same JSONC tolerance as the loader (BOM / comments / trailing comma) so a
    // stale `.tmp` written as JSONC is not needlessly discarded as "invalid".
    parsed = parseJsonc(content);
  } catch (err) {
    // Attach actionable recovery guidance (one shared source with the loader) so a
    // malformed config/stale-tmp payload tells the user how to fix it, not just that
    // it is broken. Parity: Claude's root-level JSON syntax-error tip.
    return { valid: false, error: `Invalid JSON: ${err instanceof Error ? err.message : String(err)}. ${JSON_SYNTAX_RECOVERY_HINT}` };
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return { valid: false, error: `Expected a JSON object, got ${describeJsonType(parsed)}` };
  }
  return { valid: true };
}

// ---------------------------------------------------------------------------
// Symlink write-through + pre-write backup rotation
// ---------------------------------------------------------------------------

/** The infix tagging a timestamped pre-write backup (distinct from `.corrupt-`). */
const CONFIG_BACKUP_INFIX = '.backup-';

/** Keep at most this many pre-write backups per config file (parity: Claude's MAX_BACKUPS). */
export const MAX_CONFIG_BACKUPS = 5;

/**
 * Minimum spacing (ms) between pre-write backups of the same file (parity: Claude's
 * MIN_BACKUP_INTERVAL_MS). During startup many `/config` / status-line writes can fire
 * within milliseconds; without this, each would spawn a backup file and spam disk.
 */
export const CONFIG_BACKUP_MIN_INTERVAL_MS = 60_000;

/** The epoch-ms timestamp encoded in a `<file>.backup-<ms>` name, or null if unparseable. */
function backupTimestamp(backupPath: string): number | null {
  const idx = backupPath.lastIndexOf(CONFIG_BACKUP_INFIX);
  if (idx < 0) return null;
  const n = Number(backupPath.slice(idx + CONFIG_BACKUP_INFIX.length));
  return Number.isFinite(n) ? n : null;
}

/** All `<file>.backup-*` snapshots beside `filePath`, newest (largest ts) first. */
function listConfigBackups(filePath: string): string[] {
  try {
    const dir = path.dirname(filePath);
    const base = path.basename(filePath);
    return (fsHooks.readdirSync(dir) as string[])
      .filter(e => e.startsWith(`${base}${CONFIG_BACKUP_INFIX}`))
      .map(e => path.join(dir, e))
      .sort((a, b) => (backupTimestamp(b) ?? 0) - (backupTimestamp(a) ?? 0));
  } catch {
    return [];
  }
}

/**
 * Resolve the REAL file a config write must land on: when `filePath` is a symlink
 * (e.g. a dotfile-managed `~/.ami/config.json` stow/chezmoi link), return its
 * target so the write updates the target IN PLACE and preserves the symlink —
 * instead of a tmp+rename replacing the link with a regular file. A non-symlink
 * or absent path resolves to itself. Parity: Claude Code's "if settings.json is a
 * symlink, update the target file instead".
 */
export function resolveConfigWriteTarget(filePath: string): string {
  try {
    if (fsHooks.lstatSync(filePath).isSymbolicLink()) {
      const dest = fsHooks.readlinkSync(filePath) as string;
      return path.resolve(path.dirname(filePath), dest);
    }
  } catch {
    // Not present / not inspectable → write the given path directly.
  }
  return filePath;
}

/**
 * Snapshot the CURRENT on-disk content of `filePath` to a timestamped
 * `<file>.backup-<ms>` before it is overwritten, so a logically-bad (but
 * byte-valid) write — a merge bug, a reset, a truncated payload — stays
 * recoverable even though the file was never "corrupt" at the byte level.
 *
 * Throttled to one backup per {@link CONFIG_BACKUP_MIN_INTERVAL_MS} (no backup
 * storm during startup bursts) and pruned to the {@link MAX_CONFIG_BACKUPS}
 * newest. Best-effort: a missing file or any backup/prune failure is swallowed —
 * a backup must never block or break the write it protects. Returns the backup
 * path written, or null. Parity: Claude's saveConfigWithLock backup rotation.
 */
export function backupConfigBeforeWrite(filePath: string, now: number = Date.now()): string | null {
  let content: string;
  try {
    if (!fsHooks.existsSync(filePath)) return null;
    content = fsHooks.readFileSync(filePath, 'utf-8') as string;
  } catch {
    return null; // nothing readable to back up
  }
  try {
    // Throttle: skip when a backup already exists within the interval.
    const existing = listConfigBackups(filePath);
    const newestTs = existing.length > 0 ? backupTimestamp(existing[0]!) : null;
    if (newestTs !== null && now - newestTs < CONFIG_BACKUP_MIN_INTERVAL_MS) return null;

    const backupPath = `${filePath}${CONFIG_BACKUP_INFIX}${now}`;
    fsHooks.writeFileSync(backupPath, content, 'utf-8');
    coreLog('config', `backed up ${filePath} to ${backupPath}`);

    // Prune to the newest MAX_CONFIG_BACKUPS (the just-written one included).
    for (const stale of listConfigBackups(filePath).slice(MAX_CONFIG_BACKUPS)) {
      try { fsHooks.unlinkSync(stale); } catch { /* best effort */ }
    }
    return backupPath;
  } catch {
    return null; // backup is best-effort; never surface it to the caller
  }
}

/**
 * Mode (lowest 12 permission bits) of an existing file, or null when it is absent /
 * cannot be stat'd (a brand-new config). Used to PRESERVE a user's deliberately-widened
 * (or tightened) permissions across a rewrite instead of resetting them every save.
 */
function existingFileMode(filePath: string): number | null {
  try {
    return fsHooks.statSync(filePath).mode & 0o7777;
  } catch {
    return null;
  }
}

/**
 * Write `content` to `filePath` durably and with the right permissions:
 *  - `flush: true` fsyncs the file descriptor before the call returns, so a power loss
 *    or OS crash immediately after the subsequent rename cannot leave a zero-length or
 *    half-flushed config even though the rename "succeeded" (parity: Claude Code's
 *    writeFileSyncAndFlush);
 *  - a NEW file is created `mode` (0o600 — owner-only); an EXISTING file's `preserveMode`
 *    is re-applied after the write so a rewrite never widens/narrows what the user chose.
 *
 * Node applies the `mode` option only when it CREATES the file, so the explicit chmod is
 * what carries an existing file's preserved mode onto the freshly-written temp file.
 */
function writeFileDurable(filePath: string, content: string, mode: number, preserveMode: number | null): void {
  fsHooks.writeFileSync(filePath, content, { encoding: 'utf-8', flush: true, mode });
  if (preserveMode !== null) {
    try { fsHooks.chmodSync(filePath, preserveMode); } catch { /* best effort — the write already succeeded */ }
  }
}

export function safeWriteConfig(filePath: string, data: Record<string, unknown>): void {
  // Write THROUGH a symlinked config path to its target so a dotfile-managed
  // settings symlink is updated in place rather than clobbered by a regular file
  // (parity: Claude's "if settings.json is a symlink, update the target file").
  const target = resolveConfigWriteTarget(filePath);
  const tmpPath = target + '.tmp';
  const json = JSON.stringify(data, null, 2) + '\n';
  // Snapshot the prior good content before overwriting it (throttled + rotated),
  // so a logically-bad write stays recoverable.
  backupConfigBeforeWrite(target);
  // Attribute the imminent write to AMI so the config watcher does not reload +
  // re-notify on its own echo (parity: changeDetector markInternalWrite). Mark
  // both the logical path and the resolved target (symlink case).
  markInternalWrite(filePath);
  if (target !== filePath) markInternalWrite(target);
  // Preserve an existing config's permissions across the rewrite; a brand-new file
  // is created owner-only (0o600) so it is never world/group-readable.
  const preserveMode = existingFileMode(target);
  try {
    const dir = path.dirname(target);
    if (!fsHooks.existsSync(dir)) {
      fsHooks.mkdirSync(dir, { recursive: true });
    }
    writeFileDurable(tmpPath, json, CONFIG_FILE_MODE, preserveMode);
    try {
      fsHooks.renameSync(tmpPath, target);
    } catch {
      writeFileDurable(target, json, CONFIG_FILE_MODE, preserveMode);
      try { fsHooks.unlinkSync(tmpPath); } catch { /* best effort */ }
    }
  } catch (err) {
    try { fsHooks.unlinkSync(tmpPath); } catch { /* best effort */ }
    throw err;
  }
}

/** Why a {@link updateConfigFile} call did (or did not) persist. */
export type UpdateConfigReason = 'written' | 'no-change' | 'corrupt';

/** Outcome of a guarded functional config update. */
export interface UpdateConfigResult {
  /** True iff the new config was persisted to disk. */
  written: boolean;
  reason: UpdateConfigReason;
  /** The resulting config (the new one when written, else the current on-disk one). */
  config: ProjectConfig;
  /** Present only when `reason === 'corrupt'`: the blocking load diagnostic. */
  diagnostic?: ConfigDiagnostic;
}

/**
 * Guarded functional read-modify-write of a config file (parity: Claude Code's
 * `saveGlobalConfig(updater)` / `saveConfigWithLock` lost-update + auth-loss guard).
 *
 * The caller passes a pure `updater(current) -> next`. This helper:
 *  1. RE-READS the file fresh from disk right before writing (via
 *     {@link loadConfigFileChecked}) so a concurrent writer's keys are part of
 *     `current` and the merge lands on the LATEST state — closing the classic
 *     read-then-write lost-update window where a stale in-memory base would
 *     clobber another process's changes.
 *  2. REFUSES to write when the freshly-read file is present-but-corrupt
 *     (`parse-error` / `not-object`): writing a merge built on an assumed-empty
 *     base would wipe the user's recoverable content. This is AMI's analog of
 *     Claude's "re-read returned defaults → don't persist defaults over good
 *     state" (`wouldLoseAuthState`): a momentarily-unparseable file never gets
 *     overwritten with a defaults-derived config. (Advisory `invalid-value` /
 *     `unknown-key` diagnostics still load — the update proceeds on them.)
 *  3. Treats an updater that returns the SAME reference as a no-op (no write),
 *     mirroring `saveGlobalConfig`'s same-reference skip.
 *
 * The actual write reuses {@link safeWriteConfig} (atomic temp+rename, internal-
 * write suppression), so there is one canonical write path.
 *
 * CROSS-PROCESS SERIALIZATION. The entire re-read → merge → write runs inside a
 * cross-process advisory lock ({@link withFileLockSync} on the resolved target), so
 * two AMI processes (a detached run plus an interactive one, several swarm teammates)
 * can never interleave a read and a write and lose each other's keys — the classic
 * lost-update race the step-1 re-read only narrows is closed outright. Parity: Claude
 * Code's `saveConfigWithLock` takes a `lockfile.lockSync` before the read-modify-write,
 * logs lock contention, and installs an `onCompromised` handler that downgrades a stolen
 * lock to a log (never an unhandled throw). The lock reuses the ONE canonical lock
 * primitive (no parallel lock implementation). `lockOptions` lets a caller tune the
 * retry budget / observe `onCompromised`; a stolen lock still defaults to a log.
 */
export function updateConfigFile(
  filePath: string,
  updater: (current: ProjectConfig) => ProjectConfig,
  lockOptions: FileLockSyncOptions = {},
): UpdateConfigResult {
  // Lock the RESOLVED target (symlink followed) so a symlinked path and its target
  // contend on the same lock, and so the read below sees a state no concurrent
  // AMI writer can mutate until we release.
  const lockTarget = resolveConfigWriteTarget(filePath);
  // The lock file lives beside the target, so its parent directory must exist
  // before acquisition even when the config itself is being created for the first
  // time (parity: Claude's saveConfigWithLock mkdirSync's the dir before lockSync).
  try {
    const dir = path.dirname(lockTarget);
    if (!fsHooks.existsSync(dir)) fsHooks.mkdirSync(dir, { recursive: true });
  } catch { /* safeWriteConfig re-attempts dir creation and surfaces a real error */ }
  const started = Date.now();
  return withFileLockSync(lockTarget, () => {
    // Log contention (parity: Claude's "lock took longer than expected — another
    // instance may be running"), measured as time spent waiting to enter the section.
    const waited = Date.now() - started;
    if (waited > 100) {
      coreLog('config', `config lock contention on ${lockTarget}: waited ${waited}ms (another AMI instance may be writing)`);
    }
    const checked = loadConfigFileChecked(filePath);
    // Never clobber a present-but-unparseable file: a merge built on an empty base
    // would destroy the user's (recoverable) content. Only the fatal diagnostics
    // (bad JSON / not an object) block; advisory value/key diagnostics still load.
    if (checked.diagnostic && (checked.diagnostic.kind === 'parse-error' || checked.diagnostic.kind === 'not-object')) {
      return { written: false, reason: 'corrupt', config: checked.config ?? {}, diagnostic: checked.diagnostic };
    }
    const current = checked.config ?? {};
    const next = updater(current);
    // Same-reference return means "nothing to do" — skip the write (parity:
    // saveGlobalConfig's no-op short-circuit).
    if (next === current) {
      return { written: false, reason: 'no-change', config: current };
    }
    safeWriteConfig(filePath, next as Record<string, unknown>);
    return { written: true, reason: 'written', config: next };
  }, {
    // A stolen lock is recoverable — downgrade to a log instead of letting the
    // default throw become an unhandled exception (parity: Claude's onCompromised).
    onCompromised: (err) => coreLog('config', `config write lock compromised for ${lockTarget}: ${err.message}`),
    ...lockOptions,
  });
}

export function cleanupStaleConfigTmp(filePath: string): boolean {
  const tmpPath = filePath + '.tmp';
  try {
    if (!fsHooks.existsSync(tmpPath)) return false;
  } catch {
    return false;
  }
  try {
    const content = fsHooks.readFileSync(tmpPath, 'utf-8') as string;
    const validation = validateConfigJson(content);
    if (validation.valid) {
      try {
        fsHooks.renameSync(tmpPath, filePath);
      } catch {
        fsHooks.writeFileSync(filePath, content, 'utf-8');
        fsHooks.unlinkSync(tmpPath);
      }
    } else {
      fsHooks.unlinkSync(tmpPath);
    }
    return true;
  } catch {
    try { fsHooks.unlinkSync(tmpPath); } catch { /* best effort */ }
    return true;
  }
}
