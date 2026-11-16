import * as fs from 'fs';
import * as path from 'path';
import { log as coreLog } from './logger';
import { filterSettingsEnv, isHostManagedProvider, type EnvTrustTier, type EnvRejectReason } from './env-protection';
import { validateEngineConfig, type EngineConfig } from './types';
import { withFileLockSync, type FileLockSyncOptions } from './utils/file-lock';

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
  /** Override the composer-hint surface: inject custom hints / drop the built-ins. */
  composerHintsOverride?: { hints?: Array<{ key: string; label: string; learnedAfter: number }>; excludeDefault?: boolean };
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

/** Read + JSON-parse a managed file, failing closed on any read/parse error. */
function parseManagedFile(configPath: string): ProjectConfig {
  // Tamper-resistance: refuse a symlinked or group/world-writable managed-policy
  // file before trusting its contents (fail-closed on an untrusted source).
  assertTrustedManagedFile(configPath);
  try {
    const raw = fsHooks.readFileSync(configPath, 'utf-8') as string;
    return JSON.parse(stripJsoncComments(stripBom(raw))) as ProjectConfig;
  } catch (err) {
    // A broken enterprise policy must fail closed, not be silently ignored.
    throw new ManagedConfigError(`Managed config at ${configPath} failed to parse: ${err instanceof Error ? err.message : String(err)}`);
  }
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

/** A problem found while loading a config file (distinct from "file absent"). */
export interface ConfigDiagnostic {
  path: string;
  kind: 'parse-error' | 'not-object' | 'invalid-value' | 'unknown-key';
  message: string;
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
  ['maxWebSearchesPerSession', 'number'],
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
  return issues;
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
 * Load a config file, distinguishing three outcomes:
 *  - file absent / unreadable → `{ config: null }` (no diagnostic);
 *  - present but corrupt (bad JSON / not an object) → `{ config: null, diagnostic }`,
 *    and the corrupt file is backed up so it is not lost;
 *  - valid → `{ config }`.
 */
export function loadConfigFileChecked(configPath: string): { config: ProjectConfig | null; diagnostic?: ConfigDiagnostic } {
  let raw: string;
  try {
    if (!fsHooks.existsSync(configPath)) return { config: null };
    raw = fsHooks.readFileSync(configPath, 'utf-8') as string;
  } catch {
    // Unreadable (permissions, race) — treat as absent rather than corrupt.
    return { config: null };
  }
  // Strip a leading UTF-8 BOM (e.g. config.json written by PowerShell 5.x or some
  // editors) before parsing — JSON.parse rejects a leading U+FEFF, which would
  // otherwise misreport an otherwise-valid file as corrupt.
  raw = stripBom(raw);
  let parsed: unknown;
  try {
    parsed = JSON.parse(stripJsoncComments(raw));
  } catch (err) {
    const backupPath = backupCorruptConfig(configPath, raw) ?? undefined;
    const message = err instanceof Error ? err.message : String(err);
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
    const message = `${unknownKeys.length} unknown config key(s): ${unknownKeys.join(', ')}`;
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
  const paths = [
    path.join(cwd, '.ami', 'config.json'),
    path.join(cwd, '.ami', 'config.local.json'),
    ...(home ? [path.join(home, '.ami', 'config.json')] : []),
  ];
  const diagnostics: ConfigDiagnostic[] = [];
  for (const p of paths) {
    const { diagnostic } = loadConfigFileChecked(p);
    if (diagnostic) {
      diagnostics.push({ ...diagnostic, backupPath: diagnostic.backupPath ?? findMostRecentBackup(p) ?? undefined });
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

// ---------------------------------------------------------------------------
// ConfigService — hot-reload config watcher
// ---------------------------------------------------------------------------

export type ConfigChangeListener = (config: ProjectConfig, prev: ProjectConfig) => void;

export class ConfigService {
  private config: ProjectConfig;
  private configPath: string;
  private watcher: fs.FSWatcher | null = null;
  private listeners: ConfigChangeListener[] = [];
  private debounceTimer: ReturnType<typeof setTimeout> | null = null;
  private onConfigChangeCallbacks: Array<(source: string, path: string) => void> = [];

  constructor(cwd: string) {
    this.configPath = path.join(cwd, '.ami', 'config.json');
    this.config = loadConfigFile(this.configPath) ?? {};
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
    if (this.watcher) return;

    const dir = path.dirname(this.configPath);
    try {
      if (!fsHooks.existsSync(dir)) return;

      this.watcher = fsHooks.watch(dir, (eventType, filename) => {
        if (filename !== 'config.json') return;

        // Swallow the echo of AMI's own write so it never self-triggers a reload
        // + config-change notification (parity: changeDetector consumeInternalWrite).
        if (consumeInternalWrite(this.configPath)) return;

        if (this.debounceTimer) clearTimeout(this.debounceTimer);
        this.debounceTimer = setTimeout(() => {
          this.reload();
        }, 100);
      });

      // unref so the watcher doesn't prevent process exit in test environments
      this.watcher.unref();

      this.watcher.on('error', () => {
        this.stop();
      });
    } catch {
      // Directory doesn't exist or not watchable
    }
  }

  reload(): boolean {
    const prev = this.config;
    const next = loadConfigFile(this.configPath);
    if (!next) return false;

    const changed = JSON.stringify(prev) !== JSON.stringify(next);
    if (changed) {
      this.config = next;
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
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
      this.debounceTimer = null;
    }
    if (this.watcher) {
      this.watcher.close();
      this.watcher = null;
    }
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
    if (layer.superinference !== undefined) merged.superinference = layer.superinference;
    if (layer.skill !== undefined) merged.skill = layer.skill;
    if (layer.bashOutputMaxChars !== undefined) merged.bashOutputMaxChars = layer.bashOutputMaxChars;
    if (layer.taskOutputMaxChars !== undefined) merged.taskOutputMaxChars = layer.taskOutputMaxChars;
    if (layer.cacheTtl !== undefined) merged.cacheTtl = layer.cacheTtl;
    if (layer.safeMode !== undefined) merged.safeMode = layer.safeMode;
    if (layer.maxWebSearchesPerSession !== undefined) merged.maxWebSearchesPerSession = layer.maxWebSearchesPerSession;
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
    if (layer.tipsOverride !== undefined) merged.tipsOverride = layer.tipsOverride;
    if (layer.composerHintsEnabled !== undefined) merged.composerHintsEnabled = layer.composerHintsEnabled;
    if (layer.composerHintsOverride !== undefined) merged.composerHintsOverride = layer.composerHintsOverride;
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
};

/**
 * Mockable JSON seam (ESM-safe). Tests swap `jsonHooks.parse` via `mock.method`
 * to exercise the non-Error failure arm below. Defaults to the global JSON.parse.
 */
export const jsonHooks = { parse: JSON.parse as (text: string) => unknown };

export function validateConfigJson(content: string): { valid: boolean; error?: string } {
  let parsed: unknown;
  try {
    parsed = jsonHooks.parse(content);
  } catch (err) {
    return { valid: false, error: `Invalid JSON: ${err instanceof Error ? err.message : String(err)}` };
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
  try {
    const dir = path.dirname(target);
    if (!fsHooks.existsSync(dir)) {
      fsHooks.mkdirSync(dir, { recursive: true });
    }
    fsHooks.writeFileSync(tmpPath, json, 'utf-8');
    try {
      fsHooks.renameSync(tmpPath, target);
    } catch {
      fsHooks.writeFileSync(target, json, 'utf-8');
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
