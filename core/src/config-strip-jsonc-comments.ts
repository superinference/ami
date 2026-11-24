// Split from config.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';
import type { ProjectConfig } from './config';

/**
 * One entry of the MCP allowlist ({@link ProjectConfig.allowedMcpServers}). A plain
 * string is a server NAME or upstream URL (with optional `*`-wildcards). A
 * provenance-scoped object trusts a server by the plugin that produced it rather
 * than its arbitrary local name (the reference-parity channelAllowlist): `{ pluginSource }`
 * matches {@link McpServerConfig.pluginSource}, and `{ marketplace, plugin }` matches
 * the `<plugin>@<marketplace>` provenance convention. Structurally a subset of the
 * manager's `McpPolicyEntry`, so it threads straight into `setAllowedServers`.
 */
export type McpAllowedServerEntry =
  | string
  | { pluginSource: string }
  | { marketplace: string; plugin: string };

/** True when `v` is a valid {@link McpAllowedServerEntry} (string or provenance object). */
export function isMcpAllowedServerEntry(v: unknown): v is McpAllowedServerEntry {
  if (typeof v === 'string') return true;
  if (v === null || typeof v !== 'object' || Array.isArray(v)) return false;
  const o = v as Record<string, unknown>;
  if (typeof o.pluginSource === 'string') return true;
  if (typeof o.marketplace === 'string' && typeof o.plugin === 'string') return true;
  return false;
}

/**
 * Object form of {@link ProjectConfig.statusLine} (Claude-Code-compatible
 * `settings.statusLine`): the shell `command` whose stdout is rendered, plus an
 * optional horizontal `padding` (spaces per side, CC's `<Box paddingX>`). The bare
 * string form of `statusLine` is equivalent to `{ command }` with no padding.
 *
 * `type` is the reference's status-line TYPE discriminator (`{ "type": "command",
 * "command": "..." }`): the ONLY shape CC's own `statuslineSetup` tool writes. AMI
 * renders the `command` directly, so `type` is accepted-and-ignored (forward-compat —
 * an unrecognized future type never rejects the block), which makes a `settings.json`
 * copied verbatim from the reference parse unchanged rather than being lost. Declaring it
 * here (rather than letting it fall through as an untyped extra key) documents the
 * supported CC drop-in shape and keeps the typed contract honest.
 */
export interface StatusLineConfig {
  /** Shell command whose stdout is rendered as the status line. */
  command: string;
  /** Horizontal padding (spaces per side) applied to each rendered line. */
  padding?: number;
  /**
   * the reference status-line kind discriminator (CC only ever emits `'command'`).
   * Accepted for drop-in compatibility and otherwise ignored — AMI always renders the
   * `command`'s stdout. A non-string value is surfaced by {@link validateProjectConfigValues}.
   */
  type?: string;
}

/**
 * Normalize either {@link ProjectConfig.statusLine} shape — a bare command string or
 * the `{ command, padding }` object — into a single `{ command?, padding? }` result so
 * every consumer reads one shape. A blank/whitespace command resolves to `undefined`
 * (no status line). A non-finite/≤0 object `padding` is dropped (treated as unset so
 * the env/default fallback applies). Parity: the reference accepting `statusLine` as
 * either a command string or a `{ command, padding }` object.
 */
export function normalizeStatusLine(
  value: string | StatusLineConfig | undefined,
): { command?: string; padding?: number } {
  if (value === undefined) return {};
  if (typeof value === 'string') {
    const command = value.trim();
    return command ? { command } : {};
  }
  const command = typeof value.command === 'string' && value.command.trim() ? value.command.trim() : undefined;
  const padding = typeof value.padding === 'number' && Number.isFinite(value.padding) && value.padding > 0 ? value.padding : undefined;
  return { ...(command ? { command } : {}), ...(padding !== undefined ? { padding } : {}) };
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
 * robustness is one source. Parity: the reference's `stripBOM` leaf.
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
 * {@link stripJsoncComments}. Parity: the reference's JSONC parser
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

/** LRU cap (entries) on {@link parseJsonc}'s content-keyed parse memo. Parity: the reference's safeParseJSON LRU(50). */
export const JSONC_PARSE_CACHE_MAX_ENTRIES = 50;

/** Only memoize small payloads — a huge config is cheaper to re-parse than to retain + clone. */
export const JSONC_PARSE_CACHE_MAX_BYTES = 100_000;

/**
 * One in-memory per-tier config read cache keyed by file IDENTITY (absolute path +
 * mtime + size), with NEGATIVE caching of absent files. A repeated load of an
 * unchanged file is a pure memory hit (no readFileSync); any content change — our own
 * {@link safeWriteConfig} OR another instance's write — changes mtime/size, misses,
 * and re-reads, so a stale snapshot is never served. The cached tree is pristine and
 * every caller receives a `structuredClone`, so a caller mutating its config cannot
 * poison the cache. Parity: the reference's in-memory global-config cache + settingsCache's
 * miss(undefined)/negative(null) distinction.
 */
export interface TierConfigCacheEntry {
  /** File mtime in ms; `-1` when the file is absent (negative-cache marker). */
  mtimeMs: number;
  /** File size in bytes; `-1` when absent. */
  size: number;
  /** Parsed config, or `null` for "no config at this path" (absent/corrupt). */
  config: ProjectConfig | null;
}

// ---------------------------------------------------------------------------
// Loaders
// ---------------------------------------------------------------------------

/**
 * Absolute path to a project's config file (`<cwd>/.ami/config.json`). Single source of
 * truth so the project-scoped reader ({@link loadProjectConfig}) and writer
 * ({@link saveProjectConfig}) target the same file.
 */
export function getProjectConfigPath(cwd: string): string {
  return path.join(cwd, '.ami', 'config.json');
}

/**
 * Absolute path to a project's LOCAL config file (`<cwd>/.ami/config.local.json`) — the
 * per-user, typically-gitignored override tier. Single source of truth shared by
 * {@link loadLocalConfig} and {@link saveLocalConfig}.
 */
export function getLocalConfigPath(cwd: string): string {
  return path.join(cwd, '.ami', 'config.local.json');
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
 * Opt-out seam that relaxes the STRICTER trust check applied to a user-relocatable
 * {@link SUPERINFERENCE_MANAGED_CONFIG} override (the foreign-owner refusal added by
 * {@link assertTrustedManagedFile} when `relocatable` is set). CI and non-standard dev
 * deployments that deliberately point the override at a fixture owned by neither root nor
 * the current user set this to `1`/`true` to keep using the override; the base symlink +
 * world-writable refusals ALWAYS apply regardless. Parity-additive: it restricts the
 * AMI-only env override to an admin-grade trust boundary (the reference's managed path is a
 * FIXED admin-only location the user cannot relocate) while keeping the override usable
 * behind an explicit, auditable seam.
 */
export const MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV = 'SUPERINFERENCE_MANAGED_CONFIG_ALLOW_UNTRUSTED';

/** Whether the {@link MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV} opt-out seam is engaged. */
export function managedOverrideUntrustedAllowed(env: NodeJS.ProcessEnv = process.env): boolean {
  const v = env[MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV];
  return v === '1' || v === 'true';
}

/** Merge one managed drop-in over an accumulated base (later wins; env key-merges). */
export function mergeManagedLayer(base: ProjectConfig, next: ProjectConfig): ProjectConfig {
  const merged: ProjectConfig = { ...base, ...next };
  if (base.env || next.env) merged.env = { ...base.env, ...next.env };
  return merged;
}

/**
 * One-entry memo of the merged managed-config RESULT keyed by a cheap file-identity
 * SIGNATURE over every managed source (parity: the reference's `mdmCache` behind the
 * explicit `clearMdmSettingsCache` clear hook). {@link loadManagedConfig} is re-invoked
 * on EVERY config-waterfall merge — `getMergedConfig`, the detached config path, the
 * `/doctor` sweep, the managed-tier change detector — and each call previously re-ran
 * the full `existsSync` + `readFileSync` + `parseJsonc` chain across the candidate
 * source(s) and their `config.d/` drop-ins. Managed policy is effectively static within
 * a session, so this memoizes the parsed result and serves repeats as a memory hit.
 *
 * Invalidation is twofold and never serves a stale snapshot:
 *  - IDENTITY: the signature folds in every candidate file's `mtime+size` (and the
 *    drop-in dirs' listings + per-entry identity), so ANY on-disk change — an admin
 *    editing the policy, AMI's own write, a drop-in added/removed — changes the
 *    signature, misses the cache, and re-reads fresh (the managed-tier watcher's
 *    change detection therefore keeps working through the cache).
 *  - EXPLICIT: {@link clearManagedConfigCache} is fanned out from
 *    {@link resetConfigCache} (fired by {@link safeWriteConfig} and the other write
 *    paths), covering the inputs an `mtime+size` identity cannot observe (an OS-native
 *    MDM/registry policy change), exactly as the reference clears `mdmCache` on a write.
 *
 * SAFE under the fs-mocked managed test suites: computing the signature requires a real
 * {@link fsHooks.statSync} on each PRESENT source; when that stat cannot be taken (the
 * mock seams stub `existsSync`/`readFileSync` but not `statSync`, so a fake path stats
 * as absent/throws) the call is treated as UNCACHEABLE — it reads fresh and stores
 * nothing — so a mocked scenario never serves a cached result from another `it`.
 */
export interface ManagedConfigCacheEntry {
  /** File-identity signature of every managed source at the time of the read. */
  signature: string;
  /** The merged managed config (or `null` for "no managed policy present"). */
  config: ProjectConfig | null;
}

/** A single field-level value-validation problem in an otherwise-parseable config. */
export interface ConfigValueIssue {
  /** The offending config key (e.g. "permissionMode", "maxTurns"). */
  field: string;
  /** Human-readable reason the value is invalid. */
  message: string;
  /** The offending value, so a caller can surface "you supplied X" to the user
   *  (parity: the reference's ValidationError.invalidValue). */
  value?: unknown;
  /**
   * A short, actionable fix suggestion for THIS field+kind (parity: the reference's
   * `getValidationTip`). Tells the user HOW to correct the value (e.g. "Use true
   * or false without quotes", "Valid values: ..."), not just what is wrong. Absent
   * when no specific tip applies. Surfaced alongside {@link message} by the loader
   * diagnostic + `/doctor`. */
  tip?: string;
  /**
   * A documentation URL for THIS field so a diagnostic can render an actionable
   * "Learn more: <url>" affordance, turning a bare validation error into a pointer
   * to the fix (parity: the reference's `ValidationErrorsList` per-error `docLink` +
   * its "Learn more:" line). Resolved by {@link getConfigValidationDocLink} and
   * deduplicated across issues by {@link formatConfigValueIssues} so the same anchor
   * is never repeated N times. Always populated. */
  docLink?: string;
}

/**
 * Describe the JSON runtime kind of a value for a diagnostic message, distinguishing
 * `null` and `array` from a plain `object` (mirroring JSON's own value types). Shared
 * by the "not an object" and per-field type diagnostics so the wording stays one source.
 */
export function describeJsonType(value: unknown): string {
  return value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value;
}

/**
 * Recovery guidance appended to every JSON syntax-error message AMI surfaces for a
 * config file, so the user is told HOW to fix a malformed file rather than just that
 * it is malformed. One source, reused by {@link loadConfigFileChecked} (the loader
 * diagnostic) and {@link validateConfigJson} (the stale-tmp / config-tool writer path)
 * so the wording never drifts. Parity: the reference's root-level JSON syntax-error tip
 * ("Check for missing commas, unmatched brackets, or trailing commas…").
 */
export const JSON_SYNTAX_RECOVERY_HINT =
  'Check for missing commas, unmatched brackets, or trailing commas. Use a JSON validator to identify the exact syntax error.';

/**
 * The config LAYER a value/diagnostic originates from, in merge-priority order
 * (`cli` highest, `managed` lowest). Mirrors the {@link mergeConfigs} layer stack and
 * the {@link CONFIG_ENV_TIERS} env-trust tiers, so every provenance-aware surface names
 * the same set of sources. Parity: the reference's `SettingSource` enum.
 */
export const CONFIG_SOURCES = ['cli', 'env', 'local', 'project', 'global', 'managed'] as const;

export type ConfigSource = (typeof CONFIG_SOURCES)[number];

/**
 * Human-readable provenance name for each {@link ConfigSource}, so a diagnostic / `/doctor`
 * line reads "Invalid project config …" or "Invalid managed config …" instead of leaving
 * the user to infer which file a bare path belongs to (`.ami/config.local.json` vs
 * `~/.ami/config.json` vs `/etc/...`). An introspection surface over the config waterfall.
 * Parity: the reference's `getSettingSourceName` (e.g. `localSettings` → "project, gitignored").
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
