// Split from config.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';
import { log as coreLog } from './logger';
import type { ProjectConfig } from './config';

/**
 * Resolve the persisted {@link ProjectConfig.additionalDirectories} to a de-duped list
 * of absolute directory paths to add to the tool-access workspace (parity: the reference
 * Code's `permissions.additionalDirectories`). Relative entries resolve against the
 * project root `cwd`; blank/non-string entries are dropped. Unioned with the
 * remembered `/add-dir` store into the PermissionManager allowed read roots at engine
 * setup. Takes the already-MERGED config (the accumulated cross-layer list) so there
 * is ONE place that normalizes the paths.
 */
export function resolveAdditionalDirectories(
  config: ProjectConfig | null | undefined,
  cwd: string,
): string[] {
  const dirs = config?.additionalDirectories;
  if (!Array.isArray(dirs)) return [];
  const out: string[] = [];
  for (const dir of dirs) {
    if (typeof dir !== 'string' || !dir.trim()) continue;
    const resolved = path.isAbsolute(dir) ? path.normalize(dir) : path.resolve(cwd, dir.trim());
    if (!out.includes(resolved)) out.push(resolved);
  }
  return out;
}

/**
 * Whether the plan-approval prompt should offer the extra "accept AND clear context"
 * affordance, resolved off the already-MERGED {@link ProjectConfig}. Parity: the reference
 * Code's `settings.showClearContextOnPlanAccept ?? false` gate in
 * `ExitPlanModePermissionRequest.tsx`. Default OFF — the option is additive/opt-in, so
 * only an explicit `true` (at any config tier) turns it on. Consumed by the exit-plan
 * permission flow in `common/`.
 */
export function shouldShowClearContextOnPlanAccept(
  config: ProjectConfig | null | undefined,
): boolean {
  return config?.showClearContextOnPlanAccept === true;
}

/**
 * Whether the prompt-input footer should render the branch's pull-request status line,
 * resolved off the already-MERGED {@link ProjectConfig}. Parity: the reference's
 * `getGlobalConfig().prStatusFooterEnabled ?? true` (`PromptInputFooterLeftSide.tsx`
 * `isPrStatusEnabled`). Default ON — only an explicit `false` (at any tier) suppresses
 * the PR footer. Consumed by the footer renderer in `cli/` (which already drives
 * `common/src/pr-status.ts`).
 */
export function isPrStatusFooterEnabled(
  config: ProjectConfig | null | undefined,
): boolean {
  return config?.prStatusFooterEnabled !== false;
}

/**
 * Whether voice-input mode is enabled, resolved off the already-MERGED
 * {@link ProjectConfig}. Parity: the reference's `isVoiceModeEnabled()` reading
 * `getInitialSettings().voiceEnabled`. Default OFF — only an explicit
 * {@link ProjectConfig.voiceEnabled} `=== true` (at any tier) turns it on, so the
 * UI surface that advertises voice (e.g. a `/voice` affordance) stays hidden unless
 * the user opts in.
 */
export function isVoiceModeEnabled(
  config: ProjectConfig | null | undefined,
): boolean {
  return config?.voiceEnabled === true;
}

/**
 * Whether the user has explicitly OPTED IN to the auto-allow permission mode,
 * resolved off the already-MERGED {@link ProjectConfig}. Parity: the reference's
 * `hasAutoModeOptIn()`. AMI offers auto-allow opt-OUT by default, so this is purely
 * an INTENT signal: a consumer can tell "wanted auto but it is unavailable
 * (managed-blocked)" (opt-in `true` + {@link isBypassPermissionsModeDisabled}) apart
 * from "never requested auto" (unset). It does NOT by itself grant auto-allow.
 */
export function hasAutoModeOptIn(
  config: ProjectConfig | null | undefined,
): boolean {
  return config?.autoAllowOptIn === true;
}

/**
 * Which diff renderer to use, resolved off the already-MERGED {@link ProjectConfig}.
 * Parity: the reference's `diffTool` setting (`'terminal' | 'auto'`, default `'auto'`).
 * An unset (or non-enum — the enum validator already surfaces a bad value) config
 * resolves to `'auto'`, preserving AMI's existing richest-surface behavior.
 */
export function resolveDiffTool(
  config: ProjectConfig | null | undefined,
): 'terminal' | 'auto' {
  return config?.diffTool === 'terminal' ? 'terminal' : 'auto';
}

/**
 * The default shell backing input-box `!` commands, resolved off the already-MERGED
 * {@link ProjectConfig}. Parity: the reference's `resolveDefaultShell()` reading
 * `settings.defaultShell`. Resolution order: `settings.defaultShell → 'bash'`. AMI does
 * NOT auto-flip Windows to PowerShell (that would break existing bash-hook users), so the
 * default is `'bash'` on every platform; only an explicit `'powershell'` selects it.
 */
export function resolveDefaultShell(
  config: ProjectConfig | null | undefined,
): 'bash' | 'powershell' {
  return config?.defaultShell === 'powershell' ? 'powershell' : 'bash';
}

/**
 * Pick the startup announcement to show, resolved off the already-MERGED
 * {@link ProjectConfig}. Parity: the reference's LogoV2 `companyAnnouncements` pick — on
 * the FIRST startup (`numStartups === 1`) the first entry is shown so the initial
 * announcement is deterministic; on later startups one is chosen at random. Returns
 * `undefined` when no announcements are configured. `rand` (default `Math.random`) is
 * injectable for deterministic tests.
 */
export function pickCompanyAnnouncement(
  config: ProjectConfig | null | undefined,
  numStartups: number,
  rand: () => number = Math.random,
): string | undefined {
  const announcements = config?.companyAnnouncements;
  if (!Array.isArray(announcements) || announcements.length === 0) return undefined;
  if (numStartups === 1) return announcements[0];
  return announcements[Math.floor(rand() * announcements.length)];
}

/**
 * Whether a `dangerouslyDisableSandbox` tool override may be honored, resolved off the
 * already-MERGED {@link ProjectConfig}. Parity: the reference's
 * `SandboxManager.areUnsandboxedCommandsAllowed()` consulted by `shouldUseSandbox()`.
 * Default (unset) → `true`, preserving AMI's existing behavior (the override is honored);
 * an explicit `false` (e.g. a managed/enterprise config) HARD-DISABLES the override so
 * every command is sandboxed regardless of permission mode. Only an exact `=== false`
 * disables it, so a malformed value fails safe (open) the same as unset.
 */
export function areUnsandboxedCommandsAllowed(
  config: ProjectConfig | null | undefined,
): boolean {
  return config?.allowUnsandboxedCommands !== false;
}

/**
 * Config-knob hygiene primitive: return `undefined` (i.e. OMIT the key) when a value
 * equals the resolved default, otherwise the value verbatim. Keeps persisted config
 * MINIMAL so a knob tracks a MOVING default instead of pinning a stale literal — the
 * next read re-derives the current default rather than reading back a redundant value.
 * Parity: the reference's migration/effort write paths that store `undefined` when the
 * chosen value already equals the computed default (e.g. `toPersistableEffort`,
 * `parseUserSpecifiedModel(x) === parseUserSpecifiedModel(default)` pruning). Generic
 * over the value type; both operands may be `undefined` (an unset value stays unset).
 */
export function omitWhenEqualsDefault<T>(
  value: T | undefined,
  defaultValue: T | undefined,
): T | undefined {
  return value === defaultValue ? undefined : value;
}

/**
 * Normalize a chosen effort/thinking LEVEL into its persistable form: store `undefined`
 * (unset) when it equals the model's default level, so the saved setting stays RELATIVE
 * to the model default and switching models naturally re-derives the correct default
 * rather than pinning an absolute level that overrides a differently-defaulted model.
 * Parity: the reference's `EffortCallout` persisting `value === defaultLevel ? undefined :
 * value` via `toPersistableEffort`. Delegates to {@link omitWhenEqualsDefault}; a
 * `defaultLevel` of `undefined` means "no model default known", so any concrete level is
 * kept verbatim.
 */
export function toPersistableEffort(
  value: string | undefined,
  defaultLevel: string | undefined,
): string | undefined {
  return omitWhenEqualsDefault(value, defaultLevel);
}

/**
 * Idempotent, guarded rename of a single config KEY (parity: the reference's
 * `migrate*AtStartup` pattern — e.g. `migrateReplBridgeEnabledToRemoteControlAtStartup`
 * — driven by a `saveGlobalConfig(prev => next)` read-modify-write). Returns a NEW
 * config with `oldKey`'s value copied (through `coerce`) into `newKey` and `oldKey`
 * deleted, but ONLY on the exact `(oldKey present AND newKey unset)` branch; every other
 * case is a no-op that returns the SAME reference, which gives two boundary guards:
 *   1. old key absent  → nothing to migrate (safe to re-run every launch);
 *   2. new key already set → NEVER clobber the user's explicit value.
 * Re-running after a successful migration is a no-op (the old key is gone). Pure — the
 * durable write is the caller's (see {@link runConfigKeyMigrations}).
 */
export function migrateConfigKey(
  config: ProjectConfig,
  oldKey: string,
  newKey: string,
  coerce: (value: unknown) => unknown = (v) => v,
): ProjectConfig {
  const rec = config as Record<string, unknown>;
  if (!(oldKey in rec)) return config;
  if (rec[newKey] !== undefined) return config;
  const next: Record<string, unknown> = { ...rec };
  next[newKey] = coerce(rec[oldKey]);
  delete next[oldKey];
  return next as ProjectConfig;
}

/** One deprecated→successor config-key rename applied at startup by {@link runConfigKeyMigrations}. */
export interface ConfigKeyMigration {
  /** The deprecated key to read + delete. */
  from: string;
  /** The successor key to write (only when currently unset). */
  to: string;
  /** Optional value transform (e.g. `Boolean`) applied while copying `from`→`to`. */
  coerce?: (value: unknown) => unknown;
}

/**
 * The ordered registry of deprecated config-key renames. APPEND a new
 * {@link ConfigKeyMigration} here whenever a config key is renamed/deprecated so an
 * existing user's saved value is carried forward (coerced) instead of being silently
 * orphaned — exactly as new directory/value migrations are appended to the startup set.
 * Empty until the first key deprecation; the runner below iterates it every launch.
 */
export const DEPRECATED_CONFIG_KEY_MIGRATIONS: ReadonlyArray<ConfigKeyMigration> = [];

/**
 * Locate the first top-level `[ … ]` in a JSONC string (string/comment aware).
 * Exported so residual both-arms (leading whitespace skip, unclosed → null) can
 * be forced directly — {@link addItemToJSONCArray} only reaches this helper when
 * `parseJsonc` already returned an array, which masks the failure arms.
 */
export function findTopLevelArrayBrackets(
  s: string,
): { open: number; close: number } | null {
  let inString = false;
  let escape = false;
  let open = -1;
  let depth = 0;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (escape) { escape = false; continue; }
    if (inString) {
      if (ch === '\\') escape = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') { inString = true; continue; }
    if (ch === '/' && s[i + 1] === '/') { while (i < s.length && s[i] !== '\n') i++; continue; }
    if (ch === '/' && s[i + 1] === '*') { i += 2; while (i < s.length && !(s[i] === '*' && s[i + 1] === '/')) i++; i++; continue; }
    if (open === -1) {
      if (ch === '[') { open = i; depth = 1; continue; }
      if (/\s/.test(ch!)) continue;
      return null; // first significant token is not '[' → not an array document
    }
    if (ch === '[' || ch === '{') depth++;
    else if (ch === ']' || ch === '}') {
      depth--;
      if (depth === 0) return { open, close: i };
    }
  }
  return null;
}

/**
 * Comment-preserving insertion of `newItem` into the first top-level array of `clean`.
 * Returns the edited text, or `null` when the array brackets cannot be located (caller
 * falls back to a full re-stringify).
 */
export function insertIntoJsoncArray(
  clean: string,
  isEmpty: boolean,
  newItem: unknown,
): string | null {
  const brackets = findTopLevelArrayBrackets(clean);
  if (!brackets) return null;
  const { open, close } = brackets;
  // Indentation: the whitespace on the line that holds the closing bracket, and one
  // level (two spaces) deeper for the element itself.
  const closeLineStart = clean.lastIndexOf('\n', close - 1) + 1;
  const closeIndentRaw = clean.slice(closeLineStart, close);
  const closeIndent = /^\s*$/.test(closeIndentRaw) ? closeIndentRaw : '';
  const elementIndent = closeIndent + '  ';
  const itemText = JSON.stringify(newItem, null, 2)
    .split('\n')
    .map((line, idx) => (idx === 0 ? line : elementIndent + line))
    .join('\n');
  if (isEmpty) {
    return (
      clean.slice(0, open + 1) +
      '\n' + elementIndent + itemText + '\n' + closeIndent +
      clean.slice(close)
    );
  }
  // Non-empty: find the end of the last VALUE character (skipping whitespace, commas,
  // and comments) so a comma + the new element can be spliced in after it, preserving
  // any trailing comment/whitespace that follows.
  let lastValueEnd = open + 1;
  let inString = false;
  let escape = false;
  for (let i = open + 1; i < close; i++) {
    const ch = clean[i];
    if (escape) { escape = false; lastValueEnd = i + 1; continue; }
    if (inString) {
      if (ch === '\\') escape = true;
      else if (ch === '"') inString = false;
      lastValueEnd = i + 1;
      continue;
    }
    if (ch === '"') { inString = true; lastValueEnd = i + 1; continue; }
    if (ch === '/' && clean[i + 1] === '/') { while (i < close && clean[i] !== '\n') i++; i--; continue; }
    if (ch === '/' && clean[i + 1] === '*') { i += 2; while (i < close && !(clean[i] === '*' && clean[i + 1] === '/')) i++; i++; continue; }
    if (/\s/.test(ch!) || ch === ',') continue;
    lastValueEnd = i + 1;
  }
  return (
    clean.slice(0, lastValueEnd) + ',' +
    '\n' + elementIndent + itemText +
    clean.slice(lastValueEnd, close) +
    clean.slice(close)
  );
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
 * Parity: the reference's `validateBoundedIntEnvVar` (envValidation.ts) upper-limit
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
// Unified tool-output spill threshold (parity: the reference toolResultStorage.ts)
// ---------------------------------------------------------------------------

/**
 * Built-in GLOBAL default ceiling (chars) for a tool result before it is spilled to
 * a file. Parity: the reference's `DEFAULT_MAX_RESULT_SIZE_CHARS` — the single default
 * against which each tool's own declared cap is clamped, so one tunable value governs
 * every tool that has no bespoke cap (web_fetch, PDF extraction, …). Distinct from the
 * per-tool {@link ProjectConfig.bashOutputMaxChars}/{@link ProjectConfig.taskOutputMaxChars}
 * knobs, which stay their own thing.
 */
export const DEFAULT_TOOL_OUTPUT_MAX_CHARS = 50_000;

/** Env override for the global tool-output cap (validated + bounded), mirroring the
 *  per-tool `TASK_MAX_OUTPUT_LENGTH` env knob. */
export const TOOL_OUTPUT_MAX_CHARS_ENV = 'AMI_TOOL_OUTPUT_MAX_CHARS';
