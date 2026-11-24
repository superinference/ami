// Split from config.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import { log as coreLog } from './logger';
import { validateEngineConfig, type EngineConfig } from './types';
import { describeJsonType } from './config-strip-jsonc-comments';
import type { ConfigSource, ConfigValueIssue, McpAllowedServerEntry } from './config-strip-jsonc-comments';
import { PROJECT_CONFIG_ENUMS, PROJECT_CONFIG_FIELD_TYPES, configFieldTypeLabel, getConfigValidationDocLink, getConfigValidationTip, matchesConfigFieldType } from './config-config-diagnostic';
import type { ConfigDiagnostic, ConfigFieldType, ConfigSchemaProperty } from './config-config-diagnostic';
import type { ProjectConfig } from './config';

/** Map a {@link ConfigFieldType} to its JSON-Schema property node (base shape only). */
export function configFieldTypeToSchemaNode(type: ConfigFieldType): ConfigSchemaProperty {
  switch (type) {
    case 'string': return { type: 'string', description: '' };
    case 'number': return { type: 'number', description: '' };
    case 'boolean': return { type: 'boolean', description: '' };
    case 'string[]': return { type: 'array', items: { type: 'string' }, description: '' };
    case 'object': return { type: 'object', description: '' };
    // The MCP allowlist is a heterogeneous union (string | provenance object); leave the
    // item schema permissive so either shape validates (parity with the governed schema's
    // permissive fallback for union-typed keys).
    case 'mcpAllowlist': return { type: 'array', items: {}, description: '' };
  }
}

/**
 * Validate the VALUES of a parsed config (the file parsed as a JSON object, but a
 * field may still hold the wrong type, an out-of-range number, or an invalid enum
 * string — the TypeScript type gives no runtime guarantee). Returns a field-level
 * issue list (each carrying the offending `value`) so the loader can surface "your
 * config.json has a bad value" instead of silently accepting it (parity: the reference
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
      issues.push({ field: field as string, message: `must be ${configFieldTypeLabel(type)} (got ${describeJsonType(value)})`, value, tip: getConfigValidationTip(field as string, { kind: 'type', type }) });
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
  for (const field of ['maxTurns', 'tokenBudget', 'maxBudgetUsd', 'maxWebSearchesPerSession', 'bashOutputMaxChars', 'taskOutputMaxChars', 'maxSkillProgressMessages'] as const) {
    if (config[field] === undefined || typeInvalid.has(field)) continue;
    const r = validateEngineConfig({ [field]: config[field] } as Partial<EngineConfig>);
    if (!r.valid) issues.push({ field, message: r.warnings.join('; '), value: config[field], tip: getConfigValidationTip(field, { kind: 'range' }) });
  }
  // 3b. cleanupPeriodDays BOUNDS — a session-transcript retention WINDOW in days, not
  //     a generic EngineConfig numeric knob, so it is validated here rather than via the
  //     loop above. Valid domain: a non-negative WHOLE number of days (0 disables
  //     cleanup; a positive integer keeps that many days). A negative value would
  //     silently behave as "disabled", and a fractional value mis-rounds the cutoff — so
  //     both are surfaced with the field-specific retention tip. Skip when the base type
  //     was already flagged (no double-report). Parity: the reference's cleanupPeriodDays
  //     `too_small`/bounds validation with the data-retention validation tip.
  if (config.cleanupPeriodDays !== undefined && !typeInvalid.has('cleanupPeriodDays')) {
    const d = config.cleanupPeriodDays;
    if (!Number.isInteger(d) || d < 0) {
      issues.push({ field: 'cleanupPeriodDays', message: `must be a non-negative whole number of days (0 disables cleanup) (got ${JSON.stringify(d)})`, value: d, tip: getConfigValidationTip('cleanupPeriodDays', { kind: 'range' }) });
    }
  }
  // 4. Nested `superinference` flags must be booleans when present. Step 1 only guards
  //    that `superinference` is an object; a non-boolean nested flag (e.g.
  //    `{ enforcing: "true" }`) would otherwise flow TRUTHILY into the reflection gate
  //    and silently force-enable it. Skip when the outer object type was already flagged.
  //    Parity: the reference validates nested settings-schema value types, not just the top level.
  if (config.superinference !== undefined && !typeInvalid.has('superinference')) {
    const si = config.superinference as Record<string, unknown>;
    for (const flag of ['enforcing', 'useLLMCritic'] as const) {
      if (si[flag] !== undefined && typeof si[flag] !== 'boolean') {
        issues.push({ field: `superinference.${flag}`, message: `must be a boolean (got ${describeJsonType(si[flag])})`, value: si[flag], tip: getConfigValidationTip(`superinference.${flag}`, { kind: 'type', type: 'boolean' }) });
      }
    }
  }
  // 4b. Nested `preferencesShown` flags must be booleans, and `noticeSeenCounts` entries
  //     non-negative numbers, when present. Step 1 only guards that each outer value is an
  //     object; a non-boolean flag or a non-number/negative count would otherwise flow into
  //     the one-time-flag / notice-gate logic unchecked. Skip when the outer object type was
  //     already flagged. Parity: the reference validates nested settings-schema value types.
  if (config.preferencesShown !== undefined && !typeInvalid.has('preferencesShown')) {
    const flags = config.preferencesShown as Record<string, unknown>;
    for (const [name, v] of Object.entries(flags)) {
      if (typeof v !== 'boolean') {
        issues.push({ field: `preferencesShown.${name}`, message: `must be a boolean (got ${describeJsonType(v)})`, value: v, tip: getConfigValidationTip(`preferencesShown.${name}`, { kind: 'type', type: 'boolean' }) });
      }
    }
  }
  if (config.noticeSeenCounts !== undefined && !typeInvalid.has('noticeSeenCounts')) {
    const counts = config.noticeSeenCounts as Record<string, unknown>;
    for (const [name, v] of Object.entries(counts)) {
      if (typeof v !== 'number' || !Number.isFinite(v) || v < 0) {
        issues.push({ field: `noticeSeenCounts.${name}`, message: `must be a non-negative number (got ${JSON.stringify(v)})`, value: v, tip: getConfigValidationTip(`noticeSeenCounts.${name}`, { kind: 'type', type: 'number' }) });
      }
    }
  }
  // `lastSessionMetrics` is a `name -> counter` rollup (parity: the reference's StatsProvider
  // flush). Step 1 only guards that the outer value is an object; each nested counter must
  // be a finite non-negative number, mirroring the `noticeSeenCounts` nested check, so a
  // corrupt counter is reported rather than silently carried forward.
  if (config.lastSessionMetrics !== undefined && !typeInvalid.has('lastSessionMetrics')) {
    const metrics = config.lastSessionMetrics as Record<string, unknown>;
    for (const [name, v] of Object.entries(metrics)) {
      if (typeof v !== 'number' || !Number.isFinite(v) || v < 0) {
        issues.push({ field: `lastSessionMetrics.${name}`, message: `must be a non-negative number (got ${JSON.stringify(v)})`, value: v, tip: getConfigValidationTip(`lastSessionMetrics.${name}`, { kind: 'type', type: 'number' }) });
      }
    }
  }
  // 4b-bis. Nested `githubRepoPaths` entries must each be a `string[]` (the MRU list of
  //     known clone paths) when present. Step 1 only guards that the outer value is an
  //     object; a non-array (or non-string element) would otherwise flow into the MRU
  //     update/prune logic and corrupt the registry. Skip when the outer type was flagged.
  if (config.githubRepoPaths !== undefined && !typeInvalid.has('githubRepoPaths')) {
    const entries = config.githubRepoPaths as Record<string, unknown>;
    for (const [name, v] of Object.entries(entries)) {
      if (!matchesConfigFieldType(v, 'string[]')) {
        issues.push({ field: `githubRepoPaths.${name}`, message: `must be a string[] (got ${describeJsonType(v)})`, value: v, tip: getConfigValidationTip(`githubRepoPaths.${name}`, { kind: 'type', type: 'string[]' }) });
      }
    }
  }
  // 4c. Nested `policyRestrictions` entries must each be a `{ allowed: boolean }`
  //     object (parity: the reference's policyLimits `restrictions` schema). Step 1 only
  //     guards that the outer value is an object; a malformed entry (a non-object, or
  //     `{ allowed: "yes" }`) would otherwise flow into the fail-open `isPolicyAllowed`
  //     gate and be silently treated as "allowed", masking a mistyped deny. Skip when
  //     the outer object type was already flagged.
  if (config.policyRestrictions !== undefined && !typeInvalid.has('policyRestrictions')) {
    const entries = config.policyRestrictions as Record<string, unknown>;
    for (const [name, v] of Object.entries(entries)) {
      if (v === null || typeof v !== 'object' || Array.isArray(v)) {
        issues.push({ field: `policyRestrictions.${name}`, message: `must be a { allowed: boolean } object (got ${describeJsonType(v)})`, value: v, tip: getConfigValidationTip(`policyRestrictions.${name}`, { kind: 'type', type: 'object' }) });
      } else if (typeof (v as Record<string, unknown>).allowed !== 'boolean') {
        const allowed = (v as Record<string, unknown>).allowed;
        issues.push({ field: `policyRestrictions.${name}.allowed`, message: `must be a boolean (got ${describeJsonType(allowed)})`, value: allowed, tip: getConfigValidationTip(`policyRestrictions.${name}.allowed`, { kind: 'type', type: 'boolean' }) });
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
    if (wt.tmux !== undefined && !matchesConfigFieldType(wt.tmux, 'boolean')) {
      issues.push({ field: 'worktree.tmux', message: `must be a boolean (got ${describeJsonType(wt.tmux)})`, value: wt.tmux, tip: getConfigValidationTip('worktree.tmux', { kind: 'type', type: 'boolean' }) });
    }
  }
  // 5b. Nested `webFetch.allowedDomains` / `webFetch.blockedDomains` must each be a
  //     string[] when present. Step 1 only guards that `webFetch` is an object; a
  //     non-array (or non-string element) would otherwise flow into the web_fetch
  //     domain-policy matcher and silently no-op. Skip when the outer object type was
  //     already flagged.
  if (config.webFetch !== undefined && !typeInvalid.has('webFetch')) {
    const wf = config.webFetch as Record<string, unknown>;
    for (const key of ['allowedDomains', 'blockedDomains'] as const) {
      if (wf[key] !== undefined && !matchesConfigFieldType(wf[key], 'string[]')) {
        issues.push({ field: `webFetch.${key}`, message: `must be a string[] (got ${describeJsonType(wf[key])})`, value: wf[key], tip: getConfigValidationTip(`webFetch.${key}`, { kind: 'type', type: 'string[]' }) });
      }
    }
  }
  // 6. `statusLine` union: a bare command string OR a `{ command, padding }` object
  //    (Claude-Code-compatible). It is NOT in the single-type table above because the
  //    type depends on the shape; validate both forms here so a ported CC object config
  //    is ACCEPTED while a genuinely malformed value (array/number, or an object with a
  //    non-string command / non-number padding) is still surfaced.
  if (config.statusLine !== undefined) {
    const sl = config.statusLine as unknown;
    if (typeof sl !== 'string' && !isPlainObject(sl)) {
      issues.push({ field: 'statusLine', message: `must be a string or a { command, padding } object (got ${describeJsonType(sl)})`, value: sl, tip: 'Use a command string, e.g. "statusline.sh", or an object { "command": "...", "padding": 2 }.' });
    } else if (isPlainObject(sl)) {
      if (typeof sl.command !== 'string') {
        issues.push({ field: 'statusLine.command', message: `must be a string (got ${describeJsonType(sl.command)})`, value: sl.command, tip: getConfigValidationTip('statusLine.command', { kind: 'type', type: 'string' }) });
      }
      if (sl.padding !== undefined && !matchesConfigFieldType(sl.padding, 'number')) {
        issues.push({ field: 'statusLine.padding', message: `must be a number (got ${describeJsonType(sl.padding)})`, value: sl.padding, tip: getConfigValidationTip('statusLine.padding', { kind: 'type', type: 'number' }) });
      }
      // the reference's `{ type: 'command', command }` drop-in shape: `type` is accepted
      // and ignored, but a non-string `type` is a malformed block — surface it (the
      // valid `command` still loads; a bad `type` is dropped by salvage) rather than
      // silently tolerating `type: 123`.
      if (sl.type !== undefined && typeof sl.type !== 'string') {
        issues.push({ field: 'statusLine.type', message: `must be a string (got ${describeJsonType(sl.type)})`, value: sl.type, tip: getConfigValidationTip('statusLine.type', { kind: 'type', type: 'string' }) });
      }
    }
  }
  // 7. `strictPluginOnlyCustomization` union: `true`/`false` OR an array of the known
  //    customization surfaces (`skills`/`agents`/`hooks`/`mcp`). Not in the single-type
  //    table because the type is a boolean|array union; validate both forms here so a
  //    valid policy is accepted while a malformed value (number/string/object, or an
  //    array holding a non-surface name) is surfaced and dropped by salvage.
  if (config.strictPluginOnlyCustomization !== undefined) {
    const sp = config.strictPluginOnlyCustomization as unknown;
    const SURFACES = ['skills', 'agents', 'hooks', 'mcp'];
    if (typeof sp !== 'boolean' && !Array.isArray(sp)) {
      issues.push({ field: 'strictPluginOnlyCustomization', message: `must be a boolean or an array of ${SURFACES.join('/')} (got ${describeJsonType(sp)})`, value: sp, tip: 'Use true to lock all surfaces, or an array like ["agents"] to lock specific ones.' });
    } else if (Array.isArray(sp) && !sp.every((s) => typeof s === 'string' && SURFACES.includes(s))) {
      issues.push({ field: 'strictPluginOnlyCustomization', message: `array entries must each be one of ${SURFACES.join(', ')}`, value: sp, tip: 'Valid surfaces are skills, agents, hooks, mcp.' });
    }
  }
  // Attach a per-field documentation anchor to EVERY issue in one place (parity:
  // the reference's ValidationErrorsList per-error docLink), so the loader diagnostic can
  // render an actionable "Learn more:" affordance without each push site repeating it.
  for (const issue of issues) issue.docLink = getConfigValidationDocLink(issue.field);
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
 * Parity: the reference's `parseCommandOutputAsSettings` — `filterInvalidPermissionRules`
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
 * True for a Windows UNC path (`\\server\share` or `//server/share`). Reading such a
 * path triggers a network (DNS/SMB) round-trip during what should be a cheap local
 * config probe — a hang/SSRF-adjacent hazard for an attacker-influenced path. A config
 * file is always expected on the local filesystem, so a UNC source is treated as
 * "no config here". Parity: the reference's `safeResolvePath` blocks UNC before any I/O.
 */
export function isUncConfigPath(configPath: string): boolean {
  return configPath.startsWith('//') || configPath.startsWith('\\\\');
}

/**
 * True when `stats` describes a non-regular SPECIAL file — a FIFO/named pipe, a
 * socket, or a character/block device. `readFileSync` on a FIFO BLOCKS indefinitely
 * waiting for a writer (a trivial DoS if a config path can be pointed at one), and a
 * device/socket is never a valid config file. The loader skips such a source as
 * "absent" rather than hanging on it. Parity: the reference's `safeResolvePath`
 * special-file guard before `realpathSync`.
 */
export function isSpecialFileStats(stats: fs.Stats): boolean {
  return stats.isFIFO() || stats.isSocket() || stats.isCharacterDevice() || stats.isBlockDevice();
}

/**
 * Env var the CLI `--settings <file>` flag bridges to: an ABSOLUTE path to an
 * arbitrary settings JSON file loaded into the `flag` settings tier. Bridged (not
 * passed as an arg) so both the interactive TUI and the detached runner — which
 * both route through the engine factory — pick it up identically (TUI↔CLI parity).
 */
export const FLAG_SETTINGS_ENV = 'SUPERINFERENCE_SETTINGS';

/** Identity key for a diagnostic (`path:kind:message`), used to suppress a repeat
 *  of the exact same problem reported from an overlapping source. Mirrors the reference's
 *  seenErrors key `${file}:${path}:${message}` in loadSettingsFromDisk. */
export function configDiagnosticIdentity(d: ConfigDiagnostic): string {
  return `${d.path}:${d.kind}:${d.message}`;
}

/**
 * One FILE-based config tier in the merge waterfall: its source, resolved path, and
 * whether it is ACTIVELY contributing to the effective config (the file exists and
 * parsed to a config object). Parity: one row of the reference's Settings status
 * `buildSettingSourcesProperties()` active-source list.
 */
export interface ActiveConfigSource {
  /** The config tier this entry describes. */
  source: ConfigSource;
  /** Human-readable provenance name ({@link getConfigSourceName}). */
  sourceName: string;
  /** Absolute path of the tier's config file (reported even when absent). */
  path: string;
  /**
   * True when the file exists and loaded to a non-null config — i.e. the tier actually
   * contributes to the merged config. False when the file is absent, unparseable, or
   * (for the managed tier) rejected as untrusted/broken.
   */
  active: boolean;
}

// ---------------------------------------------------------------------------
// Internal-write suppression (parity: the reference's changeDetector.ts
// markInternalWrite / consumeInternalWrite). When AMI writes a config file
// itself (a `/config` change, an atomic-write helper), the directory watcher
// observes that write and would otherwise reload + fire a config-change
// notification/hook — a self-triggered feedback loop. Every AMI-originated write
// records its path+timestamp; a watcher that fires within INTERNAL_WRITE_WINDOW_MS
// consumes the mark and swallows the event, so AMI's own writes never re-trigger.
// ---------------------------------------------------------------------------

/** Window (ms) during which a watcher event is attributed to AMI's own write. */
export const INTERNAL_WRITE_WINDOW_MS = 5000;

// ---------------------------------------------------------------------------
// Merge
// ---------------------------------------------------------------------------

/**
 * Concatenate a lower-layer array with a higher-layer one, de-duplicating while
 * preserving first-seen order. Parity: the reference's `settingsMergeCustomizer`
 * (`uniq([...target, ...source])`) so list-valued settings ACCUMULATE across
 * config layers instead of a higher layer clobbering a lower one. `existing`
 * (the already-accumulated lower-priority values) comes first so its ordering is
 * stable and higher layers only append genuinely-new entries.
 */
export function mergeConfigArray(existing: string[] | undefined, incoming: string[]): string[] {
  return [...new Set([...(existing ?? []), ...incoming])];
}

/**
 * Accumulate the MCP allowlist across config layers like {@link mergeConfigArray},
 * but over the richer {@link McpAllowedServerEntry} union — string entries dedupe by
 * value, provenance objects dedupe by their canonical JSON so a lower layer's
 * `{ pluginSource }` / `{ marketplace, plugin }` trust is never dropped by a higher
 * layer, only appended to. First-seen order is preserved.
 */
export function mergeMcpAllowlist(
  existing: McpAllowedServerEntry[] | undefined,
  incoming: McpAllowedServerEntry[],
): McpAllowedServerEntry[] {
  const out: McpAllowedServerEntry[] = [];
  const seen = new Set<string>();
  const keyOf = (e: McpAllowedServerEntry): string =>
    typeof e === 'string' ? `s:${e}` : `o:${JSON.stringify(e)}`;
  for (const e of [...(existing ?? []), ...incoming]) {
    const k = keyOf(e);
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(e);
  }
  return out;
}

/** A plain (non-null, non-array) object — the shape a nested config block always has. */
export function isPlainObject(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

/**
 * Deep-merge one config layer's nested object over the accumulated lower-layer one so
 * a higher layer AUGMENTS rather than CLOBBERS a nested block: a global
 * `superinference: { enforcing: true }` plus a project `superinference: { useLLMCritic:
 * true }` yields BOTH flags, instead of the project wiping the global's `enforcing`.
 * Recurses into nested objects; string arrays accumulate (concat + dedupe, like
 * {@link mergeConfigArray}); any other scalar/array is replaced (higher wins). Parity:
 * the reference's `settingsMergeCustomizer` / lodash `mergeWith` deep-merge of nested
 * settings objects across layers (AMI previously replaced each nested block wholesale).
 */
export function deepMergeConfigObjects(
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
