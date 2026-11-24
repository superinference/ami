// Split from config.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { treeify, type TreeNode, type TreeifyOptions } from './utils/treeify';
import { isMcpAllowedServerEntry } from './config-strip-jsonc-comments';
import type { ConfigSource, ConfigValueIssue } from './config-strip-jsonc-comments';
import type { ProjectConfig } from './config';

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
  /**
   * A copy-pasteable shell command that restores the most recent known-GOOD
   * pre-write snapshot (`<file>.backup-<ms>`) over the damaged file, e.g.
   * `cp "<backup>" "<file>"`. Present only on a fatal `parse-error`/`not-object`
   * diagnostic when such a good backup exists, so the user is told HOW to recover
   * rather than only that the file is broken. Parity: the reference's
   * `cp "<backup>" "<file>"` restore hint printed on a corrupt-config read.
   */
  restoreHint?: string;
  /** For `invalid-value`: the per-field issues (grouped by this file). */
  issues?: ConfigValueIssue[];
  /** For `unknown-key`: the unrecognized top-level keys (likely typos). */
  unknownKeys?: string[];
  /**
   * For a FATAL load failure (`parse-error` / `not-object`) where the file is
   * skipped ENTIRELY: the recognized ({@link KNOWN_CONFIG_KEYS}) top-level keys the
   * raw file text was nonetheless trying to set, detected by {@link detectIntendedConfigKeys}
   * even though the file does not parse. A broken file "fails open" — none of its
   * settings take effect — so surfacing these tells the user EXACTLY which knobs
   * (e.g. `safeMode`, `cleanupPeriodDays`, a deny/permissions key) silently lost
   * their value, instead of leaving a security-bearing setting quietly inert. Absent
   * when the broken file sets no recognized key. Parity: the reference's
   * `rawSettingsContainsKey` user-intent detection + the InvalidSettingsDialog
   * "files with errors are skipped entirely" warning.
   */
  intendedKeys?: string[];
}

/** The ProjectConfig enum fields and their allowed values. */
export const PROJECT_CONFIG_ENUMS: ReadonlyArray<readonly [keyof ProjectConfig, readonly string[]]> = [
  ['permissionMode', ['ask', 'auto-allow', 'deny-all']],
  ['cacheTtl', ['5m', '1h']],
  ['autoUpdatesChannel', ['latest', 'stable']],
  // Diff renderer + input-box shell selectors (see ProjectConfig) resolved off the merged
  // config by resolveDiffTool / resolveDefaultShell — enum-validated so a bad value is
  // surfaced instead of silently flowing through, and emitted in the JSON schema.
  ['diffTool', ['terminal', 'auto']],
  ['defaultShell', ['bash', 'powershell']],
  ['dynamicWorkflowSize', ['small', 'medium', 'large', 'off']],
];

/** The expected runtime base-type of each typed ProjectConfig field. */
export type ConfigFieldType = 'string' | 'number' | 'boolean' | 'string[]' | 'object' | 'mcpAllowlist';

/**
 * Expected runtime base-type of each typed ProjectConfig field. Enum fields live in
 * {@link PROJECT_CONFIG_ENUMS}; numeric RANGE is delegated to {@link validateEngineConfig}.
 * This table only guards the base runtime type so an untrusted JSON value of the wrong
 * type (a string where a number/boolean/array is expected) is surfaced instead of
 * silently flowing into runtime behavior — e.g. a string `maxBudgetUsd`, whose
 * `"ten" <= 0` comparison is `false`, would otherwise slip past the spend-cap range
 * check. Parity: the reference's per-field settings-schema validation.
 */
export const PROJECT_CONFIG_FIELD_TYPES: ReadonlyArray<readonly [keyof ProjectConfig, ConfigFieldType]> = [
  ['model', 'string'], ['provider', 'string'], ['baseUrl', 'string'],
  ['thinkingLevel', 'string'], ['maxEffortLevel', 'string'], ['fallbackModel', 'string'],
  ['compactionModel', 'string'], ['skill', 'string'],
  ['language', 'string'], ['subagentDefaultModel', 'string'],
  ['maxTurns', 'number'], ['tokenBudget', 'number'], ['maxBudgetUsd', 'number'],
  ['bashOutputMaxChars', 'number'], ['taskOutputMaxChars', 'number'],
  ['toolOutputMaxChars', 'number'],
  ['maxWebSearchesPerSession', 'number'], ['migrationVersion', 'number'],
  ['cleanupPeriodDays', 'number'], ['maxSkillProgressMessages', 'number'],
  ['safeMode', 'boolean'], ['hasTrustDialogAccepted', 'boolean'],
  ['trustedProjectPaths', 'string[]'],
  ['deferTools', 'boolean'], ['tipsEnabled', 'boolean'],
  ['composerHintsEnabled', 'boolean'], ['disableBypassPermissionsMode', 'boolean'],
  ['disableAllHooks', 'boolean'], ['allowManagedHooksOnly', 'boolean'],
  ['allowManagedModsOnly', 'boolean'], ['allowManagedSandboxDomainsOnly', 'boolean'],
  ['enforceAvailableModels', 'string[]'], ['allowedMcpServers', 'mcpAllowlist'],
  ['deniedMcpServers', 'string[]'],
  ['disabledMcpServers', 'string[]'], ['enabledMcpServers', 'string[]'],
  ['fallbackModels', 'string[]'], ['alwaysLoadTools', 'string[]'],
  ['sandboxExcludedCommands', 'string[]'], ['memoryExcludes', 'string[]'],
  ['externalMemoryImportsApproved', 'boolean'], ['externalMemoryImportRoots', 'string[]'],
  ['superinference', 'object'], ['managedMcpServers', 'object'],
  ['enabledPlugins', 'object'], ['env', 'object'],
  ['tipsOverride', 'object'], ['composerHintsOverride', 'object'],
  ['worktree', 'object'], ['webFetch', 'object'], ['attribution', 'object'],
  ['skillUsage', 'object'],
  ['preferencesShown', 'object'], ['noticeSeenCounts', 'object'],
  ['githubRepoPaths', 'object'],
  // Durable per-session/per-project STATE + preference knobs (see ProjectConfig) —
  // declared here so a file-tier value is recognized (no spurious unknown-key warning),
  // base-type validated, and emitted in the JSON schema. `lastSessionMetrics` is an
  // object of counters (its nested values are value-validated bespoke, like
  // noticeSeenCounts); the onboarding fatigue fields + deepLinkTerminal + background
  // deferral knobs are scalars. Parity: the reference's lastSessionMetrics / projectOnboarding* /
  // deepLinkTerminal / DELAY_VERY_SLOW_OPERATIONS defaults.
  ['lastSessionMetrics', 'object'],
  ['projectOnboardingSeenCount', 'number'], ['hasCompletedProjectOnboarding', 'boolean'],
  ['deepLinkTerminal', 'string'],
  ['backgroundSlowOpDelayMs', 'number'], ['backgroundInteractionRecencyMs', 'number'],
  // Engine-behavior knobs (see ProjectConfig) — merged by {@link mergeConfigs} and
  // read by the engine factory; declared here so they are recognized + value-validated.
  ['architectEnabled', 'boolean'], ['architectModel', 'string'],
  ['embeddedSearchTools', 'boolean'], ['teammateExecutionMode', 'string'],
  ['unattendedRetry', 'boolean'], ['fileCheckpointingEnabled', 'boolean'],
  ['heapDumpOnMemoryWarning', 'boolean'],
  ['features', 'object'], ['cachedCapabilities', 'object'],
  ['max529RetriesBeforeFallback', 'number'],
  ['openrouterZdr', 'boolean'], ['strictMcpConfig', 'boolean'],
  ['mcpRequireApproval', 'boolean'], ['maxUsageLimitWaitMs', 'number'],
  ['sessionMemoryInterval', 'number'], ['alwaysLoadMcpServers', 'string[]'],
  ['continueOnInvalidConfig', 'boolean'], ['outputStyle', 'string'],
  ['includeGitInstructions', 'boolean'],
  ['pluginHintsDisabled', 'boolean'],
  // Startup runtime-knob bridges (see ProjectConfig) — merged by {@link mergeConfigs}
  // and applied to the runtime (process.env / allowed read roots) at engine setup.
  ['extraCaCerts', 'string'], ['plansDirectory', 'string'],
  ['respectGitignore', 'boolean'], ['autoCompactEnabled', 'boolean'],
  ['fastMode', 'boolean'], ['redactThinking', 'boolean'],
  ['additionalDirectories', 'string[]'],
  // Engine-behavior knobs (round 2) the engine factory already reads but mergeConfigs
  // previously dropped — declared here so they are recognized + base-type validated.
  ['autoMemoryEnabled', 'boolean'], ['autoDreamEnabled', 'boolean'],
  ['sessionMemoryEnabled', 'boolean'], ['teamMemoryEnabled', 'boolean'],
  ['enableTitleGeneration', 'boolean'],
  ['worktreeToolsEnabled', 'boolean'], ['terminalProgressBarEnabled', 'boolean'],
  ['speculationEnabled', 'boolean'], ['alwaysThinkingEnabled', 'boolean'],
  ['showTurnDuration', 'boolean'], ['autoConnectIde', 'boolean'],
  // Cosmetic / output-detail startup knobs (see ProjectConfig) bridged to their existing
  // cli env knobs by {@link applyConfigStartupKnobs} — declared here so they are
  // recognized (no spurious unknown-key warning) + base-type validated.
  ['verbose', 'boolean'], ['copyOnSelect', 'boolean'], ['copyFullResponse', 'boolean'], ['fullscreen', 'boolean'],
  ['syntaxHighlightingDisabled', 'boolean'], ['showThinkingSummaries', 'boolean'],
  ['messageIdleNotifThresholdMs', 'number'], ['spinnerVerbs', 'object'],
  ['backgroundTasksEnabled', 'boolean'],
  // Admin-policy / engine knobs (round 3) a common/ resolver already reads but
  // mergeConfigs previously dropped / the loader flagged as an unknown key — declared
  // here so they are recognized (no spurious unknown-key warning) + base-type validated.
  // `policyRestrictions` is base-type `object`; its nested `{ allowed: boolean }` shape
  // is validated bespoke in {@link validateProjectConfigValues}.
  ['policyRestrictions', 'object'], ['policyRefresh', 'object'],
  ['pluginTrustMessage', 'string'], ['memoryCriticalRssBytes', 'number'],
  ['preferTmuxOverIterm2', 'boolean'],
  ['blockedMarketplaces', 'string[]'], ['strictKnownMarketplaces', 'string[]'],
  // UI/UX knobs (round 4) consumed by common/cli footer + plan-accept + release-notes
  // flows (see ProjectConfig) — declared here so they are recognized (no spurious
  // unknown-key warning) + base-type validated + emitted in the JSON schema.
  ['showClearContextOnPlanAccept', 'boolean'], ['prStatusFooterEnabled', 'boolean'],
  ['lastReleaseNotesSeen', 'string'],
  // Opt-in capability knobs (voice input + auto-mode intent) resolved off the merged
  // config by isVoiceModeEnabled / hasAutoModeOptIn — declared here so a file-tier value
  // is recognized (no spurious unknown-key warning) + base-type validated + schema-emitted.
  ['voiceEnabled', 'boolean'], ['autoAllowOptIn', 'boolean'],
  // Example-command cache state + managed startup announcements + sandbox-override policy
  // (see ProjectConfig) — declared here so a file-tier value is recognized (no spurious
  // unknown-key warning) + base-type validated + schema-emitted. exampleFiles /
  // companyAnnouncements are string[]; exampleFilesGeneratedAt is epoch-ms; the
  // sandbox-override policy is a boolean resolved by areUnsandboxedCommandsAllowed.
  ['exampleFiles', 'string[]'], ['exampleFilesGeneratedAt', 'number'],
  ['companyAnnouncements', 'string[]'], ['allowUnsandboxedCommands', 'boolean'],
  // Update-channel knobs (see ProjectConfig) consumed by the self-updater/launcher —
  // declared here so they are recognized + value-validated (autoUpdatesChannel is an
  // enum in PROJECT_CONFIG_ENUMS) + emitted in the JSON schema. Parity: the reference's
  // autoUpdatesChannel / minimumVersion settings.
  ['minimumVersion', 'string'],
];

/**
 * True when `value` matches the expected {@link ConfigFieldType}. Numbers must be
 * finite (an out-of-range literal is not a usable knob); arrays must be homogeneously
 * `string[]`; objects exclude `null` and arrays.
 */
export function matchesConfigFieldType(value: unknown, type: ConfigFieldType): boolean {
  switch (type) {
    case 'string': return typeof value === 'string';
    case 'number': return typeof value === 'number' && Number.isFinite(value);
    case 'boolean': return typeof value === 'boolean';
    case 'string[]': return Array.isArray(value) && value.every(v => typeof v === 'string');
    case 'object': return value !== null && typeof value === 'object' && !Array.isArray(value);
    case 'mcpAllowlist': return Array.isArray(value) && value.every(isMcpAllowedServerEntry);
  }
}

/**
 * Human-readable phrase (including its article) for a {@link ConfigFieldType}, used
 * in a type-mismatch message (`must be <label> (got <actual>)`). The base scalar/array
 * types keep their original wording; the richer `mcpAllowlist` union renders a
 * descriptive phrase rather than the opaque internal tag.
 */
export function configFieldTypeLabel(type: ConfigFieldType): string {
  switch (type) {
    case 'mcpAllowlist': return 'an array of server names/URLs or provenance entries';
    default: return `a ${type}`;
  }
}

/** The kind of validation failure a tip is being generated for. */
export type ConfigTipKind =
  | { kind: 'type'; type: ConfigFieldType }
  | { kind: 'enum'; allowed: readonly string[] }
  | { kind: 'range' };

/** Render an enum's allowed values exactly like the generic enum tip, so a
 *  field-specific override can keep the `Valid values: ….` prefix other callers
 *  (and tests) rely on and then append its semantic explanation. */
export function enumValuesPrefix(allowed: readonly string[]): string {
  return `Valid values: ${allowed.map(v => `"${v}"`).join(', ')}.`;
}

/**
 * Named-field, semantics-aware validation tips (parity: the reference's field-matched
 * `getValidationTip`). Unlike the generic per-kind tips — which describe the expected
 * SHAPE ("must be a number", "Valid values: …") — these describe what the value MEANS,
 * so a user who typed a valid-shaped-but-wrong value understands the fix. Returns
 * `undefined` for any field without a specific override, so the generic fallback still
 * applies everywhere else (purely additive). Enum overrides reuse {@link enumValuesPrefix}
 * so the `Valid values: ….` prefix is preserved verbatim.
 */
export function fieldSpecificConfigTip(field: string, ctx: ConfigTipKind): string | undefined {
  switch (field) {
    case 'permissionMode':
      // AMI's three permission modes (ProjectConfig.permissionMode). Explain the
      // security posture of each so a bad value is corrected toward intent, not guessed.
      return ctx.kind === 'enum'
        ? `${enumValuesPrefix(ctx.allowed)} "ask" prompts before each tool action (default), "auto-allow" auto-approves every action, "deny-all" blocks any action that needs approval.`
        : undefined;
    case 'defaultShell':
      // Routes the TUI input-box `!command` shortcut (resolveDefaultShell).
      return ctx.kind === 'enum'
        ? `${enumValuesPrefix(ctx.allowed)} Selects the shell that input-box "!" commands run in: "bash" (default) or "powershell" on Windows hosts.`
        : undefined;
    case 'diffTool':
      // Selects the file-edit diff renderer (resolveDiffTool).
      return ctx.kind === 'enum'
        ? `${enumValuesPrefix(ctx.allowed)} Selects the diff renderer for file edits: "terminal" forces inline terminal diffs, "auto" picks the best available.`
        : undefined;
    case 'cleanupPeriodDays':
      // Session-transcript retention window (SessionManager.cleanupOldSessions).
      return 'Retention window in whole days for persisted session transcripts. Use a positive integer to keep that many days; 0 (or omit) disables cleanup — nothing is deleted.';
    default:
      return undefined;
  }
}

/**
 * Map a per-field validation failure to a short, actionable fix suggestion
 * (parity: the reference's `getValidationTip`). Returns `undefined` when no specific
 * tip applies (the generic `message` already suffices). Keeping this as ONE table
 * (rather than inline strings at each push site) means the loader diagnostic,
 * `/doctor`, and any future renderer all surface identical guidance.
 */
export function getConfigValidationTip(field: string, ctx: ConfigTipKind): string | undefined {
  // Field-specific overrides first (most actionable), then generic per-kind tips.
  if (field.startsWith('env.') || field === 'env') {
    return 'Environment variables must be strings — wrap numbers and booleans in quotes, e.g. "DEBUG": "true".';
  }
  // Named-field overrides that explain the SEMANTICS of the value (not just its shape),
  // mirroring the reference's field-matched getValidationTip (e.g. what each permission
  // mode does, what cleanupPeriodDays=0 means). These augment — never replace — the
  // generic per-kind tips below: a field without a specific override falls straight
  // through to the shape tip, so no existing behavior changes.
  const specific = fieldSpecificConfigTip(field, ctx);
  if (specific) return specific;
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
        case 'mcpAllowlist': return 'Must be an array of server names/URLs or provenance objects, e.g. ["my-server", { "pluginSource": "acme" }, { "marketplace": "official", "plugin": "acme" }].';
      }
  }
}

/**
 * Base URL of the AMI configuration reference. One source for every per-field doc
 * anchor so the documentation host can be changed in a single place. Mirrors the
 * existing doc links AMI already surfaces (e.g. the permissions / keybindings
 * references).
 */
export const CONFIG_DOC_BASE = 'https://docs.superinference.org/configuration';

/**
 * Resolve the documentation URL for a config `field` so a validation diagnostic can
 * render an actionable "Learn more: <url>" affordance (parity: the reference's
 * `ValidationErrorsList` per-error `docLink`). A nested field (e.g.
 * `superinference.enforcing`, `worktree.sparsePaths`, `env.DEBUG`) resolves to its
 * TOP-LEVEL anchor so related sub-field errors share one deduplicated link. Always
 * returns a URL — every recognized field documents under the same reference page.
 */
export function getConfigValidationDocLink(field: string): string {
  const dot = field.indexOf('.');
  const top = dot > 0 ? field.slice(0, dot) : field;
  return `${CONFIG_DOC_BASE}#${top.toLowerCase()}`;
}

/**
 * Render a list of {@link ConfigValueIssue}s into one diagnostic body: the per-field
 * `field: message (tip)` entries, then a SINGLE deduplicated "Learn more: <url>, …"
 * line built from the issues' {@link ConfigValueIssue.docLink}s. Deduplicating the
 * doc links (rather than repeating one per issue) mirrors the reference's
 * `ValidationErrorsList`, which collapses duplicate suggestion|docLink pairs before
 * rendering each as a hint + "Learn more:" line. Shared by {@link loadConfigFileChecked}
 * so the loader diagnostic and `/doctor` surface identical, non-repetitive guidance.
 */
export function formatConfigValueIssues(issues: readonly ConfigValueIssue[]): string {
  const perIssue = issues.map(i => `${i.field}: ${i.message}${i.tip ? ` (${i.tip})` : ''}`).join('; ');
  const docLinks = [...new Set(issues.map(i => i.docLink).filter((d): d is string => typeof d === 'string'))];
  return docLinks.length > 0 ? `${perIssue}. Learn more: ${docLinks.join(', ')}` : perIssue;
}

/**
 * Group a set of {@link ConfigDiagnostic}s into a {@link TreeNode} keyed by the config
 * FILE they belong to (file labels sorted alphabetically for deterministic output),
 * each file nesting its per-field value issues ({@link ConfigValueIssue}) or, for a
 * non-`invalid-value` diagnostic, its single message under the diagnostic `kind`.
 *
 * This is the structured input for {@link formatConfigDiagnosticsTree} — parity with
 * the reference's `ValidationErrorsList`, which groups settings errors by file and renders
 * each file's errors as a nested tree. Multiple bad config files across the merge
 * waterfall (user / project / managed) are shown at once rather than one flat string.
 */
export function buildConfigDiagnosticsTree(diagnostics: readonly ConfigDiagnostic[]): TreeNode {
  // Accumulate per-file, then re-insert keys in sorted order so treeify renders files
  // alphabetically (parity: Object.keys(...).sort()).
  const byFile = new Map<string, TreeNode>();
  for (const d of diagnostics) {
    const fileLabel = d.sourceName ? `${d.sourceName} config (${d.path})` : d.path;
    let fileNode = byFile.get(fileLabel);
    if (!fileNode) {
      fileNode = {};
      byFile.set(fileLabel, fileNode);
    }
    if (d.issues && d.issues.length > 0) {
      for (const issue of d.issues) {
        fileNode[issue.field] = `${issue.message}${issue.tip ? ` (${issue.tip})` : ''}`;
      }
    } else {
      fileNode[d.kind] = d.message;
    }
  }
  const tree: TreeNode = {};
  for (const fileLabel of [...byFile.keys()].sort()) {
    tree[fileLabel] = byFile.get(fileLabel) as TreeNode;
  }
  return tree;
}

/**
 * Render {@link ConfigDiagnostic}s as a file-grouped, path-nested ASCII tree via
 * {@link treeify}. Empty input yields an empty string (nothing to show). The optional
 * {@link TreeifyOptions} are forwarded so a CLI caller can inject colour.
 */
export function formatConfigDiagnosticsTree(
  diagnostics: readonly ConfigDiagnostic[],
  options?: TreeifyOptions,
): string {
  if (diagnostics.length === 0) return '';
  return treeify(buildConfigDiagnosticsTree(diagnostics), options);
}

// ---------------------------------------------------------------------------
// JSON Schema emission for the PROJECT config file (parity: the reference's
// utils/settings/schemaOutput.ts `generateSettingsJSONSchema` + the published
// `CLAUDE_CODE_SETTINGS_SCHEMA_URL`). This complements the governed-policy schema in
// settings-guard.ts (`getSettingsJSONSchema`, which describes ONLY the managed/tier-
// locked subset) by emitting a schema for the FULL set of recognized `ProjectConfig`
// knobs — every `.ami/config.json` key an editor can autocomplete/validate.
//
// Like `getSettingsJSONSchema`, the schema is DERIVED from this module's authoritative
// validation tables ({@link PROJECT_CONFIG_FIELD_TYPES} + {@link PROJECT_CONFIG_ENUMS} +
// {@link KNOWN_CONFIG_KEYS}) rather than a hand-maintained duplicate, so it self-updates
// when a knob is added and can never silently drop one (the campaign's "dynamic
// denominator" discipline). Surfaced via `ami --introspect`.
// ---------------------------------------------------------------------------

/**
 * Canonical published URL of the AMI config JSON Schema (parity: the reference's
 * `CLAUDE_CODE_SETTINGS_SCHEMA_URL`). A user can reference it from a config file via
 * `"$schema": "<this URL>"` so an editor offers autocomplete + live validation of the
 * known knobs. Also emitted as the generated schema's `$id`.
 */
export const CONFIG_SCHEMA_URL = 'https://docs.superinference.org/schema/ami-config.json';

/** A single property node of the generated project-config JSON Schema. */
export interface ConfigSchemaProperty {
  /** JSON Schema primitive type (omitted for union-typed keys left permissive). */
  type?: 'boolean' | 'number' | 'string' | 'array' | 'object';
  /** Human-readable description of the key. */
  description: string;
  /** Element schema, present only when `type === 'array'`. */
  items?: { type: 'string' } | Record<string, never>;
  /** Allowed string values, present only for enum-typed keys. */
  enum?: string[];
}

/** The generated project-config JSON Schema document (draft-07, non-strict). */
export interface ConfigJSONSchema {
  $schema: string;
  $id: string;
  title: string;
  type: 'object';
  properties: Record<string, ConfigSchemaProperty>;
  /** Non-strict: unknown keys load (surfaced as an advisory warning, never fatal). */
  additionalProperties: boolean;
}
