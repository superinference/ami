import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { log as coreLog } from './logger';
import './env-protection';
import { mergeSkillUsageStat, type SkillUsageStat } from './types';
import { withFileLockSync, type FileLockSyncOptions } from './utils/file-lock';
import { instanceScopedTempName, parseInstanceTempPid, isPidAlive, runtimeInstancePid } from './utils/runtime-namespace';
import './utils/treeify';
import { escapeRegExp } from './utils/string';
import { getErrnoCode } from './utils/errno';
import { levenshteinDistance } from './tools/path-suggest';
import './tools/tool-gates';
import './tools/workflow-size';
import './settings-guard';
import { readNativeManagedPolicy } from './managed-policy-source';
import { errorMessage } from './credential-guard';
import { JSONC_PARSE_CACHE_MAX_BYTES, JSONC_PARSE_CACHE_MAX_ENTRIES, JSON_SYNTAX_RECOVERY_HINT, MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV, ManagedConfigError, describeJsonType, getConfigSourceName, getGlobalConfigPath, getLocalConfigPath, getProjectConfigPath, isWorldWritable, managedOverrideUntrustedAllowed, mergeManagedLayer, stripBom, stripJsoncComments, stripJsoncTrailingCommas } from './config-strip-jsonc-comments';
import type { ConfigSource, ManagedConfigCacheEntry, McpAllowedServerEntry, StatusLineConfig, TierConfigCacheEntry } from './config-strip-jsonc-comments';
import { CONFIG_SCHEMA_URL, PROJECT_CONFIG_ENUMS, PROJECT_CONFIG_FIELD_TYPES, configFieldTypeLabel, formatConfigValueIssues } from './config-config-diagnostic';
import type { ConfigDiagnostic, ConfigJSONSchema, ConfigSchemaProperty } from './config-config-diagnostic';
import { CONFIG_WATCH_DEBOUNCE_MS, FLAG_SETTINGS_ENV, INTERNAL_WRITE_WINDOW_MS, configDiagnosticIdentity, configFieldTypeToSchemaNode, isSpecialFileStats, isUncConfigPath, salvageManagedConfig, validateProjectConfigValues } from './config-validate-project-config-values';
import type { ActiveConfigSource, ConfigChangeListener } from './config-validate-project-config-values';
import { CONFIG_DELETION_GRACE_MS, CONFIG_FALLBACK_POLL_MS, CONFIG_FILE_STABILITY_THRESHOLD_MS, CONFIG_MANAGED_POLL_MS, CONFIG_WATCH_RETRY_MS, configClockHooks, isWatcherPermissionError } from './config-config-service-options';
import type { ConfigServiceOptions } from './config-config-service-options';
import { applyConfigEnv, configEnvHooks, mergeConfigs, normalizeTrustPath } from './config-merge-configs';
import type { ApplyConfigEnvResult } from './config-merge-configs';
import { applyConfigAnthropicBetas, applyConfigAutoCompact, applyConfigBackgroundTasks, applyConfigCopyOnSelect, applyConfigFeatureFlags, applyConfigFullscreen, applyConfigGitignore, applyConfigIdleNotifThreshold, applyConfigPlansDirectory, applyConfigSpinnerVerbs, applyConfigSyntaxHighlighting, applyConfigTerminalProgress, applyConfigTurnDuration, applyConfigVerbose, applyConfigWorkflowSize } from './config-apply-config-feature-flags';
import { DEPRECATED_CONFIG_KEY_MIGRATIONS, insertIntoJsoncArray, migrateConfigKey } from './config-validate-bounded-int-env-var';
import type { ConfigKeyMigration } from './config-validate-bounded-int-env-var';
import { CONFIG_BACKUP_INFIX, CONFIG_BACKUP_MIN_INTERVAL_MS, CONFIG_FILE_MODE, CONFIG_WRITE_DISPLAY_THRESHOLD, LOCAL_CONFIG_GITIGNORE_RULE, MAX_CONFIG_BACKUPS, PROJECT_ONBOARDING_SEEN_CAP, SKILL_USAGE_DEBOUNCE_MS, backupTimestamp } from './config-resolve-tool-output-max-chars';
import type { ConfigFileValidation, ResetConfigResult, UpdateConfigResult } from './config-resolve-tool-output-max-chars';
export { CONFIG_SOURCES, CONFIG_SOURCE_NAMES, JSON_SYNTAX_RECOVERY_HINT, MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV, ManagedConfigError, getConfigSourceName, getGlobalConfigPath, getLocalConfigPath, getProjectConfigPath, isMcpAllowedServerEntry, isWorldWritable, normalizeStatusLine, stripBom, stripJsoncComments, stripJsoncTrailingCommas } from './config-strip-jsonc-comments';
export type { ConfigSource, ConfigValueIssue, McpAllowedServerEntry, StatusLineConfig } from './config-strip-jsonc-comments';
export { CONFIG_DOC_BASE, CONFIG_SCHEMA_URL, buildConfigDiagnosticsTree, formatConfigDiagnosticsTree, formatConfigValueIssues, getConfigValidationDocLink, getConfigValidationTip } from './config-config-diagnostic';
export type { ConfigDiagnostic, ConfigJSONSchema, ConfigSchemaProperty, ConfigTipKind } from './config-config-diagnostic';
export { CONFIG_WATCH_DEBOUNCE_MS, FLAG_SETTINGS_ENV, INTERNAL_WRITE_WINDOW_MS, isSpecialFileStats, isUncConfigPath, salvageManagedConfig, validateProjectConfigValues } from './config-validate-project-config-values';
export type { ActiveConfigSource, ConfigChangeListener } from './config-validate-project-config-values';
export { CONFIG_DELETION_GRACE_MS, CONFIG_FALLBACK_POLL_MS, CONFIG_FILE_STABILITY_THRESHOLD_MS, CONFIG_MANAGED_POLL_MS, CONFIG_WATCH_RETRY_MS, configClockHooks, isWatcherPermissionError } from './config-config-service-options';
export type { ConfigServiceOptions } from './config-config-service-options';
export { CONFIG_ENV_TIERS, applyConfigEnv, configEnvHooks, mergeConfigs, normalizeTrustPath } from './config-merge-configs';
export type { ApplyConfigEnvResult, ConfigEnvLayers } from './config-merge-configs';
export { FAST_MODE_BETA, FORCE_ALT_SCREEN_ENV, NOTIFY_THRESHOLD_ENV, NO_ALT_SCREEN_ENV, NO_COPY_ON_SELECT_ENV, NO_PROGRESS_ENV, REDACT_THINKING_BETA, SHOW_TURN_DURATION_ENV, SPINNER_VERBS_ENV, SYNTAX_HIGHLIGHT_ENV, VERBOSE_ENV, applyConfigAnthropicBetas, applyConfigAutoCompact, applyConfigBackgroundTasks, applyConfigCopyOnSelect, applyConfigFeatureFlags, applyConfigFullscreen, applyConfigGitignore, applyConfigIdleNotifThreshold, applyConfigLanguage, applyConfigPlansDirectory, applyConfigSpinnerVerbs, applyConfigSyntaxHighlighting, applyConfigTerminalProgress, applyConfigTurnDuration, applyConfigVerbose, applyConfigWorkflowSize } from './config-apply-config-feature-flags';
export { DEFAULT_TOOL_OUTPUT_MAX_CHARS, DEPRECATED_CONFIG_KEY_MIGRATIONS, TOOL_OUTPUT_MAX_CHARS_ENV, areUnsandboxedCommandsAllowed, findTopLevelArrayBrackets, hasAutoModeOptIn, isPrStatusFooterEnabled, isVoiceModeEnabled, migrateConfigKey, omitWhenEqualsDefault, pickCompanyAnnouncement, resolveAdditionalDirectories, resolveDefaultShell, resolveDiffTool, shouldShowClearContextOnPlanAccept, toPersistableEffort, validateBoundedIntEnvVar } from './config-validate-bounded-int-env-var';
export type { BoundedIntOptions, BoundedIntResult, BoundedIntStatus, ConfigKeyMigration } from './config-validate-bounded-int-env-var';
export { CONFIG_BACKUP_MIN_INTERVAL_MS, CONFIG_FILE_MODE, CONFIG_WRITE_DISPLAY_THRESHOLD, CONFIG_WRITE_TIMEOUT_MS, LOCAL_CONFIG_GITIGNORE_RULE, MAX_CONFIG_BACKUPS, PROJECT_ONBOARDING_SEEN_CAP, SKILL_USAGE_DEBOUNCE_MS, TOOL_OUTPUT_MAX_UPPER_LIMIT, UPDATE_CONFIG_REASONS, resolveToolOutputMaxChars } from './config-resolve-tool-output-max-chars';
export type { ConfigFileValidation, ResetConfigResult, UpdateConfigReason, UpdateConfigResult } from './config-resolve-tool-output-max-chars';

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
  /** Dynamic workflow size — soft agent-count guideline for a single workflow run +
   *  the "Large workflow" warning threshold (small|medium|large|off; default medium).
   *  Bridged to the AMI_DYNAMIC_WORKFLOW_SIZE env knob the workflow tool + /flows tree
   *  read. See core/core/src/tools/workflow-size.ts. */
  dynamicWorkflowSize?: 'small' | 'medium' | 'large' | 'off';
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
  /**
   * Global default cap (chars) on a tool result before it is spilled to a file,
   * shared by tools that have no bespoke cap (web_fetch, PDF extraction, etc.).
   * Parity: the reference's single `DEFAULT_MAX_RESULT_SIZE_CHARS` in
   * `toolResultStorage.ts` against which each tool's declared cap is clamped — one
   * tunable knob governs every un-specialised tool's spill threshold instead of the
   * previously hardcoded per-tool constants. Resolved (with the
   * `AMI_TOOL_OUTPUT_MAX_CHARS` env override) by {@link resolveToolOutputMaxChars};
   * {@link bashOutputMaxChars}/{@link taskOutputMaxChars} remain their own per-tool
   * caps. An unset/invalid value falls back to {@link DEFAULT_TOOL_OUTPUT_MAX_CHARS}.
   */
  toolOutputMaxChars?: number;
  /** Anthropic prompt-cache TTL ('5m' default or '1h' extended-cache beta). */
  cacheTtl?: '5m' | '1h';
  /** Safe mode: skip untrusted project-local hooks/MCP/memory/skills. */
  safeMode?: boolean;
  /**
   * Persisted per-project FOLDER-TRUST grant (parity: the reference's
   * `hasTrustDialogAccepted` project-config knob). When `true` the user has recorded
   * that THIS workspace is trusted, so project-tier hooks (`.ami/hooks.json`) — and,
   * in an interactive session, all hooks — are allowed to load instead of being
   * skipped as an untrusted source. AMI previously had only the INVERSE
   * {@link safeMode} opt-in distrust and no config-backed way to GRANT trust, so
   * {@link import('./hooks').HookManager.setWorkspaceTrusted} had no production caller
   * and project hooks stayed permanently skipped. Consumed by
   * {@link resolveWorkspaceTrustAccepted} → `HookManager.loadFromFile`. An unset/false
   * value keeps the untrusted default; the knob only ever PROMOTES trust, never
   * demotes it. A user-level/per-project decision (lives in project/local/global
   * config); it can never WIDEN past a managed `safeMode` lockdown, which is
   * re-applied authoritatively in {@link mergeConfigs}.
   */
  hasTrustDialogAccepted?: boolean;
  /**
   * Allowlist of ABSOLUTE directory paths the user has recorded as trusted (parity:
   * the reference's per-project trust records resolved by `computeTrustDialogAccepted` /
   * `isPathTrusted`). Trust is INHERITED down the tree: a workspace is trusted when it
   * is — or is nested under — any listed path, so trusting a parent directory implies
   * trust of all its children. Normally persisted in the GLOBAL config (`~/.ami/
   * config.json`) but, like every key, merged across the full waterfall (entries
   * ACCUMULATE across layers — a higher layer adds trusted roots, never removes a lower
   * one). Consumed by {@link isPathTrusted} (→ {@link resolveWorkspaceTrustAccepted} →
   * `HookManager.loadFromFile`). Complements the single-workspace
   * {@link hasTrustDialogAccepted} boolean: that grants trust for exactly one project;
   * this grants it for a directory and everything beneath it. Only ever PROMOTES trust.
   */
  trustedProjectPaths?: string[];
  /** Session-wide cap on web_search tool calls. */
  maxWebSearchesPerSession?: number;
  /**
   * Retention window (days) for persisted session transcripts. On startup, sessions
   * whose last on-disk activity is older than this are deleted
   * ({@link SessionManager.cleanupOldSessions}). A missing/0/non-positive value DISABLES
   * cleanup (keep everything). Parity: the reference's `cleanupPeriodDays`.
   */
  cleanupPeriodDays?: number;
  /**
   * Persisted default model for spawned subagents/teammates, consulted ONLY when a
   * spawn names no model from a higher-priority source (operator env override →
   * per-call `model` → agent-definition model → explore fast default). A model id or
   * family alias forces that default; the `inherit` sentinel keeps the parent's exact
   * model ("follow the leader"); omitted falls through to the provider-aware hardcoded
   * fallback. Parity: the reference's `teammateDefaultModel` / getDefaultTeammateModel.
   */
  subagentDefaultModel?: string;
  /** Opt-in cap on how many of a forked skill's nested tool-use steps are surfaced
   *  live to the parent stream; the rest are summarized as "+K more tool use" in the
   *  skill result. A non-negative integer (0 = result-only). Omitted keeps AMI's full
   *  live streaming. Parity: the reference's SkillTool MAX_PROGRESS_MESSAGES_TO_SHOW +
   *  verbose flag (adapted to AMI's live-stream model). */
  maxSkillProgressMessages?: number;
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
   * set it; project/local/env/cli can never flip it. Parity: the reference's
   * `permissions.disableBypassPermissionsMode === 'disable'`.
   */
  disableBypassPermissionsMode?: boolean;
  /**
   * Managed hook kill-switch: when true, ALL hook execution is disabled across
   * every event (preToolUse/postToolUse/sessionStart/sessionEnd/subagentStart/…
   * including status-line and file-suggestion hooks). A {@link MANAGED_ONLY_KEYS}
   * entry — only a managed/enterprise config tier can set it; project/local/env/cli
   * can never flip it. Parity: the reference's `disableAllHooks` policy setting.
   */
  disableAllHooks?: boolean;
  /**
   * Managed hook lockdown: when true, ONLY admin/managed-sourced hooks execute —
   * user, project, and plugin hooks are blocked (managed > non-managed privilege
   * boundary). A {@link MANAGED_ONLY_KEYS} entry — managed tier is the sole source.
   * Parity: the reference's `allowManagedHooksOnly` policy setting.
   */
  allowManagedHooksOnly?: boolean;
  /**
   * Managed plugin policy map (the reference parity: `enabledPlugins`). Keyed by plugin id
   * (`name@marketplace` or a bare name for direct installs). A `false` value FORCE-
   * DISABLES that plugin at every scope and chokepoint — install, enable, and load —
   * independent of {@link allowManagedModsOnly}; a `true`/`string[]` value vouches for
   * it under allow-only mode. A {@link MANAGED_ONLY_KEYS} entry — only a managed/
   * enterprise config tier can set it; project/local/env/cli can never flip it.
   */
  enabledPlugins?: Record<string, boolean | string[]>;
  /** Managed allow-only mode for plugins/mods (the reference parity: load only vouched mods). */
  allowManagedModsOnly?: boolean;
  /**
   * Per-surface plugin-only lock (the reference parity: `strictPluginOnlyCustomization`). `true`
   * locks ALL customization surfaces (`skills`/`agents`/`hooks`/`mcp`) to plugin+managed
   * sources; an array locks only the listed surfaces. A {@link MANAGED_ONLY_KEYS} entry —
   * only a managed/enterprise config tier can set it.
   */
  strictPluginOnlyCustomization?: boolean | ('skills' | 'agents' | 'hooks' | 'mcp')[];
  /**
   * MCP allowlist: when non-empty, only these MCP servers may be connected. An entry
   * is a server NAME or upstream URL (string, `*`-wildcards allowed) OR a
   * PROVENANCE-scoped entry (the reference-parity channelAllowlist): `{ pluginSource }` to
   * approve every server a given plugin bundle produced, or the reference-shaped
   * `{ marketplace, plugin }` which matches the `<plugin>@<marketplace>` provenance
   * tag. Provenance keying trusts "servers from this plugin" regardless of each
   * server's arbitrary local name. Enforced by {@link McpManager} via setAllowedServers.
   */
  allowedMcpServers?: McpAllowedServerEntry[];
  /**
   * MCP denylist: servers named here (by name, case-insensitive, or normalized
   * upstream URL) are blocked across every scope. The denylist takes PRECEDENCE
   * over {@link allowedMcpServers} — a server on both lists is denied. Default-open:
   * an empty/absent list blocks nothing. A managed-tier value is authoritative (a
   * project/local config can only ADD denies via array accumulation, never remove a
   * managed deny). Parity: the reference's enterprise `deniedMcpServers` setting.
   */
  deniedMcpServers?: string[];
  /**
   * User-level per-server DISABLE toggle: servers named here are skipped at load
   * WITHOUT removing their config, so a server can be turned off and back on again.
   * Distinct from {@link deniedMcpServers} (an admin/enterprise policy): this is a
   * user convenience toggle. A name also present in {@link enabledMcpServers} is
   * re-enabled (the enable list wins per-name). Parity: the reference's
   * `disabledMcpServers`.
   */
  disabledMcpServers?: string[];
  /**
   * User-level per-server ENABLE override: re-enables a server that
   * {@link disabledMcpServers} turned off (matched by name, case-insensitive).
   * Parity: the reference's `enabledMcpServers`.
   */
  enabledMcpServers?: string[];
  /** Managed MCP server definitions pushed by enterprise policy. */
  managedMcpServers?: Record<string, unknown>;
  /**
   * Managed exclusive-control switch (parity: the reference's
   * `allowManagedMcpServersOnly`): when true, ONLY admin-provisioned
   * ({@link managedMcpServers}, enterprise scope) MCP servers may run — every
   * user/project/local/dynamic server is blocked at load. A {@link MANAGED_ONLY_KEYS}
   * entry, so only a managed/enterprise config tier can set it.
   */
  allowManagedMcpServersOnly?: boolean;
  /**
   * SL1: custom status line. Either a bare shell command string whose stdout is
   * rendered as the status line, OR the Claude-Code-compatible object form
   * {@link StatusLineConfig} `{ command, padding }` — so a settings file ported from
   * the reference (`statusLine: { command, padding }`) parses unchanged instead of being
   * rejected as "must be a string". The object form's `padding` takes precedence over
   * the `SUPERINFERENCE_STATUSLINE_PADDING` env knob. Normalize both shapes with
   * {@link normalizeStatusLine}; the command auto-run trust gate still applies.
   * Parity: the reference's `settings.statusLine` object `{ type, command, padding }`.
   */
  statusLine?: string | StatusLineConfig;
  /** Bash commands exempted from the container sandbox (audit #4: settable + consumed,
   *  now also typed + merged across config layers). */
  sandboxExcludedCommands?: string[];
  /**
   * Managed sandbox-egress lockdown: when true, the container sandbox permits ONLY
   * managed/approved network domains — a project/local tier cannot widen egress. A
   * {@link MANAGED_ONLY_KEYS} entry (managed tier is the sole source); authoritative
   * in {@link mergeConfigs}. Parity: the reference's `shouldAllowManagedSandboxDomainsOnly`.
   */
  allowManagedSandboxDomainsOnly?: boolean;
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
   * Parity: the reference's persisted `migrationVersion`.
   */
  migrationVersion?: number;
  /** Override the composer-hint surface: inject custom hints / drop the built-ins. */
  composerHintsOverride?: { hints?: Array<{ key: string; label: string; learnedAfter: number }>; excludeDefault?: boolean };
  /**
   * Worktree behavior knobs (parity: the reference's `settings.worktree`).
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
   * `tmux` (default false) opts a freshly-created worktree into a detached tmux
   * session rooted at the worktree (parity: the reference's `--tmux` worktree mode):
   * enter_worktree spawns the session and surfaces the `tmux attach -t <name>`
   * command, and exit_worktree tears it down on remove / reminds how to reattach on
   * keep. When ABSENT or false, no tmux session is created.
   */
  worktree?: { symlinkDirectories?: string[]; sparsePaths?: string[]; tmux?: boolean };
  /**
   * Persistent `web_fetch` domain policy (parity: the reference's preapproved /
   * trusted-docs domain list persisted as a saved setting, as opposed to the
   * per-REQUEST `allowed_domains` / `blocked_domains` tool params). A saved policy
   * lets an operator pin a durable allow/deny-list once instead of repeating it on
   * every call:
   *  - `blockedDomains` is ALWAYS enforced — a matching host is refused regardless of
   *    any per-request allow-list (deny wins), so an org/project deny-list cannot be
   *    overridden from the tool call.
   *  - `allowedDomains`, when non-empty, restricts `web_fetch` to those hosts UNLESS
   *    the individual request supplies its own `allowed_domains` (a per-request
   *    allow-list overrides the saved baseline for that one call).
   * Entries use the same host/path grammar as the request params (consumed by
   * {@link matchDomainRule}): a bare host (`example.com`, subdomain-inclusive) or a
   * path-scoped entry (`github.com/anthropics`, segment-boundary matched). Read by
   * `core/core/src/tools/web-fetch.ts` off the project config tier (like
   * `toolOutputMaxChars`).
   */
  webFetch?: { allowedDomains?: string[]; blockedDomains?: string[] };
  /**
   * Commit / PR attribution override (parity: the reference's `attribution.commit` /
   * `attribution.pr` settings). When `commit` is a non-empty string it REPLACES the
   * default `Co-Authored-By: AMI …` trailer the git_commit tool appends; when it is
   * the empty string `""` the trailer is suppressed entirely (same effect as the
   * `SUPERINFERENCE_DISABLE_CO_AUTHOR=1` env opt-out); when absent the default AMI
   * trailer is used. `pr` carries the analogous override text for PR-description
   * attribution. Consumed by {@link resolveCommitAttribution} in
   * core/core/src/tools/git-commit.ts.
   *
   * `auto` opts into repo-class-aware attribution (parity: the reference's
   * undercover auto-default): when `true` and no explicit `commit` override is
   * set, the default AMI trailer is appended ONLY when the repo's `origin` remote
   * is positively classified as owned (matches one of `ownRemotes`); on an
   * external/unknown/no-remote repo the trailer is suppressed (fail-safe-ON — the
   * strict side wins unless ownership is confirmed). `ownRemotes` holds normalized
   * remote substrings (e.g. `"github.com/myorg/"`) that mark a repo as owned.
   *
   * `enabled` is the settings-driven MASTER disable toggle (parity: the reference's
   * deprecated `includeCoAuthoredBy: false`, which zeroed both the commit and PR
   * attribution texts): setting `enabled: false` suppresses BOTH the commit trailer
   * AND the PR-body attribution line via settings.json alone — no env var required.
   * An explicit per-surface `commit`/`pr` string override still wins over it (the
   * most-specific setting takes precedence, matching the reference where the
   * `attribution` object overrides the deprecated boolean). `enabled: true` (or
   * absent) leaves normal resolution unchanged — it is not a force-on override.
   */
  attribution?: { commit?: string; pr?: string; auto?: boolean; ownRemotes?: string[]; enabled?: boolean };
  /**
   * Persisted preferred RESPONSE language (parity: the reference's `LanguagePicker`
   * settings knob). The English language NAME ("japanese", "french", …) the model
   * must answer in, regardless of the language the user writes in. Previously AMI
   * honored a response-language directive ONLY via the ephemeral
   * `SUPERINFERENCE_LANGUAGE` env var ({@link buildResponseLanguageSection}); this
   * field lets a user persist the choice as project/user config (and set it with
   * `/config set language`). Precedence: an explicit `SUPERINFERENCE_LANGUAGE` env
   * var still WINS (so a one-shot override is possible); when the env is unset this
   * field seeds it via {@link applyConfigLanguage}. An empty/blank value is treated
   * as "no preference" (English — the default — never gets a redundant directive).
   */
  language?: string;
  /**
   * Persisted per-skill usage statistics keyed by skill name (parity: the reference's
   * global-config `skillUsage`). Each entry is a {@link SkillUsageStat}
   * (`{ usageCount, lastUsedAt }`), written to the GLOBAL config on each skill
   * invocation via {@link persistSkillUsageStat} and read back at session start so a
   * skill's frecency ranking survives across sessions instead of resetting to zero on
   * every launch. A user-level, cross-project signal (lives in global config only).
   */
  skillUsage?: Record<string, SkillUsageStat>;
  /**
   * Persisted one-time preference / dialog "already shown" flags, keyed by a stable
   * preference name (parity: the reference's GlobalConfig one-shot knobs such as
   * `hasIdeAutoConnectDialogBeenShown` / `hasUsedBackgroundTask` / the bypass-permissions
   * acknowledgment). Each entry records that a one-time prompt/notice was surfaced so it
   * is never re-shown. Written to the GLOBAL config via {@link markPreferenceShown} and
   * read via {@link wasPreferenceShown}; internal, non-user-facing state (lives in global
   * config only, like {@link skillUsage}).
   */
  preferencesShown?: Record<string, boolean>;
  /**
   * Persisted seen-COUNTS for count-limited notices, keyed by a stable notice name
   * (parity: the reference's "show at most MAX_SHOW_COUNT times" persisted seen-count
   * knob). A notice is shown while its count is below its cap; {@link recordNoticeSeen}
   * increments the count (capped) via the lock-safe updater and {@link shouldShowNotice}
   * gates on it. Internal, non-user-facing state (global config only).
   */
  noticeSeenCounts?: Record<string, number>;
  /**
   * Durable rollup of the PREVIOUS session's aggregated metric counters, persisted to the
   * PROJECT config on process exit (parity: the reference's StatsProvider `process.on('exit')`
   * flush of `store.getAll()` into `lastSessionMetrics` via `saveCurrentProjectConfig`). It
   * is a plain `name -> count` snapshot carried forward so the next launch can read the prior
   * session's rollup without re-aggregating the analytics log. Written via
   * {@link saveLastSessionMetrics} (which no-ops on an empty snapshot, matching the reference's
   * `Object.keys(metrics).length > 0` guard) and read via {@link readLastSessionMetrics};
   * internal, non-user-facing state (project config only, like {@link skillUsage}).
   */
  lastSessionMetrics?: Record<string, number>;
  /**
   * How many times the per-project onboarding affordance has been surfaced, used for
   * hint-fatigue capping (parity: the reference's `projectOnboardingSeenCount`). Once it
   * reaches {@link PROJECT_ONBOARDING_SEEN_CAP} the nudge stops showing. Incremented via
   * {@link incrementProjectOnboardingSeenCount} and consulted by
   * {@link shouldShowProjectOnboarding}; internal per-project state (project config only).
   */
  projectOnboardingSeenCount?: number;
  /**
   * True once the user has completed the per-project onboarding (parity: the reference's
   * `hasCompletedProjectOnboarding`). A set flag permanently suppresses the onboarding
   * nudge regardless of the seen-count. Set via {@link maybeMarkProjectOnboardingComplete}
   * and consulted by {@link shouldShowProjectOnboarding}; internal per-project state.
   */
  hasCompletedProjectOnboarding?: boolean;
  /**
   * Preferred terminal-emulator name for deep-link launches (parity: the reference's GLOBAL
   * `deepLinkTerminal`). It is the only preference that survives into a headless
   * LaunchServices / browser-link context where `TERM_PROGRAM` is unset, so it is persisted
   * to the GLOBAL config via {@link saveDeepLinkTerminal} and read via
   * {@link resolveDeepLinkTerminal}; a user preference (global config only).
   */
  deepLinkTerminal?: string;
  /**
   * Override (ms) for the slow-operation deferral delay used by interactivity-aware
   * background scheduling (default {@link BACKGROUND_SLOW_OP_DELAY_MS}); parity: the reference
   * Code's `DELAY_VERY_SLOW_OPERATIONS_THAT_HAPPEN_EVERY_SESSION`. Read via
   * {@link resolveBackgroundSlowOpDelayMs}; a cross-layer tunable knob.
   */
  backgroundSlowOpDelayMs?: number;
  /**
   * Override (ms) for the "user recently interacted" recency window that defers heavy
   * background work so it never competes with an actively typing user (default
   * {@link BACKGROUND_INTERACTION_RECENCY_MS}); parity: the reference's 1-minute
   * `getLastInteractionTime() > Date.now() - 1000*60` guard. Read via
   * {@link resolveBackgroundInteractionRecencyMs}; a cross-layer tunable knob.
   */
  backgroundInteractionRecencyMs?: number;
  /**
   * Persistent registry of known LOCAL clone paths for a GitHub repository, keyed by
   * the lowercased `owner/repo` (parity: the reference's global-config `githubRepoPaths`).
   * Each value is a most-recently-used list of absolute, symlink-resolved, NFC-normalized
   * clone roots — the clone a repo was most recently used from is kept first. Written via
   * {@link updateGithubRepoPathMapping}/{@link removePathFromRepo} and read via
   * {@link getKnownPathsForRepo}; internal, cross-project user STATE (global config only,
   * like {@link skillUsage}). Lets a repo-scoped feature resolve "where on disk is a clone
   * of owner/repo?" without re-scanning the filesystem.
   */
  githubRepoPaths?: Record<string, string[]>;
  /**
   * Glob/path patterns for instruction (CLAUDE.md / AGENTS.md / SUPERINFERENCE.md /
   * `*.local.md`) files to EXCLUDE from the auto-loaded memory context (parity:
   * the reference's `claudeMdExcludes` setting / `isClaudeMdExcluded`). A user can opt
   * specific instruction files out of loading — e.g. a vendored
   * `"**\/node_modules/**\/CLAUDE.md"` or an oversized legacy `"docs/OLD.md"` — without
   * deleting them. Previously AMI only honored excludes from the ephemeral
   * `AMI_MEMORY_EXCLUDES` env var; this field lets the choice be persisted as
   * project/user config. Patterns ACCUMULATE across config layers and are UNIONED with
   * the env-var list (both are honored). Team-managed `.ami/rules/` files are never
   * excludable (those are the project's enforced rules). Consumed by
   * {@link resolveConfiguredMemoryExcludes} and the memory loader's
   * `resolveMemoryExcludes`.
   */
  memoryExcludes?: string[];
  /**
   * Persisted, explicit user CONSENT to let instruction (CLAUDE.md / AGENTS.md /
   * SUPERINFERENCE.md) `@path` imports resolve files OUTSIDE the launch workspace
   * (parity: the reference's `hasClaudeMdExternalIncludesApproved`,
   * components/ClaudeMdExternalIncludesDialog.tsx). AMI's default posture is a hard
   * block — an `@~/shared.md` / `@../sibling/standards.md` is replaced with a visible
   * "import blocked — outside workspace" marker so a crafted memory file can never
   * exfiltrate arbitrary files into the system prompt. This field is the sanctioned,
   * recorded escape hatch for a trusted layout (e.g. a monorepo whose project
   * CLAUDE.md imports a shared parent-dir file): a tri-state — `undefined` (never
   * asked → blocked), `true` (approved → external imports load), `false` (declined →
   * blocked). It is deliberately absent by default; a value here is a one-time human
   * decision, not something a scanned instruction file can set for itself. Consumed by
   * {@link resolveConfiguredExternalMemoryImports} and `MemoryManager.resolveAtImports`.
   */
  externalMemoryImportsApproved?: boolean;
  /**
   * OPTIONAL allowlist of absolute root directories an approved external import must
   * resolve within ({@link externalMemoryImportsApproved} must also be `true`). When
   * present it narrows the consent to specific trusted roots (e.g. the monorepo top);
   * when absent but approved, any outside-workspace import is permitted (approve-once
   * for a wholly trusted repo). `~` is expanded. Patterns ACCUMULATE across config
   * layers (a higher layer adds more trusted roots). The real (symlink-resolved) path
   * of each imported file is checked against these roots so a symlink cannot escape
   * them. Consumed by {@link resolveConfiguredExternalMemoryImports}.
   */
  externalMemoryImportRoots?: string[];
  // -------------------------------------------------------------------------
  // Engine-behavior knobs read off the MERGED config by the engine factory.
  //
  // Each of these was already READ by a `common/src/engine-factory.ts` /
  // `feature-flags.ts` resolver off the config object the factory builds with
  // {@link mergeConfigs}, yet NONE of them were copied by that merge — so a value
  // set in `.ami/config.json` (or any file tier) was silently dropped and the
  // documented "project-config <key>" knob never took effect (it only worked when a
  // caller injected a `projectConfig` object directly, bypassing the file waterfall).
  // Declaring them here (with their runtime base type in {@link PROJECT_CONFIG_FIELD_TYPES})
  // makes them recognized keys (no spurious unknown-key typo warning), value-validated,
  // and — paired with their new entries in {@link mergeConfigs} — actually honored from
  // the config files. Parity: the reference's settings-schema keys that flow through the
  // settings cascade into the running session.
  // -------------------------------------------------------------------------
  /** Opt out of the architect/advisor model slot (parity: engine-factory `architectEnabled:false`). */
  architectEnabled?: boolean;
  /** Explicit architect/advisor model id (parity: engine-factory `architectModel`). */
  architectModel?: string;
  /** Fold file search into another surface, dropping the standalone grep/glob tools. */
  embeddedSearchTools?: boolean;
  /** Teammate/subagent execution backend: `auto` | `in-process` | `tmux`. */
  teammateExecutionMode?: string;
  /** Opt in to the unattended-retry loop for a detached/autonomous run. */
  unattendedRetry?: boolean;
  /** Toggle file checkpointing/mutation snapshots (parity: the reference's fileCheckpointingEnabled). */
  fileCheckpointingEnabled?: boolean;
  /** Capture a V8 heap snapshot the first time process RSS crosses the memory-warning
   *  high-water mark, instead of only logging (parity: the reference's heapDumpService auto-1.5GB). */
  heapDumpOnMemoryWarning?: boolean;
  /**
   * Asymmetric positive-only cache of machine-local capability detections keyed by
   * name (e.g. `dockerAvailable`). Only `true` values are ever persisted — a
   * negative is never written, so a config shared across machines can't be poisoned
   * by a machine that lacks the capability (parity: the reference's
   * `cachedChromeExtensionInstalled`). Written/read via reference-journal's
   * `persistPositiveCapability` / `readCachedCapability`.
   */
  cachedCapabilities?: Record<string, boolean>;
  /**
   * Named capability feature-flags keyed by feature name (parity: the reference's
   * `features` map / `feature()`): a `false` value disables a named feature, `true`
   * force-enables it, overriding the per-feature default. Env flags
   * (`SUPERINFERENCE_FEATURE_<NAME>` / `CLAUDE_CODE_FEATURE_<NAME>`) still win.
   */
  features?: Record<string, boolean>;
  /** Provider errors (HTTP 529) tolerated before stepping to the fallback model chain. */
  max529RetriesBeforeFallback?: number;
  /** Route OpenRouter traffic through a zero-data-retention endpoint. */
  openrouterZdr?: boolean;
  /** Fail closed when an MCP server config is invalid instead of skipping it. */
  strictMcpConfig?: boolean;
  /** Require explicit approval before any MCP tool call. */
  mcpRequireApproval?: boolean;
  /** Cap (ms) on how long a 429/usage-limit wait may block before giving up. */
  maxUsageLimitWaitMs?: number;
  /** Turn interval (positive integer) at which session-memory extraction runs. */
  sessionMemoryInterval?: number;
  /** MCP server names to always keep connected even under lazy/deferred loading. */
  alwaysLoadMcpServers?: string[];
  /** Continue the launch on an invalid config file instead of exiting. */
  continueOnInvalidConfig?: boolean;
  /** Named output style/persona preset applied to the system prompt. */
  outputStyle?: string;
  /**
   * Include the git guidance + dynamic git-context block in the system prompt
   * (parity: the reference's `includeGitInstructions`). Defaults ON when unset; the
   * `SUPERINFERENCE_DISABLE_GIT_INSTRUCTIONS` env var overrides this knob.
   */
  includeGitInstructions?: boolean;
  /**
   * Suppress proactive plugin-installation hints (parity: the reference's
   * `disableHintRecommendations` preference). When `true`, the `/plugin recommend`
   * flow withholds the prompt. Toggled via `/plugin hints on|off`.
   */
  pluginHintsDisabled?: boolean;
  /**
   * Path to an additional CA-certificate bundle (PEM) applied to
   * `NODE_EXTRA_CA_CERTS` at startup — BEFORE any TLS connection — so Node picks up
   * a corporate-proxy / TLS-inspecting CA without the user having to export the env
   * var before every launch (parity: the reference's
   * `caCertsConfig.applyExtraCACertsFromConfig`). An explicit `NODE_EXTRA_CA_CERTS`
   * already in the environment WINS (one-shot override); otherwise this value seeds
   * it. The referenced bundle must be an existing, readable regular file — a
   * missing/unreadable path is ignored (fail-safe: never point Node at a bad CA
   * bundle). Applied by {@link applyConfigCaCerts}.
   */
  extraCaCerts?: string;
  /**
   * Extra filesystem directories granted to the tool-access workspace from the
   * first turn (parity: the reference's `permissions.additionalDirectories`). Unioned
   * with the remembered `/add-dir` store into the PermissionManager allowed read
   * roots at engine setup. Relative entries resolve against the project root.
   * Patterns ACCUMULATE across config layers (a higher layer adds more dirs, never
   * removes a lower layer's). Resolved by {@link resolveAdditionalDirectories}.
   */
  additionalDirectories?: string[];
  /**
   * When `true`, file globbing honors the project's `.gitignore` (nested +
   * `.git/info/exclude`); when `false` it ignores it (parity: the reference's
   * `respectGitignore` file-picker toggle). Bridged to the existing
   * `AMI_GLOB_NO_IGNORE` env knob by {@link applyConfigGitignore}; an explicit env
   * value WINS. Unset keeps AMI's default (glob does NOT filter through gitignore).
   */
  respectGitignore?: boolean;
  /**
   * When `false`, automatic context compaction is disabled (parity: the reference's
   * persisted `autoCompactEnabled` toggle). Bridged to the existing
   * `SUPERINFERENCE_DISABLE_AUTO_COMPACT` env knob by {@link applyConfigAutoCompact};
   * an explicit env value WINS. Unset/`true` keeps auto-compaction on (the default).
   */
  autoCompactEnabled?: boolean;
  /**
   * Custom directory for plan-mode `.md` files (parity: the reference's
   * `plansDirectory`). Resolved RELATIVE to the project root and validated to stay
   * within it by the plan-mode tool. Bridged to the existing `AMI_PLANS_DIR` env
   * knob by {@link applyConfigPlansDirectory}; an explicit env value WINS.
   */
  plansDirectory?: string;
  /**
   * Opt into the Anthropic `fast-mode` beta (parity: the reference's `fast-mode`
   * beta knob, token `fast-mode-2026-02-01`). When `true` the token is appended to
   * the `anthropic-beta` header via the `ANTHROPIC_BETAS` escape hatch (consumed by
   * `getExtraAnthropicBetas`). Applied by {@link applyConfigAnthropicBetas}.
   */
  fastMode?: boolean;
  /**
   * Opt into the Anthropic `redact-thinking` beta (parity: the reference's
   * `redact-thinking` request beta / `showThinkingSummaries`, token
   * `redact-thinking-2026-02-12`). When `true` the token is appended to the
   * `anthropic-beta` header via the `ANTHROPIC_BETAS` escape hatch. Applied by
   * {@link applyConfigAnthropicBetas}.
   */
  redactThinking?: boolean;
  // -------------------------------------------------------------------------
  // Engine-behavior knobs (round 2) that {@link mergeConfigs} previously DROPPED.
  //
  // Each of these was ALREADY read off the MERGED project config by a
  // `common/src/engine-factory.ts` resolver, yet NONE was copied by that merge — so a
  // value set in `.ami/config.json` (or any file tier) was silently discarded and the
  // documented knob only ever took effect when a caller injected a `projectConfig`
  // object directly (bypassing the file waterfall). Declaring them here (+ their type
  // in {@link PROJECT_CONFIG_FIELD_TYPES} and their copy in {@link mergeConfigs}) makes
  // them recognized (no spurious unknown-key warning), value-validated, and actually
  // honored from the config files — the same fix the w78 cluster applied. Same class of
  // bug, newly-found keys. Parity: the reference's settings-schema keys flowing through
  // the settings cascade into the running session.
  // -------------------------------------------------------------------------
  /**
   * Gate the eager background session-memory extraction pass (parity: the reference's
   * `autoMemoryEnabled` settings key). Read by `engine-factory` {@link resolveAutoMemory};
   * an explicit `false` forces consolidation OFF regardless of the `AMI_DISABLE_AUTO_MEMORY`
   * env/default. Unset defers to the env + default-on behavior.
   */
  autoMemoryEnabled?: boolean;
  /**
   * Alias of {@link autoMemoryEnabled} gating background memory consolidation (parity:
   * the reference's `autoDreamEnabled` settings key). `engine-factory`
   * {@link resolveAutoMemory} accepts either name (and `sessionMemoryEnabled`) as the
   * equivalent tri-state; a real boolean wins over the env/default.
   */
  autoDreamEnabled?: boolean;
  /**
   * AMI-native alias of {@link autoMemoryEnabled} (also honored by `engine-factory`
   * {@link resolveAutoMemory}). Kept so a pre-existing `sessionMemoryEnabled` in a
   * config file takes effect rather than being silently dropped by the merge.
   */
  sessionMemoryEnabled?: boolean;
  /**
   * Off-switch for loading the shared TEAM-memory store (the `.ami/memory/team/`
   * subtree {@link MemoryManager.loadTeamMemories} reads into the system prompt).
   * Parity: the reference gates team memory behind an enable flag
   * (`feature('TEAMMEM')` + `isTeamMemoryEnabled()`); AMI previously ALWAYS loaded any
   * present team-memory files with no config knob. This additive tri-state lets a user
   * or org turn team-memory loading OFF — set `false` and the team subtree is never
   * read/rendered (a project may not want VCS-shared memories injected into every
   * session). Unset/`true` keeps AMI's default (load when present), so the knob is a
   * pure superset off-switch. Resolved per workspace by {@link resolveTeamMemoryLoadingEnabled}
   * across the full config waterfall with the usual {@link mergeConfigs} last-wins
   * precedence (local > project > global > managed). Distinct from the WRITE-classification
   * gate ({@link isTeamMemoryEnabled}, env-only) — this governs READING the shared store.
   */
  teamMemoryEnabled?: boolean;
  /**
   * Persist whether the per-turn conversation-title generation pass runs (read by
   * `engine-factory` {@link resolveTitleGenerationEnabled}). An explicit boolean is the
   * config tier of its precedence waterfall (opt flag > `AMI_ENABLE_TITLE_GENERATION`
   * env > this config boolean > non-interactive/SDK default-off). Unset keeps the
   * interactive default-on.
   */
  enableTitleGeneration?: boolean;
  /**
   * Persist whether the worktree capability tools (`enter_worktree` / `exit_worktree`)
   * are advertised to the model (read by `engine-factory` {@link resolveWorktreeToolsEnabled};
   * parity: the reference's worktree-mode toggle `isWorktreeModeEnabled`). Config tier of
   * the precedence waterfall (opt flag > `AMI_WORKTREE_TOOLS` env > this config boolean >
   * default-on). A locked-down/CI config can drop them as a unit.
   */
  worktreeToolsEnabled?: boolean;
  /**
   * Off-switch for the next-prompt SPECULATION feature (parity: the reference's
   * `speculationEnabled` global-config knob + `isSpeculationEnabled()` gate). Speculation
   * defaults ON; an explicit `false` disables it. Resolved per workspace by
   * {@link resolveSpeculationEnabled} across the full config waterfall and fed to the
   * cli `resolvePromptSuggestionEnabled` waterfall (env override > non-interactive >
   * this setting) and the `isSpeculationEnabled({ enabled })` gate. Unset keeps the
   * default-on behavior, so the knob is a pure superset off-switch.
   */
  speculationEnabled?: boolean;
  /**
   * Opt-OUT of enabling model-default thinking at session start (parity: the reference's
   * `alwaysThinkingEnabled` setting consulted by `shouldEnableThinkingByDefault()`).
   * AMI already auto-adopts a reasoning model's default thinking level unless the user
   * pins one ({@link import('../../../common/src/model-thinking').deriveModelDefaultThinking}).
   * Setting this to `false` SUPPRESSES that auto-on so a reasoning model starts with no
   * thinking budget (the user can still turn it on per-session with `/thinking`). The
   * `MAX_THINKING_TOKENS` env var still wins (a positive value forces default-on, `0`
   * forces off). Unset/`true` keeps AMI's default (auto-on for reasoning models).
   * Resolved by {@link shouldEnableThinkingByDefault} across the full config waterfall.
   */
  alwaysThinkingEnabled?: boolean;
  /**
   * Persisted auto-connect-to-IDE toggle (parity: the reference's `autoConnectIde`
   * settings key / the `addIde` waterfall in useIDEIntegration.ts). ONE enabling
   * input to the engine's IDE auto-connect waterfall ({@link import('./mcp').shouldAutoConnectIde});
   * a supported editor terminal, an inherited `AMI_SSE_PORT`, or a truthy
   * `AMI_AUTO_CONNECT_IDE` env also enable it, while an explicit-falsy
   * `AMI_AUTO_CONNECT_IDE` is a hard opt-out. Threaded into
   * {@link EngineConfig.autoConnectIde} by the engine factory.
   */
  autoConnectIde?: boolean;
  /**
   * Show the OSC 9;4 taskbar/tab progress indicator in supported terminals (parity:
   * the reference's `terminalProgressBarEnabled` settings key). AMI emits progress by
   * default (its long-standing behavior); an explicit `false` suppresses it by seeding
   * the `AMI_NO_PROGRESS` kill-switch via {@link applyConfigTerminalProgress}, so the
   * persisted preference no longer requires exporting the env var. An explicit
   * `AMI_NO_PROGRESS` env value still WINS. Unset/`true` keeps AMI's default-on
   * (superset divergence from the reference's default-off-unless-set — AMI never regresses the
   * loved default, only ADDs the opt-out knob).
   */
  terminalProgressBarEnabled?: boolean;
  /**
   * Show the per-turn duration on the completion line ("✳ Worked FOR 1m 3s · done …")
   * (parity: the reference's `showTurnDuration` settings key). AMI shows it by default;
   * an explicit `false` suppresses ONLY the duration (keeping the verb/wall-clock) by
   * seeding `AMI_SHOW_TURN_DURATION` via {@link applyConfigTurnDuration}, so the choice
   * can be persisted instead of exporting the env var. An explicit `AMI_SHOW_TURN_DURATION`
   * env value still WINS. Unset/`true` keeps the default-on.
   */
  showTurnDuration?: boolean;
  /**
   * Persisted verbose output-detail toggle (parity: the reference's global-config
   * `verbose` boolean, default false). When `true`, the render consumers that already
   * key off the `AMI_VERBOSE` env knob (the API-error display + the tool-result
   * descriptor's full-vs-truncated rendering, via the cli `resolveVerboseEnabled`)
   * expand truncated transcript detail. AMI previously exposed verbose ONLY through the
   * `--verbose` flag and the explicit `--config` file key (cli `resolveVerbose`), never
   * through the persisted settings waterfall, so a `verbose: true` in a normal
   * `.ami/config.json` (project/global/local) was silently dropped. Bridged to
   * `AMI_VERBOSE` by {@link applyConfigVerbose}; an explicit `AMI_VERBOSE` env value (or
   * the `--verbose` flag / `--config` key that seeds it) still WINS. Unset/`false` keeps
   * the concise default.
   */
  verbose?: boolean;
  /**
   * Terminal selection auto-copy toggle (parity: the reference's global-config
   * `copyOnSelect ?? true`): when a drag-select completes, the highlighted text is
   * written to the clipboard (and the "ctrl+c copy" hint is surfaced). Defaults ON;
   * an explicit `false` disables the drag-release clipboard write by seeding the
   * existing `AMI_NO_COPY_ON_SELECT` opt-out that the cli `copyOnSelectEnabled` already
   * reads, so the choice can be persisted instead of exporting the env var. An explicit
   * `AMI_NO_COPY_ON_SELECT` env value still WINS. Bridged by {@link applyConfigCopyOnSelect}.
   */
  copyOnSelect?: boolean;
  /**
   * Persisted "always copy the full response" preference for `/copy` (parity: the reference
   * Code's global-config `copyFullResponse` — commands/copy/copy.tsx L89-L92,L356-L369).
   * When `true`, `/copy --list` bypasses the code-block picker menu entirely and copies
   * the full reply directly (the picker's `always` opt-in, surfaced in AMI as
   * `/copy --always`, persists this knob). Unset/`false` keeps the interactive
   * `--list`/`--block` block-selection flow. Settable via `/config set copyFullResponse`.
   */
  copyFullResponse?: boolean;
  /**
   * Interactive-TUI layout mode (parity: the reference's `isFullscreenEnvEnabled()`
   * fullscreen-vs-scrollback toggle). Defaults ON: AMI renders in the terminal's
   * ALTERNATE SCREEN buffer (a fixed-height fullscreen canvas that leaves the host
   * scrollback untouched on exit). An explicit `false` opts INTO a scrollback layout —
   * the TUI stays on the normal screen so output accumulates in the terminal's native
   * scrollback instead of a fullscreen overlay — by seeding the existing
   * `AMI_NO_ALT_SCREEN` kill-switch that {@link import('../../../cli/src/render').detectTerminalCapabilities}'s
   * `alternateScreen` capability already reads. AMI previously exposed this ONLY via the
   * `AMI_NO_ALT_SCREEN` / `AMI_FORCE_ALT_SCREEN` env vars, never the persisted settings
   * waterfall, so a user who prefers scrollback had to export an env var every session.
   * `true` symmetrically seeds `AMI_FORCE_ALT_SCREEN` so the preference survives the
   * tmux-control-mode auto-off default. Bridged by {@link applyConfigFullscreen}; an
   * explicit `AMI_NO_ALT_SCREEN` / `AMI_FORCE_ALT_SCREEN` env value still WINS.
   */
  fullscreen?: boolean;
  /**
   * Disable syntax highlighting of fenced code blocks and diff lines (parity: the reference
   * Code's `syntaxHighlightingDisabled` cosmetic config knob). When `true`, the single
   * highlight on/off source the cli reads (`syntaxHighlightEnabled` →
   * `AMI_SYNTAX_HIGHLIGHT`) renders plain source while the rest of the UI stays colored.
   * AMI previously honored this only via the `--config` file key (cli
   * `resolveSyntaxHighlightingDisabled`), never the persisted settings waterfall; bridged
   * to `AMI_SYNTAX_HIGHLIGHT='0'` by {@link applyConfigSyntaxHighlighting}. An explicit
   * `AMI_SYNTAX_HIGHLIGHT` env value still WINS. Unset/`false` keeps highlighting on.
   */
  syntaxHighlightingDisabled?: boolean;
  /**
   * Idle threshold (ms) before a "generation done" desktop notification fires (parity:
   * the reference's numeric `messageIdleNotifThresholdMs`, default 60000). AMI already has
   * desktop notifications with an `AMI_DESKTOP_NOTIFY_THRESHOLD_MS` env knob (cli
   * `getNotifyThreshold`); this persists the threshold in the settings waterfall instead
   * of requiring the env var. A positive finite value seeds the env knob via
   * {@link applyConfigIdleNotifThreshold}; an explicit `AMI_DESKTOP_NOTIFY_THRESHOLD_MS`
   * env value still WINS. A non-positive/non-finite value is ignored (keeps the default).
   */
  messageIdleNotifThresholdMs?: number;
  /**
   * Show model thinking summaries (parity: the reference's `showThinkingSummaries`
   * settings knob, default off). When explicitly `false` (summaries OFF), the
   * `redact-thinking` Anthropic beta token is appended to the `anthropic-beta` header
   * (via the same `ANTHROPIC_BETAS` escape hatch as {@link redactThinking}) — trading
   * thinking-summary display for lower latency. The inverse of {@link redactThinking}:
   * either `showThinkingSummaries === false` OR `redactThinking === true` arms the beta.
   * Applied by {@link applyConfigAnthropicBetas}. Unset/`true` is a no-op (summaries on).
   */
  showThinkingSummaries?: boolean;
  /**
   * Customize the playful working-spinner gerund pool (parity: the reference's cosmetic
   * `spinnerVerbs` config knob). `mode: 'append'` (default) widens the built-in pool
   * with `verbs`; `mode: 'replace'` swaps it entirely (an empty list falls back to the
   * built-ins). Serialized to the existing `AMI_SPINNER_VERBS` JSON env knob the cli
   * `resolveSpinnerVerbConfig` already reads, by {@link applyConfigSpinnerVerbs}, so the
   * override can be persisted in the settings waterfall instead of exporting the env var.
   * An explicit `AMI_SPINNER_VERBS` env value still WINS.
   */
  spinnerVerbs?: { mode?: 'append' | 'replace'; verbs?: string[] };
  /**
   * Persisted toggle for background-task functionality (parity: the reference's
   * `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS` env gate behind which the entire
   * background-task path — the bash tool's `run_in_background` parameter, its
   * background guidance, and the task tool's background mode — is disabled). AMI
   * already honors this as the {@link import('./tools/tool-gates').DISABLE_BACKGROUND_TASKS_ENV}
   * env knob (`AMI_DISABLE_BACKGROUND_TASKS` / the Claude-compatible
   * `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS` alias) consumed by {@link isBackgroundTasksDisabled}
   * in `tools/bash.ts` + `tools/task.ts`, but there was no way to PERSIST the choice —
   * a value in `.ami/config.json` (any file tier) was silently dropped. An explicit
   * `false` disables background tasks by seeding that env knob via
   * {@link applyConfigBackgroundTasks}, so the drop takes effect from the first turn
   * (the bash tool schema omits `run_in_background` and a background-requested command
   * runs in the foreground). An explicit `AMI_DISABLE_BACKGROUND_TASKS` /
   * `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS` env value still WINS (one-shot override).
   * Unset/`true` keeps background tasks on (AMI's default).
   */
  backgroundTasksEnabled?: boolean;
  // -------------------------------------------------------------------------
  // Admin-policy / engine knobs (round 3) that a `common/` resolver ALREADY reads
  // but config.ts neither declared, value-validated, nor (for the merged-path keys)
  // copied through {@link mergeConfigs} — so a value set in `.ami/config.json` (any
  // file tier) was silently dropped AND surfaced as a spurious unknown-key typo
  // warning. Same class of bug as the engine-behavior rounds above; newly-found keys.
  // Declaring them here (+ their entry in {@link PROJECT_CONFIG_FIELD_TYPES} + their
  // copy in {@link mergeConfigs}) makes them recognized, base-type validated, and
  // actually honored. Parity: the reference's policyLimits / pluginTrustMessage /
  // marketplace-policy settings flowing through the settings cascade.
  // -------------------------------------------------------------------------
  /**
   * Generic admin/enterprise feature-restriction map (parity: the reference's
   * `services/policyLimits` `restrictions: Record<string, { allowed: boolean }>`).
   * Keyed by a named policy/feature (`computer_use`, `bash_classifier`,
   * `task_tracker`, …); only BLOCKED policies need an entry — an absent key is
   * allowed. A present entry whose `allowed` is `false` DENIES that named feature.
   * Read by `common/src/feature-flags.ts` {@link import('./types')} `isPolicyAllowed`
   * (fail-open: no map / unknown key / malformed entry → allowed) off the MERGED
   * project config, so {@link mergeConfigs} must copy it. Managed-authoritative: a
   * lower tier may ADD denies for new policies, but it can never RELAX a managed
   * deny (managed entries win), so enterprise policy is not defeatable locally.
   */
  policyRestrictions?: Record<string, { allowed: boolean }>;
  /**
   * Org/enterprise custom trust-message appended to the plugin-install warning
   * (parity: the reference's `getPluginTrustMessage` / managed `pluginTrustMessage`
   * setting). A non-empty string replaces/augments the built-in advisory with the
   * org's own policy copy (e.g. "Only install plugins approved in the internal
   * registry — see go/plugins"). Read by `common/src/plugin-commands.ts`
   * `resolvePluginTrustWarning` (managed tier first, then global). Declaring it here
   * makes a managed/global config carrying it recognized + value-validated instead of
   * flagged as an unknown-key typo.
   */
  pluginTrustMessage?: string;
  /**
   * RSS high-water mark (bytes) at which the engine treats memory pressure as
   * CRITICAL and sheds load (parity: the reference's critical-memory RSS threshold).
   * Read by `common/src/engine-factory.ts` off the merged project config
   * (`resolveEngineConfig` → `EngineConfig.memoryCriticalRssBytes`), so
   * {@link mergeConfigs} must copy it; a non-positive value is ignored by the
   * resolver. Unset keeps the engine default.
   */
  memoryCriticalRssBytes?: number;
  /**
   * Prefer a tmux teammate backend over iTerm2 panes when both are available
   * (parity: the reference's terminal-multiplexer preference). Read by
   * `common/src/engine-factory.ts` off the merged project config to resolve the
   * teammate execution surface; {@link mergeConfigs} must copy it. Unset/`false`
   * keeps the default auto-selection.
   */
  preferTmuxOverIterm2?: boolean;
  /**
   * Managed/enterprise policy-refresh tuning block (parity: the reference's remote
   * policy-limits background refresh knobs). `{ pollIntervalMs, maxRetries,
   * fetchTimeoutMs, failOpenOnStaleCache }` — each sub-field is clamped/validated
   * by `common/src/engine-factory.ts` `resolvePolicyRefreshConfig` off the merged
   * project config (env vars still win per-field), so {@link mergeConfigs} must copy
   * the block. Unset keeps the built-in refresh defaults.
   */
  policyRefresh?: {
    pollIntervalMs?: number;
    maxRetries?: number;
    fetchTimeoutMs?: number;
    failOpenOnStaleCache?: boolean;
  };
  /**
   * Enterprise marketplace host BLOCKLIST (parity: the reference's `blockedMarketplaces`
   * managed setting). Hosts named here are refused as plugin-marketplace sources
   * (deny-wins — the union across the managed + global tiers is enforced). Read by
   * `common/src/plugin-commands.ts` `sourceBlockedMarketplaces` (managed + global raw
   * config). Declaring it here makes it recognized + value-validated; entries
   * ACCUMULATE across layers (a higher layer adds more blocked hosts, never removes a
   * lower layer's).
   */
  blockedMarketplaces?: string[];
  /**
   * Enterprise marketplace host ALLOWLIST (parity: the reference's
   * `strictKnownMarketplaces` managed setting). When configured, ONLY these hosts may
   * be added as plugin-marketplace sources. Read by `common/src/plugin-commands.ts`
   * `sourceStrictKnownMarketplaces` (managed + global raw config). Declaring it here
   * makes it recognized + value-validated; entries ACCUMULATE across layers.
   */
  strictKnownMarketplaces?: string[];
  /**
   * Gate the extra "clear context" affordance offered when the user ACCEPTS a plan
   * (exit-plan-mode approval). Parity: the reference's `showClearContextOnPlanAccept`
   * setting (`ExitPlanModePermissionRequest.tsx` reads
   * `settings.showClearContextOnPlanAccept ?? false`) — when on, the plan-approval
   * prompt adds an "approve AND clear context" option so a long planning turn's tokens
   * don't carry into execution. Default OFF (the option is additive, opt-in). Resolved
   * by {@link shouldShowClearContextOnPlanAccept} off the MERGED config and consumed by
   * the exit-plan permission flow in `common/`. A merge knob (any tier may set it).
   */
  showClearContextOnPlanAccept?: boolean;
  /**
   * Whether the prompt-input footer renders the current branch's pull-request status
   * line (`common/src/pr-status.ts`). Parity: the reference's `prStatusFooterEnabled ??
   * true` (`PromptInputFooterLeftSide.tsx` `isPrStatusEnabled`). Default ON; set `false`
   * to suppress the PR footer entirely. Resolved by {@link isPrStatusFooterEnabled} off
   * the MERGED config and consumed by the footer renderer in `cli/`. A merge knob.
   */
  prStatusFooterEnabled?: boolean;
  /**
   * The release-notes/changelog VERSION the user has already seen, persisted in the
   * GLOBAL config so the one-time "what's new" banner is shown at most once per release.
   * Parity: the reference's `lastReleaseNotesSeen` (`LogoV2.tsx` gates the banner on
   * `config.lastReleaseNotesSeen !== VERSION`, then writes the current VERSION after it
   * is shown). Read by {@link getLastReleaseNotesSeen} / {@link hasUnseenReleaseNotes}
   * and advanced by {@link markReleaseNotesSeen}; consumed by `common/src/release-notes.ts`.
   * Persisted user STATE (like {@link preferencesShown}), not a cross-layer policy knob.
   */
  lastReleaseNotesSeen?: string;
  /**
   * Release channel the self-updater/launcher installs from: `latest` (newest build)
   * or `stable` (vetted releases only). A merged config knob, recognized + value-
   * validated here (enum) so an enterprise/global config pinning a channel is honored
   * by the update path rather than flagged as an unknown key. Parity: the reference's
   * `autoUpdatesChannel` setting.
   */
  autoUpdatesChannel?: 'latest' | 'stable';
  /**
   * Minimum acceptable version the launcher must satisfy before running (a managed/
   * enterprise floor; a below-floor install is upgraded/blocked by the update path).
   * A merged config knob, recognized + base-type validated here. Parity: the reference's
   * `minimumVersion` policy setting paired with {@link autoUpdatesChannel}.
   */
  minimumVersion?: string;
  /**
   * Opt-IN flag for voice-input mode (parity: the reference's `voiceEnabled`
   * settings boolean gating `isVoiceModeEnabled()` and the advertised `/voice`
   * command). Default OFF — voice mode is additive/opt-in, so only an explicit
   * `true` (at any config tier) enables it. Resolved by {@link isVoiceModeEnabled}
   * off the MERGED config; a merge knob (any tier may set it).
   */
  voiceEnabled?: boolean;
  /**
   * Opt-IN declaration that the user WANTS the auto-allow ("auto"/bypass) permission
   * mode (parity: the reference's `hasAutoModeOptIn()` settings flag). AMI's permission
   * model is opt-OUT — `auto-allow` is always in the Shift+Tab cycle unless managed
   * policy ({@link disableBypassPermissionsMode}) removes it — so this knob adds the
   * missing INTENT signal: it lets the UI distinguish "the user asked for auto but it
   * is unavailable (managed-blocked)" from "the user never requested auto". It does NOT
   * itself grant auto-allow (managed policy still wins); it only records interest.
   * Resolved by {@link hasAutoModeOptIn} off the MERGED config; a merge knob.
   */
  autoAllowOptIn?: boolean;
  /**
   * Which renderer to use for file diffs: `'auto'` (AMI picks the richest surface the
   * terminal supports) or `'terminal'` (always the plain in-terminal diff). Parity:
   * the reference's `diffTool` global-config knob (`'terminal' | 'auto'`, default `'auto'`).
   * A merge knob resolved off the merged config by {@link resolveDiffTool}; default
   * `'auto'` so an unset config keeps AMI's existing behavior.
   */
  diffTool?: 'terminal' | 'auto';
  /**
   * Cached list of frequently-modified project files used to seed example-command
   * suggestions (parity: the reference's per-project `exampleFiles`). Populated out-of-band
   * by a background scan; read (with a staleness check against {@link exampleFilesGeneratedAt})
   * by {@link getCachedExampleFiles}. A per-project STATE field, not a cross-layer policy knob.
   */
  exampleFiles?: string[];
  /**
   * Epoch-millis timestamp of when {@link exampleFiles} was last generated (parity:
   * the reference's `exampleFilesGeneratedAt`). {@link getCachedExampleFiles} treats a
   * cache older than {@link EXAMPLE_FILES_TTL_MS} as stale and returns none so the
   * caller refreshes. A per-project STATE field.
   */
  exampleFilesGeneratedAt?: number;
  /**
   * Default shell backing input-box `!` commands: `'bash'` (default on every platform)
   * or `'powershell'`. Parity: the reference's `defaultShell` setting + `resolveDefaultShell()`.
   * AMI does NOT auto-flip Windows to PowerShell (that would break existing bash-hook
   * users), so the default is `'bash'` everywhere. Resolved by {@link resolveDefaultShell}
   * off the merged config; a merge knob (any tier may set it).
   */
  defaultShell?: 'bash' | 'powershell';
  /**
   * Startup announcement strings shown once at launch (parity: the reference's managed
   * `companyAnnouncements`). Typically set by an enterprise/managed config. On the FIRST
   * startup the first entry is shown; afterwards one is chosen at random. Resolved by
   * {@link pickCompanyAnnouncement} off the merged config.
   */
  companyAnnouncements?: string[];
  /**
   * Policy knob gating whether a `dangerouslyDisableSandbox` tool override is honored.
   * Parity: the reference's `SandboxManager.areUnsandboxedCommandsAllowed()` consulted by
   * `shouldUseSandbox()` — the sandbox is skipped ONLY when the command opts out AND this
   * policy permits unsandboxed commands. Lets an operator HARD-DISABLE the override
   * independent of permission mode: `false` (at any tier, e.g. a managed config) forces
   * every command through the sandbox even when it requests `dangerouslyDisableSandbox`.
   * Default (unset) → `true` (override honored), preserving AMI's existing behavior.
   * Resolved by {@link areUnsandboxedCommandsAllowed} off the merged config.
   */
  allowUnsandboxedCommands?: boolean;
}
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
 * applies. Parity: the reference's `parseJsonc` + `safeParseJSON` LRU memoization.
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
// Config read caches + single invalidation point
// ---------------------------------------------------------------------------
//
// Parity: the reference's settings cache (settingsCache.ts) is a set of memos behind
// ONE resetSettingsCache() trigger fired on every settings write / --add-dir /
// plugin-init / hooks-refresh. AMI's config layer likewise accreted several memos
// (the content-keyed parseJsonc LRU above; the managed-path platform memo and the
// per-tier identity cache below). {@link resetConfigCache} ties them to a SINGLE
// invalidation point wired into {@link safeWriteConfig} so a config write never
// leaves a stale read snapshot behind.

/**
 * Platform(+PROGRAMDATA)-keyed memo of the managed-config DEVICE-DEFAULT path, so
 * the platform switch + join runs once per platform instead of on every resolve
 * (parity: the reference's lodash-memoized `getManagedFilePath`). Keyed by platform (and, on
 * Windows, the PROGRAMDATA that feeds the path) rather than memoized unconditionally,
 * so a forced different `process.platform` / `PROGRAMDATA` still resolves correctly.
 */
const managedPathCache = new Map<string, string>();
const tierConfigCache = new Map<string, TierConfigCacheEntry>();

/**
 * The OS-conventional device-level managed-config path (no env override), memoized per
 * platform. See {@link managedPathCache}. {@link getManagedConfigPath} applies the
 * `SUPERINFERENCE_MANAGED_CONFIG` override first, then falls back here.
 */
function getManagedConfigDefaultPath(): string {
  const key = process.platform === 'win32'
    ? `win32|${process.env.PROGRAMDATA || ''}`
    : String(process.platform);
  const hit = managedPathCache.get(key);
  if (hit !== undefined) return hit;
  let resolved: string;
  switch (process.platform) {
    case 'darwin':
      resolved = '/Library/Application Support/SuperInference/config.json';
      break;
    case 'win32':
      resolved = path.join(process.env.PROGRAMDATA || 'C:\\ProgramData', 'SuperInference', 'config.json');
      break;
    default:
      resolved = '/etc/superinference/config.json';
  }
  managedPathCache.set(key, resolved);
  return resolved;
}

/**
 * Load a per-tier config through the file-identity read cache ({@link tierConfigCache}).
 * Used by the project / local / global tier loaders so repeated reads of an unchanged
 * file across the config waterfall and diagnostic sweeps are memory hits. The
 * ConfigService watcher path deliberately calls {@link loadConfigFile} directly
 * (uncached) — it reacts to a detected change and must always read fresh.
 */
function loadTierConfigCached(configPath: string): ProjectConfig | null {
  // Stat for the identity key. An absent/unstattable file is a NEGATIVE identity
  // (-1/-1) so the common "no config here" case is also a memory hit.
  let mtimeMs = -1;
  let size = -1;
  try {
    const st = fsHooks.statSync(configPath);
    mtimeMs = st.mtimeMs;
    size = st.size;
  } catch {
    // Absent / broken symlink / not inspectable → negative identity.
  }
  const cached = tierConfigCache.get(configPath);
  if (cached && cached.mtimeMs === mtimeMs && cached.size === size) {
    return cached.config === null ? null : structuredClone(cached.config);
  }
  // Absent files skip loadConfigFile entirely (it would resolve to null anyway).
  const config = mtimeMs === -1 ? null : loadConfigFile(configPath);
  tierConfigCache.set(configPath, { mtimeMs, size, config });
  return config === null ? null : structuredClone(config);
}

/**
 * Clear EVERY config read memo in one call: the content-keyed parse LRU, the managed
 * device-path memo, and the per-tier identity cache. This is the single invalidation
 * point (parity: the reference's `resetSettingsCache`) fired by {@link safeWriteConfig} on
 * every config write; call it too after a `--add-dir`, plugin install, or hooks
 * refresh changes what the config layer would resolve.
 */
export function resetConfigCache(): void {
  jsoncParseCache.clear();
  managedPathCache.clear();
  tierConfigCache.clear();
  clearManagedConfigCache();
}

/**
 * Load a project-level config from `<cwd>/.ami/config.json`.
 * Returns `null` when the file does not exist.
 */
export function loadProjectConfig(cwd: string): ProjectConfig | null {
  return loadTierConfigCached(getProjectConfigPath(cwd));
}

/**
 * Load a local config from `<cwd>/.ami/config.local.json`.
 * This file is typically gitignored for user-specific overrides.
 */
export function loadLocalConfig(cwd: string): ProjectConfig | null {
  return loadTierConfigCached(getLocalConfigPath(cwd));
}

/**
 * Load the global config from `~/.ami/config.json`.
 * Returns `null` when the file does not exist.
 */
export function loadGlobalConfig(): ProjectConfig | null {
  const configPath = getGlobalConfigPath();
  if (!configPath) return null;
  return loadTierConfigCached(configPath);
}

/**
 * The managed-config base file path. An explicit `SUPERINFERENCE_MANAGED_CONFIG`
 * env var always wins (testing / non-standard deployments); otherwise the default
 * is PLATFORM-SPECIFIC so an enterprise policy lands in the OS-conventional
 * machine-wide location an administrator actually writes to — mirroring the reference
 * Code's `getManagedFilePath` platform switch:
 *
 *   - macOS   → `/Library/Application Support/SuperInference/config.json`
 *   - Windows → `%PROGRAMDATA%\SuperInference\config.json` (falls back to the
 *               conventional `C:\ProgramData` when PROGRAMDATA is unset)
 *   - Linux/other → `/etc/superinference/config.json`
 *
 * Previously this was hardcoded to the Linux `/etc` path on every platform, so a
 * managed policy deployed to the macOS/Windows convention was never read.
 */
export function getManagedConfigPath(): string {
  const override = process.env.SUPERINFERENCE_MANAGED_CONFIG;
  if (override) return override;
  // The OS-conventional device-level path is memoized per platform (see
  // {@link getManagedConfigDefaultPath}) so the switch + join runs once per process.
  return getManagedConfigDefaultPath();
}

/**
 * Build the ORDERED list of candidate managed-config sources, highest-priority
 * first. {@link loadManagedConfig} reads the first candidate that exists on disk,
 * so an org pushing BOTH a device-level profile and a per-user override has the
 * per-user one win — mirroring the reference's `getMacOSPlistPaths()` per-user >
 * device-level ordering (AMI previously resolved exactly ONE managed path with no
 * precedence).
 *
 *   - An explicit `SUPERINFERENCE_MANAGED_CONFIG` env var is authoritative: the
 *     single candidate (testing / non-standard deployments), unchanged from before.
 *   - macOS → per-user `/Library/Managed Preferences/<user>/SuperInference/config.json`
 *     (highest), then device-level `/Library/Managed Preferences/SuperInference/config.json`,
 *     then the legacy `/Library/Application Support/SuperInference/config.json`
 *     ({@link getManagedConfigPath}) as a back-compat device fallback. Username
 *     resolution is wrapped so a failed `os.userInfo()` (some sandboxes throw) still
 *     yields the device-level candidates. Parity: the reference's per-user > device plist order.
 *   - Windows / Linux → the single {@link getManagedConfigPath} default (ProgramData /
 *     `/etc`); there is no second native tier to layer, so the list has one entry.
 *
 * {@link getManagedConfigPath} (used for the drop-in dir, the managed `permissions.json`
 * location, and diagnostics) is unchanged — it remains the device-level default and is
 * always the LAST candidate, so this function is a pure additive superset.
 */
export function getManagedConfigPaths(
  deps: { userInfo?: () => { username: string } } = {},
): string[] {
  const override = process.env.SUPERINFERENCE_MANAGED_CONFIG;
  if (override) return [override];
  const deviceDefault = getManagedConfigPath();
  if (process.platform !== 'darwin') return [deviceDefault];
  const candidates: string[] = [];
  try {
    // Injectable so tests can drive the uid-less-sandbox throw without mocking
    // the non-configurable `os.userInfo` ESM export.
    const username = (deps.userInfo ?? (() => os.userInfo()))().username;
    if (username) {
      candidates.push(`/Library/Managed Preferences/${username}/SuperInference/config.json`);
    }
  } catch {
    // os.userInfo() can throw in a uid-less sandbox — fall through to the device paths.
  }
  candidates.push('/Library/Managed Preferences/SuperInference/config.json');
  // Deduplicate: the legacy Application Support default is appended last unless an
  // earlier candidate already named it (so the list never carries a repeat).
  if (!candidates.includes(deviceDefault)) candidates.push(deviceDefault);
  return candidates;
}

/**
 * The managed drop-in directory (`<managedBase>/config.d`). Admins can layer
 * additional managed policy as individual `*.json` files here instead of editing
 * one monolithic file (parity: the reference's managed-settings.d/ drop-ins).
 */
export function getManagedDropInDir(): string {
  return path.join(path.dirname(getManagedConfigPath()), 'config.d');
}

/**
 * Decide whether a managed `config.d/` directory entry is eligible to be read as a
 * policy fragment. Parity: the reference's managed-settings.d enumeration guard, which
 * defensively skips anything that is not a regular file or a symlink, does not end in
 * `.json`, or is a dotfile. AMI previously filtered ONLY on the `.json` suffix, so a
 * hidden sidecar/editor-swap file (`.10-team.json.swp` is excluded by suffix, but a
 * `.hidden.json` dotfile was NOT) could be read as policy, and a `*.json` NAMED entry
 * that is actually a directory / FIFO / socket / device slipped past the suffix filter
 * — a FIFO is the dangerous case: `readFileSync` on it BLOCKS indefinitely waiting for
 * a writer (a trivial DoS if an attacker can plant one in the drop-in dir).
 *
 * Eligibility (all must hold):
 *   - the name is NOT a dotfile (does not start with `.`);
 *   - the name ends in `.json`;
 *   - the on-disk TYPE is a regular file or a symlink (never a dir/FIFO/socket/device).
 *
 * `lstatSync` is used so the type decision does not follow a symlink — a symlinked
 * fragment is still enumerated here but is refused later by {@link assertTrustedManagedFile}
 * (AMI refuses symlinked managed sources outright), matching AMI's stricter trust stance
 * while keeping the reference's enumeration shape. When the stat seam cannot report a type (a
 * minimal test stub without `isFile`), the entry falls back to eligible — the per-file
 * read + trust check downstream still fails closed on a genuinely bad entry.
 */
export function isEligibleManagedDropIn(name: string, dir: string): boolean {
  if (name.startsWith('.') || !name.endsWith('.json')) return false;
  let st: fs.Stats;
  try {
    st = fsHooks.lstatSync(path.join(dir, name));
  } catch {
    // Unreadable/vanished between readdir and lstat — treat as absent.
    return false;
  }
  if (typeof st.isFile !== 'function') return true;
  return st.isFile() || st.isSymbolicLink();
}

/**
 * Verify that a managed-policy file is a trustworthy source before it is read, and
 * throw {@link ManagedConfigError} (fail-closed) otherwise. This is AMI's analog of
 * the reference sourcing managed policy only from OS-protected, admin-only locations
 * (registry HKLM / /Library/Managed Preferences) that an end user cannot redirect:
 *
 *  - a SYMLINK is refused outright — otherwise an unprivileged user could redirect
 *    the authoritative policy path at a file they control;
 *  - on POSIX, a WORLD-WRITABLE regular file is refused — a policy file any user on
 *    the machine can rewrite is not a trust anchor.
 *
 * When `relocatable` is set — i.e. the path came from the USER-settable
 * {@link SUPERINFERENCE_MANAGED_CONFIG} override rather than the fixed device default — a
 * STRICTER, admin-grade OWNERSHIP check additionally applies (unless the
 * {@link MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV} seam is engaged): a file owned by NEITHER root
 * (uid 0) NOR the current euid is refused. An override pointed at another unprivileged
 * user's file is a policy-injection vector (victim/root induced to honor attacker-owned
 * "managed" policy), whereas a root-owned (admin) or self-owned (the user's own deliberate
 * dev/test choice) file is a legitimate relocation. Group-writability is deliberately NOT
 * refused even for an override: umask 002 routinely creates 0o664 files, so a self-owned
 * group-writable override stays valid (consistent with {@link isWorldWritable}'s group
 * tolerance). This closes the AMI-only relocation hole where an unprivileged user could
 * redirect the authoritative managed-policy source at a file a DIFFERENT unprivileged
 * principal controls — the fixed admin-only trust boundary the reference relies on.
 *
 * Windows (no `process.getuid`) carries no meaningful POSIX mode bits, so only the
 * symlink check applies there.
 */
function assertTrustedManagedFile(configPath: string, relocatable = false): void {
  let stats: fs.Stats;
  try {
    stats = fsHooks.lstatSync(configPath);
  } catch (err) {
    throw new ManagedConfigError(`Managed config at ${configPath} could not be inspected: ${errorMessage(err)}`);
  }
  if (stats.isSymbolicLink()) {
    throw new ManagedConfigError(`Managed config at ${configPath} is a symlink; refusing to follow a redirected managed-policy source.`);
  }
  // POSIX mode bits / ownership are only meaningful where the platform models uid/gid.
  const getuid = process.getuid;
  if (typeof getuid !== 'function') return;
  if (isWorldWritable(stats.mode)) {
    throw new ManagedConfigError(`Managed config at ${configPath} is world-writable; refusing an untrusted managed-policy source (chmod o-w it).`);
  }
  // Stricter, admin-grade ownership trust for a user-relocatable override path (skippable
  // via the explicit opt-out seam). The fixed device-default path does not carry this — it
  // already lives in an admin-only OS location. A non-numeric uid (an exotic filesystem
  // that does not report ownership) is treated as indeterminate, not as a violation.
  if (!relocatable || managedOverrideUntrustedAllowed()) return;
  if (typeof stats.uid === 'number' && stats.uid !== 0 && stats.uid !== getuid.call(process)) {
    throw new ManagedConfigError(`Relocated managed config at ${configPath} (via SUPERINFERENCE_MANAGED_CONFIG) is owned by uid ${stats.uid} — refusing an override owned by neither root nor the current user (set ${MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV}=1 to opt out).`);
  }
}

/**
 * Read + JSON-parse a managed file, failing closed on an untrusted source, an
 * unreadable file, a JSON syntax error, or a non-object payload. A present-but-
 * valid file is value-SALVAGED ({@link salvageManagedConfig}): each individual
 * field whose value is invalid is dropped (logged) while the valid remainder is
 * kept, so one bad enum/typed field never rejects the whole enterprise policy.
 */
function parseManagedFile(configPath: string, relocatable = false): ProjectConfig {
  // Tamper-resistance: refuse a symlinked or group/world-writable managed-policy
  // file before trusting its contents (fail-closed on an untrusted source). A
  // user-relocatable override path additionally enforces admin-grade ownership.
  assertTrustedManagedFile(configPath, relocatable);
  let raw: string;
  try {
    raw = fsHooks.readFileSync(configPath, 'utf-8') as string;
  } catch (err) {
    // An unreadable present file (EACCES/EIO) must fail closed, not be ignored.
    throw new ManagedConfigError(`Managed config at ${configPath} could not be read: ${errorMessage(err)}`);
  }
  let parsed: unknown;
  try {
    parsed = parseJsonc(raw);
  } catch (err) {
    // A broken enterprise policy must fail closed, with the SAME actionable JSON
    // syntax-recovery hint AMI surfaces for user/project config (one shared source)
    // instead of a bare parser message. Parity: the reference's root-level JSON syntax tip.
    throw new ManagedConfigError(`Managed config at ${configPath} failed to parse: ${errorMessage(err)}. ${JSON_SYNTAX_RECOVERY_HINT}`);
  }
  // Managed policy must be a JSON object — an array/primitive at the root is a
  // malformed policy, not an empty one; fail closed rather than cast it blindly.
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new ManagedConfigError(`Managed config at ${configPath} is not a JSON object (got ${describeJsonType(parsed)}).`);
  }
  // Per-field salvage: drop invalid individual values, keep the valid remainder.
  return salvageManagedConfig(parsed as ProjectConfig, configPath);
}
let managedConfigCache: ManagedConfigCacheEntry | null = null;

/**
 * Drop the memoized managed-config result so the next {@link loadManagedConfig} re-reads
 * from disk + OS-native policy. Fanned out from {@link resetConfigCache} on every config
 * write; the AMI analog of the reference's `clearMdmSettingsCache`.
 */
export function clearManagedConfigCache(): void {
  managedConfigCache = null;
}

/**
 * A cheap file-identity signature over EVERY managed-config input, or `null` when the
 * read is UNCACHEABLE (a present source whose identity cannot be stat'd — the fs-mocked
 * test path). Folds in, for each candidate, its absence or `mtime:size`, and for each
 * candidate directory's `config.d/` the sorted eligible entries + their identities, so
 * any on-disk change to the base policy OR a drop-in changes the signature. The OS-native
 * MDM/registry layer has no file identity and is deliberately excluded here — it is
 * covered by the explicit {@link clearManagedConfigCache} hook instead.
 */
function computeManagedConfigSignature(candidates: string[]): string | null {
  const parts: string[] = [];
  const seenDirs = new Set<string>();
  for (const candidate of candidates) {
    if (!fsHooks.existsSync(candidate)) {
      parts.push(`${candidate}=A`);
    } else {
      let st: fs.Stats;
      try {
        st = fsHooks.statSync(candidate);
      } catch {
        // Present but unstattable (the fs-mocked test path): not safely cacheable.
        return null;
      }
      // Fold in mode + ownership, not just mtime/size: the trust gate
      // ({@link assertTrustedManagedFile}) rejects a world-writable or foreign-owned
      // source, and a `chmod`/`chown` changes NEITHER mtime nor size — so a bare
      // mtime+size identity would serve a now-untrusted file from cache and skip the
      // re-check. Including mode/uid/gid forces a fresh trust evaluation on any such change.
      parts.push(`${candidate}=${st.mtimeMs}:${st.size}:${st.mode}:${st.uid}:${st.gid}`);
    }
    // Fold in the candidate's `config.d/` drop-in dir (dedup across shared parents):
    // which dir is ACTIVE depends on parsed content, so cover every candidate's dir —
    // over-invalidation is harmless; a stale snapshot is not.
    const dropInDir = path.join(path.dirname(candidate), 'config.d');
    if (seenDirs.has(dropInDir)) continue;
    seenDirs.add(dropInDir);
    if (!fsHooks.existsSync(dropInDir)) {
      parts.push(`${dropInDir}/=A`);
      continue;
    }
    let entries: string[];
    try {
      entries = (fsHooks.readdirSync(dropInDir) as string[])
        .filter(name => isEligibleManagedDropIn(name, dropInDir))
        .sort();
    } catch {
      return null;
    }
    for (const name of entries) {
      const dropInPath = path.join(dropInDir, name);
      try {
        const st = fsHooks.statSync(dropInPath);
        parts.push(`${dropInPath}=${st.mtimeMs}:${st.size}:${st.mode}:${st.uid}:${st.gid}`);
      } catch {
        return null;
      }
    }
  }
  return parts.join('|');
}

export function loadManagedConfig(): ProjectConfig | null {
  const candidates = getManagedConfigPaths();
  const signature = computeManagedConfigSignature(candidates);
  if (signature !== null && managedConfigCache !== null && managedConfigCache.signature === signature) {
    const cached = managedConfigCache.config;
    return cached === null ? null : structuredClone(cached);
  }
  const merged = loadManagedConfigUncached(candidates);
  if (signature !== null) {
    managedConfigCache = { signature, config: merged };
    return merged === null ? null : structuredClone(merged);
  }
  // Uncacheable read (couldn't stat a present source — e.g. under fs mocks): do not
  // leave a now-potentially-stale entry from a prior cacheable call behind either.
  managedConfigCache = null;
  return merged;
}

/**
 * Read + merge the managed-config layers from disk and OS-native policy WITHOUT the
 * {@link managedConfigCache} memo. {@link loadManagedConfig} wraps this with the
 * identity-keyed cache; this is also the fail-closed path used when the read is not
 * safely cacheable.
 */
function loadManagedConfigUncached(candidates: string[]): ProjectConfig | null {
  const started = Date.now();
  // Resolve the ACTIVE managed source: the first ordered candidate that exists on
  // disk (per-user > device-level on macOS), falling back to the device-level
  // default when none is present. This gives priority-ordered multi-path MDM
  // resolution instead of reading exactly one fixed path. Parity: the reference's
  // getMacOSPlistPaths highest-priority-first source selection.
  // When the active source came from the USER-settable SUPERINFERENCE_MANAGED_CONFIG
  // override (rather than a fixed OS-conventional device path), it is user-relocatable and
  // must clear the stricter admin-grade trust bar (not group-writable, owned by root or
  // self) before its policy is honored. The override also owns its drop-in dir, so those
  // layers are relocatable too.
  const relocatable = !!process.env.SUPERINFERENCE_MANAGED_CONFIG;
  // Select the ACTIVE base source: the first candidate that exists AND parses to a
  // NON-EMPTY policy. A present-but-EMPTY (key-less `{}`) higher-priority source does
  // NOT shadow a populated lower-priority one — it transparently falls through to the
  // next candidate. When every present source is empty, the highest-priority present
  // one is kept as the base (so its own `config.d/` drop-ins still apply); when none
  // exist, the device default is the base (merged stays null). Parity: the reference's
  // consumeRawReadResult first-non-empty-source-wins chain (an empty higher source
  // falls through to a populated lower one) layered on top of getMacOSPlistPaths'
  // highest-priority-first ordering.
  let configPath = candidates[candidates.length - 1]!;
  let merged: ProjectConfig | null = null;
  let haveEmptyFallback = false;
  for (const candidate of candidates) {
    if (!fsHooks.existsSync(candidate)) continue;
    const parsed = parseManagedFile(candidate, relocatable);
    if (Object.keys(parsed).length > 0) {
      configPath = candidate;
      merged = parsed;
      break;
    }
    // A present-but-empty source: remember the FIRST one as the fallback base (so a
    // chain of only-empty sources still resolves a base + its drop-in dir), but keep
    // scanning for a populated lower-priority source that should win outright.
    if (!haveEmptyFallback) {
      configPath = candidate;
      merged = parsed;
      haveEmptyFallback = true;
    }
  }

  // Layer `<activeBase>/config.d/*.json` drop-ins alphabetically (last file wins)
  // over the base. The drop-in dir is resolved from the ACTIVE source's directory
  // (not the fixed default) so a per-user managed source carries its own drop-ins.
  const dropInDir = path.join(path.dirname(configPath), 'config.d');
  let entries: string[] = [];
  try {
    if (fsHooks.existsSync(dropInDir)) {
      entries = (fsHooks.readdirSync(dropInDir) as string[])
        .filter(name => isEligibleManagedDropIn(name, dropInDir))
        .sort();
    }
  } catch {
    entries = [];
  }
  for (const name of entries) {
    const dropInPath = path.join(dropInDir, name);
    // A `config.d/*.json` drop-in is an AUTHORITATIVE managed-policy layer, so it
    // enforces the SAME strict trust as the base managed file: {@link parseManagedFile}
    // fails closed on a symlink-redirected, world-writable, or (for a relocatable
    // override) foreign-owned source, on an unreadable file, and on a JSON syntax
    // error / non-object payload. A broken or untrusted drop-in therefore FAILS CLOSED
    // (propagates {@link ManagedConfigError}) exactly like the base — a security-bearing
    // managed policy that cannot be honored must NEVER be silently dropped (fail-open),
    // whether it lives in the base file or a drop-in. Per-field invalid VALUES are still
    // salvaged inside parseManagedFile (one bad enum never rejects the whole policy); it
    // is only an untrusted/unreadable/unparseable drop-in that fails the load closed.
    const dropIn = parseManagedFile(dropInPath, relocatable);
    merged = merged === null ? dropIn : mergeManagedLayer(merged, dropIn);
  }
  // OS-NATIVE managed policy (macOS configuration profile / Windows Group Policy) is
  // the strongest trust anchor — an admin-only OS store (/Library/Managed Preferences
  // plist, HKLM\SOFTWARE\Policies) an end user cannot redirect. When present it wins
  // over the file-based managed layer (merged LAST). The read is timeout-bounded and
  // never-throwing (see managed-policy-source.ts), so a broken policy source can never
  // stall or fail the loader; a malformed/non-object payload is simply ignored. Each
  // valid field is value-SALVAGED like a file-based managed source. Parity: the reference's
  // native-MDM (plutil / reg query) acquisition taking precedence over file settings.
  const native = readNativeManagedPolicy();
  if (native) {
    let parsed: unknown;
    try {
      parsed = parseJsonc(native.raw);
    } catch {
      parsed = undefined;
    }
    if (parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed)) {
      const salvaged = salvageManagedConfig(parsed as ProjectConfig, native.source);
      merged = merged === null ? salvaged : mergeManagedLayer(merged, salvaged);
      coreLog('config', `applied native managed policy from ${native.source} (${Object.keys(salvaged).length} key(s))`);
    }
  }
  // Success breadcrumb: unlike the per-tier loadConfigFile ("loaded <path>"),
  // loadManagedConfig was previously silent on success, so an operator had no way to
  // confirm an enterprise policy was actually picked up or which keys it contributed.
  // Log the path + contributing key count (no values — the policy may be sensitive)
  // so a managed deployment is diagnosable. Parity: the reference's "MDM settings found: …".
  if (merged !== null) {
    // Introspection: name the ACTIVE path, the contributing key count (no values —
    // the policy may be sensitive), the drop-in count, AND the elapsed read time so a
    // slow managed source (e.g. an NFS mount) is diagnosable. Parity: the reference's
    // "MDM settings load completed in <N>ms" + "MDM settings found: …" breadcrumbs.
    coreLog('config', `loaded managed config ${configPath} (${Object.keys(merged).length} key(s)${entries.length > 0 ? `, ${entries.length} drop-in(s)` : ''}) in ${Date.now() - started}ms`);
  }
  return merged;
}

/**
 * The complete set of recognized top-level `ProjectConfig` keys, derived from the two
 * validation tables above (their union is exactly the `ProjectConfig` interface, so this
 * self-updates whenever a new field is added for validation — no hand-maintained list to
 * drift). Used by {@link findUnknownConfigKeys} to surface typos. Parity: the reference
 * warns on unrecognized settings keys rather than silently ignoring them.
 */
export const KNOWN_CONFIG_KEYS: ReadonlySet<string> = new Set<string>([
  ...PROJECT_CONFIG_FIELD_TYPES.map(([k]) => k as string),
  ...PROJECT_CONFIG_ENUMS.map(([k]) => k as string),
  // Fields with a union/custom shape not expressible in the single-type table above are
  // validated bespoke in {@link validateProjectConfigValues} but must still count as
  // recognized keys so they never surface as a spurious unknown-key typo warning.
  'statusLine',
  'strictPluginOnlyCustomization',
  // Editor-only JSON-Schema reference key (parity: the reference's `$schema` support). A
  // config file may carry `"$schema": "<url>"` so an editor offers autocomplete /
  // live validation of the known knobs ({@link getConfigJSONSchema}); it is meta, never
  // an engine knob, so it is recognized (no spurious unknown-key warning) and skipped by
  // {@link validateProjectConfigValues} (which only iterates the typed field tables).
  '$schema',
]);

/**
 * Return the top-level keys of `config` that are NOT recognized {@link KNOWN_CONFIG_KEYS}.
 * A non-empty result signals a likely typo (e.g. `"modle"` for `"model"`) or a stale key;
 * the loader surfaces these as an advisory `unknown-key` diagnostic (the config still
 * loads — unknown keys are ignored, never fatal). Parity: the reference surfaces
 * unrecognized settings so misspellings don't silently take no effect.
 */
export function findUnknownConfigKeys(config: ProjectConfig): string[] {
  return Object.keys(config).filter(k => !KNOWN_CONFIG_KEYS.has(k));
}

/**
 * Best-effort detection of whether the RAW text of a config file sets `key` as a
 * top-level property — even when the file does NOT parse. A fatal syntax error makes
 * {@link loadConfigFileChecked} skip the whole file, so the structured config can no
 * longer be inspected; this operates on the bytes so a user's INTENT survives a
 * broken file. Two tiers:
 *
 *  1. If the text parses to a JSON object, the answer is exact: an own-property check.
 *  2. Otherwise (unparseable / not-an-object) fall back to a property-name scan: `key`
 *     appearing as a JSON member name (`"<key>"` followed by `:`, at the start or after
 *     a `{` / `,` / whitespace). A key that appears only inside a string VALUE does not
 *     match, keeping false positives low.
 *
 * Parity: the reference's `rawSettingsContainsKey` (detect user intent even when settings
 * validation fails — e.g. a configured `cleanupPeriodDays` must not be mistaken for the
 * default just because an unrelated key is malformed).
 */
export function rawConfigContainsKey(raw: string, key: string): boolean {
  if (!raw || raw.trim() === '') return false;
  try {
    const parsed = parseJsonc(raw);
    if (parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return Object.prototype.hasOwnProperty.call(parsed, key);
    }
  } catch {
    // Unparseable — fall through to the raw property-name scan below.
  }
  const escaped = escapeRegExp(key);
  return new RegExp(`(^|[{,\\s])"${escaped}"\\s*:`).test(raw);
}

/**
 * The recognized ({@link KNOWN_CONFIG_KEYS}) top-level keys the RAW text of a config
 * file appears to set, detected via {@link rawConfigContainsKey} so the result is
 * meaningful even for a file that fails to parse. Returned SORTED for a stable
 * diagnostic. Reuses the one canonical key registry (self-updating — a newly-added
 * knob is detected automatically, no parallel list). Used by {@link loadConfigFileChecked}
 * to tell the user which settings a broken file silently lost.
 */
export function detectIntendedConfigKeys(raw: string): string[] {
  if (!raw || raw.trim() === '') return [];
  return [...KNOWN_CONFIG_KEYS].filter(k => rawConfigContainsKey(raw, k)).sort();
}

/**
 * Suggest the nearest recognized {@link KNOWN_CONFIG_KEYS} entry for a likely-typo
 * `key`, or `undefined` when nothing is close enough. Reuses the ONE canonical
 * edit-distance primitive ({@link levenshteinDistance}) — no parallel fuzzy matcher.
 * An exact known key gets no suggestion. Otherwise a suggestion is offered only for
 * a genuine near-miss: a case-insensitive edit distance within `min(3, max(1,
 * ceil(len/3)))` — tolerant enough for a transposition (`"modle"`→`"model"`, dist 2)
 * or a wrong case (`"StatusLine"`→`"statusLine"`, dist 0) yet capped at 3 so an
 * unrelated long key never matches. Parity: the reference's `ValidationErrorsList`
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
 * Generate a (draft-07) JSON Schema describing EVERY recognized {@link ProjectConfig}
 * knob, derived from the authoritative {@link PROJECT_CONFIG_FIELD_TYPES} /
 * {@link PROJECT_CONFIG_ENUMS} tables (plus the bespoke union keys in
 * {@link KNOWN_CONFIG_KEYS}) so it self-updates and never drops a key. Enum fields carry
 * their allowed values; union/custom-shape keys (`statusLine`,
 * `strictPluginOnlyCustomization`) are left permissive (no `type`) so a valid value of
 * either shape validates. Non-strict (`additionalProperties: true`) — unknown keys still
 * load (AMI never fails closed on an unrecognized config key; it warns). Parity: the reference
 * Code's `generateSettingsJSONSchema()`. Surfaced via `ami --introspect`.
 */
export function getConfigJSONSchema(): ConfigJSONSchema {
  const properties: Record<string, ConfigSchemaProperty> = {};

  // Deterministic order: sort each source set so the emitted schema is stable across runs.
  for (const [key, type] of [...PROJECT_CONFIG_FIELD_TYPES].sort((a, b) => (a[0] as string).localeCompare(b[0] as string))) {
    const node = configFieldTypeToSchemaNode(type);
    node.description = `Config key "${key as string}" (${configFieldTypeLabel(type)}).`;
    properties[key as string] = node;
  }
  for (const [key, allowed] of [...PROJECT_CONFIG_ENUMS].sort((a, b) => (a[0] as string).localeCompare(b[0] as string))) {
    properties[key as string] = {
      type: 'string',
      enum: [...allowed],
      description: `Config key "${key as string}" — one of ${allowed.map(v => `"${v}"`).join(', ')}.`,
    };
  }
  // Union/custom-shape recognized keys that have no single-type table entry are left
  // permissive (no `type`) so both of their accepted shapes validate — the `$schema`
  // reference key is editor-only meta, not a knob, and is intentionally omitted.
  for (const key of [...KNOWN_CONFIG_KEYS].sort()) {
    if (key === '$schema' || properties[key]) continue;
    properties[key] = { description: `Config key "${key}" (union/custom shape — value validated at load).` };
  }

  return {
    $schema: 'http://json-schema.org/draft-07/schema#',
    $id: CONFIG_SCHEMA_URL,
    title: 'AMI project configuration (.ami/config.json)',
    type: 'object',
    properties,
    additionalProperties: true,
  };
}

/**
 * Back up a corrupt config file so the user's content is never silently lost.
 * Returns the backup path (a fresh one, or an existing byte-identical one), or
 * null if the backup itself failed.
 *
 * CONTENT-DEDUP: a corrupt config is commonly re-read many times in one session
 * (the project/local/global waterfall, repeated `/doctor` sweeps, a watcher echo),
 * and each read would otherwise spawn another `.corrupt-<ts>` quarantine copy of
 * the SAME broken bytes — a backup storm. Before writing, the existing
 * `<file>.corrupt-*` quarantines are scanned; when one already holds byte-identical
 * content, that existing path is returned and NO new file is written. Parity:
 * the reference's corrupted-config quarantine skips the copy when an existing
 * corrupted backup is byte-identical (content dedup).
 */
export function backupCorruptConfig(configPath: string, content: string): string | null {
  try {
    // Dedup against an existing quarantine of the identical broken bytes.
    const existing = findIdenticalCorruptBackup(configPath, content);
    if (existing) return existing;
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const backupPath = `${configPath}.corrupt-${stamp}`;
    fsHooks.writeFileSync(backupPath, content, 'utf-8');
    return backupPath;
  } catch {
    return null;
  }
}

/**
 * The newest existing `<file>.corrupt-*` quarantine whose on-disk bytes are
 * IDENTICAL to `content`, or null when none match. Used by
 * {@link backupCorruptConfig} to avoid re-quarantining the same broken file.
 */
function findIdenticalCorruptBackup(configPath: string, content: string): string | null {
  try {
    const dir = path.dirname(configPath);
    const base = path.basename(configPath);
    const candidates = (fsHooks.readdirSync(dir) as string[])
      .filter(e => e.startsWith(`${base}.corrupt-`))
      .sort()
      .reverse();
    for (const name of candidates) {
      const full = path.join(dir, name);
      try {
        if ((fsHooks.readFileSync(full, 'utf-8') as string) === content) return full;
      } catch { /* unreadable candidate — skip it */ }
    }
  } catch { /* unreadable dir — fall through to a fresh write */ }
  return null;
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
 * Load the `--settings <file>` flag-tier config (parity: the reference's `flagSettings`
 * SETTING_SOURCES layer), from the absolute path bridged onto {@link
 * FLAG_SETTINGS_ENV}. The flag file layers ABOVE local/project/user and BELOW
 * managed policy (see {@link mergeConfigs}). Returns null when unset/blank or the
 * file is absent/corrupt — a bad flag file never crashes startup; managed-only
 * keys it carries are ignored (the merge omits them). Reuses the canonical
 * {@link loadConfigFileChecked} loader (no parallel parse path).
 */
export function loadFlagSettingsConfig(env: NodeJS.ProcessEnv = process.env): ProjectConfig | null {
  const raw = env[FLAG_SETTINGS_ENV];
  if (!raw || raw.trim() === '') return null;
  return loadConfigFileChecked(raw.trim()).config;
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
  // Path-safety guard (parity: the reference's safeResolvePath): refuse a UNC path before any
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
    // layer quietly take effect). Parity: the reference's handleFileSystemError — ENOENT is
    // a debug-level non-event, every other code is a tracked error. Classify via the
    // canonical {@link getErrnoCode} accessor (core/src/utils/errno.ts) — the single
    // source for the `err.code` cast — rather than an ad-hoc inline cast, so every fs
    // catch block agrees on how an errno is read.
    const code = getErrnoCode(err);
    if (code === 'ENOENT' || code === 'ENOTDIR') return { config: null };
    const message = errorMessage(err);
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
    // file, not merely that the parse failed (parity: the reference's JSON syntax-error tip).
    const message = `${errorMessage(err)}. ${JSON_SYNTAX_RECOVERY_HINT}`;
    // Point the user at the most recent known-GOOD pre-write snapshot (`.backup-*`),
    // if any, with a copy-pasteable restore command (parity: the reference's `cp` restore
    // hint). Distinct from `backupPath`, which quarantines the CORRUPT bytes.
    const good = findMostRecentConfigBackup(configPath);
    const restoreHint = good ? `cp "${good}" "${configPath}"` : undefined;
    // The whole file is skipped — surface which recognized knobs the user was trying to
    // set so a security-bearing setting (e.g. safeMode) doesn't silently fail open.
    const intendedKeys = detectIntendedConfigKeys(raw);
    coreLog('config', `corrupt config at ${configPath}: ${message}${backupPath ? ` (backed up to ${backupPath})` : ''}${restoreHint ? ` [restore: ${restoreHint}]` : ''}${intendedKeys.length > 0 ? ` [lost: ${intendedKeys.join(', ')}]` : ''}`);
    return { config: null, diagnostic: { path: configPath, kind: 'parse-error', message, backupPath, restoreHint, ...(intendedKeys.length > 0 ? { intendedKeys } : {}) } };
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    const message = `Expected a JSON object, got ${describeJsonType(parsed)}`;
    // A non-object root is skipped entirely too — report the keys it tried to set.
    const intendedKeys = detectIntendedConfigKeys(raw);
    coreLog('config', `invalid config at ${configPath}: ${message}${intendedKeys.length > 0 ? ` [lost: ${intendedKeys.join(', ')}]` : ''}`);
    return { config: null, diagnostic: { path: configPath, kind: 'not-object', message, ...(intendedKeys.length > 0 ? { intendedKeys } : {}) } };
  }
  const config = parsed as ProjectConfig;
  // The file is a valid JSON object, but individual VALUES may still be invalid
  // (bad enum, out-of-range number). Surface those per-field rather than silently
  // accepting them — the valid parts still load (validation is advisory).
  const issues = validateProjectConfigValues(config);
  if (issues.length > 0) {
    const message = `${issues.length} invalid config value(s): ${formatConfigValueIssues(issues)}`;
    coreLog('config', `invalid value(s) in ${configPath}: ${message}`);
    return { config, diagnostic: { path: configPath, kind: 'invalid-value', message, issues } };
  }
  // The file is a valid JSON object with valid values, but may still carry
  // unrecognized top-level keys (a typo like "modle", or a stale key). Surface those
  // as an advisory `unknown-key` diagnostic so misspellings don't silently take no
  // effect — the config still loads (unknown keys are ignored, never fatal). Parity:
  // the reference warns on unrecognized settings keys.
  const unknownKeys = findUnknownConfigKeys(config);
  if (unknownKeys.length > 0) {
    // Attach a "did you mean X?" near-match suggestion per unknown key so a typo
    // (e.g. "modle") is self-correctable, not merely flagged. Parity: the reference's
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
 *
 * Multi-source dedup (parity: the reference's loadSettingsFromDisk seenFiles/seenErrors):
 * two tiers can resolve to the SAME physical file — most commonly when AMI is run
 * from the home directory, where the `project` path (`<cwd>/.ami/config.json`) and
 * the `global` path (`<home>/.ami/config.json`) are one and the same. Each resolved
 * path is therefore loaded/diagnosed at most ONCE (first tier wins), and an identical
 * diagnostic arriving from an overlapping source is suppressed by identity — so a
 * single broken file is never reported twice.
 */
export function collectConfigDiagnostics(cwd: string): ConfigDiagnostic[] {
  const home = process.env.HOME || process.env.USERPROFILE || '';
  // Each probed path is tagged with its config LAYER so a diagnostic names its
  // provenance ("project" / "project, gitignored" / "user") rather than leaving the
  // user to infer it from a bare path. Parity: the reference's per-source settings validation.
  const paths: Array<{ path: string; source: ConfigSource }> = [
    { path: path.join(cwd, '.ami', 'config.json'), source: 'project' },
    { path: path.join(cwd, '.ami', 'config.local.json'), source: 'local' },
    ...(home ? [{ path: path.join(home, '.ami', 'config.json'), source: 'global' as const }] : []),
  ];
  const diagnostics: ConfigDiagnostic[] = [];
  // Dedup by RESOLVED absolute path (same file reached via two tiers is parsed once)
  // and by diagnostic IDENTITY (the exact same problem is never surfaced twice).
  const seenFiles = new Set<string>();
  const seenErrors = new Set<string>();
  const pushUnique = (d: ConfigDiagnostic): void => {
    const id = configDiagnosticIdentity(d);
    if (seenErrors.has(id)) return;
    seenErrors.add(id);
    diagnostics.push(d);
  };
  for (const { path: p, source } of paths) {
    const resolved = path.resolve(p);
    if (seenFiles.has(resolved)) continue;
    seenFiles.add(resolved);
    const { diagnostic } = loadConfigFileChecked(p);
    if (diagnostic) {
      pushUnique({ ...diagnostic, source, sourceName: getConfigSourceName(source), backupPath: diagnostic.backupPath ?? findMostRecentBackup(p) ?? undefined });
    }
  }
  // Managed/enterprise tier (fail-open VISIBILITY): a broken managed policy fails
  // CLOSED at load (an untrusted source — symlink redirect / world-writable file —
  // an unreadable file, a JSON syntax error, or a non-object payload all throw
  // {@link ManagedConfigError}), so it is normally invisible to /doctor. Surface it
  // HERE alongside the user/project/local diagnostics so an invalid security-bearing
  // policy file is reported across EVERY source, never silently ignored. Parity:
  // the reference's buildInstallationHealthDiagnostics aggregating invalid settings across
  // managed + user + project + local. A clean/absent managed policy adds nothing.
  try {
    loadManagedConfig();
  } catch (err) {
    if (err instanceof ManagedConfigError) {
      pushUnique({ path: getManagedConfigPath(), kind: 'unreadable', message: err.message, source: 'managed', sourceName: getConfigSourceName('managed') });
    }
  }
  return diagnostics;
}

/**
 * Enumerate the FILE-based config tiers in precedence order (HIGHEST first) with each
 * tier's resolved path and whether it is ACTIVELY contributing to the effective config.
 *
 * This answers "which config files are actually in effect, and in what precedence
 * order" — distinct from {@link resolveSettingsFilePaths} (lists the four POSSIBLE tier
 * paths regardless of existence) and {@link collectConfigDiagnostics} (surfaces only
 * tiers that have ERRORS). A user/operator can see their live config source stack at a
 * glance. Parity: the reference's Settings status `buildSettingSourcesProperties()`
 * active-source enumeration.
 *
 * Order (highest precedence first): `local` > `project` > `global` > `managed` — the
 * SAME ordering {@link mergeConfigs} applies for the file tiers (a higher tier's value
 * overrides a lower one). The `cli`/`env` tiers are intentionally omitted: they are not
 * files on disk. An absent/unparseable file is still listed (`active:false`) so the
 * full possible stack is visible for diagnosis. Managed-tier activeness fails OPEN: an
 * untrusted/broken managed policy (which throws {@link ManagedConfigError} at load) is
 * reported `active:false` rather than crashing this diagnostic.
 */
export function collectActiveConfigSources(cwd: string): ActiveConfigSource[] {
  let managedActive = false;
  try {
    managedActive = loadManagedConfig() !== null;
  } catch {
    // A present-but-untrusted/broken managed policy fails closed at load; here it is
    // simply reported inactive rather than tearing down the whole introspection surface.
    managedActive = false;
  }
  const globalPath = getGlobalConfigPath();
  const entries: Array<{ source: ConfigSource; path: string; active: boolean }> = [
    { source: 'local', path: getLocalConfigPath(cwd), active: loadLocalConfig(cwd) !== null },
    { source: 'project', path: getProjectConfigPath(cwd), active: loadProjectConfig(cwd) !== null },
    ...(globalPath
      ? [{ source: 'global' as const, path: globalPath, active: loadGlobalConfig() !== null }]
      : []),
    { source: 'managed', path: getManagedConfigPath(), active: managedActive },
  ];
  return entries.map(e => ({ ...e, sourceName: getConfigSourceName(e.source) }));
}

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

/**
 * Injectable timer seam for {@link ConfigService} debounce / deletion-grace /
 * retry / poll paths. Production binds the real globals; tests flush timers
 * synchronously (or force deletion-grace expiry) without real wall-clock waits.
 */
export const configTimerHooks: {
  setTimeout: typeof setTimeout;
  clearTimeout: typeof clearTimeout;
  setInterval: typeof setInterval;
  clearInterval: typeof clearInterval;
} = {
  setTimeout: globalThis.setTimeout.bind(globalThis) as typeof setTimeout,
  clearTimeout: globalThis.clearTimeout.bind(globalThis) as typeof clearTimeout,
  setInterval: globalThis.setInterval.bind(globalThis) as typeof setInterval,
  clearInterval: globalThis.clearInterval.bind(globalThis) as typeof clearInterval,
};

/**
 * Injectable cross-process lock seam for {@link updateConfigFile} (and every
 * save/persist helper that routes through it). Production uses
 * {@link withFileLockSync}; tests inject a thrower to hit the best-effort
 * catch-to-false arms on the preference / notice / onboarding writers.
 */
export const configLockHooks: {
  withFileLockSync: typeof withFileLockSync;
} = { withFileLockSync };

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
  /** Global-tier (`~/.ami/config.json`) watch, when `watchGlobal` is enabled. */
  private readonly globalConfigPath: string | null;
  private globalWatcher: fs.FSWatcher | null = null;
  private globalDebounceTimer: ReturnType<typeof setTimeout> | null = null;
  /** Last-seen serialized global config, so a no-op watcher echo never fires a change. */
  private globalSnapshot: string | null = null;
  /** Managed-tier (`getManagedConfigPath()`) watch, when `watchManaged` is enabled. */
  private readonly managedConfigPath: string | null;
  private readonly managedPollMs: number;
  private managedWatcher: fs.FSWatcher | null = null;
  private managedDebounceTimer: ReturnType<typeof setTimeout> | null = null;
  private managedPollTimer: ReturnType<typeof setInterval> | null = null;
  /** Last-seen serialized merged managed config, so an unchanged poll never fires. */
  private managedSnapshot: string | null = null;

  constructor(cwd: string, opts: ConfigServiceOptions = {}) {
    this.configPath = path.join(cwd, '.ami', 'config.json');
    this.config = loadConfigFile(this.configPath) ?? {};
    this.envTarget = opts.env ?? process.env;
    this.debounceMs = opts.debounceMs ?? CONFIG_WATCH_DEBOUNCE_MS;
    this.stabilityThresholdMs = opts.stabilityThresholdMs ?? CONFIG_FILE_STABILITY_THRESHOLD_MS;
    this.deletionGraceMs = opts.deletionGraceMs ?? CONFIG_DELETION_GRACE_MS;
    this.fallbackPollMs = opts.fallbackPollMs ?? CONFIG_FALLBACK_POLL_MS;
    this.watchRetryMs = opts.watchRetryMs ?? CONFIG_WATCH_RETRY_MS;
    // Resolve the global-tier path only when global watching is requested AND it is a
    // DISTINCT file from the project tier (a project rooted at ~ shares the same path —
    // then the project watcher already covers it, so there is no second tier to watch).
    const globalPath = opts.watchGlobal ? getGlobalConfigPath() : null;
    this.globalConfigPath = globalPath && globalPath !== this.configPath ? globalPath : null;
    if (this.globalConfigPath) this.globalSnapshot = this.readGlobalSnapshot();
    // Managed-tier watch is resolved from the device-level managed base path (the
    // SAME path loadManagedConfig's drop-in dir hangs off). Seed the baseline snapshot
    // so the first real change — not the initial state — fires onConfigChange('managed').
    this.managedPollMs = opts.managedPollMs ?? CONFIG_MANAGED_POLL_MS;
    this.managedConfigPath = opts.watchManaged ? getManagedConfigPath() : null;
    if (this.managedConfigPath) this.managedSnapshot = this.readManagedSnapshot();
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
    this.ensureGlobalWatch();
    this.ensureManagedWatch();
    this.startFallbackPoll();
    this.startManagedPoll();
  }

  /**
   * Establish the global-tier (`~/.ami/config.json`) watcher when `watchGlobal` is on
   * and its directory exists. A change to the global file fires the onConfigChange
   * callbacks tagged `source: 'global'` (vs the project tier's `'project'`). Kept
   * deliberately lean — it only NOTIFIES which tier changed (the engine's change
   * handler re-reads the whole waterfall), so it does not replicate the project tier's
   * merge/env/deletion machinery. A missing global dir is a quiet no-op (a user with no
   * global config simply has no second tier to watch).
   */
  private ensureGlobalWatch(): void {
    if (!this.globalConfigPath || this.globalWatcher) return;
    const dir = path.dirname(this.globalConfigPath);
    if (!fsHooks.existsSync(dir)) return;
    try {
      this.globalWatcher = fsHooks.watch(dir, (_e, filename) => this.onGlobalWatchEvent(filename));
      this.globalWatcher.unref();
      // A watcher error simply drops global hot-reload for this session (the project
      // tier is unaffected); re-establishment is not attempted to keep this lean. A
      // per-path permission error is tolerated in place (the handle still covers the
      // rest of the dir) rather than dropping global hot-reload — parity:
      // `ignorePermissionErrors`.
      this.globalWatcher.on('error', (err: unknown) => {
        if (isWatcherPermissionError(err)) {
          coreLog('config', 'global config watcher permission error on a path; tolerating', { error: String(err) });
          return;
        }
        if (this.globalWatcher) { try { this.globalWatcher.close(); } catch { /* already dead */ } this.globalWatcher = null; }
      });
      coreLog('config', `watching global config ${this.globalConfigPath} for changes`);
    } catch {
      // watch() can throw (inotify exhaustion); leave global hot-reload off rather than
      // breaking the project watcher.
      this.globalWatcher = null;
    }
  }

  /** Serialize the current global config (or `null` when absent) for change detection. */
  private readGlobalSnapshot(): string {
    const cfg = this.globalConfigPath ? loadConfigFile(this.globalConfigPath) : null;
    return JSON.stringify(cfg ?? null);
  }

  /** Debounce a global config.json event into a single change notification. */
  private onGlobalWatchEvent(filename: string | null): void {
    if (!this.globalConfigPath || filename !== path.basename(this.globalConfigPath)) return;
    // Swallow the echo of AMI's own write to the global file (a `/config set --global`).
    if (consumeInternalWrite(this.globalConfigPath)) return;
    if (this.globalDebounceTimer) configTimerHooks.clearTimeout(this.globalDebounceTimer);
    this.globalDebounceTimer = configTimerHooks.setTimeout(() => {
      this.globalDebounceTimer = null;
      this.processGlobalChange();
    }, this.debounceMs);
    this.globalDebounceTimer.unref();
  }

  /** Fire onConfigChange('global', …) when the global file's content actually changed. */
  private processGlobalChange(): void {
    const next = this.readGlobalSnapshot();
    if (next === this.globalSnapshot) return;
    this.globalSnapshot = next;
    coreLog('config', `global config ${this.globalConfigPath} changed; notifying listeners`);
    for (const cb of this.onConfigChangeCallbacks) {
      try { cb('global', this.globalConfigPath!); } catch (err) { coreLog('config', 'onConfigChange callback error', { error: String(err) }); }
    }
  }

  /**
   * Establish the managed-tier (`getManagedConfigPath()`) watcher when `watchManaged`
   * is on and its base directory exists. A change fires the onConfigChange callbacks
   * tagged `source: 'managed'`. Mirrors {@link ensureGlobalWatch}: it only NOTIFIES
   * which tier changed (the engine re-reads the whole waterfall). A missing managed
   * dir is a quiet no-op — the {@link startManagedPoll} reconcile poll still picks up
   * the file appearing later (and covers `config.d/` drop-ins a non-recursive watcher
   * cannot see).
   */
  private ensureManagedWatch(): void {
    if (!this.managedConfigPath || this.managedWatcher) return;
    const dir = path.dirname(this.managedConfigPath);
    if (!fsHooks.existsSync(dir)) return;
    try {
      this.managedWatcher = fsHooks.watch(dir, (_e, filename) => this.onManagedWatchEvent(filename));
      this.managedWatcher.unref();
      // A per-path permission error (an unreadable root-owned drop-in alongside a
      // readable policy file) is tolerated in place rather than dropping managed
      // hot-reload — parity: `ignorePermissionErrors`. The reconcile poll still covers
      // anything the kept-open watcher cannot observe.
      this.managedWatcher.on('error', (err: unknown) => {
        if (isWatcherPermissionError(err)) {
          coreLog('config', 'managed config watcher permission error on a path; tolerating', { error: String(err) });
          return;
        }
        if (this.managedWatcher) { try { this.managedWatcher.close(); } catch { /* already dead */ } this.managedWatcher = null; }
      });
      coreLog('config', `watching managed config ${this.managedConfigPath} for changes`);
    } catch {
      // watch() can throw (inotify exhaustion / an unreadable /etc dir); leave managed
      // hot-reload to the poll rather than breaking the project watcher.
      this.managedWatcher = null;
    }
  }

  /**
   * Serialize the merged managed config for change detection, failing OPEN: a managed
   * file that is absent, transiently unreadable, or (newly) tampered/broken throws from
   * {@link loadManagedConfig} — here that is caught and collapsed to the `null` snapshot
   * rather than crashing the poll, so a mid-session managed refresh never fail-closes the
   * running session. The initial LOAD path (engine bootstrap) still fails closed; this is
   * only the live-refresh safety net. Parity: the reference's background poll swallowing errors.
   */
  private readManagedSnapshot(): string {
    try {
      return JSON.stringify(loadManagedConfig() ?? null);
    } catch {
      return JSON.stringify(null);
    }
  }

  /** Debounce a managed base-dir event into a single change notification. */
  private onManagedWatchEvent(filename: string | null): void {
    if (!this.managedConfigPath) return;
    // React to the base file itself OR any `config.d/` drop-in change; a null filename
    // (some platforms omit it) also triggers a re-check — the snapshot diff is the real
    // gate, so an over-eager event is harmless (an unchanged policy fires nothing).
    if (filename !== null && filename !== path.basename(this.managedConfigPath) && !filename.endsWith('.json')) return;
    if (this.managedDebounceTimer) configTimerHooks.clearTimeout(this.managedDebounceTimer);
    this.managedDebounceTimer = configTimerHooks.setTimeout(() => {
      this.managedDebounceTimer = null;
      this.processManagedChange();
    }, this.debounceMs);
    this.managedDebounceTimer.unref();
  }

  /** Fire onConfigChange('managed', …) when the merged managed policy actually changed. */
  private processManagedChange(): void {
    if (!this.managedConfigPath) return;
    const next = this.readManagedSnapshot();
    if (next === this.managedSnapshot) return;
    this.managedSnapshot = next;
    coreLog('config', `managed config ${this.managedConfigPath} changed; notifying listeners`);
    for (const cb of this.onConfigChangeCallbacks) {
      try { cb('managed', this.managedConfigPath); } catch (err) { coreLog('config', 'onConfigChange callback error', { error: String(err) }); }
    }
  }

  /**
   * Low-frequency unref'd reconcile poll for the managed tier, so a policy change on a
   * path `fs.watch` cannot observe (root-owned `/etc`, an MDM network mount, a `config.d/`
   * drop-in) is still picked up within one interval. Disabled when managed watching is
   * off or the interval is non-positive. Parity: the reference's managed-settings background poll.
   */
  private startManagedPoll(): void {
    if (this.managedPollTimer || !this.managedConfigPath || this.managedPollMs <= 0) return;
    this.managedPollTimer = configTimerHooks.setInterval(() => { this.processManagedChange(); }, this.managedPollMs);
    this.managedPollTimer.unref();
  }

  /**
   * Establish the directory watcher, or — when the directory does not exist yet /
   * `fs.watch` throws (e.g. inotify exhaustion) — schedule a retry instead of giving
   * up permanently, so a `.ami/` created after startup (or a transient error) still
   * ends up watched. Parity: the reference's `#rewatch` retry-even-on-same-dir.
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
      this.watcher.on('error', (err: unknown) => this.onWatcherError(err));
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
    this.retryTimer = configTimerHooks.setTimeout(() => {
      this.retryTimer = null;
      if (this.started) this.ensureWatch();
    }, this.watchRetryMs);
    this.retryTimer.unref();
  }

  /**
   * A watcher `error` event closes the dead watcher and re-establishes a fresh one —
   * EXCEPT a per-path permission error ({@link isWatcherPermissionError}), which is
   * tolerated in place: the handle is still live for the rest of the directory, so it
   * is kept open rather than churned/looped. Parity: the reference's `ignorePermissionErrors`.
   */
  private onWatcherError(err?: unknown): void {
    if (isWatcherPermissionError(err)) {
      coreLog('config', 'config watcher permission error on a path; tolerating (watcher kept open)', { error: String(err) });
      return;
    }
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
      configTimerHooks.clearTimeout(this.deletionTimer);
      this.deletionTimer = null;
    }

    if (this.debounceTimer) configTimerHooks.clearTimeout(this.debounceTimer);
    this.debounceTimer = configTimerHooks.setTimeout(() => {
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
    this.stabilityTimer = configTimerHooks.setTimeout(() => {
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
    this.deletionTimer = configTimerHooks.setTimeout(() => {
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
   * the empty config, mirroring a normal change. Parity: the reference's change-detector
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
   * up within one interval. Parity: the reference's FALLBACK_POLL_MS safety net.
   */
  private startFallbackPoll(): void {
    if (this.pollTimer || this.fallbackPollMs <= 0) return;
    this.pollTimer = configTimerHooks.setInterval(() => { this.reload(); }, this.fallbackPollMs);
    this.pollTimer.unref();
  }

  reload(): boolean {
    const prev = this.config;
    const next = loadConfigFile(this.configPath);
    if (!next) return false;

    const changed = JSON.stringify(prev) !== JSON.stringify(next);
    if (changed) {
      this.config = next;
      // Positive introspection breadcrumb: announce that the PROJECT tier changed and
      // is being re-applied, naming the source tier — matching the `source: 'project'`
      // label fanned out to onConfigChange below and the global/managed tiers' own
      // "changed; notifying listeners" lines. Without this the project reload was the
      // only tier with no change-time log, so an operator tailing the debug log could
      // see a global/managed reapply but never a project one. Parity: the reference's
      // `logForDebugging('Settings changed from <source>, updating app state')`.
      coreLog('config', `project config ${this.configPath} changed; reapplying (source: project)`);
      // Take-effect-immediately: re-apply the watched project config's `env`
      // block to the process environment so a mid-session edit (a changed tuning
      // knob / model-name default) applies without a restart, instead of being
      // silently ignored until the next launch. Trust-tiered via applyConfigEnv —
      // a project-scoped file still only applies the SAFE_ENV_VARS allowlist, so
      // a committed config can never inject a dangerous var at runtime. Parity:
      // the reference's onChangeAppState re-applying config-driven env on a settings change.
      try {
        this.lastEnvApplication = configEnvHooks.applyConfigEnv({ project: next }, this.envTarget);
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

  /**
   * Tear the service down and RESOLVE only once every live `fs.FSWatcher` has emitted its
   * `'close'` event. The synchronous teardown — clear every timer, close both watchers,
   * drop the watcher handles, mark stopped — runs BEFORE this returns, so an immediate
   * post-`stop()` assertion (a cleared `retryTimer`, a nulled `watcher`) still holds; the
   * returned promise is the AWAIT-able boundary a test or shutdown uses to confirm the
   * watcher is FULLY stopped before deleting the watched directory, closing the documented
   * teardown ENOENT race (a still-settling `fs.watch` callback firing on a just-removed
   * dir). Fire-and-forget callers that ignore the promise behave exactly as before. Safe
   * to call repeatedly — a second call has no watchers left to close and resolves at once.
   * Parity: the reference's `changeDetector.dispose()` returning the watcher `close()`
   * promise so teardown can await the watcher fully stopping (ENOENT race #25253).
   */
  stop(): Promise<void> {
    if (this.debounceTimer) { configTimerHooks.clearTimeout(this.debounceTimer); this.debounceTimer = null; }
    if (this.stabilityTimer) { configTimerHooks.clearTimeout(this.stabilityTimer); this.stabilityTimer = null; }
    if (this.deletionTimer) { configTimerHooks.clearTimeout(this.deletionTimer); this.deletionTimer = null; }
    if (this.retryTimer) { configTimerHooks.clearTimeout(this.retryTimer); this.retryTimer = null; }
    if (this.pollTimer) { configTimerHooks.clearInterval(this.pollTimer); this.pollTimer = null; }
    if (this.globalDebounceTimer) { configTimerHooks.clearTimeout(this.globalDebounceTimer); this.globalDebounceTimer = null; }
    if (this.managedDebounceTimer) { configTimerHooks.clearTimeout(this.managedDebounceTimer); this.managedDebounceTimer = null; }
    if (this.managedPollTimer) { configTimerHooks.clearInterval(this.managedPollTimer); this.managedPollTimer = null; }
    const closing: Array<Promise<void>> = [];
    for (const w of [this.watcher, this.globalWatcher, this.managedWatcher]) {
      if (!w) continue;
      // A real FSWatcher is an EventEmitter and signals completion via a 'close' event;
      // await it so teardown can know the watcher has fully stopped. (A minimal test
      // fake without `once` simply resolves synchronously — nothing to await.)
      const closed = typeof w.once === 'function'
        ? new Promise<void>(resolve => { w.once('close', () => resolve()); })
        : undefined;
      try {
        w.close();
      } catch {
        // Already dead (same treatment as every other close site in this class). It
        // will never emit 'close', so do NOT await it — and keep closing the rest, so
        // one failing watcher can neither abort teardown nor hang stop().
        continue;
      }
      if (closed) closing.push(closed);
    }
    this.watcher = null;
    this.globalWatcher = null;
    this.managedWatcher = null;
    this.started = false;
    return Promise.all(closing).then(() => undefined);
  }
}

/**
 * Read managed config, treating a fail-closed {@link ManagedConfigError} (or any
 * unexpected throw) as "absent". Used by the resolve-* helpers that must never abort
 * a non-policy lookup over a broken managed file. ONE catch site so coverage and
 * behavior stay consistent across memory/trust/speculation resolvers.
 */
export function loadManagedConfigSafe(): ProjectConfig | null {
  try {
    return loadManagedConfig();
  } catch {
    return null;
  }
}

/**
 * Resolve the effective {@link ProjectConfig.memoryExcludes} for a workspace by
 * merging the full config waterfall (managed > global > project > local — CLI/env
 * carry no `memoryExcludes`) and returning the accumulated exclude-glob list (empty
 * when none is configured). The memory loader UNIONS this with the
 * `AMI_MEMORY_EXCLUDES` env list, so both the persisted config knob and the one-shot
 * env override are honored. Kept in the config layer (not memory.ts) so there is ONE
 * place that reads the config files and applies the merge semantics. Parity: the reference
 * Code resolving `claudeMdExcludes` from the settings waterfall for `isClaudeMdExcluded`.
 */
export function resolveConfiguredMemoryExcludes(cwd: string): string[] {
  // A broken/untrusted managed policy fails closed ({@link ManagedConfigError}); a
  // memory-exclude lookup must never abort memory loading over it — treat it as absent.
  const managed = loadManagedConfigSafe();
  const merged = mergeConfigs(
    {},
    {},
    loadProjectConfig(cwd),
    loadGlobalConfig(),
    loadLocalConfig(cwd),
    managed,
  );
  return merged.memoryExcludes ?? [];
}

/**
 * Resolve whether the shared TEAM-memory store should be LOADED for a workspace by
 * merging the full config waterfall and reading {@link ProjectConfig.teamMemoryEnabled}
 * with the usual {@link mergeConfigs} last-wins precedence (local > project > global >
 * managed). Defaults to `true` (load when present) so an unset knob preserves AMI's
 * always-load behavior; returns `false` only when the highest-priority tier that sets it
 * uses `false`. Kept in the config layer (not memory.ts) so there is ONE place that reads
 * the config files and applies the merge semantics, mirroring
 * {@link resolveConfiguredMemoryExcludes}. Parity: the reference gating team memory behind
 * an enable flag before reading the shared store.
 */
export function resolveTeamMemoryLoadingEnabled(cwd: string): boolean {
  // A broken/untrusted managed policy fails closed ({@link ManagedConfigError}); a
  // team-memory gate lookup must never abort memory loading over it — treat it as absent.
  const managed = loadManagedConfigSafe();
  const merged = mergeConfigs(
    {},
    {},
    loadProjectConfig(cwd),
    loadGlobalConfig(),
    loadLocalConfig(cwd),
    managed,
  );
  return merged.teamMemoryEnabled ?? true;
}

/**
 * The persisted {@link ProjectConfig.speculationEnabled} flag for `cwd`, merged across
 * the full config waterfall (managed > global > project > local). Returns the raw
 * tri-state (`true` / `false` / `undefined` when unset) so a caller can distinguish an
 * explicit opt-out from the default — the cli `resolvePromptSuggestionEnabled` waterfall
 * treats `undefined` as the default-on case, and only an explicit `false` disables
 * next-prompt speculation. A broken/untrusted managed policy fails closed and is treated
 * as absent so a speculation lookup never aborts startup. Parity: the reference reading the
 * `speculationEnabled` global-config knob for `isSpeculationEnabled()`.
 */
export function resolveSpeculationEnabled(cwd: string): boolean | undefined {
  const managed = loadManagedConfigSafe();
  const merged = mergeConfigs(
    {},
    {},
    loadProjectConfig(cwd),
    loadGlobalConfig(),
    loadLocalConfig(cwd),
    managed,
  );
  return merged.speculationEnabled;
}

/**
 * Whether thinking should be ENABLED BY DEFAULT at session start (parity: the reference's
 * `shouldEnableThinkingByDefault()` in utils/thinking.ts). Ordered gate:
 *   1. the `MAX_THINKING_TOKENS` env var, when set to a finite number, wins — a positive
 *      value forces default-on, `0` (or negative) forces off (matches the cli's
 *      `MAX_THINKING_TOKENS` handling and the reference's `parseInt(...) > 0`);
 *   2. an explicit {@link ProjectConfig.alwaysThinkingEnabled} `=== false` opts OUT;
 *   3. otherwise default ON.
 * Consumed by {@link import('../../../common/src/model-thinking').deriveModelDefaultThinking}
 * so a reasoning model's auto-adopted default thinking level is suppressed when the user
 * has opted out, without changing an explicitly-pinned level. `cwd` selects the config
 * waterfall (managed > global > project > local); a broken managed policy fails closed
 * and is treated as absent so the lookup never aborts a model switch.
 */
export function shouldEnableThinkingByDefault(
  cwd: string,
  env: NodeJS.ProcessEnv = process.env,
): boolean {
  const maxThinkingTokens = env.MAX_THINKING_TOKENS;
  if (maxThinkingTokens !== undefined && maxThinkingTokens.trim() !== '') {
    const n = Number(maxThinkingTokens);
    if (Number.isFinite(n)) return n > 0;
  }
  const managed = loadManagedConfigSafe();
  const merged = mergeConfigs(
    {},
    {},
    loadProjectConfig(cwd),
    loadGlobalConfig(),
    loadLocalConfig(cwd),
    managed,
  );
  return merged.alwaysThinkingEnabled !== false;
}

/**
 * The persisted external-memory-import CONSENT for `cwd`, merged across the full
 * config waterfall (managed > global > project > local). Returns the approval flag
 * ({@link ProjectConfig.externalMemoryImportsApproved}, defaulting to `false` — the
 * safe, blocked posture) and the accumulated trusted-roots allowlist
 * ({@link ProjectConfig.externalMemoryImportRoots}). Kept in the config layer so there
 * is ONE place that reads the config files and applies the merge semantics; the memory
 * loader UNIONS the roots with a one-shot env override and consults `approved` before
 * ever loading an outside-workspace `@import`. Parity: the reference reading
 * `hasClaudeMdExternalIncludesApproved` from the project config for external includes.
 */
export function resolveConfiguredExternalMemoryImports(cwd: string): { approved: boolean; roots: string[] } {
  // A broken/untrusted managed policy fails closed ({@link ManagedConfigError}); a
  // consent lookup must never abort memory loading over it — treat it as absent.
  const managed = loadManagedConfigSafe();
  const merged = mergeConfigs(
    {},
    {},
    loadProjectConfig(cwd),
    loadGlobalConfig(),
    loadLocalConfig(cwd),
    managed,
  );
  return {
    approved: merged.externalMemoryImportsApproved === true,
    roots: merged.externalMemoryImportRoots ?? [],
  };
}

/**
 * True-only trust latch (parity: the reference's `computeTrustDialogAccepted` latch that
 * caches ONLY the positive result). Workspace trust only ever PROMOTES within a running
 * process — once a directory resolves trusted it stays trusted for the lifetime of the
 * process even if the backing config is later edited to remove the grant, so a mid-
 * session config edit can never silently REVOKE trust the session already acted on. A
 * `false` result is never cached (a trust grant added mid-session still takes effect on
 * the next check). Keyed by {@link normalizeTrustPath}.
 */
const workspaceTrustLatch = new Set<string>();

/**
 * Resolve whether the workspace rooted at `cwd` has been granted FOLDER TRUST, by
 * merging the full config waterfall (managed > global > project > local — CLI/env carry
 * no trust flag) and honoring BOTH trust surfaces:
 *  1. the single-workspace {@link ProjectConfig.hasTrustDialogAccepted} boolean, and
 *  2. the inherited {@link ProjectConfig.trustedProjectPaths} allowlist — the normalized
 *     `cwd` is trusted when it equals, or is nested under, ANY listed path (an ancestor
 *     walk from `cwd` up to the filesystem root), so trusting a parent directory implies
 *     trust of all its children. Parity: the reference's `computeTrustDialogAccepted`
 *     walking `resolve(path, '..')` up every ancestor returning true if any has trust.
 *
 * A positive result is latched (see {@link workspaceTrustLatch}) so trust never silently
 * revokes mid-session. Kept in the config layer (not hooks.ts) so there is ONE place
 * that reads the config files and applies the merge semantics — the same pattern as
 * {@link resolveConfiguredMemoryExcludes}. A broken/untrusted managed policy fails
 * closed ({@link ManagedConfigError}); a trust lookup must never abort hook loading over
 * it, so it is treated as absent. Consumed by {@link resolveWorkspaceTrustAccepted} →
 * {@link import('./hooks').HookManager.loadFromFile} to set workspace trust.
 */
export function isPathTrusted(cwd: string): boolean {
  const normalized = normalizeTrustPath(cwd);
  if (workspaceTrustLatch.has(normalized)) return true;
  const managed = loadManagedConfigSafe();
  const merged = mergeConfigs(
    {},
    {},
    loadProjectConfig(cwd),
    loadGlobalConfig(),
    loadLocalConfig(cwd),
    managed,
  );
  let trusted = merged.hasTrustDialogAccepted === true;
  if (!trusted) {
    const roots = merged.trustedProjectPaths;
    if (Array.isArray(roots) && roots.length > 0) {
      const trustedSet = new Set(roots.map(r => normalizeTrustPath(r)));
      // Ancestor walk: from the normalized cwd up through every parent until the
      // filesystem root (where dirname(x) === x), stopping as soon as a trusted root
      // is hit — so a trusted ancestor transitively trusts every descendant.
      let current = normalized;
      for (;;) {
        if (trustedSet.has(current)) { trusted = true; break; }
        const parent = path.dirname(current);
        if (parent === current) break;
        current = parent;
      }
    }
  }
  if (trusted) workspaceTrustLatch.add(normalized);
  return trusted;
}

/**
 * Resolve whether the workspace rooted at `cwd` has been granted FOLDER TRUST. Thin
 * alias of {@link isPathTrusted} kept as the stable name consumed by
 * {@link import('./hooks').HookManager.loadFromFile}; see that function for the full
 * (boolean + inherited trusted-path + latch) resolution semantics.
 */
export function resolveWorkspaceTrustAccepted(cwd: string): boolean {
  return isPathTrusted(cwd);
}

// Wire the ConfigService reload seam to the real implementation (declared earlier
// as a placeholder so the class body can reference it without a TDZ).
configEnvHooks.applyConfigEnv = applyConfigEnv;

/**
 * Bridge the persisted {@link ProjectConfig.extraCaCerts} knob into
 * `NODE_EXTRA_CA_CERTS` so Node picks up a corporate-proxy / TLS-inspecting CA at
 * startup, without the user having to export the env var before every launch
 * (parity: the reference's `caCertsConfig.applyExtraCACertsFromConfig`). An explicit
 * `NODE_EXTRA_CA_CERTS` already in `target` WINS (one-shot override) and is returned
 * unchanged; otherwise a non-blank `config.extraCaCerts` that resolves to an
 * existing, readable REGULAR file seeds it (trimmed). A missing/unreadable/
 * non-regular path is ignored and logged (fail-safe: never point Node at a bad CA
 * bundle). Returns the effective path, or `undefined` when none applies. Applied
 * once at engine setup, mirroring {@link applyConfigLanguage}.
 */
export function applyConfigCaCerts(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): string | undefined {
  const existing = target.NODE_EXTRA_CA_CERTS;
  if (existing && existing.trim()) return existing.trim();
  const configured = config?.extraCaCerts;
  if (typeof configured === 'string' && configured.trim()) {
    const trimmed = configured.trim();
    try {
      if (fsHooks.statSync(trimmed).isFile()) {
        target.NODE_EXTRA_CA_CERTS = trimmed;
        return trimmed;
      }
      coreLog('config', `extraCaCerts path is not a regular file, ignoring: ${trimmed}`);
    } catch {
      coreLog('config', `extraCaCerts path is unreadable, ignoring: ${trimmed}`);
    }
  }
  return undefined;
}

export function applyConfigStartupKnobs(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): void {
  applyConfigFeatureFlags(config, target);
  applyConfigCaCerts(config, target);
  applyConfigGitignore(config, target);
  applyConfigAutoCompact(config, target);
  applyConfigPlansDirectory(config, target);
  applyConfigAnthropicBetas(config, target);
  applyConfigTerminalProgress(config, target);
  applyConfigTurnDuration(config, target);
  applyConfigVerbose(config, target);
  applyConfigCopyOnSelect(config, target);
  applyConfigFullscreen(config, target);
  applyConfigSyntaxHighlighting(config, target);
  applyConfigIdleNotifThreshold(config, target);
  applyConfigSpinnerVerbs(config, target);
  applyConfigBackgroundTasks(config, target);
  applyConfigWorkflowSize(config, target);
}

/**
 * Milliseconds after which a cached {@link ProjectConfig.exampleFiles} list is considered
 * stale (one week). Parity: the reference's `ONE_WEEK_IN_MS` staleness window in
 * `refreshExampleCommands`.
 */
export const EXAMPLE_FILES_TTL_MS = 7 * 24 * 60 * 60 * 1000;

/**
 * The cached frequently-modified example files, resolved off the already-MERGED
 * {@link ProjectConfig} with a staleness check. Parity: the reference's `exampleFiles` +
 * `exampleFilesGeneratedAt` read in `refreshExampleCommands` — a cache older than
 * {@link EXAMPLE_FILES_TTL_MS} (relative to `nowMs`, default `Date.now()`) is treated as
 * expired and returns `[]` so the caller refreshes it; a never-generated cache (no
 * timestamp) is likewise treated as expired. An empty `[]` is returned (never `undefined`)
 * so callers can branch on `.length`.
 */
export function getCachedExampleFiles(
  config: ProjectConfig | null | undefined,
  nowMs: number = Date.now(),
): string[] {
  const files = config?.exampleFiles;
  if (!Array.isArray(files) || files.length === 0) return [];
  const generatedAt = config?.exampleFilesGeneratedAt ?? 0;
  if (nowMs - generatedAt > EXAMPLE_FILES_TTL_MS) return [];
  return files;
}

/**
 * Apply every registered {@link DEPRECATED_CONFIG_KEY_MIGRATIONS} to the config at
 * `configPath` in ONE lock-safe read-modify-write (via {@link saveGlobalConfig}'s
 * underlying {@link updateConfigFile}). Each rename is {@link migrateConfigKey}-guarded
 * (act only when old present AND new unset), so the whole pass is idempotent and
 * clobber-safe and may run on every startup. Returns the list of `from` keys actually
 * migrated (empty when nothing changed or there is no resolvable config path), so the
 * caller can surface a one-time notice. Best-effort: a write failure is swallowed by the
 * caller's error handling, never thrown into startup.
 */
export function runConfigKeyMigrations(
  configPath: string | null = getGlobalConfigPath(),
  migrations: ReadonlyArray<ConfigKeyMigration> = DEPRECATED_CONFIG_KEY_MIGRATIONS,
): string[] {
  if (!configPath || migrations.length === 0) return [];
  const migrated: string[] = [];
  updateConfigFile(configPath, (current) => {
    let next = current;
    for (const m of migrations) {
      const applied = migrateConfigKey(next, m.from, m.to, m.coerce);
      if (applied !== next) migrated.push(m.from);
      next = applied;
    }
    return next;
  });
  return migrated;
}

/**
 * Append `newItem` to a JSONC ARRAY file's content while PRESERVING existing comments
 * and formatting (parity: the reference's `addItemToJSONCArray`, which uses jsonc-parser
 * `modify()`+`applyEdits` with `isArrayInsertion`). AMI's object config writes go
 * through {@link safeWriteConfig} (a full `JSON.stringify` rewrite that STRIPS comments);
 * this is the array-append counterpart that does NOT clobber comments. Cases mirror the
 * reference:
 *   - empty/whitespace content → a fresh single-element array;
 *   - content parses to an empty array → the item becomes index 0;
 *   - content parses to a non-empty array → the item is appended after the last element;
 *   - content is valid JSON but NOT an array → replaced with a fresh single-element array;
 *   - unparseable / the array brackets cannot be located → fall back to a full
 *     `JSON.stringify` of `[...parsed, newItem]` (or `[newItem]`).
 * The insertion edits the raw text around the array's own brackets (string/comment-aware
 * bracket matching), so sibling comments survive; only an empty array's INTERIOR comment
 * is not retained (the array is reformatted). Two-space indentation, matching AMI's
 * {@link safeWriteConfig} house style.
 */
export function addItemToJSONCArray(content: string, newItem: unknown): string {
  const fresh = () => JSON.stringify([newItem], null, 2) + '\n';
  try {
    if (!content || content.trim() === '') return fresh();
    const clean = stripBom(content);
    const parsed = parseJsonc(clean);
    if (!Array.isArray(parsed)) return fresh();
    const edited = insertIntoJsoncArray(clean, parsed.length === 0, newItem);
    if (edited === null) return JSON.stringify([...parsed, newItem], null, 2) + '\n';
    return edited;
  } catch {
    return fresh();
  }
}

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
    // it is broken. Parity: the reference's root-level JSON syntax-error tip.
    return { valid: false, error: `Invalid JSON: ${errorMessage(err)}. ${JSON_SYNTAX_RECOVERY_HINT}` };
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return { valid: false, error: `Expected a JSON object, got ${describeJsonType(parsed)}` };
  }
  return { valid: true };
}

/**
 * STRICT, schema-aware validation of raw config-file CONTENT, for the edit/check path.
 * Unlike {@link validateConfigJson} — the lightweight parse+object gate used by LOADERS,
 * which deliberately accepts a parseable-but-advisory config so the valid parts still
 * load — this performs field-level value validation (wrong type / bad enum / out-of-range
 * via {@link validateProjectConfigValues}) AND unknown-key detection
 * ({@link findUnknownConfigKeys}), and returns the full {@link getConfigJSONSchema}
 * output so a caller can show the user/agent exactly what shape is expected and repair
 * the file. Parity: the reference's `validateSettingsFileContent` (strict-mode schema
 * validation separating the parse failure from the per-field error list, returning the
 * full schema). Reuses the ONE canonical value validator + schema generator — no parallel
 * schema logic.
 */
export function validateConfigFileContent(content: string): ConfigFileValidation {
  const schema = getConfigJSONSchema();
  let parsed: unknown;
  try {
    parsed = parseJsonc(content);
  } catch (err) {
    return {
      valid: false,
      parseError: `Invalid JSON: ${errorMessage(err)}. ${JSON_SYNTAX_RECOVERY_HINT}`,
      issues: [],
      unknownKeys: [],
      schema,
    };
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return {
      valid: false,
      parseError: `Expected a JSON object, got ${describeJsonType(parsed)}`,
      issues: [],
      unknownKeys: [],
      schema,
    };
  }
  const config = parsed as ProjectConfig;
  const issues = validateProjectConfigValues(config);
  const unknownKeys = findUnknownConfigKeys(config);
  return { valid: issues.length === 0 && unknownKeys.length === 0, issues, unknownKeys, schema };
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
 * The most recent known-GOOD pre-write snapshot (`<file>.backup-<ms>`) beside
 * `filePath`, or null when none exists. These snapshots are written by
 * {@link backupConfigBeforeWrite} before every overwrite, so the newest one is
 * the last byte-valid config AMI persisted — the right source to restore over a
 * corrupt file. Distinct from {@link findMostRecentBackup}, which finds the
 * `.corrupt-*` QUARANTINE of a broken file (not a restore source).
 */
export function findMostRecentConfigBackup(filePath: string): string | null {
  return listConfigBackups(filePath)[0] ?? null;
}

/**
 * Resolve the REAL file a config write must land on: when `filePath` is a symlink
 * (e.g. a dotfile-managed `~/.ami/config.json` stow/chezmoi link), return its
 * target so the write updates the target IN PLACE and preserves the symlink —
 * instead of a tmp+rename replacing the link with a regular file. A non-symlink
 * or absent path resolves to itself. Parity: the reference's "if settings.json is a
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
 * path written, or null. Parity: the reference's saveConfigWithLock backup rotation.
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
 *    half-flushed config even though the rename "succeeded" (parity: the reference's
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

/** Number of successful writes to the GLOBAL config file this process. */
let globalConfigWriteCount = 0;

/**
 * How many times the GLOBAL config (`~/.ami/config.json`) has been written this
 * process. Introspection surface over config churn (parity: the reference's
 * getGlobalConfigWriteCount) — a value at/over {@link CONFIG_WRITE_DISPLAY_THRESHOLD}
 * signals a likely write loop before it damages the file.
 */
export function getGlobalConfigWriteCount(): number {
  return globalConfigWriteCount;
}

/** Count a successful global-config write + warn once on crossing the threshold. */
function recordGlobalConfigWrite(target: string): void {
  const globalPath = getGlobalConfigPath();
  if (globalPath === null || path.resolve(target) !== path.resolve(globalPath)) return;
  globalConfigWriteCount++;
  if (globalConfigWriteCount === CONFIG_WRITE_DISPLAY_THRESHOLD) {
    coreLog('config', `global config at ${target} has been written ${globalConfigWriteCount} times this process — an unusually high rate may indicate a write loop (watch for corruption).`);
  }
}

export function safeWriteConfig(filePath: string, data: Record<string, unknown>): void {
  // Write THROUGH a symlinked config path to its target so a dotfile-managed
  // settings symlink is updated in place rather than clobbered by a regular file
  // (parity: the reference's "if settings.json is a symlink, update the target file").
  const target = resolveConfigWriteTarget(filePath);
  // PID-scoped scratch path: two AMI instances writing the SAME config file
  // concurrently (e.g. the shared GLOBAL `~/.ami/config.json`) must NOT share one
  // `<target>.tmp` — otherwise instance A's rename can promote instance B's
  // half-written bytes, or A's cleanup can delete B's in-flight temp, corrupting
  // the target. Embedding this process's PID gives each instance an isolated
  // scratch namespace (parity: the reference's PID-scoped getSwarmSocketName). A crash
  // leaves a PID-tagged orphan that {@link cleanupStaleConfigTmp} can reclaim
  // only once that PID is dead.
  const tmpPath = instanceScopedTempName(target);
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
  // Count a successful write to the shared GLOBAL config so pathological churn
  // (a save loop, two fighting instances) is observable before it corrupts the file.
  recordGlobalConfigWrite(target);
  // Single invalidation point (parity: the reference's resetSettingsCache fired on write):
  // the file we just wrote IS the new config, so drop every config read memo. The
  // next loadProjectConfig/loadLocalConfig/loadGlobalConfig re-stats + re-reads the
  // fresh bytes instead of serving a now-stale identity-cached snapshot.
  resetConfigCache();
}

/**
 * The known-default project config written over a file by {@link resetConfigToDefault}.
 * An empty object means "all built-in defaults" — AMI has no mandatory config keys, so
 * every setting falls back to its engine default when the file is `{}`. Frozen so the
 * shared default can never be mutated in place (each write takes a fresh shallow copy).
 * Parity: the reference's InvalidConfigDialog "Reset with default configuration" branch,
 * which writes `error.defaultConfig` over the broken file.
 */
export const DEFAULT_PROJECT_CONFIG: Readonly<ProjectConfig> = Object.freeze({});

/**
 * Self-heal a config file: QUARANTINE its current bytes (if any) to a
 * `<file>.corrupt-<ts>` backup, then write {@link DEFAULT_PROJECT_CONFIG} in their
 * place — so a user whose `config.json` is corrupt (and is therefore being silently
 * ignored, with `/config set` refusing to overwrite it) can recover in ONE action
 * instead of hand-editing broken JSON. The previous content is never lost: it is backed
 * up first (content-deduped by {@link backupCorruptConfig}), then the default is written
 * through the canonical atomic {@link safeWriteConfig} path (temp+rename, durable flush,
 * mode preservation, internal-write suppression, symlink write-through). Returns the
 * quarantine path so a caller can tell the user exactly where their old file went.
 * Parity: the reference's InvalidConfigDialog "Reset with default configuration" branch.
 */
export function resetConfigToDefault(filePath: string): ResetConfigResult {
  // Read through a symlinked config path to its target so the pre-reset snapshot
  // captures the REAL current bytes (safeWriteConfig writes through the same target).
  const target = resolveConfigWriteTarget(filePath);
  let previous: string | null = null;
  try {
    if (fsHooks.existsSync(target)) previous = fsHooks.readFileSync(target, 'utf-8') as string;
  } catch {
    // Unreadable present file — nothing recoverable to quarantine; still reset it.
    previous = null;
  }
  // Quarantine the prior bytes (deduped against an identical existing quarantine) so a
  // corrupt config stays recoverable after the reset.
  const backupPath = previous !== null ? backupCorruptConfig(target, previous) : null;
  safeWriteConfig(filePath, { ...DEFAULT_PROJECT_CONFIG });
  coreLog('config', `reset ${filePath} to default config${backupPath ? ` (previous content backed up to ${backupPath})` : ''}`);
  return { path: filePath, backupPath, hadExisting: previous !== null };
}

/**
 * Guarded functional read-modify-write of a config file (parity: the reference's
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
 *     the reference's "re-read returned defaults → don't persist defaults over good
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
 * lost-update race the step-1 re-read only narrows is closed outright. Parity: the reference
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
  // time (parity: the reference's saveConfigWithLock mkdirSync's the dir before lockSync).
  try {
    const dir = path.dirname(lockTarget);
    if (!fsHooks.existsSync(dir)) fsHooks.mkdirSync(dir, { recursive: true });
  } catch { /* safeWriteConfig re-attempts dir creation and surfaces a real error */ }
  const started = configClockHooks.now();
  return configLockHooks.withFileLockSync(lockTarget, () => {
    // Log contention (parity: the reference's "lock took longer than expected — another
    // instance may be running"), measured as time spent waiting to enter the section.
    const waited = configClockHooks.now() - started;
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
    // default throw become an unhandled exception (parity: the reference's onCompromised).
    onCompromised: (err) => coreLog('config', `config write lock compromised for ${lockTarget}: ${err.message}`),
    ...lockOptions,
  });
}

/**
 * Read the persisted per-skill usage map ({@link ProjectConfig.skillUsage}) from the
 * GLOBAL config, or an empty map when none exists / the path is unavailable. Used at
 * session start to seed a skill's cross-session frecency ranking. Best-effort: any read
 * failure yields `{}` (a missing history must never block startup). Parity: the reference
 * Code's `getGlobalConfig().skillUsage` read.
 */
export function loadPersistedSkillUsage(): Record<string, SkillUsageStat> {
  // loadGlobalConfig never throws (tier loader catches I/O); absent → {}.
  return loadGlobalConfig()?.skillUsage ?? {};
}

/**
 * Record one invocation of `name` into the GLOBAL config's {@link ProjectConfig.skillUsage}
 * map (bumping `usageCount`, stamping `lastUsedAt = now`) via the lock-safe
 * read-modify-write {@link updateConfigFile}, so cross-process writers never lose each
 * other's counts. Returns `true` when the update was written, `false` when there is no
 * writable global-config path or the write was skipped/failed (best-effort — a usage
 * write must never surface as an error to the caller). The increment formula is the ONE
 * canonical {@link mergeSkillUsageStat}. Parity: the reference's `saveGlobalConfig` on each
 * skill use (`utils/suggestions/skillUsageTracking.ts`).
 */
export function persistSkillUsageStat(name: string, now: number = Date.now()): boolean {
  const target = getGlobalConfigPath();
  if (!target) return false;
  try {
    const result = updateConfigFile(target, current => ({
      ...current,
      skillUsage: mergeSkillUsageStat(current.skillUsage, name, now),
    }));
    return result.written;
  } catch {
    return false;
  }
}

/**
 * Record one invocation of `name` for frecency ranking, DEBOUNCED against the
 * caller-owned `lastWriteBySkill` map so a skill invoked repeatedly in quick succession
 * does not take the global-config lock + read + parse + write on every call. When the
 * previous recorded write for this skill was under {@link SKILL_USAGE_DEBOUNCE_MS} ago,
 * it bails BEFORE touching disk and returns `false`; otherwise it stamps the map and
 * delegates the actual read-modify-write to {@link persistSkillUsageStat}, returning
 * that function's `written` verdict. Best-effort (never throws).
 *
 * The debounce cache is passed IN (not module-global as in the reference) so each owner
 * scopes it to its own lifetime — AMI creates many `SkillManager`s per process (sessions,
 * forked sub-engines), and an instance-scoped map keeps independent sessions from
 * suppressing each other's first write. Parity with the reference's `recordSkillUsage`
 * pattern, adapted to AMI's multi-manager model.
 */
export function recordSkillUsageDebounced(
  name: string,
  lastWriteBySkill: Map<string, number>,
  now: number = Date.now(),
): boolean {
  if (!name) return false;
  const last = lastWriteBySkill.get(name);
  if (last !== undefined && now - last < SKILL_USAGE_DEBOUNCE_MS) return false;
  lastWriteBySkill.set(name, now);
  return persistSkillUsageStat(name, now);
}

/**
 * Global-scoped transactional read-modify-write of the user config (`~/.ami/config.json`)
 * — a thin wrapper over the lock-safe {@link updateConfigFile} that resolves the global
 * path and null-checks it in ONE place (parity: the reference's `saveGlobalConfig(updater)`
 * lost-update + same-reference-skip + corrupt-refuse guard). The caller passes a pure
 * `updater(current) -> next`; returning the SAME reference is a no-op write. Returns the
 * {@link UpdateConfigResult}, or `null` when there is no resolvable global config path
 * (no HOME/USERPROFILE). Best-effort callers should treat `null`/`!written` as "not
 * persisted" rather than an error. The underlying cross-process lock means two AMI
 * instances never clobber each other's global-config keys.
 */
export function saveGlobalConfig(
  updater: (current: ProjectConfig) => ProjectConfig,
): UpdateConfigResult | null {
  const target = getGlobalConfigPath();
  if (!target) return null;
  return updateConfigFile(target, updater);
}

/**
 * Read the persisted one-time preference-flag map ({@link ProjectConfig.preferencesShown})
 * from the GLOBAL config, or `{}` when none exists / the path is unavailable. Best-effort:
 * any read failure yields `{}` so a missing flag store never blocks startup.
 */
export function loadPreferenceFlags(): Record<string, boolean> {
  // loadGlobalConfig never throws (tier loader catches I/O); absent → {}.
  return loadGlobalConfig()?.preferencesShown ?? {};
}

/**
 * True iff the named one-time preference/dialog has already been marked shown (parity:
 * the reference's `shouldShow*Dialog` guards gating on a persisted `has*BeenShown` flag).
 * An empty name is never "shown". Best-effort read (see {@link loadPreferenceFlags}).
 */
export function wasPreferenceShown(name: string): boolean {
  if (!name) return false;
  return loadPreferenceFlags()[name] === true;
}

/**
 * Mark a one-time preference/dialog as shown in the GLOBAL config via the lock-safe
 * {@link saveGlobalConfig}, so a first-run prompt/notice is surfaced at most once and the
 * flag survives across sessions (parity: the reference writing `hasIdeAutoConnectDialogBeenShown`
 * / `hasUsedBackgroundTask` / the bypass-permissions acknowledgment). Concurrency-safe:
 * the updater re-checks the freshly-read flag and returns the same reference (no write)
 * when another writer already set it — so the flag is set exactly once. Returns `true`
 * iff it was newly persisted; `false` when the name is empty, the flag was already set,
 * there is no writable global path, or the best-effort write failed.
 */
export function markPreferenceShown(name: string): boolean {
  if (!name) return false;
  try {
    const result = saveGlobalConfig(current =>
      current.preferencesShown?.[name] === true
        ? current
        : { ...current, preferencesShown: { ...current.preferencesShown, [name]: true } },
    );
    return result?.written ?? false;
  } catch {
    return false;
  }
}

/**
 * The release-notes/changelog VERSION the user has already seen
 * ({@link ProjectConfig.lastReleaseNotesSeen}). Reads the passed-in config when given
 * (e.g. the merged config) and otherwise the GLOBAL config; a non-string / blank value
 * is normalized to `undefined`. Best-effort: any read failure yields `undefined` so a
 * missing store never blocks the what's-new banner decision.
 */
export function getLastReleaseNotesSeen(
  config?: ProjectConfig | null,
): string | undefined {
  const v = (config ?? loadGlobalConfig())?.lastReleaseNotesSeen;
  return typeof v === 'string' && v.trim() ? v : undefined;
}

/**
 * Whether a one-time "what's new" release-notes banner should be shown for
 * `currentVersion` — i.e. the persisted {@link ProjectConfig.lastReleaseNotesSeen}
 * differs from it (parity: the reference's `config.lastReleaseNotesSeen !== VERSION` gate
 * in `LogoV2.tsx`). A blank `currentVersion` is never "unseen" (no version to compare
 * against). Reads the passed-in config when given, otherwise the GLOBAL config.
 */
export function hasUnseenReleaseNotes(
  currentVersion: string,
  config?: ProjectConfig | null,
): boolean {
  if (!currentVersion || !currentVersion.trim()) return false;
  return getLastReleaseNotesSeen(config) !== currentVersion;
}

/**
 * Record that the user has seen the release notes for `version` by persisting it as
 * {@link ProjectConfig.lastReleaseNotesSeen} in the GLOBAL config via the lock-safe
 * {@link saveGlobalConfig}, so the what's-new banner is surfaced at most once per release
 * (parity: the reference writing `lastReleaseNotesSeen = VERSION` after the banner is
 * shown). Concurrency-safe: the updater re-reads the fresh value and returns the same
 * reference (no write) when it already matches. Returns `true` iff the value was newly
 * persisted; `false` when `version` is empty, it was already current, there is no
 * writable global path, or the best-effort write failed.
 */
export function markReleaseNotesSeen(version: string): boolean {
  if (!version || !version.trim()) return false;
  try {
    const result = saveGlobalConfig(current =>
      current.lastReleaseNotesSeen === version
        ? current
        : { ...current, lastReleaseNotesSeen: version },
    );
    return result?.written ?? false;
  } catch {
    return false;
  }
}

/**
 * Read the persisted notice seen-count map ({@link ProjectConfig.noticeSeenCounts}) from
 * the GLOBAL config, or `{}` when none exists / the path is unavailable. Best-effort.
 */
export function loadNoticeSeenCounts(): Record<string, number> {
  // loadGlobalConfig never throws (tier loader catches I/O); absent → {}.
  return loadGlobalConfig()?.noticeSeenCounts ?? {};
}

/**
 * The persisted seen-count for a count-limited notice (parity: the reference's persisted
 * per-notice seen-count). A missing / non-numeric / negative stored value normalizes to
 * `0` (a corrupt count must never suppress a notice forever). An empty name is `0`.
 */
export function getNoticeSeenCount(name: string): number {
  if (!name) return 0;
  const n = loadNoticeSeenCounts()[name];
  return typeof n === 'number' && Number.isFinite(n) && n > 0 ? Math.floor(n) : 0;
}

/**
 * Whether a count-limited notice should still be shown — i.e. its persisted seen-count is
 * below `maxCount` (parity: the reference's `shouldShow = seenCount < MAX_SHOW_COUNT` gate).
 * A non-positive `maxCount` or empty name means "never show".
 */
export function shouldShowNotice(name: string, maxCount: number): boolean {
  if (!name || !(maxCount > 0)) return false;
  return getNoticeSeenCount(name) < maxCount;
}

/**
 * Increment the persisted seen-count for a count-limited notice via the lock-safe
 * {@link saveGlobalConfig}, capped at `maxCount` so it never grows unbounded (parity:
 * the reference's concurrency-safe seen-count increment). The increment is computed from
 * the FRESH on-disk value inside the updater, so two processes mounting the same notice
 * serialize and each sees the other's increment (no double-count via a stale base); and
 * when the count is already at the cap the updater returns the same reference (no write).
 * Returns `true` iff the count was advanced; `false` when the name is empty, `maxCount`
 * is non-positive, the count is already at the cap, there is no writable global path, or
 * the best-effort write failed.
 */
export function recordNoticeSeen(name: string, maxCount: number): boolean {
  if (!name || !(maxCount > 0)) return false;
  try {
    const result = saveGlobalConfig(current => {
      const raw = current.noticeSeenCounts?.[name];
      const cur = typeof raw === 'number' && Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : 0;
      if (cur >= maxCount) return current; // already at the cap — no write
      return { ...current, noticeSeenCounts: { ...current.noticeSeenCounts, [name]: cur + 1 } };
    });
    return result?.written ?? false;
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------------------
// Project-scoped guarded writers (the project/local analogs of saveGlobalConfig)
// ---------------------------------------------------------------------------

/**
 * Lock-safe functional read-modify-write of a PROJECT config file
 * (`<cwd>/.ami/config.json`) — the project-scoped analog of {@link saveGlobalConfig},
 * reusing the one canonical {@link updateConfigFile} path (atomic temp+rename, fresh
 * re-read under a cross-process lock, same-reference no-op). Parity: the reference's
 * `saveCurrentProjectConfig(updater)` used for durable per-project state
 * (`lastSessionMetrics`, the onboarding fatigue counters).
 */
export function saveProjectConfig(
  cwd: string,
  updater: (current: ProjectConfig) => ProjectConfig,
  lockOptions: FileLockSyncOptions = {},
): UpdateConfigResult {
  return updateConfigFile(getProjectConfigPath(cwd), updater, lockOptions);
}

/**
 * Ensure the per-user LOCAL config file (`.ami/config.local.json`) is listed in the
 * project's `.gitignore`, so a user-specific override is never accidentally committed
 * (parity: the reference's `addFileGlobRuleToGitignore` fired when it writes
 * `settings.local.json`). Idempotent: the rule is matched as a whole line (ignoring
 * surrounding whitespace and an optional leading slash) so it is never appended twice.
 * Best-effort — any read/write failure is swallowed (a missing gitignore write must
 * never block the config write). Returns `true` iff the rule was newly appended.
 */
export function ensureLocalConfigGitignored(cwd: string): boolean {
  const gitignorePath = path.join(cwd, '.gitignore');
  const rule = LOCAL_CONFIG_GITIGNORE_RULE;
  try {
    let existing = '';
    try {
      existing = fsHooks.readFileSync(gitignorePath, 'utf-8') as string;
    } catch {
      existing = ''; // absent .gitignore → create it with just the rule
    }
    const alreadyListed = existing
      .split(/\r?\n/)
      .map(l => l.trim())
      .some(l => l === rule || l === '/' + rule);
    if (alreadyListed) return false;
    const needsLeadingNl = existing.length > 0 && !existing.endsWith('\n');
    fsHooks.writeFileSync(gitignorePath, `${existing}${needsLeadingNl ? '\n' : ''}${rule}\n`, 'utf-8');
    return true;
  } catch {
    return false;
  }
}

/**
 * Lock-safe functional read-modify-write of the per-user LOCAL config file
 * (`<cwd>/.ami/config.local.json`) via {@link updateConfigFile}; on a successful write it
 * fire-and-forgets {@link ensureLocalConfigGitignored} so the local override is auto-added
 * to `.gitignore` (parity: the reference auto-gitignoring `settings.local.json` on write).
 */
export function saveLocalConfig(
  cwd: string,
  updater: (current: ProjectConfig) => ProjectConfig,
  lockOptions: FileLockSyncOptions = {},
): UpdateConfigResult {
  const result = updateConfigFile(getLocalConfigPath(cwd), updater, lockOptions);
  if (result.written) ensureLocalConfigGitignored(cwd);
  return result;
}

// ---------------------------------------------------------------------------
// Durable last-session metrics rollup (parity: the reference's StatsProvider exit flush)
// ---------------------------------------------------------------------------

/**
 * Read the previous session's persisted metric rollup ({@link ProjectConfig.lastSessionMetrics})
 * from the PROJECT config, or `{}` when none exists / the path is unavailable. Corrupt
 * entries (non-finite / negative) are dropped so a bad counter never flows downstream.
 * Best-effort: any read failure yields `{}`.
 */
export function readLastSessionMetrics(cwd: string): Record<string, number> {
  // loadProjectConfig never throws (tier loader catches I/O).
  const m = loadProjectConfig(cwd)?.lastSessionMetrics;
  if (!m || typeof m !== 'object') return {};
  const out: Record<string, number> = {};
  for (const [k, v] of Object.entries(m)) {
    if (typeof v === 'number' && Number.isFinite(v) && v >= 0) out[k] = v;
  }
  return out;
}

/**
 * Persist an aggregated metric snapshot as {@link ProjectConfig.lastSessionMetrics} in the
 * PROJECT config via the lock-safe {@link saveProjectConfig}, carrying the session's rollup
 * forward for the next launch. Parity: the reference's StatsProvider `process.on('exit')`
 * flush — which writes ONLY a NON-EMPTY snapshot (`Object.keys(metrics).length > 0`), so an
 * empty map is a no-op here too. Returns `true` iff the snapshot was newly persisted;
 * `false` on an empty/invalid map or a best-effort write failure.
 */
export function saveLastSessionMetrics(cwd: string, metrics: Record<string, number>): boolean {
  if (!metrics || typeof metrics !== 'object') return false;
  const entries = Object.entries(metrics).filter(
    ([, v]) => typeof v === 'number' && Number.isFinite(v) && v >= 0,
  );
  if (entries.length === 0) return false; // parity: skip the empty-snapshot flush
  try {
    const snapshot: Record<string, number> = Object.fromEntries(entries);
    const result = saveProjectConfig(cwd, current => ({ ...current, lastSessionMetrics: snapshot }));
    return result.written;
  } catch {
    return false;
  }
}

/**
 * The persisted per-project onboarding seen-count ({@link ProjectConfig.projectOnboardingSeenCount}).
 * A missing / non-numeric / negative value normalizes to `0`. Best-effort.
 */
export function getProjectOnboardingSeenCount(cwd: string): number {
  // loadProjectConfig never throws (tier loader catches I/O).
  const n = loadProjectConfig(cwd)?.projectOnboardingSeenCount;
  return typeof n === 'number' && Number.isFinite(n) && n > 0 ? Math.floor(n) : 0;
}

/**
 * Whether the per-project onboarding affordance should still be shown — i.e. it has NOT
 * been completed, its seen-count is below {@link PROJECT_ONBOARDING_SEEN_CAP}, and this is
 * not a demo environment (parity: the reference's `shouldShowProjectOnboarding`). Best-effort:
 * a read failure is treated as "do not nag".
 */
export function shouldShowProjectOnboarding(cwd: string): boolean {
  if (process.env.IS_DEMO) return false;
  // loadProjectConfig never throws (tier loader catches I/O).
  const cfg = loadProjectConfig(cwd);
  if (cfg?.hasCompletedProjectOnboarding === true) return false;
  const raw = cfg?.projectOnboardingSeenCount;
  const seen = typeof raw === 'number' && Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : 0;
  return seen < PROJECT_ONBOARDING_SEEN_CAP;
}

/**
 * Increment the persisted per-project onboarding seen-count via the lock-safe
 * {@link saveProjectConfig}, capped at {@link PROJECT_ONBOARDING_SEEN_CAP} so it never grows
 * unbounded (parity: the reference's `incrementProjectOnboardingSeenCount`). The increment is
 * computed from the FRESH on-disk value inside the updater; at the cap the updater returns
 * the same reference (no write). Returns `true` iff the count was advanced.
 */
export function incrementProjectOnboardingSeenCount(cwd: string): boolean {
  try {
    const result = saveProjectConfig(cwd, current => {
      const raw = current.projectOnboardingSeenCount;
      const cur = typeof raw === 'number' && Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : 0;
      if (cur >= PROJECT_ONBOARDING_SEEN_CAP) return current; // capped — no write
      return { ...current, projectOnboardingSeenCount: cur + 1 };
    });
    return result.written;
  } catch {
    return false;
  }
}

/**
 * Mark the per-project onboarding complete (sets {@link ProjectConfig.hasCompletedProjectOnboarding})
 * via the lock-safe {@link saveProjectConfig}, permanently suppressing the nudge (parity:
 * the reference's `maybeMarkProjectOnboardingComplete`). Concurrency-safe: when the flag is
 * already set the updater returns the same reference (no write). Returns `true` iff newly set.
 */
export function maybeMarkProjectOnboardingComplete(cwd: string): boolean {
  try {
    const result = saveProjectConfig(cwd, current =>
      current.hasCompletedProjectOnboarding === true
        ? current
        : { ...current, hasCompletedProjectOnboarding: true },
    );
    return result.written;
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------------------
// deepLinkTerminal preference (parity: terminalLauncher.ts)
// ---------------------------------------------------------------------------

/**
 * The persisted preferred terminal-emulator name ({@link ProjectConfig.deepLinkTerminal}) —
 * read from the passed-in config when given (e.g. the merged config), else the GLOBAL config
 * (parity: the reference's `getGlobalConfig().deepLinkTerminal`). A non-string / blank value
 * normalizes to `undefined`. Best-effort.
 */
export function resolveDeepLinkTerminal(config?: ProjectConfig | null): string | undefined {
  // loadGlobalConfig never throws (tier loader catches I/O).
  const v = (config ?? loadGlobalConfig())?.deepLinkTerminal;
  return typeof v === 'string' && v.trim() ? v : undefined;
}

/**
 * Persist the preferred terminal-emulator name as {@link ProjectConfig.deepLinkTerminal} in
 * the GLOBAL config via the lock-safe {@link saveGlobalConfig} (parity: the reference storing
 * `deepLinkTerminal` so the preference survives into a headless LaunchServices context).
 * Concurrency-safe: an unchanged value is a no-op. Returns `true` iff newly persisted.
 */
export function saveDeepLinkTerminal(name: string): boolean {
  if (!name || !name.trim()) return false;
  try {
    const result = saveGlobalConfig(current =>
      current.deepLinkTerminal === name ? current : { ...current, deepLinkTerminal: name },
    );
    return result?.written ?? false;
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------------------
// Interaction-recency deferral for background work (parity: backgroundHousekeeping.ts)
// ---------------------------------------------------------------------------

/**
 * Default delay (ms) before an interactivity-aware slow background operation first runs /
 * is re-checked (parity: the reference's `DELAY_VERY_SLOW_OPERATIONS_THAT_HAPPEN_EVERY_SESSION`
 * = 10 min). Overridable per-config via {@link ProjectConfig.backgroundSlowOpDelayMs}.
 */
export const BACKGROUND_SLOW_OP_DELAY_MS = 10 * 60 * 1000;

/**
 * Default "user recently interacted" recency window (ms): while the last interaction is
 * within this window, heavy background work is deferred so it never competes with an
 * actively typing user (parity: the reference's 1-minute
 * `getLastInteractionTime() > Date.now() - 1000*60` guard). Overridable per-config via
 * {@link ProjectConfig.backgroundInteractionRecencyMs}.
 */
export const BACKGROUND_INTERACTION_RECENCY_MS = 60 * 1000;

/**
 * Resolve the effective slow-op deferral delay (ms): the config override when it is a
 * finite non-negative number, else {@link BACKGROUND_SLOW_OP_DELAY_MS}.
 */
export function resolveBackgroundSlowOpDelayMs(config?: ProjectConfig | null): number {
  const v = config?.backgroundSlowOpDelayMs;
  return typeof v === 'number' && Number.isFinite(v) && v >= 0 ? v : BACKGROUND_SLOW_OP_DELAY_MS;
}

/**
 * Resolve the effective interaction-recency window (ms): the config override when it is a
 * finite non-negative number, else {@link BACKGROUND_INTERACTION_RECENCY_MS}.
 */
export function resolveBackgroundInteractionRecencyMs(config?: ProjectConfig | null): number {
  const v = config?.backgroundInteractionRecencyMs;
  return typeof v === 'number' && Number.isFinite(v) && v >= 0 ? v : BACKGROUND_INTERACTION_RECENCY_MS;
}

/**
 * Whether a slow background operation should be DEFERRED because the user interacted
 * recently — i.e. the session is interactive AND the last interaction is within the
 * recency window (parity: the reference's
 * `getIsInteractive() && getLastInteractionTime() > Date.now() - 1000*60` guard that
 * reschedules slow ops so they never compete with an actively typing user). A
 * non-interactive session, or an absent/invalid last-interaction timestamp, never defers.
 */
export function shouldDeferBackgroundWork(opts: {
  lastInteractionMs: number | null | undefined;
  interactive?: boolean;
  now?: number;
  recencyMs?: number;
}): boolean {
  if (opts.interactive === false) return false;
  const last = opts.lastInteractionMs;
  if (typeof last !== 'number' || !Number.isFinite(last)) return false;
  const now = typeof opts.now === 'number' ? opts.now : Date.now();
  const recencyMs =
    typeof opts.recencyMs === 'number' && Number.isFinite(opts.recencyMs) && opts.recencyMs >= 0
      ? opts.recencyMs
      : BACKGROUND_INTERACTION_RECENCY_MS;
  return last > now - recencyMs;
}

// ---------------------------------------------------------------------------
// Session-startup config snapshot (parity: teammateModeSnapshot.ts)
// ---------------------------------------------------------------------------

/**
 * The config frozen once at session startup. A running session reads session-stable knobs
 * from this snapshot so a mid-session config edit cannot change the running session's
 * behavior (parity: the reference's `captureTeammateModeSnapshot` / `getTeammateModeFromSnapshot`).
 * It is DELIBERATELY not invalidated by {@link resetConfigCache} — freezing is the point.
 */
let sessionConfigSnapshot: ProjectConfig | null = null;

/**
 * Freeze the session-stable config snapshot ONCE at startup from the (already
 * waterfall-merged, CLI-override-applied) config. A second call is a no-op that preserves
 * the first frozen value, so later runtime config edits never mutate the running session's
 * view (parity: the reference capturing the snapshot once). Use {@link repointConfigSnapshot}
 * to deliberately re-point it. Returns a defensive clone of the frozen snapshot.
 */
export function captureConfigSnapshot(config: ProjectConfig): ProjectConfig {
  if (sessionConfigSnapshot === null) {
    sessionConfigSnapshot = structuredClone(config);
  }
  return structuredClone(sessionConfigSnapshot);
}

/** Whether a session-startup config snapshot has been captured. */
export function hasConfigSnapshot(): boolean {
  return sessionConfigSnapshot !== null;
}

/**
 * The frozen session-startup config snapshot (a defensive clone), or `null` when none has
 * been captured yet.
 */
export function getConfigSnapshot(): ProjectConfig | null {
  return sessionConfigSnapshot === null ? null : structuredClone(sessionConfigSnapshot);
}

/**
 * Deliberately REPLACE the frozen snapshot with a new config — the entry point for a UI
 * setting change that must take effect mid-session. The new value is passed in directly
 * (not re-read) to avoid a read-after-write race (parity: the reference's
 * `clearCliTeammateModeOverride(newMode)` passing the new value directly). Returns a clone.
 */
export function repointConfigSnapshot(config: ProjectConfig): ProjectConfig {
  sessionConfigSnapshot = structuredClone(config);
  return structuredClone(sessionConfigSnapshot);
}

/** Drop the frozen snapshot (test isolation / explicit session teardown). */
export function clearConfigSnapshot(): void {
  sessionConfigSnapshot = null;
}

/**
 * Read a session-stable knob from the frozen snapshot. When no snapshot has been captured
 * yet, a defensive LAZY-CAPTURE branch logs an initialization-order warning and captures
 * from `liveConfig` before reading (parity: the reference's lazy-capture fallback that logs
 * an init bug then re-captures); with no `liveConfig` to fall back to it returns `undefined`.
 */
export function getSnapshotConfigValue<K extends keyof ProjectConfig>(
  key: K,
  liveConfig?: ProjectConfig | null,
): ProjectConfig[K] | undefined {
  if (sessionConfigSnapshot === null) {
    if (!liveConfig) return undefined;
    coreLog('config', 'config snapshot read before capture — lazily capturing (startup-order bug)');
    captureConfigSnapshot(liveConfig);
  }
  const snap = sessionConfigSnapshot;
  if (snap === null) return undefined;
  const v = snap[key];
  return v === undefined ? undefined : structuredClone(v);
}

/**
 * Recover or discard orphaned atomic-write temp files left beside `filePath` by a
 * crashed {@link safeWriteConfig}. Scans the containing directory for both the
 * PID-scoped `<base>.<pid>.tmp` scratch files this writer now produces AND any
 * legacy un-scoped `<base>.tmp` orphan.
 *
 * Crucially, a PID-scoped orphan whose owner is a DIFFERENT, still-live process is
 * an IN-FLIGHT write by a concurrent AMI instance — it is left untouched so this
 * instance's recovery sweep never clobbers another instance's pending save. Only
 * reclaimable orphans (a dead owner PID, this process's own leftover, or a legacy
 * un-scoped temp) are promoted (if they hold valid config) or deleted (if corrupt).
 *
 * Returns `true` when at least one matching temp was found (recovered, deleted, or
 * intentionally skipped as live), `false` when none exist.
 */
export function cleanupStaleConfigTmp(filePath: string): boolean {
  const dir = path.dirname(filePath);
  const baseName = path.basename(filePath);
  let entries: string[];
  try {
    entries = fsHooks.readdirSync(dir) as string[];
  } catch {
    return false;
  }
  const ownPid = runtimeInstancePid();
  let found = false;
  for (const entry of entries) {
    const ownerPid = parseInstanceTempPid(entry, baseName);
    if (ownerPid === undefined) continue; // not a temp of this file
    found = true;
    // A different, still-live owner is mid-write — never reclaim its scratch file.
    if (ownerPid !== null && ownerPid !== ownPid && isPidAlive(ownerPid)) continue;
    const tmpPath = path.join(dir, entry);
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
    } catch {
      try { fsHooks.unlinkSync(tmpPath); } catch { /* best effort */ }
    }
  }
  return found;
}
