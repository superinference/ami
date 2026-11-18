import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { log as coreLog } from './logger';
import { filterSettingsEnv, isHostManagedProvider, type EnvTrustTier, type EnvRejectReason } from './env-protection';
import { validateEngineConfig, mergeSkillUsageStat, type EngineConfig, type SkillUsageStat } from './types';
import { withFileLockSync, type FileLockSyncOptions } from './utils/file-lock';
import { treeify, type TreeNode, type TreeifyOptions } from './utils/treeify';
import { levenshteinDistance } from './tools/path-suggest';
import { DISABLE_BACKGROUND_TASKS_ENV, DISABLE_BACKGROUND_TASKS_ENV_COMPAT, featureFlagEnvName } from './tools/tool-gates';
import { TIER_LOCKED_KEYS } from './settings-guard';
import { readNativeManagedPolicy } from './managed-policy-source';

/**
 * One entry of the MCP allowlist ({@link ProjectConfig.allowedMcpServers}). A plain
 * string is a server NAME or upstream URL (with optional `*`-wildcards). A
 * provenance-scoped object trusts a server by the plugin that produced it rather
 * than its arbitrary local name (Claude-parity channelAllowlist): `{ pluginSource }`
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
  /**
   * Global default cap (chars) on a tool result before it is spilled to a file,
   * shared by tools that have no bespoke cap (web_fetch, PDF extraction, etc.).
   * Parity: Claude Code's single `DEFAULT_MAX_RESULT_SIZE_CHARS` in
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
   * Persisted per-project FOLDER-TRUST grant (parity: Claude Code's
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
   * Claude Code's per-project trust records resolved by `computeTrustDialogAccepted` /
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
   * cleanup (keep everything). Parity: Claude Code's `cleanupPeriodDays`.
   */
  cleanupPeriodDays?: number;
  /**
   * Persisted default model for spawned subagents/teammates, consulted ONLY when a
   * spawn names no model from a higher-priority source (operator env override →
   * per-call `model` → agent-definition model → explore fast default). A model id or
   * family alias forces that default; the `inherit` sentinel keeps the parent's exact
   * model ("follow the leader"); omitted falls through to the provider-aware hardcoded
   * fallback. Parity: Claude Code's `teammateDefaultModel` / getDefaultTeammateModel.
   */
  subagentDefaultModel?: string;
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
  /**
   * Per-surface plugin-only lock (Claude parity: `strictPluginOnlyCustomization`). `true`
   * locks ALL customization surfaces (`skills`/`agents`/`hooks`/`mcp`) to plugin+managed
   * sources; an array locks only the listed surfaces. A {@link MANAGED_ONLY_KEYS} entry —
   * only a managed/enterprise config tier can set it.
   */
  strictPluginOnlyCustomization?: boolean | ('skills' | 'agents' | 'hooks' | 'mcp')[];
  /**
   * MCP allowlist: when non-empty, only these MCP servers may be connected. An entry
   * is a server NAME or upstream URL (string, `*`-wildcards allowed) OR a
   * PROVENANCE-scoped entry (Claude-parity channelAllowlist): `{ pluginSource }` to
   * approve every server a given plugin bundle produced, or the Claude-shaped
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
   * managed deny). Parity: Claude Code's enterprise `deniedMcpServers` setting.
   */
  deniedMcpServers?: string[];
  /**
   * User-level per-server DISABLE toggle: servers named here are skipped at load
   * WITHOUT removing their config, so a server can be turned off and back on again.
   * Distinct from {@link deniedMcpServers} (an admin/enterprise policy): this is a
   * user convenience toggle. A name also present in {@link enabledMcpServers} is
   * re-enabled (the enable list wins per-name). Parity: Claude Code's
   * `disabledMcpServers`.
   */
  disabledMcpServers?: string[];
  /**
   * User-level per-server ENABLE override: re-enables a server that
   * {@link disabledMcpServers} turned off (matched by name, case-insensitive).
   * Parity: Claude Code's `enabledMcpServers`.
   */
  enabledMcpServers?: string[];
  /** Managed MCP server definitions pushed by enterprise policy. */
  managedMcpServers?: Record<string, unknown>;
  /**
   * Managed exclusive-control switch (parity: Claude Code's
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
   * Claude Code (`statusLine: { command, padding }`) parses unchanged instead of being
   * rejected as "must be a string". The object form's `padding` takes precedence over
   * the `SUPERINFERENCE_STATUSLINE_PADDING` env knob. Normalize both shapes with
   * {@link normalizeStatusLine}; the command auto-run trust gate still applies.
   * Parity: Claude Code's `settings.statusLine` object `{ type, command, padding }`.
   */
  statusLine?: string | StatusLineConfig;
  /** Bash commands exempted from the container sandbox (audit #4: settable + consumed,
   *  now also typed + merged across config layers). */
  sandboxExcludedCommands?: string[];
  /**
   * Managed sandbox-egress lockdown: when true, the container sandbox permits ONLY
   * managed/approved network domains — a project/local tier cannot widen egress. A
   * {@link MANAGED_ONLY_KEYS} entry (managed tier is the sole source); authoritative
   * in {@link mergeConfigs}. Parity: Claude Code's `shouldAllowManagedSandboxDomainsOnly`.
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
   * `tmux` (default false) opts a freshly-created worktree into a detached tmux
   * session rooted at the worktree (parity: Claude Code's `--tmux` worktree mode):
   * enter_worktree spawns the session and surfaces the `tmux attach -t <name>`
   * command, and exit_worktree tears it down on remove / reminds how to reattach on
   * keep. When ABSENT or false, no tmux session is created.
   */
  worktree?: { symlinkDirectories?: string[]; sparsePaths?: string[]; tmux?: boolean };
  /**
   * Persistent `web_fetch` domain policy (parity: Claude Code's preapproved /
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
   * Commit / PR attribution override (parity: Claude Code's `attribution.commit` /
   * `attribution.pr` settings). When `commit` is a non-empty string it REPLACES the
   * default `Co-Authored-By: AMI …` trailer the git_commit tool appends; when it is
   * the empty string `""` the trailer is suppressed entirely (same effect as the
   * `SUPERINFERENCE_DISABLE_CO_AUTHOR=1` env opt-out); when absent the default AMI
   * trailer is used. `pr` carries the analogous override text for PR-description
   * attribution. Consumed by {@link resolveCommitAttribution} in
   * core/core/src/tools/git-commit.ts.
   *
   * `auto` opts into repo-class-aware attribution (parity: Claude Code's
   * undercover auto-default): when `true` and no explicit `commit` override is
   * set, the default AMI trailer is appended ONLY when the repo's `origin` remote
   * is positively classified as owned (matches one of `ownRemotes`); on an
   * external/unknown/no-remote repo the trailer is suppressed (fail-safe-ON — the
   * strict side wins unless ownership is confirmed). `ownRemotes` holds normalized
   * remote substrings (e.g. `"github.com/myorg/"`) that mark a repo as owned.
   */
  attribution?: { commit?: string; pr?: string; auto?: boolean; ownRemotes?: string[] };
  /**
   * Persisted preferred RESPONSE language (parity: Claude Code's `LanguagePicker`
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
   * Persisted per-skill usage statistics keyed by skill name (parity: Claude Code's
   * global-config `skillUsage`). Each entry is a {@link SkillUsageStat}
   * (`{ usageCount, lastUsedAt }`), written to the GLOBAL config on each skill
   * invocation via {@link persistSkillUsageStat} and read back at session start so a
   * skill's frecency ranking survives across sessions instead of resetting to zero on
   * every launch. A user-level, cross-project signal (lives in global config only).
   */
  skillUsage?: Record<string, SkillUsageStat>;
  /**
   * Persisted one-time preference / dialog "already shown" flags, keyed by a stable
   * preference name (parity: Claude Code's GlobalConfig one-shot knobs such as
   * `hasIdeAutoConnectDialogBeenShown` / `hasUsedBackgroundTask` / the bypass-permissions
   * acknowledgment). Each entry records that a one-time prompt/notice was surfaced so it
   * is never re-shown. Written to the GLOBAL config via {@link markPreferenceShown} and
   * read via {@link wasPreferenceShown}; internal, non-user-facing state (lives in global
   * config only, like {@link skillUsage}).
   */
  preferencesShown?: Record<string, boolean>;
  /**
   * Persisted seen-COUNTS for count-limited notices, keyed by a stable notice name
   * (parity: Claude Code's "show at most MAX_SHOW_COUNT times" persisted seen-count
   * knob). A notice is shown while its count is below its cap; {@link recordNoticeSeen}
   * increments the count (capped) via the lock-safe updater and {@link shouldShowNotice}
   * gates on it. Internal, non-user-facing state (global config only).
   */
  noticeSeenCounts?: Record<string, number>;
  /**
   * Persistent registry of known LOCAL clone paths for a GitHub repository, keyed by
   * the lowercased `owner/repo` (parity: Claude Code's global-config `githubRepoPaths`).
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
   * Claude Code's `claudeMdExcludes` setting / `isClaudeMdExcluded`). A user can opt
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
   * (parity: Claude Code's `hasClaudeMdExternalIncludesApproved`,
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
  // the config files. Parity: Claude Code's settings-schema keys that flow through the
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
  /** Toggle file checkpointing/mutation snapshots (parity: Claude's fileCheckpointingEnabled). */
  fileCheckpointingEnabled?: boolean;
  /** Capture a V8 heap snapshot the first time process RSS crosses the memory-warning
   *  high-water mark, instead of only logging (parity: Claude's heapDumpService auto-1.5GB). */
  heapDumpOnMemoryWarning?: boolean;
  /**
   * Asymmetric positive-only cache of machine-local capability detections keyed by
   * name (e.g. `dockerAvailable`). Only `true` values are ever persisted — a
   * negative is never written, so a config shared across machines can't be poisoned
   * by a machine that lacks the capability (parity: Claude Code's
   * `cachedChromeExtensionInstalled`). Written/read via reference-journal's
   * `persistPositiveCapability` / `readCachedCapability`.
   */
  cachedCapabilities?: Record<string, boolean>;
  /**
   * Named capability feature-flags keyed by feature name (parity: Claude Code's
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
   * (parity: Claude Code's `includeGitInstructions`). Defaults ON when unset; the
   * `SUPERINFERENCE_DISABLE_GIT_INSTRUCTIONS` env var overrides this knob.
   */
  includeGitInstructions?: boolean;
  /**
   * Suppress proactive plugin-installation hints (parity: Claude Code's
   * `disableHintRecommendations` preference). When `true`, the `/plugin recommend`
   * flow withholds the prompt. Toggled via `/plugin hints on|off`.
   */
  pluginHintsDisabled?: boolean;
  /**
   * Path to an additional CA-certificate bundle (PEM) applied to
   * `NODE_EXTRA_CA_CERTS` at startup — BEFORE any TLS connection — so Node picks up
   * a corporate-proxy / TLS-inspecting CA without the user having to export the env
   * var before every launch (parity: Claude Code's
   * `caCertsConfig.applyExtraCACertsFromConfig`). An explicit `NODE_EXTRA_CA_CERTS`
   * already in the environment WINS (one-shot override); otherwise this value seeds
   * it. The referenced bundle must be an existing, readable regular file — a
   * missing/unreadable path is ignored (fail-safe: never point Node at a bad CA
   * bundle). Applied by {@link applyConfigCaCerts}.
   */
  extraCaCerts?: string;
  /**
   * Extra filesystem directories granted to the tool-access workspace from the
   * first turn (parity: Claude Code's `permissions.additionalDirectories`). Unioned
   * with the remembered `/add-dir` store into the PermissionManager allowed read
   * roots at engine setup. Relative entries resolve against the project root.
   * Patterns ACCUMULATE across config layers (a higher layer adds more dirs, never
   * removes a lower layer's). Resolved by {@link resolveAdditionalDirectories}.
   */
  additionalDirectories?: string[];
  /**
   * When `true`, file globbing honors the project's `.gitignore` (nested +
   * `.git/info/exclude`); when `false` it ignores it (parity: Claude Code's
   * `respectGitignore` file-picker toggle). Bridged to the existing
   * `AMI_GLOB_NO_IGNORE` env knob by {@link applyConfigGitignore}; an explicit env
   * value WINS. Unset keeps AMI's default (glob does NOT filter through gitignore).
   */
  respectGitignore?: boolean;
  /**
   * When `false`, automatic context compaction is disabled (parity: Claude Code's
   * persisted `autoCompactEnabled` toggle). Bridged to the existing
   * `SUPERINFERENCE_DISABLE_AUTO_COMPACT` env knob by {@link applyConfigAutoCompact};
   * an explicit env value WINS. Unset/`true` keeps auto-compaction on (the default).
   */
  autoCompactEnabled?: boolean;
  /**
   * Custom directory for plan-mode `.md` files (parity: Claude Code's
   * `plansDirectory`). Resolved RELATIVE to the project root and validated to stay
   * within it by the plan-mode tool. Bridged to the existing `AMI_PLANS_DIR` env
   * knob by {@link applyConfigPlansDirectory}; an explicit env value WINS.
   */
  plansDirectory?: string;
  /**
   * Opt into the Anthropic `fast-mode` beta (parity: Claude Code's `fast-mode`
   * beta knob, token `fast-mode-2026-02-01`). When `true` the token is appended to
   * the `anthropic-beta` header via the `ANTHROPIC_BETAS` escape hatch (consumed by
   * `getExtraAnthropicBetas`). Applied by {@link applyConfigAnthropicBetas}.
   */
  fastMode?: boolean;
  /**
   * Opt into the Anthropic `redact-thinking` beta (parity: Claude Code's
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
  // bug, newly-found keys. Parity: Claude Code's settings-schema keys flowing through
  // the settings cascade into the running session.
  // -------------------------------------------------------------------------
  /**
   * Gate the eager background session-memory extraction pass (parity: Claude Code's
   * `autoMemoryEnabled` settings key). Read by `engine-factory` {@link resolveAutoMemory};
   * an explicit `false` forces consolidation OFF regardless of the `AMI_DISABLE_AUTO_MEMORY`
   * env/default. Unset defers to the env + default-on behavior.
   */
  autoMemoryEnabled?: boolean;
  /**
   * Alias of {@link autoMemoryEnabled} gating background memory consolidation (parity:
   * Claude Code's `autoDreamEnabled` settings key). `engine-factory`
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
   * Parity: Claude Code gates team memory behind an enable flag
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
   * parity: Claude Code's worktree-mode toggle `isWorktreeModeEnabled`). Config tier of
   * the precedence waterfall (opt flag > `AMI_WORKTREE_TOOLS` env > this config boolean >
   * default-on). A locked-down/CI config can drop them as a unit.
   */
  worktreeToolsEnabled?: boolean;
  /**
   * Off-switch for the next-prompt SPECULATION feature (parity: Claude Code's
   * `speculationEnabled` global-config knob + `isSpeculationEnabled()` gate). Speculation
   * defaults ON; an explicit `false` disables it. Resolved per workspace by
   * {@link resolveSpeculationEnabled} across the full config waterfall and fed to the
   * cli `resolvePromptSuggestionEnabled` waterfall (env override > non-interactive >
   * this setting) and the `isSpeculationEnabled({ enabled })` gate. Unset keeps the
   * default-on behavior, so the knob is a pure superset off-switch.
   */
  speculationEnabled?: boolean;
  /**
   * Opt-OUT of enabling model-default thinking at session start (parity: Claude Code's
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
   * Persisted auto-connect-to-IDE toggle (parity: Claude Code's `autoConnectIde`
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
   * Claude Code's `terminalProgressBarEnabled` settings key). AMI emits progress by
   * default (its long-standing behavior); an explicit `false` suppresses it by seeding
   * the `AMI_NO_PROGRESS` kill-switch via {@link applyConfigTerminalProgress}, so the
   * persisted preference no longer requires exporting the env var. An explicit
   * `AMI_NO_PROGRESS` env value still WINS. Unset/`true` keeps AMI's default-on
   * (superset divergence from Claude's default-off-unless-set — AMI never regresses the
   * loved default, only ADDs the opt-out knob).
   */
  terminalProgressBarEnabled?: boolean;
  /**
   * Show the per-turn duration on the completion line ("✳ Worked FOR 1m 3s · done …")
   * (parity: Claude Code's `showTurnDuration` settings key). AMI shows it by default;
   * an explicit `false` suppresses ONLY the duration (keeping the verb/wall-clock) by
   * seeding `AMI_SHOW_TURN_DURATION` via {@link applyConfigTurnDuration}, so the choice
   * can be persisted instead of exporting the env var. An explicit `AMI_SHOW_TURN_DURATION`
   * env value still WINS. Unset/`true` keeps the default-on.
   */
  showTurnDuration?: boolean;
  /**
   * Persisted verbose output-detail toggle (parity: Claude Code's global-config
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
   * Terminal selection auto-copy toggle (parity: Claude Code's global-config
   * `copyOnSelect ?? true`): when a drag-select completes, the highlighted text is
   * written to the clipboard (and the "ctrl+c copy" hint is surfaced). Defaults ON;
   * an explicit `false` disables the drag-release clipboard write by seeding the
   * existing `AMI_NO_COPY_ON_SELECT` opt-out that the cli `copyOnSelectEnabled` already
   * reads, so the choice can be persisted instead of exporting the env var. An explicit
   * `AMI_NO_COPY_ON_SELECT` env value still WINS. Bridged by {@link applyConfigCopyOnSelect}.
   */
  copyOnSelect?: boolean;
  /**
   * Persisted "always copy the full response" preference for `/copy` (parity: Claude
   * Code's global-config `copyFullResponse` — commands/copy/copy.tsx L89-L92,L356-L369).
   * When `true`, `/copy --list` bypasses the code-block picker menu entirely and copies
   * the full reply directly (the picker's `always` opt-in, surfaced in AMI as
   * `/copy --always`, persists this knob). Unset/`false` keeps the interactive
   * `--list`/`--block` block-selection flow. Settable via `/config set copyFullResponse`.
   */
  copyFullResponse?: boolean;
  /**
   * Interactive-TUI layout mode (parity: Claude Code's `isFullscreenEnvEnabled()`
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
   * Disable syntax highlighting of fenced code blocks and diff lines (parity: Claude
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
   * Claude Code's numeric `messageIdleNotifThresholdMs`, default 60000). AMI already has
   * desktop notifications with an `AMI_DESKTOP_NOTIFY_THRESHOLD_MS` env knob (cli
   * `getNotifyThreshold`); this persists the threshold in the settings waterfall instead
   * of requiring the env var. A positive finite value seeds the env knob via
   * {@link applyConfigIdleNotifThreshold}; an explicit `AMI_DESKTOP_NOTIFY_THRESHOLD_MS`
   * env value still WINS. A non-positive/non-finite value is ignored (keeps the default).
   */
  messageIdleNotifThresholdMs?: number;
  /**
   * Show model thinking summaries (parity: Claude Code's `showThinkingSummaries`
   * settings knob, default off). When explicitly `false` (summaries OFF), the
   * `redact-thinking` Anthropic beta token is appended to the `anthropic-beta` header
   * (via the same `ANTHROPIC_BETAS` escape hatch as {@link redactThinking}) — trading
   * thinking-summary display for lower latency. The inverse of {@link redactThinking}:
   * either `showThinkingSummaries === false` OR `redactThinking === true` arms the beta.
   * Applied by {@link applyConfigAnthropicBetas}. Unset/`true` is a no-op (summaries on).
   */
  showThinkingSummaries?: boolean;
  /**
   * Customize the playful working-spinner gerund pool (parity: Claude Code's cosmetic
   * `spinnerVerbs` config knob). `mode: 'append'` (default) widens the built-in pool
   * with `verbs`; `mode: 'replace'` swaps it entirely (an empty list falls back to the
   * built-ins). Serialized to the existing `AMI_SPINNER_VERBS` JSON env knob the cli
   * `resolveSpinnerVerbConfig` already reads, by {@link applyConfigSpinnerVerbs}, so the
   * override can be persisted in the settings waterfall instead of exporting the env var.
   * An explicit `AMI_SPINNER_VERBS` env value still WINS.
   */
  spinnerVerbs?: { mode?: 'append' | 'replace'; verbs?: string[] };
  /**
   * Persisted toggle for background-task functionality (parity: Claude Code's
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
  // actually honored. Parity: Claude Code's policyLimits / pluginTrustMessage /
  // marketplace-policy settings flowing through the settings cascade.
  // -------------------------------------------------------------------------
  /**
   * Generic admin/enterprise feature-restriction map (parity: Claude Code's
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
   * (parity: Claude Code's `getPluginTrustMessage` / managed `pluginTrustMessage`
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
   * CRITICAL and sheds load (parity: Claude Code's critical-memory RSS threshold).
   * Read by `common/src/engine-factory.ts` off the merged project config
   * (`resolveEngineConfig` → `EngineConfig.memoryCriticalRssBytes`), so
   * {@link mergeConfigs} must copy it; a non-positive value is ignored by the
   * resolver. Unset keeps the engine default.
   */
  memoryCriticalRssBytes?: number;
  /**
   * Prefer a tmux teammate backend over iTerm2 panes when both are available
   * (parity: Claude Code's terminal-multiplexer preference). Read by
   * `common/src/engine-factory.ts` off the merged project config to resolve the
   * teammate execution surface; {@link mergeConfigs} must copy it. Unset/`false`
   * keeps the default auto-selection.
   */
  preferTmuxOverIterm2?: boolean;
  /**
   * Managed/enterprise policy-refresh tuning block (parity: Claude Code's remote
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
   * Enterprise marketplace host BLOCKLIST (parity: Claude Code's `blockedMarketplaces`
   * managed setting). Hosts named here are refused as plugin-marketplace sources
   * (deny-wins — the union across the managed + global tiers is enforced). Read by
   * `common/src/plugin-commands.ts` `sourceBlockedMarketplaces` (managed + global raw
   * config). Declaring it here makes it recognized + value-validated; entries
   * ACCUMULATE across layers (a higher layer adds more blocked hosts, never removes a
   * lower layer's).
   */
  blockedMarketplaces?: string[];
  /**
   * Enterprise marketplace host ALLOWLIST (parity: Claude Code's
   * `strictKnownMarketplaces` managed setting). When configured, ONLY these hosts may
   * be added as plugin-marketplace sources. Read by `common/src/plugin-commands.ts`
   * `sourceStrictKnownMarketplaces` (managed + global raw config). Declaring it here
   * makes it recognized + value-validated; entries ACCUMULATE across layers.
   */
  strictKnownMarketplaces?: string[];
  /**
   * Gate the extra "clear context" affordance offered when the user ACCEPTS a plan
   * (exit-plan-mode approval). Parity: Claude Code's `showClearContextOnPlanAccept`
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
   * line (`common/src/pr-status.ts`). Parity: Claude Code's `prStatusFooterEnabled ??
   * true` (`PromptInputFooterLeftSide.tsx` `isPrStatusEnabled`). Default ON; set `false`
   * to suppress the PR footer entirely. Resolved by {@link isPrStatusFooterEnabled} off
   * the MERGED config and consumed by the footer renderer in `cli/`. A merge knob.
   */
  prStatusFooterEnabled?: boolean;
  /**
   * The release-notes/changelog VERSION the user has already seen, persisted in the
   * GLOBAL config so the one-time "what's new" banner is shown at most once per release.
   * Parity: Claude Code's `lastReleaseNotesSeen` (`LogoV2.tsx` gates the banner on
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
   * by the update path rather than flagged as an unknown key. Parity: Claude Code's
   * `autoUpdatesChannel` setting.
   */
  autoUpdatesChannel?: 'latest' | 'stable';
  /**
   * Minimum acceptable version the launcher must satisfy before running (a managed/
   * enterprise floor; a below-floor install is upgraded/blocked by the update path).
   * A merged config knob, recognized + base-type validated here. Parity: Claude Code's
   * `minimumVersion` policy setting paired with {@link autoUpdatesChannel}.
   */
  minimumVersion?: string;
  /**
   * Opt-IN flag for voice-input mode (parity: Claude Code's `voiceEnabled`
   * settings boolean gating `isVoiceModeEnabled()` and the advertised `/voice`
   * command). Default OFF — voice mode is additive/opt-in, so only an explicit
   * `true` (at any config tier) enables it. Resolved by {@link isVoiceModeEnabled}
   * off the MERGED config; a merge knob (any tier may set it).
   */
  voiceEnabled?: boolean;
  /**
   * Opt-IN declaration that the user WANTS the auto-allow ("auto"/bypass) permission
   * mode (parity: Claude Code's `hasAutoModeOptIn()` settings flag). AMI's permission
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
   * Claude Code's `diffTool` global-config knob (`'terminal' | 'auto'`, default `'auto'`).
   * A merge knob resolved off the merged config by {@link resolveDiffTool}; default
   * `'auto'` so an unset config keeps AMI's existing behavior.
   */
  diffTool?: 'terminal' | 'auto';
  /**
   * Cached list of frequently-modified project files used to seed example-command
   * suggestions (parity: Claude Code's per-project `exampleFiles`). Populated out-of-band
   * by a background scan; read (with a staleness check against {@link exampleFilesGeneratedAt})
   * by {@link getCachedExampleFiles}. A per-project STATE field, not a cross-layer policy knob.
   */
  exampleFiles?: string[];
  /**
   * Epoch-millis timestamp of when {@link exampleFiles} was last generated (parity:
   * Claude Code's `exampleFilesGeneratedAt`). {@link getCachedExampleFiles} treats a
   * cache older than {@link EXAMPLE_FILES_TTL_MS} as stale and returns none so the
   * caller refreshes. A per-project STATE field.
   */
  exampleFilesGeneratedAt?: number;
  /**
   * Default shell backing input-box `!` commands: `'bash'` (default on every platform)
   * or `'powershell'`. Parity: Claude Code's `defaultShell` setting + `resolveDefaultShell()`.
   * AMI does NOT auto-flip Windows to PowerShell (that would break existing bash-hook
   * users), so the default is `'bash'` everywhere. Resolved by {@link resolveDefaultShell}
   * off the merged config; a merge knob (any tier may set it).
   */
  defaultShell?: 'bash' | 'powershell';
  /**
   * Startup announcement strings shown once at launch (parity: Claude Code's managed
   * `companyAnnouncements`). Typically set by an enterprise/managed config. On the FIRST
   * startup the first entry is shown; afterwards one is chosen at random. Resolved by
   * {@link pickCompanyAnnouncement} off the merged config.
   */
  companyAnnouncements?: string[];
  /**
   * Policy knob gating whether a `dangerouslyDisableSandbox` tool override is honored.
   * Parity: Claude Code's `SandboxManager.areUnsandboxedCommandsAllowed()` consulted by
   * `shouldUseSandbox()` — the sandbox is skipped ONLY when the command opts out AND this
   * policy permits unsandboxed commands. Lets an operator HARD-DISABLE the override
   * independent of permission mode: `false` (at any tier, e.g. a managed config) forces
   * every command through the sandbox even when it requests `dangerouslyDisableSandbox`.
   * Default (unset) → `true` (override honored), preserving AMI's existing behavior.
   * Resolved by {@link areUnsandboxedCommandsAllowed} off the merged config.
   */
  allowUnsandboxedCommands?: boolean;
}

/**
 * Object form of {@link ProjectConfig.statusLine} (Claude-Code-compatible
 * `settings.statusLine`): the shell `command` whose stdout is rendered, plus an
 * optional horizontal `padding` (spaces per side, CC's `<Box paddingX>`). The bare
 * string form of `statusLine` is equivalent to `{ command }` with no padding.
 *
 * `type` is Claude Code's status-line TYPE discriminator (`{ "type": "command",
 * "command": "..." }`): the ONLY shape CC's own `statuslineSetup` tool writes. AMI
 * renders the `command` directly, so `type` is accepted-and-ignored (forward-compat —
 * an unrecognized future type never rejects the block), which makes a `settings.json`
 * copied verbatim from Claude Code parse unchanged rather than being lost. Declaring it
 * here (rather than letting it fall through as an untyped extra key) documents the
 * supported CC drop-in shape and keeps the typed contract honest.
 */
export interface StatusLineConfig {
  /** Shell command whose stdout is rendered as the status line. */
  command: string;
  /** Horizontal padding (spaces per side) applied to each rendered line. */
  padding?: number;
  /**
   * Claude-Code status-line kind discriminator (CC only ever emits `'command'`).
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
 * the env/default fallback applies). Parity: Claude Code accepting `statusLine` as
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
// Config read caches + single invalidation point
// ---------------------------------------------------------------------------
//
// Parity: Claude Code's settings cache (settingsCache.ts) is a set of memos behind
// ONE resetSettingsCache() trigger fired on every settings write / --add-dir /
// plugin-init / hooks-refresh. AMI's config layer likewise accreted several memos
// (the content-keyed parseJsonc LRU above; the managed-path platform memo and the
// per-tier identity cache below). {@link resetConfigCache} ties them to a SINGLE
// invalidation point wired into {@link safeWriteConfig} so a config write never
// leaves a stale read snapshot behind.

/**
 * Platform(+PROGRAMDATA)-keyed memo of the managed-config DEVICE-DEFAULT path, so
 * the platform switch + join runs once per platform instead of on every resolve
 * (parity: Claude's lodash-memoized `getManagedFilePath`). Keyed by platform (and, on
 * Windows, the PROGRAMDATA that feeds the path) rather than memoized unconditionally,
 * so a forced different `process.platform` / `PROGRAMDATA` still resolves correctly.
 */
const managedPathCache = new Map<string, string>();

/**
 * One in-memory per-tier config read cache keyed by file IDENTITY (absolute path +
 * mtime + size), with NEGATIVE caching of absent files. A repeated load of an
 * unchanged file is a pure memory hit (no readFileSync); any content change — our own
 * {@link safeWriteConfig} OR another instance's write — changes mtime/size, misses,
 * and re-reads, so a stale snapshot is never served. The cached tree is pristine and
 * every caller receives a `structuredClone`, so a caller mutating its config cannot
 * poison the cache. Parity: Claude's in-memory global-config cache + settingsCache's
 * miss(undefined)/negative(null) distinction.
 */
interface TierConfigCacheEntry {
  /** File mtime in ms; `-1` when the file is absent (negative-cache marker). */
  mtimeMs: number;
  /** File size in bytes; `-1` when absent. */
  size: number;
  /** Parsed config, or `null` for "no config at this path" (absent/corrupt). */
  config: ProjectConfig | null;
}
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
 * point (parity: Claude's `resetSettingsCache`) fired by {@link safeWriteConfig} on
 * every config write; call it too after a `--add-dir`, plugin install, or hooks
 * refresh changes what the config layer would resolve.
 */
export function resetConfigCache(): void {
  jsoncParseCache.clear();
  managedPathCache.clear();
  tierConfigCache.clear();
  clearManagedConfigCache();
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
  return loadTierConfigCached(configPath);
}

/**
 * Load a local config from `<cwd>/.ami/config.local.json`.
 * This file is typically gitignored for user-specific overrides.
 */
export function loadLocalConfig(cwd: string): ProjectConfig | null {
  return loadTierConfigCached(path.join(cwd, '.ami', 'config.local.json'));
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
 * The managed-config base file path. An explicit `SUPERINFERENCE_MANAGED_CONFIG`
 * env var always wins (testing / non-standard deployments); otherwise the default
 * is PLATFORM-SPECIFIC so an enterprise policy lands in the OS-conventional
 * machine-wide location an administrator actually writes to — mirroring Claude
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
 * per-user one win — mirroring Claude Code's `getMacOSPlistPaths()` per-user >
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
 *     yields the device-level candidates. Parity: Claude's per-user > device plist order.
 *   - Windows / Linux → the single {@link getManagedConfigPath} default (ProgramData /
 *     `/etc`); there is no second native tier to layer, so the list has one entry.
 *
 * {@link getManagedConfigPath} (used for the drop-in dir, the managed `permissions.json`
 * location, and diagnostics) is unchanged — it remains the device-level default and is
 * always the LAST candidate, so this function is a pure additive superset.
 */
export function getManagedConfigPaths(): string[] {
  const override = process.env.SUPERINFERENCE_MANAGED_CONFIG;
  if (override) return [override];
  const deviceDefault = getManagedConfigPath();
  if (process.platform !== 'darwin') return [deviceDefault];
  const candidates: string[] = [];
  try {
    const username = os.userInfo().username;
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
 * one monolithic file (parity: Claude's managed-settings.d/ drop-ins).
 */
export function getManagedDropInDir(): string {
  return path.join(path.dirname(getManagedConfigPath()), 'config.d');
}

/**
 * Decide whether a managed `config.d/` directory entry is eligible to be read as a
 * policy fragment. Parity: Claude Code's managed-settings.d enumeration guard, which
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
 * while keeping Claude's enumeration shape. When the stat seam cannot report a type (a
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
 * AMI-only env override to an admin-grade trust boundary (Claude Code's managed path is a
 * FIXED admin-only location the user cannot relocate) while keeping the override usable
 * behind an explicit, auditable seam.
 */
export const MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV = 'SUPERINFERENCE_MANAGED_CONFIG_ALLOW_UNTRUSTED';

/** Whether the {@link MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV} opt-out seam is engaged. */
function managedOverrideUntrustedAllowed(env: NodeJS.ProcessEnv = process.env): boolean {
  const v = env[MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV];
  return v === '1' || v === 'true';
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
 * principal controls — the fixed admin-only trust boundary Claude Code relies on.
 *
 * Windows (no `process.getuid`) carries no meaningful POSIX mode bits, so only the
 * symlink check applies there.
 */
function assertTrustedManagedFile(configPath: string, relocatable = false): void {
  let stats: fs.Stats;
  try {
    stats = fsHooks.lstatSync(configPath);
  } catch (err) {
    throw new ManagedConfigError(`Managed config at ${configPath} could not be inspected: ${err instanceof Error ? err.message : String(err)}`);
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

/**
 * One-entry memo of the merged managed-config RESULT keyed by a cheap file-identity
 * SIGNATURE over every managed source (parity: Claude Code's `mdmCache` behind the
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
 *    MDM/registry policy change), exactly as Claude Code clears `mdmCache` on a write.
 *
 * SAFE under the fs-mocked managed test suites: computing the signature requires a real
 * {@link fsHooks.statSync} on each PRESENT source; when that stat cannot be taken (the
 * mock seams stub `existsSync`/`readFileSync` but not `statSync`, so a fake path stats
 * as absent/throws) the call is treated as UNCACHEABLE — it reads fresh and stores
 * nothing — so a mocked scenario never serves a cached result from another `it`.
 */
interface ManagedConfigCacheEntry {
  /** File-identity signature of every managed source at the time of the read. */
  signature: string;
  /** The merged managed config (or `null` for "no managed policy present"). */
  config: ProjectConfig | null;
}
let managedConfigCache: ManagedConfigCacheEntry | null = null;

/**
 * Drop the memoized managed-config result so the next {@link loadManagedConfig} re-reads
 * from disk + OS-native policy. Fanned out from {@link resetConfigCache} on every config
 * write; the AMI analog of Claude Code's `clearMdmSettingsCache`.
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
  // resolution instead of reading exactly one fixed path. Parity: Claude's
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
  // exist, the device default is the base (merged stays null). Parity: Claude Code's
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
    let dropIn: ProjectConfig;
    try {
      dropIn = parseManagedFile(dropInPath, relocatable);
    } catch (err) {
      // Per-file fault isolation for the drop-in DIRECTORY only: one malformed,
      // unreadable, or untrusted `config.d/*.json` entry is SKIPPED (logged) so it
      // cannot take down the whole managed policy — the authoritative base file and
      // the remaining well-formed drop-ins still apply. The BASE managed file above
      // still fails closed (it IS the policy; a broken one must not be silently
      // ignored). Parity: Claude Code's managed-settings.d per-file try/catch skip.
      coreLog('config', `skipping invalid managed drop-in ${dropInPath}: ${err instanceof Error ? err.message : String(err)}`);
      continue;
    }
    merged = merged === null ? dropIn : mergeManagedLayer(merged, dropIn);
  }
  // OS-NATIVE managed policy (macOS configuration profile / Windows Group Policy) is
  // the strongest trust anchor — an admin-only OS store (/Library/Managed Preferences
  // plist, HKLM\SOFTWARE\Policies) an end user cannot redirect. When present it wins
  // over the file-based managed layer (merged LAST). The read is timeout-bounded and
  // never-throwing (see managed-policy-source.ts), so a broken policy source can never
  // stall or fail the loader; a malformed/non-object payload is simply ignored. Each
  // valid field is value-SALVAGED like a file-based managed source. Parity: Claude's
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
  // so a managed deployment is diagnosable. Parity: Claude's "MDM settings found: …".
  if (merged !== null) {
    // Introspection: name the ACTIVE path, the contributing key count (no values —
    // the policy may be sensitive), the drop-in count, AND the elapsed read time so a
    // slow managed source (e.g. an NFS mount) is diagnosable. Parity: Claude's
    // "MDM settings load completed in <N>ms" + "MDM settings found: …" breadcrumbs.
    coreLog('config', `loaded managed config ${configPath} (${Object.keys(merged).length} key(s)${entries.length > 0 ? `, ${entries.length} drop-in(s)` : ''}) in ${Date.now() - started}ms`);
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
  /**
   * A documentation URL for THIS field so a diagnostic can render an actionable
   * "Learn more: <url>" affordance, turning a bare validation error into a pointer
   * to the fix (parity: Claude Code's `ValidationErrorsList` per-error `docLink` +
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
  /**
   * A copy-pasteable shell command that restores the most recent known-GOOD
   * pre-write snapshot (`<file>.backup-<ms>`) over the damaged file, e.g.
   * `cp "<backup>" "<file>"`. Present only on a fatal `parse-error`/`not-object`
   * diagnostic when such a good backup exists, so the user is told HOW to recover
   * rather than only that the file is broken. Parity: Claude Code's
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
   * when the broken file sets no recognized key. Parity: Claude Code's
   * `rawSettingsContainsKey` user-intent detection + the InvalidSettingsDialog
   * "files with errors are skipped entirely" warning.
   */
  intendedKeys?: string[];
}

/** The ProjectConfig enum fields and their allowed values. */
const PROJECT_CONFIG_ENUMS: ReadonlyArray<readonly [keyof ProjectConfig, readonly string[]]> = [
  ['permissionMode', ['ask', 'auto-allow', 'deny-all']],
  ['cacheTtl', ['5m', '1h']],
  ['autoUpdatesChannel', ['latest', 'stable']],
  // Diff renderer + input-box shell selectors (see ProjectConfig) resolved off the merged
  // config by resolveDiffTool / resolveDefaultShell — enum-validated so a bad value is
  // surfaced instead of silently flowing through, and emitted in the JSON schema.
  ['diffTool', ['terminal', 'auto']],
  ['defaultShell', ['bash', 'powershell']],
];

/** The expected runtime base-type of each typed ProjectConfig field. */
type ConfigFieldType = 'string' | 'number' | 'boolean' | 'string[]' | 'object' | 'mcpAllowlist';

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
  ['compactionModel', 'string'], ['skill', 'string'],
  ['language', 'string'], ['subagentDefaultModel', 'string'],
  ['maxTurns', 'number'], ['tokenBudget', 'number'], ['maxBudgetUsd', 'number'],
  ['bashOutputMaxChars', 'number'], ['taskOutputMaxChars', 'number'],
  ['toolOutputMaxChars', 'number'],
  ['maxWebSearchesPerSession', 'number'], ['migrationVersion', 'number'],
  ['cleanupPeriodDays', 'number'],
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
  // enum in PROJECT_CONFIG_ENUMS) + emitted in the JSON schema. Parity: Claude's
  // autoUpdatesChannel / minimumVersion settings.
  ['minimumVersion', 'string'],
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
  // Fields with a union/custom shape not expressible in the single-type table above are
  // validated bespoke in {@link validateProjectConfigValues} but must still count as
  // recognized keys so they never surface as a spurious unknown-key typo warning.
  'statusLine',
  'strictPluginOnlyCustomization',
  // Editor-only JSON-Schema reference key (parity: Claude Code's `$schema` support). A
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
 * loads — unknown keys are ignored, never fatal). Parity: Claude Code surfaces
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
 * Parity: Claude Code's `rawSettingsContainsKey` (detect user intent even when settings
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
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
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
    case 'mcpAllowlist': return Array.isArray(value) && value.every(isMcpAllowedServerEntry);
  }
}

/**
 * Human-readable phrase (including its article) for a {@link ConfigFieldType}, used
 * in a type-mismatch message (`must be <label> (got <actual>)`). The base scalar/array
 * types keep their original wording; the richer `mcpAllowlist` union renders a
 * descriptive phrase rather than the opaque internal tag.
 */
function configFieldTypeLabel(type: ConfigFieldType): string {
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
function enumValuesPrefix(allowed: readonly string[]): string {
  return `Valid values: ${allowed.map(v => `"${v}"`).join(', ')}.`;
}

/**
 * Named-field, semantics-aware validation tips (parity: Claude Code's field-matched
 * `getValidationTip`). Unlike the generic per-kind tips — which describe the expected
 * SHAPE ("must be a number", "Valid values: …") — these describe what the value MEANS,
 * so a user who typed a valid-shaped-but-wrong value understands the fix. Returns
 * `undefined` for any field without a specific override, so the generic fallback still
 * applies everywhere else (purely additive). Enum overrides reuse {@link enumValuesPrefix}
 * so the `Valid values: ….` prefix is preserved verbatim.
 */
function fieldSpecificConfigTip(field: string, ctx: ConfigTipKind): string | undefined {
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
  // Named-field overrides that explain the SEMANTICS of the value (not just its shape),
  // mirroring Claude Code's field-matched getValidationTip (e.g. what each permission
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
 * render an actionable "Learn more: <url>" affordance (parity: Claude Code's
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
 * doc links (rather than repeating one per issue) mirrors Claude Code's
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
 * Claude Code's `ValidationErrorsList`, which groups settings errors by file and renders
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
// JSON Schema emission for the PROJECT config file (parity: Claude Code's
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
 * Canonical published URL of the AMI config JSON Schema (parity: Claude Code's
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

/** Map a {@link ConfigFieldType} to its JSON-Schema property node (base shape only). */
function configFieldTypeToSchemaNode(type: ConfigFieldType): ConfigSchemaProperty {
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
 * Generate a (draft-07) JSON Schema describing EVERY recognized {@link ProjectConfig}
 * knob, derived from the authoritative {@link PROJECT_CONFIG_FIELD_TYPES} /
 * {@link PROJECT_CONFIG_ENUMS} tables (plus the bespoke union keys in
 * {@link KNOWN_CONFIG_KEYS}) so it self-updates and never drops a key. Enum fields carry
 * their allowed values; union/custom-shape keys (`statusLine`,
 * `strictPluginOnlyCustomization`) are left permissive (no `type`) so a valid value of
 * either shape validates. Non-strict (`additionalProperties: true`) — unknown keys still
 * load (AMI never fails closed on an unrecognized config key; it warns). Parity: Claude
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
  for (const field of ['maxTurns', 'tokenBudget', 'maxBudgetUsd', 'maxWebSearchesPerSession', 'bashOutputMaxChars', 'taskOutputMaxChars'] as const) {
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
  //     was already flagged (no double-report). Parity: Claude's cleanupPeriodDays
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
  //    Parity: Claude validates nested settings-schema value types, not just the top level.
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
  //     already flagged. Parity: Claude validates nested settings-schema value types.
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
  //     object (parity: Claude's policyLimits `restrictions` schema). Step 1 only
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
      // Claude-Code's `{ type: 'command', command }` drop-in shape: `type` is accepted
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
  // Claude's ValidationErrorsList per-error docLink), so the loader diagnostic can
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
 * Returns the backup path (a fresh one, or an existing byte-identical one), or
 * null if the backup itself failed.
 *
 * CONTENT-DEDUP: a corrupt config is commonly re-read many times in one session
 * (the project/local/global waterfall, repeated `/doctor` sweeps, a watcher echo),
 * and each read would otherwise spawn another `.corrupt-<ts>` quarantine copy of
 * the SAME broken bytes — a backup storm. Before writing, the existing
 * `<file>.corrupt-*` quarantines are scanned; when one already holds byte-identical
 * content, that existing path is returned and NO new file is written. Parity:
 * Claude Code's corrupted-config quarantine skips the copy when an existing
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
 * Env var the CLI `--settings <file>` flag bridges to: an ABSOLUTE path to an
 * arbitrary settings JSON file loaded into the `flag` settings tier. Bridged (not
 * passed as an arg) so both the interactive TUI and the detached runner — which
 * both route through the engine factory — pick it up identically (TUI↔CLI parity).
 */
export const FLAG_SETTINGS_ENV = 'SUPERINFERENCE_SETTINGS';

/**
 * Load the `--settings <file>` flag-tier config (parity: Claude's `flagSettings`
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
    // Point the user at the most recent known-GOOD pre-write snapshot (`.backup-*`),
    // if any, with a copy-pasteable restore command (parity: Claude's `cp` restore
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

/**
 * Accumulate the MCP allowlist across config layers like {@link mergeConfigArray},
 * but over the richer {@link McpAllowedServerEntry} union — string entries dedupe by
 * value, provenance objects dedupe by their canonical JSON so a lower layer's
 * `{ pluginSource }` / `{ marketplace, plugin }` trust is never dropped by a higher
 * layer, only appended to. First-seen order is preserved.
 */
function mergeMcpAllowlist(
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

/**
 * Managed/policy-tier reconcile-poll interval (ms). The managed-settings file
 * ({@link getManagedConfigPath}) frequently lives on a path `fs.watch` cannot
 * reliably observe — a root-owned `/etc` entry, an MDM-pushed network/mount path,
 * or a `config.d/` drop-in dir — so when managed-tier watching is enabled a
 * dedicated low-frequency unref'd poll re-reads the merged managed layer and fires
 * {@link ConfigService.onConfigChange} `source: 'managed'` ONLY when the serialized
 * policy actually changed. Deliberately slower than the project/global reconcile
 * poll: enterprise policy changes are rare and the read may hit a slow mount.
 * Parity: Claude Code's remote/MDM managed-settings `POLLING_INTERVAL_MS` background
 * refresh that fails open (a failed read is swallowed, never fail-closed mid-session).
 */
export const CONFIG_MANAGED_POLL_MS = 60000;

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
  /**
   * Also hot-reload the GLOBAL (`~/.ami/config.json`) config tier, not just the
   * project tier. When enabled, a runtime edit to the user's global config fires the
   * {@link ConfigService.onConfigChange} callbacks with `source: 'global'` (vs the
   * project tier's `'project'`), so a consumer can tell WHICH tier changed and the
   * engine re-applies the full waterfall (env / permissions / managed) on a global
   * edit too. Defaults off so a plain `new ConfigService(cwd)` keeps the
   * project-only behavior. Parity: Claude Code's `useSettingsChange` watching every
   * SettingSource and threading the real `source` tier through `onChange`.
   */
  watchGlobal?: boolean;
  /**
   * Also hot-reload the MANAGED/policy config tier ({@link getManagedConfigPath} +
   * its `config.d/` drop-ins) mid-session. When enabled, a change to the merged
   * managed policy fires the {@link ConfigService.onConfigChange} callbacks with
   * `source: 'managed'`, so an administrator pushing an updated enterprise policy
   * (or an MDM sync) takes effect without a restart. Backed by a dedicated poll
   * ({@link CONFIG_MANAGED_POLL_MS}) PLUS an fs.watch on the managed base dir, and
   * fails OPEN — a transient/tampered read during the refresh is swallowed, never
   * fail-closed mid-session. Defaults off so a plain `new ConfigService(cwd)` keeps
   * the project-only behavior. Parity: Claude Code's background managed/policy
   * settings refresh + `notifyChange('policySettings')`.
   */
  watchManaged?: boolean;
  /** Override the managed-tier reconcile-poll interval (ms); `<= 0` disables it. Test seam. */
  managedPollMs?: number;
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
      // tier is unaffected); re-establishment is not attempted to keep this lean.
      this.globalWatcher.on('error', () => {
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
    if (this.globalDebounceTimer) clearTimeout(this.globalDebounceTimer);
    this.globalDebounceTimer = setTimeout(() => {
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
      this.managedWatcher.on('error', () => {
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
   * only the live-refresh safety net. Parity: Claude's background poll swallowing errors.
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
    if (this.managedDebounceTimer) clearTimeout(this.managedDebounceTimer);
    this.managedDebounceTimer = setTimeout(() => {
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
   * off or the interval is non-positive. Parity: Claude's managed-settings background poll.
   */
  private startManagedPoll(): void {
    if (this.managedPollTimer || !this.managedConfigPath || this.managedPollMs <= 0) return;
    this.managedPollTimer = setInterval(() => { this.processManagedChange(); }, this.managedPollMs);
    this.managedPollTimer.unref();
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
      // Positive introspection breadcrumb: announce that the PROJECT tier changed and
      // is being re-applied, naming the source tier — matching the `source: 'project'`
      // label fanned out to onConfigChange below and the global/managed tiers' own
      // "changed; notifying listeners" lines. Without this the project reload was the
      // only tier with no change-time log, so an operator tailing the debug log could
      // see a global/managed reapply but never a project one. Parity: Claude Code's
      // `logForDebugging('Settings changed from <source>, updating app state')`.
      coreLog('config', `project config ${this.configPath} changed; reapplying (source: project)`);
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
   * Parity: Claude Code's `changeDetector.dispose()` returning the watcher `close()`
   * promise so teardown can await the watcher fully stopping (ENOENT race #25253).
   */
  stop(): Promise<void> {
    if (this.debounceTimer) { clearTimeout(this.debounceTimer); this.debounceTimer = null; }
    if (this.stabilityTimer) { clearTimeout(this.stabilityTimer); this.stabilityTimer = null; }
    if (this.deletionTimer) { clearTimeout(this.deletionTimer); this.deletionTimer = null; }
    if (this.retryTimer) { clearTimeout(this.retryTimer); this.retryTimer = null; }
    if (this.pollTimer) { clearInterval(this.pollTimer); this.pollTimer = null; }
    if (this.globalDebounceTimer) { clearTimeout(this.globalDebounceTimer); this.globalDebounceTimer = null; }
    if (this.managedDebounceTimer) { clearTimeout(this.managedDebounceTimer); this.managedDebounceTimer = null; }
    if (this.managedPollTimer) { clearInterval(this.managedPollTimer); this.managedPollTimer = null; }
    const closing: Array<Promise<void>> = [];
    for (const w of [this.watcher, this.globalWatcher, this.managedWatcher]) {
      if (!w) continue;
      // A real FSWatcher is an EventEmitter and signals completion via a 'close' event;
      // await it so teardown can know the watcher has fully stopped. (A minimal test
      // fake without `once` simply resolves synchronously — nothing to await.)
      if (typeof w.once === 'function') {
        closing.push(new Promise<void>(resolve => { w.once('close', () => resolve()); }));
      }
      w.close();
    }
    this.watcher = null;
    this.globalWatcher = null;
    this.managedWatcher = null;
    this.started = false;
    return Promise.all(closing).then(() => undefined);
  }
}

/**
 * Merge configuration sources in priority order.
 *
 * Priority (highest first):
 *   1. `cliArgs`       — flags passed on the command line
 *   2. `envVars`       — values derived from environment variables
 *   3. `flagConfig`    — an arbitrary file passed with `--settings <file>`
 *   4. `localConfig`   — `.ami/config.local.json` (gitignored)
 *   5. `projectConfig` — `.ami/config.json` in the project
 *   6. `globalConfig`  — `~/.ami/config.json`
 *   7. `managedConfig` — `/etc/superinference/config.json`
 *   8. `pluginConfig`  — plugin-contributed BASE defaults (lowest priority)
 *
 * `flagConfig` (parity: Claude's `flagSettings`) layers a one-off settings file
 * ABOVE local/project/user but BELOW the managed-policy overrides applied after
 * the loop. `pluginConfig` (parity: Claude's settingsCache plugin base) is the
 * LOWEST layer: an installed plugin seeds default knobs that every file tier
 * overrides. Both are managed-only-key-free by construction (the per-field loop
 * omits managed-only keys, which are re-applied authoritatively from
 * `managedConfig` below), so a plugin or a `--settings` file can never grant
 * itself an admin/policy key.
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
  flagConfig?: ProjectConfig | null,
  pluginConfig?: ProjectConfig | null,
): ProjectConfig {
  const layers: Array<Partial<ProjectConfig>> = [
    pluginConfig ?? {},
    managedConfig ?? {},
    globalConfig ?? {},
    projectConfig ?? {},
    localConfig ?? {},
    flagConfig ?? {},
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
    if (layer.language !== undefined) merged.language = layer.language;
    if (layer.bashOutputMaxChars !== undefined) merged.bashOutputMaxChars = layer.bashOutputMaxChars;
    if (layer.taskOutputMaxChars !== undefined) merged.taskOutputMaxChars = layer.taskOutputMaxChars;
    if (layer.toolOutputMaxChars !== undefined) merged.toolOutputMaxChars = layer.toolOutputMaxChars;
    if (layer.cacheTtl !== undefined) merged.cacheTtl = layer.cacheTtl;
    if (layer.safeMode !== undefined) merged.safeMode = layer.safeMode;
    if (layer.hasTrustDialogAccepted !== undefined) merged.hasTrustDialogAccepted = layer.hasTrustDialogAccepted;
    // Trusted-path roots ACCUMULATE across layers (concat + dedupe) like the other
    // allowlists below: a higher layer adds more trusted roots, never drops a lower
    // layer's — trust only ever widens across the waterfall.
    if (layer.trustedProjectPaths !== undefined) merged.trustedProjectPaths = mergeConfigArray(merged.trustedProjectPaths, layer.trustedProjectPaths);
    if (layer.maxWebSearchesPerSession !== undefined) merged.maxWebSearchesPerSession = layer.maxWebSearchesPerSession;
    if (layer.cleanupPeriodDays !== undefined) merged.cleanupPeriodDays = layer.cleanupPeriodDays;
    if (layer.subagentDefaultModel !== undefined) merged.subagentDefaultModel = layer.subagentDefaultModel;
    if (layer.deferTools !== undefined) merged.deferTools = layer.deferTools;
    // Array-valued settings ACCUMULATE across layers (concat + dedupe), so a
    // higher layer augments rather than clobbers a lower one — parity with
    // Claude's settingsMergeCustomizer. Managed-authoritative arrays
    // (allowedMcpServers / sandboxExcludedCommands when managed owns them) are
    // still replaced wholesale by the managed override below (AMI superset).
    if (layer.alwaysLoadTools !== undefined) merged.alwaysLoadTools = mergeConfigArray(merged.alwaysLoadTools, layer.alwaysLoadTools);
    if (layer.fallbackModels !== undefined) merged.fallbackModels = mergeConfigArray(merged.fallbackModels, layer.fallbackModels);
    if (layer.allowedMcpServers !== undefined) merged.allowedMcpServers = mergeMcpAllowlist(merged.allowedMcpServers, layer.allowedMcpServers);
    // Denylist ACCUMULATES across layers like the allowlist — a lower layer's
    // deny can never be removed by a higher one, only added to (deny-wins).
    if (layer.deniedMcpServers !== undefined) merged.deniedMcpServers = mergeConfigArray(merged.deniedMcpServers, layer.deniedMcpServers);
    // Per-server user toggles accumulate across layers like the allow/deny lists so
    // a higher layer augments rather than clobbers (a project disable adds to a user
    // disable; a project enable adds to a user enable).
    if (layer.disabledMcpServers !== undefined) merged.disabledMcpServers = mergeConfigArray(merged.disabledMcpServers, layer.disabledMcpServers);
    if (layer.enabledMcpServers !== undefined) merged.enabledMcpServers = mergeConfigArray(merged.enabledMcpServers, layer.enabledMcpServers);
    if (layer.managedMcpServers !== undefined) merged.managedMcpServers = layer.managedMcpServers;
    if (layer.statusLine !== undefined) merged.statusLine = layer.statusLine;
    if (layer.sandboxExcludedCommands !== undefined) merged.sandboxExcludedCommands = mergeConfigArray(merged.sandboxExcludedCommands, layer.sandboxExcludedCommands);
    // Instruction-file exclude globs ACCUMULATE across layers (a higher layer adds
    // more exclusions, never removes a lower layer's) — parity with Claude's
    // claudeMdExcludes list-merge; unioned with the AMI_MEMORY_EXCLUDES env list at
    // load time by {@link resolveConfiguredMemoryExcludes}.
    if (layer.memoryExcludes !== undefined) merged.memoryExcludes = mergeConfigArray(merged.memoryExcludes, layer.memoryExcludes);
    // External-memory-import consent: the approval flag is last-layer-wins (a higher
    // tier's explicit decision overrides a lower one), while the optional trusted-roots
    // allowlist ACCUMULATES across layers (parity with the other memory/path list
    // settings — a higher layer adds more trusted roots, never silently drops a lower
    // layer's).
    if (layer.externalMemoryImportsApproved !== undefined) merged.externalMemoryImportsApproved = layer.externalMemoryImportsApproved;
    if (layer.externalMemoryImportRoots !== undefined) merged.externalMemoryImportRoots = mergeConfigArray(merged.externalMemoryImportRoots, layer.externalMemoryImportRoots);
    // env merges key-wise across layers (higher-priority layers override single
    // keys rather than replacing the whole block).
    if (layer.env !== undefined) merged.env = { ...merged.env, ...layer.env };
    if (layer.tipsEnabled !== undefined) merged.tipsEnabled = layer.tipsEnabled;
    if (layer.tipsOverride !== undefined) merged.tipsOverride = deepMergeConfigObjects(merged.tipsOverride as Record<string, unknown> | undefined, layer.tipsOverride as Record<string, unknown>) as ProjectConfig['tipsOverride'];
    if (layer.composerHintsEnabled !== undefined) merged.composerHintsEnabled = layer.composerHintsEnabled;
    if (layer.composerHintsOverride !== undefined) merged.composerHintsOverride = deepMergeConfigObjects(merged.composerHintsOverride as Record<string, unknown> | undefined, layer.composerHintsOverride as Record<string, unknown>) as ProjectConfig['composerHintsOverride'];
    if (layer.attribution !== undefined) merged.attribution = deepMergeConfigObjects(merged.attribution as Record<string, unknown> | undefined, layer.attribution as Record<string, unknown>) as ProjectConfig['attribution'];
    // Engine-behavior knobs: last-layer-wins for scalars (parity with the other
    // scalar settings above), key-wise merge for the `features` map (a higher layer
    // overrides a single feature rather than replacing the whole block), and
    // array-accumulate for the MCP always-load list (a higher layer augments it).
    // Before this, every one of these was READ by the engine factory off the merged
    // config yet never COPIED here, so a `.ami/config.json` value was silently dropped.
    if (layer.architectEnabled !== undefined) merged.architectEnabled = layer.architectEnabled;
    if (layer.architectModel !== undefined) merged.architectModel = layer.architectModel;
    if (layer.embeddedSearchTools !== undefined) merged.embeddedSearchTools = layer.embeddedSearchTools;
    if (layer.teammateExecutionMode !== undefined) merged.teammateExecutionMode = layer.teammateExecutionMode;
    if (layer.unattendedRetry !== undefined) merged.unattendedRetry = layer.unattendedRetry;
    if (layer.fileCheckpointingEnabled !== undefined) merged.fileCheckpointingEnabled = layer.fileCheckpointingEnabled;
    if (layer.heapDumpOnMemoryWarning !== undefined) merged.heapDumpOnMemoryWarning = layer.heapDumpOnMemoryWarning;
    if (layer.features !== undefined) merged.features = deepMergeConfigObjects(merged.features as Record<string, unknown> | undefined, layer.features as Record<string, unknown>) as ProjectConfig['features'];
    if (layer.max529RetriesBeforeFallback !== undefined) merged.max529RetriesBeforeFallback = layer.max529RetriesBeforeFallback;
    if (layer.openrouterZdr !== undefined) merged.openrouterZdr = layer.openrouterZdr;
    if (layer.strictMcpConfig !== undefined) merged.strictMcpConfig = layer.strictMcpConfig;
    if (layer.mcpRequireApproval !== undefined) merged.mcpRequireApproval = layer.mcpRequireApproval;
    if (layer.maxUsageLimitWaitMs !== undefined) merged.maxUsageLimitWaitMs = layer.maxUsageLimitWaitMs;
    if (layer.sessionMemoryInterval !== undefined) merged.sessionMemoryInterval = layer.sessionMemoryInterval;
    if (layer.alwaysLoadMcpServers !== undefined) merged.alwaysLoadMcpServers = mergeConfigArray(merged.alwaysLoadMcpServers, layer.alwaysLoadMcpServers);
    if (layer.continueOnInvalidConfig !== undefined) merged.continueOnInvalidConfig = layer.continueOnInvalidConfig;
    if (layer.outputStyle !== undefined) merged.outputStyle = layer.outputStyle;
    if (layer.includeGitInstructions !== undefined) merged.includeGitInstructions = layer.includeGitInstructions;
    // Startup runtime-knob bridges: last-layer-wins for the scalars, array-accumulate
    // for additionalDirectories (a higher layer adds more dirs, never removes a lower
    // layer's — parity with the other accumulating list settings). Before this, each
    // was declared + value-validated but never COPIED here, so a file-tier value was
    // silently dropped.
    if (layer.extraCaCerts !== undefined) merged.extraCaCerts = layer.extraCaCerts;
    if (layer.plansDirectory !== undefined) merged.plansDirectory = layer.plansDirectory;
    if (layer.respectGitignore !== undefined) merged.respectGitignore = layer.respectGitignore;
    if (layer.autoCompactEnabled !== undefined) merged.autoCompactEnabled = layer.autoCompactEnabled;
    if (layer.fastMode !== undefined) merged.fastMode = layer.fastMode;
    if (layer.redactThinking !== undefined) merged.redactThinking = layer.redactThinking;
    if (layer.additionalDirectories !== undefined) merged.additionalDirectories = mergeConfigArray(merged.additionalDirectories, layer.additionalDirectories);
    // Engine-behavior knobs (round 2): last-layer-wins scalars the engine factory
    // already reads off the merged config (resolveAutoMemory / resolveTitleGenerationEnabled
    // / resolveWorktreeToolsEnabled) and the two startup env bridges below consume. Before
    // this they were READ yet never COPIED here, so a file-tier value was silently dropped.
    if (layer.autoMemoryEnabled !== undefined) merged.autoMemoryEnabled = layer.autoMemoryEnabled;
    if (layer.autoDreamEnabled !== undefined) merged.autoDreamEnabled = layer.autoDreamEnabled;
    if (layer.sessionMemoryEnabled !== undefined) merged.sessionMemoryEnabled = layer.sessionMemoryEnabled;
    if (layer.teamMemoryEnabled !== undefined) merged.teamMemoryEnabled = layer.teamMemoryEnabled;
    if (layer.enableTitleGeneration !== undefined) merged.enableTitleGeneration = layer.enableTitleGeneration;
    if (layer.worktreeToolsEnabled !== undefined) merged.worktreeToolsEnabled = layer.worktreeToolsEnabled;
    if (layer.speculationEnabled !== undefined) merged.speculationEnabled = layer.speculationEnabled;
    if (layer.alwaysThinkingEnabled !== undefined) merged.alwaysThinkingEnabled = layer.alwaysThinkingEnabled;
    if (layer.autoConnectIde !== undefined) merged.autoConnectIde = layer.autoConnectIde;
    if (layer.terminalProgressBarEnabled !== undefined) merged.terminalProgressBarEnabled = layer.terminalProgressBarEnabled;
    if (layer.showTurnDuration !== undefined) merged.showTurnDuration = layer.showTurnDuration;
    // Cosmetic / output-detail startup knobs: last-layer-wins scalars + object the
    // applyConfigStartupKnobs env bridges below consume. Before this they would have been
    // dropped by the merge, so a file-tier value never reached the runtime.
    if (layer.verbose !== undefined) merged.verbose = layer.verbose;
    if (layer.copyOnSelect !== undefined) merged.copyOnSelect = layer.copyOnSelect;
    if (layer.copyFullResponse !== undefined) merged.copyFullResponse = layer.copyFullResponse;
    if (layer.fullscreen !== undefined) merged.fullscreen = layer.fullscreen;
    if (layer.syntaxHighlightingDisabled !== undefined) merged.syntaxHighlightingDisabled = layer.syntaxHighlightingDisabled;
    if (layer.showThinkingSummaries !== undefined) merged.showThinkingSummaries = layer.showThinkingSummaries;
    if (layer.messageIdleNotifThresholdMs !== undefined) merged.messageIdleNotifThresholdMs = layer.messageIdleNotifThresholdMs;
    if (layer.spinnerVerbs !== undefined) merged.spinnerVerbs = layer.spinnerVerbs;
    if (layer.backgroundTasksEnabled !== undefined) merged.backgroundTasksEnabled = layer.backgroundTasksEnabled;
    // UI/UX knobs (round 4): last-layer-wins scalars read by the common/cli footer +
    // plan-accept + release-notes flows. Copied here so a file-tier value is not dropped
    // by the merge before its resolver (shouldShowClearContextOnPlanAccept /
    // isPrStatusFooterEnabled / getLastReleaseNotesSeen) reads it off the merged config.
    if (layer.showClearContextOnPlanAccept !== undefined) merged.showClearContextOnPlanAccept = layer.showClearContextOnPlanAccept;
    if (layer.prStatusFooterEnabled !== undefined) merged.prStatusFooterEnabled = layer.prStatusFooterEnabled;
    if (layer.voiceEnabled !== undefined) merged.voiceEnabled = layer.voiceEnabled;
    if (layer.autoAllowOptIn !== undefined) merged.autoAllowOptIn = layer.autoAllowOptIn;
    // Diff renderer / input-box shell selectors + example-command cache state + managed
    // startup announcements + sandbox-override policy: last-layer-wins scalars/arrays read
    // off the merged config by resolveDiffTool / resolveDefaultShell / getCachedExampleFiles /
    // pickCompanyAnnouncement / areUnsandboxedCommandsAllowed. Copied here so a file-tier
    // value is not dropped by the merge before its resolver reads it.
    if (layer.diffTool !== undefined) merged.diffTool = layer.diffTool;
    if (layer.defaultShell !== undefined) merged.defaultShell = layer.defaultShell;
    if (layer.exampleFiles !== undefined) merged.exampleFiles = layer.exampleFiles;
    if (layer.exampleFilesGeneratedAt !== undefined) merged.exampleFilesGeneratedAt = layer.exampleFilesGeneratedAt;
    if (layer.companyAnnouncements !== undefined) merged.companyAnnouncements = layer.companyAnnouncements;
    if (layer.allowUnsandboxedCommands !== undefined) merged.allowUnsandboxedCommands = layer.allowUnsandboxedCommands;
    if (layer.lastReleaseNotesSeen !== undefined) merged.lastReleaseNotesSeen = layer.lastReleaseNotesSeen;
    if (layer.autoUpdatesChannel !== undefined) merged.autoUpdatesChannel = layer.autoUpdatesChannel;
    if (layer.minimumVersion !== undefined) merged.minimumVersion = layer.minimumVersion;
    // Admin-policy / engine knobs (round 3): the policy maps deep-merge across layers
    // (a higher layer ADDS denies/entries, never silently drops a lower layer's), the
    // marketplace policy lists ACCUMULATE (deny/allow-wins union), and the remaining
    // scalars are last-layer-wins. Each was READ by a common/ resolver off the merged
    // config yet never COPIED here, so a file-tier value was silently dropped.
    if (layer.policyRestrictions !== undefined) merged.policyRestrictions = deepMergeConfigObjects(merged.policyRestrictions as Record<string, unknown> | undefined, layer.policyRestrictions as Record<string, unknown>) as ProjectConfig['policyRestrictions'];
    if (layer.policyRefresh !== undefined) merged.policyRefresh = deepMergeConfigObjects(merged.policyRefresh as Record<string, unknown> | undefined, layer.policyRefresh as Record<string, unknown>) as ProjectConfig['policyRefresh'];
    if (layer.pluginTrustMessage !== undefined) merged.pluginTrustMessage = layer.pluginTrustMessage;
    if (layer.memoryCriticalRssBytes !== undefined) merged.memoryCriticalRssBytes = layer.memoryCriticalRssBytes;
    if (layer.preferTmuxOverIterm2 !== undefined) merged.preferTmuxOverIterm2 = layer.preferTmuxOverIterm2;
    if (layer.blockedMarketplaces !== undefined) merged.blockedMarketplaces = mergeConfigArray(merged.blockedMarketplaces, layer.blockedMarketplaces);
    if (layer.strictKnownMarketplaces !== undefined) merged.strictKnownMarketplaces = mergeConfigArray(merged.strictKnownMarketplaces, layer.strictKnownMarketplaces);
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
  // Managed sandbox-egress lockdown is MANAGED-authoritative: only the admin tier
  // may force domains-only egress; a lower tier can never set (or clear) it. Parity:
  // Claude's shouldAllowManagedSandboxDomainsOnly() policy knob.
  if (managedConfig?.allowManagedSandboxDomainsOnly) {
    merged.allowManagedSandboxDomainsOnly = true;
  }
  // Managed feature-restriction map is AUTHORITATIVE: a lower tier may ADD denies for
  // new policies (handled by the deep-merge in the loop), but a managed deny can never
  // be RELAXED by a lower layer re-allowing it. Re-apply the managed entries ON TOP so
  // each managed `{ allowed }` wins per-policy (deny-wins / admin-wins). Parity: Claude
  // Code's policyLimits being sourced from the managed/enterprise tier only.
  if (managedConfig?.policyRestrictions) {
    merged.policyRestrictions = deepMergeConfigObjects(
      merged.policyRestrictions as Record<string, unknown> | undefined,
      managedConfig.policyRestrictions as Record<string, unknown>,
    ) as ProjectConfig['policyRestrictions'];
  }
  // Managed tier is HIGHEST-PRECEDENCE and authoritative for the security-relevant
  // scalar settings, not just the allowlists/killswitches handled above. The
  // per-field loop merges these last-layer-wins, so without this a `--yolo` /
  // env / project value would silently override an enterprise cap. We re-apply the
  // managed value for every {@link TIER_LOCKED_KEYS} entry that exists on
  // ProjectConfig (permissionMode / maxTurns / maxBudgetUsd) PLUS safeMode, so a
  // lower tier can never widen a managed security boundary at runtime. This reuses
  // the SAME TIER_LOCKED_KEYS set the config-tool guard enforces at write time (one
  // source — no second lock list). Parity: Claude Code's policySettings being the
  // last, winning SETTING_SOURCE for all security-relevant settings.
  if (managedConfig) {
    for (const key of TIER_LOCKED_KEYS) {
      if ((managedConfig as Record<string, unknown>)[key] !== undefined) {
        (merged as Record<string, unknown>)[key] = (managedConfig as Record<string, unknown>)[key];
      }
    }
    if (managedConfig.safeMode !== undefined) {
      merged.safeMode = managedConfig.safeMode;
    }
  }

  return merged;
}

/**
 * Resolve the effective {@link ProjectConfig.memoryExcludes} for a workspace by
 * merging the full config waterfall (managed > global > project > local — CLI/env
 * carry no `memoryExcludes`) and returning the accumulated exclude-glob list (empty
 * when none is configured). The memory loader UNIONS this with the
 * `AMI_MEMORY_EXCLUDES` env list, so both the persisted config knob and the one-shot
 * env override are honored. Kept in the config layer (not memory.ts) so there is ONE
 * place that reads the config files and applies the merge semantics. Parity: Claude
 * Code resolving `claudeMdExcludes` from the settings waterfall for `isClaudeMdExcluded`.
 */
export function resolveConfiguredMemoryExcludes(cwd: string): string[] {
  let managed: ProjectConfig | null = null;
  // A broken/untrusted managed policy fails closed ({@link ManagedConfigError}); a
  // memory-exclude lookup must never abort memory loading over it — treat it as absent.
  try { managed = loadManagedConfig(); } catch { managed = null; }
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
 * {@link resolveConfiguredMemoryExcludes}. Parity: Claude Code gating team memory behind
 * an enable flag before reading the shared store.
 */
export function resolveTeamMemoryLoadingEnabled(cwd: string): boolean {
  let managed: ProjectConfig | null = null;
  // A broken/untrusted managed policy fails closed ({@link ManagedConfigError}); a
  // team-memory gate lookup must never abort memory loading over it — treat it as absent.
  try { managed = loadManagedConfig(); } catch { managed = null; }
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
 * as absent so a speculation lookup never aborts startup. Parity: Claude Code reading the
 * `speculationEnabled` global-config knob for `isSpeculationEnabled()`.
 */
export function resolveSpeculationEnabled(cwd: string): boolean | undefined {
  let managed: ProjectConfig | null = null;
  try { managed = loadManagedConfig(); } catch { managed = null; }
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
 * Whether thinking should be ENABLED BY DEFAULT at session start (parity: Claude Code's
 * `shouldEnableThinkingByDefault()` in utils/thinking.ts). Ordered gate:
 *   1. the `MAX_THINKING_TOKENS` env var, when set to a finite number, wins — a positive
 *      value forces default-on, `0` (or negative) forces off (matches the cli's
 *      `MAX_THINKING_TOKENS` handling and Claude's `parseInt(...) > 0`);
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
  let managed: ProjectConfig | null = null;
  try { managed = loadManagedConfig(); } catch { managed = null; }
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
 * ever loading an outside-workspace `@import`. Parity: Claude Code reading
 * `hasClaudeMdExternalIncludesApproved` from the project config for external includes.
 */
export function resolveConfiguredExternalMemoryImports(cwd: string): { approved: boolean; roots: string[] } {
  let managed: ProjectConfig | null = null;
  // A broken/untrusted managed policy fails closed ({@link ManagedConfigError}); a
  // consent lookup must never abort memory loading over it — treat it as absent.
  try { managed = loadManagedConfig(); } catch { managed = null; }
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
 * Normalize a directory path into the canonical, absolute form used as a trusted-path
 * key / ancestor-walk node (parity: Claude Code's `normalizePathForConfigKey`). A single
 * normalizer so a path listed in {@link ProjectConfig.trustedProjectPaths} and the `cwd`
 * being checked compare identically regardless of `.`/`..`/trailing-separator spelling.
 */
export function normalizeTrustPath(p: string): string {
  return path.resolve(p);
}

/**
 * True-only trust latch (parity: Claude Code's `computeTrustDialogAccepted` latch that
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
 *     trust of all its children. Parity: Claude Code's `computeTrustDialogAccepted`
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
  let managed: ProjectConfig | null = null;
  try { managed = loadManagedConfig(); } catch { managed = null; }
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

/**
 * Bridge the persisted {@link ProjectConfig.language} knob into the
 * `SUPERINFERENCE_LANGUAGE` env var that {@link buildResponseLanguageSection}
 * (the single response-language consumer) already reads — so a user can persist a
 * preferred response language in project/user config instead of exporting the env
 * var before every launch (parity: Claude Code's `LanguagePicker` persisted setting).
 *
 * Precedence is preserved: an explicit, non-blank `SUPERINFERENCE_LANGUAGE` already
 * in `target` WINS (a one-shot env override is still possible) and is returned
 * unchanged; otherwise a non-blank `config.language` seeds it (trimmed). A
 * missing/blank value on both sides is a no-op (English — the default — never gets a
 * redundant directive). Returns the effective language, or `undefined` when none
 * applies. Applied once at engine setup (common/src/engine-factory.ts), mirroring
 * {@link applyConfigEnv}.
 */
export function applyConfigLanguage(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): string | undefined {
  const existing = target.SUPERINFERENCE_LANGUAGE;
  if (existing && existing.trim()) return existing.trim();
  const lang = config?.language;
  if (typeof lang === 'string' && lang.trim()) {
    const trimmed = lang.trim();
    target.SUPERINFERENCE_LANGUAGE = trimmed;
    return trimmed;
  }
  return undefined;
}

/** Anthropic beta token for the `fast-mode` opt-in (parity: Claude Code's betas). */
export const FAST_MODE_BETA = 'fast-mode-2026-02-01';
/** Anthropic beta token for the `redact-thinking` request opt-in (parity: Claude Code). */
export const REDACT_THINKING_BETA = 'redact-thinking-2026-02-12';

/**
 * Bridge the persisted {@link ProjectConfig.extraCaCerts} knob into
 * `NODE_EXTRA_CA_CERTS` so Node picks up a corporate-proxy / TLS-inspecting CA at
 * startup, without the user having to export the env var before every launch
 * (parity: Claude Code's `caCertsConfig.applyExtraCACertsFromConfig`). An explicit
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

/**
 * Bridge the persisted {@link ProjectConfig.respectGitignore} knob into the existing
 * `AMI_GLOB_NO_IGNORE` env knob that {@link import('./tools/glob').globRespectsGitignore}
 * already reads — so a user can persist the file-picker gitignore preference in
 * project/user config instead of exporting the env var (parity: Claude Code's
 * `respectGitignore` setting). An explicit `AMI_GLOB_NO_IGNORE` already in `target`
 * WINS (one-shot override); otherwise `respectGitignore === true` honors `.gitignore`
 * (sets the knob falsey) and `=== false` ignores it (sets it truthy). An unset config
 * value is a no-op (AMI's default: glob does not filter through gitignore).
 */
export function applyConfigGitignore(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): void {
  if (target.AMI_GLOB_NO_IGNORE !== undefined) return;
  const respect = config?.respectGitignore;
  if (typeof respect === 'boolean') {
    // AMI_GLOB_NO_IGNORE is "do NOT respect gitignore"; it is the inverse of the knob.
    target.AMI_GLOB_NO_IGNORE = respect ? 'false' : 'true';
  }
}

/**
 * Bridge the persisted {@link ProjectConfig.autoCompactEnabled} knob into the existing
 * `SUPERINFERENCE_DISABLE_AUTO_COMPACT` env knob that
 * {@link import('../../common/src/context-manager').isAutoCompactDisabled} reads — so a
 * user can persist "turn auto-compaction off" in project/user config (parity: Claude
 * Code's `autoCompactEnabled` toggle). An explicit disable env var (either the
 * `SUPERINFERENCE_` or the Claude-compatible `DISABLE_AUTO_COMPACT` alias) already in
 * `target` WINS; otherwise `autoCompactEnabled === false` arms the disable. Unset/
 * `true` is a no-op (auto-compaction stays on, the default).
 */
export function applyConfigAutoCompact(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): void {
  if (target.SUPERINFERENCE_DISABLE_AUTO_COMPACT !== undefined || target.DISABLE_AUTO_COMPACT !== undefined) return;
  if (config?.autoCompactEnabled === false) {
    target.SUPERINFERENCE_DISABLE_AUTO_COMPACT = '1';
  }
}

/**
 * Bridge the persisted {@link ProjectConfig.plansDirectory} knob into the existing
 * `AMI_PLANS_DIR` env knob that {@link import('./tools/plan-mode').getPlansDirectory}
 * reads (and validates for path-traversal, resolving relative to the project root) —
 * so a user can persist a custom plans directory in project/user config (parity:
 * Claude Code's `plansDirectory`). An explicit `AMI_PLANS_DIR` already in `target`
 * WINS; otherwise a non-blank `config.plansDirectory` seeds it (trimmed).
 */
export function applyConfigPlansDirectory(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): void {
  if (target.AMI_PLANS_DIR !== undefined) return;
  const dir = config?.plansDirectory;
  if (typeof dir === 'string' && dir.trim()) {
    target.AMI_PLANS_DIR = dir.trim();
  }
}

/**
 * Bridge the persisted {@link ProjectConfig.fastMode} / {@link ProjectConfig.redactThinking}
 * beta opt-ins into the existing `ANTHROPIC_BETAS` env escape hatch that
 * {@link import('./provider').getExtraAnthropicBetas} appends to the `anthropic-beta`
 * header — so a user can persist an API-beta opt-in in project/user config (parity:
 * Claude Code's `fast-mode` / `redact-thinking` beta knobs). The `redact-thinking` token
 * is armed by EITHER {@link ProjectConfig.redactThinking} `=== true` OR the inverse
 * {@link ProjectConfig.showThinkingSummaries} `=== false` (parity: Claude gates it on
 * `!showThinkingSummaries`). Each enabled knob's token is appended to the comma-separated
 * list WITHOUT duplicating one already present; any tokens a user set directly in
 * `ANTHROPIC_BETAS` are preserved. A no-op when no knob is enabled.
 */
export function applyConfigAnthropicBetas(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): void {
  const tokens: string[] = [];
  if (config?.fastMode === true) tokens.push(FAST_MODE_BETA);
  // redact-thinking is armed by EITHER the explicit redactThinking opt-in OR the inverse
  // showThinkingSummaries knob being turned OFF (parity: Claude Code gates
  // REDACT_THINKING_BETA on `!showThinkingSummaries`). The token is de-duped below, so
  // setting both never double-appends.
  if (config?.redactThinking === true || config?.showThinkingSummaries === false) {
    tokens.push(REDACT_THINKING_BETA);
  }
  if (tokens.length === 0) return;
  const merged = (target.ANTHROPIC_BETAS ?? '').split(',').map(t => t.trim()).filter(Boolean);
  for (const token of tokens) {
    if (!merged.includes(token)) merged.push(token);
  }
  target.ANTHROPIC_BETAS = merged.join(',');
}

/** Env kill-switch (cli `PROGRESS_REPORTING_ENV`) consumed by the OSC 9;4 progress gate. */
export const NO_PROGRESS_ENV = 'AMI_NO_PROGRESS';
/** Env knob (cli `resolveShowTurnDuration`) that suppresses the per-turn duration. */
export const SHOW_TURN_DURATION_ENV = 'AMI_SHOW_TURN_DURATION';
/** Env knob (cli `resolveVerboseEnabled`) that enables verbose (untruncated) rendering. */
export const VERBOSE_ENV = 'AMI_VERBOSE';
/** Env opt-out (cli `copyOnSelectEnabled`) disabling terminal selection auto-copy. */
export const NO_COPY_ON_SELECT_ENV = 'AMI_NO_COPY_ON_SELECT';
/** Env kill-switch (cli `detectTerminalCapabilities`.alternateScreen) forcing scrollback layout. */
export const NO_ALT_SCREEN_ENV = 'AMI_NO_ALT_SCREEN';
/** Env force-on (cli `detectTerminalCapabilities`.alternateScreen) forcing fullscreen layout. */
export const FORCE_ALT_SCREEN_ENV = 'AMI_FORCE_ALT_SCREEN';
/** Env kill-switch (cli `syntaxHighlightEnabled`) disabling code/diff syntax highlighting. */
export const SYNTAX_HIGHLIGHT_ENV = 'AMI_SYNTAX_HIGHLIGHT';
/** Env knob (cli `getNotifyThreshold`) for the idle-before-notification threshold (ms). */
export const NOTIFY_THRESHOLD_ENV = 'AMI_DESKTOP_NOTIFY_THRESHOLD_MS';
/** Env knob (cli `resolveSpinnerVerbConfig`) carrying the JSON spinner-verb override. */
export const SPINNER_VERBS_ENV = 'AMI_SPINNER_VERBS';

/**
 * Bridge the persisted {@link ProjectConfig.terminalProgressBarEnabled} knob into the
 * existing `AMI_NO_PROGRESS` kill-switch that the cli OSC 9;4 progress gate
 * (`isProgressReportingAvailable`) already reads — so a user can persist "no progress
 * bar" in project/user config instead of exporting the env var (parity: Claude Code's
 * `terminalProgressBarEnabled` setting). An explicit `AMI_NO_PROGRESS` already in
 * `target` WINS (one-shot override); otherwise `terminalProgressBarEnabled === false`
 * arms the kill-switch. Unset/`true` is a no-op (progress stays on — AMI's default; a
 * superset divergence from Claude's default-off, keeping AMI's loved default while
 * ADDing the opt-out).
 */
export function applyConfigTerminalProgress(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): void {
  if (target[NO_PROGRESS_ENV] !== undefined) return;
  if (config?.terminalProgressBarEnabled === false) {
    target[NO_PROGRESS_ENV] = '1';
  }
}

/**
 * Bridge the persisted {@link ProjectConfig.showTurnDuration} knob into the existing
 * `AMI_SHOW_TURN_DURATION` env knob that the cli `resolveShowTurnDuration` already
 * reads — so a user can persist "hide the per-turn duration" in project/user config
 * instead of exporting the env var (parity: Claude Code's `showTurnDuration` setting).
 * An explicit `AMI_SHOW_TURN_DURATION` already in `target` WINS; otherwise
 * `showTurnDuration === false` arms the suppression (the resolver treats `'0'` as
 * falsy). Unset/`true` is a no-op (duration stays shown — the default).
 */
export function applyConfigTurnDuration(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): void {
  if (target[SHOW_TURN_DURATION_ENV] !== undefined) return;
  if (config?.showTurnDuration === false) {
    target[SHOW_TURN_DURATION_ENV] = '0';
  }
}

/**
 * Bridge the persisted {@link ProjectConfig.verbose} knob into the existing `AMI_VERBOSE`
 * env knob the cli `resolveVerboseEnabled` already reads (API-error display + tool-result
 * full-vs-truncated rendering) — so a user can persist "verbose output" in the settings
 * waterfall (`.ami/config.json`) instead of only via the `--verbose` flag / `--config`
 * key. An explicit `AMI_VERBOSE` already in `target` WINS (so the flag/`--config` bridge
 * that seeds it is preserved); otherwise `verbose === true` arms it. Unset/`false` is a
 * no-op (concise default). Parity: Claude Code's persisted `verbose` global-config knob.
 */
export function applyConfigVerbose(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): void {
  if (target[VERBOSE_ENV] !== undefined) return;
  if (config?.verbose === true) {
    target[VERBOSE_ENV] = '1';
  }
}

/**
 * Bridge the persisted {@link ProjectConfig.copyOnSelect} knob into the existing
 * `AMI_NO_COPY_ON_SELECT` opt-out the cli `copyOnSelectEnabled` already reads — so a user
 * can persist "don't auto-copy on select" in the settings waterfall instead of exporting
 * the env var. An explicit `AMI_NO_COPY_ON_SELECT` already in `target` WINS; otherwise
 * `copyOnSelect === false` arms the opt-out. Unset/`true` is a no-op (auto-copy stays on
 * — the default). Parity: Claude Code's `copyOnSelect ?? true` global-config knob.
 */
export function applyConfigCopyOnSelect(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): void {
  if (target[NO_COPY_ON_SELECT_ENV] !== undefined) return;
  if (config?.copyOnSelect === false) {
    target[NO_COPY_ON_SELECT_ENV] = '1';
  }
}

/**
 * Bridge the persisted {@link ProjectConfig.fullscreen} knob onto the existing
 * `AMI_NO_ALT_SCREEN` / `AMI_FORCE_ALT_SCREEN` env overrides that the cli's
 * {@link import('../../../cli/src/render').detectTerminalCapabilities} `alternateScreen`
 * capability already reads — so a user can persist "use terminal scrollback instead of
 * the fullscreen alternate-screen buffer" in the settings waterfall instead of exporting
 * an env var every session (parity: Claude Code's `isFullscreenEnvEnabled()` layout
 * toggle). An explicit env value for EITHER knob already present in `target` WINS (the
 * user set it on the command line / shell), so config never overrides a live env choice.
 * Otherwise `fullscreen === false` arms the `AMI_NO_ALT_SCREEN` kill-switch (scrollback
 * layout) and `fullscreen === true` arms `AMI_FORCE_ALT_SCREEN` so the fullscreen
 * preference survives the tmux-control-mode auto-off default. Unset is a no-op (the
 * terminal-capability default decides). Wired into {@link applyConfigStartupKnobs}.
 */
export function applyConfigFullscreen(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): void {
  // An explicit env override for either direction wins over the persisted preference.
  if (target[NO_ALT_SCREEN_ENV] !== undefined || target[FORCE_ALT_SCREEN_ENV] !== undefined) return;
  if (config?.fullscreen === false) {
    target[NO_ALT_SCREEN_ENV] = '1';
  } else if (config?.fullscreen === true) {
    target[FORCE_ALT_SCREEN_ENV] = '1';
  }
}

/**
 * Bridge the persisted {@link ProjectConfig.syntaxHighlightingDisabled} knob into the
 * existing `AMI_SYNTAX_HIGHLIGHT` gate the cli `syntaxHighlightEnabled` already reads
 * (the single highlight on/off source for both code blocks and diff lines) — so a user
 * can persist "no syntax highlighting" in the settings waterfall instead of only via the
 * `--config` key. An explicit `AMI_SYNTAX_HIGHLIGHT` already in `target` WINS; otherwise
 * `syntaxHighlightingDisabled === true` arms the kill-switch (a defined-falsy `'0'`).
 * Unset/`false` is a no-op (highlighting stays on). Parity: Claude Code's
 * `syntaxHighlightingDisabled` cosmetic config knob.
 */
export function applyConfigSyntaxHighlighting(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): void {
  if (target[SYNTAX_HIGHLIGHT_ENV] !== undefined) return;
  if (config?.syntaxHighlightingDisabled === true) {
    target[SYNTAX_HIGHLIGHT_ENV] = '0';
  }
}

/**
 * Bridge the persisted {@link ProjectConfig.messageIdleNotifThresholdMs} knob into the
 * existing `AMI_DESKTOP_NOTIFY_THRESHOLD_MS` env knob the cli `getNotifyThreshold` already
 * reads (idle-before-"generation done"-notification threshold) — so a user can persist
 * the timing in the settings waterfall instead of exporting the env var. An explicit
 * `AMI_DESKTOP_NOTIFY_THRESHOLD_MS` already in `target` WINS; otherwise a positive finite
 * value seeds it. A non-positive/non-finite value is ignored (keeps the built-in
 * default). Parity: Claude Code's numeric `messageIdleNotifThresholdMs` (default 60000).
 */
export function applyConfigIdleNotifThreshold(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): void {
  if (target[NOTIFY_THRESHOLD_ENV] !== undefined) return;
  const ms = config?.messageIdleNotifThresholdMs;
  if (typeof ms === 'number' && Number.isFinite(ms) && ms > 0) {
    target[NOTIFY_THRESHOLD_ENV] = String(Math.floor(ms));
  }
}

/**
 * Bridge the persisted {@link ProjectConfig.spinnerVerbs} knob into the existing
 * `AMI_SPINNER_VERBS` JSON env knob the cli `resolveSpinnerVerbConfig` already reads — so
 * a user can persist a spinner-verb override in the settings waterfall instead of
 * exporting the env var. Serialized to the `{ mode, verbs }` JSON shape the resolver
 * expects. An explicit `AMI_SPINNER_VERBS` already in `target` WINS. A value whose
 * `verbs` is not a non-empty string array is ignored (nothing to override). Parity:
 * Claude Code's cosmetic `spinnerVerbs` config knob.
 */
export function applyConfigSpinnerVerbs(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): void {
  if (target[SPINNER_VERBS_ENV] !== undefined) return;
  const sv = config?.spinnerVerbs;
  if (!sv || typeof sv !== 'object') return;
  const verbs = sv.verbs;
  if (!Array.isArray(verbs) || !verbs.some(v => typeof v === 'string' && v.trim())) return;
  const cleanVerbs = verbs.filter((v): v is string => typeof v === 'string' && v.trim() !== '');
  const mode: 'append' | 'replace' = sv.mode === 'replace' ? 'replace' : 'append';
  target[SPINNER_VERBS_ENV] = JSON.stringify({ mode, verbs: cleanVerbs });
}

/**
 * Bridge the persisted {@link ProjectConfig.backgroundTasksEnabled} knob into the
 * existing `AMI_DISABLE_BACKGROUND_TASKS` env gate the core bash/task tools already read
 * (via {@link isBackgroundTasksDisabled}) — so a user can persist "no background tasks"
 * in the settings waterfall (`.ami/config.json`) instead of exporting the env var
 * (parity: Claude Code's `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS`). `backgroundTasksEnabled
 * === false` arms the gate (sets `'1'`); unset/`true` is a no-op (background tasks stay
 * on — AMI's default). An explicit {@link DISABLE_BACKGROUND_TASKS_ENV} OR its
 * Claude-compatible {@link DISABLE_BACKGROUND_TASKS_ENV_COMPAT} alias already in `target`
 * WINS (one-shot override — the tool gate honors either spelling, so a pre-set alias must
 * not be clobbered). Because {@link isBackgroundTasksDisabled} is evaluated at tool
 * schema-build + execution time (after engine setup), seeding the env here from the
 * MERGED config makes the drop take effect from the first turn.
 */
export function applyConfigBackgroundTasks(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): void {
  if (target[DISABLE_BACKGROUND_TASKS_ENV] !== undefined) return;
  if (target[DISABLE_BACKGROUND_TASKS_ENV_COMPAT] !== undefined) return;
  if (config?.backgroundTasksEnabled === false) {
    target[DISABLE_BACKGROUND_TASKS_ENV] = '1';
  }
}

/**
 * Apply every startup config→runtime knob bridge in one call (the env-targeting
 * ones): CA certs, glob-gitignore, auto-compaction, plans directory, Anthropic betas,
 * the OSC 9;4 progress kill-switch, the per-turn duration suppression, and the cosmetic/
 * output-detail knobs (verbose, copy-on-select, syntax highlighting, idle-notification
 * threshold, spinner verbs). Called once at engine setup with the MERGED config so a
 * value from any file tier takes effect,
 * mirroring {@link applyConfigLanguage}. `{@link resolveAdditionalDirectories}` is
 * applied separately (it returns a value threaded into the PermissionManager's allowed
 * read roots rather than mutating `target`).
 */
/**
 * Bridge the persisted {@link ProjectConfig.features} map onto the generic
 * `SUPERINFERENCE_FEATURE_<NAME>` env vars that the capability gates in
 * `tools/tool-gates.ts` (`isFeatureEnabled` / the default-ON subsystem gates)
 * read, so a config-file feature toggle actually gates tool registration without
 * threading `config` through every `ToolDefinition.isEnabled()` closure. This is
 * the config-side consumer that makes the otherwise-inert `features` map live
 * (parity: Claude Code's `features` map / `feature()`).
 *
 * An already-set env override wins (operator intent beats the file), matching
 * every other `applyConfig*` bridge and the `features` doc comment
 * ("`SUPERINFERENCE_FEATURE_<NAME>` / `CLAUDE_CODE_FEATURE_<NAME>` still win").
 * Non-boolean entries are ignored.
 */
export function applyConfigFeatureFlags(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): void {
  const features = config?.features;
  if (!features) return;
  for (const [name, enabled] of Object.entries(features)) {
    if (typeof enabled !== 'boolean') continue;
    const envName = featureFlagEnvName(name);
    const aliasName = featureFlagEnvName(name, 'CLAUDE_CODE');
    if (target[envName] !== undefined || target[aliasName] !== undefined) continue;
    target[envName] = enabled ? '1' : '0';
  }
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
}

/**
 * Resolve the persisted {@link ProjectConfig.additionalDirectories} to a de-duped list
 * of absolute directory paths to add to the tool-access workspace (parity: Claude
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
 * affordance, resolved off the already-MERGED {@link ProjectConfig}. Parity: Claude
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
 * resolved off the already-MERGED {@link ProjectConfig}. Parity: Claude Code's
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
 * {@link ProjectConfig}. Parity: Claude Code's `isVoiceModeEnabled()` reading
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
 * resolved off the already-MERGED {@link ProjectConfig}. Parity: Claude Code's
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
 * Parity: Claude Code's `diffTool` setting (`'terminal' | 'auto'`, default `'auto'`).
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
 * {@link ProjectConfig}. Parity: Claude Code's `resolveDefaultShell()` reading
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
 * Milliseconds after which a cached {@link ProjectConfig.exampleFiles} list is considered
 * stale (one week). Parity: Claude Code's `ONE_WEEK_IN_MS` staleness window in
 * `refreshExampleCommands`.
 */
export const EXAMPLE_FILES_TTL_MS = 7 * 24 * 60 * 60 * 1000;

/**
 * The cached frequently-modified example files, resolved off the already-MERGED
 * {@link ProjectConfig} with a staleness check. Parity: Claude Code's `exampleFiles` +
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
 * Pick the startup announcement to show, resolved off the already-MERGED
 * {@link ProjectConfig}. Parity: Claude Code's LogoV2 `companyAnnouncements` pick — on
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
 * already-MERGED {@link ProjectConfig}. Parity: Claude Code's
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
 * Parity: Claude Code's migration/effort write paths that store `undefined` when the
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
 * Parity: Claude Code's `EffortCallout` persisting `value === defaultLevel ? undefined :
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
 * Idempotent, guarded rename of a single config KEY (parity: Claude Code's
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
 * and formatting (parity: Claude Code's `addItemToJSONCArray`, which uses jsonc-parser
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

/** Locate the first top-level `[ … ]` in a JSONC string (string/comment aware). */
function findTopLevelArrayBrackets(
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
function insertIntoJsoncArray(
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
// Unified tool-output spill threshold (parity: Claude Code toolResultStorage.ts)
// ---------------------------------------------------------------------------

/**
 * Built-in GLOBAL default ceiling (chars) for a tool result before it is spilled to
 * a file. Parity: Claude Code's `DEFAULT_MAX_RESULT_SIZE_CHARS` — the single default
 * against which each tool's own declared cap is clamped, so one tunable value governs
 * every tool that has no bespoke cap (web_fetch, PDF extraction, …). Distinct from the
 * per-tool {@link ProjectConfig.bashOutputMaxChars}/{@link ProjectConfig.taskOutputMaxChars}
 * knobs, which stay their own thing.
 */
export const DEFAULT_TOOL_OUTPUT_MAX_CHARS = 50_000;

/** Env override for the global tool-output cap (validated + bounded), mirroring the
 *  per-tool `TASK_MAX_OUTPUT_LENGTH` env knob. */
export const TOOL_OUTPUT_MAX_CHARS_ENV = 'AMI_TOOL_OUTPUT_MAX_CHARS';

/** Hard upper bound for the resolved global default so a misconfigured/hostile override
 *  cannot push the spill threshold arbitrarily high (and effectively disable spilling). */
export const TOOL_OUTPUT_MAX_UPPER_LIMIT = 2_000_000;

/**
 * Resolve the effective spill threshold (chars) for a tool whose declared cap is
 * `declaredMaxChars`, clamping it by the GLOBAL default. The global default is an
 * explicit project `toolOutputMaxChars` config value when valid (`configured`), else
 * the {@link TOOL_OUTPUT_MAX_CHARS_ENV} env knob, else {@link DEFAULT_TOOL_OUTPUT_MAX_CHARS}
 * — each clamped into `[1, TOOL_OUTPUT_MAX_UPPER_LIMIT]`. The tool's own declared cap is
 * then clamped DOWN by that global (`Math.min`), so one knob lowers every tool's spill
 * threshold while a tool asking for LESS keeps its tighter bound.
 *
 * A non-finite `declaredMaxChars` is a deliberate opt-out (parity: Claude's
 * `getPersistenceThreshold` returns `Infinity` unchanged — never spill).
 */
export function resolveToolOutputMaxChars(declaredMaxChars: number, configured?: number): number {
  if (!Number.isFinite(declaredMaxChars)) return declaredMaxChars;
  let globalDefault: number;
  if (configured !== undefined && Number.isFinite(configured) && configured > 0) {
    globalDefault = Math.min(Math.floor(configured), TOOL_OUTPUT_MAX_UPPER_LIMIT);
  } else {
    globalDefault = validateBoundedIntEnvVar(TOOL_OUTPUT_MAX_CHARS_ENV, process.env[TOOL_OUTPUT_MAX_CHARS_ENV], {
      default: DEFAULT_TOOL_OUTPUT_MAX_CHARS,
      min: 1,
      max: TOOL_OUTPUT_MAX_UPPER_LIMIT,
    }).effective;
  }
  return Math.min(declaredMaxChars, globalDefault);
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

/** Structured result of a FULL (schema-aware) config-content validation. */
export interface ConfigFileValidation {
  /** True only when the content parses AND has no field-value issues or unknown keys. */
  valid: boolean;
  /** Set when the content is not a parseable JSON object (parse/shape failure). */
  parseError?: string;
  /** Per-field value issues (wrong type / bad enum / out-of-range), empty when none. */
  issues: ConfigValueIssue[];
  /** Unrecognized top-level keys (likely typos), empty when none. */
  unknownKeys: string[];
  /** The full generated JSON schema, returned so an editor/agent can self-correct. */
  schema: ConfigJSONSchema;
}

/**
 * STRICT, schema-aware validation of raw config-file CONTENT, for the edit/check path.
 * Unlike {@link validateConfigJson} — the lightweight parse+object gate used by LOADERS,
 * which deliberately accepts a parseable-but-advisory config so the valid parts still
 * load — this performs field-level value validation (wrong type / bad enum / out-of-range
 * via {@link validateProjectConfigValues}) AND unknown-key detection
 * ({@link findUnknownConfigKeys}), and returns the full {@link getConfigJSONSchema}
 * output so a caller can show the user/agent exactly what shape is expected and repair
 * the file. Parity: Claude Code's `validateSettingsFileContent` (strict-mode schema
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
      parseError: `Invalid JSON: ${err instanceof Error ? err.message : String(err)}. ${JSON_SYNTAX_RECOVERY_HINT}`,
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

// ---------------------------------------------------------------------------
// Global-config write-count introspection (parity: Claude's globalConfigWriteCount
// / CONFIG_WRITE_DISPLAY_THRESHOLD). A pathological write RATE to the shared global
// config (a save loop, two fighting instances) is the classic precursor to a
// corrupt `~/.ami/config.json`; before this it was invisible until the file broke.
// The per-process counter + exported getter make the churn observable, and a
// one-time warning is logged the moment the rate crosses the display threshold.
// ---------------------------------------------------------------------------

/**
 * Write-rate at which an unusually high global-config churn is surfaced (parity:
 * Claude Code's CONFIG_WRITE_DISPLAY_THRESHOLD=20). Reached once per process, a
 * single warning is logged — a steady trickle of saves never spams.
 */
export const CONFIG_WRITE_DISPLAY_THRESHOLD = 20;

/** Number of successful writes to the GLOBAL config file this process. */
let globalConfigWriteCount = 0;

/**
 * How many times the GLOBAL config (`~/.ami/config.json`) has been written this
 * process. Introspection surface over config churn (parity: Claude's
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
  // Count a successful write to the shared GLOBAL config so pathological churn
  // (a save loop, two fighting instances) is observable before it corrupts the file.
  recordGlobalConfigWrite(target);
  // Single invalidation point (parity: Claude's resetSettingsCache fired on write):
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
 * Parity: Claude Code's InvalidConfigDialog "Reset with default configuration" branch,
 * which writes `error.defaultConfig` over the broken file.
 */
export const DEFAULT_PROJECT_CONFIG: Readonly<ProjectConfig> = Object.freeze({});

/** Outcome of a {@link resetConfigToDefault} self-heal. */
export interface ResetConfigResult {
  /** The config file that was (re)written. */
  path: string;
  /**
   * Where the previous bytes were quarantined (`<file>.corrupt-<ts>`, content-deduped),
   * or null when the file was absent or the quarantine write itself failed.
   */
  backupPath: string | null;
  /** True when a pre-existing file's bytes were captured (and backed up) before the reset. */
  hadExisting: boolean;
}

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
 * Parity: Claude Code's InvalidConfigDialog "Reset with default configuration" branch.
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

/**
 * Read the persisted per-skill usage map ({@link ProjectConfig.skillUsage}) from the
 * GLOBAL config, or an empty map when none exists / the path is unavailable. Used at
 * session start to seed a skill's cross-session frecency ranking. Best-effort: any read
 * failure yields `{}` (a missing history must never block startup). Parity: Claude
 * Code's `getGlobalConfig().skillUsage` read.
 */
export function loadPersistedSkillUsage(): Record<string, SkillUsageStat> {
  try {
    return loadGlobalConfig()?.skillUsage ?? {};
  } catch {
    return {};
  }
}

/**
 * Record one invocation of `name` into the GLOBAL config's {@link ProjectConfig.skillUsage}
 * map (bumping `usageCount`, stamping `lastUsedAt = now`) via the lock-safe
 * read-modify-write {@link updateConfigFile}, so cross-process writers never lose each
 * other's counts. Returns `true` when the update was written, `false` when there is no
 * writable global-config path or the write was skipped/failed (best-effort — a usage
 * write must never surface as an error to the caller). The increment formula is the ONE
 * canonical {@link mergeSkillUsageStat}. Parity: Claude Code's `saveGlobalConfig` on each
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
 * Debounce window for {@link recordSkillUsageDebounced}: a skill usage written less
 * than this long ago skips the (lock + read + parse + write) global-config round-trip
 * on the next invocation. Parity with Claude Code's `SKILL_USAGE_DEBOUNCE_MS`
 * (utils/suggestions/skillUsageTracking.ts) — the frecency ranking uses a 7-day
 * half-life, so sub-minute write granularity is irrelevant and the hot-repeat lock+I/O
 * is pure overhead.
 */
export const SKILL_USAGE_DEBOUNCE_MS = 60_000;

/**
 * Record one invocation of `name` for frecency ranking, DEBOUNCED against the
 * caller-owned `lastWriteBySkill` map so a skill invoked repeatedly in quick succession
 * does not take the global-config lock + read + parse + write on every call. When the
 * previous recorded write for this skill was under {@link SKILL_USAGE_DEBOUNCE_MS} ago,
 * it bails BEFORE touching disk and returns `false`; otherwise it stamps the map and
 * delegates the actual read-modify-write to {@link persistSkillUsageStat}, returning
 * that function's `written` verdict. Best-effort (never throws).
 *
 * The debounce cache is passed IN (not module-global as in Claude Code) so each owner
 * scopes it to its own lifetime — AMI creates many `SkillManager`s per process (sessions,
 * forked sub-engines), and an instance-scoped map keeps independent sessions from
 * suppressing each other's first write. Parity with Claude Code's `recordSkillUsage`
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
 * path and null-checks it in ONE place (parity: Claude Code's `saveGlobalConfig(updater)`
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
  try {
    return loadGlobalConfig()?.preferencesShown ?? {};
  } catch {
    return {};
  }
}

/**
 * True iff the named one-time preference/dialog has already been marked shown (parity:
 * Claude Code's `shouldShow*Dialog` guards gating on a persisted `has*BeenShown` flag).
 * An empty name is never "shown". Best-effort read (see {@link loadPreferenceFlags}).
 */
export function wasPreferenceShown(name: string): boolean {
  if (!name) return false;
  return loadPreferenceFlags()[name] === true;
}

/**
 * Mark a one-time preference/dialog as shown in the GLOBAL config via the lock-safe
 * {@link saveGlobalConfig}, so a first-run prompt/notice is surfaced at most once and the
 * flag survives across sessions (parity: Claude Code writing `hasIdeAutoConnectDialogBeenShown`
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
  try {
    const v = (config ?? loadGlobalConfig())?.lastReleaseNotesSeen;
    return typeof v === 'string' && v.trim() ? v : undefined;
  } catch {
    return undefined;
  }
}

/**
 * Whether a one-time "what's new" release-notes banner should be shown for
 * `currentVersion` — i.e. the persisted {@link ProjectConfig.lastReleaseNotesSeen}
 * differs from it (parity: Claude Code's `config.lastReleaseNotesSeen !== VERSION` gate
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
 * (parity: Claude Code writing `lastReleaseNotesSeen = VERSION` after the banner is
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
  try {
    return loadGlobalConfig()?.noticeSeenCounts ?? {};
  } catch {
    return {};
  }
}

/**
 * The persisted seen-count for a count-limited notice (parity: Claude Code's persisted
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
 * below `maxCount` (parity: Claude Code's `shouldShow = seenCount < MAX_SHOW_COUNT` gate).
 * A non-positive `maxCount` or empty name means "never show".
 */
export function shouldShowNotice(name: string, maxCount: number): boolean {
  if (!name || !(maxCount > 0)) return false;
  return getNoticeSeenCount(name) < maxCount;
}

/**
 * Increment the persisted seen-count for a count-limited notice via the lock-safe
 * {@link saveGlobalConfig}, capped at `maxCount` so it never grows unbounded (parity:
 * Claude Code's concurrency-safe seen-count increment). The increment is computed from
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
