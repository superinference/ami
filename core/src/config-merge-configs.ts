// Split from config.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';
import { filterSettingsEnv, isHostManagedProvider, type EnvTrustTier, type EnvRejectReason } from './env-protection';
import { TIER_LOCKED_KEYS } from './settings-guard';
import { deepMergeConfigObjects, mergeConfigArray, mergeMcpAllowlist } from './config-validate-project-config-values';
import type { ProjectConfig } from './config';

/**
 * Injectable env-application seam used by {@link ConfigService.reload}.
 * Production delegates to {@link applyConfigEnv}; the default is wired once
 * that function is defined (same-module forward ref). Tests inject a thrower
 * to force the "runtime env re-application failed" catch.
 */
export const configEnvHooks: {
  applyConfigEnv: (
    layers: ConfigEnvLayers,
    target?: NodeJS.ProcessEnv,
  ) => ApplyConfigEnvResult;
} = {
  // Placeholder until {@link applyConfigEnv} is assigned below — never called
  // during module init (ConfigService.reload runs after the full module loads).
  applyConfigEnv: ((_layers, _target) => {
    throw new Error('configEnvHooks.applyConfigEnv not yet wired');
  }) as (
    layers: ConfigEnvLayers,
    target?: NodeJS.ProcessEnv,
  ) => ApplyConfigEnvResult,
};

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
 * `flagConfig` (parity: the reference's `flagSettings`) layers a one-off settings file
 * ABOVE local/project/user but BELOW the managed-policy overrides applied after
 * the loop. `pluginConfig` (parity: the reference's settingsCache plugin base) is the
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
    if (layer.maxSkillProgressMessages !== undefined) merged.maxSkillProgressMessages = layer.maxSkillProgressMessages;
    if (layer.deferTools !== undefined) merged.deferTools = layer.deferTools;
    // Array-valued settings ACCUMULATE across layers (concat + dedupe), so a
    // higher layer augments rather than clobbers a lower one — parity with
    // the reference's settingsMergeCustomizer. Managed-authoritative arrays
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
    // more exclusions, never removes a lower layer's) — parity with the reference's
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
    // Preference / background-deferral tunable knobs: last-layer-wins scalars (parity with
    // the other scalar settings above). The deepLinkTerminal preference and the two
    // background slow-op deferral windows are read off the merged config by their
    // resolvers; declared + copied here so a file-tier value is not silently dropped.
    // (The durable per-project STATE fields — lastSessionMetrics + the onboarding fatigue
    // counters — are read directly from the project config file by their helpers, like
    // noticeSeenCounts/preferencesShown, so they are intentionally NOT merged here.)
    if (layer.deepLinkTerminal !== undefined) merged.deepLinkTerminal = layer.deepLinkTerminal;
    if (layer.backgroundSlowOpDelayMs !== undefined) merged.backgroundSlowOpDelayMs = layer.backgroundSlowOpDelayMs;
    if (layer.backgroundInteractionRecencyMs !== undefined) merged.backgroundInteractionRecencyMs = layer.backgroundInteractionRecencyMs;
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
  // managed policy is the sole source. Parity: the reference's disableAllHooks /
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
  // managed policy is the sole source. Parity: the reference's enabledPlugins policy.
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
  // the reference's deniedMcpServers precedence (a server denied at any tier stays denied).
  if (managedConfig?.deniedMcpServers) {
    merged.deniedMcpServers = mergeConfigArray(managedConfig.deniedMcpServers, merged.deniedMcpServers ?? []);
  }
  if (managedConfig?.managedMcpServers) {
    merged.managedMcpServers = managedConfig.managedMcpServers;
  }
  // Sandbox exemptions weaken the container sandbox, so when managed policy owns
  // them a lower (project/local) layer must not be able to add its own — parity:
  // the reference's areSandboxSettingsLockedByPolicy refuses local sandbox changes once a
  // higher-priority config owns the setting. (When managed does NOT set it, a
  // project may still exempt commands, so this only overrides when present.)
  if (managedConfig?.sandboxExcludedCommands) {
    merged.sandboxExcludedCommands = managedConfig.sandboxExcludedCommands;
  }
  // Managed sandbox-egress lockdown is MANAGED-authoritative: only the admin tier
  // may force domains-only egress; a lower tier can never set (or clear) it. Parity:
  // the reference's shouldAllowManagedSandboxDomainsOnly() policy knob.
  if (managedConfig?.allowManagedSandboxDomainsOnly) {
    merged.allowManagedSandboxDomainsOnly = true;
  }
  // Managed feature-restriction map is AUTHORITATIVE: a lower tier may ADD denies for
  // new policies (handled by the deep-merge in the loop), but a managed deny can never
  // be RELAXED by a lower layer re-allowing it. Re-apply the managed entries ON TOP so
  // each managed `{ allowed }` wins per-policy (deny-wins / admin-wins). Parity: the reference
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
  // source — no second lock list). Parity: the reference's policySettings being the
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
 * Normalize a directory path into the canonical, absolute form used as a trusted-path
 * key / ancestor-walk node (parity: the reference's `normalizePathForConfigKey`). A single
 * normalizer so a path listed in {@link ProjectConfig.trustedProjectPaths} and the `cwd`
 * being checked compare identically regardless of `.`/`..`/trailing-separator spelling.
 */
export function normalizeTrustPath(p: string): string {
  return path.resolve(p);
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
