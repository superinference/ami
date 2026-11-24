// Split from config.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { DISABLE_BACKGROUND_TASKS_ENV, DISABLE_BACKGROUND_TASKS_ENV_COMPAT, featureFlagEnvName } from './tools/tool-gates';
import { WORKFLOW_SIZE_ENV, normalizeWorkflowSize } from './tools/workflow-size';
import type { ProjectConfig } from './config';

/**
 * Bridge the persisted {@link ProjectConfig.language} knob into the
 * `SUPERINFERENCE_LANGUAGE` env var that {@link buildResponseLanguageSection}
 * (the single response-language consumer) already reads — so a user can persist a
 * preferred response language in project/user config instead of exporting the env
 * var before every launch (parity: the reference's `LanguagePicker` persisted setting).
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

/** Anthropic beta token for the `fast-mode` opt-in (parity: the reference's betas). */
export const FAST_MODE_BETA = 'fast-mode-2026-02-01';

/** Anthropic beta token for the `redact-thinking` request opt-in (parity: the reference). */
export const REDACT_THINKING_BETA = 'redact-thinking-2026-02-12';

/**
 * Bridge the persisted {@link ProjectConfig.respectGitignore} knob into the existing
 * `AMI_GLOB_NO_IGNORE` env knob that {@link import('./tools/glob').globRespectsGitignore}
 * already reads — so a user can persist the file-picker gitignore preference in
 * project/user config instead of exporting the env var (parity: the reference's
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
 * user can persist "turn auto-compaction off" in project/user config (parity: the reference
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
 * the reference's `plansDirectory`). An explicit `AMI_PLANS_DIR` already in `target`
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
 * the reference's `fast-mode` / `redact-thinking` beta knobs). The `redact-thinking` token
 * is armed by EITHER {@link ProjectConfig.redactThinking} `=== true` OR the inverse
 * {@link ProjectConfig.showThinkingSummaries} `=== false` (parity: the reference gates it on
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
  // showThinkingSummaries knob being turned OFF (parity: the reference gates
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
 * bar" in project/user config instead of exporting the env var (parity: the reference's
 * `terminalProgressBarEnabled` setting). An explicit `AMI_NO_PROGRESS` already in
 * `target` WINS (one-shot override); otherwise `terminalProgressBarEnabled === false`
 * arms the kill-switch. Unset/`true` is a no-op (progress stays on — AMI's default; a
 * superset divergence from the reference's default-off, keeping AMI's loved default while
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
 * instead of exporting the env var (parity: the reference's `showTurnDuration` setting).
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
 * no-op (concise default). Parity: the reference's persisted `verbose` global-config knob.
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
 * — the default). Parity: the reference's `copyOnSelect ?? true` global-config knob.
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
 * an env var every session (parity: the reference's `isFullscreenEnvEnabled()` layout
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
 * Unset/`false` is a no-op (highlighting stays on). Parity: the reference's
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
 * default). Parity: the reference's numeric `messageIdleNotifThresholdMs` (default 60000).
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
 * the reference's cosmetic `spinnerVerbs` config knob.
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
 * (parity: the reference's `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS`). `backgroundTasksEnabled
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
 * Bridge the persisted {@link ProjectConfig.dynamicWorkflowSize} knob into the
 * AMI_DYNAMIC_WORKFLOW_SIZE env knob the workflow tool (description guideline) and
 * the /flows progress tree ("Large workflow" warning) read. An explicit env value
 * already in `target` WINS; otherwise a valid persisted size seeds it. An invalid
 * value is ignored (the resolver falls back to the medium default). Parity: the
 * reference's "Dynamic workflow size" /config setting.
 */
export function applyConfigWorkflowSize(
  config: ProjectConfig | null | undefined,
  target: NodeJS.ProcessEnv = process.env,
): void {
  if (target[WORKFLOW_SIZE_ENV] !== undefined) return;
  const size = normalizeWorkflowSize(config?.dynamicWorkflowSize);
  if (size) target[WORKFLOW_SIZE_ENV] = size;
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
 * (parity: the reference's `features` map / `feature()`).
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
