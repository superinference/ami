/**
 * Auto-mode (YOLO) natural-language classifier rules.
 *
 * Parity with Claude Code's `settings.autoMode` / `yoloClassifier`: when the user
 * runs fully-autonomous ("auto"/YOLO) mode, a security classifier decides whether
 * each tool call is auto-approved or bounced back for confirmation. The classifier
 * reads THREE declarative, plain-English rule categories as part of its system
 * prompt:
 *
 *  - `allow`       — actions the classifier should auto-approve;
 *  - `soft_deny`   — actions the classifier should block (require confirmation) —
 *                    distinct from a HARD deny (which never runs at all);
 *  - `environment` — context about the user's setup that helps the classifier decide.
 *
 * AMI already had static glob knobs (`autoAllowPatterns` / `--allow` / `--deny`)
 * and the fixed {@link PermissionMode} set, but NO `soft_deny` category and no
 * classifier-readable natural-language rules. This module adds the declarative
 * data model + the deterministic prompt-assembly and merge semantics (the pure,
 * testable heart of the feature); the actual LLM call that consumes
 * {@link buildAutoModeClassifierPrompt} is wired by the engine/common layer.
 *
 * `AutoModeConfig` (the user-settings shape) lives in {@link ./types}; this module
 * is the consumer that turns it into the effective, defaults-merged rule set.
 */

import type { AutoModeConfig } from './types';

/**
 * The three classifier prompt sections, as REQUIRED arrays (empty when a section
 * has no rules). The JSON-dump / effective-config shape. Parity: Claude's
 * `AutoModeRules`.
 */
export interface AutoModeRules {
  allow: string[];
  soft_deny: string[];
  environment: string[];
}

/**
 * Built-in default classifier rules used when the user has not overridden a
 * section. Conservative by design: auto-approve clearly read-only / local-only
 * work, soft-deny anything that leaves the machine or mutates shared state, and
 * describe the baseline environment. Parity: Claude's
 * `getDefaultExternalAutoModeRules()` (which parses these out of the bundled
 * external permissions template); AMI inlines them as data.
 */
export const AUTO_MODE_DEFAULT_RULES: Readonly<AutoModeRules> = Object.freeze({
  allow: [
    'Reading files, listing directories, and searching code within the workspace',
    'Running read-only shell commands that do not modify files or state',
    'Creating or editing files inside the current working directory',
  ],
  soft_deny: [
    'Deleting files or directories, or any destructive filesystem operation',
    'Commands that send data over the network or install software',
    'Writing outside the workspace, or to credential, config, or system paths',
    'Git history rewrites, force pushes, or resetting/discarding the worktree',
  ],
  environment: [
    'A software engineering workspace; the user is iterating on a code project.',
  ],
});

/** A deep copy of the frozen defaults, so callers can mutate the result safely. */
function cloneDefaults(): AutoModeRules {
  return {
    allow: [...AUTO_MODE_DEFAULT_RULES.allow],
    soft_deny: [...AUTO_MODE_DEFAULT_RULES.soft_deny],
    environment: [...AUTO_MODE_DEFAULT_RULES.environment],
  };
}

/**
 * The effective default classifier rules (no user overrides). Parity:
 * `getDefaultExternalAutoModeRules()`.
 */
export function getDefaultExternalAutoModeRules(): AutoModeRules {
  return cloneDefaults();
}

/**
 * Resolve the EFFECTIVE classifier rules from a user {@link AutoModeConfig},
 * applying Claude's per-section REPLACE semantics: a non-empty user section
 * REPLACES that section's defaults entirely; an empty or absent section falls
 * through to the defaults. (It is a replace, NOT a merge — the user owns a
 * section once they define it.) Parity: `autoModeConfigHandler` /
 * `buildYoloSystemPrompt` section resolution.
 */
export function resolveAutoModeRules(config?: AutoModeConfig | null): AutoModeRules {
  const defaults = cloneDefaults();
  if (!config) return defaults;
  return {
    allow: config.allow && config.allow.length > 0 ? [...config.allow] : defaults.allow,
    soft_deny:
      config.soft_deny && config.soft_deny.length > 0 ? [...config.soft_deny] : defaults.soft_deny,
    environment:
      config.environment && config.environment.length > 0
        ? [...config.environment]
        : defaults.environment,
  };
}

/** Render one rule section as a markdown bullet list, or `(none)` when empty. */
function renderSection(items: string[]): string {
  if (items.length === 0) return '(none)';
  return items.map((item) => `- ${item}`).join('\n');
}

/**
 * Assemble the security-classifier system prompt from the effective rule set. The
 * classifier (an LLM, invoked by the engine) reads this to decide, per tool call,
 * whether to auto-approve or require user confirmation. Pure + deterministic so it
 * can be snapshot-tested and diffed; the LLM call that consumes it lives in the
 * orchestration layer. Parity: Claude's external auto-mode system prompt with the
 * three `<user_*_to_replace>` sections substituted.
 */
export function buildAutoModeClassifierPrompt(rules: AutoModeRules): string {
  return [
    'You are a security classifier for an autonomous coding agent running in auto mode.',
    'For each proposed tool call, decide whether it may be auto-approved or must be',
    'bounced back to the user for explicit confirmation. Respond with "allow" to',
    'auto-approve, or "deny" to require confirmation.',
    '',
    'Auto-approve (allow) actions that match these rules:',
    renderSection(rules.allow),
    '',
    'Require confirmation (soft deny) for actions that match these rules:',
    renderSection(rules.soft_deny),
    '',
    "Environment context about the user's setup:",
    renderSection(rules.environment),
    '',
    'When a tool call is ambiguous or matches no rule, prefer requiring confirmation.',
  ].join('\n');
}

/**
 * Convenience: resolve a user config to effective rules AND render the classifier
 * prompt in one step. The single entry point the engine calls when entering auto
 * mode.
 */
export function buildAutoModeClassifierPromptFromConfig(config?: AutoModeConfig | null): string {
  return buildAutoModeClassifierPrompt(resolveAutoModeRules(config));
}

// ---------------------------------------------------------------------------
// Auto-mode availability gating — why an elevated autonomy mode is unavailable
// ---------------------------------------------------------------------------

/**
 * Why an elevated "auto"/autonomy permission mode is unavailable when the user
 * tries to switch into it (e.g. the Shift+Tab carousel wraps past the auto slot).
 * Parity: Claude's `AutoModeUnavailableReason` (permissionSetup.ts L1045):
 *  - `settings`        — auto mode is turned off by a (user or managed) settings flag;
 *  - `circuit-breaker` — auto mode is suspended because too many actions were denied
 *                        (the permission denial circuit breaker has tripped);
 *  - `model`           — the active model does not support unattended auto mode.
 */
export const AUTO_MODE_UNAVAILABLE_REASONS = ['settings', 'circuit-breaker', 'model'] as const;
export type AutoModeUnavailableReason = (typeof AUTO_MODE_UNAVAILABLE_REASONS)[number];

/**
 * The observable state that decides whether auto mode can be entered. Passed in
 * explicitly (never read from globals) so the classifier stays pure and
 * unit-testable; the engine/manager supplies the live values.
 */
export interface AutoModeAvailabilityState {
  /** Auto mode is disabled by a settings flag (user opt-out or managed policy). */
  disabledBySettings: boolean;
  /** The denial circuit breaker has tripped (too many recent/consecutive denials). */
  circuitBroken: boolean;
  /** The active model supports unattended auto mode. */
  modelSupportsAutoMode: boolean;
}

/**
 * Classify WHY auto mode is unavailable for the given state, or `null` when it is
 * available. Checks in Claude's fixed precedence (settings disable → circuit
 * breaker → model capability), mirroring `getAutoModeUnavailableReason`.
 */
export function classifyAutoModeUnavailableReason(
  state: AutoModeAvailabilityState,
): AutoModeUnavailableReason | null {
  if (state.disabledBySettings) return 'settings';
  if (state.circuitBroken) return 'circuit-breaker';
  if (!state.modelSupportsAutoMode) return 'model';
  return null;
}

/**
 * Human-readable, reason-tagged notification text for an unavailable auto mode,
 * shown once when the user tries to switch into it. Parity: Claude's
 * `getAutoModeUnavailableNotification`.
 */
export function getAutoModeUnavailableNotification(reason: AutoModeUnavailableReason): string {
  switch (reason) {
    case 'settings':
      return 'Auto mode is disabled by settings';
    case 'circuit-breaker':
      return 'Auto mode paused after too many denied actions — review and resume manually';
    case 'model':
      return 'Auto mode is unavailable for the current model';
  }
}
