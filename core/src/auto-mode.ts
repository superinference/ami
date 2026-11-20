/**
 * Auto-mode (YOLO) natural-language classifier rules.
 *
 * Parity with the reference's `settings.autoMode` / `yoloClassifier`: when the user
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
import { classifySafetyCheckFailure, type SafetyClassifierFailure } from './error-classifier';

/**
 * The three classifier prompt sections, as REQUIRED arrays (empty when a section
 * has no rules). The JSON-dump / effective-config shape. Parity: the reference's
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
 * describe the baseline environment. Parity: the reference's
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
 * applying the reference's per-section REPLACE semantics: a non-empty user section
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
 * orchestration layer. Parity: the reference's external auto-mode system prompt with the
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
 * Parity: the reference's `AutoModeUnavailableReason` (permissionSetup.ts L1045):
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
 * available. Checks in the reference's fixed precedence (settings disable → circuit
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
 * shown once when the user tries to switch into it. Parity: the reference's
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

// ---------------------------------------------------------------------------
// Auto-mode classifier outcome — fail-safe resolution when the LLM call that
// consumes buildAutoModeClassifierPrompt errors / times out / overflows context
// ---------------------------------------------------------------------------

/**
 * The final per-tool-call decision the engine takes from an auto-mode classifier
 * outcome: either silently `auto-approve` the tool call, or `prompt` the user for
 * explicit confirmation. There is no "auto-deny" — a classifier only ever
 * downgrades an auto-approval to a prompt (a HARD deny is a separate permission
 * rule that runs before the classifier).
 */
export type AutoModeClassifierResolution = 'auto-approve' | 'prompt';

/**
 * Outcome of running the auto-mode security classifier for one tool call. Exactly
 * one of the two cases is meaningful:
 *  - `approved` set — the classifier produced a verdict (`true` = auto-approve,
 *    `false` = require confirmation);
 *  - `failure` set — the classifier call itself failed (API error / timeout /
 *    transcript overflow) and produced NO verdict (see {@link SafetyClassifierFailure}).
 * An empty outcome (neither set) is treated as indeterminate → prompt.
 */
export interface AutoModeClassifierOutcome {
  approved?: boolean;
  failure?: SafetyClassifierFailure;
}

/**
 * Resolve an {@link AutoModeClassifierOutcome} to the fail-safe permission
 * decision. The contract mirrors the reference's documented classifier-failure
 * handling: a check that could not produce a verdict falls back to normal
 * PROMPTING — it is NEVER silently auto-approved (fail-open is unsafe) and NEVER
 * hard-denied (fail-closed would block legitimate work). Only an explicit
 * classifier approval auto-approves; everything else prompts.
 */
export function resolveAutoModeClassifierOutcome(
  outcome: AutoModeClassifierOutcome,
): AutoModeClassifierResolution {
  // Fail-safe: any failure (unavailable / transcriptTooLong / timeout) → prompt.
  if (outcome.failure) return 'prompt';
  return outcome.approved === true ? 'auto-approve' : 'prompt';
}

/**
 * Build an {@link AutoModeClassifierOutcome} for a FAILED classifier call. Wraps
 * {@link classifySafetyCheckFailure} so the engine's classifier call site turns a
 * caught error (with optional measured `durationMs`) into the fail-safe outcome in
 * one step, instead of re-deriving the failure contract at the call site.
 */
export function makeAutoModeClassifierFailure(
  error: string | Error | unknown,
  opts?: { durationMs?: number },
): AutoModeClassifierOutcome {
  return { failure: classifySafetyCheckFailure(error, opts) };
}
