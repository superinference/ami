// ---------------------------------------------------------------------------
// Privacy-level resolution + telemetry opt-out gating
// ---------------------------------------------------------------------------
//
// Single canonical resolver for AMI's privacy posture. Every outbound /
// nonessential-traffic site (analytics/OTLP telemetry, model-capability refresh,
// cost reporting, …) consults these helpers so the standard opt-out env vars
// actually suppress egress. Mirrors Claude Code's `utils/privacyLevel` contract
// (default < no-telemetry < essential-traffic; most-restrictive signal wins),
// extended with AMI-native env-var names.

/** Standard env-flag truthiness: anything set and not '', '0', 'false', 'no', 'off'. */
export function isEnvTruthy(v: string | undefined): boolean {
  if (v === undefined) return false;
  const s = v.trim().toLowerCase();
  return s !== '' && s !== '0' && s !== 'false' && s !== 'no' && s !== 'off';
}

/** Ordered privacy tiers; a more-restrictive signal always wins. */
export const PRIVACY_LEVELS = ['default', 'no-telemetry', 'essential-traffic'] as const;
export type PrivacyLevel = (typeof PRIVACY_LEVELS)[number];
const PRIVACY_RANK: Record<PrivacyLevel, number> = {
  'default': 0,
  'no-telemetry': 1,
  'essential-traffic': 2,
};

/**
 * Env vars that raise the privacy level to `essential-traffic`, in resolution
 * order (AMI-native knob first, then the Claude Code-compatible alias). Single
 * source of truth consumed by {@link getPrivacyLevel} and
 * {@link getEssentialTrafficOnlyReason}.
 */
export const ESSENTIAL_TRAFFIC_VARS = [
  'SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC',
  'CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC',
] as const;

/**
 * Resolve the effective privacy level from the standard opt-out env vars,
 * taking the MOST restrictive signal (mirrors Claude's getPrivacyLevel):
 *  - DISABLE_TELEMETRY / DO_NOT_TRACK → no-telemetry
 *  - *_DISABLE_NONESSENTIAL_TRAFFIC  → essential-traffic (strictest)
 */
export function getPrivacyLevel(env: NodeJS.ProcessEnv = process.env): PrivacyLevel {
  let level: PrivacyLevel = 'default';
  const bump = (l: PrivacyLevel): void => { if (PRIVACY_RANK[l] > PRIVACY_RANK[level]) level = l; };
  if (isEnvTruthy(env.DISABLE_TELEMETRY) || isEnvTruthy(env.DO_NOT_TRACK)) bump('no-telemetry');
  if (ESSENTIAL_TRAFFIC_VARS.some(name => isEnvTruthy(env[name]))) {
    bump('essential-traffic');
  }
  return level;
}

/** True when analytics/telemetry transmission must be suppressed (no-telemetry or stricter). */
export function isTelemetryDisabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return PRIVACY_RANK[getPrivacyLevel(env)] >= PRIVACY_RANK['no-telemetry'];
}

/** True when only strictly-essential (product-function) traffic may leave the machine. */
export function isEssentialTrafficOnly(env: NodeJS.ProcessEnv = process.env): boolean {
  return getPrivacyLevel(env) === 'essential-traffic';
}

/**
 * The env var responsible for the current essential-traffic-only restriction, or
 * null when nonessential traffic is allowed. Mirrors {@link getPrivacyLevel}'s
 * essential-traffic signals (SUPERINFERENCE first, then the Claude-compatible
 * alias), so a user-facing message can name EXACTLY which var to unset to
 * re-enable nonessential traffic (parity: privacyLevel.getEssentialTrafficOnlyReason).
 * Invariant: returns non-null iff {@link isEssentialTrafficOnly} is true.
 */
export function getEssentialTrafficOnlyReason(env: NodeJS.ProcessEnv = process.env): string | null {
  if (isEnvTruthy(env.SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC)) return 'SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC';
  if (isEnvTruthy(env.CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC)) return 'CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC';
  return null;
}

/** A resolved, introspectable snapshot of the current privacy posture. */
export interface PrivacyPosture {
  /** The effective ordered privacy level. */
  level: PrivacyLevel;
  /** True when analytics/telemetry transmission is suppressed. */
  telemetryDisabled: boolean;
  /** True when ALL nonessential network traffic is suppressed. */
  essentialTrafficOnly: boolean;
  /** Env var forcing essential-traffic-only (for "unset X to re-enable"), or null. */
  essentialTrafficOnlyReason: string | null;
}

/**
 * Resolve the full privacy posture as a single introspectable object. Consumed by
 * the `ami --introspect` feature manifest so the current posture — and the exact
 * env var to unset — is discoverable, honoring the repo's introspection invariant.
 */
export function getPrivacyPosture(env: NodeJS.ProcessEnv = process.env): PrivacyPosture {
  return {
    level: getPrivacyLevel(env),
    telemetryDisabled: isTelemetryDisabled(env),
    essentialTrafficOnly: isEssentialTrafficOnly(env),
    essentialTrafficOnlyReason: getEssentialTrafficOnlyReason(env),
  };
}
