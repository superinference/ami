/**
 * Privacy-level diagnostics built on the canonical resolver in `./analytics`.
 *
 * The ordered privacy tiers (`default < no-telemetry < essential-traffic`), the
 * `getPrivacyLevel()` resolver, and the `isTelemetryDisabled()` /
 * `isEssentialTrafficOnly()` query helpers live in `./analytics` (single source
 * of truth, consumed at the OTLP send site and by the cost-tracker). This module
 * adds the *reason* accessor that pairs with the `essential-traffic` boundary so a
 * network-gated feature can tell the user WHICH env var to unset to re-enable it
 * (mirrors Claude Code's `getEssentialTrafficOnlyReason`, privacyLevel.ts L46-L55).
 */
import { isEnvTruthy, ESSENTIAL_TRAFFIC_VARS } from './analytics';

/**
 * The name of the env var responsible for the current `essential-traffic`
 * restriction, or `null` when nonessential traffic is not suppressed. Lets a
 * feature that was skipped for privacy reasons surface an actionable
 * "unset <VAR> to re-enable" message instead of failing silently.
 *
 * Iterates {@link ESSENTIAL_TRAFFIC_VARS} in the same order the resolver checks
 * them, so the returned name is exactly the knob that raised the level.
 */
export function getEssentialTrafficOnlyReason(
  env: NodeJS.ProcessEnv = process.env,
): string | null {
  for (const name of ESSENTIAL_TRAFFIC_VARS) {
    if (isEnvTruthy(env[name])) return name;
  }
  return null;
}
