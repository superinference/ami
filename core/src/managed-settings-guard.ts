import * as fs from 'fs';
import * as path from 'path';
import { atomicWriteFileSync } from './utils/fs-atomic';
import { resolveConfigDir } from './reference-journal';
import {
  extractDangerousSettings,
  hasDangerousSettings,
  hasDangerousSettingsChanged,
  formatDangerousSettingsList,
  type DangerousSettings,
  type DangerousSettingsInput,
} from './settings-guard';

// ---------------------------------------------------------------------------
// Managed-settings security guard (parity: Claude Code's
// services/remoteManagedSettings/securityCheck.tsx + ManagedSettingsSecurityDialog).
//
// A managed / remote settings file (e.g. /etc/superinference/config.json or an
// org drop-in) can silently introduce arbitrary-code-execution knobs: a
// `statusLine` / auth-helper shell command, an unknown env var that redirects
// the provider to an attacker host, or a hooks block. Claude Code gates these
// behind a consent dialog that re-prompts ONLY when the DANGEROUS subset of the
// config CHANGES — never nagging on an unchanged config.
//
// The low-level classifier (`extractDangerousSettings` / `hasDangerousSettings`
// / `hasDangerousSettingsChanged` / `formatDangerousSettingsList`) already lives
// in settings-guard.ts; this module is the orchestration + persistence layer
// that turns it into a real consumer:
//   - `reviewManagedSettings` decides whether the incoming managed config needs
//     review (no-dangerous / unchanged / needs-approval) against the last
//     approved snapshot;
//   - `loadManagedApproval` / `saveManagedApproval` persist that snapshot under
//     the data dir so the review is stable across runs (the "don't re-prompt
//     unless the dangerous set changed" behavior).
//
// The CLI consumer (`resolveManagedSettingsSecurity` in config-resolver.ts)
// surfaces a `needs-approval` review as a non-blocking advisory at startup and
// records the approval snapshot so an unchanged managed config never re-warns.
// ---------------------------------------------------------------------------

/** Outcome of comparing an incoming managed config against the approved snapshot. */
export const MANAGED_SETTINGS_DECISIONS = ['no-dangerous', 'unchanged', 'needs-approval'] as const;
export type ManagedSettingsDecision = (typeof MANAGED_SETTINGS_DECISIONS)[number];

export interface ManagedSettingsReview {
  decision: ManagedSettingsDecision;
  /** The dangerous subset of the incoming settings. */
  dangerous: DangerousSettings;
  /** Dangerous setting NAMES only (never values), for a UI/warning surface. */
  names: string[];
}

/**
 * Decide whether an incoming managed/remote config needs a fresh security review
 * versus the last-approved snapshot. Mirrors the branch order of Claude Code's
 * `checkManagedSettingsSecurity`:
 *   1. no dangerous settings in `next`            → `no-dangerous` (never prompt)
 *   2. dangerous subset unchanged vs `approved`   → `unchanged`    (don't nag)
 *   3. otherwise                                  → `needs-approval`
 * `names` is always the dangerous-name list of `next` (empty for `no-dangerous`).
 */
export function reviewManagedSettings(
  next: DangerousSettingsInput | null | undefined,
  approved: DangerousSettingsInput | null | undefined,
): ManagedSettingsReview {
  const dangerous = extractDangerousSettings(next);
  const names = formatDangerousSettingsList(dangerous);
  if (!hasDangerousSettings(dangerous)) {
    return { decision: 'no-dangerous', dangerous, names };
  }
  if (!hasDangerousSettingsChanged(approved, next)) {
    return { decision: 'unchanged', dangerous, names };
  }
  return { decision: 'needs-approval', dangerous, names };
}

/**
 * Absolute path of the persisted "last approved dangerous managed settings"
 * snapshot. Machine-scoped (alongside the managed config it guards) under the
 * resolved data dir (`~/.ami` by default; honors CLAUDE_CONFIG_DIR /
 * SUPERINFERENCE_DATA_DIR for test isolation).
 */
export function managedApprovalPath(env: NodeJS.ProcessEnv = process.env): string {
  return path.join(resolveConfigDir(env), 'managed-settings-approved.json');
}

/**
 * Load the last-approved dangerous-settings snapshot as a `DangerousSettingsInput`
 * (so it can be fed straight back into {@link reviewManagedSettings} as the
 * baseline). Returns null when no snapshot exists or it is unreadable/corrupt —
 * a missing/broken baseline simply means "nothing approved yet", which triggers
 * a fresh review rather than silently trusting the new config.
 */
export function loadManagedApproval(
  env: NodeJS.ProcessEnv = process.env,
): DangerousSettingsInput | null {
  const file = managedApprovalPath(env);
  let raw: string;
  try {
    raw = fs.readFileSync(file, 'utf-8');
  } catch {
    return null;
  }
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
    return parsed as DangerousSettingsInput;
  } catch {
    return null;
  }
}

/**
 * Persist the dangerous subset of `next` as the approved snapshot. Only the
 * dangerous projection (shell-exec settings, non-safe env vars, hooks) is stored
 * — never the full config — reconstructed into a `DangerousSettingsInput` shape so
 * a subsequent {@link reviewManagedSettings} re-extracts an identical subset.
 * Written atomically with owner-only permissions.
 */
export function saveManagedApproval(
  next: DangerousSettingsInput | null | undefined,
  env: NodeJS.ProcessEnv = process.env,
): void {
  const dangerous = extractDangerousSettings(next);
  const snapshot: DangerousSettingsInput = { ...dangerous.shellSettings, env: dangerous.envVars };
  if (dangerous.hasHooks) snapshot.hooks = dangerous.hooks;
  atomicWriteFileSync(managedApprovalPath(env), JSON.stringify(snapshot, null, 2) + '\n');
}
