// Split from config.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { ConfigValueIssue } from './config-strip-jsonc-comments';
import type { ConfigDiagnostic, ConfigJSONSchema } from './config-config-diagnostic';
import { DEFAULT_TOOL_OUTPUT_MAX_CHARS, TOOL_OUTPUT_MAX_CHARS_ENV, validateBoundedIntEnvVar } from './config-validate-bounded-int-env-var';
import type { ProjectConfig } from './config';

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
 * A non-finite `declaredMaxChars` is a deliberate opt-out (parity: the reference's
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
 * Default POSIX mode for a NEWLY-created config file: owner read/write only (0o600). * A config file can hold a provider base-URL override, an `env` block, MCP/credential-
 * adjacent knobs; an unprivileged co-user on the same host must not be able to read it.
 * An EXISTING file's mode is preserved on rewrite (only new files get this default), so
 * a user who deliberately widened permissions is not fought. Parity: the reference writes
 * its config with `mode: 0o600`.
 */
export const CONFIG_FILE_MODE = 0o600;

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

// ---------------------------------------------------------------------------
// Symlink write-through + pre-write backup rotation
// ---------------------------------------------------------------------------

/** The infix tagging a timestamped pre-write backup (distinct from `.corrupt-`). */
export const CONFIG_BACKUP_INFIX = '.backup-';

/** Keep at most this many pre-write backups per config file (parity: the reference's MAX_BACKUPS). */
export const MAX_CONFIG_BACKUPS = 5;

/**
 * Minimum spacing (ms) between pre-write backups of the same file (parity: the reference's
 * MIN_BACKUP_INTERVAL_MS). During startup many `/config` / status-line writes can fire
 * within milliseconds; without this, each would spawn a backup file and spam disk.
 */
export const CONFIG_BACKUP_MIN_INTERVAL_MS = 60_000;

/** The epoch-ms timestamp encoded in a `<file>.backup-<ms>` name, or null if unparseable. */
export function backupTimestamp(backupPath: string): number | null {
  const idx = backupPath.lastIndexOf(CONFIG_BACKUP_INFIX);
  if (idx < 0) return null;
  const n = Number(backupPath.slice(idx + CONFIG_BACKUP_INFIX.length));
  return Number.isFinite(n) ? n : null;
}

// ---------------------------------------------------------------------------
// Global-config write-count introspection (parity: the reference's globalConfigWriteCount
// / CONFIG_WRITE_DISPLAY_THRESHOLD). A pathological write RATE to the shared global
// config (a save loop, two fighting instances) is the classic precursor to a
// corrupt `~/.ami/config.json`; before this it was invisible until the file broke.
// The per-process counter + exported getter make the churn observable, and a
// one-time warning is logged the moment the rate crosses the display threshold.
// ---------------------------------------------------------------------------

/**
 * Write-rate at which an unusually high global-config churn is surfaced (parity:
 * the reference's CONFIG_WRITE_DISPLAY_THRESHOLD=20). Reached once per process, a
 * single warning is logged — a steady trickle of saves never spams.
 */
export const CONFIG_WRITE_DISPLAY_THRESHOLD = 20;

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

/** Why a {@link updateConfigFile} call did (or did not) persist. */
export const UPDATE_CONFIG_REASONS = ['written', 'no-change', 'corrupt'] as const;

export type UpdateConfigReason = (typeof UPDATE_CONFIG_REASONS)[number];

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
 * Debounce window for {@link recordSkillUsageDebounced}: a skill usage written less
 * than this long ago skips the (lock + read + parse + write) global-config round-trip
 * on the next invocation. Parity with the reference's `SKILL_USAGE_DEBOUNCE_MS`
 * (utils/suggestions/skillUsageTracking.ts) — the frecency ranking uses a 7-day
 * half-life, so sub-minute write granularity is irrelevant and the hot-repeat lock+I/O
 * is pure overhead.
 */
export const SKILL_USAGE_DEBOUNCE_MS = 60_000;

/** The .gitignore rule auto-registered for the per-user local config file. */
export const LOCAL_CONFIG_GITIGNORE_RULE = '.ami/config.local.json';

// ---------------------------------------------------------------------------
// Per-project onboarding hint-fatigue control (parity: projectOnboardingState.ts)
// ---------------------------------------------------------------------------

/**
 * The seen-count cap after which the per-project onboarding nudge stops showing
 * (parity: the reference's `projectOnboardingSeenCount >= 4` guard).
 */
export const PROJECT_ONBOARDING_SEEN_CAP = 4;
