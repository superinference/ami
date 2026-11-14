/**
 * Pinned environment variables that must not be modified by user-tier code.
 * Based on CC's sec-default plugin behavior.
 */

// Policy and sign-in variables — control where policy and auth are read from
export const POLICY_VARIABLES = new Set([
  'SUPERINFERENCE_MANAGED_SETTINGS_PATH',
  'SUPERINFERENCE_REMOTE_SETTINGS_PATH',
  'SUPERINFERENCE_MOCK_REMOTE_SETTINGS',
  'SUPERINFERENCE_DISABLE_ADMIN_ENV_UNION',
  'SUPERINFERENCE_CONFIG_DIR',
  'SUPERINFERENCE_SECURESTORAGE_CONFIG_DIR',
  'USE_LOCAL_OAUTH',
  'USE_STAGING_OAUTH',
  'SUPERINFERENCE_LOCAL_OAUTH_API_BASE',
]);

// Code-loading variables — can inject arbitrary code into child processes.
// Besides the explicit members, the whole LD_*/DYLD_* families are caught by
// CODE_LOADING_PREFIXES below (matching Claude's BINARY_HIJACK_VARS
// /^(LD_|DYLD_|PATH$)/), so a novel loader var (e.g. LD_AUDIT variants,
// DYLD_VERSIONED_LIBRARY_PATH) can't slip through by not being enumerated.
export const CODE_LOADING_VARIABLES = new Set([
  'LD_PRELOAD',
  'LD_LIBRARY_PATH',
  'LD_AUDIT',
  'DYLD_INSERT_LIBRARIES',
  'DYLD_LIBRARY_PATH',
  'DYLD_FRAMEWORK_PATH',
  'BASH_ENV',
  'ENV',
  'ZDOTDIR',
  'NODE_OPTIONS',
  'PROMPT_COMMAND',
  'PATH',
  // Per-language module/code-loading vectors — each lets an untrusted dir inject
  // code into a spawned interpreter/toolchain (parity: bashPermissions.ts SECURITY
  // note — PYTHONPATH/NODE_PATH/CLASSPATH/RUBYLIB/GOFLAGS/RUSTFLAGS are NEVER safe).
  'PYTHONPATH',
  'NODE_PATH',
  'CLASSPATH',
  'RUBYLIB',
  'GOFLAGS',
  'RUSTFLAGS',
  'PERL5LIB',
  'PERLLIB',
]);

/**
 * Env-var name prefixes whose entire family loads code into a child process.
 * The dynamic-linker families (LD_*, DYLD_*) grow across platforms/versions, so
 * they are prefix-matched rather than enumerated (parity: BINARY_HIJACK_VARS).
 */
export const CODE_LOADING_PREFIXES = ['LD_', 'DYLD_'] as const;

function matchesCodeLoadingPrefix(upper: string): boolean {
  return CODE_LOADING_PREFIXES.some(p => upper.startsWith(p));
}

// Telemetry control variables
export const TELEMETRY_VARIABLES = new Set([
  'DISABLE_TELEMETRY',
  'DO_NOT_TRACK',
  'SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC',
]);

export const PIN_CATEGORIES = ['policy', 'code_loading', 'telemetry', 'dynamic_policy'] as const;
export type PinCategory = (typeof PIN_CATEGORIES)[number];

export function isPinnedVariable(name: string): boolean {
  const upper = name.toUpperCase();
  return (
    POLICY_VARIABLES.has(upper) ||
    CODE_LOADING_VARIABLES.has(upper) ||
    TELEMETRY_VARIABLES.has(upper) ||
    matchesCodeLoadingPrefix(upper)
  );
}

// ---------------------------------------------------------------------------
// Settings-sourced env-var trust (parity: Claude's managedEnvConstants.ts /
// managedEnv.ts). A settings file can carry an `env` block. Applying it blindly
// lets a committed, project-local file redirect inference traffic
// (ANTHROPIC_BASE_URL), disable TLS verification (NODE_TLS_REJECT_UNAUTHORIZED)
// or inject code (LD_PRELOAD) into the agent process. These helpers classify
// which env vars are safe to apply from an untrusted (project-scoped) source and
// which must be stripped.
// ---------------------------------------------------------------------------

/**
 * Allowlist of env vars that are safe to apply from ANY settings source,
 * including untrusted project-scoped files. Anything NOT in this set is treated
 * as dangerous when it originates from a project-scoped source and is only
 * applied from trusted sources (global/user config, managed policy, CLI flag).
 *
 * This is the source of truth for env-var safety. Keep it conservative: a var
 * only belongs here if a malicious project cannot weaponize it (no network
 * redirect, no TLS-trust change, no auth switch, no code loading).
 */
export const SAFE_ENV_VARS = new Set([
  // Tuning knobs — behavior only, no trust boundary crossed
  'BASH_DEFAULT_TIMEOUT_MS',
  'BASH_MAX_OUTPUT_LENGTH',
  'BASH_MAX_TIMEOUT_MS',
  'MAX_MCP_OUTPUT_TOKENS',
  'MAX_THINKING_TOKENS',
  'MCP_TIMEOUT',
  'MCP_TOOL_TIMEOUT',
  'SUPERINFERENCE_MAX_OUTPUT_TOKENS',
  'SUPERINFERENCE_SUBAGENT_MODEL',
  'USE_BUILTIN_RIPGREP',
  // Model-name defaults (identifiers only, not endpoints/auth)
  'SUPERINFERENCE_MODEL',
  'SUPERINFERENCE_DEFAULT_HAIKU_MODEL',
  'SUPERINFERENCE_DEFAULT_SONNET_MODEL',
  'SUPERINFERENCE_DEFAULT_OPUS_MODEL',
  // Feature toggles
  'DISABLE_AUTOUPDATER',
  'DISABLE_COST_WARNINGS',
  'DISABLE_ERROR_REPORTING',
  'ENABLE_TOOL_SEARCH',
]);

/**
 * True when `name` is in the SAFE_ENV_VARS allowlist (case-insensitive).
 */
export function isSafeEnvVar(name: string): boolean {
  return SAFE_ENV_VARS.has(name.toUpperCase());
}

/**
 * Env vars that control inference routing — which provider, which endpoint,
 * which credentials. When the host owns routing (it sets
 * SUPERINFERENCE_PROVIDER_MANAGED_BY_HOST in the spawn env), these are stripped
 * from every settings-sourced env object so a user's config cannot redirect
 * requests away from the host-configured provider.
 *
 * Classified by pattern (any `*_API_KEY`, `*_AUTH_TOKEN`, `*_BASE_URL`) plus an
 * explicit set of routing/credential vars that don't match the suffixes.
 */
export const PROVIDER_MANAGED_ENV_VARS = new Set([
  'AI_API_KEY',
  'AI_BASE_URL',
  'PROVIDER_BASE_URL',
  'AWS_ACCESS_KEY_ID',
  'AWS_SECRET_ACCESS_KEY',
  'AWS_SESSION_TOKEN',
  'AWS_PROFILE',
  'AWS_REGION',
  'AWS_DEFAULT_REGION',
  'GOOGLE_APPLICATION_CREDENTIALS',
  'ANTHROPIC_VERTEX_PROJECT_ID',
  'HF_TOKEN',
  'SUPERINFERENCE_PROVIDER_MANAGED_BY_HOST',
]);

const PROVIDER_MANAGED_ENV_SUFFIXES = ['_API_KEY', '_AUTH_TOKEN', '_BASE_URL'];

/**
 * True when `name` is an inference-routing / provider-credential env var.
 */
export function isProviderManagedEnvVar(name: string): boolean {
  const upper = name.toUpperCase();
  if (PROVIDER_MANAGED_ENV_VARS.has(upper)) return true;
  return PROVIDER_MANAGED_ENV_SUFFIXES.some(s => upper.endsWith(s));
}

/**
 * Network-trust / routing-redirect env vars. Unlike {@link PROVIDER_MANAGED_ENV_VARS}
 * (provider base URLs, API keys, auth tokens — dangerous only when the host owns
 * routing), these subvert transport security or exfiltrate traffic REGARDLESS of
 * host mode, so an untrusted (project-scoped) source must never set them:
 *  - NODE_TLS_REJECT_UNAUTHORIZED — disables TLS certificate verification;
 *  - NODE_EXTRA_CA_CERTS — trusts an attacker-supplied CA (only user-controlled
 *    files may set it — parity: caCertsConfig.ts);
 *  - HTTP(S)_PROXY / NO_PROXY / ALL_PROXY — reroute every request through a proxy;
 *  - OTEL_EXPORTER_OTLP_*_ENDPOINT — exfiltrate telemetry to an attacker endpoint.
 *
 * (parity: managedEnvConstants.ts SAFE_ENV_VARS "REDIRECT/TRUST ATTACKER" notes.)
 */
export const NETWORK_TRUST_VARS = new Set([
  'NODE_TLS_REJECT_UNAUTHORIZED',
  'NODE_EXTRA_CA_CERTS',
  'HTTP_PROXY',
  'HTTPS_PROXY',
  'NO_PROXY',
  'ALL_PROXY',
]);

const NETWORK_TRUST_PREFIXES = ['OTEL_EXPORTER_OTLP_'];

/**
 * True when `name` controls transport trust / request routing in a way that is
 * dangerous from ANY untrusted source (independent of provider-managed mode).
 */
export function isNetworkTrustEnvVar(name: string): boolean {
  const upper = name.toUpperCase();
  if (NETWORK_TRUST_VARS.has(upper)) return true;
  return NETWORK_TRUST_PREFIXES.some(p => upper.startsWith(p)) && upper.endsWith('_ENDPOINT');
}

/** Why a settings-sourced env var was not applied to the process env. */
export const ENV_REJECT_REASONS = ['pinned', 'provider-managed', 'network-trust', 'unsafe-untrusted'] as const;
export type EnvRejectReason = (typeof ENV_REJECT_REASONS)[number];

/**
 * Classify an env var's danger when it originates from an untrusted file (a
 * project-scoped `.env` / settings). Returns the rejection reason, or null when
 * the var is safe to apply from an untrusted source.
 *
 * This is the SINGLE canonical "dangerous env var" classifier — every untrusted
 * env-application site (the `.env` loader, managed-settings gates) composes it
 * rather than re-deriving which vars are dangerous:
 *  1. `pinned`           — code-loading / policy / telemetry vars (never settable);
 *  2. `network-trust`    — TLS/proxy/OTEL redirect-or-trust vars (always dangerous);
 *  3. `provider-managed` — routing/credential vars, dangerous only when the host
 *                          owns inference routing (`hostManagedProvider`).
 */
export function classifyUntrustedEnvVar(
  name: string,
  opts: { hostManagedProvider?: boolean } = {},
): EnvRejectReason | null {
  if (isPinnedVariable(name)) return 'pinned';
  if (isNetworkTrustEnvVar(name)) return 'network-trust';
  if (opts.hostManagedProvider && isProviderManagedEnvVar(name)) return 'provider-managed';
  return null;
}

/** Trust tier of the settings source supplying an `env` block. */
export const ENV_TRUST_TIERS = ['trusted', 'project'] as const;
export type EnvTrustTier = (typeof ENV_TRUST_TIERS)[number];

export interface SettingsEnvFilterResult {
  /** Env vars that passed the trust filter and may be written to process.env. */
  applied: Record<string, string>;
  /** Env vars that were rejected, with the reason. */
  rejected: Array<{ key: string; reason: EnvRejectReason }>;
}

/**
 * Filter a settings-sourced `env` block against the trust model.
 *
 * Rejection order (first match wins):
 *  1. `pinned`           — policy/code-loading/telemetry vars are never settable
 *                          from settings regardless of tier.
 *  2. `provider-managed` — when `hostManagedProvider`, routing/credential vars
 *                          are stripped so settings cannot re-route inference.
 *  3. `unsafe-untrusted` — for a `project`-tier source, anything outside
 *                          SAFE_ENV_VARS is dropped (a committed project file
 *                          must not inject dangerous vars).
 *
 * A `trusted` source (global/user config, managed policy, explicit CLI flag)
 * may apply any var that is not pinned or host-provider-managed.
 */
export function filterSettingsEnv(
  env: Record<string, string> | undefined,
  opts: { tier: EnvTrustTier; hostManagedProvider?: boolean },
): SettingsEnvFilterResult {
  const applied: Record<string, string> = {};
  const rejected: Array<{ key: string; reason: EnvRejectReason }> = [];
  if (!env) return { applied, rejected };
  for (const [key, value] of Object.entries(env)) {
    if (typeof value !== 'string') continue;
    if (isPinnedVariable(key)) {
      rejected.push({ key, reason: 'pinned' });
      continue;
    }
    if (opts.hostManagedProvider && isProviderManagedEnvVar(key)) {
      rejected.push({ key, reason: 'provider-managed' });
      continue;
    }
    if (opts.tier === 'project' && !isSafeEnvVar(key)) {
      rejected.push({ key, reason: 'unsafe-untrusted' });
      continue;
    }
    applied[key] = value;
  }
  return { applied, rejected };
}

/** True when the host has declared that it owns inference routing. */
export function isHostManagedProvider(envSource: NodeJS.ProcessEnv = process.env): boolean {
  const v = envSource.SUPERINFERENCE_PROVIDER_MANAGED_BY_HOST;
  return v != null && v !== '' && v !== '0' && v.toLowerCase() !== 'false';
}

export function getPinCategory(name: string): PinCategory | null {
  const upper = name.toUpperCase();
  if (POLICY_VARIABLES.has(upper)) return 'policy';
  if (CODE_LOADING_VARIABLES.has(upper) || matchesCodeLoadingPrefix(upper)) return 'code_loading';
  if (TELEMETRY_VARIABLES.has(upper)) return 'telemetry';
  return null;
}

export function getAllPinnedVariables(): string[] {
  return [...POLICY_VARIABLES, ...CODE_LOADING_VARIABLES, ...TELEMETRY_VARIABLES];
}

export interface EnvSetAttempt {
  variable: string;
  category: PinCategory;
  source: string;
  timestamp: number;
}

export interface EnvCheckResult {
  blocked: boolean;
  variable: string;
  category: PinCategory | null;
  reason: string;
}

export class EnvProtectionGuard {
  private _blockedAttempts: EnvSetAttempt[] = [];
  private _maxAttempts = 1000;
  private _dynamicPins = new Set<string>();

  mergePinnedFromPolicy(keys: string[]): void {
    for (const k of keys) this._dynamicPins.add(k.toUpperCase());
  }

  isDynamicallyPinned(name: string): boolean {
    return this._dynamicPins.has(name.toUpperCase());
  }

  checkAndBlock(variable: string, source: string): EnvCheckResult {
    const category = getPinCategory(variable);
    const isDynamic = this._dynamicPins.has(variable.toUpperCase());
    if (!category && !isDynamic) {
      return { blocked: false, variable, category: null, reason: '' };
    }
    this.checkSet(variable, source);
    const effectiveCategory = category ?? 'dynamic_policy';
    return {
      blocked: true,
      variable: variable.toUpperCase(),
      category: effectiveCategory,
      reason: `Environment variable "${variable}" is pinned (${effectiveCategory}) and cannot be modified`,
    };
  }

  checkSet(variable: string, source: string): EnvSetAttempt | null {
    const category = getPinCategory(variable);
    if (!category) return null;
    const attempt: EnvSetAttempt = {
      variable: variable.toUpperCase(),
      category,
      source,
      timestamp: Date.now(),
    };
    if (this._blockedAttempts.length < this._maxAttempts) {
      this._blockedAttempts.push(attempt);
    }
    return attempt;
  }

  getBlockedAttempts(): readonly EnvSetAttempt[] {
    return this._blockedAttempts;
  }

  getBlockedCount(): number {
    return this._blockedAttempts.length;
  }

  clear(): void {
    this._blockedAttempts = [];
  }
}
