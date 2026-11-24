// Split from safe-mode.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { SAFE_ENV_VARS } from './permissions';
import { isBashToolAllowRule } from './skills';

export interface SafeModeConfig {
  enabled: boolean;
  reason?: string;
}

/**
 * Config keys that silently shell out to arbitrary commands on load (credential
 * helpers), a trust/escalation signal when deciding workspace trust. Mirrors
 * the reference's TrustDialog apiKeyHelper / aws / gcp / otel command-helper scan.
 */
export const CONFIG_COMMAND_HELPER_KEYS = [
  'apiKeyHelper',
  'awsCredentialExport', 'awsAuthRefresh',
  'gcpCommands', 'gcloudAuthRefresh',
  'otelHeadersHelper',
] as const;

/** A config value that is concretely set (not undefined/null/false/empty-string). */
export function isConfigValueSet(v: unknown): boolean {
  return v !== undefined && v !== null && v !== false && v !== '';
}

export function stringArray(v: unknown): string[] {
  return Array.isArray(v) ? v.filter((x): x is string => typeof x === 'string') : [];
}

/** Does a permission `allow` rule pre-grant the bash tool? Delegates to the single
 *  canonical {@link isBashToolAllowRule} matcher (the reference `Bash`/`Bash(…)` +
 *  AMI's lowercase `bash`) so the safe-mode scanner and the workspace trust audit
 *  never diverge on what counts as a shell grant. */
export function isBashAllowRule(rule: string): boolean {
  return isBashToolAllowRule(rule);
}

/**
 * Structured, per-capability breakdown of the concrete arbitrary-code-execution /
 * escalation vectors a project's own settings configure. Each field is its own
 * detector, mirroring the reference's TrustDialog/utils.ts fine-grained risk-signal scan,
 * so a caller can both DECIDE trust and DISCLOSE to the user exactly which risks
 * are present (and, when scanned per source file, which file introduced each).
 */
export interface ConfigRiskSignals {
  /** Credential/command-helper keys present (apiKeyHelper, aws/gcp helpers, otelHeadersHelper). */
  commandHelpers: string[];
  /** Project settings declare hooks (shell out on lifecycle events). */
  hooks: boolean;
  /** Permission `allow` rules that pre-grant the bash tool (auto-exec of shell). */
  bashPermissions: string[];
  /** `env` var names NOT in the shared SAFE_ENV_VARS allowlist (PATH/LD_PRELOAD/… hijack). */
  dangerousEnvVars: string[];
  /** Project-declared MCP server names (each can execute arbitrary code). */
  mcpServers: string[];
}

/**
 * Inspect a project's own settings object and return a structured, per-capability
 * breakdown of the concrete escalation vectors it configures: credential/command
 * helpers, hooks, bash-granting permission rules, non-allowlisted env vars, and
 * declared MCP servers. Pure and side-effect free. Broadens the single-boolean
 * trust check into the full TrustDialog vector set so the trust gate can both
 * decide and explain. Mirrors the reference's TrustDialog/utils.ts detectors.
 */
export function scanConfigRiskSignals(settings: Record<string, unknown> | null | undefined): ConfigRiskSignals {
  const signals: ConfigRiskSignals = { commandHelpers: [], hooks: false, bashPermissions: [], dangerousEnvVars: [], mcpServers: [] };
  if (!settings || typeof settings !== 'object' || Array.isArray(settings)) return signals;

  signals.commandHelpers = CONFIG_COMMAND_HELPER_KEYS.filter(k => isConfigValueSet(settings[k]));

  const hooks = settings.hooks;
  signals.hooks = hooks != null && typeof hooks === 'object' && Object.keys(hooks as object).length > 0;

  const perms = settings.permissions;
  if (perms && typeof perms === 'object' && !Array.isArray(perms)) {
    signals.bashPermissions = stringArray((perms as Record<string, unknown>).allow).filter(isBashAllowRule);
  }

  const env = settings.env;
  if (env && typeof env === 'object' && !Array.isArray(env)) {
    for (const name of Object.keys(env as Record<string, unknown>)) {
      if (!SAFE_ENV_VARS.has(name) && !SAFE_ENV_VARS.has(name.toUpperCase())) {
        signals.dangerousEnvVars.push(name);
      }
    }
  }

  const mcp = settings.mcpServers;
  if (mcp && typeof mcp === 'object' && !Array.isArray(mcp)) {
    signals.mcpServers = Object.keys(mcp as Record<string, unknown>);
  }

  return signals;
}

/** True when a scanned settings object carries ANY arbitrary-code-execution /
 *  escalation signal — the folder should require trust before its config is honored. */
export function hasAnyRiskSignal(signals: ConfigRiskSignals): boolean {
  return signals.commandHelpers.length > 0
    || signals.hooks
    || signals.bashPermissions.length > 0
    || signals.dangerousEnvVars.length > 0
    || signals.mcpServers.length > 0;
}

/**
 * Scan multiple named settings sources (e.g. `.ami/settings.json` vs
 * `.ami/settings.local.json`) and return the per-source risk breakdown, keeping
 * only the sources that carry at least one signal. Lets the trust boundary
 * disclose WHICH file introduces each dangerous capability before honoring it —
 * the reference's per-source TrustDialog attribution.
 */
export function scanConfigRiskSignalsBySource(
  sources: Record<string, Record<string, unknown> | null | undefined>,
): Record<string, ConfigRiskSignals> {
  const out: Record<string, ConfigRiskSignals> = {};
  for (const [source, settings] of Object.entries(sources)) {
    const signals = scanConfigRiskSignals(settings);
    if (hasAnyRiskSignal(signals)) out[source] = signals;
  }
  return out;
}

/**
 * Inspect a project's own settings object for concrete escalation vectors that
 * run arbitrary commands or inject dangerous environment: credential/command
 * helpers (apiKeyHelper, aws/gcp/otel), and any `env` var NOT in the shared
 * SAFE_ENV_VARS allowlist (PATH, LD_PRELOAD, NODE_OPTIONS, DOCKER_HOST, … hijack vectors).
 * Returns the list of flagged keys; empty means no escalation vector found.
 * Derives from scanConfigRiskSignals so the detector set has a single source of
 * truth. Scoped to the high-risk secret-exfiltration vectors (helpers + env) that
 * warrant forcing safe mode even inside an otherwise-trusted root; broader
 * capability signals (hooks, bash-allow, MCP) are surfaced via scanConfigRiskSignals
 * for the trust/disclosure decision rather than the unconditional override.
 * Mirrors the reference's TrustDialog getApiKeyHelperSources / hasDangerousEnvVars.
 */
export function detectConfigEscalationVectors(settings: Record<string, unknown> | null | undefined): string[] {
  const signals = scanConfigRiskSignals(settings);
  const flagged: string[] = [...signals.commandHelpers];
  for (const name of signals.dangerousEnvVars) flagged.push(`env.${name}`);
  return flagged;
}
