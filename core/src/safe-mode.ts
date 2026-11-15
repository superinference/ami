import * as path from 'path';
import { SAFE_ENV_VARS } from './permissions';

export interface SafeModeConfig {
  enabled: boolean;
  reason?: string;
}

/**
 * Config keys that silently shell out to arbitrary commands on load (credential
 * helpers), a trust/escalation signal when deciding workspace trust. Mirrors
 * Claude's TrustDialog apiKeyHelper / aws / gcp / otel command-helper scan.
 */
const CONFIG_COMMAND_HELPER_KEYS = [
  'apiKeyHelper',
  'awsCredentialExport', 'awsAuthRefresh',
  'gcpCommands', 'gcloudAuthRefresh',
  'otelHeadersHelper',
] as const;

/** A config value that is concretely set (not undefined/null/false/empty-string). */
function isConfigValueSet(v: unknown): boolean {
  return v !== undefined && v !== null && v !== false && v !== '';
}

function stringArray(v: unknown): string[] {
  return Array.isArray(v) ? v.filter((x): x is string => typeof x === 'string') : [];
}

/** Does a permission `allow` rule pre-grant the bash tool? Matches both the
 *  Claude-Code `Bash` / `Bash(…)` form and AMI's lowercase `bash` tool name. */
function isBashAllowRule(rule: string): boolean {
  const r = rule.trim().toLowerCase();
  return r === 'bash' || r.startsWith('bash(') || r.startsWith('bash ');
}

/**
 * Structured, per-capability breakdown of the concrete arbitrary-code-execution /
 * escalation vectors a project's own settings configure. Each field is its own
 * detector, mirroring Claude's TrustDialog/utils.ts fine-grained risk-signal scan,
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
 * decide and explain. Mirrors Claude's TrustDialog/utils.ts detectors.
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
 * Claude's per-source TrustDialog attribution.
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
 * Mirrors Claude's TrustDialog getApiKeyHelperSources / hasDangerousEnvVars.
 */
export function detectConfigEscalationVectors(settings: Record<string, unknown> | null | undefined): string[] {
  const signals = scanConfigRiskSignals(settings);
  const flagged: string[] = [...signals.commandHelpers];
  for (const name of signals.dangerousEnvVars) flagged.push(`env.${name}`);
  return flagged;
}

/**
 * Asymmetric trust latch: caches ONLY the trusted (true) result, and keeps
 * re-evaluating while still untrusted. Workspace trust transitions false→true
 * during a session but never back, so once granted it must stick; yet a
 * not-yet-granted state must keep re-checking so mid-session acceptance is picked
 * up on the next call. A plain memoize is wrong here — it would also cache the
 * `false` and never notice acceptance. Mirrors Claude's `_trustAccepted ||= …`.
 */
export function createTrustLatch(): {
  check(compute: () => boolean): boolean;
  isLatched(): boolean;
  reset(): void;
} {
  let latched = false;
  return {
    check(compute: () => boolean): boolean {
      if (latched) return true;
      latched = compute();
      return latched;
    },
    isLatched(): boolean {
      return latched;
    },
    reset(): void {
      latched = false;
    },
  };
}

/**
 * Is `cwd` trusted by inheritance — i.e. itself or a descendant of a previously
 * trusted root (e.g. the git-root/original project path the user already
 * accepted)? Mirrors Claude's computeTrustDialogAccepted ancestor/git-root walk,
 * so a subdirectory of an already-trusted project does not re-prompt.
 */
export function isTrustedByAncestor(cwd: string, trustedRoots: string[]): boolean {
  if (!cwd || !trustedRoots.length) return false;
  const strip = (p: string) => p.replace(/[/\\]+$/, '') || p;
  const c = strip(path.normalize(path.isAbsolute(cwd) ? cwd : path.resolve(cwd)));
  for (const root of trustedRoots) {
    if (!root) continue;
    const r = strip(path.normalize(path.isAbsolute(root) ? root : path.resolve(root)));
    if (c === r || c.startsWith(r + path.sep)) return true;
  }
  return false;
}

export const BLOCKED_RESOURCES = ['prompt_override', 'project_memory', 'project_skills', 'project_config', 'mcp_servers', 'hooks', 'plugins'] as const;
export type BlockedResource = (typeof BLOCKED_RESOURCES)[number];

const BLOCKED_IN_SAFE_MODE = new Set<BlockedResource>([
  'prompt_override',
  'project_memory',
  'project_skills',
  'mcp_servers',
  'hooks',
  'plugins',
]);

const PARTIALLY_BLOCKED = new Set<BlockedResource>([
  'project_config',
]);

export function isBlockedInSafeMode(resource: BlockedResource): boolean {
  return BLOCKED_IN_SAFE_MODE.has(resource);
}

export function isPartiallyBlocked(resource: BlockedResource): boolean {
  return PARTIALLY_BLOCKED.has(resource);
}

export function shouldEnableSafeMode(cwd: string, indicators: {
  untrustedProject?: boolean;
  clonedFromUnknown?: boolean;
  hasPromptOverride?: boolean;
  hasUntrustedMcp?: boolean;
  /** Project settings flag a config-sourced escalation vector (apiKeyHelper, aws/
   *  gcp/otel helper, or a non-allowlisted env var) — treat as untrusted. */
  hasConfigEscalation?: boolean;
  /** Roots the user has already trusted (e.g. the git-root/original project path).
   *  If `cwd` is one of these or a descendant, safe mode is NOT forced for the
   *  trust indicators (inheritance), mirroring computeTrustDialogAccepted. */
  trustedRoots?: string[];
}): boolean {
  // Ancestor/git-root trust inheritance: a cwd inside an already-trusted root is
  // not re-flagged as untrusted. Hard escalation signals (prompt-override+MCP,
  // config escalation) still engage safe mode even inside a trusted root.
  const inherited = indicators.trustedRoots ? isTrustedByAncestor(cwd, indicators.trustedRoots) : false;
  if (!inherited) {
    if (indicators.untrustedProject) return true;
    if (indicators.clonedFromUnknown) return true;
  }
  if (indicators.hasConfigEscalation) return true;
  if (indicators.hasPromptOverride && indicators.hasUntrustedMcp) return true;
  return false;
}

export class SafeModeGuard {
  private _enabled: boolean;
  private _reason: string;
  private _blocked: Array<{ resource: BlockedResource; path?: string; timestamp: number }> = [];

  constructor(config: SafeModeConfig) {
    this._enabled = config.enabled;
    this._reason = config.reason ?? 'safe mode active';
  }

  isEnabled(): boolean {
    return this._enabled;
  }

  getReason(): string {
    return this._reason;
  }

  check(resource: BlockedResource, path?: string): boolean {
    if (!this._enabled) return true;
    if (isBlockedInSafeMode(resource)) {
      this._blocked.push({ resource, path, timestamp: Date.now() });
      return false;
    }
    return true;
  }

  getBlocked(): ReadonlyArray<{ resource: BlockedResource; path?: string; timestamp: number }> {
    return this._blocked;
  }

  getBlockedCount(): number {
    return this._blocked.length;
  }

  enable(reason: string): void {
    this._enabled = true;
    this._reason = reason;
  }

  disable(): void {
    this._enabled = false;
    this._reason = '';
  }

  getBlockedResources(): BlockedResource[] {
    return [...BLOCKED_IN_SAFE_MODE];
  }

  clear(): void {
    this._blocked = [];
  }
}
