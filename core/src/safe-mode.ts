import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { SAFE_ENV_VARS } from './permissions';
import { isBashToolAllowRule } from './skills';

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

/** Does a permission `allow` rule pre-grant the bash tool? Delegates to the single
 *  canonical {@link isBashToolAllowRule} matcher (Claude-Code `Bash`/`Bash(…)` +
 *  AMI's lowercase `bash`) so the safe-mode scanner and the workspace trust audit
 *  never diverge on what counts as a shell grant. */
function isBashAllowRule(rule: string): boolean {
  return isBashToolAllowRule(rule);
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

/** Settings files (relative to a project cwd) that are honored as project-local
 *  trust surfaces. Both AMI-native (`.ami`) and Claude-compatible (`.claude`) forms
 *  and their `.local` overrides are scanned, mirroring Claude's project/local split. */
export const PROJECT_SETTINGS_SOURCES = [
  '.ami/settings.json',
  '.ami/settings.local.json',
  '.claude/settings.json',
  '.claude/settings.local.json',
] as const;

/**
 * Trust/safe-mode indicators fed into {@link shouldEnableSafeMode}. The caller may
 * supply these directly OR derive them from a project-settings scan
 * ({@link scanProjectSettingsForTrust}). Single source of truth for the indicator
 * shape — the engine factory aliases this rather than re-declaring it.
 */
export interface TrustIndicators {
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
}

/** Result of scanning a project's own settings files for trust/escalation signals:
 *  the per-source risk breakdown (for disclosure), the combined escalation vectors,
 *  and the derived {@link TrustIndicators} ready to feed the trust gate. */
export interface SafeModeScanResult {
  /** Per-source risk breakdown — only sources carrying ≥1 signal (for disclosure). */
  bySource: Record<string, ConfigRiskSignals>;
  /** Credential/command-helper keys + `env.<name>` vectors across all sources. */
  escalationVectors: string[];
  /** Trust indicators derived from the scan, ready for {@link shouldEnableSafeMode}. */
  indicators: TrustIndicators;
}

/** Read + JSON-parse a settings file, returning the plain object or null when the
 *  file is missing, unreadable, malformed, or not a JSON object. Never throws. */
function readJsonSettings(file: string): Record<string, unknown> | null {
  try {
    const parsed = JSON.parse(fs.readFileSync(file, 'utf-8'));
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}

/**
 * Pre-flight scan of a project's OWN settings files ({@link PROJECT_SETTINGS_SOURCES})
 * for the concrete arbitrary-code-execution / secret-exfiltration vectors they
 * configure, deriving the {@link TrustIndicators} the trust gate consumes. This is
 * the wiring that turns the per-capability detectors (which previously only ran on
 * caller-supplied objects) into an effective pre-flight check against the real files,
 * mirroring Claude's TrustDialog scanning project+local settings before honoring them.
 *
 * Only the high-risk secret-exfiltration vectors (credential/command helpers +
 * non-allowlisted env vars) set `hasConfigEscalation` (which forces safe mode even
 * inside a trusted root); declared MCP servers set `hasUntrustedMcp`; the remaining
 * capability signals (hooks, bash-allow rules) are returned in `bySource` for
 * disclosure without unconditionally forcing safe mode. Pure w.r.t. process state;
 * reads files side-effect-free and never throws.
 */
export function scanProjectSettingsForTrust(cwd: string): SafeModeScanResult {
  const sources: Record<string, Record<string, unknown> | null> = {};
  if (cwd) {
    for (const rel of PROJECT_SETTINGS_SOURCES) {
      sources[rel] = readJsonSettings(path.join(cwd, ...rel.split('/')));
    }
  }
  const bySource = scanConfigRiskSignalsBySource(sources);
  const escalationVectors = Array.from(
    new Set(Object.values(sources).flatMap(s => detectConfigEscalationVectors(s))),
  );
  const hasUntrustedMcp = Object.values(bySource).some(sig => sig.mcpServers.length > 0);
  return {
    bySource,
    escalationVectors,
    indicators: {
      hasConfigEscalation: escalationVectors.length > 0,
      hasUntrustedMcp,
    },
  };
}

/**
 * True when `cwd` resolves to the user's home directory. Running an agent from
 * $HOME would otherwise grant project-local trust over the entire home tree, so the
 * home dir is treated as untrusted-by-default (safe mode ON) and its trust is
 * session-only — never persisted. Parity with Claude Code's TrustDialog home-dir
 * special case (acceptance kept in memory only, never written to disk). Never throws.
 */
export function isHomeDirectory(cwd: string): boolean {
  if (!cwd) return false;
  try {
    return path.resolve(cwd) === os.homedir();
  } catch {
    return false;
  }
}

export const BLOCKED_RESOURCES = ['prompt_override', 'project_memory', 'project_skills', 'project_config', 'mcp_servers', 'hooks', 'plugins', 'dangerous_env_vars'] as const;
export type BlockedResource = (typeof BLOCKED_RESOURCES)[number];

const BLOCKED_IN_SAFE_MODE = new Set<BlockedResource>([
  'prompt_override',
  'project_memory',
  'project_skills',
  'mcp_servers',
  'hooks',
  'plugins',
  // Project-supplied env vars outside the SAFE_ENV_VARS allowlist (PATH/LD_PRELOAD/
  // NODE_OPTIONS/… hijack vectors) are an arbitrary-code-execution surface; safe mode
  // treats them as a first-class blocked resource (parity with Claude's TrustDialog
  // dangerous-env-var trust signal), surfaced alongside the other blocked categories.
  'dangerous_env_vars',
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

/** Trust tier of a resource under safe mode: `blocked` (never honored),
 *  `restricted` (partially honored), or `allowed` (unaffected). Single static
 *  classifier shared by {@link SafeModeGuard} and disclosure consumers so the
 *  partial tier is actually enforced/surfaced, not merely declared. */
export type ResourceTier = 'blocked' | 'restricted' | 'allowed';

export function resourceTier(resource: BlockedResource): ResourceTier {
  if (isBlockedInSafeMode(resource)) return 'blocked';
  if (isPartiallyBlocked(resource)) return 'restricted';
  return 'allowed';
}

/**
 * Human-readable description of each project-local resource the trust boundary
 * governs — what it is and why it is risky. Lets a frontend explain to the user
 * exactly what safe mode is withholding, mirroring Claude's TrustDialog risk
 * disclosure (which enumerates each execution vector in prose).
 */
export const RESOURCE_DESCRIPTIONS: Record<BlockedResource, string> = {
  prompt_override: 'Project-supplied system-prompt override',
  project_memory: 'Project-local memory/instructions (AGENTS.md / CLAUDE.md)',
  project_skills: 'Project-local skills (can run arbitrary tool calls)',
  project_config: 'Project-local configuration (model, permissions, settings)',
  mcp_servers: 'Project-declared MCP servers (can execute arbitrary code)',
  hooks: 'Project lifecycle hooks (shell out on tool/session events)',
  plugins: 'Project-installed plugin bundles (skills, commands, hooks)',
  dangerous_env_vars: 'Project-supplied environment variables outside the safe allowlist',
};

/**
 * Actionable next-step shown with the blocked-resource notice: how to review the
 * project and opt back in once trusted. Parity with Claude's TrustDialog
 * "Security guide" affordance / "take a moment to review what's in this folder".
 */
export const SAFE_MODE_GUIDANCE =
  'Safe mode is withholding untrusted project-local resources. Review the project\'s '
  + '.ami/.claude settings, hooks, and MCP config before trusting this folder, then set '
  + '"safeMode": false in your config to load them.';

/** One disclosure row per governed resource: its tier and a human-readable label. */
export interface BlockedResourceInfo {
  resource: BlockedResource;
  tier: ResourceTier;
  label: string;
}

/**
 * Structured disclosure of every project-local resource the trust boundary affects
 * under safe mode, each with its {@link ResourceTier} and human-readable label.
 * A frontend renders this to tell the user precisely what is blocked vs restricted
 * and why — the data behind the `/safe-mode` view. Mirrors Claude's TrustDialog
 * per-vector risk enumeration.
 */
export function describeBlocked(): BlockedResourceInfo[] {
  return BLOCKED_RESOURCES.map((resource) => ({
    resource,
    tier: resourceTier(resource),
    label: RESOURCE_DESCRIPTIONS[resource],
  }));
}

export function shouldEnableSafeMode(cwd: string, indicators: TrustIndicators): boolean {
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
  private _restricted: Array<{ resource: BlockedResource; path?: string; timestamp: number }> = [];

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

  /**
   * Resolve the effective tier for a resource given the guard's enabled state:
   * `allowed` when safe mode is off (everything loads), otherwise the resource's
   * static {@link resourceTier}. Single source of truth for both {@link check} and
   * {@link checkTier}.
   */
  tierFor(resource: BlockedResource): ResourceTier {
    if (!this._enabled) return 'allowed';
    return resourceTier(resource);
  }

  check(resource: BlockedResource, path?: string): boolean {
    if (this.tierFor(resource) === 'blocked') {
      this._blocked.push({ resource, path, timestamp: Date.now() });
      return false;
    }
    return true;
  }

  /**
   * Tier-aware access check that actually ENFORCES the partial tier: a fully-blocked
   * resource returns `blocked` (recorded as blocked), a partially-blocked resource
   * returns `restricted` (recorded separately) so the caller can honor it in a
   * degraded/read-only way instead of silently treating it as fully allowed, and
   * anything else returns `allowed`. Mirrors Claude's distinction between denied and
   * restricted project capabilities.
   */
  checkTier(resource: BlockedResource, path?: string): ResourceTier {
    const tier = this.tierFor(resource);
    if (tier === 'blocked') {
      this._blocked.push({ resource, path, timestamp: Date.now() });
    } else if (tier === 'restricted') {
      this._restricted.push({ resource, path, timestamp: Date.now() });
    }
    return tier;
  }

  getBlocked(): ReadonlyArray<{ resource: BlockedResource; path?: string; timestamp: number }> {
    return this._blocked;
  }

  getBlockedCount(): number {
    return this._blocked.length;
  }

  getRestricted(): ReadonlyArray<{ resource: BlockedResource; path?: string; timestamp: number }> {
    return this._restricted;
  }

  getRestrictedCount(): number {
    return this._restricted.length;
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
    this._restricted = [];
  }
}
