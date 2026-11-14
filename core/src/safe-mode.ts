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
];

/**
 * Inspect a project's own settings object for concrete escalation vectors that
 * run arbitrary commands or inject dangerous environment: credential/command
 * helpers (apiKeyHelper, aws/gcp/otel), and any `env` var NOT in the shared
 * SAFE_ENV_VARS allowlist (PATH, LD_PRELOAD, NODE_OPTIONS, DOCKER_HOST, … hijack vectors).
 * Returns the list of flagged keys; empty means no escalation vector found.
 * Mirrors Claude's TrustDialog getApiKeyHelperSources / hasDangerousEnvVars.
 */
export function detectConfigEscalationVectors(settings: Record<string, unknown> | null | undefined): string[] {
  if (!settings || typeof settings !== 'object') return [];
  const flagged: string[] = [];
  for (const key of CONFIG_COMMAND_HELPER_KEYS) {
    const v = settings[key];
    if (v !== undefined && v !== null && v !== false && v !== '') flagged.push(key);
  }
  const env = settings.env;
  if (env && typeof env === 'object' && !Array.isArray(env)) {
    for (const name of Object.keys(env as Record<string, unknown>)) {
      if (!SAFE_ENV_VARS.has(name) && !SAFE_ENV_VARS.has(name.toUpperCase())) {
        flagged.push(`env.${name}`);
      }
    }
  }
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
