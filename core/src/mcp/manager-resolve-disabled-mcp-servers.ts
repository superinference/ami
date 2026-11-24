// Split from manager.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { MCP_PROTOCOL_VERSION } from './client';
import { buildRedirectUri, DEFAULT_OAUTH_CALLBACK_PORT } from './oauth-port';
import { validateBoundedIntEnvVar } from '../config';

/** Read a positive-int env override, else the default. */
export function envPositiveInt(name: string, fallback: number): number {
  const env = process.env[name];
  if (env) {
    const n = parseInt(env, 10);
    if (!Number.isNaN(n) && n > 0) return n;
  }
  return fallback;
}

/** Bounded-concurrency ceiling for connecting LOCAL (stdio/container) MCP servers. */
export function getMcpConnectionBatchSize(): number {
  return envPositiveInt('SUPERINFERENCE_MCP_SERVER_CONNECTION_BATCH_SIZE', 3);
}

/** Bounded-concurrency ceiling for connecting REMOTE (http/sse) MCP servers. */
export function getMcpRemoteConnectionBatchSize(): number {
  return envPositiveInt('SUPERINFERENCE_MCP_REMOTE_SERVER_CONNECTION_BATCH_SIZE', 20);
}

/**
 * The OAuth loopback callback port pinned via `MCP_OAUTH_CALLBACK_PORT`
 * (the reference-parity env knob), or undefined when unset. When set, the redirect URI
 * uses this fixed port (no dynamic discovery) so users behind a firewall can
 * allow-list a stable callback port.
 */
export function getConfiguredOAuthCallbackPort(): number | undefined {
  const env = process.env.MCP_OAUTH_CALLBACK_PORT;
  if (!env) return undefined;
  const n = parseInt(env, 10);
  return !Number.isNaN(n) && n > 0 ? n : undefined;
}

/**
 * True when a per-server OAuth `callbackPort` config value is a usable TCP port — a
 * positive integer within the 1..65535 range (the reference-parity `--callback-port`
 * validation). A missing / non-integer / out-of-range value is rejected so a
 * malformed config falls through to the env knob / dynamic port discovery rather than
 * building an invalid redirect URI.
 */
export function isValidCallbackPort(port: number | undefined): port is number {
  return typeof port === 'number' && Number.isInteger(port) && port > 0 && port <= 65535;
}

/**
 * Resolve the OAuth loopback redirect URI for a server, with the reference-parity precedence
 * (`mcp add --callback-port` / MCP_OAUTH_CALLBACK_PORT): an explicit per-server
 * `redirectUri` wins; else a per-server `callbackPort` (validated via
 * {@link isValidCallbackPort}) wins over the global `envCallbackPort`; else no fixed
 * port is known and `dynamic` is true so the caller discovers a free loopback port at
 * consent time. Returns the static `redirectUri` to seed the session plus a `dynamic`
 * flag indicating whether a dynamic-port resolver should be installed.
 */
export function resolveMcpOAuthRedirect(
  oauthConfig: { redirectUri?: string; callbackPort?: number },
  envCallbackPort: number | undefined,
): { redirectUri: string; dynamic: boolean } {
  const configuredRedirect = oauthConfig.redirectUri;
  const serverCallbackPort = isValidCallbackPort(oauthConfig.callbackPort) ? oauthConfig.callbackPort : undefined;
  const effectivePort = serverCallbackPort ?? envCallbackPort;
  return {
    redirectUri: configuredRedirect ?? buildRedirectUri(effectivePort ?? DEFAULT_OAUTH_CALLBACK_PORT),
    dynamic: !configuredRedirect && effectivePort === undefined,
  };
}

/**
 * Run `worker` over `items` with at most `limit` in flight at once (a minimal
 * pMap). Preserves no ordering guarantee beyond each worker settling; never
 * rejects (workers are expected to capture their own errors).
 */
export async function runBoundedPool<T>(items: T[], limit: number, worker: (item: T) => Promise<void>): Promise<void> {
  if (items.length === 0) return;
  const ceiling = Math.max(1, limit);
  let cursor = 0;
  const runners: Promise<void>[] = [];
  const pump = async (): Promise<void> => {
    while (cursor < items.length) {
      const item = items[cursor++];
      await worker(item);
    }
  };
  for (let i = 0; i < Math.min(ceiling, items.length); i++) runners.push(pump());
  await Promise.all(runners);
}

export const DEFAULT_MCP_DESCRIPTION_LENGTH = 2048;

// Hard upper ceiling for SUPERINFERENCE_MAX_MCP_DESCRIPTION_LENGTH. A mistaken or
// hostile value (e.g. =99999999) would let a single MCP server bloat EVERY tool
// description and exhaust the model's context window (DoS). Parity: the reference's
// validateBoundedIntEnvVar upper-limit cap + capped/invalid diagnostic logging.
export const MCP_DESCRIPTION_UPPER_LIMIT = 100_000;

export function getMcpMaxDescriptionLength(): number {
  // Route through the single canonical bounded-int env validator so a non-numeric/
  // non-positive override falls back to the default (status 'invalid') and an
  // over-ceiling override is CLAMPED to the limit (status 'capped') — each logged
  // to the debug channel by the validator — instead of the old silent/unbounded
  // parse that applied no ceiling at all.
  return validateBoundedIntEnvVar(
    'SUPERINFERENCE_MAX_MCP_DESCRIPTION_LENGTH',
    process.env.SUPERINFERENCE_MAX_MCP_DESCRIPTION_LENGTH,
    { default: DEFAULT_MCP_DESCRIPTION_LENGTH, min: 1, max: MCP_DESCRIPTION_UPPER_LIMIT },
  ).effective;
}

/**
 * Match an MCP tool name against a list of auto-allow glob patterns. A pattern
 * ending in `*` matches by prefix; `*` alone matches everything; otherwise exact.
 * Used to auto-approve a pinned stance skill's MCP tools (e.g. `mcp__kali__*`).
 */
export function matchMcpAutoAllow(toolName: string, patterns: string[]): boolean {
  for (const pattern of patterns) {
    if (pattern === '*') return true;
    if (pattern.endsWith('*')) {
      const prefix = pattern.slice(0, -1);
      if (toolName.startsWith(prefix)) return true;
    } else if (pattern === toolName) {
      return true;
    }
  }
  return false;
}

/**
 * A structured MCP allow/deny policy entry (parity with the reference's
 * allowed/deniedMcpServers): match a server by its name, by its full stdio
 * command array (`[command, ...args]`, exact), or by upstream URL (exact or a
 * `*`-wildcard like `https://*.example.com/*`). A bare string is shorthand: a URL
 * if it parses as one (exact), otherwise a server name; either may contain `*`
 * to become a wildcard pattern. This supersets AMI's previous name/exact-URL-only
 * denylist — enabling an admin to deny e.g. `npx -y evil-pkg` regardless of the
 * server's local name, or a whole subdomain/port family of a blocked host.
 */
export type McpPolicyEntry =
  | string
  | { serverName: string }
  | { serverCommand: string[] }
  | { serverUrl: string }
  /**
   * Provenance-scoped entry (the reference-parity channelAllowlist): approval keyed to the
   * plugin bundle that produced a server rather than its arbitrary local name.
   * `pluginSource` matches {@link McpServerConfig.pluginSource} exactly (or as a
   * `*`-wildcard family). The reference-shaped `{ marketplace, plugin }` form compiles
   * to the `<plugin>@<marketplace>` provenance convention {@link scopePluginMcpServers}
   * stamps, and also admits the marketplace-agnostic bare `<plugin>`.
   */
  | { pluginSource: string }
  | { marketplace: string; plugin: string };

/**
 * Resolve the set of server names a user has toggled OFF via the persisted
 * `disabledMcpServers` / `enabledMcpServers` config knobs (parity with the reference
 * Code's per-server enable/disable toggles, which disable a configured server
 * WITHOUT removing its config). A server is effectively disabled when it appears
 * in `disabled` and is NOT re-enabled in `enabled` — so `enabledMcpServers` acts
 * as a precise override that wins over `disabledMcpServers` for the same name
 * (e.g. a server disabled at the user tier but re-enabled at the project tier).
 *
 * The result is a de-duplicated list of trimmed, non-empty server names, ready to
 * be folded into {@link McpManager.setDeniedServerList} so a toggled-off server is
 * skipped at LOAD time through the exact same deny-enforcement path as an
 * admin denylist entry. Matching is case-insensitive (server names), mirroring the
 * denylist's `caseInsensitiveNames` compilation, so a name re-enabled in a
 * different case still cancels its disabled entry.
 */
export function resolveDisabledMcpServers(
  disabled?: readonly string[],
  enabled?: readonly string[],
): string[] {
  const enabledSet = new Set(
    (enabled ?? []).map((n) => n.trim().toLowerCase()).filter((n) => n.length > 0),
  );
  const out: string[] = [];
  const seen = new Set<string>();
  for (const raw of disabled ?? []) {
    const name = raw.trim();
    const fold = name.toLowerCase();
    if (!name || enabledSet.has(fold) || seen.has(fold)) continue;
    seen.add(fold);
    out.push(name);
  }
  return out;
}

/**
 * Compile a `*`-wildcard pattern into an anchored RegExp where `*` matches any
 * run of characters and every other character is matched literally. Anchored at
 * both ends so a pattern can never match unintended substrings (no unanchored
 * regex). Used for URL and server-name wildcard policy entries.
 */
export function wildcardToRegExp(pattern: string): RegExp {
  const NUL = '\u0000';
  const escaped = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, (c) => (c === '*' ? NUL : `\\${c}`))
    .split(NUL)
    .join('.*');
  return new RegExp('^' + escaped + '$');
}

/** Internal compiled form of an allow/deny policy (see {@link McpPolicyEntry}). */
export interface CompiledMcpPolicy {
  /** When true, server-name matching is case-insensitive (denylist); the allowlist
   *  stays case-sensitive to preserve its established semantics. */
  ci: boolean;
  names: Set<string>;        // exact server names (lower-cased iff ci)
  namePatterns: RegExp[];    // name wildcard patterns (lower-cased iff ci)
  urls: Set<string>;         // exact, normalized upstream URLs
  urlPatterns: RegExp[];     // URL wildcard patterns (matched against raw url)
  commands: string[][];      // full stdio command arrays (exact match)
  pluginSources: Set<string>;     // exact provenance tags (lower-cased iff ci)
  pluginSourcePatterns: RegExp[]; // provenance wildcard patterns (lower-cased iff ci)
}

/** Config fields a policy can match against (name is passed separately). */
export interface McpPolicyTarget { url?: string; command?: string; args?: string[]; pluginSource?: string }

/** Wrap a handshake failure with a message naming the protocol version AMI speaks. */
export function handshakeError(name: string, err: unknown): Error {
  const msg = err instanceof Error ? err.message : String(err);
  return new Error(
    `MCP server "${name}" failed the MCP handshake (AMI speaks protocol ${MCP_PROTOCOL_VERSION}). ` +
    `This is often an older or non-compliant MCP server that does not support this protocol version — ` +
    `update it or use a server compatible with MCP ${MCP_PROTOCOL_VERSION}. Original error: ${msg}`,
  );
}

/**
 * True when a connect failure is an authentication/authorization problem rather
 * than a generic error — an OAuth-protected server whose consent is required or
 * unavailable (headless), or an HTTP 401/403. Lets the manager surface a distinct
 * `needs-auth` state ("click to authenticate") instead of a hard `error`, matching
 * the reference's dedicated needs-auth connection state.
 */
export function isAuthRequiredError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  return /OAuth consent|requires an interactive frontend|\b401\b|\b403\b|unauthorized|authorization failed|needs? auth|WWW-Authenticate/i.test(msg);
}

/**
 * Severity of an MCP config diagnostic (the reference-parity `ValidationError.severity`,
 * McpParsingWarnings.tsx:L210-212). A `fatal` problem means the offending config —
 * a whole file (parse failure) or a single server entry (invalid shape) — was NOT
 * loaded; a `warning` problem is recoverable (the server still loads, e.g. an unset
 * env var or the Windows npx-wrapper footgun).
 */
export const MCP_CONFIG_SEVERITYS = ['fatal', 'warning'] as const;

export type McpConfigSeverity = (typeof MCP_CONFIG_SEVERITYS)[number];

/**
 * A fatal MCP config diagnostic: a config file that exists but could not be parsed.
 * `severity` is `'fatal'` (a parse failure drops every server the file declared);
 * the shape mirrors the per-scope diagnostics a UI panel renders ("Failed to parse
 * <path>: <message>").
 */
export interface McpConfigError {
  path: string;
  message: string;
  severity: McpConfigSeverity;
}

/**
 * A single structured MCP config diagnostic with full per-entry attribution,
 * mirroring the reference's `ValidationError` + `mcpErrorMetadata` (McpParsingWarnings.tsx
 * `[Error]/[Warning] [serverName] path: message`). Unlike the coarse
 * {@link McpConfigError} (whole-file parse failures only), this unifies EVERY config
 * problem — file parse failures, per-server shape/validation errors (fatal), and
 * recoverable per-server warnings — into one severity-tagged, server-attributed,
 * JSON-path-bearing list for a `/mcp` diagnostics view.
 */
export interface McpConfigDiagnostic {
  /** Severity — fatal (config not loaded) or warning (loaded, recoverable). */
  severity: McpConfigSeverity;
  /** Originating server name, when the diagnostic is attributable to one entry. */
  serverName?: string;
  /** Absolute path of the config file the problem originates from, when known. */
  path?: string;
  /** JSON path to the offending value within the config (e.g. `mcpServers.foo`). */
  jsonPath?: string;
  /** Human-readable, actionable message. */
  message: string;
}

/**
 * The config-source scope a live MCP server was loaded from (the reference-parity
 * `ConfigScope`, MCPListPanel.tsx). `project`/`local`/`user`/`enterprise` each
 * come from a config file at that tier; `dynamic` is a built-in / programmatically
 * added server with no backing file. `MCP_SCOPE_ORDER` is the deterministic
 * display order (dynamic/built-in last). This is the LOAD-ORIGIN scope of a live
 * server, distinct from the narrower WRITE-target scope (`local|project|user`) the
 * `/mcp add` command persists to.
 */
export const MCP_SCOPE_ORDER = ['project', 'local', 'user', 'enterprise', 'dynamic'] as const;

export type McpServerScope = (typeof MCP_SCOPE_ORDER)[number];

/** Human-readable heading label for a scope group (the reference-parity getScopeHeading). */
export function mcpScopeLabel(scope: McpServerScope): string {
  switch (scope) {
    case 'project': return 'Project MCPs';
    case 'local': return 'Local MCPs';
    case 'user': return 'User MCPs';
    case 'enterprise': return 'Enterprise MCPs';
    case 'dynamic': return 'Built-in MCPs';
  }
}
