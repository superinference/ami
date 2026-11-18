import { McpClient, McpToolSchema, McpResource, McpPrompt, McpPromptMessage, McpClientState, McpTerminalError, McpSessionExpiredError, MCP_PROTOCOL_VERSION, getMcpConnectTimeout } from './client';
import type { McpElicitationRequest, McpElicitationResult, McpSamplingRequest, McpSamplingResult, McpLogMessage, McpIdeSelection, McpIdeAtMention } from './client';
import { McpOAuthSession, FileOAuthTokenStore, mcpTokenFileName, mcpCredentialKey } from './oauth';
import type { McpOAuthProvider, AuthorizeFn, FetchLike, XaaConfig } from './oauth';
import {
  MCP_NEEDS_AUTH_CACHE_TTL_MS,
  isNeedsAuthCached,
  setNeedsAuthEntry,
  clearNeedsAuthEntry,
  pruneNeedsAuthCache,
  readNeedsAuthCache,
  writeNeedsAuthCache,
  type NeedsAuthCache,
} from './needs-auth-cache';
import { resolveConfigDir } from '../reference-journal';
import { findAvailablePort, buildRedirectUri, DEFAULT_OAUTH_CALLBACK_PORT } from './oauth-port';
import { getMcpToolTimeout, normalizeMcpNameSegment, qualifiedMcpToolName } from '../tools/mcp-tool';
import { classifyMcpToolForCollapse } from './classify-collapse';
import {
  resolveEnvSecretRefs,
  resolveKeychainSecretRefs,
  headersHaveKeychainSecretRef,
  describeSecretRef,
  defaultMcpSecretStore,
  type McpSecretStore,
} from './secret-store';
import { McpToolPoller, type McpToolPollSpec } from './tool-polling';
import { stripInvisibleUnicode, recursivelySanitizeUnicode } from '../tools/input-sanitizer';
import { EventEmitter } from 'events';
import { execFile } from 'child_process';
import { createHash } from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { log as coreLog } from '../logger';
import { validateBoundedIntEnvVar } from '../config';
import { isEnvTruthy } from '../privacy-level';
import { isEnvDefinedFalsy } from '../utils/env';
import { resolveRuntime, buildContainerArgs, isImageAvailable, pullImage, type ContainerConfig } from './container';
import { logMcpError as logMcpServerError, logMcpDebug as logMcpServerDebug, getMcpServerLogPath, type McpLogSinkOptions } from './log-sink';

/** Read a positive-int env override, else the default. */
function envPositiveInt(name: string, fallback: number): number {
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
 * (Claude-parity env knob), or undefined when unset. When set, the redirect URI
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
 * positive integer within the 1..65535 range (Claude-parity `--callback-port`
 * validation). A missing / non-integer / out-of-range value is rejected so a
 * malformed config falls through to the env knob / dynamic port discovery rather than
 * building an invalid redirect URI.
 */
export function isValidCallbackPort(port: number | undefined): port is number {
  return typeof port === 'number' && Number.isInteger(port) && port > 0 && port <= 65535;
}

/**
 * Resolve the OAuth loopback redirect URI for a server, with Claude-parity precedence
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

const DEFAULT_MCP_DESCRIPTION_LENGTH = 2048;
// Hard upper ceiling for SUPERINFERENCE_MAX_MCP_DESCRIPTION_LENGTH. A mistaken or
// hostile value (e.g. =99999999) would let a single MCP server bloat EVERY tool
// description and exhaust the model's context window (DoS). Parity: Claude Code's
// validateBoundedIntEnvVar upper-limit cap + capped/invalid diagnostic logging.
const MCP_DESCRIPTION_UPPER_LIMIT = 100_000;
function getMcpMaxDescriptionLength(): number {
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
 * Produce an MCP server base URL that is SAFE to write to a log/analytics sink:
 * the query string is dropped (it can carry access tokens, e.g. a `?token=...`
 * SSE URL) and the trailing slash is normalized away. Returns undefined for a
 * stdio server (no `url`) or a config whose `url` does not parse — so a caller can
 * simply omit the URL from a log line when this returns undefined. Mirrors Claude's
 * `getLoggingSafeMcpBaseUrl`; reuses no state, so it is a pure helper the manager
 * routes every logged server URL through.
 */
export function getLoggingSafeMcpBaseUrl(config: McpServerConfig | undefined): string | undefined {
  if (!config || typeof config.url !== 'string') return undefined;
  try {
    const url = new URL(config.url);
    url.search = '';
    return url.toString().replace(/\/$/, '');
  } catch {
    return undefined;
  }
}

const TRANSIENT_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN']);
/**
 * Normalize an MCP upstream URL for denylist comparison: `protocol//host/path`
 * with host lower-cased and trailing slashes stripped. Returns null when the
 * input is not a URL (e.g. a bare server name), so callers can route it to
 * name-matching instead.
 */
export function normalizeMcpUrl(raw: string): string | null {
  try {
    const u = new URL(raw.trim());
    const pathname = u.pathname.replace(/\/+$/, '');
    return `${u.protocol}//${u.host.toLowerCase()}${pathname}`;
  } catch {
    return null;
  }
}

/**
 * A structured MCP allow/deny policy entry (parity with Claude's
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
   * Provenance-scoped entry (Claude-parity channelAllowlist): approval keyed to the
   * plugin bundle that produced a server rather than its arbitrary local name.
   * `pluginSource` matches {@link McpServerConfig.pluginSource} exactly (or as a
   * `*`-wildcard family). The Claude-shaped `{ marketplace, plugin }` form compiles
   * to the `<plugin>@<marketplace>` provenance convention {@link scopePluginMcpServers}
   * stamps, and also admits the marketplace-agnostic bare `<plugin>`.
   */
  | { pluginSource: string }
  | { marketplace: string; plugin: string };

/**
 * Resolve the set of server names a user has toggled OFF via the persisted
 * `disabledMcpServers` / `enabledMcpServers` config knobs (parity with Claude
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
interface CompiledMcpPolicy {
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

function compileMcpPolicy(entries: readonly McpPolicyEntry[], opts?: { caseInsensitiveNames?: boolean }): CompiledMcpPolicy {
  const ci = opts?.caseInsensitiveNames === true;
  const foldName = (n: string): string => (ci ? n.toLowerCase() : n);
  const p: CompiledMcpPolicy = { ci, names: new Set(), namePatterns: [], urls: new Set(), urlPatterns: [], commands: [], pluginSources: new Set(), pluginSourcePatterns: [] };
  const addName = (n: string): void => {
    const trimmed = n.trim();
    if (trimmed.includes('*')) p.namePatterns.push(wildcardToRegExp(foldName(trimmed)));
    else p.names.add(foldName(trimmed));
  };
  const addPluginSource = (s: string): void => {
    const trimmed = s.trim();
    if (!trimmed) return;
    if (trimmed.includes('*')) p.pluginSourcePatterns.push(wildcardToRegExp(foldName(trimmed)));
    else p.pluginSources.add(foldName(trimmed));
  };
  for (const e of entries || []) {
    if (!e) continue;
    if (typeof e === 'string') {
      const s = e.trim();
      if (!s) continue;
      if (s.includes('*')) {
        if (/:\/\//.test(s)) p.urlPatterns.push(wildcardToRegExp(s));
        else p.namePatterns.push(wildcardToRegExp(foldName(s)));
      } else {
        const norm = normalizeMcpUrl(s);
        if (norm) p.urls.add(norm);
        else p.names.add(foldName(s));
      }
      continue;
    }
    if ('serverName' in e && typeof e.serverName === 'string' && e.serverName) {
      addName(e.serverName);
    } else if ('serverCommand' in e && Array.isArray(e.serverCommand)) {
      p.commands.push(e.serverCommand.map(String));
    } else if ('serverUrl' in e && typeof e.serverUrl === 'string' && e.serverUrl) {
      if (e.serverUrl.includes('*')) p.urlPatterns.push(wildcardToRegExp(e.serverUrl));
      else { const norm = normalizeMcpUrl(e.serverUrl); if (norm) p.urls.add(norm); }
    } else if ('pluginSource' in e && typeof e.pluginSource === 'string' && e.pluginSource) {
      addPluginSource(e.pluginSource);
    } else if ('marketplace' in e && 'plugin' in e && typeof e.marketplace === 'string' && typeof e.plugin === 'string' && e.plugin) {
      // Claude's {marketplace, plugin} provenance maps onto AMI's single
      // `pluginSource` tag: scopePluginMcpServers stamps `<plugin>@<marketplace>`,
      // so admit that composed form AND the marketplace-agnostic bare plugin name.
      addPluginSource(`${e.plugin}@${e.marketplace}`);
      addPluginSource(e.plugin);
    }
  }
  return p;
}

/** Config fields a policy can match against (name is passed separately). */
interface McpPolicyTarget { url?: string; command?: string; args?: string[]; pluginSource?: string }

/** True when `name`/`config` matches any entry in the compiled policy. */
function matchMcpPolicy(policy: CompiledMcpPolicy, name: string, config?: McpPolicyTarget): boolean {
  const candidate = policy.ci ? name.trim().toLowerCase() : name.trim();
  if (policy.names.has(candidate)) return true;
  if (policy.namePatterns.some((re) => re.test(candidate))) return true;
  if (config?.url) {
    const raw = config.url.trim();
    const norm = normalizeMcpUrl(raw);
    if (norm && policy.urls.has(norm)) return true;
    if (policy.urlPatterns.some((re) => re.test(raw) || (norm !== null && re.test(norm)))) return true;
  }
  if (config?.command && policy.commands.length > 0) {
    const actual = [config.command, ...(config.args ?? [])];
    if (policy.commands.some((c) => c.length === actual.length && c.every((x, i) => x === actual[i]))) return true;
  }
  if (config?.pluginSource) {
    const src = policy.ci ? config.pluginSource.trim().toLowerCase() : config.pluginSource.trim();
    if (policy.pluginSources.has(src)) return true;
    if (policy.pluginSourcePatterns.some((re) => re.test(src))) return true;
  }
  return false;
}

/**
 * A non-transient connect failure that looks like an MCP protocol handshake
 * failure — typically an older/non-compliant server that dies or errors on the
 * `initialize` exchange for the current protocol version (M3). We do NOT attempt
 * backwards-compatible negotiation; we surface a clear, actionable error instead
 * of the generic "exited with code N".
 */
export function isHandshakeFailure(err: unknown): boolean {
  if (isTransientConnectError(err)) return false;
  const msg = err instanceof Error ? err.message : String(err);
  return /exited with code|\binitialize\b|protocol[- ]?version|unsupported protocol|protocolVersion/i.test(msg);
}

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
 * Claude Code's dedicated needs-auth connection state.
 */
export function isAuthRequiredError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  return /OAuth consent|requires an interactive frontend|\b401\b|\b403\b|unauthorized|authorization failed|needs? auth|WWW-Authenticate/i.test(msg);
}

export function isTransientConnectError(err: unknown): boolean {
  if (err instanceof McpSessionExpiredError) return false;
  const code = (err as { code?: string })?.code;
  if (code && TRANSIENT_CODES.has(code)) return true;
  const msg = err instanceof Error ? err.message : String(err);
  return /ECONNREFUSED|ECONNRESET|ETIMEDOUT|EPIPE|socket hang up|connect ENOENT/i.test(msg);
}

/**
 * Severity of an MCP config diagnostic (Claude-parity `ValidationError.severity`,
 * McpParsingWarnings.tsx:L210-212). A `fatal` problem means the offending config —
 * a whole file (parse failure) or a single server entry (invalid shape) — was NOT
 * loaded; a `warning` problem is recoverable (the server still loads, e.g. an unset
 * env var or the Windows npx-wrapper footgun).
 */
export type McpConfigSeverity = 'fatal' | 'warning';

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
 * mirroring Claude's `ValidationError` + `mcpErrorMetadata` (McpParsingWarnings.tsx
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
 * The config-source scope a live MCP server was loaded from (Claude-parity
 * `ConfigScope`, MCPListPanel.tsx). `project`/`local`/`user`/`enterprise` each
 * come from a config file at that tier; `dynamic` is a built-in / programmatically
 * added server with no backing file. `MCP_SCOPE_ORDER` is the deterministic
 * display order (dynamic/built-in last). This is the LOAD-ORIGIN scope of a live
 * server, distinct from the narrower WRITE-target scope (`local|project|user`) the
 * `/mcp add` command persists to.
 */
export const MCP_SCOPE_ORDER = ['project', 'local', 'user', 'enterprise', 'dynamic'] as const;
export type McpServerScope = (typeof MCP_SCOPE_ORDER)[number];

/** Human-readable heading label for a scope group (Claude-parity getScopeHeading). */
export function mcpScopeLabel(scope: McpServerScope): string {
  switch (scope) {
    case 'project': return 'Project MCPs';
    case 'local': return 'Local MCPs';
    case 'user': return 'User MCPs';
    case 'enterprise': return 'Enterprise MCPs';
    case 'dynamic': return 'Built-in MCPs';
  }
}

export interface McpServerConfig {
  command: string;
  args?: string[];
  env?: Record<string, string>;
  headers?: Record<string, string>;
  /**
   * A shell command run at connect time (http/sse) that prints a JSON object of
   * dynamic request headers on stdout — git-credential-helper style, for rotating
   * / short-lived auth tokens. Merged over the static `headers` (dynamic wins). A
   * failing or malformed helper is logged and ignored so it never blocks connect.
   */
  headersHelper?: string;
  requestTimeout?: number;
  connectTimeout?: number;
  autoConnect?: boolean;
  /**
   * When false, disable the client's automatic reconnect retry loop for this
   * server (a caller/supervisor owns recovery). Defaults to true. Parity with
   * Claude Code's `WebSocketTransportOptions.autoReconnect`.
   */
  autoReconnect?: boolean;
  /**
   * Transport discriminator. Beyond the base transports (stdio / sse / http /
   * ws / wss), AMI recognizes the Claude-parity IDE discriminants `sse-ide`
   * (an SSE endpoint exposed by a code editor) and `ws-ide` (a WebSocket one),
   * plus `sdk` (an in-process / host-wired server). An IDE discriminant carries
   * a human-readable {@link McpServerConfig.ideName} and is connected over its
   * BASE transport ({@link canonicalMcpTransport}: `sse-ide`→`sse`,
   * `ws-ide`→`ws`), so no new dispatch path is needed — only the reserved `ide`
   * server name ({@link IDE_MCP_SERVER_NAME}) and the editor-name badge differ.
   */
  transport?: 'stdio' | 'sse' | 'http' | 'ws' | 'wss' | 'sse-ide' | 'ws-ide' | 'sdk';
  url?: string;
  /**
   * Human-readable name of the code editor backing an IDE MCP server (e.g.
   * "VS Code", "JetBrains"), surfaced on the `/mcp` IDE-connection badge
   * (Claude-parity `ideName`, useIdeConnectionStatus.ts:L20-24). Only meaningful
   * when {@link McpServerConfig.transport} is `sse-ide` / `ws-ide`;
   * {@link getMcpIdeName} gates on that so a stray `ideName` on a non-IDE server
   * is ignored.
   */
  ideName?: string;
  containerConfig?: ContainerConfig;
  /**
   * User-entered config values for a plugin/bundle-declared server, keyed by the
   * field name the plugin's `userConfig` schema declares. Referenced from the
   * server's `command`/`args`/`url`/`env`/`headers` via `${user_config.<key>}`
   * placeholders (Claude-parity `resolvePluginMcpEnvironment`,
   * mcpPluginIntegration.ts:L475-490). Substituted BEFORE general `${ENV}`
   * expansion so a user/plugin value wins over a same-named process env var.
   */
  userConfig?: Record<string, string>;
  /**
   * Install directory of the plugin that packaged this server. Injected into a
   * stdio server's env as `CLAUDE_PLUGIN_ROOT` and substitutable via
   * `${CLAUDE_PLUGIN_ROOT}` so a bundled stdio server can locate its own files
   * (Claude-parity, mcpPluginIntegration.ts:L510-521).
   */
  pluginRoot?: string;
  /**
   * Persistent per-plugin data directory. Injected into a stdio server's env as
   * `CLAUDE_PLUGIN_DATA` and substitutable via `${CLAUDE_PLUGIN_DATA}`. The two
   * injected env keys are literal paths — NOT run through `${ENV}` expansion
   * (Claude-parity, mcpPluginIntegration.ts:L517).
   */
  pluginDataDir?: string;
  /**
   * Provenance tag for a server contributed by a plugin/bundle (the bundle name or
   * install source). Set by {@link scopePluginMcpServers} alongside the
   * `plugin:<name>:<server>` name scoping so the `/mcp` view can attribute a
   * scoped server back to the plugin that shipped it (Claude-parity
   * `ScopedMcpServerConfig.pluginSource`, mcpPluginIntegration.ts:L341-360).
   */
  pluginSource?: string;
  /**
   * Tools on this server to poll periodically. Each spec names a bare (server-local)
   * tool, a poll cadence, and optional fixed arguments; whenever a polled tool's
   * output *changes* between polls the manager emits `tool-polling-update` and the
   * engine surfaces it to the model as an `<mcp-polling-update type="tool" …>` block
   * (parity: Claude Code's `UserResourceUpdateMessage` polling branch). Polling runs
   * only while the server is connected (started on `ready`, stopped on close/remove).
   */
  pollTools?: McpToolPollSpec[];
  /** Enable OAuth 2.1 for this (http/sse) server. The authorizer is wired at the manager level. */
  oauth?: {
    redirectUri?: string;
    clientId?: string;
    clientSecret?: string;
    clientMetadataUrl?: string;
    scope?: string;
    authServerMetadataUrl?: string;
    loginHint?: string;
    refreshBufferMs?: number;
    /**
     * Pin the OAuth loopback callback to this fixed port for THIS server (Claude-parity
     * `mcp add --callback-port`). Per-server override of the global
     * {@link getConfiguredOAuthCallbackPort} env knob: when set, the redirect URI uses
     * this port (no dynamic discovery) so a user behind a firewall can allow-list a
     * stable per-server callback port. An explicit `redirectUri` still wins over it.
     */
    callbackPort?: number;
    /**
     * Browserless Cross-App Access config (XAA / SEP-990) for an enterprise shared-IdP
     * server. When set AND the {@link MCP_ENABLE_XAA_ENV} gate is on, the server
     * authenticates via the RFC 8693/7523 token-exchange flow using the pre-obtained
     * `idpIdToken` instead of an interactive browser consent — the headless/enterprise
     * path. Ignored (normal consent flow) when the gate is off.
     */
    xaa?: XaaConfig;
  };
}

export const MCP_SERVER_STATES = ['pending', 'connecting', 'ready', 'error', 'stopped', 'disabled', 'needs-auth'] as const;
export type McpServerState = (typeof MCP_SERVER_STATES)[number];

export interface McpServerStatus {
  name: string;
  state: McpClientState | McpServerState;
  serverInfo: { name: string; version: string } | null;
  toolCount: number;
  resourceCount: number;
  /** Count of MCP prompts (server "commands") the server exposes. */
  promptCount?: number;
  uptime: number | null;
  imageAvailable?: boolean;
  containerBacked?: boolean;
  /**
   * Authentication status for a remote (sse/http) server: true when it holds a
   * working session (connected/ready with tools) — mirrors Claude's auth badge.
   * `undefined` for stdio servers (not auth-scoped) and non-ready remotes.
   */
  isAuthenticated?: boolean;
  /**
   * True when a remote (sse/http) server is in the `needs-auth` state — its OAuth
   * consent is required/unavailable so it is not connected. Mirrors Claude's
   * per-server `needsAuth` auth-status field (MCPAgentServerMenu.tsx:L108-158),
   * the negative counterpart of {@link isAuthenticated}, so a UI can render a
   * distinct "needs authentication" affordance. `undefined` when not applicable.
   */
  needsAuth?: boolean;
  /**
   * The count of consecutive failed reconnect attempts currently in flight for a
   * dropped server (Claude-parity `reconnectAttempt`, MCPListPanel.tsx:L315-325),
   * so a status view can render "reconnecting (n/{@link maxReconnectAttempts})…".
   * `undefined`/absent when no reconnect is in progress.
   */
  reconnectAttempt?: number;
  /** The reconnect-attempt ceiling ({@link MAX_MCP_RECONNECT_ATTEMPTS}); present
   *  only alongside an in-flight {@link reconnectAttempt} for the "n/max" display. */
  maxReconnectAttempts?: number;
  /**
   * The config-source scope this server was loaded from (Claude-parity scope
   * grouping). `undefined` only before scope has been recorded. See
   * {@link McpServerScope}.
   */
  scope?: McpServerScope;
  /** Absolute path of the config file this server was loaded from (if any), so a
   *  diagnostics/listing view can show the user which file to edit. */
  sourcePath?: string;
  /**
   * The skill/stance (agent) definitions that pulled this server in — the reverse
   * mapping of which `mcp-servers`-declaring stance(s) introduced a given MCP server
   * (Claude-parity `AgentMcpServerInfo.sourceAgents`, MCPAgentServerMenu.tsx:L139-142
   * "Used by: <agent list>"). De-duplicated + alphabetically sorted. `undefined` for a
   * server no stance declares (e.g. a plain config-file server), so a `/mcp` view can
   * render provenance only where it exists. Populated via
   * {@link McpManager.addServerSourceAgent}.
   */
  sourceAgents?: string[];
  /**
   * True when the connected server OPTED IN to relaying permission prompts over a
   * message channel by declaring BOTH experimental capabilities `claude/channel`
   * and `claude/channel/permission` (Claude-parity `filterPermissionRelayClients`,
   * channelPermissions.ts:L177-194). Lets a `/mcp` diagnostics view flag a server
   * as a permission-relay surface. `false` for a server that declares neither or
   * only one; derived from {@link McpClient.supportsChannelPermissionRelay}.
   */
  channelPermissionRelay?: boolean;
  error?: string;
}

/**
 * Group a flat list of MCP server statuses by their originating stance/agent
 * (Claude-parity `MCPListPanel` agent grouping, MCPListPanel.tsx:L166-170/L408): each
 * server is emitted once under EVERY agent in its {@link McpServerStatus.sourceAgents}
 * list, agent names are de-duplicated, and both the agent headings and the servers
 * within each group are alphabetically ordered for a stable `/mcp` rendering. Servers
 * with no `sourceAgents` are omitted (they appear in the ungrouped/by-scope view). The
 * single source of truth a `/mcp` "Used by @<agent>" section renders.
 */
export function groupMcpServersBySourceAgent(
  statuses: McpServerStatus[],
): Array<{ agent: string; servers: McpServerStatus[] }> {
  const byAgent = new Map<string, McpServerStatus[]>();
  for (const status of statuses) {
    for (const agent of status.sourceAgents ?? []) {
      const bucket = byAgent.get(agent);
      if (bucket) bucket.push(status);
      else byAgent.set(agent, [status]);
    }
  }
  return [...byAgent.entries()]
    .map(([agent, servers]) => ({
      agent,
      servers: servers.slice().sort((a, b) => a.name.localeCompare(b.name)),
    }))
    .sort((a, b) => a.agent.localeCompare(b.agent));
}

/**
 * A partition of the live MCP servers into the two actionable connectivity buckets
 * Claude surfaces as proactive notifications (useMcpConnectivityStatus.tsx:L24-75):
 * `failed` servers are in a hard `error` state, `needsAuth` servers are in the
 * recoverable `needs-auth` state (the user can re-authenticate). Each bucket is a
 * de-duplicated, display-ordered (alphabetical, inherited from
 * {@link McpManager.listAllServers}) list of server names. The single source of truth
 * a `/mcp` view / startup banner renders via {@link formatMcpConnectivitySummary}.
 */
export interface McpConnectivitySummary {
  failed: string[];
  needsAuth: string[];
}

/**
 * A tally of the live MCP servers bucketed by distinct connectivity state
 * (Claude-parity `buildMcpProperties`, status.tsx:L89-115): a compact
 * `{connected, pending, needsAuth, failed}` count for a status-pane summary line
 * that stays ONE row no matter how many servers are configured (20+ servers would
 * otherwise wrap over many rows and dominate the pane). The reserved `ide`
 * pseudo-client ({@link IDE_MCP_SERVER_NAME}) is excluded — it is surfaced
 * separately as the IDE badge ({@link McpManager.getIdeConnectionStatus}) — and
 * the intentional user states (`disabled`/`stopped`/`disconnected`) are counted
 * in NO bucket (they are not actionable connectivity states, not failures).
 * Feeds {@link formatMcpStateSummary}.
 */
export interface McpServerStateSummary {
  connected: number;
  pending: number;
  needsAuth: number;
  failed: number;
}

export interface PendingServerInfo {
  name: string;
  config: McpServerConfig;
  state: McpServerState;
  error?: string;
  imageAvailable?: boolean;
  /**
   * M14 — true when this is a PROJECT-tier server from a repo's config that has
   * not yet been approved by the user. Such a server is registered as pending and
   * is NOT auto-connected (a cloned repo could point MCP at anything); it only
   * connects after an explicit {@link McpManager.approvePendingServer} call.
   */
  needsApproval?: boolean;
}

/** Expand env-var references in header values. Delegates to the single canonical
 *  {@link expandEnvVars} so headers get the same `${VAR}` / `${VAR:-default}` /
 *  bare-`$VAR` semantics as command/args/url (no second expansion dialect). */
/** Max time a `headersHelper` script may run before it is aborted (ms). */
const HEADERS_HELPER_TIMEOUT_MS = 10_000;

/** Runs a `headersHelper` shell command and resolves with its stdout. Injectable so
 *  tests exercise the parse/merge logic without spawning a real process. */
export type HeadersHelperRunner = (command: string, env: NodeJS.ProcessEnv) => Promise<string>;

const defaultHeadersHelperRunner: HeadersHelperRunner = (command, env) =>
  new Promise<string>((resolve, reject) => {
    execFile(
      command,
      [],
      { shell: true, timeout: HEADERS_HELPER_TIMEOUT_MS, maxBuffer: 1024 * 1024, env },
      (err, stdout) => (err ? reject(err) : resolve(String(stdout))),
    );
  });

/**
 * Resolve the effective request headers for a server: its static `headers` (with
 * `${VAR}` expansion) merged with the dynamic headers produced by running its
 * `headersHelper` (dynamic overrides static). The helper runs with the server name
 * and URL in the environment (git-credential-helper style) so one script can serve
 * many servers. A failing / non-object / non-string-valued helper output is logged
 * and ignored — the static headers are still returned so a bad helper never blocks
 * the connection.
 */
/**
 * Introspection hooks for {@link resolveMcpServerHeaders} (Claude-parity
 * `logMCPDebug`/`logMCPError` around header retrieval). Injected by the manager so the
 * per-server MCP log records WHEN a helper ran and HOW MANY headers it produced — key
 * debuggability into what a server will actually send. Both default to no-ops so the
 * standalone function stays side-effect-free when called directly (tests/consumers).
 */
export interface HeadersHelperLogHooks {
  onDebug?: (message: string) => void;
  onError?: (error: unknown) => void;
}

export async function resolveMcpServerHeaders(
  serverName: string,
  config: McpServerConfig,
  run: HeadersHelperRunner = defaultHeadersHelperRunner,
  logHooks?: HeadersHelperLogHooks,
  secretStore: McpSecretStore = defaultMcpSecretStore(),
): Promise<Record<string, string>> {
  // Static headers: `${VAR}` + `${secret:env:...}` already resolved by
  // resolveEnvVars; now resolve any `${secret:keychain:...}` refs against the OS
  // keychain. An unresolved (absent / unavailable / timed-out) keychain ref
  // leaves its literal token in place AND is recorded as a config warning so the
  // dropped credential is visible rather than silently sent as a broken token.
  const staticHeaders = config.headers ? resolveEnvVars(config.headers) : {};
  for (const [name, value] of Object.entries(staticHeaders)) {
    if (!headersHaveKeychainSecretRef({ [name]: value })) continue;
    const { resolved, unresolved } = await resolveKeychainSecretRefs(value, secretStore);
    staticHeaders[name] = resolved;
    for (const ref of unresolved) {
      logHooks?.onError?.(new Error(`unresolved keychain secret ref ${describeSecretRef(ref)} for header "${name}"`));
    }
    if (resolved !== value) logHooks?.onDebug?.(`Resolved keychain secret ref(s) for header "${name}"`);
  }
  if (!config.headersHelper) return staticHeaders;
  try {
    logHooks?.onDebug?.('Executing headersHelper to get dynamic headers');
    const stdout = await run(config.headersHelper, {
      ...process.env,
      AMI_MCP_SERVER_NAME: serverName,
      ...(config.url ? { AMI_MCP_SERVER_URL: config.url } : {}),
    });
    const trimmed = stdout.trim();
    if (!trimmed) throw new Error('headersHelper produced no output');
    const parsed: unknown = JSON.parse(trimmed);
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      throw new Error('headersHelper must return a JSON object of string header values');
    }
    const dynamic: Record<string, string> = {};
    for (const [k, v] of Object.entries(parsed as Record<string, unknown>)) {
      if (typeof v !== 'string') throw new Error(`headersHelper returned a non-string value for "${k}"`);
      dynamic[k] = v;
    }
    logHooks?.onDebug?.(`Successfully retrieved ${Object.keys(dynamic).length} headers from headersHelper`);
    return { ...staticHeaders, ...dynamic };
  } catch (err) {
    coreLog('mcp', `headersHelper for "${serverName}" failed: ${err instanceof Error ? err.message : String(err)}`);
    logHooks?.onError?.(err);
    return staticHeaders;
  }
}

function resolveEnvVars(headers: Record<string, string>): Record<string, string> {
  const resolved: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    // `${VAR}` expansion first, then `${secret:env:NAME}` references (both are
    // plain env lookups, safe without workspace trust). `${secret:keychain:...}`
    // refs are intentionally left as literal tokens here — they are resolved on
    // the async, trust-gated keychain pass in {@link resolveMcpServerHeaders}.
    resolved[key] = resolveEnvSecretRefs(expandEnvVars(value));
  }
  return resolved;
}

/**
 * Factory for creating the underlying MCP client. Overridable on an
 * {@link McpManager} instance purely as a dependency-injection seam for tests
 * (so the resolved command/args/timeouts passed to the client can be observed);
 * production always uses the default `new McpClient(...)`.
 */
export type McpClientFactory = (options: ConstructorParameters<typeof McpClient>[0]) => McpClient;

export class McpManager extends EventEmitter {
  private clients = new Map<string, McpClient>();
  private configs = new Map<string, McpServerConfig>();
  private _allTools = new Map<string, { serverName: string; schema: McpToolSchema }>();
  private healthCheckInterval: NodeJS.Timeout | null = null;
  /**
   * Wall-clock time of the last health-check tick, used to detect process
   * suspension (a tick that fires far later than its interval). `null` until
   * {@link startHealthChecks} seeds it.
   */
  private _lastHealthTick: number | null = null;
  private rootPaths: string[] = [];
  private reconnectTimers = new Map<string, ReturnType<typeof setTimeout>>();
  /** Per-server count of consecutive failed reconnect attempts (for backoff). */
  private reconnectAttempts = new Map<string, number>();
  /**
   * Tool-polling scheduler (parity: Claude Code's `mcp-polling-update`). Started
   * per-server from {@link McpServerConfig.pollTools} on `ready`, stopped on
   * close/remove, and torn down with {@link disconnectAll}. Change-detected polls
   * re-emit as a `tool-polling-update` event the engine drains and injects.
   */
  private _toolPoller = new McpToolPoller({
    call: (server, tool, args) => this.callTool(`${server}:${tool}`, args),
    onUpdate: (u) => this.emit('tool-polling-update', u.server, u.tool, u.reason),
    onError: (server, tool, err) =>
      coreLog('mcp', `tool-polling '${server}:${tool}' failed: ${err instanceof Error ? err.message : String(err)}`),
  });
  /**
   * Names currently being torn down intentionally (stop/remove/disconnectAll), so
   * the transport 'close' handler does NOT auto-reconnect a server the user just
   * stopped. Cleared when the server (re)connects.
   */
  private _intentionalDisconnect = new Set<string>();
  /**
   * Per-server OAuth cancellation controllers. Aborting one cancels any in-flight
   * OAuth HTTP request (discovery / DCR / consent / token exchange) for that server
   * when it is stopped or removed, so a torn-down server never leaves a hung auth
   * call running.
   */
  private _oauthAbort = new Map<string, AbortController>();
  private pendingServers = new Map<string, PendingServerInfo>();
  private serverErrors = new Map<string, string>();
  /** Config-source scope per server (Claude-parity scope grouping). Recorded at
   *  load/add time; a programmatically-added server defaults to `'dynamic'`. */
  private _serverScopes = new Map<string, McpServerScope>();
  /** Enterprise exclusive-control: when true only `enterprise`-scope servers load
   *  (Claude-parity `allowManagedMcpServersOnly`). Toggled via
   *  {@link setAllowManagedServersOnly}. */
  private _allowManagedServersOnly = false;
  /** Absolute config-file path a server was loaded from (undefined for dynamic). */
  private _serverSources = new Map<string, string>();
  /** Reverse index of which skill/stance (agent) definition(s) declared each server
   *  (Claude-parity `AgentMcpServerInfo.sourceAgents`). Recorded when a stance
   *  registers its `mcp-servers`; surfaced on {@link McpServerStatus.sourceAgents}. */
  private _serverSourceAgents = new Map<string, Set<string>>();
  /**
   * Content hash of the RAW config entry each config-loaded server was last
   * registered from (Claude-parity config-hash change detection,
   * useManageMCPConnections excludeStalePluginClients). {@link reloadFromConfig}
   * diffs a re-read file against these hashes so an UNCHANGED server is left
   * running untouched while a server whose entry CHANGED is torn down and
   * re-registered with the fresh config. Keyed by server name; absent for a
   * programmatically-added (dynamic) server. */
  private _serverConfigHashes = new Map<string, string>();
  /** Per-server non-fatal config diagnostics (e.g. unset referenced env vars) — H35. */
  private _configWarnings = new Map<string, string[]>();
  /**
   * Per-server schema/shape validation errors — a server entry that is malformed
   * (wrong-typed `args`/`env`, unknown transport, non-object) or that threw while
   * loading. Such an entry is SKIPPED individually (per-server isolation) instead
   * of aborting the whole file or being silently cast + passed through. Surfaced
   * via {@link getConfigValidationErrors} for a /mcp diagnostics view.
   */
  private _configValidationErrors = new Map<string, string>();
  /**
   * Source config-file path for each server that produced a validation error
   * (keyed by server name). A skipped invalid entry is never registered, so its
   * path cannot live in {@link _serverSources}; this preserves per-entry path
   * attribution for {@link getConfigDiagnostics}. */
  private _configValidationSources = new Map<string, string>();
  /**
   * Per-file FATAL config diagnostics — a config file that exists but could not be
   * parsed (invalid JSON). Keyed by the config file path. A MISSING file is NOT an
   * error (expected), but a malformed one is surfaced rather than silently swallowed
   * so the user learns their `.mcp.json` is broken instead of just losing its servers.
   */
  private _configErrors = new Map<string, McpConfigError>();
  /**
   * M14 — names of PROJECT-tier MCP servers the user has approved. Seeded from a
   * persisted store by the integrator ({@link setApprovedServers}) and extended by
   * {@link approvePendingServer}. A project server in this set connects normally;
   * one that is not is gated behind approval (registered pending, not auto-connected).
   */
  private _approvedServers = new Set<string>();
  /**
   * M14 (U018) — names of PROJECT-tier MCP servers the user has explicitly
   * REJECTED (Claude's persisted `disabledMcpjsonServers`). This is the negative
   * counterpart of {@link _approvedServers}: a rejected project server is
   * SUPPRESSED on every load (never registered, never re-surfaced for approval)
   * instead of re-appearing as `pending` each session. Seeded from a persisted
   * store by the integrator ({@link setRejectedServers}) and extended by
   * {@link rejectServer} / {@link rejectAllPending}. Explicit reject takes
   * precedence over approval (see {@link getProjectMcpServerStatus}).
   */
  private _rejectedServers = new Set<string>();
  /**
   * M14 (U018) — blanket "trust all project MCP servers" flag (Claude's
   * `enableAllProjectMcpServers`). When true, every project-tier server that is
   * not explicitly rejected is approved without a per-server prompt, so a user
   * who trusts a repo can opt every current AND future `.mcp.json` server in with
   * one decision. Seeded via {@link setEnableAllProjectServers}.
   */
  private _enableAllProjectServers = false;
  /**
   * Names of INTERNAL / managed MCP servers that are protected from the user-facing
   * enable/disable toggle (Claude-parity `c.name !== 'ide'` toggle guard,
   * commands/mcp/mcp.tsx:L31-32). A built-in server the host wires itself (e.g. an
   * IDE bridge) must not be clobbered by a `/mcp disable` (bulk `all` or targeted),
   * so {@link stopServer} refuses to DISABLE a protected server. Seeded by the
   * integrator via {@link setProtectedServers}. Name matching is
   * normalization-insensitive (same as the approval sets).
   */
  private _protectedServers = new Set<string>();
  /**
   * When true, every spawned stdio MCP server is told — via an env var in its
   * child process ({@link MCP_BYPASS_PERMISSION_ENV}={@link MCP_BYPASS_PERMISSION_VALUE})
   * — that the host session runs in a bypass / auto-allow permission mode, so a
   * server that gates its own tool invocations can skip its in-server prompts and
   * stay non-interactive (Claude-parity: CLAUDE_CHROME_PERMISSION_MODE=
   * 'skip_all_permission_checks' is injected into the MCP server env when the session
   * is in bypass mode). Off by default; set by the integrator from the resolved
   * session permission mode. Only stdio (process-spawning) servers receive it — a
   * remote http/sse server has no child env to carry it.
   */
  private _bypassPermissions = false;
  /**
   * Names of MCP servers that have successfully connected at least once this
   * install (Claude-parity `claudeAiMcpEverConnected`). This gates startup
   * connectivity notifications: a server that worked before and is now failed is a
   * state change worth surfacing, whereas one that has NEVER connected (e.g.
   * needs-auth since it first appeared) is one the user has demonstrably not set up
   * and should not nag about. Seeded from a persisted store by the integrator
   * ({@link setEverConnectedServers}) and extended automatically on the first
   * successful connect of each server. NOT cleared on {@link disconnectAll} — it is
   * install-scoped history, not live transport state.
   */
  private _everConnected = new Set<string>();
  /**
   * Config-loaded server names that were SUPPRESSED as duplicates of an
   * already-registered server of the same name (Claude-parity
   * `mcp-server-suppressed-duplicate`, services/mcp/config.ts:L1216-L1229). AMI
   * resolves MCP config by first-wins precedence (a higher-precedence scope's
   * server keeps the name), so a lower-precedence config declaring the same name is
   * dropped. Previously that drop surfaced as a FATAL "already registered" validation
   * error; it is instead recorded here as an INFORMATIONAL diagnostic (name →
   * `duplicateOf`, the winning server's name) so a user with two configs hitting the
   * same name learns WHY their second entry isn't showing up rather than seeing a
   * spurious parse failure. Surfaced via {@link getSuppressedDuplicates} and folded
   * into {@link getConfigDiagnostics} as a warning. Cleared on {@link disconnectAll}.
   */
  private _suppressedDuplicates = new Map<string, string>();

  /** Timestamp (ms) of the last tool call per server name. */
  _lastUsedAt = new Map<string, number>();
  /** Idle timeout in ms — servers unused for this long are stopped. Default 30 min. */
  _idleTimeoutMs = 30 * 60 * 1000;
  private _idleCheckTimer: ReturnType<typeof setInterval> | null = null;

  /** Compiled allowlist policy (null = unset = allow all). */
  private _allowedPolicy: CompiledMcpPolicy | null = null;
  private _deniedServers: string[] = [];
  /** Compiled denylist policy (name/URL/wildcard/command-array). */
  private _deniedPolicy: CompiledMcpPolicy = compileMcpPolicy([], { caseInsensitiveNames: true });

  // Container-runtime hooks. Default to the real implementations; overridable in
  // tests so the auto-pull decision path can be driven deterministically without a
  // real docker/podman. Production behavior is identical to calling the imports.
  _resolveRuntime: typeof resolveRuntime = resolveRuntime;
  _isImageAvailable: typeof isImageAvailable = isImageAvailable;
  _pullImage: typeof pullImage = pullImage;
  /** MCP client factory seam (see {@link McpClientFactory}). */
  _createClient: McpClientFactory = (options) => new McpClient(options);
  /**
   * OS-keychain secret-store seam ({@link McpSecretStore}) used to resolve
   * `${secret:keychain:...}` header refs at connect time. Defaults to the real
   * platform keychain; overridable in tests so keychain resolution can be driven
   * deterministically without a real keychain.
   */
  _secretStore: McpSecretStore = defaultMcpSecretStore();
  /**
   * Per-server structured log sink seams (Claude-parity `logMCPError`/`logMCPDebug`).
   * Default to the real on-disk JSONL writers; overridable in tests so the per-server
   * logging path is observable without touching the real config dir. Production
   * behavior is identical to calling the imports.
   */
  _logMcpError: (serverName: string, error: unknown, opts?: McpLogSinkOptions) => void = logMcpServerError;
  _logMcpDebug: (serverName: string, message: string, opts?: McpLogSinkOptions) => void = logMcpServerDebug;
  /**
   * Session id stamped into each per-server MCP log record (Claude-parity
   * `getSessionId()` in logMCPError/logMCPDebug). Set by the integrator from the live
   * session; omitted from records when unset.
   */
  private _mcpLogSessionId: string | undefined;
  /**
   * Host platform used for platform-specific config diagnostics (the Windows
   * npx-wrapper warning). Defaults to `process.platform`; overridable in tests so
   * the win32-only diagnostic path is exercisable on any CI host.
   */
  _platform: NodeJS.Platform = process.platform;

  /**
   * TTL for the needs-auth skip cache. Defaults to the Claude-parity 15 min;
   * overridable in tests so the expiry path is exercisable without real time.
   */
  _needsAuthCacheTtlMs: number = MCP_NEEDS_AUTH_CACHE_TTL_MS;
  /**
   * Lazily-loaded needs-auth cache (server name → last-401 timestamp), persisted
   * cross-session. `undefined` until first use; see {@link loadNeedsAuthCache}.
   */
  private _needsAuthCache: NeedsAuthCache | undefined;
  /**
   * Remote servers skipped THIS session because their connect was short-circuited
   * by a fresh needs-auth cache entry (so {@link getFullServerStatus}/
   * {@link getServerState} can report `needs-auth` without a probe). Cleared on a
   * successful connect or an explicit re-auth.
   */
  private _needsAuthSkipped = new Set<string>();

  /** Engine-injected resolver for server elicitation requests (server name + request → user's answer). */
  private _elicitationHandler: ((serverName: string, request: McpElicitationRequest) => Promise<McpElicitationResult>) | null = null;
  /** Engine-injected resolver for server sampling requests (run an LLM turn on the server's behalf). */
  private _samplingHandler: ((serverName: string, request: McpSamplingRequest, signal?: AbortSignal) => Promise<McpSamplingResult>) | null = null;

  /**
   * Wire the elicitation resolver. Read at call-time, so it applies to servers
   * already added as well as future ones. Absent a handler, every client
   * declines (never hangs) — the detached-mode default.
   */
  setElicitationHandler(fn: (serverName: string, request: McpElicitationRequest) => Promise<McpElicitationResult>): void {
    this._elicitationHandler = fn;
  }

  /**
   * Wire the sampling resolver. Absent a handler, clients refuse with a JSON-RPC
   * error. The optional `signal` is the per-request abort forwarded from the
   * client (server `notifications/cancelled` or a dropped connection) so an
   * in-flight server-initiated model turn can be cancelled.
   */
  setSamplingHandler(fn: (serverName: string, request: McpSamplingRequest, signal?: AbortSignal) => Promise<McpSamplingResult>): void {
    this._samplingHandler = fn;
  }

  /** HTTP fetch seam for OAuth discovery/token calls (overridable in tests). */
  _fetch: FetchLike = (globalThis.fetch as unknown as FetchLike);
  /** Interactive OAuth consent, injected by a frontend. Headless default refuses with guidance. */
  private _oauthAuthorizer: AuthorizeFn = async () => {
    throw new Error('OAuth consent requires an interactive frontend; none is wired (headless/detached run)');
  };

  /** Last OAuth authorization URL presented per server (for relaying to the model). */
  private _lastAuthUrls = new Map<string, string>();

  /** Wire the interactive OAuth consent leg (browser open + redirect capture). */
  setOAuthAuthorizer(fn: AuthorizeFn): void {
    this._oauthAuthorizer = fn;
  }

  /**
   * The authorization URL most recently generated for `name`'s OAuth flow, if any.
   * Captured whenever the consent leg is invoked (even when a headless authorizer
   * then refuses), so the `authenticate` tool can relay the URL to the model/user
   * in a detached run instead of only failing (H46/H48/H49).
   */
  getPendingAuthUrl(name: string): string | undefined {
    return this._lastAuthUrls.get(name);
  }

  setRootPaths(paths: string[]): void {
    const changed = paths.length !== this.rootPaths.length || paths.some((p, i) => p !== this.rootPaths[i]);
    this.rootPaths = paths;
    if (!changed) return;
    // Propagate the new roots to every already-connected client and let each push a
    // `notifications/roots/list_changed` (MCP spec, built on the client's public
    // outbound {@link McpClient.notify} API) so a long-lived server re-requests
    // `roots/list`. Fire-and-forget per client — a push onto a dropped/!ready
    // connection is skipped inside `setRoots`, and a failure is logged to the
    // per-server MCP log, never thrown (a roots update must not break the caller).
    for (const [name, client] of this.clients) {
      void client.setRoots(paths).catch((err) => {
        this._logMcpError(name, err, this.mcpLogOpts());
      });
    }
  }

  /** The workspace roots currently advertised to MCP servers (introspection). */
  getRootPaths(): string[] {
    return [...this.rootPaths];
  }

  /**
   * Set the session id stamped into per-server MCP log records (Claude-parity
   * `getSessionId()`). The integrator calls this once with the live session id so a
   * `/mcp` diagnostics reader can correlate a server's log entries with the session
   * that produced them. Read at log-write time, so it applies to already-registered
   * servers as well as future ones.
   */
  setMcpLogSessionId(sessionId: string | undefined): void {
    this._mcpLogSessionId = sessionId;
  }

  /** Build the per-server log-sink options from the live session id (the clock/cwd
   *  seams default inside the sink). */
  private mcpLogOpts(): McpLogSinkOptions | undefined {
    return this._mcpLogSessionId ? { sessionId: this._mcpLogSessionId } : undefined;
  }

  /**
   * The dated JSONL log-file path for `name`'s dedicated per-server MCP log
   * (Claude-parity `getMCPLogsPath`). Lets a `/mcp` diagnostics view / introspection
   * tell the user exactly which file holds a specific server's error+debug history.
   */
  getServerLogPath(name: string): string {
    return getMcpServerLogPath(name, this.mcpLogOpts());
  }

  /**
   * Configure the allowlist. Accepts plain names (back-compat) or structured
   * {@link McpPolicyEntry} entries (name/command-array/URL, with `*` wildcards) so
   * an allowlist can match by command or URL pattern, not just name. A non-null
   * (even empty) list switches to default-closed; `null` is never set here, so an
   * empty list blocks everything (unchanged semantics).
   */
  setAllowedServers(entries: McpPolicyEntry[]): void {
    this._allowedPolicy = compileMcpPolicy(entries);
    this._deniedServers = [];
  }

  /**
   * Enterprise exclusive-control switch (Claude-parity `allowManagedMcpServersOnly`):
   * when enabled, ONLY admin-provisioned (`enterprise`-scope, via
   * {@link loadManagedServers}) MCP servers are loaded — every user/project/local/
   * dynamic server is blocked at load with a `config-blocked` diagnostic. Set by the
   * integrator from the trusted managed config BEFORE any {@link loadFromConfig} so a
   * non-managed server is never registered or connected. Default off (all scopes load).
   */
  setAllowManagedServersOnly(enabled: boolean): void {
    this._allowManagedServersOnly = enabled;
  }

  /** True when only enterprise-scope MCP servers are permitted to load. */
  isManagedServersOnly(): boolean {
    return this._allowManagedServersOnly;
  }

  isServerAllowed(serverName: string, config?: McpPolicyTarget): boolean {
    if (this._allowedPolicy === null) return true;
    return matchMcpPolicy(this._allowedPolicy, serverName, config);
  }

  getDeniedServers(): string[] {
    return [...this._deniedServers];
  }

  private enforceAllowlist(name: string, config?: McpPolicyTarget): void {
    if (this._allowedPolicy !== null && !matchMcpPolicy(this._allowedPolicy, name, config)) {
      if (!this._deniedServers.includes(name)) {
        this._deniedServers.push(name);
      }
      throw new Error(`MCP server "${name}" is not in the allowed servers list`);
    }
  }

  /**
   * Configure servers to deny by name, normalized/wildcard upstream URL, or full
   * stdio command array (S9 + Claude allow/deny parity). Default-open: an empty
   * list blocks nothing. A plain-string entry is a name or exact URL (as before);
   * a `*`-bearing string is a URL wildcard (if it looks like a URL) or a name
   * wildcard; a structured entry targets `serverName`/`serverCommand`/`serverUrl`.
   * So two differently-named servers at the same upstream are both blocked, a
   * whole subdomain family can be denied via `https://*.evil.com/*`, and a stdio
   * server can be blocked by its command regardless of its local name.
   */
  setDeniedServerList(entries: McpPolicyEntry[]): void {
    this._deniedPolicy = compileMcpPolicy(entries, { caseInsensitiveNames: true });
  }

  /**
   * Per-server config diagnostics collected at load time — currently the names of
   * environment variables a server config references but that are unset. Surfaced
   * so a misconfigured server is diagnosable rather than silently blanked (H35).
   */
  getConfigWarnings(name: string): string[] {
    return this._configWarnings.get(name) ?? [];
  }

  /** All per-server config diagnostics as a map (for a /mcp diagnostics view). */
  getAllConfigWarnings(): Record<string, string[]> {
    return Object.fromEntries(this._configWarnings);
  }

  /**
   * Fatal per-file config diagnostics: config files that exist but failed to parse.
   * Surfaced in the /mcp diagnostics view so a broken `.mcp.json` is visible instead
   * of silently dropping every server it declared (distinguishes malformed from
   * missing — a missing file is expected and produces no error).
   */
  getConfigErrors(): McpConfigError[] {
    return [...this._configErrors.values()];
  }

  /**
   * Per-server config validation errors (malformed/skipped server entries), as a
   * `{ serverName: message }` map for a /mcp diagnostics view. An entry here was
   * NOT registered (skipped) because its shape was invalid.
   */
  getConfigValidationErrors(): Record<string, string> {
    return Object.fromEntries(this._configValidationErrors);
  }

  /**
   * Config-loaded servers that were suppressed as duplicates of an
   * already-registered server with the same name (first-wins precedence) —
   * `{ name, duplicateOf }` pairs for a /mcp diagnostics view (Claude-parity
   * `mcp-server-suppressed-duplicate`). Informational, not an error: the duplicate
   * entry simply never registered because a higher-precedence config already owns
   * the name.
   */
  getSuppressedDuplicates(): Array<{ name: string; duplicateOf: string }> {
    return [...this._suppressedDuplicates].map(([name, duplicateOf]) => ({ name, duplicateOf }));
  }

  /**
   * Unified, severity-tagged, per-entry MCP config diagnostics (Claude-parity
   * McpParsingWarnings `ValidationError` + `mcpErrorMetadata`): one flat list that
   * merges every config problem — whole-file PARSE failures and per-server shape
   * VALIDATION errors (both `fatal`, the config was not loaded) and recoverable
   * per-server WARNINGS (unset env vars, the Windows npx footgun — the server still
   * loaded) — each carrying its `severity`, originating `serverName` (when
   * attributable), config-file `path`, and a `jsonPath` to the offending entry
   * (`mcpServers.<name>`). Fatal diagnostics are listed before warnings; within a
   * severity, entries are ordered by server name (file-only parse errors, which have
   * no server, sort first). This is the single structured surface a `/mcp`
   * diagnostics view renders as `[Error]/[Warning] [server] path: message`.
   */
  getConfigDiagnostics(): McpConfigDiagnostic[] {
    const fatal: McpConfigDiagnostic[] = [];
    const warnings: McpConfigDiagnostic[] = [];
    // Whole-file parse failures (no server attribution).
    for (const err of this._configErrors.values()) {
      fatal.push({ severity: 'fatal', path: err.path, message: err.message });
    }
    // Per-server shape/validation failures (the entry was skipped).
    for (const [name, message] of this._configValidationErrors) {
      fatal.push({
        severity: 'fatal',
        serverName: name,
        path: this._configValidationSources.get(name) ?? this._serverSources.get(name),
        jsonPath: `mcpServers.${name}`,
        message,
      });
    }
    // Per-server recoverable warnings (the server still loaded) — one diagnostic
    // per distinct warning so each is attributable on its own row.
    for (const [name, list] of this._configWarnings) {
      for (const message of list) {
        warnings.push({
          severity: 'warning',
          serverName: name,
          path: this._serverSources.get(name),
          jsonPath: `mcpServers.${name}`,
          message,
        });
      }
    }
    // Suppressed duplicates (informational): a lower-precedence config entry
    // dropped because a server with the same name is already registered.
    for (const [name, duplicateOf] of this._suppressedDuplicates) {
      warnings.push({
        severity: 'warning',
        serverName: name,
        jsonPath: `mcpServers.${name}`,
        message: `MCP server "${name}" was suppressed: it duplicates the already-configured "${duplicateOf}" (first-wins precedence).`,
      });
    }
    const byServer = (a: McpConfigDiagnostic, b: McpConfigDiagnostic): number =>
      (a.serverName ?? '').localeCompare(b.serverName ?? '');
    fatal.sort(byServer);
    warnings.sort(byServer);
    return [...fatal, ...warnings];
  }

  /**
   * Partition the live MCP servers into the `failed` (hard `error` state) and
   * `needsAuth` (recoverable `needs-auth` state) buckets Claude surfaces as proactive
   * connectivity notifications (useMcpConnectivityStatus.tsx): an auth boundary the
   * user can fix by authenticating is kept distinct from a hard failure. Each bucket
   * is alphabetical (inherited from {@link listAllServers}). Feeds
   * {@link formatMcpConnectivitySummary} for the `/mcp` connectivity banner.
   */
  getConnectivitySummary(): McpConnectivitySummary {
    const failed: string[] = [];
    const needsAuth: string[] = [];
    for (const s of this.listAllServers()) {
      if (s.state === 'needs-auth') needsAuth.push(s.name);
      else if (s.state === 'error') failed.push(s.name);
    }
    return { failed, needsAuth };
  }

  /**
   * Bucket the live MCP servers by distinct connectivity state for a COMPACT
   * one-line status-pane summary (Claude-parity `buildMcpProperties`,
   * status.tsx:L89-115): `ready`→connected, `pending`/`connecting`→pending,
   * `needs-auth`→needsAuth, `error`→failed. The reserved `ide` pseudo-client is
   * excluded (surfaced as its own IDE badge via {@link getIdeConnectionStatus}),
   * and the intentional `disabled`/`stopped`/`disconnected` states are counted in
   * no bucket (not failures). Unlike {@link getConnectivitySummary} (which lists
   * only the two actionable buckets for proactive banners) this is the full
   * four-way COUNT the status pane renders via {@link formatMcpStateSummary}.
   */
  getServerStateSummary(): McpServerStateSummary {
    const summary: McpServerStateSummary = { connected: 0, pending: 0, needsAuth: 0, failed: 0 };
    for (const s of this.listAllServers()) {
      if (s.name === IDE_MCP_SERVER_NAME) continue;
      switch (s.state) {
        case 'ready': summary.connected++; break;
        case 'pending':
        case 'connecting': summary.pending++; break;
        case 'needs-auth': summary.needsAuth++; break;
        case 'error': summary.failed++; break;
        default: break; // disabled / stopped / disconnected: intentional, not an actionable bucket
      }
    }
    return summary;
  }

  /** True when a server should be blocked by the denylist (by name, URL, URL
   *  wildcard, or stdio command array). */
  isServerDenied(name: string, config?: McpPolicyTarget): boolean {
    return matchMcpPolicy(this._deniedPolicy, name, config);
  }

  addServer(name: string, config: McpServerConfig): void {
    if (this.isServerDenied(name, config)) {
      throw new Error(`MCP server "${name}" is blocked by the denied-servers list`);
    }
    this.enforceAllowlist(name, config);
    if (this.clients.has(name)) {
      throw new Error(`MCP server '${name}' already registered`);
    }
    // A server added without a recorded scope (programmatic/built-in add, not via
    // loadFromConfig/loadManagedServers) is the `dynamic` built-in scope.
    if (!this._serverScopes.has(name)) this._serverScopes.set(name, 'dynamic');

    let resolvedCommand = config.command || '';
    let resolvedArgs = config.args;

    if (config.containerConfig) {
      const runtime = this._resolveRuntime(config.containerConfig.runtime);
      if (runtime) {
        resolvedCommand = runtime;
        resolvedArgs = buildContainerArgs(config.containerConfig);
        coreLog('mcp', `container MCP for '${name}': ${runtime} ${resolvedArgs.join(' ')}`);
      } else {
        coreLog('mcp', `no container runtime found for '${name}'`);
      }
    }

    this.configs.set(name, config);
    // A stdio (process-spawning) server is one with no upstream `url` — a remote
    // http/sse server has no child env to carry the bypass signal.
    const isStdioServer = !config.url;
    const enrichedEnv = {
      ...config.env,
      ...(this.rootPaths[0] ? { SUPERINFERENCE_PROJECT_DIR: this.rootPaths[0] } : {}),
      // Tell a spawned stdio MCP server the host session is in bypass/auto-allow
      // mode so it can skip its own in-server permission prompts (Claude-parity
      // CLAUDE_CHROME_PERMISSION_MODE injection).
      ...(this._bypassPermissions && isStdioServer
        ? { [MCP_BYPASS_PERMISSION_ENV]: MCP_BYPASS_PERMISSION_VALUE }
        : {}),
    };
    // OAuth 2.1 (2025-06-18): for an http/sse server that opts in, give the client
    // a session that discovers the auth server on 401, drives consent, and manages
    // tokens. The interactive consent leg comes from the manager-level authorizer.
    const oauth = this.buildOAuthProvider(name, config);

    const client = this._createClient({
      command: resolvedCommand,
      args: resolvedArgs,
      env: enrichedEnv,
      headers: config.headers ? resolveEnvVars(config.headers) : undefined,
      requestTimeout: config.requestTimeout ?? (config.containerConfig ? 60000 : 30000),
      connectTimeout: config.connectTimeout ?? (config.containerConfig ? 120000 : getMcpConnectTimeout()),
      autoReconnect: config.autoReconnect,
      rootPaths: this.rootPaths,
      oauth,
    });

    client.on('error', (err) => {
      // Persist to the server's dedicated per-server log file (Claude-parity
      // logMCPError) in addition to the in-memory event, so a flaky server has an
      // isolated, auditable error trail distinct from every other server's.
      this._logMcpError(name, err, this.mcpLogOpts());
      this.emit('server-error', name, err);
    });
    client.on('close', (code, uptimeMs) => {
      // Pause polling while the transport is down — a disconnected server cannot be
      // polled; `ready` re-registers on reconnect so no poll is orphaned or leaked.
      this._toolPoller.unregister(name);
      this.emit('server-close', name, code, uptimeMs);
      // Close-triggered auto-reconnect for REMOTE (http/sse) transports: a dropped
      // network stream should recover immediately, not wait for the next 30s
      // health-check ping (which skips non-ready clients). Guarded so an intentional
      // teardown (stop/remove/disable) or a replaced client never reconnects.
      const cfg = this.configs.get(name);
      const t = this.effectiveTransport(cfg);
      // A server pinned autoReconnect:false owns its own recovery — never auto-drive
      // a reconnect for it (parity with Claude's autoReconnect=false → go to closed).
      if (isRemoteMcpTransport(t)
        && cfg?.autoReconnect !== false
        && !this._intentionalDisconnect.has(name)
        && this.clients.get(name) === client
        && !this.isDisabled(name)) {
        coreLog('mcp', `remote MCP '${name}' transport closed, scheduling reconnect`);
        this.scheduleReconnect(name);
      }
    });
    client.on('ready', () => {
      // Record the first-ever successful connect (gates startup connectivity
      // notifications — a server that has NEVER connected is not nagged about).
      this.markEverConnected(name);
      // Start (or, on reconnect, cleanly restart) this server's configured tool
      // polls now that its tools are callable (parity: Claude's mcp-polling-update).
      this._toolPoller.register(name, this.configs.get(name)?.pollTools);
      this.emit('server-ready', name);
    });
    // An unrecoverable auth failure on a live tool call flips the client to the
    // `needs-auth` state — surface it as a server-state change so the /mcp view can
    // prompt a re-auth (parity with Claude Code flipping a client to needs-auth on
    // McpAuthError during tool execution).
    client.on('needs-auth', () => {
      coreLog('mcp', `MCP '${name}' needs re-authentication (token revoked/expired)`);
      this.emit('server-state-changed', name, 'needs-auth');
    });
    client.on('notification', (method: string, params: unknown) => this.emit('notification', name, method, params));

    client.on('terminal-error', (classified: McpTerminalError) => {
      coreLog('mcp', `Terminal error for ${name}: [${classified.code}] ${classified.message}`);
      this._logMcpError(name, `[${classified.code}] ${classified.message}`, this.mcpLogOpts());
      this.emit('terminal-error', name, classified);
    });

    client.on('session-expired', () => {
      coreLog('mcp', `Session expired for ${name}, clearing cache and reconnecting`);
      this.invalidateToolCache(name);
      this.emit('session-expired', name);
    });

    client.on('tools-changed', () => {
      coreLog('mcp', `Tool list changed for ${name}, refreshing`);
      this.invalidateToolCache(name);
      this.refreshToolsForServer(name).catch(() => {});
      this.emit('tools-changed', name);
    });

    client.on('resources-changed', () => {
      coreLog('mcp', `Resource list changed for ${name}`);
      this.emit('resources-changed', name);
    });

    client.on('prompts-changed', () => {
      coreLog('mcp', `Prompt list changed for ${name}`);
      this.emit('prompts-changed', name);
    });

    client.on('log', (msg: McpLogMessage) => {
      // Route a server-emitted log line to the server's dedicated debug/error file
      // (Claude-parity logMCPDebug/logMCPError): an `error`-level line is logged as a
      // per-server error, everything else as per-server debug, so each server's log
      // is self-contained instead of interleaved in the shared engine log.
      const text = typeof msg.data === 'string' ? msg.data : JSON.stringify(msg.data);
      if (msg.level === 'error') this._logMcpError(name, text, this.mcpLogOpts());
      else this._logMcpDebug(name, text, this.mcpLogOpts());
      this.emit('server-log', name, msg);
    });
    client.on('resource-updated', (uri: string) => this.emit('resource-updated', name, uri));
    // Live per-tool-call progress (server notifications/progress for an in-flight
    // tools/call) surfaced on the manager's public event bus so a frontend can
    // render a progress bar (Claude-parity per-tool-call onProgress).
    client.on('tool-progress', (p: { name: string; percent?: number; message?: string }) => this.emit('tool-progress', name, p));
    client.on('pagination-capped', (method: string, count: number) => {
      coreLog('mcp', `Pagination cap hit for ${name} on ${method} (${count} items)`);
      this.emit('pagination-capped', name, method, count);
    });
    // IDE-as-MCP-server editor context: surface the normalized current selection /
    // at-mention on the manager's public bus (parity with Claude Code's
    // useIdeSelection / useIdeAtMentioned) so a frontend can fold the user's editor
    // context into the turn.
    client.on('ide-selection', (sel: McpIdeSelection) => this.emit('ide-selection', name, sel));
    client.on('ide-at-mentioned', (m: McpIdeAtMention) => this.emit('ide-at-mentioned', name, m));
    client.on('notification-error', (method: string, err: Error) => {
      coreLog('mcp', `Notification handler error for ${name} on ${method}: ${err.message}`);
      this.emit('notification-error', name, method, err);
    });

    // Server→client request resolvers. Read the manager handler at call-time so
    // a handler wired after the server was added still applies.
    client.onElicitation = (request) =>
      this._elicitationHandler
        ? this._elicitationHandler(name, request)
        : Promise.resolve({ action: 'decline' as const });
    client.onSampling = (request, signal) =>
      this._samplingHandler
        ? this._samplingHandler(name, request, signal)
        : Promise.reject(new Error(`sampling not supported (no handler) for server '${name}'`));

    // A freshly (re)registered client is a live server again — clear any stale
    // intentional-teardown marker so its transport closes auto-reconnect normally.
    this._intentionalDisconnect.delete(name);
    this.clients.set(name, client);
  }

  addPendingServer(name: string, config: McpServerConfig, opts?: { needsApproval?: boolean }): void {
    this.enforceAllowlist(name, config);
    if (this.clients.has(name) || this.pendingServers.has(name)) {
      throw new Error(`MCP server '${name}' already registered`);
    }
    if (!this._serverScopes.has(name)) this._serverScopes.set(name, 'dynamic');
    let imageAvailable: boolean | undefined;
    if (config.containerConfig) {
      const runtime = this._resolveRuntime(config.containerConfig.runtime);
      if (runtime) {
        imageAvailable = this._isImageAvailable(runtime, config.containerConfig.image);
      }
    }
    const info: PendingServerInfo = { name, config, state: 'pending', imageAvailable, needsApproval: opts?.needsApproval };
    this.pendingServers.set(name, info);
    this.configs.set(name, config);
    coreLog('mcp', `pending MCP server registered: ${name} (image available: ${imageAvailable ?? 'n/a'}${opts?.needsApproval ? ', awaiting approval' : ''})`);
    this.emit('server-state-changed', name, 'pending');
  }

  /**
   * M14 — seed the set of approved PROJECT-tier MCP servers. The integrator loads
   * this from a persisted store (approval survives restarts) before calling
   * {@link loadFromConfig}; a project server whose name is here connects normally,
   * all others are gated. Replaces any previously-set approvals.
   */
  setApprovedServers(names: string[]): void {
    this._approvedServers = new Set(names);
  }

  /** M14 — current approved PROJECT-tier server names (for the integrator to persist). */
  getApprovedServers(): string[] {
    return [...this._approvedServers];
  }

  /** M14 — true when the named project server has been approved. */
  isServerApproved(name: string): boolean {
    return this._approvedServers.has(name);
  }

  /**
   * M14 (U018) — seed the set of REJECTED (persistently declined) PROJECT-tier
   * MCP servers (Claude's `disabledMcpjsonServers`). The integrator loads this
   * from a persisted store before {@link loadFromConfig}; a project server whose
   * name is here is suppressed (never registered, never re-prompted). Replaces any
   * previously-set rejections.
   */
  setRejectedServers(names: string[]): void {
    this._rejectedServers = new Set(names);
  }

  /** M14 (U018) — current rejected PROJECT-tier server names (for persistence). */
  getRejectedServers(): string[] {
    return [...this._rejectedServers];
  }

  /**
   * M14 (U018) — set the blanket "trust all project MCP servers" flag
   * (`enableAllProjectMcpServers`). When on, every project server that is not
   * explicitly rejected is approved without prompting.
   */
  setEnableAllProjectServers(on: boolean): void {
    this._enableAllProjectServers = on;
  }

  /** M14 (U018) — current blanket project-trust flag (for persistence). */
  getEnableAllProjectServers(): boolean {
    return this._enableAllProjectServers;
  }

  /**
   * Seed the set of INTERNAL / managed MCP server names that are protected from the
   * user-facing enable/disable toggle (Claude-parity `ide`-client toggle guard). A
   * protected server stays under host control: {@link stopServer} refuses to DISABLE
   * it. The integrator calls this once with its built-in server names. Replaces any
   * previously-set protected names.
   */
  setProtectedServers(names: string[]): void {
    this._protectedServers = new Set(names);
  }

  /** Current protected (internal/managed) MCP server names. */
  getProtectedServers(): string[] {
    return [...this._protectedServers];
  }

  /**
   * True when `name` is an internal/managed server protected from the enable/disable
   * toggle. Matching is normalization-insensitive. A UI `/mcp disable`/`enable all`
   * path filters these out (parity with Claude's `c.name !== 'ide'`); the core
   * {@link stopServer} enforces it as the single source of truth.
   */
  isServerProtected(name: string): boolean {
    return this.nameInSet(this._protectedServers, name);
  }

  /**
   * Propagate the host session's bypass / auto-allow permission mode into spawned
   * stdio MCP servers (see {@link _bypassPermissions}). The integrator calls this
   * with `true` when the session is in `--yolo` / `bypassPermissions` /
   * `auto-allow` so an MCP server that runs its own permission layer can skip its
   * prompts and stay non-interactive, matching the host. Read at connect time, so
   * it applies to servers already registered as well as future ones. Idempotent.
   */
  setBypassPermissions(on: boolean): void {
    this._bypassPermissions = on;
  }

  /** Current bypass-permission propagation flag (introspection / persistence). */
  getBypassPermissions(): boolean {
    return this._bypassPermissions;
  }

  /**
   * Seed the set of servers that have EVER connected (Claude-parity
   * `claudeAiMcpEverConnected`). The integrator loads this from a persisted store
   * before connecting so a server that worked in a previous session is recognized
   * as a state-change (not a first-appearance) when it now fails. Replaces any
   * previous set.
   */
  setEverConnectedServers(names: string[]): void {
    this._everConnected = new Set(names);
  }

  /** Current ever-connected server names (for the integrator to persist). */
  getEverConnectedServers(): string[] {
    return [...this._everConnected];
  }

  /** True when the named server has successfully connected at least once (this
   *  session or seeded from persistence). */
  hasEverConnected(name: string): boolean {
    return this._everConnected.has(name);
  }

  /**
   * Record that `name` has successfully connected. Idempotent: the first time a
   * server is marked it emits `ever-connected` (so a persistence layer can save the
   * new entry); a repeat mark is a no-op. Returns true only on the first mark.
   */
  private markEverConnected(name: string): boolean {
    if (this._everConnected.has(name)) return false;
    this._everConnected.add(name);
    this.emit('ever-connected', name);
    return true;
  }

  /**
   * Import MCP servers from a desktop MCP client's config file (Claude-parity
   * `readClaudeDesktopMcpServers` one-shot migration). Reads the platform-resolved
   * `claude_desktop_config.json` (macOS / WSL only; see
   * {@link resolveDesktopMcpConfigPath}), validates each declared stdio server
   * through the same shape guard as a config file ({@link validateMcpServerConfigShape},
   * skipping malformed entries per-entry), resolves name collisions against the
   * ALREADY-registered servers with numbered `(2)`/`(3)` suffixes (see
   * {@link resolveMcpServerNameCollision}) so an import never clobbers an existing
   * server nor produces two tools with the same normalized `mcp__…` name, registers
   * the survivors under the `user` scope through the normal load path, and returns
   * the imported (collision-resolved) names. A no-op returning `[]` on an
   * unsupported platform or a missing/empty/unreadable config. The fs/platform
   * seams are injectable for testing.
   */
  importDesktopServers(opts?: {
    env?: DesktopImportEnv;
    readFile?: (configPath: string) => string;
  }): string[] {
    const discovered = readDesktopMcpServers(opts);
    const baseNames = Object.keys(discovered);
    if (baseNames.length === 0) return [];
    // Seed the used-normalized set with every currently-registered server so an
    // import de-dups against live servers, not just within the imported batch.
    const used = new Set<string>();
    for (const existing of this.configs.keys()) used.add(normalizeMcpNameSegment(existing));
    const toLoad: Record<string, unknown> = {};
    const imported: string[] = [];
    for (const base of baseNames) {
      const { name, normalized } = resolveMcpServerNameCollision(base, used);
      used.add(normalized);
      toLoad[name] = discovered[base];
      imported.push(name);
    }
    this.loadServerEntries(toLoad, { scope: 'user' });
    return imported;
  }

  /**
   * Introspection map from each live MCP tool's MODEL-FACING (normalized, qualified
   * `mcp__<server>__<tool>`) name back to its ORIGINAL server-side name + server
   * (Claude-parity `SerializedTool.originalToolName` / `MCPCliState.normalizedNames`).
   * Normalization sanitizes, hyphen-preserves, collapses, and (when overlong)
   * truncates+hashes a tool name, so the name the model calls can differ from the
   * real server-side tool; this lets a `/mcp` view or an error message trace a
   * sanitized name back to the tool the server actually exposes. Built from the live
   * tool cache; empty until servers have connected and listed their tools.
   */
  getToolNameMapping(): Array<{ server: string; originalName: string; normalizedName: string }> {
    const out: Array<{ server: string; originalName: string; normalizedName: string }> = [];
    for (const entry of this._allTools.values()) {
      out.push({
        server: entry.serverName,
        originalName: entry.schema.name,
        normalizedName: qualifiedMcpToolName(entry.serverName, entry.schema.name),
      });
    }
    return out;
  }

  /**
   * Reverse of {@link getToolNameMapping}: the original server-side tool name + its
   * server for a model-facing normalized `mcp__<server>__<tool>` name, or undefined
   * when no live tool maps to it.
   */
  getOriginalToolName(normalizedName: string): { server: string; originalName: string } | undefined {
    for (const entry of this._allTools.values()) {
      if (qualifiedMcpToolName(entry.serverName, entry.schema.name) === normalizedName) {
        return { server: entry.serverName, originalName: entry.schema.name };
      }
    }
    return undefined;
  }

  /** True when `name` matches any entry in `set` under MCP name normalization
   *  (Claude-parity: comparisons go through `normalizeNameForMCP` on BOTH sides,
   *  so a config name `my.server` and a stored decision `my_server` match). */
  private nameInSet(set: Set<string>, name: string): boolean {
    if (set.size === 0) return false;
    if (set.has(name)) return true;
    const norm = normalizeMcpNameSegment(name);
    for (const n of set) {
      if (normalizeMcpNameSegment(n) === norm) return true;
    }
    return false;
  }

  /**
   * M14 (U018) — resolve a PROJECT-tier server to its approval status with
   * Claude-parity layered precedence (getProjectMcpServerStatus): an explicit
   * reject wins first (`disabledMcpjsonServers`), then an explicit approval OR the
   * blanket enable-all flag (`enabledMcpjsonServers` / `enableAllProjectMcpServers`),
   * otherwise `pending`. All name comparisons are normalization-insensitive. This
   * is the single source of truth the load path uses to gate project servers.
   */
  getProjectMcpServerStatus(name: string): 'approved' | 'rejected' | 'pending' {
    if (this.nameInSet(this._rejectedServers, name)) return 'rejected';
    if (this._enableAllProjectServers || this.nameInSet(this._approvedServers, name)) return 'approved';
    return 'pending';
  }

  /**
   * Workspace-trust boundary for a config-supplied `headersHelper` SCRIPT
   * (Claude-parity getMcpHeadersFromHelper, services/mcp/headersHelper.ts:L40-57).
   * A `headersHelper` is an arbitrary shell command the config asks us to EXECUTE at
   * connect time — from a `project`-scoped (untrusted, cloned-repo) `.mcp.json` that
   * is an arbitrary-code-execution vector, so we must NOT run it until the project
   * server's trust is established. A project server is trusted here exactly when it
   * has been approved (per-server approval OR the blanket enable-all flag), which is
   * AMI's established project-MCP trust gate; every other scope (user / local /
   * enterprise / dynamic) is operator-supplied and trusted. Returns true when the
   * helper for `name` may run.
   */
  isHeadersHelperTrusted(name: string): boolean {
    if (this._serverScopes.get(name) !== 'project') return true;
    return this.getProjectMcpServerStatus(name) === 'approved';
  }

  /**
   * M14 (U018) — durably REJECT a project-tier MCP server (the approval dialog's
   * "no" branch → `disabledMcpjsonServers`). Records the rejection (so it is
   * returned by {@link getRejectedServers} for persistence and suppressed on every
   * future load), clears any stale approval, and tears down the server's live or
   * pending instance so it disappears now instead of lingering awaiting approval.
   */
  rejectServer(name: string): void {
    this._rejectedServers.add(name);
    this._approvedServers.delete(name);
    if (this.clients.has(name) || this.pendingServers.has(name)) {
      this.removeServer(name);
    }
  }

  /**
   * M14 (U018) — reject EVERY server currently awaiting approval (Claude's
   * cancel/ESC `handleEscRejectAll`): a cancellation of the new-MCP-servers prompt
   * is a committed decision, not a no-op that re-asks. Each pending-approval server
   * is durably rejected (and torn down). Returns the names rejected.
   */
  rejectAllPending(): string[] {
    const names = this.listPendingApprovals().map((p) => p.name);
    for (const n of names) this.rejectServer(n);
    return names;
  }

  /**
   * M14 (U018) — clear ALL persisted project-scope decisions (Claude's
   * `mcpResetChoicesHandler`): drop the approved set, the rejected set, and the
   * blanket enable-all flag, so the user is re-prompted for every project server
   * on the next load. The complement of {@link approvePendingServer} /
   * {@link rejectServer} — a single "forget my choices" reset.
   */
  resetProjectMcpChoices(): void {
    this._approvedServers.clear();
    this._rejectedServers.clear();
    this._enableAllProjectServers = false;
  }

  /**
   * M14 — true when the named server is registered pending BECAUSE it still needs
   * the user's approval (a not-yet-trusted project-tier config), as opposed to a
   * lazy/on-demand pending server.
   */
  needsApproval(name: string): boolean {
    const pending = this.pendingServers.get(name);
    return !!pending && pending.needsApproval === true && !this.clients.has(name);
  }

  /** M14 — list the project-tier servers currently awaiting user approval. */
  listPendingApprovals(): PendingServerInfo[] {
    const result: PendingServerInfo[] = [];
    for (const info of this.pendingServers.values()) {
      if (info.needsApproval && !this.clients.has(info.name)) result.push(info);
    }
    return result;
  }

  /**
   * M14 — approve a pending project-tier MCP server and connect it. Records the
   * approval (so it is returned by {@link getApprovedServers} for persistence and
   * the server connects normally next time), clears the approval gate, and brings
   * the server up via the normal on-demand connect path.
   */
  async approvePendingServer(name: string): Promise<void> {
    const pending = this.pendingServers.get(name);
    if (!pending) {
      throw new Error(`MCP server '${name}' is not pending approval`);
    }
    this._approvedServers.add(name);
    pending.needsApproval = false;
    await this.ensureConnected(name);
  }

  async ensureConnected(name: string): Promise<void> {
    if (this.clients.has(name)) {
      const client = this.clients.get(name)!;
      if (client.state === 'ready') return;
    }

    const pending = this.pendingServers.get(name);
    if (pending?.state === 'disabled') {
      throw new Error(`MCP server '${name}' is disabled. Re-enable it from the /mcp panel before connecting.`);
    }
    // M14 — a not-yet-approved project-tier server must not be connected on demand
    // (e.g. by the mcp-connect tool); it stays pending until explicitly approved.
    if (pending?.needsApproval) {
      throw new Error(`MCP server '${name}' requires approval before it can connect. Use /mcp approve ${name} to approve it.`);
    }
    if (!pending) {
      if (!this.clients.has(name)) throw new Error(`MCP server '${name}' not found`);
      await this.connectServer(name);
      return;
    }

    pending.state = 'connecting';
    this.serverErrors.delete(name);
    this.emit('server-state-changed', name, 'connecting');
    coreLog('mcp', `on-demand connecting MCP server: ${name}`);

    try {
      if (pending.config.containerConfig) {
        const runtime = this._resolveRuntime(pending.config.containerConfig.runtime);
        if (runtime && !this._isImageAvailable(runtime, pending.config.containerConfig.image)) {
          coreLog('mcp', `pulling image for ${name}: ${pending.config.containerConfig.image}`);
          await this._pullImage(runtime, pending.config.containerConfig.image);
          coreLog('mcp', `image pulled for ${name}`);
        }
      }
      this.pendingServers.delete(name);
      this.addServer(name, pending.config);
      await this.connectServer(name);

      pending.state = 'ready';
      this.emit('server-state-changed', name, 'ready');
      coreLog('mcp', `MCP server connected on-demand: ${name} (${this.getToolsForServer(name).length} tools)`);
    } catch (err) {
      const errMsg = err instanceof Error ? err.message : String(err);
      // Distinguish "needs authentication" from a hard error: an OAuth-protected
      // server whose consent is required/unavailable gets the recoverable
      // `needs-auth` state (surfaced for a "click to authenticate" affordance),
      // not `error` — Claude-parity for its dedicated needs-auth connection state.
      const authNeeded = isAuthRequiredError(err) || this._lastAuthUrls.has(name);
      const newState: McpServerState = authNeeded ? 'needs-auth' : 'error';
      pending.state = newState;
      pending.error = errMsg;
      this.serverErrors.set(name, errMsg);
      this.pendingServers.set(name, pending);
      if (this.clients.has(name)) {
        this.clients.get(name)!.disconnect();
        this.clients.delete(name);
      }
      this.emit('server-state-changed', name, newState);
      coreLog('mcp', `on-demand connect failed for ${name}: ${errMsg}`);
      throw err;
    }
  }

  async stopServer(name: string, disable?: boolean): Promise<void> {
    // An internal/managed server (e.g. a built-in IDE bridge) is protected from the
    // user-facing DISABLE toggle so it cannot be clobbered via `/mcp disable`
    // (targeted or bulk `all`) — Claude-parity `c.name !== 'ide'` toggle guard. A
    // plain stop (no `disable`) is still allowed (it is transient, not a persisted
    // user decision).
    if (disable && this.isServerProtected(name)) {
      throw new Error(`MCP server "${name}" is internal/managed and cannot be disabled.`);
    }
    // Cancel any in-flight OAuth flow for this server before teardown.
    this._oauthAbort.get(name)?.abort();
    this._oauthAbort.delete(name);
    const client = this.clients.get(name);
    if (client) {
      // Mark intentional so the close handler does not auto-reconnect a stopped server.
      this._intentionalDisconnect.add(name);
      client.disconnect();
      this.clients.delete(name);
      this.invalidateToolCache(name);
    }

    const timer = this.reconnectTimers.get(name);
    if (timer) { clearTimeout(timer); this.reconnectTimers.delete(name); }

    const newState: McpServerState = disable ? 'disabled' : 'stopped';
    const config = this.configs.get(name);
    if (config) {
      this.pendingServers.set(name, { name, config, state: newState });
    }
    this.serverErrors.delete(name);
    this.emit('server-state-changed', name, newState);
    coreLog('mcp', `MCP server ${newState}: ${name}`);
  }

  async restartServer(name: string): Promise<void> {
    await this.stopServer(name);
    await this.ensureConnected(name);
  }

  /**
   * Build the OAuth 2.1 provider for an http/sse server that opts in, or
   * `undefined` for a stdio server / one without an `oauth` block. Shared by
   * {@link registerClient} (consent-on-connect) and {@link authenticate}
   * (pre-authentication, no connect) so both drive the EXACT same discovery /
   * DCR / consent / token-exchange / refresh path against the SAME per-server
   * token store and cancellation controller — one canonical OAuth construction,
   * no second parallel implementation.
   */
  private buildOAuthProvider(name: string, config: McpServerConfig): McpOAuthProvider | undefined {
    if (!(config.oauth && (config.transport === 'http' || config.transport === 'sse') && config.url)) {
      return undefined;
    }
    // Resolve the loopback redirect URI + whether a dynamic free-port lookup is
    // needed, with Claude-parity precedence: explicit config redirectUri > per-server
    // `callbackPort` (`mcp add --callback-port`) > global MCP_OAUTH_CALLBACK_PORT env
    // knob > dynamic discovery at consent time.
    const redirect = resolveMcpOAuthRedirect(config.oauth, getConfiguredOAuthCallbackPort());
    // Persist tokens per server so authentication survives restarts (H22/H45).
    // Key the token file on the server NAME *and* a hash of its connection config
    // (type/url/headers) so a reconfigured server (changed URL/transport/headers)
    // does not reuse a stale grant that no longer applies — parity with Claude's
    // getServerKey. (Labeled functional change: the token filename now embeds the
    // config hash; an old name-only token file is simply re-authed, not reused.)
    const tokenStore = new FileOAuthTokenStore(
      this.mcpTokenFilePath(name, config),
      // Proactive refresh-ahead window: refresh this many ms before real expiry
      // so a long-running session never presents an expired token mid-request.
      { skewMs: config.oauth.refreshBufferMs },
    );
    // Per-server cancellation: aborting this controller (on stop/remove) aborts
    // any in-flight OAuth HTTP request so a user-cancelled / torn-down server
    // does not leave a hung consent/discovery call running.
    const oauthAbort = new AbortController();
    this._oauthAbort.get(name)?.abort();
    this._oauthAbort.set(name, oauthAbort);
    return new McpOAuthSession({
      serverUrl: config.url,
      fetchImpl: this._fetch,
      store: tokenStore,
      signal: oauthAbort.signal,
      // Step-by-step diagnostic logging: attribute each leg (discovery, DCR,
      // consent, token exchange, refresh, revoke) to the MCP log so an auth
      // failure is traceable to the exact leg. URLs/bodies are pre-redacted.
      onLog: (ev) => coreLog('mcp', `oauth[${name}] ${ev.step}: ${ev.message}${ev.ok === false ? ' (failed)' : ''}${ev.error ? ` — ${ev.error}` : ''}`),
      authorize: (params) => {
        // Record the URL before delegating so even a headless authorizer that
        // refuses still leaves the URL available to relay to the model (H46/H48/H49).
        this._lastAuthUrls.set(name, params.authorizationUrl);
        this.emit('oauth-authorization-url', name, params.authorizationUrl);
        return this._oauthAuthorizer(params);
      },
      // Port precedence: an explicit config redirectUri wins; else a per-server
      // callbackPort or the pinned MCP_OAUTH_CALLBACK_PORT env knob (fixed,
      // firewall-friendly); else a dynamically-discovered free loopback port at
      // consent time so a busy port never wedges auth (see resolveMcpOAuthRedirect).
      redirectUri: redirect.redirectUri,
      resolveRedirectUri: redirect.dynamic
        ? async () => buildRedirectUri(await findAvailablePort())
        : undefined,
      clientId: config.oauth.clientId,
      clientSecret: config.oauth.clientSecret,
      // CIMD (SEP-991): when configured (or via MCP_OAUTH_CLIENT_METADATA_URL) and
      // the AS advertises client_id_metadata_document_supported, the flow uses this
      // URL as the client_id instead of Dynamic Client Registration.
      clientMetadataUrl: config.oauth.clientMetadataUrl,
      scope: config.oauth.scope,
      authServerMetadataUrl: config.oauth.authServerMetadataUrl,
      loginHint: config.oauth.loginHint,
      // Browserless enterprise SSO (SEP-990): when configured + env-gated, skip the
      // browser and use the token-exchange flow with the pre-obtained id_token.
      xaa: config.oauth.xaa,
      // Proactively refresh long-lived server tokens ahead of expiry so a
      // long-running session never presents an expired token mid-request. The
      // background timer is unref'd and torn down with the per-server abort signal.
      autoRefresh: true,
    });
  }

  /**
   * Pre-authenticate a declared-but-not-connected http/sse OAuth server WITHOUT
   * establishing a full MCP session (Claude-parity MCPAgentServerMenu
   * `handleAuthenticate`, MCPAgentServerMenu.tsx:L56-81).
   *
   * Agent/skill-scoped MCP servers are lazy-connect: they only come up when the
   * stance that declares them is pinned. That makes their first tool call pay the
   * full interactive OAuth consent cost mid-run. This runs the OAuth flow ahead of
   * time — building a temp OAuth provider from the server's `{transport, url}`
   * config and driving discovery → consent → token-exchange so the credential is
   * cached before first use — and resolves WITHOUT spawning the transport or
   * discovering tools, exactly like Claude's temp-config `performMCPOAuthFlow`.
   *
   * Returns `{ authenticated }` — `true` when a token was obtained (cached for the
   * eventual connect) and `false` when no auth was required. Non-OAuth / stdio
   * servers (no `oauth` block or no `url`) throw, since there is nothing to
   * pre-authenticate. The `oauth-authorization-url` event fires during the flow
   * (same path as a live connect) so a frontend can relay the consent URL; an
   * optional AbortSignal cancels an in-flight flow.
   */
  async authenticate(name: string, opts?: { signal?: AbortSignal }): Promise<{ authenticated: boolean }> {
    const signal = opts?.signal;
    if (signal?.aborted) {
      throw new Error(`OAuth flow for "${name}" was aborted before it started`);
    }
    const config = this.configs.get(name) ?? this.pendingServers.get(name)?.config;
    if (!config) {
      throw new Error(`Unknown MCP server "${name}" — nothing to authenticate`);
    }
    const oauth = this.buildOAuthProvider(name, config);
    if (!oauth) {
      throw new Error(
        `MCP server "${name}" is not an OAuth http/sse server — pre-authentication only applies to remote servers with an \`oauth\` config`,
      );
    }
    // Drop any cached guaranteed-401 marker so an explicit pre-auth always re-probes
    // (parity with reconnect's clearMcpAuthCache on manual auth).
    this.clearNeedsAuthCache(name);
    // Mid-flight cancellation: aborting the external signal aborts the per-server
    // OAuth controller (cancelling any in-flight discovery/consent/token HTTP leg)
    // AND wins a race so the caller gets a deterministic abort rejection — the
    // underlying flow swallows a cancelled discovery into a `false` return, which
    // would otherwise be indistinguishable from "no auth required".
    let abortListener: (() => void) | undefined;
    const abortPromise = signal
      ? new Promise<never>((_resolve, reject) => {
          abortListener = () => {
            this._oauthAbort.get(name)?.abort();
            reject(new Error(`OAuth flow for "${name}" was aborted`));
          };
          signal.addEventListener('abort', abortListener, { once: true });
        })
      : undefined;
    // onUnauthorized(null) drives the full flow (silent refresh if a grant exists,
    // otherwise discovery → consent → token exchange) and returns true once a
    // token is cached in the per-server store — no transport is ever spawned.
    const flow = oauth.onUnauthorized(null).then((authenticated) => ({ authenticated }));
    try {
      return await (abortPromise ? Promise.race([flow, abortPromise]) : flow);
    } finally {
      if (signal && abortListener) signal.removeEventListener('abort', abortListener);
      // Never leak an unhandled rejection from the detached flow once the race has
      // already been decided (completed or aborted).
      flow.catch(() => {});
    }
  }

  /**
   * Re-establish a server's session for the `authenticate` pseudo-tool.
   *
   * FUNCTIONAL FIX (parity, U102): the `mcp__<server>__authenticate` tool called
   * `mcpManager.reconnect?.(name)`, but the manager exposed **no** `reconnect`
   * method — so against the real manager the optional call was a silent no-op and
   * the tool reported a false "already connected" success without ever attempting
   * a connection. This wires a real re-auth entry point the tool depends on.
   *
   * The auth tool calls this (not `ensureConnected`/`restartServer` directly) so
   * auth-cache invalidation and tool-set reconciliation live in one place: the
   * server's cached tool schemas are dropped first (so a post-auth tool-swap is
   * picked up on re-discovery), then the session is (re)established — a ready
   * server is fully restarted so a freshly-minted credential is used, while a
   * pending/errored/disconnected server is connected on demand (running the OAuth
   * consent leg when required). Connection failures propagate so the tool can
   * surface them instead of claiming success.
   */
  async reconnect(name: string): Promise<void> {
    this.invalidateToolCache(name);
    // An explicit re-auth/reconnect must bypass the needs-auth skip cache: the
    // user is actively asking to retry, so drop any cached guaranteed-401 marker
    // before re-probing (Claude-parity clearMcpAuthCache on manual auth).
    this.clearNeedsAuthCache(name);
    if (this.clients.get(name)?.state === 'ready') {
      await this.restartServer(name);
    } else {
      await this.ensureConnected(name);
    }
  }

  /**
   * Non-blocking, model-driven OAuth entry point for the `authenticate` tool.
   *
   * FUNCTIONAL FIX (parity, U102): unlike {@link reconnect} — which BLOCKS until
   * the full browser-consent flow settles — this starts the (re)connect in the
   * BACKGROUND and resolves as soon as EITHER the consent leg produces an
   * authorization URL (returned so a detached/headless agent can relay it to the
   * user) OR the flow completes silently (a cached/XAA token authenticated
   * without consent → `authUrl` undefined). The background reconnect runs to
   * completion and swaps the server's real tools in on success. This mirrors
   * Claude Code's `skipBrowserOpen` McpAuthTool (start → capture URL → return
   * immediately → reconnect in background) instead of hanging the tool call on
   * an interactive browser leg that a detached agent can never complete.
   *
   * The `oauth-authorization-url` event fires BEFORE the authorizer is awaited
   * (see {@link registerClient}), so the URL reliably wins the race against the
   * still-blocked consent leg. A pre-URL reconnect failure is folded into the
   * race so it surfaces to the caller. An optional AbortSignal cancels the wait:
   * if it fires before a URL or completion, the returned promise rejects with an
   * abort error (the detached reconnect is left to settle/clean up on its own).
   */
  async startAuth(name: string, opts?: { signal?: AbortSignal }): Promise<{ authUrl?: string }> {
    const signal = opts?.signal;
    if (signal?.aborted) {
      throw new Error(`OAuth flow for "${name}" was aborted before it started`);
    }

    let urlListener: ((n: string, url: string) => void) | undefined;
    const urlPromise = new Promise<string>((resolve) => {
      urlListener = (n, url) => { if (n === name) resolve(url); };
      this.on('oauth-authorization-url', urlListener);
    });

    // Background (re)connect. The consent leg emits 'oauth-authorization-url'
    // before it blocks on the authorizer, so urlPromise typically wins the race
    // well before this settles. A failure BEFORE any URL was emitted rejects the
    // race so the tool can report it instead of claiming success.
    const reconnectPromise = this.reconnect(name);
    const completion = reconnectPromise.then(() => null as string | null);

    let abortListener: (() => void) | undefined;
    const abortPromise = signal
      ? new Promise<never>((_resolve, reject) => {
          abortListener = () => reject(new Error(`OAuth flow for "${name}" was aborted`));
          signal.addEventListener('abort', abortListener, { once: true });
        })
      : undefined;

    try {
      const raced = await Promise.race<string | null>([
        urlPromise,
        completion,
        ...(abortPromise ? [abortPromise] : []),
      ]);
      return { authUrl: raced ?? undefined };
    } finally {
      if (urlListener) this.off('oauth-authorization-url', urlListener);
      if (abortListener) signal?.removeEventListener('abort', abortListener);
      // Never leak an unhandled rejection from the detached reconnect once the
      // race has already been decided (URL surfaced or aborted).
      reconnectPromise.catch(() => {});
    }
  }

  isPending(name: string): boolean {
    const pending = this.pendingServers.get(name);
    if (!pending || this.clients.has(name)) return false;
    return pending.state !== 'disabled';
  }

  isDisabled(name: string): boolean {
    const pending = this.pendingServers.get(name);
    return !!pending && pending.state === 'disabled';
  }

  enableServer(name: string): void {
    const pending = this.pendingServers.get(name);
    if (pending && pending.state === 'disabled') {
      pending.state = 'stopped';
      this.emit('server-state-changed', name, 'stopped');
      coreLog('mcp', `MCP server re-enabled: ${name}`);
    }
  }

  getServerState(name: string): McpServerState {
    const pending = this.pendingServers.get(name);
    if (pending && !this.clients.has(name)) return pending.state;
    const client = this.clients.get(name);
    if (!client) return 'pending';
    if (client.state === 'ready') return 'ready';
    // Short-circuited by the needs-auth skip cache (never probed this pass).
    if (this._needsAuthSkipped.has(name)) return 'needs-auth';
    if (client.state === 'connecting') return 'connecting';
    if (client.state === 'error') return 'error';
    // A connected client whose token died mid-session (a tool call hit an
    // unrecoverable 401/403) is parked in `needs-auth` so the UI can prompt a
    // re-auth via /mcp, rather than reporting a generic `stopped`/`error`.
    if (client.state === 'needs-auth') return 'needs-auth';
    return 'stopped';
  }

  getToolsForServer(name: string): McpToolSchema[] {
    const tools: McpToolSchema[] = [];
    for (const [, entry] of this._allTools) {
      if (entry.serverName === name) tools.push(entry.schema);
    }
    return tools;
  }

  listAllServers(): McpServerStatus[] {
    const result: McpServerStatus[] = [];
    const seen = new Set<string>();

    for (const name of this.clients.keys()) {
      seen.add(name);
      result.push(this.getFullServerStatus(name));
    }
    for (const [name, info] of this.pendingServers) {
      if (seen.has(name)) continue;
      seen.add(name);
      result.push({
        name,
        state: info.state,
        serverInfo: null,
        toolCount: 0,
        resourceCount: 0,
        uptime: null,
        imageAvailable: info.imageAvailable,
        containerBacked: !!info.config.containerConfig,
        // A pending server whose on-demand connect failed with an auth error is
        // parked in the `needs-auth` state — surface the per-server needsAuth flag.
        needsAuth: info.state === 'needs-auth' || undefined,
        ...this.reconnectProgress(name),
        scope: this._serverScopes.get(name),
        sourcePath: this._serverSources.get(name),
        ...this.sourceAgentsField(name),
        error: info.error || this.serverErrors.get(name),
      });
    }
    // Deterministic alphabetical order regardless of Map insertion order so the
    // /mcp listing is stable (Claude-parity).
    return result.sort((a, b) => a.name.localeCompare(b.name));
  }

  /** Authentication status for a remote (sse/http) server; undefined otherwise. */
  private deriveIsAuthenticated(config: McpServerConfig | undefined, client: McpClient): boolean | undefined {
    if (config?.transport !== 'http' && config?.transport !== 'sse') return undefined;
    return client.state === 'ready' && (client.tools?.length ?? 0) > 0;
  }

  /**
   * In-flight reconnect progress for `name` (Claude-parity reconnectAttempt /
   * maxReconnectAttempts). Returns the attempt count + ceiling ONLY while a
   * reconnect is actually scheduled (a pending timer with a recorded attempt),
   * else `{}` — so a healthy server carries no reconnect fields. The first attempt
   * is reported as 1 (human-facing "1/N"), not the 0-based internal counter.
   */
  private reconnectProgress(name: string): { reconnectAttempt?: number; maxReconnectAttempts?: number } {
    if (!this.reconnectTimers.has(name)) return {};
    const attempt = this.reconnectAttempts.get(name) ?? 0;
    return { reconnectAttempt: attempt + 1, maxReconnectAttempts: MAX_MCP_RECONNECT_ATTEMPTS };
  }

  /** The `sourceAgents` status field for `name` (spread into every status site), or `{}`
   *  when no stance declared the server so the field stays absent. */
  private sourceAgentsField(name: string): { sourceAgents?: string[] } {
    const agents = this.getServerSourceAgents(name);
    return agents ? { sourceAgents: agents } : {};
  }

  private getFullServerStatus(name: string): McpServerStatus {
    const client = this.clients.get(name);
    const config = this.configs.get(name);
    if (!client) {
      return {
        name, state: 'pending', serverInfo: null, toolCount: 0,
        resourceCount: 0, uptime: null, containerBacked: !!config?.containerConfig,
        scope: this._serverScopes.get(name), sourcePath: this._serverSources.get(name),
        ...this.sourceAgentsField(name),
      };
    }
    // A server whose connect was short-circuited by the needs-auth skip cache was
    // never probed, so its client is not `ready`; surface it as `needs-auth` (with
    // the affordance flag) so the /mcp view prompts a re-auth instead of showing a
    // misleading idle/connecting state.
    if (this._needsAuthSkipped.has(name) && client.state !== 'ready') {
      return {
        name, state: 'needs-auth', serverInfo: client.serverInfo, toolCount: client.tools.length,
        resourceCount: client.resources.length, promptCount: client.prompts?.length ?? 0,
        uptime: client.uptime, containerBacked: !!config?.containerConfig, needsAuth: true,
        scope: this._serverScopes.get(name), sourcePath: this._serverSources.get(name),
        ...this.sourceAgentsField(name),
      };
    }
    return {
      name,
      state: client.state,
      serverInfo: client.serverInfo,
      toolCount: client.tools.length,
      resourceCount: client.resources.length,
      promptCount: client.prompts?.length ?? 0,
      uptime: client.uptime,
      containerBacked: !!config?.containerConfig,
      isAuthenticated: this.deriveIsAuthenticated(config, client),
      // Surface the per-server needs-auth affordance when a connected client's
      // token died mid-session (parity with the pending-server needs-auth flag).
      needsAuth: client.state === 'needs-auth' || undefined,
      ...this.reconnectProgress(name),
      scope: this._serverScopes.get(name),
      sourcePath: this._serverSources.get(name),
      ...this.sourceAgentsField(name),
      error: this.serverErrors.get(name),
    };
  }

  removeServer(name: string): void {
    // Stop any tool polling for the server being forgotten.
    this._toolPoller.unregister(name);
    // Cancel any in-flight OAuth flow for this server before teardown.
    this._oauthAbort.get(name)?.abort();
    this._oauthAbort.delete(name);
    const timer = this.reconnectTimers.get(name);
    if (timer) {
      clearTimeout(timer);
      this.reconnectTimers.delete(name);
    }

    const client = this.clients.get(name);
    if (client) {
      this._intentionalDisconnect.add(name);
      // Forgetting a server is a sign-out: revoke its OAuth tokens server-side
      // (RFC 7009, best-effort) and drop the persisted credential before teardown,
      // so a discarded refresh token cannot keep minting access. Fire-and-forget —
      // a failed/slow revoke must not block removal. (A transient stopServer does
      // NOT revoke; only an explicit remove/reject does.)
      void client.revokeAuth().catch(() => {});
      client.disconnect();
      this.clients.delete(name);
    }
    // Credential lifecycle hygiene (Claude-parity `cleanupSecureStorage`,
    // cli/handlers/mcp.tsx:L79-84): forgetting a server purges its PERSISTED OAuth
    // token file so no credential is orphaned on disk — even when the server has no
    // live client (a stopped/pending/never-connected remote), where the server-side
    // `revokeAuth()` above cannot run. Read the config BEFORE it is dropped below.
    const removedConfig = this.configs.get(name);
    if (removedConfig?.oauth && removedConfig.url
      && (removedConfig.transport === 'http' || removedConfig.transport === 'sse')) {
      try { fs.rmSync(this.mcpTokenFilePath(name, removedConfig), { force: true }); } catch { /* best-effort purge */ }
    }
    this.pendingServers.delete(name);
    this.serverErrors.delete(name);
    this.configs.delete(name);
    this._serverScopes.delete(name);
    this._serverSources.delete(name);
    this._serverSourceAgents.delete(name);
    this._serverConfigHashes.delete(name);
    this.invalidateToolCache(name);
  }

  async connectServer(name: string): Promise<void> {
    const client = this.clients.get(name);
    if (!client) throw new Error(`MCP server '${name}' not found`);

    const config = this.configs.get(name);
    const maxRetries = 3;
    let lastErr: unknown;

    // Resolve the effective transport (explicit, else inferred from the URL
    // scheme) once — it drives both the connect dispatch and the needs-auth cache
    // bookkeeping (which only applies to REMOTE servers), and must be visible in
    // the catch handler, so it is hoisted out of the per-attempt try block.
    const transport = this.effectiveTransport(config);

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        // For http/sse servers, refresh resolved auth headers right before the
        // handshake when EITHER a `headersHelper` (rotating/short-lived tokens) OR
        // a `${secret:keychain:...}` header ref (secret-at-rest in the OS keychain)
        // needs an async resolve. Both are gated by the SAME workspace-trust
        // boundary: an untrusted project `.mcp.json` must neither run an arbitrary
        // helper script NOR siphon the user's OS keychain into a header it would
        // then POST to its own server URL. When untrusted, skip, record a
        // recoverable warning diagnostic, and fall back to the static (env-only)
        // headers already applied at client creation so the connect still proceeds.
        const needsKeychain = headersHaveKeychainSecretRef(config?.headers);
        if ((config?.headersHelper || needsKeychain) && (config?.transport === 'http' || config?.transport === 'sse')) {
          if (this.isHeadersHelperTrusted(name)) {
            client.setHeaders(await resolveMcpServerHeaders(name, config, undefined, {
              onDebug: (msg) => this._logMcpDebug(name, msg, this.mcpLogOpts()),
              onError: (err) => this._logMcpError(name, err, this.mcpLogOpts()),
            }, this._secretStore));
          } else {
            const what = config?.headersHelper ? 'config-supplied helper script' : 'OS-keychain secret references';
            const warning = `${config?.headersHelper ? 'headersHelper' : 'keychain secret refs'} not resolved: the project MCP server "${name}" must be approved before its ${what} can run (workspace-trust boundary)`;
            coreLog('mcp', warning);
            const existing = this._configWarnings.get(name) ?? [];
            if (!existing.includes(warning)) {
              this._configWarnings.set(name, [...existing, warning]);
              this.emit('config-warning', name, [warning]);
            }
          }
        }
        // Dispatch over the resolved transport so a `{ url: "wss://…" }` /
        // `{ url: "https://…" }` entry with no explicit `transport` connects over
        // the right remote transport instead of falling through to stdio.
        if (transport === 'http' && config?.url) {
          await client.connectHTTP(config.url);
        } else if (transport === 'sse' && config?.url) {
          await client.connectSSE(config.url);
        } else if ((transport === 'ws' || transport === 'wss') && config?.url) {
          await client.connectWebSocket(config.url);
        } else {
          await client.connect();
        }

        this._lastUsedAt.set(name, Date.now());

        const tools = await client.listTools();
        this.ingestServerTools(name, tools);
        // A successful connect proves the server no longer needs auth — drop any
        // stale needs-auth cache entry so it is not needlessly skipped next time.
        if (isRemoteMcpTransport(transport)) this.clearNeedsAuthCache(name);
        return;
      } catch (err) {
        lastErr = err;
        if (attempt < maxRetries && isTransientConnectError(err)) {
          const delay = 1000 * Math.pow(2, attempt);
          coreLog('mcp', `transient connect error for ${name} (attempt ${attempt + 1}/${maxRetries + 1}): ${err instanceof Error ? err.message : err}, retrying in ${delay}ms`);
          await new Promise(r => setTimeout(r, delay));
          continue;
        }
        // A terminal auth-required failure on a remote server is a guaranteed-401
        // for the TTL window — cache it so the next connect pass skips the probe
        // (Claude-parity setMcpAuthCacheEntry).
        if (isRemoteMcpTransport(transport) && isAuthRequiredError(err)) this.recordNeedsAuth(name);
        const finalErr = isHandshakeFailure(err) ? handshakeError(name, err) : err;
        // A connect attempt that has exhausted its retries is a per-server error
        // worth persisting to the server's dedicated log (Claude-parity logMCPError).
        this._logMcpError(name, finalErr, this.mcpLogOpts());
        throw finalErr;
      }
    }
    throw lastErr;
  }

  /**
   * Connect every registered client with BOUNDED concurrency (Claude-parity:
   * MCP_SERVER_CONNECTION_BATCH_SIZE / MCP_REMOTE_SERVER_CONNECTION_BATCH_SIZE).
   * Servers are split into local (stdio/container, process-spawning) and remote
   * (http/sse, network) groups, each run through its own concurrency pool, so a
   * config with many stdio servers never stampedes N child processes at once
   * (ENOMEM/EAGAIN under load) and remote servers get a higher ceiling. Previously
   * an unbounded `Promise.all` fired all connects simultaneously.
   */
  async connectAll(): Promise<Map<string, Error | null>> {
    const results = new Map<string, Error | null>();
    const local: string[] = [];
    const remote: string[] = [];
    for (const name of this.clients.keys()) {
      const t = this.effectiveTransport(this.configs.get(name));
      if (!isRemoteMcpTransport(t)) {
        local.push(name);
        continue;
      }
      // Needs-auth skip (Claude-parity isMcpAuthCached / hasMcpDiscoveryButNoToken):
      // a remote server that returned 401 within the TTL is a guaranteed-401, so
      // short-circuit straight to a needs-auth result instead of burning a network
      // round-trip + OAuth discovery on it. The user re-authenticating (/mcp →
      // reconnect) clears the entry and re-probes.
      if (this.isNeedsAuthCached(name)) {
        this._needsAuthSkipped.add(name);
        this.serverErrors.delete(name);
        this.emit('server-state-changed', name, 'needs-auth');
        coreLog('mcp', `skipping MCP server "${name}" connect: cached needs-auth (re-authenticate via /mcp to retry)`);
        results.set(name, null);
        continue;
      }
      remote.push(name);
    }
    const connectOne = async (name: string) => {
      try { await this.connectServer(name); results.set(name, null); }
      catch (err) { results.set(name, err instanceof Error ? err : new Error(String(err))); }
    };
    await Promise.all([
      runBoundedPool(local, getMcpConnectionBatchSize(), connectOne),
      runBoundedPool(remote, getMcpRemoteConnectionBatchSize(), connectOne),
    ]);
    return results;
  }

  async callTool(qualifiedName: string, args: Record<string, unknown> = {}, options?: { signal?: AbortSignal; timeoutMs?: number }): Promise<unknown> {
    // Tool calls get the long tool-call timeout at the TRANSPORT layer (not just a
    // higher-level race), so a slow tool is not killed by the short request/connect
    // timeout (H25/H30). Callers may override.
    const callOptions = { signal: options?.signal, timeoutMs: options?.timeoutMs ?? getMcpToolTimeout() };
    let serverName: string;
    let bareToolName: string;
    const colonIdx = qualifiedName.indexOf(':');
    if (colonIdx === -1) {
      const entry = this.findToolByName(qualifiedName);
      if (!entry) throw new Error(`MCP tool '${qualifiedName}' not found`);
      serverName = entry.serverName;
      bareToolName = qualifiedName;
    } else {
      serverName = qualifiedName.slice(0, colonIdx);
      bareToolName = qualifiedName.slice(colonIdx + 1);
    }
    this._lastUsedAt.set(serverName, Date.now());
    const client = this.clients.get(serverName);
    if (!client) throw new Error(`MCP server '${serverName}' not found`);

    try {
      return await client.callTool(bareToolName, args, callOptions);
    } catch (err) {
      // Transparent single retry after MCP session expiry (Claude-parity
      // MAX_SESSION_RETRIES=1): a routine Streamable-HTTP session rotation closes
      // the transport; clear the stale cache, reconnect, and re-run against a
      // fresh session so the rotation is invisible to the model instead of failing.
      if (err instanceof McpSessionExpiredError) {
        coreLog('mcp', `Session expired calling ${qualifiedName}, reconnecting and retrying once`);
        this.invalidateToolCache(serverName);
        this.emit('session-expired', serverName);
        await client.reconnect();
        await this.refreshToolsForServer(serverName);
        return await client.callTool(bareToolName, args, callOptions);
      }
      throw err;
    }
  }

  async listResources(serverFilter?: string): Promise<Array<McpResource & { server: string }>> {
    // A `server` filter that matches no connected client is a user/model error
    // worth reporting (with the valid names) rather than silently returning [].
    if (serverFilter && !this.clients.has(serverFilter)) {
      throw new Error(`MCP server "${serverFilter}" not found. Available servers: ${this.availableServerNames()}`);
    }
    const results: Array<McpResource & { server: string }> = [];
    for (const [name, client] of this.clients) {
      if (serverFilter && name !== serverFilter) continue;
      try {
        const resources = await this.listResourcesWithReconnect(name, client);
        for (const r of resources) {
          results.push({ ...r, server: name });
        }
      } catch (err) {
        // Isolate per-server failures (one server's error must not sink the whole
        // aggregate) AND log EVERY failure — not only session-expiry — so a server
        // that consistently errors on resources/list is observable instead of being
        // silently swallowed (Claude-parity ListMcpResourcesTool). The URL goes
        // through getLoggingSafeMcpBaseUrl so a token-bearing query string is never
        // written to the log.
        const safeUrl = getLoggingSafeMcpBaseUrl(this.configs.get(name));
        const msg = err instanceof Error ? err.message : String(err);
        coreLog('mcp', `listResources failed for ${name}${safeUrl ? ` (${safeUrl})` : ''}: ${msg}`);
      }
    }
    return results;
  }

  /**
   * List a single server's resources, reconnecting-and-retrying ONCE in-call when
   * the session has expired / the transport was closed (Claude-parity
   * `ensureConnectedClient` before fetch): a server whose streamable-HTTP session
   * rotated mid-listing still returns its resources within the same call instead of
   * being dropped and only recovered on the next async health tick. A non-session
   * error (or a failure of the retry itself) propagates to the per-server catch.
   */
  private async listResourcesWithReconnect(name: string, client: McpClient): Promise<McpResource[]> {
    // Claude-parity `ensureConnectedClient(client)` before the fetch: a server that
    // was dropped by `onclose` (its client is no longer `ready`) is transparently
    // re-opened FIRST so its resources still appear in THIS same list call, instead
    // of being skipped until a later async health tick. A no-op when already ready;
    // a reconnect failure propagates to the per-server catch in {@link listResources}.
    if (client.state !== 'ready') {
      coreLog('mcp', `listResources: "${name}" not ready (${client.state}); reconnecting before fetch`);
      await client.reconnect();
      await this.refreshToolsForServer(name);
    }
    try {
      return await client.listResources();
    } catch (err) {
      if (err instanceof McpSessionExpiredError) {
        coreLog('mcp', `Session expired for ${name} during listResources, reconnecting and retrying once`);
        this.invalidateToolCache(name);
        this.emit('session-expired', name);
        await client.reconnect();
        await this.refreshToolsForServer(name);
        return await client.listResources();
      }
      throw err;
    }
  }

  /** Comma-separated list of currently-connected server names (for error hints). */
  private availableServerNames(): string {
    return [...this.clients.keys()].sort((a, b) => a.localeCompare(b)).join(', ');
  }

  async readResource(serverName: string, uri: string, options?: { signal?: AbortSignal }): Promise<unknown> {
    const client = this.clients.get(serverName);
    if (!client) {
      // Pre-flight validation with distinct, self-correcting errors (Claude-parity
      // ReadMcpResourceTool): a name that was never registered is "not found" and
      // enumerates the valid servers so the model can self-correct; a name that is
      // known (configured/pending) but has no live client is "not connected".
      if (this.configs.has(serverName) || this.pendingServers.has(serverName)) {
        throw new Error(`MCP server "${serverName}" is not connected`);
      }
      throw new Error(`MCP server "${serverName}" not found. Available servers: ${this.availableServerNames()}`);
    }
    // Upfront resources-capability check: when the server declared a capability set
    // that does NOT include `resources`, fail fast with an actionable error instead
    // of issuing a guaranteed-to-be-rejected `resources/read`. A server that declared
    // no capabilities at all is left to answer for itself (we only hard-block on
    // positive evidence of non-support).
    const caps = client.serverCapabilities;
    if (Object.keys(caps).length > 0 && !client.hasCapability('resources')) {
      throw new Error(`MCP server "${serverName}" does not support resources`);
    }
    try {
      return await client.readResource(uri, options?.signal);
    } catch (err) {
      if (err instanceof McpSessionExpiredError) {
        coreLog('mcp', `Session expired for ${serverName} during readResource`);
        this.emit('session-expired', serverName);
      }
      throw err;
    }
  }

  async listPrompts(serverFilter?: string): Promise<Array<McpPrompt & { server: string }>> {
    const results: Array<McpPrompt & { server: string }> = [];
    for (const [name, client] of this.clients) {
      if (serverFilter && name !== serverFilter) continue;
      if (client.state !== 'ready') continue;
      try {
        const prompts = await client.listPrompts();
        for (const p of prompts) {
          // Strip invisible/control Unicode from server-provided prompt metadata
          // (names, descriptions, argument fields — keys AND values) before it
          // enters the model's context / slash-command table. A malicious MCP
          // server can otherwise hide instructions in prompt names/descriptions via
          // zero-width/Tag/bidi characters (ASCII-smuggling prompt injection,
          // HackerOne #3086545). Mirrors the sanitization already applied to tool
          // names/descriptions in createMcpTool.
          results.push({ ...recursivelySanitizeUnicode(p), server: name });
        }
      } catch {}
    }
    return results;
  }

  async getPrompt(serverName: string, promptName: string, args: Record<string, string> = {}): Promise<{ description?: string; messages: McpPromptMessage[] }> {
    const client = this.clients.get(serverName);
    if (!client) throw new Error(`MCP server "${serverName}" not connected. Available servers: ${this.availableServerNames()}`);
    const result = await client.getPrompt(promptName, args);
    // Sanitize the server-authored prompt metadata/messages before they reach the model.
    return recursivelySanitizeUnicode(result);
  }

  getAllTools(): Array<{ serverName: string; schema: McpToolSchema }> {
    return Array.from(this._allTools.values());
  }

  /**
   * Server-authored usage instructions (InitializeResult.instructions) for every
   * connected server that supplied them, capped per server at the MCP description
   * length (overlong instructions are truncated with `… [truncated]`). The engine
   * renders these as `## <server>` blocks into the model's context so a server can
   * teach the model how to use its tools (H32/H62/H63/H64).
   */
  getServerInstructions(): Array<{ name: string; instructions: string }> {
    const cap = getMcpMaxDescriptionLength();
    const out: Array<{ name: string; instructions: string }> = [];
    for (const [name, client] of this.clients) {
      if (client.state !== 'ready') continue;
      const raw = client.instructions;
      if (!raw) continue;
      const instructions = raw.length > cap ? raw.slice(0, cap) + '… [truncated]' : raw;
      out.push({ name, instructions });
    }
    return out;
  }

  getToolConflicts(): Array<{ toolName: string; servers: string[] }> {
    const byName = new Map<string, string[]>();
    for (const entry of this._allTools.values()) {
      const list = byName.get(entry.schema.name) ?? [];
      list.push(entry.serverName);
      byName.set(entry.schema.name, list);
    }
    const conflicts: Array<{ toolName: string; servers: string[] }> = [];
    for (const [toolName, servers] of byName) {
      if (servers.length > 1) conflicts.push({ toolName, servers });
    }
    return conflicts;
  }

  getServerStatus(name: string): McpServerStatus | null {
    const client = this.clients.get(name);
    if (!client) return null;
    const config = this.configs.get(name);
    return {
      name,
      state: client.state,
      serverInfo: client.serverInfo,
      toolCount: client.tools.length,
      resourceCount: client.resources.length,
      promptCount: client.prompts?.length ?? 0,
      uptime: client.uptime,
      isAuthenticated: this.deriveIsAuthenticated(config, client),
      channelPermissionRelay: client.supportsChannelPermissionRelay(),
      ...this.reconnectProgress(name),
      scope: this._serverScopes.get(name),
      sourcePath: this._serverSources.get(name),
    };
  }

  listServers(): McpServerStatus[] {
    return Array.from(this.clients.keys())
      .map(name => this.getServerStatus(name)!)
      .sort((a, b) => a.name.localeCompare(b.name));
  }

  /** The stored config for a server (transport/url/oauth), or undefined if none. */
  getServerConfig(name: string): McpServerConfig | undefined {
    return this.configs.get(name);
  }

  /**
   * The effective transport for a config: its explicit `transport` when set, else
   * one inferred from the `url` scheme ({@link inferMcpTransportFromUrl}). Single
   * source of truth so the connect dispatch, the local-vs-remote connection pool,
   * and the close-handler reconnect decision all agree on how a transport-less
   * remote URL is treated (never as stdio).
   */
  private effectiveTransport(config: McpServerConfig | undefined): McpServerConfig['transport'] {
    // Canonicalize an IDE discriminant (sse-ide/ws-ide) to its base transport so
    // the connect dispatch + remote-pool bucketing route it over the real wire
    // protocol; else infer from the URL scheme.
    return canonicalMcpTransport(config?.transport) ?? inferMcpTransportFromUrl(config?.url);
  }

  /**
   * Derived IDE editor-connection status for the reserved `ide` MCP server
   * (Claude-parity `useIdeConnectionStatus`): `{ status: null, ideName: null }`
   * when no editor is wired, else the tri-state ({@link ideStatusFromServerState})
   * plus the human-readable editor name ({@link getMcpIdeName}). Single source of
   * truth the `/mcp` view renders as the "IDE: <name> (connected)" badge, so the
   * CLI/TUI never re-derive editor state from raw client fields.
   */
  getIdeConnectionStatus(): { status: IdeStatus; ideName: string | null } {
    if (!this.clients.has(IDE_MCP_SERVER_NAME) && !this.pendingServers.has(IDE_MCP_SERVER_NAME)) {
      return { status: null, ideName: null };
    }
    return {
      status: ideStatusFromServerState(this.getServerState(IDE_MCP_SERVER_NAME)),
      ideName: getMcpIdeName(this.configs.get(IDE_MCP_SERVER_NAME)),
    };
  }

  /**
   * Auto-register the reserved `ide` MCP server for a code-editor-launched endpoint
   * when the Claude-parity auto-connect waterfall ({@link shouldAutoConnectIde}) is
   * satisfied AND the editor's SSE endpoint is resolvable from the environment
   * ({@link resolveIdeMcpEndpoint}, {@link MCP_IDE_SSE_PORT_ENV}). The server is
   * registered as the built-in `dynamic` scope over the `sse-ide` transport so the
   * normal connect pass brings it up and {@link getIdeConnectionStatus} reports the
   * editor badge. No-op (returns `false`) when the gate is off, no endpoint
   * resolves, or an `ide` server is already registered — so it is safe to call
   * unconditionally at startup and only fires when an editor actually exported its
   * port. Returns `true` when it registered the IDE server.
   */
  autoConnectIde(opts: IdeAutoConnectOptions = {}): boolean {
    if (this.clients.has(IDE_MCP_SERVER_NAME) || this.pendingServers.has(IDE_MCP_SERVER_NAME)) return false;
    if (!shouldAutoConnectIde(opts)) return false;
    const env = opts.env ?? process.env;
    const endpoint = resolveIdeMcpEndpoint(env);
    if (!endpoint) return false;
    const ideName = env.TERM_PROGRAM || 'IDE';
    this.addServer(IDE_MCP_SERVER_NAME, { command: '', transport: 'sse-ide', url: endpoint.url, ideName });
    coreLog('mcp', `IDE auto-connect: registered editor MCP endpoint ${endpoint.url} (${ideName})`);
    return true;
  }

  /**
   * Resolve the IDE the reserved `ide` server is currently wired to by matching
   * its persisted config against a freshly-detected editor list (Claude-parity
   * `findCurrentIDE`, ide.tsx:L208-219). Reads the live `ide` server config,
   * guards its transport to an IDE discriminant (`sse-ide` / `ws-ide`), then
   * linear-scans `availableIDEs` for the entry whose `url` is identical — the
   * url-identity reconciliation that drives the `/mcp` IDE badge's "already
   * connected" preselection and the disconnect path. Returns `null` when no `ide`
   * server is wired, it is not an IDE transport, or no detected editor matches.
   */
  findCurrentIde(availableIDEs: DetectedIdeInfo[]): DetectedIdeInfo | null {
    const config = this.configs.get(IDE_MCP_SERVER_NAME);
    if (!config || (config.transport !== 'sse-ide' && config.transport !== 'ws-ide')) {
      return null;
    }
    for (const ide of availableIDEs) {
      if (ide.url === config.url) return ide;
    }
    return null;
  }

  /**
   * Connect (or re-point) the reserved `ide` MCP server to a detected editor and
   * resolve once the handshake reaches a terminal state, the core analogue of
   * Claude Code's IDECommandFlow connect state machine (ide.tsx:L515-604). Any
   * existing `ide` server is torn down first (re-point semantics), then the
   * editor is registered over the transport its URL implies
   * ({@link ideTransportForUrl}) with its `ideName` (and a bearer `authToken`
   * header when the endpoint requires one) and connected. The connect races a
   * {@link IDE_CONNECTION_TIMEOUT_MS} fallback so a server that never answers
   * yields a `timeout` result instead of hanging — the outer ceiling Claude's
   * `setTimeout(onDone, …)` provides.
   *
   * (Claude's `isFirstCheckRef` stale-snapshot skip, ide.tsx:L529/536-539/594, is
   * a React-effect re-render artifact — it ignores the first post-dispatch render
   * because the store still holds the pre-config client. Core awaits the real
   * handshake promise directly, so there is no stale snapshot to skip.)
   */
  async connectIde(ide: DetectedIdeInfo, opts: IdeConnectOptions = {}): Promise<IdeConnectResult> {
    // Re-point semantics: drop any editor already wired to the `ide` slot so the
    // new selection replaces it (Claude deletes `dynamicMcpConfig.ide` first).
    if (this.clients.has(IDE_MCP_SERVER_NAME) || this.pendingServers.has(IDE_MCP_SERVER_NAME) || this.configs.has(IDE_MCP_SERVER_NAME)) {
      this.removeServer(IDE_MCP_SERVER_NAME);
    }
    const config: McpServerConfig = {
      command: '',
      transport: ideTransportForUrl(ide.url),
      url: ide.url,
      ideName: ide.name,
      ...(ide.authToken ? { headers: { Authorization: `Bearer ${ide.authToken}` } } : {}),
    };
    this.addServer(IDE_MCP_SERVER_NAME, config);

    const timeoutMs = opts.timeoutMs ?? IDE_CONNECTION_TIMEOUT_MS;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<'timeout'>((resolve) => {
      timer = setTimeout(() => resolve('timeout'), timeoutMs);
    });
    const outcome = await Promise.race([
      this.connectServer(IDE_MCP_SERVER_NAME).then(() => 'connected' as const, () => 'failed' as const),
      timeout,
    ]);
    if (timer) clearTimeout(timer);

    if (outcome === 'connected') return { outcome, message: `Connected to ${ide.name}.` };
    if (outcome === 'failed') return { outcome, message: `Failed to connect to ${ide.name}.` };
    return { outcome: 'timeout', message: `Connection to ${ide.name} timed out.` };
  }

  /**
   * Disconnect the reserved `ide` MCP server (Claude-parity IDECommandFlow
   * disconnect leg, ide.tsx:L562-586): tears down the live transport and forgets
   * the server WITHOUT triggering the close-driven auto-reconnect (Claude nulls
   * the client `onclose`; {@link removeServer} flags an intentional disconnect to
   * the same end). Returns `disconnected:false` with "No IDE selected." when no
   * editor was wired, else `disconnected:true` with "Disconnected from <name>."
   */
  disconnectIde(): { disconnected: boolean; message: string } {
    if (!this.clients.has(IDE_MCP_SERVER_NAME) && !this.pendingServers.has(IDE_MCP_SERVER_NAME) && !this.configs.has(IDE_MCP_SERVER_NAME)) {
      return { disconnected: false, message: 'No IDE selected.' };
    }
    const ideName = getMcpIdeName(this.configs.get(IDE_MCP_SERVER_NAME)) ?? 'IDE';
    this.removeServer(IDE_MCP_SERVER_NAME);
    return { disconnected: true, message: `Disconnected from ${ideName}.` };
  }

  /**
   * The on-disk path of a server's persisted OAuth token file (keyed on the server
   * NAME *and* a hash of its connection config, matching Claude's `getServerKey`).
   * Single source of truth so {@link addServer} (which writes/reads it) and
   * {@link removeServer} (which purges it) cannot drift on how the credential file
   * is located.
   */
  private mcpTokenFilePath(name: string, config: McpServerConfig): string {
    const credentialKey = mcpCredentialKey(name, {
      type: config.transport,
      url: config.url,
      headers: config.headers,
    });
    return path.join(resolveConfigDir(), 'mcp-tokens', mcpTokenFileName(credentialKey));
  }

  /** On-disk path of the persisted needs-auth skip cache (Claude-parity
   *  `mcp-needs-auth-cache.json`). Lives alongside the MCP token files. */
  private needsAuthCachePath(): string {
    return path.join(resolveConfigDir(), 'mcp-needs-auth-cache.json');
  }

  /** Lazily read the needs-auth cache from disk (once), pruning expired entries. */
  private loadNeedsAuthCache(): NeedsAuthCache {
    if (this._needsAuthCache === undefined) {
      const raw = readNeedsAuthCache(this.needsAuthCachePath());
      const pruned = pruneNeedsAuthCache(raw, Date.now(), this._needsAuthCacheTtlMs);
      this._needsAuthCache = pruned;
      // Persist a prune so stale entries do not accumulate across runs.
      if (pruned !== raw) writeNeedsAuthCache(this.needsAuthCachePath(), pruned);
    }
    return this._needsAuthCache;
  }

  /**
   * True when `name` returned 401 (or had discovery-but-no-token) within the TTL,
   * so connecting it now is a guaranteed 401 and should be skipped. Public so a
   * `/mcp` view / diagnostic can explain why a server was short-circuited.
   */
  isNeedsAuthCached(name: string): boolean {
    return isNeedsAuthCached(this.loadNeedsAuthCache(), name, Date.now(), this._needsAuthCacheTtlMs);
  }

  /** Record that `name` needs auth (a remote connect returned 401), persisting it. */
  private recordNeedsAuth(name: string): void {
    const next = setNeedsAuthEntry(this.loadNeedsAuthCache(), name, Date.now());
    this._needsAuthCache = next;
    writeNeedsAuthCache(this.needsAuthCachePath(), next);
  }

  /**
   * Clear a server's needs-auth cache entry (on a successful connect or an
   * explicit re-auth). With no `name`, clears the whole cache. Only rewrites the
   * file when something actually changed. Also drops any in-session skip marker so
   * the server stops reporting `needs-auth` once it is re-probed/authenticated.
   */
  clearNeedsAuthCache(name?: string): void {
    // Drop the in-session skip marker(s) so the server(s) stop reporting needs-auth.
    if (name === undefined) this._needsAuthSkipped.clear();
    else this._needsAuthSkipped.delete(name);
    const current = this.loadNeedsAuthCache();
    const next = name === undefined ? {} : clearNeedsAuthEntry(current, name);
    // Only rewrite the file when the persisted cache actually changed.
    const changed = name === undefined ? Object.keys(current).length > 0 : next !== current;
    if (changed) {
      this._needsAuthCache = next;
      writeNeedsAuthCache(this.needsAuthCachePath(), next);
    }
  }

  /** The config-source scope a server was loaded from, or undefined if unknown. */
  getServerScope(name: string): McpServerScope | undefined {
    return this._serverScopes.get(name);
  }

  /** Record the scope + source config path for a server (load-time metadata). */
  private recordServerScope(name: string, scope: McpServerScope, sourcePath?: string): void {
    this._serverScopes.set(name, scope);
    if (sourcePath !== undefined) this._serverSources.set(name, sourcePath);
    else this._serverSources.delete(name);
  }

  /**
   * Record that the skill/stance `agent` declared MCP server `name` — the reverse
   * provenance mapping surfaced on {@link McpServerStatus.sourceAgents} (Claude-parity
   * `AgentMcpServerInfo.sourceAgents`). Idempotent: registering the same agent twice is
   * a no-op, and a server declared by several stances accumulates every declarer. A
   * blank/whitespace agent name is ignored so a malformed stance can't inject an empty
   * provenance entry. Called from the engine when a stance registers its `mcp-servers`.
   */
  addServerSourceAgent(name: string, agent: string): void {
    const trimmed = agent.trim();
    if (!trimmed) return;
    const existing = this._serverSourceAgents.get(name);
    if (existing) existing.add(trimmed);
    else this._serverSourceAgents.set(name, new Set([trimmed]));
  }

  /**
   * The de-duplicated, alphabetically-sorted stance/agent names that declared server
   * `name`, or `undefined` when no stance did (so a status never carries an empty
   * `sourceAgents` array). See {@link addServerSourceAgent}.
   */
  getServerSourceAgents(name: string): string[] | undefined {
    const set = this._serverSourceAgents.get(name);
    if (!set || set.size === 0) return undefined;
    return [...set].sort((a, b) => a.localeCompare(b));
  }

  /**
   * Load enterprise/managed MCP servers (admin-provisioned `managedMcpServers`).
   * Every entry is tagged with the `enterprise` scope and loaded through the same
   * validation/denylist/expansion path as a config file, so a managed server is
   * grouped under "Enterprise MCPs" and is diagnosable like any other. Each entry
   * goes through per-server isolation: one malformed entry does not abort the rest.
   */
  loadManagedServers(servers: Record<string, unknown>, opts?: { sourcePath?: string }): void {
    this.loadServerEntries(servers, { scope: 'enterprise', sourcePath: opts?.sourcePath });
  }

  /**
   * Group live servers by their config-source scope for a Claude-parity listing
   * (MCPListPanel): groups are ordered by {@link MCP_SCOPE_ORDER}, servers within a
   * group are alphabetical (inherited from {@link listAllServers}), and each group
   * carries its heading `label` and the `sourcePath` of its first server so a UI can
   * show the user which file to edit. Empty scopes are omitted.
   */
  listServersByScope(): Array<{ scope: McpServerScope; label: string; sourcePath?: string; servers: McpServerStatus[] }> {
    const all = this.listAllServers();
    const groups: Array<{ scope: McpServerScope; label: string; sourcePath?: string; servers: McpServerStatus[] }> = [];
    for (const scope of MCP_SCOPE_ORDER) {
      const servers = all.filter((s) => (s.scope ?? 'dynamic') === scope);
      if (servers.length === 0) continue;
      groups.push({
        scope,
        label: mcpScopeLabel(scope),
        sourcePath: servers.find((s) => s.sourcePath)?.sourcePath,
        servers,
      });
    }
    return groups;
  }

  startHealthChecks(intervalMs: number = 30000): void {
    if (this.healthCheckInterval) return;
    // Seed the tick clock so the FIRST real tick (~intervalMs later) is not
    // mistaken for a suspension gap.
    this._lastHealthTick = Date.now();
    this.healthCheckInterval = setInterval(() => {
      void this._runHealthCheckTick(intervalMs, Date.now());
    }, intervalMs);
    this.healthCheckInterval.unref();
  }

  /**
   * One health-check pass. Pings every `ready` client and schedules a reconnect on
   * failure, with one addition over a plain ping loop: **process-suspension
   * detection**. `setInterval` does not queue missed ticks — if the host process is
   * paused (laptop sleep, VM suspend, container freeze) the pending ticks coalesce
   * into a single late fire. When a tick arrives far later than its interval
   * ({@link MCP_SUSPENSION_TICK_FACTOR}×), we conclude the process was suspended and
   * force an immediate reconnect of every ready REMOTE (http/sse) client WITHOUT
   * waiting for a ping round-trip: after a sleep the socket's NAT mapping is usually
   * dead, yet `ping()` can falsely succeed because the bytes merely enter the kernel
   * send buffer — so trusting the ping would leave a silently-dead connection in
   * place. Local (stdio/container) servers are suspended and resumed together with
   * us, so they are pinged normally. Exposed (underscore-prefixed) and
   * clock-injectable so the suspension-gap path is unit-testable without real timers.
   */
  async _runHealthCheckTick(intervalMs: number, now: number): Promise<void> {
    const last = this._lastHealthTick;
    this._lastHealthTick = now;
    const suspended = last !== null && now - last > intervalMs * MCP_SUSPENSION_TICK_FACTOR;
    const forcedReconnects: string[] = [];
    for (const [name, client] of this.clients) {
      if (client.state !== 'ready') continue;
      if (suspended) {
        const transport = this.configs.get(name)?.transport;
        if (isRemoteMcpTransport(transport)) {
          forcedReconnects.push(name);
          this.scheduleReconnect(name);
          continue;
        }
      }
      try {
        await client.ping();
      } catch {
        coreLog('mcp', `Health check failed for ${name}, attempting reconnect`);
        this.scheduleReconnect(name);
      }
    }
    if (suspended) {
      const gap = now - (last as number);
      coreLog('mcp', `Health-check tick gap ${Math.round(gap / 1000)}s exceeded ${Math.round(intervalMs / 1000)}s interval; assuming process suspension, forced reconnect of ${forcedReconnects.length} remote MCP server(s)`);
      this.emit('suspension-detected', gap, forcedReconnects);
    }
  }

  stopHealthChecks(): void {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = null;
    }
  }

  getToolAnnotations(qualifiedName: string): { readOnlyHint?: boolean; destructiveHint?: boolean; openWorldHint?: boolean } {
    const entry = this._allTools.get(qualifiedName) ?? this.findToolByName(qualifiedName);
    if (entry?.schema.annotations) {
      const a = entry.schema.annotations;
      return {
        readOnlyHint: a.readOnlyHint ?? undefined,
        destructiveHint: a.destructiveHint ?? undefined,
        openWorldHint: a.openWorldHint ?? undefined,
      };
    }

    const toolName = qualifiedName.includes(':') ? qualifiedName.slice(qualifiedName.indexOf(':') + 1) : qualifiedName;
    const readOnlyVerbs = ['get', 'list', 'search', 'find', 'read', 'view', 'show', 'describe', 'fetch', 'query'];
    const destructiveVerbs = ['delete', 'remove', 'drop', 'destroy', 'purge', 'clear', 'truncate'];
    const openWorldVerbs = ['web', 'fetch', 'http', 'curl', 'download', 'upload', 'send', 'post'];

    // Normalize camelCase + kebab-case to snake_case before classifying so a verb
    // that is NOT the leading token is still found (Claude-parity): e.g.
    // `searchJiraIssues` → `search_jira_issues`, `web_search_exa`, `brave-web-search`.
    // Fall back to the leading-prefix check too, so single-token lowercase names
    // like `getuser` keep classifying as before (strict superset of old behavior).
    const lower = toolName.toLowerCase();
    const tokens = new Set(normalizeMcpToolName(toolName).split('_').filter(Boolean));
    const classify = (verbs: string[]) => verbs.some(v => tokens.has(v) || lower.startsWith(v));
    // Union the verb heuristic with the curated per-tool read/search allowlist
    // (Claude-parity SEARCH_TOOLS/READ_TOOLS, classifyForCollapse.ts) so a
    // low-signal read/search op whose verb is NOT a token — `whoami`, `esql`,
    // `explain`, `aggregate`, `count`, `recommend`, `atlassian_user_info`, … —
    // is still classified read-only instead of defaulting to false. This reuses
    // the ONE canonical classifier (no second allowlist) and is a strict
    // superset of the prefix heuristic: a tool the allowlist cannot prove is a
    // read/search op still falls back to the verb check.
    const collapse = classifyMcpToolForCollapse('', toolName);
    const isReadOnly = classify(readOnlyVerbs) || collapse.isRead || collapse.isSearch;
    const isDestructive = classify(destructiveVerbs);
    const isOpenWorld = classify(openWorldVerbs);

    return {
      readOnlyHint: isReadOnly || undefined,
      destructiveHint: isDestructive || undefined,
      openWorldHint: isOpenWorld || undefined,
    };
  }

  truncateDescription(description: string, maxLength: number = getMcpMaxDescriptionLength()): string {
    if (description.length <= maxLength) return description;
    return description.slice(0, maxLength - 3) + '...';
  }

  startIdleChecker(intervalMs = 60_000): void {
    this.stopIdleChecker();
    this._idleCheckTimer = setInterval(() => {
      this._evictIdleServers();
    }, intervalMs);
    this._idleCheckTimer.unref();
  }

  stopIdleChecker(): void {
    if (this._idleCheckTimer !== null) {
      clearInterval(this._idleCheckTimer);
      this._idleCheckTimer = null;
    }
  }

  _evictIdleServers(): string[] {
    const now = Date.now();
    const evicted: string[] = [];
    for (const [name] of this.clients) {
      const lastUsed = this._lastUsedAt.get(name);
      if (lastUsed !== undefined && now - lastUsed > this._idleTimeoutMs) {
        coreLog('mcp', `Idle timeout: stopping server '${name}' (idle ${Math.round((now - lastUsed) / 1000)}s)`);
        this.stopServer(name).catch(() => {});
        this._lastUsedAt.delete(name);
        evicted.push(name);
      }
    }
    return evicted;
  }

  disconnectAll(): void {
    this.stopHealthChecks();
    this.stopIdleChecker();
    this._toolPoller.stopAll();
    for (const timer of this.reconnectTimers.values()) {
      clearTimeout(timer);
    }
    this.reconnectTimers.clear();
    this.reconnectAttempts.clear();
    this._configWarnings.clear();
    this._configErrors.clear();
    this._configValidationErrors.clear();
    this._configValidationSources.clear();
    for (const name of this.clients.keys()) this._intentionalDisconnect.add(name);
    for (const client of this.clients.values()) {
      client.disconnect();
    }
    this._allTools.clear();
    this.pendingServers.clear();
    this.serverErrors.clear();
    this._suppressedDuplicates.clear();
    this._serverScopes.clear();
    this._serverSources.clear();
    this._serverConfigHashes.clear();
    this._lastUsedAt.clear();
    this._deniedServers = [];
  }

  /**
   * Load MCP server configs from a JSON file.
   *
   * M14 — when `opts.projectScoped` is true, this config came from a repo (a
   * project-tier `.mcp.json` / `.ami/mcp.json`), which is untrusted: a
   * cloned repo could point MCP at anything. Each NEW server that is not already in
   * the approved set is registered PENDING (awaiting approval) instead of being
   * auto-connected; only servers the user previously approved connect normally.
   * For user/global-tier configs (the default), every server is added as before.
   */
  loadFromConfig(configPath: string, opts?: { projectScoped?: boolean; scope?: McpServerScope }): void {
    const servers = this._readServerMap(configPath);
    if (!servers) return;
    // The scope a server from this file belongs to: an explicit scope wins; else
    // a projectScoped (under-cwd) file is `project`, and any other file is `user`.
    const scope: McpServerScope = opts?.scope ?? (opts?.projectScoped ? 'project' : 'user');
    this.loadServerEntries(servers, {
      projectScoped: opts?.projectScoped,
      scope,
      sourcePath: configPath,
    });
  }

  /**
   * Read + parse an MCP config file into its `{ name: entry }` server map, with the
   * same diagnostics as {@link loadFromConfig}: a MISSING file returns `null`
   * silently (expected); an UNREADABLE file (EACCES/EISDIR) and a MALFORMED file
   * (invalid JSON) each record a distinct fatal {@link McpConfigError} + emit
   * `config-error` and return `null`; a clean parse clears any stale error for the
   * path and returns the server map (or `null` when the top level is not an object).
   * Shared by {@link loadFromConfig} and {@link reloadFromConfig} so the two never
   * drift in how they read, strip comments, or report a broken config.
   */
  private _readServerMap(configPath: string): Record<string, unknown> | null {
    // A missing config file is expected (silent); only a file that EXISTS but is
    // malformed is a diagnosable error (surfaced below).
    if (!fs.existsSync(configPath)) {
      this._configErrors.delete(configPath);
      return null;
    }
    // Read the file first, in its own guard: an UNREADABLE file (a permission
    // problem, or a directory where a file was expected) is a distinct, actionable
    // failure from a readable-but-malformed one, so surface each with its own fix.
    let raw: string;
    try {
      raw = fs.readFileSync(configPath, 'utf-8');
    } catch (err) {
      const code = (err as { code?: string })?.code;
      const hint = code === 'EACCES'
        ? 'Check the file permissions (it is not readable by the current user).'
        : code === 'EISDIR'
          ? 'A directory exists at this path where an MCP config file was expected — remove it or point MCP at the file.'
          : 'Ensure the path is a readable MCP config file.';
      const message = `Cannot read MCP config "${configPath}"${code ? ` (${code})` : ''}: ${err instanceof Error ? err.message : String(err)}. ${hint}`;
      const diag: McpConfigError = { path: configPath, message, severity: 'fatal' };
      this._configErrors.set(configPath, diag);
      coreLog('mcp', message);
      this.emit('config-error', diag);
      return null;
    }
    try {
      const stripped = stripJsonBom(raw).replace(/^\s*\/\/.*$/gm, '');
      const config = JSON.parse(stripped);
      // Parsed cleanly — clear any stale parse error for this path.
      this._configErrors.delete(configPath);
      const servers = config?.mcpServers ?? config;
      if (typeof servers !== 'object' || servers === null || Array.isArray(servers)) return null;
      return servers as Record<string, unknown>;
    } catch (err) {
      // The file exists but is malformed (invalid JSON). Surface it as a fatal
      // diagnostic + event instead of silently dropping every server it declared,
      // so the user can see and fix their broken config (vs. a missing file, which
      // is expected and handled above).
      const reason = err instanceof Error ? err.message : String(err);
      // Actionable guidance for the most common JSON hand-edit mistakes, so the user
      // can fix the file instead of just seeing a raw parser offset (Claude-parity
      // readMcpJson actionable error, services/mcp/config.ts:L1438-1468).
      const message = `Failed to parse MCP config "${configPath}" as JSON: ${reason}. `
        + 'Check for a trailing comma, an unquoted key, a missing comma between entries, or an unclosed brace/bracket.';
      const diag: McpConfigError = { path: configPath, message, severity: 'fatal' };
      this._configErrors.set(configPath, diag);
      coreLog('mcp', message);
      this.emit('config-error', diag);
      return null;
    }
  }

  /**
   * Hot-reload an MCP config file, diffing the freshly-read entries against the
   * LIVE servers previously loaded from the SAME file (Claude-parity
   * `excludeStalePluginClients` / config-hash reload, useManageMCPConnections):
   *
   *  - a server whose entry is byte-for-byte UNCHANGED (same content hash) is LEFT
   *    running untouched — no needless disconnect / respawn / OAuth re-consent;
   *  - a server whose entry CHANGED is torn down (its reconnect timer cancelled by
   *    {@link removeServer}) and re-registered with the fresh config;
   *  - a server now ABSENT from the file is removed, so an edited-out server does
   *    not keep running as a ghost with stale tools;
   *  - a newly-added entry is registered (connect/approval-gated exactly like a
   *    first-time {@link loadFromConfig}).
   *
   * Only servers whose recorded source path equals `configPath` are considered for
   * removal, so reloading one scope never disturbs servers from another file. A
   * missing/unreadable/malformed file is handled exactly as in {@link loadFromConfig}
   * (its diagnostics are recorded) and treated as "no servers" — i.e. every server
   * that came from that path becomes stale and is removed. Returns the diff and
   * emits `config-reloaded` so a `/mcp reload` view / introspection can report it.
   */
  reloadFromConfig(
    configPath: string,
    opts?: { projectScoped?: boolean; scope?: McpServerScope },
  ): { added: string[]; changed: string[]; removed: string[]; unchanged: string[] } {
    const scope: McpServerScope = opts?.scope ?? (opts?.projectScoped ? 'project' : 'user');
    const servers = this._readServerMap(configPath) ?? {};
    const newNames = new Set(Object.keys(servers));
    // Snapshot the servers previously loaded from THIS file BEFORE any teardown
    // (removeServer mutates _serverSources), so absent-now servers are detectable.
    const previouslyFromThisFile = [...this._serverSources]
      .filter(([, p]) => p === configPath)
      .map(([n]) => n);

    const added: string[] = [];
    const changed: string[] = [];
    const removed: string[] = [];
    const unchanged: string[] = [];

    const toLoad: Record<string, unknown> = {};
    for (const [name, entry] of Object.entries(servers)) {
      const hash = hashMcpServerEntry(entry);
      const isLive = this.clients.has(name) || this.pendingServers.has(name);
      if (isLive && this._serverConfigHashes.get(name) === hash) {
        unchanged.push(name);
        continue;
      }
      if (isLive) {
        // Config changed under a live server: tear it down (cancels its reconnect
        // timer + defuses the onclose auto-reconnect via _intentionalDisconnect)
        // so the fresh entry re-registers cleanly instead of hitting the
        // "already registered" guard.
        this.removeServer(name);
        changed.push(name);
      } else {
        added.push(name);
      }
      toLoad[name] = entry;
    }
    // Servers that were loaded from this file but are no longer declared in it are
    // stale ghosts — remove them (changed servers are in `newNames`, so excluded).
    for (const name of previouslyFromThisFile) {
      if (!newNames.has(name)) {
        this.removeServer(name);
        removed.push(name);
      }
    }
    if (Object.keys(toLoad).length > 0) {
      this.loadServerEntries(toLoad, { projectScoped: opts?.projectScoped, scope, sourcePath: configPath });
    }
    this.emit('config-reloaded', configPath, { added, changed, removed, unchanged });
    return { added, changed, removed, unchanged };
  }

  /**
   * Load a map of `{ name: serverConfig }` entries (shared by {@link loadFromConfig}
   * and {@link loadManagedServers}). Each entry gets per-server isolation,
   * `type`→`transport` normalization, shape validation, env expansion with
   * missing-var diagnostics, denylist skipping, M14 project-approval gating, and is
   * tagged with `opts.scope` + `opts.sourcePath` for scope grouping.
   */
  private loadServerEntries(
    servers: Record<string, unknown>,
    opts: { projectScoped?: boolean; scope: McpServerScope; sourcePath?: string },
  ): void {
      for (const [name, serverConfig] of Object.entries(servers)) {
        // Enterprise exclusive-control (allowManagedMcpServersOnly): when the managed
        // tier has claimed sole control of MCP, a non-enterprise server never loads —
        // skip it with a diagnostic so the user understands why their config server is
        // absent (Claude-parity getClaudeCodeMcpConfigs exclusive managed control).
        if (this._allowManagedServersOnly && opts.scope !== 'enterprise') {
          coreLog('mcp', `skipping non-managed MCP server "${name}": only admin-managed MCP servers are allowed (allowManagedMcpServersOnly)`);
          this.emit('config-blocked', name, 'allowManagedMcpServersOnly');
          continue;
        }
        // Per-server isolation: one malformed/throwing entry must NOT abort the
        // rest of the file (Claude-parity) — each entry is loaded in its own guard.
        try {
          // Map a Claude-Code-style `type` transport discriminator onto AMI's
          // `transport` field so a `{ "type": "http", "url": ... }` interop entry
          // is understood.
          const normalized = normalizeMcpServerEntry(serverConfig);
          // Reject a malformed entry (wrong-typed args/env/headers, unknown
          // transport, non-object) with a per-server diagnostic instead of casting
          // arbitrary JSON straight into a child-process spawn / HTTP request.
          const issues = validateMcpServerConfigShape(normalized);
          if (issues.length > 0) {
            const msg = issues.join('; ');
            this._configValidationErrors.set(name, msg);
            if (opts.sourcePath) this._configValidationSources.set(name, opts.sourcePath);
            coreLog('mcp', `invalid MCP server "${name}": ${msg}`);
            this.emit('config-invalid', name, issues);
            continue;
          }
          this._configValidationErrors.delete(name);
          this._configValidationSources.delete(name);
          const cfg = normalized as McpServerConfig;
          if (!cfg.command && !cfg.url) continue;
          const missing = new Set<string>();
          const expanded = this.expandConfigEnv(cfg, missing);
          // Collect non-fatal, recoverable config warnings for this server (the
          // server is still registered): (a) env vars referenced but unset — a common
          // cause of a server failing to start (H35); (b) the Windows npx-without-cmd
          // wrapper footgun (platform-specific, Claude-parity).
          const warnings: string[] = [];
          if (missing.size > 0) {
            const list = [...missing];
            coreLog('mcp', `MCP server "${name}" references unset env var(s): ${list.join(', ')}`);
            warnings.push(...list);
          }
          const npxWarning = detectWindowsNpxWrapperWarning(expanded, this._platform);
          if (npxWarning) {
            coreLog('mcp', `MCP server "${name}": ${npxWarning}`);
            warnings.push(npxWarning);
          }
          if (warnings.length > 0) {
            this._configWarnings.set(name, warnings);
            this.emit('config-warning', name, warnings);
          } else {
            this._configWarnings.delete(name);
          }
          // Skip denylisted servers without aborting the rest of the load (S9).
          if (this.isServerDenied(name, expanded)) {
            coreLog('mcp', `skipping denied MCP server "${name}"`);
            continue;
          }
          // M14 (U018) — a project-tier server the user previously REJECTED is
          // SUPPRESSED entirely: never registered, never re-surfaced for approval
          // (Claude's disabledMcpjsonServers). Checked BEFORE recording scope so a
          // rejected server leaves no orphan metadata behind.
          if (opts.projectScoped && this.getProjectMcpServerStatus(name) === 'rejected') {
            coreLog('mcp', `skipping rejected project MCP server "${name}"`);
            continue;
          }
          // First-wins precedence: a higher-precedence scope (e.g. local/project
          // over user) already owns this name. Rather than letting addServer throw a
          // FATAL "already registered" error, record the drop as an INFORMATIONAL
          // suppressed-duplicate diagnostic (Claude-parity mcp-server-suppressed-
          // duplicate) so the user learns why their second config entry isn't showing
          // up. The winning server keeps its scope/config untouched.
          if (this.clients.has(name) || this.pendingServers.has(name)) {
            this._suppressedDuplicates.set(name, name);
            coreLog('mcp', `suppressing duplicate MCP server "${name}": a server with this name is already configured (first-wins)`);
            this.emit('duplicate-suppressed', name, name);
            continue;
          }
          // Tag the server with its config-source scope + path BEFORE registering it
          // so the add paths see the recorded scope rather than defaulting to dynamic.
          this.recordServerScope(name, opts.scope, opts.sourcePath);
          // Record the RAW entry's content hash for config-reload change detection
          // (see {@link reloadFromConfig}) so a later reload can tell this server's
          // entry apart from an edited one.
          this._serverConfigHashes.set(name, hashMcpServerEntry(serverConfig));
          // M14 (U018) — gate project-tier servers by their resolved approval
          // status: `pending` awaits explicit approval; `approved` (an approval
          // entry OR the blanket enableAllProjectServers flag) connects normally.
          // `rejected` was already handled above.
          if (opts.projectScoped && this.getProjectMcpServerStatus(name) === 'pending') {
            coreLog('mcp', `project MCP server "${name}" requires approval — registered pending`);
            this.addPendingServer(name, expanded, { needsApproval: true });
            continue;
          }
          this.addServer(name, expanded);
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          this._configValidationErrors.set(name, message);
          if (opts.sourcePath) this._configValidationSources.set(name, opts.sourcePath);
          coreLog('mcp', `failed to load MCP server "${name}": ${message}`);
          this.emit('config-invalid', name, [message]);
        }
      }
  }

  private expandConfigEnv(config: McpServerConfig, missingOut?: Set<string>): McpServerConfig {
    const result = { ...config };
    // Plugin-scoped substitution tier (${user_config.X}, ${CLAUDE_PLUGIN_ROOT},
    // ${CLAUDE_PLUGIN_DATA}) runs BEFORE general ${ENV} expansion so a user/plugin
    // value wins over a same-named process env var (Claude-parity precedence).
    const pluginVars: PluginMcpVars = {
      userConfig: config.userConfig,
      pluginRoot: config.pluginRoot,
      pluginDataDir: config.pluginDataDir,
    };
    const hasPluginVars = config.userConfig !== undefined || config.pluginRoot !== undefined || config.pluginDataDir !== undefined;
    const expand = (v: string): string => {
      const substituted = hasPluginVars ? substitutePluginMcpVars(v, pluginVars) : v;
      const { expanded, missingVars } = expandEnvVarsTracked(substituted);
      if (missingOut) for (const m of missingVars) missingOut.add(m);
      return expanded;
    };

    if (result.command) {
      result.command = expand(result.command);
    }
    if (result.args) {
      result.args = result.args.map(expand);
    }
    if (result.url) {
      result.url = expand(result.url);
    }
    if (result.env) {
      const expanded: Record<string, string> = {};
      for (const [k, v] of Object.entries(result.env)) {
        expanded[k] = expand(v);
      }
      result.env = expanded;
    }

    // For a stdio server, inject the plugin root/data dir into the launch env as
    // literal paths so a bundled stdio server can locate its own install + data
    // directory. These two keys are NOT run through ${ENV} expansion
    // (Claude-parity, mcpPluginIntegration.ts:L517): they are already resolved
    // absolute paths. Explicit env entries win, so an author can still override.
    const isStdio = !result.url && (result.transport === undefined || result.transport === 'stdio');
    if (isStdio && (config.pluginRoot !== undefined || config.pluginDataDir !== undefined)) {
      const injected: Record<string, string> = {};
      if (config.pluginRoot !== undefined) injected.CLAUDE_PLUGIN_ROOT = config.pluginRoot;
      if (config.pluginDataDir !== undefined) injected.CLAUDE_PLUGIN_DATA = config.pluginDataDir;
      result.env = { ...injected, ...(result.env ?? {}) };
    }

    return result;
  }

  private invalidateToolCache(serverName: string): void {
    for (const [toolKey, entry] of this._allTools) {
      if (entry.serverName === serverName) this._allTools.delete(toolKey);
    }
  }

  private async refreshToolsForServer(name: string): Promise<void> {
    const client = this.clients.get(name);
    if (!client || client.state !== 'ready') return;

    try {
      const tools = await client.listTools();
      this.invalidateToolCache(name);
      this.ingestServerTools(name, tools);
    } catch (err) {
      coreLog('mcp', `Failed to refresh tools for ${name}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  /**
   * Ingest a server's advertised tools into the live tool cache, applying the
   * per-server tool-count cap ({@link MAX_MCP_TOOLS_PER_SERVER}, Claude-parity):
   * a server that advertises more than the cap has only its first `cap` tools
   * loaded, and a recoverable config warning is recorded (surfaced in /mcp
   * diagnostics) so the drop is visible instead of silent — defending the model's
   * context + tool list against a misbehaving/malicious server flooding it. A
   * cross-server tool-name conflict is logged (qualified names still disambiguate).
   * Shared by {@link connectServer} and {@link refreshToolsForServer} so the cache
   * population + cap live in one place and cannot drift.
   */
  private ingestServerTools(name: string, tools: McpToolSchema[]): void {
    let accepted = tools;
    if (tools.length > MAX_MCP_TOOLS_PER_SERVER) {
      accepted = tools.slice(0, MAX_MCP_TOOLS_PER_SERVER);
      const warning = `advertises ${tools.length} tools; only the first ${MAX_MCP_TOOLS_PER_SERVER} are loaded (per-server tool cap)`;
      coreLog('mcp', `MCP server "${name}" ${warning}`);
      const existing = this._configWarnings.get(name) ?? [];
      if (!existing.includes(warning)) {
        this._configWarnings.set(name, [...existing, warning]);
        this.emit('config-warning', name, [warning]);
      }
    }
    for (const tool of accepted) {
      const key = `${name}:${tool.name}`;
      const schema = { ...tool, description: this.truncateDescription(tool.description || '') };
      const existing = this.findToolByName(tool.name);
      if (existing && existing.serverName !== name) {
        coreLog('mcp', `tool name conflict: "${tool.name}" provided by both "${existing.serverName}" and "${name}" — use qualified name "${key}" to disambiguate`);
      }
      this._allTools.set(key, { serverName: name, schema });
    }
  }

  private scheduleReconnect(name: string): void {
    const existingTimer = this.reconnectTimers.get(name);
    if (existingTimer) {
      clearTimeout(existingTimer);
    }

    const client = this.clients.get(name);
    if (!client) return;

    const attempt = this.reconnectAttempts.get(name) ?? 0;
    // Give up after MAX_MCP_RECONNECT_ATTEMPTS so a permanently-down server does
    // not reconnect-loop forever; surface a terminal 'reconnect-failed' event.
    if (attempt >= MAX_MCP_RECONNECT_ATTEMPTS) {
      this.reconnectAttempts.delete(name);
      coreLog('mcp', `Giving up reconnecting to ${name} after ${attempt} attempts`);
      this.emit('reconnect-failed', name, attempt);
      return;
    }

    // Exponential backoff, capped: 1s, 2s, 4s, 8s, 16s (≤ MAX_MCP_RECONNECT_DELAY_MS).
    const delay = Math.min(MCP_RECONNECT_BASE_DELAY_MS * 2 ** attempt, MAX_MCP_RECONNECT_DELAY_MS);
    const timer = setTimeout(async () => {
      this.reconnectTimers.delete(name);
      // A server disabled while the timer was pending must not be revived.
      if (this.isDisabled(name)) { this.reconnectAttempts.delete(name); return; }
      try {
        await client.reconnect();
        coreLog('mcp', `Reconnected to ${name}`);
        this.reconnectAttempts.delete(name);
        await this.refreshToolsForServer(name);
      } catch (err) {
        coreLog('mcp', `Reconnect failed for ${name} (attempt ${attempt + 1}): ${err instanceof Error ? err.message : String(err)}`);
        this.reconnectAttempts.set(name, attempt + 1);
        this.scheduleReconnect(name);
      }
    }, delay);
    timer.unref();

    this.reconnectTimers.set(name, timer);
  }

  private findToolByName(name: string): { serverName: string; schema: McpToolSchema } | undefined {
    for (const entry of this._allTools.values()) {
      if (entry.schema.name === name) return entry;
    }
    return undefined;
  }

}

export const MAX_MCP_TOOLS_PER_SERVER = 200;
export const MAX_MCP_TOOL_NAME_LENGTH = 128;

/**
 * A health-check tick arriving later than `interval × this factor` is treated as
 * evidence the host process was suspended (sleep/VM-pause/freeze) rather than merely
 * busy: `setInterval` coalesces the ticks it missed while paused into one late fire.
 */
export const MCP_SUSPENSION_TICK_FACTOR = 2;

/** Reconnect backoff bounds for a dropped MCP server (H44). */
export const MAX_MCP_RECONNECT_ATTEMPTS = 5;
export const MCP_RECONNECT_BASE_DELAY_MS = 1000;
export const MAX_MCP_RECONNECT_DELAY_MS = 30_000;

/**
 * A proactive MCP connectivity notification (Claude-parity useMcpConnectivityStatus
 * `addNotification`): a dedupe `key`, a `severity` (error for failed, warning for
 * needs-auth), and a pluralized `message` ending in the ` · /mcp` pointer. The
 * frontend renders these as transient status notifications.
 */
export interface McpConnectivityNotification {
  key: 'mcp-failed' | 'mcp-needs-auth';
  severity: 'error' | 'warning';
  message: string;
}

/**
 * Format a {@link McpConnectivitySummary} into the proactive notifications Claude
 * pushes (useMcpConnectivityStatus.tsx:L36-56): `N MCP server(s) failed · /mcp`
 * (error) and `N MCP server(s) need auth · /mcp` (warning), each pluralized and
 * dedupe-keyed. Returns `[]` when nothing is failed/needs-auth (the early-return
 * "nothing to notify" case), so a caller can render zero banners.
 */
export function formatMcpConnectivitySummary(summary: McpConnectivitySummary): McpConnectivityNotification[] {
  const out: McpConnectivityNotification[] = [];
  if (summary.failed.length > 0) {
    const n = summary.failed.length;
    out.push({ key: 'mcp-failed', severity: 'error', message: `${n} MCP ${n === 1 ? 'server' : 'servers'} failed · /mcp` });
  }
  if (summary.needsAuth.length > 0) {
    const n = summary.needsAuth.length;
    out.push({ key: 'mcp-needs-auth', severity: 'warning', message: `${n} MCP ${n === 1 ? 'server needs' : 'servers need'} auth · /mcp` });
  }
  return out;
}

/**
 * One non-zero state bucket of a {@link McpServerStateSummary}, in the fixed
 * severity-aware render order the status pane uses. `severity` is the color class a
 * frontend maps to a theme color; `label` is the pre-pluralized count phrase.
 */
export interface McpStateSummaryPart {
  state: 'connected' | 'needsAuth' | 'pending' | 'failed';
  count: number;
  label: string;
  severity: 'success' | 'warning' | 'inactive' | 'error';
}

/**
 * Format a {@link McpServerStateSummary} into the ordered, non-empty parts a status
 * pane renders as a single compact "N connected, N need auth, N pending, N failed
 * · /mcp" row (Claude-parity `buildMcpProperties`, status.tsx:L99-115). Only
 * non-zero buckets appear, always in the fixed severity order
 * connected→needsAuth→pending→failed so the row reads consistently regardless of
 * which states are present. Returns `[]` when every bucket is zero (no actionable
 * MCP servers → render nothing).
 */
export function formatMcpStateSummary(summary: McpServerStateSummary): McpStateSummaryPart[] {
  const parts: McpStateSummaryPart[] = [];
  if (summary.connected > 0) parts.push({ state: 'connected', count: summary.connected, label: `${summary.connected} connected`, severity: 'success' });
  if (summary.needsAuth > 0) parts.push({ state: 'needsAuth', count: summary.needsAuth, label: `${summary.needsAuth} need auth`, severity: 'warning' });
  if (summary.pending > 0) parts.push({ state: 'pending', count: summary.pending, label: `${summary.pending} pending`, severity: 'inactive' });
  if (summary.failed > 0) parts.push({ state: 'failed', count: summary.failed, label: `${summary.failed} failed`, severity: 'error' });
  return parts;
}

/** Transport values AMI understands (for config validation / `type` mapping).
 *  Includes the Claude-parity IDE discriminants (`sse-ide`/`ws-ide`) and the
 *  in-process `sdk` transport so a `{ "type": "sse-ide", … }` entry validates. */
const SUPPORTED_MCP_TRANSPORTS = new Set(['stdio', 'sse', 'http', 'ws', 'wss', 'sse-ide', 'ws-ide', 'sdk']);

/**
 * The reserved MCP server name for the connected code editor (Claude-parity:
 * `client.name === 'ide'`, useIdeConnectionStatus.ts). The one server whose
 * connection drives the editor-integration badge; special-cased by
 * {@link McpManager.getIdeConnectionStatus}.
 */
export const IDE_MCP_SERVER_NAME = 'ide';

/**
 * Canonicalize a transport discriminator to the BASE transport its connection is
 * actually driven over (Claude-parity IDE discriminants): `sse-ide`→`sse`,
 * `ws-ide`→`ws`; every base transport (and `sdk`/`undefined`) is returned
 * unchanged. Routed through by {@link McpManager.effectiveTransport} so an IDE
 * server connects over its real transport without a bespoke dispatch arm — the
 * `-ide` suffix only tags provenance + an {@link McpServerConfig.ideName}, not a
 * different wire protocol.
 */
export function canonicalMcpTransport(
  transport: McpServerConfig['transport'] | undefined,
): 'stdio' | 'sse' | 'http' | 'ws' | 'wss' | 'sdk' | undefined {
  if (transport === 'sse-ide') return 'sse';
  if (transport === 'ws-ide') return 'ws';
  return transport;
}

/**
 * The human-readable editor name for an IDE MCP server, gated on its transport
 * being an IDE discriminant (Claude-parity, useIdeConnectionStatus.ts:L20-24:
 * `config.type === 'sse-ide' || config.type === 'ws-ide' ? config.ideName : null`).
 * Returns `null` for a non-IDE server (or a missing name) so a stray `ideName` on
 * a plain sse/http server is never surfaced as an editor badge.
 */
export function getMcpIdeName(config: McpServerConfig | undefined): string | null {
  if (!config) return null;
  return config.transport === 'sse-ide' || config.transport === 'ws-ide'
    ? config.ideName ?? null
    : null;
}

/**
 * Env var carrying the port of the code-editor's MCP SSE endpoint, inherited from
 * the editor extension that launched AMI (Claude-parity `CLAUDE_CODE_SSE_PORT`).
 * The extension passes the port through the environment because terminal
 * multiplexers (tmux/screen) clobber `TERM_PROGRAM`, so the editor can no longer
 * be detected from the terminal alone once inside a multiplexer.
 */
export const MCP_IDE_SSE_PORT_ENV = 'AMI_SSE_PORT';

/**
 * Env var that force-ENABLES (a truthy value) or force-DISABLES (an explicit
 * falsy value) IDE MCP auto-connect, overriding every other waterfall input
 * (Claude-parity `CLAUDE_CODE_AUTO_CONNECT_IDE`). A falsy value is a hard opt-out
 * that wins over the config knob, the flag, and a supported terminal.
 */
export const MCP_IDE_AUTOCONNECT_ENV = 'AMI_AUTO_CONNECT_IDE';

/** Terminals of code editors that embed a terminal and expose an MCP SSE endpoint. */
const SUPPORTED_IDE_TERM_PROGRAMS = new Set(['vscode', 'cursor', 'windsurf']);

/**
 * True when the current terminal is a known code-editor terminal
 * (Claude-parity `isSupportedTerminal`): VS Code / Cursor / Windsurf expose an
 * embedded terminal whose `TERM_PROGRAM` identifies the host editor, which is a
 * strong signal the user wants the editor integration auto-connected.
 */
export function isSupportedIdeTerminal(env: NodeJS.ProcessEnv = process.env): boolean {
  const tp = env.TERM_PROGRAM;
  return tp !== undefined && SUPPORTED_IDE_TERM_PROGRAMS.has(tp.toLowerCase());
}

/**
 * Re-exported from the canonical env-utils home ({@link isEnvDefinedFalsy}) so the
 * IDE auto-connect waterfall and every other opt-out site share ONE definition
 * (§0b.6 — no per-site re-derivation). The hard-opt-out companion to
 * {@link isEnvTruthy}: `true` ONLY for an EXPLICIT falsy env value
 * (`0`/`false`/`no`/`off`), never for unset/empty.
 */
export { isEnvDefinedFalsy };

/** Inputs to the IDE auto-connect waterfall ({@link shouldAutoConnectIde}). */
export interface IdeAutoConnectOptions {
  /** The persisted `autoConnectIde` config knob (EngineConfig.autoConnectIde). */
  configEnabled?: boolean;
  /** The `--ide` CLI flag (one-shot force-enable for this run). */
  flag?: boolean;
  /** An editor extension is pending installation (a connect intent is implied). */
  ideToInstall?: boolean;
  /** Environment to read the port / override env vars + TERM_PROGRAM from. */
  env?: NodeJS.ProcessEnv;
}

/**
 * Decide whether to auto-connect to a code-editor-launched MCP endpoint, matching
 * Claude Code's `addIde` waterfall (useIDEIntegration.ts:L32-36):
 *   (config knob OR `--ide` flag OR a supported editor terminal OR an inherited
 *    SSE port OR a pending extension install OR a truthy `AMI_AUTO_CONNECT_IDE`)
 *   AND NOT an explicit falsy `AMI_AUTO_CONNECT_IDE`.
 * The explicit-falsy override is a HARD opt-out that wins over every enabling input.
 */
export function shouldAutoConnectIde(opts: IdeAutoConnectOptions = {}): boolean {
  const env = opts.env ?? process.env;
  const override = env[MCP_IDE_AUTOCONNECT_ENV];
  if (isEnvDefinedFalsy(override)) return false;
  return Boolean(
    opts.configEnabled ||
    opts.flag ||
    isSupportedIdeTerminal(env) ||
    env[MCP_IDE_SSE_PORT_ENV] ||
    opts.ideToInstall ||
    isEnvTruthy(override),
  );
}

/**
 * Resolve the code-editor's MCP SSE endpoint URL from the inherited
 * {@link MCP_IDE_SSE_PORT_ENV} port, or `null` when the port is unset / not a
 * valid 1..65535 TCP port. The editor extension serves MCP over a loopback SSE
 * endpoint; AMI connects to `http://127.0.0.1:<port>/sse` (Claude-parity local
 * IDE transport). Loopback-only — never a remote host.
 */
export function resolveIdeMcpEndpoint(env: NodeJS.ProcessEnv = process.env): { url: string } | null {
  const raw = env[MCP_IDE_SSE_PORT_ENV];
  if (!raw) return null;
  const port = parseInt(raw, 10);
  if (!Number.isInteger(port) || port <= 0 || port > 65535) return null;
  return { url: `http://127.0.0.1:${port}/sse` };
}

/**
 * A code editor AMI can connect to as the reserved `ide` MCP server
 * (Claude-parity `DetectedIDEInfo`, utils/ide.ts): the editor's display `name`
 * and the `url` of the MCP endpoint it exposes, plus the optional `authToken`
 * the endpoint requires and whether the editor is `ideRunningInWindows` (used by
 * a WSL caller for path translation). This is the input to the interactive
 * connect flow ({@link McpManager.connectIde}) and the reconciliation lookup
 * ({@link McpManager.findCurrentIde}).
 */
export interface DetectedIdeInfo {
  /** Human-readable editor name (e.g. "VS Code", "JetBrains"). */
  name: string;
  /** MCP endpoint URL the editor exposes (an sse `http(s)://` or ws `ws(s)://`). */
  url: string;
  /** Bearer token the endpoint requires, when the editor gated it. */
  authToken?: string;
  /** True when the editor runs on the Windows host of a WSL session. */
  ideRunningInWindows?: boolean;
}

/**
 * The IDE transport discriminant implied by an endpoint URL's scheme: a
 * WebSocket endpoint (`ws://` / `wss://`) is a `ws-ide`, anything else (an
 * `http(s)://` SSE endpoint) is an `sse-ide`. The `-ide` suffix only tags
 * provenance + an {@link McpServerConfig.ideName}; {@link canonicalMcpTransport}
 * maps it back to the base wire transport. Divergence from Claude Code
 * (ide.tsx:L585, which tests only `startsWith('ws:')` and so mis-tags a `wss://`
 * endpoint as `sse-ide`): AMI also recognizes `wss://` so a secure WebSocket IDE
 * connects over its real transport.
 */
export function ideTransportForUrl(url: string): 'sse-ide' | 'ws-ide' {
  return url.startsWith('ws:') || url.startsWith('wss:') ? 'ws-ide' : 'sse-ide';
}

/**
 * How long {@link McpManager.connectIde} waits for the reserved `ide` server to
 * reach a terminal state before reporting a `timeout` (Claude-parity
 * `IDE_CONNECTION_TIMEOUT_MS`, ide.tsx:L507). The connect itself has its own
 * bounded retry/backoff; this is the outer ceiling the UI shows a result by.
 */
export const IDE_CONNECTION_TIMEOUT_MS = 35000;

/**
 * Terminal outcome of an interactive IDE connect ({@link McpManager.connectIde}):
 * `connected` (handshake succeeded), `failed` (handshake rejected), or `timeout`
 * (no terminal state within {@link IDE_CONNECTION_TIMEOUT_MS}). `message` is the
 * ready-to-display sentence, matching Claude Code's IDECommandFlow wording.
 */
export interface IdeConnectResult {
  outcome: 'connected' | 'failed' | 'timeout';
  message: string;
}

/** Options for {@link McpManager.connectIde}. */
export interface IdeConnectOptions {
  /** Override the {@link IDE_CONNECTION_TIMEOUT_MS} outer ceiling (ms). */
  timeoutMs?: number;
}

/**
 * Derived tri-state (+null) IDE connection status a UI badge renders
 * (Claude-parity `IdeStatus`, useIdeConnectionStatus.ts): `null` means no editor
 * is connected (no `ide` server at all). Present-but-not-connected buckets as
 * `connected` (server `ready`), `pending` (still connecting), or `disconnected`
 * (any other state — error/stopped/needs-auth/disabled).
 */
export type IdeStatus = 'connected' | 'pending' | 'disconnected' | null;

/**
 * Map a live MCP server state onto the editor-badge tri-state (Claude-parity
 * useIdeConnectionStatus: `connected` ↔ `ready`, `pending` ↔ connecting,
 * everything else ↔ `disconnected`). The `null` ("no editor") case is decided by
 * the caller from server presence, so this never returns `null`.
 */
export function ideStatusFromServerState(
  state: McpServerState | McpClientState | undefined,
): Exclude<IdeStatus, null> {
  if (state === 'ready') return 'connected';
  if (state === 'pending' || state === 'connecting') return 'pending';
  return 'disconnected';
}

/**
 * Whether a transport is REMOTE (network-backed) rather than a local process.
 * Remote transports (http/sse/ws/wss, plus the IDE discriminants that ride them)
 * share dropped-connection recovery: a close schedules an immediate reconnect,
 * they are grouped under the remote connection pool, and a detected
 * process-suspension force-reconnects them. Single source of truth so every
 * remote-vs-local decision stays consistent as transports are added.
 */
export function isRemoteMcpTransport(transport: string | undefined): boolean {
  return transport === 'http' || transport === 'sse' || transport === 'ws' || transport === 'wss'
    || transport === 'sse-ide' || transport === 'ws-ide';
}

/** URL schemes a remote MCP endpoint may use (well-formedness validation). */
const MCP_URL_SCHEMES = new Set(['http:', 'https:', 'ws:', 'wss:']);

/**
 * Infer a REMOTE transport from an endpoint URL's scheme when a config supplies a
 * `url` but no explicit `transport` (Claude-parity scheme inference, ide.tsx:L585-593
 * `url.startsWith('ws:') ? 'ws-ide' : 'sse-ide'`): a `ws:`/`wss:` URL is a WebSocket
 * transport, an `http:`/`https:` URL is the Streamable-HTTP transport. Returns
 * `undefined` for a non-URL / unknown scheme so the caller falls back to its default
 * (stdio) dispatch. This stops a `{ "url": "wss://…" }` entry with no `transport`
 * from being wrongly dispatched down the stdio `connect()` path.
 */
export function inferMcpTransportFromUrl(url: string | undefined): 'ws' | 'wss' | 'http' | undefined {
  if (!url) return undefined;
  try {
    const proto = new URL(url.trim()).protocol;
    if (proto === 'ws:') return 'ws';
    if (proto === 'wss:') return 'wss';
    if (proto === 'http:' || proto === 'https:') return 'http';
  } catch {
    return undefined;
  }
  return undefined;
}

/**
 * True when `raw` is a well-formed remote MCP endpoint URL — it parses as a URL AND
 * its scheme is one of http/https/ws/wss (Claude-parity add-time URL validation,
 * xaaIdpCommand.ts:L44-58: a user-supplied URL is validated as a real URL BEFORE it
 * is persisted, so one malformed value cannot poison the settings file on next load).
 * Enforced at `/mcp add` time (not config load) so a hand-edited config stays lenient,
 * exactly like {@link validateMcpServerName}.
 */
export function isValidMcpUrl(raw: string): boolean {
  try {
    return MCP_URL_SCHEMES.has(new URL(raw.trim()).protocol);
  } catch {
    return false;
  }
}

/**
 * Canonicalize an MCP tool name for verb classification: camelCase and kebab-case
 * reduce to a single snake_case key (Claude-parity `normalize()`), so a read/search
 * verb is found even when it is not the leading token (e.g. `searchJiraIssues` →
 * `search_jira_issues`, `brave-web-search` → `brave_web_search`).
 */
export function normalizeMcpToolName(name: string): string {
  return name
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/-/g, '_')
    .toLowerCase();
}

/**
 * Normalize a raw MCP server config entry: map a Claude-Code-style `type`
 * transport discriminator (`{ "type": "http", "url": ... }`) onto AMI's canonical
 * `transport` field when `transport` is absent, and drop the `type` key. Non-object
 * entries are returned unchanged for the validator to reject.
 */
export function normalizeMcpServerEntry(raw: unknown): unknown {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) return raw;
  const c = { ...(raw as Record<string, unknown>) };
  if (c.transport === undefined && c.type !== undefined) {
    c.transport = c.type; // keep even an unsupported value so validation flags it
  }
  delete c.type;
  return c;
}

/**
 * Lightweight runtime validation of an (untrusted) MCP server config entry before
 * it reaches a child-process spawn / HTTP request (Claude-parity schema guard).
 * Returns a list of human-readable problems ([] when valid): `command`/`url` must
 * be strings, `args` a string array, `env`/`headers` string→string objects, and
 * `transport` one of stdio|sse|http|ws|wss. The entry is skipped when non-empty.
 */
export function validateMcpServerConfigShape(cfg: unknown): string[] {
  if (typeof cfg !== 'object' || cfg === null || Array.isArray(cfg)) {
    return ['server config must be a JSON object'];
  }
  const c = cfg as Record<string, unknown>;
  const issues: string[] = [];
  if (c.command !== undefined && typeof c.command !== 'string') issues.push('`command` must be a string');
  if (c.url !== undefined && typeof c.url !== 'string') issues.push('`url` must be a string');
  if (c.args !== undefined && (!Array.isArray(c.args) || c.args.some((a) => typeof a !== 'string'))) {
    issues.push('`args` must be an array of strings');
  }
  for (const key of ['env', 'headers', 'userConfig'] as const) {
    const v = c[key];
    if (v !== undefined) {
      if (typeof v !== 'object' || v === null || Array.isArray(v) || Object.values(v as Record<string, unknown>).some((x) => typeof x !== 'string')) {
        issues.push(`\`${key}\` must be an object of string values`);
      }
    }
  }
  for (const key of ['pluginRoot', 'pluginDataDir', 'ideName'] as const) {
    if (c[key] !== undefined && typeof c[key] !== 'string') issues.push(`\`${key}\` must be a string`);
  }
  if (c.transport !== undefined && !SUPPORTED_MCP_TRANSPORTS.has(String(c.transport))) {
    issues.push(`unsupported transport "${String(c.transport)}" (expected stdio, sse, http, ws, wss, sse-ide, ws-ide, or sdk)`);
  }
  if (c.oauth !== undefined) {
    if (typeof c.oauth !== 'object' || c.oauth === null || Array.isArray(c.oauth)) {
      issues.push('`oauth` must be an object');
    } else {
      const o = c.oauth as Record<string, unknown>;
      // Claude-parity McpOAuthConfigSchema: an explicit AS-metadata override must be an
      // https URL (never plaintext — it drives token/credential exchange), and a pinned
      // callback port must be a usable TCP port (positive integer in range).
      if (o.authServerMetadataUrl !== undefined) {
        if (typeof o.authServerMetadataUrl !== 'string' || !isHttpsUrl(o.authServerMetadataUrl)) {
          issues.push('`oauth.authServerMetadataUrl` must be an https:// URL');
        }
      }
      if (o.callbackPort !== undefined && !isValidCallbackPort(o.callbackPort as number)) {
        issues.push('`oauth.callbackPort` must be a positive integer (1..65535)');
      }
    }
  }
  return issues;
}

/**
 * True when `value` parses as a URL whose protocol is exactly `https:`. Used to
 * validate OAuth metadata-URL overrides, which (unlike loopback auth endpoints) must
 * never be plaintext. Distinct from oauth.ts `assertSecureUrl` (which tolerates
 * http loopback) because an AS-metadata URL has no loopback exception.
 */
function isHttpsUrl(value: string): boolean {
  try {
    return new URL(value).protocol === 'https:';
  } catch {
    return false;
  }
}

/**
 * Validate a user-supplied MCP SERVER name before it is persisted (Claude-parity
 * `addMcpConfig` name check, services/mcp/config.ts:L630-L679). A server name becomes
 * part of the model-facing qualified tool namespace (`mcp__<server>__<tool>`) and
 * AMI's own `serverName:toolName` call resolution splits on the first `:`, so a name
 * containing a space, colon, dot, slash, or any other non-`[A-Za-z0-9_-]` character
 * would corrupt qualified-tool resolution or produce an ambiguous tool name. Returns
 * a human-readable error message for an empty or ill-formed name, else `null` (valid).
 * Enforced at `/mcp add` time (not config load) so a hand-edited config stays lenient,
 * exactly like Claude's add-time-only validation.
 */
export function validateMcpServerName(name: string): string | null {
  const trimmed = name.trim();
  if (!trimmed) return 'MCP server name must not be empty';
  const bad = trimmed.match(/[^a-zA-Z0-9_-]/);
  if (bad) {
    return `Invalid MCP server name "${trimmed}": only letters, numbers, hyphens, and underscores are allowed (found ${JSON.stringify(bad[0])}). The name becomes part of the "mcp__<server>__<tool>" tool namespace.`;
  }
  return null;
}

/**
 * Detect the common Windows footgun where a stdio MCP server's `command` is `npx`
 * (bare, or ending in `\npx` / `/npx`) WITHOUT the required `cmd /c` wrapper — on
 * Windows such a server fails to spawn with an opaque error. Returns an actionable
 * warning string (with the concrete fix) on win32, else null (no-op on every other
 * platform). Non-stdio entries (those carrying a `url`) are never flagged. This is a
 * recoverable WARNING, not a fatal error: the server is still registered; the hint
 * is surfaced via {@link McpManager.getConfigWarnings}. Mirrors Claude's
 * parseMcpConfig npx check.
 */
export function detectWindowsNpxWrapperWarning(
  config: McpServerConfig,
  platform: NodeJS.Platform,
): string | null {
  if (platform !== 'win32') return null;
  if (config.transport && config.transport !== 'stdio') return null;
  if (config.url) return null;
  const cmd = config.command;
  if (!cmd) return null;
  if (cmd === 'npx' || cmd.endsWith('\\npx') || cmd.endsWith('/npx')) {
    return 'Windows requires a \'cmd /c\' wrapper to execute npx: change command to "cmd" with args ["/c", "npx", ...]';
  }
  return null;
}

const ALLOWED_SCHEMA_KEYS = new Set([
  'type', 'properties', 'required', 'additionalProperties',
  'description', 'title', '$schema', 'definitions', '$defs',
  'allOf', 'anyOf', 'oneOf', 'not', 'if', 'then', 'else',
  'enum', 'const', 'default', 'examples',
]);

export function validateMcpToolSchema(schema: unknown): { valid: boolean; error?: string } {
  if (schema === null || schema === undefined) {
    return { valid: false, error: 'Schema is null or undefined' };
  }
  if (typeof schema !== 'object' || Array.isArray(schema)) {
    return { valid: false, error: 'Schema must be a non-array object' };
  }
  const s = schema as Record<string, unknown>;
  if (s.type !== 'object') {
    return { valid: false, error: `Schema root type must be 'object', got '${String(s.type)}'` };
  }
  if (s.properties !== undefined) {
    if (typeof s.properties !== 'object' || s.properties === null || Array.isArray(s.properties)) {
      return { valid: false, error: 'Schema properties must be a non-array object' };
    }
  }
  if (s.required !== undefined) {
    if (!Array.isArray(s.required)) {
      return { valid: false, error: 'Schema required must be an array' };
    }
    for (const item of s.required) {
      if (typeof item !== 'string') {
        return { valid: false, error: `Schema required items must be strings, got ${typeof item}` };
      }
    }
    if (s.properties !== undefined) {
      const props = s.properties as Record<string, unknown>;
      for (const key of s.required as string[]) {
        if (!(key in props)) {
          return { valid: false, error: `Required key '${key}' is not defined in properties` };
        }
      }
    }
  }
  return { valid: true };
}

export function sanitizeMcpToolSchema(schema: unknown): object {
  const validation = validateMcpToolSchema(schema);
  if (!validation.valid) {
    return { type: 'object', properties: {} };
  }
  const s = schema as Record<string, unknown>;
  const cleaned: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(s)) {
    if (ALLOWED_SCHEMA_KEYS.has(key)) {
      cleaned[key] = value;
    }
  }
  return cleaned;
}

/**
 * Expand `${VAR}`, `${VAR:-default}`, and bare `$VAR` references in a string.
 * Returns the expanded string plus the names of referenced variables that were
 * both undefined AND had no `:-default` (so callers can surface missing-var
 * diagnostics). An unresolved reference (undefined var with no `:-default`)
 * leaves the ORIGINAL token (`${VAR}` / `$VAR`) in place rather than collapsing
 * to '' (Claude-parity, envExpansion.ts:L30-31) so the broken placeholder stays
 * visible for debugging, in tandem with the missing-var diagnostic — the absence
 * of a value is then distinguishable from an intentionally-empty one. A
 * `${VAR:-default}` with VAR undefined expands to the default and is NOT reported
 * missing.
 */
export function expandEnvVarsTracked(value: string): { expanded: string; missingVars: string[] } {
  const missingVars: string[] = [];
  const expanded = value.replace(/\$\{([^}]+)\}|\$([A-Z_][A-Z0-9_]*)/gi, (match, braced, bare) => {
    if (bare !== undefined) {
      const v = process.env[bare];
      if (v !== undefined) return v;
      missingVars.push(bare);
      // Leave the original `$VAR` token in place (Claude-parity) so the unresolved
      // placeholder is visible rather than silently blanked.
      return match;
    }
    // Braced form may carry a `:-default`. Split on the FIRST `:-` so a default
    // value may itself contain `:-`.
    const sep = braced.indexOf(':-');
    const name = sep >= 0 ? braced.slice(0, sep) : braced;
    const def = sep >= 0 ? braced.slice(sep + 2) : undefined;
    const v = process.env[name];
    if (v !== undefined) return v;
    if (def !== undefined) return def;
    missingVars.push(name);
    // Leave the original `${VAR}` token in place (Claude-parity) so the unresolved
    // placeholder is visible rather than silently blanked.
    return match;
  });
  return { expanded, missingVars };
}

export function expandEnvVars(value: string): string {
  return expandEnvVarsTracked(value).expanded;
}

/** The plugin-scoped substitution variables resolved AHEAD of `${ENV}` expansion. */
export interface PluginMcpVars {
  /** `${user_config.<key>}` values declared by the plugin's userConfig schema. */
  userConfig?: Record<string, string>;
  /** `${CLAUDE_PLUGIN_ROOT}` — the plugin install dir. */
  pluginRoot?: string;
  /** `${CLAUDE_PLUGIN_DATA}` — the persistent per-plugin data dir. */
  pluginDataDir?: string;
}

/**
 * Substitute the plugin-scoped variable classes in a config string BEFORE the
 * general `${ENV}` tier runs (Claude-parity `resolvePluginMcpEnvironment`,
 * mcpPluginIntegration.ts:L475-490):
 *   - `${user_config.<key>}` → the user-entered value for `<key>` (empty string
 *     when the key is absent, so a stale placeholder never leaks a literal
 *     `${user_config.x}` onto a command line);
 *   - `${CLAUDE_PLUGIN_ROOT}` → `vars.pluginRoot`;
 *   - `${CLAUDE_PLUGIN_DATA}` → `vars.pluginDataDir`.
 * Any OTHER `${...}` reference is left untouched for {@link expandEnvVarsTracked}
 * to resolve, giving plugin/user values precedence over a same-named env var.
 *
 * On Windows the two plugin-PATH substitutions (`CLAUDE_PLUGIN_ROOT` /
 * `CLAUDE_PLUGIN_DATA`) have their backslashes normalized to forward slashes
 * (Claude-parity `substitutePluginVariables`, pluginOptionsStorage.ts:L312-345)
 * so a path like `C:\Users\me\plugin` dropped into a shell command/arg is not
 * mangled by backslash escape interpretation. `user_config` values are NOT
 * normalized — they are opaque user data, not plugin install paths.
 */
export function substitutePluginMcpVars(value: string, vars: PluginMcpVars): string {
  const normalizePath = (p: string): string =>
    process.platform === 'win32' ? p.replace(/\\/g, '/') : p;
  return value.replace(/\$\{(user_config\.[A-Za-z0-9_]+|CLAUDE_PLUGIN_ROOT|CLAUDE_PLUGIN_DATA)\}/g, (_match, ref: string) => {
    if (ref === 'CLAUDE_PLUGIN_ROOT') return normalizePath(vars.pluginRoot ?? '');
    if (ref === 'CLAUDE_PLUGIN_DATA') return normalizePath(vars.pluginDataDir ?? '');
    const key = ref.slice('user_config.'.length);
    return vars.userConfig?.[key] ?? '';
  });
}

/**
 * Scope a plugin/bundle's MCP server names so two different plugins can each ship
 * a server under the same local name (e.g. `github`) without one clobbering the
 * other (Claude-parity `addPluginScopeToServers`, mcpPluginIntegration.ts:L341-360).
 * Each server is renamed to `plugin:${pluginName}:${name}` and tagged with a
 * {@link McpServerConfig.pluginSource} provenance marker. Returns the scoped map
 * plus a `nameMap` (original → scoped) so the caller can carry the plugin's
 * `mcp-auto-allow-patterns` onto the scoped tool names via
 * {@link remapMcpAutoAllowPatterns}. The input map is not mutated (each config is
 * shallow-cloned).
 */
export function scopePluginMcpServers(
  servers: Record<string, McpServerConfig>,
  pluginName: string,
  pluginSource: string,
): { scoped: Record<string, McpServerConfig>; nameMap: Record<string, string> } {
  const scoped: Record<string, McpServerConfig> = {};
  const nameMap: Record<string, string> = {};
  for (const [name, config] of Object.entries(servers)) {
    const scopedName = `plugin:${pluginName}:${name}`;
    scoped[scopedName] = { ...config, pluginSource };
    nameMap[name] = scopedName;
  }
  return { scoped, nameMap };
}

/**
 * Rewrite a plugin's `mcp-auto-allow-patterns` so they still match the per-plugin
 * SCOPED tool names produced by {@link scopePluginMcpServers}. A pattern targets a
 * server via the `mcp__<server>__…` tool-name prefix (or the bare `mcp__<server>`
 * server-granularity form); after scoping, the server segment normalizes
 * differently ({@link normalizeMcpNameSegment} maps the `plugin:<name>:<server>`
 * colons to underscores), so a stale `mcp__github__*` would never match the scoped
 * `mcp__plugin_<name>_github__…`. Each pattern whose server prefix matches a
 * `nameMap` entry is rewritten onto the scoped prefix; every other pattern (and a
 * pattern that does not reference a scoped server) is returned unchanged.
 */
export function remapMcpAutoAllowPatterns(
  patterns: string[],
  nameMap: Record<string, string>,
): string[] {
  const prefixMap: Array<[string, string]> = Object.entries(nameMap).map(
    ([orig, scoped]) => [`mcp__${normalizeMcpNameSegment(orig)}`, `mcp__${normalizeMcpNameSegment(scoped)}`],
  );
  return patterns.map((pattern) => {
    for (const [oldPrefix, newPrefix] of prefixMap) {
      if (pattern.startsWith(oldPrefix + '__')) {
        return newPrefix + pattern.slice(oldPrefix.length);
      }
      if (pattern === oldPrefix || pattern.startsWith(oldPrefix + '*')) {
        return newPrefix + pattern.slice(oldPrefix.length);
      }
    }
    return pattern;
  });
}

/**
 * Strip a leading UTF-8 byte-order mark (U+FEFF) from a string before it is
 * handed to `JSON.parse` (Claude-parity `stripBOM`, utils/json.ts +
 * remoteManagedSettings/syncCacheState.ts). PowerShell 5.x (and some Windows
 * editors) write UTF-8 files WITH a leading BOM, so an `.mcp.json` generated /
 * hand-edited there begins with an invisible `﻿` that makes `JSON.parse`
 * throw ("Unexpected token in JSON at position 0") and silently drops EVERY
 * server the file declared. Removing the BOM first makes such a config load
 * normally. A string without a BOM is returned unchanged (cheap char-code check).
 */
export function stripJsonBom(raw: string): string {
  return raw.charCodeAt(0) === 0xfeff ? raw.slice(1) : raw;
}

/**
 * Deterministic content hash of a raw MCP server config entry, used by
 * {@link McpManager.reloadFromConfig} to detect whether a server's config changed
 * across a config-file reload (Claude-parity config-hash diff). Object keys are
 * sorted recursively so two logically-equal configs that differ only in JSON key
 * order hash identically (a reload of an unchanged-but-reformatted file does not
 * needlessly tear a server down and respawn it).
 */
function hashMcpServerEntry(entry: unknown): string {
  return createHash('sha256').update(stableStringifyForHash(entry)).digest('hex');
}

/** Stable JSON serialization with recursively sorted object keys (helper for
 *  {@link hashMcpServerEntry}). */
function stableStringifyForHash(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value) ?? 'null';
  if (Array.isArray(value)) return '[' + value.map(stableStringifyForHash).join(',') + ']';
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  return '{' + keys.map((k) => JSON.stringify(k) + ':' + stableStringifyForHash(obj[k])).join(',') + '}';
}

/**
 * Discover MCP config files for `cwd`, each tagged with the {@link McpServerScope}
 * it belongs to, in first-wins precedence order. Config files under cwd (the
 * shareable `.ami/mcp.json` and `.mcp.json`, walked up to the FS root) are the
 * `project` scope; the user-global `~/.ami/mcp.json` is the `user` scope. This is
 * the canonical discovery; {@link findMcpConfigPaths} is its paths-only projection.
 */
export function findMcpConfigScopes(cwd: string): Array<{ path: string; scope: McpServerScope }> {
  const entries: Array<{ path: string; scope: McpServerScope }> = [];
  const homeDir = process.env.HOME ?? process.env.USERPROFILE ?? '';

  // Precedence is first-wins (addServer rejects a duplicate name, so the first
  // config to register a server keeps it). Within a scope AMI's own config beats
  // the Claude Code interop one; project scope beats user scope.
  //
  // We intentionally interop only at PROJECT scope (the shareable `.mcp.json` a
  // repo commits), NOT user-global ~/.claude.json: that file aggregates every
  // Claude Code server a developer ever configured (and a large per-project
  // tree), so auto-loading it would silently spin up unrelated servers. Users
  // who want global servers in AMI put them in ~/.ami/mcp.json.

  // 0. AMI local (machine-private, uncommitted `.ami/mcp.local.json` in cwd) — a
  //    per-user, NOT-committed project override (Claude-parity `local` ConfigScope):
  //    add a server for THIS repo on THIS machine only. Highest precedence
  //    (first-wins) so it overrides a committed project entry of the same name, and
  //    is NEVER approval-gated (the user authored it themselves, like the user scope),
  //    since the engine only treats `scope === 'project'` as untrusted/approval-gated.
  const localConfig = path.join(cwd, '.ami', 'mcp.local.json');
  if (fs.existsSync(localConfig)) entries.push({ path: localConfig, scope: 'local' });

  // 1. AMI project (.ami/mcp.json in cwd).
  const projectConfig = path.join(cwd, '.ami', 'mcp.json');
  if (fs.existsSync(projectConfig)) entries.push({ path: projectConfig, scope: 'project' });

  // 2. Claude Code project interop — the shareable `.mcp.json`. Walk from cwd UP
  //    to the filesystem root collecting every `.mcp.json` (closer-to-cwd first),
  //    so a monorepo-root config is still found when AMI is launched from a nested
  //    subdirectory (Claude-parity). first-wins precedence means the nearest file's
  //    servers take priority over an ancestor's.
  let dir = path.resolve(cwd);
  while (true) {
    const candidate = path.join(dir, '.mcp.json');
    if (fs.existsSync(candidate)) entries.push({ path: candidate, scope: 'project' });
    const parent = path.dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }

  // 3. AMI user-global (~/.ami/mcp.json).
  const globalConfig = path.join(homeDir, '.ami', 'mcp.json');
  if (fs.existsSync(globalConfig)) entries.push({ path: globalConfig, scope: 'user' });

  return entries;
}

export function findMcpConfigPaths(cwd: string): string[] {
  return findMcpConfigScopes(cwd).map((e) => e.path);
}

/**
 * Env var (and its value) injected into a spawned stdio MCP server's child process
 * when the host session runs in a bypass / auto-allow permission mode, so a server
 * with its own permission layer can skip its prompts and stay non-interactive.
 * Mirrors Claude Code's `CLAUDE_CHROME_PERMISSION_MODE='skip_all_permission_checks'`
 * injection; the value is kept identical for cross-framework interop with servers
 * that already recognize that sentinel. See {@link McpManager.setBypassPermissions}.
 */
export const MCP_BYPASS_PERMISSION_ENV = 'MCP_PERMISSION_MODE';
export const MCP_BYPASS_PERMISSION_VALUE = 'skip_all_permission_checks';

/**
 * Resolve a non-colliding MCP server name against a set of already-used NORMALIZED
 * names (Claude-parity numbered-suffix dedup, claudeai.ts:L92-L119). Tries the base
 * name first, then appends ` (2)`, ` (3)`, … until the FINAL normalized name
 * (suffix included) is unused — the subtle edge case is that the SUFFIXED name is
 * what gets normalized+checked, so a suffix that itself collides with another
 * server's base name is still caught. Returns the chosen display `name` and its
 * `normalized` form; normalization is {@link normalizeMcpNameSegment} (the same
 * transform the qualified `mcp__…` tool name uses), so two servers can never
 * produce the same model-facing tool prefix. Does NOT mutate `usedNormalizedNames`
 * — the caller adds the returned `normalized` before resolving the next name.
 */
export function resolveMcpServerNameCollision(
  baseName: string,
  usedNormalizedNames: Set<string>,
): { name: string; normalized: string } {
  let name = baseName;
  let normalized = normalizeMcpNameSegment(name);
  let count = 1;
  while (usedNormalizedNames.has(normalized)) {
    count++;
    name = `${baseName} (${count})`;
    normalized = normalizeMcpNameSegment(name);
  }
  return { name, normalized };
}

/**
 * Injectable platform / filesystem seams for desktop-config import (see
 * {@link resolveDesktopMcpConfigPath} / {@link readDesktopMcpServers}). Mirrors the
 * ambient inputs Claude's `getClaudeDesktopConfigPath` reads (platform, USERPROFILE,
 * home dir, file/dir existence), split out so the WSL path-resolution branches are
 * testable on any CI host without a real Windows mount.
 */
export interface DesktopImportEnv {
  /** `process.platform` — only `darwin` (macOS) and `linux`+WSL are supported. */
  platform: NodeJS.Platform;
  /** True when running under WSL (a Windows filesystem is reachable at /mnt/c). */
  isWsl: boolean;
  /** Environment (read for `USERPROFILE` on WSL). */
  env: NodeJS.ProcessEnv;
  /** The current user's home directory (macOS config lives under it). */
  homedir: string;
  /** True when a path exists (defaults to `fs.existsSync`). */
  exists: (p: string) => boolean;
  /** Directory entry names (defaults to `fs.readdirSync`); may throw → treated as empty. */
  readdir: (p: string) => string[];
}

/** True when the current process is running inside WSL (Windows Subsystem for
 *  Linux): WSL sets `WSL_DISTRO_NAME`, and the kernel release string carries a
 *  `microsoft`/`WSL` marker. Used to decide whether a Windows desktop config under
 *  `/mnt/c` is reachable. */
function detectWsl(): boolean {
  if (process.env.WSL_DISTRO_NAME || process.env.WSL_INTEROP) return true;
  try {
    return /microsoft|wsl/i.test(os.release());
  } catch {
    return false;
  }
}

/** Build the default {@link DesktopImportEnv} from the real process + filesystem. */
function defaultDesktopImportEnv(): DesktopImportEnv {
  return {
    platform: process.platform,
    isWsl: detectWsl(),
    env: process.env,
    homedir: os.homedir(),
    exists: (p) => fs.existsSync(p),
    readdir: (p) => fs.readdirSync(p),
  };
}

/** The Windows directory names that are not real user profiles (skipped when
 *  scanning /mnt/c/Users for a desktop config). Mirrors Claude's skip list. */
const WSL_NON_USER_DIRS = new Set(['Public', 'Default', 'Default User', 'All Users']);

/**
 * Resolve the path to a desktop MCP client's `claude_desktop_config.json`
 * (Claude-parity `getClaudeDesktopConfigPath`), or `null` when the platform is
 * unsupported or no config file can be located:
 *  - macOS → `~/Library/Application Support/Claude/claude_desktop_config.json`;
 *  - WSL → first `${USERPROFILE}` (backslashes→slashes, drive-letter stripped) mapped
 *    to `/mnt/c<path>/AppData/Roaming/Claude/…` if it exists, else a scan of
 *    `/mnt/c/Users/<user>/AppData/Roaming/Claude/…` skipping system profiles;
 *  - every other platform → `null` (Claude throws; AMI returns null so an importer
 *    can simply report "nothing to import").
 */
export function resolveDesktopMcpConfigPath(e: DesktopImportEnv = defaultDesktopImportEnv()): string | null {
  if (e.platform === 'darwin') {
    return path.join(e.homedir, 'Library', 'Application Support', 'Claude', 'claude_desktop_config.json');
  }
  if (e.platform === 'linux' && e.isWsl) {
    const profile = e.env.USERPROFILE ? e.env.USERPROFILE.replace(/\\/g, '/') : null;
    if (profile) {
      const wslPath = profile.replace(/^[A-Za-z]:/, '');
      const candidate = `/mnt/c${wslPath}/AppData/Roaming/Claude/claude_desktop_config.json`;
      if (e.exists(candidate)) return candidate;
    }
    let names: string[] = [];
    try { names = e.readdir('/mnt/c/Users'); } catch { names = []; }
    for (const user of names) {
      if (WSL_NON_USER_DIRS.has(user)) continue;
      const candidate = path.join('/mnt/c/Users', user, 'AppData', 'Roaming', 'Claude', 'claude_desktop_config.json');
      if (e.exists(candidate)) return candidate;
    }
  }
  return null;
}

/**
 * Read + validate the MCP servers declared in a desktop client's
 * `claude_desktop_config.json` (Claude-parity `readClaudeDesktopMcpServers`), as a
 * `{ name: config }` map. Every value goes through the same
 * {@link normalizeMcpServerEntry}/{@link validateMcpServerConfigShape} guard used for
 * a config file (a malformed entry is skipped per-entry, not fatal), and only stdio
 * servers (those with a `command`) are returned — the desktop app only persists
 * stdio servers. A missing / unreadable / malformed file, or an unsupported
 * platform, yields `{}` (never throws). The fs/platform seams are injectable for
 * testing.
 */
export function readDesktopMcpServers(opts?: {
  env?: DesktopImportEnv;
  readFile?: (configPath: string) => string;
}): Record<string, McpServerConfig> {
  const env = opts?.env ?? defaultDesktopImportEnv();
  const readFile = opts?.readFile ?? ((p: string) => fs.readFileSync(p, 'utf-8'));
  const configPath = resolveDesktopMcpConfigPath(env);
  if (!configPath) return {};
  let raw: string;
  try {
    raw = readFile(configPath);
  } catch {
    return {};
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(stripJsonBom(raw).replace(/^\s*\/\/.*$/gm, ''));
  } catch {
    return {};
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {};
  const mcpServers = (parsed as Record<string, unknown>).mcpServers;
  if (!mcpServers || typeof mcpServers !== 'object' || Array.isArray(mcpServers)) return {};
  const out: Record<string, McpServerConfig> = {};
  for (const [name, serverConfig] of Object.entries(mcpServers as Record<string, unknown>)) {
    const normalized = normalizeMcpServerEntry(serverConfig);
    if (validateMcpServerConfigShape(normalized).length > 0) continue;
    const cfg = normalized as McpServerConfig;
    // The desktop app persists stdio servers (a `command`); a remote entry with no
    // command is not a desktop-importable server.
    if (!cfg.command) continue;
    out[name] = cfg;
  }
  return out;
}
