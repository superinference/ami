// Split from manager.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { McpClientState } from './client';
import { resolveEnvSecretRefs, resolveKeychainSecretRefs, headersHaveKeychainSecretRef, describeSecretRef, defaultMcpSecretStore, type McpSecretStore } from './secret-store';
import { log as coreLog } from '../logger';
import { defaultHeadersHelperRunner } from './manager-mcp-server-config';
import type { HeadersHelperLogHooks, HeadersHelperRunner, McpServerConfig, McpServerState, McpServerStateSummary, McpStateSummaryPart } from './manager-mcp-server-config';

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

export function resolveEnvVars(headers: Record<string, string>): Record<string, string> {
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
 * Format a {@link McpServerStateSummary} into the ordered, non-empty parts a status
 * pane renders as a single compact "N connected, N need auth, N pending, N failed
 * · /mcp" row (the reference-parity `buildMcpProperties`, status.tsx:L99-115). Only
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

/**
 * The reserved MCP server name for the connected code editor (the reference-parity:
 * `client.name === 'ide'`, useIdeConnectionStatus.ts). The one server whose
 * connection drives the editor-integration badge; special-cased by
 * {@link McpManager.getIdeConnectionStatus}.
 */
export const IDE_MCP_SERVER_NAME = 'ide';

/**
 * Canonicalize a transport discriminator to the BASE transport its connection is
 * actually driven over (the reference-parity IDE discriminants): `sse-ide`→`sse`,
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
 * being an IDE discriminant (the reference-parity, useIdeConnectionStatus.ts:L20-24:
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
 * the editor extension that launched AMI (the reference-parity `CLAUDE_CODE_SSE_PORT`).
 * The extension passes the port through the environment because terminal
 * multiplexers (tmux/screen) clobber `TERM_PROGRAM`, so the editor can no longer
 * be detected from the terminal alone once inside a multiplexer.
 */
export const MCP_IDE_SSE_PORT_ENV = 'AMI_SSE_PORT';

/**
 * Env var that force-ENABLES (a truthy value) or force-DISABLES (an explicit
 * falsy value) IDE MCP auto-connect, overriding every other waterfall input
 * (the reference-parity `CLAUDE_CODE_AUTO_CONNECT_IDE`). A falsy value is a hard opt-out
 * that wins over the config knob, the flag, and a supported terminal.
 */
export const MCP_IDE_AUTOCONNECT_ENV = 'AMI_AUTO_CONNECT_IDE';

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
 * Resolve the code-editor's MCP SSE endpoint URL from the inherited
 * {@link MCP_IDE_SSE_PORT_ENV} port, or `null` when the port is unset / not a
 * valid 1..65535 TCP port. The editor extension serves MCP over a loopback SSE
 * endpoint; AMI connects to `http://127.0.0.1:<port>/sse` (the reference-parity local
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
 * (the reference-parity `DetectedIDEInfo`, utils/ide.ts): the editor's display `name`
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
 * maps it back to the base wire transport. Divergence from the reference
 * (ide.tsx:L585, which tests only `startsWith('ws:')` and so mis-tags a `wss://`
 * endpoint as `sse-ide`): AMI also recognizes `wss://` so a secure WebSocket IDE
 * connects over its real transport.
 */
export function ideTransportForUrl(url: string): 'sse-ide' | 'ws-ide' {
  return url.startsWith('ws:') || url.startsWith('wss:') ? 'ws-ide' : 'sse-ide';
}

/**
 * How long {@link McpManager.connectIde} waits for the reserved `ide` server to
 * reach a terminal state before reporting a `timeout` (the reference-parity
 * `IDE_CONNECTION_TIMEOUT_MS`, ide.tsx:L507). The connect itself has its own
 * bounded retry/backoff; this is the outer ceiling the UI shows a result by.
 */
export const IDE_CONNECTION_TIMEOUT_MS = 35000;

/**
 * Terminal outcome of an interactive IDE connect ({@link McpManager.connectIde}):
 * `connected` (handshake succeeded), `failed` (handshake rejected), or `timeout`
 * (no terminal state within {@link IDE_CONNECTION_TIMEOUT_MS}). `message` is the
 * ready-to-display sentence, matching the reference's IDECommandFlow wording.
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
 * (the reference-parity `IdeStatus`, useIdeConnectionStatus.ts): `null` means no editor
 * is connected (no `ide` server at all). Present-but-not-connected buckets as
 * `connected` (server `ready`), `pending` (still connecting), or `disconnected`
 * (any other state — error/stopped/needs-auth/disabled).
 */
export type IdeStatus = 'connected' | 'pending' | 'disconnected' | null;

/**
 * Map a live MCP server state onto the editor-badge tri-state (the reference-parity
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

/**
 * Canonicalize an MCP tool name for verb classification: camelCase and kebab-case
 * reduce to a single snake_case key (the reference-parity `normalize()`), so a read/search
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
 * Normalize a raw MCP server config entry: map a reference-style `type`
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
 * Validate a user-supplied MCP SERVER name before it is persisted (the reference-parity
 * `addMcpConfig` name check, services/mcp/config.ts:L630-L679). A server name becomes
 * part of the model-facing qualified tool namespace (`mcp__<server>__<tool>`) and
 * AMI's own `serverName:toolName` call resolution splits on the first `:`, so a name
 * containing a space, colon, dot, slash, or any other non-`[A-Za-z0-9_-]` character
 * would corrupt qualified-tool resolution or produce an ambiguous tool name. Returns
 * a human-readable error message for an empty or ill-formed name, else `null` (valid).
 * Enforced at `/mcp add` time (not config load) so a hand-edited config stays lenient,
 * exactly like the reference's add-time-only validation.
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
 * is surfaced via {@link McpManager.getConfigWarnings}. Mirrors the reference's
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

/**
 * Expand `${VAR}`, `${VAR:-default}`, and bare `$VAR` references in a string.
 * Returns the expanded string plus the names of referenced variables that were
 * both undefined AND had no `:-default` (so callers can surface missing-var
 * diagnostics). An unresolved reference (undefined var with no `:-default`)
 * leaves the ORIGINAL token (`${VAR}` / `$VAR`) in place rather than collapsing
 * to '' (the reference-parity, envExpansion.ts:L30-31) so the broken placeholder stays
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
      // Leave the original `$VAR` token in place (the reference-parity) so the unresolved
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
    // Leave the original `${VAR}` token in place (the reference-parity) so the unresolved
    // placeholder is visible rather than silently blanked.
    return match;
  });
  return { expanded, missingVars };
}

export function expandEnvVars(value: string): string {
  return expandEnvVarsTracked(value).expanded;
}
