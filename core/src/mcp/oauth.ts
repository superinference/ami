/**
 * oauth.ts — MCP 2025-06-18 OAuth 2.1 client support for the HTTP transport.
 *
 * Implements the protocol pieces a resource-server client needs:
 *   - PKCE (RFC 7636)                      — generatePkce()
 *   - WWW-Authenticate parsing             — parseWwwAuthenticate()
 *   - Protected Resource Metadata (RFC 9728) — discoverProtectedResource()
 *   - Authorization Server Metadata (RFC 8414) — discoverAuthServer()
 *   - Dynamic Client Registration (RFC 7591) — registerClient()
 *   - Authorization URL w/ Resource Indicator (RFC 8707) — buildAuthorizationUrl()
 *   - Code exchange & refresh               — exchangeCode() / refreshToken()
 *   - In-memory token store with expiry     — OAuthTokenStore
 *
 * All network I/O goes through an injected `fetch` so every function is unit
 * testable without a live server. The interactive browser-consent leg (opening a
 * URL + capturing the redirect) is the one piece that cannot run headless; it is
 * provided separately by the frontend and is out of this module's scope — this
 * module gives the frontend everything needed to drive it.
 */
import { createHash, randomBytes } from 'crypto';
import * as fs from 'fs';
import * as path from 'path';

export type FetchLike = (url: string, init?: {
  method?: string;
  headers?: Record<string, string>;
  body?: string;
  signal?: AbortSignal;
}) => Promise<{ ok: boolean; status: number; json: () => Promise<unknown>; headers?: { get(name: string): string | null } }>;

/** Default per-request timeout for OAuth HTTP calls (discovery / DCR / token). */
export const OAUTH_HTTP_TIMEOUT_MS = 30_000;

/**
 * Default overall timeout (ms) for the INTERACTIVE consent leg of the authorization
 * flow. The per-request timeout ({@link OAUTH_HTTP_TIMEOUT_MS}) bounds each HTTP
 * call, but the browser-consent leg is an out-of-band human step with no HTTP
 * request to time out — if the user never completes it (closes the tab, loses the
 * redirect) the `authorize()` promise would hang forever. This 5-minute cap
 * (parity with Claude's `Authentication timeout`) tears down a stuck consent leg.
 */
export const OAUTH_FLOW_TIMEOUT_MS = 5 * 60 * 1000;

/**
 * Thrown when the interactive consent leg exceeds {@link OAUTH_FLOW_TIMEOUT_MS}
 * (or a configured `flowTimeoutMs`). Distinct from a user cancellation
 * (signal-abort) and from a generic flow failure so a frontend can attribute a
 * hung consent leg precisely rather than rendering an opaque error.
 */
export class OAuthFlowTimeoutError extends Error {
  constructor(readonly ms: number) {
    super(`OAuth authorization flow timed out after ${ms}ms (interactive consent not completed)`);
    this.name = 'OAuthFlowTimeoutError';
  }
}

/**
 * Fallback access-token lifetime (seconds) when a token response omits
 * `expires_in` AND the access token is not a JWT carrying an `exp` claim. Without
 * this a tokenless-of-expiry grant would be treated as never-expiring and only a
 * reactive 401 would ever refresh it; a sane default keeps such tokens on a
 * proactive refresh/expiry schedule (parity with Claude's 3600s default).
 */
export const DEFAULT_TOKEN_LIFETIME_SEC = 3600;

/**
 * Env var (Claude-parity) overriding the Client ID Metadata Document URL used for
 * CIMD (SEP-991). When set, it takes precedence over any configured
 * `clientMetadataUrl`, so an operator can point the flow at a testing / FedStart
 * metadata document without editing server config.
 */
export const MCP_OAUTH_CLIENT_METADATA_URL_ENV = 'MCP_OAUTH_CLIENT_METADATA_URL';

/**
 * Resolve the Client ID Metadata Document URL (CIMD / SEP-991) for an OAuth flow:
 * the `MCP_OAUTH_CLIENT_METADATA_URL` env override wins, else the operator-configured
 * `clientMetadataUrl`, else undefined (CIMD disabled — fall back to Dynamic Client
 * Registration). Unlike Claude Code, AMI ships NO hardcoded default document (that
 * would point at a vendor's infrastructure); CIMD is strictly opt-in. A resolved URL
 * MUST be https:// — a `client_id` that is a URL is a public identifier the auth
 * server dereferences, so a plaintext one is refused.
 */
export function resolveClientMetadataUrl(
  configured?: string,
  env: NodeJS.ProcessEnv = process.env,
): string | undefined {
  const raw = env[MCP_OAUTH_CLIENT_METADATA_URL_ENV] || configured;
  if (!raw) return undefined;
  if (!raw.startsWith('https://')) {
    throw new Error(`client-id metadata document URL must use https:// (got: ${raw})`);
  }
  return raw;
}

/**
 * Transport-downgrade guard. OAuth carries bearer tokens, authorization codes
 * and client secrets, so every issuer/discovery/authorization/token endpoint
 * MUST be https. Plaintext http is tolerated ONLY for loopback (local dev auth
 * servers). Throws otherwise.
 */
export function assertSecureUrl(url: string, label = 'OAuth endpoint'): void {
  let u: URL;
  try { u = new URL(url); } catch { throw new Error(`${label} is not a valid URL: ${url}`); }
  if (u.protocol === 'https:') return;
  const host = u.hostname.replace(/^\[/, '').replace(/\]$/, '');
  const loopback = host === 'localhost' || host === '127.0.0.1' || host === '::1';
  if (u.protocol === 'http:' && loopback) return;
  throw new Error(`${label} must use https:// — refusing plaintext ${u.protocol}// (loopback-only exception) for ${url}`);
}

/** A structured OAuth/token error with the server-reported error code. */
export class OAuthError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly code?: string,
    readonly description?: string,
    /** RFC 6749 §5.2 `error_uri` — a human-readable page describing the error. */
    readonly uri?: string,
  ) {
    super(message);
    this.name = 'OAuthError';
  }
}

/**
 * Extract the RFC 6749/8414 standard error envelope (`error`, `error_description`,
 * `error_uri`) from a parsed JSON response body and render it as a message suffix
 * so a failure surfaces the server's own explanation, not just an HTTP status.
 * Returns the parts (for building an {@link OAuthError}) and a `suffix` string.
 */
export function extractOAuthErrorBody(doc: Record<string, unknown>): {
  code?: string; description?: string; uri?: string; suffix: string;
} {
  const code = typeof doc.error === 'string' ? doc.error : undefined;
  const description = typeof doc.error_description === 'string' ? doc.error_description : undefined;
  const uri = typeof doc.error_uri === 'string' ? doc.error_uri : undefined;
  let suffix = '';
  if (code) {
    suffix = `: ${code}`;
    if (description) suffix += ` (${description})`;
    if (uri) suffix += ` — see ${uri}`;
  } else if (description) {
    suffix = `: ${description}`;
  }
  return { code, description, uri, suffix };
}

/**
 * Non-standard error codes some servers return that mean the grant is dead. RFC
 * 6749 specifies `invalid_grant`, but real servers (notably Slack's token-rotation
 * endpoints) emit aliases — `invalid_refresh_token`, `expired_refresh_token`,
 * `token_expired`, `expired_token` — that must map onto the canonical code so the
 * dead-grant invalidation path fires instead of treating them as transient.
 */
const INVALID_GRANT_ALIASES = new Set([
  'invalid_grant', 'invalid_refresh_token', 'expired_refresh_token', 'expired_token', 'token_expired',
]);

/**
 * Build an OAuthError from a token-endpoint response, normalising both the
 * HTTP-200-with-error-body shape (some non-compliant servers) and non-standard
 * `invalid_grant` aliases so callers can reason about one canonical code.
 */
export function normalizeTokenError(status: number, doc: Record<string, unknown>): OAuthError {
  const { code: rawCode, description, uri, suffix } = extractOAuthErrorBody(doc);
  const code = rawCode && INVALID_GRANT_ALIASES.has(rawCode) ? 'invalid_grant' : rawCode;
  return new OAuthError(`token endpoint error (${status})${suffix}`, status, code, description, uri);
}

/** Run `promise`, rejecting if it does not settle within `ms` (0 = no limit). */
export function withTimeout<T>(factory: (signal: AbortSignal | undefined) => Promise<T>, ms: number, label: string): Promise<T> {
  if (!ms || ms <= 0) return factory(undefined);
  const controller = new AbortController();
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => { controller.abort(); reject(new Error(`${label} timed out after ${ms}ms`)); }, ms);
    if (typeof (timer as { unref?: () => void }).unref === 'function') (timer as { unref: () => void }).unref();
    factory(controller.signal).then(
      (v) => { clearTimeout(timer); resolve(v); },
      (e) => { clearTimeout(timer); reject(e); },
    );
  });
}

/**
 * Compose multiple (possibly undefined) AbortSignals into one that aborts as soon
 * as ANY input aborts. Returns the lone signal when only one is present, or
 * undefined when none are — so the common single-signal/no-signal paths add no
 * overhead. Used to carry BOTH a caller's cancellation signal and a per-request
 * timeout signal into the same fetch so either can abort an in-flight OAuth call.
 */
export function combineAbortSignals(...signals: Array<AbortSignal | undefined>): AbortSignal | undefined {
  const present = signals.filter((s): s is AbortSignal => s !== undefined);
  if (present.length === 0) return undefined;
  if (present.length === 1) return present[0];
  return AbortSignal.any(present);
}

/**
 * Wrap a fetch impl so every call is bounded by a per-request timeout. Any signal
 * the caller already passed in `init.signal` (e.g. a user cancellation) is
 * COMBINED with the timeout signal rather than overriding it, so a cancel and a
 * timeout both still abort the request.
 */
export function wrapFetchWithTimeout(fetchImpl: FetchLike, ms: number): FetchLike {
  return (url, init) => withTimeout(
    (signal) => fetchImpl(url, { ...init, signal: combineAbortSignals(init?.signal, signal) }),
    ms,
    `OAuth request to ${url}`,
  );
}

export interface RetryOptions {
  retries?: number;
  baseMs?: number;
  isRetryable?: (error: unknown) => boolean;
  sleep?: (ms: number) => Promise<void>;
  /**
   * Observed once per scheduled retry, BEFORE the backoff sleep — carries the
   * 1-based attempt number, the computed backoff delay (ms), and the error that
   * triggered the retry. Lets a caller surface a progress/affordance hint for a
   * transient failure (e.g. an LSP ContentModified retry) without changing the
   * retry mechanics. The final (non-retried) failure does NOT fire this.
   */
  onRetry?: (info: { attempt: number; delayMs: number; error: unknown }) => void;
}

/** Retry `fn` with exponential backoff on transient (retryable) failures. */
export async function retryWithBackoff<T>(fn: () => Promise<T>, opts: RetryOptions = {}): Promise<T> {
  const retries = opts.retries ?? 2;
  const baseMs = opts.baseMs ?? 200;
  const sleep = opts.sleep ?? ((ms) => new Promise((r) => { const t = setTimeout(r, ms); if (typeof (t as { unref?: () => void }).unref === 'function') (t as { unref: () => void }).unref(); }));
  let lastError: unknown;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      if (attempt === retries) break;
      if (opts.isRetryable && !opts.isRetryable(error)) break;
      const delayMs = baseMs * 2 ** attempt;
      opts.onRetry?.({ attempt: attempt + 1, delayMs, error });
      await sleep(delayMs);
    }
  }
  throw lastError;
}

/** A transient token-refresh failure worth retrying (network / 5xx), NOT a dead grant. */
export function isTransientOAuthError(error: unknown): boolean {
  if (error instanceof OAuthError) return error.status >= 500 || error.status === 429;
  return true; // network-level throw (no HTTP status) — retry
}

/**
 * OAuth query parameters that must be redacted before a URL is logged: `state`
 * and `nonce` are CSRF/session-fixation secrets, `code_challenge`/`code_verifier`
 * are PKCE secrets, and `code` is the single-use authorization code. Leaking any
 * of these into a debug log would enable replay / CSRF attacks.
 */
export const SENSITIVE_OAUTH_PARAMS = ['state', 'nonce', 'code_challenge', 'code_verifier', 'code'] as const;

/**
 * Redact sensitive OAuth query parameters ({@link SENSITIVE_OAUTH_PARAMS}) from a
 * URL so an authorization/callback URL can be written to a DIAGNOSTIC LOG safely. A
 * value that is not a valid URL is returned unchanged (nothing to redact).
 *
 * This is deliberately DISTINCT from `tools/mcp-auth.ts`'s `redactAuthUrl`, which
 * redacts only credential VALUES for a URL SURFACED TO THE USER/MODEL and must
 * KEEP `state`/`code_challenge` so the user can still complete consent. A log line
 * has no such constraint, so this stricter variant also scrubs the CSRF/PKCE
 * params (`state`/`nonce`/`code_challenge`/`code_verifier`) — the two cannot be one
 * function because their param policies conflict.
 */
export function redactOAuthUrl(url: string): string {
  let u: URL;
  try { u = new URL(url); } catch { return url; }
  for (const param of SENSITIVE_OAUTH_PARAMS) {
    if (u.searchParams.has(param)) u.searchParams.set(param, '[REDACTED]');
  }
  return u.toString();
}

/**
 * Nesting-depth-agnostic redaction of token-bearing JSON keys from an error body
 * before it reaches a debug log. Matches the quoted value of any of
 * `access_token`/`refresh_token`/`id_token`/`assertion`/`subject_token`/
 * `client_secret` at ANY depth, operating on both a raw `text()` body and a
 * parsed-then-stringified object — so a misbehaving auth server that echoes a
 * request's credential back inside a 4xx envelope cannot leak it into logs.
 */
const SENSITIVE_TOKEN_RE = /"(access_token|refresh_token|id_token|assertion|subject_token|client_secret)"\s*:\s*"[^"]*"/g;

export function redactTokenBody(raw: unknown): string {
  const s = typeof raw === 'string' ? raw : JSON.stringify(raw);
  return (s ?? '').replace(SENSITIVE_TOKEN_RE, (_, k) => `"${k}":"[REDACTED]"`);
}

/**
 * One step in the multi-leg MCP OAuth flow, surfaced to an optional observer so a
 * frontend can show step-by-step progress and attribute a failure to the exact
 * leg that failed (discovery vs registration vs consent vs token exchange).
 */
export interface OAuthLogEvent {
  /** Which leg of the flow emitted this. */
  step:
    | 'cache-hit' | 'discover-prm' | 'discover-as' | 'register-client'
    | 'authorize' | 'exchange-code' | 'xaa-exchange' | 'refresh' | 'revoke' | 'cancelled' | 'token-stored';
  /** Human-readable, already-redacted message (URLs via {@link redactOAuthUrl}). */
  message: string;
  /** The MCP server URL this flow is for. */
  serverUrl: string;
  /** Present on a terminal leg: whether it succeeded. */
  ok?: boolean;
  /** Present on a failed leg: the redacted failure reason. */
  error?: string;
}

/** Observer for {@link OAuthLogEvent}s — a diagnostic/introspection seam for the frontend. */
export type OAuthLogger = (event: OAuthLogEvent) => void;

/**
 * Credential-store key for an MCP OAuth server, derived from the server NAME plus
 * a hash of its connection config (`type`/`url`/`headers`). Keying on the config
 * hash — not the name alone — prevents credentials being reused across two servers
 * that share a name but differ in URL/transport/headers, or across a reconfigured
 * server whose endpoint changed (the old grant no longer applies). Parity with
 * Claude Code's `getServerKey`.
 */
export function mcpCredentialKey(
  serverName: string,
  config: { type?: string; url?: string; headers?: Record<string, string> },
): string {
  const configJson = JSON.stringify({ type: config.type, url: config.url, headers: config.headers ?? {} });
  const hash = createHash('sha256').update(configJson).digest('hex').slice(0, 16);
  return `${serverName}|${hash}`;
}

/**
 * RFC 9728 §3.3 mix-up protection: the `resource` a protected-resource metadata
 * document advertises must identify the same server (origin) we requested it
 * from. A mismatch means the PRM is for a different resource server — reject it.
 */
export function assertResourceMatch(advertisedResource: string, serverUrl: string): void {
  let adv: URL;
  let srv: URL;
  try { adv = new URL(advertisedResource); } catch { return; /* non-absolute (fell back to metadata URL) — nothing to compare */ }
  try { srv = new URL(serverUrl); } catch { return; }
  if (adv.origin !== srv.origin) {
    throw new Error(`PRM resource mismatch (RFC 9728 §3.3): metadata advertises ${adv.origin} but the server is ${srv.origin}`);
  }
}

export interface PkcePair {
  verifier: string;
  challenge: string;
  method: 'S256';
}

export interface ProtectedResourceMetadata {
  resource: string;
  authorizationServers: string[];
  scopesSupported?: string[];
}

export interface AuthServerMetadata {
  issuer: string;
  authorizationEndpoint: string;
  tokenEndpoint: string;
  registrationEndpoint?: string;
  scopesSupported?: string[];
  /** Non-standard single `scope` string some providers advertise. */
  scope?: string;
  /** Non-standard `default_scope` string some providers advertise. */
  defaultScope?: string;
  codeChallengeMethodsSupported?: string[];
  /**
   * RFC 8414 `grant_types_supported`. When advertised, the client verifies the
   * authorization server can actually perform the `authorization_code` grant
   * before driving a consent flow it could never complete.
   */
  grantTypesSupported?: string[];
  /**
   * RFC 8414 `token_endpoint_auth_methods_supported`. Used to pick the
   * `token_endpoint_auth_method` a dynamically-registered client declares, so a
   * server that does not accept public (`none`) clients still gets a usable
   * registration (`client_secret_post` / `client_secret_basic`).
   */
  tokenEndpointAuthMethodsSupported?: string[];
  /**
   * RFC 7009 `revocation_endpoint`. When advertised, access/refresh tokens can be
   * invalidated server-side on sign-out / re-auth so a discarded or leaked token
   * cannot keep minting access past its natural expiry.
   */
  revocationEndpoint?: string;
  /** RFC 8414 `revocation_endpoint_auth_methods_supported` — client-auth methods the revocation endpoint accepts. */
  revocationEndpointAuthMethodsSupported?: string[];
  /**
   * SEP-991 `client_id_metadata_document_supported`. When true, the client MAY use
   * a Client ID Metadata Document URL directly as its `client_id` instead of
   * performing Dynamic Client Registration (RFC 7591). See {@link resolveClientMetadataUrl}.
   */
  clientIdMetadataDocumentSupported?: boolean;
}

export interface OAuthTokens {
  accessToken: string;
  tokenType: string;
  refreshToken?: string;
  /** Absolute epoch-ms expiry (computed from expires_in at receipt). */
  expiresAt?: number;
  scope?: string;
  /**
   * The `client_id` these tokens were minted under. Persisted so a session can
   * detect a configured-clientId CHANGE across restarts and invalidate the now-stale
   * grant (the old client's tokens/secret would fail with opaque `invalid_client`).
   */
  clientId?: string;
}

/**
 * Persisted authorization-server discovery result for one MCP server: the RFC 8707
 * `resource` the tokens are bound to plus the discovered RFC 8414 authorization-server
 * metadata. Caching this across sessions lets a cold process refresh an existing
 * (file-persisted) token WITHOUT first re-running the RFC 9728 → RFC 8414 discovery
 * chain — which otherwise requires provoking a fresh 401 before a refresh can even
 * reach the token endpoint. Parity with Claude Code's persisted `discoveryState`.
 */
export interface DiscoveryState {
  resource: string;
  authServer: AuthServerMetadata;
}

/**
 * A pluggable store for {@link DiscoveryState}, injected into {@link McpOAuthSession}
 * so discovery survives process restarts. All methods are best-effort (a backing
 * failure degrades to live re-discovery, never throws). {@link FileDiscoveryStateStore}
 * is the on-disk implementation; a test may inject an in-memory fake.
 */
export interface DiscoveryStateStore {
  load(): DiscoveryState | null;
  save(state: DiscoveryState): void;
  clear(): void;
}

/**
 * File-backed {@link DiscoveryStateStore}. Discovery metadata is NOT secret (it is
 * public well-known data), but it is written 0600 under a 0700 dir for consistency
 * with the token file it sits beside. All disk I/O is best-effort: a failed
 * read/write degrades to live re-discovery rather than throwing.
 */
export class FileDiscoveryStateStore implements DiscoveryStateStore {
  constructor(private readonly filePath: string) {}

  load(): DiscoveryState | null {
    try {
      const parsed = JSON.parse(fs.readFileSync(this.filePath, 'utf-8')) as DiscoveryState;
      if (parsed && typeof parsed.resource === 'string' && parsed.authServer
        && typeof parsed.authServer.issuer === 'string'
        && typeof parsed.authServer.authorizationEndpoint === 'string'
        && typeof parsed.authServer.tokenEndpoint === 'string') {
        return parsed;
      }
    } catch { /* no cached discovery (or unreadable) — fall back to live discovery */ }
    return null;
  }

  save(state: DiscoveryState): void {
    try {
      fs.mkdirSync(path.dirname(this.filePath), { recursive: true, mode: 0o700 });
      fs.writeFileSync(this.filePath, JSON.stringify(state), { mode: 0o600 });
    } catch { /* best-effort — in-memory discovery is still live for this session */ }
  }

  clear(): void {
    try { fs.rmSync(this.filePath, { force: true }); } catch { /* ignore */ }
  }
}

/**
 * Filesystem-safe discovery-state filename for a credential-store key, co-located
 * with the token file ({@link mcpTokenFileName}) but suffixed so the two never
 * collide. Pass a {@link mcpCredentialKey}.
 */
export function mcpDiscoveryFileName(credentialKey: string): string {
  const safe = credentialKey.replace(/[^a-zA-Z0-9_.-]/g, '_');
  const hash = createHash('sha256').update(credentialKey).digest('hex').slice(0, 8);
  return `${safe}.${hash}.discovery.json`;
}

/**
 * What the HTTP transport needs from an OAuth layer: the current Authorization
 * header (if any) to attach to a request, and a 401 handler that attempts to
 * (re)authorize and reports whether the request should be retried. Concrete
 * implementations compose the token store + flow functions above; tests inject a
 * fake. Keeping the transport behind this interface means client.ts stays free
 * of discovery/browser logic.
 */
export interface McpOAuthProvider {
  authorizationHeader(): Promise<string | null>;
  onUnauthorized(wwwAuthenticate: string | null): Promise<boolean>;
  /**
   * Step-up handler for a 403 `WWW-Authenticate: Bearer error="insufficient_scope",
   * scope="…"`: the current token authenticated fine but lacks a scope the resource
   * now requires. Drops the stale token (so a silent refresh can't re-mint the same
   * insufficient scope) and drives a full re-consent requesting the union of the
   * original and the newly-required scopes. Returns whether the request should be
   * retried. A 403 that is NOT an insufficient_scope challenge returns false.
   */
  onForbidden(wwwAuthenticate: string | null): Promise<boolean>;
  /**
   * User-initiated sign-out: revoke the issued tokens server-side (RFC 7009,
   * best-effort) and drop all local credential state. Optional — a provider that
   * cannot revoke simply omits it.
   */
  revoke?(): Promise<void>;
  /**
   * Local, granular credential invalidation (no server-side call): drop the named
   * slice of auth state (`tokens` / `client` / `discovery` / `verifier` / `all`).
   * Used on a full "forget" to tear down the dynamically-registered client and cached
   * discovery in addition to the tokens that revoke() already clears. Optional.
   */
  invalidateCredentials?(scope: 'all' | 'client' | 'tokens' | 'verifier' | 'discovery'): Promise<void>;
}

const base64url = (buf: Buffer): string =>
  buf.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

/** RFC 7636 PKCE pair (S256). randomness/hash come from node:crypto. */
export function generatePkce(): PkcePair {
  const verifier = base64url(randomBytes(32));
  const challenge = base64url(createHash('sha256').update(verifier).digest());
  return { verifier, challenge, method: 'S256' };
}

/** tmp */
export function generateState(): string {
  return base64url(randomBytes(32));
}

/**
 * Decode the `exp` claim (seconds since epoch) of an unverified JWT and return it
 * as absolute epoch-ms, or undefined when the token is not a three-part JWT, the
 * payload is not JSON, or it carries no numeric `exp`. The signature is NOT
 * verified — this is used solely as a local expiry hint so a JWT access token that
 * omits `expires_in` is still refreshed proactively rather than treated as
 * non-expiring. Trust in the token still derives from the server that issued it.
 */
export function decodeJwtExpiry(token: string): number | undefined {
  const parts = token.split('.');
  if (parts.length !== 3) return undefined;
  try {
    const json = Buffer.from(parts[1].replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf-8');
    const payload = JSON.parse(json) as Record<string, unknown>;
    return typeof payload.exp === 'number' ? payload.exp * 1000 : undefined;
  } catch {
    return undefined;
  }
}

/**
 * RFC 3986 §6.2.2 syntax normalization for OAuth identifier comparison: lowercase
 * the scheme and host, drop a default port, and strip a trailing slash. Used for
 * RFC 8414 §3.3 issuer mix-up protection so cosmetic URL differences do not cause
 * a false mismatch nor let a different origin masquerade as the expected one.
 */
export function normalizeOAuthUrl(url: string): string {
  const u = new URL(url);
  const port = u.port ? `:${u.port}` : '';
  const pathname = u.pathname.replace(/\/+$/, '');
  return `${u.protocol.toLowerCase()}//${u.hostname.toLowerCase()}${port}${pathname}`;
}

/**
 * RFC 8414 §3.3 mix-up protection: the `issuer` an authorization-server metadata
 * document advertises must be identical (after syntax normalization) to the issuer
 * URL the metadata was fetched from. A mismatch means the metadata endpoint is
 * self-describing as a different issuer — reject it so its authorization/token
 * endpoints are never trusted.
 */
export function assertIssuerMatch(advertisedIssuer: string, requestedIssuer: string): void {
  let adv: string;
  try { adv = normalizeOAuthUrl(advertisedIssuer); } catch {
    throw new Error(`authorization-server issuer is not a valid URL: ${advertisedIssuer}`);
  }
  if (adv !== normalizeOAuthUrl(requestedIssuer)) {
    throw new Error(`authorization-server issuer mismatch (RFC 8414 §3.3): metadata advertises ${advertisedIssuer} but was fetched from ${requestedIssuer}`);
  }
}

/**
 * Parse an HTTP 401 `WWW-Authenticate: Bearer ...` header. Returns the
 * resource-metadata URL (RFC 9728) and any error/realm fields it carries.
 */
export function parseWwwAuthenticate(header: string | null | undefined): {
  resourceMetadata?: string;
  realm?: string;
  error?: string;
  /** Space-delimited `scope` auth-param (RFC 6750 §3) — the scope the resource
   *  requires, surfaced on a 403 `insufficient_scope` for step-up re-consent. */
  scope?: string;
} {
  if (!header) return {};
  const out: { resourceMetadata?: string; realm?: string; error?: string; scope?: string } = {};
  // Match key="value" (quoted) OR key=token (unquoted) pairs — RFC 7235 auth-param
  // list; RFC 6750 permits an unquoted `scope` token.
  const re = /(\w[\w-]*)\s*=\s*(?:"([^"]*)"|([^\s,]+))/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(header)) !== null) {
    const key = m[1].toLowerCase();
    const value = m[2] ?? m[3] ?? '';
    if (key === 'resource_metadata') out.resourceMetadata = value;
    else if (key === 'realm') out.realm = value;
    else if (key === 'error') out.error = value;
    else if (key === 'scope') out.scope = value;
  }
  return out;
}

/**
 * Union two space-delimited OAuth scope strings, preserving declared order and
 * dropping duplicates/blanks. Used for 403 step-up: the newly-required scope is
 * merged into the originally-requested scope so re-consent covers both. Returns
 * undefined when the union is empty (so no `scope` param is sent).
 */
export function unionScopes(...scopes: Array<string | undefined>): string | undefined {
  const seen = new Set<string>();
  for (const s of scopes) {
    for (const tok of (s ?? '').split(/\s+/)) {
      if (tok) seen.add(tok);
    }
  }
  return seen.size ? [...seen].join(' ') : undefined;
}

/**
 * Resolve a requested scope string from authorization-server metadata, covering
 * providers that advertise it in different fields. Tries the non-standard `scope`,
 * then the non-standard `default_scope`, then the standard `scopes_supported`
 * (space-joined). Returns undefined when none is present.
 */
export function resolveScopeFromMetadata(meta: AuthServerMetadata | null | undefined): string | undefined {
  if (!meta) return undefined;
  if (typeof meta.scope === 'string' && meta.scope) return meta.scope;
  if (typeof meta.defaultScope === 'string' && meta.defaultScope) return meta.defaultScope;
  if (meta.scopesSupported && meta.scopesSupported.length) return meta.scopesSupported.join(' ');
  return undefined;
}

/**
 * Build a `.well-known` discovery URL (RFC 8414 §3.1 / RFC 9728 §3.1). The
 * `.well-known/<suffix>` segment is INSERTED between the host and any issuer
 * path component rather than replacing the path, so an issuer with a path
 * (`https://host/tenant1`) discovers at `https://host/.well-known/<suffix>/tenant1`.
 * A pathless issuer is unchanged (`https://host/.well-known/<suffix>`).
 */
/**
 * True when a URL carries a non-empty path component (ignoring a trailing slash).
 * Used to gate the path-aware RFC 8414 fallback: a root URL's well-known probe is
 * identical to the primary one, so only a path-bearing server URL warrants a retry.
 */
export function hasUrlPath(url: string): boolean {
  try { return new URL(url).pathname.replace(/\/+$/, '') !== ''; } catch { return false; }
}

export function wellKnown(baseUrl: string, suffix: string): string {
  const u = new URL(baseUrl);
  const base = `${u.protocol}//${u.host}`;
  const path = u.pathname.replace(/\/+$/, '');
  return path && path !== '/' ? `${base}/.well-known/${suffix}${path}` : `${base}/.well-known/${suffix}`;
}

async function getJson(fetchImpl: FetchLike, url: string): Promise<Record<string, unknown>> {
  const res = await fetchImpl(url, { headers: { Accept: 'application/json' } });
  if (!res.ok) {
    // RFC 8414/9728 error responses carry a JSON `{error, error_description,
    // error_uri}` body; surface it so the failure explains WHY (e.g. an AS that
    // rejects the discovery with `invalid_request`), not just the HTTP status.
    let parts: ReturnType<typeof extractOAuthErrorBody> = { suffix: '' };
    try { parts = extractOAuthErrorBody((await res.json()) as Record<string, unknown>); } catch { /* no/opaque body */ }
    throw new OAuthError(
      `OAuth discovery failed (${res.status}) for ${url}${parts.suffix}`,
      res.status, parts.code, parts.description, parts.uri,
    );
  }
  // A captive portal or intercepting proxy answers 200 with an HTML interstitial;
  // parsing it as JSON throws an opaque SyntaxError mid-auth. Diagnose it so the
  // user knows the NETWORK — not the auth server — is the problem.
  try {
    return (await res.json()) as Record<string, unknown>;
  } catch {
    throw new Error(`OAuth discovery returned a non-JSON response from ${url} (captive portal or proxy?)`);
  }
}

/**
 * Pick the `token_endpoint_auth_method` a dynamically-registered client should
 * declare, given the server's advertised `token_endpoint_auth_methods_supported`.
 * AMI registers public (PKCE) clients, so `none` is preferred; but a server that
 * omits `none` from its list would reject such a registration, so fall back to a
 * confidential method it does advertise. With no advertised list, default to `none`.
 */
export function pickRegistrationAuthMethod(supported?: string[]): string {
  if (!supported || supported.length === 0) return 'none';
  if (supported.includes('none')) return 'none';
  if (supported.includes('client_secret_post')) return 'client_secret_post';
  if (supported.includes('client_secret_basic')) return 'client_secret_basic';
  return 'none';
}

/** RFC 9728 — fetch the resource server's protected-resource metadata. */
export async function discoverProtectedResource(
  fetchImpl: FetchLike,
  metadataUrl: string,
): Promise<ProtectedResourceMetadata> {
  const doc = await getJson(fetchImpl, metadataUrl);
  const servers = doc.authorization_servers;
  if (!Array.isArray(servers) || servers.length === 0) {
    throw new Error('protected-resource metadata has no authorization_servers');
  }
  return {
    resource: typeof doc.resource === 'string' ? doc.resource : metadataUrl,
    authorizationServers: servers as string[],
    scopesSupported: Array.isArray(doc.scopes_supported) ? (doc.scopes_supported as string[]) : undefined,
  };
}

/**
 * RFC 8414 — fetch authorization-server metadata.
 *
 * By default the metadata is discovered at the issuer's well-known URL. When
 * `explicitMetadataUrl` is supplied (a server-pinned `authServerMetadataUrl`), it
 * is fetched directly instead — supporting servers whose AS metadata lives at a
 * non-conventional URL. A pinned URL MUST be https:// (no loopback exception: it
 * is an operator-configured value, not a discovered one) and, being explicitly
 * trusted, bypasses the RFC 8414 §3.3 issuer mix-up check (the operator asserts
 * the mapping). Discovered metadata still enforces the issuer-match guard.
 */
export async function discoverAuthServer(
  fetchImpl: FetchLike,
  issuer: string,
  explicitMetadataUrl?: string,
): Promise<AuthServerMetadata> {
  if (explicitMetadataUrl !== undefined && !explicitMetadataUrl.startsWith('https://')) {
    throw new Error(`authServerMetadataUrl must use https:// (got: ${explicitMetadataUrl})`);
  }
  const doc = await getJson(fetchImpl, explicitMetadataUrl ?? wellKnown(issuer, 'oauth-authorization-server'));
  const authorizationEndpoint = doc.authorization_endpoint;
  const tokenEndpoint = doc.token_endpoint;
  if (typeof authorizationEndpoint !== 'string' || typeof tokenEndpoint !== 'string') {
    throw new Error('authorization-server metadata missing endpoints');
  }
  // Transport-downgrade guard: refuse plaintext endpoints discovered from
  // metadata before we ever POST credentials or redirect for consent.
  assertSecureUrl(authorizationEndpoint, 'authorization endpoint');
  assertSecureUrl(tokenEndpoint, 'token endpoint');
  // RFC 8414 §3.3 mix-up protection: a DISCOVERED metadata document must not
  // self-describe as a different issuer than the one we fetched it from. A pinned
  // metadata URL is operator-trusted, so the check is skipped for it.
  if (explicitMetadataUrl === undefined && typeof doc.issuer === 'string') assertIssuerMatch(doc.issuer, issuer);
  return {
    issuer: typeof doc.issuer === 'string' ? doc.issuer : issuer,
    authorizationEndpoint,
    tokenEndpoint,
    registrationEndpoint: typeof doc.registration_endpoint === 'string' ? doc.registration_endpoint : undefined,
    scopesSupported: Array.isArray(doc.scopes_supported) ? (doc.scopes_supported as string[]) : undefined,
    scope: typeof doc.scope === 'string' ? doc.scope : undefined,
    defaultScope: typeof doc.default_scope === 'string' ? doc.default_scope : undefined,
    codeChallengeMethodsSupported: Array.isArray(doc.code_challenge_methods_supported)
      ? (doc.code_challenge_methods_supported as string[]) : undefined,
    grantTypesSupported: Array.isArray(doc.grant_types_supported)
      ? (doc.grant_types_supported as string[]) : undefined,
    tokenEndpointAuthMethodsSupported: Array.isArray(doc.token_endpoint_auth_methods_supported)
      ? (doc.token_endpoint_auth_methods_supported as string[]) : undefined,
    revocationEndpoint: typeof doc.revocation_endpoint === 'string' ? doc.revocation_endpoint : undefined,
    revocationEndpointAuthMethodsSupported: Array.isArray(doc.revocation_endpoint_auth_methods_supported)
      ? (doc.revocation_endpoint_auth_methods_supported as string[]) : undefined,
    clientIdMetadataDocumentSupported: doc.client_id_metadata_document_supported === true,
  };
}

/** RFC 7591 — register a client dynamically; returns the issued client_id. */
export async function registerClient(
  fetchImpl: FetchLike,
  registrationEndpoint: string,
  redirectUris: string[],
  clientName = 'SuperInference AMI',
  supportedAuthMethods?: string[],
): Promise<{ clientId: string; clientSecret?: string }> {
  const res = await fetchImpl(registrationEndpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify({
      client_name: clientName,
      redirect_uris: redirectUris,
      grant_types: ['authorization_code', 'refresh_token'],
      response_types: ['code'],
      // Honor the server's advertised token_endpoint_auth_methods_supported: a
      // server that does not accept public (`none`) clients still gets a usable
      // registration instead of a rejected one.
      token_endpoint_auth_method: pickRegistrationAuthMethod(supportedAuthMethods),
    }),
  });
  if (!res.ok) {
    let parts: ReturnType<typeof extractOAuthErrorBody> = { suffix: '' };
    try { parts = extractOAuthErrorBody((await res.json()) as Record<string, unknown>); } catch { /* no/opaque body */ }
    throw new OAuthError(
      `dynamic client registration failed (${res.status})${parts.suffix}`,
      res.status, parts.code, parts.description, parts.uri,
    );
  }
  const doc = (await res.json()) as Record<string, unknown>;
  if (typeof doc.client_id !== 'string') throw new Error('registration response missing client_id');
  return { clientId: doc.client_id, clientSecret: typeof doc.client_secret === 'string' ? doc.client_secret : undefined };
}

/** Build the authorization-code request URL (PKCE + RFC 8707 resource indicator). */
export function buildAuthorizationUrl(params: {
  authorizationEndpoint: string;
  clientId: string;
  redirectUri: string;
  resource: string;
  codeChallenge: string;
  state: string;
  scope?: string;
  /** Optional OIDC `login_hint` to pre-fill the user identity on the consent screen. */
  loginHint?: string;
}): string {
  const u = new URL(params.authorizationEndpoint);
  u.searchParams.set('response_type', 'code');
  u.searchParams.set('client_id', params.clientId);
  u.searchParams.set('redirect_uri', params.redirectUri);
  u.searchParams.set('code_challenge', params.codeChallenge);
  u.searchParams.set('code_challenge_method', 'S256');
  u.searchParams.set('state', params.state);
  u.searchParams.set('resource', params.resource); // RFC 8707
  if (params.scope) u.searchParams.set('scope', params.scope);
  // Standard OIDC parameter: pre-populate the identity on the provider's login
  // form so the consent screen is not a cold start.
  if (params.loginHint) u.searchParams.set('login_hint', params.loginHint);
  return u.toString();
}

/**
 * Coerce a token response's `expires_in` to a finite number of seconds, tolerating
 * the numeric-string form some non-conformant IdPs send (`"3600"`). Returns
 * undefined for a missing / empty / non-numeric value so the caller can fall back
 * to the JWT `exp` claim or the default lifetime.
 */
export function coerceExpiresIn(raw: unknown): number | undefined {
  if (typeof raw === 'number') return Number.isFinite(raw) ? raw : undefined;
  if (typeof raw === 'string' && raw.trim() !== '') {
    const n = Number(raw);
    if (Number.isFinite(n)) return n;
  }
  return undefined;
}

export function toTokens(doc: Record<string, unknown>, now: number): OAuthTokens {
  if (typeof doc.access_token !== 'string') throw new Error('token response missing access_token');
  // Tolerate a numeric-string `expires_in` (common from PHP-backed IdPs) the same
  // way z.coerce.number() would: a bare number is used as-is, a finite numeric
  // string is coerced, anything else (missing / empty / non-numeric) is dropped so
  // expiry falls back to the JWT `exp` claim or DEFAULT_TOKEN_LIFETIME_SEC rather
  // than being treated as never-expiring.
  const expiresIn = coerceExpiresIn(doc.expires_in);
  // Expiry resolution, in precedence order:
  //   1. the server-supplied `expires_in`;
  //   2. else the access token's own JWT `exp` claim (a JWT that omits expires_in
  //      is still treated as expiring rather than valid forever);
  //   3. else a DEFAULT_TOKEN_LIFETIME_SEC fallback so an opaque token lacking
  //      both is still proactively refreshed on a sane schedule instead of being
  //      treated as never-expiring (only a reactive 401 would recover it).
  const expiresAt = expiresIn !== undefined
    ? now + expiresIn * 1000
    : (decodeJwtExpiry(doc.access_token) ?? now + DEFAULT_TOKEN_LIFETIME_SEC * 1000);
  return {
    accessToken: doc.access_token,
    tokenType: typeof doc.token_type === 'string' ? doc.token_type : 'Bearer',
    refreshToken: typeof doc.refresh_token === 'string' ? doc.refresh_token : undefined,
    expiresAt,
    scope: typeof doc.scope === 'string' ? doc.scope : undefined,
  };
}

async function postForm(
  fetchImpl: FetchLike,
  tokenEndpoint: string,
  form: Record<string, string>,
  now: number,
): Promise<OAuthTokens> {
  const res = await fetchImpl(tokenEndpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', Accept: 'application/json' },
    body: new URLSearchParams(form).toString(),
  });
  // Some non-compliant servers return HTTP 200 with an `error` body, so inspect
  // the payload even on a 2xx and normalise the code before deciding.
  let doc: Record<string, unknown> = {};
  let parseFailed = false;
  try { doc = (await res.json()) as Record<string, unknown>; } catch { parseFailed = true; }
  if (!res.ok || typeof doc.error === 'string') throw normalizeTokenError(res.status, doc);
  // A 2xx with a body that did not parse as JSON is almost always a captive
  // portal / intercepting proxy answering with HTML — surface that instead of an
  // opaque "missing access_token".
  if (parseFailed) {
    throw new Error(`OAuth token endpoint returned a non-JSON response from ${tokenEndpoint} (captive portal or proxy?)`);
  }
  return toTokens(doc, now);
}

/** Exchange an authorization code for tokens (PKCE + resource indicator).
 *  A `clientSecret` is included for confidential clients (RFC 6749 §4.1.3). */
export function exchangeCode(
  fetchImpl: FetchLike,
  params: { tokenEndpoint: string; code: string; verifier: string; clientId: string; redirectUri: string; resource: string; clientSecret?: string },
  now: number,
): Promise<OAuthTokens> {
  const form: Record<string, string> = {
    grant_type: 'authorization_code',
    code: params.code,
    code_verifier: params.verifier,
    client_id: params.clientId,
    redirect_uri: params.redirectUri,
    resource: params.resource,
  };
  if (params.clientSecret) form.client_secret = params.clientSecret;
  return postForm(fetchImpl, params.tokenEndpoint, form, now);
}

/** Exchange a refresh token for fresh tokens.
 *  A `clientSecret` is included for confidential clients.
 *  An optional `scope` is sent to request a narrower (or server-permitting
 *  broader) scope set on the refreshed grant (RFC 6749 §6); some authorization
 *  servers otherwise drop previously-granted scopes on refresh. */
export function refreshToken(
  fetchImpl: FetchLike,
  params: { tokenEndpoint: string; refreshToken: string; clientId: string; resource: string; clientSecret?: string; scope?: string },
  now: number,
): Promise<OAuthTokens> {
  const form: Record<string, string> = {
    grant_type: 'refresh_token',
    refresh_token: params.refreshToken,
    client_id: params.clientId,
    resource: params.resource,
  };
  if (params.clientSecret) form.client_secret = params.clientSecret;
  if (params.scope) form.scope = params.scope;
  return postForm(fetchImpl, params.tokenEndpoint, form, now);
}

/** Client-auth method a confidential client uses at the token/revocation endpoint. */
export type ClientAuthMethod = 'client_secret_basic' | 'client_secret_post' | 'none';

/**
 * Pick the client-authentication method for a confidential request (RFC 7009
 * revocation, RFC 6749 token), given the endpoint's advertised
 * `*_auth_methods_supported` and whether a client secret is available. Prefers
 * `client_secret_basic` (RFC 7009 default), then `client_secret_post`; falls back
 * to `none` when there is no secret. With no advertised list, defaults to basic
 * when a secret exists (the most widely accepted) else none.
 */
export function pickClientAuthMethod(supported: string[] | undefined, hasSecret: boolean): ClientAuthMethod {
  if (!hasSecret) return 'none';
  if (!supported || supported.length === 0) return 'client_secret_basic';
  if (supported.includes('client_secret_basic')) return 'client_secret_basic';
  if (supported.includes('client_secret_post')) return 'client_secret_post';
  return 'client_secret_basic';
}

/**
 * Environment variable supplying a confidential-client secret for MCP OAuth
 * (parity with Claude Code's `readClientSecret()` env precedence — auth.ts
 * `readClientSecret`). A secret provided here is read at connect time and NEVER
 * written into the plaintext server config, so it does not land in argv/`ps`,
 * shell history, or `.ami/mcp.json`.
 */
export const MCP_CLIENT_SECRET_ENV = 'MCP_CLIENT_SECRET';

/**
 * Resolve the effective confidential-client secret for an MCP OAuth session.
 *
 * Precedence (Claude-parity `readClientSecret`): an explicitly-configured secret
 * wins (it may itself be a `${secret:…}`/`${VAR}` reference the caller already
 * resolved); otherwise fall back to the {@link MCP_CLIENT_SECRET_ENV} environment
 * variable so a secret can be supplied via the environment instead of being
 * inlined into the plaintext server config. A blank / whitespace-only value at
 * either source is treated as absent (public client — no secret). Returns
 * `undefined` when neither source provides one.
 *
 * Unlike Claude's helper this never prompts on a TTY: connect-time resolution is
 * non-interactive by design (the interactive masked prompt belongs to the
 * add-server command layer). The env fallback is the parity-relevant,
 * platform-free part and is the single source used by {@link McpOAuthSession}.
 */
export function resolveClientSecret(
  configured: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
): string | undefined {
  if (typeof configured === 'string' && configured.trim() !== '') return configured;
  const envSecret = env[MCP_CLIENT_SECRET_ENV];
  if (typeof envSecret === 'string' && envSecret.trim() !== '') return envSecret;
  return undefined;
}

/**
 * RFC 7009 token revocation. Revokes a single token (access or refresh) at the
 * authorization server's `revocation_endpoint`. Client authentication follows
 * `authMethod`: `client_secret_basic` sends an `Authorization: Basic` header,
 * `client_secret_post` puts the credentials in the body, `none` sends only the
 * `client_id`. A compliant server answers 200 even for an already-invalid token;
 * a 401 from a non-RFC-7009 server triggers ONE retry authenticating with the
 * token itself as a `Bearer` credential (some servers gate revocation that way).
 * Returns true when the server accepted the revocation. Never throws on an HTTP
 * error — revocation is best-effort — but a network throw propagates to the caller.
 */
export async function revokeToken(
  fetchImpl: FetchLike,
  params: {
    revocationEndpoint: string;
    token: string;
    tokenTypeHint?: 'access_token' | 'refresh_token';
    clientId?: string;
    clientSecret?: string;
    authMethod?: ClientAuthMethod;
  },
): Promise<boolean> {
  assertSecureUrl(params.revocationEndpoint, 'revocation endpoint');
  const method = params.authMethod ?? pickClientAuthMethod(undefined, !!params.clientSecret);
  const form: Record<string, string> = { token: params.token };
  if (params.tokenTypeHint) form.token_type_hint = params.tokenTypeHint;
  if (params.clientId) form.client_id = params.clientId;
  const headers: Record<string, string> = {
    'Content-Type': 'application/x-www-form-urlencoded',
    Accept: 'application/json',
  };
  if (method === 'client_secret_post' && params.clientSecret) {
    form.client_secret = params.clientSecret;
  } else if (method === 'client_secret_basic' && params.clientSecret && params.clientId) {
    headers.Authorization = `Basic ${Buffer.from(`${params.clientId}:${params.clientSecret}`).toString('base64')}`;
  }
  const res = await fetchImpl(params.revocationEndpoint, {
    method: 'POST', headers, body: new URLSearchParams(form).toString(),
  });
  if (res.ok) return true;
  // Non-RFC-7009 fallback: a server that returns 401 may gate revocation behind
  // the bearer token itself rather than client auth. Retry once that way.
  if (res.status === 401) {
    const bearer = await fetchImpl(params.revocationEndpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Accept: 'application/json',
        Authorization: `Bearer ${params.token}`,
      },
      body: new URLSearchParams({ token: params.token, ...(params.tokenTypeHint ? { token_type_hint: params.tokenTypeHint } : {}) }).toString(),
    });
    return bearer.ok;
  }
  return false;
}

// ─── Cross-App Access (XAA / SEP-990) — browserless enterprise SSO ───────────
//
// Obtains an MCP access token WITHOUT an interactive browser consent screen by
// chaining two OAuth grants, for enterprise/headless environments where the user
// already holds an OIDC id_token from a shared corporate IdP:
//   1. RFC 8693 Token Exchange at the IdP:  id_token → ID-JAG assertion
//   2. RFC 7523 JWT-Bearer Grant at the AS: ID-JAG   → access_token
// (ID-JAG = Identity Assertion Authorization Grant; MCP ext-auth / SEP-990.)
// This is the browserless alternative to McpOAuthSession's interactive consent
// leg — it reuses the SAME RFC 9728/8414 discovery + token helpers above, so there
// is one canonical discovery/token path, not a parallel one.

/** RFC 8693 token-exchange grant type (id_token → ID-JAG at the IdP). */
export const TOKEN_EXCHANGE_GRANT = 'urn:ietf:params:oauth:grant-type:token-exchange';
/** RFC 7523 JWT-bearer grant type (ID-JAG → access_token at the AS). */
export const JWT_BEARER_GRANT = 'urn:ietf:params:oauth:grant-type:jwt-bearer';
/** ID-JAG token type (the assertion minted by the IdP token-exchange). */
export const ID_JAG_TOKEN_TYPE = 'urn:ietf:params:oauth:token-type:id-jag';
/** OIDC id_token token type (the subject token fed into the token-exchange). */
export const ID_TOKEN_TYPE = 'urn:ietf:params:oauth:token-type:id_token';

/**
 * Env var gating the Cross-App Access (XAA / SEP-990) browserless SSO flow. XAA is
 * an ENTERPRISE feature that bypasses the interactive consent screen using a shared
 * corporate IdP, so — matching Claude Code's `CLAUDE_CODE_ENABLE_XAA` gate — it is
 * OFF unless an operator explicitly opts in. Unlike Claude, AMI uses a vendor-neutral
 * name.
 */
export const MCP_ENABLE_XAA_ENV = 'MCP_ENABLE_XAA';

/** Truthy values accepted by {@link isXaaEnabled} (parity with Claude's env-truthy check). */
function isEnvTruthy(v: string | undefined): boolean {
  if (!v) return false;
  const s = v.trim().toLowerCase();
  return s === '1' || s === 'true' || s === 'yes' || s === 'on';
}

/**
 * Whether the browserless Cross-App Access flow is enabled for this process
 * ({@link MCP_ENABLE_XAA_ENV} is set to a truthy value). A session configured with
 * an {@link XaaConfig} still only takes the XAA path when this gate is on, so the
 * enterprise flow never activates for a normal user who merely has stale config.
 */
export function isXaaEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env[MCP_ENABLE_XAA_ENV]);
}

/**
 * Input shape for the browserless Cross-App Access (XAA / SEP-990) token exchange.
 * Mirrors Claude Code's `XaaConfig`: the credentials registered at the MCP server's
 * authorization server (`clientId`/`clientSecret`) PLUS the credentials + pre-obtained
 * OIDC id_token for the shared enterprise IdP the token-exchange runs against.
 */
export interface XaaConfig {
  /** Client ID registered at the MCP server's authorization server (AS). */
  clientId: string;
  /** Client secret for the MCP server's authorization server (AS). */
  clientSecret: string;
  /** Client ID registered at the enterprise IdP (for the token-exchange request). */
  idpClientId: string;
  /** Optional IdP client secret (`client_secret_post`) — some IdPs require it even for `none`. */
  idpClientSecret?: string;
  /** The user's pre-obtained OIDC id_token from the IdP login (direct injection — no browser). */
  idpIdToken: string;
  /** The enterprise IdP token endpoint the RFC 8693 token-exchange is POSTed to. */
  idpTokenEndpoint: string;
  /** Optional scope requested on both exchange legs (RFC 8693 / RFC 7523). */
  scope?: string;
}

/** Result of a successful {@link requestJwtAuthorizationGrant} (RFC 8693 token exchange). */
export interface JwtAuthGrantResult {
  /** The ID-JAG (Identity Assertion Authorization Grant) assertion. */
  jwtAuthGrant: string;
  expiresIn?: number;
  scope?: string;
}

/**
 * RFC 8693 Token Exchange at the enterprise IdP: `id_token` → ID-JAG. The IdP mints
 * an assertion scoped to the MCP resource server (`audience`/`resource`) that the AS
 * then trades for an access token via {@link exchangeJwtAuthGrant}. A `clientSecret`
 * is sent via `client_secret_post` when present. Validates the returned
 * `issued_token_type` is the ID-JAG type so a mis-issued token is never forwarded.
 * Error bodies are surfaced via {@link extractOAuthErrorBody} and redacted.
 */
export async function requestJwtAuthorizationGrant(
  fetchImpl: FetchLike,
  opts: {
    tokenEndpoint: string;
    audience: string;
    resource: string;
    idToken: string;
    clientId: string;
    clientSecret?: string;
    scope?: string;
  },
): Promise<JwtAuthGrantResult> {
  assertSecureUrl(opts.tokenEndpoint, 'IdP token endpoint');
  const form: Record<string, string> = {
    grant_type: TOKEN_EXCHANGE_GRANT,
    requested_token_type: ID_JAG_TOKEN_TYPE,
    audience: opts.audience,
    resource: opts.resource,
    subject_token: opts.idToken,
    subject_token_type: ID_TOKEN_TYPE,
    client_id: opts.clientId,
  };
  if (opts.clientSecret) form.client_secret = opts.clientSecret;
  if (opts.scope) form.scope = opts.scope;
  const res = await fetchImpl(opts.tokenEndpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', Accept: 'application/json' },
    body: new URLSearchParams(form).toString(),
  });
  let doc: Record<string, unknown> = {};
  let parseFailed = false;
  try { doc = (await res.json()) as Record<string, unknown>; } catch { parseFailed = true; }
  if (!res.ok || typeof doc.error === 'string') throw normalizeTokenError(res.status, doc);
  if (parseFailed) {
    throw new Error(`XAA token exchange returned a non-JSON response from ${opts.tokenEndpoint} (captive portal or proxy?)`);
  }
  if (typeof doc.access_token !== 'string') {
    throw new Error('XAA token-exchange response missing access_token (ID-JAG)');
  }
  if (doc.issued_token_type !== ID_JAG_TOKEN_TYPE) {
    throw new Error(`XAA token-exchange returned unexpected issued_token_type: ${String(doc.issued_token_type)}`);
  }
  return {
    jwtAuthGrant: doc.access_token,
    expiresIn: coerceExpiresIn(doc.expires_in),
    scope: typeof doc.scope === 'string' ? doc.scope : undefined,
  };
}

/**
 * RFC 7523 JWT-Bearer Grant at the MCP server's authorization server: ID-JAG →
 * access token. `authMethod` defaults to `client_secret_basic` (credentials in an
 * `Authorization: Basic` header, per the SEP-990 conformance profile); pass
 * `client_secret_post` only when the AS requires body credentials. Returns the same
 * {@link OAuthTokens} shape the browser flow produces, so downstream handling is identical.
 */
export async function exchangeJwtAuthGrant(
  fetchImpl: FetchLike,
  opts: {
    tokenEndpoint: string;
    assertion: string;
    clientId: string;
    clientSecret: string;
    authMethod?: 'client_secret_basic' | 'client_secret_post';
    scope?: string;
  },
  now: number,
): Promise<OAuthTokens> {
  assertSecureUrl(opts.tokenEndpoint, 'token endpoint');
  const authMethod = opts.authMethod ?? 'client_secret_basic';
  const form: Record<string, string> = { grant_type: JWT_BEARER_GRANT, assertion: opts.assertion };
  if (opts.scope) form.scope = opts.scope;
  const headers: Record<string, string> = {
    'Content-Type': 'application/x-www-form-urlencoded',
    Accept: 'application/json',
  };
  if (authMethod === 'client_secret_basic') {
    headers.Authorization = `Basic ${Buffer.from(`${encodeURIComponent(opts.clientId)}:${encodeURIComponent(opts.clientSecret)}`).toString('base64')}`;
  } else {
    form.client_id = opts.clientId;
    form.client_secret = opts.clientSecret;
  }
  const res = await fetchImpl(opts.tokenEndpoint, {
    method: 'POST', headers, body: new URLSearchParams(form).toString(),
  });
  let doc: Record<string, unknown> = {};
  let parseFailed = false;
  try { doc = (await res.json()) as Record<string, unknown>; } catch { parseFailed = true; }
  if (!res.ok || typeof doc.error === 'string') throw normalizeTokenError(res.status, doc);
  if (parseFailed) {
    throw new Error(`XAA jwt-bearer grant returned a non-JSON response from ${opts.tokenEndpoint} (captive portal or proxy?)`);
  }
  return toTokens(doc, now);
}

/** Result of a full {@link performCrossAppAccess} flow. */
export interface XaaResult {
  tokens: OAuthTokens;
  /** The RFC 8707 resource (MCP server) the tokens are bound to. */
  resource: string;
  /** The AS issuer the tokens were minted at (persist for refresh/revocation). */
  authorizationServerUrl: string;
  /** The discovered AS metadata — carried so a later silent refresh/revocation can reuse it. */
  authServer: AuthServerMetadata;
}

/**
 * Full browserless XAA flow for one MCP server: RFC 9728 PRM discovery → RFC 8414 AS
 * metadata → RFC 8693 token-exchange at the IdP → RFC 7523 jwt-bearer at the AS →
 * access token. Reuses {@link discoverProtectedResource}/{@link discoverAuthServer}
 * (the same discovery the interactive flow uses) and tries each advertised AS in
 * order, skipping one whose metadata discovery fails or that explicitly omits the
 * jwt-bearer grant — so a down/incapable primary AS does not sink the flow when a
 * usable alternative is listed.
 */
export async function performCrossAppAccess(
  fetchImpl: FetchLike,
  serverUrl: string,
  config: XaaConfig,
  now: number,
): Promise<XaaResult> {
  const prm = await discoverProtectedResource(fetchImpl, wellKnown(serverUrl, 'oauth-protected-resource'));
  assertResourceMatch(prm.resource, serverUrl);
  const resource = prm.resource;
  let asMeta: AuthServerMetadata | undefined;
  const asErrors: string[] = [];
  for (const issuer of prm.authorizationServers) {
    try {
      assertSecureUrl(issuer, 'authorization server issuer');
      const meta = await discoverAuthServer(fetchImpl, issuer);
      // RFC 8414 §2: grant_types_supported is OPTIONAL — only skip an AS that
      // EXPLICITLY advertises a list omitting jwt-bearer; otherwise let the token
      // endpoint decide.
      if (meta.grantTypesSupported && !meta.grantTypesSupported.includes(JWT_BEARER_GRANT)) {
        asErrors.push(`${issuer}: does not advertise the jwt-bearer grant`);
        continue;
      }
      asMeta = meta;
      break;
    } catch (e) {
      asErrors.push(`${issuer}: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
  if (!asMeta) {
    throw new Error(`XAA: no authorization server supports jwt-bearer (tried: ${asErrors.join('; ')})`);
  }
  // 1) IdP token-exchange: id_token → ID-JAG (audience/resource = the MCP server).
  const grant = await requestJwtAuthorizationGrant(fetchImpl, {
    tokenEndpoint: config.idpTokenEndpoint,
    audience: asMeta.issuer,
    resource,
    idToken: config.idpIdToken,
    clientId: config.idpClientId,
    clientSecret: config.idpClientSecret,
    scope: config.scope,
  });
  // 2) AS jwt-bearer grant: ID-JAG → access token. Honour the AS's advertised
  // token_endpoint_auth_methods when picking basic vs post.
  const authMethod = asMeta.tokenEndpointAuthMethodsSupported?.includes('client_secret_post')
    && !asMeta.tokenEndpointAuthMethodsSupported.includes('client_secret_basic')
    ? 'client_secret_post' : 'client_secret_basic';
  const tokens = await exchangeJwtAuthGrant(fetchImpl, {
    tokenEndpoint: asMeta.tokenEndpoint,
    assertion: grant.jwtAuthGrant,
    clientId: config.clientId,
    clientSecret: config.clientSecret,
    authMethod,
    scope: grant.scope ?? config.scope,
  }, now);
  return { tokens, resource, authorizationServerUrl: asMeta.issuer, authServer: asMeta };
}

// ─── Proactive pre-expiry token-refresh scheduler ────────────────────────────

/** Default pre-expiry window: refresh 5 min before a token's real expiry. */
export const TOKEN_REFRESH_BUFFER_MS = 5 * 60 * 1000;
/** Follow-up refresh cadence when a token's own expiry is unknown/already past. */
export const FALLBACK_REFRESH_INTERVAL_MS = 30 * 60 * 1000;
/** Delay before retrying a refresh that produced no usable token. */
export const REFRESH_RETRY_DELAY_MS = 60 * 1000;
/** Max consecutive failed refreshes before the scheduler stops retrying. */
export const MAX_REFRESH_FAILURES = 5;
/** Floor for any scheduled delay so a near-expiry token never tight-loops. */
const MIN_REFRESH_DELAY_MS = 1000;

/** Injectable timer seams for {@link OAuthRefreshScheduler} (deterministic tests). */
export interface RefreshSchedulerTimers {
  setTimer?: (fn: () => void, ms: number) => unknown;
  clearTimer?: (handle: unknown) => void;
  now?: () => number;
}

/**
 * Single-target proactive token-refresh scheduler (parity with Claude Code's
 * `createTokenRefreshScheduler`, scoped to one MCP server). It fires an injected
 * `onRefresh` shortly BEFORE a token expires so a long-running session never
 * presents an expired token mid-request, rather than relying only on a reactive 401.
 *
 * Carries the three properties the reference calls out:
 *   - a GENERATION COUNTER bumped on every `schedule`/`cancel`, captured before the
 *     async `onRefresh` so a reschedule/cancel that lands mid-flight supersedes a
 *     stale result instead of letting it set an orphaned follow-up timer;
 *   - FAILURE-COUNT BACKOFF: a refresh that yields no usable token schedules a retry
 *     after {@link REFRESH_RETRY_DELAY_MS}, up to {@link MAX_REFRESH_FAILURES}, then stops;
 *   - a FALLBACK REFRESH INTERVAL ({@link FALLBACK_REFRESH_INTERVAL_MS}) used to
 *     keep the chain alive when a refreshed token's own expiry is unknown.
 *
 * `onRefresh` resolves to the next absolute expiry (epoch-ms) on success — used to
 * schedule the follow-up — or `undefined` on failure.
 */
export class OAuthRefreshScheduler {
  private timer: unknown = null;
  private generation = 0;
  private failures = 0;
  private readonly setTimer: (fn: () => void, ms: number) => unknown;
  private readonly clearTimer: (handle: unknown) => void;
  private readonly clock: () => number;

  constructor(
    private readonly onRefresh: () => Promise<number | undefined>,
    private readonly bufferMs: number = TOKEN_REFRESH_BUFFER_MS,
    timers: RefreshSchedulerTimers = {},
  ) {
    this.setTimer = timers.setTimer ?? ((fn, ms) => {
      const t = setTimeout(fn, ms);
      if (typeof (t as { unref?: () => void }).unref === 'function') (t as { unref: () => void }).unref();
      return t;
    });
    this.clearTimer = timers.clearTimer ?? ((h) => clearTimeout(h as ReturnType<typeof setTimeout>));
    this.clock = timers.now ?? (() => Date.now());
  }

  /**
   * (Re)arm a proactive refresh for a token expiring at `expiresAtMs`. Replaces any
   * pending timer and bumps the generation so an in-flight refresh is invalidated.
   * A delay at or below the floor fires as soon as possible (clamped, never a busy loop).
   */
  schedule(expiresAtMs: number | undefined): void {
    this.clearPending();
    const gen = ++this.generation;
    const delay = expiresAtMs === undefined
      ? FALLBACK_REFRESH_INTERVAL_MS
      : Math.max(expiresAtMs - this.bufferMs - this.clock(), MIN_REFRESH_DELAY_MS);
    this.timer = this.setTimer(() => { void this.fire(gen); }, delay);
  }

  /** Cancel any pending refresh and invalidate in-flight ones; resets the failure count. */
  cancel(): void {
    this.clearPending();
    this.generation++;
    this.failures = 0;
  }

  private clearPending(): void {
    if (this.timer !== null) { this.clearTimer(this.timer); this.timer = null; }
  }

  private async fire(gen: number): Promise<void> {
    let nextExpiry: number | undefined;
    try {
      nextExpiry = await this.onRefresh();
    } catch {
      nextExpiry = undefined;
    }
    // Superseded by a reschedule/cancel while awaiting — do not set a follow-up timer.
    if (gen !== this.generation) return;
    if (nextExpiry === undefined) {
      this.failures++;
      if (this.failures < MAX_REFRESH_FAILURES) {
        this.timer = this.setTimer(() => { void this.fire(gen); }, REFRESH_RETRY_DELAY_MS);
      } else {
        this.timer = null; // give up after repeated failures — a 401 will recover
      }
      return;
    }
    this.failures = 0;
    this.schedule(nextExpiry);
  }
}

/** Holds the current tokens and answers "is the access token still usable?". */
export class OAuthTokenStore {
  private tokens: OAuthTokens | null = null;
  /**
   * Monotonic generation counter bumped on every authoritative mutation
   * (`set`/`clear`). A refresh captures the generation before its async POST and
   * only commits via {@link applyRefresh} if it is unchanged — so a `clear()`
   * (user sign-out / step-up / dead-grant) or a fresh `set()` that lands WHILE the
   * refresh is in flight supersedes the stale result instead of being clobbered by it.
   */
  private generation = 0;
  /** Treat a token as expired this many ms before its real expiry (clock skew). */
  constructor(private readonly skewMs: number = 30_000) {}

  set(tokens: OAuthTokens): void { this.tokens = tokens; this.generation++; }
  get(): OAuthTokens | null { return this.tokens; }
  clear(): void { this.tokens = null; this.generation++; }

  /** Current generation — capture before an async refresh, pass to {@link applyRefresh}. */
  getGeneration(): number { return this.generation; }

  /**
   * Commit refreshed tokens only if no authoritative mutation happened since
   * `capturedGeneration` was read. Returns true when applied, false when a
   * concurrent clear/set superseded this (stale) refresh and it was discarded.
   */
  applyRefresh(tokens: OAuthTokens, capturedGeneration: number): boolean {
    if (capturedGeneration !== this.generation) return false;
    this.set(tokens);
    return true;
  }

  /** The Authorization header value, or null when there is no live token. */
  authorizationHeader(now: number): string | null {
    if (!this.tokens) return null;
    if (this.isExpired(now)) return null;
    return `${this.tokens.tokenType} ${this.tokens.accessToken}`;
  }

  isExpired(now: number): boolean {
    if (!this.tokens) return true;
    if (this.tokens.expiresAt === undefined) return false;
    return now >= this.tokens.expiresAt - this.skewMs;
  }

  hasRefreshToken(): boolean {
    return !!this.tokens?.refreshToken;
  }
}

/**
 * An {@link OAuthTokenStore} that persists its tokens to a JSON file so an MCP
 * server stays authenticated across process restarts (H22/H45). Tokens are
 * loaded on construction and rewritten on every set/clear with 0600 permissions
 * (owner-only — they are bearer credentials). All disk I/O is best-effort: a
 * failed read/write degrades to in-memory behavior rather than throwing.
 */
export interface FileOAuthTokenStoreOptions {
  skewMs?: number;
  /** Max attempts to acquire the cross-process refresh lock before proceeding without it. */
  lockRetries?: number;
  /** Steal a lock whose directory is older than this many ms (crashed holder). */
  lockStaleMs?: number;
  /** Injectable sleep (ms) for deterministic lock-contention tests. */
  sleep?: (ms: number) => Promise<void>;
  /** Clock seam (epoch ms) for stale-lock detection in tests. */
  now?: () => number;
}

export class FileOAuthTokenStore extends OAuthTokenStore {
  private readonly lockPath: string;
  private readonly lockRetries: number;
  private readonly lockStaleMs: number;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly clock: () => number;

  constructor(private readonly filePath: string, opts: FileOAuthTokenStoreOptions = {}) {
    super(opts.skewMs ?? 30_000);
    this.lockPath = `${filePath}.lock`;
    this.lockRetries = opts.lockRetries ?? 20;
    this.lockStaleMs = opts.lockStaleMs ?? 60_000;
    this.sleep = opts.sleep ?? ((ms) => new Promise((r) => { const t = setTimeout(r, ms); if (typeof (t as { unref?: () => void }).unref === 'function') (t as { unref: () => void }).unref(); }));
    this.clock = opts.now ?? (() => Date.now());
    this.reload();
  }

  /**
   * Re-read the persisted tokens from disk into memory WITHOUT rewriting the file.
   * Used after acquiring the cross-process refresh lock so a refresh landed by a
   * concurrent process is picked up instead of being re-minted redundantly. Routes
   * through the base (in-memory) set, so no disk write is triggered.
   */
  reload(): void {
    try {
      const parsed = JSON.parse(fs.readFileSync(this.filePath, 'utf-8')) as OAuthTokens;
      if (parsed && typeof parsed.accessToken === 'string' && typeof parsed.tokenType === 'string') {
        super.set(parsed);
      }
    } catch {
      /* no cached tokens (or unreadable) — leave current in-memory state */
    }
  }

  set(tokens: OAuthTokens): void {
    super.set(tokens);
    try {
      fs.mkdirSync(path.dirname(this.filePath), { recursive: true, mode: 0o700 });
      fs.writeFileSync(this.filePath, JSON.stringify(tokens), { mode: 0o600 });
    } catch {
      /* best-effort persistence — the in-memory token is still live */
    }
  }

  clear(): void {
    super.clear();
    try { fs.rmSync(this.filePath, { force: true }); } catch { /* ignore */ }
  }

  /**
   * Run `fn` holding a cross-process refresh lock so only ONE process at a time
   * refreshes this server's (typically single-use, rotating) refresh_token — a
   * concurrent refresh in another AMI process would race, and all-but-one POST
   * would get `invalid_grant` and corrupt the shared token file. The lock is an
   * atomic `mkdir` of `<file>.lock` (atomic on POSIX and Windows); on contention
   * it retries with jittered backoff, steals a stale lock left by a crashed holder
   * (older than `lockStaleMs`), and — matching Claude Code — proceeds WITHOUT the
   * lock rather than failing the refresh if it cannot be acquired. The lock dir is
   * always removed in a finally so a clean exit never leaves it behind.
   */
  async withRefreshLock<T>(fn: () => Promise<T>): Promise<T> {
    let held = false;
    for (let attempt = 0; attempt < this.lockRetries; attempt++) {
      try {
        fs.mkdirSync(this.lockPath);
        held = true;
        break;
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code !== 'EEXIST') break; // non-contention error — proceed unlocked
        // Steal a stale lock whose directory predates `lockStaleMs` (crashed holder).
        try {
          const age = this.clock() - fs.statSync(this.lockPath).mtimeMs;
          if (age > this.lockStaleMs) { fs.rmSync(this.lockPath, { recursive: true, force: true }); continue; }
        } catch { /* lock vanished between calls — retry immediately */ continue; }
        await this.sleep(50 + Math.floor(Math.random() * 100));
      }
    }
    try {
      return await fn();
    } finally {
      if (held) { try { fs.rmSync(this.lockPath, { recursive: true, force: true }); } catch { /* already gone */ } }
    }
  }
}

/**
 * Filesystem-safe token filename for a credential-store key (bearer creds live
 * under a 0700 dir). Pass a {@link mcpCredentialKey} (`name|confighash`) so tokens
 * for the same server name but a different config are stored in distinct files; a
 * bare server name is also accepted (filename stays stable per name).
 */
export function mcpTokenFileName(credentialKey: string): string {
  const safe = credentialKey.replace(/[^a-zA-Z0-9_.-]/g, '_');
  const hash = createHash('sha256').update(credentialKey).digest('hex').slice(0, 8);
  return `${safe}.${hash}.json`;
}

/**
 * Interactive consent leg, injected by the frontend. Given the authorization URL
 * it must drive the user through login (open a browser, capture the redirect) and
 * return the authorization `code`. In headless/detached runs the frontend passes
 * an implementation that throws with guidance — the documented degraded path.
 *
 * It SHOULD also return the `state` value echoed back on the redirect so the
 * session can enforce CSRF/session-fixation protection (RFC 6749 §10.12): when a
 * `state` is returned it MUST equal the one passed in, or the flow is rejected. A
 * legacy implementation that omits `state` keeps the prior (unchecked) behavior.
 *
 * It MAY also return a `reportOutcome` callback: the session invokes it exactly once
 * after the authorization code has been spent — `{ ok: true }` when tokens were
 * obtained and stored, `{ ok: false, error }` when the token exchange (or a CSRF
 * re-check) failed. A browser-based consent leg can hold its callback response open
 * until this fires and then render the REAL result to the user's tab, instead of
 * optimistically claiming success the instant the code lands. Authorizers that render
 * their page immediately simply omit it (the session's call becomes a no-op).
 */
export interface AuthorizeResult {
  code: string;
  state?: string;
  reportOutcome?: (outcome: { ok: boolean; error?: string }) => void;
}

export type AuthorizeFn = (params: { authorizationUrl: string; redirectUri: string; state: string }) => Promise<AuthorizeResult>;

export interface McpOAuthSessionOptions {
  /** The MCP server URL — used as the RFC 8707 resource and for well-known fallback. */
  serverUrl: string;
  fetchImpl: FetchLike;
  authorize: AuthorizeFn;
  /** Static loopback redirect URI. Ignored when `resolveRedirectUri` is set. */
  redirectUri: string;
  /**
   * Lazily resolve the redirect URI (e.g. via dynamic available-port discovery).
   * Resolved once on the first auth leg and cached, so the registered redirect
   * URI, the authorization request, and the token exchange all agree. Takes
   * precedence over the static `redirectUri`.
   */
  resolveRedirectUri?: () => Promise<string>;
  /** Pre-registered client id; when absent the session uses Dynamic Client Registration. */
  clientId?: string;
  /** Confidential-client secret (RFC 6749). Sent on token/refresh exchanges when set. */
  clientSecret?: string;
  /**
   * Client ID Metadata Document URL (CIMD / SEP-991). When set — and the
   * authorization server advertises `client_id_metadata_document_supported` — this
   * URL is used directly as the `client_id`, skipping Dynamic Client Registration.
   * The `MCP_OAUTH_CLIENT_METADATA_URL` env var overrides it ({@link resolveClientMetadataUrl}).
   * Ignored when a static `clientId` is configured. MUST be https://.
   */
  clientMetadataUrl?: string;
  scope?: string;
  /**
   * Pin the authorization-server metadata URL (RFC 8414), bypassing well-known
   * discovery for servers whose AS metadata lives at a non-conventional URL. MUST
   * be https://. When set, the operator-trusted URL is fetched directly.
   */
  authServerMetadataUrl?: string;
  /** Optional OIDC `login_hint` pre-filled on the provider consent screen. */
  loginHint?: string;
  /**
   * Browserless Cross-App Access config (XAA / SEP-990). When set AND the XAA env
   * gate ({@link isXaaEnabled}) is on, authorization uses the enterprise token-exchange
   * flow ({@link performCrossAppAccess}) with the pre-obtained `idpIdToken` instead of
   * opening a browser — the headless/enterprise path. When the gate is off, this is
   * ignored and the normal interactive consent flow runs.
   */
  xaa?: XaaConfig;
  /**
   * Process env used to evaluate the XAA gate ({@link isXaaEnabled}). Defaults to
   * process.env; a seam for tests so the gate can be toggled deterministically.
   */
  env?: NodeJS.ProcessEnv;
  /**
   * Enable the proactive pre-expiry refresh scheduler ({@link OAuthRefreshScheduler}):
   * after a token is obtained/refreshed AND a silent refresh is possible, a background
   * timer refreshes it shortly before expiry so a long-running session never presents an
   * expired token mid-request. The scheduler is cancelled when `signal` aborts, on
   * revoke, and on token invalidation. Off by default (reactive-only).
   */
  autoRefresh?: boolean;
  /** Injectable timer seams for the proactive refresh scheduler (deterministic tests). */
  refreshTimers?: RefreshSchedulerTimers;
  /** Clock seam (epoch ms). Defaults to Date.now. */
  now?: () => number;
  /**
   * Proactive refresh-ahead window (ms): treat an access token as expired this many
   * ms BEFORE its real expiry, so a long-running session refreshes ahead of time
   * instead of presenting a token that expires mid-request (clock-skew guard).
   * Applied to the DEFAULT in-memory store; ignored when an explicit `store` is
   * supplied (that store carries its own buffer). Defaults to 30_000.
   */
  refreshBufferMs?: number;
  store?: OAuthTokenStore;
  /**
   * Persisted authorization-server discovery cache ({@link DiscoveryStateStore}).
   * When supplied, a successful RFC 9728 → RFC 8414 discovery is saved and reloaded
   * on construction, so a cold process holding a still-refreshable (file-persisted)
   * token can refresh it WITHOUT first provoking a 401 to re-run discovery. Dropped
   * on `invalidateCredentials('discovery'|'all')`. Off by default (in-memory only).
   */
  discoveryStore?: DiscoveryStateStore;
  /** Per-request HTTP timeout (ms). Defaults to OAUTH_HTTP_TIMEOUT_MS; 0 disables. */
  timeoutMs?: number;
  /**
   * Overall timeout (ms) for the INTERACTIVE consent leg. Bounds the out-of-band
   * `authorize()` call so a never-completed browser consent cannot wedge the flow
   * forever; on expiry the flow rejects with {@link OAuthFlowTimeoutError} and
   * resolves to a non-fatal `false`. Defaults to {@link OAUTH_FLOW_TIMEOUT_MS};
   * 0 disables (unbounded, relying solely on `signal`).
   */
  flowTimeoutMs?: number;
  /** Retry policy for transient token-refresh failures. */
  retry?: RetryOptions;
  /**
   * Caller cancellation signal composed into EVERY OAuth HTTP request (discovery,
   * DCR, token exchange, refresh, revoke). When the frontend aborts it — a user
   * cancelling the consent flow / closing the server menu — the in-flight request
   * is aborted instead of running to completion, and the flow resolves to a
   * non-fatal outcome rather than hanging.
   */
  signal?: AbortSignal;
  /**
   * Optional diagnostic observer. Receives a redacted {@link OAuthLogEvent} at each
   * leg of the flow (cache hit, discovery, registration, consent, token exchange,
   * refresh, revoke, cancellation) so a frontend can show step-by-step progress and
   * attribute a failure to the exact leg. Purely observational — never affects the flow.
   */
  onLog?: OAuthLogger;
}

/**
 * A full OAuth 2.1 provider for one MCP HTTP server: lazily discovers the
 * authorization server on the first 401, drives interactive consent + PKCE code
 * exchange, caches tokens, and transparently refreshes them. Implements
 * McpOAuthProvider, so the client needs no OAuth logic itself.
 */
export class McpOAuthSession implements McpOAuthProvider {
  private readonly store: OAuthTokenStore;
  private readonly now: () => number;
  private readonly fetch: FetchLike;
  private resource: string;
  private authMeta: AuthServerMetadata | null = null;
  private effectiveClientId: string | undefined;
  private effectiveClientSecret: string | undefined;
  private resolvedRedirectUri: string | undefined;
  /** Shared in-flight refresh so concurrent callers coalesce onto one POST. */
  private refreshInFlight: Promise<string | null> | null = null;
  /** Proactive pre-expiry refresh scheduler (null when `autoRefresh` is off). */
  private readonly refreshScheduler: OAuthRefreshScheduler | null;

  /** Resolve the redirect URI once (dynamic port discovery when configured), then cache it. */
  private async getRedirectUri(): Promise<string> {
    if (this.resolvedRedirectUri !== undefined) return this.resolvedRedirectUri;
    this.resolvedRedirectUri = this.opts.resolveRedirectUri
      ? await this.opts.resolveRedirectUri()
      : this.opts.redirectUri;
    return this.resolvedRedirectUri;
  }

  constructor(private readonly opts: McpOAuthSessionOptions) {
    this.store = opts.store ?? new OAuthTokenStore(opts.refreshBufferMs);
    this.now = opts.now ?? (() => Date.now());
    // Every OAuth HTTP call (discovery, DCR, token, refresh) is bounded by a
    // per-request timeout so a hung auth server cannot wedge the session, AND
    // carries the caller's cancellation signal (opts.signal) so a user-cancel
    // aborts the in-flight request. The underlying impl is read per call so it
    // stays swappable after construction.
    const timeoutMs = opts.timeoutMs ?? OAUTH_HTTP_TIMEOUT_MS;
    this.fetch = wrapFetchWithTimeout(
      (url, init) => this.opts.fetchImpl(url, { ...init, signal: combineAbortSignals(init?.signal, this.opts.signal) }),
      timeoutMs,
    );
    this.resource = opts.serverUrl;
    this.effectiveClientId = opts.clientId;
    // Confidential-client secret: configured value wins, else the MCP_CLIENT_SECRET
    // env var (Claude-parity readClientSecret precedence) so the secret need not be
    // inlined into the plaintext server config.
    this.effectiveClientSecret = resolveClientSecret(opts.clientSecret, opts.env);
    // clientId-change invalidation (parity with Claude's xaaIdpCommand): if a static
    // clientId is configured and differs from the one the persisted tokens were minted
    // under, the stored grant is for the OLD client registration — it would fail with
    // opaque `invalid_client`. Drop the stale tokens so a fresh consent runs under the
    // new client id. (A matching or absent stored clientId keeps the tokens.)
    const stored = this.store.get();
    if (opts.clientId && stored?.clientId && stored.clientId !== opts.clientId) {
      this.store.clear();
    }
    // Cross-session discovery reuse: a cold process holding a still-refreshable
    // persisted token would otherwise have to provoke a 401 and re-run RFC 9728 →
    // RFC 8414 discovery before it could refresh. Reload the persisted discovery
    // (authMeta + resource) AND restore the client id the token was minted under,
    // so a silent refresh can proceed immediately on the first request. Skipped
    // when a static clientId change already invalidated the stored tokens above.
    const liveStored = this.store.get();
    if (opts.discoveryStore && liveStored) {
      const cached = opts.discoveryStore.load();
      if (cached) {
        this.authMeta = cached.authServer;
        this.resource = cached.resource;
      }
      if (!this.effectiveClientId && liveStored.clientId) {
        this.effectiveClientId = liveStored.clientId;
      }
    }
    // Proactive refresh scheduler (opt-in): fires a silent refresh shortly before
    // expiry. onRefresh returns the fresh token's expiry so the next proactive
    // refresh can be armed, or undefined so the scheduler's failure-backoff applies.
    this.refreshScheduler = opts.autoRefresh
      ? new OAuthRefreshScheduler(
        async () => {
          if (!this.canSilentRefresh()) return undefined;
          const header = await this.refreshAccessToken();
          if (!header) return undefined;
          return this.store.get()?.expiresAt;
        },
        opts.refreshBufferMs ?? TOKEN_REFRESH_BUFFER_MS,
        opts.refreshTimers,
      )
      : null;
    // Stop all background timers when the caller cancels (server stop/remove), so a
    // torn-down session never leaves a refresh timer armed.
    this.opts.signal?.addEventListener('abort', () => this.refreshScheduler?.cancel(), { once: true });
  }

  /** Emit a redacted diagnostic event to the optional observer (best-effort). */
  private log(event: Omit<OAuthLogEvent, 'serverUrl'>): void {
    if (!this.opts.onLog) return;
    try { this.opts.onLog({ ...event, serverUrl: this.opts.serverUrl }); } catch { /* an observer must never break the flow */ }
  }

  /** True when the caller's cancellation signal has fired. */
  private isCancelled(): boolean {
    return this.opts.signal?.aborted === true;
  }

  async authorizationHeader(): Promise<string | null> {
    const live = this.store.authorizationHeader(this.now());
    if (live) {
      this.log({ step: 'cache-hit', message: 'using cached access token (not yet expired)', ok: true });
      return live;
    }
    // Token present but expired, and we can refresh without user interaction.
    if (this.canSilentRefresh()) return this.refreshAccessToken();
    return null;
  }

  /** True when a silent (no-browser) refresh is possible: a refresh token, discovered AS, and a client id. */
  private canSilentRefresh(): boolean {
    return this.store.hasRefreshToken() && !!this.authMeta && !!this.effectiveClientId;
  }

  /**
   * Single-flight silent refresh shared by the proactive (expired-header) path and
   * the reactive (401 clock-drift) path. A burst of requests arriving at expiry
   * must not each POST the (typically single-use, rotating) refresh_token — all but
   * one would get invalid_grant and corrupt the stored token. Concurrent callers
   * share one in-flight refresh promise, cleared once it settles.
   */
  private refreshAccessToken(): Promise<string | null> {
    if (this.refreshInFlight) return this.refreshInFlight;
    this.refreshInFlight = this.doRefresh().finally(() => { this.refreshInFlight = null; });
    return this.refreshInFlight;
  }

  /**
   * Perform one silent refresh of the stored tokens; returns the new header or null.
   * When the store is file-backed the whole refresh runs under a CROSS-PROCESS lock
   * (so concurrent AMI processes do not each burn the single-use rotating
   * refresh_token): after acquiring the lock we re-read the token file, and if a
   * sibling process already landed a still-valid access token we adopt it and skip
   * the network POST entirely (re-read-after-lock dedup — parity with Claude's
   * `refreshAuthorization`).
   */
  private async doRefresh(): Promise<string | null> {
    const store = this.store;
    if (store instanceof FileOAuthTokenStore) {
      return store.withRefreshLock(async () => {
        // Another process may have refreshed while we waited for the lock — re-read
        // from disk and, if the token is live again, reuse it instead of POSTing.
        store.reload();
        const fresh = store.authorizationHeader(this.now());
        if (fresh) {
          this.log({ step: 'refresh', message: 'another process already refreshed the token; reusing it', ok: true });
          return fresh;
        }
        return this.performRefreshPost();
      });
    }
    return this.performRefreshPost();
  }

  /** The actual refresh_token POST + store-commit, shared by the locked and unlocked paths. */
  private async performRefreshPost(): Promise<string | null> {
    const authMeta = this.authMeta!;
    const clientId = this.effectiveClientId!;
    const clientSecret = this.effectiveClientSecret;
    // Capture the store generation BEFORE the async POST so a clear()/set() that
    // lands while the refresh is in flight (sign-out, step-up, a fresh consent)
    // supersedes this result instead of being overwritten by a stale token.
    const generation = this.store.getGeneration();
    this.log({ step: 'refresh', message: 'refreshing access token via refresh_token grant' });
    try {
      const refreshed = await retryWithBackoff(
        () => refreshToken(this.fetch, {
          tokenEndpoint: authMeta.tokenEndpoint,
          refreshToken: this.store.get()!.refreshToken!,
          clientId,
          clientSecret,
          resource: this.resource,
          // RFC 6749 §6: carry the configured scope across the refresh so servers
          // that would otherwise drop previously-granted scopes preserve them.
          scope: this.opts.scope,
        }, this.now()),
        { ...this.opts.retry, isRetryable: this.opts.retry?.isRetryable ?? isTransientOAuthError },
      );
      // Stamp the client id the token was minted under so a later clientId change is
      // detectable across restarts (clientId-change invalidation).
      refreshed.clientId = clientId;
      // Discard a stale refresh whose generation was superseded mid-flight.
      if (!this.store.applyRefresh(refreshed, generation)) {
        this.log({ step: 'refresh', message: 'refresh superseded by a concurrent credential change', ok: false });
        return null;
      }
      this.log({ step: 'refresh', message: 'access token refreshed', ok: true });
      return this.store.authorizationHeader(this.now());
    } catch (error) {
      // A dead grant (invalid_grant) can never recover by retry — drop the stored
      // tokens so the next 401 drives a full interactive re-auth.
      if (error instanceof OAuthError && error.code === 'invalid_grant') await this.invalidateCredentials('tokens');
      this.log({ step: 'refresh', message: 'refresh failed', ok: false, error: redactTokenBody(error instanceof Error ? error.message : String(error)) });
      return null; // fall through to a full re-auth on the next 401
    }
  }

  async onUnauthorized(wwwAuthenticate: string | null): Promise<boolean> {
    // Reactive clock-drift recovery (parity with Claude's withOAuth401Retry): the
    // server returned 401 even though the stored token may look locally valid.
    // Attempt ONE silent refresh (RFC 6749 §6) before driving a full interactive
    // re-consent — a drifted clock or an early server-side invalidation is fixed by
    // a fresh access token with no browser round-trip. Only runs once authMeta is
    // known (a prior auth happened) and a refresh token exists.
    if (this.canSilentRefresh()) {
      const header = await this.refreshAccessToken();
      if (header) {
        // Re-arm the proactive refresh against the freshly-minted expiry.
        this.refreshScheduler?.schedule(this.store.get()?.expiresAt);
        return true;
      }
    }
    return this.performAuthorization(wwwAuthenticate);
  }

  /**
   * Revoke the session's tokens server-side (RFC 7009) and clear local state — a
   * user-initiated "sign out of this MCP server". The AS revocation endpoint is
   * discovered best-effort if not already known; the refresh token is revoked
   * FIRST (the long-lived credential) then the access token, each independently
   * best-effort so one failure does not abort the other. The local store is always
   * cleared at the end (even when the server has no revocation endpoint or the
   * revocation call fails), so a discarded credential never lingers.
   */
  async revoke(): Promise<void> {
    const tokens = this.store.get();
    this.log({ step: 'revoke', message: 'revoking tokens server-side and clearing local credentials' });
    try {
      if (tokens) {
        // Discover the AS (hence the revocation endpoint) if a prior 401 flow has
        // not already populated it — otherwise there is nothing to revoke against.
        if (!this.authMeta) {
          try { await this.discoverAuthorizationServer(null); } catch { /* best-effort */ }
        }
        const endpoint = this.authMeta?.revocationEndpoint;
        if (endpoint && this.effectiveClientId) {
          const authMethod = pickClientAuthMethod(
            this.authMeta!.revocationEndpointAuthMethodsSupported
              ?? this.authMeta!.tokenEndpointAuthMethodsSupported,
            !!this.effectiveClientSecret,
          );
          // Refresh token first (long-lived), then the access token — best-effort each.
          for (const [token, hint] of [
            [tokens.refreshToken, 'refresh_token'] as const,
            [tokens.accessToken, 'access_token'] as const,
          ]) {
            if (!token) continue;
            try {
              await revokeToken(this.fetch, {
                revocationEndpoint: endpoint,
                token,
                tokenTypeHint: hint,
                clientId: this.effectiveClientId,
                clientSecret: this.effectiveClientSecret,
                authMethod,
              });
            } catch { /* best-effort — a failed revoke must not block sign-out */ }
          }
        }
      }
    } finally {
      this.store.clear();
      this.refreshScheduler?.cancel();
      this.log({ step: 'revoke', message: 'local credentials cleared', ok: true });
    }
  }

  /**
   * Granular credential invalidation (parity with Claude Code's
   * `invalidateCredentials`). Clears exactly the slice of local auth state named by
   * `scope`, leaving the rest intact so a caller can force the MINIMAL re-work:
   *   - `tokens`    — drop the access/refresh tokens (next request re-auths/refreshes);
   *   - `client`    — drop the (dynamically-registered) client id/secret so the next
   *                   flow re-registers; resets to the configured clientId/secret, if any;
   *   - `discovery` — drop cached authorization-server metadata so the next flow
   *                   re-runs RFC 9728/8414 discovery (e.g. after the AS rotated endpoints);
   *   - `verifier`  — no-op: the PKCE verifier is per-flow and never persisted here,
   *                   so there is nothing to clear (kept for API parity/completeness);
   *   - `all`       — every slice above.
   * Unlike revoke(), this is purely LOCAL (no server-side RFC 7009 call) — use it to
   * drop state that is stale/unusable rather than to sign out.
   */
  async invalidateCredentials(scope: 'all' | 'client' | 'tokens' | 'verifier' | 'discovery'): Promise<void> {
    if (scope === 'tokens' || scope === 'all') { this.store.clear(); this.refreshScheduler?.cancel(); }
    if (scope === 'client' || scope === 'all') {
      this.effectiveClientId = this.opts.clientId;
      // Re-resolve against config + MCP_CLIENT_SECRET on reset, mirroring the constructor.
      this.effectiveClientSecret = resolveClientSecret(this.opts.clientSecret, this.opts.env);
    }
    if (scope === 'discovery' || scope === 'all') {
      this.authMeta = null;
      this.resource = this.opts.serverUrl;
      // Also drop any persisted discovery so the next flow re-runs RFC 9728/8414
      // (e.g. after the AS rotated endpoints) rather than reloading stale metadata.
      this.opts.discoveryStore?.clear();
    }
    this.log({ step: 'revoke', message: `invalidated local credentials (scope: ${scope})`, ok: true });
  }

  async onForbidden(wwwAuthenticate: string | null): Promise<boolean> {
    const parsed = parseWwwAuthenticate(wwwAuthenticate);
    // Only a genuine insufficient_scope challenge warrants a step-up re-consent.
    // Any other 403 is a real authorization denial the user cannot resolve by
    // re-consenting, so do not loop them through the browser.
    if (parsed.error !== 'insufficient_scope') return false;
    // Drop the stale token so authorizationHeader() cannot silently refresh the
    // SAME insufficient scope and mask the step-up; force full interactive consent.
    // A step-up IS a re-authentication, so revoke the abandoned token server-side
    // (RFC 7009, best-effort — a no-op when the AS advertises no revocation
    // endpoint) before re-consenting, matching Claude's revoke-then-reauth.
    await this.revoke();
    return this.performAuthorization(wwwAuthenticate, parsed.scope);
  }

  /**
   * Resolve the authorization server for this MCP server, setting `this.authMeta`
   * and `this.resource`. The primary path is RFC 9728 (protected-resource
   * metadata) → RFC 8414 (authorization-server metadata). If that chain fails
   * (non-200, missing authorization_servers, schema/network error) AND no metadata
   * URL is pinned AND the server URL carries a path component, it falls back to a
   * path-aware RFC 8414 probe directly against the MCP server URL — covering legacy
   * servers that co-host AS metadata at `/.well-known/oauth-authorization-server/{path}`
   * without implementing RFC 9728. For a root (pathless) server URL the primary
   * well-known probe already covered the same endpoint, so the original error is
   * rethrown rather than retried.
   */
  private async discoverAuthorizationServer(wwwAuthenticate: string | null): Promise<{ prmScopes?: string[] }> {
    const parsed = parseWwwAuthenticate(wwwAuthenticate);
    const metadataUrl = parsed.resourceMetadata ?? wellKnown(this.opts.serverUrl, 'oauth-protected-resource');
    this.log({ step: 'discover-prm', message: `discovering protected-resource metadata at ${redactOAuthUrl(metadataUrl)}` });
    try {
      const prm = await discoverProtectedResource(this.fetch, metadataUrl);
      // RFC 9728 §3.3 mix-up protection: the resource the PRM describes must be
      // the server we are actually talking to (same origin). Reject a PRM that
      // points at a different host to avoid an authorization mix-up attack.
      assertResourceMatch(prm.resource, this.opts.serverUrl);
      this.resource = prm.resource;
      // RFC 9728 lets a PRM advertise MULTIPLE authorization servers. Try each in
      // order, skipping one whose metadata discovery fails or that cannot perform
      // the authorization_code grant, and accumulate per-AS reasons — so a down or
      // incapable primary AS does not sink the flow when a usable alternative is
      // listed. Only after every candidate fails do we fall through to the error.
      const servers = prm.authorizationServers;
      const errors: string[] = [];
      for (const issuer of servers) {
        try {
          assertSecureUrl(issuer, 'authorization server issuer');
          this.log({ step: 'discover-as', message: `discovering authorization-server metadata for ${redactOAuthUrl(issuer)}` });
          const meta = await discoverAuthServer(this.fetch, issuer, this.opts.authServerMetadataUrl);
          this.assertGrantCapable(meta);
          this.authMeta = meta;
          this.persistDiscovery();
          this.log({ step: 'discover-as', message: `authorization server resolved: ${redactOAuthUrl(issuer)}`, ok: true });
          return { prmScopes: prm.scopesSupported };
        } catch (asError) {
          errors.push(`${issuer}: ${asError instanceof Error ? asError.message : String(asError)}`);
        }
      }
      throw new Error(`no usable authorization server among ${servers.length} advertised (${errors.join('; ')})`);
    } catch (prmError) {
      if (this.opts.authServerMetadataUrl === undefined && hasUrlPath(this.opts.serverUrl)) {
        assertSecureUrl(this.opts.serverUrl, 'authorization server issuer');
        const meta = await discoverAuthServer(this.fetch, this.opts.serverUrl);
        this.assertGrantCapable(meta);
        this.authMeta = meta;
        this.resource = this.opts.serverUrl;
        this.persistDiscovery();
        return {};
      }
      throw prmError;
    }
  }

  /**
   * RFC 8414 grant-type capability check: if the AS advertises its supported
   * grants, refuse to drive an authorization_code consent flow against one that
   * cannot perform it (it could never complete). A server that omits the list is
   * assumed capable (the common case).
   */
  private assertGrantCapable(meta: AuthServerMetadata): void {
    if (meta.grantTypesSupported && !meta.grantTypesSupported.includes('authorization_code')) {
      throw new Error('authorization server does not advertise the authorization_code grant');
    }
  }

  /**
   * Persist the freshly-discovered authorization-server metadata + resource to the
   * injected {@link DiscoveryStateStore} (best-effort, no-op when none is configured)
   * so a later cold process can refresh an existing token without re-discovering.
   */
  private persistDiscovery(): void {
    if (this.opts.discoveryStore && this.authMeta) {
      this.opts.discoveryStore.save({ resource: this.resource, authServer: this.authMeta });
    }
  }

  /**
   * Race the interactive consent leg against the overall flow timeout
   * ({@link OAuthFlowTimeoutError}). A `flowTimeoutMs` of 0 disables the cap (relying
   * solely on the caller's abort signal). The timer is unref'd so it never keeps the
   * event loop alive, and always cleared when consent settles first.
   */
  private async awaitConsent(consent: Promise<AuthorizeResult>): Promise<AuthorizeResult> {
    const ms = this.opts.flowTimeoutMs ?? OAUTH_FLOW_TIMEOUT_MS;
    if (!ms || ms <= 0) return consent;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new OAuthFlowTimeoutError(ms)), ms);
      if (typeof (timer as { unref?: () => void }).unref === 'function') (timer as { unref: () => void }).unref();
    });
    try {
      return await Promise.race([consent, timeout]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  /**
   * Shared consent flow for 401 (re)authorization and 403 step-up: discover the
   * auth server, (dynamically register if needed), run interactive consent + PKCE
   * code exchange, and cache the resulting tokens. `requiredScope` (from a 403
   * challenge) is unioned with the configured/advertised scope so the new grant
   * covers what the resource demanded.
   */
  private async performAuthorization(wwwAuthenticate: string | null, requiredScope?: string): Promise<boolean> {
    // Browserless Cross-App Access (XAA / SEP-990): when an enterprise IdP config is
    // supplied AND the env gate is on, obtain the token via the token-exchange flow
    // using the pre-obtained id_token instead of opening a browser for consent.
    if (this.opts.xaa && isXaaEnabled(this.opts.env)) {
      return this.performXaaAuthorization();
    }
    // Report the token-exchange outcome back to the consent leg exactly once, so a
    // browser-based authorizer can defer its final callback page until the REAL result
    // is known (success only after tokens are stored; error on a failed exchange/CSRF
    // re-check). A no-op for authorizers that resolved their page immediately.
    let authResult: AuthorizeResult | undefined;
    let outcomeReported = false;
    const reportOutcome = (ok: boolean, error?: string): void => {
      if (outcomeReported) return;
      outcomeReported = true;
      authResult?.reportOutcome?.({ ok, error });
    };
    try {
      const { prmScopes } = await this.discoverAuthorizationServer(wwwAuthenticate);
      const authMeta = this.authMeta!;
      // The authorization server was already confirmed capable of the
      // authorization_code grant during discovery (assertGrantCapable), so an
      // incapable AS never reaches here.

      const redirectUri = await this.getRedirectUri();
      if (!this.effectiveClientId) {
        // CIMD (SEP-991): when a Client ID Metadata Document URL is configured AND
        // the authorization server advertises client_id_metadata_document_supported,
        // use that URL directly as the client_id — the server dereferences it for the
        // client's metadata, so Dynamic Client Registration is skipped entirely.
        const cimdUrl = resolveClientMetadataUrl(this.opts.clientMetadataUrl);
        if (cimdUrl && authMeta.clientIdMetadataDocumentSupported) {
          this.effectiveClientId = cimdUrl;
          this.log({ step: 'register-client', message: 'using client-id metadata document (CIMD/SEP-991), skipping dynamic registration', ok: true });
        } else {
          if (!authMeta.registrationEndpoint) throw new Error('no client_id and no registration endpoint');
          // Register with an auth method the server actually accepts (per its
          // advertised token_endpoint_auth_methods_supported), not an assumed `none`.
          this.log({ step: 'register-client', message: 'registering a dynamic OAuth client (RFC 7591)' });
          const reg = await registerClient(
            this.fetch, authMeta.registrationEndpoint, [redirectUri], undefined, authMeta.tokenEndpointAuthMethodsSupported,
          );
          this.effectiveClientId = reg.clientId;
          // A confidential client registered dynamically returns its secret once.
          if (reg.clientSecret) this.effectiveClientSecret = reg.clientSecret;
          this.log({ step: 'register-client', message: 'dynamic client registered', ok: true });
        }
      }

      const pkce = generatePkce();
      const state = generateState();
      // Scope precedence: an explicitly configured scope wins; else resolve from
      // the authorization-server metadata (scope / default_scope / scopes_supported);
      // else fall back to the protected-resource metadata's advertised scopes. Any
      // 403 step-up `requiredScope` is unioned in so re-consent covers it.
      const scope = unionScopes(
        this.opts.scope ?? resolveScopeFromMetadata(authMeta) ?? prmScopes?.join(' '),
        requiredScope,
      );
      const authorizationUrl = buildAuthorizationUrl({
        authorizationEndpoint: authMeta.authorizationEndpoint,
        clientId: this.effectiveClientId,
        redirectUri,
        resource: this.resource,
        codeChallenge: pkce.challenge,
        state,
        scope,
        loginHint: this.opts.loginHint,
      });
      this.log({ step: 'authorize', message: `awaiting interactive consent at ${redactOAuthUrl(authorizationUrl)}` });
      authResult = await this.awaitConsent(this.opts.authorize({ authorizationUrl, redirectUri, state }));
      // RFC 6749 §10.12 CSRF protection: if the consent leg echoes the callback
      // `state`, it MUST match the one we generated. A mismatch means the redirect
      // did not originate from our request — reject before spending the code.
      if (authResult.state !== undefined && authResult.state !== state) {
        throw new Error('OAuth state mismatch (possible CSRF): callback state does not match the authorization request');
      }
      const code = authResult.code;
      this.log({ step: 'exchange-code', message: 'exchanging authorization code for tokens' });
      const tokens = await exchangeCode(this.fetch, {
        tokenEndpoint: authMeta.tokenEndpoint,
        code,
        verifier: pkce.verifier,
        clientId: this.effectiveClientId,
        clientSecret: this.effectiveClientSecret,
        redirectUri,
        resource: this.resource,
      }, this.now());
      // Stamp the client id these tokens were minted under so a configured-clientId
      // change across restarts invalidates the now-stale grant (see constructor).
      tokens.clientId = this.effectiveClientId;
      this.store.set(tokens);
      // Arm the proactive pre-expiry refresh (no-op when autoRefresh is off / no
      // refresh token), so a long-running session refreshes ahead of this expiry.
      if (this.canSilentRefresh()) this.refreshScheduler?.schedule(tokens.expiresAt);
      this.log({ step: 'token-stored', message: 'access token obtained and stored', ok: true });
      reportOutcome(true);
      return true;
    } catch (error) {
      // Surface the failure to the consent leg's (optionally deferred) browser page.
      reportOutcome(false, error instanceof Error ? error.message : String(error));
      // A user cancellation (caller signal aborted) and an overall-flow timeout are
      // NOT generic failures — surface each as a distinct 'cancelled' event so a
      // frontend can attribute a hung/aborted consent leg precisely rather than
      // rendering it as an opaque error. Any other failure keeps its redacted reason.
      if (error instanceof OAuthFlowTimeoutError) {
        this.log({ step: 'cancelled', message: error.message, ok: false });
      } else if (this.isCancelled()) {
        this.log({ step: 'cancelled', message: 'OAuth flow cancelled by the user', ok: false });
      } else {
        this.log({ step: 'authorize', message: 'OAuth flow failed', ok: false, error: redactTokenBody(error instanceof Error ? error.message : String(error)) });
      }
      return false;
    }
  }

  /**
   * Browserless Cross-App Access authorization (XAA / SEP-990): run the enterprise
   * token-exchange flow with the configured {@link XaaConfig} instead of interactive
   * consent, then cache the resulting tokens exactly like the browser flow. Populates
   * `authMeta`/`resource`/`effectiveClientId` from the discovered AS so a subsequent
   * silent refresh and revocation work identically. Returns whether a token was obtained.
   */
  private async performXaaAuthorization(): Promise<boolean> {
    const xaa = this.opts.xaa!;
    this.log({ step: 'xaa-exchange', message: 'performing browserless cross-app access token exchange (SEP-990)' });
    try {
      const result = await performCrossAppAccess(this.fetch, this.opts.serverUrl, xaa, this.now());
      // Carry discovery state so refresh/revocation locate the AS token/revocation
      // endpoints (the MCP URL is not the AS URL in a typical XAA setup). Persist it
      // too so a cold process can refresh the XAA grant without re-discovering.
      this.authMeta = result.authServer;
      this.resource = result.resource;
      this.persistDiscovery();
      this.effectiveClientId = xaa.clientId;
      this.effectiveClientSecret = xaa.clientSecret;
      const tokens = result.tokens;
      tokens.clientId = xaa.clientId;
      this.store.set(tokens);
      if (this.canSilentRefresh()) this.refreshScheduler?.schedule(tokens.expiresAt);
      this.log({ step: 'token-stored', message: 'access token obtained via cross-app access and stored', ok: true });
      return true;
    } catch (error) {
      if (this.isCancelled()) {
        this.log({ step: 'cancelled', message: 'cross-app access flow cancelled by the user', ok: false });
      } else {
        this.log({ step: 'xaa-exchange', message: 'cross-app access flow failed', ok: false, error: redactTokenBody(error instanceof Error ? error.message : String(error)) });
      }
      return false;
    }
  }
}
