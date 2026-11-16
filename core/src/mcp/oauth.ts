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
 * Fallback access-token lifetime (seconds) when a token response omits
 * `expires_in` AND the access token is not a JWT carrying an `exp` claim. Without
 * this a tokenless-of-expiry grant would be treated as never-expiring and only a
 * reactive 401 would ever refresh it; a sane default keeps such tokens on a
 * proactive refresh/expiry schedule (parity with Claude's 3600s default).
 */
export const DEFAULT_TOKEN_LIFETIME_SEC = 3600;

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
  constructor(message: string, readonly status: number, readonly code?: string, readonly description?: string) {
    super(message);
    this.name = 'OAuthError';
  }
}

/** Non-standard error codes some servers return that mean the grant is dead. */
const INVALID_GRANT_ALIASES = new Set(['invalid_grant', 'invalid_refresh_token', 'expired_token', 'token_expired']);

/**
 * Build an OAuthError from a token-endpoint response, normalising both the
 * HTTP-200-with-error-body shape (some non-compliant servers) and non-standard
 * `invalid_grant` aliases so callers can reason about one canonical code.
 */
export function normalizeTokenError(status: number, doc: Record<string, unknown>): OAuthError {
  const rawCode = typeof doc.error === 'string' ? doc.error : undefined;
  const code = rawCode && INVALID_GRANT_ALIASES.has(rawCode) ? 'invalid_grant' : rawCode;
  const description = typeof doc.error_description === 'string' ? doc.error_description : undefined;
  const suffix = code ? `: ${code}${description ? ` (${description})` : ''}` : '';
  return new OAuthError(`token endpoint error (${status})${suffix}`, status, code, description);
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

/** Wrap a fetch impl so every call is bounded by a per-request timeout. */
export function wrapFetchWithTimeout(fetchImpl: FetchLike, ms: number): FetchLike {
  return (url, init) => withTimeout((signal) => fetchImpl(url, { ...init, signal: init?.signal ?? signal }), ms, `OAuth request to ${url}`);
}

export interface RetryOptions {
  retries?: number;
  baseMs?: number;
  isRetryable?: (error: unknown) => boolean;
  sleep?: (ms: number) => Promise<void>;
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
      await sleep(baseMs * 2 ** attempt);
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
}

export interface OAuthTokens {
  accessToken: string;
  tokenType: string;
  refreshToken?: string;
  /** Absolute epoch-ms expiry (computed from expires_in at receipt). */
  expiresAt?: number;
  scope?: string;
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
  if (!res.ok) throw new Error(`OAuth discovery failed (${res.status}) for ${url}`);
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
  if (!res.ok) throw new Error(`dynamic client registration failed (${res.status})`);
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

function toTokens(doc: Record<string, unknown>, now: number): OAuthTokens {
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

/** Holds the current tokens and answers "is the access token still usable?". */
export class OAuthTokenStore {
  private tokens: OAuthTokens | null = null;
  /** Treat a token as expired this many ms before its real expiry (clock skew). */
  constructor(private readonly skewMs: number = 30_000) {}

  set(tokens: OAuthTokens): void { this.tokens = tokens; }
  get(): OAuthTokens | null { return this.tokens; }
  clear(): void { this.tokens = null; }

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
export class FileOAuthTokenStore extends OAuthTokenStore {
  constructor(private readonly filePath: string, skewMs = 30_000) {
    super(skewMs);
    try {
      const parsed = JSON.parse(fs.readFileSync(filePath, 'utf-8')) as OAuthTokens;
      if (parsed && typeof parsed.accessToken === 'string' && typeof parsed.tokenType === 'string') {
        super.set(parsed);
      }
    } catch {
      /* no cached tokens (or unreadable) — start empty */
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
}

/** Filesystem-safe token filename for a server (bearer creds live under 0700 dir). */
export function mcpTokenFileName(serverName: string): string {
  const safe = serverName.replace(/[^a-zA-Z0-9_.-]/g, '_');
  const hash = createHash('sha256').update(serverName).digest('hex').slice(0, 8);
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
 */
export type AuthorizeFn = (params: { authorizationUrl: string; redirectUri: string; state: string }) => Promise<{ code: string; state?: string }>;

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
  scope?: string;
  /**
   * Pin the authorization-server metadata URL (RFC 8414), bypassing well-known
   * discovery for servers whose AS metadata lives at a non-conventional URL. MUST
   * be https://. When set, the operator-trusted URL is fetched directly.
   */
  authServerMetadataUrl?: string;
  /** Optional OIDC `login_hint` pre-filled on the provider consent screen. */
  loginHint?: string;
  /** Clock seam (epoch ms). Defaults to Date.now. */
  now?: () => number;
  store?: OAuthTokenStore;
  /** Per-request HTTP timeout (ms). Defaults to OAUTH_HTTP_TIMEOUT_MS; 0 disables. */
  timeoutMs?: number;
  /** Retry policy for transient token-refresh failures. */
  retry?: RetryOptions;
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

  /** Resolve the redirect URI once (dynamic port discovery when configured), then cache it. */
  private async getRedirectUri(): Promise<string> {
    if (this.resolvedRedirectUri !== undefined) return this.resolvedRedirectUri;
    this.resolvedRedirectUri = this.opts.resolveRedirectUri
      ? await this.opts.resolveRedirectUri()
      : this.opts.redirectUri;
    return this.resolvedRedirectUri;
  }

  constructor(private readonly opts: McpOAuthSessionOptions) {
    this.store = opts.store ?? new OAuthTokenStore();
    this.now = opts.now ?? (() => Date.now());
    // Every OAuth HTTP call (discovery, DCR, token, refresh) is bounded by a
    // per-request timeout so a hung auth server cannot wedge the session. The
    // underlying impl is read per call so it stays swappable after construction.
    const timeoutMs = opts.timeoutMs ?? OAUTH_HTTP_TIMEOUT_MS;
    this.fetch = wrapFetchWithTimeout((url, init) => this.opts.fetchImpl(url, init), timeoutMs);
    this.resource = opts.serverUrl;
    this.effectiveClientId = opts.clientId;
    this.effectiveClientSecret = opts.clientSecret;
  }

  async authorizationHeader(): Promise<string | null> {
    const live = this.store.authorizationHeader(this.now());
    if (live) return live;
    // Token present but expired, and we can refresh without user interaction.
    if (this.store.hasRefreshToken() && this.authMeta && this.effectiveClientId) {
      // Single-flight: a burst of requests arriving at expiry must not each POST
      // the (typically single-use, rotating) refresh_token — all but one would
      // get invalid_grant and corrupt the stored token. Concurrent callers share
      // one in-flight refresh promise, cleared once it settles.
      if (this.refreshInFlight) return this.refreshInFlight;
      this.refreshInFlight = this.doRefresh().finally(() => { this.refreshInFlight = null; });
      return this.refreshInFlight;
    }
    return null;
  }

  /** Perform one silent refresh of the stored tokens; returns the new header or null. */
  private async doRefresh(): Promise<string | null> {
    const authMeta = this.authMeta!;
    const clientId = this.effectiveClientId!;
    const clientSecret = this.effectiveClientSecret;
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
      this.store.set(refreshed);
      return this.store.authorizationHeader(this.now());
    } catch (error) {
      // A dead grant (invalid_grant) can never recover by retry — clear the
      // stored tokens so the next 401 drives a full interactive re-auth.
      if (error instanceof OAuthError && error.code === 'invalid_grant') this.store.clear();
      return null; // fall through to a full re-auth on the next 401
    }
  }

  async onUnauthorized(wwwAuthenticate: string | null): Promise<boolean> {
    return this.performAuthorization(wwwAuthenticate);
  }

  async onForbidden(wwwAuthenticate: string | null): Promise<boolean> {
    const parsed = parseWwwAuthenticate(wwwAuthenticate);
    // Only a genuine insufficient_scope challenge warrants a step-up re-consent.
    // Any other 403 is a real authorization denial the user cannot resolve by
    // re-consenting, so do not loop them through the browser.
    if (parsed.error !== 'insufficient_scope') return false;
    // Drop the stale token so authorizationHeader() cannot silently refresh the
    // SAME insufficient scope and mask the step-up; force full interactive consent.
    this.store.clear();
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
    try {
      const prm = await discoverProtectedResource(this.fetch, metadataUrl);
      // RFC 9728 §3.3 mix-up protection: the resource the PRM describes must be
      // the server we are actually talking to (same origin). Reject a PRM that
      // points at a different host to avoid an authorization mix-up attack.
      assertResourceMatch(prm.resource, this.opts.serverUrl);
      this.resource = prm.resource;
      const issuer = prm.authorizationServers[0];
      assertSecureUrl(issuer, 'authorization server issuer');
      this.authMeta = await discoverAuthServer(this.fetch, issuer, this.opts.authServerMetadataUrl);
      return { prmScopes: prm.scopesSupported };
    } catch (prmError) {
      if (this.opts.authServerMetadataUrl === undefined && hasUrlPath(this.opts.serverUrl)) {
        assertSecureUrl(this.opts.serverUrl, 'authorization server issuer');
        this.authMeta = await discoverAuthServer(this.fetch, this.opts.serverUrl);
        this.resource = this.opts.serverUrl;
        return {};
      }
      throw prmError;
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
    try {
      const { prmScopes } = await this.discoverAuthorizationServer(wwwAuthenticate);
      const authMeta = this.authMeta!;
      // RFC 8414 grant-type capability check: if the server advertises its
      // supported grants, refuse to drive an authorization_code consent flow
      // against one that cannot perform it (it could never complete).
      if (authMeta.grantTypesSupported && !authMeta.grantTypesSupported.includes('authorization_code')) {
        throw new Error('authorization server does not advertise the authorization_code grant');
      }

      const redirectUri = await this.getRedirectUri();
      if (!this.effectiveClientId) {
        if (!authMeta.registrationEndpoint) throw new Error('no client_id and no registration endpoint');
        // Register with an auth method the server actually accepts (per its
        // advertised token_endpoint_auth_methods_supported), not an assumed `none`.
        const reg = await registerClient(
          this.fetch, authMeta.registrationEndpoint, [redirectUri], undefined, authMeta.tokenEndpointAuthMethodsSupported,
        );
        this.effectiveClientId = reg.clientId;
        // A confidential client registered dynamically returns its secret once.
        if (reg.clientSecret) this.effectiveClientSecret = reg.clientSecret;
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
      const authResult = await this.opts.authorize({ authorizationUrl, redirectUri, state });
      // RFC 6749 §10.12 CSRF protection: if the consent leg echoes the callback
      // `state`, it MUST match the one we generated. A mismatch means the redirect
      // did not originate from our request — reject before spending the code.
      if (authResult.state !== undefined && authResult.state !== state) {
        throw new Error('OAuth state mismatch (possible CSRF): callback state does not match the authorization request');
      }
      const code = authResult.code;
      const tokens = await exchangeCode(this.fetch, {
        tokenEndpoint: authMeta.tokenEndpoint,
        code,
        verifier: pkce.verifier,
        clientId: this.effectiveClientId,
        clientSecret: this.effectiveClientSecret,
        redirectUri,
        resource: this.resource,
      }, this.now());
      this.store.set(tokens);
      return true;
    } catch {
      return false;
    }
  }
}
