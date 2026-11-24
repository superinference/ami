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
import 'crypto';
import 'fs';
import * as path from 'path';
import '../utils/env';
import { OAUTH_HTTP_TIMEOUT_MS, OAuthError, OAuthFlowTimeoutError, SENSITIVE_OAUTH_PARAMS, combineAbortSignals, extractOAuthErrorBody, isTransientOAuthError, redactTokenBody, resolveClientMetadataUrl, retryWithBackoff, wrapFetchWithTimeout } from './oauth-auth-server-metadata';
import type { AuthServerMetadata, FetchLike, OAuthLogEvent, OAuthTokens } from './oauth-auth-server-metadata';
import { ID_JAG_TOKEN_TYPE, ID_TOKEN_TYPE, JWT_BEARER_GRANT, MAX_REFRESH_FAILURES, MIN_REFRESH_DELAY_MS, TOKEN_EXCHANGE_GRANT, coerceExpiresIn, discoverProtectedResource, generatePkce, generateState, getJson, isXaaEnabled, parseWwwAuthenticate, pickClientAuthMethod, registerClient, resolveClientSecret, resolveScopeFromMetadata, toTokens, unionScopes } from './oauth-mcp-o-auth-provider';
import type { ClientAuthMethod, JwtAuthGrantResult, McpOAuthProvider, XaaConfig, XaaResult } from './oauth-mcp-o-auth-provider';
import { FileOAuthTokenStore, OAuthTokenStore } from './oauth-mcp-o-auth-session-options';
import type { AuthorizeResult, McpOAuthSessionOptions, RefreshSchedulerTimers } from './oauth-mcp-o-auth-session-options';
export { DEFAULT_TOKEN_LIFETIME_SEC, FileDiscoveryStateStore, MCP_OAUTH_CLIENT_METADATA_URL_ENV, OAUTH_HTTP_TIMEOUT_MS, OAuthError, OAuthFlowTimeoutError, SENSITIVE_OAUTH_PARAMS, combineAbortSignals, extractOAuthErrorBody, isTransientOAuthError, mcpCredentialKey, mcpDiscoveryFileName, redactTokenBody, resolveClientMetadataUrl, retryWithBackoff, withTimeout, wrapFetchWithTimeout } from './oauth-auth-server-metadata';
export type { AuthServerMetadata, DiscoveryState, DiscoveryStateStore, FetchLike, OAuthLogEvent, OAuthLogger, OAuthTokens, PkcePair, ProtectedResourceMetadata, RetryOptions } from './oauth-auth-server-metadata';
export { CLIENT_AUTH_METHODS, ID_JAG_TOKEN_TYPE, ID_TOKEN_TYPE, JWT_BEARER_GRANT, MAX_REFRESH_FAILURES, MCP_CLIENT_SECRET_ENV, MCP_ENABLE_XAA_ENV, TOKEN_EXCHANGE_GRANT, coerceExpiresIn, decodeJwtExpiry, discoverProtectedResource, generatePkce, generateState, isXaaEnabled, parseWwwAuthenticate, pickClientAuthMethod, pickRegistrationAuthMethod, registerClient, resolveClientSecret, resolveScopeFromMetadata, toTokens, unionScopes } from './oauth-mcp-o-auth-provider';
export type { ClientAuthMethod, JwtAuthGrantResult, McpOAuthProvider, XaaConfig, XaaResult } from './oauth-mcp-o-auth-provider';
export { FileOAuthTokenStore, OAuthTokenStore, mcpTokenFileName } from './oauth-mcp-o-auth-session-options';
export type { AuthorizeFn, AuthorizeResult, FileOAuthTokenStoreOptions, McpOAuthSessionOptions, RefreshSchedulerTimers } from './oauth-mcp-o-auth-session-options';

/**
 * Default overall timeout (ms) for the INTERACTIVE consent leg of the authorization
 * flow. The per-request timeout ({@link OAUTH_HTTP_TIMEOUT_MS}) bounds each HTTP
 * call, but the browser-consent leg is an out-of-band human step with no HTTP
 * request to time out — if the user never completes it (closes the tab, loses the
 * redirect) the `authorize()` promise would hang forever. This 5-minute cap
 * (parity with the reference's `Authentication timeout`) tears down a stuck consent leg.
 */
export const OAUTH_FLOW_TIMEOUT_MS = 5 * 60 * 1000;

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

/**
 * Single-target proactive token-refresh scheduler (parity with the reference's
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
    // env var (the reference-parity readClientSecret precedence) so the secret need not be
    // inlined into the plaintext server config.
    this.effectiveClientSecret = resolveClientSecret(opts.clientSecret, opts.env);
    // clientId-change invalidation (parity with the reference's xaaIdpCommand): if a static
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
   * the network POST entirely (re-read-after-lock dedup — parity with the reference's
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
    // Reactive clock-drift recovery (parity with the reference's withOAuth401Retry): the
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
   * Granular credential invalidation (parity with the reference's
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
    // endpoint) before re-consenting, matching the reference's revoke-then-reauth.
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
