// Split from oauth.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { createHash, randomBytes } from 'crypto';
import { parseEnvBool } from '../utils/env';
import { DEFAULT_TOKEN_LIFETIME_SEC, OAuthError, extractOAuthErrorBody } from './oauth-auth-server-metadata';
import type { AuthServerMetadata, FetchLike, OAuthTokens, PkcePair, ProtectedResourceMetadata } from './oauth-auth-server-metadata';

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

export const base64url = (buf: Buffer): string =>
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

export async function getJson(fetchImpl: FetchLike, url: string): Promise<Record<string, unknown>> {
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

/** Client-auth method a confidential client uses at the token/revocation endpoint. */
export const CLIENT_AUTH_METHODS = ['client_secret_basic', 'client_secret_post', 'none'] as const;

export type ClientAuthMethod = (typeof CLIENT_AUTH_METHODS)[number];

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
 * (parity with the reference's `readClientSecret()` env precedence — auth.ts
 * `readClientSecret`). A secret provided here is read at connect time and NEVER
 * written into the plaintext server config, so it does not land in argv/`ps`,
 * shell history, or `.ami/mcp.json`.
 */
export const MCP_CLIENT_SECRET_ENV = 'MCP_CLIENT_SECRET';

/**
 * Resolve the effective confidential-client secret for an MCP OAuth session.
 *
 * Precedence (the reference-parity `readClientSecret`): an explicitly-configured secret
 * wins (it may itself be a `${secret:…}`/`${VAR}` reference the caller already
 * resolved); otherwise fall back to the {@link MCP_CLIENT_SECRET_ENV} environment
 * variable so a secret can be supplied via the environment instead of being
 * inlined into the plaintext server config. A blank / whitespace-only value at
 * either source is treated as absent (public client — no secret). Returns
 * `undefined` when neither source provides one.
 *
 * Unlike the reference's helper this never prompts on a TTY: connect-time resolution is
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
 * corporate IdP, so — matching the reference's `CLAUDE_CODE_ENABLE_XAA` gate — it is
 * OFF unless an operator explicitly opts in. Unlike the reference, AMI uses a vendor-neutral
 * name.
 */
export const MCP_ENABLE_XAA_ENV = 'MCP_ENABLE_XAA';

/**
 * Whether the browserless Cross-App Access flow is enabled for this process
 * ({@link MCP_ENABLE_XAA_ENV} is set to a truthy value). A session configured with
 * an {@link XaaConfig} still only takes the XAA path when this gate is on, so the
 * enterprise flow never activates for a normal user who merely has stale config.
 */
export function isXaaEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return parseEnvBool(env[MCP_ENABLE_XAA_ENV]);
}

/**
 * Input shape for the browserless Cross-App Access (XAA / SEP-990) token exchange.
 * Mirrors the reference's `XaaConfig`: the credentials registered at the MCP server's
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

/** Max consecutive failed refreshes before the scheduler stops retrying. */
export const MAX_REFRESH_FAILURES = 5;

/** Floor for any scheduled delay so a near-expiry token never tight-loops. */
export const MIN_REFRESH_DELAY_MS = 1000;
