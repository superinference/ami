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

export type FetchLike = (url: string, init?: {
  method?: string;
  headers?: Record<string, string>;
  body?: string;
}) => Promise<{ ok: boolean; status: number; json: () => Promise<unknown>; headers?: { get(name: string): string | null } }>;

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
  codeChallengeMethodsSupported?: string[];
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
}

const base64url = (buf: Buffer): string =>
  buf.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

/** RFC 7636 PKCE pair (S256). randomness/hash come from node:crypto. */
export function generatePkce(): PkcePair {
  const verifier = base64url(randomBytes(32));
  const challenge = base64url(createHash('sha256').update(verifier).digest());
  return { verifier, challenge, method: 'S256' };
}

/** A crypto-random state/nonce value for the authorization request. */
export function generateState(): string {
  return base64url(randomBytes(16));
}

/**
 * Parse an HTTP 401 `WWW-Authenticate: Bearer ...` header. Returns the
 * resource-metadata URL (RFC 9728) and any error/realm fields it carries.
 */
export function parseWwwAuthenticate(header: string | null | undefined): {
  resourceMetadata?: string;
  realm?: string;
  error?: string;
} {
  if (!header) return {};
  const out: { resourceMetadata?: string; realm?: string; error?: string } = {};
  // Match key="value" pairs (RFC 7235 auth-param list).
  const re = /(\w[\w-]*)\s*=\s*"([^"]*)"/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(header)) !== null) {
    const key = m[1].toLowerCase();
    if (key === 'resource_metadata') out.resourceMetadata = m[2];
    else if (key === 'realm') out.realm = m[2];
    else if (key === 'error') out.error = m[2];
  }
  return out;
}

/** Build a `.well-known` discovery URL for a base URL + suffix. */
export function wellKnown(baseUrl: string, suffix: string): string {
  const u = new URL(baseUrl);
  const base = `${u.protocol}//${u.host}`;
  return `${base}/.well-known/${suffix}`;
}

async function getJson(fetchImpl: FetchLike, url: string): Promise<Record<string, unknown>> {
  const res = await fetchImpl(url, { headers: { Accept: 'application/json' } });
  if (!res.ok) throw new Error(`OAuth discovery failed (${res.status}) for ${url}`);
  return (await res.json()) as Record<string, unknown>;
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

/** RFC 8414 — fetch authorization-server metadata from an issuer URL. */
export async function discoverAuthServer(fetchImpl: FetchLike, issuer: string): Promise<AuthServerMetadata> {
  const doc = await getJson(fetchImpl, wellKnown(issuer, 'oauth-authorization-server'));
  const authorizationEndpoint = doc.authorization_endpoint;
  const tokenEndpoint = doc.token_endpoint;
  if (typeof authorizationEndpoint !== 'string' || typeof tokenEndpoint !== 'string') {
    throw new Error('authorization-server metadata missing endpoints');
  }
  return {
    issuer: typeof doc.issuer === 'string' ? doc.issuer : issuer,
    authorizationEndpoint,
    tokenEndpoint,
    registrationEndpoint: typeof doc.registration_endpoint === 'string' ? doc.registration_endpoint : undefined,
    scopesSupported: Array.isArray(doc.scopes_supported) ? (doc.scopes_supported as string[]) : undefined,
    codeChallengeMethodsSupported: Array.isArray(doc.code_challenge_methods_supported)
      ? (doc.code_challenge_methods_supported as string[]) : undefined,
  };
}

/** RFC 7591 — register a client dynamically; returns the issued client_id. */
export async function registerClient(
  fetchImpl: FetchLike,
  registrationEndpoint: string,
  redirectUris: string[],
  clientName = 'SuperInference AMI',
): Promise<{ clientId: string; clientSecret?: string }> {
  const res = await fetchImpl(registrationEndpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify({
      client_name: clientName,
      redirect_uris: redirectUris,
      grant_types: ['authorization_code', 'refresh_token'],
      response_types: ['code'],
      token_endpoint_auth_method: 'none',
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
  return u.toString();
}

function toTokens(doc: Record<string, unknown>, now: number): OAuthTokens {
  if (typeof doc.access_token !== 'string') throw new Error('token response missing access_token');
  const expiresIn = typeof doc.expires_in === 'number' ? doc.expires_in : undefined;
  return {
    accessToken: doc.access_token,
    tokenType: typeof doc.token_type === 'string' ? doc.token_type : 'Bearer',
    refreshToken: typeof doc.refresh_token === 'string' ? doc.refresh_token : undefined,
    expiresAt: expiresIn !== undefined ? now + expiresIn * 1000 : undefined,
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
  if (!res.ok) throw new Error(`token endpoint error (${res.status})`);
  return toTokens((await res.json()) as Record<string, unknown>, now);
}

/** Exchange an authorization code for tokens (PKCE + resource indicator). */
export function exchangeCode(
  fetchImpl: FetchLike,
  params: { tokenEndpoint: string; code: string; verifier: string; clientId: string; redirectUri: string; resource: string },
  now: number,
): Promise<OAuthTokens> {
  return postForm(fetchImpl, params.tokenEndpoint, {
    grant_type: 'authorization_code',
    code: params.code,
    code_verifier: params.verifier,
    client_id: params.clientId,
    redirect_uri: params.redirectUri,
    resource: params.resource,
  }, now);
}

/** Exchange a refresh token for fresh tokens. */
export function refreshToken(
  fetchImpl: FetchLike,
  params: { tokenEndpoint: string; refreshToken: string; clientId: string; resource: string },
  now: number,
): Promise<OAuthTokens> {
  return postForm(fetchImpl, params.tokenEndpoint, {
    grant_type: 'refresh_token',
    refresh_token: params.refreshToken,
    client_id: params.clientId,
    resource: params.resource,
  }, now);
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
 * Interactive consent leg, injected by the frontend. Given the authorization URL
 * it must drive the user through login (open a browser, capture the redirect) and
 * return the authorization `code`. In headless/detached runs the frontend passes
 * an implementation that throws with guidance — the documented degraded path.
 */
export type AuthorizeFn = (params: { authorizationUrl: string; redirectUri: string; state: string }) => Promise<{ code: string }>;

export interface McpOAuthSessionOptions {
  /** The MCP server URL — used as the RFC 8707 resource and for well-known fallback. */
  serverUrl: string;
  fetchImpl: FetchLike;
  authorize: AuthorizeFn;
  redirectUri: string;
  /** Pre-registered client id; when absent the session uses Dynamic Client Registration. */
  clientId?: string;
  scope?: string;
  /** Clock seam (epoch ms). Defaults to Date.now. */
  now?: () => number;
  store?: OAuthTokenStore;
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
  private resource: string;
  private authMeta: AuthServerMetadata | null = null;
  private effectiveClientId: string | undefined;

  constructor(private readonly opts: McpOAuthSessionOptions) {
    this.store = opts.store ?? new OAuthTokenStore();
    this.now = opts.now ?? (() => Date.now());
    this.resource = opts.serverUrl;
    this.effectiveClientId = opts.clientId;
  }

  async authorizationHeader(): Promise<string | null> {
    const live = this.store.authorizationHeader(this.now());
    if (live) return live;
    // Token present but expired, and we can refresh without user interaction.
    if (this.store.hasRefreshToken() && this.authMeta && this.effectiveClientId) {
      try {
        const refreshed = await refreshToken(this.opts.fetchImpl, {
          tokenEndpoint: this.authMeta.tokenEndpoint,
          refreshToken: this.store.get()!.refreshToken!,
          clientId: this.effectiveClientId,
          resource: this.resource,
        }, this.now());
        this.store.set(refreshed);
        return this.store.authorizationHeader(this.now());
      } catch {
        return null; // fall through to a full re-auth on the next 401
      }
    }
    return null;
  }

  async onUnauthorized(wwwAuthenticate: string | null): Promise<boolean> {
    try {
      const parsed = parseWwwAuthenticate(wwwAuthenticate);
      const metadataUrl = parsed.resourceMetadata ?? wellKnown(this.opts.serverUrl, 'oauth-protected-resource');
      const prm = await discoverProtectedResource(this.opts.fetchImpl, metadataUrl);
      this.resource = prm.resource;
      this.authMeta = await discoverAuthServer(this.opts.fetchImpl, prm.authorizationServers[0]);

      if (!this.effectiveClientId) {
        if (!this.authMeta.registrationEndpoint) throw new Error('no client_id and no registration endpoint');
        const reg = await registerClient(this.opts.fetchImpl, this.authMeta.registrationEndpoint, [this.opts.redirectUri]);
        this.effectiveClientId = reg.clientId;
      }

      const pkce = generatePkce();
      const state = generateState();
      const authorizationUrl = buildAuthorizationUrl({
        authorizationEndpoint: this.authMeta.authorizationEndpoint,
        clientId: this.effectiveClientId,
        redirectUri: this.opts.redirectUri,
        resource: this.resource,
        codeChallenge: pkce.challenge,
        state,
        scope: this.opts.scope ?? prm.scopesSupported?.join(' '),
      });
      const { code } = await this.opts.authorize({ authorizationUrl, redirectUri: this.opts.redirectUri, state });
      const tokens = await exchangeCode(this.opts.fetchImpl, {
        tokenEndpoint: this.authMeta.tokenEndpoint,
        code,
        verifier: pkce.verifier,
        clientId: this.effectiveClientId,
        redirectUri: this.opts.redirectUri,
        resource: this.resource,
      }, this.now());
      this.store.set(tokens);
      return true;
    } catch {
      return false;
    }
  }
}
