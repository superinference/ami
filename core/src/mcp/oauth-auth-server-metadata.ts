// Split from oauth.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { createHash } from 'crypto';
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
 * proactive refresh/expiry schedule (parity with the reference's 3600s default).
 */
export const DEFAULT_TOKEN_LIFETIME_SEC = 3600;

/**
 * Env var (the reference-parity) overriding the Client ID Metadata Document URL used for
 * CIMD (SEP-991). When set, it takes precedence over any configured
 * `clientMetadataUrl`, so an operator can point the flow at a testing / FedStart
 * metadata document without editing server config.
 */
export const MCP_OAUTH_CLIENT_METADATA_URL_ENV = 'MCP_OAUTH_CLIENT_METADATA_URL';

/**
 * Resolve the Client ID Metadata Document URL (CIMD / SEP-991) for an OAuth flow:
 * the `MCP_OAUTH_CLIENT_METADATA_URL` env override wins, else the operator-configured
 * `clientMetadataUrl`, else undefined (CIMD disabled — fall back to Dynamic Client
 * Registration). Unlike the reference, AMI ships NO hardcoded default document (that
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
 * Nesting-depth-agnostic redaction of token-bearing JSON keys from an error body
 * before it reaches a debug log. Matches the quoted value of any of
 * `access_token`/`refresh_token`/`id_token`/`assertion`/`subject_token`/
 * `client_secret` at ANY depth, operating on both a raw `text()` body and a
 * parsed-then-stringified object — so a misbehaving auth server that echoes a
 * request's credential back inside a 4xx envelope cannot leak it into logs.
 */
export const SENSITIVE_TOKEN_RE = /"(access_token|refresh_token|id_token|assertion|subject_token|client_secret)"\s*:\s*"[^"]*"/g;

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
 * the reference's `getServerKey`.
 */
export function mcpCredentialKey(
  serverName: string,
  config: { type?: string; url?: string; headers?: Record<string, string> },
): string {
  const configJson = JSON.stringify({ type: config.type, url: config.url, headers: config.headers ?? {} });
  const hash = createHash('sha256').update(configJson).digest('hex').slice(0, 16);
  return `${serverName}|${hash}`;
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
 * reach the token endpoint. Parity with the reference's persisted `discoveryState`.
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
