// Split from oauth.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { createHash } from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import type { DiscoveryStateStore, FetchLike, OAuthLogger, OAuthTokens, RetryOptions } from './oauth-auth-server-metadata';
import type { XaaConfig } from './oauth-mcp-o-auth-provider';

/** Injectable timer seams for {@link OAuthRefreshScheduler} (deterministic tests). */
export interface RefreshSchedulerTimers {
  setTimer?: (fn: () => void, ms: number) => unknown;
  clearTimer?: (handle: unknown) => void;
  now?: () => number;
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
   * (older than `lockStaleMs`), and — matching the reference — proceeds WITHOUT the
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
