import {
  parseRateLimitHeaders,
  formatResetTime,
  computeEarlyWarning,
  getRateLimitDisplayName,
  DEFAULT_EARLY_WARNING_CONFIGS,
  type UnifiedWindow,
  type EarlyWarningConfig,
} from './rate-limit-parser';

export interface RateLimitBucket {
  limit: number;
  remaining: number;
  resetAt: number;
}

/** Overage/spillover tier state parsed from the unified rate-limit headers. */
export interface RateLimitOverage {
  /** Overage tier status, typically `'allowed'` or `'rejected'`. */
  status: string;
  /** Overage reset instant (Unix epoch SECONDS), or null when the server omitted it. */
  resetsAt: number | null;
}

/** Raw utilization of a single rate-limit window (parity: Claude's RawWindowUtilization). */
export interface RawWindowUtilization {
  /** Utilization fraction in [0, 1]. */
  utilization: number;
  /** Reset instant as a Unix epoch in SECONDS, or null when unknown. */
  resetsAt: number | null;
}

/**
 * Raw per-window utilization snapshot, tracked on EVERY response (parity: Claude's
 * getRawUtilization). Exposed to statusline scripts and diagnostics so a window's
 * burn can be surfaced without waiting for a warning threshold to fire.
 */
export interface RawUtilization {
  requests?: RawWindowUtilization;
  tokens?: RawWindowUtilization;
}

export interface RateLimitStatus {
  requests: RateLimitBucket | null;
  tokens: RateLimitBucket | null;
  requestsPercent: number;
  tokensPercent: number;
  shouldWarn: boolean;
  /** Severity of the warning, when shouldWarn is true. */
  severity?: 'warning' | 'error';
  /** User-facing, graded warning message, when shouldWarn is true. */
  warnMessage?: string;
  /**
   * Server-directed cooldown (ms) parsed from the most recent response's
   * `Retry-After` header (null when the server did not direct one). Authoritative:
   * callers prefer this over a locally-guessed backoff (e.g. the coordinator feeds
   * it into credential-pool cooldowns). Cleared whenever a response arrives without
   * the header, so it only reflects the latest directive.
   */
  retryAfterMs: number | null;
  /**
   * Overage/spillover tier (parity: Anthropic unified overage headers), when the
   * server reported one. Present means a two-tier limit is in effect; see
   * {@link RateLimitStatus.isUsingOverage}.
   */
  overage?: RateLimitOverage;
  /**
   * True when the primary tier is rejected but the overage tier still permits the
   * request — i.e. the session is currently drawing on spillover quota.
   */
  isUsingOverage?: boolean;
  /** Scope the unified limit applies to (e.g. `'seven_day_opus'`), when reported. */
  representativeClaim?: string;
  /**
   * True when `shouldWarn` was raised PROACTIVELY by a unified-window early
   * warning (burning quota faster than the time window allows) rather than by a
   * per-bucket utilization/burn threshold. Lets consumers phrase it as a
   * heads-up rather than an at-limit alert. Absent when no warning, or when the
   * warning came from the per-bucket path.
   */
  proactive?: boolean;
}

/** Tunable thresholds for a {@link RateLimitTracker}. */
export interface RateLimitTrackerOptions {
  /**
   * Utilization fraction (0–1) at/above which {@link RateLimitTracker.getStatus}
   * sets `shouldWarn`. Defaults to {@link WARN_THRESHOLD} (0.80). Out-of-range or
   * non-finite values fall back to the default.
   */
  warnThreshold?: number;
  /**
   * Utilization fraction (0–1) at/above which the warning severity is `error`
   * rather than `warning`. Defaults to {@link ERROR_THRESHOLD} (0.95).
   */
  errorThreshold?: number;
  /**
   * Proactive early-warning ladders keyed by unified-window (parity: Claude's
   * EARLY_WARNING_CONFIGS). Defaults to {@link DEFAULT_EARLY_WARNING_CONFIGS}.
   * Supply custom windows/thresholds to tune when a proactive heads-up fires;
   * an empty array disables proactive early warnings entirely.
   */
  earlyWarningConfigs?: readonly EarlyWarningConfig[];
}

/** Default utilization at/above which a warning is produced. */
const WARN_THRESHOLD = 0.80;
/** Default utilization at/above which the warning is an error rather than a warning. */
const ERROR_THRESHOLD = 0.95;
/** Never trust a server-supplied reset further than this into the future. */
const MAX_RESET_MS = 24 * 60 * 60 * 1000;

/**
 * Coerce a caller-supplied threshold to a usable utilization fraction. Undefined
 * / non-finite values fall back to the default; in-range values are clamped to
 * [0, 1] so a hostile config can never disable or invert warning logic.
 */
function clampFraction(value: number | undefined, fallback: number): number {
  if (typeof value !== 'number' || !Number.isFinite(value)) return fallback;
  return Math.max(0, Math.min(1, value));
}

// Burn-rate threshold: warn when utilization is high relative to how little of
// the reset window has elapsed (mirrors Claude's getTimeRelativeEarlyWarning
// conservative 5h-window tier). We derive the window from observed resets rather
// than a fixed provider window, so only the conservative tier is applied — the
// aggressive 7d-style low tiers would false-positive on a single observation
// where observed-window == time-remaining.
const BURN_RATE_THRESHOLDS: Array<{ utilization: number; timePct: number }> = [
  { utilization: 0.9, timePct: 0.72 },
];

export class RateLimitTracker {
  private requests: RateLimitBucket | null = null;
  private tokens: RateLimitBucket | null = null;
  // Largest reset window ever observed per bucket (full window right after a
  // reset), used to estimate how much of the window has elapsed for burn-rate.
  private requestsWindowMs = 0;
  private tokensWindowMs = 0;
  // Server-directed Retry-After (ms) from the most recent response, or null.
  private retryAfterMs: number | null = null;
  // Unified two-tier state from the most recent response (parity: Anthropic
  // unified rate-limit headers). Null entries mean the server reported none.
  private unifiedStatus: string | null = null;
  private overage: RateLimitOverage | null = null;
  private representativeClaim: string | null = null;
  // Unified per-window utilization snapshots from the most recent response, used
  // for proactive early warnings. Cleared on a response that reports none.
  private unifiedWindows: UnifiedWindow[] = [];
  private readonly warnThreshold: number;
  private readonly errorThreshold: number;
  private readonly earlyWarningConfigs: readonly EarlyWarningConfig[];

  constructor(options: RateLimitTrackerOptions = {}) {
    this.warnThreshold = clampFraction(options.warnThreshold, WARN_THRESHOLD);
    this.errorThreshold = clampFraction(options.errorThreshold, ERROR_THRESHOLD);
    this.earlyWarningConfigs = options.earlyWarningConfigs ?? DEFAULT_EARLY_WARNING_CONFIGS;
  }

  update(headers: Record<string, string>): void {
    const info = parseRateLimitHeaders(headers);
    // Capture the server's Retry-After directive from this response (null when
    // absent). It is a per-response directive, so a later response without the
    // header clears it rather than letting a stale directive linger.
    this.retryAfterMs = info.retryAfterMs;
    // Capture the unified two-tier status (parity: Anthropic unified headers). A
    // response without these clears them so a stale directive never lingers.
    this.unifiedStatus = info.unifiedStatus ?? null;
    this.representativeClaim = info.representativeClaim ?? null;
    // Capture unified per-window utilization for proactive early warnings; a
    // response without these clears them so a stale snapshot never lingers.
    this.unifiedWindows = info.unifiedWindows ?? [];
    this.overage = info.overageStatus
      ? {
          status: info.overageStatus,
          resetsAt: info.overageResetAt
            ? Math.floor(this.clampResetAt(info.overageResetAt.getTime()) / 1000)
            : null,
        }
      : null;
    const reqReset = headers['x-ratelimit-reset-requests'];

    if (info.limitRequests !== null && info.remainingRequests !== null) {
      this.requests = {
        limit: info.limitRequests,
        remaining: info.remainingRequests,
        // Prefer the shared parser's resetAt; fall back to parseDuration for
        // ms/m/h/compound formats the parser does not decode. Either way the
        // absolute instant is clamped so a hostile far-future (or absolute
        // epoch) reset can't skew burn-rate/status forever.
        resetAt: this.clampResetAt(
          info.resetAt
            ? info.resetAt.getTime()
            : (reqReset ? Date.now() + this.parseDuration(reqReset) : 0),
        ),
      };
      this.requestsWindowMs = Math.max(this.requestsWindowMs, this.requests.resetAt - Date.now());
    }

    const tokReset = headers['x-ratelimit-reset-tokens'];

    if (info.limitTokens !== null && info.remainingTokens !== null) {
      this.tokens = {
        limit: info.limitTokens,
        remaining: info.remainingTokens,
        resetAt: this.clampResetAt(
          info.resetAtTokens
            ? info.resetAtTokens.getTime()
            : (tokReset ? Date.now() + this.parseDuration(tokReset) : 0),
        ),
      };
      this.tokensWindowMs = Math.max(this.tokensWindowMs, this.tokens.resetAt - Date.now());
    }
  }

  getStatus(): RateLimitStatus {
    const reqPct = this.requests
      ? (this.requests.limit > 0 ? (this.requests.limit - this.requests.remaining) / this.requests.limit : 1)
      : 0;
    const tokPct = this.tokens
      ? (this.tokens.limit > 0 ? (this.tokens.limit - this.tokens.remaining) / this.tokens.limit : 1)
      : 0;

    const reqBurn = this.burnRateWarns(this.requests, this.requestsWindowMs, reqPct);
    const tokBurn = this.burnRateWarns(this.tokens, this.tokensWindowMs, tokPct);

    const reqWarn = reqPct >= this.warnThreshold || reqBurn;
    const tokWarn = tokPct >= this.warnThreshold || tokBurn;
    const bucketWarn = reqWarn || tokWarn;

    // Proactive early warning from the unified per-window utilization headers
    // (parity: Claude's getEarlyWarningFromHeaders). Uses the FIXED provider
    // window, so the aggressive 7-day tiers — which the observed-window burn-rate
    // heuristic above cannot evaluate — can fire here.
    const earlyWarn = computeEarlyWarning(this.unifiedWindows, this.earlyWarningConfigs);
    const shouldWarn = bucketWarn || earlyWarn !== null;

    const status: RateLimitStatus = {
      requests: this.requests,
      tokens: this.tokens,
      requestsPercent: Math.round(reqPct * 100),
      tokensPercent: Math.round(tokPct * 100),
      shouldWarn,
      retryAfterMs: this.retryAfterMs,
    };

    if (this.overage) {
      status.overage = this.overage;
      // Soft overage: the primary tier is rejected but spillover quota remains.
      status.isUsingOverage = this.unifiedStatus === 'rejected' && this.overage.status === 'allowed';
    }
    if (this.representativeClaim) status.representativeClaim = this.representativeClaim;

    if (bucketWarn) {
      // Surface whichever dimension is closer to its limit.
      const useReq = reqWarn && (!tokWarn || reqPct >= tokPct);
      const pct = useReq ? reqPct : tokPct;
      const dimension = useReq ? 'request' : 'token';
      const bucket = useReq ? this.requests : this.tokens;
      status.severity = pct >= this.errorThreshold ? 'error' : 'warning';
      const resetText = bucket && bucket.resetAt > Date.now()
        ? formatResetTime(Math.floor(bucket.resetAt / 1000))
        : undefined;
      status.warnMessage =
        `You've used ${Math.round(pct * 100)}% of your ${dimension} rate limit` +
        (resetText ? ` — resets ${resetText}.` : '.');
    } else if (earlyWarn) {
      // Proactive heads-up: high utilization early in a known window.
      status.proactive = true;
      const util = earlyWarn.utilization;
      status.severity = util !== null && util >= this.errorThreshold ? 'error' : 'warning';
      const resetText = earlyWarn.resetAt && earlyWarn.resetAt.getTime() > Date.now()
        ? formatResetTime(Math.floor(earlyWarn.resetAt.getTime() / 1000))
        : undefined;
      const usedClause = util !== null ? `used ${Math.round(util * 100)}% of ` : 'nearly reached ';
      status.warnMessage =
        `Heads up: you've ${usedClause}your ${getRateLimitDisplayName(earlyWarn.rateLimitType)}` +
        (resetText ? ` — resets ${resetText}.` : '.');
    }

    return status;
  }

  /**
   * Raw per-window utilization from the most recent response, tracked on EVERY
   * update (unlike {@link RateLimitStatus.warnMessage}, which only materializes
   * once a warning threshold fires). Each bucket reports a utilization fraction in
   * [0, 1] plus its reset instant (Unix epoch SECONDS); a bucket never observed is
   * omitted. Surfaced to statusline scripts/diagnostics (parity: Claude's
   * getRawUtilization), so the current burn is visible without a warning pending.
   */
  getRawUtilization(): RawUtilization {
    const result: RawUtilization = {};
    const toWindow = (bucket: RateLimitBucket): RawWindowUtilization => ({
      utilization: bucket.limit > 0
        ? Math.max(0, Math.min(1, (bucket.limit - bucket.remaining) / bucket.limit))
        : 1,
      resetsAt: bucket.resetAt > 0 ? Math.floor(bucket.resetAt / 1000) : null,
    });
    if (this.requests) result.requests = toWindow(this.requests);
    if (this.tokens) result.tokens = toWindow(this.tokens);
    return result;
  }

  /**
   * True when utilization is high relative to how little of the reset window has
   * elapsed (burning quota faster than the window). Needs a known window size.
   */
  private burnRateWarns(bucket: RateLimitBucket | null, windowMs: number, utilization: number): boolean {
    if (!bucket || windowMs <= 0) return false;
    const remainingMs = bucket.resetAt - Date.now();
    if (remainingMs <= 0) return false;
    const timeProgress = Math.max(0, Math.min(1, 1 - remainingMs / windowMs));
    return BURN_RATE_THRESHOLDS.some(t => utilization >= t.utilization && timeProgress <= t.timePct);
  }

  /**
   * Bound an absolute reset instant (ms epoch). A server (or a parser decoding an
   * absolute epoch header) can report a reset arbitrarily far in the future; cap
   * it at MAX_RESET_MS from now so burn-rate windows and status never trust an
   * unbounded horizon. Past / zero instants are preserved as-is (0 is the
   * "unknown" sentinel the rest of the class already expects; both upstream
   * paths — parsed Date and parseDuration — always yield a finite value).
   */
  private clampResetAt(resetAtMs: number): number {
    const now = Date.now();
    return resetAtMs > now ? Math.min(resetAtMs, now + MAX_RESET_MS) : resetAtMs;
  }

  private parseDuration(value: string): number {
    const matches = value.matchAll(/(\d+)(ms|s|m|h)?/g);
    let totalMs = 0;
    let found = false;
    for (const match of matches) {
      found = true;
      const num = parseInt(match[1], 10);
      switch (match[2]) {
        case 'ms': totalMs += num; break;
        case 's': totalMs += num * 1000; break;
        case 'm': totalMs += num * 60000; break;
        case 'h': totalMs += num * 3600000; break;
        default: totalMs += num * 1000; break;
      }
    }
    if (!found) return 60000;
    // Bound a hostile/buggy server duration: never trust a reset more than 24h
    // out (an unbounded resetAt would skew burn-rate and status forever).
    if (!Number.isFinite(totalMs) || totalMs < 0) return 60000;
    return Math.min(totalMs, MAX_RESET_MS);
  }
}
