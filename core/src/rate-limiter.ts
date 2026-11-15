import { parseRateLimitHeaders, formatResetTime } from './rate-limit-parser';

export interface RateLimitBucket {
  limit: number;
  remaining: number;
  resetAt: number;
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
  private readonly warnThreshold: number;
  private readonly errorThreshold: number;

  constructor(options: RateLimitTrackerOptions = {}) {
    this.warnThreshold = clampFraction(options.warnThreshold, WARN_THRESHOLD);
    this.errorThreshold = clampFraction(options.errorThreshold, ERROR_THRESHOLD);
  }

  update(headers: Record<string, string>): void {
    const info = parseRateLimitHeaders(headers);
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
    const shouldWarn = reqWarn || tokWarn;

    const status: RateLimitStatus = {
      requests: this.requests,
      tokens: this.tokens,
      requestsPercent: Math.round(reqPct * 100),
      tokensPercent: Math.round(tokPct * 100),
      shouldWarn,
    };

    if (shouldWarn) {
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
    }

    return status;
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
