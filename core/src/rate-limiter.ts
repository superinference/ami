import { parseRateLimitHeaders } from './rate-limit-parser';

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
}

const WARN_THRESHOLD = 0.80;

export class RateLimitTracker {
  private requests: RateLimitBucket | null = null;
  private tokens: RateLimitBucket | null = null;

  update(headers: Record<string, string>): void {
    const info = parseRateLimitHeaders(headers);
    const reqReset = headers['x-ratelimit-reset-requests'];

    if (info.limitRequests !== null && info.remainingRequests !== null) {
      this.requests = {
        limit: info.limitRequests,
        remaining: info.remainingRequests,
        // Prefer the shared parser's resetAt; fall back to parseDuration for
        // ms/m/h/compound formats the parser does not decode.
        resetAt: info.resetAt
          ? info.resetAt.getTime()
          : (reqReset ? Date.now() + this.parseDuration(reqReset) : 0),
      };
    }

    const tokReset = headers['x-ratelimit-reset-tokens'];

    if (info.limitTokens !== null && info.remainingTokens !== null) {
      this.tokens = {
        limit: info.limitTokens,
        remaining: info.remainingTokens,
        resetAt: tokReset ? Date.now() + this.parseDuration(tokReset) : 0,
      };
    }
  }

  getStatus(): RateLimitStatus {
    const reqPct = this.requests
      ? (this.requests.limit > 0 ? (this.requests.limit - this.requests.remaining) / this.requests.limit : 1)
      : 0;
    const tokPct = this.tokens
      ? (this.tokens.limit > 0 ? (this.tokens.limit - this.tokens.remaining) / this.tokens.limit : 1)
      : 0;

    return {
      requests: this.requests,
      tokens: this.tokens,
      requestsPercent: Math.round(reqPct * 100),
      tokensPercent: Math.round(tokPct * 100),
      shouldWarn: reqPct >= WARN_THRESHOLD || tokPct >= WARN_THRESHOLD,
    };
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
    return found ? totalMs : 60000;
  }
}
