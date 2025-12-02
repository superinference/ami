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
    const reqLimit = parseInt(headers['x-ratelimit-limit-requests'] || '', 10);
    const reqRemaining = parseInt(headers['x-ratelimit-remaining-requests'] || '', 10);
    const reqReset = headers['x-ratelimit-reset-requests'];

    if (!isNaN(reqLimit) && !isNaN(reqRemaining)) {
      this.requests = {
        limit: reqLimit,
        remaining: reqRemaining,
        resetAt: reqReset ? Date.now() + this.parseDuration(reqReset) : 0,
      };
    }

    const tokLimit = parseInt(headers['x-ratelimit-limit-tokens'] || '', 10);
    const tokRemaining = parseInt(headers['x-ratelimit-remaining-tokens'] || '', 10);
    const tokReset = headers['x-ratelimit-reset-tokens'];

    if (!isNaN(tokLimit) && !isNaN(tokRemaining)) {
      this.tokens = {
        limit: tokLimit,
        remaining: tokRemaining,
        resetAt: tokReset ? Date.now() + this.parseDuration(tokReset) : 0,
      };
    }
  }

  getStatus(): RateLimitStatus {
    const reqPct = this.requests
      ? ((this.requests.limit - this.requests.remaining) / this.requests.limit)
      : 0;
    const tokPct = this.tokens
      ? ((this.tokens.limit - this.tokens.remaining) / this.tokens.limit)
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
    const match = value.match(/(\d+)(ms|s|m|h)?/);
    if (!match) return 60000;
    const num = parseInt(match[1], 10);
    switch (match[2]) {
      case 'ms': return num;
      case 's': return num * 1000;
      case 'm': return num * 60000;
      case 'h': return num * 3600000;
      default: return num * 1000;
    }
  }
}
