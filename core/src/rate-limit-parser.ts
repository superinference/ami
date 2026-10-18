export interface RateLimitInfo {
  retryAfterMs: number | null;
  remainingRequests: number | null;
  remainingTokens: number | null;
  resetAt: Date | null;
  limitRequests: number | null;
  limitTokens: number | null;
}

export function parseRateLimitHeaders(headers: Record<string, string | string[] | undefined>): RateLimitInfo {
  const get = (name: string): string | undefined => {
    const v = headers[name] ?? headers[name.toLowerCase()];
    return Array.isArray(v) ? v[0] : v;
  };

  return {
    retryAfterMs: parseRetryAfterHeader(get('retry-after')),
    remainingRequests: parseIntHeader(get('x-ratelimit-remaining-requests') ?? get('ratelimit-remaining')),
    remainingTokens: parseIntHeader(get('x-ratelimit-remaining-tokens')),
    resetAt: parseResetHeader(get('x-ratelimit-reset-requests') ?? get('x-ratelimit-reset')),
    limitRequests: parseIntHeader(get('x-ratelimit-limit-requests') ?? get('ratelimit-limit')),
    limitTokens: parseIntHeader(get('x-ratelimit-limit-tokens')),
  };
}

export function parseRetryAfterHeader(value: string | undefined): number | null {
  if (!value) return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const seconds = Number(trimmed);
  if (!Number.isNaN(seconds)) {
    if (seconds < 0 || seconds > 86400) return null;
    return Math.ceil(seconds * 1000);
  }
  const date = new Date(trimmed);
  if (!Number.isNaN(date.getTime())) {
    const deltaMs = date.getTime() - Date.now();
    return Math.max(0, Math.ceil(deltaMs));
  }
  return null;
}

function parseIntHeader(value: string | undefined): number | null {
  if (!value) return null;
  const n = parseInt(value.trim(), 10);
  return Number.isNaN(n) ? null : n;
}

function parseResetHeader(value: string | undefined): Date | null {
  if (!value) return null;
  const secMatch = value.trim().match(/^(\d+(?:\.\d+)?)s?$/);
  if (secMatch) {
    return new Date(Date.now() + parseFloat(secMatch[1]) * 1000);
  }
  const date = new Date(value.trim());
  return Number.isNaN(date.getTime()) ? null : date;
}

export function shouldBackoff(info: RateLimitInfo): boolean {
  if (info.retryAfterMs !== null && info.retryAfterMs > 0) return true;
  if (info.remainingRequests !== null && info.remainingRequests <= 0) return true;
  if (info.remainingTokens !== null && info.remainingTokens <= 0) return true;
  return false;
}

export function suggestedWaitMs(info: RateLimitInfo, defaultMs: number = 1000): number {
  if (info.retryAfterMs !== null && info.retryAfterMs > 0) return info.retryAfterMs;
  if (info.resetAt !== null) {
    const delta = info.resetAt.getTime() - Date.now();
    if (delta > 0) return delta;
  }
  return defaultMs;
}
