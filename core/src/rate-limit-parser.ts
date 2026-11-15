export interface RateLimitInfo {
  retryAfterMs: number | null;
  remainingRequests: number | null;
  remainingTokens: number | null;
  resetAt: Date | null;
  resetAtTokens: Date | null;
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
    resetAtTokens: parseResetHeader(get('x-ratelimit-reset-tokens')),
    limitRequests: parseIntHeader(get('x-ratelimit-limit-requests') ?? get('ratelimit-limit')),
    limitTokens: parseIntHeader(get('x-ratelimit-limit-tokens')),
  };
}

/** 24h ceiling on a server-directed retry-after (both numeric and HTTP-date forms). */
const MAX_RETRY_AFTER_SECONDS = 86400;
const MAX_RETRY_AFTER_MS = MAX_RETRY_AFTER_SECONDS * 1000;

export function parseRetryAfterHeader(value: string | undefined): number | null {
  if (!value) return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const seconds = Number(trimmed);
  if (!Number.isNaN(seconds)) {
    if (seconds < 0 || seconds > MAX_RETRY_AFTER_SECONDS) return null;
    return Math.ceil(seconds * 1000);
  }
  const date = new Date(trimmed);
  if (!Number.isNaN(date.getTime())) {
    const deltaMs = date.getTime() - Date.now();
    // Apply the same upper bound to the HTTP-date branch as the numeric branch:
    // a far-future `retry-after` date must not bypass the 24h ceiling and yield
    // an unbounded backoff.
    if (deltaMs > MAX_RETRY_AFTER_MS) return null;
    return Math.max(0, Math.ceil(deltaMs));
  }
  return null;
}

function parseIntHeader(value: string | undefined): number | null {
  if (!value) return null;
  const n = parseInt(value.trim(), 10);
  return Number.isNaN(n) ? null : n;
}

/**
 * A numeric reset value at/above this (unix-epoch seconds, ~year 2001) is an
 * ABSOLUTE timestamp, not a relative delta. Any genuine relative reset delay is
 * orders of magnitude smaller, so the magnitude disambiguates the two.
 */
const EPOCH_THRESHOLD_SECONDS = 1_000_000_000;

function parseResetHeader(value: string | undefined): Date | null {
  if (!value) return null;
  // eslint-disable-next-line security/detect-unsafe-regex -- fully anchored, linear
  const secMatch = value.trim().match(/^(\d+(?:\.\d+)?)s?$/);
  if (secMatch) {
    const n = parseFloat(secMatch[1]);
    // An absolute epoch-seconds reset (e.g. Anthropic's unified-reset header)
    // must be interpreted as the reset instant itself — treating it as a relative
    // delta would resolve ~54 years in the future and effectively hang the client.
    if (n >= EPOCH_THRESHOLD_SECONDS) {
      return new Date(n * 1000);
    }
    return new Date(Date.now() + n * 1000);
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

/** Local IANA timezone abbreviation/name for reset-time display. */
function getTimeZone(): string {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || 'local';
  } catch {
    return 'local';
  }
}

/**
 * Render a rate-limit reset timestamp as human-readable text (canonical helper;
 * mirrors Claude's formatResetTime). A reset within 24h shows just a lowercase
 * am/pm clock time ("3:45pm"); a reset further out also shows the date
 * ("Nov 17, 3pm"), adding the year when it differs from the current one.
 *
 * `timestampInSeconds` is a Unix epoch in SECONDS (matching provider reset
 * headers). Returns undefined for a missing/zero timestamp so callers can skip
 * the "resets at …" clause entirely.
 */
export function formatResetTime(
  timestampInSeconds: number | undefined,
  showTimezone = false,
  showTime = true,
): string | undefined {
  if (!timestampInSeconds) return undefined;
  const date = new Date(timestampInSeconds * 1000);
  const now = new Date();
  const minutes = date.getMinutes();
  const hoursUntilReset = (date.getTime() - now.getTime()) / (1000 * 60 * 60);

  const stripSpaceBeforeMeridiem = (s: string): string =>
    s.replace(/ ([AP]M)/i, (_m, ampm: string) => ampm.toLowerCase());
  const tz = showTimezone ? ` (${getTimeZone()})` : '';

  if (hoursUntilReset > 24) {
    const dateOptions: Intl.DateTimeFormatOptions = {
      month: 'short',
      day: 'numeric',
      hour: showTime ? 'numeric' : undefined,
      minute: !showTime || minutes === 0 ? undefined : '2-digit',
      hour12: showTime ? true : undefined,
    };
    if (date.getFullYear() !== now.getFullYear()) dateOptions.year = 'numeric';
    return stripSpaceBeforeMeridiem(date.toLocaleString('en-US', dateOptions)) + tz;
  }

  const timeString = date.toLocaleTimeString('en-US', {
    hour: 'numeric',
    minute: minutes === 0 ? undefined : '2-digit',
    hour12: true,
  });
  return stripSpaceBeforeMeridiem(timeString) + tz;
}

/**
 * Human-readable reset clause for a parsed RateLimitInfo. Prefers the request
 * reset, then the token reset. Returns undefined when neither is known.
 */
export function formatRateLimitReset(info: RateLimitInfo, showTimezone = false): string | undefined {
  const reset = info.resetAt ?? info.resetAtTokens;
  if (!reset) return undefined;
  return formatResetTime(Math.floor(reset.getTime() / 1000), showTimezone);
}
