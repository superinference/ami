export interface RateLimitInfo {
  retryAfterMs: number | null;
  remainingRequests: number | null;
  remainingTokens: number | null;
  resetAt: Date | null;
  resetAtTokens: Date | null;
  limitRequests: number | null;
  limitTokens: number | null;
  /**
   * Anthropic unified rate-limit tier status (`anthropic-ratelimit-unified-status`),
   * typically `'allowed'` or `'rejected'`; null when the server did not report one.
   * Drives the two-tier {@link isHardRateLimited} decision. Optional so hand-built
   * `RateLimitInfo` literals (which predate the two-tier fields) stay valid.
   */
  unifiedStatus?: string | null;
  /**
   * Overage/spillover tier status (`anthropic-ratelimit-unified-overage-status`).
   * When the primary tier is `'rejected'` but this is `'allowed'`, spillover quota
   * remains and the request is tolerated (soft tier) rather than hard-failed.
   */
  overageStatus?: string | null;
  /** Overage tier reset instant (`anthropic-ratelimit-unified-overage-reset`). */
  overageResetAt?: Date | null;
  /**
   * Scope the unified limit applies to (`anthropic-ratelimit-unified-representative-claim`),
   * e.g. `'seven_day_opus'`; null when absent.
   */
  representativeClaim?: string | null;
  /**
   * Per-window unified utilization snapshots parsed from the Anthropic
   * `anthropic-ratelimit-unified-<5h|7d>-{utilization,reset,surpassed-threshold}`
   * headers. Drives proactive early-warning ({@link computeEarlyWarning}). Empty
   * when the server reported no unified window headers. Optional so hand-built
   * literals stay valid.
   */
  unifiedWindows?: UnifiedWindow[];
}

/**
 * A single unified rate-limit window (parity: Claude's per-claim utilization
 * headers). `claimAbbrev` is the header segment (`'5h'` / `'7d'`); fields are
 * null when the corresponding header was absent.
 */
export interface UnifiedWindow {
  claimAbbrev: string;
  utilization: number | null;
  resetAt: Date | null;
  surpassedThreshold: number | null;
}

/** Abbreviations of the unified windows Anthropic reports utilization for. */
const UNIFIED_WINDOW_ABBREVS = ['5h', '7d'] as const;

export function parseRateLimitHeaders(headers: Record<string, string | string[] | undefined>): RateLimitInfo {
  const get = (name: string): string | undefined => {
    const v = headers[name] ?? headers[name.toLowerCase()];
    return Array.isArray(v) ? v[0] : v;
  };

  const unifiedStatus = get('anthropic-ratelimit-unified-status');
  const overageStatus = get('anthropic-ratelimit-unified-overage-status');
  const representativeClaim = get('anthropic-ratelimit-unified-representative-claim');

  const unifiedWindows: UnifiedWindow[] = [];
  for (const abbrev of UNIFIED_WINDOW_ABBREVS) {
    const util = get(`anthropic-ratelimit-unified-${abbrev}-utilization`);
    const reset = get(`anthropic-ratelimit-unified-${abbrev}-reset`);
    const surpassed = get(`anthropic-ratelimit-unified-${abbrev}-surpassed-threshold`);
    // Only record a window the server actually reported at least one field for,
    // so an all-absent window never masquerades as a zero-utilization snapshot.
    if (util === undefined && reset === undefined && surpassed === undefined) continue;
    unifiedWindows.push({
      claimAbbrev: abbrev,
      utilization: parseFloatHeader(util),
      resetAt: parseResetHeader(reset),
      surpassedThreshold: parseFloatHeader(surpassed),
    });
  }

  return {
    retryAfterMs: parseRetryAfterHeader(get('retry-after')),
    remainingRequests: parseIntHeader(get('x-ratelimit-remaining-requests') ?? get('ratelimit-remaining')),
    remainingTokens: parseIntHeader(get('x-ratelimit-remaining-tokens')),
    resetAt: parseResetHeader(get('x-ratelimit-reset-requests') ?? get('x-ratelimit-reset')),
    resetAtTokens: parseResetHeader(get('x-ratelimit-reset-tokens')),
    limitRequests: parseIntHeader(get('x-ratelimit-limit-requests') ?? get('ratelimit-limit')),
    limitTokens: parseIntHeader(get('x-ratelimit-limit-tokens')),
    unifiedStatus: unifiedStatus ? unifiedStatus.trim() : null,
    overageStatus: overageStatus ? overageStatus.trim() : null,
    overageResetAt: parseResetHeader(get('anthropic-ratelimit-unified-overage-reset')),
    representativeClaim: representativeClaim ? representativeClaim.trim() : null,
    unifiedWindows,
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

/** Parse a fractional header (e.g. a 0–1 utilization) to a finite number, or null. */
function parseFloatHeader(value: string | undefined): number | null {
  if (value === undefined) return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const n = Number(trimmed);
  return Number.isFinite(n) ? n : null;
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

/**
 * Two-tier (primary + overage/spillover) hard rate-limit decision, mirroring
 * Claude Code's rateLimitMocking `shouldThrow429`: a `'rejected'` primary tier is
 * only a HARD limit when there is no overage tier or the overage tier is ALSO
 * `'rejected'`. A `'rejected'` primary with an `'allowed'` overage means spillover
 * quota remains, so the request should be tolerated rather than hard-failed.
 * Returns false when the server reported no unified status at all.
 */
export function isHardRateLimited(info: RateLimitInfo): boolean {
  if (info.unifiedStatus !== 'rejected') return false;
  return !info.overageStatus || info.overageStatus === 'rejected';
}

export function shouldBackoff(info: RateLimitInfo): boolean {
  // An explicit server directive always wins.
  if (info.retryAfterMs !== null && info.retryAfterMs > 0) return true;
  // When the server reports a unified tier status, it is authoritative over the
  // per-bucket remaining heuristics: a soft overage (primary rejected, overage
  // allowed) still has spillover quota, so we do NOT back off even if a bucket's
  // remaining counter hit zero.
  if (info.unifiedStatus) return isHardRateLimited(info);
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

/** Local IANA timezone abbreviation/name for display (canonical helper; shared
 *  with cron_list so timestamp labelling stays consistent). */
export function getTimeZone(): string {
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

// ---------------------------------------------------------------------------
// Proactive early-warning (parity: Claude's claudeAiLimits getEarlyWarning*).
// Warns BEFORE a hard limit when quota is burning faster than the time window
// allows — e.g. 75% of the weekly window consumed in the first 60% of the week.
// ---------------------------------------------------------------------------

/**
 * One proactive warning threshold: fire when utilization is at/above
 * `utilization` AND no more than `timePct` of the time window has elapsed (i.e.
 * high usage *early* in the window). Both are fractions in [0, 1].
 */
export interface EarlyWarningThreshold {
  utilization: number;
  timePct: number;
}

/** A known rate-limit window and its proactive warning ladder. */
export interface EarlyWarningConfig {
  /** Canonical rate-limit type, e.g. `'five_hour'` / `'seven_day'`. */
  rateLimitType: string;
  /** Header abbreviation matching {@link UnifiedWindow.claimAbbrev}. */
  claimAbbrev: string;
  /** Fixed duration of the window in seconds (not the observed remaining time). */
  windowSeconds: number;
  /** Thresholds in priority order; the first satisfied one fires. */
  thresholds: EarlyWarningThreshold[];
}

/**
 * Default warning ladders (parity: Claude's EARLY_WARNING_CONFIGS). The 7-day
 * window intentionally carries aggressive low tiers that only make sense with a
 * KNOWN fixed window — the observed-window burn-rate heuristic in
 * `rate-limiter.ts` cannot use them because a single observation can't tell how
 * much of a 7-day window has elapsed.
 */
export const DEFAULT_EARLY_WARNING_CONFIGS: readonly EarlyWarningConfig[] = [
  {
    rateLimitType: 'five_hour',
    claimAbbrev: '5h',
    windowSeconds: 5 * 60 * 60,
    thresholds: [{ utilization: 0.9, timePct: 0.72 }],
  },
  {
    rateLimitType: 'seven_day',
    claimAbbrev: '7d',
    windowSeconds: 7 * 24 * 60 * 60,
    thresholds: [
      { utilization: 0.75, timePct: 0.6 },
      { utilization: 0.5, timePct: 0.35 },
      { utilization: 0.25, timePct: 0.15 },
    ],
  },
];

/** Human-readable names for the known rate-limit types (parity: RATE_LIMIT_DISPLAY_NAMES). */
const RATE_LIMIT_DISPLAY_NAMES: Record<string, string> = {
  five_hour: 'session limit',
  seven_day: 'weekly limit',
  seven_day_opus: 'Opus weekly limit',
  seven_day_sonnet: 'Sonnet weekly limit',
  overage: 'extra usage limit',
};

/** Map a rate-limit type to a user-facing name, falling back to the raw type. */
export function getRateLimitDisplayName(type: string): string {
  return RATE_LIMIT_DISPLAY_NAMES[type] || type;
}

/**
 * Fraction (0–1) of a fixed window that has elapsed, given the window's reset
 * instant (Unix epoch SECONDS) and its total duration. Parity: Claude's
 * computeTimeProgress. Clamped to [0, 1]; a non-positive window yields 0.
 */
export function computeTimeProgress(
  resetAtSeconds: number,
  windowSeconds: number,
  nowMs: number = Date.now(),
): number {
  if (!(windowSeconds > 0)) return 0;
  const nowSeconds = nowMs / 1000;
  const windowStart = resetAtSeconds - windowSeconds;
  const elapsed = nowSeconds - windowStart;
  return Math.max(0, Math.min(1, elapsed / windowSeconds));
}

/** A fired proactive early warning (parity: Claude's allowed_warning limits). */
export interface EarlyWarning {
  /** Canonical rate-limit type whose threshold fired. */
  rateLimitType: string;
  /** Header abbreviation of the firing window. */
  claimAbbrev: string;
  /** Utilization (0–1) reported for the window, when known. */
  utilization: number | null;
  /** Reset instant of the window, when known. */
  resetAt: Date | null;
  /** Server-reported surpassed threshold (header-driven path), else null. */
  surpassedThreshold: number | null;
  /** How the warning was derived. */
  source: 'header' | 'time-relative';
}

/**
 * Decide whether to raise a proactive early warning from parsed unified windows,
 * mirroring Claude's getEarlyWarningFromHeaders:
 *  1. Header-based: any window carrying a `surpassed-threshold` wins immediately.
 *  2. Time-relative fallback: for each configured window (priority order), fire
 *     when utilization is high relative to how little of the fixed window has
 *     elapsed.
 * Returns null when no threshold is surpassed.
 */
export function computeEarlyWarning(
  windows: readonly UnifiedWindow[] | undefined,
  configs: readonly EarlyWarningConfig[] = DEFAULT_EARLY_WARNING_CONFIGS,
  nowMs: number = Date.now(),
): EarlyWarning | null {
  if (!windows || windows.length === 0) return null;

  // 1. Header-based detection (authoritative when the server sends it).
  for (const w of windows) {
    if (w.surpassedThreshold !== null) {
      const cfg = configs.find(c => c.claimAbbrev === w.claimAbbrev);
      return {
        rateLimitType: cfg ? cfg.rateLimitType : w.claimAbbrev,
        claimAbbrev: w.claimAbbrev,
        utilization: w.utilization,
        resetAt: w.resetAt,
        surpassedThreshold: w.surpassedThreshold,
        source: 'header',
      };
    }
  }

  // 2. Time-relative fallback (client-side burn-rate against the fixed window).
  for (const cfg of configs) {
    const w = windows.find(x => x.claimAbbrev === cfg.claimAbbrev);
    if (!w || w.utilization === null || !w.resetAt) continue;
    const resetAtSeconds = w.resetAt.getTime() / 1000;
    const timeProgress = computeTimeProgress(resetAtSeconds, cfg.windowSeconds, nowMs);
    const fired = cfg.thresholds.some(
      t => w.utilization! >= t.utilization && timeProgress <= t.timePct,
    );
    if (fired) {
      return {
        rateLimitType: cfg.rateLimitType,
        claimAbbrev: cfg.claimAbbrev,
        utilization: w.utilization,
        resetAt: w.resetAt,
        surpassedThreshold: null,
        source: 'time-relative',
      };
    }
  }

  return null;
}
