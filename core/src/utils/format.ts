/**
 * Shared human-readable formatters for user/model-facing display.
 */

const FILE_SIZE_UNITS = ['B', 'KB', 'MB', 'GB', 'TB'] as const;

/**
 * Render a byte count as a compact, legible size string (e.g. `512B`, `1.5KB`,
 * `5MB`, `1.2GB`). Scales through binary (1024) steps and keeps at most one
 * decimal place, dropping a trailing `.0` so whole values read cleanly.
 *
 * Centralizes what was previously ad-hoc, single-unit `(bytes/1024).toFixed(0)`
 * arithmetic scattered across size/limit messages, so a genuinely large file
 * reads as `500MB` instead of `512000KB`. Returns `0B` for non-finite or
 * negative input (defensive — a size should never be either).
 */
export function formatFileSize(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return '0B';
  let size = bytes;
  let unit = 0;
  while (size >= 1024 && unit < FILE_SIZE_UNITS.length - 1) {
    size /= 1024;
    unit += 1;
  }
  const rounded = unit === 0 ? Math.round(size) : Math.round(size * 10) / 10;
  return `${rounded}${FILE_SIZE_UNITS[unit]}`;
}

/** Rendering style for {@link formatRelativeTime}. `narrow` uses AMI's compact
 *  custom units (`3d ago`, `in 2h`); `short`/`long` defer to the localized
 *  `Intl.RelativeTimeFormat` for full-word output (`3 days ago`). */
export type RelativeTimeStyle = 'narrow' | 'short' | 'long';

/** Options for {@link formatRelativeTime} / {@link formatRelativeTimeAgo}
 *  (parity with the reference's `RelativeTimeOptions`, utils/format.ts). */
export interface RelativeTimeOptions {
  /** @default 'narrow' */
  style?: RelativeTimeStyle;
  /** Whether the localized formatter always emits a number or may say
   *  "yesterday"/"now". @default 'always' */
  numeric?: 'always' | 'auto';
}

/** Interval table (largest → smallest) driving the unit selection in
 *  {@link formatRelativeTime}. Each row carries the full unit (for the localized
 *  `Intl.RelativeTimeFormat`) and the compact `narrow` suffix. */
const RELATIVE_TIME_INTERVALS = [
  { unit: 'year', seconds: 31536000, shortUnit: 'y' },
  { unit: 'month', seconds: 2592000, shortUnit: 'mo' },
  { unit: 'week', seconds: 604800, shortUnit: 'w' },
  { unit: 'day', seconds: 86400, shortUnit: 'd' },
  { unit: 'hour', seconds: 3600, shortUnit: 'h' },
  { unit: 'minute', seconds: 60, shortUnit: 'm' },
  { unit: 'second', seconds: 1, shortUnit: 's' },
] as const;

/** Memoized `Intl.RelativeTimeFormat` instances — constructing one per call is
 *  measurably expensive, and the (style, numeric) space is tiny. Parity with
 *  the reference's cached `getRelativeTimeFormat` (utils/intl.ts). */
const relativeTimeFormatCache = new Map<string, Intl.RelativeTimeFormat>();
function getRelativeTimeFormat(
  style: 'short' | 'long' | 'narrow',
  numeric: 'always' | 'auto',
): Intl.RelativeTimeFormat {
  const key = `${style}\u0000${numeric}`;
  let fmt = relativeTimeFormatCache.get(key);
  if (fmt === undefined) {
    fmt = new Intl.RelativeTimeFormat('en-US', { style, numeric });
    relativeTimeFormatCache.set(key, fmt);
  }
  return fmt;
}

/**
 * Format `date` relative to `now` (default: the current time) using an
 * interval-table algorithm. Parity with the reference's `formatRelativeTime`
 * (utils/format.ts) + its localized `Intl.RelativeTimeFormat` wrapper
 * (utils/intl.ts): the largest interval whose magnitude the gap meets or exceeds
 * selects the unit (truncating toward zero), so a 90-minute gap renders as the
 * hour unit, not 90 minutes.
 *
 * `narrow` (the default) emits AMI's compact `${value}${shortUnit} ago` / `in
 * ${value}${shortUnit}` form; `short`/`long` defer to the memoized localized
 * `Intl.RelativeTimeFormat` for full-word, locale-aware output. A matched
 * interval ≥ 1 day always renders with the long localized form regardless of
 * `style` (matching the reference), and a sub-second gap degrades to `0s ago` /
 * `in 0s` (narrow) or the localized zero-second string.
 *
 * Canonical home: this is a pure, leaf-safe display formatter (only `Intl`), so
 * it lives here in `utils/format` next to {@link formatFileSize} and
 * {@link formatLogMetadata} that compose it — mirroring the reference's single
 * `utils/format.ts`. `memory.ts` re-exports it for its diagnostics consumers.
 */
export function formatRelativeTime(
  date: Date,
  options: RelativeTimeOptions & { now?: Date } = {},
): string {
  const { style = 'narrow', numeric = 'always', now = new Date() } = options;
  const diffInMs = date.getTime() - now.getTime();
  // Truncate toward zero so a 1.9s future gap is 1s (not rounded up to 2s) and a
  // past gap is handled symmetrically.
  const diffInSeconds = Math.trunc(diffInMs / 1000);

  for (const { unit, seconds, shortUnit } of RELATIVE_TIME_INTERVALS) {
    if (Math.abs(diffInSeconds) >= seconds) {
      const value = Math.trunc(diffInSeconds / seconds);
      if (style === 'narrow') {
        return diffInSeconds < 0
          ? `${Math.abs(value)}${shortUnit} ago`
          : `in ${value}${shortUnit}`;
      }
      // Days-and-longer always read best in the long localized form.
      return getRelativeTimeFormat('long', numeric).format(value, unit);
    }
  }

  // Less than one second in either direction.
  if (style === 'narrow') {
    return diffInSeconds <= 0 ? '0s ago' : 'in 0s';
  }
  return getRelativeTimeFormat(style, numeric).format(0, 'second');
}

/**
 * {@link formatRelativeTime} specialized for a PAST timestamp: forces
 * `numeric: 'always'` for past dates so the output is a definite "X units ago"
 * rather than a localized "yesterday", while a future date falls through to the
 * caller's options unchanged. Parity with the reference's `formatRelativeTimeAgo`
 * (utils/format.ts).
 */
export function formatRelativeTimeAgo(
  date: Date,
  options: RelativeTimeOptions & { now?: Date } = {},
): string {
  const { now = new Date(), ...rest } = options;
  if (date > now) {
    return formatRelativeTime(date, { ...rest, now });
  }
  return formatRelativeTime(date, { ...rest, numeric: 'always', now });
}

/** Input schema for {@link formatLogMetadata}: the two required facts every
 *  session/log row has (when it was modified and how many messages it holds)
 *  plus a set of optional enrichments. Mirrors the reference's `formatLogMetadata`
 *  argument (utils/format.ts). */
export interface LogMetadata {
  modified: Date;
  messageCount: number;
  /** On-disk size; when present it replaces the message-count cell. */
  fileSize?: number;
  gitBranch?: string;
  tag?: string;
  agentSetting?: string;
  prNumber?: number;
  prRepository?: string;
}

/**
 * Compose a one-line, `' · '`-joined metadata summary for a session/log listing
 * row — the relative-modified-time, then branch, then size-or-message-count,
 * then any of `#tag`, `@agent`, `repo#pr`. Parity with the reference's
 * `formatLogMetadata` (utils/format.ts): exactly the same field precedence and
 * joiners, so AMI's resume/history surfaces read identically.
 *
 * `fileSize`, when given, replaces the `N messages` cell with a humanized size
 * (via {@link formatFileSize}); the relative time uses the `short` localized
 * style. Every optional field is omitted when absent, so a metadata-only row
 * degrades to just `"<relative> · N messages"`.
 *
 * @param now optional clock override (testability; defaults to the real time).
 */
export function formatLogMetadata(
  log: LogMetadata,
  now?: Date,
): string {
  const sizeOrCount =
    log.fileSize !== undefined
      ? formatFileSize(log.fileSize)
      : `${log.messageCount} messages`;
  const parts: string[] = [
    formatRelativeTimeAgo(log.modified, { style: 'short', now }),
    ...(log.gitBranch ? [log.gitBranch] : []),
    sizeOrCount,
  ];
  if (log.tag) parts.push(`#${log.tag}`);
  if (log.agentSetting) parts.push(`@${log.agentSetting}`);
  if (log.prNumber) {
    parts.push(
      log.prRepository
        ? `${log.prRepository}#${log.prNumber}`
        : `#${log.prNumber}`,
    );
  }
  return parts.join(' · ');
}
