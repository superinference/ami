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
