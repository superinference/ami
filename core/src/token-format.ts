/**
 * Canonical compact formatter for a token count, shared across every layer that
 * surfaces token figures to a user (model-switch warnings, the usage/stats
 * dashboards). A single home keeps the rendering consistent and satisfies the
 * No-Duplication rule — before this module the CLI inspector carried its own copy
 * and the core model-switch warning interpolated raw integers.
 *
 * Compact notation (parity with Claude's `formatTokens`, utils/format.ts):
 *   900 => "900", 1_321 => "1.3k", 1_000_000 => "1.0M", 2.5e9 => "2.5b".
 * Non-finite inputs (NaN / Infinity from a corrupt or derived count) degrade to
 * "0" so a user-facing surface never shows "NaN" / "Infinityb".
 */
export function formatTokenCount(n: number): string {
  if (!Number.isFinite(n)) return '0';
  if (n >= 1_000_000_000) return (n / 1_000_000_000).toFixed(1) + 'b';
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'k';
  return String(n);
}
