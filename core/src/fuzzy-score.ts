/**
 * Shared nucleo/fzf-v2-style fuzzy-scoring primitives.
 *
 * These constants and character classifiers approximate the scoring behavior of
 * nucleo (https://github.com/helix-editor/nucleo), the fuzzy matcher the reference
 * Code's native file-index wraps. They are the single canonical source for the
 * boundary / camelCase / consecutive bonuses and gap penalties used by BOTH
 * the symbol-name scorer in {@link ./workspace-indexer} and the scalable
 * file-path index in {@link ./file-index}. Keeping them here (rather than a copy
 * per scorer) enforces the No-Duplication rule: one definition of "match
 * quality" shared by every fuzzy consumer.
 */

/** Base reward per matched needle character. */
export const SCORE_MATCH = 16;
/** Bonus when the match lands right after a path/word separator (`/ \ - _ . space`). */
export const BONUS_BOUNDARY = 8;
/** Bonus when the match lands on a camelCase hump (lowercase→uppercase). */
export const BONUS_CAMEL = 6;
/** Bonus for each consecutive-run match (no gap from the previous match). */
export const BONUS_CONSECUTIVE = 4;
/** Bonus when the first needle char matches at position 0 (true prefix). */
export const BONUS_FIRST_CHAR = 8;
/** Penalty applied once when a gap opens between two matched chars. */
export const PENALTY_GAP_START = 3;
/** Penalty per extra character inside a gap. */
export const PENALTY_GAP_EXTENSION = 1;

/**
 * Hard cap on the scanned needle length (parity with the reference's native
 * file-index `MAX_QUERY_LEN`). A query longer than this is clamped before
 * scanning so a pathologically long token cannot drive a quadratic scan across
 * every candidate — a cheap resource/DoS guard. 64 chars is far longer than any
 * real file/symbol fragment, so the clamp never affects a genuine query.
 */
export const FUZZY_MAX_QUERY_LEN = 64;

/** True for a path/word separator char code: `/ \ - _ . space`. */
export function isBoundary(code: number): boolean {
  return (
    code === 47 || // /
    code === 92 || // \
    code === 45 || // -
    code === 95 || // _
    code === 46 || // .
    code === 32 // space
  );
}

/** True for an ASCII lowercase letter code. */
export function isLower(code: number): boolean {
  return code >= 97 && code <= 122;
}

/** True for an ASCII uppercase letter code. */
export function isUpper(code: number): boolean {
  return code >= 65 && code <= 90;
}

/**
 * Boundary / camelCase bonus for a match at position `pos` (`pos >= 1`) in the
 * original-case `text`. A match right after a separator earns {@link BONUS_BOUNDARY};
 * a match on a camelCase hump (lowercase char immediately before an uppercase
 * match) earns {@link BONUS_CAMEL}; otherwise 0. The first-char (`pos === 0`)
 * bonus is the caller's responsibility (it differs per scorer).
 */
export function boundaryBonus(text: string, pos: number): number {
  const prevCh = text.charCodeAt(pos - 1);
  if (isBoundary(prevCh)) return BONUS_BOUNDARY;
  if (isLower(prevCh) && isUpper(text.charCodeAt(pos))) return BONUS_CAMEL;
  return 0;
}
