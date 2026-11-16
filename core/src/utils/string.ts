/**
 * Allocation-free string helpers.
 *
 * AMI previously counted character occurrences with the `str.split(x).length - 1`
 * idiom, which allocates a full array of substrings on every call — wasteful on
 * hot validation / log-truncation paths that run over large bash commands and
 * suite logs. These helpers mirror Claude Code's `src/utils/stringUtils.ts`
 * (`countCharInString`) which walks the string with `indexOf` jumps instead.
 */

/**
 * Counts occurrences of `char` in `str` using `indexOf` jumps instead of
 * per-character iteration or a `split()` allocation. The `str` parameter is
 * structurally typed on `indexOf` so a `Buffer` works too (`Buffer.indexOf`
 * accepts string needles). Counting starts at `start` (inclusive), so callers
 * can measure only the tail of a string past a truncation boundary without
 * slicing. Matches are non-overlapping-by-jump (`i + 1`), which for a
 * single-character needle is identical to the old `split(char).length - 1`.
 */
export function countCharInString(
  str: { indexOf(search: string, start?: number): number },
  char: string,
  start = 0,
): number {
  let count = 0;
  let i = str.indexOf(char, start);
  while (i !== -1) {
    count++;
    i = str.indexOf(char, i + 1);
  }
  return count;
}
