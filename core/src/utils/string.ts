/**
 * Shared, allocation-aware string primitives — the single canonical home for the
 * small `stringUtils` helpers AMI previously open-coded across the tree.
 *
 * AMI previously counted character occurrences with the `str.split(x).length - 1`
 * idiom, which allocates a full array of substrings on every call — wasteful on
 * hot validation / log-truncation paths that run over large bash commands and
 * suite logs. Likewise the singular/plural `${n === 1 ? '' : 's'}` ternary, the
 * `charAt(0).toUpperCase() + slice(1)` capitalize idiom, and four near-identical
 * private `escapeReg(Exp|ex)` RegExp-escape helpers were duplicated at a dozen+
 * call sites. These helpers consolidate all of that into one place and mirror
 * the reference's `src/utils/stringUtils.ts` (`countCharInString`, `plural`,
 * `capitalize`, `escapeRegExp`) so the behaviour matches the reference exactly.
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

/**
 * Returns the singular or plural form of `word` based on `n`, replacing the inline
 * `word${n === 1 ? '' : 's'}` idiom. Only the WORD form is returned (no leading
 * count) so it composes inside existing templates: `` `${n} ${plural(n, 'file')}` ``.
 *
 * - `n === 1` → the singular `word`.
 * - otherwise → `pluralWord`, which defaults to `word + 's'` but can be overridden
 *   for irregular plurals (e.g. `plural(2, 'entry', 'entries')`).
 *
 * Mirrors the reference's `plural(n, word, pluralWord = word + 's')`.
 *
 * @example plural(1, 'file')              // 'file'
 * @example plural(3, 'file')              // 'files'
 * @example plural(2, 'entry', 'entries') // 'entries'
 */
export function plural(n: number, word: string, pluralWord: string = word + 's'): string {
  return n === 1 ? word : pluralWord;
}

/**
 * Uppercases the first character of `str`, leaving the rest unchanged. Unlike
 * lodash `capitalize`, this does NOT lowercase the remaining characters — so
 * `capitalize('fooBar') === 'FooBar'`. Replaces the open-coded
 * `str.charAt(0).toUpperCase() + str.slice(1)` idiom. Mirrors the reference's
 * `capitalize`. An empty string is returned unchanged.
 *
 * @example capitalize('read')      // 'Read'
 * @example capitalize('fooBar')    // 'FooBar'
 */
export function capitalize(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1);
}

/**
 * Escapes the regex metacharacters in `str` so it can be embedded as a LITERAL
 * inside a `RegExp` source. Single canonical replacement for the four private
 * `escapeReg(Exp|ex)` / `escapeForRegExp` helpers (grep, session fork-title,
 * skill arg-name, PowerShell command matching) and the inline `.replace(...)`
 * escapes (config key probe, Windows registry parse). Mirrors the reference's
 * `escapeRegExp`.
 *
 * @example escapeRegExp('a.b*c') // 'a\\.b\\*c'
 */
export function escapeRegExp(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
