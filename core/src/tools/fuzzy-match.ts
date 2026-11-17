/**
 * Graduated fuzzy matching for file edits.
 *
 * Strategies are tried in order of strictness; the first strategy that
 * yields exactly one match wins.
 *
 *   1. exact           — direct indexOf
 *   2. line_trimmed    — trim each line before comparing
 *   3. whitespace_normalized — collapse whitespace runs to single space
 *   4. indentation_flexible  — strip leading whitespace entirely
 *   5. escape_normalized     — convert \\n to newlines, smart quotes to ASCII
 *   6. desanitize_normalized — map API-sanitized token forms back to raw
 *   7. block_anchor          — last-resort: match a >=3-line block on its first
 *                              and last (trimmed) lines, ignoring the interior
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export const STRATEGY_NAMES = ['exact', 'line_trimmed', 'whitespace_normalized', 'indentation_flexible', 'escape_normalized', 'desanitize_normalized', 'block_anchor'] as const;
export type StrategyName = (typeof STRATEGY_NAMES)[number];

/**
 * Model-emitted API-sanitized token forms → the raw forms that actually live in
 * files. The provider sanitizes special control tokens in model OUTPUT, so the
 * model literally never sees (and therefore re-emits the sanitized form of) tokens
 * like `<function_results>` or a `\n\nHuman:` turn marker. When such a sanitized
 * form arrives as `old_string`, an exact match fails against the raw file bytes.
 * Desanitizing the match string (and re-applying the SAME substitutions to
 * `new_string`) recovers the edit. Mirrors Claude Code's DESANITIZATIONS.
 */
export const DESANITIZATIONS: ReadonlyArray<readonly [string, string]> = [
  ['<fnr>', '<function_results>'],
  ['</fnr>', '</function_results>'],
  ['<fnc>', '<function_calls>'],
  ['</fnc>', '</function_calls>'],
  ['<n>', '<name>'],
  ['</n>', '</name>'],
  ['\n\nH:', '\n\nHuman:'],
  ['\n\nA:', '\n\nAssistant:'],
  ['< META_START >', '<META_START>'],
  ['< META_END >', '<META_END>'],
];

/** Replace every sanitized token form with its raw form. */
export function desanitizeMatchString(s: string): string {
  let out = s;
  for (const [from, to] of DESANITIZATIONS) {
    if (out.includes(from)) out = out.split(from).join(to);
  }
  return out;
}

/**
 * Error surfaced when an edit locates its target but produces byte-identical
 * content — a no-op that normalization/reindent/quote-restyling erased (e.g.
 * replacing `‘b’` with `'b'` when the file's curly typography is preserved, or a
 * reindent that reproduces the region verbatim). Claude Code rejects such edits
 * with its post-apply identity check; AMI surfaces the same so a no-op is never
 * reported as a successful edit. Consumers compare `error === NOOP_EDIT_ERROR`
 * rather than matching the message text.
 */
export const NOOP_EDIT_ERROR = 'new_string produces no change (edit is a no-op after normalization)';

export interface MatchPosition {
  /** Byte offset (character index) in the **original** content. */
  start: number;
  /** Byte offset of the character past the end of the match. */
  end: number;
}

export interface FuzzyResult {
  /** The file content after replacement (`null` on failure). */
  newContent: string | null;
  /** Which strategy succeeded (`null` on failure). */
  strategy: StrategyName | null;
  /** How many matches the winning (or failing) strategy found. */
  matchCount: number;
  /** Non-null when the operation failed. */
  error: string | null;
}

// ---------------------------------------------------------------------------
// Strategy implementations
// ---------------------------------------------------------------------------

function findExact(content: string, pattern: string): MatchPosition[] {
  if (pattern.length === 0) return [];
  const results: MatchPosition[] = [];
  let pos = 0;
  while (true) {
    const idx = content.indexOf(pattern, pos);
    if (idx === -1) break;
    results.push({ start: idx, end: idx + pattern.length });
    pos = idx + 1;
  }
  return results;
}

/**
 * Split on newlines. A single trailing newline ends the last line; it is not
 * an extra blank line that the file must contain. A second trailing newline
 * still means a blank line.
 */
function splitLogicalLines(value: string): { lines: string[]; terminated: boolean } {
  const terminated = value.endsWith('\n');
  const lines = value.split('\n');
  if (terminated) lines.pop();
  return { lines, terminated };
}

/**
 * Line-based matching helper used by strategies 2–4.
 *
 * Both `content` and `pattern` are split into lines, each line is transformed
 * by `normalizeLine`, and we search for the normalized-pattern-lines as a
 * contiguous subsequence inside the normalized-content-lines.
 *
 * When a match is found, we map back to original byte offsets using the
 * un-normalized content lines.
 */
function findByLine(
  content: string,
  pattern: string,
  normalizeLine: (line: string) => string,
): MatchPosition[] {
  const { lines: contentLines, terminated: contentTerminated } = splitLogicalLines(content);
  const { lines: patternLines, terminated: patternTerminated } = splitLogicalLines(pattern);

  const normContentLines = contentLines.map(normalizeLine);
  const normPatternLines = patternLines.map(normalizeLine);

  // Guard: a pattern that normalizes to nothing but empty lines (empty or
  // whitespace-only old_string) must NOT silently match the file's blank lines.
  // Such a match is never what the caller intended and is inherently ambiguous;
  // fall through so the caller gets a clean not-found instead of clobbering a
  // random blank line.
  if (normPatternLines.every(l => l === '')) return [];

  const results: MatchPosition[] = [];

  // Slide the pattern window over the content lines
  for (let i = 0; i <= normContentLines.length - normPatternLines.length; i++) {
    let match = true;
    for (let j = 0; j < normPatternLines.length; j++) {
      if (normContentLines[i + j] !== normPatternLines[j]) {
        match = false;
        break;
      }
    }
    if (match) {
      // Compute original byte offset: sum lengths of all lines before line i,
      // plus newline characters.
      let start = 0;
      for (let k = 0; k < i; k++) {
        start += contentLines[k].length + 1; // +1 for \n
      }
      let end = start;
      for (let k = i; k < i + normPatternLines.length; k++) {
        end += contentLines[k].length;
        const lastPatternLine = k === i + normPatternLines.length - 1;
        const newlineFollows = k < contentLines.length - 1 || contentTerminated;
        if (!lastPatternLine || (patternTerminated && newlineFollows)) {
          end += 1;
        }
      }
      results.push({ start, end });
    }
  }

  return results;
}

function findLineTrimmed(content: string, pattern: string): MatchPosition[] {
  return findByLine(content, pattern, l => l.trim());
}

/**
 * Desanitize the pattern (map sanitized token forms back to raw) then exact-match
 * it against the raw file content. Returns [] when the pattern had no sanitized
 * tokens (nothing new to try — the earlier strategies already covered it).
 */
function findDesanitized(content: string, pattern: string): MatchPosition[] {
  const desanitized = desanitizeMatchString(pattern);
  if (desanitized === pattern) return [];
  return findExact(content, desanitized);
}

function findWhitespaceNormalized(content: string, pattern: string): MatchPosition[] {
  return findByLine(content, pattern, l => l.replace(/\s+/g, ' ').trim());
}

function findIndentationFlexible(content: string, pattern: string): MatchPosition[] {
  return findByLine(content, pattern, l => l.replace(/^\s+/, ''));
}

/**
 * Block-anchor matching (last-resort fuzzy strategy).
 *
 * When every line-level strategy fails — typically because the INTERIOR of a
 * multi-line block drifted (reformatting, a renamed local, a reflowed comment)
 * while its boundaries are stable — anchor the match on the first and last lines
 * only. A candidate is a window of exactly `patternLines.length` content lines
 * whose first and last lines (trimmed) equal the pattern's first and last lines
 * (trimmed). The interior is intentionally ignored.
 *
 * Constraints that keep this safe and additive:
 *   - requires >= 3 lines (a 2-line block is just its two anchors, which the
 *     line-level strategies already cover; anchoring there adds nothing and
 *     would over-match),
 *   - both anchors must be non-empty after trimming (blank boundaries are not
 *     distinctive enough to anchor on),
 *   - the window length is fixed to the pattern length so the replaced span is
 *     unambiguous, and multiple candidates still surface as an ambiguity error.
 */
function findBlockAnchor(content: string, pattern: string): MatchPosition[] {
  const { lines: contentLines, terminated: contentTerminated } = splitLogicalLines(content);
  const { lines: patternLines, terminated: patternTerminated } = splitLogicalLines(pattern);

  const span = patternLines.length;
  if (span < 3) return [];

  const firstAnchor = patternLines[0].trim();
  const lastAnchor = patternLines[span - 1].trim();
  if (firstAnchor.length === 0 || lastAnchor.length === 0) return [];

  const results: MatchPosition[] = [];
  for (let i = 0; i + span <= contentLines.length; i++) {
    if (contentLines[i].trim() !== firstAnchor) continue;
    if (contentLines[i + span - 1].trim() !== lastAnchor) continue;

    // Map the window back to original byte offsets (same accounting as findByLine).
    let start = 0;
    for (let k = 0; k < i; k++) {
      start += contentLines[k].length + 1; // +1 for \n
    }
    let end = start;
    for (let k = i; k < i + span; k++) {
      end += contentLines[k].length;
      const lastPatternLine = k === i + span - 1;
      const newlineFollows = k < contentLines.length - 1 || contentTerminated;
      if (!lastPatternLine || (patternTerminated && newlineFollows)) {
        end += 1;
      }
    }
    results.push({ start, end });
  }

  return results;
}

function normalizeEscapes(s: string): string {
  return s
    .replace(/\\n/g, '\n')
    .replace(/‘|’/g, "'")
    .replace(/“|”/g, '"')
    .replace(/—/g, '--')
    .replace(/–/g, '-');
}

function findEscapeNormalized(content: string, pattern: string): MatchPosition[] {
  const normContent = normalizeEscapes(content);
  const normPattern = normalizeEscapes(pattern);

  // Find matches in the normalized string, then map back.
  // Because normalizeEscapes can change string length (e.g. \\n -> \n is
  // shorter by 1), we build a character-position map.
  const map = buildPositionMap(content, normContent);

  const results: MatchPosition[] = [];
  let pos = 0;
  while (true) {
    const idx = normContent.indexOf(normPattern, pos);
    if (idx === -1) break;
    const normEnd = idx + normPattern.length;

    const origStart = mapPosition(map, idx);
    const origEnd = mapPosition(map, normEnd);

    results.push({ start: origStart, end: origEnd });
    pos = idx + 1;
  }
  return results;
}

/**
 * Build a mapping from normalized-string positions to original-string
 * positions.  Returns an array where `map[normIdx] = origIdx`.
 */
function buildPositionMap(original: string, normalized: string): number[] {
  // Walk both strings in parallel using the normalization logic.
  // For simple cases where lengths are the same, it's 1:1.
  if (original.length === normalized.length) {
    return Array.from({ length: normalized.length + 1 }, (_, i) => i);
  }

  const map: number[] = [];
  let oi = 0;
  let ni = 0;
  while (ni < normalized.length && oi < original.length) {
    map[ni] = oi;
    // Check if we're at a \\n -> \n replacement (2 chars -> 1 char)
    if (original[oi] === '\\' && oi + 1 < original.length && original[oi + 1] === 'n' && normalized[ni] === '\n') {
      ni++;
      oi += 2;
    }
    // Em dash: 1 original char → 2 normalized chars ('—' → '--')
    else if (original[oi] === '—' && normalized[ni] === '-' && ni + 1 < normalized.length && normalized[ni + 1] === '-') {
      ni++;
      map[ni] = oi;
      ni++;
      oi++;
    }
    // Smart quote replacements (single char -> single char)
    else if (original.charCodeAt(oi) > 127 && normalized.charCodeAt(ni) < 128) {
      ni++;
      oi++;
    } else {
      ni++;
      oi++;
    }
  }
  map[ni] = oi; // Sentinel for end position
  return map;
}

function mapPosition(map: number[], normIdx: number): number {
  if (normIdx < map.length) return map[normIdx];
  // Past end — return last mapped + offset
  const lastMapped = map.length > 0 ? map[map.length - 1] : 0;
  return lastMapped + (normIdx - (map.length - 1));
}

// ---------------------------------------------------------------------------
// Re-indentation
// ---------------------------------------------------------------------------

/**
 * Compute the leading whitespace of a line.
 */
function getIndent(line: string): string {
  const match = line.match(/^(\s*)/);
  return match ? match[1] : '';
}

/**
 * Adjust `newString` indentation so it matches the indentation context of
 * the region being replaced in the file.
 *
 * Computes the delta between the base indent of `oldString` (first line)
 * and the actual indentation at the match site in the file, then applies
 * that delta uniformly to every line of `newString`.
 */
export function reindentReplacement(
  fileRegion: string,
  oldString: string,
  newString: string,
): string {
  const oldLines = oldString.split('\n');
  const fileLines = fileRegion.split('\n');

  const oldBaseIndent = getIndent(oldLines[0]);
  const fileBaseIndent = getIndent(fileLines[0]);

  if (oldBaseIndent === fileBaseIndent) {
    return newString; // No adjustment needed
  }

  // Calculate delta: how many more (or fewer) spaces the file has
  const deltaLen = fileBaseIndent.length - oldBaseIndent.length;
  const deltaChar = fileBaseIndent.length > 0 ? fileBaseIndent[0] : ' ';
  const deltaPrefix = deltaLen > 0 ? deltaChar.repeat(deltaLen) : '';

  const newLines = newString.split('\n');
  const adjusted = newLines.map((line, i) => {
    if (i === 0) {
      // First line: replace old base indent with file base indent
      if (line.startsWith(oldBaseIndent)) {
        return fileBaseIndent + line.slice(oldBaseIndent.length);
      }
      return fileBaseIndent + line.trimStart();
    }

    if (deltaLen > 0) {
      return deltaPrefix + line;
    }

    if (deltaLen < 0) {
      // Remove up to |deltaLen| characters of leading whitespace
      const strip = Math.abs(deltaLen);
      const currentIndent = getIndent(line);
      const removeCount = Math.min(strip, currentIndent.length);
      return line.slice(removeCount);
    }

    return line;
  });

  return adjusted.join('\n');
}

// ---------------------------------------------------------------------------
// Quote style preservation
// ---------------------------------------------------------------------------

const LEFT_DOUBLE_CURLY = '\u201c';
const RIGHT_DOUBLE_CURLY = '\u201d';
const LEFT_SINGLE_CURLY = '\u2018';
const RIGHT_SINGLE_CURLY = '\u2019';

/**
 * Decide whether the quote at `chars[index]` sits in an OPENING context: it is
 * at the start of the string, or the preceding character is whitespace, an
 * opening bracket, or an em/en dash. Everything else is a closing context.
 *
 * This is a per-character context test (mirroring Claude Code's `isOpeningContext`)
 * \u2014 it stays correct even when earlier quotes were skipped, unlike an even/odd
 * running-parity counter which mis-flips every quote after a skipped/contraction
 * apostrophe or a bracket-opened quote.
 */
function isOpeningContext(chars: string[], index: number): boolean {
  if (index === 0) return true;
  const prev = chars[index - 1];
  return (
    prev === ' ' ||
    prev === '\t' ||
    prev === '\n' ||
    prev === '\r' ||
    prev === '(' ||
    prev === '[' ||
    prev === '{' ||
    prev === '\u2014' || // em dash
    prev === '\u2013' // en dash
  );
}

/** Restyle every ASCII `"` in `str` to the context-appropriate curly double quote. */
function applyCurlyDoubleQuotes(str: string): string {
  const chars = [...str];
  const result: string[] = [];
  for (let i = 0; i < chars.length; i++) {
    if (chars[i] === '"') {
      result.push(isOpeningContext(chars, i) ? LEFT_DOUBLE_CURLY : RIGHT_DOUBLE_CURLY);
    } else {
      result.push(chars[i]);
    }
  }
  return result.join('');
}

/**
 * Restyle every ASCII `'` in `str` to the context-appropriate curly single quote.
 * An apostrophe flanked by two letters is a contraction (don't, it's) and always
 * becomes the RIGHT single curly quote, so it never corrupts the open/close state
 * of surrounding real quotes.
 */
function applyCurlySingleQuotes(str: string): string {
  const chars = [...str];
  const result: string[] = [];
  for (let i = 0; i < chars.length; i++) {
    if (chars[i] === "'") {
      const prev = i > 0 ? chars[i - 1] : undefined;
      const next = i < chars.length - 1 ? chars[i + 1] : undefined;
      const prevIsLetter = prev !== undefined && /\p{L}/u.test(prev);
      const nextIsLetter = next !== undefined && /\p{L}/u.test(next);
      if (prevIsLetter && nextIsLetter) {
        // Contraction apostrophe \u2014 always a right single curly quote.
        result.push(RIGHT_SINGLE_CURLY);
      } else {
        result.push(isOpeningContext(chars, i) ? LEFT_SINGLE_CURLY : RIGHT_SINGLE_CURLY);
      }
    } else {
      result.push(chars[i]);
    }
  }
  return result.join('');
}

/**
 * When the matched file region uses curly typography but the replacement was
 * written with ASCII quotes, restyle the replacement's ASCII quotes to curly so
 * the edit preserves the file's typographic style. A replacement that already
 * contains curly quotes of that kind is left untouched (the author chose their
 * own glyphs).
 */
function preserveQuoteStyle(original: string, replacement: string): string {
  const hasCurlyDouble = /[\u201c\u201d]/.test(original);
  const hasCurlySingle = /[\u2018\u2019]/.test(original);
  let result = replacement;
  if (hasCurlyDouble && !/[\u201c\u201d]/.test(result)) {
    result = applyCurlyDoubleQuotes(result);
  }
  if (hasCurlySingle && !/[\u2018\u2019]/.test(result)) {
    result = applyCurlySingleQuotes(result);
  }
  return result;
}

// ---------------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------------

const STRATEGIES: Array<{
  name: StrategyName;
  find: (content: string, pattern: string) => MatchPosition[];
}> = [
  { name: 'exact', find: findExact },
  { name: 'line_trimmed', find: findLineTrimmed },
  { name: 'whitespace_normalized', find: findWhitespaceNormalized },
  { name: 'indentation_flexible', find: findIndentationFlexible },
  { name: 'escape_normalized', find: findEscapeNormalized },
  { name: 'desanitize_normalized', find: findDesanitized },
  { name: 'block_anchor', find: findBlockAnchor },
];

/**
 * Try each fuzzy-matching strategy in order.  The first strategy that
 * yields exactly one match wins.  For non-exact matches, indentation is
 * automatically adjusted via `reindentReplacement`.
 */
export function fuzzyFindAndReplace(
  content: string,
  oldString: string,
  newString: string,
): FuzzyResult {
  // An empty old_string has no anchor to locate — matching it would be a no-op
  // insertion at an arbitrary position (and the escape/indexOf paths would even
  // report every offset as a "match"). Reject it up front with a clean, typed
  // failure rather than clobbering the file. Non-empty whitespace is still
  // allowed through so a genuine exact run of spaces/tabs can match.
  if (oldString === '') {
    return { newContent: null, strategy: null, matchCount: 0, error: 'old_string is empty' };
  }

  // Deletion (new_string === ''): if old_string does not itself end in a newline but
  // the file contains old_string followed by '\n', delete the whole line INCLUDING its
  // trailing newline so no empty line is left behind. Only when that yields a single
  // match; otherwise fall through to the normal strategies.
  if (newString === '' && oldString.length > 0 && !oldString.endsWith('\n')) {
    const withNewline = findExact(content, oldString + '\n');
    if (withNewline.length === 1) {
      const { start, end } = withNewline[0];
      const newContent = content.slice(0, start) + content.slice(end);
      return { newContent, strategy: 'exact', matchCount: 1, error: null };
    }
  }

  for (const { name, find } of STRATEGIES) {
    const matches = find(content, oldString);

    if (matches.length === 1) {
      const { start, end } = matches[0];
      const matchedRegion = content.slice(start, end);

      let replacement = newString;
      if (name === 'desanitize_normalized') {
        // Re-apply the SAME sanitized→raw substitutions to the replacement so the
        // written bytes use the raw token forms, matching the surrounding file.
        replacement = desanitizeMatchString(newString);
      } else if (name !== 'exact') {
        replacement = reindentReplacement(matchedRegion, oldString, newString);
      }
      replacement = preserveQuoteStyle(matchedRegion, replacement);

      const newContent = content.slice(0, start) + replacement + content.slice(end);
      // Post-apply identity check: if normalization/reindent/quote-restyling made
      // the replacement byte-identical to what was already there, the edit changed
      // nothing. Surface it as a no-op error instead of a phantom "applied".
      if (newContent === content) {
        return { newContent: null, strategy: name, matchCount: 1, error: NOOP_EDIT_ERROR };
      }
      return { newContent, strategy: name, matchCount: 1, error: null };
    }

    if (matches.length > 1) {
      return {
        newContent: null,
        strategy: name,
        matchCount: matches.length,
        error: `old_string matches ${matches.length} locations (strategy: ${name}). Add more surrounding context to make it unique, or use replace_all: true to replace all occurrences.`,
      };
    }

    // 0 matches — try next strategy
  }

  // No strategy found a match
  return {
    newContent: null,
    strategy: null,
    matchCount: 0,
    error: 'old_string not found in file',
  };
}

// ---------------------------------------------------------------------------
// "Did you mean?" helper
// ---------------------------------------------------------------------------

/**
 * Simple character-level similarity: ratio of matching characters in order
 * (longest common subsequence length) divided by the max string length.
 * This is cheaper than full Levenshtein and good enough for hints.
 */
function similarity(a: string, b: string): number {
  if (a === b) return 1;
  const maxLen = Math.max(a.length, b.length);
  if (maxLen === 0) return 1;

  // Count matching characters (order-preserving, greedy)
  let matches = 0;
  let bIdx = 0;
  for (let i = 0; i < a.length && bIdx < b.length; i++) {
    if (a[i] === b[bIdx]) {
      matches++;
      bIdx++;
    }
  }
  return matches / maxLen;
}

/**
 * Find lines in `content` that are most similar to the first and last
 * lines of `searchLines` — useful for generating "did you mean?" hints
 * in error messages.
 */
export function findClosestLines(
  content: string,
  searchLines: string[],
  maxResults = 3,
): Array<{ lineNumber: number; line: string; similarity: number }> {
  if (searchLines.length === 0) return [];

  const targetLine = searchLines[0].trim();
  if (targetLine.length === 0) return [];

  const contentLines = content.split('\n');
  const scored: Array<{ lineNumber: number; line: string; similarity: number }> = [];

  for (let i = 0; i < contentLines.length; i++) {
    const sim = similarity(contentLines[i].trim(), targetLine);
    if (sim > 0.4) {
      scored.push({ lineNumber: i + 1, line: contentLines[i], similarity: sim });
    }
  }

  scored.sort((a, b) => b.similarity - a.similarity);
  return scored.slice(0, maxResults);
}
