import { NOOP_EDIT_ERROR, desanitizeMatchString, findBlockAnchor, findDesanitized, findExact, findIndentationFlexible, findLineTrimmed, findWhitespaceNormalized } from './fuzzy-match-desanitizations';
import type { FuzzyResult, MatchPosition, StrategyName } from './fuzzy-match-desanitizations';
export { DESANITIZATIONS, NOOP_EDIT_ERROR, STRATEGY_NAMES, desanitizeMatchString } from './fuzzy-match-desanitizations';
export type { FuzzyResult, MatchPosition, StrategyName } from './fuzzy-match-desanitizations';

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
 * This is a per-character context test (mirroring the reference's `isOpeningContext`)
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
