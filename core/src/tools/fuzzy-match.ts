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
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type StrategyName =
  | 'exact'
  | 'line_trimmed'
  | 'whitespace_normalized'
  | 'indentation_flexible'
  | 'escape_normalized';

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
  const contentLines = content.split('\n');
  const patternLines = pattern.split('\n');

  const normContentLines = contentLines.map(normalizeLine);
  const normPatternLines = patternLines.map(normalizeLine);

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
        end += contentLines[k].length + (k < i + normPatternLines.length - 1 ? 1 : 0);
      }
      results.push({ start, end });
    }
  }

  return results;
}

function findLineTrimmed(content: string, pattern: string): MatchPosition[] {
  return findByLine(content, pattern, l => l.trim());
}

function findWhitespaceNormalized(content: string, pattern: string): MatchPosition[] {
  return findByLine(content, pattern, l => l.replace(/\s+/g, ' ').trim());
}

function findIndentationFlexible(content: string, pattern: string): MatchPosition[] {
  return findByLine(content, pattern, l => l.replace(/^\s+/, ''));
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
    // Smart quote replacements (multi-byte -> single byte)
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
  for (const { name, find } of STRATEGIES) {
    const matches = find(content, oldString);

    if (matches.length === 1) {
      const { start, end } = matches[0];
      const matchedRegion = content.slice(start, end);

      let replacement = newString;
      if (name !== 'exact') {
        replacement = reindentReplacement(matchedRegion, oldString, newString);
      }

      const newContent = content.slice(0, start) + replacement + content.slice(end);
      return { newContent, strategy: name, matchCount: 1, error: null };
    }

    if (matches.length > 1) {
      return {
        newContent: null,
        strategy: name,
        matchCount: matches.length,
        error: `old_string found ${matches.length} times (strategy: ${name}). The old_string must be unique. Add more surrounding context to make it unique.`,
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
