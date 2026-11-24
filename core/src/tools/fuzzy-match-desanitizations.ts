// Split from fuzzy-match.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).

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
 * `new_string`) recovers the edit. Mirrors the reference's DESANITIZATIONS.
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
 * reindent that reproduces the region verbatim). the reference rejects such edits
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

export function findExact(content: string, pattern: string): MatchPosition[] {
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
export function splitLogicalLines(value: string): { lines: string[]; terminated: boolean } {
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
export function findByLine(
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

export function findLineTrimmed(content: string, pattern: string): MatchPosition[] {
  return findByLine(content, pattern, l => l.trim());
}

/**
 * Desanitize the pattern (map sanitized token forms back to raw) then exact-match
 * it against the raw file content. Returns [] when the pattern had no sanitized
 * tokens (nothing new to try — the earlier strategies already covered it).
 */
export function findDesanitized(content: string, pattern: string): MatchPosition[] {
  const desanitized = desanitizeMatchString(pattern);
  if (desanitized === pattern) return [];
  return findExact(content, desanitized);
}

export function findWhitespaceNormalized(content: string, pattern: string): MatchPosition[] {
  return findByLine(content, pattern, l => l.replace(/\s+/g, ' ').trim());
}

export function findIndentationFlexible(content: string, pattern: string): MatchPosition[] {
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
export function findBlockAnchor(content: string, pattern: string): MatchPosition[] {
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
