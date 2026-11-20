import { fuzzyFindAndReplace, NOOP_EDIT_ERROR } from './fuzzy-match';
import { normalizeToLf } from './tool-utils';

/**
 * A single search-and-replace edit, as accepted by the `file_edit` tool's
 * `edits[]` batch form. Mirrors the shape used by {@link applySequentialEdits}
 * and the reference's `FileEdit` type.
 */
export interface FileEditLike {
  old_string: string;
  new_string: string;
  replace_all?: boolean;
}

/**
 * Apply an ordered list of edits to `content`, returning the resulting file.
 *
 * Reuses the ONE canonical edit engine ({@link fuzzyFindAndReplace}) so an
 * equivalence verdict reflects what the tool would ACTUALLY produce — no second,
 * drifting apply implementation. Throws a deterministic, normalized `Error` when
 * an edit cannot be applied (empty needle, not found, or an ambiguous match
 * without `replace_all`), so two edit sets that fail the same way compare equal
 * (parity with the reference's `getPatchForEdits`, whose thrown message is part of
 * the equivalence contract). Edits are normalized to LF first so CRLF-vs-LF
 * differences never masquerade as a semantic divergence.
 */
export function applyEditsToContent(content: string, edits: FileEditLike[]): string {
  let current = normalizeToLf(content);
  for (const edit of edits) {
    const oldStr = normalizeToLf(edit.old_string ?? '');
    const newStr = normalizeToLf(edit.new_string ?? '');
    if (oldStr.length === 0) {
      throw new Error('old_string must not be empty');
    }
    const result = fuzzyFindAndReplace(current, oldStr, newStr);
    if (result.error) {
      // A no-op edit (normalization/reindent/quote-restyling erased the change)
      // leaves content byte-identical — carry `current` forward unchanged rather
      // than throwing, so "edit that does nothing" and "no edit" compare equal.
      if (result.error === NOOP_EDIT_ERROR) {
        continue;
      }
      if (edit.replace_all && result.matchCount > 1) {
        current = current.split(oldStr).join(newStr);
        continue;
      }
      if (result.matchCount === 0) {
        throw new Error('old_string not found');
      }
      throw new Error(`old_string matches ${result.matchCount} locations`);
    }
    current = result.newContent!;
  }
  return current;
}

/** True when two edits have byte-identical inputs (the literal fast path). */
function editInputsEqual(a: FileEditLike, b: FileEditLike): boolean {
  return (
    a.old_string === b.old_string &&
    a.new_string === b.new_string &&
    Boolean(a.replace_all) === Boolean(b.replace_all)
  );
}

/**
 * Decide whether two edit sets are EQUIVALENT — they produce the same file from
 * the same starting `originalContent`, OR they both fail identically.
 *
 * Parity with the reference's `areFileEditsEquivalent` (FileEditTool/utils.ts):
 *   1. Fast path — literal input equality (same length, same old/new/replace_all
 *      pairwise), no apply needed.
 *   2. Semantic path — apply both sets to `originalContent` and compare the
 *      resulting files. Two sets that throw are equal only when they throw the
 *      SAME message (a reproducible failure); one-throws-one-succeeds is unequal.
 *
 * This lets the tool recognize a re-submitted / duplicated edit as a no-op even
 * when it is phrased differently, without re-prompting.
 */
export function areFileEditsEquivalent(
  edits1: FileEditLike[],
  edits2: FileEditLike[],
  originalContent: string,
): boolean {
  if (
    edits1.length === edits2.length &&
    edits1.every((e1, i) => editInputsEqual(e1, edits2[i]))
  ) {
    return true;
  }

  let result1: string | null = null;
  let error1: string | null = null;
  let result2: string | null = null;
  let error2: string | null = null;

  try {
    result1 = applyEditsToContent(originalContent, edits1);
  } catch (e) {
    error1 = e instanceof Error ? e.message : String(e);
  }
  try {
    result2 = applyEditsToContent(originalContent, edits2);
  } catch (e) {
    error2 = e instanceof Error ? e.message : String(e);
  }

  // Both failed: equal only when they failed the same way.
  if (error1 !== null && error2 !== null) return error1 === error2;
  // Exactly one failed: not equivalent.
  if (error1 !== null || error2 !== null) return false;
  // Both succeeded: equal when they produce the same file.
  return result1 === result2;
}
