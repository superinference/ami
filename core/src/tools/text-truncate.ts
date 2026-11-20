/**
 * Shared single-line, grapheme-safe text truncation for background-task / cron
 * list rendering. ONE canonical implementation (No-Duplication rule) consumed by
 * both {@link ./task-list} (activity/command column) and {@link ./cron-list}
 * (prompt preview), so a cut never splits a multi-code-unit character.
 */

/**
 * Split a string into grapheme clusters so a cut never splits a multi-code-unit
 * character (emoji / surrogate pair / combining mark) in half — which would
 * otherwise emit a lone surrogate and corrupt the terminal line. Uses
 * `Intl.Segmenter` for full grapheme-cluster awareness (always present under the
 * full-ICU Node runtime AMI ships on).
 */
export function graphemes(s: string): string[] {
  const seg = new Intl.Segmenter(undefined, { granularity: 'grapheme' });
  return Array.from(seg.segment(s), (g) => g.segment);
}

/**
 * PARITY (the reference `utils/truncate.ts` `truncate(str, maxWidth, singleLine)`,
 * used by the BackgroundTask activity line with `activityLimit ?? 40`): bound a
 * task's activity/command string to a single line of at most `maxWidth` grapheme
 * clusters, appending a single-character ellipsis (`…`) when anything is cut.
 *
 * - When `singleLine` is set and the string contains a newline, only the first
 *   line is kept; the collapse is marked with a trailing `…`. If that first line
 *   is itself wider than the budget it is then width-truncated (ellipsis included
 *   within `maxWidth`).
 * - A string already within the budget (and without a collapsed newline) is
 *   returned verbatim with NO ellipsis.
 * - `maxWidth <= 0` collapses to the bare ellipsis.
 */
export function truncateActivity(str: string, maxWidth: number, singleLine = false): string {
  if (maxWidth <= 0) return '…';

  if (singleLine) {
    const nl = str.indexOf('\n');
    if (nl !== -1) {
      const firstLine = str.slice(0, nl);
      const cells = graphemes(firstLine);
      // The collapse itself consumes the ellipsis; if the first line already
      // fills (or overflows) the budget once the marker is added, width-truncate.
      if (cells.length + 1 > maxWidth) {
        return `${cells.slice(0, Math.max(0, maxWidth - 1)).join('')}…`;
      }
      return `${firstLine}…`;
    }
  }

  const cells = graphemes(str);
  if (cells.length <= maxWidth) return str;
  return `${cells.slice(0, maxWidth - 1).join('')}…`;
}
