/**
 * PARITY (additive): shared command-preview truncation for task tools.
 *
 * Mirrors the reference's TaskStop `truncateCommand` (UI.tsx): cap a stopped/listed
 * task's command to a short, bounded preview so echoing it back never floods the
 * model's context or the transcript. the reference's defaults are 2 display lines and
 * 160 characters; we keep those exact caps.
 *
 * Divergence note: the reference measures width with `stringWidth` (CJK-aware) in the ink
 * renderer. This helper lives in core/ (no ink, no terminal), so it caps by code-unit
 * length — a conservative, deterministic approximation that is correct for the common
 * ASCII case and never under-truncates. The caller surfaces the FULL command in
 * structured `metadata.command`; this preview is only for the human-readable string.
 */

export const MAX_COMMAND_DISPLAY_LINES = 2;
export const MAX_COMMAND_DISPLAY_CHARS = 160;

export interface CommandPreview {
  /** The truncated, trimmed preview text (never multi-line beyond the cap). */
  text: string;
  /** True when the preview omitted content (lines and/or characters were dropped). */
  truncated: boolean;
}

/**
 * Produce a bounded single/double-line preview of a task command.
 *
 * - Keeps at most {@link MAX_COMMAND_DISPLAY_LINES} lines.
 * - Then caps the result at {@link MAX_COMMAND_DISPLAY_CHARS} characters.
 * - Trims trailing/leading whitespace, matching the reference's `truncateCommand`.
 */
export function previewCommand(
  command: string,
  opts: { maxLines?: number; maxChars?: number } = {},
): CommandPreview {
  const maxLines = opts.maxLines ?? MAX_COMMAND_DISPLAY_LINES;
  const maxChars = opts.maxChars ?? MAX_COMMAND_DISPLAY_CHARS;

  const lines = command.split('\n');
  let truncated = command;
  let didTruncate = false;

  if (lines.length > maxLines) {
    truncated = lines.slice(0, maxLines).join('\n');
    didTruncate = true;
  }

  if (truncated.length > maxChars) {
    truncated = truncated.slice(0, maxChars);
    didTruncate = true;
  }

  const text = truncated.trim();
  return { text, truncated: didTruncate };
}
