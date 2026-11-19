/**
 * Output bounding for a single tool result (Orca-parity `tool-output-limits.ts`:
 * `pageOfText` / `truncationNote` / `capToolOutput`).
 *
 * How much one tool answer may hand an agent: long text is paged by offset, and
 * content still over the cap is cut with a marker that SAYS it was cut and how to
 * see less at once — never silently. Truncation/paging notes always go AFTER the
 * capped content so they survive the cut and reach the model.
 *
 * Two independent axes are bounded:
 *   - characters (`pageOfText` / `capToolOutput`, cap `TOOL_OUTPUT_MAX_CHARS`) —
 *     what the model reads; a page leaves room under the cap for the fence/notes.
 *   - bytes (`pageOfBytes` / `capToolOutputBytes`, cap `TOOL_DOWNLOAD_MAX_BYTES`) —
 *     what a download/attachment carries over the wire. Byte paging splits on a
 *     UTF-8 character boundary so a page is never a half-encoded codepoint.
 *
 * Pure, dependency-free, and deterministic so both the engine (common/) and any
 * frontend bound output identically — the paging offset in `nextOffset` is the
 * value the caller passes back to read the following page.
 */

/** List tool answers page this many items at a time (Orca `TOOL_LIST_PAGE_SIZE`). */
export const TOOL_LIST_PAGE_SIZE = 20;
/** The hard character cap one text answer may hand the model. */
export const TOOL_OUTPUT_MAX_CHARS = 20_000;
/** A text page leaves room under the cap for the fence and paging/truncation notes. */
export const TOOL_TEXT_PAGE_CHARS = 16_000;
/** The most bytes one answer carries (a download / attachment). */
export const TOOL_DOWNLOAD_MAX_BYTES = 2_900_000;

/** The marker prefix every AMI output-bound note carries so a model never mistakes it for content. */
export const BOUND_NOTE_PREFIX = "AMI";

/** One page of a long text, and where the next page starts when there is more. */
export function pageOfText(
  text: string,
  offset = 0,
  size = TOOL_TEXT_PAGE_CHARS,
): { text: string; total: number; nextOffset?: number } {
  const safeSize = Math.max(1, Math.floor(size));
  const start = Math.min(Math.max(0, Math.floor(offset)), text.length);
  const end = Math.min(text.length, start + safeSize);
  return {
    text: text.slice(start, end),
    total: text.length,
    ...(end < text.length ? { nextOffset: end } : {}),
  };
}

/** The line saying content was cut at a character cap, and how to see less at once. */
export function truncationNote(
  length: number,
  hint: string,
  max = TOOL_OUTPUT_MAX_CHARS,
): string {
  return `[${BOUND_NOTE_PREFIX}: truncated at ${max} of ${length} characters. ${hint}]`;
}

/** Character content cut at the cap with a marker naming how to see less; add paging notes after. */
export function capToolOutput(
  text: string,
  hint: string,
  max = TOOL_OUTPUT_MAX_CHARS,
): string {
  return text.length <= max
    ? text
    : `${text.slice(0, max)}\n${truncationNote(text.length, hint, max)}`;
}

/** Largest byte length of a prefix of `text` that is at most `maxBytes` AND ends on a char boundary. */
export function utf8SafeSliceEnd(text: string, maxBytes: number): number {
  if (maxBytes <= 0) return 0;
  if (Buffer.byteLength(text, "utf8") <= maxBytes) return text.length;
  let bytes = 0;
  let i = 0;
  for (const ch of text) {
    const chBytes = Buffer.byteLength(ch, "utf8");
    if (bytes + chBytes > maxBytes) break;
    bytes += chBytes;
    i += ch.length;
  }
  return i;
}

/** One page of a long text bounded by BYTES (never splitting a multibyte char), + where the next starts. */
export function pageOfBytes(
  text: string,
  offset = 0,
  maxBytes = TOOL_DOWNLOAD_MAX_BYTES,
): { text: string; totalBytes: number; nextOffset?: number } {
  const totalBytes = Buffer.byteLength(text, "utf8");
  const start = Math.min(Math.max(0, Math.floor(offset)), text.length);
  const rest = text.slice(start);
  const take = utf8SafeSliceEnd(rest, Math.max(0, Math.floor(maxBytes)));
  const end = start + take;
  return {
    text: text.slice(start, end),
    totalBytes,
    ...(end < text.length ? { nextOffset: end } : {}),
  };
}

/** The line saying content was cut at a BYTE cap, and how to see less at once. */
export function byteTruncationNote(
  byteLength: number,
  hint: string,
  maxBytes = TOOL_DOWNLOAD_MAX_BYTES,
): string {
  return `[${BOUND_NOTE_PREFIX}: truncated at ${formatMegabytes(maxBytes)} of ${formatMegabytes(byteLength)}. ${hint}]`;
}

/** Byte content cut at the cap (on a char boundary) with a marker; add paging notes after. */
export function capToolOutputBytes(
  text: string,
  hint: string,
  maxBytes = TOOL_DOWNLOAD_MAX_BYTES,
): string {
  const total = Buffer.byteLength(text, "utf8");
  if (total <= maxBytes) return text;
  const take = utf8SafeSliceEnd(text, maxBytes);
  return `${text.slice(0, take)}\n${byteTruncationNote(total, hint, maxBytes)}`;
}

/** Human-friendly megabytes, trimming a trailing `.0` (Orca `formatMegabytes`). */
export function formatMegabytes(bytes: number): string {
  return `${(bytes / 1_000_000).toFixed(1).replace(/\.0$/, "")} MB`;
}

/** What `boundToolOutput` did to a tool answer, so a caller can offer the next page explicitly. */
export interface BoundedToolOutput {
  /** The bounded text to hand the model (capped content + any note). */
  text: string;
  /** Total length of the ORIGINAL content in characters. */
  total: number;
  /** Whether content was cut (true ⇒ a truncation note is present in `text`). */
  truncated: boolean;
  /** The offset to pass back to read the next page, when the source had more. */
  nextOffset?: number;
}

/**
 * Bound one tool answer end to end: take the page at `offset`, cap it at `max`
 * characters, and append an explicit paging note when there is a next page — so
 * the model is NEVER handed silently-dropped output and always learns how to read
 * the rest. `hint` is the tool-specific "see less at once" advice (e.g. the flag
 * to pass) woven into the truncation note.
 */
export function boundToolOutput(
  text: string,
  options: {
    offset?: number;
    pageChars?: number;
    max?: number;
    hint?: string;
  } = {},
): BoundedToolOutput {
  const {
    offset = 0,
    pageChars = TOOL_TEXT_PAGE_CHARS,
    max = TOOL_OUTPUT_MAX_CHARS,
    hint = "Read the next page with a larger offset.",
  } = options;
  const page = pageOfText(text, offset, pageChars);
  const capped = capToolOutput(page.text, hint, max);
  const truncated = capped.length !== page.text.length;
  const parts = [capped];
  if (page.nextOffset !== undefined) {
    parts.push(
      `[${BOUND_NOTE_PREFIX}: ${page.total} characters total; next page at offset ${page.nextOffset}.]`,
    );
  }
  return {
    text: parts.join("\n"),
    total: text.length,
    truncated,
    ...(page.nextOffset !== undefined ? { nextOffset: page.nextOffset } : {}),
  };
}
