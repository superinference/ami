/**
 * Shared helper for deriving clean UI titles/previews from a raw first user
 * message. System-injected context — appended `# Context Files` blocks,
 * `<system-reminder>` wrappers, IDE metadata, hook output, task notifications,
 * channel messages — arrives wrapped in XML-like `<tag>…</tag>` blocks and must
 * never surface in a session title or transcript preview.
 */

/**
 * Matches any XML-like `<tag>…</tag>` BLOCK — tag and enclosed content alike —
 * with a lowercase tag name, optional attributes, and multi-line body.
 *
 * Three deliberate design choices make this a drop-in replacement for an
 * ever-growing allowlist of wrapper types:
 *   1. It matches ANY lowercase tag block (`[a-z][\w-]*`) rather than enumerating
 *      known notification types, so a new wrapper kind is stripped automatically.
 *   2. The `[a-z]`-first anchor is a special-case: user prose carrying JSX/HTML
 *      (`<Button>`, `<!DOCTYPE html>`, an uppercase `<CODE>`) or an unpaired
 *      `x < y` starts with an uppercase letter or `!` and passes through untouched.
 *   3. The backreferenced closing tag (`\1`) with a non-greedy body keeps adjacent
 *      blocks separate — a non-greedy match won't swallow the text sitting between
 *      two differently-named tagged blocks.
 */
// eslint-disable-next-line security/detect-unsafe-regex -- quantifiers are sequential, not nested: the tag name (`[\w-]*`), optional attribute run (`[^>]*`), and lazy body (`[\s\S]*?`) are separated by literals, so matching is linear per start position (no catastrophic backtracking). The backreference only trips safe-regex's conservative heuristic.
const XML_TAG_BLOCK_PATTERN = /<([a-z][\w-]*)(?:\s[^>]*)?>[\s\S]*?<\/\1>\n?/g;

/**
 * Strip XML-like tag blocks from text for use in a UI title/preview. If stripping
 * would leave nothing behind (the whole message was wrapper tags), the original
 * text is returned unchanged — better to show something than an empty title.
 */
export function stripDisplayTags(text: string): string {
  const result = text.replace(XML_TAG_BLOCK_PATTERN, '').trim();
  return result || text;
}

/**
 * Like {@link stripDisplayTags} but returns the EMPTY string when the message is
 * ALL tags (no human-authored prose survives the strip). Parity: Claude's
 * `stripDisplayTagsAllowEmpty` (utils/displayTags.ts:31-39), used so a title
 * deriver can DETECT a command-only / pure-injected-tag message (e.g. a bare
 * `/clear`, a lone `<session-start-hook>…</session-start-hook>`, or a
 * task-notification wrapper) and FALL THROUGH to the next real user message
 * instead of titling injected noise. `stripDisplayTags` deliberately returns the
 * original on an empty result ("better to show something than nothing") — that
 * fallback is wrong for the "is there any human content here?" question, which is
 * what this discriminator answers.
 */
export function stripDisplayTagsAllowEmpty(text: string): string {
  return text.replace(XML_TAG_BLOCK_PATTERN, '').trim();
}
