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
 * ALL tags (no human-authored prose survives the strip). Parity: the reference's
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

/**
 * Narrowly-scoped tag block pattern keyed to an EXPLICIT allowlist of the
 * wrapper tags AMI injects into a user turn — system reminders
 * (`<system-reminder>`, `<system>`), IDE context (`<ide_opened_file>`,
 * `<ide_selection>`), and local slash-command markers (`<command-name>`,
 * `<command-message>`, `<command-args>`, `<command-prefix>`,
 * `<local-command-stdout>`, `<local-command-stderr>`).
 *
 * Unlike {@link XML_TAG_BLOCK_PATTERN} (which strips ANY lowercase block), this
 * matches ONLY the injected-context families above so genuine user-typed markup —
 * lowercase HTML like `<code>foo</code>`, a `<div>`, a `<pre>` snippet — survives
 * untouched. Parity with the reference's `IDE_CONTEXT_TAGS_PATTERN`
 * (references/claude/claude-code-main/src/utils/displayTags.ts:41-43), widened to
 * the tag set AMI actually appends. The `command-[\w-]*` / `local-command-[\w-]*`
 * arms cover the whole marker family with one bounded alternative; the
 * backreference (`\1`) requires an exactly-matching close so adjacent blocks stay
 * separate and an unpaired `<command-name>` without a close is left alone.
 */
// eslint-disable-next-line security/detect-unsafe-regex -- quantifiers are sequential, not nested: the alternation of fixed/`[\w-]*` names, optional attribute run (`[^>]*`), and lazy body (`[\s\S]*?`) are separated by literals, so matching is linear per start position (no catastrophic backtracking). The backreference only trips safe-regex's conservative heuristic.
const INJECTED_CONTEXT_TAGS_PATTERN = /<(system-reminder|system|ide_opened_file|ide_selection|command-[\w-]*|local-command-[\w-]*)(?:\s[^>]*)?>[\s\S]*?<\/\1>\n?/g;

/**
 * Strip ONLY injected-context wrapper blocks ({@link INJECTED_CONTEXT_TAGS_PATTERN})
 * from text, preserving every other character — in particular user-typed lowercase
 * HTML. Used on the up-arrow recall / resubmit path so a prompt restored for
 * re-editing drops any injected context block (a `<system-reminder>` or IDE/command
 * marker that was concatenated into the stored turn) while keeping the human-authored
 * prose and markup exactly as typed. Parity: the reference's `stripIdeContextTags`
 * (utils/displayTags.ts:45-51). Returns the trimmed remainder (possibly empty, so a
 * caller can detect an all-injected turn and skip it).
 */
export function stripInjectedContextTags(text: string): string {
  return text.replace(INJECTED_CONTEXT_TAGS_PATTERN, '').trim();
}

/** Escape the regex metacharacters in an arbitrary tag name so it can be spliced
 *  into a dynamically-built pattern as a literal. Inlined (not a shared helper) to
 *  keep {@link extractTag} self-contained; it is the only consumer. */
function escapeTagName(tag: string): string {
  return tag.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Extract the inner content of the FIRST correctly-nested `<tag>…</tag>` block
 * for an arbitrary `tagName`, returning `null` when absent. The reusable,
 * parameterized complement to the strip helpers above: where {@link stripDisplayTags}
 * REMOVES wrapper blocks, this PULLS a named channel out of message content — e.g.
 * the command name/args/output markers a local slash-command turn carries
 * (`<command-name>`, `<bash-input>`, `<local-command-stdout>`). Parity: the reference's
 * `extractTag` (references/claude/claude-code-main/src/utils/messages.ts:633-687),
 * used by `UserLocalCommandOutputMessage` to render local-command stdout/stderr.
 *
 * Behaviour matched to the reference:
 *   - case-insensitive tag match; opening tag may carry attributes
 *     (`<tag foo="bar">`); the body is multi-line and matched non-greedily.
 *   - empty/blank `html` or `tagName` ⇒ `null`.
 *   - nesting-aware: a match is only returned when it sits at nesting depth 0
 *     (counting opening/closing tags of the same name that precede it), so a
 *     nested same-name block does not return the inner fragment of an outer one.
 *   - returns `null` when a match is found but its content is empty.
 */
export function extractTag(html: string, tagName: string): string | null {
  if (!html.trim() || !tagName.trim()) return null;

  const escaped = escapeTagName(tagName);
  // eslint-disable-next-line security/detect-non-literal-regexp -- `escaped` is regex-escaped above, so the only dynamic input is a literal tag name; no injection.
  const pattern = new RegExp(`<${escaped}(?:\\s+[^>]*)?>([\\s\\S]*?)<\\/${escaped}>`, 'gi');
  // eslint-disable-next-line security/detect-non-literal-regexp -- same escaped literal; counts nesting depth before a match.
  const openingTag = new RegExp(`<${escaped}(?:\\s+[^>]*?)?>`, 'gi');
  // eslint-disable-next-line security/detect-non-literal-regexp -- same escaped literal; counts nesting depth before a match.
  const closingTag = new RegExp(`<\\/${escaped}>`, 'gi');

  let match: RegExpExecArray | null;
  let lastIndex = 0;
  while ((match = pattern.exec(html)) !== null) {
    const content = match[1];
    const beforeMatch = html.slice(lastIndex, match.index);

    let depth = 0;
    openingTag.lastIndex = 0;
    while (openingTag.exec(beforeMatch) !== null) depth++;
    closingTag.lastIndex = 0;
    while (closingTag.exec(beforeMatch) !== null) depth--;

    if (depth === 0 && content) return content;
    lastIndex = match.index + match[0].length;
  }
  return null;
}
