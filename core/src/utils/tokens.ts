/**
 * Average number of characters per token. This is a rough heuristic that works
 * reasonably well for English text across most LLM tokenisers.
 */
const CHARS_PER_TOKEN = 4;

/**
 * Provides a rough token estimate for a piece of text based on a
 * characters-per-token heuristic (default ~4).
 *
 * The ratio is configurable so a caller that knows its content is denser than
 * average prose (e.g. JSON, where `{`/`}`/`:`/`,`/`"` are mostly single-char
 * tokens) can pass a tighter value and avoid under-counting. Mirrors Claude's
 * `roughTokenCountEstimation(content, bytesPerToken = 4)`.
 */
export function estimateTokens(text: string, charsPerToken: number = CHARS_PER_TOKEN): number {
  if (text.length === 0) return 0;
  // Guard against a non-positive ratio producing Infinity/NaN; fall back to the default.
  const ratio = charsPerToken > 0 ? charsPerToken : CHARS_PER_TOKEN;
  return Math.ceil(text.length / ratio);
}

/**
 * Chars-per-token ratio for a given file extension. Dense structured text
 * (JSON/JSONL/JSONC) is dominated by single-character tokens (`{`, `}`, `:`,
 * `,`, `"`), so its real ratio is closer to 2 than the default ~4 — using the
 * generic ratio badly UNDER-estimates JSON and can let an oversized tool
 * result slip past a token budget. Mirrors Claude's bytesPerTokenForFileType.
 */
export function charsPerTokenForFileType(fileExtension: string): number {
  switch (fileExtension.toLowerCase().replace(/^\./, '')) {
    case 'json':
    case 'jsonl':
    case 'jsonc':
      return 2;
    default:
      return CHARS_PER_TOKEN;
  }
}

/**
 * Like {@link estimateTokens} but uses a content-type-aware chars-per-token
 * ratio when the file type is known (dense JSON counts ~2 chars/token). Used
 * when an API-based token count is unavailable and the rough estimate must not
 * under-count a structured payload.
 */
export function estimateTokensForFileType(text: string, fileExtension: string): number {
  return estimateTokens(text, charsPerTokenForFileType(fileExtension));
}

/**
 * Estimate the total token cost of an array of tool schemas
 * (name + description + JSON-serialised inputSchema).
 *
 * `charsPerToken` defaults to the generic ~4 ratio; because a tool's
 * `inputSchema` is serialised to dense JSON, a caller that wants a tighter
 * (less optimistic) estimate can pass `charsPerTokenForFileType('json')` so the
 * schema portion is not under-counted.
 */
export function estimateToolSchemaTokens(
  tools: Array<{ name: string; description: string; inputSchema: unknown }>,
  charsPerToken: number = CHARS_PER_TOKEN,
): number {
  let total = 0;
  for (const tool of tools) {
    total += estimateTokens(tool.name + tool.description + JSON.stringify(tool.inputSchema), charsPerToken);
  }
  return total;
}

/**
 * Truncates text so that it approximately fits within `maxTokens`.
 * If the text is already within the limit it is returned unchanged.
 * Otherwise it is sliced and a `[truncated]` marker is appended.
 *
 * `charsPerToken` (default ~4) controls the ratio used for both the budget
 * check and the slice length, so dense content (e.g. JSON — pass
 * `charsPerTokenForFileType('json')`) is truncated against its real size rather
 * than the optimistic generic ratio, which would otherwise under-truncate and
 * leave the payload larger than `maxTokens` permits.
 */
export function truncateToTokenLimit(
  text: string,
  maxTokens: number,
  charsPerToken: number = CHARS_PER_TOKEN,
): string {
  if (maxTokens <= 0) return '[truncated]';
  const ratio = charsPerToken > 0 ? charsPerToken : CHARS_PER_TOKEN;
  if (estimateTokens(text, ratio) <= maxTokens) return text;

  // Reserve a small amount of space for the truncation marker
  const markerText = '\n[truncated]';
  const markerTokens = estimateTokens(markerText, ratio);
  const availableTokens = Math.max(0, maxTokens - markerTokens);
  const maxChars = availableTokens * ratio;

  return text.slice(0, maxChars) + markerText;
}
