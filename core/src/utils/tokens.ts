/**
 * Average number of characters per token. This is a rough heuristic that works
 * reasonably well for English text across most LLM tokenisers.
 */
const CHARS_PER_TOKEN = 4;

/**
 * Provides a rough token estimate for a piece of text based on the
 * ~4 characters-per-token heuristic.
 */
export function estimateTokens(text: string): number {
  if (text.length === 0) return 0;
  return Math.ceil(text.length / CHARS_PER_TOKEN);
}

/**
 * Estimate the total token cost of an array of tool schemas
 * (name + description + JSON-serialised inputSchema).
 */
export function estimateToolSchemaTokens(tools: Array<{ name: string; description: string; inputSchema: unknown }>): number {
  let total = 0;
  for (const tool of tools) {
    total += estimateTokens(tool.name + tool.description + JSON.stringify(tool.inputSchema));
  }
  return total;
}

/**
 * Truncates text so that it approximately fits within `maxTokens`.
 * If the text is already within the limit it is returned unchanged.
 * Otherwise it is sliced and a `[truncated]` marker is appended.
 */
export function truncateToTokenLimit(text: string, maxTokens: number): string {
  if (maxTokens <= 0) return '[truncated]';
  if (estimateTokens(text) <= maxTokens) return text;

  // Reserve a small amount of space for the truncation marker
  const markerText = '\n[truncated]';
  const markerTokens = estimateTokens(markerText);
  const availableTokens = Math.max(0, maxTokens - markerTokens);
  const maxChars = availableTokens * CHARS_PER_TOKEN;

  return text.slice(0, maxChars) + markerText;
}
