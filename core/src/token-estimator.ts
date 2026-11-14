import type { Message } from './types';
// Single source of truth for the ~4-chars/token heuristic (dedup: audit #16). Re-export
// it here too so existing importers of estimateTokens from this module keep working.
import { estimateTokens } from './utils/tokens';
export { estimateTokens, estimateTokensForFileType, charsPerTokenForFileType } from './utils/tokens';

const OVERHEAD_PER_MESSAGE = 4; // role header, delimiters

/**
 * Conservative fixed token estimate for an image or document (base64 PDF) block.
 *
 * These blocks must NOT be char-counted: a 1 MB PDF is ~1.33M base64 chars, which
 * the naive chars/token path would score at ~330k tokens, vs the ~2000 the API
 * actually charges. Counting them as 0 (the old behavior) is the opposite error —
 * it lets large images/PDFs slip past token budgets and trigger auto-compact too
 * late. 2000 matches Claude's IMAGE_MAX_TOKEN_SIZE conservative estimate.
 */
export const IMAGE_DOCUMENT_TOKEN_ESTIMATE = 2000;

export function estimateToolCallTokens(
  toolName: string,
  input: Record<string, unknown>,
): number {
  const serialized = JSON.stringify(input);
  return estimateTokens(toolName) + estimateTokens(serialized) + 10; // tool call framing
}

/**
 * Rough token estimate for a single content block, dispatching on block type —
 * mirrors Claude's roughTokenCountEstimationForBlock. Every non-text block type
 * is handled explicitly rather than counted as zero:
 *  - text            → the text
 *  - tool_use        → name + JSON(input) (bash commands / Edit diffs / file bodies are large)
 *  - thinking        → the thinking text
 *  - redacted_thinking → the opaque .data payload
 *  - image/document  → a fixed conservative {@link IMAGE_DOCUMENT_TOKEN_ESTIMATE}
 *  - tool_result     → recurse into its content (string or nested blocks)
 *  - anything else   → the serialized block (server_tool_use, web_search, mcp_tool_use, …)
 */
export function estimateBlockTokens(block: unknown): number {
  if (typeof block === 'string') return estimateTokens(block);
  if (!block || typeof block !== 'object') return 0;
  const b = block as { type?: string; text?: unknown; name?: unknown; input?: unknown; thinking?: unknown; data?: unknown; content?: unknown };
  switch (b.type) {
    case 'text':
      return typeof b.text === 'string' ? estimateTokens(b.text) : 0;
    case 'tool_use':
      return estimateTokens((typeof b.name === 'string' ? b.name : '') + JSON.stringify(b.input ?? {}));
    case 'thinking':
      return typeof b.thinking === 'string' ? estimateTokens(b.thinking) : 0;
    case 'redacted_thinking':
      return typeof b.data === 'string' ? estimateTokens(b.data) : 0;
    case 'image':
    case 'document':
      return IMAGE_DOCUMENT_TOKEN_ESTIMATE;
    case 'tool_result':
      return estimateContentTokens(b.content);
    default:
      return estimateTokens(JSON.stringify(block));
  }
}

/** Estimate tokens for a message `content` field: a string, an array of blocks, or nullish. */
export function estimateContentTokens(content: unknown): number {
  if (content === null || content === undefined) return 0;
  if (typeof content === 'string') return estimateTokens(content);
  if (Array.isArray(content)) {
    let total = 0;
    for (const part of content) total += estimateBlockTokens(part);
    return total;
  }
  return estimateTokens(JSON.stringify(content));
}

export function estimateMessageTokens(message: Message | { role: string; content: unknown }): number {
  if (message.content === null || message.content === undefined) {
    return OVERHEAD_PER_MESSAGE;
  }
  return estimateContentTokens(message.content) + OVERHEAD_PER_MESSAGE;
}

export function estimateConversationTokens(messages: Array<Message | { role: string; content: unknown }>): number {
  let total = 3; // conversation framing
  for (const msg of messages) {
    total += estimateMessageTokens(msg);
  }
  return total;
}
