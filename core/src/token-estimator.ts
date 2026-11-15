import type { Message } from './types';
// Single source of truth for the ~4-chars/token heuristic (dedup: audit #16). Re-export
// it here too so existing importers of estimateTokens from this module keep working.
import { estimateTokens, estimateTokensForFileType } from './utils/tokens';
export { estimateTokens, estimateTokensForFileType, charsPerTokenForFileType } from './utils/tokens';

const OVERHEAD_PER_MESSAGE = 4; // role header, delimiters

/** Default UTF-8 bytes-per-token ratio (mirrors Claude's `bytesPerToken = 4`). */
const BYTES_PER_TOKEN = 4;

/**
 * Pixels-per-token divisor for Claude's image token heuristic: an image costs
 * roughly `(width * height) / 750` tokens. Used by {@link estimateImageTokens}
 * when a block carries real pixel dimensions.
 */
const PIXELS_PER_IMAGE_TOKEN = 750;

/**
 * UTF-8 byte length of a string.
 *
 * `String.prototype.length` counts UTF-16 code units, which badly UNDER-counts
 * multi-byte content: a CJK character is 1 code unit but 3 UTF-8 bytes, and an
 * astral emoji is 2 code units but 4 bytes. Claude's `roughTokenCountEstimation`
 * divides the BYTE length (its parameter is literally `bytesPerToken`), so a
 * byte-based count is both closer to the real tokeniser and the correct parity
 * target for non-ASCII text. For pure ASCII this equals `.length`.
 */
export function utf8ByteLength(text: string): number {
  return Buffer.byteLength(text, 'utf8');
}

/**
 * Byte-aware rough token estimate for a piece of text: `ceil(utf8Bytes / ratio)`.
 *
 * Identical to {@link estimateTokens} for ASCII, but counts multi-byte unicode
 * by its UTF-8 byte length rather than UTF-16 code units — so CJK/emoji-heavy
 * text is no longer under-counted (which previously let such content slip past a
 * token budget). Mirrors Claude's byte-based `roughTokenCountEstimation`.
 */
export function estimateTextTokens(text: string, bytesPerToken: number = BYTES_PER_TOKEN): number {
  if (text.length === 0) return 0;
  const ratio = bytesPerToken > 0 ? bytesPerToken : BYTES_PER_TOKEN;
  return Math.ceil(utf8ByteLength(text) / ratio);
}

/**
 * Conservative token estimate for an image block.
 *
 * When real pixel dimensions are known, Claude charges roughly
 * `(width * height) / 750` tokens — so a small icon is a handful of tokens, not
 * the flat {@link IMAGE_DOCUMENT_TOKEN_ESTIMATE}. The result is capped at that
 * flat estimate so a very large image never scores above the conservative
 * ceiling (Claude down-samples oversized images before counting). With no valid
 * dimensions it falls back to the flat estimate, preserving prior behavior.
 */
export function estimateImageTokens(width?: number, height?: number): number {
  if (
    typeof width === 'number' && typeof height === 'number' &&
    Number.isFinite(width) && Number.isFinite(height) &&
    width > 0 && height > 0
  ) {
    return Math.min(IMAGE_DOCUMENT_TOKEN_ESTIMATE, Math.ceil((width * height) / PIXELS_PER_IMAGE_TOKEN));
  }
  return IMAGE_DOCUMENT_TOKEN_ESTIMATE;
}

/**
 * Pull numeric pixel dimensions off an image block, checking both the top level
 * (`{ width, height }`) and a nested `source` (`{ source: { width, height } }`).
 * Returns null when either dimension is missing or not a positive finite number.
 */
function readImageDimensions(block: {
  width?: unknown;
  height?: unknown;
  source?: unknown;
}): { width: number; height: number } | null {
  const pick = (w: unknown, h: unknown): { width: number; height: number } | null =>
    typeof w === 'number' && typeof h === 'number' &&
    Number.isFinite(w) && Number.isFinite(h) && w > 0 && h > 0
      ? { width: w, height: h }
      : null;
  const top = pick(block.width, block.height);
  if (top) return top;
  const src = block.source;
  if (src && typeof src === 'object') {
    const s = src as { width?: unknown; height?: unknown };
    return pick(s.width, s.height);
  }
  return null;
}

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
  // Serialized tool input is dense JSON (~2 chars/token) — the generic 4/token
  // ratio under-counts it and can let an oversized input slip past a budget.
  return estimateTokens(toolName) + estimateTokensForFileType(serialized, 'json') + 10; // tool call framing
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
  if (typeof block === 'string') return estimateTextTokens(block);
  if (!block || typeof block !== 'object') return 0;
  const b = block as { type?: string; text?: unknown; name?: unknown; input?: unknown; thinking?: unknown; data?: unknown; content?: unknown; width?: unknown; height?: unknown; source?: unknown };
  switch (b.type) {
    case 'text':
      return typeof b.text === 'string' ? estimateTextTokens(b.text) : 0;
    case 'tool_use':
      return estimateTextTokens(typeof b.name === 'string' ? b.name : '') +
        estimateTokensForFileType(JSON.stringify(b.input ?? {}), 'json');
    case 'thinking':
      return typeof b.thinking === 'string' ? estimateTextTokens(b.thinking) : 0;
    case 'redacted_thinking':
      return typeof b.data === 'string' ? estimateTextTokens(b.data) : 0;
    case 'image': {
      // Dimension-aware when real pixel sizes are present; flat estimate otherwise.
      const dims = readImageDimensions(b);
      return dims ? estimateImageTokens(dims.width, dims.height) : IMAGE_DOCUMENT_TOKEN_ESTIMATE;
    }
    case 'document':
      return IMAGE_DOCUMENT_TOKEN_ESTIMATE;
    case 'tool_result':
      return estimateContentTokens(b.content);
    default:
      return estimateTextTokens(JSON.stringify(block));
  }
}

/** Estimate tokens for a message `content` field: a string, an array of blocks, or nullish. */
export function estimateContentTokens(content: unknown): number {
  if (content === null || content === undefined) return 0;
  if (typeof content === 'string') return estimateTextTokens(content);
  if (Array.isArray(content)) {
    let total = 0;
    for (const part of content) total += estimateBlockTokens(part);
    return total;
  }
  return estimateTextTokens(JSON.stringify(content));
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
