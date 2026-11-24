// Split from provider-transform.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { Message, ToolDefinition } from './types';
import { isEnvTruthy } from './analytics';
import { API_IMAGE_MAX_BASE64_SIZE } from './tools/file-read';
import { formatFileSize } from './utils/format';
import { plural } from './utils/string';

/** Anthropic's hard per-request media ceiling (images + documents). The API rejects a
 *  request carrying more than this with a confusing error, so we proactively strip the
 *  OLDEST excess media to stay under it. Parity: the reference's `API_MAX_MEDIA_PER_REQUEST`. */
export const MAX_MEDIA_PER_REQUEST = 100;

/** Default placeholder content for a synthetic tool_result emitted when a tool_call
 *  never received a real result. Parity: the reference's SYNTHETIC_TOOL_RESULT_PLACEHOLDER. */
export const SYNTHETIC_TOOL_RESULT_PLACEHOLDER = '[Tool call interrupted — no result available]';

/**
 * Whether Anthropic prompt caching (cache_control breakpoints) should be emitted for
 * `model`. Mirrors the reference's `getPromptCachingEnabled`: a global `DISABLE_PROMPT_CACHING`
 * kill-switch plus per-family `DISABLE_PROMPT_CACHING_HAIKU/SONNET/OPUS` knobs. The
 * family match is by model-id substring (AMI has no fixed small/sonnet/opus alias set).
 */
export function getPromptCachingEnabled(model: string): boolean {
  // Global disable takes precedence.
  if (isEnvTruthy(process.env.DISABLE_PROMPT_CACHING)) return false;
  const m = model.toLowerCase();
  if (isEnvTruthy(process.env.DISABLE_PROMPT_CACHING_HAIKU) && m.includes('haiku')) return false;
  if (isEnvTruthy(process.env.DISABLE_PROMPT_CACHING_SONNET) && m.includes('sonnet')) return false;
  if (isEnvTruthy(process.env.DISABLE_PROMPT_CACHING_OPUS) && m.includes('opus')) return false;
  return true;
}

/**
 * Proactively cap the number of media items (base64 images) carried in the outbound
 * message array at `limit`, dropping the OLDEST excess first so the most recent media
 * survives. Anthropic rejects a request with more than `MAX_MEDIA_PER_REQUEST` media
 * blocks with a confusing error that is hard to recover from mid-session; stripping
 * pre-emptively keeps the request valid. Parity: the reference's `stripExcessMediaItems`
 * applied unconditionally in the request-building path (not only on error recovery).
 * Returns the same array reference when already within the limit.
 */
export function stripExcessMediaItems(
  messages: Message[],
  limit = MAX_MEDIA_PER_REQUEST,
): Message[] {
  let total = 0;
  for (const m of messages) {
    if (m.role === 'user' && Array.isArray(m.content)) {
      for (const block of m.content) if (block.type === 'image') total++;
    }
  }
  let toRemove = total - limit;
  if (toRemove <= 0) return messages;

  return messages.map((m) => {
    if (toRemove <= 0) return m;
    if (m.role !== 'user' || !Array.isArray(m.content)) return m;
    const before = toRemove;
    const filtered = m.content.filter((block) => {
      if (toRemove > 0 && block.type === 'image') {
        toRemove--;
        return false;
      }
      return true;
    });
    return before === toRemove ? m : { ...m, content: filtered };
  });
}

/**
 * A single oversized image located by its position in the outbound message array.
 * `messageIndex` is the 0-based index into the `Message[]` passed to the API;
 * `imageIndex` is the 0-based index of the image AMONG THE IMAGES in that message.
 */
export interface OversizedImage {
  messageIndex: number;
  imageIndex: number;
  /** Byte length of the base64-encoded image data (what the API actually counts). */
  base64Size: number;
}

/**
 * Thrown by {@link validateImagesForAPI} when one or more outbound images exceed
 * the API's per-image base64 ceiling. Carries the structured list so a caller/UI
 * can act on it, while `message` is a human-readable, actionable summary naming
 * exactly which images are too big. Parity: the reference's `ImageSizeError`.
 */
export class ImageSizeError extends Error {
  readonly oversized: readonly OversizedImage[];
  readonly limit: number;
  constructor(oversized: readonly OversizedImage[], limit: number) {
    const list = oversized
      .map(o => `message ${o.messageIndex} image ${o.imageIndex} (${formatFileSize(o.base64Size)} base64)`)
      .join(', ');
    super(
      `${oversized.length} ${plural(oversized.length, 'image')} exceed the API limit of ` +
      `${formatFileSize(limit)} base64 each: ${list}. Resize or crop ` +
      `${oversized.length === 1 ? 'it' : 'them'}, or inspect with bash instead of attaching.`,
    );
    this.name = 'ImageSizeError';
    this.oversized = oversized;
    this.limit = limit;
  }
}

/**
 * Proactively validate that no outbound image exceeds the API's per-image base64
 * ceiling ({@link API_IMAGE_MAX_BASE64_SIZE}) BEFORE the request is sent. Without
 * this, an oversized image surfaces as a confusing mid-request 400 that names no
 * image; here the turn fails fast with an actionable {@link ImageSizeError} that
 * identifies each offending image by message/image index. Parity: the reference's
 * `validateImagesForAPI`. A no-op (returns normally) when every image is within
 * the limit, so it is safe to call unconditionally in the request path.
 */
export function validateImagesForAPI(
  messages: Message[],
  limit = API_IMAGE_MAX_BASE64_SIZE,
): void {
  const oversized: OversizedImage[] = [];
  messages.forEach((m, messageIndex) => {
    if (!Array.isArray(m.content)) return;
    let imageIndex = 0;
    for (const block of m.content) {
      if (block.type !== 'image') continue;
      const data = block.source?.data;
      if (typeof data === 'string' && data.length > limit) {
        oversized.push({ messageIndex, imageIndex, base64Size: data.length });
      }
      imageIndex++;
    }
  });
  if (oversized.length > 0) throw new ImageSizeError(oversized, limit);
}

export function sanitizeToolCallIds(
  messages: Message[],
  provider: string,
): Message[] {
  if (provider !== 'anthropic') return messages;
  // Anthropic requires alphanumeric-only tool call IDs
  return messages.map((msg, idx) => {
    if (msg.role === 'assistant' && msg.tool_calls) {
      return {
        ...msg,
        tool_calls: msg.tool_calls.map((tc, i) => ({
          ...tc,
          id: tc.id.replace(/[^a-zA-Z0-9_-]/g, '') || `tc_${String(i).padStart(4, '0')}`,
        })),
      };
    }
    if (msg.role === 'tool') {
      const sanitized = msg.tool_call_id.replace(/[^a-zA-Z0-9_-]/g, '');
      if (sanitized) return { ...msg, tool_call_id: sanitized };
      // Count preceding consecutive tool messages to determine this tool's
      // position among its siblings — mirrors the index used for the
      // assistant-side tc_NNNN fallback so the IDs match.
      let toolIdx = 0;
      for (let j = idx - 1; j >= 0 && messages[j].role === 'tool'; j--) {
        toolIdx++;
      }
      return { ...msg, tool_call_id: `tc_${String(toolIdx).padStart(4, '0')}` };
    }
    return msg;
  });
}

/**
 * The Anthropic prompt-cache breakpoint object emitted for a cached prefix block.
 * Mirrors the shape the reference's `getCacheControl()` returns
 * (`{ type: 'ephemeral'; ttl?: '1h'; scope?: CacheScope }`):
 *  - `ttl: '1h'` opts a breakpoint into the extended-cache window instead of the
 *    implicit 5-minute default — longer-lived caching for long/idle sessions that
 *    reduces re-cache cost (requires Anthropic's extended-cache beta);
 *  - `scope` opts into a cross-session/key cache (`'global'` / `'org'`).
 * Omitting `ttl` keeps the standard 5-minute ephemeral cache, so the default
 * breakpoint is `{ type: 'ephemeral' }` and behavior is unchanged when no TTL is
 * requested.
 */
export interface CacheBreakpoint {
  type: string;
  ttl?: '1h';
  scope?: string;
}

/** The standard 5-minute ephemeral breakpoint — the default when no TTL/scope is
 *  requested, matching Anthropic's implicit cache window. */
export const DEFAULT_CACHE_BREAKPOINT: CacheBreakpoint = { type: 'ephemeral' };

export function buildConversationCachePoints(
  messages: Array<{ role: string }>,
  enablePromptCaching = true,
  breakpoint: CacheBreakpoint = DEFAULT_CACHE_BREAKPOINT,
): Record<number, CacheBreakpoint> {
  const points: Record<number, CacheBreakpoint> = {};
  if (!enablePromptCaching || messages.length === 0) return points;

  let lastNonTool = -1;
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].role !== 'tool') {
      lastNonTool = i;
      break;
    }
  }

  let secondLastNonTool = -1;
  for (let i = lastNonTool - 1; i >= 0; i--) {
    if (messages[i].role !== 'tool') {
      secondLastNonTool = i;
      break;
    }
  }

  if (secondLastNonTool >= 0) points[secondLastNonTool] = breakpoint;
  if (lastNonTool >= 0) points[lastNonTool] = breakpoint;

  return points;
}

export function buildToolCacheBreakpoints(
  tools: ToolDefinition[],
  enablePromptCaching = true,
  breakpoint: CacheBreakpoint = DEFAULT_CACHE_BREAKPOINT,
): Record<number, CacheBreakpoint> {
  if (!enablePromptCaching || tools.length === 0) return {};
  return { [tools.length - 1]: breakpoint };
}

/**
 * Strip empty/whitespace-only text content blocks from Anthropic messages and return
 * the resulting message list. Anthropic rejects any text block with empty text. If
 * stripping leaves an assistant with no content, it is replaced with '(continued)';
 * if it leaves a NON-assistant message (user/tool) with an empty content array, the
 * message is DROPPED entirely — the API rejects a message whose content is empty, so a
 * malformed/empty user or tool turn (e.g. a compaction artifact) must not be sent.
 * Parity: the reference drops messages left with empty content after block stripping.
 */
export function stripEmptyTextBlocks(
  messages: Array<Record<string, unknown>>,
): Array<Record<string, unknown>> {
  const out: Array<Record<string, unknown>> = [];
  for (const m of messages) {
    if (Array.isArray(m.content)) {
      m.content = (m.content as Array<Record<string, unknown>>).filter(
        (p) => !(p.type === 'text' && (typeof p.text !== 'string' || (p.text as string).trim() === ''))
      );
      if ((m.content as unknown[]).length === 0) {
        if (m.role === 'assistant') {
          m.content = [{ type: 'text' as const, text: '(continued)' }];
        } else {
          // Non-assistant message emptied by stripping — drop it; the API rejects
          // a message with empty content.
          continue;
        }
      }
    } else if (typeof m.content === 'string' && m.content.trim() === '' && m.role === 'assistant') {
      m.content = '(continued)';
    }
    out.push(m);
  }
  return out;
}
