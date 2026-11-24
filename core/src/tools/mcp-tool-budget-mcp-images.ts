// Split from mcp-tool.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { ImageContent } from '../types';
import { getMaxMcpOutputTokens } from '../mcp/client';
import { estimateTokens, estimateTokensForFileType } from '../utils/tokens';
import { compressImageBuffer, IMAGE_TARGET_RAW_SIZE } from './file-read';

/** Default MCP tool call timeout: 5 minutes (300 000 ms). */
export const DEFAULT_MCP_TOOL_TIMEOUT_MS = 300_000;

/** Read the MCP tool-call timeout (milliseconds) from the environment, falling
 *  back to the default. Invalid or non-positive values are silently replaced by
 *  the default.
 *
 *  Precedence: AMI's own `SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS` wins (kept as the
 *  canonical knob), then the reference's standard `MCP_TOOL_TIMEOUT` env var is
 *  honored for parity (managedEnvConstants.ts:L161 — both are milliseconds), so
 *  a user who sets the industry-standard variable gets the expected behavior
 *  without having to learn an AMI-specific name. */
export function getMcpToolTimeout(): number {
  const raw = process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS ?? process.env.MCP_TOOL_TIMEOUT;
  if (raw === undefined || raw === '') return DEFAULT_MCP_TOOL_TIMEOUT_MS;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_MCP_TOOL_TIMEOUT_MS;
  return n;
}

/** Max tool-name length accepted by the OpenAI/Anthropic APIs. A qualified MCP
 *  name longer than this poisons every request, so it must be shortened. */
export const MAX_MCP_TOOL_NAME_LEN = 128;

/** Per-image token cost charged against the MCP output budget when deciding how
 *  many image blocks to keep from a tool result. Mirrors the reference's
 *  `IMAGE_TOKEN_ESTIMATE` (utils/mcpValidation.ts:L15 — 1600 tokens per image),
 *  so a result with many images cannot blow the configured output-token budget. */
export const IMAGE_TOKEN_ESTIMATE = 1600;

/** Cheap-heuristic gate fraction of the output-token budget: if even the rough
 *  (generic) token estimate of an MCP result is at or below this fraction of the
 *  budget, the result is assumed NOT to need truncation and the more accurate
 *  (and potentially expensive) second-stage count is skipped entirely. Mirrors
 *  the reference's `MCP_TOKEN_COUNT_THRESHOLD_FACTOR` (utils/mcpValidation.ts:L14 — 0.5). */
export const MCP_TOKEN_COUNT_THRESHOLD_FACTOR = 0.5;

/** Rough, cheap token estimate of an MCP result's text, used as the first stage
 *  of the truncation gate. Mirrors the reference's `getContentSizeEstimate`
 *  (utils/mcpValidation.ts:L59 → `roughTokenCountEstimation`, ~4 chars/token). */
export function getMcpContentSizeEstimate(text: string): number {
  return estimateTokens(text);
}

/** Decide whether an MCP tool result's text must be truncated to fit the
 *  model-visible output-token budget, using the reference's TWO-STAGE design
 *  (utils/mcpValidation.ts:L151-L178 `mcpContentNeedsTruncation`):
 *
 *   1. CHEAP GATE — a rough generic estimate ({@link getMcpContentSizeEstimate}).
 *      If it is at/below `maxTokens × `{@link MCP_TOKEN_COUNT_THRESHOLD_FACTOR}
 *      the result is comfortably within budget, so we return `false` WITHOUT the
 *      second, more expensive measurement (avoids needless work on small results).
 *   2. ACCURATE COUNT — only reached when the cheap gate is exceeded. When an
 *      `accurateCount` seam is supplied (e.g. a density-aware or future API-backed
 *      counter) it is used; otherwise we fall back to a tighter file-type-aware
 *      estimate so a dense structured payload is not UNDER-counted by the generic
 *      ratio. The accurate stage is wrapped so a transient failure degrades to
 *      "assume no truncation" (`false`) rather than over-truncating — parity with
 *      the reference's `catch { return false }` fail-open on a token-count error.
 *
 *  Ties the model-visible cap to the configured `MAX_MCP_OUTPUT_TOKENS` budget
 *  (not merely a raw byte ceiling). Exported so the decision is unit-testable in
 *  isolation from a live MCP server. */
export function mcpContentNeedsTruncation(
  text: string,
  maxTokens: number = getMaxMcpOutputTokens(),
  accurateCount?: (text: string) => number,
): boolean {
  const estimate = getMcpContentSizeEstimate(text);
  if (estimate <= maxTokens * MCP_TOKEN_COUNT_THRESHOLD_FACTOR) return false;
  try {
    const count = accurateCount ? accurateCount(text) : estimateTokensForFileType(text, 'json');
    return count > maxTokens;
  } catch {
    // A token-count failure must not cause over-truncation — assume the result
    // fits (parity: the reference logs and returns false on a count error).
    return false;
  }
}

/** Width of the inline MCP progress bar rendered from a server's
 *  `notifications/progress` percentage. Mirrors the reference's 20-wide ProgressBar
 *  (tools/MCPTool/UI.tsx:L57-90). */
export const MCP_PROGRESS_BAR_WIDTH = 20;

/** Render a single in-flight MCP progress line from a server-forwarded
 *  `notifications/progress` update, for streaming through `context.onProgress`
 *  (→ a `tool_use_progress` EngineEvent the TUI/detached runner both render).
 *
 *  Mirrors the reference's `renderToolUseProgressMessage` (tools/MCPTool/UI.tsx:L57-90):
 *  a determinate bar + rounded `N%` when the server reports a total, otherwise a
 *  `Processing…`/message line when only a count/message is known. The percentage is
 *  clamped to 0–100 so a misbehaving server can never render an over/underful bar. */
export function formatMcpProgressLine(p: { percent?: number; message?: string }): string {
  const msg = typeof p.message === 'string' ? p.message.trim() : '';
  if (typeof p.percent === 'number' && Number.isFinite(p.percent)) {
    const pct = Math.max(0, Math.min(100, Math.round(p.percent)));
    const filled = Math.round((pct / 100) * MCP_PROGRESS_BAR_WIDTH);
    const bar = '█'.repeat(filled) + '░'.repeat(MCP_PROGRESS_BAR_WIDTH - filled);
    return `[${bar}] ${pct}%${msg ? ` ${msg}` : ''}`;
  }
  return `Processing…${msg ? ` ${msg}` : ''}`;
}

/** Budget-aware compression of the raw base64 image blocks an MCP tool result
 *  carries, into first-class {@link ImageContent} blocks for the model turn.
 *
 *  Mirrors the reference's `truncateContentBlocks` image handling
 *  (utils/mcpValidation.ts:L113-L149): each kept image is charged
 *  {@link IMAGE_TOKEN_ESTIMATE} against the output-token budget (so a result with
 *  many images cannot overflow it — the overflowing tail is dropped), and every
 *  retained image is run through {@link compressImageBuffer} to fit the provider's
 *  size/dimension limits ({@link IMAGE_TARGET_RAW_SIZE}). An image that cannot be
 *  compressed (sharp failure on an oversized/over-dimension payload →
 *  `ImageResizeError`) is SKIPPED rather than emitted as an oversized block, so
 *  untrusted MCP image payloads are bounded before they reach the model. */
export async function budgetMcpImages(
  images: Array<{ mimeType: string; data: string }>,
  maxTokens: number = getMaxMcpOutputTokens(),
): Promise<ImageContent[]> {
  const blocks: ImageContent[] = [];
  let remainingTokens = maxTokens;
  for (const img of images) {
    // Budget exhausted — the remaining images would push the output past the
    // token cap, so drop them (parity: truncateContentBlocks stops at the budget).
    if (remainingTokens < IMAGE_TOKEN_ESTIMATE) break;
    const buf = Buffer.from(img.data, 'base64');
    if (buf.length === 0) continue;
    try {
      const compressed = await compressImageBuffer(buf, IMAGE_TARGET_RAW_SIZE, img.mimeType);
      blocks.push({
        type: 'image',
        source: { type: 'base64', media_type: compressed.mediaType, data: compressed.base64 },
      });
      remainingTokens -= IMAGE_TOKEN_ESTIMATE;
    } catch {
      // Compression failed (ImageResizeError on an un-shrinkable oversized image):
      // skip it rather than emit a block the provider would reject.
    }
  }
  return blocks;
}

/** Deterministic, dependency-free FNV-1a hash → base36 (collision-resistant
 *  enough to disambiguate truncated tool names within a session). */
export function shortHash(s: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(36);
}

/** Normalize a single MCP name segment (a server name or a tool name) so it is
 *  safe to embed between the `__` delimiters of a qualified MCP tool name.
 *
 *  Only characters OUTSIDE the API-compatible class `[a-zA-Z0-9_-]` are replaced
 *  with `_` — hyphens are PRESERVED so the normalized segment matches the server
 *  name pattern `^[a-zA-Z0-9_-]{1,64}$` and stays faithful to the user's actual
 *  server/tool name (FUNCTIONAL FIX, parity: AMI previously collapsed hyphens to
 *  underscores, so a `my-server` config surfaced as `mcp__my_server__…`, which did
 *  not match the name the user configured; the reference's `normalizeNameForMCP`
 *  preserves hyphens). Runs of underscores are then collapsed and leading/trailing
 *  underscores stripped — explicitly so a segment can never introduce a spurious
 *  `__` that would collide with the delimiter used to separate `mcp`, the server,
 *  and the tool (which would corrupt prefix-based permission/auto-allow matching
 *  and the inverse parser). Mirrors the reference's `normalizeNameForMCP`
 *  (services/mcp/normalization.ts). */
export function normalizeMcpNameSegment(name: string): string {
  return name
    .replace(/[^a-zA-Z0-9_-]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '');
}

/** Build the qualified MCP tool name, truncating with a hash suffix so it never
 *  exceeds the API tool-name length limit. */
export function qualifiedMcpToolName(serverName: string, toolName: string): string {
  const normalizedServer = normalizeMcpNameSegment(serverName);
  const normalizedTool = normalizeMcpNameSegment(toolName);
  const full = `mcp__${normalizedServer}__${normalizedTool}`;
  if (full.length <= MAX_MCP_TOOL_NAME_LEN) return full;
  const suffix = '_' + shortHash(full);
  return full.slice(0, MAX_MCP_TOOL_NAME_LEN - suffix.length) + suffix;
}
