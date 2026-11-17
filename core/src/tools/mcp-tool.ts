import { ToolDefinition, ToolResult, ToolContext, ImageContent } from '../types';
import { spillToolText, getFormatDescription, isAlreadySpilled, formatSize } from './spill-output';
import { boundMcpResponse, getMaxMcpOutputChars, getMaxMcpOutputTokens } from '../mcp/client';
import type { McpToolAnnotations } from '../mcp/client';
import { stripInvisibleUnicode, recursivelySanitizeUnicode } from './input-sanitizer';
import { persistBinaryContent, getBinaryBlobSavedMessage } from './mcp-output-storage';
import {
  compressImageBuffer,
  IMAGE_TARGET_RAW_SIZE,
  buildFileImagesOutput,
} from './file-read';

/** Default MCP tool call timeout: 5 minutes (300 000 ms). */
export const DEFAULT_MCP_TOOL_TIMEOUT_MS = 300_000;

/** Read the MCP tool-call timeout (milliseconds) from the environment, falling
 *  back to the default. Invalid or non-positive values are silently replaced by
 *  the default.
 *
 *  Precedence: AMI's own `SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS` wins (kept as the
 *  canonical knob), then Claude Code's standard `MCP_TOOL_TIMEOUT` env var is
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
 *  many image blocks to keep from a tool result. Mirrors Claude Code's
 *  `IMAGE_TOKEN_ESTIMATE` (utils/mcpValidation.ts:L15 — 1600 tokens per image),
 *  so a result with many images cannot blow the configured output-token budget. */
export const IMAGE_TOKEN_ESTIMATE = 1600;

/** Width of the inline MCP progress bar rendered from a server's
 *  `notifications/progress` percentage. Mirrors Claude Code's 20-wide ProgressBar
 *  (tools/MCPTool/UI.tsx:L57-90). */
export const MCP_PROGRESS_BAR_WIDTH = 20;

/** Render a single in-flight MCP progress line from a server-forwarded
 *  `notifications/progress` update, for streaming through `context.onProgress`
 *  (→ a `tool_use_progress` EngineEvent the TUI/detached runner both render).
 *
 *  Mirrors Claude Code's `renderToolUseProgressMessage` (tools/MCPTool/UI.tsx:L57-90):
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
 *  Mirrors Claude Code's `truncateContentBlocks` image handling
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
function shortHash(s: string): string {
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
 *  not match the name the user configured; Claude Code's `normalizeNameForMCP`
 *  preserves hyphens). Runs of underscores are then collapsed and leading/trailing
 *  underscores stripped — explicitly so a segment can never introduce a spurious
 *  `__` that would collide with the delimiter used to separate `mcp`, the server,
 *  and the tool (which would corrupt prefix-based permission/auto-allow matching
 *  and the inverse parser). Mirrors Claude Code's `normalizeNameForMCP`
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

/** Centralized, boundary-safe inverse of {@link qualifiedMcpToolName}: parse a
 *  `mcp__<server>__<tool>` name into its `{ serverName, toolName }` parts.
 *
 *  Splits on `__`, requires the leading `mcp` segment and a non-empty server
 *  name (returns `null` for any non-MCP or malformed input), and rejoins every
 *  remaining part with `__` so a tool name that itself contains `__` survives.
 *  `toolName` is `undefined` for a bare `mcp__<server>` server-granularity
 *  prefix. Mirrors Claude Code's `mcpInfoFromString`
 *  (services/mcp/mcpStringUtils.ts). */
export function parseMcpToolName(
  name: string,
): { serverName: string; toolName: string | undefined } | null {
  const parts = name.split('__');
  const [mcpPart, serverName, ...toolNameParts] = parts;
  if (mcpPart !== 'mcp' || !serverName) return null;
  const toolName = toolNameParts.length > 0 ? toolNameParts.join('__') : undefined;
  return { serverName, toolName };
}

/** Extract a human-readable error string from an MCP tool result flagged
 *  `isError: true`: the first text content part, else a legacy `.error` field,
 *  else a generic message. Mirrors Claude Code's isError extraction. */
export function extractMcpErrorText(result: unknown): string {
  if (result && typeof result === 'object' && !Array.isArray(result)) {
    const obj = result as Record<string, unknown>;
    if (Array.isArray(obj.content) && obj.content.length > 0) {
      const first = obj.content[0] as Record<string, unknown> | undefined;
      if (first && typeof first === 'object' && typeof first.text === 'string') return first.text;
    }
    if ('error' in obj && obj.error !== undefined) return String(obj.error);
  }
  return 'MCP tool returned an error';
}

/** Read the two MCP tool-discovery hints a server can attach under the tool's
 *  `_meta` block (Claude Code parity, services/mcp/client.ts:L1779-L1785):
 *
 *   - `anthropic/searchHint` — a free-text discovery hint. Whitespace is collapsed
 *     to single spaces and trimmed; an empty result becomes `undefined` so the
 *     `tool_search` weighting layer only ever sees a meaningful one-line hint.
 *   - `anthropic/alwaysLoad` — when strictly `true`, the tool opts OUT of
 *     schema-size deferral and is kept in the initial advertised set.
 *
 *  Exported so the behavior is unit-testable in isolation from a live MCP server. */
export function readMcpToolMeta(
  meta: Record<string, unknown> | undefined,
): { searchHint: string | undefined; alwaysLoad: boolean } {
  const rawHint = meta?.['anthropic/searchHint'];
  const searchHint =
    typeof rawHint === 'string' ? rawHint.replace(/\s+/g, ' ').trim() || undefined : undefined;
  const alwaysLoad = meta?.['anthropic/alwaysLoad'] === true;
  return { searchHint, alwaysLoad };
}

/** Derive the user-facing display label for an MCP tool.
 *
 *  Prefers the server-supplied `annotations.title` (the human label the server
 *  intends, 2025-06-18 MCP spec) over the raw programmatic tool name, and renders
 *  it in Claude Code's `userFacingName` form — `<server> - <label> (MCP)`
 *  (services/mcp/client.ts:L1972-1976) — so the UI shows e.g.
 *  `github - Create Issue (MCP)` instead of the mangled `create_issue`. A blank/
 *  whitespace-only title is ignored (falls back to the tool name). */
export function getMcpToolDisplayName(
  serverName: string,
  toolName: string,
  annotations?: McpToolAnnotations,
): string {
  const title = annotations?.title?.trim();
  const label = title ? title : toolName;
  return `${serverName} - ${label} (MCP)`;
}

export function createMcpTool(
  serverName: string,
  toolName: string,
  description: string,
  inputSchema: Record<string, unknown>,
  annotations?: McpToolAnnotations,
  meta?: Record<string, unknown>,
): ToolDefinition {
  // Untrusted server-supplied metadata is advertised to the model; strip hidden
  // Unicode from the name/description and every string in the input schema so a
  // server cannot smuggle invisible prompt-injection instructions (H28).
  const safeDescription = stripInvisibleUnicode(description);
  const safeInputSchema = recursivelySanitizeUnicode(inputSchema);
  // MCP annotations declare read-only/concurrency semantics (H29).
  const readOnly = annotations?.readOnlyHint ?? false;
  // `openWorldHint` marks a tool whose effects reach outside the local env / are
  // non-deterministic (external-API calls). Carried onto the ToolDefinition as
  // `isOpenWorld` so renderers/metadata can tag it — distinct from read-only
  // (a read-only MCP tool that queries a live remote is still open-world).
  const openWorld = annotations?.openWorldHint === true;
  // Server-supplied discovery hints (`_meta`): a one-line searchHint so a deferred
  // MCP tool is findable by synonym via tool_search, and alwaysLoad so a server can
  // force-keep a tool in the initial set despite schema-size auto-deferral.
  const { searchHint, alwaysLoad } = readMcpToolMeta(meta);
  return {
    name: qualifiedMcpToolName(serverName, stripInvisibleUnicode(toolName)),
    description: `[MCP:${serverName}] ${safeDescription}`,
    inputSchema: safeInputSchema as any,
    isReadOnly: readOnly,
    isConcurrencySafe: readOnly,
    isMcp: true,
    ...(openWorld ? { isOpenWorld: true } : {}),
    ...(searchHint ? { searchHint: stripInvisibleUnicode(searchHint) } : {}),
    ...(alwaysLoad ? { alwaysLoad: true } : {}),
    async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
      const mcpManager = context._mcpManager;
      if (!mcpManager) return { output: 'Error: MCP not initialized', isError: true };

      if (mcpManager.isDisabled?.(serverName)) {
        return { output: `MCP server '${serverName}' is disabled. Re-enable it from the /mcp panel before connecting.`, isError: true };
      }

      if (mcpManager.isPending?.(serverName)) {
        try {
          await mcpManager.ensureConnected(serverName);
        } catch (err) {
          return { output: `Failed to start MCP server '${serverName}': ${err instanceof Error ? err.message : String(err)}`, isError: true };
        }
      }

      // Track whether an elicitation notification was received during this call
      let gotElicitation = false;
      const elicitListener = (_sn: string, method: string) => {
        if ((method === 'elicitation/create' || method === 'notifications/elicitation') && _sn === serverName) {
          gotElicitation = true;
        }
      };
      mcpManager.on?.('notification', elicitListener);

      // Forward the server's in-flight `notifications/progress` for THIS call as a
      // live progress hint (started → N% bar / message) through context.onProgress,
      // which the tool executor surfaces as a `tool_use_progress` EngineEvent the
      // TUI and detached runner both render — so a long MCP call shows advancing
      // progress instead of a silent spinner (parity: services/mcp/client.ts:1846-1892,
      // tools/MCPTool/UI.tsx:L57-90). The mcp client emits `tool-progress` keyed by
      // the `<server>:<tool>` call name.
      const callName = serverName + ':' + toolName;
      const progressListener = (ev: { name?: string; percent?: number; message?: string }) => {
        if (ev && ev.name === callName) {
          context.onProgress?.(formatMcpProgressLine({ percent: ev.percent, message: ev.message }));
        }
      };
      mcpManager.on?.('tool-progress', progressListener);
      context.onProgress?.(`Running ${toolName}…`);
      const removeProgressListener = () => mcpManager.removeListener?.('tool-progress', progressListener);

      const timeoutMs = getMcpToolTimeout();
      let timer: ReturnType<typeof setTimeout>;
      const timeoutPromise = new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(
          `MCP tool call timed out after ${Math.round(timeoutMs / 1000)} seconds. The MCP server may be unresponsive.`
        )), timeoutMs);
        timer.unref();
      });
      // Resolve as soon as the engine/user aborts so an in-flight MCP request is
      // cancelled (signal forwarded to the transport) instead of leaking until
      // the long tool-call timeout fires (H27).
      const abortSignal = context.abortSignal;
      let onAbort: (() => void) | undefined;
      const abortPromise = new Promise<never>((_, reject) => {
        if (!abortSignal) return;
        if (abortSignal.aborted) { reject(new Error('MCP tool call aborted')); return; }
        onAbort = () => reject(new Error('MCP tool call aborted'));
        abortSignal.addEventListener('abort', onAbort, { once: true });
      });
      try {
        const result = await Promise.race([
          mcpManager.callTool(serverName + ':' + toolName, input, { signal: abortSignal }),
          timeoutPromise,
          abortPromise,
        ]);
        clearTimeout(timer!);
        if (onAbort && abortSignal) abortSignal.removeEventListener('abort', onAbort);
        mcpManager.removeListener?.('notification', elicitListener);
        removeProgressListener();

        // A result flagged isError:true is a tool-level failure — surface it as
        // an error (with the server's own message) rather than a success (H33).
        if (result && typeof result === 'object' && !Array.isArray(result) &&
            (result as Record<string, unknown>).isError === true) {
          return { output: extractMcpErrorText(result), isError: true };
        }

        // Try structured content processing before falling back to JSON. Pass the
        // workspace cwd so binary resource blobs are persisted to the tool-results
        // dir (and the model gets a readable path) rather than reported size-only,
        // and the server name so inlined resource blocks carry their provenance.
        const processed = processMcpContent(result, context.cwd, serverName);
        const raw = processed ? processed.text : (typeof result === 'string' ? result : JSON.stringify(result, null, 2));
        // Prefer the server's explicit per-result hint; otherwise fall back to the
        // token-budget-derived default (MAX_MCP_OUTPUT_TOKENS × 4 chars) so every
        // MCP result is bounded by the token cap instead of the 10 MB byte ceiling.
        const maxResultChars = extractMaxResultSizeChars(result) ?? getMaxMcpOutputChars();
        const bounded = boundMcpResponse(raw, maxResultChars);
        const spillCap = Math.min(maxResultChars, 500_000);
        // Image blocks from the result are now first-class multimodal content:
        // compressed to fit the provider's size/dimension limits and charged against
        // the output-token budget, then emitted via the shared file_images protocol so
        // the engine queues them for the next model turn (parity with Claude Code's
        // truncateContentBlocks + image resource forwarding). This REPLACES the old
        // behavior of stuffing raw, unbounded base64 into unread `metadata.mcpImages`.
        const rawImages = processed?.metadata?.mcpImages as Array<{ mimeType: string; data: string }> | undefined;
        if (rawImages && rawImages.length > 0) {
          const imageBlocks = await budgetMcpImages(rawImages, getMaxMcpOutputTokens());
          if (imageBlocks.length > 0) {
            return { output: buildFileImagesOutput('mcp', bounded.content, imageBlocks) };
          }
          // Every image was dropped (budget exhausted) or un-compressible: fall
          // through to the text-only path (the `[Image: …]` placeholders remain in
          // the bounded text so the result is not silently emptied).
        }
        const baseMeta = bounded.truncated ? { mcpTruncated: true, mcpOriginalBytes: bounded.originalBytes } : undefined;
        // `processed.metadata` only ever carried `mcpImages`, which is now consumed
        // above, so the result metadata is just the truncation marker.
        const mergedMeta = baseMeta;
        // Already a persisted-output banner (e.g. a replayed result) — don't
        // re-spill and re-wrap it.
        if (isAlreadySpilled(bounded.content)) {
          return { output: bounded.content, metadata: mergedMeta };
        }
        // When the result must spill to disk, tell the model the file's format so
        // it can parse/jq it, and emit the sequential-read protocol.
        const formatDescription = processed?.format
          ?? (typeof result === 'string' ? 'Plain text' : getFormatDescription('structuredContent'));
        return { output: spillToolText(context.cwd, 'mcp', bounded.content, spillCap, { formatDescription }), metadata: mergedMeta };
      } catch (err) {
        clearTimeout(timer!);
        if (onAbort && abortSignal) abortSignal.removeEventListener('abort', onAbort);
        mcpManager.removeListener?.('notification', elicitListener);
        removeProgressListener();
        if (gotElicitation && context.detachedMode) {
          return { output: 'MCP tool call timed out because the server requested user input (elicitation), but no user was available to answer in non-interactive mode. Do not retry this tool call.', isError: true };
        }
        return { output: `MCP tool error: ${err instanceof Error ? err.message : String(err)}`, isError: true };
      }
    },
  };
}

const MAX_RESULT_SIZE_CAP = 500_000;

export function extractMaxResultSizeChars(result: unknown): number | undefined {
  if (result && typeof result === 'object' && !Array.isArray(result)) {
    const meta = (result as Record<string, unknown>)._meta as Record<string, unknown> | undefined;
    if (meta) {
      const val = meta['anthropic/maxResultSizeChars'] ?? meta['maxResultSizeChars'];
      if (typeof val === 'number' && val > 0) {
        return Math.min(val, MAX_RESULT_SIZE_CAP);
      }
    }
  }
  return undefined;
}

/** Generate a compact, jq-friendly type signature for a value, e.g.
 *  `{title: string, items: [{id: number, name: string}]}`. Mirrors Claude Code's
 *  `inferCompactSchema` (services/mcp/client.ts:L2644-2660): descends at most
 *  `depth` levels (deeper objects collapse to `{...}`), samples the first element
 *  of an array, and caps an object at its first 10 keys (appending `, ...` when
 *  more exist) so the signature stays short enough to prepend to a large-output
 *  spill banner. Lets the model see the shape of a persisted structured result
 *  and write a precise `jq` query without having to read the whole file first. */
export function inferCompactSchema(value: unknown, depth = 2): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) {
    if (value.length === 0) return '[]';
    return `[${inferCompactSchema(value[0], depth - 1)}]`;
  }
  if (typeof value === 'object') {
    if (depth <= 0) return '{...}';
    const keys = Object.keys(value as Record<string, unknown>);
    const entries = Object.entries(value as Record<string, unknown>).slice(0, 10);
    const props = entries.map(([k, v]) => `${k}: ${inferCompactSchema(v, depth - 1)}`);
    const suffix = keys.length > 10 ? ', ...' : '';
    return `{${props.join(', ')}${suffix}}`;
  }
  return typeof value;
}

/** Supported image MIME types for inline representation. */
const SUPPORTED_IMAGE_TYPES = new Set(['image/png', 'image/jpeg', 'image/gif', 'image/webp']);
/** Text-like MIME types that can be decoded and shown inline. */
const TEXT_RESOURCE_PATTERN = /^(text\/|application\/json|application\/xml)/;

/** Process an MCP tool result, handling the prioritized result shapes and the
 *  text / image / audio / resource / resource_link content-item types.
 *  Returns null if the result carries none of these (fallback to JSON.stringify).
 *
 *  Result shapes are checked in Claude-compatible priority order: a legacy
 *  `toolResult` string, then a `structuredContent` object, then the `content`
 *  array. The first two are otherwise lost to a raw stringify of the whole
 *  wrapper object (empty content array / _meta and all).
 *
 *  `serverName` (when given) labels every inlined resource block with its
 *  provenance — `[Resource from <server> at <uri>] …` — mirroring Claude Code's
 *  `transformResultContent` (services/mcp/client.ts:L2526-2534) so the model can
 *  tell which MCP server/resource an inlined block came from. */
export function processMcpContent(result: unknown, cwd?: string, serverName?: string): { text: string; metadata?: Record<string, unknown>; format?: string } | null {
  if (!result || typeof result !== 'object' || Array.isArray(result)) return null;
  const obj = result as Record<string, unknown>;

  // Priority 1: legacy `toolResult` plain-string shape.
  if (typeof obj.toolResult === 'string') {
    return { text: obj.toolResult, format: getFormatDescription('toolResult') };
  }
  // Priority 2: `structuredContent` object — emit the structured payload as JSON
  // so it survives instead of being dropped to a stringify of the whole wrapper.
  // The format carries a compact jq-friendly schema signature so a spilled result
  // tells the model the shape to query (Claude parity: transformMCPResult attaches
  // inferCompactSchema() to structuredContent).
  if (obj.structuredContent && typeof obj.structuredContent === 'object') {
    const schema = inferCompactSchema(obj.structuredContent);
    return { text: JSON.stringify(obj.structuredContent, null, 2), format: getFormatDescription('structuredContent', schema) };
  }

  if (!Array.isArray(obj.content)) return null;

  const parts: string[] = [];
  const images: Array<{ mimeType: string; data: string }> = [];

  for (const item of obj.content as Array<Record<string, unknown>>) {
    if (item.type === 'text' && typeof item.text === 'string') {
      parts.push(item.text);
    } else if (item.type === 'image' && typeof item.data === 'string') {
      const mime = (item.mimeType as string) || 'image/png';
      if (SUPPORTED_IMAGE_TYPES.has(mime)) {
        const sizeBytes = Math.ceil(((item.data as string).length * 3) / 4);
        parts.push(`[Image: ${mime}, ${formatSize(sizeBytes)} base64]`);
        images.push({ mimeType: mime, data: item.data as string });
      } else {
        parts.push(`[Unsupported image type: ${mime}]`);
      }
    } else if (item.type === 'audio' && typeof item.data === 'string') {
      // Audio content is not inlined; surface a size/mime placeholder (mirroring
      // the image branch) so the block is not silently dropped from the output.
      const mime = (item.mimeType as string) || 'audio/mpeg';
      const sizeBytes = Math.ceil(((item.data as string).length * 3) / 4);
      parts.push(`[Audio: ${mime}, ${formatSize(sizeBytes)} base64]`);
    } else if (item.type === 'resource_link') {
      // A link to a resource the server can serve — surface name, uri and any
      // description so the model knows the referenced content exists.
      const uri = typeof item.uri === 'string' ? item.uri : 'unknown';
      const name = typeof item.name === 'string' ? item.name : uri;
      const desc = typeof item.description === 'string' ? ` (${item.description})` : '';
      parts.push(`[Resource link: ${name}] ${uri}${desc}`);
    } else if (item.type === 'resource') {
      const resource = item.resource as Record<string, unknown> | undefined;
      if (resource) {
        const mime = (resource.mimeType as string) || '';
        // Provenance prefix: every inlined MCP resource is labeled with the
        // originating server and resource URI so the model knows the content's
        // source (Claude parity, services/mcp/client.ts:L2526-2534 — the same
        // `[Resource from <server> at <uri>] ` prefix is applied to text, decoded
        // text-ish, and persisted-binary resource blocks). `serverName` is the
        // real server for live tool calls; pure-function callers that omit it get
        // the neutral `MCP` label.
        const resourceUri = (resource.uri as string) ?? 'unknown';
        const provenance = `[Resource from ${serverName ?? 'MCP'} at ${resourceUri}] `;
        if (typeof resource.text === 'string') {
          // Text resource — include inline, labeled with its provenance.
          parts.push(`${provenance}${resource.text}`);
        } else if (TEXT_RESOURCE_PATTERN.test(mime) && typeof resource.blob === 'string') {
          // Text-ish MIME with base64 blob — decode, labeled with its provenance.
          try {
            const decoded = Buffer.from(resource.blob as string, 'base64').toString('utf-8');
            parts.push(`${provenance}${decoded}`);
          } catch {
            parts.push(`[Binary resource: ${resource.uri ?? 'unknown'} (${mime})]`);
          }
        } else if (typeof resource.blob === 'string') {
          // Binary resource — persist the raw bytes to the tool-results dir with a
          // mime-correct extension (so the Read tool can later open the PDF/xlsx/
          // image) and surface an actionable "saved to <path>" message instead of
          // dropping the base64 into context. Mirrors Claude's persistBlobToTextBlock.
          // Without a cwd (pure-function callers), fall back to a size placeholder.
          let bytes: Buffer | null = null;
          try { bytes = Buffer.from(resource.blob as string, 'base64'); } catch { bytes = null; }
          const persisted = cwd && bytes ? persistBinaryContent(cwd, bytes, mime || undefined) : undefined;
          if (persisted && !('error' in persisted)) {
            parts.push(getBinaryBlobSavedMessage(persisted.filepath, mime || undefined, persisted.size, provenance));
          } else {
            const sizeBytes = bytes ? bytes.length : Math.ceil(((resource.blob as string).length * 3) / 4);
            parts.push(`[Binary resource: ${resource.uri ?? 'unknown'} (${mime || 'unknown type'}, ${formatSize(sizeBytes)})]`);
          }
        } else {
          parts.push(`[Resource: ${resource.uri ?? 'unknown'}]`);
        }
      }
    }
  }

  if (parts.length === 0) return null;
  const metadata = images.length > 0 ? { mcpImages: images } : undefined;
  // The content array is flattened to newline-joined text, so the persisted
  // file is plain text (not a raw JSON array).
  return { text: parts.join('\n'), metadata, format: getFormatDescription('toolResult') };
}
