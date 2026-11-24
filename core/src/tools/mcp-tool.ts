import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { spillToolText, getFormatDescription, isAlreadySpilled, formatSize } from './spill-output';
import { boundMcpResponse, getMaxMcpOutputChars, getMaxMcpOutputTokens, MCP_CHARS_PER_TOKEN } from '../mcp/client';
import type { McpToolAnnotations } from '../mcp/client';
import { estimateTokensForFileType } from '../utils/tokens';
import { logError } from '../logger';
import { stripInvisibleUnicode, recursivelySanitizeUnicode } from './input-sanitizer';
import { persistBinaryContent, getBinaryBlobSavedMessage } from './mcp-output-storage';
import { buildFileImagesOutput } from './file-read';
import { budgetMcpImages, formatMcpProgressLine, getMcpToolTimeout, mcpContentNeedsTruncation, qualifiedMcpToolName } from './mcp-tool-budget-mcp-images';
export { DEFAULT_MCP_TOOL_TIMEOUT_MS, IMAGE_TOKEN_ESTIMATE, MAX_MCP_TOOL_NAME_LEN, MCP_PROGRESS_BAR_WIDTH, MCP_TOKEN_COUNT_THRESHOLD_FACTOR, budgetMcpImages, formatMcpProgressLine, getMcpContentSizeEstimate, getMcpToolTimeout, mcpContentNeedsTruncation, normalizeMcpNameSegment, qualifiedMcpToolName } from './mcp-tool-budget-mcp-images';

/** Centralized, boundary-safe inverse of {@link qualifiedMcpToolName}: parse a
 *  `mcp__<server>__<tool>` name into its `{ serverName, toolName }` parts.
 *
 *  Splits on `__`, requires the leading `mcp` segment and a non-empty server
 *  name (returns `null` for any non-MCP or malformed input), and rejoins every
 *  remaining part with `__` so a tool name that itself contains `__` survives.
 *  `toolName` is `undefined` for a bare `mcp__<server>` server-granularity
 *  prefix. Mirrors the reference's `mcpInfoFromString`
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
 *  else a generic message. Mirrors the reference's isError extraction. */
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
 *  `_meta` block (the reference parity, services/mcp/client.ts:L1779-L1785):
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
 *  it in the reference's `userFacingName` form — `<server> - <label> (MCP)`
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
      if (!mcpManager) {
        // Diagnostic surface for a called-before-init ordering bug: an MCP tool is
        // only ever advertised once its manager exists, so reaching here means the
        // manager was dropped from the tool context — record it on the in-memory
        // error ring (retrievable via getInMemoryErrors for diagnostics) instead of
        // failing silently. Unlike the reference's teammate-mode snapshot accessor,
        // no lazy recovery/safe-default is viable here — without a manager the tool
        // genuinely cannot run — so we still return the hard error after logging.
        logError(
          'mcp-tool',
          `MCP tool '${serverName}:${toolName}' invoked before its MCP manager was initialized (context._mcpManager missing)`,
          { serverName, toolName },
        );
        return { output: 'Error: MCP not initialized', isError: true };
      }

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
        // Two-stage, token-aware truncation gate (parity: mcpContentNeedsTruncation,
        // utils/mcpValidation.ts:L151-L178). A result comfortably within the
        // output-token budget skips the byte-walk entirely; only one over the
        // cheap gate is bounded to the token-budget-derived char cap. The accurate
        // second stage is density-aware — structured (non-string) MCP results are
        // counted at the JSON ratio (~2 chars/token) so a dense payload is not
        // under-counted, while plain-text results use the generic ratio.
        const maxTokens = Math.max(1, Math.floor(maxResultChars / MCP_CHARS_PER_TOKEN));
        const isStructured = typeof result !== 'string';
        const accurateCount = (t: string) => estimateTokensForFileType(t, isStructured ? 'json' : 'txt');
        const bounded = mcpContentNeedsTruncation(raw, maxTokens, accurateCount)
          ? boundMcpResponse(raw, maxResultChars)
          : { content: raw, truncated: false, originalBytes: Buffer.byteLength(raw, 'utf-8') };
        const spillCap = Math.min(maxResultChars, 500_000);
        // Image blocks from the result are now first-class multimodal content:
        // compressed to fit the provider's size/dimension limits and charged against
        // the output-token budget, then emitted via the shared file_images protocol so
        // the engine queues them for the next model turn (parity with the reference's
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
 *  `{title: string, items: [{id: number, name: string}]}`. Mirrors the reference's
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
 *  provenance — `[Resource from <server> at <uri>] …` — mirroring the reference's
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
  // tells the model the shape to query (the reference parity: transformMCPResult attaches
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
        // source (the reference parity, services/mcp/client.ts:L2526-2534 — the same
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
          // dropping the base64 into context. Mirrors the reference's persistBlobToTextBlock.
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
