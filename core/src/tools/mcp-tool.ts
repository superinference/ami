import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { spillToolText, getFormatDescription, isAlreadySpilled } from './spill-output';
import { boundMcpResponse, getMaxMcpOutputChars } from '../mcp/client';
import type { McpToolAnnotations } from '../mcp/client';
import { stripInvisibleUnicode, recursivelySanitizeUnicode } from './input-sanitizer';
import { persistBinaryContent, getBinaryBlobSavedMessage } from './mcp-output-storage';

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

export function createMcpTool(
  serverName: string,
  toolName: string,
  description: string,
  inputSchema: Record<string, unknown>,
  annotations?: McpToolAnnotations,
): ToolDefinition {
  // Untrusted server-supplied metadata is advertised to the model; strip hidden
  // Unicode from the name/description and every string in the input schema so a
  // server cannot smuggle invisible prompt-injection instructions (H28).
  const safeDescription = stripInvisibleUnicode(description);
  const safeInputSchema = recursivelySanitizeUnicode(inputSchema);
  // MCP annotations declare read-only/concurrency semantics (H29).
  const readOnly = annotations?.readOnlyHint ?? false;
  return {
    name: qualifiedMcpToolName(serverName, stripInvisibleUnicode(toolName)),
    description: `[MCP:${serverName}] ${safeDescription}`,
    inputSchema: safeInputSchema as any,
    isReadOnly: readOnly,
    isConcurrencySafe: readOnly,
    isMcp: true,
    async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
      const mcpManager = context._mcpManager;
      if (!mcpManager) return { output: 'Error: MCP not initialized', isError: true };

      if (mcpManager.isDisabled?.(serverName)) {
        return { output: `MCP server '${serverName}' is disabled. Use /mcp start ${serverName} to re-enable it.`, isError: true };
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

        // A result flagged isError:true is a tool-level failure — surface it as
        // an error (with the server's own message) rather than a success (H33).
        if (result && typeof result === 'object' && !Array.isArray(result) &&
            (result as Record<string, unknown>).isError === true) {
          return { output: extractMcpErrorText(result), isError: true };
        }

        // Try structured content processing before falling back to JSON. Pass the
        // workspace cwd so binary resource blobs are persisted to the tool-results
        // dir (and the model gets a readable path) rather than reported size-only.
        const processed = processMcpContent(result, context.cwd);
        const raw = processed ? processed.text : (typeof result === 'string' ? result : JSON.stringify(result, null, 2));
        // Prefer the server's explicit per-result hint; otherwise fall back to the
        // token-budget-derived default (MAX_MCP_OUTPUT_TOKENS × 4 chars) so every
        // MCP result is bounded by the token cap instead of the 10 MB byte ceiling.
        const maxResultChars = extractMaxResultSizeChars(result) ?? getMaxMcpOutputChars();
        const bounded = boundMcpResponse(raw, maxResultChars);
        const spillCap = Math.min(maxResultChars, 500_000);
        const baseMeta = bounded.truncated ? { mcpTruncated: true, mcpOriginalBytes: bounded.originalBytes } : undefined;
        const mergedMeta = processed?.metadata ? { ...baseMeta, ...processed.metadata } : baseMeta;
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
 *  wrapper object (empty content array / _meta and all). */
export function processMcpContent(result: unknown, cwd?: string): { text: string; metadata?: Record<string, unknown>; format?: string } | null {
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
        parts.push(`[Image: ${mime}, ${sizeBytes} bytes base64]`);
        images.push({ mimeType: mime, data: item.data as string });
      } else {
        parts.push(`[Unsupported image type: ${mime}]`);
      }
    } else if (item.type === 'audio' && typeof item.data === 'string') {
      // Audio content is not inlined; surface a size/mime placeholder (mirroring
      // the image branch) so the block is not silently dropped from the output.
      const mime = (item.mimeType as string) || 'audio/mpeg';
      const sizeBytes = Math.ceil(((item.data as string).length * 3) / 4);
      parts.push(`[Audio: ${mime}, ${sizeBytes} bytes base64]`);
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
        if (typeof resource.text === 'string') {
          // Text resource — include inline
          parts.push(resource.text);
        } else if (TEXT_RESOURCE_PATTERN.test(mime) && typeof resource.blob === 'string') {
          // Text-ish MIME with base64 blob — decode
          try {
            const decoded = Buffer.from(resource.blob as string, 'base64').toString('utf-8');
            parts.push(decoded);
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
            const source = `[Resource from MCP at ${resource.uri ?? 'unknown'}] `;
            parts.push(getBinaryBlobSavedMessage(persisted.filepath, mime || undefined, persisted.size, source));
          } else {
            const sizeBytes = bytes ? bytes.length : Math.ceil(((resource.blob as string).length * 3) / 4);
            parts.push(`[Binary resource: ${resource.uri ?? 'unknown'} (${mime || 'unknown type'}, ${sizeBytes} bytes)]`);
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
