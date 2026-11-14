import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { spillToolText } from './spill-output';
import { boundMcpResponse } from '../mcp/client';
import type { McpToolAnnotations } from '../mcp/client';
import { stripInvisibleUnicode, recursivelySanitizeUnicode } from './input-sanitizer';

/** Default MCP tool call timeout: 5 minutes (300 000 ms). */
export const DEFAULT_MCP_TOOL_TIMEOUT_MS = 300_000;

/** Read the MCP tool timeout from the environment, falling back to the default.
 *  Invalid or non-positive values are silently replaced by the default. */
export function getMcpToolTimeout(): number {
  const raw = process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS;
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

/** Build the qualified MCP tool name, truncating with a hash suffix so it never
 *  exceeds the API tool-name length limit. */
export function qualifiedMcpToolName(serverName: string, toolName: string): string {
  const normalizedServer = serverName.replace(/[^a-zA-Z0-9_]/g, '_');
  const normalizedTool = toolName.replace(/[^a-zA-Z0-9_]/g, '_');
  const full = `mcp__${normalizedServer}__${normalizedTool}`;
  if (full.length <= MAX_MCP_TOOL_NAME_LEN) return full;
  const suffix = '_' + shortHash(full);
  return full.slice(0, MAX_MCP_TOOL_NAME_LEN - suffix.length) + suffix;
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

        // Try structured content processing before falling back to JSON
        const processed = processMcpContent(result);
        const raw = processed ? processed.text : (typeof result === 'string' ? result : JSON.stringify(result, null, 2));
        const maxResultChars = extractMaxResultSizeChars(result);
        const bounded = boundMcpResponse(raw, maxResultChars);
        const spillCap = maxResultChars ? Math.min(maxResultChars, 500_000) : 100_000;
        const baseMeta = bounded.truncated ? { mcpTruncated: true, mcpOriginalBytes: bounded.originalBytes } : undefined;
        const mergedMeta = processed?.metadata ? { ...baseMeta, ...processed.metadata } : baseMeta;
        return { output: spillToolText(context.cwd, 'mcp', bounded.content, spillCap), metadata: mergedMeta };
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

/** Supported image MIME types for inline representation. */
const SUPPORTED_IMAGE_TYPES = new Set(['image/png', 'image/jpeg', 'image/gif', 'image/webp']);
/** Text-like MIME types that can be decoded and shown inline. */
const TEXT_RESOURCE_PATTERN = /^(text\/|application\/json|application\/xml)/;

/** Process MCP result content array, handling text, image, and resource types.
 *  Returns null if the result does not have a content array (fallback to JSON.stringify). */
export function processMcpContent(result: unknown): { text: string; metadata?: Record<string, unknown> } | null {
  if (!result || typeof result !== 'object' || Array.isArray(result)) return null;
  const obj = result as Record<string, unknown>;
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
          // Binary resource — report size
          const sizeBytes = Math.ceil(((resource.blob as string).length * 3) / 4);
          parts.push(`[Binary resource: ${resource.uri ?? 'unknown'} (${mime || 'unknown type'}, ${sizeBytes} bytes)]`);
        } else {
          parts.push(`[Resource: ${resource.uri ?? 'unknown'}]`);
        }
      }
    }
  }

  if (parts.length === 0) return null;
  const metadata = images.length > 0 ? { mcpImages: images } : undefined;
  return { text: parts.join('\n'), metadata };
}
