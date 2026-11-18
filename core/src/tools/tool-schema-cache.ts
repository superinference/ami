// Session-scoped cache of rendered PROVIDER tool-schema entries.
//
// The tool-schema array renders at server position 2 (before the system prompt),
// so any byte-level change to it busts the entire multi-K-token tool block AND
// everything downstream in the provider's prompt cache. The churn sources Claude
// Code guards against (dynamic tool.prompt() content, GrowthBook gate flips) plus
// the one that bites AMI in practice — an MCP server re-advertising the SAME tool
// as a FRESH ToolDefinition object on reconnect / tools-changed, whose inputSchema
// comes verbatim from the server and can differ only in key ORDER — all shift the
// serialized tool bytes and silently invalidate the cache.
//
// AMI already converted tool definitions once per turn via an identity-keyed
// WeakMap, but object identity does NOT survive an MCP reconnect: a re-created tool
// object missed the cache and re-rendered, and a key-reordered-but-equal schema
// produced byte-different output. This cache keys by tool NAME + a key-order-
// insensitive serialization of the inputSchema ({@link stableStringify}) instead,
// so a structurally-identical tool reuses the bytes FROZEN at first render — the
// provider cache prefix stays stable across reconnects and refreshes.
//
// It lives in a leaf module (imports only types + the shared stable serializer) so
// a consumer that must drop the cache on a lifecycle event — e.g. the MCP manager
// invalidating a server's tools — can call {@link clearToolSchemaCache} without
// pulling in the provider/engine layers (mirrors Claude Code's rationale for
// keeping toolSchemaCache a leaf so auth.ts can clear it without an import cycle).

import type { ToolDefinition, ToolInputSchema } from '../types';
import { stableStringify } from '../utils/stable-hash';

const TOOL_SCHEMA_CACHE = new Map<string, unknown>();

/**
 * Cache key for a tool's rendered schema: the tool name, plus a key-order-
 * insensitive serialization of its inputSchema when present. Two tools that share
 * a name but carry structurally different schemas (distinct keys) never collide,
 * while the same tool re-advertised with a reordered-but-equal schema maps to the
 * SAME key — so its first-rendered bytes are reused. Mirrors Claude Code's
 * `${name}:${jsonStringify(inputJSONSchema)}` keying, hardened for key order.
 */
export function toolSchemaCacheKey(name: string, inputSchema?: ToolInputSchema): string {
  return inputSchema ? `${name}:${stableStringify(inputSchema)}` : name;
}

/**
 * The raw session cache. Exposed for introspection/parity with Claude Code's
 * `getToolSchemaCache()`; consumers should prefer {@link memoizeToolSchema}.
 */
export function getToolSchemaCache(): Map<string, unknown> {
  return TOOL_SCHEMA_CACHE;
}

/**
 * Drop every memoized tool schema. Called on a lifecycle event that may change a
 * tool's rendered bytes while keeping its name (e.g. an MCP server re-advertising
 * tools on reconnect / tools-changed). The cache re-populates lazily on the next
 * render, so clearing is cheap and never serves a stale description/schema.
 */
export function clearToolSchemaCache(): void {
  TOOL_SCHEMA_CACHE.clear();
}

/**
 * Return the schema rendered once for `tool` (by name + stable schema), computing
 * and freezing it via `render` on the first miss. Subsequent calls for a tool with
 * the same name + structurally-equal schema — even a brand-new ToolDefinition
 * object (MCP reconnect) or a key-reordered schema — reuse the frozen value, so the
 * provider tool block stays byte-stable across the session.
 */
export function memoizeToolSchema<T>(
  tool: Pick<ToolDefinition, 'name' | 'inputSchema'>,
  render: () => T,
): T {
  const key = toolSchemaCacheKey(tool.name, tool.inputSchema);
  const existing = TOOL_SCHEMA_CACHE.get(key);
  if (existing !== undefined) return existing as T;
  const rendered = render();
  TOOL_SCHEMA_CACHE.set(key, rendered);
  return rendered;
}

/**
 * Introspection: how many distinct tool-schema variants are currently frozen
 * (one per unique name + schema). Surfaced for prompt-cache attribution and tests.
 */
export function toolSchemaCacheStats(): { size: number } {
  return { size: TOOL_SCHEMA_CACHE.size };
}
