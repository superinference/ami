/**
 * PARITY (additive): human-readable capability categories for the tool set.
 * Mirrors the reference's `ToolSelector` buckets — 'Read-only tools', 'Edit tools',
 * 'Execution tools', 'MCP tools', 'Other tools' — so a tool can be reasoned about
 * by what it can do (read vs edit vs execute). Surfaced via `tool_search` results
 * (tool discovery) so the model sees each discovered tool's category.
 */

export type ToolCategory = 'read-only' | 'edit' | 'execution' | 'mcp' | 'other';

/** Human-readable label for each category (parity with the reference's bucket names). */
export const TOOL_CATEGORY_LABELS: Record<ToolCategory, string> = {
  'read-only': 'Read-only tools',
  edit: 'Edit tools',
  execution: 'Execution tools',
  mcp: 'MCP tools',
  other: 'Other tools',
};

/** Tools that execute arbitrary commands/code (the reference's EXECUTION bucket). */
const EXECUTION_TOOLS: ReadonlySet<string> = new Set(['bash']);

/** Tools that mutate file/notebook content (the reference's EDIT bucket). */
const EDIT_TOOLS: ReadonlySet<string> = new Set(['file_edit', 'notebook_edit']);

/** Minimal structural shape needed to categorize a tool. */
export interface CategorizableTool {
  name: string;
  isReadOnly?: boolean;
  isMcp?: boolean;
  /** Per-invocation read-only predicate (see {@link isToolReadOnlyForInput}). */
  isReadOnlyForInput?: (input: Record<string, unknown>) => boolean;
  /** Static flag OR per-invocation predicate declaring concurrency safety
   *  (see {@link isToolConcurrencySafe}). */
  isConcurrencySafe?: boolean | ((input: Record<string, unknown>) => boolean);
}

/**
 * Per-invocation read-only verdict for a single tool call. Most tools are read-only
 * or writable for their whole lifetime (`isReadOnly`), but a few — notably the config
 * tool — are read-only for some inputs and writable for others: a config `get` only
 * reads, while a `set` mutates on-disk config. When a tool declares
 * `isReadOnlyForInput`, that dynamic verdict wins for THIS call (parity: the reference's
 * `Tool.isReadOnly(input)`), so a read auto-allows without a permission prompt while a
 * write stays on the permission-asking path. Tools without the hook fall back to their
 * static `isReadOnly` flag.
 */
export function isToolReadOnlyForInput(
  tool: CategorizableTool,
  input: Record<string, unknown>,
): boolean {
  if (tool.isReadOnlyForInput) return tool.isReadOnlyForInput(input);
  return !!tool.isReadOnly;
}

/**
 * Per-invocation concurrency-safety verdict for a single tool call. Most tools are
 * either always parallel-safe or always serial; a few are safe for some inputs and
 * must serialize for others (e.g. a read whose target another batched call writes).
 * When a tool declares `isConcurrencySafe` as a FUNCTION, that dynamic verdict wins
 * for THIS call (parity: the reference's `Tool.isConcurrencySafe(input)`); a BOOLEAN is used
 * verbatim; and when the field is ABSENT the verdict defaults to the per-invocation
 * read-only classification (a read-only call is concurrency-safe). The executor uses
 * this as an ADDITIONAL gate on top of read-only when forming the concurrent batch, so
 * a call that is read-only yet explicitly concurrency-UNSAFE becomes a serial barrier.
 */
export function isToolConcurrencySafe(
  tool: CategorizableTool,
  input: Record<string, unknown>,
): boolean {
  const flag = tool.isConcurrencySafe;
  if (typeof flag === 'function') return flag(input);
  if (typeof flag === 'boolean') return flag;
  return isToolReadOnlyForInput(tool, input);
}

/**
 * Whether a tool is an MCP tool — the dynamic `isMcp` flag OR the canonical
 * `mcp__<server>__<tool>` name prefix. Single source for the MCP discriminator so
 * the categorizer, the per-mode agent-tool filter (which always allows MCP tools
 * regardless of mode), and any prefix-based permission/auto-allow matching all agree
 * on what counts as an MCP tool. Parity: the reference treats `mcp__`-prefixed tools
 * as unconditionally available to every agent mode (agentToolUtils.ts:L82-85).
 */
export function isMcpTool(tool: CategorizableTool): boolean {
  return !!tool.isMcp || tool.name.startsWith('mcp__');
}

/**
 * Classify a tool into one capability category. Resolution order (first match
 * wins): MCP (dynamic, by `isMcp` flag or `mcp__` name prefix) → execution →
 * edit → read-only (by the tool's own `isReadOnly` metadata) → other (catch-all).
 */
export function getToolCategory(tool: CategorizableTool): ToolCategory {
  if (isMcpTool(tool)) return 'mcp';
  if (EXECUTION_TOOLS.has(tool.name)) return 'execution';
  if (EDIT_TOOLS.has(tool.name)) return 'edit';
  if (tool.isReadOnly) return 'read-only';
  return 'other';
}

/**
 * Parse the SERVER segment out of a canonical MCP tool name
 * (`mcp__<server>__<tool>`). Returns `undefined` for a non-MCP name or one with no
 * server segment (`mcp__`). Shared single source for the server discriminator so
 * {@link getMcpServerBuckets} and any caller grouping MCP tools by integration
 * agree on how the server is extracted. Parity: the reference's MCP tool-name parsing.
 */
export function mcpServerName(name: string): string | undefined {
  if (!name.startsWith('mcp__')) return undefined;
  const rest = name.slice('mcp__'.length);
  const sep = rest.indexOf('__');
  const server = sep === -1 ? rest : rest.slice(0, sep);
  return server || undefined;
}

/** One MCP server and the tools it contributes (sorted by tool name). */
export interface McpServerBucket<T extends CategorizableTool> {
  server: string;
  tools: T[];
}

/**
 * Group a tool set's MCP tools by their contributing server, sorting the servers —
 * and each server's tools — stably by name via `localeCompare`. Non-MCP tools are
 * ignored; an MCP tool with no parseable server segment is bucketed under
 * `'(unknown)'`. Parity: the reference's `getMcpServerBuckets` (ToolSelector.tsx),
 * which presents an agent's MCP tools grouped per server and alphabetically sorted
 * rather than as one flat list. Used by `tool_search` to surface discovered MCP
 * tools grouped by integration.
 */
export function getMcpServerBuckets<T extends CategorizableTool>(
  tools: readonly T[],
): McpServerBucket<T>[] {
  const byServer = new Map<string, T[]>();
  for (const t of tools) {
    if (!isMcpTool(t)) continue;
    const server = mcpServerName(t.name) ?? '(unknown)';
    let list = byServer.get(server);
    if (!list) { list = []; byServer.set(server, list); }
    list.push(t);
  }
  return [...byServer.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([server, ts]) => ({
      server,
      tools: [...ts].sort((a, b) => a.name.localeCompare(b.name)),
    }));
}
