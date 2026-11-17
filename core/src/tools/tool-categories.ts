/**
 * PARITY (additive): human-readable capability categories for the tool set.
 * Mirrors Claude Code's `ToolSelector` buckets — 'Read-only tools', 'Edit tools',
 * 'Execution tools', 'MCP tools', 'Other tools' — so a tool can be reasoned about
 * by what it can do (read vs edit vs execute). Surfaced via `tool_search` results
 * (tool discovery) so the model sees each discovered tool's category.
 */

export type ToolCategory = 'read-only' | 'edit' | 'execution' | 'mcp' | 'other';

/** Human-readable label for each category (parity with Claude's bucket names). */
export const TOOL_CATEGORY_LABELS: Record<ToolCategory, string> = {
  'read-only': 'Read-only tools',
  edit: 'Edit tools',
  execution: 'Execution tools',
  mcp: 'MCP tools',
  other: 'Other tools',
};

/** Tools that execute arbitrary commands/code (Claude's EXECUTION bucket). */
const EXECUTION_TOOLS: ReadonlySet<string> = new Set(['bash']);

/** Tools that mutate file/notebook content (Claude's EDIT bucket). */
const EDIT_TOOLS: ReadonlySet<string> = new Set(['file_edit', 'notebook_edit']);

/** Minimal structural shape needed to categorize a tool. */
export interface CategorizableTool {
  name: string;
  isReadOnly?: boolean;
  isMcp?: boolean;
  /** Per-invocation read-only predicate (see {@link isToolReadOnlyForInput}). */
  isReadOnlyForInput?: (input: Record<string, unknown>) => boolean;
}

/**
 * Per-invocation read-only verdict for a single tool call. Most tools are read-only
 * or writable for their whole lifetime (`isReadOnly`), but a few — notably the config
 * tool — are read-only for some inputs and writable for others: a config `get` only
 * reads, while a `set` mutates on-disk config. When a tool declares
 * `isReadOnlyForInput`, that dynamic verdict wins for THIS call (parity: Claude's
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
 * Whether a tool is an MCP tool — the dynamic `isMcp` flag OR the canonical
 * `mcp__<server>__<tool>` name prefix. Single source for the MCP discriminator so
 * the categorizer, the per-mode agent-tool filter (which always allows MCP tools
 * regardless of mode), and any prefix-based permission/auto-allow matching all agree
 * on what counts as an MCP tool. Parity: Claude Code treats `mcp__`-prefixed tools
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
