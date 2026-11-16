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
}

/**
 * Classify a tool into one capability category. Resolution order (first match
 * wins): MCP (dynamic, by `isMcp` flag or `mcp__` name prefix) → execution →
 * edit → read-only (by the tool's own `isReadOnly` metadata) → other (catch-all).
 */
export function getToolCategory(tool: CategorizableTool): ToolCategory {
  if (tool.isMcp || tool.name.startsWith('mcp__')) return 'mcp';
  if (EXECUTION_TOOLS.has(tool.name)) return 'execution';
  if (EDIT_TOOLS.has(tool.name)) return 'edit';
  if (tool.isReadOnly) return 'read-only';
  return 'other';
}
