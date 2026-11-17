/**
 * Per-activity read/search classification for a sub-agent's live progress.
 *
 * Parity with Claude Code's `getToolSearchOrReadInfo`
 * (tasks/LocalAgentTask/utils/collapseReadSearch): each recorded tool activity is
 * pre-classified as a SEARCH (query-style — grep/glob/symbol/web search) or a READ
 * (fetch-style — file_read/list_dir/web_fetch) op so the live-progress UI can
 * collapse a run of consecutive low-signal Read/Grep/Glob operations into ONE line
 * instead of a noisy per-file scroll. A tool that is neither (an edit, bash, a
 * spawn, …) returns both flags false and therefore never collapses — a mutation
 * must stay visible.
 *
 * This is the ONE canonical classifier for AMI's NATIVE tools. For MCP tools
 * (`mcp__*`) it delegates to the existing {@link classifyMcpToolForCollapse}
 * (the MCP allowlist + verb-prefix heuristic) rather than re-deriving the rule,
 * so there is no second implementation of "is this a read/search op".
 */
import { classifyMcpToolForCollapse } from '../mcp/classify-collapse';

/** Whether a tool activity is a low-signal search op, a read op, or neither. */
export interface ToolSearchOrReadInfo {
  isSearch: boolean;
  isRead: boolean;
}

/** Native AMI tools that are SEARCH (query-style) operations. */
const NATIVE_SEARCH_TOOLS = new Set([
  'grep', 'glob', 'search_symbols', 'web_search',
]);

/** Native AMI tools that are READ (fetch-style) operations. */
const NATIVE_READ_TOOLS = new Set([
  'file_read', 'list_dir', 'notebook_read', 'web_fetch',
]);

/**
 * Classify a tool call by name as a search / read op for live-progress collapsing.
 * Native AMI tools resolve from the two curated sets above; an `mcp__*` tool
 * delegates to the canonical MCP classifier; anything else (edit/bash/spawn/…) is
 * neither, so it is never collapsed away.
 */
export function getToolSearchOrReadInfo(toolName: string): ToolSearchOrReadInfo {
  if (toolName.startsWith('mcp__')) {
    // AMI MCP tool calls are named `mcp__<server>__<tool>`; the canonical MCP
    // classifier is keyed on the bare tool name, so strip the `mcp__server__` prefix.
    const parts = toolName.split('__');
    const bare = parts.length > 2 ? parts.slice(2).join('__') : parts[parts.length - 1];
    const { isSearch, isRead } = classifyMcpToolForCollapse('', bare);
    return { isSearch, isRead };
  }
  if (NATIVE_SEARCH_TOOLS.has(toolName)) return { isSearch: true, isRead: false };
  if (NATIVE_READ_TOOLS.has(toolName)) return { isSearch: false, isRead: true };
  return { isSearch: false, isRead: false };
}

/** Whether a tool call is a collapsible read/search op (either flag set). */
export function isReadOrSearchActivity(toolName: string): boolean {
  const { isSearch, isRead } = getToolSearchOrReadInfo(toolName);
  return isSearch || isRead;
}

/**
 * Collapse the trailing run of consecutive read/search operations in a recorded
 * tool-activity list into a single compact progress label, or `undefined` when the
 * most recent op is NOT a collapsible read/search (so the caller keeps the normal
 * per-tool summary). Mirrors Claude Code's collapseReadSearch, which folds a run of
 * adjacent Read/Grep/Glob activities into one "Reading/searching (N)" line. A run
 * of a single op is NOT collapsed (there is nothing to fold) so one-off reads still
 * show their own detail.
 */
export function collapseReadSearchRun(toolNames: readonly string[]): string | undefined {
  let run = 0;
  for (let i = toolNames.length - 1; i >= 0; i--) {
    if (!isReadOrSearchActivity(toolNames[i])) break;
    run++;
  }
  if (run < 2) return undefined;
  return `Reading/searching (${run} ops)`;
}
