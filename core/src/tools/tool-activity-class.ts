/**
 * Per-activity read/search/list classification + human-readable activity phrases
 * for a sub-agent's live progress.
 *
 * Parity with Claude Code's `getToolSearchOrReadInfo`
 * (tasks/LocalAgentTask/utils/collapseReadSearch) and the per-tool
 * `Tool.getActivityDescription(input)` / `Tool.isSearchOrReadCommand(input)`
 * hooks (per-tool definitions): each recorded tool activity is pre-classified as a SEARCH
 * (query-style — grep/glob/symbol/web search), a READ (fetch-style —
 * file_read/web_fetch) or a LIST (directory enumeration — list_dir) op so the
 * live-progress UI can collapse a run of consecutive low-signal operations into
 * ONE line instead of a noisy per-file scroll, and can show a compact
 * "Reading src/foo.ts" / "Searching for TODO" phrase instead of a raw tool name.
 * A tool that is none of these (an edit, a spawn, a mutating bash command, …)
 * returns all flags false and therefore never collapses — a mutation must stay
 * visible.
 *
 * This is the ONE canonical native-tool classifier for AMI. The richer
 * {@link classifyToolAccess} is the single implementation: it PREFERS a tool's
 * own per-invocation {@link ToolDefinition.isSearchOrReadCommand} hook (so bash is
 * classified from its actual command, and a tool is the source of truth for its
 * own calls) and otherwise falls back to the curated name sets; the legacy
 * name-only {@link getToolSearchOrReadInfo} is a thin two-flag adapter over it.
 * For MCP tools (`mcp__*`) it delegates to {@link classifyMcpToolForCollapse}.
 */
import { classifyMcpToolForCollapse } from '../mcp/classify-collapse';
import type { ToolAccessClassification, ToolDefinition } from '../types';

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
  'file_read', 'notebook_read', 'web_fetch',
]);

/** Native AMI tools that are LIST (directory-enumeration) operations. Split from
 *  the READ set so a summary can say "Listed N directories" rather than the
 *  misleading "Read N files" (parity: Claude's BASH_LIST_COMMANDS split). */
const NATIVE_LIST_TOOLS = new Set([
  'list_dir',
]);

// Bash sub-commands that classify a shell call as search / read / list. Ported
// verbatim from Claude Code's BashTool classifier (BASH_SEARCH/READ/LIST_COMMANDS)
// so a read-only shell command (`cat`, `grep`, `ls`) collapses in live progress
// exactly as the equivalent native tool would, while a mutating command does not.
const BASH_SEARCH_COMMANDS = new Set(['find', 'grep', 'rg', 'ag', 'ack', 'locate', 'which', 'whereis']);
const BASH_READ_COMMANDS = new Set([
  'cat', 'head', 'tail', 'less', 'more',
  'wc', 'stat', 'file', 'strings',
  'jq', 'awk', 'cut', 'sort', 'uniq', 'tr',
]);
const BASH_LIST_COMMANDS = new Set(['ls', 'tree', 'du']);
// Pure output/status commands that do not change the read/search nature of a
// pipeline, so `ls dir && echo ---` stays a list op (parity: SEMANTIC_NEUTRAL).
const BASH_NEUTRAL_COMMANDS = new Set(['echo', 'printf', 'true', 'false', ':']);

const NONE: ToolAccessClassification = { isSearch: false, isRead: false, isList: false };

/**
 * Classify a bash command string as a search / read / list op from its
 * sub-commands. A compound command is classified by the UNION of every
 * non-neutral sub-command; if ANY sub-command is not a known search/read/list op
 * (i.e. a mutation), the whole command is classified as none (not collapsible),
 * matching Claude's `isSearchOrReadBashCommand`.
 */
export function classifyBashAccess(command: string): ToolAccessClassification {
  if (typeof command !== 'string' || command.trim() === '') return NONE;
  // Split on pipes and top-level compound separators; a redirection target token
  // is dropped (its base command, not the file, decides the classification).
  const segments = command.split(/\||&&|\|\||;|\n|&/);
  let hasSearch = false;
  let hasRead = false;
  let hasList = false;
  let hasNonNeutral = false;
  for (const seg of segments) {
    const base = seg.trim().split(/\s+/)[0]?.replace(/^.*\//, '');
    if (!base) continue;
    if (BASH_NEUTRAL_COMMANDS.has(base)) continue;
    hasNonNeutral = true;
    const isSearch = BASH_SEARCH_COMMANDS.has(base);
    const isRead = BASH_READ_COMMANDS.has(base);
    const isList = BASH_LIST_COMMANDS.has(base);
    // An unknown / mutating sub-command disqualifies the whole pipeline.
    if (!isSearch && !isRead && !isList) return NONE;
    if (isSearch) hasSearch = true;
    if (isRead) hasRead = true;
    if (isList) hasList = true;
  }
  if (!hasNonNeutral) return NONE;
  return { isSearch: hasSearch, isRead: hasRead, isList: hasList };
}

/**
 * Canonical per-invocation read/search/list classifier. Prefers the tool's own
 * {@link ToolDefinition.isSearchOrReadCommand} hook (input-aware — e.g. bash from
 * its command) when a definition + input are supplied; otherwise classifies by the
 * tool NAME via the curated native sets, delegating `mcp__*` names to the MCP
 * classifier. Returns all-false for a mutation / unknown tool so it never collapses.
 */
export function classifyToolAccess(
  toolName: string,
  input?: Record<string, unknown>,
  tool?: ToolDefinition,
): ToolAccessClassification {
  if (tool?.isSearchOrReadCommand && input) {
    const c = tool.isSearchOrReadCommand(input);
    return { isSearch: !!c.isSearch, isRead: !!c.isRead, isList: !!c.isList };
  }
  if (toolName.startsWith('mcp__')) {
    // AMI MCP tool calls are named `mcp__<server>__<tool>`; the canonical MCP
    // classifier is keyed on the bare tool name, so strip the `mcp__server__` prefix.
    const parts = toolName.split('__');
    const bare = parts.length > 2 ? parts.slice(2).join('__') : parts[parts.length - 1];
    const { isSearch, isRead } = classifyMcpToolForCollapse('', bare);
    return { isSearch, isRead, isList: false };
  }
  if (NATIVE_SEARCH_TOOLS.has(toolName)) return { isSearch: true, isRead: false, isList: false };
  if (NATIVE_READ_TOOLS.has(toolName)) return { isSearch: false, isRead: true, isList: false };
  if (NATIVE_LIST_TOOLS.has(toolName)) return { isSearch: false, isRead: false, isList: true };
  return NONE;
}

/**
 * Classify a tool call by name as a search / read op for live-progress collapsing
 * (legacy two-flag view — a LIST op reports as neither here). Thin adapter over
 * {@link classifyToolAccess} so there is ONE classification implementation.
 */
export function getToolSearchOrReadInfo(toolName: string): ToolSearchOrReadInfo {
  const { isSearch, isRead, isList } = classifyToolAccess(toolName);
  // The legacy two-flag view folds a LIST op into READ (a directory enumeration is
  // a kind of read for collapse purposes); {@link classifyToolAccess} keeps the
  // finer isList distinction for callers that want it.
  return { isSearch, isRead: isRead || isList };
}

/** Whether a tool call is a collapsible read/search/list op (any flag set). */
export function isReadOrSearchActivity(
  toolName: string,
  input?: Record<string, unknown>,
  tool?: ToolDefinition,
): boolean {
  const { isSearch, isRead, isList } = classifyToolAccess(toolName, input, tool);
  return isSearch || isRead || isList;
}

/**
 * Collapse a trailing run of consecutive collapsible (read/search/list) operations
 * into a single compact progress label, or `undefined` when the most recent op is
 * NOT collapsible (so the caller keeps the normal per-tool summary). A run of a
 * single op is NOT collapsed (there is nothing to fold). This is the core engine;
 * {@link collapseReadSearchRun} is the name-only adapter.
 */
export function collapseActivityRun(collapsible: readonly boolean[]): string | undefined {
  let run = 0;
  for (let i = collapsible.length - 1; i >= 0; i--) {
    if (!collapsible[i]) break;
    run++;
  }
  if (run < 2) return undefined;
  return `Reading/searching (${run} ops)`;
}

/**
 * Collapse the trailing run of consecutive read/search operations in a recorded
 * tool-NAME list into a single compact progress label. Mirrors Claude Code's
 * collapseReadSearch. Prefer {@link collapseActivityRun} with input-aware flags
 * (so a read-only bash command collapses too) where the inputs are available.
 */
export function collapseReadSearchRun(toolNames: readonly string[]): string | undefined {
  return collapseActivityRun(toolNames.map((n) => isReadOrSearchActivity(n)));
}

/** Resolves a human-readable activity phrase for a tool call (parity: Claude's
 *  `ActivityDescriptionResolver`). */
export type ActivityDescriptionResolver = (
  toolName: string,
  input: Record<string, unknown>,
) => string | undefined;

function firstString(...vals: unknown[]): string | undefined {
  for (const v of vals) if (typeof v === 'string' && v.trim() !== '') return v;
  return undefined;
}

const ACTIVITY_SUMMARY_MAX = 60;
function truncate(s: string): string {
  const t = s.trim();
  return t.length > ACTIVITY_SUMMARY_MAX ? `${t.slice(0, ACTIVITY_SUMMARY_MAX - 1)}…` : t;
}

/**
 * Targeted-cell label for a notebook_edit activity phrase: the explicit `cell_id`
 * when present, else the positional `cell-N` alias derived from a finite
 * `cell_number` (accepting the quoted-number form providers sometimes emit).
 * Returns `undefined` when no cell can be resolved so the caller shows the
 * notebook path alone instead of a `cell-NaN`/`cell-undefined` artifact.
 */
function notebookCellLabel(inp: Record<string, unknown>): string | undefined {
  const id = firstString(inp.cell_id);
  if (id) return id;
  const raw = inp.cell_number;
  const n =
    typeof raw === 'number'
      ? raw
      : typeof raw === 'string' && raw.trim() !== ''
        ? Number(raw)
        : NaN;
  return Number.isFinite(n) ? `cell-${n}` : undefined;
}

/**
 * Canonical human-readable activity phrase for a tool call computed from its
 * name + input (e.g. "Reading src/foo.ts", "Searching for TODO", "Running npm
 * test"). Returns `undefined` for a tool with no meaningful phrase so the caller
 * falls back to its own summary. This is the shared implementation each tool's
 * {@link ToolDefinition.getActivityDescription} hook delegates to, so there is no
 * second copy of the phrasing rules. Parity: Claude's per-tool
 * `getActivityDescription(input)`.
 */
export function describeToolActivity(
  toolName: string,
  input: Record<string, unknown>,
): string | undefined {
  const inp = input ?? {};
  switch (toolName) {
    case 'bash': {
      const desc = firstString(inp.description, inp.command_description);
      const cmd = firstString(inp.command);
      if (desc) return `Running ${truncate(desc)}`;
      return cmd ? `Running ${truncate(cmd)}` : 'Running command';
    }
    case 'file_read': {
      const p = firstString(inp.file_path);
      return p ? `Reading ${truncate(p)}` : 'Reading file';
    }
    case 'notebook_read': {
      const p = firstString(inp.notebook_path, inp.file_path);
      return p ? `Reading ${truncate(p)}` : 'Reading notebook';
    }
    case 'file_edit': {
      const p = firstString(inp.file_path, inp.notebook_path);
      return p ? `Editing ${truncate(p)}` : 'Editing file';
    }
    case 'notebook_edit': {
      // Parity: Claude's NotebookEdit getActivityDescription ("Editing notebook
      // <summary>"), and consistent with cli/src/subagent-roster.ts, which already
      // uses the "Editing notebook" verb. Surface the TARGETED cell (cell_id, else
      // the positional cell-N alias derived from cell_number) so the live-progress
      // hint names the exact cell being edited, not just the file.
      const p = firstString(inp.notebook_path, inp.file_path);
      if (!p) return 'Editing notebook';
      const cell = notebookCellLabel(inp);
      return cell ? `Editing notebook ${truncate(p)} ${cell}` : `Editing notebook ${truncate(p)}`;
    }
    case 'file_write':
    case 'write': {
      const p = firstString(inp.file_path);
      return p ? `Writing ${truncate(p)}` : 'Writing file';
    }
    case 'grep': {
      const pat = firstString(inp.pattern);
      return pat ? `Searching for ${truncate(pat)}` : 'Searching';
    }
    case 'search_symbols': {
      const q = firstString(inp.query, inp.name, inp.pattern);
      return q ? `Searching for ${truncate(q)}` : 'Searching symbols';
    }
    case 'glob': {
      const pat = firstString(inp.pattern);
      return pat ? `Finding ${truncate(pat)}` : 'Finding files';
    }
    case 'list_dir': {
      const p = firstString(inp.path);
      return p ? `Listing ${truncate(p)}` : 'Listing directory';
    }
    case 'web_fetch': {
      const u = firstString(inp.url);
      return u ? `Fetching ${truncate(u)}` : 'Fetching page';
    }
    case 'web_search': {
      const q = firstString(inp.query);
      return q ? `Searching the web for ${truncate(q)}` : 'Searching the web';
    }
    case 'enter_worktree': {
      // Human-friendly present-tense progress label while the worktree is created,
      // distinct from the raw snake_case tool name. Parity: Claude's EnterWorktreeTool
      // userFacingName() => 'Creating worktree'.
      const label = firstString(inp.name, inp.description);
      return label ? `Creating worktree ${truncate(label)}` : 'Creating worktree';
    }
    case 'exit_worktree': {
      // Parity: Claude's ExitWorktreeTool human-friendly progress label, with the
      // keep-vs-remove action surfaced.
      const action = firstString(inp.action);
      return action ? `Exiting worktree (${truncate(action)})` : 'Exiting worktree';
    }
    default:
      return undefined;
  }
}

/**
 * Build an {@link ActivityDescriptionResolver} from a tools list: look up the tool
 * by name (or alias) and prefer its own
 * {@link ToolDefinition.getActivityDescription} hook, falling back to the shared
 * {@link describeToolActivity} phrasing when a tool does not define one. Parity:
 * Claude's `createActivityDescriptionResolver(tools)`.
 */
export function createActivityDescriptionResolver(
  tools: readonly ToolDefinition[],
): ActivityDescriptionResolver {
  return (toolName, input) => {
    const tool = tools.find((t) => t.name === toolName || t.aliases?.includes(toolName));
    return tool?.getActivityDescription?.(input) ?? describeToolActivity(toolName, input);
  };
}
