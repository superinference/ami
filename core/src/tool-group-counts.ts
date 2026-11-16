/**
 * Collapsed read/search tool-group counting — classify a single tool call into
 * its collapse "kind" and aggregate a run of consecutive read/search calls into
 * per-kind counts, so a frontend can render one summary row ("Read 4 files ·
 * Searched 3× · 2 git ops") instead of a wall of individual tool rows.
 *
 * Parity: Claude Code's `utils/collapseReadSearch.ts`
 * (`getToolSearchOrReadInfo`) + the `CollapsedReadSearchGroup` schema consumed by
 * `components/messages/CollapsedReadSearchContent.tsx`.
 *
 * Self-contained core building block. It reuses the existing git-operation
 * detector (`detectGitCommit`) and the MCP name parser (`parseMcpToolName`)
 * rather than re-implementing either (AGENTS.md No-Duplication Rule). Memory-path
 * recognition is injected by the caller (which knows the resolved memory dir),
 * keeping memory-dir resolution out of core's leaf layer.
 */

import type { ToolGroupCounts, ToolGroupKind } from './types';
import { detectGitCommit, GIT_VALUE_OPTS } from './tools/bash';
import { parseMcpToolName } from './tools/mcp-tool';

/** Tool calls absorbed into a collapse group visually but contributing no count
 *  (parity: Claude's silently-absorbed ToolSearch/Snip meta-operations). */
const ABSORBED_TOOLS = new Set(['tool_search']);
/** File/content read tools. */
const READ_TOOLS = new Set(['file_read']);
/** Search tools (text/symbol/reference search over the workspace). */
const SEARCH_TOOLS = new Set(['grep', 'glob', 'search_symbols', 'find_references', 'scout']);
/** Directory-listing tools. */
const LIST_TOOLS = new Set(['list_dir']);

interface ToolCallInput {
  file_path?: unknown;
  path?: unknown;
  pattern?: unknown;
  glob?: unknown;
  command?: unknown;
  [key: string]: unknown;
}

/** Extract the primary file/dir path an input names (`file_path` then `path`). */
function pathOf(input: ToolCallInput | undefined): string | undefined {
  const fp = input?.file_path;
  if (typeof fp === 'string') return fp;
  const p = input?.path;
  return typeof p === 'string' ? p : undefined;
}

/** Git subcommands that are counted as a git operation on their own, mirroring
 *  Claude's gitOperationTracking detectors (GIT_PUSH_RE / GIT_CHERRY_PICK_RE /
 *  GIT_MERGE_RE / GIT_REBASE_RE) plus the branch-creating navigation verbs. The
 *  hyphenated forms (`merge-base`, `merge-tree`, `cherry` without `-pick`) are
 *  naturally excluded because token equality requires the EXACT subcommand —
 *  this is the token-scan counterpart of Claude's `merge` `(?!-)` lookahead. */
const GIT_OP_SUBCOMMANDS = new Set(['push', 'branch', 'cherry-pick', 'merge', 'rebase']);

/** `gh pr <action>` / `glab mr <action>` verbs Claude's GH_PR_ACTIONS tracks as a
 *  PR operation (create/edit/merge/comment/close/ready + glab `mr create`). */
const PR_ACTIONS = new Set(['create', 'edit', 'merge', 'comment', 'close', 'ready']);

/**
 * Advance past a CLI's global option tokens to the index of its first positional
 * subcommand token. Options that consume a following value token (`valueOpts`,
 * e.g. git's `-C <path>`) skip two tokens; any other `-`-prefixed token skips
 * one. Generalized from the git-only scan so the same boundary logic serves git,
 * gh and glab (parity: Claude's flag/subcommand boundary skipping is not
 * git-specific). Returns the token count when no positional remains.
 */
function subcommandIndex(tokens: string[], start: number, valueOpts: ReadonlySet<string>): number {
  let j = start;
  while (j < tokens.length) {
    const t = tokens[j]!;
    if (valueOpts.has(t)) { j += 2; continue; }   // option + separate value token
    if (t.startsWith('-')) { j += 1; continue; }  // --opt=val / boolean / short flags
    break;                                        // first positional = subcommand
  }
  return j;
}

/**
 * True when a bash command is a git/forge operation that should be counted as a
 * git op rather than a generic bash call: a `git commit` (reusing
 * {@link detectGitCommit}), a `git push` / `branch` / `cherry-pick` / `merge` /
 * `rebase`, a branch-creating `git switch -c` / `git checkout -b`, or a forge PR
 * operation `gh pr <create|edit|merge|comment|close|ready>` / `glab mr <action>`.
 * Token scan (no backtracking) to stay ReDoS-safe, matching the posture of the
 * detectors in `tools/bash.ts`. Parity: Claude's `gitOperationTracking`
 * subcommand set (GIT_PUSH/CHERRY_PICK/MERGE/REBASE_RE + GH_PR_ACTIONS + glab).
 */
export function isGitOpBashCommand(command: string): boolean {
  if (typeof command !== 'string' || command === '') return false;
  if (detectGitCommit(command)) return true;
  const stripped = command.replace(/"[^"]*"|'[^']*'/g, '');
  const tokens = stripped.replace(/[;&|()]/g, ' ').split(/\s+/).filter(Boolean);
  for (let i = 0; i < tokens.length; i++) {
    const bin = tokens[i]!.replace(/^.*\//, '');
    if (bin === 'git') {
      // Find the first positional subcommand (skip global options + their values,
      // matching detectGitCommit so `git -C /x push` is still recognised).
      const j = subcommandIndex(tokens, i + 1, GIT_VALUE_OPTS);
      const sub = tokens[j];
      if (sub !== undefined && GIT_OP_SUBCOMMANDS.has(sub)) return true;
      if ((sub === 'switch' || sub === 'checkout') && tokens.slice(j + 1).some((t) => t === '-c' || t === '-b')) {
        return true;
      }
    } else if (bin === 'gh') {
      // `gh pr <action>` with pr immediately after gh (parity: Claude's
      // GH_PR_ACTIONS regex `\bgh\s+pr\s+<action>\b` tolerates no flags between).
      if (tokens[i + 1] === 'pr' && PR_ACTIONS.has(tokens[i + 2] ?? '')) return true;
    } else if (bin === 'glab') {
      // `glab mr <action>` — GitLab's gh analogue (parity: Claude's `glab mr create`).
      if (tokens[i + 1] === 'mr' && PR_ACTIONS.has(tokens[i + 2] ?? '')) return true;
    }
  }
  return false;
}

/** Options for {@link classifyToolGroupKind} / {@link aggregateToolGroupCounts}. */
export interface ToolGroupClassifyOptions {
  /** Caller-supplied predicate (it knows the resolved memory dir) that recognises
   *  a path targeting auto-managed memory, so a grep/glob over memory counts as a
   *  memory search rather than a generic search. */
  isMemoryPath?: (path: string) => boolean;
}

/**
 * Classify a single tool call into the collapse kind it contributes. Returns
 * `'none'` for a tool that is not part of a read/search collapse group.
 * Parity: Claude's `getToolSearchOrReadInfo` discriminator.
 */
export function classifyToolGroupKind(
  toolName: string,
  input?: Record<string, unknown>,
  opts?: ToolGroupClassifyOptions,
): ToolGroupKind {
  if (ABSORBED_TOOLS.has(toolName)) return 'absorbed';
  if (toolName === 'read_memory') return 'memoryRead';
  if (toolName === 'write_memory') return 'memoryWrite';
  if (toolName.startsWith('mcp__') && parseMcpToolName(toolName)) return 'mcpCall';
  if (READ_TOOLS.has(toolName)) return 'read';
  if (SEARCH_TOOLS.has(toolName)) {
    const p = pathOf(input as ToolCallInput);
    if (p !== undefined && opts?.isMemoryPath?.(p)) return 'memorySearch';
    return 'search';
  }
  if (LIST_TOOLS.has(toolName)) return 'list';
  if (toolName === 'bash') {
    const cmd = (input as ToolCallInput | undefined)?.command;
    if (typeof cmd === 'string' && isGitOpBashCommand(cmd)) return 'gitOpBash';
    return 'bash';
  }
  return 'none';
}

/** Create a zeroed {@link ToolGroupCounts}. */
export function emptyToolGroupCounts(): ToolGroupCounts {
  return {
    searchCount: 0,
    readCount: 0,
    listCount: 0,
    replCount: 0,
    bashCount: 0,
    gitOpBashCount: 0,
    mcpCallCount: 0,
    memoryReadCount: 0,
    memorySearchCount: 0,
    memoryWriteCount: 0,
    mcpServerNames: [],
  };
}

/**
 * Aggregate a run of consecutive tool calls into per-kind counts. `absorbed` and
 * `none` calls increment nothing (the former stays visible in verbose mode, the
 * latter is simply not a collapse member). MCP calls additionally record the
 * distinct server names (first-seen order, deduped) via {@link parseMcpToolName}.
 * Parity: Claude's `CollapsedReadSearchGroup` accumulation.
 */
export function aggregateToolGroupCounts(
  calls: ReadonlyArray<{ name: string; input?: Record<string, unknown> }>,
  opts?: ToolGroupClassifyOptions,
): ToolGroupCounts {
  const counts = emptyToolGroupCounts();
  const servers = new Set<string>();
  for (const call of calls) {
    const kind = classifyToolGroupKind(call.name, call.input, opts);
    switch (kind) {
      case 'search': counts.searchCount++; break;
      case 'read': counts.readCount++; break;
      case 'list': counts.listCount++; break;
      case 'repl': counts.replCount++; break;
      case 'bash': counts.bashCount++; break;
      case 'gitOpBash': counts.gitOpBashCount++; break;
      case 'memoryRead': counts.memoryReadCount++; break;
      case 'memorySearch': counts.memorySearchCount++; break;
      case 'memoryWrite': counts.memoryWriteCount++; break;
      case 'mcpCall': {
        counts.mcpCallCount++;
        const parsed = parseMcpToolName(call.name);
        if (parsed && !servers.has(parsed.serverName)) {
          servers.add(parsed.serverName);
          counts.mcpServerNames.push(parsed.serverName);
        }
        break;
      }
      // 'absorbed' and 'none' contribute no count.
    }
  }
  return counts;
}

/** True when a group has at least one non-memory read/search/bash/mcp operation
 *  (parity: Claude's `hasNonMemoryOps`), i.e. it is worth rendering as a collapsed
 *  read/search group rather than a pure memory-ops group. */
export function hasNonMemoryOps(counts: ToolGroupCounts): boolean {
  return (
    counts.searchCount > 0 ||
    counts.readCount > 0 ||
    counts.listCount > 0 ||
    counts.replCount > 0 ||
    counts.mcpCallCount > 0 ||
    counts.bashCount > 0 ||
    counts.gitOpBashCount > 0
  );
}
