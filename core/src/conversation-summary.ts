import type { Message, AssistantMessage, ToolMessage } from './types';
import { estimateMessageTokens, estimateToolCallTokens } from './token-estimator';
import { computeLineDiff } from './line-diff';
import { FILE_MUTATING_TOOLS } from './tools/background-live-turns';

/**
 * File-extension → human language label, mirroring Claude Code's insights
 * `EXTENSION_TO_LANGUAGE`. Used to build a per-language histogram from the
 * `file_path` the model already passes to mutating tools (no extra data).
 */
const EXTENSION_TO_LANGUAGE: Record<string, string> = {
  '.ts': 'TypeScript',
  '.tsx': 'TypeScript',
  '.js': 'JavaScript',
  '.jsx': 'JavaScript',
  '.mjs': 'JavaScript',
  '.cjs': 'JavaScript',
  '.py': 'Python',
  '.rb': 'Ruby',
  '.go': 'Go',
  '.rs': 'Rust',
  '.java': 'Java',
  '.md': 'Markdown',
  '.json': 'JSON',
  '.yaml': 'YAML',
  '.yml': 'YAML',
  '.sh': 'Shell',
  '.css': 'CSS',
  '.html': 'HTML',
};

/**
 * Map a file path to a language label via its extension, or `null` when the
 * extension is unknown. Mirrors Claude Code's `getLanguageFromPath`.
 */
export function getLanguageFromPath(filePath: string): string | null {
  const dot = filePath.lastIndexOf('.');
  const slash = Math.max(filePath.lastIndexOf('/'), filePath.lastIndexOf('\\'));
  // No extension (or a leading-dot "dotfile" with no further extension).
  if (dot <= 0 || dot < slash || dot === slash + 1) return null;
  const ext = filePath.slice(dot).toLowerCase();
  return EXTENSION_TO_LANGUAGE[ext] ?? null;
}

/**
 * Categorise a tool-error result string into a stable bucket, mirroring Claude
 * Code's insights error taxonomy. Returns `'Other'` for unrecognised failures.
 */
export function categorizeToolError(content: string): string {
  const lower = content.toLowerCase();
  if (lower.includes('exit code')) return 'Command Failed';
  if (lower.includes('rejected') || lower.includes("doesn't want")) return 'User Rejected';
  if (lower.includes('string to replace not found') || lower.includes('no changes')) return 'Edit Failed';
  if (lower.includes('modified since read')) return 'File Changed';
  if (lower.includes('exceeds maximum') || lower.includes('too large')) return 'File Too Large';
  if (lower.includes('file not found') || lower.includes('does not exist')) return 'File Not Found';
  return 'Other';
}

export function countToolUses(messages: Message[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const msg of messages) {
    if (msg.role === 'assistant' && (msg as AssistantMessage).tool_calls) {
      for (const tc of (msg as AssistantMessage).tool_calls!) {
        counts.set(tc.function.name, (counts.get(tc.function.name) ?? 0) + 1);
      }
    }
  }
  return counts;
}

export function extractFilesModified(messages: Message[]): Set<string> {
  const files = new Set<string>();
  for (const msg of messages) {
    if (msg.role === 'assistant' && (msg as AssistantMessage).tool_calls) {
      for (const tc of (msg as AssistantMessage).tool_calls!) {
        if (FILE_MUTATING_TOOLS.has(tc.function.name)) {
          try {
            const args = JSON.parse(tc.function.arguments);
            // FUNCTIONAL FIX: resolve the mutated path from all keys the real
            // tools use, not just `file_path`. The `notebook_edit` tool writes
            // `notebook_path` (see tools/notebook-edit.ts `required:
            // ['notebook_path', ...]`), and some tools accept `path`. The old
            // code only read `file_path`, so every notebook modification was
            // silently dropped from the compaction summary's filesModified set.
            // Mirrors the established pick('file_path','path','notebook_path')
            // convention in tools/subagent-relay.ts. Superset: `file_path`
            // continues to win when present.
            const p = args.file_path ?? args.path ?? args.notebook_path;
            if (typeof p === 'string' && p) files.add(p);
          } catch {}
        }
      }
    }
  }
  return files;
}

export interface ConversationMeta {
  userTurnCount: number;
  /** User turns that represent a REAL human prompt — excludes engine-injected
   *  ephemeral control messages (`isMeta`). Tool results are a separate `tool`
   *  role and were never counted here. Use this for "how many times did the human
   *  actually speak" metrics. */
  humanTurnCount: number;
  assistantTurnCount: number;
  totalToolCalls: number;
  toolUseCounts: Map<string, number>;
  filesModified: Set<string>;
  /** Lines added across all `file_edit` (LCS diff of old→new) and `file_write`
   *  (all content lines) tool calls — transcript-derived productivity metric. */
  linesAdded: number;
  /** Lines removed across all `file_edit` tool calls (LCS diff of old→new). */
  linesRemoved: number;
  /** Per-language histogram keyed by {@link getLanguageFromPath} of every
   *  mutated `file_path` (e.g. `{TypeScript: 4, Markdown: 1}`). */
  languages: Map<string, number>;
  /** Number of `bash` tool calls whose command contained `git commit`. */
  gitCommits: number;
  /** Number of `bash` tool calls whose command contained `git push`. */
  gitPushes: number;
  /** Capability-class flags derived from tool-name shape, matching Claude Code's
   *  insights special-casing: any `mcp__*` tool, `web_search`, `web_fetch`, and
   *  the `task` sub-agent tool. */
  usesMcp: boolean;
  usesWebSearch: boolean;
  usesWebFetch: boolean;
  usesSubagent: boolean;
  /** Total tool results flagged as errors (`ToolMessage.isError`). */
  toolErrors: number;
  /** Error counts bucketed by {@link categorizeToolError}. */
  toolErrorCategories: Map<string, number>;
  /** Human think-time between an assistant turn and the following human turn,
   *  in seconds, filtered to the plausible 2s–3600s window (matching Claude
   *  Code's insights response-time analysis). Order of occurrence. */
  userResponseTimes: number[];
  /** Median of {@link userResponseTimes} in seconds, or 0 when none. */
  responseTimeMedianSec: number;
  /** Mean of {@link userResponseTimes} in seconds, or 0 when none. */
  responseTimeAvgSec: number;
}

/** Response-time histogram bucket boundaries, in seconds (upper-exclusive). */
const RESPONSE_TIME_BUCKETS: ReadonlyArray<{ label: string; maxSec: number }> = [
  { label: '2-10s', maxSec: 10 },
  { label: '10-30s', maxSec: 30 },
  { label: '30-60s', maxSec: 60 },
  { label: '1-5m', maxSec: 300 },
  { label: '5-60m', maxSec: 3600 },
];

/**
 * Bucket response times (seconds) into the fixed {@link RESPONSE_TIME_BUCKETS}
 * histogram — the data a usage chart renders. Buckets with a zero count are
 * omitted so the caller only renders populated bars. Mirrors Claude Code's
 * insights response-time histogram.
 */
export function responseTimeHistogram(timesSec: number[]): Map<string, number> {
  const hist = new Map<string, number>();
  for (const t of timesSec) {
    const bucket = RESPONSE_TIME_BUCKETS.find((b) => t < b.maxSec);
    if (!bucket) continue;
    hist.set(bucket.label, (hist.get(bucket.label) ?? 0) + 1);
  }
  return hist;
}

function median(values: number[]): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

/** A message counts as a human turn when it is a user message that is NOT an
 *  engine-injected meta/control message. */
export function isHumanTurn(msg: Message): boolean {
  return msg.role === 'user' && !(msg as { isMeta?: boolean }).isMeta;
}

export function extractConversationMeta(messages: Message[]): ConversationMeta {
  let userTurnCount = 0;
  let humanTurnCount = 0;
  let assistantTurnCount = 0;
  let totalToolCalls = 0;

  let linesAdded = 0;
  let linesRemoved = 0;
  const languages = new Map<string, number>();
  let gitCommits = 0;
  let gitPushes = 0;
  let usesMcp = false;
  let usesWebSearch = false;
  let usesWebFetch = false;
  let usesSubagent = false;
  let toolErrors = 0;
  const toolErrorCategories = new Map<string, number>();
  const userResponseTimes: number[] = [];
  let lastAssistantTs: number | null = null;

  for (const msg of messages) {
    const ts = (msg as { _timestamp?: number })._timestamp;

    if (msg.role === 'user') {
      userTurnCount++;
      if (isHumanTurn(msg)) {
        humanTurnCount++;
        // Human think-time: gap from the last assistant turn to this human turn,
        // filtered to the plausible 2s–3600s window (anything shorter is a
        // tool-loop echo, longer is an away session), matching Claude Code.
        if (lastAssistantTs != null && typeof ts === 'number') {
          const gapSec = (ts - lastAssistantTs) / 1000;
          if (gapSec > 2 && gapSec < 3600) userResponseTimes.push(gapSec);
        }
      }
    }

    if (msg.role === 'assistant') {
      assistantTurnCount++;
      if (typeof ts === 'number') lastAssistantTs = ts;
      const calls = (msg as AssistantMessage).tool_calls;
      if (calls) {
        totalToolCalls += calls.length;
        for (const tc of calls) {
          const name = tc.function.name;
          if (name.startsWith('mcp__')) usesMcp = true;
          else if (name === 'web_search') usesWebSearch = true;
          else if (name === 'web_fetch') usesWebFetch = true;
          else if (name === 'task') usesSubagent = true;

          let args: Record<string, unknown>;
          try {
            const parsed = JSON.parse(tc.function.arguments);
            args = parsed && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : {};
          } catch {
            continue;
          }

          const p = (args.file_path ?? args.path ?? args.notebook_path) as unknown;
          if (typeof p === 'string' && p) {
            const lang = getLanguageFromPath(p);
            if (lang) languages.set(lang, (languages.get(lang) ?? 0) + 1);
          }

          if (name === 'file_edit') {
            const oldStr = typeof args.old_string === 'string' ? args.old_string : '';
            const newStr = typeof args.new_string === 'string' ? args.new_string : '';
            const d = computeLineDiff(oldStr, newStr);
            linesAdded += d.linesAdded;
            linesRemoved += d.linesRemoved;
          } else if (name === 'file_write') {
            const content = typeof args.content === 'string' ? args.content : '';
            if (content) linesAdded += content.split('\n').length;
          } else if (name === 'bash') {
            const cmd = typeof args.command === 'string' ? args.command : '';
            if (cmd.includes('git commit')) gitCommits++;
            if (cmd.includes('git push')) gitPushes++;
          }
        }
      }
    }

    if (msg.role === 'tool' && (msg as ToolMessage).isError) {
      toolErrors++;
      const category = categorizeToolError((msg as ToolMessage).content ?? '');
      toolErrorCategories.set(category, (toolErrorCategories.get(category) ?? 0) + 1);
    }
  }

  return {
    userTurnCount,
    humanTurnCount,
    assistantTurnCount,
    totalToolCalls,
    toolUseCounts: countToolUses(messages),
    filesModified: extractFilesModified(messages),
    linesAdded,
    linesRemoved,
    languages,
    gitCommits,
    gitPushes,
    usesMcp,
    usesWebSearch,
    usesWebFetch,
    usesSubagent,
    toolErrors,
    toolErrorCategories,
    userResponseTimes,
    responseTimeMedianSec: median(userResponseTimes),
    responseTimeAvgSec: userResponseTimes.length
      ? userResponseTimes.reduce((a, b) => a + b, 0) / userResponseTimes.length
      : 0,
  };
}

export function getConversationDuration(messages: Message[]): number {
  if (messages.length < 2) return 0;
  const first = (messages[0] as any)._timestamp;
  const last = (messages[messages.length - 1] as any)._timestamp;
  if (!first || !last) return 0;
  return last - first;
}

/**
 * Format a millisecond duration as a compact `h/m/s` string (e.g. `1h 2m`,
 * `3m 4s`, `5s`) rather than raw milliseconds — matching Claude Code's
 * CompactSummary duration wording. Sub-second durations read as `0s`.
 */
export function formatDuration(ms: number): string {
  const totalSec = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(totalSec / 3600);
  const m = Math.floor((totalSec % 3600) / 60);
  const s = totalSec % 60;
  if (h > 0) return m > 0 ? `${h}h ${m}m` : `${h}h`;
  if (m > 0) return s > 0 ? `${m}m ${s}s` : `${m}m`;
  return `${s}s`;
}

/**
 * Produce a concise, human-readable one-line conversation summary from the
 * extracted {@link ConversationMeta} plus a duration: turn counts, the top
 * tools by use count, a files-modified count, and a formatted duration. Ends
 * with an affordance hint telling the user how to see the full breakdown.
 *
 * Mirrors the wording/affordance of Claude Code's CompactSummary surface. The
 * exact rendering (colours, layout) is left to each frontend; this returns the
 * capability-defining string so TUI and detached stay 1:1.
 */
export function formatConversationSummary(meta: ConversationMeta, durationMs: number): string {
  const parts: string[] = [];
  parts.push(`${meta.humanTurnCount} human turn${meta.humanTurnCount === 1 ? '' : 's'}`);
  parts.push(`${meta.totalToolCalls} tool call${meta.totalToolCalls === 1 ? '' : 's'}`);

  const topTools = [...meta.toolUseCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3)
    .map(([name, count]) => `${name}×${count}`);
  if (topTools.length > 0) parts.push(`top: ${topTools.join(', ')}`);

  if (meta.filesModified.size > 0) {
    parts.push(`${meta.filesModified.size} file${meta.filesModified.size === 1 ? '' : 's'} modified (+${meta.linesAdded}/-${meta.linesRemoved})`);
  }

  parts.push(formatDuration(durationMs));
  return `${parts.join(' · ')} — run /recap for the full breakdown`;
}

// ---------------------------------------------------------------------------
// Compaction summary: message selection / token budgeting / tool-pair safety.
//
// When a conversation is compacted, the engine keeps a recent tail of messages
// (and optionally a few pinned turns) within a token budget and summarises the
// rest. These helpers structure that selection so the kept window is (a) within
// budget, (b) never empty for a non-empty input, and (c) free of orphaned tool
// results — a `tool` message whose originating assistant `tool_call` was
// dropped makes most providers reject the request, so the boundary MUST be
// repaired after truncation.
// ---------------------------------------------------------------------------

/**
 * Full token cost of a single message for compaction budgeting: its content
 * (via the shared {@link estimateMessageTokens}) PLUS, for assistant messages,
 * the cost of each tool call. The canonical content estimator ignores
 * `tool_calls`, so a tool-heavy turn (often `content: null`) would otherwise be
 * priced at bare per-message overhead and slip past the budget — this composes
 * the two existing estimators rather than duplicating either.
 */
export function estimateCompactionMessageTokens(msg: Message): number {
  let total = estimateMessageTokens(msg);
  if (msg.role === 'assistant' && (msg as AssistantMessage).tool_calls) {
    for (const tc of (msg as AssistantMessage).tool_calls!) {
      let input: Record<string, unknown> = {};
      try {
        const parsed = JSON.parse(tc.function.arguments);
        if (parsed && typeof parsed === 'object') input = parsed as Record<string, unknown>;
      } catch {
        // Unparseable arguments: fall back to their raw length as the cost.
        input = { __raw: tc.function.arguments };
      }
      total += estimateToolCallTokens(tc.function.name, input);
    }
  }
  return total;
}

/**
 * Remove orphaned tool results: any `tool` message whose `tool_call_id` has no
 * matching assistant `tool_call` within the SAME window. After truncating the
 * head of a conversation, a leading tool result can be left without its
 * originating assistant message — providers reject that. This preserves
 * tool-call pairing by dropping only the dangling results (never the assistant
 * calls). Returns a new array; the input is not mutated.
 */
export function dropOrphanedToolResults(messages: Message[]): Message[] {
  const callIds = new Set<string>();
  for (const msg of messages) {
    if (msg.role === 'assistant' && (msg as AssistantMessage).tool_calls) {
      for (const tc of (msg as AssistantMessage).tool_calls!) callIds.add(tc.id);
    }
  }
  return messages.filter(
    (m) => !(m.role === 'tool' && !callIds.has((m as ToolMessage).tool_call_id)),
  );
}

export interface BudgetSelection {
  /** The selected messages, in original order, with tool-call pairing repaired. */
  kept: Message[];
  /** How many of the input messages were dropped (`messages.length - kept.length`). */
  droppedCount: number;
}

/**
 * Select the most recent contiguous window of messages that fits within
 * `tokenBudget`, for context compaction.
 *
 * Rules:
 *  - The single most recent message is ALWAYS kept, even if it alone exceeds
 *    the budget, so a non-empty conversation never compacts to nothing.
 *  - Older messages are added one at a time walking backward from the tail
 *    while they still fit; the first that does not fit closes the contiguous
 *    recent window (compaction keeps a contiguous tail, not a sparse pick).
 *  - `opts.pin(msg, index)` messages are kept regardless of where they fall
 *    (e.g. the first human turn / a user-pinned turn); they do not reopen the
 *    window or evict tail messages.
 *  - The result is passed through {@link dropOrphanedToolResults} so the kept
 *    window never starts with a tool result whose assistant call was dropped.
 *
 * Empty input yields an empty selection. A non-positive budget keeps only the
 * last message (plus any pinned ones).
 */
export function selectRecentWithinBudget(
  messages: Message[],
  tokenBudget: number,
  opts?: { pin?: (msg: Message, index: number) => boolean },
): BudgetSelection {
  if (messages.length === 0) return { kept: [], droppedCount: 0 };
  const pin = opts?.pin;
  const keep = new Set<number>();
  const lastIdx = messages.length - 1;
  keep.add(lastIdx);
  let used = estimateCompactionMessageTokens(messages[lastIdx]);
  let windowOpen = true;
  for (let i = lastIdx - 1; i >= 0; i--) {
    const cost = estimateCompactionMessageTokens(messages[i]);
    if (windowOpen && used + cost <= tokenBudget) {
      keep.add(i);
      used += cost;
    } else {
      windowOpen = false;
      if (pin && pin(messages[i], i)) keep.add(i);
    }
  }
  const kept = messages.filter((_, i) => keep.has(i));
  const repaired = dropOrphanedToolResults(kept);
  return { kept: repaired, droppedCount: messages.length - repaired.length };
}

/**
 * Build a human-readable truncation marker noting how many earlier messages
 * were elided during compaction, or `null` when nothing was dropped (so the
 * caller can skip inserting a marker). Pluralises correctly.
 */
export function buildTruncationMarker(droppedCount: number): string | null {
  if (droppedCount <= 0) return null;
  const noun = droppedCount === 1 ? 'message' : 'messages';
  return `[${droppedCount} earlier ${noun} omitted during context compaction]`;
}
