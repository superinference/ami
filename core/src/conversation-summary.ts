import type { Message, AssistantMessage, ToolMessage } from './types';
import { estimateMessageTokens, estimateToolCallTokens } from './token-estimator';

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

const FILE_MUTATING_TOOLS = new Set(['file_edit', 'file_write', 'notebook_edit', 'multi_edit']);

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

  for (const msg of messages) {
    if (msg.role === 'user') {
      userTurnCount++;
      if (isHumanTurn(msg)) humanTurnCount++;
    }
    if (msg.role === 'assistant') {
      assistantTurnCount++;
      if ((msg as AssistantMessage).tool_calls) {
        totalToolCalls += (msg as AssistantMessage).tool_calls!.length;
      }
    }
  }

  return {
    userTurnCount,
    humanTurnCount,
    assistantTurnCount,
    totalToolCalls,
    toolUseCounts: countToolUses(messages),
    filesModified: extractFilesModified(messages),
  };
}

export function getConversationDuration(messages: Message[]): number {
  if (messages.length < 2) return 0;
  const first = (messages[0] as any)._timestamp;
  const last = (messages[messages.length - 1] as any)._timestamp;
  if (!first || !last) return 0;
  return last - first;
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
