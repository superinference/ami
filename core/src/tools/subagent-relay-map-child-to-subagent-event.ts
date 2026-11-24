// Split from subagent-relay.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { EngineEvent } from '../types';

/**
 * AMI's synthetic final-output tool. Its calls are internal result-emission, not
 * work worth surfacing, so the live-roster activity preview omits them (parity:
 * the reference special-cases `SYNTHETIC_OUTPUT_TOOL_NAME` out of its
 * recentActivities preview while still counting the tool use).
 */
export const SYNTHETIC_OUTPUT_TOOL_NAME = 'structured_output';

/**
 * Map an AMI tool name to a human-readable action verb for the live-roster activity
 * line. Parity: the reference's `TOOL_VERBS` table (bridge/sessionRunner.ts) keyed to
 * AMI's snake_case tool names (vs the reference's PascalCase). A tool with no mapped verb
 * falls back to its own name (parity: the reference's `TOOL_VERBS[name] ?? name`).
 */
export const TOOL_VERBS: Record<string, string> = {
  file_read: 'Reading',
  read_memory: 'Reading',
  file_edit: 'Editing',
  notebook_edit: 'Editing notebook',
  write_memory: 'Writing',
  bash: 'Running',
  grep: 'Searching',
  glob: 'Searching',
  web_search: 'Searching',
  scout: 'Searching',
  find_references: 'Searching',
  search_symbols: 'Searching',
  web_fetch: 'Fetching',
  agentic_fetch: 'Fetching',
  list_dir: 'Listing',
  task: 'Running task',
  lsp: 'LSP',
  diagnostics: 'LSP',
};

/** The action verb for a tool, falling back to the bare tool name when unmapped. */
export function toolVerb(toolName: string): string {
  return TOOL_VERBS[toolName] ?? toolName;
}

/**
 * Build a human-readable "verb + target" activity line for a sub-agent's tool call
 * so the live roster reads "Running npm test" / "Reading src/x.ts" / "Searching TODO"
 * instead of the bare tool name. Parity: the reference's `toolSummary(name, input)`
 * (bridge/sessionRunner.ts) — a verb from {@link TOOL_VERBS} plus the most salient
 * target pulled from the input via {@link summarizeToolInput}. Returns just the verb
 * when the input carries no salient target.
 */
export function toolActivitySummary(toolName: string, input: unknown): string {
  const verb = toolVerb(toolName);
  const target = summarizeToolInput(toolName, input);
  return target ? `${verb} ${target}` : verb;
}

/**
 * Build a compact, human-readable argument summary for a sub-agent's tool call so
 * the live roster shows "bash npm test" or "file_read src/x.ts" instead of just
 * the tool name. Picks the most salient field per tool and truncates. Returns ''
 * when there's nothing useful to show (keeps the bare tool name).
 */
export function summarizeToolInput(toolName: string, input: unknown): string {
  if (!input || typeof input !== 'object') return '';
  const o = input as Record<string, unknown>;
  const pick = (...keys: string[]): string | undefined => {
    for (const k of keys) {
      const v = o[k];
      if (typeof v === 'string' && v.trim()) return v.trim();
    }
    return undefined;
  };
  // Most salient argument per tool family, with sensible fallbacks.
  const raw =
    pick('command') ??
    pick('file_path', 'path', 'notebook_path') ??
    pick('pattern', 'query', 'regex') ??
    pick('url') ??
    pick('prompt', 'subject', 'description') ??
    undefined;
  if (!raw) return '';
  const oneLine = raw.replace(/\s+/g, ' ').trim();
  return oneLine.length > 80 ? `${oneLine.slice(0, 77)}...` : oneLine;
}

/**
 * Max characters of relayed child output (streamed text / thinking, live tool
 * progress, error text) surfaced on the parent roster in a single progress tick.
 * Parity: the reference bounds relayed preview text (truncateForPreview) BEFORE it
 * crosses the agent boundary so a chatty or run-away child can't flood the
 * parent's roster/stream with an unbounded blob.
 */
export const MAX_RELAY_PREVIEW_CHARS = 500;

/**
 * Clamp a relayed preview string to {@link MAX_RELAY_PREVIEW_CHARS}, appending a
 * single ellipsis when truncated. Internal spacing is otherwise preserved (the
 * caller decides whether to collapse whitespace first).
 */
export function clampRelayPreview(text: string, max = MAX_RELAY_PREVIEW_CHARS): string {
  return text.length > max ? `${text.slice(0, max)}…` : text;
}

/**
 * First non-empty line of a failed tool's output, bounded to a short slice for the
 * roster's error detail. Parity: the reference appends a bounded failure reason to a
 * sub-agent tool-error row (not just a ✗ glyph) so the roster conveys WHY it failed.
 * Returns '' when the output has no printable content.
 */
export function summarizeErrorOutput(output: string, max = 120): string {
  const firstLine = output.split('\n').map((l) => l.trim()).find((l) => l.length > 0) ?? '';
  return firstLine.length > max ? `${firstLine.slice(0, max - 1)}…` : firstLine;
}

/**
 * Pure mapper: translate a child sub-engine's EngineEvent into a parent-facing
 * `subagent_progress` event tagged with the child's node id + depth, or null for
 * events that should not be relayed (everything the live roster doesn't render).
 * Kept pure + separate from task.ts so the relay policy is fully unit-testable.
 *
 * PARITY (additive): the reference's Task tool surfaces a sub-agent's failures,
 * retries, and in-flight tool progress to the parent as they happen, not only in
 * the final summary. This mapper relays:
 *  - `error` live as an `error` activity (bounded) so a failing child isn't silent;
 *  - `retry_attempt` / `error_classified` as a `retry` activity so a recovering
 *    child reflects liveness instead of appearing frozen;
 *  - `tool_use_progress` as a bounded `text` activity so a long-running child tool
 *    (e.g. a slow `bash`) surfaces live output instead of going dark;
 *  - a failed `tool_use_result` with a bounded failure reason, not just a ✗ glyph;
 *  - `user_question` as a `waiting` activity (approval affordance) so a child
 *    BLOCKED on a permission `ask` / `AskUserQuestion` / plan-mode consent reads as
 *    "awaiting approval" on the roster instead of freezing on its last row — parity
 *    with the reference's swarmWorkerHandler `pendingWorkerRequest` "waiting for leader
 *    approval" indicator;
 * and drops whitespace-only deltas / empty progress so the roster never flickers
 * blank lines, with ALL relayed free text clamped via {@link clampRelayPreview}.
 */
export function mapChildToSubagentEvent(
  child: EngineEvent,
  nodeId: string,
  depth: number,
): EngineEvent | null {
  switch (child.type) {
    case 'text_delta':
      // PARITY: drop whitespace-only deltas (blank roster flicker) but relay the
      // text with internal spacing preserved, bounded so a run-away child can't
      // flood the parent roster in one tick (clampRelayPreview).
      return child.text && child.text.trim()
        ? { type: 'subagent_progress', nodeId, depth, activity: 'text', detail: clampRelayPreview(child.text) }
        : null;
    case 'thinking_delta':
      return child.text && child.text.trim()
        ? { type: 'subagent_progress', nodeId, depth, activity: 'thinking', detail: clampRelayPreview(child.text) }
        : null;
    case 'tool_use_progress':
      // PARITY (additive): surface a long-running child tool's live output on the
      // parent roster instead of going dark until completion. Relayed as a bounded
      // `text` activity — a progress tick is streamed output of an ALREADY-counted
      // tool, not a new tool start, so mapping it to `tool` would double-count it in
      // the roster's tool tally. Empty chunks drop like blank text/thinking deltas.
      return child.data && child.data.trim()
        ? { type: 'subagent_progress', nodeId, depth, activity: 'text', detail: clampRelayPreview(child.data) }
        : null;
    case 'tool_use_start': {
      // Omit the internal synthetic-output tool from the activity preview — it is a
      // result-emission call, not visible work (parity: the reference's recentActivities skip).
      if (child.toolName === SYNTHETIC_OUTPUT_TOOL_NAME) return null;
      // Human-readable verb + target (parity: the reference's toolSummary), e.g.
      // "Running npm test" / "Reading src/x.ts" instead of the bare tool name.
      // Carry the structured tool name (parity: the reference's progress `last_tool_name`) so the
      // roster can collapse a read/search burst without re-parsing the free-text detail.
      return {
        type: 'subagent_progress', nodeId, depth, activity: 'tool',
        detail: toolActivitySummary(child.toolName, child.input),
        lastToolName: child.toolName,
      };
    }
    case 'tool_use_result': {
      if (child.toolName === SYNTHETIC_OUTPUT_TOOL_NAME) return null;
      // Same verb as the matching start (parity: the reference's toolSummary verb) plus the
      // outcome glyph, e.g. "Running ✓" / "Searching ✗"; lastToolName keeps identity.
      // On failure, append a bounded first-line slice of the output so the roster
      // conveys WHY it failed (parity: the reference's tool-error detail), not just a ✗.
      const reason = child.isError ? summarizeErrorOutput(child.output) : '';
      return {
        type: 'subagent_progress', nodeId, depth, activity: 'tool',
        detail: `${toolVerb(child.toolName)} ${child.isError ? '✗' : '✓'}${reason ? ` — ${reason}` : ''}`,
        lastToolName: child.toolName,
      };
    }
    case 'error':
      // PARITY: surface the sub-agent's error on the live roster the moment it
      // happens, as a dedicated `error` activity (previously the parent only saw it
      // in the final summary text). Bounded so a huge stack trace can't flood the row.
      return child.error && child.error.trim()
        ? {
            type: 'subagent_progress', nodeId, depth, activity: 'error',
            detail: clampRelayPreview(`⚠ ${child.error.replace(/\s+/g, ' ').trim()}`),
          }
        : null;
    case 'retry_attempt':
      // PARITY: a transient retry is liveness, not a freeze — surface the attempt so
      // the roster shows the child is recovering (bridge/sdkProgress retry relay).
      return {
        type: 'subagent_progress', nodeId, depth, activity: 'retry',
        detail: `retry ${child.attempt}/${child.maxRetries}${child.delayMs ? ` in ${child.delayMs}ms` : ''}`,
      };
    case 'error_classified':
      // PARITY: surface a classified error's category + whether a retry follows. A
      // retrying classification reads as `retry` liveness; a terminal one as `error`.
      return {
        type: 'subagent_progress', nodeId, depth,
        activity: child.willRetry ? 'retry' : 'error',
        detail: `${child.category}${child.willRetry ? ' (retrying)' : ''}`,
      };
    case 'user_question': {
      // PARITY (additive): a child engine yields `user_question` whenever it is
      // BLOCKED on a human decision — an interactive permission `ask`, an
      // `AskUserQuestion`, or plan-mode enter/exit consent. Relay it as a dedicated
      // `waiting` activity with an "awaiting approval" affordance so the roster shows
      // the sub-agent is stalled on the user, not frozen on its last tool/text row
      // (the reference's swarmWorkerHandler `pendingWorkerRequest`). The optional
      // `toolInputSummary` (WHAT the ask is about) is woven in so the row conveys the
      // pending action, bounded via clampRelayPreview like every other relayed detail.
      const q = child.question && child.question.trim() ? child.question.trim() : 'a decision';
      const ctx = child.toolInputSummary && child.toolInputSummary.trim()
        ? ` — ${child.toolInputSummary.trim()}`
        : '';
      return {
        type: 'subagent_progress', nodeId, depth, activity: 'waiting',
        detail: clampRelayPreview(`⏳ awaiting approval: ${q}${ctx}`),
      };
    }
    default:
      return null;
  }
}
