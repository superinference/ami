/**
 * Message / tool-call / tool-result DISPLAY DISCRIMINATORS (pure, platform-free).
 *
 * Building blocks that classify how a message, tool call, or tool result should be
 * rendered — the branching logic a frontend needs, kept out of any UI layer so both
 * the CLI and any other frontend derive identical states from the same source.
 *
 * Parity targets (the reference):
 *  - `UserToolResultMessage` 4-way result discrimination (canceled/rejected/interrupted/error/success)
 *  - `AssistantToolUseMessage` queued (requested-but-not-started) tool-call state
 *  - `SystemTextMessage` severity-level styling + verbose-gated info suppression
 *  - `AssistantRedactedThinkingMessage` redacted/encrypted reasoning placeholder
 */

import type { ToolResult, Message, UserMessage } from './types';
import { SYNTHETIC_TOOL_RESULT_PLACEHOLDER } from './provider-transform';
import { stripInjectedContextTags } from './utils/display-tags';
import { contentToText } from './session';

// ---------------------------------------------------------------------------
// Tool-result state (parity: UserToolResultMessage.tsx L40-L104)
// ---------------------------------------------------------------------------

export const TOOL_RESULT_STATES = ['canceled', 'rejected', 'interrupted', 'ask', 'error', 'success'] as const;
export type ToolResultState = (typeof TOOL_RESULT_STATES)[number];

/**
 * Discriminate a {@link ToolResult} into one of six mutually-exclusive render
 * states, instead of collapsing everything non-successful into a single `isError`.
 *
 * Precedence mirrors the reference's component order: an explicit `canceled` marker wins,
 * then `rejected` (user denied at a permission prompt), then `interrupted`
 * (stop/abort with no real result — also inferred from the synthetic interrupt
 * placeholder content so a result that only carries the sentinel text is still
 * classified correctly), then `ask` (a RECOVERABLE validation failure the model is
 * expected to fix and retry — `behavior: 'ask'`, distinct from a fatal error so a
 * frontend can present an "ask and retry" affordance instead of a hard stop), then a
 * plain `isError`, else `success`. Parity: the reference's FileEditTool `behavior: 'ask'`
 * vs `'error'` split surfaced through the tool-result message renderer.
 */
export function classifyToolResultState(result: ToolResult): ToolResultState {
  if (result.canceled) return 'canceled';
  if (result.rejected) return 'rejected';
  if (result.interrupted || result.output === SYNTHETIC_TOOL_RESULT_PLACEHOLDER) return 'interrupted';
  if (result.behavior === 'ask') return 'ask';
  if (result.isError) return 'error';
  return 'success';
}

/** Whether a result state represents a failed/aborted call (anything but success). */
export function isUnsuccessfulToolResult(state: ToolResultState): boolean {
  return state !== 'success';
}

/**
 * Whether a tool result should be rendered to the user at all.
 *
 * A tool may mark its own result non-rendering via {@link ToolResult.suppressRender}
 * when a higher-level lifecycle message already covers it (the per-item cleanup result
 * is noise once a batched shutdown summary reports the same thing). This is the single
 * canonical gate so every frontend suppresses identically instead of reading the flag
 * ad hoc. The model always keeps `output`; only the user-facing render is skipped.
 * Parity: the reference's `renderToolResultMessage` returning `null` to suppress a redundant
 * result (TeamDeleteTool/UI.tsx).
 */
export function shouldRenderToolResult(result: ToolResult): boolean {
  return result.suppressRender !== true;
}

// ---------------------------------------------------------------------------
// Tool-use field summary (parity: SyntheticOutputTool renderToolUseMessage
// — compact affordance of the structured fields a tool call is producing)
// ---------------------------------------------------------------------------

/**
 * Compact one-line summary of a tool call's structured input fields, for the
 * tool-use header a frontend renders before a result arrives.
 *
 * Mirrors the reference's `SyntheticOutputTool.renderToolUseMessage`: with **≤ 3** keys it
 * inlines them as `k: <value>, …` so the user sees exactly what is being produced;
 * with **> 3** keys it collapses to `<n> fields: a, b, c…` (first three key names,
 * ellipsis) to stay terse. Keys are taken in object order. A non-object / empty input
 * yields an empty string (nothing to summarize). Values are coerced to a single-line
 * string and bounded so one huge value can't blow out the header.
 *
 * Pure + platform-free: both the CLI and any other frontend derive the identical
 * summary string from the same source, consistent with this module's other
 * display discriminators.
 */
export function summarizeToolInputFields(
  input: unknown,
  opts: { maxInlineKeys?: number; maxValueChars?: number } = {},
): string {
  if (input === null || typeof input !== 'object' || Array.isArray(input)) return '';
  const maxInlineKeys = opts.maxInlineKeys ?? 3;
  const maxValueChars = opts.maxValueChars ?? 40;
  const keys = Object.keys(input as Record<string, unknown>);
  if (keys.length === 0) return '';

  if (keys.length <= maxInlineKeys) {
    const rec = input as Record<string, unknown>;
    return keys
      .map(k => `${k}: ${summarizeFieldValue(rec[k], maxValueChars)}`)
      .join(', ');
  }
  // > maxInlineKeys: name the first three fields, then an ellipsis.
  return `${keys.length} fields: ${keys.slice(0, 3).join(', ')}…`;
}

/** Coerce a single structured-field value to a bounded one-line display string. */
function summarizeFieldValue(value: unknown, maxValueChars: number): string {
  let str: string;
  if (value === null) str = 'null';
  else if (typeof value === 'string') str = value;
  else if (typeof value === 'object') str = Array.isArray(value) ? `[${value.length}]` : '{…}';
  else str = String(value);
  // Collapse newlines so a multi-line value stays on the one-line header.
  str = str.replace(/\s*\n\s*/g, ' ');
  if (str.length > maxValueChars) str = `${str.slice(0, maxValueChars - 1)}…`;
  return str;
}

// ---------------------------------------------------------------------------
// Tool-call display state (parity: AssistantToolUseMessage.tsx L110-L121,L186)
// ---------------------------------------------------------------------------

export const TOOL_CALL_DISPLAY_STATES = ['queued', 'running', 'resolved'] as const;
export type ToolCallDisplayState = (typeof TOOL_CALL_DISPLAY_STATES)[number];

/**
 * Derive the per-block display state of a tool call from the two lookup sets a
 * frontend maintains, exactly as the reference does:
 *  - `resolved`  — the id is in `resolvedToolUseIds` (it has a result);
 *  - `running`   — not resolved, but in `inProgressToolUseIds` (executing now);
 *  - `queued`    — neither: requested in the assistant message but not yet started
 *                  (rendered as a dim dot so a whole batch of pending calls is visible).
 *
 * Resolved takes precedence over in-progress so a stale in-progress entry can never
 * mask a completed call.
 */
export function deriveToolCallDisplayState(
  toolCallId: string,
  inProgressToolUseIds: ReadonlySet<string>,
  resolvedToolUseIds: ReadonlySet<string>,
): ToolCallDisplayState {
  if (resolvedToolUseIds.has(toolCallId)) return 'resolved';
  if (inProgressToolUseIds.has(toolCallId)) return 'running';
  return 'queued';
}

// ---------------------------------------------------------------------------
// System-message severity display (parity: SystemTextMessage.tsx L200-L239)
// ---------------------------------------------------------------------------

export type SystemMessageLevel = 'info' | 'warning' | 'error';

export interface SystemMessageDisplay {
  /** When true the message must not be rendered at all (info-level, non-verbose). */
  suppressed: boolean;
  /** Show the leading severity dot (every level except info). */
  showDot: boolean;
  /** Text color token: `'warning'` for warning level, else undefined (default). */
  color: 'warning' | undefined;
  /** Dim the text (info-level only). */
  dimColor: boolean;
}

export interface SystemMessageDisplayOptions {
  /** Verbose mode renders info-level messages that would otherwise be suppressed. */
  verbose?: boolean;
  /** A stop-hook summary is always shown regardless of level/verbose. */
  isStopHookSummary?: boolean;
}

/**
 * Compute how a system message of a given severity `level` should render.
 *
 * Mirrors the reference's `SystemTextMessage`: info-level messages are suppressed unless
 * verbose (or it is a stop-hook summary); the dot shows for every level except info;
 * color is `'warning'` only for warnings; dimColor applies only to info. An absent
 * level is treated as `info` (AMI's default low-importance notice).
 */
export function systemMessageDisplay(
  level: SystemMessageLevel | undefined,
  opts: SystemMessageDisplayOptions = {},
): SystemMessageDisplay {
  const effective: SystemMessageLevel = level ?? 'info';
  const isInfo = effective === 'info';
  const suppressed = isInfo && !opts.verbose && !opts.isStopHookSummary;
  return {
    suppressed,
    showDot: !isInfo,
    color: effective === 'warning' ? 'warning' : undefined,
    dimColor: isInfo,
  };
}

// ---------------------------------------------------------------------------
// Redacted/encrypted reasoning placeholder (parity: AssistantRedactedThinkingMessage.tsx)
// ---------------------------------------------------------------------------

/** Dim-italic placeholder shown when the model produced an encrypted/redacted
 *  thinking block with no displayable plaintext, so the user still sees that
 *  reasoning happened. Parity: the reference's `✻ Thinking…` redacted-thinking affordance. */
export const REDACTED_THINKING_PLACEHOLDER = '✻ Thinking…';

/** A minimal structural view of a provider content block, enough to detect the
 *  `redacted_thinking` variant without depending on any provider SDK type. */
interface MaybeContentBlock {
  type?: unknown;
}

/** True when `block` is an encrypted/redacted thinking content block (the model
 *  reasoned but the plaintext was withheld). Such a block has no renderable text
 *  and must surface {@link REDACTED_THINKING_PLACEHOLDER} instead. */
export function isRedactedThinkingBlock(block: unknown): boolean {
  return (
    typeof block === 'object' &&
    block !== null &&
    (block as MaybeContentBlock).type === 'redacted_thinking'
  );
}

/** Resolve the display text for a thinking block: the redacted placeholder when the
 *  block is encrypted, otherwise the provided plaintext (trimmed to undefined when
 *  empty so a caller can skip rendering an empty thinking line). */
export function thinkingDisplayText(block: unknown, plaintext?: string): string | undefined {
  if (isRedactedThinkingBlock(block)) return REDACTED_THINKING_PLACEHOLDER;
  const text = (plaintext ?? '').trim();
  return text.length > 0 ? text : undefined;
}

// ---------------------------------------------------------------------------
// Human-turn classification (parity: messagePredicates.ts isHumanTurn, L6-8)
// ---------------------------------------------------------------------------

/**
 * Marker prefixing the engine-injected SuperInference retrieval context block when
 * it is surfaced as a user turn. SINGLE SOURCE OF TRUTH — `common/src/si-observe`
 * re-exports this so the string that is WRITTEN (the injector) and the string that
 * is DETECTED here (turn classification) can never drift.
 */
export const SI_RETRIEVAL_PREFIX = '[SI retrieval]';

/**
 * True when a user-role message is engine-injected rather than typed by the human:
 *   - a transcript-only or hook-additional-context attachment, or
 *   - content that begins with the {@link SI_RETRIEVAL_PREFIX} retrieval block, or
 *   - content consisting ENTIRELY of injected-context wrapper tags
 *     (`<system-reminder>` / IDE / local-command markers — detected via the canonical
 *     {@link stripInjectedContextTags}, so a turn that still carries human prose
 *     alongside an injected block is NOT treated as injected).
 * Non-user messages are never "injected user messages".
 */
export function isInjectedUserMessage(m: Message): boolean {
  if (m.role !== 'user') return false;
  const u = m as UserMessage;
  if (u.isVisibleInTranscriptOnly || u.hookAdditionalContext) return true;
  const text = contentToText(u.content).trimStart();
  if (!text) return false;
  if (text.startsWith(SI_RETRIEVAL_PREFIX)) return true;
  // All-injected turn: nothing survives stripping the injected-context wrappers.
  return stripInjectedContextTags(text) === '';
}

/**
 * The canonical human-turn discriminant: a user-role message that is neither an
 * ephemeral `isMeta` control message nor an {@link isInjectedUserMessage}. Used by
 * every human-turn count / lookup so engine-injected framing (system-reminder
 * blocks, the SI retrieval prefix, meta nudges) never inflates the tally.
 *
 * Parity: the reference's `isHumanTurn` discriminant
 * (references/claude/claude-code-main/src/utils/messagePredicates.ts:L6-8),
 * adapted to AMI's schema (tool results are `role:'tool'`, so the `toolUseResult`
 * half is structural; the `!isMeta` half plus injected-message exclusion is here).
 */
export function isHumanTurn(m: Message): m is UserMessage {
  return m.role === 'user' && !(m as UserMessage).isMeta && !isInjectedUserMessage(m);
}
