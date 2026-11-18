import type { Message, ToolDefinition } from './types';
import { isEnvTruthy } from './analytics';
import { API_IMAGE_MAX_BASE64_SIZE } from './tools/file-read';
import { formatFileSize } from './utils/format';

/** Anthropic's hard per-request media ceiling (images + documents). The API rejects a
 *  request carrying more than this with a confusing error, so we proactively strip the
 *  OLDEST excess media to stay under it. Parity: Claude's `API_MAX_MEDIA_PER_REQUEST`. */
export const MAX_MEDIA_PER_REQUEST = 100;

/** Default placeholder content for a synthetic tool_result emitted when a tool_call
 *  never received a real result. Parity: Claude's SYNTHETIC_TOOL_RESULT_PLACEHOLDER. */
export const SYNTHETIC_TOOL_RESULT_PLACEHOLDER = '[Tool call interrupted — no result available]';

/**
 * Whether Anthropic prompt caching (cache_control breakpoints) should be emitted for
 * `model`. Mirrors Claude's `getPromptCachingEnabled`: a global `DISABLE_PROMPT_CACHING`
 * kill-switch plus per-family `DISABLE_PROMPT_CACHING_HAIKU/SONNET/OPUS` knobs. The
 * family match is by model-id substring (AMI has no fixed small/sonnet/opus alias set).
 */
export function getPromptCachingEnabled(model: string): boolean {
  // Global disable takes precedence.
  if (isEnvTruthy(process.env.DISABLE_PROMPT_CACHING)) return false;
  const m = model.toLowerCase();
  if (isEnvTruthy(process.env.DISABLE_PROMPT_CACHING_HAIKU) && m.includes('haiku')) return false;
  if (isEnvTruthy(process.env.DISABLE_PROMPT_CACHING_SONNET) && m.includes('sonnet')) return false;
  if (isEnvTruthy(process.env.DISABLE_PROMPT_CACHING_OPUS) && m.includes('opus')) return false;
  return true;
}

/**
 * Proactively cap the number of media items (base64 images) carried in the outbound
 * message array at `limit`, dropping the OLDEST excess first so the most recent media
 * survives. Anthropic rejects a request with more than `MAX_MEDIA_PER_REQUEST` media
 * blocks with a confusing error that is hard to recover from mid-session; stripping
 * pre-emptively keeps the request valid. Parity: Claude's `stripExcessMediaItems`
 * applied unconditionally in the request-building path (not only on error recovery).
 * Returns the same array reference when already within the limit.
 */
export function stripExcessMediaItems(
  messages: Message[],
  limit = MAX_MEDIA_PER_REQUEST,
): Message[] {
  let total = 0;
  for (const m of messages) {
    if (m.role === 'user' && Array.isArray(m.content)) {
      for (const block of m.content) if (block.type === 'image') total++;
    }
  }
  let toRemove = total - limit;
  if (toRemove <= 0) return messages;

  return messages.map((m) => {
    if (toRemove <= 0) return m;
    if (m.role !== 'user' || !Array.isArray(m.content)) return m;
    const before = toRemove;
    const filtered = m.content.filter((block) => {
      if (toRemove > 0 && block.type === 'image') {
        toRemove--;
        return false;
      }
      return true;
    });
    return before === toRemove ? m : { ...m, content: filtered };
  });
}

/**
 * A single oversized image located by its position in the outbound message array.
 * `messageIndex` is the 0-based index into the `Message[]` passed to the API;
 * `imageIndex` is the 0-based index of the image AMONG THE IMAGES in that message.
 */
export interface OversizedImage {
  messageIndex: number;
  imageIndex: number;
  /** Byte length of the base64-encoded image data (what the API actually counts). */
  base64Size: number;
}

/**
 * Thrown by {@link validateImagesForAPI} when one or more outbound images exceed
 * the API's per-image base64 ceiling. Carries the structured list so a caller/UI
 * can act on it, while `message` is a human-readable, actionable summary naming
 * exactly which images are too big. Parity: Claude Code's `ImageSizeError`.
 */
export class ImageSizeError extends Error {
  readonly oversized: readonly OversizedImage[];
  readonly limit: number;
  constructor(oversized: readonly OversizedImage[], limit: number) {
    const list = oversized
      .map(o => `message ${o.messageIndex} image ${o.imageIndex} (${formatFileSize(o.base64Size)} base64)`)
      .join(', ');
    super(
      `${oversized.length} image${oversized.length === 1 ? '' : 's'} exceed the API limit of ` +
      `${formatFileSize(limit)} base64 each: ${list}. Resize or crop ` +
      `${oversized.length === 1 ? 'it' : 'them'}, or inspect with bash instead of attaching.`,
    );
    this.name = 'ImageSizeError';
    this.oversized = oversized;
    this.limit = limit;
  }
}

/**
 * Proactively validate that no outbound image exceeds the API's per-image base64
 * ceiling ({@link API_IMAGE_MAX_BASE64_SIZE}) BEFORE the request is sent. Without
 * this, an oversized image surfaces as a confusing mid-request 400 that names no
 * image; here the turn fails fast with an actionable {@link ImageSizeError} that
 * identifies each offending image by message/image index. Parity: Claude Code's
 * `validateImagesForAPI`. A no-op (returns normally) when every image is within
 * the limit, so it is safe to call unconditionally in the request path.
 */
export function validateImagesForAPI(
  messages: Message[],
  limit = API_IMAGE_MAX_BASE64_SIZE,
): void {
  const oversized: OversizedImage[] = [];
  messages.forEach((m, messageIndex) => {
    if (!Array.isArray(m.content)) return;
    let imageIndex = 0;
    for (const block of m.content) {
      if (block.type !== 'image') continue;
      const data = block.source?.data;
      if (typeof data === 'string' && data.length > limit) {
        oversized.push({ messageIndex, imageIndex, base64Size: data.length });
      }
      imageIndex++;
    }
  });
  if (oversized.length > 0) throw new ImageSizeError(oversized, limit);
}

export function sanitizeToolCallIds(
  messages: Message[],
  provider: string,
): Message[] {
  if (provider !== 'anthropic') return messages;
  // Anthropic requires alphanumeric-only tool call IDs
  return messages.map((msg, idx) => {
    if (msg.role === 'assistant' && msg.tool_calls) {
      return {
        ...msg,
        tool_calls: msg.tool_calls.map((tc, i) => ({
          ...tc,
          id: tc.id.replace(/[^a-zA-Z0-9_-]/g, '') || `tc_${String(i).padStart(4, '0')}`,
        })),
      };
    }
    if (msg.role === 'tool') {
      const sanitized = msg.tool_call_id.replace(/[^a-zA-Z0-9_-]/g, '');
      if (sanitized) return { ...msg, tool_call_id: sanitized };
      // Count preceding consecutive tool messages to determine this tool's
      // position among its siblings — mirrors the index used for the
      // assistant-side tc_NNNN fallback so the IDs match.
      let toolIdx = 0;
      for (let j = idx - 1; j >= 0 && messages[j].role === 'tool'; j--) {
        toolIdx++;
      }
      return { ...msg, tool_call_id: `tc_${String(toolIdx).padStart(4, '0')}` };
    }
    return msg;
  });
}

export function buildConversationCachePoints(
  messages: Array<{ role: string }>,
  enablePromptCaching = true,
): Record<number, { type: string }> {
  const points: Record<number, { type: string }> = {};
  if (!enablePromptCaching || messages.length === 0) return points;

  let lastNonTool = -1;
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].role !== 'tool') {
      lastNonTool = i;
      break;
    }
  }

  let secondLastNonTool = -1;
  for (let i = lastNonTool - 1; i >= 0; i--) {
    if (messages[i].role !== 'tool') {
      secondLastNonTool = i;
      break;
    }
  }

  if (secondLastNonTool >= 0) points[secondLastNonTool] = { type: 'ephemeral' };
  if (lastNonTool >= 0) points[lastNonTool] = { type: 'ephemeral' };

  return points;
}

export function buildToolCacheBreakpoints(
  tools: ToolDefinition[],
  enablePromptCaching = true,
): Record<number, { type: string }> {
  if (!enablePromptCaching || tools.length === 0) return {};
  return { [tools.length - 1]: { type: 'ephemeral' } };
}

/**
 * Strip empty/whitespace-only text content blocks from Anthropic messages and return
 * the resulting message list. Anthropic rejects any text block with empty text. If
 * stripping leaves an assistant with no content, it is replaced with '(continued)';
 * if it leaves a NON-assistant message (user/tool) with an empty content array, the
 * message is DROPPED entirely — the API rejects a message whose content is empty, so a
 * malformed/empty user or tool turn (e.g. a compaction artifact) must not be sent.
 * Parity: Claude drops messages left with empty content after block stripping.
 */
export function stripEmptyTextBlocks(
  messages: Array<Record<string, unknown>>,
): Array<Record<string, unknown>> {
  const out: Array<Record<string, unknown>> = [];
  for (const m of messages) {
    if (Array.isArray(m.content)) {
      m.content = (m.content as Array<Record<string, unknown>>).filter(
        (p) => !(p.type === 'text' && (typeof p.text !== 'string' || (p.text as string).trim() === ''))
      );
      if ((m.content as unknown[]).length === 0) {
        if (m.role === 'assistant') {
          m.content = [{ type: 'text' as const, text: '(continued)' }];
        } else {
          // Non-assistant message emptied by stripping — drop it; the API rejects
          // a message with empty content.
          continue;
        }
      }
    } else if (typeof m.content === 'string' && m.content.trim() === '' && m.role === 'assistant') {
      m.content = '(continued)';
    }
    out.push(m);
  }
  return out;
}

/**
 * Remove duplicate tool_use / tool_result ids across messages (CC-1212).
 *
 * Compaction, resume, split-streaming re-pushes, or an orphan handler that ran
 * twice can produce two assistant tool_call blocks carrying the SAME id, or two
 * `role:'tool'` results for the same tool_call_id. The Anthropic API rejects
 * these ("tool_use ids must be unique" / "duplicate tool_result") with a 400 that
 * permanently deadlocks a resumed/compacted session. This keeps the FIRST
 * occurrence of each id and drops later duplicates, mirroring Claude's
 * `allSeenToolUseIds` pre-send pairing repair. Runs before orphan healing so the
 * synthetic-result pass sees the deduped set.
 */
export function dedupeToolCallIds(messages: Message[]): Message[] {
  const seenCallIds = new Set<string>();
  const seenResultIds = new Set<string>();
  const result: Message[] = [];

  for (const msg of messages) {
    // Drop a duplicate tool_result (same tool_call_id already emitted).
    if (msg.role === 'tool') {
      if (seenResultIds.has(msg.tool_call_id)) continue;
      seenResultIds.add(msg.tool_call_id);
      result.push(msg);
      continue;
    }
    // Strip any tool_call whose id was already emitted (cross-message or within
    // the same assistant message).
    if (msg.role === 'assistant' && msg.tool_calls && msg.tool_calls.length > 0) {
      const kept: typeof msg.tool_calls = [];
      for (const tc of msg.tool_calls) {
        if (seenCallIds.has(tc.id)) continue;
        seenCallIds.add(tc.id);
        kept.push(tc);
      }
      if (kept.length === msg.tool_calls.length) {
        result.push(msg);
      } else if (kept.length > 0) {
        result.push({ ...msg, tool_calls: kept });
      } else {
        // Every tool_call was a duplicate; keep the message but without tool_calls
        // so a stray assistant turn doesn't re-introduce the rejected ids.
        const rest = { ...msg };
        delete (rest as { tool_calls?: unknown }).tool_calls;
        result.push(rest);
      }
      continue;
    }
    result.push(msg);
  }

  return result;
}

/** Env knob gating {@link getStrictToolResultPairing}: when truthy, a transcript whose
 *  tool_use/tool_result pairing had to be repaired is treated as corruption and the
 *  turn fails fast instead of silently injecting synthetic placeholders. Parity:
 *  Claude's `getStrictToolResultPairing()` HFI opt-in (utils/messages.ts). */
export const STRICT_TOOL_PAIRING_ENV = 'SUPERINFERENCE_STRICT_TOOL_PAIRING';

/**
 * Whether strict tool-pairing mode is armed. In strict mode {@link healOrphanedToolCalls}
 * THROWS a {@link ToolPairingError} on any detected corruption (orphaned/duplicate/
 * unanswered tool blocks) rather than repairing it — so a corrupted transcript surfaces
 * loudly (with full diagnostic structure) instead of producing a model response
 * conditioned on synthetic placeholders. Off by default; env-injectable for testing.
 * Parity: Claude's `getStrictToolResultPairing()`.
 */
export function getStrictToolResultPairing(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env[STRICT_TOOL_PAIRING_ENV]);
}

/**
 * Structured record of what a tool-pairing repair pass changed, so the mutation of a
 * (possibly corrupted) transcript can be traced to its root cause instead of mutating
 * the model's context silently. Emitted via {@link OrphanHealOptions.onRepair} and
 * carried by {@link ToolPairingError} in strict mode. Parity: Claude's
 * `tengu_tool_result_pairing_repaired` analytics payload + diagnostic message-structure
 * summary.
 */
export interface ToolPairingRepairDiagnostics {
  /** Messages in the transcript before the repair pass. */
  messageCountBefore: number;
  /** Messages in the transcript after the repair pass. */
  messageCountAfter: number;
  /** Synthetic error tool_result placeholders inserted for unanswered tool_calls. */
  syntheticResultsInserted: number;
  /** Orphaned tool_results dropped (referenced a tool_call that no longer exists). */
  orphanedResultsDropped: number;
  /** Duplicate tool_use / tool_result ids removed (CC-1212 dedupe). */
  duplicateIdsRemoved: number;
  /** Whether a leading-orphan placeholder user turn was substituted on resume. */
  leadingOrphanPlaceholderInserted: boolean;
  /** Per-index message-structure summary, e.g.
   *  `[0] user(tool_results=[a]); [1] assistant(tool_uses=[b])`. Parity: Claude's
   *  `messageTypes` diagnostic string. */
  messageStructure: string;
}

/**
 * Thrown by {@link healOrphanedToolCalls} in strict mode ({@link getStrictToolResultPairing})
 * when a transcript's tool_use/tool_result pairing would have to be repaired. Carries the
 * structured {@link ToolPairingRepairDiagnostics} and a human-readable message naming the
 * corrupted message structure. Parity: Claude's strict-mode `ensureToolResultPairing` throw.
 */
export class ToolPairingError extends Error {
  readonly diagnostics: ToolPairingRepairDiagnostics;
  constructor(diagnostics: ToolPairingRepairDiagnostics) {
    super(
      'tool_use/tool_result pairing mismatch detected (strict mode). Refusing to repair — ' +
      'would inject synthetic placeholders into model context. ' +
      `Message structure: ${diagnostics.messageStructure}.`,
    );
    this.name = 'ToolPairingError';
    this.diagnostics = diagnostics;
  }
}

/** Count the tool_use blocks (assistant `tool_calls`) and tool_result messages
 *  (`role:'tool'`) in a message list — used to measure what the dedupe pass removed. */
function countToolUnits(messages: Message[]): { uses: number; results: number } {
  let uses = 0;
  let results = 0;
  for (const m of messages) {
    if (m.role === 'assistant' && m.tool_calls) uses += m.tool_calls.length;
    else if (m.role === 'tool') results++;
  }
  return { uses, results };
}

/** Build the per-index message-structure summary used in repair diagnostics. Mirrors
 *  Claude's `messageTypes` string (assistant → its tool_use ids, user/tool → tool_result
 *  ids, everything else → its role). */
export function buildToolPairingDiagnosticSummary(messages: Message[]): string {
  return messages
    .map((m, idx) => {
      if (m.role === 'assistant' && m.tool_calls && m.tool_calls.length > 0) {
        return `[${idx}] assistant(tool_uses=[${m.tool_calls.map(tc => tc.id).join(',')}])`;
      }
      if (m.role === 'tool') {
        return `[${idx}] tool(tool_result=${m.tool_call_id})`;
      }
      return `[${idx}] ${m.role}`;
    })
    .join('; ');
}

/** Options for {@link healOrphanedToolCalls}. */
export interface OrphanHealOptions {
  /** Content for the synthetic tool_result blocks emitted for unanswered tool_calls.
   *  Different causes warrant different wording — e.g. 'Result dropped during context
   *  compaction' for a truncation artifact vs 'Interrupted by user' for a real abort —
   *  instead of a single misleading generic string. Defaults to
   *  {@link SYNTHETIC_TOOL_RESULT_PLACEHOLDER}. Parity: Claude's cause-specific wording. */
  placeholder?: string;
  /** Strict mode: when true, THROW a {@link ToolPairingError} with full diagnostics if
   *  any repair would be needed, instead of silently repairing. Parity: Claude's
   *  getStrictToolResultPairing() path. Defaults to false (repair). */
  strict?: boolean;
  /** Observability hook invoked with {@link ToolPairingRepairDiagnostics} whenever the
   *  repair pass actually mutated the transcript (and strict mode did not throw). Lets a
   *  consumer log/emit an analytics event so a corrupted transcript's root cause is
   *  traceable instead of repaired silently. Parity: Claude's
   *  `tengu_tool_result_pairing_repaired` event. */
  onRepair?: (diagnostics: ToolPairingRepairDiagnostics) => void;
}

export function healOrphanedToolCalls(
  messages: Message[],
  options: OrphanHealOptions = {},
): Message[] {
  const placeholder = options.placeholder ?? SYNTHETIC_TOOL_RESULT_PLACEHOLDER;
  // Diagnostics: measure what each repair step changes so the mutation is traceable
  // (and so strict mode can refuse loudly). Built from the ORIGINAL input transcript.
  const inputCount = messages.length;
  const beforeUnits = countToolUnits(messages);
  const messageStructure = buildToolPairingDiagnosticSummary(messages);

  const deduped = dedupeToolCallIds(messages);
  const afterDedupeUnits = countToolUnits(deduped);
  const duplicateIdsRemoved =
    (beforeUnits.uses - afterDedupeUnits.uses) + (beforeUnits.results - afterDedupeUnits.results);
  messages = deduped;
  let syntheticResultsInserted = 0;
  let orphanedResultsDropped = 0;
  const result: Message[] = [];
  const answeredIds = new Set<string>();
  // All assistant-issued tool_call ids, so we can drop tool_results that refer
  // to a call that no longer exists (compaction/truncation artifacts) — these
  // otherwise cause a provider 400 ("tool_result refers to unknown tool_use") loop.
  const issuedCallIds = new Set<string>();

  for (const msg of messages) {
    if (msg.role === 'tool') {
      answeredIds.add(msg.tool_call_id);
    }
    if (msg.role === 'assistant' && msg.tool_calls) {
      for (const tc of msg.tool_calls) issuedCallIds.add(tc.id);
    }
  }

  for (const msg of messages) {
    // Drop an orphaned tool_result (no matching assistant tool_call anywhere).
    if (msg.role === 'tool' && !issuedCallIds.has(msg.tool_call_id)) {
      orphanedResultsDropped++;
      continue;
    }
    result.push(msg);

    if (msg.role === 'assistant' && msg.tool_calls) {
      const orphans = msg.tool_calls.filter(tc => !answeredIds.has(tc.id));
      for (const tc of orphans) {
        result.push({
          role: 'tool',
          tool_call_id: tc.id,
          content: placeholder,
          isError: true,
        });
        syntheticResultsInserted++;
      }
    }
  }

  // Resume safety: when the transcript began with an orphaned tool_result (whose
  // assistant tool_call was removed by earlier compaction) and dropping it leaves
  // the payload starting with an assistant turn — or empties it entirely (the only
  // surviving messages were orphaned tool_results) — which the Anthropic API rejects
  // (it must lead with a user message and contain at least one message) — substitute
  // a placeholder user turn to preserve role alternation / a non-empty payload.
  // Mirrors Claude's "Orphaned tool result removed" repair. Scoped to that specific
  // case so a slice that legitimately starts with an assistant turn is left untouched.
  const first = messages[0];
  const droppedLeadingOrphan = !!first && first.role === 'tool' && !issuedCallIds.has(first.tool_call_id);
  let leadingOrphanPlaceholderInserted = false;
  if (droppedLeadingOrphan && (result.length === 0 || result[0].role === 'assistant')) {
    result.unshift({
      role: 'user',
      content: '[Orphaned tool result removed due to conversation resume]',
    });
    leadingOrphanPlaceholderInserted = true;
  }

  // Strict-mode / observability surface: any mutation of the tool-pairing stream means
  // the transcript was corrupted. In strict mode fail fast with full diagnostics; else
  // notify the observability hook so the repair is traceable (not silent).
  const repaired =
    duplicateIdsRemoved > 0 ||
    orphanedResultsDropped > 0 ||
    syntheticResultsInserted > 0 ||
    leadingOrphanPlaceholderInserted;
  if (repaired && (options.strict || options.onRepair)) {
    const diagnostics: ToolPairingRepairDiagnostics = {
      messageCountBefore: inputCount,
      messageCountAfter: result.length,
      syntheticResultsInserted,
      orphanedResultsDropped,
      duplicateIdsRemoved,
      leadingOrphanPlaceholderInserted,
      messageStructure,
    };
    if (options.strict) throw new ToolPairingError(diagnostics);
    options.onRepair?.(diagnostics);
  }

  return result;
}

/**
 * Drop orphaned "thinking-only" assistant messages before a transcript is resumed/
 * restored. In AMI an {@link AssistantMessage} stores reasoning OUT of band, so a turn
 * that produced only a thinking block is persisted as an assistant message with no text
 * content (`null`/empty/whitespace) AND no `tool_calls`. These arise when streaming
 * yields one message per content block and interleaved user turns prevent id-merging;
 * on resume the provider rejects such an empty assistant turn ("all messages must have
 * non-empty content"), permanently wedging the session. Dropping them is safe — they
 * carry nothing sendable. A turn with text, or with `tool_calls` (content may be `null`),
 * is preserved. Parity: Claude's `filterOrphanedThinkingOnlyMessages`, adapted to AMI's
 * role-based {@link Message} shape. Order-preserving; returns the same reference when
 * nothing is dropped.
 */
export function filterOrphanedThinkingOnlyMessages(messages: Message[]): Message[] {
  const isThinkingOnly = (m: Message): boolean => {
    if (m.role !== 'assistant') return false;
    if (m.tool_calls && m.tool_calls.length > 0) return false;
    const c = m.content;
    return c === null || c === undefined || (typeof c === 'string' && c.trim() === '');
  };
  if (!messages.some(isThinkingOnly)) return messages;
  return messages.filter(m => !isThinkingOnly(m));
}

/** Placeholder substituted into an assistant turn that was emptied by stripping every
 *  one of its tool_calls (a turn that had NO text, only now-unavailable tool_calls).
 *  Parity: Claude's '[Tool references removed - tools no longer available]'. */
export const UNAVAILABLE_TOOL_REFERENCE_PLACEHOLDER =
  '[Tool references removed - tools no longer available]';

/** Diagnostics describing what {@link stripUnavailableToolReferences} removed, so the
 *  drop is observable (logged/emitted) instead of silent. Parity: Claude's
 *  `logForDebugging('Filtering out tool_reference for unavailable tool: …')`. */
export interface UnavailableToolReferenceDiagnostics {
  /** Assistant tool_calls removed because their tool name was not available. */
  toolCallsDropped: number;
  /** tool_result messages removed because they answered a dropped tool_call. */
  toolResultsDropped: number;
  /** Assistant turns whose content was replaced by the placeholder (all tool_calls
   *  stripped and no surviving text). */
  placeholdersInserted: number;
  /** Distinct tool names that were stripped, for the diagnostic payload. */
  droppedToolNames: string[];
}

/** Options for {@link stripUnavailableToolReferences}. */
export interface StripUnavailableToolReferencesOptions {
  /** Content substituted when an assistant turn is emptied by the strip. Defaults to
   *  {@link UNAVAILABLE_TOOL_REFERENCE_PLACEHOLDER}. */
  placeholder?: string;
  /** Observability hook invoked with {@link UnavailableToolReferenceDiagnostics} when
   *  (and only when) the transform actually removed something. Parity: Claude logs the
   *  drop; AMI surfaces it so a disconnected-MCP / removed-plugin tool reference is
   *  traceable instead of silently repaired. */
  onStrip?: (diagnostics: UnavailableToolReferenceDiagnostics) => void;
}

/**
 * Strip references to tools that are NOT in the request's available tool set.
 *
 * Between turns a tool can vanish — an MCP server disconnects, a plugin is removed, a
 * tier changes — leaving a transcript whose assistant `tool_calls` (and their matching
 * `tool_result` messages) point at a tool name the provider no longer knows. Anthropic
 * rejects such a request ("tool_use/tool_result refers to a tool not in the request")
 * and the session wedges. {@link healOrphanedToolCalls} only reconciles id-pairing; it
 * never checks the referenced tool NAME against what is actually available, so a
 * stale-tool reference slips straight through to the provider.
 *
 * This transform closes that gap by taking the live {@link ToolDefinition} name set and:
 *  - dropping every assistant `tool_call` whose `function.name` is absent from it;
 *  - dropping the matching `tool_result` message (by `tool_call_id`);
 *  - when an assistant turn is emptied (all tool_calls stripped AND no text content),
 *    substituting a placeholder text block so the turn stays non-empty/valid.
 *
 * Order-preserving; returns the SAME reference when nothing references an unavailable
 * tool (fast no-op). Mirrors Claude's `stripUnavailableToolReferencesFromUserMessage`,
 * adapted to AMI's role-based {@link Message} shape (tool name lives on the assistant
 * `tool_call`, not on the `tool_result`). Run BEFORE {@link healOrphanedToolCalls} so the
 * heal pass sees a transcript already free of stale-tool references.
 */
export function stripUnavailableToolReferences(
  messages: Message[],
  availableToolNames: Iterable<string>,
  options: StripUnavailableToolReferencesOptions = {},
): Message[] {
  const available =
    availableToolNames instanceof Set
      ? availableToolNames
      : new Set<string>(availableToolNames);
  const placeholder = options.placeholder ?? UNAVAILABLE_TOOL_REFERENCE_PLACEHOLDER;

  // First pass: identify the tool_call ids that reference an unavailable tool, so the
  // second pass can drop both the call and its paired result.
  const unavailableCallIds = new Set<string>();
  const droppedToolNames = new Set<string>();
  for (const msg of messages) {
    if (msg.role === 'assistant' && msg.tool_calls) {
      for (const tc of msg.tool_calls) {
        if (!available.has(tc.function.name)) {
          unavailableCallIds.add(tc.id);
          droppedToolNames.add(tc.function.name);
        }
      }
    }
  }
  if (unavailableCallIds.size === 0) return messages;

  let toolCallsDropped = 0;
  let toolResultsDropped = 0;
  let placeholdersInserted = 0;
  const result: Message[] = [];
  for (const msg of messages) {
    // Drop a tool_result that answered a now-unavailable tool_call.
    if (msg.role === 'tool' && unavailableCallIds.has(msg.tool_call_id)) {
      toolResultsDropped++;
      continue;
    }
    if (msg.role === 'assistant' && msg.tool_calls) {
      const kept = msg.tool_calls.filter(tc => !unavailableCallIds.has(tc.id));
      if (kept.length !== msg.tool_calls.length) {
        toolCallsDropped += msg.tool_calls.length - kept.length;
        const hasText = typeof msg.content === 'string' && msg.content.trim() !== '';
        if (kept.length === 0 && !hasText) {
          // Turn emptied entirely → substitute the placeholder text so the provider
          // does not reject an empty assistant turn. Parity: Claude's placeholder block.
          result.push({ ...msg, content: placeholder, tool_calls: undefined });
          placeholdersInserted++;
        } else {
          result.push({ ...msg, tool_calls: kept.length > 0 ? kept : undefined });
        }
        continue;
      }
    }
    result.push(msg);
  }

  options.onStrip?.({
    toolCallsDropped,
    toolResultsDropped,
    placeholdersInserted,
    droppedToolNames: [...droppedToolNames],
  });
  return result;
}
