import type { Message, ToolDefinition } from './types';
import { isEnvTruthy } from './analytics';

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

/** Options for {@link healOrphanedToolCalls}. */
export interface OrphanHealOptions {
  /** Content for the synthetic tool_result blocks emitted for unanswered tool_calls.
   *  Different causes warrant different wording — e.g. 'Result dropped during context
   *  compaction' for a truncation artifact vs 'Interrupted by user' for a real abort —
   *  instead of a single misleading generic string. Defaults to
   *  {@link SYNTHETIC_TOOL_RESULT_PLACEHOLDER}. Parity: Claude's cause-specific wording. */
  placeholder?: string;
}

export function healOrphanedToolCalls(
  messages: Message[],
  options: OrphanHealOptions = {},
): Message[] {
  const placeholder = options.placeholder ?? SYNTHETIC_TOOL_RESULT_PLACEHOLDER;
  messages = dedupeToolCallIds(messages);
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
  if (droppedLeadingOrphan && (result.length === 0 || result[0].role === 'assistant')) {
    result.unshift({
      role: 'user',
      content: '[Orphaned tool result removed due to conversation resume]',
    });
  }

  return result;
}
