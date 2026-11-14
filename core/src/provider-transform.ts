import type { Message, ToolDefinition } from './types';

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
): Record<number, { type: string }> {
  const points: Record<number, { type: string }> = {};
  if (messages.length === 0) return points;

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
): Record<number, { type: string }> {
  if (tools.length === 0) return {};
  return { [tools.length - 1]: { type: 'ephemeral' } };
}

/**
 * Strip empty/whitespace-only text content blocks from Anthropic messages.
 * Anthropic rejects any text block with empty text. If stripping leaves
 * an assistant with no content, replace it with '(continued)'.
 */
export function stripEmptyTextBlocks(messages: Array<Record<string, unknown>>): void {
  for (const m of messages) {
    if (Array.isArray(m.content)) {
      m.content = (m.content as Array<Record<string, unknown>>).filter(
        (p) => !(p.type === 'text' && (typeof p.text !== 'string' || (p.text as string).trim() === ''))
      );
      if ((m.content as unknown[]).length === 0 && m.role === 'assistant') {
        m.content = [{ type: 'text' as const, text: '(continued)' }];
      }
    } else if (typeof m.content === 'string' && m.content.trim() === '' && m.role === 'assistant') {
      m.content = '(continued)';
    }
  }
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

export function healOrphanedToolCalls(messages: Message[]): Message[] {
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
          content: '[Tool call interrupted — no result available]',
        });
      }
    }
  }

  // Resume safety: when the transcript began with an orphaned tool_result (whose
  // assistant tool_call was removed by earlier compaction) and dropping it leaves
  // the payload starting with an assistant turn — which the Anthropic API rejects
  // (it must lead with a user message) — substitute a placeholder user turn to
  // preserve role alternation. Mirrors Claude's "Orphaned tool result removed"
  // repair. Scoped to that specific case so a slice that legitimately starts with
  // an assistant turn is left untouched.
  const first = messages[0];
  const droppedLeadingOrphan = !!first && first.role === 'tool' && !issuedCallIds.has(first.tool_call_id);
  if (droppedLeadingOrphan && result.length > 0 && result[0].role === 'assistant') {
    result.unshift({
      role: 'user',
      content: '[Orphaned tool result removed due to conversation resume]',
    });
  }

  return result;
}
