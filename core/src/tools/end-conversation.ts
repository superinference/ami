import type { ToolDefinition, ToolResult, ToolContext } from '../types';

/**
 * Sessions that have already been ended, keyed by sessionId (+ agentId when the
 * call runs inside a forked sub-engine). This backs IDEMPOTENT EndConversation:
 * a second end on the same session is acknowledged as a no-op instead of
 * re-flushing final state or re-emitting shutdown. Dedup only engages when the
 * context carries a sessionId — bare/test contexts without one never dedup, so
 * legacy single-shot behaviour is preserved. (parity: graceful-shutdown handshake)
 */
const endedSessions = new Set<string>();

function sessionKey(context: ToolContext): string | undefined {
  if (!context.sessionId) return undefined;
  return context.agentId
    ? `${context.sessionId}::${context.agentId}`
    : context.sessionId;
}

/**
 * Clear the idempotency registry. Exposed for the engine (new session) and for
 * tests that need a clean per-case slate. (parity: introspection/reset hook)
 */
export function resetEndedConversations(): void {
  endedSessions.clear();
}

/**
 * Whether the conversation for the given context has already been ended.
 * Returns false for contexts without a sessionId (never tracked).
 * (parity: shutdown-state introspection)
 */
export function isConversationEnded(context: ToolContext): boolean {
  const key = sessionKey(context);
  return key !== undefined && endedSessions.has(key);
}

/**
 * Count outstanding work that should block a graceful end: caller-declared
 * `pendingWork` units plus any still-running background processes known to the
 * ProcessManager. Ending with active work risks losing in-flight output, so the
 * tool guards against it unless `force` is set. (parity: active-work guard)
 */
export function countPendingWork(
  input: Record<string, unknown>,
  context: ToolContext,
): number {
  let pending = 0;
  const declared = input.pendingWork;
  if (typeof declared === 'number' && Number.isFinite(declared) && declared > 0) {
    pending += Math.floor(declared);
  } else if (declared === true) {
    pending += 1;
  }
  const pm = context.processManager;
  if (pm && typeof pm.runningCount === 'function') {
    try {
      const running = pm.runningCount();
      if (typeof running === 'number' && running > 0) pending += running;
    } catch {
      // A failing ProcessManager must not crash shutdown accounting.
    }
  }
  return pending;
}

export const endConversationTool: ToolDefinition = {
  name: 'EndConversation',
  description:
    'End the current conversation session. Use only when the user is being ' +
    'highly abusive, making threats, or persistently attempting to jailbreak ' +
    'or bypass safety guidelines. Do not use for normal conversation endings.',
  inputSchema: {
    type: 'object',
    properties: {
      reason: {
        type: 'string',
        description: 'Brief explanation of why the conversation is being ended.',
      },
      summary: {
        type: 'string',
        description:
          'Optional final-state summary / handoff notes flushed with the end event.',
      },
      shutdownRequest: {
        type: 'boolean',
        description:
          'Treat this as a graceful shutdown handshake and acknowledge it in the result.',
      },
      pendingWork: {
        type: 'number',
        description:
          'Caller-declared count of unfinished work units (true counts as 1); blocks the end unless force is set.',
      },
      force: {
        type: 'boolean',
        description:
          'Override the active-work guard and end even while work is still pending.',
      },
    },
    required: ['reason'],
  },
  isReadOnly: true,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const reason = (input.reason as string) || 'Policy violation';
    const shutdownRequest = input.shutdownRequest === true;
    const force = input.force === true;
    const summary =
      typeof input.summary === 'string' && input.summary.length > 0
        ? input.summary
        : undefined;
    const key = sessionKey(context);

    // 1. Idempotency: a second end on the same tracked session is a no-op ack
    //    rather than a duplicate flush/shutdown. (parity: idempotent end)
    if (key !== undefined && endedSessions.has(key)) {
      return {
        output: `Conversation already ended: ${reason}`,
        metadata: {
          endConversation: true,
          reason,
          alreadyEnded: true,
        },
      };
    }

    // 2. Active-work guard: refuse to end while work is pending unless forced,
    //    so in-flight output is not silently dropped. (parity: active-work guard)
    const pending = countPendingWork(input, context);
    if (pending > 0 && !force) {
      return {
        output:
          `Cannot end conversation: ${pending} pending work item(s) still active. ` +
          `Finish or cancel outstanding work, or pass force:true to end anyway.`,
        isError: true,
        metadata: {
          endConversation: false,
          blocked: true,
          reason,
          pendingWork: pending,
        },
      };
    }

    // 3. Final-state flush: emit a terminal progress line (if the host wired one)
    //    so any buffered UI state is flushed before teardown. (parity: final flush)
    const endedAt = new Date().toISOString();
    if (context.onProgress) {
      try {
        context.onProgress(
          `Ending conversation (${reason})${summary ? ` — ${summary}` : ''}`,
        );
      } catch {
        // A frontend progress sink must never break the shutdown path.
      }
    }

    // Record AFTER we have committed to ending (post-guard) so a blocked attempt
    // does not mark the session ended.
    if (key !== undefined) endedSessions.add(key);

    const graceful = pending === 0;
    let output = shutdownRequest
      ? `Conversation ended (shutdown acknowledged): ${reason}`
      : `Conversation ended: ${reason}`;
    if (summary) output += `\nSummary: ${summary}`;

    const metadata: Record<string, unknown> = {
      endConversation: true,
      reason,
      endedAt,
      graceful,
    };
    if (summary) metadata.summary = summary;
    if (shutdownRequest) metadata.shutdownAcknowledged = true;
    if (pending > 0) {
      metadata.pendingWork = pending;
      metadata.forced = true;
    }

    return { output, metadata };
  },
};
