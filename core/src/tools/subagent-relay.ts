import type { EngineEvent } from '../types';

/**
 * AMI's synthetic final-output tool. Its calls are internal result-emission, not
 * work worth surfacing, so the live-roster activity preview omits them (parity:
 * Claude Code special-cases `SYNTHETIC_OUTPUT_TOOL_NAME` out of its
 * recentActivities preview while still counting the tool use).
 */
const SYNTHETIC_OUTPUT_TOOL_NAME = 'structured_output';

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
 * Pure mapper: translate a child sub-engine's EngineEvent into a parent-facing
 * `subagent_progress` event tagged with the child's node id + depth, or null for
 * events that should not be relayed (everything the live roster doesn't render).
 * Kept pure + separate from task.ts so the relay policy is fully unit-testable.
 *
 * PARITY (additive): Claude Code's Task tool surfaces a sub-agent's failures to the
 * parent as they happen, not only in the final summary. This mapper now relays
 * `error` events live (as a flagged `text` activity) and drops whitespace-only
 * text/thinking deltas so the roster doesn't flicker blank lines mid-stream.
 */
export function mapChildToSubagentEvent(
  child: EngineEvent,
  nodeId: string,
  depth: number,
): EngineEvent | null {
  switch (child.type) {
    case 'text_delta':
      // PARITY: drop whitespace-only deltas (blank roster flicker) but relay the
      // raw text verbatim so internal spacing between streamed tokens is preserved.
      return child.text && child.text.trim()
        ? { type: 'subagent_progress', nodeId, depth, activity: 'text', detail: child.text }
        : null;
    case 'thinking_delta':
      return child.text && child.text.trim()
        ? { type: 'subagent_progress', nodeId, depth, activity: 'thinking', detail: child.text }
        : null;
    case 'tool_use_start': {
      // Omit the internal synthetic-output tool from the activity preview — it is a
      // result-emission call, not visible work (parity: Claude's recentActivities skip).
      if (child.toolName === SYNTHETIC_OUTPUT_TOOL_NAME) return null;
      const arg = summarizeToolInput(child.toolName, child.input);
      return { type: 'subagent_progress', nodeId, depth, activity: 'tool', detail: arg ? `${child.toolName} ${arg}` : child.toolName };
    }
    case 'tool_use_result':
      if (child.toolName === SYNTHETIC_OUTPUT_TOOL_NAME) return null;
      return {
        type: 'subagent_progress', nodeId, depth, activity: 'tool',
        detail: `${child.toolName} ${child.isError ? '✗' : '✓'}`,
      };
    case 'error':
      // PARITY: surface the sub-agent's error on the live roster the moment it
      // happens (previously the parent only saw it in the final summary text).
      return child.error && child.error.trim()
        ? {
            type: 'subagent_progress', nodeId, depth, activity: 'text',
            detail: `⚠ ${child.error.replace(/\s+/g, ' ').trim()}`,
          }
        : null;
    default:
      return null;
  }
}

/* ------------------------------------------------------------------------- *
 * Sub-agent message relay (PARITY: Claude Code subagent messaging)
 *
 * Claude Code routes messages between a parent and its spawned sub-agents with an
 * addressed mailbox: messages are queued per recipient, delivered in order, and
 * a sub-agent that has finished (or was never spawned) can no longer receive mail
 * — such sends are rejected and parked as dead-letters rather than silently lost.
 * The relay below is a pure, allocation-light model of that contract so the
 * orchestration layer can wire parent<->subagent routing without re-deriving the
 * lifecycle/ordering/dead-letter rules (and so they are unit-testable in core).
 * ------------------------------------------------------------------------- */

/** Lifecycle of a registered sub-agent from the relay's point of view. */
export type SubagentState = 'pending' | 'completed' | 'failed';

/** A routed message with a monotonic sequence number fixing delivery order. */
export interface RelayMessage {
  /** Recipient node id (the sub-agent, or 'parent'). */
  readonly to: string;
  /** Sender node id. Defaults to 'parent' when a sender is not supplied. */
  readonly from: string;
  /** Message payload. */
  readonly body: string;
  /** Monotonic, relay-global sequence number (ordering across all recipients). */
  readonly seq: number;
}

/** Why a `send` could not be accepted (only present when `accepted` is false). */
export type RelayRejectReason = 'unknown-recipient' | 'recipient-finished';

/** Result of attempting to route a message to a recipient. */
export interface RelaySendResult {
  readonly accepted: boolean;
  /** Assigned sequence number when accepted, else null. */
  readonly seq: number | null;
  readonly reason?: RelayRejectReason;
}

/** Notification emitted when a sub-agent transitions to a terminal state. */
export interface RelayCompletion {
  readonly nodeId: string;
  readonly state: SubagentState;
  /** Queued-but-undelivered messages flushed to dead-letters on completion. */
  readonly dropped: readonly RelayMessage[];
  /** True only on the transition itself; re-completing a finished agent is a no-op. */
  readonly transitioned: boolean;
}

/** Pure parent<->subagent message router. See module comment. */
export interface SubagentRelay {
  /** Register (or re-arm) a sub-agent as able to receive messages. */
  register(nodeId: string): void;
  /** True when the node is registered and has not finished. */
  isAlive(nodeId: string): boolean;
  /** Current lifecycle state, or 'unknown' for an unregistered node. */
  state(nodeId: string): SubagentState | 'unknown';
  /** Route a message to `to`. Rejected (dead-lettered) if the recipient is unknown or finished. */
  send(to: string, body: string, from?: string): RelaySendResult;
  /** FIFO-drain all pending messages for a node (empty for unknown/finished nodes). */
  deliver(nodeId: string): RelayMessage[];
  /** Count of queued-but-undelivered messages for a node. */
  pending(nodeId: string): number;
  /** Transition a node to a terminal state, flushing undelivered mail to dead-letters. */
  complete(nodeId: string, state?: 'completed' | 'failed'): RelayCompletion;
  /** All messages that could not be delivered (immutable snapshot, in arrival order). */
  deadLetters(): readonly RelayMessage[];
}

/**
 * Create a pure sub-agent message relay. No I/O, no timers — a deterministic model
 * of addressed routing, in-order delivery, terminal lifecycle and dead-lettering.
 */
export function createSubagentRelay(): SubagentRelay {
  const states = new Map<string, SubagentState>();
  const queues = new Map<string, RelayMessage[]>();
  const dead: RelayMessage[] = [];
  let seq = 0;

  const register = (nodeId: string): void => {
    states.set(nodeId, 'pending');
    if (!queues.has(nodeId)) queues.set(nodeId, []);
  };

  const state = (nodeId: string): SubagentState | 'unknown' => states.get(nodeId) ?? 'unknown';

  const isAlive = (nodeId: string): boolean => states.get(nodeId) === 'pending';

  const send = (to: string, body: string, from = 'parent'): RelaySendResult => {
    const current = states.get(to);
    // Dead-agent / dropped-message handling: an unregistered or finished recipient
    // cannot receive mail — park the message as a dead-letter and report why.
    if (current === undefined || current !== 'pending') {
      const reason: RelayRejectReason = current === undefined ? 'unknown-recipient' : 'recipient-finished';
      dead.push({ to, from, body, seq: seq++ });
      return { accepted: false, seq: null, reason };
    }
    const msg: RelayMessage = { to, from, body, seq: seq++ };
    // Ordering: append to the recipient's FIFO queue (drained oldest-first).
    (queues.get(to) as RelayMessage[]).push(msg);
    return { accepted: true, seq: msg.seq };
  };

  const deliver = (nodeId: string): RelayMessage[] => {
    const q = queues.get(nodeId);
    if (!q || q.length === 0) return [];
    // FIFO: hand back everything queued, in sequence order, and clear the mailbox.
    const out = q.slice();
    q.length = 0;
    return out;
  };

  const pending = (nodeId: string): number => queues.get(nodeId)?.length ?? 0;

  const complete = (nodeId: string, next: 'completed' | 'failed' = 'completed'): RelayCompletion => {
    const current = states.get(nodeId);
    if (current === undefined || current !== 'pending') {
      // Idempotent: re-completing (or completing an unknown node) changes nothing.
      return { nodeId, state: current ?? 'completed', dropped: [], transitioned: false };
    }
    states.set(nodeId, next);
    const q = queues.get(nodeId) as RelayMessage[];
    // Dropped-message handling: anything still queued at completion is undeliverable.
    const dropped = q.slice();
    q.length = 0;
    for (const m of dropped) dead.push(m);
    return { nodeId, state: next, dropped, transitioned: true };
  };

  const deadLetters = (): readonly RelayMessage[] => dead.slice();

  return { register, isAlive, state, send, deliver, pending, complete, deadLetters };
}
