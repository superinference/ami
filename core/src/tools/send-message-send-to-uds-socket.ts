// Split from send-message.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { ToolResult, DeliveryHold } from '../types';
import type { AgentMessage } from './send-message-agent-message';

/**
 * Parse a request id back into its parts, with defensive guards: returns null if
 * the `@` separator is missing, the `{type}-{timestamp}` prefix has no `-`, or
 * the timestamp is not a number. A caller can therefore trust a non-null result
 * and treat null as "not one of our ids" rather than crashing on a malformed one.
 */
export function parseRequestId(
  requestId: string,
): { requestType: string; timestamp: number; agentId: string } | null {
  const atIndex = requestId.indexOf('@');
  if (atIndex === -1) return null;
  const prefix = requestId.slice(0, atIndex);
  const agentId = requestId.slice(atIndex + 1);
  const lastDash = prefix.lastIndexOf('-');
  if (lastDash === -1) return null;
  const requestType = prefix.slice(0, lastDash);
  const timestamp = parseInt(prefix.slice(lastDash + 1), 10);
  if (Number.isNaN(timestamp)) return null;
  return { requestType, timestamp, agentId };
}

/**
 * Build a short " [re: <type> request for <agent>]" annotation for a response
 * message when its echoed request id is one of our structured ids. Returns ''
 * for a free-form/foreign id (parseRequestId returns null), so a response that
 * doesn't carry a self-describing id is simply unannotated rather than rejected.
 */
export function requestOriginTag(requestId: string): string {
  const origin = parseRequestId(requestId);
  return origin ? ` [re: ${origin.requestType} request for ${origin.agentId}]` : '';
}

/**
 * Structured-output correlation fields attached to a control-message send result,
 * riding the ToolResult `metadata` channel (the same programmatic-contract channel
 * {@link DeliveryWarning}/`errorCode` use). Mirrors the reference's
 * SendMessageToolOutput discriminators (tools/SendMessageTool/UI.tsx L21-26):
 *  - a REQUEST send carries `request_id` + `target` so a later response can be
 *    correlated back to the exact request and recipient;
 *  - a RESPONSE send additionally carries `routing: true`, marking it a
 *    fire-and-forward routed reply a consumer can correlate and suppress from the
 *    transcript (the reference's `routing` null-render branch).
 * ADDITIVE only — delivery behavior is unchanged; this is a structured hint a
 * programmatic consumer reads off the result.
 */
export interface SendMessageCorrelation {
  request_id: string;
  target: string;
  routing?: true;
}

/** Correlation metadata for a REQUEST-type send (no routing marker). */
export function requestCorrelation(requestId: string, target: string): SendMessageCorrelation {
  return { request_id: requestId, target };
}

/** Correlation metadata for a RESPONSE-type send (fire-and-forward, suppressible). */
export function responseCorrelation(requestId: string, target: string): SendMessageCorrelation {
  return { routing: true, request_id: requestId, target };
}

/**
 * Decide whether a send_message result's human confirmation should be ABSORBED
 * from the transcript (rendered as nothing) rather than echoed as a dim line. The
 * exact mirror of the reference's renderToolResultMessage two null-render branches
 * (tools/SendMessageTool/UI.tsx L21-26), keyed on the SAME {@link SendMessageCorrelation}
 * shape AMI stamps onto a control-send's `metadata`:
 *  - a truthy `routing` field → a fire-and-forward / hub-routed RESPONSE send
 *    (shutdown/plan/permission/sandbox response) a consumer correlates and suppresses;
 *  - both `request_id` AND `target` present → an async REQUEST/response correlation
 *    shape (a forwarded request awaiting a correlated reply).
 * A plain DM or broadcast carries neither (only `status`/`deliveryWarning`), so it
 * still falls through to be displayed — matching the reference, where only a bare
 * `result.message` is rendered. Purely a UI-noise decision; the model still receives
 * the tool_result in context. Returns false for absent/foreign metadata.
 */
export function shouldSuppressSendMessageTranscript(metadata: Record<string, unknown> | undefined): boolean {
  if (!metadata) return false;
  if (metadata.routing) return true;
  if (metadata.request_id !== undefined && metadata.target !== undefined) return true;
  return false;
}

// --- Recipient address schemes ---------------------------------------------
// A `to` value is normally a bare teammate name, but it may be a scheme-qualified
// cross-session address: `uds:<socket-path>` for a local peer process, or
// `bridge:<session-id>` for a Remote Control peer. A bare absolute path is a
// legacy UDS form. Everything else is an in-session teammate name ('other').
// Mirrors the reference's utils/peerAddress.ts parseAddress.

export type PeerAddress = { scheme: 'uds' | 'bridge' | 'other'; target: string };

/** Parse a URI-style recipient address into scheme + target. */
export function parseAddress(to: string): PeerAddress {
  if (to.startsWith('uds:')) return { scheme: 'uds', target: to.slice(4) };
  if (to.startsWith('bridge:')) return { scheme: 'bridge', target: to.slice(7) };
  // Legacy bare socket path → UDS (so replies to an old-style sender route to
  // the socket, not into teammate name resolution). No bare-session-id fallback:
  // a prefix-less id would hijack real teammate names.
  if (to.startsWith('/')) return { scheme: 'uds', target: to };
  return { scheme: 'other', target: to };
}

/**
 * Deliver a payload to a local peer over a Unix-domain socket (one connect →
 * write one newline-delimited JSON frame → close). Resolves when the frame is
 * flushed and the socket closes; rejects on any connection/write error so the
 * caller can report an out-of-process delivery failure instead of silently
 * dropping it. Mirrors the reference's udsClient.sendToUdsSocket.
 */
export function sendToUdsSocket(socketPath: string, payload: string): Promise<void> {
  return new Promise((resolve, reject) => {
    let settled = false;
    const done = (err?: Error) => {
      if (settled) return;
      settled = true;
      if (err) reject(err); else resolve();
    };
    // Lazy require keeps `net` out of tool-enumeration import cost.
    const net = require('net') as typeof import('net');
    const sock = net.createConnection(socketPath);
    sock.on('error', (e: Error) => done(e));
    sock.on('connect', () => {
      const frame = payload.endsWith('\n') ? payload : payload + '\n';
      sock.write(frame, () => sock.end());
    });
    sock.on('close', () => done());
  });
}

// --- Cross-process (file-backed) mailbox transport --------------------------
// The in-memory `mailboxes` Map reaches only teammates spawned IN this process.
// When `AMI_MAILBOX_FS` is set AND a team directory is established, a durable,
// per-team file inbox ({@link ./mailbox-fs}) ALSO carries each delivery so a
// teammate running as a SEPARATE OS process in the same swarm is reachable. The
// Map stays the default/fallback transport; the file inbox is purely additive.
// Resolution is lazy (and env-gated first) so the common no-team / disabled path
// pays nothing and pulls in no fs/lock machinery.
export function fsMailboxEnabled(): boolean {
  const v = process.env.AMI_MAILBOX_FS;
  if (!v) return false;
  const lc = v.toLowerCase();
  return lc !== '0' && lc !== 'false' && lc !== 'off' && lc !== 'no';
}

// --- Last outbound peer-DM tracking -----------------------------------------
// When a teammate goes idle it should tell the lead what it was last coordinating
// on, WITHOUT the model having to restate it. the reference derives that from the
// conversation (utils/teammateMailbox.ts getLastPeerDmSummary: backward-scan the
// turn for the last SendMessage tool_use whose `to` is a peer — not "*" and not
// the team lead — formatted as `[to <name>] <summary>`). AMI has no raw
// conversation here, so the equivalent is captured at the send site: every plain-
// text DM a sender emits is recorded, and the idle path scans that record
// backward for the most recent PEER DM. A non-destructive, predicate-filtered
// read (never mutates the record) — the mailbox is untouched.
/**
 * Upper bound (chars) on a derived plain-text preview — a short, 5-10 word UI hint,
 * not the full message. Mirrors the reference's `message.slice(0, 80)` preview length.
 */
export const MAX_SUMMARY_PREVIEW_CHARS = 80;

/**
 * Derive a short, whitespace-collapsed UI preview from a plain-text message. the reference
 * Code REQUIRES a `summary` on every string message (SendMessageTool validateInput)
 * so the UI always has a 5-10 word preview; AMI keeps the send working (superset —
 * no hard reject that would break the engine's plain-text teammate relay) and instead
 * DEFAULTS this preview onto the stored message when the model omits one. Collapses
 * runs of whitespace, keeps at most ~10 words, and caps the result at
 * {@link MAX_SUMMARY_PREVIEW_CHARS}. Returns '' for blank content.
 */
export function deriveSummary(content: string): string {
  const collapsed = content.replace(/\s+/g, ' ').trim();
  if (!collapsed) return '';
  const words = collapsed.split(' ').slice(0, 10).join(' ');
  return words.length > MAX_SUMMARY_PREVIEW_CHARS
    ? words.slice(0, MAX_SUMMARY_PREVIEW_CHARS).trimEnd()
    : words;
}

export interface OutboundDm {
  /** The recipient's bare name. */
  to: string;
  /** The model-supplied summary, else a truncation of the message content. */
  summary: string;
  /** True when `to` is a peer (not "*" and not the team lead). */
  peer: boolean;
}

/**
 * Bound on how many recent outbound DMs we retain per sender. Only the most
 * recent peer DM is ever read (getLastPeerDmSummary), so a small window is ample;
 * the cap keeps the record from growing without bound for a long-lived sender.
 */
export const MAX_OUTBOUND_DM_HISTORY = 50;

// --- Predicate-filtered + blocking consumption (Mailbox parity) -------------
// Mirrors the reference's utils/mailbox.ts Mailbox: in addition to the drain-all
// pollMailbox and the non-destructive peek/ack, a caller can SELECT a single
// message by an arbitrary predicate (pollMessage) and AWAIT a matching one that
// has not arrived yet (receiveMessage). A pending receiveMessage registers a
// Waiter; the canonical delivery site (enqueue) scans waiters first and, on a
// match, resolves that promise DIRECTLY — the message is handed off and never
// enqueued (bypassing the queue, so it is not also drained later). This lets a
// requester correlate a specific response (by requestId) without draining
// unrelated traffic.
export type MailboxWaiter = { fn: (msg: AgentMessage) => boolean; resolve: (msg: AgentMessage) => void };

/**
 * Hard upper bound on how many undelivered messages a single agent's mailbox may
 * retain. Without a cap a never-draining (crashed/evicted) recipient that keeps
 * being written to grows the buffer without limit — a memory-exhaustion DoS.
 * Mirrors the reference's appendCappedMessage (InProcessTeammateTask/types.ts):
 * append is bounded by dropping the OLDEST entries, keeping the newest MAX. The
 * cap is generous (a real drain happens every tool round) so legitimate traffic
 * is never lost in practice; it only engages on a pathological backlog.
 */
export const MAX_MAILBOX = 1000;

/**
 * Hard upper bound on the size (in characters) of a single message's content.
 * Complements MAX_MAILBOX: the mailbox cap bounds the *count* of retained
 * messages, this bounds the *size* of each one, so a single oversized payload
 * (or a broadcast of one, fanned into every mailbox) cannot exhaust memory.
 * The limit is generous — far larger than any legitimate coordination message —
 * so it only engages on a pathological/abusive payload.
 */
export const MAX_MESSAGE_CHARS = 100_000;

/**
 * Render a {@link DeliveryHold} into a one-line, honest "Queued. …" receipt naming
 * WHY the message waits, so the sender can reason about the peer's state. Mirrors
 * Orca's describeMailboxDeliveryHold per-kind wording.
 */
export function describeDeliveryHold(recipient: string, hold: DeliveryHold): string {
  switch (hold.kind) {
    case 'busy':
      return `Queued. ${recipient} is busy (running, not settled at its prompt); it will pick this up at its next tool round.`;
    case 'blocked':
      return `Queued. ${recipient} is blocked (${hold.reason}); delivery is held until that clears.`;
    case 'detached':
      return `Queued. ${recipient} is not attached/online yet; delivery waits until it comes up.`;
    case 'failed-turn':
      return `Queued. ${recipient}'s last turn failed; delivery waits for its next turn rather than being dropped.`;
    case 'unreachable':
      return `Queued. No live agent answers to "${hold.recipient}" (check the name); delivery waits in case it comes online.`;
  }
}

/**
 * Machine-readable error code carried on every input-SHAPE validation rejection of
 * a send_message call — empty `to`, an empty scheme-address target, an `@` in a bare
 * `to`, or empty content. Mirrors the reference's SendMessageTool.validateInput, which
 * returns `errorCode: 9` for exactly these input-shape rejections. AMI validates
 * inline in execute() rather than a separate validateInput pass, so the code rides on
 * the ToolResult `metadata` channel (the same structured-hints field cron-delete uses
 * for its `errorCode`), letting a programmatic consumer distinguish a malformed-input
 * rejection from a downstream routing/permission error.
 */
export const SEND_MESSAGE_VALIDATION_ERROR_CODE = 9;

/**
 * Machine-readable classification of a SOFT delivery warning on a plain-text send,
 * carried on the ToolResult `metadata.deliveryWarning` channel ALONGSIDE the human
 * warning string (the same structured-hints channel {@link SEND_MESSAGE_VALIDATION_ERROR_CODE}
 * rides on). Mirrors the reference's sendDirectMemberMessage typed error discriminants
 * (utils/directMemberMessage.ts DirectMessageResult.error):
 *  - `unknown_recipient` — a team roster IS established but the recipient is not a
 *    member of it (a likely typo / stale name);
 *  - `no_team_context`   — no roster is established, but other agents are live this
 *    session and the recipient is not among them.
 * AMI still QUEUES the message (its deliberate queue-to-coming-online superset — never
 * a hard reject that would break a teammate that is still spawning), so this is an
 * ADDITIVE structured hint: a programmatic consumer gets the same classification
 * the reference surfaces as a hard failure, without regressing AMI's delivery behavior.
 */
export const DELIVERY_WARNINGS = ['unknown_recipient', 'no_team_context'] as const;

export type DeliveryWarning = (typeof DELIVERY_WARNINGS)[number];

/** Build an input-shape validation error carrying {@link SEND_MESSAGE_VALIDATION_ERROR_CODE}. */
export function validationError(message: string): ToolResult {
  return { output: `Error: ${message}`, isError: true, metadata: { errorCode: SEND_MESSAGE_VALIDATION_ERROR_CODE } };
}

// --- User-facing @agent-name direct message (bypasses the model) ------------
// A human can hand a message straight to a named teammate's mailbox from the
// prompt box WITHOUT spending a model turn, mirroring the reference's
// utils/directMemberMessage.ts (parseDirectMemberMessage + sendDirectMemberMessage).
// AMI exposes teammate messaging to the MODEL via send_message; this is the
// complementary USER fast path. The parsing/detection lives in common
// (direct-member-mode.ts, the input-mode convention); THIS is the real mailbox
// delivery leg, reusing the canonical in-session transport (enqueue +
// recordOutboundDm + isReachable/roster), so there is no second messaging path.

/**
 * Result of a user-initiated direct member message. Mirrors the reference's
 * DirectMessageResult discriminants:
 *  - `success` — delivered to the named teammate's mailbox (from the lead/user);
 *  - `no_team_context`  — no team roster is established and no agent is live, so
 *    there is nobody to direct-message;
 *  - `unknown_recipient` — a team IS active but the name is not a reachable member
 *    (a likely typo). Unlike the MODEL tool (which soft-queues to a coming-online
 *    teammate), the user fast path HARD-fails an unknown name: the CLI then falls
 *    through to a normal model turn, so AMI's `@agent-<name>` composer mention is
 *    never hijacked by this shortcut.
 */
export type DirectMessageResult =
  | { success: true; recipientName: string; from: string }
  | { success: false; error: 'no_team_context' | 'unknown_recipient'; recipientName?: string };

/**
 * Normalize the model-supplied message payload to the canonical string content the
 * rest of the tool operates on. A model (or a cross-framework the reference peer) may
 * pass the message as EITHER a plain string OR a discriminated structured OBJECT —
 * the reference's SendMessage `Input.message` is a typed object carrying a `type`
 * discriminator plus payload fields (e.g. `approve`) with `to` as a sibling
 * (tools/SendMessageTool/UI.tsx). AMI routes structured control messages by JSON-
 * parsing the string content (parseStructuredMessage), so an object form is simply
 * serialized to that same JSON here — the single canonical coercion both execute()
 * and isReadOnlyForInput() use, so the string and object forms take IDENTICAL paths
 * (no parallel structured-input path). Precedence matches execute's historical
 * `message ?? content` alias. Returns:
 *  - the string verbatim when a string is supplied,
 *  - the JSON serialization when an object is supplied,
 *  - undefined when neither is present, the value is a non-string primitive, or the
 *    object cannot be serialized (e.g. a cyclic structure) — the caller then reports
 *    the normal empty-content validation error rather than crashing on `.trim()`.
 */
export function coerceMessageContent(input: Record<string, unknown>): string | undefined {
  const raw = input.message ?? input.content;
  if (typeof raw === 'string') return raw;
  if (raw && typeof raw === 'object') {
    try { return JSON.stringify(raw); } catch { return undefined; }
  }
  return undefined;
}
