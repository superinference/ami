import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { isTerminalTaskStatus } from '../process-manager';
import { normalizePermissionMode, PermissionMode, PERMISSION_MODES } from '../permissions';
import { isTeammatesEnabled } from './tool-gates';

interface AgentMessage {
  /**
   * Stable per-message identifier, assigned at enqueue. It is the ack handle for
   * crash-safe at-least-once delivery: a consumer peeks (reads without removing),
   * processes, then acks by id (markMailboxDelivered). An unacked message is NOT
   * removed, so it is redelivered on the next peek — a consumer that crashes mid-
   * processing never drops the message, and acking by id (not index) is immune to
   * a concurrent enqueue arriving between the peek and the ack.
   *
   * Optional only at the type level: the single canonical append site
   * (appendCapped) assigns one when absent, so every message actually sitting in a
   * mailbox always has an id.
   */
  id?: string;
  from: string;
  to: string;
  content: string;
  summary?: string;
  timestamp: number;
  /**
   * Origin category of the message, classified at the single canonical delivery
   * site (enqueue) so every queued/handed-off message carries it. Lets a consumer
   * filter/route by where a message came from (e.g. relay 'teammate' chatter but
   * drain 'system'/'task' control traffic differently) without re-parsing the
   * content. Mirrors Claude Code's mailbox `Message.source` (MessageSource). Set
   * automatically — a caller never supplies it.
   */
  source?: MessageSource;
}

/**
 * The origin category of a mailbox message. Mirrors Claude Code's
 * utils/mailbox.ts `MessageSource`:
 * - `user`     — authored by the user-facing lead/main session.
 * - `teammate` — plain conversational text from another agent.
 * - `system`   — a structured control-plane message (shutdown/plan/permission/mode).
 * - `task`     — a work-lifecycle control message (task_assignment / idle_notification).
 * - `tick`     — reserved for timer/heartbeat-injected messages (not produced by
 *                send_message itself; kept for schema parity with consumers).
 */
export type MessageSource = 'user' | 'teammate' | 'system' | 'tick' | 'task';

/**
 * The canonical name of the team lead / main session. Control messages
 * (shutdown/plan/permission responses) are authorized against and routed to
 * this identity. Mirrors Claude Code's `TEAM_LEAD_NAME`.
 */
export const TEAM_LEAD_NAME = 'main';

// --- Typed structured-message protocol -------------------------------------
// A discriminated union of control messages that ride over the same mailbox as
// plain text. Each is validated structurally before routing so a malformed
// control message is rejected rather than silently delivered as opaque text.

export interface ShutdownRequest {
  type: 'shutdown_request';
  reason?: string;
  // Optional on the wire: a model may omit it and send_message auto-generates a
  // structured id (generateRequestId) before queueing so the recipient always
  // receives one to echo back. Mirrors Claude Code's SendMessageTool, which
  // never requires the caller to invent a request id for a *request*.
  requestId?: string;
}
export interface ShutdownResponse {
  type: 'shutdown_response';
  requestId: string;
  approve: boolean;
  reason?: string;
}
export interface PlanApprovalRequest {
  type: 'plan_approval_request';
  // Optional on the wire — auto-generated before queueing when omitted.
  requestId?: string;
  planContent: string;
  planFilePath?: string;
}
export interface PlanApprovalResponse {
  type: 'plan_approval_response';
  requestId: string;
  approve: boolean;
  feedback?: string;
  // The permission mode the approving lead grants the teammate to inherit once
  // its plan is approved (plan→default, else the lead's current mode). Carried on
  // the response so the approved worker can exit plan-mode into the right posture
  // without a second round-trip. Mirrors Claude Code's PlanApprovalResponseMessage
  // `permissionMode` (validated via PermissionModeSchema).
  permissionMode?: PermissionMode;
}
export interface PermissionRequest {
  type: 'permission_request';
  // Optional on the wire — auto-generated before queueing when omitted.
  requestId?: string;
  tool: string;
  description: string;
}
export interface PermissionResponse {
  type: 'permission_response';
  requestId: string;
  approve: boolean;
}
/**
 * A lead assigns a unit of work to a teammate. Routed to the named teammate (not
 * broadcast). `assignedBy` is auto-filled from the trusted sender when omitted so
 * the recipient always knows who assigned it. Mirrors Claude Code's
 * teammateMailbox TaskAssignmentMessage.
 */
export interface TaskAssignment {
  type: 'task_assignment';
  taskId: string;
  subject: string;
  description?: string;
  assignedBy?: string;
}
/**
 * A teammate reports it has gone idle / finished its turn, surfaced to the lead so
 * the lead can reassign work without polling. Mirrors Claude Code's
 * IdleNotificationMessage (idleReason / completedTaskId / completedStatus /
 * failureReason / summary — all optional).
 */
export interface IdleNotification {
  type: 'idle_notification';
  idleReason?: 'available' | 'interrupted' | 'failed';
  summary?: string;
  completedTaskId?: string;
  completedStatus?: 'resolved' | 'blocked' | 'failed';
  failureReason?: string;
}
/**
 * A lead requests a teammate switch to a given permission mode. Lead-only; routed
 * to the named teammate. The mode is validated through the single canonical
 * acceptance boundary (normalizePermissionMode). Mirrors Claude Code's
 * ModeSetRequestMessage / createModeSetRequestMessage.
 */
export interface ModeSetRequest {
  type: 'mode_set_request';
  mode: PermissionMode;
}

export type StructuredMessage =
  | ShutdownRequest
  | ShutdownResponse
  | PlanApprovalRequest
  | PlanApprovalResponse
  | PermissionRequest
  | PermissionResponse
  | TaskAssignment
  | IdleNotification
  | ModeSetRequest;

const STRUCTURED_TYPES = new Set([
  'shutdown_request', 'shutdown_response', 'plan_approval_request',
  'plan_approval_response', 'permission_request', 'permission_response',
  'task_assignment', 'idle_notification', 'mode_set_request',
]);

const IDLE_REASONS = new Set(['available', 'interrupted', 'failed']);
const COMPLETED_STATUSES = new Set(['resolved', 'blocked', 'failed']);

/**
 * Providers sometimes serialize booleans as strings; accept both. The decision
 * key is read as either `approve` (AMI) or `approved` (Claude Code's field name)
 * so a response authored to either convention parses identically.
 */
function coerceApprove(o: Record<string, unknown>): boolean {
  const v = o.approve ?? o.approved;
  return v === true || v === 'true';
}

/**
 * Parse+validate a structured control message from raw content. Returns the
 * typed message, `{ error }` when the discriminator is known but the payload is
 * malformed, or `null` when the content is just plain text.
 */
export function parseStructuredMessage(content: string): StructuredMessage | { error: string } | null {
  let parsed: unknown;
  try { parsed = JSON.parse(content); } catch { return null; }
  if (!parsed || typeof parsed !== 'object') return null;
  const o = parsed as Record<string, unknown>;
  const type = o.type;
  if (typeof type !== 'string' || !STRUCTURED_TYPES.has(type)) return null;
  const requestId = (o.requestId ?? o.request_id);
  // Request-type messages may omit requestId — execute() auto-generates a
  // structured id before queueing (see generateRequestId). Response-type
  // messages MUST echo the id they are answering, so it stays required there.
  const optionalReqId = typeof requestId === 'string' ? requestId : undefined;
  switch (type) {
    case 'shutdown_request':
      return { type, requestId: optionalReqId, reason: typeof o.reason === 'string' ? o.reason : undefined };
    case 'shutdown_response':
      if (typeof requestId !== 'string') return { error: 'shutdown_response requires a requestId' };
      return { type, requestId, approve: coerceApprove(o), reason: typeof o.reason === 'string' ? o.reason : undefined };
    case 'plan_approval_request':
      if (typeof o.planContent !== 'string' || !o.planContent.trim()) return { error: 'plan_approval_request requires planContent' };
      return { type, requestId: optionalReqId, planContent: o.planContent, planFilePath: typeof o.planFilePath === 'string' ? o.planFilePath : undefined };
    case 'plan_approval_response': {
      if (typeof requestId !== 'string') return { error: 'plan_approval_response requires a requestId' };
      let permissionMode: PermissionMode | undefined;
      if (o.permissionMode !== undefined) {
        if (typeof o.permissionMode !== 'string') return { error: 'plan_approval_response permissionMode must be a string' };
        const norm = normalizePermissionMode(o.permissionMode);
        if (!norm) return { error: `plan_approval_response permissionMode must be one of: ${PERMISSION_MODES.join(', ')}` };
        permissionMode = norm;
      }
      return { type, requestId, approve: coerceApprove(o), feedback: typeof o.feedback === 'string' ? o.feedback : undefined, permissionMode };
    }
    case 'permission_request':
      if (typeof o.tool !== 'string') return { error: 'permission_request requires a tool' };
      return { type, requestId: optionalReqId, tool: o.tool, description: typeof o.description === 'string' ? o.description : '' };
    case 'permission_response':
      if (typeof requestId !== 'string') return { error: 'permission_response requires a requestId' };
      return { type, requestId, approve: coerceApprove(o) };
    case 'task_assignment': {
      if (typeof o.taskId !== 'string' || !o.taskId.trim()) return { error: 'task_assignment requires a taskId' };
      if (typeof o.subject !== 'string' || !o.subject.trim()) return { error: 'task_assignment requires a subject' };
      return {
        type,
        taskId: o.taskId,
        subject: o.subject,
        description: typeof o.description === 'string' ? o.description : undefined,
        assignedBy: typeof o.assignedBy === 'string' ? o.assignedBy : undefined,
      };
    }
    case 'idle_notification': {
      // All fields optional (an idle teammate may report nothing but "I'm idle"),
      // but a PRESENT enum field must be valid — a bad value is rejected with a
      // self-correcting message rather than silently coerced.
      if (o.idleReason !== undefined && (typeof o.idleReason !== 'string' || !IDLE_REASONS.has(o.idleReason))) {
        return { error: `idle_notification idleReason must be one of: ${[...IDLE_REASONS].join(', ')}` };
      }
      if (o.completedStatus !== undefined && (typeof o.completedStatus !== 'string' || !COMPLETED_STATUSES.has(o.completedStatus))) {
        return { error: `idle_notification completedStatus must be one of: ${[...COMPLETED_STATUSES].join(', ')}` };
      }
      return {
        type,
        idleReason: o.idleReason as IdleNotification['idleReason'],
        summary: typeof o.summary === 'string' ? o.summary : undefined,
        completedTaskId: typeof o.completedTaskId === 'string' ? o.completedTaskId : undefined,
        completedStatus: o.completedStatus as IdleNotification['completedStatus'],
        failureReason: typeof o.failureReason === 'string' ? o.failureReason : undefined,
      };
    }
    case 'mode_set_request': {
      if (typeof o.mode !== 'string') return { error: 'mode_set_request requires a mode' };
      const norm = normalizePermissionMode(o.mode);
      if (!norm) return { error: `mode_set_request mode must be one of: ${PERMISSION_MODES.join(', ')}` };
      return { type, mode: norm };
    }
    default:
      return null;
  }
}

/**
 * True when raw content is a structured control-plane message (a known protocol
 * discriminator), as opposed to plain conversational text. Mirrors Claude Code's
 * teammateMailbox.isStructuredProtocolMessage: such a message must be ROUTED and
 * rendered concisely, never dumped as raw JSON into the model's context — bundling
 * it as opaque text both leaks control-plane noise into the LLM and bypasses its
 * intended handling. Returns true for both a well-formed protocol message and one
 * whose discriminator is recognized but payload is malformed (parseStructuredMessage
 * returns `{ error }`), since either way it is control-plane, not conversation.
 */
export function isStructuredProtocolMessage(content: string): boolean {
  return parseStructuredMessage(content) !== null;
}

/**
 * A brief, one-line summary of a shutdown message with a distinct format per state
 * (request / approved / rejected), for inbox/queue listings and concise relay into
 * context. Returns null when the content is not a shutdown message. Mirrors Claude
 * Code's ShutdownMessage.getShutdownMessageSummary; `from` (the envelope sender) is
 * woven into the summary the way Claude reads it off the payload.
 */
export function getShutdownMessageSummary(content: string, from?: string): string | null {
  const parsed = parseStructuredMessage(content);
  if (!parsed || 'error' in parsed) return null;
  const who = from ? ` from ${from}` : '';
  if (parsed.type === 'shutdown_request') {
    return `[Shutdown Request${who}]${parsed.reason ? ` ${parsed.reason}` : ''}`;
  }
  if (parsed.type === 'shutdown_response') {
    if (parsed.approve) {
      return `[Shutdown Approved]${from ? ` ${from} is now exiting` : ''}`;
    }
    // Rejection carries the required reason plus the non-fatal fallback boundary
    // ("continue working; request again later") so a requester reading the queue
    // knows the worker declined and how to proceed. Mirrors ShutdownRejectedDisplay.
    const reasonTag = parsed.reason ? `: ${parsed.reason}` : '';
    return `[Shutdown Rejected]${from ? ` ${from}` : ''}${reasonTag} — teammate is continuing to work; you may request shutdown again later`;
  }
  return null;
}

/**
 * A concise, human/model-readable one-line summary of any structured control
 * message, used to relay it into context WITHOUT exposing the raw JSON. Returns
 * null for plain text or a malformed protocol payload (nothing meaningful to
 * summarize). Shutdown messages delegate to getShutdownMessageSummary so the
 * per-state wording stays single-sourced.
 */
export function summarizeStructuredMessage(content: string, from?: string): string | null {
  const m = parseStructuredMessage(content);
  if (!m || 'error' in m) return null;
  switch (m.type) {
    case 'shutdown_request':
    case 'shutdown_response':
      return getShutdownMessageSummary(content, from);
    case 'plan_approval_request':
      return `[Plan Approval Request]${m.planFilePath ? ` (${m.planFilePath})` : ''} — teammate submitted a plan for review`;
    case 'plan_approval_response':
      return m.approve
        ? `[Plan Approved] You can now proceed with implementation${m.permissionMode ? ` (mode: ${m.permissionMode})` : ''}`
        // Default guidance when a plan is rejected with empty/missing feedback, so
        // the teammate always has an actionable next step (mirrors Claude Code's
        // PlanApprovalMessage: `[Plan Rejected] ${feedback || 'Please revise your plan'}`).
        : `[Plan Rejected] ${m.feedback || 'Please revise your plan'} — revise and call ExitPlanMode again`;
    case 'permission_request':
      return `[Permission Request] tool "${m.tool}"${m.description ? `: ${m.description}` : ''}`;
    case 'permission_response':
      return m.approve ? '[Permission Granted]' : '[Permission Denied]';
    case 'task_assignment':
      return `[Task Assigned] #${m.taskId} - ${m.subject}`;
    case 'idle_notification': {
      const parts: string[] = [];
      if (m.completedTaskId) parts.push(`#${m.completedTaskId} ${m.completedStatus ?? 'completed'}`);
      if (m.failureReason) parts.push(`failed: ${m.failureReason}`);
      if (m.summary) parts.push(m.summary);
      const whoIdle = from ? ` ${from}` : '';
      const reason = m.idleReason ? ` (${m.idleReason})` : '';
      return `[Teammate Idle${whoIdle}]${reason}${parts.length ? ` — ${parts.join(' · ')}` : ''}`;
    }
    case 'mode_set_request':
      return `[Mode Set Request]${from ? ` from ${from}` : ''} → ${m.mode}`;
  }
}

// --- Structured request identifiers ----------------------------------------
// A request id is self-describing: `{requestType}-{timestamp}@{agentId}`, e.g.
// `shutdown-1702500000000@researcher`. It encodes who the request is about and
// when it was raised, so a response can be correlated back without a side table
// and a stale/replayed id is detectable. Mirrors Claude Code's utils/agentId.ts
// (generateRequestId / parseRequestId) so ids are interchangeable across the two
// implementations.

/** Format a request id as `{requestType}-{timestamp}@{agentId}`. */
export function generateRequestId(requestType: string, agentId: string): string {
  return `${requestType}-${Date.now()}@${agentId}`;
}

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
function requestOriginTag(requestId: string): string {
  const origin = parseRequestId(requestId);
  return origin ? ` [re: ${origin.requestType} request for ${origin.agentId}]` : '';
}

// --- Recipient address schemes ---------------------------------------------
// A `to` value is normally a bare teammate name, but it may be a scheme-qualified
// cross-session address: `uds:<socket-path>` for a local peer process, or
// `bridge:<session-id>` for a Remote Control peer. A bare absolute path is a
// legacy UDS form. Everything else is an in-session teammate name ('other').
// Mirrors Claude Code's utils/peerAddress.ts parseAddress.

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
 * dropping it. Mirrors Claude Code's udsClient.sendToUdsSocket.
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

const mailboxes = new Map<string, AgentMessage[]>();

// --- Predicate-filtered + blocking consumption (Mailbox parity) -------------
// Mirrors Claude Code's utils/mailbox.ts Mailbox: in addition to the drain-all
// pollMailbox and the non-destructive peek/ack, a caller can SELECT a single
// message by an arbitrary predicate (pollMessage) and AWAIT a matching one that
// has not arrived yet (receiveMessage). A pending receiveMessage registers a
// Waiter; the canonical delivery site (enqueue) scans waiters first and, on a
// match, resolves that promise DIRECTLY — the message is handed off and never
// enqueued (bypassing the queue, so it is not also drained later). This lets a
// requester correlate a specific response (by requestId) without draining
// unrelated traffic.
type MailboxWaiter = { fn: (msg: AgentMessage) => boolean; resolve: (msg: AgentMessage) => void };
const mailboxWaiters = new Map<string, MailboxWaiter[]>();

/**
 * Monotonic per-agent delivery counter. Incremented on EVERY delivery to an
 * agent — whether the message is enqueued or handed straight to a waiter — so a
 * reactive consumer can cheaply detect "something changed" / staleness without
 * diffing the queue. Mirrors Claude Code's Mailbox.revision.
 */
const mailboxRevisions = new Map<string, number>();

/**
 * Monotonic sequence behind every message id. Combined with a base-36 timestamp
 * it yields a short, collision-free handle that is stable for the lifetime of the
 * message. Reset by resetAllMailboxes() so tests start from a known state.
 */
let messageSeq = 0;
function nextMessageId(): string {
  return `m${++messageSeq}-${Date.now().toString(36)}`;
}

/**
 * Hard upper bound on how many undelivered messages a single agent's mailbox may
 * retain. Without a cap a never-draining (crashed/evicted) recipient that keeps
 * being written to grows the buffer without limit — a memory-exhaustion DoS.
 * Mirrors Claude Code's appendCappedMessage (InProcessTeammateTask/types.ts):
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
 * Append to an agent's mailbox, capping retention at MAX_MAILBOX by dropping the
 * oldest entries (drop-oldest, newest-wins) so the buffer cannot grow without
 * bound. The single canonical mailbox-append site — every enqueue path routes
 * through here so the cap is impossible to bypass.
 */
function appendCapped(box: AgentMessage[], msg: AgentMessage): void {
  // Every queued message carries a stable id — the canonical append site assigns
  // one when absent so no enqueue path (direct, structured, or broadcast) can
  // queue an un-ackable message. A broadcast reuses one object across mailboxes;
  // the shared id is fine because acks remove entries per-mailbox by id.
  if (!msg.id) { msg.id = nextMessageId(); }
  box.push(msg);
  if (box.length > MAX_MAILBOX) {
    box.splice(0, box.length - MAX_MAILBOX);
  }
}

/**
 * Eager-delivery notifiers (F3). A running agent registers a callback keyed by
 * its name; send_message fires it synchronously after queueing a message so the
 * agent can react right away (e.g. background its current command and poll its
 * mailbox) instead of waiting until it next happens to check. Pull-based polling
 * via pollMailbox() still works unchanged when no notifier is registered.
 */
const mailboxNotifiers = new Map<string, () => void>();

export function registerMailboxNotifier(agentName: string, notify: () => void): void {
  mailboxNotifiers.set(agentName, notify);
}

export function unregisterMailboxNotifier(agentName: string): void {
  mailboxNotifiers.delete(agentName);
}

/** Fire a registered notifier for an agent, if any. Never throws. */
export function notifyMailbox(agentName: string): void {
  const fn = mailboxNotifiers.get(agentName);
  if (fn) { try { fn(); } catch { /* a notifier must never break delivery */ } }
}

export function getMailbox(agentName: string): AgentMessage[] {
  return mailboxes.get(agentName) || [];
}

export function clearMailbox(agentName: string): void {
  mailboxes.delete(agentName);
}

export function resetAllMailboxes(): void {
  mailboxes.clear();
  mailboxNotifiers.clear();
  mailboxWaiters.clear();
  mailboxRevisions.clear();
  messageSeq = 0;
}

/**
 * Number of messages currently queued for an agent (not counting any handed
 * straight to a waiter). Mirrors Claude Code's Mailbox.length getter.
 */
export function mailboxLength(agentName: string): number {
  return (mailboxes.get(agentName) || []).length;
}

/**
 * Monotonic count of deliveries made to an agent for its entire lifetime,
 * including waiter hand-offs. Starts at 0 for an unknown agent. Mirrors Claude
 * Code's Mailbox.revision getter — a cheap change/staleness signal.
 */
export function mailboxRevision(agentName: string): number {
  return mailboxRevisions.get(agentName) || 0;
}

/**
 * Selectively extract ONE queued message by predicate (default: the first one),
 * removing it and leaving every other message queued. Returns undefined when no
 * queued message matches. Non-blocking. Mirrors Claude Code's Mailbox.poll(fn).
 */
export function pollMessage(
  agentName: string,
  fn: (msg: AgentMessage) => boolean = () => true,
): AgentMessage | undefined {
  const box = mailboxes.get(agentName);
  if (!box || box.length === 0) return undefined;
  const idx = box.findIndex(fn);
  if (idx === -1) return undefined;
  return box.splice(idx, 1)[0];
}

/**
 * Await a message matching the predicate (default: any). If one is already
 * queued it resolves immediately and is removed; otherwise a Waiter is
 * registered and the promise resolves when the next matching message is
 * delivered — handed off directly by enqueue(), bypassing the queue. Mirrors
 * Claude Code's Mailbox.receive(fn), enabling request/response correlation
 * (e.g. await the shutdown_response whose requestId I sent) without draining
 * unrelated mailbox traffic.
 */
export function receiveMessage(
  agentName: string,
  fn: (msg: AgentMessage) => boolean = () => true,
): Promise<AgentMessage> {
  const existing = pollMessage(agentName, fn);
  if (existing) return Promise.resolve(existing);
  return new Promise<AgentMessage>(resolve => {
    const waiters = mailboxWaiters.get(agentName) ?? [];
    waiters.push({ fn, resolve });
    mailboxWaiters.set(agentName, waiters);
  });
}

export function pollMailbox(agentName: string): AgentMessage[] {
  const messages = mailboxes.get(agentName) || [];
  if (messages.length > 0) {
    mailboxes.set(agentName, []);
  }
  return messages;
}

/**
 * Read an agent's pending messages WITHOUT removing them (the "read" half of the
 * crash-safe at-least-once protocol — mirrors Claude Code's readUnreadMessages).
 * Returns a snapshot copy so a caller mutating the returned array cannot corrupt
 * the live mailbox. Because nothing is cleared, a consumer that crashes before it
 * acks (markMailboxDelivered) will see the same messages again on its next peek —
 * at-least-once delivery, never at-most-once loss.
 */
export function peekMailbox(agentName: string): AgentMessage[] {
  return (mailboxes.get(agentName) || []).slice();
}

/**
 * Ack the "mark-as-read" half of the protocol: remove the messages whose ids were
 * successfully processed, leaving every other message queued (an unacked message
 * is requeued automatically — it simply stays). Removal is BY ID, not by index or
 * wholesale clear, so a message enqueued (e.g. via the eager notifier) between a
 * peek and this ack is never silently dropped. Returns the number removed. Mirrors
 * Claude Code's markMessagesAsRead.
 */
export function markMailboxDelivered(agentName: string, ids: string[]): number {
  const box = mailboxes.get(agentName);
  if (!box || box.length === 0 || ids.length === 0) return 0;
  const acked = new Set(ids);
  let removed = 0;
  const kept = box.filter(m => {
    if (m.id !== undefined && acked.has(m.id)) { removed++; return false; }
    return true;
  });
  mailboxes.set(agentName, kept);
  return removed;
}

/**
 * Classify the origin category of a message from its content + sender, for the
 * typed `source` field. A structured control message is 'task' for a work-
 * lifecycle type (task_assignment / idle_notification) and 'system' for every
 * other control type (shutdown/plan/permission/mode); plain text is 'user' when
 * it comes from the lead/main session and 'teammate' otherwise. Mirrors Claude
 * Code's MessageSource discrimination. ('tick' is reserved for timer-injected
 * messages, which do not flow through send_message.)
 */
export function classifyMessageSource(content: string, from: string): MessageSource {
  const parsed = parseStructuredMessage(content);
  if (parsed && !('error' in parsed)) {
    return parsed.type === 'task_assignment' || parsed.type === 'idle_notification'
      ? 'task'
      : 'system';
  }
  return from === TEAM_LEAD_NAME ? 'user' : 'teammate';
}

function enqueue(to: string, msg: AgentMessage): void {
  // Stamp the origin category once, at the canonical delivery site, so every
  // queued OR waiter-handed-off message carries a typed `source` a consumer can
  // route on. Idempotent: a broadcast reuses one object across mailboxes and the
  // classification depends only on content+from, so re-entry is a no-op.
  if (!msg.source) { msg.source = classifyMessageSource(msg.content, msg.from); }
  // Every delivery bumps the recipient's monotonic revision — whether it is
  // handed to a waiter or queued — so a reactive consumer sees the change.
  mailboxRevisions.set(to, (mailboxRevisions.get(to) || 0) + 1);
  // Direct waiter hand-off: if a receiveMessage() predicate matches, resolve it
  // and DO NOT enqueue (bypass the queue so the message is not later re-drained).
  // Ensure the handed-off message still carries a stable id, matching the one
  // appendCapped would have assigned on the queued path.
  const waiters = mailboxWaiters.get(to);
  if (waiters && waiters.length > 0) {
    const idx = waiters.findIndex(w => w.fn(msg));
    if (idx !== -1) {
      const [waiter] = waiters.splice(idx, 1);
      if (!msg.id) { msg.id = nextMessageId(); }
      waiter.resolve(msg);
      notifyMailbox(to);
      return;
    }
  }
  const box = mailboxes.get(to) ?? [];
  appendCapped(box, msg);
  mailboxes.set(to, box);
  notifyMailbox(to);
}

/**
 * A recipient is "reachable" if it is a registered agent, has a live notifier,
 * or already owns a mailbox. Used to warn on a likely-dead/typo recipient.
 */
function isReachable(name: string): boolean {
  if (mailboxNotifiers.has(name)) return true;
  if (mailboxes.has(name)) return true;
  try {
    const { getAgentByName } = require('./task');
    if (getAgentByName(name)) return true;
  } catch { /* task module unavailable */ }
  return false;
}

export const sendMessageTool: ToolDefinition = {
  name: 'send_message',
  // PARITY (additive): declarative per-tool gate for the teammate subsystem.
  isEnabled: () => isTeammatesEnabled(),
  description:
    'Send a message to a named teammate agent, or "*" to broadcast to all (expensive — linear in team size, ' +
    'use only when everyone genuinely needs it).\n' +
    '\n' +
    'Your plain text output is NOT visible to other agents — to communicate with a teammate you MUST call this ' +
    'tool. Messages from teammates are delivered to you automatically; you do not poll an inbox. Refer to ' +
    'teammates by their bare name (no "@", never a UUID). To reply to an incoming message, copy its sender ' +
    '("from"/<peer-message from=...>) value into "to". When relaying a teammate message to the user, do not ' +
    'quote the original — it is already rendered.\n' +
    '\n' +
    'Your sender identity is taken from your trusted agent context, not from the "from" field. Messages queue ' +
    'in the recipient\'s mailbox and drain at its next tool round.\n' +
    '\n' +
    'Protocol responses: if you receive a JSON message with type "shutdown_request" or "plan_approval_request", ' +
    'reply with the matching "_response" type, echoing the request_id and setting approve true/false (a ' +
    'rejection requires a reason/feedback). Approving a shutdown terminates you — do NOT originate a ' +
    'shutdown_request unless explicitly asked to. To submit your own plan for review, send a ' +
    '"plan_approval_request" (with planContent, optional planFilePath) to the reviewer. The protocol also carries ' +
    'permission_request/permission_response for forwarding a worker\'s permission prompt to the lead. ' +
    'The lead can assign work with a "task_assignment" (taskId, subject, description) or ask a teammate to switch ' +
    'permission posture with a "mode_set_request" (mode); a teammate reports it has finished its turn with an ' +
    '"idle_notification" (optional idleReason/completedTaskId/completedStatus/summary) sent to the lead. Approving a ' +
    'plan may carry a "permissionMode" the teammate inherits on exiting plan-mode.',
  inputSchema: {
    type: 'object',
    properties: {
      to: {
        type: 'string',
        description:
          'Bare name of the target teammate, "*" to broadcast to the whole team, ' +
          'or a cross-session peer address: "uds:<socket-path>" for a local peer process ' +
          '(plain text only). No "@" prefix on a bare teammate name.',
      },
      content: {
        type: 'string',
        description: 'The message content to send (plain text, or JSON for a structured control message).',
      },
      message: {
        type: 'string',
        description: 'Alias for content — the message to send.',
      },
      from: {
        type: 'string',
        description: 'Deprecated: sender is derived from your trusted agent context. Only honored for the lead/main session.',
      },
      summary: {
        type: 'string',
        description: 'Short 5-10 word summary shown as a preview in the UI.',
      },
    },
    required: ['to', 'content'],
  },
  isReadOnly: false,
  // PARITY (additive): per-invocation read-only verdict. A plain-text chat message
  // mutates nothing (it just queues text), so it is read-only and auto-allows
  // without a permission prompt; a STRUCTURED control message (shutdown / plan /
  // permission / mode / task) changes a teammate's lifecycle or posture and stays
  // on the permission-asking path. Mirrors Claude's `isReadOnly(input) =>
  // typeof input.message === 'string'` (plain text = read-only), discriminating on
  // AMI's string-encoded content via the single canonical structured-message
  // detector. Consumed by tool-executor through isToolReadOnlyForInput.
  isReadOnlyForInput: (input: Record<string, unknown>): boolean => {
    const c = (input.message as string) ?? (input.content as string);
    if (typeof c !== 'string') return true;
    return !isStructuredProtocolMessage(c);
  },

  async execute(input, context: ToolContext): Promise<ToolResult> {
    let toRaw = input.to as string;
    const content = (input.message as string) ?? (input.content as string);

    if (!toRaw || !toRaw.trim()) {
      return { output: 'Error: "to" must not be empty.', isError: true };
    }
    // Normalize the recipient name: a model frequently emits a padded name
    // (" worker ", "worker\n") copied from prose/UI. Without trimming, the
    // lookup (getAgentByName / notifier / mailbox key) misses the real agent and
    // the message silently lands in a dead " worker " mailbox nobody drains —
    // and " * " is not recognized as broadcast. Trim once, up front, so every
    // downstream resolution, broadcast check, and display uses the clean name.
    // (Mirrors Claude Code normalizing the teammate name before resolution.)
    toRaw = toRaw.trim();
    if (!content || !content.trim()) {
      return { output: 'Error: "content" must not be empty.', isError: true };
    }
    if (content.length > MAX_MESSAGE_CHARS) {
      return {
        output: `Error: message content is too large (${content.length} chars; max ${MAX_MESSAGE_CHARS}). Send a summary or a file reference instead.`,
        isError: true,
      };
    }
    // Resolve the recipient address scheme. A bare name / "*" is an in-session
    // teammate ('other'); "uds:<socket-path>" / "bridge:<session-id>" (and a
    // legacy bare absolute socket path) are cross-session peers delivered
    // out-of-process. (Mirrors Claude Code's parseAddress.)
    const addr = parseAddress(toRaw);
    if (addr.scheme !== 'other' && !addr.target.trim()) {
      return { output: 'Error: address target must not be empty.', isError: true };
    }
    // Bare teammate names only — there is one team per session, so "@" or
    // "team@host" style addresses are rejected (mirrors Claude's validateInput).
    // Scheme-qualified addresses are exempt: they use the ":" separator, not "@".
    if (addr.scheme === 'other' && toRaw.includes('@')) {
      return { output: 'Error: "to" must be a bare teammate name or "*" — do not use "@".', isError: true };
    }

    // Anti-impersonation: when we run inside a sub-agent, the sender is the
    // agent's trusted identity (derived from context.agentId), NOT whatever the
    // model put in `from`. Only the lead/main session (no agentId) may set from.
    let trustedSelf: string | undefined;
    if (context.agentId) {
      try {
        const { getAgentNameById } = require('./task');
        trustedSelf = getAgentNameById(context.agentId) || context.agentId;
      } catch { trustedSelf = context.agentId; }
    }
    const from = trustedSelf ?? ((input.from as string) || TEAM_LEAD_NAME);
    const isSenderLead = !context.agentId || from === TEAM_LEAD_NAME;

    const { getAgentByName } = (() => {
      try { return require('./task'); } catch { return { getAgentByName: () => undefined }; }
    })();
    const to = getAgentByName(toRaw) || toRaw;

    // --- Cross-session peers (uds:/bridge:) ------------------------------
    // Delivered OUT of the in-session mailbox: only plain text crosses a session
    // boundary (structured control messages are session-local), and the in-session
    // self-send/terminal/roster guards below do not apply to a remote peer.
    if (addr.scheme !== 'other') {
      if (parseStructuredMessage(content) !== null) {
        return { output: 'Error: structured messages cannot be sent cross-session — only plain text.', isError: true };
      }
      if (addr.scheme === 'bridge') {
        return {
          output: `Error: cross-session bridge delivery is not available in this build — use a teammate name or a "uds:" peer (target: ${addr.target}).`,
          isError: true,
        };
      }
      const summaryXS = input.summary as string | undefined;
      try {
        await sendToUdsSocket(
          addr.target,
          JSON.stringify({ from, text: content, summary: summaryXS || undefined, timestamp: Date.now() }),
        );
        const sTag = summaryXS ? ` — "${summaryXS}"` : '';
        return { output: `Message delivered to local peer "${toRaw}"${sTag} [from: ${from}] (${content.length} chars)` };
      } catch (e) {
        return { output: `Error: failed to deliver to local peer "${toRaw}": ${(e as Error).message}`, isError: true };
      }
    }

    // Self-send guard: addressing your own identity is a no-op that would echo a
    // message into your own mailbox (and, for a sub-agent with a live notifier,
    // risk a self-notify/poll loop). Reject it. The sender may be addressed by
    // its trusted name OR its raw agentId, and the recipient resolves to either
    // form, so compare the target against the FULL self-identity set rather than
    // a single string. Broadcast ("*") is never a self-send. (Mirrors Claude
    // Code refusing to deliver a teammate message back to its own author.)
    if (toRaw !== '*') {
      const selfIds = new Set<string>([from]);
      if (context.agentId) selfIds.add(context.agentId);
      if (trustedSelf) selfIds.add(trustedSelf);
      if (selfIds.has(toRaw) || selfIds.has(to)) {
        return {
          output: `Error: cannot send a message to yourself ("${toRaw}"). Address a teammate by name or use "*" to broadcast.`,
          isError: true,
        };
      }
    }

    // Terminal-status guard: never inject a message into a dead teammate. When the
    // target resolves to a tracked task (by name or taskId) that has already
    // completed/failed/been killed, refuse rather than queue into a mailbox no
    // running engine will ever drain. An untracked name is left to the
    // unreachable-warning path below (it may still be coming online).
    if (toRaw !== '*' && context.processManager) {
      const targetTaskId = getAgentByName(toRaw) || toRaw;
      const status = context.processManager.getStatus(targetTaskId);
      if (isTerminalTaskStatus(status)) {
        return {
          output: `Error: cannot send to "${toRaw}" — that agent has ${status} and is no longer running.`,
          isError: true,
        };
      }
    }

    // --- Structured control messages -------------------------------------
    const structured = parseStructuredMessage(content);
    if (structured && 'error' in structured) {
      return { output: `Error: ${structured.error}`, isError: true };
    }
    if (structured) {
      if (toRaw === '*') {
        return { output: 'Error: structured messages cannot be broadcast (to: "*").', isError: true };
      }
      switch (structured.type) {
        case 'shutdown_request': {
          // Auto-generate a structured request id when the model omits one, and
          // rewrite the queued payload so the recipient always receives an id to
          // echo back in its shutdown_response (mirrors Claude's generateRequestId).
          const reqId = structured.requestId || generateRequestId('shutdown', toRaw);
          const wire = structured.requestId ? content : JSON.stringify({ ...structured, requestId: reqId });
          enqueue(to, { from, to, content: wire, timestamp: Date.now() });
          return { output: `Shutdown request sent to ${toRaw} (id: ${reqId}) [from: ${from}]` };
        }
        case 'shutdown_response': {
          // A shutdown decision is always reported back to the lead.
          if (toRaw !== TEAM_LEAD_NAME) {
            return { output: `Error: shutdown_response must be sent to "${TEAM_LEAD_NAME}".`, isError: true };
          }
          if (!structured.approve && (!structured.reason || !structured.reason.trim())) {
            return { output: 'Error: a reason is required when rejecting a shutdown request.', isError: true };
          }
          const originTag = requestOriginTag(structured.requestId);
          enqueue(TEAM_LEAD_NAME, { from, to: TEAM_LEAD_NAME, content, timestamp: Date.now() });
          if (structured.approve) {
            // Approving a shutdown TERMINATES the approving teammate: abort its own
            // task so the engine loop actually stops, matching Claude Code's
            // handleShutdownApproval (abortController.abort()). The confirmation is
            // already queued to the lead above; the notifier lets a listening engine
            // drain it before the abort unwinds.
            notifyMailbox(from);
            let aborted = false;
            if (context.agentId && context.processManager) {
              aborted = context.processManager.kill(context.agentId);
            }
            return {
              output: aborted
                ? `Shutdown approved (id: ${structured.requestId})${originTag} — ${from} aborted; confirmation sent to ${TEAM_LEAD_NAME}.`
                : `Shutdown approved (id: ${structured.requestId})${originTag} — confirmation sent to ${TEAM_LEAD_NAME}; ${from} is exiting.`,
            };
          }
          return { output: `Shutdown rejected (id: ${structured.requestId})${originTag}: "${structured.reason}" — ${from} continues working.` };
        }
        case 'plan_approval_request': {
          // The request half: a teammate submits a plan to another agent (usually
          // the lead) for approval. One-way queue like shutdown_request; the
          // reviewer replies with a plan_approval_response.
          const reqId = structured.requestId || generateRequestId('plan', toRaw);
          const wire = structured.requestId ? content : JSON.stringify({ ...structured, requestId: reqId });
          enqueue(to, { from, to, content: wire, timestamp: Date.now() });
          const fileTag = structured.planFilePath ? ` (${structured.planFilePath})` : '';
          return { output: `Plan submitted to ${toRaw} for approval (id: ${reqId})${fileTag} [from: ${from}]` };
        }
        case 'plan_approval_response': {
          // Authorization: only the team lead may approve or reject plans.
          if (!isSenderLead) {
            return { output: 'Error: only the team lead can approve or reject plans.', isError: true };
          }
          const originTag = requestOriginTag(structured.requestId);
          enqueue(to, { from, to, content, timestamp: Date.now() });
          if (structured.approve) {
            // The granted permission mode (if any) is surfaced so the lead sees
            // what posture the teammate will inherit on exiting plan-mode.
            const modeTag = structured.permissionMode ? ` (mode: ${structured.permissionMode})` : '';
            return { output: `Plan approved for ${toRaw} (id: ${structured.requestId})${originTag}${modeTag} [from: ${from}]` };
          }
          // Rejection always carries actionable guidance, defaulting when the lead
          // gave no explicit feedback (mirrors Claude's 'Please revise your plan').
          const guidance = structured.feedback || 'Please revise your plan';
          return { output: `Plan rejected for ${toRaw} (id: ${structured.requestId})${originTag} [from: ${from}] — ${guidance}; revise and call ExitPlanMode again` };
        }
        case 'task_assignment': {
          // A lead assigns work to a named teammate. assignedBy is stamped from the
          // trusted sender (not model-supplied) so provenance cannot be spoofed.
          const assignedBy = from;
          const wire = JSON.stringify({ ...structured, assignedBy, description: structured.description ?? '' });
          enqueue(to, { from, to, content: wire, timestamp: Date.now() });
          return { output: `Task #${structured.taskId} "${structured.subject}" assigned to ${toRaw} [from: ${from}]` };
        }
        case 'idle_notification': {
          // A teammate reports it is idle; always surfaced to the lead so it can
          // reassign work. Routed to the canonical lead mailbox.
          if (toRaw !== TEAM_LEAD_NAME) {
            return { output: `Error: idle_notification must be sent to "${TEAM_LEAD_NAME}".`, isError: true };
          }
          enqueue(TEAM_LEAD_NAME, { from, to: TEAM_LEAD_NAME, content, timestamp: Date.now() });
          const reasonTag = structured.idleReason ? ` (${structured.idleReason})` : '';
          return { output: `Idle notification sent to ${TEAM_LEAD_NAME}${reasonTag} [from: ${from}]` };
        }
        case 'mode_set_request': {
          // Only the lead may change a teammate's permission mode. Routed to the
          // named teammate, which applies it on drain.
          if (!isSenderLead) {
            return { output: 'Error: only the team lead can set a teammate permission mode.', isError: true };
          }
          enqueue(to, { from, to, content, timestamp: Date.now() });
          return { output: `Permission mode "${structured.mode}" requested for ${toRaw} [from: ${from}]` };
        }
        case 'permission_request': {
          // A worker forwards a permission prompt up to the lead for a decision.
          // The request id is keyed on the requester (from): the lead's response
          // routes back to that worker.
          const reqId = structured.requestId || generateRequestId('permission', from);
          const wire = structured.requestId ? content : JSON.stringify({ ...structured, requestId: reqId });
          enqueue(TEAM_LEAD_NAME, { from, to: TEAM_LEAD_NAME, content: wire, timestamp: Date.now() });
          return { output: `Permission request for "${structured.tool}" forwarded to ${TEAM_LEAD_NAME} (id: ${reqId}) [from: ${from}]` };
        }
        case 'permission_response': {
          // Only the lead may answer a forwarded permission request.
          if (!isSenderLead) {
            return { output: 'Error: only the team lead can answer a permission request.', isError: true };
          }
          const originTag = requestOriginTag(structured.requestId);
          enqueue(to, { from, to, content, timestamp: Date.now() });
          return { output: `Permission ${structured.approve ? 'granted' : 'denied'} to ${toRaw} (id: ${structured.requestId})${originTag}` };
        }
      }
    }

    // --- Plain text ------------------------------------------------------
    const summary = input.summary as string | undefined;
    const msg: AgentMessage = {
      from,
      to,
      content,
      summary: summary || undefined,
      timestamp: Date.now(),
    };

    if (toRaw === '*') {
      // Broadcast to the full LIVE roster, not just agents that already own a
      // mailbox: a teammate that registered an eager-delivery notifier on spawn
      // but has not yet been messaged must still receive a broadcast (mirrors
      // Claude enumerating the team-file members, reaching not-yet-contacted
      // teammates). Union mailbox keys with notifier-registered names, dedup, and
      // exclude the sender case-insensitively.
      const fromLc = from.toLowerCase();
      const roster = new Set<string>();
      for (const name of mailboxes.keys()) roster.add(name);
      for (const name of mailboxNotifiers.keys()) roster.add(name);
      const recipients: string[] = [];
      for (const name of roster) {
        if (name.toLowerCase() === fromLc) continue;
        // Route each fan-out delivery through the canonical enqueue primitive so
        // a broadcast also bumps the per-recipient revision and resolves any
        // pending receiveMessage() waiter — identical delivery semantics to a DM.
        enqueue(name, msg);
        recipients.push(name);
      }
      if (recipients.length === 0) {
        return { output: 'No teammates to broadcast to (no other agents are reachable).' };
      }
      return { output: `Broadcast sent to ${recipients.length} agent(s): ${recipients.join(', ')}` };
    }

    // Dead-recipient boundary: if other agents are live this session but the
    // target isn't reachable, it's almost certainly a typo/stale name. Warn
    // (still queued, in case it comes online) rather than fail silently.
    // Computed BEFORE enqueue, which would otherwise create the mailbox.
    const unreachableWarn = (!isReachable(to) && mailboxNotifiers.size > 0)
      ? ` [warning: "${toRaw}" is not a known/live agent — check the name]`
      : '';

    enqueue(to, msg);

    const summaryTag = summary ? ` — "${summary}"` : '';
    return {
      output: `Message sent to "${toRaw}"${summaryTag} [from: ${from}] (${content.length} chars)${unreachableWarn}`,
    };
  },
};
