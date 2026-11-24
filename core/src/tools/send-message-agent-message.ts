// Split from send-message.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { PermissionMode } from '../permissions';
import { log as coreLog } from '../logger';

// --- Gated lifecycle diagnostic trace ---------------------------------------
// The teammate mailbox is a multi-agent coordination surface where "my message
// never arrived" / "the shutdown wasn't re-sent" failures are otherwise invisible.
// Mirrors the reference's InProcessBackend `logForDebugging('[InProcessBackend] ...')`
// opt-in lifecycle trace (utils/swarm/backends/InProcessBackend.ts): every delivery
// and handshake transition emits one terse, prefixed line through AMI's CANONICAL
// logger (reused — not a second logging facility, per the no-duplication rule) at
// VERBOSE level, so it is OFF by default (the logger's default min level is 'debug')
// and only surfaces when a developer/CI sets AMI_DEBUG_LOG_LEVEL=verbose (optionally
// narrowed with AMI_DEBUG_COMPONENTS=teammate-mailbox). No-op cost beyond the
// logger's leading level check when disabled.
export const MAILBOX_TRACE_COMPONENT = 'teammate-mailbox';

/**
 * Emit one gated, verbose lifecycle-trace line for a mailbox transition. Always
 * routes through the canonical {@link coreLog} (which performs the level/component
 * gating); callers never guard it, so each transition is a single terse statement.
 */
export function traceMailbox(event: string, data?: Record<string, unknown>): void {
  coreLog(MAILBOX_TRACE_COMPONENT, event, data, 'verbose');
}

export interface AgentMessage {
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
   * content. Mirrors the reference's mailbox `Message.source` (MessageSource). Set
   * automatically — a caller never supplies it.
   */
  source?: MessageSource;
  /**
   * The sender's assigned display color, resolved from the trusted team roster at
   * the single canonical delivery site (enqueue) — NOT model-supplied. Lets a
   * consumer render an incoming teammate/peer message in that teammate's own color
   * so concurrent agents stay visually distinguishable. Mirrors the reference's
   * per-message `color` (teammateMailbox TeammateMessage.color / UserTeammateMessage),
   * sourced from the same round-robin palette team.ts assigns on spawn. Undefined
   * when the sender is not a colored roster member (e.g. the bare lead session).
   */
  color?: string;
  /**
   * Urgency discriminator for a plain-text message: `proactive` marks an unsolicited
   * surfacing the recipient needs to see NOW (a blocker, a completion while it was
   * away) rather than a `normal` reply. Mirrors the reference's BriefTool
   * `status: 'normal' | 'proactive'` (tools/BriefTool/BriefTool.ts) — AMI's send_message
   * IS the model→teammate/user channel, so the same discriminator rides here. Stamped
   * from the model-supplied `status` at the single canonical delivery site; absent
   * (treated as `normal`) when the sender does not set it. Consumed by
   * {@link drainMailboxPrioritized} (a proactive peer message drains ahead of ordinary
   * peer chatter so it is not starved) and surfaced on the send result's metadata so a
   * frontend can special-case it (e.g. a notification). Only meaningful on plain text —
   * a structured control message carries its own lifecycle semantics.
   */
  status?: MessageStatus;
  /**
   * Receive-side marker set when a message was delivered from the FILE-BACKED
   * cross-process inbox ({@link ./mailbox-fs}) — i.e. it originated in a SEPARATE
   * OS process / session of the same swarm, not this process's in-memory mailbox.
   * Mirrors the reference wrapping an inbound out-of-session message as
   * `<cross-session-message from=...>` so the recipient knows the message crossed a
   * session boundary. Never persisted (stripped before the message is written to the
   * durable inbox); stamped only on the drained copy a local consumer receives.
   */
  crossSession?: boolean;
}

/**
 * Urgency discriminator for a plain-text teammate/user message. Mirrors the reference
 * Code's BriefTool status enum:
 * - `normal`    — an ordinary reply to something just said (the default).
 * - `proactive` — an unsolicited surfacing the recipient needs to see now (a
 *                 blocker, a completion while it was away, an unprompted update).
 */
export const MESSAGE_STATUS = ['normal', 'proactive'] as const;

export type MessageStatus = (typeof MESSAGE_STATUS)[number];

/**
 * The origin category of a mailbox message. Mirrors the reference's
 * utils/mailbox.ts `MessageSource`:
 * - `user`     — authored by the user-facing lead/main session.
 * - `teammate` — plain conversational text from another agent.
 * - `system`   — a structured control-plane message (shutdown/plan/permission/mode).
 * - `task`     — a work-lifecycle control message (task_assignment / idle_notification).
 * - `tick`     — reserved for timer/heartbeat-injected messages (not produced by
 *                send_message itself; kept for schema parity with consumers).
 */
export const MESSAGE_SOURCES = ['user', 'teammate', 'system', 'tick', 'task'] as const;

export type MessageSource = (typeof MESSAGE_SOURCES)[number];

/**
 * The canonical name of the team lead / main session. Control messages
 * (shutdown/plan/permission responses) are authorized against and routed to
 * this identity. Mirrors the reference's `TEAM_LEAD_NAME`.
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
  // receives one to echo back. Mirrors the reference's SendMessageTool, which
  // never requires the caller to invent a request id for a *request*.
  requestId?: string;
  // The requesting agent's identity, stamped from the trusted sender at send
  // time so the shutdown payload is self-contained (the recipient knows who
  // asked without reading the envelope). Mirrors the reference's
  // ShutdownRequestMessage `from` field (createShutdownRequestMessage).
  from?: string;
}

export interface ShutdownResponse {
  type: 'shutdown_response';
  requestId: string;
  approve: boolean;
  reason?: string;
  // Teardown-routing hints echoed back to the lead so it can tear down the right
  // backend/pane for the exiting teammate without a lookup. Both optional (an
  // in-process teammate has neither). Mirrors the reference's
  // ShutdownApprovedMessage `paneId` / `backendType`.
  paneId?: string;
  backendType?: string;
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
  // without a second round-trip. Mirrors the reference's PlanApprovalResponseMessage
  // `permissionMode` (validated via PermissionModeSchema).
  permissionMode?: PermissionMode;
}

export interface PermissionRequest {
  type: 'permission_request';
  // Optional on the wire — auto-generated before queueing when omitted.
  requestId?: string;
  tool: string;
  description: string;
  /**
   * The tool_use id of the worker's pending call the lead is being asked to
   * approve. Lets the resolver correlate the decision back to the exact blocked
   * invocation (not just the worker). Mirrors the reference's SwarmPermissionRequest
   * `toolUseId` (utils/swarm/permissionSync.ts). Optional — a forward that predates
   * a concrete tool_use id omits it.
   */
  toolUseId?: string;
  /**
   * The arguments of the worker's pending tool call, so the lead can inspect WHAT
   * the worker wants to run (e.g. the exact bash command / file path) before
   * approving, and optionally return an edited `updatedInput`. Mirrors the reference
   * Code's SwarmPermissionRequest `input`. Must be a plain object; a non-object is
   * dropped (the request still forwards with its tool/description).
   */
  input?: Record<string, unknown>;
}

export interface PermissionResponse {
  type: 'permission_response';
  requestId: string;
  approve: boolean;
  /**
   * Human-readable rejection reason the resolver attaches when denying, carried
   * back to the worker so it understands WHY the request was refused (not just
   * that it was). Mirrors the reference's SwarmPermissionResolution `feedback`
   * (utils/swarm/permissionSync.ts) round-tripped via pollForResponse.
   */
  feedback?: string;
  /**
   * The resolver's edited tool input: a leader may approve a worker's tool call
   * but modify its arguments first (e.g. tighten a glob, redact a flag) and the
   * worker must run the EDITED input, not its original. Carried on the response so
   * the worker's permission path can substitute it. Mirrors the reference's
   * SwarmPermissionResolution `updatedInput`.
   */
  updatedInput?: Record<string, unknown>;
  /**
   * Who resolved the forwarded request: the team `leader` (the normal path — a
   * worker forwarded its prompt up and the lead decided) or the `worker` itself
   * (it un-blocked locally, e.g. a timeout/self-approval fallback). Lets a consumer
   * attribute the decision. Mirrors the reference's SwarmPermissionResolution
   * `resolvedBy` enum. Optional; absent defaults to a leader-originated decision.
   */
  resolvedBy?: 'leader' | 'worker';
}

/**
 * A lifecycle notice that a teammate has terminated (exited or was force-killed),
 * surfaced so a consumer can release the dead agent's work and stop routing to it.
 * `message` is an optional human-readable note. Mirrors the reference's
 * teammate_terminated mailbox message (killTeammate injects it into the lead's
 * inbox; UserTeammateMessage filters it out of the model-facing render — it is a
 * silent control-plane signal, never conversational chatter).
 */
export interface TeammateTerminated {
  type: 'teammate_terminated';
  message?: string;
}

/**
 * A worker asks the lead to approve network egress to a specific host that the
 * sandbox blocked. Routed to the lead (like permission_request). Mirrors the reference
 * Code's SandboxPermissionRequestMessage — AMI flattens the reference's `hostPattern`
 * to a bare `host` and derives the worker identity from the trusted sender
 * rather than carrying workerId/workerName on the wire.
 */
export interface SandboxPermissionRequest {
  type: 'sandbox_permission_request';
  // Optional on the wire — auto-generated before queueing when omitted.
  requestId?: string;
  host: string;
}

/**
 * The lead's decision on a worker's blocked-host egress request, routed back to
 * the named worker. Mirrors the reference's SandboxPermissionResponseMessage
 * (requestId / host / allow).
 */
export interface SandboxPermissionResponse {
  type: 'sandbox_permission_response';
  requestId: string;
  host: string;
  allow: boolean;
}

/**
 * The lead broadcasts (or directs) a permission-rule change to teammates so the
 * whole team adopts an "always allow/deny/ask" decision without re-prompting
 * each worker. Lead-only. Mirrors the reference's TeamPermissionUpdateMessage
 * (permissionUpdate.rules/behavior + directoryPath + toolName), flattened to the
 * fields AMI's permission layer consumes.
 */
export interface TeamPermissionUpdate {
  type: 'team_permission_update';
  toolName: string;
  behavior: 'allow' | 'deny' | 'ask';
  rule?: string;
  directoryPath?: string;
}

/**
 * A lead assigns a unit of work to a teammate. Routed to the named teammate (not
 * broadcast). `assignedBy` is auto-filled from the trusted sender when omitted so
 * the recipient always knows who assigned it. Mirrors the reference's
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
 * the lead can reassign work without polling. Mirrors the reference's
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
 * acceptance boundary (normalizePermissionMode). Mirrors the reference's
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
  | SandboxPermissionRequest
  | SandboxPermissionResponse
  | TeamPermissionUpdate
  | TaskAssignment
  | IdleNotification
  | ModeSetRequest
  | TeammateTerminated;

/**
 * Providers sometimes serialize booleans as strings; accept both. The decision
 * key is read as either `approve` (AMI) or `approved` (the reference's field name)
 * so a response authored to either convention parses identically.
 */
export function coerceApprove(o: Record<string, unknown>): boolean {
  const v = o.approve ?? o.approved;
  return v === true || v === 'true';
}

/**
 * Upper bound (chars) on the compact one-line projection of a permission request's
 * pending tool `input`, so a large args blob never bloats the leader-facing summary.
 */
export const MAX_INPUT_PREVIEW_CHARS = 120;

/**
 * Compact one-line, length-bounded projection of a tool-call input object for the
 * leader-facing permission-request summary. Collapses whitespace in the JSON and
 * caps it at {@link MAX_INPUT_PREVIEW_CHARS} (with an ellipsis) so the lead sees the
 * shape of the pending args without a multi-line blob. Falls back to '{…}' if the
 * object cannot be stringified (e.g. a cyclic structure).
 */
export function compactInput(input: Record<string, unknown>): string {
  let s: string;
  try { s = JSON.stringify(input); } catch { return '{…}'; }
  s = s.replace(/\s+/g, ' ').trim();
  return s.length > MAX_INPUT_PREVIEW_CHARS ? s.slice(0, MAX_INPUT_PREVIEW_CHARS - 1) + '…' : s;
}

// --- Receive-side relay router ----------------------------------------------
// A consumer draining a mailbox must NOT feed every message verbatim into the
// model's context. the reference runs an ordered router over each incoming teammate
// message (components/messages/UserTeammateMessage.tsx L62-136): it pre-filters
// exit/lifecycle signals (shutdown-approved, teammate_terminated), silently drops
// `idle_notification`, renders a completed task as a terse `✓ Completed task #id`
// line, and summarizes every other structured control message — only plain text
// is relayed verbatim. This is the single canonical receive-side equivalent.

/**
 * The verdict for relaying ONE incoming mailbox message into a consumer's
 * context, produced by the canonical receive-side router {@link relayIncomingMessage}.
 * `drop: true` marks a lifecycle control signal that is processed silently and
 * MUST NOT be surfaced to the model; otherwise `text` is the concise body to relay.
 */
export interface IncomingRelay {
  drop: boolean;
  text?: string;
}

/**
 * Terse one-line completion marker for a finished task, mirroring the reference's
 * `✓ Completed task #id (subject)` render (UserTeammateMessage L117-136). The
 * subject is parenthesized only when present.
 */
export function formatTaskCompleted(taskId: string | undefined, subject?: string): string {
  const subj = subject ? ` (${subject})` : '';
  return `✓ Completed task #${taskId ?? '?'}${subj}`;
}

// --- Structured request identifiers ----------------------------------------
// A request id is self-describing: `{requestType}-{timestamp}@{agentId}`, e.g.
// `shutdown-1702500000000@researcher`. It encodes who the request is about and
// when it was raised, so a response can be correlated back without a side table
// and a stale/replayed id is detectable. Mirrors the reference's utils/agentId.ts
// (generateRequestId / parseRequestId) so ids are interchangeable across the two
// implementations.

/** Format a request id as `{requestType}-{timestamp}@{agentId}`. */
export function generateRequestId(requestType: string, agentId: string): string {
  return `${requestType}-${Date.now()}@${agentId}`;
}
