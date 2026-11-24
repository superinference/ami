import { ToolDefinition, ToolResult, ToolContext, DeliveryHold } from '../types';
import { isTerminalTaskStatus } from '../process-manager';
import { normalizePermissionMode, PermissionMode, PERMISSION_MODES } from '../permissions';
import { isTeammatesEnabled } from './tool-gates';
import { processSandboxPermissionResponse } from './sandbox-permission-registry';
import '../logger';
import { TEAM_LEAD_NAME, coerceApprove, compactInput, formatTaskCompleted, generateRequestId, traceMailbox } from './send-message-agent-message';
import type { AgentMessage, IdleNotification, IncomingRelay, MessageSource, MessageStatus, StructuredMessage, TeamPermissionUpdate, TeammateTerminated } from './send-message-agent-message';
import { MAX_MAILBOX, MAX_MESSAGE_CHARS, MAX_OUTBOUND_DM_HISTORY, coerceMessageContent, deriveSummary, fsMailboxEnabled, parseAddress, requestCorrelation, requestOriginTag, responseCorrelation, sendToUdsSocket, validationError } from './send-message-send-to-uds-socket';
import type { DeliveryWarning, DirectMessageResult, MailboxWaiter, OutboundDm } from './send-message-send-to-uds-socket';
export { MAX_INPUT_PREVIEW_CHARS, MESSAGE_SOURCES, MESSAGE_STATUS, TEAM_LEAD_NAME, compactInput, generateRequestId } from './send-message-agent-message';
export type { AgentMessage, IdleNotification, IncomingRelay, MessageSource, MessageStatus, ModeSetRequest, PermissionRequest, PermissionResponse, PlanApprovalRequest, PlanApprovalResponse, SandboxPermissionRequest, SandboxPermissionResponse, ShutdownRequest, ShutdownResponse, StructuredMessage, TaskAssignment, TeamPermissionUpdate, TeammateTerminated } from './send-message-agent-message';
export { DELIVERY_WARNINGS, MAX_MAILBOX, MAX_MESSAGE_CHARS, MAX_OUTBOUND_DM_HISTORY, MAX_SUMMARY_PREVIEW_CHARS, SEND_MESSAGE_VALIDATION_ERROR_CODE, coerceMessageContent, deriveSummary, describeDeliveryHold, parseAddress, parseRequestId, sendToUdsSocket, shouldSuppressSendMessageTranscript } from './send-message-send-to-uds-socket';
export type { DeliveryWarning, DirectMessageResult, PeerAddress, SendMessageCorrelation } from './send-message-send-to-uds-socket';

/** The accepted {@link MessageStatus} values, for input validation. */
const MESSAGE_STATUSES = new Set<MessageStatus>(['normal', 'proactive']);

const STRUCTURED_TYPES = new Set([
  'shutdown_request', 'shutdown_response', 'plan_approval_request',
  'plan_approval_response', 'permission_request', 'permission_response',
  'sandbox_permission_request', 'sandbox_permission_response',
  'team_permission_update',
  'task_assignment', 'idle_notification', 'mode_set_request',
  'teammate_terminated',
]);

const IDLE_REASONS = new Set(['available', 'interrupted', 'failed']);
const COMPLETED_STATUSES = new Set(['resolved', 'blocked', 'failed']);
const PERMISSION_BEHAVIORS = new Set(['allow', 'deny', 'ask']);

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
      return {
        type,
        requestId: optionalReqId,
        reason: typeof o.reason === 'string' ? o.reason : undefined,
        from: typeof o.from === 'string' ? o.from : undefined,
      };
    case 'shutdown_response':
      if (typeof requestId !== 'string') return { error: 'shutdown_response requires a requestId' };
      return {
        type,
        requestId,
        approve: coerceApprove(o),
        reason: typeof o.reason === 'string' ? o.reason : undefined,
        paneId: typeof o.paneId === 'string' ? o.paneId : undefined,
        backendType: typeof o.backendType === 'string' ? o.backendType : undefined,
      };
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
    case 'permission_request': {
      if (typeof o.tool !== 'string') return { error: 'permission_request requires a tool' };
      // The pending tool's arguments the lead is asked to approve — a plain object
      // only (a string/array/primitive is dropped so a malformed `input` can't be
      // forwarded as the args a worker would run).
      const reqInput = (o.input && typeof o.input === 'object' && !Array.isArray(o.input))
        ? (o.input as Record<string, unknown>)
        : undefined;
      return {
        type,
        requestId: optionalReqId,
        tool: o.tool,
        description: typeof o.description === 'string' ? o.description : '',
        toolUseId: typeof o.toolUseId === 'string' ? o.toolUseId : undefined,
        input: reqInput,
      };
    }
    case 'permission_response': {
      if (typeof requestId !== 'string') return { error: 'permission_response requires a requestId' };
      // A resolver-edited tool input must be a plain object (never a string/array/
      // primitive) so a malformed `updatedInput` can't be forwarded as the input a
      // worker runs. A bad shape is ignored rather than fatal (the approve/deny
      // decision still stands).
      const updatedInput = (o.updatedInput && typeof o.updatedInput === 'object' && !Array.isArray(o.updatedInput))
        ? (o.updatedInput as Record<string, unknown>)
        : undefined;
      // Who resolved it: only the explicit enum values are honored; anything else
      // (incl. absent) leaves it undefined → treated as a leader decision.
      const resolvedBy = (o.resolvedBy === 'leader' || o.resolvedBy === 'worker')
        ? o.resolvedBy
        : undefined;
      return {
        type,
        requestId,
        approve: coerceApprove(o),
        feedback: typeof o.feedback === 'string' ? o.feedback : undefined,
        updatedInput,
        resolvedBy,
      };
    }
    case 'sandbox_permission_request': {
      // Accept both AMI's flat `host` and the reference's nested `hostPattern.host` so a
      // cross-framework payload parses identically.
      const hostPattern = (o.hostPattern && typeof o.hostPattern === 'object')
        ? (o.hostPattern as Record<string, unknown>).host
        : undefined;
      const host = typeof o.host === 'string' ? o.host : (typeof hostPattern === 'string' ? hostPattern : undefined);
      if (!host || !host.trim()) return { error: 'sandbox_permission_request requires a host' };
      return { type, requestId: optionalReqId, host };
    }
    case 'sandbox_permission_response': {
      if (typeof requestId !== 'string') return { error: 'sandbox_permission_response requires a requestId' };
      if (typeof o.host !== 'string' || !o.host.trim()) return { error: 'sandbox_permission_response requires a host' };
      // `allow` is the decision key; also accept the reference's `approved` field for
      // cross-framework compatibility. Either may arrive as a boolean or string.
      const allow = o.allow === true || o.allow === 'true' || o.approved === true || o.approved === 'true';
      return { type, requestId, host: o.host, allow };
    }
    case 'team_permission_update': {
      // Accept both AMI's flat shape and the reference's nested permissionUpdate.{rules,behavior}.
      const pu = (o.permissionUpdate && typeof o.permissionUpdate === 'object')
        ? (o.permissionUpdate as Record<string, unknown>)
        : undefined;
      let toolName = typeof o.toolName === 'string' ? o.toolName : undefined;
      let rule = typeof o.rule === 'string' ? o.rule : (typeof o.ruleContent === 'string' ? o.ruleContent : undefined);
      if (pu && Array.isArray(pu.rules) && pu.rules.length > 0) {
        const first = pu.rules[0] as Record<string, unknown>;
        if (!toolName && typeof first.toolName === 'string') toolName = first.toolName;
        if (!rule && typeof first.ruleContent === 'string') rule = first.ruleContent;
      }
      const behavior = typeof o.behavior === 'string' ? o.behavior : (pu && typeof pu.behavior === 'string' ? pu.behavior : undefined);
      if (!toolName || !toolName.trim()) return { error: 'team_permission_update requires a toolName' };
      if (!behavior || !PERMISSION_BEHAVIORS.has(behavior)) {
        return { error: `team_permission_update behavior must be one of: ${[...PERMISSION_BEHAVIORS].join(', ')}` };
      }
      return {
        type,
        toolName,
        behavior: behavior as TeamPermissionUpdate['behavior'],
        rule,
        directoryPath: typeof o.directoryPath === 'string' ? o.directoryPath : undefined,
      };
    }
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
    case 'teammate_terminated':
      // A pure lifecycle notice — every field optional (a bare termination signal
      // carries no `message`). `message` is coerced to a string or dropped.
      return { type, message: typeof o.message === 'string' ? o.message : undefined };
    default:
      return null;
  }
}

/**
 * True when raw content is a structured control-plane message (a known protocol
 * discriminator), as opposed to plain conversational text. Mirrors the reference's
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
 * True when raw content is a permission-DECISION message routed back to a worker —
 * either an ordinary tool `permission_response` OR a `sandbox_permission_response`
 * (per-host network egress). Mirrors the reference's inProcessTeammateHelpers
 * `isPermissionRelatedResponse`, which deliberately special-cases BOTH kinds so a
 * worker that forwarded a permission/sandbox prompt to the lead can recognize the
 * reply and unblock. Distinct from {@link isStructuredProtocolMessage} (true for
 * EVERY control type): this picks out only the two response types a blocked worker
 * is waiting on, so the drain can prioritize them (see {@link drainMailboxPrioritized}).
 */
export function isPermissionRelatedResponse(content: string): boolean {
  const m = parseStructuredMessage(content);
  if (!m || 'error' in m) return false;
  return m.type === 'permission_response' || m.type === 'sandbox_permission_response';
}

/**
 * A brief, one-line summary of a shutdown message with a distinct format per state
 * (request / approved / rejected), for inbox/queue listings and concise relay into
 * context. Returns null when the content is not a shutdown message. Mirrors the reference
 * Code's ShutdownMessage.getShutdownMessageSummary; `from` (the envelope sender) is
 * woven into the summary the way the reference reads it off the payload.
 */
export function getShutdownMessageSummary(content: string, from?: string): string | null {
  const parsed = parseStructuredMessage(content);
  if (!parsed || 'error' in parsed) return null;
  if (parsed.type === 'shutdown_request') {
    // Prefer the self-describing payload `from` (set at send time) over the
    // envelope sender so the summary names the requester even when read off a
    // relayed/forwarded copy. Mirrors the reference reading `from` off the payload.
    const requester = parsed.from ?? from;
    const who = requester ? ` from ${requester}` : '';
    return `[Shutdown Request${who}]${parsed.reason ? ` ${parsed.reason}` : ''}`;
  }
  if (parsed.type === 'shutdown_response') {
    if (parsed.approve) {
      // Teardown-routing hint so the lead knows which backend/pane to reclaim.
      const teardown = parsed.backendType || parsed.paneId
        ? ` (teardown: ${[parsed.backendType, parsed.paneId].filter(Boolean).join(' ')})`
        : '';
      return `[Shutdown Approved]${from ? ` ${from} is now exiting` : ''}${teardown}`;
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
        // the teammate always has an actionable next step (mirrors the reference's
        // PlanApprovalMessage: `[Plan Rejected] ${feedback || 'Please revise your plan'}`).
        : `[Plan Rejected] ${m.feedback || 'Please revise your plan'} — revise and call ExitPlanMode again`;
    case 'permission_request': {
      // Surface the pending tool's args (compact one-line projection) so the lead
      // sees WHAT it is approving, not just which tool. Mirrors the reference showing the
      // request `input` in the leader's permission prompt.
      const argTag = m.input ? ` args: ${compactInput(m.input)}` : '';
      return `[Permission Request] tool "${m.tool}"${m.description ? `: ${m.description}` : ''}${argTag}`;
    }
    case 'permission_response': {
      const edited = m.updatedInput ? ' (input edited by approver)' : '';
      const byTag = m.resolvedBy === 'worker' ? ' (resolved by worker)' : '';
      if (m.approve) return `[Permission Granted]${edited}${byTag}`;
      // Default a missing rejection feedback to actionable guidance so the worker
      // always has a next step, mirroring the reference's feedback fallback.
      return `[Permission Denied] ${m.feedback || 'request refused by the team lead'}${byTag}`;
    }
    case 'sandbox_permission_request':
      return `[Sandbox Egress Request]${from ? ` from ${from}` : ''} — approve outbound network access to host "${m.host}"?`;
    case 'sandbox_permission_response':
      return `[Sandbox Egress ${m.allow ? 'Granted' : 'Denied'}] host "${m.host}"`;
    case 'team_permission_update': {
      const scope = m.directoryPath ? ` in ${m.directoryPath}` : '';
      const ruleTag = m.rule ? ` (${m.rule})` : '';
      return `[Team Permission Update] ${m.behavior} "${m.toolName}"${ruleTag}${scope}`;
    }
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
    case 'teammate_terminated':
      return `[Teammate Terminated]${from ? ` ${from}` : ''}${m.message ? ` — ${m.message}` : ''}`;
  }
}

/**
 * Compact, per-message-type natural-language PROJECTION of a send_message call for
 * the permission auto-classifier. Mirrors the reference's SendMessageTool
 * `toAutoClassifierInput`: the classifier that decides whether a send is auto-
 * allowable should see a short, intent-revealing phrase ("shutdown_request to X",
 * "plan_approval approve to X") rather than a raw JSON control blob (which both
 * bloats the classifier input and lets a crafted payload obscure intent). Plain
 * text projects as `to <recipient>: <text>`; a structured control message projects
 * to a terse `<type> <decision?> <recipient-or-id>` phrase. Distinct from
 * {@link summarizeStructuredMessage} (human/model-facing inbox wording with "[...]"
 * tags) — this is the classifier-facing projection keyed on the OUTBOUND `to`.
 */
export function toAutoClassifierInput(content: string, to: string): string {
  const m = parseStructuredMessage(content);
  if (!m || 'error' in m) return `to ${to}: ${content}`;
  switch (m.type) {
    case 'shutdown_request':
      return `shutdown_request to ${to}`;
    case 'shutdown_response':
      return `shutdown_response ${m.approve ? 'approve' : 'reject'} ${m.requestId}`;
    case 'plan_approval_request':
      return `plan_approval_request to ${to}`;
    case 'plan_approval_response':
      return `plan_approval ${m.approve ? 'approve' : 'reject'} to ${to}`;
    case 'permission_request':
      return `permission_request "${m.tool}" to ${to}`;
    case 'permission_response':
      return `permission_response ${m.approve ? 'grant' : 'deny'} ${m.requestId}`;
    case 'sandbox_permission_request':
      return `sandbox_permission_request host "${m.host}" to ${to}`;
    case 'sandbox_permission_response':
      return `sandbox_permission_response ${m.allow ? 'allow' : 'deny'} host "${m.host}"`;
    case 'team_permission_update':
      return `team_permission_update ${m.behavior} "${m.toolName}" to ${to}`;
    case 'task_assignment':
      return `task_assignment #${m.taskId} to ${to}`;
    case 'idle_notification':
      return `idle_notification to ${to}`;
    case 'mode_set_request':
      return `mode_set_request ${m.mode} to ${to}`;
    case 'teammate_terminated':
      return `teammate_terminated to ${to}`;
  }
}

/**
 * Ordered receive-side router: given a raw mailbox message, decide whether it is
 * relayed into the consumer's context and, if so, as what concise body. Mirrors
 * the reference's UserTeammateMessage ordered dispatch so a raw control-plane JSON
 * blob is never fed to the model. Resolution order (first match wins):
 *  1. `teammate_terminated` (Claude-compatible exit notice) → dropped (handled by
 *     the process layer, never surfaced as chatter).
 *  2. an APPROVED `shutdown_response` → dropped (the teammate is exiting; the
 *     approval is processed silently, mirroring the reference's isShutdownApproved pre-filter).
 *  3. `idle_notification` → a terse `✓ Completed task #id` line when it carries a
 *     completed task (AMI folds the reference's separate `task_completed` into
 *     idle_notification's `completedTaskId`), otherwise dropped (silent "I'm idle").
 *  4. any other structured control message → its concise summary (never raw JSON).
 *  5. a Claude-compatible `task_completed` message (not part of AMI's own union,
 *     but a cross-framework peer may emit it) → the same terse completion line.
 *  6. plain conversational text → relayed verbatim.
 */
export function relayIncomingMessage(content: string, from?: string): IncomingRelay {
  // Probe for a JSON discriminator covering BOTH AMI's structured protocol and
  // Claude-compatible lifecycle types a cross-framework peer may emit.
  let type: string | undefined;
  let obj: Record<string, unknown> | undefined;
  try {
    const p = JSON.parse(content);
    if (p && typeof p === 'object') {
      obj = p as Record<string, unknown>;
      if (typeof obj.type === 'string') type = obj.type;
    }
  } catch { /* plain conversational text */ }

  if (type === 'teammate_terminated') return { drop: true };

  const parsed = parseStructuredMessage(content);
  if (parsed && !('error' in parsed)) {
    if (parsed.type === 'shutdown_response' && parsed.approve) return { drop: true };
    // A sandbox egress decision is APPLIED by the worker's poll: record the
    // leader's grant in the session-scoped registry (so the worker's next
    // sandboxed command can reach the host) and drop it — it is machinery, not
    // conversational text. The request direction (seen by the leader) still
    // summarizes below. Wires the U022 transport to the U117 grant registry.
    if (parsed.type === 'sandbox_permission_response') {
      processSandboxPermissionResponse({ requestId: parsed.requestId, host: parsed.host, allow: parsed.allow });
      return { drop: true };
    }
    if (parsed.type === 'idle_notification') {
      if (parsed.completedTaskId) {
        return { drop: false, text: formatTaskCompleted(parsed.completedTaskId, parsed.summary) };
      }
      return { drop: true };
    }
    return { drop: false, text: summarizeStructuredMessage(content, from) ?? content };
  }

  if (type === 'task_completed' && obj) {
    const taskId = typeof obj.taskId === 'string' ? obj.taskId : undefined;
    const subject = typeof obj.taskSubject === 'string'
      ? obj.taskSubject
      : (typeof obj.subject === 'string' ? obj.subject : undefined);
    return { drop: false, text: formatTaskCompleted(taskId, subject) };
  }
  // A malformed/foreign idle_notification JSON (rejected by AMI's parser) is still
  // a silent lifecycle signal, never relayed as raw JSON.
  if (type === 'idle_notification') return { drop: true };

  return { drop: false, text: content };
}

const mailboxes = new Map<string, AgentMessage[]>();

/**
 * The active cross-process mailbox binding, or null when the feature is disabled or
 * no team directory is established. Resolves the shared `<teamsRoot>/<teamName>`
 * location from the team singleton so every process in the swarm addresses the same
 * inbox tree.
 */
function activeFsMailbox():
  | { mod: typeof import('./mailbox-fs'); loc: import('./mailbox-fs').FsMailboxLocation }
  | null {
  if (!fsMailboxEnabled()) return null;
  try {
    const { getCurrentTeamDir } = require('./team') as typeof import('./team');
    const dir = getCurrentTeamDir();
    if (!dir) return null;
    const p = require('path') as typeof import('path');
    const mod = require('./mailbox-fs') as typeof import('./mailbox-fs');
    return { mod, loc: { teamsRoot: p.dirname(dir), teamName: p.basename(dir) } };
  } catch {
    return null;
  }
}

/**
 * Names of teammates reachable over the cross-process inbox tree (empty when the
 * file-backed transport is disabled or no team is established). Parity with the reference
 * Code's `ListPeers` peer discovery — a separate-process teammate is addressable by
 * bare name because its inbox file is enumerated here. Exposed for introspection and
 * the corrective "known agents" affordance.
 */
export function listCrossSessionPeers(): string[] {
  const active = activeFsMailbox();
  if (!active) return [];
  try { return active.mod.listInboxPeers(active.loc); } catch { return []; }
}
const outboundDms = new Map<string, OutboundDm[]>();

/**
 * Record one plain-text direct message a sender emitted, so the idle path can
 * later derive an idle-notification summary from the sender's last peer DM.
 * Broadcasts ("*") are not recorded (they are not a peer DM — mirrors the reference
 * excluding `to === '*'`). Capped at {@link MAX_OUTBOUND_DM_HISTORY} per sender.
 */
function recordOutboundDm(from: string, to: string, summary: string): void {
  const peer = to !== '*' && to.toLowerCase() !== TEAM_LEAD_NAME.toLowerCase();
  const list = outboundDms.get(from) ?? [];
  list.push({ to, summary, peer });
  if (list.length > MAX_OUTBOUND_DM_HISTORY) {
    list.splice(0, list.length - MAX_OUTBOUND_DM_HISTORY);
  }
  outboundDms.set(from, list);
}

/**
 * The sender's most recent PEER direct message, formatted `[to <name>] <summary>`,
 * or undefined when the sender has sent no peer DM. Backward scan with the same
 * peer/lead/"*" exclusion as the reference's getLastPeerDmSummary. Non-destructive:
 * the record is only read, never consumed, so repeated calls are idempotent.
 */
export function getLastPeerDmSummary(agentName: string): string | undefined {
  const list = outboundDms.get(agentName);
  if (!list) return undefined;
  for (let i = list.length - 1; i >= 0; i--) {
    const dm = list[i];
    if (dm.peer) return `[to ${dm.to}] ${dm.summary}`;
  }
  return undefined;
}

/**
 * Proactively push an `idle_notification` (idleReason 'available') into the team
 * lead's mailbox when a teammate FINISHES its turn, so the lead learns a peer is
 * now free to take new work WITHOUT polling. This is the automatic, push-based half
 * of the idle lifecycle: the manual `idle_notification` send path (execute's
 * structured branch) lets a teammate report idleness explicitly, but a teammate that
 * simply stops never emits one; the lead would otherwise have to poll. Mirrors
 * the reference's Stop hook writing `createIdleNotification(agentName,
 * { idleReason: 'available', summary })` to the leader's inbox (utils/swarm/
 * teammateInit.ts). The summary is auto-derived from the teammate's LAST peer DM
 * ({@link getLastPeerDmSummary}) — the same source the manual idle path uses — so the
 * lead sees what the teammate was last coordinating on without it restating. Routed
 * through the canonical {@link enqueue} so it bumps the lead's revision and resolves
 * any pending receiveMessage() waiter, exactly like a model-sent idle_notification.
 *
 * No-op (returns false) when the finishing agent IS the lead (the lead has no lead to
 * notify) or has no resolvable identity. Returns true when a notification was queued.
 */
export function notifyLeadTeammateIdle(agentName: string): boolean {
  if (!agentName || !agentName.trim()) return false;
  if (agentName.toLowerCase() === TEAM_LEAD_NAME.toLowerCase()) return false;
  const summary = getLastPeerDmSummary(agentName);
  const payload: IdleNotification = {
    type: 'idle_notification',
    idleReason: 'available',
    ...(summary ? { summary } : {}),
  };
  enqueue(TEAM_LEAD_NAME, {
    from: agentName,
    to: TEAM_LEAD_NAME,
    content: JSON.stringify(payload),
    timestamp: Date.now(),
  });
  return true;
}

/**
 * Proactively push a `teammate_terminated` notice into the team lead's mailbox when
 * a teammate exits ABNORMALLY (killed / failed), so the lead learns a peer's work is
 * orphaned and can release/reassign it — WITHOUT polling, and WITHOUT the dead agent
 * having to send anything itself (it may have crashed). This is the termination
 * counterpart of {@link notifyLeadTeammateIdle}: a clean finish pushes an
 * idle/available notice, an abnormal exit pushes a termination notice. Mirrors the reference
 * Code's killTeammate injecting a `teammate_terminated` system message into the lead's
 * inbox (components/teams/TeamsDialog.tsx) and its always-run cleanup. The notice is
 * surfaced silently by the receive-side relay ({@link relayIncomingMessage} drops it),
 * never dumped as chatter. Routed through the canonical {@link enqueue} so it bumps the
 * lead's revision and resolves any pending receiveMessage() waiter.
 *
 * No-op (returns false) when the exiting agent IS the lead (no lead to notify) or has
 * no resolvable identity. Returns true when a notice was queued.
 */
export function notifyLeadTeammateTerminated(agentName: string, message?: string): boolean {
  if (!agentName || !agentName.trim()) return false;
  if (agentName.toLowerCase() === TEAM_LEAD_NAME.toLowerCase()) return false;
  // Any still-open tasks assigned to the dying teammate are now orphaned. Fold a
  // terse "now unassigned: #id (subject), …" summary into the notice so the lead can
  // reassign them, then release them from the registry (they are no longer owned by
  // anyone). Mirrors the reference's unassignTeammateTasks notificationMessage.
  const orphaned = getOpenTaskAssignments(agentName);
  openTaskAssignments.delete(agentName);
  const base = (message && message.trim()) ? message.trim() : undefined;
  let note = base;
  if (orphaned.length > 0) {
    const list = orphaned.map(t => `#${t.taskId}${t.subject ? ` (${t.subject})` : ''}`).join(', ');
    const summary = `${orphaned.length} task(s) now unassigned: ${list}`;
    note = base ? `${base} — ${summary}` : summary;
  }
  const payload: TeammateTerminated = {
    type: 'teammate_terminated',
    ...(note ? { message: note } : {}),
  };
  enqueue(TEAM_LEAD_NAME, {
    from: agentName,
    to: TEAM_LEAD_NAME,
    content: JSON.stringify(payload),
    timestamp: Date.now(),
  });
  return true;
}
const mailboxWaiters = new Map<string, MailboxWaiter[]>();

/**
 * Monotonic per-agent delivery counter. Incremented on EVERY delivery to an
 * agent — whether the message is enqueued or handed straight to a waiter — so a
 * reactive consumer can cheaply detect "something changed" / staleness without
 * diffing the queue. Mirrors the reference's Mailbox.revision.
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
 * Hard upper bound on how long (ms) an undelivered message may sit in a mailbox
 * before it is self-healed away. Complements MAX_MAILBOX (which bounds the COUNT):
 * this bounds the AGE, so a recipient that never drains but keeps being written to
 * cannot accumulate stale traffic indefinitely — the memory-growth vector the reference
 * Code guards with `cleanupOldResolutions`'s age-based pruning (maxAgeMs). The TTL
 * is deliberately generous (1h ≫ the sub-second cadence of a real tool round), so
 * it only engages on a long-dead/evicted recipient, never on live coordination.
 */
export const MAILBOX_ENTRY_TTL_MS = 60 * 60 * 1000;

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
  // Self-healing age prune: drop any already-queued entry older than the TTL before
  // appending, so a never-draining recipient that keeps being written to cannot
  // hoard stale traffic (mirrors the reference's cleanupOldResolutions age bound).
  // In-place splice so the caller's live array reference stays valid.
  const cutoff = Date.now() - MAILBOX_ENTRY_TTL_MS;
  for (let i = box.length - 1; i >= 0; i--) {
    if (box[i].timestamp < cutoff) box.splice(i, 1);
  }
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

/**
 * Multi-subscriber mailbox change signal (parity with the reference's
 * utils/mailbox.ts `Mailbox.subscribe`, backed by utils/signal.ts createSignal).
 * Distinct from the single eager-delivery notifier above: that one is a replace-
 * semantics wake-hook whose PRESENCE means "this agent's engine loop is actively
 * listening" (consumed by deliveryAffordance/isReachable); this is a fan-out
 * observer registry where ANY number of independent reactive consumers can learn
 * that "something changed" on an agent's mailbox (e.g. a revision-counter view via
 * useSyncExternalStore) without any of them clobbering another. Both fire on every
 * delivery (see notifyMailbox) so the eager wake-hook and reactive observers
 * coexist. Returns an unsubscribe function (AMI differentiator: the eager hook is
 * kept alongside the reference's multi-subscriber signal, not replaced by it).
 */
const mailboxSubscribers = new Map<string, Set<() => void>>();

export function subscribeMailbox(agentName: string, listener: () => void): () => void {
  const set = mailboxSubscribers.get(agentName) ?? new Set<() => void>();
  set.add(listener);
  mailboxSubscribers.set(agentName, set);
  return () => {
    const s = mailboxSubscribers.get(agentName);
    if (!s) return;
    s.delete(listener);
    if (s.size === 0) mailboxSubscribers.delete(agentName);
  };
}

/**
 * Fire BOTH the single eager-delivery notifier AND every reactive subscriber for
 * an agent, if any. Never throws — one listener failing must never break delivery
 * or starve the others (each is isolated in its own try/catch).
 */
export function notifyMailbox(agentName: string): void {
  const fn = mailboxNotifiers.get(agentName);
  if (fn) { try { fn(); } catch { /* a notifier must never break delivery */ } }
  const subs = mailboxSubscribers.get(agentName);
  if (subs) {
    for (const s of subs) { try { s(); } catch { /* one observer must not starve the rest */ } }
  }
}

// --- Idempotent graceful-shutdown handshake ---------------------------------
// A shutdown_request opens a negotiated cancellation with the target teammate: it
// either approves (exits) or rejects (keeps working). Re-sending a shutdown_request
// to a teammate that already has one outstanding is a NO-OP — a second request must
// not queue a duplicate or restart the handshake. Mirrors the reference's
// InProcessBackend.terminate(): it sets a `shutdownRequested` flag on the task and,
// on re-entry, returns early ("shutdown already requested") instead of sending
// another request. The handshake is RESOLVED (flag cleared) when the teammate emits
// its shutdown_response, and torn down on purge/reset. Keyed by the bare teammate
// name the request was addressed to (the same identity the responder stamps as its
// trusted `from`), so request-open and response-close line up.
const outstandingShutdowns = new Map<string, string>();

/**
 * True when a shutdown_request has been sent to `agentName` and no shutdown_response
 * has resolved it yet. Introspection surface for the idempotent handshake — lets a
 * caller (and tests) see the pending-cancellation state without re-sending.
 */
export function hasOutstandingShutdownRequest(agentName: string): boolean {
  return outstandingShutdowns.has(agentName);
}

// --- Blocked-on-plan-approval boundary --------------------------------------
// A teammate that submits a plan_approval_request enters a BLOCKED state: it has
// handed a plan to the lead and must wait for a decision before proceeding. The
// boundary is RESOLVED when the lead sends the matching plan_approval_response
// back to that teammate — whether approved (proceed) or rejected (revise) — so the
// teammate is cleanly unblocked either way. Keyed by the submitting teammate's bare
// trusted name (the same identity it stamps as `from`), so submit-open and
// response-close line up. Mirrors the reference's inProcessTeammateHelpers
// setAwaitingPlanApproval / handlePlanApprovalResponse (utils/inProcessTeammateHelpers.ts),
// which tracks a per-teammate `awaitingPlanApproval` flag and resets it exactly when
// a plan_approval_response arrives from the leader. AMI already routed both plan
// messages but tracked no blocked state; this adds the boundary entirely within the
// send_message execute() paths (no engine wiring), mirroring the shutdown handshake.
const awaitingPlanApproval = new Map<string, string>();

/**
 * True when `agentName` has submitted a plan and is still awaiting the lead's
 * decision. Introspection surface for the plan-approval boundary — lets a caller
 * (and tests) see the blocked state without re-sending.
 */
export function isAwaitingPlanApproval(agentName: string): boolean {
  return awaitingPlanApproval.has(agentName);
}

/**
 * The request id of the plan `agentName` is awaiting approval on, or undefined when
 * it is not blocked. Lets a consumer correlate the pending plan to the eventual
 * plan_approval_response.
 */
export function getAwaitingPlanApprovalRequestId(agentName: string): string | undefined {
  return awaitingPlanApproval.get(agentName);
}

/**
 * Purge the PENDING cross-agent request/handshake registries that must NOT survive a
 * session reset (`/clear`): the outstanding shutdown-request handshakes
 * ({@link hasOutstandingShutdownRequest}) and the blocked-on-plan-approval boundaries
 * ({@link isAwaitingPlanApproval}). These are AMI's analog of the reference's pending
 * permission-callback registry, which `/clear` wipes via `clearAllPendingCallbacks`
 * (hooks/useSwarmPermissionPoller.ts L108-116) so a fresh conversation never inherits a
 * stale "teammate X is still mid-shutdown / still awaiting plan approval" decision. Left
 * unresolved, such state wrongly SHORT-CIRCUITS a new request after `/clear`: a re-sent
 * shutdown_request is treated as an idempotent no-op against last session's outstanding
 * handshake (execute's `outstandingShutdowns.get(toRaw)` early return), and a teammate is
 * reported blocked on a plan it submitted in a conversation that no longer exists.
 *
 * Deliberately leaves the live mailbox QUEUES untouched — an in-process teammate that is
 * still running keeps its inbox; only the pending-DECISION bookkeeping is reset. For a
 * full per-agent teardown (queues, waiters, notifiers, …) use {@link purgeMailbox}; for a
 * whole-process reset (tests) use {@link resetAllMailboxes}. Returns the total number of
 * pending entries cleared, so a caller can surface an introspectable confirmation.
 */
export function clearPendingCrossAgentHandshakes(): number {
  const cleared = outstandingShutdowns.size + awaitingPlanApproval.size;
  outstandingShutdowns.clear();
  awaitingPlanApproval.clear();
  return cleared;
}

// --- Open task-assignment tracking (for the shutdown/terminate summary) ------
// A task_assignment routed to a teammate opens a unit of work owned BY that
// teammate; a matching idle_notification carrying `completedTaskId` closes it.
// Tracking the set of still-open assignments per assignee lets the termination
// path report WHICH tasks are now orphaned when a teammate dies, so the lead can
// reassign them — mirroring the reference's unassignTeammateTasks building a
// "X has shut down. The following tasks are now unassigned: …" notice
// (hooks/useInboxPoller.ts). Keyed by the assignee's bare name (the same identity
// task_assignment addresses and the terminate path resolves). Maps taskId→subject
// so the summary can name each orphaned task.
const openTaskAssignments = new Map<string, Map<string, string>>();

/** Record a task as open/assigned to a teammate (idempotent on taskId). */
function recordTaskAssignment(assignee: string, taskId: string, subject: string): void {
  const tasks = openTaskAssignments.get(assignee) ?? new Map<string, string>();
  tasks.set(taskId, subject);
  openTaskAssignments.set(assignee, tasks);
}

/** Close one of a teammate's open assignments (e.g. it reported the task done). */
function clearTaskAssignment(assignee: string, taskId: string): void {
  const tasks = openTaskAssignments.get(assignee);
  if (!tasks) return;
  tasks.delete(taskId);
  if (tasks.size === 0) openTaskAssignments.delete(assignee);
}

/**
 * The still-open tasks assigned to a teammate, as `{ taskId, subject }` records in
 * assignment order. Introspection surface for the unassignment summary — lets a
 * caller (and tests) see what work would be orphaned if the teammate died, without
 * mutating the registry. Empty array when the teammate owns no open task.
 */
export function getOpenTaskAssignments(assignee: string): { taskId: string; subject: string }[] {
  const tasks = openTaskAssignments.get(assignee);
  if (!tasks) return [];
  return [...tasks.entries()].map(([taskId, subject]) => ({ taskId, subject }));
}

export function getMailbox(agentName: string): AgentMessage[] {
  return mailboxes.get(agentName) || [];
}

export function clearMailbox(agentName: string): void {
  mailboxes.delete(agentName);
}

/**
 * Full exit-boundary teardown of one agent's mailbox state. Beyond the queue that
 * clearMailbox flushes, this reclaims every per-agent resource an exiting agent
 * leaves behind: pending receive() waiters (whose promises would otherwise dangle
 * forever), the eager-delivery notifier, the revision counter, and the recorded
 * outbound-DM history. Called at an agent's exit / a team's disband so a future
 * agent that reuses the name never inherits a dead agent's queue, a leaked waiter,
 * or a stale notifier. Mirrors the reference purging an exiting teammate's inbox at
 * its lifecycle boundary — clearMailbox is the lighter "flush the queue only" form.
 */
export function purgeMailbox(agentName: string): void {
  clearMailbox(agentName);
  mailboxWaiters.delete(agentName);
  mailboxNotifiers.delete(agentName);
  mailboxSubscribers.delete(agentName);
  mailboxRevisions.delete(agentName);
  outboundDms.delete(agentName);
  outstandingShutdowns.delete(agentName);
  awaitingPlanApproval.delete(agentName);
  openTaskAssignments.delete(agentName);
}

export function resetAllMailboxes(): void {
  mailboxes.clear();
  mailboxNotifiers.clear();
  mailboxSubscribers.clear();
  mailboxWaiters.clear();
  mailboxRevisions.clear();
  outboundDms.clear();
  outstandingShutdowns.clear();
  awaitingPlanApproval.clear();
  openTaskAssignments.clear();
  messageSeq = 0;
}

/**
 * Number of messages currently queued for an agent (not counting any handed
 * straight to a waiter). Mirrors the reference's Mailbox.length getter.
 */
export function mailboxLength(agentName: string): number {
  return (mailboxes.get(agentName) || []).length;
}

/**
 * Monotonic count of deliveries made to an agent for its entire lifetime,
 * including waiter hand-offs. Starts at 0 for an unknown agent. Mirrors the reference
 * Code's Mailbox.revision getter — a cheap change/staleness signal.
 */
export function mailboxRevision(agentName: string): number {
  return mailboxRevisions.get(agentName) || 0;
}

/**
 * Selectively extract ONE queued message by predicate (default: the first one),
 * removing it and leaving every other message queued. Returns undefined when no
 * queued message matches. Non-blocking. Mirrors the reference's Mailbox.poll(fn).
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
 * the reference's Mailbox.receive(fn), enabling request/response correlation
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

/**
 * Drain an agent's mailbox. With no predicate it removes and returns EVERY queued
 * message (the common relay path). With a predicate it drains only the matching
 * messages and leaves the rest queued, so a consumer can selectively consume one
 * class of message (e.g. only control traffic) without discarding the others.
 * Mirrors the reference's useMailboxBridge predicate/filter on the mailbox poll.
 */
export function pollMailbox(
  agentName: string,
  fn?: (msg: AgentMessage) => boolean,
): AgentMessage[] {
  const messages = mailboxes.get(agentName) || [];
  // Merge the cross-process file inbox (opt-in). Same-process copies (already queued
  // in memory, so their id is known) are dropped from the file so nothing is
  // delivered twice; genuine cross-session arrivals are stamped so a consumer can
  // render them as `<cross-session-message from=...>`. Drained copies respect the
  // same optional predicate as the in-memory drain.
  let fsTaken: AgentMessage[] = [];
  const active = activeFsMailbox();
  if (active) {
    const knownIds = new Set<string>();
    for (const m of messages) if (m.id) knownIds.add(m.id);
    try { fsTaken = active.mod.takeFsInbox(active.loc, agentName, knownIds, fn); } catch { fsTaken = []; }
    for (const m of fsTaken) {
      m.crossSession = true;
      if (!m.source) m.source = classifyMessageSource(m.content, m.from);
    }
  }
  if (messages.length === 0) return fsTaken;
  if (!fn) {
    mailboxes.set(agentName, []);
    return fsTaken.length ? [...fsTaken, ...messages] : messages;
  }
  const taken: AgentMessage[] = [];
  const kept: AgentMessage[] = [];
  for (const m of messages) {
    (fn(m) ? taken : kept).push(m);
  }
  if (taken.length > 0) mailboxes.set(agentName, kept);
  return fsTaken.length ? [...fsTaken, ...taken] : taken;
}

/**
 * Drain an agent's mailbox in PRIORITY order (anti-starvation), removing every
 * queued message but returning them two-tier-ordered instead of raw FIFO. Mirrors
 * the reference's inProcessRunner.waitForNextPromptOrShutdown scan order:
 *  1. `shutdown_request` messages first — a pending cancellation must never be
 *     starved behind a flood of peer-to-peer chatter;
 *  2. then permission-DECISION responses (tool + sandbox) — a worker blocked on a
 *     forwarded permission/sandbox prompt is waiting on exactly this reply to
 *     unblock, so it must not sit behind a flood of peer chatter
 *     ({@link isPermissionRelatedResponse}, mirroring the reference's control-first scan);
 *  3. then messages FROM the team lead — the lead carries user intent and
 *     coordination, so its messages are not starved behind peer chatter;
 *  4. then PROACTIVE peer messages — a teammate that marked a message `proactive`
 *     is surfacing a blocker/completion the recipient needs to see now, so it is
 *     not starved behind ordinary peer chatter (mirrors the reference's BriefTool
 *     proactive-status urgency; the urgency rides on {@link AgentMessage.status});
 *  5. then everything else (ordinary/`normal` peer messages).
 * FIFO order is preserved WITHIN each tier (a stable partition), so same-tier
 * messages keep their arrival order. Equivalent to the unordered drain of
 * `pollMailbox(agentName)` in membership — nothing is dropped — only the order
 * differs, so it is a drop-in replacement for a drain-all consumer.
 */
export function drainMailboxPrioritized(agentName: string): AgentMessage[] {
  const messages = pollMailbox(agentName);
  if (messages.length <= 1) return messages;
  const shutdowns: AgentMessage[] = [];
  const permissions: AgentMessage[] = [];
  const lead: AgentMessage[] = [];
  const proactive: AgentMessage[] = [];
  const rest: AgentMessage[] = [];
  for (const m of messages) {
    // Parse each message ONCE and reuse the result for both the shutdown and the
    // permission-response classification (the old code re-parsed via
    // isPermissionRelatedResponse, doubling the JSON.parse + validate per message).
    const parsed = parseStructuredMessage(m.content);
    const ok = parsed && !('error' in parsed);
    if (ok && parsed.type === 'shutdown_request') {
      shutdowns.push(m);
    } else if (ok && (parsed.type === 'permission_response' || parsed.type === 'sandbox_permission_response')) {
      permissions.push(m);
    } else if (m.from === TEAM_LEAD_NAME) {
      lead.push(m);
    } else if (m.status === 'proactive') {
      proactive.push(m);
    } else {
      rest.push(m);
    }
  }
  return [...shutdowns, ...permissions, ...lead, ...proactive, ...rest];
}

/**
 * Read an agent's pending messages WITHOUT removing them (the "read" half of the
 * crash-safe at-least-once protocol — mirrors the reference's readUnreadMessages).
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
 * the reference's markMessagesAsRead.
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
 * it comes from the lead/main session and 'teammate' otherwise. Mirrors the reference
 * Code's MessageSource discrimination. ('tick' is reserved for timer-injected
 * messages, which do not flow through send_message.)
 */
export function classifyMessageSource(content: string, from: string): MessageSource {
  const parsed = parseStructuredMessage(content);
  if (parsed && !('error' in parsed)) {
    return parsed.type === 'task_assignment' || parsed.type === 'idle_notification' || parsed.type === 'teammate_terminated'
      ? 'task'
      : 'system';
  }
  return from === TEAM_LEAD_NAME ? 'user' : 'teammate';
}

/**
 * Resolve the sender's assigned display color from the trusted team roster (by
 * name or agentId). Returns undefined when there is no team, the sender is not a
 * roster member, or the team module is unavailable — the color is a best-effort
 * display hint, never required for delivery. Keyed off the SAME color team.ts
 * assigns on spawn (round-robin palette), so an incoming message can be rendered
 * in its sender's color. Never model-supplied.
 */
function resolveSenderColor(from: string): string | undefined {
  try {
    const { getCurrentTeam } = require('./team');
    const team = getCurrentTeam();
    if (!team || !Array.isArray(team.members)) return undefined;
    const member = team.members.find(
      (m: { name: string; agentId?: string; color?: string }) => m.name === from || m.agentId === from,
    );
    return member?.color;
  } catch { return undefined; }
}

function enqueue(to: string, msg: AgentMessage): void {
  // Stamp the origin category once, at the canonical delivery site, so every
  // queued OR waiter-handed-off message carries a typed `source` a consumer can
  // route on. Idempotent: a broadcast reuses one object across mailboxes and the
  // classification depends only on content+from, so re-entry is a no-op.
  if (!msg.source) { msg.source = classifyMessageSource(msg.content, msg.from); }
  // Stamp the sender's trusted display color once, at the same canonical site, so
  // every delivered message (direct, structured, broadcast, waiter hand-off)
  // carries it for distinct per-teammate rendering. Resolved from the roster, not
  // the model; absent when the sender is not a colored member. Idempotent.
  if (msg.color === undefined) {
    const c = resolveSenderColor(msg.from);
    if (c) { msg.color = c; }
  }
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
      traceMailbox('deliver', { to, from: msg.from, source: msg.source, via: 'waiter' });
      notifyMailbox(to);
      return;
    }
  }
  const box = mailboxes.get(to) ?? [];
  appendCapped(box, msg);
  mailboxes.set(to, box);
  // Durable cross-process copy: a recipient running in a SEPARATE process drains
  // this from its file inbox (this process's in-memory copy above is its orphan).
  // Only on the QUEUE path — a same-process waiter hand-off above already delivered
  // it locally. Best-effort: a transport failure never blocks the in-memory send.
  const active = activeFsMailbox();
  if (active) {
    try { active.mod.appendToInbox(active.loc, to, msg); } catch { /* durable copy best-effort */ }
  }
  traceMailbox('deliver', { to, from: msg.from, source: msg.source, via: 'queue', depth: box.length });
  notifyMailbox(to);
}

/**
 * Delivery-state affordance for a plain-text DM's receipt, so the sender knows
 * HOW its message will be picked up. Mirrors the reference's call() branching on
 * the recipient's lifecycle (SendMessageTool.ts L800-874: queue-to-running vs
 * injecting into an actively-listening agent):
 *  - the recipient has a live eager-delivery notifier → it is actively listening
 *    and `enqueue` just fired that notifier, so it picks the message up now;
 *  - otherwise it is a tracked, still-running task with no live notifier → the
 *    message simply waits in its mailbox until its next tool round;
 *  - otherwise (untracked / not yet online) → no affordance (the separate
 *    unreachable warning covers a likely-dead/typo recipient).
 * `to` is the resolved recipient key (agentId or bare name) the message was
 * enqueued under — the same key `enqueue`/`notifyMailbox` and `getStatus` use.
 */
function deliveryAffordance(toRaw: string, to: string, context: ToolContext): string {
  if (mailboxNotifiers.has(to)) {
    return ` — ${toRaw} is listening and was notified; it will pick this up now`;
  }
  if (context.processManager && context.processManager.getStatus(to) === 'running') {
    return ` — queued for ${toRaw}'s next tool round`;
  }
  return '';
}

// --- Honest delivery-hold receipt (parity: Orca describeMailboxDeliveryHold) --
// A send receipt must state only what is TRUE at send time: a message to a
// not-yet-settled recipient is QUEUED, not "delivered". {@link readDeliveryHold}
// reads the SAME gates send_message already enforces and returns a typed
// {@link DeliveryHold} naming WHY the message will wait (or null when it goes
// through now); {@link describeDeliveryHold} renders that into a one-line, honest
// "Queued. …" note. Mirrors Orca's runtime/orchestration/mailbox/delivery-hold
// (readMailboxDeliveryHold → chatMailHold + describeMailboxDeliveryHold), narrowed
// to the recipient states AMI can honestly observe. Pure: no mutation, no delivery.

/**
 * Read the delivery-hold state for a plain-text DM to `to` (resolved key) /
 * `toRaw` (addressed name), from the same gates `send_message` enforces. Returns a
 * typed {@link DeliveryHold}, or `null` when delivery would go through immediately
 * (the recipient has a live eager-delivery notifier — it is settled and listening).
 * Resolution order mirrors Orca's readMailboxDeliveryHold (most-decisive first):
 *  1. a tracked task in a terminal/blocked state — `failed` → `failed-turn`,
 *     `killed`/`completed` → `unreachable`, running-but-aborting → `blocked`;
 *  2. the recipient's OWN blocked state (awaiting plan approval / outstanding
 *     shutdown handshake) → `blocked`;
 *  3. a live notifier → `null` (settled, delivers now);
 *  4. a tracked `running` task with no live notifier → `busy` (waits for its next
 *     tool round);
 *  5. a known/roster/cross-process peer not yet online → `detached`;
 *  6. an unrecognized name while a roster or other live agents exist → `unreachable`.
 * Never a hold for a broadcast caller (the caller passes a concrete recipient).
 */
export function readDeliveryHold(toRaw: string, to: string, context: ToolContext): DeliveryHold | null {
  const pm = context.processManager;
  if (pm) {
    const status = pm.getStatus(to);
    if (status === 'failed') return { kind: 'failed-turn' };
    if (status === 'killed' || status === 'completed') return { kind: 'unreachable', recipient: toRaw };
    if (status === 'running' && typeof pm.isActive === 'function' && !pm.isActive(to)) {
      return { kind: 'blocked', reason: 'shutting down' };
    }
  }
  // The recipient's own blocked state (its status says it is blocked), keyed by the
  // bare addressed name the handshake/boundary registries use.
  if (hasOutstandingShutdownRequest(toRaw)) return { kind: 'blocked', reason: 'shutdown requested' };
  if (isAwaitingPlanApproval(toRaw)) return { kind: 'blocked', reason: 'awaiting plan approval' };
  // Settled + listening: the eager-delivery notifier is live, so enqueue fires it
  // and the recipient picks the message up now — no hold.
  if (mailboxNotifiers.has(to)) return null;
  if (pm && pm.getStatus(to) === 'running') return { kind: 'busy' };
  // Known teammate (roster / cross-process peer / tracked) that is not yet online.
  if (isReachable(to)) return { kind: 'detached' };
  // Nobody answers to this name, but a roster or other live agents exist → a likely
  // typo/stale name. With no team context at all, there is no honest hold to report
  // (the message simply queues into a fresh mailbox for a coming-online teammate).
  if (mailboxNotifiers.size > 0 || rosterMemberNames().length > 0) {
    return { kind: 'unreachable', recipient: toRaw };
  }
  return null;
}

/**
 * Deliver a message the durable parked mailbox ({@link common/src/mailbox}) has
 * decided is now settled, routing it through the SAME canonical in-session
 * {@link enqueue} primitive a direct send uses — so a redriven parked message is
 * delivered with identical semantics (revision bump, waiter hand-off, durable
 * cross-process copy) to one sent while the recipient was already settled. The park
 * layer lives in common (it may import core); this is the core-side delivery seam it
 * targets so there is never a second delivery path. Exposed for that layer and tests.
 */
export function enqueueParkedDelivery(recipient: string, msg: AgentMessage): void {
  enqueue(recipient, { ...msg, to: recipient });
}

/**
 * Display names of every member in the current team roster (from the trusted team
 * file, never model-supplied), or `[]` when no team is established. Used both to
 * recognize a declared-but-not-yet-spawned teammate as a valid recipient and to
 * build the corrective "known agents" affordance. Mirrors the reference's
 * teamContext.teammates lookup in utils/directMemberMessage.ts.
 */
function rosterMemberNames(): string[] {
  try {
    const { getCurrentTeam } = require('./team');
    const team = getCurrentTeam();
    if (!team || !Array.isArray(team.members)) return [];
    return team.members
      .map((m: { name?: string }) => m.name)
      .filter((n: unknown): n is string => typeof n === 'string' && n.length > 0);
  } catch { return []; }
}

/**
 * A recipient is "reachable" if it is a registered agent, has a live notifier,
 * already owns a mailbox, or is a declared member of the current team roster.
 * Used to warn on a likely-dead/typo recipient. Including roster members means a
 * pre-declared teammate that has not yet come online is not mis-flagged as unknown.
 */
function isReachable(name: string): boolean {
  if (mailboxNotifiers.has(name)) return true;
  if (mailboxes.has(name)) return true;
  try {
    const { getAgentByName } = require('./task');
    if (getAgentByName(name)) return true;
  } catch { /* task module unavailable */ }
  const lc = name.toLowerCase();
  if (rosterMemberNames().some(n => n.toLowerCase() === lc)) return true;
  // A separate-process teammate advertises itself by its cross-process inbox file.
  return listCrossSessionPeers().some(n => n.toLowerCase() === lc);
}

/**
 * The deduplicated set of recipient names a sender could legitimately address this
 * session — live notifier/mailbox owners plus the trusted team roster, always
 * including the lead. Backs the corrective affordance that lists the KNOWN agents
 * when a send targets an unrecognized recipient, so the model can self-correct a
 * typo/stale name instead of silently creating an orphan mailbox nobody drains.
 * Mirrors the reference's PaneBackendExecutor "Invalid agentId … expected …" hint.
 */
function knownRecipientNames(exclude?: string): string[] {
  const names = new Set<string>([TEAM_LEAD_NAME]);
  for (const n of mailboxes.keys()) names.add(n);
  for (const n of mailboxNotifiers.keys()) names.add(n);
  for (const n of rosterMemberNames()) names.add(n);
  for (const n of listCrossSessionPeers()) names.add(n);
  if (exclude) names.delete(exclude);
  return [...names].sort();
}

/**
 * Deliver a user-typed `@agent-name message` straight to that teammate's mailbox,
 * bypassing the model. The sender is the lead/user ({@link TEAM_LEAD_NAME}) — the
 * human drives the lead session — so the message classifies as a `user` source and
 * the teammate receives it exactly like a lead-authored send. Routes through the
 * canonical {@link enqueue} (so it bumps the recipient's revision and resolves any
 * pending waiter) and records it as an outbound DM for idle-summary derivation —
 * identical delivery semantics to the model tool's plain-text path, never a parallel
 * transport. Returns a typed {@link DirectMessageResult}: a HARD unknown/no-team
 * result when the name is not a reachable teammate, so the CLI can fall through to a
 * normal model turn and preserve AMI's `@`-mention differentiator.
 */
export function deliverDirectMemberMessage(recipientName: string, message: string): DirectMessageResult {
  const to = (recipientName || '').trim();
  const body = (message || '').trim();
  const hasRoster = rosterMemberNames().length > 0;
  const liveOrRoster = hasRoster || mailboxNotifiers.size > 0 || mailboxes.size > 0;
  const failKind: 'no_team_context' | 'unknown_recipient' = liveOrRoster ? 'unknown_recipient' : 'no_team_context';
  if (!to || !body) {
    return { success: false, error: failKind, recipientName: to || undefined };
  }
  // Resolve a display name to its stable agentId when the task layer knows it, so
  // the message lands under the same key the teammate's engine drains.
  let resolved = to;
  try {
    const { getAgentByName } = require('./task');
    resolved = getAgentByName(to) || to;
  } catch { /* task module optional */ }
  // The user cannot direct-message itself / the lead through this path.
  if (to.toLowerCase() === TEAM_LEAD_NAME.toLowerCase()) {
    return { success: false, error: failKind, recipientName: to };
  }
  if (!isReachable(resolved) && !isReachable(to)) {
    return { success: false, error: failKind, recipientName: to };
  }
  const from = TEAM_LEAD_NAME;
  const summary = deriveSummary(body);
  enqueue(resolved, { from, to: resolved, content: body, summary: summary || undefined, timestamp: Date.now() });
  recordOutboundDm(from, to, summary || body.slice(0, 80));
  traceMailbox('direct-member', { to, from });
  return { success: true, recipientName: to, from };
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
    'permission_request/permission_response for forwarding a worker\'s permission prompt to the lead, and ' +
    'sandbox_permission_request/sandbox_permission_response (host, allow) for forwarding a blocked network-egress ' +
    'host to the lead. The lead can push a team-wide rule to everyone with a "team_permission_update" ' +
    '(toolName, behavior allow/deny/ask, optional rule/directoryPath), sent to one teammate or "*". ' +
    'The lead can assign work with a "task_assignment" (taskId, subject, description) or ask a teammate to switch ' +
    'permission posture with a "mode_set_request" (mode); a teammate reports it has finished its turn with an ' +
    '"idle_notification" (optional idleReason/completedTaskId/completedStatus/summary) sent to the lead. Approving a ' +
    'plan may carry a "permissionMode" the teammate inherits on exiting plan-mode.\n' +
    '\n' +
    'Cross-session delivery: "to" may be a local peer address "uds:<socket-path>" instead of a bare teammate ' +
    'name, to reach an agent running in a SEPARATE session/process. Only PLAIN TEXT crosses a session ' +
    'boundary — structured control messages (shutdown/plan/permission/mode/task) are session-local and ' +
    'cannot be sent cross-session. A reachable local peer is alive and will process your message: there is ' +
    'no busy state to wait on — the message enqueues and drains at the receiver\'s next tool round.\n' +
    '\n' +
    'File-backed swarm: when AMI_MAILBOX_FS is set, teammates running as SEPARATE processes of the same team ' +
    'are reachable by their bare name too — their inboxes are discovered automatically and appear in the ' +
    'known-agents list, so you address a cross-process peer exactly like an in-process one. A message that ' +
    'arrived from another session is marked (delivered as a <cross-session-message from=...>) so you know it ' +
    'crossed a session boundary.',
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
        // Widened to allow a structured OBJECT, not only a string, so the schema
        // advertises that typed control messages exist (schema-level discoverability
        // of the discriminated `type`/`approve` fields) and a cross-framework the reference
        // Code peer that passes `message` as a typed object is accepted. Parity with
        // the reference's SendMessage Input.message discriminated object.
        type: ['string', 'object'],
        description:
          'Alias for content — the message to send. Either plain text, or a structured ' +
          'control message as a discriminated object carrying a "type" field (e.g. ' +
          '"shutdown_request" / "plan_approval_response") plus its typed payload (e.g. ' +
          '"approve", "requestId", "reason"). A string is sent verbatim; an object is ' +
          'serialized to the JSON control message.',
      },
      from: {
        type: 'string',
        description: 'Deprecated: sender is derived from your trusted agent context. Only honored for the lead/main session.',
      },
      summary: {
        type: 'string',
        description: 'Short 5-10 word summary shown as a preview in the UI.',
      },
      status: {
        type: 'string',
        enum: ['normal', 'proactive'],
        description:
          "Urgency of a plain-text message. Use 'proactive' when surfacing something the " +
          "recipient has NOT asked for and needs to see now (a blocker, a completion while " +
          "they were away); use 'normal' (the default) when replying. A proactive message " +
          'drains ahead of ordinary peer chatter at the recipient. Ignored for structured ' +
          'control messages.',
      },
    },
    required: ['to', 'content'],
  },
  isReadOnly: false,
  // PARITY (additive): per-invocation read-only verdict. A plain-text chat message
  // mutates nothing (it just queues text), so it is read-only and auto-allows
  // without a permission prompt; a STRUCTURED control message (shutdown / plan /
  // permission / mode / task) changes a teammate's lifecycle or posture and stays
  // on the permission-asking path. Mirrors the reference's `isReadOnly(input) =>
  // typeof input.message === 'string'` (plain text = read-only), discriminating on
  // AMI's string-encoded content via the single canonical structured-message
  // detector. Consumed by tool-executor through isToolReadOnlyForInput.
  isReadOnlyForInput: (input: Record<string, unknown>): boolean => {
    // Normalize a string OR a structured object through the single canonical coercion
    // so an object-form control message is correctly classified non-read-only (it
    // mutates a teammate's lifecycle/posture) instead of being mis-read as read-only.
    const c = coerceMessageContent(input);
    if (typeof c !== 'string') return true;
    return !isStructuredProtocolMessage(c);
  },

  async execute(input, context: ToolContext): Promise<ToolResult> {
    let toRaw = input.to as string;
    // Normalize the payload once: a string is used verbatim, a structured object
    // (the reference's discriminated Input.message) is serialized to the JSON content
    // the structured-message parser already routes — the single canonical path for
    // both forms, so an object can never reach a `.trim()` on a non-string below.
    const content = coerceMessageContent(input) as string;

    if (!toRaw || !toRaw.trim()) {
      return validationError('"to" must not be empty.');
    }
    // Normalize the recipient name: a model frequently emits a padded name
    // (" worker ", "worker\n") copied from prose/UI. Without trimming, the
    // lookup (getAgentByName / notifier / mailbox key) misses the real agent and
    // the message silently lands in a dead " worker " mailbox nobody drains —
    // and " * " is not recognized as broadcast. Trim once, up front, so every
    // downstream resolution, broadcast check, and display uses the clean name.
    // (Mirrors the reference normalizing the teammate name before resolution.)
    toRaw = toRaw.trim();
    if (!content || !content.trim()) {
      return validationError('"content" must not be empty.');
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
    // out-of-process. (Mirrors the reference's parseAddress.)
    const addr = parseAddress(toRaw);
    if (addr.scheme !== 'other' && !addr.target.trim()) {
      return validationError('address target must not be empty.');
    }
    // Bare teammate names only — there is one team per session, so "@" or
    // "team@host" style addresses are rejected (mirrors the reference's validateInput).
    // Scheme-qualified addresses are exempt: they use the ":" separator, not "@".
    if (addr.scheme === 'other' && toRaw.includes('@')) {
      return validationError('"to" must be a bare teammate name or "*" — do not use "@".');
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
        // Cross-session delivery is a distinct transport from the in-session
        // mailbox; emit the same gated lifecycle trace so an out-of-process
        // delivery is observable alongside in-session ones (off by default).
        traceMailbox('deliver', { to: addr.target, from, via: 'uds' });
        return { output: `Message delivered to local peer "${toRaw}"${sTag} [from: ${from}] (${content.length} chars)` };
      } catch (e) {
        traceMailbox('cross-session delivery failed', { to: addr.target, from, scheme: addr.scheme });
        return { output: `Error: failed to deliver to local peer "${toRaw}": ${(e as Error).message}`, isError: true };
      }
    }

    // Self-send guard: addressing your own identity is a no-op that would echo a
    // message into your own mailbox (and, for a sub-agent with a live notifier,
    // risk a self-notify/poll loop). Reject it. The sender may be addressed by
    // its trusted name OR its raw agentId, and the recipient resolves to either
    // form, so compare the target against the FULL self-identity set rather than
    // a single string. Broadcast ("*") is never a self-send. (Mirrors the reference
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
        // Carry the honest typed receipt even on the hard reject: a `failed` agent is
        // a `failed-turn`, a `killed`/`completed` one is `unreachable`.
        const termHold: DeliveryHold = status === 'failed'
          ? { kind: 'failed-turn' }
          : { kind: 'unreachable', recipient: toRaw };
        return {
          output: `Error: cannot send to "${toRaw}" — that agent has ${status} and is no longer running.`,
          isError: true,
          metadata: { deliveryHold: termHold },
        };
      }
      // Composite-liveness boundary (parity with the reference's InProcessBackend
      // isActive, surfaced by ProcessManager.isActive): a TRACKED agent may still
      // read `running` while it is actively SHUTTING DOWN — it was signalled to
      // abort (its AbortSignal fired) but its status has not flipped to killed/failed
      // yet. The coarse terminal check above misses that window, so a message would
      // queue into a mailbox the aborting engine will never drain — a silent orphan
      // and an invisible coordination failure for the sender. When the manager
      // exposes the composite isActive() query, refuse a running-but-inactive target
      // too. Scoped to a currently-`running` tracked task: an UNTRACKED name
      // (undefined) is left to the unreachable-warning path (it may be coming online),
      // and a `pending` task is still allowed to queue (it is about to start).
      if (
        status === 'running' &&
        typeof context.processManager.isActive === 'function' &&
        !context.processManager.isActive(targetTaskId)
      ) {
        return {
          output: `Error: cannot send to "${toRaw}" — that agent is shutting down (abort in progress) and is no longer accepting messages.`,
          isError: true,
          metadata: { deliveryHold: { kind: 'blocked', reason: 'shutting down' } satisfies DeliveryHold },
        };
      }
    }

    // --- Structured control messages -------------------------------------
    const structured = parseStructuredMessage(content);
    if (structured && 'error' in structured) {
      return { output: `Error: ${structured.error}`, isError: true };
    }
    if (structured) {
      // A team_permission_update is the ONE structured message that is a
      // team-wide decision, so (lead-only) it may broadcast; every other
      // structured message is a point-to-point control message and may not.
      if (toRaw === '*' && structured.type !== 'team_permission_update') {
        return { output: 'Error: structured messages cannot be broadcast (to: "*").', isError: true };
      }
      switch (structured.type) {
        case 'shutdown_request': {
          // Idempotent handshake: a shutdown_request to a teammate that already has
          // one outstanding is a NO-OP — do not queue a duplicate or restart the
          // negotiation. Mirrors the reference's InProcessBackend.terminate() early
          // return on an already-set `shutdownRequested` flag. The flag is cleared
          // when the teammate emits its shutdown_response (see that case), on
          // purge/reset.
          const existing = outstandingShutdowns.get(toRaw);
          if (existing) {
            traceMailbox('shutdown skipped (already requested)', { to: toRaw, id: existing });
            return { output: `Shutdown already requested for ${toRaw} (id: ${existing}) — not re-sent (idempotent).` };
          }
          // Auto-generate a structured request id when the model omits one, and
          // stamp the trusted sender as `from` so the payload is self-contained;
          // rewrite the queued payload so the recipient always receives both an id
          // to echo back (mirrors the reference's generateRequestId) and the requester.
          const reqId = structured.requestId || generateRequestId('shutdown', toRaw);
          const wire = (structured.requestId && structured.from === from)
            ? content
            : JSON.stringify({ ...structured, requestId: reqId, from });
          enqueue(to, { from, to, content: wire, timestamp: Date.now() });
          // Open the handshake: record the outstanding request keyed by the bare
          // teammate name so a re-send is guarded until the teammate responds.
          outstandingShutdowns.set(toRaw, reqId);
          traceMailbox('shutdown requested', { to: toRaw, id: reqId, from });
          return { output: `Shutdown request sent to ${toRaw} (id: ${reqId}) [from: ${from}]`, metadata: { ...requestCorrelation(reqId, toRaw) } };
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
          // Resolve the handshake: whether the teammate approves or rejects, the
          // outstanding shutdown-request for this responder is closed, so the lead
          // may open a fresh one later (a rejection explicitly invites "request
          // again later"). Keyed by the responder's own trusted identity.
          outstandingShutdowns.delete(from);
          enqueue(TEAM_LEAD_NAME, { from, to: TEAM_LEAD_NAME, content, timestamp: Date.now() });
          if (structured.approve) {
            // Approving a shutdown TERMINATES the approving teammate: abort its own
            // task so the engine loop actually stops, matching the reference's
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
              metadata: { ...responseCorrelation(structured.requestId, TEAM_LEAD_NAME) },
            };
          }
          return { output: `Shutdown rejected (id: ${structured.requestId})${originTag}: "${structured.reason}" — ${from} continues working.`, metadata: { ...responseCorrelation(structured.requestId, TEAM_LEAD_NAME) } };
        }
        case 'plan_approval_request': {
          // The request half: a teammate submits a plan to another agent (usually
          // the lead) for approval. One-way queue like shutdown_request; the
          // reviewer replies with a plan_approval_response.
          const reqId = structured.requestId || generateRequestId('plan', toRaw);
          const wire = structured.requestId ? content : JSON.stringify({ ...structured, requestId: reqId });
          enqueue(to, { from, to, content: wire, timestamp: Date.now() });
          // Open the blocked-on-approval boundary: the submitting teammate is now
          // awaiting the lead's decision, keyed by its trusted identity so the
          // matching plan_approval_response (addressed back to it) resolves it.
          awaitingPlanApproval.set(from, reqId);
          const fileTag = structured.planFilePath ? ` (${structured.planFilePath})` : '';
          return { output: `Plan submitted to ${toRaw} for approval (id: ${reqId})${fileTag} [from: ${from}]`, metadata: { ...requestCorrelation(reqId, toRaw) } };
        }
        case 'plan_approval_response': {
          // Authorization: only the team lead may approve or reject plans.
          if (!isSenderLead) {
            return { output: 'Error: only the team lead can approve or reject plans.', isError: true };
          }
          const originTag = requestOriginTag(structured.requestId);
          enqueue(to, { from, to, content, timestamp: Date.now() });
          // Resolve the blocked-on-approval boundary: the teammate the lead just
          // answered is unblocked whether the plan was approved (proceed) or
          // rejected (revise). Clear by the addressed bare name AND the resolved
          // recipient key so it lines up with however the submit stamped it.
          awaitingPlanApproval.delete(toRaw);
          if (to !== toRaw) awaitingPlanApproval.delete(to);
          if (structured.approve) {
            // The granted permission mode (if any) is surfaced so the lead sees
            // what posture the teammate will inherit on exiting plan-mode.
            const modeTag = structured.permissionMode ? ` (mode: ${structured.permissionMode})` : '';
            return { output: `Plan approved for ${toRaw} (id: ${structured.requestId})${originTag}${modeTag} [from: ${from}]`, metadata: { ...responseCorrelation(structured.requestId, toRaw) } };
          }
          // Rejection always carries actionable guidance, defaulting when the lead
          // gave no explicit feedback (mirrors the reference's 'Please revise your plan').
          const guidance = structured.feedback || 'Please revise your plan';
          return { output: `Plan rejected for ${toRaw} (id: ${structured.requestId})${originTag} [from: ${from}] — ${guidance}; revise and call ExitPlanMode again`, metadata: { ...responseCorrelation(structured.requestId, toRaw) } };
        }
        case 'task_assignment': {
          // A lead assigns work to a named teammate. assignedBy is stamped from the
          // trusted sender (not model-supplied) so provenance cannot be spoofed.
          const assignedBy = from;
          const wire = JSON.stringify({ ...structured, assignedBy, description: structured.description ?? '' });
          enqueue(to, { from, to, content: wire, timestamp: Date.now() });
          // Track the open assignment (keyed by the addressed teammate) so a later
          // termination can report it as orphaned/unassigned.
          recordTaskAssignment(toRaw, structured.taskId, structured.subject);
          return { output: `Task #${structured.taskId} "${structured.subject}" assigned to ${toRaw} [from: ${from}]` };
        }
        case 'idle_notification': {
          // A teammate reports it is idle; always surfaced to the lead so it can
          // reassign work. Routed to the canonical lead mailbox.
          if (toRaw !== TEAM_LEAD_NAME) {
            return { output: `Error: idle_notification must be sent to "${TEAM_LEAD_NAME}".`, isError: true };
          }
          // Auto-derive the idle summary from the teammate's LAST peer DM when the
          // model omitted one, so the lead sees what the teammate was last
          // coordinating on without the teammate restating it. Mirrors the reference
          // Code feeding getLastPeerDmSummary into the idle_notification summary.
          let wire = content;
          let derivedTag = '';
          if (!structured.summary) {
            const lastDm = getLastPeerDmSummary(from);
            if (lastDm) {
              wire = JSON.stringify({ ...structured, summary: lastDm });
              derivedTag = ` (summary: ${lastDm})`;
            }
          }
          enqueue(TEAM_LEAD_NAME, { from, to: TEAM_LEAD_NAME, content: wire, timestamp: Date.now() });
          // A reported completed task is no longer open for this teammate — close it
          // so the termination summary never lists already-finished work as orphaned.
          if (structured.completedTaskId) clearTaskAssignment(from, structured.completedTaskId);
          const reasonTag = structured.idleReason ? ` (${structured.idleReason})` : '';
          return { output: `Idle notification sent to ${TEAM_LEAD_NAME}${reasonTag}${derivedTag} [from: ${from}]` };
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
          // Privilege separation (parity: the reference's WorkerPendingPermission — a
          // worker cannot self-approve a tool; its request is held pending and routed
          // UP to the human-supervised lead). Only a non-lead WORKER may originate a
          // permission_request: the lead is the approver, so a lead forwarding a
          // permission prompt to itself has no one to escalate to and is rejected.
          if (isSenderLead) {
            return { output: 'Error: the team lead cannot forward a permission request — the lead is the approver. Only a worker escalates a permission prompt to the lead.', isError: true };
          }
          // A worker forwards a permission prompt up to the lead for a decision.
          // The request id is keyed on the requester (from): the lead's response
          // routes back to that worker.
          const reqId = structured.requestId || generateRequestId('permission', from);
          const wire = structured.requestId ? content : JSON.stringify({ ...structured, requestId: reqId });
          enqueue(TEAM_LEAD_NAME, { from, to: TEAM_LEAD_NAME, content: wire, timestamp: Date.now() });
          return { output: `Permission request for "${structured.tool}" forwarded to ${TEAM_LEAD_NAME} (id: ${reqId}) [from: ${from}]`, metadata: { ...requestCorrelation(reqId, TEAM_LEAD_NAME) } };
        }
        case 'permission_response': {
          // Only the lead may answer a forwarded permission request.
          if (!isSenderLead) {
            return { output: 'Error: only the team lead can answer a permission request.', isError: true };
          }
          const originTag = requestOriginTag(structured.requestId);
          enqueue(to, { from, to, content, timestamp: Date.now() });
          // Surface the resolver's feedback (on a denial) and whether it edited the
          // worker's tool input, so the lead's confirmation reflects the full
          // decision the worker will act on (mirrors the reference's feedback/updatedInput).
          const editedTag = structured.updatedInput ? ' with edited input' : '';
          const feedbackTag = !structured.approve && structured.feedback ? `: ${structured.feedback}` : '';
          return { output: `Permission ${structured.approve ? 'granted' : 'denied'}${editedTag} to ${toRaw} (id: ${structured.requestId})${originTag}${feedbackTag}`, metadata: { ...responseCorrelation(structured.requestId, toRaw) } };
        }
        case 'sandbox_permission_request': {
          // A worker whose sandbox blocked egress to a host asks the lead to
          // approve it. Always routed to the lead; the request id is keyed on the
          // requester so the lead's sandbox_permission_response routes back.
          const reqId = structured.requestId || generateRequestId('sandbox', from);
          const wire = structured.requestId ? content : JSON.stringify({ ...structured, requestId: reqId });
          enqueue(TEAM_LEAD_NAME, { from, to: TEAM_LEAD_NAME, content: wire, timestamp: Date.now() });
          return { output: `Sandbox network request for host "${structured.host}" forwarded to ${TEAM_LEAD_NAME} (id: ${reqId}) [from: ${from}]`, metadata: { ...requestCorrelation(reqId, TEAM_LEAD_NAME) } };
        }
        case 'sandbox_permission_response': {
          // Only the lead may approve/deny a host-egress request; routed back to
          // the named worker which resumes (or aborts) the blocked connection.
          if (!isSenderLead) {
            return { output: 'Error: only the team lead can answer a sandbox network request.', isError: true };
          }
          const originTag = requestOriginTag(structured.requestId);
          enqueue(to, { from, to, content, timestamp: Date.now() });
          return { output: `Sandbox access to "${structured.host}" ${structured.allow ? 'allowed' : 'denied'} for ${toRaw} (id: ${structured.requestId})${originTag}`, metadata: { ...responseCorrelation(structured.requestId, toRaw) } };
        }
        case 'team_permission_update': {
          // A team-wide permission decision. Lead-only; may be directed to one
          // teammate or broadcast ("*") to the whole roster.
          if (!isSenderLead) {
            return { output: 'Error: only the team lead can broadcast a team permission update.', isError: true };
          }
          if (toRaw === '*') {
            const fromLc = from.toLowerCase();
            const roster = new Set<string>();
            for (const name of mailboxes.keys()) roster.add(name);
            for (const name of mailboxNotifiers.keys()) roster.add(name);
            const recipients: string[] = [];
            for (const name of roster) {
              if (name.toLowerCase() === fromLc) continue;
              enqueue(name, { from, to: name, content, timestamp: Date.now() });
              recipients.push(name);
            }
            if (recipients.length === 0) {
              return { output: 'No teammates to broadcast the permission update to (no other agents are reachable).' };
            }
            return { output: `Team permission update (${structured.behavior} "${structured.toolName}") broadcast to ${recipients.length} agent(s): ${recipients.join(', ')}` };
          }
          enqueue(to, { from, to, content, timestamp: Date.now() });
          return { output: `Team permission update (${structured.behavior} "${structured.toolName}") sent to ${toRaw} [from: ${from}]` };
        }
        case 'teammate_terminated': {
          // A lifecycle notice that a peer has exited/been killed, routed to the
          // lead so it can release the dead agent's work. Point-to-point (the
          // broadcast guard above already rejects "*"); surfaced silently by the
          // receive-side relay (never dumped as chatter).
          if (toRaw !== TEAM_LEAD_NAME) {
            return { output: `Error: teammate_terminated must be sent to "${TEAM_LEAD_NAME}".`, isError: true };
          }
          enqueue(TEAM_LEAD_NAME, { from, to: TEAM_LEAD_NAME, content, timestamp: Date.now() });
          const note = structured.message ? ` — ${structured.message}` : '';
          return { output: `Teammate termination notice sent to ${TEAM_LEAD_NAME} [from: ${from}]${note}` };
        }
      }
    }

    // --- Plain text ------------------------------------------------------
    const summary = input.summary as string | undefined;
    // Urgency discriminator (plain text only). A present value must be valid — a bad
    // value is rejected with a self-correcting message rather than silently coerced,
    // mirroring the parser's treatment of the structured enums. Absent => normal.
    let status: MessageStatus | undefined;
    if (input.status !== undefined) {
      if (typeof input.status !== 'string' || !MESSAGE_STATUSES.has(input.status as MessageStatus)) {
        return validationError(`"status" must be one of: ${[...MESSAGE_STATUSES].join(', ')}.`);
      }
      // Only `proactive` is carried (normal is the default — no need to stamp it).
      if (input.status === 'proactive') status = 'proactive';
    }
    // Every plain-text message must carry a short UI preview (the reference's summary
    // contract). The model's summary wins; when it omits one we DEFAULT a derived
    // preview onto the stored message so the UI / idle-summary path always has one
    // (AMI superset: default, not a hard reject that would break plain-text relay).
    const effectiveSummary = (summary && summary.trim()) ? summary.trim() : deriveSummary(content);
    const msg: AgentMessage = {
      from,
      to,
      content,
      summary: effectiveSummary || undefined,
      timestamp: Date.now(),
      ...(status ? { status } : {}),
    };

    if (toRaw === '*') {
      // Broadcast to the full LIVE roster, not just agents that already own a
      // mailbox: a teammate that registered an eager-delivery notifier on spawn
      // but has not yet been messaged must still receive a broadcast (mirrors
      // the reference enumerating the team-file members, reaching not-yet-contacted
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
      traceMailbox('broadcast', { from, recipients: recipients.length });
      return {
        output: `Broadcast sent to ${recipients.length} agent(s): ${recipients.join(', ')}`,
        ...(status ? { metadata: { status } } : {}),
      };
    }

    // Dead-recipient boundary: if other agents are live this session (a live
    // notifier) OR a team roster is established but the target isn't reachable,
    // it's almost certainly a typo/stale name. Warn — and name the KNOWN agents so
    // the model can self-correct — while still queuing (in case it comes online)
    // rather than failing silently or hard-rejecting a teammate that is spawning.
    // Mirrors the reference's corrective "known agents" affordance (PaneBackendExecutor)
    // without regressing AMI's deliberate queue-to-coming-online superset behavior.
    // Computed BEFORE enqueue, which would otherwise create the mailbox.
    const hasRoster = rosterMemberNames().length > 0;
    let unreachableWarn = '';
    let deliveryWarning: DeliveryWarning | undefined;
    if (!isReachable(to) && (mailboxNotifiers.size > 0 || hasRoster)) {
      const known = knownRecipientNames(trustedSelf ?? from);
      const knownTag = known.length > 0 ? ` Known agents: ${known.join(', ')}.` : '';
      unreachableWarn = ` [warning: "${toRaw}" is not a known/live agent — check the name.${knownTag}]`;
      // Typed discriminant alongside the human warning: a roster-established-but-
      // not-a-member target is an `unknown_recipient`; without a roster (only live
      // peers) it is a `no_team_context`. Mirrors the reference's DirectMessageResult.error.
      deliveryWarning = hasRoster ? 'unknown_recipient' : 'no_team_context';
    }

    // Honest delivery-hold receipt: read the SAME gates just enforced and, when the
    // recipient is not settled, capture a typed {@link DeliveryHold} naming WHY the
    // message waits (busy / blocked / detached / failed-turn / unreachable). Null
    // means it will be delivered to a listening recipient now. Computed BEFORE
    // enqueue — which creates the mailbox and would otherwise flip the recipient to
    // `detached` — so the receipt reflects the state at SEND time.
    const hold = readDeliveryHold(toRaw, to, context);

    enqueue(to, msg);

    // Record this plain-text DM so a later idle_notification can derive its
    // summary from the sender's last PEER DM (getLastPeerDmSummary). Use the bare
    // target name (toRaw) the sender addressed, and the effective preview (model
    // summary when given, else the derived one) — mirrors the reference's
    // `summary ?? message.slice(0, 80)`.
    recordOutboundDm(from, toRaw, effectiveSummary || content.slice(0, 80));

    const summaryTag = summary ? ` — "${summary}"` : '';
    const affordance = deliveryAffordance(toRaw, to, context);
    // Surface the typed delivery-warning discriminant and/or the proactive urgency on
    // the structured metadata channel (the same programmatic contract errorCode rides
    // on) so a frontend can special-case either without re-parsing the output string.
    const meta: Record<string, unknown> = {};
    if (deliveryWarning) meta.deliveryWarning = deliveryWarning;
    if (status) meta.status = status;
    if (hold) meta.deliveryHold = hold;
    return {
      output: `Message sent to "${toRaw}"${summaryTag} [from: ${from}] (${content.length} chars)${affordance}${unreachableWarn}`,
      ...(Object.keys(meta).length > 0 ? { metadata: meta } : {}),
    };
  },
};
