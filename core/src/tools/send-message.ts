import { ToolDefinition, ToolResult, ToolContext } from '../types';

interface AgentMessage {
  from: string;
  to: string;
  content: string;
  summary?: string;
  timestamp: number;
}

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
  requestId: string;
}
export interface ShutdownResponse {
  type: 'shutdown_response';
  requestId: string;
  approve: boolean;
  reason?: string;
}
export interface PlanApprovalResponse {
  type: 'plan_approval_response';
  requestId: string;
  approve: boolean;
  feedback?: string;
}
export interface PermissionRequest {
  type: 'permission_request';
  requestId: string;
  tool: string;
  description: string;
}
export interface PermissionResponse {
  type: 'permission_response';
  requestId: string;
  approve: boolean;
}

export type StructuredMessage =
  | ShutdownRequest
  | ShutdownResponse
  | PlanApprovalResponse
  | PermissionRequest
  | PermissionResponse;

const STRUCTURED_TYPES = new Set([
  'shutdown_request', 'shutdown_response', 'plan_approval_response',
  'permission_request', 'permission_response',
]);

/** Providers sometimes serialize booleans as strings; accept both. */
function coerceApprove(v: unknown): boolean {
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
  switch (type) {
    case 'shutdown_request':
      if (typeof requestId !== 'string') return { error: 'shutdown_request requires a requestId' };
      return { type, requestId, reason: typeof o.reason === 'string' ? o.reason : undefined };
    case 'shutdown_response':
      if (typeof requestId !== 'string') return { error: 'shutdown_response requires a requestId' };
      return { type, requestId, approve: coerceApprove(o.approve), reason: typeof o.reason === 'string' ? o.reason : undefined };
    case 'plan_approval_response':
      if (typeof requestId !== 'string') return { error: 'plan_approval_response requires a requestId' };
      return { type, requestId, approve: coerceApprove(o.approve), feedback: typeof o.feedback === 'string' ? o.feedback : undefined };
    case 'permission_request':
      if (typeof requestId !== 'string' || typeof o.tool !== 'string') return { error: 'permission_request requires requestId and tool' };
      return { type, requestId, tool: o.tool, description: typeof o.description === 'string' ? o.description : '' };
    case 'permission_response':
      if (typeof requestId !== 'string') return { error: 'permission_response requires a requestId' };
      return { type, requestId, approve: coerceApprove(o.approve) };
    default:
      return null;
  }
}

const mailboxes = new Map<string, AgentMessage[]>();

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
}

export function pollMailbox(agentName: string): AgentMessage[] {
  const messages = mailboxes.get(agentName) || [];
  if (messages.length > 0) {
    mailboxes.set(agentName, []);
  }
  return messages;
}

function enqueue(to: string, msg: AgentMessage): void {
  const box = mailboxes.get(to) ?? [];
  box.push(msg);
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
  description:
    'Send a message to a named teammate agent, or "*" to broadcast to all. Plain text coordinates work; ' +
    'structured control messages (shutdown_request/shutdown_response, plan_approval_response, ' +
    'permission_request/permission_response) drive the swarm protocol. Messages queue in the recipient\'s ' +
    'mailbox and are delivered when it next checks — address teammates by their bare name (no "@"). ' +
    'Your sender identity is taken from your agent context, not from the "from" field.',
  inputSchema: {
    type: 'object',
    properties: {
      to: {
        type: 'string',
        description: 'Bare name of the target teammate, or "*" to broadcast. No "@" prefix.',
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

  async execute(input, context: ToolContext): Promise<ToolResult> {
    const toRaw = input.to as string;
    const content = (input.message as string) ?? (input.content as string);

    if (!toRaw || !toRaw.trim()) {
      return { output: 'Error: "to" must not be empty.', isError: true };
    }
    if (!content || !content.trim()) {
      return { output: 'Error: "content" must not be empty.', isError: true };
    }
    // Bare teammate names only — there is one team per session, so "@" or
    // "team@host" style addresses are rejected (mirrors Claude's validateInput).
    if (toRaw.includes('@')) {
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
          enqueue(to, { from, to, content, timestamp: Date.now() });
          return { output: `Shutdown request sent to ${toRaw} (id: ${structured.requestId}) [from: ${from}]` };
        }
        case 'shutdown_response': {
          // A shutdown decision is always reported back to the lead.
          if (toRaw !== TEAM_LEAD_NAME) {
            return { output: `Error: shutdown_response must be sent to "${TEAM_LEAD_NAME}".`, isError: true };
          }
          if (!structured.approve && (!structured.reason || !structured.reason.trim())) {
            return { output: 'Error: a reason is required when rejecting a shutdown request.', isError: true };
          }
          enqueue(TEAM_LEAD_NAME, { from, to: TEAM_LEAD_NAME, content, timestamp: Date.now() });
          if (structured.approve) {
            // The approving teammate terminates itself: fire its notifier so the
            // running engine can drain the approval and shut down gracefully.
            notifyMailbox(from);
            return { output: `Shutdown approved (id: ${structured.requestId}) — confirmation sent to ${TEAM_LEAD_NAME}; ${from} is exiting.` };
          }
          return { output: `Shutdown rejected (id: ${structured.requestId}): "${structured.reason}" — ${from} continues working.` };
        }
        case 'plan_approval_response': {
          // Authorization: only the team lead may approve or reject plans.
          if (!isSenderLead) {
            return { output: 'Error: only the team lead can approve or reject plans.', isError: true };
          }
          enqueue(to, { from, to, content, timestamp: Date.now() });
          return { output: `Plan ${structured.approve ? 'approved' : 'rejected'} for ${toRaw} (id: ${structured.requestId}) [from: ${from}]` };
        }
        case 'permission_request': {
          // A worker forwards a permission prompt up to the lead for a decision.
          enqueue(TEAM_LEAD_NAME, { from, to: TEAM_LEAD_NAME, content, timestamp: Date.now() });
          return { output: `Permission request for "${structured.tool}" forwarded to ${TEAM_LEAD_NAME} (id: ${structured.requestId}) [from: ${from}]` };
        }
        case 'permission_response': {
          // Only the lead may answer a forwarded permission request.
          if (!isSenderLead) {
            return { output: 'Error: only the team lead can answer a permission request.', isError: true };
          }
          enqueue(to, { from, to, content, timestamp: Date.now() });
          return { output: `Permission ${structured.approve ? 'granted' : 'denied'} to ${toRaw} (id: ${structured.requestId})` };
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
      let sent = 0;
      for (const [name, box] of mailboxes) {
        if (name !== from) { box.push(msg); notifyMailbox(name); sent++; }
      }
      return { output: `Broadcast sent to ${sent} agents.` };
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
