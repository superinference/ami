import { ToolDefinition, ToolResult } from '../types';

interface AgentMessage {
  from: string;
  to: string;
  content: string;
  summary?: string;
  timestamp: number;
}

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

const mailboxes = new Map<string, AgentMessage[]>();

export function getMailbox(agentName: string): AgentMessage[] {
  return mailboxes.get(agentName) || [];
}

export function clearMailbox(agentName: string): void {
  mailboxes.delete(agentName);
}

export function resetAllMailboxes(): void {
  mailboxes.clear();
}

export function pollMailbox(agentName: string): AgentMessage[] {
  const messages = mailboxes.get(agentName) || [];
  if (messages.length > 0) {
    mailboxes.set(agentName, []);
  }
  return messages;
}

export const sendMessageTool: ToolDefinition = {
  name: 'send_message',
  description:
    'Send a message to a named agent. Messages are queued in the target agent\'s mailbox and delivered when it next checks. ' +
    'Use to coordinate between agents in multi-agent workflows.',
  inputSchema: {
    type: 'object',
    properties: {
      to: {
        type: 'string',
        description: 'Name or ID of the target agent.',
      },
      content: {
        type: 'string',
        description: 'The message content to send.',
      },
      message: {
        type: 'string',
        description: 'Alias for content — the message to send.',
      },
      from: {
        type: 'string',
        description: 'Sender name (defaults to "main").',
      },
      summary: {
        type: 'string',
        description: 'Short summary for plain-text messages.',
      },
    },
    required: ['to', 'content'],
  },
  isReadOnly: false,

  async execute(input, _context): Promise<ToolResult> {
    const toRaw = input.to as string;
    const content = (input.message as string) ?? (input.content as string);
    const from = (input.from as string) || 'main';

    if (!toRaw || !toRaw.trim()) {
      return { output: 'Error: "to" must not be empty.', isError: true };
    }
    if (!content || !content.trim()) {
      return { output: 'Error: "content" must not be empty.', isError: true };
    }

    const { getAgentByName } = require('./task');
    const to = getAgentByName(toRaw) || toRaw;

    let parsedContent: any = null;
    try { parsedContent = JSON.parse(content); } catch {}
    if (parsedContent && parsedContent.type === 'shutdown_request') {
      const request = parsedContent as ShutdownRequest;
      const box = mailboxes.get(to) ?? [];
      box.push({ from, to, content, timestamp: Date.now() });
      mailboxes.set(to, box);
      const routeTarget = parsedContent.routeTo || to;
      return { output: `Shutdown request sent to ${routeTarget} (id: ${request.requestId}) [from: ${from}]` };
    }

    if (parsedContent && parsedContent.type === 'plan_approval_response') {
      const box = mailboxes.get(to) ?? [];
      box.push({ from, to, content: JSON.stringify(parsedContent), timestamp: Date.now() });
      mailboxes.set(to, box);
      return { output: `Plan ${parsedContent.approve ? 'approved' : 'rejected'} — sent to ${to}` };
    }

    const summary = input.summary as string | undefined;
    const msg: AgentMessage = {
      from,
      to,
      content,
      summary: summary || undefined,
      timestamp: Date.now(),
    };

    if (to === '*') {
      for (const [name, box] of mailboxes) {
        if (name !== from) box.push(msg);
      }
      return { output: `Broadcast sent to ${mailboxes.size - 1} agents.` };
    }

    if (!mailboxes.has(to)) {
      mailboxes.set(to, []);
    }
    mailboxes.get(to)!.push(msg);

    const summaryTag = summary ? ` — "${summary}"` : '';
    return {
      output: `Message sent to "${to}"${summaryTag} [from: ${from}] (${content.length} chars)`,
    };
  },
};
