import type { ToolDefinition, ToolResult, ToolContext } from '../types';

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
    },
    required: ['reason'],
  },
  isReadOnly: true,

  async execute(
    input: Record<string, unknown>,
    _context: ToolContext,
  ): Promise<ToolResult> {
    const reason = (input.reason as string) || 'Policy violation';
    return {
      output: `Conversation ended: ${reason}`,
      metadata: { endConversation: true, reason },
    };
  },
};
