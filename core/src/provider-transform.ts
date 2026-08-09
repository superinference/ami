import type { Message, ToolDefinition } from './types';

export function sanitizeToolCallIds(
  messages: Message[],
  provider: string,
): Message[] {
  if (provider !== 'anthropic') return messages;
  // Anthropic requires alphanumeric-only tool call IDs
  return messages.map(msg => {
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
      return {
        ...msg,
        tool_call_id: msg.tool_call_id.replace(/[^a-zA-Z0-9_-]/g, '') || 'tc_fallback',
      };
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

export function healOrphanedToolCalls(messages: Message[]): Message[] {
  const result: Message[] = [];
  const answeredIds = new Set<string>();

  for (const msg of messages) {
    if (msg.role === 'tool') {
      answeredIds.add(msg.tool_call_id);
    }
  }

  for (const msg of messages) {
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

  return result;
}
