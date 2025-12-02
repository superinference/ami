import type { Message } from './types';

export function applyCacheControl(
  messages: { role: string; content: unknown }[],
  provider: string,
): void {
  if (provider !== 'anthropic') return;
  // Mark system messages for caching (stable across turns)
  let systemCount = 0;
  for (const msg of messages) {
    if (msg.role === 'system' && systemCount < 2) {
      (msg as any).providerOptions = { anthropic: { cacheControl: { type: 'ephemeral' } } };
      systemCount++;
    }
  }
  // Mark last 2 user messages for caching (most recent context)
  const userIndices: number[] = [];
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].role === 'user') {
      userIndices.push(i);
      if (userIndices.length >= 2) break;
    }
  }
  for (const idx of userIndices) {
    (messages[idx] as any).providerOptions = { anthropic: { cacheControl: { type: 'ephemeral' } } };
  }
}

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
        tool_calls: msg.tool_calls.map(tc => ({
          ...tc,
          id: tc.id.replace(/[^a-zA-Z0-9_-]/g, ''),
        })),
      };
    }
    if (msg.role === 'tool') {
      return {
        ...msg,
        tool_call_id: msg.tool_call_id.replace(/[^a-zA-Z0-9_-]/g, ''),
      };
    }
    return msg;
  });
}
