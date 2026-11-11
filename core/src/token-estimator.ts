import type { Message } from './types';
// Single source of truth for the ~4-chars/token heuristic (dedup: audit #16). Re-export
// it here too so existing importers of estimateTokens from this module keep working.
import { estimateTokens } from './utils/tokens';
export { estimateTokens } from './utils/tokens';

const OVERHEAD_PER_MESSAGE = 4; // role header, delimiters

export function estimateToolCallTokens(
  toolName: string,
  input: Record<string, unknown>,
): number {
  const serialized = JSON.stringify(input);
  return estimateTokens(toolName) + estimateTokens(serialized) + 10; // tool call framing
}

export function estimateMessageTokens(message: Message | { role: string; content: unknown }): number {
  let textContent = '';
  if (typeof message.content === 'string') {
    textContent = message.content;
  } else if (message.content === null || message.content === undefined) {
    return OVERHEAD_PER_MESSAGE;
  } else if (Array.isArray(message.content)) {
    for (const part of message.content) {
      if (part && typeof part === 'object' && 'text' in part && typeof part.text === 'string') {
        textContent += part.text;
      }
    }
  }
  return estimateTokens(textContent) + OVERHEAD_PER_MESSAGE;
}

export function estimateConversationTokens(messages: Array<Message | { role: string; content: unknown }>): number {
  let total = 3; // conversation framing
  for (const msg of messages) {
    total += estimateMessageTokens(msg);
  }
  return total;
}
