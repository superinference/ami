import type { Message } from './types';

const CHARS_PER_TOKEN = 4;
const OVERHEAD_PER_MESSAGE = 4; // role header, delimiters

export function estimateTokens(text: string): number {
  if (!text) return 0;
  return Math.ceil(text.length / CHARS_PER_TOKEN);
}

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
