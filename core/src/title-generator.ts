import type { ProviderConfig } from './types';
import { streamChatCompletion } from './provider';

export async function generateTitle(
  userMessage: string,
  provider: ProviderConfig,
  compactionModel?: string,
): Promise<string> {
  const titleProvider: ProviderConfig = {
    ...provider,
    model: compactionModel || provider.model,
    maxTokens: 50,
  };

  const messages = [{
    role: 'user' as const,
    content: `Generate a concise title (max 50 chars, single line, no quotes, same language as the text) for this conversation:\n\n"${userMessage.slice(0, 500)}"`,
  }];

  let title = '';
  try {
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), 30000);
    try {
      for await (const chunk of streamChatCompletion(titleProvider, messages, [], ac.signal)) {
        if (chunk.type === 'content_delta' && chunk.text) title += chunk.text;
        if (chunk.type === 'done' || chunk.type === 'error') break;
      }
    } finally {
      clearTimeout(timer);
    }
  } catch {}

  // Clean up: remove quotes, thinking tags (paired and unclosed), truncate
  title = title.replace(/<think>[\s\S]*?<\/think>/g, '').replace(/<think>[\s\S]*$/g, '').replace(/^["']|["']$/g, '').trim();
  return title.slice(0, 50) || userMessage.slice(0, 50);
}
