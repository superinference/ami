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
    for await (const chunk of streamChatCompletion(titleProvider, messages, [], new AbortController().signal)) {
      if (chunk.type === 'content_delta' && chunk.text) title += chunk.text;
      if (chunk.type === 'done' || chunk.type === 'error') break;
    }
  } catch {}

  // Clean up: remove quotes, thinking tags, truncate
  title = title.replace(/<think>[\s\S]*?<\/think>/g, '').replace(/^["']|["']$/g, '').trim();
  return title.slice(0, 50) || userMessage.slice(0, 50);
}
