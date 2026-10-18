import type { Message, AssistantMessage } from './types';

export function countToolUses(messages: Message[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const msg of messages) {
    if (msg.role === 'assistant' && (msg as AssistantMessage).tool_calls) {
      for (const tc of (msg as AssistantMessage).tool_calls!) {
        counts.set(tc.function.name, (counts.get(tc.function.name) ?? 0) + 1);
      }
    }
  }
  return counts;
}

const FILE_MUTATING_TOOLS = new Set(['file_edit', 'file_write', 'notebook_edit', 'multi_edit']);

export function extractFilesModified(messages: Message[]): Set<string> {
  const files = new Set<string>();
  for (const msg of messages) {
    if (msg.role === 'assistant' && (msg as AssistantMessage).tool_calls) {
      for (const tc of (msg as AssistantMessage).tool_calls!) {
        if (FILE_MUTATING_TOOLS.has(tc.function.name)) {
          try {
            const args = JSON.parse(tc.function.arguments);
            if (args.file_path) files.add(args.file_path);
          } catch {}
        }
      }
    }
  }
  return files;
}

export interface ConversationMeta {
  userTurnCount: number;
  assistantTurnCount: number;
  totalToolCalls: number;
  toolUseCounts: Map<string, number>;
  filesModified: Set<string>;
}

export function extractConversationMeta(messages: Message[]): ConversationMeta {
  let userTurnCount = 0;
  let assistantTurnCount = 0;
  let totalToolCalls = 0;

  for (const msg of messages) {
    if (msg.role === 'user') userTurnCount++;
    if (msg.role === 'assistant') {
      assistantTurnCount++;
      if ((msg as AssistantMessage).tool_calls) {
        totalToolCalls += (msg as AssistantMessage).tool_calls!.length;
      }
    }
  }

  return {
    userTurnCount,
    assistantTurnCount,
    totalToolCalls,
    toolUseCounts: countToolUses(messages),
    filesModified: extractFilesModified(messages),
  };
}

export function getConversationDuration(messages: Message[]): number {
  if (messages.length < 2) return 0;
  const first = (messages[0] as any)._timestamp;
  const last = (messages[messages.length - 1] as any)._timestamp;
  if (!first || !last) return 0;
  return last - first;
}
