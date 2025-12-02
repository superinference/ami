/**
 * Session Memory Extraction — extracts structured facts from tool results
 * and recent conversation context for cross-turn memory persistence.
 */

import type { ProviderConfig, Message } from './types';
import { streamChatCompletion } from './provider';
import { log as coreLog } from './logger';

export interface SessionFact {
  fact: string;
  category: 'decision' | 'file_modification' | 'user_preference' | 'error_fix' | 'convention';
  confidence: number;
}

const EXTRACTION_PROMPT = `Extract key facts from these tool results that would be useful to remember across conversation turns. Return a JSON array of objects with the following shape:

[
  {
    "fact": "concise description of the fact",
    "category": "decision" | "file_modification" | "user_preference" | "error_fix" | "convention",
    "confidence": 0.0 to 1.0
  }
]

Categories:
- decision: architectural or design decisions made during the conversation
- file_modification: files created, modified, or deleted and why
- user_preference: user preferences about coding style, tools, workflows
- error_fix: errors encountered and how they were resolved
- convention: project conventions or patterns discovered

Only include facts that would be genuinely useful in future turns. Be concise. Return ONLY the JSON array, no other text.`;

export class SessionMemoryExtractor {
  private provider: ProviderConfig;
  private compactionModel?: string;

  constructor(provider: ProviderConfig, compactionModel?: string) {
    this.provider = provider;
    this.compactionModel = compactionModel;
  }

  /**
   * Extract structured facts from tool results and recent messages.
   * Uses the compaction model if provided, otherwise the primary provider model.
   * Only includes the last 4 messages as context.
   */
  async extractFacts(
    toolResults: Array<{ toolName: string; output: string; isError: boolean }>,
    recentMessages: Message[],
  ): Promise<SessionFact[]> {
    // Only use the last 4 messages
    const contextMessages = recentMessages.slice(-4);

    // Build the user prompt with tool results and context
    const toolResultsSummary = toolResults
      .map(r => `[${r.toolName}]${r.isError ? ' (ERROR)' : ''}: ${r.output.slice(0, 500)}`)
      .join('\n\n');

    const messageSummary = contextMessages
      .map(m => {
        const content = typeof m.content === 'string' ? m.content : JSON.stringify(m.content);
        return `[${m.role}]: ${content?.slice(0, 300) ?? ''}`;
      })
      .join('\n');

    const userContent = `## Tool Results\n${toolResultsSummary}\n\n## Recent Conversation\n${messageSummary}`;

    // Use compaction model if available, otherwise primary model
    const effectiveConfig: ProviderConfig = this.compactionModel
      ? { ...this.provider, model: this.compactionModel }
      : this.provider;

    const messages: Message[] = [
      { role: 'system', content: EXTRACTION_PROMPT },
      { role: 'user', content: userContent },
    ];

    const abortController = new AbortController();
    // Set a timeout to avoid hanging
    const timeout = setTimeout(() => abortController.abort(), 30_000);

    try {
      let responseText = '';

      for await (const chunk of streamChatCompletion(
        { ...effectiveConfig, maxTokens: 500 },
        messages,
        [], // no tools
        abortController.signal,
      )) {
        if (chunk.type === 'content_delta' && chunk.text) {
          responseText += chunk.text;
        }
        if (chunk.type === 'error') {
          coreLog('session-memory', 'Extraction error', { error: chunk.error ?? 'unknown' });
          return [];
        }
      }

      return this.parseFactsFromResponse(responseText);
    } catch (err) {
      coreLog('session-memory', 'Failed to extract facts', { error: String(err) });
      return [];
    } finally {
      clearTimeout(timeout);
    }
  }

  /**
   * Merge new facts into existing facts, deduplicating by content similarity.
   * Higher-confidence facts take precedence when duplicates are found.
   */
  mergeFacts(existing: SessionFact[], newFacts: SessionFact[]): SessionFact[] {
    const merged = [...existing];

    for (const newFact of newFacts) {
      const duplicateIndex = merged.findIndex(
        e => e.fact.toLowerCase() === newFact.fact.toLowerCase(),
      );

      if (duplicateIndex >= 0) {
        // Keep the higher-confidence version
        if (newFact.confidence > merged[duplicateIndex].confidence) {
          merged[duplicateIndex] = newFact;
        }
      } else {
        merged.push(newFact);
      }
    }

    return merged;
  }

  /**
   * Parse JSON facts from LLM response, filtering by confidence threshold.
   */
  private parseFactsFromResponse(response: string): SessionFact[] {
    try {
      // Try to extract JSON array from the response (handle markdown code blocks)
      let jsonStr = response.trim();
      const jsonMatch = jsonStr.match(/```(?:json)?\s*([\s\S]*?)```/);
      if (jsonMatch) {
        jsonStr = jsonMatch[1].trim();
      }

      const parsed = JSON.parse(jsonStr);
      if (!Array.isArray(parsed)) {
        return [];
      }

      const validCategories = new Set(['decision', 'file_modification', 'user_preference', 'error_fix', 'convention']);

      return parsed
        .filter((item: unknown): item is SessionFact => {
          if (typeof item !== 'object' || item === null) return false;
          const obj = item as Record<string, unknown>;
          return (
            typeof obj.fact === 'string' &&
            typeof obj.category === 'string' &&
            validCategories.has(obj.category) &&
            typeof obj.confidence === 'number' &&
            obj.confidence >= 0.7
          );
        });
    } catch {
      coreLog('session-memory', 'Failed to parse facts JSON from response');
      return [];
    }
  }
}
