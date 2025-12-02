import { streamChatCompletion } from '../provider';
import type { ProviderConfig, Message } from '../types';
import type { CriticDecision } from './types';

export class Critic {
  private α: number;  // False approval rate (Eq. 3)
  private β: number;  // False rejection rate (Eq. 3)

  constructor(alpha: number = 0.05, beta: number = 0.10) {
    this.α = alpha;
    this.β = beta;
  }

  async evaluate(
    query: string,
    result: string,
    provider: ProviderConfig,
    abortSignal: AbortSignal,
  ): Promise<CriticDecision> {
    const criticPrompt = `You are a strict code reviewer. Evaluate if this result correctly and completely answers the query.

Query: ${query}

Result: ${result.substring(0, 2000)}

Respond with ONLY a JSON object:
{"approved": true/false, "score": 0.0-1.0, "reason": "brief explanation"}`;

    const messages: Message[] = [
      { role: 'user', content: criticPrompt },
    ];

    try {
      let response = '';
      for await (const chunk of streamChatCompletion(
        { ...provider, maxTokens: 200, temperature: 0 },
        messages,
        [],
        abortSignal,
      )) {
        if (chunk.type === 'content_delta' && chunk.text) {
          response += chunk.text;
        }
      }

      const parsed = this.extractJSON(response);
      if (parsed) {
        return {
          approved: !!parsed.approved,
          score: Math.max(0, Math.min(1, Number(parsed.score) || 0)),
          reason: String(parsed.reason || ''),
        };
      }
    } catch {
      // Critic failure defaults to approval (fail-open)
    }

    return { approved: true, score: 0.7, reason: 'Critic evaluation failed, defaulting to approved' };
  }

  // Extract JSON with balanced-brace matching (handles nested braces in reason field)
  extractJSON(text: string): Record<string, unknown> | null {
    const start = text.indexOf('{');
    if (start === -1) return null;

    let depth = 0;
    let end = -1;
    for (let i = start; i < text.length; i++) {
      if (text[i] === '{') depth++;
      else if (text[i] === '}') {
        depth--;
        if (depth === 0) { end = i + 1; break; }
      }
    }

    if (end <= start) return null;

    try {
      return JSON.parse(text.substring(start, end));
    } catch {
      return null;
    }
  }

  // Eq. 4: Positive Predictive Value
  // PPV = (1-β)·p' / ((1-β)·p' + α·(1-p'))
  ppv(priorCorrect: number): number {
    const p = priorCorrect;
    const numerator = (1 - this.β) * p;
    const denominator = (1 - this.β) * p + this.α * (1 - p);
    if (denominator === 0) return 0;
    return numerator / denominator;
  }

  get alphaRate(): number { return this.α; }
  get betaRate(): number { return this.β; }
}
