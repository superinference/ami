import { streamChatCompletion } from '../provider';
import type { ProviderConfig, Message } from '../types';
import type { CriticDecision } from './types';

export class Critic {
  private α: number;  // False approval rate (Eq. 3)
  private β: number;  // False rejection rate (Eq. 3)
  // DI seam (test-only): the streaming function used to query the critic LLM.
  // Defaults to the real provider; overridable so tests can exercise the
  // fail-closed path when the stream throws without hitting the network.
  private streamFn: typeof streamChatCompletion;

  constructor(
    alpha: number = 0.05,
    beta: number = 0.10,
    streamFn: typeof streamChatCompletion = streamChatCompletion,
  ) {
    this.α = alpha;
    this.β = beta;
    this.streamFn = streamFn;
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
      for await (const chunk of this.streamFn(
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
        const approved = !!parsed.approved;
        const rawScore = Number(parsed.score);
        // A missing/non-numeric score must not coerce an approval to 0 — that would
        // DECREASE belief on what the critic actually approved. Default to full
        // confidence (1.0) on approval and 0.0 on rejection; clamp valid numbers.
        const score = Number.isFinite(rawScore)
          ? Math.max(0, Math.min(1, rawScore))
          : (approved ? 1.0 : 0.0);
        return {
          approved,
          score,
          reason: String(parsed.reason || ''),
        };
      }
    } catch (err) {
      return { approved: false, score: 0.0, reason: `Critic evaluation failed: ${err instanceof Error ? err.message : String(err)}` };
    }

    return { approved: false, score: 0.0, reason: 'Critic evaluation failed: no valid JSON in response' };
  }

  // Extract JSON with balanced-brace matching (handles nested braces in reason field)
  extractJSON(text: string): Record<string, unknown> | null {
    const start = text.indexOf('{');
    if (start === -1) return null;

    let depth = 0;
    let end = -1;
    let inString = false;
    for (let i = start; i < text.length; i++) {
      if (inString) {
        if (text[i] === '\\') { i++; continue; }
        if (text[i] === '"') inString = false;
        continue;
      }
      if (text[i] === '"') { inString = true; continue; }
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
