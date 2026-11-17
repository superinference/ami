import { streamChatCompletion, streamChatCompletionWithRetry, REDACTED_THINKING_PLACEHOLDER } from '../provider';
import type { ProviderConfig, Message, StreamChunk } from '../types';
import type { CriticDecision } from './types';

/** Retry tuning for the critic's auxiliary LLM call. `undefined` fields defer to the
 *  provider's own defaults (MAX_RETRIES / BASE_DELAY_MS). Exposed so a caller/test can
 *  tighten the backoff; production uses the shared defaults. */
export interface CriticRetryOptions {
  maxRetries?: number;
  baseDelayMs?: number;
}

/**
 * Env-var kill switch for the LLM critic/advisor. Honors both the AMI name and the
 * Claude-Code-compatible alias, mirroring the pattern used by web_fetch
 * (CLAUDE_CODE_DISABLE_WEB_FETCH / SUPERINFERENCE_DISABLE_WEB_FETCH). When set, the
 * engine must fall back to the proxy critic instead of issuing the auxiliary model
 * call. Parity: Claude's advisor `CLAUDE_CODE_DISABLE_ADVISOR_TOOL` override.
 */
export function isCriticDisabledByEnv(env: NodeJS.ProcessEnv = process.env): boolean {
  return env.CLAUDE_CODE_DISABLE_CRITIC === '1' || env.SUPERINFERENCE_DISABLE_CRITIC === '1';
}

export class Critic {
  private α: number;  // False approval rate (Eq. 3)
  private β: number;  // False rejection rate (Eq. 3)
  // DI seam (test-only): the single-attempt streaming function the retry wrapper
  // drives. Defaults to the real provider; overridable so tests can exercise the
  // retry / fail-closed paths without hitting the network.
  private streamFn: typeof streamChatCompletion;
  // Dedicated reviewer model (parity: Claude's advisor `advisorModel`). Empty string
  // inherits the main-loop model passed to evaluate().
  private criticModel: string;
  private retry: CriticRetryOptions;

  constructor(
    alpha: number = 0.05,
    beta: number = 0.10,
    streamFn: typeof streamChatCompletion = streamChatCompletion,
    criticModel: string = '',
    retry: CriticRetryOptions = {},
  ) {
    this.α = alpha;
    this.β = beta;
    this.streamFn = streamFn;
    this.criticModel = criticModel;
    this.retry = retry;
  }

  /** Resolved reviewer model id, or '' when the critic inherits the caller's model. */
  get criticModelId(): string { return this.criticModel; }

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

    // Resolve the dedicated reviewer model (parity: Claude's advisor model override);
    // '' inherits the main-loop model. The verdict is attributed to this resolved id.
    const reviewerModel = this.criticModel || provider.model;

    try {
      let response = '';
      let streamError: string | undefined;
      // Per-call token usage for this auxiliary reviewer request, captured from the
      // provider's terminal 'done' chunk so a usage view can attribute reviewer-model
      // spend distinctly from the main loop. Parity: Claude's getAdvisorUsage.
      let lastUsage: StreamChunk['usage'] | undefined;
      // Whether the reviewer's reasoning came back encrypted/redacted. The provider maps
      // a `redacted_thinking` block to a thinking_delta carrying REDACTED_THINKING_PLACEHOLDER;
      // if a genuine verdict still parses, the review is COMPLETE but its rationale was
      // withheld. Parity: Claude's redacted-review completion state.
      let redactedReasoning = false;
      // Route the auxiliary call through the SHARED retry wrapper (querySource 'critic')
      // so transient capacity/connection errors (429/5xx, ECONNRESET/EPIPE) are retried
      // with backoff exactly like every other model call, instead of failing closed on
      // the first blip. The wrapper suppresses only 529 overload retries for background
      // sources (a capacity cascade must not be amplified). It yields error chunks rather
      // than throwing for retry-exhausted/HTTP failures; a non-network/SSL error is still
      // rethrown and handled by the catch below.
      for await (const chunk of streamChatCompletionWithRetry(
        { ...provider, model: reviewerModel, maxTokens: 200, temperature: 0 },
        messages,
        [],
        abortSignal,
        {
          streamImpl: this.streamFn,
          querySource: 'critic',
          ...(this.retry.maxRetries !== undefined ? { maxRetries: this.retry.maxRetries } : {}),
          ...(this.retry.baseDelayMs !== undefined ? { baseDelayMs: this.retry.baseDelayMs } : {}),
        },
      )) {
        if (chunk.type === 'content_delta' && chunk.text) {
          response += chunk.text;
        } else if (chunk.type === 'thinking_delta' && chunk.text?.includes(REDACTED_THINKING_PLACEHOLDER)) {
          redactedReasoning = true;
        } else if (chunk.type === 'error' && chunk.error) {
          streamError = chunk.error;
        }
        if (chunk.usage) lastUsage = chunk.usage;
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
          // Attribute the verdict to the model that produced it (the resolved critic
          // model, which may differ from the main-loop model). Parity: Claude's
          // advisor-message model attribution.
          model: reviewerModel,
          // A redacted-but-complete review: the verdict stands, only the rationale was
          // withheld (encrypted reasoning). Parity: Claude's redacted-review state.
          ...(redactedReasoning ? { redacted: true } : {}),
          // Per-auxiliary-model token usage, tagged with the reviewer model so a usage
          // view can attribute reviewer spend separately. Parity: getAdvisorUsage.
          ...(lastUsage ? {
            usage: {
              model: reviewerModel,
              promptTokens: lastUsage.promptTokens,
              completionTokens: lastUsage.completionTokens,
              totalTokens: lastUsage.totalTokens,
            },
          } : {}),
        };
      }

      // The retry wrapper surfaced a terminal error chunk (retries exhausted, or a
      // non-retryable HTTP failure): the reviewer was UNAVAILABLE, distinct from a
      // substantive rejection. Preserve the (credential-sanitized) wire error.
      if (streamError) {
        return this.unavailable(abortSignal, streamError, 'stream_error');
      }
    } catch (err) {
      // A rethrown SSL/non-network error means the reviewer was UNAVAILABLE — NOT a
      // substantive rejection. Flag it so the consumer can soften the belief floor
      // instead of treating {approved:false,score:0} as a real "no". Parity: Claude's
      // AdvisorMessage "Advisor unavailable (code)" error branch.
      return this.unavailable(abortSignal, err instanceof Error ? err.message : String(err), 'stream_error');
    }

    // Reached the end of the stream with no parseable JSON and no error chunk: the
    // reviewer did respond but produced unusable output. This is also an UNAVAILABLE
    // state (no genuine verdict was rendered), distinct from a real rejection.
    return this.unavailable(abortSignal, 'no valid JSON in response', 'malformed_response');
  }

  /** Build an UNAVAILABLE decision, tagging an aborted run with the 'aborted' code so
   *  a cancellation is never conflated with a genuine infrastructure failure. */
  private unavailable(abortSignal: AbortSignal, detail: string, code: string): CriticDecision {
    return {
      approved: false,
      score: 0.0,
      reason: `Critic evaluation failed: ${detail}`,
      unavailable: true,
      errorCode: abortSignal.aborted ? 'aborted' : code,
    };
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
