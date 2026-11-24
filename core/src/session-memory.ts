/**
 * Session Memory Extraction — extracts structured facts from tool results
 * and recent conversation context for cross-turn memory persistence.
 */

import 'node:fs/promises';
import 'node:path';
import type { ProviderConfig, Message } from './types';
import { streamChatCompletion, streamChatCompletionWithRetry } from './provider';
import { log as coreLog } from './logger';
import './reference-journal';
import { estimateTextTokens } from './token-estimator';
import { DEFAULT_SESSION_MEMORY_CONFIG, EXTRACTION_PROMPT, SUSPICIOUS_PATTERNS, factSimilarity, loadSessionMemoryPromptTemplate, normalizeRelativeDates, redactSecrets, substituteMemoryVariables } from './session-memory-session-memory-config';
import type { SessionFact, SessionMemoryConfig } from './session-memory-session-memory-config';
export { DEFAULT_MEMORY_PROMPT_TEMPLATE, DEFAULT_SESSION_MEMORY_CONFIG, loadSessionMemoryPromptTemplate, normalizeRelativeDates, redactSecrets, selectIncrementalWindow, substituteMemoryVariables } from './session-memory-session-memory-config';
export type { IncrementalWindow, SessionFact, SessionMemoryConfig } from './session-memory-session-memory-config';

export const MAX_MEMORY_ENTRIES = DEFAULT_SESSION_MEMORY_CONFIG.maxEntries;
export const MAX_MEMORY_ENTRY_CHARS = DEFAULT_SESSION_MEMORY_CONFIG.maxEntryChars;
/** Aggregate token budget for the whole persisted session-memory document (parity:
 *  the reference's `MAX_TOTAL_SESSION_MEMORY_TOKENS`). See {@link SessionMemoryConfig.maxTotalTokens}. */
export const MAX_SESSION_MEMORY_TOTAL_TOKENS = DEFAULT_SESSION_MEMORY_CONFIG.maxTotalTokens;
/** Max time a consumer blocks waiting for an in-flight extraction (parity:
 *  the reference's `EXTRACTION_WAIT_TIMEOUT_MS`). See {@link SessionMemoryConfig.extractionWaitTimeoutMs}. */
export const EXTRACTION_WAIT_TIMEOUT_MS = DEFAULT_SESSION_MEMORY_CONFIG.extractionWaitTimeoutMs;
/** Age past which an in-progress extraction is treated as stale and not waited on
 *  (parity: the reference's `EXTRACTION_STALE_THRESHOLD_MS`). See {@link SessionMemoryConfig.extractionStaleThresholdMs}. */
export const EXTRACTION_STALE_THRESHOLD_MS = DEFAULT_SESSION_MEMORY_CONFIG.extractionStaleThresholdMs;

export function validateMemoryEntry(entry: string): { valid: boolean; reason?: string } {
  if (!entry || entry.trim().length === 0) {
    return { valid: false, reason: 'empty' };
  }
  if (entry.length > MAX_MEMORY_ENTRY_CHARS) {
    return { valid: false, reason: 'too_long' };
  }
  for (const pattern of SUSPICIOUS_PATTERNS) {
    if (pattern.test(entry)) {
      return { valid: false, reason: 'suspicious_content' };
    }
  }
  return { valid: true };
}

export function sanitizeMemoryEntry(entry: string): string {
  let sanitized = entry.trim();
  sanitized = sanitized.replace(/<[^>]*>/g, '');
  // Redact credentials BEFORE truncation so a secret near the cap cannot survive
  // by being partially sliced off mid-token.
  sanitized = redactSecrets(sanitized);
  if (sanitized.length > MAX_MEMORY_ENTRY_CHARS) {
    sanitized = sanitized.slice(0, MAX_MEMORY_ENTRY_CHARS - 15) + '... [truncated]';
  }
  return sanitized;
}

export class SessionMemoryExtractor {
  private provider: ProviderConfig;
  private compactionModel?: string;
  /** Resolved, immutable tuning config (defaults merged with constructor overrides). */
  readonly config: SessionMemoryConfig;
  /** Test seam: the streaming function used to call the model. Defaults to the
   *  retry-wrapping provider stream so transient 429/5xx/529 responses during
   *  background extraction are retried with backoff instead of discarding all
   *  facts on the first error; tests may replace it to exercise failure paths. */
  _streamFn: typeof streamChatCompletion = streamChatCompletionWithRetry;
  /** Extraction watchdog timeout (ms). Seeded from `config.extractionTimeoutMs`;
   *  tests may override it to exercise the abort path without the full wait. */
  _timeoutMs: number;
  /** Test seam: the clock used to resolve relative dates in extracted facts.
   *  Defaults to the real wall clock; tests override it for determinism. */
  _now: () => Date = () => new Date();
  /** Config root under which the user-overridable extraction prompt is looked up
   *  (`<_configDir>/session-memory/config/prompt.md`). `undefined` → the resolved
   *  AMI config home (`~/.ami`). Tests point it at a temp dir. */
  _configDir?: string;
  /** Test seam: the sleep used by {@link waitForSessionMemoryExtraction}'s poll
   *  loop. Defaults to a real timer; tests override it to resolve instantly (and to
   *  advance a fake clock) so the wait/stale/timeout branches run without real time. */
  _sleep: (ms: number) => Promise<void> = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  /** Poll interval (ms) of the extraction wait loop (parity: the reference polls every 1s). */
  _waitPollMs = 1000;

  /**
   * Wall-clock timestamp (ms, from {@link _now}) at which the CURRENT extraction
   * pass started, or `undefined` when no pass is in flight. Set by
   * {@link markExtractionStarted} (called at the top of {@link extractFacts}) and
   * cleared by {@link markExtractionCompleted} (its `finally`), so
   * {@link isExtractionInProgress} / {@link waitForSessionMemoryExtraction} always
   * reflect reality. Parity: the reference's module-level `extractionStartedAt`
   * (SessionMemory/sessionMemoryUtils.ts:L46,L74-82).
   */
  private _extractionStartedAt?: number;

  /**
   * Whether session memory has crossed its initialization threshold at least once.
   * Before this is true, {@link shouldExtractFacts} defers all extraction until the
   * context reaches `config.minInitContextTokens` — the first extraction is held off
   * until there is enough context to be worth persisting. Parity: the reference's
   * `isSessionMemoryInitialized` / `markSessionMemoryInitialized`.
   */
  private _initialized = false;
  /**
   * Total context-window token count recorded at the last extraction, used to
   * measure growth for the `minTokensBetweenUpdate` gate. Parity: the reference's
   * `tokensAtLastExtraction` (0 until the first extraction).
   */
  private _tokensAtLastExtraction = 0;

  constructor(provider: ProviderConfig, compactionModel?: string, config?: Partial<SessionMemoryConfig>) {
    this.provider = provider;
    this.compactionModel = compactionModel;
    this.config = { ...DEFAULT_SESSION_MEMORY_CONFIG, ...config };
    this._timeoutMs = this.config.extractionTimeoutMs;
  }

  /**
   * Threshold/cadence gate: whether enough has happened to warrant an extraction
   * pass, given the CURRENT total context-window token count. Mirrors the reference's
   * `shouldExtractMemory` exactly:
   *
   * 1. **Init gate** — before the first extraction, defer until the context reaches
   *    `config.minInitContextTokens`; there is nothing worth persisting from a tiny
   *    conversation. Once crossed, the extractor is marked initialized (a one-way
   *    latch) so later passes use only the growth gate.
   * 2. **Token (growth) gate** — context growth since the last extraction must reach
   *    `config.minTokensBetweenUpdate`. This gate is ALWAYS required: a burst of tool
   *    calls alone never triggers extraction until the context has actually grown,
   *    which prevents excessive extractions.
   * 3. **Trigger** — extract when the growth gate is met AND either the tool-call
   *    gate (`toolCallsSinceLast >= config.toolCallsBetweenUpdate`) is met, OR the
   *    turn ended at a natural break (`naturalBreak`, i.e. no tool calls in the last
   *    assistant turn) so pending work is captured at the boundary.
   *
   * This is a predicate with one documented side effect: it latches `initialized`
   * when the init threshold is first met (matching the reference). Call
   * {@link recordExtraction} after a pass actually runs to advance the growth
   * baseline, and {@link resetExtractionState} to clear the latch (e.g. a new session).
   */
  shouldExtractFacts(opts: {
    currentContextTokens: number;
    toolCallsSinceLast: number;
    naturalBreak?: boolean;
  }): boolean {
    const { currentContextTokens, toolCallsSinceLast, naturalBreak } = opts;

    // 1. Init gate (latches on first threshold-met).
    if (!this._initialized) {
      if (currentContextTokens < this.config.minInitContextTokens) return false;
      this._initialized = true;
    }

    // 2. Token-growth gate — measured against the context size at the last pass.
    const growth = currentContextTokens - this._tokensAtLastExtraction;
    const hasMetTokenThreshold = growth >= this.config.minTokensBetweenUpdate;

    // 3. Tool-call gate + natural-break special case. The token gate is always required.
    const hasMetToolCallThreshold = toolCallsSinceLast >= this.config.toolCallsBetweenUpdate;
    return (
      (hasMetTokenThreshold && hasMetToolCallThreshold) ||
      (hasMetTokenThreshold && naturalBreak === true)
    );
  }

  /**
   * Record that an extraction pass ran at `currentContextTokens`, advancing the
   * growth baseline for the next `minTokensBetweenUpdate` check and latching the
   * initialized state. Parity: the reference's `recordExtractionTokenCount` +
   * `markSessionMemoryInitialized`. Call this only after a pass actually completes.
   */
  recordExtraction(currentContextTokens: number): void {
    this._initialized = true;
    this._tokensAtLastExtraction = currentContextTokens;
  }

  /**
   * Clear the extraction cadence state (initialized latch + growth baseline) so the
   * init gate applies again — e.g. when switching to a fresh session. Parity:
   * the reference's `resetSessionMemoryState`.
   */
  resetExtractionState(): void {
    this._initialized = false;
    this._tokensAtLastExtraction = 0;
  }

  /**
   * Mark that a fact-extraction pass has started. Records the current timestamp so a
   * concurrent consumer (e.g. an interrupt-driven shutdown, or pre-compaction) can
   * coordinate with the in-flight pass via {@link waitForSessionMemoryExtraction}.
   * Called at the top of {@link extractFacts}; idempotent-safe to call directly.
   * Parity: the reference's `markExtractionStarted` (SessionMemory/sessionMemoryUtils.ts:L74-76).
   */
  markExtractionStarted(): void {
    this._extractionStartedAt = this._now().getTime();
  }

  /**
   * Mark the in-flight extraction pass as finished (clears the start timestamp), so
   * {@link waitForSessionMemoryExtraction} stops blocking. Called from
   * {@link extractFacts}'s `finally`. Parity: the reference's `markExtractionCompleted`
   * (SessionMemory/sessionMemoryUtils.ts:L81-83).
   */
  markExtractionCompleted(): void {
    this._extractionStartedAt = undefined;
  }

  /** Whether a fact-extraction pass is currently in flight (introspection seam for
   *  consumers/tests). Parity: the `extractionStartedAt` truthiness check the reference's
   *  wait helper performs (SessionMemory/sessionMemoryUtils.ts:L89). */
  isExtractionInProgress(): boolean {
    return this._extractionStartedAt !== undefined;
  }

  /**
   * Block until any in-flight extraction completes, so a consumer (compaction /
   * shutdown) does not race a pass that is still writing session memory. Returns
   * IMMEDIATELY when no pass is in flight. Two bail-out guards keep it from ever
   * hanging a caller:
   *  - **stale** — if the in-progress pass is older than
   *    `config.extractionStaleThresholdMs`, it is assumed crashed/abandoned (its
   *    completion mark never fired) and the wait returns without blocking further.
   *  - **timeout** — after `config.extractionWaitTimeoutMs` of waiting, the wait
   *    returns and the caller proceeds anyway.
   * Parity: the reference's `waitForSessionMemoryExtraction`
   * (SessionMemory/sessionMemoryUtils.ts:L88-106).
   */
  async waitForSessionMemoryExtraction(): Promise<void> {
    const startTime = this._now().getTime();
    while (this._extractionStartedAt !== undefined) {
      const extractionAge = this._now().getTime() - this._extractionStartedAt;
      // Stale extraction — do not wait on an abandoned pass.
      if (extractionAge > this.config.extractionStaleThresholdMs) return;
      // Overall wait budget exhausted — proceed anyway.
      if (this._now().getTime() - startTime > this.config.extractionWaitTimeoutMs) return;
      await this._sleep(this._waitPollMs);
    }
  }

  /**
   * Extract structured facts from tool results and recent messages.
   * Uses the compaction model if provided, otherwise the primary provider model.
   * Only includes the last `config.contextWindowMessages` messages as context.
   *
   * An optional caller `signal` is combined with the internal watchdog so a user
   * interrupt cancels extraction promptly instead of blocking up to the timeout.
   *
   * `existingFacts` (the facts already persisted for this session) are pre-injected
   * into the prompt as a manifest so the extraction model dedupes/updates against
   * them instead of re-deriving facts it already knows — parity with the reference's
   * formatMemoryManifest, which pre-injects the memory listing "so the extraction
   * agent doesn't spend a turn on `ls`" (memdir/memoryScan.ts:L24-34).
   */
  async extractFacts(
    toolResults: Array<{ toolName: string; output: string; isError: boolean }>,
    recentMessages: Message[],
    signal?: AbortSignal,
    existingFacts?: SessionFact[],
  ): Promise<SessionFact[]> {
    // Record the in-flight start so a concurrent consumer (shutdown/compaction) can
    // coordinate via waitForSessionMemoryExtraction instead of racing this pass.
    this.markExtractionStarted();
    // Only use the last N messages
    const contextMessages = recentMessages.slice(-this.config.contextWindowMessages);

    // Build the user prompt with tool results and context. Redact secrets BEFORE
    // the content is sent to the extraction model so credentials in tool output
    // never leave the process inside a prompt.
    const toolResultsSummary = toolResults
      .map(r => `[${r.toolName}]${r.isError ? ' (ERROR)' : ''}: ${redactSecrets(r.output.slice(0, this.config.toolResultTruncateChars))}`)
      .join('\n\n');

    const messageSummary = contextMessages
      .map(m => {
        const content = typeof m.content === 'string' ? m.content : JSON.stringify(m.content);
        return `[${m.role}]: ${redactSecrets(content?.slice(0, this.config.messageTruncateChars) ?? '')}`;
      })
      .join('\n');

    // Pre-inject a manifest of already-persisted facts so the model can dedupe /
    // update against them instead of re-emitting known facts. Capped at maxEntries
    // and category-tagged, mirroring the reference's formatMemoryManifest affordance hint.
    const existingManifest = existingFacts && existingFacts.length > 0
      ? `\n\n## Existing Memory (already saved — do NOT repeat; only add new or updated facts)\n${existingFacts
          .slice(0, this.config.maxEntries)
          .map(f => `- [${f.category}] ${f.fact}`)
          .join('\n')}`
      : '';

    // Build the user content from the (possibly user-overridden) prompt template,
    // substituting the three content variables. The default template reproduces the
    // original fixed layout exactly; a user prompt.md can reorder/reword it.
    const promptTemplate = await loadSessionMemoryPromptTemplate(this._configDir);
    const userContent = substituteMemoryVariables(promptTemplate, {
      toolResults: toolResultsSummary,
      recentConversation: messageSummary,
      existingMemory: existingManifest,
    });

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
    const timeout = setTimeout(() => abortController.abort(), this._timeoutMs);
    // Thread the caller's abort signal into the extraction stream so an interrupt
    // cancels immediately rather than waiting out the watchdog.
    const onExternalAbort = () => abortController.abort();
    if (signal) {
      if (signal.aborted) abortController.abort();
      else signal.addEventListener('abort', onExternalAbort, { once: true });
    }

    let facts: SessionFact[] = [];
    try {
      let responseText = '';

      for await (const chunk of this._streamFn(
        { ...effectiveConfig, maxTokens: this.config.maxTokens },
        messages,
        [], // no tools
        abortController.signal,
        // Background fact extraction: tag the request so a 529 overload bails
        // immediately instead of amplifying a capacity cascade with retries the
        // user never sees (shouldRetry529). All other statuses still retry.
        { querySource: 'session_memory' },
      )) {
        if (chunk.type === 'content_delta' && chunk.text) {
          responseText += chunk.text;
        }
        if (chunk.type === 'error') {
          coreLog('session-memory', 'Extraction error', { error: chunk.error ?? 'unknown' });
          return [];
        }
      }

      // Assign and fall through to the finally cleanup (rather than returning
      // from inside the try) so timeout cleanup and the success return share one
      // normal-completion path.
      facts = this.parseFactsFromResponse(responseText);
    } catch (err) {
      coreLog('session-memory', 'Failed to extract facts', { error: String(err) });
      return [];
    } finally {
      clearTimeout(timeout);
      if (signal) signal.removeEventListener('abort', onExternalAbort);
      // Clear the in-flight mark on EVERY exit (success, error-chunk early return,
      // thrown/aborted) so waitForSessionMemoryExtraction never blocks past the pass.
      this.markExtractionCompleted();
    }
    return facts;
  }

  /**
   * Merge new facts into existing facts, deduplicating by content similarity.
   * Near-duplicates (normalized-equal, or Jaccard token similarity at/above
   * `config.dedupSimilarityThreshold`) collapse so paraphrased restatements do
   * not accumulate; the higher-confidence variant wins. When the merged list
   * exceeds `config.maxEntries` OR its combined token count exceeds
   * `config.maxTotalTokens`, the newest `config.recencyProtectedCount` facts are
   * retained regardless of confidence and the remaining slots are filled by a
   * criticality-aware score until BOTH the entry-count and token budgets are
   * satisfied, so current-work and critical facts are not evicted by stale
   * high-confidence noise. The returned list is ordered by descending confidence.
   */
  mergeFacts(existing: SessionFact[], newFacts: SessionFact[]): SessionFact[] {
    const merged = [...existing];

    for (const newFact of newFacts) {
      const validation = validateMemoryEntry(newFact.fact);
      if (!validation.valid) {
        coreLog('session-memory', `Skipping invalid fact: ${validation.reason}`);
        continue;
      }
      newFact.fact = sanitizeMemoryEntry(newFact.fact);

      const duplicateIndex = merged.findIndex(
        e =>
          e.fact.toLowerCase() === newFact.fact.toLowerCase() ||
          factSimilarity(e.fact, newFact.fact) >= this.config.dedupSimilarityThreshold,
      );

      if (duplicateIndex >= 0) {
        if (newFact.confidence > merged[duplicateIndex].confidence) {
          merged[duplicateIndex] = newFact;
        }
      } else {
        merged.push(newFact);
      }
    }

    const overEntries = merged.length > this.config.maxEntries;
    const overTokens = this.totalFactTokens(merged) > this.config.maxTotalTokens;
    if (overEntries || overTokens) {
      // Insertion order == recency (existing first, newest appended), so the tail
      // is the most recent. Protect the newest N from eviction, then fill the
      // remaining slots from the older facts by a criticality-aware score, honoring
      // BOTH the entry-count cap and the aggregate token budget.
      const protectCount = Math.min(this.config.recencyProtectedCount, this.config.maxEntries);
      const recent = merged.slice(merged.length - protectCount);
      const older = merged.slice(0, merged.length - protectCount);
      // Rank eviction survivors by a criticality-aware score (confidence plus a
      // boost for critical categories) rather than confidence alone, so important
      // facts (errors, decisions, user preferences) are not crowded out by stale
      // high-confidence noise. The stored confidence is never mutated.
      older.sort((a, b) => this.evictionScore(b) - this.evictionScore(a));
      // Recency-protected facts are always kept (even if they alone exceed the token
      // budget — current work must survive). Then add older facts, highest-score
      // first, skipping any that would push the running total over the token budget,
      // and stopping once the entry-count cap is reached.
      const survivors = [...recent];
      let tokens = this.totalFactTokens(recent);
      for (const fact of older) {
        if (survivors.length >= this.config.maxEntries) break;
        const factTokens = estimateTextTokens(fact.fact);
        if (tokens + factTokens > this.config.maxTotalTokens) continue;
        survivors.push(fact);
        tokens += factTokens;
      }
      survivors.sort((a, b) => b.confidence - a.confidence);
      return survivors;
    }

    return merged;
  }

  /** Combined byte-aware token estimate of a set of facts' text, used to enforce the
   *  aggregate {@link SessionMemoryConfig.maxTotalTokens} budget in {@link mergeFacts}. */
  private totalFactTokens(facts: SessionFact[]): number {
    let total = 0;
    for (const fact of facts) total += estimateTextTokens(fact.fact);
    return total;
  }

  /**
   * Eviction ranking score for a fact: its confidence plus `criticalityBoost` when
   * its category is in `criticalCategories`. Used only to choose survivors when the
   * list exceeds `maxEntries`; it never mutates the fact's stored confidence.
   */
  private evictionScore(fact: SessionFact): number {
    const critical = this.config.criticalCategories.includes(fact.category);
    return fact.confidence + (critical ? this.config.criticalityBoost : 0);
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
            obj.confidence >= this.config.confidenceFloor
          );
        })
        // Normalize any unambiguous relative dates the model left in the fact text,
        // so facts stay interpretable when recalled in a later session.
        .map(f => ({ ...f, fact: normalizeRelativeDates(f.fact, this._now()) }));
    } catch {
      coreLog('session-memory', 'Failed to parse facts JSON from response');
      return [];
    }
  }
}
