export interface TurnData {
  toolCalls: number;
  tokensUsed: number;
  durationMs: number;
  /** Portion of durationMs spent in retry attempts (0 when the turn did not retry). */
  retryDurationMs?: number;
  /** Model that served this turn; enables per-model attribution across /model switches. */
  model?: string;
  /** PARITY: USD cost of this turn (provider-computed); rolled into cumulative + per-model totals. */
  costUsd?: number;
  /** PARITY: input (prompt) tokens — a breakdown of `tokensUsed` matching provider Usage shape. */
  inputTokens?: number;
  /** PARITY: output (completion) tokens. */
  outputTokens?: number;
  /** PARITY: cache-read tokens (prompt-cache hit). */
  cacheReadTokens?: number;
  /** PARITY: cache-creation tokens (prompt-cache write). */
  cacheCreationTokens?: number;
}

/** Per-model usage attribution (survives mid-session model switches). */
export interface ModelTurnUsage {
  turnCount: number;
  toolCalls: number;
  tokens: number;
  durationMs: number;
  /** PARITY: cumulative USD cost attributed to this model. */
  costUsd: number;
}

/** PARITY: breakdown of cumulative token usage by category (mirrors provider Usage). */
export interface TokenBreakdown {
  input: number;
  output: number;
  cacheRead: number;
  cacheCreation: number;
}

/**
 * PARITY: serializable cumulative state for session persistence / cross-restart
 * restore. Produced by `snapshot()`, consumed by `restore()`; round-trips 1:1.
 */
export interface TurnMetricsSnapshot {
  turnCount: number;
  totalToolCalls: number;
  totalTokens: number;
  totalDurationMs: number;
  totalRetryDurationMs: number;
  longestTurnMs: number;
  totalCostUsd: number;
  tokens: TokenBreakdown;
  byModel: Record<string, ModelTurnUsage>;
}

/** Per-turn token-budget status (used/limit with a nudge count), or null when no budget is set. */
export interface TurnBudgetStatus {
  limit: number;
  tokens: number;
  /** Integer percentage of the limit consumed by the most recent turn. */
  pct: number;
  exceeded: boolean;
  nudges: number;
}

export interface TurnSummary {
  turnCount: number;
  totalToolCalls: number;
  totalTokens: number;
  totalDurationMs: number;
  /** Total wall time excluding retry attempts. */
  totalDurationMsWithoutRetries: number;
  avgToolCallsPerTurn: number;
  avgTokensPerTurn: number;
  tokensPerSecond: number;
  /** Tokens/sec computed against retry-excluded duration (>= tokensPerSecond). */
  tokensPerSecondWithoutRetries: number;
  longestTurnMs: number;
  /** Per-model usage breakdown keyed by model name. */
  byModel: Record<string, ModelTurnUsage>;
  /** Overlap-deduplicated wall-clock active time (concurrent ops counted once). */
  activeTimeMs: number;
  /** Per-turn token budget status, or null when no budget was configured. */
  budget: TurnBudgetStatus | null;
  /** PARITY: cumulative USD cost across all turns. */
  totalCostUsd: number;
  /** PARITY: mean USD cost per turn (0 when no turns). */
  avgCostPerTurn: number;
  /** PARITY: cumulative token usage split by category. */
  tokens: TokenBreakdown;
}

/** Coerce an arbitrary usage field to a finite, non-negative number (missing/NaN/Infinity/neg → 0). */
function sanitize(x: number | undefined): number {
  return typeof x === 'number' && Number.isFinite(x) && x > 0 ? x : 0;
}

/**
 * PARITY: add two non-negative counters without silently losing integer precision.
 * Repeated large turns can push a raw `+=` past Number.MAX_SAFE_INTEGER, after which
 * integer arithmetic is lossy; clamp at the ceiling instead of drifting.
 */
function addClamped(a: number, b: number): number {
  const sum = a + b;
  return sum > Number.MAX_SAFE_INTEGER ? Number.MAX_SAFE_INTEGER : sum;
}

export class TurnMetrics {
  private _turnCount = 0;
  private _totalToolCalls = 0;
  private _totalTokens = 0;
  private _totalDurationMs = 0;
  private _totalRetryDurationMs = 0;
  private _longestTurnMs = 0;
  private _byModel = new Map<string, ModelTurnUsage>();

  // --- PARITY: cost rollup + token-type breakdown ----------------------------
  private _totalCostUsd = 0;
  private _inputTokens = 0;
  private _outputTokens = 0;
  private _cacheReadTokens = 0;
  private _cacheCreationTokens = 0;

  // --- Per-turn token budget -------------------------------------------------
  private _budgetLimit = 0;
  private _budgetNudges = 0;
  private _lastTurnTokens = 0;

  // --- Overlap-deduplicated active-time ref counting -------------------------
  // _activeStart is -1 when no window is open (0 is a valid timestamp).
  private _activeOps = new Set<string>();
  private _activeStart = -1;
  private _activeTimeMs = 0;

  recordTurn(data: TurnData): void {
    const toolCalls = sanitize(data.toolCalls);
    const tokens = sanitize(data.tokensUsed);
    const durationMs = sanitize(data.durationMs);
    // retry time can never exceed the turn's own duration
    const retryDurationMs = Math.min(sanitize(data.retryDurationMs), durationMs);
    // PARITY: cost + token-type breakdown
    const costUsd = sanitize(data.costUsd);
    const inputTokens = sanitize(data.inputTokens);
    const outputTokens = sanitize(data.outputTokens);
    const cacheReadTokens = sanitize(data.cacheReadTokens);
    const cacheCreationTokens = sanitize(data.cacheCreationTokens);

    this._turnCount++;
    this._totalToolCalls = addClamped(this._totalToolCalls, toolCalls);
    this._totalTokens = addClamped(this._totalTokens, tokens);
    this._totalDurationMs = addClamped(this._totalDurationMs, durationMs);
    this._totalRetryDurationMs = addClamped(this._totalRetryDurationMs, retryDurationMs);
    this._totalCostUsd += costUsd;
    this._inputTokens = addClamped(this._inputTokens, inputTokens);
    this._outputTokens = addClamped(this._outputTokens, outputTokens);
    this._cacheReadTokens = addClamped(this._cacheReadTokens, cacheReadTokens);
    this._cacheCreationTokens = addClamped(this._cacheCreationTokens, cacheCreationTokens);
    this._lastTurnTokens = tokens;
    if (durationMs > this._longestTurnMs) {
      this._longestTurnMs = durationMs;
    }

    if (data.model) {
      const m = this._byModel.get(data.model) ?? { turnCount: 0, toolCalls: 0, tokens: 0, durationMs: 0, costUsd: 0 };
      m.turnCount++;
      m.toolCalls = addClamped(m.toolCalls, toolCalls);
      m.tokens = addClamped(m.tokens, tokens);
      m.durationMs = addClamped(m.durationMs, durationMs);
      m.costUsd += costUsd;
      this._byModel.set(data.model, m);
    }
  }

  /** Configure a per-turn token budget limit (0 disables the budget display). */
  setTurnBudget(limit: number): void {
    this._budgetLimit = sanitize(limit);
  }

  /** Record that the model was nudged about the turn budget (suffix count in the UI). */
  recordBudgetNudge(): void {
    this._budgetNudges++;
  }

  /**
   * Mark an operation active. Overlapping operations are ref-counted by id so N
   * concurrent tool calls collapse into ONE active-time window (no double counting).
   */
  beginOperation(id: string, now: number = Date.now()): void {
    if (this._activeOps.size === 0) this._activeStart = now;
    this._activeOps.add(id);
  }

  /** Mark an operation finished; records elapsed wall-clock only on the last (set→empty) transition. */
  endOperation(id: string, now: number = Date.now()): void {
    if (!this._activeOps.delete(id)) return;
    if (this._activeOps.size === 0 && this._activeStart >= 0) {
      this._activeTimeMs += Math.max(0, now - this._activeStart);
      this._activeStart = -1;
    }
  }

  /** Accumulated active time, including any currently-open window up to `now`. */
  getActiveTimeMs(now: number = Date.now()): number {
    const open = this._activeOps.size > 0 && this._activeStart >= 0 ? Math.max(0, now - this._activeStart) : 0;
    return this._activeTimeMs + open;
  }

  getBudgetStatus(): TurnBudgetStatus | null {
    if (this._budgetLimit <= 0) return null;
    return {
      limit: this._budgetLimit,
      tokens: this._lastTurnTokens,
      pct: Math.round((this._lastTurnTokens / this._budgetLimit) * 100),
      exceeded: this._lastTurnTokens > this._budgetLimit,
      nudges: this._budgetNudges,
    };
  }

  getModelUsage(): Record<string, ModelTurnUsage> {
    const out: Record<string, ModelTurnUsage> = {};
    for (const [model, u] of this._byModel) out[model] = { ...u };
    return out;
  }

  get turnCount(): number { return this._turnCount; }
  get totalToolCalls(): number { return this._totalToolCalls; }
  get totalTokens(): number { return this._totalTokens; }
  get totalDurationMs(): number { return this._totalDurationMs; }
  get totalDurationMsWithoutRetries(): number { return Math.max(0, this._totalDurationMs - this._totalRetryDurationMs); }
  get longestTurnMs(): number { return this._longestTurnMs; }

  /** PARITY: cumulative USD cost across all turns. */
  get totalCostUsd(): number { return this._totalCostUsd; }

  /** PARITY: mean USD cost per turn (0 when no turns). */
  get avgCostPerTurn(): number {
    return this._turnCount > 0 ? this._totalCostUsd / this._turnCount : 0;
  }

  /** PARITY: cumulative token usage split by category (mirrors provider Usage shape). */
  getTokenBreakdown(): TokenBreakdown {
    return {
      input: this._inputTokens,
      output: this._outputTokens,
      cacheRead: this._cacheReadTokens,
      cacheCreation: this._cacheCreationTokens,
    };
  }

  get avgToolCallsPerTurn(): number {
    return this._turnCount > 0 ? this._totalToolCalls / this._turnCount : 0;
  }

  get avgTokensPerTurn(): number {
    return this._turnCount > 0 ? this._totalTokens / this._turnCount : 0;
  }

  get tokensPerSecond(): number {
    return this._totalDurationMs > 0 ? (this._totalTokens / this._totalDurationMs) * 1000 : 0;
  }

  get tokensPerSecondWithoutRetries(): number {
    const d = this.totalDurationMsWithoutRetries;
    return d > 0 ? (this._totalTokens / d) * 1000 : 0;
  }

  getSummary(): TurnSummary {
    return {
      turnCount: this._turnCount,
      totalToolCalls: this._totalToolCalls,
      totalTokens: this._totalTokens,
      totalDurationMs: this._totalDurationMs,
      totalDurationMsWithoutRetries: this.totalDurationMsWithoutRetries,
      avgToolCallsPerTurn: this.avgToolCallsPerTurn,
      avgTokensPerTurn: this.avgTokensPerTurn,
      tokensPerSecond: this.tokensPerSecond,
      tokensPerSecondWithoutRetries: this.tokensPerSecondWithoutRetries,
      longestTurnMs: this._longestTurnMs,
      byModel: this.getModelUsage(),
      activeTimeMs: this.getActiveTimeMs(),
      budget: this.getBudgetStatus(),
      totalCostUsd: this._totalCostUsd,
      avgCostPerTurn: this.avgCostPerTurn,
      tokens: this.getTokenBreakdown(),
    };
  }

  /**
   * PARITY: capture cumulative session state as a plain serializable object so metrics
   * survive process restarts / cross-agent persistence. Pair with `restore()`. Excludes
   * transient live state (open active-time windows, budget config) by design.
   */
  snapshot(): TurnMetricsSnapshot {
    return {
      turnCount: this._turnCount,
      totalToolCalls: this._totalToolCalls,
      totalTokens: this._totalTokens,
      totalDurationMs: this._totalDurationMs,
      totalRetryDurationMs: this._totalRetryDurationMs,
      longestTurnMs: this._longestTurnMs,
      totalCostUsd: this._totalCostUsd,
      tokens: this.getTokenBreakdown(),
      byModel: this.getModelUsage(),
    };
  }

  /**
   * PARITY: reload cumulative state produced by `snapshot()`. Resets everything first,
   * then applies the snapshot; each numeric field is re-sanitized so a corrupt/partial
   * persisted blob cannot poison the live totals.
   */
  restore(snap: Partial<TurnMetricsSnapshot> | null | undefined): void {
    this.reset();
    if (!snap || typeof snap !== 'object') return;
    this._turnCount = sanitize(snap.turnCount);
    this._totalToolCalls = sanitize(snap.totalToolCalls);
    this._totalTokens = sanitize(snap.totalTokens);
    this._totalDurationMs = sanitize(snap.totalDurationMs);
    this._totalRetryDurationMs = Math.min(sanitize(snap.totalRetryDurationMs), this._totalDurationMs);
    this._longestTurnMs = sanitize(snap.longestTurnMs);
    this._totalCostUsd = sanitize(snap.totalCostUsd);
    if (snap.tokens && typeof snap.tokens === 'object') {
      this._inputTokens = sanitize(snap.tokens.input);
      this._outputTokens = sanitize(snap.tokens.output);
      this._cacheReadTokens = sanitize(snap.tokens.cacheRead);
      this._cacheCreationTokens = sanitize(snap.tokens.cacheCreation);
    }
    if (snap.byModel && typeof snap.byModel === 'object') {
      for (const [model, u] of Object.entries(snap.byModel)) {
        if (!u || typeof u !== 'object') continue;
        this._byModel.set(model, {
          turnCount: sanitize(u.turnCount),
          toolCalls: sanitize(u.toolCalls),
          tokens: sanitize(u.tokens),
          durationMs: sanitize(u.durationMs),
          costUsd: sanitize(u.costUsd),
        });
      }
    }
  }

  reset(): void {
    this._turnCount = 0;
    this._totalToolCalls = 0;
    this._totalTokens = 0;
    this._totalDurationMs = 0;
    this._totalRetryDurationMs = 0;
    this._longestTurnMs = 0;
    this._byModel.clear();
    this._totalCostUsd = 0;
    this._inputTokens = 0;
    this._outputTokens = 0;
    this._cacheReadTokens = 0;
    this._cacheCreationTokens = 0;
    this._budgetLimit = 0;
    this._budgetNudges = 0;
    this._lastTurnTokens = 0;
    this._activeOps.clear();
    this._activeStart = -1;
    this._activeTimeMs = 0;
  }
}
