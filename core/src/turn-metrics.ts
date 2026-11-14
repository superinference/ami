export interface TurnData {
  toolCalls: number;
  tokensUsed: number;
  durationMs: number;
  /** Portion of durationMs spent in retry attempts (0 when the turn did not retry). */
  retryDurationMs?: number;
  /** Model that served this turn; enables per-model attribution across /model switches. */
  model?: string;
}

/** Per-model usage attribution (survives mid-session model switches). */
export interface ModelTurnUsage {
  turnCount: number;
  toolCalls: number;
  tokens: number;
  durationMs: number;
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
}

/** Coerce an arbitrary usage field to a finite, non-negative number (missing/NaN/Infinity/neg → 0). */
function sanitize(x: number | undefined): number {
  return typeof x === 'number' && Number.isFinite(x) && x > 0 ? x : 0;
}

export class TurnMetrics {
  private _turnCount = 0;
  private _totalToolCalls = 0;
  private _totalTokens = 0;
  private _totalDurationMs = 0;
  private _totalRetryDurationMs = 0;
  private _longestTurnMs = 0;
  private _byModel = new Map<string, ModelTurnUsage>();

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

    this._turnCount++;
    this._totalToolCalls += toolCalls;
    this._totalTokens += tokens;
    this._totalDurationMs += durationMs;
    this._totalRetryDurationMs += retryDurationMs;
    this._lastTurnTokens = tokens;
    if (durationMs > this._longestTurnMs) {
      this._longestTurnMs = durationMs;
    }

    if (data.model) {
      const m = this._byModel.get(data.model) ?? { turnCount: 0, toolCalls: 0, tokens: 0, durationMs: 0 };
      m.turnCount++;
      m.toolCalls += toolCalls;
      m.tokens += tokens;
      m.durationMs += durationMs;
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
    };
  }

  reset(): void {
    this._turnCount = 0;
    this._totalToolCalls = 0;
    this._totalTokens = 0;
    this._totalDurationMs = 0;
    this._totalRetryDurationMs = 0;
    this._longestTurnMs = 0;
    this._byModel.clear();
    this._budgetLimit = 0;
    this._budgetNudges = 0;
    this._lastTurnTokens = 0;
    this._activeOps.clear();
    this._activeStart = -1;
    this._activeTimeMs = 0;
  }
}
