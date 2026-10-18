export interface TurnData {
  toolCalls: number;
  tokensUsed: number;
  durationMs: number;
}

export interface TurnSummary {
  turnCount: number;
  totalToolCalls: number;
  totalTokens: number;
  totalDurationMs: number;
  avgToolCallsPerTurn: number;
  avgTokensPerTurn: number;
  tokensPerSecond: number;
  longestTurnMs: number;
}

export class TurnMetrics {
  private _turnCount = 0;
  private _totalToolCalls = 0;
  private _totalTokens = 0;
  private _totalDurationMs = 0;
  private _longestTurnMs = 0;

  recordTurn(data: TurnData): void {
    this._turnCount++;
    this._totalToolCalls += data.toolCalls;
    this._totalTokens += data.tokensUsed;
    this._totalDurationMs += data.durationMs;
    if (data.durationMs > this._longestTurnMs) {
      this._longestTurnMs = data.durationMs;
    }
  }

  get turnCount(): number { return this._turnCount; }
  get totalToolCalls(): number { return this._totalToolCalls; }
  get totalTokens(): number { return this._totalTokens; }
  get totalDurationMs(): number { return this._totalDurationMs; }
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

  getSummary(): TurnSummary {
    return {
      turnCount: this._turnCount,
      totalToolCalls: this._totalToolCalls,
      totalTokens: this._totalTokens,
      totalDurationMs: this._totalDurationMs,
      avgToolCallsPerTurn: this.avgToolCallsPerTurn,
      avgTokensPerTurn: this.avgTokensPerTurn,
      tokensPerSecond: this.tokensPerSecond,
      longestTurnMs: this._longestTurnMs,
    };
  }

  reset(): void {
    this._turnCount = 0;
    this._totalToolCalls = 0;
    this._totalTokens = 0;
    this._totalDurationMs = 0;
    this._longestTurnMs = 0;
  }
}
