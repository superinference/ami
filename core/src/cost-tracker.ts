/**
 * Cost tracking for SuperInference engine.
 *
 * Tracks token usage, tool calls, turns, and estimates dollar cost
 * based on known model pricing (per million tokens).
 *
 * Supports prompt caching cost tracking: cached prompt tokens are priced
 * at a fraction (CACHED_PRICE_RATIO) of the normal input price.
 */

import type { UsageStats } from './types';

/**
 * Extended stats that include prompt caching information.
 * Defined inline to avoid modifying types.ts (handled by another agent).
 */
export interface ExtendedUsageStats extends UsageStats {
  cachedPromptTokens: number;
  uncachedPromptTokens: number;
  /** Fraction of prompt tokens served from cache (0..1). */
  cacheHitRate: number;
  /** Estimated dollar savings from prompt caching. */
  cachedCostSavings: number;
}

// Pricing per million tokens: [input, output]
const MODEL_PRICING: Record<string, [number, number]> = {
  'gpt-4o': [2.5, 10],
  'gpt-4o-mini': [0.15, 0.6],
  'gemini-2.0-flash': [0.1, 0.4],
  'gemini-1.5-pro': [1.25, 5],
  'claude-sonnet-4': [3, 15],
  'claude-haiku': [0.25, 1.25],
  'claude-opus-4': [15, 75],
  'o1': [15, 60],
  'o1-mini': [3, 12],
  'o3': [10, 40],
  'o3-mini': [1.10, 4.40],
  'o4-mini': [1.10, 4.40],
  'gemini-2.5-pro': [1.25, 10],
  'gemini-2.5-flash': [0.15, 0.6],
  'deepseek-r1': [0.55, 2.19],
  'gpt-4-turbo': [10, 30],
  'gemini-3': [1.25, 10],
};

const DEFAULT_PRICING: [number, number] = [1, 3];

/** Cached tokens are priced at 10% of normal input price. */
const CACHED_PRICE_RATIO = 0.1;

function matchPricing(model: string): [number, number] {
  if (MODEL_PRICING[model]) {
    return MODEL_PRICING[model];
  }
  // Sort keys by length descending so longer (more specific) keys match first
  // e.g. 'gpt-4o-mini' is checked before 'gpt-4o'
  const sortedKeys = Object.keys(MODEL_PRICING).sort((a, b) => b.length - a.length);
  for (const key of sortedKeys) {
    if (model.includes(key)) {
      return MODEL_PRICING[key];
    }
  }
  return DEFAULT_PRICING;
}

export class CostTracker {
  private stats: UsageStats = {
    promptTokens: 0,
    completionTokens: 0,
    reasoningTokens: 0,
    totalTokens: 0,
    totalCost: 0,
    requestCount: 0,
    toolCallCount: 0,
    turnCount: 0,
  };

  /** Cumulative cached prompt tokens across all requests. */
  private _cachedPromptTokens = 0;
  /** Cumulative uncached prompt tokens across all requests. */
  private _uncachedPromptTokens = 0;
  /** Accumulated cost savings from caching. */
  private _cachedCostSavings = 0;

  private model: string;

  constructor(model: string) {
    this.model = model;
  }

  trackUsage(usage: {
    promptTokens: number;
    completionTokens: number;
    reasoningTokens?: number;
    cachedPromptTokens?: number;
  }): void {
    const cachedTokens = usage.cachedPromptTokens ?? 0;
    const uncachedTokens = usage.promptTokens - cachedTokens;

    this.stats.promptTokens += usage.promptTokens;
    this.stats.completionTokens += usage.completionTokens;
    this.stats.reasoningTokens += usage.reasoningTokens ?? 0;
    this.stats.totalTokens += usage.promptTokens + usage.completionTokens;

    // Calculate cost with cache-aware pricing
    const cost = this.estimateCostWithCache(
      this.model,
      uncachedTokens,
      cachedTokens,
      usage.completionTokens,
    );
    this.stats.totalCost += cost;
    this.stats.requestCount++;

    // Track cache-specific metrics
    this._cachedPromptTokens += cachedTokens;
    this._uncachedPromptTokens += uncachedTokens;

    // Calculate savings: difference between what full-price would have cost
    // and what cached tokens actually cost
    if (cachedTokens > 0) {
      const [inputPricePerMillion] = matchPricing(this.model);
      const fullPriceCost = (cachedTokens / 1_000_000) * inputPricePerMillion;
      const cachedCost = (cachedTokens / 1_000_000) * inputPricePerMillion * CACHED_PRICE_RATIO;
      this._cachedCostSavings += fullPriceCost - cachedCost;
    }
  }

  trackToolCall(): void {
    this.stats.toolCallCount++;
  }

  trackTurn(): void {
    this.stats.turnCount++;
  }

  getStats(): ExtendedUsageStats {
    const totalPromptTokens = this._cachedPromptTokens + this._uncachedPromptTokens;
    const cacheHitRate = totalPromptTokens > 0
      ? this._cachedPromptTokens / totalPromptTokens
      : 0;

    return {
      ...this.stats,
      cachedPromptTokens: this._cachedPromptTokens,
      uncachedPromptTokens: this._uncachedPromptTokens,
      cacheHitRate,
      cachedCostSavings: this._cachedCostSavings,
    };
  }

  reset(): void {
    this.stats = {
      promptTokens: 0,
      completionTokens: 0,
      reasoningTokens: 0,
      totalTokens: 0,
      totalCost: 0,
      requestCount: 0,
      toolCallCount: 0,
      turnCount: 0,
    };
    this._cachedPromptTokens = 0;
    this._uncachedPromptTokens = 0;
    this._cachedCostSavings = 0;
  }

  /**
   * Estimate cost with cache-aware pricing.
   * Cached prompt tokens are charged at CACHED_PRICE_RATIO of the normal input price.
   */
  private estimateCostWithCache(
    model: string,
    uncachedPromptTokens: number,
    cachedPromptTokens: number,
    completionTokens: number,
  ): number {
    const [inputPricePerMillion, outputPricePerMillion] = matchPricing(model);
    return (
      (uncachedPromptTokens / 1_000_000) * inputPricePerMillion +
      (cachedPromptTokens / 1_000_000) * inputPricePerMillion * CACHED_PRICE_RATIO +
      (completionTokens / 1_000_000) * outputPricePerMillion
    );
  }
}
