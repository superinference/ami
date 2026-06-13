/**
 * Cost tracking for SuperInference engine.
 *
 * Tracks token usage, tool calls, turns, and estimates dollar cost.
 * Prices are fetched from the Vercel AI Gateway on first use, with
 * a hardcoded fallback table for offline / timeout scenarios.
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

// ---------------------------------------------------------------------------
// Pricing: [inputPerMillion, outputPerMillion, cachedInputPerMillion | null]
// ---------------------------------------------------------------------------

type PricingEntry = [number, number, number | null];

// Fallback table used when the gateway fetch fails or hasn't completed yet.
const FALLBACK_PRICING: Record<string, PricingEntry> = {
  'gpt-4o': [2.5, 10, null],
  'gpt-4o-mini': [0.15, 0.6, null],
  'gemini-2.0-flash': [0.1, 0.4, null],
  'gemini-1.5-pro': [1.25, 5, null],
  'claude-sonnet-4': [3, 15, null],
  'claude-haiku': [0.25, 1.25, null],
  'claude-opus-4': [15, 75, null],
  'o1': [15, 60, null],
  'o1-mini': [3, 12, null],
  'o3': [10, 40, null],
  'o3-mini': [1.10, 4.40, null],
  'o4-mini': [1.10, 4.40, null],
  'gemini-2.5-pro': [1.25, 10, null],
  'gemini-2.5-flash': [0.15, 0.6, null],
  'deepseek-r1': [0.55, 2.19, null],
  'gpt-4-turbo': [10, 30, null],
  'gemini-3': [1.25, 10, null],
};

const DEFAULT_PRICING: PricingEntry = [1, 3, null];

/** Default ratio when no explicit cached-input price is available. */
const CACHED_PRICE_RATIO = 0.1;

// ---------------------------------------------------------------------------
// Live pricing from Vercel AI Gateway
// ---------------------------------------------------------------------------

const GATEWAY_URL = 'https://ai-gateway.vercel.sh/v1/models';
const FETCH_TIMEOUT_MS = 5_000;

let livePricing: Record<string, PricingEntry> | null = null;
let fetchPromise: Promise<void> | null = null;

function perTokenToPerMillion(perToken: string): number {
  return parseFloat(perToken) * 1_000_000;
}

interface GatewayModel {
  id?: string;
  pricing?: {
    input?: string;
    output?: string;
    input_cache_read?: string;
  };
}

async function fetchLivePricing(): Promise<void> {
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

    const res = await fetch(GATEWAY_URL, { signal: controller.signal });
    clearTimeout(timer);

    if (!res.ok) return;

    const json = await res.json() as { data?: GatewayModel[] };
    const models = json.data;
    if (!Array.isArray(models)) return;

    const map: Record<string, PricingEntry> = {};
    for (const m of models) {
      if (!m.id || !m.pricing?.input || !m.pricing?.output) continue;

      const input = perTokenToPerMillion(m.pricing.input);
      const output = perTokenToPerMillion(m.pricing.output);
      const cached = m.pricing.input_cache_read
        ? perTokenToPerMillion(m.pricing.input_cache_read)
        : null;

      if (isNaN(input) || isNaN(output)) continue;

      // Store under both the full id ("openai/gpt-4o") and the model name ("gpt-4o")
      map[m.id] = [input, output, cached];
      const slash = m.id.indexOf('/');
      if (slash !== -1) {
        const shortName = m.id.slice(slash + 1);
        if (!map[shortName]) map[shortName] = [input, output, cached];
      }
    }

    if (Object.keys(map).length > 0) {
      livePricing = map;
    }
  } catch {
    // Network error, timeout, or parse failure — fall back to hardcoded.
  }
}

/** Kick off the fetch (idempotent). Called once by the first CostTracker. */
function ensurePricingFetch(): void {
  if (!fetchPromise) {
    fetchPromise = fetchLivePricing();
  }
}

// ---------------------------------------------------------------------------
// Pricing lookup
// ---------------------------------------------------------------------------

function matchPricing(model: string): PricingEntry {
  const table = livePricing ?? FALLBACK_PRICING;

  if (table[model]) return table[model];

  // Sort keys by length descending so longer (more specific) keys match first
  const sortedKeys = Object.keys(table).sort((a, b) => b.length - a.length);
  for (const key of sortedKeys) {
    if (model.includes(key)) return table[key];
  }
  return DEFAULT_PRICING;
}

// ---------------------------------------------------------------------------
// CostTracker
// ---------------------------------------------------------------------------

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

  private _cachedPromptTokens = 0;
  private _uncachedPromptTokens = 0;
  private _cachedCostSavings = 0;
  private model: string;

  constructor(model: string) {
    this.model = model;
    ensurePricingFetch();
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

    const cost = this.estimateCostWithCache(
      this.model,
      uncachedTokens,
      cachedTokens,
      usage.completionTokens,
    );
    this.stats.totalCost += cost;
    this.stats.requestCount++;

    this._cachedPromptTokens += cachedTokens;
    this._uncachedPromptTokens += uncachedTokens;

    if (cachedTokens > 0) {
      const [inputPPM, , cachedPPM] = matchPricing(this.model);
      const fullPriceCost = (cachedTokens / 1_000_000) * inputPPM;
      const actualCachedPPM = cachedPPM ?? inputPPM * CACHED_PRICE_RATIO;
      const cachedCost = (cachedTokens / 1_000_000) * actualCachedPPM;
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

  private estimateCostWithCache(
    model: string,
    uncachedPromptTokens: number,
    cachedPromptTokens: number,
    completionTokens: number,
  ): number {
    const [inputPPM, outputPPM, cachedPPM] = matchPricing(model);
    const actualCachedPPM = cachedPPM ?? inputPPM * CACHED_PRICE_RATIO;
    return (
      (uncachedPromptTokens / 1_000_000) * inputPPM +
      (cachedPromptTokens / 1_000_000) * actualCachedPPM +
      (completionTokens / 1_000_000) * outputPPM
    );
  }
}

/** Reset the live pricing cache (for testing). */
export function _resetPricingCache(): void {
  livePricing = null;
  fetchPromise = null;
}

/** Fetch and await live pricing (for testing / eager init). */
export async function _fetchPricing(): Promise<void> {
  fetchPromise = fetchLivePricing();
  await fetchPromise;
}
