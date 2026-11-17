/**
 * Cost tracking for SuperInference engine.
 *
 * Tracks token usage, tool calls, turns, and estimates dollar cost.
 * Prices are fetched from the Vercel AI Gateway on first use, with
 * a hardcoded fallback table for offline / timeout scenarios.
 */

import { mkdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { UsageStats } from './types';
import type { FpsMetrics } from './profiler';
import { isEssentialTrafficOnly } from './analytics';
import { getContextWindow, getModelMaxOutputTokens } from './model-capabilities';

/**
 * Per-request execution speed tier. A `fast` (premium) request can be billed at an
 * elevated pricing tier — mirrors Claude's `usage.speed` → fast-mode cost tier. AMI
 * maps Anthropic's `service_tier: 'priority'` to `'fast'`. Absent/`medium`/`slow`
 * price at the model's standard rate.
 */
export type ModelSpeed = 'fast' | 'medium' | 'slow';

/**
 * Extended stats that include prompt caching information.
 * Defined inline to avoid modifying types.ts (handled by another agent).
 */
export interface ExtendedUsageStats extends UsageStats {
  cachedPromptTokens: number;
  uncachedPromptTokens: number;
  /** Cache-write (cache_creation) tokens billed at a premium over input. */
  cacheCreationTokens: number;
  /** Fraction of prompt tokens served from cache (0..1). */
  cacheHitRate: number;
  /** Estimated dollar savings from prompt caching. */
  cachedCostSavings: number;
  byCategory?: Record<string, number>;
  /** Per-model usage/cost breakdown — survives mid-session model switches. */
  byModel: Record<string, ModelCostUsage>;
  /** Total server-side web-search requests billed this session. */
  webSearchRequests: number;
  /** 'estimated' when any priced model wasn't in the pricing table (dollars are approximate). */
  costAccuracy: 'exact' | 'estimated';
  /** Models whose cost fell back to the generic default rate (unknown pricing). */
  unknownModels: string[];
  /**
   * Total wall time (ms) spent in API requests, INCLUDING retries — so time lost to
   * transient failures/retries is observable. 0 when no duration was recorded.
   */
  apiDurationMs: number;
  /**
   * Total API wall time (ms) counting only the successful (non-retry) attempt of each
   * request. `apiDurationMs - apiDurationMsWithoutRetries` is the time cost of retries.
   */
  apiDurationMsWithoutRetries: number;
}

/** Per-model usage accumulation used by the running cost total. */
export interface ModelCostUsage {
  promptTokens: number;
  completionTokens: number;
  cachedPromptTokens: number;
  cacheCreationTokens: number;
  totalTokens: number;
  cost: number;
  requestCount: number;
  /** Server-side web_search requests billed against THIS model (for a per-model breakdown). */
  webSearchRequests: number;
  /**
   * This model's total context window (tokens), resolved from model-capabilities — lets the
   * Usage view annotate each model's window alongside its spend. Mirrors Claude's per-model
   * `contextWindow` on the usage record. Undefined when the model is unknown to capabilities.
   */
  contextWindow?: number;
  /**
   * This model's max-output-token ceiling, resolved from model-capabilities. Undefined when
   * the model has no known output cap (callers then show only the context window).
   */
  maxOutputTokens?: number;
}

// ---------------------------------------------------------------------------
// Pricing: [inputPerMillion, outputPerMillion, cachedInputPerMillion | null,
//           cacheWritePerMillion? | null,
//           fastInputPerMillion? | null, fastOutputPerMillion? | null]
//
// The optional 4th slot is a MODEL-SPECIFIC cache-write (cache_creation) price.
// Providers' cache-write multiplier is not uniformly 1.25x (Anthropic 5m TTL),
// so when the gateway advertises one we honor it; absent, we fall back to the
// global CACHE_WRITE_PRICE_RATIO. Legacy 3-element entries are still valid.
//
// The optional 5th/6th slots are an ELEVATED-SPEED (fast / priority) pricing tier:
// a `fast` request is billed at fastInput/fastOutput instead of the standard rate,
// mirroring Claude's speed-dependent cost tier (getOpus46CostTier(fastMode)). When
// absent, a fast request prices at the standard rate (no premium). Cached/cache-write
// derive from the fast input rate at the same ratios when not themselves explicit.
// ---------------------------------------------------------------------------

type PricingEntry = [
  number, number, number | null, (number | null)?, (number | null)?, (number | null)?,
];

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

/**
 * Cache-WRITE multiplier on the input rate. Anthropic bills cache_creation
 * tokens at 1.25× the base input price for the default 5-minute TTL (2× for the
 * 1-hour TTL). We use the 5m default; without it, cache writes are billed as
 * free and the session undercounts real spend.
 */
const CACHE_WRITE_PRICE_RATIO = 1.25;

/**
 * Cache-WRITE multiplier for the 1-hour (extended) TTL. Anthropic bills a
 * 1-hour cache write at 2× the base input price (vs 1.25× for the 5-minute TTL).
 * When a usage record splits its cache-creation tokens into an ephemeral-1h vs
 * ephemeral-5m breakdown (Anthropic Usage.cache_creation), re-pricing the 1h
 * portion at 1.25× would undercount a long-TTL-cache session's real spend.
 */
const CACHE_WRITE_1H_PRICE_RATIO = 2;

/**
 * Billing service tiers (Anthropic Usage.service_tier). `batch` is billed at
 * HALF the standard per-token rate; `priority`/`standard` at the standard rate
 * (priority's surcharge is not publicly a flat multiplier, so it re-prices at
 * standard rather than guessing). Re-pricing a batch-billed session at the
 * standard rate would OVERSTATE its cost by 2×, so the cross-agent spend view
 * must honor the recorded tier.
 */
export type UsageServiceTier = 'standard' | 'batch' | 'priority';

/** Per-token discount applied to a `batch` service-tier usage record (−50%). */
const BATCH_TIER_DISCOUNT = 0.5;

/**
 * Flat per-request charge for a server-side web_search tool use. Anthropic bills
 * web search at $10 / 1000 requests = $0.01 each, independent of token usage; a
 * session that web-searches undercounts spend without this component.
 */
export const WEB_SEARCH_COST_PER_REQUEST = 0.01;

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
    /** Per-token cache-WRITE (cache_creation) price, when the gateway advertises one. */
    input_cache_write?: string;
    /** Per-token ELEVATED-SPEED (fast / priority service-tier) input price, when advertised. */
    input_priority?: string;
    /** Per-token ELEVATED-SPEED (fast / priority service-tier) output price, when advertised. */
    output_priority?: string;
  };
}

async function fetchLivePricing(): Promise<void> {
  // Live pricing is a NONESSENTIAL outbound request — honor the privacy/offline
  // kill-switch (SUPERINFERENCE_/CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC) and skip
  // the gateway fetch entirely, falling back to the hardcoded table.
  if (isEssentialTrafficOnly()) return;
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
      const cacheWrite = m.pricing.input_cache_write
        ? perTokenToPerMillion(m.pricing.input_cache_write)
        : null;
      const fastInput = m.pricing.input_priority
        ? perTokenToPerMillion(m.pricing.input_priority)
        : null;
      const fastOutput = m.pricing.output_priority
        ? perTokenToPerMillion(m.pricing.output_priority)
        : null;

      if (isNaN(input) || isNaN(output) || (cached !== null && isNaN(cached)) ||
          (cacheWrite !== null && isNaN(cacheWrite)) ||
          (fastInput !== null && isNaN(fastInput)) ||
          (fastOutput !== null && isNaN(fastOutput))) continue;

      // Store under both the full id ("openai/gpt-4o") and the model name ("gpt-4o")
      map[m.id] = [input, output, cached, cacheWrite, fastInput, fastOutput];
      const slash = m.id.indexOf('/');
      if (slash !== -1) {
        const shortName = m.id.slice(slash + 1);
        if (!map[shortName]) map[shortName] = [input, output, cached, cacheWrite, fastInput, fastOutput];
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

function matchPricing(model: string, fallbackModel?: string): PricingEntry {
  const table = livePricing ?? FALLBACK_PRICING;

  if (table[model]) return table[model];

  // Sort keys by length descending so longer (more specific) keys match first
  const sortedKeys = Object.keys(table).sort((a, b) => b.length - a.length);
  for (const key of sortedKeys) {
    if (model.includes(key)) return table[key];
  }
  // Unknown model: before using the generic DEFAULT_PRICING, fall back to the
  // user's configured default (main-loop) model's REAL tier when one was supplied
  // and is itself priced — mirroring Claude's getModelCosts(), which prices an
  // unknown model at MODEL_COSTS[defaultMainLoopModel] rather than a flat constant.
  // The caller still flags the request 'estimated' (isPricedModel is false for it).
  if (fallbackModel && fallbackModel !== model && isPricedModel(fallbackModel)) {
    return matchPricing(fallbackModel);
  }
  return DEFAULT_PRICING;
}

/**
 * True when `model` resolves to a REAL pricing-table entry (exact or substring), not the generic
 * DEFAULT_PRICING fallback. The cross-session reader uses this to flag spend as estimated for
 * models it can't price precisely (common for other agents' models in a cross-agent view).
 */
export function isPricedModel(model: string): boolean {
  if (!model) return false;
  const table = livePricing ?? FALLBACK_PRICING;
  if (table[model]) return true;
  return Object.keys(table).some(key => model.includes(key));
}

// ---------------------------------------------------------------------------
// Pricing display (model-picker affordance)
// ---------------------------------------------------------------------------

/** Format a per-million price: integers without decimals ("$3"), else 2dp ("$0.80", "$22.50"). */
export function formatPrice(perMillion: number): string {
  if (!Number.isFinite(perMillion) || perMillion < 0) return '$?';
  return Number.isInteger(perMillion) ? `$${perMillion}` : `$${perMillion.toFixed(2)}`;
}

/** Format an input/output per-million pair as "$3/$15 per Mtok". */
export function formatModelPricing(inputPerMillion: number, outputPerMillion: number): string {
  return `${formatPrice(inputPerMillion)}/${formatPrice(outputPerMillion)} per Mtok`;
}

/**
 * Human-readable pricing string for a model ("$3/$15 per Mtok"), resolved via the
 * same live/fallback table as the cost estimator — a cost-aware affordance for the
 * model picker. Returns '' when the model only resolves to the generic default tier
 * (so the picker doesn't present a guessed price as if it were real).
 */
export function getModelPricingString(model: string): string {
  if (!isPricedModel(model)) return '';
  const [inputPPM, outputPPM] = matchPricing(model);
  return formatModelPricing(inputPPM, outputPPM);
}

// ---------------------------------------------------------------------------
// CostTracker
// ---------------------------------------------------------------------------

/** Classify a tool call for per-category usage breakdown. */
export function categorizeTool(name: string): 'mcp' | 'skill' | 'subagent' | 'builtin' {
  if (name.startsWith('mcp__') || name.includes(':')) return 'mcp';
  if (name === 'task' || name.startsWith('task_') || name === 'workflow') return 'subagent';
  if (name === 'skill' || name.toLowerCase().startsWith('skill')) return 'skill';
  return 'builtin';
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

  private _cachedPromptTokens = 0;
  private _uncachedPromptTokens = 0;
  private _cacheCreationTokens = 0;
  private _cachedCostSavings = 0;
  private _webSearchRequests = 0;
  private _byCategory: Record<string, number> = {};
  private _byModel = new Map<string, ModelCostUsage>();
  private _unknownModels = new Set<string>();
  private _apiDurationMs = 0;
  private _apiDurationMsWithoutRetries = 0;
  private model: string;

  constructor(model: string) {
    this.model = model;
    ensurePricingFetch();
  }

  /**
   * Record one usage sample. `model` prices THIS request by its actual model and
   * attributes it in the per-model map, so the running total survives a mid-session
   * /model switch (defaults to the tracker's constructor model). `webSearchRequests`
   * adds the flat per-request server-tool charge.
   */
  trackUsage(usage: {
    promptTokens: number;
    completionTokens: number;
    reasoningTokens?: number;
    cachedPromptTokens?: number;
    cacheCreationTokens?: number;
    webSearchRequests?: number;
    /** Execution speed of this request; `'fast'` bills at the model's premium tier when it has one. */
    speed?: ModelSpeed;
  }, model?: string): void {
    const activeModel = model ?? this.model;
    const cachedTokens = usage.cachedPromptTokens ?? 0;
    const cacheCreationTokens = usage.cacheCreationTokens ?? 0;
    const webSearchRequests = Math.max(0, usage.webSearchRequests ?? 0);
    const uncachedTokens = usage.promptTokens - cachedTokens;

    this.stats.promptTokens += usage.promptTokens;
    this.stats.completionTokens += usage.completionTokens;
    this.stats.reasoningTokens += usage.reasoningTokens ?? 0;
    this.stats.totalTokens += usage.promptTokens + usage.completionTokens;

    const cost = estimateUsageCost(activeModel, {
      uncachedPromptTokens: uncachedTokens,
      cachedPromptTokens: cachedTokens,
      cacheCreationTokens,
      completionTokens: usage.completionTokens,
      webSearchRequests,
    }, this.model, usage.speed);
    this.stats.totalCost += cost;
    this.stats.requestCount++;

    this._cachedPromptTokens += cachedTokens;
    this._uncachedPromptTokens += uncachedTokens;
    this._cacheCreationTokens += cacheCreationTokens;
    this._webSearchRequests += webSearchRequests;

    // Per-model attribution (cost survives model switches).
    const m = this._byModel.get(activeModel) ?? {
      promptTokens: 0, completionTokens: 0, cachedPromptTokens: 0,
      cacheCreationTokens: 0, totalTokens: 0, cost: 0, requestCount: 0,
      webSearchRequests: 0,
    };
    m.promptTokens += usage.promptTokens;
    m.completionTokens += usage.completionTokens;
    m.cachedPromptTokens += cachedTokens;
    m.cacheCreationTokens += cacheCreationTokens;
    m.totalTokens += usage.promptTokens + usage.completionTokens;
    m.cost += cost;
    m.requestCount++;
    m.webSearchRequests += webSearchRequests;
    this._byModel.set(activeModel, m);

    // Flag models we couldn't price precisely so the live display can warn.
    if (activeModel && !isPricedModel(activeModel)) this._unknownModels.add(activeModel);

    if (cachedTokens > 0) {
      const [stdInputPPM, , cachedPPM, , fastInputPPM] = matchPricing(activeModel, this.model);
      const inputPPM = usage.speed === 'fast' && fastInputPPM != null ? fastInputPPM : stdInputPPM;
      const fullPriceCost = (cachedTokens / 1_000_000) * inputPPM;
      const actualCachedPPM = cachedPPM ?? inputPPM * CACHED_PRICE_RATIO;
      const cachedCost = (cachedTokens / 1_000_000) * actualCachedPPM;
      this._cachedCostSavings += fullPriceCost - cachedCost;
    }
  }

  /**
   * Re-seed the running aggregate totals from a prior session's persisted
   * {@link CostState} so a RESUMED session's live `/cost` continues from where it
   * left off instead of restarting at $0 (PARITY — Claude's
   * restoreCostStateForSession/setCostStateForRestore, gated on a session-id match
   * by the caller). Only the coarse totals are restored — cost, input/output token
   * counts (and their sum), and request count — since those are all the on-disk
   * cost state carries; the per-model/per-category/cache breakdowns rebuild as the
   * resumed session does new work. Called once, on resume, before any new usage is
   * tracked; negative fields are clamped to 0.
   */
  restoreTotals(state: CostState): void {
    this.stats.totalCost += Math.max(0, state.totalCost);
    this.stats.promptTokens += Math.max(0, state.totalInputTokens);
    this.stats.completionTokens += Math.max(0, state.totalOutputTokens);
    this.stats.totalTokens += Math.max(0, state.totalInputTokens) + Math.max(0, state.totalOutputTokens);
    this.stats.requestCount += Math.max(0, state.requestCount);
  }

  trackToolCall(toolName = ''): void {
    this.stats.toolCallCount++;
    if (toolName) {
      const cat = categorizeTool(toolName);
      this._byCategory[cat] = (this._byCategory[cat] ?? 0) + 1;
    }
  }

  trackTurn(): void {
    this.stats.turnCount++;
  }

  /**
   * Record the wall time (ms) of one API request so retry overhead is observable.
   * Every call adds to the total; a retry attempt (`wasRetry: true`) adds to the
   * total ONLY, so the without-retries figure reflects just the successful attempt.
   * Non-finite / negative durations are ignored.
   */
  trackRequestDuration(durationMs: number, opts?: { wasRetry?: boolean }): void {
    const ms = Number.isFinite(durationMs) && durationMs > 0 ? durationMs : 0;
    if (ms === 0) return;
    this._apiDurationMs += ms;
    if (!opts?.wasRetry) this._apiDurationMsWithoutRetries += ms;
  }

  getStats(): ExtendedUsageStats {
    const totalPromptTokens = this._cachedPromptTokens + this._uncachedPromptTokens;
    const cacheHitRate = totalPromptTokens > 0
      ? this._cachedPromptTokens / totalPromptTokens
      : 0;

    const byModel: Record<string, ModelCostUsage> = {};
    for (const [model, u] of this._byModel) {
      // Annotate each per-model rollup with its context window / max-output ceiling so the
      // Usage view can show each model's limits next to its spend (mirrors Claude's per-model
      // contextWindow/maxOutputTokens on the usage record).
      byModel[model] = {
        ...u,
        contextWindow: getContextWindow(model),
        maxOutputTokens: getModelMaxOutputTokens(model).default,
      };
    }

    return {
      ...this.stats,
      cachedPromptTokens: this._cachedPromptTokens,
      uncachedPromptTokens: this._uncachedPromptTokens,
      cacheCreationTokens: this._cacheCreationTokens,
      cacheHitRate,
      cachedCostSavings: this._cachedCostSavings,
      byCategory: { ...this._byCategory },
      byModel,
      webSearchRequests: this._webSearchRequests,
      costAccuracy: this._unknownModels.size > 0 ? 'estimated' : 'exact',
      unknownModels: [...this._unknownModels],
      apiDurationMs: this._apiDurationMs,
      apiDurationMsWithoutRetries: this._apiDurationMsWithoutRetries,
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
    this._cacheCreationTokens = 0;
    this._cachedCostSavings = 0;
    this._webSearchRequests = 0;
    this._byCategory = {};
    this._byModel.clear();
    this._unknownModels.clear();
    this._apiDurationMs = 0;
    this._apiDurationMsWithoutRetries = 0;
  }
}

/**
 * Pure cost estimate for one usage record — the SINGLE source of truth shared by the live
 * CostTracker and any after-the-fact re-pricing (e.g. the cross-session journal reader), so
 * a replayed session can never disagree with the live session's dollars. Uses the same
 * live/fallback pricing table + cache ratios as trackUsage.
 */
export function estimateUsageCost(
  model: string,
  u: {
    uncachedPromptTokens: number;
    cachedPromptTokens: number;
    cacheCreationTokens: number;
    completionTokens: number;
    /** Count of server-side web_search requests in this usage record. */
    webSearchRequests?: number;
    /**
     * Portion of `cacheCreationTokens` written at the 1-hour (extended) TTL — priced
     * at {@link CACHE_WRITE_1H_PRICE_RATIO}× input instead of the 5-minute 1.25×. The
     * remaining `cacheCreationTokens − cacheCreation1hTokens` is billed at the 5m rate.
     * Omit (or 0) for a pure 5-minute cache write.
     */
    cacheCreation1hTokens?: number;
    /**
     * Anthropic billing service tier for this record. A `batch`-tier record is
     * re-priced at half the standard per-token cost; `standard`/`priority` at the
     * standard rate. Omit to assume standard-tier pricing.
     */
    serviceTier?: UsageServiceTier;
  },
  /**
   * Configured default (main-loop) model whose REAL pricing tier is used when `model`
   * itself is unknown, instead of the generic DEFAULT_PRICING constant — mirrors Claude's
   * unknown-model fallback to the default model's cost. Omit to keep the generic fallback.
   */
  fallbackModel?: string,
  /**
   * Per-request execution speed. When `'fast'` AND the model advertises an elevated-speed
   * (priority) tier, the request is billed at that premium tier — mirrors Claude's
   * speed-dependent cost tier (getOpus46CostTier(fastMode)). Absent / `'medium'` / `'slow'`
   * bills at the standard rate.
   */
  speed?: ModelSpeed,
): number {
  const [stdInputPPM, stdOutputPPM, cachedPPM, cacheWritePPM, fastInputPPM, fastOutputPPM] =
    matchPricing(model, fallbackModel);
  // Elevated-speed (fast/priority) tier: only when the request is fast AND the model
  // actually advertises a fast tier, otherwise fall back to the standard rate.
  const useFast = speed === 'fast' && fastInputPPM != null && fastOutputPPM != null;
  const inputPPM = useFast ? fastInputPPM : stdInputPPM;
  const outputPPM = useFast ? fastOutputPPM : stdOutputPPM;
  const actualCachedPPM = cachedPPM ?? inputPPM * CACHED_PRICE_RATIO;
  // Prefer a model-specific cache-write price; fall back to the global 1.25x ratio.
  const actualCacheWritePPM = cacheWritePPM ?? inputPPM * CACHE_WRITE_PRICE_RATIO;
  // Split cache-creation into its 1-hour (2× input) and 5-minute (1.25×) portions.
  // The 1h price is always input-rate-relative (no per-model 1h column exists); the
  // 5m portion keeps the model-specific/global cache-write rate. Clamp the 1h slice to
  // the total so a malformed record can't price negative 5m tokens.
  const totalCacheCreate = Math.max(0, u.cacheCreationTokens);
  const create1h = Math.min(totalCacheCreate, Math.max(0, u.cacheCreation1hTokens ?? 0));
  const create5m = totalCacheCreate - create1h;
  const gross =
    (Math.max(0, u.uncachedPromptTokens) / 1_000_000) * inputPPM +
    (Math.max(0, u.cachedPromptTokens) / 1_000_000) * actualCachedPPM +
    (create5m / 1_000_000) * actualCacheWritePPM +
    (create1h / 1_000_000) * inputPPM * CACHE_WRITE_1H_PRICE_RATIO +
    (Math.max(0, u.completionTokens) / 1_000_000) * outputPPM +
    Math.max(0, u.webSearchRequests ?? 0) * WEB_SEARCH_COST_PER_REQUEST;
  // Batch-tier records are billed at half the standard per-token/-request cost.
  return u.serviceTier === 'batch' ? gross * BATCH_TIER_DISCOUNT : gross;
}

// ---------------------------------------------------------------------------
// Cache miss diagnostics
// ---------------------------------------------------------------------------

export const CACHE_MISS_CAUSES = [
  'tool_definitions_changed', 'system_prompt_changed', 'idle_past_ttl', 'idle_past_1h_ttl',
  'thinking_mode_changed', 'model_switched', 'betas_changed', 'cache_control_changed',
  'extra_body_changed', 'effort_changed', 'fast_mode_changed', 'global_cache_strategy_changed',
  'first_request', 'unknown',
] as const;
export type CacheMissCause = (typeof CACHE_MISS_CAUSES)[number];

export interface CacheDiagRequest {
  toolDefHash: string;
  systemPromptHash: string;
  thinkingMode: string;
  model: string;
  /**
   * Optional per-tool schema hash map (tool name → hash of its full stripped
   * schema). When present, a tool-definition cache break can name EXACTLY which
   * tool's description/schema changed — ~77% of tool breaks are same-tool-set
   * description changes a name-only hash can't see.
   */
  perToolHashes?: Record<string, string>;
  /** Length (chars) of the joined system prompt — used for the "+N chars" delta. */
  systemPromptChars?: number;
  /** Sorted anthropic-beta header list (diffed added/removed between calls). */
  betas?: string[];
  /** Hash of the cache_control scope/TTL config (global↔org, 1h↔5m flips a stripped hash misses). */
  cacheControlHash?: string;
  /** Hash of CLAUDE_CODE_EXTRA_BODY / anthropic_internal extra request body. */
  extraBodyHash?: string;
  /** Reasoning-effort value (e.g. default / high). */
  effortValue?: string;
  /** Fast-mode flag. */
  fastMode?: boolean;
  /** Global cache strategy label (e.g. none / tool_based). */
  globalCacheStrategy?: string;
  /**
   * Lazy thunk returning this request's canonical diffable prompt-state string (model +
   * system prompt + sorted per-tool schema, via buildDiffableContent). Deferred so it is
   * only materialized when a break is actually scored — zero overhead on cache hits.
   * When both the previous and current requests supply one, a unified diff of the two is
   * written to a temp file on break and its path surfaced via getLastDiffPath().
   */
  buildDiffableContent?: () => string;
}

/** One concurrent cache-break cause with a human-readable magnitude/detail. */
export interface CacheBreakCause {
  cause: CacheMissCause;
  detail: string;
}

/** Which tools were added/removed/changed between two per-tool hash maps. */
export interface ToolSchemaDelta {
  added: string[];
  removed: string[];
  changed: string[];
}

/** Diff two per-tool hash maps into added/removed/changed tool-name lists. */
export function diffToolHashes(
  prev: Record<string, string> | undefined,
  next: Record<string, string> | undefined,
): ToolSchemaDelta {
  const p = prev ?? {};
  const n = next ?? {};
  const added: string[] = [];
  const removed: string[] = [];
  const changed: string[] = [];
  for (const name of Object.keys(n)) {
    if (!(name in p)) added.push(name);
    else if (p[name] !== n[name]) changed.push(name);
  }
  for (const name of Object.keys(p)) {
    if (!(name in n)) removed.push(name);
  }
  return { added: added.sort(), removed: removed.sort(), changed: changed.sort() };
}

const DEFAULT_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes
const DEFAULT_CACHE_TTL_1H_MS = 60 * 60 * 1000; // 1 hour (extended TTL)
/** Minimum absolute cache-read token drop to count a PARTIAL cache break (noise floor). */
export const MIN_CACHE_MISS_TOKENS = 2000;
/** A partial break also requires cache reads to drop below this fraction of the prior call. */
export const PARTIAL_CACHE_BREAK_RATIO = 0.95;

/** Diff two string lists into sorted added/removed name arrays (betas, etc.). */
export function diffStringLists(
  prev: readonly string[] | undefined,
  next: readonly string[] | undefined,
): { added: string[]; removed: string[] } {
  const p = new Set(prev ?? []);
  const n = new Set(next ?? []);
  return {
    added: [...n].filter(x => !p.has(x)).sort(),
    removed: [...p].filter(x => !n.has(x)).sort(),
  };
}

/**
 * Models excluded from cache-break detection. Some models' server-side prompt-cache
 * semantics differ enough (e.g. Anthropic Haiku) that a cache-read drop is NOT a
 * comparable break signal, so scoring them produces false-positive break notifications.
 * Mirrors Claude's isExcludedModel (promptCacheBreakDetection.ts).
 */
export function isExcludedModel(model: string): boolean {
  return /haiku/i.test(model);
}

export function fingerprint(input: string): string {
  const len = input.length;
  const head = input.slice(0, 100);
  const tail = input.slice(-100);
  let hash = 0;
  const sample = head + tail;
  for (let i = 0; i < sample.length; i++) {
    hash = ((hash << 5) - hash + sample.charCodeAt(i)) | 0;
  }
  return `${len}:${hash >>> 0}`;
}

// ---------------------------------------------------------------------------
// Prompt-state diff artifact (cache-break debugging affordance)
//
// On a prompt-cache break Claude Code serializes the full before/after prompt
// state (model + joined system prompt + sorted per-tool name/description/schema)
// and writes a unified diff to a temp file, embedding the path in the break
// warning so the exact byte that busted the cache is inspectable. AMI mirrors
// this: buildDiffableContent() renders the canonical state string, and on a break
// CacheMissDiagnostics writes createUnifiedPatch() of the prev→current state to a
// temp file whose path is surfaced through getLastDiffPath() (→ /usage + inspector).
// ---------------------------------------------------------------------------

/** Minimal tool shape needed to render a diffable prompt-state representation. */
export interface DiffableTool {
  name?: string;
  description?: string;
  inputSchema?: unknown;
}

/**
 * Render the canonical, order-stable "prompt state" string diffed on a cache break:
 * the model, the joined system prompt, and every tool's name/description/input-schema
 * sorted by name (so a stable tool set with a reordered advertise list does NOT look
 * like a change). Mirrors Claude's buildDiffableContent (promptCacheBreakDetection.ts).
 */
export function buildDiffableContent(
  model: string,
  systemText: string,
  tools: readonly DiffableTool[],
): string {
  const toolDetails = tools
    .map(t => {
      const name = t.name ?? 'unknown';
      const desc = t.description ?? '';
      let schema = '';
      if (t.inputSchema !== undefined) {
        try { schema = JSON.stringify(t.inputSchema); } catch { schema = '[unserializable]'; }
      }
      return `${name}\n  description: ${desc}\n  input_schema: ${schema}`;
    })
    .sort()
    .join('\n\n');
  return `Model: ${model}\n\n=== System Prompt ===\n\n${systemText}\n\n=== Tools (${tools.length}) ===\n\n${toolDetails}\n`;
}

type DiffOp = { t: '=' | '-' | '+'; line: string };

/** Split into lines, dropping a single trailing empty element from a trailing newline. */
function splitDiffLines(s: string): string[] {
  const lines = s.split('\n');
  if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
  return lines;
}

/** Guard: beyond this LCS cell count fall back to a coarse remove-all/add-all diff. */
const MAX_LCS_CELLS = 4_000_000;

/** LCS-based line diff of two already prefix/suffix-trimmed segments. */
function lcsDiff(a: string[], b: string[]): DiffOp[] {
  const n = a.length;
  const m = b.length;
  // dp[i][j] = LCS length of a[i:] and b[j:].
  const dp: number[][] = Array.from({ length: n + 1 }, () => new Array<number>(m + 1).fill(0));
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      dp[i][j] = a[i] === b[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
    }
  }
  const out: DiffOp[] = [];
  let i = 0;
  let j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j]) { out.push({ t: '=', line: a[i] }); i++; j++; }
    else if (dp[i + 1][j] >= dp[i][j + 1]) { out.push({ t: '-', line: a[i] }); i++; }
    else { out.push({ t: '+', line: b[j] }); j++; }
  }
  while (i < n) out.push({ t: '-', line: a[i++] });
  while (j < m) out.push({ t: '+', line: b[j++] });
  return out;
}

/** Diff two line arrays into ops, trimming common prefix/suffix first (cheap for a small change). */
function diffOps(a: string[], b: string[]): DiffOp[] {
  let start = 0;
  while (start < a.length && start < b.length && a[start] === b[start]) start++;
  let endA = a.length;
  let endB = b.length;
  while (endA > start && endB > start && a[endA - 1] === b[endB - 1]) { endA--; endB--; }
  const midA = a.slice(start, endA);
  const midB = b.slice(start, endB);
  const ops: DiffOp[] = [];
  for (let k = 0; k < start; k++) ops.push({ t: '=', line: a[k] });
  if (midA.length * midB.length > MAX_LCS_CELLS) {
    // Too large for an O(n·m) LCS — emit a coarse (correct, if noisy) diff.
    for (const line of midA) ops.push({ t: '-', line });
    for (const line of midB) ops.push({ t: '+', line });
  } else {
    ops.push(...lcsDiff(midA, midB));
  }
  for (let k = endA; k < a.length; k++) ops.push({ t: '=', line: a[k] });
  return ops;
}

/** Number of unchanged context lines kept around each change hunk. */
const DIFF_CONTEXT = 3;

/**
 * Produce a standard unified diff (`--- / +++` header, `@@ -a,b +c,d @@` hunks with
 * {@link DIFF_CONTEXT} lines of context) of `oldStr`→`newStr`. A self-contained
 * replacement for the `diff` package's createPatch (no third-party dependency), used
 * to serialize a prompt-cache-break's before/after state. Returns '' when identical.
 */
export function createUnifiedPatch(
  fileName: string,
  oldStr: string,
  newStr: string,
  oldHeader = 'before',
  newHeader = 'after',
): string {
  const ops = diffOps(splitDiffLines(oldStr), splitDiffLines(newStr));
  if (!ops.some(o => o.t !== '=')) return '';

  // Group ops into hunks separated by runs of > 2·context unchanged lines.
  type Hunk = { oldStart: number; oldLines: number; newStart: number; newLines: number; rows: string[] };
  const hunks: Hunk[] = [];
  let oldLine = 1;
  let newLine = 1;
  let current: Hunk | null = null;
  let trailingContext = 0;

  const flush = (): void => { if (current) { hunks.push(current); current = null; trailingContext = 0; } };

  for (let idx = 0; idx < ops.length; idx++) {
    const op = ops[idx];
    if (op.t === '=') {
      if (current) {
        // Look ahead: is another change within 2·context lines? If so keep the gap as context.
        let nextChange = -1;
        for (let k = idx; k < ops.length; k++) { if (ops[k].t !== '=') { nextChange = k; break; } }
        const gap = nextChange === -1 ? Infinity : nextChange - idx;
        if (gap > DIFF_CONTEXT * 2) {
          // Close the hunk with up to DIFF_CONTEXT trailing context lines.
          for (let c = 0; c < DIFF_CONTEXT && idx + c < ops.length && ops[idx + c].t === '='; c++) {
            current.rows.push(` ${ops[idx + c].line}`);
            current.oldLines++; current.newLines++;
          }
          oldLine += gap;
          newLine += gap;
          flush();
          idx += gap - 1;
          continue;
        }
        current.rows.push(` ${op.line}`);
        current.oldLines++; current.newLines++;
        trailingContext++;
      }
      oldLine++; newLine++;
    } else {
      if (!current) {
        // Open a hunk with up to DIFF_CONTEXT leading context lines from just before idx.
        const lead: string[] = [];
        let li = idx - 1;
        while (li >= 0 && ops[li].t === '=' && lead.length < DIFF_CONTEXT) { lead.unshift(` ${ops[li].line}`); li--; }
        current = {
          oldStart: oldLine - lead.length,
          newStart: newLine - lead.length,
          oldLines: lead.length,
          newLines: lead.length,
          rows: lead,
        };
      }
      trailingContext = 0;
      if (op.t === '-') { current.rows.push(`-${op.line}`); current.oldLines++; oldLine++; }
      else { current.rows.push(`+${op.line}`); current.newLines++; newLine++; }
    }
  }
  if (current) {
    // Drop excess trailing context beyond DIFF_CONTEXT accumulated at the tail.
    const extra = Math.max(0, trailingContext - DIFF_CONTEXT);
    if (extra > 0) {
      current.rows.splice(current.rows.length - extra, extra);
      current.oldLines -= extra; current.newLines -= extra;
    }
    flush();
  }

  const headerLines = [`--- ${fileName}\t${oldHeader}`, `+++ ${fileName}\t${newHeader}`];
  const body = hunks.map(h => {
    const head = `@@ -${h.oldStart},${h.oldLines} +${h.newStart},${h.newLines} @@`;
    return [head, ...h.rows].join('\n');
  });
  return `${headerLines.join('\n')}\n${body.join('\n')}\n`;
}

let _diffArtifactCounter = 0;

/**
 * Write a unified diff of the prev→current prompt-state to a temp file and return its
 * path (undefined on any failure). Mirrors Claude's writeCacheBreakDiff — the path is
 * surfaced in the cache-break warning so the exact prompt change is inspectable.
 */
export function writeCacheBreakDiff(prevContent: string, newContent: string): string | undefined {
  try {
    const patch = createUnifiedPatch('prompt-state', prevContent, newContent, 'before', 'after');
    if (!patch) return undefined;
    const dir = join(tmpdir(), 'ami-cache-break-diffs');
    mkdirSync(dir, { recursive: true });
    const path = join(dir, `cache-break-${Date.now()}-${process.pid}-${_diffArtifactCounter++}.diff`);
    writeFileSync(path, patch);
    return path;
  } catch {
    return undefined;
  }
}

export class CacheMissDiagnostics {
  private causes = new Map<CacheMissCause, number>();
  private lastCause: CacheMissCause | null = null;
  private prev: CacheDiagRequest | null = null;
  private lastRequestTime = 0;
  private cacheTtlMs: number;
  private isFirstRequest = true;
  private _prevCacheReadTokens = 0;
  private _partialBreaks = 0;
  private _lastDelta: ToolSchemaDelta = { added: [], removed: [], changed: [] };
  /** All concurrent causes (with magnitudes) of the most recent cache break. */
  private _lastCauses: CacheBreakCause[] = [];
  /** When true, the NEXT cache-read drop is EXPECTED (compaction/cache-deletion) and not flagged. */
  private _expectNextDrop = false;
  private _expectedDrops = 0;
  /** Temp-file path of the prev→current prompt-state unified diff for the most recent break. */
  private _lastDiffPath: string | undefined;

  constructor(cacheTtlMs?: number) {
    this.cacheTtlMs = cacheTtlMs ?? DEFAULT_CACHE_TTL_MS;
  }

  recordRequest(opts: CacheDiagRequest): void {
    this._pendingRequest = opts;
    this._pendingRequestTime = Date.now();
  }

  private _pendingRequest: CacheDiagRequest | null = null;
  private _pendingRequestTime = 0;

  recordResponse(cacheReadTokens: number, _cacheCreationTokens: number): void {
    const req = this._pendingRequest;
    if (!req) return;
    this._pendingRequest = null;

    // Excluded models (e.g. haiku) have different server-side caching semantics, so a
    // cache-read drop isn't a comparable break signal — skip scoring to avoid false
    // positives, but still advance the baseline so a later switch to a tracked model
    // compares against fresh state.
    if (isExcludedModel(req.model)) {
      this._lastDelta = { added: [], removed: [], changed: [] };
      this._lastCauses = [];
      this._lastDiffPath = undefined;
      this._advanceBaseline(req, cacheReadTokens);
      return;
    }

    // An EXPECTED drop (after a compaction / cache-deletion) is a legitimate cache
    // shrink, not a break — swallow it and re-baseline so it isn't mis-attributed.
    if (this._expectNextDrop) {
      this._expectNextDrop = false;
      this._expectedDrops++;
      this._lastDelta = { added: [], removed: [], changed: [] };
      this._lastCauses = [];
      this._lastDiffPath = undefined;
      this._advanceBaseline(req, cacheReadTokens);
      return;
    }

    const fullMiss = cacheReadTokens === 0;
    // Partial break: cache reads shrank well below the previous call (relative AND
    // absolute thresholds) without dropping to zero — a shrunk-but-nonzero prefix.
    const partialBreak = !fullMiss && !this.isFirstRequest && this._prevCacheReadTokens > 0 &&
      cacheReadTokens < this._prevCacheReadTokens * PARTIAL_CACHE_BREAK_RATIO &&
      (this._prevCacheReadTokens - cacheReadTokens) > MIN_CACHE_MISS_TOKENS;

    if (fullMiss || partialBreak) {
      const causes = this.diagnoseCauses(req, this._pendingRequestTime);
      this._lastCauses = causes;
      const cause = causes[0].cause;
      this.lastCause = cause;
      this.causes.set(cause, (this.causes.get(cause) ?? 0) + 1);
      if (partialBreak) this._partialBreaks++;
      this._lastDelta = diffToolHashes(this.prev?.perToolHashes, req.perToolHashes);
      // Write a before/after prompt-state unified diff so the exact change that busted
      // the cache is inspectable (path surfaced via getLastDiffPath). Only when BOTH the
      // previous and current requests supplied a diffable-content thunk.
      const prevThunk = this.prev?.buildDiffableContent;
      const curThunk = req.buildDiffableContent;
      this._lastDiffPath = (prevThunk && curThunk)
        ? writeCacheBreakDiff(prevThunk(), curThunk())
        : undefined;
    } else {
      this._lastDelta = { added: [], removed: [], changed: [] };
      this._lastCauses = [];
      this._lastDiffPath = undefined;
    }

    this._advanceBaseline(req, cacheReadTokens);
  }

  /** Advance the prev-request / prev-cache-read / timestamp baseline after a response. */
  private _advanceBaseline(req: CacheDiagRequest, cacheReadTokens: number): void {
    this.prev = req;
    this._prevCacheReadTokens = cacheReadTokens;
    this.lastRequestTime = this._pendingRequestTime;
    this.isFirstRequest = false;
  }

  /**
   * Signal that a compaction happened: the conversation legitimately shrank, so the
   * next request's smaller cached prefix must NOT be scored as a cache break.
   */
  notifyCompaction(): void {
    this._expectNextDrop = true;
  }

  /** Signal an expected cache deletion (cached-microcompact cache_edits) — same suppression. */
  notifyCacheDeletion(): void {
    this._expectNextDrop = true;
  }

  /** Count of cache-read drops suppressed as expected (compaction / cache-deletion). */
  getExpectedDropCount(): number {
    return this._expectedDrops;
  }

  /** Number of PARTIAL cache breaks detected (nonzero-but-shrunk cache reads). */
  getPartialBreakCount(): number {
    return this._partialBreaks;
  }

  /** Which tools were added/removed/changed on the most recent cache break. */
  getLastToolSchemaDelta(): ToolSchemaDelta {
    return { added: [...this._lastDelta.added], removed: [...this._lastDelta.removed], changed: [...this._lastDelta.changed] };
  }

  /** The extended (1-hour) cache TTL threshold used for TTL classification. */
  static readonly EXTENDED_TTL_MS = DEFAULT_CACHE_TTL_1H_MS;

  private diagnoseCause(req: CacheDiagRequest, requestTime: number): CacheMissCause {
    return this.diagnoseCauses(req, requestTime)[0].cause;
  }

  /**
   * Diagnose ALL concurrent causes of a cache break (priority-ordered, each with a
   * human-readable magnitude), mirroring Claude's multi-cause explanation: model
   * switch, idle-past-TTL (5min vs 1h bucket), tool-def changes (which tool's
   * schema), system-prompt char delta, thinking mode, betas added/removed,
   * cache-control / extra-body / effort / fast-mode / global-cache-strategy flips.
   */
  diagnoseCauses(req: CacheDiagRequest, requestTime: number): CacheBreakCause[] {
    if (this.isFirstRequest || !this.prev) return [{ cause: 'first_request', detail: 'first request' }];
    const prev = this.prev;
    const out: CacheBreakCause[] = [];

    if (req.model !== prev.model) out.push({ cause: 'model_switched', detail: `model changed (${prev.model} → ${req.model})` });

    if (this.lastRequestTime > 0) {
      const elapsed = requestTime - this.lastRequestTime;
      if (elapsed > DEFAULT_CACHE_TTL_1H_MS) {
        out.push({ cause: 'idle_past_1h_ttl', detail: `idle ${Math.round(elapsed / 60_000)}min (past 1h TTL)` });
      } else if (elapsed > this.cacheTtlMs) {
        out.push({ cause: 'idle_past_ttl', detail: `idle ${Math.round(elapsed / 1000)}s (past ${Math.round(this.cacheTtlMs / 1000)}s TTL)` });
      }
    }

    if (req.toolDefHash !== prev.toolDefHash) {
      const d = diffToolHashes(prev.perToolHashes, req.perToolHashes);
      let detail: string;
      if (d.added.length || d.removed.length) {
        detail = `tools changed (+${d.added.length}/-${d.removed.length} tools)` +
          (d.changed.length ? `, schema changed: ${d.changed.join(', ')}` : '');
      } else if (d.changed.length) {
        detail = `tool prompt/schema changed (same tool set): ${d.changed.join(', ')}`;
      } else {
        detail = 'tool definitions changed';
      }
      out.push({ cause: 'tool_definitions_changed', detail });
    }

    if (req.systemPromptHash !== prev.systemPromptHash) {
      let delta = '';
      if (req.systemPromptChars !== undefined && prev.systemPromptChars !== undefined) {
        const diff = req.systemPromptChars - prev.systemPromptChars;
        delta = ` (${diff >= 0 ? '+' : ''}${diff} chars)`;
      }
      out.push({ cause: 'system_prompt_changed', detail: `system prompt changed${delta}` });
    }

    if (req.thinkingMode !== prev.thinkingMode) {
      out.push({ cause: 'thinking_mode_changed', detail: `thinking mode changed (${prev.thinkingMode} → ${req.thinkingMode})` });
    }

    const betaDelta = diffStringLists(prev.betas, req.betas);
    if (betaDelta.added.length || betaDelta.removed.length) {
      out.push({ cause: 'betas_changed', detail: `betas changed (+${betaDelta.added.join(',') || 'none'} / -${betaDelta.removed.join(',') || 'none'})` });
    }
    if ((req.cacheControlHash ?? '') !== (prev.cacheControlHash ?? '')) {
      out.push({ cause: 'cache_control_changed', detail: 'cache control scope/TTL changed' });
    }
    if ((req.extraBodyHash ?? '') !== (prev.extraBodyHash ?? '')) {
      out.push({ cause: 'extra_body_changed', detail: 'extra request body changed' });
    }
    if ((req.effortValue ?? '') !== (prev.effortValue ?? '')) {
      out.push({ cause: 'effort_changed', detail: `effort changed (${prev.effortValue ?? 'default'} → ${req.effortValue ?? 'default'})` });
    }
    if ((req.fastMode ?? false) !== (prev.fastMode ?? false)) {
      out.push({ cause: 'fast_mode_changed', detail: `fast mode ${req.fastMode ? 'enabled' : 'disabled'}` });
    }
    if ((req.globalCacheStrategy ?? '') !== (prev.globalCacheStrategy ?? '')) {
      out.push({ cause: 'global_cache_strategy_changed', detail: `global cache strategy changed (${prev.globalCacheStrategy ?? 'none'} → ${req.globalCacheStrategy ?? 'none'})` });
    }

    if (out.length === 0) out.push({ cause: 'unknown', detail: 'cache break with no detected input change (likely server-side)' });
    return out;
  }

  /** All concurrent causes (with magnitudes) of the most recent cache break. */
  getLastCauses(): CacheBreakCause[] {
    return this._lastCauses.map(c => ({ ...c }));
  }

  /** One-line quantified explanation of the most recent cache break ('' if none). */
  getLastExplanation(): string {
    return this._lastCauses.map(c => c.detail).join('; ');
  }

  /**
   * Temp-file path of the unified diff of the prev→current prompt state for the most
   * recent break (undefined when no break, no diffable content was supplied, the states
   * were identical, or the write failed). Surfaced in the /usage break warning and the
   * inspector Cache view as a debugging affordance — the exact prompt change inspectable.
   */
  getLastDiffPath(): string | undefined {
    return this._lastDiffPath;
  }

  getLastMissCause(): CacheMissCause | null {
    return this.lastCause;
  }

  getCacheMissCauses(): Map<CacheMissCause, number> {
    return new Map(this.causes);
  }

  getTotalMisses(): number {
    let total = 0;
    for (const count of this.causes.values()) total += count;
    return total;
  }

  /** For testing: override the last request timestamp to simulate idle. */
  _setLastRequestTime(ts: number): void {
    this.lastRequestTime = ts;
  }

  reset(): void {
    this.causes.clear();
    this.lastCause = null;
    this.prev = null;
    this.lastRequestTime = 0;
    this.isFirstRequest = true;
    this._pendingRequest = null;
    this._pendingRequestTime = 0;
    this._prevCacheReadTokens = 0;
    this._partialBreaks = 0;
    this._lastDelta = { added: [], removed: [], changed: [] };
    this._lastCauses = [];
    this._lastDiffPath = undefined;
    this._expectNextDrop = false;
    this._expectedDrops = 0;
  }
}

// ---------------------------------------------------------------------------
// Cost tracking safety — bounds checking, serialization for persistence
// ---------------------------------------------------------------------------

export const MAX_REASONABLE_COST_USD = 1000;
export const MAX_REASONABLE_TOKENS = 100_000_000;

export function validateCostEntry(
  cost: number,
  inputTokens: number,
  outputTokens: number,
): { valid: boolean; reason?: string } {
  if (cost < 0) return { valid: false, reason: 'Negative cost' };
  if (inputTokens < 0) return { valid: false, reason: 'Negative input token count' };
  if (outputTokens < 0) return { valid: false, reason: 'Negative output token count' };
  const totalTokens = inputTokens + outputTokens;
  if (cost > MAX_REASONABLE_COST_USD) {
    return { valid: true, reason: `Cost $${cost.toFixed(2)} exceeds $${MAX_REASONABLE_COST_USD} threshold` };
  }
  if (totalTokens > MAX_REASONABLE_TOKENS) {
    return { valid: true, reason: `Token count ${totalTokens} exceeds ${MAX_REASONABLE_TOKENS} threshold` };
  }
  return { valid: true };
}

export interface CostState {
  totalCost: number;
  totalInputTokens: number;
  totalOutputTokens: number;
  requestCount: number;
  /**
   * The session this cost state belongs to. Serialized so a resume can enforce a
   * session-id boundary check (parity: Claude's getStoredSessionCosts, which returns
   * undefined when projectConfig.lastSessionId !== the resuming sessionId) — another
   * session's spend can never be restored into the wrong session. Omitted for an unscoped
   * (session-agnostic) state.
   */
  sessionId?: string;
  /**
   * Optional TUI render-performance snapshot captured at save time and persisted alongside
   * the session's costs (parity: Claude's saveCurrentSessionCosts(getFpsMetrics()) →
   * lastFpsAverage/lastFpsLow1Pct). Absent for non-interactive / unsampled sessions.
   */
  fpsMetrics?: FpsMetrics;
}

/** The serialized on-disk shape of a {@link CostState}, including optional scoping fields. */
interface SerializedCostState {
  totalCost: number;
  totalInputTokens: number;
  totalOutputTokens: number;
  requestCount: number;
  version: 1;
  sessionId?: string;
  fpsMetrics?: FpsMetrics;
}

/** True when an {@link FpsMetrics}-shaped value carries two finite, non-negative numbers. */
function isValidFpsMetrics(v: unknown): v is FpsMetrics {
  return (
    typeof v === 'object' && v !== null &&
    Number.isFinite((v as FpsMetrics).averageFps) &&
    Number.isFinite((v as FpsMetrics).low1PctFps) &&
    (v as FpsMetrics).averageFps >= 0 &&
    (v as FpsMetrics).low1PctFps >= 0
  );
}

export function serializeCostState(tracker: CostState): string {
  const out: SerializedCostState = {
    totalCost: tracker.totalCost,
    totalInputTokens: tracker.totalInputTokens,
    totalOutputTokens: tracker.totalOutputTokens,
    requestCount: tracker.requestCount,
    version: 1,
  };
  if (typeof tracker.sessionId === 'string' && tracker.sessionId) {
    out.sessionId = tracker.sessionId;
  }
  if (isValidFpsMetrics(tracker.fpsMetrics)) {
    out.fpsMetrics = { averageFps: tracker.fpsMetrics.averageFps, low1PctFps: tracker.fpsMetrics.low1PctFps };
  }
  return JSON.stringify(out);
}

export function deserializeCostState(json: string): CostState | null {
  try {
    const parsed = JSON.parse(json);
    if (
      typeof parsed !== 'object' || parsed === null ||
      typeof parsed.totalCost !== 'number' ||
      typeof parsed.totalInputTokens !== 'number' ||
      typeof parsed.totalOutputTokens !== 'number' ||
      typeof parsed.requestCount !== 'number'
    ) {
      return null;
    }
    if (parsed.totalCost < 0 || parsed.totalInputTokens < 0 ||
        parsed.totalOutputTokens < 0 || parsed.requestCount < 0) {
      return null;
    }
    const state: CostState = {
      totalCost: parsed.totalCost,
      totalInputTokens: parsed.totalInputTokens,
      totalOutputTokens: parsed.totalOutputTokens,
      requestCount: parsed.requestCount,
    };
    if (typeof parsed.sessionId === 'string' && parsed.sessionId) {
      state.sessionId = parsed.sessionId;
    }
    if (isValidFpsMetrics(parsed.fpsMetrics)) {
      state.fpsMetrics = { averageFps: parsed.fpsMetrics.averageFps, low1PctFps: parsed.fpsMetrics.low1PctFps };
    }
    return state;
  } catch {
    return null;
  }
}

/**
 * Deserialize a persisted cost state ONLY when it belongs to `sessionId` — a session-id
 * boundary check mirroring Claude's getStoredSessionCosts (which returns undefined when
 * projectConfig.lastSessionId !== the resuming sessionId). A state carrying a DIFFERENT
 * embedded sessionId (e.g. a copied or corrupted session file) is rejected (`null`) so
 * another session's spend can never be restored into this one. A state with NO embedded
 * sessionId is treated as unscoped and allowed — the cost state lives in the session's own
 * file, so file identity already scopes it. Malformed JSON → `null` (via deserializeCostState).
 */
export function deserializeCostStateForSession(json: string, sessionId: string): CostState | null {
  const state = deserializeCostState(json);
  if (!state) return null;
  if (state.sessionId !== undefined && state.sessionId !== sessionId) return null;
  return state;
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
