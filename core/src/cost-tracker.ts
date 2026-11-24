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

import { isEssentialTrafficOnly } from './analytics';
import { getContextWindow, getModelMaxOutputTokens } from './model-capabilities';
import { BATCH_TIER_DISCOUNT, CACHED_PRICE_RATIO, CACHE_WRITE_1H_PRICE_RATIO, CACHE_WRITE_PRICE_RATIO, DEFAULT_PRICING, FALLBACK_PRICING, FETCH_TIMEOUT_MS, GATEWAY_URL, MIN_CACHE_MISS_TOKENS, PARTIAL_CACHE_BREAK_RATIO, WEB_SEARCH_COST_PER_REQUEST, categorizeTool, diffStringLists, diffToolHashes, formatModelPricing, isExcludedModel, perTokenToPerMillion } from './cost-tracker-cache-diag-request';
import type { CacheBreakCause, CacheDiagRequest, CacheMissCause, ExtendedUsageStats, GatewayModel, ModelCostUsage, ModelSpeed, PricingEntry, ToolSchemaDelta, UsageServiceTier } from './cost-tracker-cache-diag-request';
import { createUnifiedPatch } from './cost-tracker-create-unified-patch';
import type { CostState } from './cost-tracker-create-unified-patch';
export { CACHE_MISS_CAUSES, MIN_CACHE_MISS_TOKENS, MODEL_SPEEDS, PARTIAL_CACHE_BREAK_RATIO, USAGE_SERVICE_TIERS, WEB_SEARCH_COST_PER_REQUEST, buildDiffableContent, categorizeTool, diffStringLists, diffToolHashes, fingerprint, formatModelPricing, formatPrice, isExcludedModel } from './cost-tracker-cache-diag-request';
export type { CacheBreakCause, CacheDiagRequest, CacheMissCause, DiffableTool, ExtendedUsageStats, ModelCostUsage, ModelSpeed, ToolSchemaDelta, UsageServiceTier } from './cost-tracker-cache-diag-request';
export { MAX_REASONABLE_COST_USD, MAX_REASONABLE_TOKENS, createUnifiedPatch, deserializeCostState, deserializeCostStateForSession, serializeCostState, validateCostEntry } from './cost-tracker-create-unified-patch';
export type { CostState } from './cost-tracker-create-unified-patch';

let livePricing: Record<string, PricingEntry> | null = null;
let fetchPromise: Promise<void> | null = null;

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
  // and is itself priced — mirroring the reference's getModelCosts(), which prices an
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
   * left off instead of restarting at $0 (PARITY — the reference's
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
      // Usage view can show each model's limits next to its spend (mirrors the reference's per-model
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
   * itself is unknown, instead of the generic DEFAULT_PRICING constant — mirrors the reference's
   * unknown-model fallback to the default model's cost. Omit to keep the generic fallback.
   */
  fallbackModel?: string,
  /**
   * Per-request execution speed. When `'fast'` AND the model advertises an elevated-speed
   * (priority) tier, the request is billed at that premium tier — mirrors the reference's
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

const DEFAULT_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes
const DEFAULT_CACHE_TTL_1H_MS = 60 * 60 * 1000; // 1 hour (extended TTL)

let _diffArtifactCounter = 0;

/**
 * Write a unified diff of the prev→current prompt-state to a temp file and return its
 * path (undefined on any failure). Mirrors the reference's writeCacheBreakDiff — the path is
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
   * human-readable magnitude), mirroring the reference's multi-cause explanation: model
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
