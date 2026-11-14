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

/**
 * Cache-WRITE multiplier on the input rate. Anthropic bills cache_creation
 * tokens at 1.25× the base input price for the default 5-minute TTL (2× for the
 * 1-hour TTL). We use the 5m default; without it, cache writes are billed as
 * free and the session undercounts real spend.
 */
const CACHE_WRITE_PRICE_RATIO = 1.25;

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

      if (isNaN(input) || isNaN(output) || (cached !== null && isNaN(cached))) continue;

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
    });
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
    };
    m.promptTokens += usage.promptTokens;
    m.completionTokens += usage.completionTokens;
    m.cachedPromptTokens += cachedTokens;
    m.cacheCreationTokens += cacheCreationTokens;
    m.totalTokens += usage.promptTokens + usage.completionTokens;
    m.cost += cost;
    m.requestCount++;
    this._byModel.set(activeModel, m);

    // Flag models we couldn't price precisely so the live display can warn.
    if (activeModel && !isPricedModel(activeModel)) this._unknownModels.add(activeModel);

    if (cachedTokens > 0) {
      const [inputPPM, , cachedPPM] = matchPricing(activeModel);
      const fullPriceCost = (cachedTokens / 1_000_000) * inputPPM;
      const actualCachedPPM = cachedPPM ?? inputPPM * CACHED_PRICE_RATIO;
      const cachedCost = (cachedTokens / 1_000_000) * actualCachedPPM;
      this._cachedCostSavings += fullPriceCost - cachedCost;
    }
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

  getStats(): ExtendedUsageStats {
    const totalPromptTokens = this._cachedPromptTokens + this._uncachedPromptTokens;
    const cacheHitRate = totalPromptTokens > 0
      ? this._cachedPromptTokens / totalPromptTokens
      : 0;

    const byModel: Record<string, ModelCostUsage> = {};
    for (const [model, u] of this._byModel) byModel[model] = { ...u };

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
  },
): number {
  const [inputPPM, outputPPM, cachedPPM] = matchPricing(model);
  const actualCachedPPM = cachedPPM ?? inputPPM * CACHED_PRICE_RATIO;
  return (
    (Math.max(0, u.uncachedPromptTokens) / 1_000_000) * inputPPM +
    (Math.max(0, u.cachedPromptTokens) / 1_000_000) * actualCachedPPM +
    (Math.max(0, u.cacheCreationTokens) / 1_000_000) * inputPPM * CACHE_WRITE_PRICE_RATIO +
    (Math.max(0, u.completionTokens) / 1_000_000) * outputPPM +
    Math.max(0, u.webSearchRequests ?? 0) * WEB_SEARCH_COST_PER_REQUEST
  );
}

// ---------------------------------------------------------------------------
// Cache miss diagnostics
// ---------------------------------------------------------------------------

export const CACHE_MISS_CAUSES = ['tool_definitions_changed', 'system_prompt_changed', 'idle_past_ttl', 'thinking_mode_changed', 'model_switched', 'first_request', 'unknown'] as const;
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

    const fullMiss = cacheReadTokens === 0;
    // Partial break: cache reads shrank well below the previous call (relative AND
    // absolute thresholds) without dropping to zero — a shrunk-but-nonzero prefix.
    const partialBreak = !fullMiss && !this.isFirstRequest && this._prevCacheReadTokens > 0 &&
      cacheReadTokens < this._prevCacheReadTokens * PARTIAL_CACHE_BREAK_RATIO &&
      (this._prevCacheReadTokens - cacheReadTokens) > MIN_CACHE_MISS_TOKENS;

    if (fullMiss || partialBreak) {
      const cause = this.diagnoseCause(req, this._pendingRequestTime);
      this.lastCause = cause;
      this.causes.set(cause, (this.causes.get(cause) ?? 0) + 1);
      if (partialBreak) this._partialBreaks++;
      this._lastDelta = diffToolHashes(this.prev?.perToolHashes, req.perToolHashes);
    } else {
      this._lastDelta = { added: [], removed: [], changed: [] };
    }

    this.prev = req;
    this._prevCacheReadTokens = cacheReadTokens;
    this.lastRequestTime = this._pendingRequestTime;
    this.isFirstRequest = false;
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
    if (this.isFirstRequest || !this.prev) return 'first_request';

    // Priority order: model > idle > tool defs > system prompt > thinking > unknown
    if (req.model !== this.prev.model) return 'model_switched';

    if (this.lastRequestTime > 0) {
      const elapsed = requestTime - this.lastRequestTime;
      if (elapsed > this.cacheTtlMs) return 'idle_past_ttl';
    }

    if (req.toolDefHash !== this.prev.toolDefHash) return 'tool_definitions_changed';
    if (req.systemPromptHash !== this.prev.systemPromptHash) return 'system_prompt_changed';
    if (req.thinkingMode !== this.prev.thinkingMode) return 'thinking_mode_changed';

    return 'unknown';
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

interface CostState {
  totalCost: number;
  totalInputTokens: number;
  totalOutputTokens: number;
  requestCount: number;
}

export function serializeCostState(tracker: CostState): string {
  return JSON.stringify({
    totalCost: tracker.totalCost,
    totalInputTokens: tracker.totalInputTokens,
    totalOutputTokens: tracker.totalOutputTokens,
    requestCount: tracker.requestCount,
    version: 1,
  });
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
    return {
      totalCost: parsed.totalCost,
      totalInputTokens: parsed.totalInputTokens,
      totalOutputTokens: parsed.totalOutputTokens,
      requestCount: parsed.requestCount,
    };
  } catch {
    return null;
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
