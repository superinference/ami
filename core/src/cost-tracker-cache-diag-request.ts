// Split from cost-tracker.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { UsageStats } from './types';

/**
 * Per-request execution speed tier. A `fast` (premium) request can be billed at an
 * elevated pricing tier — mirrors the reference's `usage.speed` → fast-mode cost tier. AMI
 * maps Anthropic's `service_tier: 'priority'` to `'fast'`. Absent/`medium`/`slow`
 * price at the model's standard rate.
 */
export const MODEL_SPEEDS = ['fast', 'medium', 'slow'] as const;

export type ModelSpeed = (typeof MODEL_SPEEDS)[number];

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
   * Usage view annotate each model's window alongside its spend. Mirrors the reference's per-model
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
// mirroring the reference's speed-dependent cost tier (getOpus46CostTier(fastMode)). When
// absent, a fast request prices at the standard rate (no premium). Cached/cache-write
// derive from the fast input rate at the same ratios when not themselves explicit.
// ---------------------------------------------------------------------------

export type PricingEntry = [
  number, number, number | null, (number | null)?, (number | null)?, (number | null)?,
];

// Fallback table used when the gateway fetch fails or hasn't completed yet.
export const FALLBACK_PRICING: Record<string, PricingEntry> = {
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

export const DEFAULT_PRICING: PricingEntry = [1, 3, null];

/** Default ratio when no explicit cached-input price is available. */
export const CACHED_PRICE_RATIO = 0.1;

/**
 * Cache-WRITE multiplier on the input rate. Anthropic bills cache_creation
 * tokens at 1.25× the base input price for the default 5-minute TTL (2× for the
 * 1-hour TTL). We use the 5m default; without it, cache writes are billed as
 * free and the session undercounts real spend.
 */
export const CACHE_WRITE_PRICE_RATIO = 1.25;

/**
 * Cache-WRITE multiplier for the 1-hour (extended) TTL. Anthropic bills a
 * 1-hour cache write at 2× the base input price (vs 1.25× for the 5-minute TTL).
 * When a usage record splits its cache-creation tokens into an ephemeral-1h vs
 * ephemeral-5m breakdown (Anthropic Usage.cache_creation), re-pricing the 1h
 * portion at 1.25× would undercount a long-TTL-cache session's real spend.
 */
export const CACHE_WRITE_1H_PRICE_RATIO = 2;

/**
 * Billing service tiers (Anthropic Usage.service_tier). `batch` is billed at
 * HALF the standard per-token rate; `priority`/`standard` at the standard rate
 * (priority's surcharge is not publicly a flat multiplier, so it re-prices at
 * standard rather than guessing). Re-pricing a batch-billed session at the
 * standard rate would OVERSTATE its cost by 2×, so the cross-agent spend view
 * must honor the recorded tier.
 */
export const USAGE_SERVICE_TIERS = ['standard', 'batch', 'priority'] as const;

export type UsageServiceTier = (typeof USAGE_SERVICE_TIERS)[number];

/** Per-token discount applied to a `batch` service-tier usage record (−50%). */
export const BATCH_TIER_DISCOUNT = 0.5;

/**
 * Flat per-request charge for a server-side web_search tool use. Anthropic bills
 * web search at $10 / 1000 requests = $0.01 each, independent of token usage; a
 * session that web-searches undercounts spend without this component.
 */
export const WEB_SEARCH_COST_PER_REQUEST = 0.01;

// ---------------------------------------------------------------------------
// Live pricing from Vercel AI Gateway
// ---------------------------------------------------------------------------

export const GATEWAY_URL = 'https://ai-gateway.vercel.sh/v1/models';

export const FETCH_TIMEOUT_MS = 5_000;

export function perTokenToPerMillion(perToken: string): number {
  return parseFloat(perToken) * 1_000_000;
}

export interface GatewayModel {
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
 * Mirrors the reference's isExcludedModel (promptCacheBreakDetection.ts).
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
// On a prompt-cache break the reference serializes the full before/after prompt
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
 * like a change). Mirrors the reference's buildDiffableContent (promptCacheBreakDetection.ts).
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

export type DiffOp = { t: '=' | '-' | '+'; line: string };

/** Split into lines, dropping a single trailing empty element from a trailing newline. */
export function splitDiffLines(s: string): string[] {
  const lines = s.split('\n');
  if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
  return lines;
}

/** Guard: beyond this LCS cell count fall back to a coarse remove-all/add-all diff. */
export const MAX_LCS_CELLS = 4_000_000;
