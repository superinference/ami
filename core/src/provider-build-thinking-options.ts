// Split from provider.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { ToolSet, ToolCallRepairFunction } from 'ai';
import { getModelCapabilities, resolveThinkingBudget, resolveMaxEffortLevel, modelAlwaysOnThinking } from './model-capabilities';
import { estimateTokens } from './utils/tokens';
import { sleep } from './utils/sleep';
import { isEnvTruthy } from './analytics';
import type { ProviderConfig, Message, ToolDefinition, ThinkingConfig } from './types';
import { EXTENDED_CACHE_TTL_BETA, experimentalBetasDisabled, getExtraAnthropicBetas, injectAnthropicCacheControl, interleavedThinkingDisabled, structuredOutputsEnabled, tokenEfficientToolsEnabled } from './provider-inject-anthropic-cache-control';
import { ADAPTIVE_EFFORT_BY_LEVEL, OPENAI_EFFORT_BY_LEVEL, alwaysEnableEffort } from './provider-convert-messages';

/**
 * Wrap a fetch so every outgoing Anthropic-on-Vertex request carries cache_control
 * (injected into the body) and, for 1h TTL, the extended-cache beta header. This
 * is the ONLY path that makes prompt caching work on anthropic-vertex given the
 * SDK's dropped providerOptions.
 */
export function withVertexAnthropicCache(
  baseFetch: typeof globalThis.fetch,
  cacheTtl?: '5m' | '1h',
): typeof globalThis.fetch {
  const cc = buildAnthropicCacheControl(cacheTtl) as unknown as Record<string, unknown>;
  return (async (input: any, init?: any) => { // eslint-disable-line @typescript-eslint/no-explicit-any
    if (init && typeof init.body === 'string') {
      const body = injectAnthropicCacheControl(init.body, cc);
      const headers = new Headers(init.headers as Record<string, string> | undefined);
      if (cacheTtl === '1h' && !headers.has('anthropic-beta')) {
        headers.set('anthropic-beta', EXTENDED_CACHE_TTL_BETA);
      }
      init = { ...init, body, headers };
    }
    return baseFetch(input, init);
  }) as typeof globalThis.fetch;
}

/**
 * Fingerprint of every env/settings input {@link getModelBaseBetas} reads, folded
 * into the cache key so the memo never hides a stale env read behind an old entry.
 * AMI reads these knobs LIVE from process.env (unlike the reference, which memoizes
 * on the model alone and relies solely on explicit cache clears), so a mid-session
 * settings change yields a new key — freshness is preserved automatically, and
 * {@link clearBetasCaches} drops the whole map so it can never grow unboundedly
 * across env generations.
 */
export function betaSettingsFingerprint(): string {
  return [
    experimentalBetasDisabled() ? '1' : '0',
    interleavedThinkingDisabled() ? '1' : '0',
    tokenEfficientToolsEnabled() ? '1' : '0',
    structuredOutputsEnabled() ? '1' : '0',
    resolvePromptCacheScope() ?? '',
    getExtraAnthropicBetas().join(','),
  ].join('|');
}

/**
 * Env knobs that FORCE reasoning off for every request, regardless of the
 * per-session `--thinking` / `/thinking` config. AMI-native name first, the reference
 * Code's name as a drop-in alias. Mirrors the reference's `CLAUDE_CODE_DISABLE_THINKING`
 * gate (`hasThinking = … && !isEnvTruthy(CLAUDE_CODE_DISABLE_THINKING)`,
 * services/api/claude.ts). Routed through the standard not-enabled path so an
 * always-on-thinking model still receives the minimal config it requires (sending
 * none would 400) while every other model omits thinking entirely.
 */
export const DISABLE_THINKING_ENV_VARS = ['AMI_DISABLE_THINKING', 'CLAUDE_CODE_DISABLE_THINKING'] as const;

/** True when any {@link DISABLE_THINKING_ENV_VARS} knob is set truthy. */
export function thinkingDisabledByEnv(env: NodeJS.ProcessEnv = process.env): boolean {
  return DISABLE_THINKING_ENV_VARS.some(name => isEnvTruthy(env[name]));
}

/**
 * Env knobs that FORCE the legacy fixed-budget thinking shape
 * (`{ type:'enabled', budget_tokens }`) even on models AMI would otherwise drive
 * with adaptive thinking. AMI-native name first, the reference's name as a drop-in
 * alias. Mirrors the reference's `CLAUDE_CODE_DISABLE_ADAPTIVE_THINKING` gate
 * (`!isEnvTruthy(CLAUDE_CODE_DISABLE_ADAPTIVE_THINKING) && modelSupportsAdaptiveThinking(...)`,
 * services/api/claude.ts) — for gateways/proxies that reject the adaptive shape.
 */
export const DISABLE_ADAPTIVE_THINKING_ENV_VARS = ['AMI_DISABLE_ADAPTIVE_THINKING', 'CLAUDE_CODE_DISABLE_ADAPTIVE_THINKING'] as const;

/** True when any {@link DISABLE_ADAPTIVE_THINKING_ENV_VARS} knob is set truthy. */
export function adaptiveThinkingDisabledByEnv(env: NodeJS.ProcessEnv = process.env): boolean {
  return DISABLE_ADAPTIVE_THINKING_ENV_VARS.some(name => isEnvTruthy(env[name]));
}

export function buildThinkingOptions(
  modelId: string,
  thinkingConfig: ThinkingConfig | undefined,
): Record<string, unknown> {
  // CLAUDE_CODE_DISABLE_THINKING / AMI_DISABLE_THINKING: force reasoning off for
  // this request regardless of the per-session config, by dropping the thinking
  // config so the standard not-enabled path below runs (which still emits the
  // minimal config an always-on model requires).
  const thinking = thinkingDisabledByEnv() ? undefined : thinkingConfig;
  if (!thinking?.enabled) {
    // An always-on-thinking model (the 5.x the reference line) REJECTS a disabled-thinking
    // request and reasons regardless — so even when the user turned thinking off we
    // must still send a thinking config, at the minimal 'low' effort floor, rather
    // than omit it. Mirrors the reference's AntModel.alwaysOnThinking handling. Every other
    // model honors "off" by sending no thinking options.
    if (modelAlwaysOnThinking(modelId)) {
      return { providerOptions: { anthropic: { thinking: { type: 'adaptive' as const }, effort: 'low' } } };
    }
    return {};
  }
  const caps = getModelCapabilities(modelId);

  // Anthropic the reference (adaptive family: Opus 4.x / Sonnet 4.x) — adaptive thinking
  // + output-config effort. The legacy { type: 'enabled', budget_tokens } shape is
  // DEPRECATED on Opus/Sonnet 4.6 and REJECTED with a 400 on Opus 4.7/4.8 (and removed
  // on the 5.x family), which made thinking requests fail outright — ui#17. Adaptive
  // thinking lets the model decide depth; `effort` (mapped from the thinking level) is
  // the supported depth control that replaces the fixed token budget.
  if (caps?.supportsAdaptiveThinking && !adaptiveThinkingDisabledByEnv()) {
    // The top 'max' effort tier is Opus-4.6+-class only; the API rejects
    // effort:'max' on every other adaptive model (e.g. sonnet-4-6). Downgrade
    // max→high per model before mapping so the request never 400s — mirrors
    // the reference's resolveAppliedEffort (utils/effort.ts). Other levels pass through.
    const appliedLevel = resolveMaxEffortLevel(thinking.level, modelId);
    const effort = ADAPTIVE_EFFORT_BY_LEVEL[appliedLevel] ?? 'high';
    return {
      providerOptions: {
        anthropic: {
          thinking: { type: 'adaptive' as const },
          effort,
        },
      },
    };
  }

  // Anthropic the reference PRE-4.6 (opus 4/4.1/4.5, sonnet 4/4.5): adaptive thinking is REJECTED
  // with an HTTP 400 ("adaptive thinking is not supported on this model"), so these reasoning
  // models must use the legacy { type:'enabled', budget_tokens } shape instead — ui#24. The
  // budget is clamped below a conservative output ceiling so budget_tokens stays < max_tokens
  // (Anthropic requires budget_tokens < max_tokens, floor 1024).
  if (caps?.reasoning && /claude-(?:opus|sonnet)/i.test(modelId)) {
    const resolved = thinking.budgetTokens ?? resolveThinkingBudget(thinking.level, modelId);
    const budgetTokens = Math.max(1024, Math.min(resolved, 24000));
    return {
      providerOptions: {
        anthropic: {
          thinking: { type: 'enabled' as const, budgetTokens },
        },
      },
    };
  }

  // OpenAI o-series — reasoning_effort via providerOptions
  if (caps?.temperatureMustBeUnset) {
    return { providerOptions: { openai: { reasoningEffort: OPENAI_EFFORT_BY_LEVEL[thinking.level] ?? 'medium' } } };
  }

  // Google Gemini 2.5 — thinkingConfig via providerOptions
  if (modelId.startsWith('gemini-2.5') || modelId.startsWith('gemini-3')) {
    const budgetTokens = thinking.budgetTokens ?? resolveThinkingBudget(thinking.level, modelId);
    return { providerOptions: { google: { thinkingConfig: { thinkingBudget: budgetTokens, includeThoughts: true } } } };
  }

  // Force-enable-effort override (parity: CLAUDE_CODE_ALWAYS_ENABLE_EFFORT). A model
  // AMI did not statically recognize as a reasoning model (a new 3P OpenAI-compatible
  // reasoning model) would otherwise fall through to NO effort even with --thinking
  // set. When the env knob is on, emit the OpenAI-compatible `reasoning_effort` field
  // (the near-universal 3P spelling) so the effort control still reaches the endpoint.
  if (alwaysEnableEffort()) {
    return { providerOptions: { openai: { reasoningEffort: OPENAI_EFFORT_BY_LEVEL[thinking.level] ?? 'medium' } } };
  }

  // DeepSeek / other: no special options needed, thinking comes via inline tags
  return {};
}

// ---------------------------------------------------------------------------
// Prompt cache optimization for Anthropic
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// streamChatCompletion — main streaming function
// ---------------------------------------------------------------------------

export function promptTokensForFit(
  systemPrompt: string | undefined,
  messages: Message[],
  tools: ToolDefinition[],
): number {
  let text = systemPrompt ?? '';
  for (const msg of messages) {
    text += typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content ?? '');
    if (msg.role === 'assistant' && msg.tool_calls) text += JSON.stringify(msg.tool_calls);
  }
  if (tools.length > 0) text += JSON.stringify(tools);
  return estimateTokens(text);
}

/**
 * Cross-session prompt-cache scope. `'org'` lets any API key in the same
 * organization reuse a cache block this run wrote; `'global'` is broader still.
 * Mirrors the reference's `CacheScope` (utils/api.ts). Unset = the default,
 * key-private ephemeral cache.
 */
export const PROMPT_CACHE_SCOPES = ['global', 'org'] as const;

export type PromptCacheScope = (typeof PROMPT_CACHE_SCOPES)[number];

/** Env var that opts a run into a wider cross-session prompt-cache scope. */
export const PROMPT_CACHE_SCOPE_ENV = 'AMI_PROMPT_CACHE_SCOPE';

/**
 * Resolve the effective prompt-cache scope from {@link PROMPT_CACHE_SCOPE_ENV}.
 * Only the exact values `'global'` / `'org'` (case-insensitive, trimmed) are
 * honored; anything else — including unset, whitespace, or junk — resolves to
 * `undefined`, i.e. the default key-private cache. Purely ADDITIVE and
 * behavior-preserving when the env is absent. Read at call time so it is
 * toggleable per process. Exported for unit testing.
 */
export function resolvePromptCacheScope(env: NodeJS.ProcessEnv = process.env): PromptCacheScope | undefined {
  const raw = env[PROMPT_CACHE_SCOPE_ENV]?.trim().toLowerCase();
  return raw === 'global' || raw === 'org' ? raw : undefined;
}

/**
 * Build the Anthropic ephemeral cache-control object, honoring an optional TTL
 * and an optional cross-session {@link PromptCacheScope}. '1h' requires
 * Anthropic's extended-cache beta; default (undefined/'5m') uses the standard
 * 5-minute ephemeral cache. A `scope` ('global'/'org'), when supplied, is
 * attached so the block can be reused across sessions/keys; it requires
 * {@link PROMPT_CACHING_SCOPE_BETA}, which {@link computeAnthropicBetas} adds
 * whenever a scope is active. `scope` defaults to {@link resolvePromptCacheScope}
 * so every existing call site picks the knob up with no threading; pass an
 * explicit value (or `undefined`) to override.
 */
export function buildAnthropicCacheControl(
  cacheTtl?: '5m' | '1h',
  scope: PromptCacheScope | undefined = resolvePromptCacheScope(),
): { type: 'ephemeral'; ttl?: '1h'; scope?: PromptCacheScope } {
  const base: { type: 'ephemeral'; ttl?: '1h'; scope?: PromptCacheScope } =
    cacheTtl === '1h' ? { type: 'ephemeral', ttl: '1h' } : { type: 'ephemeral' };
  if (scope) base.scope = scope;
  return base;
}

/** Env var that sets a configurable DEFAULT prompt-cache TTL. */
export const PROMPT_CACHE_TTL_ENV = 'AMI_PROMPT_CACHE_TTL';

/**
 * L4: resolve the effective prompt-cache TTL. An explicit per-request
 * `config.cacheTtl` always wins; when it is unset, `AMI_PROMPT_CACHE_TTL`
 * supplies a configurable DEFAULT (accepts only '5m' or '1h' — any other value,
 * including whitespace or junk, is ignored). With neither set this returns
 * `undefined` (Anthropic's standard 5-minute ephemeral cache), so the default
 * behavior is unchanged. Purely ADDITIVE + behavior-preserving when the env is
 * absent. Exported for unit testing.
 */
export function resolveCacheTtl(
  configTtl: '5m' | '1h' | undefined,
  env: NodeJS.ProcessEnv = process.env,
): '5m' | '1h' | undefined {
  if (configTtl !== undefined) return configTtl;
  const raw = env[PROMPT_CACHE_TTL_ENV]?.trim();
  return raw === '5m' || raw === '1h' ? raw : undefined;
}

/** Env var that selects the global Anthropic prompt-cache marker strategy. */
export const PROMPT_CACHE_STRATEGY_ENV = 'AMI_PROMPT_CACHE_STRATEGY';

/**
 * Selectable global prompt-cache strategy — mirrors the reference's `tool_based` /
 * `system_prompt` / `none` cache-strategy knob (services/api/logging.ts). It
 * decides WHICH prefix blocks get an Anthropic `cache_control` marker:
 *
 * - `'tool_based'` (DEFAULT) — mark BOTH the tools block and the conversation
 *   breakpoints (the current/established AMI behavior; largest cached prefix).
 * - `'system_prompt'` — mark only the message/system breakpoints, NOT the tools
 *   block (for setups whose tool set churns every turn and would bust a tool cache).
 * - `'none'` — write no cache markers at all (equivalent to a per-request
 *   {@link ProviderConfig.skipCacheWrite}, applied globally via env).
 *
 * Any unrecognized / unset value resolves to `'tool_based'`, so behavior is
 * unchanged unless the operator opts in. Read at call time (toggleable per process).
 */
export const PROMPT_CACHE_STRATEGYS = ['tool_based', 'system_prompt', 'none'] as const;

export type PromptCacheStrategy = (typeof PROMPT_CACHE_STRATEGYS)[number];

export function resolvePromptCacheStrategy(env: NodeJS.ProcessEnv = process.env): PromptCacheStrategy {
  const raw = env[PROMPT_CACHE_STRATEGY_ENV]?.trim().toLowerCase();
  return raw === 'system_prompt' || raw === 'none' ? raw : 'tool_based';
}

/**
 * Cache-preserving tool denial for a forked / derived request.
 *
 * Parity: the reference's AgentSummary fork (`services/AgentSummary/agentSummary.ts`)
 * shares the parent agent's prompt cache by sending the IDENTICAL cache-key params —
 * system prompt, messages prefix, model, thinking config AND the full `tools` array —
 * then forbids every call via a `canUseTool` callback that returns `behavior:'deny'`.
 * It deliberately does NOT send `tools:[]`, because dropping the tools block changes
 * the request's cached prefix and busts the shared prompt cache (a cache miss on the
 * forked side-question/summary).
 *
 * AMI's provider stream has no per-call `canUseTool` callback layer, so the exact
 * provider-level equivalent is `tool_choice: 'none'`: the identical tools array is
 * still advertised to the model (so the cached tools prefix is byte-matched) but the
 * model is forbidden from invoking any of them — the fork can only narrate, never act.
 * Unlike `tools:[]`, `tool_choice` is a top-level request parameter that is NOT part
 * of the cached content blocks, so forcing it to `'none'` preserves the cache key.
 *
 * Returns a shallow copy with `toolChoice` forced to `'none'`; the caller must send
 * the parent request's tools alongside it (an empty tools array makes the choice a
 * no-op, since {@link resolveModel} omits `toolChoice` when no tools are advertised).
 */
export function withCacheSafeToolDeny(config: ProviderConfig): ProviderConfig {
  return { ...config, toolChoice: 'none' };
}

// ---------------------------------------------------------------------------
// repairToolCall — fix hallucinated tool names at the stream level
// ---------------------------------------------------------------------------

/**
 * Map of commonly-hallucinated tool names to the correct tool name.
 * Used by buildRepairToolCall to correct tool names before they reach
 * the engine's tool executor.
 */
export const TOOL_NAME_CORRECTIONS: Record<string, string> = {
  read_file: 'file_read',
  edit_file: 'file_edit',
  write_file: 'file_edit',
  list_directory: 'list_dir',
  agentic_fetch: 'web_fetch',
  run_command: 'bash',
  execute: 'bash',
  search_files: 'grep',
  find_files: 'glob',
  search: 'grep',
  cat: 'file_read',
  ls: 'list_dir',
  find: 'glob',
  curl: 'web_fetch',
};

/**
 * Build a repairToolCall callback for the AI SDK's streamText().
 * When the model hallucinates a tool name (e.g. "read_file" instead of
 * "file_read"), this function maps it to the correct registered name.
 * Returns null if no correction is possible.
 */
export function buildRepairToolCall(tools: ToolDefinition[]): ToolCallRepairFunction<ToolSet> {
  const registeredNames = new Set(tools.map(t => t.name));
  return async ({ toolCall }) => {
    const hallucinated = toolCall.toolName;
    const corrected = TOOL_NAME_CORRECTIONS[hallucinated] ?? TOOL_NAME_CORRECTIONS[hallucinated.toLowerCase()];
    if (corrected && registeredNames.has(corrected)) {
      return { ...toolCall, toolName: corrected };
    }
    return null;
  };
}

/**
 * Sentinel error text for an empty / degenerate response stream — the stream
 * ended without producing ANY content, tool call, or finish-step. Mirrors the reference
 * Code's "Stream ended without receiving any events" (claude.ts), which treats
 * two proxy failure modes as a retryable failure rather than a silent empty turn:
 * (1) no events at all — a proxy returned HTTP 200 with a non-SSE body; and
 * (2) a message_start was received but zero content blocks completed AND no
 * stop_reason arrived — a truncated SSE body. A legitimate structured-output
 * "end_turn with no content blocks" response carries a stop_reason, so it ends on
 * a finish-step above and never reaches this sentinel (no false positive).
 */
export const EMPTY_STREAM_ERROR =
  'Empty response stream: completed with no content, tool call, or finish reason';

/** True when an error chunk carries the empty/degenerate-stream sentinel. */
export function isEmptyStreamError(errorText: string | undefined): boolean {
  return errorText === EMPTY_STREAM_ERROR;
}

export const MAX_RETRIES = 5;

export const BASE_DELAY_MS = 1000;

/**
 * Default heartbeat slice for a long retry backoff. Mirrors the reference's
 * `HEARTBEAT_INTERVAL_MS` (services/api/withRetry.ts): a multi-minute wait is
 * sliced so the host/terminal keeps seeing activity and the UI shows a live
 * countdown instead of a single static "retrying in Xs".
 */
export const RETRY_HEARTBEAT_INTERVAL_MS = 30_000;

/**
 * Sleep `totalMs`, chunking a LONG wait into `intervalMs` slices and re-firing
 * `onHeartbeat(remainingMs)` after each completed slice (except the final one, which
 * resolves the wait). For a short wait (`totalMs <= intervalMs`) it performs a SINGLE
 * sleep and NEVER fires a heartbeat — byte-identical to a plain `sleep`, so normal
 * sub-30s backoffs are unchanged. During a multi-minute retry backoff this keeps
 * emitting periodic activity (so an unattended host does not mark the session idle)
 * and drives a live countdown. Rejects on abort exactly like {@link sleep}, so the
 * surrounding retry loop's abort handling is preserved.
 */
export async function sleepWithRetryHeartbeat(
  totalMs: number,
  signal: AbortSignal | undefined,
  onHeartbeat?: (remainingMs: number) => void,
  intervalMs: number = RETRY_HEARTBEAT_INTERVAL_MS,
): Promise<void> {
  const step = intervalMs > 0 ? intervalMs : RETRY_HEARTBEAT_INTERVAL_MS;
  let remaining = totalMs;
  while (remaining > 0) {
    const slice = Math.min(step, remaining);
    // A retry backoff must ABORT (not silently continue) when the turn is
    // cancelled, so this caller opts into reject-on-abort; the rejection bubbles
    // up and cancels the whole retry loop (see {@link SleepOptions}).
    await sleep(slice, signal, { throwOnAbort: true });
    remaining -= slice;
    if (remaining > 0 && onHeartbeat) onHeartbeat(remaining);
  }
}
