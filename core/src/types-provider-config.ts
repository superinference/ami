// Split from types.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { ThinkingLevel } from './model-capabilities';

export interface ProviderConfig {
  baseUrl: string;
  apiKey: string;
  model: string;
  maxTokens?: number;
  temperature?: number;
  provider?: string;
  /** Anthropic prompt-cache TTL. '1h' requires the extended-cache beta; default is the 5-minute ephemeral cache. */
  cacheTtl?: '5m' | '1h';
  /**
   * Actual context window size reported by the serving backend (e.g. vLLM
   * max_model_len). When set, overrides the theoretical value from
   * getContextWindow() for headroom calculations and system prompt sizing.
   */
  contextWindow?: number;
  /**
   * Provider-specific request-body fields merged verbatim into every
   * OpenAI-compatible request (e.g. `chat_template_kwargs` for vLLM-served
   * Qwen/Granite reasoning control). Deep-merged into the AI SDK request body
   * at the transport layer, so it survives SDK schema changes and works for any
   * current or future OpenAI-compatible parameter. Ignored by native
   * Anthropic/Google providers.
   */
  extraBody?: Record<string, unknown>;
  /**
   * Stable id for OpenAI prompt-cache routing. The same conversation must send
   * the same key on every turn. Official OpenAI hosts only; other
   * OpenAI-compatible servers never see `prompt_cache_key`.
   */
  promptCacheKey?: string;
  /**
   * OpenAI SDK-native prompt-cache options. `mode` controls whether caching is
   * implicit (automatic prefix matching, default) or explicit. `ttl` sets the
   * server-side cache lifetime ('30m' or '12h'). Only effective on official
   * OpenAI hosts; ignored by other providers.
   */
  promptCacheOptions?: { mode?: 'implicit' | 'explicit'; ttl?: '30m' | '12h' };
  /** Extra headers merged into every LLM API request. Used for gateway hint
   *  headers (request class, agent type, compaction state). */
  extraHeaders?: Record<string, string>;
  fallbackModel?: string;
  /**
   * Internal recovery flag set by `streamChatCompletionWithRetry` after a STALE
   * keep-alive socket error (ECONNRESET/EPIPE): forces the next attempt to open a
   * FRESH connection with HTTP keep-alive disabled, so the retry does not grab the
   * same poisoned pooled socket. Not user-facing; off by default (pooling stays on).
   */
  forceFreshConnection?: boolean;
  /**
   * Force or constrain the model's tool selection for this request. `'auto'` lets
   * the model decide (the API default), `'required'` forces it to call SOME tool,
   * `'none'` forbids tool calls, and `{ type: 'tool', toolName }` pins one specific
   * tool. Mirrors the reference's `tool_choice` request parameter (services/api/claude.ts).
   * Threaded into the AI SDK `toolChoice` option; omitted (model default) when unset.
   */
  toolChoice?: 'auto' | 'required' | 'none' | { type: 'tool'; toolName: string };
  /**
   * Generation stop strings. When the model emits ANY of these sequences the
   * provider halts generation at that point (the sequence itself is not included
   * in the output). Useful for bounded classifier / validator outputs and format
   * fencing — a caller can terminate on a delimiter (e.g. `['</answer>']`).
   * Forwarded verbatim as the AI SDK `stopSequences` option; omitted (no stop
   * strings) when unset or empty. Mirrors the reference's `stop_sequences` request
   * parameter (utils/sideQuery.ts).
   */
  stopSequences?: string[];
  /**
   * Stable user/session identifier forwarded to the provider as the request
   * metadata user id — Anthropic `metadata.user_id`, OpenAI `user`. A non-lock-in
   * input parameter that drives abuse detection and can influence rate-limit
   * bucketing. Mirrors the reference's `getAPIMetadata().user_id` (services/api/claude.ts).
   * Populated from this field or the AMI_METADATA_USER_ID / CLAUDE_CODE_METADATA_USER_ID
   * env knob ({@link resolveMetadataUserId}); unset ⇒ no metadata is sent.
   *
   * `extra` carries arbitrary correlation fields (device/session ids, deployment
   * tags) merged into the forwarded metadata value, mirroring the reference's
   * `CLAUDE_CODE_EXTRA_METADATA` JSON knob (also populatable via AMI_EXTRA_METADATA /
   * CLAUDE_CODE_EXTRA_METADATA). When present, the value becomes a JSON object
   * `{ ...extra, user_id? }` so a gateway can parse every field.
   */
  metadata?: { userId?: string; extra?: Record<string, unknown> };
  /**
   * Suppress prompt-cache WRITES for this one request. Set on fire-and-forget /
   * one-off auxiliary model calls (compaction summaries, insight extraction) whose
   * prefix no future turn will ever read back — writing a fresh cache entry for them
   * is pure cost with no hit. The read path is unaffected; only the Anthropic
   * `cache_control` write markers are dropped. Mirrors the reference's `skipCacheWrite`.
   */
  skipCacheWrite?: boolean;
  /**
   * Strip ALL `anthropic-beta` tokens from the outgoing request. Set internally by
   * {@link streamChatCompletionWithRetry} after a gateway/proxy rejects the beta
   * header with a 400 — the first retry drops the ENTIRE beta set (not just the
   * extended-cache token), since a strict proxy that rejects one unknown beta
   * typically rejects every one. Mirrors the reference's gateway beta-rejection
   * fallback covering all beta headers. Not user-facing; off by default.
   */
  disableBetas?: boolean;
  /**
   * Opt in to the NON-STREAMING request fallback. When a streaming attempt fails
   * in a way that signals a broken SSE transport — a gateway/proxy that returns a
   * 200 with a non-SSE body (the empty-stream sentinel) or a 404 on the streaming
   * endpoint — `streamChatCompletionWithRetry` makes ONE bounded, token-capped
   * non-streaming request (AI SDK `generateText`) and replays its result as the
   * normal stream, recovering a turn a proxy would otherwise have killed. Mirrors
   * the reference's executeNonStreamingRequest fallback (services/api/claude.ts).
   * Also enabled process-wide via SUPERINFERENCE_NONSTREAMING_FALLBACK /
   * CLAUDE_CODE_NONSTREAMING_FALLBACK. Off by default (streaming-only retry is
   * unchanged); an explicit falsy config value overrides a truthy env knob.
   */
  nonStreamingFallback?: boolean;
  /**
   * Provider-native structured output: constrain the model response to a JSON
   * Schema so the returned text is GUARANTEED schema-valid JSON instead of being
   * coaxed by a "respond with ONLY JSON" prompt and then parsed best-effort.
   * Threaded into the AI SDK `output: Output.object({ schema })` option, which
   * each provider maps to its native structured-output mechanism (OpenAI
   * `response_format: { type: 'json_schema' }`, Anthropic tool-schema). Set on
   * auxiliary/LLM-judge calls (e.g. the critic's {approved,score,reason} verdict)
   * so a formatting miss can never masquerade as a substantive negative verdict.
   * Omitted (free-form text) when unset. Mirrors the reference's `outputFormat:
   * { type: 'json_schema', schema }` on auxiliary queries (commands/rename/
   * generateSessionName.ts → services/api/claude.ts output_config.format).
   */
  responseFormat?: {
    type: 'json_schema';
    /** JSON Schema object the model output must satisfy. */
    schema: Record<string, unknown>;
    /** Optional schema name forwarded to providers that use it for LLM guidance. */
    name?: string;
  };
}

export const STREAM_CHUNK_TYPES = ['content_delta', 'tool_call_delta', 'thinking_delta', 'done', 'error'] as const;

export type StreamChunkType = (typeof STREAM_CHUNK_TYPES)[number];

export interface StreamChunk {
  type: StreamChunkType;
  text?: string;
  toolCall?: {
    index: number;
    id?: string;
    function?: {
      name?: string;
      arguments?: string;
    };
  };
  finishReason?: string;
  usage?: {
    promptTokens: number;
    completionTokens: number;
    totalTokens: number;
    reasoningTokens?: number;
    cachedPromptTokens?: number;
    cacheCreationTokens?: number;
    /** Cache-WRITE tokens billed at the 5-minute ephemeral TTL (parity:
     *  `cache_creation.ephemeral_5m_input_tokens`). */
    cacheCreation5mTokens?: number;
    /** Cache-WRITE tokens billed at the 1-hour ephemeral TTL (parity:
     *  `cache_creation.ephemeral_1h_input_tokens`). */
    cacheCreation1hTokens?: number;
    /** API service tier reported for this request (parity: `service_tier`). */
    serviceTier?: ServiceTier;
    /** Server-side web-search requests reported in this chunk's usage
     *  (parity: `server_tool_use.web_search_requests`). */
    webSearchRequests?: number;
    /** Server-side web-fetch requests reported in this chunk's usage
     *  (parity: `server_tool_use.web_fetch_requests`). */
    webFetchRequests?: number;
  };
  error?: string;
  /** The original Error object from the SDK or provider, preserved for structured classification. */
  errorObject?: Error;
  /** Present on error chunks — the Retry-After value in seconds from the response header, if any. */
  retryAfter?: number;
  /** Response headers from the API call (available on 'done' and 'error' chunks). */
  responseHeaders?: Record<string, string>;
}

/** Per-request API service tier the model call ran under (parity: the reference's usage
 *  `service_tier`, default 'standard'). A throughput/latency class reflected per
 *  request for cost/latency attribution. */
export const SERVICE_TIERS = ['standard', 'priority', 'batch'] as const;

export type ServiceTier = (typeof SERVICE_TIERS)[number];

export interface UsageStats {
  promptTokens: number;
  completionTokens: number;
  reasoningTokens: number;
  totalTokens: number;
  totalCost: number;
  requestCount: number;
  toolCallCount: number;
  turnCount: number;
  /** Prompt tokens served from cache. */
  cachedPromptTokens?: number;
  /** Prompt tokens that were not cached. */
  uncachedPromptTokens?: number;
  /** Fraction of prompt tokens served from cache (0..1). */
  cacheHitRate?: number;
  /** Estimated dollar savings from prompt caching. */
  cachedCostSavings?: number;
  /** API service tier the request(s) ran under (parity: the reference usage `service_tier`,
   *  default 'standard'). Carried for cost/latency attribution. */
  serviceTier?: ServiceTier;
  /** Count of server-side web-search requests folded into this usage record
   *  (parity: the reference `server_tool_use.web_search_requests`). */
  webSearchRequests?: number;
  /** Count of server-side web-fetch requests folded into this usage record
   *  (parity: the reference `server_tool_use.web_fetch_requests`). */
  webFetchRequests?: number;
}

export interface ThinkingConfig {
  enabled: boolean;
  level: ThinkingLevel;
  budgetTokens?: number;
  type?: 'enabled' | 'disabled';
}

/**
 * Every reason the engine loop can terminate a submit() and emit its final
 * `done` event. This runtime array is the single source of truth: the
 * `ExitReason` union below is derived from it, so adding a variant here adds it
 * to the type (and vice versa is impossible). Tests introspect this value to
 * guarantee every variant is covered.
 */
export const EXIT_REASONS = [
  'completed',
  'max_turns',
  'error',
  'abort',
  'session_timeout',
  'max_steps',
  'budget_exceeded',
  'hook_denied',
  'superinference_stop',
  'consecutive_errors',
  'escalation_terminate',
  'no_edit_stall',
  'keepalive_exhausted',
  'context_too_small',
  'usage_limit_watchdog',
] as const;

export type ExitReason = (typeof EXIT_REASONS)[number];

export const ENGINE_EVENT_TYPES = [
  'text_delta', 'thinking_delta', 'tool_call_generating', 'tool_use_start',
  'tool_use_progress', 'tool_use_result', 'turn_complete', 'usage_update',
  'error', 'retry_attempt', 'error_classified', 'provider_changed',
  'checkpoint_created', 'analytics_summary', 'superinference_state',
  'suggest_file_save', 'user_question', 'session_title', 'session_init', 'memory_warning',
  'plan_mode_changed', 'task_updated', 'skill_activated',
  'subagent_start', 'subagent_progress', 'subagent_result', 'todo_update',
  'mcp_resource_update', 'mcp_polling_update', 'mcp_connection_changed',
  'external_import_blocked', 'peer_message', 'background_task_complete',
  'hook_response', 'max_turns_reached', 'classifier_checking',
  'skill_improvement', 'agent_turn_stop', 'done',
] as const;

/** A single todo item as rendered to the user (T1/T5). `id` is optional
 *  (the reference parity); `activeForm` is the present-continuous label shown while
 *  the item is in progress. */
export interface TodoItemView {
  id?: string;
  content: string;
  status: 'pending' | 'in_progress' | 'completed' | 'cancelled';
  activeForm?: string;
}

/**
 * One selectable option in an AskUserQuestion prompt (parity: the reference's
 * `QuestionOption`). Single source of truth for the option shape, shared by the
 * `user_question` {@link EngineEvent} and the {@link EngineConfig.onUserQuestion}
 * callback so the carried fields can never drift between emit and handler.
 */
export interface UserQuestionOption {
  label: string;
  description: string;
  /** Markdown preview rendered in a bordered box when this option is focused
   *  (side-by-side preview panel). Parity: the reference's `option.preview`. */
  preview?: string;
  /** When true the option is shown but not selectable (dimmed; Enter/number/space
   *  refuse to act on it). Parity: the reference's `option.disabled`. */
  disabled?: boolean;
  /** Controls whether this option's DESCRIPTION column renders dimmed. Descriptions
   *  are dimmed by default (the secondary-context look); set `false` to keep a
   *  particular option's description at full brightness so a load-bearing note
   *  (e.g. a warning or the recommended choice's rationale) is not visually
   *  demoted. Parity: the reference's CustomSelect per-option `dimDescription` override
   *  (which defaults to dimmed but can be turned off per option). */
  dimDescription?: boolean;
}

/**
 * A per-question annotation the user attached to their AskUserQuestion answer,
 * relayed back to the model as structured context (parity: the reference's AskUserQuestion
 * per-option preview/notes annotation value). Keyed by question text in the
 * `user_question` {@link EngineEvent}'s `annotations` map. Typed (rather than an
 * opaque `unknown`) so the preview the user actually chose and any free-text notes
 * are discriminable by both the renderer and the model-facing result builder.
 */
export interface UserQuestionAnnotation {
  /** The preview content of the option the user selected, when the question used
   *  previews — lets the model see WHICH concrete artifact the user picked. */
  preview?: string;
  /** Free-text notes the user added alongside their selection. */
  notes?: string;
}

/** Three-state terminal outcome of a (sub)agent/task, distinguishing a clean finish
 *  from an error from a user/abort stop (parity: the reference's task_notification `status`:
 *  'completed' | 'failed' | 'stopped'). Richer than a bare success boolean. */
export const TASK_TERMINAL_STATUS = ['completed', 'failed', 'stopped'] as const;

export type TaskTerminalStatus = (typeof TASK_TERMINAL_STATUS)[number];

/** Live machine-readable usage snapshot for a running (sub)agent, carried on progress
 *  and terminal events (parity: the reference's task_progress usage `{ total_tokens, tool_uses,
 *  duration_ms }`). */
export interface TaskUsageSnapshot {
  /** Total tokens consumed by the (sub)agent so far. */
  totalTokens: number;
  /** Number of tool calls the (sub)agent has made so far. */
  toolUses: number;
  /** Wall-clock milliseconds elapsed since the (sub)agent started. */
  durationMs: number;
}

/** Persisted per-skill usage statistic (parity: the reference's global-config
 *  `skillUsage` map, `utils/suggestions/skillUsageTracking.ts`). Keyed by skill name
 *  in the global config, each entry records how many times the skill was invoked and
 *  when it was last used (epoch ms). Lets a skill's frecency ranking survive across
 *  sessions instead of resetting to zero every launch (AMI previously tracked this
 *  only in-memory, session-scoped). */
export interface SkillUsageStat {
  /** Total number of times this skill has been invoked (monotonic, persisted). */
  usageCount: number;
  /** Epoch-ms timestamp of the most recent invocation. */
  lastUsedAt: number;
}

/** Fold one invocation of `name` into a persisted {@link SkillUsageStat} map at time
 *  `now`, returning a NEW map (the input is never mutated). The single canonical
 *  increment+stamp formula so every persistence call site agrees — bumps `usageCount`
 *  and refreshes `lastUsedAt`. A malformed/absent prior entry is treated as a zero
 *  baseline. Parity: the reference's `skillUsage[name] = { usageCount: prev+1, lastUsedAt: now }`. */
export function mergeSkillUsageStat(
  record: Record<string, SkillUsageStat> | undefined,
  name: string,
  now: number = Date.now(),
): Record<string, SkillUsageStat> {
  const prev = record?.[name];
  const prevCount = typeof prev?.usageCount === 'number' && Number.isFinite(prev.usageCount) && prev.usageCount > 0
    ? prev.usageCount
    : 0;
  return { ...(record ?? {}), [name]: { usageCount: prevCount + 1, lastUsedAt: now } };
}

/** The operating MODE a session ran under, persisted so `--resume` can reconcile a
 *  resumed session against the live environment (parity: the reference's
 *  `coordinatorMode.ts` `SessionMode` / `matchSessionMode`). `'coordinator'` is the
 *  pure-orchestrator stance (SUPERINFERENCE_COORDINATOR_MODE); `'normal'` is the
 *  default implementer stance. Distinct from permission mode and sandbox mode. */
export const SESSION_OPERATING_MODES = ['normal', 'coordinator'] as const;

export type SessionOperatingMode = (typeof SESSION_OPERATING_MODES)[number];

/** Outcome of reconciling a resumed session's stored operating mode against the live
 *  mode (see {@link reconcileSessionMode}). `changed` is true only when the caller must
 *  flip the live mode; `target` is the mode the session should run under; `warning` is a
 *  human-readable notice to surface when (and only when) a flip occurred. */
export interface SessionModeReconciliation {
  changed: boolean;
  target: SessionOperatingMode;
  warning?: string;
}

/**
 * Reconcile a resumed session's STORED operating mode against the CURRENT (live) mode,
 * returning what the caller should do. Pure — it mutates nothing and reads no globals, so
 * every resume path can share one decision formula. Branch logic mirrors the reference's
 * `matchSessionMode` (coordinator/coordinatorMode.ts):
 *   - `stored === undefined` → no-op (`changed:false`), back-compat for sessions saved
 *     before operating-mode tracking existed (they carry no mode to match).
 *   - `stored === current` → no-op; the live mode already matches the resumed session.
 *   - mismatch → `changed:true`, `target = stored` (the resumed session wins), and a
 *     `warning` describing the correction ("Entered/Exited coordinator mode to match
 *     resumed session.") so a silently-wrong mode on resume is surfaced + corrected.
 */
export function reconcileSessionMode(
  stored: SessionOperatingMode | undefined,
  current: SessionOperatingMode,
): SessionModeReconciliation {
  if (stored === undefined || stored === current) {
    return { changed: false, target: current };
  }
  const warning = stored === 'coordinator'
    ? 'Entered coordinator mode to match resumed session.'
    : 'Exited coordinator mode to match resumed session.';
  return { changed: true, target: stored, warning };
}
