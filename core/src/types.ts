import type { SuperInferenceConfig } from './superinference/types';

export interface TextContent {
  type: 'text';
  text: string;
}

export interface ImageContent {
  type: 'image';
  source: {
    type: 'base64';
    media_type: string;
    data: string;
  };
}

export interface ToolCallFunction {
  name: string;
  arguments: string;
}

export interface ToolCallContent {
  id: string;
  type: 'function';
  function: ToolCallFunction;
}

export interface UserMessage {
  role: 'user';
  content: string | Array<TextContent | ImageContent>;
}

export interface AssistantMessage {
  role: 'assistant';
  content: string | null;
  tool_calls?: ToolCallContent[];
}

export interface ToolMessage {
  role: 'tool';
  tool_call_id: string;
  content: string;
}

export interface SystemMessage {
  role: 'system';
  content: string;
}

export type Message = UserMessage | AssistantMessage | ToolMessage | SystemMessage;

export interface JsonSchemaProperty {
  type: string;
  description?: string;
  enum?: string[];
  default?: unknown;
  items?: JsonSchemaProperty;
  properties?: Record<string, JsonSchemaProperty>;
  required?: string[];
  additionalProperties?: boolean | JsonSchemaProperty;
  [key: string]: unknown;
}

export interface ToolInputSchema {
  type: 'object';
  properties: Record<string, JsonSchemaProperty>;
  required?: string[];
}

export interface ToolResult {
  output: string;
  isError?: boolean;
  metadata?: Record<string, unknown>;
}

export interface ToolContext {
  cwd: string;
  abortSignal: AbortSignal;
  onProgress?: (data: string) => void;
  /** Tracks file paths that have been read during this session.
   *  Used by file_edit to enforce read-before-write. */
  filesRead?: Set<string>;
  processManager?: import('./process-manager').ProcessManager;
  _providerConfig?: ProviderConfig;
  _permissionPromptHandler?: import('./permissions').PermissionPromptHandler;
  _allowLocalhostForTesting?: boolean;
  _engineFactory?: (config: EngineConfig) => { submit(prompt: string): AsyncIterable<EngineEvent>; shutdown?(): void };
  _skillManager?: import('./skills').SkillManager;
  _hookManager?: import('./hooks').HookManager;
  _mcpManager?: any;
  _parentMessages?: Message[];
  _engineAddSystemReminder?: (msg: string) => void;
  /** Signal the engine to load a deferred tool into the active set (tool_search select). */
  _loadDeferredTool?: (name: string) => void;
  /** Per-tool output budgets (e.g. inline bash stdout cap before spill). */
  outputBudget?: { bashMaxChars?: number; taskTailLines?: number; taskMaxChars?: number };
  /** Parent engine's thinking config, inherited by subagents unless overridden. */
  _parentThinking?: ThinkingConfig;
  /** Parent engine's permission rules, forwarded to subagents so deny rules are inherited. */
  _parentPermissionRules?: Array<{ tool: string; pattern?: string; action: 'allow' | 'deny' | 'ask' }>;
  /** Session-wide web-search budget with token-bucket refill to stop runaway loops. */
  _webSearchBudget?: { count: number; max: number; lastRefillTime?: number; refillRate?: number };
  /** Test-only dependency seam for web_search (mock HTTP getter / SSRF validator /
   *  search-URL builder). Never set in production; typed `unknown` here and cast at
   *  the web-search call site to avoid a types.ts -> tools/web-search import cycle. */
  _webSearchHooks?: unknown;
  /** Test-only dependency seam for web_fetch's HTTP getter. Never set in
   *  production; typed `unknown` (cast at the web-fetch call site) to avoid a
   *  types.ts -> tools/web-utils import cycle. */
  _webFetchHttpGet?: unknown;
  detachedMode?: boolean;
  /** Current subagent nesting depth (0 = top-level). */
  _agentDepth?: number;
  /** Maximum subagent nesting depth (default 5). */
  _maxAgentDepth?: number;
  /** Shared mutable counter: total agents spawned this session (never decreases). */
  _agentSpawnCount?: { value: number };
  /** Max total agents allowed in a single session (default 50). */
  _maxAgentsPerSession?: number;
  /** Max concurrently running agents (default 10). */
  _maxConcurrentAgents?: number;
  /** Session-lifetime subagent budget (honors CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION
   *  and related env vars). task.ts consults isLifetimeBudgetExhausted() to block
   *  spawns past the per-session cap; depth/concurrency stay governed by the
   *  _agentDepth/_maxConcurrentAgents logic above. */
  subagentLimiter?: import('./subagent-limiter').SubagentLimiter;
}

export interface ToolDefinition {
  name: string;
  description: string;
  inputSchema: ToolInputSchema;
  isReadOnly: boolean;
  isConcurrencySafe?: boolean;
  execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult>;
}

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
  /** Extra headers merged into every LLM API request. Used for gateway hint
   *  headers (request class, agent type, compaction state). */
  extraHeaders?: Record<string, string>;
  fallbackModel?: string;
}

export interface StreamChunk {
  type: 'content_delta' | 'tool_call_delta' | 'thinking_delta' | 'done' | 'error';
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
  };
  error?: string;
  /** The original Error object from the SDK or provider, preserved for structured classification. */
  errorObject?: Error;
  /** Present on error chunks — the Retry-After value in seconds from the response header, if any. */
  retryAfter?: number;
  /** Response headers from the API call (available on 'done' and 'error' chunks). */
  responseHeaders?: Record<string, string>;
}

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
}

import type { ThinkingLevel } from './model-capabilities';
export type { ThinkingLevel } from './model-capabilities';

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

export type EngineEvent =
  | { type: 'text_delta'; text: string }
  | { type: 'thinking_delta'; text: string }
  | { type: 'tool_call_generating'; toolName: string; toolIndex: number; argsDelta: string }
  | { type: 'tool_use_start'; toolName: string; toolCallId: string; input: Record<string, unknown> }
  | { type: 'tool_use_progress'; toolCallId: string; data: string }
  | { type: 'tool_use_result'; toolCallId: string; toolName: string; output: string; isError: boolean }
  | { type: 'turn_complete'; content: string | null; toolCalls: ToolCallContent[]; finishReason?: string }
  | { type: 'usage_update'; stats: UsageStats }
  | { type: 'error'; error: string }
  | { type: 'retry_attempt'; attempt: number; maxRetries: number; delayMs: number; statusCode: number }
  | { type: 'error_classified'; category: string; retryable: boolean; willRetry: boolean; message?: string }
  | { type: 'provider_changed'; provider: string; model: string; reason: string }
  | { type: 'checkpoint_created'; checkpointId: string; files: string[] }
  | { type: 'analytics_summary'; summary: Record<string, number> }
  | { type: 'superinference_state'; state: { value: number; entropy: number; eig: number; step: number; ppv: number }; stopReason: { type: string; detail: string } }
  | { type: 'suggest_file_save'; content: string; suggestedPath: string; lineCount: number }
  | { type: 'user_question'; toolCallId: string; question: string; options: Array<{ label: string; description: string }>; allowFreeText: boolean; header?: string; multiSelect?: boolean; answers?: string[]; annotations?: Record<string, unknown> }
  | { type: 'session_title'; title: string }
  | { type: 'memory_warning'; rssBytes: number; thresholdBytes: number; recoverySteps: string[] }
  | { type: 'plan_mode_changed'; enabled: boolean }
  | { type: 'task_updated'; taskId: number; status: string; subject: string }
  | { type: 'done'; totalTurns: number; exitReason: ExitReason };

export interface EngineConfig {
  provider: ProviderConfig;
  cwd: string;
  tools?: ToolDefinition[];
  systemPrompt?: string;
  maxTurns?: number;
  maxTurnsCeiling?: number;
  /** Max time (ms) to wait for MCP servers to connect before the first submit proceeds (default 3000). */
  mcpStartupBudgetMs?: number;
  /** Session-wide cap on web_search tool calls (default 200) to stop runaway loops. */
  maxWebSearchesPerSession?: number;
  /** Cap (chars) on inline bash stdout before it is spilled to a file. */
  bashOutputMaxChars?: number;
  /** Cap (chars) on inline background task output before it is spilled to a file. */
  taskOutputMaxChars?: number;
  /** Inactivity watchdog (ms) for a stalled-but-open stream, distinct from the total request timeout. */
  streamIdleTimeoutMs?: number;
  /** RSS threshold (bytes) that emits a one-time memory_warning with recovery steps (default 1.5 GiB). */
  memoryWarningRssBytes?: number;
  /** Injectable RSS reader for tests (defaults to process.memoryUsage().rss). */
  memoryUsageReader?: () => number;
  /** Defer long-tail tools from the per-turn catalog until tool_search loads them, saving prompt tokens. */
  deferTools?: boolean;
  /** Extra tool names to always keep loaded when deferTools is on (beyond the essential set + tool_search). */
  alwaysLoadTools?: string[];
  abortController?: AbortController;
  onPermissionRequest?: (toolName: string, input: Record<string, unknown>) => Promise<boolean>;
  /** Centralized permission prompt handler. Both CLI and VSCode implement this interface. */
  permissionPromptHandler?: import('./permissions').PermissionPromptHandler;
  /** Maximum token budget for context management. Defaults to 100 000. */
  tokenBudget?: number;
  /** Resume a specific session. When set, the engine loads its messages on init. */
  sessionId?: string;
  /** Override the default session directory ({cwd}/.superinference/sessions/). */
  sessionDir?: string;
  /** Permission mode: 'ask' (default), 'auto-allow', or 'deny-all'. */
  permissionMode?: string;
  /** Permission rules evaluated in order; first match wins. */
  permissionRules?: Array<{ tool: string; pattern?: string; action: 'allow' | 'deny' | 'ask' }>;
  /** Model to fall back to on errors (e.g., 'gemini-2.0-flash' as fallback for 'gemini-1.5-pro'). */
  fallbackModel?: string;
  /** Ordered fallback model chain (up to 3), stepped through on provider errors. */
  fallbackModels?: string[];
  /** Cheap/fast model for auxiliary tasks (compaction, follow-up suggestions). Falls back to primary model if not set. */
  compactionModel?: string;
  /** Architect model for dual-model (plan + execute) mode. When set, the engine runs a
   *  reasoning phase with this model before each tool-execution phase with the primary model. */
  architectModel?: string;
  /** Extended thinking/reasoning configuration. */
  thinking?: ThinkingConfig;
  /** Cap the reasoning effort level (e.g. 'medium' prevents 'high'/'max'). */
  maxEffortLevel?: string;
  /** SuperInference belief tracking. shouldStop() ends the session only when enforcing is true. */
  superinference?: SuperInferenceConfig;
  /** Lifecycle hooks for post-sampling, stop, and error events. */
  hooks?: import('./hooks').HookManager;
  /** Persona name — changes system prompt identity and defaults. */
  persona?: string;
  /** Named output style overlay appended to the system prompt (e.g. 'concise'). */
  outputStyle?: string;
  /** Managed allowlist of selectable models; non-listed models are blocked. */
  enforceAvailableModels?: string[];
  /** Merged project-level configuration (from .superinference/config.json). */
  projectConfig?: import('./config').ProjectConfig;
  /** Maximum number of tool-call steps before forcing a summary. When reached,
   *  the engine injects an assistant prefill requesting a summary and stops
   *  processing further tool calls. Default: unlimited. */
  maxSteps?: number;
  /** When true, the engine operates in read-only "plan" mode: writable tools
   *  are blocked, and the system prompt instructs the model to only read,
   *  search, and describe its plan. */
  planMode?: boolean;
  /** Handler for AskUserQuestion tool. The UI implements this to show
   *  the question and return the user's answer. */
  onUserQuestion?: (question: string, options: Array<{ label: string; description: string }>, allowFreeText: boolean, multiSelect?: boolean) => Promise<string>;
  /** When true, the engine generates a session title via LLM after the first turn. */
  enableTitleGeneration?: boolean;
  /** When true, the engine is running in non-interactive (detached / --prompt) mode. */
  detachedMode?: boolean;
  /** Current subagent nesting depth — propagated to child ToolContext. */
  agentDepth?: number;
  /** Maximum subagent nesting depth (default 5). */
  maxAgentDepth?: number;
  /** Maximum USD budget for the session. If set, the engine stops when total cost exceeds this amount. */
  maxBudgetUsd?: number;
  /** Wall-clock session limit in milliseconds. If set, the engine stops when elapsed time exceeds this. */
  maxSessionMs?: number;
  /** Chat mode — controls tool availability and autonomy level. */
  mode?: 'ask' | 'edit' | 'agent';
  /** Per-request timeout in milliseconds for the API provider. Default: 300 000 (5 min). */
  requestTimeoutMs?: number;
  /** @internal Override provider-level max retries (for testing). */
  _providerMaxRetries?: number;
  /** @internal Override provider-level base delay in ms (for testing). */
  _providerBaseDelayMs?: number;
  /** Maximum tool iterations before prompting to continue. Default 200. */
  maxToolIterations?: number;
  /** Safe mode: skip loading untrusted project-local config — no project hooks
   *  (.superinference/hooks.json), no MCP server auto-connect, and no project
   *  memory/skills in the system prompt. For running in untrusted repos. */
  safeMode?: boolean;
  /** Bare mode: strip system prompt to minimal identity + date. No persona,
   *  memory, skills, project config, or tool instructions. For benchmarking
   *  and raw LLM interaction. */
  bareMode?: boolean;
  /** Behavior when tool iteration limit is reached: 'stop' or 'confirm'. Default 'confirm'. */
  toolIterationBehavior?: 'stop' | 'confirm';
  /** Minimum estimated tokens before idle compaction will run (default 200 000). */
  idleCompactionMinTokens?: number;
  /** Milliseconds of inactivity after a turn before proactive compaction fires (default 3 240 000 = 54 min). */
  idleCompactionDelayMs?: number;
  /** Maximum milliseconds to wait for a usage-limit retry-after in detached mode.
   *  If the Retry-After header exceeds this, the session terminates instead of
   *  waiting indefinitely. Default: 300 000 (5 min). Set 0 to disable. */
  maxUsageLimitWaitMs?: number;
  /** Maximum delay cap for exponential backoff on overloaded (529) retries, in ms.
   *  Prevents unbounded waits. Default: 60 000 (1 min). Env: SUPERINFERENCE_OVERLOADED_RETRY_MAX_DELAY_MS. */
  overloadedRetryMaxDelayMs?: number;
  /** Base delay for overloaded (529) retries, in ms. Default: 1000.
   *  Env: SUPERINFERENCE_OVERLOADED_RETRY_BASE_DELAY_MS. */
  overloadedRetryBaseDelayMs?: number;
  /** Maximum total wall-clock time for all retry attempts, in ms. If total retry
   *  time exceeds this, stop retrying. Default: 0 (no limit).
   *  Env: SUPERINFERENCE_RETRY_MAX_TOTAL_WAIT_MS. */
  retryMaxTotalWaitMs?: number;
  /** Maximum retries specifically for non-streaming timeout fallbacks.
   *  Env: SUPERINFERENCE_NONSTREAMING_TIMEOUT_RETRIES. */
  nonstreamingTimeoutRetries?: number;
  /** Maximum transient error retries (connection resets, 502/503/529). Default: 5.
   *  Env: SUPERINFERENCE_MAX_TRANSIENT_RETRIES. */
  maxTransientRetries?: number;
  /** Maximum delay cap for transient error backoff, in ms. Default: 30 000 (30s).
   *  Env: SUPERINFERENCE_TRANSIENT_RETRY_MAX_DELAY_MS. */
  transientRetryMaxDelayMs?: number;
  /** Enable gateway hint headers on LLM requests (x-superinference-request-class,
   *  x-superinference-agent-type, x-superinference-compaction).
   *  Env: SUPERINFERENCE_GATEWAY_HINT_HEADERS=1. */
  gatewayHintHeaders?: boolean;
  /** @internal Override the chat-completion stream implementation (for testing).
   *  Defaults to the real streamChatCompletionWithRetry. Injecting a generator
   *  that emits thinking-only chunks or throws mid-iteration drives the engine's
   *  thinking/abort/error stream branches that mock SSE transports cannot reach.
   *  Typed as `unknown` (cast at the common/engine.ts call site) to avoid a
   *  types.ts -> provider.ts import cycle. */
  _streamChatImpl?: unknown;
}

// ---------------------------------------------------------------------------
// DI subsystem interfaces — typed groups of related EngineConfig fields
// ---------------------------------------------------------------------------

export interface ProviderSubsystem {
  primary: ProviderConfig;
  fallbackModel?: string;
  /** Ordered fallback chain (normalized from fallbackModels + fallbackModel, deduped, capped). */
  fallbackModels?: string[];
  compactionModel?: string;
  /** Architect model for dual-model (plan + execute) mode. */
  architectModel?: string;
  thinking?: ThinkingConfig;
}

export interface PermissionSubsystem {
  mode: string;
  rules: Array<{ tool: string; pattern?: string; action: 'allow' | 'deny' | 'ask' }>;
  promptHandler?: import('./permissions').PermissionPromptHandler;
}

export interface SessionSubsystem {
  sessionId?: string;
  sessionDir?: string;
  tokenBudget: number;
  maxTurns: number;
  maxSteps?: number;
}

export function buildSubsystems(config: EngineConfig): {
  provider: ProviderSubsystem;
  permissions: PermissionSubsystem;
  session: SessionSubsystem;
} {
  return {
    provider: {
      primary: config.provider,
      fallbackModel: config.fallbackModel,
      fallbackModels: (() => {
        const chain = [...(config.fallbackModels ?? []), ...(config.fallbackModel ? [config.fallbackModel] : [])]
          .filter((m): m is string => !!m);
        return [...new Set(chain)].slice(0, 3);
      })(),
      compactionModel: config.compactionModel,
      architectModel: config.architectModel,
      thinking: config.thinking,
    },
    permissions: {
      mode: config.permissionMode ?? 'ask',
      rules: config.permissionRules ?? [],
      promptHandler: config.permissionPromptHandler,
    },
    session: {
      sessionId: config.sessionId,
      sessionDir: config.sessionDir,
      tokenBudget: config.tokenBudget ?? 100_000,
      maxTurns: (config.maxTurns && config.maxTurns > 0 && Number.isFinite(config.maxTurns))
        ? config.maxTurns
        : 100,
      maxSteps: config.maxSteps,
    },
  };
}

export interface EngineSubmitOptions {
  systemPromptOverride?: string;
  contextFiles?: ContextFile[];
  /** JSON Schema for structured output. When provided, the model output is validated against this schema. */
  outputSchema?: Record<string, unknown>;
  /** Max retries for schema validation (default 3). */
  maxStructuredRetries?: number;
  /**
   * Image attachments for this user turn (e.g. drag-dropped or path-referenced
   * files). Attached to the user message as multimodal content when the active
   * model supports vision; otherwise ignored with a note.
   */
  images?: ImageContent[];
}

export interface ContextFile {
  path: string;
  content: string;
  language?: string;
}

// ---------------------------------------------------------------------------
// EngineConfig validation & sanitization
// ---------------------------------------------------------------------------

export function validateEngineConfig(config: Partial<EngineConfig>): { valid: boolean; warnings: string[] } {
  const warnings: string[] = [];
  let valid = true;

  if (config.maxTurns !== undefined) {
    if (!Number.isInteger(config.maxTurns) || config.maxTurns < 1) {
      warnings.push('maxTurns must be a positive integer');
      valid = false;
    } else if (config.maxTurns > 10000) {
      warnings.push('maxTurns exceeds 10000 — likely misconfigured');
    }
  }
  if (config.maxSessionMs !== undefined) {
    if (config.maxSessionMs <= 0) { warnings.push('maxSessionMs must be positive'); valid = false; }
    else if (config.maxSessionMs > 86_400_000) { warnings.push('maxSessionMs exceeds 24 hours'); valid = false; }
    else if (config.maxSessionMs > 3_600_000) { warnings.push('maxSessionMs exceeds 1 hour — are you sure?'); }
  }
  if (config.maxBudgetUsd !== undefined) {
    if (config.maxBudgetUsd <= 0) { warnings.push('maxBudgetUsd must be positive'); valid = false; }
    else if (config.maxBudgetUsd > 1000) { warnings.push('maxBudgetUsd exceeds $1000'); valid = false; }
    else if (config.maxBudgetUsd > 100) { warnings.push('maxBudgetUsd exceeds $100 — are you sure?'); }
  }
  if (config.maxAgentDepth !== undefined) {
    if (!Number.isInteger(config.maxAgentDepth) || config.maxAgentDepth < 1 || config.maxAgentDepth > 20) {
      warnings.push('maxAgentDepth must be 1-20');
      valid = false;
    } else if (config.maxAgentDepth > 10) {
      warnings.push('maxAgentDepth > 10 — deep nesting may be expensive');
    }
  }
  if (config.maxUsageLimitWaitMs !== undefined && config.maxUsageLimitWaitMs < 0) {
    warnings.push('maxUsageLimitWaitMs must be non-negative');
    valid = false;
  }
  if (config.requestTimeoutMs !== undefined) {
    if (config.requestTimeoutMs < 1000 || config.requestTimeoutMs > 600_000) {
      warnings.push('requestTimeoutMs must be 1000-600000');
      valid = false;
    } else if (config.requestTimeoutMs > 300_000) {
      warnings.push('requestTimeoutMs > 5min may cause timeouts elsewhere');
    }
  }
  if (config.tokenBudget !== undefined && config.tokenBudget <= 0) {
    warnings.push('tokenBudget must be positive');
    valid = false;
  }

  return { valid, warnings };
}

export function sanitizeEngineConfig(config: Partial<EngineConfig>): Partial<EngineConfig> {
  const out = { ...config };
  if (out.maxTurns !== undefined) out.maxTurns = Math.max(1, Math.min(10000, Math.round(out.maxTurns)));
  if (out.maxSessionMs !== undefined) out.maxSessionMs = Math.max(1, Math.min(86_400_000, out.maxSessionMs));
  if (out.maxBudgetUsd !== undefined) out.maxBudgetUsd = Math.max(0.01, Math.min(1000, out.maxBudgetUsd));
  if (out.maxAgentDepth !== undefined) out.maxAgentDepth = Math.max(1, Math.min(20, Math.round(out.maxAgentDepth)));
  if (out.maxUsageLimitWaitMs !== undefined) out.maxUsageLimitWaitMs = Math.max(0, out.maxUsageLimitWaitMs);
  if (out.requestTimeoutMs !== undefined) out.requestTimeoutMs = Math.max(1000, Math.min(600_000, out.requestTimeoutMs));
  if (out.tokenBudget !== undefined) out.tokenBudget = Math.max(1, out.tokenBudget);
  return out;
}
