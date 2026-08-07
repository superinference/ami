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
   *  Used by file_edit/file_write to enforce read-before-write. */
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
  detachedMode?: boolean;
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
  };
  error?: string;
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
}

export type EngineEvent =
  | { type: 'text_delta'; text: string }
  | { type: 'thinking_delta'; text: string }
  | { type: 'tool_use_start'; toolName: string; toolCallId: string; input: Record<string, unknown> }
  | { type: 'tool_use_progress'; toolCallId: string; data: string }
  | { type: 'tool_use_result'; toolCallId: string; toolName: string; output: string; isError: boolean }
  | { type: 'turn_complete'; content: string | null; toolCalls: ToolCallContent[] }
  | { type: 'usage_update'; stats: UsageStats }
  | { type: 'error'; error: string }
  | { type: 'retry_attempt'; attempt: number; maxRetries: number; delayMs: number; statusCode: number }
  | { type: 'error_classified'; category: string; retryable: boolean; willRetry: boolean }
  | { type: 'provider_changed'; provider: string; model: string; reason: string }
  | { type: 'checkpoint_created'; checkpointId: string; files: string[] }
  | { type: 'analytics_summary'; summary: Record<string, number> }
  | { type: 'superinference_state'; state: { value: number; entropy: number; eig: number; step: number; ppv: number }; stopReason: { type: string; detail: string } }
  | { type: 'suggest_file_save'; content: string; suggestedPath: string; lineCount: number }
  | { type: 'user_question'; toolCallId: string; question: string; options: Array<{ label: string; description: string }>; allowFreeText: boolean; header?: string; multiSelect?: boolean; answers?: string[]; annotations?: Record<string, unknown> }
  | { type: 'session_title'; title: string }
  | { type: 'plan_mode_changed'; enabled: boolean }
  | { type: 'task_updated'; taskId: number; status: string; subject: string }
  | { type: 'done'; totalTurns: number };

export interface EngineConfig {
  provider: ProviderConfig;
  cwd: string;
  tools?: ToolDefinition[];
  systemPrompt?: string;
  maxTurns?: number;
  maxTurnsCeiling?: number;
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
  /** Cheap/fast model for auxiliary tasks (compaction, follow-up suggestions). Falls back to primary model if not set. */
  compactionModel?: string;
  /** Extended thinking/reasoning configuration. */
  thinking?: ThinkingConfig;
  /** SuperInference PRE-loop configuration. When enabled, the engine tracks
   *  belief state, runs a critic, and can stop early on confidence/EIG thresholds. */
  superinference?: SuperInferenceConfig;
  /** Lifecycle hooks for post-sampling, stop, and error events. */
  hooks?: import('./hooks').HookManager;
  /** Persona name — changes system prompt identity and defaults. */
  persona?: string;
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
  /** Maximum USD budget for the session. If set, the engine stops when total cost exceeds this amount. */
  maxBudgetUsd?: number;
  /** Chat mode — controls tool availability and autonomy level. */
  mode?: 'ask' | 'edit' | 'agent';
  /** Maximum tool iterations before prompting to continue. Default 200. */
  maxToolIterations?: number;
  /** Behavior when tool iteration limit is reached: 'stop' or 'confirm'. Default 'confirm'. */
  toolIterationBehavior?: 'stop' | 'confirm';
}

// ---------------------------------------------------------------------------
// DI subsystem interfaces — typed groups of related EngineConfig fields
// ---------------------------------------------------------------------------

export interface ProviderSubsystem {
  primary: ProviderConfig;
  fallbackModel?: string;
  compactionModel?: string;
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
      compactionModel: config.compactionModel,
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
      maxTurns: config.maxTurns === 0 ? Infinity : (config.maxTurns ?? 100),
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
}

export interface ContextFile {
  path: string;
  content: string;
  language?: string;
}
