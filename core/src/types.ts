import type { SuperInferenceConfig } from './superinference/types';

/**
 * Type-branded session id: a nominal `string` subtype so a raw string can't be silently
 * passed where a persisted session id is expected. It stays assignable TO `string`
 * (reads / pass-through / Map keys need no cast); only MINTING one from a raw string goes
 * through {@link asSessionId}. The brand symbol is unexported, so the type is truly
 * nominal. (round-2 "type-branded ids".)
 */
declare const SESSION_ID_BRAND: unique symbol;
export type SessionId = string & { readonly [SESSION_ID_BRAND]: true };
/** Mint a {@link SessionId} from a raw string (the single branding boundary). */
export function asSessionId(id: string): SessionId { return id as SessionId; }

/**
 * Type-branded agent id — a nominal `string` subtype so a free-text teammate name
 * cannot be silently passed where a real agent id is expected (parity: Claude's
 * `AgentId` in types/ids.ts). {@link asAgentId} mints one unconditionally (the
 * pass-through boundary); {@link toAgentId} VALIDATES the format first and returns
 * `null` for a non-conforming string, which is what discriminates a genuine agent id
 * from an arbitrary name at the mint boundary.
 */
declare const AGENT_ID_BRAND: unique symbol;
export type AgentId = string & { readonly [AGENT_ID_BRAND]: true };
/** Mint an {@link AgentId} from a raw string without validation (pass-through boundary). */
export function asAgentId(id: string): AgentId { return id as AgentId; }
/** Validate and brand an agent id; returns `null` when `id` is not a well-formed
 *  agent id (empty, or containing whitespace / disallowed characters). */
export function toAgentId(id: string): AgentId | null {
  return /^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(id) ? (id as AgentId) : null;
}

export const OUTPUT_FORMATS = ['json', 'jsonl', 'text'] as const;
export type OutputFormat = (typeof OUTPUT_FORMATS)[number];

export const INPUT_FORMATS = ['text', 'stream-json'] as const;
export type InputFormat = (typeof INPUT_FORMATS)[number];

export const CHAT_MODES = ['ask', 'edit', 'agent'] as const;
export type ChatMode = (typeof CHAT_MODES)[number];

export const THEMES = ['default', 'dark', 'light'] as const;
export type Theme = (typeof THEMES)[number];

/**
 * Terminal notification channels for turn-completion alerts, selectable per
 * terminal family (parity: Claude's NOTIFICATION_CHANNELS). `auto` picks the best
 * mechanism for the detected terminal; `notifications_disabled` turns alerts off.
 * The runtime array is the single source of truth — {@link NotificationChannel}
 * is derived from it so the enum and the type can never drift.
 */
export const NOTIFICATION_CHANNELS = [
  'auto',
  'iterm2',
  'iterm2_with_bell',
  'terminal_bell',
  'kitty',
  'ghostty',
  'notifications_disabled',
] as const;
export type NotificationChannel = (typeof NOTIFICATION_CHANNELS)[number];

// The built-in output-style NAMES (the string overlays live in common's
// output-style-manager BUILTIN_OUTPUT_STYLES). `learning` is a first-class built-in —
// accepted by the engine, /output-style, and --output-style, and documented in HELP —
// so it MUST be advertised here too (audit #5: the manifest was under-advertising it).
// A gate (output-style-manager test) asserts these names === the manager's built-in set.
export const OUTPUT_STYLES = ['default', 'concise', 'explanatory', 'learning'] as const;
export type OutputStyle = (typeof OUTPUT_STYLES)[number];

/**
 * Single source of truth for the "explanatory" teaching-callout contract. The
 * `explanatory` output-style overlay instructs the model to open each callout with
 * a line beginning with this header; the TUI renderer keys on the SAME constant to
 * style the block. Keeping one constant prevents the emission↔render drift that
 * AGENTS.md warns about for dual prompt paths.
 */
export const INSIGHT_CALLOUT_HEADER = '★ Insight';

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

/**
 * Provenance of an inbound user/queued message (parity: Claude's `MessageOrigin`),
 * which drives per-origin framing — e.g. a `task-notification`/`coordinator`/`channel`
 * message gets an untrusted-source warning that a plain `user` message does not.
 */
export type MessageOrigin =
  | { kind: 'user' }
  | { kind: 'task-notification' }
  | { kind: 'coordinator' }
  | { kind: 'teammate'; name?: string }
  | { kind: 'channel'; source: string; user?: string; chatId?: string };

/**
 * Optional transcript-tree / provenance metadata persisted on a message so subagent
 * sidechains form a resumable tree and each message records its origin (parity:
 * Claude's TranscriptMessage `parentUuid`/`logicalParentUuid`/`isSidechain`/`agentId`/
 * `promptId` plus swarm `teamName`/`agentName` identity used to detect a resumed teammate).
 */
export interface TranscriptMeta {
  /** Stable id of this message. */
  uuid?: string;
  /** Parent message id (threading within a session). */
  parentUuid?: string;
  /** Logical parent, preserved across session breaks/resumes. */
  logicalParentUuid?: string;
  /** True when this message belongs to a subagent sidechain. */
  isSidechain?: boolean;
  /** Owning agent id, for per-agent transcript records. */
  agentId?: string;
  /** Correlates to the OTel `prompt.id` for this turn. */
  promptId?: string;
  /** Swarm team name — present on the first message of a resumed teammate session. */
  teamName?: string;
  /** Swarm agent name — present on the first message of a resumed teammate session. */
  agentName?: string;
  /** Render this message ONLY in verbose/transcript mode while STILL sending it to
   *  the model — a third visibility state distinct from {@link UserMessage.isMeta}
   *  (which hides it from the model entirely). Consumers treat
   *  `isMeta || isVisibleInTranscriptOnly` as "synthetic" for display. Parity:
   *  Claude's `createUserMessage({ isVisibleInTranscriptOnly })` + `shouldShowUserMessage`. */
  isVisibleInTranscriptOnly?: boolean;
}

export interface UserMessage extends TranscriptMeta {
  role: 'user';
  content: string | Array<TextContent | ImageContent>;
  /** Engine-injected ephemeral control message (turn-local nudge). Excluded from
   *  the persisted session transcript so --resume is not polluted; still sent to
   *  the model during the live turn. */
  isMeta?: boolean;
  /** Provenance of this message, driving per-origin framing. Parity: `MessageOrigin`. */
  origin?: MessageOrigin;
  /** Permission mode active when this prompt was submitted, preserved per turn for
   *  audit/replay. Parity: Claude's per-message `permissionMode`. */
  permissionMode?: string;
  /** Ids of inline pasted-image placeholders (e.g. `[Image #1]`) in this message's
   *  text, correlating each placeholder with the image content block appended to the
   *  message. Set only when non-empty (else omitted). Parity: Claude's
   *  `processTextPrompt({ imagePasteIds })` user-message field. */
  imagePasteIds?: number[];
}

export interface AssistantMessage extends TranscriptMeta {
  role: 'assistant';
  content: string | null;
  tool_calls?: ToolCallContent[];
  /** Model that produced this assistant message (stamped per turn, incl. architect/
   *  fallback/provider switches), for the transcript model-attribution label.
   *  Parity: Claude's MessageModel. */
  model?: string;
}

export interface ToolMessage extends TranscriptMeta {
  role: 'tool';
  tool_call_id: string;
  content: string;
}

export interface SystemMessage extends TranscriptMeta {
  role: 'system';
  content: string;
  /** Engine-injected ephemeral control message (turn-local nudge). Excluded from
   *  the persisted session transcript so --resume is not polluted; still sent to
   *  the model during the live turn. */
  isMeta?: boolean;
  /** Severity level: 'info' messages are suppressed unless verbose; 'warning'/'error'
   *  change the styling (color/dot). Parity: Claude's SystemMessage `level`. */
  level?: 'info' | 'warning' | 'error';
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

/** One hunk of a structured diff (parity: Claude's FileEdit `structuredPatch`),
 *  so a frontend can render a file mutation per-hunk instead of parsing text. */
export interface DiffHunk {
  oldStart: number;
  oldLines: number;
  newStart: number;
  newLines: number;
  lines: string[];
}

export interface ToolResult {
  output: string;
  isError?: boolean;
  metadata?: Record<string, unknown>;
  /** Structured per-hunk diff for a file-mutating result, carried alongside the
   *  text `output` for programmatic rendering. Parity: Claude's `structuredPatch`. */
  structuredPatch?: DiffHunk[];
}

/** Per-host outbound-network allow/deny lists for the sandbox. `deniedHosts`
 *  takes precedence over `allowedHosts`. Proxy ports route the container's
 *  outbound traffic through a host proxy (parity: Claude's SandboxNetworkConfig). */
export interface SandboxNetworkConfig {
  /** Hostnames/domains permitted for outbound network (allowlist). */
  allowedHosts?: string[];
  /** Hostnames/domains blocked for outbound network (denylist; wins over allowedHosts). */
  deniedHosts?: string[];
  /** Unix-socket paths to allow (macOS only; ignored where the backend can't filter by path). */
  allowUnixSockets?: string[];
  /** Allow ALL Unix sockets (disables socket blocking entirely). */
  allowAllUnixSockets?: boolean;
  /** Allow the sandboxed process to bind local ports. */
  allowLocalBinding?: boolean;
  /** Host port the sandbox routes outbound HTTP(S) through. 1..65535. */
  httpProxyPort?: number;
  /** Host port the sandbox routes outbound traffic through (SOCKS). 1..65535. */
  socksProxyPort?: number;
}

/** Granular filesystem carve-outs layered on top of the project-only writable
 *  mount (parity: Claude's SandboxFilesystemConfig). Deny wins over allow, and
 *  `allowRead` re-opens specific paths inside a `denyRead` region. */
export interface SandboxFilesystemConfig {
  /** Extra absolute paths writable inside the sandbox (merged with Edit allow rules). */
  allowWrite?: string[];
  /** Extra absolute paths denied for writing (merged with Edit deny rules; wins over allowWrite). */
  denyWrite?: string[];
  /** Extra absolute paths denied for reading (merged with Read deny rules). */
  denyRead?: string[];
  /** Paths re-allowed for reading within a denyRead region (precedence over denyRead). */
  allowRead?: string[];
}

/** Policy controlling whether/how bash commands run inside an isolation backend.
 *  `mode: 'container'` forces every bash command through a container whose only
 *  writable mount is the project dir; the host filesystem is unreachable. When
 *  the backend is unavailable the bash tool fails closed (refuses to run on the
 *  host) when `failIfUnavailable` is set, otherwise it warns and runs on the
 *  host. `mode: 'off'` (or undefined) runs on the host as before. */
export interface SandboxPolicy {
  mode: 'off' | 'container';
  /** Container image (must contain the project toolchain). Default: node:26. */
  image?: string;
  /** The single writable mount (absolute). Defaults to the engine launch cwd. */
  projectDir?: string;
  /** Allow network inside the container (needed for npm/git). Default: true. */
  allowNetwork?: boolean;
  /** Per-host outbound network allow/deny lists + proxy ports. */
  network?: SandboxNetworkConfig;
  /** Granular filesystem read/write carve-outs. */
  filesystem?: SandboxFilesystemConfig;
  /** Auto-approve the bash tool (skip the permission prompt) when a command runs
   *  sandboxed — the isolation is the safety boundary. Default: true. */
  autoAllowBashIfSandboxed?: boolean;
  /** Hard gate on the `dangerouslyDisableSandbox` escape hatch. When false, the
   *  parameter is ignored entirely and EVERY command must run sandboxed. Default: true. */
  allowUnsandboxedCommands?: boolean;
  /** Fail closed at startup if the sandbox is enabled but cannot start (missing
   *  backend / unsupported platform). When false (default) a warning is shown and
   *  commands run unsandboxed. Managed-deployment hard gate. */
  failIfUnavailable?: boolean;
  /** Per-command map of sandbox-violation strings to suppress (command → allowlisted violations). */
  ignoreViolations?: Record<string, string[]>;
  /** Restrict sandboxing (and auto-allow) to specific platforms (e.g. ['linux']). */
  enabledPlatforms?: string[];
}

export interface ToolContext {
  cwd: string;
  /** Absolute paths/dirs OUTSIDE the launch cwd that the permission layer has already
   *  granted read access to for this call — via `/add-dir`, `--context-file`/`--image`,
   *  an approved out-of-workspace prompt, or auto-allow/yolo. The core path guards treat
   *  these as in-bounds in ADDITION to cwd, so a skill loaded from an external folder
   *  (e.g. ~/.ami/plugins/<name>) can read its own bundled resources. Populated by the
   *  tool executor from PermissionManager.getAllowedReadRoots() right AFTER the per-call
   *  permission check (which is what asks/denies/auto-allows and registers the root), so
   *  it reflects the just-granted dir. Absent/empty → confined to cwd only (the default
   *  for bare engines and tests with no permission manager). */
  allowedReadRoots?: string[];
  /** Env vars exported into the `bash` tool's subprocess for the active skill — notably
   *  CLAUDE_SKILL_DIR (and CLAUDE_PLUGIN_ROOT for plugin-packaged skills), so imported
   *  Claude-Code skills that shell out via `$CLAUDE_SKILL_DIR/scripts/...` work (issue #22). */
  skillEnv?: Record<string, string>;
  abortSignal: AbortSignal;
  onProgress?: (data: string) => void;
  /** Tracks file paths that have been read during this session.
   *  Used by file_edit to enforce read-before-write. */
  filesRead?: Set<string>;
  /** Subset of {@link filesRead} read only as a PARTIAL view (offset/limit or a
   *  size-truncated read). A partial read does NOT satisfy the read-before-OVERWRITE
   *  guard — overwriting the whole file after seeing only part silently clobbers the
   *  unseen remainder. Parity: Claude's `readTimestamp.isPartialView`. */
  filesReadPartial?: Set<string>;
  /** Session id for session/agent-scoped tool state (e.g. todo_write store, T2).
   *  Populated by the engine; absent in bare/test contexts (falls back to global). */
  sessionId?: string;
  /** Sub-agent id, when the tool runs inside a forked sub-engine. Combined with
   *  sessionId to key per-agent tool state (T2). */
  agentId?: string;
  /** Operator-configured command prefixes exempt from the bash sandbox (M11).
   *  Threaded to shouldUseSandbox; undefined → the hardcoded defaults only. */
  sandboxExcludedCommands?: string[];
  processManager?: import('./process-manager').ProcessManager;
  _providerConfig?: ProviderConfig;
  _permissionPromptHandler?: import('./permissions').PermissionPromptHandler;
  _allowLocalhostForTesting?: boolean;
  _engineFactory?: (config: EngineConfig) => { submit(prompt: string): AsyncIterable<EngineEvent>; shutdown?(): void };
  _skillManager?: import('./skills').SkillManagerLike;
  _hookManager?: import('./hooks').HookManager;
  _mcpManager?: any;
  _parentMessages?: Message[];
  _engineAddSystemReminder?: (msg: string) => void;
  _engineSetActiveSkill?: (name: string | undefined) => void;
  /** Signal the engine to load a deferred tool into the active set (tool_search select). */
  _loadDeferredTool?: (name: string) => void;
  /** Relay a child (sub-engine) event up to the parent engine's event stream so
   *  foreground subagents are visible live. Injected by the tool executor (which
   *  drains it onto the parent generator); absent → no relay (back-compat). */
  _emitChild?: (event: EngineEvent) => void;
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
  /** Bash isolation policy. When mode==='container', every bash command runs in
   *  a container (project-only writable); fail-closed if the backend is absent. */
  sandbox?: SandboxPolicy;
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

/** Result of a tool's structured pre-execution {@link ToolDefinition.validateInput}
 *  (parity: Claude's `ValidationResult`): either success, or a machine-readable
 *  `errorCode` plus a model-facing `message`, produced BEFORE the permission check
 *  and before the tool body runs. */
export type ToolValidationResult =
  | { result: true }
  | { result: false; message: string; errorCode: number };

/** Fine-grained read/search/list classification of a single tool invocation
 *  (parity: Claude's `isSearchOrReadCommand(input)`), used to drive concise,
 *  action-specific transcript summaries. */
export interface ToolAccessClassification {
  isSearch: boolean;
  isRead: boolean;
  isList: boolean;
}

export interface ToolDefinition {
  name: string;
  description: string;
  inputSchema: ToolInputSchema;
  isReadOnly: boolean;
  isConcurrencySafe?: boolean;
  /** True for tools originating from MCP servers — used for auto-deferral when schema tokens exceed the context budget. */
  isMcp?: boolean;
  /** Extra capability keywords (3-10 words) weighted by `tool_search`, using terms
   *  NOT already in the name/description (e.g. 'jupyter' for notebook_edit) so a
   *  deferred tool is findable by synonym. Parity: Claude's `Tool.searchHint`. */
  searchHint?: string;
  /** When true, keep this (native) tool OUT of the default advertised set and surface
   *  it on demand via `tool_search` — the per-tool opt-in equivalent of MCP schema-size
   *  auto-deferral. Parity: Claude's `Tool.shouldDefer`. */
  shouldDefer?: boolean;
  /** Input-aware irreversible-operation predicate (delete / overwrite / send). Absent
   *  or `false` = not destructive. Lets the permission layer add an extra guard for ANY
   *  tool, not just bash. Parity: Claude's `Tool.isDestructive(input)`. */
  isDestructive?: (input: Record<string, unknown>) => boolean;
  /** Runtime availability gate — when it returns false the tool is dropped from the
   *  advertised set (feature flag / env condition). Parity: Claude's `Tool.isEnabled()`. */
  isEnabled?: (context?: ToolContext) => boolean;
  /** Per-tool cap (chars) on result size before spill-to-disk, overriding the
   *  engine-global default; `Infinity` = never spill. Parity: `maxResultSizeChars`. */
  maxResultSizeChars?: number;
  /** Alternate names this tool also answers to, for legacy/compat dispatch
   *  (e.g. `KillShell` → task_stop). Parity: Claude's tool `aliases`. */
  aliases?: string[];
  /** Force the provider to constrain generated arguments to exactly match
   *  {@link inputSchema} (OpenAI/Anthropic strict/structured tool mode). Parity: strict. */
  strict?: boolean;
  /** JSON Schema describing the tool's structured result — used to validate a
   *  persisted/resumed tool result before rendering it. Parity: Claude's `Tool.outputSchema`. */
  outputSchema?: ToolInputSchema;
  /** What happens to an in-flight call when the user submits a new message:
   *  `'cancel'` aborts and discards the result; `'block'` (default) runs to completion
   *  while the message waits. Parity: Claude's `Tool.interruptBehavior()`. */
  interruptBehavior?: 'cancel' | 'block';
  /** Structured pre-execution validation run BEFORE the permission check; returns a
   *  machine-readable {@link ToolValidationResult} so the model gets a clean, schema-derived
   *  correction without entering the tool body. Parity: Claude's `Tool.validateInput`. */
  validateInput?: (input: Record<string, unknown>, context: ToolContext) => Promise<ToolValidationResult>;
  /** Compact human-readable activity phrase for progress UIs (e.g. "Reading src/foo.ts")
   *  computed from the input. Parity: Claude's `Tool.getActivityDescription(input)`. */
  getActivityDescription?: (input: Record<string, unknown>) => string;
  /** Fine-grained per-invocation read/search/list classification driving transcript
   *  summaries. Parity: Claude's `Tool.isSearchOrReadCommand(input)`. */
  isSearchOrReadCommand?: (input: Record<string, unknown>) => ToolAccessClassification;
  /** Compact representation of this tool use fed to an auto-mode security classifier
   *  (`''` = skip). Parity: Claude's `Tool.toAutoClassifierInput(input)`. */
  toAutoClassifierInput?: (input: Record<string, unknown>) => string;
  /** Long-form proactive-use / when-to-use (and when-NOT-to-use) guidance carried
   *  alongside the terse `description` and surfaced to the model. Parity: TodoWrite prompt. */
  prompt?: string;
  /** Tool-authored concise transcript label for a call, preferred by renderers over the
   *  generic field-sniffer/JSON fallback. Parity: Claude's `renderToolUseMessage`. */
  renderUseMessage?: (input: Record<string, unknown>) => string;
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

/** Per-request API service tier the model call ran under (parity: Claude's usage
 *  `service_tier`, default 'standard'). A throughput/latency class reflected per
 *  request for cost/latency attribution. */
export type ServiceTier = 'standard' | 'priority' | 'batch';

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
  /** API service tier the request(s) ran under (parity: Claude usage `service_tier`,
   *  default 'standard'). Carried for cost/latency attribution. */
  serviceTier?: ServiceTier;
  /** Count of server-side web-search requests folded into this usage record
   *  (parity: Claude `server_tool_use.web_search_requests`). */
  webSearchRequests?: number;
  /** Count of server-side web-fetch requests folded into this usage record
   *  (parity: Claude `server_tool_use.web_fetch_requests`). */
  webFetchRequests?: number;
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

export const ENGINE_EVENT_TYPES = [
  'text_delta', 'thinking_delta', 'tool_call_generating', 'tool_use_start',
  'tool_use_progress', 'tool_use_result', 'turn_complete', 'usage_update',
  'error', 'retry_attempt', 'error_classified', 'provider_changed',
  'checkpoint_created', 'analytics_summary', 'superinference_state',
  'suggest_file_save', 'user_question', 'session_title', 'memory_warning',
  'plan_mode_changed', 'task_updated', 'skill_activated',
  'subagent_start', 'subagent_progress', 'subagent_result', 'todo_update', 'done',
] as const;

/** A single todo item as rendered to the user (T1/T5). `id` is optional
 *  (Claude-Code parity); `activeForm` is the present-continuous label shown while
 *  the item is in progress. */
export interface TodoItemView {
  id?: string;
  content: string;
  status: 'pending' | 'in_progress' | 'completed' | 'cancelled';
  activeForm?: string;
}

/**
 * One selectable option in an AskUserQuestion prompt (parity: Claude's
 * `QuestionOption`). Single source of truth for the option shape, shared by the
 * `user_question` {@link EngineEvent} and the {@link EngineConfig.onUserQuestion}
 * callback so the carried fields can never drift between emit and handler.
 */
export interface UserQuestionOption {
  label: string;
  description: string;
  /** Markdown preview rendered in a bordered box when this option is focused
   *  (side-by-side preview panel). Parity: Claude's `option.preview`. */
  preview?: string;
  /** When true the option is shown but not selectable (dimmed; Enter/number/space
   *  refuse to act on it). Parity: Claude's `option.disabled`. */
  disabled?: boolean;
}

/** Three-state terminal outcome of a (sub)agent/task, distinguishing a clean finish
 *  from an error from a user/abort stop (parity: Claude's task_notification `status`:
 *  'completed' | 'failed' | 'stopped'). Richer than a bare success boolean. */
export type TaskTerminalStatus = 'completed' | 'failed' | 'stopped';

/** Live machine-readable usage snapshot for a running (sub)agent, carried on progress
 *  and terminal events (parity: Claude's task_progress usage `{ total_tokens, tool_uses,
 *  duration_ms }`). */
export interface TaskUsageSnapshot {
  /** Total tokens consumed by the (sub)agent so far. */
  totalTokens: number;
  /** Number of tool calls the (sub)agent has made so far. */
  toolUses: number;
  /** Wall-clock milliseconds elapsed since the (sub)agent started. */
  durationMs: number;
}

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
  | { type: 'user_question'; toolCallId: string; question: string; options: UserQuestionOption[]; allowFreeText: boolean; header?: string; multiSelect?: boolean; answers?: string[]; annotations?: Record<string, unknown>; defaultIndex?: number }
  | { type: 'session_title'; title: string }
  | { type: 'memory_warning'; rssBytes: number; thresholdBytes: number; recoverySteps: string[] }
  | { type: 'plan_mode_changed'; enabled: boolean }
  | { type: 'task_updated'; taskId: number; status: string; subject: string }
  | { type: 'skill_activated'; skill: string; description: string; hasMcpServers: boolean; source: 'model' | 'user' }
  | { type: 'subagent_start'; nodeId: string; depth: number; agentName?: string; model?: string; prompt: string }
  | { type: 'subagent_progress'; nodeId: string; depth: number; activity: 'text' | 'thinking' | 'tool'; detail: string; usage?: TaskUsageSnapshot; lastToolName?: string }
  | { type: 'subagent_result'; nodeId: string; depth: number; success: boolean; summary: string; tokens: number; status?: TaskTerminalStatus; toolUses?: number; durationMs?: number }
  | { type: 'todo_update'; todos: TodoItemView[] }
  | { type: 'done'; totalTurns: number; exitReason: ExitReason };

/**
 * Natural-language auto-mode (YOLO) rule configuration (parity: Claude's
 * `settings.autoMode` / `AutoModeConfig`): plain-English allow / soft-deny rules
 * plus an environment description that are substituted into the security-classifier
 * prompt, an optional dedicated classifier model, and a two-stage strategy selector.
 */
export interface AutoModeConfig {
  /** Plain-English descriptions of autonomous actions to permit. */
  allow?: string[];
  /** Plain-English descriptions of actions to soft-block (ask instead of run). */
  soft_deny?: string[];
  /** Plain-English description of the runtime environment, for the classifier. */
  environment?: string[];
  /** Dedicated model for the security classifier (falls back to the main-loop model). */
  model?: string;
  /** Classifier strategy: tool_use (false/undefined), both stages (true), stage-1-only
   *  ('fast'), or stage-2-only ('thinking'). Parity: `twoStageClassifier`. */
  twoStageClassifier?: boolean | 'fast' | 'thinking';
}

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
  /** MCP server names whose tools bypass the MCP schema-size deferral threshold. */
  alwaysLoadMcpServers?: string[];
  abortController?: AbortController;
  /**
   * Tool names to exclude from this engine's toolset. Used for hook-spawned
   * sub-engines (prompt/agent hooks) so a verification hook cannot recursively
   * spawn subagents (`task`) or enter the interactive `plan_mode`
   * (Claude Code's ALL_AGENT_DISALLOWED_TOOLS).
   */
  disallowedTools?: string[];
  onPermissionRequest?: (toolName: string, input: Record<string, unknown>) => Promise<boolean>;
  /** Centralized permission prompt handler. Both CLI and VSCode implement this interface. */
  permissionPromptHandler?: import('./permissions').PermissionPromptHandler;
  /**
   * Resolve an MCP server elicitation request (2025-06-18). Interactive
   * frontends prompt the user; absent this handler the engine declines, so a
   * detached run never blocks. Receives the server name and the request.
   */
  onMcpElicitation?: (serverName: string, request: import('./mcp').McpElicitationRequest) => Promise<import('./mcp').McpElicitationResult>;
  /**
   * Approve/deny an MCP server sampling request (run an LLM turn on the server's
   * behalf) before the engine executes the model call. Return false to refuse.
   * Absent this handler the engine refuses sampling (safe default), per the
   * spec's human-in-the-loop requirement.
   */
  onMcpSamplingApproval?: (serverName: string, request: import('./mcp').McpSamplingRequest) => Promise<boolean>;
  /**
   * Interactive OAuth 2.1 consent leg for HTTP MCP servers (open browser +
   * capture the redirect code). Interactive frontends provide it; detached runs
   * omit it so the manager's headless default refuses with guidance.
   */
  onMcpOAuthAuthorize?: import('./mcp').AuthorizeFn;
  /**
   * MCP servers to block, by name (case-insensitive) or normalized upstream URL.
   * Default-open: an empty/absent list blocks nothing. A URL entry blocks every
   * configured server pointing at that upstream, regardless of its name.
   */
  deniedMcpServers?: string[];
  /**
   * M14 — opt-in gate for PROJECT-tier MCP servers (configs under cwd:
   * `.ami/mcp.json` + `.mcp.json`). Default OFF/undefined: project MCP
   * servers auto-connect exactly as before. When true, a NEW (not-yet-approved)
   * project server is registered PENDING instead of auto-connecting; the user
   * approves it via `/mcp approve`, and the approval persists to
   * `<cwd>/.ami/approved-mcp.json`. User/global-tier configs are never
   * gated.
   */
  mcpRequireApproval?: boolean;
  /** Maximum token budget for context management. Defaults to 100 000. */
  tokenBudget?: number;
  /** Resume a specific session. When set, the engine loads its messages on init. */
  sessionId?: string;
  /** Override the default session directory (~/.ami/projects/<cwd-slug>/sessions/). */
  sessionDir?: string;
  /** Permission mode: 'ask' (default), 'auto-allow', or 'deny-all'. */
  permissionMode?: string;
  /** Bash isolation policy forwarded to every ToolContext. When mode==='container',
   *  all bash commands run inside a container (project-only writable), fail-closed.
   *  Undefined = host execution (the default for programmatic/test use; the CLI sets
   *  container mode for real runs). */
  sandbox?: SandboxPolicy;
  /** Operator-configured command prefixes exempt from the bash sandbox (M11),
   *  forwarded to every ToolContext. Additive to the built-in defaults. */
  sandboxExcludedCommands?: string[];
  /** Permission rules evaluated in order; first match wins. */
  permissionRules?: Array<{ tool: string; pattern?: string; action: 'allow' | 'deny' | 'ask' }>;
  /** Extra absolute paths/dirs the user has granted read access to OUTSIDE the launch
   *  cwd (e.g. dirs of `--context-file`/`--image` targets, or `--add-dir`). The tools are
   *  confined to the launch cwd; these roots widen that boundary to user-pointed resources. */
  allowedReadRoots?: string[];
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
  /** Admin/settings policy for HTTP hooks — URL allowlist + header env-var
   *  allowlist. Undefined leaves HTTP hooks unrestricted (SSRF guard + non-2xx
   *  handling always apply regardless). */
  httpHookPolicy?: import('./hooks').HttpHookPolicy;
  /** Stance skill name (--skill) — a skill pinned to govern the whole session/run:
   *  its body becomes the system-prompt overlay, plus its tool-scoping, bash
   *  auto-allow, MCP servers, and thinking level. Replaces the old persona. */
  skill?: string;
  /** Named output style overlay appended to the system prompt (e.g. 'concise'). */
  outputStyle?: string;
  /** Managed allowlist of selectable models; non-listed models are blocked. */
  enforceAvailableModels?: string[];
  /** Merged project-level configuration (from .ami/config.json). */
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
   *  the question and return the user's answer. Options carry optional per-option
   *  `preview`/`disabled` and an optional `defaultIndex` to preselect/focus a
   *  recommended option. */
  onUserQuestion?: (question: string, options: UserQuestionOption[], allowFreeText: boolean, multiSelect?: boolean, defaultIndex?: number) => Promise<string>;
  /** When true, the engine generates a session title via LLM after the first turn. */
  enableTitleGeneration?: boolean;
  /** When true, the engine is running in non-interactive (detached / --prompt) mode. */
  detachedMode?: boolean;
  /** Task classification for detached mode. 'bugfix' injects the full bug-fix
   *  overlay (mandatory tool calls, bug-fix loop, edit-before-complete). Omit or
   *  set to 'generic' for a lightweight overlay that lets the model answer
   *  directly when no tools are needed. */
  taskType?: 'generic' | 'bugfix';
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
  /** @internal Override provider-level max retries. Set for testing and by the
   *  unattended-retry mode (persistent retry through long outages). */
  _providerMaxRetries?: number;
  /** Unattended/persistent retry mode for long detached runs. When on, the
   *  provider keeps retrying transient failures through a long outage window
   *  (per-attempt delay capped, total window bounded) instead of giving up after
   *  the default ~5 attempts. Opt-in via SUPERINFERENCE_UNATTENDED_RETRY=1
   *  (CLAUDE_CODE_UNATTENDED_RETRY accepted for compatibility) or project config.
   *  Mode-agnostic: identical in interactive and detached runs. */
  unattendedRetry?: boolean;
  /** @internal Override provider-level base delay in ms (for testing). */
  _providerBaseDelayMs?: number;
  /** Maximum tool iterations before prompting to continue. Default 200. */
  maxToolIterations?: number;
  /** Safe mode: skip loading untrusted project-local config — no project hooks
   *  (.ami/hooks.json), no MCP server auto-connect, and no project
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

  // -------------------------------------------------------------------------
  // Parity config knobs (Claude Code settings.json / CLI flags). All optional;
  // each is consumed by its owning subsystem (prompt assembly, permissions,
  // memory, MCP, sandbox, UI). Added here as the single config surface so the
  // CLI resolver and `ami --introspect` thread them to the engine.
  // -------------------------------------------------------------------------
  /** Append text to the END of the assembled system prompt instead of replacing it
   *  (keeps the full default prompt). Parity: `--append-system-prompt`. */
  appendSystemPrompt?: string;
  /** Suppress writing the session transcript/state to disk for the whole run
   *  (ephemeral session). Parity: `sessionPersistenceDisabled` / `--no-save`. */
  sessionPersistenceDisabled?: boolean;
  /** Composer editing mode: modal 'vim' (Esc toggles normal/insert) or 'normal'
   *  (default). Parity: `editorMode`. */
  editorMode?: 'vim' | 'normal';
  /** Render full (untruncated) long API-error text + extra diagnostics. Parity: `verbose`. */
  verbose?: boolean;
  /** Separate, typically-stronger reviewer/advisor model that reviews the conversation
   *  mid-session. Parity: `advisorModel`. */
  advisorModel?: string;
  /** Natural-language auto-mode (YOLO) rule configuration. Parity: `settings.autoMode`. */
  autoMode?: AutoModeConfig;
  /** Default install/update channel. Parity: `autoUpdatesChannel`. */
  autoUpdatesChannel?: 'latest' | 'stable';
  /** Remembered responses for env-detected API keys (truncated), so a detected key is
   *  prompted once then trusted/rejected. Parity: `customApiKeyResponses`. */
  customApiKeyResponses?: { approved: string[]; rejected: string[] };
  /** Minutes-away threshold after which an idle-return prompt is shown. Parity: IdleReturnDialog. */
  idleReturnPromptMinutes?: number;
  /** Enable automatic session-memory extraction/consolidation (default true). Parity:
   *  `isAutoMemoryEnabled` / `autoDreamEnabled`. */
  autoMemoryEnabled?: boolean;
  /** Run session-memory extraction only every N eligible turns. Parity: extraction throttle. */
  sessionMemoryInterval?: number;
  /** Master switch for time-based idle microcompact (default off). Parity: timeBasedMCConfig.enabled. */
  idleMicrocompactEnabled?: boolean;
  /** Idle-gap threshold (minutes) before microcompact fires, tuned against the server
   *  prompt-cache TTL. Parity: `gapThresholdMinutes`. */
  idleMicrocompactThresholdMinutes?: number;
  /** Enable file checkpointing/history (default true); false disables snapshots.
   *  Parity: `fileCheckpointingEnabled`. */
  fileCheckpointingEnabled?: boolean;
  /** Ignore all on-disk/discovered MCP configs and use ONLY explicitly-passed servers.
   *  Parity: `--strict-mcp-config`. */
  strictMcpConfig?: boolean;
  /** Persisted per-server disable list (opt-out without removing). Parity: `disabledMcpServers`. */
  disabledMcpServers?: string[];
  /** Persisted per-server enable list for default-disabled servers. Parity: `enabledMcpServers`. */
  enabledMcpServers?: string[];
  /** Cap on output tokens for this run/fork (sizes the request `max_tokens`). Parity:
   *  `maxOutputTokens`. */
  maxOutputTokens?: number;
  /** Show rotating capability tips (default true). Parity: `spinnerTipsEnabled`. */
  spinnerTipsEnabled?: boolean;
  /** Disable fenced-code syntax highlighting while keeping UI color. Parity:
   *  `syntaxHighlightingDisabled`. */
  syntaxHighlightingDisabled?: boolean;
  /** Reduce/disable animations for accessibility (persistent, vs env-only). Parity:
   *  `prefersReducedMotion`. */
  prefersReducedMotion?: boolean;
  /** Enable AI-generated inline prompt suggestions. Parity: `promptSuggestionEnabled`. */
  promptSuggestionEnabled?: boolean;
  /** Retention window (days) for stale worktrees/files cleanup. Parity: `cleanupPeriodDays`. */
  cleanupPeriodDays?: number;
  /** Enable the LLM-generated permission explainer (default true). Parity:
   *  `permissionExplainerEnabled`. */
  permissionExplainerEnabled?: boolean;
  /** Directory (relative to project root) where plan .md files are written. Parity:
   *  `plansDirectory`. */
  plansDirectory?: string;
  /** Capture a heap dump when the RSS memory-warning threshold is crossed (not just a
   *  warning). Parity: heapDumpService `auto-1.5GB`. */
  heapDumpOnMemoryWarning?: boolean;
  /** Apply /model, /thinking, /fast immediately to an in-flight turn rather than
   *  deferring to turn end. Parity: `shouldInferenceConfigCommandBeImmediate`. */
  immediateInferenceConfig?: boolean;
  /** Backend that runs teammates/subagents; 'auto' resolves against the live
   *  environment. Parity: `teammateMode`. */
  teammateMode?: 'auto' | 'tmux' | 'in-process';
  /** Persisted default model for spawned teammates/subagents. Parity: `teammateDefaultModel`. */
  subagentDefaultModel?: string;
  /** Map of canonical model id → provider-specific model string (e.g. a Bedrock
   *  inference-profile ARN), layered on top of the resolved model. Parity: `modelOverrides`. */
  modelOverrides?: Record<string, string>;
  /** Terminal notification channel for turn-completion alerts. Parity: NOTIFICATION_CHANNELS. */
  notificationChannel?: NotificationChannel;
  /** Plugin ids never suggested for LSP recommendations. Parity: `lspRecommendationNeverPlugins`. */
  lspRecommendationNeverPlugins?: string[];
  /** Hard off-switch for LSP-plugin recommendations. Parity: `lspRecommendationDisabled`. */
  lspRecommendationDisabled?: boolean;
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
  /** Text appended to the END of the active system prompt (default / override / agent)
   *  for this turn, preserving the full prompt rather than replacing it. Parity:
   *  Claude's per-call `appendSystemPrompt`. */
  appendSystemPrompt?: string;
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

/** Canonical bounded-integer/number knob validator. Pushes a warning and returns
 *  `false` (invalid) when `value` is out of the hard [min,max] range or (when
 *  `integer`) non-integer; pushes a soft advisory (still valid) past `softMax`.
 *  Single source for every numeric EngineConfig/sandbox knob check so the range
 *  logic is never re-implemented per site (dedup). */
function validateNumericKnob(
  value: number,
  name: string,
  opts: { min: number; max?: number; softMax?: number; integer?: boolean },
  warnings: string[],
): boolean {
  if (opts.integer && !Number.isInteger(value)) {
    warnings.push(`${name} must be an integer`);
    return false;
  }
  if (!Number.isFinite(value) || value < opts.min) {
    warnings.push(`${name} must be >= ${opts.min}`);
    return false;
  }
  if (opts.max !== undefined && value > opts.max) {
    warnings.push(`${name} must be <= ${opts.max}`);
    return false;
  }
  if (opts.softMax !== undefined && value > opts.softMax) {
    warnings.push(`${name} exceeds ${opts.softMax} — likely misconfigured`);
  }
  return true;
}

/** True when `n` is a usable TCP port (integer in 1..65535). */
function isValidPort(n: number): boolean {
  return Number.isInteger(n) && n >= 1 && n <= 65535;
}

/** Validate a {@link SandboxPolicy}: mode enum, proxy ports, host/path list
 *  shapes, and cross-field sanity (fail-closed only meaningful when enabled).
 *  Pushes warnings and returns `false` on a hard error. Reached by
 *  {@link validateEngineConfig}. */
function validateSandboxPolicy(sandbox: SandboxPolicy, warnings: string[]): boolean {
  let valid = true;
  if (sandbox.mode !== 'off' && sandbox.mode !== 'container') {
    warnings.push(`sandbox.mode must be 'off' or 'container'`);
    valid = false;
  }
  const net = sandbox.network;
  if (net) {
    for (const port of [['httpProxyPort', net.httpProxyPort], ['socksProxyPort', net.socksProxyPort]] as const) {
      if (port[1] !== undefined && !isValidPort(port[1])) {
        warnings.push(`sandbox.network.${port[0]} must be a port in 1-65535`);
        valid = false;
      }
    }
    for (const [field, list] of [['allowedHosts', net.allowedHosts], ['deniedHosts', net.deniedHosts]] as const) {
      if (list && list.some((h) => typeof h !== 'string' || h.trim() === '')) {
        warnings.push(`sandbox.network.${field} entries must be non-empty strings`);
        valid = false;
      }
    }
  }
  const fs = sandbox.filesystem;
  if (fs) {
    for (const [field, list] of [['allowWrite', fs.allowWrite], ['denyWrite', fs.denyWrite], ['denyRead', fs.denyRead], ['allowRead', fs.allowRead]] as const) {
      if (list && list.some((p) => typeof p !== 'string' || p.trim() === '')) {
        warnings.push(`sandbox.filesystem.${field} entries must be non-empty paths`);
        valid = false;
      }
    }
  }
  if (sandbox.failIfUnavailable && sandbox.mode === 'off') {
    warnings.push('sandbox.failIfUnavailable has no effect when sandbox.mode is off');
  }
  return valid;
}

export function validateEngineConfig(config: Partial<EngineConfig>): { valid: boolean; warnings: string[] } {
  const warnings: string[] = [];
  let valid = true;

  if (config.maxTurns !== undefined) {
    if (!validateNumericKnob(config.maxTurns, 'maxTurns', { min: 1, softMax: 10000, integer: true }, warnings)) valid = false;
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
  if (config.sandbox !== undefined) {
    if (!validateSandboxPolicy(config.sandbox, warnings)) valid = false;
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
  if (out.sandbox !== undefined) out.sandbox = sanitizeSandboxPolicy(out.sandbox);
  return out;
}

/** Drop invalid proxy ports and normalize host/path lists (trim, drop empties,
 *  dedupe). Returns a new policy; never mutates the input. */
function sanitizeSandboxPolicy(sandbox: SandboxPolicy): SandboxPolicy {
  const cleanList = (list?: string[]): string[] | undefined => {
    if (!list) return undefined;
    const cleaned = [...new Set(list.map((s) => (typeof s === 'string' ? s.trim() : '')).filter((s) => s !== ''))];
    return cleaned.length ? cleaned : undefined;
  };
  const out: SandboxPolicy = { ...sandbox };
  if (sandbox.network) {
    const net = { ...sandbox.network };
    if (net.httpProxyPort !== undefined && !isValidPort(net.httpProxyPort)) delete net.httpProxyPort;
    if (net.socksProxyPort !== undefined && !isValidPort(net.socksProxyPort)) delete net.socksProxyPort;
    net.allowedHosts = cleanList(net.allowedHosts);
    net.deniedHosts = cleanList(net.deniedHosts);
    net.allowUnixSockets = cleanList(net.allowUnixSockets);
    out.network = net;
  }
  if (sandbox.filesystem) {
    const fsc = { ...sandbox.filesystem };
    fsc.allowWrite = cleanList(fsc.allowWrite);
    fsc.denyWrite = cleanList(fsc.denyWrite);
    fsc.denyRead = cleanList(fsc.denyRead);
    fsc.allowRead = cleanList(fsc.allowRead);
    out.filesystem = fsc;
  }
  if (sandbox.enabledPlatforms) out.enabledPlatforms = cleanList(sandbox.enabledPlatforms);
  return out;
}
