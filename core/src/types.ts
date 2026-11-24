import 'node:path';
import type { SuperInferenceConfig } from './superinference/types';
import type { ToolCapabilityCeiling } from './tools/scoped-permissions';
import type { RateLimitTrackerOptions } from './rate-limiter';

/** Policy controlling whether/how bash commands run inside an isolation backend.
 *  `mode: 'container'` forces every bash command through a container whose only
 *  writable mount is the project dir; the host filesystem is unreachable. When
 *  the backend is unavailable the bash tool fails closed (refuses to run on the
 *  host) when `failIfUnavailable` is set, otherwise it warns and runs on the
 *  host. `mode: 'off'` (or undefined) runs on the host as before. */
export interface SandboxPolicy {
  mode: 'off' | 'container';
  /** Global off-switch for the lightweight host (unshare) auto-sandbox. When set
   *  to exactly `false`, the non-container branch NEVER wraps a command with the
   *  unshare backend even when a trigger pattern matches — the config-level
   *  analogue of the {@link import('./tools/tool-gates').DISABLE_HOST_SANDBOX_ENV}
   *  env knob. Default (unset/true) keeps the auto-wrap on. Parity: the reference's
   *  `SandboxManager.isSandboxingEnabled()`. Does NOT affect `mode: 'container'`,
   *  which is a separate, stronger boundary. */
  enabled?: boolean;
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
  /** Custom seccomp syscall-filter profile for the container backend. */
  seccomp?: SandboxSeccompConfig;
  /** OPT-IN: auto-approve the bash tool (skip the permission prompt) in the
   *  prompt-driven modes (ask/acceptEdits) for commands protected by the sandbox.
   *  Two arms, both honored by {@link import('./permissions').PermissionManager}
   *  only when set to exactly `true` (default unset/false keeps the normal bash
   *  prompt): in CONTAINER mode (`mode === 'container'`) EVERY command is
   *  auto-allowed because the container — not the prompt — is the safety boundary
   *  (the host filesystem is not mounted); in HOST (non-container) mode only the
   *  commands that WILL actually be wrapped by the host (unshare) auto-sandbox are
   *  auto-allowed (per {@link import('./tools/bash-sandbox').commandWillBeHostSandboxed}),
   *  while sandbox-incompatible commands fall through to a regular permission prompt —
   *  the reference's "auto-allow" sandbox mode (try sandboxed, else regular
   *  permissions). Parity: enterprise `sandbox.autoAllowBashIfSandboxed`. Hardline
   *  blocks, `dangerouslyDisableSandbox`, explicit deny rules, and (in host mode)
   *  sensitive-path reads/writes still prompt. */
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
  /** Whether a bash command that cannot be sandboxed may still run UNsandboxed.
   *  Default (unset/true) keeps the warn-and-run-on-host behavior; `false` makes
   *  the host sandbox a hard gate — a command that would otherwise degrade to the
   *  weaker ulimits-only path (no namespace isolation) is REFUSED instead. Mirrors
   *  the reference's `areUnsandboxedCommandsAllowed()` / `, fallback allowed` status.
   *  Security-weakening when true; surfaced by {@link import('./tools/bash-sandbox').describeSandboxSecurityTradeoffs}. */
  allowUnsandboxedFallback?: boolean;
  /** OPT-IN security relaxation for NESTED sandbox environments (inside an outer
   *  container / CI runner where the user-namespace UID map is already consumed):
   *  when true the host unshare backend drops the `--map-root-user` flag that fails
   *  in a nested user namespace, allowing a (weaker) sandbox to still start. Mirrors
   *  the reference's `enableWeakerNestedSandbox`. **Reduces security** — see
   *  {@link import('./tools/bash-sandbox').describeSandboxSecurityTradeoffs}. Default false. */
  enableWeakerNestedSandbox?: boolean;
  /** OPT-IN security relaxation of NETWORK isolation: when true the host unshare
   *  backend does NOT bind `/dev/null` over the default-denied Unix domain sockets,
   *  so a local trust/CA service (e.g. a MITM proxy's cert validator) remains
   *  reachable. Mirrors the reference's `enableWeakerNetworkIsolation`. **Reduces
   *  security** — opens a potential data-exfiltration vector; see
   *  {@link import('./tools/bash-sandbox').describeSandboxSecurityTradeoffs}. Default false. */
  enableWeakerNetworkIsolation?: boolean;
  /** Config-level override for the ripgrep binary used by the search tools (grep/glob),
   *  taking precedence over the `USE_BUILTIN_RIPGREP`/`AMI_RIPGREP_PATH` env waterfall.
   *  `command` is the binary to spawn (an absolute path or a bare name resolved via PATH);
   *  `args` are extra flags prepended to every ripgrep invocation; `argv0` is the explicit
   *  argv[0] handed to the child (for a multi-call / busybox-style binary that dispatches on
   *  argv[0]==='rg' while `command` is the wrapper path). Consumed by
   *  {@link import('./tools/ripgrep-config').resolveRipgrepConfig}. Parity: the reference's
   *  sandbox `ripgrep: { command, args, argv0 }` setting. */
  ripgrep?: { command: string; args?: string[]; argv0?: string };
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
  /** File-READ deny GLOBS from the permission layer (PermissionManager.getFileReadIgnorePatterns()).
   *  The search tools (grep, glob) EXCLUDE any file matching one of these so a denied file's
   *  matching lines — or even its path — never leak into results, in every output mode. Populated
   *  by the tool executor from the permission manager right after the per-call check (same seam as
   *  allowedReadRoots). Absent/empty → only the hardcoded VCS exclusions apply (bare engines/tests
   *  with no permission manager). Mirrors the reference's read-deny → `--glob !pattern` injection. */
  readIgnorePatterns?: string[];
  /** Env vars exported into the `bash` tool's subprocess for the active skill — notably
   *  CLAUDE_SKILL_DIR (and CLAUDE_PLUGIN_ROOT for plugin-packaged skills), so imported
   *  the reference skills that shell out via `$CLAUDE_SKILL_DIR/scripts/...` work (issue #22). */
  skillEnv?: Record<string, string>;
  /** Force the per-call permission check to run even when a hook / auto-approval path
   *  would otherwise bypass it (a forked/untrusted sub-agent context that must not
   *  inherit the parent's auto-grants). When `true`, callers MUST route through the
   *  normal permission flow regardless of any auto-approve verdict. Resolve the final
   *  decision with {@link shouldForcePermissionCheck}. Parity: the reference's forked-agent
   *  `requireCanUseTool` override (utils/forkedAgent.ts). */
  requireCanUseTool?: boolean;
  abortSignal: AbortSignal;
  /** Early-wake hook for the `sleep` tool: returns true when inbound user input is
   *  queued (e.g. a mid-turn message pushed via injectMidTurnUserMessage). When
   *  present, `sleep` polls it and resolves early so the agent reacts promptly
   *  instead of blocking the full duration. Parity: the reference's SleepTool polls
   *  `hasCommandsInQueue()` and wakes within ~1s of an inbound channel message. */
  hasQueuedInput?: () => boolean;
  onProgress?: (data: string) => void;
  /** Tracks file paths that have been read during this session.
   *  Used by file_edit to enforce read-before-write. */
  filesRead?: Set<string>;
  /** Subset of {@link filesRead} read only as a PARTIAL view (offset/limit or a
   *  size-truncated read). A partial read does NOT satisfy the read-before-OVERWRITE
   *  guard — overwriting the whole file after seeing only part silently clobbers the
   *  unseen remainder. Parity: the reference's `readTimestamp.isPartialView`. */
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
  /** The engine's persisted {@link EngineConfig.subagentDefaultModel} knob, forwarded so
   *  the task tool can apply it as the default model for a spawn that names none (tri-state:
   *  string = force that model, `null` = follow the leader, absent = provider-aware fallback). */
  _subagentDefaultModel?: string | null;
  /** Forwarded {@link EngineConfig.maxSkillProgressMessages}: caps how many of a
   *  forked skill's nested tool-use steps the skill tool surfaces live to the parent
   *  stream (the rest are summarized as "+K more tool use"). Undefined / negative /
   *  non-finite → surface every nested event (AMI's full live-stream default). */
  _maxSkillProgressMessages?: number;
  _permissionPromptHandler?: import('./permissions').PermissionPromptHandler;
  _allowLocalhostForTesting?: boolean;
  _engineFactory?: (config: EngineConfig) => { submit(prompt: string): AsyncIterable<EngineEvent>; shutdown?(): void };
  _skillManager?: import('./skills').SkillManagerLike;
  _hookManager?: import('./hooks').HookManager;
  _mcpManager?: any;
  _parentMessages?: Message[];
  /**
   * Parent engine's ALREADY-RENDERED system prompt (byte-exact), threaded into a
   * `fork`-mode child so the child reuses the parent's cached prompt prefix VERBATIM
   * instead of re-deriving the stance from scratch. Re-rendering a fork child's
   * stance can diverge from the parent (e.g. a cold→warm feature flag, a differently
   * ordered skill block) and bust the provider prompt cache at the first differing
   * byte — so a cache-identical fork pays full input-token cost. Parity with the reference
   * Code's FORK_AGENT threading `toolUseContext.renderedSystemPrompt` instead of
   * calling `getSystemPrompt()` again (forkSubagent.ts:L44-L71). Only consumed by the
   * fork branch of the task tool; other modes keep their own authoritative prompt. */
  _parentSystemPrompt?: string;
  /**
   * The ORDERED names of the parent engine's exact tool array, threaded into a
   * `fork`-mode child so it inherits the parent's EXACT tool pool (same tools, same
   * order) rather than a freshly-rebuilt general pool. Rebuilding the pool under a
   * child's permission mode can reorder tools or change a tool-def's serialization,
   * which busts the prompt cache at the first differing tool. Parity with the reference
   * Code's `useExactTools` / `tools:['*']` fork inheritance (forkSubagent.ts:L44-L71,
   * resumeAgent.ts:L183-L190). The fork branch selects from the live tool registry in
   * this order; the universal subagent denylist/depth gate still applies afterwards. */
  _parentToolNames?: string[];
  _engineAddSystemReminder?: (msg: string) => void;
  /**
   * Activate (or, with `undefined`, clear) the inline skill named `name`. The
   * optional `scopedAllowRules` are the scoped command auto-allow rules the skill
   * contributes from its frontmatter `allowed-tools` (e.g. `Bash(git:*)` →
   * `{tool:'bash', pattern:'git:*', action:'allow'}`), computed via the canonical
   * {@link scopedToolAllowRules}. The engine applies them to the SESSION permission
   * context for as long as the inline skill stays active and removes them when it is
   * deactivated or replaced — parity with the reference's security-review command,
   * which injects a command's allowed-tools into `alwaysAllowRules.command` for the
   * duration of that command. Decoupled from `isStanceSkill`, so a plain inline
   * skill (or a skill-backed slash command) can pre-approve its own scoped commands
   * without being promoted to a pinned stance.
   */
  _engineSetActiveSkill?: (
    name: string | undefined,
    scopedAllowRules?: Array<{ tool: string; pattern?: string; action: 'allow' }>,
  ) => void;
  /** Signal the engine to load a deferred tool into the active set (tool_search select). */
  _loadDeferredTool?: (name: string) => void;
  /** Relay a child (sub-engine) event up to the parent engine's event stream so
   *  foreground subagents are visible live. Injected by the tool executor (which
   *  drains it onto the parent generator); absent → no relay (back-compat). */
  _emitChild?: (event: EngineEvent) => void;
  /** Per-tool output budgets (e.g. inline bash stdout cap before spill).
   *  `fileReadMaxTokens`/`fileReadMaxSizeBytes` are per-call overrides for the
   *  Read tool's output-token budget and pre-read size gate; each falls through
   *  to its env override (AMI_FILE_READ_MAX_OUTPUT_TOKENS /
   *  AMI_FILE_READ_MAX_SIZE_BYTES) and then the hardcoded default, validated so a
   *  malformed value can never disable the cap (see tools/file-read-limits.ts). */
  outputBudget?: { bashMaxChars?: number; taskTailLines?: number; taskMaxChars?: number; fileReadMaxTokens?: number; fileReadMaxSizeBytes?: number };
  /** Parent engine's thinking config, inherited by subagents unless overridden. */
  _parentThinking?: ThinkingConfig;
  /** Optional sub-agent handoff safety classifier (parity: the reference's classifyHandoffIfNeeded).
   *  When provided, it reviews an autonomous (yolo/auto) sub-agent's tool calls before the
   *  result is handed back to the parent; omitted, the task tool uses its built-in
   *  dangerous-command heuristic. See tools/handoff-review.ts. */
  _handoffClassifier?: (
    actions: Array<{ tool: string; command?: string }>,
  ) => { flagged: boolean; reason?: string; unavailable?: boolean }
    | Promise<{ flagged: boolean; reason?: string; unavailable?: boolean }>;
  /** Parent engine's permission rules, forwarded to subagents so deny rules are inherited. */
  _parentPermissionRules?: Array<{ tool: string; pattern?: string; action: 'allow' | 'deny' | 'ask' }>;
  /** Parent engine's LIVE permission mode (the resolved stance, incl. yolo/propagation).
   *  Forwarded so the task tool can CLAMP a spawned subagent's permission mode: a custom
   *  (non-built-in) agent definition may tighten but never ESCALATE past the parent's
   *  stance. Parity: the reference's runAgent permission clamp + source-trust gate. */
  _parentPermissionMode?: string;
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
  /** Test-only dependency seam for the `advisor` tool's reviewer stream. Never set
   *  in production (the tool falls through to the real streamChatCompletion); typed
   *  `unknown` and cast at the advisor call site to avoid a types.ts -> provider
   *  import coupling. */
  _advisorStreamImpl?: unknown;
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
  /** True when the user EDITED this tool call's input at the permission prompt before
   *  approving it (the executed input differs from the model's original). Threaded by the
   *  permission layer so a tool can tailor its behavior/telemetry to a human-corrected call.
   *  Parity: the reference's toolUseContext `userModified` (services/tools/toolExecution.ts). */
  userModified?: boolean;
}

import type { ThinkingLevel } from './model-capabilities';
import type { Message } from './types-transcript-meta';
import type { SandboxFilesystemConfig, SandboxNetworkConfig, SandboxSeccompConfig, ToolDefinition } from './types-tool-definition';
import type { ProviderConfig, ThinkingConfig, UserQuestionOption } from './types-provider-config';
import type { AutoModeConfig, EngineEvent } from './types-engine-event';
export { CHAT_MODES, IMAGE_MEDIA_TYPES, INPUT_FORMATS, INSIGHT_CALLOUT_HEADER, NOTIFICATION_CHANNELS, OUTPUT_FORMATS, OUTPUT_STYLES, THEMES, THEME_LABELS, asAgentId, asSessionId, describeTheme, isSupportedImageMediaType, toAgentId } from './types-transcript-meta';
export type { AgentId, AssistantMessage, ChatMode, DiffHunk, DocumentContent, GitDiffSummary, ImageContent, ImageMediaType, InputFormat, JsonSchemaProperty, Message, MessageOrigin, NotificationChannel, OutputFormat, OutputStyle, SessionId, SystemMessage, SystemMessageKind, TextContent, Theme, ToolCallContent, ToolCallFunction, ToolInputSchema, ToolMessage, TranscriptMeta, UserMessage } from './types-transcript-meta';
export { TOOL_GROUP_KINDS } from './types-tool-definition';
export type { ChannelMessageMeta, SandboxFilesystemConfig, SandboxNetworkConfig, SandboxSeccompConfig, StreamingToolProgress, ToolAccessClassification, ToolDefinition, ToolGroupCounts, ToolGroupKind, ToolResult, ToolValidationResult } from './types-tool-definition';
export { ENGINE_EVENT_TYPES, EXIT_REASONS, SERVICE_TIERS, SESSION_OPERATING_MODES, STREAM_CHUNK_TYPES, TASK_TERMINAL_STATUS, mergeSkillUsageStat, reconcileSessionMode } from './types-provider-config';
export type { ExitReason, ProviderConfig, ServiceTier, SessionModeReconciliation, SessionOperatingMode, SkillUsageStat, StreamChunk, StreamChunkType, TaskTerminalStatus, TaskUsageSnapshot, ThinkingConfig, TodoItemView, UsageStats, UserQuestionAnnotation, UserQuestionOption } from './types-provider-config';
export { AGENT_TURN_STOP_REASONS, MAX_LSP_RECOMMENDATION_IGNORED_COUNT, MIN_SLOW_PROGRESS_REVEAL_SECONDS, RETRY_ERROR_KINDS, SUBAGENT_ACTIVITYS, isLspRecommendationDisabled, resolveSentAt, sanitizeToolInputSummary, selectModelFacingOutput, selectUserFacingOutput, shouldEnablePromptSuggestion, shouldRenderGroupedToolUse, shouldRevealSlowProgress } from './types-engine-event';
export type { AgentTurnStopReason, AutoModeConfig, EngineEvent, PromptSuggestionResolution, RetryErrorKind, SubagentActivity } from './types-engine-event';
export { __validateNumericKnobForTest, buildSubsystems, isSuggestionMessage, nextTaskBudgetRemaining, resolveToolOpenWorld, shouldForcePermissionCheck, validateEngineConfig, validateSummarizeMetadata } from './types-validate-engine-config';
export type { ContextFile, EngineSubmitOptions, ProviderSubsystem, SessionSubsystem, SummarizeMetadata, TaskBudget } from './types-validate-engine-config';
export { AGENT_LIFECYCLE_STATES, COMPACT_DIFF_PREVIEW_WIDTH, DEFAULT_NOTIFICATION_TIMEOUT_MS, LEGAL_TRANSITIONS, NOTIFICATION_PRIORITYS, SLASH_COMMAND_LIFECYCLE_EVENT, SLASH_COMMAND_LIFECYCLE_STATES, VERBOSE_DIFF_PREVIEW_WIDTH, buildSlashCommandLifecycleEvent, describeSymlinkEdit, isInferenceConfigImmediate, planNotificationQueue, resolveDiffPreviewWidth, resolveNotificationTimeout, sanitizeEngineConfig, shouldShowWorkerBadge } from './types-delivery-hold';
export type { AgentLifecycleState, DeliveryHold, InferenceConfigImmediacy, NotificationPriority, ReplNotification, SlashCommandLifecycleEvent, SlashCommandLifecycleState, SpawnCreator, SpawnLineage, SymlinkEditDisclosure, WorkerAttribution } from './types-delivery-hold';
export type { ThinkingLevel } from './model-capabilities';

export interface EngineConfig {
  provider: ProviderConfig;
  cwd: string;
  tools?: ToolDefinition[];
  systemPrompt?: string;
  maxTurns?: number;
  maxTurnsCeiling?: number;
  /** Max time (ms) to wait for MCP servers to connect before the first submit proceeds (default 3000). */
  mcpStartupBudgetMs?: number;
  /**
   * Auto-connect to a code-editor-launched MCP endpoint at startup (the reference-parity
   * `autoConnectIde`). One input to the auto-connect waterfall — a supported editor
   * terminal, an inherited `AMI_SSE_PORT`, or a truthy `AMI_AUTO_CONNECT_IDE` also
   * enable it; an explicit falsy `AMI_AUTO_CONNECT_IDE` is a hard opt-out. Only
   * fires when the editor actually exported its SSE port.
   */
  autoConnectIde?: boolean;
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
  /** Independent ABSOLUTE RSS ceiling (bytes) at which the session is treated as
   *  CRITICAL — escalating the memory_warning straight to its top tier regardless of
   *  the (possibly lower) {@link memoryWarningRssBytes} warning threshold. This is a
   *  second, absolute threshold rather than a value derived from the warning one, so
   *  an operator can keep an early soft warning while only declaring a hard-critical
   *  state at a true memory ceiling. Parity: the reference's useMemoryUsage two
   *  independent absolute thresholds (HIGH 1.5 GiB / CRITICAL 2.5 GiB). Default 2.5 GiB. */
  memoryCriticalRssBytes?: number;
  /** Injectable RSS reader for tests (defaults to process.memoryUsage().rss). */
  memoryUsageReader?: () => number;
  /**
   * Rate-limit tracker tuning, threaded to the {@link ProviderCoordinator}'s
   * {@link RateLimitTracker}: `warnThreshold` / `errorThreshold` utilization
   * fractions and the proactive `earlyWarningConfigs` ladder (an empty array
   * disables proactive "approaching limit" warnings). Undefined = provider
   * defaults ({@link DEFAULT_EARLY_WARNING_CONFIGS}).
   */
  rateLimit?: RateLimitTrackerOptions;
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
   * (the reference's ALL_AGENT_DISALLOWED_TOOLS).
   */
  disallowedTools?: string[];
  onPermissionRequest?: (toolName: string, input: Record<string, unknown>) => Promise<boolean>;
  /** Centralized permission prompt handler. Both CLI and VSCode implement this interface. */
  permissionPromptHandler?: import('./permissions').PermissionPromptHandler;
  /**
   * Name of a registered tool to DELEGATE every non-interactive ("ask") permission decision
   * to (parity: the reference's `--permission-prompt-tool <name>`). In a headless/detached run,
   * instead of auto-denying an `ask`, the engine invokes this tool with
   * `{ tool_name, input, tool_use_id }` and parses its JSON result into an allow/deny decision
   * (see core `parsePermissionPromptToolResult`). The named tool is removed from the advertised
   * toolset so the model cannot call it directly. Absent → the standard headless policy applies.
   */
  permissionPromptToolName?: string;
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
  /** Forked sub-agent id. When set (by the `task` tool for a sub-engine), the engine
   *  threads it into every tool's {@link ToolContext.agentId} so file-reading tools use
   *  an ISOLATED per-agent file cache (clone-on-fork / release-on-finish) rather than the
   *  directory-shared one. Absent for the top-level engine. */
  agentId?: string;
  /**
   * Seed the engine's conversation with these messages at construction (only when
   * no `sessionId` restore supplied any). Used by a `fork` sub-agent to inherit the
   * parent's REAL message history (user turns, assistant tool_use/thinking, tool
   * results) as a context-continuing clone instead of a lossy text summary, which
   * both preserves full structured context and maximizes prompt-cache prefix
   * sharing. Routed through the same validation/healing as a session restore so a
   * trailing/orphaned tool_use cannot produce an API-invalid transcript. Parity:
   * the reference's `buildForkedMessages` + `filterIncompleteToolCalls` (runAgent.ts).
   */
  initialMessages?: Message[];
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
  /**
   * Managed killswitch mirrored from the merged config's
   * `disableBypassPermissionsMode`. When true the engine arms
   * `PermissionManager.setDisableBypass(true)` so bypass/auto-allow modes stay
   * forbidden for the whole session (startup, Shift+Tab, `/permissions`, resume,
   * and live config reloads). Managed-tier only; see {@link ProjectConfig}.
   */
  disableBypassPermissionsMode?: boolean;
  /**
   * Managed hook kill-switch mirrored from the merged config's `disableAllHooks`.
   * When true the engine calls `HookManager.setDisableAllHooks(true, true)` before
   * loading hooks, so ZERO hooks register/run for the whole session. Managed-tier
   * only; see {@link ProjectConfig}. Parity: the reference's `disableAllHooks`.
   */
  disableAllHooks?: boolean;
  /**
   * Managed hook lockdown mirrored from the merged config's `allowManagedHooksOnly`.
   * When true the engine calls `HookManager.setAllowManagedHooksOnly(true)` so only
   * admin/managed-sourced hooks load (user/project/plugin hooks blocked). Managed-tier
   * only; see {@link ProjectConfig}. Parity: the reference's `allowManagedHooksOnly`.
   */
  allowManagedHooksOnly?: boolean;
  /** Permission rules evaluated in order; first match wins. */
  permissionRules?: Array<{ tool: string; pattern?: string; action: 'allow' | 'deny' | 'ask' }>;
  /** Least-privilege capability ceiling for a forked/background sub-engine (parity:
   *  the reference's forked-agent `canUseTool`). A HARD cap enforced ahead of every mode —
   *  confines the sub-agent to read-only bash and/or writes within a directory even
   *  under auto-allow. Set by the skill tool when a forked skill declares a sandbox;
   *  undefined for the main engine (inert). */
  capabilityCeiling?: ToolCapabilityCeiling;
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
  /** Invocation-scoped hooks a forked skill or spawned subagent contributes from its
   *  OWN definition's frontmatter `hooks` field (parity with the reference's
   *  registerFrontmatterHooks). Registered on this (sub-)engine's HookManager AFTER
   *  the file-loaded hooks, so they layer on top for the lifetime of the sub-engine
   *  and are torn down when it shuts down. Threaded by the skill/task tools from
   *  SkillDefinition.hooks / AgentDefinition.hooks. */
  frontmatterHooks?: import('./hooks').HookConfigEntry[];
  /** True when {@link frontmatterHooks} come from a SUBAGENT's own frontmatter (set by
   *  the task tool): a `stop` hook is remapped to `subagentStop` on registration, since a
   *  completing subagent fires SubagentStop (parity: the reference registerFrontmatterHooks
   *  `isAgent`). Left false/undefined for forked-skill hooks, which keep `stop`. */
  frontmatterHooksIsAgent?: boolean;
  /** Human-readable origin of {@link frontmatterHooks} (e.g. `agent 'reviewer'` /
   *  `skill 'deploy'`) for the attribution + Stop→SubagentStop conversion debug log. */
  frontmatterHooksSource?: string;
  /** Stance skill name (--skill) — a skill pinned to govern the whole session/run:
   *  its body becomes the system-prompt overlay, plus its tool-scoping, bash
   *  auto-allow, MCP servers, and thinking level. Replaces the old persona. */
  skill?: string;
  /** Named output style overlay appended to the system prompt (e.g. 'concise'). */
  outputStyle?: string;
  /** Managed allowlist of selectable models; non-listed models are blocked.
   *
   *  UNSET-vs-EMPTY semantics (parity: the reference modelAllowlist.ts L103-108):
   *  - UNSET (`undefined`) ⇒ NO restriction — any model may be selected.
   *  - DEFINED but EMPTY (`[]`) ⇒ HARD LOCKDOWN — EVERY user-initiated model switch
   *    (`/model`, `/architect <model>`) is blocked; the admin has disallowed all
   *    user model selection. This is the opposite of unset and is honored by the
   *    user-switch gate (`isUserModelSwitchAllowed` / `isManagedModelLockdown`).
   *  - NON-EMPTY ⇒ only the listed family aliases / version prefixes / full ids are
   *    selectable (family-aware match via `isModelAllowed`).
   *  The non-user DEFAULT slots (startup model, fallback/compaction) deliberately
   *  stay lenient (empty ⇒ off) so they always resolve to some model. */
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
  /** When true, output-gating ("channel") mode is active: the model's plain-text
   *  completions are HIDDEN from the user and every user-facing message must flow
   *  through the `user_message` tool (two-way synced with the tool's availability).
   *  Opt-in and gated by {@link isOutputGateEnabled} (AMI_OUTPUT_GATE). DISTINCT from
   *  the `concise` output STYLE toggled by `/brief` (terseness, a presentation
   *  overlay) — this gates the output CHANNEL, not verbosity. Parity: the reference's
   *  brief-only mode (`isBriefOnly` + `setUserMsgOptIn`, commands/brief.ts). */
  outputGateMode?: boolean;
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
  /** The agent type/name this engine runs as when forked (the `subagent_type` or the
   *  built-in mode). Combined with {@link agentIsBuiltIn} to derive the agent
   *  query-source provenance tag (`agent:builtin:<type>` / `agent:default` /
   *  `agent:custom`) stamped on this engine's turn/api-call analytics events so local
   *  usage introspection can be segmented by agent category. Absent on the main thread.
   *  Parity: the reference's getQuerySourceForAgent (promptCategory.ts). */
  agentType?: string;
  /** Whether {@link agentType} names a BUILT-IN agent identity (vs a user-defined/custom
   *  one). Non-built-in collapses the provenance tag to `agent:custom` so a custom agent
   *  name never leaks into analytics. */
  agentIsBuiltIn?: boolean;
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
  /** Number of CONSECUTIVE capacity-overload errors (HTTP 503/529, "overloaded")
   *  the engine rides out on the SAME model before switching to a fallback model.
   *  An overload is usually transient, so switching models on the first 529 is
   *  premature; the engine retries up to this many times first, then falls back.
   *  A genuine model-unavailable (e.g. `model_not_found`) is unaffected and still
   *  falls back immediately. Default: 3. Env: SUPERINFERENCE_MAX_529_RETRIES
   *  (CLAUDE_CODE_MAX_529_RETRIES accepted as an alias). Parity: the reference's
   *  MAX_529_RETRIES threshold before FallbackTriggeredError. */
  max529RetriesBeforeFallback?: number;
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
  /** @internal Override the auto-mode safety-classifier RUNNER (for testing). Lets a
   *  test drive the full engine→classifier→permission path with a deterministic
   *  verdict instead of a live model call. Defaults to the model-backed runner built
   *  from {@link autoMode} + the provider. Typed `unknown` (cast at the engine call
   *  site) to avoid a types.ts -> common import. Only consulted when {@link autoMode}
   *  is set. */
  _autoModeClassifierRunner?: unknown;

  // -------------------------------------------------------------------------
  // Parity config knobs (the reference settings.json / CLI flags). All optional;
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
  /** Natural-language auto-mode (YOLO) rule configuration. Parity: `settings.autoMode`. */
  autoMode?: AutoModeConfig;
  /** Default install/update channel. Parity: `autoUpdatesChannel`. */
  autoUpdatesChannel?: 'latest' | 'stable';
  /** Remembered responses for env-detected API keys (truncated), so a detected key is
   *  prompted once then trusted/rejected. Parity: `customApiKeyResponses`. */
  customApiKeyResponses?: { approved: string[]; rejected: string[] };
  /** Minutes-away threshold after which an idle-return prompt is shown. Parity: IdleReturnDialog. */
  /** Enable automatic session-memory extraction/consolidation (default true). Parity:
   *  `isAutoMemoryEnabled` / `autoDreamEnabled`. */
  autoMemoryEnabled?: boolean;
  /** Run session-memory extraction only every N eligible turns. Parity: extraction throttle. */
  sessionMemoryInterval?: number;
  /** Master switch for time-based idle microcompact (default off). Parity: timeBasedMCConfig.enabled. */
  /** Idle-gap threshold (minutes) before microcompact fires, tuned against the server
   *  prompt-cache TTL. Parity: `gapThresholdMinutes`. */
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
  /** Disable fenced-code syntax highlighting while keeping UI color. Parity:
   *  `syntaxHighlightingDisabled`. */
  /** Reduce/disable animations for accessibility (persistent, vs env-only). Parity:
   *  `prefersReducedMotion`. */
  prefersReducedMotion?: boolean;
  /** Enable AI-generated inline prompt suggestions. Parity: `promptSuggestionEnabled`. */
  promptSuggestionEnabled?: boolean;
  /** Retention window (days) for stale worktrees/files cleanup. Parity: `cleanupPeriodDays`. */
  cleanupPeriodDays?: number;
  /** Enable the LLM-generated permission explainer (default true). Parity:
   *  `permissionExplainerEnabled`. */
  /** Directory (relative to project root) where plan .md files are written. Parity:
   *  `plansDirectory`. */
  /** Capture a heap dump when the RSS memory-warning threshold is crossed (not just a
   *  warning). Reuses {@link memoryWarningRssBytes} as the trigger level. Parity:
   *  heapDumpService `auto-1.5GB`. */
  heapDumpOnMemoryWarning?: boolean;
  /** Apply /model, /thinking, /fast immediately to an in-flight turn rather than
   *  deferring to turn end. Parity: `shouldInferenceConfigCommandBeImmediate`. */
  /** Backend that runs teammates/subagents; 'auto' resolves against the live
   *  environment. Parity: `teammateMode`. */
  teammateMode?: 'auto' | 'tmux' | 'in-process';
  /** Persisted default model for spawned teammates/subagents, consulted only when a
   *  spawn names no model from a higher-priority source. Tri-state (parity:
   *  `teammateDefaultModel` / getDefaultTeammateModel): a model id/alias string forces
   *  that default; `null` means "Default → follow the leader" (keep the parent's exact
   *  model); omitted falls through to the provider-aware hardcoded fallback. */
  subagentDefaultModel?: string | null;
  /** Opt-in cap on how many of a forked skill's nested tool-use steps are surfaced
   *  LIVE to the parent stream. When set to a non-negative integer N, only the first
   *  N `tool_use_start` steps (and their progress/result rows) of a forked skill are
   *  forwarded as child events; further tool-use steps are suppressed from the live
   *  stream and summarized as a "+K more tool use" line appended to the skill's
   *  result. `0` surfaces the result only. Default (unset / negative / non-finite)
   *  keeps AMI's full live streaming — every nested event is surfaced (verbose).
   *  Parity: the reference's SkillTool `MAX_PROGRESS_MESSAGES_TO_SHOW` (=3) + `verbose`
   *  flag, adapted to AMI's live-stream model (opt-in cap; verbose is the default). */
  maxSkillProgressMessages?: number;
  /** Map of canonical model id → provider-specific model string (e.g. a Bedrock
   *  inference-profile ARN), layered on top of the resolved model. Parity: `modelOverrides`. */
  modelOverrides?: Record<string, string>;
  /** Terminal notification channel for turn-completion alerts. Parity: NOTIFICATION_CHANNELS. */
  /** Plugin ids never suggested for LSP recommendations. Parity: `lspRecommendationNeverPlugins`. */
  lspRecommendationNeverPlugins?: string[];
  /** Hard off-switch for LSP-plugin recommendations. Parity: `lspRecommendationDisabled`. */
  lspRecommendationDisabled?: boolean;
  /** Running count of LSP recommendations the user has ignored. Once it reaches
   *  {@link MAX_LSP_RECOMMENDATION_IGNORED_COUNT}, recommendations auto-disable
   *  (see {@link isLspRecommendationDisabled}). Parity: `lspRecommendationIgnoredCount`
   *  (utils/plugins/lspRecommendation.ts incrementIgnoredCount/isLspRecommendationsDisabled). */
  lspRecommendationIgnoredCount?: number;
  /** Skip the code-block picker on `/copy` and always copy the full response. Parity:
   *  `copyFullResponse`. */
  copyFullResponse?: boolean;
  /** Execute tool calls as they stream in during the assistant message (vs waiting for the
   *  whole message). Runtime gate. Parity: `streamingToolExecution` (query/config.ts). */
  /** Show a one-line preview of each teammate's latest message in the teammate spinner tree.
   *  Parity: `showTeammateMessagePreview`. */
  /** Opt-in consent flag that suppresses the auto-mode (YOLO) permission-opt-in dialog on
   *  subsequent runs once the user has accepted it. Parity: `skipAutoPermissionPrompt`
   *  (components/AutoModeOptInDialog.tsx). */
  /** Explicit column width for the inline diff preview shown in a file-edit permission
   *  prompt. When unset, the width is derived from the verbose flag by
   *  {@link resolveDiffPreviewWidth} ({@link VERBOSE_DIFF_PREVIEW_WIDTH} vs
   *  {@link COMPACT_DIFF_PREVIEW_WIDTH}). Parity: the reference's
   *  `NotebookEditPermissionRequest` `verbose ? 120 : 80` diff width. */
  diffPreviewWidth?: number;
}

export interface PermissionSubsystem {
  mode: string;
  rules: Array<{ tool: string; pattern?: string; action: 'allow' | 'deny' | 'ask' }>;
  promptHandler?: import('./permissions').PermissionPromptHandler;
}
