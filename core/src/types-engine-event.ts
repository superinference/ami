// Split from types.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { DiffHunk, ImageContent, ToolCallContent } from './types-transcript-meta';
import type { ToolDefinition, ToolResult } from './types-tool-definition';
import type { ExitReason, TaskTerminalStatus, TaskUsageSnapshot, TodoItemView, UsageStats, UserQuestionAnnotation, UserQuestionOption } from './types-provider-config';
import type { EngineConfig } from './types';

/**
 * The kind of live activity a relayed `subagent_progress` event describes. The
 * base kinds (`text`/`thinking`/`tool`) carry the sub-agent's streamed prose,
 * reasoning, and tool calls; `error`/`retry` surface failure + retry liveness on
 * the roster the moment they happen (parity: the reference's AgentTool relaying a
 * sub-agent's error/retry so the roster reflects it instead of appearing frozen).
 * `waiting` surfaces that the sub-agent is BLOCKED on an interactive
 * approval/question (a permission `ask`, an `AskUserQuestion`, or plan-mode
 * consent) so the roster shows "awaiting approval" instead of freezing on the
 * child's last tool/text row — parity: the reference's swarmWorkerHandler
 * `pendingWorkerRequest` "waiting for leader approval" affordance + the
 * AssistantToolUseMessage hook-progress "waiting" subtree.
 */
export const SUBAGENT_ACTIVITYS = ['text', 'thinking', 'tool', 'error', 'retry', 'waiting'] as const;

export type SubagentActivity = (typeof SUBAGENT_ACTIVITYS)[number];

/**
 * Why a request is being retried, carried on the `retry_attempt` {@link EngineEvent}
 * so a frontend can render a connection/timeout/idle retry distinctly from an
 * HTTP-status retry (which has a real `statusCode`). Parity: the reference's withRetry cause
 * classification (services/api/withRetry.ts) distinguishing HTTP errors from
 * connection resets, request timeouts, and idle-stream aborts.
 *   - `http`               — the server returned a real HTTP status (429/500/503/529…).
 *   - `connection_reset`   — a connection-level network failure (ECONNRESET/EPIPE/…) with no HTTP response.
 *   - `connection_timeout` — the request hard-timeout fired before any response.
 *   - `stream_idle`        — the inactivity watchdog aborted a quiet-but-open stream.
 */
export const RETRY_ERROR_KINDS = ['http', 'connection_reset', 'connection_timeout', 'stream_idle'] as const;

export type RetryErrorKind = (typeof RETRY_ERROR_KINDS)[number];

export type EngineEvent =
  | { type: 'text_delta'; text: string }
  | { type: 'thinking_delta'; text: string }
  | { type: 'tool_call_generating'; toolName: string; toolIndex: number; argsDelta: string }
  | { type: 'tool_use_start'; toolName: string; toolCallId: string; input: Record<string, unknown> }
  | { type: 'tool_use_progress'; toolCallId: string; data: string;
      /** Live wall-clock seconds the underlying command/task has been running, so a
       *  frontend can show a ticking "(1m3s · N lines)" affordance on a slow in-flight
       *  call without re-parsing `data`. Revealed only past the
       *  {@link MIN_SLOW_PROGRESS_REVEAL_SECONDS} threshold (see
       *  {@link shouldRevealSlowProgress}) so brief commands don't flicker a timer.
       *  Parity: the reference's bash_progress/powershell_progress `elapsedTimeSeconds`
       *  (components/messages/CollapsedReadSearchContent.tsx). Omitted when unmeasured. */
      elapsedSeconds?: number;
      /** Total output lines seen so far for the running command, for the live
       *  "· N lines" counter. Parity: the reference's shell-progress `totalLines`. Omitted
       *  when the tool does not count lines. */
      totalLines?: number }
  | { type: 'tool_use_result'; toolCallId: string; toolName: string; output: string; isError: boolean;
      /** The frontend should ABSORB this result from the transcript (render nothing)
       *  rather than echoing a confirmation row, while the model still receives the
       *  `output` in context. Set only for a routed / async-request `send_message`
       *  control send (see {@link shouldSuppressSendMessageTranscript}) — the parity
       *  of the reference's SendMessageTool renderToolResultMessage null-render branches
       *  (tools/SendMessageTool/UI.tsx). Absent/false → render normally. */
      suppressTranscript?: boolean;
      /** Per-hunk structured diff of a file-mutating result, forwarded from the
       *  tool's {@link ToolResult.structuredPatch} so a consumer (the session
       *  journal, a replay/introspection surface) can record WHERE an edit landed
       *  without re-parsing `output`. Absent for non-mutating tools. */
      structuredPatch?: DiffHunk[];
      /** The structured patch was omitted because the file exceeded the diff-scan
       *  cap (forwarded from {@link ToolResult.structuredPatchTruncated}) — the
       *  edit succeeded but its location could not be rendered. */
      structuredPatchTruncated?: boolean;
      /** Image content blocks the model should receive as real MULTIMODAL content
       *  alongside this result's text — e.g. an image a hook/user attached to a
       *  permission decision ({@link PermissionPromptResult.contentBlocks}). The
       *  engine queues these into the pending-tool-image path (gated on vision
       *  support) so they are injected as a user image message before the next model
       *  turn, exactly like a tool-produced image. Absent for ordinary results.
       *  Parity: the reference's `PermissionDecision.contentBlocks` image blocks flowing
       *  back as tool_result content. */
      resultImages?: ImageContent[] }
  | { type: 'turn_complete'; content: string | null; toolCalls: ToolCallContent[]; finishReason?: string }
  | { type: 'usage_update'; stats: UsageStats }
  | { type: 'error'; error: string }
  // `statusCode` is `null` for a connection-level failure that produced NO HTTP
  // response (timeout / network drop), distinct from a real HTTP status such as 429
  // or 503 (parity: the reference's SDKAPIRetryMessage `error_status: number | null`). It is
  // now OPTIONAL: a connection/timeout/idle retry may omit it entirely and carry only
  // `errorKind`, so a frontend never has to render a meaningless "HTTP null".
  // `errorKind` discriminates WHY the retry fired (parity: the reference's withRetry cause
  // classification) so connection/timeout/idle retries render distinctly from an
  // HTTP-status retry even when no status is present.
  | { type: 'retry_attempt'; attempt: number; maxRetries: number; delayMs: number; statusCode?: number | null; errorKind?: RetryErrorKind }
  // `retryAfterMs` (rate-limit category only): the server-directed wait the engine
  // derived from the `Retry-After` header / 429 body ("try again in Ns"), so a UI
  // can show WHEN the limit resets (parity: the reference's getLimitReachedText "· resets
  // <time>"), not just a bare "[RATE_LIMITED]". Absent when no hint was provided.
  | { type: 'error_classified'; category: string; retryable: boolean; willRetry: boolean; message?: string; retryAfterMs?: number }
  | { type: 'provider_changed'; provider: string; model: string; reason: string }
  | { type: 'checkpoint_created'; checkpointId: string; files: string[] }
  | { type: 'analytics_summary'; summary: Record<string, number> }
  | { type: 'superinference_state'; state: { value: number; entropy: number; eig: number; step: number; ppv: number }; stopReason: { type: string; detail: string } }
  | { type: 'suggest_file_save'; content: string; suggestedPath: string; lineCount: number }
  | { type: 'user_question'; toolCallId: string; question: string; options: UserQuestionOption[]; allowFreeText: boolean; header?: string; multiSelect?: boolean; answers?: string[]; annotations?: Record<string, UserQuestionAnnotation>; defaultIndex?: number;
      /** Dimmed secondary context shown beside the prompt title: a terse summary of
       *  the TOOL INPUT that triggered this question (e.g. the command/file a
       *  permission-style ask refers to), so the user sees WHAT is being asked about
       *  without it crowding the main `question`. `null`/empty are normalized to
       *  undefined by {@link sanitizeToolInputSummary}. Parity: the reference's PromptDialog
       *  `toolInputSummary` rendered in the `titleRight` slot. */
      toolInputSummary?: string | null }
  | { type: 'session_title'; title: string }
  /** A live per-session handshake snapshot emitted ONCE as the FIRST event of a
   *  fresh session (before the first turn runs), so a remote-control / SDK /
   *  detached consumer can obtain the session's resolved state in one message
   *  instead of reconstructing it from later deltas: the `sessionId`, launch
   *  `cwd`, active `model` + `permissionMode`, the resolved `tools` (names), the
   *  live `mcpServers` ([{name,status}]) and the running `version`. Emitted from a
   *  SINGLE engine site so the TUI and the detached/SDK runner receive an IDENTICAL
   *  shape (the TUI↔CLI parity invariant). Parity: the reference's `system/init`
   *  snapshot message (utils/messages/systemInit.ts `buildSystemInitMessage`). */
  | { type: 'session_init'; sessionId: string; cwd: string; model: string; permissionMode: string; tools: string[]; mcpServers: { name: string; status: string }[]; version: string }
  /** Progressive memory-pressure warning. `level` is the escalation tier the live
   *  RSS reading entered — `1` (high / soft warning), `2` (critical), `3` (severe —
   *  the warning×2 escalation ceiling OR the independent absolute CRITICAL ceiling).
   *  The tier lets a frontend surface DISTINCT severity wording ('High' vs 'Critical'
   *  vs 'Severe') and escalate the notification kind at the top tier, rather than a
   *  single flat "high memory" message. Parity: the reference's `useMemoryUsage`
   *  high/critical status drives distinct notification severity. Always `>= 1` because
   *  the event is only emitted when the tier rises above 0. */
  | { type: 'memory_warning'; level: 1 | 2 | 3; rssBytes: number; thresholdBytes: number; recoverySteps: string[] }
  | { type: 'plan_mode_changed'; enabled: boolean }
  | { type: 'task_updated'; taskId: number; status: string; subject: string }
  | { type: 'skill_activated'; skill: string; description: string; hasMcpServers: boolean; source: 'model' | 'user' }
  | { type: 'subagent_start'; nodeId: string; depth: number; agentName?: string; model?: string; prompt: string; color?: string;
      /** The resolved sub-agent KIND this task was spawned as (`subagent_type`): a built-in
       *  mode (`explore`/`plan`/`general`) or a named agent definition. Distinct from
       *  {@link agentName} (a per-spawn display/identity name) — two agents can share a name
       *  but differ in type. Lets a roster surface the classification at a glance. Parity:
       *  the reference's `task_started` event `task_type` (sdkEventQueue.ts). Omitted when unset. */
      taskType?: string }
  | { type: 'subagent_progress'; nodeId: string; depth: number; activity: SubagentActivity; detail: string; usage?: TaskUsageSnapshot; lastToolName?: string;
      /** A concise mid-turn progress summary for the sub-agent's work so far (a forked
       *  summarizer phrase emitted periodically), distinct from the per-step `detail`
       *  activity line — lets a roster show "what this agent is accomplishing" rather
       *  than only its latest raw action. Parity: the reference's task progress `summary`
       *  field alongside `last_tool_name` (utils/task/sdkProgress.ts). Omitted until a
       *  summary has been computed. */
      summary?: string }
  | { type: 'subagent_result'; nodeId: string; depth: number; success: boolean; summary: string; tokens: number; status?: TaskTerminalStatus; toolUses?: number; durationMs?: number; outputPath?: string }
  | { type: 'todo_update'; todos: TodoItemView[] }
  /** An MCP server pushed a resource-update (`notifications/resources/updated`) or
   *  resource-list-change (`notifications/resources/list_changed`) notification.
   *  Carries server provenance so the user has an audit trail of WHICH server
   *  changed WHAT (and why, for a list change). Parity: the reference's
   *  `UserResourceUpdateMessage`. `uri` is empty for a whole-list change. */
  | { type: 'mcp_resource_update'; server: string; uri: string; reason?: string }
  /** A polled MCP tool's output changed between polls. Carries server + tool
   *  provenance so the user has an audit trail of WHICH polled tool (on WHICH server)
   *  produced new output. The engine also injects a model-facing
   *  `<mcp-polling-update type="tool" …>` user turn. Parity: the reference's
   *  `UserResourceUpdateMessage` polling branch. */
  | { type: 'mcp_polling_update'; server: string; tool: string; reason?: string }
  /** An MCP server's connection state changed mid-session in a user-relevant way:
   *  `'lost'` (a ready server dropped), `'recovered'` (it came back after a drop), or
   *  `'failed'` (reconnection was abandoned). Carries server provenance + an optional
   *  `detail` (the raw state / attempt count) so a dropped tool/MCP connection is
   *  surfaced proactively instead of silently vanishing. Parity: the reference's IDE/MCP
   *  connection status indicator (`useIDEStatusIndicator`). */
  | { type: 'mcp_connection_changed'; server: string; state: 'lost' | 'recovered' | 'failed'; detail?: string }
  /** A project instruction file (CLAUDE.md / AGENTS.md) `@import`ed a file OUTSIDE
   *  the workspace trust boundary, so its content was stripped and replaced with an
   *  inline `[import blocked …]` placeholder. The engine detects this silently via the
   *  `instructionsLoaded` hook with `loadReason:'import-blocked-outside-workspace'`.
   *  This event surfaces it so an interactive user sees WHICH external paths were
   *  dropped instead of acting on instruction files that were quietly truncated.
   *  `paths` lists every blocked import drained together. Parity: the reference's
   *  `ClaudeMdExternalIncludesDialog` (enumerated external-import warning). */
  | { type: 'external_import_blocked'; paths: string[] }
  /** An inbound peer/teammate message arrived on this session's mailbox and was
   *  relayed into the conversation. The engine already injects it as a fenced
   *  `<peer-message>` user turn for the model, but the TUI renders from EngineEvents,
   *  not the message list — so without this event the human never sees that a peer
   *  spoke. Carries the sender (`from`), the relayed `body`, and the optional
   *  per-message `summary`/roster `color` the sender stamped. Parity: the reference's
   *  dedicated `UserTeammateMessage` transcript renderer. */
  | { type: 'peer_message'; from: string; body: string; summary?: string; color?: string }
  /** A backgrounded task (bash/agent/monitor) reached a terminal state. The engine
   *  already injects a collapsed completion note for the model, and this event surfaces
   *  each completion distinctly to the TUI/detached frontend with its status-aware
   *  `summary` phrase (`completed (exit code: 0)` / `failed with exit code 1` /
   *  `was stopped`) so a finished/failed/killed background task is visible inline
   *  instead of only in the model's context. Parity: the reference's shell/local-agent
   *  completion notification. `outputFile` is the on-disk path the task's captured
   *  output was streamed to (the `${taskId}.output` spool), so a frontend can offer a
   *  "view full output" affordance that reads the complete transcript instead of only
   *  the collapsed summary. Parity: the reference's task terminal notification
   *  `output_file` field (`utils/sdkEventQueue.ts`). Omitted only on a legacy direct
   *  emit that never recorded a spool path. */
  | { type: 'background_task_complete'; taskId: string; status: string; summary: string; outputFile?: string }
  /** A backgrounded (async) hook finished. The engine's `hookRewake` channel only
   *  surfaces a DENY/exit-2 or an error as a flattened `[hookRewake:…]` system string
   *  for the MODEL; this event is the USER/introspection-facing completion RECORD for
   *  EVERY async hook — including a clean success — classified by `outcome`
   *  (`'success'` | `'error'` | `'cancelled'`), with the command hook's `exitCode` and
   *  SEPARATELY-captured `stdout`/`stderr` rather than one merged blob, plus the
   *  installing plugin's provenance when the hook came from a bundle. Emitted once per
   *  async hook from a SINGLE engine site. Parity: the reference's
   *  `finalizeHook`/`emitHookResponse` structured async-hook response
   *  (utils/hooks/AsyncHookRegistry.ts). */
  | { type: 'hook_response'; event: string; outcome: 'success' | 'error' | 'cancelled'; exitCode?: number; stdout?: string; stderr?: string; pluginName?: string; pluginId?: string }
  /** The engine reached its per-submit turn cap (`maxTurns`) without the model
   *  signaling completion. This is a NORMAL, expected stop BOUNDARY — not a failure —
   *  so it is surfaced as its own informational event (carrying the configured
   *  `maxTurns` and the `turnCount` actually run) instead of a generic `error`, letting
   *  a frontend render the limit neutrally rather than as a stream/engine failure. The
   *  terminal `done` event still carries `exitReason: 'max_turns'` for exit-code
   *  mapping. Parity: the reference's `max_turns_reached` informational attachment
   *  (query.ts) — an expected boundary the UI renders neutrally, distinct from an error. */
  | { type: 'max_turns_reached'; maxTurns: number; turnCount: number }
  /** The autonomous-mode safety classifier is ACTIVELY evaluating an about-to-be
   *  auto-approved tool call (`checking:true`), or has just finished (`checking:false`).
   *  On the autonomous auto-approve path (`auto-allow` / `bypassPermissions` / screened
   *  `auto` / inherited plan-bypass) a model-backed classifier runs a real async query
   *  before a tool call is silently allowed (see
   *  {@link PermissionManager.setAutoModeSafetyClassifier}); until it resolves the user
   *  has no signal that an approval is not instantaneous/unchecked. This event brackets
   *  that async check so a frontend can surface an in-flight "safety check in progress"
   *  indicator and the engine's {@link ToolConfirmationService} can track the checking
   *  state (parity: the reference's `CLASSIFIER_CHECKING` set +
   *  `setClassifierChecking`/`clearClassifierChecking`/`isClassifierChecking`/
   *  `subscribeClassifierChecking` in `classifierApprovals.ts`). Emitted only when a
   *  classifier is installed and actually consulted for this call. */
  | { type: 'classifier_checking'; toolName: string; checking: boolean }
  /** The skill-improvement lifecycle surfaced a user/introspection-facing signal.
   *  AMI can write learned corrections back into a project `SKILL.md` (the `/skill
   *  improve` flow + `SkillImprovementSurvey` apply affordance); this event makes
   *  that otherwise-silent feature OBSERVABLE so the TUI, a diagnostic, and
   *  `ami --introspect` can see WHEN it fired — mirroring the reference's structured
   *  `skill_improvement_detected` signal (utils/hooks/skillImprovement.ts) and its
   *  survey appeared/responded lifecycle events (hooks/useSkillImprovementSurvey.ts),
   *  minus the vendor telemetry routing. `phase` discriminates the moment:
   *    - `'detected'` — the classifier proposed `updateCount` edits for `skillName`
   *      (the once-per-detection signal; `applied` omitted);
   *    - `'responded'` — the user acted on a proposal: `applied` is `true` when the
   *      SKILL.md was actually rewritten, `false` when the suggestion was dismissed.
   *  `updateCount` is the number of proposed edits (carried on both phases so a
   *  frontend can show "N improvements"). Emitted from a SINGLE engine site
   *  ({@link drainSkillImprovementNotices}) so the TUI and the detached/SDK runner
   *  receive an identical shape (the TUI↔CLI parity invariant). */
  | { type: 'skill_improvement'; phase: 'detected' | 'responded'; skillName: string; updateCount: number; applied?: boolean }
  /** A supervised child (sub)agent's turn ENDED without a proper completion, so the
   *  supervisor classified the stop and raised a structured notice to its parent. This
   *  is the agent-supervision parity of Orca's worker turn-stop report
   *  (runtime/orchestration/workers/turn-stops/worker-turn-stop-report.ts): when a child
   *  turn stops, `reason` classifies HOW (`finished` = at rest without reporting,
   *  `errored` = ended on an error, `needs-input` = waiting on a prompt/approval a human
   *  must answer, `exited` = the process ended, `lost` = it vanished). `reportId` is a
   *  content-hashed stable id (`stopReportId`) so the parent is told ONCE per distinct
   *  event, not once per supervision poll — a re-emit with the same `reportId` is a no-op
   *  for a deduping consumer. `subject`/`body` are the ready-to-render notice (what
   *  happened, a redacted output tail, left-behind artifacts), `recommendedNextStep` is
   *  the single actionable line, `failureCount` is the child's accumulated consecutive
   *  failure count, and `settled` names how the stop closed the child's dispatch:
   *  `'failed'` (retryable) or `'circuit_broken'` (the retry budget is spent — give up and
   *  tell the user). `awaitsReply` is true when a `needs-input` stop can be answered with a
   *  message (vs a prompt a person must answer in place). `outputTail` is the redacted,
   *  length-capped tail of the child's last output. Append-only EngineEvent member. */
  | { type: 'agent_turn_stop'; nodeId: string; depth?: number; reason: AgentTurnStopReason; reportId: string; subject: string; body: string; recommendedNextStep: string; failureCount: number; settled?: 'failed' | 'circuit_broken'; awaitsReply?: boolean; outputTail?: string }
  | { type: 'done'; totalTurns: number; exitReason: ExitReason };

/** How a supervised child (sub)agent's turn stopped without a proper completion
 *  (parity: Orca's `WorkerTurnStopReason`): `finished` = ended at rest without sending a
 *  done report, `errored` = ended on an error, `needs-input` = stopped at a prompt/approval
 *  a human must answer, `exited` = the process ended, `lost` = it vanished with no cause.
 *  Single source of truth for the {@link EngineEvent} `agent_turn_stop` `reason` and the
 *  common-layer supervision module. */
export const AGENT_TURN_STOP_REASONS = ['finished', 'errored', 'needs-input', 'exited', 'lost'] as const;

export type AgentTurnStopReason = (typeof AGENT_TURN_STOP_REASONS)[number];

/**
 * Natural-language auto-mode (YOLO) rule configuration (parity: the reference's
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
  /** Serialize the proposed tool call handed to the classifier as a compact JSONL dict
   *  line (`{"bash":"ls"}` for a tool call, the tool's input projected to its most
   *  salient value) instead of the default verbose human sentence. JSON escaping keeps
   *  hostile tool input from breaking out of its string context to forge another
   *  transcript line. Off (undefined/false) → the default descriptive rendering. Parity:
   *  the reference's `AutoModeConfig.jsonlTranscript` / `isJsonlTranscriptEnabled` →
   *  `toCompactBlock` ({@code {"Bash":"ls"}} vs {@code `Bash ls`}). */
  jsonlTranscript?: boolean;
}

// ---------------------------------------------------------------------------
// LSP-plugin recommendation gating (parity: utils/plugins/lspRecommendation.ts)
// ---------------------------------------------------------------------------

/** After this many ignored LSP recommendations, suggestions auto-disable.
 *  Parity: lspRecommendation.ts `MAX_IGNORED_COUNT = 5`. */
export const MAX_LSP_RECOMMENDATION_IGNORED_COUNT = 5;

/**
 * Whether LSP-plugin recommendations are currently suppressed for a config.
 * Disabled when the user explicitly turned them off, OR they have ignored at
 * least {@link MAX_LSP_RECOMMENDATION_IGNORED_COUNT} recommendations. Pure —
 * mirrors the reference's `isLspRecommendationsDisabled()`.
 */
export function isLspRecommendationDisabled(
  config: Pick<EngineConfig, 'lspRecommendationDisabled' | 'lspRecommendationIgnoredCount'>,
): boolean {
  return (
    config.lspRecommendationDisabled === true ||
    (config.lspRecommendationIgnoredCount ?? 0) >= MAX_LSP_RECOMMENDATION_IGNORED_COUNT
  );
}

// ---------------------------------------------------------------------------
// Inline prompt-suggestion default resolution (parity:
// services/PromptSuggestion/promptSuggestion.ts `shouldEnablePromptSuggestion`)
// ---------------------------------------------------------------------------

/** Resolved inputs for {@link shouldEnablePromptSuggestion}. The caller resolves
 *  the env override (via the shared env truthy/falsy helpers) and interactivity
 *  so this stays a pure, dependency-free decision function. */
export interface PromptSuggestionResolution {
  /** Persisted/config toggle ({@link EngineConfig.promptSuggestionEnabled}).
   *  Defaults to ON when omitted. */
  configEnabled?: boolean;
  /** A set env override: `true`/`false` force the outcome; `undefined` defers to
   *  the config toggle. Parity: `CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION`. */
  envOverride?: boolean;
  /** Print/piped/SDK sessions never get inline suggestions. Parity:
   *  `getIsNonInteractiveSession()`. */
  nonInteractive?: boolean;
}

/**
 * Decide whether AI-generated inline prompt suggestions are enabled, matching
 * the reference's precedence: an explicit env override wins; otherwise non-interactive
 * sessions are always off; otherwise the config toggle decides (default ON).
 */
export function shouldEnablePromptSuggestion(resolution: PromptSuggestionResolution = {}): boolean {
  if (resolution.envOverride !== undefined) return resolution.envOverride;
  if (resolution.nonInteractive) return false;
  return resolution.configEnabled ?? true;
}

// ---------------------------------------------------------------------------
// w88 parity primitives — pure, dependency-free building blocks for the typed
// schema additions above (event fields, ToolResult/ToolDefinition/SystemMessage
// fields). Each mirrors a specific the reference behavior; display/rendering stays
// in the cli layer, these only derive/validate data.
// ---------------------------------------------------------------------------

/** Minimum wall-clock seconds an in-flight command must have been running before a
 *  live "(elapsed · N lines)" progress suffix is revealed, so brief commands never
 *  flicker a timer. Parity: the reference's 2-second slow-command reveal threshold
 *  (components/messages/CollapsedReadSearchContent.tsx). */
export const MIN_SLOW_PROGRESS_REVEAL_SECONDS = 2;

/** Whether a running tool call has been slow enough to reveal its live elapsed/line
 *  progress affordance. `undefined`/unmeasured elapsed ⇒ not revealed. Pure; mirrors
 *  the reference's `elapsedTimeSeconds >= 2` gate for the ticking progress suffix. */
export function shouldRevealSlowProgress(
  elapsedSeconds: number | undefined,
  thresholdSeconds: number = MIN_SLOW_PROGRESS_REVEAL_SECONDS,
): boolean {
  return typeof elapsedSeconds === 'number' && elapsedSeconds >= thresholdSeconds;
}

/** Normalize a {@link EngineEvent}'s `toolInputSummary` (a `user_question` field): a
 *  `null`, `undefined`, or whitespace-only value collapses to `undefined` so a
 *  renderer can treat "no summary" uniformly; otherwise the trimmed text is returned.
 *  Parity: the reference's PromptDialog `toolInputSummary?: string | null` handling. */
export function sanitizeToolInputSummary(summary: string | null | undefined): string | undefined {
  if (summary == null) return undefined;
  const trimmed = summary.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

/** Resolve a {@link ToolResult.sentAt} emit timestamp: keep an already-stamped value
 *  (so a resumed/replayed result shows its ORIGINAL time), otherwise stamp `nowIso`
 *  (defaulting to the current clock). This enforces the "optional for replay, stamped
 *  once at execution" invariant in one place. Parity: the reference's BriefTool
 *  `sentAt = new Date().toISOString()` captured at execution and preserved on resume. */
export function resolveSentAt(existing: string | undefined, nowIso: string = new Date().toISOString()): string {
  return existing ?? nowIso;
}

/** The MODEL-facing output for a {@link ToolResult}: always the terse `output`
 *  (the model never re-ingests a decoupled user-facing payload). Parity: the reference's
 *  `mapToolResultToToolResultBlockParam` returning only the receipt. */
export function selectModelFacingOutput(result: Pick<ToolResult, 'output'>): string {
  return result.output;
}

/** The USER-facing output for a {@link ToolResult}: the decoupled
 *  {@link ToolResult.userFacingContent} when set, else the plain `output`. Parity:
 *  the reference's BriefTool `output.data` (UI) vs the model receipt. */
export function selectUserFacingOutput(result: Pick<ToolResult, 'output' | 'userFacingContent'>): string {
  return result.userFacingContent ?? result.output;
}

/** Whether a tool opts into grouped (collapsed) rendering of consecutive calls.
 *  Absent field ⇒ `false` (render each call individually). Parity: the reference's per-tool
 *  `renderGroupedToolUse` opt-in. */
export function shouldRenderGroupedToolUse(tool: Pick<ToolDefinition, 'renderGroupedToolUse'>): boolean {
  return tool.renderGroupedToolUse === true;
}
