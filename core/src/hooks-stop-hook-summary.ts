// Split from hooks.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { type StopFailureErrorClass } from './error-classifier';
import type { PermissionUpdate, PermissionMode } from './permissions';
import type { CompactContext, HookContext, SessionContext, SubagentContext } from './hooks-session-context';

export interface PermissionContext {
  toolName: string;
  toolInput: Record<string, unknown>;
  command?: string;
  reason?: string;
  retry?: boolean;
  /** Active permission posture (`ask` | `deny-all` | `auto-allow` | `plan`) so a
   *  PermissionRequest hook can branch on the mode when deciding — the reference's
   *  CoordinatorPermissionParams.permissionMode threaded into `runHooks`. */
  permissionMode?: PermissionMode;
  /** Permission-rule updates the engine is already suggesting for this call; a
   *  hook may inspect them before returning its own `updatedPermissions`. Mirrors
   *  the reference's `suggestions: PermissionUpdate[]` param to `runHooks`. */
  suggestions?: PermissionUpdate[];
  /** Turn abort signal — threaded in so a slow/hanging in-process permission hook
   *  (and a command-hook subprocess) is cancelled when the user interrupts (Esc /
   *  Ctrl-C). Parity with the reference threading `abortController.signal` into
   *  `executePermissionRequestHooks` / the classifier. */
  abortSignal?: AbortSignal;
  /** Correlation id of the pending tool call this permission request is for
   *  (the reference's permission-prompt-tool `tool_use_id`). Threaded through to the
   *  hook's stdin payload (serialized as `tool_use_id`) so a delegated
   *  permission-prompt tool / SDK hook can tie its decision back to the exact call
   *  and dedup/correlate concurrent requests. Optional: callers that don't have an
   *  id (synthetic/non-tool contexts) omit it. */
  toolUseId?: string;
}

export interface TaskEventContext {
  taskId: string;
  subject: string;
  status?: string;
  description?: string;
  /** Abort/cancellation boundary (the reference parity): the surrounding tool call's
   *  AbortSignal, forwarded so a long-running task-event hook is cancellable when the
   *  turn is aborted. When already aborted, the hook loop short-circuits and runs no
   *  further hooks. Optional so non-tool callers may omit it. */
  signal?: AbortSignal;
}

export interface UserPromptContext {
  prompt: string;
  turnCount: number;
}

/**
 * Context for the TeammateIdle hook (the reference's TeammateIdleHookInputSchema —
 * `teammate_name` + `team_name`). Fires when a teammate agent in a multi-agent
 * team finishes its run and transitions to idle, so a hook can react (notify,
 * reassign work, record metrics) to an idle teammate. A `matcher` on a
 * teammateIdle hook is tested against `teammateName`
 * (HOOK_MATCHER_FIELD.teammateIdle) so a hook can scope to one teammate.
 */
export interface TeammateContext {
  teammateName: string;
  teamName: string;
}

export interface NotificationContext {
  message: string;
  notificationType?: string;
  title?: string;
}

export interface ConfigChangeContext {
  source: string;
  filePath?: string;
}

export interface InstructionsContext {
  filePath: string;
  loadReason: string;
  /**
   * Which memory tier the instruction file belongs to — `Project` | `Local` |
   * `User` | `Managed` (parity: the reference's InstructionsLoaded `memory_type`,
   * sdk/coreSchemas.ts). Optional: emitters that don't classify the source omit it.
   */
  memoryType?: string;
}

export interface WorktreeContext {
  name?: string;
  worktreePath?: string;
}

export interface FileChangeContext {
  filePath: string;
  event: 'change' | 'add' | 'unlink';
}

export interface CwdChangedContext {
  oldCwd: string;
  newCwd: string;
}

export interface StopFailureContext {
  /**
   * The matchable failure CLASS (the reference's StopFailure `error` discriminator),
   * one of {@link StopFailureErrorClass}. A `matcher` on a StopFailure hook scopes
   * on this value, so the sole emitter classifies the raw failure into the taxonomy
   * via {@link stopFailureErrorClass} before firing — a matcher configured with one
   * of the advertised classes (`rate_limit`, `authentication_failed`, …) then
   * actually matches, instead of failing against a raw, unmatchable error string.
   */
  error: StopFailureErrorClass;
  /** The raw, human-readable failure detail (the thrown message / API error text),
   *  surfaced to the hook payload for logging without being the matchable field. */
  errorMessage?: string;
  hookIndex: number;
}

export interface ElicitationContext {
  mcpServerName: string;
  message?: string;
  mode?: string;
  /** The JSON-schema of the fields the MCP server is requesting the user fill in
   *  (the reference's elicitation `requestedSchema`). Forwarded to the hook stdin as
   *  `requested_schema` so an elicitation hook can inspect WHAT is being asked
   *  before deciding accept/decline/cancel. Sourced from the MCP elicitation
   *  request params (see mcp/client.ts). Optional — observe-only callers omit it. */
  requestedSchema?: Record<string, unknown>;
  /** The URL an MCP server asked the user to visit for this elicitation (the reference
   *  Code's elicitation `url`), when the request is URL-mode. Serialized as `url`. */
  url?: string;
  /** Correlation id of the MCP elicitation request (the reference's `elicitationId`),
   *  so a hook can tie its decision back to the exact request / dedup concurrent
   *  elicitations. Serialized as `elicitation_id`. Optional. */
  elicitationId?: string;
}

/** SessionStart/Setup hook result. A hook may inject a single `initialUserMessage`
 *  OR, matching the reference's SessionStart hook result (an ARRAY of injectable
 *  messages, not a single string), multiple messages via `initialUserMessages`.
 *  A hook may ALSO contribute `additionalContexts[]` — model-facing context that
 *  executeSessionStart/executeSetup accumulate across every hook and surface as a
 *  single combined `hook_additional_context` attachment (the reference's
 *  SessionStart/Setup additionalContexts aggregation).
 *  executeSessionStart aggregates ALL messages from ALL hooks (not last-wins). */
export type SessionHookFn = (context: SessionContext) => Promise<{ initialUserMessage?: string; initialUserMessages?: string[]; additionalContexts?: string[]; watchPaths?: string[] } | void>;

export type SubagentHookFn = (context: SubagentContext) => Promise<void>;

/** SubagentStart hook: may contribute additionalContexts prepended to the
 *  subagent's initial messages (the reference parity). */
export type SubagentStartHookFn = (context: SubagentContext) => Promise<{ additionalContexts?: string[] } | void>;

/** PostCompact hook: receives the generated compaction summary + trigger via
 *  {@link CompactContext} and may surface a user-facing display message (the reference
 *  Code's `executePostCompactHooks` → `userDisplayMessage`). */
export type PostCompactHookFn = (context: CompactContext) => Promise<{ userDisplayMessage?: string } | void>;

/** PreCompact hook: may inject/override compaction instructions and surface a
 *  user-facing display message (the reference parity). */
export type PreCompactHookFn = (context: CompactContext) => Promise<{ newCustomInstructions?: string; userDisplayMessage?: string } | void>;

/** How a permission grant/denial should be classified (the reference's
 *  `PermissionDecisionClassificationSchema`): `user_temporary` (session-scoped
 *  grant), `user_permanent` (durable grant the engine persists), or `user_reject`
 *  (explicit rejection). Carried on BOTH allow and deny permission decisions so a
 *  host/engine can distinguish a one-off approval from a remembered rule. */
export const HOOK_DECISION_CLASSIFICATIONS = ['user_temporary', 'user_permanent', 'user_reject'] as const;

export type HookDecisionClassification = (typeof HOOK_DECISION_CLASSIFICATIONS)[number];

/** Narrow an arbitrary value to a known decision classification. Matches the reference
 *  Code's lenient `z.enum(...).catch(undefined)`: a malformed value degrades to
 *  `false` (→ undefined at the call site) rather than rejecting the decision. */
export function isHookDecisionClassification(v: unknown): v is HookDecisionClassification {
  return typeof v === 'string' && (HOOK_DECISION_CLASSIFICATIONS as readonly string[]).includes(v);
}

/**
 * Result a PermissionRequest hook may return (the reference's PermissionDecision
 * from `runHooks`). Beyond allow/deny, an ALLOW may rewrite the tool input
 * (`updatedInput`) and/or add durable permission rules the engine persists
 * (`updatedPermissions`); a DENY may set `interrupt` to abort the whole turn
 * instead of just skipping this one tool call.
 */
export interface PermissionHookResult {
  action?: 'allow' | 'deny';
  reason?: string;
  updatedInput?: Record<string, unknown>;
  updatedPermissions?: PermissionUpdate[];
  interrupt?: boolean;
  /** Classifies the grant/denial as session-scoped / durable / explicit-reject
   *  (the reference's `decisionClassification`). Parsed leniently — a malformed
   *  value from the hook is dropped rather than rejecting the whole decision. */
  decisionClassification?: HookDecisionClassification;
}

export type PermissionHookFn = (context: PermissionContext) => Promise<PermissionHookResult | void>;

/** TaskCreated hook: like a TaskCompleted hook, a blocking TaskCreated hook can
 *  VETO a just-created task by returning a blockingError; the task tool then rolls
 *  back the created task and surfaces the reason (the reference parity —
 *  TaskCreateTool's taskCreated-hook blockingError + compensating deleteTask). A
 *  plain void return is the non-blocking case. */
export type TaskEventHookFn = (context: TaskEventContext) => Promise<{ blockingError?: string } | void>;

/** TaskCompleted hook: a blocking hook can veto a status→completed transition
 *  by returning a blockingError (the reference parity). */
export type TaskCompletedHookFn = (context: TaskEventContext) => Promise<{ blockingError?: string } | void>;

/** UserPromptSubmit hook result: may inject context, or BLOCK/deny the prompt
 *  (blockingError) / prevent continuation, matching the reference. */
export interface UserPromptSubmitHookResult {
  additionalContext?: string;
  blockingError?: string;
  preventContinuation?: boolean;
  stopReason?: string;
  /** USER-facing warning text a hook emitted via `systemMessage` (the reference's
   *  user-shown warning channel, distinct from model-facing `additionalContext`).
   *  Aggregated across every UserPromptSubmit hook and surfaced to the human by the
   *  engine as a `hook_warning` event — never injected into the model's context. */
  systemMessage?: string;
}

export type UserPromptHookFn = (context: UserPromptContext) => Promise<UserPromptSubmitHookResult | void>;

export type NotificationHookFn = (context: NotificationContext) => Promise<void>;

/** TeammateIdle hook: observational lifecycle hook fired when a teammate agent
 *  goes idle (finished its run). Fire-and-forget — AMI surfaces the idle event
 *  with the teammate/team identity (the reference parity on the event + payload).
 */
export type TeammateIdleHookFn = (context: TeammateContext) => Promise<void>;

/** ConfigChange hook: a blocking hook can VETO a config change before it is
 *  applied (the reference's hasBlockingResult). */
export type ConfigChangeHookFn = (context: ConfigChangeContext) => Promise<{ blockingError?: string } | void>;

export type InstructionsHookFn = (context: InstructionsContext) => Promise<void>;

export type WorktreeHookFn = (context: WorktreeContext) => Promise<void>;

/** WorktreeCreate hook: a *provider* hook that can supply the filesystem path of
 *  the worktree it created, from stdout (first non-empty line) or
 *  `hookSpecificOutput.worktreePath` (the reference's executeWorktreeCreateHook).
 *  The harness uses the returned path as the created worktree instead of the
 *  default `.ami/worktrees/<slug>` location. A void return is the observe-only
 *  case (the hook materialised the default path or merely reacted). */
export type WorktreeCreateHookFn = (context: WorktreeContext) => Promise<{ worktreePath?: string } | void>;

export type FileChangeHookFn = (context: FileChangeContext) => Promise<{ watchPaths?: string[] } | void>;

export type CwdChangedHookFn = (context: CwdChangedContext) => Promise<{ watchPaths?: string[] } | void>;

/** Elicitation action a hook may return to short-circuit the user prompt. */
export const ELICITATION_ACTIONS = ['accept', 'decline', 'cancel'] as const;

export type ElicitationAction = (typeof ELICITATION_ACTIONS)[number];

export interface ElicitationHookResult {
  /** Programmatic response that short-circuits / overrides the user prompt. */
  action?: ElicitationAction;
  content?: Record<string, unknown>;
  /** Blocking error → decline the elicitation. */
  blockingError?: string;
}

export type ElicitationHookFn = (context: ElicitationContext) => Promise<ElicitationHookResult | void>;

export type StopFailureHookFn = (context: StopFailureContext) => Promise<void>;

/** Aggregated result of running Stop hooks (the reference's StopHookResult). */
export interface StopHookResult {
  /** Blocking reasons fed back into the turn as user messages. */
  blockingErrors: string[];
  /** A hook asked to prevent the turn from ending (continue the loop). */
  preventContinuation: boolean;
  /** Reason surfaced when preventContinuation is set. */
  stopReason?: string;
}

/** Stop hook: may block turn-end (blockingError) or prevent continuation. */
export type StopHookFn = (context: HookContext) => Promise<{ blockingError?: string; preventContinuation?: boolean; stopReason?: string } | void>;

/**
 * Default human-readable reason attached when a Stop hook halts the agent loop
 * without supplying its own `stopReason` (the reference's hook_stopped_continuation
 * attachment default — stopHooks.ts:L268-280 "Stop hook prevented continuation").
 * Gives the engine/operator an introspectable record of WHY the turn stopped even
 * when the hook was terse.
 */
export const DEFAULT_STOP_HOOK_PREVENT_REASON = 'Stop hook prevented continuation';

/**
 * Resolve the reason surfaced for a Stop-hook-initiated halt: the hook's own
 * `stopReason` when it provided a non-empty one, otherwise the canonical
 * {@link DEFAULT_STOP_HOOK_PREVENT_REASON} wording. Pure; used by
 * {@link HookManager.executeStop} so a prevented continuation always carries a
 * displayable reason.
 */
export function formatHookStoppedContinuation(stopReason?: string): string {
  const r = stopReason?.trim();
  return r ? r : DEFAULT_STOP_HOOK_PREVENT_REASON;
}

/**
 * User-facing message when a UserPromptSubmit hook BLOCKS a submitted prompt via
 * `blockingError` (the reference's `getUserPromptSubmitHookBlockingMessage`,
 * utils/hooks.ts:L1936-L1939 → processUserInput.ts:L193-L209). The engine surfaces
 * this and erases the user's input, so a policy/guardrail hook can refuse a prompt
 * outright with a displayable reason. Pure. The caller appends `Original prompt: …`
 * so the erased input is still visible to the user (the reference parity).
 */
export function formatUserPromptSubmitBlockedMessage(blockingError: string): string {
  return `UserPromptSubmit operation blocked by hook:\n${blockingError}`;
}

/**
 * User-facing message when a UserPromptSubmit hook STOPS a prompt via
 * `preventContinuation` (the reference processUserInput.ts:L213-L224): the prompt is
 * kept in context but the turn halts. With a non-empty `stopReason` the message is
 * `Operation stopped by hook: <reason>`, otherwise the bare `Operation stopped by
 * hook`. Pure. This intentionally differs from {@link formatHookStoppedContinuation}
 * (the Stop-hook default wording), matching the reference's two distinct messages.
 */
export function formatUserPromptStoppedMessage(stopReason?: string): string {
  const r = stopReason?.trim();
  return r ? `Operation stopped by hook: ${r}` : 'Operation stopped by hook';
}

/**
 * Event-specific wording for a Stop hook's blocking feedback re-injected into the
 * model's context (the reference's `getStopHookMessage`, utils/hooks.ts:L1894-L1896 →
 * `Stop hook feedback:\n<error>`). When a Stop hook BLOCKS turn-end with a
 * `blockingError`, the engine frames it with this prefix so the model can attribute
 * the re-injected feedback to a Stop guard, mirroring the already-framed
 * UserPromptSubmit phase ({@link formatUserPromptSubmitBlockedMessage}). Pure; the
 * error body is preserved verbatim (no trimming).
 */
export function formatStopHookBlockingMessage(blockingError: string): string {
  return `Stop hook feedback:\n${blockingError}`;
}

/** Per-hook timing + error record from a Stop-hook run (the reference's hookInfos
 *  in createStopHookSummaryMessage). One entry per Stop hook that actually ran. */
export interface StopHookInfo {
  /** Wall-clock duration of this individual Stop hook, in milliseconds. */
  durationMs: number;
  /** Error text if this Stop hook threw/failed; undefined on success. */
  error?: string;
  /** The shell command backing a command-type Stop hook (the reference's
   *  StopHookInfo.command) — present when the hook was registered from a
   *  command-hook config / with a display descriptor, so the summary can attribute
   *  a slow/failing hook to its config instead of a bare ordinal. */
  command?: string;
  /** The prompt text backing a prompt/agent-type Stop hook (the reference's
   *  StopHookInfo.promptText) — the identifier for a non-command Stop hook. */
  promptText?: string;
}

/** Display descriptor identifying a registered Stop hook in the run summary
 *  (the reference's per-hook command / promptText). Either/both may be set. */
export interface StopHookDisplay {
  command?: string;
  promptText?: string;
}

/**
 * Aggregated Stop-hook run summary emitted on the HookManager `stopHookSummary`
 * event after every Stop-hook chain (the reference's createStopHookSummaryMessage
 * inputs). Carries per-hook timings + errors + whether a hook prevented
 * continuation so a consumer can surface a "Ran N stop hook(s)" line (with the
 * wall-clock-vs-summed timing, per-hook failures, and prevented-continuation) to
 * the OPERATOR — never fed to the model. A broken/slow Stop hook is observable
 * instead of being silently swallowed into the debug log.
 */
export interface StopHookSummary {
  /** Number of Stop hooks that actually ran this chain (= hookInfos.length). */
  count: number;
  /** Per-hook timing + error records, in execution order. */
  hookInfos: StopHookInfo[];
  /** Wall-clock elapsed for the whole Stop-hook chain, in ms. the reference prefers this
   *  over the summed per-hook time because stop hooks can run back-to-back. */
  totalDurationMs: number;
  /** Error texts for the hooks that failed (subset of hookInfos), for quick display. */
  hookErrors: string[];
  /** A hook asked to prevent the turn from ending. */
  preventContinuation: boolean;
  /** Reason surfaced when preventContinuation is set. */
  stopReason?: string;
  /** Whether the chain produced any surfaced output worth showing the operator —
   *  a blocking error, a hook error, or a prevented continuation (the reference's
   *  `hasOutput` flag on createStopHookSummaryMessage). A silent, fast, non-blocking
   *  chain has `hasOutput:false` and need not be rendered. */
  hasOutput: boolean;
}

/**
 * Render an operator-facing one-shot summary line for a completed Stop-hook chain
 * (the reference's stop-hook run summary — stopHooks.ts createStopHookSummaryMessage):
 * the hook count, the wall-clock total, each hook that was slow (over
 * {@link HOOK_TIMING_DISPLAY_THRESHOLD_MS}) or errored with its duration, every
 * error line, and a prevented-continuation note. NEVER fed to the model — this is a
 * user/operator diagnostic so a broken or slow Stop hook is observable instead of
 * silently swallowed. Returns an empty string when the summary has nothing worth
 * surfacing ({@link StopHookSummary.hasOutput} false AND the chain ran under the
 * display threshold). Pure; consumed by {@link HookManager.getLastStopHookSummaryMessage}.
 */
/** Collapse a Stop hook's command/promptText to a compact single-line identifier
 *  for a summary row: whitespace-folded and length-capped so a multi-line prompt
 *  does not blow out the operator summary. Pure. */
export function stopHookInfoLabel(id: string): string {
  const flat = id.replace(/\s+/g, ' ').trim();
  return flat.length > 60 ? `${flat.slice(0, 57)}...` : flat;
}
