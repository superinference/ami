import * as fs from 'fs';
import { resolveConfigDir } from './reference-journal';
import * as path from 'path';
import * as os from 'os';
import { spawn } from 'child_process';
import type { Message, AssistantMessage } from './types';
import { log as coreLog } from './logger';
import { EventEmitter } from 'events';
import {
  canHookEvent,
  shouldSkipHookDueToTrust,
  canRunHookUnderManagedOnly,
  filterManagedOnlyHooks,
  type HookSourceKind,
} from './hook-protection';
import { isRestrictedToPluginOnly } from './plugin-only-policy';
import { stopFailureErrorClass, STOP_FAILURE_ERROR_CLASSES, type StopFailureErrorClass } from './error-classifier';
import { stripBom, resolveWorkspaceTrustAccepted } from './config';
import { clearCwdEnvFiles, invalidateSessionEnvCache, sessionEnvDirPath, hookEnvFragmentPath } from './session-environment';
import { verdictRank, wildcardMatch, PERMISSION_TOOL_ALIASES } from './permissions';
import type { PermissionUpdate, PermissionMode } from './permissions';
import { validateUrlSafety, pinnedDnsLookup } from './tools/web-utils';
import { getSmallFastModel } from './model-capabilities';
import { isEnvTruthy } from './privacy-level';

// ---------------------------------------------------------------------------
// Hook output sanitization — strip system-reminder tags to prevent injection
// ---------------------------------------------------------------------------

const SYSTEM_TAG_RE = /<\/?system-reminder>/gi;
export function sanitizeHookOutput(output: string): string {
  return output.replace(SYSTEM_TAG_RE, '[tag-escaped]');
}

// ---------------------------------------------------------------------------
// Hook environment sanitization — strip sensitive keys (API keys, tokens,
// secrets) so project-scoped hooks cannot exfiltrate credentials.
// ---------------------------------------------------------------------------

export const SENSITIVE_ENV_PATTERNS: ReadonlyArray<RegExp> = [
  /^ANTHROPIC_API_KEY$/i,
  /^OPENAI_API_KEY$/i,
  /^GOOGLE_API_KEY$/i,
  /^AZURE_API_KEY$/i,
  /^AWS_SECRET_ACCESS_KEY$/i,
  /^AWS_SESSION_TOKEN$/i,
  /^GITHUB_TOKEN$/i,
  /^GH_TOKEN$/i,
  /^NPM_TOKEN$/i,
  /^HF_TOKEN$/i,
  /^HUGGING_FACE_HUB_TOKEN$/i,
  /^REPLICATE_API_TOKEN$/i,
  /^DATABASE_URL$/i,
  /^REDIS_URL$/i,
  /^SSH_AUTH_SOCK$/i,
  /_API_KEY$/i,
  /_SECRET$/i,
  /_TOKEN$/i,
  /_PASSWORD$/i,
  /_CREDENTIALS?$/i,
  /_PRIVATE_KEY$/i,
  /_SECRET_/i,
  /_AUTH_TOKEN$/i,
];

export function sanitizeHookEnv(env: Record<string, string | undefined>): Record<string, string> {
  const clean: Record<string, string> = {};
  for (const [key, value] of Object.entries(env)) {
    if (value !== undefined && !SENSITIVE_ENV_PATTERNS.some(p => p.test(key))) {
      clean[key] = value;
    }
  }
  return clean;
}

export const HOOK_EVENTS = [
  'preToolUse', 'postToolUse', 'preSampling', 'postSampling',
  'stop', 'error', 'sessionStart', 'sessionEnd',
  'subagentStart', 'subagentStop', 'preCompact', 'postCompact',
  'permissionRequest', 'permissionDenied', 'userPromptSubmit',
  'taskCreated', 'taskCompleted', 'teammateIdle', 'postToolUseFailure', 'notification',
  'setup', 'configChange', 'instructionsLoaded',
  'worktreeCreate', 'worktreeRemove', 'fileChanged', 'cwdChanged',
  'elicitation', 'elicitationResult', 'stopFailure',
] as const;
export type HookEvent = (typeof HOOK_EVENTS)[number];

export const HOOK_CONFIG_TYPES = ['command', 'prompt', 'http', 'agent'] as const;
export type HookConfigType = (typeof HOOK_CONFIG_TYPES)[number];

export type HookFn = (context: HookContext) => Promise<void>;

export interface HookContext {
  messages: Message[];
  lastAssistantMessage?: AssistantMessage;
  toolResults?: Array<{ toolName: string; output: string; isError: boolean }>;
  turnCount: number;
  /** Whether this Stop hook is firing as a RE-ENTRANT continuation (a previous
   *  Stop hook blocked turn-end and the engine re-entered the loop), serialized
   *  into the Stop hook stdin as `stop_hook_active` so a hook can detect — and
   *  break — a block→retry recursion (Claude Code's StopHookInput.stop_hook_active). */
  stopHookActive?: boolean;
  /** Parent cancellation signal (user Ctrl-C / turn abort). Threaded into
   *  command/http/prompt/agent hook execution so a long-running hook is torn
   *  down when the surrounding operation is cancelled, combined with the
   *  per-hook timeout. */
  abortSignal?: AbortSignal;
  /** Origin of the model-sampling call that triggered this hook (Claude Code's
   *  MagicDocs `querySource` discriminator). `'main'` = the top-level REPL/agent
   *  loop; `'subagent'` = a forked/nested agent's own sampling. Surfaced to
   *  pre/postSampling hooks (as the matcher field + serialized `sampling_origin`)
   *  so a hook that itself spawns a sampling call can EXCLUDE background/forked
   *  samplings and avoid re-entrant/recursive work (MagicDocs L222-224:
   *  `if (querySource !== 'repl_main_thread') return`). */
  samplingOrigin?: SamplingOrigin;
}

/** Allowed {@link HookContext.samplingOrigin} discriminators — the matcher value
 *  set for pre/postSampling hooks (Claude Code's query-source boundary). */
export const SAMPLING_ORIGINS = ['main', 'subagent'] as const;
export type SamplingOrigin = (typeof SAMPLING_ORIGINS)[number];

/** Narrow an arbitrary value to a {@link SamplingOrigin}. */
export function isSamplingOrigin(v: unknown): v is SamplingOrigin {
  return typeof v === 'string' && (SAMPLING_ORIGINS as readonly string[]).includes(v);
}

// ---------------------------------------------------------------------------
// Extended hook context types
// ---------------------------------------------------------------------------

export interface PreToolUseContext extends HookContext {
  toolName: string;
  toolInput: Record<string, unknown>;
}

export interface PostToolUseContext extends HookContext {
  toolName: string;
  toolInput: Record<string, unknown>;
  toolOutput: string;
  isError: boolean;
  /** Elapsed time for the tool execution in milliseconds. */
  durationMs?: number;
}

export interface PreSamplingContext extends HookContext {
  apiMessages: Message[];
}

export type HookDecision =
  | { action: 'allow'; additionalContext?: string }
  | { action: 'deny'; reason: string; additionalContext?: string }
  // Claude Code's PreToolUse `permissionDecision: 'ask'` — the hook does not
  // allow or deny outright but FORCES an interactive permission confirmation,
  // optionally with its own `reason` (shown at the prompt) and `updatedInput`
  // (the input the user is asked to approve). The consumer (tool-executor) must
  // surface the prompt even when a standing rule / auto-allow would have skipped
  // it. Without this arm a hook's `ask` silently downgrades to `allow`.
  | { action: 'ask'; reason?: string; updatedInput?: Record<string, unknown>; additionalContext?: string }
  | { action: 'modify'; updatedInput?: Record<string, unknown>; updatedMessages?: Message[]; additionalContext?: string };

// ---------------------------------------------------------------------------
// Hook function types for new hooks
// ---------------------------------------------------------------------------

export type PreToolUseHookFn = (context: PreToolUseContext) => Promise<HookDecision>;
/**
 * PostToolUse hook result. Beyond context/output mutation, a PostToolUse hook
 * can BLOCK (feed a blocking error back to the model) or PREVENT CONTINUATION
 * (halt the agent loop after the tool ran), matching Claude Code's
 * toolHooks `{ blockingError, preventContinuation, stopReason }`.
 */
export interface PostToolUseHookResult {
  additionalContext?: string;
  updatedToolOutput?: string;
  blockingError?: string;
  preventContinuation?: boolean;
  stopReason?: string;
}
export type PostToolUseHookFn = (context: PostToolUseContext) => Promise<PostToolUseHookResult | void>;
export type PreSamplingHookFn = (context: PreSamplingContext) => Promise<HookDecision>;

/**
 * Allowed SessionStart `source` discriminators (Claude Code's
 * SessionStartHookInputSchema `source` enum). Surfaced to a sessionStart hook as
 * the matcher field so a hook can scope to e.g. only `resume`.
 */
export const SESSION_START_SOURCES = ['startup', 'resume', 'clear', 'compact'] as const;
export type SessionStartSource = (typeof SESSION_START_SOURCES)[number];
/** Narrow an arbitrary value to a known SessionStart source. */
export function isSessionStartSource(v: unknown): v is SessionStartSource {
  return typeof v === 'string' && (SESSION_START_SOURCES as readonly string[]).includes(v);
}

/**
 * Session-exit-reason classification for the SessionEnd hook (Claude Code's
 * EXIT_REASONS / ExitReasonSchema). This is the SessionEnd `reason` discriminator
 * — distinct from the engine-loop `EXIT_REASONS` in types.ts (which classifies why
 * the agent turn-loop terminated). Enumerated in the sessionEnd introspection
 * metadata so `/hooks` and `--introspect` show the allowed values.
 */
export const SESSION_END_REASONS = [
  'clear', 'resume', 'logout', 'prompt_input_exit', 'other', 'bypass_permissions_disabled',
] as const;
export type SessionEndReason = (typeof SESSION_END_REASONS)[number];
/** Narrow an arbitrary value to a known SessionEnd reason. */
export function isSessionEndReason(v: unknown): v is SessionEndReason {
  return typeof v === 'string' && (SESSION_END_REASONS as readonly string[]).includes(v);
}

/**
 * Setup hook `trigger` discriminator (Claude Code's SetupHookInputSchema
 * `trigger` enum): `init` (first-run setup) vs `maintenance` (periodic). Used as
 * the setup matcher field so a hook can scope to just one trigger.
 */
export const SETUP_TRIGGERS = ['init', 'maintenance'] as const;
export type SetupTrigger = (typeof SETUP_TRIGGERS)[number];
/** Narrow an arbitrary value to a known setup trigger. */
export function isSetupTrigger(v: unknown): v is SetupTrigger {
  return typeof v === 'string' && (SETUP_TRIGGERS as readonly string[]).includes(v);
}

export interface SessionContext {
  sessionId: string;
  cwd: string;
  /** Why the SessionStart hook is firing — Claude Code's `source` discriminator
   *  (startup | resume | clear | compact). A `matcher` on a sessionStart hook is
   *  tested against this (HOOK_MATCHER_FIELD.sessionStart = 'source') so a hook
   *  can scope to e.g. only `resume`. */
  source?: string;
  /** Why the SessionEnd hook is firing — Claude Code's `reason` discriminator
   *  (clear | logout | prompt_input_exit | other). A `matcher` on a sessionEnd
   *  hook is tested against this (HOOK_MATCHER_FIELD.sessionEnd = 'reason'). */
  reason?: string;
  /** Why the Setup hook is firing — Claude Code's `trigger` discriminator
   *  (init | maintenance). A `matcher` on a setup hook is tested against this
   *  (HOOK_MATCHER_FIELD.setup = 'trigger'). */
  trigger?: string;
  /** Active model id, forwarded to SessionStart/Setup hooks in the stdin payload
   *  (Claude Code's SessionStart `model`). */
  model?: string;
  /** Agent type the session is running as, forwarded to SessionStart hooks as
   *  `agent_type` (Claude Code's SessionStart `agent_type`). */
  agentType?: string;
}

export interface SubagentContext {
  /** The INVOKING (parent) engine's session id — lets a hook correlate the
   *  subagent boundary back to its real parent across concurrent/nested runs
   *  (Claude Code parity: SubagentContext.parentSessionId). */
  parentSessionId: string;
  subagentSessionId: string;
  prompt: string;
  mode?: string;
  /** Parent cancellation signal (user Ctrl-C / turn abort). When aborted, the
   *  SubagentStop hook chain stops immediately (Claude Code's abort-signal
   *  cancellation of in-flight Stop/SubagentStop hook chains). Optional so
   *  non-cancellable call sites may omit it. */
  abortSignal?: AbortSignal;
  /** Whether this boundary is a fresh `spawn` or a `resume` of an existing agent
   *  session (Claude Code parity: invocationKind). Absent in bare/test contexts. */
  invocationKind?: 'spawn' | 'resume';
}

export interface CompactContext {
  messageCount: number;
  tokenEstimate: number;
  /** Whether compaction was auto-triggered (token pressure) or user-requested
   *  (`/compact`). Surfaced to PreCompact hooks (Claude Code parity). */
  trigger?: 'auto' | 'manual';
  /** The user-supplied custom compaction instructions a PreCompact hook may
   *  read and extend/override via `newCustomInstructions`. */
  customInstructions?: string | null;
}

export interface PermissionContext {
  toolName: string;
  toolInput: Record<string, unknown>;
  command?: string;
  reason?: string;
  retry?: boolean;
  /** Active permission posture (`ask` | `deny-all` | `auto-allow` | `plan`) so a
   *  PermissionRequest hook can branch on the mode when deciding — Claude Code's
   *  CoordinatorPermissionParams.permissionMode threaded into `runHooks`. */
  permissionMode?: PermissionMode;
  /** Permission-rule updates the engine is already suggesting for this call; a
   *  hook may inspect them before returning its own `updatedPermissions`. Mirrors
   *  Claude's `suggestions: PermissionUpdate[]` param to `runHooks`. */
  suggestions?: PermissionUpdate[];
  /** Turn abort signal — threaded in so a slow/hanging in-process permission hook
   *  (and a command-hook subprocess) is cancelled when the user interrupts (Esc /
   *  Ctrl-C). Parity with Claude threading `abortController.signal` into
   *  `executePermissionRequestHooks` / the classifier. */
  abortSignal?: AbortSignal;
}

export interface TaskEventContext {
  taskId: string;
  subject: string;
  status?: string;
  description?: string;
  /** Abort/cancellation boundary (Claude Code parity): the surrounding tool call's
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
 * Context for the TeammateIdle hook (Claude Code's TeammateIdleHookInputSchema —
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
   * `User` | `Managed` (parity: Claude Code's InstructionsLoaded `memory_type`,
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
   * The matchable failure CLASS (Claude Code's StopFailure `error` discriminator),
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
}

/** SessionStart/Setup hook result. A hook may inject a single `initialUserMessage`
 *  OR, matching Claude Code's SessionStart hook result (an ARRAY of injectable
 *  messages, not a single string), multiple messages via `initialUserMessages`.
 *  executeSessionStart aggregates ALL messages from ALL hooks (not last-wins). */
export type SessionHookFn = (context: SessionContext) => Promise<{ initialUserMessage?: string; initialUserMessages?: string[]; watchPaths?: string[] } | void>;
export type SubagentHookFn = (context: SubagentContext) => Promise<void>;
/** SubagentStart hook: may contribute additionalContexts prepended to the
 *  subagent's initial messages (Claude Code parity). */
export type SubagentStartHookFn = (context: SubagentContext) => Promise<{ additionalContexts?: string[] } | void>;
export type CompactHookFn = (context: CompactContext) => Promise<void>;
/** PreCompact hook: may inject/override compaction instructions and surface a
 *  user-facing display message (Claude Code parity). */
export type PreCompactHookFn = (context: CompactContext) => Promise<{ newCustomInstructions?: string; userDisplayMessage?: string } | void>;
/**
 * Result a PermissionRequest hook may return (Claude Code's PermissionDecision
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
}

export type PermissionHookFn = (context: PermissionContext) => Promise<PermissionHookResult | void>;
/** TaskCreated hook: like a TaskCompleted hook, a blocking TaskCreated hook can
 *  VETO a just-created task by returning a blockingError; the task tool then rolls
 *  back the created task and surfaces the reason (Claude Code parity —
 *  TaskCreateTool's taskCreated-hook blockingError + compensating deleteTask). A
 *  plain void return is the non-blocking case. */
export type TaskEventHookFn = (context: TaskEventContext) => Promise<{ blockingError?: string } | void>;
/** TaskCompleted hook: a blocking hook can veto a status→completed transition
 *  by returning a blockingError (Claude Code parity). */
export type TaskCompletedHookFn = (context: TaskEventContext) => Promise<{ blockingError?: string } | void>;
/** UserPromptSubmit hook result: may inject context, or BLOCK/deny the prompt
 *  (blockingError) / prevent continuation, matching Claude Code. */
export interface UserPromptSubmitHookResult {
  additionalContext?: string;
  blockingError?: string;
  preventContinuation?: boolean;
  stopReason?: string;
}
export type UserPromptHookFn = (context: UserPromptContext) => Promise<UserPromptSubmitHookResult | void>;
export type NotificationHookFn = (context: NotificationContext) => Promise<void>;
/** TeammateIdle hook: observational lifecycle hook fired when a teammate agent
 *  goes idle (finished its run). Fire-and-forget — AMI surfaces the idle event
 *  with the teammate/team identity (Claude Code parity on the event + payload).
 */
export type TeammateIdleHookFn = (context: TeammateContext) => Promise<void>;
/** ConfigChange hook: a blocking hook can VETO a config change before it is
 *  applied (Claude Code's hasBlockingResult). */
export type ConfigChangeHookFn = (context: ConfigChangeContext) => Promise<{ blockingError?: string } | void>;
export type InstructionsHookFn = (context: InstructionsContext) => Promise<void>;
export type WorktreeHookFn = (context: WorktreeContext) => Promise<void>;
/** WorktreeCreate hook: a *provider* hook that can supply the filesystem path of
 *  the worktree it created, from stdout (first non-empty line) or
 *  `hookSpecificOutput.worktreePath` (Claude Code's executeWorktreeCreateHook).
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

/** Aggregated result of running Stop hooks (Claude Code's StopHookResult). */
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

/** Per-hook timing + error record from a Stop-hook run (Claude Code's hookInfos
 *  in createStopHookSummaryMessage). One entry per Stop hook that actually ran. */
export interface StopHookInfo {
  /** Wall-clock duration of this individual Stop hook, in milliseconds. */
  durationMs: number;
  /** Error text if this Stop hook threw/failed; undefined on success. */
  error?: string;
}

/**
 * Aggregated Stop-hook run summary emitted on the HookManager `stopHookSummary`
 * event after every Stop-hook chain (Claude Code's createStopHookSummaryMessage
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
  /** Wall-clock elapsed for the whole Stop-hook chain, in ms. Claude prefers this
   *  over the summed per-hook time because stop hooks can run back-to-back. */
  totalDurationMs: number;
  /** Error texts for the hooks that failed (subset of hookInfos), for quick display. */
  hookErrors: string[];
  /** A hook asked to prevent the turn from ending. */
  preventContinuation: boolean;
  /** Reason surfaced when preventContinuation is set. */
  stopReason?: string;
}

/**
 * Minimum wall-clock duration (ms) a Stop-hook chain must exceed before its
 * summary is worth DISPLAYING when there were no errors / no prevented
 * continuation (Claude Code's HOOK_TIMING_DISPLAY_THRESHOLD_MS): a fast no-op
 * Stop hook does not deserve a summary row, but a slow one (or one with errors)
 * does. Consumed by the summary surface so the display threshold matches parity.
 */
export const HOOK_TIMING_DISPLAY_THRESHOLD_MS = 500;

/**
 * A settings/hooks/config file that failed to parse while loading hooks. Surfaced
 * (via {@link HookManager.getConfigErrors} + the `configError` event + `/hooks`)
 * so a malformed file — which silently disables EVERY hook it would contribute —
 * is visible to the user instead of only hitting the debug log (Claude Code's
 * "a broken settings.json silently disables ALL settings from that file" warning).
 */
export interface HookConfigError {
  /** Absolute path of the file that failed to parse. */
  path: string;
  /** The parse/validation error text. */
  error: string;
}

// ---------------------------------------------------------------------------
// Hook concurrency limiter — at most N of one event execute at a time
// ---------------------------------------------------------------------------

export const MAX_CONCURRENT_PER_EVENT = 12;

/**
 * Default per-callback timeout for in-process ('function') hook callbacks —
 * matches Claude Code's addFunctionHook default. A slow/stuck in-process
 * callback cannot stall the turn indefinitely; after this deadline the callback
 * is abandoned (JS callbacks are not forcibly cancellable) and logged.
 */
export const DEFAULT_CALLBACK_HOOK_TIMEOUT_MS = 5_000;

/**
 * Canonical timeout wrapper for in-process (function) hook callbacks: race the
 * work against a deadline and reject with a labelled timeout Error if it fires.
 * The underlying JS callback cannot be forcibly cancelled, but the caller stops
 * awaiting it. Shared so no call site re-implements Promise.race timeouts.
 */
export function runWithTimeout<T>(work: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
    work.then(
      v => { clearTimeout(timer); resolve(v); },
      e => { clearTimeout(timer); reject(e); },
    );
  });
}

/**
 * Race an in-process (function) hook callback against an abort signal so a
 * user interrupt (Esc / Ctrl-C) stops the caller awaiting a slow/hanging
 * callback — the JS callback itself cannot be forcibly cancelled, but the caller
 * stops waiting and the signal short-circuits the remaining callbacks. Parity
 * with Claude threading `abortController.signal` into its permission-hook loop.
 * No signal → the work passes through unchanged. Shared so no call site
 * re-implements the abort Promise.race.
 */
export function raceAbort<T>(work: Promise<T>, signal: AbortSignal | undefined): Promise<T> {
  if (!signal) return work;
  if (signal.aborted) return Promise.reject(new Error('aborted'));
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(new Error('aborted'));
    signal.addEventListener('abort', onAbort, { once: true });
    work.then(
      v => { signal.removeEventListener('abort', onAbort); resolve(v); },
      e => { signal.removeEventListener('abort', onAbort); reject(e); },
    );
  });
}

export class EventConcurrencyLimiter {
  private active = new Map<string, number>();
  private waiters = new Map<string, Array<() => void>>();

  constructor(private maxConcurrent = MAX_CONCURRENT_PER_EVENT) {}

  async acquire(event: string): Promise<void> {
    const current = this.active.get(event) ?? 0;
    if (current < this.maxConcurrent) {
      this.active.set(event, current + 1);
      return;
    }
    await new Promise<void>(resolve => {
      const queue = this.waiters.get(event) ?? [];
      queue.push(resolve);
      this.waiters.set(event, queue);
    });
  }

  release(event: string): void {
    const current = this.active.get(event) ?? 1;
    const queue = this.waiters.get(event);
    if (queue && queue.length > 0) {
      const next = queue.shift()!;
      if (queue.length === 0) this.waiters.delete(event);
      next();
    } else {
      const next = current - 1;
      if (next <= 0) this.active.delete(event);
      else this.active.set(event, next);
    }
  }

  getActive(event: string): number {
    return this.active.get(event) ?? 0;
  }

  getWaiting(event: string): number {
    return this.waiters.get(event)?.length ?? 0;
  }
}

// ---------------------------------------------------------------------------
// Hook validation and safety
// ---------------------------------------------------------------------------

export const MAX_HOOK_TIMEOUT_MS = 600_000; // 10 min — matches Claude Code's hook ceiling
export const DEFAULT_HOOK_TIMEOUT_MS = 60_000; // CC default when a hook sets no timeout
/** Default lifetime budget for an async ('background') hook when it sets no
 *  `asyncTimeout` — 15s, matching Claude Code's dedicated background-hook deadline.
 *  A background hook must NOT inherit the long synchronous command/HTTP default
 *  (60s / 10min): it runs detached from the turn, so it is bounded on a short,
 *  distinct clock and aborted when exceeded (child killed / request aborted). In
 *  milliseconds (clamped via validateHookTimeout). */
export const DEFAULT_ASYNC_HOOK_TIMEOUT_MS = 15_000;
/** Default HTTP-hook deadline when the hook sets no `timeout` — 10 minutes,
 *  matching Claude Code's DEFAULT_HTTP_HOOK_TIMEOUT_MS (= TOOL_HOOK_EXECUTION_
 *  TIMEOUT_MS). A webhook round-trip may legitimately be slow; a too-short
 *  default (previously 10s) spuriously failed/denied slow hooks. */
export const DEFAULT_HTTP_HOOK_TIMEOUT_MS = 10 * 60 * 1000;
export const MAX_HOOKS_PER_EVENT = 10;

/** Default execution budget for SessionEnd hooks (Claude Code's
 *  SESSION_END_HOOK_TIMEOUT_MS_DEFAULT). SessionEnd hooks run during
 *  shutdown/clear and must NOT block teardown, so they get a deliberately tight
 *  default — far below DEFAULT_HOOK_TIMEOUT_MS (60s) — overridable via the
 *  CLAUDE_CODE_SESSIONEND_HOOKS_TIMEOUT_MS env var. A slow or hanging teardown
 *  hook would otherwise stall process exit for up to a minute. */
export const DEFAULT_SESSIONEND_HOOK_TIMEOUT_MS = 1_500;

/** Resolve the SessionEnd hook execution budget (ms): honor the
 *  CLAUDE_CODE_SESSIONEND_HOOKS_TIMEOUT_MS env override when it is a positive
 *  integer, else fall back to DEFAULT_SESSIONEND_HOOK_TIMEOUT_MS. The result is
 *  clamped to [100ms, MAX_HOOK_TIMEOUT_MS] so a hostile/typo value can neither
 *  make the budget unusably small nor exceed the global hook ceiling. */
export function getSessionEndHookTimeoutMs(
  env: Record<string, string | undefined> = process.env,
): number {
  const raw = env.CLAUDE_CODE_SESSIONEND_HOOKS_TIMEOUT_MS;
  const parsed = raw !== undefined ? Number.parseInt(raw, 10) : NaN;
  const ms = Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_SESSIONEND_HOOK_TIMEOUT_MS;
  return Math.max(100, Math.min(ms, MAX_HOOK_TIMEOUT_MS));
}

/** Hard turn-count cap on an agent (multi-turn LLM) hook, independent of the
 *  timeout, so a verification agent cannot loop indefinitely even within budget
 *  (Claude Code's MAX_AGENT_TURNS). Passed to the hook sub-engine. */
export const MAX_AGENT_TURNS = 50;

/** Cap on hook-contributed context injected into the turn (Claude Code's
 *  MAX_HOOK_OUTPUT_LENGTH). A UserPromptSubmit hook that emits a huge blob is
 *  truncated with a marker so it cannot blow out the context window. */
export const MAX_HOOK_OUTPUT_LENGTH = 10_000;

/** Truncate hook-contributed output to MAX_HOOK_OUTPUT_LENGTH, appending the
 *  Claude-parity marker when it overflows. Shared so no site re-implements it. */
export function truncateHookOutput(content: string): string {
  if (content.length > MAX_HOOK_OUTPUT_LENGTH) {
    return `${content.substring(0, MAX_HOOK_OUTPUT_LENGTH)}… [output truncated - exceeded ${MAX_HOOK_OUTPUT_LENGTH} characters]`;
  }
  return content;
}

export const BLOCKED_HOOK_COMMANDS: ReadonlySet<string> = new Set([
  'rm -rf /',
  'mkfs',
  'dd if=',
  ':(){',
  'fork bomb',
  'shutdown',
  'reboot',
  'init 0',
  'init 6',
  'halt',
]);

const DEVICE_WRITE_RE = />\s*\/dev\/[a-z]/;
const SYSTEM_CHMOD_RE = /chmod\s+777\s+\/(etc|usr|bin|sbin)/;
const MAX_HOOK_COMMAND_LENGTH = 4096;

export function validateHookCommand(command: string): { valid: boolean; reason?: string } {
  if (!command || !command.trim()) {
    return { valid: false, reason: 'Hook command must not be empty' };
  }
  if (command.length > MAX_HOOK_COMMAND_LENGTH) {
    return { valid: false, reason: `Hook command exceeds maximum length of ${MAX_HOOK_COMMAND_LENGTH} characters` };
  }
  for (const blocked of BLOCKED_HOOK_COMMANDS) {
    if (command.includes(blocked)) {
      return { valid: false, reason: `Hook command contains blocked pattern: ${blocked}` };
    }
  }
  if (DEVICE_WRITE_RE.test(command)) {
    return { valid: false, reason: 'Hook command must not write to device files' };
  }
  if (SYSTEM_CHMOD_RE.test(command)) {
    return { valid: false, reason: 'Hook command must not chmod 777 system directories' };
  }
  return { valid: true };
}

export function validateHookTimeout(timeoutMs: number): number {
  return Math.max(100, Math.min(timeoutMs, MAX_HOOK_TIMEOUT_MS));
}

/**
 * Default interpreter for command hooks when none is configured — matches
 * Claude Code's DEFAULT_HOOK_SHELL. A hook may override it per-entry via the
 * `shell` config field (bash | sh | powershell | pwsh).
 */
export const DEFAULT_HOOK_SHELL = 'bash';

/** Interpreter selectors a command hook (or the default-shell override) may name. */
const HOOK_SHELL_SELECTORS: ReadonlySet<string> = new Set(['bash', 'sh', 'powershell', 'pwsh']);

/**
 * Resolve the DEFAULT command-hook interpreter when a hook sets no `shell`.
 * Honors the `SUPERINFERENCE_HOOK_SHELL` env override (bash | sh | powershell |
 * pwsh) so a deployment can change the default interpreter (Claude Code's
 * configurable DEFAULT_HOOK_SHELL) — e.g. a Windows host that wants `pwsh` by
 * default. An unset/unrecognized value falls back to DEFAULT_HOOK_SHELL.
 */
export function resolveDefaultHookShell(env: Record<string, string | undefined> = process.env): string {
  const raw = (env.SUPERINFERENCE_HOOK_SHELL ?? '').trim().toLowerCase();
  return HOOK_SHELL_SELECTORS.has(raw) ? raw : DEFAULT_HOOK_SHELL;
}

/**
 * POSIX single-quote a string so it embeds safely as one argument inside a
 * `sh -c` / `bash -c` command line (any embedded single quote is closed,
 * escaped, and reopened).
 */
export function posixSingleQuote(s: string): string {
  return `'` + s.split(`'`).join(`'\\''`) + `'`;
}

/**
 * Wrap a hook command with an outer shell prefix (Claude Code's
 * formatShellPrefixCommand / CLAUDE_CODE_SHELL_PREFIX). The prefix may carry a
 * trailing `-flag` run (split on the last ` -`); the executable and the wrapped
 * command are POSIX-quoted so neither can break out of the prefix invocation.
 *   "firejail"            + "echo hi" → 'firejail' 'echo hi'
 *   "/usr/bin/env -i"     + "echo hi" → '/usr/bin/env' -i 'echo hi'
 */
export function formatShellPrefixCommand(prefix: string, command: string): string {
  const spaceBeforeDash = prefix.lastIndexOf(' -');
  if (spaceBeforeDash > 0) {
    const execPath = prefix.substring(0, spaceBeforeDash);
    const args = prefix.substring(spaceBeforeDash + 1);
    return `${posixSingleQuote(execPath)} ${args} ${posixSingleQuote(command)}`;
  }
  return `${posixSingleQuote(prefix)} ${posixSingleQuote(command)}`;
}

/** Options for resolveHookShell — injectable env/platform for testability. */
export interface ResolveHookShellOpts {
  env?: Record<string, string | undefined>;
  platform?: NodeJS.Platform;
}

/**
 * Resolve a command hook's `shell` selector to a concrete spawn descriptor
 * (Claude Code's per-hook shell selection: `hook.shell ?? DEFAULT_HOOK_SHELL`).
 * PowerShell hooks spawn `pwsh -NoProfile -NonInteractive -Command <command>`
 * (non-interactive, no profile — matching the reference), every POSIX shell
 * uses `<shell> -c <command>`. Single source of truth so command execution
 * never hard-codes `bash`.
 *
 * Two Claude-Code command-construction behaviors are applied for POSIX shells:
 *  - Windows `.sh` fallback: on win32 a bare `.sh` script won't execute without
 *    an explicit interpreter, so `bash ` is auto-prepended.
 *  - `CLAUDE_CODE_SHELL_PREFIX`: when set, the command is wrapped by the prefix
 *    (POSIX-quoted). PowerShell ignores the prefix (its quoting differs), matching
 *    the reference.
 */
export function resolveHookShell(
  command: string,
  shell: string | undefined,
  opts: ResolveHookShellOpts = {},
): { file: string; args: string[] } {
  const env = opts.env ?? process.env;
  const platform = opts.platform ?? process.platform;
  const sel = (shell ?? resolveDefaultHookShell(env)).toLowerCase();
  if (sel === 'powershell' || sel === 'pwsh') {
    return { file: 'pwsh', args: ['-NoProfile', '-NonInteractive', '-Command', command] };
  }
  const file = sel === 'sh' ? 'sh' : 'bash';
  let finalCommand = command;
  // Windows (bash) fallback: auto-prepend `bash ` for a .sh script so it
  // executes instead of opening in the default file handler (Claude Code).
  if (platform === 'win32' && /\.sh(\s|$|")/.test(command.trim()) && !command.trim().startsWith('bash ')) {
    finalCommand = `bash ${command}`;
  }
  // CLAUDE_CODE_SHELL_PREFIX wraps the command via POSIX quoting (not applied to
  // PowerShell above — its quoting model differs; matches the reference).
  const prefix = env.CLAUDE_CODE_SHELL_PREFIX;
  if (prefix) finalCommand = formatShellPrefixCommand(prefix, finalCommand);
  return { file, args: ['-c', finalCommand] };
}

/**
 * Validate a hook's working directory before spawning. A stale/removed cwd
 * (e.g. a deleted worktree the session was launched from) makes `spawn` throw
 * ENOENT, which for a guardrail hook is indistinguishable from a real failure.
 * Claude Code pre-checks the hook cwd and falls back; this mirrors that by
 * returning the home directory when the configured cwd no longer exists.
 * Single source so every spawn path validates once.
 */
export function resolveHookCwd(cwd: string): string {
  try {
    if (cwd && fs.statSync(cwd).isDirectory()) return cwd;
  } catch { /* missing / inaccessible — fall back below */ }
  return os.homedir();
}

/**
 * Claude Code's hook `timeout` field is in SECONDS for EVERY hook type
 * (command/prompt/http/agent). This resolves a config value (seconds) to a
 * validated millisecond deadline, falling back to a per-type default when unset.
 * Single source of truth so prompt/http/agent no longer treat `timeout` as raw
 * milliseconds (which turned a Claude-compatible `timeout: 30` into 30 ms).
 */
export function resolveHookTimeoutMs(timeoutSeconds: number | undefined, defaultMs: number): number {
  return validateHookTimeout(timeoutSeconds != null ? timeoutSeconds * 1000 : defaultMs);
}

/**
 * Combine an optional parent AbortSignal with a hard timeout into one signal.
 * Mirrors Claude Code's createCombinedAbortSignal: the returned signal aborts
 * when EITHER the parent aborts (user Ctrl-C / turn cancel) OR the timeout
 * fires, so a stalled hook (command/http/model call) is actively torn down
 * instead of relying on a cooperative between-events deadline. `timedOut()`
 * distinguishes a deadline abort from a parent/user cancellation.
 */
export function combineHookAbort(
  parent: AbortSignal | undefined,
  timeoutMs: number,
): { controller: AbortController; signal: AbortSignal; cleanup: () => void; timedOut: () => boolean; aborted: () => boolean } {
  const ac = new AbortController();
  let didTimeout = false;
  const timer = setTimeout(() => { didTimeout = true; ac.abort(); }, timeoutMs);
  const onParent = () => ac.abort();
  if (parent) {
    if (parent.aborted) ac.abort();
    else parent.addEventListener('abort', onParent, { once: true });
  }
  return {
    controller: ac,
    signal: ac.signal,
    cleanup: () => { clearTimeout(timer); if (parent) parent.removeEventListener('abort', onParent); },
    timedOut: () => didTimeout,
    aborted: () => ac.signal.aborted,
  };
}

/**
 * Tools a hook-spawned agent/prompt engine must NOT get — Claude Code filters a
 * stop/verification hook agent's toolset through ALL_AGENT_DISALLOWED_TOOLS so it
 * cannot recursively spawn subagents, launch a workflow, enter the interactive
 * plan mode, block on an AskUserQuestion (no user answers a hook), or reach into
 * the parent's background tasks. This is the hook superset: the canonical
 * sub-agent denylist (ALL_AGENT_DISALLOWED_TOOLS in tools/task.ts —
 * plan_mode/workflow/AskUserQuestion/task_output/task_kill) PLUS `task` itself
 * (sub-agents may nest, but a hook agent must not spawn ANY subagent). It is
 * duplicated verbatim here rather than imported because tools/task.ts →
 * tools/index.ts → hooks.ts is a module cycle; keep the two in sync.
 */
export const HOOK_AGENT_DISALLOWED_TOOLS: readonly string[] = [
  'task', 'plan_mode', 'workflow', 'AskUserQuestion', 'task_output', 'task_kill',
];

/**
 * Three-state HTTP-hook URL allowlist check (Claude Code semantics):
 *   undefined  → no restriction (allow)
 *   []         → block everything
 *   non-empty  → the URL must match at least one `*`-wildcard pattern
 * Uses the canonical glob matcher shared with MCP/permission allowlists.
 */
export function isHttpHookUrlAllowed(url: string, allowed: string[] | undefined): boolean {
  if (allowed === undefined) return true;
  if (allowed.length === 0) return false;
  return allowed.some(pattern => wildcardMatch(pattern, url));
}

/**
 * Stable error code for an HTTP hook whose target was refused by the SSRF guard
 * (resolves to a private/link-local/metadata address, or is otherwise unsafe).
 * Mirrors Claude Code's `ssrfError` code so the block is machine-identifiable in
 * logs and surfaced messages regardless of the human-readable reason text.
 */
export const ERR_HTTP_HOOK_BLOCKED_ADDRESS = 'ERR_HTTP_HOOK_BLOCKED_ADDRESS';

/**
 * Build the user-facing message for an SSRF-blocked HTTP hook, mirroring Claude
 * Code's `ssrfError` wording (names the blocked host, explains loopback is
 * allowed) and tagging it with the stable {@link ERR_HTTP_HOOK_BLOCKED_ADDRESS}
 * code. `reason` is the detail from the URL-safety validator (which already names
 * the resolved private IP). The host is extracted from the URL so the message
 * names the offending target even when the validator reason is generic.
 */
export function formatHttpHookBlockedMessage(url: string, reason: string): string {
  let host = url;
  try { host = new URL(url).hostname || url; } catch { /* non-parseable URL: keep raw */ }
  return `HTTP hook blocked: ${host} — ${reason}. Loopback (127.0.0.1, ::1) is allowed for local dev. [${ERR_HTTP_HOOK_BLOCKED_ADDRESS}]`;
}

/**
 * Strip CR, LF, and NUL bytes from an HTTP-hook header value to prevent HTTP
 * header (CRLF) injection via an env-var value or a hook-configured header
 * template (Claude Code's sanitizeHeaderValue). A malicious env var such as
 * `token\r\nX-Evil: 1` would otherwise smuggle a second header into the request.
 * Implemented with split/join (not a regex) so the control-char class does not
 * trip the security lint's no-control-regex rule.
 */
export function sanitizeHeaderValue(value: string): string {
  return value.split('\r').join('').split('\n').join('').split('\u0000').join('');
}

/**
 * Interpolate `$VAR` / `${VAR}` references in HTTP-hook header values from the
 * process environment, gated by an allowlist (the hook's own `allowedEnvVars`
 * intersected with the global `httpHookAllowedEnvVars` setting). Any var not on
 * the allowlist resolves to the empty string so a malicious hooks.json cannot
 * exfiltrate arbitrary secrets through a header. The fully-interpolated value is
 * then CRLF/NUL-sanitized (sanitizeHeaderValue) so neither the template literal
 * NOR a resolved env value can inject an extra header.
 */
export function interpolateHookHeaderEnv(
  value: string,
  allowed: Set<string>,
  env: Record<string, string | undefined> = process.env,
): string {
  const interpolated = value.replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)/g, (_m, braced, bare) => {
    const name = braced ?? bare;
    if (!allowed.has(name)) return '';
    return env[name] ?? '';
  });
  return sanitizeHeaderValue(interpolated);
}

/**
 * Claude Code's canonical hook output wraps event-specific fields in a
 * `hookSpecificOutput` object discriminated by `hookEventName`
 * (PreToolUse → permissionDecision/permissionDecisionReason/updatedInput/
 * additionalContext; UserPromptSubmit/SessionStart/… each with their own keys).
 * AMI's decision parse reads FLAT top-level fields, so a stock CC hook that only
 * emits `{"hookSpecificOutput": {...}}` was ignored. This flattens the envelope
 * onto the top level (top-level keys win if both present) so both shapes work.
 */
export function unwrapHookSpecificOutput(parsed: unknown): HookDecisionOutput {
  if (!parsed || typeof parsed !== 'object') return {} as HookDecisionOutput;
  const obj = parsed as Record<string, unknown>;
  const inner = obj.hookSpecificOutput;
  if (!inner || typeof inner !== 'object') return obj as HookDecisionOutput;
  const nested = inner as Record<string, unknown>;
  const merged: Record<string, unknown> = { ...obj };
  delete merged.hookSpecificOutput;
  for (const [k, v] of Object.entries(nested)) {
    if (k === 'hookEventName') continue;
    // permissionDecisionReason is CC's name for the human reason string
    const key = k === 'permissionDecisionReason' ? 'reason' : k;
    if (merged[key] === undefined) merged[key] = v;
  }
  return merged as HookDecisionOutput;
}

/**
 * hookEventName spoof check (Claude Code's parseHookOutput validation): when a
 * hook's `hookSpecificOutput.hookEventName` is present it MUST equal the event
 * the hook actually fired for. A mismatch means the hook is trying to act as a
 * different event (e.g. a SessionStart hook claiming PreToolUse to inject a
 * permission decision) — reject it. Returns a human-readable validation message
 * on mismatch, or undefined when it matches / is absent / has no expected event.
 * `expectedPascalEvent` is the PascalCase event name (e.g. 'PreToolUse').
 */
export function hookEventNameMismatch(
  parsed: unknown,
  expectedPascalEvent: string | undefined,
): string | undefined {
  if (!expectedPascalEvent) return undefined;
  if (!parsed || typeof parsed !== 'object') return undefined;
  const inner = (parsed as Record<string, unknown>).hookSpecificOutput;
  if (!inner || typeof inner !== 'object') return undefined;
  const name = (inner as Record<string, unknown>).hookEventName;
  if (typeof name !== 'string' || name.length === 0) return undefined;
  if (name !== expectedPascalEvent) {
    return `Hook returned incorrect event name: expected '${expectedPascalEvent}' but got '${name}'.`;
  }
  return undefined;
}

/**
 * Expected command-hook JSON output shape, surfaced as an affordance when a
 * hook emits malformed JSON (Claude Code's parseHookOutput error hint).
 */
export const HOOK_JSON_SCHEMA_HINT = JSON.stringify({
  continue: 'boolean (optional)',
  suppressOutput: 'boolean (optional)',
  stopReason: 'string (optional)',
  decision: '"approve" | "block" (optional)',
  reason: 'string (optional)',
  systemMessage: 'string (optional)',
  permissionDecision: '"allow" | "deny" | "ask" (optional)',
  additionalContext: 'string (optional)',
  updatedInput: 'object (optional)',
  hookSpecificOutput: 'object (optional)',
}, null, 2);

/**
 * Build a user-facing validation message for a command hook that emitted text
 * starting with `{` (i.e. intended to be JSON) which failed to parse. Mirrors
 * Claude Code's parseHookOutput `validationError` + schema affordance so a
 * malformed hook output is reported to the user instead of being silently
 * swallowed / treated as opaque plain text.
 */
export function formatHookValidationError(raw: string, err: unknown): string {
  const msg = err instanceof Error ? err.message : String(err);
  return `Hook JSON output validation failed: ${msg}\n\nThe hook's output was: ${raw}\n\nExpected schema:\n${HOOK_JSON_SCHEMA_HINT}`;
}

// ---------------------------------------------------------------------------
// Hook event serialization (stdin payload for command hooks)
// ---------------------------------------------------------------------------
// Claude Code hooks read a snake_case JSON object from stdin terminated by a
// newline (hook_event_name / tool_name / tool_input / tool_response / …). AMI
// historically emitted camelCase with an `event` key. serializeHookEvent emits
// BOTH: the original camelCase keys (back-compat for AMI's own consumers/tests)
// AND the Claude Code snake_case aliases + a trailing newline, so stock CC hooks
// work unchanged. (R2-H4)
const HOOK_EVENT_PASCAL: Record<string, string> = {
  preToolUse: 'PreToolUse', postToolUse: 'PostToolUse', userPromptSubmit: 'UserPromptSubmit',
  sessionStart: 'SessionStart', sessionEnd: 'SessionEnd', subagentStart: 'SubagentStart',
  subagentStop: 'SubagentStop', preCompact: 'PreCompact', postCompact: 'PostCompact',
  stop: 'Stop', error: 'Error', preSampling: 'PreSampling', postSampling: 'PostSampling',
  permissionRequest: 'PermissionRequest', permissionDenied: 'PermissionDenied',
  setup: 'Setup', teammateIdle: 'TeammateIdle',
};
const HOOK_KEY_SNAKE: Record<string, string> = {
  toolName: 'tool_name', toolInput: 'tool_input', toolOutput: 'tool_response',
  sessionId: 'session_id', subagentSessionId: 'subagent_session_id',
  durationMs: 'duration_ms', turnCount: 'turn_count', messageCount: 'message_count',
  agentType: 'agent_type', teammateName: 'teammate_name', teamName: 'team_name',
  loadReason: 'load_reason', memoryType: 'memory_type',
  notificationType: 'notification_type',
  permissionMode: 'permission_mode',
  samplingOrigin: 'sampling_origin',
};

/**
 * Substitute a plugin/skill bundle's base directory into a command-hook string
 * (command or an arg). Replaces the `${CLAUDE_PLUGIN_ROOT}` placeholder (and the
 * AMI-native `${AMI_PLUGIN_ROOT}` alias) with `pluginRoot`, so a plugin- or
 * skill-registered command hook can reference files co-located with its bundle
 * (`${CLAUDE_PLUGIN_ROOT}/scripts/check.sh`) regardless of the session cwd —
 * Claude Code's CLAUDE_PLUGIN_ROOT command substitution (utils/hooks.ts) and
 * registerSkillHooks skillRoot. Both `${VAR}` and bare `$VAR` forms are handled.
 * Returns the string unchanged when `pluginRoot` is empty. Canonical — reused by
 * every command-hook call site; never re-implement the substitution per site.
 */
export function substitutePluginRoot(text: string, pluginRoot: string | undefined): string {
  if (!pluginRoot || !text) return text;
  return text
    .replace(/\$\{CLAUDE_PLUGIN_ROOT\}/g, pluginRoot)
    .replace(/\$\{AMI_PLUGIN_ROOT\}/g, pluginRoot)
    .replace(/\$CLAUDE_PLUGIN_ROOT\b/g, pluginRoot)
    .replace(/\$AMI_PLUGIN_ROOT\b/g, pluginRoot);
}

export function serializeHookEvent(obj: Record<string, unknown>): string {
  const out: Record<string, unknown> = { ...obj };
  if (typeof obj.event === 'string') {
    out.hook_event_name = HOOK_EVENT_PASCAL[obj.event] ?? obj.event;
  }
  for (const [camel, snake] of Object.entries(HOOK_KEY_SNAKE)) {
    if (camel in obj && !(snake in obj)) out[snake] = obj[camel];
  }
  return JSON.stringify(out) + '\n';
}

// ---------------------------------------------------------------------------
// Shared hook-output decoders (canonical — reused by every execute* method
// that consumes runHookByType's flattened result; never re-implement per site).
// ---------------------------------------------------------------------------

/**
 * Canonical extractor for a blocking reason from a flattened hook result.
 * runHookByType maps exit-2 / `decision:block` / `continue:false` to the
 * `DENY:<reason>` sentinel; this returns the reason when a hook blocked, else
 * undefined. Used by Stop / PostToolUse / TaskCompleted blocking consumers.
 */
export function extractHookBlockingError(result: string): string | undefined {
  if (typeof result === 'string' && result.startsWith('DENY:')) {
    return result.slice(5).trim();
  }
  return undefined;
}

/**
 * Strict validation of a hook's decision fields (Claude Code parseHookOutput: an
 * unrecognized `decision` or `permissionDecision` value is an ERROR, not a silent
 * no-op). Returns a human-readable validation message when the parsed output
 * carries a `decision`/`permissionDecision` field whose value is outside the
 * allowed set, else undefined. Reused by the permissionRequest parser so a typo'd
 * decision fails CLOSED (deny with the reason) instead of being ignored — which
 * would let an intended block silently fall through to the default permission
 * flow. Valid sets match AMI's HookDecisionOutput: decision ∈
 * {approve,deny,ask,block}, permissionDecision ∈ {allow,deny,ask}.
 */
export function unknownHookDecisionError(
  parsed: Record<string, unknown> | undefined,
): string | undefined {
  if (!parsed) return undefined;
  const dec = parsed.decision;
  if (dec !== undefined && dec !== 'approve' && dec !== 'deny' && dec !== 'ask' && dec !== 'block') {
    return `Unknown hook decision type: ${String(dec)}. Valid types are: approve, block`;
  }
  const pdec = parsed.permissionDecision;
  if (pdec !== undefined && pdec !== 'allow' && pdec !== 'deny' && pdec !== 'ask') {
    return `Unknown hook permissionDecision type: ${String(pdec)}. Valid types are: allow, deny, ask`;
  }
  return undefined;
}

/**
 * Strict shape/type validation of a parsed hook JSON output before any field is
 * acted on (Claude Code's parseHookOutput / hookJSONOutputSchema safeParse): a
 * hook that declares a KNOWN decision field with a wrong-typed value (e.g.
 * `continue: "yes"`, `updatedInput: []`, `asyncTimeout: "15"`) is malformed and
 * must be surfaced as a validation error rather than silently mis-interpreted
 * (`"yes"` is truthy, `continue:false` would never fire). Reuses
 * {@link unknownHookDecisionError} for the decision/permissionDecision enums (no
 * duplicate), then checks the remaining typed fields. Returns a human-readable
 * message for the first offending field, else undefined.
 *
 * Unknown/extra properties are intentionally NOT rejected (no
 * `additionalProperties:false`): AMI's forging defense already STRIPS unrecognized
 * fields (hook-forging-attack.test.ts), so a hook carrying extra keys is cleaned,
 * not errored — matching AMI's superset behavior while still type-gating every
 * recognized field.
 */
export function validateHookDecisionOutput(
  parsed: Record<string, unknown> | undefined,
): string | undefined {
  if (!parsed) return undefined;
  const enumErr = unknownHookDecisionError(parsed);
  if (enumErr) return enumErr;
  for (const k of ['reason', 'stopReason', 'systemMessage', 'additionalContext'] as const) {
    if (parsed[k] !== undefined && typeof parsed[k] !== 'string') {
      return `Hook output field "${k}" must be a string.`;
    }
  }
  for (const k of ['continue', 'suppressOutput', 'async', 'interrupt'] as const) {
    if (parsed[k] !== undefined && typeof parsed[k] !== 'boolean') {
      return `Hook output field "${k}" must be a boolean.`;
    }
  }
  if (parsed.updatedPermissions !== undefined && !Array.isArray(parsed.updatedPermissions)) {
    return `Hook output field "updatedPermissions" must be an array.`;
  }
  if (
    parsed.asyncTimeout !== undefined &&
    (typeof parsed.asyncTimeout !== 'number' || !Number.isFinite(parsed.asyncTimeout))
  ) {
    return `Hook output field "asyncTimeout" must be a number.`;
  }
  if (
    parsed.updatedInput !== undefined &&
    (typeof parsed.updatedInput !== 'object' ||
      parsed.updatedInput === null ||
      Array.isArray(parsed.updatedInput))
  ) {
    return `Hook output field "updatedInput" must be an object.`;
  }
  return undefined;
}

/**
 * Event-specific default deny/feedback wording (Claude Code's phase-specific
 * block messages): when a hook blocks WITHOUT supplying its own reason, the
 * surfaced text names the lifecycle phase instead of a bare 'Hook blocked', so a
 * user/model can tell WHICH guard fired. A hook-supplied `reason` always wins.
 */
export function formatHookDenyReason(event: string | undefined, reason?: string): string {
  const r = reason?.trim();
  if (r) return r;
  switch (event) {
    case 'PreToolUse':
    case 'preToolUse': return 'PreToolUse hook blocked this tool call';
    case 'PostToolUse':
    case 'postToolUse': return 'PostToolUse hook blocked the result';
    case 'UserPromptSubmit':
    case 'userPromptSubmit': return 'UserPromptSubmit hook blocked this prompt';
    case 'Stop':
    case 'stop': return 'Stop hook prevented the turn from ending';
    default: return event ? `${event} hook blocked this action` : 'Hook blocked this action';
  }
}

/** Global hook kill-switch via environment (Claude Code's `CLAUDE_CODE_SIMPLE`):
 *  when set truthy, EVERY hook source is skipped at load time — a fast,
 *  config-file-free escape hatch for a user who wants a hook-free session. The
 *  AMI-native aliases `AMI_SIMPLE` / `AMI_DISABLE_HOOKS` are honored too. Pure;
 *  consulted by {@link HookManager.isAllHooksDisabled} alongside the managed
 *  `disableAllHooks` policy. */
export function isHookKillSwitchEnv(env: Record<string, string | undefined> = process.env): boolean {
  return isEnvTruthy(env.CLAUDE_CODE_SIMPLE) || isEnvTruthy(env.AMI_SIMPLE) || isEnvTruthy(env.AMI_DISABLE_HOOKS);
}

/**
 * Canonical best-effort JSON decode of a hook's raw stdout. Returns the parsed
 * object (for reading structured fields like watchPaths / additionalContexts /
 * newCustomInstructions / elicitation action) or undefined when the output is
 * not a JSON object.
 */
export function tryParseHookJson(result: string): Record<string, unknown> | undefined {
  if (typeof result !== 'string') return undefined;
  const trimmed = result.trim();
  if (!trimmed || (trimmed[0] !== '{' && trimmed[0] !== '[')) return undefined;
  try {
    const parsed = JSON.parse(trimmed);
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
  } catch { /* not JSON */ }
  return undefined;
}

/**
 * Canonical merge of user-supplied compaction instructions with hook-injected
 * ones (mirrors Claude Code's mergeHookInstructions): hook text is appended,
 * blank-line separated; either side may be empty.
 */
export function mergeHookInstructions(
  userInstructions: string | undefined | null,
  hookInstructions: string | undefined | null,
): string | undefined {
  if (!hookInstructions) return userInstructions || undefined;
  if (!userInstructions) return hookInstructions;
  return `${userInstructions}\n\n${hookInstructions}`;
}

/** Collect additional-context strings a hook contributed, from any of:
 *  `additionalContexts: string[]`, `additionalContext: string`,
 *  `systemMessage: string`, or the flattened `SYSTEM:<text>` sentinel. Shared by
 *  SubagentStart / other context-injecting consumers. */
export function extractAdditionalContexts(result: string): string[] {
  const out: string[] = [];
  const obj = tryParseHookJson(result);
  if (obj) {
    if (Array.isArray(obj.additionalContexts)) {
      for (const c of obj.additionalContexts) if (typeof c === 'string' && c) out.push(c);
    }
    if (typeof obj.additionalContext === 'string' && obj.additionalContext) out.push(obj.additionalContext);
    if (typeof obj.systemMessage === 'string' && obj.systemMessage) out.push(obj.systemMessage);
  } else if (typeof result === 'string' && result.startsWith('SYSTEM:')) {
    const text = result.slice(7).trim();
    if (text) out.push(text);
  }
  return out;
}

/** Extract the worktree path a WorktreeCreate provider hook reported, matching
 *  Claude Code's executeWorktreeCreateHook: prefer `hookSpecificOutput.worktreePath`
 *  (http/JSON hooks), else the flattened top-level `worktreePath`, else the first
 *  non-empty trimmed stdout line (command hooks that just `echo` the path). Returns
 *  undefined when the hook produced no usable path (observe-only). */
export function extractWorktreePath(result: string): string | undefined {
  const obj = tryParseHookJson(result);
  if (obj) {
    const flat = unwrapHookSpecificOutput(obj) as Record<string, unknown>;
    const wp = flat.worktreePath;
    if (typeof wp === 'string' && wp.trim().length > 0) return wp.trim();
    return undefined;
  }
  if (typeof result === 'string') {
    for (const line of result.split('\n')) {
      const t = line.trim();
      if (t.length > 0) return t;
    }
  }
  return undefined;
}

/** Decode an elicitation hook's output into a structured result: a programmatic
 *  `action`/`content` response (short-circuits the user prompt) or a
 *  `blockingError` (→ decline). A DENY sentinel maps to a blockingError. */
export function parseElicitationResult(result: string): ElicitationHookResult | undefined {
  const blockingError = extractHookBlockingError(result);
  if (blockingError) return { blockingError };
  const obj = tryParseHookJson(result);
  if (!obj) return undefined;
  const out: ElicitationHookResult = {};
  if (obj.action === 'accept' || obj.action === 'decline' || obj.action === 'cancel') out.action = obj.action;
  if (obj.content && typeof obj.content === 'object' && !Array.isArray(obj.content)) out.content = obj.content as Record<string, unknown>;
  if (typeof obj.blockingError === 'string' && obj.blockingError) out.blockingError = obj.blockingError;
  if (out.action || out.content || out.blockingError) return out;
  return undefined;
}

/**
 * System-prompt contract appended to prompt/agent (AI-driven) hooks so the model
 * knows exactly what to return: a single JSON object {"ok": boolean, "reason"?:
 * string}. Mirrors Claude Code's hookResponseSchema / createStructuredOutputTool.
 */
export const HOOK_VERIFICATION_CONTRACT =
  'You are a verification hook. Evaluate the condition described below against the ' +
  'event data. Respond with EXACTLY one JSON object and nothing else: ' +
  '{"ok": true} if the condition is met, or {"ok": false, "reason": "<why>"} if it is not.';

/**
 * Substitute the hook input JSON into a prompt/agent hook's prompt template
 * (Claude Code's addArgumentsToPrompt / substituteArguments):
 *   - `$ARGUMENTS`        → the full hook-input JSON string
 *   - `$ARGUMENTS[n]`/`$n`→ the n-th whitespace-delimited token of the input
 *   - if NO placeholder appears, the input is appended as `\n\nARGUMENTS: <json>`
 * so an AI-driven hook can reference the event data positionally OR rely on the
 * append fallback. Indexed tokens use a simple whitespace split (the common case
 * for hook input); `$ARGUMENTS` always yields the verbatim JSON.
 */
export function substituteHookArguments(prompt: string, jsonInput: string): string {
  const tokens = jsonInput.trim().length ? jsonInput.trim().split(/\s+/) : [];
  const original = prompt;
  let out = prompt
    .replace(/\$ARGUMENTS\[(\d+)\]/g, (_m, i: string) => tokens[parseInt(i, 10)] ?? '')
    .replace(/\$(\d+)(?!\w)/g, (_m, i: string) => tokens[parseInt(i, 10)] ?? '');
  out = out.split('$ARGUMENTS').join(jsonInput);
  if (out === original && jsonInput) out = `${out}\n\nARGUMENTS: ${jsonInput}`;
  return out;
}

/**
 * Parse an AI-driven hook's output against the {ok, reason} verification schema.
 * Returns the structured result when the output is a valid verification object,
 * or undefined when it is free-form (callers fall back to raw-text handling).
 */
export function parseHookVerification(output: string): { ok: boolean; reason?: string } | undefined {
  const obj = tryParseHookJson(output);
  if (!obj || typeof obj.ok !== 'boolean') return undefined;
  const reason = typeof obj.reason === 'string' ? obj.reason : undefined;
  return { ok: obj.ok, reason };
}

/** Pull an array of absolute watch paths from a hook's JSON output (watchPaths),
 *  filtering to non-empty strings. Shared by sessionStart / cwdChanged /
 *  fileChanged which can register dynamic FileChanged watches.
 *
 *  Path scoping (Claude Code's FileChanged `watchPaths` contract): each entry MUST
 *  be ABSOLUTE. A relative path from untrusted hook output is ambiguous (resolved
 *  against whose cwd?) and is dropped rather than silently resolved — the host only
 *  ever watches absolute, hook-declared locations. */
export function parseWatchPaths(result: string): string[] {
  const obj = tryParseHookJson(result);
  const raw = obj?.watchPaths;
  if (!Array.isArray(raw)) return [];
  return raw.filter((p): p is string => typeof p === 'string' && p.length > 0 && path.isAbsolute(p));
}

/**
 * Per-event matcher field (Claude Code's hooksConfigManager fieldToMatch): for
 * non-tool events the `matcher` is applied against this context field instead of
 * the tool name, so a hook can be scoped to e.g. a specific MCP server, compact
 * trigger, config source, or subagent type. Events absent from this map (and the
 * tool events, handled inline against toolName) treat a matcher as a no-op.
 */
export const HOOK_MATCHER_FIELD: Record<string, string> = {
  notification: 'notificationType',
  subagentStart: 'mode',
  subagentStop: 'mode',
  preCompact: 'trigger',
  postCompact: 'trigger',
  // Sampling: scope on the query ORIGIN (Claude Code's MagicDocs querySource
  // boundary) so a pre/postSampling hook can fire for only `main` (the top-level
  // loop) or only `subagent` (forked/nested agents) — the re-entrancy guard that
  // lets a hook which itself samples the model exclude background work.
  preSampling: 'samplingOrigin',
  postSampling: 'samplingOrigin',
  // Session lifecycle: scope on the boundary cause (Claude Code's
  // SessionStart `source` / SessionEnd `reason` discriminators) so a hook can
  // fire for e.g. only `resume` or only `clear`.
  sessionStart: 'source',
  sessionEnd: 'reason',
  // Setup: scope on the boundary cause (Claude Code's Setup `trigger`
  // discriminator — init | maintenance).
  setup: 'trigger',
  // A StopFailure hook scopes on the failure CLASS (Claude Code's `error` field,
  // e.g. rate_limit / authentication_failed) — the emitter classifies the raw
  // failure into this taxonomy (stopFailureErrorClass) before firing.
  stopFailure: 'error',
  elicitation: 'mcpServerName',
  elicitationResult: 'mcpServerName',
  configChange: 'source',
  instructionsLoaded: 'loadReason',
  // TeammateIdle: scope on the teammate's name (Claude Code's TeammateIdle
  // teammate_name field) so a hook can fire for just one teammate.
  teammateIdle: 'teammateName',
  permissionRequest: 'toolName',
  permissionDenied: 'toolName',
};

/** True when a hook's `matcher` applies to the given non-tool event context
 *  (matches the event's fieldToMatch value), or when there is no matcher / no
 *  matchable field. Shared by every non-tool event registration. */
export function matcherAppliesToCtx(event: string, matcher: string | undefined, ctx: Record<string, unknown>): boolean {
  if (!matcher) return true;
  const field = HOOK_MATCHER_FIELD[event];
  if (field === undefined) return true;
  return matchesPattern(matcher, String(ctx[field] ?? ''));
}

/**
 * Declarative per-event metadata (Claude Code's getHookEventMetadata): a
 * human-readable summary + description (input JSON shape + command-hook exit-code
 * semantics) for every hook event. Drives `/hooks`-style introspection and the
 * `--introspect` manifest. The matcher field for each event is derived from the
 * single source HOOK_MATCHER_FIELD (tool events match on the tool name) so the
 * catalog can never drift from the actual matcher scoping.
 */
export interface HookEventMetadata {
  summary: string;
  description: string;
  /** The context field a `matcher` is applied against, or undefined when the
   *  event has no matchable field (a matcher is a no-op). */
  matcherField?: string;
  /** Enumerated allowed matcher values for this event's matcher field (Claude
   *  Code's MatcherMetadata.values). Present whenever `matcherField` is — a static
   *  enumeration (session sources, compact triggers, config sources, …) or, for
   *  dynamically-scoped events (tool names, agent types, MCP server names), the
   *  caller-supplied set via {@link HookEventMetadataOpts} (defaulting to `[]`).
   *  Drives matcher-value discovery in `/hooks` + `--introspect`. */
  matcherValues?: string[];
}

/** Dynamic matcher-value sources for {@link getHookEventMetadata}: the valid set
 *  for some events (tool/agent/MCP-scoped) is runtime state, not a fixed enum. */
export interface HookEventMetadataOpts {
  /** Tool names enumerated for tool-matched events (preToolUse, postToolUse, …). */
  toolNames?: string[];
  /** Agent types enumerated for subagentStart / subagentStop. */
  agentTypes?: string[];
  /** MCP server names enumerated for elicitation / elicitationResult. */
  mcpServerNames?: string[];
}

/** Static enumerations of allowed matcher values, keyed by event (Claude Code's
 *  per-event MatcherMetadata.values). Events whose valid set is dynamic (tool/
 *  agent/MCP scoped) are resolved from {@link HookEventMetadataOpts} instead. */
const HOOK_MATCHER_VALUES: Record<string, readonly string[]> = {
  sessionStart: SESSION_START_SOURCES,
  sessionEnd: SESSION_END_REASONS,
  setup: SETUP_TRIGGERS,
  preCompact: ['manual', 'auto'],
  postCompact: ['manual', 'auto'],
  preSampling: SAMPLING_ORIGINS,
  postSampling: SAMPLING_ORIGINS,
  // Single source: the StopFailure taxonomy lives in error-classifier.ts, where
  // stopFailureErrorClass() maps onto it, so the advertised matcher values and the
  // emitted `error` class can never drift.
  stopFailure: STOP_FAILURE_ERROR_CLASSES,
  configChange: ['user_settings', 'project_settings', 'local_settings', 'policy_settings', 'skills'],
  instructionsLoaded: ['session_start', 'nested_traversal', 'path_glob_match', 'include', 'compact'],
  notification: [
    'permission_prompt', 'idle_prompt', 'auth_success',
    'elicitation_dialog', 'elicitation_complete', 'elicitation_response',
  ],
};

/** Resolve the enumerated matcher values for an event given its matcher field:
 *  dynamic sets come from `opts`, otherwise the static {@link HOOK_MATCHER_VALUES}
 *  enumeration (or `[]` when the valid set is open-ended / unknown). */
function resolveMatcherValues(event: string, matcherField: string, opts: HookEventMetadataOpts): string[] {
  if (matcherField === 'toolName') return opts.toolNames ? [...opts.toolNames] : [];
  if (matcherField === 'mode') return opts.agentTypes ? [...opts.agentTypes] : [];
  if (matcherField === 'mcpServerName') return opts.mcpServerNames ? [...opts.mcpServerNames] : [];
  const stat = HOOK_MATCHER_VALUES[event];
  return stat ? [...stat] : [];
}

/** Whether a `matcher` is meaningful for this event (Claude Code's
 *  eventSupportsMatcher): true when the event has a matchable field (tool name
 *  or a non-tool fieldToMatch). Events without one treat a matcher as a no-op, so
 *  the introspection UI hides/ignores it. */
export function eventSupportsMatcher(event: string): boolean {
  return TOOL_MATCHER_EVENTS.has(event) || HOOK_MATCHER_FIELD[event] !== undefined;
}

/** Display label for a hook matcher: an empty / absent matcher targets everything,
 *  rendered as '(all)' (Claude Code's matcher list fallback). */
export function formatHookMatcherLabel(matcher: string | undefined): string {
  return matcher && matcher.length > 0 ? matcher : '(all)';
}

/** Source-tier priority for ordering matchers/hooks in introspection (lower =
 *  higher authority). Mirrors Claude Code's SOURCES order with plugin/builtin
 *  pinned lowest. */
export const HOOK_SOURCE_PRIORITY: Record<string, number> = {
  managed: 0,
  project: 1,
  user: 2,
  config: 3,
  plugin: 999,
  builtin: 999,
};

/** Deterministically order matchers within an event (Claude Code's
 *  sortMatchersByPriority): by their highest-priority (lowest-number) contributing
 *  source first, tie-broken by matcher name. Pure — never mutates its input. */
export function sortMatchersByPriority(
  matchers: string[],
  sourcesByMatcher: Record<string, readonly string[]>,
): string[] {
  const pri = (s: string): number => HOOK_SOURCE_PRIORITY[s] ?? 500;
  const highest = (m: string): number => {
    const srcs = sourcesByMatcher[m] ?? [];
    return srcs.length ? Math.min(...srcs.map(pri)) : 1000;
  };
  return [...matchers].sort((a, b) => {
    const d = highest(a) - highest(b);
    return d !== 0 ? d : a.localeCompare(b);
  });
}

/** Events whose matcher applies to the tool name (checked inline at dispatch,
 *  so they are not all in HOOK_MATCHER_FIELD). */
const TOOL_MATCHER_EVENTS: ReadonlySet<string> = new Set([
  'preToolUse', 'postToolUse', 'postToolUseFailure', 'permissionRequest', 'permissionDenied',
]);

const EXIT_CODE_BLOCKING =
  '\nExit code 0 — success (stdout parsed for a decision).' +
  '\nExit code 2 — blocking error: stderr is shown to the model and the action is blocked.' +
  '\nOther non-zero — non-blocking: stderr is surfaced but the action continues.';
const EXIT_CODE_OBSERVE =
  '\nExit code 0 — success (stdout not shown).' +
  '\nOther non-zero — stderr is surfaced to the user only.';

const HOOK_EVENT_SUMMARY_DESC: Record<string, { summary: string; description: string }> = {
  preToolUse: { summary: 'Before tool execution', description: 'Input: JSON of the tool call (tool_name, tool_input).' + EXIT_CODE_BLOCKING },
  postToolUse: { summary: 'After tool execution', description: 'Input: JSON with tool_name, tool_input, tool_response, duration_ms.' + EXIT_CODE_BLOCKING },
  postToolUseFailure: { summary: 'After a tool execution fails', description: 'Input: JSON with tool_name, is_error, duration_ms.' + EXIT_CODE_OBSERVE },
  permissionRequest: { summary: 'When a tool needs permission', description: 'Input: JSON with tool_name, tool_input. Return a permissionDecision (allow|deny|ask).' + EXIT_CODE_BLOCKING },
  permissionDenied: { summary: 'After a permission is denied', description: 'Input: JSON with tool_name. Return {retry:true} to let the model retry.' + EXIT_CODE_OBSERVE },
  preSampling: { summary: 'Before a model sampling call', description: 'Input: JSON event marker.' + EXIT_CODE_BLOCKING },
  postSampling: { summary: 'After a model sampling call', description: 'Input: JSON event marker.' + EXIT_CODE_OBSERVE },
  userPromptSubmit: { summary: 'When the user submits a prompt', description: 'Input: JSON with prompt, turn_count. May inject additionalContext or block.' + EXIT_CODE_BLOCKING },
  stop: { summary: 'When the agent turn is about to end', description: 'Input: JSON with stop_hook_active, last_assistant_message. May block turn-end.' + EXIT_CODE_BLOCKING },
  stopFailure: { summary: 'When the turn ends due to an API error (or a Stop hook itself errors)', description: `Input: JSON with error (failure class: ${STOP_FAILURE_ERROR_CLASSES.join('|')}), errorMessage, hookIndex.` + EXIT_CODE_OBSERVE },
  error: { summary: 'On an engine/turn error', description: 'Input: JSON event marker.' + EXIT_CODE_OBSERVE },
  sessionStart: { summary: 'At session start', description: `Input: JSON with session_id, source (${SESSION_START_SOURCES.join('|')}), model, agent_type. May return initialUserMessage / initialUserMessages[] / watchPaths.` + EXIT_CODE_OBSERVE },
  sessionEnd: { summary: 'At session end', description: `Input: JSON with session_id, reason (${SESSION_END_REASONS.join('|')}).` + EXIT_CODE_OBSERVE },
  setup: { summary: 'On first-run setup', description: `Input: JSON with session_id, cwd, trigger (${SETUP_TRIGGERS.join('|')}).` + EXIT_CODE_OBSERVE },
  subagentStart: { summary: 'When a subagent starts', description: 'Input: JSON with prompt. May contribute additionalContexts.' + EXIT_CODE_OBSERVE },
  subagentStop: { summary: 'When a subagent stops', description: 'Input: JSON with subagent_session_id.' + EXIT_CODE_OBSERVE },
  preCompact: { summary: 'Before context compaction', description: 'Input: JSON with message_count, trigger, customInstructions. May override instructions.' + EXIT_CODE_OBSERVE },
  postCompact: { summary: 'After context compaction', description: 'Input: JSON with message_count.' + EXIT_CODE_OBSERVE },
  notification: { summary: 'When a notification is sent', description: 'Input: JSON with message, notification type, title.' + EXIT_CODE_OBSERVE },
  configChange: { summary: 'When configuration changes', description: 'Input: JSON with source, filePath. A blocking hook vetoes the change.' + EXIT_CODE_BLOCKING },
  instructionsLoaded: { summary: 'When instruction files load', description: 'Input: JSON with filePath, loadReason.' + EXIT_CODE_OBSERVE },
  taskCreated: { summary: 'When a task is created', description: 'Input: JSON with taskId, subject.' + EXIT_CODE_OBSERVE },
  taskCompleted: { summary: 'When a task completes', description: 'Input: JSON with taskId, subject, status. A blocking hook vetoes completion.' + EXIT_CODE_BLOCKING },
  teammateIdle: { summary: 'When a teammate goes idle', description: 'Input: JSON with teammate_name, team_name. Fires when a teammate agent finishes its run and becomes idle.' + EXIT_CODE_OBSERVE },
  worktreeCreate: { summary: 'When a worktree is created', description: 'Input: JSON with name, worktreePath.' + EXIT_CODE_OBSERVE },
  worktreeRemove: { summary: 'When a worktree is removed', description: 'Input: JSON with name, worktreePath.' + EXIT_CODE_OBSERVE },
  fileChanged: { summary: 'When a watched file changes', description: 'Input: JSON with filePath, fileEvent. May return watchPaths.' + EXIT_CODE_OBSERVE },
  cwdChanged: { summary: 'When the working directory changes', description: 'Input: JSON with oldCwd, newCwd. May return watchPaths.' + EXIT_CODE_OBSERVE },
  elicitation: { summary: 'Before an MCP elicitation prompt', description: 'Input: JSON with mcp_server_name, message. May return action/content or block.' + EXIT_CODE_BLOCKING },
  elicitationResult: { summary: 'After an MCP elicitation reply', description: 'Input: JSON with mcp_server_name. May override action/content or block.' + EXIT_CODE_BLOCKING },
};

/** Return the declarative metadata (summary, description, matcher field +
 *  enumerated matcher values) for a hook event, or undefined for an unknown event.
 *  `opts` supplies the dynamic matcher-value sets (tool names, agent types, MCP
 *  server names) for events whose valid matcher set is runtime state. */
export function getHookEventMetadata(event: string, opts: HookEventMetadataOpts = {}): HookEventMetadata | undefined {
  const base = HOOK_EVENT_SUMMARY_DESC[event];
  if (!base) return undefined;
  const matcherField = TOOL_MATCHER_EVENTS.has(event) ? 'toolName' : HOOK_MATCHER_FIELD[event];
  const matcherValues = matcherField !== undefined ? resolveMatcherValues(event, matcherField, opts) : undefined;
  return {
    summary: base.summary,
    description: base.description,
    ...(matcherField ? { matcherField } : {}),
    ...(matcherValues ? { matcherValues } : {}),
  };
}

/**
 * Content-identity equality for hook configs (Claude Code's isHookEqual): two
 * hooks are the same when their type + content (command/prompt/url) + `if`
 * condition match; timeout is ignored. Used to de-duplicate hook entries loaded
 * from overlapping sources.
 */
export function isHookEqual(
  a: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig,
  b: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig,
): boolean {
  if (a.type !== b.type) return false;
  const sameIf = (a.if ?? '') === (b.if ?? '');
  switch (a.type) {
    case 'command': return b.type === 'command' && a.command === b.command && sameIf;
    case 'prompt': return b.type === 'prompt' && a.prompt === b.prompt && sameIf;
    case 'agent': return b.type === 'agent' && a.prompt === b.prompt && sameIf;
    case 'http': return b.type === 'http' && a.url === b.url && sameIf;
    default: return false;
  }
}

// ---------------------------------------------------------------------------
// HookManager
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Hook config file types
// ---------------------------------------------------------------------------

export interface CommandHookConfig {
  type: 'command';
  command: string;
  /** Exec form: spawn command directly without a shell, avoiding quoting issues. */
  args?: string[];
  /** Interpreter for the command (Claude Code's per-hook shell selection):
   *  'bash' (default) | 'sh' | 'powershell' | 'pwsh'. Ignored when `args` is set
   *  (exec form bypasses the shell). */
  shell?: string;
  timeout?: number;
  once?: boolean;
  if?: string;
  async?: boolean;
  /** When `async` is true, the background hook's lifetime budget in MILLISECONDS
   *  (default DEFAULT_ASYNC_HOOK_TIMEOUT_MS = 15_000). Distinct from the synchronous
   *  `timeout` (seconds): a background hook is bounded on this short clock and
   *  aborted when it elapses. Clamped to [100ms, MAX_HOOK_TIMEOUT_MS]. */
  asyncTimeout?: number;
  asyncRewake?: boolean;
  /** 'block' = a hook that times out or errors denies the action (fail-closed);
   *  default 'allow' preserves today's fail-open behavior. */
  onFailure?: 'block' | 'allow';
  /** Human-readable label shown in progress/introspection surfaces in place of the
   *  raw command (Claude Code's getHookDisplayText `statusMessage`). Lets a hook
   *  present e.g. 'Running security scan…' instead of leaking the shell command. */
  statusMessage?: string;
  /** Filesystem base directory of the plugin bundle (or skill) that contributed
   *  this command hook. When set, the command/args have `${CLAUDE_PLUGIN_ROOT}`
   *  (and the `${AMI_PLUGIN_ROOT}` alias) substituted to this path, and the hook
   *  child process receives `CLAUDE_PLUGIN_ROOT`/`AMI_PLUGIN_ROOT` env vars so a
   *  plugin/skill-registered hook can locate its own co-located files regardless of
   *  the session cwd (Claude Code's CLAUDE_PLUGIN_ROOT / registerSkillHooks
   *  skillRoot). Populated by loadFromFile's plugin-discovery block; absent for
   *  filesystem (.ami/.claude/config) hooks. */
  pluginRoot?: string;
}

export interface PromptHookConfig {
  type: 'prompt';
  prompt: string;
  model?: string;
  timeout?: number;
  once?: boolean;
  if?: string;
  async?: boolean;
  /** When `async` is true, the background hook's lifetime budget in MILLISECONDS
   *  (default DEFAULT_ASYNC_HOOK_TIMEOUT_MS = 15_000). Distinct from the synchronous
   *  `timeout` (seconds): a background hook is bounded on this short clock and
   *  aborted when it elapses. Clamped to [100ms, MAX_HOOK_TIMEOUT_MS]. */
  asyncTimeout?: number;
  asyncRewake?: boolean;
  /** Friendly label for progress/introspection (Claude Code `statusMessage`). */
  statusMessage?: string;
}

export interface HttpHookConfig {
  type: 'http';
  url: string;
  headers?: Record<string, string>;
  timeout?: number;
  /** 'block' = a hook that times out or errors denies the action (fail-closed). */
  onFailure?: 'block' | 'allow';
  /** Env vars this hook may interpolate into header values ($VAR/${VAR}),
   *  intersected with the global httpHookAllowedEnvVars allowlist. */
  allowedEnvVars?: string[];
  once?: boolean;
  if?: string;
  async?: boolean;
  /** When `async` is true, the background hook's lifetime budget in MILLISECONDS
   *  (default DEFAULT_ASYNC_HOOK_TIMEOUT_MS = 15_000). Distinct from the synchronous
   *  `timeout` (seconds): a background hook is bounded on this short clock and
   *  aborted when it elapses. Clamped to [100ms, MAX_HOOK_TIMEOUT_MS]. */
  asyncTimeout?: number;
  asyncRewake?: boolean;
  /** Friendly label for progress/introspection (Claude Code `statusMessage`). */
  statusMessage?: string;
}

/**
 * Admin/settings-level policy for HTTP hooks (merged from settings sources).
 *   allowedHttpHookUrls   — undefined=no restriction, []=block all, non-empty=wildcard allowlist
 *   httpHookAllowedEnvVars — env vars a hook may interpolate into headers
 */
export interface HttpHookPolicy {
  allowedHttpHookUrls?: string[];
  httpHookAllowedEnvVars?: string[];
}

export interface AgentHookConfig {
  type: 'agent';
  prompt: string;
  model?: string;
  timeout?: number;
  once?: boolean;
  if?: string;
  async?: boolean;
  /** When `async` is true, the background hook's lifetime budget in MILLISECONDS
   *  (default DEFAULT_ASYNC_HOOK_TIMEOUT_MS = 15_000). Distinct from the synchronous
   *  `timeout` (seconds): a background hook is bounded on this short clock and
   *  aborted when it elapses. Clamped to [100ms, MAX_HOOK_TIMEOUT_MS]. */
  asyncTimeout?: number;
  asyncRewake?: boolean;
  /** Friendly label for progress/introspection (Claude Code `statusMessage`). */
  statusMessage?: string;
}

export interface HookDecisionOutput {
  decision?: 'approve' | 'deny' | 'ask' | 'block';
  permissionDecision?: 'allow' | 'deny' | 'ask';
  updatedInput?: Record<string, unknown>;
  additionalContext?: string;
  continue?: boolean;
  stopReason?: string;
  systemMessage?: string;
  suppressOutput?: boolean;
  reason?: string;
  /** A hook that emits `{ "async": true }` on stdout self-declares that it should
   *  be treated as a background/async hook (Claude Code's AsyncHookJSONOutput).
   *  Validated by {@link validateHookDecisionOutput}; carried so a consumer can
   *  detect the runtime async-declaration channel. */
  async?: boolean;
  /** Background-hook lifetime budget (ms) a hook may self-declare alongside
   *  `async:true` (Claude Code's `asyncTimeout`). */
  asyncTimeout?: number;
  /** PermissionRequest ALLOW: durable permission-rule mutations the engine should
   *  persist (Claude Code's `decision.updatedPermissions`). */
  updatedPermissions?: PermissionUpdate[];
  /** PermissionRequest DENY: abort the whole turn (not just skip this tool) —
   *  Claude Code's `decision.interrupt` → `abortController.abort()`. */
  interrupt?: boolean;
}

export type HookCallback = (event: string, data: unknown) => Promise<HookDecisionOutput | string | void>;

export interface HookConfigEntry {
  event: string;
  hook: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig;
  matcher?: string;
  _source?: 'project' | 'user' | 'config';
  /** Name of the plugin bundle that contributed this hook, when it came from a
   *  plugin (Claude Code's per-hook `pluginName`/`pluginId` provenance). Absent
   *  for filesystem (.ami/.claude/config) hooks. Retained so a hook can be
   *  attributed to — and audited per — the plugin that installed it. */
  pluginName?: string;
}

export interface HookConfigFile {
  hooks: HookConfigEntry[];
}

/**
 * A single file-loaded hook, retained for introspection (Claude Code's per-hook
 * ViewHookMode details): reveals the ACTUAL configured command / prompt / url —
 * not just an aggregate count — so `/hooks` and `--introspect` can show what each
 * hook runs, its matcher, and which settings tier contributed it.
 */
export interface HookDescriptor {
  event: string;
  type: HookConfigType;
  /** The matcher scoping this hook ('' / absent = all targets). */
  matcher?: string;
  /** Settings tier that contributed the hook (project | user | config | …). */
  source: string;
  /** The hook's command, prompt, or URL (sanitized of secret-looking tokens). */
  content: string;
  /** Plugin bundle name when the hook came from a plugin (Claude Code's per-hook
   *  plugin attribution). Absent for filesystem/config hooks. */
  pluginName?: string;
  /** The hook author's friendly status label (Claude Code `statusMessage`), when
   *  set — surfaced in progress/audit views instead of the raw command. */
  statusMessage?: string;
}

/**
 * Per-hook-type display label for the hook's primary content field — Claude Code's
 * ViewHookMode label resolution: command→'Command', http→'URL', and BOTH prompt
 * and agent hooks reuse the 'Prompt' label (an agent hook is a prompt-driven
 * verification agent). Pure; used by {@link HookManager.getHookAuditView}.
 */
export function hookContentLabel(type: HookConfigType): string {
  switch (type) {
    case 'command': return 'Command';
    case 'http': return 'URL';
    case 'prompt':
    case 'agent': return 'Prompt';
  }
}

/**
 * Human-readable text to show WHILE a hook runs / in an audit row (Claude Code's
 * getHookDisplayText): the author-supplied `statusMessage` when present, else the
 * hook's own content (command/prompt/url). Never leaks a secret the content
 * sanitizer already stripped. Pure; used by {@link HookManager.getHookAuditView}.
 */
export function getHookDisplayText(d: { statusMessage?: string; content: string }): string {
  const sm = d.statusMessage?.trim();
  return sm ? sm : d.content;
}

/**
 * Human-readable provenance string for a hook's source tier, with Claude Code's
 * plugin special-case: a plugin-contributed hook appends `(pluginName)` so the
 * user sees WHICH plugin installed it (SelectHookMode /
 * hookSourceDescriptionDisplayString). Pure; used by
 * {@link HookManager.getHookAuditView}.
 */
export function hookSourceDisplayString(source: string, pluginName?: string): string {
  const base = ((): string => {
    switch (source) {
      case 'project': return 'Project settings';
      case 'user': return 'User settings';
      case 'config': return 'Project config';
      case 'managed': return 'Managed policy';
      case 'plugin': return 'Plugin';
      default: return source;
    }
  })();
  return pluginName ? `${base} (${pluginName})` : base;
}

/** A single hook's full audit row (Claude Code's ViewHookMode): identity, matcher,
 *  typed content with its display label, plugin attribution, and the human-readable
 *  source + display text. */
export interface HookAuditRow {
  event: string;
  type: HookConfigType;
  matcher?: string;
  source: string;
  /** Human-readable source description incl. `(pluginName)` for plugin hooks. */
  sourceLabel: string;
  pluginName?: string;
  /** Per-type content label ('Command' | 'Prompt' | 'URL'). */
  contentLabel: string;
  /** The sanitized command / prompt / url. */
  content: string;
  /** What to show while the hook runs (statusMessage || content). */
  displayText: string;
}

/** One matcher group under an event in the introspection view. */
export interface HookEventMatcherGroup {
  /** Raw matcher ('' = all). */
  matcher: string;
  /** Display label ('(all)' for the empty matcher). */
  label: string;
  /** Hooks registered under this matcher. */
  count: number;
  /** Distinct settings tiers contributing hooks under this matcher. */
  sources: string[];
}

/** Per-active-event introspection record (Claude Code's SelectEventMode row):
 *  count + human-readable summary/description + matcher metadata + the matcher
 *  groups (sorted by source priority). */
export interface HookEventDescription {
  event: string;
  count: number;
  summary?: string;
  description?: string;
  matcherField?: string;
  matcherValues?: string[];
  supportsMatcher: boolean;
  matchers: HookEventMatcherGroup[];
}

/** Human-readable content for a hook config (its command / prompt / url),
 *  sanitized of secret-looking tokens before it is surfaced for introspection. */
function describeHookContent(
  hook: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig,
): string {
  switch (hook.type) {
    case 'command': return sanitizeHookOutput(hook.args ? hook.args.join(' ') : hook.command);
    case 'prompt':
    case 'agent': return sanitizeHookOutput(hook.prompt);
    case 'http': return hook.url;
  }
}

/**
 * Translate Claude Code's map-keyed hooks.json into AMI's flat entry array:
 *   { "PreToolUse": [{ matcher?, hooks: [{ type, command, ... }] }] }
 * →  [{ event: 'preToolUse', matcher?, hook: { type, command, ... } }, ...]
 * Capitalized event names (PreToolUse) are lower-camel-cased (preToolUse).
 * Unknown shapes are skipped defensively.
 */
/** Map Claude Code's capitalized tool names to AMI tool names so a matcher like
 *  'Bash' or 'Edit|Write' actually gates the AMI tools at execution time. Shares
 *  the single canonical alias map with the permission-rule parser (no duplicate). */
const CLAUDE_TO_AMI_TOOL: Record<string, string> = PERMISSION_TOOL_ALIASES;

/** Hook-matcher-only tool-name aliases layered OVER the shared permission alias
 *  map. The plan-exit tool is named `ExitPlanMode`/`ExitPlanModeV2` in Claude Code
 *  but `plan_mode` in AMI, so a Claude-Code-format hook matcher of `ExitPlanMode`
 *  must gate AMI's plan-mode tool. Kept hook-local (not in PERMISSION_TOOL_ALIASES)
 *  because the permission layer does not expose a plan-exit tool name. */
const HOOK_MATCHER_EXTRA_ALIASES: Record<string, string> = {
  ExitPlanMode: 'plan_mode',
  ExitPlanModeV2: 'plan_mode',
};

/** Translate the tool-name tokens in a hook matcher ('Edit|Write' → 'file_edit|
 *  file_edit'). Unknown tokens (already-AMI names, custom regex) pass through. */
export function translateHookMatcher(matcher: string): string {
  return matcher.split('|').map(tok => {
    const t = tok.trim();
    return HOOK_MATCHER_EXTRA_ALIASES[t] ?? CLAUDE_TO_AMI_TOOL[t] ?? t;
  }).join('|');
}

export function normalizeMapKeyedHooks(map: unknown): HookConfigEntry[] {
  if (!map || typeof map !== 'object' || Array.isArray(map)) return [];
  const out: HookConfigEntry[] = [];
  for (const [rawEvent, groups] of Object.entries(map as Record<string, unknown>)) {
    const event = rawEvent.charAt(0).toLowerCase() + rawEvent.slice(1);
    if (!Array.isArray(groups)) continue;
    for (const group of groups) {
      if (!group || typeof group !== 'object') continue;
      const rawMatcher = typeof (group as any).matcher === 'string' ? (group as any).matcher : undefined;
      const matcher = rawMatcher !== undefined ? translateHookMatcher(rawMatcher) : undefined;
      const innerHooks = (group as any).hooks;
      if (!Array.isArray(innerHooks)) continue;
      for (const hook of innerHooks) {
        if (hook && typeof hook === 'object') {
          out.push({ event, hook: hook as HookConfigEntry['hook'], ...(matcher ? { matcher } : {}) });
        }
      }
    }
  }
  return out;
}

/**
 * Parse a skill/agent frontmatter `hooks` value into AMI's flat {@link HookConfigEntry}
 * array (parity with Claude Code's per-skill/per-agent frontmatter `hooks` field,
 * registered scoped to the invocation/agent lifecycle by registerFrontmatterHooks).
 *
 * Accepts the authoring shapes used across AMI, reusing the single canonical
 * translators so the two never drift from the on-disk `hooks.json` loader:
 *   - A JSON STRING (inline or a `|` block scalar) holding either shape below —
 *     this is how a SKILL.md / agent frontmatter field is authored, mirroring the
 *     `mcp-servers` / `vars` JSON-string convention, because the line-based
 *     frontmatter parser surfaces nested config as a raw string. Parsed via
 *     `JSON.parse`; malformed JSON yields `[]`.
 *   - AMI's flat array form — `[{ event, hook: {type,...}, matcher? }]` — each entry is
 *     kept only when it has a string `event` and an object `hook`; the matcher's
 *     tool-name tokens are translated via {@link translateHookMatcher} so a
 *     Claude-Code-style `Edit|Write` matcher gates the AMI tools.
 *   - Claude Code's map-keyed form — `{ PreToolUse: [{ matcher?, hooks: [...] }] }` —
 *     delegated verbatim to {@link normalizeMapKeyedHooks}.
 * Any other value (absent/number/boolean) yields `[]` so a malformed `hooks:` block
 * can never crash the loader or register a garbage hook.
 */
export function parseFrontmatterHooks(value: unknown): HookConfigEntry[] {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed.length === 0) return [];
    let parsed: unknown;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      return []; // malformed JSON authoring → no hooks (never throws)
    }
    return parseFrontmatterHooks(parsed);
  }
  if (Array.isArray(value)) {
    const out: HookConfigEntry[] = [];
    for (const raw of value) {
      if (!raw || typeof raw !== 'object' || Array.isArray(raw)) continue;
      const e = raw as Record<string, unknown>;
      if (typeof e.event !== 'string' || e.event.trim().length === 0) continue;
      if (!e.hook || typeof e.hook !== 'object' || Array.isArray(e.hook)) continue;
      const matcher = typeof e.matcher === 'string' ? translateHookMatcher(e.matcher) : undefined;
      out.push({
        event: e.event.trim(),
        hook: e.hook as HookConfigEntry['hook'],
        ...(matcher ? { matcher } : {}),
      });
    }
    return out;
  }
  return normalizeMapKeyedHooks(value);
}

export type EngineFactory = (config: any) => { submit(prompt: string): AsyncIterable<{ type: string; text?: string; error?: string }>; shutdown?(): void };

async function runCommandHookAsync(
  command: string,
  cwd: string,
  env: Record<string, string>,
  timeout: number,
  stdinData?: string,
  args?: string[],
  signal?: AbortSignal,
  shell?: string,
): Promise<{ exitCode: number; stdout: string; stderr: string; output: string }> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) { reject(new Error('Hook cancelled')); return; }
    // Validate the working directory first — a stale/removed cwd would make
    // spawn throw ENOENT, masquerading as a hook failure (resolveHookCwd).
    const safeCwd = resolveHookCwd(cwd);
    const spawnDesc = args && args.length > 0
      ? { file: args[0], args: args.slice(1) }       // exec form — bypass the shell
      : resolveHookShell(command, shell);            // shell form — per-hook interpreter
    const proc = spawn(spawnDesc.file, spawnDesc.args, { cwd: safeCwd, env, stdio: ['pipe', 'pipe', 'pipe'] });

    // Capture stdout and stderr SEPARATELY (Claude Code contract): a command hook's
    // decision JSON is read from stdout, while the human-facing reason comes from
    // stderr. `output` keeps the combined stream for back-compat logging.
    let stdout = '';
    let stderr = '';
    proc.stdout?.on('data', (d: Buffer) => { stdout += d.toString(); });
    proc.stderr?.on('data', (d: Buffer) => { stderr += d.toString(); });

    proc.stdin?.on('error', (err: NodeJS.ErrnoException) => {
      if (err.code !== 'EPIPE') coreLog('hooks', `Hook stdin error: ${err.message}`);
    });

    if (stdinData) {
      try {
        proc.stdin?.write(stdinData, 'utf-8');
        proc.stdin?.end();
      } catch { proc.stdin?.end(); }
    } else {
      proc.stdin?.end();
    }

    const timer = setTimeout(() => {
      proc.kill('SIGKILL');
      reject(new Error(`Hook timed out after ${timeout}ms`));
    }, timeout);

    // Parent/user cancellation: kill the child and surface a distinct "cancelled"
    // outcome (vs timeout) so callers can tell them apart.
    const onAbort = () => {
      proc.kill('SIGKILL');
      clearTimeout(timer);
      reject(new Error('Hook cancelled'));
    };
    if (signal) signal.addEventListener('abort', onAbort, { once: true });
    const detach = () => { if (signal) signal.removeEventListener('abort', onAbort); };

    proc.on('close', (code) => {
      clearTimeout(timer);
      detach();
      resolve({ exitCode: code ?? 1, stdout, stderr, output: stdout + stderr });
    });

    proc.on('error', (err) => {
      clearTimeout(timer);
      detach();
      reject(err);
    });
  });
}

export class HookManager extends EventEmitter {
  private _engineFactory: EngineFactory | null = null;
  private _providerConfig: any = null;
  private _cwd: string = process.cwd();
  private _sessionId: string = '';
  private _workspaceTrusted = false;
  private _interactive = false;
  private _httpHookPolicy: HttpHookPolicy = {};
  /** SSRF validator seam (defaults to the shared web-utils validateUrlSafety).
   *  Injectable so tests can exercise the HTTP-hook success/non-2xx paths against
   *  a loopback mock server that the real validator would (correctly) block. */
  private _validateUrlSafety: typeof validateUrlSafety = validateUrlSafety;
  private _callbackHooks: Map<string, HookCallback[]> = new Map();
  /** Per-callback timeout (ms) for in-process ('function') hooks, keyed by the
   *  callback. Unset callbacks use DEFAULT_CALLBACK_HOOK_TIMEOUT_MS. */
  private _callbackTimeouts = new WeakMap<HookCallback, number>();

  // Gap 1: Tier-aware verdict hold-over
  private _hookTiers = new WeakMap<Function, string>();
  private _denyRulesHold = false;
  private _concurrency = new EventConcurrencyLimiter();
  private _effortLevel: string = 'high';
  /** Absolute paths hooks have asked to watch for FileChanged (via watchPaths
   *  in sessionStart/cwdChanged/fileChanged output). Exposed for a file-watcher
   *  consumer and surfaced via the 'watchPaths' event. */
  private _watchedPaths = new Set<string>();

  /** File-loaded hook descriptors retained for introspection (describeHooks /
   *  describeEvents). Populated by processHookEntry and reset at the start of each
   *  loadFromFile so a reload reflects the current on-disk config. */
  private _descriptors: HookDescriptor[] = [];

  /** Settings/hooks/config files that failed to parse during the last
   *  {@link loadFromFile}. Reset at the start of each load so it reflects the
   *  current on-disk state. Surfaced via {@link getConfigErrors} + the
   *  `configError` event + `/hooks` so a malformed file (which silently drops all
   *  its hooks) is visible instead of only logged. */
  private _configErrors: HookConfigError[] = [];

  /** The most recent Stop-hook run summary (per-hook timings + errors). Retained
   *  for introspection; also emitted live on the `stopHookSummary` event. */
  private _lastStopHookSummary: StopHookSummary | null = null;

  /**
   * When enabled, org/builtin deny verdicts cannot be loosened by user-tier hooks.
   */
  setDenyRulesHold(hold: boolean): void { this._denyRulesHold = hold; }

  constructor() {
    super();
  }

  setEngineFactory(factory: EngineFactory): void { this._engineFactory = factory; }
  setProviderConfig(config: any): void { this._providerConfig = config; }
  setCwd(cwd: string): void { this._cwd = cwd; }
  setSessionId(id: string): void { this._sessionId = id; }

  setWorkspaceTrusted(trusted: boolean): void { this._workspaceTrusted = trusted; }
  setEffortLevel(level: string): void { this._effortLevel = level; }
  getEffortLevel(): string { return this._effortLevel; }
  isWorkspaceTrusted(): boolean { return this._workspaceTrusted; }

  /** Whether this is an interactive (TUI) session. Drives the workspace-trust
   *  gate: in interactive mode untrusted workspaces skip ALL hooks; in
   *  non-interactive/SDK mode trust is implicit (matches Claude Code). */
  setInteractive(interactive: boolean): void { this._interactive = interactive; }
  isInteractive(): boolean { return this._interactive; }

  /** Install the merged HTTP-hook policy (URL allowlist + header env allowlist). */
  setHttpHookPolicy(policy: HttpHookPolicy | undefined): void { this._httpHookPolicy = policy ?? {}; }
  getHttpHookPolicy(): HttpHookPolicy { return this._httpHookPolicy; }

  /** Permission mode for engine-backed (prompt/agent) hooks. Defaults to
   *  'auto-allow' but the session can set its real permission mode so a hook's
   *  sub-engine runs under the session's permission context, not blanket allow. */
  private _hookEnginePermissionMode: string = 'auto-allow';
  setHookEnginePermissionMode(mode: string): void { this._hookEnginePermissionMode = mode; }
  getHookEnginePermissionMode(): string { return this._hookEnginePermissionMode; }

  /** When true (org policy), ONLY admin/managed-settings hooks execute — project,
   *  user, and plugin hooks are not loaded (Claude Code's allowManagedHooksOnly). */
  private _allowManagedHooksOnly = false;
  setAllowManagedHooksOnly(on: boolean): void { this._allowManagedHooksOnly = on; }
  isAllowManagedHooksOnly(): boolean { return this._allowManagedHooksOnly; }

  /**
   * Read the raw `strictPluginOnlyCustomization` value from the managed/policy
   * settings file so {@link isRestrictedToPluginOnly} can decide whether the
   * `hooks` surface is locked to plugin/managed-only sources. Managed-sourced by
   * construction (this is the admin policy file). Never throws: a missing or
   * malformed file degrades to `undefined` (nothing locked). Parity: Claude's
   * `getSettingsForSource('policySettings')?.strictPluginOnlyCustomization`.
   */
  private readManagedPluginOnlyPolicy(managedPath: string): unknown {
    try {
      const parsed = JSON.parse(stripBom(fs.readFileSync(managedPath, 'utf-8')));
      return parsed && typeof parsed === 'object'
        ? (parsed as Record<string, unknown>).strictPluginOnlyCustomization
        : undefined;
    } catch {
      return undefined;
    }
  }

  /** Managed-policy kill-switch (Claude Code's disableAllHooks). When set via a
   *  managed source it skips ALL hooks, including managed ones. */
  private _disableAllHooks = false;

  /**
   * Set the disableAllHooks kill-switch with Claude Code's managed > non-managed
   * precedence: a MANAGED `disableAllHooks` disables ALL hooks (including
   * managed); a NON-managed `disableAllHooks` can only disable non-managed hooks
   * (i.e. it collapses to allowManagedHooksOnly — managed hooks still run) and can
   * never re-enable a managed disable. Consulted by loadFromFile before any hook
   * is registered.
   */
  setDisableAllHooks(disabled: boolean, managed = true): void {
    if (managed) this._disableAllHooks = disabled;
    else if (disabled) this._allowManagedHooksOnly = true;
  }
  /** True when every hook is disabled — either the managed `disableAllHooks`
   *  policy OR the `CLAUDE_CODE_SIMPLE` (AMI_SIMPLE / AMI_DISABLE_HOOKS) env
   *  kill-switch. The env switch is evaluated live so a hook-free session needs
   *  no settings file. */
  isAllHooksDisabled(): boolean { return this._disableAllHooks || isHookKillSwitchEnv(); }

  /** Override the HTTP-hook SSRF validator (tests only; defaults to the shared
   *  web-utils validateUrlSafety). */
  setUrlSafetyValidator(fn: typeof validateUrlSafety): void { this._validateUrlSafety = fn; }

  registerCallback(event: string, callback: HookCallback, timeoutMs?: number): () => void {
    const hooks = this._callbackHooks.get(event) || [];
    hooks.push(callback);
    this._callbackHooks.set(event, hooks);
    // Per-callback timeout (Claude Code addFunctionHook `timeout`): bound a
    // slow/stuck in-process callback so it cannot stall the turn.
    if (timeoutMs !== undefined) this._callbackTimeouts.set(callback, timeoutMs);
    return () => {
      const current = this._callbackHooks.get(event) || [];
      const index = current.indexOf(callback);
      if (index >= 0) current.splice(index, 1);
      this._callbackTimeouts.delete(callback);
    };
  }

  /**
   * Execute all registered callback hooks for the given event.
   * Returns the first non-void HookDecisionOutput if any callback returns one.
   */
  async executeCallbacks(event: string, data: unknown): Promise<HookDecisionOutput | void> {
    const hooks = this._callbackHooks.get(event);
    if (!hooks || hooks.length === 0) return;
    return this.withConcurrencyGate(`cb:${event}`, async () => {
      for (const cb of hooks) {
        try {
          // Bound each in-process callback by its per-callback timeout (or the
          // default) so a stuck function hook cannot hang the event.
          const timeoutMs = this._callbackTimeouts.get(cb) ?? DEFAULT_CALLBACK_HOOK_TIMEOUT_MS;
          const result = await runWithTimeout(
            Promise.resolve(cb(event, data)), timeoutMs, `Callback hook "${event}"`,
          );
          if (result && typeof result === 'object') return result as HookDecisionOutput;
        } catch (err) {
          coreLog('hooks', `Callback hook error for event "${event}": ${err}`);
        }
      }
      return undefined as unknown as HookDecisionOutput | void;
    });
  }

  // ---- Async ('background') hook registry ----
  /** In-flight async hook promises keyed to their metadata (firing event +
   *  start timestamp), so they can be awaited (finalize) or cancelled (kill) at
   *  shutdown AND introspected while running (which events, how long) instead of
   *  leaking as untracked fire-and-forget promises. */
  private _pendingAsync = new Map<Promise<unknown>, { event: string; startedAt: number }>();
  /** Shutdown signal shared by all async hooks; aborting it tears down every
   *  in-flight async hook (command child killed / HTTP request aborted / engine
   *  hook query aborted). Reset after cancelAll so later hooks are not born
   *  pre-aborted. */
  private _asyncAbort = new AbortController();

  /** Live view of every in-flight async hook (introspection surface): the event
   *  it fired for and how long it has been running. Claude Code tracks pending
   *  background hooks with elapsed time so a stall is visible; this exposes the
   *  same (the richer companion to {@link getPendingAsyncCount}). */
  getPendingAsyncHooks(): Array<{ event: string; elapsedMs: number }> {
    const now = Date.now();
    return Array.from(this._pendingAsync.values()).map(r => ({ event: r.event, elapsedMs: now - r.startedAt }));
  }

  /** Number of async hooks still running (introspection surface). */
  getPendingAsyncCount(): number { return this._pendingAsync.size; }

  /**
   * Fire an async ('background') hook: run it under a shutdown-cancellable signal
   * (combined with any parent/turn signal), track the promise so it can be
   * finalized/cancelled at shutdown, and swallow its errors. Replaces the old
   * untracked fire-and-forget firing of async hooks.
   */
  private fireAsyncHook(
    config: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig,
    cwd: string,
    eventData: string,
    event: string,
    parentSignal?: AbortSignal,
    // Fired ONLY after the background hook completes successfully, so a
    // `once:true` async hook self-removes just like its synchronous sibling
    // (previously an async once-hook was never removed and re-fired every event).
    onSuccess?: () => void,
    // Per-hook env-file hint forwarded to runHookByType (CLAUDE_ENV_FILE parity).
    envFileHint?: { event: string; index: number },
  ): void {
    const ac = new AbortController();
    const onAbort = () => ac.abort();
    if (this._asyncAbort.signal.aborted || parentSignal?.aborted) {
      ac.abort();
    } else {
      this._asyncAbort.signal.addEventListener('abort', onAbort, { once: true });
      parentSignal?.addEventListener('abort', onAbort, { once: true });
    }
    // A background hook is bounded on its OWN short clock (asyncTimeout, default
    // 15s), independent of the long synchronous command/HTTP default: once the
    // budget elapses the hook is aborted (child killed / request aborted) so a
    // runaway background hook cannot linger for minutes. A hook that settles
    // first clears the timer in the finally below.
    const asyncTimeoutMs = validateHookTimeout(config.asyncTimeout ?? DEFAULT_ASYNC_HOOK_TIMEOUT_MS);
    const timer = setTimeout(onAbort, asyncTimeoutMs);
    if (typeof timer.unref === 'function') timer.unref();
    const outcome = { success: true };
    const p: Promise<unknown> = this.runHookByType(config, cwd, eventData, ac.signal, outcome, envFileHint)
      .then(() => { if (outcome.success && onSuccess) onSuccess(); })
      .catch(() => { /* async hooks never surface errors to the turn */ })
      .finally(() => {
        clearTimeout(timer);
        this._asyncAbort.signal.removeEventListener('abort', onAbort);
        parentSignal?.removeEventListener('abort', onAbort);
        this._pendingAsync.delete(p);
      });
    this._pendingAsync.set(p, { event, startedAt: Date.now() });
  }

  /**
   * Canonical per-event hook runner for config-registered hooks. Runs the hook
   * (sync or fire-and-forget async) and handles `once:true` self-removal with
   * Claude Code's onHookSuccess semantics: the one-shot hook is spliced out ONLY
   * after a SUCCESSFUL execution (not when it errored, timed out, or was blocked
   * by an error), for BOTH the sync and async paths. Returns the flattened result
   * string; for an async hook it returns `''` (no blocking result is awaited),
   * which every event arm already handles identically to a no-output hook.
   */
  private async runArmHook<T>(
    config: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig,
    cwd: string,
    eventData: string,
    signal: AbortSignal | undefined,
    arr: T[],
    fn: T,
    event: string,
  ): Promise<string> {
    // Index of this hook within its event array — keys the per-hook env-file path
    // so two hooks on the same event export into distinct, deterministic fragments.
    const idx = arr.indexOf(fn);
    const envFileHint = idx >= 0 ? { event, index: idx } : undefined;
    if (config.async) {
      this.fireAsyncHook(
        config, cwd, eventData, event, signal,
        config.once ? () => this.removeHookOnce(arr, fn, event) : undefined,
        envFileHint,
      );
      return '';
    }
    const outcome = { success: true };
    const result = await this.runHookByType(config, cwd, eventData, signal, outcome, envFileHint);
    if (config.once && outcome.success) this.removeHookOnce(arr, fn, event);
    return result;
  }

  /** Await every in-flight async hook to settle (graceful shutdown). */
  async finalizePendingAsyncHooks(): Promise<void> {
    await Promise.all(Array.from(this._pendingAsync.keys()));
  }

  /** Cancel every in-flight async hook (hard shutdown) and reset the signal so
   *  subsequently-fired hooks are not born aborted. Returns the count cancelled. */
  cancelAllAsyncHooks(): number {
    const count = this._pendingAsync.size;
    this._asyncAbort.abort();
    this._asyncAbort = new AbortController();
    return count;
  }

  /** Dynamically-registered FileChanged watch paths contributed by hook output. */
  getWatchedPaths(): string[] { return Array.from(this._watchedPaths); }

  /**
   * Parse `watchPaths` from a hook's output, register any new absolute paths,
   * and emit a 'watchPaths' event so a file-watcher consumer can start watching
   * them. Returns the parsed paths (empty when none). Idempotent per path.
   */
  private registerWatchPaths(event: string, result: string): string[] {
    const paths = parseWatchPaths(result);
    if (paths.length === 0) return [];
    const added: string[] = [];
    for (const p of paths) {
      if (!this._watchedPaths.has(p)) { this._watchedPaths.add(p); added.push(p); }
    }
    if (added.length > 0) this.emit('watchPaths', { event, paths: added });
    return paths;
  }

  private buildHookEnv(event: string, toolName?: string, envFileHint?: { event: string; index: number }): Record<string, string> {
    const base: Record<string, string> = {
      ...sanitizeHookEnv(process.env as Record<string, string | undefined>),
      SUPERINFERENCE_PROJECT_DIR: this._cwd,
      SUPERINFERENCE_SESSION_ID: this._sessionId,
      SUPERINFERENCE_TOOL_NAME: toolName || '',
      SUPERINFERENCE_HOOK_EVENT: event,
      SUPERINFERENCE_CWD: this._cwd,
      SUPERINFERENCE_MODEL: this._providerConfig?.model ?? '',
      SUPERINFERENCE_PROVIDER: this._providerConfig?.provider ?? '',
      SUPERINFERENCE_EFFORT: this._effortLevel,
      // Session-environment handoff: a lifecycle hook can write an
      // `<event>-hook-<index>.sh` fragment (e.g. `export FOO=bar`,
      // `source .venv/bin/activate`) into this dir and it is folded into the
      // script prepended to subsequent bash commands, so hook-exported env
      // persists across stateless tool calls (Claude Code CLAUDE_ENV_FILE parity).
      SUPERINFERENCE_SESSION_ENV_DIR: sessionEnvDirPath(this._sessionId),
    };
    // Per-hook env-file path (Claude Code's hookIndex-derived CLAUDE_ENV_FILE):
    // an env-ingestible lifecycle hook (setup/sessionStart/cwdChanged/fileChanged)
    // gets the EXACT deterministic fragment path to append `export KEY=VAL` to,
    // so it does not have to reconstruct the `<event>-hook-<index>.sh` naming
    // convention itself. Re-ingested by getSessionEnvironmentScript for later bash
    // calls. The CLAUDE_ENV_FILE alias keeps cross-framework hooks working verbatim.
    if (envFileHint) {
      const envFile = hookEnvFragmentPath(this._sessionId, envFileHint.event, envFileHint.index);
      if (envFile) {
        // Ensure the session-env directory exists so the hook can append to the
        // handed path without first mkdir-ing it (a missing dir would make the
        // hook's `>> "$CLAUDE_ENV_FILE"` fail silently on the fail-open path).
        try { fs.mkdirSync(path.dirname(envFile), { recursive: true }); } catch { /* best-effort */ }
        base.SUPERINFERENCE_ENV_FILE = envFile;
        base.CLAUDE_ENV_FILE = envFile;
      }
    }
    return base;
  }

  /**
   * Surface a hook EXECUTION failure (throw / hard-timeout / transport error /
   * non-2xx) as a visible `hookError` event so a crashed or misconfigured hook is
   * never silent on the fail-OPEN path — Claude Code always yields a
   * `hook_error_during_execution` attachment with the formatted error + hook name.
   * This is distinct from a hook's DENY *decision* (a successful run that chose to
   * block, already surfaced to the model). The engine forwards it to the operator.
   */
  private emitHookError(config: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig, error: string): void {
    this.emit('hookError', { hookType: config.type, hook: describeHookContent(config), error });
  }

  private postSamplingHooks: HookFn[] = [];
  private stopHooks: StopHookFn[] = [];
  /** Re-entrancy guard: a Stop hook (or its blocking-error re-entry) must not
   *  recursively trigger Stop hooks again (Claude Code's stopHookActive). */
  private _stopHookActive = false;
  private errorHooks: HookFn[] = [];
  private preToolUseHooks: PreToolUseHookFn[] = [];
  private postToolUseHooks: PostToolUseHookFn[] = [];
  private preSamplingHooks: PreSamplingHookFn[] = [];
  private sessionStartHooks: SessionHookFn[] = [];
  private sessionEndHooks: SessionHookFn[] = [];
  private subagentStartHooks: SubagentStartHookFn[] = [];
  private subagentStopHooks: SubagentHookFn[] = [];
  private preCompactHooks: PreCompactHookFn[] = [];
  private postCompactHooks: CompactHookFn[] = [];
  private permissionRequestHooks: PermissionHookFn[] = [];
  private permissionDeniedHooks: PermissionHookFn[] = [];
  private userPromptSubmitHooks: UserPromptHookFn[] = [];
  private taskCreatedHooks: TaskEventHookFn[] = [];
  private taskCompletedHooks: TaskCompletedHookFn[] = [];
  private teammateIdleHooks: TeammateIdleHookFn[] = [];
  private postToolUseFailureHooks: PostToolUseHookFn[] = [];
  private notificationHooks: NotificationHookFn[] = [];
  private setupHooks: SessionHookFn[] = [];
  private configChangeHooks: ConfigChangeHookFn[] = [];
  private instructionsLoadedHooks: InstructionsHookFn[] = [];
  private worktreeCreateHooks: WorktreeCreateHookFn[] = [];
  private worktreeRemoveHooks: WorktreeHookFn[] = [];
  private cwdChangedHooks: CwdChangedHookFn[] = [];
  private fileChangedHooks: FileChangeHookFn[] = [];
  private elicitationHooks: ElicitationHookFn[] = [];
  private elicitationResultHooks: ElicitationHookFn[] = [];
  private stopFailureHooks: StopFailureHookFn[] = [];

  /**
   * Load hooks from .ami/hooks.json.
   * Command hooks execute a shell command; exit code 0 = allow, non-zero = deny.
   */
  loadFromFile(cwd: string): void {
    // Reset the introspection descriptor set so a reload reflects the current
    // on-disk config (no stale accumulation across loads).
    this._descriptors = [];
    // Reset the parse-error set too, so a fixed file clears its prior error.
    this._configErrors = [];
    // Folder-trust grant (parity: Claude Code's checkHasTrustDialogAccepted): consult
    // the persisted `hasTrustDialogAccepted` project-config knob so a user who recorded
    // trust for this workspace actually gets project-tier hooks (and, interactively, all
    // hooks) loaded instead of silently skipped. Only PROMOTES trust — an explicitly-set
    // trusted flag is never demoted, and an unset/false knob keeps the untrusted default.
    if (!this._workspaceTrusted && resolveWorkspaceTrustAccepted(cwd)) {
      this._workspaceTrusted = true;
    }
    // Defense-in-depth: in an interactive session an untrusted workspace fires
    // NO hooks of any tier (Claude Code's shouldSkipHookDueToTrust). Project
    // entries are also gated below, but this blocks user/config-tier hooks too.
    if (shouldSkipHookDueToTrust(this._workspaceTrusted, this._interactive)) {
      coreLog('hooks', 'Skipping ALL hooks: workspace not trusted (interactive)');
      return;
    }
    // Kill-switch: a managed `disableAllHooks` policy OR the CLAUDE_CODE_SIMPLE
    // (AMI_SIMPLE / AMI_DISABLE_HOOKS) env switch skips EVERY hook, including
    // managed ones (Claude Code's shouldDisableAllHooksIncludingManaged).
    if (this.isAllHooksDisabled()) {
      coreLog('hooks', "Skipping ALL hooks: 'disableAllHooks' policy / CLAUDE_CODE_SIMPLE env is set");
      return;
    }
    const filePath = path.join(cwd, '.ami', 'hooks.json');
    const userHooksPath = path.join(os.homedir(), '.ami', 'hooks.json');

    // Each source carries a provenance `kind` (hook-protection.ts HookSourceKind)
    // that the managed-only policy filter consults. `managed` = admin/policy
    // settings; `plugin` = plugin-bundle contributed. Both are admin-trusted
    // sources that bypass the strictPluginOnlyCustomization hooks lock.
    const sources: Array<{ path: string; source: 'project' | 'user'; kind: HookSourceKind; pluginName?: string; pluginRoot?: string }> = [];
    // Admin/managed settings (highest authority) — the ONLY source honored when
    // allowManagedHooksOnly is set.
    const managedPath = process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    if (managedPath) sources.push({ path: managedPath, source: 'user', kind: 'managed' });
    sources.push(
      { path: filePath, source: 'project', kind: 'project' },
      // Per-directory LOCAL override tier: `.ami/settings.local.json` is the
      // developer's untracked, machine-local settings file (gitignored by
      // convention). It is read AFTER the shared project `.ami/hooks.json` so a
      // local entry layers on top with higher precedence — matching Claude Code's
      // `settings.local.json` `local` tier (addDirPluginSettings / settings-guard).
      { path: path.join(cwd, '.ami', 'settings.local.json'), source: 'project', kind: 'project' },
      { path: userHooksPath, source: 'user', kind: 'user' },
      // Claude Code compatibility: load hooks from .claude/settings.json
      // ({ "hooks": { "PreToolUse": [{ matcher, hooks: [...] }] } }). The
      // map-keyed `hooks` field is handled by normalizeMapKeyedHooks below, and
      // project-scoped entries are gated by workspace trust. (R2-M8)
      { path: path.join(cwd, '.claude', 'settings.json'), source: 'project', kind: 'project' },
      // Claude Code's per-directory local override: `.claude/settings.local.json`
      // is read right after `.claude/settings.json` (local-after-project), so a
      // developer's untracked local settings take precedence over the shared,
      // committed project settings — the three-layer user < project < local
      // precedence (updateConfig three-layer settings precedence).
      { path: path.join(cwd, '.claude', 'settings.local.json'), source: 'project', kind: 'project' },
      { path: path.join(os.homedir(), '.claude', 'settings.json'), source: 'user', kind: 'user' },
    );

    // Plugin bundles contribute hooks.json (project-scoped ones gated by trust).
    // Skipped entirely under allowManagedHooksOnly (only managed hooks run).
    if (canRunHookUnderManagedOnly('plugin', this._allowManagedHooksOnly)) try {
      const { discoverPlugins } = require('./plugins');
      const pluginDirs: Array<[string, 'user' | 'project']> = [
        [path.join(resolveConfigDir(), 'plugins'), 'user'],
        [path.join(cwd, '.ami', 'plugins'), 'project'],
      ];
      for (const [dir, src] of pluginDirs) {
        for (const bundle of discoverPlugins(dir)) {
          // A disabled plugin must NOT contribute executable hooks: skip bundles
          // whose root carries a `.disabled` marker (mirrors skill-manager gating).
          if (fs.existsSync(path.join(bundle.root, '.disabled'))) {
            coreLog('hooks', `Skipping hooks from disabled plugin: ${bundle.root}`);
            continue;
          }
          if (bundle.hooksFile) sources.push({ path: bundle.hooksFile, source: src, kind: 'plugin', pluginName: bundle.name, pluginRoot: bundle.root });
        }
      }
    } catch { /* plugins module optional */ }

    // Org policy: when allowManagedHooksOnly is set, drop every non-managed source
    // so only admin/managed-settings hooks execute. The provenance-based decision
    // is owned by hook-protection.ts (filterManagedOnlyHooks), the single authority
    // for "may this source kind run under the policy".
    let effectiveSources = filterManagedOnlyHooks(sources, this._allowManagedHooksOnly);

    // Managed per-surface lock: `strictPluginOnlyCustomization` (admin/managed-only
    // policy) can confine the `hooks` surface to plugin- and managed-provided sources
    // only, dropping user/project filesystem hooks (.ami/.claude). Composes with
    // allowManagedHooksOnly (which is stricter — managed only). Parity: Claude Code's
    // strictPluginOnlyCustomization. The policy lives in the managed settings file.
    if (managedPath && isRestrictedToPluginOnly('hooks', this.readManagedPluginOnlyPolicy(managedPath))) {
      effectiveSources = effectiveSources.filter(s => s.kind === 'managed' || s.kind === 'plugin');
      coreLog('hooks', "strictPluginOnlyCustomization locks the 'hooks' surface: loading plugin/managed hooks only");
    }

    // Content-identity de-dup across overlapping sources: the same hook defined
    // in two places (e.g. .ami/hooks.json and .claude/settings.json) registers once.
    const accepted: Array<{ event: string; hook: HookConfigEntry['hook'] }> = [];
    const alreadyAccepted = (entry: HookConfigEntry): boolean =>
      accepted.some(a => a.event === entry.event && entry.hook && isHookEqual(a.hook, entry.hook));

    for (const { path: p, source, pluginName, pluginRoot } of effectiveSources) {
      let raw: string;
      try {
        raw = fs.readFileSync(p, 'utf-8');
      } catch {
        continue;
      }
      try {
        const config: HookConfigFile = JSON.parse(stripBom(raw));
        // Accept both AMI's array form and Claude Code's map-keyed form
        // ({ "PreToolUse": [{ matcher, hooks: [...] }] }).
        const entries = Array.isArray(config.hooks)
          ? config.hooks
          : normalizeMapKeyedHooks(config.hooks);
        if (!entries.length) continue;
        for (const entry of entries) {
          entry._source = source;
          // Carry the plugin bundle's name onto each entry it contributed so the
          // retained descriptor can attribute the hook to its plugin.
          if (pluginName) entry.pluginName = pluginName;
          // Carry the plugin bundle's base directory onto a command hook so its
          // `${CLAUDE_PLUGIN_ROOT}` placeholders / CLAUDE_PLUGIN_ROOT env resolve to
          // the bundle root at execution time (Claude Code plugin-hook substitution).
          if (pluginRoot && entry.hook && entry.hook.type === 'command') entry.hook.pluginRoot = pluginRoot;
          if (source === 'project' && !this._workspaceTrusted) {
            coreLog('hooks', `Skipping project hook (workspace not trusted): ${entry.event}`);
            continue;
          }
          if (alreadyAccepted(entry)) {
            coreLog('hooks', `Skipping duplicate hook for event "${entry.event}"`);
            continue;
          }
          if (entry.hook) accepted.push({ event: entry.event, hook: entry.hook });
          this.processHookEntry(entry, cwd);
        }
      } catch (err) {
        this.recordConfigError(p, err);
      }
    }

    if (canRunHookUnderManagedOnly('config', this._allowManagedHooksOnly)) {
      const configPath = path.join(cwd, '.ami', 'config.json');
      try {
        if (fs.existsSync(configPath)) {
          const raw = fs.readFileSync(configPath, 'utf-8');
          const config = JSON.parse(stripBom(raw));
          if (config.hooks && Array.isArray(config.hooks)) {
            for (const entry of config.hooks) {
              entry._source = 'config';
              this.processHookEntry(entry, cwd);
            }
          }
        }
      } catch (err) {
        this.recordConfigError(configPath, err);
      }
    }
  }

  /**
   * Record a settings/hooks/config file that failed to parse: log it, retain it
   * for {@link getConfigErrors}/`/hooks`, and emit a `configError` event so a live
   * consumer (e.g. a file-watcher reload) can surface it. A malformed file
   * silently disables EVERY hook it would contribute, so making it visible matters
   * (Claude Code's "silently disables ALL settings from that file" warning).
   */
  private recordConfigError(filePath: string, err: unknown): void {
    const message = err instanceof Error ? err.message : String(err);
    coreLog('hooks', `Invalid hooks config at ${filePath}: ${message}`);
    this._configErrors.push({ path: filePath, error: message });
    this.emit('configError', { path: filePath, error: message });
  }

  /**
   * Introspection: settings/hooks/config files that failed to parse during the
   * last {@link loadFromFile}. Returns copies so callers cannot mutate the set.
   * Surfaced via `/hooks` + `--introspect` (parity with the MCP manager's
   * getConfigErrors) so a broken config file is never a silent no-op.
   */
  getConfigErrors(): HookConfigError[] {
    return this._configErrors.map(e => ({ ...e }));
  }

  /** The most recent Stop-hook run summary (per-hook timings + errors), or null if
   *  no Stop hooks have run yet. Returns a deep copy (introspection surface). */
  getLastStopHookSummary(): StopHookSummary | null {
    const s = this._lastStopHookSummary;
    return s ? { ...s, hookInfos: s.hookInfos.map(h => ({ ...h })), hookErrors: [...s.hookErrors] } : null;
  }

  /**
   * Register a set of already-parsed {@link HookConfigEntry}s scoped to THIS engine
   * instance (parity with Claude Code's registerFrontmatterHooks, which installs a
   * skill/agent's own frontmatter hooks for the duration of its invocation). The
   * forked-skill / spawned-subagent sub-engine is a fresh short-lived engine, so a
   * hook registered here lives exactly as long as that engine — the invocation-scoped
   * lifecycle boundary — and is torn down naturally when the sub-engine shuts down
   * (no dangling global hook).
   *
   * The same managed kill-switches as {@link loadFromFile} apply: `disableAllHooks`
   * skips everything, and under `allowManagedHooksOnly` these author-sourced (non-
   * managed) hooks do NOT register. Each entry is registered through the shared
   * {@link processHookEntry} path (so event-protection tiers, introspection
   * descriptors, and matcher gating all behave identically to file-loaded hooks),
   * defaulting the provenance tier to `project` when the caller left `_source` unset.
   */
  registerConfigEntries(entries: readonly HookConfigEntry[], cwd: string): void {
    if (!entries || entries.length === 0) return;
    if (this.isAllHooksDisabled()) {
      coreLog('hooks', 'Skipping frontmatter hooks: disableAllHooks policy / env is set');
      return;
    }
    if (this._allowManagedHooksOnly) {
      coreLog('hooks', 'Skipping frontmatter hooks: allowManagedHooksOnly policy is set');
      return;
    }
    for (const raw of entries) {
      if (!raw || typeof raw.event !== 'string' || !raw.hook) continue;
      const entry: HookConfigEntry = { ...raw, _source: raw._source ?? 'project' };
      this.processHookEntry(entry, cwd);
    }
  }

  private processHookEntry(entry: HookConfigEntry, cwd: string): void {
        const hookConfig = entry.hook;
        if (!hookConfig) return;
        const matcher = entry.matcher;
        const tier = entry._source || 'user';

        // Gap 15: Block user-tier hooks on protected events
        if (!canHookEvent(entry.event, tier as import('./hook-protection').HookTier)) {
          coreLog('hooks', `Blocked ${tier}-tier hook on protected event "${entry.event}"`);
          return;
        }

        // Retain an introspection descriptor for every accepted (unblocked)
        // file-loaded hook so /hooks + --introspect can reveal its actual
        // command/prompt/url, matcher, and source tier.
        this._descriptors.push({
          event: entry.event,
          type: hookConfig.type,
          ...(matcher ? { matcher } : {}),
          source: tier,
          content: describeHookContent(hookConfig),
          ...(entry.pluginName ? { pluginName: entry.pluginName } : {}),
          ...(hookConfig.statusMessage ? { statusMessage: hookConfig.statusMessage } : {}),
        });

        switch (entry.event) {
        case 'preToolUse': {
          const fn: PreToolUseHookFn = async (ctx) => {
            if (hookConfig.if && !matchesHookIf(hookConfig.if, ctx.toolName, ctx.toolInput)) return { action: 'allow' };
            if (matcher && !matchesPattern(matcher, ctx.toolName)) return { action: 'allow' };
            const eventData = serializeHookEvent({ event: 'preToolUse', toolName: ctx.toolName, toolInput: ctx.toolInput });
            const result = await this.runArmHook(hookConfig, cwd, eventData, ctx.abortSignal, this.preToolUseHooks, fn, 'preToolUse');
            if (result.startsWith('DENY:')) return { action: 'deny', reason: result.slice(5).trim() };
            // ASK: a PreToolUse hook (decision/permissionDecision 'ask') forces an
            // interactive permission prompt rather than allowing/denying outright.
            // The sentinel carries the optional reason to show at the prompt; a
            // MODIFY-style updatedInput can also travel with it when the hook wants
            // the user to approve a rewritten input (Claude Code hookPermissionResult
            // { behavior:'ask', updatedInput, message }).
            if (result.startsWith('ASK:')) return { action: 'ask', reason: result.slice(4).trim() || undefined };
            if (result.startsWith('MODIFY:')) {
              try {
                const parsed = JSON.parse(result.slice(7));
                if (parsed.additionalContext && !parsed.updatedInput && Object.keys(parsed).length === 1) {
                  return { action: 'allow', additionalContext: parsed.additionalContext };
                }
                return { action: 'modify', updatedInput: parsed.updatedInput ?? parsed, additionalContext: parsed.additionalContext };
              } catch { /* ignore parse errors */ }
            }
            if (result.startsWith('SYSTEM:')) return { action: 'allow', additionalContext: result.slice(7).trim() };
            return { action: 'allow' };
          };
          this.onPreToolUse(fn, tier);
          break;
        }
        case 'postToolUse': {
          const fn: PostToolUseHookFn = async (ctx) => {
            if (hookConfig.if && !matchesHookIf(hookConfig.if, ctx.toolName, ctx.toolInput)) return;
            if (matcher && !matchesPattern(matcher, ctx.toolName)) return;
            const eventData = serializeHookEvent({ event: 'postToolUse', toolName: ctx.toolName, toolInput: ctx.toolInput, toolOutput: ctx.toolOutput, isError: ctx.isError, duration_ms: ctx.durationMs });
            const result = await this.runArmHook(hookConfig, cwd, eventData, ctx.abortSignal, this.postToolUseHooks, fn, 'postToolUse');
            // A PostToolUse hook may block (DENY / exit-2) or request a stop
            // (continue:false) — both surface via the DENY sentinel; feed the
            // reason back to the model as a blocking error (Claude Code parity).
            const blockingError = extractHookBlockingError(result);
            if (blockingError) return { blockingError };
            const parsed = tryParseHookJson(result);
            if (parsed) {
              const out: PostToolUseHookResult = {};
              if (typeof parsed.additionalContext === 'string') out.additionalContext = parsed.additionalContext;
              if (typeof parsed.updatedToolOutput === 'string') out.updatedToolOutput = parsed.updatedToolOutput;
              // Claude Code's PostToolUse hooks rewrite MCP tool output via the
              // `updatedMCPToolOutput` key (typed `unknown`). Accept it as an alias
              // of updatedToolOutput — updatedToolOutput wins if both are present;
              // a non-string value is JSON-serialized into the string channel.
              else if (parsed.updatedMCPToolOutput !== undefined) {
                out.updatedToolOutput = typeof parsed.updatedMCPToolOutput === 'string'
                  ? parsed.updatedMCPToolOutput
                  : JSON.stringify(parsed.updatedMCPToolOutput);
              }
              if (parsed.continue === false) { out.preventContinuation = true; if (typeof parsed.stopReason === 'string') out.stopReason = parsed.stopReason; }
              if (out.additionalContext || out.updatedToolOutput || out.preventContinuation) return out;
            }
            return undefined;
          };
          this.onPostToolUse(fn);
          break;
        }
        case 'preSampling': {
          const fn: PreSamplingHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('preSampling', matcher, ctx as unknown as Record<string, unknown>)) return { action: 'allow' };
            const eventData = serializeHookEvent({ event: 'preSampling', samplingOrigin: ctx.samplingOrigin });
            const result = await this.runArmHook(hookConfig, cwd, eventData, ctx.abortSignal, this.preSamplingHooks, fn, 'preSampling');
            if (result.startsWith('DENY:')) return { action: 'deny', reason: result.slice(5).trim() };
            return { action: 'allow' };
          };
          this.onPreSampling(fn, tier);
          break;
        }
        case 'postSampling': {
          const fn: HookFn = async (ctx) => {
            if (!matcherAppliesToCtx('postSampling', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'postSampling', samplingOrigin: ctx.samplingOrigin });
            await this.runArmHook(hookConfig, cwd, eventData, ctx.abortSignal, this.postSamplingHooks, fn, 'postSampling');
          };
          this.onPostSampling(fn);
          break;
        }
        case 'stop': {
          const fn: StopHookFn = async (ctx) => {
            // Claude Code parity: a Stop hook reads `stop_hook_active` (to break a
            // block→retry recursion) and `last_assistant_message` (the final model
            // text) from its stdin without reading the transcript file.
            const lastText = ctx.lastAssistantMessage?.content?.trim() || undefined;
            const eventData = serializeHookEvent({
              event: 'stop',
              stop_hook_active: ctx.stopHookActive ?? false,
              ...(lastText ? { last_assistant_message: lastText } : {}),
            });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.stopHooks, fn, 'stop');
            const blockingError = extractHookBlockingError(result);
            if (blockingError) return { blockingError };
            return undefined;
          };
          this.onStop(fn);
          break;
        }
        case 'error': {
          const fn: HookFn = async () => {
            const eventData = serializeHookEvent({ event: 'error' });
            await this.runArmHook(hookConfig, cwd, eventData, undefined, this.errorHooks, fn, 'error');
          };
          this.onError(fn);
          break;
        }
        case 'sessionStart': {
          const fn: SessionHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('sessionStart', matcher, ctx as unknown as Record<string, unknown>)) return;
            // `model` falls back to the session's configured model (Claude Code
            // includes it in the SessionStart payload); `agent_type` is supplied
            // by the engine when a non-default agent is active.
            const eventData = serializeHookEvent({ event: 'sessionStart', sessionId: ctx.sessionId, source: ctx.source, model: ctx.model ?? this._providerConfig?.model, agentType: ctx.agentType });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.sessionStartHooks, fn, 'sessionStart');
            const watchPaths = this.registerWatchPaths('sessionStart', result);
            try {
              const parsed = JSON.parse(result);
              // Claude Code's SessionStart hook result is an ARRAY of injectable
              // messages; accept `initialUserMessages: string[]` as well as the
              // single `initialUserMessage` / `systemMessage` forms.
              if (Array.isArray(parsed.initialUserMessages)) {
                const msgs = parsed.initialUserMessages.filter((m: unknown): m is string => typeof m === 'string' && m.length > 0);
                if (msgs.length > 0) return { initialUserMessages: msgs, watchPaths };
              }
              if (parsed.initialUserMessage) return { initialUserMessage: parsed.initialUserMessage, watchPaths };
              if (parsed.systemMessage) return { initialUserMessage: parsed.systemMessage, watchPaths };
            } catch {}
            if (result.startsWith('SYSTEM:')) return { initialUserMessage: result.slice(7).trim(), watchPaths };
            if (watchPaths.length > 0) return { watchPaths };
            return undefined;
          };
          this.onSessionStart(fn);
          break;
        }
        case 'sessionEnd': {
          const fn: SessionHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('sessionEnd', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'sessionEnd', sessionId: ctx.sessionId, reason: ctx.reason });
            // SessionEnd runs during shutdown: kill a slow command hook at the
            // same tight budget that bounds the overall executeSessionEnd run,
            // so a lingering child process cannot outlive teardown.
            await this.runArmHook(hookConfig, cwd, eventData, AbortSignal.timeout(getSessionEndHookTimeoutMs()), this.sessionEndHooks, fn, 'sessionEnd');
          };
          this.onSessionEnd(fn);
          break;
        }
        case 'subagentStart': {
          const fn: SubagentStartHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('subagentStart', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'subagentStart', prompt: ctx.prompt });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.subagentStartHooks, fn, 'subagentStart');
            const contexts = extractAdditionalContexts(result);
            if (contexts.length > 0) return { additionalContexts: contexts };
            return undefined;
          };
          this.onSubagentStart(fn);
          break;
        }
        case 'subagentStop': {
          const fn: SubagentHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('subagentStop', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'subagentStop', subagentSessionId: ctx.subagentSessionId });
            await this.runArmHook(hookConfig, cwd, eventData, undefined, this.subagentStopHooks, fn, 'subagentStop');
          };
          this.onSubagentStop(fn);
          break;
        }
        case 'preCompact': {
          const fn: PreCompactHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('preCompact', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'preCompact', messageCount: ctx.messageCount, trigger: ctx.trigger, customInstructions: ctx.customInstructions ?? undefined });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.preCompactHooks, fn, 'preCompact');
            const parsed = tryParseHookJson(result);
            if (parsed) {
              const out: { newCustomInstructions?: string; userDisplayMessage?: string } = {};
              if (typeof parsed.newCustomInstructions === 'string') out.newCustomInstructions = parsed.newCustomInstructions;
              if (typeof parsed.userDisplayMessage === 'string') out.userDisplayMessage = parsed.userDisplayMessage;
              if (out.newCustomInstructions || out.userDisplayMessage) return out;
            }
            return undefined;
          };
          this.onPreCompact(fn);
          break;
        }
        case 'postCompact': {
          const fn: CompactHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('postCompact', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'postCompact', messageCount: ctx.messageCount });
            await this.runArmHook(hookConfig, cwd, eventData, undefined, this.postCompactHooks, fn, 'postCompact');
          };
          this.onPostCompact(fn);
          break;
        }
        case 'permissionRequest': {
          const fn: PermissionHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('permissionRequest', matcher, ctx as unknown as Record<string, unknown>)) return;
            // Surface the active posture + already-suggested rule updates to the
            // hook (Claude's permissionMode + suggestions params), and thread the
            // turn abort signal so a slow command hook is cancelled on interrupt.
            const eventData = serializeHookEvent({ event: 'permissionRequest', toolName: ctx.toolName, toolInput: ctx.toolInput, permissionMode: ctx.permissionMode, suggestions: ctx.suggestions });
            const result = await this.runArmHook(hookConfig, cwd, eventData, ctx.abortSignal, this.permissionRequestHooks, fn, 'permissionRequest');
            if (result.startsWith('DENY:')) return { action: 'deny' as const, reason: result.slice(5).trim() };
            if (result === 'ALLOW' || result.startsWith('ALLOW:')) return { action: 'allow' as const };
            // A command hook emitting `{permissionDecision:'allow', updatedInput}`
            // is flattened by runHookByType into the generic `MODIFY:<json>`
            // sentinel; recover it as an ALLOW that rewrites the tool input
            // (Claude's decision.updatedInput on allow). An additionalContext-only
            // MODIFY carries no rewrite, so allow without touching the input.
            if (result.startsWith('MODIFY:')) {
              const out: PermissionHookResult = { action: 'allow' as const };
              try {
                const payload = JSON.parse(result.slice(7));
                if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
                  const keys = Object.keys(payload);
                  if (!(keys.length === 1 && keys[0] === 'additionalContext')) out.updatedInput = payload as Record<string, unknown>;
                }
              } catch { /* non-JSON MODIFY payload → bare allow */ }
              return out;
            }
            try {
              const parsed: HookDecisionOutput = JSON.parse(result);
              if (parsed.permissionDecision === 'allow' || parsed.decision === 'approve') {
                // An ALLOW may rewrite the tool input and/or carry durable
                // permission-rule mutations the engine persists (Claude's
                // decision.updatedInput / decision.updatedPermissions).
                const out: PermissionHookResult = { action: 'allow' as const };
                if (parsed.updatedInput && typeof parsed.updatedInput === 'object' && !Array.isArray(parsed.updatedInput)) out.updatedInput = parsed.updatedInput;
                if (Array.isArray(parsed.updatedPermissions) && parsed.updatedPermissions.length > 0) out.updatedPermissions = parsed.updatedPermissions as PermissionUpdate[];
                return out;
              }
              if (parsed.permissionDecision === 'deny' || parsed.decision === 'deny' || parsed.decision === 'block') {
                // A DENY may set `interrupt` to abort the whole turn rather than
                // just skipping this one tool (Claude's decision.interrupt).
                const out: PermissionHookResult = { action: 'deny' as const, reason: parsed.reason ?? 'Hook denied' };
                if (parsed.interrupt === true) out.interrupt = true;
                return out;
              }
              // Strict rejection (Claude Code parseHookOutput): an unrecognized
              // decision/permissionDecision value is a validation error, not a
              // silent no-op. Fail CLOSED so a typo can't let a tool through.
              const invalid = unknownHookDecisionError(parsed as Record<string, unknown>);
              if (invalid) { coreLog('hooks', invalid); return { action: 'deny' as const, reason: invalid }; }
            } catch {}
            return undefined;
          };
          this.onPermissionRequest(fn);
          break;
        }
        case 'permissionDenied': {
          const fn: PermissionHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('permissionDenied', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'permissionDenied', toolName: ctx.toolName });
            await this.runArmHook(hookConfig, cwd, eventData, undefined, this.permissionDeniedHooks, fn, 'permissionDenied');
          };
          this.onPermissionDenied(fn);
          break;
        }
        case 'userPromptSubmit': {
          const fn: UserPromptHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'userPromptSubmit', turnCount: ctx.turnCount, prompt: ctx.prompt });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.userPromptSubmitHooks, fn, 'userPromptSubmit');
            // A UserPromptSubmit hook can BLOCK the prompt (DENY / exit-2) or
            // request a stop (continue:false) — feed the reason back (Claude parity).
            const blockingError = extractHookBlockingError(result);
            if (blockingError) return { blockingError };
            const parsed = tryParseHookJson(result);
            if (parsed) {
              const out: UserPromptSubmitHookResult = {};
              if (typeof parsed.additionalContext === 'string') out.additionalContext = parsed.additionalContext;
              if (parsed.continue === false) { out.preventContinuation = true; if (typeof parsed.stopReason === 'string') out.stopReason = parsed.stopReason; }
              if (out.additionalContext || out.preventContinuation) return out;
            }
            return undefined;
          };
          this.onUserPromptSubmit(fn);
          break;
        }
        case 'taskCreated': {
          const fn: TaskEventHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'taskCreated', taskId: ctx.taskId, subject: ctx.subject });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.taskCreatedHooks, fn, 'taskCreated');
            // A blocking taskCreated hook VETOES creation (the task tool rolls back).
            const blockingError = extractHookBlockingError(result);
            if (blockingError) return { blockingError };
            return undefined;
          };
          this.onTaskCreated(fn);
          break;
        }
        case 'taskCompleted': {
          const fn: TaskCompletedHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'taskCompleted', taskId: ctx.taskId, subject: ctx.subject, status: ctx.status });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.taskCompletedHooks, fn, 'taskCompleted');
            // A blocking TaskCompleted hook vetoes the status→completed transition.
            const blockingError = extractHookBlockingError(result);
            if (blockingError) return { blockingError };
            return undefined;
          };
          this.onTaskCompleted(fn);
          break;
        }
        case 'postToolUseFailure': {
          const fn: PostToolUseHookFn = async (ctx) => {
            if (hookConfig.if && !matchesHookIf(hookConfig.if, ctx.toolName, ctx.toolInput)) return;
            if (matcher && !matchesPattern(matcher, ctx.toolName)) return;
            const eventData = serializeHookEvent({ event: 'postToolUseFailure', toolName: ctx.toolName, isError: ctx.isError, duration_ms: ctx.durationMs });
            await this.runArmHook(hookConfig, cwd, eventData, ctx.abortSignal, this.postToolUseFailureHooks, fn, 'postToolUseFailure');
          };
          this.onPostToolUseFailure(fn);
          break;
        }
        case 'teammateIdle': {
          const fn: TeammateIdleHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('teammateIdle', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'teammateIdle', teammateName: ctx.teammateName, teamName: ctx.teamName });
            await this.runArmHook(hookConfig, cwd, eventData, undefined, this.teammateIdleHooks, fn, 'teammateIdle');
          };
          this.onTeammateIdle(fn);
          break;
        }
        case 'notification': {
          const fn: NotificationHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('notification', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'notification', message: ctx.message, title: ctx.title, notificationType: ctx.notificationType });
            await this.runArmHook(hookConfig, cwd, eventData, undefined, this.notificationHooks, fn, 'notification');
          };
          this.onNotification(fn);
          break;
        }
        case 'setup': {
          const fn: SessionHookFn = async (ctx) => {
            // Scope on the Setup `trigger` (init | maintenance) when a matcher is set.
            if (!matcherAppliesToCtx('setup', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'setup', sessionId: ctx.sessionId, cwd: ctx.cwd, trigger: ctx.trigger });
            await this.runArmHook(hookConfig, cwd, eventData, undefined, this.setupHooks, fn, 'setup');
          };
          this.onSetup(fn);
          break;
        }
        case 'configChange': {
          const fn: ConfigChangeHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('configChange', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'configChange', source: ctx.source, filePath: ctx.filePath });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.configChangeHooks, fn, 'configChange');
            const blockingError = extractHookBlockingError(result);
            if (blockingError) return { blockingError };
            return undefined;
          };
          this.onConfigChange(fn);
          break;
        }
        case 'instructionsLoaded': {
          const fn: InstructionsHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('instructionsLoaded', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'instructionsLoaded', filePath: ctx.filePath, loadReason: ctx.loadReason, memoryType: ctx.memoryType });
            await this.runArmHook(hookConfig, cwd, eventData, undefined, this.instructionsLoadedHooks, fn, 'instructionsLoaded');
          };
          this.onInstructionsLoaded(fn);
          break;
        }
        case 'worktreeCreate': {
          const fn: WorktreeCreateHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'worktreeCreate', name: ctx.name, worktreePath: ctx.worktreePath });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.worktreeCreateHooks, fn, 'worktreeCreate');
            // A command/http WorktreeCreate hook acts as a provider: the path it
            // emits (stdout or hookSpecificOutput.worktreePath) overrides the
            // default worktree location (Claude Code parity).
            const worktreePath = extractWorktreePath(result);
            if (worktreePath) return { worktreePath };
            return undefined;
          };
          this.onWorktreeCreate(fn);
          break;
        }
        case 'worktreeRemove': {
          const fn: WorktreeHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'worktreeRemove', name: ctx.name, worktreePath: ctx.worktreePath });
            await this.runArmHook(hookConfig, cwd, eventData, undefined, this.worktreeRemoveHooks, fn, 'worktreeRemove');
          };
          this.onWorktreeRemove(fn);
          break;
        }
        case 'cwdChanged': {
          const fn: CwdChangedHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'cwdChanged', oldCwd: ctx.oldCwd, newCwd: ctx.newCwd });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.cwdChangedHooks, fn, 'cwdChanged');
            const watchPaths = this.registerWatchPaths('cwdChanged', result);
            if (watchPaths.length > 0) return { watchPaths };
            return undefined;
          };
          this.onCwdChanged(fn);
          break;
        }
        case 'fileChanged': {
          const fn: FileChangeHookFn = async (ctx) => {
            if (hookConfig.if && !matchesPattern(hookConfig.if, ctx.filePath)) return;
            if (matcher && !matchesPattern(matcher, ctx.filePath)) return;
            const eventData = serializeHookEvent({ event: 'fileChanged', filePath: ctx.filePath, fileEvent: ctx.event });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.fileChangedHooks, fn, 'fileChanged');
            const watchPaths = this.registerWatchPaths('fileChanged', result);
            if (watchPaths.length > 0) return { watchPaths };
            return undefined;
          };
          this.onFileChanged(fn);
          break;
        }
        case 'elicitation': {
          const fn: ElicitationHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('elicitation', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'elicitation', mcpServerName: ctx.mcpServerName, message: ctx.message });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.elicitationHooks, fn, 'elicitation');
            return parseElicitationResult(result);
          };
          this.onElicitation(fn);
          break;
        }
        case 'elicitationResult': {
          const fn: ElicitationHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('elicitationResult', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'elicitationResult', mcpServerName: ctx.mcpServerName, message: ctx.message });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.elicitationResultHooks, fn, 'elicitationResult');
            return parseElicitationResult(result);
          };
          this.onElicitationResult(fn);
          break;
        }
        case 'stopFailure': {
          const fn: StopFailureHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('stopFailure', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'stopFailure', error: ctx.error, errorMessage: ctx.errorMessage, hookIndex: ctx.hookIndex });
            await this.runArmHook(hookConfig, cwd, eventData, undefined, this.stopFailureHooks, fn, 'stopFailure');
          };
          this.onStopFailure(fn);
          break;
        }
        default:
          coreLog('hooks', `Unknown hook event: ${entry.event}`);
      }
  }

  // ---- Existing registration methods (backward-compatible) ----

  onPostSampling(fn: HookFn): void {
    this.postSamplingHooks.push(fn);
  }

  onStop(fn: StopHookFn): void {
    this.stopHooks.push(fn);
  }

  onError(fn: HookFn): void {
    this.errorHooks.push(fn);
  }

  // ---- New registration methods ----

  onPreToolUse(fn: PreToolUseHookFn, tier?: string): void {
    this.preToolUseHooks.push(fn);
    this._hookTiers.set(fn, tier || 'user');
  }

  onPostToolUse(fn: PostToolUseHookFn): void {
    this.postToolUseHooks.push(fn);
  }

  onPreSampling(fn: PreSamplingHookFn, tier?: string): void {
    this.preSamplingHooks.push(fn);
    this._hookTiers.set(fn, tier || 'user');
  }

  onSessionStart(fn: SessionHookFn): void { this.sessionStartHooks.push(fn); }
  onSessionEnd(fn: SessionHookFn): void { this.sessionEndHooks.push(fn); }
  onSubagentStart(fn: SubagentStartHookFn): void { this.subagentStartHooks.push(fn); }
  onSubagentStop(fn: SubagentHookFn): void { this.subagentStopHooks.push(fn); }
  onPreCompact(fn: PreCompactHookFn): void { this.preCompactHooks.push(fn); }
  onPostCompact(fn: CompactHookFn): void { this.postCompactHooks.push(fn); }
  onPermissionRequest(fn: PermissionHookFn): void { this.permissionRequestHooks.push(fn); }
  onPermissionDenied(fn: PermissionHookFn): void { this.permissionDeniedHooks.push(fn); }
  onUserPromptSubmit(fn: UserPromptHookFn): void { this.userPromptSubmitHooks.push(fn); }
  onTaskCreated(fn: TaskEventHookFn): void { this.taskCreatedHooks.push(fn); }
  onTaskCompleted(fn: TaskCompletedHookFn): void { this.taskCompletedHooks.push(fn); }
  onTeammateIdle(fn: TeammateIdleHookFn): void { this.teammateIdleHooks.push(fn); }
  onPostToolUseFailure(fn: PostToolUseHookFn): void { this.postToolUseFailureHooks.push(fn); }
  onNotification(fn: NotificationHookFn): void { this.notificationHooks.push(fn); }
  onSetup(fn: SessionHookFn): void { this.setupHooks.push(fn); }
  onConfigChange(fn: ConfigChangeHookFn): void { this.configChangeHooks.push(fn); }
  onInstructionsLoaded(fn: InstructionsHookFn): void { this.instructionsLoadedHooks.push(fn); }
  onWorktreeCreate(fn: WorktreeCreateHookFn): void { this.worktreeCreateHooks.push(fn); }
  onWorktreeRemove(fn: WorktreeHookFn): void { this.worktreeRemoveHooks.push(fn); }
  onCwdChanged(fn: CwdChangedHookFn): void { this.cwdChangedHooks.push(fn); }
  onFileChanged(fn: FileChangeHookFn): void { this.fileChangedHooks.push(fn); }
  onElicitation(fn: ElicitationHookFn): void { this.elicitationHooks.push(fn); }
  onElicitationResult(fn: ElicitationHookFn): void { this.elicitationResultHooks.push(fn); }
  onStopFailure(fn: StopFailureHookFn): void { this.stopFailureHooks.push(fn); }

  // ---- Concurrency-gated execution helper ----

  private async withConcurrencyGate<T>(event: string, fn: () => Promise<T>): Promise<T> {
    await this._concurrency.acquire(event);
    try {
      return await fn();
    } finally {
      this._concurrency.release(event);
    }
  }

  // ---- Existing execution methods (backward-compatible, now with error logging) ----

  async executePostSampling(ctx: HookContext): Promise<void> {
    return this.withConcurrencyGate('postSampling', async () => {
      for (const hook of this.postSamplingHooks) {
        try {
          await hook(ctx);
        } catch (err) {
          coreLog('hooks', 'Hook error', { error: String(err) });
        }
      }
    });
  }

  /**
   * Execute Stop hooks. A Stop hook can BLOCK turn-end (blockingError, fed back
   * into the loop as a user message) or PREVENT CONTINUATION (continue the turn
   * instead of ending). Returns the aggregated {blockingErrors, preventContinuation,
   * stopReason} so the engine can re-enter the loop (Claude Code parity).
   * Re-entrant calls (while Stop hooks are already running) short-circuit to a
   * neutral result via the stopHookActive guard — a Stop hook cannot recursively
   * fire Stop hooks.
   */
  async executeStop(ctx: HookContext): Promise<StopHookResult> {
    if (this._stopHookActive) return { blockingErrors: [], preventContinuation: false };
    return this.withConcurrencyGate('stop', async (): Promise<StopHookResult> => {
      this._stopHookActive = true;
      const blockingErrors: string[] = [];
      // Per-hook timings + errors for the end-of-chain summary (Claude Code's
      // hookInfos). Wall-clock total is measured around the whole chain because
      // Claude prefers it over the summed per-hook time.
      const hookInfos: StopHookInfo[] = [];
      const hookErrors: string[] = [];
      const chainStart = Date.now();
      let preventContinuation = false;
      let stopReason: string | undefined;
      try {
        for (let i = 0; i < this.stopHooks.length; i++) {
          // Abort-signal awareness (Claude Code's stopHooks abort check): a user
          // interrupt (Ctrl-C / turn abort) mid-chain stops the Stop-hook chain
          // immediately — no further hooks run — and the in-flight per-hook child
          // is torn down via the abortSignal threaded into runArmHook. The partial
          // result accumulated so far is returned (not discarded), matching the
          // "aborting yields an interruption and stops the chain" contract.
          if (ctx.abortSignal?.aborted) {
            coreLog('hooks', 'Stop hooks interrupted by abort signal');
            break;
          }
          const hookStart = Date.now();
          try {
            const r = await this.stopHooks[i](ctx);
            hookInfos.push({ durationMs: Date.now() - hookStart });
            if (r) {
              if (r.blockingError) blockingErrors.push(r.blockingError);
              if (r.preventContinuation) { preventContinuation = true; if (r.stopReason) stopReason = r.stopReason; }
            }
          } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            hookInfos.push({ durationMs: Date.now() - hookStart, error: message });
            hookErrors.push(message);
            coreLog('hooks', 'Hook error', { error: message });
            // Classify the raw failure into the matchable StopFailure taxonomy so a
            // hook scoped on e.g. `rate_limit` actually fires; keep the raw detail.
            await this.executeStopFailure({ error: stopFailureErrorClass(message), errorMessage: message, hookIndex: i });
          }
        }
      } finally {
        this._stopHookActive = false;
      }
      // Build + retain + emit the per-hook summary (operator-facing; NEVER fed to
      // the model). Only when hooks actually ran — a no-op turn gets no summary.
      if (hookInfos.length > 0) {
        const summary: StopHookSummary = {
          count: hookInfos.length,
          hookInfos,
          totalDurationMs: Date.now() - chainStart,
          hookErrors,
          preventContinuation,
          stopReason,
        };
        this._lastStopHookSummary = summary;
        this.emit('stopHookSummary', summary);
      }
      return { blockingErrors, preventContinuation, stopReason };
    });
  }

  async executeError(ctx: HookContext): Promise<void> {
    return this.withConcurrencyGate('error', async () => {
      for (const hook of this.errorHooks) {
        try {
          await hook(ctx);
        } catch (err) {
          coreLog('hooks', 'Hook error', { error: String(err) });
        }
      }
    });
  }

  // ---- New execution methods ----

  /**
   * Execute pre-tool-use hooks. First deny wins; modify decisions are accumulated
   * (later modify hooks can override earlier ones).
   */
  async executePreToolUse(ctx: PreToolUseContext): Promise<HookDecision> {
    return this.withConcurrencyGate('preToolUse', () =>
      this.executeDecisionHooks(this.preToolUseHooks, ctx));
  }

  /**
   * Execute post-tool-use hooks. Returns the last additionalContext/updatedToolOutput if any hook provides one.
   */
  async executePostToolUse(ctx: PostToolUseContext): Promise<PostToolUseHookResult | void> {
    return this.withConcurrencyGate('postToolUse', async (): Promise<PostToolUseHookResult | void> => {
      // Aggregate ALL contributions across every matching hook (Claude Code's
      // AggregatedHookResult collects additionalContexts[]/blockingErrors[]),
      // not last-wins: a later hook must not silently drop an earlier hook's
      // context or block reason. updatedToolOutput stays last-wins (a single
      // canonical rewrite of the tool output).
      const additionalContexts: string[] = [];
      const blockingErrors: string[] = [];
      let updatedToolOutput: string | undefined;
      let preventContinuation = false;
      let stopReason: string | undefined;
      for (const hook of this.postToolUseHooks) {
        try {
          const result = await hook(ctx);
          if (result?.additionalContext) additionalContexts.push(result.additionalContext);
          if (result?.updatedToolOutput) updatedToolOutput = result.updatedToolOutput;
          if (result?.blockingError) blockingErrors.push(result.blockingError);
          if (result?.preventContinuation) { preventContinuation = true; if (result.stopReason) stopReason = result.stopReason; }
        } catch (err) {
          coreLog('hooks', 'Hook error', { error: String(err) });
        }
      }
      const additionalContext = additionalContexts.length ? additionalContexts.join('\n\n') : undefined;
      const blockingError = blockingErrors.length ? blockingErrors.join('\n') : undefined;
      if (additionalContext || updatedToolOutput || blockingError || preventContinuation) {
        return { additionalContext, updatedToolOutput, blockingError, preventContinuation: preventContinuation || undefined, stopReason };
      }
    });
  }

  /**
   * Execute pre-sampling hooks. First deny wins; modify decisions are accumulated
   * (later modify hooks can override earlier ones).
   */
  async executePreSampling(ctx: PreSamplingContext): Promise<HookDecision> {
    return this.withConcurrencyGate('preSampling', () =>
      this.executeDecisionHooks(this.preSamplingHooks, ctx));
  }

  async executeSessionStart(ctx: SessionContext): Promise<{ initialUserMessage?: string; watchPaths?: string[] } | void> {
    if (ctx.source !== undefined && !isSessionStartSource(ctx.source)) {
      coreLog('hooks', `SessionStart: unrecognized source '${ctx.source}' (expected ${SESSION_START_SOURCES.join('|')})`);
    }
    return this.withConcurrencyGate('sessionStart', async (): Promise<{ initialUserMessage?: string; watchPaths?: string[] } | void> => {
      // Aggregate ALL messages from ALL SessionStart hooks (Claude Code injects
      // an array of hook messages, not just the last) — a hook may contribute one
      // (`initialUserMessage`) or many (`initialUserMessages[]`); all are joined.
      const messages: string[] = [];
      const watchPaths: string[] = [];
      for (const hook of this.sessionStartHooks) {
        try {
          const result = await hook(ctx);
          if (result?.initialUserMessage) messages.push(result.initialUserMessage);
          if (result?.initialUserMessages?.length) messages.push(...result.initialUserMessages);
          if (result?.watchPaths?.length) watchPaths.push(...result.watchPaths);
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      const initialUserMessage = messages.length ? messages.join('\n\n') : undefined;
      if (initialUserMessage || watchPaths.length > 0) return { initialUserMessage, watchPaths: watchPaths.length > 0 ? watchPaths : undefined };
    });
  }

  async executeSessionEnd(ctx: SessionContext): Promise<void> {
    if (ctx.reason !== undefined && !isSessionEndReason(ctx.reason)) {
      coreLog('hooks', `SessionEnd: unrecognized reason '${ctx.reason}' (expected ${SESSION_END_REASONS.join('|')})`);
    }
    return this.withConcurrencyGate('sessionEnd', async () => {
      // SessionEnd hooks run during shutdown/clear; bound the WHOLE run with a
      // tight budget (getSessionEndHookTimeoutMs) so a slow or hanging teardown
      // hook — command OR in-process callback — cannot block process exit. On
      // budget exhaustion we LOG and return (teardown proceeds) rather than
      // reject, matching Claude Code's "don't block shutdown" contract. Command
      // hooks additionally receive an AbortSignal at the same budget (see the
      // sessionEnd registration) so their child processes are actually killed.
      const budgetMs = getSessionEndHookTimeoutMs();
      const run = (async () => {
        for (const hook of this.sessionEndHooks) {
          try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
        }
      })();
      try {
        await runWithTimeout(run, budgetMs, 'sessionEnd hooks');
      } catch (err) {
        coreLog('hooks', 'SessionEnd hooks exceeded budget', { budgetMs, error: String(err) });
      }
    });
  }

  /**
   * Execute SubagentStart hooks and aggregate the additionalContexts they
   * contribute, which the Task tool prepends to the subagent's initial messages
   * (Claude Code parity).
   */
  async executeSubagentStart(ctx: SubagentContext): Promise<{ additionalContexts: string[] } | void> {
    return this.withConcurrencyGate('subagentStart', async (): Promise<{ additionalContexts: string[] } | void> => {
      const additionalContexts: string[] = [];
      for (const hook of this.subagentStartHooks) {
        try {
          const r = await hook(ctx);
          if (r?.additionalContexts?.length) additionalContexts.push(...r.additionalContexts);
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      if (additionalContexts.length > 0) return { additionalContexts };
    });
  }

  async executeSubagentStop(ctx: SubagentContext): Promise<void> {
    return this.withConcurrencyGate('subagentStop', async () => {
      for (const hook of this.subagentStopHooks) {
        // Abort-signal awareness: a user interrupt stops the SubagentStop chain
        // mid-flight (Claude Code parity) — no further hooks run.
        if (ctx.abortSignal?.aborted) {
          coreLog('hooks', 'SubagentStop hooks interrupted by abort signal');
          break;
        }
        try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  /**
   * Execute PreCompact hooks. A hook can inject/override the compaction
   * instructions (newCustomInstructions, merged into the user's via
   * mergeHookInstructions) and surface a user-facing display message
   * (Claude Code parity). Returns the merged instructions + last display message.
   */
  async executePreCompact(ctx: CompactContext): Promise<{ newCustomInstructions?: string; userDisplayMessage?: string } | void> {
    return this.withConcurrencyGate('preCompact', async (): Promise<{ newCustomInstructions?: string; userDisplayMessage?: string } | void> => {
      coreLog('hooks', `[compact] Pre-compact triggered: ${ctx.messageCount} messages, ~${ctx.tokenEstimate} tokens`);
      let newCustomInstructions: string | undefined;
      let userDisplayMessage: string | undefined;
      for (const hook of this.preCompactHooks) {
        try {
          const r = await hook(ctx);
          if (r?.newCustomInstructions) newCustomInstructions = mergeHookInstructions(newCustomInstructions, r.newCustomInstructions);
          if (r?.userDisplayMessage) userDisplayMessage = r.userDisplayMessage;
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      if (newCustomInstructions || userDisplayMessage) return { newCustomInstructions, userDisplayMessage };
    });
  }

  async executePostCompact(ctx: CompactContext): Promise<void> {
    return this.withConcurrencyGate('postCompact', async () => {
      for (const hook of this.postCompactHooks) {
        try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  async executePermissionRequest(ctx: PermissionContext): Promise<PermissionHookResult | void> {
    return this.withConcurrencyGate('permissionRequest', async (): Promise<PermissionHookResult | void> => {
      for (const hook of this.permissionRequestHooks) {
        // Stop consulting hooks once the turn is aborted (user interrupt): a
        // cancelled permission request yields no decision and falls through to
        // policy (Claude's abort-aware runHooks loop).
        if (ctx.abortSignal?.aborted) break;
        try {
          const result = await raceAbort(hook(ctx), ctx.abortSignal);
          if (result && result.action) return result;
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  async executePermissionDenied(ctx: PermissionContext): Promise<{ retry?: boolean } | void> {
    return this.withConcurrencyGate('permissionDenied', async (): Promise<{ retry?: boolean } | void> => {
      for (const hook of this.permissionDeniedHooks) {
        try {
          const result = await hook(ctx);
          if (result && (result as any).action === 'allow') {
            return { retry: true };
          }
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      if (ctx.retry) return { retry: true };
    });
  }

  /**
   * Execute UserPromptSubmit hooks. A hook may inject additionalContext, or BLOCK
   * the prompt (blockingError, surfaced to the user) / prevent continuation —
   * Claude Code parity. Returns the aggregated result.
   */
  async executeUserPromptSubmit(ctx: UserPromptContext): Promise<UserPromptSubmitHookResult | void> {
    return this.withConcurrencyGate('userPromptSubmit', async (): Promise<UserPromptSubmitHookResult | void> => {
      // Aggregate ALL contributions (Claude Code AggregatedHookResult), not
      // last-wins: every UserPromptSubmit hook's context + block reason is kept.
      const additionalContexts: string[] = [];
      const blockingErrors: string[] = [];
      let preventContinuation = false;
      let stopReason: string | undefined;
      for (const hook of this.userPromptSubmitHooks) {
        try {
          const result = await hook(ctx);
          if (result?.additionalContext) additionalContexts.push(result.additionalContext);
          if (result?.blockingError) blockingErrors.push(result.blockingError);
          if (result?.preventContinuation) { preventContinuation = true; if (result.stopReason) stopReason = result.stopReason; }
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      const additionalContext = additionalContexts.length ? additionalContexts.join('\n\n') : undefined;
      const blockingError = blockingErrors.length ? blockingErrors.join('\n') : undefined;
      if (additionalContext || blockingError || preventContinuation) {
        // Bound hook-contributed context so a UserPromptSubmit hook that emits a
        // huge blob cannot blow out the context window (Claude Code's
        // MAX_HOOK_OUTPUT_LENGTH truncation with a marker).
        return {
          additionalContext: additionalContext !== undefined ? truncateHookOutput(additionalContext) : undefined,
          blockingError,
          preventContinuation: preventContinuation || undefined,
          stopReason,
        };
      }
    });
  }

  /**
   * Execute TaskCreated hooks. A blocking hook can VETO a just-created task; the
   * aggregated blockingErrors are returned so the task tool can roll the task back
   * and surface the reason (Claude Code parity). If the forwarded AbortSignal is
   * already aborted the loop short-circuits (cancellation boundary).
   */
  async executeTaskCreated(ctx: TaskEventContext): Promise<{ blockingErrors: string[] } | void> {
    return this.withConcurrencyGate('taskCreated', async (): Promise<{ blockingErrors: string[] } | void> => {
      const blockingErrors: string[] = [];
      for (const hook of this.taskCreatedHooks) {
        if (ctx.signal?.aborted) break;
        try {
          const r = await hook(ctx);
          if (r?.blockingError) blockingErrors.push(r.blockingError);
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      if (blockingErrors.length > 0) return { blockingErrors };
    });
  }

  /**
   * Execute TaskCompleted hooks. A blocking hook can veto a status→completed
   * transition; the aggregated blockingErrors are returned so the task tool can
   * refuse to mutate the status and surface the reason (Claude Code parity).
   */
  async executeTaskCompleted(ctx: TaskEventContext): Promise<{ blockingErrors: string[] } | void> {
    return this.withConcurrencyGate('taskCompleted', async (): Promise<{ blockingErrors: string[] } | void> => {
      const blockingErrors: string[] = [];
      for (const hook of this.taskCompletedHooks) {
        // Cancellation boundary (Claude Code parity: executeTaskCompletedHooks is
        // driven by context.abortController.signal). An already-aborted turn runs
        // no further completion hooks, and a hook that blocks (e.g. spawns a
        // subprocess) is actively cancelled via raceAbort so it cannot leak past a
        // Ctrl-C / engine abort — the forwarded signal is also on ctx for the hook
        // itself to observe.
        if (ctx.signal?.aborted) break;
        try {
          const r = await raceAbort(hook(ctx), ctx.signal);
          if (r?.blockingError) blockingErrors.push(r.blockingError);
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      if (blockingErrors.length > 0) return { blockingErrors };
    });
  }

  /**
   * Fire TeammateIdle hooks when a teammate agent finishes its run and goes idle.
   * Observational (fire-and-forget); mirrors Claude Code's TeammateIdle event with
   * the teammate/team identity payload (teammate_name / team_name).
   */
  async executeTeammateIdle(ctx: TeammateContext): Promise<void> {
    return this.withConcurrencyGate('teammateIdle', async () => {
      for (const hook of this.teammateIdleHooks) {
        try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  async executePostToolUseFailure(ctx: PostToolUseContext): Promise<void> {
    return this.withConcurrencyGate('postToolUseFailure', async () => {
      for (const hook of this.postToolUseFailureHooks) {
        try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  async executeNotification(ctx: NotificationContext): Promise<void> {
    return this.withConcurrencyGate('notification', async () => {
      for (const hook of this.notificationHooks) {
        try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  async executeSetup(ctx: SessionContext): Promise<void> {
    if (ctx.trigger !== undefined && !isSetupTrigger(ctx.trigger)) {
      coreLog('hooks', `Setup: unrecognized trigger '${ctx.trigger}' (expected ${SETUP_TRIGGERS.join('|')})`);
    }
    return this.withConcurrencyGate('setup', async () => {
      for (const hook of this.setupHooks) {
        try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  /**
   * Execute ConfigChange hooks. A blocking hook can VETO the change before it is
   * applied; the aggregated blockingErrors are returned so the config loader can
   * refuse to commit and surface the reason (Claude Code's hasBlockingResult).
   */
  async executeConfigChange(ctx: ConfigChangeContext): Promise<{ blockingErrors: string[] } | void> {
    return this.withConcurrencyGate('configChange', async (): Promise<{ blockingErrors: string[] } | void> => {
      const blockingErrors: string[] = [];
      for (const hook of this.configChangeHooks) {
        try {
          const r = await hook(ctx);
          if (r?.blockingError) blockingErrors.push(r.blockingError);
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      if (blockingErrors.length > 0) return { blockingErrors };
    });
  }

  async executeInstructionsLoaded(ctx: InstructionsContext): Promise<void> {
    return this.withConcurrencyGate('instructionsLoaded', async () => {
      for (const hook of this.instructionsLoadedHooks) {
        try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  /**
   * Execute WorktreeCreate hooks. A provider hook may return a `worktreePath`
   * (from stdout or hookSpecificOutput.worktreePath); the FIRST non-empty path
   * wins and is returned so the caller uses it as the created worktree (Claude
   * Code's executeWorktreeCreateHook — first successful non-empty output). A plain
   * void return across all hooks is the observe-only case.
   */
  async executeWorktreeCreate(ctx: WorktreeContext): Promise<{ worktreePath?: string } | void> {
    return this.withConcurrencyGate('worktreeCreate', async (): Promise<{ worktreePath?: string } | void> => {
      for (const hook of this.worktreeCreateHooks) {
        try {
          const r = await hook(ctx);
          if (r?.worktreePath && r.worktreePath.trim().length > 0) return { worktreePath: r.worktreePath.trim() };
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  async executeWorktreeRemove(ctx: WorktreeContext): Promise<void> {
    return this.withConcurrencyGate('worktreeRemove', async () => {
      for (const hook of this.worktreeRemoveHooks) {
        try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  async executeCwdChanged(ctx: CwdChangedContext): Promise<void> {
    // No-op short-circuit (Claude Code `onCwdChangedForHooks`: `if (oldCwd ===
    // newCwd) return`). When the "change" lands on the same directory the cwd did
    // not actually move, so clearing the session-env cache and firing CwdChanged
    // hooks would be wasted work with observable side effects (a spurious env
    // re-read, a redundant hook run). Bail before any of it.
    if (ctx.oldCwd === ctx.newCwd) return;
    // The cwd changed: drop stale directory-scoped env fragments and invalidate
    // the persisted session-environment cache so the next bash command re-reads
    // it (Claude Code `clearCwdEnvFiles` / `invalidateSessionEnvCache` parity).
    await clearCwdEnvFiles(this._sessionId);
    invalidateSessionEnvCache();
    return this.withConcurrencyGate('cwdChanged', async () => {
      for (const hook of this.cwdChangedHooks) {
        try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  async executeFileChanged(ctx: FileChangeContext): Promise<void> {
    return this.withConcurrencyGate('fileChanged', async () => {
      for (const hook of this.fileChangedHooks) {
        try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  /**
   * Execute pre-response Elicitation hooks. The FIRST hook that returns an
   * action (programmatic response) or a blockingError short-circuits the user
   * prompt (Claude Code parity: a hook response resolves the elicitation
   * directly; a blockingError declines it).
   */
  async executeElicitation(ctx: ElicitationContext): Promise<ElicitationHookResult | void> {
    return this.withConcurrencyGate('elicitation', async (): Promise<ElicitationHookResult | void> => {
      for (const hook of this.elicitationHooks) {
        try {
          const r = await hook(ctx);
          if (r && (r.action || r.content || r.blockingError)) return r;
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  /**
   * Execute post-response ElicitationResult hooks. A hook can override the
   * action/content the user supplied, or block the reply (blockingError →
   * decline). The first hook that returns an override/block wins.
   */
  async executeElicitationResult(ctx: ElicitationContext): Promise<ElicitationHookResult | void> {
    return this.withConcurrencyGate('elicitationResult', async (): Promise<ElicitationHookResult | void> => {
      for (const hook of this.elicitationResultHooks) {
        try {
          const r = await hook(ctx);
          if (r && (r.action || r.content || r.blockingError)) return r;
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  async executeStopFailure(ctx: StopFailureContext): Promise<void> {
    return this.withConcurrencyGate('stopFailure', async () => {
      for (const hook of this.stopFailureHooks) {
        try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  getSummary(): Record<string, number> {
    return {
      preToolUse: this.preToolUseHooks.length,
      postToolUse: this.postToolUseHooks.length,
      preSampling: this.preSamplingHooks.length,
      postSampling: this.postSamplingHooks.length,
      stop: this.stopHooks.length,
      error: this.errorHooks.length,
      sessionStart: this.sessionStartHooks.length,
      sessionEnd: this.sessionEndHooks.length,
      subagentStart: this.subagentStartHooks.length,
      subagentStop: this.subagentStopHooks.length,
      preCompact: this.preCompactHooks.length,
      postCompact: this.postCompactHooks.length,
      permissionRequest: this.permissionRequestHooks.length,
      permissionDenied: this.permissionDeniedHooks.length,
      userPromptSubmit: this.userPromptSubmitHooks.length,
      taskCreated: this.taskCreatedHooks.length,
      taskCompleted: this.taskCompletedHooks.length,
      teammateIdle: this.teammateIdleHooks.length,
      postToolUseFailure: this.postToolUseFailureHooks.length,
      notification: this.notificationHooks.length,
      setup: this.setupHooks.length,
      configChange: this.configChangeHooks.length,
      instructionsLoaded: this.instructionsLoadedHooks.length,
      worktreeCreate: this.worktreeCreateHooks.length,
      worktreeRemove: this.worktreeRemoveHooks.length,
      cwdChanged: this.cwdChangedHooks.length,
      fileChanged: this.fileChangedHooks.length,
      elicitation: this.elicitationHooks.length,
      elicitationResult: this.elicitationResultHooks.length,
      stopFailure: this.stopFailureHooks.length,
      callbackHooks: Array.from(this._callbackHooks.values()).reduce((a, b) => a + b.length, 0),
    };
  }

  /**
   * Introspection: the ACTUAL file-loaded hooks (command/prompt/url, matcher,
   * source), not just counts (Claude Code's per-hook ViewHookMode details). Returns
   * a copy so callers cannot mutate the retained set.
   */
  describeHooks(): HookDescriptor[] {
    return this._descriptors.map(d => ({ ...d }));
  }

  /**
   * Full per-hook audit view (Claude Code's ViewHookMode): for every retained
   * hook, its typed content with the type-specific label (command→'Command',
   * http→'URL', prompt/agent→'Prompt'), a human-readable source description with
   * plugin attribution (`Plugin (name)`), and the progress display text
   * (statusMessage || content). This is the canonical consumer of
   * {@link hookContentLabel}, {@link hookSourceDisplayString} and
   * {@link getHookDisplayText}, so `/hooks` + `--introspect` can reveal exactly
   * what each configured hook runs, where it came from, and which plugin (if any)
   * installed it. Returns copies; callers cannot mutate the retained set.
   */
  getHookAuditView(): HookAuditRow[] {
    // Build from describeHooks() (the canonical retained-descriptor copy) rather
    // than re-reading _descriptors, so the two introspection surfaces share one
    // source of truth and the audit view can never drift from the raw view.
    return this.describeHooks().map(d => ({
      event: d.event,
      type: d.type,
      ...(d.matcher ? { matcher: d.matcher } : {}),
      source: d.source,
      sourceLabel: hookSourceDisplayString(d.source, d.pluginName),
      ...(d.pluginName ? { pluginName: d.pluginName } : {}),
      contentLabel: hookContentLabel(d.type),
      content: d.content,
      displayText: getHookDisplayText(d),
    }));
  }

  /**
   * Introspection view of active (count>0) hook events (Claude Code's
   * SelectEventMode): each event with its human-readable summary/description, the
   * matcher field + enumerated matcher values, whether it supports a matcher, and
   * its matcher groups (sorted by source priority). `opts` supplies the dynamic
   * matcher-value sets (tool names, agent types, MCP server names).
   */
  describeEvents(opts: HookEventMetadataOpts = {}): HookEventDescription[] {
    const summary = this.getSummary();
    // Group retained descriptors by event, then by matcher.
    const byEvent = new Map<string, HookDescriptor[]>();
    for (const d of this._descriptors) {
      const arr = byEvent.get(d.event) ?? [];
      arr.push(d);
      byEvent.set(d.event, arr);
    }
    const out: HookEventDescription[] = [];
    for (const [event, count] of Object.entries(summary)) {
      if (!count || event === 'callbackHooks') continue;
      const meta = getHookEventMetadata(event, opts);
      const descs = byEvent.get(event) ?? [];
      const matcherMap = new Map<string, HookDescriptor[]>();
      for (const d of descs) {
        const key = d.matcher ?? '';
        const arr = matcherMap.get(key) ?? [];
        arr.push(d);
        matcherMap.set(key, arr);
      }
      const sourcesByMatcher: Record<string, string[]> = {};
      for (const [m, arr] of matcherMap) {
        sourcesByMatcher[m] = Array.from(new Set(arr.map(a => a.source)));
      }
      const matchers: HookEventMatcherGroup[] = sortMatchersByPriority(
        Array.from(matcherMap.keys()),
        sourcesByMatcher,
      ).map(m => ({
        matcher: m,
        label: formatHookMatcherLabel(m),
        count: matcherMap.get(m)!.length,
        sources: sourcesByMatcher[m],
      }));
      out.push({
        event,
        count,
        summary: meta?.summary,
        description: meta?.description,
        matcherField: meta?.matcherField,
        matcherValues: meta?.matcherValues,
        supportsMatcher: eventSupportsMatcher(event),
        matchers,
      });
    }
    out.sort((a, b) => a.event.localeCompare(b.event));
    return out;
  }

  /**
   * Shared decision-hook executor. First deny wins; modify decisions are
   * accumulated (later hooks override earlier ones).
   *
   * Gap 2: Fail-closed — if a hook throws, we return deny immediately rather
   *   than silently allowing the action through.
   *
   * Gap 1: Tier-aware hold-over — when _denyRulesHold is true, a user-tier
   *   hook cannot loosen a verdict set by a higher-privilege tier.
   */
  private async executeDecisionHooks<T extends HookContext>(
    hooks: Array<(ctx: T) => Promise<HookDecision>>,
    ctx: T,
  ): Promise<HookDecision> {
    let accumulated: HookDecision = { action: 'allow' };
    // Track the strictest verdict from non-user tiers for hold-over logic
    let preUserVerdict: HookDecision | null = null;

    for (const hook of hooks) {
      try {
        const decision = await hook(ctx);

        // Gap 1: When denyRulesHold is active, track pre-user-tier verdict
        if (this._denyRulesHold) {
          const hookTier = this._hookTiers.get(hook) || 'user';
          if (hookTier !== 'user' && hookTier !== 'project' && hookTier !== 'config') {
            // This is a builtin/org hook — record its verdict if strictest so far
            if (!preUserVerdict || verdictRank(decision.action) > verdictRank(preUserVerdict.action)) {
              preUserVerdict = decision;
            }
          }
        }

        if (decision.action === 'deny') {
          return decision;
        }
        if (decision.action === 'modify') {
          accumulated = {
            action: 'modify',
            updatedInput: decision.updatedInput ?? (accumulated.action === 'modify' ? accumulated.updatedInput : undefined),
            updatedMessages: decision.updatedMessages ?? (accumulated.action === 'modify' ? accumulated.updatedMessages : undefined),
            additionalContext: decision.additionalContext ?? accumulated.additionalContext,
          };
        } else if (decision.action === 'ask') {
          // 'ask' forces an interactive permission prompt. It is stricter than
          // allow/modify (verdictRank 2) so it holds against a later allow and is
          // only overridden by a deny (handled above). Carry its reason +
          // updatedInput so the user is prompted to approve the hook's rewrite.
          // The carry values are read (with explicit types) BEFORE reassigning so
          // the new value does not reference `accumulated` circularly.
          const carryReason: string | undefined = accumulated.action === 'ask' ? accumulated.reason : undefined;
          // Both the prior `ask` and `modify` arms carry `updatedInput`; read it
          // structurally (allow/deny arms simply have none → undefined).
          const carryInput: Record<string, unknown> | undefined = (accumulated as { updatedInput?: Record<string, unknown> }).updatedInput;
          const carryCtx: string | undefined = accumulated.additionalContext;
          accumulated = {
            action: 'ask',
            reason: decision.reason ?? carryReason,
            updatedInput: decision.updatedInput ?? carryInput,
            additionalContext: decision.additionalContext ?? carryCtx,
          };
        } else if (decision.action === 'allow') {
          // An explicit "allow" can loosen a prior "modify"; the hold-over
          // check at the end guards against user-tier loosening. A prior "ask"
          // (forced prompt) is NOT loosened by a later allow — keep the prompt,
          // just fold in any additionalContext.
          if (accumulated.action === 'ask') {
            accumulated = { action: 'ask', reason: accumulated.reason, updatedInput: accumulated.updatedInput, additionalContext: decision.additionalContext ?? accumulated.additionalContext };
          } else {
            accumulated = { action: 'allow', additionalContext: decision.additionalContext ?? accumulated.additionalContext };
          }
        }
      } catch (err) {
        // Gap 2: Fail-closed — hook errors deny the action
        coreLog('hooks', 'Hook error (fail-closed)', { error: String(err) });
        return { action: 'deny' as const, reason: `Hook verification failed (fail-closed): ${err instanceof Error ? err.message : String(err)}` };
      }
    }

    // Gap 1: If denyRulesHold is active and a privileged tier had a stricter
    // verdict than the accumulated result, hold the privileged verdict.
    if (this._denyRulesHold && preUserVerdict && verdictRank(preUserVerdict.action) > verdictRank(accumulated.action)) {
      return { action: 'deny' as const, reason: 'Org-tier verdict held over user-tier loosening (denyRulesHold)' };
    }

    return accumulated;
  }

  private async runHookByType(
    config: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig,
    cwd: string,
    eventData: string,
    signal?: AbortSignal,
    // Out-param success signal (mirrors Claude Code's onHookSuccess): set to false
    // when the hook ERRORS/times out/blocks-via-error so a `once:true` hook is NOT
    // consumed by a failed attempt and stays registered to retry. A clean run
    // (exit 0 / no engine error / 2xx) leaves it true even if the hook returned a
    // DENY *decision* — that is a successful execution that happened to block.
    outcome?: { success: boolean },
    // Per-hook env-file hint: the real lifecycle event + this hook's index, so an
    // env-ingestible command hook receives its deterministic CLAUDE_ENV_FILE path
    // (see buildHookEnv). Absent for non-arm callers / non-ingestible events.
    envFileHint?: { event: string; index: number },
  ): Promise<string> {
    switch (config.type) {
      case 'command': {
        // Claude Code hook contract: config.timeout is in SECONDS. Route through
        // resolveHookTimeoutMs so it is clamped to [100ms, MAX_HOOK_TIMEOUT_MS].
        const timeout = resolveHookTimeoutMs(config.timeout, DEFAULT_HOOK_TIMEOUT_MS);
        const hookEnv = this.buildHookEnv(
          'command',
          eventData ? (() => { try { return JSON.parse(eventData).toolName; } catch { return undefined; } })() : undefined,
          envFileHint,
        );
        // Plugin/skill-contributed command hooks resolve their own base directory:
        // inject CLAUDE_PLUGIN_ROOT (+ AMI_PLUGIN_ROOT alias) into the child env and
        // substitute the `${CLAUDE_PLUGIN_ROOT}` placeholder in the command + args so
        // the hook can reference files co-located with its bundle regardless of the
        // session cwd (Claude Code plugin-hook env/substitution + skillRoot parity).
        const pluginRoot = config.pluginRoot;
        if (pluginRoot) {
          hookEnv.CLAUDE_PLUGIN_ROOT = pluginRoot;
          hookEnv.AMI_PLUGIN_ROOT = pluginRoot;
        }
        const command = substitutePluginRoot(config.command, pluginRoot);
        const args = config.args?.map(a => substitutePluginRoot(a, pluginRoot));
        try {
          const { exitCode, stdout, stderr } = await runCommandHookAsync(command, cwd, hookEnv, timeout, eventData, args, signal, config.shell);
          // Claude Code exit-code contract:
          //   0           = success — parse stdout for a decision
          //   2           = BLOCKING error — reason from stderr → DENY
          //   other != 0  = NON-blocking error — surface stderr, do NOT block the tool
          if (exitCode === 2) {
            // Exit 2 is a BLOCKING error, not a clean run: a once hook must retry.
            if (outcome) outcome.success = false;
            if (config.asyncRewake) {
              this.emit('hookRewake', { event: 'command', message: (stderr || stdout).trim() || 'Background hook completed with exit code 2' });
            }
            return `DENY:${(stderr || stdout || 'Hook blocked (exit 2)').trim()}`;
          }
          if (exitCode !== 0) {
            // Non-zero (non-2) is a NON-blocking error — also not a successful run.
            if (outcome) outcome.success = false;
            const msg = (stderr || stdout).trim();
            return msg ? `SYSTEM:${msg}` : '';
          }
          const trimmed = stdout.trim();
          try {
            const rawParsed = JSON.parse(trimmed);
            const firedEvent: string | undefined =
              (() => { try { return JSON.parse(eventData).hook_event_name; } catch { return undefined; } })();
            // hookEventName spoof check: a hook whose hookSpecificOutput names a
            // DIFFERENT event than the one it fired for is rejected (not acted
            // on) and surfaced as a validation error — a failed run, so a `once`
            // hook retries (Claude Code parity).
            const spoof = hookEventNameMismatch(rawParsed, firedEvent);
            if (spoof) {
              if (outcome) outcome.success = false;
              return `SYSTEM:${sanitizeHookOutput(spoof)}`;
            }
            const parsed: HookDecisionOutput = unwrapHookSpecificOutput(rawParsed);
            // Strict shape validation (Claude Code parseHookOutput): a KNOWN
            // decision field with a wrong-typed value is malformed output, not a
            // silent no-op. Surface it as a validation error + failed run so a
            // `once` hook retries instead of mis-acting on e.g. continue:"yes".
            // An unknown-decision ENUM error is deliberately NOT short-circuited
            // here — the specialized permission/decision executors handle it
            // fail-closed downstream (unknownHookDecisionError), so only TYPE
            // errors on the remaining fields block at this layer.
            const typeErr = validateHookDecisionOutput(parsed as Record<string, unknown>);
            if (typeErr && !unknownHookDecisionError(parsed as Record<string, unknown>)) {
              if (outcome) outcome.success = false;
              return `SYSTEM:${sanitizeHookOutput(typeErr)}`;
            }
            if (parsed.decision === 'block' || parsed.decision === 'deny') return `DENY:${formatHookDenyReason(firedEvent, parsed.reason)}`;
            if (parsed.decision === 'ask' || parsed.permissionDecision === 'ask') return `ASK:${parsed.reason ?? ''}`;
            if (parsed.continue === false) return `DENY:${formatHookDenyReason(firedEvent, parsed.stopReason)}`;
            if (parsed.updatedInput) return `MODIFY:${JSON.stringify(parsed.updatedInput)}`;
            if (parsed.additionalContext) return `MODIFY:${JSON.stringify({ additionalContext: parsed.additionalContext })}`;
            if (parsed.systemMessage) return `SYSTEM:${parsed.systemMessage}`;
            // suppressOutput: a JSON hook result with suppressOutput:true hides
            // the plain-text success surface — return '' instead of echoing the
            // (otherwise decision-less) JSON back as a system message. Explicit
            // decision/systemMessage results above take precedence (Claude Code).
            if (parsed.suppressOutput) return '';
          } catch (parseErr) {
            // A hook that emitted text starting with `{` intended to return a
            // JSON decision; a parse failure is malformed output surfaced to the
            // user as a validation error (Claude Code parseHookOutput), NOT
            // silently treated as opaque plain text. Non-`{` output (plain text /
            // DENY:/SYSTEM: prefixes) still falls through below.
            if (trimmed.startsWith('{')) {
              return `SYSTEM:${sanitizeHookOutput(formatHookValidationError(trimmed, parseErr))}`;
            }
          }
          return sanitizeHookOutput(trimmed);
        } catch (err: unknown) {
          // A thrown command (timeout / spawn failure) is not a successful run.
          if (outcome) outcome.success = false;
          coreLog('hooks', `Command hook error: ${err}`);
          // Fail-closed: a guardrail hook that times out or errors must DENY,
          // not silently allow the action through.
          if ((config as CommandHookConfig).onFailure === 'block') {
            return `DENY:Hook execution failed (fail-closed): ${err instanceof Error ? err.message : String(err)}`;
          }
          // Fail-OPEN: the hook did not block, but a crashed/timed-out hook must
          // not be invisible — emit a visible error notice (Claude Code's
          // hook_error_during_execution) so a misconfigured hook is observable.
          this.emitHookError(config, err instanceof Error ? err.message : String(err));
          return '';
        }
      }
      case 'prompt':
        return this.runPromptHook(config, eventData, signal, outcome);
      case 'http':
        return this.runHttpHook(config, eventData, signal, outcome);
      case 'agent':
        return this.runAgentHook(config, eventData, signal, outcome);
      default:
        return '';
    }
  }

  private async runPromptHook(config: PromptHookConfig, eventData: string, signal?: AbortSignal, outcome?: { success: boolean }): Promise<string> {
    const r = await this.runEngineHook('prompt', config.prompt, 'Event data', config.model, config.timeout, 30_000, eventData, signal);
    // An engine error (throw / hard-timeout) is a failed run — a once hook retries.
    if (r.error && outcome) outcome.success = false;
    // Fail-OPEN: a prompt hook whose sub-engine errored/timed out returns '' —
    // surface the error visibly so it is not silent (Claude Code parity).
    if (r.error) this.emitHookError(config, r.error);
    if (config.asyncRewake && r.error) this.emit('hookRewake', { event: 'prompt', message: `Prompt hook error: ${r.error}` });
    if (config.asyncRewake && r.output.startsWith('DENY:')) this.emit('hookRewake', { event: 'prompt', message: r.output });
    return r.output;
  }

  /**
   * Shared runner for engine-backed hooks (prompt + agent). Spins up a hook
   * sub-engine with:
   *  - `timeout` interpreted as SECONDS (resolveHookTimeoutMs), matching command/http;
   *  - a HARD timeout + parent-signal AbortController passed to the engine so a
   *    stalled model call is actively torn down (not a cooperative between-events
   *    deadline that a pre-first-event hang never reaches);
   *  - HOOK_AGENT_DISALLOWED_TOOLS filtered out so a hook agent cannot spawn
   *    subagents or enter plan mode.
   */
  private async runEngineHook(
    kind: 'prompt' | 'agent',
    promptText: string,
    dataLabel: string,
    model: string | undefined,
    timeoutSeconds: number | undefined,
    defaultMs: number,
    eventData: string,
    signal?: AbortSignal,
  ): Promise<{ output: string; error?: string }> {
    if (!this._engineFactory) {
      coreLog('hooks', `${kind} hook skipped: no engine factory available`);
      return { output: '' };
    }
    const providerConfig = { ...(this._providerConfig || {}) };
    if (model) {
      providerConfig.model = model;
    } else if (kind === 'prompt') {
      // A prompt hook is a cheap, single-shot verification verdict — default it to
      // the operator-configured small/fast model when one is set, so it doesn't burn
      // the main model's budget/latency. Parity: execPromptHook `model: hook.model ??
      // getSmallFastModel()`. With no env set, getSmallFastModel() returns undefined
      // and the session model is kept unchanged (current behavior preserved).
      const fast = getSmallFastModel();
      if (fast) providerConfig.model = fast;
    }

    const timeoutMs = resolveHookTimeoutMs(timeoutSeconds, defaultMs);
    const abort = combineHookAbort(signal, timeoutMs);

    const engine = this._engineFactory({
      provider: providerConfig,
      cwd: this._cwd,
      // Run under the session's permission context (not blanket auto-allow unless
      // that is the session's mode), so a hook agent cannot exceed the user's grant.
      permissionMode: this._hookEnginePermissionMode,
      abortController: abort.controller,
      disallowedTools: HOOK_AGENT_DISALLOWED_TOOLS,
      // Disable extended thinking for hook evaluation — a verification hook needs a
      // fast structured verdict, not a reasoning trace (Claude Code's
      // thinkingConfig:{type:'disabled'} for prompt/agent hooks).
      disableThinking: true,
      // Hard turn-count cap on agent (multi-turn) hooks so a verification agent
      // cannot loop indefinitely even inside the timeout budget (MAX_AGENT_TURNS).
      // Prompt hooks are single-shot, so the cap only applies to the agent kind.
      ...(kind === 'agent' ? { maxTurns: MAX_AGENT_TURNS } : {}),
    });

    // Compose the hook prompt, then prepend the {ok, reason} verification contract
    // so an AI-driven hook returns a parseable decision rather than free prose
    // (Claude Code's hookResponseSchema). When the prompt references the hook input
    // via a placeholder ($ARGUMENTS / $ARGUMENTS[n] / $n), substitute it in place
    // (Claude Code's addArgumentsToPrompt); otherwise append the event data under
    // the per-kind `dataLabel` ('Event data' / 'Context').
    const hasArgPlaceholder = /\$ARGUMENTS|\$\d/.test(promptText);
    const body = hasArgPlaceholder
      ? substituteHookArguments(promptText, eventData)
      : `${promptText}\n\n${dataLabel}:\n${eventData}`;
    const fullPrompt = `${HOOK_VERIFICATION_CONTRACT}\n\n${body}`;
    // Introspection debug trace (Claude Code execPromptHook's logForDebugging
    // "Processing prompt hook with prompt: …"): the exact processed prompt the
    // hook evaluates, so `--debug` / AMI_DEBUG_TO_STDERR reveals what it saw.
    coreLog('hooks', `${kind} hook processing prompt: ${body}`);
    let result = '';
    let error: string | undefined;
    // Distinguish a cancelled/timed-out/errored run from a genuine completion: a
    // run that did not finish produced NO trustworthy verdict, so its partial
    // model text must NEVER be returned as a hook decision (Claude Code's
    // execPromptHook/execAgentHook return a 'cancelled'/non_blocking_error
    // outcome on abort/error, not the partial text).
    let errored = false;
    try {
      for await (const event of engine.submit(fullPrompt)) {
        if (abort.aborted()) break;
        if (event.type === 'text_delta' && event.text) result += event.text;
        if (event.type === 'error') { error = event.error || 'hook engine error'; errored = true; break; }
      }
    } catch (err) {
      error = String(err);
      errored = true;
      coreLog('hooks', `${kind} hook error: ${err}`);
    } finally {
      abort.cleanup();
      engine.shutdown?.();
    }
    if (abort.timedOut()) coreLog('hooks', `${kind} hook hard-timeout after ${timeoutMs}ms`);
    // Introspection debug trace (Claude Code's "Model response: …"): the raw
    // reply the hook produced, logged before any decision is derived from it.
    coreLog('hooks', `${kind} hook model reply: ${result}`);
    // A cancelled/timed-out/errored run yields no decision — discard the partial
    // output (never block/allow on half a reply) and report the run as failed so
    // a `once` hook retries instead of self-removing.
    if (abort.aborted() || errored) {
      const why = abort.timedOut() ? 'timed out' : abort.aborted() ? 'cancelled' : `errored: ${error}`;
      coreLog('hooks', `${kind} hook did not complete (${why}) — discarding partial output`);
      return { output: '', error: error ?? why };
    }
    // Structured verification result → block (ok:false) or allow (ok:true).
    const verification = parseHookVerification(result);
    if (verification) {
      // Claude Code wording parity: "<Prompt|Agent> hook condition was not met: …"
      const label = kind === 'agent' ? 'Agent' : 'Prompt';
      coreLog('hooks', verification.ok
        ? `${kind} hook condition was met`
        : `${kind} hook condition was not met: ${verification.reason ?? 'condition not met'}`);
      return {
        output: verification.ok ? '' : `DENY:${label} hook condition was not met: ${verification.reason ?? 'condition not met'}`,
        error,
      };
    }
    // Non-conforming output (did not match the {ok,reason} schema): surface a
    // distinct, non-blocking diagnostic (Claude Code's hook_non_blocking_error
    // "model response does not conform to expected schema") instead of silently
    // treating free-form prose as a verdict. AMI keeps its SYSTEM:/raw-context
    // passthrough superset here — the output is still applied as context, never
    // as a block — so the run stays non-blocking exactly as Claude Code's
    // non_blocking_error outcome does.
    coreLog('hooks', `${kind} hook output did not match the {ok,reason} schema — treating as non-blocking raw output`);
    return { output: sanitizeHookOutput(result), error };
  }

  private async runHttpHook(config: HttpHookConfig, eventData: string, signal?: AbortSignal, outcome?: { success: boolean }): Promise<string> {
    const http = require('http');
    const https = require('https');
    // Fail-closed: an HTTP hook that times out or errors DENIES when onFailure:block.
    // Any failure path (allowlist/SSRF block, non-2xx, transport error, timeout) is
    // NOT a successful run — a once hook stays registered to retry (Claude parity).
    const onFail = () => { if (outcome) outcome.success = false; return config.onFailure === 'block' ? 'DENY:HTTP hook failed (fail-closed)' : ''; };
    // Fail-closed WITH the underlying cause: a transport error / timeout surfaces
    // its specific detail in the DENY reason and the debug log (Claude Code's
    // execHttpHook `logForDebugging("HTTP hook error: <msg>")` + distinct errorMsg),
    // instead of a generic "failed" with no diagnosable cause.
    const onFailWith = (detail: string) => {
      coreLog('hooks', `HTTP hook error: ${detail}`);
      if (outcome) outcome.success = false;
      // Fail-OPEN transport error/timeout would otherwise return '' silently —
      // surface it visibly (Claude Code parity). The block path already DENYs.
      if (config.onFailure !== 'block') this.emitHookError(config, detail);
      return config.onFailure === 'block' ? `DENY:HTTP hook failed: ${detail}` : '';
    };

    // (1) URL allowlist gate (three-state) — enforced BEFORE any I/O.
    if (!isHttpHookUrlAllowed(config.url, this._httpHookPolicy.allowedHttpHookUrls)) {
      coreLog('hooks', `HTTP hook blocked: ${config.url} not in allowedHttpHookUrls`);
      return onFail();
    }

    // (2) SSRF guard — resolve + validate the hostname, reusing the shared
    // web-utils validator (blocks private/link-local/metadata IPs) and pin the
    // resolved IP to stop DNS-rebinding (TOCTOU). Loopback is allowed (local dev
    // webhook servers), matching Claude Code's ssrfGuardedLookup. No separate impl.
    const safety = await this._validateUrlSafety(config.url, undefined, { allowLoopback: true });
    if ('error' in safety) {
      // User-facing, code-tagged block message (Claude Code's ssrfError): always
      // inform why the hook was refused — fail-closed DENY when onFailure:block,
      // otherwise a non-blocking SYSTEM notice so the target is never silently
      // dropped (the affordance Claude surfaces to the user).
      const msg = formatHttpHookBlockedMessage(config.url, safety.error);
      coreLog('hooks', msg);
      if (outcome) outcome.success = false;
      return config.onFailure === 'block' ? `DENY:${msg}` : `SYSTEM:${msg}`;
    }

    const url = new URL(config.url);
    const client = url.protocol === 'https:' ? https : http;

    // (3) Header env-var interpolation, gated by the hook's allowedEnvVars ∩ the
    // global httpHookAllowedEnvVars setting.
    const envAllow = new Set(
      (config.allowedEnvVars ?? []).filter(v => (this._httpHookPolicy.httpHookAllowedEnvVars ?? []).includes(v)),
    );
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    for (const [k, v] of Object.entries(config.headers ?? {})) {
      headers[k] = interpolateHookHeaderEnv(v, envAllow);
    }

    // (5) Cancellation + hard timeout as ONE combined signal (the canonical
    // combineHookAbort): the request is torn down on parent/user cancel OR the
    // per-hook timeout, matching command/prompt/agent hooks.
    const timeoutMs = resolveHookTimeoutMs(config.timeout, DEFAULT_HTTP_HOOK_TIMEOUT_MS);
    const abort = combineHookAbort(signal, timeoutMs);
    // Introspection: log the outbound request target before any I/O (Claude Code's
    // execHttpHook `logForDebugging("Hooks: HTTP hook POST to <url>")`).
    coreLog('hooks', `HTTP hook POST to ${config.url}`);
    // Classify a transport failure: a combined-signal abort is a timeout/cancel
    // (name it so), anything else carries the Node error message.
    const failDetail = (err?: Error): string =>
      abort.timedOut() ? `timed out after ${timeoutMs}ms`
        : abort.aborted() ? 'request cancelled'
          : (err?.message || 'transport error');
    return new Promise((resolve) => {
      let settled = false;
      const done = (val: string) => { if (!settled) { settled = true; abort.cleanup(); resolve(val); } };
      const req = client.request(url, {
        method: 'POST',
        headers,
        signal: abort.signal,
        maxRedirects: 0,                       // stop redirect-based SSRF bypass
        lookup: pinnedDnsLookup(safety.resolvedIP),
      }, (res: any) => {
        let body = '';
        res.on('data', (d: Buffer) => body += d);
        res.on('end', () => {
          // (4) Non-2xx is a failure — do NOT parse an error body as a decision.
          const status = res.statusCode ?? 0;
          // Introspection: response status + body length (Claude Code's
          // execHttpHook `logForDebugging("… response status <code>, body length <n>")`).
          coreLog('hooks', `HTTP hook response status ${status}, body length ${body.length}`);
          if (status < 200 || status >= 300) {
            // Non-2xx is "not ok" but not a transport error — keep the generic
            // fail-closed DENY (the specific status is already in the debug log).
            coreLog('hooks', `HTTP hook non-2xx (${status})`);
            done(onFail());
            return;
          }
          done(sanitizeHookOutput(body));
        });
        res.on('error', (err: Error) => done(onFailWith(failDetail(err))));
      });
      req.on('error', (err: Error) => done(onFailWith(failDetail(err))));
      req.write(eventData);
      req.end();
    });
  }

  private async runAgentHook(config: AgentHookConfig, eventData: string, signal?: AbortSignal, outcome?: { success: boolean }): Promise<string> {
    const r = await this.runEngineHook('agent', config.prompt, 'Context', config.model, config.timeout, 60_000, eventData, signal);
    // An engine error (throw / hard-timeout) is a failed run — a once hook retries.
    if (r.error && outcome) outcome.success = false;
    // Fail-OPEN: surface a visible error notice when the agent hook errored so it
    // is not silent (Claude Code parity).
    if (r.error) this.emitHookError(config, r.error);
    return r.output;
  }

  private removeHookOnce<T>(hookArray: T[], hookFn: T, event?: string): void {
    const idx = hookArray.indexOf(hookFn);
    if (idx >= 0) {
      hookArray.splice(idx, 1);
      // Observable one-shot lifecycle (Claude Code's
      // "Removing one-shot hook for event <name>" debug line).
      if (event) coreLog('hooks', `Removing one-shot hook for event ${event}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Pattern matching utility
// ---------------------------------------------------------------------------

function matchesPattern(pattern: string, value: string): boolean {
  if (!pattern || pattern === '*') return true;
  // Claude Code semantics (matchesPattern): a matcher made only of simple token
  // chars (alphanumeric/underscore, optionally pipe-separated) is an EXACT match
  // — NOT a substring/regex — so `Edit` scopes to the `Edit` tool alone and never
  // leaks onto `EditNotebook`. Only a matcher containing regex metacharacters is
  // treated as a regex (invalid-regex fallback handled below).
  if (/^[A-Za-z0-9_|]+$/.test(pattern)) {
    if (pattern.includes('|')) return pattern.split('|').some(p => p === value);
    return pattern === value;
  }
  // A matcher with regex metacharacters is a regex; an invalid one falls back to
  // literal equality (AMI's long-standing behavior — safer than silently matching
  // nothing when a user wrote a plain string that happens to contain a metachar).
  try { return new RegExp(pattern).test(value); } catch { return pattern === value; }
}

/** Pull the content string a `Tool(content)` matcher checks against the tool
 *  input (the command for bash, the path for file tools, the url for web). */
function hookInputContent(toolInput: Record<string, unknown>): string {
  const candidate =
    toolInput.command ?? toolInput.file_path ?? toolInput.path ?? toolInput.url ??
    toolInput.pattern ?? toolInput.query;
  if (typeof candidate === 'string') return candidate;
  return Object.values(toolInput).filter(v => typeof v === 'string').join(' ');
}

const TOOL_IF_RE = /^([A-Za-z_][A-Za-z0-9_]*)\(([\s\S]*)\)$/;

/**
 * Evaluate a hook `if` condition. Beyond a bare tool-NAME regex, this supports
 * Claude Code's `Tool(content)` rule syntax (e.g. `Bash(git *)`,
 * `Edit(src/**)`) — it checks the tool name matches the rule's tool and then
 * glob-matches the inner content against the tool's input, so a hook can be
 * gated to e.g. only git commands or only certain paths rather than every Bash.
 */
export function matchesHookIf(
  ifCond: string,
  toolName: string,
  toolInput: Record<string, unknown>,
): boolean {
  const m = TOOL_IF_RE.exec(ifCond.trim());
  if (!m) return matchesPattern(ifCond, toolName);
  const ruleTool = translateHookMatcher(m[1]);
  const inner = m[2];
  if (!matchesPattern(ruleTool, toolName)) return false;
  if (inner === '' || inner === '*') return true;
  return wildcardMatch(inner, hookInputContent(toolInput));
}

