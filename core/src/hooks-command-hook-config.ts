// Split from hooks.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { createHash } from 'crypto';
import type { Message } from './types';
import type { PermissionUpdate } from './permissions';
import { HOOK_EVENTS } from './hooks-session-context';
import type { HookConfigType } from './hooks-session-context';
import type { HookDecisionClassification } from './hooks-stop-hook-summary';
import { tryParseHookJson } from './hooks-async-rewake-delivery-message';

/**
 * the reference's canonical hook output wraps event-specific fields in a
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

/** Extract the worktree path a WorktreeCreate provider hook reported, matching
 *  the reference's executeWorktreeCreateHook: prefer `hookSpecificOutput.worktreePath`
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

/**
 * Content-identity equality for hook configs (the reference's isHookEqual): two
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
  /** Interpreter for the command (the reference's per-hook shell selection):
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
   *  raw command (the reference's getHookDisplayText `statusMessage`). Lets a hook
   *  present e.g. 'Running security scan…' instead of leaking the shell command. */
  statusMessage?: string;
  /** Filesystem base directory of the plugin bundle (or skill) that contributed
   *  this command hook. When set, the command/args have `${CLAUDE_PLUGIN_ROOT}`
   *  (and the `${AMI_PLUGIN_ROOT}` alias) substituted to this path, and the hook
   *  child process receives `CLAUDE_PLUGIN_ROOT`/`AMI_PLUGIN_ROOT` env vars so a
   *  plugin/skill-registered hook can locate its own co-located files regardless of
   *  the session cwd (the reference's CLAUDE_PLUGIN_ROOT / registerSkillHooks
   *  skillRoot). Populated by loadFromFile's plugin-discovery block; absent for
   *  filesystem (.ami/.claude/config) hooks. */
  pluginRoot?: string;
  /** Name of the plugin bundle that contributed this hook, when it came from a
   *  plugin. Carried onto the runtime config so a backgrounded (asyncRewake) hook's
   *  completion/wake payload is attributable to its plugin (the reference's per-hook
   *  `pluginName`/`pluginId` provenance). Absent for filesystem/config hooks. */
  pluginName?: string;
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
  /** Friendly label for progress/introspection (the reference `statusMessage`). */
  statusMessage?: string;
  /** Plugin bundle root + name when contributed by a plugin — carried so a
   *  backgrounded prompt hook's rewake payload is attributable to its plugin
   *  (the reference `pluginId` provenance). Absent for filesystem/config hooks. */
  pluginRoot?: string;
  pluginName?: string;
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
  /** Friendly label for progress/introspection (the reference `statusMessage`). */
  statusMessage?: string;
  /** Plugin bundle root + name when contributed by a plugin — carried so a
   *  backgrounded HTTP hook's rewake payload is attributable to its plugin
   *  (the reference `pluginId` provenance). Absent for filesystem/config hooks. */
  pluginRoot?: string;
  pluginName?: string;
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
  /** Friendly label for progress/introspection (the reference `statusMessage`). */
  statusMessage?: string;
  /** Plugin bundle root + name when contributed by a plugin — carried so a
   *  backgrounded agent hook's rewake payload is attributable to its plugin
   *  (the reference `pluginId` provenance). Absent for filesystem/config hooks. */
  pluginRoot?: string;
  pluginName?: string;
  /** Opt-in structured-output enforcement (the reference's
   *  `registerStructuredOutputEnforcement`, hookHelpers.ts L66-L83): when `true`, an
   *  agent hook run that STOPS without ever calling the {@link VERIFICATION_OUTPUT_TOOL_NAME}
   *  verification tool is re-prompted ONCE with a precise nudge
   *  ({@link buildStructuredOutputEnforcementNudge}) telling the model it MUST call
   *  the tool now, rather than silently falling back to free-text. Off by default so
   *  existing hooks are unaffected; set it on verification/agent hooks that must emit
   *  a machine-readable verdict. */
  enforceStructuredOutput?: boolean;
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
   *  be treated as a background/async hook (the reference's AsyncHookJSONOutput).
   *  Validated by {@link validateHookDecisionOutput}; carried so a consumer can
   *  detect the runtime async-declaration channel. */
  async?: boolean;
  /** Background-hook lifetime budget (ms) a hook may self-declare alongside
   *  `async:true` (the reference's `asyncTimeout`). */
  asyncTimeout?: number;
  /** PermissionRequest ALLOW: durable permission-rule mutations the engine should
   *  persist (the reference's `decision.updatedPermissions`). */
  updatedPermissions?: PermissionUpdate[];
  /** PermissionRequest DENY: abort the whole turn (not just skip this tool) —
   *  the reference's `decision.interrupt` → `abortController.abort()`. */
  interrupt?: boolean;
  /** PermissionRequest allow/deny classification (the reference's
   *  `decisionClassification`): user_temporary | user_permanent | user_reject.
   *  Parsed leniently — a malformed value is ignored, not an error. */
  decisionClassification?: HookDecisionClassification;
}

/**
 * In-process ('callback') hook. Receives the firing `event`, its `data`, and an
 * optional `abort` AbortSignal (the reference's `HookCallback` `abort: AbortSignal
 * | undefined`) so a long-running callback can observe a user interrupt / turn
 * abort and bail early via cooperative cancellation. The signal is threaded by
 * {@link HookManager.executeCallbacks}; a callback that ignores it still works
 * (the caller stops awaiting it on abort regardless — see {@link raceAbort}).
 */
export type HookCallback = (event: string, data: unknown, abort?: AbortSignal) => Promise<HookDecisionOutput | string | void>;

/**
 * Function-hook callback (the reference's {@link FunctionHookCallback}): an
 * in-memory TypeScript gate that inspects the conversation messages and returns
 * `true` to let the event proceed or `false` to block it. Session-scoped only —
 * it embeds a live callback and so can never be persisted to settings.json.
 */
export type FunctionHookCallback = (messages: Message[], signal?: AbortSignal) => boolean | Promise<boolean>;

/**
 * A runtime ('function') hook with an embedded callback (the reference's
 * sessionHooks.ts `FunctionHook`). Registered via {@link HookManager.addFunctionHook},
 * returns `true`=pass / `false`=block, and surfaces {@link errorMessage} as the
 * block reason. Never loaded from a config file (hence NOT in HOOK_CONFIG_TYPES,
 * which is the file-config union): a JS callback cannot be serialized.
 */
export interface FunctionHook {
  type: 'function';
  /** Stable id for id-based removal; auto-generated when not supplied. */
  id?: string;
  /** Per-callback timeout (ms); defaults to {@link DEFAULT_CALLBACK_HOOK_TIMEOUT_MS}. */
  timeout?: number;
  callback: FunctionHookCallback;
  /** Block reason surfaced when the callback returns `false`. */
  errorMessage: string;
  /** Optional author-facing progress label. */
  statusMessage?: string;
}

/**
 * Scoping metadata for a runtime callback hook (the reference's SessionHookMatcher
 * `matcher`/`skillRoot` + addSessionHook `sessionId`). Lets a skill/stance/agent
 * register hooks at runtime that are matcher-gated and can be torn down as a group
 * by the skill root or session that installed them.
 */
export interface RuntimeHookMeta {
  /** Glob matcher gating which tool/event names fire this hook ('' / absent = all). */
  matcher?: string;
  /** Owning skill/plugin base directory (the reference `skillRoot`) for grouped teardown. */
  skillRoot?: string;
  /** Owning session/agent id (the reference `sessionId`) for end-of-session teardown. */
  sessionId?: string;
  /** Stable id (function hooks) for id-based removal. */
  id?: string;
  /**
   * Marks a SYSTEM-registered in-process callback (the reference's HookCallback
   * `internal:true` — e.g. the session-file-access guard). An internal hook runs
   * exactly like any other callback but is hidden from the user-facing hook count
   * ({@link getSummary} `callbackHooks`) so an operator's `/hooks` view reflects
   * only the hooks they configured, not AMI's own built-ins. The built-in count is
   * still surfaced separately as `internalCallbackHooks` for full introspection.
   */
  internal?: boolean;
}

/**
 * A hook-execution lifecycle event (the reference's hookEvents.ts
 * HookExecutionEvent): `progress` when a hook starts running, `resolved` when it
 * settles. Delivered to a {@link HookManager.registerHookEventHandler} subscriber,
 * and BUFFERED (bounded FIFO) when no subscriber is attached so an early event
 * (e.g. a SessionStart hook firing before the UI attaches) is replayed, never lost.
 */
export interface HookLifecycleEvent {
  phase: 'progress' | 'resolved';
  event: string;
  hookType: HookConfigType;
  label: string;
  /** Present on `resolved`: whether the hook completed successfully. */
  success?: boolean;
  /** Stable per-hook identifier (the reference's HookStartedEvent/HookResponseEvent
   *  `hookId`): the SAME value on the start-`progress`, every streaming-`progress`,
   *  and the terminal `resolved` event for one hook run, so a frontend can correlate
   *  a hook's lifecycle across emissions. Derived by {@link computeHookId}. */
  hookId?: string;
  /** Human-facing hook name (the reference's `hookName`): the display label
   *  (statusMessage when set, else the hook's content). Carried alongside the
   *  internal {@link HookLifecycleEvent.label} for parity with the reference's event shape. */
  hookName?: string;
  /** Present on `resolved`: the 3-way terminal classification (the reference's
   *  HookResponseEvent.outcome) — `success` | `error` | `cancelled`. Finer-grained
   *  than {@link HookLifecycleEvent.success}; `cancelled` means the run was aborted
   *  (user interrupt / shutdown) rather than failing on its own. Set via
   *  {@link classifyHookOutcome}. */
  outcome?: HookLifecycleOutcome;
  /** Present on a STREAMING `progress` event (the reference's emitHookProgress): the
   *  hook's incremental captured stdout so far while it is still running. Absent on
   *  the initial announce-`progress` and on `resolved`. */
  stdout?: string;
  /** Present on a STREAMING `progress` event: incremental captured stderr so far. */
  stderr?: string;
  /** Present on a STREAMING `progress` event: combined stdout+stderr so far. */
  output?: string;
}

/** Terminal 3-way outcome of a hook run (the reference's HookResponseEvent.outcome). */
export const HOOK_LIFECYCLE_OUTCOMES = ['success', 'error', 'cancelled'] as const;

export type HookLifecycleOutcome = (typeof HOOK_LIFECYCLE_OUTCOMES)[number];

/**
 * Classify a hook run's terminal outcome (the reference's finalizeHook outcome:
 * `aborted ? 'cancelled' : success ? 'success' : 'error'`). `aborted` takes
 * precedence — a cancelled (interrupt/shutdown/async-timeout) run is `cancelled`
 * even if its own work had "succeeded" up to the abort. The single source for the
 * 3-way classification shared by the sync `hookResolved` lifecycle event and the
 * async `hookResponse` completion record (no per-site re-derivation). Pure.
 */
export function classifyHookOutcome(success: boolean, aborted: boolean): HookLifecycleOutcome {
  if (aborted) return 'cancelled';
  return success ? 'success' : 'error';
}

/**
 * Stable per-hook identifier for lifecycle-event correlation (the reference's
 * per-hook `hookId`). Deterministic in the firing event, the hook type, the hook's
 * content, and its index within the event's hook array — so two identical hooks on
 * the same event still get distinct ids, while the same hook gets the same id on
 * every one of its lifecycle emissions (start → streaming → resolved). Pure.
 */
export function computeHookId(event: string, hookType: HookConfigType, content: string, index: number): string {
  return createHash('sha256').update(`${event}\0${hookType}\0${index}\0${content}`).digest('hex').slice(0, 12);
}

/** AMI's live-progress polling cadence (ms) for a running command hook. the reference
 *  Code's startHookProgressInterval defaults to 1000ms; AMI polls more frequently
 *  so a short hook still surfaces at least one incremental update. */
export const HOOK_PROGRESS_INTERVAL_MS = 250;

/** Bounded cap on buffered hook-lifecycle events (the reference's MAX_PENDING_EVENTS). */
export const MAX_PENDING_HOOK_EVENTS = 100;

/**
 * Hook events whose lifecycle (`progress`/`resolved`) notifications are ALWAYS
 * published to a {@link HookManager.registerHookEventHandler} subscriber,
 * regardless of the all-events toggle (the reference's `ALWAYS_EMITTED_HOOK_EVENTS`
 * in hookEvents.ts): the low-noise startup lifecycle events kept on for
 * backwards-compatible observability even when the broader channel is quieted.
 */
export const ALWAYS_EMITTED_HOOK_EVENTS = ['sessionStart', 'setup'] as const;

/**
 * Whether a hook-lifecycle event for `event` should be published to subscribers,
 * given whether all-events emission is enabled (the reference's hookEvents.ts
 * `shouldEmit`): events on the {@link ALWAYS_EMITTED_HOOK_EVENTS} allowlist always
 * pass; every other known {@link HOOK_EVENTS} event passes only when
 * `allEnabled` is set; an unknown event name is never emitted. Pure; the gate used
 * by {@link HookManager.publishHookLifecycle} together with
 * {@link HookManager.setAllHookEventsEnabled}.
 */
export function shouldEmitHookLifecycle(event: string, allEnabled: boolean): boolean {
  if ((ALWAYS_EMITTED_HOOK_EVENTS as readonly string[]).includes(event)) return true;
  return allEnabled && (HOOK_EVENTS as readonly string[]).includes(event);
}
