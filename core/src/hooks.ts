import * as fs from 'fs';
import { resolveConfigDir } from './reference-journal';
import * as path from 'path';
import * as os from 'os';
import { spawn } from 'child_process';
import type { Message, AssistantMessage } from './types';
import { log as coreLog } from './logger';
import { EventEmitter } from 'events';
import { canHookEvent, shouldSkipHookDueToTrust } from './hook-protection';
import { verdictRank, wildcardMatch } from './permissions';
import { validateUrlSafety, pinnedDnsLookup } from './tools/web-utils';

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
  'taskCreated', 'taskCompleted', 'postToolUseFailure', 'notification',
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
  /** Parent cancellation signal (user Ctrl-C / turn abort). Threaded into
   *  command/http/prompt/agent hook execution so a long-running hook is torn
   *  down when the surrounding operation is cancelled, combined with the
   *  per-hook timeout. */
  abortSignal?: AbortSignal;
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

export interface SessionContext {
  sessionId: string;
  cwd: string;
}

export interface SubagentContext {
  parentSessionId: string;
  subagentSessionId: string;
  prompt: string;
  mode?: string;
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
}

export interface TaskEventContext {
  taskId: string;
  subject: string;
  status?: string;
  description?: string;
}

export interface UserPromptContext {
  prompt: string;
  turnCount: number;
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
  error: string;
  hookIndex: number;
}

export interface ElicitationContext {
  mcpServerName: string;
  message?: string;
  mode?: string;
}

export type SessionHookFn = (context: SessionContext) => Promise<{ initialUserMessage?: string; watchPaths?: string[] } | void>;
export type SubagentHookFn = (context: SubagentContext) => Promise<void>;
/** SubagentStart hook: may contribute additionalContexts prepended to the
 *  subagent's initial messages (Claude Code parity). */
export type SubagentStartHookFn = (context: SubagentContext) => Promise<{ additionalContexts?: string[] } | void>;
export type CompactHookFn = (context: CompactContext) => Promise<void>;
/** PreCompact hook: may inject/override compaction instructions and surface a
 *  user-facing display message (Claude Code parity). */
export type PreCompactHookFn = (context: CompactContext) => Promise<{ newCustomInstructions?: string; userDisplayMessage?: string } | void>;
export type PermissionHookFn = (context: PermissionContext) => Promise<{ action?: 'allow' | 'deny'; reason?: string } | void>;
export type TaskEventHookFn = (context: TaskEventContext) => Promise<void>;
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
/** ConfigChange hook: a blocking hook can VETO a config change before it is
 *  applied (Claude Code's hasBlockingResult). */
export type ConfigChangeHookFn = (context: ConfigChangeContext) => Promise<{ blockingError?: string } | void>;
export type InstructionsHookFn = (context: InstructionsContext) => Promise<void>;
export type WorktreeHookFn = (context: WorktreeContext) => Promise<void>;
export type FileChangeHookFn = (context: FileChangeContext) => Promise<{ watchPaths?: string[] } | void>;
export type CwdChangedHookFn = (context: CwdChangedContext) => Promise<{ watchPaths?: string[] } | void>;
/** Elicitation action a hook may return to short-circuit the user prompt. */
export type ElicitationAction = 'accept' | 'decline' | 'cancel';
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
export const MAX_HOOKS_PER_EVENT = 10;

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

/**
 * Resolve a command hook's `shell` selector to a concrete spawn descriptor
 * (Claude Code's per-hook shell selection: `hook.shell ?? DEFAULT_HOOK_SHELL`).
 * PowerShell hooks spawn `pwsh -NoProfile -NonInteractive -Command <command>`
 * (non-interactive, no profile — matching the reference), every POSIX shell
 * uses `<shell> -c <command>`. Single source of truth so command execution
 * never hard-codes `bash`.
 */
export function resolveHookShell(command: string, shell: string | undefined): { file: string; args: string[] } {
  const sel = (shell ?? DEFAULT_HOOK_SHELL).toLowerCase();
  if (sel === 'powershell' || sel === 'pwsh') {
    return { file: 'pwsh', args: ['-NoProfile', '-NonInteractive', '-Command', command] };
  }
  const file = sel === 'sh' ? 'sh' : 'bash';
  return { file, args: ['-c', command] };
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
 * Tools a hook-spawned agent/prompt engine must NOT get — prevents a stop/
 * verification hook agent from recursively spawning subagents or entering the
 * interactive plan mode (Claude Code's ALL_AGENT_DISALLOWED_TOOLS). The engine
 * factory filters these out of the hook engine's toolset.
 */
export const HOOK_AGENT_DISALLOWED_TOOLS: readonly string[] = ['task', 'plan_mode'];

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
 * Interpolate `$VAR` / `${VAR}` references in HTTP-hook header values from the
 * process environment, gated by an allowlist (the hook's own `allowedEnvVars`
 * intersected with the global `httpHookAllowedEnvVars` setting). Any var not on
 * the allowlist resolves to the empty string so a malicious hooks.json cannot
 * exfiltrate arbitrary secrets through a header.
 */
export function interpolateHookHeaderEnv(
  value: string,
  allowed: Set<string>,
  env: Record<string, string | undefined> = process.env,
): string {
  return value.replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)/g, (_m, braced, bare) => {
    const name = braced ?? bare;
    if (!allowed.has(name)) return '';
    return env[name] ?? '';
  });
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
};
const HOOK_KEY_SNAKE: Record<string, string> = {
  toolName: 'tool_name', toolInput: 'tool_input', toolOutput: 'tool_response',
  sessionId: 'session_id', subagentSessionId: 'subagent_session_id',
  durationMs: 'duration_ms', turnCount: 'turn_count', messageCount: 'message_count',
};

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
 *  fileChanged which can register dynamic FileChanged watches. */
export function parseWatchPaths(result: string): string[] {
  const obj = tryParseHookJson(result);
  const raw = obj?.watchPaths;
  if (!Array.isArray(raw)) return [];
  return raw.filter((p): p is string => typeof p === 'string' && p.length > 0);
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
  elicitation: 'mcpServerName',
  elicitationResult: 'mcpServerName',
  configChange: 'source',
  instructionsLoaded: 'loadReason',
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
  asyncRewake?: boolean;
  /** 'block' = a hook that times out or errors denies the action (fail-closed);
   *  default 'allow' preserves today's fail-open behavior. */
  onFailure?: 'block' | 'allow';
}

export interface PromptHookConfig {
  type: 'prompt';
  prompt: string;
  model?: string;
  timeout?: number;
  once?: boolean;
  if?: string;
  async?: boolean;
  asyncRewake?: boolean;
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
  asyncRewake?: boolean;
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
  asyncRewake?: boolean;
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
}

export type HookCallback = (event: string, data: unknown) => Promise<HookDecisionOutput | string | void>;

export interface HookConfigEntry {
  event: string;
  hook: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig;
  matcher?: string;
  _source?: 'project' | 'user' | 'config';
}

export interface HookConfigFile {
  hooks: HookConfigEntry[];
}

/**
 * Translate Claude Code's map-keyed hooks.json into AMI's flat entry array:
 *   { "PreToolUse": [{ matcher?, hooks: [{ type, command, ... }] }] }
 * →  [{ event: 'preToolUse', matcher?, hook: { type, command, ... } }, ...]
 * Capitalized event names (PreToolUse) are lower-camel-cased (preToolUse).
 * Unknown shapes are skipped defensively.
 */
/** Map Claude Code's capitalized tool names to AMI tool names so a matcher like
 *  'Bash' or 'Edit|Write' actually gates the AMI tools at execution time. */
const CLAUDE_TO_AMI_TOOL: Record<string, string> = {
  Bash: 'bash', Read: 'file_read', Edit: 'file_edit', Write: 'file_edit',
  MultiEdit: 'file_edit', Glob: 'glob', Grep: 'grep', LS: 'list_dir',
  WebFetch: 'web_fetch', WebSearch: 'web_search', NotebookEdit: 'notebook_edit',
  Task: 'task', TodoWrite: 'todo_write',
};

/** Translate the tool-name tokens in a hook matcher ('Edit|Write' → 'file_edit|
 *  file_edit'). Unknown tokens (already-AMI names, custom regex) pass through. */
export function translateHookMatcher(matcher: string): string {
  return matcher.split('|').map(tok => {
    const t = tok.trim();
    return CLAUDE_TO_AMI_TOOL[t] ?? t;
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
  /** True when the managed kill-switch has disabled every hook. */
  isAllHooksDisabled(): boolean { return this._disableAllHooks; }

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
  /** In-flight async hook promises, tracked so they can be awaited (finalize)
   *  or cancelled (kill) at shutdown instead of leaking as fire-and-forget. */
  private _pendingAsync = new Set<Promise<unknown>>();
  /** Shutdown signal shared by all async hooks; aborting it tears down every
   *  in-flight async hook (command child killed / HTTP request aborted / engine
   *  hook query aborted). Reset after cancelAll so later hooks are not born
   *  pre-aborted. */
  private _asyncAbort = new AbortController();

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
    parentSignal?: AbortSignal,
  ): void {
    const ac = new AbortController();
    const onAbort = () => ac.abort();
    if (this._asyncAbort.signal.aborted || parentSignal?.aborted) {
      ac.abort();
    } else {
      this._asyncAbort.signal.addEventListener('abort', onAbort, { once: true });
      parentSignal?.addEventListener('abort', onAbort, { once: true });
    }
    const p: Promise<unknown> = this.runHookByType(config, cwd, eventData, ac.signal)
      .catch(() => { /* async hooks never surface errors to the turn */ })
      .finally(() => {
        this._asyncAbort.signal.removeEventListener('abort', onAbort);
        parentSignal?.removeEventListener('abort', onAbort);
        this._pendingAsync.delete(p);
      });
    this._pendingAsync.add(p);
  }

  /** Await every in-flight async hook to settle (graceful shutdown). */
  async finalizePendingAsyncHooks(): Promise<void> {
    await Promise.all(Array.from(this._pendingAsync));
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

  private buildHookEnv(event: string, toolName?: string): Record<string, string> {
    return {
      ...sanitizeHookEnv(process.env as Record<string, string | undefined>),
      SUPERINFERENCE_PROJECT_DIR: this._cwd,
      SUPERINFERENCE_SESSION_ID: this._sessionId,
      SUPERINFERENCE_TOOL_NAME: toolName || '',
      SUPERINFERENCE_HOOK_EVENT: event,
      SUPERINFERENCE_CWD: this._cwd,
      SUPERINFERENCE_MODEL: this._providerConfig?.model ?? '',
      SUPERINFERENCE_PROVIDER: this._providerConfig?.provider ?? '',
      SUPERINFERENCE_EFFORT: this._effortLevel,
    };
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
  private postToolUseFailureHooks: PostToolUseHookFn[] = [];
  private notificationHooks: NotificationHookFn[] = [];
  private setupHooks: SessionHookFn[] = [];
  private configChangeHooks: ConfigChangeHookFn[] = [];
  private instructionsLoadedHooks: InstructionsHookFn[] = [];
  private worktreeCreateHooks: WorktreeHookFn[] = [];
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
    // Defense-in-depth: in an interactive session an untrusted workspace fires
    // NO hooks of any tier (Claude Code's shouldSkipHookDueToTrust). Project
    // entries are also gated below, but this blocks user/config-tier hooks too.
    if (shouldSkipHookDueToTrust(this._workspaceTrusted, this._interactive)) {
      coreLog('hooks', 'Skipping ALL hooks: workspace not trusted (interactive)');
      return;
    }
    // Managed-policy kill-switch: a managed `disableAllHooks` skips EVERY hook,
    // including managed ones (Claude Code's shouldDisableAllHooksIncludingManaged).
    if (this._disableAllHooks) {
      coreLog('hooks', "Skipping ALL hooks: 'disableAllHooks' managed policy is set");
      return;
    }
    const filePath = path.join(cwd, '.ami', 'hooks.json');
    const userHooksPath = path.join(os.homedir(), '.ami', 'hooks.json');

    const sources: Array<{ path: string; source: 'project' | 'user'; managed?: boolean }> = [];
    // Admin/managed settings (highest authority) — the ONLY source honored when
    // allowManagedHooksOnly is set.
    const managedPath = process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    if (managedPath) sources.push({ path: managedPath, source: 'user', managed: true });
    sources.push(
      { path: filePath, source: 'project' },
      { path: userHooksPath, source: 'user' },
      // Claude Code compatibility: load hooks from .claude/settings.json
      // ({ "hooks": { "PreToolUse": [{ matcher, hooks: [...] }] } }). The
      // map-keyed `hooks` field is handled by normalizeMapKeyedHooks below, and
      // project-scoped entries are gated by workspace trust. (R2-M8)
      { path: path.join(cwd, '.claude', 'settings.json'), source: 'project' },
      { path: path.join(os.homedir(), '.claude', 'settings.json'), source: 'user' },
    );

    // Plugin bundles contribute hooks.json (project-scoped ones gated by trust).
    // Skipped entirely under allowManagedHooksOnly (only managed hooks run).
    if (!this._allowManagedHooksOnly) try {
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
          if (bundle.hooksFile) sources.push({ path: bundle.hooksFile, source: src });
        }
      }
    } catch { /* plugins module optional */ }

    // Org policy: when allowManagedHooksOnly is set, drop every non-managed source
    // so only admin/managed-settings hooks execute.
    const effectiveSources = this._allowManagedHooksOnly ? sources.filter(s => s.managed) : sources;

    // Content-identity de-dup across overlapping sources: the same hook defined
    // in two places (e.g. .ami/hooks.json and .claude/settings.json) registers once.
    const accepted: Array<{ event: string; hook: HookConfigEntry['hook'] }> = [];
    const alreadyAccepted = (entry: HookConfigEntry): boolean =>
      accepted.some(a => a.event === entry.event && entry.hook && isHookEqual(a.hook, entry.hook));

    for (const { path: p, source } of effectiveSources) {
      let raw: string;
      try {
        raw = fs.readFileSync(p, 'utf-8');
      } catch {
        continue;
      }
      try {
        const config: HookConfigFile = JSON.parse(raw);
        // Accept both AMI's array form and Claude Code's map-keyed form
        // ({ "PreToolUse": [{ matcher, hooks: [...] }] }).
        const entries = Array.isArray(config.hooks)
          ? config.hooks
          : normalizeMapKeyedHooks(config.hooks);
        if (!entries.length) continue;
        for (const entry of entries) {
          entry._source = source;
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
        coreLog('hooks', `Invalid hooks.json at ${p}: ${err}`);
      }
    }

    if (!this._allowManagedHooksOnly) try {
      const configPath = path.join(cwd, '.ami', 'config.json');
      if (fs.existsSync(configPath)) {
        const raw = fs.readFileSync(configPath, 'utf-8');
        const config = JSON.parse(raw);
        if (config.hooks && Array.isArray(config.hooks)) {
          for (const entry of config.hooks) {
            entry._source = 'config';
            this.processHookEntry(entry, cwd);
          }
        }
      }
    } catch {}
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

        switch (entry.event) {
        case 'preToolUse': {
          const fn: PreToolUseHookFn = async (ctx) => {
            if (hookConfig.if && !matchesHookIf(hookConfig.if, ctx.toolName, ctx.toolInput)) return { action: 'allow' };
            if (matcher && !matchesPattern(matcher, ctx.toolName)) return { action: 'allow' };
            const eventData = serializeHookEvent({ event: 'preToolUse', toolName: ctx.toolName, toolInput: ctx.toolInput });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData, ctx.abortSignal);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData, ctx.abortSignal); return { action: 'allow' }; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.preToolUseHooks, fn);
            if (result.startsWith('DENY:')) return { action: 'deny', reason: result.slice(5).trim() };
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
            const runner = () => this.runHookByType(hookConfig, cwd, eventData, ctx.abortSignal);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData, ctx.abortSignal); return; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.postToolUseHooks, fn);
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
            const eventData = serializeHookEvent({ event: 'preSampling' });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData, ctx.abortSignal); return { action: 'allow' }; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.preSamplingHooks, fn);
            if (result.startsWith('DENY:')) return { action: 'deny', reason: result.slice(5).trim() };
            return { action: 'allow' };
          };
          this.onPreSampling(fn, tier);
          break;
        }
        case 'postSampling': {
          const fn: HookFn = async () => {
            const eventData = serializeHookEvent({ event: 'postSampling' });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.postSamplingHooks, fn);
          };
          this.onPostSampling(fn);
          break;
        }
        case 'stop': {
          const fn: StopHookFn = async () => {
            const eventData = serializeHookEvent({ event: 'stop' });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.stopHooks, fn);
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
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.errorHooks, fn);
          };
          this.onError(fn);
          break;
        }
        case 'sessionStart': {
          const fn: SessionHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'sessionStart', sessionId: ctx.sessionId });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.sessionStartHooks, fn);
            const watchPaths = this.registerWatchPaths('sessionStart', result);
            try {
              const parsed = JSON.parse(result);
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
            const eventData = serializeHookEvent({ event: 'sessionEnd', sessionId: ctx.sessionId });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.sessionEndHooks, fn);
          };
          this.onSessionEnd(fn);
          break;
        }
        case 'subagentStart': {
          const fn: SubagentStartHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('subagentStart', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'subagentStart', prompt: ctx.prompt });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.subagentStartHooks, fn);
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
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.subagentStopHooks, fn);
          };
          this.onSubagentStop(fn);
          break;
        }
        case 'preCompact': {
          const fn: PreCompactHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('preCompact', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'preCompact', messageCount: ctx.messageCount, trigger: ctx.trigger, customInstructions: ctx.customInstructions ?? undefined });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.preCompactHooks, fn);
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
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.postCompactHooks, fn);
          };
          this.onPostCompact(fn);
          break;
        }
        case 'permissionRequest': {
          const fn: PermissionHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('permissionRequest', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'permissionRequest', toolName: ctx.toolName, toolInput: ctx.toolInput });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.permissionRequestHooks, fn);
            if (result.startsWith('DENY:')) return { action: 'deny' as const, reason: result.slice(5).trim() };
            if (result === 'ALLOW' || result.startsWith('ALLOW:')) return { action: 'allow' as const };
            try {
              const parsed: HookDecisionOutput = JSON.parse(result);
              if (parsed.permissionDecision === 'allow' || parsed.decision === 'approve') return { action: 'allow' as const };
              if (parsed.permissionDecision === 'deny' || parsed.decision === 'deny' || parsed.decision === 'block') return { action: 'deny' as const, reason: parsed.reason ?? 'Hook denied' };
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
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.permissionDeniedHooks, fn);
          };
          this.onPermissionDenied(fn);
          break;
        }
        case 'userPromptSubmit': {
          const fn: UserPromptHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'userPromptSubmit', turnCount: ctx.turnCount, prompt: ctx.prompt });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.userPromptSubmitHooks, fn);
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
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.taskCreatedHooks, fn);
          };
          this.onTaskCreated(fn);
          break;
        }
        case 'taskCompleted': {
          const fn: TaskCompletedHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'taskCompleted', taskId: ctx.taskId, subject: ctx.subject, status: ctx.status });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.taskCompletedHooks, fn);
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
            const runner = () => this.runHookByType(hookConfig, cwd, eventData, ctx.abortSignal);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData, ctx.abortSignal); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.postToolUseFailureHooks, fn);
          };
          this.onPostToolUseFailure(fn);
          break;
        }
        case 'notification': {
          const fn: NotificationHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('notification', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'notification', message: ctx.message, title: ctx.title });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.notificationHooks, fn);
          };
          this.onNotification(fn);
          break;
        }
        case 'setup': {
          const fn: SessionHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'setup', sessionId: ctx.sessionId, cwd: ctx.cwd });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.setupHooks, fn);
          };
          this.onSetup(fn);
          break;
        }
        case 'configChange': {
          const fn: ConfigChangeHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('configChange', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'configChange', source: ctx.source, filePath: ctx.filePath });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.configChangeHooks, fn);
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
            const eventData = serializeHookEvent({ event: 'instructionsLoaded', filePath: ctx.filePath, loadReason: ctx.loadReason });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.instructionsLoadedHooks, fn);
          };
          this.onInstructionsLoaded(fn);
          break;
        }
        case 'worktreeCreate': {
          const fn: WorktreeHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'worktreeCreate', name: ctx.name, worktreePath: ctx.worktreePath });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.worktreeCreateHooks, fn);
          };
          this.onWorktreeCreate(fn);
          break;
        }
        case 'worktreeRemove': {
          const fn: WorktreeHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'worktreeRemove', name: ctx.name, worktreePath: ctx.worktreePath });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.worktreeRemoveHooks, fn);
          };
          this.onWorktreeRemove(fn);
          break;
        }
        case 'cwdChanged': {
          const fn: CwdChangedHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'cwdChanged', oldCwd: ctx.oldCwd, newCwd: ctx.newCwd });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.cwdChangedHooks, fn);
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
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.fileChangedHooks, fn);
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
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.elicitationHooks, fn);
            return parseElicitationResult(result);
          };
          this.onElicitation(fn);
          break;
        }
        case 'elicitationResult': {
          const fn: ElicitationHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('elicitationResult', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'elicitationResult', mcpServerName: ctx.mcpServerName, message: ctx.message });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.elicitationResultHooks, fn);
            return parseElicitationResult(result);
          };
          this.onElicitationResult(fn);
          break;
        }
        case 'stopFailure': {
          const fn: StopFailureHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'stopFailure', error: ctx.error, hookIndex: ctx.hookIndex });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { this.fireAsyncHook(hookConfig, cwd, eventData); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.stopFailureHooks, fn);
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
  onPostToolUseFailure(fn: PostToolUseHookFn): void { this.postToolUseFailureHooks.push(fn); }
  onNotification(fn: NotificationHookFn): void { this.notificationHooks.push(fn); }
  onSetup(fn: SessionHookFn): void { this.setupHooks.push(fn); }
  onConfigChange(fn: ConfigChangeHookFn): void { this.configChangeHooks.push(fn); }
  onInstructionsLoaded(fn: InstructionsHookFn): void { this.instructionsLoadedHooks.push(fn); }
  onWorktreeCreate(fn: WorktreeHookFn): void { this.worktreeCreateHooks.push(fn); }
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
      let preventContinuation = false;
      let stopReason: string | undefined;
      try {
        for (let i = 0; i < this.stopHooks.length; i++) {
          try {
            const r = await this.stopHooks[i](ctx);
            if (r) {
              if (r.blockingError) blockingErrors.push(r.blockingError);
              if (r.preventContinuation) { preventContinuation = true; if (r.stopReason) stopReason = r.stopReason; }
            }
          } catch (err) {
            coreLog('hooks', 'Hook error', { error: String(err) });
            await this.executeStopFailure({ error: String(err), hookIndex: i });
          }
        }
      } finally {
        this._stopHookActive = false;
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
      let additionalContext: string | undefined;
      let updatedToolOutput: string | undefined;
      let blockingError: string | undefined;
      let preventContinuation = false;
      let stopReason: string | undefined;
      for (const hook of this.postToolUseHooks) {
        try {
          const result = await hook(ctx);
          if (result?.additionalContext) additionalContext = result.additionalContext;
          if (result?.updatedToolOutput) updatedToolOutput = result.updatedToolOutput;
          if (result?.blockingError) blockingError = result.blockingError;
          if (result?.preventContinuation) { preventContinuation = true; if (result.stopReason) stopReason = result.stopReason; }
        } catch (err) {
          coreLog('hooks', 'Hook error', { error: String(err) });
        }
      }
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
    return this.withConcurrencyGate('sessionStart', async (): Promise<{ initialUserMessage?: string; watchPaths?: string[] } | void> => {
      let initialUserMessage: string | undefined;
      const watchPaths: string[] = [];
      for (const hook of this.sessionStartHooks) {
        try {
          const result = await hook(ctx);
          if (result && result.initialUserMessage) initialUserMessage = result.initialUserMessage;
          if (result && result.watchPaths?.length) watchPaths.push(...result.watchPaths);
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      if (initialUserMessage || watchPaths.length > 0) return { initialUserMessage, watchPaths: watchPaths.length > 0 ? watchPaths : undefined };
    });
  }

  async executeSessionEnd(ctx: SessionContext): Promise<void> {
    return this.withConcurrencyGate('sessionEnd', async () => {
      for (const hook of this.sessionEndHooks) {
        try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
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

  async executePermissionRequest(ctx: PermissionContext): Promise<{ action?: 'allow' | 'deny'; reason?: string } | void> {
    return this.withConcurrencyGate('permissionRequest', async (): Promise<{ action?: 'allow' | 'deny'; reason?: string } | void> => {
      for (const hook of this.permissionRequestHooks) {
        try {
          const result = await hook(ctx);
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
      let additionalContext: string | undefined;
      let blockingError: string | undefined;
      let preventContinuation = false;
      let stopReason: string | undefined;
      for (const hook of this.userPromptSubmitHooks) {
        try {
          const result = await hook(ctx);
          if (result?.additionalContext) additionalContext = result.additionalContext;
          if (result?.blockingError) blockingError = result.blockingError;
          if (result?.preventContinuation) { preventContinuation = true; if (result.stopReason) stopReason = result.stopReason; }
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      if (additionalContext || blockingError || preventContinuation) {
        return { additionalContext, blockingError, preventContinuation: preventContinuation || undefined, stopReason };
      }
    });
  }

  async executeTaskCreated(ctx: TaskEventContext): Promise<void> {
    return this.withConcurrencyGate('taskCreated', async () => {
      for (const hook of this.taskCreatedHooks) {
        try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
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
        try {
          const r = await hook(ctx);
          if (r?.blockingError) blockingErrors.push(r.blockingError);
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      if (blockingErrors.length > 0) return { blockingErrors };
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

  async executeWorktreeCreate(ctx: WorktreeContext): Promise<void> {
    return this.withConcurrencyGate('worktreeCreate', async () => {
      for (const hook of this.worktreeCreateHooks) {
        try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
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
        } else if (decision.action === 'allow') {
          // An explicit "allow" can loosen a prior "modify"; the hold-over
          // check at the end guards against user-tier loosening.
          accumulated = { action: 'allow', additionalContext: decision.additionalContext ?? accumulated.additionalContext };
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
  ): Promise<string> {
    switch (config.type) {
      case 'command': {
        // Claude Code hook contract: config.timeout is in SECONDS. Route through
        // resolveHookTimeoutMs so it is clamped to [100ms, MAX_HOOK_TIMEOUT_MS].
        const timeout = resolveHookTimeoutMs(config.timeout, DEFAULT_HOOK_TIMEOUT_MS);
        const hookEnv = this.buildHookEnv(
          'command',
          eventData ? (() => { try { return JSON.parse(eventData).toolName; } catch { return undefined; } })() : undefined,
        );
        try {
          const { exitCode, stdout, stderr } = await runCommandHookAsync(config.command, cwd, hookEnv, timeout, eventData, config.args, signal, config.shell);
          // Claude Code exit-code contract:
          //   0           = success — parse stdout for a decision
          //   2           = BLOCKING error — reason from stderr → DENY
          //   other != 0  = NON-blocking error — surface stderr, do NOT block the tool
          if (exitCode === 2) {
            if (config.asyncRewake) {
              this.emit('hookRewake', { event: 'command', message: (stderr || stdout).trim() || 'Background hook completed with exit code 2' });
            }
            return `DENY:${(stderr || stdout || 'Hook blocked (exit 2)').trim()}`;
          }
          if (exitCode !== 0) {
            const msg = (stderr || stdout).trim();
            return msg ? `SYSTEM:${msg}` : '';
          }
          const trimmed = stdout.trim();
          try {
            const parsed: HookDecisionOutput = unwrapHookSpecificOutput(JSON.parse(trimmed));
            if (parsed.decision === 'block' || parsed.decision === 'deny') return `DENY:${parsed.reason ?? 'Hook blocked'}`;
            if (parsed.decision === 'ask' || parsed.permissionDecision === 'ask') return `ASK:${parsed.reason ?? ''}`;
            if (parsed.continue === false) return `DENY:${parsed.stopReason ?? 'Hook requested stop'}`;
            if (parsed.updatedInput) return `MODIFY:${JSON.stringify(parsed.updatedInput)}`;
            if (parsed.additionalContext) return `MODIFY:${JSON.stringify({ additionalContext: parsed.additionalContext })}`;
            if (parsed.systemMessage) return `SYSTEM:${parsed.systemMessage}`;
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
          coreLog('hooks', `Command hook error: ${err}`);
          // Fail-closed: a guardrail hook that times out or errors must DENY,
          // not silently allow the action through.
          if ((config as CommandHookConfig).onFailure === 'block') {
            return `DENY:Hook execution failed (fail-closed): ${err instanceof Error ? err.message : String(err)}`;
          }
          return '';
        }
      }
      case 'prompt':
        return this.runPromptHook(config, eventData, signal);
      case 'http':
        return this.runHttpHook(config, eventData, signal);
      case 'agent':
        return this.runAgentHook(config, eventData, signal);
      default:
        return '';
    }
  }

  private async runPromptHook(config: PromptHookConfig, eventData: string, signal?: AbortSignal): Promise<string> {
    const r = await this.runEngineHook('prompt', config.prompt, 'Event data', config.model, config.timeout, 30_000, eventData, signal);
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
    if (model) providerConfig.model = model;

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
    });

    // Prepend the {ok, reason} verification contract so an AI-driven hook returns
    // a parseable decision rather than free prose (Claude Code's hookResponseSchema).
    const fullPrompt = `${HOOK_VERIFICATION_CONTRACT}\n\n${promptText}\n\n${dataLabel}:\n${eventData}`;
    let result = '';
    let error: string | undefined;
    try {
      for await (const event of engine.submit(fullPrompt)) {
        if (abort.aborted()) break;
        if (event.type === 'text_delta' && event.text) result += event.text;
        if (event.type === 'error') { result += event.error ?? ''; break; }
      }
    } catch (err) {
      error = String(err);
      coreLog('hooks', `${kind} hook error: ${err}`);
    } finally {
      abort.cleanup();
      engine.shutdown?.();
    }
    if (abort.timedOut()) coreLog('hooks', `${kind} hook hard-timeout after ${timeoutMs}ms`);
    // Structured verification result → block (ok:false) or allow (ok:true).
    const verification = parseHookVerification(result);
    if (verification) {
      return {
        output: verification.ok ? '' : `DENY:Hook condition not met: ${verification.reason ?? 'condition not met'}`,
        error,
      };
    }
    return { output: sanitizeHookOutput(result), error };
  }

  private async runHttpHook(config: HttpHookConfig, eventData: string, signal?: AbortSignal): Promise<string> {
    const http = require('http');
    const https = require('https');
    // Fail-closed: an HTTP hook that times out or errors DENIES when onFailure:block.
    const onFail = () => config.onFailure === 'block' ? 'DENY:HTTP hook failed (fail-closed)' : '';

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
      coreLog('hooks', `HTTP hook blocked (SSRF): ${safety.error}`);
      return onFail();
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
    const timeoutMs = resolveHookTimeoutMs(config.timeout, 10_000);
    const abort = combineHookAbort(signal, timeoutMs);
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
          if (status < 200 || status >= 300) {
            coreLog('hooks', `HTTP hook non-2xx (${status})`);
            done(onFail());
            return;
          }
          done(sanitizeHookOutput(body));
        });
        res.on('error', () => done(onFail()));
      });
      req.on('error', () => done(onFail()));
      req.write(eventData);
      req.end();
    });
  }

  private async runAgentHook(config: AgentHookConfig, eventData: string, signal?: AbortSignal): Promise<string> {
    const r = await this.runEngineHook('agent', config.prompt, 'Context', config.model, config.timeout, 60_000, eventData, signal);
    return r.output;
  }

  private removeHookOnce<T>(hookArray: T[], hookFn: T): void {
    const idx = hookArray.indexOf(hookFn);
    if (idx >= 0) hookArray.splice(idx, 1);
  }
}

// ---------------------------------------------------------------------------
// Pattern matching utility
// ---------------------------------------------------------------------------

function matchesPattern(pattern: string, value: string): boolean {
  if (!pattern || pattern === '*') return true;
  if (pattern.includes('|')) return pattern.split('|').some(p => matchesPattern(p, value));
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

