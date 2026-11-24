// Split from hooks.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import { plural } from './utils/string';
import * as os from 'os';
import { wildcardMatch } from './permissions';
import { formatHookStoppedContinuation, stopHookInfoLabel } from './hooks-stop-hook-summary';
import type { StopHookSummary } from './hooks-stop-hook-summary';

export function formatStopHookSummaryMessage(summary: StopHookSummary): string {
  const slow = summary.totalDurationMs >= HOOK_TIMING_DISPLAY_THRESHOLD_MS;
  if (!summary.hasOutput && !slow) return '';
  const lines: string[] = [`Ran ${summary.count} ${plural(summary.count, 'stop hook')} in ${summary.totalDurationMs}ms`];
  summary.hookInfos.forEach((info, i) => {
    // Attribute the row to its backing config when known (the reference's StopHookInfo
    // command / promptText) so a slow/failing hook is named, not just numbered.
    const id = info.command ?? info.promptText;
    const label = id ? ` [${stopHookInfoLabel(id)}]` : '';
    // Only surface a per-hook row when it is interesting: it errored, or it was
    // individually slow. A fast, clean hook adds no noise.
    if (info.error) {
      lines.push(`  • hook ${i + 1}${label} (${info.durationMs}ms): ${info.error}`);
    } else if (info.durationMs >= HOOK_TIMING_DISPLAY_THRESHOLD_MS) {
      lines.push(`  • hook ${i + 1}${label}: ${info.durationMs}ms`);
    }
  });
  if (summary.preventContinuation) {
    lines.push(`Continuation prevented: ${formatHookStoppedContinuation(summary.stopReason)}`);
  }
  return lines.join('\n');
}

/**
 * Minimum wall-clock duration (ms) a Stop-hook chain must exceed before its
 * summary is worth DISPLAYING when there were no errors / no prevented
 * continuation (the reference's HOOK_TIMING_DISPLAY_THRESHOLD_MS): a fast no-op
 * Stop hook does not deserve a summary row, but a slow one (or one with errors)
 * does. Consumed by the summary surface so the display threshold matches parity.
 */
export const HOOK_TIMING_DISPLAY_THRESHOLD_MS = 500;

/**
 * A settings/hooks/config file that failed to parse while loading hooks. Surfaced
 * (via {@link HookManager.getConfigErrors} + the `configError` event + `/hooks`)
 * so a malformed file — which silently disables EVERY hook it would contribute —
 * is visible to the user instead of only hitting the debug log (the reference's
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
 * matches the reference's addFunctionHook default. A slow/stuck in-process
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
 * with the reference threading `abortController.signal` into its permission-hook loop.
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

export const MAX_HOOK_TIMEOUT_MS = 600_000; // 10 min — matches the reference's hook ceiling

export const DEFAULT_HOOK_TIMEOUT_MS = 60_000; // CC default when a hook sets no timeout

/** Default lifetime budget for an async ('background') hook when it sets no
 *  `asyncTimeout` — 15s, matching the reference's dedicated background-hook deadline.
 *  A background hook must NOT inherit the long synchronous command/HTTP default
 *  (60s / 10min): it runs detached from the turn, so it is bounded on a short,
 *  distinct clock and aborted when exceeded (child killed / request aborted). In
 *  milliseconds (clamped via validateHookTimeout). */
export const DEFAULT_ASYNC_HOOK_TIMEOUT_MS = 15_000;

export const MAX_HOOKS_PER_EVENT = 10;

/** Default execution budget for SessionEnd hooks (the reference's
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
 *  (the reference's MAX_AGENT_TURNS). Passed to the hook sub-engine. */
export const MAX_AGENT_TURNS = 50;

/** Cap on hook-contributed context injected into the turn (the reference's
 *  MAX_HOOK_OUTPUT_LENGTH). A UserPromptSubmit hook that emits a huge blob is
 *  truncated with a marker so it cannot blow out the context window. */
export const MAX_HOOK_OUTPUT_LENGTH = 10_000;

/** Truncate hook-contributed output to MAX_HOOK_OUTPUT_LENGTH, appending the
 *  the reference-parity marker when it overflows. Shared so no site re-implements it. */
export function truncateHookOutput(content: string): string {
  if (content.length > MAX_HOOK_OUTPUT_LENGTH) {
    return `${content.substring(0, MAX_HOOK_OUTPUT_LENGTH)}… [output truncated - exceeded ${MAX_HOOK_OUTPUT_LENGTH} characters]`;
  }
  return content;
}

/**
 * Head-preserving bounded append for a command hook's captured stream. Appends
 * `chunk` to `current` only up to `maxBytes` total (measured in UTF-8 bytes);
 * once the cap is reached nothing more is retained. Returns the new text and
 * whether this append hit/exceeded the cap (so the caller can emit a single
 * truncation notice at close). Canonical — reused for both stdout and stderr so
 * the bounding logic is never re-implemented per stream.
 */
export function boundedHookAppend(
  current: string,
  chunk: string,
  maxBytes: number,
  currentBytes?: number,
): { text: string; truncated: boolean; bytes: number } {
  // Accept the caller's running byte count to avoid re-measuring the WHOLE
  // accumulated stream on every chunk (the old per-chunk Buffer.byteLength(current)
  // made streaming a large hook output O(N·K) / quadratic). `bytes` is returned so
  // the caller can thread it forward; when omitted we fall back to measuring
  // `current` (backward-compatible for one-shot callers).
  const curBytes = currentBytes ?? Buffer.byteLength(current, 'utf-8');
  if (curBytes >= maxBytes) return { text: current, truncated: true, bytes: curBytes };
  const remaining = maxBytes - curBytes;
  const chunkBytes = Buffer.byteLength(chunk, 'utf-8');
  if (chunkBytes <= remaining) {
    return { text: current + chunk, truncated: false, bytes: curBytes + chunkBytes };
  }
  // Keep exactly `remaining` bytes of the chunk (byte-accurate; a split
  // multibyte sequence degrades to the Unicode replacement char — acceptable at
  // a truncation boundary).
  const slice = Buffer.from(chunk, 'utf-8').subarray(0, remaining).toString('utf-8');
  return { text: current + slice, truncated: true, bytes: curBytes + Buffer.byteLength(slice, 'utf-8') };
}

export const DEVICE_WRITE_RE = />\s*\/dev\/[a-z]/;

export const SYSTEM_CHMOD_RE = /chmod\s+777\s+\/(etc|usr|bin|sbin)/;

export const MAX_HOOK_COMMAND_LENGTH = 4096;

export function validateHookTimeout(timeoutMs: number): number {
  return Math.max(100, Math.min(timeoutMs, MAX_HOOK_TIMEOUT_MS));
}

/**
 * Default interpreter for command hooks when none is configured — matches
 * the reference's DEFAULT_HOOK_SHELL. A hook may override it per-entry via the
 * `shell` config field (bash | sh | powershell | pwsh).
 */
export const DEFAULT_HOOK_SHELL = 'bash';

/**
 * POSIX single-quote a string so it embeds safely as one argument inside a
 * `sh -c` / `bash -c` command line (any embedded single quote is closed,
 * escaped, and reopened).
 */
export function posixSingleQuote(s: string): string {
  return `'` + s.split(`'`).join(`'\\''`) + `'`;
}

/**
 * Wrap a hook command with an outer shell prefix (the reference's
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
 * Validate a hook's working directory before spawning. A stale/removed cwd
 * (e.g. a deleted worktree the session was launched from) makes `spawn` throw
 * ENOENT, which for a guardrail hook is indistinguishable from a real failure.
 * the reference pre-checks the hook cwd and falls back; this mirrors that by
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
 * the reference's hook `timeout` field is in SECONDS for EVERY hook type
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
 * Mirrors the reference's createCombinedAbortSignal: the returned signal aborts
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
 * Tools a hook-spawned agent/prompt engine must NOT get — the reference filters a
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
 * Three-state HTTP-hook URL allowlist check (the reference semantics):
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
 * Mirrors the reference's `ssrfError` code so the block is machine-identifiable in
 * logs and surfaced messages regardless of the human-readable reason text.
 */
export const ERR_HTTP_HOOK_BLOCKED_ADDRESS = 'ERR_HTTP_HOOK_BLOCKED_ADDRESS';

/**
 * Strip CR, LF, and NUL bytes from an HTTP-hook header value to prevent HTTP
 * header (CRLF) injection via an env-var value or a hook-configured header
 * template (the reference's sanitizeHeaderValue). A malicious env var such as
 * `token\r\nX-Evil: 1` would otherwise smuggle a second header into the request.
 * Implemented with split/join (not a regex) so the control-char class does not
 * trip the security lint's no-control-regex rule.
 */
export function sanitizeHeaderValue(value: string): string {
  return value.split('\r').join('').split('\n').join('').split('\u0000').join('');
}
