// Split from hooks.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { journalPathFor } from './reference-journal';
import type { Message, AssistantMessage, ToolMessage } from './types';
import { escapeHarnessMarkerTags } from './tools/bash-security';

/**
 * Injectable journal-path seam for agent-hook transcript scoping. Production
 * delegates to {@link journalPathFor}; tests inject a throwing resolver to hit
 * the resolveHookTranscriptPath catch (malformed cwd/session → no scoped Read).
 */
export const hookJournalHooks: {
  journalPathFor: (cwd: string, sessionId: string) => string;
} = { journalPathFor: (cwd, sessionId) => journalPathFor(cwd, sessionId) };

// ---------------------------------------------------------------------------
// Hook output sanitization — strip system-reminder tags to prevent injection
// ---------------------------------------------------------------------------

// Hook output and bash tool output cross the SAME model boundary, so the
// harness-marker-tag neutralization is the one canonical escapeHarnessMarkerTags
// in bash-security (single source of truth — no duplicate regex here).
export function sanitizeHookOutput(output: string): string {
  return escapeHarnessMarkerTags(output);
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
   *  break — a block→retry recursion (the reference's StopHookInput.stop_hook_active). */
  stopHookActive?: boolean;
  /** Parent cancellation signal (user Ctrl-C / turn abort). Threaded into
   *  command/http/prompt/agent hook execution so a long-running hook is torn
   *  down when the surrounding operation is cancelled, combined with the
   *  per-hook timeout. */
  abortSignal?: AbortSignal;
  /** Origin of the model-sampling call that triggered this hook (the reference's
   *  MagicDocs `querySource` discriminator). `'main'` = the top-level REPL/agent
   *  loop; `'subagent'` = a forked/nested agent's own sampling. Surfaced to
   *  pre/postSampling hooks (as the matcher field + serialized `sampling_origin`)
   *  so a hook that itself spawns a sampling call can EXCLUDE background/forked
   *  samplings and avoid re-entrant/recursive work (MagicDocs L222-224:
   *  `if (querySource !== 'repl_main_thread') return`). */
  samplingOrigin?: SamplingOrigin;
  /** The active system prompt in effect for the turn (the reference's
   *  REPLHookContext.systemPrompt). Surfaced to post-sampling / stop hooks (as the
   *  serialized `system_prompt`) so a hook can judge the model's output against what
   *  it was actually told. Populated by the engine when it builds the stop /
   *  post-sampling context; omitted otherwise. */
  systemPrompt?: string;
  /** Mutable user-facing key/value context bag (the reference's
   *  REPLHookContext.userContext). Serialized as `user_context` for post-sampling /
   *  stop hooks so a hook can read session context keys the user/UI attached. */
  userContext?: Record<string, string>;
  /** System key/value context bag (the reference's REPLHookContext.systemContext),
   *  serialized as `system_context`. Distinct from {@link userContext}: these keys
   *  are machine/session metadata rather than user-provided. */
  systemContext?: Record<string, string>;
}

/** Allowed {@link HookContext.samplingOrigin} discriminators — the matcher value
 *  set for pre/postSampling hooks (the reference's query-source boundary). */
export const SAMPLING_ORIGINS = ['main', 'subagent'] as const;

export type SamplingOrigin = (typeof SAMPLING_ORIGINS)[number];

/** Narrow an arbitrary value to a {@link SamplingOrigin}. */
export function isSamplingOrigin(v: unknown): v is SamplingOrigin {
  return typeof v === 'string' && (SAMPLING_ORIGINS as readonly string[]).includes(v);
}

/**
 * Whether the conversation is mid tool-chain — the reference's
 * `hasToolCallsInLastAssistantTurn` (MagicDocs idle-gating). Returns true when the
 * LAST assistant message issued tool calls that are NOT yet all answered by
 * subsequent tool results. A background pre/postSampling or maintenance hook reads
 * the inverse (serialized as the `is_idle` stdin field) so it runs ONLY when the
 * agent is idle — i.e. it skips while the main loop is still executing a tool
 * chain, so a file-mutating hook never races the loop's own edits. Pure; a
 * conversation with no assistant message, or whose last assistant turn made no
 * tool calls, is considered idle (no pending calls).
 */
export function hasPendingToolCalls(messages: Message[]): boolean {
  let lastAssistantIdx = -1;
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].role === 'assistant') { lastAssistantIdx = i; break; }
  }
  if (lastAssistantIdx === -1) return false;
  const calls = (messages[lastAssistantIdx] as AssistantMessage).tool_calls;
  if (!calls || calls.length === 0) return false;
  const answered = new Set<string>();
  for (let i = lastAssistantIdx + 1; i < messages.length; i++) {
    const m = messages[i];
    if (m.role === 'tool') answered.add((m as ToolMessage).tool_call_id);
  }
  return calls.some(c => !answered.has(c.id));
}

// ---------------------------------------------------------------------------
// Extended hook context types
// ---------------------------------------------------------------------------

export interface PreToolUseContext extends HookContext {
  toolName: string;
  toolInput: Record<string, unknown>;
  /** The originating user prompt for the turn that produced this tool call (the reference
   *  Code's `toolUseContext.requestPrompt`). Serialized as `request_prompt` so a
   *  PreToolUse guardrail hook can decide against the user's actual request, not
   *  just the mechanical tool input. Populated by the tool executor when known;
   *  omitted for synthetic/non-turn calls. */
  requestPrompt?: string;
}

export interface PostToolUseContext extends HookContext {
  toolName: string;
  toolInput: Record<string, unknown>;
  toolOutput: string;
  isError: boolean;
  /** Elapsed time for the tool execution in milliseconds. */
  durationMs?: number;
  /** The tool's error text (the reference PostToolUseFailure `error`). Lets a
   *  failure hook react to WHAT failed, not just that something did. Serialized as
   *  `error`. Optional — populated for the postToolUseFailure event. */
  error?: string;
  /** Whether the failure was a user/turn interrupt rather than an ordinary error
   *  (the reference `is_interrupt`): a failure hook can skip remediation when the
   *  user themselves cancelled. Serialized as `is_interrupt`. */
  isInterrupt?: boolean;
  /** Correlation id of the failed tool call (the reference `tool_use_id`) so a
   *  failure hook can tie back to the exact call. Serialized as `tool_use_id`. */
  toolUseId?: string;
}

export interface PreSamplingContext extends HookContext {
  apiMessages: Message[];
}

export type HookDecision =
  // `permissionBypass` marks an EXPLICIT PreToolUse approval (the reference's
  // `permissionDecision: 'allow'` / `decision: 'approve'`) that PRE-APPROVES the call
  // so the consumer (tool-executor) bypasses a would-be interactive permission prompt.
  // A bare `allow` (the default "no objection" verdict) leaves it unset and does NOT
  // bypass — deny rules / the hardline check still resolve first, so an explicit
  // approval can never override a deny.
  | { action: 'allow'; additionalContext?: string; permissionBypass?: boolean }
  | { action: 'deny'; reason: string; additionalContext?: string }
  // the reference's PreToolUse `permissionDecision: 'ask'` — the hook does not
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
 * (halt the agent loop after the tool ran), matching the reference's
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
 * Allowed SessionStart `source` discriminators (the reference's
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
 * Session-exit-reason classification for the SessionEnd hook (the reference's
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
 * Setup hook `trigger` discriminator (the reference's SetupHookInputSchema
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
  /** Why the SessionStart hook is firing — the reference's `source` discriminator
   *  (startup | resume | clear | compact). A `matcher` on a sessionStart hook is
   *  tested against this (HOOK_MATCHER_FIELD.sessionStart = 'source') so a hook
   *  can scope to e.g. only `resume`. */
  source?: string;
  /** Why the SessionEnd hook is firing — the reference's `reason` discriminator
   *  (clear | logout | prompt_input_exit | other). A `matcher` on a sessionEnd
   *  hook is tested against this (HOOK_MATCHER_FIELD.sessionEnd = 'reason'). */
  reason?: string;
  /** Why the Setup hook is firing — the reference's `trigger` discriminator
   *  (init | maintenance). A `matcher` on a setup hook is tested against this
   *  (HOOK_MATCHER_FIELD.setup = 'trigger'). */
  trigger?: string;
  /** Active model id, forwarded to SessionStart/Setup hooks in the stdin payload
   *  (the reference's SessionStart `model`). */
  model?: string;
  /** Agent type the session is running as, forwarded to SessionStart hooks as
   *  `agent_type` (the reference's SessionStart `agent_type`). */
  agentType?: string;
  /** Cooperative-cancellation deadline for the session lifecycle hooks. the reference
   *  Code threads `AbortSignal.timeout(sessionEndTimeoutMs)` into
   *  executeSessionEndHooks (commands/clear/conversation.ts) so a slow in-process
   *  SessionStart/SessionEnd callback can observe the deadline and bail instead of
   *  blocking shutdown/clear. Threaded to in-process callbacks via {@link raceAbort}
   *  and to spawned command hooks as the child-kill signal. Optional: when absent,
   *  executeSessionEnd synthesizes one from its own teardown budget. */
  signal?: AbortSignal;
  /** Force SessionStart/Setup hooks that self-declare `async:true` to run
   *  SYNCHRONOUSLY (blocking) for this invocation instead of being backgrounded
   *  (the reference's `forceSyncExecution` option on executeSessionStart/
   *  executeSetupHooks). Needed when the caller must have every hook's
   *  `initialUserMessage` / `additionalContexts` available before the first turn
   *  is assembled (e.g. a `source:'resume'` restore or a print-mode run), where
   *  fire-and-forget backgrounding would otherwise drop a late-arriving message.
   *  Only sessionStart/setup honor it; all other events ignore it. Default: the
   *  hook's own `async` flag governs (unchanged behavior). */
  forceSyncExecution?: boolean;
}

export interface SubagentContext {
  /** The INVOKING (parent) engine's session id — lets a hook correlate the
   *  subagent boundary back to its real parent across concurrent/nested runs
   *  (the reference parity: SubagentContext.parentSessionId). */
  parentSessionId: string;
  subagentSessionId: string;
  prompt: string;
  mode?: string;
  /** Parent cancellation signal (user Ctrl-C / turn abort). When aborted, the
   *  SubagentStop hook chain stops immediately (the reference's abort-signal
   *  cancellation of in-flight Stop/SubagentStop hook chains). Optional so
   *  non-cancellable call sites may omit it. */
  abortSignal?: AbortSignal;
  /** Whether this boundary is a fresh `spawn` or a `resume` of an existing agent
   *  session (the reference parity: invocationKind). Absent in bare/test contexts. */
  invocationKind?: 'spawn' | 'resume';
  /** Whether the spawned agent is a BUILT-IN identity (a bare mode like
   *  explore/general/coordinator/fork, or a bundled roster agent) vs a USER-DEFINED
   *  custom project/plugin agent (the reference parity: agentContext `isBuiltIn`). */
  isBuiltIn?: boolean;
  /** Privacy-safe agent-type label for analytics: the real type for built-in agents,
   *  collapsed to `'user-defined'` for custom agents so a private repo's agent names
   *  never leak into telemetry (the reference parity: agentContext name collapse). */
  analyticsAgentType?: string;
  /** The INVOKING agent's identity — the immediate parent subagent id when nested,
   *  else the invoking session id. Roots this invocation's edge in the cross-agent
   *  attribution tree (the reference parity: invokingRequestId). */
  invokingRequestId?: string;
  /** Fresh per-invocation correlation id for the query-tracking lineage so nested
   *  fork/subagent chains can be correlated (the reference parity: chainId). */
  chainId?: string;
  /** This agent's depth in the spawn tree (parent.depth + 1; 0 = top level). Paired
   *  with chainId for lineage correlation (the reference parity: query-tracking depth). */
  depth?: number;
  /** Discriminated-union agent-context kind: a named spawn joined to a team is a
   *  `'teammate'`; every other spawn is a plain `'subagent'` (the reference parity:
   *  subagent-vs-teammate agent-context discriminator). */
  agentContextKind?: 'subagent' | 'teammate';
}

export interface CompactContext {
  messageCount: number;
  tokenEstimate: number;
  /** Whether compaction was auto-triggered (token pressure) or user-requested
   *  (`/compact`). Surfaced to PreCompact hooks (the reference parity). */
  trigger?: 'auto' | 'manual';
  /** The user-supplied custom compaction instructions a PreCompact hook may
   *  read and extend/override via `newCustomInstructions`. */
  customInstructions?: string | null;
  /** The compaction summary text produced by the compaction run. Supplied to
   *  PostCompact hooks (the reference's `executePostCompactHooks({ trigger,
   *  compactSummary })`) so a post-compaction automation can read, log, or
   *  annotate the generated summary. Absent on PreCompact (no summary yet). */
  compactSummary?: string;
  /** The compaction's cancellation signal. the reference passes
   *  `context.abortController.signal` into executePreCompactHooks
   *  (commands/compact/compact.ts) so a PreCompact hook subprocess is killed the
   *  instant the user interrupts the compaction, instead of lingering until its own
   *  timeout. Threaded to in-process callbacks via {@link raceAbort} and into the
   *  spawned command/prompt/http/agent hook as its abort signal. */
  signal?: AbortSignal;
}
