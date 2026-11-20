import type { EngineEvent, TaskUsageSnapshot } from '../types';

/**
 * AMI's synthetic final-output tool. Its calls are internal result-emission, not
 * work worth surfacing, so the live-roster activity preview omits them (parity:
 * the reference special-cases `SYNTHETIC_OUTPUT_TOOL_NAME` out of its
 * recentActivities preview while still counting the tool use).
 */
const SYNTHETIC_OUTPUT_TOOL_NAME = 'structured_output';

/**
 * Map an AMI tool name to a human-readable action verb for the live-roster activity
 * line. Parity: the reference's `TOOL_VERBS` table (bridge/sessionRunner.ts) keyed to
 * AMI's snake_case tool names (vs the reference's PascalCase). A tool with no mapped verb
 * falls back to its own name (parity: the reference's `TOOL_VERBS[name] ?? name`).
 */
const TOOL_VERBS: Record<string, string> = {
  file_read: 'Reading',
  read_memory: 'Reading',
  file_edit: 'Editing',
  notebook_edit: 'Editing notebook',
  write_memory: 'Writing',
  bash: 'Running',
  grep: 'Searching',
  glob: 'Searching',
  web_search: 'Searching',
  scout: 'Searching',
  find_references: 'Searching',
  search_symbols: 'Searching',
  web_fetch: 'Fetching',
  agentic_fetch: 'Fetching',
  list_dir: 'Listing',
  task: 'Running task',
  lsp: 'LSP',
  diagnostics: 'LSP',
};

/** The action verb for a tool, falling back to the bare tool name when unmapped. */
export function toolVerb(toolName: string): string {
  return TOOL_VERBS[toolName] ?? toolName;
}

/**
 * Build a human-readable "verb + target" activity line for a sub-agent's tool call
 * so the live roster reads "Running npm test" / "Reading src/x.ts" / "Searching TODO"
 * instead of the bare tool name. Parity: the reference's `toolSummary(name, input)`
 * (bridge/sessionRunner.ts) — a verb from {@link TOOL_VERBS} plus the most salient
 * target pulled from the input via {@link summarizeToolInput}. Returns just the verb
 * when the input carries no salient target.
 */
export function toolActivitySummary(toolName: string, input: unknown): string {
  const verb = toolVerb(toolName);
  const target = summarizeToolInput(toolName, input);
  return target ? `${verb} ${target}` : verb;
}

/**
 * Build a compact, human-readable argument summary for a sub-agent's tool call so
 * the live roster shows "bash npm test" or "file_read src/x.ts" instead of just
 * the tool name. Picks the most salient field per tool and truncates. Returns ''
 * when there's nothing useful to show (keeps the bare tool name).
 */
export function summarizeToolInput(toolName: string, input: unknown): string {
  if (!input || typeof input !== 'object') return '';
  const o = input as Record<string, unknown>;
  const pick = (...keys: string[]): string | undefined => {
    for (const k of keys) {
      const v = o[k];
      if (typeof v === 'string' && v.trim()) return v.trim();
    }
    return undefined;
  };
  // Most salient argument per tool family, with sensible fallbacks.
  const raw =
    pick('command') ??
    pick('file_path', 'path', 'notebook_path') ??
    pick('pattern', 'query', 'regex') ??
    pick('url') ??
    pick('prompt', 'subject', 'description') ??
    undefined;
  if (!raw) return '';
  const oneLine = raw.replace(/\s+/g, ' ').trim();
  return oneLine.length > 80 ? `${oneLine.slice(0, 77)}...` : oneLine;
}

/**
 * Max characters of relayed child output (streamed text / thinking, live tool
 * progress, error text) surfaced on the parent roster in a single progress tick.
 * Parity: the reference bounds relayed preview text (truncateForPreview) BEFORE it
 * crosses the agent boundary so a chatty or run-away child can't flood the
 * parent's roster/stream with an unbounded blob.
 */
export const MAX_RELAY_PREVIEW_CHARS = 500;

/**
 * Clamp a relayed preview string to {@link MAX_RELAY_PREVIEW_CHARS}, appending a
 * single ellipsis when truncated. Internal spacing is otherwise preserved (the
 * caller decides whether to collapse whitespace first).
 */
export function clampRelayPreview(text: string, max = MAX_RELAY_PREVIEW_CHARS): string {
  return text.length > max ? `${text.slice(0, max)}…` : text;
}

/**
 * First non-empty line of a failed tool's output, bounded to a short slice for the
 * roster's error detail. Parity: the reference appends a bounded failure reason to a
 * sub-agent tool-error row (not just a ✗ glyph) so the roster conveys WHY it failed.
 * Returns '' when the output has no printable content.
 */
export function summarizeErrorOutput(output: string, max = 120): string {
  const firstLine = output.split('\n').map((l) => l.trim()).find((l) => l.length > 0) ?? '';
  return firstLine.length > max ? `${firstLine.slice(0, max - 1)}…` : firstLine;
}

/**
 * Pure mapper: translate a child sub-engine's EngineEvent into a parent-facing
 * `subagent_progress` event tagged with the child's node id + depth, or null for
 * events that should not be relayed (everything the live roster doesn't render).
 * Kept pure + separate from task.ts so the relay policy is fully unit-testable.
 *
 * PARITY (additive): the reference's Task tool surfaces a sub-agent's failures,
 * retries, and in-flight tool progress to the parent as they happen, not only in
 * the final summary. This mapper relays:
 *  - `error` live as an `error` activity (bounded) so a failing child isn't silent;
 *  - `retry_attempt` / `error_classified` as a `retry` activity so a recovering
 *    child reflects liveness instead of appearing frozen;
 *  - `tool_use_progress` as a bounded `text` activity so a long-running child tool
 *    (e.g. a slow `bash`) surfaces live output instead of going dark;
 *  - a failed `tool_use_result` with a bounded failure reason, not just a ✗ glyph;
 *  - `user_question` as a `waiting` activity (approval affordance) so a child
 *    BLOCKED on a permission `ask` / `AskUserQuestion` / plan-mode consent reads as
 *    "awaiting approval" on the roster instead of freezing on its last row — parity
 *    with the reference's swarmWorkerHandler `pendingWorkerRequest` "waiting for leader
 *    approval" indicator;
 * and drops whitespace-only deltas / empty progress so the roster never flickers
 * blank lines, with ALL relayed free text clamped via {@link clampRelayPreview}.
 */
export function mapChildToSubagentEvent(
  child: EngineEvent,
  nodeId: string,
  depth: number,
): EngineEvent | null {
  switch (child.type) {
    case 'text_delta':
      // PARITY: drop whitespace-only deltas (blank roster flicker) but relay the
      // text with internal spacing preserved, bounded so a run-away child can't
      // flood the parent roster in one tick (clampRelayPreview).
      return child.text && child.text.trim()
        ? { type: 'subagent_progress', nodeId, depth, activity: 'text', detail: clampRelayPreview(child.text) }
        : null;
    case 'thinking_delta':
      return child.text && child.text.trim()
        ? { type: 'subagent_progress', nodeId, depth, activity: 'thinking', detail: clampRelayPreview(child.text) }
        : null;
    case 'tool_use_progress':
      // PARITY (additive): surface a long-running child tool's live output on the
      // parent roster instead of going dark until completion. Relayed as a bounded
      // `text` activity — a progress tick is streamed output of an ALREADY-counted
      // tool, not a new tool start, so mapping it to `tool` would double-count it in
      // the roster's tool tally. Empty chunks drop like blank text/thinking deltas.
      return child.data && child.data.trim()
        ? { type: 'subagent_progress', nodeId, depth, activity: 'text', detail: clampRelayPreview(child.data) }
        : null;
    case 'tool_use_start': {
      // Omit the internal synthetic-output tool from the activity preview — it is a
      // result-emission call, not visible work (parity: the reference's recentActivities skip).
      if (child.toolName === SYNTHETIC_OUTPUT_TOOL_NAME) return null;
      // Human-readable verb + target (parity: the reference's toolSummary), e.g.
      // "Running npm test" / "Reading src/x.ts" instead of the bare tool name.
      // Carry the structured tool name (parity: the reference's progress `last_tool_name`) so the
      // roster can collapse a read/search burst without re-parsing the free-text detail.
      return {
        type: 'subagent_progress', nodeId, depth, activity: 'tool',
        detail: toolActivitySummary(child.toolName, child.input),
        lastToolName: child.toolName,
      };
    }
    case 'tool_use_result': {
      if (child.toolName === SYNTHETIC_OUTPUT_TOOL_NAME) return null;
      // Same verb as the matching start (parity: the reference's toolSummary verb) plus the
      // outcome glyph, e.g. "Running ✓" / "Searching ✗"; lastToolName keeps identity.
      // On failure, append a bounded first-line slice of the output so the roster
      // conveys WHY it failed (parity: the reference's tool-error detail), not just a ✗.
      const reason = child.isError ? summarizeErrorOutput(child.output) : '';
      return {
        type: 'subagent_progress', nodeId, depth, activity: 'tool',
        detail: `${toolVerb(child.toolName)} ${child.isError ? '✗' : '✓'}${reason ? ` — ${reason}` : ''}`,
        lastToolName: child.toolName,
      };
    }
    case 'error':
      // PARITY: surface the sub-agent's error on the live roster the moment it
      // happens, as a dedicated `error` activity (previously the parent only saw it
      // in the final summary text). Bounded so a huge stack trace can't flood the row.
      return child.error && child.error.trim()
        ? {
            type: 'subagent_progress', nodeId, depth, activity: 'error',
            detail: clampRelayPreview(`⚠ ${child.error.replace(/\s+/g, ' ').trim()}`),
          }
        : null;
    case 'retry_attempt':
      // PARITY: a transient retry is liveness, not a freeze — surface the attempt so
      // the roster shows the child is recovering (bridge/sdkProgress retry relay).
      return {
        type: 'subagent_progress', nodeId, depth, activity: 'retry',
        detail: `retry ${child.attempt}/${child.maxRetries}${child.delayMs ? ` in ${child.delayMs}ms` : ''}`,
      };
    case 'error_classified':
      // PARITY: surface a classified error's category + whether a retry follows. A
      // retrying classification reads as `retry` liveness; a terminal one as `error`.
      return {
        type: 'subagent_progress', nodeId, depth,
        activity: child.willRetry ? 'retry' : 'error',
        detail: `${child.category}${child.willRetry ? ' (retrying)' : ''}`,
      };
    case 'user_question': {
      // PARITY (additive): a child engine yields `user_question` whenever it is
      // BLOCKED on a human decision — an interactive permission `ask`, an
      // `AskUserQuestion`, or plan-mode enter/exit consent. Relay it as a dedicated
      // `waiting` activity with an "awaiting approval" affordance so the roster shows
      // the sub-agent is stalled on the user, not frozen on its last tool/text row
      // (the reference's swarmWorkerHandler `pendingWorkerRequest`). The optional
      // `toolInputSummary` (WHAT the ask is about) is woven in so the row conveys the
      // pending action, bounded via clampRelayPreview like every other relayed detail.
      const q = child.question && child.question.trim() ? child.question.trim() : 'a decision';
      const ctx = child.toolInputSummary && child.toolInputSummary.trim()
        ? ` — ${child.toolInputSummary.trim()}`
        : '';
      return {
        type: 'subagent_progress', nodeId, depth, activity: 'waiting',
        detail: clampRelayPreview(`⏳ awaiting approval: ${q}${ctx}`),
      };
    }
    default:
      return null;
  }
}

/**
 * Stamp a live cumulative usage snapshot (tokens / tool-uses / elapsed ms) onto a
 * relayed `subagent_progress` event so a roster/inspector can show a running
 * (sub)agent's spend AND wall-clock as it happens — not only in the final
 * `subagent_result`. Pure: a NEW event is returned (the input is never mutated);
 * non-progress events (and `null`) pass through untouched so the caller can pipe
 * `mapChildToSubagentEvent(...)` straight through this. Parity: the reference's
 * task_progress `usage { total_tokens, tool_uses, duration_ms }` carried live
 * (utils/task/sdkProgress.ts).
 */
export function attachSubagentUsage(
  event: EngineEvent | null,
  usage: TaskUsageSnapshot,
): EngineEvent | null {
  if (event && event.type === 'subagent_progress') {
    return { ...event, usage };
  }
  return event;
}

/* ------------------------------------------------------------------------- *
 * Sub-agent message relay (PARITY: the reference subagent messaging)
 *
 * the reference routes messages between a parent and its spawned sub-agents with an
 * addressed mailbox: messages are queued per recipient, delivered in order, and
 * a sub-agent that has finished (or was never spawned) can no longer receive mail
 * — such sends are rejected and parked as dead-letters rather than silently lost.
 * The relay below is a pure, allocation-light model of that contract so the
 * orchestration layer can wire parent<->subagent routing without re-deriving the
 * lifecycle/ordering/dead-letter rules (and so they are unit-testable in core).
 * ------------------------------------------------------------------------- */

/**
 * Lifecycle of a registered sub-agent from the relay's point of view.
 *
 * `'idle'` is a NON-terminal waiting state distinct from the terminal
 * `'completed'`/`'failed'` states: a sub-agent that finished its turn and is
 * awaiting more input is idle, NOT done and NOT errored. Parity: the reference's
 * TeamCreateTool guidance that a teammate going idle after a turn is a
 * first-class waiting state — idle teammates can still receive messages and must
 * never be treated as an error or as unavailable.
 */
export type SubagentState = 'pending' | 'idle' | 'completed' | 'failed';

/** A routed message with a monotonic sequence number fixing delivery order. */
export interface RelayMessage {
  /** Recipient node id (the sub-agent, or 'parent'). */
  readonly to: string;
  /** Sender node id. Defaults to 'parent' when a sender is not supplied. */
  readonly from: string;
  /** Message payload. */
  readonly body: string;
  /** Monotonic, relay-global sequence number (ordering across all recipients). */
  readonly seq: number;
}

/** Why a `send` could not be accepted (only present when `accepted` is false). */
export type RelayRejectReason = 'unknown-recipient' | 'recipient-finished';

/** Result of attempting to route a message to a recipient. */
export interface RelaySendResult {
  readonly accepted: boolean;
  /** Assigned sequence number when accepted, else null. */
  readonly seq: number | null;
  readonly reason?: RelayRejectReason;
}

/** Notification emitted when a sub-agent transitions to a terminal state. */
export interface RelayCompletion {
  readonly nodeId: string;
  readonly state: SubagentState;
  /** Queued-but-undelivered messages flushed to dead-letters on completion. */
  readonly dropped: readonly RelayMessage[];
  /** True only on the transition itself; re-completing a finished agent is a no-op. */
  readonly transitioned: boolean;
}

/** Notification emitted when a sub-agent enters the non-terminal `idle` state. */
export interface RelayIdle {
  readonly nodeId: string;
  /** Resulting state: `'idle'` on an accepted transition, else the unchanged state. */
  readonly state: SubagentState | 'unknown';
  /** True only on the alive→idle transition itself; re-marking idle is a no-op. */
  readonly transitioned: boolean;
}

/** Pure parent<->subagent message router. See module comment. */
export interface SubagentRelay {
  /** Register (or re-arm) a sub-agent as able to receive messages. */
  register(nodeId: string): void;
  /** True when the node is registered and has not finished (pending OR idle). */
  isAlive(nodeId: string): boolean;
  /** Current lifecycle state, or 'unknown' for an unregistered node. */
  state(nodeId: string): SubagentState | 'unknown';
  /**
   * Transition an alive (pending/idle) node into the non-terminal `idle` state.
   * Idempotent for an already-idle node; a no-op for unknown/finished nodes (idle
   * cannot resurrect a completed/failed agent). Parity: idle is a waiting state,
   * not an error — an idle teammate stays alive and can still receive messages.
   */
  markIdle(nodeId: string): RelayIdle;
  /** Route a message to `to`. Accepted for an alive (pending/idle) recipient; rejected (dead-lettered) if unknown or finished. */
  send(to: string, body: string, from?: string): RelaySendResult;
  /** FIFO-drain all pending messages for a node (empty for unknown/finished nodes). */
  deliver(nodeId: string): RelayMessage[];
  /** Count of queued-but-undelivered messages for a node. */
  pending(nodeId: string): number;
  /** Transition a node to a terminal state, flushing undelivered mail to dead-letters. */
  complete(nodeId: string, state?: 'completed' | 'failed'): RelayCompletion;
  /** All messages that could not be delivered (immutable snapshot, in arrival order). */
  deadLetters(): readonly RelayMessage[];
}

/**
 * Create a pure sub-agent message relay. No I/O, no timers — a deterministic model
 * of addressed routing, in-order delivery, terminal lifecycle and dead-lettering.
 */
export function createSubagentRelay(): SubagentRelay {
  const states = new Map<string, SubagentState>();
  const queues = new Map<string, RelayMessage[]>();
  const dead: RelayMessage[] = [];
  let seq = 0;

  const register = (nodeId: string): void => {
    states.set(nodeId, 'pending');
    if (!queues.has(nodeId)) queues.set(nodeId, []);
  };

  const state = (nodeId: string): SubagentState | 'unknown' => states.get(nodeId) ?? 'unknown';

  // Alive = registered and not finished. Both 'pending' and the non-terminal
  // 'idle' waiting state count as alive (an idle teammate can still receive mail).
  const alive = (s: SubagentState | undefined): boolean => s === 'pending' || s === 'idle';

  const isAlive = (nodeId: string): boolean => alive(states.get(nodeId));

  const markIdle = (nodeId: string): RelayIdle => {
    const current = states.get(nodeId);
    // Idle only from an alive state; never resurrects a finished/unknown node, and
    // re-marking an already-idle node is a no-op transition.
    if (current !== 'pending') {
      return { nodeId, state: current ?? 'unknown', transitioned: false };
    }
    states.set(nodeId, 'idle');
    return { nodeId, state: 'idle', transitioned: true };
  };

  const send = (to: string, body: string, from = 'parent'): RelaySendResult => {
    const current = states.get(to);
    // Dead-agent / dropped-message handling: an unregistered or finished recipient
    // cannot receive mail — park the message as a dead-letter and report why. An
    // idle recipient is still alive, so its mail is accepted (parity: idle != gone).
    if (!alive(current)) {
      const reason: RelayRejectReason = current === undefined ? 'unknown-recipient' : 'recipient-finished';
      dead.push({ to, from, body, seq: seq++ });
      return { accepted: false, seq: null, reason };
    }
    const msg: RelayMessage = { to, from, body, seq: seq++ };
    // Ordering: append to the recipient's FIFO queue (drained oldest-first).
    (queues.get(to) as RelayMessage[]).push(msg);
    return { accepted: true, seq: msg.seq };
  };

  const deliver = (nodeId: string): RelayMessage[] => {
    const q = queues.get(nodeId);
    if (!q || q.length === 0) return [];
    // FIFO: hand back everything queued, in sequence order, and clear the mailbox.
    const out = q.slice();
    q.length = 0;
    return out;
  };

  const pending = (nodeId: string): number => queues.get(nodeId)?.length ?? 0;

  const complete = (nodeId: string, next: 'completed' | 'failed' = 'completed'): RelayCompletion => {
    const current = states.get(nodeId);
    // An alive node (pending OR idle) can transition to a terminal state; completing
    // an unknown or already-finished node is an idempotent no-op.
    if (!alive(current)) {
      return { nodeId, state: current ?? 'completed', dropped: [], transitioned: false };
    }
    states.set(nodeId, next);
    const q = queues.get(nodeId) as RelayMessage[];
    // Dropped-message handling: anything still queued at completion is undeliverable.
    const dropped = q.slice();
    q.length = 0;
    for (const m of dropped) dead.push(m);
    return { nodeId, state: next, dropped, transitioned: true };
  };

  const deadLetters = (): readonly RelayMessage[] => dead.slice();

  return { register, isAlive, state, markIdle, send, deliver, pending, complete, deadLetters };
}

/**
 * Build a `subagent_progress` notice announcing that a sub-agent has gone idle
 * (finished its turn and is awaiting more input). Parity: the reference emits an
 * automatic idle notification when a teammate's turn ends — idle is a waiting
 * state, NOT an error and NOT done. The notice is surfaced through the existing
 * `text` activity so no renderer change is required; the wording makes the
 * "idle is not an error" boundary explicit on the live roster.
 */
export function buildSubagentIdleEvent(
  nodeId: string,
  depth: number,
  reason?: string,
): EngineEvent {
  const why = reason && reason.trim() ? reason.replace(/\s+/g, ' ').trim() : 'waiting for input';
  return {
    type: 'subagent_progress',
    nodeId,
    depth,
    activity: 'text',
    detail: `💤 idle — ${why} (idle is not an error)`,
  };
}

/* ------------------------------------------------------------------------- *
 * Spawn-vs-resume invocation accounting (PARITY: the reference agentContext)
 *
 * the reference's agent context carries `invokingRequestId` + `invocationKind`
 * ('spawn'|'resume') plus a mutable `invocationEmitted` flag, and
 * `consumeInvokingRequestId()` returns the id EXACTLY ONCE per spawn/resume
 * boundary (flipping `invocationEmitted`, returning undefined thereafter). That
 * yields a sparse edge marking each spawn/resume boundary exactly once, from
 * which a parent->child invocation graph (and a resume-vs-fresh-spawn
 * distinction) can be reconstructed downstream. This is a pure, deterministic
 * model of that contract so the attribution layer can wire it without
 * re-deriving the once-per-invocation semantics.
 * ------------------------------------------------------------------------- */

/** Whether an agent invocation is a fresh spawn or a resume of an existing node. */
export type InvocationKind = 'spawn' | 'resume';

/** A sparse invocation edge marking one spawn/resume boundary exactly once. */
export interface InvocationEdge {
  readonly nodeId: string;
  /** Parent node id, when the invocation recorded a parent linkage. */
  readonly parentId?: string;
  readonly kind: InvocationKind;
  /** The request id that opened this boundary (only edges with one are sparse-emitted). */
  readonly invokingRequestId?: string;
}

/** Pure spawn-vs-resume invocation ledger. See module comment. */
export interface AgentInvocationLedger {
  /** Record a spawn/resume boundary for a node, (re-)arming its sparse edge. */
  record(nodeId: string, kind: InvocationKind, opts?: { parentId?: string; invokingRequestId?: string }): void;
  /** Most recent invocation kind for a node, or undefined if never recorded. */
  kind(nodeId: string): InvocationKind | undefined;
  /** Parent node id recorded for a node, or undefined. */
  parentOf(nodeId: string): string | undefined;
  /** The node's invoking request id EXACTLY ONCE per boundary (undefined thereafter). */
  consumeInvokingRequestId(nodeId: string): string | undefined;
  /** Sparse edges not yet consumed (one per unemitted spawn/resume boundary). */
  pendingEdges(): InvocationEdge[];
  /** Running tally of spawn vs resume invocations recorded. */
  counts(): { spawn: number; resume: number };
}

interface InvocationRecord {
  kind: InvocationKind;
  parentId?: string;
  invokingRequestId?: string;
  emitted: boolean;
}

/**
 * Create a pure agent invocation ledger modeling the reference's spawn/resume
 * accounting + once-per-invocation sparse-edge emission. No I/O, no timers.
 */
export function createAgentInvocationLedger(): AgentInvocationLedger {
  const records = new Map<string, InvocationRecord>();
  let spawnCount = 0;
  let resumeCount = 0;

  const record = (
    nodeId: string,
    kind: InvocationKind,
    opts?: { parentId?: string; invokingRequestId?: string },
  ): void => {
    if (kind === 'spawn') spawnCount++;
    else resumeCount++;
    const prev = records.get(nodeId);
    records.set(nodeId, {
      kind,
      // A resume keeps the parent linkage from the original spawn unless overridden.
      parentId: opts?.parentId ?? prev?.parentId,
      invokingRequestId: opts?.invokingRequestId,
      // Re-arm the sparse edge: a new boundary is unemitted until consumed.
      emitted: false,
    });
  };

  const kind = (nodeId: string): InvocationKind | undefined => records.get(nodeId)?.kind;

  const parentOf = (nodeId: string): string | undefined => records.get(nodeId)?.parentId;

  const consumeInvokingRequestId = (nodeId: string): string | undefined => {
    const rec = records.get(nodeId);
    // Sparse edge: emit the id exactly once per boundary, then flip emitted.
    if (!rec || rec.emitted || rec.invokingRequestId === undefined) return undefined;
    rec.emitted = true;
    return rec.invokingRequestId;
  };

  const pendingEdges = (): InvocationEdge[] => {
    const out: InvocationEdge[] = [];
    for (const [nodeId, rec] of records) {
      if (!rec.emitted && rec.invokingRequestId !== undefined) {
        out.push({ nodeId, parentId: rec.parentId, kind: rec.kind, invokingRequestId: rec.invokingRequestId });
      }
    }
    return out;
  };

  const counts = (): { spawn: number; resume: number } => ({ spawn: spawnCount, resume: resumeCount });

  return { record, kind, parentOf, consumeInvokingRequestId, pendingEdges, counts };
}

/* ------------------------------------------------------------------------- *
 * Progress-summary gating (PARITY: the reference AgentTool resumeAgent.ts)
 *
 * the reference gates progress-summary generation for background/resumed async agents
 * on `enableSummarization = isCoordinatorMode() || isForkSubagentEnabled() ||
 * getSdkAgentProgressSummariesEnabled()`. AMI keeps live progress summaries ON
 * by default (a superset perk — never regress) but exposes an explicit disable
 * knob; the three coordinator/fork/sdk mode flags force-enable regardless.
 * ------------------------------------------------------------------------- */

const SUMMARY_OFF = new Set(['0', 'off', 'false', 'no']);
const SUMMARY_ON = new Set(['1', 'on', 'true', 'yes']);

const isFlagSet = (v: string | undefined): boolean =>
  v !== undefined && v.trim() !== '' && !SUMMARY_OFF.has(v.trim().toLowerCase());

/**
 * Resolve whether a background/resumed sub-agent should emit live progress
 * summaries. Defaults to enabled (AMI superset). An explicit
 * `AMI_SUBAGENT_SUMMARIES=off|0|false|no` disables it; any of the coordinator /
 * fork-subagent / SDK-progress mode flags force-enable it even over an explicit
 * off (parity: the reference's three-flag `enableSummarization` gate).
 */
export function resolveEnableSummarization(env: NodeJS.ProcessEnv): boolean {
  // Coordinator / fork / SDK-progress modes always get summaries (the reference parity).
  if (
    isFlagSet(env.AMI_COORDINATOR_MODE) ||
    isFlagSet(env.AMI_FORK_SUBAGENT) ||
    isFlagSet(env.AMI_SDK_PROGRESS_SUMMARIES)
  ) {
    return true;
  }
  const explicit = env.AMI_SUBAGENT_SUMMARIES?.trim().toLowerCase();
  if (explicit && SUMMARY_OFF.has(explicit)) return false;
  if (explicit && SUMMARY_ON.has(explicit)) return true;
  // Default: ON (AMI keeps its live progress-summary perk).
  return true;
}
