import * as fs from 'fs';
import { projectStateDir } from '../reference-journal';
import * as path from 'path';
import * as crypto from 'crypto';
import * as child_process from 'child_process';
import { ToolDefinition, ToolContext, ToolResult, EngineConfig, ToolInputSchema, JsonSchemaProperty, Message } from '../types';
import { createDefaultTools } from './index';
import { coerceBool } from '../utils/coerce';
import { isEnvTruthy } from '../analytics';
import { spillToolText } from './spill-output';
import { mapChildToSubagentEvent } from './subagent-relay';
import { getCurrentTeam, registerTeamMember, setTeamMemberActive } from './team';
import { SessionManager } from '../session';
import { PERMISSION_MODES } from '../permissions';
import { isModelFamilyAlias, resolveModelAlias } from '../model-registry';

/** Auto-background threshold for foreground agents exceeding this duration. */
const AUTO_BG_MS = 120_000; // eslint-disable-line @typescript-eslint/no-unused-vars

const SUBAGENT_STALL_TIMEOUT_MS = 10 * 60 * 1000; // 10 minutes

/**
 * Parse a positive-integer env value, falling back to `fallback` when the value
 * is absent, non-numeric, zero, or negative. A bare `parseInt(env) || fallback`
 * only catches NaN and zero — it lets a NEGATIVE value through unchanged, and a
 * negative stall timeout makes `setTimeout` fire effectively immediately, so
 * every subagent would be killed as "stalled" the instant it starts. Rejecting
 * any non-positive value closes that footgun while keeping the same default.
 */
export function parsePositiveIntEnv(raw: string | undefined, fallback: number): number {
  const n = parseInt(raw ?? '', 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

/**
 * Coerce a model-supplied `max_turns` tool argument to a positive integer, or
 * `undefined` when it is absent/invalid. Providers serialize numbers as either a
 * JSON number or a string ("200"), so both are accepted; a zero, negative,
 * fractional, or non-numeric value is rejected (returns undefined) rather than
 * silently capping a subagent at an absurd turn budget.
 */
export function parsePositiveIntInput(raw: unknown): number | undefined {
  if (raw === undefined || raw === null) return undefined;
  const n = typeof raw === 'number' ? raw : parseInt(String(raw), 10);
  return Number.isInteger(n) && n > 0 ? n : undefined;
}

/** Turn budget for a `fork` child when its agent definition sets none (mirrors
 *  Claude Code's FORK_AGENT maxTurns:200 — a context-inheriting clone needs a
 *  generous ceiling because it continues the parent's full task). */
export const DEFAULT_FORK_MAX_TURNS = 200;

/**
 * Git config flags that neutralize arbitrary-code-execution vectors when AMI
 * auto-invokes git on a repository it has not been told is trusted. A malicious
 * `.git/config` can run code via `core.fsmonitor`, `core.hooksPath`, or
 * `diff.external`; these `-c` overrides disable all three for the one invocation.
 * Prepended to every git call the task tool makes on a model-/worktree-derived cwd.
 *
 * NOTE: worktree-manager.ts hardens its OWN git calls with a parallel
 * (hooks/fsmonitor/gpgsign) set; this one adds the `diff.external` vector the
 * cleanup path needs. They should be consolidated into one exported constant once
 * the owning lane can touch worktree-manager.ts.
 */
export const GIT_HARDENING_ARGS = [
  '-c', 'core.fsmonitor=false',
  '-c', 'core.hooksPath=/dev/null',
  '-c', 'diff.external=',
];

/**
 * Generate a high-entropy, symlink-attack-resistant task/agent ID: 8 random
 * bytes rendered as 16 hex chars (2^64 combinations). The ID derives an on-disk
 * output path, so weak entropy (the old `randomBytes(4)` = 2^32) would make a
 * brute-force symlink race on that path feasible; 8 bytes closes it (Claude Code
 * uses a 36-char alphabet for the same 8-byte entropy — hex keeps every existing
 * `agent-[a-f0-9]+` consumer working while matching the entropy floor).
 */
export function generateTaskId(prefix = 'agent'): string {
  return `${prefix}-${crypto.randomBytes(8).toString('hex')}`;
}

/**
 * Framing prepended to a mode-only sub-agent's task (no subagent_type identity) so
 * it returns its result and STOPS instead of running the full interactive coding
 * loop. Mirrors how a workflow agent is told "your final message IS the result".
 */
export const SUBAGENT_RETURN_DIRECTIVE =
  'You are a sub-agent handling one delegated task. Do exactly what the task asks and nothing more. ' +
  'When you have the answer, give it directly as your final message and STOP — do not create or edit ' +
  'files, run commands, or add verification or extra work unless the task explicitly requires it.';

/**
 * Dedicated LEAN system prompt for a mode-only sub-agent (a `task` delegation with no
 * named `subagent_type`). It REPLACES the full interactive coding stance so a bounded
 * delegation returns its result and STOPS instead of over-working (the measured failure:
 * a trivial "reply X" delegation wrote files, ran commands, and looped without
 * terminating because the full coding system prompt — "write files, verify, never stop"
 * — won over a stapled user-message directive). Safety is unaffected: bash/permission
 * guards run at the tool/executor layer, not in the system prompt. ui#13.
 *
 * `fork` keeps the full stance (it is an explicit context-inheriting clone), and a named
 * `subagent_type` keeps its own authoritative identity — neither uses this.
 */
export const SUBAGENT_SYSTEM_PROMPT =
  `You are a focused sub-agent handling exactly one delegated task inside a larger session.

- Do precisely what the task asks — nothing more. Do not add verification, cleanup, refactors, or "while I'm here" work the task did not request.
- Use tools only when the task requires them; prefer answering directly. Do NOT create or edit files, run commands, or run tests unless the task explicitly requires it.
- When searching: start broad and narrow down; if the first strategy finds nothing, try another; consider alternate naming conventions and check multiple locations before concluding something is absent.
- When you have the result, give it directly and concisely as your final message, then STOP. Your final message IS the deliverable returned to the caller.
- Do not loop re-checking, re-running, or re-announcing completion. One correct answer, then stop.
- Read any error in full and fix the root cause; never fabricate results. If the task is impossible or underspecified, say so plainly and stop.`;

/**
 * Minimum number of distinct searches an `explore`-mode sub-agent should run
 * before it concludes something is absent or reports back (mirrors Claude Code's
 * EXPLORE_AGENT_MIN_QUERIES). It guards the UNDER-exploration edge: the lean
 * prompt's "do not over-explore" is only an upper bound; without a floor an
 * explorer can return prematurely after a single miss and wrongly conclude a
 * symbol/file does not exist. The prompt below cites this floor so the agent runs
 * at least this many search strategies before declaring absence.
 */
export const EXPLORE_AGENT_MIN_QUERIES = 3;

/** Lean system prompt for a read-only `explore`-mode sub-agent. ui#13. */
export const EXPLORE_SUBAGENT_SYSTEM_PROMPT =
  `You are a read-only exploration sub-agent handling one search/investigation task.

- Investigate using read and search tools only. Do NOT modify files, run mutating commands, or make any changes.
- Adapt your search breadth to the thoroughness the caller requests ("quick" for a basic lookup, "medium" for moderate exploration, "very thorough" for comprehensive analysis).
- Start broad and narrow down; if one search strategy yields nothing, try another; consider alternate naming conventions and check multiple locations before concluding something does not exist.
- Run at least ${EXPLORE_AGENT_MIN_QUERIES} distinct searches (different strategies, terms, or locations) before concluding something is absent — do not report "not found" after a single miss.
- Gather exactly what the task asks for, cite concrete locations (file:line) where relevant, then report your findings directly and concisely as your final message and STOP. Your final message IS the deliverable.
- Do not over-explore beyond what the task asks. One focused report, then stop.`;

const agentNameRegistry = new Map<string, string>();
// taskId -> the sub-engine sessionId it ran under. Lets `resume` recreate an
// agent on its PRIOR session (the Engine reloads saved messages when given a
// known sessionId), so a follow-up prompt continues the same context.
const agentSessionRegistry = new Map<string, string>();

export function getAgentByName(name: string): string | undefined {
  return agentNameRegistry.get(name);
}

export function getAgentNameById(taskId: string): string | undefined {
  for (const [name, id] of agentNameRegistry) {
    if (id === taskId) return name;
  }
  return undefined;
}

export function resetAgentRegistry(): void {
  agentNameRegistry.clear();
  agentSessionRegistry.clear();
}

/**
 * Resolve a caller-supplied agent `name` to a UNIQUE addressable name. The task
 * tool documents `name` as "must be unique" (it is the send_message / resume
 * key), but a model reusing a base name would silently overwrite the earlier
 * agent's registry entry, orphaning it. On collision we append an incrementing
 * numeric suffix (`name-2`, `name-3`, …) so every spawned agent keeps a distinct,
 * addressable identity (mirrors Claude Code's generateUniqueTeammateName).
 */
export function uniqueAgentName(name: string): string {
  if (!agentNameRegistry.has(name)) return name;
  let i = 2;
  while (agentNameRegistry.has(`${name}-${i}`)) i++;
  return `${name}-${i}`;
}

/** Routing-significant characters that must never appear in an agent name: `@`
 *  is the reserved `agentName@teamName` identity separator and `*` is the
 *  send_message broadcast wildcard. */
const AGENT_NAME_RESERVED_CHARS = /[@*]/g;

/**
 * Sanitize a caller-supplied agent `name` so it is safe to use as a routing /
 * mailbox key (mirrors Claude Code's sanitizeAgentName, which strips the `@`
 * team separator). The agent name becomes the send_message / resume address and
 * is baked into the team-scoped identity, so a name containing `@` would corrupt
 * `agentName@teamName` parsing and a name of (or containing) `*` would collide
 * with send_message's broadcast wildcard — a model-chosen `*` agent could then
 * silently intercept every broadcast. Reserved routing chars are replaced with
 * `-`, runs collapsed, and stray leading/trailing separators trimmed. Returns an
 * empty string only when the input was ENTIRELY reserved characters, so callers
 * can fall back to no-name rather than register a degenerate key.
 */
export function sanitizeAgentName(name: string): string {
  return name
    .replace(AGENT_NAME_RESERVED_CHARS, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

const DEFAULT_MAX_AGENT_DEPTH = 5;
export const DEFAULT_MAX_AGENTS_PER_SESSION = 50;
export const DEFAULT_MAX_CONCURRENT_AGENTS = 10;

/**
 * Universal baseline denylist applied to EVERY sub-agent regardless of its mode
 * or explicit tool allowlist (mirrors Claude Code's ALL_AGENT_DISALLOWED_TOOLS /
 * CUSTOM_AGENT_DISALLOWED_TOOLS — the one source-of-truth set unioned into every
 * agent's denylist):
 *  - plan_mode is interactive-only — its "exit" awaits a user approval that never
 *    comes in a sub-agent, and its state is module-global (would corrupt the
 *    parent's plan state).
 *  - workflow would let a sub-agent launch another workflow run recursively.
 *  - AskUserQuestion is interactive-only: a sub-agent has no user to answer it, so
 *    it would hang forever waiting for input that never arrives.
 *  - task_output / task_kill are lead-only coordination tools that read/terminate
 *    background tasks tracked in the PARENT's process manager — a sub-agent calling
 *    them would collect or kill the lead's tasks, not its own.
 * An explicit agent-def `tools` list (even `['*']`) cannot reintroduce these.
 */
export const ALL_AGENT_DISALLOWED_TOOLS = new Set([
  'plan_mode', 'workflow', 'AskUserQuestion', 'task_output', 'task_kill',
]);

/**
 * Explicit allowlist (SOURCE OF TRUTH) of the tools an ASYNC / background
 * sub-agent (`run_in_background: true`) may use — mirrors Claude Code's
 * ASYNC_AGENT_ALLOWED_TOOLS (`constants/tools.ts`). A detached background agent
 * runs with no live parent turn to supervise it, so it is restricted to
 * self-contained read / search / edit / planning tools and must NOT get:
 *  - `task` (AgentTool) — a detached agent spawning more detached agents is an
 *    unsupervised recursion fan-out.
 *  - lead-only coordination (`task_output` / `task_kill`) — already in the
 *    universal floor, but listed as blocked here for the same reason: they read /
 *    terminate the PARENT's process-manager entries.
 *  - interactive/stateful tools (`plan_mode`, `AskUserQuestion`, `send_message`,
 *    `workflow`, cron/team/config/memory) that assume a live session or would
 *    mutate shared state from a background context.
 * Unlike mode/agent-def filtering this is an INTERSECTION applied last on the
 * background path: whatever the mode or agent `tools` list resolved to is further
 * narrowed to this set, so no allowlist (even `['*']`) can widen a background
 * agent beyond it. `file_write`/`notebook_edit` map to AMI's `file_edit`/
 * `notebook_edit`; `structured_output` is AMI's synthetic-output tool; the
 * AMI-native read tools `list_dir`/`search_symbols` are the direct analogs of the
 * allowed `grep`/`glob`/`file_read` read class, so they are included too.
 */
export const ASYNC_AGENT_ALLOWED_TOOLS = new Set([
  'file_read', 'grep', 'glob', 'list_dir', 'search_symbols',
  'web_search', 'web_fetch',
  'bash',
  'file_edit', 'notebook_edit',
  'todo_write',
  'skill', 'tool_search',
  'structured_output',
  'enter_worktree', 'exit_worktree',
]);

/**
 * Positive allowlist for a `coordinator`-mode sub-agent — an orchestration-only
 * lead that may delegate and report but may NOT touch the filesystem or shell.
 * Mirrors Claude Code's `COORDINATOR_MODE_ALLOWED_TOOLS`
 * (`constants/tools.ts`: {AgentTool, TaskStop, SendMessage, SyntheticOutput}),
 * mapped to AMI's tool names:
 *  - `task`              ← AGENT_TOOL_NAME        (spawn/delegate to workers)
 *  - `send_message`      ← SEND_MESSAGE_TOOL_NAME (coordinate with teammates)
 *  - `structured_output` ← SYNTHETIC_OUTPUT_TOOL_NAME (produce the final result)
 * Claude's TaskStop / TaskOutput are deliberately NOT included: AMI's universal
 * floor ({@link ALL_AGENT_DISALLOWED_TOOLS}) makes `task_kill` / `task_output`
 * lead-only (a sub-agent calling them would terminate/collect the PARENT's tasks,
 * not its own), so they are never grantable to a spawned coordinator. The result
 * is a pure-delegation agent: it can orchestrate and emit output, nothing else.
 */
export const COORDINATOR_MODE_ALLOWED_TOOLS = new Set([
  'task', 'send_message', 'structured_output',
]);

/**
 * Lean system prompt for a `coordinator`-mode sub-agent. It has an
 * orchestration-only tool set ({@link COORDINATOR_MODE_ALLOWED_TOOLS}) with no
 * file/shell access, so it is told to delegate concrete work to workers via
 * `task`, coordinate via `send_message`, and return its synthesis — never to
 * attempt edits/commands itself (it has no tools for them).
 */
export const COORDINATOR_SUBAGENT_SYSTEM_PROMPT =
  `You are an orchestration coordinator. You have NO file, edit, or shell tools — you cannot read or change code yourself.
- Delegate every concrete piece of work to a worker sub-agent with the task tool; run independent pieces as parallel task calls in one turn.
- Coordinate with teammates using send_message.
- Your job is to plan, delegate, and synthesize the workers' results into one answer. Do not attempt to do the implementation yourself; you have no tools for it.`;

/** The sentinel a sub-agent model request uses to mean "inherit the parent's model". */
export const INHERIT_MODEL_SENTINEL = 'inherit';

/**
 * Resolve a sub-agent's model from a requested value, with ANTI-DOWNGRADE:
 * when the request is a bare family alias (sonnet/opus/haiku) and the PARENT is
 * already running a model of that same family, inherit the parent's EXACT model
 * string instead of the generic alias default. So a parent on
 * `claude-opus-4-8` that spawns `task({ model: 'opus' })` keeps the child on
 * `claude-opus-4-8` rather than silently downgrading it to `claude-opus-4`.
 * A full model name is returned verbatim; an unknown alias passes through.
 *
 * Alias recognition/resolution is delegated to the single canonical alias table
 * in model-registry (`isModelFamilyAlias` / `resolveModelAlias`) so the subagent
 * path, validateModel, and any CLI/slash path all resolve the same short names.
 */
export function resolveSubagentModel(requested: string | undefined, parentModel: string | undefined): string | undefined {
  if (!requested) return undefined;
  // Normalize the request for sentinel/alias MATCHING only: a provider tool-call
  // or an agent-definition `model` field may emit a family alias or the inherit
  // sentinel in any case and with stray surrounding whitespace ("Opus", " INHERIT ").
  // A full model name or unknown value is still returned verbatim (the final
  // branch), so exact provider model IDs are never lower-cased or trimmed away.
  const normalized = requested.trim().toLowerCase();
  // 'inherit' is an explicit sentinel meaning "run on the SAME model as the main
  // conversation" (Claude's default subagent model). Resolve it to the parent's
  // exact model string — never to a generic alias default — so a parent on
  // claude-opus-4-8 that spawns task({ model: 'inherit' }) keeps the child there.
  if (normalized === INHERIT_MODEL_SENTINEL) return parentModel;
  if (isModelFamilyAlias(normalized) && parentModel && parentModel.toLowerCase().includes(normalized)) {
    return parentModel;
  }
  return resolveModelAlias(normalized) ?? requested;
}

/**
 * Sentinels AMI writes into an assistant message's content when a turn did NOT
 * finish cleanly: the engine's server-error interrupt
 * (`[Response interrupted by server error]`) and the tool-call interrupt family
 * (`[Tool call interrupted …]`). AMI's Message type carries no `stop_reason`
 * field, so these in-band markers are the only discriminator for "this assistant
 * turn is incomplete/in-progress" — the same marker set `session-search`
 * recognizes for its rendered-as-sentinel exclusion.
 */
const INCOMPLETE_ASSISTANT_SENTINEL_RE =
  /\[(?:Response interrupted by server error|Tool call interrupted[^\]]*)\]/;

/**
 * Whether a message is an assistant turn that did not complete cleanly (an
 * interrupted/in-progress turn). Mirrors Claude Code's `stop_reason === null`
 * discriminator in `buildSideQuestionFallbackParams`; since AMI messages have no
 * `stop_reason`, it detects the in-band interrupt sentinels instead.
 */
export function isIncompleteAssistantMessage(msg: Message): boolean {
  return (
    msg.role === 'assistant' &&
    typeof msg.content === 'string' &&
    INCOMPLETE_ASSISTANT_SENTINEL_RE.test(msg.content)
  );
}

/**
 * Drop a TRAILING in-progress/interrupted assistant message before a conversation
 * is forked into a sub-agent (mirrors Claude Code's
 * `buildSideQuestionFallbackParams`, which discards the last message when it is an
 * assistant turn with `stop_reason === null`). A fork can be requested mid-turn,
 * and an unterminated/interrupted assistant block would otherwise leak a corrupt
 * half-answer into the forked context. Only the trailing message is considered —
 * an interrupted turn deeper in history was already followed by a completed one.
 */
export function stripIncompleteTrailingMessages(messages: Message[]): Message[] {
  if (messages.length === 0) return messages;
  const last = messages[messages.length - 1];
  return isIncompleteAssistantMessage(last) ? messages.slice(0, -1) : messages;
}

/**
 * Pick a sub-agent's returned deliverable: the text of its FINAL assistant turn
 * (`lastTurnText`) when one carried text, else the accumulated text fallback.
 * Mirrors Claude Code's `finalizeAgentTool` (agentToolUtils.ts:L297-317), which
 * returns the last assistant message's text and — when that final turn is a pure
 * tool_use block — walks backward to the most recent turn that had text. Tracking
 * the last text-bearing `turn_complete` performs that backward walk incrementally,
 * so only the clean final answer is handed back instead of every intermediate
 * narration delta concatenated across the run.
 */
export function selectFinalAnswer(lastTurnText: string | undefined, accumulated: string): string {
  return lastTurnText !== undefined ? lastTurnText : accumulated;
}

/**
 * Decide whether an isolated agent worktree is safe to remove after the
 * foreground agent returns.
 *
 * Remove ONLY when all three hold:
 *  - `status` (git status --porcelain, including untracked) is empty — no
 *    uncommitted changes, so no new files a background agent may have written.
 *  - `unpushed` (git rev-list HEAD --not --remotes) is empty — no commits that
 *    would be lost.
 *  - `runningAgentCount === 0` — no background subagent is still live inside the
 *    worktree. A background subagent inherits the worktree cwd and runs detached
 *    from the parent abort, so removing the worktree while it is working would
 *    pull the rug out from under it (lost/partial writes, git errors).
 */
export function shouldRemoveWorktree(
  status: string,
  unpushed: string,
  runningAgentCount: number,
): boolean {
  return !status && !unpushed && runningAgentCount === 0;
}

/**
 * Check whether spawning a new agent is allowed under session and concurrency caps.
 * Returns null if allowed, or an error string if blocked.
 */
export function checkAgentCaps(context: {
  _agentSpawnCount?: { value: number };
  _maxAgentsPerSession?: number;
  _maxConcurrentAgents?: number;
  processManager?: import('../process-manager').ProcessManager;
}): string | null {
  const sessionCap = context._maxAgentsPerSession ?? DEFAULT_MAX_AGENTS_PER_SESSION;
  const spawnCount = context._agentSpawnCount?.value ?? 0;
  if (spawnCount >= sessionCap) {
    return `Agent session cap reached (${sessionCap}). ${spawnCount} agents already spawned this session. Increase _maxAgentsPerSession to raise the limit.`;
  }

  const concurrentCap = context._maxConcurrentAgents ?? DEFAULT_MAX_CONCURRENT_AGENTS;
  if (context.processManager) {
    const running = context.processManager.runningAgentCount();
    if (running >= concurrentCap) {
      return `Concurrent agent cap reached (${concurrentCap}). ${running} agents currently running. Wait for agents to complete or increase _maxConcurrentAgents.`;
    }
  }

  return null;
}

function incrementSpawnCount(context: { _agentSpawnCount?: { value: number } }): void {
  if (!context._agentSpawnCount) {
    context._agentSpawnCount = { value: 0 };
  }
  context._agentSpawnCount.value++;
}

export function shouldAllowNestedTask(currentDepth: number, maxDepth: number): boolean {
  return currentDepth < maxDepth;
}

/**
 * Notice prepended to a subagent that runs in an isolated git worktree (mirrors
 * Claude Code's buildWorktreeNotice). It tells the child three concrete things:
 * (1) paths in any inherited/parent context refer to the PARENT's cwd and must be
 * translated to the worktree root, (2) re-read files before editing since the
 * parent may have modified them, and (3) its changes are isolated to the worktree.
 */
export function buildWorktreeNotice(parentCwd: string, worktreeCwd: string): string {
  return `You are operating in an isolated git worktree at ${worktreeCwd} — same repository, same relative file structure, separate working copy from the parent agent working in ${parentCwd}. Paths in any inherited context refer to the parent's working directory; translate them to your worktree root. Re-read files before editing if the parent may have modified them. Your changes stay in this worktree and will not affect the parent's files.`;
}

/**
 * Validate a model-requested subagent `cwd` override against the parent's trusted
 * permission boundary (mirrors Claude deriving the agent's working-dir set from the
 * PARENT's toolPermissionContext rather than a model-chosen path). The override is
 * accepted only when it is an absolute, existing directory that resolves within the
 * parent cwd OR one of the already-approved additional working directories. Returns
 * null when allowed, or an error string describing the violation.
 */
export function validateSubagentCwd(
  requestedCwd: string,
  parentCwd: string,
  additionalRoots: string[] = [],
): string | null {
  if (!path.isAbsolute(requestedCwd)) {
    return `cwd must be an absolute path (got "${requestedCwd}").`;
  }
  let stat: fs.Stats;
  try {
    stat = fs.statSync(requestedCwd);
  } catch {
    return `cwd "${requestedCwd}" does not exist.`;
  }
  if (!stat.isDirectory()) {
    return `cwd "${requestedCwd}" is not a directory.`;
  }
  const resolved = fs.realpathSync(requestedCwd);
  const roots = [parentCwd, ...additionalRoots]
    .filter(Boolean)
    .map(r => { try { return fs.realpathSync(r); } catch { return path.resolve(r); } });
  const within = roots.some(root => resolved === root || resolved.startsWith(root + path.sep));
  if (!within) {
    return `cwd "${requestedCwd}" is outside the parent working directory and approved directories.`;
  }
  return null;
}

export function getSubagentDepthContext(currentDepth: number, maxDepth: number): { _agentDepth: number; _maxAgentDepth: number } {
  return { _agentDepth: currentDepth + 1, _maxAgentDepth: maxDepth };
}

/**
 * Filter the tool set for an agent based on mode and nesting depth.
 * Exported for testing; used internally by the task tool execute method.
 */
export function filterToolsForAgent(
  allTools: ToolDefinition[],
  mode: string,
  agentDepth?: number,
  maxAgentDepth?: number,
): ToolDefinition[] {
  const readOnlyNames = new Set([
    'file_read', 'grep', 'glob', 'list_dir', 'search_symbols',
    'web_search', 'web_fetch',
  ]);

  if (mode === 'explore') {
    return allTools.filter(t => readOnlyNames.has(t.name));
  }

  const depth = agentDepth ?? 0;
  const maxDepth = maxAgentDepth ?? DEFAULT_MAX_AGENT_DEPTH;
  const allowNested = shouldAllowNestedTask(depth, maxDepth);

  // Coordinator mode: a pure-delegation lead restricted to the orchestration-only
  // allowlist (spawn/message/output). Honours the same nested-task depth cap as
  // `general` — a coordinator's primary tool IS `task`, but it still cannot spawn
  // past the max nesting depth.
  if (mode === 'coordinator') {
    return allTools.filter(t =>
      COORDINATOR_MODE_ALLOWED_TOOLS.has(t.name) && (allowNested || t.name !== 'task'));
  }

  // plan_mode is an INTERACTIVE-session-only feature: "exit" submits the plan for
  // user approval, and a subagent has no user to approve it — so a subagent that
  // enters plan mode hangs forever "awaiting approval". Its state is also module-
  // global, so a subagent entering plan mode would corrupt the parent engine's
  // plan state. Autonomous subagents must never get it.
  return allTools.filter(t =>
    t.name !== 'plan_mode' && (allowNested || t.name !== 'task'));
}

/**
 * Narrow an already-resolved tool set to the async/background allowlist
 * (`ASYNC_AGENT_ALLOWED_TOOLS`). Applied as the LAST filter on the
 * `run_in_background: true` spawn path so a detached agent — which has no live
 * parent turn supervising it — cannot use recursion-prone (`task`) or
 * interactive/stateful tools even when its mode or agent-def `tools` list (incl.
 * `['*']`) resolved to a wider set. Exported for testing; used internally by the
 * task tool execute method.
 */
export function filterToolsForAsyncAgent(tools: ToolDefinition[]): ToolDefinition[] {
  return tools.filter(t => ASYNC_AGENT_ALLOWED_TOOLS.has(t.name));
}

/**
 * Normalize a caller-supplied `allowed_agent_types` value into a string[] (or
 * undefined when the field is absent). Mirrors Claude Code's `allowedAgentTypes`
 * boundary (`AgentTool/prompt.ts`): when a parent scopes which subagent types a
 * given invocation may spawn (`Agent(x,y)`), only the listed types are
 * spawnable. Returns `{ error }` for a non-array / non-string-element shape so
 * the caller can reject it; `{ value: undefined }` means "no restriction".
 */
export function parseAllowedAgentTypes(
  raw: unknown,
): { value: string[] | undefined } | { error: string } {
  if (raw === undefined || raw === null) return { value: undefined };
  if (!Array.isArray(raw) || raw.some(t => typeof t !== 'string')) {
    return { error: 'allowed_agent_types must be an array of strings (the subagent types this call may spawn).' };
  }
  return { value: raw as string[] };
}

/**
 * Whether a requested `subagent_type` is permitted under an `allowedAgentTypes`
 * restriction. Mirrors Claude's `allowedAgentTypes.includes(a.agentType)` filter:
 * `undefined` means no restriction (every type allowed); a provided list (even
 * empty) restricts spawning to exactly its members.
 */
export function isAgentTypeAllowed(
  subagentType: string,
  allowedAgentTypes: string[] | undefined,
): boolean {
  return allowedAgentTypes === undefined || allowedAgentTypes.includes(subagentType);
}

/**
 * Env knob that globally disables background (`run_in_background`) subagents —
 * mirrors Claude Code's `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS`. When truthy the
 * `run_in_background` field is OMITTED from the advertised input schema (so the
 * model is never offered the flag) AND a late attempt to set it (e.g. a stale /
 * replayed call) is rejected at runtime. Operators/CI use it to turn detached
 * agent spawning off in environments where unsupervised background work is
 * disallowed.
 */
export const DISABLE_BACKGROUND_TASKS_ENV = 'CLAUDE_CODE_DISABLE_BACKGROUND_TASKS';

/** True when background subagents are disabled via {@link DISABLE_BACKGROUND_TASKS_ENV}. */
export function isBackgroundTasksDisabled(): boolean {
  return isEnvTruthy(process.env[DISABLE_BACKGROUND_TASKS_ENV]);
}

/** Valid values for the per-spawn `system_prompt_mode` knob. */
export const SYSTEM_PROMPT_MODES = ['default', 'replace', 'append'] as const;
export type SystemPromptMode = (typeof SYSTEM_PROMPT_MODES)[number];

/**
 * System-prompt addendum appended to a subagent that is spawned as a named member
 * of an active team (mirrors Claude Code's teammatePromptAddendum). AMI already
 * has all the machinery — `send_message` (with `to: "*"` broadcast), team
 * mailboxes with eager delivery, the shared team task list — but nothing TOLD a
 * teammate that (a) it is one agent in a team, (b) it reaches teammates via
 * `send_message({ to: "<name>" })`, (c) a plain text reply is invisible to
 * teammates so `send_message` is REQUIRED, and (d) the user talks mainly to the
 * lead and work is coordinated through the task system + messaging.
 */
export const TEAMMATE_PROMPT_ADDENDUM =
  `You are running as an agent in a team of agents collaborating on this work.
- Reach a teammate with the send_message tool: send_message({ to: "<name>", message: ... }); broadcast to everyone with to: "*".
- CRITICAL: just writing a response in text is NOT visible to others on your team — you MUST use the send_message tool to communicate with them.
- The user interacts primarily with the team lead. Your work is coordinated through the shared task system and teammate messaging, not by replying in plain text.`;

/**
 * Build the caller-requested thoroughness directive for an `explore`-mode
 * sub-agent (mirrors Claude Code's exploreAgent thoroughness parameter). The
 * explore prompt tells the agent to "adapt your search breadth to the
 * thoroughness the caller requests"; this turns an abstract instruction into a
 * concrete, caller-chosen depth dial so a "very thorough" request produces a
 * comprehensive multi-location/naming-convention sweep and a "quick" request a
 * single-pass lookup.
 */
export function buildThoroughnessDirective(level: string): string {
  switch (level) {
    case 'quick':
      return '[Thoroughness: quick] Do a fast, single-pass lookup — the smallest search that answers the question, then stop.';
    case 'very thorough':
      return `[Thoroughness: very thorough] Do a comprehensive sweep: search across multiple locations and alternate naming conventions, run well beyond the ${EXPLORE_AGENT_MIN_QUERIES}-query floor, and cross-check before concluding.`;
    case 'medium':
    default:
      return '[Thoroughness: medium] Do moderate exploration — check the obvious locations and a couple of alternate strategies before reporting.';
  }
}

/** Valid values for the explore `thoroughness` dial. */
export const EXPLORE_THOROUGHNESS_LEVELS = ['quick', 'medium', 'very thorough'] as const;

/**
 * Static property set for the task tool's input schema. Extracted to a named
 * constant so the schema can be rebuilt per-advertisement (the `inputSchema`
 * getter below omits `run_in_background` when background tasks are disabled).
 */
const TASK_TOOL_PROPERTIES: Record<string, JsonSchemaProperty> = {
  prompt: {
    type: 'string',
    description: 'The task for the subagent to perform.',
  },
  mode: {
    type: 'string',
    description: 'Agent mode. "explore" gives read-only tools only (state the desired thoroughness in the prompt or via the thoroughness field: "quick", "medium", or "very thorough"). "general" gives all tools including nested task (up to max depth). "fork" clones parent conversation context. "coordinator" gives orchestration-only tools (spawn subagents, send_message, structured_output) with NO file/shell access — a pure delegation lead.',
    enum: ['explore', 'general', 'fork', 'coordinator'],
  },
  permission_mode: {
    type: 'string',
    description: 'Permission mode for the spawned subagent (overrides the default). E.g. "plan", "acceptEdits", "auto-allow". Distinct from "mode", which selects the tool tier.',
    enum: [...PERMISSION_MODES],
  },
  isolation: {
    type: 'string',
    enum: ['worktree'],
    description: 'Run the subagent in an isolated git worktree.',
  },
  subagent_type: {
    type: 'string',
    description: 'Name of an agent definition to use (from .ami/agents/). Overrides mode, tools, and system prompt.',
  },
  allowed_agent_types: {
    type: 'array',
    items: { type: 'string' },
    description: 'Restrict which subagent_type values this call may spawn. When set, a subagent_type outside this list is rejected (an empty list forbids every named agent type). Omit for no restriction.',
  },
  model: {
    type: 'string',
    description: 'Model override. Accepts full names or aliases: "sonnet", "opus", "haiku", "gpt-4o", "gemini-pro". The special value "inherit" runs the subagent on the same model as the main conversation.',
  },
  system_prompt: {
    type: 'string',
    description: 'Inline system prompt for this one spawn, decoupled from any on-disk agent definition. By default it REPLACES the subagent\'s base system prompt; use system_prompt_mode:"append" to layer it on top of the base stance instead.',
  },
  system_prompt_mode: {
    type: 'string',
    enum: [...SYSTEM_PROMPT_MODES],
    description: 'How system_prompt is applied: "replace"/"default" fully replaces the subagent\'s base system prompt; "append" appends it to the base (explore/general/named-agent only — fork has no replaceable base). Ignored when system_prompt is absent.',
  },
  thoroughness: {
    type: 'string',
    enum: [...EXPLORE_THOROUGHNESS_LEVELS],
    description: 'Explore-mode search depth dial: "quick" for a single-pass lookup, "medium" for moderate exploration, "very thorough" for a comprehensive multi-location/naming-convention sweep. Only affects mode:"explore".',
  },
  cwd: {
    type: 'string',
    description: 'Override working directory for the subagent. Must be an absolute path.',
  },
  name: {
    type: 'string',
    description: 'Name for the agent (addressable via send_message). Must be unique.',
  },
  description: {
    type: 'string',
    description: 'Short human-readable label for the task (3-5 words).',
  },
  run_in_background: {
    type: 'boolean',
    description: 'When true, runs the subagent in the background and returns a task_id immediately instead of waiting for completion.',
  },
  resume: {
    type: 'string',
    description: 'Resume a previously stopped agent by task ID or name. The prompt is sent as a follow-up message to the existing agent context.',
  },
  effort: {
    type: 'string',
    description: 'Reasoning effort for this subagent. Controls thinking depth.',
    enum: ['low', 'medium', 'high', 'max'],
  },
  max_turns: {
    type: 'number',
    description: 'Maximum number of agentic turns (API round-trips) for this subagent before it stops. Caps a mode-only general/explore/fork subagent that has no named agent definition to set one. Must be a positive integer.',
  },
};

export const taskTool: ToolDefinition = {
  name: 'task',
  description:
    'Spawn a subagent to perform a specific task in an isolated context with its own conversation. ' +
    'Use for parallel codebase exploration, research, or delegating independent subtasks. The subagent\'s ' +
    'final text response is returned as the result. When you are searching for a keyword or file and are ' +
    'not confident you will find the right match in the first few tries, delegate the search to a subagent ' +
    'instead of burning your own context on repeated misses.\n' +
    '\n' +
    'Choosing an agent by capability: read-only agents (mode:"explore", or a custom agent whose tools are ' +
    'restricted to search/read) must ONLY be given research, search, or planning tasks — never implementation/' +
    'edit work, which they have no tools to do. Use a full-capability agent (mode:"general" or a subagent_type ' +
    'with edit/bash tools) for changes. Before assigning work to a named subagent_type, review its tool set so ' +
    'the task matches what the agent can actually do.\n' +
    '\n' +
    'Writing the prompt: brief the agent like a smart colleague who just walked into the room — it has not ' +
    'seen this conversation. Explain what you are trying to accomplish and why, what you have already ruled ' +
    'out, and enough surrounding context that it can make judgment calls. Include concrete file paths and ' +
    'line numbers. For lookups hand over the exact command; for investigations hand over the question. Do ' +
    'not delegate understanding ("based on your findings, fix the bug") — terse command-style prompts produce ' +
    'shallow work. If you need a short answer, say so ("report in under 200 words").\n' +
    '\n' +
    'When NOT to use: to read a specific file, use file_read; to find a symbol/class, use search_symbols or ' +
    'grep; to look in 2-3 known files, read them directly — a subagent is slower for those.\n' +
    '\n' +
    'Usage notes: always pass a short description (3-5 words). Launch multiple agents concurrently by issuing ' +
    'several task calls in one turn. The result is not shown to the user automatically — summarize it back. ' +
    'With run_in_background:true the agent runs detached and you collect it with task_output. Set ' +
    'isolation:"worktree" to give the agent an isolated copy of the repo (auto-removed if it makes no changes). ' +
    'Use resume with an agent id/name to continue a prior agent with its context preserved.',
  // Rebuilt per advertisement so `run_in_background` can be dynamically OMITTED
  // when background tasks are disabled via CLAUDE_CODE_DISABLE_BACKGROUND_TASKS —
  // the model is then never offered the flag (mirrors Claude Code's
  // fullInputSchema().omit({run_in_background:true}) gate).
  get inputSchema(): ToolInputSchema {
    const properties: Record<string, JsonSchemaProperty> = { ...TASK_TOOL_PROPERTIES };
    if (isBackgroundTasksDisabled()) {
      delete properties.run_in_background;
    }
    return { type: 'object', properties, required: ['prompt'] };
  },
  isReadOnly: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const prompt = input.prompt as string;
    const mode = (input.mode as string) || 'general';
    const subagentType = input.subagent_type as string | undefined;
    const taskDescription = input.description as string | undefined;
    const resumeTarget = input.resume as string | undefined;
    const effortOverride = input.effort as string | undefined;
    const requestedPermissionMode = input.permission_mode as string | undefined;
    const customSystemPromptRaw = input.system_prompt as string | undefined;
    const customSystemPrompt = customSystemPromptRaw && customSystemPromptRaw.trim().length > 0
      ? customSystemPromptRaw
      : undefined;
    const systemPromptMode = (input.system_prompt_mode as string | undefined) || 'default';
    const thoroughness = input.thoroughness as string | undefined;

    if (!prompt || prompt.trim().length === 0) {
      return { output: 'Error: prompt must not be empty.', isError: true };
    }

    if (requestedPermissionMode && !(PERMISSION_MODES as readonly string[]).includes(requestedPermissionMode)) {
      return {
        output: `Error: invalid permission_mode "${requestedPermissionMode}". Valid values: ${PERMISSION_MODES.join(', ')}.`,
        isError: true,
      };
    }

    if (input.system_prompt_mode !== undefined && !(SYSTEM_PROMPT_MODES as readonly string[]).includes(systemPromptMode)) {
      return {
        output: `Error: invalid system_prompt_mode "${systemPromptMode}". Valid values: ${SYSTEM_PROMPT_MODES.join(', ')}.`,
        isError: true,
      };
    }

    if (thoroughness !== undefined && !(EXPLORE_THOROUGHNESS_LEVELS as readonly string[]).includes(thoroughness)) {
      return {
        output: `Error: invalid thoroughness "${thoroughness}". Valid values: ${EXPLORE_THOROUGHNESS_LEVELS.join(', ')}.`,
        isError: true,
      };
    }

    // allowed_agent_types: a per-invocation boundary scoping which named
    // subagent_type values this call may spawn (mirrors Claude's allowedAgentTypes
    // / Agent(x,y) restriction). Validate the shape, then reject a requested
    // subagent_type that falls outside the permitted subset.
    const allowedAgentTypesResult = parseAllowedAgentTypes(input.allowed_agent_types);
    if ('error' in allowedAgentTypesResult) {
      return { output: `Error: ${allowedAgentTypesResult.error}`, isError: true };
    }
    const allowedAgentTypes = allowedAgentTypesResult.value;
    if (subagentType !== undefined && !isAgentTypeAllowed(subagentType, allowedAgentTypes)) {
      const permitted = (allowedAgentTypes as string[]).length
        ? (allowedAgentTypes as string[]).join(', ')
        : '(none)';
      return {
        output: `Error: subagent_type "${subagentType}" is not permitted here. Allowed agent types: ${permitted}.`,
        isError: true,
      };
    }

    // Background tasks can be globally disabled by CLAUDE_CODE_DISABLE_BACKGROUND_TASKS.
    // The flag is already omitted from the advertised schema when disabled; a stale /
    // replayed call that still sets it is rejected here so it cannot silently spawn a
    // detached agent in an environment that forbids them.
    if (coerceBool(input.run_in_background) && isBackgroundTasksDisabled()) {
      return {
        output: `Error: background subagents are disabled (${DISABLE_BACKGROUND_TASKS_ENV} is set). ` +
          'Run this task in the foreground (omit run_in_background), or unset the env knob.',
        isError: true,
      };
    }

    // Resume: resolve the prior sub-engine sessionId so the spawn below reuses
    // it (the Engine reloads saved messages for a known sessionId), turning this
    // prompt into a follow-up on the existing agent context.
    let resumeSessionId: string | undefined;
    if (resumeTarget) {
      const resolvedId = agentNameRegistry.get(resumeTarget) || resumeTarget;
      resumeSessionId = agentSessionRegistry.get(resolvedId);
      if (!resumeSessionId) {
        const known = [...agentSessionRegistry.keys()];
        return {
          output: `Error: no resumable agent found for "${resumeTarget}". ` +
            `Known agent ids: ${known.length ? known.join(', ') : '(none in this session)'}.`,
          isError: true,
        };
      }
    }

    // Enforce session and concurrency caps before any spawn work
    const capError = checkAgentCaps(context);
    if (capError) {
      return { output: `Error: ${capError}`, isError: true };
    }

    // Enforce the per-session lifetime budget (env-configurable via
    // CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION). Depth and concurrency remain
    // governed by checkAgentCaps/shouldAllowNestedTask above — the limiter only
    // owns the lifetime cap here.
    if (context.subagentLimiter?.isLifetimeBudgetExhausted()) {
      return {
        output: `Error: Session subagent limit reached (${context.subagentLimiter.maxLifetime}). ` +
          'Use /clear to reset or set CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION to increase.',
        isError: true,
      };
    }

    const rawModel = input.model as string | undefined;
    const parentModel = context._providerConfig?.model;
    const modelOverride = resolveSubagentModel(rawModel, parentModel);

    if (context._hookManager) {
      // Gate on the SPECIFIC named agent type when one is requested (mirrors
      // Claude Code's Agent(<type>) deny rules), falling back to the tool tier
      // `mode` for a mode-only spawn — so an `Agent(researcher)` deny rule can
      // block exactly that subagent type rather than only the coarse mode.
      const agentTypeForGate = subagentType || mode;
      const agentTypeCheck = await context._hookManager.executePreToolUse?.({
        messages: [], turnCount: 0,
        toolName: 'task',
        toolInput: { subagent_type: agentTypeForGate, prompt: prompt.slice(0, 100) },
      });
      if (agentTypeCheck?.action === 'deny') {
        return { output: `Error: Agent type "${agentTypeForGate}" denied by permission rules.`, isError: true };
      }
    }

    const currentDepth = context._agentDepth ?? 0;
    const maxDepth = context._maxAgentDepth ?? DEFAULT_MAX_AGENT_DEPTH;

    const allTools = createDefaultTools(context.cwd);

    let tools: ToolDefinition[];
    let agentSystemPrompt: string | undefined;
    let agentModel: string | undefined;
    let agentMaxTurns: number | undefined;
    let agentPermissionMode: string | undefined;
    let agentTokenBudget: number | undefined;
    // Per-agent `background: true` default (parity: Claude's agent background flag).
    // Applied only when the caller did not explicitly pass run_in_background.
    let agentBackgroundDefault = false;
    // Per-agent `isolation` default (parity: Claude's agent isolation field) — used
    // when the caller did not pass an explicit `isolation` on the task call.
    let agentIsolationDefault: string | undefined;
    let effectiveMode = mode;

    if (subagentType && context._skillManager) {
      const agentDef = context._skillManager.getAgent(subagentType);
      if (!agentDef) {
        return { output: `Error: agent "${subagentType}" not found. Available agents: ${context._skillManager.listAgents().map(a => a.name).join(', ') || '(none)'}`, isError: true };
      }
      agentSystemPrompt = agentDef.systemPrompt;
      agentModel = agentDef.model;
      agentMaxTurns = agentDef.maxTurns;
      agentPermissionMode = agentDef.permissionMode;
      agentTokenBudget = agentDef.autoCompactWindow;
      agentBackgroundDefault = agentDef.background === true;
      agentIsolationDefault = agentDef.isolation;

      if (agentDef.tools && agentDef.tools.length > 0) {
        const everything = allTools.getAll();
        if (agentDef.tools.includes('*')) {
          // Wildcard allowlist: the agent gets every tool (the baseline denylist
          // below still strips plan_mode/workflow).
          tools = everything;
        } else {
          // Reject an allowlist naming a tool that does not exist — a silent
          // filter-to-nothing hides the typo and strands the agent.
          const known = new Set(everything.map(t => t.name));
          const invalid = agentDef.tools.filter(n => !known.has(n));
          if (invalid.length > 0) {
            return {
              output: `Error: agent "${subagentType}" lists unknown tool(s): ${invalid.join(', ')}. ` +
                `Available tools: ${[...known].sort().join(', ')}.`,
              isError: true,
            };
          }
          const allowed = new Set(agentDef.tools);
          tools = everything.filter(t => allowed.has(t.name));
        }
        // Deny wins even when an allowlist is set: an agent may declare BOTH
        // `tools` and `disallowedTools` (e.g. Claude's planAgent allows the
        // explore set yet denies edit/write/exit for defense-in-depth). Subtract
        // the denylist from the allowlist result so the two compose instead of
        // the allowlist silently ignoring the denylist.
        if (agentDef.disallowedTools && agentDef.disallowedTools.length > 0) {
          const denied = new Set(agentDef.disallowedTools);
          tools = tools.filter(t => !denied.has(t.name));
        }
        if (!shouldAllowNestedTask(currentDepth, maxDepth)) {
          tools = tools.filter(t => t.name !== 'task');
        }
      } else if (agentDef.disallowedTools && agentDef.disallowedTools.length > 0) {
        const disallowed = new Set(agentDef.disallowedTools);
        tools = allTools.getAll().filter(t => !disallowed.has(t.name));
        if (!shouldAllowNestedTask(currentDepth, maxDepth)) {
          tools = tools.filter(t => t.name !== 'task');
        }
      } else {
        tools = filterToolsForAgent(allTools.getAll(), 'general', currentDepth, maxDepth);
      }
      effectiveMode = 'general';
    } else if (mode === 'explore') {
      tools = filterToolsForAgent(allTools.getAll(), 'explore', currentDepth, maxDepth);
    } else if (mode === 'coordinator') {
      tools = filterToolsForAgent(allTools.getAll(), 'coordinator', currentDepth, maxDepth);
    } else if (mode === 'fork') {
      tools = filterToolsForAgent(allTools.getAll(), 'general', currentDepth, maxDepth);
      effectiveMode = 'general';
    } else {
      tools = filterToolsForAgent(allTools.getAll(), 'general', currentDepth, maxDepth);
    }

    // Universal baseline denylist — applied last so no mode or explicit
    // allowlist (incl. `['*']`) can reintroduce plan_mode or workflow.
    tools = tools.filter(t => !ALL_AGENT_DISALLOWED_TOOLS.has(t.name));

    // Providers often serialize booleans as the STRING "true"/"false" in tool-call
    // args; coerceBool handles both (a strict `=== true` silently dropped
    // `run_in_background: "true"` to foreground, serializing a batch the model
    // meant to run concurrently). Shared with bash.ts for consistent coercion.
    // An explicit run_in_background (even "false") wins; otherwise fall back to the
    // agent definition's `background: true` default (parity: Claude's agent background).
    const runInBackground = input.run_in_background !== undefined
      ? coerceBool(input.run_in_background)
      : agentBackgroundDefault;

    // Async/background allowlist — a detached background agent has no live parent
    // turn supervising it, so after all mode/agent-def/denylist resolution its tool
    // set is further INTERSECTED with ASYNC_AGENT_ALLOWED_TOOLS (mirrors Claude's
    // isAsync → ASYNC_AGENT_ALLOWED_TOOLS gate). This strips recursion-prone (`task`)
    // and interactive/stateful tools even when a `['*']` allowlist widened the set.
    if (runInBackground) {
      tools = filterToolsForAsyncAgent(tools);
    }

    // Boundary guard: a custom agent whose `tools` allowlist resolves to zero
    // available tools — every name offline/typo'd, or an allowlist fully cancelled
    // by its denylist or the universal floor — would otherwise spawn a subagent
    // that can take no action: an opaque, hard-to-diagnose stall. Fail fast with an
    // explicit error instead of silently launching a zero-tool agent (mirrors
    // Claude's non-empty selected-tools boundary guard).
    if (tools.length === 0) {
      return {
        output: `Error: the resolved tool set for this subagent is empty — it would be unable to act. ` +
          (subagentType
            ? `Check agent "${subagentType}"'s tools/disallowedTools lists`
            : 'Check the requested tool restrictions') +
          ` (tool names must match registered tools and must not all be baseline-denied: ${[...ALL_AGENT_DISALLOWED_TOOLS].join(', ')}).`,
        isError: true,
      };
    }

    // A direct per-call `max_turns` override caps any mode-only general/explore/
    // fork subagent (mirrors Claude's runForkedAgent maxTurns parameter). It wins
    // over an agent definition's maxTurns AND the fork default below, so an ad-hoc
    // delegation can bound its API round-trips without a named agent definition.
    const requestedMaxTurns = parsePositiveIntInput(input.max_turns);
    if (requestedMaxTurns !== undefined) {
      agentMaxTurns = requestedMaxTurns;
    }

    // A `fork` child continues the parent's full task as a context-inheriting
    // clone, so it needs a generous turn budget. When no agent definition and no
    // explicit max_turns set one, default to DEFAULT_FORK_MAX_TURNS (mirrors
    // Claude's FORK_AGENT).
    if (mode === 'fork' && agentMaxTurns === undefined) {
      agentMaxTurns = DEFAULT_FORK_MAX_TURNS;
    }

    const subAbort = new AbortController();
    const forwardAbort = () => subAbort.abort();
    if (!runInBackground) {
      context.abortSignal.addEventListener('abort', forwardAbort, { once: true });
    }

    // An explicit per-call `isolation` wins; otherwise fall back to the agent
    // definition's `isolation` default (parity: Claude's agent isolation field).
    const isolation = (input.isolation as string | undefined) ?? agentIsolationDefault;
    const requestedCwd = input.cwd as string | undefined;
    if (requestedCwd) {
      // The subagent's working directory is bounded by the PARENT's trusted
      // permission context — a model-chosen cwd cannot escape it.
      const cwdError = validateSubagentCwd(requestedCwd, context.cwd, context.allowedReadRoots ?? []);
      if (cwdError) {
        return { output: `Error: ${cwdError}`, isError: true };
      }
    }
    let effectiveCwd = requestedCwd || context.cwd;
    if (isolation === 'worktree') {
      try {
        const { createWorktreeSession, symlinkLargeDirectories, copyWorktreeIncludes } = require('../worktree-manager');
        const slug = `agent-${crypto.randomBytes(4).toString('hex')}`;
        const session = createWorktreeSession(effectiveCwd, slug);
        symlinkLargeDirectories(effectiveCwd, session.worktreePath);
        copyWorktreeIncludes(effectiveCwd, session.worktreePath);
        effectiveCwd = session.worktreePath;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        return { output: `Error: Failed to create worktree for isolated subagent: ${msg}`, isError: true };
      }
    }

    const providerConfig = { ...(context._providerConfig || { baseUrl: '', apiKey: '', model: '' }) };
    const envSubagentModel = process.env.SUPERINFERENCE_SUBAGENT_MODEL;
    // SUPERINFERENCE_SUBAGENT_MODEL is a HARD, highest-priority operator override
    // (mirrors Claude Code's CLAUDE_CODE_SUBAGENT_MODEL, which is checked first and
    // returns immediately): it beats a per-call `model` arg, an agent definition's
    // pinned model, AND the `inherit` sentinel, so an operator policy forcing every
    // subagent onto a cheap/local model cannot be silently overridden by a model
    // named in a tool call or an on-disk agent file.
    if (envSubagentModel) {
      providerConfig.model = resolveModelAlias(envSubagentModel) ?? envSubagentModel;
    } else if (modelOverride) {
      providerConfig.model = modelOverride;
    } else if (agentModel) {
      providerConfig.model = agentModel;
    }

    // Permission mode priority:
    // 1. Explicit `permission_mode` input (caller sets the spawned agent's mode,
    //    e.g. "plan" / "acceptEdits"; validated against PERMISSION_MODES above)
    // 2. Agent definition's permissionMode (e.g. verifier → auto-allow; safe, restricted tools)
    // 3. explore mode → always auto-allow (read-only tool set)
    // 4. Fallback → 'ask' (uses parent's permissionPromptHandler for TUI/YOLO propagation)
    const resolvedPermissionMode =
      requestedPermissionMode ||
      agentPermissionMode ||
      (effectiveMode === 'explore' ? 'auto-allow' : 'ask');

    // ui#13: a mode-only sub-agent (no named subagent_type) runs on a dedicated LEAN
    // system prompt that REPLACES the full coding stance, so it returns its result and
    // STOPS instead of over-working. `fork` is exempt (an explicit context-inheriting
    // clone that should behave like the parent); a named agent keeps agentSystemPrompt.
    // Use the RAW `mode` (not effectiveMode, which fork reassigns to 'general'): fork is
    // the documented context-inheriting clone and keeps the full stance + directive staple.
    const modeOnlySystemPrompt = (!agentSystemPrompt && mode !== 'fork')
      ? (mode === 'explore'
        ? EXPLORE_SUBAGENT_SYSTEM_PROMPT
        : mode === 'coordinator'
          ? COORDINATOR_SUBAGENT_SYSTEM_PROMPT
          : SUBAGENT_SYSTEM_PROMPT)
      : undefined;

    // The subagent's base system prompt (named-agent identity or the lean
    // mode-only stance); undefined for `fork`, which inherits the parent's full
    // stance assembled by the engine (no string base here to replace/append to).
    const baseSystemPrompt = agentSystemPrompt || modeOnlySystemPrompt;

    // Per-spawn `system_prompt` knob (mirrors Claude Code's config.systemPrompt +
    // systemPromptMode): layer a one-off inline persona on a spawn WITHOUT a
    // persisted agent file. "replace"/"default" fully replaces the base; "append"
    // adds it after the base. Append needs a string base to build on — `fork` has
    // none (its stance is assembled by the child engine), so append is rejected
    // there rather than silently replacing the inherited stance.
    let finalSystemPrompt = baseSystemPrompt;
    if (customSystemPrompt) {
      if (systemPromptMode === 'append') {
        if (!baseSystemPrompt) {
          return {
            output: 'Error: system_prompt_mode "append" has no base system prompt to append to in ' +
              'fork mode (the fork inherits the parent\'s full stance). Use mode "explore"/"general", a ' +
              'subagent_type, or system_prompt_mode "replace".',
            isError: true,
          };
        }
        finalSystemPrompt = `${baseSystemPrompt}\n\n${customSystemPrompt}`;
      } else {
        finalSystemPrompt = customSystemPrompt;
      }
    }

    const childDepth = getSubagentDepthContext(currentDepth, maxDepth);
    const subConfig: EngineConfig = {
      provider: providerConfig,
      cwd: effectiveCwd,
      tools,
      sessionId: resumeSessionId ?? SessionManager.newId(),
      permissionMode: resolvedPermissionMode,
      permissionRules: context._parentPermissionRules,
      permissionPromptHandler: context._permissionPromptHandler,
      abortController: subAbort,
      maxTurns: agentMaxTurns,
      detachedMode: !!context.detachedMode,
      agentDepth: childDepth._agentDepth,
      maxAgentDepth: childDepth._maxAgentDepth,
      // Thinking is DISABLED by default for non-fork subagents to control
      // output-token cost across a fleet of delegations (mirrors Claude's runAgent,
      // which sets thinkingConfig {type:'disabled'} for regular sub-agents and only
      // inherits the parent's thinking on the fork/useExactTools path). An explicit
      // `effort` override always re-enables it; `fork` inherits the parent's config
      // as a context-continuing clone. The explicit disabled object (not undefined)
      // is required so the child engine's reasoning-model auto-enable does not turn
      // thinking back on.
      thinking: effortOverride
        ? { enabled: true, level: effortOverride as import('../model-capabilities').ThinkingLevel }
        : mode === 'fork'
          ? (context._parentThinking || undefined)
          : { enabled: false, level: 'off', type: 'disabled' as const },
      // When a named agent type is used, its system prompt is the authoritative
      // identity — skip stance-skill resolution (which would inject e.g. the
      // swebench stance workflow into a verification-only agent).
      systemPrompt: finalSystemPrompt || undefined,
      tokenBudget: agentTokenBudget || undefined,
    };

    if (!context._engineFactory) {
      return { output: 'Error: engine factory not available in this context.', isError: true };
    }

    // agentSystemPrompt is now in subConfig.systemPrompt — the user message is
    // just the task prompt. ui#13: general/explore mode-only sub-agents now carry the
    // return-and-STOP framing in a dedicated LEAN *system* prompt (modeOnlySystemPrompt)
    // that replaces the full coding stance — so the user-message directive is redundant
    // for them and is dropped (the system role is authoritative; stapling it to a
    // user-message that the full stance overrode was the original, ineffective design).
    // Only `fork` (no agentSystemPrompt, no lean prompt — it inherits the full stance as
    // a clone) still needs the user-message directive to bound it. A named subagent_type
    // keeps its own authoritative identity (no directive).
    // A subagent that has an authoritative system prompt (named-agent, lean
    // mode-only, or a per-spawn custom system_prompt) does not need the
    // user-message return directive. Only `fork` with no custom system_prompt —
    // which inherits the full stance as a clone — still needs it to bound it.
    let effectivePrompt = finalSystemPrompt
      ? prompt
      : `${SUBAGENT_RETURN_DIRECTIVE}\n\n---\n\n${prompt}`;

    // Caller-specified thoroughness dial for explore-mode agents: turns the
    // explore prompt's abstract "adapt to the requested thoroughness" into a
    // concrete, caller-chosen search depth (mirrors Claude Code's exploreAgent
    // thoroughness parameter). Only meaningful when exploring.
    if (thoroughness && mode === 'explore') {
      effectivePrompt = `${buildThoroughnessDirective(thoroughness)}\n\n---\n\n${effectivePrompt}`;
    }

    // When the subagent runs in an isolated worktree, prepend a notice so it
    // translates inherited paths to the worktree root, re-reads stale files, and
    // knows its changes are isolated (mirrors Claude's buildWorktreeNotice).
    if (isolation === 'worktree' && effectiveCwd !== context.cwd) {
      effectivePrompt = `${buildWorktreeNotice(context.cwd, effectiveCwd)}\n\n---\n\n${effectivePrompt}`;
    }

    // Sanitize the caller-supplied name against reserved routing characters
    // (`@` identity separator, `*` broadcast wildcard) and THEN reconcile it to a
    // unique one BEFORE registering it, so a reused base name does not silently
    // overwrite an earlier agent's entry and a routing-char name cannot hijack
    // addressing. A name that was ENTIRELY reserved chars sanitizes to empty →
    // fall back to no name rather than register a degenerate key.
    const rawAgentName = input.name as string | undefined;
    const sanitizedName = rawAgentName ? sanitizeAgentName(rawAgentName) : undefined;
    const agentName = sanitizedName ? uniqueAgentName(sanitizedName) : undefined;

    // A named agent spawned while a team is active joins that team as a member
    // (see registerTeamMember below). Tell it HOW to collaborate: it is one agent
    // in a team, it reaches teammates via send_message, a plain-text reply is
    // invisible to them, and the user talks mainly to the lead (mirrors Claude
    // Code's teammatePromptAddendum). Without this the machinery exists but the
    // teammate is never told it can/should message its peers.
    if (agentName && getCurrentTeam()) {
      effectivePrompt = `${TEAMMATE_PROMPT_ADDENDUM}\n\n---\n\n${effectivePrompt}`;
    }

    if (runInBackground) {
      incrementSpawnCount(context);
      context.subagentLimiter?.recordSpawn();
      const taskId = generateTaskId();
      if (agentName) {
        agentNameRegistry.set(agentName, taskId);
      }
      if (subConfig.sessionId) agentSessionRegistry.set(taskId, subConfig.sessionId);
      // If a team is active, a named teammate joins it as an active member so the
      // team-delete guard can see it and refuse to disband while it runs.
      if (agentName && getCurrentTeam()) {
        registerTeamMember({ name: agentName, agentId: taskId, agentType: subagentType, model: subConfig.provider?.model, cwd: effectiveCwd });
      }
      const tasksDir = projectStateDir(context.cwd, 'tasks');
      fs.mkdirSync(tasksDir, { recursive: true });
      const outputPath = path.join(tasksDir, `${taskId}.output`);
      fs.writeFileSync(outputPath, '');

      if (context.processManager) {
        const label = taskDescription || `${prompt.slice(0, 80)}`;
        context.processManager.registerAgent(taskId, {
          command: `[agent] ${label}`,
          description: `Background agent: ${label}`,
          outputPath,
          abortFn: () => { subAbort.abort(); },
        });
        const fm = context.processManager.flowManager;
        if (fm) {
          const flowId = fm.getOrCreateActiveFlow(label);
          fm.addAgent(flowId, taskId);
        }
      }

      const subEngine = context._engineFactory(subConfig);
      (async () => {
        let result = '';
        let sawError = false;
        const fd = fs.openSync(outputPath, 'w');
        const w = (s: string) => { try { fs.writeSync(fd, s); } catch { /* fd closed */ } };
        const bgStallMs = parsePositiveIntEnv(process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS, SUBAGENT_STALL_TIMEOUT_MS);
        let bgStallTimer: ReturnType<typeof setTimeout> | null = null;
        let bgStalled = false;
        const resetBgStall = () => {
          if (bgStallTimer) clearTimeout(bgStallTimer);
          bgStallTimer = setTimeout(() => { bgStalled = true; subEngine.shutdown?.(); }, bgStallMs);
        };
        const clearBgStall = () => { if (bgStallTimer) { clearTimeout(bgStallTimer); bgStallTimer = null; } };
        resetBgStall();
        try {
          for await (const event of subEngine.submit(effectivePrompt)) {
            resetBgStall();
            if (bgStalled) break;
            if (event.type === 'thinking_delta') {
              w(event.text);
            }
            if (event.type === 'text_delta') {
              result += event.text;
              w(event.text);
            }
            if (event.type === 'tool_use_start') {
              const summary = typeof event.input?.command === 'string'
                ? event.input.command
                : typeof event.input?.file_path === 'string'
                  ? (event.input.file_path as string)
                  : JSON.stringify(event.input || {}).slice(0, 120);
              w(`\n▸ ${event.toolName} ${summary}\n`);
            }
            if (event.type === 'tool_use_progress') {
              w(event.data || '');
            }
            if (event.type === 'tool_use_result') {
              const lines = event.output.split('\n');
              const preview = lines.slice(0, 20).join('\n');
              const suffix = lines.length > 20 ? `\n  ... (${lines.length - 20} more lines)` : '';
              w(`${preview}${suffix}\n`);
            }
            if (event.type === 'usage_update') {
              if (context.processManager) {
                context.processManager.updateEntry(taskId, {
                  tokenCount: event.stats?.totalTokens,
                });
              }
            }
            if (event.type === 'error') {
              sawError = true;
              const errText = `\nError: ${event.error}`;
              result += errText;
              w(errText);
            }
          }
          if (bgStalled) {
            sawError = true;
            const msg = `\n[Subagent stalled — no events for ${Math.round(bgStallMs / 1000)}s, terminated]`;
            result += msg;
            w(msg);
          }
        } catch (err) {
          sawError = true;
          const errText = `\nSubagent error: ${err instanceof Error ? err.message : String(err)}`;
          result += errText;
          w(errText);
        } finally {
          clearBgStall();
          subEngine.shutdown?.();
        }
        try { fs.closeSync(fd); } catch { /* already closed */ }
        if (!result) {
          try { fs.writeFileSync(outputPath, '(subagent produced no output)'); } catch { /* dir removed */ }
        }
        // The teammate has finished — mark it idle so team_delete no longer
        // treats it as an active member blocking teardown.
        setTeamMemberActive(taskId, false);
        if (context.processManager) {
          const label = taskDescription || `${prompt.slice(0, 80)}`;
          context.processManager.updateEntry(taskId, {
            status: sawError ? 'failed' : 'completed',
            exitCode: sawError ? 1 : 0,
          });
          context.processManager.emit('complete', { taskId, exitCode: sawError ? 1 : 0, command: `[agent] ${label}`, description: `Background agent: ${label}` });
        }
      })().catch(() => {});

      return { output: `Background agent started: ${taskId}\nTo collect its result, call task_output({ task_id: "${taskId}", wait: true }) — this blocks until it finishes and returns in one call. Do NOT poll in a loop. When spawning several agents, issue all the task_output(wait) calls in a single turn so they run concurrently.\nDo not duplicate this agent's work — avoid working with the same files or topics it is using. Work on non-overlapping tasks, or briefly tell the user what you launched and end your response.` };
    }

    incrementSpawnCount(context);
    context.subagentLimiter?.recordSpawn();
    const subEngine = context._engineFactory(subConfig);
    const taskId = generateTaskId();
    if (agentName) {
      agentNameRegistry.set(agentName, taskId);
    }
    if (subConfig.sessionId) agentSessionRegistry.set(taskId, subConfig.sessionId);
    if (agentName && getCurrentTeam()) {
      registerTeamMember({ name: agentName, agentId: taskId, agentType: subagentType, model: subConfig.provider?.model, cwd: effectiveCwd });
    }
    let resultText = '';

    let effectivePromptForSubmit = effectivePrompt;
    if (mode === 'fork' && context._parentMessages && context._parentMessages.length > 0) {
      // Drop a trailing in-progress/interrupted assistant turn before forking so a
      // corrupt half-answer (the fork can be requested mid-turn) does not leak into
      // the child's context (mirrors Claude's buildSideQuestionFallbackParams).
      const contextSummary = stripIncompleteTrailingMessages(context._parentMessages)
        .filter(m => m.role === 'assistant' && typeof m.content === 'string')
        .map(m => (m.content as string).slice(0, 500))
        .join('\n---\n')
        .slice(0, 3000);
      if (contextSummary) {
        effectivePromptForSubmit = `[Parent conversation context]\n${contextSummary}\n\n[New task]\n${effectivePromptForSubmit}`;
      }
    }

    if (context._hookManager) {
      context._hookManager.executeSubagentStart({ parentSessionId: taskId, subagentSessionId: subConfig.sessionId || taskId, prompt, mode: effectiveMode }).catch(() => {});
    }

    // Surface this foreground subagent live to the parent engine's UI.
    const relayDepth = childDepth._agentDepth;
    context._emitChild?.({
      type: 'subagent_start', nodeId: taskId, depth: relayDepth,
      agentName: agentName || undefined, model: subConfig.provider?.model, prompt,
    });

    let toolUseCount = 0;
    let totalTokens = 0;
    const startedAt = Date.now();
    const recentActivities: string[] = [];
    let exitReason: import('../types').ExitReason | undefined;
    let sawAbortError = false;
    // The text of the sub-agent's most recent text-bearing assistant turn. Tracking
    // the last `turn_complete` that carried text performs Claude's finalizeAgentTool
    // backward-walk incrementally, so the returned deliverable is the clean FINAL
    // answer, not every intermediate narration delta concatenated across the run.
    let lastTurnText: string | undefined;
    // The chosen deliverable (final turn text, else accumulated fallback) plus any
    // status markers; assembled after the loop and returned to the parent.
    let deliverable = '';

    const stallTimeoutMs = parsePositiveIntEnv(process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS, SUBAGENT_STALL_TIMEOUT_MS);
    let stallTimer: ReturnType<typeof setTimeout> | null = null;
    let stalled = false;
    const resetStallTimer = () => {
      if (stallTimer) clearTimeout(stallTimer);
      stallTimer = setTimeout(() => {
        stalled = true;
        subAbort.abort('subagent stalled — no events for ' + Math.round(stallTimeoutMs / 1000) + 's');
      }, stallTimeoutMs);
    };
    const clearStallTimer = () => { if (stallTimer) { clearTimeout(stallTimer); stallTimer = null; } };
    resetStallTimer();

    try {
      for await (const event of subEngine.submit(effectivePromptForSubmit)) {
        resetStallTimer();
        if (stalled) break;
        const relayed = mapChildToSubagentEvent(event, taskId, relayDepth);
        if (relayed) context._emitChild?.(relayed);
        if (event.type === 'text_delta') {
          resultText += event.text;
        }
        if (event.type === 'error') {
          resultText += `\nError: ${event.error}`;
          if (/\baborted\b/i.test(event.error)) sawAbortError = true;
        }
        if (event.type === 'tool_use_start') {
          toolUseCount++;
          recentActivities.push(`tool:${event.toolName}`);
          if (recentActivities.length > 5) recentActivities.shift();
        }
        if (event.type === 'usage_update') {
          totalTokens = event.stats?.totalTokens ?? totalTokens;
        }
        // The deliverable is the FINAL assistant turn's text (agentToolUtils
        // finalizeAgentTool): record every text-bearing turn so the last one wins,
        // and a trailing pure-tool_use turn (empty/null content) falls back to it.
        if (event.type === 'turn_complete' && typeof event.content === 'string' && event.content.trim().length > 0) {
          lastTurnText = event.content;
        }
        if (event.type === 'done') {
          exitReason = event.exitReason;
        }
      }
      // Choose the clean final answer (last text turn, else accumulated fallback),
      // then append any run-status markers to IT so the parent gets the deliverable
      // plus the truncation notice — not the intermediate narration.
      deliverable = selectFinalAnswer(lastTurnText, resultText);
      if (stalled) {
        deliverable += `\n[Subagent timed out — no activity for ${Math.round(stallTimeoutMs / 1000)}s]`;
      } else if (exitReason === 'max_turns') {
        // Distinguish a turn-capped partial result from a clean completion so the
        // parent/UI does not mistake a truncated run for a finished one.
        deliverable += `\n[Subagent stopped — reached max turns limit${agentMaxTurns ? ` (${agentMaxTurns})` : ''}]`;
      }
    } catch (err) {
      context.abortSignal.removeEventListener('abort', forwardAbort);
      if (context._hookManager) {
        context._hookManager.executeSubagentStop({ parentSessionId: taskId, subagentSessionId: subConfig.sessionId || taskId, prompt, mode: effectiveMode }).catch(() => {});
      }
      const errMsg = err instanceof Error ? err.message : String(err);
      const output = resultText.trim()
        ? `[Partial output before error]\n\n${resultText.trim()}\n\n[Error]\n${errMsg}`
        : `Subagent error: ${errMsg}`;
      context._emitChild?.({
        type: 'subagent_result', nodeId: taskId, depth: relayDepth,
        success: false, summary: errMsg.split('\n')[0].slice(0, 120), tokens: totalTokens,
      });
      return {
        output,
        isError: true,
      };
    } finally {
      clearStallTimer();
      subEngine.shutdown?.();
      // Foreground teammate finished (success or error) — mark it idle so it no
      // longer blocks team_delete.
      setTeamMemberActive(taskId, false);
    }
    context.abortSignal.removeEventListener('abort', forwardAbort);

    if (context._hookManager) {
      context._hookManager.executeSubagentStop({ parentSessionId: taskId, subagentSessionId: subConfig.sessionId || taskId, prompt, mode: effectiveMode }).catch(() => {});
    }

    if (isolation === 'worktree' && effectiveCwd !== context.cwd) {
      try {
        // Shell-free: pass the worktree path as an argv element to git, never
        // interpolated into a shell command line. A worktree path with a space
        // or shell metacharacter would otherwise let `execSync` run arbitrary
        // commands during cleanup. GIT_HARDENING_ARGS additionally neutralizes
        // config-driven code-execution vectors (fsmonitor/hooks/external-diff),
        // since the worktree's `.git/config` is not a trusted input.
        const status = child_process.execFileSync('git', [...GIT_HARDENING_ARGS, 'status', '--porcelain'], { cwd: effectiveCwd, encoding: 'utf-8', timeout: 5000 }).trim();
        const unpushed = child_process.execFileSync('git', [...GIT_HARDENING_ARGS, 'rev-list', 'HEAD', '--not', '--remotes'], { cwd: effectiveCwd, encoding: 'utf-8', timeout: 5000 }).trim();
        const runningAgents = context.processManager ? context.processManager.runningAgentCount() : 0;
        if (shouldRemoveWorktree(status, unpushed, runningAgents)) {
          child_process.execFileSync('git', [...GIT_HARDENING_ARGS, 'worktree', 'remove', '--force', effectiveCwd], { cwd: context.cwd, timeout: 10000 });
          deliverable += '\n[Agent worktree cleaned up — no changes detected]';
        }
      } catch {}
    }

    // Cap the subagent transcript to the configured budget (spills overflow to
    // disk), so a chatty subagent cannot blow up the parent's context.
    const taskMaxChars = context.outputBudget?.taskMaxChars;
    const cappedResult = taskMaxChars && taskMaxChars > 0
      ? spillToolText(context.cwd, 'task', deliverable, taskMaxChars)
      : deliverable;

    // A stall-kill or an abort is a FAILURE, not a successful result — surface it
    // so the parent does not treat a cancelled/timed-out run as a finished one
    // (mirrors Claude's AbortError-on-abort semantics).
    const failed = stalled || exitReason === 'abort' || sawAbortError;

    context._emitChild?.({
      type: 'subagent_result', nodeId: taskId, depth: relayDepth,
      success: !failed, summary: (deliverable.trim().split('\n')[0] || 'done').slice(0, 120), tokens: totalTokens,
    });
    const durationS = ((Date.now() - startedAt) / 1000).toFixed(1);
    // A metadata-only tail (empty body + stats) reads as "nothing to act on" to
    // some models and ends the parent turn prematurely — substitute an explicit
    // no-output marker (mirrors Claude's "(Subagent completed but returned no output.)").
    const body = cappedResult.trim() ? cappedResult : '(Subagent completed but returned no output.)';
    // Continue-affordance: tell the parent it CAN message/resume this agent to
    // carry the work forward. Skipped for one-shot `explore` runs (read-only
    // reports that are never continued), mirroring Claude's ONE_SHOT agent types.
    const continueHint = effectiveMode === 'explore'
      ? ''
      : `\nTo continue this agent, call task({ resume: "${agentName || taskId}", prompt: ... }).`;
    return {
      output: `${body}\n\n[Agent stats: ${toolUseCount} tool calls, ${totalTokens} tokens, ${durationS}s]${continueHint}`,
      ...(failed ? { isError: true } : {}),
    };
  },
};
