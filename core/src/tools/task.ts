import * as fs from 'fs';
import { projectStateDir } from '../reference-journal';
import * as path from 'path';
import * as crypto from 'crypto';
import * as child_process from 'child_process';
import { ToolDefinition, ToolContext, ToolResult, EngineConfig, ToolInputSchema, JsonSchemaProperty, Message } from '../types';
import { createDefaultTools } from './index';
import { releaseFileCache } from '../file-cache';
import { coerceBool } from '../utils/coerce';
import { isEnvTruthy } from '../analytics';
import { spillToolText } from './spill-output';
import { mapChildToSubagentEvent } from './subagent-relay';
import { getCurrentTeam, registerTeamMember, setTeamMemberActive, teamAllowedPathsToRules } from './team';
import { isTeammatesEnabled } from './tool-gates';
import { SessionManager } from '../session';
import { PERMISSION_MODES, permissionRuleValueFromString } from '../permissions';
import { isModelFamilyAlias, resolveModelAlias, getDefaultSubagentModel } from '../model-registry';
import type { ProviderConfig } from '../types';
import { validateBoundedIntEnvVar } from '../config';
import { buildPreloadedSkillsBlock, SYSTEM_PROMPT_MODES, type SystemPromptMode, parseAgentColor, AGENT_COLORS, type AgentColorName } from '../skills';

/**
 * Open flags for a sub-agent's output-capture file. `O_NOFOLLOW` refuses to open
 * the file if the predictable `<taskId>.output` path has been replaced by a
 * symlink (parity: Claude Code's `O_WRONLY | O_CREAT | O_APPEND | O_NOFOLLOW` on
 * command-output fds — "to prevent symlink-following attacks from the sandbox").
 * A sandboxed/malicious sub-agent could otherwise pre-plant a symlink at that
 * path and redirect the captured output into an attacker-chosen file (e.g.
 * `~/.bashrc`, a git `HEAD`); with `O_NOFOLLOW` the open throws `ELOOP` instead.
 * `O_TRUNC` preserves the "start from empty" semantics of the old `'w'` flag /
 * `writeFileSync(path, '')` it replaces. Exported for the symlink-guard test.
 */
export const OUTPUT_FILE_OPEN_FLAGS =
  fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_TRUNC | fs.constants.O_NOFOLLOW;

/** Auto-background threshold for foreground agents exceeding this duration. */
const AUTO_BG_MS = 120_000;

/** Master env knob that enables auto-backgrounding of long-running foreground
 *  subagents (parity: Claude's `CLAUDE_AUTO_BACKGROUND_TASKS`). */
export const AUTO_BACKGROUND_TASKS_ENV = 'AMI_AUTO_BACKGROUND_TASKS';
/** Optional override of the auto-background threshold in ms (default AUTO_BG_MS). */
export const AUTO_BACKGROUND_MS_ENV = 'AMI_AUTO_BACKGROUND_MS';

/**
 * Resolve the auto-background threshold in milliseconds, or `0` when disabled.
 *
 * Mirrors Claude Code's `getAutoBackgroundMs()` (AgentTool.tsx:72-77): a
 * foreground subagent that is still actively working past this threshold is
 * automatically promoted to a background task so the parent turn is never blocked
 * indefinitely (AMI's only other bound is the 10-min *inactivity* stall timeout,
 * which never fires for an agent that keeps emitting events). Disabled by default
 * (returns 0); set {@link AUTO_BACKGROUND_TASKS_ENV} truthy to enable the 120s
 * default, and optionally {@link AUTO_BACKGROUND_MS_ENV} to override the duration
 * (positive-int only — a non-positive/garbage override falls back to AUTO_BG_MS).
 */
export function getAutoBackgroundMs(env: NodeJS.ProcessEnv = process.env): number {
  if (!isEnvTruthy(env[AUTO_BACKGROUND_TASKS_ENV])) return 0;
  return parsePositiveIntEnv(env[AUTO_BACKGROUND_MS_ENV], AUTO_BG_MS);
}

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
  // Delegate to the canonical bounded-int parser (no upper cap, positive-only) so
  // there is a single implementation of "parse a positive-integer env value" —
  // the name is omitted so this thin wrapper stays silent (no debug log).
  return validateBoundedIntEnvVar('', raw, { default: fallback, min: 1 }).effective;
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
 * Recursion guard directive stapled into a `fork` child's first user turn
 * (mirrors Claude Code's fork boilerplate rule #1 in `buildChildMessage`,
 * forkSubagent.ts:L178). A fork child inherits the parent's full coding stance —
 * which tells the model to "default to delegating/forking" — AND, at depth 0,
 * keeps the `task` tool in its pool. Without an explicit counter-instruction a
 * fork child would re-fork recursively (an unsupervised fan-out) instead of doing
 * the work itself. AMI's only other bound is the numeric nesting-depth cap; this
 * is the call-time behavioral guard independent of depth. The child is told
 * plainly: you ARE the fork — execute the work directly, do not spawn sub-agents.
 */
export const FORK_NO_SUBSPAWN_DIRECTIVE =
  'You ARE the fork — a worker continuing the parent conversation directly. Your inherited system prompt may say to delegate or fork work; IGNORE that here — it is for the parent. Do NOT spawn sub-agents (no nested fork or task delegation); execute the work yourself with your own tools and report once when you are done.';

/**
 * Structured worker-discipline contract stapled into a `fork` child's first user
 * turn, ALONGSIDE {@link FORK_NO_SUBSPAWN_DIRECTIVE} (which owns the no-subspawn
 * rule — this constant deliberately omits it to avoid duplicating that guidance).
 * Mirrors the non-negotiable worker rules + fixed plain-text report schema of
 * Claude Code's `buildChildMessage` (forkSubagent.ts:L171-198): begin the reply
 * with `Scope:`, no preamble/meta-commentary, no text emitted between tool calls,
 * commit modified files and include the commit hash in the report, stay strictly
 * within scope, cap at ~500 words, and report-then-stop in a fixed label format
 * (Scope / Result / Key files / Files changed+hash / Issues). Without this a fork
 * child — which inherits the full coding stance — returns free-form prose with no
 * machine-/parent-parseable structure and no commit-hash provenance.
 */
export const FORK_WORKER_DIRECTIVE =
  'You are a forked worker continuing the parent\'s task directly. Worker rules (non-negotiable):\n' +
  '- Do NOT converse, ask questions, suggest next steps, or add meta-commentary.\n' +
  '- Use your tools directly and silently; do NOT emit text between tool calls — act, then report once at the end.\n' +
  '- If you modify files, commit your changes before reporting and include the commit hash in the report.\n' +
  '- Stay strictly within your assigned scope; if you notice related work outside it, mention it in at most one sentence.\n' +
  '- Keep your report under 500 words. Be factual and concise, then stop.\n' +
  'Your response MUST begin with "Scope:" (no preamble, no thinking-out-loud) and use this exact plain-text label format:\n' +
  '  Scope: <echo your assigned scope in one sentence>\n' +
  '  Result: <the answer or key findings, limited to the scope above>\n' +
  '  Key files: <relevant file paths — include for research tasks>\n' +
  '  Files changed: <list with commit hash — include only if you modified files>\n' +
  '  Issues: <list — include only if there are issues to flag>';

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
  `You are a fast, read-only exploration sub-agent handling one search/investigation task. You are meant to return a focused answer as quickly as possible.

=== READ-ONLY MODE — NO FILE MODIFICATIONS ===
This is a strictly read-only task. You are PROHIBITED from: creating files (no write, touch, or file creation of any kind); modifying files (no edits); deleting, moving, or copying files (no rm/mv/cp); creating temporary files anywhere (including /tmp); using redirect operators (>, >>) or heredocs to write to files; and running ANY command that changes system state (mkdir, git add, git commit, npm/pip install, …). Your role is exclusively to search and analyze existing code.

- Investigate using read and search tools only. If dedicated glob/grep search tools are unavailable in your tool set, fall back to read-only \`find\` and \`grep\` via bash (never mutating commands) to locate files and search contents.
- Adapt your search breadth to the thoroughness the caller requests ("quick" for a basic lookup, "medium" for moderate exploration, "very thorough" for comprehensive analysis).
- Start broad and narrow down; if one search strategy yields nothing, try another; consider alternate naming conventions and check multiple locations before concluding something does not exist.
- Run at least ${EXPLORE_AGENT_MIN_QUERIES} distinct searches (different strategies, terms, or locations) before concluding something is absent — do not report "not found" after a single miss.
- Be fast: spawn multiple parallel tool calls in one turn when grepping and reading independent files rather than searching one at a time.
- Gather exactly what the task asks for, cite concrete locations (file:line) where relevant, then report your findings directly and concisely as your final message and STOP. Your final message IS the deliverable.
- Do not over-explore beyond what the task asks. One focused report, then stop.`;

const agentNameRegistry = new Map<string, string>();
// taskId -> the sub-engine sessionId it ran under. Lets `resume` recreate an
// agent on its PRIOR session (the Engine reloads saved messages when given a
// known sessionId), so a follow-up prompt continues the same context.
const agentSessionRegistry = new Map<string, string>();
// taskId -> the isolated working directory the agent ran in (an isolated git
// WORKTREE created for an `isolation:"worktree"` spawn, or an explicit `cwd`
// override). Lets `resume` restore the agent to the SAME files it was editing
// instead of silently reverting to the parent cwd and stranding its in-progress
// work. Only populated when the agent ran outside the parent cwd. Mirrors Claude
// Code's per-agent worktree restoration on resume.
const agentCwdRegistry = new Map<string, string>();

/**
 * The identity/defaults a spawned agent ran under, keyed by taskId. Lets a later
 * `resume` restore the ORIGINAL agent's identity (its `subagent_type`, model,
 * permission mode, and description) when the resume call does not re-supply them,
 * instead of silently re-deriving a generic general-purpose agent. Mirrors Claude
 * Code's resumeAgent metadata restore (resumeAgent.ts:L99-114): a resumed agent
 * keeps its agentType/description/model/permissionMode from the persisted record.
 * In-memory (process-lifetime) — cross-process durability is a separate concern.
 */
export interface AgentMeta {
  agentType?: string;
  model?: string;
  permissionMode?: string;
  description?: string;
}
const agentMetaRegistry = new Map<string, AgentMeta>();

/** The recorded identity/defaults a prior agent ran under (for `resume`). */
export function getAgentMeta(taskId: string): AgentMeta | undefined {
  return agentMetaRegistry.get(taskId);
}

/**
 * Record a spawned agent's identity/defaults so a later `resume` can restore them.
 * Only non-empty fields are stored (an undefined field means "the caller set none",
 * which should not shadow a real restore on resume).
 */
export function recordAgentMeta(taskId: string, meta: AgentMeta): void {
  const entry: AgentMeta = {};
  if (meta.agentType) entry.agentType = meta.agentType;
  if (meta.model) entry.model = meta.model;
  if (meta.permissionMode) entry.permissionMode = meta.permissionMode;
  if (meta.description) entry.description = meta.description;
  agentMetaRegistry.set(taskId, entry);
}

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
  agentCwdRegistry.clear();
  agentMetaRegistry.clear();
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

/**
 * Internal worker-coordination tools filtered OUT of the worker-capability
 * affordance shown to a coordinator (mirrors Claude Code's `INTERNAL_WORKER_TOOLS`
 * in `coordinator/coordinatorMode.ts`). These are plumbing the workers use to talk
 * to the team / emit their final result — not capabilities the coordinator should
 * reason about when deciding WHAT concrete work a worker can do. Removing them from
 * the advertised set keeps the affordance focused on actionable read/edit/shell/
 * search tools.
 */
export const INTERNAL_WORKER_TOOLS = new Set([
  'team_create', 'team_delete', 'send_message', 'structured_output',
]);

/**
 * Build the runtime worker-capability affordance injected into a `coordinator`-mode
 * sub-agent's context (mirrors Claude Code's `getCoordinatorUserContext`). A
 * coordinator cannot touch the filesystem/shell itself — it only delegates — so it
 * must know EXACTLY which tools its spawned workers have and which MCP servers they
 * can reach, or it will mis-delegate (e.g. ask a worker to do something no worker
 * tool supports). The worker tool list is the async-agent source-of-truth set
 * ({@link ASYNC_AGENT_ALLOWED_TOOLS}) minus the internal coordination plumbing
 * ({@link INTERNAL_WORKER_TOOLS}), sorted deterministically so the block is stable
 * across spawns (cache-friendly). Connected MCP servers are appended by name when
 * present.
 */
export function buildCoordinatorWorkerContext(mcpServerNames: string[] = []): string {
  const workerTools = [...ASYNC_AGENT_ALLOWED_TOOLS]
    .filter(name => !INTERNAL_WORKER_TOOLS.has(name))
    .sort()
    .join(', ');
  let content = `Workers you spawn with the task tool have access to these tools: ${workerTools}.`;
  if (mcpServerNames.length > 0) {
    content += `\n\nWorkers also have access to MCP tools from these connected MCP servers: ${mcpServerNames.join(', ')}.`;
  }
  return content;
}

/**
 * Names of the currently CONNECTED MCP servers reachable from this tool context,
 * de-duplicated and sorted (locale-aware). Reads the optional MCP manager's
 * `listAllServers()` roster — the same accessor `tool_search` uses — and keeps only
 * entries in the `connected` state (a pending/failed server offers no tools to a
 * worker yet). Returns `[]` when there is no manager, it lacks the accessor, or the
 * call throws, so callers never have to guard. Used to populate the coordinator's
 * worker-capability affordance ({@link buildCoordinatorWorkerContext}).
 */
export function connectedMcpServerNames(context: ToolContext): string[] {
  const mgr = context._mcpManager as { listAllServers?: () => Array<{ name: string; state: string }> } | undefined;
  if (!mgr || typeof mgr.listAllServers !== 'function') return [];
  try {
    const names = mgr.listAllServers()
      .filter(s => s && s.state === 'connected')
      .map(s => s.name);
    return [...new Set(names)].sort((a, b) => a.localeCompare(b));
  } catch {
    return [];
  }
}

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
 * Final subagent-model fallback: return `currentModel` when it names a model, else
 * the provider-aware default from {@link getDefaultSubagentModel}. Used after every
 * higher-priority source (env override → per-call model → agent-def model → explore
 * default → inherited parent model) has had its say, so a subagent spawned with no
 * model configured ANYWHERE runs on a concrete provider-correct fast model instead
 * of dispatching an empty model string. AMI KEEPS its parent-inheritance perk (a
 * non-empty `currentModel` passes through untouched); this only ADDS the hardcoded
 * default for the genuinely-unset case — parity with Claude Code's
 * getHardcodedTeammateModelFallback without regressing the inherit behavior.
 */
export function defaultSubagentModelFallback(
  currentModel: string | undefined,
  providerConfig: ProviderConfig,
): string {
  const trimmed = (currentModel ?? '').trim();
  return trimmed !== '' ? trimmed : getDefaultSubagentModel(providerConfig);
}

/** Operator override for the model an `explore`-mode sub-agent runs on. Set to a
 *  model id/alias to force one, or the `inherit` sentinel to keep the parent's
 *  model (behavior-only knob — mirrors Claude Code's `USER_TYPE==='ant' ? inherit
 *  : haiku` gate on the explore agent's model). */
export const EXPLORE_MODEL_ENV = 'AMI_EXPLORE_MODEL';

/**
 * The model a mode-only `explore` sub-agent should default to when the caller set
 * no explicit model (no per-call `model`, no agent-def model, no
 * SUPERINFERENCE_SUBAGENT_MODEL override). Mirrors Claude Code's explore agent,
 * which pins external runs to the fast/cheap `haiku` model for speed because
 * read-only exploration does not need the main model's capability. Returns the
 * resolved fast model id, or `undefined` to keep the parent's model.
 *
 * Rules:
 *  - {@link EXPLORE_MODEL_ENV} wins when set: `inherit` → keep parent (undefined);
 *    any other value → that model id/alias (alias-resolved).
 *  - A NON-Claude parent (e.g. a GPT/Gemini run) keeps its own model — forcing a
 *    Claude `haiku` onto a different provider's session would be wrong. Only a
 *    Claude-family (or absent) parent is auto-downshifted to the fast model.
 */
export function defaultExploreModel(
  parentModel: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
): string | undefined {
  const override = env[EXPLORE_MODEL_ENV]?.trim();
  if (override) {
    if (override.toLowerCase() === INHERIT_MODEL_SENTINEL) return undefined;
    return resolveModelAlias(override) ?? override;
  }
  if (parentModel && !/claude/i.test(parentModel)) return undefined;
  return resolveModelAlias('haiku');
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
 * Drop assistant messages that carry a tool_call with no matching tool result
 * (orphaned/incomplete tool_use) before the parent conversation is seeded into a
 * `fork` child. A fork can be requested mid-turn, so the trailing assistant turn
 * may hold tool_use blocks whose results have not arrived yet; sending those to the
 * child's provider would raise an orphaned-tool_use API error. Build the set of
 * answered tool-call ids (every `tool` message's `tool_call_id`) and drop any
 * assistant message with an unanswered call, keeping everything else intact.
 * Mirrors Claude Code's `filterIncompleteToolCalls` (runAgent.ts).
 */
export function filterIncompleteToolCalls(messages: Message[]): Message[] {
  const answered = new Set<string>();
  for (const m of messages) {
    if (m.role === 'tool' && m.tool_call_id) answered.add(m.tool_call_id);
  }
  return messages.filter(m => {
    if (m.role === 'assistant' && m.tool_calls && m.tool_calls.length > 0) {
      return !m.tool_calls.some(tc => !answered.has(tc.id));
    }
    return true;
  });
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
 * Wrap a named agent's `criticalSystemReminder` into a reinforced constraint block
 * appended to its authoritative system prompt (mirrors Claude Code's
 * `criticalSystemReminder_EXPERIMENTAL`, verificationAgent.ts:L150-151). A hard
 * constraint ("you CANNOT write files", "you MUST end with VERDICT:") belongs in a
 * short, strongly-flagged reminder distinct from the agent's main prose so the
 * model does not let it drift; this framing makes it conspicuous. Empty/blank
 * reminders produce no block.
 */
export function buildCriticalReminderBlock(reminder: string | undefined): string {
  const trimmed = reminder?.trim();
  return trimmed ? `\n\n[CRITICAL — non-negotiable constraint]\n${trimmed}` : '';
}

/**
 * Prompt-cache introspection clause folded into a foreground sub-agent's
 * completion stats footer (mirrors Claude Code's per-fork cacheRead / cacheHitRate
 * line, forkedAgent.ts:L606-654). A sub-agent that reuses the parent's cached
 * prefix is far cheaper; surfacing how many prompt tokens were served from cache —
 * and the hit rate when known — makes that observable to the parent/model instead
 * of hidden. Returns an empty string when there is no cache signal (so the footer
 * is unchanged for providers/runs that report none).
 */
export function buildCacheStatsClause(cacheReadTokens: number, cacheHitRate: number | undefined): string {
  if (!cacheReadTokens && cacheHitRate === undefined) return '';
  const rate = cacheHitRate !== undefined ? `, ${Math.round(cacheHitRate * 100)}% cache hit` : '';
  return `, ${cacheReadTokens} cached tokens${rate}`;
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
 * Create an isolated git worktree for a sub-agent and prime it: a fresh worktree
 * session off `baseCwd`, large dirs (node_modules) symlinked in, and the
 * configured include-globs copied over. Returns the worktree path the child
 * should run in. Throws on failure (non-git cwd, worktree limit reached, …) so
 * the caller can convert it into its own clean error. Shared single-source setup
 * for both the task tool and the workflow tool's per-agent isolation knob — no
 * second parallel worktree-creation path.
 */
export function setupIsolatedWorktree(baseCwd: string, name: string): string {
  const { createWorktreeSession, symlinkLargeDirectories, copyWorktreeIncludes } = require('../worktree-manager');
  const session = createWorktreeSession(baseCwd, name);
  symlinkLargeDirectories(baseCwd, session.worktreePath);
  copyWorktreeIncludes(baseCwd, session.worktreePath);
  return session.worktreePath;
}

/**
 * Tear down an isolated worktree iff it is change-free (no dirty status, no
 * unpushed commits) and no other agent is still running in it
 * ({@link shouldRemoveWorktree}). Returns true when the worktree was removed.
 * Shell-free (argv-only) and hardened with {@link GIT_HARDENING_ARGS} — the
 * worktree's `.git/config` is untrusted. Best-effort: any git failure is
 * swallowed and reported as "not removed". Shared by the task tool and the
 * workflow tool so the cleanup policy lives in exactly one place.
 */
export function removeWorktreeIfClean(baseCwd: string, worktreePath: string, runningAgentCount: number): boolean {
  try {
    const status = child_process.execFileSync('git', [...GIT_HARDENING_ARGS, 'status', '--porcelain'], { cwd: worktreePath, encoding: 'utf-8', timeout: 5000 }).trim();
    const unpushed = child_process.execFileSync('git', [...GIT_HARDENING_ARGS, 'rev-list', 'HEAD', '--not', '--remotes'], { cwd: worktreePath, encoding: 'utf-8', timeout: 5000 }).trim();
    if (shouldRemoveWorktree(status, unpushed, runningAgentCount)) {
      child_process.execFileSync('git', [...GIT_HARDENING_ARGS, 'worktree', 'remove', '--force', worktreePath], { cwd: baseCwd, timeout: 10000 });
      return true;
    }
  } catch { /* best-effort cleanup */ }
  return false;
}

/**
 * Read the git branch currently checked out in an isolated subagent worktree.
 * Shell-free (argv-only) and hardened with {@link GIT_HARDENING_ARGS} — the
 * worktree's `.git/config` is untrusted. Returns the branch name, or `undefined`
 * on any failure OR a detached HEAD (`rev-parse --abbrev-ref` prints the literal
 * "HEAD" when detached, which is not a branch to report). Used to surface the
 * agent's worktree branch to the model on completion.
 */
export function readWorktreeBranch(worktreePath: string): string | undefined {
  try {
    const branch = child_process.execFileSync(
      'git', [...GIT_HARDENING_ARGS, 'rev-parse', '--abbrev-ref', 'HEAD'],
      { cwd: worktreePath, encoding: 'utf-8', timeout: 5000 },
    ).trim();
    return branch && branch !== 'HEAD' ? branch : undefined;
  } catch {
    return undefined;
  }
}

/**
 * Completion notice telling the model WHERE an isolated-worktree subagent's changes
 * live (path + branch), appended when the worktree is RETAINED because it has
 * uncommitted changes or unpushed commits (mirrors Claude Code's
 * `<worktree><worktree_path>…</worktree_path><worktree_branch>…</worktree_branch></worktree>`
 * completion section). Without it the model only hears "cleaned up" for empty
 * worktrees and has to rediscover where a change-bearing worktree landed. The
 * branch clause is omitted on a detached HEAD / branch-read failure.
 */
export function buildWorktreeCompletionNotice(worktreePath: string, branch?: string): string {
  const branchClause = branch ? ` on branch ${branch}` : '';
  return `\n[Agent changes are in the isolated git worktree at ${worktreePath}${branchClause} — review, commit, or merge them from there.]`;
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
 * Mark a finished teammate idle AND fire the TeammateIdle lifecycle hook so a
 * hook can react to an idle teammate (Claude Code's TeammateIdle event —
 * teammate_name / team_name). Only fires when the id resolves to a real team
 * member; best-effort/fire-and-forget so hook failures never break teardown.
 */
export function markTeammateIdle(context: ToolContext, taskId: string): void {
  setTeamMemberActive(taskId, false);
  const team = getCurrentTeam();
  if (!team) return;
  const member = team.members.find(m => m.name === taskId || m.agentId === taskId);
  if (!member) return;
  // Proactive 'now idle / available' push to the team lead (PARITY, additive):
  // a teammate that simply finishes its turn automatically tells the lead it is
  // free to take new work, so the lead never has to poll. Mirrors Claude Code's
  // Stop hook writing createIdleNotification(agentName, { idleReason: 'available' })
  // to the leader's inbox. Best-effort — a messaging failure must never break
  // teardown. Lazy require avoids a static import cycle (send-message lazy-requires
  // task). A no-op when the finishing agent IS the lead.
  try {
    const { notifyLeadTeammateIdle } = require('./send-message');
    notifyLeadTeammateIdle(member.name);
  } catch { /* best-effort — never fail teardown on an idle push */ }
  if (!context._hookManager) return;
  context._hookManager.executeTeammateIdle({ teammateName: member.name, teamName: team.name }).catch(() => {});
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
 * Resolve an agent-definition / per-spawn tool-spec list (its `tools`,
 * `disallowedTools`, or a per-call `allowed_tools`) to canonical AMI tool names.
 * Each entry may be a bare tool name (`bash`), a legacy / cross-framework alias
 * (`Bash`, `Read`, `Edit`), or a PATTERNED permission spec (`Bash(git:*)`,
 * `Read(src/**)`). Every form is run through the single canonical
 * {@link permissionRuleValueFromString} parser (shared with the permission layer —
 * no second parser) so the BASE tool name is extracted and aliased to AMI's
 * registered name. Mirrors Claude Code's `resolveAgentTools`, which runs each tool
 * spec through `permissionRuleValueFromString` before matching — without this a
 * pattern-form allow entry silently filtered the tool out and a pattern-form
 * disallow entry silently failed to block (exact-string matching never matched
 * `Bash(git:*)` against the registered `bash`).
 */
export function canonicalAgentToolNames(specs: string[]): string[] {
  return specs.map(spec => permissionRuleValueFromString(spec).toolName);
}

/**
 * Built-in agent types that run ONCE and return a report — the parent never
 * resumes/messages them to continue, so the per-result "to continue this agent"
 * trailer is dead weight on them (mirrors Claude Code's
 * ONE_SHOT_BUILTIN_AGENT_TYPES in `AgentTool/constants.ts`, which gates whether the
 * agentId/SendMessage continuation trailer is emitted). `explore` MODE is the third
 * one-shot form (a read-only report), handled by {@link isOneShotAgent}.
 */
export const ONE_SHOT_BUILTIN_AGENT_TYPES = new Set(['Explore', 'Plan']);

/**
 * Whether a spawned subagent is a ONE-SHOT agent that is never continued — a
 * read-only `explore`-mode run or a named one-shot built-in agent type
 * ({@link ONE_SHOT_BUILTIN_AGENT_TYPES}). The task tool omits the "to continue this
 * agent" affordance for these (parity with Claude Code's one-shot trailer gate).
 */
export function isOneShotAgent(subagentType: string | undefined, mode: string): boolean {
  return mode === 'explore' || (subagentType !== undefined && ONE_SHOT_BUILTIN_AGENT_TYPES.has(subagentType));
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
 * Normalize a caller-supplied per-spawn `allowed_tools` value into a string[] (or
 * undefined when absent). Mirrors Claude Code's spawn-time `permissions` /
 * allowedTools list (`swarm/backends/types.ts`): the caller scopes ONE subagent's
 * tool set inline at task-call time, without a pre-authored named agent definition.
 * Returns `{ error }` for a non-array / non-string-element shape so the caller can
 * reject it; `{ value: undefined }` means "no inline restriction". Entries may be
 * bare names, aliases, or patterned specs — {@link canonicalAgentToolNames}
 * resolves them to canonical tool names at apply time.
 */
export function parseAllowedTools(
  raw: unknown,
): { value: string[] | undefined } | { error: string } {
  if (raw === undefined || raw === null) return { value: undefined };
  if (!Array.isArray(raw) || raw.some(t => typeof t !== 'string')) {
    return { error: 'allowed_tools must be an array of strings (the tool names this subagent may use).' };
  }
  return { value: raw as string[] };
}

/**
 * Resolve the per-spawn `allow_permission_prompts` knob into a permission-mode
 * contribution. Mirrors Claude Code's `allowPermissionPrompts` spawn flag
 * (`swarm/backends/types.ts`, default false): a spawned agent cannot show an
 * interactive permission prompt for a tool outside its granted scope — those calls
 * are auto-DENIED (`deny-all`) rather than blocking a detached/teammate run on a
 * prompt no one will answer. Passing `true` opts the subagent back into the
 * interactive `ask` handler. Returns `undefined` when the flag is absent, so the
 * existing fallback chain (explicit permission_mode → agent-def → explore → `ask`)
 * is untouched — this is an ADDITIVE opt-in that never changes AMI's default. The
 * companion `permissions` grant from Claude maps onto AMI's existing `allowed_tools`
 * inline tool scope (no separate field — see {@link parseAllowedTools}).
 */
export function resolvePromptGatePermissionMode(raw: unknown): 'ask' | 'deny-all' | undefined {
  if (raw === undefined || raw === null) return undefined;
  return coerceBool(raw) ? 'ask' : 'deny-all';
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

/** Valid values for the per-spawn `system_prompt_mode` knob — the single canonical
 *  list lives in core/skills.ts (shared with {@link AgentDefinition.systemPromptMode}),
 *  re-exported here so existing importers keep resolving it from the task tool. */
export { SYSTEM_PROMPT_MODES };
export type { SystemPromptMode };

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
    description: 'Run the subagent in an isolated git worktree. Mutually exclusive with "cwd" (the worktree is created from the base cwd) — pass one or the other, not both.',
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
  allowed_tools: {
    type: 'array',
    items: { type: 'string' },
    description: 'Scope this one subagent to exactly these tools (inline, without a named subagent_type). The resolved tool set is intersected with this list. Entries may be bare names ("bash", "file_read"), aliases ("Bash", "Read"), or patterned specs ("Bash(git:*)"). Omit for the full tool set for the chosen mode.',
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
    description: 'Override working directory for the subagent. Must be an absolute path. Mutually exclusive with isolation:"worktree".',
  },
  worktree_path: {
    type: 'string',
    description: 'Point the subagent at an EXISTING git worktree (absolute path) instead of creating a fresh one. Use to continue work in a worktree you already created. Mutually exclusive with isolation:"worktree" and cwd. The worktree is NOT auto-removed when the agent finishes (you own its lifecycle).',
  },
  allow_permission_prompts: {
    type: 'boolean',
    description: 'Whether this subagent may show interactive permission prompts for tools outside its granted scope. Default (false) auto-DENIES such calls so a detached/teammate run never blocks on a prompt no one will answer; set true to route unlisted-tool calls to the interactive "ask" handler. An explicit permission_mode overrides this.',
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
  max_output_tokens: {
    type: 'number',
    description: 'Cap the subagent\'s per-request output tokens (sizes the model\'s max_tokens). Useful for bounded delegations — a short summary or a terse report — where a full-length response would waste tokens. Must be a positive integer; omit to inherit the parent\'s output budget.',
  },
  color: {
    type: 'string',
    description: 'Display color for this subagent\'s badge/row in the live roster so parallel agents are visually distinguishable. Overrides a named subagent_type\'s own color. Omit to let the UI derive a stable color from the agent name.',
    enum: [...AGENT_COLORS],
  },
};

/** Parameters for {@link runBackgroundAgentDrain}. */
interface BackgroundDrainParams {
  context: ToolContext;
  subEngine: { submit(prompt: string): AsyncIterable<import('../types').EngineEvent>; shutdown?: () => void };
  /** Prompt to submit when no pre-started `iterator` is supplied (explicit
   *  run_in_background path — kept so submit() runs INSIDE the drain's try/catch, so a
   *  synchronous submit() throw is written to the output file rather than escaping). */
  effectivePrompt?: string;
  /** A pre-started async iterator of the sub-engine's event stream (auto-background
   *  handoff). When omitted, the drain starts one from {@link effectivePrompt}. */
  iterator?: AsyncIterator<import('../types').EngineEvent>;
  /** An in-flight `iterator.next()` whose result must be consumed first (used by the
   *  auto-background handoff, where the foreground loop had a pending next() when it
   *  promoted). Awaited before any further `iterator.next()` so the generator is never
   *  driven by two concurrent consumers. */
  firstPending?: Promise<IteratorResult<import('../types').EngineEvent>> | null;
  taskId: string;
  outputPath: string;
  label: string;
  effectiveCwd: string;
  isolation: string | undefined;
  subAbort: AbortController;
  /** Output already accumulated by a foreground run before it was auto-backgrounded,
   *  preserved so promotion never loses work. */
  initialOutput?: string;
  /** Tool-use / token tallies carried over from a promoted foreground run. */
  initialToolUseCount?: number;
  initialTokens?: number;
  /** Start timestamp carried over so the duration stat spans the whole run. */
  startedAt?: number;
}

/**
 * Drain a background (or auto-backgrounded) sub-agent's event stream to its
 * `tasks/<id>.output` file and drive its terminal bookkeeping (process-manager
 * status, worktree teardown, usage footer). Shared by the explicit
 * `run_in_background` path AND the auto-background promotion path so there is ONE
 * canonical background-completion implementation (no semantic duplication).
 */
async function runBackgroundAgentDrain(p: BackgroundDrainParams): Promise<void> {
  const { context, subEngine, taskId, outputPath, label, effectiveCwd, isolation, subAbort } = p;
  let result = p.initialOutput ?? '';
  let sawError = false;
  // Live usage accounting for the background completion summary (parity with
  // Claude's runAsyncAgentLifecycle usage block: tokens / tool uses / duration).
  let bgToolUseCount = p.initialToolUseCount ?? 0;
  let bgTotalTokens = p.initialTokens ?? 0;
  const bgStartedAt = p.startedAt ?? Date.now();
  const fd = fs.openSync(outputPath, OUTPUT_FILE_OPEN_FLAGS);
  const w = (s: string) => { try { fs.writeSync(fd, s); } catch { /* fd closed */ } };
  // Preserve any output accumulated before an auto-background promotion.
  if (result) w(result);
  const bgStallMs = parsePositiveIntEnv(process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS, SUBAGENT_STALL_TIMEOUT_MS);
  let bgStallTimer: ReturnType<typeof setTimeout> | null = null;
  let bgStalled = false;
  const resetBgStall = () => {
    if (bgStallTimer) clearTimeout(bgStallTimer);
    bgStallTimer = setTimeout(() => { bgStalled = true; subEngine.shutdown?.(); }, bgStallMs);
  };
  const clearBgStall = () => { if (bgStallTimer) { clearTimeout(bgStallTimer); bgStallTimer = null; } };
  resetBgStall();
  let pending = p.firstPending ?? null;
  try {
    // Start the stream INSIDE the try (explicit-background path) so a synchronous
    // submit() throw is captured and written to the output file, not left to reject
    // out of this detached drain. The auto-background path passes a live iterator.
    const iterator = p.iterator ?? subEngine.submit(p.effectivePrompt ?? '')[Symbol.asyncIterator]();
    while (true) {
      const { value: event, done } = await (pending ?? iterator.next());
      pending = null;
      if (done) break;
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
        bgToolUseCount++;
        const summary = typeof event.input?.command === 'string'
          ? event.input.command
          : typeof event.input?.file_path === 'string'
            ? (event.input.file_path as string)
            : JSON.stringify(event.input || {}).slice(0, 120);
        // Live tool-call progress: surface the running count on the task entry so
        // task_list can show how much work this background agent has done and the
        // model can see incremental deltas (reportProgressDelta). Mirrors Claude
        // Code's live task.progress.toolCount counter. Also surface a short AI
        // progress summary of the CURRENT action (progress.summary parity — takes
        // precedence over the static description in task_list); this stamps the
        // entry's lastActivity so a quiet agent can be flagged idle.
        context.processManager?.updateEntry(taskId, {
          toolCount: bgToolUseCount,
          progressSummary: `${event.toolName}: ${summary}`.slice(0, 80),
        });
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
        bgTotalTokens = event.stats?.totalTokens ?? bgTotalTokens;
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
    // Reap any background shells this agent left running so they don't
    // outlive the agent (zombie prevention — killShellTasksForAgent parity).
    context.processManager?.killTasksForAgent(taskId);
    // Release-on-finish: drop this sub-agent's isolated file cache.
    releaseFileCache(effectiveCwd, taskId);
  }
  try { fs.closeSync(fd); } catch { /* already closed */ }
  if (!result) {
    try { fs.writeFileSync(outputPath, '(subagent produced no output)'); } catch { /* dir removed */ }
  }
  // Completion usage summary (parity: Claude's async-agent usage block) — the
  // tokens / tool-uses / duration the model would otherwise only get from a
  // foreground run. The worktree outcome (cleaned-up / retained) is appended
  // below AFTER the agent is marked terminal, so cleanup can run.
  const bgDurationS = ((Date.now() - bgStartedAt) / 1000).toFixed(1);
  let bgFooter = `\n[Agent stats: ${bgToolUseCount} tool calls, ${bgTotalTokens} tokens, ${bgDurationS}s]`;
  // The teammate has finished — mark it idle so team_delete no longer
  // treats it as an active member blocking teardown, and fire TeammateIdle.
  markTeammateIdle(context, taskId);
  if (context.processManager) {
    // Three terminal states (parity with Claude's completed/failed/killed):
    // a stall-termination or an external abort (task_kill → abortFn →
    // subAbort.abort) is a KILL, not a crash — distinct from a genuine engine
    // `error` event / thrown exception, which is a FAILURE. Reporting a kill as
    // 'failed' would conflate a deliberate cancellation with a real error (and
    // would clobber the 'killed' status task_kill already latched).
    const killed = bgStalled || subAbort.signal.aborted;
    const terminalStatus = killed ? 'killed' : sawError ? 'failed' : 'completed';
    const exitCode = terminalStatus === 'completed' ? 0 : 1;
    // Record the FINAL-result stats distinct from the live progress counters
    // (parity: Claude Code's AsyncAgentDetailDialog result-vs-progress split) — the
    // authoritative tool/token totals at the terminal transition, which task_list
    // prefers for a finished agent (falling back to the live counter if absent).
    context.processManager.updateEntry(taskId, {
      status: terminalStatus,
      exitCode,
      resultToolCount: bgToolUseCount,
      resultTokenCount: bgTotalTokens,
    });
    context.processManager.emit('complete', { taskId, exitCode, command: `[agent] ${label}`, description: `Background agent: ${label}` });
  }
  // Tear down the isolated worktree on the background completion path too
  // (parity: Claude cleans the worktree when an async agent finishes). Done
  // AFTER updateEntry() flips this agent out of the 'running' state so it no
  // longer counts against runningAgentCount() for its OWN worktree; the
  // shouldRemoveWorktree() guard still keeps a dirty/unpushed worktree or one
  // another live agent is using. Append the outcome to the footer so a reader
  // of the output file learns where the work landed (or that it was cleaned up).
  if (isolation === 'worktree' && effectiveCwd !== context.cwd) {
    const runningAgents = context.processManager ? context.processManager.runningAgentCount() : 0;
    if (removeWorktreeIfClean(context.cwd, effectiveCwd, runningAgents)) {
      bgFooter += '\n[Agent worktree cleaned up — no changes detected]';
    } else {
      bgFooter += buildWorktreeCompletionNotice(effectiveCwd, readWorktreeBranch(effectiveCwd));
    }
  }
  try { fs.appendFileSync(outputPath, bgFooter); } catch { /* dir removed */ }
}

export const taskTool: ToolDefinition = {
  name: 'task',
  // PARITY (additive): declarative per-tool gate for the teammate subsystem.
  isEnabled: () => isTeammatesEnabled(),
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
    'the task matches what the agent can actually do. If a named agent\'s description says it should be used ' +
    'proactively, reach for it on your own — spawn it when its trigger arises without waiting for the user to ' +
    'ask.\n' +
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
    'Use resume with an agent id/name to continue a prior agent with its context preserved.\n' +
    '\n' +
    'Example usage:\n' +
    '\n' +
    '<example>\n' +
    'A well-briefed research delegation (no named agent — starts fresh, so the prompt carries full context):\n' +
    'task({\n' +
    '  description: "Audit auth middleware",\n' +
    '  subagent_type: "code-reviewer",\n' +
    '  prompt: "Review src/auth/middleware.ts for session-fixation risks. Context: we rotate the session id on login (line 88) but I am not sure the old id is invalidated server-side. I have already confirmed cookies are httpOnly+secure. Report: is the old session invalidated, and if not, exactly where to fix it? Under 200 words."\n' +
    '})\n' +
    '</example>\n' +
    '\n' +
    '<example>\n' +
    'Launch several explorations concurrently (issue the calls in ONE turn):\n' +
    'task({ description: "Find all API routes", mode: "explore", prompt: "List every HTTP route handler under src/api and the file:line each is defined at." })\n' +
    'task({ description: "Find all DB models", mode: "explore", prompt: "List every ORM model class under src/models and its table name." })\n' +
    '</example>',
  // Rebuilt per advertisement so `run_in_background` can be dynamically OMITTED
  // when background tasks are disabled via CLAUDE_CODE_DISABLE_BACKGROUND_TASKS —
  // the model is then never offered the flag (mirrors Claude Code's
  // fullInputSchema().omit({run_in_background:true}) gate).
  get inputSchema(): ToolInputSchema {
    const properties: Record<string, JsonSchemaProperty> = { ...TASK_TOOL_PROPERTIES };
    if (isBackgroundTasksDisabled()) {
      delete properties.run_in_background;
    }
    // `description` is REQUIRED (parity with Claude's baseInputSchema, where
    // `description: z.string()` is not optional): every spawned agent must carry an
    // explicit short human-readable label for UI/tracking rather than falling back
    // to a truncated raw prompt. execute() keeps a prompt-slice fallback for defense
    // in depth, but the schema now mandates the model supply a real label.
    return { type: 'object', properties, required: ['prompt', 'description'] };
  },
  isReadOnly: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const prompt = input.prompt as string;
    const mode = (input.mode as string) || 'general';
    const resumeTarget = input.resume as string | undefined;
    // Resume identity restore (parity: Claude Code's resumeAgent metadata restore,
    // resumeAgent.ts:L99-114): when resuming an agent and the resume call does not
    // re-supply its identity/defaults, fall back to what the ORIGINAL spawn ran
    // under (subagent_type, description, permission_mode, model) — recorded in
    // agentMetaRegistry — instead of silently re-deriving a generic general-purpose
    // agent with a fresh label. The authoritative session/cwd resolution still
    // happens in the resume block below; this only restores the identity fields.
    const resumeMeta = resumeTarget
      ? agentMetaRegistry.get(agentNameRegistry.get(resumeTarget) ?? resumeTarget)
      : undefined;
    const subagentType = (input.subagent_type as string | undefined) ?? resumeMeta?.agentType;
    const taskDescription = (input.description as string | undefined) ?? resumeMeta?.description;
    const effortOverride = input.effort as string | undefined;
    const requestedPermissionMode = (input.permission_mode as string | undefined) ?? resumeMeta?.permissionMode;
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

    // Per-spawn `color` override for the live-roster badge. Validated against the
    // known agent palette so a typo'd color is rejected with the valid options
    // rather than silently ignored (consumers derive a stable name-color fallback
    // only for an ABSENT color, not an invalid one).
    const colorOverride = input.color as string | undefined;
    if (colorOverride !== undefined && parseAgentColor(colorOverride) === undefined) {
      return {
        output: `Error: invalid color "${colorOverride}". Valid values: ${AGENT_COLORS.join(', ')}.`,
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

    // allowed_tools: a per-spawn inline tool scope (mirrors Claude's spawn-time
    // permissions/allowedTools). Validate the shape here; the resolved tool set is
    // intersected with it after mode/agent-def resolution below.
    const allowedToolsResult = parseAllowedTools(input.allowed_tools);
    if ('error' in allowedToolsResult) {
      return { output: `Error: ${allowedToolsResult.error}`, isError: true };
    }
    const allowedToolsSpec = allowedToolsResult.value;

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
    // The isolated worktree / overridden cwd the resumed agent last ran in, so a
    // resume continues on the same files instead of reverting to the parent cwd.
    let resumeCwd: string | undefined;
    if (resumeTarget) {
      const resolvedId = agentNameRegistry.get(resumeTarget) || resumeTarget;
      resumeSessionId = agentSessionRegistry.get(resolvedId);
      resumeCwd = agentCwdRegistry.get(resolvedId);
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

    // On resume, inherit the original agent's model when the resume call names none
    // (part of the resumeAgent identity restore above).
    const rawModel = (input.model as string | undefined) ?? resumeMeta?.model;
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
    // Per-agent `effort` default (parity: Claude's agent effort field) — the sub-engine
    // thinking level, used when the caller did not pass an explicit per-call `effort`.
    let agentEffort: string | undefined;
    // Per-agent `initialPrompt` (parity: Claude's agent initialPrompt) — text prepended
    // to the first user turn on the initial spawn (never on resume).
    let agentInitialPrompt: string | undefined;
    // Per-agent `skills` preload (parity: Claude's runAgent skill preloading) —
    // a one-line note naming any declared-but-unresolvable skill, prepended to the
    // agent's returned output so a typo'd/disabled preload is visible, not silent.
    let agentSkillPreloadNote = '';
    // Per-agent `color` (parity: Claude's agent/teammate `color`) — the named
    // agent's declared display color, used for the live-roster badge unless the
    // caller passes an explicit per-spawn `color` override below.
    let agentColor: AgentColorName | undefined;
    let effectiveMode = mode;

    if (subagentType && context._skillManager) {
      const agentDef = context._skillManager.getAgent(subagentType);
      if (!agentDef) {
        return { output: `Error: agent "${subagentType}" not found. Available agents: ${context._skillManager.listAgents().map(a => a.name).join(', ') || '(none)'}`, isError: true };
      }
      agentSystemPrompt = agentDef.systemPrompt;
      // Per-agent `systemPromptMode` (parity: Claude swarm teammate systemPromptMode).
      // 'append' layers the agent's own prompt AFTER the base subagent stance so the
      // agent EXTENDS the default guidance; 'replace'/'default'/absent keep the agent
      // prompt as the authoritative identity (AMI's existing replace behavior). A
      // blank agent prompt has nothing to append, so the base stance resolves via the
      // usual modeOnly fallback below.
      if ((agentDef.systemPromptMode as SystemPromptMode | undefined) === 'append' && agentDef.systemPrompt) {
        agentSystemPrompt = `${SUBAGENT_SYSTEM_PROMPT}\n\n${agentDef.systemPrompt}`;
      }
      agentModel = agentDef.model;
      agentMaxTurns = agentDef.maxTurns;
      agentPermissionMode = agentDef.permissionMode;
      agentTokenBudget = agentDef.autoCompactWindow;
      agentBackgroundDefault = agentDef.background === true;
      agentIsolationDefault = agentDef.isolation;
      agentEffort = agentDef.effort;
      agentInitialPrompt = agentDef.initialPrompt;
      agentColor = agentDef.color;

      // Preload the bodies of the skills this agent declares (parity: Claude's
      // runAgent skill preloading) into its authoritative system prompt, so the
      // spawned agent starts already carrying that instruction context instead of
      // having to discover + invoke each skill. Resolution reuses the SkillManager's
      // name/alias lookup (getSkill); a disabled or blank-bodied skill is treated as
      // unresolved. Unknown/invalid names are surfaced as a spawn note rather than a
      // hard failure (a missing optional skill should not strand the whole agent).
      if (agentDef.skills && agentDef.skills.length > 0) {
        const preload = buildPreloadedSkillsBlock(
          agentDef.skills,
          (n) => context._skillManager!.getSkill(n),
        );
        if (preload.block) {
          agentSystemPrompt = `${agentSystemPrompt}\n\n${preload.block}`;
        }
        if (preload.missing.length > 0) {
          agentSkillPreloadNote = `Note: agent "${subagentType}" references unknown or disabled skill(s) in its "skills" preload: ${preload.missing.join(', ')}.`;
        }
      }

      // Per-agent `criticalSystemReminder` (parity: Claude's
      // criticalSystemReminder_EXPERIMENTAL): a short, strongly-flagged hard
      // constraint appended to the agent's authoritative system prompt so a
      // must-hold rule (e.g. "no project writes", "end with VERDICT:") stays
      // conspicuous and does not drift into the agent's main prose.
      if (agentDef.criticalSystemReminder && agentSystemPrompt) {
        agentSystemPrompt = `${agentSystemPrompt}${buildCriticalReminderBlock(agentDef.criticalSystemReminder)}`;
      }

      if (agentDef.tools !== undefined) {
        const everything = allTools.getAll();
        if (agentDef.tools.length === 0) {
          // Explicit EMPTY allowlist = NO tools (parity: Claude distinguishes
          // `tools === undefined` → all tools from `tools === []` → no tools). This
          // resolves to a zero-tool set, which the zero-tool boundary guard below
          // then rejects with an actionable error — instead of the previous
          // behavior where an empty array silently fell through to granting
          // EVERY tool minus the denylist.
          tools = [];
        } else if (agentDef.tools.includes('*')) {
          // Wildcard allowlist: the agent gets every tool (the baseline denylist
          // below still strips plan_mode/workflow).
          tools = everything;
        } else {
          // Reject an allowlist naming a tool that does not exist — a silent
          // filter-to-nothing hides the typo and strands the agent. Each entry is
          // parsed to its canonical base tool name first (canonicalAgentToolNames),
          // so an alias (`Bash`) or a patterned spec (`Bash(git:*)`) matches the
          // registered tool instead of being misread as an unknown name.
          const known = new Set(everything.map(t => t.name));
          const invalid = agentDef.tools.filter(spec => !known.has(permissionRuleValueFromString(spec).toolName));
          if (invalid.length > 0) {
            return {
              output: `Error: agent "${subagentType}" lists unknown tool(s): ${invalid.join(', ')}. ` +
                `Available tools: ${[...known].sort().join(', ')}.`,
              isError: true,
            };
          }
          const allowed = new Set(canonicalAgentToolNames(agentDef.tools));
          tools = everything.filter(t => allowed.has(t.name));
        }
        // Deny wins even when an allowlist is set: an agent may declare BOTH
        // `tools` and `disallowedTools` (e.g. Claude's planAgent allows the
        // explore set yet denies edit/write/exit for defense-in-depth). Subtract
        // the denylist from the allowlist result so the two compose instead of
        // the allowlist silently ignoring the denylist.
        if (agentDef.disallowedTools && agentDef.disallowedTools.length > 0) {
          const denied = new Set(canonicalAgentToolNames(agentDef.disallowedTools));
          tools = tools.filter(t => !denied.has(t.name));
        }
        if (!shouldAllowNestedTask(currentDepth, maxDepth)) {
          tools = tools.filter(t => t.name !== 'task');
        }
      } else if (agentDef.disallowedTools && agentDef.disallowedTools.length > 0) {
        const disallowed = new Set(canonicalAgentToolNames(agentDef.disallowedTools));
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

    // Per-spawn `allowed_tools` inline scope (mirrors Claude's spawn-time
    // permissions/allowedTools): intersect the resolved set with exactly the tools
    // the caller named for this one subagent. A name that matches no registered tool
    // is rejected so a typo is not silently dropped (leaving a narrower set than
    // intended); patterned/alias specs are canonicalized first. The empty-set guard
    // below still fires if the intersection removes every tool.
    if (allowedToolsSpec) {
      const known = new Set(allTools.getAll().map(t => t.name));
      const invalid = allowedToolsSpec.filter(spec => !known.has(permissionRuleValueFromString(spec).toolName));
      if (invalid.length > 0) {
        return {
          output: `Error: allowed_tools names unknown tool(s): ${invalid.join(', ')}. ` +
            `Available tools: ${[...known].sort().join(', ')}.`,
          isError: true,
        };
      }
      const allowed = new Set(canonicalAgentToolNames(allowedToolsSpec));
      tools = tools.filter(t => allowed.has(t.name));
    }

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
    // `worktree_path` points the subagent at an EXISTING worktree (parity: Claude's
    // TeammateSpawnConfig.worktreePath). It is mutually exclusive with both
    // `isolation:"worktree"` (which CREATES a fresh worktree) and `cwd` (a different
    // working-dir override) — all three set the working directory, so accepting more
    // than one would silently pick a winner. Unlike a created worktree, an
    // existing-worktree run is NOT auto-removed on completion: the caller owns it.
    const requestedWorktreePath = input.worktree_path as string | undefined;
    if (requestedWorktreePath && (isolation === 'worktree' || requestedCwd)) {
      return {
        output: 'Error: `worktree_path` is mutually exclusive with `isolation:"worktree"` and `cwd` — pass exactly one working-directory option.',
        isError: true,
      };
    }
    // `cwd` and `isolation:"worktree"` are mutually exclusive (parity: Claude's
    // documented cwd contract). A worktree is created FROM the base cwd, so honoring
    // both would silently root the worktree at the override and defeat one of the two
    // conflicting intents — reject the combination explicitly instead.
    if (requestedCwd && isolation === 'worktree') {
      return {
        output: 'Error: `cwd` and `isolation:"worktree"` are mutually exclusive — a worktree is always created from the base working directory, so pass one or the other, not both.',
        isError: true,
      };
    }
    if (requestedCwd) {
      // The subagent's working directory is bounded by the PARENT's trusted
      // permission context — a model-chosen cwd cannot escape it.
      const cwdError = validateSubagentCwd(requestedCwd, context.cwd, context.allowedReadRoots ?? []);
      if (cwdError) {
        return { output: `Error: ${cwdError}`, isError: true };
      }
    }
    if (requestedWorktreePath) {
      // The existing worktree must resolve inside the parent's trusted boundary, same
      // as any model-supplied cwd override — a worktree outside it cannot be targeted.
      const wtError = validateSubagentCwd(requestedWorktreePath, context.cwd, context.allowedReadRoots ?? []);
      if (wtError) {
        return { output: `Error: ${wtError}`, isError: true };
      }
    }
    // Whether the subagent runs inside a git worktree distinct from the parent cwd —
    // a freshly created one (isolation:"worktree") OR a caller-supplied existing one
    // (worktree_path). Drives the worktree-orientation NOTICE; only the created form
    // is auto-cleaned (that path gates on `isolation === 'worktree'`).
    const usingExistingWorktree = !!requestedWorktreePath;
    let effectiveCwd = requestedCwd || requestedWorktreePath || context.cwd;
    // Introspection note surfaced to the parent when a resumed agent's recorded
    // worktree/cwd could not be restored (it was cleaned up) and we fell back.
    let cwdFallbackNote = '';
    // Resume restore: when resuming an agent that last ran in an isolated worktree
    // (or an explicit cwd) and the caller did not request a fresh cwd/worktree,
    // continue it in that SAME directory so the follow-up turn sees the files the
    // agent was editing. Revalidate the directory still exists — a worktree-cleanup
    // pass may have removed it — and fall back to the parent cwd instead of
    // crashing the sub-engine on an invalid chdir. On success, bump the directory's
    // mtime so a stale-worktree reaper does not treat the just-resumed worktree as
    // abandoned (mirrors Claude Code's resume worktree revalidation + touch).
    if (resumeCwd && !requestedCwd && isolation !== 'worktree') {
      try {
        if (!fs.statSync(resumeCwd).isDirectory()) throw new Error('not a directory');
        const now = new Date();
        fs.utimesSync(resumeCwd, now, now);
        effectiveCwd = resumeCwd;
      } catch {
        cwdFallbackNote = `Note: the resumed agent's worktree (${resumeCwd}) no longer exists — continuing in ${effectiveCwd} instead.\n\n`;
      }
    }
    if (isolation === 'worktree') {
      try {
        effectiveCwd = setupIsolatedWorktree(effectiveCwd, generateTaskId('agent'));
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
    } else if (mode === 'explore' && !subagentType) {
      // No explicit model anywhere → a read-only mode-only `explore` sub-agent defaults
      // to the fast/cheap model (parity: Claude's explore agent pins external runs to
      // haiku for speed). Only downshifts a Claude-family/absent parent; a non-Claude
      // parent keeps its own model. Operator-overridable via AMI_EXPLORE_MODEL. Skipped
      // for a named subagent_type, whose own identity/model governs.
      const fastModel = defaultExploreModel(parentModel);
      if (fastModel) providerConfig.model = fastModel;
    }

    // Final fallback: if NO model was resolvable from any source above and the
    // parent provided none to inherit, run the subagent on the provider-aware fast
    // default instead of dispatching an empty model id (parity with Claude Code's
    // getHardcodedTeammateModelFallback; keeps AMI's parent-inheritance perk when a
    // model IS present — see defaultSubagentModelFallback).
    providerConfig.model = defaultSubagentModelFallback(providerConfig.model, providerConfig);

    // Per-call `max_output_tokens` cap (parity: Claude's ForkedAgentParams.maxOutputTokens
    // → maxOutputTokensOverride): size the sub-engine's per-request max_tokens so a
    // bounded delegation (short summary / terse report) cannot emit a full-length
    // response. Threaded through the provider config's maxTokens, which the engine
    // already honors as the request output ceiling. A non-positive/garbage value is
    // ignored (parsePositiveIntInput → undefined) rather than capping at an absurd budget.
    const requestedMaxOutputTokens = parsePositiveIntInput(input.max_output_tokens);
    if (requestedMaxOutputTokens !== undefined) {
      providerConfig.maxTokens = requestedMaxOutputTokens;
    }

    // Per-spawn `allow_permission_prompts` gate (parity: Claude's allowPermissionPrompts
    // spawn flag): when the caller passes it, false → 'deny-all' (unlisted-tool calls are
    // auto-denied, never prompted) and true → 'ask' (interactive handler). Absent → no
    // contribution, so the existing fallback chain is unchanged (additive opt-in).
    const promptGatePermissionMode = resolvePromptGatePermissionMode(input.allow_permission_prompts);

    // Permission mode priority:
    // 1. Explicit `permission_mode` input (caller sets the spawned agent's mode,
    //    e.g. "plan" / "acceptEdits"; validated against PERMISSION_MODES above)
    // 2. Per-spawn `allow_permission_prompts` gate (deny-all when false, ask when true)
    // 3. Agent definition's permissionMode (e.g. verifier → auto-allow; safe, restricted tools)
    // 4. explore mode → always auto-allow (read-only tool set)
    // 5. Fallback → 'ask' (uses parent's permissionPromptHandler for TUI/YOLO propagation)
    const resolvedPermissionMode =
      requestedPermissionMode ||
      promptGatePermissionMode ||
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
      thinking: (effortOverride ?? agentEffort)
        ? { enabled: true, level: (effortOverride ?? agentEffort) as import('../model-capabilities').ThinkingLevel }
        : mode === 'fork'
          ? (context._parentThinking || undefined)
          : { enabled: false, level: 'off', type: 'disabled' as const },
      // When a named agent type is used, its system prompt is the authoritative
      // identity — skip stance-skill resolution (which would inject e.g. the
      // swebench stance workflow into a verification-only agent).
      systemPrompt: finalSystemPrompt || undefined,
      tokenBudget: agentTokenBudget || undefined,
    };

    // fork: thread the parent's REAL conversation history into the child engine as
    // seeded messages (via subConfig.initialMessages), instead of a lossy
    // assistant-string-only text summary. Done BEFORE the background/foreground
    // split so a background fork (run_in_background:true) inherits context too —
    // previously it got ZERO parent context. Drop a trailing interrupted assistant
    // turn and any assistant message with an unanswered tool_use first so the seeded
    // transcript is API-valid (the engine re-validates/heals it on construction).
    // Parity: Claude's buildForkedMessages + filterIncompleteToolCalls.
    if (mode === 'fork' && context._parentMessages && context._parentMessages.length > 0) {
      const seeded = filterIncompleteToolCalls(stripIncompleteTrailingMessages(context._parentMessages));
      if (seeded.length > 0) subConfig.initialMessages = seeded;
    }

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
    // `fork` has no replaceable base system prompt (it inherits the parent's full
    // coding stance as a clone), so its only bound is the stapled user-message
    // directive. The fork lead ALSO carries the recursion guard
    // ({@link FORK_NO_SUBSPAWN_DIRECTIVE}) — a fork child keeps the task tool at
    // depth 0, and its inherited stance tells it to delegate, so without an explicit
    // counter-instruction it would re-fork recursively. The guard is appended AFTER
    // the return directive so the prompt still LEADS with SUBAGENT_RETURN_DIRECTIVE.
    let effectivePrompt: string;
    if (finalSystemPrompt) {
      effectivePrompt = prompt;
    } else {
      const lead = mode === 'fork'
        ? `${SUBAGENT_RETURN_DIRECTIVE}\n\n${FORK_NO_SUBSPAWN_DIRECTIVE}\n\n${FORK_WORKER_DIRECTIVE}`
        : SUBAGENT_RETURN_DIRECTIVE;
      effectivePrompt = `${lead}\n\n---\n\n${prompt}`;
    }

    // Per-agent `initialPrompt` (parity: Claude's agent initialPrompt): prepend the
    // agent's standing instruction ahead of the caller's task prompt on the FIRST
    // user turn only — a resumed agent already carries its seeded context, so skip it.
    if (agentInitialPrompt && !resumeSessionId) {
      effectivePrompt = `${agentInitialPrompt}\n\n${effectivePrompt}`;
    }

    // Caller-specified thoroughness dial for explore-mode agents: turns the
    // explore prompt's abstract "adapt to the requested thoroughness" into a
    // concrete, caller-chosen search depth (mirrors Claude Code's exploreAgent
    // thoroughness parameter). Only meaningful when exploring.
    if (thoroughness && mode === 'explore') {
      effectivePrompt = `${buildThoroughnessDirective(thoroughness)}\n\n---\n\n${effectivePrompt}`;
    }

    // Coordinator-mode worker-capability affordance (mirrors Claude Code's
    // getCoordinatorUserContext): a coordinator has NO file/shell tools and only
    // delegates, so it is told at runtime exactly which tools its spawned workers
    // have and which MCP servers they can reach — otherwise it mis-delegates work
    // no worker tool can do. Prepended to the coordinator's first user turn.
    if (mode === 'coordinator') {
      effectivePrompt = `${buildCoordinatorWorkerContext(connectedMcpServerNames(context))}\n\n---\n\n${effectivePrompt}`;
    }

    // When the subagent runs in a git worktree distinct from the parent cwd — a
    // freshly created one (isolation:"worktree") OR a caller-supplied existing one
    // (worktree_path) — prepend a notice so it translates inherited paths to the
    // worktree root, re-reads stale files, and knows its changes are isolated
    // (mirrors Claude's buildWorktreeNotice).
    if ((isolation === 'worktree' || usingExistingWorktree) && effectiveCwd !== context.cwd) {
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

    // PARITY (additive): a named teammate spawned into an active team inherits the
    // team-wide declarative allowed-path permissions (teamAllowedPaths) as session
    // allow-rules, mirroring Claude Code's teammateInit path→rule conversion. The
    // lead sets these when creating the team; each teammate gets them on spawn so
    // the whole team shares a consistent, team-scoped path allowlist.
    if (agentName && getCurrentTeam()) {
      const teamRules = teamAllowedPathsToRules(getCurrentTeam());
      if (teamRules.length > 0) {
        subConfig.permissionRules = [...(subConfig.permissionRules ?? []), ...teamRules];
      }
    }

    if (runInBackground) {
      incrementSpawnCount(context);
      context.subagentLimiter?.recordSpawn();
      const taskId = generateTaskId();
      // Isolate this sub-agent's read-file-state: the sub-engine gets its own
      // clone-on-fork file cache keyed by taskId (released when it finishes).
      subConfig.agentId = taskId;
      if (agentName) {
        agentNameRegistry.set(agentName, taskId);
      }
      if (subConfig.sessionId) agentSessionRegistry.set(taskId, subConfig.sessionId);
      // Record a non-parent working dir (isolated worktree or explicit cwd) so a
      // later `resume` can restore the agent to the same files it was editing.
      if (effectiveCwd !== context.cwd) agentCwdRegistry.set(taskId, effectiveCwd);
      // Record this agent's identity/defaults so a later `resume` restores them when
      // the resume call names none (parity: Claude's resumeAgent metadata restore).
      recordAgentMeta(taskId, { agentType: subagentType, model: subConfig.provider?.model, permissionMode: resolvedPermissionMode, description: taskDescription });
      // If a team is active, a named teammate joins it as an active member so the
      // team-delete guard can see it and refuse to disband while it runs.
      if (agentName && getCurrentTeam()) {
        registerTeamMember({ name: agentName, agentId: taskId, agentType: subagentType, model: subConfig.provider?.model, cwd: effectiveCwd });
      }
      const tasksDir = projectStateDir(context.cwd, 'tasks');
      fs.mkdirSync(tasksDir, { recursive: true });
      const outputPath = path.join(tasksDir, `${taskId}.output`);
      fs.closeSync(fs.openSync(outputPath, OUTPUT_FILE_OPEN_FLAGS));

      const label = taskDescription || `${prompt.slice(0, 80)}`;
      if (context.processManager) {
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
      runBackgroundAgentDrain({
        context, subEngine, effectivePrompt, taskId, outputPath, label,
        effectiveCwd, isolation, subAbort,
      }).catch(() => {});

      return { output: `Background agent started: ${taskId}\nOutput file: ${outputPath} (its streamed transcript is appended here live).\nTo collect its result, call task_output({ task_id: "${taskId}", wait: true }) — this blocks until it finishes and returns in one call. To check progress mid-flight WITHOUT blocking, call task_output({ task_id: "${taskId}" }) (no wait) or tail the output file. Do NOT poll in a loop. When spawning several agents, issue all the task_output(wait) calls in a single turn so they run concurrently.\nDo not duplicate this agent's work — avoid working with the same files or topics it is using. Work on non-overlapping tasks, or briefly tell the user what you launched and end your response.` };
    }

    incrementSpawnCount(context);
    context.subagentLimiter?.recordSpawn();
    const taskId = generateTaskId();
    // Isolate this sub-agent's read-file-state: the sub-engine gets its own
    // clone-on-fork file cache keyed by taskId (released in the finally below).
    subConfig.agentId = taskId;
    const subEngine = context._engineFactory(subConfig);
    if (agentName) {
      agentNameRegistry.set(agentName, taskId);
    }
    if (subConfig.sessionId) agentSessionRegistry.set(taskId, subConfig.sessionId);
    // Record a non-parent working dir (isolated worktree or explicit cwd) so a
    // later `resume` can restore the agent to the same files it was editing.
    if (effectiveCwd !== context.cwd) agentCwdRegistry.set(taskId, effectiveCwd);
    // Record this agent's identity/defaults so a later `resume` restores them when
    // the resume call names none (parity: Claude's resumeAgent metadata restore).
    recordAgentMeta(taskId, { agentType: subagentType, model: subConfig.provider?.model, permissionMode: resolvedPermissionMode, description: taskDescription });
    if (agentName && getCurrentTeam()) {
      registerTeamMember({ name: agentName, agentId: taskId, agentType: subagentType, model: subConfig.provider?.model, cwd: effectiveCwd });
    }
    let resultText = '';

    // Shared Subagent lifecycle-hook payload for this foreground spawn. parentSessionId
    // is the INVOKING engine's session id (context.sessionId) — not the child's taskId —
    // so a SubagentStart/Stop hook can correlate the subagent boundary back to its real
    // parent across concurrent/nested runs (parity: Claude's SubagentContext.parentSessionId;
    // the prior code mis-reported the child id as its own parent). invocationKind
    // discriminates a fresh `spawn` from a `resume` of an existing agent session (parity:
    // Claude's invocationKind). Falls back to taskId only in bare/test contexts with no
    // session id.
    const subagentHookCtx = {
      parentSessionId: context.sessionId || taskId,
      subagentSessionId: subConfig.sessionId || taskId,
      prompt,
      mode: effectiveMode,
      invocationKind: (resumeSessionId ? 'resume' : 'spawn') as 'resume' | 'spawn',
    };

    if (context._hookManager) {
      context._hookManager.executeSubagentStart(subagentHookCtx).catch(() => {});
    }

    // Surface this foreground subagent live to the parent engine's UI.
    const relayDepth = childDepth._agentDepth;
    // Per-spawn `color` wins over the named agent's declared color; absent both,
    // the roster derives a stable name-based color. Resolved here so the roster
    // badge is tinted the same way whether the color came from the task call or
    // the agent definition.
    const effectiveColor = (colorOverride as AgentColorName | undefined) ?? agentColor;
    context._emitChild?.({
      type: 'subagent_start', nodeId: taskId, depth: relayDepth,
      agentName: agentName || undefined, model: subConfig.provider?.model, prompt,
      color: effectiveColor,
      // The spawned KIND (parity: Claude's task_started.task_type): the named agent
      // definition when one was requested, else the built-in mode it runs as.
      taskType: subagentType || mode,
    });

    let toolUseCount = 0;
    let totalTokens = 0;
    // Prompt-cache introspection carried into the completion footer (parity: Claude's
    // per-fork cacheRead / cacheHitRate). Latched from usage_update stats; 0/undefined
    // when the provider reports no cache signal (clause then omitted).
    let cacheReadTokens = 0;
    let cacheHitRate: number | undefined;
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
    // Whether this subagent's isolated worktree was torn down during cleanup. Set in
    // the `finally` so the removal runs on EVERY exit path (success, error, abort,
    // stall), mirroring Claude's cleanupWorktreeIfNeeded() living in finally — the
    // success-path notice below reads this instead of re-running cleanup.
    let worktreeRemoved = false;

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

    // Auto-background: a foreground subagent that is STILL ACTIVELY WORKING past the
    // configured threshold is promoted to a background task so the parent turn is not
    // blocked for its full lifetime (parity: Claude's autoBackgroundMs race against a
    // background signal, AgentTool.tsx:808-833). Disabled by default (getAutoBackgroundMs
    // returns 0); the stall timeout only bounds INACTIVITY, so without this a subagent
    // that keeps emitting events blocks the parent indefinitely. The timer is cleared on
    // normal completion/error (parity: Claude's cancelAutoBackground).
    const autoBgMs = getAutoBackgroundMs();
    let promotedToBackground = false;
    let autoBgTimer: ReturnType<typeof setTimeout> | null = null;
    let autoBgResolve: (() => void) | null = null;
    const autoBgPromise = autoBgMs > 0
      ? new Promise<void>((res) => { autoBgResolve = res; })
      : null;
    if (autoBgMs > 0) {
      autoBgTimer = setTimeout(() => { autoBgResolve?.(); }, autoBgMs);
    }
    const clearAutoBg = () => { if (autoBgTimer) { clearTimeout(autoBgTimer); autoBgTimer = null; } };

    // Consume the sub-engine's event stream via a manual iterator so the auto-background
    // timer can be raced against the next event (a `for await` cannot be interrupted
    // mid-await without finalizing the generator, which would kill the subagent).
    // Derive the background-race signal ONCE (parity: Claude creates backgroundPromise
    // outside the loop) so each iteration does not stack a new .then() reaction on it.
    const bgSignal = autoBgPromise ? autoBgPromise.then(() => ({ kind: 'bg' as const })) : null;
    let pendingNext: Promise<IteratorResult<import('../types').EngineEvent>> | null = null;
    try {
      // submit() inside the try so a synchronous throw is caught by the catch below
      // (parity with the original `for await (… of subEngine.submit(…))`).
      const iterator = subEngine.submit(effectivePrompt)[Symbol.asyncIterator]();
      while (true) {
        if (!pendingNext) pendingNext = iterator.next();
        const np = pendingNext;
        let evResult: IteratorResult<import('../types').EngineEvent>;
        if (bgSignal) {
          const raced = await Promise.race([
            np.then((r) => ({ kind: 'ev' as const, r })),
            bgSignal,
          ]);
          if (raced.kind === 'bg') {
            // Threshold fired while the subagent is still running — promote it. `np`
            // is still in-flight; it is handed to the background drain as firstPending
            // so no event is dropped and the generator keeps a single consumer.
            promotedToBackground = true;
            break;
          }
          evResult = raced.r;
        } else {
          evResult = await np;
        }
        pendingNext = null;
        resetStallTimer();
        if (stalled) break;
        const { value: event, done } = evResult;
        if (done) break;
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
          cacheReadTokens = event.stats?.cachedPromptTokens ?? cacheReadTokens;
          if (event.stats?.cacheHitRate !== undefined) cacheHitRate = event.stats.cacheHitRate;
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
      if (promotedToBackground) {
        // Hand the still-running sub-engine off to the shared background drain and
        // return a task_id immediately, exactly like an explicit run_in_background
        // spawn. Teardown is skipped in the `finally` below (the drain owns it now).
        const tasksDir = projectStateDir(context.cwd, 'tasks');
        fs.mkdirSync(tasksDir, { recursive: true });
        const outputPath = path.join(tasksDir, `${taskId}.output`);
        fs.closeSync(fs.openSync(outputPath, OUTPUT_FILE_OPEN_FLAGS));
        const label = taskDescription || `${prompt.slice(0, 80)}`;
        if (context.processManager) {
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
        runBackgroundAgentDrain({
          context, subEngine, iterator, firstPending: pendingNext, taskId, outputPath, label,
          effectiveCwd, isolation, subAbort,
          initialOutput: resultText, initialToolUseCount: toolUseCount, initialTokens: totalTokens, startedAt,
        }).catch(() => {});
        context._emitChild?.({
          type: 'subagent_result', nodeId: taskId, depth: relayDepth,
          success: true, summary: `auto-backgrounded after ${Math.round(autoBgMs / 1000)}s`, tokens: totalTokens,
        });
        return { output: `Background agent started (auto-backgrounded after ${Math.round(autoBgMs / 1000)}s): ${taskId}\nThe foreground delegation ran past the auto-background threshold and is now running detached. To collect its result, call task_output({ task_id: "${taskId}", wait: true }) — this blocks until it finishes and returns in one call. Do NOT poll in a loop.` };
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
        context._hookManager.executeSubagentStop(subagentHookCtx).catch(() => {});
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
      // Always cancel the auto-background timer (parity: Claude's cancelAutoBackground
      // on completion) so it cannot fire after the run is over.
      clearAutoBg();
      // When the subagent was promoted to the background, the drain now OWNS teardown
      // (sub-engine shutdown, shell reaping, file-cache release, teammate-idle, worktree
      // cleanup) — running any of it here would kill the still-live background run.
      if (!promotedToBackground) {
        subEngine.shutdown?.();
        // Reap any background shells this agent left running so they don't outlive
        // the agent (zombie prevention — killShellTasksForAgent parity).
        context.processManager?.killTasksForAgent(taskId);
        // Release-on-finish: drop this sub-agent's isolated file cache.
        releaseFileCache(effectiveCwd, taskId);
        // Foreground teammate finished (success or error) — mark it idle so it no
        // longer blocks team_delete, and fire TeammateIdle.
        markTeammateIdle(context, taskId);
        // Tear down the isolated worktree here in `finally` so it runs on the error /
        // abort / stall paths too — not only the success return (parity: Claude runs
        // cleanupWorktreeIfNeeded in finally "to handle abort/error paths"). The
        // shouldRemoveWorktree() guard inside removeWorktreeIfClean still keeps a dirty
        // or unpushed worktree, and a worktree with a live background agent in it.
        if (isolation === 'worktree' && effectiveCwd !== context.cwd) {
          const runningAgents = context.processManager ? context.processManager.runningAgentCount() : 0;
          worktreeRemoved = removeWorktreeIfClean(context.cwd, effectiveCwd, runningAgents);
        }
      }
    }
    context.abortSignal.removeEventListener('abort', forwardAbort);

    if (context._hookManager) {
      context._hookManager.executeSubagentStop(subagentHookCtx).catch(() => {});
    }

    if (isolation === 'worktree' && effectiveCwd !== context.cwd) {
      // Cleanup already ran in the `finally` above (so it also covers error/abort);
      // here we only surface the OUTCOME on the success path.
      if (worktreeRemoved) {
        deliverable += '\n[Agent worktree cleaned up — no changes detected]';
      } else {
        // Worktree RETAINED (it has changes / unpushed commits / a live agent):
        // surface where the agent's work landed so the model can find it instead of
        // rediscovering it (parity: Claude's <worktree_path>/<worktree_branch>).
        deliverable += buildWorktreeCompletionNotice(effectiveCwd, readWorktreeBranch(effectiveCwd));
      }
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
    const rawBody = cappedResult.trim() ? cappedResult : '(Subagent completed but returned no output.)';
    // Prepend any skills-preload note (a declared preload skill that could not be
    // resolved) so the typo/disabled skill is visible to the parent, not silent.
    const body = `${cwdFallbackNote}${agentSkillPreloadNote ? `${agentSkillPreloadNote}\n\n` : ''}${rawBody}`;
    // Continue-affordance: tell the parent it CAN message/resume this agent to
    // carry the work forward. Skipped for ONE-SHOT agents — a read-only `explore`
    // run OR a named one-shot built-in type (Explore/Plan) — which are never
    // continued, so the trailer is dead weight (mirrors Claude Code's
    // ONE_SHOT_BUILTIN_AGENT_TYPES continuation-trailer gate). Uses the RAW `mode`
    // (not effectiveMode, which fork/explore paths may reassign) so the one-shot
    // classification matches what the caller requested.
    const continueHint = isOneShotAgent(subagentType, mode)
      ? ''
      : `\nTo continue this agent, call task({ resume: "${agentName || taskId}", prompt: ... }).`;
    return {
      output: `${body}\n\n[Agent stats: ${toolUseCount} tool calls, ${totalTokens} tokens${buildCacheStatsClause(cacheReadTokens, cacheHitRate)}, ${durationS}s]${continueHint}`,
      ...(failed ? { isError: true } : {}),
    };
  },
};
