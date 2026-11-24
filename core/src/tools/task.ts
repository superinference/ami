import * as fs from 'fs';
import { sanitizeAgentName } from './agent-name';
export { sanitizeAgentName, AGENT_NAME_RESERVED_CHARS } from './agent-name';
import { projectStateDir } from '../reference-journal';
import * as path from 'path';
import 'crypto';
import * as child_process from 'child_process';
import { promisify } from 'util';

/** Promisified execFile so the worktree-cleanup git calls (run from the detached,
 *  concurrent background-drain path) do not block the event loop for the duration of
 *  each git subprocess — unlike execFileSync, which freezes every other concurrent
 *  drain, the foreground stream, and the TUI render while git runs. */
const execFileAsyncTask = promisify(child_process.execFile);
import { ToolDefinition, ToolContext, ToolResult, EngineConfig, ToolInputSchema, JsonSchemaProperty } from '../types';
import { createDefaultTools } from './index';
import { isMcpTool } from './tool-categories';
import { releaseFileCache } from '../file-cache';
import { isReadOnlyBashCommand } from './bash-cache';
import { isTerminalTaskStatus } from '../process-manager';
import { parseOptionalBool } from '../utils/coerce';
import { createBoundedAbortController } from '../utils/abort';
import '../analytics';
import { spillToolText, resolveTaskMaxChars } from './spill-output';
import { mapChildToSubagentEvent, attachSubagentUsage, resolveEnableSummarization } from './subagent-relay';
import { BackgroundLiveTurns, resolveLiveTurnRetention } from './background-live-turns';
import {
  collapseActivityRun,
  createActivityDescriptionResolver,
  isReadOrSearchActivity,
} from './tool-activity-class';
import { inheritBedrockRegionPrefix } from '../bedrock';
import { getCurrentTeam, registerTeamMember, registerTeamMemberInto, setTeamMemberActive, teamAllowedPathsToRules, teamScratchpadRules, getTeamScratchpadDir, loadTeamFromDisk } from './team';
import { reviewSubagentHandoff, type HandoffAction } from './handoff-review';
import { isTeammatesEnabled } from './tool-gates';
import { SessionManager } from '../session';
import { PERMISSION_MODES, permissionRuleValueFromString } from '../permissions';
import { resolveModelAlias, getRuntimeMainLoopModel } from '../model-registry';

import '../config';
import { buildPreloadedSkillsBlock, resolveSkillName, SYSTEM_PROMPT_MODES, type SystemPromptMode, parseAgentColor, AGENT_COLORS, type AgentColorName, isBuiltInAgent, agentPluginRoot, BUBBLE_PERMISSION_MODE } from '../skills';
import { withFrontmatterHookPluginRoot } from '../hooks';
import type { SubagentContext } from '../hooks';
import { buildAgentMemoryPrompt, isAutoMemoryEnabled } from '../memory';
import { log } from '../logger';
import { AGENT_ID_MAX_LENGTH, COORDINATOR_SUBAGENT_SYSTEM_PROMPT, DEFAULT_FORK_MAX_TURNS, DEFAULT_MAX_AGENTS_PER_SESSION, DEFAULT_MAX_AGENT_DEPTH, DEFAULT_MAX_CONCURRENT_AGENTS, EXPLORE_AGENT_MIN_QUERIES, FORK_NO_SUBSPAWN_DIRECTIVE, GIT_HARDENING_ARGS, SUBAGENT_SYSTEM_PROMPT, agentRegistryPath, clampPermissionMode, generateTaskId, getAutoBackgroundMs, isSafeAgentId, linkChildAbortToParent, parsePositiveIntEnv, parsePositiveIntInput, resolveSpawnMode, resolveSpawnTaskId } from './task-link-child-abort-to-parent';
import type { AgentMeta, AgentModeSpec, PersistedAgentRegistry, ResumeResolution } from './task-link-child-abort-to-parent';
import { INHERIT_MODEL_SENTINEL, availableAgentTypeNames, connectedMcpServerNames, defaultExploreModel, defaultSubagentModelFallback, ensureRequiredMcpServersReady, readMcpServerStates, resolveAgentProjectContext, resolveSubagentDefaultModel, resolveSubagentModel, sanitizeTranscriptForReplay } from './task-ensure-required-mcp-servers-ready';
import { MAX_RECENT_ACTIVITIES, buildCacheStatsClause, buildCriticalReminderBlock, buildLiveStateTrail, buildRecentActivityTrail, buildSubEngineWithRetry, buildWorktreeCompletionNotice, buildWorktreeNotice, canonicalAgentToolNames, describeActivity, getSubagentDepthContext, incrementSpawnCount, isAgentTypeAllowed, isInterruptedFinalText, parseAllowedAgentTypes, parseAllowedTools, parseAutoApproveTools, selectExactForkToolPool, selectFinalAnswer, shouldAllowNestedTask, shouldRemoveWorktree, validateSubagentCwd } from './task-select-exact-fork-tool-pool';
import type { AgentCapContext, PruneDeadAgentParams } from './task-select-exact-fork-tool-pool';
import { DISABLE_BACKGROUND_TASKS_ENV, EXPLORE_THOROUGHNESS_LEVELS, TEAMMATE_PROMPT_ADDENDUM, agentResultPath, applyPlanRequirement, buildAutoApproveRules, buildPlanRequiredNotice, buildSubagentHookCtx, buildThoroughnessDirective, isBackgroundTasksDisabled, resolvePlanRequired, resolvePromptGatePermissionMode } from './task-build-subagent-hook-ctx';
import type { SubagentModelOption } from './task-build-subagent-hook-ctx';
export { AGENT_ID_MAX_LENGTH, AUTO_BACKGROUND_MS_ENV, AUTO_BACKGROUND_TASKS_ENV, COORDINATOR_SUBAGENT_SYSTEM_PROMPT, DEFAULT_FORK_MAX_TURNS, DEFAULT_MAX_AGENTS_PER_SESSION, DEFAULT_MAX_CONCURRENT_AGENTS, EXPLORE_AGENT_MIN_QUERIES, FORK_NO_SUBSPAWN_DIRECTIVE, FORK_SUBAGENT_ENV, GIT_HARDENING_ARGS, SUBAGENT_SYSTEM_PROMPT, agentRegistryPath, clampPermissionMode, collapseAgentTypeForAnalytics, generateTaskId, getAutoBackgroundMs, isForkSubagentEnabled, isSafeAgentId, linkChildAbortToParent, parsePositiveIntEnv, parsePositiveIntInput, resolveSpawnMode, resolveSpawnTaskId } from './task-link-child-abort-to-parent';
export type { AgentMeta, AgentModeSpec, ResumeResolution } from './task-link-child-abort-to-parent';
export { EXPLORE_MODEL_ENV, INHERIT_MODEL_SENTINEL, REQUIRED_MCP_MAX_WAIT_MS, REQUIRED_MCP_POLL_INTERVAL_MS, availableAgentTypeNames, connectedMcpServerNames, defaultExploreModel, defaultSubagentModelFallback, ensureRequiredMcpServersReady, filterIncompleteToolCalls, filterOrphanedToolResults, isIncompleteAssistantMessage, normalizeMcpServerState, readMcpServerStates, resolveAgentProjectContext, resolveSubagentDefaultModel, resolveSubagentModel, sanitizeTranscriptForReplay, stripIncompleteTrailingMessages } from './task-ensure-required-mcp-servers-ready';
export type { RequiredMcpGateOptions } from './task-ensure-required-mcp-servers-ready';
export { BACKGROUND_SPAWN_MAX_ATTEMPTS, MAX_RECENT_ACTIVITIES, buildCacheStatsClause, buildCriticalReminderBlock, buildLiveStateTrail, buildRecentActivityTrail, buildSubEngineWithRetry, buildWorktreeCompletionNotice, buildWorktreeNotice, canonicalAgentToolNames, describeActivity, getSubagentDepthContext, isAgentTypeAllowed, isInterruptedFinalText, parseAllowedAgentTypes, parseAllowedTools, parseAutoApproveTools, selectExactForkToolPool, selectFinalAnswer, shouldAllowNestedTask, shouldRemoveWorktree, validateSubagentCwd } from './task-select-exact-fork-tool-pool';
export type { AgentCapContext, PruneDeadAgentParams } from './task-select-exact-fork-tool-pool';
export { CLAUDE_PLAN_MODE_REQUIRED_ENV, DISABLE_BACKGROUND_TASKS_ENV, EXPLORE_THOROUGHNESS_LEVELS, SUBAGENT_PLAN_REQUIRED_ENV, TEAMMATE_PROMPT_ADDENDUM, agentResultPath, applyPlanRequirement, buildAutoApproveRules, buildPlanRequiredNotice, buildSubagentHookCtx, buildThoroughnessDirective, isBackgroundTasksDisabled, isSubagentPlanRequiredByEnv, resolvePlanRequired, resolvePromptGatePermissionMode } from './task-build-subagent-hook-ctx';
export type { SubagentModelOption } from './task-build-subagent-hook-ctx';

/**
 * Open flags for a sub-agent's output-capture file. `O_NOFOLLOW` refuses to open
 * the file if the predictable `<taskId>.output` path has been replaced by a
 * symlink (parity: the reference's `O_WRONLY | O_CREAT | O_APPEND | O_NOFOLLOW` on
 * command-output fds — "to prevent symlink-following attacks from the sandbox").
 * A sandboxed/malicious sub-agent could otherwise pre-plant a symlink at that
 * path and redirect the captured output into an attacker-chosen file (e.g.
 * `~/.bashrc`, a git `HEAD`); with `O_NOFOLLOW` the open throws `ELOOP` instead.
 * `O_TRUNC` preserves the "start from empty" semantics of the old `'w'` flag /
 * `writeFileSync(path, '')` it replaces. Exported for the symlink-guard test.
 */
export const OUTPUT_FILE_OPEN_FLAGS =
  fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_TRUNC | fs.constants.O_NOFOLLOW;

const SUBAGENT_STALL_TIMEOUT_MS = 10 * 60 * 1000; // 10 minutes

/**
 * Structured worker-discipline contract stapled into a `fork` child's first user
 * turn, ALONGSIDE {@link FORK_NO_SUBSPAWN_DIRECTIVE} (which owns the no-subspawn
 * rule — this constant deliberately omits it to avoid duplicating that guidance).
 * Mirrors the non-negotiable worker rules + fixed plain-text report schema of
 * the reference's `buildChildMessage` (forkSubagent.ts:L171-198): begin the reply
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
 * Framing prepended to a mode-only sub-agent's task (no subagent_type identity) so
 * it returns its result and STOPS instead of running the full interactive coding
 * loop. Mirrors how a workflow agent is told "your final message IS the result".
 */
export const SUBAGENT_RETURN_DIRECTIVE =
  'You are a sub-agent handling one delegated task. Do exactly what the task asks and nothing more. ' +
  'When you have the answer, give it directly as your final message and STOP — do not create or edit ' +
  'files, run commands, or add verification or extra work unless the task explicitly requires it.';

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
// work. Only populated when the agent ran outside the parent cwd. Mirrors the reference
// Code's per-agent worktree restoration on resume.
const agentCwdRegistry = new Map<string, string>();
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

/**
 * Resolve a human-readable DISPLAY LABEL for a (background) agent, mirroring the reference
 * Code's `useSwarmBanner` reverse-lookup precedence (useSwarmBanner.ts:L105-L122):
 * prefer the agent's registered addressable `@name` — reverse-looked-up from the
 * agent registry by its `taskId` ({@link getAgentNameById}) — then its task
 * `description`, then a trimmed `prompt` preview, then the raw `taskId`, and only a
 * literal `'agent'` when nothing is known. A leader viewing a background agent (or a
 * roster/task_list row) sees the SAME name it messages the agent by, not an opaque
 * id or a truncated prompt. Blank (whitespace-only) description/prompt values are
 * skipped so they never shadow a lower-rung fallback. The name lookup is the real
 * production path (a named spawn registers its name BEFORE this label is built), so
 * the banner stays consistent with the send_message / resume address.
 */
export function resolveAgentLabel(opts: {
  taskId?: string;
  description?: string;
  prompt?: string;
}): string {
  const byName = opts.taskId ? getAgentNameById(opts.taskId) : undefined;
  if (byName) return byName;
  const desc = opts.description?.trim();
  if (desc) return desc;
  if (opts.prompt && opts.prompt.trim()) return opts.prompt.slice(0, 80);
  return opts.taskId || 'agent';
}

export function resetAgentRegistry(): void {
  agentNameRegistry.clear();
  agentSessionRegistry.clear();
  agentCwdRegistry.clear();
  agentMetaRegistry.clear();
}

/**
 * Persist the current agent registries for `cwd` so a later process can `resume` an
 * agent spawned in a prior run (durable cross-process resume). Best-effort — a write
 * failure never breaks a spawn. Called after each spawn records its session/identity.
 */
export function persistAgentRegistry(cwd: string): void {
  try {
    const data: PersistedAgentRegistry = {
      names: Object.fromEntries(agentNameRegistry),
      sessions: Object.fromEntries(agentSessionRegistry),
      cwds: Object.fromEntries(agentCwdRegistry),
      meta: Object.fromEntries(agentMetaRegistry),
    };
    fs.mkdirSync(projectStateDir(cwd), { recursive: true });
    fs.writeFileSync(agentRegistryPath(cwd), JSON.stringify(data));
  } catch { /* best-effort persistence — never fail a spawn on a write error */ }
}

/**
 * Rehydrate the in-memory agent registries from `cwd`'s persisted snapshot so a
 * `resume` works after a process restart. LIVE in-memory entries win over the
 * snapshot (an agent spawned in THIS process is authoritative over a stale record).
 * Best-effort — a missing/corrupt snapshot leaves the in-memory maps untouched.
 */
export function rehydrateAgentRegistry(cwd: string): void {
  let data: PersistedAgentRegistry;
  try {
    data = JSON.parse(fs.readFileSync(agentRegistryPath(cwd), 'utf-8')) as PersistedAgentRegistry;
  } catch { return; }
  for (const [k, v] of Object.entries(data.names ?? {})) if (!agentNameRegistry.has(k)) agentNameRegistry.set(k, v);
  for (const [k, v] of Object.entries(data.sessions ?? {})) if (!agentSessionRegistry.has(k)) agentSessionRegistry.set(k, v);
  for (const [k, v] of Object.entries(data.cwds ?? {})) if (!agentCwdRegistry.has(k)) agentCwdRegistry.set(k, v);
  for (const [k, v] of Object.entries(data.meta ?? {})) if (!agentMetaRegistry.has(k)) agentMetaRegistry.set(k, v);
}

/**
 * Resolve a `resume` target — an agent NAME, a taskId, OR a raw sessionId — to the
 * sub-engine session to reload. Mirrors the reference's resume, which resolves a
 * resumable agent from the on-disk store and accepts the agent id directly. A raw
 * sessionId is accepted by reverse-scanning the taskId→sessionId index so a caller
 * holding only the persisted sessionId can still continue the run. Call
 * {@link rehydrateAgentRegistry} first so cross-process targets are visible. Returns
 * `undefined` when the target matches no known name / taskId / sessionId.
 */
export function resolveResumeTarget(target: string): ResumeResolution | undefined {
  const resolvedId = agentNameRegistry.get(target) ?? target;
  const sessionId = agentSessionRegistry.get(resolvedId);
  if (sessionId) return { sessionId, cwd: agentCwdRegistry.get(resolvedId), resolvedId };
  // Raw sessionId: the target IS a sessionId some agent ran under — resume on it
  // directly, restoring that agent's recorded worktree/cwd and identity.
  for (const [taskId, sid] of agentSessionRegistry) {
    if (sid === target) return { sessionId: sid, cwd: agentCwdRegistry.get(taskId), resolvedId: taskId };
  }
  return undefined;
}

/**
 * Resolve a caller-supplied agent `name` to a UNIQUE addressable name. The task
 * tool documents `name` as "must be unique" (it is the send_message / resume
 * key), but a model reusing a base name would silently overwrite the earlier
 * agent's registry entry, orphaning it. On collision we append an incrementing
 * numeric suffix (`name-2`, `name-3`, …) so every spawned agent keeps a distinct,
 * addressable identity (mirrors the reference's generateUniqueTeammateName).
 *
 * STATUS-AWARE RECLAMATION (parity: the reference's findTeammateTaskByAgentId prefers
 * the RUNNING task over a stale killed/completed one, InProcessTeammateTask.tsx:L92-108):
 * a name whose current holder has already TERMINATED (completed/failed/killed) is
 * reclaimed verbatim for the new live agent rather than being pushed to a suffixed
 * alias. Without this, killing `scout` then respawning `scout` produced `scout-2`
 * while the DEAD `scout` kept the canonical send_message/resume address — so a message
 * or resume aimed at `scout` routed to the dead instance. Reclaiming the lowest free
 * OR dead slot keeps the live agent addressable under the name the model expects. The
 * caller supplies an `isTerminal` status probe (the process manager); with no probe we
 * cannot prove a holder is dead, so the behavior degrades to pure suffixing.
 */
export function uniqueAgentName(
  name: string,
  isTerminal?: (taskId: string) => boolean,
): string {
  const existing = agentNameRegistry.get(name);
  if (existing === undefined) return name;              // free — take the base name
  if (isTerminal?.(existing)) return name;             // dead holder — reclaim the base name
  for (let i = 2; ; i++) {
    const candidate = `${name}-${i}`;
    const holder = agentNameRegistry.get(candidate);
    if (holder === undefined) return candidate;         // free suffix
    if (isTerminal?.(holder)) return candidate;         // dead suffixed holder — reclaim it
  }
}

/**
 * Universal baseline denylist applied to EVERY sub-agent regardless of its mode
 * or explicit tool allowlist (mirrors the reference's ALL_AGENT_DISALLOWED_TOOLS /
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
 * Extra tools DENIED to any NON-built-in (user/project/plugin) agent definition, on
 * top of the universal {@link ALL_AGENT_DISALLOWED_TOOLS} floor. Mirrors the reference's
 * `CUSTOM_AGENT_DISALLOWED_TOOLS`, which `filterToolsForAgent` subtracts whenever an
 * agent's `source !== 'built-in'` (i.e. a definition loaded from a possibly untrusted
 * cloned repo's `.ami/agents/` or `.claude/agents/`). Such a definition must not be
 * able to grant ITSELF a stricter class of powerful tools regardless of what its own
 * `tools` whitelist (even `['*']`) requests — in particular the nested-spawn `task`
 * tool, so an untrusted agent cannot fan out an unsupervised recursion of further
 * agents (the reference denies AgentTool to non-ant agents the same way). Built-in agents
 * are trusted and keep the full set; this floor is additive and cannot be overridden
 * by an agent-def allowlist. Applied via {@link applyCustomAgentDenylist}.
 */
export const CUSTOM_AGENT_DISALLOWED_TOOLS = new Set([
  ...ALL_AGENT_DISALLOWED_TOOLS,
  'task',
]);

/**
 * Subtract {@link CUSTOM_AGENT_DISALLOWED_TOOLS} from a resolved tool set when the
 * agent definition is NOT built-in (parity: the reference's source-trust tool gate). A
 * built-in agent is trusted and keeps its tools unchanged; a user/project/plugin
 * definition — which may originate from an untrusted repo — is floored so its own
 * `tools` whitelist cannot grant it a denied powerful tool. Returns the (possibly
 * narrowed) tool list; MCP tools are never in the denylist so they always survive.
 */
export function applyCustomAgentDenylist(
  tools: ToolDefinition[],
  agent: Pick<import('../skills').AgentDefinition, 'source'>,
): ToolDefinition[] {
  if (isBuiltInAgent(agent)) return tools;
  return tools.filter(t => !CUSTOM_AGENT_DISALLOWED_TOOLS.has(t.name));
}

/**
 * The sentinel permission-mode value a FORK child inherits (parity: the reference's
 * `FORK_AGENT.permissionMode = 'bubble'`, forkSubagent.ts:L67). `'bubble'` is NOT a
 * real engine permission mode — it means "do not pin a mode of my own; bubble every
 * permission decision up to the parent's live stance". A fork is an in-context clone,
 * so it must behave with EXACTLY the parent's permissions, not a fixed snapshot.
 */
export const FORK_BUBBLE_PERMISSION_MODE = BUBBLE_PERMISSION_MODE;

/**
 * Resolve the `'bubble'` fork-child sentinel to the parent's live permission mode.
 * Any other value (including undefined) passes through unchanged, so this is purely
 * additive: only an explicit `'bubble'` is rewritten. When the parent mode is not
 * threaded (bare/test contexts) `'bubble'` falls back to the interactive `'ask'`
 * baseline rather than an invalid mode. Applied BEFORE {@link PERMISSION_MODES}
 * validation so a caller/agent-def may legitimately pass `permission_mode: 'bubble'`.
 */
export function resolveBubblePermissionMode(
  mode: string | undefined,
  parentMode: string | undefined,
): string | undefined {
  if (mode === FORK_BUBBLE_PERMISSION_MODE) return parentMode ?? 'ask';
  return mode;
}

/**
 * Explicit allowlist (SOURCE OF TRUTH) of the tools an ASYNC / background
 * sub-agent (`run_in_background: true`) may use — mirrors the reference's
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
 * Mirrors the reference's `COORDINATOR_MODE_ALLOWED_TOOLS`
 * (`constants/tools.ts`: {AgentTool, TaskStop, SendMessage, SyntheticOutput}),
 * mapped to AMI's tool names:
 *  - `task`              ← AGENT_TOOL_NAME        (spawn/delegate to workers)
 *  - `send_message`      ← SEND_MESSAGE_TOOL_NAME (coordinate with teammates)
 *  - `structured_output` ← SYNTHETIC_OUTPUT_TOOL_NAME (produce the final result)
 * the reference's TaskStop / TaskOutput are deliberately NOT included: AMI's universal
 * floor ({@link ALL_AGENT_DISALLOWED_TOOLS}) makes `task_kill` / `task_output`
 * lead-only (a sub-agent calling them would terminate/collect the PARENT's tasks,
 * not its own), so they are never grantable to a spawned coordinator. The result
 * is a pure-delegation agent: it can orchestrate and emit output, nothing else.
 */
export const COORDINATOR_MODE_ALLOWED_TOOLS = new Set([
  'task', 'send_message', 'structured_output',
]);

/**
 * The read-only tool class an `explore`-mode sub-agent is restricted to — search,
 * read, web lookup, AND read-only shell (`bash`), no edit/orchestration (mirrors
 * the reference's read-only Explore agent tool bucket). Extracted to a named const
 * so {@link AGENT_MODES} can describe the explore pool as data and
 * {@link filterToolsForAgent} resolves it from the single source rather than an
 * inline literal.
 *
 * `bash` is included so an explorer can run the read-only shell operations the reference's
 * Explore agent keeps — `git status`/`git log`/`git diff`, `ls`, `cat`, `head`,
 * `tail`, `find`, `grep` (exploreAgent.ts:L47-48) — which the explore system prompt
 * already instructs it to use ("fall back to read-only `find`/`grep` via bash").
 * Without it that instruction was a dead-letter: the tool it named was stripped.
 * MUTATIONS stay impossible: {@link filterToolsForAgent} swaps the raw bash tool for
 * a {@link wrapExploreReadOnlyBash} variant that structurally refuses any command
 * that is not provably read-only.
 */
export const EXPLORE_READ_ONLY_TOOLS = new Set([
  'file_read', 'grep', 'glob', 'list_dir', 'search_symbols',
  'web_search', 'web_fetch', 'bash',
]);

/**
 * Discoverable SOURCE OF TRUTH for per-mode agent tool availability — the data
 * behind {@link filterToolsForAgent} / {@link filterToolsForAsyncAgent}. Mirrors
 * the reference's named per-context tool sets (`constants/tools.ts`:
 * EXPLORE/COORDINATOR/ASYNC_AGENT_ALLOWED_TOOLS) collected into one introspectable
 * map so "what can a <mode> agent use" has exactly one answer.
 *
 * Two rules hold across EVERY mode and are therefore NOT encoded per-entry:
 *  - MCP tools (`mcp__*`) are unconditionally available in every mode (parity:
 *    the reference's `mcp__` bypass, agentToolUtils.ts:L82-85) — see {@link isMcpTool}.
 *  - the universal {@link ALL_AGENT_DISALLOWED_TOOLS} floor is subtracted last by
 *    the caller, so no allowlist here can reintroduce a baseline-denied tool.
 */
export const AGENT_MODES: Record<'explore' | 'general' | 'coordinator' | 'async', AgentModeSpec> = {
  explore: { allow: EXPLORE_READ_ONLY_TOOLS, depthGated: false, stripsPlanMode: false },
  general: { allow: undefined, depthGated: true, stripsPlanMode: true },
  coordinator: { allow: COORDINATOR_MODE_ALLOWED_TOOLS, depthGated: true, stripsPlanMode: false },
  async: { allow: ASYNC_AGENT_ALLOWED_TOOLS, depthGated: false, stripsPlanMode: false },
};

/**
 * Internal worker-coordination tools filtered OUT of the worker-capability
 * affordance shown to a coordinator (mirrors the reference's `INTERNAL_WORKER_TOOLS`
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
 * Team-essential coordination tools that a named teammate must ALWAYS retain so it
 * stays reachable and can coordinate — even when its agent definition narrows the
 * toolset with an explicit `tools` allowlist (mirrors the reference's
 * inProcessRunner force-union of SEND_MESSAGE + TEAM_CREATE/DELETE +
 * TASK_CREATE/GET/LIST/UPDATE onto a restricted agent's declared allowlist,
 * inProcessRunner.ts:L979-995). Without this a lead could spawn a teammate scoped
 * to e.g. `['file_read']` that can never answer a shutdown request, send a result,
 * or update the shared task list — stranding it from the team. AMI's analogs:
 * `send_message` (talk to teammates), `task_list` + `task_tracker` (the shared
 * task system). The lead-only `task_output`/`task_kill` are deliberately NOT here
 * (they are in {@link ALL_AGENT_DISALLOWED_TOOLS} — a teammate calling them would
 * read/terminate the PARENT's tasks).
 */
export const TEAMMATE_ESSENTIAL_TOOLS = new Set([
  'send_message', 'task_list', 'task_tracker',
]);

/**
 * Union the {@link TEAMMATE_ESSENTIAL_TOOLS} into an already-resolved tool set,
 * pulling the concrete tool definitions from `allTools`. Only tools that (a) exist
 * in the registry, (b) are not already present, and (c) are not baseline-denied
 * ({@link ALL_AGENT_DISALLOWED_TOOLS}) are added, so the floor can never
 * reintroduce a denied tool. Returns the original array unchanged when there is
 * nothing to add (stable identity for the common no-op case). Applied only to a
 * NAMED teammate spawned into an active team whose agent def declared an explicit
 * `tools` allowlist — an unrestricted teammate already has these.
 */
export function ensureTeammateCoordinationTools(
  tools: ToolDefinition[],
  allTools: ToolDefinition[],
): ToolDefinition[] {
  const present = new Set(tools.map(t => t.name));
  const additions = allTools.filter(
    t => TEAMMATE_ESSENTIAL_TOOLS.has(t.name)
      && !present.has(t.name)
      && !ALL_AGENT_DISALLOWED_TOOLS.has(t.name),
  );
  return additions.length > 0 ? [...tools, ...additions] : tools;
}

/**
 * Agent-coordination + scheduling tools RESERVED for an in-process teammate (a
 * NAMED sub-agent spawned into a team) — never granted to a plain general/agent-def
 * sub-agent, including an untrusted one. Mirrors the reference's
 * `IN_PROCESS_TEAMMATE_ALLOWED_TOOLS` (`constants/tools.ts`: TaskCreate/Get/List/
 * Update, SendMessage, and the AGENT_TRIGGERS-gated cron tools), which the reference
 * injects selectively via an `isInProcessTeammate()` check instead of handing them
 * to every async agent. AMI registers these in the one default registry, so without
 * this gate a solo `mode:"general"` delegation (or any custom agent def) silently
 * inherited the shared team task list ({@link TEAMMATE_ESSENTIAL_TOOLS}), teammate
 * messaging, team create/delete, and the whole scheduler — capabilities that only
 * make sense for a teammate coordinating inside a live team. The `coordinator` mode
 * keeps `send_message` through its own positive allowlist
 * ({@link COORDINATOR_MODE_ALLOWED_TOOLS}); a `fork` keeps the parent's exact
 * cache-identical pool — so {@link filterTeammateOnlyTools} is applied only to the
 * other spawn paths (see the task tool's execute).
 */
export const IN_PROCESS_TEAMMATE_ONLY_TOOLS = new Set([
  'send_message', 'task_list', 'task_tracker',
  'team_create', 'team_delete',
  'cron_create', 'cron_delete', 'cron_list', 'cron_get', 'cron_run',
  'schedule_wakeup',
]);

/**
 * Strip the {@link IN_PROCESS_TEAMMATE_ONLY_TOOLS} from a resolved sub-agent tool
 * set unless the spawn is an in-process teammate (named + joining a team). A no-op
 * — returning the original array identity — for a teammate so the common path keeps
 * its coordination tools untouched. Exported for testing.
 */
export function filterTeammateOnlyTools(
  tools: ToolDefinition[],
  isInProcessTeammate: boolean,
): ToolDefinition[] {
  if (isInProcessTeammate) return tools;
  return tools.filter(t => !IN_PROCESS_TEAMMATE_ONLY_TOOLS.has(t.name));
}

/**
 * Build the runtime worker-capability affordance injected into a `coordinator`-mode
 * sub-agent's context (mirrors the reference's `getCoordinatorUserContext`). A
 * coordinator cannot touch the filesystem/shell itself — it only delegates — so it
 * must know EXACTLY which tools its spawned workers have and which MCP servers they
 * can reach, or it will mis-delegate (e.g. ask a worker to do something no worker
 * tool supports). The worker tool list is the async-agent source-of-truth set
 * ({@link ASYNC_AGENT_ALLOWED_TOOLS}) minus the internal coordination plumbing
 * ({@link INTERNAL_WORKER_TOOLS}), sorted deterministically so the block is stable
 * across spawns (cache-friendly). Connected MCP servers are appended by name when
 * present. The NAMED agent types the coordinator may delegate to (via each worker's
 * `subagent_type`) are appended too when known — mirroring the reference's practice of
 * injecting the available config surface (MCP servers + plugin/agent catalog) into an
 * agent's runtime context so it can delegate to the specialist it actually has rather
 * than guessing. Without this a coordinator (whose own prompt is the lean subagent
 * prompt, which does NOT enumerate the agent catalog) cannot see which specialized
 * worker types exist and falls back to generic `mode:"general"` delegation.
 */
export function buildCoordinatorWorkerContext(
  mcpServerNames: string[] = [],
  agentTypeNames: string[] = [],
): string {
  const workerTools = [...ASYNC_AGENT_ALLOWED_TOOLS]
    .filter(name => !INTERNAL_WORKER_TOOLS.has(name))
    .sort()
    .join(', ');
  let content = `Workers you spawn with the task tool have access to these tools: ${workerTools}.`;
  if (mcpServerNames.length > 0) {
    content += `\n\nWorkers also have access to MCP tools from these connected MCP servers: ${mcpServerNames.join(', ')}.`;
  }
  if (agentTypeNames.length > 0) {
    content += `\n\nYou can delegate to these specialized named agent types by setting a worker's subagent_type: ${agentTypeNames.join(', ')}.`;
  }
  return content;
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
export async function removeWorktreeIfClean(baseCwd: string, worktreePath: string, runningAgentCount: number): Promise<boolean> {
  try {
    // `--no-optional-locks`: a read-only status/rev-list must not take git's
    // optional index lock, so this background cleanliness check can never contend
    // with a concurrent git operation the user is running in the worktree (parity:
    // the reference prefixes all read-only status reads with --no-optional-locks,
    // utils/git.ts getIsClean/getChangedFiles).
    const status = (await execFileAsyncTask('git', [...GIT_HARDENING_ARGS, '--no-optional-locks', 'status', '--porcelain'], { cwd: worktreePath, encoding: 'utf-8', timeout: 5000 })).stdout.trim();
    const unpushed = (await execFileAsyncTask('git', [...GIT_HARDENING_ARGS, '--no-optional-locks', 'rev-list', 'HEAD', '--not', '--remotes'], { cwd: worktreePath, encoding: 'utf-8', timeout: 5000 })).stdout.trim();
    if (shouldRemoveWorktree(status, unpushed, runningAgentCount)) {
      await execFileAsyncTask('git', [...GIT_HARDENING_ARGS, 'worktree', 'remove', '--force', worktreePath], { cwd: baseCwd, timeout: 10000 });
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
export async function readWorktreeBranch(worktreePath: string): Promise<string | undefined> {
  try {
    const branch = (await execFileAsyncTask(
      'git', [...GIT_HARDENING_ARGS, 'rev-parse', '--abbrev-ref', 'HEAD'],
      { cwd: worktreePath, encoding: 'utf-8', timeout: 5000 },
    )).stdout.trim();
    return branch && branch !== 'HEAD' ? branch : undefined;
  } catch {
    return undefined;
  }
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

/**
 * ATOMICALLY reserve one agent-spawn slot: re-check the session/concurrency caps
 * ({@link checkAgentCaps}) and, only if they pass, consume the slot by incrementing
 * the spawn counter ({@link incrementSpawnCount}) — in ONE synchronous step with no
 * `await` between the check and the increment.
 *
 * This closes a check-then-act (TOCTOU) race the bare `checkAgentCaps` at the top of
 * the task handler cannot: that early check runs BEFORE the handler's awaits (the
 * pre-tool-use hook and the required-MCP-server readiness gate), while the actual
 * `incrementSpawnCount` used to run much later at the spawn commit. Two `task` calls
 * issued in parallel (one turn's parallel tool_calls) therefore BOTH observed the cap
 * as not-yet-reached, both proceeded past the awaits, and both incremented — spawning
 * `cap + N` agents and overshooting the session limit. Performing the authoritative
 * check-and-reserve as one indivisible synchronous operation at the commit point makes
 * the shared spawn-bookkeeping mutation serialized: the first concurrent spawn to reach
 * it consumes the last slot, and the next sees the already-incremented count and is
 * rejected. This is AMI's in-process analog of the reference's serialized teammate-spawn
 * critical section (the pane-backend `acquirePaneCreationLock` / `TmuxBackend`
 * `paneCreationLock` try/finally mutex) — AMI spawns sub-engines in-process rather than
 * into terminal panes, so the shared state to protect is the spawn counter, not a pane
 * split target. Reuses {@link checkAgentCaps} + {@link incrementSpawnCount} (no second
 * cap implementation). Returns `null` on success (slot consumed), or the cap error
 * string (nothing consumed).
 */
export function reserveAgentSlot(context: AgentCapContext): string | null {
  const error = checkAgentCaps(context);
  if (error) return error;
  incrementSpawnCount(context);
  return null;
}

/**
 * Mark a finished teammate idle AND fire the TeammateIdle lifecycle hook so a
 * hook can react to an idle teammate (the reference's TeammateIdle event —
 * teammate_name / team_name). Only fires when the id resolves to a real team
 * member; best-effort/fire-and-forget so hook failures never break teardown.
 */
export function markTeammateIdle(
  context: ToolContext,
  taskId: string,
  terminalStatus?: 'completed' | 'failed' | 'killed',
): void {
  setTeamMemberActive(taskId, false);
  // Mark the teammate idle on the process manager (parity: the reference's
  // InProcessTeammateTask isIdle) so a leader awaiting its idle transition via
  // waitForIdle (e.g. a graceful task_kill) is woken event-driven. No-op when the
  // task is already terminal / not a running agent.
  context.processManager?.markIdle?.(taskId, true);
  const team = getCurrentTeam();
  if (!team) return;
  const member = team.members.find(m => m.name === taskId || m.agentId === taskId);
  if (!member) return;
  // Proactive push to the team lead (PARITY, additive): a teammate that finishes
  // its turn automatically tells the lead its status, so the lead never has to
  // poll. A CLEAN finish (completed / unspecified) pushes an idle/available notice
  // (the reference's Stop hook writing createIdleNotification(agentName,
  // { idleReason: 'available' })); an ABNORMAL exit (killed / failed) instead
  // pushes a teammate_terminated notice so the lead can release the dead agent's
  // orphaned work (the reference's killTeammate injecting teammate_terminated into the
  // lead's inbox) — reporting a crashed agent as "available" would be wrong.
  // Best-effort — a messaging failure must never break teardown. Lazy require
  // avoids a static import cycle (send-message lazy-requires task). A no-op when
  // the finishing agent IS the lead.
  try {
    const sm = require('./send-message');
    if (terminalStatus === 'killed' || terminalStatus === 'failed') {
      sm.notifyLeadTeammateTerminated(member.name, `agent ${terminalStatus}`);
    } else {
      sm.notifyLeadTeammateIdle(member.name);
    }
  } catch { /* best-effort — never fail teardown on a status push */ }
  if (!context._hookManager) return;
  context._hookManager.executeTeammateIdle({ teammateName: member.name, teamName: team.name }).catch(() => {});
}

/**
 * Tear down a background sub-agent whose engine FAILED TO CONSTRUCT (a dead child),
 * reversing the roster/registry bookkeeping already recorded at spawn time so no
 * phantom `'running'` agent is left behind in the process-manager roster or the
 * resumable-agent registry. Parity: the reference's dead-target prune — a spawn whose
 * backend resource is dead is removed from the running-agent map rather than left
 * dangling. Steps: unlink the parent->child abort cascade, mark the process-manager
 * entry `failed`, fire the teammate-terminated notice + TeammateIdle hook, release
 * the agent's isolated file cache, drop its name->id / session / cwd / meta registry
 * entries (a never-started agent is not resumable), and re-persist the snapshot so
 * the on-disk store matches. Every step is best-effort — a cleanup failure must
 * never mask the original construction error.
 */
export function pruneDeadBackgroundAgent(p: PruneDeadAgentParams): void {
  const { context, taskId, agentName, effectiveCwd, unlinkParentAbort } = p;
  try { unlinkParentAbort?.(); } catch { /* best-effort — never mask the spawn error */ }
  try { context.processManager?.updateEntry(taskId, { status: 'failed', exitCode: 1 }); } catch { /* best-effort */ }
  markTeammateIdle(context, taskId, 'failed');
  try { releaseFileCache(effectiveCwd, taskId); } catch { /* best-effort */ }
  if (agentName) agentNameRegistry.delete(agentName);
  agentSessionRegistry.delete(taskId);
  agentCwdRegistry.delete(taskId);
  agentMetaRegistry.delete(taskId);
  try { persistAgentRegistry(context.cwd); } catch { /* best-effort */ }
}

/**
 * Error returned when an `explore`-mode sub-agent attempts a bash command that is
 * not provably read-only. Exported so the test asserting the read-only refusal and
 * any renderer can share the exact text (single source).
 */
export const EXPLORE_BASH_WRITE_REFUSAL =
  'Error: explore mode is READ-ONLY — this bash command is not a provably read-only ' +
  'operation and was refused. Only read-only shell commands are permitted here ' +
  '(ls, cat, head, tail, find, grep, git status/log/diff); use file_read/grep/glob ' +
  'for reads and spawn a non-explore agent (mode:"general") for anything that writes.';

/**
 * Wrap the `bash` tool for a read-only `explore`-mode sub-agent so it RETAINS bash
 * for read-only operations (parity: the reference's Explore agent keeps Bash but
 * prohibits mutations — exploreAgent.ts:L47-48) while structurally REFUSING any
 * command that is not provably read-only. The verdict reuses the canonical
 * {@link isReadOnlyBashCommand} classifier (bash-cache.ts — fail-closed: anything it
 * cannot prove side-effect-free returns false) so there is ONE definition of
 * "read-only bash" shared with the result-cache/permission layers, not a second
 * parser. Returns a shallow clone (all schema/metadata preserved) with `isReadOnly`
 * pinned true and the guarded `execute`; the original tool is never mutated.
 */
export function wrapExploreReadOnlyBash(tool: ToolDefinition): ToolDefinition {
  return {
    ...tool,
    isReadOnly: true,
    // This variant is unconditionally read-only — drop any per-input override that
    // could otherwise re-classify a refused write as writable for the permission layer.
    isReadOnlyForInput: undefined,
    execute: async (input, context) => {
      const command = typeof input.command === 'string' ? input.command : '';
      // A blank/non-string command is not a real read-only operation (the canonical
      // classifier treats an empty string as vacuously safe) — refuse it so the guard
      // never forwards an empty call to the shell.
      if (!command.trim() || !isReadOnlyBashCommand(command)) {
        return { output: EXPLORE_BASH_WRITE_REFUSAL, isError: true };
      }
      return tool.execute(input, context);
    },
  };
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
  const depth = agentDepth ?? 0;
  const maxDepth = maxAgentDepth ?? DEFAULT_MAX_AGENT_DEPTH;
  const allowNested = shouldAllowNestedTask(depth, maxDepth);

  // Resolve the mode's availability spec from the single source of truth
  // ({@link AGENT_MODES}); an unknown mode falls back to `general` (all tools).
  // `explore` and `coordinator` are positive allowlists; `general` is "all tools".
  const spec = AGENT_MODES[mode as keyof typeof AGENT_MODES] ?? AGENT_MODES.general;

  const filtered = allTools.filter(t => {
    // MCP tools (`mcp__*`) are unconditionally available in EVERY agent mode
    // (parity: the reference's mcp__ bypass) — a read-only explore or a pure-delegation
    // coordinator can still reach connected MCP servers. Checked first so the
    // per-mode allowlist below never has to enumerate dynamic MCP tool names.
    if (isMcpTool(t)) return true;
    // plan_mode is INTERACTIVE-session-only: "exit" awaits a user approval that
    // never comes in a subagent (it would hang), and its state is module-global
    // (would corrupt the parent's plan state). Stripped for the general tier.
    if (spec.stripsPlanMode && t.name === 'plan_mode') return false;
    // The nested-spawn `task` tool is gated by the nesting-depth cap.
    if (spec.depthGated && t.name === 'task' && !allowNested) return false;
    // A positive allowlist (explore/coordinator) keeps only its members; `general`
    // (allow === undefined) keeps everything that survived the rules above.
    return spec.allow ? spec.allow.has(t.name) : true;
  });

  // Explore is read-only: the `bash` it keeps (for git status/log/diff, ls, cat,
  // head, tail, find, grep) must refuse every mutating command. Swap the raw bash
  // tool for its read-only-guarded variant so the explorer can run read-only shell
  // ops without ever being able to write (parity: the reference's Explore keeps Bash but
  // prohibits mutations). Other modes keep the unguarded bash tool untouched.
  if (mode === 'explore') {
    return filtered.map(t => (t.name === 'bash' ? wrapExploreReadOnlyBash(t) : t));
  }
  return filtered;
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
  // MCP tools (`mcp__*`) stay available on the background path too (parity: the reference's
  // mcp__ bypass is unconditional across modes) — a detached agent can still reach
  // connected MCP servers; only recursion-prone/interactive tools are stripped.
  return tools.filter(t => isMcpTool(t) || AGENT_MODES.async.allow!.has(t.name));
}

/**
 * Built-in agent types that run ONCE and return a report — the parent never
 * resumes/messages them to continue, so the per-result "to continue this agent"
 * trailer is dead weight on them (mirrors the reference's
 * ONE_SHOT_BUILTIN_AGENT_TYPES in `AgentTool/constants.ts`, which gates whether the
 * agentId/SendMessage continuation trailer is emitted). `explore` MODE is the third
 * one-shot form (a read-only report), handled by {@link isOneShotAgent}.
 */
export const ONE_SHOT_BUILTIN_AGENT_TYPES = new Set(['Explore', 'Plan']);

/**
 * Whether a spawned subagent is a ONE-SHOT agent that is never continued — a
 * read-only `explore`-mode run or a named one-shot built-in agent type
 * ({@link ONE_SHOT_BUILTIN_AGENT_TYPES}). The task tool omits the "to continue this
 * agent" affordance for these (parity with the reference's one-shot trailer gate).
 */
export function isOneShotAgent(subagentType: string | undefined, mode: string): boolean {
  return mode === 'explore' || (subagentType !== undefined && ONE_SHOT_BUILTIN_AGENT_TYPES.has(subagentType));
}

/**
 * STRUCTURAL read-only-bash boundary for a plan-required subagent (parity: the reference
 * Code's planAgent, which keeps bash for read-only investigation — ls, git
 * status/log/diff, find, grep, cat, head, tail — but prohibits mutating shell
 * commands, planAgent.ts:L47-48). A plan-required spawn already clamps its permission
 * mode to `plan` ({@link applyPlanRequirement}), so a mutating bash command is denied
 * at the permission layer; this adds DEFENSE IN DEPTH by swapping the raw `bash` tool
 * for the {@link wrapExploreReadOnlyBash} variant that structurally REFUSES any
 * command the canonical classifier cannot prove side-effect-free — so even in an
 * auto-approving/yolo parent context a plan agent can never mutate the tree. Reuses
 * the SAME read-only-bash wrapper as explore mode (no second guard). A no-op when
 * `planRequired` is false, so default behavior is unchanged. Other tools pass through.
 */
export function applyPlanReadOnlyBash(tools: ToolDefinition[], planRequired: boolean): ToolDefinition[] {
  if (!planRequired) return tools;
  return tools.map(t => (t.name === 'bash' ? wrapExploreReadOnlyBash(t) : t));
}

/** Valid values for the per-spawn `system_prompt_mode` knob — the single canonical
 *  list lives in core/skills.ts (shared with {@link AgentDefinition.systemPromptMode}),
 *  re-exported here so existing importers keep resolving it from the task tool. */
export { SYSTEM_PROMPT_MODES };
export type { SystemPromptMode };

/**
 * Curated, selectable sub-agent model options (SOURCE OF TRUTH) with per-tier
 * affordance guidance and an explicit inherit-from-parent choice. Drives the
 * {@link TASK_TOOL_PROPERTIES}.model description so the advertised schema and any
 * agent-config surface describe the SAME tiers, and surfaces the AMI
 * parent-inheritance perk as the first-class default rather than a buried note.
 */
export const SUBAGENT_MODEL_OPTIONS: readonly SubagentModelOption[] = [
  { value: '', label: 'Inherit from parent (default)', description: 'Omit `model` to run the subagent on the SAME model as the main conversation — the right default for most delegations.' },
  { value: INHERIT_MODEL_SENTINEL, label: 'Inherit (explicit)', description: 'The explicit "inherit" sentinel: identical to omitting the field — run on the parent conversation\'s model.' },
  { value: 'opus', label: 'Opus (most capable)', description: 'Deepest reasoning for genuinely hard implementation/analysis; slowest and most expensive — reserve it.' },
  { value: 'sonnet', label: 'Sonnet (balanced)', description: 'Balanced capability and speed for everyday coding, edits, and investigation.' },
  { value: 'haiku', label: 'Haiku (fastest)', description: 'Fast and cheap for read-only exploration and simple lookups where top-tier reasoning is unnecessary.' },
] as const;

/**
 * Build the `model` param description from {@link SUBAGENT_MODEL_OPTIONS} so the one
 * curated tier list is the single source for both the advertised tool schema and any
 * agent-config UI. Leads with the inherit-by-default behavior, then one line per
 * named tier, then a note that any full model name / provider alias is also accepted.
 */
export function buildSubagentModelParamDescription(
  options: readonly SubagentModelOption[] = SUBAGENT_MODEL_OPTIONS,
): string {
  const tiers = options
    .filter(o => o.value !== '')
    .map(o => `"${o.value}" — ${o.description}`)
    .join(' ');
  return 'Model override for this subagent. Omit to INHERIT the parent conversation\'s model (the default). ' +
    `Curated tiers: ${tiers} ` +
    'Any full model name or provider alias ("gpt-4o", "gemini-pro", a pinned "claude-…" id) is also accepted verbatim.';
}

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
  auto_approve_tools: {
    type: 'array',
    items: { type: 'string' },
    description: 'Pre-approve (auto-run WITHOUT a permission prompt) exactly these tools for this one subagent — distinct from "allowed_tools", which governs which tools EXIST. A listed tool is auto-allowed; every unlisted tool still exists but is prompted/denied per the resolved permission_mode. Entries may be bare names ("bash", "file_read"), aliases ("Bash", "Read"), or patterned specs ("Bash(git:*)") to scope the approval to matching args. A matching deny rule still wins and destructive/arbitrary-code commands still prompt.',
  },
  model: {
    type: 'string',
    description: buildSubagentModelParamDescription(),
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
  share_abort_controller: {
    type: 'boolean',
    description: 'Whether a parent-turn abort (ESC / interrupt) cascades to this subagent. Default (true) links the child to the parent so interrupting the parent also stops the child — AMI\'s safe default that never orphans a detached agent. Set false to give the child an INDEPENDENT abort controller (parity: the reference\'s SubagentContextOverrides.shareAbortController=false) so a long-lived background delegation keeps running after the spawning turn is interrupted; you then stop it explicitly with task_kill.',
  },
  plan_mode_required: {
    type: 'boolean',
    description: 'Require this subagent to PLAN before implementing: run it read-only (explore + return an implementation plan; edits/writes/mutating bash are denied) so you can review the plan before any code is written. Overrides the resolved permission mode down to "plan" (a stricter deny-all is kept). Defaults to the AMI_SUBAGENT_PLAN_REQUIRED / CLAUDE_CODE_PLAN_MODE_REQUIRED env knob when omitted.',
  },
  agent_id: {
    type: 'string',
    description: 'Optional STABLE machine id for this agent, distinct from the display "name". When set, the agent runs (and is later resumed/collected) under exactly this id instead of a random one, so you can address it predictably without first reading back a generated id — and the display name may diverge from it (rename the name freely; the agent_id stays the resume/output key). Must be 1-64 chars of letters, digits, ".", "_", or "-" (no path separators or ".."), and unique among running agents. Ignored on resume (the resumed agent keeps its original id).',
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
  team_name: {
    type: 'string',
    description: 'Join this spawned agent to a named team (created by team_create) as a member — it is registered in that team\'s roster and tagged with the team so the lead can address and track it. The team must already exist. Requires `name` (an unnamed agent cannot be an addressable teammate). Omit to join the currently-active team, if any.',
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
  /** The sub-agent's tool definitions, used to resolve each tool call's
   *  human-readable activity phrase (`getActivityDescription`) and input-aware
   *  read/search/list classification (`isSearchOrReadCommand`) for the live
   *  progressSummary. Omitted in bare/test contexts (falls back to the shared
   *  name/input phrasing). */
  tools?: readonly ToolDefinition[];
  /** Unlink the parent→child abort cascade when the background run finishes, so a
   *  completed background agent does not leak its parent-abort listener. */
  unlinkParentAbort?: () => void;
  /** Output already accumulated by a foreground run before it was auto-backgrounded,
   *  preserved so promotion never loses work. */
  initialOutput?: string;
  /** Tool-use / token tallies carried over from a promoted foreground run. */
  initialToolUseCount?: number;
  initialTokens?: number;
  /** Start timestamp carried over so the duration stat spans the whole run. */
  startedAt?: number;
  /** The SubagentStart/Stop lifecycle payload for this background agent. When
   *  present the drain fires `executeSubagentStop(subagentHookCtx)` once the stream
   *  terminates (success, error, or stall) so a background agent's lifecycle is as
   *  observable to hooks as a foreground one (parity: the reference fires the subagent
   *  lifecycle hooks for async agents too). The matching SubagentStart is fired by
   *  the spawn site before the drain begins. */
  subagentHookCtx?: SubagentContext;
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
  // the reference's runAsyncAgentLifecycle usage block: tokens / tool uses / duration).
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
  // Gate live progress-summary generation (parity: the reference's enableSummarization =
  // coordinator || fork-subagent || sdk-progress). ON by default (AMI superset);
  // AMI_SUBAGENT_SUMMARIES=off disables it, the mode flags force-enable it. The
  // tool-use COUNT is always tracked; only the free-text progressSummary is gated.
  const summariesEnabled = resolveEnableSummarization(process.env);
  // Classified recent tool-activity window (parity: the reference's collapseReadSearch) so
  // the live progressSummary folds a run of consecutive read/search ops into one
  // compact label instead of scrolling a noisy per-file trail. Bounded so it never
  // grows unboundedly across a long background run. We track the per-call
  // collapsibility (input-aware — a read-only bash command collapses, a mutation
  // does not) alongside the names so the fold is accurate.
  const bgRecentCollapsible: boolean[] = [];
  // Resolve each tool call's human-readable activity phrase from the sub-agent's own
  // tool definitions (`getActivityDescription`), falling back to the shared
  // name+input phrasing when a tool does not define the hook.
  const describeActivity = createActivityDescriptionResolver(p.tools ?? []);
  const findTool = (name: string): ToolDefinition | undefined =>
    p.tools?.find((t) => t.name === name || t.aliases?.includes(name));
  // The text of the agent's most recent text-bearing assistant turn — the CLEAN
  // final answer written to the `.result` sidecar on completion (parity: the reference's
  // TaskOutputTool preferring the in-memory final answer over the raw transcript).
  let bgLastTurnText: string | undefined;
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
        const input = event.input ?? {};
        // Human-readable action phrase (parity: the reference's getActivityDescription),
        // e.g. "Reading src/foo.ts" / "Running npm test" — resolved from the tool's
        // own hook when present, else the shared name+input phrasing. Falls back to
        // the raw command/file_path/JSON when no phrase applies (an unknown tool).
        const summary = describeActivity(event.toolName, input)
          ?? (typeof input.command === 'string'
            ? input.command
            : typeof input.file_path === 'string'
              ? (input.file_path as string)
              : JSON.stringify(input).slice(0, 120));
        // Live tool-call progress: surface the running count on the task entry so
        // task_list can show how much work this background agent has done and the
        // model can see incremental deltas (reportProgressDelta). Mirrors the reference
        // Code's live task.progress.toolCount counter. Also surface a short AI
        // progress summary of the CURRENT action (progress.summary parity — takes
        // precedence over the static description in task_list); this stamps the
        // entry's lastActivity so a quiet agent can be flagged idle.
        bgRecentCollapsible.push(isReadOrSearchActivity(event.toolName, input, findTool(event.toolName)));
        if (bgRecentCollapsible.length > 20) bgRecentCollapsible.shift();
        // Collapse a run of consecutive read/search ops into one line (parity:
        // the reference's collapseReadSearch); a single op / a mutation keeps its own
        // per-tool detail so writes stay visible.
        const collapsed = collapseActivityRun(bgRecentCollapsible);
        context.processManager?.updateEntry(taskId, {
          toolCount: bgToolUseCount,
          // Live progress summary is gated by enableSummarization (parity); the
          // tool count is always surfaced so task_list stays accurate when off.
          ...(summariesEnabled ? { progressSummary: (collapsed ?? summary).slice(0, 80) } : {}),
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
      // Track the last text-bearing assistant turn as the clean final answer
      // (parity: the reference's finalizeAgentTool backward-walk) so the `.result` sidecar
      // holds only the deliverable, not the whole streamed transcript.
      if (event.type === 'turn_complete' && typeof event.content === 'string' && event.content.trim().length > 0) {
        bgLastTurnText = event.content;
      }
      // COOPERATIVE GRACEFUL SHUTDOWN (parity: the reference's InProcessTeammateTask
      // shutdownRequested check between turns): a graceful task_kill set the flag;
      // honour it at a TURN BOUNDARY so the in-flight turn finishes cleanly instead of
      // being truncated mid-thought (a hard kill does the truncating). Breaking here
      // drives the normal terminal bookkeeping in the finally below.
      if (event.type === 'turn_complete' && context.processManager?.isShutdownRequested?.(taskId)) {
        w('\n[graceful shutdown requested — stopping after this turn]\n');
        break;
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
    // Unlink the parent→child abort cascade now the background run is over, so a
    // finished background agent does not leak its parent-abort listener.
    p.unlinkParentAbort?.();
  }
  try { fs.closeSync(fd); } catch { /* already closed */ }
  if (!result) {
    try { fs.writeFileSync(outputPath, '(subagent produced no output)'); } catch { /* dir removed */ }
  }
  // Persist the CLEAN final answer to the `.result` sidecar so task_output can hand
  // the parent the deliverable instead of the raw streamed transcript (parity:
  // the reference's TaskOutputTool preferring the in-memory final answer). Only written
  // when the agent actually produced a text-bearing final turn; best-effort.
  if (bgLastTurnText && bgLastTurnText.trim()) {
    try { fs.writeFileSync(agentResultPath(outputPath), bgLastTurnText); } catch { /* dir removed */ }
  }
  // Completion usage summary (parity: the reference's async-agent usage block) — the
  // tokens / tool-uses / duration the model would otherwise only get from a
  // foreground run. The worktree outcome (cleaned-up / retained) is appended
  // below AFTER the agent is marked terminal, so cleanup can run.
  const bgDurationS = ((Date.now() - bgStartedAt) / 1000).toFixed(1);
  let bgFooter = `\n[Agent stats: ${bgToolUseCount} tool calls, ${bgTotalTokens} tokens, ${bgDurationS}s]`;
  // Classify the terminal state up front (parity with the reference's completed/failed/
  // killed): a stall-termination or an external abort (task_kill → abortFn →
  // subAbort.abort) is a KILL, not a crash — distinct from a genuine engine
  // `error` event / thrown exception, which is a FAILURE. Reporting a kill as
  // 'failed' would conflate a deliberate cancellation with a real error (and
  // would clobber the 'killed' status task_kill already latched).
  const killed = bgStalled || subAbort.signal.aborted;
  const terminalStatus = killed ? 'killed' : sawError ? 'failed' : 'completed';
  // The teammate has finished — mark it idle so team_delete no longer treats it as
  // an active member blocking teardown, and fire TeammateIdle. Pass the terminal
  // status so an abnormal exit (killed/failed) pushes a teammate_terminated notice
  // to the lead instead of an idle/available one.
  markTeammateIdle(context, taskId, terminalStatus);
  // Background agent's lifecycle is over — fire SubagentStop so a lifecycle hook sees
  // the SAME start/stop pair it gets for a foreground subagent (parity: the reference
  // fires the subagent lifecycle hooks for async agents too). The matching
  // SubagentStart was fired by the spawn site before this drain began.
  if (p.subagentHookCtx && context._hookManager) {
    context._hookManager.executeSubagentStop(p.subagentHookCtx).catch(() => {});
  }
  if (context.processManager) {
    const exitCode = terminalStatus === 'completed' ? 0 : 1;
    // Record the FINAL-result stats distinct from the live progress counters
    // (parity: the reference's AsyncAgentDetailDialog result-vs-progress split) — the
    // authoritative tool/token totals at the terminal transition, which task_list
    // prefers for a finished agent (falling back to the live counter if absent).
    context.processManager.updateEntry(taskId, {
      status: terminalStatus,
      exitCode,
      resultToolCount: bgToolUseCount,
      resultTokenCount: bgTotalTokens,
    });
    // Route through the DURABLE buffer (emitCompletion) rather than a bare
    // emit('complete'): with no live 'complete' listener anywhere, a bare emit
    // surfaced NO model-facing note for a finished background agent. emitCompletion
    // buffers it for the engine's per-turn drain and enriches it with the
    // status-aware summary + usage clause (duration + token/tool totals recorded on
    // the entry by the updateEntry() above) — parity with the reference's LocalAgentTask
    // completion notification. Dedup via `notified` still suppresses it when the
    // model already consumed the result via task_output.
    context.processManager.emitCompletion({ taskId, exitCode, command: `[agent] ${label}`, description: `Background agent: ${label}`, outputPath, status: terminalStatus });
  }
  // Tear down the isolated worktree on the background completion path too
  // (parity: the reference cleans the worktree when an async agent finishes). Done
  // AFTER updateEntry() flips this agent out of the 'running' state so it no
  // longer counts against runningAgentCount() for its OWN worktree; the
  // shouldRemoveWorktree() guard still keeps a dirty/unpushed worktree or one
  // another live agent is using. Append the outcome to the footer so a reader
  // of the output file learns where the work landed (or that it was cleaned up).
  if (isolation === 'worktree' && effectiveCwd !== context.cwd) {
    const runningAgents = context.processManager ? context.processManager.runningAgentCount() : 0;
    if (await removeWorktreeIfClean(context.cwd, effectiveCwd, runningAgents)) {
      bgFooter += '\n[Agent worktree cleaned up — no changes detected]';
    } else {
      bgFooter += buildWorktreeCompletionNotice(effectiveCwd, await readWorktreeBranch(effectiveCwd));
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
    'Use resume with an agent id/name to continue a prior agent with its context preserved. Before spawning a ' +
    'NEW agent for a follow-up, check whether a running or recently-completed agent of the same kind already ' +
    'exists and CONTINUE it (via resume, or send_message to a named teammate) instead of spawning a duplicate — ' +
    'a continued agent keeps its accumulated context, so it is both cheaper and better-informed than a fresh spawn.\n' +
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
  // the model is then never offered the flag (mirrors the reference's
  // fullInputSchema().omit({run_in_background:true}) gate).
  get inputSchema(): ToolInputSchema {
    const properties: Record<string, JsonSchemaProperty> = { ...TASK_TOOL_PROPERTIES };
    if (isBackgroundTasksDisabled()) {
      delete properties.run_in_background;
    }
    // `description` is REQUIRED (parity with the reference's baseInputSchema, where
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
    // Effective spawn mode: an explicit `mode` wins; otherwise, when implicit-fork
    // is enabled AND no `subagent_type` is named, an omitted mode resolves to
    // `fork` (parity: the reference's FORK_SUBAGENT schema mutation). Default stays
    // `general`. Uses the RAW `subagent_type` arg — a resume target carries its
    // own identity and never triggers an implicit fork.
    const mode = resolveSpawnMode(
      input.mode as string | undefined,
      input.subagent_type as string | undefined,
    );
    const resumeTarget = input.resume as string | undefined;
    // Durable cross-process resume (parity: the reference's readAgentMetadata /
    // getAgentTranscript): the taskId/name→session index is persisted to the project
    // state dir and rehydrated here, so a `resume` resolves an agent spawned in a
    // PRIOR process (AMI's in-memory maps are otherwise process-lifetime only).
    // Live in-memory entries still win over the snapshot.
    if (resumeTarget) rehydrateAgentRegistry(context.cwd);
    // Resolve the resume target — name, taskId, OR raw sessionId — up front so both
    // the identity restore and the session/cwd resolution below read ONE resolution.
    const resumeResolution = resumeTarget ? resolveResumeTarget(resumeTarget) : undefined;
    // Resume identity restore (parity: the reference's resumeAgent metadata restore,
    // resumeAgent.ts:L99-114): when resuming an agent and the resume call does not
    // re-supply its identity/defaults, fall back to what the ORIGINAL spawn ran
    // under (subagent_type, description, permission_mode, model) — recorded in
    // agentMetaRegistry — instead of silently re-deriving a generic general-purpose
    // agent with a fresh label. The authoritative session/cwd resolution still
    // happens in the resume block below; this only restores the identity fields.
    const resumeMeta = resumeResolution
      ? agentMetaRegistry.get(resumeResolution.resolvedId)
      : undefined;
    const subagentType = (input.subagent_type as string | undefined) ?? resumeMeta?.agentType;
    const taskDescription = (input.description as string | undefined) ?? resumeMeta?.description;
    // Caller-supplied STABLE machine id (parity: the reference's agentId distinct from
    // the display agentName, utils/teammate.ts:L44-45). Validated up front: it becomes
    // the taskId (→ on-disk output path, file-cache key, registry key), so it must be a
    // single path-safe segment. On resume the resumed agent keeps its original id, so
    // a freshly-supplied agent_id is only honored for a new spawn.
    const rawAgentId = (input.agent_id as string | undefined)?.trim();
    if (rawAgentId !== undefined && rawAgentId !== '' && !isSafeAgentId(rawAgentId)) {
      return {
        output: `Error: invalid agent_id "${rawAgentId}". Use 1-${AGENT_ID_MAX_LENGTH} chars of letters, digits, '.', '_', or '-' (no path separators or '..').`,
        isError: true,
      };
    }
    const requestedAgentId = rawAgentId && rawAgentId !== '' ? rawAgentId : undefined;
    // Reject a fresh spawn whose agent_id collides with a still-RUNNING agent (that
    // would clobber the live agent's output file / registry entry). A terminal/unknown
    // id is free to reuse; resume is exempt (it intentionally continues the same id).
    if (!resumeResolution && requestedAgentId) {
      const existingStatus = context.processManager?.getStatus(requestedAgentId);
      if (existingStatus && !isTerminalTaskStatus(existingStatus)) {
        return {
          output: `Error: agent_id "${requestedAgentId}" is already in use by a running agent. Choose a unique agent_id, or use resume to continue it.`,
          isError: true,
        };
      }
    }
    const effortOverride = input.effort as string | undefined;
    // A fork child may request `permission_mode: 'bubble'` — the sentinel that means
    // "run with the parent's live stance" (parity: the reference's FORK_AGENT 'bubble'). Resolve
    // it to the parent mode BEFORE the PERMISSION_MODES enum check so 'bubble' is accepted.
    const requestedPermissionMode = resolveBubblePermissionMode(
      (input.permission_mode as string | undefined) ?? resumeMeta?.permissionMode,
      context._parentPermissionMode,
    );
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

    // team_name (parity: the reference's Agent-tool `team_name`): join the spawned agent to
    // a NAMED, already-existing team. Resolve it up front so an invalid/nonexistent
    // team or an unnamed spawn is rejected before any sub-engine is built. When
    // omitted, the agent joins the currently-active team (if any) as before.
    const rawTeamName = input.team_name as string | undefined;
    const requestedTeamName = rawTeamName && rawTeamName.trim().length > 0 ? rawTeamName.trim() : undefined;
    if (requestedTeamName && !(input.name as string | undefined)?.trim()) {
      return { output: 'Error: team_name requires name — an unnamed agent cannot join a team as an addressable member.', isError: true };
    }
    const resolvedTeam = requestedTeamName
      ? (getCurrentTeam()?.name === requestedTeamName ? getCurrentTeam() : (loadTeamFromDisk(context.cwd, requestedTeamName)?.team ?? null))
      : getCurrentTeam();
    if (requestedTeamName && !resolvedTeam) {
      return { output: `Error: team "${requestedTeamName}" not found. Create it first with team_create, or omit team_name to join the active team.`, isError: true };
    }
    const resolvedTeamName = resolvedTeam?.name;

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
    // subagent_type values this call may spawn (mirrors the reference's allowedAgentTypes
    // / Agent(x,y) restriction). Validate the shape, then reject a requested
    // subagent_type that falls outside the permitted subset.
    const allowedAgentTypesResult = parseAllowedAgentTypes(input.allowed_agent_types);
    if ('error' in allowedAgentTypesResult) {
      return { output: `Error: ${allowedAgentTypesResult.error}`, isError: true };
    }
    const allowedAgentTypes = allowedAgentTypesResult.value;

    // allowed_tools: a per-spawn inline tool scope (mirrors the reference's spawn-time
    // permissions/allowedTools). Validate the shape here; the resolved tool set is
    // intersected with it after mode/agent-def resolution below.
    const allowedToolsResult = parseAllowedTools(input.allowed_tools);
    if ('error' in allowedToolsResult) {
      return { output: `Error: ${allowedToolsResult.error}`, isError: true };
    }
    const allowedToolsSpec = allowedToolsResult.value;

    // auto_approve_tools: a per-spawn AUTO-APPROVE allowlist (mirrors the reference's
    // spawn-time `allowedTools` auto-allow list) — DISTINCT from allowed_tools
    // (availability). Validate the shape here; the named tools become `allow`
    // permission rules appended to the child's rule set below (buildAutoApproveRules).
    const autoApproveResult = parseAutoApproveTools(input.auto_approve_tools);
    if ('error' in autoApproveResult) {
      return { output: `Error: ${autoApproveResult.error}`, isError: true };
    }
    const autoApproveSpec = autoApproveResult.value;

    if (subagentType !== undefined && !isAgentTypeAllowed(subagentType, allowedAgentTypes)) {
      const permitted = (allowedAgentTypes as string[]).length
        ? (allowedAgentTypes as string[]).join(', ')
        : '(none)';
      return {
        output: `Error: subagent_type "${subagentType}" is not permitted here. Allowed agent types: ${permitted}.`,
        isError: true,
      };
    }

    // Strict-parse the optional run_in_background flag so a malformed value (e.g.
    // "yes"/1) is REJECTED rather than silently treated as false (the trap of the
    // lenient coerceBool). Shared with bash.ts for consistent coercion.
    const ribParsed = parseOptionalBool(input.run_in_background, 'run_in_background');
    if ('error' in ribParsed) {
      return { output: `Error: ${ribParsed.error}`, isError: true };
    }

    // Per-spawn `share_abort_controller` knob (parity: the reference's
    // SubagentContextOverrides.shareAbortController). AMI's default (true) cascades a
    // parent-turn abort to the child — the safe default that never orphans a detached
    // agent. An explicit `false` opts the child OUT of the cascade (independent
    // controller) so a long-lived background delegation survives the spawning turn's
    // interrupt and is stopped explicitly via task_kill. Strict-parsed so a malformed
    // value is rejected rather than silently treated as opt-out.
    const sacParsed = parseOptionalBool(input.share_abort_controller, 'share_abort_controller', true);
    if ('error' in sacParsed) {
      return { output: `Error: ${sacParsed.error}`, isError: true };
    }
    const shareAbortController = sacParsed.value;

    // Background tasks can be globally disabled by CLAUDE_CODE_DISABLE_BACKGROUND_TASKS.
    // The flag is already omitted from the advertised schema when disabled; a stale /
    // replayed call that still sets it is rejected here so it cannot silently spawn a
    // detached agent in an environment that forbids them.
    if (ribParsed.value && isBackgroundTasksDisabled()) {
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
      if (!resumeResolution) {
        const known = [...agentSessionRegistry.keys()];
        return {
          output: `Error: no resumable agent found for "${resumeTarget}". ` +
            `Known agent ids: ${known.length ? known.join(', ') : '(none in this session)'}.`,
          isError: true,
        };
      }
      resumeSessionId = resumeResolution.sessionId;
      resumeCwd = resumeResolution.cwd;
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
      // the reference's Agent(<type>) deny rules), falling back to the tool tier
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
    // Per-agent `background: true` default (parity: the reference's agent background flag).
    // Applied only when the caller did not explicitly pass run_in_background.
    let agentBackgroundDefault = false;
    // Per-agent `isolation` default (parity: the reference's agent isolation field) — used
    // when the caller did not pass an explicit `isolation` on the task call.
    let agentIsolationDefault: string | undefined;
    // Per-agent `effort` default (parity: the reference's agent effort field) — the sub-engine
    // thinking level, used when the caller did not pass an explicit per-call `effort`.
    let agentEffort: string | undefined;
    // Per-agent `initialPrompt` (parity: the reference's agent initialPrompt) — text prepended
    // to the first user turn on the initial spawn (never on resume).
    let agentInitialPrompt: string | undefined;
    // Per-agent `skills` preload (parity: the reference's runAgent skill preloading) —
    // a one-line note naming any declared-but-unresolvable skill, prepended to the
    // agent's returned output so a typo'd/disabled preload is visible, not silent.
    let agentSkillPreloadNote = '';
    // Per-agent `color` (parity: the reference's agent/teammate `color`) — the named
    // agent's declared display color, used for the live-roster badge unless the
    // caller passes an explicit per-spawn `color` override below.
    let agentColor: AgentColorName | undefined;
    // Per-agent frontmatter hooks (parity: the reference's agent `hooks` + registerFrontmatterHooks
    // with isAgent=true) — registered on the spawned sub-engine for the duration of this
    // agent's run and torn down when it shuts down. Plugin-sourced agents never carry hooks
    // (stripped at load), so this is always author-trusted.
    let agentFrontmatterHooks: import('../hooks').HookConfigEntry[] | undefined;
    let effectiveMode = mode;
    // Whether this subagent runs a BUILT-IN identity (a bare mode like explore/general/
    // coordinator/fork, or a bundled roster agent) vs a USER-DEFINED custom agent.
    // A mode-only spawn (no subagent_type) is always a built-in mode; a named agent is
    // resolved below. Threaded into the subagent hook/attribution context (parity:
    // the reference's agentContext isBuiltIn) and used to collapse analytics names privacy-safely.
    let subagentIsBuiltIn = true;
    // Whether a named agent definition narrowed the toolset with an explicit,
    // non-wildcard `tools` allowlist. Drives the teammate-coordination-tools floor
    // ({@link ensureTeammateCoordinationTools}) so a restricted teammate still keeps
    // send_message / task coordination when spawned into a team.
    let agentHadExplicitTools = false;

    if (subagentType && context._skillManager) {
      const agentDef = context._skillManager.getAgent(subagentType);
      if (!agentDef) {
        return { output: `Error: agent "${subagentType}" not found. Available agents: ${context._skillManager.listAgents().map(a => a.name).join(', ') || '(none)'}`, isError: true };
      }
      // PARITY (additive): an agent definition that declares `requiredMcpServers`
      // cannot do its job until those servers are connected — so before building the
      // sub-engine, wait for any still-connecting required server (bounded poll,
      // early-exit on a failed one) and refuse the spawn with a guided error if a
      // required server is failed/missing. Mirrors the reference's AgentTool
      // requiredMcpServers readiness gate (AgentTool.tsx:L367-410). No-op for agents
      // without the field, so existing spawns are untouched.
      const mcpGate = await ensureRequiredMcpServersReady(
        (agentDef as { requiredMcpServers?: string[] }).requiredMcpServers,
        () => readMcpServerStates(context),
        subagentType,
      );
      if (!mcpGate.ready) return { output: mcpGate.error, isError: true };
      agentSystemPrompt = agentDef.systemPrompt;
      // Per-agent `systemPromptMode` (parity: the reference swarm teammate systemPromptMode).
      // 'append' layers the agent's own prompt AFTER the base subagent stance so the
      // agent EXTENDS the default guidance; 'replace'/'default'/absent keep the agent
      // prompt as the authoritative identity (AMI's existing replace behavior). A
      // blank agent prompt has nothing to append, so the base stance resolves via the
      // usual modeOnly fallback below.
      if ((agentDef.systemPromptMode as SystemPromptMode | undefined) === 'append' && agentDef.systemPrompt) {
        agentSystemPrompt = `${SUBAGENT_SYSTEM_PROMPT}\n\n${agentDef.systemPrompt}`;
      }
      // Per-agent project-context OPT-IN (parity: the reference's agent `omitClaudeMd`,
      // inverted for AMI's lean-subagent perk — see resolveAgentProjectContext).
      // AMI subagents normally omit the project CLAUDE.md/AGENTS.md entirely; an agent
      // that declares `omit-claude-md: false` gets those instructions appended to its
      // identity prompt so it can honor project conventions. No-op (empty) for the
      // lean default, so existing agents are untouched.
      const agentProjectContext = resolveAgentProjectContext(context.cwd, agentDef.omitClaudeMd);
      if (agentProjectContext) {
        agentSystemPrompt = `${agentSystemPrompt}\n\n${agentProjectContext}`;
      }
      agentModel = agentDef.model;
      agentMaxTurns = agentDef.maxTurns;
      // A NON-built-in (user/project/plugin) agent definition — which may come from an
      // untrusted cloned repo — may TIGHTEN but never ESCALATE the subagent's permission
      // mode past the parent's live stance (parity: the reference's runAgent permission clamp +
      // source-trust gate). A repo-local `.ami/agents/*.md` declaring
      // `permission-mode: auto-allow` therefore cannot auto-approve every tool call when
      // the parent session is in `ask` / detached `deny-all`. Built-in agents are trusted
      // and keep their declared mode. The clamp is a no-op when the parent mode is not
      // threaded (bare/test contexts), so it never regresses existing behavior.
      subagentIsBuiltIn = isBuiltInAgent(agentDef);
      // An agent definition may declare the `'bubble'` sentinel (parity: the reference's
      // async/background-agent bubble mode) — resolve it to the parent's LIVE stance
      // FIRST so every permission decision bubbles up to the parent terminal instead
      // of being auto-denied or mis-clamped as an unknown mode. A concrete declared
      // mode passes through resolveBubblePermissionMode unchanged.
      const resolvedAgentMode = resolveBubblePermissionMode(agentDef.permissionMode, context._parentPermissionMode);
      agentPermissionMode = resolvedAgentMode && !subagentIsBuiltIn
        ? clampPermissionMode(resolvedAgentMode, context._parentPermissionMode)
        : resolvedAgentMode;
      agentTokenBudget = agentDef.autoCompactWindow;
      agentBackgroundDefault = agentDef.background === true;
      agentIsolationDefault = agentDef.isolation;
      agentEffort = agentDef.effort;
      agentInitialPrompt = agentDef.initialPrompt;
      agentColor = agentDef.color;
      // Stamp the agent's bundle/base root onto its command hooks so a
      // `${CLAUDE_PLUGIN_ROOT}`-referencing agent hook resolves its bundled asset
      // path (parity with the reference's agent `baseDir` → registerFrontmatterHooks plugin
      // root). Returns a cloned list — never mutates the cached agentDef.hooks.
      agentFrontmatterHooks = withFrontmatterHookPluginRoot(agentDef.hooks, agentPluginRoot(agentDef));

      // Per-agent persistent memory (parity: the reference's loadAgentMemoryPrompt).
      // When the agent definition declares a `memory` scope, load/seed its OWN
      // file-based memory store — namespaced by agent type, separate from the
      // global auto-memory — and PREPEND it to the authoritative system prompt so
      // the spawned agent starts already carrying its durable cross-run context.
      // Keyed on the parent's cwd (its canonical git root), not the eventual
      // worktree, so every spawn of this agent in the repo shares one store.
      // Capability-gated: the per-agent memory block is only configured when the
      // auto-memory capability is active (parity with the reference's MemoryStep
      // `getSystemPrompt: isAutoMemoryEnabled() && memory && agentType ? …` and
      // the agent-creation wizard only inserting MemoryStep when
      // `isAutoMemoryEnabled()`). With `AMI_DISABLE_AUTO_MEMORY` set, a declared
      // `memory` scope is inert — the agent spawns with no memory block, matching
      // the rest of the gated auto-memory system (section/team/tool injection).
      if (agentDef.memory && isAutoMemoryEnabled()) {
        const agentMemoryBlock = buildAgentMemoryPrompt(subagentType, agentDef.memory, context.cwd);
        agentSystemPrompt = agentSystemPrompt
          ? `${agentMemoryBlock}\n\n${agentSystemPrompt}`
          : agentMemoryBlock;
      }

      // Preload the bodies of the skills this agent declares (parity: the reference's
      // runAgent skill preloading) into its authoritative system prompt, so the
      // spawned agent starts already carrying that instruction context instead of
      // having to discover + invoke each skill. Resolution reuses the SkillManager's
      // name/alias lookup (getSkill); a disabled or blank-bodied skill is treated as
      // unresolved. Unknown/invalid names are surfaced as a spawn note rather than a
      // hard failure (a missing optional skill should not strand the whole agent).
      if (agentDef.skills && agentDef.skills.length > 0) {
        // Resolution uses the SkillManager's exact name/alias lookup FIRST (covers
        // all registered skills incl. non-invocable), then falls back to the reference's
        // multi-strategy resolveSkillName over the invocable set so a
        // plugin-namespaced skill (`plugin:name`) still resolves when the agent
        // references it by its bare name and the bare form is unavailable.
        const preloadCandidates = context._skillManager!.listSkills();
        const preload = buildPreloadedSkillsBlock(
          agentDef.skills,
          (n) => context._skillManager!.getSkill(n) ?? resolveSkillName(n, preloadCandidates),
        );
        if (preload.block) {
          agentSystemPrompt = `${agentSystemPrompt}\n\n${preload.block}`;
        }
        if (preload.missing.length > 0) {
          agentSkillPreloadNote = `Note: agent "${subagentType}" references unknown or disabled skill(s) in its "skills" preload: ${preload.missing.join(', ')}.`;
        }
      }

      // Per-agent `criticalSystemReminder` (parity: the reference's
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
          // Explicit EMPTY allowlist = NO tools (parity: the reference distinguishes
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
          // The def narrowed the toolset with a concrete (non-wildcard) allowlist —
          // a named teammate spawned into a team then re-gains the essential
          // coordination tools below so it never becomes unreachable.
          agentHadExplicitTools = true;
        }
        // Deny wins even when an allowlist is set: an agent may declare BOTH
        // `tools` and `disallowedTools` (e.g. the reference's planAgent allows the
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
      // Source-trust floor: a NON-built-in agent definition cannot grant itself a
      // denied powerful tool (incl. the nested-spawn `task`) via its own `tools`
      // whitelist (parity: the reference's CUSTOM_AGENT_DISALLOWED_TOOLS on source !==
      // 'built-in'). Applied AFTER the agent's own tools/disallowedTools resolution so
      // no allowlist (even `['*']`) can reintroduce them; built-in agents are unaffected.
      tools = applyCustomAgentDenylist(tools, agentDef);
      effectiveMode = 'general';
    } else if (mode === 'explore') {
      tools = filterToolsForAgent(allTools.getAll(), 'explore', currentDepth, maxDepth);
    } else if (mode === 'coordinator') {
      tools = filterToolsForAgent(allTools.getAll(), 'coordinator', currentDepth, maxDepth);
    } else if (mode === 'fork') {
      // Cache-identical fork prefix: inherit the parent's EXACT ordered tool pool
      // ({@link selectExactForkToolPool}) when the parent threaded it, so the child's
      // serialized tool array matches the parent's cached prefix byte-for-byte instead
      // of a freshly-rebuilt general pool (parity: the reference's useExactTools). Falls back
      // to the general rebuild when no parent pool was threaded (e.g. a fork spawned
      // outside a live engine turn).
      tools = context._parentToolNames && context._parentToolNames.length > 0
        ? selectExactForkToolPool(allTools.getAll(), context._parentToolNames, currentDepth, maxDepth)
        : filterToolsForAgent(allTools.getAll(), 'general', currentDepth, maxDepth);
      effectiveMode = 'general';
    } else {
      tools = filterToolsForAgent(allTools.getAll(), 'general', currentDepth, maxDepth);
    }

    // Universal baseline denylist — applied last so no mode or explicit
    // allowlist (incl. `['*']`) can reintroduce plan_mode or workflow.
    tools = tools.filter(t => !ALL_AGENT_DISALLOWED_TOOLS.has(t.name));

    // In-process teammate-only tool gating (parity: the reference's
    // IN_PROCESS_TEAMMATE_ALLOWED_TOOLS injected only via isInProcessTeammate()).
    // Agent-coordination + scheduling tools ({@link IN_PROCESS_TEAMMATE_ONLY_TOOLS}:
    // the shared task list, teammate messaging, team create/delete, cron, and
    // schedule_wakeup) are reserved for a NAMED teammate spawned into a team. A plain
    // general / agent-def sub-agent — including an untrusted one — must not inherit
    // them. The `coordinator` mode keeps `send_message` through its own positive
    // allowlist and a `fork` keeps the parent's exact cache-identical pool, so both
    // are exempt; every other spawn path is gated. A named teammate whose def later
    // narrowed its toolset re-gains the essential coordination tools via
    // {@link ensureTeammateCoordinationTools} below.
    const isInProcessTeammateSpawn =
      !!(input.name as string | undefined)?.trim() && !!resolvedTeam;
    if (mode !== 'coordinator' && mode !== 'fork') {
      tools = filterTeammateOnlyTools(tools, isInProcessTeammateSpawn);
    }

    // Per-spawn `allowed_tools` inline scope (mirrors the reference's spawn-time
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

    // Per-spawn `auto_approve_tools` auto-allow list (mirrors the reference's spawn-time
    // `allowedTools`): the named tools are pre-approved for this subagent. Validate each
    // names a real tool (typo-safe, same as allowed_tools) BEFORE building the allow
    // rules appended to the child's permission set below. Auto-approval is independent of
    // the availability `allowed_tools` scope (a tool need not appear there to be
    // pre-approved), but an unknown name is a mistake — reject it with the registry listing.
    if (autoApproveSpec) {
      const known = new Set(allTools.getAll().map(t => t.name));
      const invalid = autoApproveSpec.filter(spec => !known.has(permissionRuleValueFromString(spec).toolName));
      if (invalid.length > 0) {
        return {
          output: `Error: auto_approve_tools names unknown tool(s): ${invalid.join(', ')}. ` +
            `Available tools: ${[...known].sort().join(', ')}.`,
          isError: true,
        };
      }
    }
    const autoApproveRules = buildAutoApproveRules(autoApproveSpec);

    // Providers often serialize booleans as the STRING "true"/"false" in tool-call
    // args; parseOptionalBool (strict-parsed above) handles both (a strict `=== true`
    // silently dropped `run_in_background: "true"` to foreground, serializing a batch
    // the model meant to run concurrently). An explicit run_in_background (even
    // "false") wins; otherwise fall back to the agent definition's `background: true`
    // default (parity: the reference's agent background).
    const runInBackground = input.run_in_background !== undefined
      ? ribParsed.value
      : agentBackgroundDefault;

    // Async/background allowlist — a detached background agent has no live parent
    // turn supervising it, so after all mode/agent-def/denylist resolution its tool
    // set is further INTERSECTED with ASYNC_AGENT_ALLOWED_TOOLS (mirrors the reference's
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
    // the reference's non-empty selected-tools boundary guard).
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
    // fork subagent (mirrors the reference's runForkedAgent maxTurns parameter). It wins
    // over an agent definition's maxTurns AND the fork default below, so an ad-hoc
    // delegation can bound its API round-trips without a named agent definition.
    const requestedMaxTurns = parsePositiveIntInput(input.max_turns);
    if (requestedMaxTurns !== undefined) {
      agentMaxTurns = requestedMaxTurns;
    }

    // A `fork` child continues the parent's full task as a context-inheriting
    // clone, so it needs a generous turn budget. When no agent definition and no
    // explicit max_turns set one, default to DEFAULT_FORK_MAX_TURNS (mirrors
    // the reference's FORK_AGENT).
    if (mode === 'fork' && agentMaxTurns === undefined) {
      agentMaxTurns = DEFAULT_FORK_MAX_TURNS;
    }

    // A sub-agent's own signal is forwarded to its nested grandchildren (and the
    // tool executor fans a child signal per parallel tool over it), so raise its
    // listener cap up front to avoid a MaxListenersExceededWarning on a wide turn.
    const subAbort = createBoundedAbortController();
    // Cascade a parent-turn abort to the subagent for BOTH foreground AND background
    // spawns (parity: the reference's createChildAbortController in registerAsyncAgent). The
    // prior code linked ONLY the foreground path, leaving a detached background
    // subagent (and its nested grandchildren) orphaned to keep running until an
    // explicit task_kill, the stall timeout, or engine shutdown when the spawning turn
    // aborted. unlinkParentAbort is invoked on foreground teardown and inside the
    // background drain's finally so a completed agent never leaks its listener.
    // When the caller opts OUT via `share_abort_controller: false` the cascade is
    // skipped entirely (independent child controller) — the unlink is then a no-op so
    // every teardown path stays uniform.
    const unlinkParentAbort = shareAbortController
      ? linkChildAbortToParent(context.abortSignal, subAbort)
      : () => {};

    // An explicit per-call `isolation` wins; otherwise fall back to the agent
    // definition's `isolation` default (parity: the reference's agent isolation field).
    const isolation = (input.isolation as string | undefined) ?? agentIsolationDefault;
    const requestedCwd = input.cwd as string | undefined;
    // `worktree_path` points the subagent at an EXISTING worktree (parity: the reference's
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
    // `cwd` and `isolation:"worktree"` are mutually exclusive (parity: the reference's
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
    // abandoned (mirrors the reference's resume worktree revalidation + touch).
    if (resumeCwd && !requestedCwd && isolation !== 'worktree') {
      try {
        if (!fs.statSync(resumeCwd).isDirectory()) throw new Error('not a directory');
        const now = new Date();
        fs.utimesSync(resumeCwd, now, now);
        effectiveCwd = resumeCwd;
      } catch (err) {
        cwdFallbackNote = `Note: the resumed agent's worktree (${resumeCwd}) no longer exists — continuing in ${effectiveCwd} instead.\n\n`;
        // Introspection parity (resumeAgent.ts:L80-92): besides the user-visible
        // header note above, emit a debug-log hint so a developer tracing a resume
        // can see WHY the agent's recorded worktree/cwd was not restored (it was
        // cleaned up) and which fallback cwd it continued in. Best-effort / behind
        // the debug-log verbosity gate — never affects the spawn outcome.
        log('task', 'resume: recorded worktree/cwd missing — falling back to parent cwd', {
          resumeTarget,
          missingCwd: resumeCwd,
          fallbackCwd: effectiveCwd,
          reason: err instanceof Error ? err.message : String(err),
        }, 'debug');
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
    // (mirrors the reference's CLAUDE_CODE_SUBAGENT_MODEL, which is checked first and
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
      // to the fast/cheap model (parity: the reference's explore agent pins external runs to
      // haiku for speed). Only downshifts a reference-family/absent parent; a non-the reference
      // parent keeps its own model. Operator-overridable via AMI_EXPLORE_MODEL. Skipped
      // for a named subagent_type, whose own identity/model governs.
      const fastModel = defaultExploreModel(parentModel);
      if (fastModel) providerConfig.model = fastModel;
    } else {
      // No per-spawn model from any higher-priority source → consult the PERSISTED
      // `subagentDefaultModel` config knob (parity: the reference's getDefaultTeammateModel).
      // A configured string sets this spawn's default; `null` means "follow the leader"
      // (keep the inherited parent model); `undefined` leaves the inherited parent model
      // in place for the provider-aware final fallback below. This is the ONLY site that
      // reads the knob, so it never fights the env/per-call/agent-def overrides above.
      const configuredModel = resolveSubagentDefaultModel(context._subagentDefaultModel, parentModel);
      if (configuredModel) providerConfig.model = configuredModel;
    }

    // Final fallback: if NO model was resolvable from any source above and the
    // parent provided none to inherit, run the subagent on the provider-aware fast
    // default instead of dispatching an empty model id (parity with the reference's
    // getHardcodedTeammateModelFallback; keeps AMI's parent-inheritance perk when a
    // model IS present — see defaultSubagentModelFallback).
    providerConfig.model = defaultSubagentModelFallback(providerConfig.model, providerConfig);

    // Bedrock data-residency boundary (parity: the reference's subagent model resolution):
    // when running on amazon-bedrock, a sub-agent must invoke an inference profile in
    // the SAME account region as its parent. If the resolved sub-agent model does not
    // already pin its own cross-region prefix, inherit the parent model's prefix so a
    // delegation never silently crosses regions (a no-op when neither carries one).
    if (providerConfig.provider === 'amazon-bedrock') {
      providerConfig.model = inheritBedrockRegionPrefix(providerConfig.model, parentModel);
    }

    // Per-call `max_output_tokens` cap (parity: the reference's ForkedAgentParams.maxOutputTokens
    // → maxOutputTokensOverride): size the sub-engine's per-request max_tokens so a
    // bounded delegation (short summary / terse report) cannot emit a full-length
    // response. Threaded through the provider config's maxTokens, which the engine
    // already honors as the request output ceiling. A non-positive/garbage value is
    // ignored (parsePositiveIntInput → undefined) rather than capping at an absurd budget.
    const requestedMaxOutputTokens = parsePositiveIntInput(input.max_output_tokens);
    if (requestedMaxOutputTokens !== undefined) {
      providerConfig.maxTokens = requestedMaxOutputTokens;
    }

    // Per-spawn `allow_permission_prompts` gate (parity: the reference's allowPermissionPrompts
    // spawn flag): when the caller passes it, false → 'deny-all' (unlisted-tool calls are
    // auto-denied, never prompted) and true → 'ask' (interactive handler). Absent → no
    // contribution, so the existing fallback chain is unchanged (additive opt-in).
    const promptGatePermissionMode = resolvePromptGatePermissionMode(input.allow_permission_prompts);

    // Per-spawn plan-required gate (parity: the reference's planModeRequired knob +
    // CLAUDE_CODE_PLAN_MODE_REQUIRED env default): when set, the subagent runs
    // read-only ('plan') — it investigates and returns an implementation plan but
    // cannot mutate the workspace until the plan is reviewed. Per-spawn input wins
    // over the env default; see applyPlanRequirement below for how it clamps the
    // otherwise-resolved mode.
    const planRequired = resolvePlanRequired(input.plan_mode_required);

    // Permission mode priority:
    // 1. Explicit `permission_mode` input (caller sets the spawned agent's mode,
    //    e.g. "plan" / "acceptEdits"; validated against PERMISSION_MODES above) —
    //    CLAMPED to the parent's active mode so a model-chosen permission_mode can
    //    never ESCALATE a subagent above the parent's safety stance (parity:
    //    the reference's spawn-boundary de-escalation, spawnUtils.ts:L45-56 — a spawned
    //    agent is never more permissive than its parent). The clamp is a no-op when
    //    the parent mode is not threaded (bare/test contexts), so it is purely
    //    additive. The agent-def mode (agentPermissionMode) is already clamped above.
    // 2. Per-spawn `allow_permission_prompts` gate (deny-all when false, ask when true)
    // 3. Agent definition's permissionMode (e.g. verifier → auto-allow; safe, restricted tools)
    // 4. explore mode → always auto-allow (read-only tool set)
    // 5. A BACKGROUND agent with NO permission-prompt handler cannot surface a
    //    prompt to anyone, so its unlisted-tool calls auto-DENY ('deny-all') rather
    //    than defaulting to 'ask' and blocking a detached run on a prompt no one can
    //    answer (parity: the reference forces shouldAvoidPermissionPrompts on a
    //    non-interactive subagent, forkedAgent.ts:L356-374). When a handler IS
    //    present (detached-YOLO / auto-allow propagation) the 'ask' default is kept
    //    so the handler still decides — no regression to background YOLO workers.
    // 6. Fallback → 'ask' (uses parent's permissionPromptHandler for TUI/YOLO propagation)
    // 7. Plan-required floor (applyPlanRequirement): when this spawn is plan-required,
    //    clamp the resolved mode down to read-only 'plan' (a stricter deny-all stays),
    //    so a delegated agent must produce a plan before it can implement.
    const resolvedPermissionMode = applyPlanRequirement(
      (requestedPermissionMode
        ? clampPermissionMode(requestedPermissionMode, context._parentPermissionMode)
        : undefined) ||
      promptGatePermissionMode ||
      agentPermissionMode ||
      (effectiveMode === 'explore'
        ? 'auto-allow'
        : runInBackground && !context._permissionPromptHandler
          ? 'deny-all'
          : 'ask'),
      planRequired,
    );

    // Permission-mode-aware runtime re-resolution of an INHERITED model (parity:
    // the reference's getRuntimeMainLoopModel on the agent-inherit path, utils/model/agent.ts
    // L80-88). When a subagent ends up running the PARENT's model — either the explicit
    // `inherit` sentinel resolved to it, or no model was specified anywhere so the spread
    // parent model was retained — re-resolve it the way the main loop would at spawn time
    // instead of copying the raw string. The behavioral difference is the deferred
    // `opusplan` alias, which runs Opus while the spawned agent is in plan mode and Sonnet
    // during execution. A concrete model id passes through unchanged, so this is a no-op
    // for the common case and NEVER rewrites an explicitly-pinned (non-inherited) model.
    if (parentModel && providerConfig.model === parentModel) {
      providerConfig.model =
        getRuntimeMainLoopModel({ permissionMode: resolvedPermissionMode, mainLoopModel: providerConfig.model })
        ?? providerConfig.model;
    }

    // Plan-required STRUCTURAL read-only-bash boundary (parity: the reference's planAgent
    // keeps bash for read-only investigation but refuses mutating commands). The
    // `plan` permission clamp above already denies mutations at the permission layer;
    // this swaps the raw bash tool for the read-only-guarded variant so a plan agent
    // cannot mutate even under an auto-approving parent. No-op when not plan-required.
    tools = applyPlanReadOnlyBash(tools, planRequired);

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

    // Per-spawn `system_prompt` knob (mirrors the reference's config.systemPrompt +
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
      // Per-spawn auto-approve rules (from `auto_approve_tools`) are prepended to the
      // inherited parent rules so this spawn's pre-approvals are in effect, while a
      // matching parent/user DENY still wins unconditionally in the permission layer.
      permissionRules: autoApproveRules.length > 0
        ? [...autoApproveRules, ...(context._parentPermissionRules ?? [])]
        : context._parentPermissionRules,
      permissionPromptHandler: context._permissionPromptHandler,
      abortController: subAbort,
      maxTurns: agentMaxTurns,
      detachedMode: !!context.detachedMode,
      agentDepth: childDepth._agentDepth,
      maxAgentDepth: childDepth._maxAgentDepth,
      // Agent provenance for the child's query-source analytics tag (parity: the reference's
      // getQuerySourceForAgent). The raw type is only emitted for a BUILT-IN identity;
      // a custom agent collapses to `agent:custom` in the helper so its name never leaks.
      agentType: subagentType || mode,
      agentIsBuiltIn: subagentIsBuiltIn,
      // Thinking is DISABLED by default for non-fork subagents to control
      // output-token cost across a fleet of delegations (mirrors the reference's runAgent,
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
      //
      // Cache-identical fork prefix (parity: the reference's forkSubagent renderedSystemPrompt
      // threading, forkSubagent.ts:L44-L71): a `fork` child with NO custom/named prompt
      // reuses the parent's ALREADY-RENDERED system prompt VERBATIM instead of leaving
      // systemPrompt undefined (which makes the child re-derive the stance from scratch —
      // re-rendering can diverge from the parent and bust the provider prompt cache). The
      // fork still carries the user-message return/worker directives below (finalSystemPrompt
      // stays undefined, so that branch is unchanged). Falls back to the re-derived stance
      // when the parent threaded no prompt (a fork spawned outside a live engine turn).
      systemPrompt: finalSystemPrompt
        || (mode === 'fork' ? context._parentSystemPrompt : undefined)
        || undefined,
      tokenBudget: agentTokenBudget || undefined,
      ...(agentFrontmatterHooks && agentFrontmatterHooks.length > 0
        ? {
            frontmatterHooks: agentFrontmatterHooks,
            // isAgent=true: this is a spawned SUBAGENT, so its frontmatter `stop` hook is
            // remapped to `subagentStop` (a completing subagent fires SubagentStop, not
            // Stop) — parity with the reference's registerFrontmatterHooks(isAgent=true).
            frontmatterHooksIsAgent: true,
            frontmatterHooksSource: subagentType ? `agent '${subagentType}'` : 'agent',
          }
        : {}),
    };

    // fork: thread the parent's REAL conversation history into the child engine as
    // seeded messages (via subConfig.initialMessages), instead of a lossy
    // assistant-string-only text summary. Done BEFORE the background/foreground
    // split so a background fork (run_in_background:true) inherits context too —
    // previously it got ZERO parent context. Drop a trailing interrupted assistant
    // turn and any assistant message with an unanswered tool_use first so the seeded
    // transcript is API-valid (the engine re-validates/heals it on construction).
    // Parity: the reference's buildForkedMessages + filterIncompleteToolCalls.
    //
    // RESUME guard (parity: the reference's resumeAgent passing forkContextMessages:undefined,
    // resumeAgent.ts:L116-L148): on a fork RESUME the sub-engine already reloads the
    // child's saved transcript from `resumeSessionId`, which ALREADY contains the
    // originally-seeded parent fork context. Re-seeding it here would replay the parent's
    // tool_use blocks a second time — duplicate tool_use IDs that corrupt the request —
    // so the fork context is seeded ONLY on the initial spawn, never on resume.
    if (mode === 'fork' && !resumeSessionId && context._parentMessages && context._parentMessages.length > 0) {
      const seeded = sanitizeTranscriptForReplay(context._parentMessages);
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

    // Per-agent `initialPrompt` (parity: the reference's agent initialPrompt): prepend the
    // agent's standing instruction ahead of the caller's task prompt on the FIRST
    // user turn only — a resumed agent already carries its seeded context, so skip it.
    if (agentInitialPrompt && !resumeSessionId) {
      effectivePrompt = `${agentInitialPrompt}\n\n${effectivePrompt}`;
    }

    // Caller-specified thoroughness dial for explore-mode agents: turns the
    // explore prompt's abstract "adapt to the requested thoroughness" into a
    // concrete, caller-chosen search depth (mirrors the reference's exploreAgent
    // thoroughness parameter). Only meaningful when exploring.
    if (thoroughness && mode === 'explore') {
      effectivePrompt = `${buildThoroughnessDirective(thoroughness)}\n\n---\n\n${effectivePrompt}`;
    }

    // Coordinator-mode worker-capability affordance (mirrors the reference's
    // getCoordinatorUserContext): a coordinator has NO file/shell tools and only
    // delegates, so it is told at runtime exactly which tools its spawned workers
    // have and which MCP servers they can reach — otherwise it mis-delegates work
    // no worker tool can do. Prepended to the coordinator's first user turn.
    if (mode === 'coordinator') {
      effectivePrompt = `${buildCoordinatorWorkerContext(connectedMcpServerNames(context), availableAgentTypeNames(context))}\n\n---\n\n${effectivePrompt}`;
    }

    // When the subagent runs in a git worktree distinct from the parent cwd — a
    // freshly created one (isolation:"worktree") OR a caller-supplied existing one
    // (worktree_path) — prepend a notice so it translates inherited paths to the
    // worktree root, re-reads stale files, and knows its changes are isolated
    // (mirrors the reference's buildWorktreeNotice).
    if ((isolation === 'worktree' || usingExistingWorktree) && effectiveCwd !== context.cwd) {
      effectivePrompt = `${buildWorktreeNotice(context.cwd, effectiveCwd)}\n\n---\n\n${effectivePrompt}`;
    }

    // Plan-required spawn: tell the subagent WHY its mutating tools are denied — it
    // must explore and return an implementation plan for review, not write code. The
    // 'plan' permission mode enforces read-only; this notice makes the boundary
    // intentional so the agent produces a plan instead of thrashing on denials.
    if (planRequired) {
      effectivePrompt = `${buildPlanRequiredNotice()}\n\n---\n\n${effectivePrompt}`;
    }

    // Sanitize the caller-supplied name against reserved routing characters
    // (`@` identity separator, `*` broadcast wildcard) and THEN reconcile it to a
    // unique one BEFORE registering it, so a reused base name does not silently
    // overwrite an earlier agent's entry and a routing-char name cannot hijack
    // addressing. A name that was ENTIRELY reserved chars sanitizes to empty →
    // fall back to no name rather than register a degenerate key.
    const rawAgentName = input.name as string | undefined;
    const sanitizedName = rawAgentName ? sanitizeAgentName(rawAgentName) : undefined;
    // Reclaim the name of a terminated prior agent (status-aware): a dead teammate
    // must not keep the canonical send_message/resume address while the respawn is
    // pushed to a suffixed alias. The probe reads the live process-manager status so
    // only completed/failed/killed holders are reclaimed; running holders still suffix.
    const agentName = sanitizedName
      ? uniqueAgentName(sanitizedName, id => isTerminalTaskStatus(context.processManager?.getStatus(id)))
      : undefined;

    // A named agent spawned while a team is active joins that team as a member
    // (see registerTeamMember below). Tell it HOW to collaborate: it is one agent
    // in a team, it reaches teammates via send_message, a plain-text reply is
    // invisible to them, and the user talks mainly to the lead (mirrors the reference
    // Code's teammatePromptAddendum). Without this the machinery exists but the
    // teammate is never told it can/should message its peers.
    if (agentName && resolvedTeam) {
      // Advertise the team's shared scratchpad (parity: the reference's coordinator
      // scratchpadDir) so a teammate knows where durable cross-worker knowledge
      // goes — it has prompt-free read/write there (teamScratchpadRules below).
      const scratchpadDir = getTeamScratchpadDir(context.cwd, resolvedTeam);
      const scratchpadNote = scratchpadDir
        ? `\n\nShared team scratchpad (durable cross-teammate knowledge, no permission prompt needed): ${scratchpadDir}`
        : '';
      effectivePrompt = `${TEAMMATE_PROMPT_ADDENDUM}${scratchpadNote}\n\n---\n\n${effectivePrompt}`;

      // PARITY (additive): a named teammate whose agent definition narrowed its
      // toolset with an explicit allowlist re-gains the essential coordination
      // tools (send_message + the shared task system) so it can always answer a
      // shutdown request, send its result, and update the team task list — even if
      // the lead scoped it to e.g. `['file_read']` (mirrors the reference's force-union of
      // team-essential tools onto a restricted agent, inProcessRunner.ts:L979-995).
      // Unrestricted teammates already have these, so this only fires for a
      // deliberately-narrowed def.
      if (agentHadExplicitTools) {
        subConfig.tools = ensureTeammateCoordinationTools(
          subConfig.tools as ToolDefinition[],
          allTools.getAll(),
        );
      }
    }

    // PARITY (additive): a named teammate spawned into a team (the active team, or a
    // team named via `team_name`) inherits the team-wide declarative allowed-path
    // permissions (teamAllowedPaths) as session allow-rules, mirroring the reference's
    // teammateInit path→rule conversion. The lead sets these when creating the team;
    // each teammate gets them on spawn so the whole team shares a consistent,
    // team-scoped path allowlist.
    if (agentName && resolvedTeam) {
      // PARITY (additive): a teammate inherits BOTH the declarative team-wide
      // allowed-path rules AND a prompt-free grant to the team's shared scratchpad
      // dir (teamScratchpadRules — the reference's gated coordinator scratchpadDir), so
      // workers persist durable cross-worker knowledge without a permission prompt.
      const teamRules = [
        ...teamAllowedPathsToRules(resolvedTeam),
        ...teamScratchpadRules(context.cwd, resolvedTeam),
      ];
      if (teamRules.length > 0) {
        subConfig.permissionRules = [...(subConfig.permissionRules ?? []), ...teamRules];
      }
    }

    if (runInBackground) {
      // ATOMIC cap re-check + slot reservation at the spawn commit (no await between
      // the check and the increment) — closes the TOCTOU race where two parallel task
      // calls both pass the early checkAgentCaps (before the hook/MCP-gate awaits) and
      // both spawn, overshooting the cap. See reserveAgentSlot.
      const bgCapError = reserveAgentSlot(context);
      if (bgCapError) return { output: `Error: ${bgCapError}`, isError: true };
      context.subagentLimiter?.recordSpawn();
      // Reuse the original taskId on resume (continue the SAME agent) rather than
      // registering a duplicate background entry/output file; honor a caller-supplied
      // stable agent_id for a fresh spawn — see resolveSpawnTaskId.
      const taskId = resolveSpawnTaskId(resumeResolution, requestedAgentId);
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
      // the resume call names none (parity: the reference's resumeAgent metadata restore).
      recordAgentMeta(taskId, { agentType: subagentType, model: subConfig.provider?.model, permissionMode: resolvedPermissionMode, description: taskDescription });
      // Persist the registry snapshot so this agent is resumable from a LATER process
      // (durable cross-process resume — parity: the reference's on-disk agent metadata).
      persistAgentRegistry(context.cwd);
      // If a team is active, a named teammate joins it as an active member so the
      // team-delete guard can see it and refuse to disband while it runs.
      if (agentName && resolvedTeam) {
        const memberRecord = { name: agentName, agentId: taskId, agentType: subagentType, model: subConfig.provider?.model, cwd: effectiveCwd };
        if (requestedTeamName) registerTeamMemberInto(context.cwd, requestedTeamName, memberRecord);
        else registerTeamMember(memberRecord);
      }
      const tasksDir = projectStateDir(context.cwd, 'tasks');
      fs.mkdirSync(tasksDir, { recursive: true });
      const outputPath = path.join(tasksDir, `${taskId}.output`);
      fs.closeSync(fs.openSync(outputPath, OUTPUT_FILE_OPEN_FLAGS));

      // Reverse-lookup the agent's registered @name (set above) for the roster /
      // task_list label so a NAMED background agent surfaces under the address a
      // leader messages it by, not just its description/prompt (useSwarmBanner parity).
      const label = resolveAgentLabel({ taskId, description: taskDescription, prompt });
      if (context.processManager) {
        context.processManager.registerAgent(taskId, {
          command: `[agent] ${label}`,
          description: `Background agent: ${label}`,
          outputPath,
          abortFn: () => { subAbort.abort(); },
          // Introspection markers (parity: the reference resumeAgent / pillLabel): tag a
          // resumed continuation vs a fresh spawn, and the team the agent joined so
          // task_list can tally distinct teams.
          resumed: !!resumeTarget,
          ...(resolvedTeamName ? { teamName: resolvedTeamName } : {}),
        });
        const fm = context.processManager.flowManager;
        if (fm) {
          const flowId = fm.getOrCreateActiveFlow(label);
          fm.addAgent(flowId, taskId);
        }
      }

      // Construct the sub-engine with a bounded retry. A background spawn registers
      // its process-manager roster entry + resumable-agent registry BEFORE the engine
      // exists, so a factory that throws (a DEAD CHILD) would otherwise orphan a
      // phantom 'running' agent that never drains. On persistent construction failure
      // we prune those entries and return a classified error instead of rejecting out
      // of the tool (parity: the reference's dead-target detection + bounded prune-and-retry).
      let subEngine: ReturnType<NonNullable<ToolContext['_engineFactory']>>;
      try {
        subEngine = buildSubEngineWithRetry(context._engineFactory, subConfig);
      } catch (err) {
        pruneDeadBackgroundAgent({ context, taskId, agentName, effectiveCwd, unlinkParentAbort });
        const msg = err instanceof Error ? err.message : String(err);
        return { output: `Error: failed to start background agent ${taskId}: ${msg}`, isError: true };
      }
      // Fire the SubagentStart lifecycle hook for this background agent with the SAME
      // attribution/lineage + discriminated `agentContextKind` payload a foreground
      // spawn emits (parity: the reference fires the subagent lifecycle hooks for async
      // agents too). Before this, a background spawn fired NO lifecycle hook, so a
      // SubagentStart/Stop hook never observed async agents. SubagentStop is fired by
      // the drain when the stream terminates (success/error/stall).
      const bgChainId = generateTaskId('chain');
      const bgSubagentHookCtx = buildSubagentHookCtx({
        context, taskId, subSessionId: subConfig.sessionId, prompt,
        mode: effectiveMode, rawMode: mode, resumed: !!resumeSessionId,
        isBuiltIn: subagentIsBuiltIn, subagentType, chainId: bgChainId,
        depth: childDepth._agentDepth, isTeammate: !!(agentName && resolvedTeam),
      });
      if (context._hookManager) {
        context._hookManager.executeSubagentStart(bgSubagentHookCtx).catch(() => {});
      }
      runBackgroundAgentDrain({
        context, subEngine, effectivePrompt, taskId, outputPath, label,
        effectiveCwd, isolation, subAbort, unlinkParentAbort, tools,
        subagentHookCtx: bgSubagentHookCtx,
      }).catch(() => {});

      return { output: `Background agent started: ${taskId}\nOutput file: ${outputPath} (its streamed transcript is appended here live).\nTo collect its result, call task_output({ task_id: "${taskId}", wait: true }) — this blocks until it finishes and returns in one call. To check progress mid-flight WITHOUT blocking, call task_output({ task_id: "${taskId}" }) (no wait) or tail the output file. Do NOT poll in a loop. When spawning several agents, issue all the task_output(wait) calls in a single turn so they run concurrently.\nDo not duplicate this agent's work — avoid working with the same files or topics it is using. Work on non-overlapping tasks, or briefly tell the user what you launched and end your response.` };
    }

    // ATOMIC cap re-check + slot reservation at the foreground spawn commit (see the
    // background path above and reserveAgentSlot): closes the parallel-spawn TOCTOU
    // overshoot before any sub-engine is created.
    const fgCapError = reserveAgentSlot(context);
    if (fgCapError) return { output: `Error: ${fgCapError}`, isError: true };
    context.subagentLimiter?.recordSpawn();
    // Reuse the original taskId on resume (continue the SAME agent) rather than
    // minting a fresh id for what is one continued run; honor a caller-supplied stable
    // agent_id for a fresh spawn — see resolveSpawnTaskId.
    const taskId = resolveSpawnTaskId(resumeResolution, requestedAgentId);
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
    // the resume call names none (parity: the reference's resumeAgent metadata restore).
    recordAgentMeta(taskId, { agentType: subagentType, model: subConfig.provider?.model, permissionMode: resolvedPermissionMode, description: taskDescription });
    // Persist the registry snapshot so this agent is resumable from a LATER process
    // (durable cross-process resume — parity: the reference's on-disk agent metadata).
    persistAgentRegistry(context.cwd);
    if (agentName && resolvedTeam) {
      const memberRecord = { name: agentName, agentId: taskId, agentType: subagentType, model: subConfig.provider?.model, cwd: effectiveCwd };
      if (requestedTeamName) registerTeamMemberInto(context.cwd, requestedTeamName, memberRecord);
      else registerTeamMember(memberRecord);
    }
    let resultText = '';

    // Shared Subagent lifecycle-hook payload for this foreground spawn. parentSessionId
    // is the INVOKING engine's session id (context.sessionId) — not the child's taskId —
    // so a SubagentStart/Stop hook can correlate the subagent boundary back to its real
    // parent across concurrent/nested runs (parity: the reference's SubagentContext.parentSessionId;
    // the prior code mis-reported the child id as its own parent). invocationKind
    // discriminates a fresh `spawn` from a `resume` of an existing agent session (parity:
    // the reference's invocationKind). Falls back to taskId only in bare/test contexts with no
    // session id.
    // Fresh per-invocation correlation id for the cross-agent attribution tree (parity:
    // the reference's per-subagent query-tracking lineage, forkedAgent.ts:L451-455 — a new
    // chainId plus a monotonically incremented depth = parent.depth + 1). childDepth
    // ._agentDepth is already the CHILD's depth (one below the parent), so it is the
    // parent.depth+1 value directly.
    const chainId = generateTaskId('chain');
    // Built from the shared {@link buildSubagentHookCtx} so the FOREGROUND and
    // BACKGROUND spawn paths emit an identical lifecycle payload (attribution,
    // lineage, and the discriminated `agentContextKind`). effectiveMode is the ctx
    // `mode`; the raw `mode` drives the privacy-safe analytics label.
    const subagentHookCtx = buildSubagentHookCtx({
      context, taskId, subSessionId: subConfig.sessionId, prompt,
      mode: effectiveMode, rawMode: mode, resumed: !!resumeSessionId,
      isBuiltIn: subagentIsBuiltIn, subagentType, chainId,
      depth: childDepth._agentDepth, isTeammate: !!(agentName && resolvedTeam),
    });

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
      // The spawned KIND (parity: the reference's task_started.task_type): the named agent
      // definition when one was requested, else the built-in mode it runs as.
      taskType: subagentType || mode,
    });

    // Make THIS foreground subagent individually abortable from the UI (parity:
    // InProcessTeammateDetailDialog 'x' → onKill for a running teammate). A foreground
    // subagent blocks the parent turn and is not a tracked background row, so without
    // this the only way to stop it was Ctrl+C aborting the whole turn. The registration
    // is cleared on teardown (finally) or when the subagent is promoted to a tracked
    // background task (which then owns cancellation via processManager.kill).
    context.processManager?.registerForegroundAbort(taskId, () => {
      subAbort.abort('aborted foreground subagent');
    });

    let toolUseCount = 0;
    let totalTokens = 0;
    // Prompt-cache introspection carried into the completion footer (parity: the reference's
    // per-fork cacheRead / cacheHitRate). Latched from usage_update stats; 0/undefined
    // when the provider reports no cache signal (clause then omitted).
    let cacheReadTokens = 0;
    let cacheHitRate: number | undefined;
    const startedAt = Date.now();
    const recentActivities: string[] = [];
    // Live-display state for this subagent (PARITY: the reference's DreamTask): a bounded
    // ring buffer of the most recent assistant turns + a 'starting'|'updating'
    // sub-phase flipped on the first file-mutating tool_use. Surfaced in the
    // stall/max-turns introspection footer so a terminated/capped agent's live
    // state (was it idle or actively changing files? what was it last doing?) is
    // observable instead of opaque.
    const liveTurns = new BackgroundLiveTurns(resolveLiveTurnRetention());
    // Record each tool call this sub-agent makes so its handoff can be safety-reviewed
    // before control returns to the parent (yolo/auto only — reviewSubagentHandoff).
    const handoffActions: HandoffAction[] = [];
    let exitReason: import('../types').ExitReason | undefined;
    let sawAbortError = false;
    // The text of the sub-agent's most recent text-bearing assistant turn. Tracking
    // the last `turn_complete` that carried text performs the reference's finalizeAgentTool
    // backward-walk incrementally, so the returned deliverable is the clean FINAL
    // answer, not every intermediate narration delta concatenated across the run.
    let lastTurnText: string | undefined;
    // The chosen deliverable (final turn text, else accumulated fallback) plus any
    // status markers; assembled after the loop and returned to the parent.
    let deliverable = '';
    // Whether this subagent's isolated worktree was torn down during cleanup. Set in
    // the `finally` so the removal runs on EVERY exit path (success, error, abort,
    // stall), mirroring the reference's cleanupWorktreeIfNeeded() living in finally — the
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
    // blocked for its full lifetime (parity: the reference's autoBackgroundMs race against a
    // background signal, AgentTool.tsx:808-833). Disabled by default (getAutoBackgroundMs
    // returns 0); the stall timeout only bounds INACTIVITY, so without this a subagent
    // that keeps emitting events blocks the parent indefinitely. The timer is cleared on
    // normal completion/error (parity: the reference's cancelAutoBackground).
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
    // Derive the background-race signal ONCE (parity: the reference creates backgroundPromise
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
        // Stamp a live cumulative usage snapshot (tokens / tool-uses / elapsed) onto
        // the relayed progress so the roster shows running spend + wall-clock as it
        // happens, not just at the terminal subagent_result (parity: task_progress usage).
        const relayed = attachSubagentUsage(
          mapChildToSubagentEvent(event, taskId, relayDepth),
          { totalTokens, toolUses: toolUseCount, durationMs: Date.now() - startedAt },
        );
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
          // Record a name+input activity label (parity: the reference's progress
          // recentActivities carry the tool name AND its input), bounded to the most
          // recent MAX_RECENT_ACTIVITIES so the trail stays compact.
          recentActivities.push(describeActivity(event.toolName, event.input));
          if (recentActivities.length > MAX_RECENT_ACTIVITIES) recentActivities.shift();
          // Live-display turn accounting (parity: the reference's DreamTask) — tallies this
          // turn's tool uses and flips the sub-phase to 'updating' on the first
          // file-mutating tool.
          liveTurns.recordToolUse(event.toolName);
          // Capture the tool call for the handoff safety review (yolo/auto only).
          const cmd = event.input?.command;
          handoffActions.push({ tool: event.toolName, command: typeof cmd === 'string' ? cmd : undefined });
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
        // Commit each assistant turn into the bounded live-display ring buffer
        // (parity: the reference's addDreamTurn — tool uses collapsed to a count, the N
        // most recent turns retained, pure no-op turns skipped).
        if (event.type === 'turn_complete') {
          liveTurns.commitTurn(typeof event.content === 'string' ? event.content : '');
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
        // Reverse-lookup the agent's registered @name (set above) so an auto-/promoted
        // background agent surfaces under its addressable name in the roster, matching
        // the fresh-background path above (useSwarmBanner parity).
        const label = resolveAgentLabel({ taskId, description: taskDescription, prompt });
        if (context.processManager) {
          // The subagent becomes a tracked background task now; its cancellation is
          // owned by processManager.kill from here, so drop the foreground-abort entry.
          context.processManager.unregisterForegroundAbort(taskId);
          context.processManager.registerAgent(taskId, {
            command: `[agent] ${label}`,
            description: `Background agent: ${label}`,
            outputPath,
            abortFn: () => { subAbort.abort(); },
            // Introspection markers (parity: the reference resumeAgent / pillLabel), same
            // as the explicit-background path above.
            resumed: !!resumeTarget,
            ...(resolvedTeamName ? { teamName: resolvedTeamName } : {}),
          });
          const fm = context.processManager.flowManager;
          if (fm) {
            const flowId = fm.getOrCreateActiveFlow(label);
            fm.addAgent(flowId, taskId);
          }
        }
        runBackgroundAgentDrain({
          context, subEngine, iterator, firstPending: pendingNext, taskId, outputPath, label,
          effectiveCwd, isolation, subAbort, unlinkParentAbort, tools,
          initialOutput: resultText, initialToolUseCount: toolUseCount, initialTokens: totalTokens, startedAt,
          // The foreground already fired SubagentStart; on promotion the foreground
          // SubagentStop is skipped (the agent lives on in the background), so the drain
          // fires it when the background run finally terminates — one start/stop pair.
          subagentHookCtx,
        }).catch(() => {});
        context._emitChild?.({
          type: 'subagent_result', nodeId: taskId, depth: relayDepth,
          success: true, summary: `auto-backgrounded after ${Math.round(autoBgMs / 1000)}s`, tokens: totalTokens, outputPath,
        });
        return { output: `Background agent started (auto-backgrounded after ${Math.round(autoBgMs / 1000)}s): ${taskId}\nThe foreground delegation ran past the auto-background threshold and is now running detached. To collect its result, call task_output({ task_id: "${taskId}", wait: true }) — this blocks until it finishes and returns in one call. Do NOT poll in a loop.` };
      }
      // Choose the clean final answer (last text turn, else accumulated fallback),
      // then append any run-status markers to IT so the parent gets the deliverable
      // plus the truncation notice — not the intermediate narration.
      deliverable = selectFinalAnswer(lastTurnText, resultText);
      if (stalled) {
        // Surface the last few tool actions (parity: the reference's recentActivities) so a
        // stalled agent's final state is observable instead of opaque.
        deliverable += `\n[Subagent timed out — no activity for ${Math.round(stallTimeoutMs / 1000)}s]${buildRecentActivityTrail(recentActivities)}${buildLiveStateTrail(liveTurns)}`;
      } else if (exitReason === 'max_turns') {
        // Distinguish a turn-capped partial result from a clean completion so the
        // parent/UI does not mistake a truncated run for a finished one.
        deliverable += `\n[Subagent stopped — reached max turns limit${agentMaxTurns ? ` (${agentMaxTurns})` : ''}]${buildLiveStateTrail(liveTurns)}`;
      }
    } catch (err) {
      unlinkParentAbort();
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
      // Always cancel the auto-background timer (parity: the reference's cancelAutoBackground
      // on completion) so it cannot fire after the run is over.
      clearAutoBg();
      // When the subagent was promoted to the background, the drain now OWNS teardown
      // (sub-engine shutdown, shell reaping, file-cache release, teammate-idle, worktree
      // cleanup) — running any of it here would kill the still-live background run.
      if (!promotedToBackground) {
        // The foreground run is over — drop its individual-abort registration so a
        // stale callback can never fire against a finished subagent.
        context.processManager?.unregisterForegroundAbort(taskId);
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
        // abort / stall paths too — not only the success return (parity: the reference runs
        // cleanupWorktreeIfNeeded in finally "to handle abort/error paths"). The
        // shouldRemoveWorktree() guard inside removeWorktreeIfClean still keeps a dirty
        // or unpushed worktree, and a worktree with a live background agent in it.
        if (isolation === 'worktree' && effectiveCwd !== context.cwd) {
          const runningAgents = context.processManager ? context.processManager.runningAgentCount() : 0;
          worktreeRemoved = await removeWorktreeIfClean(context.cwd, effectiveCwd, runningAgents);
        }
      }
    }
    unlinkParentAbort();

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
        // rediscovering it (parity: the reference's <worktree_path>/<worktree_branch>).
        deliverable += buildWorktreeCompletionNotice(effectiveCwd, await readWorktreeBranch(effectiveCwd));
      }
    }

    // Cap the subagent transcript (spills overflow to disk) so a chatty subagent
    // cannot blow up the parent's context. The cap is ALWAYS applied: an explicit
    // outputBudget wins, else TASK_MAX_OUTPUT_LENGTH, else the built-in default —
    // so subagent output is bounded even with no env var and no project config.
    const taskMaxChars = resolveTaskMaxChars(context.outputBudget?.taskMaxChars);
    const cappedResult = spillToolText(context.cwd, 'task', deliverable, taskMaxChars);

    // A stall-kill or an abort is a FAILURE, not a successful result — surface it
    // so the parent does not treat a cancelled/timed-out run as a finished one
    // (mirrors the reference's AbortError-on-abort semantics). A run whose FINAL deliverable
    // is an interrupted/synthetic message (server-error / tool-call interrupt) was
    // cancelled mid-flight too, even without an explicit abort event — treat it as a
    // failure so the parent does not act on a truncated half-answer (parity: the reference's
    // post-loop synthetic-final re-throw, AgentTool.tsx:L1205-1218).
    const failed = stalled || exitReason === 'abort' || sawAbortError || isInterruptedFinalText(deliverable);

    context._emitChild?.({
      type: 'subagent_result', nodeId: taskId, depth: relayDepth,
      success: !failed, summary: (deliverable.trim().split('\n')[0] || 'done').slice(0, 120), tokens: totalTokens,
    });
    const durationS = ((Date.now() - startedAt) / 1000).toFixed(1);
    // A metadata-only tail (empty body + stats) reads as "nothing to act on" to
    // some models and ends the parent turn prematurely — substitute an explicit
    // no-output marker (mirrors the reference's "(Subagent completed but returned no output.)").
    const rawBody = cappedResult.trim() ? cappedResult : '(Subagent completed but returned no output.)';
    // Prepend any skills-preload note (a declared preload skill that could not be
    // resolved) so the typo/disabled skill is visible to the parent, not silent.
    const bodyBase = `${cwdFallbackNote}${agentSkillPreloadNote ? `${agentSkillPreloadNote}\n\n` : ''}${rawBody}`;
    // Handoff safety review (parity: the reference's classifyHandoffIfNeeded): when the
    // sub-agent ran in an AUTONOMOUS mode (auto-allow/bypassPermissions) its tool
    // calls were auto-approved with no human in the loop, so review them across the
    // trust boundary and prepend a SECURITY WARNING if it took a dangerous action —
    // or a soft "classifier unavailable, verify" note if the reviewer could not run.
    // A no-op in interactive/ask/plan modes. The parent-supplied classifier (if any)
    // overrides the built-in dangerous-command heuristic.
    const body = await reviewSubagentHandoff({
      output: bodyBase,
      permissionMode: resolvedPermissionMode,
      actions: handoffActions,
      cwd: effectiveCwd,
      classifier: context._handoffClassifier,
    });
    // Continue-affordance: tell the parent it CAN message/resume this agent to
    // carry the work forward. Skipped for ONE-SHOT agents — a read-only `explore`
    // run OR a named one-shot built-in type (Explore/Plan) — which are never
    // continued, so the trailer is dead weight (mirrors the reference's
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
