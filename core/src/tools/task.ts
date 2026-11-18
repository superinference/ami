import * as fs from 'fs';
import { projectStateDir } from '../reference-journal';
import * as path from 'path';
import * as crypto from 'crypto';
import * as child_process from 'child_process';
import { ToolDefinition, ToolContext, ToolResult, EngineConfig, ToolInputSchema, JsonSchemaProperty, Message } from '../types';
import { createDefaultTools } from './index';
import { isMcpTool } from './tool-categories';
import { releaseFileCache } from '../file-cache';
import { isReadOnlyBashCommand } from './bash-cache';
import { isTerminalTaskStatus } from '../process-manager';
import { coerceBool, parseOptionalBool } from '../utils/coerce';
import { createBoundedAbortController, raiseAbortListenerCap } from '../utils/abort';
import { isEnvTruthy } from '../analytics';
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
import { isModelFamilyAlias, resolveModelAlias, getDefaultSubagentModel, getRuntimeMainLoopModel } from '../model-registry';
import type { ProviderConfig } from '../types';
import { validateBoundedIntEnvVar } from '../config';
import { buildPreloadedSkillsBlock, resolveSkillName, SYSTEM_PROMPT_MODES, type SystemPromptMode, parseAgentColor, AGENT_COLORS, type AgentColorName, isBuiltInAgent, agentPluginRoot, requiredMcpServerReadiness, BUBBLE_PERMISSION_MODE } from '../skills';
import { withFrontmatterHookPluginRoot } from '../hooks';
import { buildAgentMemoryPrompt } from '../memory';
import { log } from '../logger';

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
 * Env knob that enables IMPLICIT FORK (parity: Claude Code's `FORK_SUBAGENT`
 * feature gate + the schema mutation that makes `subagent_type` optional and
 * defaults an omitted type to a fork, forkSubagent.ts:L22-39). When truthy, a
 * `task` call that supplies NEITHER an explicit `mode` NOR a `subagent_type`
 * resolves to `mode: 'fork'` — a context-inheriting clone of the parent — instead
 * of the bounded `general` default. Disabled by default: the advertised default
 * stays `general`, so this never changes AMI behavior unless an operator opts in.
 * (`fork` remains always available as an explicit `mode` value regardless — this
 * gate only governs the IMPLICIT default, so it is purely additive and never
 * removes AMI's unconditional explicit-fork capability.)
 */
export const FORK_SUBAGENT_ENV = 'AMI_FORK_SUBAGENT';

/** Whether implicit-fork is enabled via {@link FORK_SUBAGENT_ENV}. */
export function isForkSubagentEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env[FORK_SUBAGENT_ENV]);
}

/**
 * Resolve the effective spawn mode for a `task` call. An explicit, non-blank
 * `mode` always wins. Otherwise, when implicit-fork is enabled
 * ({@link isForkSubagentEnabled}) AND the call names no `subagent_type` (so there
 * is no authoritative agent identity to honor), an omitted mode resolves to
 * `fork` — the context-inheriting clone (parity: Claude's omit-subagent_type →
 * fork). With implicit-fork off, or when a `subagent_type` is present, an omitted
 * mode stays `general`, preserving AMI's existing default.
 */
export function resolveSpawnMode(
  rawMode: string | undefined,
  subagentType: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
): string {
  if (typeof rawMode === 'string' && rawMode.trim() !== '') return rawMode.trim();
  if (!subagentType && isForkSubagentEnabled(env)) return 'fork';
  return 'general';
}

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
 * The taskId a spawn registers under: on RESUME, reuse the ORIGINAL agent's taskId
 * (carried by {@link resolveResumeTarget}) instead of minting a new one; on a fresh
 * spawn, generate a new id. Mirrors Claude Code's task framework, where a resume
 * re-registers under the SAME task id — carrying forward the agent's prior state and
 * output stream rather than double-registering a second background entry for what is
 * one continued agent (framework.ts:L74-117). Without this, resuming a named agent in
 * the same process spawned a NEW taskId: a duplicate task_list/processManager entry
 * and a second output file, orphaning the agent's original id.
 */
export function resolveSpawnTaskId(resume?: ResumeResolution): string {
  return resume ? resume.resolvedId : generateTaskId();
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

/**
 * Resolve a human-readable DISPLAY LABEL for a (background) agent, mirroring Claude
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

/** On-disk snapshot of the agent registries so a `resume` can resolve a
 *  previously-spawned agent across PROCESS RESTARTS (parity: Claude's
 *  readAgentMetadata / getAgentTranscript, which resolve a resumable agent from the
 *  on-disk store rather than an in-memory map). Written per project-state dir. */
interface PersistedAgentRegistry {
  names?: Record<string, string>;      // agentName -> taskId
  sessions?: Record<string, string>;   // taskId -> sub-engine sessionId
  cwds?: Record<string, string>;       // taskId -> working dir (worktree / cwd override)
  meta?: Record<string, AgentMeta>;    // taskId -> recorded identity/defaults
}

/** Absolute path of the durable agent-registry snapshot for a project (cwd). */
export function agentRegistryPath(cwd: string): string {
  return path.join(projectStateDir(cwd), 'agent-registry.json');
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

/** The resolution of a `resume` target to the sub-engine session (+ identity) to reload. */
export interface ResumeResolution {
  /** The sub-engine sessionId whose saved transcript the resume reloads. */
  sessionId: string;
  /** The isolated worktree / cwd the agent last ran in (restored on resume), if any. */
  cwd?: string;
  /** The canonical taskId the target resolved to (keys agentMetaRegistry for identity restore). */
  resolvedId: string;
}

/**
 * Resolve a `resume` target — an agent NAME, a taskId, OR a raw sessionId — to the
 * sub-engine session to reload. Mirrors Claude Code's resume, which resolves a
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
 * addressable identity (mirrors Claude Code's generateUniqueTeammateName).
 *
 * STATUS-AWARE RECLAMATION (parity: Claude Code's findTeammateTaskByAgentId prefers
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
 * Extra tools DENIED to any NON-built-in (user/project/plugin) agent definition, on
 * top of the universal {@link ALL_AGENT_DISALLOWED_TOOLS} floor. Mirrors Claude Code's
 * `CUSTOM_AGENT_DISALLOWED_TOOLS`, which `filterToolsForAgent` subtracts whenever an
 * agent's `source !== 'built-in'` (i.e. a definition loaded from a possibly untrusted
 * cloned repo's `.ami/agents/` or `.claude/agents/`). Such a definition must not be
 * able to grant ITSELF a stricter class of powerful tools regardless of what its own
 * `tools` whitelist (even `['*']`) requests — in particular the nested-spawn `task`
 * tool, so an untrusted agent cannot fan out an unsupervised recursion of further
 * agents (Claude denies AgentTool to non-ant agents the same way). Built-in agents
 * are trusted and keep the full set; this floor is additive and cannot be overridden
 * by an agent-def allowlist. Applied via {@link applyCustomAgentDenylist}.
 */
export const CUSTOM_AGENT_DISALLOWED_TOOLS = new Set([
  ...ALL_AGENT_DISALLOWED_TOOLS,
  'task',
]);

/**
 * Subtract {@link CUSTOM_AGENT_DISALLOWED_TOOLS} from a resolved tool set when the
 * agent definition is NOT built-in (parity: Claude's source-trust tool gate). A
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
 * Link a child subagent's {@link AbortController} to the PARENT turn's abort signal so
 * a parent abort CASCADES to the child (parity: Claude's `createChildAbortController`
 * in `registerAsyncAgent`). Applied to BOTH the foreground AND the background /
 * auto-backgrounded spawn paths — previously only the foreground path forwarded the
 * parent abort, leaving a detached background subagent (and its nested grandchildren)
 * orphaned to run until an explicit `task_kill`, the stall timeout, or engine
 * shutdown when the spawning turn aborted. If the parent is already aborted the child
 * is aborted immediately. Returns an UNLINK function the caller invokes when the child
 * finishes (foreground teardown, or the background drain's finally) so a completed
 * agent does not leak its listener until the next parent abort.
 */
export function linkChildAbortToParent(parent: AbortSignal, child: AbortController): () => void {
  const reasonOf = (s: AbortSignal) => (s as { reason?: unknown }).reason;
  if (parent.aborted) {
    child.abort(reasonOf(parent));
    return () => { /* nothing to unlink — never attached */ };
  }
  // Many sub-agents (and their tool-executor child signals) link to ONE parent
  // turn signal; raise its listener cap before attaching so a wide fan-out of
  // concurrent children never trips Node's default-10 MaxListenersExceededWarning.
  raiseAbortListenerCap(parent);
  const forward = () => child.abort(reasonOf(parent));
  parent.addEventListener('abort', forward, { once: true });
  return () => parent.removeEventListener('abort', forward);
}

/**
 * Relative restrictiveness of each permission mode (higher = MORE permissive /
 * dangerous). Drives {@link clampPermissionMode}: `bypassPermissions` auto-approves
 * everything, `auto-allow` auto-approves tool calls, `acceptEdits` auto-approves edits,
 * `ask` prompts, `plan` blocks mutations until approval, and `deny-all` rejects
 * everything — ordered least→most restrictive accordingly.
 */
const PERMISSION_MODE_RANK: Record<string, number> = {
  'deny-all': 0,
  'plan': 1,
  'ask': 2,
  'acceptEdits': 3,
  // 'auto' is screened autonomy: it runs without prompts (more permissive than
  // acceptEdits) but blocks risky/injection-bearing calls (less permissive than the
  // blanket auto-allow), so it ranks between them.
  'auto': 4,
  'auto-allow': 5,
  'bypassPermissions': 6,
};

/**
 * Clamp a requested subagent permission mode so it can only make the subagent MORE
 * restrictive than its parent, never escalate past the parent's stance (parity:
 * Claude's runAgent permission clamp — an agent definition's permissionMode is not
 * honored when it would be more permissive than the parent's mode). When `parentMode`
 * is unknown (bare/test contexts that do not thread it) the requested mode is returned
 * unchanged, so this is a purely additive hardening that never regresses existing
 * behavior. An unknown mode string is treated as the interactive `ask` baseline.
 */
export function clampPermissionMode(requested: string, parentMode: string | undefined): string {
  if (!parentMode) return requested;
  const reqRank = PERMISSION_MODE_RANK[requested] ?? PERMISSION_MODE_RANK.ask;
  const parRank = PERMISSION_MODE_RANK[parentMode] ?? PERMISSION_MODE_RANK.ask;
  return reqRank > parRank ? parentMode : requested;
}

/**
 * The sentinel permission-mode value a FORK child inherits (parity: Claude Code's
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
 * Privacy-safe agent-type label for analytics/attribution (parity: Claude Code's
 * subagent-context `isBuiltIn` collapse, agentContext.ts:L41-42). A BUILT-IN agent
 * type (explore/general/coordinator/fork or a bundled roster agent) is a public,
 * non-sensitive identifier and is reported verbatim. A USER-DEFINED (custom
 * project/plugin) agent name may leak a private repo's naming, so it is collapsed to
 * the opaque `'user-defined'` bucket before it ever reaches telemetry.
 */
export function collapseAgentTypeForAnalytics(
  agentType: string | undefined,
  isBuiltIn: boolean,
): string {
  if (!isBuiltIn) return 'user-defined';
  return agentType && agentType.length > 0 ? agentType : 'unknown';
}

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
 * The read-only tool class an `explore`-mode sub-agent is restricted to — search,
 * read, web lookup, AND read-only shell (`bash`), no edit/orchestration (mirrors
 * Claude Code's read-only Explore agent tool bucket). Extracted to a named const
 * so {@link AGENT_MODES} can describe the explore pool as data and
 * {@link filterToolsForAgent} resolves it from the single source rather than an
 * inline literal.
 *
 * `bash` is included so an explorer can run the read-only shell operations Claude's
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

/** How an agent mode's tool pool is derived (data form of {@link filterToolsForAgent}). */
export interface AgentModeSpec {
  /** Positive allowlist of tool names, or `undefined` for "all tools" (general). */
  allow?: ReadonlySet<string>;
  /** Whether the nested-spawn `task` tool is gated by the nesting-depth cap. */
  depthGated: boolean;
  /** `plan_mode` is stripped (interactive-only — would hang a subagent). */
  stripsPlanMode: boolean;
}

/**
 * Discoverable SOURCE OF TRUTH for per-mode agent tool availability — the data
 * behind {@link filterToolsForAgent} / {@link filterToolsForAsyncAgent}. Mirrors
 * Claude Code's named per-context tool sets (`constants/tools.ts`:
 * EXPLORE/COORDINATOR/ASYNC_AGENT_ALLOWED_TOOLS) collected into one introspectable
 * map so "what can a <mode> agent use" has exactly one answer.
 *
 * Two rules hold across EVERY mode and are therefore NOT encoded per-entry:
 *  - MCP tools (`mcp__*`) are unconditionally available in every mode (parity:
 *    Claude's `mcp__` bypass, agentToolUtils.ts:L82-85) — see {@link isMcpTool}.
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
 * Team-essential coordination tools that a named teammate must ALWAYS retain so it
 * stays reachable and can coordinate — even when its agent definition narrows the
 * toolset with an explicit `tools` allowlist (mirrors Claude Code's
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

/**
 * Poll cadence + ceiling for the pre-spawn required-MCP-server readiness gate.
 * Parity with Claude Code's AgentTool requiredMcpServers wait (AgentTool.tsx:L376-391):
 * poll every 500ms for up to 30s while a required server is still connecting.
 */
export const REQUIRED_MCP_POLL_INTERVAL_MS = 500;
export const REQUIRED_MCP_MAX_WAIT_MS = 30_000;

/**
 * Normalize a raw MCP server/client state into the `connected|failed|pending`
 * vocabulary {@link requiredMcpServerReadiness} understands. A server is
 * `connected` only when fully ready; still-connecting states (`pending`/`connecting`)
 * are `pending`; every terminal non-ready state (`error`, `needs-auth`,
 * `disconnected`, `stopped`, `disabled`) is `failed` so the gate fails fast on a
 * server that will never come up rather than polling the full ceiling.
 */
export function normalizeMcpServerState(raw: string): 'connected' | 'failed' | 'pending' {
  switch (raw) {
    case 'ready':
    case 'connected':
      return 'connected';
    case 'pending':
    case 'connecting':
      return 'pending';
    default:
      return 'failed';
  }
}

/**
 * Read the current `{ serverName -> normalized-state }` map from the tool context's
 * MCP manager (empty when no manager / `listAllServers` is wired). Normalizes each
 * raw state via {@link normalizeMcpServerState} so {@link ensureRequiredMcpServersReady}
 * can classify readiness without knowing the manager's internal state vocabulary.
 */
export function readMcpServerStates(context: ToolContext): Record<string, string> {
  const mgr = context._mcpManager as { listAllServers?: () => Array<{ name: string; state: string }> } | undefined;
  if (!mgr || typeof mgr.listAllServers !== 'function') return {};
  try {
    const out: Record<string, string> = {};
    for (const s of mgr.listAllServers()) {
      if (s && typeof s.name === 'string' && typeof s.state === 'string') {
        out[s.name] = normalizeMcpServerState(s.state);
      }
    }
    return out;
  } catch {
    return {};
  }
}

const defaultMcpGateSleep = (ms: number): Promise<void> => new Promise(resolve => { setTimeout(resolve, ms); });

/** Knobs for {@link ensureRequiredMcpServersReady} (poll ceiling + injectable clock/sleep for tests). */
export interface RequiredMcpGateOptions {
  maxWaitMs?: number;
  pollIntervalMs?: number;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
}

/**
 * Pre-spawn gate: block a subagent spawn until its agent definition's
 * `requiredMcpServers` are all connected. While ANY required server is still
 * connecting (and none has failed yet) it polls up to a bounded ceiling, EARLY-EXITING
 * the moment a required server enters a failed state (no point waiting on siblings
 * when the check will fail regardless). Returns `{ ready: true }` when every
 * requirement is satisfied (or there are no requirements), and
 * `{ ready: false, error }` — carrying the unsatisfied server patterns, the list of
 * connected servers, and a `/mcp` configure hint — when a required server is
 * failed/missing (or still pending after the ceiling). Mirrors Claude Code's
 * AgentTool requiredMcpServers readiness gate (AgentTool.tsx:L367-410). The state
 * reader is injected (not the manager) so this stays a pure, testable building block.
 */
export async function ensureRequiredMcpServersReady(
  required: readonly string[] | undefined,
  readStates: () => Record<string, string>,
  agentLabel: string,
  opts: RequiredMcpGateOptions = {},
): Promise<{ ready: true } | { ready: false; error: string }> {
  if (!required || required.length === 0) return { ready: true };
  const maxWaitMs = opts.maxWaitMs ?? REQUIRED_MCP_MAX_WAIT_MS;
  const pollIntervalMs = opts.pollIntervalMs ?? REQUIRED_MCP_POLL_INTERVAL_MS;
  const sleep = opts.sleep ?? defaultMcpGateSleep;
  const now = opts.now ?? Date.now;

  let readiness = requiredMcpServerReadiness(required, readStates());
  // Wait only while something is still connecting AND nothing has already failed —
  // a failed/missing requirement can never be satisfied by waiting.
  if (!readiness.ready && readiness.pending.length > 0 && readiness.failed.length === 0) {
    const deadline = now() + maxWaitMs;
    while (now() < deadline) {
      await sleep(pollIntervalMs);
      readiness = requiredMcpServerReadiness(required, readStates());
      if (readiness.ready) break;
      if (readiness.failed.length > 0) break; // early-exit: a required server failed
      if (readiness.pending.length === 0) break; // resolved to failed/missing
    }
  }
  if (readiness.ready) return { ready: true };
  const connected = Object.entries(readStates())
    .filter(([, st]) => st === 'connected')
    .map(([name]) => name)
    .sort((a, b) => a.localeCompare(b));
  const unsatisfied = [...readiness.failed, ...readiness.missing, ...readiness.pending];
  return {
    ready: false,
    error:
      `Error: agent "${agentLabel}" requires MCP server(s) matching: ${unsatisfied.join(', ')}. `
      + `Connected MCP servers: ${connected.length > 0 ? connected.join(', ') : 'none'}. `
      + 'Use /mcp to configure and authenticate the required MCP servers.',
  };
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
 * Resolve the PERSISTED `subagentDefaultModel` config knob (EngineConfig →
 * ToolContext._subagentDefaultModel) into a concrete model id. Applied ONLY when a
 * spawn names no model from any higher-priority source (operator env override →
 * per-call `model` → agent-definition model → explore fast default). Tri-state,
 * parity with Claude Code's getDefaultTeammateModel (tools/shared/spawnMultiAgent.ts:L72-82):
 *   - `null`      → user picked "Default (follow leader)" in /config → inherit the
 *                   PARENT's exact model (`parentModel`), so a config choice never
 *                   downgrades an otherwise-inheriting subagent;
 *   - a string    → that configured model becomes the default (the `inherit` sentinel
 *                   and family aliases resolve through {@link resolveSubagentModel}, so
 *                   this path reuses the one canonical alias/anti-downgrade table —
 *                   no second resolver);
 *   - `undefined` → knob never set → return undefined so the existing provider-aware
 *                   fallback chain ({@link defaultSubagentModelFallback}) stays in charge.
 * This WIRES the previously-inert EngineConfig.subagentDefaultModel into real spawn
 * behavior without duplicating the hardcoded provider-aware fallback (that stays in
 * {@link getDefaultSubagentModel}).
 */
export function resolveSubagentDefaultModel(
  configured: string | null | undefined,
  parentModel: string | undefined,
): string | undefined {
  if (configured === undefined) return undefined;
  if (configured === null) return parentModel;
  // A blank/whitespace-only knob is treated as unset (fall through to the fallback),
  // never dispatched as an empty model id.
  if (configured.trim() === '') return undefined;
  return resolveSubagentModel(configured, parentModel);
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
 * Drop `tool` (tool_result) messages whose `tool_call_id` no longer matches any
 * assistant tool_call in the transcript — ORPHANED tool results. This is the exact
 * complement of {@link filterIncompleteToolCalls}: that pass removes an assistant
 * turn when ANY of its tool_calls is unanswered, which strands the tool_results of
 * its OTHER (answered) calls — a result block with no preceding tool_use. A provider
 * rejects an orphaned tool_result just as it rejects an orphaned tool_use, so the
 * seed must be reconciled in BOTH directions before it is replayed. Mirrors Claude
 * Code's `filterOrphanedToolResults` (resumeAgent.ts:L70-74), the sibling of its
 * `filterUnresolvedToolUses`.
 */
export function filterOrphanedToolResults(messages: Message[]): Message[] {
  const calledIds = new Set<string>();
  for (const m of messages) {
    if (m.role === 'assistant' && m.tool_calls) {
      for (const tc of m.tool_calls) calledIds.add(tc.id);
    }
  }
  return messages.filter(m => {
    if (m.role === 'tool' && m.tool_call_id) return calledIds.has(m.tool_call_id);
    return true;
  });
}

/**
 * Reconcile a persisted/parent transcript into an API-valid seed before it is
 * replayed into a `fork` child or a resumed sub-engine. Applies the full
 * sanitization pipeline in order: drop a trailing interrupted assistant turn
 * ({@link stripIncompleteTrailingMessages}), drop assistant turns with an unanswered
 * tool_use ({@link filterIncompleteToolCalls}), then drop the tool_results that
 * dropping those turns ORPHANED ({@link filterOrphanedToolResults}). The last step
 * is what the prior fork seed (which only ran the first two) omitted, leaking an
 * orphaned-tool_result into the child's provider request. Mirrors Claude Code's
 * resume/fork transcript reconciliation (`filterUnresolvedToolUses` +
 * `filterOrphanedToolResults`, resumeAgent.ts:L70-74) so the child never inherits a
 * half-answered tool exchange.
 */
export function sanitizeTranscriptForReplay(messages: Message[]): Message[] {
  return filterOrphanedToolResults(
    filterIncompleteToolCalls(stripIncompleteTrailingMessages(messages)),
  );
}

/**
 * Whether a sub-agent's final deliverable text is an INTERRUPTED/synthetic ending
 * rather than a clean answer — it carries one of the in-band interrupt sentinels
 * ({@link INCOMPLETE_ASSISTANT_SENTINEL_RE}: a server-error or tool-call interrupt).
 * A run that ends on such a message was CANCELLED mid-flight, not completed; the
 * parent must treat it as a failure (so it does not act on a truncated half-answer),
 * mirroring Claude Code's post-loop check that re-throws `AbortError` when the final
 * message is a synthetic interruption (AgentTool.tsx:L1205-1218). AMI messages carry
 * no `stop_reason`, so the in-band sentinel is the discriminator.
 */
export function isInterruptedFinalText(text: string | undefined): boolean {
  return typeof text === 'string' && INCOMPLETE_ASSISTANT_SENTINEL_RE.test(text);
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

/** Max recent tool activities retained for the live-progress trail (parity: Claude
 *  Code's MAX_RECENT_ACTIVITIES window on a backgrounded task's progress object). */
export const MAX_RECENT_ACTIVITIES = 5;

/**
 * One-line label for a tool call recorded into a sub-agent's recent-activity trail:
 * the tool name plus a short, single-line summary of its most salient input
 * (`bash(git status)`, `file_read(src/app.ts)`), mirroring Claude Code's progress
 * `recentActivities` entries which carry the tool name AND its input. The input hint
 * is the first present of the common salient fields (command / file_path / path /
 * pattern / query / url), trimmed, de-newlined, and capped so the trail stays compact.
 * Returns bare `tool:<name>` when no salient input field is present.
 */
export function describeActivity(toolName: string, input: Record<string, unknown> | undefined): string {
  const salient = ['command', 'file_path', 'path', 'pattern', 'query', 'url', 'description'];
  let hint: string | undefined;
  if (input) {
    for (const k of salient) {
      const v = input[k];
      if (typeof v === 'string' && v.trim()) { hint = v; break; }
    }
  }
  if (!hint) return `tool:${toolName}`;
  const compact = hint.replace(/\s+/g, ' ').trim().slice(0, 40);
  return `${toolName}(${compact})`;
}

/**
 * Render a sub-agent's recent-activity trail into a human-readable clause for a
 * completion/stall notice (parity: surfacing Claude Code's progress
 * `recentActivities` so the last few tool actions are observable). Returns an empty
 * string when there is nothing to show, so callers can append unconditionally.
 */
export function buildRecentActivityTrail(activities: readonly string[]): string {
  if (activities.length === 0) return '';
  return ` Recent activity: ${activities.join(', ')}.`;
}

/**
 * Render a sub-agent's live-display state (PARITY: Claude's DreamTask phase + turn
 * buffer) into a clause for a terminated/capped completion notice: the sub-phase
 * ('starting' = began but never changed files, 'updating' = actively mutating
 * files) plus a short snippet of the most recent retained assistant turn from the
 * bounded ring buffer. Returns an empty string when nothing was tracked, so callers
 * can append unconditionally.
 */
export function buildLiveStateTrail(liveTurns: BackgroundLiveTurns): string {
  const last = liveTurns.lastTurn();
  let clause = ` [${liveTurns.describe()}]`;
  if (last && last.text.trim()) {
    const snippet = last.text.replace(/\s+/g, ' ').trim().slice(0, 80);
    clause += ` [last turn: "${snippet}"]`;
  }
  return clause;
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
export function markTeammateIdle(
  context: ToolContext,
  taskId: string,
  terminalStatus?: 'completed' | 'failed' | 'killed',
): void {
  setTeamMemberActive(taskId, false);
  // Mark the teammate idle on the process manager (parity: Claude Code's
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
  // (Claude's Stop hook writing createIdleNotification(agentName,
  // { idleReason: 'available' })); an ABNORMAL exit (killed / failed) instead
  // pushes a teammate_terminated notice so the lead can release the dead agent's
  // orphaned work (Claude's killTeammate injecting teammate_terminated into the
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
 * Max attempts to construct a background sub-agent's engine before pruning it as a
 * dead child. Parity: Claude Code's bounded prune-and-retry for a teammate-spawn
 * resource (utils/swarm/backends/ITermBackend.ts:L131-208 — a momentarily-dead
 * target is retried a bounded number of times, and only a PERSISTENT failure is
 * treated as systemic). One retry (two attempts) distinguishes a transient
 * construction failure from a hard systemic one without unbounded looping.
 */
export const BACKGROUND_SPAWN_MAX_ATTEMPTS = 2;

/**
 * Construct a sub-agent engine with a BOUNDED retry so a transient factory failure
 * (a momentarily-dead resource) does not immediately abort the spawn, while a
 * persistent failure still surfaces after {@link BACKGROUND_SPAWN_MAX_ATTEMPTS}
 * attempts (a systemic error). Rethrows the LAST error when every attempt fails.
 * Mirrors Claude's dead-target detection: retry a bounded number of times, then
 * give up so the caller can prune the roster rather than hang on a dead child.
 */
export function buildSubEngineWithRetry(
  factory: NonNullable<ToolContext['_engineFactory']>,
  subConfig: EngineConfig,
  maxAttempts: number = BACKGROUND_SPAWN_MAX_ATTEMPTS,
): ReturnType<NonNullable<ToolContext['_engineFactory']>> {
  let lastErr: unknown;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return factory(subConfig);
    } catch (err) {
      lastErr = err;
    }
  }
  throw lastErr;
}

/** Inputs for {@link pruneDeadBackgroundAgent}. */
export interface PruneDeadAgentParams {
  context: ToolContext;
  taskId: string;
  agentName?: string;
  effectiveCwd: string;
  unlinkParentAbort?: () => void;
}

/**
 * Tear down a background sub-agent whose engine FAILED TO CONSTRUCT (a dead child),
 * reversing the roster/registry bookkeeping already recorded at spawn time so no
 * phantom `'running'` agent is left behind in the process-manager roster or the
 * resumable-agent registry. Parity: Claude's dead-target prune — a spawn whose
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
 * for read-only operations (parity: Claude Code's Explore agent keeps Bash but
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
    // (parity: Claude's mcp__ bypass) — a read-only explore or a pure-delegation
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
  // ops without ever being able to write (parity: Claude's Explore keeps Bash but
  // prohibits mutations). Other modes keep the unguarded bash tool untouched.
  if (mode === 'explore') {
    return filtered.map(t => (t.name === 'bash' ? wrapExploreReadOnlyBash(t) : t));
  }
  return filtered;
}

/**
 * Select a `fork` child's tool pool from the parent engine's EXACT ordered tool
 * names ({@link ToolContext._parentToolNames}) rather than rebuilding a general
 * pool. The child keeps the parent's tools in the parent's order (intersected with
 * the live registry — a tool the parent had but the child's registry lacks is
 * skipped) so the serialized tool array — and therefore the provider prompt-cache
 * prefix — stays byte-identical to the parent's. Mirrors Claude Code's
 * `useExactTools` / `tools:['*']` fork inheritance (forkSubagent.ts:L44-L71), which
 * deliberately avoids re-deriving the pool under a different permission mode because
 * that reorders tools / changes a tool-def's serialization and busts the cache at the
 * first differing tool.
 *
 * The one deviation from byte-exact is the nested-spawn depth cap: `task` is dropped
 * when the child is at/over the nesting limit ({@link shouldAllowNestedTask}), exactly
 * as {@link filterToolsForAgent} does — an unbounded recursive re-fork is never worth a
 * cache hit. The universal subagent denylist ({@link ALL_AGENT_DISALLOWED_TOOLS}) is
 * still applied by the caller afterwards. Returns the ordered pool; a parent name with
 * no live tool is silently skipped. Exported for testing.
 */
export function selectExactForkToolPool(
  allTools: ToolDefinition[],
  parentToolNames: readonly string[],
  agentDepth?: number,
  maxAgentDepth?: number,
): ToolDefinition[] {
  const allowNested = shouldAllowNestedTask(agentDepth ?? 0, maxAgentDepth ?? DEFAULT_MAX_AGENT_DEPTH);
  const byName = new Map(allTools.map(t => [t.name, t] as const));
  const seen = new Set<string>();
  const pool: ToolDefinition[] = [];
  for (const name of parentToolNames) {
    if (seen.has(name)) continue;            // de-dupe a parent list with repeats
    if (name === 'task' && !allowNested) continue;
    const tool = byName.get(name);
    if (!tool) continue;                     // parent had it, child's registry doesn't
    seen.add(name);
    pool.push(tool);
  }
  return pool;
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
  // MCP tools (`mcp__*`) stay available on the background path too (parity: Claude's
  // mcp__ bypass is unconditional across modes) — a detached agent can still reach
  // connected MCP servers; only recursion-prone/interactive tools are stripped.
  return tools.filter(t => isMcpTool(t) || AGENT_MODES.async.allow!.has(t.name));
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

/**
 * Env knob that forces EVERY spawned subagent into read-only PLAN mode by default —
 * the agent explores and must return an implementation plan, it may NOT mutate the
 * workspace (edit/write/bash-write are denied by the `plan` permission mode) until a
 * human/parent has reviewed that plan. Mirrors Claude Code's `planModeRequired`
 * teammate knob and its `CLAUDE_CODE_PLAN_MODE_REQUIRED` env default (the Claude name
 * is honored too, matching how AMI accepts the Claude spelling for other knobs). A
 * per-spawn `plan_mode_required` input overrides this default either way.
 */
export const SUBAGENT_PLAN_REQUIRED_ENV = 'AMI_SUBAGENT_PLAN_REQUIRED';

/** Claude Code cross-framework alias for {@link SUBAGENT_PLAN_REQUIRED_ENV}. */
export const CLAUDE_PLAN_MODE_REQUIRED_ENV = 'CLAUDE_CODE_PLAN_MODE_REQUIRED';

/** True when spawned subagents default to plan-required via either env spelling. */
export function isSubagentPlanRequiredByEnv(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env[SUBAGENT_PLAN_REQUIRED_ENV]) || isEnvTruthy(env[CLAUDE_PLAN_MODE_REQUIRED_ENV]);
}

/**
 * Resolve whether THIS spawn must run plan-required (read-only until a plan is
 * reviewed). Three-tier, matching Claude Code's `isPlanModeRequired()` precedence:
 * an explicit per-spawn `plan_mode_required` input wins; absent → the env default
 * ({@link isSubagentPlanRequiredByEnv}). A missing/garbage input contributes nothing
 * so the env default decides — purely additive (default off → no change).
 */
export function resolvePlanRequired(raw: unknown, env: NodeJS.ProcessEnv = process.env): boolean {
  if (raw !== undefined && raw !== null) return coerceBool(raw);
  return isSubagentPlanRequiredByEnv(env);
}

/**
 * Clamp a resolved permission mode to an "at least read-only plan" floor when the
 * spawn is plan-required. The `plan` mode allows read-only tools and DENIES every
 * mutation, so forcing it is always a DE-escalation. `deny-all` is stricter still
 * (denies reads too) and is never loosened; everything more permissive than `plan`
 * (ask/acceptEdits/auto-allow/bypassPermissions) is locked down to `plan`. A no-op
 * when `planRequired` is false, so the existing resolution is unchanged by default.
 */
export function applyPlanRequirement(mode: string, planRequired: boolean): string {
  if (!planRequired) return mode;
  return mode === 'deny-all' ? mode : 'plan';
}

/**
 * Notice prepended to a plan-required subagent's first task turn so it knows the
 * read-only boundary is intentional: produce a concrete implementation plan and
 * return it, do NOT attempt to write code (edits/writes/mutating bash are denied
 * and will fail). Mirrors Claude Code's plan-mode spawn directive.
 */
export function buildPlanRequiredNotice(): string {
  return 'PLAN MODE REQUIRED: You are running read-only. Investigate, then return a concrete, step-by-step implementation plan for this task. Do NOT edit files, write files, or run mutating shell commands — those tools are denied in this mode and any such attempt will fail. Hand the plan back for review; implementation happens only after it is approved.';
}

/**
 * STRUCTURAL read-only-bash boundary for a plan-required subagent (parity: Claude
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
 * One curated, selectable sub-agent model option with a per-tier affordance
 * description (parity: Claude Code's curated agent model options with per-tier
 * guidance + an explicit "Inherit from parent" choice, `utils/model/agent.ts`).
 * `value` is what the caller passes as the `model` arg — the empty string denotes
 * the default of OMITTING the field (inherit the parent conversation's model).
 */
export interface SubagentModelOption {
  /** The `model` arg to pass; '' means omit the field (inherit the parent). */
  value: string;
  /** Short human label. */
  label: string;
  /** When to pick this tier. */
  description: string;
}

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
  plan_mode_required: {
    type: 'boolean',
    description: 'Require this subagent to PLAN before implementing: run it read-only (explore + return an implementation plan; edits/writes/mutating bash are denied) so you can review the plan before any code is written. Overrides the resolved permission mode down to "plan" (a stricter deny-all is kept). Defaults to the AMI_SUBAGENT_PLAN_REQUIRED / CLAUDE_CODE_PLAN_MODE_REQUIRED env knob when omitted.',
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

/**
 * Path of the clean FINAL-ANSWER sidecar for a background agent, derived from its
 * `<taskId>.output` transcript path (`<taskId>.result`). Parity with Claude Code's
 * TaskOutputTool preferring the agent's in-memory final answer over the raw on-disk
 * transcript: the `.output` file streams the whole noisy run (thinking, tool calls,
 * tool results), whereas `.result` holds ONLY the agent's last text-bearing turn —
 * the deliverable the parent actually wants. A non-agent background task (a bash
 * job) never writes this sidecar, so its presence also discriminates an agent task.
 */
export function agentResultPath(outputPath: string): string {
  return outputPath.replace(/\.output$/, '.result');
}

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
  // Gate live progress-summary generation (parity: Claude's enableSummarization =
  // coordinator || fork-subagent || sdk-progress). ON by default (AMI superset);
  // AMI_SUBAGENT_SUMMARIES=off disables it, the mode flags force-enable it. The
  // tool-use COUNT is always tracked; only the free-text progressSummary is gated.
  const summariesEnabled = resolveEnableSummarization(process.env);
  // Classified recent tool-activity window (parity: Claude's collapseReadSearch) so
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
  // final answer written to the `.result` sidecar on completion (parity: Claude's
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
        // Human-readable action phrase (parity: Claude's getActivityDescription),
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
        // model can see incremental deltas (reportProgressDelta). Mirrors Claude
        // Code's live task.progress.toolCount counter. Also surface a short AI
        // progress summary of the CURRENT action (progress.summary parity — takes
        // precedence over the static description in task_list); this stamps the
        // entry's lastActivity so a quiet agent can be flagged idle.
        bgRecentCollapsible.push(isReadOrSearchActivity(event.toolName, input, findTool(event.toolName)));
        if (bgRecentCollapsible.length > 20) bgRecentCollapsible.shift();
        // Collapse a run of consecutive read/search ops into one line (parity:
        // Claude's collapseReadSearch); a single op / a mutation keeps its own
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
      // (parity: Claude's finalizeAgentTool backward-walk) so the `.result` sidecar
      // holds only the deliverable, not the whole streamed transcript.
      if (event.type === 'turn_complete' && typeof event.content === 'string' && event.content.trim().length > 0) {
        bgLastTurnText = event.content;
      }
      // COOPERATIVE GRACEFUL SHUTDOWN (parity: Claude Code's InProcessTeammateTask
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
  // Claude's TaskOutputTool preferring the in-memory final answer). Only written
  // when the agent actually produced a text-bearing final turn; best-effort.
  if (bgLastTurnText && bgLastTurnText.trim()) {
    try { fs.writeFileSync(agentResultPath(outputPath), bgLastTurnText); } catch { /* dir removed */ }
  }
  // Completion usage summary (parity: Claude's async-agent usage block) — the
  // tokens / tool-uses / duration the model would otherwise only get from a
  // foreground run. The worktree outcome (cleaned-up / retained) is appended
  // below AFTER the agent is marked terminal, so cleanup can run.
  const bgDurationS = ((Date.now() - bgStartedAt) / 1000).toFixed(1);
  let bgFooter = `\n[Agent stats: ${bgToolUseCount} tool calls, ${bgTotalTokens} tokens, ${bgDurationS}s]`;
  // Classify the terminal state up front (parity with Claude's completed/failed/
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
  if (context.processManager) {
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
    // Route through the DURABLE buffer (emitCompletion) rather than a bare
    // emit('complete'): with no live 'complete' listener anywhere, a bare emit
    // surfaced NO model-facing note for a finished background agent. emitCompletion
    // buffers it for the engine's per-turn drain and enriches it with the
    // status-aware summary + usage clause (duration + token/tool totals recorded on
    // the entry by the updateEntry() above) — parity with Claude's LocalAgentTask
    // completion notification. Dedup via `notified` still suppresses it when the
    // model already consumed the result via task_output.
    context.processManager.emitCompletion({ taskId, exitCode, command: `[agent] ${label}`, description: `Background agent: ${label}`, outputPath, status: terminalStatus });
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
    // Effective spawn mode: an explicit `mode` wins; otherwise, when implicit-fork
    // is enabled AND no `subagent_type` is named, an omitted mode resolves to
    // `fork` (parity: Claude's FORK_SUBAGENT schema mutation). Default stays
    // `general`. Uses the RAW `subagent_type` arg — a resume target carries its
    // own identity and never triggers an implicit fork.
    const mode = resolveSpawnMode(
      input.mode as string | undefined,
      input.subagent_type as string | undefined,
    );
    const resumeTarget = input.resume as string | undefined;
    // Durable cross-process resume (parity: Claude's readAgentMetadata /
    // getAgentTranscript): the taskId/name→session index is persisted to the project
    // state dir and rehydrated here, so a `resume` resolves an agent spawned in a
    // PRIOR process (AMI's in-memory maps are otherwise process-lifetime only).
    // Live in-memory entries still win over the snapshot.
    if (resumeTarget) rehydrateAgentRegistry(context.cwd);
    // Resolve the resume target — name, taskId, OR raw sessionId — up front so both
    // the identity restore and the session/cwd resolution below read ONE resolution.
    const resumeResolution = resumeTarget ? resolveResumeTarget(resumeTarget) : undefined;
    // Resume identity restore (parity: Claude Code's resumeAgent metadata restore,
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
    const effortOverride = input.effort as string | undefined;
    // A fork child may request `permission_mode: 'bubble'` — the sentinel that means
    // "run with the parent's live stance" (parity: Claude's FORK_AGENT 'bubble'). Resolve
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

    // team_name (parity: Claude's Agent-tool `team_name`): join the spawned agent to
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

    // Strict-parse the optional run_in_background flag so a malformed value (e.g.
    // "yes"/1) is REJECTED rather than silently treated as false (the trap of the
    // lenient coerceBool). Shared with bash.ts for consistent coercion.
    const ribParsed = parseOptionalBool(input.run_in_background, 'run_in_background');
    if ('error' in ribParsed) {
      return { output: `Error: ${ribParsed.error}`, isError: true };
    }

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
    // Per-agent frontmatter hooks (parity: Claude's agent `hooks` + registerFrontmatterHooks
    // with isAgent=true) — registered on the spawned sub-engine for the duration of this
    // agent's run and torn down when it shuts down. Plugin-sourced agents never carry hooks
    // (stripped at load), so this is always author-trusted.
    let agentFrontmatterHooks: import('../hooks').HookConfigEntry[] | undefined;
    let effectiveMode = mode;
    // Whether this subagent runs a BUILT-IN identity (a bare mode like explore/general/
    // coordinator/fork, or a bundled roster agent) vs a USER-DEFINED custom agent.
    // A mode-only spawn (no subagent_type) is always a built-in mode; a named agent is
    // resolved below. Threaded into the subagent hook/attribution context (parity:
    // Claude's agentContext isBuiltIn) and used to collapse analytics names privacy-safely.
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
      // required server is failed/missing. Mirrors Claude Code's AgentTool
      // requiredMcpServers readiness gate (AgentTool.tsx:L367-410). No-op for agents
      // without the field, so existing spawns are untouched.
      const mcpGate = await ensureRequiredMcpServersReady(
        (agentDef as { requiredMcpServers?: string[] }).requiredMcpServers,
        () => readMcpServerStates(context),
        subagentType,
      );
      if (!mcpGate.ready) return { output: mcpGate.error, isError: true };
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
      // A NON-built-in (user/project/plugin) agent definition — which may come from an
      // untrusted cloned repo — may TIGHTEN but never ESCALATE the subagent's permission
      // mode past the parent's live stance (parity: Claude's runAgent permission clamp +
      // source-trust gate). A repo-local `.ami/agents/*.md` declaring
      // `permission-mode: auto-allow` therefore cannot auto-approve every tool call when
      // the parent session is in `ask` / detached `deny-all`. Built-in agents are trusted
      // and keep their declared mode. The clamp is a no-op when the parent mode is not
      // threaded (bare/test contexts), so it never regresses existing behavior.
      subagentIsBuiltIn = isBuiltInAgent(agentDef);
      // An agent definition may declare the `'bubble'` sentinel (parity: Claude's
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
      // path (parity with Claude's agent `baseDir` → registerFrontmatterHooks plugin
      // root). Returns a cloned list — never mutates the cached agentDef.hooks.
      agentFrontmatterHooks = withFrontmatterHookPluginRoot(agentDef.hooks, agentPluginRoot(agentDef));

      // Per-agent persistent memory (parity: Claude's loadAgentMemoryPrompt).
      // When the agent definition declares a `memory` scope, load/seed its OWN
      // file-based memory store — namespaced by agent type, separate from the
      // global auto-memory — and PREPEND it to the authoritative system prompt so
      // the spawned agent starts already carrying its durable cross-run context.
      // Keyed on the parent's cwd (its canonical git root), not the eventual
      // worktree, so every spawn of this agent in the repo shares one store.
      if (agentDef.memory) {
        const agentMemoryBlock = buildAgentMemoryPrompt(subagentType, agentDef.memory, context.cwd);
        agentSystemPrompt = agentSystemPrompt
          ? `${agentMemoryBlock}\n\n${agentSystemPrompt}`
          : agentMemoryBlock;
      }

      // Preload the bodies of the skills this agent declares (parity: Claude's
      // runAgent skill preloading) into its authoritative system prompt, so the
      // spawned agent starts already carrying that instruction context instead of
      // having to discover + invoke each skill. Resolution reuses the SkillManager's
      // name/alias lookup (getSkill); a disabled or blank-bodied skill is treated as
      // unresolved. Unknown/invalid names are surfaced as a spawn note rather than a
      // hard failure (a missing optional skill should not strand the whole agent).
      if (agentDef.skills && agentDef.skills.length > 0) {
        // Resolution uses the SkillManager's exact name/alias lookup FIRST (covers
        // all registered skills incl. non-invocable), then falls back to Claude's
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
          // The def narrowed the toolset with a concrete (non-wildcard) allowlist —
          // a named teammate spawned into a team then re-gains the essential
          // coordination tools below so it never becomes unreachable.
          agentHadExplicitTools = true;
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
      // Source-trust floor: a NON-built-in agent definition cannot grant itself a
      // denied powerful tool (incl. the nested-spawn `task`) via its own `tools`
      // whitelist (parity: Claude's CUSTOM_AGENT_DISALLOWED_TOOLS on source !==
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
      // of a freshly-rebuilt general pool (parity: Claude's useExactTools). Falls back
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
    // args; parseOptionalBool (strict-parsed above) handles both (a strict `=== true`
    // silently dropped `run_in_background: "true"` to foreground, serializing a batch
    // the model meant to run concurrently). An explicit run_in_background (even
    // "false") wins; otherwise fall back to the agent definition's `background: true`
    // default (parity: Claude's agent background).
    const runInBackground = input.run_in_background !== undefined
      ? ribParsed.value
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

    // A sub-agent's own signal is forwarded to its nested grandchildren (and the
    // tool executor fans a child signal per parallel tool over it), so raise its
    // listener cap up front to avoid a MaxListenersExceededWarning on a wide turn.
    const subAbort = createBoundedAbortController();
    // Cascade a parent-turn abort to the subagent for BOTH foreground AND background
    // spawns (parity: Claude's createChildAbortController in registerAsyncAgent). The
    // prior code linked ONLY the foreground path, leaving a detached background
    // subagent (and its nested grandchildren) orphaned to keep running until an
    // explicit task_kill, the stall timeout, or engine shutdown when the spawning turn
    // aborted. unlinkParentAbort is invoked on foreground teardown and inside the
    // background drain's finally so a completed agent never leaks its listener.
    const unlinkParentAbort = linkChildAbortToParent(context.abortSignal, subAbort);

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
    } else {
      // No per-spawn model from any higher-priority source → consult the PERSISTED
      // `subagentDefaultModel` config knob (parity: Claude's getDefaultTeammateModel).
      // A configured string sets this spawn's default; `null` means "follow the leader"
      // (keep the inherited parent model); `undefined` leaves the inherited parent model
      // in place for the provider-aware final fallback below. This is the ONLY site that
      // reads the knob, so it never fights the env/per-call/agent-def overrides above.
      const configuredModel = resolveSubagentDefaultModel(context._subagentDefaultModel, parentModel);
      if (configuredModel) providerConfig.model = configuredModel;
    }

    // Final fallback: if NO model was resolvable from any source above and the
    // parent provided none to inherit, run the subagent on the provider-aware fast
    // default instead of dispatching an empty model id (parity with Claude Code's
    // getHardcodedTeammateModelFallback; keeps AMI's parent-inheritance perk when a
    // model IS present — see defaultSubagentModelFallback).
    providerConfig.model = defaultSubagentModelFallback(providerConfig.model, providerConfig);

    // Bedrock data-residency boundary (parity: Claude's subagent model resolution):
    // when running on amazon-bedrock, a sub-agent must invoke an inference profile in
    // the SAME account region as its parent. If the resolved sub-agent model does not
    // already pin its own cross-region prefix, inherit the parent model's prefix so a
    // delegation never silently crosses regions (a no-op when neither carries one).
    if (providerConfig.provider === 'amazon-bedrock') {
      providerConfig.model = inheritBedrockRegionPrefix(providerConfig.model, parentModel);
    }

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

    // Per-spawn plan-required gate (parity: Claude's planModeRequired knob +
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
    //    Claude's spawn-boundary de-escalation, spawnUtils.ts:L45-56 — a spawned
    //    agent is never more permissive than its parent). The clamp is a no-op when
    //    the parent mode is not threaded (bare/test contexts), so it is purely
    //    additive. The agent-def mode (agentPermissionMode) is already clamped above.
    // 2. Per-spawn `allow_permission_prompts` gate (deny-all when false, ask when true)
    // 3. Agent definition's permissionMode (e.g. verifier → auto-allow; safe, restricted tools)
    // 4. explore mode → always auto-allow (read-only tool set)
    // 5. A BACKGROUND agent with NO permission-prompt handler cannot surface a
    //    prompt to anyone, so its unlisted-tool calls auto-DENY ('deny-all') rather
    //    than defaulting to 'ask' and blocking a detached run on a prompt no one can
    //    answer (parity: Claude forces shouldAvoidPermissionPrompts on a
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
    // Claude's getRuntimeMainLoopModel on the agent-inherit path, utils/model/agent.ts
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

    // Plan-required STRUCTURAL read-only-bash boundary (parity: Claude's planAgent
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
      //
      // Cache-identical fork prefix (parity: Claude's forkSubagent renderedSystemPrompt
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
            // Stop) — parity with Claude Code's registerFrontmatterHooks(isAgent=true).
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
    // Parity: Claude's buildForkedMessages + filterIncompleteToolCalls.
    //
    // RESUME guard (parity: Claude's resumeAgent passing forkContextMessages:undefined,
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
    // invisible to them, and the user talks mainly to the lead (mirrors Claude
    // Code's teammatePromptAddendum). Without this the machinery exists but the
    // teammate is never told it can/should message its peers.
    if (agentName && resolvedTeam) {
      // Advertise the team's shared scratchpad (parity: Claude's coordinator
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
      // the lead scoped it to e.g. `['file_read']` (mirrors Claude's force-union of
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
    // permissions (teamAllowedPaths) as session allow-rules, mirroring Claude Code's
    // teammateInit path→rule conversion. The lead sets these when creating the team;
    // each teammate gets them on spawn so the whole team shares a consistent,
    // team-scoped path allowlist.
    if (agentName && resolvedTeam) {
      // PARITY (additive): a teammate inherits BOTH the declarative team-wide
      // allowed-path rules AND a prompt-free grant to the team's shared scratchpad
      // dir (teamScratchpadRules — Claude's gated coordinator scratchpadDir), so
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
      incrementSpawnCount(context);
      context.subagentLimiter?.recordSpawn();
      // Reuse the original taskId on resume (continue the SAME agent) rather than
      // registering a duplicate background entry/output file — see resolveSpawnTaskId.
      const taskId = resolveSpawnTaskId(resumeResolution);
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
      // Persist the registry snapshot so this agent is resumable from a LATER process
      // (durable cross-process resume — parity: Claude's on-disk agent metadata).
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
          // Introspection markers (parity: Claude resumeAgent / pillLabel): tag a
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
      // of the tool (parity: Claude's dead-target detection + bounded prune-and-retry).
      let subEngine: ReturnType<NonNullable<ToolContext['_engineFactory']>>;
      try {
        subEngine = buildSubEngineWithRetry(context._engineFactory, subConfig);
      } catch (err) {
        pruneDeadBackgroundAgent({ context, taskId, agentName, effectiveCwd, unlinkParentAbort });
        const msg = err instanceof Error ? err.message : String(err);
        return { output: `Error: failed to start background agent ${taskId}: ${msg}`, isError: true };
      }
      runBackgroundAgentDrain({
        context, subEngine, effectivePrompt, taskId, outputPath, label,
        effectiveCwd, isolation, subAbort, unlinkParentAbort, tools,
      }).catch(() => {});

      return { output: `Background agent started: ${taskId}\nOutput file: ${outputPath} (its streamed transcript is appended here live).\nTo collect its result, call task_output({ task_id: "${taskId}", wait: true }) — this blocks until it finishes and returns in one call. To check progress mid-flight WITHOUT blocking, call task_output({ task_id: "${taskId}" }) (no wait) or tail the output file. Do NOT poll in a loop. When spawning several agents, issue all the task_output(wait) calls in a single turn so they run concurrently.\nDo not duplicate this agent's work — avoid working with the same files or topics it is using. Work on non-overlapping tasks, or briefly tell the user what you launched and end your response.` };
    }

    incrementSpawnCount(context);
    context.subagentLimiter?.recordSpawn();
    // Reuse the original taskId on resume (continue the SAME agent) rather than
    // minting a fresh id for what is one continued run — see resolveSpawnTaskId.
    const taskId = resolveSpawnTaskId(resumeResolution);
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
    // Persist the registry snapshot so this agent is resumable from a LATER process
    // (durable cross-process resume — parity: Claude's on-disk agent metadata).
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
    // parent across concurrent/nested runs (parity: Claude's SubagentContext.parentSessionId;
    // the prior code mis-reported the child id as its own parent). invocationKind
    // discriminates a fresh `spawn` from a `resume` of an existing agent session (parity:
    // Claude's invocationKind). Falls back to taskId only in bare/test contexts with no
    // session id.
    // Fresh per-invocation correlation id for the cross-agent attribution tree (parity:
    // Claude's per-subagent query-tracking lineage, forkedAgent.ts:L451-455 — a new
    // chainId plus a monotonically incremented depth = parent.depth + 1). childDepth
    // ._agentDepth is already the CHILD's depth (one below the parent), so it is the
    // parent.depth+1 value directly.
    const chainId = generateTaskId('chain');
    const subagentHookCtx = {
      parentSessionId: context.sessionId || taskId,
      subagentSessionId: subConfig.sessionId || taskId,
      prompt,
      mode: effectiveMode,
      invocationKind: (resumeSessionId ? 'resume' : 'spawn') as 'resume' | 'spawn',
      // Whether this is a built-in identity vs a user-defined custom agent, plus a
      // privacy-safe analytics label (user-defined names are collapsed). Parity:
      // Claude's agentContext isBuiltIn + 'user-defined' telemetry collapse.
      isBuiltIn: subagentIsBuiltIn,
      analyticsAgentType: collapseAgentTypeForAnalytics(subagentType || mode, subagentIsBuiltIn),
      // The INVOKING agent's identity (the immediate parent subagent when nested, else
      // the invoking session) — the boundary marker that roots this invocation's edge in
      // the attribution tree (parity: Claude's invokingRequestId). session_id already
      // bundles the whole tree; this names the immediate invoker.
      invokingRequestId: context.agentId || context.sessionId || taskId,
      // Per-invocation lineage: a fresh chain id and this child's depth (parent.depth+1).
      chainId,
      depth: childDepth._agentDepth,
      // Discriminated-union agent-context kind (parity: Claude's subagent-vs-teammate
      // discriminator, agentContext.ts:L124-131): a named spawn joined to a team is a
      // teammate; every other spawn is a plain subagent.
      agentContextKind: (agentName && resolvedTeam ? 'teammate' : 'subagent') as 'teammate' | 'subagent',
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
    // Prompt-cache introspection carried into the completion footer (parity: Claude's
    // per-fork cacheRead / cacheHitRate). Latched from usage_update stats; 0/undefined
    // when the provider reports no cache signal (clause then omitted).
    let cacheReadTokens = 0;
    let cacheHitRate: number | undefined;
    const startedAt = Date.now();
    const recentActivities: string[] = [];
    // Live-display state for this subagent (PARITY: Claude's DreamTask): a bounded
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
          // Record a name+input activity label (parity: Claude's progress
          // recentActivities carry the tool name AND its input), bounded to the most
          // recent MAX_RECENT_ACTIVITIES so the trail stays compact.
          recentActivities.push(describeActivity(event.toolName, event.input));
          if (recentActivities.length > MAX_RECENT_ACTIVITIES) recentActivities.shift();
          // Live-display turn accounting (parity: Claude's DreamTask) — tallies this
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
        // (parity: Claude's addDreamTurn — tool uses collapsed to a count, the N
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
            // Introspection markers (parity: Claude resumeAgent / pillLabel), same
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
        // Surface the last few tool actions (parity: Claude's recentActivities) so a
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
      // Always cancel the auto-background timer (parity: Claude's cancelAutoBackground
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
        // rediscovering it (parity: Claude's <worktree_path>/<worktree_branch>).
        deliverable += buildWorktreeCompletionNotice(effectiveCwd, readWorktreeBranch(effectiveCwd));
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
    // (mirrors Claude's AbortError-on-abort semantics). A run whose FINAL deliverable
    // is an interrupted/synthetic message (server-error / tool-call interrupt) was
    // cancelled mid-flight too, even without an explicit abort event — treat it as a
    // failure so the parent does not act on a truncated half-answer (parity: Claude's
    // post-loop synthetic-final re-throw, AgentTool.tsx:L1205-1218).
    const failed = stalled || exitReason === 'abort' || sawAbortError || isInterruptedFinalText(deliverable);

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
    const bodyBase = `${cwdFallbackNote}${agentSkillPreloadNote ? `${agentSkillPreloadNote}\n\n` : ''}${rawBody}`;
    // Handoff safety review (parity: Claude's classifyHandoffIfNeeded): when the
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
