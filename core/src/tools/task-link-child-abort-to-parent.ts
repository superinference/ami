// Split from task.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { projectStateDir } from '../reference-journal';
import * as path from 'path';
import * as crypto from 'crypto';
import { raiseAbortListenerCap } from '../utils/abort';
import { isEnvTruthy } from '../analytics';
import { validateBoundedIntEnvVar } from '../config';

/** Auto-background threshold for foreground agents exceeding this duration. */
export const AUTO_BG_MS = 120_000;

/** Master env knob that enables auto-backgrounding of long-running foreground
 *  subagents (parity: the reference's `CLAUDE_AUTO_BACKGROUND_TASKS`). */
export const AUTO_BACKGROUND_TASKS_ENV = 'AMI_AUTO_BACKGROUND_TASKS';

/** Optional override of the auto-background threshold in ms (default AUTO_BG_MS). */
export const AUTO_BACKGROUND_MS_ENV = 'AMI_AUTO_BACKGROUND_MS';

/**
 * Resolve the auto-background threshold in milliseconds, or `0` when disabled.
 *
 * Mirrors the reference's `getAutoBackgroundMs()` (AgentTool.tsx:72-77): a
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
 *  the reference's FORK_AGENT maxTurns:200 — a context-inheriting clone needs a
 *  generous ceiling because it continues the parent's full task). */
export const DEFAULT_FORK_MAX_TURNS = 200;

/**
 * Env knob that enables IMPLICIT FORK (parity: the reference's `FORK_SUBAGENT`
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
 * `fork` — the context-inheriting clone (parity: the reference's omit-subagent_type →
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
 * (mirrors the reference's fork boilerplate rule #1 in `buildChildMessage`,
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
 * brute-force symlink race on that path feasible; 8 bytes closes it (the reference
 * uses a 36-char alphabet for the same 8-byte entropy — hex keeps every existing
 * `agent-[a-f0-9]+` consumer working while matching the entropy floor).
 */
export function generateTaskId(prefix = 'agent'): string {
  return `${prefix}-${crypto.randomBytes(8).toString('hex')}`;
}

/** Max length of a caller-supplied stable {@link isSafeAgentId} agent_id. */
export const AGENT_ID_MAX_LENGTH = 64;

/** Charset for a caller-supplied stable agent_id: must START with an alphanumeric
 *  (no leading separator) then allow letters, digits, `.`, `_`, `-`. */
export const AGENT_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]*$/;

/**
 * Whether a caller-supplied stable `agent_id` is safe to use as a taskId. The taskId
 * derives an on-disk output path (`<taskId>.output`), the file-cache key, and the
 * process-manager/registry key, so a model-chosen id must be a single path-safe
 * segment: it may NOT contain a path separator (`/` or `\`), a `..` traversal, be
 * empty, or exceed {@link AGENT_ID_MAX_LENGTH}. Mirrors the reference's separation of a
 * stable machine `agentId` from the display `agentName` (utils/teammate.ts:L44-45):
 * the caller pins a durable addressing id distinct from the human `name`, so a
 * display rename never changes the resume/output identity. A value failing this is
 * rejected by the task tool rather than silently sanitized (so the caller learns
 * their chosen id was unusable instead of addressing a different id than they think).
 */
export function isSafeAgentId(id: string): boolean {
  if (id.length === 0 || id.length > AGENT_ID_MAX_LENGTH) return false;
  if (id.includes('..')) return false; // no path-traversal segment
  return AGENT_ID_PATTERN.test(id);
}

/**
 * The taskId a spawn registers under: on RESUME, reuse the ORIGINAL agent's taskId
 * (carried by {@link resolveResumeTarget}) instead of minting a new one; on a fresh
 * spawn, use a caller-supplied stable {@link isSafeAgentId} `agent_id` when present,
 * else generate a new random id. Mirrors the reference's task framework, where a resume
 * re-registers under the SAME task id — carrying forward the agent's prior state and
 * output stream rather than double-registering a second background entry for what is
 * one continued agent (framework.ts:L74-117) — PLUS the reference's stable caller-assigned
 * `agentId` distinct from the display name (utils/teammate.ts:L44-45): a caller that
 * wants a durable, predictable address (to resume/collect the agent later without
 * first reading back a random id) supplies `agent_id`, and the display `name` can
 * still diverge from it. An unsafe/absent agent_id falls back to a random id, so this
 * is purely additive. Without it, resuming a named agent in the same process spawned a
 * NEW taskId: a duplicate task_list/processManager entry and a second output file,
 * orphaning the agent's original id.
 */
export function resolveSpawnTaskId(resume?: ResumeResolution, requestedAgentId?: string): string {
  if (resume) return resume.resolvedId;
  if (requestedAgentId && isSafeAgentId(requestedAgentId)) return requestedAgentId;
  return generateTaskId();
}

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
 * before it concludes something is absent or reports back (mirrors the reference's
 * EXPLORE_AGENT_MIN_QUERIES). It guards the UNDER-exploration edge: the lean
 * prompt's "do not over-explore" is only an upper bound; without a floor an
 * explorer can return prematurely after a single miss and wrongly conclude a
 * symbol/file does not exist. The prompt below cites this floor so the agent runs
 * at least this many search strategies before declaring absence.
 */
export const EXPLORE_AGENT_MIN_QUERIES = 3;

/**
 * The identity/defaults a spawned agent ran under, keyed by taskId. Lets a later
 * `resume` restore the ORIGINAL agent's identity (its `subagent_type`, model,
 * permission mode, and description) when the resume call does not re-supply them,
 * instead of silently re-deriving a generic general-purpose agent. Mirrors the reference
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

/** On-disk snapshot of the agent registries so a `resume` can resolve a
 *  previously-spawned agent across PROCESS RESTARTS (parity: the reference's
 *  readAgentMetadata / getAgentTranscript, which resolve a resumable agent from the
 *  on-disk store rather than an in-memory map). Written per project-state dir. */
export interface PersistedAgentRegistry {
  names?: Record<string, string>;      // agentName -> taskId
  sessions?: Record<string, string>;   // taskId -> sub-engine sessionId
  cwds?: Record<string, string>;       // taskId -> working dir (worktree / cwd override)
  meta?: Record<string, AgentMeta>;    // taskId -> recorded identity/defaults
}

/** Absolute path of the durable agent-registry snapshot for a project (cwd). */
export function agentRegistryPath(cwd: string): string {
  return path.join(projectStateDir(cwd), 'agent-registry.json');
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

/** Routing-significant characters that must never appear in an agent name: `@`
 *  is the reserved `agentName@teamName` identity separator and `*` is the
 *  send_message broadcast wildcard. */

/**
 * Sanitize a caller-supplied agent `name` so it is safe to use as a routing /
 * mailbox key (mirrors the reference's sanitizeAgentName, which strips the `@`
 * team separator). The agent name becomes the send_message / resume address and
 * is baked into the team-scoped identity, so a name containing `@` would corrupt
 * `agentName@teamName` parsing and a name of (or containing) `*` would collide
 * with send_message's broadcast wildcard — a model-chosen `*` agent could then
 * silently intercept every broadcast. Reserved routing chars are replaced with
 * `-`, runs collapsed, and stray leading/trailing separators trimmed. Returns an
 * empty string only when the input was ENTIRELY reserved characters, so callers
 * can fall back to no-name rather than register a degenerate key.
 */

export const DEFAULT_MAX_AGENT_DEPTH = 5;

export const DEFAULT_MAX_AGENTS_PER_SESSION = 50;

export const DEFAULT_MAX_CONCURRENT_AGENTS = 10;

/**
 * Link a child subagent's {@link AbortController} to the PARENT turn's abort signal so
 * a parent abort CASCADES to the child (parity: the reference's `createChildAbortController`
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
export const PERMISSION_MODE_RANK: Record<string, number> = {
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
 * the reference's runAgent permission clamp — an agent definition's permissionMode is not
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
 * Privacy-safe agent-type label for analytics/attribution (parity: the reference's
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
