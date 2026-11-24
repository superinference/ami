// Split from task.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, EngineConfig } from '../types';
import { BackgroundLiveTurns } from './background-live-turns';
import { permissionRuleValueFromString } from '../permissions';
import { DEFAULT_MAX_AGENT_DEPTH } from './task-link-child-abort-to-parent';
import { INCOMPLETE_ASSISTANT_SENTINEL_RE } from './task-ensure-required-mcp-servers-ready';
import type { checkAgentCaps } from './task';

/**
 * Whether a sub-agent's final deliverable text is an INTERRUPTED/synthetic ending
 * rather than a clean answer — it carries one of the in-band interrupt sentinels
 * ({@link INCOMPLETE_ASSISTANT_SENTINEL_RE}: a server-error or tool-call interrupt).
 * A run that ends on such a message was CANCELLED mid-flight, not completed; the
 * parent must treat it as a failure (so it does not act on a truncated half-answer),
 * mirroring the reference's post-loop check that re-throws `AbortError` when the final
 * message is a synthetic interruption (AgentTool.tsx:L1205-1218). AMI messages carry
 * no `stop_reason`, so the in-band sentinel is the discriminator.
 */
export function isInterruptedFinalText(text: string | undefined): boolean {
  return typeof text === 'string' && INCOMPLETE_ASSISTANT_SENTINEL_RE.test(text);
}

/**
 * Pick a sub-agent's returned deliverable: the text of its FINAL assistant turn
 * (`lastTurnText`) when one carried text, else the accumulated text fallback.
 * Mirrors the reference's `finalizeAgentTool` (agentToolUtils.ts:L297-317), which
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
 * appended to its authoritative system prompt (mirrors the reference's
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
 * completion stats footer (mirrors the reference's per-fork cacheRead / cacheHitRate
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

/** Max recent tool activities retained for the live-progress trail (parity: the reference
 *  Code's MAX_RECENT_ACTIVITIES window on a backgrounded task's progress object). */
export const MAX_RECENT_ACTIVITIES = 5;

/**
 * One-line label for a tool call recorded into a sub-agent's recent-activity trail:
 * the tool name plus a short, single-line summary of its most salient input
 * (`bash(git status)`, `file_read(src/app.ts)`), mirroring the reference's progress
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
 * completion/stall notice (parity: surfacing the reference's progress
 * `recentActivities` so the last few tool actions are observable). Returns an empty
 * string when there is nothing to show, so callers can append unconditionally.
 */
export function buildRecentActivityTrail(activities: readonly string[]): string {
  if (activities.length === 0) return '';
  return ` Recent activity: ${activities.join(', ')}.`;
}

/**
 * Render a sub-agent's live-display state (PARITY: the reference's DreamTask phase + turn
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
 * Completion notice telling the model WHERE an isolated-worktree subagent's changes
 * live (path + branch), appended when the worktree is RETAINED because it has
 * uncommitted changes or unpushed commits (mirrors the reference's
 * `<worktree><worktree_path>…</worktree_path><worktree_branch>…</worktree_branch></worktree>`
 * completion section). Without it the model only hears "cleaned up" for empty
 * worktrees and has to rediscover where a change-bearing worktree landed. The
 * branch clause is omitted on a detached HEAD / branch-read failure.
 */
export function buildWorktreeCompletionNotice(worktreePath: string, branch?: string): string {
  const branchClause = branch ? ` on branch ${branch}` : '';
  return `\n[Agent changes are in the isolated git worktree at ${worktreePath}${branchClause} — review, commit, or merge them from there.]`;
}

export function incrementSpawnCount(context: { _agentSpawnCount?: { value: number } }): void {
  if (!context._agentSpawnCount) {
    context._agentSpawnCount = { value: 0 };
  }
  context._agentSpawnCount.value++;
}

/** Context shape {@link reserveAgentSlot} needs — the union of the cap inputs and
 *  the mutable spawn counter. */
export type AgentCapContext = Parameters<typeof checkAgentCaps>[0];

export function shouldAllowNestedTask(currentDepth: number, maxDepth: number): boolean {
  return currentDepth < maxDepth;
}

/**
 * Max attempts to construct a background sub-agent's engine before pruning it as a
 * dead child. Parity: the reference's bounded prune-and-retry for a teammate-spawn
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
 * Mirrors the reference's dead-target detection: retry a bounded number of times, then
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
 * Notice prepended to a subagent that runs in an isolated git worktree (mirrors
 * the reference's buildWorktreeNotice). It tells the child three concrete things:
 * (1) paths in any inherited/parent context refer to the PARENT's cwd and must be
 * translated to the worktree root, (2) re-read files before editing since the
 * parent may have modified them, and (3) its changes are isolated to the worktree.
 */
export function buildWorktreeNotice(parentCwd: string, worktreeCwd: string): string {
  return `You are operating in an isolated git worktree at ${worktreeCwd} — same repository, same relative file structure, separate working copy from the parent agent working in ${parentCwd}. Paths in any inherited context refer to the parent's working directory; translate them to your worktree root. Re-read files before editing if the parent may have modified them. Your changes stay in this worktree and will not affect the parent's files.`;
}

/**
 * Validate a model-requested subagent `cwd` override against the parent's trusted
 * permission boundary (mirrors the reference deriving the agent's working-dir set from the
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
 * Select a `fork` child's tool pool from the parent engine's EXACT ordered tool
 * names ({@link ToolContext._parentToolNames}) rather than rebuilding a general
 * pool. The child keeps the parent's tools in the parent's order (intersected with
 * the live registry — a tool the parent had but the child's registry lacks is
 * skipped) so the serialized tool array — and therefore the provider prompt-cache
 * prefix — stays byte-identical to the parent's. Mirrors the reference's
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
 * Resolve an agent-definition / per-spawn tool-spec list (its `tools`,
 * `disallowedTools`, or a per-call `allowed_tools`) to canonical AMI tool names.
 * Each entry may be a bare tool name (`bash`), a legacy / cross-framework alias
 * (`Bash`, `Read`, `Edit`), or a PATTERNED permission spec (`Bash(git:*)`,
 * `Read(src/**)`). Every form is run through the single canonical
 * {@link permissionRuleValueFromString} parser (shared with the permission layer —
 * no second parser) so the BASE tool name is extracted and aliased to AMI's
 * registered name. Mirrors the reference's `resolveAgentTools`, which runs each tool
 * spec through `permissionRuleValueFromString` before matching — without this a
 * pattern-form allow entry silently filtered the tool out and a pattern-form
 * disallow entry silently failed to block (exact-string matching never matched
 * `Bash(git:*)` against the registered `bash`).
 */
export function canonicalAgentToolNames(specs: string[]): string[] {
  return specs.map(spec => permissionRuleValueFromString(spec).toolName);
}

/**
 * Normalize a caller-supplied `allowed_agent_types` value into a string[] (or
 * undefined when the field is absent). Mirrors the reference's `allowedAgentTypes`
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
 * restriction. Mirrors the reference's `allowedAgentTypes.includes(a.agentType)` filter:
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
 * undefined when absent). Mirrors the reference's spawn-time `permissions` /
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
  return parseToolSpecArray(
    raw,
    'allowed_tools must be an array of strings (the tool names this subagent may use).',
  );
}

/**
 * Shared string[]-shape validator for a per-spawn tool-name list input (the
 * availability `allowed_tools` AND the auto-approve {@link parseAutoApproveTools}
 * lists have IDENTICAL shape rules — absent → no restriction, otherwise an array of
 * strings — so they share one implementation rather than duplicating the check).
 * Returns `{ value: undefined }` for an absent list, `{ error }` with the
 * caller-supplied message for a non-array / non-string-element shape.
 */
export function parseToolSpecArray(
  raw: unknown,
  errorMsg: string,
): { value: string[] | undefined } | { error: string } {
  if (raw === undefined || raw === null) return { value: undefined };
  if (!Array.isArray(raw) || raw.some(t => typeof t !== 'string')) {
    return { error: errorMsg };
  }
  return { value: raw as string[] };
}

/**
 * Normalize a caller-supplied per-spawn `auto_approve_tools` value into a string[]
 * (or undefined when absent). Mirrors the reference's spawn-time `allowedTools`
 * auto-approve allowlist (`swarm/inProcessRunner.ts:L492-493`, "Tool permissions to
 * auto-allow for this teammate"), which is DISTINCT from the set of tools that merely
 * EXIST for the subagent (AMI's availability {@link parseAllowedTools}). An entry
 * here pre-approves that tool for the spawned agent so it runs WITHOUT a permission
 * prompt, while any tool NOT listed still exists and is prompted/denied per the
 * resolved permission mode. Returns `{ error }` for a non-array / non-string-element
 * shape; `{ value: undefined }` means "no auto-approvals". Entries may be bare names,
 * aliases, or patterned specs ("Bash(git:*)"), resolved by {@link buildAutoApproveRules}.
 */
export function parseAutoApproveTools(
  raw: unknown,
): { value: string[] | undefined } | { error: string } {
  return parseToolSpecArray(
    raw,
    'auto_approve_tools must be an array of strings (the tool names to auto-approve for this subagent).',
  );
}
