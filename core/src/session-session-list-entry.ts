// Split from session.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { SessionId } from './types';
import { stripDisplayTags } from './utils/display-tags';

/**
 * Resolve a session's display summary by the precedence chain customTitle >
 * aiTitle > lastPrompt > firstPrompt, returning `undefined` when none exist (a
 * metadata-only session). The `'(no messages)'` sentinel is treated as absent so
 * an empty session never masquerades as having a real summary. Parity: the reference's
 * listSessionsImpl summary precedence (which drops metadata-only sessions).
 */
export function resolveSessionSummary(
  session: { customTitle?: string; aiTitle?: string },
  prompts: { lastPrompt?: string; firstPrompt?: string } = {},
): string | undefined {
  for (const candidate of [session.customTitle, session.aiTitle, prompts.lastPrompt, prompts.firstPrompt]) {
    if (typeof candidate !== 'string') continue;
    const t = candidate.trim();
    if (t && t !== '(no messages)') return t;
  }
  return undefined;
}

/**
 * Classification of how a session is being run, persisted so the resume/picker
 * paths can discriminate a plain interactive session from a backgrounded or
 * daemon-owned one (parity: the reference's `SessionKind` in concurrentSessions.ts):
 *  - `'interactive'` — a normal foreground TUI / `--prompt` run (the default);
 *  - `'bg'` — a user-backgrounded session that should keep running when its
 *    frontend detaches (and must NOT be auto-picked as "the most recent" by a
 *    no-arg `--continue`);
 *  - `'daemon'` / `'daemon-worker'` — a long-lived service session and its
 *    workers, likewise excluded from the ordinary resume picker.
 * Orthogonal to {@link Session.entrypoint} (which records the invocation surface,
 * e.g. `cli`/`detached`/`sdk`): a `detached` run can still be `interactive`.
 */
export type SessionKind = (typeof SESSION_KINDS)[number];

/** The valid {@link SessionKind} values, exported so a consumer (CLI/introspection)
 *  can validate an override without duplicating the union. */
export const SESSION_KINDS = ['interactive', 'bg', 'daemon', 'daemon-worker'] as const;

/**
 * Read the session-kind override from the environment, if any. Honors the AMI
 * knob `AMI_SESSION_KIND` and — consistent with the dual-env convention used
 * elsewhere in this file ({@link isSessionPersistenceDisabled}) — the reference's
 * `CLAUDE_CODE_SESSION_KIND`, taking the AMI value first. Returns `undefined`
 * when unset or not one of {@link SESSION_KINDS}, so a bad value falls back to
 * the caller's default instead of persisting garbage. Parity: the reference's
 * `envSessionKind`.
 */
export function envSessionKind(env: NodeJS.ProcessEnv = process.env): SessionKind | undefined {
  const raw = env.AMI_SESSION_KIND ?? env.CLAUDE_CODE_SESSION_KIND;
  if (typeof raw !== 'string') return undefined;
  const v = raw.trim() as SessionKind;
  return SESSION_KINDS.includes(v) ? v : undefined;
}

/**
 * True when a session is a background/daemon kind — one that keeps running on
 * frontend detach and must be skipped when a no-arg `--continue` auto-picks the
 * most-recent resumable session (parity: the reference's `isBgSession`, which flips exit
 * semantics for `--bg`/daemon sessions). An absent `kind` is treated as the
 * foreground default (`false`), so legacy sessions written before this field are
 * still resumable.
 */
export function isBgSession(session: { kind?: SessionKind }): boolean {
  return session.kind === 'bg' || session.kind === 'daemon' || session.kind === 'daemon-worker';
}

/**
 * Sentinel {@link SessionAgentIdentity.model} value meaning "do NOT override the
 * main-loop model — use whatever model the resumed run is otherwise configured
 * with". Distinct from an absent model (also no override) only in intent: an agent
 * can be authored to explicitly inherit. Parity: the reference's `resumedAgent.model !==
 * 'inherit'` guard in `restoreAgentFromSession`.
 */
export const AGENT_MODEL_INHERIT = 'inherit';

/**
 * Agent/persona identity a session ran under, persisted so `--resume` can restore
 * it (parity: the reference's conversationRecovery session metadata
 * `agentName`/`agentColor`/`agentSetting` + the resumed agent's model override).
 *  - `name` / `color` — display identity for the picker and status line;
 *  - `setting` — the agent TYPE identifier used to re-resolve the live agent
 *    definition on resume (the reference's `agentSetting`);
 *  - `model` — the agent's model override, or the {@link AGENT_MODEL_INHERIT}
 *    sentinel. Applied on resume via {@link resolveAgentModelOverride}.
 */
export interface SessionAgentIdentity {
  name?: string;
  color?: string;
  setting?: string;
  model?: string;
}

/**
 * Decide the model override to apply when resuming a session whose saved agent had
 * a model (parity: the reference's `restoreAgentFromSession` model-apply branch). Returns
 * the model id to set as the main-loop override, or `null` when NO override should
 * be applied, in precedence order:
 *  1. the user passed `--agent` on the CLI → keep the CLI agent, apply nothing
 *     (`userPassedAgentOverride`);
 *  2. a main-loop model override is already set (e.g. a `--model` flag) → it wins
 *     (`existingModelOverride`);
 *  3. the saved model is absent/blank or the {@link AGENT_MODEL_INHERIT} sentinel
 *     → use the main-loop model (no override);
 *  4. otherwise → apply the saved agent model (trimmed).
 * Pure (all state injected) so the resume precedence is unit-testable without the
 * CLI/agent registry — the single acceptance boundary a `--resume` path reuses.
 */
export function resolveAgentModelOverride(opts: {
  savedModel?: string;
  userPassedAgentOverride?: boolean;
  existingModelOverride?: boolean;
}): string | null {
  if (opts.userPassedAgentOverride) return null;
  if (opts.existingModelOverride) return null;
  if (typeof opts.savedModel !== 'string') return null;
  const model = opts.savedModel.trim();
  if (!model || model === AGENT_MODEL_INHERIT) return null;
  return model;
}

/**
 * Project a persisted {@link SessionAgentIdentity} down to the display subset a
 * {@link SessionListEntry} carries (name/color/setting — never the model override,
 * which is a resume-time concern). Returns `undefined` when there is no agent or it
 * has no displayable field, so a plain session's list entry carries no `agent` key
 * at all. The ONE place the picker-projection lives so {@link SessionManager.list}
 * never re-derives it inline (§0b.6).
 */
export function toListEntryAgent(
  agent: SessionAgentIdentity | undefined,
): { name?: string; color?: string; setting?: string } | undefined {
  if (!agent) return undefined;
  const { name, color, setting } = agent;
  if (name === undefined && color === undefined && setting === undefined) return undefined;
  return { name, color, setting };
}

/** The display title for a session: a user rename wins over the AI title. */
export function resolveSessionTitle(session: {
  customTitle?: string;
  aiTitle?: string;
}): string | undefined {
  return session.customTitle ?? session.aiTitle;
}

/**
 * Deterministic display title for the /resume + /sessions picker with the full
 * fallback chain, so a session ALWAYS renders a sensible label even when it has
 * no title and no meaningful prompt. Order:
 *   1. {@link resolveSessionSummary} — customTitle > aiTitle > lastPrompt > firstPrompt
 *      (display tags stripped so a command-only turn can't leak raw XML);
 *   2. an explicit `defaultTitle` the caller supplies;
 *   3. `'Autonomous session'` when the session was launched detached (`entrypoint`
 *      `'detached'`) and nothing above resolved — an autonomous run with no real
 *      user prompt still gets a human label instead of a bare id;
 *   4. the first 8 chars of the session id (lite entries with no metadata).
 * Returns `''` only for a session with no summary, no defaultTitle, not detached,
 * and no id. Consolidates with {@link resolveSessionSummary} (no duplicated
 * precedence logic). Parity: the reference's getLogDisplayTitle (defaultTitle param,
 * autonomous-session special-case, sessionId.slice(0,8) fallback).
 */
export function getSessionDisplayTitle(
  session: { id?: SessionId | string; customTitle?: string; aiTitle?: string; entrypoint?: string },
  prompts: { lastPrompt?: string; firstPrompt?: string } = {},
  defaultTitle?: string,
): string {
  const summary = resolveSessionSummary(session, prompts);
  if (summary) {
    const cleaned = stripDisplayTags(summary).trim();
    return cleaned || summary;
  }
  if (typeof defaultTitle === 'string' && defaultTitle.trim()) return defaultTitle.trim();
  if (session.entrypoint === 'detached') return 'Autonomous session';
  const id = session.id ? String(session.id) : '';
  return id.slice(0, 8);
}

export interface SessionListEntry {
  id: SessionId;
  date: string;
  preview: string;
  /** Number of persisted messages — lets the picker tell a 3-message dead-end
   *  from a 200-message deep session. */
  messageCount: number;
  /** On-disk size of the session JSON in bytes (for display). */
  fileSize?: number;
  /** Git branch the session was saved on, when recorded. */
  gitBranch?: string;
  /** Resolved display title (user rename ?? AI title), when present. */
  title?: string;
  /** The most-recent meaningful user instruction, distinct from {@link preview}
   *  (which is the FIRST prompt) — lets the picker surface "what was this session
   *  last asked to do" (parity: the reference's lastPrompt log entry). */
  lastPrompt?: string;
  /** Display summary via the precedence chain customTitle > aiTitle > lastPrompt >
   *  firstPrompt (parity: the reference's listSessionsImpl summary). `undefined` for a
   *  metadata-only session with no title and no prompts. */
  summary?: string;
  /** User-set searchable tag, when recorded. */
  tag?: string;
  /** Persisted session mode, when recorded. */
  mode?: string;
  /** Linked GitHub PR, when recorded. */
  pr?: { number: number; url: string; repository?: string };
  /** Worktree path the session was last inside, when it is currently in one. */
  worktree?: string;
  /** The originating project directory (cwd) the session belongs to, when
   *  recorded — lets a cross-project/worktree-spanning listing show WHICH directory
   *  each session came from (parity: the reference's SessionInfo.cwd, falling back to the
   *  project path). Populated from {@link Session.projectPath}. */
  projectPath?: string;
  /** Invocation surface that last wrote the session, when recorded. */
  entrypoint?: string;
  /** How the session is being run ({@link SessionKind}) — lets the picker tell a
   *  foreground session from a `'bg'`/`'daemon'` one, when recorded. */
  kind?: SessionKind;
  /** Persisted permission mode, when recorded. */
  permissionMode?: string;
  /** True when this entry is a subagent sidechain transcript (only present when
   *  {@link SessionManager.list} is called with `includeSidechain`). */
  isSidechain?: boolean;
  /** The session this one was forked/regenerated from, when recorded — lets a
   *  picker/analytics show the lineage of a cleared-through chain of sessions. */
  parentSessionId?: SessionId;
  /** Agent/persona identity (name/color/setting) the session ran under, when
   *  recorded — lets the picker/status line show which agent a session used
   *  (parity: the reference's agentName/agentColor list metadata). The model override is
   *  intentionally NOT surfaced here (it is a resume-time concern via
   *  {@link resolveAgentModelOverride}, not a picker label). */
  agent?: { name?: string; color?: string; setting?: string };
  /** True when this entry was built from a bounded lite metadata read
   *  ({@link SessionManager.readSessionMetaLite}) rather than a full parse: its
   *  {@link preview}/{@link lastPrompt}/{@link summary} come from the title only and
   *  {@link messageCount} is 0 until the session is opened (parity: the reference's
   *  not-yet-enriched resume-picker rows). A full parse is done on open. */
  lite?: boolean;
}

/**
 * Introspection counts for a {@link SessionManager.list} / {@link SessionManager.listWithDiagnostics}
 * pass — the data that answers "why didn't my session show up in /resume?". Mirrors
 * the reference's resume-discovery debug lines (sessionStorage.ts `found N session files`
 * / `enriched N, M filtered out, K visible` + per-session filter reasons): instead of
 * silently dropping an unreadable/filtered row, the listing tallies how many files were
 * found, how many are visible, and how many were skipped broken down by reason, so a
 * missing session is diagnosable rather than invisible.
 */
export interface SessionListDiagnostics {
  /** Total session files discovered on disk ({@link SessionManager.listFiles}). */
  found: number;
  /** Entries returned to the caller (visible rows after every filter). */
  listed: number;
  /** Rows dropped for any reason (sum of {@link skippedByReason} values). */
  skipped: number;
  /** Per-reason skip tally — e.g. `unreadable`, `sidechain`, `current-session`,
   *  `duplicate-id`, `empty`, `branch-filter`, `tag-filter`, `pr-filter`. A reason
   *  with no drops is simply absent from the map. */
  skippedByReason: Record<string, number>;
}

/** The skip reasons {@link SessionManager.list} records into a {@link SessionListDiagnostics}
 *  sink. Exported so a consumer (e.g. the `/sessions` / `/resume` renderer) can label
 *  each reason without re-deriving the string set. */
export const SESSION_SKIP_REASONS = [
  'unreadable', 'sidechain', 'current-session', 'duplicate-id',
  'empty', 'branch-filter', 'tag-filter', 'pr-filter',
] as const;

export type SessionSkipReason = (typeof SESSION_SKIP_REASONS)[number];

/**
 * Metadata recovered by a bounded head/tail read of a session file WITHOUT a full
 * `JSON.parse` of its transcript ({@link SessionManager.readSessionMetaLite}). Only
 * top-level scalar fields are extractable this way (the first-prompt preview and the
 * message count need the full messages array, so they are left to enrichment on
 * open). Parity: the reference's lite session-list metadata.
 */
export interface SessionMetaLite {
  id: string;
  createdAt?: string;
  updatedAt?: string;
  customTitle?: string;
  aiTitle?: string;
  gitBranch?: string;
  tag?: string;
  version?: string;
  projectPath?: string;
  entrypoint?: string;
  mode?: string;
  kind?: SessionKind;
  /** True when a top-level `"isSidechain": true` was seen in the head buffer. */
  isSidechain?: boolean;
}

/**
 * Parse a PR identifier the way the resume/session picker's PR filter accepts it:
 * either a bare positive integer (`"42"`) or a GitHub pull-request URL
 * (`https://github.com/owner/repo/pull/42`), returning the numeric PR number, or
 * `null` when the input is neither. A non-positive or non-numeric direct value
 * falls through to the URL probe; an unmatched URL yields `null`. Parity: the reference's
 * `parsePrIdentifier` in the resume screen — the single acceptance boundary behind
 * {@link filterSessionsByPr} so a `--pr <n|url>` resume filter and any picker share
 * one parser (no duplicated regex).
 */
export function parsePrIdentifier(value: string): number | null {
  const directNumber = parseInt(value, 10);
  if (!isNaN(directNumber) && directNumber > 0) {
    return directNumber;
  }
  const urlMatch = value.match(/github\.com\/[^/]+\/[^/]+\/pull\/(\d+)/);
  if (urlMatch?.[1]) {
    return parseInt(urlMatch[1], 10);
  }
  return null;
}

/**
 * The PR filter accepted by {@link SessionManager.list}'s `filterByPr` option and
 * {@link filterSessionsByPr}:
 *  - `true` — keep only sessions that are linked to ANY PR;
 *  - `false` — no PR constraint (keep everything, same as omitting the filter);
 *  - a `number` — keep only sessions linked to exactly that PR number;
 *  - a `string` — parse it ({@link parsePrIdentifier}: integer or GitHub PR URL)
 *    and keep only sessions linked to the resolved number (an unparseable string
 *    matches nothing).
 */
export type PrFilter = boolean | number | string;

/**
 * Filter a list of session entries by their linked PR (parity: the reference's
 * `filteredLogs` PR filter in the resume screen). Shared by
 * {@link SessionManager.list} and any caller that already holds entries, so the
 * `true`/number/string semantics live in ONE place. An entry matches by its
 * `pr.number` ({@link SessionListEntry.pr}); entries with no linked PR are dropped
 * whenever a filter is active.
 */
export function filterSessionsByPr<T extends { pr?: { number: number } }>(
  entries: T[],
  filter: PrFilter,
): T[] {
  if (typeof filter === 'boolean') {
    return filter ? entries.filter(e => e.pr?.number !== undefined) : entries;
  }
  if (typeof filter === 'number') {
    return entries.filter(e => e.pr?.number === filter);
  }
  const prNumber = parsePrIdentifier(filter);
  if (prNumber === null) return [];
  return entries.filter(e => e.pr?.number === prNumber);
}

/**
 * Canonical "most-recent first" ordering for session list entries: sort by
 * recorded `date` descending, breaking ties on the session id (the newer id — ids
 * embed a monotonic timestamp+sequence — sorts first) so a paginated or merged
 * listing is deterministic across calls. The ONE comparator shared by
 * {@link SessionManager.list} and the cross-project aggregator
 * ({@link SessionManager.listAcrossProjects}) so a single-project and an
 * all-projects listing can never diverge in their ordering rules (§0b.6).
 */
export function compareSessionEntriesByDate(
  a: { id: SessionId | string; date?: string },
  b: { id: SessionId | string; date?: string },
): number {
  const bTime = b.date ? new Date(b.date).getTime() : 0;
  const aTime = a.date ? new Date(a.date).getTime() : 0;
  const byTime = (isNaN(bTime) ? 0 : bTime) - (isNaN(aTime) ? 0 : aTime);
  if (byTime !== 0) return byTime;
  return String(b.id).localeCompare(String(a.id));
}

/** Single-quote a string for safe embedding in a POSIX shell command: wrap in
 *  single quotes and escape any embedded single quote as `'\''`. Used to build the
 *  copyable cross-project resume command in {@link checkCrossProjectResume}. */
export function shellSingleQuote(s: string): string {
  return `'${s.replace(/'/g, `'\\''`)}'`;
}

/**
 * Three-way classification of resuming a session that lives in a DIFFERENT project
 * directory than the current cwd (parity: the reference's `checkCrossProjectResume`):
 *  - not cross-project (scope off, no recorded path, or same cwd) → resume in place;
 *  - cross-project but a sibling git worktree of the SAME repo → resume directly
 *    (no `cd` needed, the worktree shares the repo);
 *  - a genuinely different project → surface a copyable `command` the user can run
 *    to switch directories and resume there.
 */
export type CrossProjectResumeResult =
  | { isCrossProject: false }
  | { isCrossProject: true; isSameRepoWorktree: true; projectPath: string }
  | { isCrossProject: true; isSameRepoWorktree: false; command: string; projectPath: string };
