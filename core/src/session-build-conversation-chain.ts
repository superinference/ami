// Split from session.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import type { Message, SessionId } from './types';
import { projectSlug, MAX_SLUG_LENGTH } from './reference-journal';
import { atomicWriteFileSync } from './utils/fs-atomic';
import { shellSingleQuote } from './session-session-list-entry';
import type { CrossProjectResumeResult } from './session-session-list-entry';

/**
 * Classify whether resuming `log` crosses a project boundary relative to
 * `currentCwd`, and (for a genuinely different project) produce a copyable
 * `cd … && ami --resume <id>` command. Pure: all environment (the current cwd and
 * the set of same-repo git worktree paths) is passed in, so this core primitive has
 * no process/git dependency and the CLI picker can render the result however it
 * likes (inline badge for a same-repo worktree, clipboard-copy affordance for a
 * different project).
 *
 * FUNCTIONAL (intentional parity divergence): AMI does NOT gate worktree detection
 * behind the reference's internal `USER_TYPE === 'ant'` staged-rollout flag — the same-repo
 * worktree shortcut is always available. Parity: the reference's `checkCrossProjectResume`
 * (same discriminated result + longest-literal `startsWith(wt + sep)` containment).
 */
export function checkCrossProjectResume(
  log: { id: SessionId | string; projectPath?: string },
  currentCwd: string,
  worktreePaths: string[],
  showAllProjects: boolean,
): CrossProjectResumeResult {
  if (!showAllProjects || !log.projectPath || log.projectPath === currentCwd) {
    return { isCrossProject: false };
  }
  const projectPath = log.projectPath;
  const isSameRepoWorktree = worktreePaths.some(
    wt => projectPath === wt || projectPath.startsWith(wt + path.sep),
  );
  if (isSameRepoWorktree) {
    return { isCrossProject: true, isSameRepoWorktree: true, projectPath };
  }
  const command = `cd ${shellSingleQuote(projectPath)} && ami --resume ${shellSingleQuote(String(log.id))}`;
  return { isCrossProject: true, isSameRepoWorktree: false, command, projectPath };
}

/**
 * What the resume path should do with a session's persisted {@link Session.worktreeState}:
 *  - `'reenter'` — re-enter the recorded worktree (its coordinates are returned);
 *  - `'cleared'` — the worktree was recorded but its directory no longer exists on
 *    disk (e.g. removed by an `/exit` dialog between sessions), so the worktree
 *    binding must be dropped gracefully rather than cd'ing into a missing path;
 *  - `'none'` — the session was never inside a worktree (`undefined`) or explicitly
 *    EXITED it (`null`), so there is nothing to restore.
 */
export type WorktreeResumeResolution =
  | { action: 'none' }
  | { action: 'cleared' }
  | { action: 'reenter'; worktreePath: string; originalCwd: string; branch?: string };

export function safeCharSlice(s: string, start: number, end?: number): string {
  let result = s.slice(start, end);
  if (result.length === 0) return result;
  const lastCode = result.charCodeAt(result.length - 1);
  if (lastCode >= 0xD800 && lastCode <= 0xDBFF) result = result.slice(0, -1);
  const firstCode = result.charCodeAt(0);
  if (firstCode >= 0xDC00 && firstCode <= 0xDFFF) result = result.slice(1);
  return result;
}

export function sanitizeSessionId(id: string): string {
  return id.replace(/[\/\\]/g, '_').replace(/\.\./g, '_');
}

/** Rejects a session id that is unsafe to interpolate into a `${id}.json`
 *  filename: a path separator, NUL, or any C0/C1 control byte or DEL. */
// eslint-disable-next-line no-control-regex
export const UNSAFE_SESSION_ID_RE = /[/\\\u0000-\u001f\u007f-\u009f]/;

/**
 * Defensive format-validation boundary for a session id ingested from an untrusted
 * source (a `--resume` argument, a session URL, an on-disk filename). Accepts
 * `unknown` and returns `false` — never throws — for a non-string, an empty or
 * over-long (>255) id, `.`/`..` or anything containing `..`, or any
 * separator/control byte ({@link UNSAFE_SESSION_ID_RE}).
 *
 * Parity: the reference's `validateUuid` boundary guard, adapted to AMI's id contract
 * (AMI ids are `session-…`/user `--session-id` segments, NOT UUIDs). This is the
 * stronger up-front check the old denylist {@link sanitizeSessionId} lacked, while
 * staying consistent with the `isSafeSegment` contract used for `--session-id`
 * (spaces / non-separator Unicode remain valid). Used as the first check in
 * {@link SessionManager.load}/{@link SessionManager.loadAsync}/
 * {@link SessionManager.delete} so garbage is rejected before any path
 * construction (with {@link assertPathWithin} still the containment backstop).
 */
export function isValidSessionId(id: unknown): id is string {
  if (typeof id !== 'string') return false;
  if (id.length === 0 || id.length > 255) return false;
  if (id === '.' || id === '..' || id.includes('..')) return false;
  return !UNSAFE_SESSION_ID_RE.test(id);
}

/**
 * Classification of a `--resume`/`--continue` argument (parity: the reference's
 * `parseSessionIdentifier`): a value the user passes can be a transcript FILE
 * path, a managed session ID, or a cloud-ingress URL.
 *  - `'jsonl'` — a path ending in `.jsonl` → resume from that transcript file and
 *    mint a fresh session id (checked FIRST because a Windows absolute path like
 *    `C:\x\s.jsonl` otherwise parses as a URL with `c:` as the protocol);
 *  - `'id'` — a bare, format-valid managed session id ({@link isValidSessionId});
 *  - `'url'` — an `http`/`https` URL (cloud session ingress).
 */
export type ParsedSessionIdentifier =
  | { kind: 'jsonl'; path: string }
  | { kind: 'id'; id: string }
  | { kind: 'url'; url: string };

/**
 * Normalize a user-supplied tag before it is persisted: strip C0/C1 control bytes
 * (incl. DEL) and invisible/bidirectional Unicode (zero-width, BOM, LRM/RLM, bidi
 * embeddings/overrides/isolates) that could carry a hidden-character attack, then
 * collapse whitespace and trim. Returns `''` when nothing meaningful remains.
 * Parity: the reference's `recursivelySanitizeUnicode(tag).trim()` before `saveTag` — a
 * tag-specific normalizer (does NOT strip markdown the way the title sanitizer
 * does, so a tag token is preserved verbatim).
 */
export function normalizeSessionTag(raw: string): string {
  return raw
    // eslint-disable-next-line no-control-regex
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f-\u009f]/g, '') // C0/C1 ctrl + DEL (keep \t\n\r for the \s+ collapse below)
    .replace(/[\u200b-\u200f\u202a-\u202e\u2060\u2066-\u2069\ufeff]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

/** Outcome of {@link SessionManager.toggleTag}: a 3-way state machine over the
 *  session's single tag (add when none, replace when different, remove when the
 *  same tag is applied again) plus the two input-guard failures. */
export interface TagToggleResult {
  ok: boolean;
  /** What happened on success. Absent on failure. */
  action?: 'added' | 'replaced' | 'removed';
  /** The tag now on the session (absent when removed or on failure). */
  tag?: string;
  /** The tag that was on the session before the op, when there was one. */
  previous?: string;
  /** Guard failure reason: no active session, or an empty-after-normalize tag. */
  error?: string;
}

/**
 * Decide whether a project-slug directory NAME under the global projects root
 * belongs to one of `worktreeCwds`, using the reference's longest-prefix,
 * truncation-aware match (parity: listSessionsImpl's worktree-aware gathering):
 *  - each worktree cwd is reduced to its {@link projectSlug} (the exact slug a
 *    store dir is named after) and the candidates are tried LONGEST slug first so
 *    the most specific worktree wins over a shorter ancestor;
 *  - a dir matches by EXACT equality, or — ONLY when the slug was truncated to
 *    {@link MAX_SLUG_LENGTH} (a `-<hash>` suffix follows) — by `startsWith(slug + '-')`,
 *    so a short `…-project` dir never matches a `…-project-foo` slug;
 *  - on win32 the comparison is case-insensitive.
 * Returns the matched worktree cwd, or `null`. Pure (no fs/git): the caller passes
 * already-canonical cwds, so the matching boundary is unit-testable in isolation.
 */
export function matchWorktreeForProjectDir(
  dirName: string,
  worktreeCwds: string[],
): string | null {
  const caseInsensitive = process.platform === 'win32';
  const norm = (s: string): string => (caseInsensitive ? s.toLowerCase() : s);
  const target = norm(dirName);
  const indexed = worktreeCwds
    .map(cwd => ({ cwd, slug: norm(projectSlug(cwd)) }))
    .sort((a, b) => b.slug.length - a.slug.length);
  for (const { cwd, slug } of indexed) {
    const truncated = slug.length >= MAX_SLUG_LENGTH;
    if (target === slug || (truncated && target.startsWith(slug + '-'))) {
      return cwd;
    }
  }
  return null;
}

/** Marker file (under the store dir) recording the epoch-ms of the last completed
 *  housekeeping sweep, used to throttle the sweep to once per {@link HOUSEKEEPING_PERIOD_MS}
 *  across concurrent processes. Named with a leading dot + non-`.json` suffix so it is
 *  never picked up by {@link SessionManager.list}'s `.json` filter. */
export const HOUSEKEEPING_MARKER_FILE = '.housekeeping.marker';

/** Sub-directory of the store dir whose {@link SESSION_LOCK_FILE} serializes the
 *  housekeeping sweep across processes (reuses {@link acquireSessionLock} rather
 *  than a second lock primitive — §0b.6). */
export const HOUSEKEEPING_LOCK_SUBDIR = '.housekeeping';

/**
 * True when session retention cleanup is enabled for `cleanupPeriodDays`: a finite,
 * strictly-positive day count. A missing / non-finite / non-positive value DISABLES
 * cleanup (parity: the reference's `cleanupPeriodDays === 0` keeps everything). The ONE
 * acceptance boundary shared by {@link SessionManager.cleanupOldSessions} and the
 * throttled housekeeping runner so the "is cleanup on?" rule lives in one place.
 */
export function isSessionCleanupEnabled(cleanupPeriodDays?: number): boolean {
  return (
    cleanupPeriodDays !== undefined &&
    Number.isFinite(cleanupPeriodDays) &&
    cleanupPeriodDays > 0
  );
}

/** Sub-directory of a session store holding per-session content-addressed blob
 *  sidecars — one `<sessionId>/` dir per session (see
 *  {@link SessionManager.offloadMessage}). The ONE place the name lives so
 *  {@link SessionManager.blobDirFor} and {@link SessionManager.cleanupOrphanedBlobs}
 *  can never disagree about where blobs are stored (§0b.6). */
export const BLOBS_SUBDIR = 'blobs';

/**
 * Of a cache base's per-session sub-directory names, the ones that are ORPHANED —
 * whose name is not in `liveIds` (the session ids that still have a surviving owner
 * on disk). The pure cross-session cache-GC boundary behind
 * {@link SessionManager.cleanupOrphanedBlobs}, so the "which dirs are stale?" rule is
 * unit-testable without touching the filesystem.
 *
 * Parity: the reference's `cleanupOldImageCaches` sweep (which drops every session dir that
 * is not the CURRENT one). AMI diverges intentionally — its per-session dir is a
 * content-addressed BLOB store backing a RESUMABLE transcript, not an ephemeral
 * pasted-image cache — so EVERY live session's dir is kept (deleting a non-current
 * session's blobs would corrupt its `--resume`) and only truly owner-less dirs are
 * swept. A `"current only"` caller just passes a single-element iterable.
 */
export function collectOrphanedCacheDirs(dirNames: string[], liveIds: Iterable<string>): string[] {
  const live = liveIds instanceof Set ? liveIds : new Set(liveIds);
  return dirNames.filter(name => !live.has(name));
}

/**
 * Injectable dependencies for {@link maybeRunDailyHousekeeping} so the throttle +
 * cross-process-skip boundary is unit-testable without real time, real marker I/O,
 * or a real cross-process lock race.
 */
export interface HousekeepingDeps {
  /** Current time source (epoch ms). */
  now: () => number;
  /** Throttle period (defaults to {@link HOUSEKEEPING_PERIOD_MS}). */
  periodMs: number;
  /** Read the last-run timestamp (epoch ms) from the marker, or `null` when
   *  absent/unreadable/non-numeric. */
  readMarker: (markerPath: string) => number | null;
  /** Persist the last-run timestamp to the marker (best-effort). */
  writeMarker: (markerPath: string, ts: number) => void;
  /** Acquire the cross-process housekeeping lock; `acquired:false` means another
   *  live process holds it (skip this sweep). */
  acquireLock: (lockDir: string) => { acquired: boolean; release: () => void };
}

export function defaultReadHousekeepingMarker(markerPath: string): number | null {
  try {
    const n = parseInt(fs.readFileSync(markerPath, 'utf-8').trim(), 10);
    return Number.isFinite(n) ? n : null;
  } catch {
    return null; // absent / unreadable — treat as never-run
  }
}

export function defaultWriteHousekeepingMarker(markerPath: string, ts: number): void {
  try {
    atomicWriteFileSync(markerPath, String(ts));
  } catch {
    /* best-effort — a failed marker write just means the next process re-runs */
  }
}

// ---------------------------------------------------------------------------
// Resume-time turn-interruption recovery
// ---------------------------------------------------------------------------

/** Result of scanning a loaded transcript for a turn that was cut off mid-flight
 *  (parity: the reference's conversationRecovery). `interrupted` is true when the model
 *  requested tool calls that were never answered — i.e. the process died between
 *  the assistant's tool_use and the tool_result — so the resume path can inject a
 *  synthetic continuation instead of resuming into an inconsistent state. */
export interface TurnInterruption {
  interrupted: boolean;
  /** tool_call ids that were requested but have no matching tool result. */
  pendingToolCallIds: string[];
}

/**
 * Detect a mid-turn interruption over a loaded message list: any assistant
 * `tool_calls` id that is not satisfied by a later `tool` message (`tool_call_id`)
 * means the turn was cut off before the tool result was recorded. Pure + order-
 * preserving (pending ids are returned in request order), so the --resume path can
 * decide to synthesize a continuation prompt.
 */
export function detectTurnInterruption(messages: Message[]): TurnInterruption {
  const answered = new Set<string>();
  for (const m of messages) {
    if (m.role === 'tool' && typeof m.tool_call_id === 'string') answered.add(m.tool_call_id);
  }
  const pendingToolCallIds: string[] = [];
  for (const m of messages) {
    if (m.role !== 'assistant' || !Array.isArray(m.tool_calls)) continue;
    for (const tc of m.tool_calls) {
      if (tc && typeof tc.id === 'string' && !answered.has(tc.id)) {
        pendingToolCallIds.push(tc.id);
      }
    }
  }
  return { interrupted: pendingToolCallIds.length > 0, pendingToolCallIds };
}

// ---------------------------------------------------------------------------
// Transcript DAG reconstruction (parentUuid threading)
// ---------------------------------------------------------------------------

/**
 * The single-glyph assistant sentinel appended after a trailing user turn on
 * resume, so a transcript that ends on an unanswered user prompt is still a
 * valid request/response conversation if --resume takes no action. Parity:
 * the reference's `NO_RESPONSE_REQUESTED` (createAssistantMessage sentinel in
 * conversationRecovery.deserializeMessages).
 */
export const NO_RESPONSE_REQUESTED = '[No response requested]';

/**
 * Select the newest resumable LEAF of a transcript's parentUuid DAG: the latest
 * (last-in-order) non-sidechain message that is not referenced as any message's
 * `parentUuid` (i.e. has no child). A sidechain leaf is skipped so resume follows
 * the MAIN conversation, not a subagent branch (parity: the reference's newest
 * non-sidechain leaf selection in the --resume chain walk). Returns `undefined`
 * for an empty list or one with only sidechain messages.
 */
export function selectNewestLeaf(messages: Message[]): Message | undefined {
  const parents = new Set<string>();
  for (const m of messages) {
    if (typeof m.parentUuid === 'string') parents.add(m.parentUuid);
  }
  let leaf: Message | undefined;
  for (const m of messages) {
    if (m.isSidechain) continue;
    if (typeof m.uuid === 'string' && parents.has(m.uuid)) continue; // has a child
    leaf = m; // keep the last candidate in order → newest
  }
  return leaf;
}

/**
 * Reconstruct a linear conversation by walking the `parentUuid` chain from a leaf
 * back to its root, then returning it in chronological order (root → leaf). The
 * walk is cycle-safe: a `parentUuid` cycle (corrupt transcript) terminates the
 * walk at the repeat and returns the partial chain instead of looping forever
 * (parity: the reference's `buildConversationChain` cycle guard). When `leafUuid` is
 * omitted the {@link selectNewestLeaf newest non-sidechain leaf} is used, so a
 * loaded transcript resolves to its main-thread conversation. Branch/sidechain
 * aware: only messages on the chosen leaf's ancestor chain are returned, so a
 * forked/sidechain branch never bleeds into the reconstructed main thread.
 * Returns `[]` when no leaf exists.
 */
export function buildConversationChain(messages: Message[], leafUuid?: string): Message[] {
  const byUuid = new Map<string, Message>();
  for (const m of messages) {
    if (typeof m.uuid === 'string') byUuid.set(m.uuid, m);
  }
  const leaf = leafUuid !== undefined ? byUuid.get(leafUuid) : selectNewestLeaf(messages);
  if (!leaf) return [];
  const chain: Message[] = [];
  const seen = new Set<string>();
  let cur: Message | undefined = leaf;
  while (cur) {
    const id = cur.uuid;
    if (typeof id === 'string') {
      if (seen.has(id)) break; // cycle — return the partial chain
      seen.add(id);
    }
    chain.push(cur);
    let next: Message | undefined = typeof cur.parentUuid === 'string' ? byUuid.get(cur.parentUuid) : undefined;
    // Session-break continuation: when `parentUuid` was deliberately nullified (or
    // points outside the loaded set) to mark a session break, fall back to
    // `logicalParentUuid` so lineage spanning the break is still reconstructed into
    // one thread instead of fragmenting into two roots (parity: the reference's
    // TranscriptMessage.logicalParentUuid preservation across session-break
    // nullification). The `seen` cycle guard above bounds this just like the
    // parentUuid walk, so a logical-link loop returns a partial chain, never spins.
    if (!next && typeof cur.logicalParentUuid === 'string') {
      next = byUuid.get(cur.logicalParentUuid);
    }
    cur = next;
  }
  chain.reverse();
  return chain;
}

// ---------------------------------------------------------------------------
// Forward chunked, compact-boundary-truncating resume reader
// ---------------------------------------------------------------------------

/** Result of {@link readTranscriptForLoad} — the surviving transcript segment. */
export interface TranscriptLoadResult {
  /** Surviving transcript lines in file order: when boundary truncation applied,
   *  only the segment at/after the LAST non-preserved `compact_boundary`; with at
   *  most the single most-recent `attribution-snapshot` line appended at the end
   *  (position-independent — a restore reads only the last snapshot). Each entry is
   *  a raw JSONL string with no trailing newline. */
  lines: string[];
  /** File byte offset where the surviving segment begins (`0` when nothing was
   *  truncated). Parity: the reference's `boundaryStartOffset`. */
  boundaryStartOffset: number;
  /** `true` when the truncating boundary carried a `preservedSegment`, so messages
   *  before it were intentionally kept rather than dropped. Parity: the reference's
   *  `hasPreservedSegment`. */
  hasPreservedSegment: boolean;
}
