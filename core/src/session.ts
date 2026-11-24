import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import 'child_process';
import { StringDecoder } from 'string_decoder';
import type { Message, SessionId } from './types';
import { asSessionId } from './types';
import { projectStateDir, ndjsonSafeStringify, canonicalGitRoot, resolveConfigDir } from './reference-journal';
import { assertPathWithin } from './path-safety';
import { atomicWriteFileSync, secureMkdirSync, fsHooks, OWNER_ONLY_FILE_MODE, OWNER_ONLY_DIR_MODE } from './utils/fs-atomic';
import { getGitBranch } from './utils/git';
import './utils/display-tags';
import { escapeRegExp } from './utils/string';
import { AMI_VERSION } from './version';
import './analytics';
import { isWSL } from './utils/env';
import { normalizePermissionMode } from './permissions';
import { type ProcessIdentity, readProcessStartTimeMsSync, probeProcessIdentitySync, isSafeToSteal, isProvenAlive, mintSpawnToken } from './utils/process-identity';
import { COMPACTION_SUMMARY_PREFIX, INITIAL_ENRICH_COUNT, MAX_CONTENT_LENGTH, applyRehydratedBlob, collectMeaningfulPrompts, deriveFirstPrompt, extractJsonStringField, extractLastJsonStringField, isLoggableMessage, isSessionBlobRef, isSessionPersistenceDisabled } from './session-unescape-json-string';
import type { SessionBlobRef } from './session-unescape-json-string';
import { SESSION_KINDS, compareSessionEntriesByDate, envSessionKind, filterSessionsByPr, isBgSession, resolveSessionSummary, resolveSessionTitle, toListEntryAgent } from './session-session-list-entry';
import type { PrFilter, SessionKind, SessionListDiagnostics, SessionListEntry, SessionMetaLite, SessionSkipReason } from './session-session-list-entry';
import { BLOBS_SUBDIR, HOUSEKEEPING_LOCK_SUBDIR, HOUSEKEEPING_MARKER_FILE, buildConversationChain, collectOrphanedCacheDirs, defaultReadHousekeepingMarker, defaultWriteHousekeepingMarker, isSessionCleanupEnabled, isValidSessionId, matchWorktreeForProjectDir, normalizeSessionTag, safeCharSlice, sanitizeSessionId } from './session-build-conversation-chain';
import type { HousekeepingDeps, ParsedSessionIdentifier, TagToggleResult, TranscriptLoadResult } from './session-build-conversation-chain';
import { forkContentReplacements, isAttributionSnapshotLine, parseCompactBoundaryLine, sha256Hex } from './session-session';
import type { Session } from './session-session';
import { LOCK_RETRY_DEFAULT_DELAY_MS, LOCK_RETRY_MAX_DELAY_MS, SESSION_LOCK_FILE, assertSessionFence, formatSessionLockConflict, getProcessCommand, isPidAlive, isSessionLockingDisabled, nextFenceAfter, readLockPayload, withLockConflictMessage } from './session-session-diff-tracker';
import type { SessionLockCheck, SessionLockDeps, SessionLockOptions, SessionLockPayload } from './session-session-diff-tracker';
import { TRANSCRIPT_FILE_EXTENSION, clearResumePointer, resumePointerPath, sleepSync, validateTranscriptLine, writeResumePointer } from './session-safe-append-transcript';
import type { LockInfo, ResumePointer } from './session-safe-append-transcript';
export { INITIAL_ENRICH_COUNT, SAVE_HOOK_ADDITIONAL_CONTEXT_ENV, contentToText, contentToTexts, deriveFirstPrompt, extractJsonStringField, extractLastJsonStringField, extractLastPrompt, extractMeaningfulPreview, isHookAdditionalContextMessage, isLoggableMessage, isResumeAllProjectsEnabled, isSaveHookAdditionalContextEnabled, isSessionPersistenceDisabled, unescapeJsonString } from './session-unescape-json-string';
export { AGENT_MODEL_INHERIT, SESSION_KINDS, SESSION_SKIP_REASONS, compareSessionEntriesByDate, envSessionKind, filterSessionsByPr, getSessionDisplayTitle, isBgSession, parsePrIdentifier, resolveAgentModelOverride, resolveSessionSummary, resolveSessionTitle, toListEntryAgent } from './session-session-list-entry';
export type { CrossProjectResumeResult, PrFilter, SessionAgentIdentity, SessionKind, SessionListDiagnostics, SessionListEntry, SessionMetaLite, SessionSkipReason } from './session-session-list-entry';
export { BLOBS_SUBDIR, HOUSEKEEPING_LOCK_SUBDIR, HOUSEKEEPING_MARKER_FILE, NO_RESPONSE_REQUESTED, buildConversationChain, checkCrossProjectResume, collectOrphanedCacheDirs, detectTurnInterruption, isSessionCleanupEnabled, isValidSessionId, matchWorktreeForProjectDir, normalizeSessionTag, safeCharSlice, selectNewestLeaf } from './session-build-conversation-chain';
export type { HousekeepingDeps, ParsedSessionIdentifier, TagToggleResult, TranscriptLoadResult, TurnInterruption, WorktreeResumeResolution } from './session-build-conversation-chain';
export { appendResumeSentinel, collectContentReplacements, forkContentReplacements, getSessionMessages, resolveWorktreeStateForResume, sha256Hex } from './session-session';
export type { AttributionSnapshot, ContentReplacementEntry, ContentReplacementRecord, FileAttributionState, GetSessionMessagesOptions, Session } from './session-session';
export { LOCK_RETRY_DEFAULT_DELAY_MS, LOCK_RETRY_MAX_DELAY_MS, SESSION_LOCK_FILE, SessionDiffTracker, SessionFenceError, agentContributionRatio, applyFileContribution, assertSessionFence, formatSessionLockConflict, getProcessCommand, isPidAlive, isSessionFenceCurrent, isSessionLockingDisabled, readSessionFence, recordFileContribution } from './session-session-diff-tracker';
export type { FileChange, SessionLockCheck, SessionLockDeps, SessionLockOptions, SessionLockPayload } from './session-session-diff-tracker';
export { RESUME_POINTER_FILE, TRANSCRIPT_FILE_EXTENSION, clearResumePointer, evaluateSessionAcquisition, safeAppendTranscript, validateTranscriptLine, writeResumePointer } from './session-safe-append-transcript';
export type { LockInfo, ResumePointer, SessionAcquisitionDecision } from './session-safe-append-transcript';
export { assertPathWithin } from './path-safety';

/**
 * Upper bound on a session/transcript file we will read back into memory. A
 * corrupt or maliciously huge file must never OOM the process on --resume; a file
 * over this size is treated as unreadable (skipped) rather than loaded. Mirrors
 * the reference's size boundary on session reads.
 */
export const MAX_SESSION_READ_BYTES = 100 * 1024 * 1024; // 100 MiB

/**
 * Size of the bounded head/tail buffer used by {@link SessionManager.readSessionMetaLite}
 * when deriving a session-list row's metadata WITHOUT a full `JSON.parse` of the
 * whole file. Only this many bytes are read from each end of the file, so a 50 MB
 * transcript costs the same lite-metadata read as a 5 KB one. Parity: the reference's
 * `LITE_READ_BUF_SIZE` (sessionStoragePortable) bounded head/tail metadata read.
 */
export const LITE_READ_BUF_SIZE = 64 * 1024; // 65536

/**
 * A session file at or above this size is ALWAYS read via the bounded lite path for
 * a listing row (never fully parsed just to render a picker line), even when it falls
 * inside the {@link INITIAL_ENRICH_COUNT} enrichment window — parsing a multi-megabyte
 * transcript to show one row is never worth it. Parity: the reference's
 * `SKIP_PRECOMPACT_THRESHOLD` resume-read size guard.
 */
export const SKIP_PRECOMPACT_THRESHOLD = 5 * 1024 * 1024; // 5 MiB

/**
 * Chunk size for the forward transcript reader {@link readTranscriptForLoad}. The
 * resume load path reads the transcript fd one megabyte at a time — balancing the
 * number of read syscalls against buffer growth — instead of slurping the whole
 * file with a single `readFileSync`, so peak memory tracks the SURVIVING
 * (post-last-compact-boundary) output rather than the raw on-disk file size. Parity:
 * the reference's `TRANSCRIPT_READ_CHUNK_SIZE` (sessionStoragePortable) forward reader.
 */
export const TRANSCRIPT_READ_CHUNK_SIZE = 1024 * 1024; // 1 MiB

/**
 * Classify a `--resume` argument into {@link ParsedSessionIdentifier}, or `null`
 * when it is none of the three forms (a non-string, empty/whitespace, or a
 * path/string that is neither a `.jsonl` file, a safe session id, nor an
 * http(s) URL). The `.jsonl` test runs before URL parsing so a Windows absolute
 * transcript path is never misread as a URL. The id branch reuses the SAME
 * {@link isValidSessionId} safety boundary `load()` uses, so a `--resume <id>`
 * that classifies here is exactly the set of ids `load()` will accept — no
 * second, divergent id grammar (§0b.6).
 *
 * FUNCTIONAL (intentional parity divergence): the URL branch is restricted to
 * `http:`/`https:` schemes (the reference accepts any `new URL()`), so a transport-smuggling
 * scheme (`ext::`, `file:`, `ssh:`) can never be classified as a resumable session
 * URL — the same hardening posture AMI applies to other untrusted URL inputs.
 */
export function parseSessionIdentifier(arg: unknown): ParsedSessionIdentifier | null {
  if (typeof arg !== 'string') return null;
  const value = arg.trim();
  if (!value) return null;
  // JSONL transcript path first — a Windows absolute path parses as a URL otherwise.
  if (value.toLowerCase().endsWith(TRANSCRIPT_FILE_EXTENSION)) {
    return { kind: 'jsonl', path: value };
  }
  // A managed session id — same acceptance boundary as load()/delete().
  if (isValidSessionId(value)) {
    return { kind: 'id', id: value };
  }
  // A cloud-ingress URL (http/https only).
  try {
    const url = new URL(value);
    if (url.protocol === 'http:' || url.protocol === 'https:') {
      return { kind: 'url', url: url.href };
    }
  } catch { /* not a URL */ }
  return null;
}

/** The four persisted message roles ({@link Message}). A loaded transcript entry
 *  whose `role` is not one of these is not a turn the engine can replay, so it is
 *  dropped by {@link sanitizeLoadedSession} rather than cast-trusted. */
const VALID_MESSAGE_ROLES: ReadonlySet<string> = new Set(['user', 'assistant', 'tool', 'system']);

/**
 * True when an UNTRUSTED on-disk value is a minimally well-formed {@link Message}
 * record: a plain object carrying one of the {@link VALID_MESSAGE_ROLES}. Shape
 * beyond `role` is intentionally NOT over-constrained — `content` may be a string,
 * a {@link SessionBlobRef}, or a multimodal array, all already handled downstream —
 * so the guard's job is to reject a tampered non-message blob, not to re-type every
 * field (which would risk dropping legitimately-shaped turns written by a newer build).
 */
function isValidMessageRecord(value: unknown): value is Message {
  return (
    !!value &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    typeof (value as { role?: unknown }).role === 'string' &&
    VALID_MESSAGE_ROLES.has((value as { role: string }).role)
  );
}

/**
 * Validate + allowlist-sanitize a session parsed back from disk BEFORE it is handed
 * to any consumer. The on-disk JSON is UNTRUSTED (it may have been written by a
 * different build, hand-edited, or synced from another machine), so it is structurally
 * validated — not cast-trusted — mirroring the safeParse-before-apply posture AMI
 * already uses for other persisted configs (frito/provision `safeParse`) and the reference's
 * zod-validated resume state (sessionRestore):
 *  - the blob MUST be a plain object with a non-empty string `id`; a shapeless /
 *    top-level-array / id-less payload is REJECTED (returns `null` → the caller
 *    treats it as not-found) instead of being cast to `Session` and resumed;
 *  - `messages` is coerced to an array and every entry that is not a well-formed
 *    {@link isValidMessageRecord} is DROPPED, so a corrupt/tampered transcript can
 *    never inject an arbitrary-shaped "turn" into the restored conversation;
 *  - `settings.permissionMode` must round-trip through {@link normalizePermissionMode}
 *    (the single {@link PERMISSION_MODES} acceptance boundary); an unknown mode is
 *    DROPPED so --resume can never reinstate a bogus or dangerous permission posture
 *    from a foreign/corrupt transcript.
 * Returns the validated {@link Session} (mutated in place — the just-parsed object is
 * not shared) or `null` when the blob is not a usable session. FUNCTIONAL: a
 * structurally-broken session that was previously cast-trusted (non-array `messages`,
 * garbage message entries) is now sanitized/dropped rather than handed to the engine.
 */
function sanitizeLoadedSession(parsed: unknown): Session | null {
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
  const obj = parsed as Record<string, unknown>;
  // A session with no usable id cannot be resolved/rehydrated — reject outright.
  if (typeof obj.id !== 'string' || obj.id.trim() === '') return null;
  // Coerce messages to an array and drop any non-message entry.
  obj.messages = (Array.isArray(obj.messages) ? obj.messages : []).filter(isValidMessageRecord);
  const session = obj as unknown as Session;
  const mode = session.settings?.permissionMode;
  if (typeof mode === 'string' && normalizePermissionMode(mode) === null) {
    delete session.settings!.permissionMode;
  }
  // A tampered/foreign `agent` that is not a plain object (e.g. a string or array)
  // is dropped so consumers never destructure a non-record identity.
  const agent = (obj as { agent?: unknown }).agent;
  if (agent !== undefined && (typeof agent !== 'object' || agent === null || Array.isArray(agent))) {
    delete (obj as { agent?: unknown }).agent;
  }
  return session;
}

/**
 * Minimum interval between two housekeeping (retention) sweeps for one store: 24h.
 * Mirrors the reference's once-per-day cleanup cadence (backgroundHousekeeping's
 * `setInterval(..., 24h)` + marker throttle).
 */
export const HOUSEKEEPING_PERIOD_MS = 24 * 60 * 60 * 1000;

/**
 * Decide whether a daily housekeeping sweep is due given the last-run timestamp
 * (`null` when never run / no marker) and the current time. Due when: never run,
 * at least `periodMs` has elapsed since the last run, OR the recorded timestamp is
 * in the FUTURE relative to `now` (a corrupt / clock-skewed marker — treated as due
 * so a bad marker self-heals on the next sweep instead of wedging cleanup forever).
 * Pure — the single throttle predicate used by {@link maybeRunDailyHousekeeping}.
 */
export function isHousekeepingDue(
  lastRunMs: number | null,
  now: number,
  periodMs: number = HOUSEKEEPING_PERIOD_MS,
): boolean {
  if (lastRunMs === null) return true;
  if (lastRunMs > now) return true; // future/corrupt marker — self-heal
  return now - lastRunMs >= periodMs;
}

/**
 * Run `task` AT MOST once per {@link HousekeepingDeps.periodMs} across all processes
 * sharing `markerDir`, skipping immediately when another process is already running
 * it. This reproduces the reference's recurring-cleanup concurrency boundary
 * ("marker files and locks to throttle to once per day and skip immediately if
 * another process holds the lock", backgroundHousekeeping):
 *   1. read the day-marker — if a sweep ran within the period, skip (`false`);
 *   2. try to take the cross-process housekeeping lock — if another live process
 *      holds it, skip (`false`) rather than piling on a concurrent sweep;
 *   3. re-check the marker UNDER the lock (a racer may have finished between the
 *      first check and winning the lock) — skip if no longer due;
 *   4. run `task()`, stamp the marker with the completion time, release the lock.
 * Returns `true` only when `task` actually ran. Reuses {@link acquireSessionLock}
 * (pointed at a dedicated {@link HOUSEKEEPING_LOCK_SUBDIR}) so the stale-lock
 * reclaim / cross-host logic is shared, not re-implemented (§0b.6).
 */
export function maybeRunDailyHousekeeping(
  markerDir: string,
  task: () => void,
  depsOverride: Partial<HousekeepingDeps> = {},
): boolean {
  const deps: HousekeepingDeps = {
    now: Date.now,
    periodMs: HOUSEKEEPING_PERIOD_MS,
    readMarker: defaultReadHousekeepingMarker,
    writeMarker: defaultWriteHousekeepingMarker,
    acquireLock: (lockDir) => acquireSessionLock(lockDir),
    ...depsOverride,
  };
  const markerPath = path.join(markerDir, HOUSEKEEPING_MARKER_FILE);
  if (!isHousekeepingDue(deps.readMarker(markerPath), deps.now(), deps.periodMs)) {
    return false;
  }
  const lock = deps.acquireLock(path.join(markerDir, HOUSEKEEPING_LOCK_SUBDIR));
  if (!lock.acquired) return false; // another process is already sweeping
  try {
    // Double-check under the lock: a racer may have completed the sweep (and
    // stamped the marker) between our due-check above and winning the lock.
    if (!isHousekeepingDue(deps.readMarker(markerPath), deps.now(), deps.periodMs)) {
      return false;
    }
    task();
    deps.writeMarker(markerPath, deps.now());
    return true;
  } finally {
    lock.release();
  }
}

export class SessionManager {
  private sessionDir: string;
  private cwd: string;
  private _gitBranch?: string;
  private _gitBranchResolved = false;
  private static _idCounter = 0;
  /** Single-writer ownership held by THIS manager: the monotonic fence won at
   *  {@link acquireOwnership} + the lock release. Absent until ownership is taken
   *  (every direct `new SessionManager().save()` leaves this unset → no fence
   *  assert, unchanged behavior). */
  private _ownership?: { fence: number; release: () => void };

  constructor(cwd: string, sessionDirOverride?: string) {
    // Resume blobs live GLOBALLY under ~/.ami/projects/<slug>/sessions (like the reference's
    // ~/.claude/projects/<slug>/), not inside the repo. The slug is keyed on the CANONICAL
    // git root so every worktree/subdirectory of one repo shares a single session store
    // (resume a session started in any worktree of the repo), not a fragment per directory.
    this.cwd = cwd;
    this.sessionDir = sessionDirOverride ?? projectStateDir(canonicalGitRoot(cwd), 'sessions');
  }

  /** Git branch for the session's cwd, resolved once per manager and memoized —
   *  saves run many times per turn, so we must not re-spawn `git` each time. */
  private gitBranch(): string | undefined {
    if (!this._gitBranchResolved) {
      this._gitBranch = getGitBranch(this.cwd);
      this._gitBranchResolved = true;
    }
    return this._gitBranch;
  }

  /**
   * Ensure the session directory exists (owner-only, 0o700).
   */
  private ensureDir(): void {
    secureMkdirSync(this.sessionDir);
  }

  /**
   * Take hard single-writer ownership of this session directory: acquire the
   * cross-process {@link acquireSessionLock} (a fail-closed, PID-reuse-safe CAS —
   * a stale/dead/reused prior owner is reclaimed, a provably-live or unverifiable
   * one blocks) and remember the monotonic {@link SessionLockPayload.runtimeFence}
   * it won. Once held, every {@link save} asserts the fence first, so a superseded
   * owner (another `ami --resume`/detached run took over and advanced the fence)
   * is rejected instead of clobbering the new owner's transcript.
   *
   * Idempotent — a second call with ownership already held is a no-op reporting the
   * held fence. When the lock is force-disabled ({@link isSessionLockingDisabled})
   * or the acquire returns no fence, no ownership is recorded and `save` stays
   * unguarded (unchanged behavior). When a live OTHER holder blocks, returns
   * `{ acquired:false, blocked:true, message }` without recording ownership.
   */
  acquireOwnership(depsOverride?: Partial<SessionLockDeps>): {
    acquired: boolean;
    blocked?: boolean;
    fence?: number;
    message?: string;
  } {
    if (this._ownership) return { acquired: true, fence: this._ownership.fence };
    this.ensureDir();
    const result = acquireSessionLock(this.sessionDir, depsOverride);
    if (!result.acquired) {
      return { acquired: false, blocked: true, message: result.message };
    }
    // Only a numeric fence is enforceable; a disabled-lock no-op acquire carries
    // none, so we record nothing and leave save() unguarded (unchanged behavior).
    if (typeof result.runtimeFence === 'number') {
      this._ownership = { fence: result.runtimeFence, release: result.release };
      return { acquired: true, fence: result.runtimeFence };
    }
    return { acquired: true };
  }

  /** The monotonic fence this manager currently owns, or `undefined` if it holds
   *  no enforceable ownership (never acquired, or acquired with locking disabled). */
  ownershipFence(): number | undefined {
    return this._ownership?.fence;
  }

  /**
   * Assert this manager still owns the session at the fence it won. A no-op when
   * no enforceable ownership is held; otherwise throws {@link SessionFenceError}
   * if a newer owner has superseded this one. This is the single-writer guard
   * {@link save} runs before writing and that the engine wires into the
   * {@link import('../../../common/src/checkpoint').CheckpointManager} ownership
   * guard so an in-flight rollback from a superseded owner is rejected too.
   */
  assertOwnership(): void {
    if (this._ownership) assertSessionFence(this.sessionDir, this._ownership.fence);
  }

  /** A bound {@link assertOwnership} thunk for consumers that take a `() => void`
   *  ownership guard (e.g. the checkpoint manager), so the fence is enforced on
   *  file mutations too — not just transcript saves. */
  ownershipGuard(): () => void {
    return () => this.assertOwnership();
  }

  /** Release this manager's single-writer ownership (unlink the lock if still ours)
   *  and forget the held fence. Idempotent; a no-op when no ownership is held. The
   *  process-exit hook in {@link acquireSessionLock} is the backstop, but a clean
   *  engine shutdown should release eagerly so a peer can take over at once. */
  releaseOwnership(): void {
    if (!this._ownership) return;
    const { release } = this._ownership;
    this._ownership = undefined;
    try { release(); } catch { /* best effort — exit hook is the backstop */ }
  }

  /**
   * Save a session to disk as a JSON file.
   * Message content longer than 5000 chars is offloaded to a content-addressed
   * blob sidecar (full content preserved) with only a preview kept inline, so
   * --resume restores the complete conversation (H14).
   */
  save(session: Session): void {
    // Honour an opt-out of transcript persistence (privacy / ephemeral runs):
    // write nothing to disk when disabled. Checked first so no dir is created.
    if (isSessionPersistenceDisabled()) return;
    // Single-writer fence assert: when this manager holds ownership, reject the
    // save if a newer owner took over (fence advanced on the lock) BEFORE any I/O,
    // so a superseded process can never clobber the new owner's transcript. A
    // manager without acquired ownership (every direct `save()`) is a no-op here.
    this.assertOwnership();
    this.ensureDir();
    const safeId = sanitizeSessionId(session.id);
    const blobDir = this.blobDirFor(safeId);
    // Canonical save-persistence filter (parity: the reference's isLoggableMessage): drop
    // engine-injected turn-local nudges (isMeta) and hook-produced additional-context
    // messages (unless the operator opted in to persist them across --resume) BEFORE
    // offloading, so the saved transcript carries only loggable messages.
    const storedMessages = session.messages
      .filter(m => isLoggableMessage(m))
      .map(m => this.offloadMessage(m, blobDir));
    const filePath = path.join(this.sessionDir, `${safeId}.json`);
    assertPathWithin(filePath, this.sessionDir);
    // A user rename (customTitle) must never be clobbered by an auto-title save:
    // if this save doesn't carry one, preserve whatever is already on disk.
    // Recover a prior user rename from a bounded ~64KB head/tail read
    // ({@link readSessionMetaLite}) instead of a full fs.readFileSync + JSON.parse +
    // sanitize of the whole transcript: saveSession() runs many times per turn and
    // the transcript grows unbounded, so the old readSessionFile fallback was an
    // O(filesize) re-parse on nearly every save. save() serializes scalar metadata
    // before the messages array, so customTitle is always within the lite reader's
    // head buffer (with the reader's own tail fallback for legacy message-first files).
    const customTitle = session.customTitle ?? this.readSessionMetaLite(filePath)?.customTitle;
    // Serialize all scalar metadata BEFORE the (potentially huge) `messages` array
    // so a bounded head read ({@link readSessionMetaLite} / LITE_READ_BUF_SIZE) always
    // captures the session's metadata without parsing the whole transcript. Pulling
    // `messages` out of the spread guarantees it is emitted LAST regardless of the
    // caller's key insertion order (a spread would otherwise keep its original slot).
    const scalarMeta: Record<string, unknown> = { ...session };
    delete scalarMeta.messages;
    const data = {
      ...scalarMeta,
      id: safeId,
      customTitle,
      updatedAt: new Date().toISOString(),
      // Stamp provenance so /resume can show branch/project and migrations can
      // detect old schemas. Caller-supplied values win; otherwise derive from cwd.
      gitBranch: session.gitBranch ?? this.gitBranch(),
      projectPath: session.projectPath ?? this.cwd,
      version: session.version ?? AMI_VERSION,
      // Classify the run: a caller-supplied kind wins; else honor an env override
      // (AMI_SESSION_KIND / CLAUDE_CODE_SESSION_KIND); else default foreground.
      kind: session.kind ?? envSessionKind() ?? 'interactive',
      // Emitted LAST (see above) so metadata stays in the head buffer.
      messages: storedMessages,
    };
    // Crash-safe + owner-only: a truncated/corrupt newest session can never
    // result from an interrupted write, and transcripts stay 0o600 on shared hosts.
    // NDJSON-safe serialization escapes U+2028/U+2029 so content can't tear a line.
    atomicWriteFileSync(filePath, ndjsonSafeStringify(data, 2));
  }

  /** Directory holding a session's content-addressed blobs. */
  private blobDirFor(safeId: string): string {
    return path.join(this.sessionDir, BLOBS_SUBDIR, safeId);
  }

  /**
   * Offload an over-long string content to a blob, returning a message whose
   * content is a {@link SessionBlobRef}. Compaction summaries stay inline in
   * full; assistant content is left untouched (it was never truncated). If the
   * blob write fails we fall back to an inline truncation so a save never
   * crashes or bloats the JSON.
   */
  private offloadMessage(msg: Message, blobDir: string): Message {
    if (
      msg.role === 'system' &&
      typeof msg.content === 'string' &&
      msg.content.startsWith(COMPACTION_SUMMARY_PREFIX)
    ) {
      return msg;
    }
    if (
      (msg.role === 'user' || msg.role === 'system' || msg.role === 'tool') &&
      typeof msg.content === 'string' &&
      msg.content.length > MAX_CONTENT_LENGTH
    ) {
      const full = msg.content;
      const preview = safeCharSlice(full, 0, MAX_CONTENT_LENGTH);
      try {
        const sha = sha256Hex(full);
        secureMkdirSync(blobDir);
        const blobPath = path.join(blobDir, `${sha}.blob`);
        // Content-addressed: identical content across saves dedupes to one file.
        // Crash-safe + owner-only — blobs hold the FULL untruncated tool output /
        // user content, which routinely contains secrets and file contents.
        if (!fs.existsSync(blobPath)) atomicWriteFileSync(blobPath, full);
        const ref: SessionBlobRef = { __siSessionBlob: sha, preview, length: full.length };
        return { ...msg, content: ref as unknown as string };
      } catch {
        return { ...msg, content: preview + '\n... (truncated)' };
      }
    }
    return msg;
  }

  /** Rehydrate a stored message, replacing any {@link SessionBlobRef} content
   *  with the full blob contents (or the preview if the blob is unavailable). */
  private rehydrateMessage(msg: Message, blobDir: string): Message {
    const stored = (msg as { content?: unknown }).content;
    if (!isSessionBlobRef(stored)) return msg;
    let bytes: string | null;
    try {
      bytes = fs.readFileSync(path.join(blobDir, `${stored.__siSessionBlob}.blob`), 'utf-8');
    } catch {
      bytes = null;
    }
    return applyRehydratedBlob(msg, stored, bytes);
  }

  /** Async counterpart to {@link rehydrateMessage}: reads the blob sidecar off
   *  the event loop (so a transcript with many/large blobs doesn't block the
   *  caller) and reuses {@link applyRehydratedBlob} so the fallback text is
   *  byte-for-byte identical to the sync path. */
  private async rehydrateMessageAsync(msg: Message, blobDir: string): Promise<Message> {
    const stored = (msg as { content?: unknown }).content;
    if (!isSessionBlobRef(stored)) return msg;
    let bytes: string | null;
    try {
      bytes = await fs.promises.readFile(path.join(blobDir, `${stored.__siSessionBlob}.blob`), 'utf-8');
    } catch {
      bytes = null;
    }
    return applyRehydratedBlob(msg, stored, bytes);
  }

  /**
   * Load the most recently modified session file.
   */
  loadLatest(opts: { skipBackground?: boolean; skipLocked?: boolean; lockDeps?: Partial<SessionLockDeps> } = {}): Session | null {
    // Liveness guard for a no-arg `--continue`: when another live engine is
    // actively holding THIS store's lock, auto-resuming its newest session would
    // interleave two engines onto the same transcript/blob dir and corrupt it.
    // `checkSessionLock` returns `'blocked'` ONLY for a live, verified OTHER holder
    // (a dead / reused / cross-host-stale lock is reclaimed in-place and reads as
    // free), so a crashed session never wedges --continue (parity: the reference's
    // --continue skip of a session another live process is writing).
    if (opts.skipLocked && checkSessionLock(this.sessionDir, opts.lockDeps).kind === 'blocked') {
      return null;
    }
    const files = this.listFiles();
    if (files.length === 0) return null;

    // Sort by modification time descending. Stat each file ONCE up front (a
    // Schwartzian transform) rather than calling fs.statSync twice per comparison
    // inside the comparator — the latter costs ~2·N·log(N) synchronous stat
    // syscalls for N session files; this is O(N). Files that fail to stat sort to
    // the end (mtime 0) and are harmlessly skipped by readSessionFile's own guard.
    const withMtime = files.map((f) => {
      try {
        return { f, m: fs.statSync(f).mtimeMs };
      } catch {
        return { f, m: 0 };
      }
    });
    withMtime.sort((a, b) => b.m - a.m);
    const sortedFiles = withMtime.map((e) => e.f);

    // Return the first file that parses successfully, falling through past any
    // corrupt/truncated newest file (e.g. a save() interrupted mid-write) so
    // intact older sessions are not hidden by a single unreadable file. With
    // `skipBackground` (the no-arg `--continue` path) a `'bg'`/`'daemon'` session
    // is passed over so auto-resume picks the most-recent FOREGROUND session, not
    // a still-running backgrounded one (parity: the reference's --continue bg skip).
    for (const file of sortedFiles) {
      const session = this.readSessionFile(file);
      if (!session) continue;
      if (opts.skipBackground && isBgSession(session)) continue;
      return session;
    }
    return null;
  }

  /**
   * Load a specific session by ID.
   */
  load(id: string): Session | null {
    // Allowlist-validate the id up front: a malformed/untrusted resume arg is
    // rejected before any path construction (the stronger posture over the
    // denylist sanitizer; assertPathWithin remains the containment backstop).
    if (!isValidSessionId(id)) return null;
    const safeId = sanitizeSessionId(id);
    const filePath = path.join(this.sessionDir, `${safeId}.json`);
    assertPathWithin(filePath, this.sessionDir);
    if (!fs.existsSync(filePath)) return null;
    return this.readSessionFile(filePath);
  }

  /**
   * Async counterpart to {@link load}: lazily read a session's full transcript
   * (and rehydrate its content-addressed blob sidecars) off the main thread, so
   * resuming a large/blob-heavy conversation does not block the event loop /
   * freeze the TUI while the body streams in from disk. Returns `null` for a
   * missing, oversized (> {@link MAX_SESSION_READ_BYTES}), or corrupt file —
   * exactly like {@link load}, just non-blocking.
   *
   * Parity: the reference's async `loadFullLog` — a picker shows the cheap lite entry
   * ({@link SessionListEntry} from {@link list}) immediately and only awaits the
   * full message load when a session is actually opened, enabling a
   * "Loading session…" affordance in front of a potentially slow read.
   */
  async loadAsync(id: string): Promise<Session | null> {
    // Same allowlist boundary as the sync load() so both reject identical garbage.
    if (!isValidSessionId(id)) return null;
    const safeId = sanitizeSessionId(id);
    const filePath = path.join(this.sessionDir, `${safeId}.json`);
    assertPathWithin(filePath, this.sessionDir);
    return this.readSessionFileAsync(filePath);
  }

  /**
   * List sessions with metadata (id, date, first message preview).
   *
   * Two-pass for cost: a cheap stat-only candidate pass (1 syscall/file, no
   * content read) sorts by mtime descending, then only the `offset..offset+limit`
   * window is actually JSON-parsed — so `list({ limit })` over a large store reads
   * N stats + ~limit parses instead of parsing every file. With no `limit` the
   * full set is parsed (unchanged behaviour). The returned entries are always
   * sorted by recorded date descending so callers see a stable order regardless
   * of the mtime pre-sort.
   */
  list(opts: {
    limit?: number;
    offset?: number;
    includeSidechain?: boolean;
    dropEmpty?: boolean;
    /** Drop a specific session id from the listing — used to exclude the LIVE
     *  current session from its own resume picker so a user can't "resume" the
     *  session they're already in (parity: the reference excludes the current log). */
    excludeId?: string;
    /** Keep only sessions linked to a PR, per {@link PrFilter} (true = any PR, a
     *  number = that PR, a string = parsed via {@link parsePrIdentifier}). */
    filterByPr?: PrFilter;
    /** Keep only sessions whose recorded {@link Session.gitBranch} equals this
     *  branch — the data-model side of the resume picker's branch-scope toggle
     *  (parity: the reference's LogSelector Ctrl+B `branchFilterEnabled`
     *  `log.gitBranch === currentBranch`). A session with no recorded branch never
     *  matches an active branch filter. */
    filterByBranch?: string;
    /** Keep only sessions whose recorded {@link Session.tag} equals this tag — the
     *  data-model side of the resume picker's per-#tag tab (parity: the reference's TagTabs
     *  per-tag filtering). An empty/whitespace value is ignored (no constraint). */
    filterByTag?: string;
    /** How many of the most-recent rows are ENRICHED with a full parse (real
     *  first/last-prompt preview + message count); rows past this count — and any
     *  file at/above {@link SKIP_PRECOMPACT_THRESHOLD} regardless of position — are
     *  built from the cheap bounded {@link readSessionMetaLite} path instead. Defaults
     *  to {@link INITIAL_ENRICH_COUNT}. Pass `Infinity` to force a full parse of every
     *  row (the pre-enrichment behaviour). Parity: the reference's progressive enrichment. */
    enrichCount?: number;
    /** Optional diagnostics sink: when supplied, {@link list} populates it with the
     *  found/listed/skipped-by-reason counts for this pass (the introspection surface
     *  behind {@link listWithDiagnostics} + the `/sessions` result's `diagnostics`),
     *  so a session that is silently filtered out becomes diagnosable. */
    diagnostics?: SessionListDiagnostics;
  } = {}): SessionListEntry[] {
    const files = this.listFiles();
    // Diagnostics sink (parity: the reference's resume-discovery debug counts). `found` is
    // every discovered file; `skip`/`skipN` tally dropped rows by reason as the pass
    // runs; `listed` is stamped with the final visible count before returning.
    const diag = opts.diagnostics;
    if (diag) {
      diag.found = files.length;
      diag.listed = 0;
      diag.skipped = 0;
      diag.skippedByReason = {};
    }
    const skip = (reason: SessionSkipReason): void => {
      if (!diag) return;
      diag.skipped += 1;
      diag.skippedByReason[reason] = (diag.skippedByReason[reason] ?? 0) + 1;
    };
    const skipN = (reason: SessionSkipReason, n: number): void => {
      if (!diag || n <= 0) return;
      diag.skipped += n;
      diag.skippedByReason[reason] = (diag.skippedByReason[reason] ?? 0) + n;
    };

    // Cheap stat-only pass: carry mtime (for candidate ordering) + size (reused
    // for fileSize so we never stat a file twice). A stat race just omits the file.
    const candidates = files.map(filePath => {
      let mtimeMs = 0;
      let size: number | undefined;
      try {
        const st = fs.statSync(filePath);
        mtimeMs = st.mtimeMs;
        size = st.size;
      } catch { /* stat race — treat as oldest / size-less */ }
      return { filePath, mtimeMs, size };
    });
    candidates.sort((a, b) => b.mtimeMs - a.mtimeMs);

    const offset = opts.offset !== undefined && opts.offset > 0 ? opts.offset : 0;
    const window = opts.limit === undefined
      ? candidates.slice(offset)
      : candidates.slice(offset, offset + Math.max(0, opts.limit));

    // Dedup by the session's INTERNAL id keeping the newest readable copy: because
    // `candidates` is mtime-descending, the FIRST successful read of an id is the
    // newest valid copy; a later/staler file carrying the same id is dropped. The
    // dedup is POST-read (an id is not dropped by an unreadable newer copy), parity
    // with the reference's applySortAndLimit `seen` set. (AMI stores one id per filename,
    // so this reconciles a manually-copied/stale duplicate rather than being the
    // common path.)
    const seenIds = new Set<string>();
    const entries: SessionListEntry[] = [];
    // Progressive enrichment: full-parse the most-recent `enrichCount` rows (real
    // preview + message count); older rows, and any pathologically large file, come
    // from the cheap bounded lite read so a huge store doesn't parse every file.
    const enrichCount = opts.enrichCount ?? INITIAL_ENRICH_COUNT;
    let enriched = 0;
    for (const { filePath, size: fileSize } of window) {
      const lite = enriched >= enrichCount
        || (fileSize !== undefined && fileSize >= SKIP_PRECOMPACT_THRESHOLD);
      if (lite) {
        const entry = this.liteEntry(filePath, fileSize);
        if (!entry) { skip('unreadable'); continue; }
        if (entry.isSidechain && !opts.includeSidechain) { skip('sidechain'); continue; }
        const liteKey = String(entry.id);
        if (opts.excludeId !== undefined && liteKey === String(opts.excludeId)) { skip('current-session'); continue; }
        if (seenIds.has(liteKey)) { skip('duplicate-id'); continue; }
        seenIds.add(liteKey);
        if (opts.dropEmpty && entry.summary === undefined) { skip('empty'); continue; }
        entries.push(entry);
        continue;
      }

      // Preview only needs inline data — skip blob rehydration for speed.
      const session = this.readSessionFile(filePath, false);
      if (!session) { skip('unreadable'); continue; }

      // Subagent sidechain transcripts never pollute the resume picker unless the
      // caller explicitly opts in (parity: the reference filters `isSidechain` logs out).
      if (session.isSidechain && !opts.includeSidechain) { skip('sidechain'); continue; }

      const idKey = String(session.id);
      // Exclude the live current session so it can't appear in its own picker.
      if (opts.excludeId !== undefined && idKey === String(opts.excludeId)) { skip('current-session'); continue; }
      if (seenIds.has(idKey)) { skip('duplicate-id'); continue; }
      seenIds.add(idKey);

      enriched++;
      // Walk the message list ONCE: both the preview (first meaningful prompt) and
      // lastPrompt (last) come from the same collectMeaningfulPrompts pass, which is
      // the expensive per-message scan (stripDisplayTags + regex + extractTag). The
      // old code called extractPreview -> collectMeaningfulPrompts and then
      // extractLastPrompt -> collectMeaningfulPrompts, scanning every message twice
      // for every enriched row. Fallback semantics match extractMeaningfulPreview /
      // extractLastPrompt exactly (commandFallback is '' when absent, so use `||`).
      const { prompts, commandFallback } = collectMeaningfulPrompts(session.messages);
      const preview = prompts.length > 0 ? prompts[0] : (commandFallback || '(no messages)');
      const lastPrompt = prompts.length > 0 ? prompts[prompts.length - 1] : (commandFallback || '(no messages)');
      const summary = resolveSessionSummary(session, { lastPrompt, firstPrompt: preview });
      // Optionally drop a metadata-only session (no title and no real prompts),
      // matching the reference's listSessionsImpl which returns null for such logs.
      if (opts.dropEmpty && summary === undefined) { skip('empty'); continue; }

      // messageCount / gitBranch / metadata come at ~zero extra cost from the
      // already-parsed inline JSON; fileSize is reused from the stat pass.
      entries.push({
        id: session.id,
        date: session.updatedAt || session.createdAt,
        preview,
        lastPrompt,
        summary,
        messageCount: Array.isArray(session.messages) ? session.messages.length : 0,
        fileSize,
        gitBranch: session.gitBranch,
        title: resolveSessionTitle(session),
        tag: session.tag,
        mode: session.mode,
        pr: session.pr,
        worktree: session.worktreeState ? session.worktreeState.worktreePath : undefined,
        projectPath: session.projectPath,
        entrypoint: session.entrypoint,
        kind: session.kind,
        permissionMode: session.settings?.permissionMode,
        isSidechain: session.isSidechain,
        parentSessionId: session.parentSessionId,
        agent: toListEntryAgent(session.agent),
      });
    }

    // Branch / tag scope filters (parity: the reference's LogSelector Ctrl+B branch filter
    // and TagTabs per-#tag filtering) — applied over the parsed entries before the
    // PR filter. An entry with no recorded branch/tag never matches an active filter.
    let scoped = entries;
    if (opts.filterByBranch !== undefined) {
      const branch = opts.filterByBranch;
      const before = scoped.length;
      scoped = scoped.filter(e => e.gitBranch === branch);
      skipN('branch-filter', before - scoped.length);
    }
    if (opts.filterByTag !== undefined && opts.filterByTag.trim() !== '') {
      const tag = opts.filterByTag;
      const before = scoped.length;
      scoped = scoped.filter(e => e.tag === tag);
      skipN('tag-filter', before - scoped.length);
    }

    // PR scope filter (parity: the reference's filteredLogs PR filter) — applied after the
    // read pass so the single {@link filterSessionsByPr} acceptance boundary is reused.
    const filtered = opts.filterByPr !== undefined
      ? filterSessionsByPr(scoped, opts.filterByPr)
      : scoped;
    skipN('pr-filter', scoped.length - filtered.length);

    // Sort by date descending (most recent first), ties broken on id for a stable,
    // deterministic paginated window — via the shared {@link compareSessionEntriesByDate}
    // comparator (reused by the cross-project aggregator so ordering never diverges).
    filtered.sort(compareSessionEntriesByDate);
    if (diag) diag.listed = filtered.length;
    return filtered;
  }

  /**
   * {@link list} plus the resume-discovery {@link SessionListDiagnostics} for the
   * same pass — the introspection surface that answers "why didn't my session show up
   * in /resume?" (parity: the reference's `/resume: enriched N, M filtered out, K visible`
   * debug counts). Returns the visible entries AND a found/listed/skipped-by-reason
   * tally, so `/sessions` can report how many files were on disk and why each hidden
   * one was dropped, instead of silently omitting it. The entries are byte-for-byte
   * what {@link list} returns with the same `opts` (the shared loop fills the sink —
   * no second enumeration, no divergent filtering).
   */
  listWithDiagnostics(
    opts: Parameters<SessionManager['list']>[0] = {},
  ): { entries: SessionListEntry[]; diagnostics: SessionListDiagnostics } {
    const diagnostics: SessionListDiagnostics = { found: 0, listed: 0, skipped: 0, skippedByReason: {} };
    const entries = this.list({ ...opts, diagnostics });
    return { entries, diagnostics };
  }

  /**
   * Enumerate the distinct {@link Session.tag}s in this store with a per-tag count,
   * most-used first (ties broken alphabetically). Backs the resume picker's tab
   * strip — an `All` tab plus one `#tag` tab per tag (parity: the reference's TagTabs tab
   * model). Reads inline JSON only (no blob rehydration) via the same cheap read
   * {@link list} uses, so building the tab set over a large store stays cheap; a
   * session with no tag is ignored. The returned `tag` values can be passed back as
   * {@link list}'s `filterByTag` to realize the selected tab.
   */
  listTags(): Array<{ tag: string; count: number }> {
    const counts = new Map<string, number>();
    for (const file of this.listFiles()) {
      const s = this.readSessionFile(file, false);
      if (!s || typeof s.tag !== 'string' || !s.tag) continue;
      counts.set(s.tag, (counts.get(s.tag) ?? 0) + 1);
    }
    return [...counts.entries()]
      .map(([tag, count]) => ({ tag, count }))
      .sort((a, b) => (b.count - a.count) || a.tag.localeCompare(b.tag));
  }

  /**
   * Delete a session file by ID.
   */
  delete(id: string): void {
    // Reject a malformed id up front — same allowlist boundary as load().
    if (!isValidSessionId(id)) return;
    const safeId = sanitizeSessionId(id);
    const filePath = path.join(this.sessionDir, `${safeId}.json`);
    assertPathWithin(filePath, this.sessionDir);
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
    // Remove the session's blob sidecar so deleted content is not left on disk.
    const blobDir = this.blobDirFor(safeId);
    assertPathWithin(blobDir, this.sessionDir);
    try { fs.rmSync(blobDir, { recursive: true, force: true }); } catch { /* best effort */ }
  }

  /**
   * Session retention: delete persisted sessions whose last on-disk activity is
   * older than `cleanupPeriodDays` days, returning the number removed. A missing,
   * non-finite, or non-positive value DISABLES cleanup (returns 0 immediately) —
   * matching the reference, where `cleanupPeriodDays === 0` keeps everything and
   * the persistence skip is a separate concern ({@link isSessionPersistenceDisabled}).
   *
   * Cheap + best-effort: it relies on the stat-only mtime (set by every
   * crash-safe {@link save}/resume write) instead of parsing each file, skips a
   * file that vanishes mid-sweep (concurrent delete race), and routes each
   * removal through {@link delete} so the content-addressed blob sidecar is
   * cleaned up alongside the JSON. Parity: the reference's cleanupPeriodDays retention.
   */
  cleanupOldSessions(cleanupPeriodDays?: number): number {
    if (!isSessionCleanupEnabled(cleanupPeriodDays)) {
      return 0;
    }
    const cutoff = Date.now() - cleanupPeriodDays! * 24 * 60 * 60 * 1000;
    let removed = 0;
    for (const file of this.listFiles()) {
      let mtimeMs: number;
      try {
        mtimeMs = fs.statSync(file).mtimeMs;
      } catch {
        continue; // vanished / stat race — skip
      }
      if (mtimeMs >= cutoff) continue; // still within the retention window
      const safeId = path.basename(file).replace(/\.json$/, '');
      // Strict content guard before deleting (parity: the reference's `/^\d+\.json$/`
      // filename check before its parseInt stale sweep — adapted to AMI's non-numeric
      // ids): only remove a file whose stem is a valid session id AND whose contents
      // actually parse as a session (an `id` + `messages` array). This guarantees a
      // foreign/user `.json` that happens to sit in the session dir past the retention
      // window is NEVER deleted by cleanup. The parse is paid only for already-expired
      // files (rare), so the sweep stays cheap in the common case.
      if (!isValidSessionId(safeId)) continue;
      const parsed = this.readSessionFile(file, false);
      if (!parsed || typeof parsed.id !== 'string' || !Array.isArray(parsed.messages)) continue;
      try {
        this.delete(safeId);
        removed++;
      } catch { /* best effort — one undeletable file must not abort the sweep */ }
    }
    return removed;
  }

  /**
   * Throttled, cross-process-safe variant of {@link cleanupOldSessions}: run the
   * retention sweep for THIS store AT MOST once per {@link HOUSEKEEPING_PERIOD_MS},
   * skipping immediately when another process is already sweeping. Without this,
   * every engine start (and every one of N concurrent processes) ran a full sweep;
   * now a day-marker + lock in the store dir throttle it to once per day per store
   * with no concurrent pile-up (parity: the reference's backgroundHousekeeping marker +
   * lock-skip boundary). Returns `{ ran, removed }`: `ran` is false when the sweep
   * was throttled or another process held the lock (in which case `removed` is 0).
   * Retention still DISABLED (no `cleanupPeriodDays`) short-circuits before touching
   * the marker/lock, so the default keep-everything behaviour is unchanged.
   */
  cleanupOldSessionsThrottled(
    cleanupPeriodDays?: number,
    depsOverride?: Partial<HousekeepingDeps>,
  ): { ran: boolean; removed: number } {
    if (!isSessionCleanupEnabled(cleanupPeriodDays)) {
      return { ran: false, removed: 0 };
    }
    let removed = 0;
    const ran = maybeRunDailyHousekeeping(
      this.sessionDir,
      () => { removed = this.cleanupOldSessions(cleanupPeriodDays); },
      depsOverride,
    );
    return { ran, removed };
  }

  /**
   * Garbage-collect ORPHANED per-session blob directories under
   * `<sessionDir>/{@link BLOBS_SUBDIR}/`: a `<sessionId>/` blob dir whose owning
   * `<sessionId>.json` session file no longer exists (the session was deleted
   * out-of-band, crash-orphaned before its JSON landed, or synced without its blobs)
   * is removed, and the blob base dir itself is `rmdir`'d when it ends up empty.
   * Returns the number of blob dirs swept.
   *
   * This is the AMI-faithful analog of the reference's cross-session cache GC
   * (`cleanupOldImageCaches`, which deletes every NON-current session's image-cache
   * dir then rmdir's the base when empty). AMI diverges intentionally: its per-session
   * dir is a content-addressed BLOB store backing a RESUMABLE transcript, not an
   * ephemeral pasted-image cache — so every LIVE session's blobs are kept (deleting a
   * non-current session's blobs would corrupt its `--resume`) and only owner-less dirs
   * are swept. Which dirs are stale is decided by the pure
   * {@link collectOrphanedCacheDirs} boundary. Best-effort: a stat/read race or an
   * undeletable dir is skipped (never aborting the sweep), a stray non-dir entry under
   * the base is left untouched, and an unreadable/absent base degrades to `0`. Each
   * removal is {@link assertPathWithin}-guarded so the sweep can never escape the store.
   */
  cleanupOrphanedBlobs(): number {
    const blobsBase = path.join(this.sessionDir, BLOBS_SUBDIR);
    let dirNames: string[];
    try {
      dirNames = fs.readdirSync(blobsBase);
    } catch {
      return 0; // no blob base / unreadable — nothing to sweep
    }
    // Live ids = the stems of the session .json files currently in the store, so a
    // blob dir is kept iff its session still exists on disk.
    const liveIds = new Set(
      this.listFiles().map(f => path.basename(f).replace(/\.json$/, '')),
    );
    let removed = 0;
    for (const name of collectOrphanedCacheDirs(dirNames, liveIds)) {
      const dir = path.join(blobsBase, name);
      try {
        assertPathWithin(dir, this.sessionDir);
        if (!fs.statSync(dir).isDirectory()) continue; // leave a stray file alone
        fs.rmSync(dir, { recursive: true, force: true });
        removed++;
      } catch { /* best effort — one undeletable dir must not abort the sweep */ }
    }
    // rmdir the base when it ends up empty (parity: cleanupOldImageCaches final rmdir).
    try {
      if (fs.readdirSync(blobsBase).length === 0) fs.rmdirSync(blobsBase);
    } catch { /* non-empty / raced / gone — leave it */ }
    return removed;
  }

  /**
   * Toggle the session's single searchable {@link Session.tag} (parity: the reference's
   * `/tag` toggle/replace/remove state machine). Normalizes the input with
   * {@link normalizeSessionTag}, then:
   *  - no loadable session → `{ ok:false, error:'No active session to tag.' }`;
   *  - empty after normalize → `{ ok:false, error:'Tag name cannot be empty.' }`;
   *  - the SAME tag is already set → remove it (toggle off), `action:'removed'`;
   *  - a DIFFERENT tag is set → replace it, `action:'replaced'`;
   *  - none set → add it, `action:'added'`.
   * One tag per session; the change is persisted via {@link save}.
   */
  toggleTag(id: string, rawTag: string): TagToggleResult {
    const session = this.load(id);
    if (!session) return { ok: false, error: 'No active session to tag.' };
    const normalized = normalizeSessionTag(rawTag);
    if (!normalized) return { ok: false, error: 'Tag name cannot be empty.' };
    const previous = typeof session.tag === 'string' && session.tag ? session.tag : undefined;
    if (previous === normalized) {
      delete session.tag;
      this.save(session);
      return { ok: true, action: 'removed', previous };
    }
    session.tag = normalized;
    this.save(session);
    return { ok: true, action: previous ? 'replaced' : 'added', tag: normalized, previous };
  }

  /**
   * Branch a conversation into a NEW session (parity: the reference's forkSession).
   *
   * Every message uuid is remapped to a fresh id while the `parentUuid` threading
   * chain is preserved under the remap, so the fork is an independent transcript
   * tree that shares no ids with its source (editing one never corrupts the
   * other). `upToMessageId` cuts the branch at a specific message (matched by its
   * source uuid, inclusive) — the classic "rewind and try a different path"
   * operation. A `parentUuid` that pointed outside the kept prefix is dropped so
   * the first retained message becomes a root. The forked session is persisted and
   * returned.
   *
   * Subagent sidechain messages are excluded so the fork carries only the main
   * conversation (parity: the reference's createFork filters `isSidechain` entries). The
   * fork is given a unique `"<base> (Branch)"` title — deduped with a numbered
   * suffix on collision (`(Branch 2)`, `(Branch 3)`, …) — from `opts.title`, else
   * the source title, else its first user prompt (parity: getUniqueForkName).
   *
   * Boundary guards (parity: the reference's createFork `No conversation to branch` /
   * `No messages to branch`): forking a brand-new / empty / message-less source is
   * a clean error, never a zero-message fork. A source with no transcript at all
   * (missing/non-array/empty `messages` — the analog of the reference's unreadable or
   * zero-length transcript file) throws `'No conversation to branch'`; a source
   * that HAS entries but none that survive the main-conversation filter (every
   * entry is a subagent sidechain) throws `'No messages to branch'`.
   */
  fork(source: Session, opts: { upToMessageId?: string; title?: string } = {}): Session {
    // No transcript at all — the analog of the reference's unreadable / zero-length
    // transcript file (createFork L79-87). Caught before any work so a brand-new
    // session can never be branched into a 0-message fork.
    if (!Array.isArray(source.messages) || source.messages.length === 0) {
      throw new Error('No conversation to branch');
    }
    // Branch only the main conversation — subagent sidechains are their own trees.
    const msgsAll = source.messages.filter(m => !m.isSidechain);
    // The source had entries but none are main-conversation messages (every turn
    // was a subagent sidechain) — parity: createFork L113-115 `No messages to
    // branch` when `mainConversationEntries.length === 0`.
    if (msgsAll.length === 0) {
      throw new Error('No messages to branch');
    }
    let msgs = msgsAll;
    if (opts.upToMessageId !== undefined) {
      const cut = msgs.findIndex(m => m.uuid === opts.upToMessageId);
      if (cut === -1) {
        throw new Error(`fork: upToMessageId '${opts.upToMessageId}' not found in source session`);
      }
      msgs = msgs.slice(0, cut + 1);
    }
    // Pre-assign a fresh uuid for every message that carries one, so parentUuid
    // references within the kept prefix can be rewritten to the new ids.
    const idMap = new Map<string, string>();
    for (const m of msgs) {
      if (m.uuid) idMap.set(m.uuid, crypto.randomUUID());
    }
    const remapped: Message[] = msgs.map(m => {
      const next = { ...m } as Message;
      if (m.uuid) next.uuid = idMap.get(m.uuid);
      if (m.parentUuid !== undefined) {
        // Rewrite to the remapped parent, or drop it if the parent was truncated.
        next.parentUuid = idMap.get(m.parentUuid);
      }
      return next;
    });
    const now = new Date().toISOString();
    const baseName = (opts.title && opts.title.trim()) || this.deriveForkBaseName(source);
    const forked: Session = {
      ...source,
      id: SessionManager.newId(),
      // A distinct, collision-free branch title so /resume and the picker can tell
      // a fork apart from its source (and from other forks of the same base).
      customTitle: this.getUniqueBranchName(baseName),
      messages: remapped,
      createdAt: now,
      updatedAt: now,
    };
    // Carry the source's content-replacement records across the fork, rewritten
    // to the fork's id (the `...source` spread copied them verbatim under the OLD
    // sessionId, which the resume reader would never match). Without this the fork
    // reconstructs with an empty replacements set → previously-stubbed tool_results
    // are re-sent in full (prompt-cache miss + permanent overage). Parity: the reference's
    // createFork content-replacement preservation.
    const forkedReplacements = forkContentReplacements(source.contentReplacements, source.id, forked.id);
    if (forkedReplacements.length > 0) forked.contentReplacements = forkedReplacements;
    else delete forked.contentReplacements;
    this.save(forked);
    return forked;
  }

  /** Base name for a fork title: the source's display title, else its first user
   *  prompt via the shared {@link deriveFirstPrompt} single-line deriver
   *  (whitespace-collapsed, 100-char capped, generic fallback). */
  private deriveForkBaseName(source: Session): string {
    const title = resolveSessionTitle(source);
    if (title && title.trim()) return title.trim();
    return deriveFirstPrompt(source.messages);
  }

  /**
   * Find persisted sessions by their user-set {@link Session.customTitle}: exact
   * match when `exact` is set, otherwise a prefix match. Only inline JSON is read
   * (no blob rehydration) so a collision scan over a large store stays cheap.
   * Backs {@link getUniqueBranchName} (parity: searchSessionsByCustomTitle).
   */
  searchByTitle(query: string, opts: { exact?: boolean } = {}): Session[] {
    const out: Session[] = [];
    for (const file of this.listFiles()) {
      const s = this.readSessionFile(file, false);
      if (!s || typeof s.customTitle !== 'string') continue;
      if (opts.exact ? s.customTitle === query : s.customTitle.startsWith(query)) {
        out.push(s);
      }
    }
    return out;
  }

  /**
   * Generate a unique fork title: `"<base> (Branch)"`, or `"<base> (Branch N)"`
   * when that collides — the smallest N≥2 not already taken by an existing fork of
   * the same base. Exact + prefix search over persisted customTitles with a
   * regex-escaped base (parity: the reference's getUniqueForkName).
   */
  getUniqueBranchName(baseName: string): string {
    const candidate = `${baseName} (Branch)`;
    if (this.searchByTitle(candidate, { exact: true }).length === 0) return candidate;
    // Collision — scan every existing "<base> (Branch...)" and claim the first gap.
    const used = new Set<number>([1]); // "(Branch)" with no number counts as 1.
    const pattern = new RegExp(`^${escapeRegExp(baseName)} \\(Branch(?: (\\d+))?\\)$`);
    for (const s of this.searchByTitle(`${baseName} (Branch`)) {
      const m = s.customTitle?.match(pattern);
      if (m) used.add(m[1] ? parseInt(m[1], 10) : 1);
    }
    let next = 2;
    while (used.has(next)) next++;
    return `${baseName} (Branch ${next})`;
  }

  /**
   * Generate a timestamp-based session ID.
   */
  static newId(): SessionId {
    const now = new Date();
    const pad = (n: number, len = 2) => String(n).padStart(len, '0');
    const seq = SessionManager._idCounter++;
    const stamp = [
      now.getFullYear(),
      pad(now.getMonth() + 1),
      pad(now.getDate()),
      '-',
      pad(now.getHours()),
      pad(now.getMinutes()),
      pad(now.getSeconds()),
      '-',
      pad(now.getMilliseconds(), 3),
    ].join('');
    return asSessionId(`session-${stamp}-${seq}`);
  }

  /**
   * Mint a fresh session id for a conversation reset (e.g. `/clear`), optionally
   * recording the outgoing session as the new one's parent for lineage tracking.
   * With `setCurrentAsParent` + `currentId`, the returned descriptor carries
   * `parentSessionId` so the caller can stamp it onto the new {@link Session} and
   * the cleared-through chain stays walkable. Parity: the reference's
   * `regenerateSessionId({ setCurrentAsParent })` which sets `STATE.parentSessionId`.
   */
  static regenerateSessionId(
    opts: { setCurrentAsParent?: boolean; currentId?: SessionId } = {},
  ): { id: SessionId; parentSessionId?: SessionId } {
    const id = SessionManager.newId();
    if (opts.setCurrentAsParent && opts.currentId) {
      return { id, parentSessionId: opts.currentId };
    }
    return { id };
  }

  /**
   * Return the session directory path (useful for testing).
   */
  getSessionDir(): string {
    return this.sessionDir;
  }

  /**
   * Mark this store's session resumable after an unclean exit: write/refresh the
   * crash-recovery pointer ({@link writeResumePointer}) so a hard crash leaves a
   * fresh pointer the next startup can detect. Call on session start and
   * periodically during a run (a refresh just bumps the mtime clock).
   */
  markResumable(sessionId: SessionId | string, surface?: string): void {
    writeResumePointer(this.sessionDir, { sessionId: String(sessionId), surface });
  }

  /**
   * Read this store's crash-recovery pointer with the same read-time staleness GC
   * as {@link readResumePointer}; `null` when there is no fresh pointer (clean exit
   * last time, or a stale/corrupt one just GC'd).
   */
  resumePointer(): (ResumePointer & { ageMs: number }) | null {
    return readResumePointer(this.sessionDir);
  }

  /**
   * Clear this store's crash-recovery pointer on CLEAN exit ({@link clearResumePointer}),
   * so the next startup does not offer to resume a session that ended normally.
   */
  clearResumable(): void {
    clearResumePointer(this.sessionDir);
  }

  /** The GLOBAL projects root — `<configDir>/projects` — holding one per-cwd-slug
   *  subdir of project state. Static so cross-project listing can enumerate every
   *  project's session store without a cwd-bound manager (parity: the reference's
   *  getProjectsDir). */
  static projectsRoot(env: NodeJS.ProcessEnv = process.env): string {
    return path.join(resolveConfigDir(env), 'projects');
  }

  /**
   * Enumerate every project's `sessions` directory under the global projects root
   * (parity: the reference's gatherAllCandidates — the set scanned when a listing's `dir`
   * is omitted). Each returned path backs a dir-scoped {@link SessionManager}. A
   * missing/unreadable projects root yields `[]`; a project slug with no `sessions`
   * subdir is skipped.
   */
  static listProjectSessionDirs(env: NodeJS.ProcessEnv = process.env): string[] {
    const root = SessionManager.projectsRoot(env);
    let slugs: string[];
    try {
      slugs = fs.readdirSync(root);
    } catch {
      return [];
    }
    const dirs: string[] = [];
    for (const slug of slugs) {
      const dir = path.join(root, slug, 'sessions');
      try {
        if (fs.statSync(dir).isDirectory()) dirs.push(dir);
      } catch { /* not a dir / stat race — skip */ }
    }
    return dirs;
  }

  /**
   * The session-store dirs for a set of worktree/project cwds (parity: the reference's
   * worktree-aware gathering): reads the global projects root and returns the
   * `sessions` dir of every slug subdir that matches one of `worktreeCwds` via
   * {@link matchWorktreeForProjectDir} (longest-prefix, truncation-aware). The
   * caller supplies the full candidate set (including the current cwd), so a
   * subdirectory whose slug matches no worktree root is included simply by being in
   * the list. Deduped; a slug subdir with no `sessions` subfolder (or an unreadable
   * projects root) is skipped. Scopes {@link listAcrossProjects} to worktrees
   * instead of EVERY project.
   */
  static listWorktreeSessionDirs(
    worktreeCwds: string[],
    env: NodeJS.ProcessEnv = process.env,
  ): string[] {
    const root = SessionManager.projectsRoot(env);
    let names: string[];
    try {
      names = fs.readdirSync(root);
    } catch {
      return [];
    }
    const out: string[] = [];
    const seen = new Set<string>();
    for (const name of names) {
      if (matchWorktreeForProjectDir(name, worktreeCwds) === null) continue;
      const dir = path.join(root, name, 'sessions');
      if (seen.has(dir)) continue;
      try {
        if (fs.statSync(dir).isDirectory()) {
          seen.add(dir);
          out.push(dir);
        }
      } catch { /* no sessions subdir / stat race — skip */ }
    }
    return out;
  }

  /**
   * List sessions across MULTIPLE project session stores (parity: the reference's
   * all-projects / cross-project resume listing — `listSessionsImpl` with `dir`
   * omitted, and the picker's `showAllProjects` scope).
   *
   * With `dirs` omitted every project's sessions dir under the global projects root
   * is scanned ({@link listProjectSessionDirs}); pass an explicit set to scope the
   * aggregation (e.g. the reference's `dir` option). Each store is read via a dir-scoped
   * manager so every entry carries its own {@link SessionListEntry.projectPath}
   * (which directory it belongs to). The per-store filters (`includeSidechain`,
   * `dropEmpty`, `excludeId`, `filterByPr`, `filterByBranch`, `filterByTag`) are
   * forwarded to each {@link list} call; the merged result is sorted most-recent
   * first via the shared {@link compareSessionEntriesByDate} comparator and the
   * `offset`/`limit` window is applied to the MERGED list (so pagination spans
   * projects, not per-store). A broken store is skipped, never aborting the sweep.
   */
  static listAcrossProjects(opts: {
    dirs?: string[];
    /** Scope the aggregation to the session stores of these worktree/project cwds
     *  (via {@link listWorktreeSessionDirs}) instead of EVERY project under the
     *  global root. Ignored when an explicit `dirs` set is passed; when both are
     *  omitted every project is scanned (unchanged behaviour). Parity: the reference's
     *  `--continue` worktree fan-out. */
    worktreePaths?: string[];
    env?: NodeJS.ProcessEnv;
    limit?: number;
    offset?: number;
    includeSidechain?: boolean;
    dropEmpty?: boolean;
    excludeId?: string;
    filterByPr?: PrFilter;
    filterByBranch?: string;
    filterByTag?: string;
  } = {}): SessionListEntry[] {
    const env = opts.env ?? process.env;
    const dirs = opts.dirs
      ?? (opts.worktreePaths
        ? SessionManager.listWorktreeSessionDirs(opts.worktreePaths, env)
        : SessionManager.listProjectSessionDirs(env));
    const perStore = {
      includeSidechain: opts.includeSidechain,
      dropEmpty: opts.dropEmpty,
      excludeId: opts.excludeId,
      filterByPr: opts.filterByPr,
      filterByBranch: opts.filterByBranch,
      filterByTag: opts.filterByTag,
    };
    const merged: SessionListEntry[] = [];
    const seen = new Set<string>();
    for (const dir of dirs) {
      let entries: SessionListEntry[];
      try {
        // Dir-scoped manager: the cwd is irrelevant for listing (gitBranch is never
        // resolved on the read path), so the override alone targets the store.
        entries = new SessionManager('', dir).list(perStore);
      } catch {
        continue; // one broken store must not abort the cross-project sweep
      }
      for (const e of entries) {
        // Dedup by id across stores (a session manually copied between projects).
        const idKey = String(e.id);
        if (seen.has(idKey)) continue;
        seen.add(idKey);
        merged.push(e);
      }
    }
    merged.sort(compareSessionEntriesByDate);
    const offset = opts.offset !== undefined && opts.offset > 0 ? opts.offset : 0;
    return opts.limit === undefined
      ? merged.slice(offset)
      : merged.slice(offset, offset + Math.max(0, opts.limit));
  }

  /**
   * Resolve a session by id when its owning project directory is unknown: scan
   * every project's session store under the global projects root
   * ({@link listProjectSessionDirs}) and return the first VALID copy found (a
   * zero-byte/corrupt copy is skipped via {@link readSessionFile}'s guards, so a
   * crash-truncated copy in one project never shadows a good copy in another).
   * Returns `null` for a malformed id ({@link isValidSessionId}) or when no project
   * holds it. This is the all-projects branch of the reference's `resolveSessionFilePath`
   * (and the sibling-worktree fallback, since every worktree of one repo shares a
   * single canonical-git-root slug store) — reused by a cross-project `--resume <id>`
   * so a session recorded under another directory is still resumable.
   */
  static loadAcrossProjects(id: string, env: NodeJS.ProcessEnv = process.env): Session | null {
    if (!isValidSessionId(id)) return null;
    for (const dir of SessionManager.listProjectSessionDirs(env)) {
      const session = new SessionManager('', dir).load(id);
      if (session) return session;
    }
    return null;
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  private listFiles(): string[] {
    // FUNCTIONAL: degrade to an empty listing if the session dir can't be read
    // rather than throwing. readdirSync raises ENOENT when the dir is absent or
    // was removed between a prior existsSync and here (TOCTOU race under a
    // concurrent delete), and EACCES on a present-but-non-listable dir — in all
    // of those cases list()/loadLatest()/--resume must report "no sessions", not
    // crash the whole resume path. A single try/catch over readdir subsumes the
    // old existsSync pre-check and closes the race window.
    let entries: string[];
    try {
      entries = fs.readdirSync(this.sessionDir);
    } catch {
      return [];
    }
    return entries
      .filter(e => e.endsWith('.json'))
      .map(e => path.join(this.sessionDir, e));
  }

  private readSessionFile(filePath: string, rehydrate = true): Session | null {
    try {
      // OOM boundary: never read a pathologically large session file into memory
      // (a corrupt/hostile file must not crash --resume). Skip it as unreadable.
      // A zero-byte file (a crash-truncated/empty copy) is likewise treated as
      // NOT-found so it never shadows a valid sibling/cross-project copy (parity:
      // the reference's resolveSessionFilePath `if (s.size > 0)` guard).
      const size = fs.statSync(filePath).size;
      if (size === 0 || size > MAX_SESSION_READ_BYTES) return null;
      const raw = fs.readFileSync(filePath, 'utf-8');
      // Structural validation before trust: a shapeless/tampered blob returns null
      // (skipped as unreadable, like a parse failure). `messages` is guaranteed an
      // array of well-formed records by sanitizeLoadedSession.
      const parsed = sanitizeLoadedSession(JSON.parse(raw));
      if (!parsed) return null;
      if (rehydrate) {
        const safeId = path.basename(filePath).replace(/\.json$/, '');
        const blobDir = this.blobDirFor(safeId);
        parsed.messages = parsed.messages.map(m => this.rehydrateMessage(m, blobDir));
      }
      return parsed;
    } catch {
      return null;
    }
  }

  /** Async counterpart to {@link readSessionFile}: same OOM boundary, JSON parse,
   *  and blob rehydration, but all file I/O goes through `fs.promises` so a large
   *  read never blocks the event loop. Blobs are rehydrated concurrently. */
  private async readSessionFileAsync(filePath: string, rehydrate = true): Promise<Session | null> {
    try {
      const st = await fs.promises.stat(filePath);
      // Zero-byte-as-not-found + OOM boundary, identical to the sync reader.
      if (st.size === 0 || st.size > MAX_SESSION_READ_BYTES) return null;
      const raw = await fs.promises.readFile(filePath, 'utf-8');
      // Same structural validation as the sync reader (shapeless/tampered → null).
      const parsed = sanitizeLoadedSession(JSON.parse(raw));
      if (!parsed) return null;
      if (rehydrate) {
        const safeId = path.basename(filePath).replace(/\.json$/, '');
        const blobDir = this.blobDirFor(safeId);
        parsed.messages = await Promise.all(
          parsed.messages.map(m => this.rehydrateMessageAsync(m, blobDir)),
        );
      }
      return parsed;
    } catch {
      return null;
    }
  }


  /**
   * Recover a session's top-level metadata by a BOUNDED head (+ tail) read, without
   * a full `JSON.parse` of the transcript — the cheap path behind {@link list}'s
   * progressive enrichment so a store with thousands of sessions (or a single
   * multi-megabyte one) does not parse every message just to render a picker row.
   *
   * Reads at most {@link LITE_READ_BUF_SIZE} bytes from the head (where {@link save}
   * now serializes all scalar metadata, before the `messages` array) and, for a file
   * larger than the buffer, the same-sized tail (so an `updatedAt` that trails a huge
   * array — e.g. a legacy file written before metadata-first ordering — is still
   * found via {@link extractLastJsonStringField}). Fields are parsed only from the
   * region BEFORE the first `"messages"` key so an `id`/`tag` nested inside a message
   * can never be mistaken for the top-level value. Returns `null` when the file is
   * missing/unreadable or carries no recoverable top-level `id`.
   */
  readSessionMetaLite(filePath: string, sizeHint?: number): SessionMetaLite | null {
    let fd: number | undefined;
    try {
      const size = sizeHint ?? fs.statSync(filePath).size;
      if (size === 0) return null;
      fd = fs.openSync(filePath, 'r');
      const headLen = Math.min(size, LITE_READ_BUF_SIZE);
      const headBuf = Buffer.allocUnsafe(headLen);
      fs.readSync(fd, headBuf, 0, headLen, 0);
      const head = headBuf.toString('utf-8');
      // Only parse the metadata prefix (before the messages array) so a scalar key
      // that also appears inside a message is never read as the top-level value.
      const msgIdx = head.indexOf('"messages"');
      const metaHead = msgIdx === -1 ? head : head.slice(0, msgIdx);

      let tail = '';
      if (size > LITE_READ_BUF_SIZE) {
        const tailBuf = Buffer.allocUnsafe(LITE_READ_BUF_SIZE);
        fs.readSync(fd, tailBuf, 0, LITE_READ_BUF_SIZE, size - LITE_READ_BUF_SIZE);
        tail = tailBuf.toString('utf-8');
      }

      const id = extractJsonStringField(metaHead, 'id');
      if (id === undefined || !isValidSessionId(id)) return null;

      // Prefer the metadata prefix (collision-proof: a scalar key that also appears
      // inside a message can't be read as the top-level value). Fall back to the full
      // head (legacy/hand-written files that serialized `messages` before metadata)
      // and finally the bounded tail (fields that trail a huge array). `id` stays
      // strict to the prefix so a message's own `"id"` is never mistaken for it.
      const field = (name: string): string | undefined =>
        extractJsonStringField(metaHead, name)
        ?? extractJsonStringField(head, name)
        ?? (tail ? extractLastJsonStringField(tail, name) : undefined);

      const kindRaw = field('kind');
      const kind = kindRaw && (SESSION_KINDS as readonly string[]).includes(kindRaw)
        ? (kindRaw as SessionKind)
        : undefined;
      const modeRaw = field('mode');
      const mode = modeRaw === 'coordinator' || modeRaw === 'normal' ? modeRaw : undefined;

      return {
        id,
        createdAt: field('createdAt'),
        updatedAt: field('updatedAt'),
        customTitle: field('customTitle'),
        aiTitle: field('aiTitle'),
        gitBranch: field('gitBranch'),
        tag: field('tag'),
        version: field('version'),
        projectPath: field('projectPath'),
        entrypoint: field('entrypoint'),
        mode,
        kind,
        isSidechain: /"isSidechain"\s*:\s*true/.test(metaHead),
      };
    } catch {
      return null;
    } finally {
      if (fd !== undefined) { try { fs.closeSync(fd); } catch { /* already closed */ } }
    }
  }

  /** Build a {@link SessionListEntry} for a session OUTSIDE the enrichment window (or
   *  one too large to parse for a single row) from a bounded {@link readSessionMetaLite}
   *  read. `fileSize` is carried over from the stat pass. Returns `null` when the lite
   *  read recovers nothing usable, so the row is simply omitted. */
  private liteEntry(filePath: string, fileSize: number | undefined): SessionListEntry | null {
    const meta = this.readSessionMetaLite(filePath, fileSize);
    if (!meta) return null;
    const title = resolveSessionTitle(meta);
    const summary = resolveSessionSummary(meta);
    const preview = summary ?? '(open to load preview)';
    return {
      id: asSessionId(meta.id),
      date: meta.updatedAt || meta.createdAt || '',
      preview,
      lastPrompt: summary,
      summary,
      messageCount: 0,
      fileSize,
      gitBranch: meta.gitBranch,
      title,
      tag: meta.tag,
      mode: meta.mode,
      projectPath: meta.projectPath,
      entrypoint: meta.entrypoint,
      kind: meta.kind,
      isSidechain: meta.isSidechain,
      lite: true,
    };
  }
}

/**
 * Cross-directory resume: reconstruct a conversation from an ARBITRARY `.jsonl`
 * transcript path, not just a session in this manager's store. Reads the file
 * (honouring the same {@link MAX_SESSION_READ_BYTES} OOM boundary as the JSON
 * loaders), parses each line as a message record, skips any corrupt/truncated line
 * (a crash can tear the final line), then walks the `parentUuid` DAG via
 * {@link buildConversationChain} to return the newest non-sidechain thread in
 * chronological order. A missing / oversized / unreadable path, or one with no
 * valid message rows, yields `[]`. An explicit `leafUuid` resumes a specific
 * branch; otherwise the newest non-sidechain leaf is chosen.
 *
 * This is the core primitive behind `--resume <path.jsonl>` (parity: the reference's
 * `loadMessagesFromJsonlPath` / `loadConversationForResume(sourceJsonlFile)` for
 * resuming a session whose transcript lives OUTSIDE the current project dir). It
 * reuses the existing DAG walk rather than re-implementing leaf selection (§0b.6).
 */
export function loadMessagesFromTranscriptPath(
  jsonlPath: string,
  opts: { leafUuid?: string } = {},
): Message[] {
  let size: number;
  try {
    size = fs.statSync(jsonlPath).size;
  } catch {
    return []; // missing / inaccessible
  }
  if (size > MAX_SESSION_READ_BYTES) return []; // OOM boundary — don't slurp it
  // Forward chunked read instead of a whole-file readFileSync slurp: peak memory
  // tracks the surviving (post-last-compact-boundary) output, not the raw file
  // size. Precompact truncation is only applied for large transcripts
  // ({@link SKIP_PRECOMPACT_THRESHOLD}) — a big file almost always got big because
  // auto-compact ran, so dropping its pre-compaction prefix is both safe and the
  // whole point (parity: the reference's size-gated readTranscriptForLoad).
  const { lines } = readTranscriptForLoad(jsonlPath, {
    fileSize: size,
    applyBoundaryTruncation: size >= SKIP_PRECOMPACT_THRESHOLD,
  });
  const messages: Message[] = [];
  for (const line of lines) {
    if (!line.trim()) continue;
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      continue; // corrupt / truncated line (e.g. a crash tore the tail) — skip it
    }
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) continue;
    if (typeof (parsed as { role?: unknown }).role !== 'string') continue; // only message rows
    messages.push(parsed as Message);
  }
  return buildConversationChain(messages, opts.leafUuid);
}

/**
 * Single forward chunked read of a `.jsonl` transcript for the `--resume` load
 * path. The file is read {@link TRANSCRIPT_READ_CHUNK_SIZE} bytes at a time through
 * a `StringDecoder` (so a multi-byte UTF-8 sequence split across a chunk seam is
 * reassembled correctly) and processed line-by-line:
 *
 *  - `attribution-snapshot` lines are stripped in-stream; only the LAST one is kept
 *    and appended at EOF (a restore reads only the most-recent snapshot, so its
 *    position doesn't matter).
 *  - when `applyBoundaryTruncation`, a real `compact_boundary` line without a
 *    `preservedSegment` DISCARDS everything accumulated before it (the pre-compaction
 *    prefix) and records its offset in {@link TranscriptLoadResult.boundaryStartOffset};
 *    a boundary WITH a preserved segment keeps the earlier output.
 *
 * Because the accumulator is reset at each truncating boundary, peak memory tracks
 * the surviving output rather than the whole file — the point of the chunked read
 * for multi-megabyte sessions. A boundaryless file yields every line (identical to
 * the previous whole-file read, no behaviour change). A missing / oversized /
 * unreadable path yields an empty result. Parity: the reference's `readTranscriptForLoad`.
 */
export function readTranscriptForLoad(
  jsonlPath: string,
  opts: { applyBoundaryTruncation?: boolean; chunkSize?: number; fileSize?: number } = {},
): TranscriptLoadResult {
  const empty: TranscriptLoadResult = { lines: [], boundaryStartOffset: 0, hasPreservedSegment: false };

  let fileSize = opts.fileSize;
  if (fileSize === undefined) {
    try {
      fileSize = fs.statSync(jsonlPath).size;
    } catch {
      return empty; // missing / inaccessible
    }
  }
  if (fileSize > MAX_SESSION_READ_BYTES) return empty; // OOM boundary — don't read it
  if (fileSize === 0) return empty;

  const chunkSize = opts.chunkSize !== undefined && opts.chunkSize > 0
    ? opts.chunkSize
    : TRANSCRIPT_READ_CHUNK_SIZE;
  const applyTruncation = opts.applyBoundaryTruncation ?? false;

  let fd: number;
  try {
    fd = fs.openSync(jsonlPath, 'r');
  } catch {
    return empty;
  }

  const kept: string[] = []; // surviving data/boundary lines (reset on truncation)
  let lastSnapshot: string | undefined;
  let boundaryStartOffset = 0;
  let hasPreservedSegment = false;
  // Running file byte offset of the next line's first byte (for boundaryStartOffset).
  let offset = 0;

  const processLine = (line: string, terminated: boolean): void => {
    const lineOffset = offset;
    offset += Buffer.byteLength(line, 'utf-8') + (terminated ? 1 : 0); // +1 for the '\n'
    if (!line.trim()) return;
    if (applyTruncation) {
      const boundary = parseCompactBoundaryLine(line);
      if (boundary) {
        if (boundary.hasPreservedSegment) {
          hasPreservedSegment = true; // keep the already-accumulated output
        } else {
          kept.length = 0; // drop the entire pre-compaction prefix
          lastSnapshot = undefined;
          boundaryStartOffset = lineOffset;
          hasPreservedSegment = false;
        }
        kept.push(line); // the boundary system line itself stays in the stream
        return;
      }
    }
    if (isAttributionSnapshotLine(line)) {
      lastSnapshot = line; // retain only the most-recent snapshot
      return;
    }
    kept.push(line);
  };

  const decoder = new StringDecoder('utf-8');
  const buf = Buffer.allocUnsafe(chunkSize);
  let carry = ''; // partial trailing line spanning a chunk seam
  try {
    let filePos = 0;
    while (filePos < fileSize) {
      const bytesRead = fs.readSync(fd, buf, 0, Math.min(chunkSize, fileSize - filePos), filePos);
      if (bytesRead === 0) break;
      filePos += bytesRead;
      const text = carry + decoder.write(buf.subarray(0, bytesRead));
      let start = 0;
      let nl = text.indexOf('\n');
      while (nl !== -1) {
        processLine(text.slice(start, nl), true);
        start = nl + 1;
        nl = text.indexOf('\n', start);
      }
      carry = text.slice(start);
    }
    carry += decoder.end();
    if (carry.length > 0) processLine(carry, false); // final line without a trailing '\n'
  } finally {
    fs.closeSync(fd);
  }

  const lines = kept;
  if (lastSnapshot !== undefined) lines.push(lastSnapshot);
  return { lines, boundaryStartOffset, hasPreservedSegment };
}

/** A lock older than this whose holder identity cannot be verified (e.g. a PID
 *  from another host on an NFS/SMB-mounted session dir) is treated as stale, so
 *  cross-host locks self-heal instead of wedging --resume forever. */
export const LOCK_FALLBACK_STALE_MS = 2 * 60 * 60 * 1000; // 2h

const DEFAULT_LOCK_DEPS: SessionLockDeps = {
  isPidAlive,
  getProcessCommand,
  readProcessStartTimeMs: (pid: number) => readProcessStartTimeMsSync(pid),
  now: Date.now,
  isWsl: () => isWSL(),
};

/**
 * Spawn token for THIS process, minted once. Recorded in every lock payload this
 * process writes so a reclaimer that can echo it back (future wiring) can prove
 * reuse; today it rides alongside the start time as secondary identity evidence.
 */
const PROCESS_SPAWN_TOKEN = mintSpawnToken();

/** Is the lock currently held by a live process that is really one of ours?
 *  A dead PID, a sentinel PID, a reused PID now running an unrelated program, or
 *  an old lock we can't verify (NFS/cross-host) are all treated as NOT held. */
function lockHolderActive(
  payload: SessionLockPayload,
  deps: SessionLockDeps,
  staleMs: number = LOCK_FALLBACK_STALE_MS,
): boolean {
  const { pid } = payload;
  // PID-reuse-safe path: a lock written by a reuse-aware build carries identity
  // (a start time — possibly null — and/or a spawn token). For those locks we
  // DO NOT fall back to "steal on mtime-stale"; instead we run the fail-closed
  // four-state probe. Only a PROVABLY-gone owner (pid absent, or a verified
  // identity mismatch from PID reuse) is reclaimable; a verified-live owner, OR
  // an owner we cannot verify (indeterminate), keeps the lock held — so two
  // writers never land on one transcript. This is the deliberate opposite of the
  // legacy steal-on-(pid-dead OR mtime-stale) heuristic below.
  const hasIdentity = payload.processStartTimeMs !== undefined || payload.spawnToken !== undefined;
  if (hasIdentity) {
    if (pid === process.pid) return true;
    const identity: ProcessIdentity = {
      pid,
      processStartTimeMs: payload.processStartTimeMs ?? null,
      spawnToken: payload.spawnToken ?? null,
    };
    const probe = probeProcessIdentitySync(identity, {
      isPidPresent: (p) => deps.isPidAlive(p),
      readProcessStartTimeMs: (p) => deps.readProcessStartTimeMs(p),
    });
    if (isProvenAlive(probe)) return true;
    // `exited` / `identity-mismatch` ⇒ safe to steal ⇒ not held. `indeterminate`
    // ⇒ fail closed ⇒ held (never reclaimed on an unverifiable owner).
    return !isSafeToSteal(probe);
  }
  if (!deps.isPidAlive(pid)) {
    // WSL cross-OS boundary guard: when the session dir is shared with a
    // Windows-native AMI (symlink / AMI_DATA_DIR on /mnt), the holder's PID is a
    // Windows PID that is NOT probeable from WSL and `isPidAlive` reports it dead.
    // Reclaiming on liveness alone would evict a live cross-OS holder, so under
    // WSL we fall through to the time-based fallback: the lock is treated as held
    // until it ages past {@link LOCK_FALLBACK_STALE_MS} (bounded self-heal), never
    // reclaimed purely because an un-probeable PID "looks dead" (parity: the reference's
    // `getPlatform() !== 'wsl'` guard before a stale-session sweep).
    if (deps.isWsl()) return deps.now() - payload.startedAt <= staleMs;
    return false;                                // dead / sentinel → stale
  }
  if (pid === process.pid) return true;          // it's us → held
  const command = deps.getProcessCommand(pid);
  if (command === null) {
    // Can't verify identity — trust the PID check unless the lock is very old
    // (self-heal cross-host / NFS locks whose PID is meaningless locally).
    const age = deps.now() - payload.startedAt;
    return age <= staleMs;
  }
  // Verify the live process looks like ours; otherwise the PID was reused.
  const c = command.toLowerCase();
  const execHint = payload.execPath ? path.basename(payload.execPath).toLowerCase() : '';
  return c.includes('ami') || (!!execHint && c.includes(execHint));
}

function makeLockPayload(deps: SessionLockDeps, runtimeFence = 1): string {
  const payload: SessionLockPayload = {
    pid: process.pid,
    startedAt: deps.now(),
    execPath: process.execPath,
    // Reuse-safe identity: record OUR start time + spawn token so a later
    // reclaimer runs the fail-closed probe instead of the mtime heuristic.
    processStartTimeMs: deps.readProcessStartTimeMs(process.pid),
    spawnToken: PROCESS_SPAWN_TOKEN,
    // Monotonic single-writer fence: 1 for a fresh lock, prior+1 on reclaim.
    runtimeFence,
  };
  return JSON.stringify(payload);
}

// Release handlers for locks held by THIS process, run on graceful exit so a
// lock is freed even if the caller never invokes release() (signal / crash /
// forgotten cleanup). Mirrors the reference's registerCleanup-at-acquire.
const activeLockReleases = new Set<() => void>();

// Zero-syscall ownership map: the lock paths this process currently holds a fresh
// (un-released) handle for. Added when a fresh acquire wins, removed on release —
// so {@link isSessionLockHeldLocally} can answer "do I hold this?" with no disk I/O
// (parity: the reference's `isLockHeldLocally`, used to gate a per-turn release so a
// non-holding turn never touches disk).
const locallyHeldLockPaths = new Set<string>();

/**
 * Zero-syscall check: does THIS process currently believe it holds the session
 * lock for `sessionDir`? True iff a fresh {@link acquireSessionLock} won the lock
 * here and its `release()` has not run yet. No filesystem access — purely the
 * in-process ownership map. Parity: the reference's `isLockHeldLocally()`.
 */
export function isSessionLockHeldLocally(sessionDir: string): boolean {
  return locallyHeldLockPaths.has(path.join(sessionDir, SESSION_LOCK_FILE));
}
let exitHookInstalled = false;
function installExitHook(): void {
  if (exitHookInstalled) return;
  exitHookInstalled = true;
  process.once('exit', releaseAllSessionLocks);
}

/** Release every session lock this process still holds. Registered as the
 *  process-exit cleanup and callable directly (introspection / shutdown). */
export function releaseAllSessionLocks(): void {
  for (const release of [...activeLockReleases]) {
    try { release(); } catch { /* best effort */ }
  }
}

function makeAcquiredHandle(
  lockPath: string,
  onCompromised?: (reason: string) => void,
  runtimeFence?: number,
): { acquired: true; fresh: true; runtimeFence?: number; release: () => void } {
  let released = false;
  const release = (): void => {
    if (released) return;
    released = true;
    activeLockReleases.delete(release);
    locallyHeldLockPaths.delete(lockPath);
    // Ownership-verified release: only unlink the lock if it STILL belongs to us.
    // If another live process judged our lock stale (dead PID / cross-host fallback)
    // and reclaimed it, the file now carries their pid — deleting it would wipe a
    // lock we no longer own (boundary violation). A missing/corrupt lock is a no-op.
    const payload = readLockPayload(lockPath);
    if (payload && payload.pid !== process.pid) {
      // Our held lock was taken over by another process while we ran — surface the
      // compromise so the caller can react (abort the write, warn, re-acquire)
      // instead of silently losing exclusivity (parity: proper-lockfile
      // onCompromised). A throwing callback must never break release().
      if (onCompromised) {
        try { onCompromised(`session lock at ${lockPath} taken over by pid ${payload.pid}`); }
        catch { /* a recovery callback must never break release */ }
      }
      return;
    }
    try { fsHooks.unlinkSync(lockPath); } catch { /* already gone */ }
  };
  activeLockReleases.add(release);
  locallyHeldLockPaths.add(lockPath);
  installExitHook();
  return { acquired: true, fresh: true, runtimeFence, release };
}

export function acquireSessionLock(
  sessionDir: string,
  depsOverride?: Partial<SessionLockDeps>,
  options: SessionLockOptions = {},
): { acquired: boolean; fresh?: boolean; pid?: number; runtimeFence?: number; message?: string; release: () => void } {
  // Force-disabled: report an un-fresh, no-op "acquired" so every caller behaves as
  // if it owns the dir, without ever touching the filesystem (no lock written, no
  // exit hook, no reclaim). `fresh:false` so a fresh-gated side effect does not fire.
  if (isSessionLockingDisabled()) {
    return { acquired: true, fresh: false, pid: process.pid, release: () => {} };
  }
  const deps: SessionLockDeps = { ...DEFAULT_LOCK_DEPS, ...depsOverride };
  const staleMs = options.stale ?? LOCK_FALLBACK_STALE_MS;
  const retries = options.retries !== undefined && options.retries > 0 ? options.retries : 0;
  const baseDelay = options.retryDelayMs !== undefined && options.retryDelayMs >= 0
    ? options.retryDelayMs
    : LOCK_RETRY_DEFAULT_DELAY_MS;
  const lockPath = path.join(sessionDir, SESSION_LOCK_FILE);
  // Create the dir owner-only when absent; do NOT chmod an existing dir here so
  // a deliberately read-only session dir still surfaces the write EACCES (the
  // save() path tightens pre-existing dir perms via secureMkdirSync instead).
  if (!fs.existsSync(sessionDir)) {
    fs.mkdirSync(sessionDir, { recursive: true, mode: OWNER_ONLY_DIR_MODE });
  }

  // A single acquire attempt (with the inner bounded stale-reclaim retry). Returns
  // the final result; `blockedByLiveHolder` tells the outer contended-retry loop
  // whether waiting could help (a live holder may release) vs. giving up now.
  const tryAcquireOnce = (): {
    result: { acquired: boolean; fresh?: boolean; pid?: number; release: () => void };
    blockedByLiveHolder: boolean;
  } => {
    // Bounded: at most one stale-reclaim retry (replaces unbounded self-recursion
    // that a racing writer recreating the lock could blow the stack with).
    // `reclaimFence` starts at 1 (a brand-new lock) and is bumped to prior+1 when
    // we reclaim a stale lock, so the fence advances monotonically on every
    // takeover and a superseded owner's write can be fenced out.
    let reclaimFence = 1;
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        fsHooks.writeFileSync(lockPath, makeLockPayload(deps, reclaimFence), { flag: 'wx', mode: OWNER_ONLY_FILE_MODE });
      } catch (err: any) {
        if (err?.code !== 'EEXIST') throw err;
        const payload = readLockPayload(lockPath);
        // Re-entrant: this process already holds the lock for this dir. Report an
        // acquired, NON-fresh result with a no-op release (the original fresh handle
        // still owns the on-disk lock and is the one that unlinks it) — so a caller
        // can fire an "entered" side effect only on `fresh` and never double-unlink
        // (parity: the reference's AcquireResult `{kind:'acquired',fresh:false}` reentrant).
        if (payload && payload.pid === process.pid) {
          return { result: { acquired: true, fresh: false, pid: payload.pid, release: () => {} }, blockedByLiveHolder: false };
        }
        if (payload && lockHolderActive(payload, deps, staleMs)) {
          return { result: { acquired: false, pid: payload.pid, release: () => {} }, blockedByLiveHolder: true };
        }
        // Stale / corrupt / unreadable → reclaim and retry the exclusive create,
        // advancing the monotonic fence past the reclaimed owner's.
        reclaimFence = nextFenceAfter(payload?.runtimeFence);
        try { fsHooks.unlinkSync(lockPath); } catch { /* raced away */ }
        continue;
      }
      // Verify we actually won the create (a racer may have recreated the lock
      // between our unlink and this wx-write, which would have thrown EEXIST; if
      // instead both reclaimed and one overwrote, re-read confirms ownership).
      const verify = readLockPayload(lockPath);
      if (verify?.pid !== process.pid) {
        return { result: { acquired: false, pid: verify?.pid, release: () => {} }, blockedByLiveHolder: true };
      }
      return { result: makeAcquiredHandle(lockPath, options.onCompromised, verify.runtimeFence), blockedByLiveHolder: false };
    }
    // Lost the recovery race after the bounded retry.
    const payload = readLockPayload(lockPath);
    return { result: { acquired: false, pid: payload?.pid, release: () => {} }, blockedByLiveHolder: true };
  };

  // Outer contended-retry loop: when a LIVE holder blocks us and the caller asked
  // for retries, wait (bounded linear backoff) and re-probe — the holder may have
  // released in the meantime (parity: proper-lockfile LockOptions.retries).
  for (let round = 0; ; round++) {
    const { result, blockedByLiveHolder } = tryAcquireOnce();
    if (result.acquired || !blockedByLiveHolder || round >= retries) {
      return withLockConflictMessage(result, sessionDir);
    }
    sleepSync(Math.min(baseDelay * (round + 1), LOCK_RETRY_MAX_DELAY_MS));
  }
}

export function isSessionLocked(
  sessionDir: string,
  depsOverride?: Partial<SessionLockDeps>,
): boolean {
  if (isSessionLockingDisabled()) return false;
  const deps: SessionLockDeps = { ...DEFAULT_LOCK_DEPS, ...depsOverride };
  const lockPath = path.join(sessionDir, SESSION_LOCK_FILE);
  if (!fs.existsSync(lockPath)) return false;
  const payload = readLockPayload(lockPath);
  if (!payload) return false;
  return lockHolderActive(payload, deps);
}

/**
 * Check the session lock for `sessionDir` WITHOUT acquiring it, yet still perform
 * stale-PID recovery: a lock whose holder is dead / a reused PID / a cross-host
 * lock aged past the fallback window (and a corrupt/unparseable lock) is UNLINKED
 * during the check so a crashed session's lock can't block a pure "may I?" query —
 * but no new lock is created (the caller defers the real acquire). Returns:
 *  - `{kind:'free'}` — no lock, or a stale one that was just recovered;
 *  - `{kind:'held_by_self',pid}` — this process already holds it (zero-syscall
 *    local ownership first, else the on-disk pid matches);
 *  - `{kind:'blocked',pid}` — a live, verified other holder owns it.
 * Parity: the reference's `checkComputerUseLock` (the `defersLockAcquire` contract).
 */
export function checkSessionLock(
  sessionDir: string,
  depsOverride?: Partial<SessionLockDeps>,
  options: Pick<SessionLockOptions, 'stale'> = {},
): SessionLockCheck {
  // Force-disabled: every dir reads as free, so a deferred-acquire caller never
  // treats a live holder as blocking (matches acquireSessionLock's always-succeed).
  if (isSessionLockingDisabled()) return { kind: 'free' };
  const deps: SessionLockDeps = { ...DEFAULT_LOCK_DEPS, ...depsOverride };
  const staleMs = options.stale ?? LOCK_FALLBACK_STALE_MS;
  const lockPath = path.join(sessionDir, SESSION_LOCK_FILE);
  if (!fs.existsSync(lockPath)) return { kind: 'free' };
  const payload = readLockPayload(lockPath);
  if (!payload) {
    // Corrupt/unparseable — can't identify a holder; recover so it can't wedge.
    try { fsHooks.unlinkSync(lockPath); } catch { /* raced away */ }
    return { kind: 'free' };
  }
  if (isSessionLockHeldLocally(sessionDir) || payload.pid === process.pid) {
    return { kind: 'held_by_self', pid: payload.pid };
  }
  if (lockHolderActive(payload, deps, staleMs)) {
    return { kind: 'blocked', pid: payload.pid, message: formatSessionLockConflict(sessionDir, payload.pid) };
  }
  // Stale (dead / reused PID / cross-host-expired) — recover, but do NOT acquire.
  try { fsHooks.unlinkSync(lockPath); } catch { /* raced away */ }
  return { kind: 'free' };
}

/**
 * Diagnostics for the session lock in `sessionDir`: returns a {@link LockInfo}
 * snapshot, or `null` when no lock file exists or it is corrupt/unparseable.
 * Read-only — never acquires, reclaims, or deletes. Parity: the reference's per-lock
 * diagnostics read.
 */
export function getSessionLockInfo(
  sessionDir: string,
  depsOverride?: Partial<SessionLockDeps>,
): LockInfo | null {
  const deps: SessionLockDeps = { ...DEFAULT_LOCK_DEPS, ...depsOverride };
  const lockPath = path.join(sessionDir, SESSION_LOCK_FILE);
  if (!fs.existsSync(lockPath)) return null;
  const payload = readLockPayload(lockPath);
  if (!payload) return null;
  return {
    path: lockPath,
    pid: payload.pid,
    startedAt: payload.startedAt,
    execPath: payload.execPath,
    ageMs: deps.now() - payload.startedAt,
    active: lockHolderActive(payload, deps),
    self: payload.pid === process.pid,
  };
}

/**
 * Bulk lock introspection across several session directories: returns a
 * {@link LockInfo} for every dir that currently holds a (parseable) lock, skipping
 * dirs with no/corrupt lock. Per-dir errors are isolated so one unreadable dir
 * never aborts the sweep. Parity: the reference's `getAllLockInfo` bulk diagnostics.
 */
export function getAllLockInfo(
  sessionDirs: string[],
  depsOverride?: Partial<SessionLockDeps>,
): LockInfo[] {
  const out: LockInfo[] = [];
  for (const dir of sessionDirs) {
    try {
      const info = getSessionLockInfo(dir, depsOverride);
      if (info) out.push(info);
    } catch { /* best effort — skip an unreadable dir */ }
  }
  return out;
}

/**
 * Bulk reclaim stale session locks across a set of session directories, returning
 * the number swept. For each dir's `.session.lock`: a corrupt/unparseable lock,
 * or one whose holder is no longer active ({@link lockHolderActive} — a dead,
 * sentinel, reused, or cross-host-expired PID), is UNLINKED so a crashed/abandoned
 * session can't wedge `--resume` for that directory forever. A lock currently held
 * by THIS process ({@link isSessionLockHeldLocally}) is never swept, and per-dir
 * errors are isolated so one unreadable dir never aborts the sweep.
 *
 * This is the proactive counterpart to the lazy reclaim in {@link acquireSessionLock}
 * (which only reclaims when a NEW acquire hits the same dir): it garbage-collects
 * orphaned locks in dirs that are never re-acquired. Parity: the reference's
 * `cleanupStaleLocks(locksDir)` directory sweep, adapted to AMI's one-lock-per-
 * session-dir model (callers pass {@link SessionManager.listProjectSessionDirs}).
 */
export function cleanupStaleLocks(
  sessionDirs: string[],
  depsOverride?: Partial<SessionLockDeps>,
): number {
  const deps: SessionLockDeps = { ...DEFAULT_LOCK_DEPS, ...depsOverride };
  let cleaned = 0;
  for (const dir of sessionDirs) {
    try {
      const lockPath = path.join(dir, SESSION_LOCK_FILE);
      if (!fs.existsSync(lockPath)) continue;
      // Never sweep a lock this process still owns.
      if (isSessionLockHeldLocally(dir)) continue;
      const payload = readLockPayload(lockPath);
      if (payload && lockHolderActive(payload, deps)) continue; // live, verified holder
      // Corrupt/unparseable OR dead-holder lock → reclaim.
      try {
        fsHooks.unlinkSync(lockPath);
        cleaned++;
      } catch { /* raced away — already gone */ }
    } catch { /* per-dir error isolated — never abort the sweep */ }
  }
  return cleaned;
}

/** A resume pointer older than this (measured by the file MTIME — NOT an embedded
 *  timestamp — so a periodic no-content-change re-write serves as a liveness
 *  refresh) is treated as stale and GC'd on read, so a crash-recovery offer never
 *  keeps re-firing after a session is long abandoned (parity: the reference's bridge
 *  pointer 4h rolling TTL). */
export const RESUME_POINTER_TTL_MS = 4 * 60 * 60 * 1000; // 4h

/**
 * Read the crash-recovery resume pointer for a session store, returning the pointer
 * plus its `ageMs` (ms since last write, derived from the file MTIME — the rolling
 * staleness anchor) or `null`. A missing / unreadable / schema-invalid / STALE
 * (older than {@link RESUME_POINTER_TTL_MS}) pointer yields `null`; an invalid or
 * stale pointer is additionally DELETED on read so a crashed session's offer
 * self-heals and never re-fires forever (parity: the reference's readBridgePointer
 * read-time GC). `now` is injectable for deterministic TTL tests.
 */
export function readResumePointer(
  sessionDir: string,
  now: () => number = Date.now,
): (ResumePointer & { ageMs: number }) | null {
  const file = resumePointerPath(sessionDir);
  let mtimeMs: number;
  let raw: string;
  try {
    // stat for mtime (the staleness anchor) then read; mtime IS returned data, not
    // a TOCTOU guard — a vanished/unreadable file simply yields null below.
    mtimeMs = fs.statSync(file).mtimeMs;
    raw = fs.readFileSync(file, 'utf-8');
  } catch {
    return null; // missing / unreadable
  }
  let parsed: unknown;
  try { parsed = JSON.parse(raw); } catch { parsed = null; }
  const sessionId = (parsed as { sessionId?: unknown } | null)?.sessionId;
  if (typeof sessionId !== 'string' || !isValidSessionId(sessionId)) {
    clearResumePointer(sessionDir); // corrupt / foreign — GC so it can't wedge
    return null;
  }
  const ageMs = Math.max(0, now() - mtimeMs);
  if (ageMs > RESUME_POINTER_TTL_MS) {
    clearResumePointer(sessionDir); // stale — GC and decline the offer
    return null;
  }
  const surface = (parsed as { surface?: unknown }).surface;
  return {
    sessionId,
    ...(typeof surface === 'string' ? { surface } : {}),
    ageMs,
  };
}

export function repairTranscriptFile(
  filePath: string,
): { repairedLines: number; droppedLines: number } {
  if (!fs.existsSync(filePath)) {
    return { repairedLines: 0, droppedLines: 0 };
  }

  // OOM boundary: a transcript larger than the read cap is not slurped into
  // memory for repair (it would risk crashing the process); leave it untouched.
  if (fs.statSync(filePath).size > MAX_SESSION_READ_BYTES) {
    return { repairedLines: 0, droppedLines: 0 };
  }

  const raw = fs.readFileSync(filePath, 'utf-8');
  if (!raw.trim()) {
    return { repairedLines: 0, droppedLines: 0 };
  }

  const lines = raw.split('\n').filter(l => l.trim() !== '');
  const valid: string[] = [];
  let droppedLines = 0;

  for (let i = 0; i < lines.length; i++) {
    if (validateTranscriptLine(lines[i])) {
      valid.push(lines[i]);
    } else if (i === lines.length - 1) {
      // Last line is truncated/corrupt — drop it silently
      droppedLines++;
    } else {
      // Mid-file corruption — also drop
      droppedLines++;
    }
  }

  if (droppedLines > 0) {
    // Crash-safe + owner-only rewrite of the repaired file.
    atomicWriteFileSync(filePath, valid.map(l => l + '\n').join(''));
  }

  return { repairedLines: valid.length, droppedLines };
}
