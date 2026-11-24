// Split from session.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import { ndjsonSafeStringify } from './reference-journal';
import { atomicWriteFileSync, secureMkdirSync, fsHooks, OWNER_ONLY_FILE_MODE } from './utils/fs-atomic';
import { type ProcessIdentityProbe, isSafeToSteal, isProvenAlive } from './utils/process-identity';
import { isValidSessionId } from './session-build-conversation-chain';
import { assertSessionFence, isPidAlive, nextFenceAfter } from './session-session-diff-tracker';
import type { SessionLockPayload } from './session-session-diff-tracker';

/** Fail-closed CAS acquisition decision (parity: Orca's evaluateAgentSessionAcquisition). */
export type SessionAcquisitionDecision =
  | { decision: 'granted'; nextFence: number }
  | { decision: 'refused'; reason: 'fenceStale' | 'ownerAlive' | 'ownerUnproven' };

/**
 * Pure fail-closed adjudication of whether a caller may take over a lease.
 *
 * Every arm fails closed: a fence mismatch refuses (`fenceStale`); a verified-live
 * owner refuses (`ownerAlive`); and crucially an UNVERIFIABLE owner refuses
 * (`ownerUnproven`) rather than granting — expiry/indeterminacy never creates a
 * second writer. A grant is issued ONLY when the prior owner is provably gone
 * (`exited` / `identity-mismatch`), and it advances the monotonic fence. A null
 * payload (no recorded owner) grants at fence 1.
 *
 * Mirrors Orca `evaluateAgentSessionAcquisition`: "a wrong answer here creates two
 * writers on one provider session," so it refuses on any doubt.
 */
export function evaluateSessionAcquisition(args: {
  payload: SessionLockPayload | null;
  expectedFence: number;
  probe: ProcessIdentityProbe;
}): SessionAcquisitionDecision {
  const { payload, expectedFence, probe } = args;
  if (!payload) return { decision: 'granted', nextFence: 1 };
  const currentFence = payload.runtimeFence ?? 1;
  if (expectedFence !== currentFence) {
    return { decision: 'refused', reason: 'fenceStale' };
  }
  if (isProvenAlive(probe)) {
    return { decision: 'refused', reason: 'ownerAlive' };
  }
  if (!isSafeToSteal(probe)) {
    // indeterminate — fail closed, never grant a second writer on an unproven owner.
    return { decision: 'refused', reason: 'ownerUnproven' };
  }
  return { decision: 'granted', nextFence: nextFenceAfter(payload.runtimeFence) };
}

/** Introspection snapshot of one session lock, for diagnostics / a `/doctor`-style
 *  surface. `active` reflects {@link lockHolderActive} (a dead/sentinel/reused/
 *  cross-host-stale holder reads as inactive, i.e. reclaimable). Parity: the reference's
 *  pidLock `LockInfo` + `getAllLockInfo` diagnostics. */
export interface LockInfo {
  /** Absolute path of the lock file. */
  path: string;
  pid: number;
  startedAt: number;
  execPath: string;
  /** Milliseconds since the lock was acquired (by the deps clock). */
  ageMs: number;
  /** True when a live, verified holder currently owns the lock. */
  active: boolean;
  /** True when the lock is held by THIS process. */
  self: boolean;
}

// ---------------------------------------------------------------------------
// Crash-recovery resume pointer — "the process died unclean last run → offer to
// resume". Written on session start, refreshed during a run (an mtime bump), and
// cleared on clean exit. If the process dies hard (crash / kill -9 / terminal
// closed) the pointer survives so the next startup can detect it and offer resume.
// ---------------------------------------------------------------------------

/** Filename of the per-session-store crash-recovery pointer. */
export const RESUME_POINTER_FILE = '.resume-pointer.json';

/** On-disk crash-recovery pointer payload. */
export interface ResumePointer {
  /** The session id to offer resuming. */
  sessionId: string;
  /** The invocation surface that wrote it (e.g. `'cli'`, `'detached'`). */
  surface?: string;
}

export function resumePointerPath(sessionDir: string): string {
  return path.join(sessionDir, RESUME_POINTER_FILE);
}

/**
 * Write (or REFRESH) the crash-recovery resume pointer for a session store. Called
 * on session start and periodically during a run: re-writing with the same
 * sessionId is a cheap no-content-change write whose only job is to bump the file
 * MTIME — the rolling-staleness clock {@link readResumePointer} checks (parity:
 * the reference's writeBridgePointer mtime-refresh idiom). A malformed sessionId is
 * rejected (never persist garbage). Best-effort: a crash-recovery aid must never
 * itself crash the session, so a write failure is swallowed.
 */
export function writeResumePointer(sessionDir: string, pointer: ResumePointer): void {
  if (!isValidSessionId(pointer.sessionId)) return;
  const payload: ResumePointer = {
    sessionId: pointer.sessionId,
    ...(typeof pointer.surface === 'string' && pointer.surface ? { surface: pointer.surface } : {}),
  };
  try {
    secureMkdirSync(sessionDir);
    atomicWriteFileSync(resumePointerPath(sessionDir), JSON.stringify(payload));
  } catch { /* best effort — never crash on a recovery-aid write */ }
}

/**
 * Clear the crash-recovery resume pointer (clean shutdown, or GC of a stale/corrupt
 * pointer during a read). Best-effort — a missing file is a no-op. Parity: the reference's
 * clearBridgePointer on clean exit.
 */
export function clearResumePointer(sessionDir: string): void {
  try { fs.unlinkSync(resumePointerPath(sessionDir)); } catch { /* already gone */ }
}

// ---------------------------------------------------------------------------
// Transcript integrity — JSONL transcript files used by headless / Agent SDK
// sessions.  A crash can truncate the last line; resuming must detect and
// drop the corrupted tail rather than losing the entire file.
// ---------------------------------------------------------------------------

export const TRANSCRIPT_FILE_EXTENSION = '.jsonl';

export function validateTranscriptLine(line: string): boolean {
  if (!line || !line.trim()) return false;
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return false;
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) return false;
  const obj = parsed as Record<string, unknown>;
  if (typeof obj.type !== 'string') return false;
  if (obj.timestamp === undefined) return false;
  if (typeof obj.timestamp !== 'number' && typeof obj.timestamp !== 'string') return false;
  return true;
}

/** Synchronous bounded sleep (no busy-spin) for the append-lock retry loop. */
export function sleepSync(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

/**
 * Acquire a short-lived cross-process advisory lock (O_EXCL sidecar) with
 * bounded retries, reclaiming a stale lock whose holder PID is dead or whose
 * file is older than the stale window. Returns a release() or null if it could
 * not be taken within the retry budget.
 */
export function acquireAppendLock(
  lockPath: string,
  retries = 5,
  minTimeoutMs = 25,
  staleMs = 10_000,
): (() => void) | null {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      fs.writeFileSync(lockPath, String(process.pid), { flag: 'wx', mode: OWNER_ONLY_FILE_MODE });
      return () => { try { fs.unlinkSync(lockPath); } catch { /* already gone */ } };
    } catch (err: any) {
      if (err?.code !== 'EEXIST') return null;
      // Reclaim a stale lock: dead holder PID, or a file older than the window.
      let stale = false;
      try {
        const holder = parseInt(fs.readFileSync(lockPath, 'utf-8').trim(), 10);
        const ageMs = Date.now() - fs.statSync(lockPath).mtimeMs;
        stale = Number.isNaN(holder) || !isPidAlive(holder) || ageMs > staleMs;
      } catch {
        stale = true; // unreadable lock — treat as stale
      }
      if (stale) {
        try { fs.unlinkSync(lockPath); } catch { /* raced away */ }
        continue; // retry the exclusive create immediately
      }
      if (attempt < retries) sleepSync(minTimeoutMs);
    }
  }
  return null;
}

/**
 * Append one JSONL entry to a transcript concurrency-safely: a cross-process
 * advisory lock (so two engine/SDK writers serialize instead of tearing a line)
 * plus an fsync (so a crash can't lose the just-written line), written
 * owner-only. Best-effort — transcript loss is preferable to crashing the
 * session, and if the lock can't be taken we still append (never block a save).
 */
export function safeAppendTranscript(
  filePath: string,
  entry: Record<string, unknown>,
  ownership?: { sessionDir: string; expectedFence: number },
): void {
  // Single-writer fence assert: when the caller holds a fence, reject the append
  // if a newer owner has taken over (fence advanced on the lock). This throws a
  // SessionFenceError OUT of the function — unlike the best-effort transcript I/O
  // below, a superseded write MUST NOT silently land and corrupt the transcript.
  if (ownership) {
    assertSessionFence(ownership.sessionDir, ownership.expectedFence);
  }
  // NDJSON-safe: escape U+2028/U+2029 so an entry can never be split into two lines.
  const line = ndjsonSafeStringify(entry) + '\n';

  // The lock+fsync critical section. Isolated so the retry below can recreate the
  // parent dir and run it a second time without duplicating the body.
  const appendOnce = (): void => {
    const release = acquireAppendLock(filePath + '.lock');
    try {
      // File ops go through the fsHooks DI seam so the recreate-and-retry fallback
      // below is deterministically testable (mock.method(fsHooks,'openSync',…)).
      const fd = fsHooks.openSync(filePath, 'a', OWNER_ONLY_FILE_MODE);
      try {
        fs.writeSync(fd, line);
        fsHooks.fsyncSync(fd);
      } finally {
        fsHooks.closeSync(fd);
      }
    } finally {
      release?.();
    }
  };

  try {
    secureMkdirSync(path.dirname(filePath));
    appendOnce();
  } catch {
    // The append (or the pre-mkdir) failed. Rather than discriminate on an error
    // code — NFS-like filesystems surface unexpected codes for a vanished parent
    // dir — recreate the parent dir and RETRY the append once, so a transient /
    // racing-rmdir failure still lands the entry instead of silently dropping it
    // (parity: the reference's appendToFile mkdir-and-retry). Only if the retry also
    // throws do we give up (transcript loss is preferable to crashing).
    try {
      secureMkdirSync(path.dirname(filePath));
      appendOnce();
    } catch {
      /* best effort — transcript loss is preferable to crashing the session */
    }
  }
}
