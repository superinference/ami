// Split from session.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import { execSync as childExecSync } from 'child_process';
import { fsHooks } from './utils/fs-atomic';
import { isEnvTruthy } from './analytics';
import { sha256Hex } from './session-session';
import type { FileAttributionState } from './session-session';

/**
 * Record that the agent authored `contributedChars` characters of `filePath`, whose
 * new full content is `content` (last modified at `mtime`), returning the UPDATED
 * attribution map (input not mutated — pure). The file's `agentContribution`
 * ACCUMULATES across edits; `contentHash`/`mtime` are refreshed to the latest
 * content so a later external edit (different hash) is detectable. A non-positive
 * `contributedChars` still refreshes the hash/mtime without inflating the count
 * (an edit that only deletes text contributes no authored characters). Parity:
 * the reference's per-edit FileAttributionState update.
 */
export function recordFileContribution(
  states: Record<string, FileAttributionState>,
  filePath: string,
  content: string,
  contributedChars: number,
  mtime: number,
): Record<string, FileAttributionState> {
  const next = { ...states };
  applyFileContribution(next, filePath, content, contributedChars, mtime);
  return next;
}

/**
 * MUTATING in-place variant of {@link recordFileContribution}: accumulate the
 * agent's `contributedChars` for `filePath` directly INTO `target` (the file's
 * {@link FileAttributionState} is replaced, the surrounding object is mutated).
 * This is the single canonical accumulation step — {@link recordFileContribution}
 * wraps it with one copy, and a BULK tracker ({@link
 * import('./tools/attribution-store').recordBulkAgentFileContributions}) copies
 * the map ONCE then calls this per file, avoiding the O(n²) blow-up of copying
 * per file across a diff that touches hundreds of thousands of paths. Parity:
 * the reference's `trackBulkFileChanges` single-Map-copy mutation (commitAttribution
 * L489-L542). A non-positive `contributedChars` still refreshes hash/mtime without
 * inflating the running count.
 */
export function applyFileContribution(
  target: Record<string, FileAttributionState>,
  filePath: string,
  content: string,
  contributedChars: number,
  mtime: number,
): void {
  const prev = target[filePath];
  const added = contributedChars > 0 ? contributedChars : 0;
  target[filePath] = {
    contentHash: sha256Hex(content),
    agentContribution: (prev?.agentContribution ?? 0) + added,
    mtime,
  };
}

/**
 * Fraction (0..1) of a file's current size the agent authored, per its
 * {@link FileAttributionState} — `agentContribution / totalChars`, clamped to
 * [0,1] and `0` for an empty/zero-size file. Lets a commit surface report an
 * "N% agent-authored" figure from a snapshot without re-reading the file.
 */
export function agentContributionRatio(state: FileAttributionState, totalChars: number): number {
  if (totalChars <= 0) return 0;
  return Math.min(1, Math.max(0, state.agentContribution / totalChars));
}

// ---------------------------------------------------------------------------
// Session diff / file-change tracking
// ---------------------------------------------------------------------------

export interface FileChange {
  path: string;
  type: 'create' | 'modify' | 'delete' | 'rename';
  timestamp: number;
  toolCallId?: string;
  originalContent?: string;
  size?: number;
}

export class SessionDiffTracker {
  private _changes: FileChange[] = [];
  private _maxChanges: number;
  private _seenPaths = new Set<string>();

  constructor(maxChanges: number = 10_000) {
    this._maxChanges = maxChanges;
  }

  recordChange(change: FileChange): boolean {
    if (this._changes.length >= this._maxChanges) return false;
    this._changes.push(change);
    this._seenPaths.add(change.path);
    return true;
  }

  getChanges(): readonly FileChange[] {
    return this._changes;
  }

  getChangesForPath(filePath: string): FileChange[] {
    return this._changes.filter(c => c.path === filePath);
  }

  getModifiedPaths(): string[] {
    return [...this._seenPaths];
  }

  getChangeCount(): number {
    return this._changes.length;
  }

  getChangesSince(timestamp: number): FileChange[] {
    return this._changes.filter(c => c.timestamp >= timestamp);
  }

  getChangesByType(type: FileChange['type']): FileChange[] {
    return this._changes.filter(c => c.type === type);
  }

  hasChanges(): boolean {
    return this._changes.length > 0;
  }

  getSummary(): { creates: number; modifies: number; deletes: number; renames: number; uniquePaths: number } {
    let creates = 0, modifies = 0, deletes = 0, renames = 0;
    for (const c of this._changes) {
      if (c.type === 'create') creates++;
      else if (c.type === 'modify') modifies++;
      else if (c.type === 'delete') deletes++;
      else if (c.type === 'rename') renames++;
    }
    return { creates, modifies, deletes, renames, uniquePaths: this._seenPaths.size };
  }

  clear(): void {
    this._changes = [];
    this._seenPaths.clear();
  }
}

// ---------------------------------------------------------------------------
// Session lock file — prevents two Engine instances from sharing one session
// directory, which can corrupt state files and analytics.
// ---------------------------------------------------------------------------

export const SESSION_LOCK_FILE = '.session.lock';

/** Upper bound on the linear backoff wait between contended lock-acquire retries,
 *  so a large {@link SessionLockOptions.retries} can never grow a single wait into
 *  an unbounded sleep. */
export const LOCK_RETRY_MAX_DELAY_MS = 1000;

/** Default base delay between contended lock-acquire retries (ms). */
export const LOCK_RETRY_DEFAULT_DELAY_MS = 50;

/**
 * Locking is force-DISABLED when `AMI_DISABLE_SESSION_LOCK` is truthy: every
 * acquire reports success with a no-op release and never writes a `.session.lock`,
 * and every probe reports the dir unlocked. Escape hatch for environments where
 * the cross-process lock is counter-productive — a read-only / single-tenant FS,
 * a networked store whose PID probes are meaningless, or a test harness that wants
 * concurrent engines on one dir (parity: the reference's pidLock force-enable/disable env
 * override, `ENABLE_PID_BASED_VERSION_LOCKING`). Default (unset) = locking ON.
 */
export function isSessionLockingDisabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env.AMI_DISABLE_SESSION_LOCK);
}

/**
 * Tunables for {@link acquireSessionLock} (and the `stale` subset for
 * {@link checkSessionLock}). Mirrors the subset of proper-lockfile's `LockOptions`
 * AMI needs: bounded contended-retry, a per-call staleness override, and a
 * lock-compromised callback.
 */
export interface SessionLockOptions {
  /** Extra attempts to wait out a LIVE holder before giving up. `0` (default)
   *  fails immediately on a live holder — the historical behaviour. Each retry
   *  re-probes the holder, so a lock freed mid-wait is acquired on the next pass
   *  (parity: proper-lockfile `LockOptions.retries`). */
  retries?: number;
  /** Base delay between contended retries (ms); the wait grows linearly with the
   *  retry index (attempt N waits `retryDelayMs * (N+1)`), capped at
   *  {@link LOCK_RETRY_MAX_DELAY_MS} (parity: proper-lockfile retry backoff).
   *  Defaults to {@link LOCK_RETRY_DEFAULT_DELAY_MS}. */
  retryDelayMs?: number;
  /** Override the age past which an unverifiable / cross-host holder is treated as
   *  stale and reclaimable. Defaults to {@link LOCK_FALLBACK_STALE_MS} (parity:
   *  proper-lockfile `LockOptions.stale`). */
  stale?: number;
  /** Invoked if, at release time, the on-disk lock has been taken over by another
   *  process (our lock was judged stale and reclaimed) — i.e. the lock we believed
   *  we held was compromised. Never throws into the caller; a throwing callback is
   *  swallowed (parity: proper-lockfile `LockOptions.onCompromised`). */
  onCompromised?: (reason: string) => void;
}

/** On-disk lock payload. Richer than a bare PID so we can (a) detect PID reuse
 *  by comparing the live process's command against our recorded identity and
 *  (b) apply a time-based stale fallback when the PID check is inconclusive. */
export interface SessionLockPayload {
  pid: number;
  /** Date.now() at acquire — drives the time-based stale fallback. */
  startedAt: number;
  /** process.execPath — identity hint for PID-reuse detection. */
  execPath: string;
  /**
   * Boot-anchored process start time (ms) of the holder, or null when the host
   * could not provide it. Present only on locks written by a PID-reuse-aware
   * build; its presence switches {@link lockHolderActive} to the fail-closed
   * identity probe (see {@link ProcessIdentity}).
   */
  processStartTimeMs?: number | null;
  /** Opaque token the holder minted, for reuse-safe identity matching. */
  spawnToken?: string | null;
  /**
   * Monotonic single-writer fence. Starts at 1 for a brand-new lock and is
   * BUMPED (prior + 1) every time a stale lock is reclaimed, so an in-flight
   * write from a superseded owner can be rejected by comparing the fence it
   * holds against the current on-disk fence ({@link assertSessionFence}).
   */
  runtimeFence?: number;
}

/** Injectable system probes so lock behavior (PID liveness, process identity,
 *  clock) can be driven deterministically in tests without spawning processes. */
export interface SessionLockDeps {
  isPidAlive: (pid: number) => boolean;
  getProcessCommand: (pid: number) => string | null;
  /** Boot-anchored start-time read for the PID-reuse-safe identity probe.
   *  Injectable so the fail-closed takeover logic is testable without `/proc`. */
  readProcessStartTimeMs: (pid: number) => number | null;
  now: () => number;
  /** True when running under WSL — drives the cross-OS PID-probe boundary guard in
   *  {@link lockHolderActive}. Injectable so the guard is testable without a real
   *  WSL kernel. Defaults to {@link isWSL}. */
  isWsl: () => boolean;
}

export function isPidAlive(pid: number): boolean {
  // PID 0 targets the caller's own process group (process.kill(0,0) would
  // spuriously succeed) and PID 1 is init/systemd (always alive, never a
  // legitimate lock holder) — a corrupt/sentinel value must never be read as a
  // live holder, or the lock could never be reclaimed.
  if (pid <= 1) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err: any) {
    if (err?.code === 'ESRCH') return false;
    // EPERM means the process exists but we can't signal it
    if (err?.code === 'EPERM') return true;
    return false;
  }
}

/** Best-effort read of the command line for a live PID — used to detect PID
 *  reuse (a dead holder's PID reassigned to an unrelated program). Returns null
 *  when the command cannot be determined (then the caller trusts the PID check). */
export function getProcessCommand(pid: number): string | null {
  if (pid <= 1) return null;
  try {
    const cmd =
      process.platform === 'win32'
        ? `powershell.exe -NoProfile -Command "(Get-CimInstance Win32_Process -Filter \\"ProcessId=${pid}\\").CommandLine"`
        : `ps -o command= -p ${pid}`;
    const out = childExecSync(cmd, { encoding: 'utf-8', timeout: 1000, stdio: ['ignore', 'pipe', 'ignore'] }).trim();
    return out || null;
  } catch {
    return null;
  }
}

export function readLockPayload(lockPath: string): SessionLockPayload | null {
  let raw: string;
  try {
    raw = fsHooks.readFileSync(lockPath, 'utf-8') as string;
  } catch {
    return null;
  }
  try {
    const parsed = JSON.parse(raw);
    if (
      parsed && typeof parsed === 'object' && !Array.isArray(parsed) &&
      typeof parsed.pid === 'number'
    ) {
      return {
        pid: parsed.pid,
        startedAt: typeof parsed.startedAt === 'number' ? parsed.startedAt : 0,
        execPath: typeof parsed.execPath === 'string' ? parsed.execPath : '',
        // Optional reuse-safe identity: a number|null start time, a string token,
        // and a monotonic fence. Absent on legacy locks (undefined ⇒ identity
        // probe not engaged, see lockHolderActive).
        processStartTimeMs:
          typeof parsed.processStartTimeMs === 'number'
            ? parsed.processStartTimeMs
            : parsed.processStartTimeMs === null
              ? null
              : undefined,
        spawnToken: typeof parsed.spawnToken === 'string' ? parsed.spawnToken : undefined,
        runtimeFence:
          typeof parsed.runtimeFence === 'number' && Number.isFinite(parsed.runtimeFence)
            ? parsed.runtimeFence
            : undefined,
      };
    }
  } catch {
    /* corrupt/legacy bare-pid content — treated as unparseable below */
  }
  return null;
}

/** Next monotonic fence for a reclaim: the reclaimed lock's fence + 1, defaulting
 *  to a fresh 1 when the prior lock recorded no (or an invalid) fence. */
export function nextFenceAfter(priorFence: number | undefined): number {
  return (typeof priorFence === 'number' && Number.isFinite(priorFence) ? priorFence : 0) + 1;
}

/**
 * Build the user-facing lock-conflict message naming the holder PID.
 *
 * When a session is already locked by a live OTHER process, callers must be able
 * to tell the user *which* process holds it so the conflict is actionable
 * (resume the session there, or wait for / terminate that PID) instead of an
 * opaque "could not start" failure. This is the single canonical wording so every
 * surface (acquire result, a `/doctor`-style diagnostic, the resume flow) reads
 * identically. Mirrors the reference's pidLock conflict log — `Cannot acquire lock
 * for <name> - held by PID <pid>` (utils/nativeInstaller/pidLock.ts L248-254) —
 * adapted to a session (the lock "name" is the session directory's basename).
 */
export function formatSessionLockConflict(sessionDir: string, pid: number): string {
  const name = path.basename(sessionDir.replace(/[/\\]+$/, '')) || sessionDir;
  return `Session "${name}" is already in use by another process (PID ${pid}). ` +
    `Resume it in that process, or wait for PID ${pid} to exit before retrying.`;
}

/** Attach the {@link formatSessionLockConflict} message to a blocked acquire
 *  result so the holder PID is surfaced to the user by every caller, not just
 *  silently returned. A successful (or holder-less) result is returned unchanged. */
export function withLockConflictMessage(
  result: { acquired: boolean; fresh?: boolean; pid?: number; runtimeFence?: number; release: () => void },
  sessionDir: string,
): { acquired: boolean; fresh?: boolean; pid?: number; runtimeFence?: number; message?: string; release: () => void } {
  if (!result.acquired && result.pid !== undefined) {
    return { ...result, message: formatSessionLockConflict(sessionDir, result.pid) };
  }
  return result;
}

/** Tri-state result of {@link checkSessionLock} — a check-without-acquire probe:
 *  `free` (no live holder — reclaimable), `held_by_self` (this process owns it), or
 *  `blocked` (another live process owns it). Parity: the reference's computer-use lock
 *  `CheckResult` (`free` / `held_by_self` / `blocked`). */
export type SessionLockCheck =
  | { kind: 'free' }
  | { kind: 'held_by_self'; pid: number }
  | { kind: 'blocked'; pid: number; message: string };

/**
 * Thrown when a write is attempted under a session fence that a newer owner has
 * already superseded — the single-writer invariant was violated and the stale
 * owner's in-flight mutation MUST be rejected rather than corrupt the transcript.
 */
export class SessionFenceError extends Error {
  constructor(
    readonly expectedFence: number,
    readonly currentFence: number,
    sessionDir: string,
  ) {
    super(
      `Session ownership lost: fence ${expectedFence} was superseded by ${currentFence} ` +
        `(another process took over "${path.basename(sessionDir)}"). Write rejected.`,
    );
    this.name = 'SessionFenceError';
  }
}

/**
 * Read the current monotonic single-writer fence recorded in `sessionDir`'s lock,
 * or `null` when there is no lock / no fence on disk (legacy or locking disabled).
 */
export function readSessionFence(sessionDir: string): number | null {
  if (isSessionLockingDisabled()) return null;
  const lockPath = path.join(sessionDir, SESSION_LOCK_FILE);
  if (!fs.existsSync(lockPath)) return null;
  const payload = readLockPayload(lockPath);
  return payload?.runtimeFence ?? null;
}

/**
 * True when `fence` is still the current owner fence for `sessionDir`. A record
 * that carries no fence (legacy / locking disabled) reads as current — there is
 * no newer owner to supersede the caller. A STRICTLY newer on-disk fence means a
 * takeover happened and the caller is stale.
 */
export function isSessionFenceCurrent(sessionDir: string, fence: number): boolean {
  const current = readSessionFence(sessionDir);
  return current === null || current === fence;
}

/**
 * Assert the caller still owns `sessionDir` at `expectedFence`. Throws a
 * {@link SessionFenceError} when a strictly newer fence is on disk (another
 * process reclaimed the lock and advanced the fence) so an in-flight write from a
 * superseded owner is rejected on takeover. A missing/legacy fence is a no-op
 * (nothing proves a takeover). This is the assertion callers (transcript append,
 * checkpoint writes) run before a mutation to enforce single-writer ownership.
 */
export function assertSessionFence(sessionDir: string, expectedFence: number): void {
  const current = readSessionFence(sessionDir);
  if (current !== null && current !== expectedFence) {
    throw new SessionFenceError(expectedFence, current, sessionDir);
  }
}
