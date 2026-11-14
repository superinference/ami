import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { execSync as childExecSync } from 'child_process';
import type { Message, ProviderConfig, SessionId } from './types';
import { asSessionId } from './types';
import { projectStateDir, ndjsonSafeStringify } from './reference-journal';
import { assertPathWithin } from './path-safety';
import { atomicWriteFileSync, secureMkdirSync, fsHooks, OWNER_ONLY_FILE_MODE, OWNER_ONLY_DIR_MODE } from './utils/fs-atomic';
import { getGitBranch } from './utils/git';
import { AMI_VERSION } from './version';
import { isEnvTruthy } from './analytics';
export { assertPathWithin } from './path-safety';

/**
 * Upper bound on a session/transcript file we will read back into memory. A
 * corrupt or maliciously huge file must never OOM the process on --resume; a file
 * over this size is treated as unreadable (skipped) rather than loaded. Mirrors
 * the reference's size boundary on session reads.
 */
export const MAX_SESSION_READ_BYTES = 100 * 1024 * 1024; // 100 MiB

/**
 * Session/transcript persistence is disabled when the user opts out: the
 * reference honours CLAUDE_CODE_SKIP_PROMPT_HISTORY; we add AMI_SKIP_SESSION_HISTORY.
 * When disabled, save() is a no-op so no transcript is written to disk (privacy /
 * ephemeral-run support). Callers (CLI flag, cleanupPeriodDays===0) can also pass
 * an explicit override.
 */
export function isSessionPersistenceDisabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env.AMI_SKIP_SESSION_HISTORY) || isEnvTruthy(env.CLAUDE_CODE_SKIP_PROMPT_HISTORY);
}

/**
 * Content longer than this is offloaded to a content-addressed blob sidecar
 * rather than truncated in place. The value also caps the inline on-screen
 * preview stored next to the blob reference.
 */
const MAX_CONTENT_LENGTH = 5000;

/**
 * On-disk reference that replaces a large message content string. The full
 * content lives in `<sessionDir>/blobs/<sessionId>/<sha256>.blob`; only a short
 * preview is kept inline so `list()` and corrupt-blob fallbacks stay cheap.
 * Rehydrated back to the full string on load — so --resume no longer loses
 * tool output or long user messages (H14).
 */
interface SessionBlobRef {
  __siSessionBlob: string; // sha256 of the full UTF-8 content
  preview: string;
  length: number;
}

function isSessionBlobRef(v: unknown): v is SessionBlobRef {
  return (
    !!v &&
    typeof v === 'object' &&
    !Array.isArray(v) &&
    typeof (v as { __siSessionBlob?: unknown }).__siSessionBlob === 'string'
  );
}

export interface Session {
  id: SessionId;
  /** A user-set rename. Always wins over {@link aiTitle} and is never
   *  overwritten by auto-title (re)generation (preserved across save()). */
  customTitle?: string;
  /** Auto-generated, regeneratable title. May be overwritten by a newer AI
   *  title but never clobbers a {@link customTitle}. */
  aiTitle?: string;
  messages: Message[];
  createdAt: string;
  updatedAt: string;
  provider: { model: string; baseUrl: string };
  /** Git branch the session was last saved on — lets /resume show and filter by
   *  branch. Populated on save() from the SessionManager's cwd. */
  gitBranch?: string;
  /** The originating project directory (cwd), so a listed/resumed session can
   *  report which directory it belongs to. Populated on save(). */
  projectPath?: string;
  /** App/schema version that wrote this session, so resume/migration can detect
   *  and adapt to sessions written by an older build. Stamped on save(). */
  version?: string;
  /** Optional user-set label, searchable in /resume + /search. */
  tag?: string;
  /** Per-session settings saved on disk so /resume can restore them. */
  settings?: {
    skill?: string;
    thinkingLevel?: string;
    outputStyle?: string;
    goal?: string;
  };
}

/** The display title for a session: a user rename wins over the AI title. */
export function resolveSessionTitle(session: {
  customTitle?: string;
  aiTitle?: string;
}): string | undefined {
  return session.customTitle ?? session.aiTitle;
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
}

/** Compaction summaries are the ONLY record of the pre-compaction conversation.
 *  They are kept inline in full (never offloaded) so a missing blob can never
 *  cost us the summarized history on --resume. */
const COMPACTION_SUMMARY_PREFIX = '[Previous conversation summary]';

export function safeCharSlice(s: string, start: number, end?: number): string {
  let result = s.slice(start, end);
  if (result.length === 0) return result;
  const lastCode = result.charCodeAt(result.length - 1);
  if (lastCode >= 0xD800 && lastCode <= 0xDBFF) result = result.slice(0, -1);
  const firstCode = result.charCodeAt(0);
  if (firstCode >= 0xDC00 && firstCode <= 0xDFFF) result = result.slice(1);
  return result;
}

function sanitizeSessionId(id: string): string {
  return id.replace(/[\/\\]/g, '_').replace(/\.\./g, '_');
}

export class SessionManager {
  private sessionDir: string;
  private cwd: string;
  private _gitBranch?: string;
  private _gitBranchResolved = false;
  private static _idCounter = 0;

  constructor(cwd: string, sessionDirOverride?: string) {
    // Resume blobs live GLOBALLY under ~/.ami/projects/<cwd-slug>/sessions (like Claude Code's
    // ~/.claude/projects/<slug>/), not inside the repo — keyed by cwd slug.
    this.cwd = cwd;
    this.sessionDir = sessionDirOverride ?? projectStateDir(cwd, 'sessions');
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
   * Save a session to disk as a JSON file.
   * Message content longer than 5000 chars is offloaded to a content-addressed
   * blob sidecar (full content preserved) with only a preview kept inline, so
   * --resume restores the complete conversation (H14).
   */
  save(session: Session): void {
    // Honour an opt-out of transcript persistence (privacy / ephemeral runs):
    // write nothing to disk when disabled. Checked first so no dir is created.
    if (isSessionPersistenceDisabled()) return;
    this.ensureDir();
    const safeId = sanitizeSessionId(session.id);
    const blobDir = this.blobDirFor(safeId);
    const storedMessages = session.messages.map(m => this.offloadMessage(m, blobDir));
    const filePath = path.join(this.sessionDir, `${safeId}.json`);
    assertPathWithin(filePath, this.sessionDir);
    // A user rename (customTitle) must never be clobbered by an auto-title save:
    // if this save doesn't carry one, preserve whatever is already on disk.
    const customTitle = session.customTitle ?? this.readSessionFile(filePath, false)?.customTitle;
    const data = {
      ...session,
      id: safeId,
      customTitle,
      messages: storedMessages,
      updatedAt: new Date().toISOString(),
      // Stamp provenance so /resume can show branch/project and migrations can
      // detect old schemas. Caller-supplied values win; otherwise derive from cwd.
      gitBranch: session.gitBranch ?? this.gitBranch(),
      projectPath: session.projectPath ?? this.cwd,
      version: session.version ?? AMI_VERSION,
    };
    // Crash-safe + owner-only: a truncated/corrupt newest session can never
    // result from an interrupted write, and transcripts stay 0o600 on shared hosts.
    // NDJSON-safe serialization escapes U+2028/U+2029 so content can't tear a line.
    atomicWriteFileSync(filePath, ndjsonSafeStringify(data, 2));
  }

  /** Directory holding a session's content-addressed blobs. */
  private blobDirFor(safeId: string): string {
    return path.join(this.sessionDir, 'blobs', safeId);
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
        const sha = crypto.createHash('sha256').update(full, 'utf-8').digest('hex');
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
    if (isSessionBlobRef(stored)) {
      let content: string;
      try {
        content = fs.readFileSync(path.join(blobDir, `${stored.__siSessionBlob}.blob`), 'utf-8');
      } catch {
        content = stored.preview + '\n... (truncated; blob unavailable)';
      }
      return { ...msg, content: content as unknown as string };
    }
    return msg;
  }

  /**
   * Load the most recently modified session file.
   */
  loadLatest(): Session | null {
    const files = this.listFiles();
    if (files.length === 0) return null;

    // Sort by modification time descending
    files.sort((a, b) => {
      try {
        return fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs;
      } catch {
        return 0;
      }
    });

    // Return the first file that parses successfully, falling through past any
    // corrupt/truncated newest file (e.g. a save() interrupted mid-write) so
    // intact older sessions are not hidden by a single unreadable file.
    for (const file of files) {
      const session = this.readSessionFile(file);
      if (session) return session;
    }
    return null;
  }

  /**
   * Load a specific session by ID.
   */
  load(id: string): Session | null {
    const safeId = sanitizeSessionId(id);
    const filePath = path.join(this.sessionDir, `${safeId}.json`);
    assertPathWithin(filePath, this.sessionDir);
    if (!fs.existsSync(filePath)) return null;
    return this.readSessionFile(filePath);
  }

  /**
   * List all sessions with metadata (id, date, first message preview).
   */
  list(): SessionListEntry[] {
    const files = this.listFiles();
    const entries: SessionListEntry[] = [];

    for (const filePath of files) {
      // Preview only needs inline data — skip blob rehydration for speed.
      const session = this.readSessionFile(filePath, false);
      if (!session) continue;

      const preview = this.extractPreview(session);
      // messageCount / fileSize / gitBranch are all available at ~zero extra
      // cost from the already-parsed inline JSON (no blob rehydration needed).
      let fileSize: number | undefined;
      try { fileSize = fs.statSync(filePath).size; } catch { /* stat race — omit */ }
      entries.push({
        id: session.id,
        date: session.updatedAt || session.createdAt,
        preview,
        messageCount: Array.isArray(session.messages) ? session.messages.length : 0,
        fileSize,
        gitBranch: session.gitBranch,
        title: resolveSessionTitle(session),
      });
    }

    // Sort by date descending (most recent first)
    entries.sort((a, b) => {
      const bTime = b.date ? new Date(b.date).getTime() : 0;
      const aTime = a.date ? new Date(a.date).getTime() : 0;
      return (isNaN(bTime) ? 0 : bTime) - (isNaN(aTime) ? 0 : aTime);
    });
    return entries;
  }

  /**
   * Delete a session file by ID.
   */
  delete(id: string): void {
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
   * Return the session directory path (useful for testing).
   */
  getSessionDir(): string {
    return this.sessionDir;
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  private listFiles(): string[] {
    if (!fs.existsSync(this.sessionDir)) return [];
    const entries = fs.readdirSync(this.sessionDir);
    return entries
      .filter(e => e.endsWith('.json'))
      .map(e => path.join(this.sessionDir, e));
  }

  private readSessionFile(filePath: string, rehydrate = true): Session | null {
    try {
      // OOM boundary: never read a pathologically large session file into memory
      // (a corrupt/hostile file must not crash --resume). Skip it as unreadable.
      if (fs.statSync(filePath).size > MAX_SESSION_READ_BYTES) return null;
      const raw = fs.readFileSync(filePath, 'utf-8');
      const parsed = JSON.parse(raw) as Session;
      if (rehydrate && Array.isArray(parsed.messages)) {
        const safeId = path.basename(filePath).replace(/\.json$/, '');
        const blobDir = this.blobDirFor(safeId);
        parsed.messages = parsed.messages.map(m => this.rehydrateMessage(m, blobDir));
      }
      return parsed;
    } catch {
      return null;
    }
  }

  private extractPreview(session: Session): string {
    const firstUser = session.messages.find(m => m.role === 'user');
    if (!firstUser) return '(no messages)';
    // An over-long first message is stored as a blob ref — use its preview.
    const raw = (firstUser as { content?: unknown }).content;
    const text = isSessionBlobRef(raw) ? raw.preview : raw;
    if (typeof text !== 'string') return '(no messages)';
    const flat = text.replace(/\n/g, ' ').trim();
    if (!flat) return '(no messages)';
    return flat.length > 100 ? flat.slice(0, 100) + '...' : flat;
  }
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

/** A lock older than this whose holder identity cannot be verified (e.g. a PID
 *  from another host on an NFS/SMB-mounted session dir) is treated as stale, so
 *  cross-host locks self-heal instead of wedging --resume forever. */
export const LOCK_FALLBACK_STALE_MS = 2 * 60 * 60 * 1000; // 2h

/** On-disk lock payload. Richer than a bare PID so we can (a) detect PID reuse
 *  by comparing the live process's command against our recorded identity and
 *  (b) apply a time-based stale fallback when the PID check is inconclusive. */
export interface SessionLockPayload {
  pid: number;
  /** Date.now() at acquire — drives the time-based stale fallback. */
  startedAt: number;
  /** process.execPath — identity hint for PID-reuse detection. */
  execPath: string;
}

/** Injectable system probes so lock behavior (PID liveness, process identity,
 *  clock) can be driven deterministically in tests without spawning processes. */
export interface SessionLockDeps {
  isPidAlive: (pid: number) => boolean;
  getProcessCommand: (pid: number) => string | null;
  now: () => number;
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

const DEFAULT_LOCK_DEPS: SessionLockDeps = {
  isPidAlive,
  getProcessCommand,
  now: Date.now,
};

function readLockPayload(lockPath: string): SessionLockPayload | null {
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
      };
    }
  } catch {
    /* corrupt/legacy bare-pid content — treated as unparseable below */
  }
  return null;
}

/** Is the lock currently held by a live process that is really one of ours?
 *  A dead PID, a sentinel PID, a reused PID now running an unrelated program, or
 *  an old lock we can't verify (NFS/cross-host) are all treated as NOT held. */
function lockHolderActive(
  payload: SessionLockPayload,
  deps: SessionLockDeps,
): boolean {
  const { pid } = payload;
  if (!deps.isPidAlive(pid)) return false;       // dead / sentinel → stale
  if (pid === process.pid) return true;          // it's us → held
  const command = deps.getProcessCommand(pid);
  if (command === null) {
    // Can't verify identity — trust the PID check unless the lock is very old
    // (self-heal cross-host / NFS locks whose PID is meaningless locally).
    const age = deps.now() - payload.startedAt;
    return age <= LOCK_FALLBACK_STALE_MS;
  }
  // Verify the live process looks like ours; otherwise the PID was reused.
  const c = command.toLowerCase();
  const execHint = payload.execPath ? path.basename(payload.execPath).toLowerCase() : '';
  return c.includes('ami') || (!!execHint && c.includes(execHint));
}

function makeLockPayload(deps: SessionLockDeps): string {
  const payload: SessionLockPayload = {
    pid: process.pid,
    startedAt: deps.now(),
    execPath: process.execPath,
  };
  return JSON.stringify(payload);
}

// Release handlers for locks held by THIS process, run on graceful exit so a
// lock is freed even if the caller never invokes release() (signal / crash /
// forgotten cleanup). Mirrors Claude's registerCleanup-at-acquire.
const activeLockReleases = new Set<() => void>();
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

function makeAcquiredHandle(lockPath: string): { acquired: true; release: () => void } {
  let released = false;
  const release = (): void => {
    if (released) return;
    released = true;
    activeLockReleases.delete(release);
    try { fsHooks.unlinkSync(lockPath); } catch { /* already gone */ }
  };
  activeLockReleases.add(release);
  installExitHook();
  return { acquired: true, release };
}

export function acquireSessionLock(
  sessionDir: string,
  depsOverride?: Partial<SessionLockDeps>,
): { acquired: boolean; pid?: number; release: () => void } {
  const deps: SessionLockDeps = { ...DEFAULT_LOCK_DEPS, ...depsOverride };
  const lockPath = path.join(sessionDir, SESSION_LOCK_FILE);
  // Create the dir owner-only when absent; do NOT chmod an existing dir here so
  // a deliberately read-only session dir still surfaces the write EACCES (the
  // save() path tightens pre-existing dir perms via secureMkdirSync instead).
  if (!fs.existsSync(sessionDir)) {
    fs.mkdirSync(sessionDir, { recursive: true, mode: OWNER_ONLY_DIR_MODE });
  }

  // Bounded: at most one stale-reclaim retry (replaces unbounded self-recursion
  // that a racing writer recreating the lock could blow the stack with).
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      fsHooks.writeFileSync(lockPath, makeLockPayload(deps), { flag: 'wx', mode: OWNER_ONLY_FILE_MODE });
    } catch (err: any) {
      if (err?.code !== 'EEXIST') throw err;
      const payload = readLockPayload(lockPath);
      if (payload && lockHolderActive(payload, deps)) {
        return { acquired: false, pid: payload.pid, release: () => {} };
      }
      // Stale / corrupt / unreadable → reclaim and retry the exclusive create.
      try { fsHooks.unlinkSync(lockPath); } catch { /* raced away */ }
      continue;
    }
    // Verify we actually won the create (a racer may have recreated the lock
    // between our unlink and this wx-write, which would have thrown EEXIST; if
    // instead both reclaimed and one overwrote, re-read confirms ownership).
    const verify = readLockPayload(lockPath);
    if (verify?.pid !== process.pid) {
      return { acquired: false, pid: verify?.pid, release: () => {} };
    }
    return makeAcquiredHandle(lockPath);
  }

  // Lost the recovery race after the bounded retry.
  const payload = readLockPayload(lockPath);
  return { acquired: false, pid: payload?.pid, release: () => {} };
}

export function isSessionLocked(
  sessionDir: string,
  depsOverride?: Partial<SessionLockDeps>,
): boolean {
  const deps: SessionLockDeps = { ...DEFAULT_LOCK_DEPS, ...depsOverride };
  const lockPath = path.join(sessionDir, SESSION_LOCK_FILE);
  if (!fs.existsSync(lockPath)) return false;
  const payload = readLockPayload(lockPath);
  if (!payload) return false;
  return lockHolderActive(payload, deps);
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

/** Synchronous bounded sleep (no busy-spin) for the append-lock retry loop. */
function sleepSync(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

/**
 * Acquire a short-lived cross-process advisory lock (O_EXCL sidecar) with
 * bounded retries, reclaiming a stale lock whose holder PID is dead or whose
 * file is older than the stale window. Returns a release() or null if it could
 * not be taken within the retry budget.
 */
function acquireAppendLock(
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
): void {
  try {
    secureMkdirSync(path.dirname(filePath));
    // NDJSON-safe: escape U+2028/U+2029 so an entry can never be split into two lines.
    const line = ndjsonSafeStringify(entry) + '\n';
    const release = acquireAppendLock(filePath + '.lock');
    try {
      const fd = fs.openSync(filePath, 'a', OWNER_ONLY_FILE_MODE);
      try {
        fs.writeSync(fd, line);
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
    } finally {
      release?.();
    }
  } catch {
    /* best effort — transcript loss is preferable to crashing the session */
  }
}
