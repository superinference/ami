import * as fs from 'fs';
import * as path from 'path';
import type { Message, ProviderConfig } from './types';

const MAX_CONTENT_LENGTH = 5000;

export interface Session {
  id: string;
  title?: string;
  messages: Message[];
  createdAt: string;
  updatedAt: string;
  provider: { model: string; baseUrl: string };
}

export interface SessionListEntry {
  id: string;
  date: string;
  preview: string;
}

/**
 * Truncate message content to avoid bloated session files.
 * Tool outputs and long user messages are capped at MAX_CONTENT_LENGTH chars.
 */
/** Compaction summaries are the ONLY record of the pre-compaction conversation.
 *  Truncating them loses history on --resume, so they are exempt from the cap. */
const COMPACTION_SUMMARY_PREFIX = '[Previous conversation summary]';

function truncateMessage(msg: Message): Message {
  if (msg.role === 'system' && typeof msg.content === 'string' &&
      msg.content.startsWith(COMPACTION_SUMMARY_PREFIX)) {
    return msg;
  }
  if (msg.role === 'user' || msg.role === 'system') {
    if (typeof msg.content === 'string' && msg.content.length > MAX_CONTENT_LENGTH) {
      return { ...msg, content: msg.content.slice(0, MAX_CONTENT_LENGTH) + '\n... (truncated)' };
    }
  }
  if (msg.role === 'tool') {
    if (typeof msg.content === 'string' && msg.content.length > MAX_CONTENT_LENGTH) {
      return { ...msg, content: msg.content.slice(0, MAX_CONTENT_LENGTH) + '\n... (truncated)' };
    }
  }
  return msg;
}

function sanitizeSessionId(id: string): string {
  return id.replace(/[\/\\]/g, '_').replace(/\.\./g, '_');
}

function assertPathWithin(filePath: string, baseDir: string): void {
  const resolved = path.resolve(filePath);
  const resolvedBase = path.resolve(baseDir);
  if (!resolved.startsWith(resolvedBase + path.sep) && resolved !== resolvedBase) {
    throw new Error(`Path traversal blocked: ${filePath} escapes ${baseDir}`);
  }
}

export class SessionManager {
  private sessionDir: string;
  private static _idCounter = 0;

  constructor(cwd: string, sessionDirOverride?: string) {
    this.sessionDir = sessionDirOverride ?? path.join(cwd, '.superinference', 'sessions');
  }

  /**
   * Ensure the session directory exists.
   */
  private ensureDir(): void {
    if (!fs.existsSync(this.sessionDir)) {
      fs.mkdirSync(this.sessionDir, { recursive: true });
    }
  }

  /**
   * Save a session to disk as a JSON file.
   * Message content longer than 5000 chars is truncated.
   */
  save(session: Session): void {
    this.ensureDir();
    const safeId = sanitizeSessionId(session.id);
    const truncatedMessages = session.messages.map(truncateMessage);
    const data: Session = {
      ...session,
      id: safeId,
      messages: truncatedMessages,
      updatedAt: new Date().toISOString(),
    };
    const filePath = path.join(this.sessionDir, `${safeId}.json`);
    assertPathWithin(filePath, this.sessionDir);
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf-8');
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
      const session = this.readSessionFile(filePath);
      if (!session) continue;

      const preview = this.extractPreview(session);
      entries.push({
        id: session.id,
        date: session.updatedAt || session.createdAt,
        preview,
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
  }

  /**
   * Generate a timestamp-based session ID.
   */
  static newId(): string {
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
    return `session-${stamp}-${seq}`;
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

  private readSessionFile(filePath: string): Session | null {
    try {
      const raw = fs.readFileSync(filePath, 'utf-8');
      return JSON.parse(raw) as Session;
    } catch {
      return null;
    }
  }

  private extractPreview(session: Session): string {
    const firstUser = session.messages.find(m => m.role === 'user');
    if (!firstUser || typeof firstUser.content !== 'string') return '(no messages)';
    const flat = firstUser.content.replace(/\n/g, ' ').trim();
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

export function isPidAlive(pid: number): boolean {
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

export function acquireSessionLock(
  sessionDir: string,
): { acquired: boolean; pid?: number; release: () => void } {
  const lockPath = path.join(sessionDir, SESSION_LOCK_FILE);

  if (!fs.existsSync(sessionDir)) {
    fs.mkdirSync(sessionDir, { recursive: true });
  }

  try {
    fs.writeFileSync(lockPath, String(process.pid), { flag: 'wx' });
    return {
      acquired: true,
      release: () => {
        try { fs.unlinkSync(lockPath); } catch {}
      },
    };
  } catch (err: any) {
    if (err?.code !== 'EEXIST') throw err;

    // Lock file exists — check if holder is still alive
    let existingPid: number;
    try {
      existingPid = parseInt(fs.readFileSync(lockPath, 'utf-8').trim(), 10);
    } catch {
      // Can't read the lock file — remove and retry
      try { fs.unlinkSync(lockPath); } catch {}
      return acquireSessionLock(sessionDir);
    }

    if (Number.isNaN(existingPid) || !isPidAlive(existingPid)) {
      // Stale lock — reclaim
      try { fs.unlinkSync(lockPath); } catch {}
      fs.writeFileSync(lockPath, String(process.pid), { flag: 'wx' });
      return {
        acquired: true,
        release: () => {
          try { fs.unlinkSync(lockPath); } catch {}
        },
      };
    }

    return { acquired: false, pid: existingPid, release: () => {} };
  }
}

export function isSessionLocked(sessionDir: string): boolean {
  const lockPath = path.join(sessionDir, SESSION_LOCK_FILE);
  if (!fs.existsSync(lockPath)) return false;

  try {
    const pid = parseInt(fs.readFileSync(lockPath, 'utf-8').trim(), 10);
    if (Number.isNaN(pid)) return false;
    return isPidAlive(pid);
  } catch {
    return false;
  }
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
    fs.writeFileSync(filePath, valid.map(l => l + '\n').join(''));
  }

  return { repairedLines: valid.length, droppedLines };
}

export function safeAppendTranscript(
  filePath: string,
  entry: Record<string, unknown>,
): void {
  try {
    const dir = path.dirname(filePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    fs.appendFileSync(filePath, JSON.stringify(entry) + '\n');
  } catch {
    /* best effort — transcript loss is preferable to crashing the session */
  }
}
