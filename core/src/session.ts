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
function truncateMessage(msg: Message): Message {
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

    return this.readSessionFile(files[0]);
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
    const stamp = [
      now.getFullYear(),
      pad(now.getMonth() + 1),
      pad(now.getDate()),
      '-',
      pad(now.getHours()),
      pad(now.getMinutes()),
      pad(now.getSeconds()),
    ].join('');
    return `session-${stamp}`;
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
