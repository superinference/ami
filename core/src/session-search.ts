import * as fs from 'fs';
import * as path from 'path';

/** @public Search result returned by session search for library consumers. */
export interface SearchResult {
  sessionId: string;
  role: string;
  content: string;
  score: number;
  timestamp?: string;
}

export function searchSessions(cwd: string, query: string, limit: number = 20): SearchResult[] {
  const sessionsDir = path.join(cwd, '.superinference', 'sessions');
  if (!fs.existsSync(sessionsDir)) return [];

  const terms = query.toLowerCase().split(/\s+/).filter(Boolean);
  if (terms.length === 0) return [];

  const results: SearchResult[] = [];

  for (const file of fs.readdirSync(sessionsDir)) {
    if (!file.endsWith('.json')) continue;
    try {
      const raw = fs.readFileSync(path.join(sessionsDir, file), 'utf-8');
      const session = JSON.parse(raw);
      for (const msg of session.messages ?? []) {
        const content = typeof msg.content === 'string' ? msg.content : '';
        if (!content) continue;
        const lower = content.toLowerCase();
        const matchCount = terms.filter(t => lower.includes(t)).length;
        if (matchCount > 0) {
          results.push({
            sessionId: session.id ?? file.replace('.json', ''),
            role: msg.role,
            content: content.slice(0, 300),
            score: matchCount / terms.length,
            timestamp: session.createdAt,
          });
        }
      }
    } catch {
      // Skip malformed session files
    }
  }

  return results.sort((a, b) => b.score - a.score).slice(0, limit);
}
