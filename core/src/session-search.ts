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

/** Parse an ISO timestamp into epoch ms for recency ranking; unknown/invalid -> 0. */
function recencyMs(timestamp?: string): number {
  if (!timestamp) return 0;
  const parsed = Date.parse(timestamp);
  return Number.isNaN(parsed) ? 0 : parsed;
}

export function searchSessions(cwd: string, query: string, limit: number = 20): SearchResult[] {
  const sessionsDir = path.join(cwd, '.superinference', 'sessions');
  if (!fs.existsSync(sessionsDir)) return [];

  const phrase = query.toLowerCase().trim();
  const terms = phrase.split(/\s+/).filter(Boolean);
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
          // Hoist literal matches (every token verbatim and contiguous) above
          // scattered-term matches of the same coverage.
          const literalBonus = terms.length > 1 && lower.includes(phrase) ? 1 : 0;
          results.push({
            sessionId: session.id ?? file.replace('.json', ''),
            role: msg.role,
            content: content.slice(0, 300),
            score: matchCount / terms.length + literalBonus,
            timestamp: session.createdAt,
          });
        }
      }
    } catch {
      // Skip malformed session files
    }
  }

  // Rank by score, then break ties by recency so newer sessions surface first.
  return results
    .sort((a, b) => b.score - a.score || recencyMs(b.timestamp) - recencyMs(a.timestamp))
    .slice(0, limit);
}
