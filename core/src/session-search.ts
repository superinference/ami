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
  const sessionsDir = path.join(cwd, '.ami', 'sessions');
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

/**
 * @public Optional re-ranker injected by the caller. Given the query and the
 * substring candidates, it returns the candidate `sessionId`s in the desired
 * order (best first). Core NEVER performs a provider/LLM call itself — a real
 * LLM ranker is supplied by the consumer so this module stays dependency-free
 * and testable.
 */
export type SessionRankFn = (query: string, candidates: SearchResult[]) => Promise<string[]>;

/** @public Options for {@link searchSessionsRanked}. */
export interface RankedSearchOptions {
  limit?: number;
  /** When omitted, no reranking happens: result equals {@link searchSessions} (zero network). */
  rankFn?: SessionRankFn;
}

/**
 * Reorder `candidates` to follow the sequence of `orderedIds` (by `sessionId`).
 * Candidates sharing a `sessionId` keep their original relative order; any
 * candidate whose `sessionId` is absent from `orderedIds` is appended at the end
 * in its original order. Unknown ids in `orderedIds` are ignored. This makes the
 * reorder robust to partial / duplicate / unknown ranker output.
 */
function reorderByIds(candidates: SearchResult[], orderedIds: string[]): SearchResult[] {
  const used = new Set<number>();
  const ranked: SearchResult[] = [];
  for (const id of orderedIds) {
    for (let i = 0; i < candidates.length; i++) {
      if (!used.has(i) && candidates[i].sessionId === id) {
        used.add(i);
        ranked.push(candidates[i]);
      }
    }
  }
  for (let i = 0; i < candidates.length; i++) {
    if (!used.has(i)) ranked.push(candidates[i]);
  }
  return ranked;
}

/**
 * @public Optional LLM-ranking tier over {@link searchSessions}.
 *
 * Runs the fast, zero-cost literal substring search first (unchanged default
 * behavior). Only when a `rankFn` is supplied does it re-order the candidates
 * per the injected ranker. With no `rankFn` the result is byte-for-byte the same
 * as `searchSessions` — zero behavior change and zero network. A `rankFn` that
 * rejects or throws degrades gracefully to the substring order.
 */
export async function searchSessionsRanked(
  cwd: string,
  query: string,
  options: RankedSearchOptions = {},
): Promise<SearchResult[]> {
  const { limit = 20, rankFn } = options;
  const candidates = searchSessions(cwd, query, limit);
  if (!rankFn || candidates.length === 0) return candidates;
  try {
    const orderedIds = await rankFn(query, candidates);
    return reorderByIds(candidates, orderedIds);
  } catch {
    // Ranker failure must never break search: fall back to substring order.
    return candidates;
  }
}
