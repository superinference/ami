import * as fs from 'fs';
import * as path from 'path';
import { projectStateDir } from './reference-journal';

/** @public Search result returned by session search for library consumers. */
export interface SearchResult {
  sessionId: string;
  role: string;
  content: string;
  score: number;
  timestamp?: string;
}

/**
 * Scan/read budgets — a session directory can grow unbounded, and a single
 * corrupt/huge session JSON could OOM the process. These caps keep search bounded
 * in both file count and per-file memory (mirrors the reference's search budgets).
 */
export const MAX_SESSION_SEARCH_BYTES = 25 * 1024 * 1024; // skip a session file larger than this
export const MAX_SESSIONS_TO_SEARCH = 2000;               // newest-first cap on files scanned
export const MAX_MESSAGES_TO_SCAN = 5000;                 // per-session message cap
export const MAX_SNIPPET_CHARS = 300;                     // result snippet budget (incl. ellipses)

/** Model-only context (system-reminder spans) must not produce phantom search
 *  hits — strip the whole block, open tag to close tag, before matching. */
const SYSTEM_REMINDER_RE = /<system-reminder\b[^>]*>[\s\S]*?<\/system-reminder>/gi;

/** Parse an ISO timestamp into epoch ms for recency ranking; unknown/invalid -> 0. */
function recencyMs(timestamp?: string): number {
  if (!timestamp) return 0;
  const parsed = Date.parse(timestamp);
  return Number.isNaN(parsed) ? 0 : parsed;
}

/**
 * Flatten a message's content into one searchable string. A message's `content`
 * may be a plain string OR a structured block array (text / tool_use with an
 * `input` object / tool_result with nested `content`); the assistant form also
 * carries `tool_calls`. A string-only match would miss everything a tool call or
 * tool result said — so recurse structured content, stringify tool inputs, and
 * fold in tool-call names + arguments. (H33)
 */
export function flattenMessageText(msg: unknown): string {
  if (!msg || typeof msg !== 'object') return '';
  const m = msg as Record<string, unknown>;
  const parts: string[] = [];
  parts.push(flattenContent(m.content));
  // Assistant tool calls — the OpenAI-style shape used across the engine.
  const toolCalls = m.tool_calls;
  if (Array.isArray(toolCalls)) {
    for (const tc of toolCalls) {
      const fn = tc && typeof tc === 'object' ? (tc as Record<string, unknown>).function : undefined;
      if (fn && typeof fn === 'object') {
        const f = fn as Record<string, unknown>;
        if (typeof f.name === 'string') parts.push(f.name);
        if (typeof f.arguments === 'string') parts.push(f.arguments);
      }
    }
  }
  // Drop model-only system-reminder spans so they can't generate phantom matches.
  return parts.filter(Boolean).join(' ').replace(SYSTEM_REMINDER_RE, ' ');
}

/**
 * Build a result snippet centered on the earliest match (phrase first, else any
 * term), with leading/trailing ellipsis when the surrounding text is clipped. The
 * total length never exceeds {@link MAX_SNIPPET_CHARS} (ellipses included). Text
 * that already fits is returned verbatim. (match-centered snippet + truncation
 * indicator)
 */
export function matchSnippet(text: string, terms: string[], phrase: string, max = MAX_SNIPPET_CHARS): string {
  if (text.length <= max) return text;
  const lower = text.toLowerCase();
  let idx = phrase ? lower.indexOf(phrase) : -1;
  if (idx === -1) {
    for (const t of terms) {
      const i = lower.indexOf(t);
      if (i !== -1 && (idx === -1 || i < idx)) idx = i;
    }
  }
  if (idx === -1) return text.slice(0, max);
  const half = Math.floor(max / 2);
  let end = Math.min(text.length, Math.max(0, idx - half) + max);
  const start = Math.max(0, end - max);
  end = Math.min(text.length, start + max);
  const prefix = start > 0 ? '…' : '';
  const suffix = end < text.length ? '…' : '';
  const room = max - prefix.length - suffix.length;
  let snip = text.slice(start, end);
  if (snip.length > room) snip = snip.slice(0, room);
  return prefix + snip + suffix;
}

/** Recursively flatten string | block-array content into searchable text. */
function flattenContent(content: unknown): string {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  const out: string[] = [];
  for (const block of content) {
    if (typeof block === 'string') { out.push(block); continue; }
    if (!block || typeof block !== 'object') continue;
    const b = block as Record<string, unknown>;
    if (typeof b.text === 'string') out.push(b.text);
    if (typeof b.thinking === 'string') out.push(b.thinking);
    // tool_use carries a structured `input`; tool_result carries nested content.
    if (b.input !== undefined) {
      try { out.push(typeof b.input === 'string' ? b.input : JSON.stringify(b.input)); } catch { /* unserializable */ }
    }
    if (b.content !== undefined) out.push(flattenContent(b.content));
    if (typeof b.name === 'string') out.push(b.name);
  }
  return out.join(' ');
}

export function searchSessions(cwd: string, query: string, limit: number = 20): SearchResult[] {
  const sessionsDir = projectStateDir(cwd, 'sessions');
  if (!fs.existsSync(sessionsDir)) return [];

  const phrase = query.toLowerCase().trim();
  const terms = phrase.split(/\s+/).filter(Boolean);
  if (terms.length === 0) return [];

  const results: SearchResult[] = [];

  // Newest-first, capped file set: a huge session dir must not make search
  // unbounded. Sort by mtime desc, take at most MAX_SESSIONS_TO_SEARCH.
  const candidates: { file: string; mtimeMs: number }[] = [];
  for (const file of fs.readdirSync(sessionsDir)) {
    if (!file.endsWith('.json')) continue;
    let mtimeMs = 0;
    try { mtimeMs = fs.statSync(path.join(sessionsDir, file)).mtimeMs; } catch { /* stat race */ }
    candidates.push({ file, mtimeMs });
  }
  candidates.sort((a, b) => b.mtimeMs - a.mtimeMs);

  for (const { file } of candidates.slice(0, MAX_SESSIONS_TO_SEARCH)) {
    try {
      const filePath = path.join(sessionsDir, file);
      // OOM guard: skip a session file larger than the read cap rather than
      // slurping it into memory.
      if (fs.statSync(filePath).size > MAX_SESSION_SEARCH_BYTES) continue;
      const raw = fs.readFileSync(filePath, 'utf-8');
      const session = JSON.parse(raw);
      const sessionId = session.id ?? file.replace('.json', '');
      // Session-level metadata (user tag + title + git branch) is searchable in
      // /resume, so a labeled session — or one on a named branch — surfaces even
      // when no message body matches the query. (H13)
      const metaText = [session.tag, session.customTitle, session.aiTitle, session.gitBranch]
        .filter((v: unknown): v is string => typeof v === 'string' && v.length > 0)
        .join(' ');
      if (metaText) {
        const lowerMeta = metaText.toLowerCase();
        const metaMatch = terms.filter(t => lowerMeta.includes(t)).length;
        if (metaMatch > 0) {
          const literalBonus = terms.length > 1 && lowerMeta.includes(phrase) ? 1 : 0;
          results.push({
            sessionId,
            role: 'meta',
            content: matchSnippet(metaText, terms, phrase),
            // Hoist a tag/title hit above scattered body matches of equal coverage.
            score: metaMatch / terms.length + literalBonus + 0.5,
            timestamp: session.createdAt,
          });
        }
      }
      const messages = Array.isArray(session.messages) ? session.messages : [];
      for (const msg of messages.slice(0, MAX_MESSAGES_TO_SCAN)) {
        // Flatten string | structured-block content + tool calls into one
        // searchable string, so a query hits text inside a tool_use input or a
        // tool_result that a string-only check would miss. (H33)
        const content = flattenMessageText(msg);
        if (!content) continue;
        const lower = content.toLowerCase();
        const matchCount = terms.filter(t => lower.includes(t)).length;
        if (matchCount > 0) {
          // Hoist literal matches (every token verbatim and contiguous) above
          // scattered-term matches of the same coverage.
          const literalBonus = terms.length > 1 && lower.includes(phrase) ? 1 : 0;
          results.push({
            sessionId,
            role: msg.role,
            // Match-centered snippet (…before <match> after…) instead of a fixed head.
            content: matchSnippet(content, terms, phrase),
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
