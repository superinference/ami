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

/**
 * Subsequence (fuzzy) fallback tier. When a short single-token query matches no
 * session by substring, try a subsequence match ("tststp" ⊂ "test stop") so a
 * typo/abbreviation still surfaces — ranked STRICTLY below every substring hit.
 * Gated to a single short token because subsequence over long transcript bodies
 * is noisy (mirrors the reference's "gate fuzzy for short queries" guidance).
 */
export const SUBSEQUENCE_MAX_QUERY_LEN = 32; // longest query eligible for the fuzzy tier
export const MAX_SUBSEQUENCE_SCAN_CHARS = 50000; // per-session searchable-text cap for the fuzzy scan
/** Fuzzy score band: strictly below any substring score (min 1/terms.length,
 *  which for the single-token fuzzy gate is 1.0) so exact always outranks fuzzy. */
const SUBSEQUENCE_SCORE = 0.05;

/**
 * Does `query` appear as a (case-insensitive) subsequence of `text` — its
 * characters in order but not necessarily contiguous? An empty query trivially
 * matches. The single canonical subsequence primitive: the CLI fuzzy filters
 * (inspector-actions) import this rather than re-implementing it.
 */
export function isSubsequence(text: string, query: string): boolean {
  if (!query) return true;
  const t = text.toLowerCase();
  const q = query.toLowerCase();
  let i = 0;
  for (let j = 0; j < t.length && i < q.length; j++) {
    if (t[j] === q[i]) i++;
  }
  return i === q.length;
}

/** Model-only context (system-reminder spans) must not produce phantom search
 *  hits — strip the whole block, open tag to close tag, before matching. */
const SYSTEM_REMINDER_RE = /<system-reminder\b[^>]*>[\s\S]*?<\/system-reminder>/gi;
/** Break-guard for an unclosed reminder: once every well-formed span is removed,
 *  a surviving `<system-reminder …>` (no matching close — e.g. a truncated
 *  transcript) would otherwise leak its text into the index. Strip it, and
 *  everything after it, to the end of the string. */
const SYSTEM_REMINDER_OPEN_RE = /<system-reminder\b[^>]*>[\s\S]*$/i;

/**
 * Interrupt placeholders written into message content render in the TUI as a
 * component ("Interrupted by user"/a stopped-tool marker), not as their literal
 * text — so indexing them produces phantom hits (e.g. a `/interrupted` query
 * matching text the user never sees). Mirrors the reference's RENDERED_AS_SENTINEL
 * exclusion. The AMI sentinels are `[Response interrupted by server error]`
 * (engine) and the `[Tool call interrupted …]` family (provider-transform,
 * speculation); strip them from the searchable text before matching. */
const RENDERED_AS_SENTINEL_RE =
  /\[(?:Response interrupted by server error|Tool call interrupted[^\]]*)\]/g;

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
  // Closed spans first, then any unclosed trailing span (break-guard).
  return parts
    .filter(Boolean)
    .join(' ')
    .replace(SYSTEM_REMINDER_RE, ' ')
    .replace(SYSTEM_REMINDER_OPEN_RE, ' ')
    // Drop render-replaced interrupt sentinels so their raw text can't match.
    .replace(RENDERED_AS_SENTINEL_RE, ' ');
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

/**
 * Newest-first, capped list of session filenames in a sessions dir. A huge
 * session dir must not make a scan unbounded: sort by mtime desc, take at most
 * {@link MAX_SESSIONS_TO_SEARCH}. Shared by the literal search and the recent-fill
 * tier so both apply the same ordering + cap.
 */
function sessionFilesByRecency(sessionsDir: string): string[] {
  const candidates: { file: string; mtimeMs: number }[] = [];
  for (const file of fs.readdirSync(sessionsDir)) {
    if (!file.endsWith('.json')) continue;
    let mtimeMs = 0;
    try { mtimeMs = fs.statSync(path.join(sessionsDir, file)).mtimeMs; } catch { /* stat race */ }
    candidates.push({ file, mtimeMs });
  }
  candidates.sort((a, b) => b.mtimeMs - a.mtimeMs);
  return candidates.slice(0, MAX_SESSIONS_TO_SEARCH).map(c => c.file);
}

export function searchSessions(cwd: string, query: string, limit: number = 20): SearchResult[] {
  const sessionsDir = projectStateDir(cwd, 'sessions');
  if (!fs.existsSync(sessionsDir)) return [];

  const phrase = query.toLowerCase().trim();
  const terms = phrase.split(/\s+/).filter(Boolean);
  if (terms.length === 0) return [];

  const results: SearchResult[] = [];

  for (const file of sessionFilesByRecency(sessionsDir)) {
    try {
      const filePath = path.join(sessionsDir, file);
      // OOM guard: skip a session file larger than the read cap rather than
      // slurping it into memory.
      if (fs.statSync(filePath).size > MAX_SESSION_SEARCH_BYTES) continue;
      const raw = fs.readFileSync(filePath, 'utf-8');
      const session = JSON.parse(raw);
      const sessionId = session.id ?? file.replace('.json', '');
      // A session contributes substring hits first; only if it produced NONE do
      // we fall back to the subsequence (fuzzy) tier below, so exact always wins.
      let sessionHadSubstringHit = false;
      // Accumulate the session's searchable text (capped) for the fuzzy scan.
      let subsequenceText = '';
      const addSubsequenceText = (t: string): void => {
        if (subsequenceText.length >= MAX_SUBSEQUENCE_SCAN_CHARS) return;
        subsequenceText += (subsequenceText ? ' ' : '') + t;
      };
      // Session-level metadata (user tag + title + git branch) is searchable in
      // /resume, so a labeled session — or one on a named branch — surfaces even
      // when no message body matches the query. (H13)
      const metaText = [session.tag, session.customTitle, session.aiTitle, session.gitBranch]
        .filter((v: unknown): v is string => typeof v === 'string' && v.length > 0)
        .join(' ');
      if (metaText) {
        addSubsequenceText(metaText);
        const lowerMeta = metaText.toLowerCase();
        const metaMatch = terms.filter(t => lowerMeta.includes(t)).length;
        if (metaMatch > 0) {
          sessionHadSubstringHit = true;
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
        addSubsequenceText(content);
        const lower = content.toLowerCase();
        const matchCount = terms.filter(t => lower.includes(t)).length;
        if (matchCount > 0) {
          sessionHadSubstringHit = true;
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
      // Fuzzy fallback: a short single-token query with no substring hit anywhere
      // in this session still surfaces via subsequence match, ranked strictly
      // below every exact hit.
      if (
        !sessionHadSubstringHit &&
        terms.length === 1 &&
        phrase.length <= SUBSEQUENCE_MAX_QUERY_LEN &&
        subsequenceText &&
        isSubsequence(subsequenceText.toLowerCase(), phrase)
      ) {
        results.push({
          sessionId,
          role: 'fuzzy',
          content: matchSnippet(subsequenceText, terms, phrase),
          score: SUBSEQUENCE_SCORE,
          timestamp: session.createdAt,
        });
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
export type SessionRankFn = (
  query: string,
  candidates: SearchResult[],
  signal?: AbortSignal,
) => Promise<string[]>;

/** @public Options for {@link searchSessionsRanked}. */
export interface RankedSearchOptions {
  limit?: number;
  /** When omitted, no reranking happens: result equals {@link searchSessions} (zero network). */
  rankFn?: SessionRankFn;
  /**
   * Optional cancellation token. If already aborted when reranking would start,
   * the (fast, local) substring order is returned and the ranker is never
   * invoked; otherwise the signal is threaded into `rankFn` so a consumer can
   * abort its in-flight LLM rerank (e.g. the user retypes the query).
   */
  signal?: AbortSignal;
  /**
   * When true AND a `rankFn` is supplied: if literal matches number fewer than
   * `limit`, pad the ranker's candidate pool with the most-recent non-matching
   * sessions (newest first) so a semantic rerank can surface relevant sessions
   * that lack the literal query term. Off by default — plain literal search stays
   * zero-network and never fabricates non-matching results. Mirrors the
   * reference's agentic recent-fill (agenticSessionSearch.ts L161-175).
   */
  recentFill?: boolean;
}

/**
 * Build context-only {@link SearchResult} entries for the most-recent sessions
 * (newest `createdAt` first), excluding any `excludeIds`. These carry score 0 —
 * they rank below every literal/fuzzy hit and exist solely to widen a semantic
 * reranker's candidate pool. Reads are size-capped like the literal scan.
 */
function recentSessionResults(cwd: string, count: number, excludeIds: Set<string>): SearchResult[] {
  if (count <= 0) return [];
  const sessionsDir = projectStateDir(cwd, 'sessions');
  if (!fs.existsSync(sessionsDir)) return [];
  const out: SearchResult[] = [];
  for (const file of sessionFilesByRecency(sessionsDir)) {
    try {
      const filePath = path.join(sessionsDir, file);
      if (fs.statSync(filePath).size > MAX_SESSION_SEARCH_BYTES) continue;
      const session = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
      const sessionId = session.id ?? file.replace('.json', '');
      if (excludeIds.has(sessionId)) continue;
      // A lightweight context snippet: title/tag/branch, else the first non-empty
      // flattened message — enough for a reranker to judge relevance.
      let snippet = [session.customTitle, session.aiTitle, session.tag, session.gitBranch]
        .filter((v: unknown): v is string => typeof v === 'string' && v.length > 0)
        .join(' ');
      if (!snippet) {
        const messages = Array.isArray(session.messages) ? session.messages : [];
        for (const msg of messages) {
          const t = flattenMessageText(msg);
          if (t) { snippet = t; break; }
        }
      }
      out.push({
        sessionId,
        role: 'recent',
        content: matchSnippet(snippet, [], ''),
        score: 0,
        timestamp: session.createdAt,
      });
    } catch { /* skip malformed session files */ }
  }
  out.sort((a, b) => recencyMs(b.timestamp) - recencyMs(a.timestamp));
  return out.slice(0, count);
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
  const { limit = 20, rankFn, signal, recentFill = false } = options;
  const candidates = searchSessions(cwd, query, limit);
  if (!rankFn) return candidates;
  // Fallback-to-recent: when enabled and literal matches under-fill the limit,
  // widen the ranker's pool with the most-recent non-matching sessions so a
  // semantic rerank can still surface sessions lacking the literal query term.
  let pool = candidates;
  if (recentFill && candidates.length < limit) {
    const have = new Set(candidates.map(c => c.sessionId));
    pool = [...candidates, ...recentSessionResults(cwd, limit - candidates.length, have)];
  }
  if (pool.length === 0) return pool;
  // Already-cancelled search: skip the (potentially networked) rerank entirely
  // and hand back the fast local order.
  if (signal?.aborted) return pool;
  try {
    const orderedIds = await rankFn(query, pool, signal);
    return reorderByIds(pool, orderedIds);
  } catch {
    // Ranker failure (incl. an abort mid-flight) must never break search: fall
    // back to the local order.
    return pool;
  }
}
