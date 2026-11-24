import * as fs from 'fs';
import * as path from 'path';
import { projectStateDir } from './reference-journal';
import './session';
import { MAX_MESSAGES_TO_SCAN, MAX_RANKING_CANDIDATES, MAX_RANKING_MESSAGES_TO_SCAN, MAX_RANKING_TRANSCRIPT_CHARS, MAX_SESSIONS_TO_SEARCH, MAX_SNIPPET_CHARS, MAX_SUBSEQUENCE_SCAN_CHARS, SUBSEQUENCE_MAX_QUERY_LEN, SUBSEQUENCE_SCORE, isSubsequence, matchSnippet, prSearchText, recencyMs } from './session-search-match-snippet';
import type { SearchOrdering, SearchResult, SearchScanOptions, SearchTrace } from './session-search-match-snippet';
import { computeFlattenedMessageText, sessionFilesByRecency } from './session-search-ranked-search-options';
import type { RankedSearchOptions, TranscriptMatch, TranscriptSearchOptions } from './session-search-ranked-search-options';
export { MAX_MESSAGES_TO_SCAN, MAX_RANKING_CANDIDATES, MAX_RANKING_MESSAGES_TO_SCAN, MAX_RANKING_TRANSCRIPT_CHARS, MAX_SESSIONS_TO_SEARCH, MAX_SNIPPET_CHARS, MAX_SUBSEQUENCE_SCAN_CHARS, SEARCH_ORDERINGS, SUBSEQUENCE_MAX_QUERY_LEN, attachmentSearchText, isSubsequence, matchSnippet, prSearchText, toolResultSearchText, toolUseSearchText } from './session-search-match-snippet';
export type { SearchOrdering, SearchResult, SearchScanOptions, SearchTrace, SearchTraceFn } from './session-search-match-snippet';
export { TRANSCRIPT_ORDERINGS } from './session-search-ranked-search-options';
export type { RankedSearchOptions, SessionRankFn, TranscriptMatch, TranscriptOrdering, TranscriptSearchOptions } from './session-search-ranked-search-options';

/**
 * Scan/read budgets — a session directory can grow unbounded, and a single
 * corrupt/huge session JSON could OOM the process. These caps keep search bounded
 * in both file count and per-file memory (mirrors the reference's search budgets).
 */
export const MAX_SESSION_SEARCH_BYTES = 25 * 1024 * 1024;

/**
 * Tie window (ms) for the hybrid recency ordering: two results whose timestamps
 * are within this window count as "the same time", so relevance score breaks the
 * tie between them; outside the window the newer one always wins. Mirrors the
 * reference's `DATE_TIE_THRESHOLD_MS` (LogSelector.tsx L76, _temp3 L1476-1484).
 */
export const DATE_TIE_THRESHOLD_MS = 60 * 1000;

/** Comparator for {@link SearchOrdering}. `relevance`: score desc, then recency
 *  desc. `recency`: recency desc, but relevance desc within the tie window. */
function resultComparator(ordering: SearchOrdering): (a: SearchResult, b: SearchResult) => number {
  if (ordering === 'recency') {
    return (a, b) => {
      const dt = recencyMs(b.timestamp) - recencyMs(a.timestamp);
      if (Math.abs(dt) > DATE_TIE_THRESHOLD_MS) return dt;
      return b.score - a.score;
    };
  }
  return (a, b) => b.score - a.score || recencyMs(b.timestamp) - recencyMs(a.timestamp);
}

/**
 * Per-message memoization of the flattened searchable text. Messages in a loaded
 * transcript are append-only and immutable, so a flatten result keyed on the
 * message object identity is permanently valid. Caching skips re-running the
 * (recursive, regex-heavy) flatten for the same message across repeated searches —
 * e.g. an interactive keystroke search re-scanning the same in-memory transcript on
 * every character ("backspace hang"). Mirrors the reference's `searchTextCache`
 * WeakMap (transcriptSearch.ts L16-29). Non-object messages are never cached. */
const flattenTextCache = new WeakMap<object, string>();
/** Companion cache for the LOWERCASED flattened text (see {@link renderableSearchText}):
 *  the matching side lowercases once at cache time, not on every keystroke. */
const lowerSearchTextCache = new WeakMap<object, string>();

/**
 * Flatten a message's content into one searchable string. A message's `content`
 * may be a plain string OR a structured block array (text / tool_use with an
 * `input` object / tool_result with nested `content`); the assistant form also
 * carries `tool_calls`. A string-only match would miss everything a tool call or
 * tool result said — so recurse structured content, stringify tool inputs, and
 * fold in tool-call names + arguments. (H33)
 *
 * WeakMap-memoized per immutable message object (see {@link flattenTextCache}) so a
 * repeated flatten of the same message is free.
 */
export function flattenMessageText(msg: unknown): string {
  if (!msg || typeof msg !== 'object') return '';
  const cached = flattenTextCache.get(msg);
  if (cached !== undefined) return cached;
  const result = computeFlattenedMessageText(msg as Record<string, unknown>);
  flattenTextCache.set(msg, result);
  return result;
}

/**
 * Lowercased flattened searchable text for a message, WeakMap-cached (lowercased
 * once at cache time). Every matching path lowercases the flattened body; doing it
 * once per immutable message — rather than re-lowering the whole transcript on each
 * keystroke — removes the quadratic "backspace hang" on large transcripts. Mirrors
 * the reference's `renderableSearchText` (transcriptSearch.ts L23-29). @public
 */
export function renderableSearchText(msg: unknown): string {
  if (!msg || typeof msg !== 'object') return '';
  const cached = lowerSearchTextCache.get(msg);
  if (cached !== undefined) return cached;
  // flattenMessageText is itself cached, so this never double-flattens a message
  // already flattened for its display snippet.
  const result = flattenMessageText(msg).toLowerCase();
  lowerSearchTextCache.set(msg, result);
  return result;
}

export function searchSessions(
  cwd: string,
  query: string,
  limit: number = 20,
  ordering: SearchOrdering = 'relevance',
  options: SearchScanOptions = {},
): SearchResult[] {
  const {
    snippetChars = MAX_SNIPPET_CHARS,
    maxSessions = MAX_SESSIONS_TO_SEARCH,
    maxMessages = MAX_MESSAGES_TO_SCAN,
    trace,
  } = options;
  const sessionsDir = projectStateDir(cwd, 'sessions');
  if (!fs.existsSync(sessionsDir)) {
    trace?.({ query, scanned: 0, matched: 0, hits: 0, limit, ordering, results: 0 });
    return [];
  }

  const phrase = query.toLowerCase().trim();
  const terms = phrase.split(/\s+/).filter(Boolean);
  if (terms.length === 0) {
    trace?.({ query, scanned: 0, matched: 0, hits: 0, limit, ordering, results: 0 });
    return [];
  }

  const results: SearchResult[] = [];
  // Diagnostics: files actually scanned + distinct sessions that produced a hit.
  let scanned = 0;
  let matched = 0;

  for (const file of sessionFilesByRecency(sessionsDir, maxSessions)) {
    scanned++;
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
      // Did THIS session push any result row (meta/body/fuzzy)? Drives `matched`.
      let sessionContributed = false;
      // Accumulate the session's searchable text (capped) for the fuzzy scan.
      let subsequenceText = '';
      const addSubsequenceText = (t: string): void => {
        if (subsequenceText.length >= MAX_SUBSEQUENCE_SCAN_CHARS) return;
        subsequenceText += (subsequenceText ? ' ' : '') + t;
      };
      // Session-level metadata (user tag + title + git branch) is searchable in
      // /resume, so a labeled session — or one on a named branch — surfaces even
      // when no message body matches the query. (H13)
      const metaText = [session.tag, session.customTitle, session.aiTitle, session.gitBranch, prSearchText(session.pr)]
        .filter((v: unknown): v is string => typeof v === 'string' && v.length > 0)
        .join(' ');
      if (metaText) {
        addSubsequenceText(metaText);
        const lowerMeta = metaText.toLowerCase();
        const metaMatch = terms.filter(t => lowerMeta.includes(t)).length;
        if (metaMatch > 0) {
          sessionHadSubstringHit = true;
          sessionContributed = true;
          const literalBonus = terms.length > 1 && lowerMeta.includes(phrase) ? 1 : 0;
          results.push({
            sessionId,
            role: 'meta',
            content: matchSnippet(metaText, terms, phrase, snippetChars),
            // Hoist a tag/title hit above scattered body matches of equal coverage.
            score: metaMatch / terms.length + literalBonus + 0.5,
            timestamp: session.createdAt,
          });
        }
      }
      const messages = Array.isArray(session.messages) ? session.messages : [];
      for (const msg of messages.slice(0, maxMessages)) {
        // Flatten string | structured-block content + tool calls into one
        // searchable string, so a query hits text inside a tool_use input or a
        // tool_result that a string-only check would miss. (H33)
        const content = flattenMessageText(msg);
        if (!content) continue;
        addSubsequenceText(content);
        // Matching uses the WeakMap-cached lowercased text (flattenMessageText is
        // itself cached, so the message is flattened once for both snippet + match).
        const lower = renderableSearchText(msg);
        const matchCount = terms.filter(t => lower.includes(t)).length;
        if (matchCount > 0) {
          sessionHadSubstringHit = true;
          sessionContributed = true;
          // Hoist literal matches (every token verbatim and contiguous) above
          // scattered-term matches of the same coverage.
          const literalBonus = terms.length > 1 && lower.includes(phrase) ? 1 : 0;
          results.push({
            sessionId,
            role: msg.role,
            // Match-centered snippet (…before <match> after…) instead of a fixed head.
            content: matchSnippet(content, terms, phrase, snippetChars),
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
        sessionContributed = true;
        results.push({
          sessionId,
          role: 'fuzzy',
          content: matchSnippet(subsequenceText, terms, phrase, snippetChars),
          score: SUBSEQUENCE_SCORE,
          timestamp: session.createdAt,
        });
      }
      if (sessionContributed) matched++;
    } catch {
      // Skip malformed session files
    }
  }

  // Rank per the selected ordering: relevance-first (default) or newest-first with
  // relevance breaking ties inside the DATE_TIE_THRESHOLD_MS window (`--recent`).
  const ranked = results.sort(resultComparator(ordering)).slice(0, limit);
  trace?.({
    query,
    scanned,
    matched,
    hits: results.length,
    limit,
    ordering,
    results: ranked.length,
  });
  return ranked;
}

/**
 * @public Live, in-memory transcript search — the model-space counterpart to the
 * disk-backed {@link searchSessions}. Given the CURRENT conversation's messages (as
 * the engine holds them, including turns not yet flushed to disk) and a query, it
 * returns the matching messages with a match-centered snippet, scored and ordered.
 *
 * It matches ONLY user-visible rendered text via the WeakMap-cached
 * {@link renderableSearchText} (so a tool_use argument, tool_result body, recalled
 * memory, or queued prompt is searchable while system-reminders / meta chrome /
 * interrupt sentinels are not), and reuses that per-message cache so re-searching
 * the same transcript on every keystroke does not re-flatten ~1.5MB of text (the
 * "backspace hang") — mirroring the reference's `renderableSearchText`
 * (transcriptSearch.ts L23-29). Shares every extraction/snippet/scoring helper with
 * {@link searchSessions}, so there is one canonical searchable-text definition.
 *
 * Fuzzy tier: identical gating to {@link searchSessions} — only a single short
 * token with NO substring hit anywhere in the transcript falls back to a
 * subsequence match, scored strictly below every substring hit.
 */
export function searchTranscript(
  messages: unknown[],
  query: string,
  options: TranscriptSearchOptions = {},
): TranscriptMatch[] {
  const { limit, snippetChars = MAX_SNIPPET_CHARS, ordering = 'relevance' } = options;
  if (!Array.isArray(messages) || messages.length === 0) return [];
  const phrase = query.toLowerCase().trim();
  const terms = phrase.split(/\s+/).filter(Boolean);
  if (terms.length === 0) return [];

  const matches: TranscriptMatch[] = [];
  let hadSubstringHit = false;
  for (let index = 0; index < messages.length; index++) {
    const msg = messages[index];
    // Flatten once (cached) for the snippet; match against the cached lowered text.
    const content = flattenMessageText(msg);
    if (!content) continue;
    const lower = renderableSearchText(msg);
    const matchCount = terms.filter(t => lower.includes(t)).length;
    if (matchCount > 0) {
      hadSubstringHit = true;
      const literalBonus = terms.length > 1 && lower.includes(phrase) ? 1 : 0;
      matches.push({
        index,
        role: typeof (msg as { role?: unknown }).role === 'string' ? (msg as { role: string }).role : 'unknown',
        content: matchSnippet(content, terms, phrase, snippetChars),
        score: matchCount / terms.length + literalBonus,
      });
    }
  }

  // Fuzzy fallback: a short single-token query with NO substring hit anywhere in
  // the transcript still surfaces messages by subsequence, ranked strictly below
  // every substring hit (parity with searchSessions' fuzzy tier).
  if (!hadSubstringHit && terms.length === 1 && phrase.length <= SUBSEQUENCE_MAX_QUERY_LEN) {
    for (let index = 0; index < messages.length; index++) {
      const msg = messages[index];
      const content = flattenMessageText(msg);
      if (!content) continue;
      if (isSubsequence(renderableSearchText(msg), phrase)) {
        matches.push({
          index,
          role: typeof (msg as { role?: unknown }).role === 'string' ? (msg as { role: string }).role : 'unknown',
          content: matchSnippet(content, terms, phrase, snippetChars),
          score: SUBSEQUENCE_SCORE,
        });
      }
    }
  }

  const ordered =
    ordering === 'position'
      ? matches // already in transcript order
      : matches.slice().sort((a, b) => b.score - a.score || a.index - b.index);
  return typeof limit === 'number' ? ordered.slice(0, Math.max(0, limit)) : ordered;
}

/**
 * Scalable transcript extraction for the (optional) ranking payload: sample
 * messages from BOTH ends of a long session (so the ranker sees the opening ask
 * and the latest state, not just a truncated head), flatten each to searchable
 * text, collapse whitespace, and hard-cap the total with a trailing ellipsis.
 * Mirrors the reference's `extractTranscript` (agenticSessionSearch.ts L83-108)
 * and its MAX_* boundary caps (L10-13). @public
 */
export function extractRankingTranscript(
  messages: unknown[],
  maxChars: number = MAX_RANKING_TRANSCRIPT_CHARS,
  maxMessages: number = MAX_RANKING_MESSAGES_TO_SCAN,
): string {
  if (!Array.isArray(messages) || messages.length === 0) return '';
  const half = Math.floor(maxMessages / 2);
  const sampled =
    messages.length <= maxMessages
      ? messages
      : [...messages.slice(0, half), ...messages.slice(-half)];
  const text = sampled
    .map(flattenMessageText)
    .filter(Boolean)
    .join(' ')
    .replace(/\s+/g, ' ')
    .trim();
  return text.length > maxChars ? text.slice(0, maxChars) + '…' : text;
}

/**
 * Build context-only {@link SearchResult} entries for the most-recent sessions
 * (newest `createdAt` first), excluding any `excludeIds`. These carry score 0 —
 * they rank below every literal/fuzzy hit and exist solely to widen a semantic
 * reranker's candidate pool. Reads are size-capped like the literal scan.
 */
function recentSessionResults(
  cwd: string,
  count: number,
  excludeIds: Set<string>,
  maxSessions: number = MAX_SESSIONS_TO_SEARCH,
): SearchResult[] {
  if (count <= 0) return [];
  const sessionsDir = projectStateDir(cwd, 'sessions');
  if (!fs.existsSync(sessionsDir)) return [];
  const out: SearchResult[] = [];
  for (const file of sessionFilesByRecency(sessionsDir, maxSessions)) {
    try {
      const filePath = path.join(sessionsDir, file);
      if (fs.statSync(filePath).size > MAX_SESSION_SEARCH_BYTES) continue;
      const session = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
      const sessionId = session.id ?? file.replace('.json', '');
      if (excludeIds.has(sessionId)) continue;
      // A context snippet for the reranker: title/tag/branch PLUS a bounded
      // head+tail transcript sample (so a semantic ranker sees the opening ask
      // and the latest state of a long session, char-capped for the payload).
      const metaPrefix = [session.customTitle, session.aiTitle, session.tag, session.gitBranch, prSearchText(session.pr)]
        .filter((v: unknown): v is string => typeof v === 'string' && v.length > 0)
        .join(' ');
      const body = extractRankingTranscript(
        Array.isArray(session.messages) ? session.messages : [],
      );
      const snippet = [metaPrefix, body].filter(Boolean).join(' ');
      out.push({
        sessionId,
        role: 'recent',
        // Keep the richer head+tail transcript (bounded by extractRankingTranscript)
        // so a semantic reranker has real context — not just a 300-char head.
        content: matchSnippet(snippet, [], '', MAX_RANKING_TRANSCRIPT_CHARS),
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
  const {
    limit = 20, rankFn, signal, recentFill = false, ordering = 'relevance',
    snippetChars, maxSessions, maxMessages, trace,
  } = options;
  // Capture the literal pass's trace counts so the single ranked-search trace can
  // carry them alongside the rerank block (one trace per ranked search).
  let base: SearchTrace | undefined;
  const candidates = searchSessions(cwd, query, limit, ordering, {
    snippetChars, maxSessions, maxMessages,
    trace: trace ? t => { base = t; } : undefined,
  });
  // Emit the ranked-search trace: literal counts + a rerank block describing what
  // happened at the reorder stage (or why it was skipped).
  const emit = (rerank: SearchTrace['rerank'], results: number): void => {
    if (!trace) return;
    const b = base ?? { query, scanned: 0, matched: 0, hits: candidates.length, limit, ordering, results };
    trace({ ...b, results, rerank });
  };
  if (!rankFn) {
    emit({ applied: false, skipped: 'no-rankfn', candidates: candidates.length, recentFilled: 0 }, candidates.length);
    return candidates;
  }
  // Fallback-to-recent: when enabled and literal matches under-fill the limit,
  // widen the ranker's pool with the most-recent non-matching sessions so a
  // semantic rerank can still surface sessions lacking the literal query term.
  let pool = candidates;
  let recentFilled = 0;
  if (recentFill && candidates.length < limit) {
    const have = new Set(candidates.map(c => c.sessionId));
    const filler = recentSessionResults(cwd, limit - candidates.length, have, maxSessions);
    recentFilled = filler.length;
    pool = [...candidates, ...filler];
  }
  // Boundary cap on the payload sent to the ranking model: never hand a reranker
  // more than MAX_RANKING_CANDIDATES sessions in one call (reference L12).
  if (pool.length > MAX_RANKING_CANDIDATES) pool = pool.slice(0, MAX_RANKING_CANDIDATES);
  if (pool.length === 0) {
    emit({ applied: false, skipped: 'empty', candidates: 0, recentFilled }, 0);
    return pool;
  }
  // Already-cancelled search: skip the (potentially networked) rerank entirely
  // and hand back the fast local order.
  if (signal?.aborted) {
    emit({ applied: false, skipped: 'aborted', candidates: pool.length, recentFilled }, pool.length);
    return pool;
  }
  try {
    const orderedIds = await rankFn(query, pool, signal);
    const reordered = reorderByIds(pool, orderedIds);
    emit({ applied: true, candidates: pool.length, recentFilled }, reordered.length);
    return reordered;
  } catch {
    // Ranker failure (incl. an abort mid-flight) must never break search: fall
    // back to the local order.
    emit({ applied: false, skipped: 'error', candidates: pool.length, recentFilled }, pool.length);
    return pool;
  }
}
