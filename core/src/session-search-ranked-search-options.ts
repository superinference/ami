// Split from session-search.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import { isValidSessionId } from './session';
import { MAX_SESSIONS_TO_SEARCH, RENDERED_AS_SENTINEL_RE, SYSTEM_REMINDER_OPEN_RE, SYSTEM_REMINDER_RE, attachmentSearchText, flattenContent, memoryListText, toolUseSearchText } from './session-search-match-snippet';
import type { SearchOrdering, SearchResult, SearchTraceFn } from './session-search-match-snippet';

/** Uncached flatten core for {@link flattenMessageText}. */
export function computeFlattenedMessageText(m: Record<string, unknown>): string {
  // Engine-injected ephemeral control turns (`isMeta`) render as a component /
  // nudge, not as their literal text (or are hidden from the user entirely), so
  // indexing them produces phantom search hits for text the user never saw. A
  // `task-notification` queued command is likewise machine-owned chrome. Mirror
  // the reference's isMeta / commandMode==='task-notification' exclusion knobs
  // (transcriptSearch.ts L82-112) and drop them from the searchable text. (H33)
  if (m.isMeta === true) return '';
  if (m.commandMode === 'task-notification' || m.mode === 'task-notification') return '';
  // AMI's canonical message provenance is {@link MessageOrigin}; a `task-notification`
  // origin is machine-owned chrome (the model's own task-completion report), rendered
  // as a notification component — not user-authored text. Mirror the reference's
  // commandMode==='task-notification' exclusion using AMI's provenance field so a
  // provenance-tagged notification never produces a phantom search hit. (H33)
  const origin = m.origin;
  if (origin && typeof origin === 'object' && (origin as { kind?: unknown }).kind === 'task-notification') return '';
  const parts: string[] = [];
  parts.push(flattenContent(m.content));
  // Attachment-shaped messages render their text in the transcript (a relevant-memories
  // recall, a mid-turn queued prompt) but carry it OUTSIDE `content`, so the string/
  // block flatten above never sees it — a query matching only a recalled memory or a
  // queued prompt would return nothing. Mirror the reference's `computeSearchText`
  // attachment arm (transcriptSearch.ts L82-112), honoring the SAME meta/
  // task-notification exclusion knobs for a queued command.
  if (m.attachment !== undefined) parts.push(attachmentSearchText(m.attachment));
  // Collapsed read/search groups absorb relevant-memories attachments; their content
  // still renders (collapsed_read_search arm), so index it too.
  if (Array.isArray(m.relevantMemories)) parts.push(memoryListText(m.relevantMemories));
  // Assistant tool calls — the OpenAI-style shape used across the engine.
  const toolCalls = m.tool_calls;
  if (Array.isArray(toolCalls)) {
    for (const tc of toolCalls) {
      const fn = tc && typeof tc === 'object' ? (tc as Record<string, unknown>).function : undefined;
      if (fn && typeof fn === 'object') {
        const f = fn as Record<string, unknown>;
        if (typeof f.name === 'string') parts.push(f.name);
        // `arguments` is a JSON string (OpenAI shape). Parse it and index only
        // the allowlisted rendered fields — indexing the raw JSON would leak
        // non-rendered bookkeeping keys and produce phantom matches. Non-JSON
        // arguments (non-standard) fall back to the raw string so nothing is lost.
        if (typeof f.arguments === 'string') {
          let parsed: unknown;
          try { parsed = JSON.parse(f.arguments); } catch { parsed = undefined; }
          parts.push(parsed !== undefined ? toolUseSearchText(parsed) : f.arguments);
        }
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
 * Newest-first, capped list of session filenames in a sessions dir. A huge
 * session dir must not make a scan unbounded: sort by mtime desc, take at most
 * `maxSessions` (default {@link MAX_SESSIONS_TO_SEARCH}). Shared by the literal
 * search and the recent-fill tier so both apply the same ordering + cap.
 */
export function sessionFilesByRecency(sessionsDir: string, maxSessions: number = MAX_SESSIONS_TO_SEARCH): string[] {
  const candidates: { file: string; mtimeMs: number }[] = [];
  for (const file of fs.readdirSync(sessionsDir)) {
    if (!file.endsWith('.json')) continue;
    // Session-id filename validation before enumeration (parity with the
    // reference's `listSessionsImpl` UUID-filename check): a stray `*.json` file
    // whose stem is not a format-valid session id (an empty stem `.json`, or a
    // crafted traversal stem `...json` → `..`, or one carrying a separator /
    // control byte) is NOT a managed session and must not be scanned — indexing
    // it yields phantom hits from non-session JSON a user never saved as a
    // conversation. AMI ids are `session-…` (not UUIDs), so the stem is validated
    // with the SAME canonical {@link isValidSessionId} boundary `SessionManager`
    // uses — no second, divergent id grammar.
    const stem = file.slice(0, -'.json'.length);
    if (!isValidSessionId(stem)) continue;
    let mtimeMs = 0;
    try { mtimeMs = fs.statSync(path.join(sessionsDir, file)).mtimeMs; } catch { /* stat race */ }
    candidates.push({ file, mtimeMs });
  }
  candidates.sort((a, b) => b.mtimeMs - a.mtimeMs);
  return candidates.slice(0, Math.max(0, maxSessions)).map(c => c.file);
}

/**
 * Result ordering for {@link searchTranscript}:
 * - `'relevance'` (default): best relevance score first, earliest position breaks ties.
 * - `'position'`: transcript order (earliest message first) — the ordering a live
 *   find overlay navigates with next/prev (`n`/`N`), where a match's place in the
 *   conversation, not its score, is what the user steps through.
 */
export const TRANSCRIPT_ORDERINGS = ['relevance', 'position'] as const;

export type TranscriptOrdering = (typeof TRANSCRIPT_ORDERINGS)[number];

/**
 * @public One hit from {@link searchTranscript}: the matched message's position in
 * the in-memory transcript (`index`), its `role`, a match-centered `content`
 * snippet, and the relevance `score` (term coverage + literal/phrase bonus, or
 * {@link SUBSEQUENCE_SCORE} for a fuzzy-tier hit).
 */
export interface TranscriptMatch {
  /** Position of the matched message in the input array (0-based). */
  index: number;
  role: string;
  /** Match-centered snippet (…before <match> after…), capped at `snippetChars`. */
  content: string;
  score: number;
}

/** @public Options for {@link searchTranscript}. */
export interface TranscriptSearchOptions {
  /** Max matches to return (default: all). */
  limit?: number;
  /** Max chars per result snippet (default {@link MAX_SNIPPET_CHARS}). */
  snippetChars?: number;
  /** Result ordering (default `'relevance'`; see {@link TranscriptOrdering}). */
  ordering?: TranscriptOrdering;
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
  /**
   * Ordering applied to the fast literal pass (see {@link SearchOrdering}). Defaults
   * to `'relevance'`. `'recency'` orders newest-first with relevance breaking ties
   * within {@link DATE_TIE_THRESHOLD_MS}. When a `rankFn` reorders the pool this only
   * affects which candidates fall inside the `limit` before reranking.
   */
  ordering?: SearchOrdering;
  /** Max chars per result snippet for the literal pass (default {@link MAX_SNIPPET_CHARS}). */
  snippetChars?: number;
  /** Newest-first cap on session files scanned (default {@link MAX_SESSIONS_TO_SEARCH}). */
  maxSessions?: number;
  /** Per-session message cap for the literal pass (default {@link MAX_MESSAGES_TO_SCAN}). */
  maxMessages?: number;
  /**
   * Optional diagnostic-trace sink. Invoked ONCE per ranked search with the literal
   * pass's counts PLUS a `rerank` block (applied/skipped reason, candidate-pool
   * size, recent-fill count) so a mis-ranked or empty result is diagnosable.
   * Mirrors the reference's step-by-step `logForDebugging` pipeline instrumentation.
   */
  trace?: SearchTraceFn;
}
