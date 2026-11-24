// Split from session-search.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).

/** @public Search result returned by session search for library consumers. */
export interface SearchResult {
  sessionId: string;
  role: string;
  content: string;
  score: number;
  timestamp?: string;
}

 // skip a session file larger than this
export const MAX_SESSIONS_TO_SEARCH = 2000;

               // newest-first cap on files scanned
export const MAX_MESSAGES_TO_SCAN = 5000;

                 // per-session message cap
export const MAX_SNIPPET_CHARS = 300;

                     // result snippet budget (incl. ellipses)

/**
 * Boundary caps on the transcript payload handed to an (optional) LLM reranker —
 * mirrors the reference's agentic-search budgets (agenticSessionSearch.ts L10-13).
 * A ranking model must not be fed an unbounded session body: sample messages from
 * both ends, collapse whitespace, and hard-cap the char count with an ellipsis.
 */
export const MAX_RANKING_TRANSCRIPT_CHARS = 2000;

 // per-session transcript chars sent to the ranker
export const MAX_RANKING_MESSAGES_TO_SCAN = 100;

  // messages sampled from both ends of a session
export const MAX_RANKING_CANDIDATES = 100;

        // sessions sent to the ranker in one call

/**
 * Tool-use input fields that RENDER as the visible argument of a tool call
 * ("⏺ Bash(command)", "Grep(pattern)", "Read(file_path)", "Agent(prompt)"), and
 * therefore are what a user expects a search to match. Mirrors the reference's
 * `toolUseSearchText` allowlist (transcriptSearch.ts L134-164). Indexing the WHOLE
 * input object (a blind `JSON.stringify`) leaks non-rendered bookkeeping fields
 * (`backgroundTaskId`, `rawOutputPath`, internal flags) into the index and yields
 * phantom matches — under-count beats phantom, so allowlist the rendered fields. */
export const TOOL_USE_STRING_FIELDS = [
  'command', 'pattern', 'file_path', 'path', 'prompt', 'description', 'query', 'url', 'skill',
] as const;

/** Array-valued tool-use fields rendered as a joined string (Tmux `args[]`,
 *  SendUserFile `files[]`). */
export const TOOL_USE_ARRAY_FIELDS = ['args', 'files'] as const;

/** Tool-result string fields a tool actually renders (Bash `stdout`/`stderr`,
 *  Read `file.content`, generic `content`/`output`/`result`/`text`/`message`). */
export const TOOL_RESULT_STRING_FIELDS = ['content', 'output', 'result', 'text', 'message'] as const;

/** Array-valued tool-result fields (Grep/Glob `filenames`, match `lines`/`results`). */
export const TOOL_RESULT_ARRAY_FIELDS = ['filenames', 'lines', 'results'] as const;

/**
 * Subsequence (fuzzy) fallback tier. When a short single-token query matches no
 * session by substring, try a subsequence match ("tststp" ⊂ "test stop") so a
 * typo/abbreviation still surfaces — ranked STRICTLY below every substring hit.
 * Gated to a single short token because subsequence over long transcript bodies
 * is noisy (mirrors the reference's "gate fuzzy for short queries" guidance).
 */
export const SUBSEQUENCE_MAX_QUERY_LEN = 32;

 // longest query eligible for the fuzzy tier
export const MAX_SUBSEQUENCE_SCAN_CHARS = 50000;

 // per-session searchable-text cap for the fuzzy scan
/** Fuzzy score band: strictly below any substring score (min 1/terms.length,
 *  which for the single-token fuzzy gate is 1.0) so exact always outranks fuzzy. */
export const SUBSEQUENCE_SCORE = 0.05;

 // 1 minute

/**
 * Result ordering for {@link searchSessions} / {@link searchSessionsRanked}:
 * - `'relevance'` (default): best relevance score first, recency breaks ties —
 *   the existing AMI behavior (zero change for existing callers).
 * - `'recency'`: newest first (chronological), with relevance score breaking ties
 *   only WITHIN {@link DATE_TIE_THRESHOLD_MS}. Parity with the reference
 *   LogSelector deep-search ordering. Exposed via `/search --recent`.
 */
export const SEARCH_ORDERINGS = ['relevance', 'recency'] as const;

export type SearchOrdering = (typeof SEARCH_ORDERINGS)[number];

/**
 * @public Step-by-step diagnostic trace of one search pass, mirroring the
 * reference's `logForDebugging` instrumentation of the agentic search pipeline
 * (agenticSessionSearch.ts L178-200): how many session files were scanned, how
 * many produced a hit, how many raw hit rows that yielded, the limit/ordering in
 * effect, and the final result count. For a ranked search the `rerank` block adds
 * whether the LLM reorder ran (or why it was skipped), the candidate-pool size,
 * and how many recent non-matching sessions were back-filled. Emitted ONLY when a
 * `trace` callback is supplied, so a zero/odd result is diagnosable without any
 * cost on the normal path. (AMI "introspection on every change".)
 */
export interface SearchTrace {
  query: string;
  /** Session files scanned (post newest-first recency cap). */
  scanned: number;
  /** Sessions that produced >=1 hit (meta / body / fuzzy). */
  matched: number;
  /** Raw hit rows accumulated before the `limit` truncation. */
  hits: number;
  /** Result-row cap requested by the caller. */
  limit: number;
  ordering: SearchOrdering;
  /** Result rows returned (post-limit). */
  results: number;
  /** Rerank stage — present only for {@link searchSessionsRanked}. */
  rerank?: {
    /** A `rankFn` ran and reordered the pool. */
    applied: boolean;
    /** Why the rerank did not run (absent when it did). */
    skipped?: 'no-rankfn' | 'aborted' | 'empty' | 'error';
    /** Sessions in the pool handed (or that would be handed) to the ranker. */
    candidates: number;
    /** Recent non-matching sessions appended by `recentFill`. */
    recentFilled: number;
  };
}

/** @public Diagnostic-trace sink for {@link searchSessions} / {@link searchSessionsRanked}. */
export type SearchTraceFn = (trace: SearchTrace) => void;

/**
 * @public Optional scan knobs for {@link searchSessions}. Every field defaults to
 * the module-level budget constant, so omitting the bag is byte-for-byte the old
 * behavior. Mirrors the reference's named, tunable extraction constants
 * (agenticSessionSearch.ts L10-13): a caller can shrink the snippet or cap how
 * many sessions/messages are scanned for a cheaper search, or widen them when a
 * deeper scan is wanted.
 */
export interface SearchScanOptions {
  /** Max chars per result snippet (default {@link MAX_SNIPPET_CHARS}). */
  snippetChars?: number;
  /** Newest-first cap on session files scanned (default {@link MAX_SESSIONS_TO_SEARCH}). */
  maxSessions?: number;
  /** Per-session message cap (default {@link MAX_MESSAGES_TO_SCAN}). */
  maxMessages?: number;
  /** Optional diagnostic-trace sink (see {@link SearchTrace}). */
  trace?: SearchTraceFn;
}

/**
 * Searchable text for a session's PR link (`pr #<number> <owner/repo>`), mirroring
 * the reference's `pr #${prNumber} ${prRepository}` field (LogSelector.tsx L432) so
 * a search for a PR number or its repo slug surfaces the linked session. Empty when
 * no PR (or no numeric PR number) is recorded. @public
 */
export function prSearchText(pr: unknown): string {
  if (!pr || typeof pr !== 'object') return '';
  const p = pr as { number?: unknown; repository?: unknown };
  if (typeof p.number !== 'number' || !Number.isFinite(p.number)) return '';
  const repo = typeof p.repository === 'string' && p.repository ? ` ${p.repository}` : '';
  return `pr #${p.number}${repo}`;
}

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
export const SYSTEM_REMINDER_RE = /<system-reminder\b[^>]*>[\s\S]*?<\/system-reminder>/gi;

/** Break-guard for an unclosed reminder: once every well-formed span is removed,
 *  a surviving `<system-reminder …>` (no matching close — e.g. a truncated
 *  transcript) would otherwise leak its text into the index. Strip it, and
 *  everything after it, to the end of the string. */
export const SYSTEM_REMINDER_OPEN_RE = /<system-reminder\b[^>]*>[\s\S]*$/i;

/**
 * Interrupt placeholders written into message content render in the TUI as a
 * component ("Interrupted by user"/a stopped-tool marker), not as their literal
 * text — so indexing them produces phantom hits (e.g. a `/interrupted` query
 * matching text the user never sees). Mirrors the reference's RENDERED_AS_SENTINEL
 * exclusion. The AMI sentinels are `[Response interrupted by server error]`
 * (engine) and the `[Tool call interrupted …]` family (provider-transform,
 * speculation); strip them from the searchable text before matching. */
export const RENDERED_AS_SENTINEL_RE =
  /\[(?:Response interrupted by server error|Tool call interrupted[^\]]*)\]/g;

/** Parse an ISO timestamp into epoch ms for recency ranking; unknown/invalid -> 0. */
export function recencyMs(timestamp?: string): number {
  if (!timestamp) return 0;
  const parsed = Date.parse(timestamp);
  return Number.isNaN(parsed) ? 0 : parsed;
}

/**
 * Snap a START index so the slice never begins on a lone LOW surrogate — i.e. in
 * the middle of an astral codepoint (emoji). Advancing past the orphaned low half
 * drops the broken leading glyph. Mirrors the reference's surrogate-pair accounting
 * in its code-unit→cell mapping (render-to-screen.ts scanPositions, L163-196),
 * reduced to what a string snippet needs. @see matchSnippet
 */
export function surrogateSafeStart(text: string, i: number): number {
  const c = text.charCodeAt(i);
  return c >= 0xdc00 && c <= 0xdfff ? i + 1 : i;
}

/**
 * Snap an EXCLUSIVE END index so the slice never ends on a lone HIGH surrogate —
 * the pair's low half would be excluded, leaving a broken trailing glyph. Retreating
 * one code unit drops it. @see matchSnippet
 */
export function surrogateSafeEnd(text: string, i: number): number {
  const prev = i > 0 ? text.charCodeAt(i - 1) : 0;
  return prev >= 0xd800 && prev <= 0xdbff ? i - 1 : i;
}

/**
 * Build a result snippet centered on the earliest match (phrase first, else any
 * term), with leading/trailing ellipsis when the surrounding text is clipped. The
 * total length never exceeds {@link MAX_SNIPPET_CHARS} (ellipses included). Text
 * that already fits is returned verbatim. (match-centered snippet + truncation
 * indicator)
 *
 * Every clip boundary is snapped to a UTF-16 codepoint boundary
 * ({@link surrogateSafeStart} / {@link surrogateSafeEnd}) so an astral character
 * (emoji) at a snippet edge is never split into a lone surrogate — which would
 * render as a broken replacement glyph (U+FFFD). Parity with the reference's
 * surrogate-pair handling in its match positioning (render-to-screen.ts L163-196).
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
  if (idx === -1) return text.slice(0, surrogateSafeEnd(text, max));
  const half = Math.floor(max / 2);
  let end = Math.min(text.length, Math.max(0, idx - half) + max);
  let start = Math.max(0, end - max);
  end = Math.min(text.length, start + max);
  // Snap both clip boundaries off any surrogate pair before slicing.
  start = surrogateSafeStart(text, start);
  end = surrogateSafeEnd(text, end);
  const prefix = start > 0 ? '…' : '';
  const suffix = end < text.length ? '…' : '';
  const room = max - prefix.length - suffix.length;
  let snip = text.slice(start, end);
  if (snip.length > room) snip = snip.slice(0, surrogateSafeEnd(snip, room));
  return prefix + snip + suffix;
}

/**
 * Allowlisted searchable text for a tool-use `input` object. Only the fields a
 * tool renders as its visible argument are indexed — a blind stringify of the
 * whole input would index non-rendered bookkeeping fields and produce phantom
 * matches. Unknown shapes index empty (under-count > phantom). Mirrors the
 * reference's `toolUseSearchText` (transcriptSearch.ts L134-164). @public
 */
export function toolUseSearchText(input: unknown): string {
  if (!input || typeof input !== 'object') return typeof input === 'string' ? input : '';
  const o = input as Record<string, unknown>;
  const parts: string[] = [];
  for (const k of TOOL_USE_STRING_FIELDS) {
    const v = o[k];
    if (typeof v === 'string') parts.push(v);
  }
  for (const k of TOOL_USE_ARRAY_FIELDS) {
    const v = o[k];
    if (Array.isArray(v) && v.every(x => typeof x === 'string')) parts.push((v as string[]).join(' '));
  }
  return parts.join(' ');
}

/**
 * Allowlisted searchable text for a tool's native result (`Out`) shape. Known
 * shapes first (Bash `{stdout,stderr}`, Read `{file:{content}}`), then an
 * allowlist of rendered output fields. Indexing the model-facing serialization
 * (reminders, `<persisted-output>` wrappers, internal id strings) would yield
 * phantom matches, so only the fields the UI renders are indexed; unknown shapes
 * index empty. Mirrors the reference's `toolResultSearchText`
 * (transcriptSearch.ts L171-202). @public
 */
export function toolResultSearchText(r: unknown): string {
  if (typeof r === 'string') return r;
  if (!r || typeof r !== 'object') return '';
  const o = r as Record<string, unknown>;
  // Known shapes first.
  if (typeof o.stdout === 'string') {
    const err = typeof o.stderr === 'string' ? o.stderr : '';
    return o.stdout + (err ? '\n' + err : '');
  }
  const file = o.file;
  if (file && typeof file === 'object' && typeof (file as { content?: unknown }).content === 'string') {
    return (file as { content: string }).content;
  }
  const parts: string[] = [];
  for (const k of TOOL_RESULT_STRING_FIELDS) {
    const v = o[k];
    if (typeof v === 'string') parts.push(v);
  }
  for (const k of TOOL_RESULT_ARRAY_FIELDS) {
    const v = o[k];
    if (Array.isArray(v) && v.every(x => typeof x === 'string')) parts.push((v as string[]).join('\n'));
  }
  return parts.join('\n');
}

/**
 * Join the user-visible `content` of a relevant-memories list into searchable
 * text. Each memory entry renders its `content` in the transcript (the reference's
 * AttachmentMessage `<Ansi>{m.content}</Ansi>`), so that is the indexable field;
 * non-object / non-string entries contribute nothing. Shared by the `attachment`
 * and `collapsed_read_search` extraction arms so the memory field-selector lives
 * in ONE place.
 */
export function memoryListText(memories: unknown): string {
  if (!Array.isArray(memories)) return '';
  const out: string[] = [];
  for (const m of memories) {
    if (m && typeof m === 'object' && typeof (m as { content?: unknown }).content === 'string') {
      out.push((m as { content: string }).content);
    }
  }
  return out.join('\n');
}

/**
 * Searchable text for an attachment-shaped message's payload, mirroring the
 * reference's `computeSearchText` attachment arm (transcriptSearch.ts L82-112):
 * - `relevant_memories` → the joined `content` of each recalled memory (it renders
 *   in transcript mode and is otherwise searchable-invisible);
 * - `queued_command` → the mid-turn queued prompt (a plain string, or the `text`
 *   blocks of a block array), gated by the SAME exclusion knobs as a live queued
 *   command — a `task-notification` command (machine-owned chrome) and an `isMeta`
 *   attachment contribute no searchable text.
 * Unknown attachment types index empty (under-count beats phantom). @public
 */
export function attachmentSearchText(attachment: unknown): string {
  if (!attachment || typeof attachment !== 'object') return '';
  const a = attachment as Record<string, unknown>;
  if (a.type === 'relevant_memories') {
    return memoryListText(a.memories);
  }
  if (
    a.type === 'queued_command' &&
    a.commandMode !== 'task-notification' &&
    a.isMeta !== true
  ) {
    const p = a.prompt;
    // The queued prompt is user-authored content — flatten it with the canonical
    // content flattener (string or text-block array) rather than re-implementing
    // the text-block select here.
    return typeof p === 'string' ? p : flattenContent(p);
  }
  return '';
}

/** Recursively flatten string | block-array content into searchable text. */
export function flattenContent(content: unknown): string {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  const out: string[] = [];
  for (const block of content) {
    if (typeof block === 'string') { out.push(block); continue; }
    if (!block || typeof block !== 'object') continue;
    const b = block as Record<string, unknown>;
    if (typeof b.text === 'string') out.push(b.text);
    if (typeof b.thinking === 'string') out.push(b.thinking);
    if (typeof b.name === 'string') out.push(b.name);
    // tool_result carries the tool's native Out — duck-type its rendered fields.
    if (b.type === 'tool_result') {
      out.push(toolResultSearchText(b.content));
    } else if (b.input !== undefined) {
      // tool_use carries a structured `input` — allowlist its rendered args only.
      out.push(toolUseSearchText(b.input));
    } else if (b.content !== undefined) {
      out.push(flattenContent(b.content));
    }
  }
  return out.filter(Boolean).join(' ');
}
