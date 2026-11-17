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
 * Boundary caps on the transcript payload handed to an (optional) LLM reranker —
 * mirrors the reference's agentic-search budgets (agenticSessionSearch.ts L10-13).
 * A ranking model must not be fed an unbounded session body: sample messages from
 * both ends, collapse whitespace, and hard-cap the char count with an ellipsis.
 */
export const MAX_RANKING_TRANSCRIPT_CHARS = 2000; // per-session transcript chars sent to the ranker
export const MAX_RANKING_MESSAGES_TO_SCAN = 100;  // messages sampled from both ends of a session
export const MAX_RANKING_CANDIDATES = 100;        // sessions sent to the ranker in one call

/**
 * Tool-use input fields that RENDER as the visible argument of a tool call
 * ("⏺ Bash(command)", "Grep(pattern)", "Read(file_path)", "Agent(prompt)"), and
 * therefore are what a user expects a search to match. Mirrors the reference's
 * `toolUseSearchText` allowlist (transcriptSearch.ts L134-164). Indexing the WHOLE
 * input object (a blind `JSON.stringify`) leaks non-rendered bookkeeping fields
 * (`backgroundTaskId`, `rawOutputPath`, internal flags) into the index and yields
 * phantom matches — under-count beats phantom, so allowlist the rendered fields. */
const TOOL_USE_STRING_FIELDS = [
  'command', 'pattern', 'file_path', 'path', 'prompt', 'description', 'query', 'url', 'skill',
] as const;
/** Array-valued tool-use fields rendered as a joined string (Tmux `args[]`,
 *  SendUserFile `files[]`). */
const TOOL_USE_ARRAY_FIELDS = ['args', 'files'] as const;

/** Tool-result string fields a tool actually renders (Bash `stdout`/`stderr`,
 *  Read `file.content`, generic `content`/`output`/`result`/`text`/`message`). */
const TOOL_RESULT_STRING_FIELDS = ['content', 'output', 'result', 'text', 'message'] as const;
/** Array-valued tool-result fields (Grep/Glob `filenames`, match `lines`/`results`). */
const TOOL_RESULT_ARRAY_FIELDS = ['filenames', 'lines', 'results'] as const;

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
 * Tie window (ms) for the hybrid recency ordering: two results whose timestamps
 * are within this window count as "the same time", so relevance score breaks the
 * tie between them; outside the window the newer one always wins. Mirrors the
 * reference's `DATE_TIE_THRESHOLD_MS` (LogSelector.tsx L76, _temp3 L1476-1484).
 */
export const DATE_TIE_THRESHOLD_MS = 60 * 1000; // 1 minute

/**
 * Result ordering for {@link searchSessions} / {@link searchSessionsRanked}:
 * - `'relevance'` (default): best relevance score first, recency breaks ties —
 *   the existing AMI behavior (zero change for existing callers).
 * - `'recency'`: newest first (chronological), with relevance score breaking ties
 *   only WITHIN {@link DATE_TIE_THRESHOLD_MS}. Parity with the reference
 *   LogSelector deep-search ordering. Exposed via `/search --recent`.
 */
export type SearchOrdering = 'relevance' | 'recency';

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

/** Uncached flatten core for {@link flattenMessageText}. */
function computeFlattenedMessageText(m: Record<string, unknown>): string {
  // Engine-injected ephemeral control turns (`isMeta`) render as a component /
  // nudge, not as their literal text (or are hidden from the user entirely), so
  // indexing them produces phantom search hits for text the user never saw. A
  // `task-notification` queued command is likewise machine-owned chrome. Mirror
  // the reference's isMeta / commandMode==='task-notification' exclusion knobs
  // (transcriptSearch.ts L82-112) and drop them from the searchable text. (H33)
  if (m.isMeta === true) return '';
  if (m.commandMode === 'task-notification' || m.mode === 'task-notification') return '';
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

/**
 * Newest-first, capped list of session filenames in a sessions dir. A huge
 * session dir must not make a scan unbounded: sort by mtime desc, take at most
 * `maxSessions` (default {@link MAX_SESSIONS_TO_SEARCH}). Shared by the literal
 * search and the recent-fill tier so both apply the same ordering + cap.
 */
function sessionFilesByRecency(sessionsDir: string, maxSessions: number = MAX_SESSIONS_TO_SEARCH): string[] {
  const candidates: { file: string; mtimeMs: number }[] = [];
  for (const file of fs.readdirSync(sessionsDir)) {
    if (!file.endsWith('.json')) continue;
    let mtimeMs = 0;
    try { mtimeMs = fs.statSync(path.join(sessionsDir, file)).mtimeMs; } catch { /* stat race */ }
    candidates.push({ file, mtimeMs });
  }
  candidates.sort((a, b) => b.mtimeMs - a.mtimeMs);
  return candidates.slice(0, Math.max(0, maxSessions)).map(c => c.file);
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
