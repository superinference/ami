// Split from title-generator.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { ProviderConfig, Message, StreamChunk } from './types';
import { streamChatCompletion, extractProviderRequestId } from './provider';
import { contentToText } from './session';
import { stripDisplayTagsAllowEmpty } from './utils/display-tags';

/**
 * Cap on the characters of conversation text fed to the title model. Parity with
 * the reference's `MAX_CONVERSATION_TEXT = 1000` (utils/sessionTitle.ts:24). The cap is
 * applied as a RECENCY-BIASED TAIL slice (see {@link capTitleInput}) so that when
 * the conversation is long the MOST RECENT context is what the title model sees.
 */
export const MAX_CONVERSATION_TEXT = 1000;

/** Maximum rendered length of a session title (parity: the reference's `TITLE_MAX_LEN`,
 *  initReplBridge.ts). Shared by {@link deriveTitle} and {@link truncateTitle}. */
export const TITLE_MAX_LEN = 50;

/** Wall-clock cap for a single auxiliary/background model query. Parity: the reference's
 *  title generation runs behind its own abort; AMI additionally bounds it so a
 *  wedged provider can never hang title/branch generation indefinitely. */
export const AUX_QUERY_TIMEOUT_MS = 30000;

// ---------------------------------------------------------------------------
// Structured, source-tagged result record for auxiliary/background LLM queries
// (parity: the reference's `ApiQueryResult` + `logResult`, utils/hooks/apiQueryHookHelper.ts
// :40-54,113-139). AMI's background queries — session-title and worktree-branch
// generation — now emit a UNIFORM discriminated success|error record so a failed,
// aborted, timed-out or unparseable background query is OBSERVABLE (attributed by
// named source + model + correlation id) instead of being silently swallowed by a
// bare `catch {}` and masked behind a fallback string.
// ---------------------------------------------------------------------------

/** Named source of a background query (parity: the reference's `querySource`/`queryName`).
 *  `tool_use_summary` tags the LLM-written past-tense label describing what a batch
 *  of completed tool calls accomplished (parity: the reference's
 *  `querySource: 'tool_use_summary_generation'`, toolUseSummaryGenerator.ts). */
export const AUX_QUERY_SOURCES = ['session_title', 'branch_name', 'compaction_summary', 'tool_use_summary', 'issue_title'] as const;

export type AuxQuerySource = (typeof AUX_QUERY_SOURCES)[number];

/** Why a background query yielded no usable model output. Distinguishes an OUTER
 *  failure (stream error / abort / timeout / exception) from a two-tier INNER
 *  `unparseable` failure where the query returned but produced nothing usable —
 *  mirroring the reference's parse-error vs outer-error split (apiQueryHookHelper.ts:126,137). */
export const AUX_QUERY_ERROR_REASONS = ['stream_error', 'aborted', 'timeout', 'exception', 'unparseable'] as const;

export type AuxQueryErrorReason = (typeof AUX_QUERY_ERROR_REASONS)[number];

export interface AuxQuerySuccess {
  type: 'success';
  source: AuxQuerySource;
  /** The model the query actually ran against (parity: `ApiQueryResult.model`). */
  model: string;
  /** The usable output (title / branch slug) that was accepted. */
  result: string;
  /** Provider correlation id from the response headers, when one was returned
   *  (parity: `ApiQueryResult.messageId`). */
  messageId?: string;
  /** Per-query id for log correlation (parity: `ApiQueryResult.uuid`). */
  uuid: string;
}

export interface AuxQueryError {
  type: 'error';
  source: AuxQuerySource;
  model: string;
  reason: AuxQueryErrorReason;
  /** Human-readable detail (stream error text / exception message), when available. */
  error?: string;
  messageId?: string;
  uuid: string;
}

/** Discriminated success|error record for a single background query. */
export type AuxQueryResult = AuxQuerySuccess | AuxQueryError;

/** Introspection sink for a background query's structured outcome (parity: the reference's
 *  `logResult` callback). Fire-and-forget: the aux query never fails because its
 *  result sink threw. */
export type OnAuxQueryResult = (result: AuxQueryResult) => void;

/**
 * Cap title-generation input to the last {@link MAX_CONVERSATION_TEXT} characters.
 * Parity: the reference tail-slices (`text.slice(-MAX)`) so "recent context wins when the
 * conversation is long" (utils/sessionTitle.ts:52-53). This replaces a HEAD slice,
 * which kept only the oldest opening prefix (preamble/boilerplate) and dropped the
 * substantive recent ask. A string already within the cap is returned unchanged.
 */
export function capTitleInput(text: string): string {
  return text.length > MAX_CONVERSATION_TEXT ? text.slice(-MAX_CONVERSATION_TEXT) : text;
}

/**
 * A message is a valid title SOURCE only when it is genuinely human-authored.
 * Parity: the reference's title-source filter (initReplBridge `onUserMessage` /
 * `extractTitleText` + `isSyntheticMessage`) which skips meta nudges, synthetic/
 * transcript-only injections, tool results, and non-`human`-origin messages
 * (task notifications, channel pushes, coordinator/teammate traffic). Without
 * this an engine-injected `isMeta` nudge or a task-notification could become the
 * session title. Tool results are `role: 'tool'` in AMI, so the role check
 * already excludes them.
 */
export function isHumanAuthoredUserMessage(m: Message): boolean {
  if (m.role !== 'user') return false;
  if (m.isMeta) return false;
  if (m.isVisibleInTranscriptOnly) return false;
  if (m.origin && m.origin.kind !== 'user') return false;
  // Skip command-only / pure-injected-tag messages: a turn that is ENTIRELY
  // system-injected wrapper tags (a bare `/clear` marker, a lone
  // `<session-start-hook>…</session-start-hook>`, a task-notification wrapper)
  // carries no human-authored prose and must not become a title source — the
  // deriver falls through to the next real user message (parity: the reference's
  // `stripDisplayTagsAllowEmpty` command-only detection, displayTags.ts:31-39).
  return stripDisplayTagsAllowEmpty(contentToText(m.content)).length > 0;
}

/**
 * Pick the message to derive a session title from by scanning BACKWARD for the
 * most recent human-authored user message (parity: the reference's backward
 * `initialMessages` scan). Replaces a naive forward `find()` that could latch
 * onto an injected meta/synthetic message. Returns the message's flattened text,
 * or undefined when no human-authored user message exists.
 */
export function selectTitleSource(messages: readonly Message[]): string | undefined {
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i]!;
    if (isHumanAuthoredUserMessage(m)) {
      return contentToText(m.content).trim();
    }
  }
  return undefined;
}

/**
 * Count the human-authored user messages in the transcript. Drives the
 * count-based title refinement (parity: the reference re-derives the title at user
 * message count 3) so injected meta/synthetic/non-human messages don't inflate
 * the count and trigger a premature re-derivation.
 */
export function countHumanUserMessages(messages: readonly Message[]): number {
  let n = 0;
  for (const m of messages) if (isHumanAuthoredUserMessage(m)) n++;
  return n;
}

/**
 * Flatten the conversation into title-generation input for the richer count-3
 * re-derivation (parity: the reference's `extractConversationText`). Includes every
 * human-authored user message and every non-empty assistant message, in order,
 * each prefixed by its role so the title model sees the actual dialogue rather
 * than only the first prompt. Meta/synthetic/non-human and tool messages are
 * excluded.
 */
export function extractConversationText(messages: readonly Message[]): string {
  const parts: string[] = [];
  for (const m of messages) {
    if (m.role === 'user') {
      if (!isHumanAuthoredUserMessage(m)) continue;
      const text = contentToText(m.content).trim();
      if (text) parts.push(`User: ${text}`);
    } else if (m.role === 'assistant') {
      const text = (m.content ?? '').trim();
      if (text) parts.push(`Assistant: ${text}`);
    }
  }
  // Recency-biased cap: keep the LAST MAX_CONVERSATION_TEXT chars so a long
  // conversation is titled from where it actually went, not its opening preamble
  // (parity: the reference's tail-slice, utils/sessionTitle.ts:52-53).
  return capTitleInput(parts.join('\n').trim());
}

/**
 * A session title is rendered on a single-line separator/tab. Raw model output (or
 * the raw user-message fallback) can carry newlines — which wrap into and corrupt
 * the separator bar — and markdown markers (`# heading`, fences, `- list`, emphasis)
 * which read as noise on one line. Flatten to a clean single line: strip the common
 * markdown markers, then collapse every whitespace run (incl. newlines/tabs) to a
 * single space. Order matters — line-anchored markers are removed BEFORE newlines
 * collapse, or the `^` anchors would no longer match.
 *
 * ADDITIVE HARDENING (functional change, superset — all prior behaviour preserved):
 * raw model output can also carry terminal-hostile bytes that are NOT whitespace and
 * NOT markdown, and so survived the old pass and corrupted/spoofed the separator bar:
 *   - ANSI/VT escape sequences (CSI colour/cursor codes, OSC title-setting) — stripped
 *     first so a `\x1b[2J` or colour run can't blank/recolour the status chrome.
 *   - C0/C1 control chars + DEL (NUL, BEL, backspace, …) — removed, EXCEPT the
 *     `\t`/`\n`/`\r` the whitespace/anchor passes below still need.
 *   - Invisible & bidirectional Unicode (zero-width space/joiner, BOM, LRM/RLM, the
 *     bidi embeddings/overrides/isolates U+202A–U+202E / U+2066–U+2069, line/para
 *     separators U+2028/U+2029) — removed so a title can't carry an invisible RLO
 *     "Trojan-source" reversal or zero-width noise.
 * These run BEFORE the markdown/line-anchored passes so a leading escape or control
 * byte can't shield a `#`/`-` marker from its `^` anchor.
 */
export function sanitizeTitle(s: string): string {
  return s
    // eslint-disable-next-line no-control-regex
    .replace(/\u001b\][^\u0007\u001b]*(?:\u0007|\u001b\\)/g, '') // OSC sequences (e.g. set-title)
    // eslint-disable-next-line no-control-regex
    .replace(/\u001b\[[0-?]*[ -/]*[@-~]/g, '')   // CSI escape sequences (colour/cursor)
    // eslint-disable-next-line no-control-regex
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f-\u009f]/g, '') // C0/C1 ctrl + DEL (keep \t\n\r)
    .replace(/[\u200b-\u200f\u202a-\u202e\u2060\u2066-\u2069\ufeff]/g, '') // zero-width / bidi / BOM (line-sep U+2028/9 fall to \s+ below)
    .replace(/```+[a-z0-9]*/gi, ' ')             // fenced code markers (+ optional lang)
    .replace(/`/g, '')                            // inline code backticks
    .replace(/^\s*#{1,6}\s+/gm, '')               // ATX headings (# .. ######)
    .replace(/^\s*>\s?/gm, '')                    // blockquote markers
    .replace(/^\s*(?:[-*+]|\d+\.)\s+/gm, '')      // unordered/ordered list markers
    .replace(/[*_]{1,3}(\S[^*_]*?)[*_]{1,3}/g, '$1') // bold/italic emphasis
    .replace(/\s+/g, ' ')                         // collapse all whitespace incl newlines
    .trim();
}

/**
 * Trim a flattened title to a maximum length (default 50). ADDITIVE HARDENING over a
 * bare `s.slice(0, max)`: when the hard cut would land mid-word and a word boundary
 * exists within the budget, break on that boundary instead so the title ends on a
 * whole word rather than a severed fragment. The boundary is only honoured when it
 * keeps most of the budget (>= 60% of `max`) — otherwise a single very long word
 * (e.g. a URL or hash) would collapse the title to almost nothing, so we hard-cut.
 * Any trailing space left by the cut is trimmed. A title already within `max` is
 * returned unchanged, so no previously-short title is altered.
 *
 * TRUNCATION AFFORDANCE (parity: the reference's `slice(0, MAX - 1) + '…'`,
 * initReplBridge.ts:566-567): when a cut actually happens, a horizontal ellipsis
 * `…` is appended so the user can tell the title continued (rather than a silent
 * mid-thought stop). One char of the budget is reserved for the ellipsis so the
 * returned string — ellipsis included — never exceeds `max`. A title within `max`
 * is still returned verbatim with NO ellipsis.
 */
export function truncateTitle(title: string, max = TITLE_MAX_LEN): string {
  if (title.length <= max) return title;
  // Reserve one char of the budget for the trailing ellipsis so the result
  // (ellipsis included) stays within `max`.
  const budget = max - 1;
  const clipped = title.slice(0, budget);
  const lastSpace = clipped.lastIndexOf(' ');
  const body = lastSpace >= Math.floor(max * 0.6)
    ? clipped.slice(0, lastSpace).trimEnd()
    : clipped.trimEnd();
  return `${body}…`;
}

/**
 * Synchronous, local placeholder/intent title derived from a raw user message —
 * NO model round-trip. Parity with the reference's `deriveTitle` (initReplBridge.ts
 * :549-569, L293-348): strip the system-injected display tags, take the FIRST
 * SENTENCE (`/^(.*?[.!?])\s/` — the intent; the rest is usually context/detail),
 * flatten to a single line, then run the shared {@link sanitizeTitle} /
 * {@link truncateTitle} (so control bytes/markdown never leak and the result ends
 * on a word boundary with an ellipsis affordance). Returns `undefined` when there
 * is nothing title-worthy left (a pure display-tag / command-only message) so the
 * caller can skip it rather than render an empty placeholder.
 *
 * Used as {@link generateTitle}'s FALLBACK — replacing the crude first-50-chars
 * `sanitizeTitle(cleaned).slice(…)` with first-sentence intent extraction — and
 * exported so a UI can show an instant placeholder before the LLM title resolves.
 */
export function deriveTitle(raw: string): string | undefined {
  const clean = stripDisplayTagsAllowEmpty(raw);
  // Capture group (not lookbehind) keeps the regex JIT-friendly and matches
  // the reference's deriveTitle exactly. No trailing `\s` after the terminator (e.g. a
  // message that IS a single sentence) → fall back to the whole cleaned text.
  const firstSentence = /^(.*?[.!?])\s/.exec(clean)?.[1] ?? clean;
  const sanitized = sanitizeTitle(firstSentence);
  if (!sanitized) return undefined;
  return truncateTitle(sanitized);
}

/**
 * Boundary check for model-produced title/branch text (parity: the reference's
 * schema-validated title that `safeParse`s to `null` on garbage,
 * utils/sessionTitle.ts:116-117). After sanitisation a value is USABLE only when
 * it carries real title content — at least one letter or digit. An empty result,
 * or one that is pure punctuation/symbols after sanitisation (model noise, a
 * stray fence, an emoji-only reply), is rejected so the caller falls back to the
 * user-message-derived title instead of surfacing the noise as the title.
 */
export function isUsableTitle(s: string): boolean {
  return /[\p{L}\p{N}]/u.test(s);
}

/**
 * Parse a structured title out of the model's raw output. Parity with the reference's
 * JSON-schema title path (utils/sessionTitle.ts:90-120 / generateSessionName.ts):
 * the model is asked for `{"title": "..."}` (or `{"name": "..."}` on the
 * kebab-case /rename path), and we read that field directly instead of
 * regex-scrubbing free-form prose. Tolerant like the reference's `safeParseJSON`:
 *   - parses a clean JSON object first;
 *   - else extracts the first `{...}` block so a model that wrapped the JSON in
 *     prose or ```json fences still parses;
 *   - `title` wins over `name`; both must be non-empty strings.
 * Returns `undefined` when there is no usable JSON object (bare string, array,
 * non-string field, empty, or non-JSON text) — the caller then falls back to the
 * free-form sanitize pipeline.
 */
export function parseTitleJson(raw: string): string | undefined {
  return extractJsonStringField(raw, ['title', 'name']);
}

/**
 * Shared, tolerant "read a single non-empty string field out of the model's raw
 * output" extractor used by BOTH the title path (`parseTitleJson`) and the
 * branch-name path (`parseBranchJson`) so there is ONE canonical JSON-field
 * reader (no semantic duplication). Tolerant like the reference's `safeParseJSON`:
 *   - parses a clean JSON object first;
 *   - else extracts the first `{...}` block so a model that wrapped the JSON in
 *     prose or ```json fences still parses;
 *   - `fields` are tried in order (first present, non-empty string wins).
 * Returns `undefined` when there is no usable JSON object (bare string, array,
 * non-string field, empty, or non-JSON text) — the caller then falls back to the
 * free-form sanitize pipeline.
 */
export function extractJsonStringField(raw: string, fields: readonly string[]): string | undefined {
  const text = raw.trim();
  if (!text) return undefined;
  const fromObject = (s: string): string | undefined => {
    let parsed: unknown;
    try { parsed = JSON.parse(s); } catch { return undefined; }
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return undefined;
    const rec = parsed as Record<string, unknown>;
    for (const field of fields) {
      const value = rec[field];
      if (typeof value === 'string' && value.trim().length > 0) return value.trim();
    }
    return undefined;
  };
  const direct = fromObject(text);
  if (direct !== undefined) return direct;
  const block = text.match(/\{[\s\S]*\}/);
  return block ? fromObject(block[0]) : undefined;
}

export interface AuxQueryFailure { reason: AuxQueryErrorReason; error?: string }

export interface AuxQueryOutcome {
  /** Raw concatenated model output (empty on an outer failure). */
  raw: string;
  /** Set when the stream produced an observable OUTER failure; undefined on a
   *  clean stream (a clean-but-unusable response is classified later as the
   *  two-tier `unparseable` inner failure by the caller). */
  failure?: AuxQueryFailure;
  /** Provider correlation id from the terminal chunk, when present. */
  messageId?: string;
}

/**
 * Run ONE deterministic auxiliary/background model query and return its raw output
 * plus a classified OUTER-failure record. Single canonical implementation shared by
 * {@link generateTitle} and {@link generateBranchName} (no per-site duplication of
 * the stream loop, abort-linking, or timeout). Replaces the old bare `catch {}`:
 * every outer failure is CLASSIFIED (timeout / external-abort / stream-error chunk /
 * exception) so the caller can surface it as an {@link AuxQueryResult} instead of
 * silently swallowing it.
 *
 * Abort/timeout wiring mirrors the previous behaviour: the caller's `signal` is
 * linked into an internal controller (already-aborted → abort now; later abort →
 * propagate; listener cleaned up in `finally`), and an internal timer
 * ({@link AUX_QUERY_TIMEOUT_MS} by default) bounds a wedged provider.
 *
 * `timeoutMs` lets a caller override that wall-clock cap; pass a non-positive /
 * non-finite value (e.g. `0`) to run UNTIMED. Compaction-summary generation uses
 * the untimed mode because a large-transcript summary legitimately runs longer
 * than the terse title/branch queries the 30s default was sized for.
 */
export async function runAuxQuery(
  provider: ProviderConfig,
  messages: Message[],
  streamImpl: typeof streamChatCompletion,
  signal: AbortSignal | undefined,
  timeoutMs: number = AUX_QUERY_TIMEOUT_MS,
): Promise<AuxQueryOutcome> {
  let raw = '';
  let failure: AuxQueryFailure | undefined;
  let messageId: string | undefined;
  let timedOut = false;
  const ac = new AbortController();
  const onExternalAbort = () => ac.abort();
  if (signal) {
    if (signal.aborted) ac.abort();
    else signal.addEventListener('abort', onExternalAbort, { once: true });
  }
  // Only arm the wall-clock guard for a finite, positive cap; a non-positive /
  // non-finite `timeoutMs` opts the query out of timing entirely (untimed).
  const timer = (Number.isFinite(timeoutMs) && timeoutMs > 0)
    ? setTimeout(() => { timedOut = true; ac.abort(); }, timeoutMs)
    : undefined;
  // Classify which abort source won — timeout takes precedence, then an external
  // caller abort, else a plain stream error / exception.
  const classify = (fallback: AuxQueryErrorReason): AuxQueryErrorReason =>
    timedOut ? 'timeout' : (signal?.aborted ? 'aborted' : fallback);
  try {
    for await (const chunk of streamImpl(provider, messages, [], ac.signal)) {
      if (chunk.type === 'content_delta' && chunk.text) raw += chunk.text;
      if (chunk.type === 'error') {
        failure = { reason: classify('stream_error'), error: (chunk as StreamChunk).error };
        break;
      }
      if (chunk.type === 'done') {
        messageId = extractProviderRequestId((chunk as StreamChunk).responseHeaders);
        break;
      }
    }
  } catch (e) {
    failure = { reason: classify('exception'), error: e instanceof Error ? e.message : String(e) };
  } finally {
    clearTimeout(timer);
    if (signal) signal.removeEventListener('abort', onExternalAbort);
  }
  return { raw, failure, messageId };
}
