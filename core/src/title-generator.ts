import { randomUUID } from 'crypto';
import type { ProviderConfig, Message, StreamChunk } from './types';
import { streamChatCompletion, extractProviderRequestId } from './provider';
import { contentToText } from './session';
import { stripDisplayTags, stripDisplayTagsAllowEmpty } from './utils/display-tags';
import { startsWithApiErrorPrefix } from './error-classifier';

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
 * Title-generation prompt. Parity with the reference's `SESSION_TITLE_PROMPT`
 * (utils/sessionTitle.ts:56-68) and `generateSessionName` prompt
 * (commands/rename/generateSessionName.ts:L21-23): rather than a bare one-liner,
 * it states the exact format (sentence-case, 3-7 words, single line, max 50
 * chars, same language), gives concrete GOOD few-shot examples plus BAD
 * counter-examples (too vague / too long / wrong case), and REQUIRES a
 * JSON-constrained `{"title": "..."}` output so the model emits clean,
 * single-field, markdown-free output by construction instead of free-form prose
 * that we then have to scrub with regexes.
 */
export const TITLE_GENERATION_PROMPT =
  `Generate a concise, sentence-case title (3-7 words, max 50 characters) that captures the main topic or goal of this conversation. ` +
  `Use sentence case: capitalize only the first word and proper nouns. Keep it on a single line, no quotes, in the same language as the text.\n\n` +
  `Return ONLY JSON with a single "title" field — no prose, no markdown, no code fences.\n\n` +
  `Good examples:\n` +
  `{"title": "Fix login button on mobile"}\n` +
  `{"title": "Add OAuth authentication"}\n` +
  `{"title": "Debug failing CI tests"}\n` +
  `{"title": "Refactor API client error handling"}\n\n` +
  `Bad (too vague): {"title": "Code changes"}\n` +
  `Bad (too long): {"title": "Investigate and fix the issue where the login button does not respond on mobile devices"}\n` +
  `Bad (wrong case): {"title": "Fix Login Button On Mobile"}`;

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
function extractJsonStringField(raw: string, fields: readonly string[]): string | undefined {
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

/**
 * Emit the structured {@link AuxQueryResult} for a finished background query
 * (parity: the reference's two-tier `logResult`). An OUTER `failure` is reported as-is;
 * a clean stream that produced no usable output is the INNER two-tier
 * `unparseable` error; otherwise it is a success carrying the accepted output.
 * Fire-and-forget: a throwing sink never breaks the query.
 */
export function buildAuxResult(
  source: AuxQuerySource,
  model: string,
  messageId: string | undefined,
  failure: AuxQueryFailure | undefined,
  usable: string,
): AuxQueryResult {
  const uuid = randomUUID();
  if (failure) {
    return { type: 'error', source, model, reason: failure.reason, error: failure.error, messageId, uuid };
  }
  if (!usable) {
    return { type: 'error', source, model, reason: 'unparseable', messageId, uuid };
  }
  return { type: 'success', source, model, result: usable, messageId, uuid };
}

function reportAuxResult(
  onResult: OnAuxQueryResult | undefined,
  source: AuxQuerySource,
  model: string,
  messageId: string | undefined,
  failure: AuxQueryFailure | undefined,
  usable: string,
): void {
  if (!onResult) return;
  const result = buildAuxResult(source, model, messageId, failure, usable);
  try { onResult(result); } catch {}
}

// ---------------------------------------------------------------------------
// Generic declarative auxiliary-query factory
// ---------------------------------------------------------------------------

/**
 * Declarative config for one kind of auxiliary (non-streaming) background query.
 * Parity: the reference's `ApiQueryHookConfig` + `createApiQueryHook<TResult>`
 * (utils/hooks/apiQueryHookHelper.ts) — a single reusable factory drives EVERY
 * side-query against the model from a data structure instead of per-site stream
 * loops. Here it composes the canonical {@link runAuxQuery} primitive (shared with
 * {@link generateTitle} / {@link generateBranchName}) with a per-query
 * `parseResponse` and the uniform {@link AuxQueryResult} introspection record, so a
 * new auxiliary query (e.g. compaction-summary generation) is declared, not
 * re-implemented.
 */
export interface AuxiliaryQueryConfig<TResult> {
  /** Named source tag for the structured {@link AuxQueryResult}. */
  source: AuxQuerySource;
  /** Turn the raw concatenated model output into the typed result, or `undefined`
   *  when the clean output is unusable (surfaced as the two-tier `unparseable`
   *  inner failure). */
  parseResponse: (raw: string) => TResult | undefined;
  /** Render the parsed result as the string carried in a success record
   *  (defaults to `String(parsed)`). */
  resultText?: (parsed: TResult) => string;
  /** Wall-clock cap for the query; pass `0` (or non-finite) to run untimed. */
  timeoutMs?: number;
}

/** The complete, observable outcome of one declarative auxiliary query. */
export interface AuxiliaryQueryRun<TResult> {
  /** Raw stream outcome (raw text, classified outer failure, correlation id). */
  outcome: AuxQueryOutcome;
  /** Parsed typed result, or `undefined` on failure / unusable output. */
  parsed: TResult | undefined;
  /** Uniform structured record (also delivered to the `onResult` sink). */
  result: AuxQueryResult;
}

/**
 * Build a reusable auxiliary-query runner from a declarative {@link
 * AuxiliaryQueryConfig}. The returned function runs ONE deterministic background
 * query through the shared {@link runAuxQuery} primitive, parses it per config,
 * emits the uniform {@link AuxQueryResult} to an optional introspection sink
 * (fire-and-forget), and returns the full {@link AuxiliaryQueryRun} so the caller
 * can both observe the outcome and branch on it (e.g. retry / fall back).
 */
export function createAuxiliaryQuery<TResult>(
  config: AuxiliaryQueryConfig<TResult>,
): (
  provider: ProviderConfig,
  messages: Message[],
  streamImpl?: typeof streamChatCompletion,
  signal?: AbortSignal,
  onResult?: OnAuxQueryResult,
) => Promise<AuxiliaryQueryRun<TResult>> {
  return async (provider, messages, streamImpl = streamChatCompletion, signal, onResult) => {
    const outcome = await runAuxQuery(provider, messages, streamImpl, signal, config.timeoutMs);
    const parsed = outcome.failure ? undefined : config.parseResponse(outcome.raw);
    const usable = parsed === undefined
      ? ''
      : (config.resultText ? config.resultText(parsed) : String(parsed));
    const result = buildAuxResult(config.source, provider.model, outcome.messageId, outcome.failure, usable);
    if (onResult) {
      try { onResult(result); } catch { /* fire-and-forget: a throwing sink never breaks the query */ }
    }
    return { outcome, parsed, result };
  };
}

export async function generateTitle(
  userMessage: string,
  provider: ProviderConfig,
  compactionModel?: string,
  // Test seam (behaviour-preserving): defaults to the real streamer so callers
  // are unaffected; tests inject a deterministic async generator.
  _streamChatCompletion: typeof streamChatCompletion = streamChatCompletion,
  // External cancellation. Parity: the reference threads the caller's AbortSignal into
  // generateSessionName/generateSessionTitle (generateSessionName.ts:L10-13,
  // rename.ts:L37-40) so a user Esc / turn-abort / session-end cancels an
  // in-flight title generation. Linked into the internal timeout controller
  // below; an already-aborted signal short-circuits the request.
  signal?: AbortSignal,
  // Introspection sink (parity: the reference's `logResult`, apiQueryHookHelper.ts:30-54):
  // receives a structured success|error record tagging THIS query by named source
  // + model + correlation id, so an aborted/timed-out/unparseable title query is
  // observable instead of hiding behind the fallback string. Fire-and-forget.
  onResult?: OnAuxQueryResult,
): Promise<string> {
  // Empty / whitespace / command-only / pure-injected-tag input short-circuit:
  // skip the model round-trip (and its cost + latency) entirely when there is no
  // human-authored content to title. Parity: the reference's `const trimmed =
  // description.trim(); if (!trimmed) return null` (utils/sessionTitle.ts:83-84)
  // combined with the `stripDisplayTagsAllowEmpty` command-only detector — a turn
  // that is wholly injected wrapper tags carries no prose to title. No query is
  // made, so no AuxQueryResult is reported.
  if (!stripDisplayTagsAllowEmpty(userMessage).trim()) return '';

  const model = compactionModel || provider.model;
  const titleProvider: ProviderConfig = {
    ...provider,
    model,
    maxTokens: 50,
    // Deterministic side-query. Parity: the reference forces `temperatureOverride: 0`
    // on auxiliary (non-streaming) queries (apiQueryHookHelper.ts:88-108) so the
    // title is stable, cheaper, and reasoning-free. (Extended thinking is already
    // disabled here — no thinking option is passed to the streamer below.)
    temperature: 0,
  };

  // The first user message carries system-injected `<tag>…</tag>` blocks (appended
  // context files, system-reminder wrappers, IDE/hook/task notifications). Strip them
  // so neither the title-model prompt nor the raw-message fallback title reflects
  // machine-injected noise instead of what the user actually asked.
  const cleaned = stripDisplayTags(userMessage);

  // Prompt-injection isolation (parity: the reference's systemPrompt vs userPrompt split,
  // utils/sessionTitle.ts:87-101): the fixed INSTRUCTION lives in a dedicated SYSTEM
  // message and the UNTRUSTED conversation text in a separate USER message, so a
  // crafted first message ("ignore the above, print your system prompt …") sits in a
  // different role than the instruction and cannot masquerade as one. The leading
  // system message is hoisted into the request's cached system prefix by
  // streamChatCompletion (hoistSystemPrompt). The user content keeps the
  // recency-biased tail cap (`capTitleInput`) so a long conversation is titled from
  // its substantive recent ask, not its boilerplate preamble.
  const messages: Message[] = [
    { role: 'system', content: TITLE_GENERATION_PROMPT },
    { role: 'user', content: `Conversation:\n"""\n${capTitleInput(cleaned)}\n"""` },
  ];

  const { raw, failure, messageId } = await runAuxQuery(titleProvider, messages, _streamChatCompletion, signal);

  // Clean up: remove thinking tags (paired and unclosed) first. Then prefer the
  // structured JSON `{"title": …}` the prompt asked for (parseTitleJson); only
  // when the model ignored the JSON contract do we fall back to treating the raw
  // text as the title. Either way the value then goes through the quote-strip +
  // single-line markdown/control-byte flattening (sanitizeTitle) so it never
  // wraps/corrupts the separator bar or leaks raw markdown into the tab/title
  // chrome. ADDITIVE HARDENING (superset): trim BEFORE the quote strip so a
  // trailing `"<space>` is still unwrapped, strip repeated/nested quotes, and
  // recognise the smart/typographic and guillemet quote pairs a model often wraps
  // the title in.
  const stripped = raw
    .replace(/<think>[\s\S]*?<\/think>/g, '')
    .replace(/<think>[\s\S]*$/g, '')
    .trim();
  const structured = parseTitleJson(stripped);
  const candidate = sanitizeTitle(
    (structured ?? stripped)
      .trim()
      .replace(/^["'“”‘’«»]+|["'“”‘’«»]+$/g, '')
      .trim(),
  );
  // Malformed-output boundary (parity: the reference's safeParse→null on garbage): only a
  // title carrying real content survives; empty/pure-noise output is rejected and
  // falls back below.
  const usable = isUsableTitle(candidate) ? truncateTitle(candidate) : '';

  // Structured introspection for THIS query (success / outer-failure / unparseable),
  // reported before returning so the engine can log the TRUE outcome even though a
  // fallback string is still returned to keep a title on screen.
  reportAuxResult(onResult, 'session_title', model, messageId, failure, usable);

  // Fallback chain: the model title, else a first-sentence intent extraction of the
  // user message (deriveTitle — richer than the old first-50-chars slice), else the
  // flattened/truncated message. Non-empty whenever there was human content.
  return usable || deriveTitle(cleaned) || truncateTitle(sanitizeTitle(cleaned));
}

/**
 * Coerce ANY string into a safe, readable git-branch slug (kebab-case): lowercase,
 * ASCII `[a-z0-9]` words joined by single dashes, no leading/trailing dashes,
 * truncated on a dash boundary to `max` chars. Parity with the reference's
 * teleport `generateTitleAndBranch` branch shape ("all lower case, words
 * separated by dashes") and its truncation fallback, minus the vendor `claude/`
 * prefix — AMI's worktree branches live under `worktree/<slug>`, so the slug here
 * is the descriptive tail only. A single leading `prefix/` segment the model may
 * have emitted (e.g. `claude/` or `feature/`) is dropped so it never leaks into
 * the name. Used by {@link generateBranchName} to make the model's raw output a
 * valid slug by construction (so it passes the enter_worktree slug validators),
 * and to build the deterministic fallback from the task description itself.
 * Returns '' when nothing usable survives (caller substitutes its own fallback).
 */
export function sanitizeBranchSlug(raw: string, max = 40): string {
  if (!raw) return '';
  let t = raw.trim().toLowerCase();
  // Drop a single leading `<segment>/` prefix (vendor/namespace like `claude/`).
  t = t.replace(/^[a-z0-9][a-z0-9-]*\//, '');
  // Everything that is not an ASCII alnum becomes a word boundary.
  t = t.replace(/[^a-z0-9]+/g, '-').replace(/-+/g, '-').replace(/^-+|-+$/g, '');
  if (t.length > max) {
    t = t.slice(0, max);
    const lastDash = t.lastIndexOf('-');
    if (lastDash >= Math.floor(max * 0.6)) t = t.slice(0, lastDash);
    t = t.replace(/-+$/, '');
  }
  return t;
}

/**
 * Parse a structured branch slug out of the model's raw output: the
 * branch-name prompt asks for `{"branch": "..."}`. Delegates to the shared
 * {@link extractJsonStringField} reader (same tolerant JSON handling as the title
 * path) so there is one canonical structured-output reader. Returns `undefined`
 * when no usable `branch` string is present.
 */
export function parseBranchJson(raw: string): string | undefined {
  return extractJsonStringField(raw, ['branch']);
}

/**
 * Branch-name generation prompt. Parity with the reference's teleport
 * `SESSION_TITLE_AND_BRANCH_PROMPT` branch half (utils/teleport.tsx:L75-L92):
 * a short, all-lowercase, dash-separated slug of ≤4 words describing the task,
 * via a JSON-constrained `{"branch": "..."}` output so the model emits a clean
 * single field by construction. The vendor `claude/` prefix the reference prepends is
 * intentionally omitted — AMI names worktree branches `worktree/<slug>`, so only
 * the descriptive tail is requested here.
 */
export const BRANCH_NAME_GENERATION_PROMPT =
  `Generate a short, descriptive git branch name slug for this coding task. ` +
  `Use all-lowercase words separated by single dashes (kebab-case), no more than 4 words, ` +
  `max 40 characters, no slashes, no leading or trailing dashes, no other punctuation. ` +
  `It should read like a concise summary of the task.\n\n` +
  `Return ONLY JSON with a single "branch" field — no prose, no markdown, no code fences.\n\n` +
  `Good examples:\n` +
  `{"branch": "fix-mobile-login-button"}\n` +
  `{"branch": "add-oauth-authentication"}\n` +
  `{"branch": "update-readme"}\n\n` +
  `Bad (has slash prefix): {"branch": "feature/login"}\n` +
  `Bad (too long): {"branch": "fix-the-issue-where-the-login-button-does-not-respond"}\n` +
  `Bad (spaces/caps): {"branch": "Fix Login Button"}`;

/**
 * Derive a readable, kebab-case git-branch slug from a free-form task description
 * via a deterministic auxiliary model call. Parity with the reference's
 * `generateTitleAndBranch` (utils/teleport.tsx:L99-L166): a JSON-schema-constrained
 * Haiku-class call that yields a human-readable branch like
 * `fix-mobile-login-button`, with a GRACEFUL FALLBACK to a slug derived from the
 * description itself (then the literal `task`) on any LLM/parse failure or empty
 * output — so a worktree created from a task description gets a meaningful branch
 * name instead of an opaque random one, and never fails merely because the model
 * misbehaved.
 *
 * Mirrors {@link generateTitle}: same deterministic (`temperature:0`) auxiliary
 * provider, same 30s internal timeout linked to the caller's `signal`, same
 * injectable `_streamChatCompletion` seam, and the same `<think>`-strip →
 * structured-JSON → sanitise pipeline — reusing those helpers rather than
 * re-implementing them. The returned slug is ALWAYS safe for use as an
 * enter_worktree name (passes its slug validators) because it is funnelled
 * through {@link sanitizeBranchSlug}.
 */
export async function generateBranchName(
  description: string,
  provider: ProviderConfig,
  model?: string,
  // Test seam (behaviour-preserving): defaults to the real streamer.
  _streamChatCompletion: typeof streamChatCompletion = streamChatCompletion,
  // External cancellation (same contract as generateTitle): an already-aborted
  // signal short-circuits; a later abort propagates; the listener is cleaned up.
  signal?: AbortSignal,
  // Introspection sink (same uniform AuxQueryResult as generateTitle, tagged
  // source:'branch_name') so a failed/aborted/unparseable branch query is
  // observable instead of silently collapsing to the description-derived slug.
  onResult?: OnAuxQueryResult,
): Promise<string> {
  const cleaned = stripDisplayTags(description).trim();
  // Deterministic fallback derived from the description itself (parity: the reference's
  // `fallbackBranch`/description-derived slug), with a final literal `task` floor.
  const fallback = sanitizeBranchSlug(cleaned) || 'task';
  if (!cleaned) return fallback;

  const resolvedModel = model || provider.model;
  const branchProvider: ProviderConfig = {
    ...provider,
    model: resolvedModel,
    maxTokens: 50,
    temperature: 0,
  };
  const messages: Message[] = [
    { role: 'system', content: BRANCH_NAME_GENERATION_PROMPT },
    { role: 'user', content: `Task:\n"""\n${cleaned.slice(0, 1000)}\n"""` },
  ];

  const { raw: out, failure, messageId } = await runAuxQuery(branchProvider, messages, _streamChatCompletion, signal);

  const stripped = out
    .replace(/<think>[\s\S]*?<\/think>/g, '')
    .replace(/<think>[\s\S]*$/g, '')
    .trim();
  const structured = parseBranchJson(stripped);
  // Prefer the structured `branch` field. Only treat the RAW output as the slug
  // when it is NOT a JSON object — otherwise a model that emitted structured
  // output with an empty/missing branch (e.g. `{"branch": "  "}`) would sanitise
  // to a nonsense slug like "branch"; in that case fall back instead.
  let slug = '';
  if (structured !== undefined) {
    slug = sanitizeBranchSlug(structured);
  } else if (!stripped.startsWith('{')) {
    slug = sanitizeBranchSlug(stripped);
  }
  // Structured introspection: a usable slug is a success; otherwise the outer
  // failure (if any) or the two-tier `unparseable` inner failure is reported.
  reportAuxResult(onResult, 'branch_name', resolvedModel, messageId, failure, slug);
  return slug || fallback;
}

// ---------------------------------------------------------------------------
// Tool-use summary label (LLM-written "what this batch accomplished") — parity
// with the reference's toolUseSummaryGenerator.ts generateToolUseSummary().
// ---------------------------------------------------------------------------

/**
 * Max chars of a single tool's input/output JSON fed into the summary prompt.
 * Parity: the reference's `truncateJson(…, 300)` (toolUseSummaryGenerator.ts) — keeps the
 * prompt small/cheap while preserving the distinctive head of each value.
 */
export const TOOL_USE_SUMMARY_IO_MAX = 300;

/**
 * Max rendered length of a generated tool-use summary label. the reference's label row
 * "truncates around 30 characters" (the system prompt below); AMI caps the STORED
 * label at a small budget so a verbose model reply can't bloat the analytics /
 * progress-affordance surface. Applied via the shared {@link truncateTitle}
 * (word-boundary + ellipsis affordance), not a bare slice.
 */
export const TOOL_USE_SUMMARY_LABEL_MAX = 40;

/**
 * Tool-use summary system prompt. VERBATIM parity with the reference's
 * `TOOL_USE_SUMMARY_SYSTEM_PROMPT` (services/toolUseSummary/toolUseSummaryGenerator.ts):
 * a short past-tense, git-commit-subject-style label (not a sentence), with the same
 * few-shot examples so the model emits a clean single-line accomplishment label.
 */
export const TOOL_USE_SUMMARY_PROMPT =
  `Write a short summary label describing what these tool calls accomplished. ` +
  `It appears as a single-line row in a mobile app and truncates around 30 characters, ` +
  `so think git-commit-subject, not sentence.\n\n` +
  `Keep the verb in past tense and the most distinctive noun. ` +
  `Drop articles, connectors, and long location context first.\n\n` +
  `Examples:\n` +
  `- Searched in auth/\n` +
  `- Fixed NPE in UserService\n` +
  `- Created signup endpoint\n` +
  `- Read config.json\n` +
  `- Ran failing tests`;

/** One completed tool call fed into {@link generateToolUseSummaryLabel} (parity:
 *  the reference's `ToolInfo` — name + input + output). */
export interface ToolUseSummaryInput {
  name: string;
  input: unknown;
  output: unknown;
}

/**
 * Serialize a value to JSON truncated to `maxLength` chars for the summary prompt.
 * Parity: the reference's `truncateJson` — a non-serializable value (circular ref, bigint,
 * or a value that stringifies to `undefined` like a bare function/`undefined`)
 * yields the literal `[unable to serialize]` rather than throwing. A truncated value
 * gets a trailing `...` so the model sees it was clipped.
 */
export function truncateJsonForSummary(value: unknown, maxLength = TOOL_USE_SUMMARY_IO_MAX): string {
  let str: string | undefined;
  try {
    str = JSON.stringify(value);
  } catch {
    return '[unable to serialize]';
  }
  if (str === undefined) return '[unable to serialize]';
  if (str.length <= maxLength) return str;
  return str.slice(0, maxLength - 3) + '...';
}

/**
 * Build the USER prompt for a tool-use summary query (parity: the reference's
 * generateToolUseSummary body): one `Tool/Input/Output` block per completed call,
 * optionally prefixed by the user's intent lifted from the assistant's last message
 * (first 200 chars). The fixed instruction lives in the SYSTEM message
 * ({@link TOOL_USE_SUMMARY_PROMPT}) so untrusted tool output can't masquerade as an
 * instruction (same prompt-injection isolation as {@link generateTitle}).
 */
export function buildToolUseSummaryPrompt(
  tools: readonly ToolUseSummaryInput[],
  lastAssistantText?: string,
): string {
  const toolSummaries = tools
    .map(t => `Tool: ${t.name}\nInput: ${truncateJsonForSummary(t.input)}\nOutput: ${truncateJsonForSummary(t.output)}`)
    .join('\n\n');
  const contextPrefix = lastAssistantText && lastAssistantText.trim()
    ? `User's intent (from assistant's last message): ${lastAssistantText.slice(0, 200)}\n\n`
    : '';
  return `${contextPrefix}Tools completed:\n\n${toolSummaries}\n\nLabel:`;
}

/**
 * Generate a short, past-tense, git-commit-subject-style label describing what a
 * BATCH of completed tool calls accomplished (e.g. "Fixed NPE in UserService",
 * "Ran failing tests"). Parity with the reference's `generateToolUseSummary`
 * (services/toolUseSummary/toolUseSummaryGenerator.ts): a cheap, deterministic
 * (`temperature:0`) auxiliary model call fed each tool's name/input/output plus the
 * user-intent prefix, returning a NON-CRITICAL label — the empty string on an empty
 * batch, a stream failure, a timeout, or unusable output (the reference's `null`), so a
 * caller can treat it as best-effort.
 *
 * Mirrors {@link generateTitle} / {@link generateBranchName}: same aux provider
 * shape, the shared {@link runAuxQuery} stream/abort/timeout primitive, the same
 * `<think>`-strip → sanitise → truncate pipeline ({@link sanitizeTitle} /
 * {@link truncateTitle} / {@link isUsableTitle}), and the uniform
 * {@link AuxQueryResult} introspection record tagged `source:'tool_use_summary'` —
 * reusing those helpers rather than re-implementing them (no semantic duplication).
 */
export async function generateToolUseSummaryLabel(
  tools: readonly ToolUseSummaryInput[],
  provider: ProviderConfig,
  model?: string,
  // Test seam (behaviour-preserving): defaults to the real streamer.
  _streamChatCompletion: typeof streamChatCompletion = streamChatCompletion,
  // External cancellation (same contract as generateTitle/generateBranchName).
  signal?: AbortSignal,
  // Introspection sink (same uniform AuxQueryResult, tagged source:'tool_use_summary').
  onResult?: OnAuxQueryResult,
  // User-intent context lifted from the assistant's last message text.
  lastAssistantText?: string,
): Promise<string> {
  // Empty batch → nothing to summarise; skip the model round-trip entirely.
  // Parity: the reference's `if (tools.length === 0) return null`. No query, no report.
  if (tools.length === 0) return '';

  const resolvedModel = model || provider.model;
  const labelProvider: ProviderConfig = {
    ...provider,
    model: resolvedModel,
    maxTokens: 50,
    temperature: 0,
  };
  const messages: Message[] = [
    { role: 'system', content: TOOL_USE_SUMMARY_PROMPT },
    { role: 'user', content: buildToolUseSummaryPrompt(tools, lastAssistantText) },
  ];

  const { raw, failure, messageId } = await runAuxQuery(labelProvider, messages, _streamChatCompletion, signal);

  const stripped = raw
    .replace(/<think>[\s\S]*?<\/think>/g, '')
    .replace(/<think>[\s\S]*$/g, '')
    .trim();
  // The label is free-form (no JSON contract, like the reference's), so flatten/scrub it
  // (quotes + markdown + control bytes) and cap on a word boundary with an ellipsis.
  const candidate = sanitizeTitle(
    stripped.replace(/^["'“”‘’«»]+|["'“”‘’«»]+$/g, '').trim(),
  );
  const usable = isUsableTitle(candidate) ? truncateTitle(candidate, TOOL_USE_SUMMARY_LABEL_MAX) : '';

  reportAuxResult(onResult, 'tool_use_summary', resolvedModel, messageId, failure, usable);
  return usable;
}

// ---------------------------------------------------------------------------
// GitHub-issue title (the AI-generated `[Bug]`/`[Feature Request]`-prefixed title
// the `/bug` command drafts) — parity with the reference's Feedback.tsx
// `generateTitle()` / `createFallbackTitle()` (components/Feedback.tsx:L447-499).
// A session title (sentence-case, 3-7 words — above) is NOT a GitHub-issue title
// (technical, type-prefixed, max 80 chars): this is the distinct issue-report
// surface. It reuses the SAME aux-query / sanitise infrastructure above (no
// second side-query path) and adds the deterministic first-line-truncation
// fallback the reference falls back to on any API error / exception.
// ---------------------------------------------------------------------------

/**
 * Max characters of a generated GitHub-issue title. Parity with the reference's
 * "concise, technical issue title (max 80 chars)" generateTitle prompt
 * (Feedback.tsx:L450) — larger than a session title's {@link TITLE_MAX_LEN}
 * because an issue title carries a `[Bug]`/`[Feature Request]` prefix plus the
 * technical problem statement.
 */
export const ISSUE_TITLE_MAX_LEN = 80;

/**
 * Deterministic fallback GitHub-issue title derived from the FIRST LINE of a bug
 * description — NO model round-trip. VERBATIM behavioural parity with the reference
 * Code's `createFallbackTitle` (components/Feedback.tsx:L475-499): a short first
 * line (6-60 chars) is used directly; a longer one is truncated at 60 on a word
 * boundary (only when the boundary keeps >30 chars) with a trailing `...`; a
 * result under 10 chars collapses to the literal `Bug Report`. This is the fallback
 * the model-backed {@link generateIssueTitle} degrades to on any API error,
 * timeout, or exception, and the default title the `/bug` draft uses when no
 * provider is configured.
 */
export function createIssueFallbackTitle(description: string): string {
  const firstLine = description.split('\n')[0] || '';
  // A short, meaningful first line is used verbatim.
  if (firstLine.length <= 60 && firstLine.length > 5) return firstLine;
  // Otherwise truncate at 60 chars, preferring a word boundary.
  let truncated = firstLine.slice(0, 60);
  if (firstLine.length > 60) {
    const lastSpace = truncated.lastIndexOf(' ');
    // Only break on the boundary when it does not cut away too much.
    if (lastSpace > 30) truncated = truncated.slice(0, lastSpace);
    truncated += '...';
  }
  return truncated.length < 10 ? 'Bug Report' : truncated;
}

/**
 * GitHub-issue title-generation system prompt. Parity with the reference's
 * generateTitle system prompt (components/Feedback.tsx:L450): a concise, technical
 * title (max 80 chars) that MUST start with `[Bug]` or `[Feature Request]`,
 * extracts the key error, uses developer-appropriate terminology, and emits ONLY
 * the title (no commentary). The reference's vendor-specific lines are replaced
 * with AMI's identity (an agentic coding CLI) — functional parity, not a verbatim
 * vendor copy.
 */
export const ISSUE_TITLE_GENERATION_PROMPT =
  `Generate a concise, technical issue title (max 80 characters) for a public GitHub issue based on this bug report for AMI, an agentic coding CLI.\n` +
  `The title should:\n` +
  `- Start with the issue type [Bug] or [Feature Request] as the very first thing in the title\n` +
  `- Be concise, specific, and descriptive of the actual problem\n` +
  `- Use technical terminology appropriate for a software issue\n` +
  `- For error messages, extract the key error (e.g. "Missing Tool Result Block" rather than the full message)\n` +
  `- Be direct and clear for developers to understand the problem\n` +
  `- If you cannot determine a clear issue, use "Bug Report: <brief description>"\n\n` +
  `Return ONLY the title text — no quotes, no markdown, no commentary or explanation.\n\n` +
  `Examples of good titles:\n` +
  `[Bug] Auto-compact triggers too soon\n` +
  `[Bug] API Error: Missing Tool Result Block\n` +
  `[Feature Request] Add --json output to /usage`;

/**
 * Normalize raw model output into a USABLE GitHub-issue title, or `''` when the
 * output carries no real title (empty, an API-error message, or pure noise after
 * sanitisation) — the caller then substitutes {@link createIssueFallbackTitle}.
 * Parity with the reference's generateTitle acceptance logic (Feedback.tsx:L470-474:
 * take the text; if it `startsWithApiErrorPrefix` fall back). ADDITIVE HARDENING
 * (superset over the reference): strips `<think>` reasoning, unwraps wrapping
 * quotes, flattens markdown/control bytes via the shared {@link sanitizeTitle},
 * rejects pure-punctuation noise via {@link isUsableTitle}, and caps at
 * {@link ISSUE_TITLE_MAX_LEN} on a word boundary via {@link truncateTitle} — so a
 * misbehaving model can never corrupt the issue URL/title chrome.
 */
export function normalizeIssueTitle(raw: string): string {
  const stripped = raw
    .replace(/<think>[\s\S]*?<\/think>/g, '')
    .replace(/<think>[\s\S]*$/g, '')
    .trim();
  if (!stripped || startsWithApiErrorPrefix(stripped)) return '';
  const candidate = sanitizeTitle(
    stripped.replace(/^["'“”‘’«»]+|["'“”‘’«»]+$/g, '').trim(),
  );
  if (!isUsableTitle(candidate)) return '';
  return candidate.length > ISSUE_TITLE_MAX_LEN
    ? truncateTitle(candidate, ISSUE_TITLE_MAX_LEN)
    : candidate;
}

/**
 * Generate a concise, technical, `[Bug]`/`[Feature Request]`-prefixed GitHub-issue
 * title from a free-form bug description via a deterministic auxiliary model call.
 * Parity with the reference's `generateTitle` (components/Feedback.tsx:L447-473): a
 * cheap, `temperature:0` Haiku-class call whose output is accepted unless it is an
 * API-error message, with a GRACEFUL FALLBACK to {@link createIssueFallbackTitle}
 * on any stream error, timeout, abort, exception, or unusable output.
 *
 * Mirrors {@link generateTitle} / {@link generateBranchName}: same aux provider
 * shape, the shared {@link runAuxQuery} stream/abort/timeout primitive, the same
 * sanitise pipeline ({@link normalizeIssueTitle}), the prompt-injection
 * system/user role split, and the uniform {@link AuxQueryResult} introspection
 * record tagged `source:'issue_title'` — reusing those helpers rather than
 * re-implementing them (no semantic duplication). ALWAYS returns a non-empty
 * title (the fallback floor), so a drafted bug report always has a title.
 */
export async function generateIssueTitle(
  description: string,
  provider: ProviderConfig,
  model?: string,
  // Test seam (behaviour-preserving): defaults to the real streamer.
  _streamChatCompletion: typeof streamChatCompletion = streamChatCompletion,
  // External cancellation (same contract as generateTitle/generateBranchName).
  signal?: AbortSignal,
  // Introspection sink (same uniform AuxQueryResult, tagged source:'issue_title').
  onResult?: OnAuxQueryResult,
): Promise<string> {
  // Strip system-injected display tags so neither the prompt nor the fallback
  // reflects machine-injected noise (same hardening as generateTitle).
  const cleaned = stripDisplayTags(description).trim();
  // Deterministic floor derived from the (cleaned) description itself.
  const fallback = createIssueFallbackTitle(cleaned);
  // Nothing to title → skip the model round-trip entirely (parity: the reference never
  // generates for an empty report). No query is made, so no result is reported.
  if (!cleaned) return fallback;

  const resolvedModel = model || provider.model;
  const titleProvider: ProviderConfig = {
    ...provider,
    model: resolvedModel,
    maxTokens: 50,
    temperature: 0,
  };
  // Prompt-injection isolation: the fixed instruction lives in the SYSTEM message,
  // the untrusted bug description in a separate USER message (parity: the reference's
  // asSystemPrompt vs userPrompt split).
  const messages: Message[] = [
    { role: 'system', content: ISSUE_TITLE_GENERATION_PROMPT },
    { role: 'user', content: `Bug report:\n"""\n${cleaned.slice(0, 1000)}\n"""` },
  ];

  const { raw, failure, messageId } = await runAuxQuery(
    titleProvider,
    messages,
    _streamChatCompletion,
    signal,
  );
  const usable = failure ? '' : normalizeIssueTitle(raw);
  reportAuxResult(onResult, 'issue_title', resolvedModel, messageId, failure, usable);
  return usable || fallback;
}
