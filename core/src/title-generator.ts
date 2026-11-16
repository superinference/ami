import type { ProviderConfig, Message } from './types';
import { streamChatCompletion } from './provider';
import { contentToText } from './session';
import { stripDisplayTags } from './utils/display-tags';

/**
 * A message is a valid title SOURCE only when it is genuinely human-authored.
 * Parity: Claude's title-source filter (initReplBridge `onUserMessage` /
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
  return contentToText(m.content).trim().length > 0;
}

/**
 * Pick the message to derive a session title from by scanning BACKWARD for the
 * most recent human-authored user message (parity: Claude's backward
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
 * count-based title refinement (parity: Claude re-derives the title at user
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
 * re-derivation (parity: Claude's `extractConversationText`). Includes every
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
  return parts.join('\n').trim();
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
 */
export function truncateTitle(title: string, max = 50): string {
  if (title.length <= max) return title;
  const clipped = title.slice(0, max);
  const lastSpace = clipped.lastIndexOf(' ');
  if (lastSpace >= Math.floor(max * 0.6)) {
    return clipped.slice(0, lastSpace).trimEnd();
  }
  return clipped.trimEnd();
}

/**
 * Title-generation prompt. Parity with Claude's `SESSION_TITLE_PROMPT`
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
 * Parse a structured title out of the model's raw output. Parity with Claude's
 * JSON-schema title path (utils/sessionTitle.ts:90-120 / generateSessionName.ts):
 * the model is asked for `{"title": "..."}` (or `{"name": "..."}` on the
 * kebab-case /rename path), and we read that field directly instead of
 * regex-scrubbing free-form prose. Tolerant like Claude's `safeParseJSON`:
 *   - parses a clean JSON object first;
 *   - else extracts the first `{...}` block so a model that wrapped the JSON in
 *     prose or ```json fences still parses;
 *   - `title` wins over `name`; both must be non-empty strings.
 * Returns `undefined` when there is no usable JSON object (bare string, array,
 * non-string field, empty, or non-JSON text) — the caller then falls back to the
 * free-form sanitize pipeline.
 */
export function parseTitleJson(raw: string): string | undefined {
  const text = raw.trim();
  if (!text) return undefined;
  const fromObject = (s: string): string | undefined => {
    let parsed: unknown;
    try { parsed = JSON.parse(s); } catch { return undefined; }
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return undefined;
    const rec = parsed as Record<string, unknown>;
    const value = rec.title ?? rec.name;
    if (typeof value !== 'string') return undefined;
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : undefined;
  };
  const direct = fromObject(text);
  if (direct !== undefined) return direct;
  const block = text.match(/\{[\s\S]*\}/);
  return block ? fromObject(block[0]) : undefined;
}

export async function generateTitle(
  userMessage: string,
  provider: ProviderConfig,
  compactionModel?: string,
  // Test seam (behaviour-preserving): defaults to the real streamer so callers
  // are unaffected; tests inject a deterministic async generator.
  _streamChatCompletion: typeof streamChatCompletion = streamChatCompletion,
  // External cancellation. Parity: Claude threads the caller's AbortSignal into
  // generateSessionName/generateSessionTitle (generateSessionName.ts:L10-13,
  // rename.ts:L37-40) so a user Esc / turn-abort / session-end cancels an
  // in-flight title generation. Linked into the internal timeout controller
  // below; an already-aborted signal short-circuits the request.
  signal?: AbortSignal,
): Promise<string> {
  const titleProvider: ProviderConfig = {
    ...provider,
    model: compactionModel || provider.model,
    maxTokens: 50,
    // Deterministic side-query. Parity: Claude forces `temperatureOverride: 0`
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

  const messages = [{
    role: 'user' as const,
    content: `${TITLE_GENERATION_PROMPT}\n\nConversation:\n"""\n${cleaned.slice(0, 1000)}\n"""`,
  }];

  let title = '';
  try {
    const ac = new AbortController();
    // Link the caller's signal: an already-aborted signal aborts immediately, a
    // later abort propagates, and the listener is cleaned up in `finally` so no
    // handler leaks onto a long-lived turn signal.
    const onExternalAbort = () => ac.abort();
    if (signal) {
      if (signal.aborted) ac.abort();
      else signal.addEventListener('abort', onExternalAbort, { once: true });
    }
    const timer = setTimeout(() => ac.abort(), 30000);
    try {
      for await (const chunk of _streamChatCompletion(titleProvider, messages, [], ac.signal)) {
        if (chunk.type === 'content_delta' && chunk.text) title += chunk.text;
        if (chunk.type === 'done' || chunk.type === 'error') break;
      }
    } finally {
      clearTimeout(timer);
      if (signal) signal.removeEventListener('abort', onExternalAbort);
    }
  } catch {}

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
  const stripped = title
    .replace(/<think>[\s\S]*?<\/think>/g, '')
    .replace(/<think>[\s\S]*$/g, '')
    .trim();
  const structured = parseTitleJson(stripped);
  title = (structured ?? stripped)
    .trim()
    .replace(/^["'“”‘’«»]+|["'“”‘’«»]+$/g, '')
    .trim();
  title = sanitizeTitle(title);
  // Word-boundary-aware truncation; fall back to the sanitised user message when the
  // model produced nothing usable (same fallback as before, now also word-aware).
  return truncateTitle(title) || truncateTitle(sanitizeTitle(cleaned));
}
