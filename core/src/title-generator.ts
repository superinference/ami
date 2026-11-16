import type { ProviderConfig, Message } from './types';
import { streamChatCompletion } from './provider';
import { contentToText } from './session';

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

export async function generateTitle(
  userMessage: string,
  provider: ProviderConfig,
  compactionModel?: string,
  // Test seam (behaviour-preserving): defaults to the real streamer so callers
  // are unaffected; tests inject a deterministic async generator.
  _streamChatCompletion: typeof streamChatCompletion = streamChatCompletion,
): Promise<string> {
  const titleProvider: ProviderConfig = {
    ...provider,
    model: compactionModel || provider.model,
    maxTokens: 50,
  };

  const messages = [{
    role: 'user' as const,
    content: `Generate a concise title (max 50 chars, single line, no quotes, same language as the text) for this conversation:\n\n"${userMessage.slice(0, 500)}"`,
  }];

  let title = '';
  try {
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), 30000);
    try {
      for await (const chunk of _streamChatCompletion(titleProvider, messages, [], ac.signal)) {
        if (chunk.type === 'content_delta' && chunk.text) title += chunk.text;
        if (chunk.type === 'done' || chunk.type === 'error') break;
      }
    } finally {
      clearTimeout(timer);
    }
  } catch {}

  // Clean up: remove thinking tags (paired and unclosed), then strip surrounding
  // quotes, then flatten to a single markdown-free line (see sanitizeTitle) so it
  // never wraps/corrupts the separator bar or leaks raw markdown into the tab/title
  // chrome. ADDITIVE HARDENING (superset): trim BEFORE the quote strip so a trailing
  // `"<space>` is still unwrapped, strip repeated/nested quotes, and recognise the
  // smart/typographic and guillemet quote pairs a model often wraps the title in —
  // the old pass only removed a single leading/trailing straight quote.
  title = title
    .replace(/<think>[\s\S]*?<\/think>/g, '')
    .replace(/<think>[\s\S]*$/g, '')
    .trim()
    .replace(/^["'“”‘’«»]+|["'“”‘’«»]+$/g, '')
    .trim();
  title = sanitizeTitle(title);
  // Word-boundary-aware truncation; fall back to the sanitised user message when the
  // model produced nothing usable (same fallback as before, now also word-aware).
  return truncateTitle(title) || truncateTitle(sanitizeTitle(userMessage));
}
