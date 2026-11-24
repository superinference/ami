// Split from session.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { Message, UserMessage } from './types';
import { stripDisplayTags, extractTag } from './utils/display-tags';
import { isEnvTruthy } from './analytics';

/**
 * How many of the most-recent sessions in a listing are ENRICHED with a full parse
 * (real first/last-prompt preview + message count) before the rest fall back to the
 * cheap {@link SessionManager.readSessionMetaLite} path. The picker shows the top N
 * fully, and older rows get a lite (title/date-only) entry until opened — so a store
 * with thousands of sessions parses ~N files instead of all of them. Parity: the reference's
 * `INITIAL_ENRICH_COUNT` resume-picker enrichment default.
 */
export const INITIAL_ENRICH_COUNT = 50;

/**
 * Session/transcript persistence is disabled when the user opts out: the
 * reference honours CLAUDE_CODE_SKIP_PROMPT_HISTORY; we add AMI_SKIP_SESSION_HISTORY.
 * When disabled, save() is a no-op so no transcript is written to disk (privacy /
 * ephemeral-run support). Callers (CLI flag, cleanupPeriodDays===0) can also pass
 * an explicit override.
 */
export function isSessionPersistenceDisabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env.AMI_SKIP_SESSION_HISTORY) || isEnvTruthy(env.CLAUDE_CODE_SKIP_PROMPT_HISTORY);
}

/**
 * Whether the session picker's DEFAULT scope is "all projects" — every project's
 * session store under the global projects root ({@link SessionManager.listAcrossProjects}
 * / {@link SessionManager.loadAcrossProjects}) — instead of only the current cwd.
 * Opt in via `AMI_RESUME_ALL_PROJECTS` (Claude-compat: `CLAUDE_CODE_SHOW_ALL_PROJECTS`).
 *
 * Parity: the reference's resume picker `showAllProjects` scope toggle — the default
 * (unset/falsy) keeps `/sessions` + `/resume` scoped to the current project; truthy
 * flips them to the cross-project aggregation so a session recorded under another
 * directory is browsable/resumable without first `cd`-ing there. A per-invocation
 * `all` keyword (`/sessions all`, `/resume all <arg>`) overrides this default for a
 * single call regardless of the env knob.
 */
export function isResumeAllProjectsEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env.AMI_RESUME_ALL_PROJECTS) || isEnvTruthy(env.CLAUDE_CODE_SHOW_ALL_PROJECTS);
}

/**
 * Env var that opts IN to persisting hook-produced additional-context messages into
 * the saved transcript (so they survive `--resume`) instead of filtering them out on
 * save. Honored alongside the reference's `CLAUDE_CODE_SAVE_HOOK_ADDITIONAL_CONTEXT` (AMI
 * value first), matching the dual-env convention used by {@link isSessionPersistenceDisabled}
 * / {@link isResumeAllProjectsEnabled}. Parity: the reference's
 * `CLAUDE_CODE_SAVE_HOOK_ADDITIONAL_CONTEXT` gate in `isLoggableMessage`.
 */
export const SAVE_HOOK_ADDITIONAL_CONTEXT_ENV = 'AMI_SAVE_HOOK_ADDITIONAL_CONTEXT';

/**
 * Whether hook-produced additional-context messages ({@link isHookAdditionalContextMessage})
 * are PERSISTED into the saved transcript rather than dropped on save. Off by default
 * — AMI re-runs the SessionStart/Setup hooks on resume, so fresh context is derived
 * each run and the stale copy is not needed; the operator opts in when they want the
 * exact hook output preserved verbatim across `--resume`. Parity: the reference's
 * {@link SAVE_HOOK_ADDITIONAL_CONTEXT_ENV} knob consulted by `isLoggableMessage`.
 */
export function isSaveHookAdditionalContextEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env[SAVE_HOOK_ADDITIONAL_CONTEXT_ENV]) || isEnvTruthy(env.CLAUDE_CODE_SAVE_HOOK_ADDITIONAL_CONTEXT);
}

/** True when a message carries hook-produced additional context (a SessionStart/
 *  Setup `additionalContexts[]` aggregation — {@link UserMessage.hookAdditionalContext}).
 *  Parity: the reference's `attachment.type === 'hook_additional_context'` check. */
export function isHookAdditionalContextMessage(msg: Message): boolean {
  return msg.role === 'user' && (msg as UserMessage).hookAdditionalContext === true;
}

/**
 * The single canonical predicate deciding whether a message is PERSISTED into the
 * saved transcript ({@link SessionManager.save}). Mirrors the reference's `isLoggableMessage`:
 *  - engine-injected turn-local control nudges (`isMeta`) never persist;
 *  - hook-produced additional-context messages are dropped UNLESS the operator opts
 *    in via {@link isSaveHookAdditionalContextEnabled} (so they survive `--resume`);
 *  - everything else is loggable.
 * Reads load everything (this gates only writes), exactly like the reference.
 */
export function isLoggableMessage(msg: Message, env: NodeJS.ProcessEnv = process.env): boolean {
  if ((msg as { isMeta?: boolean }).isMeta) return false;
  if (isHookAdditionalContextMessage(msg)) return isSaveHookAdditionalContextEnabled(env);
  return true;
}

/**
 * Content longer than this is offloaded to a content-addressed blob sidecar
 * rather than truncated in place. The value also caps the inline on-screen
 * preview stored next to the blob reference.
 */
export const MAX_CONTENT_LENGTH = 5000;

/**
 * On-disk reference that replaces a large message content string. The full
 * content lives in `<sessionDir>/blobs/<sessionId>/<sha256>.blob`; only a short
 * preview is kept inline so `list()` and corrupt-blob fallbacks stay cheap.
 * Rehydrated back to the full string on load — so --resume no longer loses
 * tool output or long user messages (H14).
 */
export interface SessionBlobRef {
  __siSessionBlob: string; // sha256 of the full UTF-8 content
  preview: string;
  length: number;
}

export function isSessionBlobRef(v: unknown): v is SessionBlobRef {
  return (
    !!v &&
    typeof v === 'object' &&
    !Array.isArray(v) &&
    typeof (v as { __siSessionBlob?: unknown }).__siSessionBlob === 'string'
  );
}

/**
 * Build a rehydrated message from a stored message, its {@link SessionBlobRef},
 * and the blob bytes that were read for it (`null` when the blob is
 * missing/unreadable → the inline preview is kept with a truncation marker).
 * Pure: shared by the sync ({@link SessionManager.rehydrateMessage}) and async
 * ({@link SessionManager.rehydrateMessageAsync}) paths so both produce an
 * identical result regardless of how the bytes were fetched (no duplicated
 * fallback logic — §0b.6).
 */
export function applyRehydratedBlob(msg: Message, stored: SessionBlobRef, bytes: string | null): Message {
  const content = bytes !== null ? bytes : stored.preview + '\n... (truncated; blob unavailable)';
  return { ...msg, content: content as unknown as string };
}

/**
 * Resolve a persisted message's `content` to its plain-text form for title /
 * preview derivation. A string stays as-is; an offloaded {@link SessionBlobRef}
 * yields its inline preview; an array of content blocks (e.g. a pasted-image
 * user message: `[{type:'text',text},{type:'image',…}]`) yields the FIRST text
 * block's text; anything else (undefined/null/non-text-only array) yields `''`.
 *
 * Parity: the reference's deriveFirstPrompt content handling — without this, an array
 * (multimodal) first message produced an empty title/preview, so a fork or the
 * /resume picker showed a generic placeholder instead of the user's actual text.
 */
export function contentToTexts(raw: unknown): string[] {
  if (isSessionBlobRef(raw)) return raw.preview ? [raw.preview] : [];
  if (typeof raw === 'string') return raw ? [raw] : [];
  if (Array.isArray(raw)) {
    const out: string[] = [];
    for (const b of raw) {
      if (
        !!b &&
        typeof b === 'object' &&
        (b as { type?: unknown }).type === 'text' &&
        typeof (b as { text?: unknown }).text === 'string'
      ) {
        out.push((b as { text: string }).text);
      }
    }
    return out;
  }
  return [];
}

export function contentToText(raw: unknown): string {
  const texts = contentToTexts(raw);
  return texts.length > 0 ? texts[0] : '';
}

// ---------------------------------------------------------------------------
// Truncation-tolerant partial-JSON field extraction. These read a single scalar
// string field out of a RAW JSON text slice WITHOUT a full `JSON.parse`, so a
// bounded head/tail read of a huge session file can recover its metadata (id,
// title, dates, branch, tag) cheaply and without OOM. Tolerant of a slice that
// was cut mid-string/mid-escape by the bounded read (a lone trailing `\` or an
// unterminated final string yields the best-effort partial, never a throw).
// Parity: the reference's extractJsonStringField / extractLastJsonStringField /
// unescapeJsonString (sessionStoragePortable).
// ---------------------------------------------------------------------------

export function isJsonWs(ch: string | undefined): boolean {
  return ch === ' ' || ch === '\t' || ch === '\n' || ch === '\r';
}

/**
 * Decode JSON string-escape sequences in an already quote-stripped slice. Handles
 * `\n \t \r \b \f \" \\ \/` and `\uXXXX`; an invalid/truncated `\u` or a lone
 * trailing backslash is dropped rather than throwing (a bounded read can cut mid
 * escape). Parity: the reference's `unescapeJsonString`.
 */
export function unescapeJsonString(s: string): string {
  if (s.indexOf('\\') === -1) return s; // fast path: nothing to decode
  let out = '';
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (c !== '\\') { out += c; continue; }
    const next = s[i + 1];
    if (next === undefined) break; // truncated trailing backslash — drop it
    switch (next) {
      case 'n': out += '\n'; i++; break;
      case 't': out += '\t'; i++; break;
      case 'r': out += '\r'; i++; break;
      case 'b': out += '\b'; i++; break;
      case 'f': out += '\f'; i++; break;
      case '"': out += '"'; i++; break;
      case '\\': out += '\\'; i++; break;
      case '/': out += '/'; i++; break;
      case 'u': {
        const hex = s.slice(i + 2, i + 6);
        if (/^[0-9a-fA-F]{4}$/.test(hex)) {
          out += String.fromCharCode(parseInt(hex, 16));
          i += 5;
        } else {
          out += next; // invalid/truncated \u — keep the literal char, drop the backslash
          i++;
        }
        break;
      }
      default: out += next; i++; break;
    }
  }
  return out;
}

/** Read a JSON string value that opens at `openIdx` (the index of the leading `"`),
 *  returning the DECODED value and the index just past the closing quote. When the
 *  string is unterminated (the slice was truncated) the best-effort decoded partial
 *  is returned with `end = text.length`. */
export function readJsonStringAt(text: string, openIdx: number): { value: string; end: number } {
  let raw = '';
  let i = openIdx + 1;
  for (; i < text.length; i++) {
    const ch = text[i];
    if (ch === '\\') {
      // Keep the escape pair verbatim for unescapeJsonString; an escaped quote is
      // NOT a terminator. A trailing lone backslash is handled by unescapeJsonString.
      raw += ch;
      if (i + 1 < text.length) { raw += text[i + 1]; i++; }
      continue;
    }
    if (ch === '"') return { value: unescapeJsonString(raw), end: i + 1 };
    raw += ch;
  }
  return { value: unescapeJsonString(raw), end: text.length }; // truncated — best effort
}

/** Scan forward for `"field"` used as an OBJECT KEY (followed by `:` then a string
 *  value) starting at `from`, returning the match's value-open index, or -1. A
 *  `"field"` that appears as a string value (not followed by `:`) or whose value is
 *  not a string is skipped. */
export function locateStringField(text: string, field: string, from: number): { openIdx: number; keyEnd: number } | null {
  const key = '"' + field + '"';
  let cursor = from;
  for (;;) {
    const ki = text.indexOf(key, cursor);
    if (ki === -1) return null;
    let j = ki + key.length;
    while (isJsonWs(text[j])) j++;
    if (text[j] !== ':') { cursor = ki + key.length; continue; }
    j++;
    while (isJsonWs(text[j])) j++;
    if (text[j] !== '"') { cursor = ki + key.length; continue; } // non-string value
    return { openIdx: j, keyEnd: ki + key.length };
  }
}

/**
 * Extract the FIRST top-level string value for `field` from a raw JSON text slice,
 * or `undefined` when the key is absent or its value is not a string. Parity:
 * the reference's `extractJsonStringField`.
 */
export function extractJsonStringField(text: string, field: string): string | undefined {
  const hit = locateStringField(text, field, 0);
  if (!hit) return undefined;
  return readJsonStringAt(text, hit.openIdx).value;
}

/**
 * Extract the LAST string value for `field` from a raw JSON text slice — used on a
 * bounded TAIL read where the newest-wins value (e.g. `updatedAt`) sits near the end.
 * Returns `undefined` when no string-valued occurrence exists. Parity: the reference's
 * `extractLastJsonStringField`.
 */
export function extractLastJsonStringField(text: string, field: string): string | undefined {
  let found: string | undefined;
  let from = 0;
  for (;;) {
    const hit = locateStringField(text, field, from);
    if (!hit) return found;
    found = readJsonStringAt(text, hit.openIdx).value;
    from = hit.keyEnd;
  }
}

/**
 * Matches an auto-generated / system first-line that must be skipped when
 * deriving the picker preview: anything that STARTS with a lowercase XML-like
 * tag (IDE context `<ide_opened_file>`/`<ide_selection>`, hook output, task
 * notifications, channel messages) or a synthetic `[Request interrupted by
 * user]` marker. Parity: the reference's SKIP_FIRST_PROMPT_PATTERN
 * (sessionStoragePortable.extractFirstPromptFromHead /
 * sessionStorage.getFirstMeaningfulUserMessageTextContent). */
export const SKIP_FIRST_PROMPT_PATTERN =
  /^(?:\s*<[a-z][\w-]*[\s>]|\[Request interrupted by user[^\]]*\])/;

/** Compaction summaries are the ONLY record of the pre-compaction conversation.
 *  They are kept inline in full (never offloaded) so a missing blob can never
 *  cost us the summarized history on --resume. */
export const COMPACTION_SUMMARY_PREFIX = '[Previous conversation summary]';

/**
 * Derive a clean, meaningful one-line preview from a transcript's messages for
 * the /resume + /sessions picker — the single canonical first-prompt extractor.
 *
 * Walks user turns in order and, per the reference's getFirstMeaningfulUserMessageText-
 * Content / extractFirstPromptFromHead, skips noise so the label shows what the
 * user actually asked, not framing:
 *  - synthetic/ephemeral turns (`isMeta`, transcript-only) and subagent sidechains;
 *  - compaction-summary turns (the `[Previous conversation summary]` sentinel);
 *  - every text block of a multimodal/array message is scanned so a real prompt
 *    hidden behind an `<ide_selection>`/`<ide_opened_file>` block is still found;
 *  - a `<command-name>` slash-command turn is remembered as a FALLBACK but kept
 *    scanning for a real prompt (so `/clear`-only sessions still get a label);
 *  - a `<bash-input>` turn is formatted as `! <cmd>` (checked before the XML skip);
 *  - any line matching {@link SKIP_FIRST_PROMPT_PATTERN} (leading lowercase XML
 *    tag / interrupt marker) is dropped.
 * Returns `'(no messages)'` when nothing meaningful exists. Result is collapsed
 * to one line and capped at 100 chars with an ellipsis.
 */
/**
 * Single forward pass that collects every meaningful user prompt (in order) from
 * a transcript plus the first slash-command name as a fallback label. The ONE
 * canonical extractor shared by both {@link extractMeaningfulPreview} (takes the
 * FIRST prompt) and {@link extractLastPrompt} (takes the LAST) so the two can
 * never diverge in their skip/normalization rules (§0b.6 — no duplicated label
 * logic). Each returned prompt is already whitespace-collapsed and (for non-bash
 * turns) capped at 100 chars with an ellipsis.
 */
export function collectMeaningfulPrompts(messages: Message[]): { prompts: string[]; commandFallback: string } {
  let commandFallback = '';
  const prompts: string[] = [];
  for (const msg of messages) {
    if (msg.role !== 'user') continue;
    const m = msg as { isMeta?: boolean; isSidechain?: boolean; isVisibleInTranscriptOnly?: boolean; content?: unknown };
    if (m.isMeta || m.isSidechain || m.isVisibleInTranscriptOnly) continue;

    for (const rawText of contentToTexts(m.content)) {
      // Strip system-injected `<tag>…</tag>` blocks (appended context files,
      // system-reminder wrappers, IDE/hook/task notifications) so the label
      // reflects what the user actually typed, not machine-injected noise (U188).
      const flat = stripDisplayTags(rawText).replace(/\n/g, ' ').trim();
      if (!flat) continue;

      // Compaction summaries are history, not a prompt — never a picker label.
      if (flat.startsWith(COMPACTION_SUMMARY_PREFIX)) continue;

      // Slash command: remember the first as a fallback, keep looking for a prompt.
      // Single canonical named-tag extraction (extractTag) rather than an ad-hoc
      // per-tag regex (§0b.6 — no duplicated tag-parsing logic).
      const cmd = extractTag(flat, 'command-name');
      if (cmd !== null) {
        if (!commandFallback) commandFallback = cmd.trim();
        continue;
      }

      // Bash input: label as typed — checked BEFORE the generic XML-tag skip.
      const bash = extractTag(flat, 'bash-input');
      if (bash !== null) { prompts.push(`! ${bash.trim()}`); continue; }

      if (SKIP_FIRST_PROMPT_PATTERN.test(flat)) continue;

      prompts.push(flat.length > 100 ? flat.slice(0, 100) + '...' : flat);
    }
  }
  return { prompts, commandFallback };
}

export function extractMeaningfulPreview(messages: Message[]): string {
  const { prompts, commandFallback } = collectMeaningfulPrompts(messages);
  if (prompts.length > 0) return prompts[0];
  if (commandFallback) return commandFallback;
  return '(no messages)';
}

/**
 * The most-recent meaningful user instruction (the LAST prompt), as opposed to
 * {@link extractMeaningfulPreview}'s FIRST. Shares {@link collectMeaningfulPrompts}
 * so the skip/normalization rules are identical. Returns `'(no messages)'` when no
 * meaningful user turn exists. Parity: the reference's lastPrompt (distinct from
 * firstPrompt) so the resume list can show what a session was last asked to do.
 */
export function extractLastPrompt(messages: Message[]): string {
  const { prompts, commandFallback } = collectMeaningfulPrompts(messages);
  if (prompts.length > 0) return prompts[prompts.length - 1];
  if (commandFallback) return commandFallback;
  return '(no messages)';
}

/**
 * Derive a single-line title base from a transcript's first user message
 * (parity: the reference's `deriveFirstPrompt`). Finds the first non-sidechain `user`
 * turn, resolves its content to text ({@link contentToText} — handles a string,
 * an offloaded blob preview, or a multimodal array's first text block), then
 * COLLAPSES all whitespace to single spaces and caps at 100 chars. Collapsing is
 * the whole point: a pasted multi-line stack / code block otherwise flows into a
 * saved title and breaks the one-line resume/fork hint. Returns the shared
 * `'Branched conversation'` fallback when there is no first user message, it has
 * no text, or it is whitespace-only. The ONE canonical single-line title deriver,
 * reused by {@link SessionManager.fork}'s base-name logic (no duplicated
 * collapse/slice/fallback — §0b.6).
 */
export function deriveFirstPrompt(messages: Message[]): string {
  const FALLBACK = 'Branched conversation';
  const firstUser = messages.find(m => m.role === 'user' && !m.isSidechain);
  if (!firstUser) return FALLBACK;
  const text = contentToText((firstUser as { content?: unknown }).content);
  const flat = text.replace(/\s+/g, ' ').trim().slice(0, 100);
  return flat || FALLBACK;
}
