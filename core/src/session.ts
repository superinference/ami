import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { execSync as childExecSync } from 'child_process';
import { StringDecoder } from 'string_decoder';
import type { Message, ProviderConfig, SessionId, UserMessage, AssistantMessage, SessionOperatingMode } from './types';
import { asSessionId } from './types';
import { projectStateDir, ndjsonSafeStringify, canonicalGitRoot, resolveConfigDir, projectSlug, MAX_SLUG_LENGTH } from './reference-journal';
import { assertPathWithin } from './path-safety';
import { atomicWriteFileSync, secureMkdirSync, fsHooks, OWNER_ONLY_FILE_MODE, OWNER_ONLY_DIR_MODE } from './utils/fs-atomic';
import { getGitBranch } from './utils/git';
import { stripDisplayTags, extractTag } from './utils/display-tags';
import { escapeRegExp } from './utils/string';
import { AMI_VERSION } from './version';
import { isEnvTruthy } from './analytics';
import { isWSL } from './utils/env';
import { normalizePermissionMode } from './permissions';
import {
  type ProcessIdentity,
  type ProcessIdentityProbe,
  readProcessStartTimeMsSync,
  probeProcessIdentitySync,
  isSafeToSteal,
  isProvenAlive,
  mintSpawnToken,
} from './utils/process-identity';
export { assertPathWithin } from './path-safety';

/**
 * Upper bound on a session/transcript file we will read back into memory. A
 * corrupt or maliciously huge file must never OOM the process on --resume; a file
 * over this size is treated as unreadable (skipped) rather than loaded. Mirrors
 * the reference's size boundary on session reads.
 */
export const MAX_SESSION_READ_BYTES = 100 * 1024 * 1024; // 100 MiB

/**
 * Size of the bounded head/tail buffer used by {@link SessionManager.readSessionMetaLite}
 * when deriving a session-list row's metadata WITHOUT a full `JSON.parse` of the
 * whole file. Only this many bytes are read from each end of the file, so a 50 MB
 * transcript costs the same lite-metadata read as a 5 KB one. Parity: the reference's
 * `LITE_READ_BUF_SIZE` (sessionStoragePortable) bounded head/tail metadata read.
 */
export const LITE_READ_BUF_SIZE = 64 * 1024; // 65536

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
 * A session file at or above this size is ALWAYS read via the bounded lite path for
 * a listing row (never fully parsed just to render a picker line), even when it falls
 * inside the {@link INITIAL_ENRICH_COUNT} enrichment window — parsing a multi-megabyte
 * transcript to show one row is never worth it. Parity: the reference's
 * `SKIP_PRECOMPACT_THRESHOLD` resume-read size guard.
 */
export const SKIP_PRECOMPACT_THRESHOLD = 5 * 1024 * 1024; // 5 MiB

/**
 * Chunk size for the forward transcript reader {@link readTranscriptForLoad}. The
 * resume load path reads the transcript fd one megabyte at a time — balancing the
 * number of read syscalls against buffer growth — instead of slurping the whole
 * file with a single `readFileSync`, so peak memory tracks the SURVIVING
 * (post-last-compact-boundary) output rather than the raw on-disk file size. Parity:
 * the reference's `TRANSCRIPT_READ_CHUNK_SIZE` (sessionStoragePortable) forward reader.
 */
export const TRANSCRIPT_READ_CHUNK_SIZE = 1024 * 1024; // 1 MiB

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
const MAX_CONTENT_LENGTH = 5000;

/**
 * On-disk reference that replaces a large message content string. The full
 * content lives in `<sessionDir>/blobs/<sessionId>/<sha256>.blob`; only a short
 * preview is kept inline so `list()` and corrupt-blob fallbacks stay cheap.
 * Rehydrated back to the full string on load — so --resume no longer loses
 * tool output or long user messages (H14).
 */
interface SessionBlobRef {
  __siSessionBlob: string; // sha256 of the full UTF-8 content
  preview: string;
  length: number;
}

function isSessionBlobRef(v: unknown): v is SessionBlobRef {
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
function applyRehydratedBlob(msg: Message, stored: SessionBlobRef, bytes: string | null): Message {
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

function isJsonWs(ch: string | undefined): boolean {
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
function readJsonStringAt(text: string, openIdx: number): { value: string; end: number } {
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
function locateStringField(text: string, field: string, from: number): { openIdx: number; keyEnd: number } | null {
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
const SKIP_FIRST_PROMPT_PATTERN =
  /^(?:\s*<[a-z][\w-]*[\s>]|\[Request interrupted by user[^\]]*\])/;

/** Compaction summaries are the ONLY record of the pre-compaction conversation.
 *  They are kept inline in full (never offloaded) so a missing blob can never
 *  cost us the summarized history on --resume. */
const COMPACTION_SUMMARY_PREFIX = '[Previous conversation summary]';

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
function collectMeaningfulPrompts(messages: Message[]): { prompts: string[]; commandFallback: string } {
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

/**
 * Resolve a session's display summary by the precedence chain customTitle >
 * aiTitle > lastPrompt > firstPrompt, returning `undefined` when none exist (a
 * metadata-only session). The `'(no messages)'` sentinel is treated as absent so
 * an empty session never masquerades as having a real summary. Parity: the reference's
 * listSessionsImpl summary precedence (which drops metadata-only sessions).
 */
export function resolveSessionSummary(
  session: { customTitle?: string; aiTitle?: string },
  prompts: { lastPrompt?: string; firstPrompt?: string } = {},
): string | undefined {
  for (const candidate of [session.customTitle, session.aiTitle, prompts.lastPrompt, prompts.firstPrompt]) {
    if (typeof candidate !== 'string') continue;
    const t = candidate.trim();
    if (t && t !== '(no messages)') return t;
  }
  return undefined;
}

/**
 * Classification of how a session is being run, persisted so the resume/picker
 * paths can discriminate a plain interactive session from a backgrounded or
 * daemon-owned one (parity: the reference's `SessionKind` in concurrentSessions.ts):
 *  - `'interactive'` — a normal foreground TUI / `--prompt` run (the default);
 *  - `'bg'` — a user-backgrounded session that should keep running when its
 *    frontend detaches (and must NOT be auto-picked as "the most recent" by a
 *    no-arg `--continue`);
 *  - `'daemon'` / `'daemon-worker'` — a long-lived service session and its
 *    workers, likewise excluded from the ordinary resume picker.
 * Orthogonal to {@link Session.entrypoint} (which records the invocation surface,
 * e.g. `cli`/`detached`/`sdk`): a `detached` run can still be `interactive`.
 */
export type SessionKind = 'interactive' | 'bg' | 'daemon' | 'daemon-worker';

/** The valid {@link SessionKind} values, exported so a consumer (CLI/introspection)
 *  can validate an override without duplicating the union. */
export const SESSION_KINDS: readonly SessionKind[] = ['interactive', 'bg', 'daemon', 'daemon-worker'];

/**
 * Read the session-kind override from the environment, if any. Honors the AMI
 * knob `AMI_SESSION_KIND` and — consistent with the dual-env convention used
 * elsewhere in this file ({@link isSessionPersistenceDisabled}) — the reference's
 * `CLAUDE_CODE_SESSION_KIND`, taking the AMI value first. Returns `undefined`
 * when unset or not one of {@link SESSION_KINDS}, so a bad value falls back to
 * the caller's default instead of persisting garbage. Parity: the reference's
 * `envSessionKind`.
 */
export function envSessionKind(env: NodeJS.ProcessEnv = process.env): SessionKind | undefined {
  const raw = env.AMI_SESSION_KIND ?? env.CLAUDE_CODE_SESSION_KIND;
  if (typeof raw !== 'string') return undefined;
  const v = raw.trim() as SessionKind;
  return SESSION_KINDS.includes(v) ? v : undefined;
}

/**
 * True when a session is a background/daemon kind — one that keeps running on
 * frontend detach and must be skipped when a no-arg `--continue` auto-picks the
 * most-recent resumable session (parity: the reference's `isBgSession`, which flips exit
 * semantics for `--bg`/daemon sessions). An absent `kind` is treated as the
 * foreground default (`false`), so legacy sessions written before this field are
 * still resumable.
 */
export function isBgSession(session: { kind?: SessionKind }): boolean {
  return session.kind === 'bg' || session.kind === 'daemon' || session.kind === 'daemon-worker';
}

/**
 * Sentinel {@link SessionAgentIdentity.model} value meaning "do NOT override the
 * main-loop model — use whatever model the resumed run is otherwise configured
 * with". Distinct from an absent model (also no override) only in intent: an agent
 * can be authored to explicitly inherit. Parity: the reference's `resumedAgent.model !==
 * 'inherit'` guard in `restoreAgentFromSession`.
 */
export const AGENT_MODEL_INHERIT = 'inherit';

/**
 * Agent/persona identity a session ran under, persisted so `--resume` can restore
 * it (parity: the reference's conversationRecovery session metadata
 * `agentName`/`agentColor`/`agentSetting` + the resumed agent's model override).
 *  - `name` / `color` — display identity for the picker and status line;
 *  - `setting` — the agent TYPE identifier used to re-resolve the live agent
 *    definition on resume (the reference's `agentSetting`);
 *  - `model` — the agent's model override, or the {@link AGENT_MODEL_INHERIT}
 *    sentinel. Applied on resume via {@link resolveAgentModelOverride}.
 */
export interface SessionAgentIdentity {
  name?: string;
  color?: string;
  setting?: string;
  model?: string;
}

/**
 * Decide the model override to apply when resuming a session whose saved agent had
 * a model (parity: the reference's `restoreAgentFromSession` model-apply branch). Returns
 * the model id to set as the main-loop override, or `null` when NO override should
 * be applied, in precedence order:
 *  1. the user passed `--agent` on the CLI → keep the CLI agent, apply nothing
 *     (`userPassedAgentOverride`);
 *  2. a main-loop model override is already set (e.g. a `--model` flag) → it wins
 *     (`existingModelOverride`);
 *  3. the saved model is absent/blank or the {@link AGENT_MODEL_INHERIT} sentinel
 *     → use the main-loop model (no override);
 *  4. otherwise → apply the saved agent model (trimmed).
 * Pure (all state injected) so the resume precedence is unit-testable without the
 * CLI/agent registry — the single acceptance boundary a `--resume` path reuses.
 */
export function resolveAgentModelOverride(opts: {
  savedModel?: string;
  userPassedAgentOverride?: boolean;
  existingModelOverride?: boolean;
}): string | null {
  if (opts.userPassedAgentOverride) return null;
  if (opts.existingModelOverride) return null;
  if (typeof opts.savedModel !== 'string') return null;
  const model = opts.savedModel.trim();
  if (!model || model === AGENT_MODEL_INHERIT) return null;
  return model;
}

/**
 * Project a persisted {@link SessionAgentIdentity} down to the display subset a
 * {@link SessionListEntry} carries (name/color/setting — never the model override,
 * which is a resume-time concern). Returns `undefined` when there is no agent or it
 * has no displayable field, so a plain session's list entry carries no `agent` key
 * at all. The ONE place the picker-projection lives so {@link SessionManager.list}
 * never re-derives it inline (§0b.6).
 */
export function toListEntryAgent(
  agent: SessionAgentIdentity | undefined,
): { name?: string; color?: string; setting?: string } | undefined {
  if (!agent) return undefined;
  const { name, color, setting } = agent;
  if (name === undefined && color === undefined && setting === undefined) return undefined;
  return { name, color, setting };
}

export interface Session {
  id: SessionId;
  /** A user-set rename. Always wins over {@link aiTitle} and is never
   *  overwritten by auto-title (re)generation (preserved across save()). */
  customTitle?: string;
  /** Auto-generated, regeneratable title. May be overwritten by a newer AI
   *  title but never clobbers a {@link customTitle}. */
  aiTitle?: string;
  messages: Message[];
  createdAt: string;
  updatedAt: string;
  provider: { model: string; baseUrl: string };
  /** Git branch the session was last saved on — lets /resume show and filter by
   *  branch. Populated on save() from the SessionManager's cwd. */
  gitBranch?: string;
  /** The originating project directory (cwd), so a listed/resumed session can
   *  report which directory it belongs to. Populated on save(). */
  projectPath?: string;
  /** App/schema version that wrote this session, so resume/migration can detect
   *  and adapt to sessions written by an older build. Stamped on save(). */
  version?: string;
  /** Optional user-set label, searchable in /resume + /search. One tag per
   *  session (toggle/replace/remove via {@link SessionManager.toggleTag}). */
  tag?: string;
  /** Session mode persisted so /resume can restore the posture the user left the
   *  session in (parity: the reference's per-session `mode`). */
  mode?: 'coordinator' | 'normal';
  /** GitHub PR this session is linked to, for list navigation / tracking
   *  (parity: the reference's prNumber/prUrl/prRepository PR-link fields). `repository`
   *  is an `owner/repo` slug. */
  pr?: { number: number; url: string; repository?: string };
  /** Worktree the session was last inside, persisted so --resume can restore the
   *  correct cwd (parity: the reference's WorktreeStateEntry / PersistedWorktreeSession).
   *  `null` = the session explicitly EXITED its worktree; `undefined`/absent =
   *  it was never inside one. */
  worktreeState?: { originalCwd: string; worktreePath: string; branch?: string } | null;
  /** Invocation surface that created / last wrote the session — e.g. `'cli'`
   *  (interactive TUI), `'detached'` (`--prompt`), or an SDK surface. Lets later
   *  filtering/analytics discriminate how a session was launched (parity: the reference's
   *  CLAUDE_CODE_ENTRYPOINT `entrypoint` discriminator). */
  entrypoint?: string;
  /** How the session is being run ({@link SessionKind}): a foreground
   *  `'interactive'` session vs a `'bg'`/`'daemon'`(-worker) one that keeps
   *  running on detach and is skipped by a no-arg `--continue`. Stamped on save()
   *  from the caller-supplied value, else {@link envSessionKind}, else
   *  `'interactive'`. Parity: the reference's persisted `SessionKind`. */
  kind?: SessionKind;
  /** Subagent/sidechain transcript marker: when true this session is a task
   *  sub-engine's conversation and is excluded from the resume picker by default
   *  (parity: the reference filters `isSidechain` logs out of the resumable list). */
  isSidechain?: boolean;
  /** The session this one was forked/regenerated from — e.g. the pre-`/clear`
   *  session recorded as the fresh session's parent for lineage tracking (parity:
   *  the reference's `regenerateSessionId({ setCurrentAsParent })` + `STATE.parentSessionId`).
   *  Lets analytics/resume walk the chain of related sessions a user cleared through. */
  parentSessionId?: SessionId;
  /** Agent/persona identity the session ran under ({@link SessionAgentIdentity}),
   *  persisted so --resume can restore the agent's name/color/display setting and
   *  re-apply its model override via {@link resolveAgentModelOverride} (parity:
   *  the reference's agentName/agentColor/agentSetting + 'inherit' model sentinel). Absent
   *  on a plain (default-agent) session. */
  agent?: SessionAgentIdentity;
  /** Content-replacement decisions ({@link ContentReplacementEntry}) persisted for
   *  resume: which in-context tool_result blocks were replaced with a smaller stub
   *  by a per-message budget pass, so `--resume` reconstructs the same stubs and the
   *  prompt cache stays warm (a lost record re-sends the full content → cache miss +
   *  permanent overage). Carried across {@link SessionManager.fork} by
   *  {@link forkContentReplacements}. Parity: the reference's `ContentReplacementEntry` +
   *  `createFork` content-replacement preservation. Absent when nothing was ever
   *  replaced. */
  contentReplacements?: ContentReplacementEntry[];
  /** Per-session settings saved on disk so /resume can restore them. */
  settings?: {
    skill?: string;
    thinkingLevel?: string;
    outputStyle?: string;
    goal?: string;
    /** Permission mode the user left the session in (e.g. `'ask'`, `'auto-allow'`,
     *  `'deny-all'`), persisted so --resume restores the permission posture
     *  instead of silently reverting to the flag-derived default (parity: the reference's
     *  SessionIndexEntry.permissionMode). */
    permissionMode?: string;
    /** Active plan state when the session was saved: the on-disk plan file path and
     *  whether plan mode was engaged. Persisted so `--resume` restores the active
     *  plan (re-entering plan mode + re-pointing at the plan file), recovering from
     *  the transcript if the file is missing. Parity: the reference's copyPlanForResume
     *  plan-slug restoration. Absent when the session was never in plan mode. */
    plan?: { filePath: string; active: boolean };
    /** Operating mode (`'normal'` | `'coordinator'`) the session ran under when saved,
     *  persisted so `--resume` can reconcile a resumed session against the live
     *  environment (flip into/out of coordinator mode to match) instead of silently
     *  running under whatever the current env dictates. Absent on sessions saved before
     *  mode tracking existed → reconciliation is a no-op (back-compat). Parity: the reference
     *  Code's `matchSessionMode` / persisted session mode. */
    operatingMode?: SessionOperatingMode;
    /** Serialized cost state (cumulative cost + input/output tokens + request count,
     *  `serializeCostState` JSON) at save time, so `--resume` of the SAME session can
     *  re-seed the live CostTracker and continue its `/cost` totals instead of showing
     *  `$0` while the on-disk journal holds the prior spend. Absent on sessions saved
     *  before cost persistence existed → resume keeps the fresh ($0) tracker. Parity:
     *  the reference's lastCost/lastModelUsage restored via restoreCostStateForSession. */
    costState?: string;
    /** Serialized {@link AttributionSnapshot} (full-state dump: per-file agent
     *  contributions + effort counters) at save time, so `--resume` of the SAME
     *  session restores the attribution store instead of losing the running
     *  per-file/char and "N-shotted" accounting on restart. A restore reads ONLY
     *  this most-recent snapshot (never sums deltas). Parity: the reference's
     *  stateToSnapshotMessage persisted + restoreAttributionStateFromSnapshots. */
    attributionSnapshot?: string;
  };
}

/** The display title for a session: a user rename wins over the AI title. */
export function resolveSessionTitle(session: {
  customTitle?: string;
  aiTitle?: string;
}): string | undefined {
  return session.customTitle ?? session.aiTitle;
}

/**
 * Deterministic display title for the /resume + /sessions picker with the full
 * fallback chain, so a session ALWAYS renders a sensible label even when it has
 * no title and no meaningful prompt. Order:
 *   1. {@link resolveSessionSummary} — customTitle > aiTitle > lastPrompt > firstPrompt
 *      (display tags stripped so a command-only turn can't leak raw XML);
 *   2. an explicit `defaultTitle` the caller supplies;
 *   3. `'Autonomous session'` when the session was launched detached (`entrypoint`
 *      `'detached'`) and nothing above resolved — an autonomous run with no real
 *      user prompt still gets a human label instead of a bare id;
 *   4. the first 8 chars of the session id (lite entries with no metadata).
 * Returns `''` only for a session with no summary, no defaultTitle, not detached,
 * and no id. Consolidates with {@link resolveSessionSummary} (no duplicated
 * precedence logic). Parity: the reference's getLogDisplayTitle (defaultTitle param,
 * autonomous-session special-case, sessionId.slice(0,8) fallback).
 */
export function getSessionDisplayTitle(
  session: { id?: SessionId | string; customTitle?: string; aiTitle?: string; entrypoint?: string },
  prompts: { lastPrompt?: string; firstPrompt?: string } = {},
  defaultTitle?: string,
): string {
  const summary = resolveSessionSummary(session, prompts);
  if (summary) {
    const cleaned = stripDisplayTags(summary).trim();
    return cleaned || summary;
  }
  if (typeof defaultTitle === 'string' && defaultTitle.trim()) return defaultTitle.trim();
  if (session.entrypoint === 'detached') return 'Autonomous session';
  const id = session.id ? String(session.id) : '';
  return id.slice(0, 8);
}

export interface SessionListEntry {
  id: SessionId;
  date: string;
  preview: string;
  /** Number of persisted messages — lets the picker tell a 3-message dead-end
   *  from a 200-message deep session. */
  messageCount: number;
  /** On-disk size of the session JSON in bytes (for display). */
  fileSize?: number;
  /** Git branch the session was saved on, when recorded. */
  gitBranch?: string;
  /** Resolved display title (user rename ?? AI title), when present. */
  title?: string;
  /** The most-recent meaningful user instruction, distinct from {@link preview}
   *  (which is the FIRST prompt) — lets the picker surface "what was this session
   *  last asked to do" (parity: the reference's lastPrompt log entry). */
  lastPrompt?: string;
  /** Display summary via the precedence chain customTitle > aiTitle > lastPrompt >
   *  firstPrompt (parity: the reference's listSessionsImpl summary). `undefined` for a
   *  metadata-only session with no title and no prompts. */
  summary?: string;
  /** User-set searchable tag, when recorded. */
  tag?: string;
  /** Persisted session mode, when recorded. */
  mode?: string;
  /** Linked GitHub PR, when recorded. */
  pr?: { number: number; url: string; repository?: string };
  /** Worktree path the session was last inside, when it is currently in one. */
  worktree?: string;
  /** The originating project directory (cwd) the session belongs to, when
   *  recorded — lets a cross-project/worktree-spanning listing show WHICH directory
   *  each session came from (parity: the reference's SessionInfo.cwd, falling back to the
   *  project path). Populated from {@link Session.projectPath}. */
  projectPath?: string;
  /** Invocation surface that last wrote the session, when recorded. */
  entrypoint?: string;
  /** How the session is being run ({@link SessionKind}) — lets the picker tell a
   *  foreground session from a `'bg'`/`'daemon'` one, when recorded. */
  kind?: SessionKind;
  /** Persisted permission mode, when recorded. */
  permissionMode?: string;
  /** True when this entry is a subagent sidechain transcript (only present when
   *  {@link SessionManager.list} is called with `includeSidechain`). */
  isSidechain?: boolean;
  /** The session this one was forked/regenerated from, when recorded — lets a
   *  picker/analytics show the lineage of a cleared-through chain of sessions. */
  parentSessionId?: SessionId;
  /** Agent/persona identity (name/color/setting) the session ran under, when
   *  recorded — lets the picker/status line show which agent a session used
   *  (parity: the reference's agentName/agentColor list metadata). The model override is
   *  intentionally NOT surfaced here (it is a resume-time concern via
   *  {@link resolveAgentModelOverride}, not a picker label). */
  agent?: { name?: string; color?: string; setting?: string };
  /** True when this entry was built from a bounded lite metadata read
   *  ({@link SessionManager.readSessionMetaLite}) rather than a full parse: its
   *  {@link preview}/{@link lastPrompt}/{@link summary} come from the title only and
   *  {@link messageCount} is 0 until the session is opened (parity: the reference's
   *  not-yet-enriched resume-picker rows). A full parse is done on open. */
  lite?: boolean;
}

/**
 * Introspection counts for a {@link SessionManager.list} / {@link SessionManager.listWithDiagnostics}
 * pass — the data that answers "why didn't my session show up in /resume?". Mirrors
 * the reference's resume-discovery debug lines (sessionStorage.ts `found N session files`
 * / `enriched N, M filtered out, K visible` + per-session filter reasons): instead of
 * silently dropping an unreadable/filtered row, the listing tallies how many files were
 * found, how many are visible, and how many were skipped broken down by reason, so a
 * missing session is diagnosable rather than invisible.
 */
export interface SessionListDiagnostics {
  /** Total session files discovered on disk ({@link SessionManager.listFiles}). */
  found: number;
  /** Entries returned to the caller (visible rows after every filter). */
  listed: number;
  /** Rows dropped for any reason (sum of {@link skippedByReason} values). */
  skipped: number;
  /** Per-reason skip tally — e.g. `unreadable`, `sidechain`, `current-session`,
   *  `duplicate-id`, `empty`, `branch-filter`, `tag-filter`, `pr-filter`. A reason
   *  with no drops is simply absent from the map. */
  skippedByReason: Record<string, number>;
}

/** The skip reasons {@link SessionManager.list} records into a {@link SessionListDiagnostics}
 *  sink. Exported so a consumer (e.g. the `/sessions` / `/resume` renderer) can label
 *  each reason without re-deriving the string set. */
export const SESSION_SKIP_REASONS = [
  'unreadable', 'sidechain', 'current-session', 'duplicate-id',
  'empty', 'branch-filter', 'tag-filter', 'pr-filter',
] as const;
export type SessionSkipReason = (typeof SESSION_SKIP_REASONS)[number];

/**
 * Metadata recovered by a bounded head/tail read of a session file WITHOUT a full
 * `JSON.parse` of its transcript ({@link SessionManager.readSessionMetaLite}). Only
 * top-level scalar fields are extractable this way (the first-prompt preview and the
 * message count need the full messages array, so they are left to enrichment on
 * open). Parity: the reference's lite session-list metadata.
 */
export interface SessionMetaLite {
  id: string;
  createdAt?: string;
  updatedAt?: string;
  customTitle?: string;
  aiTitle?: string;
  gitBranch?: string;
  tag?: string;
  version?: string;
  projectPath?: string;
  entrypoint?: string;
  mode?: string;
  kind?: SessionKind;
  /** True when a top-level `"isSidechain": true` was seen in the head buffer. */
  isSidechain?: boolean;
}

/**
 * Parse a PR identifier the way the resume/session picker's PR filter accepts it:
 * either a bare positive integer (`"42"`) or a GitHub pull-request URL
 * (`https://github.com/owner/repo/pull/42`), returning the numeric PR number, or
 * `null` when the input is neither. A non-positive or non-numeric direct value
 * falls through to the URL probe; an unmatched URL yields `null`. Parity: the reference's
 * `parsePrIdentifier` in the resume screen — the single acceptance boundary behind
 * {@link filterSessionsByPr} so a `--pr <n|url>` resume filter and any picker share
 * one parser (no duplicated regex).
 */
export function parsePrIdentifier(value: string): number | null {
  const directNumber = parseInt(value, 10);
  if (!isNaN(directNumber) && directNumber > 0) {
    return directNumber;
  }
  const urlMatch = value.match(/github\.com\/[^/]+\/[^/]+\/pull\/(\d+)/);
  if (urlMatch?.[1]) {
    return parseInt(urlMatch[1], 10);
  }
  return null;
}

/**
 * The PR filter accepted by {@link SessionManager.list}'s `filterByPr` option and
 * {@link filterSessionsByPr}:
 *  - `true` — keep only sessions that are linked to ANY PR;
 *  - `false` — no PR constraint (keep everything, same as omitting the filter);
 *  - a `number` — keep only sessions linked to exactly that PR number;
 *  - a `string` — parse it ({@link parsePrIdentifier}: integer or GitHub PR URL)
 *    and keep only sessions linked to the resolved number (an unparseable string
 *    matches nothing).
 */
export type PrFilter = boolean | number | string;

/**
 * Filter a list of session entries by their linked PR (parity: the reference's
 * `filteredLogs` PR filter in the resume screen). Shared by
 * {@link SessionManager.list} and any caller that already holds entries, so the
 * `true`/number/string semantics live in ONE place. An entry matches by its
 * `pr.number` ({@link SessionListEntry.pr}); entries with no linked PR are dropped
 * whenever a filter is active.
 */
export function filterSessionsByPr<T extends { pr?: { number: number } }>(
  entries: T[],
  filter: PrFilter,
): T[] {
  if (typeof filter === 'boolean') {
    return filter ? entries.filter(e => e.pr?.number !== undefined) : entries;
  }
  if (typeof filter === 'number') {
    return entries.filter(e => e.pr?.number === filter);
  }
  const prNumber = parsePrIdentifier(filter);
  if (prNumber === null) return [];
  return entries.filter(e => e.pr?.number === prNumber);
}

/**
 * Canonical "most-recent first" ordering for session list entries: sort by
 * recorded `date` descending, breaking ties on the session id (the newer id — ids
 * embed a monotonic timestamp+sequence — sorts first) so a paginated or merged
 * listing is deterministic across calls. The ONE comparator shared by
 * {@link SessionManager.list} and the cross-project aggregator
 * ({@link SessionManager.listAcrossProjects}) so a single-project and an
 * all-projects listing can never diverge in their ordering rules (§0b.6).
 */
export function compareSessionEntriesByDate(
  a: { id: SessionId | string; date?: string },
  b: { id: SessionId | string; date?: string },
): number {
  const bTime = b.date ? new Date(b.date).getTime() : 0;
  const aTime = a.date ? new Date(a.date).getTime() : 0;
  const byTime = (isNaN(bTime) ? 0 : bTime) - (isNaN(aTime) ? 0 : aTime);
  if (byTime !== 0) return byTime;
  return String(b.id).localeCompare(String(a.id));
}

/** Single-quote a string for safe embedding in a POSIX shell command: wrap in
 *  single quotes and escape any embedded single quote as `'\''`. Used to build the
 *  copyable cross-project resume command in {@link checkCrossProjectResume}. */
function shellSingleQuote(s: string): string {
  return `'${s.replace(/'/g, `'\\''`)}'`;
}

/**
 * Three-way classification of resuming a session that lives in a DIFFERENT project
 * directory than the current cwd (parity: the reference's `checkCrossProjectResume`):
 *  - not cross-project (scope off, no recorded path, or same cwd) → resume in place;
 *  - cross-project but a sibling git worktree of the SAME repo → resume directly
 *    (no `cd` needed, the worktree shares the repo);
 *  - a genuinely different project → surface a copyable `command` the user can run
 *    to switch directories and resume there.
 */
export type CrossProjectResumeResult =
  | { isCrossProject: false }
  | { isCrossProject: true; isSameRepoWorktree: true; projectPath: string }
  | { isCrossProject: true; isSameRepoWorktree: false; command: string; projectPath: string };

/**
 * Classify whether resuming `log` crosses a project boundary relative to
 * `currentCwd`, and (for a genuinely different project) produce a copyable
 * `cd … && ami --resume <id>` command. Pure: all environment (the current cwd and
 * the set of same-repo git worktree paths) is passed in, so this core primitive has
 * no process/git dependency and the CLI picker can render the result however it
 * likes (inline badge for a same-repo worktree, clipboard-copy affordance for a
 * different project).
 *
 * FUNCTIONAL (intentional parity divergence): AMI does NOT gate worktree detection
 * behind the reference's internal `USER_TYPE === 'ant'` staged-rollout flag — the same-repo
 * worktree shortcut is always available. Parity: the reference's `checkCrossProjectResume`
 * (same discriminated result + longest-literal `startsWith(wt + sep)` containment).
 */
export function checkCrossProjectResume(
  log: { id: SessionId | string; projectPath?: string },
  currentCwd: string,
  worktreePaths: string[],
  showAllProjects: boolean,
): CrossProjectResumeResult {
  if (!showAllProjects || !log.projectPath || log.projectPath === currentCwd) {
    return { isCrossProject: false };
  }
  const projectPath = log.projectPath;
  const isSameRepoWorktree = worktreePaths.some(
    wt => projectPath === wt || projectPath.startsWith(wt + path.sep),
  );
  if (isSameRepoWorktree) {
    return { isCrossProject: true, isSameRepoWorktree: true, projectPath };
  }
  const command = `cd ${shellSingleQuote(projectPath)} && ami --resume ${shellSingleQuote(String(log.id))}`;
  return { isCrossProject: true, isSameRepoWorktree: false, command, projectPath };
}

/**
 * What the resume path should do with a session's persisted {@link Session.worktreeState}:
 *  - `'reenter'` — re-enter the recorded worktree (its coordinates are returned);
 *  - `'cleared'` — the worktree was recorded but its directory no longer exists on
 *    disk (e.g. removed by an `/exit` dialog between sessions), so the worktree
 *    binding must be dropped gracefully rather than cd'ing into a missing path;
 *  - `'none'` — the session was never inside a worktree (`undefined`) or explicitly
 *    EXITED it (`null`), so there is nothing to restore.
 */
export type WorktreeResumeResolution =
  | { action: 'none' }
  | { action: 'cleared' }
  | { action: 'reenter'; worktreePath: string; originalCwd: string; branch?: string };

/**
 * Resolve how to restore a session's worktree binding on `--resume`, applying the
 * disk-existence fallback that distinguishes a live worktree from a stale one
 * (parity: the reference's `restoreWorktreeForResume`, which re-enters the worktree ONLY
 * if its path still exists — "the /exit dialog may have removed it"). Pure: the
 * existence probe is injected (defaults to {@link fs.existsSync}) so the tri-state
 * boundary (`undefined` never-entered / `null` exited / present-but-gone) is
 * testable without touching the real filesystem.
 */
export function resolveWorktreeStateForResume(
  state: Session['worktreeState'],
  existsOnDisk: (p: string) => boolean = (p) => fs.existsSync(p),
): WorktreeResumeResolution {
  // `undefined` = never entered a worktree; `null` = explicitly exited one.
  if (state === undefined || state === null) return { action: 'none' };
  if (!existsOnDisk(state.worktreePath)) return { action: 'cleared' };
  return {
    action: 'reenter',
    worktreePath: state.worktreePath,
    originalCwd: state.originalCwd,
    branch: state.branch,
  };
}

export function safeCharSlice(s: string, start: number, end?: number): string {
  let result = s.slice(start, end);
  if (result.length === 0) return result;
  const lastCode = result.charCodeAt(result.length - 1);
  if (lastCode >= 0xD800 && lastCode <= 0xDBFF) result = result.slice(0, -1);
  const firstCode = result.charCodeAt(0);
  if (firstCode >= 0xDC00 && firstCode <= 0xDFFF) result = result.slice(1);
  return result;
}

function sanitizeSessionId(id: string): string {
  return id.replace(/[\/\\]/g, '_').replace(/\.\./g, '_');
}

/** Rejects a session id that is unsafe to interpolate into a `${id}.json`
 *  filename: a path separator, NUL, or any C0/C1 control byte or DEL. */
// eslint-disable-next-line no-control-regex
const UNSAFE_SESSION_ID_RE = /[/\\\u0000-\u001f\u007f-\u009f]/;

/**
 * Defensive format-validation boundary for a session id ingested from an untrusted
 * source (a `--resume` argument, a session URL, an on-disk filename). Accepts
 * `unknown` and returns `false` — never throws — for a non-string, an empty or
 * over-long (>255) id, `.`/`..` or anything containing `..`, or any
 * separator/control byte ({@link UNSAFE_SESSION_ID_RE}).
 *
 * Parity: the reference's `validateUuid` boundary guard, adapted to AMI's id contract
 * (AMI ids are `session-…`/user `--session-id` segments, NOT UUIDs). This is the
 * stronger up-front check the old denylist {@link sanitizeSessionId} lacked, while
 * staying consistent with the `isSafeSegment` contract used for `--session-id`
 * (spaces / non-separator Unicode remain valid). Used as the first check in
 * {@link SessionManager.load}/{@link SessionManager.loadAsync}/
 * {@link SessionManager.delete} so garbage is rejected before any path
 * construction (with {@link assertPathWithin} still the containment backstop).
 */
export function isValidSessionId(id: unknown): id is string {
  if (typeof id !== 'string') return false;
  if (id.length === 0 || id.length > 255) return false;
  if (id === '.' || id === '..' || id.includes('..')) return false;
  return !UNSAFE_SESSION_ID_RE.test(id);
}

/**
 * Classification of a `--resume`/`--continue` argument (parity: the reference's
 * `parseSessionIdentifier`): a value the user passes can be a transcript FILE
 * path, a managed session ID, or a cloud-ingress URL.
 *  - `'jsonl'` — a path ending in `.jsonl` → resume from that transcript file and
 *    mint a fresh session id (checked FIRST because a Windows absolute path like
 *    `C:\x\s.jsonl` otherwise parses as a URL with `c:` as the protocol);
 *  - `'id'` — a bare, format-valid managed session id ({@link isValidSessionId});
 *  - `'url'` — an `http`/`https` URL (cloud session ingress).
 */
export type ParsedSessionIdentifier =
  | { kind: 'jsonl'; path: string }
  | { kind: 'id'; id: string }
  | { kind: 'url'; url: string };

/**
 * Classify a `--resume` argument into {@link ParsedSessionIdentifier}, or `null`
 * when it is none of the three forms (a non-string, empty/whitespace, or a
 * path/string that is neither a `.jsonl` file, a safe session id, nor an
 * http(s) URL). The `.jsonl` test runs before URL parsing so a Windows absolute
 * transcript path is never misread as a URL. The id branch reuses the SAME
 * {@link isValidSessionId} safety boundary `load()` uses, so a `--resume <id>`
 * that classifies here is exactly the set of ids `load()` will accept — no
 * second, divergent id grammar (§0b.6).
 *
 * FUNCTIONAL (intentional parity divergence): the URL branch is restricted to
 * `http:`/`https:` schemes (the reference accepts any `new URL()`), so a transport-smuggling
 * scheme (`ext::`, `file:`, `ssh:`) can never be classified as a resumable session
 * URL — the same hardening posture AMI applies to other untrusted URL inputs.
 */
export function parseSessionIdentifier(arg: unknown): ParsedSessionIdentifier | null {
  if (typeof arg !== 'string') return null;
  const value = arg.trim();
  if (!value) return null;
  // JSONL transcript path first — a Windows absolute path parses as a URL otherwise.
  if (value.toLowerCase().endsWith(TRANSCRIPT_FILE_EXTENSION)) {
    return { kind: 'jsonl', path: value };
  }
  // A managed session id — same acceptance boundary as load()/delete().
  if (isValidSessionId(value)) {
    return { kind: 'id', id: value };
  }
  // A cloud-ingress URL (http/https only).
  try {
    const url = new URL(value);
    if (url.protocol === 'http:' || url.protocol === 'https:') {
      return { kind: 'url', url: url.href };
    }
  } catch { /* not a URL */ }
  return null;
}

/** The four persisted message roles ({@link Message}). A loaded transcript entry
 *  whose `role` is not one of these is not a turn the engine can replay, so it is
 *  dropped by {@link sanitizeLoadedSession} rather than cast-trusted. */
const VALID_MESSAGE_ROLES: ReadonlySet<string> = new Set(['user', 'assistant', 'tool', 'system']);

/**
 * True when an UNTRUSTED on-disk value is a minimally well-formed {@link Message}
 * record: a plain object carrying one of the {@link VALID_MESSAGE_ROLES}. Shape
 * beyond `role` is intentionally NOT over-constrained — `content` may be a string,
 * a {@link SessionBlobRef}, or a multimodal array, all already handled downstream —
 * so the guard's job is to reject a tampered non-message blob, not to re-type every
 * field (which would risk dropping legitimately-shaped turns written by a newer build).
 */
function isValidMessageRecord(value: unknown): value is Message {
  return (
    !!value &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    typeof (value as { role?: unknown }).role === 'string' &&
    VALID_MESSAGE_ROLES.has((value as { role: string }).role)
  );
}

/**
 * Validate + allowlist-sanitize a session parsed back from disk BEFORE it is handed
 * to any consumer. The on-disk JSON is UNTRUSTED (it may have been written by a
 * different build, hand-edited, or synced from another machine), so it is structurally
 * validated — not cast-trusted — mirroring the safeParse-before-apply posture AMI
 * already uses for other persisted configs (frito/provision `safeParse`) and the reference's
 * zod-validated resume state (sessionRestore):
 *  - the blob MUST be a plain object with a non-empty string `id`; a shapeless /
 *    top-level-array / id-less payload is REJECTED (returns `null` → the caller
 *    treats it as not-found) instead of being cast to `Session` and resumed;
 *  - `messages` is coerced to an array and every entry that is not a well-formed
 *    {@link isValidMessageRecord} is DROPPED, so a corrupt/tampered transcript can
 *    never inject an arbitrary-shaped "turn" into the restored conversation;
 *  - `settings.permissionMode` must round-trip through {@link normalizePermissionMode}
 *    (the single {@link PERMISSION_MODES} acceptance boundary); an unknown mode is
 *    DROPPED so --resume can never reinstate a bogus or dangerous permission posture
 *    from a foreign/corrupt transcript.
 * Returns the validated {@link Session} (mutated in place — the just-parsed object is
 * not shared) or `null` when the blob is not a usable session. FUNCTIONAL: a
 * structurally-broken session that was previously cast-trusted (non-array `messages`,
 * garbage message entries) is now sanitized/dropped rather than handed to the engine.
 */
function sanitizeLoadedSession(parsed: unknown): Session | null {
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
  const obj = parsed as Record<string, unknown>;
  // A session with no usable id cannot be resolved/rehydrated — reject outright.
  if (typeof obj.id !== 'string' || obj.id.trim() === '') return null;
  // Coerce messages to an array and drop any non-message entry.
  obj.messages = (Array.isArray(obj.messages) ? obj.messages : []).filter(isValidMessageRecord);
  const session = obj as unknown as Session;
  const mode = session.settings?.permissionMode;
  if (typeof mode === 'string' && normalizePermissionMode(mode) === null) {
    delete session.settings!.permissionMode;
  }
  // A tampered/foreign `agent` that is not a plain object (e.g. a string or array)
  // is dropped so consumers never destructure a non-record identity.
  const agent = (obj as { agent?: unknown }).agent;
  if (agent !== undefined && (typeof agent !== 'object' || agent === null || Array.isArray(agent))) {
    delete (obj as { agent?: unknown }).agent;
  }
  return session;
}

/**
 * Normalize a user-supplied tag before it is persisted: strip C0/C1 control bytes
 * (incl. DEL) and invisible/bidirectional Unicode (zero-width, BOM, LRM/RLM, bidi
 * embeddings/overrides/isolates) that could carry a hidden-character attack, then
 * collapse whitespace and trim. Returns `''` when nothing meaningful remains.
 * Parity: the reference's `recursivelySanitizeUnicode(tag).trim()` before `saveTag` — a
 * tag-specific normalizer (does NOT strip markdown the way the title sanitizer
 * does, so a tag token is preserved verbatim).
 */
export function normalizeSessionTag(raw: string): string {
  return raw
    // eslint-disable-next-line no-control-regex
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f-\u009f]/g, '') // C0/C1 ctrl + DEL (keep \t\n\r for the \s+ collapse below)
    .replace(/[\u200b-\u200f\u202a-\u202e\u2060\u2066-\u2069\ufeff]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

/** Outcome of {@link SessionManager.toggleTag}: a 3-way state machine over the
 *  session's single tag (add when none, replace when different, remove when the
 *  same tag is applied again) plus the two input-guard failures. */
export interface TagToggleResult {
  ok: boolean;
  /** What happened on success. Absent on failure. */
  action?: 'added' | 'replaced' | 'removed';
  /** The tag now on the session (absent when removed or on failure). */
  tag?: string;
  /** The tag that was on the session before the op, when there was one. */
  previous?: string;
  /** Guard failure reason: no active session, or an empty-after-normalize tag. */
  error?: string;
}


/**
 * Decide whether a project-slug directory NAME under the global projects root
 * belongs to one of `worktreeCwds`, using the reference's longest-prefix,
 * truncation-aware match (parity: listSessionsImpl's worktree-aware gathering):
 *  - each worktree cwd is reduced to its {@link projectSlug} (the exact slug a
 *    store dir is named after) and the candidates are tried LONGEST slug first so
 *    the most specific worktree wins over a shorter ancestor;
 *  - a dir matches by EXACT equality, or — ONLY when the slug was truncated to
 *    {@link MAX_SLUG_LENGTH} (a `-<hash>` suffix follows) — by `startsWith(slug + '-')`,
 *    so a short `…-project` dir never matches a `…-project-foo` slug;
 *  - on win32 the comparison is case-insensitive.
 * Returns the matched worktree cwd, or `null`. Pure (no fs/git): the caller passes
 * already-canonical cwds, so the matching boundary is unit-testable in isolation.
 */
export function matchWorktreeForProjectDir(
  dirName: string,
  worktreeCwds: string[],
): string | null {
  const caseInsensitive = process.platform === 'win32';
  const norm = (s: string): string => (caseInsensitive ? s.toLowerCase() : s);
  const target = norm(dirName);
  const indexed = worktreeCwds
    .map(cwd => ({ cwd, slug: norm(projectSlug(cwd)) }))
    .sort((a, b) => b.slug.length - a.slug.length);
  for (const { cwd, slug } of indexed) {
    const truncated = slug.length >= MAX_SLUG_LENGTH;
    if (target === slug || (truncated && target.startsWith(slug + '-'))) {
      return cwd;
    }
  }
  return null;
}

/**
 * Minimum interval between two housekeeping (retention) sweeps for one store: 24h.
 * Mirrors the reference's once-per-day cleanup cadence (backgroundHousekeeping's
 * `setInterval(..., 24h)` + marker throttle).
 */
export const HOUSEKEEPING_PERIOD_MS = 24 * 60 * 60 * 1000;

/** Marker file (under the store dir) recording the epoch-ms of the last completed
 *  housekeeping sweep, used to throttle the sweep to once per {@link HOUSEKEEPING_PERIOD_MS}
 *  across concurrent processes. Named with a leading dot + non-`.json` suffix so it is
 *  never picked up by {@link SessionManager.list}'s `.json` filter. */
export const HOUSEKEEPING_MARKER_FILE = '.housekeeping.marker';

/** Sub-directory of the store dir whose {@link SESSION_LOCK_FILE} serializes the
 *  housekeeping sweep across processes (reuses {@link acquireSessionLock} rather
 *  than a second lock primitive — §0b.6). */
export const HOUSEKEEPING_LOCK_SUBDIR = '.housekeeping';

/**
 * True when session retention cleanup is enabled for `cleanupPeriodDays`: a finite,
 * strictly-positive day count. A missing / non-finite / non-positive value DISABLES
 * cleanup (parity: the reference's `cleanupPeriodDays === 0` keeps everything). The ONE
 * acceptance boundary shared by {@link SessionManager.cleanupOldSessions} and the
 * throttled housekeeping runner so the "is cleanup on?" rule lives in one place.
 */
export function isSessionCleanupEnabled(cleanupPeriodDays?: number): boolean {
  return (
    cleanupPeriodDays !== undefined &&
    Number.isFinite(cleanupPeriodDays) &&
    cleanupPeriodDays > 0
  );
}

/** Sub-directory of a session store holding per-session content-addressed blob
 *  sidecars — one `<sessionId>/` dir per session (see
 *  {@link SessionManager.offloadMessage}). The ONE place the name lives so
 *  {@link SessionManager.blobDirFor} and {@link SessionManager.cleanupOrphanedBlobs}
 *  can never disagree about where blobs are stored (§0b.6). */
export const BLOBS_SUBDIR = 'blobs';

/**
 * Of a cache base's per-session sub-directory names, the ones that are ORPHANED —
 * whose name is not in `liveIds` (the session ids that still have a surviving owner
 * on disk). The pure cross-session cache-GC boundary behind
 * {@link SessionManager.cleanupOrphanedBlobs}, so the "which dirs are stale?" rule is
 * unit-testable without touching the filesystem.
 *
 * Parity: the reference's `cleanupOldImageCaches` sweep (which drops every session dir that
 * is not the CURRENT one). AMI diverges intentionally — its per-session dir is a
 * content-addressed BLOB store backing a RESUMABLE transcript, not an ephemeral
 * pasted-image cache — so EVERY live session's dir is kept (deleting a non-current
 * session's blobs would corrupt its `--resume`) and only truly owner-less dirs are
 * swept. A `"current only"` caller just passes a single-element iterable.
 */
export function collectOrphanedCacheDirs(dirNames: string[], liveIds: Iterable<string>): string[] {
  const live = liveIds instanceof Set ? liveIds : new Set(liveIds);
  return dirNames.filter(name => !live.has(name));
}

/**
 * Decide whether a daily housekeeping sweep is due given the last-run timestamp
 * (`null` when never run / no marker) and the current time. Due when: never run,
 * at least `periodMs` has elapsed since the last run, OR the recorded timestamp is
 * in the FUTURE relative to `now` (a corrupt / clock-skewed marker — treated as due
 * so a bad marker self-heals on the next sweep instead of wedging cleanup forever).
 * Pure — the single throttle predicate used by {@link maybeRunDailyHousekeeping}.
 */
export function isHousekeepingDue(
  lastRunMs: number | null,
  now: number,
  periodMs: number = HOUSEKEEPING_PERIOD_MS,
): boolean {
  if (lastRunMs === null) return true;
  if (lastRunMs > now) return true; // future/corrupt marker — self-heal
  return now - lastRunMs >= periodMs;
}

/**
 * Injectable dependencies for {@link maybeRunDailyHousekeeping} so the throttle +
 * cross-process-skip boundary is unit-testable without real time, real marker I/O,
 * or a real cross-process lock race.
 */
export interface HousekeepingDeps {
  /** Current time source (epoch ms). */
  now: () => number;
  /** Throttle period (defaults to {@link HOUSEKEEPING_PERIOD_MS}). */
  periodMs: number;
  /** Read the last-run timestamp (epoch ms) from the marker, or `null` when
   *  absent/unreadable/non-numeric. */
  readMarker: (markerPath: string) => number | null;
  /** Persist the last-run timestamp to the marker (best-effort). */
  writeMarker: (markerPath: string, ts: number) => void;
  /** Acquire the cross-process housekeeping lock; `acquired:false` means another
   *  live process holds it (skip this sweep). */
  acquireLock: (lockDir: string) => { acquired: boolean; release: () => void };
}

function defaultReadHousekeepingMarker(markerPath: string): number | null {
  try {
    const n = parseInt(fs.readFileSync(markerPath, 'utf-8').trim(), 10);
    return Number.isFinite(n) ? n : null;
  } catch {
    return null; // absent / unreadable — treat as never-run
  }
}

function defaultWriteHousekeepingMarker(markerPath: string, ts: number): void {
  try {
    atomicWriteFileSync(markerPath, String(ts));
  } catch {
    /* best-effort — a failed marker write just means the next process re-runs */
  }
}

/**
 * Run `task` AT MOST once per {@link HousekeepingDeps.periodMs} across all processes
 * sharing `markerDir`, skipping immediately when another process is already running
 * it. This reproduces the reference's recurring-cleanup concurrency boundary
 * ("marker files and locks to throttle to once per day and skip immediately if
 * another process holds the lock", backgroundHousekeeping):
 *   1. read the day-marker — if a sweep ran within the period, skip (`false`);
 *   2. try to take the cross-process housekeeping lock — if another live process
 *      holds it, skip (`false`) rather than piling on a concurrent sweep;
 *   3. re-check the marker UNDER the lock (a racer may have finished between the
 *      first check and winning the lock) — skip if no longer due;
 *   4. run `task()`, stamp the marker with the completion time, release the lock.
 * Returns `true` only when `task` actually ran. Reuses {@link acquireSessionLock}
 * (pointed at a dedicated {@link HOUSEKEEPING_LOCK_SUBDIR}) so the stale-lock
 * reclaim / cross-host logic is shared, not re-implemented (§0b.6).
 */
export function maybeRunDailyHousekeeping(
  markerDir: string,
  task: () => void,
  depsOverride: Partial<HousekeepingDeps> = {},
): boolean {
  const deps: HousekeepingDeps = {
    now: Date.now,
    periodMs: HOUSEKEEPING_PERIOD_MS,
    readMarker: defaultReadHousekeepingMarker,
    writeMarker: defaultWriteHousekeepingMarker,
    acquireLock: (lockDir) => acquireSessionLock(lockDir),
    ...depsOverride,
  };
  const markerPath = path.join(markerDir, HOUSEKEEPING_MARKER_FILE);
  if (!isHousekeepingDue(deps.readMarker(markerPath), deps.now(), deps.periodMs)) {
    return false;
  }
  const lock = deps.acquireLock(path.join(markerDir, HOUSEKEEPING_LOCK_SUBDIR));
  if (!lock.acquired) return false; // another process is already sweeping
  try {
    // Double-check under the lock: a racer may have completed the sweep (and
    // stamped the marker) between our due-check above and winning the lock.
    if (!isHousekeepingDue(deps.readMarker(markerPath), deps.now(), deps.periodMs)) {
      return false;
    }
    task();
    deps.writeMarker(markerPath, deps.now());
    return true;
  } finally {
    lock.release();
  }
}

export class SessionManager {
  private sessionDir: string;
  private cwd: string;
  private _gitBranch?: string;
  private _gitBranchResolved = false;
  private static _idCounter = 0;
  /** Single-writer ownership held by THIS manager: the monotonic fence won at
   *  {@link acquireOwnership} + the lock release. Absent until ownership is taken
   *  (every direct `new SessionManager().save()` leaves this unset → no fence
   *  assert, unchanged behavior). */
  private _ownership?: { fence: number; release: () => void };

  constructor(cwd: string, sessionDirOverride?: string) {
    // Resume blobs live GLOBALLY under ~/.ami/projects/<slug>/sessions (like the reference's
    // ~/.claude/projects/<slug>/), not inside the repo. The slug is keyed on the CANONICAL
    // git root so every worktree/subdirectory of one repo shares a single session store
    // (resume a session started in any worktree of the repo), not a fragment per directory.
    this.cwd = cwd;
    this.sessionDir = sessionDirOverride ?? projectStateDir(canonicalGitRoot(cwd), 'sessions');
  }

  /** Git branch for the session's cwd, resolved once per manager and memoized —
   *  saves run many times per turn, so we must not re-spawn `git` each time. */
  private gitBranch(): string | undefined {
    if (!this._gitBranchResolved) {
      this._gitBranch = getGitBranch(this.cwd);
      this._gitBranchResolved = true;
    }
    return this._gitBranch;
  }

  /**
   * Ensure the session directory exists (owner-only, 0o700).
   */
  private ensureDir(): void {
    secureMkdirSync(this.sessionDir);
  }

  /**
   * Take hard single-writer ownership of this session directory: acquire the
   * cross-process {@link acquireSessionLock} (a fail-closed, PID-reuse-safe CAS —
   * a stale/dead/reused prior owner is reclaimed, a provably-live or unverifiable
   * one blocks) and remember the monotonic {@link SessionLockPayload.runtimeFence}
   * it won. Once held, every {@link save} asserts the fence first, so a superseded
   * owner (another `ami --resume`/detached run took over and advanced the fence)
   * is rejected instead of clobbering the new owner's transcript.
   *
   * Idempotent — a second call with ownership already held is a no-op reporting the
   * held fence. When the lock is force-disabled ({@link isSessionLockingDisabled})
   * or the acquire returns no fence, no ownership is recorded and `save` stays
   * unguarded (unchanged behavior). When a live OTHER holder blocks, returns
   * `{ acquired:false, blocked:true, message }` without recording ownership.
   */
  acquireOwnership(depsOverride?: Partial<SessionLockDeps>): {
    acquired: boolean;
    blocked?: boolean;
    fence?: number;
    message?: string;
  } {
    if (this._ownership) return { acquired: true, fence: this._ownership.fence };
    this.ensureDir();
    const result = acquireSessionLock(this.sessionDir, depsOverride);
    if (!result.acquired) {
      return { acquired: false, blocked: true, message: result.message };
    }
    // Only a numeric fence is enforceable; a disabled-lock no-op acquire carries
    // none, so we record nothing and leave save() unguarded (unchanged behavior).
    if (typeof result.runtimeFence === 'number') {
      this._ownership = { fence: result.runtimeFence, release: result.release };
      return { acquired: true, fence: result.runtimeFence };
    }
    return { acquired: true };
  }

  /** The monotonic fence this manager currently owns, or `undefined` if it holds
   *  no enforceable ownership (never acquired, or acquired with locking disabled). */
  ownershipFence(): number | undefined {
    return this._ownership?.fence;
  }

  /**
   * Assert this manager still owns the session at the fence it won. A no-op when
   * no enforceable ownership is held; otherwise throws {@link SessionFenceError}
   * if a newer owner has superseded this one. This is the single-writer guard
   * {@link save} runs before writing and that the engine wires into the
   * {@link import('../../../common/src/checkpoint').CheckpointManager} ownership
   * guard so an in-flight rollback from a superseded owner is rejected too.
   */
  assertOwnership(): void {
    if (this._ownership) assertSessionFence(this.sessionDir, this._ownership.fence);
  }

  /** A bound {@link assertOwnership} thunk for consumers that take a `() => void`
   *  ownership guard (e.g. the checkpoint manager), so the fence is enforced on
   *  file mutations too — not just transcript saves. */
  ownershipGuard(): () => void {
    return () => this.assertOwnership();
  }

  /** Release this manager's single-writer ownership (unlink the lock if still ours)
   *  and forget the held fence. Idempotent; a no-op when no ownership is held. The
   *  process-exit hook in {@link acquireSessionLock} is the backstop, but a clean
   *  engine shutdown should release eagerly so a peer can take over at once. */
  releaseOwnership(): void {
    if (!this._ownership) return;
    const { release } = this._ownership;
    this._ownership = undefined;
    try { release(); } catch { /* best effort — exit hook is the backstop */ }
  }

  /**
   * Save a session to disk as a JSON file.
   * Message content longer than 5000 chars is offloaded to a content-addressed
   * blob sidecar (full content preserved) with only a preview kept inline, so
   * --resume restores the complete conversation (H14).
   */
  save(session: Session): void {
    // Honour an opt-out of transcript persistence (privacy / ephemeral runs):
    // write nothing to disk when disabled. Checked first so no dir is created.
    if (isSessionPersistenceDisabled()) return;
    // Single-writer fence assert: when this manager holds ownership, reject the
    // save if a newer owner took over (fence advanced on the lock) BEFORE any I/O,
    // so a superseded process can never clobber the new owner's transcript. A
    // manager without acquired ownership (every direct `save()`) is a no-op here.
    this.assertOwnership();
    this.ensureDir();
    const safeId = sanitizeSessionId(session.id);
    const blobDir = this.blobDirFor(safeId);
    // Canonical save-persistence filter (parity: the reference's isLoggableMessage): drop
    // engine-injected turn-local nudges (isMeta) and hook-produced additional-context
    // messages (unless the operator opted in to persist them across --resume) BEFORE
    // offloading, so the saved transcript carries only loggable messages.
    const storedMessages = session.messages
      .filter(m => isLoggableMessage(m))
      .map(m => this.offloadMessage(m, blobDir));
    const filePath = path.join(this.sessionDir, `${safeId}.json`);
    assertPathWithin(filePath, this.sessionDir);
    // A user rename (customTitle) must never be clobbered by an auto-title save:
    // if this save doesn't carry one, preserve whatever is already on disk.
    const customTitle = session.customTitle ?? this.readSessionFile(filePath, false)?.customTitle;
    // Serialize all scalar metadata BEFORE the (potentially huge) `messages` array
    // so a bounded head read ({@link readSessionMetaLite} / LITE_READ_BUF_SIZE) always
    // captures the session's metadata without parsing the whole transcript. Pulling
    // `messages` out of the spread guarantees it is emitted LAST regardless of the
    // caller's key insertion order (a spread would otherwise keep its original slot).
    const scalarMeta: Record<string, unknown> = { ...session };
    delete scalarMeta.messages;
    const data = {
      ...scalarMeta,
      id: safeId,
      customTitle,
      updatedAt: new Date().toISOString(),
      // Stamp provenance so /resume can show branch/project and migrations can
      // detect old schemas. Caller-supplied values win; otherwise derive from cwd.
      gitBranch: session.gitBranch ?? this.gitBranch(),
      projectPath: session.projectPath ?? this.cwd,
      version: session.version ?? AMI_VERSION,
      // Classify the run: a caller-supplied kind wins; else honor an env override
      // (AMI_SESSION_KIND / CLAUDE_CODE_SESSION_KIND); else default foreground.
      kind: session.kind ?? envSessionKind() ?? 'interactive',
      // Emitted LAST (see above) so metadata stays in the head buffer.
      messages: storedMessages,
    };
    // Crash-safe + owner-only: a truncated/corrupt newest session can never
    // result from an interrupted write, and transcripts stay 0o600 on shared hosts.
    // NDJSON-safe serialization escapes U+2028/U+2029 so content can't tear a line.
    atomicWriteFileSync(filePath, ndjsonSafeStringify(data, 2));
  }

  /** Directory holding a session's content-addressed blobs. */
  private blobDirFor(safeId: string): string {
    return path.join(this.sessionDir, BLOBS_SUBDIR, safeId);
  }

  /**
   * Offload an over-long string content to a blob, returning a message whose
   * content is a {@link SessionBlobRef}. Compaction summaries stay inline in
   * full; assistant content is left untouched (it was never truncated). If the
   * blob write fails we fall back to an inline truncation so a save never
   * crashes or bloats the JSON.
   */
  private offloadMessage(msg: Message, blobDir: string): Message {
    if (
      msg.role === 'system' &&
      typeof msg.content === 'string' &&
      msg.content.startsWith(COMPACTION_SUMMARY_PREFIX)
    ) {
      return msg;
    }
    if (
      (msg.role === 'user' || msg.role === 'system' || msg.role === 'tool') &&
      typeof msg.content === 'string' &&
      msg.content.length > MAX_CONTENT_LENGTH
    ) {
      const full = msg.content;
      const preview = safeCharSlice(full, 0, MAX_CONTENT_LENGTH);
      try {
        const sha = sha256Hex(full);
        secureMkdirSync(blobDir);
        const blobPath = path.join(blobDir, `${sha}.blob`);
        // Content-addressed: identical content across saves dedupes to one file.
        // Crash-safe + owner-only — blobs hold the FULL untruncated tool output /
        // user content, which routinely contains secrets and file contents.
        if (!fs.existsSync(blobPath)) atomicWriteFileSync(blobPath, full);
        const ref: SessionBlobRef = { __siSessionBlob: sha, preview, length: full.length };
        return { ...msg, content: ref as unknown as string };
      } catch {
        return { ...msg, content: preview + '\n... (truncated)' };
      }
    }
    return msg;
  }

  /** Rehydrate a stored message, replacing any {@link SessionBlobRef} content
   *  with the full blob contents (or the preview if the blob is unavailable). */
  private rehydrateMessage(msg: Message, blobDir: string): Message {
    const stored = (msg as { content?: unknown }).content;
    if (!isSessionBlobRef(stored)) return msg;
    let bytes: string | null;
    try {
      bytes = fs.readFileSync(path.join(blobDir, `${stored.__siSessionBlob}.blob`), 'utf-8');
    } catch {
      bytes = null;
    }
    return applyRehydratedBlob(msg, stored, bytes);
  }

  /** Async counterpart to {@link rehydrateMessage}: reads the blob sidecar off
   *  the event loop (so a transcript with many/large blobs doesn't block the
   *  caller) and reuses {@link applyRehydratedBlob} so the fallback text is
   *  byte-for-byte identical to the sync path. */
  private async rehydrateMessageAsync(msg: Message, blobDir: string): Promise<Message> {
    const stored = (msg as { content?: unknown }).content;
    if (!isSessionBlobRef(stored)) return msg;
    let bytes: string | null;
    try {
      bytes = await fs.promises.readFile(path.join(blobDir, `${stored.__siSessionBlob}.blob`), 'utf-8');
    } catch {
      bytes = null;
    }
    return applyRehydratedBlob(msg, stored, bytes);
  }

  /**
   * Load the most recently modified session file.
   */
  loadLatest(opts: { skipBackground?: boolean; skipLocked?: boolean; lockDeps?: Partial<SessionLockDeps> } = {}): Session | null {
    // Liveness guard for a no-arg `--continue`: when another live engine is
    // actively holding THIS store's lock, auto-resuming its newest session would
    // interleave two engines onto the same transcript/blob dir and corrupt it.
    // `checkSessionLock` returns `'blocked'` ONLY for a live, verified OTHER holder
    // (a dead / reused / cross-host-stale lock is reclaimed in-place and reads as
    // free), so a crashed session never wedges --continue (parity: the reference's
    // --continue skip of a session another live process is writing).
    if (opts.skipLocked && checkSessionLock(this.sessionDir, opts.lockDeps).kind === 'blocked') {
      return null;
    }
    const files = this.listFiles();
    if (files.length === 0) return null;

    // Sort by modification time descending
    files.sort((a, b) => {
      try {
        return fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs;
      } catch {
        return 0;
      }
    });

    // Return the first file that parses successfully, falling through past any
    // corrupt/truncated newest file (e.g. a save() interrupted mid-write) so
    // intact older sessions are not hidden by a single unreadable file. With
    // `skipBackground` (the no-arg `--continue` path) a `'bg'`/`'daemon'` session
    // is passed over so auto-resume picks the most-recent FOREGROUND session, not
    // a still-running backgrounded one (parity: the reference's --continue bg skip).
    for (const file of files) {
      const session = this.readSessionFile(file);
      if (!session) continue;
      if (opts.skipBackground && isBgSession(session)) continue;
      return session;
    }
    return null;
  }

  /**
   * Load a specific session by ID.
   */
  load(id: string): Session | null {
    // Allowlist-validate the id up front: a malformed/untrusted resume arg is
    // rejected before any path construction (the stronger posture over the
    // denylist sanitizer; assertPathWithin remains the containment backstop).
    if (!isValidSessionId(id)) return null;
    const safeId = sanitizeSessionId(id);
    const filePath = path.join(this.sessionDir, `${safeId}.json`);
    assertPathWithin(filePath, this.sessionDir);
    if (!fs.existsSync(filePath)) return null;
    return this.readSessionFile(filePath);
  }

  /**
   * Async counterpart to {@link load}: lazily read a session's full transcript
   * (and rehydrate its content-addressed blob sidecars) off the main thread, so
   * resuming a large/blob-heavy conversation does not block the event loop /
   * freeze the TUI while the body streams in from disk. Returns `null` for a
   * missing, oversized (> {@link MAX_SESSION_READ_BYTES}), or corrupt file —
   * exactly like {@link load}, just non-blocking.
   *
   * Parity: the reference's async `loadFullLog` — a picker shows the cheap lite entry
   * ({@link SessionListEntry} from {@link list}) immediately and only awaits the
   * full message load when a session is actually opened, enabling a
   * "Loading session…" affordance in front of a potentially slow read.
   */
  async loadAsync(id: string): Promise<Session | null> {
    // Same allowlist boundary as the sync load() so both reject identical garbage.
    if (!isValidSessionId(id)) return null;
    const safeId = sanitizeSessionId(id);
    const filePath = path.join(this.sessionDir, `${safeId}.json`);
    assertPathWithin(filePath, this.sessionDir);
    return this.readSessionFileAsync(filePath);
  }

  /**
   * List sessions with metadata (id, date, first message preview).
   *
   * Two-pass for cost: a cheap stat-only candidate pass (1 syscall/file, no
   * content read) sorts by mtime descending, then only the `offset..offset+limit`
   * window is actually JSON-parsed — so `list({ limit })` over a large store reads
   * N stats + ~limit parses instead of parsing every file. With no `limit` the
   * full set is parsed (unchanged behaviour). The returned entries are always
   * sorted by recorded date descending so callers see a stable order regardless
   * of the mtime pre-sort.
   */
  list(opts: {
    limit?: number;
    offset?: number;
    includeSidechain?: boolean;
    dropEmpty?: boolean;
    /** Drop a specific session id from the listing — used to exclude the LIVE
     *  current session from its own resume picker so a user can't "resume" the
     *  session they're already in (parity: the reference excludes the current log). */
    excludeId?: string;
    /** Keep only sessions linked to a PR, per {@link PrFilter} (true = any PR, a
     *  number = that PR, a string = parsed via {@link parsePrIdentifier}). */
    filterByPr?: PrFilter;
    /** Keep only sessions whose recorded {@link Session.gitBranch} equals this
     *  branch — the data-model side of the resume picker's branch-scope toggle
     *  (parity: the reference's LogSelector Ctrl+B `branchFilterEnabled`
     *  `log.gitBranch === currentBranch`). A session with no recorded branch never
     *  matches an active branch filter. */
    filterByBranch?: string;
    /** Keep only sessions whose recorded {@link Session.tag} equals this tag — the
     *  data-model side of the resume picker's per-#tag tab (parity: the reference's TagTabs
     *  per-tag filtering). An empty/whitespace value is ignored (no constraint). */
    filterByTag?: string;
    /** How many of the most-recent rows are ENRICHED with a full parse (real
     *  first/last-prompt preview + message count); rows past this count — and any
     *  file at/above {@link SKIP_PRECOMPACT_THRESHOLD} regardless of position — are
     *  built from the cheap bounded {@link readSessionMetaLite} path instead. Defaults
     *  to {@link INITIAL_ENRICH_COUNT}. Pass `Infinity` to force a full parse of every
     *  row (the pre-enrichment behaviour). Parity: the reference's progressive enrichment. */
    enrichCount?: number;
    /** Optional diagnostics sink: when supplied, {@link list} populates it with the
     *  found/listed/skipped-by-reason counts for this pass (the introspection surface
     *  behind {@link listWithDiagnostics} + the `/sessions` result's `diagnostics`),
     *  so a session that is silently filtered out becomes diagnosable. */
    diagnostics?: SessionListDiagnostics;
  } = {}): SessionListEntry[] {
    const files = this.listFiles();
    // Diagnostics sink (parity: the reference's resume-discovery debug counts). `found` is
    // every discovered file; `skip`/`skipN` tally dropped rows by reason as the pass
    // runs; `listed` is stamped with the final visible count before returning.
    const diag = opts.diagnostics;
    if (diag) {
      diag.found = files.length;
      diag.listed = 0;
      diag.skipped = 0;
      diag.skippedByReason = {};
    }
    const skip = (reason: SessionSkipReason): void => {
      if (!diag) return;
      diag.skipped += 1;
      diag.skippedByReason[reason] = (diag.skippedByReason[reason] ?? 0) + 1;
    };
    const skipN = (reason: SessionSkipReason, n: number): void => {
      if (!diag || n <= 0) return;
      diag.skipped += n;
      diag.skippedByReason[reason] = (diag.skippedByReason[reason] ?? 0) + n;
    };

    // Cheap stat-only pass: carry mtime (for candidate ordering) + size (reused
    // for fileSize so we never stat a file twice). A stat race just omits the file.
    const candidates = files.map(filePath => {
      let mtimeMs = 0;
      let size: number | undefined;
      try {
        const st = fs.statSync(filePath);
        mtimeMs = st.mtimeMs;
        size = st.size;
      } catch { /* stat race — treat as oldest / size-less */ }
      return { filePath, mtimeMs, size };
    });
    candidates.sort((a, b) => b.mtimeMs - a.mtimeMs);

    const offset = opts.offset !== undefined && opts.offset > 0 ? opts.offset : 0;
    const window = opts.limit === undefined
      ? candidates.slice(offset)
      : candidates.slice(offset, offset + Math.max(0, opts.limit));

    // Dedup by the session's INTERNAL id keeping the newest readable copy: because
    // `candidates` is mtime-descending, the FIRST successful read of an id is the
    // newest valid copy; a later/staler file carrying the same id is dropped. The
    // dedup is POST-read (an id is not dropped by an unreadable newer copy), parity
    // with the reference's applySortAndLimit `seen` set. (AMI stores one id per filename,
    // so this reconciles a manually-copied/stale duplicate rather than being the
    // common path.)
    const seenIds = new Set<string>();
    const entries: SessionListEntry[] = [];
    // Progressive enrichment: full-parse the most-recent `enrichCount` rows (real
    // preview + message count); older rows, and any pathologically large file, come
    // from the cheap bounded lite read so a huge store doesn't parse every file.
    const enrichCount = opts.enrichCount ?? INITIAL_ENRICH_COUNT;
    let enriched = 0;
    for (const { filePath, size: fileSize } of window) {
      const lite = enriched >= enrichCount
        || (fileSize !== undefined && fileSize >= SKIP_PRECOMPACT_THRESHOLD);
      if (lite) {
        const entry = this.liteEntry(filePath, fileSize);
        if (!entry) { skip('unreadable'); continue; }
        if (entry.isSidechain && !opts.includeSidechain) { skip('sidechain'); continue; }
        const liteKey = String(entry.id);
        if (opts.excludeId !== undefined && liteKey === String(opts.excludeId)) { skip('current-session'); continue; }
        if (seenIds.has(liteKey)) { skip('duplicate-id'); continue; }
        seenIds.add(liteKey);
        if (opts.dropEmpty && entry.summary === undefined) { skip('empty'); continue; }
        entries.push(entry);
        continue;
      }

      // Preview only needs inline data — skip blob rehydration for speed.
      const session = this.readSessionFile(filePath, false);
      if (!session) { skip('unreadable'); continue; }

      // Subagent sidechain transcripts never pollute the resume picker unless the
      // caller explicitly opts in (parity: the reference filters `isSidechain` logs out).
      if (session.isSidechain && !opts.includeSidechain) { skip('sidechain'); continue; }

      const idKey = String(session.id);
      // Exclude the live current session so it can't appear in its own picker.
      if (opts.excludeId !== undefined && idKey === String(opts.excludeId)) { skip('current-session'); continue; }
      if (seenIds.has(idKey)) { skip('duplicate-id'); continue; }
      seenIds.add(idKey);

      enriched++;
      const preview = this.extractPreview(session);
      const lastPrompt = extractLastPrompt(session.messages);
      const summary = resolveSessionSummary(session, { lastPrompt, firstPrompt: preview });
      // Optionally drop a metadata-only session (no title and no real prompts),
      // matching the reference's listSessionsImpl which returns null for such logs.
      if (opts.dropEmpty && summary === undefined) { skip('empty'); continue; }

      // messageCount / gitBranch / metadata come at ~zero extra cost from the
      // already-parsed inline JSON; fileSize is reused from the stat pass.
      entries.push({
        id: session.id,
        date: session.updatedAt || session.createdAt,
        preview,
        lastPrompt,
        summary,
        messageCount: Array.isArray(session.messages) ? session.messages.length : 0,
        fileSize,
        gitBranch: session.gitBranch,
        title: resolveSessionTitle(session),
        tag: session.tag,
        mode: session.mode,
        pr: session.pr,
        worktree: session.worktreeState ? session.worktreeState.worktreePath : undefined,
        projectPath: session.projectPath,
        entrypoint: session.entrypoint,
        kind: session.kind,
        permissionMode: session.settings?.permissionMode,
        isSidechain: session.isSidechain,
        parentSessionId: session.parentSessionId,
        agent: toListEntryAgent(session.agent),
      });
    }

    // Branch / tag scope filters (parity: the reference's LogSelector Ctrl+B branch filter
    // and TagTabs per-#tag filtering) — applied over the parsed entries before the
    // PR filter. An entry with no recorded branch/tag never matches an active filter.
    let scoped = entries;
    if (opts.filterByBranch !== undefined) {
      const branch = opts.filterByBranch;
      const before = scoped.length;
      scoped = scoped.filter(e => e.gitBranch === branch);
      skipN('branch-filter', before - scoped.length);
    }
    if (opts.filterByTag !== undefined && opts.filterByTag.trim() !== '') {
      const tag = opts.filterByTag;
      const before = scoped.length;
      scoped = scoped.filter(e => e.tag === tag);
      skipN('tag-filter', before - scoped.length);
    }

    // PR scope filter (parity: the reference's filteredLogs PR filter) — applied after the
    // read pass so the single {@link filterSessionsByPr} acceptance boundary is reused.
    const filtered = opts.filterByPr !== undefined
      ? filterSessionsByPr(scoped, opts.filterByPr)
      : scoped;
    skipN('pr-filter', scoped.length - filtered.length);

    // Sort by date descending (most recent first), ties broken on id for a stable,
    // deterministic paginated window — via the shared {@link compareSessionEntriesByDate}
    // comparator (reused by the cross-project aggregator so ordering never diverges).
    filtered.sort(compareSessionEntriesByDate);
    if (diag) diag.listed = filtered.length;
    return filtered;
  }

  /**
   * {@link list} plus the resume-discovery {@link SessionListDiagnostics} for the
   * same pass — the introspection surface that answers "why didn't my session show up
   * in /resume?" (parity: the reference's `/resume: enriched N, M filtered out, K visible`
   * debug counts). Returns the visible entries AND a found/listed/skipped-by-reason
   * tally, so `/sessions` can report how many files were on disk and why each hidden
   * one was dropped, instead of silently omitting it. The entries are byte-for-byte
   * what {@link list} returns with the same `opts` (the shared loop fills the sink —
   * no second enumeration, no divergent filtering).
   */
  listWithDiagnostics(
    opts: Parameters<SessionManager['list']>[0] = {},
  ): { entries: SessionListEntry[]; diagnostics: SessionListDiagnostics } {
    const diagnostics: SessionListDiagnostics = { found: 0, listed: 0, skipped: 0, skippedByReason: {} };
    const entries = this.list({ ...opts, diagnostics });
    return { entries, diagnostics };
  }

  /**
   * Enumerate the distinct {@link Session.tag}s in this store with a per-tag count,
   * most-used first (ties broken alphabetically). Backs the resume picker's tab
   * strip — an `All` tab plus one `#tag` tab per tag (parity: the reference's TagTabs tab
   * model). Reads inline JSON only (no blob rehydration) via the same cheap read
   * {@link list} uses, so building the tab set over a large store stays cheap; a
   * session with no tag is ignored. The returned `tag` values can be passed back as
   * {@link list}'s `filterByTag` to realize the selected tab.
   */
  listTags(): Array<{ tag: string; count: number }> {
    const counts = new Map<string, number>();
    for (const file of this.listFiles()) {
      const s = this.readSessionFile(file, false);
      if (!s || typeof s.tag !== 'string' || !s.tag) continue;
      counts.set(s.tag, (counts.get(s.tag) ?? 0) + 1);
    }
    return [...counts.entries()]
      .map(([tag, count]) => ({ tag, count }))
      .sort((a, b) => (b.count - a.count) || a.tag.localeCompare(b.tag));
  }

  /**
   * Delete a session file by ID.
   */
  delete(id: string): void {
    // Reject a malformed id up front — same allowlist boundary as load().
    if (!isValidSessionId(id)) return;
    const safeId = sanitizeSessionId(id);
    const filePath = path.join(this.sessionDir, `${safeId}.json`);
    assertPathWithin(filePath, this.sessionDir);
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
    // Remove the session's blob sidecar so deleted content is not left on disk.
    const blobDir = this.blobDirFor(safeId);
    assertPathWithin(blobDir, this.sessionDir);
    try { fs.rmSync(blobDir, { recursive: true, force: true }); } catch { /* best effort */ }
  }

  /**
   * Session retention: delete persisted sessions whose last on-disk activity is
   * older than `cleanupPeriodDays` days, returning the number removed. A missing,
   * non-finite, or non-positive value DISABLES cleanup (returns 0 immediately) —
   * matching the reference, where `cleanupPeriodDays === 0` keeps everything and
   * the persistence skip is a separate concern ({@link isSessionPersistenceDisabled}).
   *
   * Cheap + best-effort: it relies on the stat-only mtime (set by every
   * crash-safe {@link save}/resume write) instead of parsing each file, skips a
   * file that vanishes mid-sweep (concurrent delete race), and routes each
   * removal through {@link delete} so the content-addressed blob sidecar is
   * cleaned up alongside the JSON. Parity: the reference's cleanupPeriodDays retention.
   */
  cleanupOldSessions(cleanupPeriodDays?: number): number {
    if (!isSessionCleanupEnabled(cleanupPeriodDays)) {
      return 0;
    }
    const cutoff = Date.now() - cleanupPeriodDays! * 24 * 60 * 60 * 1000;
    let removed = 0;
    for (const file of this.listFiles()) {
      let mtimeMs: number;
      try {
        mtimeMs = fs.statSync(file).mtimeMs;
      } catch {
        continue; // vanished / stat race — skip
      }
      if (mtimeMs >= cutoff) continue; // still within the retention window
      const safeId = path.basename(file).replace(/\.json$/, '');
      // Strict content guard before deleting (parity: the reference's `/^\d+\.json$/`
      // filename check before its parseInt stale sweep — adapted to AMI's non-numeric
      // ids): only remove a file whose stem is a valid session id AND whose contents
      // actually parse as a session (an `id` + `messages` array). This guarantees a
      // foreign/user `.json` that happens to sit in the session dir past the retention
      // window is NEVER deleted by cleanup. The parse is paid only for already-expired
      // files (rare), so the sweep stays cheap in the common case.
      if (!isValidSessionId(safeId)) continue;
      const parsed = this.readSessionFile(file, false);
      if (!parsed || typeof parsed.id !== 'string' || !Array.isArray(parsed.messages)) continue;
      try {
        this.delete(safeId);
        removed++;
      } catch { /* best effort — one undeletable file must not abort the sweep */ }
    }
    return removed;
  }

  /**
   * Throttled, cross-process-safe variant of {@link cleanupOldSessions}: run the
   * retention sweep for THIS store AT MOST once per {@link HOUSEKEEPING_PERIOD_MS},
   * skipping immediately when another process is already sweeping. Without this,
   * every engine start (and every one of N concurrent processes) ran a full sweep;
   * now a day-marker + lock in the store dir throttle it to once per day per store
   * with no concurrent pile-up (parity: the reference's backgroundHousekeeping marker +
   * lock-skip boundary). Returns `{ ran, removed }`: `ran` is false when the sweep
   * was throttled or another process held the lock (in which case `removed` is 0).
   * Retention still DISABLED (no `cleanupPeriodDays`) short-circuits before touching
   * the marker/lock, so the default keep-everything behaviour is unchanged.
   */
  cleanupOldSessionsThrottled(
    cleanupPeriodDays?: number,
    depsOverride?: Partial<HousekeepingDeps>,
  ): { ran: boolean; removed: number } {
    if (!isSessionCleanupEnabled(cleanupPeriodDays)) {
      return { ran: false, removed: 0 };
    }
    let removed = 0;
    const ran = maybeRunDailyHousekeeping(
      this.sessionDir,
      () => { removed = this.cleanupOldSessions(cleanupPeriodDays); },
      depsOverride,
    );
    return { ran, removed };
  }

  /**
   * Garbage-collect ORPHANED per-session blob directories under
   * `<sessionDir>/{@link BLOBS_SUBDIR}/`: a `<sessionId>/` blob dir whose owning
   * `<sessionId>.json` session file no longer exists (the session was deleted
   * out-of-band, crash-orphaned before its JSON landed, or synced without its blobs)
   * is removed, and the blob base dir itself is `rmdir`'d when it ends up empty.
   * Returns the number of blob dirs swept.
   *
   * This is the AMI-faithful analog of the reference's cross-session cache GC
   * (`cleanupOldImageCaches`, which deletes every NON-current session's image-cache
   * dir then rmdir's the base when empty). AMI diverges intentionally: its per-session
   * dir is a content-addressed BLOB store backing a RESUMABLE transcript, not an
   * ephemeral pasted-image cache — so every LIVE session's blobs are kept (deleting a
   * non-current session's blobs would corrupt its `--resume`) and only owner-less dirs
   * are swept. Which dirs are stale is decided by the pure
   * {@link collectOrphanedCacheDirs} boundary. Best-effort: a stat/read race or an
   * undeletable dir is skipped (never aborting the sweep), a stray non-dir entry under
   * the base is left untouched, and an unreadable/absent base degrades to `0`. Each
   * removal is {@link assertPathWithin}-guarded so the sweep can never escape the store.
   */
  cleanupOrphanedBlobs(): number {
    const blobsBase = path.join(this.sessionDir, BLOBS_SUBDIR);
    let dirNames: string[];
    try {
      dirNames = fs.readdirSync(blobsBase);
    } catch {
      return 0; // no blob base / unreadable — nothing to sweep
    }
    // Live ids = the stems of the session .json files currently in the store, so a
    // blob dir is kept iff its session still exists on disk.
    const liveIds = new Set(
      this.listFiles().map(f => path.basename(f).replace(/\.json$/, '')),
    );
    let removed = 0;
    for (const name of collectOrphanedCacheDirs(dirNames, liveIds)) {
      const dir = path.join(blobsBase, name);
      try {
        assertPathWithin(dir, this.sessionDir);
        if (!fs.statSync(dir).isDirectory()) continue; // leave a stray file alone
        fs.rmSync(dir, { recursive: true, force: true });
        removed++;
      } catch { /* best effort — one undeletable dir must not abort the sweep */ }
    }
    // rmdir the base when it ends up empty (parity: cleanupOldImageCaches final rmdir).
    try {
      if (fs.readdirSync(blobsBase).length === 0) fs.rmdirSync(blobsBase);
    } catch { /* non-empty / raced / gone — leave it */ }
    return removed;
  }

  /**
   * Toggle the session's single searchable {@link Session.tag} (parity: the reference's
   * `/tag` toggle/replace/remove state machine). Normalizes the input with
   * {@link normalizeSessionTag}, then:
   *  - no loadable session → `{ ok:false, error:'No active session to tag.' }`;
   *  - empty after normalize → `{ ok:false, error:'Tag name cannot be empty.' }`;
   *  - the SAME tag is already set → remove it (toggle off), `action:'removed'`;
   *  - a DIFFERENT tag is set → replace it, `action:'replaced'`;
   *  - none set → add it, `action:'added'`.
   * One tag per session; the change is persisted via {@link save}.
   */
  toggleTag(id: string, rawTag: string): TagToggleResult {
    const session = this.load(id);
    if (!session) return { ok: false, error: 'No active session to tag.' };
    const normalized = normalizeSessionTag(rawTag);
    if (!normalized) return { ok: false, error: 'Tag name cannot be empty.' };
    const previous = typeof session.tag === 'string' && session.tag ? session.tag : undefined;
    if (previous === normalized) {
      delete session.tag;
      this.save(session);
      return { ok: true, action: 'removed', previous };
    }
    session.tag = normalized;
    this.save(session);
    return { ok: true, action: previous ? 'replaced' : 'added', tag: normalized, previous };
  }

  /**
   * Branch a conversation into a NEW session (parity: the reference's forkSession).
   *
   * Every message uuid is remapped to a fresh id while the `parentUuid` threading
   * chain is preserved under the remap, so the fork is an independent transcript
   * tree that shares no ids with its source (editing one never corrupts the
   * other). `upToMessageId` cuts the branch at a specific message (matched by its
   * source uuid, inclusive) — the classic "rewind and try a different path"
   * operation. A `parentUuid` that pointed outside the kept prefix is dropped so
   * the first retained message becomes a root. The forked session is persisted and
   * returned.
   *
   * Subagent sidechain messages are excluded so the fork carries only the main
   * conversation (parity: the reference's createFork filters `isSidechain` entries). The
   * fork is given a unique `"<base> (Branch)"` title — deduped with a numbered
   * suffix on collision (`(Branch 2)`, `(Branch 3)`, …) — from `opts.title`, else
   * the source title, else its first user prompt (parity: getUniqueForkName).
   *
   * Boundary guards (parity: the reference's createFork `No conversation to branch` /
   * `No messages to branch`): forking a brand-new / empty / message-less source is
   * a clean error, never a zero-message fork. A source with no transcript at all
   * (missing/non-array/empty `messages` — the analog of the reference's unreadable or
   * zero-length transcript file) throws `'No conversation to branch'`; a source
   * that HAS entries but none that survive the main-conversation filter (every
   * entry is a subagent sidechain) throws `'No messages to branch'`.
   */
  fork(source: Session, opts: { upToMessageId?: string; title?: string } = {}): Session {
    // No transcript at all — the analog of the reference's unreadable / zero-length
    // transcript file (createFork L79-87). Caught before any work so a brand-new
    // session can never be branched into a 0-message fork.
    if (!Array.isArray(source.messages) || source.messages.length === 0) {
      throw new Error('No conversation to branch');
    }
    // Branch only the main conversation — subagent sidechains are their own trees.
    const msgsAll = source.messages.filter(m => !m.isSidechain);
    // The source had entries but none are main-conversation messages (every turn
    // was a subagent sidechain) — parity: createFork L113-115 `No messages to
    // branch` when `mainConversationEntries.length === 0`.
    if (msgsAll.length === 0) {
      throw new Error('No messages to branch');
    }
    let msgs = msgsAll;
    if (opts.upToMessageId !== undefined) {
      const cut = msgs.findIndex(m => m.uuid === opts.upToMessageId);
      if (cut === -1) {
        throw new Error(`fork: upToMessageId '${opts.upToMessageId}' not found in source session`);
      }
      msgs = msgs.slice(0, cut + 1);
    }
    // Pre-assign a fresh uuid for every message that carries one, so parentUuid
    // references within the kept prefix can be rewritten to the new ids.
    const idMap = new Map<string, string>();
    for (const m of msgs) {
      if (m.uuid) idMap.set(m.uuid, crypto.randomUUID());
    }
    const remapped: Message[] = msgs.map(m => {
      const next = { ...m } as Message;
      if (m.uuid) next.uuid = idMap.get(m.uuid);
      if (m.parentUuid !== undefined) {
        // Rewrite to the remapped parent, or drop it if the parent was truncated.
        next.parentUuid = idMap.get(m.parentUuid);
      }
      return next;
    });
    const now = new Date().toISOString();
    const baseName = (opts.title && opts.title.trim()) || this.deriveForkBaseName(source);
    const forked: Session = {
      ...source,
      id: SessionManager.newId(),
      // A distinct, collision-free branch title so /resume and the picker can tell
      // a fork apart from its source (and from other forks of the same base).
      customTitle: this.getUniqueBranchName(baseName),
      messages: remapped,
      createdAt: now,
      updatedAt: now,
    };
    // Carry the source's content-replacement records across the fork, rewritten
    // to the fork's id (the `...source` spread copied them verbatim under the OLD
    // sessionId, which the resume reader would never match). Without this the fork
    // reconstructs with an empty replacements set → previously-stubbed tool_results
    // are re-sent in full (prompt-cache miss + permanent overage). Parity: the reference's
    // createFork content-replacement preservation.
    const forkedReplacements = forkContentReplacements(source.contentReplacements, source.id, forked.id);
    if (forkedReplacements.length > 0) forked.contentReplacements = forkedReplacements;
    else delete forked.contentReplacements;
    this.save(forked);
    return forked;
  }

  /** Base name for a fork title: the source's display title, else its first user
   *  prompt via the shared {@link deriveFirstPrompt} single-line deriver
   *  (whitespace-collapsed, 100-char capped, generic fallback). */
  private deriveForkBaseName(source: Session): string {
    const title = resolveSessionTitle(source);
    if (title && title.trim()) return title.trim();
    return deriveFirstPrompt(source.messages);
  }

  /**
   * Find persisted sessions by their user-set {@link Session.customTitle}: exact
   * match when `exact` is set, otherwise a prefix match. Only inline JSON is read
   * (no blob rehydration) so a collision scan over a large store stays cheap.
   * Backs {@link getUniqueBranchName} (parity: searchSessionsByCustomTitle).
   */
  searchByTitle(query: string, opts: { exact?: boolean } = {}): Session[] {
    const out: Session[] = [];
    for (const file of this.listFiles()) {
      const s = this.readSessionFile(file, false);
      if (!s || typeof s.customTitle !== 'string') continue;
      if (opts.exact ? s.customTitle === query : s.customTitle.startsWith(query)) {
        out.push(s);
      }
    }
    return out;
  }

  /**
   * Generate a unique fork title: `"<base> (Branch)"`, or `"<base> (Branch N)"`
   * when that collides — the smallest N≥2 not already taken by an existing fork of
   * the same base. Exact + prefix search over persisted customTitles with a
   * regex-escaped base (parity: the reference's getUniqueForkName).
   */
  getUniqueBranchName(baseName: string): string {
    const candidate = `${baseName} (Branch)`;
    if (this.searchByTitle(candidate, { exact: true }).length === 0) return candidate;
    // Collision — scan every existing "<base> (Branch...)" and claim the first gap.
    const used = new Set<number>([1]); // "(Branch)" with no number counts as 1.
    const pattern = new RegExp(`^${escapeRegExp(baseName)} \\(Branch(?: (\\d+))?\\)$`);
    for (const s of this.searchByTitle(`${baseName} (Branch`)) {
      const m = s.customTitle?.match(pattern);
      if (m) used.add(m[1] ? parseInt(m[1], 10) : 1);
    }
    let next = 2;
    while (used.has(next)) next++;
    return `${baseName} (Branch ${next})`;
  }

  /**
   * Generate a timestamp-based session ID.
   */
  static newId(): SessionId {
    const now = new Date();
    const pad = (n: number, len = 2) => String(n).padStart(len, '0');
    const seq = SessionManager._idCounter++;
    const stamp = [
      now.getFullYear(),
      pad(now.getMonth() + 1),
      pad(now.getDate()),
      '-',
      pad(now.getHours()),
      pad(now.getMinutes()),
      pad(now.getSeconds()),
      '-',
      pad(now.getMilliseconds(), 3),
    ].join('');
    return asSessionId(`session-${stamp}-${seq}`);
  }

  /**
   * Mint a fresh session id for a conversation reset (e.g. `/clear`), optionally
   * recording the outgoing session as the new one's parent for lineage tracking.
   * With `setCurrentAsParent` + `currentId`, the returned descriptor carries
   * `parentSessionId` so the caller can stamp it onto the new {@link Session} and
   * the cleared-through chain stays walkable. Parity: the reference's
   * `regenerateSessionId({ setCurrentAsParent })` which sets `STATE.parentSessionId`.
   */
  static regenerateSessionId(
    opts: { setCurrentAsParent?: boolean; currentId?: SessionId } = {},
  ): { id: SessionId; parentSessionId?: SessionId } {
    const id = SessionManager.newId();
    if (opts.setCurrentAsParent && opts.currentId) {
      return { id, parentSessionId: opts.currentId };
    }
    return { id };
  }

  /**
   * Return the session directory path (useful for testing).
   */
  getSessionDir(): string {
    return this.sessionDir;
  }

  /**
   * Mark this store's session resumable after an unclean exit: write/refresh the
   * crash-recovery pointer ({@link writeResumePointer}) so a hard crash leaves a
   * fresh pointer the next startup can detect. Call on session start and
   * periodically during a run (a refresh just bumps the mtime clock).
   */
  markResumable(sessionId: SessionId | string, surface?: string): void {
    writeResumePointer(this.sessionDir, { sessionId: String(sessionId), surface });
  }

  /**
   * Read this store's crash-recovery pointer with the same read-time staleness GC
   * as {@link readResumePointer}; `null` when there is no fresh pointer (clean exit
   * last time, or a stale/corrupt one just GC'd).
   */
  resumePointer(): (ResumePointer & { ageMs: number }) | null {
    return readResumePointer(this.sessionDir);
  }

  /**
   * Clear this store's crash-recovery pointer on CLEAN exit ({@link clearResumePointer}),
   * so the next startup does not offer to resume a session that ended normally.
   */
  clearResumable(): void {
    clearResumePointer(this.sessionDir);
  }

  /** The GLOBAL projects root — `<configDir>/projects` — holding one per-cwd-slug
   *  subdir of project state. Static so cross-project listing can enumerate every
   *  project's session store without a cwd-bound manager (parity: the reference's
   *  getProjectsDir). */
  static projectsRoot(env: NodeJS.ProcessEnv = process.env): string {
    return path.join(resolveConfigDir(env), 'projects');
  }

  /**
   * Enumerate every project's `sessions` directory under the global projects root
   * (parity: the reference's gatherAllCandidates — the set scanned when a listing's `dir`
   * is omitted). Each returned path backs a dir-scoped {@link SessionManager}. A
   * missing/unreadable projects root yields `[]`; a project slug with no `sessions`
   * subdir is skipped.
   */
  static listProjectSessionDirs(env: NodeJS.ProcessEnv = process.env): string[] {
    const root = SessionManager.projectsRoot(env);
    let slugs: string[];
    try {
      slugs = fs.readdirSync(root);
    } catch {
      return [];
    }
    const dirs: string[] = [];
    for (const slug of slugs) {
      const dir = path.join(root, slug, 'sessions');
      try {
        if (fs.statSync(dir).isDirectory()) dirs.push(dir);
      } catch { /* not a dir / stat race — skip */ }
    }
    return dirs;
  }

  /**
   * The session-store dirs for a set of worktree/project cwds (parity: the reference's
   * worktree-aware gathering): reads the global projects root and returns the
   * `sessions` dir of every slug subdir that matches one of `worktreeCwds` via
   * {@link matchWorktreeForProjectDir} (longest-prefix, truncation-aware). The
   * caller supplies the full candidate set (including the current cwd), so a
   * subdirectory whose slug matches no worktree root is included simply by being in
   * the list. Deduped; a slug subdir with no `sessions` subfolder (or an unreadable
   * projects root) is skipped. Scopes {@link listAcrossProjects} to worktrees
   * instead of EVERY project.
   */
  static listWorktreeSessionDirs(
    worktreeCwds: string[],
    env: NodeJS.ProcessEnv = process.env,
  ): string[] {
    const root = SessionManager.projectsRoot(env);
    let names: string[];
    try {
      names = fs.readdirSync(root);
    } catch {
      return [];
    }
    const out: string[] = [];
    const seen = new Set<string>();
    for (const name of names) {
      if (matchWorktreeForProjectDir(name, worktreeCwds) === null) continue;
      const dir = path.join(root, name, 'sessions');
      if (seen.has(dir)) continue;
      try {
        if (fs.statSync(dir).isDirectory()) {
          seen.add(dir);
          out.push(dir);
        }
      } catch { /* no sessions subdir / stat race — skip */ }
    }
    return out;
  }

  /**
   * List sessions across MULTIPLE project session stores (parity: the reference's
   * all-projects / cross-project resume listing — `listSessionsImpl` with `dir`
   * omitted, and the picker's `showAllProjects` scope).
   *
   * With `dirs` omitted every project's sessions dir under the global projects root
   * is scanned ({@link listProjectSessionDirs}); pass an explicit set to scope the
   * aggregation (e.g. the reference's `dir` option). Each store is read via a dir-scoped
   * manager so every entry carries its own {@link SessionListEntry.projectPath}
   * (which directory it belongs to). The per-store filters (`includeSidechain`,
   * `dropEmpty`, `excludeId`, `filterByPr`, `filterByBranch`, `filterByTag`) are
   * forwarded to each {@link list} call; the merged result is sorted most-recent
   * first via the shared {@link compareSessionEntriesByDate} comparator and the
   * `offset`/`limit` window is applied to the MERGED list (so pagination spans
   * projects, not per-store). A broken store is skipped, never aborting the sweep.
   */
  static listAcrossProjects(opts: {
    dirs?: string[];
    /** Scope the aggregation to the session stores of these worktree/project cwds
     *  (via {@link listWorktreeSessionDirs}) instead of EVERY project under the
     *  global root. Ignored when an explicit `dirs` set is passed; when both are
     *  omitted every project is scanned (unchanged behaviour). Parity: the reference's
     *  `--continue` worktree fan-out. */
    worktreePaths?: string[];
    env?: NodeJS.ProcessEnv;
    limit?: number;
    offset?: number;
    includeSidechain?: boolean;
    dropEmpty?: boolean;
    excludeId?: string;
    filterByPr?: PrFilter;
    filterByBranch?: string;
    filterByTag?: string;
  } = {}): SessionListEntry[] {
    const env = opts.env ?? process.env;
    const dirs = opts.dirs
      ?? (opts.worktreePaths
        ? SessionManager.listWorktreeSessionDirs(opts.worktreePaths, env)
        : SessionManager.listProjectSessionDirs(env));
    const perStore = {
      includeSidechain: opts.includeSidechain,
      dropEmpty: opts.dropEmpty,
      excludeId: opts.excludeId,
      filterByPr: opts.filterByPr,
      filterByBranch: opts.filterByBranch,
      filterByTag: opts.filterByTag,
    };
    const merged: SessionListEntry[] = [];
    const seen = new Set<string>();
    for (const dir of dirs) {
      let entries: SessionListEntry[];
      try {
        // Dir-scoped manager: the cwd is irrelevant for listing (gitBranch is never
        // resolved on the read path), so the override alone targets the store.
        entries = new SessionManager('', dir).list(perStore);
      } catch {
        continue; // one broken store must not abort the cross-project sweep
      }
      for (const e of entries) {
        // Dedup by id across stores (a session manually copied between projects).
        const idKey = String(e.id);
        if (seen.has(idKey)) continue;
        seen.add(idKey);
        merged.push(e);
      }
    }
    merged.sort(compareSessionEntriesByDate);
    const offset = opts.offset !== undefined && opts.offset > 0 ? opts.offset : 0;
    return opts.limit === undefined
      ? merged.slice(offset)
      : merged.slice(offset, offset + Math.max(0, opts.limit));
  }

  /**
   * Resolve a session by id when its owning project directory is unknown: scan
   * every project's session store under the global projects root
   * ({@link listProjectSessionDirs}) and return the first VALID copy found (a
   * zero-byte/corrupt copy is skipped via {@link readSessionFile}'s guards, so a
   * crash-truncated copy in one project never shadows a good copy in another).
   * Returns `null` for a malformed id ({@link isValidSessionId}) or when no project
   * holds it. This is the all-projects branch of the reference's `resolveSessionFilePath`
   * (and the sibling-worktree fallback, since every worktree of one repo shares a
   * single canonical-git-root slug store) — reused by a cross-project `--resume <id>`
   * so a session recorded under another directory is still resumable.
   */
  static loadAcrossProjects(id: string, env: NodeJS.ProcessEnv = process.env): Session | null {
    if (!isValidSessionId(id)) return null;
    for (const dir of SessionManager.listProjectSessionDirs(env)) {
      const session = new SessionManager('', dir).load(id);
      if (session) return session;
    }
    return null;
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  private listFiles(): string[] {
    // FUNCTIONAL: degrade to an empty listing if the session dir can't be read
    // rather than throwing. readdirSync raises ENOENT when the dir is absent or
    // was removed between a prior existsSync and here (TOCTOU race under a
    // concurrent delete), and EACCES on a present-but-non-listable dir — in all
    // of those cases list()/loadLatest()/--resume must report "no sessions", not
    // crash the whole resume path. A single try/catch over readdir subsumes the
    // old existsSync pre-check and closes the race window.
    let entries: string[];
    try {
      entries = fs.readdirSync(this.sessionDir);
    } catch {
      return [];
    }
    return entries
      .filter(e => e.endsWith('.json'))
      .map(e => path.join(this.sessionDir, e));
  }

  private readSessionFile(filePath: string, rehydrate = true): Session | null {
    try {
      // OOM boundary: never read a pathologically large session file into memory
      // (a corrupt/hostile file must not crash --resume). Skip it as unreadable.
      // A zero-byte file (a crash-truncated/empty copy) is likewise treated as
      // NOT-found so it never shadows a valid sibling/cross-project copy (parity:
      // the reference's resolveSessionFilePath `if (s.size > 0)` guard).
      const size = fs.statSync(filePath).size;
      if (size === 0 || size > MAX_SESSION_READ_BYTES) return null;
      const raw = fs.readFileSync(filePath, 'utf-8');
      // Structural validation before trust: a shapeless/tampered blob returns null
      // (skipped as unreadable, like a parse failure). `messages` is guaranteed an
      // array of well-formed records by sanitizeLoadedSession.
      const parsed = sanitizeLoadedSession(JSON.parse(raw));
      if (!parsed) return null;
      if (rehydrate) {
        const safeId = path.basename(filePath).replace(/\.json$/, '');
        const blobDir = this.blobDirFor(safeId);
        parsed.messages = parsed.messages.map(m => this.rehydrateMessage(m, blobDir));
      }
      return parsed;
    } catch {
      return null;
    }
  }

  /** Async counterpart to {@link readSessionFile}: same OOM boundary, JSON parse,
   *  and blob rehydration, but all file I/O goes through `fs.promises` so a large
   *  read never blocks the event loop. Blobs are rehydrated concurrently. */
  private async readSessionFileAsync(filePath: string, rehydrate = true): Promise<Session | null> {
    try {
      const st = await fs.promises.stat(filePath);
      // Zero-byte-as-not-found + OOM boundary, identical to the sync reader.
      if (st.size === 0 || st.size > MAX_SESSION_READ_BYTES) return null;
      const raw = await fs.promises.readFile(filePath, 'utf-8');
      // Same structural validation as the sync reader (shapeless/tampered → null).
      const parsed = sanitizeLoadedSession(JSON.parse(raw));
      if (!parsed) return null;
      if (rehydrate) {
        const safeId = path.basename(filePath).replace(/\.json$/, '');
        const blobDir = this.blobDirFor(safeId);
        parsed.messages = await Promise.all(
          parsed.messages.map(m => this.rehydrateMessageAsync(m, blobDir)),
        );
      }
      return parsed;
    } catch {
      return null;
    }
  }

  private extractPreview(session: Session): string {
    return extractMeaningfulPreview(session.messages);
  }

  /**
   * Recover a session's top-level metadata by a BOUNDED head (+ tail) read, without
   * a full `JSON.parse` of the transcript — the cheap path behind {@link list}'s
   * progressive enrichment so a store with thousands of sessions (or a single
   * multi-megabyte one) does not parse every message just to render a picker row.
   *
   * Reads at most {@link LITE_READ_BUF_SIZE} bytes from the head (where {@link save}
   * now serializes all scalar metadata, before the `messages` array) and, for a file
   * larger than the buffer, the same-sized tail (so an `updatedAt` that trails a huge
   * array — e.g. a legacy file written before metadata-first ordering — is still
   * found via {@link extractLastJsonStringField}). Fields are parsed only from the
   * region BEFORE the first `"messages"` key so an `id`/`tag` nested inside a message
   * can never be mistaken for the top-level value. Returns `null` when the file is
   * missing/unreadable or carries no recoverable top-level `id`.
   */
  readSessionMetaLite(filePath: string, sizeHint?: number): SessionMetaLite | null {
    let fd: number | undefined;
    try {
      const size = sizeHint ?? fs.statSync(filePath).size;
      if (size === 0) return null;
      fd = fs.openSync(filePath, 'r');
      const headLen = Math.min(size, LITE_READ_BUF_SIZE);
      const headBuf = Buffer.allocUnsafe(headLen);
      fs.readSync(fd, headBuf, 0, headLen, 0);
      const head = headBuf.toString('utf-8');
      // Only parse the metadata prefix (before the messages array) so a scalar key
      // that also appears inside a message is never read as the top-level value.
      const msgIdx = head.indexOf('"messages"');
      const metaHead = msgIdx === -1 ? head : head.slice(0, msgIdx);

      let tail = '';
      if (size > LITE_READ_BUF_SIZE) {
        const tailBuf = Buffer.allocUnsafe(LITE_READ_BUF_SIZE);
        fs.readSync(fd, tailBuf, 0, LITE_READ_BUF_SIZE, size - LITE_READ_BUF_SIZE);
        tail = tailBuf.toString('utf-8');
      }

      const id = extractJsonStringField(metaHead, 'id');
      if (id === undefined || !isValidSessionId(id)) return null;

      // Prefer the metadata prefix (collision-proof: a scalar key that also appears
      // inside a message can't be read as the top-level value). Fall back to the full
      // head (legacy/hand-written files that serialized `messages` before metadata)
      // and finally the bounded tail (fields that trail a huge array). `id` stays
      // strict to the prefix so a message's own `"id"` is never mistaken for it.
      const field = (name: string): string | undefined =>
        extractJsonStringField(metaHead, name)
        ?? extractJsonStringField(head, name)
        ?? (tail ? extractLastJsonStringField(tail, name) : undefined);

      const kindRaw = field('kind');
      const kind = kindRaw && (SESSION_KINDS as readonly string[]).includes(kindRaw)
        ? (kindRaw as SessionKind)
        : undefined;
      const modeRaw = field('mode');
      const mode = modeRaw === 'coordinator' || modeRaw === 'normal' ? modeRaw : undefined;

      return {
        id,
        createdAt: field('createdAt'),
        updatedAt: field('updatedAt'),
        customTitle: field('customTitle'),
        aiTitle: field('aiTitle'),
        gitBranch: field('gitBranch'),
        tag: field('tag'),
        version: field('version'),
        projectPath: field('projectPath'),
        entrypoint: field('entrypoint'),
        mode,
        kind,
        isSidechain: /"isSidechain"\s*:\s*true/.test(metaHead),
      };
    } catch {
      return null;
    } finally {
      if (fd !== undefined) { try { fs.closeSync(fd); } catch { /* already closed */ } }
    }
  }

  /** Build a {@link SessionListEntry} for a session OUTSIDE the enrichment window (or
   *  one too large to parse for a single row) from a bounded {@link readSessionMetaLite}
   *  read. `fileSize` is carried over from the stat pass. Returns `null` when the lite
   *  read recovers nothing usable, so the row is simply omitted. */
  private liteEntry(filePath: string, fileSize: number | undefined): SessionListEntry | null {
    const meta = this.readSessionMetaLite(filePath, fileSize);
    if (!meta) return null;
    const title = resolveSessionTitle(meta);
    const summary = resolveSessionSummary(meta);
    const preview = summary ?? '(open to load preview)';
    return {
      id: asSessionId(meta.id),
      date: meta.updatedAt || meta.createdAt || '',
      preview,
      lastPrompt: summary,
      summary,
      messageCount: 0,
      fileSize,
      gitBranch: meta.gitBranch,
      title,
      tag: meta.tag,
      mode: meta.mode,
      projectPath: meta.projectPath,
      entrypoint: meta.entrypoint,
      kind: meta.kind,
      isSidechain: meta.isSidechain,
      lite: true,
    };
  }
}

// ---------------------------------------------------------------------------
// Resume-time turn-interruption recovery
// ---------------------------------------------------------------------------

/** Result of scanning a loaded transcript for a turn that was cut off mid-flight
 *  (parity: the reference's conversationRecovery). `interrupted` is true when the model
 *  requested tool calls that were never answered — i.e. the process died between
 *  the assistant's tool_use and the tool_result — so the resume path can inject a
 *  synthetic continuation instead of resuming into an inconsistent state. */
export interface TurnInterruption {
  interrupted: boolean;
  /** tool_call ids that were requested but have no matching tool result. */
  pendingToolCallIds: string[];
}

/**
 * Detect a mid-turn interruption over a loaded message list: any assistant
 * `tool_calls` id that is not satisfied by a later `tool` message (`tool_call_id`)
 * means the turn was cut off before the tool result was recorded. Pure + order-
 * preserving (pending ids are returned in request order), so the --resume path can
 * decide to synthesize a continuation prompt.
 */
export function detectTurnInterruption(messages: Message[]): TurnInterruption {
  const answered = new Set<string>();
  for (const m of messages) {
    if (m.role === 'tool' && typeof m.tool_call_id === 'string') answered.add(m.tool_call_id);
  }
  const pendingToolCallIds: string[] = [];
  for (const m of messages) {
    if (m.role !== 'assistant' || !Array.isArray(m.tool_calls)) continue;
    for (const tc of m.tool_calls) {
      if (tc && typeof tc.id === 'string' && !answered.has(tc.id)) {
        pendingToolCallIds.push(tc.id);
      }
    }
  }
  return { interrupted: pendingToolCallIds.length > 0, pendingToolCallIds };
}

// ---------------------------------------------------------------------------
// Transcript DAG reconstruction (parentUuid threading)
// ---------------------------------------------------------------------------

/**
 * The single-glyph assistant sentinel appended after a trailing user turn on
 * resume, so a transcript that ends on an unanswered user prompt is still a
 * valid request/response conversation if --resume takes no action. Parity:
 * the reference's `NO_RESPONSE_REQUESTED` (createAssistantMessage sentinel in
 * conversationRecovery.deserializeMessages).
 */
export const NO_RESPONSE_REQUESTED = '[No response requested]';

/**
 * Select the newest resumable LEAF of a transcript's parentUuid DAG: the latest
 * (last-in-order) non-sidechain message that is not referenced as any message's
 * `parentUuid` (i.e. has no child). A sidechain leaf is skipped so resume follows
 * the MAIN conversation, not a subagent branch (parity: the reference's newest
 * non-sidechain leaf selection in the --resume chain walk). Returns `undefined`
 * for an empty list or one with only sidechain messages.
 */
export function selectNewestLeaf(messages: Message[]): Message | undefined {
  const parents = new Set<string>();
  for (const m of messages) {
    if (typeof m.parentUuid === 'string') parents.add(m.parentUuid);
  }
  let leaf: Message | undefined;
  for (const m of messages) {
    if (m.isSidechain) continue;
    if (typeof m.uuid === 'string' && parents.has(m.uuid)) continue; // has a child
    leaf = m; // keep the last candidate in order → newest
  }
  return leaf;
}

/**
 * Reconstruct a linear conversation by walking the `parentUuid` chain from a leaf
 * back to its root, then returning it in chronological order (root → leaf). The
 * walk is cycle-safe: a `parentUuid` cycle (corrupt transcript) terminates the
 * walk at the repeat and returns the partial chain instead of looping forever
 * (parity: the reference's `buildConversationChain` cycle guard). When `leafUuid` is
 * omitted the {@link selectNewestLeaf newest non-sidechain leaf} is used, so a
 * loaded transcript resolves to its main-thread conversation. Branch/sidechain
 * aware: only messages on the chosen leaf's ancestor chain are returned, so a
 * forked/sidechain branch never bleeds into the reconstructed main thread.
 * Returns `[]` when no leaf exists.
 */
export function buildConversationChain(messages: Message[], leafUuid?: string): Message[] {
  const byUuid = new Map<string, Message>();
  for (const m of messages) {
    if (typeof m.uuid === 'string') byUuid.set(m.uuid, m);
  }
  const leaf = leafUuid !== undefined ? byUuid.get(leafUuid) : selectNewestLeaf(messages);
  if (!leaf) return [];
  const chain: Message[] = [];
  const seen = new Set<string>();
  let cur: Message | undefined = leaf;
  while (cur) {
    const id = cur.uuid;
    if (typeof id === 'string') {
      if (seen.has(id)) break; // cycle — return the partial chain
      seen.add(id);
    }
    chain.push(cur);
    let next: Message | undefined = typeof cur.parentUuid === 'string' ? byUuid.get(cur.parentUuid) : undefined;
    // Session-break continuation: when `parentUuid` was deliberately nullified (or
    // points outside the loaded set) to mark a session break, fall back to
    // `logicalParentUuid` so lineage spanning the break is still reconstructed into
    // one thread instead of fragmenting into two roots (parity: the reference's
    // TranscriptMessage.logicalParentUuid preservation across session-break
    // nullification). The `seen` cycle guard above bounds this just like the
    // parentUuid walk, so a logical-link loop returns a partial chain, never spins.
    if (!next && typeof cur.logicalParentUuid === 'string') {
      next = byUuid.get(cur.logicalParentUuid);
    }
    cur = next;
  }
  chain.reverse();
  return chain;
}

/**
 * Cross-directory resume: reconstruct a conversation from an ARBITRARY `.jsonl`
 * transcript path, not just a session in this manager's store. Reads the file
 * (honouring the same {@link MAX_SESSION_READ_BYTES} OOM boundary as the JSON
 * loaders), parses each line as a message record, skips any corrupt/truncated line
 * (a crash can tear the final line), then walks the `parentUuid` DAG via
 * {@link buildConversationChain} to return the newest non-sidechain thread in
 * chronological order. A missing / oversized / unreadable path, or one with no
 * valid message rows, yields `[]`. An explicit `leafUuid` resumes a specific
 * branch; otherwise the newest non-sidechain leaf is chosen.
 *
 * This is the core primitive behind `--resume <path.jsonl>` (parity: the reference's
 * `loadMessagesFromJsonlPath` / `loadConversationForResume(sourceJsonlFile)` for
 * resuming a session whose transcript lives OUTSIDE the current project dir). It
 * reuses the existing DAG walk rather than re-implementing leaf selection (§0b.6).
 */
export function loadMessagesFromTranscriptPath(
  jsonlPath: string,
  opts: { leafUuid?: string } = {},
): Message[] {
  let size: number;
  try {
    size = fs.statSync(jsonlPath).size;
  } catch {
    return []; // missing / inaccessible
  }
  if (size > MAX_SESSION_READ_BYTES) return []; // OOM boundary — don't slurp it
  // Forward chunked read instead of a whole-file readFileSync slurp: peak memory
  // tracks the surviving (post-last-compact-boundary) output, not the raw file
  // size. Precompact truncation is only applied for large transcripts
  // ({@link SKIP_PRECOMPACT_THRESHOLD}) — a big file almost always got big because
  // auto-compact ran, so dropping its pre-compaction prefix is both safe and the
  // whole point (parity: the reference's size-gated readTranscriptForLoad).
  const { lines } = readTranscriptForLoad(jsonlPath, {
    fileSize: size,
    applyBoundaryTruncation: size >= SKIP_PRECOMPACT_THRESHOLD,
  });
  const messages: Message[] = [];
  for (const line of lines) {
    if (!line.trim()) continue;
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      continue; // corrupt / truncated line (e.g. a crash tore the tail) — skip it
    }
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) continue;
    if (typeof (parsed as { role?: unknown }).role !== 'string') continue; // only message rows
    messages.push(parsed as Message);
  }
  return buildConversationChain(messages, opts.leafUuid);
}

// ---------------------------------------------------------------------------
// Forward chunked, compact-boundary-truncating resume reader
// ---------------------------------------------------------------------------

/** Result of {@link readTranscriptForLoad} — the surviving transcript segment. */
export interface TranscriptLoadResult {
  /** Surviving transcript lines in file order: when boundary truncation applied,
   *  only the segment at/after the LAST non-preserved `compact_boundary`; with at
   *  most the single most-recent `attribution-snapshot` line appended at the end
   *  (position-independent — a restore reads only the last snapshot). Each entry is
   *  a raw JSONL string with no trailing newline. */
  lines: string[];
  /** File byte offset where the surviving segment begins (`0` when nothing was
   *  truncated). Parity: the reference's `boundaryStartOffset`. */
  boundaryStartOffset: number;
  /** `true` when the truncating boundary carried a `preservedSegment`, so messages
   *  before it were intentionally kept rather than dropped. Parity: the reference's
   *  `hasPreservedSegment`. */
  hasPreservedSegment: boolean;
}

/**
 * Determine whether a transcript line is a genuine `compact_boundary` system entry
 * (the marker string can appear inside ordinary user content, so a real JSON parse
 * — not a substring test — is required), returning whether it preserves a segment,
 * or `null` when the line is not a boundary. Parity: the reference's `parseBoundaryLine`.
 */
function parseCompactBoundaryLine(line: string): { hasPreservedSegment: boolean } | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
  const obj = parsed as { type?: unknown; subtype?: unknown; compactMetadata?: { preservedSegment?: unknown } };
  if (obj.type !== 'system' || obj.subtype !== 'compact_boundary') return null;
  return { hasPreservedSegment: Boolean(obj.compactMetadata?.preservedSegment) };
}

/** Whether a transcript line is an {@link AttributionSnapshot} entry (type guard on
 *  the parsed `type` field). Only the most-recent one is retained by the reader. */
function isAttributionSnapshotLine(line: string): boolean {
  // Cheap prefix reject before the full parse — nearly every line is not a snapshot.
  if (!line.startsWith('{"type":"attribution-snapshot"')) return false;
  try {
    const parsed = JSON.parse(line) as { type?: unknown };
    return parsed?.type === 'attribution-snapshot';
  } catch {
    return false;
  }
}

/**
 * Single forward chunked read of a `.jsonl` transcript for the `--resume` load
 * path. The file is read {@link TRANSCRIPT_READ_CHUNK_SIZE} bytes at a time through
 * a `StringDecoder` (so a multi-byte UTF-8 sequence split across a chunk seam is
 * reassembled correctly) and processed line-by-line:
 *
 *  - `attribution-snapshot` lines are stripped in-stream; only the LAST one is kept
 *    and appended at EOF (a restore reads only the most-recent snapshot, so its
 *    position doesn't matter).
 *  - when `applyBoundaryTruncation`, a real `compact_boundary` line without a
 *    `preservedSegment` DISCARDS everything accumulated before it (the pre-compaction
 *    prefix) and records its offset in {@link TranscriptLoadResult.boundaryStartOffset};
 *    a boundary WITH a preserved segment keeps the earlier output.
 *
 * Because the accumulator is reset at each truncating boundary, peak memory tracks
 * the surviving output rather than the whole file — the point of the chunked read
 * for multi-megabyte sessions. A boundaryless file yields every line (identical to
 * the previous whole-file read, no behaviour change). A missing / oversized /
 * unreadable path yields an empty result. Parity: the reference's `readTranscriptForLoad`.
 */
export function readTranscriptForLoad(
  jsonlPath: string,
  opts: { applyBoundaryTruncation?: boolean; chunkSize?: number; fileSize?: number } = {},
): TranscriptLoadResult {
  const empty: TranscriptLoadResult = { lines: [], boundaryStartOffset: 0, hasPreservedSegment: false };

  let fileSize = opts.fileSize;
  if (fileSize === undefined) {
    try {
      fileSize = fs.statSync(jsonlPath).size;
    } catch {
      return empty; // missing / inaccessible
    }
  }
  if (fileSize > MAX_SESSION_READ_BYTES) return empty; // OOM boundary — don't read it
  if (fileSize === 0) return empty;

  const chunkSize = opts.chunkSize !== undefined && opts.chunkSize > 0
    ? opts.chunkSize
    : TRANSCRIPT_READ_CHUNK_SIZE;
  const applyTruncation = opts.applyBoundaryTruncation ?? false;

  let fd: number;
  try {
    fd = fs.openSync(jsonlPath, 'r');
  } catch {
    return empty;
  }

  const kept: string[] = []; // surviving data/boundary lines (reset on truncation)
  let lastSnapshot: string | undefined;
  let boundaryStartOffset = 0;
  let hasPreservedSegment = false;
  // Running file byte offset of the next line's first byte (for boundaryStartOffset).
  let offset = 0;

  const processLine = (line: string, terminated: boolean): void => {
    const lineOffset = offset;
    offset += Buffer.byteLength(line, 'utf-8') + (terminated ? 1 : 0); // +1 for the '\n'
    if (!line.trim()) return;
    if (applyTruncation) {
      const boundary = parseCompactBoundaryLine(line);
      if (boundary) {
        if (boundary.hasPreservedSegment) {
          hasPreservedSegment = true; // keep the already-accumulated output
        } else {
          kept.length = 0; // drop the entire pre-compaction prefix
          lastSnapshot = undefined;
          boundaryStartOffset = lineOffset;
          hasPreservedSegment = false;
        }
        kept.push(line); // the boundary system line itself stays in the stream
        return;
      }
    }
    if (isAttributionSnapshotLine(line)) {
      lastSnapshot = line; // retain only the most-recent snapshot
      return;
    }
    kept.push(line);
  };

  const decoder = new StringDecoder('utf-8');
  const buf = Buffer.allocUnsafe(chunkSize);
  let carry = ''; // partial trailing line spanning a chunk seam
  try {
    let filePos = 0;
    while (filePos < fileSize) {
      const bytesRead = fs.readSync(fd, buf, 0, Math.min(chunkSize, fileSize - filePos), filePos);
      if (bytesRead === 0) break;
      filePos += bytesRead;
      const text = carry + decoder.write(buf.subarray(0, bytesRead));
      let start = 0;
      let nl = text.indexOf('\n');
      while (nl !== -1) {
        processLine(text.slice(start, nl), true);
        start = nl + 1;
        nl = text.indexOf('\n', start);
      }
      carry = text.slice(start);
    }
    carry += decoder.end();
    if (carry.length > 0) processLine(carry, false); // final line without a trailing '\n'
  } finally {
    fs.closeSync(fd);
  }

  const lines = kept;
  if (lastSnapshot !== undefined) lines.push(lastSnapshot);
  return { lines, boundaryStartOffset, hasPreservedSegment };
}

/** Options for {@link getSessionMessages} (parity: the reference's
 *  `GetSessionMessagesOptions` — `includeSystemMessages`, `limit`, `offset`). */
export interface GetSessionMessagesOptions {
  /** Include `system` messages in the result. Default `false` — only the
   *  user/assistant/tool conversation is returned, matching the SDK default. */
  includeSystemMessages?: boolean;
  /** Max number of messages to return (after offset). */
  limit?: number;
  /** Number of messages to skip from the start (for pagination). */
  offset?: number;
  /** Drop subagent sidechain messages (`isSidechain`), so only the main-thread
   *  conversation is returned. Default `false`. Parity: the reference's `getSessionMessages`
   *  excluding sidechain (sub-agent) sessions from the reconstructed transcript. */
  excludeSidechain?: boolean;
  /** Reconstruct the conversation by walking the `parentUuid` links to the newest
   *  non-sidechain leaf (via {@link buildConversationChain}) before filtering, so a
   *  branched/forked transcript linearizes to its main thread in chronological
   *  order instead of being read as a flat array. Default `false` (the stored order
   *  is used as-is). Parity: the reference's "builds the conversation chain via parentUuid
   *  links" in `getSessionMessages`. */
  buildChain?: boolean;
}

/**
 * Read a session's conversation messages with SDK-style filtering/pagination:
 * drops `system` messages unless `includeSystemMessages`, then applies
 * `offset`/`limit`. Pure over an already-ordered message array (AMI persists the
 * transcript in chronological order), so a caller can page a long session without
 * materializing the whole thing downstream. Parity: the reference's `getSessionMessages`
 * (includeSystemMessages / limit / offset).
 */
export function getSessionMessages(messages: Message[], opts: GetSessionMessagesOptions = {}): Message[] {
  // Optionally linearize the parentUuid DAG to the newest main-thread leaf first,
  // so a branched/forked transcript resolves to its real conversation chain (reuses
  // the shared {@link buildConversationChain} walk — no duplicated leaf selection).
  let base = opts.buildChain ? buildConversationChain(messages) : messages;
  // Optionally drop subagent sidechain turns so only the main conversation remains.
  if (opts.excludeSidechain) base = base.filter(m => !m.isSidechain);
  let out = opts.includeSystemMessages ? base.slice() : base.filter(m => m.role !== 'system');
  const offset = opts.offset !== undefined && opts.offset > 0 ? opts.offset : 0;
  if (offset > 0) out = out.slice(offset);
  if (opts.limit !== undefined) out = out.slice(0, Math.max(0, opts.limit));
  return out;
}

/**
 * Append a synthetic assistant sentinel ({@link NO_RESPONSE_REQUESTED}) after a
 * transcript that ends on an unanswered user prompt, so the restored conversation
 * is a valid request/response sequence if --resume takes no further action. The
 * LAST turn-relevant message is found by skipping trailing `system` messages; a
 * sentinel is added only when it is a genuine user prompt — a `isMeta` control
 * turn or a `[Previous conversation summary]` compaction turn is NOT a real
 * prompt and is left as-is. Returns a NEW array (input untouched); a transcript
 * already ending on an assistant/tool message is returned unchanged. Parity:
 * the reference's deserializeMessages synthetic-sentinel insertion.
 */
export function appendResumeSentinel(messages: Message[]): Message[] {
  const out = messages.slice();
  let idx = -1;
  for (let i = out.length - 1; i >= 0; i--) {
    if (out[i].role === 'system') continue;
    idx = i;
    break;
  }
  if (idx === -1) return out;
  const last = out[idx];
  if (last.role !== 'user') return out;
  const u = last as UserMessage;
  if (u.isMeta) return out;
  if (typeof u.content === 'string' && u.content.startsWith(COMPACTION_SUMMARY_PREFIX)) return out;
  const sentinel: AssistantMessage = { role: 'assistant', content: NO_RESPONSE_REQUESTED };
  out.splice(idx + 1, 0, sentinel);
  return out;
}

// ---------------------------------------------------------------------------
// Content-replacement records (resume prompt-cache continuity)
// ---------------------------------------------------------------------------

/**
 * A single content-replacement decision: an in-context `tool_result` block whose
 * representation was swapped for a smaller `replacement` stub by a per-message
 * budget pass (the full output lives elsewhere — a blob / the un-stubbed history).
 * Replayed on resume so the reconstructed context is byte-identical and the prompt
 * cache stays warm. Parity: the reference's `ContentReplacementRecord`
 * (`utils/toolResultStorage.ts` — `kind: 'tool-result'`, `toolUseId`, `replacement`).
 */
export interface ContentReplacementRecord {
  /** Discriminator — only `tool_result` blocks are budget-replaced today. */
  kind: 'tool-result';
  /** The tool-call id whose result block was replaced (correlates to the stub). */
  toolUseId: string;
  /** The smaller stub text that replaced the full block in-context. */
  replacement: string;
}

/**
 * One enforcement pass's worth of {@link ContentReplacementRecord}s, keyed by the
 * owning `sessionId` and discriminated by `agentId`: when `agentId` is set the
 * record belongs to a subagent sidechain (AgentTool resume reads these); when it is
 * absent the record is main-thread (`/resume` reads these). Parity: the reference's
 * `ContentReplacementEntry` (`types/logs.ts`) — the `agentId` main-thread-vs-sidechain
 * discriminator and the keyed-lookup `sessionId`.
 */
export interface ContentReplacementEntry {
  /** Session the records belong to — the resume reader keys its lookup on this. */
  sessionId: string;
  /** When set, these records belong to a subagent sidechain with this id; when
   *  absent, they are main-thread records. */
  agentId?: string;
  /** The replacement decisions made in this pass. */
  replacements: ContentReplacementRecord[];
}

/** A value is a well-formed {@link ContentReplacementRecord} (guards against a
 *  tampered/foreign persisted entry producing a malformed record on resume). */
function isContentReplacementRecord(v: unknown): v is ContentReplacementRecord {
  if (!v || typeof v !== 'object') return false;
  const r = v as Partial<ContentReplacementRecord>;
  return r.kind === 'tool-result' && typeof r.toolUseId === 'string' && typeof r.replacement === 'string';
}

/**
 * Collect the flat list of {@link ContentReplacementRecord}s to replay for a
 * resume, applying the two selectors the reference's keyed lookup uses:
 *  - `sessionId` (optional) — keep only entries for that session (how the reference's
 *    `loadTranscriptFile` keys the lookup by the session's messages' sessionId);
 *  - `agentId` discriminator — `undefined` selects ONLY main-thread entries (no
 *    `agentId`); a set value selects ONLY the matching subagent sidechain's entries.
 * Malformed records in a tampered/foreign transcript are dropped (never replayed).
 * Parity: the reference's content-replacement reconstruction read (main-thread `/resume`
 * vs AgentTool sidechain resume).
 */
export function collectContentReplacements(
  entries: ContentReplacementEntry[] | undefined,
  opts: { sessionId?: string; agentId?: string } = {},
): ContentReplacementRecord[] {
  if (!Array.isArray(entries)) return [];
  const out: ContentReplacementRecord[] = [];
  for (const e of entries) {
    if (!e || typeof e !== 'object' || !Array.isArray(e.replacements)) continue;
    if (opts.sessionId !== undefined && e.sessionId !== opts.sessionId) continue;
    // agentId discriminator: a main-thread read (agentId undefined) takes only
    // records with NO agentId; a sidechain read takes only the matching agent's.
    if (opts.agentId === undefined) {
      if (e.agentId !== undefined) continue;
    } else if (e.agentId !== opts.agentId) {
      continue;
    }
    for (const r of e.replacements) {
      if (isContentReplacementRecord(r)) out.push({ kind: 'tool-result', toolUseId: r.toolUseId, replacement: r.replacement });
    }
  }
  return out;
}

/**
 * Carry a source session's content-replacement records across a fork: keep only the
 * ORIGINAL session's MAIN-THREAD records (subagent sidechain records belong to their
 * own trees and are not forked — mirrors `createFork` filtering to main-conversation
 * entries), then re-emit them as a SINGLE {@link ContentReplacementEntry} under the
 * FORK's `sessionId` so the resume reader's keyed lookup finds them for the fork.
 * Returns `[]` when there is nothing to carry (so a fork of a session that never had
 * a replacement stays `contentReplacements`-free). Parity: the reference's `createFork`
 * content-replacement preservation (`commands/branch/branch.ts` L98-158).
 */
export function forkContentReplacements(
  entries: ContentReplacementEntry[] | undefined,
  originalSessionId: string,
  forkSessionId: string,
): ContentReplacementEntry[] {
  const records = collectContentReplacements(entries, { sessionId: originalSessionId });
  if (records.length === 0) return [];
  return [{ sessionId: forkSessionId, replacements: records }];
}

// ---------------------------------------------------------------------------
// Per-file agent-attribution snapshots
// ---------------------------------------------------------------------------

/** SHA-256 (hex) of a UTF-8 string — the ONE content-hash primitive shared by the
 *  blob offload ({@link SessionManager.offloadMessage}) and attribution snapshots,
 *  so the two can never diverge in how they hash content (§0b.6 — no duplicated
 *  hashing). */
export function sha256Hex(content: string): string {
  return crypto.createHash('sha256').update(content, 'utf-8').digest('hex');
}

/**
 * Per-file record of how much of a file's CURRENT content the agent authored
 * (parity: the reference's FileAttributionState). `contentHash` pins the content the
 * contribution was measured against (so a later external edit is detectable by a
 * hash mismatch), `agentContribution` is the running count of characters the agent
 * authored across edits, and `mtime` is the file's modification time at snapshot.
 * AMI names the field `agentContribution` (vendor-neutral) but the shape is 1:1
 * with the reference's `claudeContribution`.
 */
export interface FileAttributionState {
  /** SHA-256 (hex) of the file content this contribution was measured against. */
  contentHash: string;
  /** Running count of characters authored by the agent in this file. */
  agentContribution: number;
  /** File modification time (ms epoch) at the moment of this snapshot. */
  mtime: number;
}

/**
 * A transcript entry capturing per-file agent attribution plus session-level
 * prompt / permission-prompt / escape counters, persisted so commit attribution
 * can report how much of a diff the agent authored (parity: the reference's
 * AttributionSnapshotMessage). Each `*AtLastCommit` counter snapshots the matching
 * total AS OF the previous commit, so a "since last commit" delta is derivable.
 */
export interface AttributionSnapshot {
  type: 'attribution-snapshot';
  /** Id of the message this snapshot is anchored to. */
  messageId: string;
  /** Client surface that produced it (e.g. `'cli'`, `'detached'`, `'sdk'`). */
  surface: string;
  /** Per-file attribution keyed by file path. */
  fileStates: Record<string, FileAttributionState>;
  /** Total user prompts in the session so far. */
  promptCount?: number;
  /** {@link promptCount} as of the last commit. */
  promptCountAtLastCommit?: number;
  /** Total permission prompts shown so far. */
  permissionPromptCount?: number;
  /** {@link permissionPromptCount} as of the last commit. */
  permissionPromptCountAtLastCommit?: number;
  /** Total ESC presses (cancelled permission prompts) so far. */
  escapeCount?: number;
  /** {@link escapeCount} as of the last commit. */
  escapeCountAtLastCommit?: number;
}

/**
 * Record that the agent authored `contributedChars` characters of `filePath`, whose
 * new full content is `content` (last modified at `mtime`), returning the UPDATED
 * attribution map (input not mutated — pure). The file's `agentContribution`
 * ACCUMULATES across edits; `contentHash`/`mtime` are refreshed to the latest
 * content so a later external edit (different hash) is detectable. A non-positive
 * `contributedChars` still refreshes the hash/mtime without inflating the count
 * (an edit that only deletes text contributes no authored characters). Parity:
 * the reference's per-edit FileAttributionState update.
 */
export function recordFileContribution(
  states: Record<string, FileAttributionState>,
  filePath: string,
  content: string,
  contributedChars: number,
  mtime: number,
): Record<string, FileAttributionState> {
  const next = { ...states };
  applyFileContribution(next, filePath, content, contributedChars, mtime);
  return next;
}

/**
 * MUTATING in-place variant of {@link recordFileContribution}: accumulate the
 * agent's `contributedChars` for `filePath` directly INTO `target` (the file's
 * {@link FileAttributionState} is replaced, the surrounding object is mutated).
 * This is the single canonical accumulation step — {@link recordFileContribution}
 * wraps it with one copy, and a BULK tracker ({@link
 * import('./tools/attribution-store').recordBulkAgentFileContributions}) copies
 * the map ONCE then calls this per file, avoiding the O(n²) blow-up of copying
 * per file across a diff that touches hundreds of thousands of paths. Parity:
 * the reference's `trackBulkFileChanges` single-Map-copy mutation (commitAttribution
 * L489-L542). A non-positive `contributedChars` still refreshes hash/mtime without
 * inflating the running count.
 */
export function applyFileContribution(
  target: Record<string, FileAttributionState>,
  filePath: string,
  content: string,
  contributedChars: number,
  mtime: number,
): void {
  const prev = target[filePath];
  const added = contributedChars > 0 ? contributedChars : 0;
  target[filePath] = {
    contentHash: sha256Hex(content),
    agentContribution: (prev?.agentContribution ?? 0) + added,
    mtime,
  };
}

/**
 * Fraction (0..1) of a file's current size the agent authored, per its
 * {@link FileAttributionState} — `agentContribution / totalChars`, clamped to
 * [0,1] and `0` for an empty/zero-size file. Lets a commit surface report an
 * "N% agent-authored" figure from a snapshot without re-reading the file.
 */
export function agentContributionRatio(state: FileAttributionState, totalChars: number): number {
  if (totalChars <= 0) return 0;
  return Math.min(1, Math.max(0, state.agentContribution / totalChars));
}

// ---------------------------------------------------------------------------
// Session diff / file-change tracking
// ---------------------------------------------------------------------------

export interface FileChange {
  path: string;
  type: 'create' | 'modify' | 'delete' | 'rename';
  timestamp: number;
  toolCallId?: string;
  originalContent?: string;
  size?: number;
}

export class SessionDiffTracker {
  private _changes: FileChange[] = [];
  private _maxChanges: number;
  private _seenPaths = new Set<string>();

  constructor(maxChanges: number = 10_000) {
    this._maxChanges = maxChanges;
  }

  recordChange(change: FileChange): boolean {
    if (this._changes.length >= this._maxChanges) return false;
    this._changes.push(change);
    this._seenPaths.add(change.path);
    return true;
  }

  getChanges(): readonly FileChange[] {
    return this._changes;
  }

  getChangesForPath(filePath: string): FileChange[] {
    return this._changes.filter(c => c.path === filePath);
  }

  getModifiedPaths(): string[] {
    return [...this._seenPaths];
  }

  getChangeCount(): number {
    return this._changes.length;
  }

  getChangesSince(timestamp: number): FileChange[] {
    return this._changes.filter(c => c.timestamp >= timestamp);
  }

  getChangesByType(type: FileChange['type']): FileChange[] {
    return this._changes.filter(c => c.type === type);
  }

  hasChanges(): boolean {
    return this._changes.length > 0;
  }

  getSummary(): { creates: number; modifies: number; deletes: number; renames: number; uniquePaths: number } {
    let creates = 0, modifies = 0, deletes = 0, renames = 0;
    for (const c of this._changes) {
      if (c.type === 'create') creates++;
      else if (c.type === 'modify') modifies++;
      else if (c.type === 'delete') deletes++;
      else if (c.type === 'rename') renames++;
    }
    return { creates, modifies, deletes, renames, uniquePaths: this._seenPaths.size };
  }

  clear(): void {
    this._changes = [];
    this._seenPaths.clear();
  }
}

// ---------------------------------------------------------------------------
// Session lock file — prevents two Engine instances from sharing one session
// directory, which can corrupt state files and analytics.
// ---------------------------------------------------------------------------

export const SESSION_LOCK_FILE = '.session.lock';

/** A lock older than this whose holder identity cannot be verified (e.g. a PID
 *  from another host on an NFS/SMB-mounted session dir) is treated as stale, so
 *  cross-host locks self-heal instead of wedging --resume forever. */
export const LOCK_FALLBACK_STALE_MS = 2 * 60 * 60 * 1000; // 2h

/** Upper bound on the linear backoff wait between contended lock-acquire retries,
 *  so a large {@link SessionLockOptions.retries} can never grow a single wait into
 *  an unbounded sleep. */
export const LOCK_RETRY_MAX_DELAY_MS = 1000;

/** Default base delay between contended lock-acquire retries (ms). */
export const LOCK_RETRY_DEFAULT_DELAY_MS = 50;

/**
 * Locking is force-DISABLED when `AMI_DISABLE_SESSION_LOCK` is truthy: every
 * acquire reports success with a no-op release and never writes a `.session.lock`,
 * and every probe reports the dir unlocked. Escape hatch for environments where
 * the cross-process lock is counter-productive — a read-only / single-tenant FS,
 * a networked store whose PID probes are meaningless, or a test harness that wants
 * concurrent engines on one dir (parity: the reference's pidLock force-enable/disable env
 * override, `ENABLE_PID_BASED_VERSION_LOCKING`). Default (unset) = locking ON.
 */
export function isSessionLockingDisabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env.AMI_DISABLE_SESSION_LOCK);
}

/**
 * Tunables for {@link acquireSessionLock} (and the `stale` subset for
 * {@link checkSessionLock}). Mirrors the subset of proper-lockfile's `LockOptions`
 * AMI needs: bounded contended-retry, a per-call staleness override, and a
 * lock-compromised callback.
 */
export interface SessionLockOptions {
  /** Extra attempts to wait out a LIVE holder before giving up. `0` (default)
   *  fails immediately on a live holder — the historical behaviour. Each retry
   *  re-probes the holder, so a lock freed mid-wait is acquired on the next pass
   *  (parity: proper-lockfile `LockOptions.retries`). */
  retries?: number;
  /** Base delay between contended retries (ms); the wait grows linearly with the
   *  retry index (attempt N waits `retryDelayMs * (N+1)`), capped at
   *  {@link LOCK_RETRY_MAX_DELAY_MS} (parity: proper-lockfile retry backoff).
   *  Defaults to {@link LOCK_RETRY_DEFAULT_DELAY_MS}. */
  retryDelayMs?: number;
  /** Override the age past which an unverifiable / cross-host holder is treated as
   *  stale and reclaimable. Defaults to {@link LOCK_FALLBACK_STALE_MS} (parity:
   *  proper-lockfile `LockOptions.stale`). */
  stale?: number;
  /** Invoked if, at release time, the on-disk lock has been taken over by another
   *  process (our lock was judged stale and reclaimed) — i.e. the lock we believed
   *  we held was compromised. Never throws into the caller; a throwing callback is
   *  swallowed (parity: proper-lockfile `LockOptions.onCompromised`). */
  onCompromised?: (reason: string) => void;
}

/** On-disk lock payload. Richer than a bare PID so we can (a) detect PID reuse
 *  by comparing the live process's command against our recorded identity and
 *  (b) apply a time-based stale fallback when the PID check is inconclusive. */
export interface SessionLockPayload {
  pid: number;
  /** Date.now() at acquire — drives the time-based stale fallback. */
  startedAt: number;
  /** process.execPath — identity hint for PID-reuse detection. */
  execPath: string;
  /**
   * Boot-anchored process start time (ms) of the holder, or null when the host
   * could not provide it. Present only on locks written by a PID-reuse-aware
   * build; its presence switches {@link lockHolderActive} to the fail-closed
   * identity probe (see {@link ProcessIdentity}).
   */
  processStartTimeMs?: number | null;
  /** Opaque token the holder minted, for reuse-safe identity matching. */
  spawnToken?: string | null;
  /**
   * Monotonic single-writer fence. Starts at 1 for a brand-new lock and is
   * BUMPED (prior + 1) every time a stale lock is reclaimed, so an in-flight
   * write from a superseded owner can be rejected by comparing the fence it
   * holds against the current on-disk fence ({@link assertSessionFence}).
   */
  runtimeFence?: number;
}

/** Injectable system probes so lock behavior (PID liveness, process identity,
 *  clock) can be driven deterministically in tests without spawning processes. */
export interface SessionLockDeps {
  isPidAlive: (pid: number) => boolean;
  getProcessCommand: (pid: number) => string | null;
  /** Boot-anchored start-time read for the PID-reuse-safe identity probe.
   *  Injectable so the fail-closed takeover logic is testable without `/proc`. */
  readProcessStartTimeMs: (pid: number) => number | null;
  now: () => number;
  /** True when running under WSL — drives the cross-OS PID-probe boundary guard in
   *  {@link lockHolderActive}. Injectable so the guard is testable without a real
   *  WSL kernel. Defaults to {@link isWSL}. */
  isWsl: () => boolean;
}

export function isPidAlive(pid: number): boolean {
  // PID 0 targets the caller's own process group (process.kill(0,0) would
  // spuriously succeed) and PID 1 is init/systemd (always alive, never a
  // legitimate lock holder) — a corrupt/sentinel value must never be read as a
  // live holder, or the lock could never be reclaimed.
  if (pid <= 1) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err: any) {
    if (err?.code === 'ESRCH') return false;
    // EPERM means the process exists but we can't signal it
    if (err?.code === 'EPERM') return true;
    return false;
  }
}

/** Best-effort read of the command line for a live PID — used to detect PID
 *  reuse (a dead holder's PID reassigned to an unrelated program). Returns null
 *  when the command cannot be determined (then the caller trusts the PID check). */
export function getProcessCommand(pid: number): string | null {
  if (pid <= 1) return null;
  try {
    const cmd =
      process.platform === 'win32'
        ? `powershell.exe -NoProfile -Command "(Get-CimInstance Win32_Process -Filter \\"ProcessId=${pid}\\").CommandLine"`
        : `ps -o command= -p ${pid}`;
    const out = childExecSync(cmd, { encoding: 'utf-8', timeout: 1000, stdio: ['ignore', 'pipe', 'ignore'] }).trim();
    return out || null;
  } catch {
    return null;
  }
}

const DEFAULT_LOCK_DEPS: SessionLockDeps = {
  isPidAlive,
  getProcessCommand,
  readProcessStartTimeMs: (pid: number) => readProcessStartTimeMsSync(pid),
  now: Date.now,
  isWsl: () => isWSL(),
};

/**
 * Spawn token for THIS process, minted once. Recorded in every lock payload this
 * process writes so a reclaimer that can echo it back (future wiring) can prove
 * reuse; today it rides alongside the start time as secondary identity evidence.
 */
const PROCESS_SPAWN_TOKEN = mintSpawnToken();

function readLockPayload(lockPath: string): SessionLockPayload | null {
  let raw: string;
  try {
    raw = fsHooks.readFileSync(lockPath, 'utf-8') as string;
  } catch {
    return null;
  }
  try {
    const parsed = JSON.parse(raw);
    if (
      parsed && typeof parsed === 'object' && !Array.isArray(parsed) &&
      typeof parsed.pid === 'number'
    ) {
      return {
        pid: parsed.pid,
        startedAt: typeof parsed.startedAt === 'number' ? parsed.startedAt : 0,
        execPath: typeof parsed.execPath === 'string' ? parsed.execPath : '',
        // Optional reuse-safe identity: a number|null start time, a string token,
        // and a monotonic fence. Absent on legacy locks (undefined ⇒ identity
        // probe not engaged, see lockHolderActive).
        processStartTimeMs:
          typeof parsed.processStartTimeMs === 'number'
            ? parsed.processStartTimeMs
            : parsed.processStartTimeMs === null
              ? null
              : undefined,
        spawnToken: typeof parsed.spawnToken === 'string' ? parsed.spawnToken : undefined,
        runtimeFence:
          typeof parsed.runtimeFence === 'number' && Number.isFinite(parsed.runtimeFence)
            ? parsed.runtimeFence
            : undefined,
      };
    }
  } catch {
    /* corrupt/legacy bare-pid content — treated as unparseable below */
  }
  return null;
}

/** Is the lock currently held by a live process that is really one of ours?
 *  A dead PID, a sentinel PID, a reused PID now running an unrelated program, or
 *  an old lock we can't verify (NFS/cross-host) are all treated as NOT held. */
function lockHolderActive(
  payload: SessionLockPayload,
  deps: SessionLockDeps,
  staleMs: number = LOCK_FALLBACK_STALE_MS,
): boolean {
  const { pid } = payload;
  // PID-reuse-safe path: a lock written by a reuse-aware build carries identity
  // (a start time — possibly null — and/or a spawn token). For those locks we
  // DO NOT fall back to "steal on mtime-stale"; instead we run the fail-closed
  // four-state probe. Only a PROVABLY-gone owner (pid absent, or a verified
  // identity mismatch from PID reuse) is reclaimable; a verified-live owner, OR
  // an owner we cannot verify (indeterminate), keeps the lock held — so two
  // writers never land on one transcript. This is the deliberate opposite of the
  // legacy steal-on-(pid-dead OR mtime-stale) heuristic below.
  const hasIdentity = payload.processStartTimeMs !== undefined || payload.spawnToken !== undefined;
  if (hasIdentity) {
    if (pid === process.pid) return true;
    const identity: ProcessIdentity = {
      pid,
      processStartTimeMs: payload.processStartTimeMs ?? null,
      spawnToken: payload.spawnToken ?? null,
    };
    const probe = probeProcessIdentitySync(identity, {
      isPidPresent: (p) => deps.isPidAlive(p),
      readProcessStartTimeMs: (p) => deps.readProcessStartTimeMs(p),
    });
    if (isProvenAlive(probe)) return true;
    // `exited` / `identity-mismatch` ⇒ safe to steal ⇒ not held. `indeterminate`
    // ⇒ fail closed ⇒ held (never reclaimed on an unverifiable owner).
    return !isSafeToSteal(probe);
  }
  if (!deps.isPidAlive(pid)) {
    // WSL cross-OS boundary guard: when the session dir is shared with a
    // Windows-native AMI (symlink / AMI_DATA_DIR on /mnt), the holder's PID is a
    // Windows PID that is NOT probeable from WSL and `isPidAlive` reports it dead.
    // Reclaiming on liveness alone would evict a live cross-OS holder, so under
    // WSL we fall through to the time-based fallback: the lock is treated as held
    // until it ages past {@link LOCK_FALLBACK_STALE_MS} (bounded self-heal), never
    // reclaimed purely because an un-probeable PID "looks dead" (parity: the reference's
    // `getPlatform() !== 'wsl'` guard before a stale-session sweep).
    if (deps.isWsl()) return deps.now() - payload.startedAt <= staleMs;
    return false;                                // dead / sentinel → stale
  }
  if (pid === process.pid) return true;          // it's us → held
  const command = deps.getProcessCommand(pid);
  if (command === null) {
    // Can't verify identity — trust the PID check unless the lock is very old
    // (self-heal cross-host / NFS locks whose PID is meaningless locally).
    const age = deps.now() - payload.startedAt;
    return age <= staleMs;
  }
  // Verify the live process looks like ours; otherwise the PID was reused.
  const c = command.toLowerCase();
  const execHint = payload.execPath ? path.basename(payload.execPath).toLowerCase() : '';
  return c.includes('ami') || (!!execHint && c.includes(execHint));
}

function makeLockPayload(deps: SessionLockDeps, runtimeFence = 1): string {
  const payload: SessionLockPayload = {
    pid: process.pid,
    startedAt: deps.now(),
    execPath: process.execPath,
    // Reuse-safe identity: record OUR start time + spawn token so a later
    // reclaimer runs the fail-closed probe instead of the mtime heuristic.
    processStartTimeMs: deps.readProcessStartTimeMs(process.pid),
    spawnToken: PROCESS_SPAWN_TOKEN,
    // Monotonic single-writer fence: 1 for a fresh lock, prior+1 on reclaim.
    runtimeFence,
  };
  return JSON.stringify(payload);
}

/** Next monotonic fence for a reclaim: the reclaimed lock's fence + 1, defaulting
 *  to a fresh 1 when the prior lock recorded no (or an invalid) fence. */
function nextFenceAfter(priorFence: number | undefined): number {
  return (typeof priorFence === 'number' && Number.isFinite(priorFence) ? priorFence : 0) + 1;
}

// Release handlers for locks held by THIS process, run on graceful exit so a
// lock is freed even if the caller never invokes release() (signal / crash /
// forgotten cleanup). Mirrors the reference's registerCleanup-at-acquire.
const activeLockReleases = new Set<() => void>();

// Zero-syscall ownership map: the lock paths this process currently holds a fresh
// (un-released) handle for. Added when a fresh acquire wins, removed on release —
// so {@link isSessionLockHeldLocally} can answer "do I hold this?" with no disk I/O
// (parity: the reference's `isLockHeldLocally`, used to gate a per-turn release so a
// non-holding turn never touches disk).
const locallyHeldLockPaths = new Set<string>();

/**
 * Zero-syscall check: does THIS process currently believe it holds the session
 * lock for `sessionDir`? True iff a fresh {@link acquireSessionLock} won the lock
 * here and its `release()` has not run yet. No filesystem access — purely the
 * in-process ownership map. Parity: the reference's `isLockHeldLocally()`.
 */
export function isSessionLockHeldLocally(sessionDir: string): boolean {
  return locallyHeldLockPaths.has(path.join(sessionDir, SESSION_LOCK_FILE));
}
let exitHookInstalled = false;
function installExitHook(): void {
  if (exitHookInstalled) return;
  exitHookInstalled = true;
  process.once('exit', releaseAllSessionLocks);
}

/** Release every session lock this process still holds. Registered as the
 *  process-exit cleanup and callable directly (introspection / shutdown). */
export function releaseAllSessionLocks(): void {
  for (const release of [...activeLockReleases]) {
    try { release(); } catch { /* best effort */ }
  }
}

function makeAcquiredHandle(
  lockPath: string,
  onCompromised?: (reason: string) => void,
  runtimeFence?: number,
): { acquired: true; fresh: true; runtimeFence?: number; release: () => void } {
  let released = false;
  const release = (): void => {
    if (released) return;
    released = true;
    activeLockReleases.delete(release);
    locallyHeldLockPaths.delete(lockPath);
    // Ownership-verified release: only unlink the lock if it STILL belongs to us.
    // If another live process judged our lock stale (dead PID / cross-host fallback)
    // and reclaimed it, the file now carries their pid — deleting it would wipe a
    // lock we no longer own (boundary violation). A missing/corrupt lock is a no-op.
    const payload = readLockPayload(lockPath);
    if (payload && payload.pid !== process.pid) {
      // Our held lock was taken over by another process while we ran — surface the
      // compromise so the caller can react (abort the write, warn, re-acquire)
      // instead of silently losing exclusivity (parity: proper-lockfile
      // onCompromised). A throwing callback must never break release().
      if (onCompromised) {
        try { onCompromised(`session lock at ${lockPath} taken over by pid ${payload.pid}`); }
        catch { /* a recovery callback must never break release */ }
      }
      return;
    }
    try { fsHooks.unlinkSync(lockPath); } catch { /* already gone */ }
  };
  activeLockReleases.add(release);
  locallyHeldLockPaths.add(lockPath);
  installExitHook();
  return { acquired: true, fresh: true, runtimeFence, release };
}

/**
 * Build the user-facing lock-conflict message naming the holder PID.
 *
 * When a session is already locked by a live OTHER process, callers must be able
 * to tell the user *which* process holds it so the conflict is actionable
 * (resume the session there, or wait for / terminate that PID) instead of an
 * opaque "could not start" failure. This is the single canonical wording so every
 * surface (acquire result, a `/doctor`-style diagnostic, the resume flow) reads
 * identically. Mirrors the reference's pidLock conflict log — `Cannot acquire lock
 * for <name> - held by PID <pid>` (utils/nativeInstaller/pidLock.ts L248-254) —
 * adapted to a session (the lock "name" is the session directory's basename).
 */
export function formatSessionLockConflict(sessionDir: string, pid: number): string {
  const name = path.basename(sessionDir.replace(/[/\\]+$/, '')) || sessionDir;
  return `Session "${name}" is already in use by another process (PID ${pid}). ` +
    `Resume it in that process, or wait for PID ${pid} to exit before retrying.`;
}

/** Attach the {@link formatSessionLockConflict} message to a blocked acquire
 *  result so the holder PID is surfaced to the user by every caller, not just
 *  silently returned. A successful (or holder-less) result is returned unchanged. */
function withLockConflictMessage(
  result: { acquired: boolean; fresh?: boolean; pid?: number; runtimeFence?: number; release: () => void },
  sessionDir: string,
): { acquired: boolean; fresh?: boolean; pid?: number; runtimeFence?: number; message?: string; release: () => void } {
  if (!result.acquired && result.pid !== undefined) {
    return { ...result, message: formatSessionLockConflict(sessionDir, result.pid) };
  }
  return result;
}

export function acquireSessionLock(
  sessionDir: string,
  depsOverride?: Partial<SessionLockDeps>,
  options: SessionLockOptions = {},
): { acquired: boolean; fresh?: boolean; pid?: number; runtimeFence?: number; message?: string; release: () => void } {
  // Force-disabled: report an un-fresh, no-op "acquired" so every caller behaves as
  // if it owns the dir, without ever touching the filesystem (no lock written, no
  // exit hook, no reclaim). `fresh:false` so a fresh-gated side effect does not fire.
  if (isSessionLockingDisabled()) {
    return { acquired: true, fresh: false, pid: process.pid, release: () => {} };
  }
  const deps: SessionLockDeps = { ...DEFAULT_LOCK_DEPS, ...depsOverride };
  const staleMs = options.stale ?? LOCK_FALLBACK_STALE_MS;
  const retries = options.retries !== undefined && options.retries > 0 ? options.retries : 0;
  const baseDelay = options.retryDelayMs !== undefined && options.retryDelayMs >= 0
    ? options.retryDelayMs
    : LOCK_RETRY_DEFAULT_DELAY_MS;
  const lockPath = path.join(sessionDir, SESSION_LOCK_FILE);
  // Create the dir owner-only when absent; do NOT chmod an existing dir here so
  // a deliberately read-only session dir still surfaces the write EACCES (the
  // save() path tightens pre-existing dir perms via secureMkdirSync instead).
  if (!fs.existsSync(sessionDir)) {
    fs.mkdirSync(sessionDir, { recursive: true, mode: OWNER_ONLY_DIR_MODE });
  }

  // A single acquire attempt (with the inner bounded stale-reclaim retry). Returns
  // the final result; `blockedByLiveHolder` tells the outer contended-retry loop
  // whether waiting could help (a live holder may release) vs. giving up now.
  const tryAcquireOnce = (): {
    result: { acquired: boolean; fresh?: boolean; pid?: number; release: () => void };
    blockedByLiveHolder: boolean;
  } => {
    // Bounded: at most one stale-reclaim retry (replaces unbounded self-recursion
    // that a racing writer recreating the lock could blow the stack with).
    // `reclaimFence` starts at 1 (a brand-new lock) and is bumped to prior+1 when
    // we reclaim a stale lock, so the fence advances monotonically on every
    // takeover and a superseded owner's write can be fenced out.
    let reclaimFence = 1;
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        fsHooks.writeFileSync(lockPath, makeLockPayload(deps, reclaimFence), { flag: 'wx', mode: OWNER_ONLY_FILE_MODE });
      } catch (err: any) {
        if (err?.code !== 'EEXIST') throw err;
        const payload = readLockPayload(lockPath);
        // Re-entrant: this process already holds the lock for this dir. Report an
        // acquired, NON-fresh result with a no-op release (the original fresh handle
        // still owns the on-disk lock and is the one that unlinks it) — so a caller
        // can fire an "entered" side effect only on `fresh` and never double-unlink
        // (parity: the reference's AcquireResult `{kind:'acquired',fresh:false}` reentrant).
        if (payload && payload.pid === process.pid) {
          return { result: { acquired: true, fresh: false, pid: payload.pid, release: () => {} }, blockedByLiveHolder: false };
        }
        if (payload && lockHolderActive(payload, deps, staleMs)) {
          return { result: { acquired: false, pid: payload.pid, release: () => {} }, blockedByLiveHolder: true };
        }
        // Stale / corrupt / unreadable → reclaim and retry the exclusive create,
        // advancing the monotonic fence past the reclaimed owner's.
        reclaimFence = nextFenceAfter(payload?.runtimeFence);
        try { fsHooks.unlinkSync(lockPath); } catch { /* raced away */ }
        continue;
      }
      // Verify we actually won the create (a racer may have recreated the lock
      // between our unlink and this wx-write, which would have thrown EEXIST; if
      // instead both reclaimed and one overwrote, re-read confirms ownership).
      const verify = readLockPayload(lockPath);
      if (verify?.pid !== process.pid) {
        return { result: { acquired: false, pid: verify?.pid, release: () => {} }, blockedByLiveHolder: true };
      }
      return { result: makeAcquiredHandle(lockPath, options.onCompromised, verify.runtimeFence), blockedByLiveHolder: false };
    }
    // Lost the recovery race after the bounded retry.
    const payload = readLockPayload(lockPath);
    return { result: { acquired: false, pid: payload?.pid, release: () => {} }, blockedByLiveHolder: true };
  };

  // Outer contended-retry loop: when a LIVE holder blocks us and the caller asked
  // for retries, wait (bounded linear backoff) and re-probe — the holder may have
  // released in the meantime (parity: proper-lockfile LockOptions.retries).
  for (let round = 0; ; round++) {
    const { result, blockedByLiveHolder } = tryAcquireOnce();
    if (result.acquired || !blockedByLiveHolder || round >= retries) {
      return withLockConflictMessage(result, sessionDir);
    }
    sleepSync(Math.min(baseDelay * (round + 1), LOCK_RETRY_MAX_DELAY_MS));
  }
}

export function isSessionLocked(
  sessionDir: string,
  depsOverride?: Partial<SessionLockDeps>,
): boolean {
  if (isSessionLockingDisabled()) return false;
  const deps: SessionLockDeps = { ...DEFAULT_LOCK_DEPS, ...depsOverride };
  const lockPath = path.join(sessionDir, SESSION_LOCK_FILE);
  if (!fs.existsSync(lockPath)) return false;
  const payload = readLockPayload(lockPath);
  if (!payload) return false;
  return lockHolderActive(payload, deps);
}

/** Tri-state result of {@link checkSessionLock} — a check-without-acquire probe:
 *  `free` (no live holder — reclaimable), `held_by_self` (this process owns it), or
 *  `blocked` (another live process owns it). Parity: the reference's computer-use lock
 *  `CheckResult` (`free` / `held_by_self` / `blocked`). */
export type SessionLockCheck =
  | { kind: 'free' }
  | { kind: 'held_by_self'; pid: number }
  | { kind: 'blocked'; pid: number; message: string };

/**
 * Check the session lock for `sessionDir` WITHOUT acquiring it, yet still perform
 * stale-PID recovery: a lock whose holder is dead / a reused PID / a cross-host
 * lock aged past the fallback window (and a corrupt/unparseable lock) is UNLINKED
 * during the check so a crashed session's lock can't block a pure "may I?" query —
 * but no new lock is created (the caller defers the real acquire). Returns:
 *  - `{kind:'free'}` — no lock, or a stale one that was just recovered;
 *  - `{kind:'held_by_self',pid}` — this process already holds it (zero-syscall
 *    local ownership first, else the on-disk pid matches);
 *  - `{kind:'blocked',pid}` — a live, verified other holder owns it.
 * Parity: the reference's `checkComputerUseLock` (the `defersLockAcquire` contract).
 */
export function checkSessionLock(
  sessionDir: string,
  depsOverride?: Partial<SessionLockDeps>,
  options: Pick<SessionLockOptions, 'stale'> = {},
): SessionLockCheck {
  // Force-disabled: every dir reads as free, so a deferred-acquire caller never
  // treats a live holder as blocking (matches acquireSessionLock's always-succeed).
  if (isSessionLockingDisabled()) return { kind: 'free' };
  const deps: SessionLockDeps = { ...DEFAULT_LOCK_DEPS, ...depsOverride };
  const staleMs = options.stale ?? LOCK_FALLBACK_STALE_MS;
  const lockPath = path.join(sessionDir, SESSION_LOCK_FILE);
  if (!fs.existsSync(lockPath)) return { kind: 'free' };
  const payload = readLockPayload(lockPath);
  if (!payload) {
    // Corrupt/unparseable — can't identify a holder; recover so it can't wedge.
    try { fsHooks.unlinkSync(lockPath); } catch { /* raced away */ }
    return { kind: 'free' };
  }
  if (isSessionLockHeldLocally(sessionDir) || payload.pid === process.pid) {
    return { kind: 'held_by_self', pid: payload.pid };
  }
  if (lockHolderActive(payload, deps, staleMs)) {
    return { kind: 'blocked', pid: payload.pid, message: formatSessionLockConflict(sessionDir, payload.pid) };
  }
  // Stale (dead / reused PID / cross-host-expired) — recover, but do NOT acquire.
  try { fsHooks.unlinkSync(lockPath); } catch { /* raced away */ }
  return { kind: 'free' };
}

/**
 * Thrown when a write is attempted under a session fence that a newer owner has
 * already superseded — the single-writer invariant was violated and the stale
 * owner's in-flight mutation MUST be rejected rather than corrupt the transcript.
 */
export class SessionFenceError extends Error {
  constructor(
    readonly expectedFence: number,
    readonly currentFence: number,
    sessionDir: string,
  ) {
    super(
      `Session ownership lost: fence ${expectedFence} was superseded by ${currentFence} ` +
        `(another process took over "${path.basename(sessionDir)}"). Write rejected.`,
    );
    this.name = 'SessionFenceError';
  }
}

/**
 * Read the current monotonic single-writer fence recorded in `sessionDir`'s lock,
 * or `null` when there is no lock / no fence on disk (legacy or locking disabled).
 */
export function readSessionFence(sessionDir: string): number | null {
  if (isSessionLockingDisabled()) return null;
  const lockPath = path.join(sessionDir, SESSION_LOCK_FILE);
  if (!fs.existsSync(lockPath)) return null;
  const payload = readLockPayload(lockPath);
  return payload?.runtimeFence ?? null;
}

/**
 * True when `fence` is still the current owner fence for `sessionDir`. A record
 * that carries no fence (legacy / locking disabled) reads as current — there is
 * no newer owner to supersede the caller. A STRICTLY newer on-disk fence means a
 * takeover happened and the caller is stale.
 */
export function isSessionFenceCurrent(sessionDir: string, fence: number): boolean {
  const current = readSessionFence(sessionDir);
  return current === null || current === fence;
}

/**
 * Assert the caller still owns `sessionDir` at `expectedFence`. Throws a
 * {@link SessionFenceError} when a strictly newer fence is on disk (another
 * process reclaimed the lock and advanced the fence) so an in-flight write from a
 * superseded owner is rejected on takeover. A missing/legacy fence is a no-op
 * (nothing proves a takeover). This is the assertion callers (transcript append,
 * checkpoint writes) run before a mutation to enforce single-writer ownership.
 */
export function assertSessionFence(sessionDir: string, expectedFence: number): void {
  const current = readSessionFence(sessionDir);
  if (current !== null && current !== expectedFence) {
    throw new SessionFenceError(expectedFence, current, sessionDir);
  }
}

/** Fail-closed CAS acquisition decision (parity: Orca's evaluateAgentSessionAcquisition). */
export type SessionAcquisitionDecision =
  | { decision: 'granted'; nextFence: number }
  | { decision: 'refused'; reason: 'fenceStale' | 'ownerAlive' | 'ownerUnproven' };

/**
 * Pure fail-closed adjudication of whether a caller may take over a lease.
 *
 * Every arm fails closed: a fence mismatch refuses (`fenceStale`); a verified-live
 * owner refuses (`ownerAlive`); and crucially an UNVERIFIABLE owner refuses
 * (`ownerUnproven`) rather than granting — expiry/indeterminacy never creates a
 * second writer. A grant is issued ONLY when the prior owner is provably gone
 * (`exited` / `identity-mismatch`), and it advances the monotonic fence. A null
 * payload (no recorded owner) grants at fence 1.
 *
 * Mirrors Orca `evaluateAgentSessionAcquisition`: "a wrong answer here creates two
 * writers on one provider session," so it refuses on any doubt.
 */
export function evaluateSessionAcquisition(args: {
  payload: SessionLockPayload | null;
  expectedFence: number;
  probe: ProcessIdentityProbe;
}): SessionAcquisitionDecision {
  const { payload, expectedFence, probe } = args;
  if (!payload) return { decision: 'granted', nextFence: 1 };
  const currentFence = payload.runtimeFence ?? 1;
  if (expectedFence !== currentFence) {
    return { decision: 'refused', reason: 'fenceStale' };
  }
  if (isProvenAlive(probe)) {
    return { decision: 'refused', reason: 'ownerAlive' };
  }
  if (!isSafeToSteal(probe)) {
    // indeterminate — fail closed, never grant a second writer on an unproven owner.
    return { decision: 'refused', reason: 'ownerUnproven' };
  }
  return { decision: 'granted', nextFence: nextFenceAfter(payload.runtimeFence) };
}

/** Introspection snapshot of one session lock, for diagnostics / a `/doctor`-style
 *  surface. `active` reflects {@link lockHolderActive} (a dead/sentinel/reused/
 *  cross-host-stale holder reads as inactive, i.e. reclaimable). Parity: the reference's
 *  pidLock `LockInfo` + `getAllLockInfo` diagnostics. */
export interface LockInfo {
  /** Absolute path of the lock file. */
  path: string;
  pid: number;
  startedAt: number;
  execPath: string;
  /** Milliseconds since the lock was acquired (by the deps clock). */
  ageMs: number;
  /** True when a live, verified holder currently owns the lock. */
  active: boolean;
  /** True when the lock is held by THIS process. */
  self: boolean;
}

/**
 * Diagnostics for the session lock in `sessionDir`: returns a {@link LockInfo}
 * snapshot, or `null` when no lock file exists or it is corrupt/unparseable.
 * Read-only — never acquires, reclaims, or deletes. Parity: the reference's per-lock
 * diagnostics read.
 */
export function getSessionLockInfo(
  sessionDir: string,
  depsOverride?: Partial<SessionLockDeps>,
): LockInfo | null {
  const deps: SessionLockDeps = { ...DEFAULT_LOCK_DEPS, ...depsOverride };
  const lockPath = path.join(sessionDir, SESSION_LOCK_FILE);
  if (!fs.existsSync(lockPath)) return null;
  const payload = readLockPayload(lockPath);
  if (!payload) return null;
  return {
    path: lockPath,
    pid: payload.pid,
    startedAt: payload.startedAt,
    execPath: payload.execPath,
    ageMs: deps.now() - payload.startedAt,
    active: lockHolderActive(payload, deps),
    self: payload.pid === process.pid,
  };
}

/**
 * Bulk lock introspection across several session directories: returns a
 * {@link LockInfo} for every dir that currently holds a (parseable) lock, skipping
 * dirs with no/corrupt lock. Per-dir errors are isolated so one unreadable dir
 * never aborts the sweep. Parity: the reference's `getAllLockInfo` bulk diagnostics.
 */
export function getAllLockInfo(
  sessionDirs: string[],
  depsOverride?: Partial<SessionLockDeps>,
): LockInfo[] {
  const out: LockInfo[] = [];
  for (const dir of sessionDirs) {
    try {
      const info = getSessionLockInfo(dir, depsOverride);
      if (info) out.push(info);
    } catch { /* best effort — skip an unreadable dir */ }
  }
  return out;
}

/**
 * Bulk reclaim stale session locks across a set of session directories, returning
 * the number swept. For each dir's `.session.lock`: a corrupt/unparseable lock,
 * or one whose holder is no longer active ({@link lockHolderActive} — a dead,
 * sentinel, reused, or cross-host-expired PID), is UNLINKED so a crashed/abandoned
 * session can't wedge `--resume` for that directory forever. A lock currently held
 * by THIS process ({@link isSessionLockHeldLocally}) is never swept, and per-dir
 * errors are isolated so one unreadable dir never aborts the sweep.
 *
 * This is the proactive counterpart to the lazy reclaim in {@link acquireSessionLock}
 * (which only reclaims when a NEW acquire hits the same dir): it garbage-collects
 * orphaned locks in dirs that are never re-acquired. Parity: the reference's
 * `cleanupStaleLocks(locksDir)` directory sweep, adapted to AMI's one-lock-per-
 * session-dir model (callers pass {@link SessionManager.listProjectSessionDirs}).
 */
export function cleanupStaleLocks(
  sessionDirs: string[],
  depsOverride?: Partial<SessionLockDeps>,
): number {
  const deps: SessionLockDeps = { ...DEFAULT_LOCK_DEPS, ...depsOverride };
  let cleaned = 0;
  for (const dir of sessionDirs) {
    try {
      const lockPath = path.join(dir, SESSION_LOCK_FILE);
      if (!fs.existsSync(lockPath)) continue;
      // Never sweep a lock this process still owns.
      if (isSessionLockHeldLocally(dir)) continue;
      const payload = readLockPayload(lockPath);
      if (payload && lockHolderActive(payload, deps)) continue; // live, verified holder
      // Corrupt/unparseable OR dead-holder lock → reclaim.
      try {
        fsHooks.unlinkSync(lockPath);
        cleaned++;
      } catch { /* raced away — already gone */ }
    } catch { /* per-dir error isolated — never abort the sweep */ }
  }
  return cleaned;
}

// ---------------------------------------------------------------------------
// Crash-recovery resume pointer — "the process died unclean last run → offer to
// resume". Written on session start, refreshed during a run (an mtime bump), and
// cleared on clean exit. If the process dies hard (crash / kill -9 / terminal
// closed) the pointer survives so the next startup can detect it and offer resume.
// ---------------------------------------------------------------------------

/** Filename of the per-session-store crash-recovery pointer. */
export const RESUME_POINTER_FILE = '.resume-pointer.json';

/** A resume pointer older than this (measured by the file MTIME — NOT an embedded
 *  timestamp — so a periodic no-content-change re-write serves as a liveness
 *  refresh) is treated as stale and GC'd on read, so a crash-recovery offer never
 *  keeps re-firing after a session is long abandoned (parity: the reference's bridge
 *  pointer 4h rolling TTL). */
export const RESUME_POINTER_TTL_MS = 4 * 60 * 60 * 1000; // 4h

/** On-disk crash-recovery pointer payload. */
export interface ResumePointer {
  /** The session id to offer resuming. */
  sessionId: string;
  /** The invocation surface that wrote it (e.g. `'cli'`, `'detached'`). */
  surface?: string;
}

function resumePointerPath(sessionDir: string): string {
  return path.join(sessionDir, RESUME_POINTER_FILE);
}

/**
 * Write (or REFRESH) the crash-recovery resume pointer for a session store. Called
 * on session start and periodically during a run: re-writing with the same
 * sessionId is a cheap no-content-change write whose only job is to bump the file
 * MTIME — the rolling-staleness clock {@link readResumePointer} checks (parity:
 * the reference's writeBridgePointer mtime-refresh idiom). A malformed sessionId is
 * rejected (never persist garbage). Best-effort: a crash-recovery aid must never
 * itself crash the session, so a write failure is swallowed.
 */
export function writeResumePointer(sessionDir: string, pointer: ResumePointer): void {
  if (!isValidSessionId(pointer.sessionId)) return;
  const payload: ResumePointer = {
    sessionId: pointer.sessionId,
    ...(typeof pointer.surface === 'string' && pointer.surface ? { surface: pointer.surface } : {}),
  };
  try {
    secureMkdirSync(sessionDir);
    atomicWriteFileSync(resumePointerPath(sessionDir), JSON.stringify(payload));
  } catch { /* best effort — never crash on a recovery-aid write */ }
}

/**
 * Clear the crash-recovery resume pointer (clean shutdown, or GC of a stale/corrupt
 * pointer during a read). Best-effort — a missing file is a no-op. Parity: the reference's
 * clearBridgePointer on clean exit.
 */
export function clearResumePointer(sessionDir: string): void {
  try { fs.unlinkSync(resumePointerPath(sessionDir)); } catch { /* already gone */ }
}

/**
 * Read the crash-recovery resume pointer for a session store, returning the pointer
 * plus its `ageMs` (ms since last write, derived from the file MTIME — the rolling
 * staleness anchor) or `null`. A missing / unreadable / schema-invalid / STALE
 * (older than {@link RESUME_POINTER_TTL_MS}) pointer yields `null`; an invalid or
 * stale pointer is additionally DELETED on read so a crashed session's offer
 * self-heals and never re-fires forever (parity: the reference's readBridgePointer
 * read-time GC). `now` is injectable for deterministic TTL tests.
 */
export function readResumePointer(
  sessionDir: string,
  now: () => number = Date.now,
): (ResumePointer & { ageMs: number }) | null {
  const file = resumePointerPath(sessionDir);
  let mtimeMs: number;
  let raw: string;
  try {
    // stat for mtime (the staleness anchor) then read; mtime IS returned data, not
    // a TOCTOU guard — a vanished/unreadable file simply yields null below.
    mtimeMs = fs.statSync(file).mtimeMs;
    raw = fs.readFileSync(file, 'utf-8');
  } catch {
    return null; // missing / unreadable
  }
  let parsed: unknown;
  try { parsed = JSON.parse(raw); } catch { parsed = null; }
  const sessionId = (parsed as { sessionId?: unknown } | null)?.sessionId;
  if (typeof sessionId !== 'string' || !isValidSessionId(sessionId)) {
    clearResumePointer(sessionDir); // corrupt / foreign — GC so it can't wedge
    return null;
  }
  const ageMs = Math.max(0, now() - mtimeMs);
  if (ageMs > RESUME_POINTER_TTL_MS) {
    clearResumePointer(sessionDir); // stale — GC and decline the offer
    return null;
  }
  const surface = (parsed as { surface?: unknown }).surface;
  return {
    sessionId,
    ...(typeof surface === 'string' ? { surface } : {}),
    ageMs,
  };
}

// ---------------------------------------------------------------------------
// Transcript integrity — JSONL transcript files used by headless / Agent SDK
// sessions.  A crash can truncate the last line; resuming must detect and
// drop the corrupted tail rather than losing the entire file.
// ---------------------------------------------------------------------------

export const TRANSCRIPT_FILE_EXTENSION = '.jsonl';

export function validateTranscriptLine(line: string): boolean {
  if (!line || !line.trim()) return false;
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return false;
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) return false;
  const obj = parsed as Record<string, unknown>;
  if (typeof obj.type !== 'string') return false;
  if (obj.timestamp === undefined) return false;
  if (typeof obj.timestamp !== 'number' && typeof obj.timestamp !== 'string') return false;
  return true;
}

export function repairTranscriptFile(
  filePath: string,
): { repairedLines: number; droppedLines: number } {
  if (!fs.existsSync(filePath)) {
    return { repairedLines: 0, droppedLines: 0 };
  }

  // OOM boundary: a transcript larger than the read cap is not slurped into
  // memory for repair (it would risk crashing the process); leave it untouched.
  if (fs.statSync(filePath).size > MAX_SESSION_READ_BYTES) {
    return { repairedLines: 0, droppedLines: 0 };
  }

  const raw = fs.readFileSync(filePath, 'utf-8');
  if (!raw.trim()) {
    return { repairedLines: 0, droppedLines: 0 };
  }

  const lines = raw.split('\n').filter(l => l.trim() !== '');
  const valid: string[] = [];
  let droppedLines = 0;

  for (let i = 0; i < lines.length; i++) {
    if (validateTranscriptLine(lines[i])) {
      valid.push(lines[i]);
    } else if (i === lines.length - 1) {
      // Last line is truncated/corrupt — drop it silently
      droppedLines++;
    } else {
      // Mid-file corruption — also drop
      droppedLines++;
    }
  }

  if (droppedLines > 0) {
    // Crash-safe + owner-only rewrite of the repaired file.
    atomicWriteFileSync(filePath, valid.map(l => l + '\n').join(''));
  }

  return { repairedLines: valid.length, droppedLines };
}

/** Synchronous bounded sleep (no busy-spin) for the append-lock retry loop. */
function sleepSync(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

/**
 * Acquire a short-lived cross-process advisory lock (O_EXCL sidecar) with
 * bounded retries, reclaiming a stale lock whose holder PID is dead or whose
 * file is older than the stale window. Returns a release() or null if it could
 * not be taken within the retry budget.
 */
function acquireAppendLock(
  lockPath: string,
  retries = 5,
  minTimeoutMs = 25,
  staleMs = 10_000,
): (() => void) | null {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      fs.writeFileSync(lockPath, String(process.pid), { flag: 'wx', mode: OWNER_ONLY_FILE_MODE });
      return () => { try { fs.unlinkSync(lockPath); } catch { /* already gone */ } };
    } catch (err: any) {
      if (err?.code !== 'EEXIST') return null;
      // Reclaim a stale lock: dead holder PID, or a file older than the window.
      let stale = false;
      try {
        const holder = parseInt(fs.readFileSync(lockPath, 'utf-8').trim(), 10);
        const ageMs = Date.now() - fs.statSync(lockPath).mtimeMs;
        stale = Number.isNaN(holder) || !isPidAlive(holder) || ageMs > staleMs;
      } catch {
        stale = true; // unreadable lock — treat as stale
      }
      if (stale) {
        try { fs.unlinkSync(lockPath); } catch { /* raced away */ }
        continue; // retry the exclusive create immediately
      }
      if (attempt < retries) sleepSync(minTimeoutMs);
    }
  }
  return null;
}

/**
 * Append one JSONL entry to a transcript concurrency-safely: a cross-process
 * advisory lock (so two engine/SDK writers serialize instead of tearing a line)
 * plus an fsync (so a crash can't lose the just-written line), written
 * owner-only. Best-effort — transcript loss is preferable to crashing the
 * session, and if the lock can't be taken we still append (never block a save).
 */
export function safeAppendTranscript(
  filePath: string,
  entry: Record<string, unknown>,
  ownership?: { sessionDir: string; expectedFence: number },
): void {
  // Single-writer fence assert: when the caller holds a fence, reject the append
  // if a newer owner has taken over (fence advanced on the lock). This throws a
  // SessionFenceError OUT of the function — unlike the best-effort transcript I/O
  // below, a superseded write MUST NOT silently land and corrupt the transcript.
  if (ownership) {
    assertSessionFence(ownership.sessionDir, ownership.expectedFence);
  }
  // NDJSON-safe: escape U+2028/U+2029 so an entry can never be split into two lines.
  const line = ndjsonSafeStringify(entry) + '\n';

  // The lock+fsync critical section. Isolated so the retry below can recreate the
  // parent dir and run it a second time without duplicating the body.
  const appendOnce = (): void => {
    const release = acquireAppendLock(filePath + '.lock');
    try {
      // File ops go through the fsHooks DI seam so the recreate-and-retry fallback
      // below is deterministically testable (mock.method(fsHooks,'openSync',…)).
      const fd = fsHooks.openSync(filePath, 'a', OWNER_ONLY_FILE_MODE);
      try {
        fs.writeSync(fd, line);
        fsHooks.fsyncSync(fd);
      } finally {
        fsHooks.closeSync(fd);
      }
    } finally {
      release?.();
    }
  };

  try {
    secureMkdirSync(path.dirname(filePath));
    appendOnce();
  } catch {
    // The append (or the pre-mkdir) failed. Rather than discriminate on an error
    // code — NFS-like filesystems surface unexpected codes for a vanished parent
    // dir — recreate the parent dir and RETRY the append once, so a transient /
    // racing-rmdir failure still lands the entry instead of silently dropping it
    // (parity: the reference's appendToFile mkdir-and-retry). Only if the retry also
    // throws do we give up (transcript loss is preferable to crashing).
    try {
      secureMkdirSync(path.dirname(filePath));
      appendOnce();
    } catch {
      /* best effort — transcript loss is preferable to crashing the session */
    }
  }
}
