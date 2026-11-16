import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { execSync as childExecSync } from 'child_process';
import type { Message, ProviderConfig, SessionId, UserMessage, AssistantMessage } from './types';
import { asSessionId } from './types';
import { projectStateDir, ndjsonSafeStringify, canonicalGitRoot } from './reference-journal';
import { assertPathWithin } from './path-safety';
import { atomicWriteFileSync, secureMkdirSync, fsHooks, OWNER_ONLY_FILE_MODE, OWNER_ONLY_DIR_MODE } from './utils/fs-atomic';
import { getGitBranch } from './utils/git';
import { stripDisplayTags } from './utils/display-tags';
import { AMI_VERSION } from './version';
import { isEnvTruthy } from './analytics';
export { assertPathWithin } from './path-safety';

/**
 * Upper bound on a session/transcript file we will read back into memory. A
 * corrupt or maliciously huge file must never OOM the process on --resume; a file
 * over this size is treated as unreadable (skipped) rather than loaded. Mirrors
 * the reference's size boundary on session reads.
 */
export const MAX_SESSION_READ_BYTES = 100 * 1024 * 1024; // 100 MiB

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
 * Parity: Claude's deriveFirstPrompt content handling — without this, an array
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

/**
 * Matches an auto-generated / system first-line that must be skipped when
 * deriving the picker preview: anything that STARTS with a lowercase XML-like
 * tag (IDE context `<ide_opened_file>`/`<ide_selection>`, hook output, task
 * notifications, channel messages) or a synthetic `[Request interrupted by
 * user]` marker. Parity: Claude's SKIP_FIRST_PROMPT_PATTERN
 * (sessionStoragePortable.extractFirstPromptFromHead /
 * sessionStorage.getFirstMeaningfulUserMessageTextContent). */
const SKIP_FIRST_PROMPT_PATTERN =
  /^(?:\s*<[a-z][\w-]*[\s>]|\[Request interrupted by user[^\]]*\])/;

/** Captures the command name from a `<command-name>…</command-name>` wrapper so a
 *  slash-command-only turn becomes a clean label fallback instead of raw XML. */
const COMMAND_NAME_RE = /<command-name>(.*?)<\/command-name>/;

/** Captures bash-mode input so a `! cmd` turn is labelled as the user typed it. */
const BASH_INPUT_RE = /<bash-input>([\s\S]*?)<\/bash-input>/;

/** Compaction summaries are the ONLY record of the pre-compaction conversation.
 *  They are kept inline in full (never offloaded) so a missing blob can never
 *  cost us the summarized history on --resume. */
const COMPACTION_SUMMARY_PREFIX = '[Previous conversation summary]';

/**
 * Derive a clean, meaningful one-line preview from a transcript's messages for
 * the /resume + /sessions picker — the single canonical first-prompt extractor.
 *
 * Walks user turns in order and, per Claude's getFirstMeaningfulUserMessageText-
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
      const cmd = COMMAND_NAME_RE.exec(flat);
      if (cmd) {
        if (!commandFallback) commandFallback = cmd[1].trim();
        continue;
      }

      // Bash input: label as typed — checked BEFORE the generic XML-tag skip.
      const bash = BASH_INPUT_RE.exec(flat);
      if (bash) { prompts.push(`! ${bash[1].trim()}`); continue; }

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
 * meaningful user turn exists. Parity: Claude's lastPrompt (distinct from
 * firstPrompt) so the resume list can show what a session was last asked to do.
 */
export function extractLastPrompt(messages: Message[]): string {
  const { prompts, commandFallback } = collectMeaningfulPrompts(messages);
  if (prompts.length > 0) return prompts[prompts.length - 1];
  if (commandFallback) return commandFallback;
  return '(no messages)';
}

/**
 * Resolve a session's display summary by the precedence chain customTitle >
 * aiTitle > lastPrompt > firstPrompt, returning `undefined` when none exist (a
 * metadata-only session). The `'(no messages)'` sentinel is treated as absent so
 * an empty session never masquerades as having a real summary. Parity: Claude's
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
 * daemon-owned one (parity: Claude's `SessionKind` in concurrentSessions.ts):
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
 * elsewhere in this file ({@link isSessionPersistenceDisabled}) — Claude's
 * `CLAUDE_CODE_SESSION_KIND`, taking the AMI value first. Returns `undefined`
 * when unset or not one of {@link SESSION_KINDS}, so a bad value falls back to
 * the caller's default instead of persisting garbage. Parity: Claude's
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
 * most-recent resumable session (parity: Claude's `isBgSession`, which flips exit
 * semantics for `--bg`/daemon sessions). An absent `kind` is treated as the
 * foreground default (`false`), so legacy sessions written before this field are
 * still resumable.
 */
export function isBgSession(session: { kind?: SessionKind }): boolean {
  return session.kind === 'bg' || session.kind === 'daemon' || session.kind === 'daemon-worker';
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
   *  session in (parity: Claude's per-session `mode`). */
  mode?: 'coordinator' | 'normal';
  /** GitHub PR this session is linked to, for list navigation / tracking
   *  (parity: Claude's prNumber/prUrl/prRepository PR-link fields). `repository`
   *  is an `owner/repo` slug. */
  pr?: { number: number; url: string; repository?: string };
  /** Worktree the session was last inside, persisted so --resume can restore the
   *  correct cwd (parity: Claude's WorktreeStateEntry / PersistedWorktreeSession).
   *  `null` = the session explicitly EXITED its worktree; `undefined`/absent =
   *  it was never inside one. */
  worktreeState?: { originalCwd: string; worktreePath: string; branch?: string } | null;
  /** Invocation surface that created / last wrote the session — e.g. `'cli'`
   *  (interactive TUI), `'detached'` (`--prompt`), or an SDK surface. Lets later
   *  filtering/analytics discriminate how a session was launched (parity: Claude's
   *  CLAUDE_CODE_ENTRYPOINT `entrypoint` discriminator). */
  entrypoint?: string;
  /** How the session is being run ({@link SessionKind}): a foreground
   *  `'interactive'` session vs a `'bg'`/`'daemon'`(-worker) one that keeps
   *  running on detach and is skipped by a no-arg `--continue`. Stamped on save()
   *  from the caller-supplied value, else {@link envSessionKind}, else
   *  `'interactive'`. Parity: Claude's persisted `SessionKind`. */
  kind?: SessionKind;
  /** Subagent/sidechain transcript marker: when true this session is a task
   *  sub-engine's conversation and is excluded from the resume picker by default
   *  (parity: Claude filters `isSidechain` logs out of the resumable list). */
  isSidechain?: boolean;
  /** The session this one was forked/regenerated from — e.g. the pre-`/clear`
   *  session recorded as the fresh session's parent for lineage tracking (parity:
   *  Claude's `regenerateSessionId({ setCurrentAsParent })` + `STATE.parentSessionId`).
   *  Lets analytics/resume walk the chain of related sessions a user cleared through. */
  parentSessionId?: SessionId;
  /** Per-session settings saved on disk so /resume can restore them. */
  settings?: {
    skill?: string;
    thinkingLevel?: string;
    outputStyle?: string;
    goal?: string;
    /** Permission mode the user left the session in (e.g. `'ask'`, `'auto-allow'`,
     *  `'deny-all'`), persisted so --resume restores the permission posture
     *  instead of silently reverting to the flag-derived default (parity: Claude's
     *  SessionIndexEntry.permissionMode). */
    permissionMode?: string;
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
 * precedence logic). Parity: Claude's getLogDisplayTitle (defaultTitle param,
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
   *  last asked to do" (parity: Claude's lastPrompt log entry). */
  lastPrompt?: string;
  /** Display summary via the precedence chain customTitle > aiTitle > lastPrompt >
   *  firstPrompt (parity: Claude's listSessionsImpl summary). `undefined` for a
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
 * Parity: Claude's `validateUuid` boundary guard, adapted to AMI's id contract
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
 * Normalize a user-supplied tag before it is persisted: strip C0/C1 control bytes
 * (incl. DEL) and invisible/bidirectional Unicode (zero-width, BOM, LRM/RLM, bidi
 * embeddings/overrides/isolates) that could carry a hidden-character attack, then
 * collapse whitespace and trim. Returns `''` when nothing meaningful remains.
 * Parity: Claude's `recursivelySanitizeUnicode(tag).trim()` before `saveTag` — a
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

/** Escape a literal string for safe embedding in a RegExp source (used when a
 *  user-supplied title becomes the base of a fork-name collision pattern). */
function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

export class SessionManager {
  private sessionDir: string;
  private cwd: string;
  private _gitBranch?: string;
  private _gitBranchResolved = false;
  private static _idCounter = 0;

  constructor(cwd: string, sessionDirOverride?: string) {
    // Resume blobs live GLOBALLY under ~/.ami/projects/<slug>/sessions (like Claude Code's
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
   * Save a session to disk as a JSON file.
   * Message content longer than 5000 chars is offloaded to a content-addressed
   * blob sidecar (full content preserved) with only a preview kept inline, so
   * --resume restores the complete conversation (H14).
   */
  save(session: Session): void {
    // Honour an opt-out of transcript persistence (privacy / ephemeral runs):
    // write nothing to disk when disabled. Checked first so no dir is created.
    if (isSessionPersistenceDisabled()) return;
    this.ensureDir();
    const safeId = sanitizeSessionId(session.id);
    const blobDir = this.blobDirFor(safeId);
    const storedMessages = session.messages.map(m => this.offloadMessage(m, blobDir));
    const filePath = path.join(this.sessionDir, `${safeId}.json`);
    assertPathWithin(filePath, this.sessionDir);
    // A user rename (customTitle) must never be clobbered by an auto-title save:
    // if this save doesn't carry one, preserve whatever is already on disk.
    const customTitle = session.customTitle ?? this.readSessionFile(filePath, false)?.customTitle;
    const data = {
      ...session,
      id: safeId,
      customTitle,
      messages: storedMessages,
      updatedAt: new Date().toISOString(),
      // Stamp provenance so /resume can show branch/project and migrations can
      // detect old schemas. Caller-supplied values win; otherwise derive from cwd.
      gitBranch: session.gitBranch ?? this.gitBranch(),
      projectPath: session.projectPath ?? this.cwd,
      version: session.version ?? AMI_VERSION,
      // Classify the run: a caller-supplied kind wins; else honor an env override
      // (AMI_SESSION_KIND / CLAUDE_CODE_SESSION_KIND); else default foreground.
      kind: session.kind ?? envSessionKind() ?? 'interactive',
    };
    // Crash-safe + owner-only: a truncated/corrupt newest session can never
    // result from an interrupted write, and transcripts stay 0o600 on shared hosts.
    // NDJSON-safe serialization escapes U+2028/U+2029 so content can't tear a line.
    atomicWriteFileSync(filePath, ndjsonSafeStringify(data, 2));
  }

  /** Directory holding a session's content-addressed blobs. */
  private blobDirFor(safeId: string): string {
    return path.join(this.sessionDir, 'blobs', safeId);
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
        const sha = crypto.createHash('sha256').update(full, 'utf-8').digest('hex');
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
  loadLatest(opts: { skipBackground?: boolean } = {}): Session | null {
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
    // a still-running backgrounded one (parity: Claude's --continue bg skip).
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
   * Parity: Claude's async `loadFullLog` — a picker shows the cheap lite entry
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
  list(opts: { limit?: number; offset?: number; includeSidechain?: boolean; dropEmpty?: boolean } = {}): SessionListEntry[] {
    const files = this.listFiles();

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
    // with Claude's applySortAndLimit `seen` set. (AMI stores one id per filename,
    // so this reconciles a manually-copied/stale duplicate rather than being the
    // common path.)
    const seenIds = new Set<string>();
    const entries: SessionListEntry[] = [];
    for (const { filePath, size: fileSize } of window) {
      // Preview only needs inline data — skip blob rehydration for speed.
      const session = this.readSessionFile(filePath, false);
      if (!session) continue;

      // Subagent sidechain transcripts never pollute the resume picker unless the
      // caller explicitly opts in (parity: Claude filters `isSidechain` logs out).
      if (session.isSidechain && !opts.includeSidechain) continue;

      const idKey = String(session.id);
      if (seenIds.has(idKey)) continue;
      seenIds.add(idKey);

      const preview = this.extractPreview(session);
      const lastPrompt = extractLastPrompt(session.messages);
      const summary = resolveSessionSummary(session, { lastPrompt, firstPrompt: preview });
      // Optionally drop a metadata-only session (no title and no real prompts),
      // matching Claude's listSessionsImpl which returns null for such logs.
      if (opts.dropEmpty && summary === undefined) continue;

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
        entrypoint: session.entrypoint,
        kind: session.kind,
        permissionMode: session.settings?.permissionMode,
        isSidechain: session.isSidechain,
        parentSessionId: session.parentSessionId,
      });
    }

    // Sort by date descending (most recent first). FUNCTIONAL: break ties on the
    // session id so a paginated list() (limit/offset) over sessions sharing a
    // timestamp returns a stable, deterministic window across calls instead of
    // one that reflects the underlying readdir/stat order. Session ids embed a
    // monotonic timestamp+sequence, so the newer id sorts first — matching the
    // recency ordering callers expect.
    entries.sort((a, b) => {
      const bTime = b.date ? new Date(b.date).getTime() : 0;
      const aTime = a.date ? new Date(a.date).getTime() : 0;
      const byTime = (isNaN(bTime) ? 0 : bTime) - (isNaN(aTime) ? 0 : aTime);
      if (byTime !== 0) return byTime;
      return String(b.id).localeCompare(String(a.id));
    });
    return entries;
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
   * cleaned up alongside the JSON. Parity: Claude's cleanupPeriodDays retention.
   */
  cleanupOldSessions(cleanupPeriodDays?: number): number {
    if (
      cleanupPeriodDays === undefined ||
      !Number.isFinite(cleanupPeriodDays) ||
      cleanupPeriodDays <= 0
    ) {
      return 0;
    }
    const cutoff = Date.now() - cleanupPeriodDays * 24 * 60 * 60 * 1000;
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
      try {
        this.delete(safeId);
        removed++;
      } catch { /* best effort — one undeletable file must not abort the sweep */ }
    }
    return removed;
  }

  /**
   * Toggle the session's single searchable {@link Session.tag} (parity: Claude's
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
   * Branch a conversation into a NEW session (parity: Claude's forkSession).
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
   * conversation (parity: Claude's createFork filters `isSidechain` entries). The
   * fork is given a unique `"<base> (Branch)"` title — deduped with a numbered
   * suffix on collision (`(Branch 2)`, `(Branch 3)`, …) — from `opts.title`, else
   * the source title, else its first user prompt (parity: getUniqueForkName).
   */
  fork(source: Session, opts: { upToMessageId?: string; title?: string } = {}): Session {
    // Branch only the main conversation — subagent sidechains are their own trees.
    let msgs = source.messages.filter(m => !m.isSidechain);
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
    this.save(forked);
    return forked;
  }

  /** Base name for a fork title: the source's display title, else its first user
   *  prompt (whitespace-collapsed, 100-char capped), else a generic fallback. */
  private deriveForkBaseName(source: Session): string {
    const title = resolveSessionTitle(source);
    if (title && title.trim()) return title.trim();
    const firstUser = source.messages.find(m => m.role === 'user' && !m.isSidechain);
    const raw = firstUser && (firstUser as { content?: unknown }).content;
    const text = contentToText(raw);
    const flat = text.replace(/\s+/g, ' ').trim().slice(0, 100);
    return flat || 'Branched conversation';
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
   * regex-escaped base (parity: Claude's getUniqueForkName).
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
   * the cleared-through chain stays walkable. Parity: Claude's
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
      if (fs.statSync(filePath).size > MAX_SESSION_READ_BYTES) return null;
      const raw = fs.readFileSync(filePath, 'utf-8');
      const parsed = JSON.parse(raw) as Session;
      if (rehydrate && Array.isArray(parsed.messages)) {
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
      if (st.size > MAX_SESSION_READ_BYTES) return null;
      const raw = await fs.promises.readFile(filePath, 'utf-8');
      const parsed = JSON.parse(raw) as Session;
      if (rehydrate && Array.isArray(parsed.messages)) {
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
}

// ---------------------------------------------------------------------------
// Resume-time turn-interruption recovery
// ---------------------------------------------------------------------------

/** Result of scanning a loaded transcript for a turn that was cut off mid-flight
 *  (parity: Claude's conversationRecovery). `interrupted` is true when the model
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
 * Claude's `NO_RESPONSE_REQUESTED` (createAssistantMessage sentinel in
 * conversationRecovery.deserializeMessages).
 */
export const NO_RESPONSE_REQUESTED = '[No response requested]';

/**
 * Select the newest resumable LEAF of a transcript's parentUuid DAG: the latest
 * (last-in-order) non-sidechain message that is not referenced as any message's
 * `parentUuid` (i.e. has no child). A sidechain leaf is skipped so resume follows
 * the MAIN conversation, not a subagent branch (parity: Claude's newest
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
 * (parity: Claude's `buildConversationChain` cycle guard). When `leafUuid` is
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
    cur = typeof cur.parentUuid === 'string' ? byUuid.get(cur.parentUuid) : undefined;
  }
  chain.reverse();
  return chain;
}

/** Options for {@link getSessionMessages} (parity: Claude's
 *  `GetSessionMessagesOptions` — `includeSystemMessages`, `limit`, `offset`). */
export interface GetSessionMessagesOptions {
  /** Include `system` messages in the result. Default `false` — only the
   *  user/assistant/tool conversation is returned, matching the SDK default. */
  includeSystemMessages?: boolean;
  /** Max number of messages to return (after offset). */
  limit?: number;
  /** Number of messages to skip from the start (for pagination). */
  offset?: number;
}

/**
 * Read a session's conversation messages with SDK-style filtering/pagination:
 * drops `system` messages unless `includeSystemMessages`, then applies
 * `offset`/`limit`. Pure over an already-ordered message array (AMI persists the
 * transcript in chronological order), so a caller can page a long session without
 * materializing the whole thing downstream. Parity: Claude's `getSessionMessages`
 * (includeSystemMessages / limit / offset).
 */
export function getSessionMessages(messages: Message[], opts: GetSessionMessagesOptions = {}): Message[] {
  let out = opts.includeSystemMessages ? messages.slice() : messages.filter(m => m.role !== 'system');
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
 * Claude's deserializeMessages synthetic-sentinel insertion.
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

/** On-disk lock payload. Richer than a bare PID so we can (a) detect PID reuse
 *  by comparing the live process's command against our recorded identity and
 *  (b) apply a time-based stale fallback when the PID check is inconclusive. */
export interface SessionLockPayload {
  pid: number;
  /** Date.now() at acquire — drives the time-based stale fallback. */
  startedAt: number;
  /** process.execPath — identity hint for PID-reuse detection. */
  execPath: string;
}

/** Injectable system probes so lock behavior (PID liveness, process identity,
 *  clock) can be driven deterministically in tests without spawning processes. */
export interface SessionLockDeps {
  isPidAlive: (pid: number) => boolean;
  getProcessCommand: (pid: number) => string | null;
  now: () => number;
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
  now: Date.now,
};

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
): boolean {
  const { pid } = payload;
  if (!deps.isPidAlive(pid)) return false;       // dead / sentinel → stale
  if (pid === process.pid) return true;          // it's us → held
  const command = deps.getProcessCommand(pid);
  if (command === null) {
    // Can't verify identity — trust the PID check unless the lock is very old
    // (self-heal cross-host / NFS locks whose PID is meaningless locally).
    const age = deps.now() - payload.startedAt;
    return age <= LOCK_FALLBACK_STALE_MS;
  }
  // Verify the live process looks like ours; otherwise the PID was reused.
  const c = command.toLowerCase();
  const execHint = payload.execPath ? path.basename(payload.execPath).toLowerCase() : '';
  return c.includes('ami') || (!!execHint && c.includes(execHint));
}

function makeLockPayload(deps: SessionLockDeps): string {
  const payload: SessionLockPayload = {
    pid: process.pid,
    startedAt: deps.now(),
    execPath: process.execPath,
  };
  return JSON.stringify(payload);
}

// Release handlers for locks held by THIS process, run on graceful exit so a
// lock is freed even if the caller never invokes release() (signal / crash /
// forgotten cleanup). Mirrors Claude's registerCleanup-at-acquire.
const activeLockReleases = new Set<() => void>();
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

function makeAcquiredHandle(lockPath: string): { acquired: true; release: () => void } {
  let released = false;
  const release = (): void => {
    if (released) return;
    released = true;
    activeLockReleases.delete(release);
    // Ownership-verified release: only unlink the lock if it STILL belongs to us.
    // If another live process judged our lock stale (dead PID / cross-host fallback)
    // and reclaimed it, the file now carries their pid — deleting it would wipe a
    // lock we no longer own (boundary violation). A missing/corrupt lock is a no-op.
    const payload = readLockPayload(lockPath);
    if (payload && payload.pid !== process.pid) return;
    try { fsHooks.unlinkSync(lockPath); } catch { /* already gone */ }
  };
  activeLockReleases.add(release);
  installExitHook();
  return { acquired: true, release };
}

export function acquireSessionLock(
  sessionDir: string,
  depsOverride?: Partial<SessionLockDeps>,
): { acquired: boolean; pid?: number; release: () => void } {
  const deps: SessionLockDeps = { ...DEFAULT_LOCK_DEPS, ...depsOverride };
  const lockPath = path.join(sessionDir, SESSION_LOCK_FILE);
  // Create the dir owner-only when absent; do NOT chmod an existing dir here so
  // a deliberately read-only session dir still surfaces the write EACCES (the
  // save() path tightens pre-existing dir perms via secureMkdirSync instead).
  if (!fs.existsSync(sessionDir)) {
    fs.mkdirSync(sessionDir, { recursive: true, mode: OWNER_ONLY_DIR_MODE });
  }

  // Bounded: at most one stale-reclaim retry (replaces unbounded self-recursion
  // that a racing writer recreating the lock could blow the stack with).
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      fsHooks.writeFileSync(lockPath, makeLockPayload(deps), { flag: 'wx', mode: OWNER_ONLY_FILE_MODE });
    } catch (err: any) {
      if (err?.code !== 'EEXIST') throw err;
      const payload = readLockPayload(lockPath);
      if (payload && lockHolderActive(payload, deps)) {
        return { acquired: false, pid: payload.pid, release: () => {} };
      }
      // Stale / corrupt / unreadable → reclaim and retry the exclusive create.
      try { fsHooks.unlinkSync(lockPath); } catch { /* raced away */ }
      continue;
    }
    // Verify we actually won the create (a racer may have recreated the lock
    // between our unlink and this wx-write, which would have thrown EEXIST; if
    // instead both reclaimed and one overwrote, re-read confirms ownership).
    const verify = readLockPayload(lockPath);
    if (verify?.pid !== process.pid) {
      return { acquired: false, pid: verify?.pid, release: () => {} };
    }
    return makeAcquiredHandle(lockPath);
  }

  // Lost the recovery race after the bounded retry.
  const payload = readLockPayload(lockPath);
  return { acquired: false, pid: payload?.pid, release: () => {} };
}

export function isSessionLocked(
  sessionDir: string,
  depsOverride?: Partial<SessionLockDeps>,
): boolean {
  const deps: SessionLockDeps = { ...DEFAULT_LOCK_DEPS, ...depsOverride };
  const lockPath = path.join(sessionDir, SESSION_LOCK_FILE);
  if (!fs.existsSync(lockPath)) return false;
  const payload = readLockPayload(lockPath);
  if (!payload) return false;
  return lockHolderActive(payload, deps);
}

/** Introspection snapshot of one session lock, for diagnostics / a `/doctor`-style
 *  surface. `active` reflects {@link lockHolderActive} (a dead/sentinel/reused/
 *  cross-host-stale holder reads as inactive, i.e. reclaimable). Parity: Claude's
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
 * Read-only — never acquires, reclaims, or deletes. Parity: Claude's per-lock
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
 * never aborts the sweep. Parity: Claude's `getAllLockInfo` bulk diagnostics.
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
): void {
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
    // (parity: Claude's appendToFile mkdir-and-retry). Only if the retry also
    // throws do we give up (transcript loss is preferable to crashing).
    try {
      secureMkdirSync(path.dirname(filePath));
      appendOnce();
    } catch {
      /* best effort — transcript loss is preferable to crashing the session */
    }
  }
}
