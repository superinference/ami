// Split from session.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as crypto from 'crypto';
import type { Message, SessionId, UserMessage, AssistantMessage, SessionOperatingMode } from './types';
import { COMPACTION_SUMMARY_PREFIX } from './session-unescape-json-string';
import type { SessionAgentIdentity, SessionKind } from './session-session-list-entry';
import { NO_RESPONSE_REQUESTED, buildConversationChain } from './session-build-conversation-chain';
import type { WorktreeResumeResolution } from './session-build-conversation-chain';

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

/**
 * Determine whether a transcript line is a genuine `compact_boundary` system entry
 * (the marker string can appear inside ordinary user content, so a real JSON parse
 * — not a substring test — is required), returning whether it preserves a segment,
 * or `null` when the line is not a boundary. Parity: the reference's `parseBoundaryLine`.
 */
export function parseCompactBoundaryLine(line: string): { hasPreservedSegment: boolean } | null {
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
export function isAttributionSnapshotLine(line: string): boolean {
  // Cheap prefix reject before the full parse — nearly every line is not a snapshot.
  if (!line.startsWith('{"type":"attribution-snapshot"')) return false;
  try {
    const parsed = JSON.parse(line) as { type?: unknown };
    return parsed?.type === 'attribution-snapshot';
  } catch {
    return false;
  }
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
export function isContentReplacementRecord(v: unknown): v is ContentReplacementRecord {
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
