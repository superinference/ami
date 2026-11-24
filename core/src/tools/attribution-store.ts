import 'fs';
import 'path';
import { FileAttributionState, AttributionSnapshot, applyFileContribution } from '../session';
import { isGeneratedFile } from '../generated-files';
import { log } from '../logger';
export { computeAgentCharContribution, normalizeAttributionPath } from './attribution-store-compute-agent-char-contribution';

const GLOBAL_KEY = '__global__';

const sessionFileAttribution = new Map<string, Record<string, FileAttributionState>>();

function keyFor(sessionId: string | undefined): string {
  return sessionId && sessionId.length > 0 ? sessionId : GLOBAL_KEY;
}

/**
 * Record that the agent authored `contributedChars` characters of `filePath`,
 * whose new full content is `content` (last modified at `mtime`). Delegates the
 * per-file accumulation + content-hash refresh to session.ts's
 * {@link recordFileContribution} (the one canonical update), keeping the running
 * total across successive edits in the same session.
 */
export function recordAgentFileContribution(
  sessionId: string | undefined,
  filePath: string,
  content: string,
  contributedChars: number,
  mtime: number,
): void {
  // Single edit is just a one-element bulk change — the one canonical write path,
  // so the per-edit and bulk cases can never diverge (no semantic duplication).
  recordBulkAgentFileContributions(sessionId, [
    { path: filePath, content, contributedChars, mtime },
  ]);
}

/** One file's authored-character delta for {@link recordBulkAgentFileContributions}. */
export interface AgentFileChange {
  /** Attribution key (already normalized by the caller, e.g. via {@link normalizeAttributionPath}). */
  path: string;
  /** The file's full new content, whose hash pins the measured contribution. */
  content: string;
  /** Characters the agent authored in THIS change (non-positive = refresh only). */
  contributedChars: number;
  /** File modification time (ms epoch) at the moment of this change. */
  mtime: number;
}

/**
 * Accumulate MANY file contributions for one session making exactly ONE copy of
 * that session's attribution map, then mutating it per change via session.ts's
 * {@link applyFileContribution}. This is the scalable variant of
 * {@link recordAgentFileContribution}: a single git/jj operation (or a MultiEdit)
 * that touches a large set of files is tracked in O(n) instead of the O(n²) cost
 * of copying the whole map per file. Parity: the reference's `trackBulkFileChanges`
 * single-Map-copy mitigation (commitAttribution.ts L489-L542). An empty `changes`
 * list is a no-op (no needless copy). Each change still emits the standard verbose
 * `Tracked N chars for <path>` observability hint.
 */
export function recordBulkAgentFileContributions(
  sessionId: string | undefined,
  changes: ReadonlyArray<AgentFileChange>,
): void {
  if (changes.length === 0) return;
  const key = keyFor(sessionId);
  // ONE copy of the session map, then mutate it in place for every change.
  const next = { ...(sessionFileAttribution.get(key) ?? {}) };
  for (const c of changes) {
    applyFileContribution(next, c.path, c.content, c.contributedChars, c.mtime);
    // Observability affordance (parity: the reference's `Attribution: Tracked N chars
    // for <path>` debug hint). Routed through the standard verbose debug path, so it
    // only surfaces under AMI_DEBUG_LOG_LEVEL=verbose (optionally narrowed with
    // AMI_DEBUG_COMPONENTS=attribution) and costs nothing otherwise.
    const added = c.contributedChars > 0 ? c.contributedChars : 0;
    log('attribution', `Tracked ${added} chars for ${c.path}`, { filePath: c.path, added }, 'verbose');
  }
  sessionFileAttribution.set(key, next);
}

/**
 * The per-file attribution map for a session (keyed by absolute file path), or an
 * empty object when nothing has been recorded. Returns the live reference — the
 * caller must not mutate it (consumers only read).
 */
export function getSessionFileAttribution(
  sessionId: string | undefined,
): Record<string, FileAttributionState> {
  return sessionFileAttribution.get(keyFor(sessionId)) ?? {};
}

/**
 * Session-scoped count of USER PROMPTS (each top-level engine `submit()` is one
 * prompt — a "shot"). This is the honest source for the "N-shotted" figure in a
 * commit's attribution introspection (parity: the reference's transcript
 * `countUserPromptsFromEntries` / "N-shotted by …"). Keyed like the file
 * attribution store so a commit can read the live count via the same `sessionId`
 * the tool executor already threads. Process-lifetime only; the git note is the
 * durable surface.
 */
/**
 * Per-session EFFORT accounting — the counters that let a commit report how much
 * steering/permission/cancellation happened FOR THAT COMMIT, not cumulatively.
 * Each running counter is paired with an `*AtLastCommit` baseline so the delta
 * since the previous commit is a subtraction. Parity: the reference's
 * `AttributionState` effort fields (commitAttribution.ts L182-192). Vendor-neutral
 * shape; mirrors the persisted {@link AttributionSnapshot} schema 1:1.
 */
export interface SessionEffortState {
  /** Total user prompts ("shots") in the session so far. */
  promptCount: number;
  /** {@link promptCount} as of the last commit (per-commit steer baseline). */
  promptCountAtLastCommit: number;
  /** Total interactive permission prompts shown in the session so far. */
  permissionPromptCount: number;
  /** {@link permissionPromptCount} as of the last commit. */
  permissionPromptCountAtLastCommit: number;
  /** Total ESC cancellations of a pending permission prompt in the session so far. */
  escapeCount: number;
  /** {@link escapeCount} as of the last commit. */
  escapeCountAtLastCommit: number;
  /** HEAD SHA captured at session start, for external-commit detection; `null` until set. */
  startingHeadSha: string | null;
}

/** The per-commit effort deltas computed from a {@link SessionEffortState}. */
export interface CommitEffort {
  /** User prompts submitted since the last commit (the reference's "steers"). */
  steers: number;
  /** Interactive permission prompts shown since the last commit. */
  permissionPrompts: number;
  /** ESC cancellations since the last commit. */
  escapes: number;
}

const sessionEffort = new Map<string, SessionEffortState>();

function emptyEffort(): SessionEffortState {
  return {
    promptCount: 0,
    promptCountAtLastCommit: 0,
    permissionPromptCount: 0,
    permissionPromptCountAtLastCommit: 0,
    escapeCount: 0,
    escapeCountAtLastCommit: 0,
    startingHeadSha: null,
  };
}

function effortFor(key: string): SessionEffortState {
  let e = sessionEffort.get(key);
  if (!e) {
    e = emptyEffort();
    sessionEffort.set(key, e);
  }
  return e;
}

/**
 * Record that one user prompt was submitted in this session (increments the
 * session-scoped "shot" counter). Wired from the engine's `submit()` entry so the
 * count reflects genuine user turns, not internal tool loops.
 */
export function recordUserPrompt(sessionId: string | undefined): void {
  effortFor(keyFor(sessionId)).promptCount += 1;
}

/**
 * Record that one interactive permission prompt was shown this session. Wired from
 * the tool executor's interactive permission wait so the per-commit permission
 * count reflects genuine user-facing prompts (parity: the reference's
 * `incrementPermissionPromptCount`).
 */
export function recordPermissionPrompt(sessionId: string | undefined): void {
  effortFor(keyFor(sessionId)).permissionPromptCount += 1;
}

/**
 * Record that the user cancelled (ESC / turn abort) a pending permission prompt
 * this session (parity: the reference's ESC-press / cancel tracking). Wired from the tool
 * executor's `cancelled` permission outcome.
 */
export function recordEscapeCancel(sessionId: string | undefined): void {
  effortFor(keyFor(sessionId)).escapeCount += 1;
}

/**
 * The number of user prompts ("shots") recorded for a session so far, or `0` when
 * none were recorded (bare engines / tests). A thin accessor over the effort state's
 * cumulative `promptCount`; `computeCommitEffort` derives the per-commit delta.
 */
export function getSessionPromptCount(sessionId: string | undefined): number {
  return sessionEffort.get(keyFor(sessionId))?.promptCount ?? 0;
}

/** A read-only copy of a session's full {@link SessionEffortState} (empty defaults
 *  when nothing was recorded). */
export function getSessionEffort(sessionId: string | undefined): SessionEffortState {
  return { ...(sessionEffort.get(keyFor(sessionId)) ?? emptyEffort()) };
}

/**
 * The per-commit effort deltas (steers / permission prompts / escapes SINCE the
 * last commit) for a session, each clamped to `>= 0`. Read by `git_commit` to
 * compose the attribution note's per-commit "N-shotted" figure rather than a
 * cumulative session total. Parity: the reference's per-commit steer/permission/
 * escape accounting via the `*AtLastCommit` baselines.
 */
export function computeCommitEffort(sessionId: string | undefined): CommitEffort {
  const e = sessionEffort.get(keyFor(sessionId)) ?? emptyEffort();
  return {
    steers: Math.max(0, e.promptCount - e.promptCountAtLastCommit),
    permissionPrompts: Math.max(0, e.permissionPromptCount - e.permissionPromptCountAtLastCommit),
    escapes: Math.max(0, e.escapeCount - e.escapeCountAtLastCommit),
  };
}

/**
 * Snapshot the current running counters into their `*AtLastCommit` baselines so the
 * NEXT commit's {@link computeCommitEffort} measures only the effort after this
 * commit. Wired from `git_commit` right after a commit lands. Parity: the reference
 * advancing the effort baselines on commit.
 */
export function advanceEffortBaselines(sessionId: string | undefined): void {
  const e = effortFor(keyFor(sessionId));
  e.promptCountAtLastCommit = e.promptCount;
  e.permissionPromptCountAtLastCommit = e.permissionPromptCount;
  e.escapeCountAtLastCommit = e.escapeCount;
}

/**
 * Record the HEAD SHA at session start (idempotent — only the FIRST non-empty SHA
 * sticks) so later tooling can detect commits made OUTSIDE this session by
 * comparing against it. Parity: the reference's `startingHeadSha`.
 */
export function setStartingHeadSha(sessionId: string | undefined, sha: string): void {
  const e = effortFor(keyFor(sessionId));
  if (e.startingHeadSha === null && sha) {
    e.startingHeadSha = sha;
  }
}

/** The HEAD SHA captured at session start, or `null` if never set. */
export function getStartingHeadSha(sessionId: string | undefined): string | null {
  return sessionEffort.get(keyFor(sessionId))?.startingHeadSha ?? null;
}

/** Overall authorship summary for a commit (parity: the reference's
 *  `AttributionSummary`, with vendor-neutral `agent*` naming). */
export interface AttributionSummary {
  /** Integer percent (0..100) of the committed tracked chars authored by the agent. */
  agentPercent: number;
  /** Total characters authored by the agent across the committed tracked files. */
  agentChars: number;
  /** Total characters attributed to a human (pre-existing content in tracked files). */
  humanChars: number;
  /** Distinct client surfaces that produced the edits. */
  surfaces: string[];
}

/** Per-file authorship record in {@link AttributionData} (parity: the reference's
 *  `FileAttribution`). */
export interface FileAttribution {
  agentChars: number;
  humanChars: number;
  /** Integer percent (0..100) of this file's tracked chars authored by the agent. */
  percent: number;
  /** The client surface that authored this file. */
  surface: string;
}

/**
 * Versioned, structured attribution payload for a commit — the durable schema a
 * git note (or any downstream tool) can persist and re-parse. Parity: the reference
 * Code's `AttributionData` (commitAttribution.ts L214-224), with vendor-neutral
 * naming (`agentChars`/`agentPercent` rather than `claude*`). The numeric
 * `version` discriminant lets the schema evolve without ambiguity.
 */
export interface AttributionData {
  version: 1;
  summary: AttributionSummary;
  /** Per-file authorship, keyed by staged (repo-relative) path. */
  files: Record<string, FileAttribution>;
  /** Agent-authored chars + percent-of-total, grouped by client surface. */
  surfaceBreakdown: Record<string, { agentChars: number; percent: number }>;
  /** Staged paths skipped as machine-generated (never counted toward authorship). */
  excludedGenerated: string[];
  /** Session ids whose tracked contributions fed this payload. */
  sessions: string[];
}

function roundPercent(part: number, whole: number): number {
  if (whole <= 0) return 0;
  return Math.min(100, Math.max(0, Math.round((100 * part) / whole)));
}

/**
 * Assemble the versioned {@link AttributionData} payload for a commit from the
 * per-file contributions the edit/write tools recorded this session. Generated
 * staged files ({@link isGeneratedFile}) are excluded from authorship and surfaced
 * under {@link AttributionData.excludedGenerated}. For each tracked, non-generated
 * file the agent's authored chars come from the recorded contribution and the
 * human share is the MEASURED remainder of the committed size
 * (`max(0, totalChars - agentChars)`) — never a fabricated figure. A
 * `surfaceBreakdown` groups authored chars by surface with each surface's percent
 * of the total tracked content. Pure (no fs / git) so it is deterministic and
 * unit-testable; the caller supplies the already-sized contributions.
 */
export function buildAttributionData(params: {
  surface: string;
  sessions: string[];
  stagedFiles: readonly string[];
  contributions: ReadonlyArray<{ path: string; agentContribution: number; totalChars: number }>;
}): AttributionData {
  const { surface, sessions, stagedFiles, contributions } = params;
  const excludedGenerated = stagedFiles.filter(isGeneratedFile);
  const generated = new Set(excludedGenerated);

  const files: Record<string, FileAttribution> = {};
  const surfaceBreakdown: Record<string, { agentChars: number; percent: number }> = {};
  let totalAgentChars = 0;
  let totalHumanChars = 0;

  for (const c of contributions) {
    if (generated.has(c.path)) continue;
    const agentChars = Math.max(0, c.agentContribution);
    const total = Math.max(0, c.totalChars);
    const humanChars = Math.max(0, total - agentChars);
    files[c.path] = {
      agentChars,
      humanChars,
      percent: roundPercent(agentChars, agentChars + humanChars),
      surface,
    };
    totalAgentChars += agentChars;
    totalHumanChars += humanChars;
  }

  const grandTotal = totalAgentChars + totalHumanChars;
  if (totalAgentChars > 0 || Object.keys(files).length > 0) {
    surfaceBreakdown[surface] = {
      agentChars: totalAgentChars,
      percent: roundPercent(totalAgentChars, grandTotal),
    };
  }

  return {
    version: 1,
    summary: {
      agentPercent: roundPercent(totalAgentChars, grandTotal),
      agentChars: totalAgentChars,
      humanChars: totalHumanChars,
      surfaces: [surface],
    },
    files,
    surfaceBreakdown,
    excludedGenerated,
    sessions,
  };
}

// ---------------------------------------------------------------------------
// Snapshot persistence / restore (survives compaction + --resume)
// ---------------------------------------------------------------------------

/**
 * Serialize a session's live in-memory attribution (per-file contributions +
 * effort counters) into a full-state {@link AttributionSnapshot} anchored to
 * `messageId`. The snapshot is a COMPLETE dump, never a delta — so a restore reads
 * only the most-recent one (see {@link restoreAttributionStore}). Parity: the reference
 * Code's `stateToSnapshotMessage` (commitAttribution.ts L869-894).
 */
export function buildAttributionSnapshot(
  sessionId: string | undefined,
  messageId: string,
  surface: string,
): AttributionSnapshot {
  const e = getSessionEffort(sessionId);
  return {
    type: 'attribution-snapshot',
    messageId,
    surface,
    // Shallow copy so a later mutation of the live store can't bleed into the
    // persisted snapshot (the values are immutable FileAttributionState records).
    fileStates: { ...getSessionFileAttribution(sessionId) },
    promptCount: e.promptCount,
    promptCountAtLastCommit: e.promptCountAtLastCommit,
    permissionPromptCount: e.permissionPromptCount,
    permissionPromptCountAtLastCommit: e.permissionPromptCountAtLastCommit,
    escapeCount: e.escapeCount,
    escapeCountAtLastCommit: e.escapeCountAtLastCommit,
  };
}

/**
 * Rebuild a session's in-memory attribution from persisted snapshots, reading ONLY
 * the LAST one. Snapshots are full-state dumps (see {@link buildAttributionSnapshot}),
 * so the newest already holds the most-recent count for every path and counter —
 * iterating and SUMMING across snapshots would cause quadratic growth on restore
 * (the reference documents 837 snapshots × 280 files summing to ~1.15 quadrillion
 * phantom chars for a 5KB file over a 5-day session). Returns `true` when a snapshot
 * was applied, `false` for an empty list (nothing to restore). Parity: the reference's
 * `restoreAttributionStateFromSnapshots` (commitAttribution.ts L899-929).
 */
export function restoreAttributionStore(
  sessionId: string | undefined,
  snapshots: ReadonlyArray<AttributionSnapshot>,
): boolean {
  const last = snapshots[snapshots.length - 1];
  if (!last) return false;
  const key = keyFor(sessionId);
  // Full-state replacement — never merge/accumulate (that is the quadratic bug).
  sessionFileAttribution.set(key, { ...last.fileStates });
  sessionEffort.set(key, {
    promptCount: last.promptCount ?? 0,
    promptCountAtLastCommit: last.promptCountAtLastCommit ?? 0,
    permissionPromptCount: last.permissionPromptCount ?? 0,
    permissionPromptCountAtLastCommit: last.permissionPromptCountAtLastCommit ?? 0,
    escapeCount: last.escapeCount ?? 0,
    escapeCountAtLastCommit: last.escapeCountAtLastCommit ?? 0,
    startingHeadSha: getStartingHeadSha(sessionId),
  });
  return true;
}
