/**
 * Background memory-consolidation primitives (parity: the reference's
 * `services/autoDream/` — `autoDream.ts`, `consolidationLock.ts`,
 * `consolidationPrompt.ts`).
 *
 * A consolidation ("dream") pass is a reflective sweep over the auto-memory
 * directory that merges recent signal into durable, well-organized memories.
 * the reference gates how often it fires behind two scheduling knobs and records the
 * last run in a single dual-purpose lock file whose filesystem **mtime encodes
 * the last-consolidated timestamp** and whose body holds the holder PID — so one
 * `stat()` per turn answers "when did we last consolidate?" and a crash can
 * never strand the schedule (the mtime survives, a dead PID is reclaimed).
 *
 * These are the self-contained building blocks (core layer): the scheduling
 * config + validation, the mtime-is-timestamp lock, the due-gate, and the
 * consolidation-prompt builder. A background runner that actually drives the
 * pass is common-layer orchestration that composes them.
 */

import * as fs from 'fs';
import * as path from 'path';
import { MAX_ENTRYPOINT_LINES, MEMORY_INDEX_FILENAME } from './memory';
import { log } from './logger';
import { formatDuration } from './conversation-summary';
import { plural } from './tools/tool-utils';

/**
 * Namespaced, human-readable debug sink for consolidation diagnostics (parity:
 * the reference's `[autoDream]` {@link logForDebugging} lines in
 * `consolidationLock.ts`). Each call explains WHY a consolidation was skipped or
 * delayed so an operator can trace scheduling decisions. The default routes to
 * the shared logger under the `autoDream` component, so every line is emitted as
 * `[…] [DEBUG] [autoDream] <message>` into the real debug log / `--debug-to-stderr`
 * stream; tests inject a capturing sink.
 */
export type ConsolidationDebug = (message: string) => void;

/** Component name the default sink namespaces consolidation diagnostics under. */
export const CONSOLIDATION_DEBUG_COMPONENT = 'autoDream';

/** Default debug sink — emits each line through the shared logger, namespaced. */
const defaultDebug: ConsolidationDebug = (message) => log(CONSOLIDATION_DEBUG_COMPONENT, message);

/** Scheduling knobs for background memory consolidation (parity: the reference's
 *  `AutoDreamConfig` — `minHours` / `minSessions`). */
export interface AutoDreamConfig {
  /** Minimum hours since the last consolidation before another may fire. */
  minHours: number;
  /** Minimum number of accumulated sessions before a consolidation may fire. */
  minSessions: number;
}

/** Defaults mirroring the reference's `DEFAULTS` record. */
export const AUTO_DREAM_DEFAULTS: AutoDreamConfig = {
  minHours: 24,
  minSessions: 5,
};

/**
 * Resolve an {@link AutoDreamConfig} from an untrusted raw value (e.g. a remote
 * feature-flag payload or a settings blob), applying the reference's defensive
 * per-field validation: a field is used only when it is a finite positive
 * number, otherwise the default is substituted. A stale / wrong-type value can
 * therefore never corrupt scheduling.
 */
export function resolveAutoDreamConfig(raw: unknown): AutoDreamConfig {
  const r = raw && typeof raw === 'object' ? (raw as Record<string, unknown>) : {};
  const pick = (v: unknown, fallback: number): number =>
    typeof v === 'number' && Number.isFinite(v) && v > 0 ? v : fallback;
  return {
    minHours: pick(r.minHours, AUTO_DREAM_DEFAULTS.minHours),
    minSessions: pick(r.minSessions, AUTO_DREAM_DEFAULTS.minSessions),
  };
}

/** Name of the dual-purpose lock file (mtime = lastConsolidatedAt, body = PID). */
const LOCK_FILE = '.consolidate-lock';

/** A held lock is considered stale past this even if its PID is live (guards
 *  against PID reuse after a hard crash). Mirrors the reference's `HOLDER_STALE_MS`. */
const HOLDER_STALE_MS = 60 * 60 * 1000;

const MS_PER_HOUR = 60 * 60 * 1000;

/** Absolute path to the consolidation lock inside a memory directory. */
export function consolidationLockPath(lockDir: string): string {
  return path.join(lockDir, LOCK_FILE);
}

/**
 * The lock file's mtime IS the last-consolidated timestamp (ms since epoch);
 * `0` when no lock exists yet. Per-turn cost: a single `stat`.
 */
export function readLastConsolidatedAt(lockDir: string): number {
  try {
    return fs.statSync(consolidationLockPath(lockDir)).mtimeMs;
  } catch {
    return 0;
  }
}

/** True when `pid` names a live process (EPERM still means it exists). */
function isProcessRunning(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (e) {
    return (e as NodeJS.ErrnoException).code === 'EPERM';
  }
}

/**
 * Acquire the consolidation lock: write our PID and bump mtime to now. Returns
 * the PRE-acquire mtime (for {@link rollbackConsolidationLock}), or `null` when
 * the lock is held by a live PID and still fresh, or when we lost a write race.
 *
 * A held-but-stale lock (older than {@link HOLDER_STALE_MS}) or one owned by a
 * dead / unparseable PID is reclaimed. Two reclaimers that both write race on
 * the final PID; the loser detects it on re-read and bails.
 */
export function tryAcquireConsolidationLock(
  lockDir: string,
  now: number = Date.now(),
  debug: ConsolidationDebug = defaultDebug,
): number | null {
  const p = consolidationLockPath(lockDir);
  let mtimeMs: number | undefined;
  let holderPid: number | undefined;
  try {
    mtimeMs = fs.statSync(p).mtimeMs;
    const parsed = parseInt(fs.readFileSync(p, 'utf-8').trim(), 10);
    holderPid = Number.isFinite(parsed) ? parsed : undefined;
  } catch {
    // ENOENT — no prior lock.
  }

  if (mtimeMs !== undefined && now - mtimeMs < HOLDER_STALE_MS) {
    if (holderPid !== undefined && isProcessRunning(holderPid)) {
      debug(`lock held by live PID ${holderPid} (mtime ${Math.round((now - mtimeMs) / 1000)}s ago)`);
      return null;
    }
    // Dead PID or unparseable body — reclaim.
  }

  try {
    fs.mkdirSync(lockDir, { recursive: true });
  } catch {
    // Best effort — the write below surfaces a genuinely unwritable dir.
  }
  fs.writeFileSync(p, String(process.pid));

  // Two reclaimers both write → last wins the PID; the loser bails on re-read.
  let verify: string;
  try {
    verify = fs.readFileSync(p, 'utf-8');
  } catch {
    return null;
  }
  if (parseInt(verify.trim(), 10) !== process.pid) return null;

  return mtimeMs ?? 0;
}

/**
 * Rewind the lock to its pre-acquire state after a failed consolidation launch,
 * clearing the PID body so our still-running process doesn't look like a holder.
 * `priorMtime === 0` means "no lock existed" → remove the file.
 */
export function rollbackConsolidationLock(
  lockDir: string,
  priorMtime: number,
  debug: ConsolidationDebug = defaultDebug,
): void {
  const p = consolidationLockPath(lockDir);
  try {
    if (priorMtime === 0) {
      fs.unlinkSync(p);
      return;
    }
    fs.writeFileSync(p, '');
    const t = priorMtime / 1000; // utimes wants seconds
    fs.utimesSync(p, t, t);
  } catch (e) {
    // Best effort — next trigger is merely delayed by up to minHours. Surface
    // WHY so the delay is traceable rather than silent.
    debug(`rollback failed: ${(e as Error).message} — next trigger delayed to minHours`);
  }
}

/**
 * Stamp "consolidated now" (best effort) — e.g. after a manual consolidation.
 * Writes our PID and bumps mtime, creating the memory dir if needed.
 */
export function recordConsolidation(
  lockDir: string,
  debug: ConsolidationDebug = defaultDebug,
): void {
  try {
    fs.mkdirSync(lockDir, { recursive: true });
    fs.writeFileSync(consolidationLockPath(lockDir), String(process.pid));
  } catch (e) {
    // Best effort — but surface the failure so a missed stamp is diagnosable.
    debug(`recordConsolidation write failed: ${(e as Error).message}`);
  }
}

/**
 * Hours elapsed since the last consolidation. `Infinity` when none has ever run
 * (`lastConsolidatedAt === 0`), so the time-gate always opens on a first run.
 * Single source of the elapsed-hours math shared by {@link isConsolidationDue}
 * and {@link createConsolidationScheduler} so the two can never drift.
 */
function hoursSinceConsolidation(lastConsolidatedAt: number, now: number): number {
  return lastConsolidatedAt === 0 ? Infinity : (now - lastConsolidatedAt) / MS_PER_HOUR;
}

/** Result of a {@link isConsolidationDue} scheduling evaluation. */
export interface ConsolidationGate {
  /** True only when BOTH the time-gate and session-gate pass. */
  due: boolean;
  /** Hours since the last consolidation (`Infinity` when never consolidated). */
  hoursSince: number;
  /** Last-consolidated timestamp (ms since epoch), `0` if never. */
  lastConsolidatedAt: number;
  /** The resolved (validated) config that drove the decision. */
  config: AutoDreamConfig;
}

/**
 * Decide whether a background consolidation should fire now. Mirrors the reference's
 * two-gate rule: enough HOURS must have elapsed since the last run AND enough
 * SESSIONS must have accumulated. Config is resolved defensively so a bad remote
 * value can't force (or suppress) a run.
 */
export function isConsolidationDue(opts: {
  lockDir: string;
  sessionCount: number;
  rawConfig?: unknown;
  now?: number;
}): ConsolidationGate {
  const config = resolveAutoDreamConfig(opts.rawConfig);
  const now = opts.now ?? Date.now();
  const lastConsolidatedAt = readLastConsolidatedAt(opts.lockDir);
  const hoursSince = hoursSinceConsolidation(lastConsolidatedAt, now);
  const timeGate = hoursSince >= config.minHours;
  const sessionGate = opts.sessionCount >= config.minSessions;
  return { due: timeGate && sessionGate, hoursSince, lastConsolidatedAt, config };
}

/**
 * How often the (more expensive) per-turn SESSION scan may run while the cheap
 * time-gate keeps passing. Mirrors the reference's `SESSION_SCAN_INTERVAL_MS`
 * (10 min). Rationale: the lock's mtime only advances once a consolidation
 * actually fires, so when the time-gate opens but the session-gate does NOT, the
 * time-gate keeps opening on EVERY subsequent turn — and without a throttle the
 * scheduler would re-scan the transcript directory each turn. This same interval
 * doubles as the retry backoff after a failed launch (the lock mtime is rewound,
 * the time-gate re-opens, but the scan won't re-run until the interval lapses).
 */
export const SESSION_SCAN_INTERVAL_MS = 10 * 60 * 1000;

/** Why a {@link ConsolidationScheduler.evaluate} turn did (or did not) fire. */
export type ConsolidationScheduleReason = 'time-gate' | 'scan-throttled' | 'session-gate' | 'due';

/** Outcome of one {@link ConsolidationScheduler.evaluate} turn. */
export interface ConsolidationScheduleDecision {
  /** True only on the `'due'` reason — both gates passed and the throttle allowed a scan. */
  fire: boolean;
  /** Which gate decided the turn (introspection / diagnostics). */
  reason: ConsolidationScheduleReason;
  /** Hours since the last consolidation (`Infinity` when never consolidated). */
  hoursSince: number;
  /** Last-consolidated timestamp (ms since epoch), `0` if never. */
  lastConsolidatedAt: number;
  /**
   * Sessions counted by the scan, or `null` when the scan was SKIPPED (the
   * time-gate was closed or the scan throttle suppressed it) — the whole point of
   * the throttle is that `scanSessions` is not invoked on a skipped turn.
   */
  sessionCount: number | null;
  /** The resolved (validated) config that drove the decision. */
  config: AutoDreamConfig;
}

/** A stateful scan-throttled consolidation scheduler (see {@link createConsolidationScheduler}). */
export interface ConsolidationScheduler {
  /** Evaluate whether a consolidation should fire now, applying the scan throttle. */
  evaluate(opts: {
    lockDir: string;
    /** Invoked ONLY after the time-gate opens AND the scan throttle allows a scan. */
    scanSessions: () => number;
    rawConfig?: unknown;
    now?: number;
  }): ConsolidationScheduleDecision;
  /** Epoch ms of the last session scan (`0` until one runs) — introspection. */
  lastSessionScanAt(): number;
}

/**
 * Create a stateful consolidation scheduler that guards the expensive session
 * scan behind a throttle/backoff (parity: the reference's `autoDream` runner, whose
 * closure-scoped `lastSessionScanAt` + `SESSION_SCAN_INTERVAL_MS` stop it from
 * re-scanning the transcript directory on every turn once the time-gate is
 * perpetually open).
 *
 * Gate order (cheapest first), matching the reference:
 *   1. **Time gate** — `hoursSince >= minHours`; else `'time-gate'` (no scan).
 *   2. **Scan throttle** — if the last scan was under `SESSION_SCAN_INTERVAL_MS`
 *      ago, bail with `'scan-throttled'` WITHOUT calling `scanSessions` (this is
 *      both the per-turn de-bounce AND the post-failure backoff).
 *   3. **Session gate** — run `scanSessions()` (now stamping `lastSessionScanAt`);
 *      if the count is below `minSessions`, bail with `'session-gate'`.
 *   4. Otherwise `'due'` → `fire: true`. The caller then acquires the lock
 *      ({@link beginConsolidationRun}) and launches the run.
 *
 * The scan timestamp advances ONLY after the time-gate opens and the throttle
 * passes — so a turn blocked at the time-gate never consumes the throttle window.
 * State is closure-scoped (not module-level) so each `createConsolidationScheduler()`
 * is independent and tests get a fresh throttle per instance.
 */
export function createConsolidationScheduler(opts: {
  scanIntervalMs?: number;
  debug?: ConsolidationDebug;
} = {}): ConsolidationScheduler {
  const interval = opts.scanIntervalMs ?? SESSION_SCAN_INTERVAL_MS;
  const debug = opts.debug ?? defaultDebug;
  let lastScanAt = 0;

  return {
    lastSessionScanAt: () => lastScanAt,
    evaluate(e) {
      const config = resolveAutoDreamConfig(e.rawConfig);
      const now = e.now ?? Date.now();
      const lastConsolidatedAt = readLastConsolidatedAt(e.lockDir);
      const hoursSince = hoursSinceConsolidation(lastConsolidatedAt, now);

      // 1. Time gate — cheapest (one stat already done above).
      if (hoursSince < config.minHours) {
        return { fire: false, reason: 'time-gate', hoursSince, lastConsolidatedAt, sessionCount: null, config };
      }

      // 2. Scan throttle — the time-gate is open; don't re-scan every turn.
      const sinceScan = now - lastScanAt;
      if (sinceScan < interval) {
        debug(`scan throttle — time-gate passed but last scan was ${Math.round(sinceScan / 1000)}s ago`);
        return { fire: false, reason: 'scan-throttled', hoursSince, lastConsolidatedAt, sessionCount: null, config };
      }
      lastScanAt = now;

      // 3. Session gate — only now do we pay for the (expensive) scan.
      const sessionCount = Math.max(0, Math.trunc(e.scanSessions()));
      if (sessionCount < config.minSessions) {
        debug(`skip — ${sessionCount} sessions since last consolidation, need ${config.minSessions}`);
        return { fire: false, reason: 'session-gate', hoursSince, lastConsolidatedAt, sessionCount, config };
      }

      // 4. Due.
      debug(`due — ${hoursSince === Infinity ? 'never' : `${hoursSince.toFixed(1)}h`} since last, ${sessionCount} sessions to review`);
      return { fire: true, reason: 'due', hoursSince, lastConsolidatedAt, sessionCount, config };
    },
  };
}

/**
 * Build the consolidation ("dream") prompt pointed at a memory root and a
 * transcript directory. Parity with the reference's `buildConsolidationPrompt`,
 * including the optional `extra` additional-context slot. Reuses the single-source
 * entrypoint filename + line cap from {@link ./memory} so the index discipline
 * never drifts.
 */
export function buildConsolidationPrompt(memoryRoot: string, transcriptDir: string, extra: string = ''): string {
  return `# Memory Consolidation

You are performing a reflective pass over your memory files. Synthesize what you've learned recently into durable, well-organized memories so that future sessions can orient quickly.

Memory directory: \`${memoryRoot}\`
If the directory does not exist yet, there is nothing to consolidate — create memories only when you have something durable worth persisting.

Session transcripts: \`${transcriptDir}\` (large JSONL files — grep narrowly, don't read whole files)

---

## Phase 1 — Orient

- \`ls\` the memory directory to see what already exists
- Read \`${MEMORY_INDEX_FILENAME}\` to understand the current index
- Skim existing topic files so you improve them rather than creating duplicates

## Phase 2 — Gather recent signal

Gather in PRIORITY ORDER — cheapest, highest-signal sources first:

1. **Dated daily working logs** (append-only, already distilled by you) at \`${memoryRoot}/logs/YYYY/MM/YYYY-MM-DD.md\`. Read the most recent days first — these are the primary signal.
2. **Drifted existing memories** — the topic files you skimmed in Phase 1 that now look stale or contradicted.
3. **Session transcripts** (last resort, large) — if you still need specific context, grep the JSONL transcripts for narrow terms rather than reading them whole:
\`grep -rn "<narrow term>" ${transcriptDir}/ --include="*.jsonl" | tail -50\`

## Phase 3 — Consolidate

For each thing worth remembering, write or update a memory file at the top level of the memory directory. Use the memory format and type conventions from your system prompt's auto-memory section — it is the source of truth.

Focus on:
- Merging new signal into existing topic files rather than creating near-duplicates
- Converting relative dates ("yesterday", "last week") to ABSOLUTE dates so they remain interpretable after time passes
- Deleting contradicted facts — if today's investigation disproves an old memory, fix it at the source

## Phase 4 — Prune and index

Update \`${MEMORY_INDEX_FILENAME}\` so it stays under ${MAX_ENTRYPOINT_LINES} lines AND under ~25KB. It is an **index**, not a dump — each entry is one line under ~150 characters: \`- [Title](file.md) — one-line hook\`. Never write memory content directly into it.

- Remove pointers to memories that are now stale, wrong, or superseded
- Add pointers to newly important memories
- Resolve contradictions — if two files disagree, fix the wrong one

---

Return a brief summary of what you consolidated, updated, or pruned. If nothing changed, say so.${extra ? `\n\n## Additional context\n\n${extra}` : ''}`;
}

/**
 * Discriminator for the spawnable background memory-consolidation task type
 * (parity: the reference's `TaskType` 'dream' entry in `tasks.ts` and
 * `DreamTaskState.type` in `DreamTask.ts`). AMI's `task` tool `mode` enum
 * (explore/general/fork) covers interactive delegation; `'dream'` is the distinct
 * NON-interactive, background consolidation run, carried as a typed descriptor
 * ({@link ConsolidationRunParams}) rather than a user-selectable tool mode.
 */
export const CONSOLIDATION_TASK_TYPE = 'dream' as const;

/**
 * Typed parameters driving one background memory-consolidation ("dream") run —
 * the core-layer analog of the reference's `registerDreamTask` opts /
 * `DreamTaskState`. Bundles the three run parameters so a
 * consumer surfacing the run (footer pill, task dialog, `/dream`) carries the
 * same state the reference does:
 *
 * - `sessionsReviewing` — how many prior sessions this pass is consolidating
 *   (recorded for display / diagnostics; never negative, always an integer).
 * - `priorMtime` — the pre-acquire lock mtime stashed so an abort/kill can rewind
 *   the schedule exactly like a failed launch (see {@link rollbackConsolidationLock}).
 * - `abortController` — cancels the in-flight consolidation subagent.
 */
export interface ConsolidationRunParams {
  /** Task-type discriminator (always {@link CONSOLIDATION_TASK_TYPE}). */
  type: typeof CONSOLIDATION_TASK_TYPE;
  /** Count of prior sessions being consolidated in this pass (>= 0, integer). */
  sessionsReviewing: number;
  /** Pre-acquire lock mtime, for rewinding the schedule on abort. */
  priorMtime: number;
  /** Cancels the in-flight consolidation subagent. */
  abortController: AbortController;
  /**
   * Live progress of the run — a user-facing introspection surface (parity:
   * the reference's `DreamTaskState`: a ticking elapsed timer, a
   * `reviewing N sessions · M files touched` summary, a running/completed/error
   * status, and a per-turn transcript with tool counts). Created running by
   * {@link beginConsolidationRun}; mutate it with {@link recordConsolidationTurn} /
   * {@link recordConsolidationFileTouched} and seal it with
   * {@link completeConsolidationProgress} / {@link failConsolidationProgress}.
   */
  progress: ConsolidationProgress;
}

/** Lifecycle state of a consolidation run (parity: the reference's DreamTask status). */
export type ConsolidationStatus = 'running' | 'completed' | 'error';

/**
 * One assistant turn of a consolidation run, as surfaced in the live transcript
 * (parity: the reference's `DreamTaskState.turns` rendered by `DreamDetailDialog` with a
 * per-turn tool count). `summary` is a short human label; `toolCount` is the
 * number of tool calls the turn issued (clamped to a non-negative integer).
 */
export interface ConsolidationTurn {
  /** Short human-readable label for the turn (e.g. an assistant text preview). */
  summary: string;
  /** Number of tool calls made in this turn (>= 0, integer). */
  toolCount: number;
}

/**
 * Live, mutable progress of a background consolidation ("dream") run — the core
 * building block behind the progress/introspection surface a frontend renders
 * (parity: the reference's `DreamTaskState`). Holds the start time (for a ticking
 * elapsed timer), the count of sessions under review, the set of files the pass
 * has touched, a running per-turn transcript, and a status that moves
 * `running` → `completed` | `error` exactly once.
 */
export interface ConsolidationProgress {
  /** Current lifecycle status. */
  status: ConsolidationStatus;
  /** Epoch ms the run started (anchors the elapsed timer). */
  startedAt: number;
  /** Epoch ms the run reached a terminal status, or `null` while running. */
  endedAt: number | null;
  /** Count of prior sessions this pass is consolidating (>= 0, integer). */
  sessionsReviewing: number;
  /** Absolute paths the pass has touched, de-duplicated in first-seen order. */
  filesTouched: string[];
  /** Per-turn transcript (oldest first). */
  turns: ConsolidationTurn[];
  /** Error message when `status === 'error'`, else `null`. */
  error: string | null;
}

/**
 * Create a fresh, `running` {@link ConsolidationProgress}. `sessionsReviewing`
 * is clamped to a non-negative integer (mirrors {@link beginConsolidationRun}).
 */
export function createConsolidationProgress(
  sessionsReviewing: number,
  now: number = Date.now(),
): ConsolidationProgress {
  return {
    status: 'running',
    startedAt: now,
    endedAt: null,
    sessionsReviewing: Math.max(0, Math.trunc(sessionsReviewing)),
    filesTouched: [],
    turns: [],
    error: null,
  };
}

/**
 * Append a turn to a {@link ConsolidationProgress} transcript. `toolCount` is
 * clamped to a non-negative integer. Appends regardless of status so a terminal
 * summary turn can still be recorded.
 */
export function recordConsolidationTurn(
  progress: ConsolidationProgress,
  turn: { summary: string; toolCount?: number },
): void {
  progress.turns.push({
    summary: turn.summary,
    toolCount: Math.max(0, Math.trunc(turn.toolCount ?? 0)),
  });
}

/**
 * Record that the run touched `absPath`. De-duplicates (first-seen order
 * preserved) so the `M files touched` count matches the reference's `filesTouched`
 * set semantics. Returns `true` when the path was newly added, `false` when it
 * was already present.
 */
export function recordConsolidationFileTouched(
  progress: ConsolidationProgress,
  absPath: string,
): boolean {
  if (progress.filesTouched.includes(absPath)) return false;
  progress.filesTouched.push(absPath);
  return true;
}

/**
 * Seal a run as `completed`. Idempotent: a no-op once the run already reached a
 * terminal status, so a completion can never overwrite a recorded error.
 */
export function completeConsolidationProgress(
  progress: ConsolidationProgress,
  now: number = Date.now(),
): void {
  if (progress.status !== 'running') return;
  progress.status = 'completed';
  progress.endedAt = now;
}

/**
 * Seal a run as `error` with a message. Idempotent once terminal (the first
 * terminal transition wins), so an abort-after-complete won't clobber the
 * recorded outcome.
 */
export function failConsolidationProgress(
  progress: ConsolidationProgress,
  error: string,
  now: number = Date.now(),
): void {
  if (progress.status !== 'running') return;
  progress.status = 'error';
  progress.endedAt = now;
  progress.error = error;
}

/**
 * Elapsed milliseconds for a run: live (`now - startedAt`) while `running`,
 * frozen (`endedAt - startedAt`) once terminal. Never negative.
 */
export function consolidationElapsedMs(
  progress: ConsolidationProgress,
  now: number = Date.now(),
): number {
  const end = progress.status === 'running' ? now : (progress.endedAt ?? now);
  return Math.max(0, end - progress.startedAt);
}

/**
 * Render the one-line progress summary a frontend shows beneath the title
 * (parity: the reference's `{elapsed} · reviewing N sessions{· M files touched}` in
 * `DreamDetailDialog`). The files clause is omitted when nothing has been
 * touched yet (the reference renders it only when `filesTouched.length > 0`). Elapsed
 * is formatted via the shared {@link formatDuration}; counts are pluralized via
 * the shared {@link plural} helper — no bespoke re-implementation.
 */
export function formatConsolidationProgress(
  progress: ConsolidationProgress,
  now: number = Date.now(),
): string {
  const elapsed = formatDuration(consolidationElapsedMs(progress, now));
  const sessions = `reviewing ${progress.sessionsReviewing} ${plural(progress.sessionsReviewing, 'session')}`;
  const files = progress.filesTouched.length > 0
    ? ` · ${progress.filesTouched.length} ${plural(progress.filesTouched.length, 'file')} touched`
    : '';
  return `${elapsed} · ${sessions}${files}`;
}

/**
 * A no-PII snapshot of a run for the `ami --introspect` / diagnostics surface
 * (parity: the DreamDetailDialog's displayed fields, minus any transcript text).
 * Carries counts and timing only — never file paths or turn summaries — so it is
 * safe to log/emit. `totalToolCalls` is the sum of per-turn tool counts.
 */
export function consolidationProgressSnapshot(
  progress: ConsolidationProgress,
  now: number = Date.now(),
): {
  status: ConsolidationStatus;
  elapsedMs: number;
  sessionsReviewing: number;
  filesTouched: number;
  turns: number;
  totalToolCalls: number;
  summary: string;
} {
  return {
    status: progress.status,
    elapsedMs: consolidationElapsedMs(progress, now),
    sessionsReviewing: progress.sessionsReviewing,
    filesTouched: progress.filesTouched.length,
    turns: progress.turns.length,
    totalToolCalls: progress.turns.reduce((sum, t) => sum + t.toolCount, 0),
    summary: formatConsolidationProgress(progress, now),
  };
}

/**
 * Type guard for a consolidation ("dream") run descriptor (parity: the reference's
 * `isDreamTask`). Lets a dispatcher distinguish a background consolidation run
 * from other spawnable work without a structural check at each call site.
 */
export function isDreamTask(task: unknown): task is ConsolidationRunParams {
  return (
    typeof task === 'object' &&
    task !== null &&
    'type' in task &&
    (task as { type?: unknown }).type === CONSOLIDATION_TASK_TYPE
  );
}

/**
 * Acquire the consolidation lock and, on success, return a typed
 * {@link ConsolidationRunParams} describing the run (parity: the reference's
 * acquire-then-`registerDreamTask`). Returns `null` when the lock is held by a
 * live, fresh holder (a diagnostic is emitted by {@link tryAcquireConsolidationLock}),
 * so a caller never double-launches a consolidation. An {@link AbortController}
 * is created when one is not supplied so the run is always cancellable.
 */
export function beginConsolidationRun(opts: {
  lockDir: string;
  sessionsReviewing: number;
  now?: number;
  abortController?: AbortController;
  debug?: ConsolidationDebug;
}): ConsolidationRunParams | null {
  const now = opts.now ?? Date.now();
  const priorMtime = tryAcquireConsolidationLock(opts.lockDir, now, opts.debug);
  if (priorMtime === null) return null;
  const sessionsReviewing = Math.max(0, Math.trunc(opts.sessionsReviewing));
  return {
    type: CONSOLIDATION_TASK_TYPE,
    sessionsReviewing,
    priorMtime,
    abortController: opts.abortController ?? new AbortController(),
    progress: createConsolidationProgress(sessionsReviewing, now),
  };
}

/**
 * Cancel an in-flight consolidation run and rewind the lock to its pre-acquire
 * state (parity: the reference's dream-task kill, which aborts the agent then rolls the
 * lock mtime back via the same path as a fork-failure). Idempotent: aborting an
 * already-aborted controller is a no-op. The run's live {@link ConsolidationProgress}
 * is sealed as an error ("aborted") so a surface watching it reflects the stop
 * (a no-op if the run already reached a terminal status).
 */
export function abortConsolidationRun(
  lockDir: string,
  run: ConsolidationRunParams,
  debug: ConsolidationDebug = defaultDebug,
): void {
  run.abortController.abort();
  failConsolidationProgress(run.progress, 'aborted');
  rollbackConsolidationLock(lockDir, run.priorMtime, debug);
}
