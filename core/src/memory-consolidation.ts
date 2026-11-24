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
import 'path';
import './memory';
import './logger';
import { formatDuration } from './conversation-summary';
import { plural } from './tools/tool-utils';
import { CONSOLIDATION_TASK_TYPE, consolidationLockPath, defaultDebug, isProcessRunning, resolveAutoDreamConfig, rollbackConsolidationLock } from './memory-consolidation-build-consolidation-prompt';
import type { ConsolidationDebug, ConsolidationGate, ConsolidationProgress, ConsolidationScheduler, ConsolidationStatus } from './memory-consolidation-build-consolidation-prompt';
export { AUTO_DREAM_DEFAULTS, CONSOLIDATION_DEBUG_COMPONENT, CONSOLIDATION_SCHEDULE_REASONS, CONSOLIDATION_STATUS, CONSOLIDATION_TASK_TYPE, buildConsolidationPrompt, consolidationLockPath, recordConsolidation, resolveAutoDreamConfig, rollbackConsolidationLock } from './memory-consolidation-build-consolidation-prompt';
export type { AutoDreamConfig, ConsolidationDebug, ConsolidationGate, ConsolidationProgress, ConsolidationScheduleDecision, ConsolidationScheduleReason, ConsolidationScheduler, ConsolidationStatus, ConsolidationTurn } from './memory-consolidation-build-consolidation-prompt';

/**
 * DI seam for lock I/O (ESM `fs` is frozen under Node 26). Production defaults to
 * real `fs`; tests force the empty mkdir catch / verify-read catch / PID-mismatch
 * arms by swapping individual methods.
 */
export const _consolidationFsHooks: {
  mkdirSync: typeof fs.mkdirSync;
  writeFileSync: typeof fs.writeFileSync;
  readFileSync: typeof fs.readFileSync;
  statSync: typeof fs.statSync;
} = {
  mkdirSync: fs.mkdirSync.bind(fs),
  writeFileSync: fs.writeFileSync.bind(fs),
  readFileSync: fs.readFileSync.bind(fs),
  statSync: fs.statSync.bind(fs),
};

/** A held lock is considered stale past this even if its PID is live (guards
 *  against PID reuse after a hard crash). Mirrors the reference's `HOLDER_STALE_MS`. */
const HOLDER_STALE_MS = 60 * 60 * 1000;

const MS_PER_HOUR = 60 * 60 * 1000;

/**
 * The lock file's mtime IS the last-consolidated timestamp (ms since epoch);
 * `0` when no lock exists yet. Per-turn cost: a single `stat`.
 */
export function readLastConsolidatedAt(lockDir: string): number {
  try {
    return _consolidationFsHooks.statSync(consolidationLockPath(lockDir)).mtimeMs;
  } catch {
    return 0;
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
    mtimeMs = _consolidationFsHooks.statSync(p).mtimeMs;
    const parsed = parseInt(_consolidationFsHooks.readFileSync(p, 'utf-8').trim(), 10);
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
    _consolidationFsHooks.mkdirSync(lockDir, { recursive: true });
  } catch {
    // Best effort — the write below surfaces a genuinely unwritable dir.
  }
  _consolidationFsHooks.writeFileSync(p, String(process.pid));

  // Two reclaimers both write → last wins the PID; the loser bails on re-read.
  let verify: string;
  try {
    verify = _consolidationFsHooks.readFileSync(p, 'utf-8');
  } catch {
    return null;
  }
  if (parseInt(verify.trim(), 10) !== process.pid) return null;

  return mtimeMs ?? 0;
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
