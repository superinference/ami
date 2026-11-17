/**
 * Background memory-consolidation primitives (parity: Claude Code's
 * `services/autoDream/` — `autoDream.ts`, `consolidationLock.ts`,
 * `consolidationPrompt.ts`).
 *
 * A consolidation ("dream") pass is a reflective sweep over the auto-memory
 * directory that merges recent signal into durable, well-organized memories.
 * Claude gates how often it fires behind two scheduling knobs and records the
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

/**
 * Namespaced, human-readable debug sink for consolidation diagnostics (parity:
 * Claude Code's `[autoDream]` {@link logForDebugging} lines in
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

/** Scheduling knobs for background memory consolidation (parity: Claude's
 *  `AutoDreamConfig` — `minHours` / `minSessions`). */
export interface AutoDreamConfig {
  /** Minimum hours since the last consolidation before another may fire. */
  minHours: number;
  /** Minimum number of accumulated sessions before a consolidation may fire. */
  minSessions: number;
}

/** Defaults mirroring Claude Code's `DEFAULTS` record. */
export const AUTO_DREAM_DEFAULTS: AutoDreamConfig = {
  minHours: 24,
  minSessions: 5,
};

/**
 * Resolve an {@link AutoDreamConfig} from an untrusted raw value (e.g. a remote
 * feature-flag payload or a settings blob), applying Claude Code's defensive
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
 *  against PID reuse after a hard crash). Mirrors Claude's `HOLDER_STALE_MS`. */
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
 * Decide whether a background consolidation should fire now. Mirrors Claude's
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
  const hoursSince = lastConsolidatedAt === 0 ? Infinity : (now - lastConsolidatedAt) / MS_PER_HOUR;
  const timeGate = hoursSince >= config.minHours;
  const sessionGate = opts.sessionCount >= config.minSessions;
  return { due: timeGate && sessionGate, hoursSince, lastConsolidatedAt, config };
}

/**
 * Build the consolidation ("dream") prompt pointed at a memory root and a
 * transcript directory. Parity with Claude Code's `buildConsolidationPrompt`,
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
 * (parity: Claude Code's `TaskType` 'dream' entry in `tasks.ts` and
 * `DreamTaskState.type` in `DreamTask.ts`). AMI's `task` tool `mode` enum
 * (explore/general/fork) covers interactive delegation; `'dream'` is the distinct
 * NON-interactive, background consolidation run, carried as a typed descriptor
 * ({@link ConsolidationRunParams}) rather than a user-selectable tool mode.
 */
export const CONSOLIDATION_TASK_TYPE = 'dream' as const;

/**
 * Typed parameters driving one background memory-consolidation ("dream") run —
 * the core-layer analog of Claude Code's `registerDreamTask` opts /
 * `DreamTaskState`. Bundles the three run parameters so a
 * consumer surfacing the run (footer pill, task dialog, `/dream`) carries the
 * same state Claude does:
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
}

/**
 * Type guard for a consolidation ("dream") run descriptor (parity: Claude's
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
 * {@link ConsolidationRunParams} describing the run (parity: Claude's
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
  const priorMtime = tryAcquireConsolidationLock(opts.lockDir, opts.now ?? Date.now(), opts.debug);
  if (priorMtime === null) return null;
  return {
    type: CONSOLIDATION_TASK_TYPE,
    sessionsReviewing: Math.max(0, Math.trunc(opts.sessionsReviewing)),
    priorMtime,
    abortController: opts.abortController ?? new AbortController(),
  };
}

/**
 * Cancel an in-flight consolidation run and rewind the lock to its pre-acquire
 * state (parity: Claude's dream-task kill, which aborts the agent then rolls the
 * lock mtime back via the same path as a fork-failure). Idempotent: aborting an
 * already-aborted controller is a no-op.
 */
export function abortConsolidationRun(
  lockDir: string,
  run: ConsolidationRunParams,
  debug: ConsolidationDebug = defaultDebug,
): void {
  run.abortController.abort();
  rollbackConsolidationLock(lockDir, run.priorMtime, debug);
}
