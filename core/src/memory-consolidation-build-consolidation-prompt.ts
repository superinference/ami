// Split from memory-consolidation.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import { MAX_ENTRYPOINT_LINES, MEMORY_INDEX_FILENAME } from './memory';
import { log } from './logger';

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
export const defaultDebug: ConsolidationDebug = (message) => log(CONSOLIDATION_DEBUG_COMPONENT, message);

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
export const LOCK_FILE = '.consolidate-lock';

/** Absolute path to the consolidation lock inside a memory directory. */
export function consolidationLockPath(lockDir: string): string {
  return path.join(lockDir, LOCK_FILE);
}

/** True when `pid` names a live process (EPERM still means it exists). */
export function isProcessRunning(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (e) {
    return (e as NodeJS.ErrnoException).code === 'EPERM';
  }
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

/** Why a {@link ConsolidationScheduler.evaluate} turn did (or did not) fire. */
export const CONSOLIDATION_SCHEDULE_REASONS = ['time-gate', 'scan-throttled', 'session-gate', 'due'] as const;

export type ConsolidationScheduleReason = (typeof CONSOLIDATION_SCHEDULE_REASONS)[number];

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

/** Lifecycle state of a consolidation run (parity: the reference's DreamTask status). */
export const CONSOLIDATION_STATUS = ['running', 'completed', 'error'] as const;

export type ConsolidationStatus = (typeof CONSOLIDATION_STATUS)[number];

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
