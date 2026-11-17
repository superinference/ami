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
export function tryAcquireConsolidationLock(lockDir: string, now: number = Date.now()): number | null {
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
    if (holderPid !== undefined && isProcessRunning(holderPid)) return null;
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
export function rollbackConsolidationLock(lockDir: string, priorMtime: number): void {
  const p = consolidationLockPath(lockDir);
  try {
    if (priorMtime === 0) {
      fs.unlinkSync(p);
      return;
    }
    fs.writeFileSync(p, '');
    const t = priorMtime / 1000; // utimes wants seconds
    fs.utimesSync(p, t, t);
  } catch {
    // Best effort — next trigger is merely delayed by up to minHours.
  }
}

/**
 * Stamp "consolidated now" (best effort) — e.g. after a manual consolidation.
 * Writes our PID and bumps mtime, creating the memory dir if needed.
 */
export function recordConsolidation(lockDir: string): void {
  try {
    fs.mkdirSync(lockDir, { recursive: true });
    fs.writeFileSync(consolidationLockPath(lockDir), String(process.pid));
  } catch {
    // Best effort.
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

Look for new information worth persisting. If you need specific context, grep the JSONL transcripts for narrow terms rather than reading them whole:
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
