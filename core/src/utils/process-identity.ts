import { readFileSync } from 'fs';
import { readFile } from 'fs/promises';
import { randomBytes } from 'crypto';

/**
 * PID-reuse-safe process liveness.
 *
 * A bare `process.kill(pid, 0)` cannot protect a single-writer resource: the OS
 * reuses PIDs, so a crashed owner's PID may now belong to an unrelated live
 * process. Stealing a lock on "the PID is dead" is then wrong in both directions
 * — it can evict a live owner (reused PID looks alive) or, worse, authorize a
 * second writer onto a resource the original owner still holds.
 *
 * This module records a richer *identity* for a process — its boot-time-anchored
 * start time (`/proc/<pid>/stat` field 22 against `/proc/stat` btime on Linux)
 * plus an optional opaque spawn token — and probes it to a four-state verdict:
 *
 *   - `live`               — PID present AND at least one identity field verified.
 *   - `exited`             — PID is absent (ESRCH) or an exit was observed.
 *   - `identity-mismatch`  — PID present but a recorded field disagrees (reuse).
 *   - `indeterminate`      — PID present but no reuse-safe evidence could be read.
 *
 * The decision helper {@link isSafeToSteal} FAILS CLOSED: a resource may be
 * reclaimed only on `exited` / `identity-mismatch` (the owner is provably gone);
 * `live` and `indeterminate` both keep the resource held. This is the opposite
 * polarity from the legacy "steal on (pid-dead OR mtime-stale)" heuristic — an
 * unverifiable owner is treated as possibly-alive, never reclaimed, so two
 * writers can never land on one transcript.
 *
 * Mirrors Orca's `agent-session-process-identity-probe` (`readProcessStartTimeMs`
 * + `probeAgentSessionProcessIdentity`) and the fail-closed polarity of its lease
 * adjudication, adapted to AMI's four-state verdict.
 */

/** Scheduler/clock granularity slack allowed between two readings of one start time. */
export const PROCESS_START_TIME_TOLERANCE_MS = 2_000;

/** The reuse-safe identity recorded for a process (beyond its bare PID). */
export type ProcessIdentity = {
  pid: number;
  /** Boot-anchored start time (ms). `null` when the host cannot provide it. */
  processStartTimeMs: number | null;
  /** Opaque token the owner minted + recorded. `null` when none was minted. */
  spawnToken: string | null;
};

/** Which identity field a probe compared. */
export const PROCESS_IDENTITY_FIELDS = ['process-start-time', 'spawn-token'] as const;
export type ProcessIdentityField = (typeof PROCESS_IDENTITY_FIELDS)[number];

/** Four-state liveness verdict. */
export const PROCESS_LIVENESS_OUTCOMES = ['live', 'exited', 'identity-mismatch', 'indeterminate'] as const;
export type ProcessLivenessOutcome = (typeof PROCESS_LIVENESS_OUTCOMES)[number];

export type ProcessIdentityProbe = {
  outcome: ProcessLivenessOutcome;
  /** Field that drove a mismatch (diagnostics). */
  field?: ProcessIdentityField;
  /** Human-readable reason; set on `indeterminate`. */
  reason?: string;
  /** Fields positively verified (non-empty ⇒ `live`). */
  matchedOn?: readonly ProcessIdentityField[];
};

/** Mint a fresh opaque spawn token (128 bits of entropy, hex-encoded). */
export function mintSpawnToken(): string {
  return randomBytes(16).toString('hex');
}

/**
 * Presence probe for a PID. Only ESRCH proves absence; EPERM (present but owned
 * by another user) and every other/transient error are treated as PRESENT so a
 * restricted host never mistakes an unreadable live owner for a dead one.
 * PID ≤ 1 (self group / init) is never a legitimate recorded owner → absent.
 */
export function isPidPresent(pid: number): boolean {
  if (pid <= 1) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return (err as NodeJS.ErrnoException)?.code !== 'ESRCH';
  }
}

/**
 * Pure parse of a process start time from the raw `/proc/<pid>/stat` +
 * `/proc/stat` contents. Split out (no I/O) so the field-22-after-comm parsing
 * and the btime anchoring are unit-testable without a real `/proc`. `comm` can
 * contain spaces and parentheses, so fields are taken AFTER the last `") "`.
 * Returns `null` when either the start ticks or boot time is unparseable.
 */
export function computeStartTimeMs(procStat: string, systemStat: string): number | null {
  const after = procStat.slice(procStat.lastIndexOf(') ') + 2);
  const fields = after.split(' ');
  // starttime is field 22 overall; field 3 (index 0 here is field 3 "state").
  const ticks = Number(fields[19]);
  const bootTimeSeconds = Number(/^btime\s+(\d+)$/m.exec(systemStat)?.[1]);
  if (!Number.isFinite(ticks) || !Number.isFinite(bootTimeSeconds)) return null;
  return Math.round(bootTimeSeconds * 1000 + (ticks / 100) * 1000);
}

/**
 * Synchronous boot-anchored start-time read. Linux reads `/proc`; other
 * platforms return `null` (unavailable identity stays indeterminate, never a
 * false liveness proof). Any read/parse failure is `null`.
 */
export function readProcessStartTimeMsSync(
  pid: number,
  platform: NodeJS.Platform = process.platform,
): number | null {
  if (platform !== 'linux') return null;
  try {
    const stat = readFileSync(`/proc/${pid}/stat`, 'utf-8');
    const systemStat = readFileSync('/proc/stat', 'utf-8');
    return computeStartTimeMs(stat, systemStat);
  } catch {
    return null;
  }
}

/** Async counterpart to {@link readProcessStartTimeMsSync}. */
export async function readProcessStartTimeMs(
  pid: number,
  platform: NodeJS.Platform = process.platform,
): Promise<number | null> {
  if (platform !== 'linux') return null;
  try {
    const [stat, systemStat] = await Promise.all([
      readFile(`/proc/${pid}/stat`, 'utf-8'),
      readFile('/proc/stat', 'utf-8'),
    ]);
    return computeStartTimeMs(stat, systemStat);
  } catch {
    return null;
  }
}

/** Snapshot the current (or a given) process's reuse-safe identity. */
export function captureProcessIdentitySync(opts?: {
  pid?: number;
  spawnToken?: string | null;
  platform?: NodeJS.Platform;
}): ProcessIdentity {
  const pid = opts?.pid ?? process.pid;
  return {
    pid,
    processStartTimeMs: readProcessStartTimeMsSync(pid, opts?.platform),
    spawnToken: opts?.spawnToken ?? null,
  };
}

/** Injectable probes so a probe runs deterministically in tests. */
export interface ProcessProbeDeps {
  /** Observed-exit shortcut: when true the owner is `exited` without any probe. */
  observedExit?: boolean;
  isPidPresent?: (pid: number) => boolean;
  readProcessStartTimeMs?: (pid: number, platform?: NodeJS.Platform) => number | null;
  /** Token the running child echoed back; `null`/`undefined` ⇒ no token evidence. */
  readEchoedSpawnToken?: (identity: ProcessIdentity) => string | null | undefined;
  platform?: NodeJS.Platform;
}

/**
 * Core adjudication shared by the sync/async probes. `startTime` is the already
 * read observed start time (`null` when unread/unavailable) and `echoedToken`
 * the already read echoed token — so both entry points reuse ONE verdict ladder.
 */
function adjudicate(
  identity: ProcessIdentity,
  present: boolean,
  startTime: number | null,
  echoedToken: string | null | undefined,
): ProcessIdentityProbe {
  if (!present) return { outcome: 'exited' };
  const matchedOn: ProcessIdentityField[] = [];
  if (echoedToken !== null && echoedToken !== undefined && identity.spawnToken !== null) {
    if (echoedToken !== identity.spawnToken) {
      return { outcome: 'identity-mismatch', field: 'spawn-token' };
    }
    matchedOn.push('spawn-token');
  }
  if (identity.processStartTimeMs !== null && startTime !== null) {
    if (Math.abs(startTime - identity.processStartTimeMs) > PROCESS_START_TIME_TOLERANCE_MS) {
      if (matchedOn.includes('spawn-token')) {
        // A token-authenticated child cannot be proved dead by a contradicting clock.
        return { outcome: 'indeterminate', reason: 'process identity evidence contradicted' };
      }
      return { outcome: 'identity-mismatch', field: 'process-start-time' };
    }
    matchedOn.push('process-start-time');
  }
  if (matchedOn.length === 0) {
    // Present PID without reuse-safe evidence cannot authorize another writer.
    return {
      outcome: 'indeterminate',
      reason: 'pid present but neither spawn token nor start time could be verified',
    };
  }
  return { outcome: 'live', matchedOn };
}

/** Synchronous four-state liveness probe. */
export function probeProcessIdentitySync(
  identity: ProcessIdentity,
  deps: ProcessProbeDeps = {},
): ProcessIdentityProbe {
  if (deps.observedExit) return { outcome: 'exited' };
  const present = (deps.isPidPresent ?? isPidPresent)(identity.pid);
  if (!present) return { outcome: 'exited' };
  const echoedToken = deps.readEchoedSpawnToken?.(identity);
  let startTime: number | null = null;
  if (identity.processStartTimeMs !== null) {
    const read = deps.readProcessStartTimeMs ?? readProcessStartTimeMsSync;
    startTime = read(identity.pid, deps.platform ?? process.platform);
  }
  return adjudicate(identity, present, startTime, echoedToken);
}

export interface ProcessProbeAsyncDeps {
  observedExit?: boolean;
  isPidPresent?: (pid: number) => boolean;
  readProcessStartTimeMs?: (pid: number, platform?: NodeJS.Platform) => Promise<number | null>;
  readEchoedSpawnToken?: (identity: ProcessIdentity) => Promise<string | null | undefined>;
  platform?: NodeJS.Platform;
}

/** Asynchronous four-state liveness probe (reads `/proc` off the event loop). */
export async function probeProcessIdentity(
  identity: ProcessIdentity,
  deps: ProcessProbeAsyncDeps = {},
): Promise<ProcessIdentityProbe> {
  if (deps.observedExit) return { outcome: 'exited' };
  const present = (deps.isPidPresent ?? isPidPresent)(identity.pid);
  if (!present) return { outcome: 'exited' };
  const echoedToken = deps.readEchoedSpawnToken
    ? await deps.readEchoedSpawnToken(identity).catch(() => null)
    : undefined;
  let startTime: number | null = null;
  if (identity.processStartTimeMs !== null) {
    const read = deps.readProcessStartTimeMs ?? readProcessStartTimeMs;
    startTime = await read(identity.pid, deps.platform ?? process.platform).catch(() => null);
  }
  return adjudicate(identity, present, startTime, echoedToken);
}

/**
 * Fail-closed steal decision. A resource may be reclaimed ONLY when its prior
 * owner is PROVABLY gone — `exited` or `identity-mismatch`. `live` keeps it (the
 * owner is still writing); `indeterminate` ALSO keeps it (an unverifiable owner
 * is treated as possibly-alive, never stolen). This is the invariant that
 * prevents two writers on one resource.
 */
export function isSafeToSteal(probe: ProcessIdentityProbe): boolean {
  return probe.outcome === 'exited' || probe.outcome === 'identity-mismatch';
}

/** True only when the probe positively verified the owner is still alive. */
export function isProvenAlive(probe: ProcessIdentityProbe): boolean {
  return probe.outcome === 'live';
}
