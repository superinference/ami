/**
 * PID-scoped runtime namespacing — the ONE canonical home for naming EPHEMERAL
 * runtime artifacts so that two AMI instances running CONCURRENTLY against the
 * SAME project never collide on a shared scratch path.
 *
 * AMI keys all PERSISTED per-project state by cwd slug (`projectStateDir`,
 * reference-journal.ts) — correct for durable state that must survive a restart
 * and be shared across every worktree of a repo. But a cwd-only key is NOT enough
 * for EPHEMERAL, in-flight artifacts (atomic-write `.tmp` files, coordination
 * sidecars): two instances in one project derive the SAME scratch path and can
 * clobber each other's in-progress write, corrupting the target on rename.
 *
 * Parity: the reference's `getSwarmSocketName()`
 * (references/claude/claude-code-main/src/utils/swarm/constants.ts:L7-L14) embeds
 * `process.pid` (`claude-swarm-${process.pid}`) into its ephemeral swarm socket for
 * exactly this reason — a per-process token so concurrent instances get isolated
 * runtime namespaces rather than sharing/clobbering one. This module generalizes
 * that one deliberate pattern (a per-process scratch namespace + the liveness probe
 * needed to reclaim a dead instance's leftovers) into a reusable core primitive so
 * every call site uses ONE implementation instead of re-deriving
 * `${path}.${process.pid}.tmp` and `process.kill(pid, 0)` per file
 * (AGENTS.md No-Duplication Rule).
 *
 * It is a self-contained core building block: it imports nothing from
 * common/cli/vscode and depends only on the Node runtime.
 */

/**
 * The current process's runtime-instance namespace token — its PID. This is the
 * value embedded into every per-instance ephemeral path so two concurrent
 * instances (same project, same cwd) never share one scratch file. Direct parity
 * analog of the reference's `getSwarmSocketName()` PID embedding.
 */
export function runtimeInstancePid(): number {
  return process.pid;
}

/**
 * Whether `pid` refers to a live process. `process.kill(pid, 0)` sends no signal
 * but validates the target: it succeeds for a live process, raises ESRCH when no
 * such process exists (dead), and EPERM when the process exists but is owned by
 * another user (still alive). Anything that is not an explicit ESRCH is treated
 * as alive so a still-running holder is never mistaken for dead and its in-flight
 * artifact reclaimed out from under it. This is the ONE canonical liveness probe;
 * `utils/file-lock.ts` imports it rather than re-implementing the same try/catch.
 */
export function isPidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return (err as NodeJS.ErrnoException).code !== 'ESRCH';
  }
}

/**
 * Derive the PID-scoped atomic-write temp path for `target`:
 * `<target>.<pid>.tmp`. The default `pid` is this instance's
 * ({@link runtimeInstancePid}); an explicit `pid` is accepted for tests. A
 * writer renames this temp over `target` on success, so a crash leaves a
 * recoverable, OWNER-tagged orphan rather than a nameless `<target>.tmp` that a
 * concurrent instance might be mid-writing.
 */
export function instanceScopedTempName(target: string, pid: number = runtimeInstancePid()): string {
  return `${target}.${pid}.tmp`;
}

/**
 * Classify a directory entry `fileName` relative to a `baseName` config/state
 * file, identifying atomic-write temp orphans and (when present) the PID that
 * wrote them:
 *   - `undefined` — `fileName` is not a temp of `baseName`.
 *   - `null` — a legacy un-scoped `<baseName>.tmp` (no owner PID recorded).
 *   - a positive integer — a PID-scoped `<baseName>.<pid>.tmp` written by that PID.
 *
 * A crash-recovery sweep uses the returned PID to decide whether an orphan is
 * safe to reclaim: a temp whose PID is a DIFFERENT, still-{@link isPidAlive live}
 * process is an in-flight write by a concurrent instance and MUST be left alone;
 * a dead-PID (or legacy, or own-PID) orphan is reclaimable.
 */
export function parseInstanceTempPid(fileName: string, baseName: string): number | null | undefined {
  const legacy = `${baseName}.tmp`;
  if (fileName === legacy) return null;
  const prefix = `${baseName}.`;
  if (!fileName.startsWith(prefix) || !fileName.endsWith('.tmp')) return undefined;
  const middle = fileName.slice(prefix.length, fileName.length - '.tmp'.length);
  // A single run of digits with no leading zero (PIDs are positive integers).
  if (!/^[1-9][0-9]*$/.test(middle)) return undefined;
  const pid = Number(middle);
  return Number.isSafeInteger(pid) && pid > 0 ? pid : undefined;
}
