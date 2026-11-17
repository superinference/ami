import {
  shellHooks,
  readParentPidMap,
  collectAncestorPids,
} from './shell';

// Platform-agnostic `ps`-style process-introspection helpers (parity with Claude
// Code's genericProcessUtils.ts). These complement the ancestor/descendant PID
// walks already in shell.ts (collectAncestorPids / collectProcessTree) by adding
// the two capabilities that walk COMMAND LINES and DIRECT CHILDREN of an
// arbitrary pid — used upstream for parent-shell / terminal / IDE detection and
// for lock-holder identification. Every helper is no-throw: an unavailable
// `ps`/`wmic`, a non-zero exit, or a timeout degrades to a null/empty result so a
// caller never crashes on process-table introspection.
//
// NOTE on Windows: `ps` inside cygwin/WSL cannot see host processes reliably, so
// the Windows branches shell out to `wmic`, matching readParentPidMap's choice in
// shell.ts (one consistent Windows process-table tool, not powershell here + wmic
// there).

/**
 * Hard timeout (ms) for a single-pid command lookup and for direct-child
 * enumeration — parity with Claude Code's getProcessCommand/getChildPids 1000ms
 * cap, so a hung `ps`/`wmic` can never stall the caller.
 */
export const PROCESS_COMMAND_TIMEOUT_MS = 1000;

/**
 * Prefix wmic emits for each `get CommandLine /value` record.
 */
const WMIC_COMMANDLINE_PREFIX = 'CommandLine=';

/**
 * Get the command-line string for a single PID, or `null` when it cannot be
 * determined (process gone, non-zero exit, timeout, or an invalid pid).
 *
 * Parity with Claude Code's getProcessCommand (genericProcessUtils.ts:L94-L107):
 * POSIX reads `ps -o command= -p <pid>`; Windows reads
 * `wmic process where processid=<pid> get CommandLine /value` (the `/value` form
 * prints a single `CommandLine=<value>` record that survives command lines
 * containing spaces, unlike column-formatted `get`). Bounded by
 * {@link PROCESS_COMMAND_TIMEOUT_MS}; no-throw → `null`.
 *
 * `pid` is validated to a positive integer before interpolation so a malformed
 * value can never be injected into the shell command.
 */
export function getProcessCommand(pid: string | number): string | null {
  const pidNum = Number(pid);
  if (!Number.isInteger(pidNum) || pidNum <= 0) return null;
  const isWindows = shellHooks.platform() === 'win32';
  let out: string;
  try {
    const cmd = isWindows
      ? `wmic process where processid=${pidNum} get CommandLine /value`
      : `ps -o command= -p ${pidNum}`;
    out = shellHooks
      .execSync(cmd, {
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'ignore'],
        timeout: PROCESS_COMMAND_TIMEOUT_MS,
      })
      .toString();
  } catch {
    return null; // no-throw fallback
  }
  if (isWindows) {
    for (const line of out.split('\n')) {
      const trimmed = line.trim();
      if (trimmed.startsWith(WMIC_COMMANDLINE_PREFIX)) {
        const value = trimmed.slice(WMIC_COMMANDLINE_PREFIX.length).trim();
        return value ? value : null;
      }
    }
    return null;
  }
  const trimmed = out.trim();
  return trimmed ? trimmed : null;
}

/**
 * Enumerate the DIRECT child PIDs of `pid` (its immediate children only, not the
 * whole descendant tree — use {@link collectProcessTree} for that).
 *
 * Parity with Claude Code's getChildPids (genericProcessUtils.ts:L163-L184),
 * which runs `pgrep -P <pid>` / a Win32 ParentProcessId query. AMI instead reuses
 * the canonical {@link readParentPidMap} process-table reader (child → parent) and
 * filters it for entries whose parent is `pid` — a single table read shared with
 * the ancestor walk (No-Duplication), bounded by
 * {@link PROCESS_COMMAND_TIMEOUT_MS}, no-throw → `[]`.
 */
export function collectChildPids(pid: string | number): number[] {
  const pidNum = Number(pid);
  if (!Number.isInteger(pidNum) || pidNum < 0) return [];
  const parentOf = readParentPidMap(PROCESS_COMMAND_TIMEOUT_MS);
  const children: number[] = [];
  for (const [child, parent] of parentOf) {
    if (parent === pidNum) children.push(child);
  }
  return children;
}

/**
 * Collect the command lines for `pid` and each of its ancestors, nearest first —
 * parity with Claude Code's getAncestorCommandsAsync (genericProcessUtils.ts:
 * L115-L156). The returned array starts with `pid`'s own command and then each
 * ancestor's, capped at `maxDepth` entries total (the start pid counts as the
 * first), with pids whose command cannot be read skipped (parity with the
 * reference's `filter(Boolean)`).
 *
 * Built from the two canonical primitives rather than a second bespoke shell
 * loop: {@link collectAncestorPids} supplies the parent chain (one process-table
 * read, maxDepth- and cycle-bounded) and {@link getProcessCommand} resolves each
 * pid's command line (one per pid, individually time-bounded). This avoids
 * duplicating a second — and on Windows unreliable — command-table parser, at the
 * cost of up to `maxDepth` bounded `ps`/`wmic` invocations instead of the
 * reference's single one. No-throw → `[]`.
 */
export function collectAncestorCommands(
  pid: string | number,
  maxDepth = 10,
): string[] {
  const pidNum = Number(pid);
  if (!Number.isInteger(pidNum) || pidNum <= 1 || maxDepth <= 0) return [];
  // Start pid counts as the first of maxDepth entries, so walk maxDepth-1 ancestors.
  const chain = [pidNum, ...collectAncestorPids(pidNum, maxDepth - 1)];
  const commands: string[] = [];
  for (const p of chain) {
    const cmd = getProcessCommand(p);
    if (cmd) commands.push(cmd);
  }
  return commands;
}
