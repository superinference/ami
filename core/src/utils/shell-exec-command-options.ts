// Split from shell.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as child_process from 'child_process';
import type { ShellType } from './shell-build-ripgrep-shell-integration';

export interface ExecCommandOptions {
  cwd: string;
  timeout?: number;
  abortSignal?: AbortSignal;
  onData?: (chunk: string) => void;
  env?: NodeJS.ProcessEnv;
  stallTimeoutMs?: number;
  /**
   * Optional string fed to the child process's stdin (then closed). When
   * omitted, stdin is `'ignore'`d as before. Lets callers pipe a payload into a
   * command (e.g. `git apply`, `patch`, `gpg`) without shell interpolation.
   */
  input?: string;
  /**
   * stdin mode for the child when no {@link input} payload is supplied
   * (`'ignore'` by default so interactive prompts are detected-and-killed by
   * stall detection). `'inherit'` connects the parent terminal's stdin; `'pipe'`
   * opens a pipe the caller drives. Ignored when `input` is set (that path always
   * pipes). Parity with the reference's exec wrapper `stdin` option
   * (execFileNoThrow.ts:L22).
   */
  stdin?: 'ignore' | 'inherit' | 'pipe';
  /**
   * When `false`, a command that exits with a non-zero code resolves with empty
   * `stdout`/`stderr` (only the exit code/signal are kept). Useful for
   * probe/detection commands whose noisy output on failure should be discarded.
   * Defaults to `true` (output always preserved). Parity with the reference's
   * `preserveOutputOnError` (execFileNoThrow.ts:L17). A timeout/abort/launch
   * failure (`exitCode === null`) always keeps its output so the caller can see
   * why it failed.
   */
  preserveOutputOnError?: boolean;
  /**
   * Shell family to run the command through (`'bash'` | `'powershell'`). When
   * omitted, {@link resolveDefaultShell} picks it from the `AMI_DEFAULT_SHELL`
   * env knob (default `'bash'`). Parity with the reference's `exec({ shellType })`
   * (Shell.ts). PowerShell is spawned with hardened, non-interactive flags and
   * exit-code normalization (see {@link buildPowerShellArgs} /
   * {@link withPowerShellExitCode}).
   */
  shellType?: ShellType;
  /**
   * Explicit shell executable path, overriding auto-resolution for this call.
   * For `'bash'` it replaces {@link findSuitableShell}; for `'powershell'` it
   * replaces {@link getCachedPowerShellPath}. Parity with the reference's
   * configurable `shellPath` provider parameter (shellProvider.ts). When blank,
   * the resolver / env override is used.
   */
  shellPath?: string;
  /**
   * When `true`, skip shell-environment snapshot sourcing for this call even if
   * {@link SHELL_SNAPSHOT_ENV} is enabled (the command runs with a plain
   * non-login shell). Used for probe/internal commands that must not inherit the
   * user's aliases/functions. Parity with the reference's `createBashShellProvider({
   * skipSnapshot })` (bashProvider.ts). No effect when snapshots are disabled.
   */
  skipSnapshot?: boolean;
  /**
   * Structured progress callback, distinct from {@link onData}'s raw-chunk pipe.
   * Fires on a cadence ({@link progressIntervalMs}) with, positionally:
   *   `(lastLines, allLines, totalLines, totalBytes, isIncomplete)`
   * — the last 5 lines, the last 100 lines, the running (monotone) total line
   * count, total bytes seen, and whether the delivered sample omits earlier
   * output (`totalLines > 100`). It fires EVEN when no new output arrived since
   * the last tick (empty `lastLines`/`allLines`) so a caller's progress loop
   * wakes on a silent long-running command and can decide to background it.
   * Parity with the reference's `ExecOptions.onProgress` (Shell.ts:L161-169) +
   * the always-tick poller (TaskOutput.ts:L119-149). `onData` still streams raw
   * chunks for callers that only want the bytes.
   */
  onProgress?: (lastLines: string, allLines: string, totalLines: number, totalBytes: number, isIncomplete: boolean) => void;
  /**
   * Heartbeat cadence (ms) for {@link onProgress}. Only used when `onProgress`
   * is set; defaults to {@link DEFAULT_PROGRESS_INTERVAL_MS}. A value `<= 0`
   * disables the heartbeat entirely (no progress ticks).
   */
  progressIntervalMs?: number;
  /**
   * When `false`, the command's final working directory (captured via `pwd -P`
   * on the bash path) is written into {@link ExecCommandResult.cwd} so a caller
   * can persist a `cd` across invocations (parity with the reference's
   * `ExecOptions.preventCwdChanges` / `nativeCwdFilePath`, Shell.ts:L170,
   * L385-414). Defaults to NOT tracking (undefined/`true`) so AMI's existing
   * per-call `cwd` contract and all current callers are unchanged — a caller
   * opts in by passing `preventCwdChanges: false`. No-op for the PowerShell
   * shell family.
   */
  preventCwdChanges?: boolean;
  /**
   * When `true`, stream the command's COMPLETE stdout to a temp file (capped at
   * {@link MAX_PERSISTED_OUTPUT_BYTES} = 64MB) so the model can retrieve the whole
   * output — including the middle that the in-memory head+tail window
   * ({@link HEAD_CHARS}/{@link TAIL_CHARS}) drops — via `file_read`. The path +
   * byte size are returned in {@link ExecCommandResult.fullOutputPath} /
   * {@link ExecCommandResult.fullOutputSize}, but ONLY when the captured output
   * actually overflowed the in-memory window (otherwise the inline result is
   * already complete and the temp file is discarded, so small commands pay no
   * retrieval surface). Defaults to off. Parity with the reference's
   * persist-complete-output-to-disk model (BashTool.tsx:L725-753).
   */
  persistFullOutput?: boolean;
}

export interface ExecCommandResult {
  stdout: string;
  stderr: string;
  exitCode: number | null;
  /**
   * The terminating signal (e.g. `'SIGSEGV'`, `'SIGKILL'`) when the process was
   * killed by one, else `null`. Lets callers distinguish a signal-kill / crash
   * from a timeout or clean exit, both of which otherwise surface as
   * `exitCode: null`.
   */
  signal: NodeJS.Signals | null;
  /**
   * `true` when a SOFT `'interrupt'` abort (the caller submitted a new message
   * mid-command, i.e. `abortSignal.reason === 'interrupt'`) resolved this call
   * WITHOUT killing the process — it is left running so the caller can background
   * it and keep its in-flight/partial work. A hard abort (any other reason)
   * still kills the process tree and leaves this unset. Parity with the reference
   * Code's `#abortHandler` interrupt branch (ShellCommand.ts:L186-193).
   */
  backgrounded?: boolean;
  /**
   * The command's final working directory, captured via `pwd -P` on the bash
   * path, present ONLY when the caller opted in with
   * {@link ExecCommandOptions.preventCwdChanges} `=== false`. Lets a caller
   * persist a `cd` across invocations. Absent when tracking is off, on the
   * PowerShell path, or when the command exited before `pwd -P` ran.
   */
  cwd?: string;
  /**
   * Path to the temp file holding the command's COMPLETE stdout, present ONLY
   * when the caller set {@link ExecCommandOptions.persistFullOutput} AND the
   * captured stdout overflowed the in-memory head+tail window (so the complete
   * output — including the dropped middle — is on disk for `file_read`). Absent
   * when persistence was off, the output fit in memory, or the command was
   * backgrounded / its output was suppressed.
   */
  fullOutputPath?: string;
  /**
   * Byte size of the persisted complete stdout ({@link fullOutputPath}), capped
   * at {@link MAX_PERSISTED_OUTPUT_BYTES}. Present iff {@link fullOutputPath} is.
   */
  fullOutputSize?: number;
}

/**
 * Destroy stdout/stderr/stdin streams before killing a process to prevent
 * EBADF errors. When the child process is killed, the OS closes its pipe
 * ends; if the parent-side streams are still open, Node.js may throw EBADF
 * when it later tries to close the now-invalid file descriptors.
 */
export function destroyStreams(proc: child_process.ChildProcess): void {
  for (const stream of [proc.stdout, proc.stderr, proc.stdin]) {
    if (stream && !stream.destroyed) {
      try { stream.destroy(); } catch { /* EBADF is expected here */ }
    }
  }
}

/**
 * The POSIX `ps` invocation that dumps the whole process table as `pid ppid`
 * rows. Shared by the descendant ({@link collectProcessTree}) and ancestor
 * ({@link collectAncestorPids}) process-tree walks so the table-read command has
 * ONE definition (No-Duplication rule).
 */
export const PS_TABLE_CMD = 'ps -A -o pid=,ppid=';

/**
 * Default hard timeout (ms) for the single process-table read backing the
 * ancestor walk (parity with the reference's getAncestorPidsAsync 3000ms cap), so a
 * hung `ps`/`wmic` can never stall the caller.
 */
export const ANCESTOR_WALK_TIMEOUT_MS = 3000;

/**
 * Default hard timeout (ms) for the single process-table read backing the
 * DESCENDANT walk ({@link collectProcessTree}) used by {@link killTree} (parity
 * with the reference's getChildPids 1000ms cap), so a hung `ps` during a
 * kill/cleanup/shutdown can never stall the kill path. On timeout the read
 * throws and collectProcessTree degrades to `[rootPid]` — the same empty-result
 * fallback the reference's getChildPids applies.
 */
export const DESCENDANT_WALK_TIMEOUT_MS = 1000;
