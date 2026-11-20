/**
 * TMUX SOCKET ISOLATION (core helper)
 * ===================================
 * Canonical home for AMI's isolated-tmux-socket logic. Parity with the reference's
 * `src/utils/tmuxSocket.ts`.
 *
 * WHY THIS EXISTS
 * ---------------
 * Without isolation, a model-issued `tmux kill-server` / `tmux kill-session`
 * run through the Bash tool would tear down the *user's* live tmux session when
 * AMI was launched from inside tmux (same destructive-side-effect class as the
 * rm-rf safety invariant). `bash-security.ts` blocks the destructive subcommands
 * outright; this module provides the complementary *positive* isolation: point
 * AMI-run tmux at a private, AMI-managed socket directory so its servers never
 * collide with the user's default socket.
 *
 * AMI ADAPTATION vs the reference
 * -------------------------------
 * The reference creates a named socket `claude-<PID>` (via tmux's `-L` flag) and
 * overrides the `TMUX` env var in every child. AMI instead isolates via
 * `TMUX_TMPDIR` (the directory tmux uses for its default server socket): tmux
 * still auto-starts/connects its server on demand instead of mistaking an
 * overridden `TMUX` for a nested session, and no per-command rewriting is needed.
 * The socket NAME/PATH helpers below mirror the reference's `getClaudeSocketName`
 * / fallback-path builder so the isolated server can be named for teardown and
 * introspection.
 *
 * All of this is OPT-IN via {@link TMUX_ISOLATION_ENV} so default behavior is
 * unchanged. The state machine (cached availability probe, concurrent-init
 * dedup, graceful degradation, on-exit teardown) mirrors the reference's
 * `checkTmuxAvailable` / `ensureSocketInitialized` ordering.
 */

import * as child_process from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
// Canonical shell debug knob lives in shell.ts; imported (not redefined) to keep a
// single source of truth. Only read lazily inside onDebug at runtime, so the
// shell.ts <-> tmux-socket.ts cycle never touches the binding during module eval.
import { SHELL_DEBUG_ENV } from './shell-debug-env';

/** Socket-name prefix for AMI's isolated tmux server (parity: `claude` -> `ami`). */
export const AMI_TMUX_SOCKET_PREFIX = 'ami';

/** Directory (under the resolved tmp base) that holds AMI's isolated tmux sockets. */
export const AMI_TMUX_SOCKET_DIR = 'ami-tmux';

/**
 * The default socket name tmux opens under a given `TMUX_TMPDIR`. tmux names its
 * default server socket `default` inside `$TMUX_TMPDIR/tmux-<uid>/`.
 */
export const AMI_TMUX_DEFAULT_SOCKET = 'default';

/**
 * Env knob (opt-in) that enables AMI's isolated tmux socket for bash commands.
 * When truthy (`1`/`true`/`on`/`yes`), a bash command that references `tmux`
 * (see {@link commandUsesTmux}) runs with `TMUX_TMPDIR` pointed at an AMI-managed
 * socket directory (created on first use), isolating AMI-run tmux servers from
 * the user's default socket. Unset/blank/falsey leaves commands untouched. An
 * opt-in introspection surface per AMI's introspection-on-every-change invariant.
 */
export const TMUX_ISOLATION_ENV = 'AMI_TMUX_ISOLATION';

/**
 * Mockable seam (ESM-safe). Tests replace these via
 * `mock.method(tmuxSocketHooks, 'name', …)` to drive the availability probe,
 * the async dir-creation / failure path, the on-exit teardown, and the
 * best-effort server kill without touching a real tmux binary. Behavior-
 * preserving: production routes every side-effecting call through this object.
 */
export const tmuxSocketHooks = {
  /**
   * Synchronous, best-effort tmux availability probe (parity with the reference's
   * cached `checkTmuxAvailable`, which runs `which tmux` / `tmux -V`). Returns
   * true only when `tmux -V` exits 0. Never throws.
   */
  probeTmuxAvailable: ((): boolean => {
    try {
      const r = child_process.spawnSync('tmux', ['-V'], { stdio: 'ignore' });
      return !r.error && r.status === 0;
    } catch {
      return false;
    }
  }) as () => boolean,
  /** Async dir creation for the isolated socket directory. */
  mkdirAsync: fs.promises.mkdir as typeof fs.promises.mkdir,
  /** Current uid (0 on platforms without `process.getuid`, e.g. Windows/WSL root). */
  getuid: ((): number => (typeof process.getuid === 'function' ? process.getuid() : 0)) as () => number,
  /**
   * Register an on-process-exit teardown. Defaults to `process.once('exit', …)`
   * so the handler runs exactly once at shutdown; the handler must be synchronous
   * (exit handlers cannot await), which is why {@link killAmiTmuxServerSync} uses
   * `spawnSync`.
   */
  registerTeardown: ((fn: () => void): void => { process.once('exit', fn); }) as (fn: () => void) => void,
  /**
   * Best-effort synchronous kill of the isolated tmux server at `socketPath`.
   * Never throws — the server may already be gone, which is fine.
   */
  killAmiTmuxServerSync: ((socketPath: string): void => {
    try {
      child_process.spawnSync('tmux', ['-S', socketPath, 'kill-server'], { stdio: 'ignore' });
    } catch {
      /* best effort — server may already be dead */
    }
  }) as (socketPath: string) => void,
  /**
   * Diagnostic sink (gated on {@link SHELL_DEBUG_ENV}). Surfaces the isolation
   * decision, availability degradation, and setup failures — the introspection
   * surface for this feature. Default writes one `[tmux-socket]` line to stderr
   * only when debug is enabled, so default runs are silent.
   */
  onDebug: ((message: string): void => {
    if ((process.env[SHELL_DEBUG_ENV] || '').trim() !== '') {
      process.stderr.write(`[tmux-socket] ${message}\n`);
    }
  }) as (message: string) => void,
};

/** True when {@link TMUX_ISOLATION_ENV} names an enabled value. */
export function tmuxIsolationEnabled(): boolean {
  const raw = (process.env[TMUX_ISOLATION_ENV] || '').trim().toLowerCase();
  return raw === '1' || raw === 'true' || raw === 'on' || raw === 'yes';
}

/** True when a shell command references the `tmux` binary (word-boundary match). */
export function commandUsesTmux(command: string): boolean {
  return /\btmux\b/.test(command);
}

/**
 * The socket name for AMI's isolated tmux session: `ami-<PID>` (parity with the
 * reference's `claude-<PID>`). Stable for the lifetime of the process.
 */
export function getAmiTmuxSocketName(): string {
  return `${AMI_TMUX_SOCKET_PREFIX}-${process.pid}`;
}

/**
 * The isolated `TMUX_TMPDIR` directory under a given tmp base
 * (`<tmpBase>/ami-tmux`).
 */
export function getAmiTmuxSocketDir(tmpBase: string): string {
  return path.join(tmpBase, AMI_TMUX_SOCKET_DIR);
}

/**
 * The absolute path of the isolated tmux *server socket* for a given tmp base.
 * Mirrors the reference's TMPDIR-aware, uid-scoped fallback-path builder
 * (`tmuxSocket.ts:L376-382`, `posix.join($TMPDIR, tmux-<uid>, socket)`): tmux
 * opens its default server socket at `$TMUX_TMPDIR/tmux-<uid>/default`. POSIX
 * separators are used because on Windows this path lives inside WSL. Used for
 * on-exit teardown and introspection.
 */
export function getAmiTmuxServerSocketPath(tmpBase: string): string {
  const uid = tmuxSocketHooks.getuid();
  return path.posix.join(getAmiTmuxSocketDir(tmpBase), `tmux-${uid}`, AMI_TMUX_DEFAULT_SOCKET);
}

// ---------------------------------------------------------------------------
// Lazy singleton state machine (parity with the reference's init ordering):
//   • availability is probed once and cached (checkTmuxAvailable),
//   • concurrent initializers share one initPromise (no double setup),
//   • failures degrade gracefully (never throw into the bash path),
//   • the isolated server is torn down once on process exit.
// ---------------------------------------------------------------------------

let availabilityChecked = false;
let tmuxAvailable = false;
let initPromise: Promise<string | null> | null = null;
let teardownRegistered = false;

/** Cached tmux-availability status (false until {@link checkTmuxAvailable} has run). */
export function isTmuxAvailable(): boolean {
  return availabilityChecked && tmuxAvailable;
}

/**
 * Probe tmux availability once and cache it for the process lifetime (parity
 * with the reference's cached `checkTmuxAvailable`). When tmux is absent, bash
 * commands run un-isolated (graceful degradation) rather than failing.
 */
export function checkTmuxAvailable(): boolean {
  if (!availabilityChecked) {
    tmuxAvailable = tmuxSocketHooks.probeTmuxAvailable();
    availabilityChecked = true;
    if (!tmuxAvailable) {
      tmuxSocketHooks.onDebug('tmux not installed; bash commands run without tmux isolation');
    }
  }
  return tmuxAvailable;
}

function registerTeardownOnce(serverSocketPath: string): void {
  if (teardownRegistered) return;
  teardownRegistered = true;
  tmuxSocketHooks.registerTeardown(() => tmuxSocketHooks.killAmiTmuxServerSync(serverSocketPath));
}

async function doInitialize(tmpBase: string): Promise<string | null> {
  if (!checkTmuxAvailable()) {
    return null;
  }
  const dir = getAmiTmuxSocketDir(tmpBase);
  await tmuxSocketHooks.mkdirAsync(dir, { recursive: true });
  registerTeardownOnce(getAmiTmuxServerSocketPath(tmpBase));
  tmuxSocketHooks.onDebug(`TMUX_TMPDIR=${dir} (isolated socket ${getAmiTmuxSocketName()})`);
  return dir;
}

/**
 * Ensure the isolated tmux socket directory exists and return it (or `null` when
 * tmux is unavailable / setup failed — graceful degradation, never throws).
 *
 * Idempotent and concurrency-safe: concurrent callers share a single in-flight
 * `initPromise` so the directory is prepared exactly once per burst and a race
 * cannot run two setups. On failure the promise is cleared so a later command
 * can retry (parity with the reference's `ensureSocketInitialized` dedup +
 * graceful-degradation shape).
 */
export async function ensureTmuxIsolationDir(tmpBase: string): Promise<string | null> {
  if (initPromise) {
    try {
      return await initPromise;
    } catch {
      // The owning caller logs + degrades; non-owning callers just degrade.
      return null;
    }
  }
  initPromise = doInitialize(tmpBase);
  try {
    return await initPromise;
  } catch (err) {
    tmuxSocketHooks.onDebug(`socket dir setup failed (${(err as Error).message}); not overriding TMUX_TMPDIR`);
    return null;
  } finally {
    initPromise = null;
  }
}

/**
 * Compute the bash environment overrides for a tmux-using command: ensure the
 * AMI-managed socket dir exists and point `TMUX_TMPDIR` at it. Best-effort — a
 * failed/unavailable setup resolves to `{}` (bash runs un-isolated) instead of
 * rejecting, so tmux isolation can never fail a command.
 */
export async function prepareBashTmuxOverrides(tmpBase: string): Promise<Record<string, string>> {
  const dir = await ensureTmuxIsolationDir(tmpBase);
  return dir ? { TMUX_TMPDIR: dir } : {};
}

/** Reset cached isolation state. Test-only seam (mirrors `__resetPlatformCacheForTest`). */
export function __resetTmuxSocketStateForTest(): void {
  availabilityChecked = false;
  tmuxAvailable = false;
  initPromise = null;
  teardownRegistered = false;
}
