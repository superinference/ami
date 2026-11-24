// Split from config.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { getErrnoCode } from './utils/errno';

/**
 * Write-stability window (ms): when a watched config file is observed PRESENT but
 * momentarily unparseable (the classic half-written / truncate-then-append save),
 * the reload is retried once after this delay instead of silently dropping the
 * change. Parity: the reference's changeDetector `FILE_STABILITY_THRESHOLD_MS` /
 * chokidar `awaitWriteFinish` stabilityThreshold.
 */
export const CONFIG_FILE_STABILITY_THRESHOLD_MS = 1000;

/**
 * Deletion grace window (ms): a vanished config file is not acted on immediately —
 * the common atomic delete-then-recreate pattern (another session's tmp+rename,
 * an auto-updater) is coalesced into a single change when the file reappears within
 * this window; only a deletion that OUTLASTS it reverts the in-memory project layer
 * to defaults. Parity: the reference's `DELETION_GRACE_MS`.
 */
export const CONFIG_DELETION_GRACE_MS = 1000;

/**
 * Fallback reconcile-poll interval (ms). `fs.watch` is known to silently miss
 * events (network filesystems, some Linux configurations, editors that save via
 * atomic rename), so a low-frequency unref'd poll re-reads the file to pick up a
 * dropped change event. Parity: the reference's `FALLBACK_POLL_MS`.
 */
export const CONFIG_FALLBACK_POLL_MS = 5000;

/**
 * Retry interval (ms) for (re-)establishing the watcher when the config directory
 * does not exist yet or the underlying watcher errored — instead of giving up
 * permanently, so a project whose `.ami/` appears mid-session (or a transient
 * inotify error) still ends up with live config reload. Parity: the reference's `#rewatch`.
 */
export const CONFIG_WATCH_RETRY_MS = 1000;

/**
 * Managed/policy-tier reconcile-poll interval (ms). The managed-settings file
 * ({@link getManagedConfigPath}) frequently lives on a path `fs.watch` cannot
 * reliably observe — a root-owned `/etc` entry, an MDM-pushed network/mount path,
 * or a `config.d/` drop-in dir — so when managed-tier watching is enabled a
 * dedicated low-frequency unref'd poll re-reads the merged managed layer and fires
 * {@link ConfigService.onConfigChange} `source: 'managed'` ONLY when the serialized
 * policy actually changed. Deliberately slower than the project/global reconcile
 * poll: enterprise policy changes are rare and the read may hit a slow mount.
 * Parity: the reference's remote/MDM managed-settings `POLLING_INTERVAL_MS` background
 * refresh that fails open (a failed read is swallowed, never fail-closed mid-session).
 */
export const CONFIG_MANAGED_POLL_MS = 60000;

/**
 * Whether a watcher `error` event is a per-path PERMISSION error (`EACCES` / `EPERM`)
 * — an unreadable entry in the watched directory (a root-owned drop-in, a sibling file
 * whose mode was tightened) rather than a fault in the watch handle itself. Such an
 * error must NOT tear the whole watcher down: the rest of the directory is still
 * observable, so reacting by closing + re-establishing would churn the watcher (and,
 * on a persistently-unreadable sibling, loop) while dropping live reload for every
 * other file. A non-permission error (inotify exhaustion, a genuinely dead handle) is
 * still treated as fatal and re-established. Parity: the reference's changeDetector
 * chokidar `ignorePermissionErrors: true`.
 */
export function isWatcherPermissionError(err: unknown): boolean {
  const code = getErrnoCode(err);
  return code === 'EACCES' || code === 'EPERM';
}

/**
 * Injectable clock for lock-contention logging in {@link updateConfigFile}.
 * Production uses `Date.now`; tests advance the clock past the 100ms threshold
 * without sleeping so the contention log arm is deterministic.
 */
export const configClockHooks: { now: () => number } = {
  now: () => Date.now(),
};

export interface ConfigServiceOptions {
  /**
   * Target env object the project config's `env` block is re-applied to when the
   * watched file changes at runtime (defaults to `process.env`). Injectable so a
   * test can observe the take-effect-immediately behavior without mutating the
   * real process environment.
   */
  env?: NodeJS.ProcessEnv;
  /** Override the watcher debounce window (ms). Test seam. */
  debounceMs?: number;
  /** Override the write-stability retry window (ms). Test seam. */
  stabilityThresholdMs?: number;
  /** Override the deletion grace window (ms). Test seam. */
  deletionGraceMs?: number;
  /** Override the fallback reconcile-poll interval (ms); `<= 0` disables it. Test seam. */
  fallbackPollMs?: number;
  /** Override the watcher (re-)establish retry interval (ms). Test seam. */
  watchRetryMs?: number;
  /**
   * Also hot-reload the GLOBAL (`~/.ami/config.json`) config tier, not just the
   * project tier. When enabled, a runtime edit to the user's global config fires the
   * {@link ConfigService.onConfigChange} callbacks with `source: 'global'` (vs the
   * project tier's `'project'`), so a consumer can tell WHICH tier changed and the
   * engine re-applies the full waterfall (env / permissions / managed) on a global
   * edit too. Defaults off so a plain `new ConfigService(cwd)` keeps the
   * project-only behavior. Parity: the reference's `useSettingsChange` watching every
   * SettingSource and threading the real `source` tier through `onChange`.
   */
  watchGlobal?: boolean;
  /**
   * Also hot-reload the MANAGED/policy config tier ({@link getManagedConfigPath} +
   * its `config.d/` drop-ins) mid-session. When enabled, a change to the merged
   * managed policy fires the {@link ConfigService.onConfigChange} callbacks with
   * `source: 'managed'`, so an administrator pushing an updated enterprise policy
   * (or an MDM sync) takes effect without a restart. Backed by a dedicated poll
   * ({@link CONFIG_MANAGED_POLL_MS}) PLUS an fs.watch on the managed base dir, and
   * fails OPEN — a transient/tampered read during the refresh is swallowed, never
   * fail-closed mid-session. Defaults off so a plain `new ConfigService(cwd)` keeps
   * the project-only behavior. Parity: the reference's background managed/policy
   * settings refresh + `notifyChange('policySettings')`.
   */
  watchManaged?: boolean;
  /** Override the managed-tier reconcile-poll interval (ms); `<= 0` disables it. Test seam. */
  managedPollMs?: number;
}
