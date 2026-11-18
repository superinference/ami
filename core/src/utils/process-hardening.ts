/**
 * Best-effort process hardening that reduces exfiltration of in-memory secrets
 * (provider API keys / session tokens held live on the agent heap for the whole
 * session) by a SAME-UID attacker — e.g. a prompt-injected `bash` command the
 * model is tricked into running, which executes as the same UID and could read a
 * core dump of the agent process to scrape those secrets.
 *
 * This is the pure-Node port of the defense-in-depth idea behind Claude Code's
 * `setNonDumpable()` / `prctl(PR_SET_DUMPABLE, 0)` hardening. Node has no `prctl`
 * binding, so the FULL `PR_SET_DUMPABLE(0)` behaviour (which also blocks live
 * same-UID `ptrace`/`gdb -p` and `/proc/<pid>/mem` reads) is NOT reachable without
 * a native addon — intentionally out of scope here. What IS reachable in pure Node
 * on Linux is clearing the per-process core-dump filter at
 * `/proc/self/coredump_filter` so that anonymous/private heap pages (where the
 * secrets live) are EXCLUDED from any core dump the kernel writes for this process.
 * That removes the "trigger a crash, then scrape the heap out of the core file"
 * vector, which is the exfiltration path a sandboxed-but-same-UID bash command can
 * most easily reach.
 *
 * It FAILS OPEN everywhere: a non-Linux platform, a missing `/proc`, an unreadable
 * or unwritable filter file, or an explicit opt-out are never errors — hardening is
 * strictly a bonus and must never break or slow startup. The structured result is
 * surfaced read-only via `ami --introspect` (see {@link describeProcessHardening}).
 */

import * as fs from 'fs';

/** Opt-out env var: set truthy to SKIP best-effort process hardening (e.g. when a
 *  developer needs full core dumps for a crash investigation). Default (unset) →
 *  hardening is attempted on Linux. */
export const PROCESS_HARDENING_DISABLE_ENV = 'AMI_DISABLE_PROCESS_HARDENING';

/** The Linux per-process core-dump page-mask pseudo-file. Writing `0` excludes
 *  every page class (anonymous private heap included) from any core dump. */
export const COREDUMP_FILTER_PATH = '/proc/self/coredump_filter';

/** What a hardening attempt did.
 *  - `applied`      — the core-dump filter was cleared (heap excluded from dumps).
 *  - `already`      — the filter was already fully cleared (no-op, still hardened).
 *  - `disabled`     — the opt-out env var was set.
 *  - `unsupported`  — not Linux / no `/proc/self/coredump_filter` (fail-open).
 *  - `failed`       — Linux + filter present but the read/write failed (fail-open). */
export type ProcessHardeningStatus =
  | 'applied'
  | 'already'
  | 'disabled'
  | 'unsupported'
  | 'failed';

export interface ProcessHardeningResult {
  status: ProcessHardeningStatus;
  platform: NodeJS.Platform;
  method: 'coredump-filter';
  /** Human-readable reason, suitable for a debug log. */
  detail: string;
  /** Raw filter value before the attempt (hex string), when it could be read. */
  coredumpFilterBefore?: string;
  /** Raw filter value written, when a write was attempted. */
  coredumpFilterAfter?: string;
}

/** Injectable dependencies — default to the real platform/fs/env so production
 *  callers pass nothing and tests inject fakes (no real `/proc` mutation). */
export interface ProcessHardeningDeps {
  platform?: NodeJS.Platform;
  env?: NodeJS.ProcessEnv;
  readFileSync?: (path: string, encoding: 'utf8') => string;
  writeFileSync?: (path: string, data: string) => void;
  log?: (message: string) => void;
}

/** Value written to fully exclude all page classes from a core dump. */
const CLEARED_FILTER = '0';

/** True when the opt-out env var is set to a truthy value. */
export function processHardeningDisabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env[PROCESS_HARDENING_DISABLE_ENV];
  if (typeof raw !== 'string') return false;
  return ['1', 'true', 'on', 'yes', 'enable', 'enabled'].includes(raw.trim().toLowerCase());
}

/**
 * Attempt best-effort process hardening. Synchronous (it only touches a tiny
 * pseudo-file) and NEVER throws — all failure paths collapse to a fail-open
 * {@link ProcessHardeningResult} and an optional debug log.
 */
export function hardenProcess(deps: ProcessHardeningDeps = {}): ProcessHardeningResult {
  const platform = deps.platform ?? process.platform;
  const env = deps.env ?? process.env;
  const log = deps.log ?? (() => {});
  const method = 'coredump-filter' as const;

  if (processHardeningDisabled(env)) {
    const detail = `disabled via ${PROCESS_HARDENING_DISABLE_ENV}`;
    log(`process-hardening: ${detail}`);
    return { status: 'disabled', platform, method, detail };
  }

  if (platform !== 'linux') {
    const detail = `no-op on ${platform} (coredump_filter is Linux-only); failing open`;
    log(`process-hardening: ${detail}`);
    return { status: 'unsupported', platform, method, detail };
  }

  // Default to real fs; tests inject fakes so no real /proc mutation happens.
  const readFileSync = deps.readFileSync ?? ((p: string, enc: 'utf8') => fs.readFileSync(p, enc));
  const writeFileSync = deps.writeFileSync ?? ((p: string, data: string) => fs.writeFileSync(p, data));

  let before: string | undefined;
  try {
    before = readFileSync(COREDUMP_FILTER_PATH, 'utf8').trim();
  } catch {
    const detail = `${COREDUMP_FILTER_PATH} unreadable (no /proc?); failing open`;
    log(`process-hardening: ${detail}`);
    return { status: 'unsupported', platform, method, detail };
  }

  // Already fully cleared (0 in any base) → nothing to do, still hardened.
  if (/^0x?0*$/i.test(before)) {
    const detail = 'core-dump filter already cleared (heap excluded from dumps)';
    return { status: 'already', platform, method, detail, coredumpFilterBefore: before };
  }

  try {
    writeFileSync(COREDUMP_FILTER_PATH, CLEARED_FILTER);
  } catch {
    const detail = `${COREDUMP_FILTER_PATH} unwritable; failing open`;
    log(`process-hardening: ${detail}`);
    return { status: 'failed', platform, method, detail, coredumpFilterBefore: before };
  }

  const detail = 'cleared core-dump filter: heap pages excluded from any core dump';
  log(`process-hardening: ${detail} (was ${before})`);
  return {
    status: 'applied',
    platform,
    method,
    detail,
    coredumpFilterBefore: before,
    coredumpFilterAfter: CLEARED_FILTER,
  };
}

/**
 * Read-only introspection view of the process-hardening capability for the
 * `ami --introspect` manifest. Pure — reports what hardening WOULD do for the
 * current platform/env WITHOUT mutating `/proc` (the `--introspect` path exits
 * before startup hooks run, so it must not apply hardening as a side effect).
 */
export function describeProcessHardening(
  deps: Pick<ProcessHardeningDeps, 'platform' | 'env'> = {},
): {
  method: 'coredump-filter';
  disableEnv: string;
  disabled: boolean;
  platform: NodeJS.Platform;
  /** True when this platform supports the hardening (Linux). */
  supported: boolean;
  /** The `/proc` path the hardening writes on a supported platform. */
  coredumpFilterPath: string;
  /** Short note on scope/limits (ptrace blocking needs a native prctl addon). */
  note: string;
} {
  const platform = deps.platform ?? process.platform;
  const env = deps.env ?? process.env;
  const supported = platform === 'linux';
  return {
    method: 'coredump-filter',
    disableEnv: PROCESS_HARDENING_DISABLE_ENV,
    disabled: processHardeningDisabled(env),
    platform,
    supported,
    coredumpFilterPath: COREDUMP_FILTER_PATH,
    note: supported
      ? 'clears /proc/self/coredump_filter so heap pages (holding API keys/session tokens) are excluded from core dumps; live same-UID ptrace blocking (PR_SET_DUMPABLE) needs a native prctl addon and is not applied (fail-open)'
      : 'core-dump-filter hardening is Linux-only; no-op (fail-open) on this platform',
  };
}
