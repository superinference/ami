/**
 * Best-effort process hardening that reduces exfiltration of in-memory secrets
 * (provider API keys / session tokens held live on the agent heap for the whole
 * session) by a SAME-UID attacker — e.g. a prompt-injected `bash` command the
 * model is tricked into running, which executes as the same UID and could read a
 * core dump of the agent process to scrape those secrets.
 *
 * This is the pure-Node port of the defense-in-depth idea behind the reference's
 * `setNonDumpable()` / `prctl(PR_SET_DUMPABLE, 0)` hardening. It applies TWO
 * complementary, independent, fail-open measures:
 *
 * 1. **Core-dump filter** (always reachable in pure Node on Linux): clearing the
 *    per-process core-dump page mask at `/proc/self/coredump_filter` so anonymous/
 *    private heap pages (where the secrets live) are EXCLUDED from any core dump
 *    the kernel writes for this process. That removes the "trigger a crash, then
 *    scrape the heap out of the core file" vector, which is the exfiltration path a
 *    sandboxed-but-same-UID bash command can most easily reach.
 * 2. **Live-ptrace block** via `prctl(PR_SET_DUMPABLE, 0)` (the reference's exact
 *    `setNonDumpable()` call): this ALSO blocks a same-UID `gdb -p $PPID` /
 *    `/proc/<pid>/mem` read of the running heap, not just the post-crash core file.
 *    Node has no built-in `prctl`, so this measure is only reachable when the
 *    runtime exposes a libc FFI (a Bun-style `bun:ffi`); on plain Node it is a
 *    documented no-op (`no-ffi`) and the core-dump filter above still applies.
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

/** `prctl(2)` option number for `PR_SET_DUMPABLE` (uapi `linux/prctl.h`). Setting
 *  it to 0 marks the process non-dumpable: no core dump AND no same-UID ptrace of
 *  the live process (`gdb -p`, `/proc/<pid>/mem`). */
export const PR_SET_DUMPABLE = 4;

/** What a hardening attempt did.
 *  - `applied`      — the core-dump filter was cleared (heap excluded from dumps).
 *  - `already`      — the filter was already fully cleared (no-op, still hardened).
 *  - `disabled`     — the opt-out env var was set.
 *  - `unsupported`  — not Linux / no `/proc/self/coredump_filter` (fail-open).
 *  - `failed`       — Linux + filter present but the read/write failed (fail-open). */
export const PROCESS_HARDENING_STATUS = ['applied', 'already', 'disabled', 'unsupported', 'failed'] as const;
export type ProcessHardeningStatus = (typeof PROCESS_HARDENING_STATUS)[number];

/** Result of the core-dump-filter measure (measure 1). */
export interface CoreDumpHardeningResult {
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

/** What the live-ptrace block (measure 2) did.
 *  - `applied`     — `prctl(PR_SET_DUMPABLE,0)` returned 0 (live ptrace blocked).
 *  - `disabled`    — the opt-out env var was set.
 *  - `unsupported` — not Linux (PR_SET_DUMPABLE is Linux-only; fail-open).
 *  - `no-ffi`      — Linux but the runtime has no libc FFI (plain Node); fail-open.
 *  - `failed`      — prctl was called but returned nonzero; fail-open.
 *  - `error`       — the FFI call threw; fail-open. */
export const PTRACE_HARDENING_STATUS = ['applied', 'disabled', 'unsupported', 'no-ffi', 'failed', 'error'] as const;
export type PtraceHardeningStatus = (typeof PTRACE_HARDENING_STATUS)[number];

/** Result of the `prctl(PR_SET_DUMPABLE,0)` live-ptrace block (measure 2). */
export interface PtraceHardeningResult {
  status: PtraceHardeningStatus;
  platform: NodeJS.Platform;
  method: 'prctl-set-dumpable';
  /** Human-readable reason, suitable for a debug log. */
  detail: string;
}

/** Combined result: the core-dump-filter measure PLUS the live-ptrace block. The
 *  top-level fields describe measure 1 (preserved for back-compat with callers that
 *  only read the core-dump status); {@link ProcessHardeningResult.ptrace} carries
 *  measure 2. */
export interface ProcessHardeningResult extends CoreDumpHardeningResult {
  /** The live-ptrace block (`prctl(PR_SET_DUMPABLE,0)`) outcome. */
  ptrace: PtraceHardeningResult;
}

/** A `prctl(2)` invoker: `(option, arg2) => returnCode`. */
export type PrctlFn = (option: number, arg2: bigint) => number;

/** Injectable dependencies — default to the real platform/fs/env so production
 *  callers pass nothing and tests inject fakes (no real `/proc` mutation, no real
 *  FFI). */
export interface ProcessHardeningDeps {
  platform?: NodeJS.Platform;
  env?: NodeJS.ProcessEnv;
  readFileSync?: (path: string, encoding: 'utf8') => string;
  writeFileSync?: (path: string, data: string) => void;
  log?: (message: string) => void;
  /**
   * prctl invoker for the live-ptrace block. `undefined` → use {@link loadLibcPrctl}
   * (which is itself `null` on runtimes without libc FFI, e.g. plain Node).
   * `null` → force the "no FFI" fail-open path. A function → call it directly
   * (tests inject a fake so no real syscall happens).
   */
  prctl?: PrctlFn | null;
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
 * Default FFI module loader for {@link loadLibcPrctl}. Uses the runtime's global
 * `require` (present on Bun, which can `require('bun:ffi')`). Throws when no such
 * require exists — plain Node ESM — so {@link loadLibcPrctl} fails open to `null`.
 * Extracted so tests can exercise both the present/absent-require branches without
 * a real FFI runtime.
 */
export function bunFfiRequire(id: string): unknown {
  const req = (globalThis as { require?: (id: string) => unknown }).require;
  if (typeof req !== 'function') throw new Error('no runtime require available for FFI');
  return req(id);
}

/**
 * Best-effort loader for a `prctl(2)` invoker backed by libc via the runtime's FFI.
 * Only runtimes that expose a Bun-style `bun:ffi` (`typeof Bun !== 'undefined'`)
 * can do this; plain Node has no built-in FFI and yields `null`, so the caller
 * fails open. ANY loader error (no `bun:ffi`, `dlopen` failure) also yields `null`
 * — this is strictly best-effort defense-in-depth and must never throw. Deps are
 * injectable so tests can cover the FFI path without a real Bun runtime.
 */
export function loadLibcPrctl(
  deps: { hasFfi?: boolean; ffiRequire?: (id: string) => unknown } = {},
): PrctlFn | null {
  const hasFfi = deps.hasFfi ?? typeof (globalThis as { Bun?: unknown }).Bun !== 'undefined';
  if (!hasFfi) return null;
  try {
    const req = deps.ffiRequire ?? bunFfiRequire;
    const ffi = req('bun:ffi') as {
      dlopen: (
        path: string,
        symbols: Record<string, unknown>,
      ) => { symbols: { prctl: (...args: Array<number | bigint>) => number } };
    };
    const lib = ffi.dlopen('libc.so.6', {
      prctl: { args: ['int', 'u64', 'u64', 'u64', 'u64'], returns: 'int' },
    });
    return (option, arg2) => lib.symbols.prctl(option, arg2, 0n, 0n, 0n);
  } catch {
    return null;
  }
}

/**
 * Measure 2: block live same-UID ptrace of this process via
 * `prctl(PR_SET_DUMPABLE, 0)` — the reference's exact `setNonDumpable()` call. On a
 * runtime with libc FFI this stops a prompt-injected `gdb -p $PPID` / `/proc/<pid>/
 * mem` read from scraping provider keys out of the live heap. NEVER throws: a
 * non-Linux platform, an opt-out, a runtime without FFI, a nonzero prctl return, or
 * an FFI error all collapse to a fail-open {@link PtraceHardeningResult}.
 */
export function blockPtrace(
  deps: Pick<ProcessHardeningDeps, 'platform' | 'env' | 'log' | 'prctl'> = {},
): PtraceHardeningResult {
  const platform = deps.platform ?? process.platform;
  const env = deps.env ?? process.env;
  const log = deps.log ?? (() => {});
  const method = 'prctl-set-dumpable' as const;

  if (processHardeningDisabled(env)) {
    const detail = `disabled via ${PROCESS_HARDENING_DISABLE_ENV}`;
    log(`ptrace-hardening: ${detail}`);
    return { status: 'disabled', platform, method, detail };
  }
  if (platform !== 'linux') {
    const detail = `no-op on ${platform} (PR_SET_DUMPABLE is Linux-only); failing open`;
    log(`ptrace-hardening: ${detail}`);
    return { status: 'unsupported', platform, method, detail };
  }
  const prctl = deps.prctl === undefined ? loadLibcPrctl() : deps.prctl;
  if (!prctl) {
    const detail = 'no libc FFI available (plain Node has no prctl); failing open';
    log(`ptrace-hardening: ${detail}`);
    return { status: 'no-ffi', platform, method, detail };
  }
  try {
    const rc = prctl(PR_SET_DUMPABLE, 0n);
    if (rc === 0) {
      const detail = 'prctl(PR_SET_DUMPABLE,0): live same-UID ptrace/gdb/proc-mem reads blocked';
      log(`ptrace-hardening: ${detail}`);
      return { status: 'applied', platform, method, detail };
    }
    const detail = `prctl(PR_SET_DUMPABLE,0) returned ${rc}; failing open`;
    log(`ptrace-hardening: ${detail}`);
    return { status: 'failed', platform, method, detail };
  } catch (err) {
    const detail = `prctl unavailable (${err instanceof Error ? err.message : String(err)}); failing open`;
    log(`ptrace-hardening: ${detail}`);
    return { status: 'error', platform, method, detail };
  }
}

/**
 * Attempt best-effort process hardening — BOTH the core-dump filter (measure 1) and
 * the live-ptrace block (measure 2). Synchronous and NEVER throws: every failure
 * path on either measure collapses to a fail-open result and an optional debug log.
 */
export function hardenProcess(deps: ProcessHardeningDeps = {}): ProcessHardeningResult {
  const coredump = hardenCoreDumpFilter(deps);
  const ptrace = blockPtrace(deps);
  return { ...coredump, ptrace };
}

/**
 * Measure 1: clear the per-process core-dump page mask so heap pages are excluded
 * from any core dump. Synchronous (it only touches a tiny pseudo-file) and NEVER
 * throws — all failure paths collapse to a fail-open {@link CoreDumpHardeningResult}
 * and an optional debug log.
 */
function hardenCoreDumpFilter(deps: ProcessHardeningDeps = {}): CoreDumpHardeningResult {
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
  /** Short note on scope/limits of the core-dump-filter measure. */
  note: string;
  /** The live-ptrace block (measure 2) capability on this platform/runtime. */
  ptrace: {
    method: 'prctl-set-dumpable';
    /** `prctl(2)` option number applied (PR_SET_DUMPABLE). */
    prctlOption: number;
    /** True when this platform could apply it (Linux). */
    supported: boolean;
    /** True when the runtime exposes a libc FFI able to call prctl (Bun-style). */
    ffiAvailable: boolean;
    /** True when it would actually apply right now (supported + FFI + not opted out). */
    active: boolean;
    note: string;
  };
} {
  const platform = deps.platform ?? process.platform;
  const env = deps.env ?? process.env;
  const supported = platform === 'linux';
  const disabled = processHardeningDisabled(env);
  const ffiAvailable = loadLibcPrctl() !== null;
  const ptraceActive = supported && ffiAvailable && !disabled;
  return {
    method: 'coredump-filter',
    disableEnv: PROCESS_HARDENING_DISABLE_ENV,
    disabled,
    platform,
    supported,
    coredumpFilterPath: COREDUMP_FILTER_PATH,
    note: supported
      ? 'clears /proc/self/coredump_filter so heap pages (holding API keys/session tokens) are excluded from core dumps'
      : 'core-dump-filter hardening is Linux-only; no-op (fail-open) on this platform',
    ptrace: {
      method: 'prctl-set-dumpable',
      prctlOption: PR_SET_DUMPABLE,
      supported,
      ffiAvailable,
      active: ptraceActive,
      note: !supported
        ? 'prctl(PR_SET_DUMPABLE,0) ptrace block is Linux-only; no-op (fail-open) on this platform'
        : ffiAvailable
          ? 'prctl(PR_SET_DUMPABLE,0) blocks live same-UID ptrace/gdb/proc-mem reads of the heap (the reference setNonDumpable parity)'
          : 'prctl(PR_SET_DUMPABLE,0) needs a libc FFI (Bun-style); plain Node has none, so this measure is a no-op (fail-open) and only the core-dump filter applies',
    },
  };
}
