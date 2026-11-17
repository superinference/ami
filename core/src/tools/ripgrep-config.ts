import { execFileSync } from 'child_process';

/**
 * Centralized ripgrep binary resolution shared by the grep and glob tools, plus a
 * cached availability self-test. Mirrors Claude Code's `utils/ripgrep.ts`
 * (getRipgrepConfig / getRipgrepStatus / testRipgrepOnFirstUse), adapted to AMI:
 * AMI ships no bundled ripgrep, so "builtin" means an operator-configured binary
 * path (AMI_RIPGREP_PATH) and "system" means the PATH-resolved `rg`.
 */

/** Env knob: when falsy, never use an operator-configured binary — force system `rg`. */
export const USE_BUILTIN_RIPGREP_ENV = 'USE_BUILTIN_RIPGREP';
/** Env knob: an explicit ripgrep binary path to prefer (the AMI "builtin" slot). */
export const RIPGREP_PATH_ENV = 'AMI_RIPGREP_PATH';

export type RipgrepMode = 'system' | 'builtin' | 'config';

/** A config-level ripgrep override (from `sandbox.ripgrep` in settings): an explicit
 *  binary plus optional extra flags, taking precedence over the env waterfall. */
export interface RipgrepOverride {
  command: string;
  args?: string[];
  /** The argv[0] the child should see, independent of the spawned binary path.
   *  Mirrors Claude Code's `sandbox.ripgrep.argv0`: a multi-call / busybox-style
   *  binary (one executable dispatching on argv[0]) needs argv[0]==='rg' to behave
   *  as ripgrep even when `command` is the wrapper path. Omitted → the OS default
   *  (argv[0] === the spawned path). */
  argv0?: string;
}

export interface RipgrepConfig {
  mode: RipgrepMode;
  /** The command to spawn: either the bare name `rg` or an operator-configured path. */
  command: string;
  /** Extra flags prepended to every invocation (from a config-level override). */
  args?: string[];
  /** Explicit argv[0] for the spawned child (from `sandbox.ripgrep.argv0`), passed
   *  through to the spawn/execFile `argv0` option so a multi-call ripgrep binary is
   *  dispatched correctly. Only ever set alongside a `config`-mode `command`. */
  argv0?: string;
}

/**
 * True unless the env value is an explicit falsy literal ("0"/"false"/"no"/"off",
 * case-insensitive). Unset/empty defaults TRUE, matching Claude Code's
 * USE_BUILTIN_RIPGREP default-on behavior.
 */
function useBuiltinDefaultTrue(raw: string | undefined): boolean {
  const v = (raw ?? '').trim().toLowerCase();
  if (v === '') return true;
  return v !== '0' && v !== 'false' && v !== 'no' && v !== 'off';
}

/**
 * Resolve which ripgrep binary the search tools should spawn.
 *
 * - A config-level `override` (from `sandbox.ripgrep`) with a non-empty `command`
 *   WINS over everything → mode `config`, carrying its optional extra `args` and
 *   optional explicit `argv0` (for multi-call ripgrep binaries).
 * - `USE_BUILTIN_RIPGREP` truthy (default) AND `AMI_RIPGREP_PATH` set → the
 *   configured path, mode `builtin`.
 * - Otherwise → the bare name `rg`, mode `system`.
 *
 * SECURITY: when falling back we return the bare command name `rg` (never a
 * relative path) so the OS resolves it via PATH and a planted `./rg` in the cwd
 * cannot be executed (PATH-hijack protection, mirroring Claude Code).
 */
export function resolveRipgrepConfig(
  env: NodeJS.ProcessEnv = process.env,
  override?: RipgrepOverride,
): RipgrepConfig {
  const overrideCommand = (override?.command ?? '').trim();
  if (overrideCommand) {
    const args = (override?.args ?? []).map((a) => a.trim()).filter((a) => a !== '');
    const argv0 = (override?.argv0 ?? '').trim();
    const config: RipgrepConfig = { mode: 'config', command: overrideCommand };
    if (args.length) config.args = args;
    if (argv0) config.argv0 = argv0;
    return config;
  }
  const useBuiltin = useBuiltinDefaultTrue(env[USE_BUILTIN_RIPGREP_ENV]);
  const configured = (env[RIPGREP_PATH_ENV] ?? '').trim();
  if (useBuiltin && configured) {
    return { mode: 'builtin', command: configured };
  }
  return { mode: 'system', command: 'rg' };
}

/**
 * Indirection seam for the version probe so tests can simulate a working / broken /
 * missing ripgrep without spawning a real binary. Production uses `execFileSync`.
 */
export const ripgrepHooks: { execFileSync: typeof execFileSync } = { execFileSync };

// Cached availability status (singleton), keyed by the resolved command so a config
// change (e.g. AMI_RIPGREP_PATH set at runtime) re-probes instead of serving a stale
// result. Mirrors Claude Code's `ripgrepStatus` singleton + `testRipgrepOnFirstUse`.
let ripgrepStatus: { working: boolean; lastTested: number; config: RipgrepConfig } | null = null;

/** Clear the cached probe result (used by tests and after a config change). */
export function resetRipgrepStatus(): void {
  ripgrepStatus = null;
}

/**
 * Report ripgrep status WITHOUT probing: the resolved mode + binary path and the
 * last cached working flag (`null` when never tested). Mirrors Claude Code's
 * `getRipgrepStatus()` "return config immediately, working if available".
 */
export function getRipgrepStatus(env: NodeJS.ProcessEnv = process.env): {
  mode: RipgrepMode;
  path: string;
  working: boolean | null;
} {
  const config = resolveRipgrepConfig(env);
  const working = ripgrepStatus && ripgrepStatus.config.command === config.command
    ? ripgrepStatus.working
    : null;
  return { mode: config.mode, path: config.command, working };
}

/**
 * Functionally probe ripgrep (`<rg> --version`) and cache the result. A second call
 * returns the cache unless the resolved binary changed. "Working" requires a clean
 * exit AND version output that starts with `ripgrep ` (a wrong binary on PATH that
 * exits 0 but is not ripgrep is reported as not working). Mirrors Claude Code's
 * `testRipgrepOnFirstUse`.
 */
export function probeRipgrep(env: NodeJS.ProcessEnv = process.env): boolean {
  const config = resolveRipgrepConfig(env);
  if (ripgrepStatus && ripgrepStatus.config.command === config.command) {
    return ripgrepStatus.working;
  }
  let working = false;
  try {
    // `encoding: 'utf8'` makes execFileSync return a string (not a Buffer).
    const out = ripgrepHooks.execFileSync(config.command, ['--version'], {
      timeout: 5000,
      encoding: 'utf8',
    });
    working = typeof out === 'string' && out.startsWith('ripgrep ');
  } catch {
    working = false;
  }
  ripgrepStatus = { working, lastTested: Date.now(), config };
  return working;
}
