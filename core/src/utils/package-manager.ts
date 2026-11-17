/**
 * Python package-manager detection + install-command construction.
 *
 * Parity with Claude Code's `detectPythonPackageManager` / `installIt2`
 * (utils/swarm/backends/it2Setup.ts): when an external Python-based CLI tool
 * (e.g. a pip/uv-installable language server) is missing, AMI can tell the user
 * HOW to bootstrap it instead of only reporting "not found".
 *
 * Two things this adds over AMI's existing single-`which` probe:
 *   1. an ORDERED preference probe — uv (isolated envs) > pipx (isolated) > pip —
 *      so the best available installer wins; and
 *   2. ALIAS COLLAPSE — `pip3` is probed as a last resort but reported as the
 *      logical `'pip'` kind, so downstream install logic stays uniform.
 *
 * The ordering itself reuses the shared {@link firstAvailableBinary} resolver
 * (core's single canonical "first installed from an ordered list" primitive) —
 * this module does NOT re-implement a second `which` loop. It is pure/decision
 * only: it NEVER executes an install (the returned command is data a caller can
 * surface or run under its own permission/supply-chain gate).
 */
import { firstAvailableBinary, isBinaryInstalled } from './which';

/**
 * Logical Python package-manager kind, with aliases collapsed. Listed in order
 * of preference. `'uvx'` denotes `uv` (the type name is kept for Claude-Code
 * schema compatibility); `'pip'` covers both `pip` and `pip3`.
 */
export type PythonPackageManager = 'uvx' | 'pipx' | 'pip';

/**
 * Ordered probe table: the concrete binary to look for on PATH → the logical
 * package-manager kind it maps to. Preference order top-to-bottom; `pip3`
 * collapses to the same `'pip'` logical kind as `pip`.
 */
const PYTHON_PM_PROBES: ReadonlyArray<{ binary: string; kind: PythonPackageManager }> = [
  { binary: 'uv', kind: 'uvx' },
  { binary: 'pipx', kind: 'pipx' },
  { binary: 'pip', kind: 'pip' },
  { binary: 'pip3', kind: 'pip' },
];

/**
 * Detect the best available Python package manager, probing `uv → pipx → pip →
 * pip3` in preference order and collapsing `pip3` to the logical `'pip'` kind.
 * Returns `null` when none are installed.
 *
 * @param isInstalled injectable binary-presence probe (defaults to the shared,
 *   session-cached {@link isBinaryInstalled}); lets a caller/test drive detection
 *   deterministically, sharing the ONE ordering primitive instead of duplicating it.
 */
export function detectPythonPackageManager(
  isInstalled: (command: string) => boolean = isBinaryInstalled,
): PythonPackageManager | null {
  const binary = firstAvailableBinary(
    PYTHON_PM_PROBES.map((p) => p.binary),
    isInstalled,
  );
  if (!binary) return null;
  // `firstAvailableBinary` returns the first matching entry in the SAME order, so
  // the first probe whose binary matches is the correct logical kind.
  return PYTHON_PM_PROBES.find((p) => p.binary === binary)!.kind;
}

/**
 * Build the spawn-ready install invocation for `tool` using `packageManager`.
 * Pure data — this does NOT execute anything; the caller decides whether/how to
 * run it (under its own permission + supply-chain gate). Mirrors Claude Code's
 * `installIt2` command table:
 *   - uv:   `uv tool install <tool>`   (isolated global install)
 *   - pipx: `pipx install <tool>`      (isolated global install)
 *   - pip:  `pip install --user <tool>` (no-sudo user install)
 */
export function pythonInstallCommand(
  tool: string,
  packageManager: PythonPackageManager,
): { cmd: string; args: string[] } {
  switch (packageManager) {
    case 'uvx':
      return { cmd: 'uv', args: ['tool', 'install', tool] };
    case 'pipx':
      return { cmd: 'pipx', args: ['install', tool] };
    case 'pip':
      return { cmd: 'pip', args: ['install', '--user', tool] };
  }
}

/**
 * Human-readable bootstrap affordance for a missing Python-installable `tool`:
 * detect the best package manager and describe the exact command to install it,
 * or — when no Python package manager is available — say so. Suitable for
 * appending to a "command not found" diagnostic. Never executes anything.
 *
 * @param isInstalled injectable probe, forwarded to {@link detectPythonPackageManager}.
 */
export function pythonBootstrapHint(
  tool: string,
  isInstalled: (command: string) => boolean = isBinaryInstalled,
): string {
  const pm = detectPythonPackageManager(isInstalled);
  if (!pm) {
    return `No Python package manager (uv/pipx/pip) found to install "${tool}".`;
  }
  const { cmd, args } = pythonInstallCommand(tool, pm);
  return `Install it with: ${cmd} ${args.join(' ')}`;
}
