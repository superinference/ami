/**
 * Python package-manager detection + install-command construction.
 *
 * Parity with the reference's `detectPythonPackageManager` / `installIt2`
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
import { homedir } from 'node:os';

import { firstAvailableBinary, isBinaryInstalled } from './which';

/**
 * Logical Python package-manager kind, with aliases collapsed. Listed in order
 * of preference. `'uvx'` denotes `uv` (the type name is kept for the reference
 * schema compatibility); `'pip'` covers both `pip` and `pip3`.
 */
export const PYTHON_PACKAGE_MANAGERS = ['uvx', 'pipx', 'pip'] as const;
export type PythonPackageManager = (typeof PYTHON_PACKAGE_MANAGERS)[number];

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
 * run it (under its own permission + supply-chain gate). Mirrors the reference's
 * `installIt2` command table:
 *   - uv:   `uv tool install <tool>`   (isolated global install)
 *   - pipx: `pipx install <tool>`      (isolated global install)
 *   - pip:  `pip install --user <tool>` (no-sudo user install)
 *
 * SUPPLY-CHAIN HARDENING: `cwd` is always the user's home directory. uv (`uv.toml`
 * / `pyproject.toml`), pip (`pip.conf`) and pipx all read package-index settings
 * from the CURRENT working directory first, so running an installer inside an
 * untrusted project lets a planted `uv.toml`/`pip.conf` with `index-url =
 * http://attacker/...` silently redirect the download to a malicious PyPI mirror.
 * Pinning the install cwd to `$HOME` defeats that project-level redirection.
 * Parity with the reference's `installIt2`, which spawns every installer with
 * `cwd: homedir()` for exactly this reason (utils/swarm/backends/it2Setup.ts
 * L95-96). The `--user` flag on pip additionally avoids a sudo escalation.
 */
export function pythonInstallCommand(
  tool: string,
  packageManager: PythonPackageManager,
): { cmd: string; args: string[]; cwd: string } {
  const cwd = homedir();
  switch (packageManager) {
    case 'uvx':
      return { cmd: 'uv', args: ['tool', 'install', tool], cwd };
    case 'pipx':
      return { cmd: 'pipx', args: ['install', tool], cwd };
    case 'pip':
      return { cmd: 'pip', args: ['install', '--user', tool], cwd };
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
  // pip → pip3 RUNTIME fallback (parity with installIt2, it2Setup.ts L112-125):
  // `pip` can be present on PATH yet fail to install (e.g. a stale Python-2 `pip`),
  // so the reference transparently retries with `pip3`. AMI never auto-installs, so
  // the retry is surfaced as explicit user guidance rather than executed.
  const pip3Fallback =
    pm === 'pip' ? ` If \`${cmd}\` fails, retry with \`pip3 ${args.join(' ')}\`.` : '';
  // Surface the supply-chain advisory: the install should run from $HOME so a
  // project-local pip.conf/uv.toml cannot redirect the package index (the same
  // hardening pythonInstallCommand bakes into its `cwd`). The user runs this by
  // hand (AMI never auto-installs), so the guidance has to be explicit.
  return `Install it with: ${cmd} ${args.join(' ')} (run it from your home directory so a project-local pip.conf/uv.toml can't redirect the package index).${pip3Fallback}`;
}
