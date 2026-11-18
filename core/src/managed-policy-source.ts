import { execFileSync } from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import { log as coreLog } from './logger';
import { escapeRegExp } from './utils/string';

/**
 * OS-NATIVE managed-policy sources (macOS configuration profiles / Windows Group
 * Policy). This complements the file-based managed config
 * (`/Library/Managed Preferences/.../config.json`, `C:\ProgramData\...`,
 * `/etc/superinference/config.json`) resolved in {@link module:config} by reading the
 * SAME policy an MDM/Group-Policy deployment pushes through the native OS stores an
 * end user cannot redirect:
 *
 *  - **macOS** → a managed `.plist` under `/Library/Managed Preferences/` (per-user
 *    then device-level), converted to JSON with `plutil -convert json -o -`.
 *  - **Windows** → the `Settings` REG_SZ/REG_EXPAND_SZ value under
 *    `HKLM\SOFTWARE\Policies\SuperInference` (device, highest) then
 *    `HKCU\SOFTWARE\Policies\SuperInference` (per-user). These live under
 *    `SOFTWARE\Policies`, which is on the WOW64 shared-key list so 32- and 64-bit
 *    processes read the same values without WOW6432Node redirection.
 *  - **Linux / everything else** → no native policy store; returns `null`.
 *
 * Every read is TIMEOUT-BOUNDED ({@link MDM_SUBPROCESS_TIMEOUT_MS}) and NEVER throws
 * (a hung, missing, or malformed policy source yields `null`, not an exception), so a
 * broken enterprise policy source can never stall or crash startup. Parity: Claude
 * Code's `utils/settings/mdm/{constants,rawRead,settings}.ts` native-MDM acquisition.
 */

/**
 * macOS managed-preference domain (the `.plist` basename under
 * `/Library/Managed Preferences/`). Mirrors AMI's `SuperInference` managed naming.
 */
export const MACOS_MANAGED_PREFERENCE_DOMAIN = 'org.superinference.ami';

/**
 * Windows registry key path for DEVICE-level (admin) managed policy. Under
 * `SOFTWARE\Policies` (WOW64 shared) so it is never redirected to WOW6432Node.
 */
export const WINDOWS_REGISTRY_KEY_PATH_HKLM = 'HKLM\\SOFTWARE\\Policies\\SuperInference';

/** Windows registry key path for PER-USER managed policy. */
export const WINDOWS_REGISTRY_KEY_PATH_HKCU = 'HKCU\\SOFTWARE\\Policies\\SuperInference';

/** Registry value (under either key) that holds the JSON settings blob. */
export const WINDOWS_REGISTRY_VALUE_NAME = 'Settings';

/** Absolute path to the macOS `plutil` binary (fixed system location). */
export const PLUTIL_PATH = '/usr/bin/plutil';

/** `plutil` args that emit a plist as JSON on stdout; the plist path is appended. */
export const PLUTIL_ARGS_PREFIX: readonly string[] = ['-convert', 'json', '-o', '-', '--'];

/**
 * Hard upper bound on any native-policy subprocess. A hung policy source (e.g. a
 * wedged `reg`/`plutil`) can never stall startup past this. Parity: Claude's
 * `MDM_SUBPROCESS_TIMEOUT_MS`.
 */
export const MDM_SUBPROCESS_TIMEOUT_MS = 5000;

/** A successfully-acquired native managed policy: the raw JSON + a diagnostic label. */
export interface NativeManagedPolicy {
  /** Raw JSON text extracted from the native policy store. */
  raw: string;
  /** Human-readable provenance (plist path / registry key) for diagnostics. */
  source: string;
}

/**
 * Mockable subprocess + fs seam (ESM-safe). Tests swap `execFileSync`/`existsSync`
 * via `mock.method(execHooks, 'name', ...)` to drive the darwin/win32 branches and
 * the timeout/error arms deterministically without a real `plutil`/`reg`. Defaults
 * to the real implementations; production behavior is unchanged.
 */
export const execHooks = {
  execFileSync: execFileSync as typeof execFileSync,
  existsSync: fs.existsSync as typeof fs.existsSync,
};

/**
 * macOS managed `.plist` candidate paths, highest-priority first: the per-user
 * managed-preferences plist (when a username resolves) then the device-level plist.
 * Parity: Claude's `getMacOSPlistPaths()` per-user > device ordering.
 */
export function getMacOSManagedPlistPaths(): Array<{ path: string; label: string }> {
  const paths: Array<{ path: string; label: string }> = [];
  let username = '';
  try {
    username = os.userInfo().username;
  } catch {
    // os.userInfo() throws in some uid-less sandboxes — fall through to device-level.
  }
  if (username) {
    paths.push({
      path: `/Library/Managed Preferences/${username}/${MACOS_MANAGED_PREFERENCE_DOMAIN}.plist`,
      label: 'per-user managed preferences',
    });
  }
  paths.push({
    path: `/Library/Managed Preferences/${MACOS_MANAGED_PREFERENCE_DOMAIN}.plist`,
    label: 'device managed preferences',
  });
  return paths;
}

/**
 * Extract the JSON string from `reg query` stdout for a given value name, matching
 * both `REG_SZ` and `REG_EXPAND_SZ` (case-insensitive). Expected line shape:
 *
 *     Settings    REG_SZ    {"json":"value"}
 *
 * Returns the trimmed value, or `null` when the value is absent. Pure + exported for
 * direct unit coverage. Parity: Claude's `parseRegQueryStdout`.
 */
export function parseRegQueryStdout(stdout: string, valueName: string = WINDOWS_REGISTRY_VALUE_NAME): string | null {
  const escaped = escapeRegExp(valueName);
  const re = new RegExp(`^\\s+${escaped}\\s+REG_(?:EXPAND_)?SZ\\s+(.*)$`, 'i');
  for (const line of stdout.split(/\r?\n/)) {
    const match = line.match(re);
    if (match && match[1]) return match[1].trimEnd();
  }
  return null;
}

/**
 * Run a subprocess, bounded + fail-safe: returns the RAW stdout (untrimmed — the
 * leading-whitespace layout of `reg query` output is load-bearing for
 * {@link parseRegQueryStdout}), or `null` on any error/timeout/non-zero exit or empty
 * output. Callers that expect plain JSON (plutil) trim the result themselves.
 */
function runBounded(cmd: string, args: readonly string[]): string | null {
  try {
    const out = execHooks.execFileSync(cmd, args as string[], {
      encoding: 'utf-8',
      timeout: MDM_SUBPROCESS_TIMEOUT_MS,
      stdio: ['ignore', 'pipe', 'ignore'],
    }) as string;
    return typeof out === 'string' && out.trim().length > 0 ? out : null;
  } catch {
    // A missing binary, a non-zero exit (e.g. `reg query` on an absent key), or a
    // timeout must all degrade to "no native policy", never propagate.
    return null;
  }
}

/**
 * Acquire the OS-native managed policy for the current platform, highest-priority
 * first, or `null` when none is present. Sync, timeout-bounded, and NEVER-throwing —
 * see the module doc. On success the raw JSON + a provenance label is returned for the
 * caller ({@link module:config} `loadManagedConfig`) to parse + salvage like any other
 * managed layer.
 */
export function readNativeManagedPolicy(): NativeManagedPolicy | null {
  if (process.platform === 'darwin') {
    for (const { path: plistPath, label } of getMacOSManagedPlistPaths()) {
      // Fast-path: a non-MDM machine never has these files; skip the ~5ms spawn.
      if (!execHooks.existsSync(plistPath)) continue;
      const stdout = runBounded(PLUTIL_PATH, [...PLUTIL_ARGS_PREFIX, plistPath]);
      if (stdout) {
        coreLog('config', `read native managed policy from ${label} (${plistPath})`);
        return { raw: stdout.trim(), source: `${label} (${plistPath})` };
      }
    }
    return null;
  }
  if (process.platform === 'win32') {
    for (const keyPath of [WINDOWS_REGISTRY_KEY_PATH_HKLM, WINDOWS_REGISTRY_KEY_PATH_HKCU]) {
      const stdout = runBounded('reg', ['query', keyPath, '/v', WINDOWS_REGISTRY_VALUE_NAME]);
      if (!stdout) continue;
      const json = parseRegQueryStdout(stdout, WINDOWS_REGISTRY_VALUE_NAME);
      if (json) {
        coreLog('config', `read native managed policy from ${keyPath}\\${WINDOWS_REGISTRY_VALUE_NAME}`);
        return { raw: json, source: `${keyPath}\\${WINDOWS_REGISTRY_VALUE_NAME}` };
      }
    }
    return null;
  }
  // Linux / other: no native OS policy store.
  return null;
}
