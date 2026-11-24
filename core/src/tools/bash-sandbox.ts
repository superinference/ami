import { execSync } from 'child_process';
import { release } from 'os';
import 'path';
import '../permissions';
import type { SandboxPolicy } from '../types';
import { getGrantedSandboxHosts, isSandboxHostGranted } from './sandbox-permission-registry';
import { isHostSandboxDisabled, isSandboxDefaultDeny } from './tool-gates';
import { buildSandboxRunExpr, collectGlobPatternPaths, detectWslVersion, getDefaultMemoryMB, isCommandAvailable, isSandboxEncodeCommandEnabled } from './bash-sandbox-sandbox-config';
import type { SandboxConfig, SandboxDependencyCheck, SandboxExemptionOptions, SandboxPlatformSupport, WslVersion } from './bash-sandbox-sandbox-config';
import { buildLoopbackSetup, dedupeHostList, shouldUseSandbox } from './bash-sandbox-host-sandbox-config-from-policy';
import type { HostSandboxEligibilityOptions } from './bash-sandbox-host-sandbox-config-from-policy';
import { buildBareGitRepoGuard, buildConfigWriteGuard, buildCredentialScrub, buildDeniedDomainRules, buildNetworkAllowlist, buildProxyEnv, buildReadDenials, buildReadOnlyMounts, buildSandboxTmpDir, buildSocketDenial, buildUlimits, buildWriteDenials, wrapWithResourceLimits } from './bash-sandbox-build-network-allowlist';
export { BASH_SANDBOX_SHOW_INDICATOR_ENV, SANDBOX_ENCODE_COMMAND_ENV, WSL_VERSIONS, buildSandboxExemptPatterns, buildSandboxRunExpr, detectWslVersion, getDefaultMemoryMB, isSandboxEncodeCommandEnabled, isSandboxIndicatorEnabled, sandboxBashDisplayName } from './bash-sandbox-sandbox-config';
export type { SandboxConfig, SandboxDependencyCheck, SandboxExemptionOptions, SandboxPlatformSupport, WslVersion } from './bash-sandbox-sandbox-config';
export { buildExcludedRules, buildLoopbackSetup, dedupeHostList, hostSandboxConfigFromPolicy, normalizeSandboxFsPath, shouldUseSandbox } from './bash-sandbox-host-sandbox-config-from-policy';
export type { HostSandboxEligibilityOptions } from './bash-sandbox-host-sandbox-config-from-policy';
export { BARE_GIT_REPO_FILES, buildBareGitRepoGuard, buildConfigWriteGuard, buildCredentialScrub, buildDeniedDomainRules, buildNetworkAllowlist, buildProxyEnv, buildReadDenials, buildSandboxTmpDir, buildSocketDenial, buildWriteDenials, isValidProxyPort } from './bash-sandbox-build-network-allowlist';

const DEFAULT_SANDBOX_CONFIG: SandboxConfig = {
  allowNetwork: false,
  readOnlyPaths: ['/usr', '/bin', '/lib', '/lib64', '/etc'],
  writablePaths: ['/tmp'],
  maxMemoryMB: getDefaultMemoryMB(),
  maxProcesses: 8192,
  timeout: 30000,
};

let _unshareAvailable: boolean | null = null;
let _wslVersion: 'none' | 'wsl1' | 'wsl2' | null = null;

/** Seam over `os.release()` so the WSL-generation probe is testable without a
 *  real WSL1/WSL2 kernel (the `os` namespace import is frozen and cannot be
 *  monkeypatched). Behavior-preserving: defaults to the real kernel release. */
export const _sandboxPlatformHooks: { release: () => string } = { release };

/**
 * Memoized WSL generation of the current host. Non-Linux is always `'none'`.
 * Determined from the kernel release string ({@link detectWslVersion}); when a
 * WSL env marker (`WSL_INTEROP`/`WSL_DISTRO_NAME`) is present but the kernel
 * string carries no generation tag, we assume `'wsl2'` (the modern default) so a
 * real WSL2 host is never mis-gated as unsupported WSL1. Reset by
 * {@link resetSandboxCache}.
 */
function currentWslVersion(): WslVersion {
  if (_wslVersion !== null) return _wslVersion;
  if (process.platform !== 'linux') {
    _wslVersion = 'none';
    return _wslVersion;
  }
  let kernel = '';
  try {
    kernel = _sandboxPlatformHooks.release();
  } catch {
    kernel = '';
  }
  let v = detectWslVersion(kernel);
  if (v === 'none' && (process.env.WSL_INTEROP || process.env.WSL_DISTRO_NAME)) {
    v = 'wsl2';
  }
  _wslVersion = v;
  return v;
}

/**
 * PARITY (the reference's `isSupportedPlatform()` with WSL1 special-casing): decide
 * whether the host namespace sandbox is runnable on the given platform + WSL
 * generation. Non-Linux and WSL1 are unsupported, each with a SPECIFIC reason so
 * the diagnostic names the actual blocker instead of a generic unshare failure.
 * Pure (platform + WSL generation are injectable for tests); the defaults read the
 * live host. Consumed by {@link checkSandboxDependencies}.
 */
export function getSandboxPlatformSupport(
  platform: string = process.platform,
  wsl: WslVersion = currentWslVersion(),
): SandboxPlatformSupport {
  if (platform !== 'linux') {
    return {
      supported: false,
      reason:
        `Namespace sandbox requires Linux; current platform is ${platform}. ` +
        'Only resource limits (ulimit) are applied — filesystem and network isolation are NOT enforced.',
    };
  }
  if (wsl === 'wsl1') {
    return {
      supported: false,
      reason:
        'Sandboxing requires WSL2. WSL1 is not supported — it is a syscall-translation layer ' +
        'with no Linux user-namespace support, so filesystem and network isolation cannot be enforced. ' +
        'Upgrade the distro to WSL2 (`wsl --set-version <distro> 2`), or set sandbox.enabled to false.',
    };
  }
  return { supported: true };
}

function isUnshareAvailable(): boolean {
  if (_unshareAvailable !== null) return _unshareAvailable;
  try {
    execSync('unshare --map-root-user --mount --pid --fork -- true', { stdio: 'pipe', timeout: 5000 });
    _unshareAvailable = true;
  } catch {
    _unshareAvailable = false;
  }
  return _unshareAvailable;
}

export function checkSandboxDependencies(cfg?: Partial<SandboxConfig>): SandboxDependencyCheck {
  const errors: string[] = [];
  const warnings: string[] = [];

  // --- namespace isolation availability (the real filesystem/network sandbox) ---
  // Platform support is checked FIRST so an unsupported platform/WSL generation
  // gets its TARGETED reason (non-Linux, or WSL1-requires-WSL2) instead of the
  // generic unshare failure that WSL1 would otherwise surface.
  const support = getSandboxPlatformSupport();
  if (!support.supported) {
    errors.push(support.reason!);
  } else if (!isUnshareAvailable()) {
    // Distinguish "not installed" from "installed but user namespaces blocked" so
    // the remediation hint names the actual missing capability.
    if (!isCommandAvailable('unshare')) {
      errors.push(
        'Namespace sandbox dependency `unshare` is not installed. Install util-linux ' +
          '(Debian/Ubuntu: `apt-get install util-linux`; Fedora/RHEL: `dnf install util-linux`; ' +
          'Alpine: `apk add util-linux`).',
      );
    } else {
      errors.push(
        'Namespace sandbox dependency `unshare` is present but could not create a user namespace ' +
          '(unprivileged user namespaces are likely disabled). Enable them with ' +
          '`sysctl -w kernel.unprivileged_userns_clone=1`, or set sandbox.enabled to false.',
      );
    }
  }

  // --- capability-degradation warnings for the REQUESTED config. These apply
  //     even when namespaces work, because the allow/deny list is realized by
  //     iptables + getent INSIDE the namespace. ---
  const wantsAllowlist =
    !!cfg && cfg.allowNetwork === false && !!cfg.allowedDomains && cfg.allowedDomains.length > 0;
  const wantsDenylist = !!cfg?.deniedDomains && cfg.deniedDomains.length > 0;
  if (wantsAllowlist && !isCommandAvailable('iptables')) {
    warnings.push(
      'allowedDomains is configured but `iptables` is not available; the outbound network ' +
        'allowlist will NOT be enforced — all outbound traffic is permitted. Install iptables ' +
        '(Debian/Ubuntu: `apt-get install iptables`; Fedora/RHEL: `dnf install iptables`).',
    );
  }
  if ((wantsAllowlist || wantsDenylist) && !isCommandAvailable('getent')) {
    warnings.push(
      'A network allow/deny domain list is configured but `getent` (glibc name resolution) is ' +
        'not available; configured domains cannot be resolved to IPs and the rules will not match. ' +
        'Install glibc tools (Debian/Ubuntu: `apt-get install libc-bin`).',
    );
  }

  // --- glob-pattern degradation warning. The host sandbox enforces filesystem
  //     path restrictions with literal bind mounts, which do NOT expand shell
  //     globs. A path value containing glob metacharacters (`*?[]{}`) therefore
  //     silently matches nothing and the restriction is a no-op — surface it so
  //     an operator who wrote `denyRead: ['/home/*/.ssh']` learns it won't apply
  //     rather than believing the path is protected. Mirrors the reference's
  //     "glob pattern unsupported on this platform" restriction warning. ---
  if (cfg && process.platform === 'linux') {
    const globbed = collectGlobPatternPaths(cfg);
    if (globbed.length > 0) {
      warnings.push(
        'Sandbox filesystem path restriction(s) contain glob metacharacters which the host ' +
          'bind-mount sandbox cannot expand; these entries will NOT be enforced: ' +
          globbed.join(', ') +
          '. Use literal absolute paths instead.',
      );
    }
  }

  return { errors, warnings };
}

/**
 * Canonical decision: will `command` actually be wrapped by the lightweight HOST
 * (unshare) auto-sandbox? True ONLY when the platform allows host sandboxing, the
 * host sandbox is not globally disabled (env knob or `enabled === false`), AND the
 * command is a sandbox trigger / default-deny target that is not exempt. This is
 * the SINGLE source of the host-sandbox gating used by `bashTool` (to decide
 * wrapping) and by the permission layer's `autoAllowBashIfSandboxed` host-mode
 * path, which auto-allows exactly the commands that WILL be sandboxed and lets
 * sandbox-incompatible commands fall through to a regular permission prompt
 * (parity: the reference's "auto-allow" sandbox mode — try sandboxed, otherwise
 * regular permissions). Does NOT account for `dangerouslyDisableSandbox` (a
 * per-call input the caller handles separately). Pure.
 */
export function commandWillBeHostSandboxed(
  command: string,
  options: HostSandboxEligibilityOptions = {},
): boolean {
  const env = options.env ?? process.env;
  if (!isSandboxPlatformEnabled(options.enabledPlatforms, options.platform ?? process.platform)) return false;
  if (isHostSandboxDisabled(env) || options.enabled === false) return false;
  const exemptionOptions: SandboxExemptionOptions = {
    ...(options.excludedCommands ? { excludedCommands: options.excludedCommands } : {}),
    ...(isSandboxDefaultDeny(env) ? { defaultSandbox: true } : {}),
  };
  const impl = options.shouldUseSandboxImpl ?? shouldUseSandbox;
  return impl(command, exemptionOptions);
}

/** The isolation backend {@link wrapWithSandbox} will actually use on this host:
 *  `'unshare'` = full namespace isolation (filesystem + network); `'ulimits'` =
 *  the DEGRADED resource-limits-only fallback (CPU/memory/process caps only, NO
 *  filesystem or network isolation), used on non-Linux or when `unshare` /
 *  unprivileged user namespaces are unavailable. Pure: `platform` and the
 *  unshare-availability probe are injectable so both arms are deterministically
 *  testable on any host; the defaults read the live host (memoized). This is the
 *  single source of the wrap-time backend decision, reused by
 *  {@link describeSandboxFallbackWarning} so the surfaced warning can never drift
 *  from the branch {@link wrapWithSandbox} actually took. */
export function resolveSandboxMethod(
  platform: string = process.platform,
  unshareAvailable: () => boolean = isUnshareAvailable,
): 'unshare' | 'ulimits' {
  if (platform !== 'linux') return 'ulimits';
  return unshareAvailable() ? 'unshare' : 'ulimits';
}

export function wrapWithSandbox(
  command: string,
  cwd: string,
  config: Partial<SandboxConfig> = {},
): string {
  const cfg = { ...DEFAULT_SANDBOX_CONFIG, ...config };
  // PARITY (encoded-command seam): let the env gate opt the host sandbox into
  // base64 command passing when the caller did not already set it explicitly, so
  // the escape hatch is reachable from a single flag (self-wired, no bash.ts edit).
  if (cfg.encodeCommand === undefined && isSandboxEncodeCommandEnabled()) {
    cfg.encodeCommand = true;
  }
  if (resolveSandboxMethod() === 'unshare') {
    return wrapWithUnshare(command, cwd, cfg);
  }
  return wrapWithResourceLimits(command, cfg);
}

/**
 * PARITY (explicit weaker-fallback warning — the reference's "Cannot block …" degraded-
 * mode notices): when the host sandbox silently degrades to the resource-limits-
 * only backend (namespace isolation unavailable) but the operator EXPLICITLY
 * configured one or more network/filesystem carve-outs, enumerate the requested
 * protections that are now UNenforced so the user/model is told which isolation
 * was lost instead of believing the command ran fully sandboxed. Returns null when
 * the full `unshare` backend is in use (nothing degraded) OR when no explicit
 * carve-out was requested (the bare auto-sandbox has nothing beyond the ulimits it
 * still applies — stays silent to avoid noise). Inspects ONLY the caller-set
 * fields of `cfg` (not the merged defaults), mirroring
 * {@link checkSandboxDependencies}'s "a REQUESTED control is silently
 * unenforced" contract. `method` defaults to the live {@link resolveSandboxMethod}
 * and is injectable for tests.
 */
export function describeSandboxFallbackWarning(
  cfg: Partial<SandboxConfig> = {},
  method: 'unshare' | 'ulimits' = resolveSandboxMethod(),
): string | null {
  if (method === 'unshare') return null;
  const lost: string[] = [];
  if ((cfg.allowedDomains?.length ?? 0) > 0) lost.push('outbound network allowlist (allowedDomains)');
  if ((cfg.deniedDomains?.length ?? 0) > 0) lost.push('outbound network denylist (deniedDomains)');
  if ((cfg.denyReadPaths?.length ?? 0) > 0) lost.push('filesystem read denials (denyRead)');
  if ((cfg.denyWritePaths?.length ?? 0) > 0) lost.push('filesystem write denials (denyWrite)');
  if (cfg.denyConfigWrites) lost.push('config-write protection (denyConfigWrites)');
  if (cfg.denyBareGitPoisoning) lost.push('bare-repo poisoning guard (denyBareGitPoisoning)');
  if ((cfg.allowedUnixSockets?.length ?? 0) > 0) lost.push('per-path Unix-socket controls (allowUnixSockets)');
  if (lost.length === 0) return null;
  return (
    'the host sandbox degraded to resource-limits-only because namespace isolation ' +
    '(`unshare` / unprivileged user namespaces) is unavailable on this host, so the ' +
    'following REQUESTED sandbox protections are NOT enforced for this command: ' +
    lost.join('; ') +
    '. Only CPU/memory/process limits apply. Install util-linux and enable ' +
    'unprivileged user namespaces, or set sandbox.failIfUnavailable=true / ' +
    'allowUnsandboxedFallback=false to refuse running when the sandbox cannot start.'
  );
}

export function wrapWithUnshare(command: string, cwd: string, cfg: SandboxConfig): string {
  // `--map-root-user` fails inside a NESTED user namespace (the UID map is
  // already consumed), so `enableWeakerNestedSandbox` drops it to let a weaker
  // sandbox still start there. Default keeps the flag for full isolation.
  const flags: string[] = cfg.enableWeakerNestedSandbox
    ? ['--mount', '--pid', '--fork']
    : ['--map-root-user', '--mount', '--pid', '--fork'];

  // PARITY (host-backend per-host egress grant — mirrors wrapWithDocker): fold any
  // host the leader approved (`sandbox_permission_response`) OR a user approved
  // interactively (`requestSandboxHostAccess`) into the effective outbound allowlist,
  // and drop it from the denylist so a granted host WINS. This makes an on-demand
  // egress approval honored on the unshare backend too — previously only the
  // container backend honored grants, so a granted host worked in container mode but
  // was still blackholed on the host backend (the exact parity hole this closes).
  const effectiveAllowedDomains = dedupeHostList([
    ...(cfg.allowedDomains ?? []),
    ...getGrantedSandboxHosts(),
  ]);
  const effectiveDeniedDomains = (cfg.deniedDomains ?? []).filter((d) => !isSandboxHostGranted(d));
  const hasAllowedDomains = effectiveAllowedDomains.length > 0;

  if (!cfg.allowNetwork) {
    // Always create a network namespace. When allowedDomains is set we keep
    // --net so we get our own namespace but add iptables rules inside it
    // instead of blanket-blocking all traffic.
    flags.push('--net');
  }

  const ulimits = buildUlimits(cfg);
  const escapedCmd = command.replace(/'/g, "'\\''");

  const escapedCwd = cwd.replace(/'/g, "'\\''");
  const mounts = buildReadOnlyMounts(cfg);
  const writeDenials = buildWriteDenials(cfg);
  const configGuard = buildConfigWriteGuard(cwd, cfg);
  const gitGuard = buildBareGitRepoGuard(cwd, cfg);
  const readDenials = buildReadDenials(cfg);
  const credentialScrub = buildCredentialScrub(cfg);
  const socketDenial = buildSocketDenial(cfg);
  const loopbackSetup = buildLoopbackSetup(cfg);
  // Scratch-dir + proxy env exports apply right before the command runs so a
  // sandboxed command's temp files are contained and its outbound traffic is
  // proxy-routed (parity: the reference's sandboxTmpDir / http(s)/socks proxy knobs).
  const tmpDirEnv = buildSandboxTmpDir(cfg);
  const proxyEnv = buildProxyEnv(cfg);
  // Deny rules are emitted (and thus iptables-appended) BEFORE the allowlist's
  // accepts/default-drop, so a denied host is DROPped first → deny wins over allow.
  const networkRules =
    !cfg.allowNetwork && hasAllowedDomains
      ? buildDeniedDomainRules(effectiveDeniedDomains) + buildNetworkAllowlist(effectiveAllowedDomains)
      : '';
  // PARITY (encoded-command seam): emit either the raw single-quote-escaped command
  // or a base64-decode-and-run expression immune to any nested re-quoting layer.
  const runExpr = buildSandboxRunExpr(command, escapedCmd, !!cfg.encodeCommand);
  return `unshare ${flags.join(' ')} -- bash -c '${ulimits}${mounts}${writeDenials}${configGuard}${gitGuard}${readDenials}${credentialScrub}${socketDenial}${loopbackSetup}${networkRules}${tmpDirEnv}${proxyEnv}cd ${escapedCwd} && ${runExpr}'`;
}

/**
 * Report the host-side isolation capability plus a `warnings` channel surfacing
 * any degraded/unenforced controls (parity with the reference's dependency-aware status).
 * `available` stays true (AMI always has at least the ulimits fallback). When the
 * namespace sandbox cannot run, its diagnostic is folded into `warnings` (the
 * ulimits fallback still runs, so from the user's view it's a degradation, not a
 * hard error). When a `cfg` with an allow/deny domain list is supplied, a missing
 * `iptables`/`getent` is reported here too — so `/sandbox` can tell the user their
 * `allowedDomains` policy is silently a no-op instead of hiding it behind `2>/dev/null`.
 */
/**
 * PARITY (sandbox.enabledPlatforms): decide whether the sandbox (and its
 * auto-allow) applies on a given platform. Semantics mirror the reference's
 * sandbox-adapter `enabledPlatforms`:
 *   - `undefined` → enabled on ALL supported platforms (the default),
 *   - `[]`        → enabled on NONE (sandbox fully disabled by config),
 *   - otherwise   → a membership test against `process.platform`.
 * Pure (platform is injectable for tests); the default reads `process.platform`.
 */
export function isSandboxPlatformEnabled(
  enabledPlatforms: string[] | undefined,
  platform: string = process.platform,
): boolean {
  if (enabledPlatforms === undefined) return true;
  return enabledPlatforms.includes(platform);
}

/** A single enabled security-weakening sandbox option and its blunt tradeoff
 *  warning, surfaced to the operator (e.g. via `/sandbox`). */
export interface SandboxTradeoffWarning {
  /** The SandboxPolicy option that is weakening security. */
  option: string;
  /** User-facing "**Reduces security** — …" wording for that option. */
  warning: string;
}

/**
 * PARITY (explicit security-tradeoff warnings): enumerate every ENABLED
 * security-weakening knob on a {@link SandboxPolicy} with blunt, user-facing
 * wording, so the operator sees the tradeoff instead of a silent relaxation.
 * Mirrors the reference's per-option "**Reduces security** — …" descriptions on
 * `enableWeakerNetworkIsolation` et al. Returns `[]` for a policy with no
 * weakening options set (and for `mode: 'off'`, where the sandbox is inactive).
 */
export function describeSandboxSecurityTradeoffs(policy?: SandboxPolicy): SandboxTradeoffWarning[] {
  const out: SandboxTradeoffWarning[] = [];
  if (!policy || policy.mode === 'off') return out;
  if (policy.allowNetwork) {
    out.push({ option: 'allowNetwork', warning: '**Reduces security** — outbound network is reachable from inside the sandbox, enabling data exfiltration. Default: false.' });
  }
  if (policy.network?.allowAllUnixSockets) {
    out.push({ option: 'network.allowAllUnixSockets', warning: '**Reduces security** — all Unix domain sockets are reachable, including privileged daemon sockets (e.g. the Docker socket). Default: false.' });
  }
  if (policy.enableWeakerNestedSandbox) {
    out.push({ option: 'enableWeakerNestedSandbox', warning: '**Reduces security** — drops user-namespace root mapping so a nested command runs with weaker isolation. Default: false.' });
  }
  if (policy.enableWeakerNetworkIsolation) {
    out.push({ option: 'enableWeakerNetworkIsolation', warning: '**Reduces security** — opens a potential data-exfiltration vector through local trust/proxy sockets. Default: false.' });
  }
  if (policy.allowUnsandboxedFallback) {
    out.push({ option: 'allowUnsandboxedFallback', warning: '**Reduces security** — a command that cannot be sandboxed runs UNsandboxed on the host instead of being refused. Default: false.' });
  }
  return out;
}

/**
 * PARITY (dependency-aware socket-warning suppression — the reference's
 * `showSocketWarning = hasWarnings && !allowAllUnixSockets`): the host's ability to
 * block privileged Unix-domain sockets (bind /dev/null over the Docker/containerd
 * socket) depends on a working MOUNT namespace. When that capability is degraded
 * (`socketBlockingAvailable === false`, i.e. the sandbox fell back to
 * resource-limits-only) a sandboxed command could still reach those sockets — a
 * capability-degradation the user should be told about. But an operator who opted
 * into `allowAllUnixSockets` (every socket reachable by choice) has already
 * acknowledged that posture, so the notice is SUPPRESSED for them. Pure; both
 * inputs injectable for testing. Returns true only when the caveat should be shown.
 */
export const SOCKET_BLOCK_UNAVAILABLE_WARNING =
  'Unix domain socket denial cannot be enforced on this host: privileged daemon ' +
  'sockets (e.g. the Docker socket `/var/run/docker.sock`) may be reachable from ' +
  'inside the sandbox because the mount namespace is unavailable (resource-limits-' +
  'only fallback). Install util-linux and enable unprivileged user namespaces, or ' +
  'set network.allowAllUnixSockets to acknowledge and suppress this notice.';

export function shouldWarnSocketBlocking(
  socketBlockingAvailable: boolean,
  allowAllUnixSockets: boolean | undefined,
): boolean {
  return !socketBlockingAvailable && !allowAllUnixSockets;
}

export function getSandboxStatus(
  cfg?: Partial<SandboxConfig>,
  policy?: SandboxPolicy,
): { available: boolean; method: string; warnings: string[] } {
  const dep = checkSandboxDependencies(cfg);
  const nsAvailable = process.platform === 'linux' && isUnshareAvailable();
  const method = nsAvailable ? 'unshare' : 'ulimits';
  // On the ulimits fallback the namespace "errors" are a user-facing degradation
  // notice (AMI still runs, just with weaker isolation), so fold them in.
  const baseWarnings = nsAvailable ? dep.warnings : [...dep.errors, ...dep.warnings];
  // PARITY (socket-warning suppression): the bind-mount socket denial needs the
  // mount namespace, so it is unavailable on the ulimits fallback. Surface the
  // caveat unless the operator already opted into all-sockets-reachable (via the
  // policy's allowAllUnixSockets or the equivalent cfg.allowUnixSockets boolean).
  const allowAllUnixSockets = policy?.network?.allowAllUnixSockets ?? cfg?.allowUnixSockets;
  if (shouldWarnSocketBlocking(nsAvailable, allowAllUnixSockets)) {
    baseWarnings.push(SOCKET_BLOCK_UNAVAILABLE_WARNING);
  }
  // Fold in the blunt security-tradeoff wording for any enabled weakening knob so
  // `/sandbox` surfaces the relaxation instead of hiding it (parity).
  const tradeoffs = describeSandboxSecurityTradeoffs(policy).map((t) => `${t.option}: ${t.warning}`);
  return { available: true, method, warnings: [...baseWarnings, ...tradeoffs] };
}

export interface SandboxValidation {
  valid: boolean;
  error?: string;
  method?: string;
  // PARITY (fail-closed gate): when the sandbox is NOT valid, `fatal` tells the
  // caller whether to abort (true → the `failIfUnavailable` hard gate was set)
  // or degrade to running unsandboxed with a warning (false → the default).
  // Always false/absent on the valid path. Mirrors the reference's
  // `SandboxSettings.failIfUnavailable` semantics.
  fatal?: boolean;
}

/**
 * Validate that the host bash sandbox can actually start. When it cannot, the
 * `fatal` flag reflects the caller's `failIfUnavailable` policy: a managed
 * deployment that passes `{ failIfUnavailable: true }` gets `fatal: true` (abort
 * at startup) while the default gets `fatal: false` (warn + run unsandboxed).
 */
export function validateSandboxEnabled(opts?: { failIfUnavailable?: boolean }): SandboxValidation {
  const fatal = !!opts?.failIfUnavailable;
  // Reuse the structured dependency check so the error ENUMERATES the specific
  // missing dependency (unshare not installed vs. present-but-userns-disabled vs.
  // non-Linux platform) instead of one generic "not available or lacks
  // permissions" string that conflates distinct causes.
  const { errors } = checkSandboxDependencies();
  if (errors.length > 0) {
    return { valid: false, fatal, error: errors.join(' ') };
  }
  return { valid: true, method: 'unshare' };
}

export function resetSandboxCache(): void {
  _unshareAvailable = null;
  _wslVersion = null;
}
