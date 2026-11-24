// Split from bash-sandbox.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { SandboxConfig } from './bash-sandbox-sandbox-config';
import { CONFIG_ESCAPE_RELPATHS, joinUnderCwd } from './bash-sandbox-host-sandbox-config-from-policy';

/**
 * Bind-remount each path onto itself read-only, but ONLY when it exists — a
 * missing path is skipped so no 0-byte stub is left behind (which would both
 * pollute the host and break tools like git). Reuses the exact bind+remount
 * technique as {@link buildReadOnlyMounts} / {@link buildWriteDenials}; every
 * step is best-effort (`2>/dev/null`). Shared by the config-write and bare-git
 * guards so the ro-bind logic exists in one place (No-Duplication Rule).
 */
export function roBindExistingPaths(paths: string[]): string {
  return paths
    .map((p) => {
      const esc = escapeShell(p).replace(/'/g, "'\\''");
      return `if [ -e ${esc} ]; then mount --bind ${esc} ${esc} 2>/dev/null && mount -o remount,ro,bind ${esc} 2>/dev/null; fi; `;
    })
    .join('');
}

/**
 * PARITY (config-rewrite / privilege-escalation escape): ro-bind the agent's own
 * settings/config files and auto-loaded skills/commands/agents directories
 * (under cwd) so a sandboxed command can neither weaken the sandbox by editing
 * trusted config nor plant an auto-loaded skill/command/agent. Returns '' when
 * `denyConfigWrites` is off. Mirrors the reference's sandbox-adapter unconditional
 * `denyWrite` of settings files + `.claude/skills`/commands/agents.
 */
export function buildConfigWriteGuard(cwd: string, cfg: SandboxConfig): string {
  if (!cfg.denyConfigWrites) return '';
  return roBindExistingPaths(CONFIG_ESCAPE_RELPATHS.map((rel) => joinUnderCwd(cwd, rel)));
}

/**
 * The files git's `is_git_directory()` keys on: a dir containing HEAD +
 * objects/ + refs/ is treated as a (bare) repo, and a planted `config`/`hooks`
 * can run code when an UNsandboxed git later runs there.
 */
export const BARE_GIT_REPO_FILES = ['HEAD', 'objects', 'refs', 'hooks', 'config'] as const;

/**
 * PARITY (bare-repo .git-file poisoning defense): ro-bind any existing bare-repo
 * marker files under cwd so a sandboxed command cannot modify a planted/pre-
 * existing HEAD/objects/refs/hooks/config that a later unsandboxed `git` would
 * honor. Returns '' when `denyBareGitPoisoning` is off. Existence-guarded so a
 * non-existent marker is never stubbed (matching the reference's avoidance of
 * 0-byte stubs that break `git`). Mirrors the reference's sandbox-adapter guard.
 */
export function buildBareGitRepoGuard(cwd: string, cfg: SandboxConfig): string {
  if (!cfg.denyBareGitPoisoning) return '';
  return roBindExistingPaths(BARE_GIT_REPO_FILES.map((f) => joinUnderCwd(cwd, f)));
}

/**
 * Realize the advertised readOnlyPaths inside the --mount namespace: bind each
 * path onto itself and remount it read-only so a sandboxed command cannot write
 * to it. Failures (e.g. a path that does not exist on this host) are tolerated
 * so they never abort the user's command. Returns '' when no readOnlyPaths are
 * configured. Runs before cd/command.
 */
export function buildReadOnlyMounts(cfg: SandboxConfig): string {
  return cfg.readOnlyPaths
    .map((p) => {
      // escapeShell single-quotes the path; re-escape those quotes so it is safe
      // to embed inside the already single-quoted `bash -c '...'` body.
      const esc = escapeShell(p).replace(/'/g, "'\\''");
      return `mount --bind ${esc} ${esc} 2>/dev/null && mount -o remount,ro,bind ${esc} 2>/dev/null; `;
    })
    .join('');
}

/**
 * PARITY (filesystem write-deny with allowWrite precedence): bind-remount each
 * `denyWritePaths` entry read-only inside the mount namespace so it cannot be
 * written even if it would otherwise be writable — EXCEPT any entry that is equal
 * to, or nested under, an `allowWritePaths` region, which is left writable
 * ("allowWrite wins over denyWrite for matching paths"). This is the exact write
 * mirror of {@link buildReadDenials}'s allowRead-over-denyRead precedence, and it
 * is where a `permissions.additionalDirectories`/`--add-dir` write grant that
 * overlaps a broader deny is re-opened on the host backend. Reuses the same
 * bind+remount technique as {@link buildReadOnlyMounts}; failures are tolerated
 * (`2>/dev/null`). Returns '' when nothing is denied (or everything is
 * re-allowed). Mirrors the reference's nested `SandboxFilesystemConfig.allowWrite` /
 * `denyWrite` carve-out model.
 */
export function buildWriteDenials(cfg: SandboxConfig): string {
  if (!cfg.denyWritePaths || cfg.denyWritePaths.length === 0) return '';
  const allow = (cfg.allowWritePaths ?? []).map((a) => a.trim()).filter((a) => a.length > 0);
  return cfg.denyWritePaths
    .filter((p) => !allow.some((a) => pathIsWithin(p, a)))
    .map((p) => {
      const esc = escapeShell(p).replace(/'/g, "'\\''");
      return `mount --bind ${esc} ${esc} 2>/dev/null && mount -o remount,ro,bind ${esc} 2>/dev/null; `;
    })
    .join('');
}

/**
 * True when `child` is the same path as, or nested under, `parent` (segment-
 * aware: `/a/b` is under `/a` but `/ab` is not). Trailing slashes are normalized
 * so `/a/` and `/a` compare equal. Used to realize the `allowRead`-over-`denyRead`
 * precedence (a denied path inside an allowed region is re-opened).
 */
export function pathIsWithin(child: string, parent: string): boolean {
  const c = child.replace(/\/+$/, '');
  const p = parent.replace(/\/+$/, '');
  if (p.length === 0) return false;
  return c === p || c.startsWith(p + '/');
}

/**
 * PARITY (filesystem read-deny with allowRead precedence): bind /dev/null over
 * each `denyReadPaths` entry so a read inside the sandbox returns empty rather
 * than the real contents — EXCEPT any entry that is equal to, or nested under,
 * an `allowReadPaths` region, which is left readable ("allowRead wins over
 * denyRead for matching paths"). Reuses the same /dev/null-bind technique as the
 * credential file-hiding path; each bind is best-effort (`2>/dev/null`). Returns
 * '' when nothing is denied (or everything is re-allowed). Mirrors the reference's
 * `SandboxFilesystemConfig.denyRead` + `allowRead`.
 */
export function buildReadDenials(cfg: SandboxConfig): string {
  if (!cfg.denyReadPaths || cfg.denyReadPaths.length === 0) return '';
  const allow = (cfg.allowReadPaths ?? []).map(a => a.trim()).filter(a => a.length > 0);
  return cfg.denyReadPaths
    .filter((p) => !allow.some((a) => pathIsWithin(p, a)))
    .map((p) => {
      const esc = p.replace(/'/g, "'\\''");
      return `mount --bind /dev/null '${esc}' 2>/dev/null; `;
    })
    .join('');
}

/** A valid TCP port (1..65535) usable for a loopback proxy. Non-integers and
 * out-of-range values are rejected so a bad config can never emit a bogus env.
 * Shared with the container backend ({@link import('./docker-sandbox')}) so both
 * sandboxes validate proxy ports identically — no duplicate predicate. */
export function isValidProxyPort(port: unknown): port is number {
  return typeof port === 'number' && Number.isInteger(port) && port >= 1 && port <= 65535;
}

/**
 * PARITY (proxy-routed network isolation): export the standard proxy env vars so
 * a sandboxed command's outbound traffic is routed through a loopback proxy
 * (which can allowlist/MITM-filter) instead of (or alongside) the raw iptables IP
 * allowlist. `httpProxyPort` sets http/https proxy vars; `socksProxyPort` sets
 * all_proxy (SOCKS). Returns '' when neither is a valid port. Ports are numbers
 * validated to 1..65535, so the emitted env is injection-free. Mirrors the reference's
 * `SandboxNetworkConfig.httpProxyPort` / `socksProxyPort`.
 */
export function buildProxyEnv(cfg: SandboxConfig): string {
  let out = '';
  if (isValidProxyPort(cfg.httpProxyPort)) {
    const url = `http://127.0.0.1:${cfg.httpProxyPort}`;
    out += `export http_proxy='${url}' https_proxy='${url}' HTTP_PROXY='${url}' HTTPS_PROXY='${url}'; `;
  }
  if (isValidProxyPort(cfg.socksProxyPort)) {
    const url = `socks5://127.0.0.1:${cfg.socksProxyPort}`;
    out += `export all_proxy='${url}' ALL_PROXY='${url}'; `;
  }
  return out;
}

/**
 * PARITY (sandbox-scoped temp dir): pin TMPDIR/TMP/TEMP to `sandboxTmpDir` inside
 * the sandbox (creating it if missing) so scratch/heredoc temp files land in a
 * writable, contained location rather than a user-env TMPDIR that could point
 * outside the sandbox. Returns '' when unset. Mirrors the reference's shell-provider
 * `sandboxTmpDir` TMPDIR override.
 */
export function buildSandboxTmpDir(cfg: SandboxConfig): string {
  if (!cfg.sandboxTmpDir) return '';
  const esc = cfg.sandboxTmpDir.replace(/'/g, "'\\''");
  // Besides TMPDIR/TMP/TEMP, pin two more env knobs so NO scratch file leaks to
  // the host (PARITY — the reference's shell providers set these when sandboxed,
  // bashProvider.ts:L235-L247):
  //   - TMPPREFIX: zsh writes heredoc temp files under $TMPPREFIX (default
  //     /tmp/zsh*), NOT TMPDIR, so a sandboxed zsh heredoc would otherwise land on
  //     the host /tmp. Pin it to `<scratch>/zsh` (the reference's posixJoin(tmpDir,'zsh')).
  //     Safe unconditionally — non-zsh shells ignore TMPPREFIX.
  //   - AMI_SANDBOX_TMPDIR: exposes the scratch dir to child processes that look it
  //     up explicitly (the reference's CLAUDE_CODE_TMPDIR).
  return (
    `mkdir -p '${esc}' 2>/dev/null; export TMPDIR='${esc}' TMP='${esc}' TEMP='${esc}' ` +
    `TMPPREFIX='${esc}/zsh' AMI_SANDBOX_TMPDIR='${esc}'; `
  );
}

/**
 * Build shell commands to scrub sensitive credentials from the sandbox
 * environment. Unsets listed env vars and bind-mounts /dev/null over listed
 * files so their contents are invisible inside the namespace.
 */
export function buildCredentialScrub(cfg: SandboxConfig): string {
  let cmds = '';
  if (cfg.hiddenEnvVars && cfg.hiddenEnvVars.length > 0) {
    cmds += 'unset ' + cfg.hiddenEnvVars.join(' ') + '; ';
  }
  if (cfg.hiddenFiles && cfg.hiddenFiles.length > 0) {
    for (const f of cfg.hiddenFiles) {
      const esc = f.replace(/'/g, "'\\''");
      cmds += "mount --bind /dev/null '" + esc + "' 2>/dev/null; ";
    }
  }
  return cmds;
}

/**
 * Unix-domain sockets that grant control of a privileged host daemon and are a
 * well-known sandbox-escape vector: a sandboxed command that can talk to the
 * Docker/containerd socket can launch a privileged container and break out
 * entirely (`curl --unix-socket /var/run/docker.sock ...`). The auto-sandbox
 * only wraps network/exec triggers (curl, wget, interpreters), which never
 * legitimately need these sockets, so by default we bind /dev/null over them
 * inside the private mount namespace. The denial is confined to the sandbox's
 * own namespace and never affects the host.
 */
export const DEFAULT_DENIED_SOCKETS = [
  '/var/run/docker.sock',
  '/run/docker.sock',
  '/var/run/containerd/containerd.sock',
  '/run/containerd/containerd.sock',
];

/**
 * Build shell commands that bind /dev/null over privileged daemon sockets so a
 * sandboxed command cannot connect to them. Returns '' when the operator has
 * opted out via `allowUnixSockets`. Each bind is best-effort (`2>/dev/null`) so
 * a socket that does not exist on this host is silently skipped. Reuses the same
 * /dev/null-bind technique as the credential file-hiding path.
 */
export function buildSocketDenial(cfg: SandboxConfig): string {
  // `enableWeakerNetworkIsolation` (parity) leaves ALL default-denied sockets
  // reachable so a local trust/CA service (MITM-proxy validator) works — the same
  // all-sockets-open effect as the blanket `allowUnixSockets` opt-out.
  if (cfg.allowUnixSockets || cfg.enableWeakerNetworkIsolation) return '';
  // PARITY (per-path socket allow): re-allow any default-denied socket whose path
  // the operator explicitly listed in `allowedUnixSockets`, leaving the rest
  // denied — the reference's granular `allowUnixSockets: string[]` instead of the
  // all-or-nothing boolean. Normalize whitespace so a padded entry still matches.
  const allowed = new Set(
    (cfg.allowedUnixSockets ?? []).map(s => s.trim()).filter(s => s.length > 0),
  );
  return DEFAULT_DENIED_SOCKETS.filter(s => !allowed.has(s))
    .map(s => `mount --bind /dev/null '${s}' 2>/dev/null; `)
    .join('');
}

/**
 * PARITY (host denylist): build iptables DROP rules for each denied domain's
 * resolved IPs. Returns '' for an empty/undefined list. These rules are emitted
 * (and iptables-appended) BEFORE {@link buildNetworkAllowlist}'s ACCEPT rules so
 * a denied host is dropped first even if it also appears in / overlaps the
 * allowlist — "deny wins over allow". Trailing dots are stripped (FQDN
 * normalization), matching the allowlist builder. Mirrors the reference's
 * `SandboxNetworkConfig.deniedHosts`.
 */
export function buildDeniedDomainRules(domains?: string[]): string {
  if (!domains || domains.length === 0) return '';
  let rules = '';
  for (const raw of domains) {
    const d = raw.replace(/\.+$/, '');
    const esc = d.replace(/'/g, "'\\''");
    rules +=
      "for ip in $(getent ahosts '" +
      esc +
      "' 2>/dev/null | awk '{print $1}' | sort -u); do iptables -A OUTPUT -d $ip -j DROP 2>/dev/null; done; ";
  }
  return rules;
}

/**
 * Build iptables rules that allow outbound traffic only to the listed domains
 * (plus loopback and DNS). Returns '' for an empty list. Meant to run inside
 * a network namespace created by unshare --net.
 */
export function buildNetworkAllowlist(domains: string[]): string {
  if (domains.length === 0) return '';
  let rules = '';
  // Allow loopback always
  rules += 'iptables -A OUTPUT -o lo -j ACCEPT 2>/dev/null; ';
  // Allow DNS
  rules += 'iptables -A OUTPUT -p udp --dport 53 -j ACCEPT 2>/dev/null; ';
  rules += 'iptables -A OUTPUT -p tcp --dport 53 -j ACCEPT 2>/dev/null; ';
  // Allow each domain — strip trailing dots (FQDN normalization)
  for (const raw of domains) {
    const d = raw.replace(/\.+$/, '');
    const esc = d.replace(/'/g, "'\\''");
    rules +=
      "for ip in $(getent ahosts '" +
      esc +
      "' 2>/dev/null | awk '{print $1}' | sort -u); do iptables -A OUTPUT -d $ip -j ACCEPT 2>/dev/null; done; ";
  }
  // Default deny outbound
  rules += 'iptables -A OUTPUT -j DROP 2>/dev/null; ';
  return rules;
}

export function wrapWithResourceLimits(command: string, cfg: SandboxConfig): string {
  // In resource-limits-only mode (non-Linux or no unshare), we can still hide
  // env vars (no mount namespace available, so file hiding is skipped).
  let envScrub = '';
  if (cfg.hiddenEnvVars && cfg.hiddenEnvVars.length > 0) {
    envScrub = 'unset ' + cfg.hiddenEnvVars.join(' ') + '; ';
  }
  // Scratch-dir + proxy env also apply in the resource-limits-only fallback
  // (non-Linux / no unshare): they need no mount namespace, so a sandboxed
  // command still gets a contained TMPDIR and proxy-routed network there.
  const tmpDirEnv = buildSandboxTmpDir(cfg);
  const proxyEnv = buildProxyEnv(cfg);
  return `${buildUlimits(cfg)}${envScrub}${tmpDirEnv}${proxyEnv}${command}`;
}

export function buildUlimits(cfg: SandboxConfig): string {
  const limits: string[] = [];
  const memKB = cfg.maxMemoryMB * 1024;
  limits.push(`ulimit -v ${memKB} 2>/dev/null`);
  limits.push(`ulimit -u ${cfg.maxProcesses} 2>/dev/null`);
  limits.push(`ulimit -f 104857600 2>/dev/null`);
  return limits.join('; ') + '; ';
}

export function escapeShell(s: string): string {
  return "'" + s.replace(/'/g, "'\\''") + "'";
}
