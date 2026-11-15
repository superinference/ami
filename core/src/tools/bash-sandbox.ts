import { execSync } from 'child_process';

export interface SandboxConfig {
  allowNetwork: boolean;
  readOnlyPaths: string[];
  writablePaths: string[];
  maxMemoryMB: number;
  maxProcesses: number;
  timeout: number;
  hiddenEnvVars?: string[];
  hiddenFiles?: string[];
  allowedDomains?: string[];
  // PARITY (host denylist): domains blocked for outbound traffic. Honored only
  // within the allowlist path (alongside `allowedDomains`), where their resolved
  // IPs are DROPped BEFORE the allow ACCEPT rules so a denied host wins over the
  // allowlist even if it also matches an allowed entry. Mirrors Claude's
  // `SandboxNetworkConfig.deniedHosts` ("deny wins over allow"). Additive:
  // undefined/empty leaves the allowlist behavior unchanged.
  deniedDomains?: string[];
  // When false/omitted (the secure default), the unshare sandbox bind-mounts
  // /dev/null over well-known privileged daemon sockets (Docker/containerd)
  // inside its mount namespace so a sandboxed command cannot reach them — a
  // classic sandbox-escape vector (e.g. `curl --unix-socket /var/run/docker.sock`
  // can launch a privileged container). Set true to opt out of that denial.
  allowUnixSockets?: boolean;
  // PARITY (per-path socket allow): specific Unix-socket paths to RE-ALLOW (i.e.
  // NOT bind /dev/null over) even while the blanket socket denial is otherwise
  // active. Mirrors Claude's `SandboxNetworkConfig.allowUnixSockets: string[]`:
  // granular control instead of the all-or-nothing `allowUnixSockets` boolean.
  // A path listed here is removed from the default-denied set. Additive:
  // undefined/empty keeps every default socket denied.
  allowedUnixSockets?: string[];
  // PARITY (filesystem read-deny): absolute paths that must be UNreadable inside
  // the sandbox. Each is bind-mounted with /dev/null so a read returns empty,
  // using the same technique as credential file-hiding. Mirrors Claude's
  // `SandboxFilesystemConfig.denyRead`. Additive: undefined/empty is a no-op.
  denyReadPaths?: string[];
  // PARITY (filesystem write-deny): absolute paths that must be read-only inside
  // the sandbox even if they would otherwise be writable. Each is bind-remounted
  // read-only, the same technique as `readOnlyPaths`. Mirrors Claude's
  // `SandboxFilesystemConfig.denyWrite`. Additive: undefined/empty is a no-op.
  denyWritePaths?: string[];
}

export function getDefaultMemoryMB(): number {
  const env = process.env.AMI_TOOL_MEMORY_LIMIT_MB;
  if (env) {
    const n = parseInt(env, 10);
    if (n >= 64 && n <= 65536) return n;
  }
  return 512;
}

const DEFAULT_SANDBOX_CONFIG: SandboxConfig = {
  allowNetwork: false,
  readOnlyPaths: ['/usr', '/bin', '/lib', '/lib64', '/etc'],
  writablePaths: ['/tmp'],
  maxMemoryMB: getDefaultMemoryMB(),
  maxProcesses: 8192,
  timeout: 30000,
};

let _unshareAvailable: boolean | null = null;

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

const SANDBOX_TRIGGER_PATTERNS = [
  /\bcurl\b/,
  /\bwget\b/,
  /\bpython\s/,
  /\bpython3?\s/,
  /\bnode\s+-e\b/,
  /\bperl\s+-e\b/,
  /\bruby\s+-e\b/,
  /\bphp\s+-r\b/,
  /\beval\b/,
  /\bsh\s+-c\b/,
  /\bbash\s+-c\b/,
];

const SANDBOX_EXEMPT_PATTERNS = [
  /\bgit\b/,
  /\bnpm\b/,
  /\bnpx\b/,
  /\byarn\b/,
  /\bpnpm\b/,
  /\bmake\b/,
  /\bcargo\b/,
  /\bgo\s+(build|test|run|mod)\b/,
  /\bpytest\b/,
  /\bpython\S*\s+-m\s+pytest\b/,
  /\bpython\S*\s+-m\s+unittest\b/,
  /\bruntests\.py\b/,
  /\bmanage\.py\s+test\b/,
  /\bpip\s+install\b/,
  /\buv\s+pip\b/,
];

/**
 * Options controlling which command prefixes bypass the sandbox.
 *
 * By default the hardcoded `SANDBOX_EXEMPT_PATTERNS` (package managers, build
 * tools, test runners) are exempt. Operators/policy can extend or replace that
 * set via `excludedCommands`:
 *   - `excludedCommands` is a list of literal command prefixes (e.g. `'docker'`,
 *     `'bazel build'`) that should NOT be sandboxed. Each is matched at a word
 *     boundary against the command.
 *   - When `overrideDefaults` is false/omitted, `excludedCommands` is ADDITIVE
 *     to the hardcoded defaults (no behavior change by default — an empty/absent
 *     list keeps exactly the current exemptions).
 *   - When `overrideDefaults` is true, `excludedCommands` REPLACES the hardcoded
 *     defaults entirely (an operator opting into a stricter or fully custom
 *     exemption policy).
 */
export interface SandboxExemptionOptions {
  excludedCommands?: string[];
  overrideDefaults?: boolean;
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Resolve the effective list of sandbox-exempt patterns for a given policy.
 * Returns the hardcoded defaults when no options are supplied, so the default
 * behavior is unchanged.
 */
export function buildSandboxExemptPatterns(options?: SandboxExemptionOptions): RegExp[] {
  const custom = (options?.excludedCommands ?? [])
    .map(c => c.trim())
    .filter(c => c.length > 0)
    .map(c => new RegExp('\\b' + escapeRegExp(c) + '\\b'));
  if (options?.overrideDefaults) return custom;
  return [...SANDBOX_EXEMPT_PATTERNS, ...custom];
}

/**
 * Split a command line into its top-level subcommands on shell control
 * operators (`&&`, `||`, `;`, `|`, `&`, newline) WITHOUT splitting inside
 * single/double quotes. Each subcommand is evaluated independently by
 * `shouldUseSandbox` so a compound command cannot escape the sandbox merely
 * because ONE of its subcommands is exempt (e.g. the `curl` in
 * `git pull && curl https://evil` must still be sandboxed even though `git` is
 * exempt). Mirrors the per-subcommand matching the permission layer already does
 * for command chaining.
 */
function splitTopLevelSubcommands(command: string): string[] {
  const parts: string[] = [];
  let cur = '';
  let quote: string | null = null;
  for (let i = 0; i < command.length; i++) {
    const ch = command[i];
    if (quote) {
      cur += ch;
      if (ch === quote) quote = null;
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      cur += ch;
      continue;
    }
    const two = command.slice(i, i + 2);
    if (two === '&&' || two === '||') {
      parts.push(cur);
      cur = '';
      i++;
      continue;
    }
    if (ch === ';' || ch === '|' || ch === '&' || ch === '\n') {
      parts.push(cur);
      cur = '';
      continue;
    }
    cur += ch;
  }
  parts.push(cur);
  return parts.map(p => p.trim()).filter(p => p.length > 0);
}

/**
 * Strip URL arguments (`scheme://...`) from a subcommand before exemption
 * matching. An exempt keyword appearing inside a URL/arg handed to a network
 * trigger (e.g. `curl https://git.example.com/x` or
 * `curl https://raw.githubusercontent.com/npm/pkg`) must NOT flip the exemption
 * decision — only the command itself may. Attacker-influenced URLs/args can
 * otherwise defeat the sandbox trigger.
 */
function stripUrlArgs(s: string): string {
  return s.replace(/\b[a-z][a-z0-9+.-]*:\/\/\S+/gi, '');
}

export function shouldUseSandbox(command: string, options?: SandboxExemptionOptions): boolean {
  const exemptPatterns = buildSandboxExemptPatterns(options);
  // Evaluate each top-level subcommand independently. A compound command is
  // sandboxed if ANY of its subcommands is a trigger that is not itself exempt,
  // so an exempt subcommand (git/npm/test-runner) can never suppress the
  // sandbox for a sibling trigger subcommand (the compound-escape vector).
  for (const sub of splitTopLevelSubcommands(command)) {
    // Exemption matches on the subcommand with URL args removed, so an exempt
    // keyword inside a trigger's URL cannot exempt it. Quotes are KEPT here so
    // test runners hidden in a quoted argument (`bash -c "pytest ..."`,
    // `python tests/runtests.py`) remain exempt — AMI's deliberate superset.
    const forExempt = stripUrlArgs(sub);
    if (exemptPatterns.some(p => p.test(forExempt))) continue;
    // Triggers match on the quote-stripped subcommand so trigger text inside
    // quotes (`echo "curl is a tool"`) does not force the sandbox.
    const stripped = sub.replace(/"[^"]*"|'[^']*'/g, '');
    if (SANDBOX_TRIGGER_PATTERNS.some(p => p.test(stripped))) return true;
  }
  return false;
}

export function wrapWithSandbox(
  command: string,
  cwd: string,
  config: Partial<SandboxConfig> = {},
): string {
  const cfg = { ...DEFAULT_SANDBOX_CONFIG, ...config };

  if (process.platform !== 'linux') {
    return wrapWithResourceLimits(command, cfg);
  }

  if (isUnshareAvailable()) {
    return wrapWithUnshare(command, cwd, cfg);
  }

  return wrapWithResourceLimits(command, cfg);
}

export function wrapWithUnshare(command: string, cwd: string, cfg: SandboxConfig): string {
  const flags: string[] = ['--map-root-user', '--mount', '--pid', '--fork'];

  const hasAllowedDomains =
    cfg.allowedDomains && cfg.allowedDomains.length > 0;

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
  const readDenials = buildReadDenials(cfg);
  const credentialScrub = buildCredentialScrub(cfg);
  const socketDenial = buildSocketDenial(cfg);
  // Deny rules are emitted (and thus iptables-appended) BEFORE the allowlist's
  // accepts/default-drop, so a denied host is DROPped first → deny wins over allow.
  const networkRules =
    !cfg.allowNetwork && hasAllowedDomains
      ? buildDeniedDomainRules(cfg.deniedDomains) + buildNetworkAllowlist(cfg.allowedDomains!)
      : '';
  return `unshare ${flags.join(' ')} -- bash -c '${ulimits}${mounts}${writeDenials}${readDenials}${credentialScrub}${socketDenial}${networkRules}cd ${escapedCwd} && ${escapedCmd}'`;
}

/**
 * Realize the advertised readOnlyPaths inside the --mount namespace: bind each
 * path onto itself and remount it read-only so a sandboxed command cannot write
 * to it. Failures (e.g. a path that does not exist on this host) are tolerated
 * so they never abort the user's command. Returns '' when no readOnlyPaths are
 * configured. Runs before cd/command.
 */
function buildReadOnlyMounts(cfg: SandboxConfig): string {
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
 * PARITY (filesystem write-deny): bind-remount each `denyWritePaths` entry
 * read-only inside the mount namespace so it cannot be written even if it would
 * otherwise be writable. Reuses the exact bind+remount technique as
 * {@link buildReadOnlyMounts}; failures are tolerated (`2>/dev/null`). Returns ''
 * when no denyWritePaths are configured. Mirrors Claude's
 * `SandboxFilesystemConfig.denyWrite`.
 */
export function buildWriteDenials(cfg: SandboxConfig): string {
  if (!cfg.denyWritePaths || cfg.denyWritePaths.length === 0) return '';
  return cfg.denyWritePaths
    .map((p) => {
      const esc = escapeShell(p).replace(/'/g, "'\\''");
      return `mount --bind ${esc} ${esc} 2>/dev/null && mount -o remount,ro,bind ${esc} 2>/dev/null; `;
    })
    .join('');
}

/**
 * PARITY (filesystem read-deny): bind /dev/null over each `denyReadPaths` entry
 * so a read inside the sandbox returns empty rather than the real contents.
 * Reuses the same /dev/null-bind technique as the credential file-hiding path;
 * each bind is best-effort (`2>/dev/null`). Returns '' when none are configured.
 * Mirrors Claude's `SandboxFilesystemConfig.denyRead`.
 */
export function buildReadDenials(cfg: SandboxConfig): string {
  if (!cfg.denyReadPaths || cfg.denyReadPaths.length === 0) return '';
  return cfg.denyReadPaths
    .map((p) => {
      const esc = p.replace(/'/g, "'\\''");
      return `mount --bind /dev/null '${esc}' 2>/dev/null; `;
    })
    .join('');
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
const DEFAULT_DENIED_SOCKETS = [
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
  if (cfg.allowUnixSockets) return '';
  // PARITY (per-path socket allow): re-allow any default-denied socket whose path
  // the operator explicitly listed in `allowedUnixSockets`, leaving the rest
  // denied — Claude's granular `allowUnixSockets: string[]` instead of the
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
 * normalization), matching the allowlist builder. Mirrors Claude's
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

function wrapWithResourceLimits(command: string, cfg: SandboxConfig): string {
  // In resource-limits-only mode (non-Linux or no unshare), we can still hide
  // env vars (no mount namespace available, so file hiding is skipped).
  let envScrub = '';
  if (cfg.hiddenEnvVars && cfg.hiddenEnvVars.length > 0) {
    envScrub = 'unset ' + cfg.hiddenEnvVars.join(' ') + '; ';
  }
  return `${buildUlimits(cfg)}${envScrub}${command}`;
}

function buildUlimits(cfg: SandboxConfig): string {
  const limits: string[] = [];
  const memKB = cfg.maxMemoryMB * 1024;
  limits.push(`ulimit -v ${memKB} 2>/dev/null`);
  limits.push(`ulimit -u ${cfg.maxProcesses} 2>/dev/null`);
  limits.push(`ulimit -f 104857600 2>/dev/null`);
  return limits.join('; ') + '; ';
}

function escapeShell(s: string): string {
  return "'" + s.replace(/'/g, "'\\''") + "'";
}

export function getSandboxStatus(): { available: boolean; method: string } {
  if (process.platform === 'linux' && isUnshareAvailable()) {
    return { available: true, method: 'unshare' };
  }
  return { available: true, method: 'ulimits' };
}

export interface SandboxValidation {
  valid: boolean;
  error?: string;
  method?: string;
}

export function validateSandboxEnabled(): SandboxValidation {
  if (process.platform !== 'linux') {
    return {
      valid: false,
      error: 'Sandbox mode requires Linux with namespace support (unshare). ' +
        `Current platform is ${process.platform}. ` +
        'Set sandbox.enabled to false or run on Linux.',
    };
  }
  if (!isUnshareAvailable()) {
    return {
      valid: false,
      error: 'Sandbox mode is enabled but the unshare command is not available or lacks permissions. ' +
        'Install util-linux and ensure user namespaces are enabled (sysctl kernel.unprivileged_userns_clone=1), ' +
        'or set sandbox.enabled to false.',
    };
  }
  return { valid: true, method: 'unshare' };
}

export function resetSandboxCache(): void {
  _unshareAvailable = null;
}
