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

export function shouldUseSandbox(command: string): boolean {
  // Match exemptions on the original command so `bash -c "pytest ..."` and
  // `python tests/runtests.py` are not wrapped. Quote-stripping hides the
  // inner command and leaves `bash -c`, which is a sandbox trigger.
  if (SANDBOX_EXEMPT_PATTERNS.some(p => p.test(command))) return false;
  const stripped = command.replace(/"[^"]*"|'[^']*'/g, '');
  if (SANDBOX_EXEMPT_PATTERNS.some(p => p.test(stripped))) return false;
  return SANDBOX_TRIGGER_PATTERNS.some(p => p.test(stripped));
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
  const credentialScrub = buildCredentialScrub(cfg);
  const networkRules =
    !cfg.allowNetwork && hasAllowedDomains
      ? buildNetworkAllowlist(cfg.allowedDomains!)
      : '';
  return `unshare ${flags.join(' ')} -- bash -c '${ulimits}${mounts}${credentialScrub}${networkRules}cd ${escapedCwd} && ${escapedCmd}'`;
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
