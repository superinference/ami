import { execSync } from 'child_process';

export interface SandboxConfig {
  allowNetwork: boolean;
  readOnlyPaths: string[];
  writablePaths: string[];
  maxMemoryMB: number;
  maxProcesses: number;
  timeout: number;
}

const DEFAULT_SANDBOX_CONFIG: SandboxConfig = {
  allowNetwork: false,
  readOnlyPaths: ['/usr', '/bin', '/lib', '/lib64', '/etc'],
  writablePaths: ['/tmp'],
  maxMemoryMB: 512,
  maxProcesses: 64,
  timeout: 30000,
};

let _unshareAvailable: boolean | null = null;

function isUnshareAvailable(): boolean {
  if (_unshareAvailable !== null) return _unshareAvailable;
  try {
    execSync('which unshare', { stdio: 'pipe', timeout: 2000 });
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
];

export function shouldUseSandbox(command: string): boolean {
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

function wrapWithUnshare(command: string, cwd: string, cfg: SandboxConfig): string {
  const flags: string[] = ['--map-root-user', '--mount', '--pid', '--fork'];

  if (!cfg.allowNetwork) {
    flags.push('--net');
  }

  const ulimits = buildUlimits(cfg);
  const escapedCmd = command.replace(/'/g, "'\\''");

  return `unshare ${flags.join(' ')} -- bash -c '${ulimits}cd ${escapeShell(cwd)} && ${escapedCmd}'`;
}

function wrapWithResourceLimits(command: string, cfg: SandboxConfig): string {
  return `${buildUlimits(cfg)}${command}`;
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

export function resetSandboxCache(): void {
  _unshareAvailable = null;
}
