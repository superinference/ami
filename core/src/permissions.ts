import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { expandPath } from './utils/path';
import { extractCommandPaths } from './tools/bash-security';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type PermissionMode = 'ask' | 'auto-allow' | 'deny-all' | 'acceptEdits' | 'dontAsk' | 'bypassPermissions' | 'plan';

export interface PermissionRule {
  tool: string;       // tool name or '*'
  pattern?: string;   // glob pattern for args (e.g., "git *")
  action: 'allow' | 'deny' | 'ask';
  reason?: string;    // optional reason for deny rules
}

export type BashClassification = 'safe' | 'unsafe' | 'destructive';

export interface PermissionPromptResult {
  action: 'allow_once' | 'allow_pattern' | 'deny';
  pattern?: string;
}

export interface PermissionPromptHandler {
  showPrompt(
    toolName: string,
    inputSummary: string,
    classification?: BashClassification,
  ): Promise<PermissionPromptResult>;
}

export interface DenyCheckResult {
  denied: boolean;
  reason?: string;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/**
 * Commands that are inherently read-only or side-effect-free.
 * These are auto-allowed in 'ask' mode.
 *
 * NOTE: `env` and `printenv` are intentionally excluded — they expose
 * sensitive environment variables (API keys, tokens, etc.).
 */
const SAFE_COMMANDS = new Set([
  'ls', 'cat', 'head', 'tail', 'wc', 'grep', 'echo', 'pwd',
  'which', 'whoami', 'date', 'uname', 'hostname',
  'id', 'df', 'du', 'file', 'stat', 'readlink', 'basename', 'dirname',
  'sort', 'uniq', 'diff', 'tr', 'cut', 'less', 'more',
  'test', 'true', 'false', 'type', 'man', 'help',
  // JSON/YAML processors (flag-validated)
  'jq', 'yq',
  // Command builder (flag-validated, target-restricted)
  'xargs',
  // Network/process inspection (flag-validated)
  'netstat', 'lsof', 'pgrep', 'ss',
  // Terminal capabilities (flag-validated)
  'tput',
  // File finders (flag-validated)
  'fd', 'fdfind',
  // Directory tree (flag-validated)
  'tree',
]);

const SAFE_COMMAND_FLAGS: Record<string, { allowed: Set<string>; blocked: Set<string> }> = {
  jq: {
    allowed: new Set(['-r', '-e', '-c', '-S', '-s', '--raw-output', '--exit-status', '--compact-output', '--sort-keys', '--arg', '--argjson', '--null-input', '-n']),
    blocked: new Set(['-f', '--from-file', '--rawfile', '--slurpfile', '-L']),
  },
  yq: {
    allowed: new Set(['-r', '-e', '-c', '-S', '-s', '--raw-output', '--exit-status', '--compact-output', '--sort-keys', '--arg', '--argjson', '--null-input', '-n']),
    blocked: new Set(['-f', '--from-file', '--rawfile', '--slurpfile', '-L']),
  },
  xargs: { allowed: new Set(['-n', '-P', '-I', '-0', '-d', '-t', '-p', '-r', '-L', '--max-procs', '--null']), blocked: new Set(['-i', '-e']) },
  ps: { allowed: new Set(['-e', '-f', '-a', '-u', '-x', '--sort', '-o', '--no-headers', '-p']), blocked: new Set(['e']) },
  tree: { allowed: new Set(['-L', '-d', '-f', '-a', '--dirsfirst', '-I', '--prune', '-P', '--charset', '-i']), blocked: new Set(['-R']) },
  fd: { allowed: new Set(['-t', '-e', '-E', '--type', '--extension', '--exclude', '-H', '--hidden', '-I', '--no-ignore', '-g', '-p', '--max-depth', '-d']), blocked: new Set(['-x', '--exec', '-X', '--exec-batch']) },
  fdfind: { allowed: new Set(['-t', '-e', '-E', '--type', '--extension', '--exclude', '-H', '--hidden', '-I', '--no-ignore', '-g', '-p', '--max-depth', '-d']), blocked: new Set(['-x', '--exec', '-X', '--exec-batch']) },
  find: { allowed: new Set(['-name', '-type', '-maxdepth', '-mindepth', '-path', '-iname', '-size', '-newer', '-print']), blocked: new Set(['-delete', '-exec', '-execdir', '-ok', '-okdir']) },
  date: { allowed: new Set(['-u', '--utc', '-d', '--date', '-R', '--rfc-email', '-I', '--iso-8601']), blocked: new Set(['-s', '--set']) },
  netstat: { allowed: new Set(['-t', '-u', '-l', '-n', '-p', '-a', '--tcp', '--udp', '--listening', '--numeric']), blocked: new Set([]) },
  lsof: { allowed: new Set(['-i', '-n', '-P', '-p']), blocked: new Set([]) },
  pgrep: { allowed: new Set(['-l', '-a', '-f', '-x']), blocked: new Set([]) },
  tput: { allowed: new Set(['cols', 'lines', 'colors', 'setaf', 'sgr0']), blocked: new Set([]) },
  ss: { allowed: new Set(['-t', '-u', '-l', '-n', '-p', '-a']), blocked: new Set([]) },
};

/** Safe target commands for xargs — only these can follow xargs. */
const XARGS_SAFE_TARGETS = new Set([
  'echo', 'cat', 'ls', 'grep', 'wc', 'head', 'tail', 'basename', 'dirname',
]);

function validateCommandFlags(cmd: string, args: string[]): boolean {
  const flagDef = SAFE_COMMAND_FLAGS[cmd];
  if (!flagDef) return true;
  for (const arg of args) {
    if (arg.startsWith('-')) {
      const flag = arg.replace(/^--?/, '').split('=')[0];
      if (flagDef.blocked.has(arg) || flagDef.blocked.has(flag)) return false;
    }
  }
  return true;
}

/**
 * Check whether a jq/yq expression contains dangerous constructs.
 * Blocks `-f`, `--rawfile`, `--slurpfile`, `-L` flags and `system()` calls.
 */
function isJqDangerous(args: string[]): boolean {
  const dangerousFlags = ['-f', '--rawfile', '--slurpfile', '-L', '--from-file'];
  for (const arg of args) {
    if (dangerousFlags.includes(arg)) return true;
  }
  // Check for system() call in jq expression
  const expr = args.find(a => !a.startsWith('-'));
  if (expr && /\bsystem\b/.test(expr)) return true;
  return false;
}

/**
 * Check whether xargs is being used with a safe target command.
 * xargs is only safe when its target is a read-only command.
 */
function isXargsSafe(command: string): boolean {
  // Extract the target command after xargs and its flags
  // eslint-disable-next-line security/detect-unsafe-regex
  const xargsMatch = command.match(/\bxargs\s+(?:-\S+\s+)*(\S+)/);
  if (!xargsMatch) return false;
  const target = xargsMatch[1];
  return XARGS_SAFE_TARGETS.has(target);
}

/**
 * Check whether a python3 -e/-c one-liner is read-only.
 * Blocks imports of os, subprocess, shutil which can mutate state.
 */
function isScriptOneLinerSafe(lang: string, code: string): boolean {
  if (lang === 'python3') {
    return !/\bimport\s+(os|subprocess|shutil)\b/.test(code) &&
           !/\bfrom\s+(os|subprocess|shutil)\b/.test(code);
  }
  if (lang === 'node') {
    return !/\brequire\s*\(\s*['"](?:child_process|fs)['"]\s*\)/.test(code) &&
           !/\bimport\b.*\bfrom\s+['"](?:child_process|fs)['"]/.test(code);
  }
  // ruby -e and perl -e are read-only by default (no specific block list)
  return true;
}

/**
 * Strip quoted sections from a command to analyze unquoted content.
 */
function stripQuotedSections(command: string): string {
  // Remove double-quoted sections
  let result = command.replace(/"[^"]*"/g, '');
  // Remove single-quoted sections
  result = result.replace(/'[^']*'/g, '');
  return result;
}

/**
 * Check whether a command contains unquoted variable expansions or glob
 * patterns that could be exploited for injection.
 */
export function containsUnquotedExpansion(command: string): boolean {
  const unquoted = stripQuotedSections(command);
  // Check for $VAR, ${VAR}, $(cmd)
  if (/\$[A-Za-z_{(]/.test(unquoted)) return true;
  // Check for glob characters * and ?
  if (/[*?]/.test(unquoted)) return true;
  // Check for bracket expressions [...]
  if (/\[.*\]/.test(unquoted)) return true;
  return false;
}

/** Safe read-only docker subcommands. */
const DOCKER_SAFE_SUBCOMMANDS = new Set([
  'ps', 'images', 'logs', 'inspect', 'stats', 'top', 'port', 'version', 'info',
]);

/** Compound safe prefixes: "command subcommand" pairs that are safe. */
const SAFE_PREFIXES = new Set([
  'git status', 'git log', 'git diff', 'git branch', 'git show',
  'git remote', 'git tag', 'git stash list', 'git rev-parse',
  'npm list', 'npm ls', 'npm view',
  'tsc', 'tsc --noEmit',
  'cargo build', 'cargo check', 'cargo test', 'cargo clippy',
  'go build', 'go test', 'go vet', 'go fmt',
  'make build', 'make test', 'make check', 'make lint',
]);

/**
 * Commands that modify state and should prompt the user.
 */
const UNSAFE_COMMANDS = new Set([
  'git push', 'git commit', 'git merge', 'git rebase', 'git cherry-pick',
  'npm install', 'npm ci', 'npm update', 'npm uninstall', 'npm publish',
  'pip install', 'pip uninstall',
  'docker-compose',
  'curl', 'wget',
  'ssh', 'scp', 'rsync',
  'chmod', 'chown', 'chgrp',
  'sudo', 'su', 'doas',
  'apt', 'apt-get', 'dnf', 'yum', 'pacman', 'brew',
  'mv', 'cp', 'ln', 'mkdir', 'touch',
  'node', 'python', 'python3', 'ruby', 'perl',
]);

/**
 * Commands that are destructive and always require explicit permission
 * with an extra warning.
 */
const DESTRUCTIVE_COMMANDS = new Set([
  'rm', 'rmdir',
  'git reset --hard', 'git clean',
  'kill', 'pkill', 'killall',
  'format', 'mkfs',
  'dd',
  'truncate', 'shred',
  'redirect-to-system-path',
]);

/** SQL statements that are destructive. */
const DESTRUCTIVE_SQL_PATTERNS = [
  /\bDROP\s+TABLE\b/i,
  /\bDROP\s+DATABASE\b/i,
  /\bDELETE\s+FROM\b/i,
  /\bTRUNCATE\s+TABLE\b/i,
  /\bTRUNCATE\b/i,
];

const SAFE_ENV_VARS = new Set([
  'NODE_ENV', 'LANG', 'LC_ALL', 'LC_CTYPE', 'TERM', 'COLORTERM',
  'NO_COLOR', 'FORCE_COLOR', 'RUST_LOG', 'RUST_BACKTRACE',
  'GOOS', 'GOARCH', 'GOPATH', 'GOBIN', 'GOROOT',
  'PYTHONDONTWRITEBYTECODE', 'PYTHONUNBUFFERED', 'VIRTUAL_ENV',
  'EDITOR', 'VISUAL', 'PAGER', 'LESS', 'TZ',
  'CI', 'GITHUB_ACTIONS', 'GITLAB_CI', 'JENKINS_URL',
  'npm_config_loglevel', 'npm_config_registry',
  'CARGO_HOME', 'RUSTUP_HOME',
]);

export function stripSafeEnvVars(command: string): string {
  // eslint-disable-next-line security/detect-unsafe-regex
  return command.replace(/^(\s*(?:\w+=\S+\s+)*)/g, (match) => {
    const assignments = match.trim().split(/\s+/);
    const unsafe = assignments.filter(a => {
      const varName = a.split('=')[0];
      return !SAFE_ENV_VARS.has(varName);
    });
    return unsafe.length > 0 ? unsafe.join(' ') + ' ' : '';
  });
}

/**
 * Hardline patterns — blocked unconditionally, even in auto-allow mode.
 * These prevent catastrophic system damage regardless of settings.
 */
const HARDLINE_PATTERNS: Array<{ pattern: RegExp; description: string }> = [
  { pattern: /\brm\s+(-[a-zA-Z]*r[a-zA-Z]*\s+|.*\s+)\/\s*$/,  description: 'rm -r on filesystem root' },
  { pattern: /\brm\s+(-[a-zA-Z]*r[a-zA-Z]*\s+|.*\s+)\/(home|etc|usr|var|bin|sbin|boot|lib|lib64)\b/, description: 'rm -r on system directory' },
  { pattern: /\bmkfs\b/, description: 'filesystem format' },
  { pattern: /\bdd\b.*\bof\s*=\s*\/dev\/[sh]d/, description: 'dd to block device' },
  { pattern: /:\(\)\s*\{\s*:\s*\|\s*:\s*&\s*\}\s*;\s*:/, description: 'fork bomb' },
  { pattern: /\b(shutdown|reboot|halt|poweroff)\b/, description: 'system power control' },
  { pattern: /\binit\s+[06]\b/, description: 'init level change' },
  { pattern: /\bsystemctl\s+(poweroff|reboot|halt)\b/, description: 'systemctl power control' },
  { pattern: />\s*\/dev\/[sh]d/, description: 'redirect to block device' },
  { pattern: /\bchmod\b.{0,20}777\s+\//, description: 'chmod 777 on system path' },
  { pattern: /\bcurl\b.*\|\s*(ba|z|da|fi|k|c|tc)?sh\b/, description: 'pipe curl to shell' },
  { pattern: /\bwget\b.*\|\s*(ba|z|da|fi|k|c|tc)?sh\b/, description: 'pipe wget to shell' },
  { pattern: /\bpkill\b.*\bnode\b/, description: 'pkill node would kill AMI — use kill <pid> to target a specific process' },
  { pattern: /\bkillall\b.*\bnode\b/, description: 'killall node would kill AMI — use kill <pid> to target a specific process' },
  { pattern: /\bpkill\b.*\btsx\b/, description: 'pkill tsx would kill AMI — use kill <pid> to target a specific process' },
  { pattern: /\bkillall\b.*\btsx\b/, description: 'killall tsx would kill AMI — use kill <pid> to target a specific process' },
  { pattern: /\bpkill\b.*\bsuperinference\b/i, description: 'would kill AMI — use kill <pid> to target a specific process' },
];

export function detectHardlineCommand(command: string): { blocked: boolean; description: string | null } {
  for (const { pattern, description } of HARDLINE_PATTERNS) {
    if (pattern.test(command)) {
      return { blocked: true, description };
    }
  }
  return { blocked: false, description: null };
}

export function detectCommandChaining(command: string): { chained: boolean; operators: string[]; count: number } {
  const stripped = command.replace(/"[^"]*"|'[^']*'/g, '');
  const operators: string[] = [];
  if (/&&/.test(stripped)) operators.push('&&');
  if (/\|\|/.test(stripped)) operators.push('||');
  if (/;/.test(stripped)) operators.push(';');

  const segments = stripped.split(/\s*(?:&&|\|\||;)\s*/).filter(s => s.trim());
  return { chained: operators.length > 0, operators, count: segments.length };
}

/** System directories that should never be written to. */
const SYSTEM_DIRECTORIES = [
  '/etc', '/usr', '/bin', '/sbin', '/var',
  '/sys', '/proc', '/dev', '/boot', '/lib', '/lib64',
  '/opt',
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Shell wrappers that execute their arguments as commands.
 * If we see these, we must also classify the inner command.
 */
const SHELL_WRAPPERS = new Set([
  'bash', 'sh', 'zsh', 'dash', 'fish', 'ksh', 'csh', 'tcsh',
  'env', 'sudo', 'su', 'doas', 'nohup', 'nice', 'timeout',
  'xargs', 'exec', 'eval',
]);

/**
 * Extract all command names from a shell string.
 * Handles: pipes, semicolons, &&/||, command substitution $(...),
 * subshells (...), shell wrappers (env, sudo, bash -c), and
 * environment variable assignments as prefixes.
 */
function findMatchingParen(s: string, start: number): number {
  let depth = 1;
  for (let i = start; i < s.length; i++) {
    if (s[i] === '(') depth++;
    if (s[i] === ')') { depth--; if (depth === 0) return i; }
  }
  return -1;
}

function extractBaseCommands(command: string): string[] {
  const commands: string[] = [];

  function extractDelimited(
    cmd: string,
    match: (c: string, idx: number, src: string) => { innerStart: number; end: number } | null,
  ): void {
    let i = 0;
    while (i < cmd.length) {
      const m = match(cmd[i], i, cmd);
      if (m) {
        commands.push(...extractBaseCommands(cmd.slice(m.innerStart, m.end)));
        i = m.end + 1;
      } else {
        i++;
      }
    }
  }

  // 1. Extract commands from $(...) and `...` substitutions
  extractDelimited(command, (ch, i, src) => {
    if (ch === '$' && i + 1 < src.length && src[i + 1] === '(') {
      const end = findMatchingParen(src, i + 2);
      return end !== -1 ? { innerStart: i + 2, end } : null;
    }
    if (ch === '`') {
      const end = src.indexOf('`', i + 1);
      return end !== -1 ? { innerStart: i + 1, end } : null;
    }
    return null;
  });

  // 2. Extract commands from subshells (...)
  extractDelimited(command, (ch, i, src) => {
    if (ch === '(' && (i === 0 || src[i - 1] !== '$')) {
      const end = findMatchingParen(src, i + 1);
      return end !== -1 ? { innerStart: i + 1, end } : null;
    }
    return null;
  });

  // 3. Split on pipes, semicolons, && and ||
  const segments = command.split(/\s*(?:\|\||&&|[|;])\s*/);

  for (const segment of segments) {
    const trimmed = segment.trim();
    if (!trimmed) continue;

    const tokens = trimmed.split(/\s+/);
    let i = 0;

    // Skip environment variable assignments
    while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) {
      i++;
    }

    if (i >= tokens.length) continue;

    let cmd = tokens[i]!;

    // Unwrap shell wrappers: env cmd, sudo cmd, bash -c "cmd"
    while (SHELL_WRAPPERS.has(cmd) && i + 1 < tokens.length) {
      commands.push(cmd);
      i++;
      // Skip flags (e.g., sudo -u root, bash -c)
      while (i < tokens.length && tokens[i]!.startsWith('-')) {
        const flag = tokens[i]!;
        i++;
        // -c takes the next arg as a command string — parse it
        if ((flag === '-c') && i < tokens.length) {
          const innerCmd = tokens.slice(i).join(' ').replace(/^["']|["']$/g, '');
          commands.push(...extractBaseCommands(innerCmd));
          i = tokens.length;
          break;
        }
      }
      // After unwrapping a shell wrapper (especially env), skip env var
      // assignments (e.g., env PATH=/usr/bin ls -> skip PATH=/usr/bin)
      while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) {
        i++;
      }
      if (i < tokens.length) {
        cmd = tokens[i]!;
      }
    }

    if (i < tokens.length) {
      commands.push(cmd);
      if (i + 1 < tokens.length) {
        commands.push(cmd + ' ' + tokens[i + 1]!);
        if (i + 2 < tokens.length) {
          commands.push(cmd + ' ' + tokens[i + 1]! + ' ' + tokens[i + 2]!);
        }
      }
    }
  }

  // 4. Detect dangerous redirects to system paths
  if (/>\s*\/etc\/|>\s*\/usr\/|>\s*\/bin\//i.test(command)) {
    commands.push('redirect-to-system-path');
  }

  return commands;
}

/**
 * Match a simple glob-like pattern against a string.
 * Supports '*' as a wildcard for any sequence of characters.
 */
const _patternCache = new Map<string, RegExp>();
function matchPattern(pattern: string, value: string): boolean {
  if (pattern === value) return true;
  if (pattern === '*') return true;

  let regex = _patternCache.get(pattern);
  if (!regex) {
    const escaped = pattern
      .replace(/[.+^${}()|[\]\\?]/g, '\\$&')
      .replace(/\*/g, '.*');
    regex = new RegExp(`^${escaped}$`);
    _patternCache.set(pattern, regex);
  }
  return regex.test(value);
}

/**
 * Build a denial key from a tool name and its input for deduplication.
 */
function denialKey(toolName: string, input: Record<string, unknown>): string {
  const inputStr = typeof input.command === 'string'
    ? input.command
    : typeof input.file_path === 'string'
      ? input.file_path
      : JSON.stringify(input);
  return `${toolName}::${inputStr}`;
}

// ---------------------------------------------------------------------------
// PermissionManager
// ---------------------------------------------------------------------------

export class PermissionManager {
  private mode: PermissionMode = 'ask';
  private rules: PermissionRule[] = [];
  private deniedCommands: Set<string> = new Set();

  // Denial circuit breaker — abort after too many consecutive or total denials
  private consecutiveDenials = 0;
  private totalDenials = 0;
  private readonly MAX_CONSECUTIVE = 3;
  private readonly MAX_TOTAL = 20;

  constructor(mode?: PermissionMode, rules?: PermissionRule[]) {
    if (mode) this.mode = mode;
    if (rules) this.rules = [...rules];
  }

  // -------------------------------------------------------------------------
  // Core permission check
  // -------------------------------------------------------------------------

  /**
   * Check whether a tool invocation is allowed, denied, or needs user input.
   */
  async check(
    toolName: string,
    input: Record<string, unknown>,
  ): Promise<'allow' | 'deny' | 'ask'> {
    // Hardline safety — unconditional, overrides all modes including auto-allow
    if (toolName === 'bash' && typeof input.command === 'string') {
      const hardline = detectHardlineCommand(input.command);
      if (hardline.blocked) return 'deny';
    }

    // Fast path: mode-level overrides
    if (this.mode === 'auto-allow') return 'allow';
    if (this.mode === 'deny-all') return 'deny';
    if (this.mode === 'dontAsk') return 'deny';

    if (this.mode === 'bypassPermissions') {
      const safetyPaths = [
        '.git/', '.superinference/', '.bashrc', '.zshrc', '.profile', '.gitconfig',
        '.vscode/', '.ssh/', '.gnupg/', '.aws/', '.config/',
      ];
      if (toolName === 'bash' || toolName === 'file_edit' || toolName === 'file_write') {
        const pathArg = (input.file_path || input.command || '') as string;
        for (const sp of safetyPaths) {
          if (typeof pathArg === 'string' && pathArg.includes(sp)) return 'ask';
        }
      }
      return 'allow';
    }

    if (this.mode === 'plan') {
      const readOnlyTools = new Set(['file_read', 'grep', 'glob', 'list_dir', 'web_fetch', 'web_search', 'search_symbols', 'tool_search', 'task_tracker', 'task_list', 'task_output']);
      if (readOnlyTools.has(toolName)) return 'allow';
      return 'deny';
    }

    // acceptEdits: auto-allow file-mutating tools and safe bash
    if (this.mode === 'acceptEdits') {
      const editTools = new Set(['file_edit', 'file_write', 'notebook_edit', 'multi_edit']);
      if (editTools.has(toolName)) return 'allow';
      if (toolName === 'bash' && typeof input.command === 'string') {
        const classification = this.classifyBashCommand(input.command);
        if (classification === 'safe') return 'allow';
      }
    }

    // Check if this exact invocation was already denied
    const key = denialKey(toolName, input);
    if (this.deniedCommands.has(key)) return 'deny';


    // Destructive bash commands always require confirmation in 'ask' mode,
    // even when matched by a wildcard rule — prevents rm -rf of user output
    if (this.mode === 'ask' && toolName === 'bash' && typeof input.command === 'string') {
      const classification = this.classifyBashCommand(input.command);
      if (classification === 'destructive') return 'ask';
    }

    // Evaluate rules in order (first match wins)
    for (const rule of this.rules) {
      const toolMatches = rule.tool === '*' || rule.tool === toolName;
      if (!toolMatches) continue;

      if (rule.pattern) {
        // Build a representative string from the input to match against
        const inputStr = this.inputToString(toolName, input);
        if (matchPattern(rule.pattern, inputStr)) {
          return rule.action;
        }
      } else {
        // No pattern -- rule applies to the whole tool
        return rule.action;
      }
    }

    // Default: 'ask' mode means non-read-only tools require a prompt
    return 'ask';
  }

  // -------------------------------------------------------------------------
  // Per-tool deny rules
  // -------------------------------------------------------------------------

  /**
   * Check deny rules for a specific tool and input combination.
   * Returns denied: true if any deny rule with a matching pattern fires.
   */
  checkDenyRules(toolName: string, input: string): DenyCheckResult {
    for (const rule of this.rules) {
      if (rule.action === 'deny' && (rule.tool === toolName || rule.tool === '*')) {
        if (!rule.pattern || matchPattern(rule.pattern, input)) {
          return { denied: true, reason: rule.reason || `Denied by rule for ${rule.tool}` };
        }
      }
    }
    return { denied: false };
  }

  // -------------------------------------------------------------------------
  // Bash command classification
  // -------------------------------------------------------------------------

  /**
   * Classify a bash command as safe, unsafe, or destructive.
   */
  classifyBashCommand(command: string): BashClassification {
    const trimmed = command.trim();
    if (!trimmed) return 'safe';

    const bases = extractBaseCommands(trimmed);

    // Check destructive first (highest severity wins)
    for (const cmd of bases) {
      if (DESTRUCTIVE_COMMANDS.has(cmd)) return 'destructive';
    }

    // Check destructive SQL patterns on the entire command string
    for (const pattern of DESTRUCTIVE_SQL_PATTERNS) {
      if (pattern.test(trimmed)) return 'destructive';
    }

    // Check if "rm" with -rf or -r flags
    if (/\brm\s+.*-[^\s]*r/i.test(trimmed) || /\brm\s+.*-[^\s]*f/i.test(trimmed)) {
      return 'destructive';
    }

    // Flag-aware checks for commands that are safe/destructive depending on flags
    if (/\bsed\s+.*-[^\s]*i/.test(trimmed)) return 'unsafe'; // sed -i modifies files in place
    if (/\bfind\s+.*-delete\b/.test(trimmed)) return 'destructive';
    if (/\bfind\s+.*-exec\s+rm\b/.test(trimmed)) return 'destructive';
    if (/\bxargs\s+.*\brm\b/.test(trimmed)) return 'destructive';
    if (/\bxargs\s+.*\bmv\b/.test(trimmed)) return 'unsafe';
    if (/\bmake\s+(clean|install|distclean|uninstall)\b/.test(trimmed)) return 'unsafe';

    // Script one-liner safety: python3 -c, node -e, ruby -e, perl -e
    // Must come before UNSAFE_COMMANDS so safe one-liners aren't blocked
    const scriptMatch = trimmed.match(/^(python3?|node|ruby|perl)\s+(-[ce])\s+['"]?(.*?)['"]?\s*$/);
    if (scriptMatch) {
      const [, lang, , code] = scriptMatch;
      if (isScriptOneLinerSafe(lang!, code!)) return 'safe';
      return 'unsafe';
    }

    // Check unsafe
    for (const cmd of bases) {
      if (UNSAFE_COMMANDS.has(cmd)) return 'unsafe';
    }

    // Docker: only read-only subcommands are safe
    if (/\bdocker\s+/.test(trimmed)) {
      const dockerMatch = trimmed.match(/\bdocker\s+(\S+)/);
      if (dockerMatch && DOCKER_SAFE_SUBCOMMANDS.has(dockerMatch[1])) {
        return 'safe';
      }
      return 'unsafe';
    }

    // Check safe prefixes (e.g., "git status", "npm test")
    for (const cmd of bases) {
      if (SAFE_PREFIXES.has(cmd)) return 'safe';
    }

    // Check single-word safe commands with flag validation
    for (const cmd of bases) {
      if (SAFE_COMMANDS.has(cmd)) {
        const tokens = trimmed.split(/\s+/);
        const cmdIdx = tokens.indexOf(cmd);
        const args = cmdIdx >= 0 ? tokens.slice(cmdIdx + 1) : [];

        if (!validateCommandFlags(cmd, args)) return 'unsafe';

        // jq/yq: additional dangerous-construct check
        if ((cmd === 'jq' || cmd === 'yq') && isJqDangerous(args)) return 'unsafe';

        // xargs: must target a safe command
        if (cmd === 'xargs' && !isXargsSafe(trimmed)) return 'unsafe';

        // Read-only commands with unquoted expansions are suspicious
        if (containsUnquotedExpansion(trimmed)) return 'unsafe';

        return 'safe';
      }
    }

    // sed without -i is read-only (prints to stdout)
    if (/\bsed\b/.test(trimmed) && !/\bsed\s+.*-[^\s]*i/.test(trimmed)) return 'safe';
    // find without -delete/-exec rm is read-only
    if (/\bfind\b/.test(trimmed) && !/\b-delete\b/.test(trimmed) && !/\b-exec\s+rm\b/.test(trimmed)) return 'safe';

    // Check if extracted paths target system directories
    const cmdPaths = extractCommandPaths(trimmed);
    for (const p of cmdPaths) {
      for (const sysDir of SYSTEM_DIRECTORIES) {
        const resolved = path.resolve(p);
        if (resolved === sysDir || resolved.startsWith(sysDir + path.sep)) {
          return 'destructive';
        }
      }
    }

    // Unknown commands default to unsafe
    return 'unsafe';
  }

  // -------------------------------------------------------------------------
  // Path safety
  // -------------------------------------------------------------------------

  /**
   * Check whether a file path is safe to operate on.
   *
   * Allowed:
   *   - Paths within the working directory
   *   - Paths within the user's home directory
   *
   * Denied:
   *   - System directories (/etc, /usr, /bin, /var, /sys, /proc, etc.)
   *   - Paths containing ".." segments that escape cwd
   */
  isPathAllowed(filePath: string, cwd: string): boolean {
    let resolved = expandPath(filePath, cwd);

    // Resolve symlinks to prevent symlink-based escapes
    try {
      resolved = fs.realpathSync(resolved);
    } catch {
      // File may not exist yet (write operation) — keep the normalized path
    }

    // Block sensitive files
    const basename = path.basename(resolved).toLowerCase();
    const sensitiveNames = ['.env', 'credentials', 'secrets', 'id_rsa', 'id_ed25519'];
    const sensitiveExts = ['.pem', '.key', '.p12', '.pfx', '.keystore'];
    if (sensitiveNames.some(s => basename.includes(s)) || sensitiveExts.some(e => basename.endsWith(e))) {
      return false;
    }

    // Deny paths with ".." that escape the cwd
    const relative = path.relative(cwd, resolved);
    if (relative.startsWith('..') && path.isAbsolute(filePath)) {
      // Absolute path outside cwd -- check if it's in the home directory
      const homeDir = os.homedir();
      const normalizedHome = path.normalize(homeDir);
      const isInHome = resolved === normalizedHome || resolved.startsWith(normalizedHome + path.sep);
      if (!isInHome) {
        // Check against system directories
        for (const sysDir of SYSTEM_DIRECTORIES) {
          if (resolved === sysDir || resolved.startsWith(sysDir + path.sep)) {
            return false;
          }
        }
      }
      return isInHome;
    }

    // Check against system directories for absolute paths
    for (const sysDir of SYSTEM_DIRECTORIES) {
      if (resolved === sysDir || resolved.startsWith(sysDir + path.sep)) {
        return false;
      }
    }

    // Path is within cwd or within home
    const normalizedCwd = path.normalize(cwd);
    const homeDir = path.normalize(os.homedir());

    const inCwd = resolved === normalizedCwd || resolved.startsWith(normalizedCwd + path.sep);
    const inHome = resolved === homeDir || resolved.startsWith(homeDir + path.sep);

    return inCwd || inHome;
  }

  // -------------------------------------------------------------------------
  // Denial tracking & circuit breaker
  // -------------------------------------------------------------------------

  /**
   * Record a denied tool invocation so that repeat requests are auto-denied
   * without re-prompting the user.
   */
  trackDenial(toolName: string, input: Record<string, unknown>): void {
    const key = denialKey(toolName, input);
    this.deniedCommands.add(key);
  }

  /**
   * Check whether a given invocation has already been denied.
   */
  isDenied(toolName: string, input: Record<string, unknown>): boolean {
    const key = denialKey(toolName, input);
    return this.deniedCommands.has(key);
  }

  /**
   * Clear all tracked denials.
   */
  clearDenials(): void {
    this.deniedCommands.clear();
  }

  /**
   * Record a denial for the circuit breaker.
   * Call this when a permission check results in a denial.
   */
  recordDenial(): void {
    this.consecutiveDenials++;
    this.totalDenials++;
  }

  /**
   * Record a successful permission grant.
   * Resets the consecutive denial counter.
   */
  recordSuccess(): void {
    this.consecutiveDenials = 0;
  }

  /**
   * Check whether the circuit breaker has tripped.
   * Returns true if too many consecutive or total denials have occurred,
   * indicating the agent should abort rather than keep trying.
   */
  shouldAbort(): boolean {
    return this.consecutiveDenials >= this.MAX_CONSECUTIVE || this.totalDenials >= this.MAX_TOTAL;
  }

  /**
   * Reset the circuit breaker counters.
   */
  resetCircuitBreaker(): void {
    this.consecutiveDenials = 0;
    this.totalDenials = 0;
  }

  // -------------------------------------------------------------------------
  // Rule management
  // -------------------------------------------------------------------------

  /**
   * Get the current permission mode.
   */
  getMode(): PermissionMode {
    return this.mode;
  }

  /**
   * Set the permission mode.
   */
  setMode(mode: PermissionMode): void {
    this.mode = mode;
  }

  /**
   * Get the current rule list.
   */
  getRules(): PermissionRule[] {
    return [...this.rules];
  }

  /**
   * Add a rule. It is prepended so that later-added rules take priority.
   */
  addRule(rule: PermissionRule): void {
    this.rules.unshift(rule);
  }

  /**
   * Detect rules that are shadowed by higher-priority rules and will never fire.
   */
  detectShadowedRules(): Array<{ rule: PermissionRule; shadowedBy: PermissionRule }> {
    const shadows: Array<{ rule: PermissionRule; shadowedBy: PermissionRule }> = [];
    for (let i = 0; i < this.rules.length; i++) {
      for (let j = 0; j < i; j++) {
        if (this.rules[i].tool === this.rules[j].tool &&
            this.rules[i].action === 'allow' &&
            (this.rules[j].action === 'ask' || this.rules[j].action === 'deny') &&
            (!this.rules[j].pattern || this.rules[j].pattern === '*')) {
          shadows.push({ rule: this.rules[i], shadowedBy: this.rules[j] });
        }
      }
    }
    return shadows;
  }

  // -------------------------------------------------------------------------
  // Persistence
  // -------------------------------------------------------------------------

  /**
   * Load permission rules from `.superinference/permissions.json` relative
   * to the given working directory.
   */
  loadRules(cwd: string): void {
    const filePath = path.join(cwd, '.superinference', 'permissions.json');
    if (!fs.existsSync(filePath)) return;

    try {
      const raw = fs.readFileSync(filePath, 'utf-8');
      const data = JSON.parse(raw);

      if (data.mode && typeof data.mode === 'string') {
        const validModes: PermissionMode[] = ['ask', 'auto-allow', 'deny-all', 'acceptEdits', 'dontAsk', 'bypassPermissions', 'plan'];
        if (validModes.includes(data.mode as PermissionMode)) {
          this.mode = data.mode as PermissionMode;
        }
      }

      if (Array.isArray(data.rules)) {
        for (const rule of data.rules) {
          if (
            rule &&
            typeof rule.tool === 'string' &&
            typeof rule.action === 'string' &&
            ['allow', 'deny', 'ask'].includes(rule.action)
          ) {
            this.rules.push({
              tool: rule.tool,
              pattern: typeof rule.pattern === 'string' ? rule.pattern : undefined,
              action: rule.action,
              reason: typeof rule.reason === 'string' ? rule.reason : undefined,
            });
          }
        }
      }
    } catch {
      // Silently ignore malformed files
    }
  }

  /**
   * Persist the current rules to `.superinference/permissions.json`.
   */
  saveRules(cwd: string): void {
    const dirPath = path.join(cwd, '.superinference');
    if (!fs.existsSync(dirPath)) {
      fs.mkdirSync(dirPath, { recursive: true });
    }

    const filePath = path.join(dirPath, 'permissions.json');
    const data = {
      mode: this.mode,
      rules: this.rules,
    };

    fs.writeFileSync(filePath, JSON.stringify(data, null, 2) + '\n', 'utf-8');
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  /**
   * Convert tool input into a flat string for pattern matching.
   */
  private inputToString(toolName: string, input: Record<string, unknown>): string {
    if (typeof input.command === 'string') return input.command;
    if (typeof input.file_path === 'string') return input.file_path;
    return JSON.stringify(input);
  }
}
