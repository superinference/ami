import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type PermissionMode = 'ask' | 'auto-allow' | 'deny-all';

export interface PermissionRule {
  tool: string;       // tool name or '*'
  pattern?: string;   // glob pattern for args (e.g., "git *")
  action: 'allow' | 'deny' | 'ask';
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

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/**
 * Commands that are inherently read-only or side-effect-free.
 * These are auto-allowed in 'ask' mode.
 */
const SAFE_COMMANDS = new Set([
  'ls', 'cat', 'head', 'tail', 'wc', 'grep', 'echo', 'pwd',
  'which', 'whoami', 'date', 'env', 'printenv', 'uname', 'hostname',
  'id', 'df', 'du', 'file', 'stat', 'readlink', 'basename', 'dirname',
  'sort', 'uniq', 'diff', 'tr', 'cut', 'less', 'more',
  'test', 'true', 'false', 'type', 'man', 'help',
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
  'docker', 'docker-compose',
  'curl', 'wget',
  'ssh', 'scp', 'rsync',
  'chmod', 'chown', 'chgrp',
  'sudo', 'su', 'doas',
  'apt', 'apt-get', 'dnf', 'yum', 'pacman', 'brew',
  'mv', 'cp', 'ln', 'mkdir', 'touch',
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
];

export function detectHardlineCommand(command: string): { blocked: boolean; description: string | null } {
  for (const { pattern, description } of HARDLINE_PATTERNS) {
    if (pattern.test(command)) {
      return { blocked: true, description };
    }
  }
  return { blocked: false, description: null };
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
function extractBaseCommands(command: string): string[] {
  const commands: string[] = [];

  // 1. Extract commands from $(...) and `...` substitutions
  const subRegex = /\$\(([^)]+)\)|`([^`]+)`/g;
  let subMatch;
  while ((subMatch = subRegex.exec(command)) !== null) {
    const inner = subMatch[1] || subMatch[2] || '';
    commands.push(...extractBaseCommands(inner));
  }

  // 2. Extract commands from subshells (...)
  const subshellRegex = /\(([^)]+)\)/g;
  let shellMatch;
  while ((shellMatch = subshellRegex.exec(command)) !== null) {
    commands.push(...extractBaseCommands(shellMatch[1]!));
  }

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
      .replace(/[.+^${}()|[\]\\]/g, '\\$&')
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

    // Check if this exact invocation was already denied
    const key = denialKey(toolName, input);
    if (this.deniedCommands.has(key)) return 'deny';

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

    // Check unsafe
    for (const cmd of bases) {
      if (UNSAFE_COMMANDS.has(cmd)) return 'unsafe';
    }

    // Check safe prefixes (e.g., "git status", "npm test")
    for (const cmd of bases) {
      if (SAFE_PREFIXES.has(cmd)) return 'safe';
    }

    // Check single-word safe commands
    for (const cmd of bases) {
      if (SAFE_COMMANDS.has(cmd)) return 'safe';
    }

    // sed without -i is read-only (prints to stdout)
    if (/\bsed\b/.test(trimmed) && !/\bsed\s+.*-[^\s]*i/.test(trimmed)) return 'safe';
    // find without -delete/-exec rm is read-only
    if (/\bfind\b/.test(trimmed) && !/\b-delete\b/.test(trimmed) && !/\b-exec\s+rm\b/.test(trimmed)) return 'safe';

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
    // Expand ~ to home directory
    let resolved = filePath;
    if (resolved === '~' || resolved.startsWith('~/') || resolved.startsWith('~\\')) {
      resolved = path.join(os.homedir(), resolved.slice(1));
    }

    // Resolve relative paths against cwd
    if (!path.isAbsolute(resolved)) {
      resolved = path.resolve(cwd, resolved);
    }

    resolved = path.normalize(resolved);

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
  // Denial tracking
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
        const validModes: PermissionMode[] = ['ask', 'auto-allow', 'deny-all'];
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
