import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { expandPath } from './utils/path';
import { extractCommandPaths, commandTargetsCredentialPath, commandTargetsSystemDir, isCredentialPath } from './tools/bash-security';
import { deobfuscateEnvExpansion } from './tools/command-normalize';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// Each mode is a DISTINCT permission semantic (names mix AMI-native kebab and
// Claude-Code wire-names — that is intentional, we match CC semantics, not a naming
// convention). `dontAsk` was removed: it was a semantic DUPLICATE of `deny-all` (both
// deny every tool call); normalizePermissionMode maps the legacy spelling to deny-all.
export const PERMISSION_MODES = ['ask', 'auto-allow', 'deny-all', 'acceptEdits', 'bypassPermissions', 'plan'] as const;
export type PermissionMode = (typeof PERMISSION_MODES)[number];

/**
 * The single acceptance boundary for a permission-mode string (discovery-driven: the
 * valid set IS {@link PERMISSION_MODES}, so a new mode is accepted everywhere the moment
 * it is added to the const — no hardcoded subset can drift, cf. ui#19). Returns the mode,
 * maps the removed legacy duplicate `dontAsk` → `deny-all` for back-compat, or null if
 * unknown. Every consumer (CLI `--permission-mode`, project config, `/permissions <mode>`)
 * MUST validate through this rather than an inline list.
 */
export function normalizePermissionMode(raw: string): PermissionMode | null {
  const m = raw.trim();
  if (m === 'dontAsk') return 'deny-all';
  return (PERMISSION_MODES as readonly string[]).includes(m) ? (m as PermissionMode) : null;
}

/**
 * Where a permission rule/update is persisted, in precedence order (later wins at
 * runtime; earlier is broader scope). Mirrors Claude's PermissionUpdateDestination:
 * - `userSettings`   — global (~/.ami), applies everywhere;
 * - `projectSettings`— shared per-project, committed (cwd/.ami);
 * - `localSettings`  — per-project but gitignored (cwd/.ami, *.local.json);
 * - `session`        — in-memory only, this run;
 * - `cliArg`         — supplied via a command-line flag.
 */
export const PERMISSION_UPDATE_DESTINATIONS = [
  'userSettings', 'projectSettings', 'localSettings', 'session', 'cliArg',
] as const;
export type PermissionUpdateDestination = (typeof PERMISSION_UPDATE_DESTINATIONS)[number];

export interface PermissionRule {
  tool: string;       // tool name or '*'
  pattern?: string;   // glob pattern for args (e.g., "git *")
  /** Glob match on named input params, e.g. { model: 'opus*' } for Tool(param:value). */
  paramPattern?: Record<string, string>;
  action: 'allow' | 'deny' | 'ask';
  reason?: string;    // optional reason for deny rules
  /** Tier this rule originated from (set when loaded from a destination file). */
  source?: PermissionUpdateDestination;
}

/**
 * Resolve the on-disk file a permission destination maps to, or null for the
 * in-memory tiers (`session`, `cliArg`). `userSettings` is global (~/.ami);
 * `projectSettings`/`localSettings` are per-project under `cwd/.ami` (local is
 * the gitignored `.local.json` variant). Mirrors Claude's per-destination
 * settings-file resolution.
 */
export function permissionDestinationPath(
  cwd: string,
  destination: PermissionUpdateDestination,
): string | null {
  switch (destination) {
    case 'userSettings':
      return path.join(os.homedir(), '.ami', 'permissions.json');
    case 'projectSettings':
      return path.join(cwd, '.ami', 'permissions.json');
    case 'localSettings':
      return path.join(cwd, '.ami', 'permissions.local.json');
    case 'session':
    case 'cliArg':
      return null; // in-memory only — never persisted
  }
}

export const BASH_CLASSIFICATIONS = ['safe', 'unsafe', 'destructive'] as const;
export type BashClassification = (typeof BASH_CLASSIFICATIONS)[number];

/** Scope for an `allow_session` grant on the agent's own config folder. */
export type SessionGrantScope = 'config-folder' | 'global-config-folder';

export interface PermissionPromptResult {
  action: 'allow_once' | 'allow_pattern' | 'allow_session' | 'deny' | 'cancelled';
  pattern?: string;
  /** Session-grant scope (for `allow_session` self-config edits). */
  scope?: SessionGrantScope;
  /**
   * Free-text the user attached when DENYING ("tell the model what to do
   * differently") — routed back to the model so it can steer the next turn.
   */
  feedback?: string;
  /**
   * Free-text the user attached when APPROVING ("tell the model what to do
   * next") — extra guidance delivered alongside the allowed result.
   */
  acceptFeedback?: string;
}

export interface PermissionPromptHandler {
  showPrompt(
    toolName: string,
    inputSummary: string,
    classification?: BashClassification,
    /**
     * Optional request abort signal. When it fires while the prompt is pending
     * the handler should resolve `{ action: 'cancelled' }` so a cancelled turn
     * doesn't leave a permission dialog hanging.
     */
    abortSignal?: AbortSignal,
  ): Promise<PermissionPromptResult>;
}

/**
 * Structured reason a permission decision needs confirmation/was denied. The
 * `type` discriminator lets the UI explain WHY approval is required instead of a
 * bare "needs approval". Mirrors Claude's permissionResult.decisionReason union.
 */
export type PermissionDecisionReason =
  | { type: 'hardline'; description: string }
  | { type: 'rule'; action: 'allow' | 'deny' | 'ask'; ruleValue?: string; source?: string }
  | { type: 'mode'; mode: PermissionMode }
  | { type: 'safetyCheck'; check: SafetyCheckKind; message: string }
  | { type: 'classifier'; name?: string; reason?: string }
  | { type: 'other'; message?: string };

export type SafetyCheckKind =
  | 'credential' | 'systemDir' | 'suspiciousPath' | 'agentConfig' | 'buildConfig' | 'destructive';

/** Build a human-readable explanation from a structured decision reason. */
export function createPermissionRequestMessage(reason: PermissionDecisionReason): string {
  switch (reason.type) {
    case 'hardline':
      return `Blocked: ${reason.description}`;
    case 'rule': {
      const from = reason.source ? ` from ${reason.source}` : '';
      const which = reason.ruleValue ? ` "${reason.ruleValue}"` : '';
      return `Permission rule${which}${from} requires ${reason.action === 'deny' ? 'denial' : 'approval'} for this command`;
    }
    case 'mode':
      return `Current permission mode (${reason.mode}) requires approval for this action`;
    case 'safetyCheck':
      return reason.message;
    case 'classifier':
      return `The safety classifier${reason.name ? ` (${reason.name})` : ''} flagged this action${reason.reason ? `: ${reason.reason}` : ''}`;
    case 'other':
      return reason.message ?? 'This action requires approval';
  }
}

export interface DenyCheckResult {
  denied: boolean;
  reason?: string;
}

/** A recency-ordered record of a denied action, for user review (`/permissions`). */
export interface RecentDenial {
  /** Human-readable command / summary of the denied action. */
  display: string;
  /** What produced the denial. */
  source: 'rule' | 'hardline' | 'classifier' | 'mode';
  /** Optional explanation. */
  reason?: string;
  timestamp: number;
}

// ---------------------------------------------------------------------------
// Permission verdict hold-over
// ---------------------------------------------------------------------------

export interface PermissionVerdict {
  decision: 'allow' | 'deny' | 'ask';
  rule?: string;
  source?: string;
  reason?: string;
}

export function verdictRank(decision: string): number {
  switch (decision) {
    case 'deny': return 3;
    case 'modify': return 2;
    case 'ask': return 2;
    case 'allow': return 1;
    default: return 0;
  }
}

export function holdsOver(held: PermissionVerdict, answer: PermissionVerdict): boolean {
  const heldRank = verdictRank(held.decision);
  const answerRank = verdictRank(answer.decision);
  if (answerRank > heldRank) return false;
  if (heldRank > answerRank) return true;
  const heldHasAttribution = held.rule !== undefined || held.source !== undefined;
  const answerHasAttribution = answer.rule !== undefined || answer.source !== undefined;
  return heldHasAttribution && !answerHasAttribution;
}

export function mostRestrictiveVerdict(verdicts: PermissionVerdict[]): PermissionVerdict {
  if (verdicts.length === 0) return { decision: 'allow' };
  let best = verdicts[0];
  for (let i = 1; i < verdicts.length; i++) {
    if (holdsOver(verdicts[i], best)) {
      best = verdicts[i];
    }
  }
  return best;
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
  'ls', 'cat', 'head', 'tail', 'wc', 'grep', 'rg', 'echo', 'pwd',
  'which', 'whoami', 'date', 'uname', 'hostname',
  'cd', // directory change — no side effects on the filesystem
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
  // Computational / text read-only utilities (side-effect-free)
  'seq', 'expr', 'printf', 'comm', 'cmp', 'numfmt', 'getconf', 'tsort', 'pr', 'fmt',
]);

const SAFE_COMMAND_FLAGS: Record<string, { allowed: Set<string>; blocked: Set<string>; strict?: boolean }> = {
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
  // ripgrep: read-only search. Block preprocessor/exec-adjacent flags that can
  // run an arbitrary command or escape the search sandbox (S7).
  rg: { allowed: new Set(['-i', '-n', '-N', '-w', '-l', '-c', '-v', '-e', '-F', '-o', '-A', '-B', '-C', '-g', '-t', '-T', '-S', '-s', '-U', '-m', '-r', '--json', '--count', '--files', '--files-with-matches', '--no-heading', '--color', '--max-count', '--glob', '--type', '--hidden', '--no-ignore', '--context', '--ignore-case', '--line-number', '--fixed-strings', '--word-regexp']), blocked: new Set(['--pre', '--pre-glob', '--hostname-bin', '--search-zip', '-z']) },
  // which: display/lookup only. Any flag outside this set falls through to a
  // prompt instead of auto-safe (S8).
  which: { allowed: new Set(['-a', '--all', '-s', '--read-alias', '--read-functions', '--skip-alias', '--skip-dot', '--skip-tilde', '-i', '--tty-only', '--version', '-v', '-V']), blocked: new Set([]), strict: true },
  tput: { allowed: new Set(['cols', 'lines', 'colors', 'setaf', 'sgr0']), blocked: new Set([]) },
  ss: { allowed: new Set(['-t', '-u', '-l', '-n', '-p', '-a']), blocked: new Set([]) },
  // sort is read-only EXCEPT `-o/--output FILE`, which writes the sorted result to
  // a file. Block the output flags; other sort flags stay safe (S: strict off).
  sort: { allowed: new Set([]), blocked: new Set(['-o', '--output']) },
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
      const dashedFlag = arg.split('=')[0];
      if (flagDef.blocked.has(arg) || flagDef.blocked.has(flag) || flagDef.blocked.has(dashedFlag)) return false;
      // strict allowlist: any flag not explicitly allowed is treated as unsafe
      // (falls through to a prompt) rather than silently auto-safe.
      if (flagDef.strict && !flagDef.allowed.has(dashedFlag)) return false;
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

/** xargs short flags that CONSUME an argument (their value is either inline —
 *  `-n1` — or the next token — `-I {}`). Getopt-accurate modeling of these is
 *  what prevents a validator-vs-xargs parser differential: a naive scanner that
 *  skips `-I` but then reads `-I`'s VALUE as the target command would validate the
 *  wrong word and wave an unsafe real target (`xargs -I echo rm` → RCE) through. */
const XARGS_ARG_TAKING = new Set(['I', 'i', 'd', 'E', 'e', 'L', 'l', 'n', 'P', 's', 'a']);

/**
 * Check whether xargs is being used with a safe (read-only) target command.
 *
 * Getopt-accurate: walks xargs' own options, correctly consuming each arg-taking
 * flag's value (inline like `-n1`, bundled like `-rI {}`, or the next token like
 * `-I {}`) and honoring a `--` end-of-options marker, so the REAL target command
 * is identified — not a flag's value. Mirrors Claude Code's xargs read-only
 * validation / bundled arg-taking-flag handling (readOnlyCommandValidation.ts).
 */
function isXargsSafe(command: string): boolean {
  const m = command.match(/\bxargs\b(.*)$/s);
  if (!m) return false;
  const tokens = m[1]!.trim().split(/\s+/).filter(Boolean);
  let k = 0;
  while (k < tokens.length) {
    const t = tokens[k]!;
    if (t === '--') { k++; break; } // end of options — next token is the target
    if (t.startsWith('--')) { k++; continue; } // long opts (value is `=`-attached)
    if (t.startsWith('-')) {
      // Short-flag cluster: scan chars; an arg-taking flag consumes the rest of
      // the token as an inline value, or (if it is the last char) the NEXT token.
      const body = t.slice(1);
      let consumedNext = false;
      for (let c = 0; c < body.length; c++) {
        const ch = body[c]!;
        if (XARGS_ARG_TAKING.has(ch)) {
          if (c === body.length - 1) consumedNext = true; // value is the next token
          break; // rest of the cluster is this flag's inline value
        }
      }
      k += consumedNext ? 2 : 1;
      continue;
    }
    break; // first non-flag token is the target command
  }
  const target = tokens[k];
  if (!target) return false;
  return XARGS_SAFE_TARGETS.has(target);
}

/**
 * Check whether a python3 -e/-c one-liner is read-only.
 * Blocks imports of os, subprocess, shutil which can mutate state.
 */
function isScriptOneLinerSafe(lang: string, code: string): boolean {
  if (lang === 'python3' || lang === 'python') {
    return !/\bimport\s+(os|subprocess|shutil)\b/.test(code) &&
           !/\bfrom\s+(os|subprocess|shutil)\b/.test(code);
  }
  if (lang === 'node') {
    return !/\brequire\s*\(\s*['"](?:child_process|fs)['"]\s*\)/.test(code) &&
           !/\bimport\b.*\bfrom\s+['"](?:child_process|fs)['"]/.test(code);
  }
  if (lang === 'ruby') {
    return !/\bsystem\b/.test(code) && !/\bexec\b/.test(code) &&
           !/`/.test(code) && !/\bIO\.popen\b/.test(code);
  }
  if (lang === 'perl') {
    return !/\bsystem\b/.test(code) && !/\bexec\b/.test(code) &&
           !/`/.test(code) && !/\bqx\b/.test(code);
  }
  return false;
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

/**
 * Check whether a command contains unquoted shell substitutions ($VAR,
 * ${VAR}, $(cmd)) but tolerate glob patterns (* ? [...]).
 * Used for safe read-only commands where globs are harmless filename expansion.
 */
export function containsShellSubstitution(command: string): boolean {
  const unquoted = stripQuotedSections(command);
  return /\$[A-Za-z_{(]/.test(unquoted);
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

/**
 * Env vars that are safe to pass through / strip before allow-rule matching. The
 * allowlist deliberately EXCLUDES PATH, LD_*, DYLD_*, PYTHONPATH, NODE_OPTIONS,
 * HOME, BASH_ENV and endpoint/daemon vars (DOCKER_HOST/KUBECONFIG) — those are an
 * escalation vector. Exported so the trust/safe-mode config scanner reuses the
 * SAME allowlist (one canonical source, no duplication).
 */
export const SAFE_ENV_VARS = new Set([
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
  // Match `rm … /` targeting the filesystem root, including a trailing token/
  // redirect (`rm -rf / 2>/dev/null`) and the catastrophic glob (`rm -rf /*`).
  // The arg run is `[^;&|]*` so it cannot span a command separator and
  // false-positive on a later bare `/` (e.g. `rm x.txt && ls /`).
  { pattern: /\brm\s+(-[a-zA-Z]*r[a-zA-Z]*\s+|[^;&|]*\s+)\/(?=\s|$|[;&|><*])/,  description: 'rm -r on filesystem root' },
  { pattern: /\brm\s+(-[a-zA-Z]*r[a-zA-Z]*\s+|[^;&|]*\s+)\/(home|etc|usr|var|bin|sbin|boot|lib|lib64|Volumes|mnt|media)\b/, description: 'rm -r on system directory' },
  { pattern: /\brm\s+(-[a-zA-Z]*r[a-zA-Z]*\s+|[^;&|]*\s+)\/private\/(etc|var|tmp|home)\b/, description: 'rm -r on macOS system directory (/private/*)' },
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
  { pattern: /\b(curl|wget|fetch|nc|ncat|http|https)\b[^\n]*(?:\b(?:169\.254\.169\.254|metadata\.google\.internal|metadata\.google\.com|100\.100\.100\.200|instance-data)\b|fd00:ec2::254)/i, description: 'cloud instance-metadata credential fetch (SSRF/credential-exfil vector)' },
  // PowerShell power-control / disk-format cmdlets — the Windows equivalents of
  // the POSIX shutdown/reboot/mkfs hardlines above (AMI runs PowerShell on Windows).
  { pattern: /\b(Stop-Computer|Restart-Computer)\b/i, description: 'PowerShell power control' },
  { pattern: /\bClear-RecycleBin\b/i, description: 'PowerShell empties the recycle bin (irreversible)' },
  { pattern: /\b(Format-Volume|Clear-Disk)\b/i, description: 'PowerShell disk format / wipe' },
];

/**
 * Expand two documented evasions of the unconditional hardline guard so the
 * patterns still fire: (a) a catastrophic base built via a bare assignment and
 * referenced by variable (`c=rm; $c -rf /`), and (b) a quoted payload inside
 * `sh -c '...'` / `bash -c "..."` / `eval '...'` where the trailing quote
 * defeats the end-anchored patterns. Returns the original command augmented with
 * the normalized/unwrapped forms (newline-joined) so pattern matching sees both.
 */
const HARDLINE_BASES = 'rm|dd|mkfs|shutdown|reboot|halt|poweroff|chmod|curl|wget|pkill|killall';

function normalizeForHardline(command: string): string {
  let out = command;

  // (a) Substitute variable references with their assigned values. Covers both a
  // bare dangerous base (`name=rm`) and a full quoted command (`c='rm -rf /'`),
  // so `eval "$c"` is normalized to the real command.
  // Optional assignment-prefix keywords (export/declare/typeset/readonly/local)
  // must not let `export c=rm; $c -rf /` bypass the variable-indirection guard.
  const ASSIGN_PREFIX = `(?:(?:export|declare|typeset|readonly|local)\\s+)?`;
  const assigns = new Map<string, string>();
  const baseAssignRe = new RegExp(
    `(?:^|;|&&|\\|\\||\\n)\\s*${ASSIGN_PREFIX}([A-Za-z_][A-Za-z0-9_]*)=(?:\\/[^\\s"';|&]*\\/)?(${HARDLINE_BASES})(?=\\s|;|&|\\||$)`,
    'g',
  );
  let a: RegExpExecArray | null;
  while ((a = baseAssignRe.exec(command)) !== null) assigns.set(a[1], a[2]);
  const quotedAssignRe = new RegExp(
    `(?:^|;|&&|\\|\\||\\n)\\s*${ASSIGN_PREFIX}([A-Za-z_][A-Za-z0-9_]*)=(['"])([\\s\\S]*?)\\2`,
    'g',
  );
  let q: RegExpExecArray | null;
  while ((q = quotedAssignRe.exec(command)) !== null) assigns.set(q[1], q[3]);
  // Multiple space-separated assignments on one statement, e.g.
  // `export a=1 c=rm` — scan every name=BASE pair within each statement
  // (prefixed by export/declare/… or a bare assignment run). Split first to
  // keep the per-pair regex simple and ReDoS-safe.
  const pairRe = new RegExp(`\\b([A-Za-z_][A-Za-z0-9_]*)=(?:\\/[^\\s"';|&]*\\/)?(${HARDLINE_BASES})(?=\\s|$|[|&])`, 'g');
  for (const rawSeg of command.split(/[;\n]|&&|\|\|/)) {
    const seg = rawSeg.replace(/^\s*(?:export|declare|typeset|readonly|local)\s+/, '');
    let pr: RegExpExecArray | null;
    pairRe.lastIndex = 0;
    while ((pr = pairRe.exec(seg)) !== null) assigns.set(pr[1], pr[2]);
  }
  if (assigns.size) {
    out = out.replace(/\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?/g, (full, name) =>
      assigns.has(name) ? assigns.get(name)! : full);
  }

  // (b) Expose command-substitution-built bases: $(echo rm)/`which rm` -> rm.
  out = out.replace(/\$\(\s*(?:echo|which|command\s+-v)\s+([^)]*)\)/g, '$1');
  out = out.replace(/`\s*(?:echo|which|command\s+-v)\s+([^`]*)`/g, '$1');

  // (c) Recursively unwrap sh/bash/eval -c quoted payloads (handles nesting),
  // appending each inner script as its own line so end-anchored patterns match.
  for (let depth = 0; depth < 5; depth++) {
    const wrapRe = /\b(?:sh|bash|zsh|dash|ksh|eval)\b[^'"]*(['"])([\s\S]*?)\1/g;
    const inners: string[] = [];
    let w: RegExpExecArray | null;
    while ((w = wrapRe.exec(out)) !== null) {
      if (w[2] && !out.includes('\n' + w[2])) inners.push(w[2]);
    }
    if (!inners.length) break;
    out += '\n' + inners.join('\n');
  }
  return out;
}

export function detectHardlineCommand(command: string): { blocked: boolean; description: string | null } {
  // Collapse quote/expansion/backslash obfuscation (r""m, r$()m, r\m → rm) so a
  // split command word cannot evade the hardline patterns.
  const deobfuscated = deobfuscateEnvExpansion(command);
  const normalized = normalizeForHardline(deobfuscated);
  // Test the original, the deobfuscated form, the whole normalized string, AND
  // each normalized line, so an end-anchored pattern (e.g. `rm -rf /$`) still
  // matches a catastrophic statement that is not the last token of a
  // multi-statement command.
  const candidates = [command, deobfuscated, normalized, ...normalized.split('\n')];
  for (const { pattern, description } of HARDLINE_PATTERNS) {
    for (const candidate of candidates) {
      if (pattern.test(candidate)) return { blocked: true, description };
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

// ---------------------------------------------------------------------------
// Build-configuration file guard (supply-chain attack surface)
// ---------------------------------------------------------------------------

/**
 * Basenames of build/CI/package-manager config files that can be weaponized
 * for supply-chain attacks. These require explicit approval even in
 * `acceptEdits` mode.
 */
const BUILD_CONFIG_BASENAMES = new Set([
  // Make
  'makefile', 'gnumakefile',
  // Docker
  'dockerfile', 'docker-compose.yml', 'docker-compose.yaml', '.dockerignore',
  // CI
  '.gitlab-ci.yml', '.gitlab-ci.yaml', 'jenkinsfile',
  // Python packaging
  'pyproject.toml', 'setup.py', 'setup.cfg',
  // Rust
  'cargo.toml',
  // Go
  'go.mod', 'go.sum',
  // Node/npm/yarn — package.json scripts run arbitrary code on npm install
  'package.json', 'package-lock.json',
  '.npmrc', '.yarnrc', '.yarnrc.yml',
  'pnpm-lock.yaml', 'yarn.lock',
  // Git/tool config files that redirect fetches or run arbitrary commands:
  // `.gitmodules` points submodules at attacker URLs, `.ripgreprc` can set a
  // `--pre` preprocessor command (RCE), `.mailmap` rewrites identities. Mirrors
  // Claude's DANGEROUS_FILES list.
  '.gitmodules', '.ripgreprc', '.mailmap',
  // Env files
  '.env', '.env.local', '.env.production',
  // Shell startup / login files
  '.bashrc', '.bash_profile', '.bash_login', '.bash_logout',
  '.zshrc', '.zshenv', '.zprofile', '.zlogin', '.zlogout',
  '.profile', '.login',
  // Git config
  '.gitconfig',
]);

/**
 * Path patterns (checked against the full normalized path) for build config
 * files whose identity depends on directory context, not just the basename.
 */
const BUILD_CONFIG_PATH_PATTERNS: RegExp[] = [
  // GitHub Actions workflows
  /\/\.github\/workflows\/[^/]+\.ya?ml$/,
  // CircleCI
  /\/\.circleci\/config\.ya?ml$/,
  // Husky git hooks
  /\/\.husky\//,
  // Git hooks directory — writing here runs code on git operations
  /\/\.git\/hooks\//,
  // SSH directory — authorized_keys, config, known_hosts, private keys
  /\/\.ssh\//,
  // GPG keys and config
  /\/\.gnupg\//,
  // AWS credentials and config
  /\/\.aws\//,
  // Kubernetes configs
  /\/\.kube\//,
  // Docker credentials and config
  /\/\.docker\//,
];

/**
 * Return true if `filePath` names a build-configuration file that should
 * require explicit approval even in `acceptEdits` mode.
 */
export function isBuildConfigFile(filePath: string): boolean {
  const normalized = filePath.replace(/\\/g, '/');
  const basename = path.basename(normalized).toLowerCase();

  if (BUILD_CONFIG_BASENAMES.has(basename)) return true;

  for (const pattern of BUILD_CONFIG_PATH_PATTERNS) {
    if (pattern.test(normalized)) return true;
  }

  return false;
}

// ---------------------------------------------------------------------------
// Agent-own-config guard — edits to the agent's own settings/config dirs need
// explicit approval even in acceptEdits (prompt-injection / self-reconfig vector)
// ---------------------------------------------------------------------------

/**
 * Config/IDE directory segments that must never be silently auto-edited: the
 * agent's own settings (`.ami`, `.ami-plugin`, `.claude`), IDE config (`.vscode`,
 * `.idea`) and the git metadata dir (`.git`). Writing into these can reconfigure
 * the agent, add hooks/commands/skills, or weaponize IDE tasks. Mirrors Claude's
 * DANGEROUS_DIRECTORIES + isClaudeConfigFilePath guards.
 */
const AGENT_CONFIG_DIR_SEGMENTS = ['.ami', '.ami-plugin', '.claude', '.vscode', '.idea', '.git'];

/** Settings/config basenames that are sensitive wherever they live. */
const AGENT_CONFIG_BASENAMES = new Set([
  'settings.json', 'settings.local.json', 'permissions.json', 'hooks.json',
  'plugin.json', 'config.json', 'mcp.json', '.mcp.json',
]);

/**
 * Return true if `filePath` targets the agent's own config/settings/IDE/git
 * directories or a sensitive settings file — such edits require explicit approval
 * even in acceptEdits mode, mirroring Claude's isInClaudeFolder / isAgentSettingsFile.
 */
export function isAgentConfigPath(filePath: string): boolean {
  const normalized = filePath.replace(/\\/g, '/');
  const segments = normalized.split('/');
  for (const seg of segments) {
    if (AGENT_CONFIG_DIR_SEGMENTS.includes(seg)) return true;
  }
  if (AGENT_CONFIG_BASENAMES.has(path.basename(normalized))) return true;
  return false;
}

// ---------------------------------------------------------------------------
// Suspicious path-pattern detection (canonicalization-bypass vectors)
// ---------------------------------------------------------------------------

/** UNC path (`\\server\share` or `//server/share`) — remote-resource / creds vector. */
function isUncPath(p: string): boolean {
  if (/^\\\\[^\\]/.test(p)) return true;      // \\server
  if (/^\/\/[^/]/.test(p)) return true;       // //server
  return false;
}

/**
 * Detect path-canonicalization bypass vectors that string-based blocklists miss,
 * applied on ALL platforms (NTFS can be mounted on Linux/macOS via ntfs-3g):
 * - NTFS Alternate Data Streams (`file.txt::$DATA`, `.bashrc:$DATA`);
 * - 8.3 short names (`GIT~1`, `SETTIN~1.JSON`);
 * - long-path prefixes (`\\?\`, `\\.\`, `//?/`, `//./`);
 * - trailing dots/spaces Windows strips (`.git.`, `settings.json `);
 * - DOS device-name suffixes (`.git.CON`, `settings.json.PRN`);
 * - triple-dot path components (`/.../`);
 * - UNC paths.
 *
 * The ADS colon check is restricted to the explicit `:$`/`::` stream markers so
 * legitimate colons in POSIX filenames are not false-positived (a deliberate
 * tightening of Claude's platform-gated "any colon after the drive letter" rule).
 * Mirrors Claude's hasSuspiciousWindowsPathPattern.
 */
export function hasSuspiciousPathPattern(p: string): boolean {
  if (!p) return false;
  // NTFS ADS stream markers (explicit, to stay cross-platform safe).
  if (/::|:\$/.test(p)) return true;
  // 8.3 short names.
  if (/~\d/.test(p)) return true;
  // Long-path prefixes.
  if (p.startsWith('\\\\?\\') || p.startsWith('\\\\.\\') || p.startsWith('//?/') || p.startsWith('//./')) {
    return true;
  }
  // Trailing dots / spaces.
  if (/[.\s]+$/.test(p)) return true;
  // DOS device-name suffixes.
  if (/\.(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])$/i.test(p)) return true;
  // Triple-dot path component.
  if (/(^|[/\\])\.{3,}([/\\]|$)/.test(p)) return true;
  // UNC path.
  if (isUncPath(p)) return true;
  return false;
}

/**
 * Walk the FULL symlink chain for `filePath` hop-by-hop and return the original
 * path, every intermediate symlink target, AND the final resolved path — so a
 * deny/boundary check matches at ANY hop (e.g. `test.txt → /etc/passwd` yields
 * both, closing the symlink-escape vector where only the final realpath is
 * checked). Bounded by `maxDepth` (SYMLOOP_MAX=40) with a visited-set loop guard.
 * Mirrors Claude's getPathsForPermissionCheck. Non-existent / unreadable hops stop
 * the walk gracefully and still return what was collected.
 */
export function getPathsForPermissionCheck(filePath: string, maxDepth = 40): string[] {
  const results: string[] = [];
  const visited = new Set<string>();
  let current = filePath;
  for (let depth = 0; depth < maxDepth; depth++) {
    const abs = path.isAbsolute(current) ? current : path.resolve(current);
    const norm = path.normalize(abs);
    if (visited.has(norm)) break; // circular symlink loop
    visited.add(norm);
    if (!results.includes(norm)) results.push(norm);
    let target: string;
    try {
      const stat = fs.lstatSync(norm);
      if (!stat.isSymbolicLink()) break;
      target = fs.readlinkSync(norm);
    } catch {
      break; // non-existent or unreadable — stop, keep what we have
    }
    // Resolve a relative link target against the link's own directory.
    current = path.isAbsolute(target) ? target : path.resolve(path.dirname(norm), target);
  }
  return results;
}

/** System directories that should never be written to. */
const SYSTEM_DIRECTORIES = [
  '/etc', '/usr', '/bin', '/sbin', '/var',
  '/sys', '/proc', '/dev', '/boot', '/lib', '/lib64',
  '/opt', '/Volumes', '/mnt', '/media', '/run/media',
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
  'watch', 'ionice', 'setarch', 'taskset', 'chrt', 'strace', 'ltrace',
]);

/**
 * Unwrap shell control structures (for/while/until/if/case) so that the
 * body commands are what gets classified, not the keywords themselves.
 *
 * for f in *.ts; do cat "$f"; done  →  cat "$f"
 * while read l; do echo "$l"; done  →  echo "$l"
 * if test -f x; then cat x; else echo no; fi  →  cat x\necho no
 *
 * Operates recursively — nested loops are unwrapped.
 */
export function unwrapShellControlStructures(command: string): string {
  let result = command;
  // For/while/until loops: extract body between do...done
  // Handles: for ...; do BODY; done | while ...; do BODY; done
  const loopRe = /\b(?:for|while|until)\b[^;]*;\s*do\b([\s\S]*?)\bdone\b/g;
  let prev = '';
  while (prev !== result) {
    prev = result;
    result = result.replace(loopRe, (_match, body: string) => body.trim());
  }
  // If/then/else/elif/fi: extract bodies from then/else/elif branches
  const ifRe = /\bif\b[^;]*;\s*then\b([\s\S]*?)\bfi\b/g;
  prev = '';
  while (prev !== result) {
    prev = result;
    result = result.replace(ifRe, (_match, body: string) => {
      // Split on else/elif to get all branch bodies
      return body.replace(/\b(?:elif\b[^;]*;\s*then|else)\b/g, ';').trim();
    });
  }
  return result;
}

/**
 * Extract all command names from a shell string.
 * Handles: pipes, semicolons, &&/||, command substitution $(...),
 * subshells (...), shell wrappers (env, sudo, bash -c),
 * shell control structures (for/while/if), and
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

  // 0. Unwrap shell control structures so for/while/if bodies are classified
  command = unwrapShellControlStructures(command);

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

  // 3. Split on pipes, semicolons, newlines, && and ||
  const segments = command.split(/\s*(?:\|\||&&|[|;\n])\s*/);

  for (const segment of segments) {
    const trimmed = segment.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;

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
      const wrapper = cmd;
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
      // Skip a leading duration/priority operand so the WRAPPED command is
      // reached: `timeout 30 ls` -> ls, `nice 10 ls` -> ls (mirrors
      // stripSafeWrappers in bash-security). Without this the base command is
      // mis-detected as the numeric token and classified unsafe (S7).
      if ((wrapper === 'timeout' || wrapper === 'stdbuf') && i < tokens.length && /^[\d.]/.test(tokens[i]!)) {
        i++;
      } else if (wrapper === 'nice' && i < tokens.length && /^\d+$/.test(tokens[i]!)) {
        i++;
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

function allSegmentsKnown(command: string): boolean {
  const unwrapped = unwrapShellControlStructures(command);
  const segments = unwrapped.split(/\s*(?:\|\||&&|[|;])\s*/).filter(s => s.trim());
  if (segments.length <= 1) return true;
  for (const seg of segments) {
    const tokens = seg.trim().split(/\s+/);
    let i = 0;
    while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) i++;
    if (i >= tokens.length) continue;
    const cmdTokens = tokens.slice(i);
    let found = false;
    for (let j = 1; j <= Math.min(cmdTokens.length, 3); j++) {
      const prefix = cmdTokens.slice(0, j).join(' ');
      if (SAFE_COMMANDS.has(prefix) || SAFE_PREFIXES.has(prefix)) { found = true; break; }
    }
    if (!found) return false;
  }
  return true;
}

/**
 * Git read-only write-guard — shared by the bash classifier AND the bash-result
 * cache (`isReadOnlyBashCommand`) so there is ONE place that knows which git
 * forms actually mutate state. A git subcommand can appear in a "read-only"
 * allowlist yet still WRITE: creating a ref (`git branch <name>`, `git tag
 * <name>`), rewriting remotes (`git remote add/remove/set-url/rename`), writing
 * a file via `--output`/`-o`, or setting config (`git config key value`).
 * Claude Code's readOnlyCommandValidation.ts rejects each of these before
 * auto-approval; this mirrors that.
 *
 * `tokens` is a single git invocation beginning at the `git` token. Returns
 * true when the invocation is a genuine read-only form, false when it writes.
 */
export function isGitInvocationReadOnly(tokens: string[]): boolean {
  if (tokens[0] !== 'git') return true; // not a git invocation — not our concern

  // Skip git global options (and the value they consume) to reach the subcommand.
  const valueGlobals = new Set(['-C', '-c', '--git-dir', '--work-tree', '--namespace']);
  let i = 1;
  while (i < tokens.length) {
    const t = tokens[i]!;
    if (!t.startsWith('-')) break;
    if (valueGlobals.has(t)) { i += 2; continue; } // `--opt value`
    i += 1;                                         // flag or `--opt=value`
  }
  const sub = tokens[i];
  if (!sub) return true; // bare `git` prints help — harmless
  const rest = tokens.slice(i + 1);

  // A file-writing output flag turns ANY read subcommand into a write.
  for (const a of rest) {
    if (a === '-o' || a === '--output' || a.startsWith('--output=')) return false;
  }

  if (sub === 'branch' || sub === 'tag') {
    const writeFlags = sub === 'branch'
      ? new Set(['-d', '-D', '--delete', '-m', '-M', '--move', '-c', '-C', '--copy',
          '-f', '--force', '--set-upstream-to', '-u', '--unset-upstream', '--edit-description'])
      : new Set(['-d', '--delete', '-a', '--annotate', '-s', '--sign', '-m', '--message',
          '-F', '--file', '-f', '--force']);
    // Read flags whose FOLLOWING token is a value (not a ref name / positional).
    const valueFlags = new Set(['--contains', '--no-contains', '--merged', '--no-merged',
      '--points-at', '--sort', '--format', '--color', '--column']);
    let hasList = false;
    let hasPositional = false;
    let endOfOptions = false; // after a bare `--`, everything is a positional
    for (let k = 0; k < rest.length; k++) {
      const a = rest[k]!;
      if (endOfOptions) { hasPositional = true; continue; } // post-`--` tokens are ref names
      if (a === '--') { endOfOptions = true; continue; } // `git branch -- -l` → `-l` is a ref name
      if (a === '-l' || a === '--list') { hasList = true; continue; }
      if (writeFlags.has(a)) return false;
      if (a.startsWith('-')) {
        if (!a.includes('=') && valueFlags.has(a)) k++; // skip the consumed value
        continue;
      }
      hasPositional = true; // a bare ref name
    }
    // A bare positional without --list is ref CREATION (a write).
    return !(hasPositional && !hasList);
  }

  if (sub === 'remote') {
    const first = rest.find(a => !a.startsWith('-'));
    if (!first) return true; // bare `git remote` / `git remote -v`
    return first === 'show' || first === 'get-url';
  }

  if (sub === 'config') {
    const writeSubops = ['--add', '--unset', '--unset-all', '--replace-all',
      '--remove-section', '--rename-section', '--edit', '-e'];
    if (rest.some(a => writeSubops.includes(a))) return false;
    const readSubops = ['--get', '--get-all', '--get-regexp', '--get-urlmatch', '--list', '-l'];
    if (rest.some(a => readSubops.includes(a))) return true;
    // `git config key value` writes; `git config key` reads a single value.
    const positionals = rest.filter(a => !a.startsWith('-'));
    return positionals.length < 2;
  }

  return true; // other read subcommands (status/log/diff/show/...) stay read-only
}

/**
 * Whole-command git read-only guard for the bash classifier. Splits a (possibly
 * compound) command into segments, validates every git invocation via
 * isGitInvocationReadOnly, and refuses a compound that changes directory before
 * running git (`cd /attacker && git status`) — a sandbox escape where git runs
 * attacker-planted .git/hooks from the new cwd (Claude's cd+git guard).
 */
export function isGitReadOnlyCommandSafe(command: string): boolean {
  const unwrapped = unwrapShellControlStructures(command);
  const segments = unwrapped.split(/\s*(?:\|\||&&|[|;])\s*/).filter(s => s.trim());
  let hasCd = false;
  let hasGit = false;
  for (const seg of segments) {
    const tokens = seg.trim().split(/\s+/);
    let i = 0;
    while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) i++;
    const cmd = tokens[i];
    if (cmd === 'cd' || cmd === 'pushd' || cmd === 'popd' || cmd === 'chdir') hasCd = true;
    if (cmd === 'git') {
      hasGit = true;
      if (!isGitInvocationReadOnly(tokens.slice(i))) return false;
    }
  }
  if (hasCd && hasGit) return false;
  return true;
}

/**
 * Detect a compound command that changes directory (`cd`/`pushd`/`chdir`/
 * PowerShell `Set-Location`) AND then performs a file-writing output redirection
 * (`>`/`>>` to a file, not an fd-dup like `2>&1`). The validator classifies the
 * redirect target against the launch cwd, but at runtime it resolves against the
 * NEW cwd — a path-resolution/TOCTOU desync that can write outside the workspace
 * (`cd /outside && echo payload > config`). Claude requires manual approval for
 * this shape (pathValidation.ts cd+write guard, PowerShell Set-Location desync).
 */
export function commandHasCdWriteDesync(command: string): boolean {
  const unwrapped = unwrapShellControlStructures(command);
  const segments = unwrapped.split(/\s*(?:\|\||&&|[|;])\s*/).filter(s => s.trim());
  let hasCd = false;
  let hasRedirect = false;
  for (const seg of segments) {
    const tokens = seg.trim().split(/\s+/);
    let i = 0;
    while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) i++;
    const cmd = tokens[i];
    if (cmd === 'cd' || cmd === 'pushd' || cmd === 'chdir' ||
        (cmd && cmd.toLowerCase() === 'set-location')) hasCd = true;
    // A file redirect: optional leading fd, `>`/`>>`, then a target that is not
    // an fd-dup (`&1`). Quoted sections are stripped so `echo "a > b"` is ignored.
    const unquoted = stripQuotedSections(seg);
    if (/\d*>>?\s*(?![&\s])\S/.test(unquoted)) hasRedirect = true;
  }
  return hasCd && hasRedirect;
}

/**
 * Count the directory-change segments (`cd`/`pushd`/`chdir`/PowerShell
 * `Set-Location`) in a (possibly compound) command. More than one directory
 * change in a single command is an unusual navigation pattern that defeats
 * cwd-relative path validation and must require approval (Claude's multi-cd
 * guard, bashCommandHelpers.ts).
 */
export function countDirectoryChanges(command: string): number {
  const unwrapped = unwrapShellControlStructures(command);
  const segments = unwrapped.split(/\s*(?:\|\||&&|[|;])\s*/).filter(s => s.trim());
  let count = 0;
  for (const seg of segments) {
    const tokens = seg.trim().split(/\s+/);
    let i = 0;
    while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) i++;
    const cmd = tokens[i];
    if (cmd === 'cd' || cmd === 'pushd' || cmd === 'chdir' ||
        (cmd && cmd.toLowerCase() === 'set-location')) count++;
  }
  return count;
}

/**
 * Match a simple glob-like pattern against a string.
 * Supports '*' as a wildcard for any sequence of characters.
 */
const _patternCache = new Map<string, RegExp>();
/**
 * Match a glob-like pattern (only `*` is special, expanding to any run of
 * characters; every other metachar is escaped) against a value. This is the
 * canonical wildcard matcher reused by MCP-server allowlists, permission rules
 * and the HTTP-hook URL allowlist — do not re-implement it per site.
 */
export function wildcardMatch(pattern: string, value: string): boolean {
  return matchPattern(pattern, value);
}

function matchPattern(pattern: string, value: string): boolean {
  if (pattern.includes('\x00') || value.includes('\x00')) return false;
  if (pattern === value) return true;
  if (pattern === '*') return true;

  let regex = _patternCache.get(pattern);
  if (!regex) {
    const escaped = pattern
      .replace(/[.+^${}()|[\]\\?]/g, '\\$&')
      .replace(/\*/g, '.*');
    // dotAll (`s`): `*` must also match across embedded newlines, so a rule like
    // `*rm -rf*` still matches a multi-line command and a deny cannot be bypassed
    // by splitting the command across lines (Claude's dotAll wildcard matching).
    regex = new RegExp(`^${escaped}$`, 's');
    _patternCache.set(pattern, regex);
  }
  return regex.test(value);
}

/**
 * Normalize line continuations (backslash + newline) in a bash command.
 * Bash treats `\<newline>` as a line join — stripping it reveals the
 * actual command that will execute, preventing pattern-bypass via
 * multi-line splitting.
 */
export function normalizeLineContinuations(cmd: string): string {
  // A `\<newline>` is a true line-continuation ONLY when the backslash run
  // immediately before the newline is ODD (the last backslash escapes the
  // newline). With an EVEN run the backslashes pair up as literal escapes and
  // the newline is a REAL command separator that must be KEPT — otherwise
  // `echo \\<newline>rm -rf /` would be mis-joined into one command and the
  // second statement could slip past an end-anchored pattern. Mirrors Claude's
  // odd/even backslash-count discrimination (utils/bash/commands.ts L96-139).
  let out = '';
  for (let i = 0; i < cmd.length; i++) {
    if (cmd[i] === '\n') {
      let bs = 0;
      let j = out.length - 1;
      while (j >= 0 && out[j] === '\\') { bs++; j--; }
      if (bs % 2 === 1) {
        out = out.slice(0, out.length - 1); // drop the escaping backslash, join lines
        continue; // and drop the newline
      }
      out += '\n'; // even run (or none) — real separator, keep it
      continue;
    }
    out += cmd[i];
  }
  return out;
}

const COMPOUND_SPLIT_RE = /\s*(?:\|\||&&|[;|])\s*/;

/**
 * Split a compound bash command into individual sub-commands.
 * First unwraps shell control structures (for/while/if), then splits
 * on &&, ||, ;, and | operators.
 */
function splitCompoundCommand(cmd: string): string[] {
  const unwrapped = unwrapShellControlStructures(cmd);
  return unwrapped.split(COMPOUND_SPLIT_RE).map(s => s.trim()).filter(s => s.length > 0);
}

/**
 * Extract the inner command string(s) from `$(...)` command substitutions and
 * `` `...` `` backticks, recursively (nested substitutions included).
 *
 * Used ONLY for deny/ask matching (M1): a DENIED command must not be able to hide
 * inside a substitution (e.g. `echo $(rm -rf x)` or `` echo `rm -rf x` ``). Allow
 * matching never consults this — decomposition can only ADD denies, never broaden
 * an allow.
 */
function extractCommandSubstitutions(command: string): string[] {
  const results: string[] = [];
  const walk = (s: string): void => {
    for (let i = 0; i < s.length; i++) {
      if (s[i] === '$' && i + 1 < s.length && s[i + 1] === '(') {
        const end = findMatchingParen(s, i + 2);
        if (end !== -1) {
          const inner = s.slice(i + 2, end);
          results.push(inner);
          walk(inner); // nested $(...) / backticks
          i = end;
        }
      } else if (s[i] === '`') {
        const end = s.indexOf('`', i + 1);
        if (end !== -1) {
          const inner = s.slice(i + 1, end);
          results.push(inner);
          walk(inner);
          i = end;
        }
      }
    }
  };
  walk(command);
  return results;
}

/**
 * Match a bash command against a permission rule pattern, handling:
 * 1. Line continuation normalization (\<newline> → join)
 * 2. Compound command decomposition:
 *    - allow rules: ALL sub-commands must match (prevents "git status && rm -rf /")
 *    - deny rules: ANY sub-command matching triggers deny
 */
export function stripEnvVarPrefix(cmd: string, safeEnvOnly = false): string {
  // Allow-rule matching passes safeEnvOnly=true: only provably-harmless env vars
  // (SAFE_ENV_VARS) are stripped, so an escalation prefix like `PATH=/evil` or
  // `LD_PRELOAD=x` STAYS in the command and breaks the match — forcing a prompt
  // instead of silently auto-approving. Deny/ask matching strips ALL assignments
  // aggressively so a denied command cannot hide behind any env-var prefix.
  // Mirrors Claude's split (allow = safe-list strip, deny = strip-all).
  let stripped: string;
  if (safeEnvOnly) {
    // Drop leading assignments one at a time, but STOP at the first non-safe var
    // (it stays, breaking any allow match). The command tail is preserved exactly.
    let s = cmd;
    for (;;) {
      const m = s.match(/^\s*([A-Za-z_]\w*)=(?:"[^"]*"|'[^']*'|\S+)\s+/);
      if (!m || !SAFE_ENV_VARS.has(m[1]!)) break;
      s = s.slice(m[0].length);
    }
    stripped = s;
  } else {
    // eslint-disable-next-line security/detect-unsafe-regex -- anchored, disjoint alternation
    stripped = cmd.replace(/^(\s*[A-Za-z_]\w*=(?:"[^"]*"|'[^']*'|\S+)\s+)+/, '');
  }
  // Privilege/shell wrappers (sudo, env, bash -c, eval, …) are stripped ONLY for
  // deny/ask matching — so a deny rule still catches `sudo python evil.py`. They
  // are NEVER stripped for allow matching (safeEnvOnly): auto-approving the inner
  // command would silently grant the escalated/wrapped form (`sudo git status`
  // must not match an allow rule for `git status`). Mirrors Claude's wrapper
  // handling (strip-for-deny, keep-for-allow).
  if (!safeEnvOnly) {
    const tokens = stripped.split(/\s+/);
    let i = 0;
    while (i < tokens.length && SHELL_WRAPPERS.has(tokens[i]!)) {
      i++;
      while (i < tokens.length && tokens[i]!.startsWith('-')) i++;
      while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) i++;
      // A numeric operand belongs to the wrapper, not the wrapped command:
      // `timeout 5 CMD` / `timeout 1m CMD` (duration), `nice 10 CMD` /
      // `ionice 2 CMD` (priority). Skip it so the deny/ask match reaches the real
      // command (`timeout 5 rm -rf /` must still hit a `rm` deny rule).
      while (i < tokens.length && /^\d+[smhd]?$/.test(tokens[i]!)) i++;
    }
    if (i > 0 && i < tokens.length) stripped = tokens.slice(i).join(' ');
  }
  return stripped;
}

// ---------------------------------------------------------------------------
// Scoped command-prefix derivation for "allow always" permission rules
// ---------------------------------------------------------------------------

const ENV_VAR_ASSIGN_RE = /^[A-Za-z_][A-Za-z0-9_]*=/;
// Subcommand shape: lowercase alnum, hyphen-separated (e.g. `commit`, `rev-parse`).
// The `[a-z0-9]*` run and the `-[a-z0-9]+` groups start with disjoint chars
// (alnum vs `-`), so there is no ambiguous overlap / ReDoS backtracking.
// eslint-disable-next-line security/detect-unsafe-regex
const SUBCOMMAND_SHAPE_RE = /^[a-z][a-z0-9]*(-[a-z0-9]+)*$/;

/**
 * Bare single-word prefixes that must NEVER be offered as an "allow always"
 * scope: shells and arg-executing wrappers (a `bash:*` / `env:*` / `sudo:*` rule
 * auto-approves arbitrary code via `-c` or by wrapping any command) plus
 * cross-platform code interpreters (a `node:*` / `python:*` rule would
 * auto-allow arbitrary code execution). Mirrors Claude's BARE_SHELL_PREFIXES +
 * NEVER_SUGGEST / CROSS_PLATFORM_CODE_EXEC denylists.
 */
export const BARE_SHELL_PREFIXES = new Set([
  'sh', 'bash', 'zsh', 'fish', 'csh', 'tcsh', 'ksh', 'dash', 'cmd', 'powershell', 'pwsh',
  // wrappers that exec their args as a command
  'env', 'xargs', 'nice', 'stdbuf', 'nohup', 'timeout', 'time',
  // privilege escalation
  'sudo', 'doas', 'pkexec',
  // cross-platform code-exec interpreters
  'node', 'deno', 'bun', 'python', 'python3', 'ruby', 'perl', 'php', 'osascript', 'iex',
]);

/**
 * Commands that take subcommands or depth-scoped flags: a bare single-word
 * `git:*` / `docker:*` rule would auto-allow dangerous subcommands forever
 * (`git push --force`, `docker run`). For these we refuse a bare-root suggestion
 * and require the two-word `command subcommand` form (or fall back to the exact
 * command). Mirrors Claude's DEPTH_RULES + "fig-spec declares subcommands"
 * bare-root guard (staticPrefix.ts L142-156).
 */
export const SUBCOMMAND_AWARE_ROOTS = new Set([
  'git', 'gh', 'docker', 'kubectl', 'az', 'aws', 'gcloud', 'npm', 'pnpm', 'yarn',
  'cargo', 'go', 'pip', 'pip3', 'brew', 'apt', 'apt-get', 'dnf', 'yum', 'pacman',
  'systemctl', 'terraform', 'helm', 'make',
]);

/**
 * Two-word scoped prefix ("command subcommand", e.g. `git commit`, `npm run`).
 * Skips leading SAFE_ENV_VARS assignments; returns null if an unsafe env var is
 * seen (so matching falls back to the exact command), if there is no second
 * token, or if the second token is not subcommand-shaped (a flag, path, number
 * or filename). Mirrors Claude's getSimpleCommandPrefix.
 */
export function getSimpleCommandPrefix(command: string): string | null {
  const tokens = command.trim().split(/\s+/).filter(Boolean);
  if (tokens.length === 0) return null;
  let i = 0;
  while (i < tokens.length && ENV_VAR_ASSIGN_RE.test(tokens[i]!)) {
    if (!SAFE_ENV_VARS.has(tokens[i]!.split('=')[0]!)) return null;
    i++;
  }
  const remaining = tokens.slice(i);
  if (remaining.length < 2) return null;
  if (!SUBCOMMAND_SHAPE_RE.test(remaining[1]!)) return null;
  return remaining.slice(0, 2).join(' ');
}

/**
 * First-word fallback prefix (e.g. `npm`, `cargo`). Skips safe env vars, rejects
 * paths/flags/numbers/filenames and anything in BARE_SHELL_PREFIXES. Mirrors
 * Claude's getFirstWordPrefix.
 */
export function getFirstWordPrefix(command: string): string | null {
  const tokens = command.trim().split(/\s+/).filter(Boolean);
  let i = 0;
  while (i < tokens.length && ENV_VAR_ASSIGN_RE.test(tokens[i]!)) {
    if (!SAFE_ENV_VARS.has(tokens[i]!.split('=')[0]!)) return null;
    i++;
  }
  const cmd = tokens[i];
  if (!cmd) return null;
  if (!SUBCOMMAND_SHAPE_RE.test(cmd)) return null;
  if (BARE_SHELL_PREFIXES.has(cmd)) return null;
  return cmd;
}

/**
 * Derive a narrow, scoped "allow always" prefix for a bash command, or null if
 * none is safe to suggest (the caller then falls back to the exact command).
 * Prefers the two-word `command subcommand` form; falls back to the first word
 * ONLY when that root is neither subcommand-aware nor a shell/interpreter/
 * wrapper. This is the single canonical prefix-derivation helper — it replaces
 * ad-hoc "first N words" heuristics and enforces the bare-root + NEVER_SUGGEST
 * guards in one place.
 */
export function suggestAllowPrefix(command: string): string | null {
  const two = getSimpleCommandPrefix(command);
  if (two) return two;
  const one = getFirstWordPrefix(command);
  if (one && !SUBCOMMAND_AWARE_ROOTS.has(one)) return one;
  return null;
}

// ---------------------------------------------------------------------------
// Permission-rule string format — `Tool` / `Tool(specifier)`
// ---------------------------------------------------------------------------

/** Parsed form of a canonical permission-rule string. */
export interface PermissionRuleValue {
  toolName: string;
  ruleContent?: string;
}

/**
 * Legacy / cross-framework tool-name aliases → AMI canonical tool names. A
 * permission rule written with a Claude-Code capitalized name (`Bash`, `Edit`)
 * or an AMI legacy name resolves to the same canonical AMI tool, so rules keep
 * matching after a rename or when imported from a Claude settings file.
 */
const PERMISSION_TOOL_ALIASES: Record<string, string> = {
  Bash: 'bash', Read: 'file_read', Edit: 'file_edit', Write: 'file_write',
  MultiEdit: 'file_edit', Glob: 'glob', Grep: 'grep', LS: 'list_dir',
  WebFetch: 'web_fetch', WebSearch: 'web_search', NotebookEdit: 'notebook_edit',
  Task: 'task', TodoWrite: 'todo_write',
  // AMI legacy aliases
  KillShell: 'task_stop', BashOutputTool: 'task_output', AgentOutputTool: 'task_output',
};

/** Canonicalize a tool name used in a permission rule (legacy/alias → current). */
export function normalizeToolName(name: string): string {
  return PERMISSION_TOOL_ALIASES[name] ?? name;
}

/** True if `str[index]` is escaped (preceded by an odd number of backslashes). */
function isEscapedAt(str: string, index: number): boolean {
  let backslashCount = 0;
  let j = index - 1;
  while (j >= 0 && str[j] === '\\') { backslashCount++; j--; }
  return backslashCount % 2 !== 0;
}

function findFirstUnescapedChar(str: string, char: string): number {
  for (let i = 0; i < str.length; i++) {
    if (str[i] === char && !isEscapedAt(str, i)) return i;
  }
  return -1;
}

function findLastUnescapedChar(str: string, char: string): number {
  for (let i = str.length - 1; i >= 0; i--) {
    if (str[i] === char && !isEscapedAt(str, i)) return i;
  }
  return -1;
}

/** Escape backslashes then parentheses so rule content survives round-tripping. */
export function escapeRuleContent(content: string): string {
  return content.replace(/\\/g, '\\\\').replace(/\(/g, '\\(').replace(/\)/g, '\\)');
}

/** Inverse of {@link escapeRuleContent} (parens first, then backslashes). */
export function unescapeRuleContent(content: string): string {
  return content.replace(/\\\(/g, '(').replace(/\\\)/g, ')').replace(/\\\\/g, '\\');
}

/**
 * Parse a canonical permission-rule string into `{ toolName, ruleContent? }`,
 * honouring escaped parentheses and canonicalizing the tool name. `Tool()` and
 * `Tool(*)` collapse to a tool-wide rule (no ruleContent). Mirrors Claude's
 * permissionRuleValueFromString.
 */
export function permissionRuleValueFromString(ruleString: string): PermissionRuleValue {
  const openParenIndex = findFirstUnescapedChar(ruleString, '(');
  if (openParenIndex === -1) return { toolName: normalizeToolName(ruleString) };
  const closeParenIndex = findLastUnescapedChar(ruleString, ')');
  if (closeParenIndex === -1 || closeParenIndex <= openParenIndex) {
    return { toolName: normalizeToolName(ruleString) };
  }
  if (closeParenIndex !== ruleString.length - 1) {
    return { toolName: normalizeToolName(ruleString) };
  }
  const toolName = ruleString.substring(0, openParenIndex);
  const rawContent = ruleString.substring(openParenIndex + 1, closeParenIndex);
  if (!toolName) return { toolName: normalizeToolName(ruleString) };
  if (rawContent === '' || rawContent === '*') return { toolName: normalizeToolName(toolName) };
  return { toolName: normalizeToolName(toolName), ruleContent: unescapeRuleContent(rawContent) };
}

/** Serialize a {@link PermissionRuleValue} back to its canonical string form. */
export function permissionRuleValueToString(ruleValue: PermissionRuleValue): string {
  if (!ruleValue.ruleContent) return ruleValue.toolName;
  return `${ruleValue.toolName}(${escapeRuleContent(ruleValue.ruleContent)})`;
}

/**
 * Validate a user-supplied permission-rule string, returning an actionable
 * diagnostic. Catches empty rules, mismatched / empty parentheses, empty tool
 * names, and bash/file pattern mistakes (`:*` placement, `:*` in file patterns).
 * Mirrors Claude's validatePermissionRule, minus the Claude-specific
 * uppercase-first convention (AMI canonical tool names are lowercase).
 */
export function validatePermissionRule(rule: string): {
  valid: boolean; error?: string; suggestion?: string; examples?: string[];
} {
  if (!rule || rule.trim() === '') {
    return { valid: false, error: 'Permission rule cannot be empty' };
  }
  let openCount = 0, closeCount = 0;
  for (let i = 0; i < rule.length; i++) {
    if (rule[i] === '(' && !isEscapedAt(rule, i)) openCount++;
    if (rule[i] === ')' && !isEscapedAt(rule, i)) closeCount++;
  }
  if (openCount !== closeCount) {
    return {
      valid: false, error: 'Mismatched parentheses',
      suggestion: 'Ensure all opening parentheses have matching closing parentheses',
    };
  }
  // Empty parentheses (escape-aware).
  for (let i = 0; i < rule.length - 1; i++) {
    if (rule[i] === '(' && rule[i + 1] === ')' && !isEscapedAt(rule, i)) {
      const toolName = rule.substring(0, rule.indexOf('('));
      if (!toolName) {
        return {
          valid: false, error: 'Empty parentheses with no tool name',
          suggestion: 'Specify a tool name before the parentheses',
        };
      }
      return {
        valid: false, error: 'Empty parentheses',
        suggestion: `Either specify a pattern or use just "${toolName}" without parentheses`,
        examples: [`${toolName}`, `${toolName}(some-pattern)`],
      };
    }
  }
  const parsed = permissionRuleValueFromString(rule);
  if (!parsed.toolName || parsed.toolName.length === 0) {
    return { valid: false, error: 'Tool name cannot be empty' };
  }
  // Bash prefix-rule mistakes: `:*` must be at the end and non-empty.
  if ((parsed.toolName === 'bash' || parsed.toolName === 'Bash') && parsed.ruleContent !== undefined) {
    const content = parsed.ruleContent;
    if (content === ':*') {
      return {
        valid: false, error: 'Prefix cannot be empty before :*',
        suggestion: 'Specify a command prefix before :*',
        examples: ['bash(npm:*)', 'bash(git:*)'],
      };
    }
    if (content.includes(':*') && !content.endsWith(':*')) {
      return {
        valid: false, error: 'The :* pattern must be at the end',
        suggestion: 'Move :* to the end for prefix matching, or use * for wildcard matching',
        examples: ['bash(npm run:*) - prefix matching', 'bash(npm run *) - wildcard matching'],
      };
    }
  }
  return { valid: true };
}

/**
 * Match a single bash sub-command against a rule pattern.
 *
 * For deny/ask rules we ALSO test a de-obfuscated form (r""m, r''m, "rm", r\m →
 * rm) so a command-word split with empty quotes / backslashes / quotes cannot
 * slip a real command past a user deny or ask rule (S3). Allow rules stay
 * STRICT — they only match the literal stripped form — so de-obfuscation can
 * never *broaden* an allow into auto-approving an obfuscated command.
 */
function matchBashSub(pattern: string, sub: string, ruleAction: 'allow' | 'deny' | 'ask'): boolean {
  // Allow rules strip only SAFE_ENV_VARS (an unsafe env prefix must break the
  // match → prompt); deny/ask rules strip ALL env vars so no prefix can hide a
  // denied command.
  const strippedRaw = stripEnvVarPrefix(sub, /* safeEnvOnly */ ruleAction === 'allow');
  if (matchPattern(pattern, strippedRaw)) return true;
  if (ruleAction === 'allow') return false;
  // Strip env prefix FIRST (handles quoted values like FOO="a b"), THEN collapse
  // obfuscation and drop quote/backtick characters from the remaining command.
  const deob = deobfuscateEnvExpansion(strippedRaw).replace(/["'`]/g, '');
  return deob !== strippedRaw && matchPattern(pattern, deob);
}

function matchBashPattern(pattern: string, command: string, ruleAction: 'allow' | 'deny' | 'ask'): boolean {
  const normalized = normalizeLineContinuations(command);
  const subCommands = splitCompoundCommand(normalized);

  let baseMatch: boolean;
  if (subCommands.length <= 1) {
    // Use the single sub-command (which has been unwrapped from control
    // structures) rather than the original normalized string, so that
    // e.g. "for f in *.ts; do cat $f; done" matches a "cat *" rule.
    const effective = subCommands.length === 1 ? subCommands[0] : normalized;
    baseMatch = matchBashSub(pattern, effective, ruleAction);
  } else if (ruleAction === 'allow') {
    baseMatch = subCommands.every(sub => matchBashSub(pattern, sub, 'allow'));
  } else {
    // deny / ask: any sub-command match triggers
    baseMatch = subCommands.some(sub => matchBashSub(pattern, sub, ruleAction));
  }
  if (baseMatch) return true;

  // M1: deny/ask rules ALSO decompose `$(...)` / backtick command substitutions
  // (recursively) and match each extracted inner command, so a denied command
  // cannot hide inside a substitution (e.g. `echo $(rm -rf x)`). Allow matching
  // stays STRICT above — decomposition can only ADD denies, never broaden allows.
  if (ruleAction === 'allow') return false;
  for (const inner of extractCommandSubstitutions(normalized)) {
    for (const sub of splitCompoundCommand(inner)) {
      if (matchBashSub(pattern, sub, ruleAction)) return true;
    }
  }
  return false;
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
  private _disableBypass = false;
  /** Extra absolute paths/dirs the USER has explicitly granted read access to,
   *  OUTSIDE the launch cwd — e.g. via `/add-dir`, `--context-file`, `--image`, or an
   *  approved out-of-workspace read prompt. The launch cwd is always allowed (passed
   *  per-call to the path checks); everything else outside it is confined unless listed
   *  here. This is what makes "point the agent at /some/other/image.png" work while a
   *  silent `cat ../../other-project/secret` stays blocked. */
  private allowedReadRoots: Set<string> = new Set();

  // Denial circuit breaker — abort after too many consecutive or total denials
  private consecutiveDenials = 0;
  private totalDenials = 0;
  private readonly MAX_CONSECUTIVE = 3;
  private readonly MAX_TOTAL = 20;

  constructor(mode?: PermissionMode, rules?: PermissionRule[], allowedRoots?: string[]) {
    if (mode) this.mode = mode;
    if (rules) this.rules = [...rules];
    if (allowedRoots) for (const r of allowedRoots) this.registerAllowedRoot(r);
  }

  /**
   * Grant read access to an absolute path (file or directory) that lies OUTSIDE the
   * launch cwd. Sources: `/add-dir`, `--context-file`/`--image` launch flags, and
   * user-approved out-of-workspace read prompts. Stored resolved + normalized so the
   * containment check is a simple prefix match. Idempotent; ignores empty input.
   */
  registerAllowedRoot(p: string): void {
    if (!p || typeof p !== 'string') return;
    try {
      const abs = path.isAbsolute(p) ? p : path.resolve(p);
      this.allowedReadRoots.add(path.normalize(abs));
    } catch { /* non-fatal: a bad path just isn't granted */ }
  }

  /**
   * Revoke a previously-granted read root (inverse of {@link registerAllowedRoot}).
   * Normalizes `p` the same way so a path granted via `/add-dir` can be dropped by
   * `/remove-dir`, letting the workspace boundary SHRINK, not just widen. Returns
   * true if a matching root was removed. The launch cwd is granted per-call and is
   * never in this set, so it can never be revoked here.
   */
  removeAllowedRoot(p: string): boolean {
    if (!p || typeof p !== 'string') return false;
    try {
      const abs = path.isAbsolute(p) ? p : path.resolve(p);
      return this.allowedReadRoots.delete(path.normalize(abs));
    } catch {
      return false;
    }
  }

  /** The user-granted read roots (absolute, normalized). For inspection/tests. */
  getAllowedReadRoots(): string[] {
    return [...this.allowedReadRoots];
  }

  // -------------------------------------------------------------------------
  // Auto-mode / classifier denial store (recency-ordered, bounded, displayable)
  // -------------------------------------------------------------------------

  private _recentDenials: RecentDenial[] = [];
  private readonly MAX_RECENT_DENIALS = 100;

  /** Recency-ordered (newest-first) snapshot of recorded denials. */
  getRecentDenials(): readonly RecentDenial[] {
    return this._recentDenials;
  }

  /** Clear the recorded-denial log (but not the circuit-breaker counters). */
  clearRecentDenials(): void {
    this._recentDenials = [];
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

    // dangerouslyDisableSandbox always requires explicit confirmation
    if (toolName === 'bash' && input.dangerouslyDisableSandbox === true) {
      if (this.mode === 'deny-all') return 'deny';
      return 'ask';
    }

    // Fast path: mode-level overrides — but in auto-allow mode, explicit
    // deny rules still fire (Gap #32). M9: a matching deny UNCONDITIONALLY wins
    // over the auto-allow, regardless of rule order (an earlier allow can no
    // longer shadow a later deny). paramPattern rules stay skipped on this
    // fast-path (unchanged behavior); everything else is auto-allowed.
    if (this.mode === 'auto-allow') {
      if (this.anyDenyMatches(toolName, input, /* skipParamPattern */ true)) return 'deny';
      return 'allow';
    }
    if (this.mode === 'deny-all') return 'deny';

    if (this.mode === 'bypassPermissions') {
      // Explicit user deny rules are BYPASS-IMMUNE: a `{tool, action:'deny'}` rule
      // fires even in bypassPermissions mode, before the blanket auto-allow — so a
      // user's hard stop is never silently overridden (Claude orders deny/safety
      // checks before the bypass allow, permissions.ts L1169-1281).
      if (this.anyDenyMatches(toolName, input)) return 'deny';
      // User-configured content-specific `ask` rules are ALSO bypass-immune: a
      // `{tool, action:'ask'}` rule still prompts, before the blanket allow, so an
      // explicit "always confirm this" is never silently overridden by bypass mode.
      if (this.anyAskMatches(toolName, input)) return 'ask';
      const safetyPaths = [
        '.git/', '.ami/', '.bashrc', '.zshrc', '.profile', '.gitconfig',
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
      if (editTools.has(toolName)) {
        // Build-config files can be weaponized for supply-chain attacks —
        // require explicit approval even in acceptEdits mode.
        const filePath = (input.file_path || input.path || '') as string;
        if (filePath && isBuildConfigFile(filePath)) return 'ask';
        // The agent's OWN config/settings/IDE/git dirs are a self-reconfig /
        // prompt-injection vector — never silently auto-edit them.
        if (filePath && isAgentConfigPath(filePath)) return 'ask';
        // A path-canonicalization bypass (ADS, 8.3 short name, trailing dot,
        // UNC, triple-dot, DOS device) could sneak past the basename guards above.
        if (filePath && hasSuspiciousPathPattern(filePath)) return 'ask';
        return 'allow';
      }
      if (toolName === 'bash' && typeof input.command === 'string') {
        const classification = this.classifyBashCommand(input.command);
        if (classification === 'safe') return 'allow';
      }
    }

    // Check if this exact invocation was already denied
    const key = denialKey(toolName, input);
    if (this.deniedCommands.has(key)) return 'deny';

    // Determine if this is a destructive bash command (used to guard allow
    // rules from auto-approving dangerous commands via wildcards).
    const isDestructiveBash = this.mode === 'ask' && toolName === 'bash' &&
      typeof input.command === 'string' &&
      this.classifyBashCommand(input.command) === 'destructive';

    // Credential/secret access always confirms in rule-evaluated modes (ask and
    // acceptEdits), even under a wildcard allow — unless there is an explicit
    // deny rule (which must still win below).
    if ((this.mode === 'ask' || this.mode === 'acceptEdits') &&
        toolName === 'bash' && typeof input.command === 'string' &&
        commandTargetsCredentialPath(input.command) && !this.hasExplicitDeny(toolName, input)) {
      return 'ask';
    }
    // System directory write guard — commands that write to /etc, /usr, /boot,
    // etc. always require explicit approval, even under a wildcard allow rule.
    if ((this.mode === 'ask' || this.mode === 'acceptEdits') &&
        toolName === 'bash' && typeof input.command === 'string' &&
        commandTargetsSystemDir(input.command)) {
      return 'ask';
    }

    // Policy memo: return cached decision for this exact tool+input if available
    if (this._memo) {
      const inputHash = this._hashInput(input);
      const cached = this._memo.get(toolName, inputHash);
      if (cached !== null) return cached;
    }

    // M9: a matching deny rule UNCONDITIONALLY wins over any allow/ask, regardless
    // of rule order. Scan the whole rule list for a deny first; only if none matches
    // do allow/ask rules get evaluated in order (first match wins) below.
    if (this.anyDenyMatches(toolName, input)) {
      if (this._memo) this._memo.set(toolName, this._hashInput(input), 'deny');
      return 'deny';
    }

    // Evaluate allow/ask rules in order (first match wins). Deny rules were already
    // resolved above and unconditionally win, so they are skipped here.
    for (const rule of this.rules) {
      if (rule.action === 'deny') continue;
      const toolMatches = this.toolNameMatches(rule.tool, toolName);
      if (!toolMatches) continue;
      // Tool(param:value) matching — all named params must match their globs.
      if (rule.paramPattern && !this.paramMatches(rule.paramPattern, input)) continue;

      if (rule.pattern) {
        const inputStr = this.inputToString(toolName, input);
        const matched = toolName === 'bash' && typeof input.command === 'string'
          ? matchBashPattern(rule.pattern, inputStr, rule.action)
          : matchPattern(rule.pattern, inputStr);
        if (matched) {
          const effective = (rule.action === 'allow' && isDestructiveBash) ? 'ask' : rule.action;
          if (this._memo) this._memo.set(toolName, this._hashInput(input), effective);
          return effective;
        }
      } else {
        // No pattern -- rule applies to the whole tool (param match already gated above)
        const effective = (rule.action === 'allow' && isDestructiveBash) ? 'ask' : rule.action;
        if (this._memo) this._memo.set(toolName, this._hashInput(input), effective);
        return effective;
      }
    }

    // Default: 'ask' mode means non-read-only tools require a prompt
    if (this._memo) this._memo.set(toolName, this._hashInput(input), 'ask');
    return 'ask';
  }

  /**
   * Like {@link check} but also returns a STRUCTURED reason explaining the verdict,
   * so a UI can tell the user WHY approval is required (or why it was denied)
   * instead of a bare "needs approval". Reuses check() for the decision (single
   * source of truth — no parallel decision path) and derives the reason from the
   * same guard predicates. `reason`/`message` are undefined for an `allow`.
   */
  async checkWithReason(
    toolName: string,
    input: Record<string, unknown>,
  ): Promise<{ decision: 'allow' | 'deny' | 'ask'; reason?: PermissionDecisionReason; message?: string }> {
    const decision = await this.check(toolName, input);
    if (decision === 'allow') return { decision };
    const reason = this.deriveDecisionReason(toolName, input, decision);
    return { decision, reason, message: createPermissionRequestMessage(reason) };
  }

  /** Best-effort attribution of a deny/ask verdict to a structured reason. */
  private deriveDecisionReason(
    toolName: string,
    input: Record<string, unknown>,
    decision: 'deny' | 'ask',
  ): PermissionDecisionReason {
    const command = typeof input.command === 'string' ? input.command : undefined;
    if (toolName === 'bash' && command) {
      const hardline = detectHardlineCommand(command);
      if (hardline.blocked) return { type: 'hardline', description: hardline.description ?? 'blocked command' };
    }
    // An explicit matching rule.
    for (const rule of this.rules) {
      if (!this.toolNameMatches(rule.tool, toolName)) continue;
      if (rule.paramPattern && !this.paramMatches(rule.paramPattern, input)) continue;
      const matches = rule.pattern
        ? (toolName === 'bash' && command
            ? matchBashPattern(rule.pattern, command, rule.action)
            : matchPattern(rule.pattern, this.inputToString(toolName, input)))
        : true;
      if (matches && (rule.action === decision)) {
        return { type: 'rule', action: rule.action, ruleValue: rule.pattern ? `${rule.tool}(${rule.pattern})` : rule.tool, source: rule.reason };
      }
    }
    // Safety checks (credential / system-dir / suspicious-path / config files).
    if (toolName === 'bash' && command) {
      if (commandTargetsCredentialPath(command)) {
        return { type: 'safetyCheck', check: 'credential', message: 'Command accesses a credential/secret path and requires approval' };
      }
      if (commandTargetsSystemDir(command)) {
        return { type: 'safetyCheck', check: 'systemDir', message: 'Command writes to a system directory and requires approval' };
      }
      if (this.classifyBashCommand(command) === 'destructive') {
        return { type: 'safetyCheck', check: 'destructive', message: 'Command is destructive and requires approval' };
      }
    }
    const filePath = (input.file_path || input.path) as string | undefined;
    if (filePath) {
      if (isBuildConfigFile(filePath)) return { type: 'safetyCheck', check: 'buildConfig', message: 'Edit targets a build/CI config file and requires approval' };
      if (isAgentConfigPath(filePath)) return { type: 'safetyCheck', check: 'agentConfig', message: "Edit targets the agent's own config and requires approval" };
      if (hasSuspiciousPathPattern(filePath)) return { type: 'safetyCheck', check: 'suspiciousPath', message: 'Path uses a canonicalization-bypass pattern and requires approval' };
    }
    // Fall back to the mode.
    return { type: 'mode', mode: this.mode };
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
      // paramPattern rules match structured named params, which this string-only
      // check cannot evaluate — skip them (they are enforced by check()).
      if (rule.paramPattern) continue;
      if (rule.action === 'deny' && this.toolNameMatches(rule.tool, toolName)) {
        if (!rule.pattern) {
          return { denied: true, reason: rule.reason || `Denied by rule for ${rule.tool}` };
        }
        const matched = toolName === 'bash'
          ? matchBashPattern(rule.pattern, input, 'deny')
          : matchPattern(rule.pattern, input);
        if (matched) {
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
    const result = this.classifyBashCommandCore(command);
    // Any command that touches a credential/secret path — even a read like
    // `cat ~/.ssh/id_rsa` — must never be auto-'safe'; elevate to a prompt.
    // Never downgrade a stronger verdict (destructive/unsafe) here.
    if (result === 'safe' && commandTargetsCredentialPath(command.trim())) return 'unsafe';
    return result;
  }

  private classifyBashCommandCore(command: string): BashClassification {
    // Strip shell comment lines — they are no-ops in bash
    const stripped = command.split('\n').filter(l => !l.trim().startsWith('#')).join('\n');
    const trimmed = stripped.trim();
    if (!trimmed) return 'safe';

    // Unwrap shell control structures so classification sees the body
    // commands, not the loop/if keywords. The original trimmed is kept
    // for regex checks that need the full command syntax (like rm -rf).
    const unwrapped = unwrapShellControlStructures(trimmed);
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

    // PowerShell destructive cmdlets (AMI runs PowerShell on Windows). Power-control
    // / disk-format variants are blocked unconditionally by HARDLINE above; these
    // are the recursive/forced deletions that need an explicit-approval prompt.
    if (/\bRemove-Item\b[^\n]*-(Recurse|Force)\b/i.test(trimmed)) return 'destructive';
    if (/\bClear-Content\b/i.test(trimmed)) return 'destructive';

    // Destructive git operations — discard local changes irreversibly
    if (/\bgit\s+(checkout|restore)\s+.*--\s*\./.test(trimmed)) return 'destructive';
    if (/\bgit\s+restore\s+\./.test(trimmed)) return 'destructive';
    if (/\bgit\s+stash\s+drop\b/.test(trimmed)) return 'destructive';
    if (/\bgit\s+stash\s+clear\b/.test(trimmed)) return 'destructive';
    if (/\bgit\s+reset\s+--merge\b/.test(trimmed)) return 'destructive';
    if (/\bgit\s+reset\s+--keep\b/.test(trimmed)) return 'destructive';

    // A `cd`/`Set-Location` followed by a file-writing redirection resolves the
    // target against the NEW cwd at runtime — a path-desync the validator can't see.
    // Require manual approval (Claude's cd+write guard).
    if (commandHasCdWriteDesync(trimmed)) return 'unsafe';

    // More than one directory change in a single command defeats cwd-relative path
    // validation (the validator can't track which cwd each later statement runs in).
    // Require approval (Claude's multi-cd guard).
    if (countDirectoryChanges(trimmed) > 1) return 'unsafe';

    // xargs anywhere in the command (even behind a pipe like `ls | xargs …`) must
    // target a read-only command. Validate it up-front so a safe leading command
    // can't short-circuit the classifier to 'safe' before the xargs target is
    // checked (parser-differential RCE: `ls | xargs -I echo ./evil.sh`).
    if (bases.includes('xargs') && !isXargsSafe(unwrapped)) return 'unsafe';

    // Flag-aware checks for commands that are safe/destructive depending on flags
    if (/\bsed\s+.*-[^\s]*i/.test(trimmed)) return 'unsafe'; // sed -i modifies files in place
    if (/\bfind\s+.*-delete\b/.test(trimmed)) return 'destructive';
    if (/\bfind\s+.*-(?:exec|ok)(dir)?\s+rm\b/.test(trimmed)) return 'destructive';
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

    // Docker: only read-only subcommands are safe. Reject ANY `$`-substitution
    // first — the validator sees the literal `$VAR`/`$(cmd)` text but the shell
    // expands it at runtime, a parser differential that can smuggle a non-read
    // subcommand or secret past this check (Claude's isDockerSafe `$` rejection).
    if (/\bdocker\s+/.test(trimmed)) {
      if (containsShellSubstitution(unwrapped)) return 'unsafe';
      const dockerMatch = trimmed.match(/\bdocker\s+(\S+)/);
      if (dockerMatch && DOCKER_SAFE_SUBCOMMANDS.has(dockerMatch[1])) {
        return 'safe';
      }
      return 'unsafe';
    }

    // Check safe prefixes (e.g., "git status", "npm test")
    for (const cmd of bases) {
      if (SAFE_PREFIXES.has(cmd)) {
        // A git read-only prefix can still smuggle a write (ref creation via
        // `git branch/tag <name>`, remote mutation, `--output` file write) or a
        // sandbox escape (`cd /attacker && git ...` runs planted .git/hooks).
        // Validate the full command before auto-allowing.
        if (cmd.startsWith('git ') && !isGitReadOnlyCommandSafe(unwrapped)) return 'unsafe';
        // Reject `$`-substitution in ANY auto-allowed compound/external read
        // command (git, gh, npm, go, cargo, make, …) — the validator sees the
        // literal `$VAR`/`$(cmd)` text but the shell expands it at runtime, a
        // parser differential that can smuggle a non-read subcommand or leak a
        // secret via an error message past this check. Mirrors Claude's blanket
        // `$`-rejection on auto-allowed external commands (isGitSafe/isDockerSafe
        // and the PowerShell readOnlyValidation variable-expansion guard).
        if (containsShellSubstitution(unwrapped)) return 'unsafe';
        if (allSegmentsKnown(unwrapped)) return 'safe';
        break;
      }
    }

    // Check single-word safe commands with flag validation
    for (const cmd of bases) {
      if (SAFE_COMMANDS.has(cmd)) {
        const tokens = unwrapped.split(/\s+/);
        const cmdIdx = tokens.indexOf(cmd);
        const args = cmdIdx >= 0 ? tokens.slice(cmdIdx + 1) : [];

        if (!validateCommandFlags(cmd, args)) return 'unsafe';

        // `date`: a bare positional like `010203042024` SETS the system clock.
        // Only a format string (leading '+') is read-only — any other positional
        // (or -s/--set/-f) must prompt. Mirrors Claude's date dangerous-callback.
        if (cmd === 'date') {
          for (const a of args) {
            if (a.startsWith('-')) {
              if (/^(-s|--set|-f)$/.test(a)) return 'unsafe';
              continue;
            }
            const bare = a.replace(/^["']/, ''); // tolerate a quoted "+..." format
            if (!bare.startsWith('+')) return 'unsafe'; // clock-setting positional
          }
        }

        // jq/yq: additional dangerous-construct check
        if ((cmd === 'jq' || cmd === 'yq') && isJqDangerous(args)) return 'unsafe';

        // `hostname`: a bare positional SETS the system hostname (`hostname newname`).
        // Only flag-driven reads (`hostname -f`, `-i`, …) are safe. Mirrors Claude's
        // hostname positional-argument guard.
        if (cmd === 'hostname') {
          for (const a of args) {
            if (!a.startsWith('-')) return 'unsafe'; // a positional sets the hostname
          }
        }

        // `tput`: capability names are positionals. Only read-only capabilities are
        // safe; the dangerous ones (reset/init/clear/mc5/pfloc/… and the `-S` stdin
        // batch flag) alter terminal state or run arbitrary sequences. Strict: any
        // capability not on the allowlist prompts. Mirrors Claude's tput guard.
        if (cmd === 'tput') {
          const allowedCaps = SAFE_COMMAND_FLAGS['tput']!.allowed;
          for (const a of args) {
            if (a === '-S') return 'unsafe'; // batch-from-stdin: unbounded capabilities
            if (a.startsWith('-')) continue; // `-T<term>` etc. handled by flag validation
            if (!allowedCaps.has(a)) return 'unsafe'; // unlisted capability
          }
        }

        // `lsof`: `+` options write/augment state — notably `+m`/`+m<path>` emits a
        // mount-supplement file. Only `-` read flags are safe. Mirrors Claude's lsof
        // `+`-option rejection.
        if (cmd === 'lsof') {
          for (const a of args) {
            if (a.startsWith('+')) return 'unsafe';
          }
        }

        // xargs: must target a safe command
        if (cmd === 'xargs' && !isXargsSafe(unwrapped)) return 'unsafe';

        // Block shell substitutions ($VAR, $(cmd)) but allow glob patterns
        if (containsShellSubstitution(unwrapped)) return 'unsafe';

        if (allSegmentsKnown(unwrapped)) return 'safe';
        break;
      }
    }

    // sed without -i is read-only (prints to stdout)
    if (/\bsed\b/.test(trimmed) && !/\bsed\s+.*-[^\s]*i/.test(trimmed)) return 'safe';
    // find is read-only only without any command-running primary
    // (-delete, -exec, -execdir, -ok, -okdir).
    if (/\bfind\b/.test(trimmed) && !/(^|\s)-delete\b/.test(trimmed) && !/(^|\s)-exec(dir)?\b/.test(trimmed) && !/(^|\s)-ok(dir)?\b/.test(trimmed)) return 'safe';

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
  /**
   * Resolve a path for permission checks: expand ~, make absolute against cwd, and
   * resolve symlinks (so an in-cwd symlink pointing at /etc or a sibling dir cannot
   * smuggle access past the prefix check). A not-yet-existing path (a write target)
   * keeps its normalized form.
   */
  private resolveForCheck(filePath: string, cwd: string): string {
    let resolved = expandPath(filePath, cwd);
    try {
      resolved = fs.realpathSync(resolved);
    } catch {
      // File may not exist yet (write operation) — keep the normalized path
    }
    return resolved;
  }

  /**
   * FORBIDDEN paths are never readable by the agent — even inside the workspace, even in
   * a granted root, even with explicit user consent: credential/secret files and system
   * directories. Mirrors the credential write-guard so reads are no narrower than writes
   * (.aws/credentials, .git-credentials, .kube/config, .ssh keys, .env, .pem/.key, /etc…).
   */
  private isForbiddenReadPath(resolved: string): boolean {
    if (isCredentialPath(resolved)) return true;
    const basename = path.basename(resolved).toLowerCase();
    const sensitiveNames = ['.env', 'id_rsa', 'id_ed25519', 'id_ecdsa', 'id_dsa'];
    const sensitiveExts = ['.pem', '.key', '.p12', '.pfx', '.keystore'];
    const sensitivePatterns = [/^\.env$/, /^\.env\.local$/, /^\.env\.production$/,
      /^credentials\.json$/, /^secrets\.ya?ml$/, /^\.netrc$/];
    if (sensitiveNames.some(s => basename === s) ||
        sensitiveExts.some(e => basename.endsWith(e)) ||
        sensitivePatterns.some(p => p.test(basename))) {
      return true;
    }
    for (const sysDir of SYSTEM_DIRECTORIES) {
      if (resolved === sysDir || resolved.startsWith(sysDir + path.sep)) return true;
    }
    return false;
  }

  /**
   * WORKSPACE containment: a resolved path is inside the workspace iff it is under the
   * launch cwd OR under a user-granted read root (registerAllowedRoot). This is the
   * boundary that confines the agent to where it was started — sibling, parent, and
   * other-$HOME paths are NOT inside unless the user explicitly granted them.
   */
  private isWithinWorkspace(resolved: string, cwd: string): boolean {
    // Strip trailing separators so a cwd/root passed as "/ws/" doesn't become "/ws//"
    // (path.normalize preserves a trailing slash), which would break the prefix match.
    const strip = (p: string) => p.replace(/[/\\]+$/, '') || p;
    const r = strip(resolved);
    const ncwd = strip(path.normalize(cwd));
    if (r === ncwd || r.startsWith(ncwd + path.sep)) return true;
    for (const root of this.allowedReadRoots) {
      const nr = strip(root);
      if (r === nr || r.startsWith(nr + path.sep)) return true;
    }
    return false;
  }

  /**
   * Hard boundary for the tools' path guard: true only when a path is BOTH non-forbidden
   * AND inside the workspace (launch cwd or a user-granted root). Confines the agent to
   * its launch directory — reading elsewhere in $HOME (e.g. ~/dev/other-project), a
   * sibling, or a parent is denied unless the user granted it via /add-dir or an approved
   * out-of-workspace prompt. NOTE: callers that support user consent should prefer
   * classifyReadAccess() so a user-pointed out-of-workspace file can be approved.
   */
  isPathAllowed(filePath: string, cwd: string): boolean {
    const resolved = this.resolveForCheck(filePath, cwd);
    if (this.isForbiddenReadPath(resolved)) return false;
    return this.isWithinWorkspace(resolved, cwd);
  }

  /**
   * Three-way read classification that drives the consent flow:
   *   'deny'   — forbidden (credential/system): never read, even with consent.
   *   'allow'  — inside the workspace (cwd or a granted root): read without prompting.
   *   'prompt' — outside the workspace but NOT forbidden: the user must consent. This is
   *              what lets a file the user explicitly points at (e.g. an image under
   *              /some/other/path) work, while a silent `cat ../../other/secret` is gated.
   *              On approval the caller should registerAllowedRoot(dirname(path)).
   */
  classifyReadAccess(filePath: string, cwd: string): 'allow' | 'deny' | 'prompt' {
    const resolved = this.resolveForCheck(filePath, cwd);
    if (this.isForbiddenReadPath(resolved)) return 'deny';
    if (this.isWithinWorkspace(resolved, cwd)) return 'allow';
    return 'prompt';
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
   * Record a denial. Always advances the circuit-breaker counters; when a
   * `display` string is supplied it is ALSO appended (newest-first, bounded) to
   * the recency-ordered denial log so the user can later review exactly what was
   * blocked (mirrors Claude's getAutoModeDenials / AutoModeDenial store). This is
   * the single canonical denial recorder — do not add a parallel one. `source`
   * distinguishes a model/classifier block from a rule/hardline/mode deny.
   */
  recordDenial(display?: string, source: RecentDenial['source'] = 'rule', reason?: string): void {
    this.consecutiveDenials++;
    this.totalDenials++;
    if (display !== undefined) {
      this._recentDenials.unshift({ display, source, reason, timestamp: Date.now() });
      if (this._recentDenials.length > this.MAX_RECENT_DENIALS) {
        this._recentDenials.length = this.MAX_RECENT_DENIALS;
      }
    }
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
   * Returns true if the mode was set successfully, false if blocked by policy.
   */
  setMode(mode: PermissionMode): boolean {
    if (this._disableBypass && (mode === 'bypassPermissions' || mode === 'auto-allow')) {
      return false; // blocked by managed policy
    }
    this.mode = mode;
    return true;
  }

  /**
   * Enable or disable the bypass-permissions guard.
   * When enabled, setMode() rejects 'bypassPermissions' and 'auto-allow'.
   */
  setDisableBypass(disable: boolean): void {
    this._disableBypass = disable;
  }

  /**
   * Check whether bypass is disabled by managed policy.
   */
  isDisableBypass(): boolean {
    return this._disableBypass;
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
   * Does an EXPLICIT deny apply to this call — a previously-recorded denial, or
   * a matching rule (first match wins, like check()) whose action is 'deny'?
   *
   * Unlike check(), this deliberately ignores mode-level decisions
   * ('deny-all' / default 'ask'). Read-only tools are safe by
   * design and must keep running under those modes (detached mode defaults to
   * 'deny-all' yet relies on read-only tools), so the read-only path uses this
   * to honor only a rule/denial the user set explicitly — closing the gap where
   * an explicit `{tool:'grep', action:'deny'}` rule was silently bypassed.
   */
  /**
   * Match a rule's tool field against a tool name, supporting glob/prefix
   * wildcards (e.g. `github:*` to scope a rule to one MCP server) in addition to
   * exact names and `*`. Lets allow/deny rules target a namespace without the
   * over-broad `{tool:'*'}` grant.
   */
  private toolNameMatches(ruleTool: string, toolName: string): boolean {
    // Canonicalize legacy/cross-framework names (e.g. a `Bash`/`Edit` rule
    // imported from a Claude settings file matches AMI's `bash`/`file_edit`).
    const rt = normalizeToolName(ruleTool);
    return rt === '*' || rt === toolName
      || (rt.includes('*') && matchPattern(rt, toolName));
  }

  /** Coerce a persisted paramPattern into a validated Record<string,string>,
   *  dropping non-string values (a malformed {"model":123} must not load as a
   *  rule that matches nothing-or-everything). Returns undefined if nothing valid. */
  private sanitizeParamPattern(raw: unknown): Record<string, string> | undefined {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return undefined;
    const entries = Object.entries(raw as Record<string, unknown>);
    if (entries.length === 0) return undefined;
    // Strict: if ANY value is non-string the pattern is malformed. Keeping only
    // the valid subset would silently widen the rule (fewer constraints → more
    // matches), so reject the whole thing (loadRules then skips the rule).
    const out: Record<string, string> = {};
    for (const [k, v] of entries) {
      if (typeof v !== 'string') return undefined;
      out[k] = v;
    }
    return out;
  }

  /** All named-param globs in a rule's paramPattern must match the tool input. */
  private paramMatches(paramPattern: Record<string, string>, input: Record<string, unknown>): boolean {
    for (const [key, glob] of Object.entries(paramPattern)) {
      const v = input[key];
      if (typeof v !== 'string' || !matchPattern(glob, v)) return false;
    }
    return true;
  }

  /**
   * M9: does ANY deny rule match this call, regardless of rule order? A matching
   * deny UNCONDITIONALLY wins over allow/ask. Unlike hasExplicitDeny (first-match,
   * returns the first matching rule's action), this scans the WHOLE rule list for a
   * deny, so an earlier allow can never shadow a later deny.
   *
   * `skipParamPattern` preserves the auto-allow fast-path's long-standing behavior
   * of ignoring structured paramPattern rules (they are enforced by the ordered
   * rule loop in check()'s rule-evaluated modes, not by the auto-allow fast-path).
   */
  private anyDenyMatches(
    toolName: string,
    input: Record<string, unknown>,
    skipParamPattern = false,
  ): boolean {
    for (const rule of this.rules) {
      if (rule.action !== 'deny') continue;
      if (skipParamPattern && rule.paramPattern) continue;
      if (!this.toolNameMatches(rule.tool, toolName)) continue;
      if (rule.paramPattern && !this.paramMatches(rule.paramPattern, input)) continue;
      if (rule.pattern) {
        const inputStr = this.inputToString(toolName, input);
        const matched = toolName === 'bash' && typeof input.command === 'string'
          ? matchBashPattern(rule.pattern, inputStr, 'deny')
          : matchPattern(rule.pattern, inputStr);
        if (matched) return true;
      } else {
        return true;
      }
    }
    return false;
  }

  /**
   * Does any user-configured `ask` rule match this tool+input? Mirrors
   * {@link anyDenyMatches} but for content-specific `ask` rules. Used to keep
   * explicit `ask` rules BYPASS-IMMUNE: a user's `{tool, action:'ask'}` rule must
   * still prompt in bypassPermissions mode, before the blanket auto-allow (Claude
   * evaluates ask/safety-check rules BEFORE the bypass allow, permissions.ts
   * L1169-1281).
   */
  private anyAskMatches(toolName: string, input: Record<string, unknown>): boolean {
    for (const rule of this.rules) {
      if (rule.action !== 'ask') continue;
      if (!this.toolNameMatches(rule.tool, toolName)) continue;
      if (rule.paramPattern && !this.paramMatches(rule.paramPattern, input)) continue;
      if (rule.pattern) {
        const inputStr = this.inputToString(toolName, input);
        const matched = toolName === 'bash' && typeof input.command === 'string'
          ? matchBashPattern(rule.pattern, inputStr, 'ask')
          : matchPattern(rule.pattern, inputStr);
        if (matched) return true;
      } else {
        return true;
      }
    }
    return false;
  }

  hasExplicitDeny(toolName: string, input: Record<string, unknown>): boolean {
    if (this.deniedCommands.has(denialKey(toolName, input))) return true;
    // First-match semantics (consistent with check()).
    for (const rule of this.rules) {
      if (!this.toolNameMatches(rule.tool, toolName)) continue;
      if (rule.paramPattern && !this.paramMatches(rule.paramPattern, input)) continue;
      if (rule.pattern) {
        if (matchPattern(rule.pattern, this.inputToString(toolName, input))) return rule.action === 'deny';
      } else {
        return rule.action === 'deny';
      }
    }
    return false;
  }

  /**
   * Remove every rule matching the predicate. Returns the number removed.
   * Used to drop scoped rule sets (e.g. a persona's auto-allow rules) when
   * that scope ends, so they do not accumulate or leak across switches.
   */
  removeRule(predicate: (rule: PermissionRule) => boolean): number {
    const before = this.rules.length;
    this.rules = this.rules.filter(r => !predicate(r));
    return before - this.rules.length;
  }

  /**
   * Detect rules that are shadowed by higher-priority rules and will never fire.
   */
  detectShadowedRules(): Array<{ rule: PermissionRule; shadowedBy: PermissionRule }> {
    const shadows: Array<{ rule: PermissionRule; shadowedBy: PermissionRule }> = [];
    for (let i = 0; i < this.rules.length; i++) {
      const allow = this.rules[i];
      if (allow.action !== 'allow') continue;
      for (let j = 0; j < this.rules.length; j++) {
        if (i === j) continue;
        const shadower = this.rules[j];
        if (shadower.tool !== allow.tool) continue;
        // A paramPattern rule is narrow (matches only specific named params),
        // so it never blanket-shadows a later allow.
        if (shadower.paramPattern) continue;
        const blanket = !shadower.pattern || shadower.pattern === '*';
        if (!blanket) continue;
        // ASK shadowers only count when they precede the allow (order matters:
        // an ask AFTER the allow never fires because the allow wins first). DENY
        // shadowers are ORDER-INDEPENDENT — a matching deny UNCONDITIONALLY wins
        // over any allow regardless of position (check()'s anyDenyMatches), so an
        // allow shadowed by ANY tool-wide deny is unreachable wherever the deny
        // sits. Mirrors Claude's isAllowRuleShadowedByDenyRule full-list scan.
        if (shadower.action === 'deny') {
          shadows.push({ rule: allow, shadowedBy: shadower });
        } else if (shadower.action === 'ask' && j < i) {
          shadows.push({ rule: allow, shadowedBy: shadower });
        }
      }
    }
    return shadows;
  }

  // -------------------------------------------------------------------------
  // Persistence
  // -------------------------------------------------------------------------

  /**
   * Load permission rules from `.ami/permissions.json` relative
   * to the given working directory.
   */
  loadRules(cwd: string): string[] {
    const warnings: string[] = [];
    const filePath = path.join(cwd, '.ami', 'permissions.json');
    if (!fs.existsSync(filePath)) return warnings;

    try {
      const raw = fs.readFileSync(filePath, 'utf-8');
      const data = JSON.parse(raw);

      if (data.mode && typeof data.mode === 'string') {
        if ((PERMISSION_MODES as readonly string[]).includes(data.mode)) {
          this.mode = data.mode as PermissionMode;
        } else {
          warnings.push(`Invalid permission mode "${data.mode}" was ignored`);
        }
      }

      if (Array.isArray(data.rules)) {
        for (const rule of data.rules) {
          const { rule: coerced, warning } = this._coerceRule(rule, 'projectSettings');
          if (coerced) this.rules.push(coerced);
          else if (warning) warnings.push(warning);
        }
      }
    } catch {
      warnings.push('permissions.json could not be parsed and was ignored');
    }
    return warnings;
  }

  /**
   * Coerce+validate one raw rule object (shared by loadRules and loadTieredRules
   * — one canonical validator, no per-loader duplication). Returns the typed rule
   * tagged with its `source` tier, or a human-readable warning for a dropped rule.
   */
  private _coerceRule(rule: unknown, source: PermissionUpdateDestination): { rule?: PermissionRule; warning?: string } {
    const r = rule as Record<string, unknown> | null;
    if (r && typeof r.tool === 'string' && typeof r.action === 'string' &&
        ['allow', 'deny', 'ask'].includes(r.action)) {
      const paramPattern = this.sanitizeParamPattern(r.paramPattern);
      // A rule that specified a paramPattern but whose values were all invalid is
      // malformed — drop it rather than load it as a broad (pattern-less) rule.
      if (r.paramPattern !== undefined && paramPattern === undefined) {
        return { warning: `Invalid permission rule for "${r.tool}" was skipped: malformed paramPattern` };
      }
      return {
        rule: {
          tool: r.tool,
          pattern: typeof r.pattern === 'string' ? r.pattern : undefined,
          action: r.action as PermissionRule['action'],
          reason: typeof r.reason === 'string' ? r.reason : undefined,
          paramPattern,
          source,
        },
      };
    }
    // A user who typos a rule gets told WHICH rule was dropped, not a silent no-op.
    return { warning: `Invalid permission rule ${JSON.stringify(rule)} was skipped` };
  }

  /**
   * Load permission rules from ALL persisted tiers and merge them with documented
   * precedence: most-specific first so check()'s first-match allow/ask wins for
   * localSettings over projectSettings over userSettings (deny stays
   * order-independent). Each loaded rule is tagged with its `source` tier. Returns
   * collected warnings. Mirrors Claude's destination-tiered rule merge.
   */
  loadTieredRules(cwd: string): string[] {
    const warnings: string[] = [];
    // Precedence (highest first): local → project → user. Push highest first so
    // first-match evaluation prefers the more-specific tier.
    const tiers: PermissionUpdateDestination[] = ['localSettings', 'projectSettings', 'userSettings'];
    for (const tier of tiers) {
      const filePath = permissionDestinationPath(cwd, tier);
      if (!filePath || !fs.existsSync(filePath)) continue;
      try {
        const data = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
        if (Array.isArray(data.rules)) {
          for (const rule of data.rules) {
            const { rule: coerced, warning } = this._coerceRule(rule, tier);
            if (coerced) this.rules.push(coerced);
            else if (warning) warnings.push(`${tier}: ${warning}`);
          }
        }
      } catch {
        warnings.push(`${tier}: settings file could not be parsed and was ignored`);
      }
    }
    return warnings;
  }

  /**
   * Persist the rules whose `source` matches `destination` to that tier's file
   * (in-memory tiers session/cliArg are a no-op). Returns {ok,error}; a persist
   * failure is surfaced, never silently lost (see {@link saveRules}).
   */
  saveRulesToDestination(cwd: string, destination: PermissionUpdateDestination): { ok: boolean; error?: string } {
    const filePath = permissionDestinationPath(cwd, destination);
    if (!filePath) return { ok: true }; // session / cliArg are in-memory only
    try {
      const dir = path.dirname(filePath);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      const rules = this.rules.filter(r => r.source === destination);
      fs.writeFileSync(filePath, JSON.stringify({ rules }, null, 2) + '\n', 'utf-8');
      return { ok: true };
    } catch (e) {
      return { ok: false, error: e instanceof Error ? e.message : String(e) };
    }
  }

  /**
   * Persist the current rules to `.ami/permissions.json`. Returns a success/error
   * signal so callers can surface a write failure instead of silently losing a
   * "don't ask again" rule (a read-only `.ami/`, a full disk, or a permission
   * error must not look like success). Mirrors Claude's addPermissionRulesToSettings
   * try/catch + boolean-return behavior (permissionsLoader.ts L258-295).
   */
  saveRules(cwd: string): { ok: boolean; error?: string } {
    try {
      const dirPath = path.join(cwd, '.ami');
      if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, { recursive: true });
      }
      const filePath = path.join(dirPath, 'permissions.json');
      const data = { mode: this.mode, rules: this.rules };
      fs.writeFileSync(filePath, JSON.stringify(data, null, 2) + '\n', 'utf-8');
      return { ok: true };
    } catch (e) {
      return { ok: false, error: e instanceof Error ? e.message : String(e) };
    }
  }

  // -------------------------------------------------------------------------
  // Policy memo (permission cache)
  // -------------------------------------------------------------------------

  private _memo: PolicyMemo | null = null;

  enableMemo(ttlMs?: number): void {
    this._memo = new PolicyMemo(ttlMs);
  }

  disableMemo(): void {
    this._memo = null;
  }

  getMemo(): PolicyMemo | null {
    return this._memo;
  }

  private _hashInput(input: Record<string, unknown>): string {
    const str = JSON.stringify(input);
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      hash = ((hash << 5) - hash + str.charCodeAt(i)) | 0;
    }
    return hash.toString(36);
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

// ---------------------------------------------------------------------------
// PolicyMemo — memoize permission decisions across bursts of tool calls
// ---------------------------------------------------------------------------

const POLICY_MEMO_MAX_ENTRIES = 1000;

interface MemoEntry {
  decision: 'allow' | 'deny' | 'ask';
  expiresAt: number;
  lastAccess: number;
}

export class PolicyMemo {
  private readonly _ttlMs: number;
  private readonly _cache = new Map<string, MemoEntry>();

  constructor(ttlMs: number = 5000) {
    this._ttlMs = ttlMs;
  }

  get(toolName: string, inputHash: string): 'allow' | 'deny' | 'ask' | null {
    const key = `${toolName}:${inputHash}`;
    const entry = this._cache.get(key);
    if (!entry) return null;
    if (Date.now() > entry.expiresAt) {
      this._cache.delete(key);
      return null;
    }
    entry.lastAccess = Date.now();
    return entry.decision;
  }

  set(toolName: string, inputHash: string, decision: 'allow' | 'deny' | 'ask'): void {
    const key = `${toolName}:${inputHash}`;
    this._cache.set(key, {
      decision,
      expiresAt: Date.now() + this._ttlMs,
      lastAccess: Date.now(),
    });
    if (this._cache.size > POLICY_MEMO_MAX_ENTRIES) {
      this._evictLRU();
    }
  }

  clear(): void {
    this._cache.clear();
  }

  size(): number {
    return this._cache.size;
  }

  prune(): number {
    const now = Date.now();
    let removed = 0;
    for (const [key, entry] of this._cache) {
      if (now > entry.expiresAt) {
        this._cache.delete(key);
        removed++;
      }
    }
    return removed;
  }

  private _evictLRU(): void {
    let oldestKey: string | null = null;
    let oldestAccess = Infinity;
    for (const [key, entry] of this._cache) {
      if (entry.lastAccess < oldestAccess) {
        oldestAccess = entry.lastAccess;
        oldestKey = key;
      }
    }
    if (oldestKey) this._cache.delete(oldestKey);
  }
}
