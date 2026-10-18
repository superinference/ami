import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { detectCommandChaining } from '../permissions';
import { execCommand } from '../utils/shell';
import { validateBashSecurity, detectCredentialPathWrite, detectSessionTranscriptWrite, detectDataExfiltration, detectCodeLoadingEnvSet, detectNuclearCommand } from './bash-security';
import { shouldUseSandbox, wrapWithSandbox } from './bash-sandbox';
import { spillToolText } from './spill-output';
import { compactTestLog, isTestRunnerCommand, looksLikeSuiteLog } from './test-log';
import { isReadOnlyBashCommand as isCacheableReadOnly, BashResultCache } from './bash-cache';

const MAX_OUTPUT_LENGTH = 200_000;
const DEFAULT_TIMEOUT_MS = 120000;
const HARD_MAX_TIMEOUT_MS = 3_600_000; // 1 hour
const _bashCache = new BashResultCache();

function parseEnvMs(name: string): number | undefined {
  const raw = process.env[name];
  if (!raw) return undefined;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : undefined;
}

/**
 * Resolve the effective bash timeout. Honors BASH_DEFAULT_TIMEOUT_MS (default
 * when no per-call timeout) and BASH_MAX_TIMEOUT_MS (ceiling), matching Claude
 * Code. A per-call `timeout` still wins but is clamped to the max.
 */
export function resolveBashTimeout(inputTimeout?: number): number {
  const def = parseEnvMs('BASH_DEFAULT_TIMEOUT_MS') ?? DEFAULT_TIMEOUT_MS;
  const max = parseEnvMs('BASH_MAX_TIMEOUT_MS') ?? HARD_MAX_TIMEOUT_MS;
  const requested = inputTimeout && inputTimeout > 0 ? inputTimeout : def;
  return Math.min(requested, max);
}

let _cachedPermissionManager: InstanceType<typeof import('../permissions').PermissionManager> | null = null;

function isReadOnlyBashCommand(command: string): boolean {
  try {
    if (!_cachedPermissionManager) {
      const { PermissionManager } = require('../permissions');
      _cachedPermissionManager = new PermissionManager();
    }
    const pm = _cachedPermissionManager!;
    return pm.classifyBashCommand(command) === 'safe';
  } catch {
    return false;
  }
}

export function isBashForkExhaustion(stdout: string, stderr: string): boolean {
  const combined = `${stdout}\n${stderr}`;
  return /fork:\s*Resource temporarily unavailable/i.test(combined)
    || /Cannot fork/i.test(combined)
    || /\bEAGAIN\b/i.test(combined);
}

export const FORK_RETRY_MAX = 5;
export const FORK_RETRY_BASE_MS = 500;

async function execCommandWithForkRetry(
  command: string,
  options: Parameters<typeof execCommand>[1],
): Promise<Awaited<ReturnType<typeof execCommand>>> {
  let last = await execCommand(command, options);
  for (let attempt = 0; attempt < FORK_RETRY_MAX && isBashForkExhaustion(last.stdout, last.stderr); attempt++) {
    const base = FORK_RETRY_BASE_MS * Math.pow(2, attempt);
    const jitter = Math.floor(Math.random() * base);
    await new Promise(r => setTimeout(r, base + jitter));
    last = await execCommand(command, options);
  }
  if (isBashForkExhaustion(last.stdout, last.stderr)) {
    last.stderr += '\n[fork exhaustion: all retries failed — too many concurrent processes. Try closing other sessions.]';
  }
  return last;
}

function looksLikeHungPrompt(output: string): boolean {
  const lastLine = output.trim().split('\n').pop() ?? '';
  return /\(y\/n\)|\[y\/n\]|\(yes\/no\)|password:|passphrase:|Press Enter|Continue\?|Overwrite\?|Are you sure/i.test(lastLine);
}

/** Global git options that take a separate value token (so the token after them
 *  is a value, not the subcommand). */
const GIT_VALUE_OPTS = new Set([
  '-C', '-c', '--git-dir', '--work-tree', '--namespace', '--exec-path',
  '--super-prefix', '--config-env',
]);

export function detectGitCommit(command: string): boolean {
  const stripped = command.replace(/"[^"]*"|'[^']*'/g, '');
  // Tokenize (separators → spaces) and look for `git [global-opts…] commit`.
  // A linear scan avoids the ReDoS risk of a backtracking mega-regex while
  // still catching `git -C /x commit`, `git -c k=v commit`,
  // `git --git-dir /x commit`, `git --git-dir=/x commit`, etc. The final
  // subcommand must be exactly `commit` (not `commit-graph`/`commit-tree`).
  const tokens = stripped.replace(/[;&|()]/g, ' ').split(/\s+/).filter(Boolean);
  for (let i = 0; i < tokens.length; i++) {
    // Strip any path prefix so `/usr/bin/git` and `./git` are still recognized
    // (the old \bgit regex matched these because `/` is a word boundary).
    if (tokens[i].replace(/^.*\//, '') !== 'git') continue;
    let j = i + 1;
    while (j < tokens.length) {
      const t = tokens[j];
      if (GIT_VALUE_OPTS.has(t)) { j += 2; continue; }        // option + value token
      if (t.startsWith('-')) { j += 1; continue; }            // --opt=val / boolean / short flags
      break;                                                  // first positional = subcommand
    }
    if (j < tokens.length && tokens[j] === 'commit') return true;
  }
  return false;
}

const DETACHED_GIT_DISCARD_MSG =
  'git stash/reset/restore is not allowed in non-interactive eval; it discards the patch.';

const STASH_POP_APPLY = new Set(['pop', 'apply']);
const STASH_PUSH_SAVE = new Set(['push', 'save', '']);
const STASH_DROP_CLEAR = new Set(['drop', 'clear']);
const STASH_SUBCOMMANDS = new Set([
  'push', 'save', 'pop', 'apply', 'list', 'show', 'drop', 'clear',
  'branch', 'create', 'store', 'export', 'import',
]);

function isShellMeta(t: string): boolean {
  return t === '&&' || t === '||' || t === ';' || t === '|' || t === '|&' || t === '&' || t === '\n';
}

/**
 * Tokenize a shell command for the git guards. Unlike a naive
 * `replace(quotes).split(/\s+/)`, this removes the quote characters while
 * keeping their content as live tokens (bash does the same), so a quoted flag
 * or subcommand (`git reset "--hard"`, `git "stash"`) stays visible to the
 * guard — and quoted whitespace does not split a token, so `echo "git stash"`
 * stays a single argument rather than being parsed as a git invocation.
 * Unquoted newlines/CRs become `\n` separator tokens so a command boundary is
 * never silently swallowed into the next invocation's positionals.
 */
function tokenizeShell(command: string): string[] {
  const tokens: string[] = [];
  let cur = '';
  let quote: '"' | "'" | null = null;
  const flush = () => {
    if (cur.length > 0) {
      tokens.push(cur);
      cur = '';
    }
  };
  for (let i = 0; i < command.length; i++) {
    const c = command[i];
    if (quote) {
      if (c === quote) quote = null;
      else cur += c;
      continue;
    }
    if (c === '"' || c === "'") { quote = c; continue; }
    if (c === '\n' || c === '\r') { flush(); tokens.push('\n'); continue; }
    if (c === ' ' || c === '\t') { flush(); continue; }
    cur += c;
  }
  flush();
  return tokens;
}

interface GitInvocation {
  sub: string;
  rest: string[];
  stashSub: string;
}

function parseStashSubcommand(rest: string[]): string {
  for (let i = 0; i < rest.length; i++) {
    const t = rest[i];
    if (t === '--') break;
    if (t.startsWith('-')) {
      if (t === '-m' || t === '--message' || t === '--pathspec-from-file') i += 1;
      continue;
    }
    const name = t.replace(/[;&]+$/, '');
    return STASH_SUBCOMMANDS.has(name) ? name : '';
  }
  return '';
}

function parseGitInvocations(command: string): GitInvocation[] {
  const tokens = tokenizeShell(command);
  const invocations: GitInvocation[] = [];
  for (let i = 0; i < tokens.length; i++) {
    const tok = tokens[i].replace(/^.*\//, '').replace(/[;&]+$/, '');
    if (tok !== 'git') continue;
    let j = i + 1;
    while (j < tokens.length) {
      const t = tokens[j];
      if (isShellMeta(t)) break;
      if (t === '-C' || t === '-c') { j += 2; continue; }
      if (t.startsWith('--git-dir') || t.startsWith('--work-tree')) {
        j += t.includes('=') ? 1 : 2;
        continue;
      }
      if (t.startsWith('-') && t !== '--') { j += 1; continue; }
      break;
    }
    const sub = (tokens[j] || '').replace(/[;&]+$/, '');
    const rest: string[] = [];
    for (let k = j + 1; k < tokens.length; k++) {
      if (isShellMeta(tokens[k])) break;
      rest.push(tokens[k]);
    }
    invocations.push({
      sub,
      rest,
      stashSub: sub === 'stash' ? parseStashSubcommand(rest) : '',
    });
  }
  return invocations;
}

/**
 * Detect git commands that discard the working tree. Used in detached
 * sessions where the final `git diff` is the session artifact — stash push /
 * reset / restore would wipe successful edits before that capture.
 *
 * Allowed: `git stash list|show|pop|apply`, and compound
 * `git stash && tests && git stash pop` (stash without a later pop/apply is
 * still blocked).
 */
function checkoutPathExists(token: string, cwd?: string): boolean {
  // `git checkout .`/`..` (and their `./`,`../` forms) are worktree pathspecs,
  // not branches: they restore every tracked file from the index — the
  // canonical "discard ALL working-tree changes" command. Always treat them as
  // paths so a single-positional `git checkout .` is blocked.
  if (token === '.' || token === '..' || token === './' || token === '../') return true;
  if (!cwd) return false;
  try {
    const abs = path.resolve(cwd, token);
    return fs.existsSync(abs) && fs.statSync(abs).isFile();
  } catch {
    return false;
  }
}

export function detectDetachedGitDiscard(command: string, cwd?: string): string | null {
  const invocations = parseGitInvocations(command);
  const hasLaterPopOrApply = (from: number): boolean =>
    invocations.slice(from + 1).some(inv => inv.sub === 'stash' && STASH_POP_APPLY.has(inv.stashSub));

  for (let i = 0; i < invocations.length; i++) {
    const { sub, rest, stashSub } = invocations[i];
    if (sub === 'stash') {
      if (STASH_DROP_CLEAR.has(stashSub) || stashSub === 'branch') return DETACHED_GIT_DISCARD_MSG;
      if (STASH_PUSH_SAVE.has(stashSub) && !hasLaterPopOrApply(i)) return DETACHED_GIT_DISCARD_MSG;
      continue;
    }
    if (sub === 'restore') return DETACHED_GIT_DISCARD_MSG;
    if (sub === 'reset' && rest.some(t => t === '--hard' || t === '--merge' || t === '--keep')) {
      return DETACHED_GIT_DISCARD_MSG;
    }
    if (sub === 'checkout') {
      if (rest.includes('--')) return DETACHED_GIT_DISCARD_MSG;
      // `git checkout <tree-ish> <path>` discards the worktree. A single
      // positional is a branch switch unless it names a file in cwd
      // (`git checkout file.ts`). `git checkout -b feature` stays allowed.
      const positional = rest.filter(t => t !== '--' && !t.startsWith('-'));
      if (positional.length >= 2) return DETACHED_GIT_DISCARD_MSG;
      if (positional.length === 1 && checkoutPathExists(positional[0], cwd)) {
        return DETACHED_GIT_DISCARD_MSG;
      }
    }
    if (sub === 'clean' && rest.some(t => t === '--force' || /^-[a-zA-Z]*f[a-zA-Z]*$/.test(t))) {
      return DETACHED_GIT_DISCARD_MSG;
    }
  }
  return null;
}

const PROTECTED_BRANCHES = /\b(main|master|production|release|develop)\b/;

export function detectForcePushToProtected(command: string): string | null {
  const norm = command.replace(/\s+/g, ' ').trim();
  if (!/\bgit\s+push\b/.test(norm)) return null;
  if (!/--force\b|-f\b|--force-with-lease\b/.test(norm)) return null;
  // Don't match inside echo, printf, or quoted strings used as args to other cmds
  if (/^\s*(echo|printf)\b/.test(norm)) return null;
  const args = norm.split(/\s+/);
  const gitIdx = args.indexOf('git');
  if (gitIdx < 0) return null;
  const pushIdx = args.indexOf('push', gitIdx);
  if (pushIdx < 0) return null;
  const afterPush = args.slice(pushIdx + 1).filter(a => !a.startsWith('-'));
  for (const a of afterPush) {
    if (PROTECTED_BRANCHES.test(a)) {
      return `Force push to protected branch "${a}" is blocked. Use a feature branch instead.`;
    }
  }
  return null;
}

export function detectSelfKill(command: string): string | null {
  const stripped = command.replace(/"[^"]*"|'[^']*'/g, '');
  const pid = process.pid;
  const ppid = process.ppid;
  if (new RegExp(`\\bkill\\s+(-\\w+\\s+)*${pid}\\b`).test(stripped))
    return 'This would kill AMI itself (PID match).';
  if (new RegExp(`\\bkill\\s+(-\\w+\\s+)*${ppid}\\b`).test(stripped))
    return 'This would kill AMI\'s parent process.';
  return null;
}

function detectBlockedSleep(command: string, runInBackground: boolean, hasCustomTimeout: boolean): string | null {
  if (runInBackground || hasCustomTimeout) return null;
  const match = command.match(/^\s*sleep\s+(\d+)/);
  if (match && parseInt(match[1]) > 10) {
    return `Blocking sleep for ${match[1]}s detected. Use run_in_background: true for long waits, or schedule_wakeup for timed delays.`;
  }
  return null;
}

export const SENSITIVE_ENV_SET = new Set([
  'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN',
  'AWS_SECURITY_TOKEN',
  'GITHUB_TOKEN', 'GH_TOKEN', 'GITHUB_APP_PRIVATE_KEY',
  'ANTHROPIC_API_KEY', 'OPENAI_API_KEY',
  'GOOGLE_APPLICATION_CREDENTIALS', 'GOOGLE_API_KEY',
  'AZURE_CLIENT_SECRET', 'AZURE_TENANT_ID',
  'DATABASE_URL', 'REDIS_URL',
  'NPM_TOKEN', 'PYPI_TOKEN',
  'SSH_AUTH_SOCK', 'GIT_ASKPASS',
]);

const SENSITIVE_SUFFIX_PATTERNS = [
  /_KEY$/i, /_SECRET$/i, /_TOKEN$/i, /_PASSWORD$/i,
  /_CREDENTIALS$/i, /_PRIVATE_KEY$/i, /_API_KEY$/i,
];

const SAFE_PREFIX_RE = /^(PATH|HOME|USER|SHELL|TERM|LANG|DISPLAY|TMPDIR|PWD|OLDPWD|HOSTNAME|LOGNAME|SHLVL|_)$/i;
const SAFE_PREFIX_PATTERNS = [
  /^LC_/i, /^XDG_/i, /^SUPERINFERENCE_/i, /^NODE_/i,
  /^COLORTERM$/i, /^COLUMNS$/i, /^LINES$/i,
  /^NO_COLOR$/i, /^FORCE_COLOR$/i,
];

function isSafeEnvVar(key: string): boolean {
  if (SAFE_PREFIX_RE.test(key)) return true;
  return SAFE_PREFIX_PATTERNS.some(p => p.test(key));
}

function isSensitiveEnvVar(key: string): boolean {
  if (SENSITIVE_ENV_SET.has(key)) return true;
  if (isSafeEnvVar(key)) return false;
  return SENSITIVE_SUFFIX_PATTERNS.some(p => p.test(key));
}

export function sanitizeEnvForSubprocess(env: Record<string, string | undefined>): Record<string, string | undefined> {
  const out: Record<string, string | undefined> = {};
  for (const key of Object.keys(env)) {
    if (!isSensitiveEnvVar(key)) {
      out[key] = env[key];
    }
  }
  return out;
}

function scrubEnv(): NodeJS.ProcessEnv {
  const env = sanitizeEnvForSubprocess({ ...process.env }) as NodeJS.ProcessEnv;
  env.GIT_EDITOR = 'false';
  env.GIT_PAGER = 'cat';
  env.EDITOR = 'false';
  env.VISUAL = 'false';
  env.PAGER = 'cat';
  env.DEBIAN_FRONTEND = 'noninteractive';
  env.GIT_TERMINAL_PROMPT = '0';
  env.AI_AGENT = 'superinference';
  env.PIP_PROGRESS_BAR = 'off';
  env.TQDM_DISABLE = '1';
  env.PYTHONDONTWRITEBYTECODE = '1';
  env.MANPAGER = 'cat';
  return env;
}

export const bashTool: ToolDefinition = {
  name: 'bash',
  description:
    'Execute a bash command. Use for running scripts, installing packages, compiling, git operations, and any shell task. IMPORTANT: Do not use bash for reading files (use file_read), editing files (use file_edit), or searching (use grep/glob). Use absolute paths. Quote paths with spaces. Git: new commits only (never amend unless asked), never --no-verify, never force-push, never commit unless explicitly asked. Do not sleep between commands. Chain dependent commands with &&. In assistant/detached mode, commands exceeding 15s are auto-backgrounded — use run_in_background for intentionally long tasks.',
  inputSchema: {
    type: 'object',
    properties: {
      command: {
        type: 'string',
        description: 'The bash command to execute.',
      },
      timeout: {
        type: 'number',
        description:
          'Optional timeout in milliseconds. Defaults to 120000 (2 minutes), or BASH_DEFAULT_TIMEOUT_MS if set; capped at BASH_MAX_TIMEOUT_MS (default 1 hour).',
      },
      run_in_background: {
        type: 'boolean',
        description:
          'Run this command in the background. Returns immediately with a task ID. ' +
          'Use task_output to check results later, task_kill to stop it, or task_list to see all background tasks.',
      },
      description: {
        type: 'string',
        description:
          'A short human-readable description of what this command does.',
      },
      dangerouslyDisableSandbox: {
        type: 'boolean',
        description:
          'Override sandbox mode and run commands without resource restrictions. Use only when the sandbox interferes with legitimate operations.',
      },
    },
    required: ['command'],
  },
  isReadOnly: false,
  isConcurrencySafe: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const command = input.command as string;
    const timeout = resolveBashTimeout(input.timeout as number | undefined);

    if (!command || command.trim().length === 0) {
      return { output: 'Error: command must not be empty.', isError: true };
    }

    // Nuclear command blocklist — un-bypassable, runs before ALL other checks.
    // No flag, mode, or env var can disable this. Even --yolo cannot override it.
    const nuclear = detectNuclearCommand(command);
    if (nuclear.blocked) {
      return {
        output: [
          `Error: BLOCKED — ${nuclear.reason}.`,
          `Command: ${command.length > 200 ? command.slice(0, 200) + '...' : command}`,
          '',
          'This command is on the nuclear blocklist and is NEVER allowed, regardless of',
          'permission mode (including --yolo). These are commands that can cause',
          'unrecoverable system-wide damage (filesystem destruction, disk wipe, fork bomb).',
          '',
          'If you need to delete files, target a specific directory instead of root paths.',
        ].join('\n'),
        isError: true,
      };
    }

    if (!fs.existsSync(context.cwd)) {
      return {
        output: `Error: working directory "${context.cwd}" does not exist. It may have been deleted or moved. Use /add-dir to set a new working directory.`,
        isError: true,
      };
    }

    if (detectGitCommit(command)) {
      return {
        output:
          'Error: Use the git_commit tool instead of running git commit via bash. ' +
          'The git_commit tool ensures proper Co-Authored-By attribution is always included.\n\n' +
          'Example: git_commit({ message: "your commit message", files: ["file1.ts", "file2.ts"] })',
        isError: true,
      };
    }

    if (context.detachedMode) {
      const discard = detectDetachedGitDiscard(command, context.cwd);
      if (discard) {
        return { output: `Error: ${discard}`, isError: true };
      }
    }

    const selfKill = detectSelfKill(command);
    if (selfKill) {
      return {
        output: `Error: ${selfKill} Use kill <pid> with the specific child process PID instead.`,
        isError: true,
      };
    }

    const codeLoadCheck = detectCodeLoadingEnvSet(command);
    if (codeLoadCheck.blocked) {
      return { output: `Error: Setting ${codeLoadCheck.variable} is blocked — it can load arbitrary code into child processes.`, isError: true };
    }

    const forcePush = detectForcePushToProtected(command);
    if (forcePush) {
      return { output: `Error: ${forcePush}`, isError: true };
    }

    const secCheck = validateBashSecurity(command, { detachedMode: context.detachedMode });
    if (!secCheck.safe) {
      return {
        output: `Error: ${secCheck.message}`,
        isError: true,
      };
    }

    const credWrite = detectCredentialPathWrite(command);
    if (credWrite.blocked) {
      return { output: `Error: blocked — ${credWrite.description} (credential path, not allowed)`, isError: true };
    }

    const sessionWrite = detectSessionTranscriptWrite(command);
    if (sessionWrite.blocked) {
      return { output: `Error: blocked — ${sessionWrite.description} (session transcript is protected)`, isError: true };
    }

    const exfil = detectDataExfiltration(command);
    if (exfil.blocked) {
      return { output: `Error: blocked — ${exfil.reason} (data exfiltration risk)`, isError: true };
    }

    const sleepBlock = detectBlockedSleep(command, !!input.run_in_background, !!input.timeout);
    if (sleepBlock) return { output: `Error: ${sleepBlock}`, isError: true };

    const readOnly = isReadOnlyBashCommand(command);

    const effectiveCwd = context.cwd;
    if (isCacheableReadOnly(command)) {
      const cached = _bashCache.get(command, effectiveCwd);
      if (cached !== null) return { output: cached, metadata: { readOnly: true, cached: true } };
    }

    if (input.run_in_background && context.processManager) {
      const desc = (input.description as string) || command.slice(0, 80);
      const taskId = context.processManager.spawn(command, {
        cwd: context.cwd,
        description: desc,
        env: scrubEnv(),
      });
      const task = context.processManager.get(taskId);
      return {
        output: `[background: true]\nBackground task started.\n  Task ID: ${taskId}\n  PID: ${task?.pid ?? 'unknown'}\n  Command: ${command}\n\nUse task_output({ task_id: "${taskId}" }) to check status and output.\nUse task_kill({ task_id: "${taskId}" }) to stop it.\nUse task_list() to see all background tasks.`,
        isError: false,
      };
    }

    const disableSandbox = input.dangerouslyDisableSandbox === true;
    const useSandbox = !disableSandbox && shouldUseSandbox(command);
    const effectiveCommand = useSandbox ? wrapWithSandbox(command, context.cwd) : command;

    const result = await execCommandWithForkRetry(effectiveCommand, {
      cwd: context.cwd,
      timeout,
      abortSignal: context.abortSignal,
      env: scrubEnv(),
      onData: context.onProgress ? (chunk) => context.onProgress!(chunk) : undefined,
    });

    // Fold before any return. A failing suite is an error result, and the
    // engine stores error output without the normal length cap.
    const presented = presentCommandOutput(command, result.stdout, result.stderr);

    if (result.exitCode === null && !presented.stdout && presented.stderr === 'Aborted') {
      return { output: 'Command aborted.', isError: true };
    }

    if (result.exitCode === null) {
      if (context.abortSignal?.aborted) {
        return {
          output: formatOutput(presented.stdout, presented.stderr, null, 'Command aborted.'),
          isError: true,
        };
      }
      const combinedOutput = presented.stdout + presented.stderr;
      if (looksLikeHungPrompt(combinedOutput)) {
        const lastLine = combinedOutput.trim().split('\n').pop();
        return {
          output: `Command appears to be waiting for interactive input. Last line: "${lastLine}"\nSuggestion: Use 'echo y | ${command}' or 'yes | ${command}' to auto-respond, or run with run_in_background: true.`,
          isError: true,
        };
      }
      if (context.processManager) {
        const taskId = context.processManager.spawn(command, {
          cwd: context.cwd,
          description: `[auto-bg] ${command.slice(0, 60)}`,
          env: scrubEnv(),
        });
        return {
          output: `Command timed out after ${timeout}ms. Auto-moved to background as task ${taskId}. Use task_output to check progress.`,
        };
      }
      return {
        output: formatOutput(presented.stdout, presented.stderr, null, `Command timed out after ${timeout}ms.`),
        isError: true,
      };
    }

    // Already folded. Spill only non-suite output that is still over the cap.
    const bashMaxChars = context.outputBudget?.bashMaxChars ?? MAX_OUTPUT_LENGTH;
    let stdout = spillToolText(context.cwd, 'bash', presented.stdout, bashMaxChars);
    const stderr = presented.stderr;

    const noOutputExpected = /^\s*(mkdir|touch|mv|cp|rm|chmod|chown)\b/.test(command);
    if (noOutputExpected && !stdout.trim() && result.exitCode === 0) {
      stdout = '[Command completed successfully (no output expected)]';
    }

    let output = formatOutput(stdout, stderr, result.exitCode === 0 ? 0 : null);
    const chaining = detectCommandChaining(command);
    if (chaining.chained && chaining.count > 2) {
      output += '\n\n[Note: This command chains ' + chaining.count +
        ' commands via ' + chaining.operators.join(', ') +
        '. Consider using separate tool calls for better error handling and readability.]';
    }

    let exitCodeInfo: ExitCodeInfo | null = null;
    if (result.exitCode !== 0) {
      exitCodeInfo = interpretExitCode(command, result.exitCode);
      if (exitCodeInfo) output += `\n${exitCodeInfo.message}`;
      if (!exitCodeInfo?.isNonError) {
        const hint = diagnoseError(output);
        if (hint) output += `\n\n[Hint: ${hint}]`;
      }
    }

    const isRealError = result.exitCode !== 0 && !exitCodeInfo?.isNonError;
    const exitInfo = result.exitCode !== 0 ? `\n[exit code: ${result.exitCode}${exitCodeInfo ? ' ' + exitCodeInfo.message : ''}]` : '';
    output = `${output}${exitInfo}`.trim();

    if (!isRealError && isCacheableReadOnly(command)) {
      _bashCache.set(command, effectiveCwd, output);
    } else if (!readOnly && !isRealError) {
      _bashCache.invalidateForCwd(effectiveCwd);
    }

    return {
      output,
      isError: isRealError,
      metadata: { readOnly },
    };
  },
};

function presentCommandOutput(
  command: string,
  stdout: string,
  stderr: string,
): { stdout: string; stderr: string } {
  const foldOut = isTestRunnerCommand(command) || looksLikeSuiteLog(stdout);
  const foldErr = isTestRunnerCommand(command) || looksLikeSuiteLog(stderr);
  return {
    stdout: foldOut ? compactTestLog(stdout) : stdout,
    stderr: foldErr ? compactTestLog(stderr) : stderr,
  };
}

const ERROR_HINTS: Array<{ pattern: RegExp; hint: string }> = [
  { pattern: /Transform failed|SyntaxError:.*Unexpected/i, hint: 'Your code has a syntax error (missing bracket, semicolon, or mismatched quotes). Use file_read to check the file you last edited.' },
  { pattern: /Cannot find module|ERR_MODULE_NOT_FOUND/i, hint: 'Module not found. Check the import path with list_dir or glob, and try running via npm test instead of raw node commands.' },
  { pattern: /ENOENT.*no such file/i, hint: 'File or directory does not exist. Use list_dir to check what files are available.' },
  { pattern: /TypeError:.*is not a function/i, hint: 'A function call is wrong. Use file_read to check the export names and signatures in the imported module.' },
  { pattern: /ReferenceError:.*is not defined/i, hint: 'A variable or import is missing. Check your imports and variable declarations with file_read.' },
  { pattern: /EACCES.*permission denied/i, hint: 'Permission denied. Check file permissions with ls -la, or use a different directory.' },
  { pattern: /EADDRINUSE/i, hint: 'Port already in use. Try a different port or find and kill the process using it (lsof -i :PORT).' },
  { pattern: /ENOMEM|Cannot allocate memory|out of memory/i, hint: 'Out of memory. Try reducing the workload, closing other processes, or increasing available memory.' },
  { pattern: /ETIMEDOUT|ECONNREFUSED|ECONNRESET/i, hint: 'Network error. Check if the target service is running and reachable.' },
  { pattern: /EPIPE|broken pipe/i, hint: 'Broken pipe. The receiving process terminated before all input was consumed.' },
  { pattern: /EMFILE|ENFILE|too many open files/i, hint: 'Too many open files. Close unused file handles or increase the ulimit (ulimit -n).' },
  { pattern: /TS\d+:/i, hint: 'TypeScript compilation error. Use file_read to check the file at the reported line number.' },
  { pattern: /error\[E\d+\]/i, hint: 'Rust compilation error. Check the file and line number in the error message.' },
  { pattern: /npm ERR!.*ERESOLVE/i, hint: 'NPM dependency resolution conflict. Try npm install --legacy-peer-deps or check package.json version constraints.' },
  { pattern: /fatal: not a git repository/i, hint: 'Not inside a git repository. Check the current directory.' },
];

function diagnoseError(output: string): string | null {
  for (const { pattern, hint } of ERROR_HINTS) {
    if (pattern.test(output)) return hint;
  }
  return null;
}

interface ExitCodeInfo {
  message: string;
  isNonError?: boolean;
}

function interpretExitCode(command: string, exitCode: number): ExitCodeInfo | null {
  const parts = command.trim().split(/[|;&]\s*/);
  const lastCmd = parts[parts.length - 1].trim().split(/\s+/)[0];
  const base = lastCmd || command.trim().split(/\s+/)[0];
  if (exitCode === 1 && (base === 'grep' || base === 'rg')) return { message: '(no matches found — not an error)', isNonError: true };
  if (exitCode === 1 && base === 'diff') return { message: '(files differ — not an error)', isNonError: true };
  if (exitCode === 1 && (base === 'test' || base === '[')) return { message: '(condition is false)', isNonError: true };
  if (exitCode === 1 && base === 'find') return { message: '(some paths were inaccessible)', isNonError: true };
  if (exitCode === 2 && base === 'grep') return { message: '(error in grep pattern or file access)' };
  if (exitCode === 126) return { message: '(permission denied — command not executable)' };
  if (exitCode === 127) return { message: '(command not found)' };
  if (exitCode === 128 + 9) return { message: '(killed by SIGKILL)' };
  if (exitCode === 128 + 15) return { message: '(killed by SIGTERM)' };
  return null;
}

function truncate(text: string, maxLength: number): string {
  if (text.length <= maxLength) {
    return text;
  }
  const half = Math.floor((maxLength - 100) / 2);
  return (
    text.slice(0, half) +
    `\n\n... [truncated ${text.length - maxLength + 100} characters] ...\n\n` +
    text.slice(text.length - half)
  );
}

function formatOutput(
  stdout: string,
  stderr: string,
  exitCode: number | null,
  extra?: string,
): string {
  const parts: string[] = [];

  if (stdout.length > 0) {
    parts.push(truncate(stdout, MAX_OUTPUT_LENGTH));
  }

  if (stderr.length > 0) {
    const label = stdout.length > 0 ? '\n[stderr]\n' : '';
    const remaining = Math.max(
      1000,
      MAX_OUTPUT_LENGTH - (parts.join('').length),
    );
    parts.push(label + truncate(stderr, remaining));
  }

  if (extra) {
    parts.push(extra);
  }

  if (exitCode !== null && exitCode !== undefined) {
    parts.push(`Exit code: ${exitCode}`);
  }

  return parts.join('\n').trim() || '(no output)';
}
