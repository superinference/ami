// Split from bash.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { ToolInputSchema } from '../types';
import { compactTestLog, isTestRunnerCommand, looksLikeSuiteLog } from './test-log';
import { findOnPath } from '../utils/which';
import { foldBashControlOutput, resolveHeadlessRenderMode, type BashHeadlessFold } from './bash/headless-output';
import { stripEmptyLines } from './bash-strip-redundant-cd-prefix';

export const DETACHED_GIT_DISCARD_MSG =
  'git stash/reset/restore is not allowed in non-interactive eval; it discards the patch.';

export function isShellMeta(t: string): boolean {
  return t === '&&' || t === '||' || t === ';' || t === '|' || t === '|&' || t === '&' || t === '\n' || t === '(' || t === ')';
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
export function tokenizeShell(command: string): string[] {
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
    // Shell metacharacters: flush current token, emit as separators.
    // Compound operators (&&, ||, |&) peek ahead and emit as two-char tokens.
    if (c === '(' || c === ')' || c === ';') { flush(); tokens.push(c); continue; }
    if (c === '&') {
      flush();
      if (command[i + 1] === '&') { tokens.push('&&'); i++; } else { tokens.push('&'); }
      continue;
    }
    if (c === '|') {
      flush();
      if (command[i + 1] === '|') { tokens.push('||'); i++; }
      else if (command[i + 1] === '&') { tokens.push('|&'); i++; }
      else { tokens.push('|'); }
      continue;
    }
    cur += c;
  }
  flush();
  return tokens;
}

export interface GitInvocation {
  sub: string;
  rest: string[];
  stashSub: string;
}

export const PROTECTED_BRANCHES = /\b(main|master|production|release|develop)\b/;

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

/**
 * Non-blocking advisory for ANY force push, independent of branch name.
 * `detectForcePushToProtected` hard-blocks force pushes to protected branches
 * (main/master/…); this surfaces the reference's destructiveCommandWarning
 * "may overwrite remote history" notice for every OTHER force push (feature
 * branches, no explicit branch arg, `--force-with-lease`, bare `-f`) so the
 * model is warned before trusting the result. Returns null when the command is
 * not a force push. Parity: destructiveCommandWarning.ts flags any
 * `git push --force/--force-with-lease/-f` regardless of the target branch.
 */
export function detectForcePushAdvisory(command: string): string | null {
  const norm = command.replace(/\s+/g, ' ').trim();
  // Don't match inside echo/printf or quoted strings used as args to other cmds.
  if (/^\s*(echo|printf)\b/.test(norm)) return null;
  if (!/\bgit\s+push\b/.test(norm)) return null;
  if (!/--force\b|-f\b|--force-with-lease\b/.test(norm)) return null;
  return 'git push --force/--force-with-lease may overwrite remote history. '
    + 'Confirm the push is intended and that no collaborator has pushed to the branch.';
}

/**
 * Non-blocking advisory for a compound command that both changes directory
 * (`cd <dir>`) and invokes `git`. Entering an attacker-controlled repository and
 * then running git lets that repo's on-disk `.git/config`
 * (core.fsmonitor / core.hooksPath / a `!`-prefixed alias) execute arbitrary
 * code — the documented "bare repository" attack. AMI's git-config-RCE guard
 * (bash-security `detectGitConfigRce`) only catches the agent *setting* such keys,
 * not *reading* a repo that already carries them, so a `cd evil && git status`
 * slips through. This warns the model to treat the output as untrusted.
 * Parity: bashCommandHelpers.ts forces approval for compound cd+git commands.
 */
export function detectCompoundCdGit(command: string): string | null {
  const segments = command.split(/(?:&&|\|\||[;|])/).map(s => s.trim()).filter(Boolean);
  if (segments.length < 2) return null;
  // `cd <arg>`: a bare `cd`/`cd -`/`cd ~` goes to a known-safe place; only a
  // directory argument can move git into an attacker-controlled tree.
  const hasCd = segments.some(s => /^cd\s+(?![-~](?:\s|$))\S/.test(s));
  if (!hasCd) return null;
  const hasGit = segments.some(s => {
    const first = s.replace(/^sudo\s+/, '').split(/\s+/)[0] ?? '';
    return (first.split('/').pop() ?? '') === 'git';
  });
  if (!hasGit) return null;
  return 'This command runs `git` after `cd` into another directory. A '
    + 'malicious `.git/config` (core.fsmonitor/hooksPath/alias) in that repo can '
    + 'execute arbitrary code — treat the output as untrusted if the directory is not yours.';
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

export const SLEEP_SUFFIX_SECONDS: Record<string, number> = { s: 1, m: 60, h: 3600, d: 86400 };

/**
 * Total duration (seconds) of a leading `sleep` command, or null if the command
 * is not a bare `sleep`. Mirrors GNU coreutils grammar: `sleep NUMBER[SUFFIX]...`
 * where SUFFIX ∈ {s,m,h,d} (default seconds), NUMBER may be a decimal, and
 * multiple arguments are summed (`sleep 1m 30` = 90s).
 */
export function parseSleepSeconds(command: string): number | null {
  const m = command.match(/^\s*sleep\s+(.+?)\s*$/i);
  if (!m) return null;
  const args = m[1].split(/\s+/);
  let total = 0;
  for (const arg of args) {
    const a = arg.match(/^(\d*\.?\d+)([smhd]?)$/i);
    if (!a) return null; // not a plain sleep duration (var, expr, flag, …) — don't guess
    total += parseFloat(a[1]) * SLEEP_SUFFIX_SECONDS[(a[2] || 's').toLowerCase()];
  }
  return total;
}

export function detectBlockedSleep(command: string, runInBackground: boolean, hasCustomTimeout: boolean): string | null {
  if (runInBackground || hasCustomTimeout) return null;
  // A blocking sleep can hide inside a compound foreground command
  // (`build && sleep 600`, `a; sleep 300`), so scan each sequencing segment, not
  // just the whole string. Split only on FOREGROUND separators (&&, ||, ;, |) —
  // a trailing single `&` backgrounds the sleep, which is not blocking, so it is
  // deliberately NOT a split point. parseSleepSeconds stays strict, so a segment
  // with any non-duration token (quotes, flags, extra args) is skipped.
  let seconds: number | null = null;
  for (const seg of command.split(/(?:&&|\|\||[;|])/)) {
    const s = parseSleepSeconds(seg);
    if (s !== null && (seconds === null || s > seconds)) seconds = s;
  }
  if (seconds !== null && seconds > 10) {
    const pretty = Number.isInteger(seconds) ? `${seconds}` : seconds.toFixed(2).replace(/\.?0+$/, '');
    return `Blocking sleep for ${pretty}s detected. Use run_in_background: true for long waits, or schedule_wakeup for timed delays.`;
  }
  return null;
}

export const SENSITIVE_SUFFIX_PATTERNS = [
  /_KEY$/i, /_SECRET$/i, /_TOKEN$/i, /_PASSWORD$/i,
  /_CREDENTIALS$/i, /_PRIVATE_KEY$/i, /_API_KEY$/i,
  // Any *_HEADERS var — header values commonly carry Authorization=Bearer tokens.
  /_HEADERS$/i,
];

export const SAFE_PREFIX_RE = /^(PATH|HOME|USER|SHELL|TERM|LANG|DISPLAY|TMPDIR|PWD|OLDPWD|HOSTNAME|LOGNAME|SHLVL|_)$/i;

export const SAFE_PREFIX_PATTERNS = [
  /^LC_/i, /^XDG_/i, /^SUPERINFERENCE_/i, /^NODE_/i,
  /^COLORTERM$/i, /^COLUMNS$/i, /^LINES$/i,
  /^NO_COLOR$/i, /^FORCE_COLOR$/i,
];

export function isSafeEnvVar(key: string): boolean {
  if (SAFE_PREFIX_RE.test(key)) return true;
  return SAFE_PREFIX_PATTERNS.some(p => p.test(key));
}

/**
 * True if `name` resolves to an executable file on any dir of `pathValue`.
 * Delegates to the canonical no-spawn {@link findOnPath} PATH resolver (which
 * handles POSIX `X_OK`, win32 `PATHEXT`, and an injectable env) instead of
 * re-implementing a second PATH scan here (No-Duplication: one `which`-style
 * probe for the whole tree). `pathValue` is threaded through as the probed
 * `PATH` so the python-shim decision stays deterministic/testable.
 */
export function isExecutableOnPath(name: string, pathValue: string | undefined): boolean {
  return findOnPath(name, { PATH: pathValue }) !== null;
}

/**
 * Fan a single configured CA bundle out to every runtime-specific trust-store env
 * var so an operator behind a corporate MITM proxy can point AMI at one custom CA
 * and have the agent's spawned `curl`, `python`/`pip` (requests) and other tools
 * trust it — not just AMI's own Node process. The source of truth is
 * `NODE_EXTRA_CA_CERTS` (the one knob AMI already documents/honors); when it is
 * set, `SSL_CERT_FILE` (OpenSSL/curl), `REQUESTS_CA_BUNDLE` (python-requests) and
 * `CURL_CA_BUNDLE` (curl) are each populated with the same path — but ONLY when
 * the user has not already set that specific var, so an explicit per-runtime
 * override always wins. Parity: the reference's upstreamproxy `getUpstreamProxyEnv`
 * CA-bundle fan-out. Mutates and returns `env`.
 */
export function applyCaBundleFanout(env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const caBundle = env.NODE_EXTRA_CA_CERTS;
  if (caBundle) {
    for (const key of ['SSL_CERT_FILE', 'REQUESTS_CA_BUNDLE', 'CURL_CA_BUNDLE'] as const) {
      if (!env[key]) env[key] = caBundle;
    }
  }
  return env;
}

/**
 * Proxy concepts a spawned child honors, each with the case/name variants
 * different runtimes read. curl/wget/git honor LOWER-case (`https_proxy`,
 * `http_proxy`, `all_proxy`, `no_proxy`) first; Go/gh, python-requests and many
 * JS clients read UPPER-case (`HTTPS_PROXY`, …). A relay/MITM-proxy operator who
 * exports only one case therefore silently loses the other half of the toolchain.
 */
export const PROXY_FANOUT_GROUPS: ReadonlyArray<readonly [string, string]> = [
  ['HTTPS_PROXY', 'https_proxy'],
  ['HTTP_PROXY', 'http_proxy'],
  ['ALL_PROXY', 'all_proxy'],
  ['NO_PROXY', 'no_proxy'],
] as const;

/**
 * Fan each configured proxy var out to its other-case sibling so a child
 * `curl`/`gh`/`python`/`git` all route through the SAME relay regardless of which
 * case the operator (or an upstream relay launcher) exported. For each group the
 * first set variant is the source of truth; the unset sibling is populated with
 * it, but an explicitly-set sibling is never overwritten (explicit per-runtime
 * override always wins). Parity: the reference's upstreamproxy `getUpstreamProxyEnv`
 * injects `HTTPS_PROXY` + friends into every subprocess so relayed sessions work;
 * AMI has no relay process but honors whatever proxy the environment provides and
 * makes it consistent across tools. Mirrors {@link applyCaBundleFanout}. Mutates
 * and returns `env`.
 */
export function applyProxyFanout(env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  for (const [upper, lower] of PROXY_FANOUT_GROUPS) {
    const source = env[upper] ?? env[lower];
    if (source === undefined) continue;
    if (env[upper] === undefined) env[upper] = source;
    if (env[lower] === undefined) env[lower] = source;
  }
  return env;
}

// Closing sleep guidance, always present (it is not git- or background-specific).
// [functional parity] Anti-retry-loop / anti-poll guidance (the reference prompt.ts:L310-L328):
// never retry a failing command inside a sleep loop — diagnose the root cause
// instead of polling. (The companion "you'll be notified, don't poll a background
// task" line lives in BASH_DESCRIPTION_BACKGROUND so it is dropped with the
// run_in_background capability when background tasks are disabled.)
export const BASH_DESCRIPTION_TAIL =
  'Do not sleep between commands. Do not retry a failing command in a sleep loop — diagnose the root cause instead of polling.';

/**
 * Model-facing bash input schema. When background tasks are disabled the
 * `run_in_background` property is omitted entirely from the advertised schema
 * (parity: the reference's BashTool lazySchema omit behind CLAUDE_CODE_DISABLE_BACKGROUND_TASKS)
 * so the model cannot request a background run in a locked-down deployment.
 */
export function buildBashInputSchema(backgroundDisabled: boolean, sandboxEscapeDisabled = false): ToolInputSchema {
  const schema: ToolInputSchema = {
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
          'Run this command in the background. Returns immediately with a task ID; you do NOT need to append `&`. ' +
          'You will be notified when it completes — do not poll. Use task_output to inspect results, task_kill to stop it, ' +
          'or task_list to see all background tasks.',
      },
      background_kind: {
        type: 'string',
        enum: ['bash', 'monitor'],
        description:
          'Kind of background task (only meaningful with run_in_background). Defaults to "bash". ' +
          'Use "monitor" for a long-lived STREAMING command that is EXPECTED to idle between bursts of output ' +
          '(e.g. `tail -f`, a dev server, a log follower): the stall watchdog is disabled for it, so a healthy ' +
          'monitor is not mis-flagged as blocked on an interactive prompt, and its termination is reported as a ' +
          'stream ending rather than a command succeeding or failing.',
      },
      description: {
        type: 'string',
        description:
          'Clear, concise description of what this command does, in active voice (5-10 words for simple commands). ' +
          'Examples: `ls` -> "List files in current directory"; `git status` -> "Show working tree status"; ' +
          '`npm install` -> "Install project dependencies". Avoid vague words like "complex" or "risk".',
      },
      dangerouslyDisableSandbox: {
        type: 'boolean',
        description:
          'Run this command outside the sandbox (no filesystem/network/resource restrictions). ' +
          'When a command fails with evidence of a sandbox restriction — "Operation not permitted", access denied to paths ' +
          'outside the project, or a network/unix-socket connection error — retry that SAME command with ' +
          'dangerouslyDisableSandbox: true. Scope the override to the individual command that needs it; do not disable the ' +
          'sandbox pre-emptively. ' +
          // [functional parity] dangerouslyDisableSandbox guardrails (the reference
          // prompt.ts:L228-L257): prefer the sandbox-writable $TMPDIR for temp
          // files, and never weaken the sandbox to reach credential paths.
          'Prefer $TMPDIR (sandbox-writable) over /tmp for temporary files, and never weaken the sandbox to reach ' +
          'credential paths such as ~/.bashrc, ~/.zshrc, ~/.ssh/*, or other secret files. ' +
          // [functional parity] Unsandboxed-override policy gate wording (the reference
          // sandboxTypes.ts): the flag is NOT a free escape — in enforced
          // container-sandbox mode it is ignored (the command still runs inside
          // the container), and whenever it is honoured it always requires
          // explicit confirmation before the command runs unsandboxed.
          'This flag is ignored in enforced container-sandbox mode (the command still runs inside the container), and ' +
          'when it is honoured running unsandboxed always requires explicit confirmation.',
      },
    },
    required: ['command'],
  };
  if (backgroundDisabled) {
    delete schema.properties.run_in_background;
    // background_kind only gates a background run, so it is meaningless — and must
    // not be advertised — when background tasks are disabled by policy.
    delete schema.properties.background_kind;
  }
  // When policy forbids the escape hatch, omit it from the advertised schema so
  // the model cannot even request it (parity: the reference's prompt states the
  // parameter is 'disabled by policy' and drops it). The runtime also ignores a
  // smuggled flag — see the execute() sandbox branch.
  if (sandboxEscapeDisabled) {
    delete schema.properties.dangerouslyDisableSandbox;
  }
  return schema;
}

export function presentCommandOutput(
  command: string,
  stdout: string,
  stderr: string,
): { stdout: string; stderr: string; headless: BashHeadlessFold } {
  // Headless terminal fold FIRST: collapse `\r` progress-bar overwrites, cursor-
  // addressed redraws and curses alt-screen frames into the final rendered screen
  // so the model sees what a human would, not raw ANSI soup. Heuristic-gated, so
  // plain `\n`-delimited logs (the common case) pass through byte-for-byte; a hard
  // AMI_BASH_HEADLESS_RENDER=off env kill switch disables it entirely. This runs
  // before compactTestLog/stripEmptyLines so those line-oriented passes operate on
  // the already-flattened text.
  const headless = foldBashControlOutput(stdout, stderr, resolveHeadlessRenderMode());
  const foldOut = isTestRunnerCommand(command) || looksLikeSuiteLog(headless.stdout);
  const foldErr = isTestRunnerCommand(command) || looksLikeSuiteLog(headless.stderr);
  // Strip only whole leading/trailing empty lines (a trailing newline from most
  // commands, or a leading blank from a banner) while preserving interior blank
  // lines and indentation, so the model sees clean output without losing layout.
  return {
    stdout: stripEmptyLines(foldOut ? compactTestLog(headless.stdout) : headless.stdout),
    stderr: stripEmptyLines(foldErr ? compactTestLog(headless.stderr) : headless.stderr),
    headless,
  };
}

/** A bare base64 image data URI occupying the whole of a command's stdout. */
export const SHELL_IMAGE_DATA_URI_RE = /^data:(image\/[a-z0-9.+_-]+);base64,([A-Za-z0-9+/=\s]+)$/i;
