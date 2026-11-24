// Split from bash-security.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { getOutputRedirections } from './bash-parser';
import { CHECK_IDS, PASS, fail, stripInvisibleUnicode } from './bash-security-strip-invisible-unicode';
import type { SecurityCheckResult } from './bash-security-strip-invisible-unicode';
import { splitShellSegments } from './bash-security-strip-heredoc-bodies';
import { redirectTargetIsDangerousExpansion } from './bash-security-contains-vulnerable-unc-path';

/**
 * Validate every `>`/`>>`/`<` redirect TARGET for a dangerous (non-static)
 * expansion. Unlike {@link validateRedirections} (which blocks ANY redirect and
 * is skipped in opt-in `relaxDetachedValidators` mode), this guard runs in EVERY
 * mode: it is a strict subset of validateRedirections (plain static-path
 * redirects like `> report.txt` stay allowed — essential to detached
 * test-output / report workflows), so it never changes interactive behavior, but
 * it closes the hole where a relaxed-detached run would otherwise let
 * `echo x > $HOME_LIKE_VAR`, `> ~/.ssh/authorized_keys`, or `cat < $SECRET` write
 * to / read from an unvalidated expanded path.
 */
export function validateDangerousRedirectTargets(command: string): SecurityCheckResult {
  let outputs: { target: string }[];
  try {
    outputs = getOutputRedirections(command);
  } catch {
    outputs = [];
  }
  for (const { target } of outputs) {
    if (redirectTargetIsDangerousExpansion(target)) {
      return fail(
        CHECK_IDS.DANGEROUS_REDIRECT_TARGET,
        `Blocked: output redirection target "${target}" uses a shell expansion (variable/glob/tilde/substitution) that resolves to a path static validation cannot verify`,
      );
    }
  }
  for (const target of extractInputRedirectionPaths(command)) {
    if (redirectTargetIsDangerousExpansion(target)) {
      return fail(
        CHECK_IDS.DANGEROUS_REDIRECT_TARGET,
        `Blocked: input redirection target "${target}" uses a shell expansion (variable/glob/tilde/substitution) that resolves to a path static validation cannot verify`,
      );
    }
  }
  return PASS;
}

// ---------------------------------------------------------------------------
// Dangerous rm detector — flags `rm` with unguarded variables
// ---------------------------------------------------------------------------

export function detectDangerousRm(command: string): { dangerous: boolean; suggestion?: string; variable?: string } {
  if (!/\brm\b/.test(command)) return { dangerous: false };

  // Detect rm -rf with command substitution target — the output is unknowable
  // at static analysis time, so it must always require permission.
  // eslint-disable-next-line security/detect-unsafe-regex -- [rfdi] and \s are disjoint, no overlap
  const rmRecursive = /\brm\s+(?:-[rfdi]+\s+)*/;
  if (rmRecursive.test(command)) {
    const afterRm = command.replace(rmRecursive, '');
    if (/\$\(/.test(afterRm) || /`[^`]+`/.test(afterRm)) {
      return {
        dangerous: true,
        suggestion: 'rm with command substitution target — cannot verify safety at analysis time',
      };
    }
  }

  // Detect rm targeting a backslash-only path (e.g. `rm -rf \`)
  // eslint-disable-next-line security/detect-unsafe-regex -- [rfdi] and \s are disjoint, no overlap
  if (/\brm\s+(?:-[rfdi]+\s+)*\\(?:\s|$)/.test(command)) {
    return { dangerous: true, suggestion: 'rm targeting a backslash-only path' };
  }

  // eslint-disable-next-line security/detect-unsafe-regex -- [rfdi] and \s are disjoint, no overlap
  const rmWithVar = /\brm\s+(?:-[rfdi]+\s+)*["']?\$\{?(\w+)\}?(?:\/(\w+))?/;
  const match = command.match(rmWithVar);
  if (!match) return { dangerous: false };
  const varName = match[1];
  const trailingDir = match[2];
  const guardedPattern = new RegExp(`\\$\\{${varName}:[-?]`);
  if (guardedPattern.test(command)) return { dangerous: false };

  // Variable derived from working directory (PWD, pwd output)
  const cwdDerived = new Set(['PWD', 'OLDPWD', 'cwd']);
  if (cwdDerived.has(varName)) {
    return {
      dangerous: true,
      variable: varName,
      suggestion: `rm targeting a variable derived from the working directory ($${varName})`,
    };
  }

  // Variable followed by a top-level directory name (e.g. $PREFIX/usr)
  const topLevelDirs = new Set(['usr', 'etc', 'var', 'bin', 'sbin', 'lib', 'lib64', 'opt', 'boot', 'home', 'root', 'tmp', 'dev', 'proc', 'sys', 'run', 'srv', 'mnt', 'media']);
  if (trailingDir && topLevelDirs.has(trailingDir)) {
    return {
      dangerous: true,
      variable: varName,
      suggestion: `rm with variable followed by top-level directory (/${trailingDir}) — use \${${varName}:?}`,
    };
  }

  return {
    dangerous: true,
    variable: varName,
    suggestion: `Use \${${varName}:?} guard: rm -rf "\${${varName}:?}"`,
  };
}

// ---------------------------------------------------------------------------
// Data-exfiltration detector
// ---------------------------------------------------------------------------
// Blocks commands that combine bulk file reads with outbound network transfer.
// Standalone tar, standalone curl, etc. are fine — it's the combination that
// signals data exfiltration.

export const FILE_READ_CMDS = /\b(tar|zip|cat|base64|dd|gzip|bzip2|xz|zcat|head|tail|xxd|od)\b/;

export const NETWORK_SEND_CMDS = /\b(curl|wget|nc|ncat|netcat|socat)\b/;

export const PIPE_OR_REDIRECT = /[|]|<\s*\//;

/**
 * Split a single shell segment into whitespace-delimited argument tokens with
 * surrounding quotes/escapes removed (so `--repo "evil.com/x/y"` yields the bare
 * value `evil.com/x/y`). This is argument-level tokenization — distinct from
 * {@link splitShellSegments}, which splits on the pipeline/list OPERATORS
 * (`; | &`). Used only by the gh-exfiltration guard to recover gh's repo-spec
 * operands for inspection.
 */
export function tokenizeSegmentArgs(seg: string): string[] {
  const out: string[] = [];
  let cur = '';
  let inS = false, inD = false, esc = false, started = false;
  for (let i = 0; i < seg.length; i++) {
    const c = seg[i];
    if (esc) { cur += c; esc = false; started = true; continue; }
    if (c === '\\' && !inS) { esc = true; started = true; continue; }
    if (c === "'" && !inD) { inS = !inS; started = true; continue; }
    if (c === '"' && !inS) { inD = !inD; started = true; continue; }
    if (!inS && !inD && /\s/.test(c)) {
      if (started) { out.push(cur); cur = ''; started = false; }
      continue;
    }
    cur += c;
    started = true;
  }
  if (started) out.push(cur);
  return out;
}

/**
 * Apply the reference's `ghIsDangerousCallback` rejection rules to gh's argument
 * tokens. gh accepts a repository spec of the form `[HOST/]OWNER/REPO`; when a
 * HOST segment is present (2+ slashes) gh talks to that host's API, so a
 * prompt-injected model can smuggle secrets out as the OWNER segment. Also
 * rejects `://` URLs and `@` SSH-style specs. Both bare (`--repo val`) and
 * equals-attached (`--repo=val`) forms are inspected (cobra accepts both).
 */
export function ghArgsExfiltrate(args: string[]): boolean {
  for (const token of args) {
    if (!token) continue;
    let value = token;
    if (token.startsWith('-')) {
      const eqIdx = token.indexOf('=');
      if (eqIdx === -1) continue; // flag without inline value
      value = token.slice(eqIdx + 1);
      if (!value) continue;
    }
    // Not a repo-spec shape at all → nothing to inspect.
    if (!value.includes('/') && !value.includes('://') && !value.includes('@')) {
      continue;
    }
    if (value.includes('://')) return true; // https://, git://, ssh://, …
    if (value.includes('@')) return true; // git@host:owner/repo
    // 2+ slashes = 3+ segments = [HOST/]OWNER/REPO (normal is OWNER/REPO, 1 slash)
    if ((value.match(/\//g) || []).length >= 2) return true;
  }
  return false;
}

/** True when a token contains a shell expansion that can smuggle runtime bytes. */
export function hasDynamicExpansion(token: string): boolean {
  return /\$[({\w]|`/.test(token);
}

/** True when a token has the shape of a git transport URL (scheme:// or scp-style host:path). */
export function isGitTransportUrlShaped(token: string): boolean {
  // scheme:// form, or scp-style `user@host:path` (host may itself contain an
  // expansion like `git@$HOST:owner/repo`, so match any non-`@` host run).
  return token.includes('://') || /^[^@\s]+@[^@\s]*:/.test(token);
}

// ---------------------------------------------------------------------------
// System directory write guard
// ---------------------------------------------------------------------------

export const SYSTEM_DIR_PREFIXES = [
  '/etc', '/usr', '/bin', '/sbin', '/var',
  '/sys', '/proc', '/dev', '/boot', '/lib', '/lib64', '/opt',
];

export function isSystemDirPath(p: string): boolean {
  const resolved = require('path').resolve(p);
  return SYSTEM_DIR_PREFIXES.some(
    sd => resolved === sd || resolved.startsWith(sd + '/'),
  );
}

// ---------------------------------------------------------------------------
// Input redirection path extraction (Gap #34, #43)
// ---------------------------------------------------------------------------
// Extracts file paths from bash input redirections (`< file`).
// Does NOT flag heredocs (`<< WORD`), herestrings (`<<< string`),
// or `/dev/null` (safe no-op).

export function extractInputRedirectionPaths(command: string): string[] {
  const paths: string[] = [];
  const len = command.length;
  let i = 0;
  let inSingle = false;
  let inDouble = false;

  while (i < len) {
    const ch = command[i];

    if (ch === '\\' && !inSingle && i + 1 < len) {
      i += 2;
      continue;
    }
    if (ch === "'" && !inDouble) {
      inSingle = !inSingle;
      i++;
      continue;
    }
    if (ch === '"' && !inSingle) {
      inDouble = !inDouble;
      i++;
      continue;
    }

    if (ch === '<' && !inSingle && !inDouble) {
      // Check for heredoc (<<) or herestring (<<<)
      if (i + 1 < len && command[i + 1] === '<') {
        // Skip << or <<< — not input redirections
        i += command[i + 2] === '<' ? 3 : 2;
        continue;
      }

      // Skip optional fd number before < (e.g. 0< file)
      let start = i + 1;
      // Skip whitespace after <
      while (start < len && (command[start] === ' ' || command[start] === '\t')) start++;
      if (start >= len) break;

      // Extract the path (possibly quoted)
      let path = '';
      if (command[start] === '"') {
        const end = command.indexOf('"', start + 1);
        if (end > start) {
          path = command.slice(start + 1, end);
          i = end + 1;
        } else {
          i = start + 1;
          continue;
        }
      } else if (command[start] === "'") {
        const end = command.indexOf("'", start + 1);
        if (end > start) {
          path = command.slice(start + 1, end);
          i = end + 1;
        } else {
          i = start + 1;
          continue;
        }
      } else {
        let end = start;
        while (end < len && !/[\s;|&<>()]/.test(command[end])) end++;
        path = command.slice(start, end);
        i = end;
      }

      // Skip /dev/null (safe no-op) and variable expansions
      if (path && path !== '/dev/null' && !path.startsWith('/dev/fd/')) {
        paths.push(path);
      }
      continue;
    }

    i++;
  }

  return paths;
}

export const POLICY_SOURCE_VARIABLES = [
  'SUPERINFERENCE_MANAGED_SETTINGS_PATH',
  'SUPERINFERENCE_CONFIG_DIR',
] as const;

/**
 * Detect BASH_ARGV0 assignment used to masquerade a command's process name.
 * `BASH_ARGV0=fake_name real_command` makes the process appear as `fake_name`
 * in `ps` and `$0`, which can bypass pattern-matching permission checks.
 */
export function detectBashArgv0Assignment(command: string): { blocked: boolean; reason?: string } {
  if (/\bBASH_ARGV0\s*=/.test(command)) {
    return { blocked: true, reason: 'BASH_ARGV0 assignment can masquerade process name — blocks permission-check bypass' };
  }
  return { blocked: false };
}

export function detectBashpidAssignment(command: string): { blocked: boolean; reason?: string } {
  if (/\bBASHPID\s*=/.test(command)) {
    return { blocked: true, reason: 'BASHPID assignment with arithmetic expression can execute code — blocked' };
  }
  return { blocked: false };
}

/**
 * Detect PS4 trace-time RCE. PS4 is re-expanded via promptvars (default on)
 * before every command traced under `set -x` / `bash -x`, so a PS4 value that
 * carries a command substitution or expansion runs at trace time:
 *   PS4='$(id)' && set -x && :        → executes `id`
 *   for PS4 in '$(id)'; do set -x; …  → same, via the loop variable
 * The payload is invisible to argv-based checks because it lives in a variable
 * value. We reject any PS4 *assignment* (plain, `+=`, `export`/`declare -x`/
 * `local`/`readonly`/`typeset` prefixed, env-prefix form, or a `for PS4` loop)
 * whose value contains `$` or a backtick — i.e. a substitution/expansion that
 * promptvars would run. Literal PS4 values (`PS4='+ '`) are allowed, and PS4
 * mentioned as a non-assignment argument (`echo "PS4=$HOME"`) does not match
 * because the check is anchored to the assignment position of each segment.
 */
export function detectPs4TraceRce(command: string): { blocked: boolean; reason?: string } {
  const stripped = stripInvisibleUnicode(command);
  const EXPANSION_RE = /[$`]/;
  // eslint-disable-next-line security/detect-unsafe-regex -- each repeated `-\w+\s+` group starts with a literal `-`, so groups cannot overlap; linear.
  const PREFIX_RE = /^(?:export\s+|declare\s+(?:-\w+\s+)+|local\s+|readonly\s+|typeset\s+(?:-\w+\s+)*)/;
  // Value token after `PS4=` / `PS4+=`: ANSI-C `$'…'`, single/double-quoted,
  // command substitution, backtick, or a bare unquoted run.
  const VALUE_RE = /^PS4\+?=\s*(\$'(?:[^'\\]|\\.)*'|'[^']*'|"[^"]*"|\$\([^)]*\)|`[^`]*`|[^\s]*)/;
  for (const seg of splitShellSegments(stripped)) {
    const trimmed = seg.trim();
    // `for PS4 in …` assigns PS4 each iteration, bypassing assignment checks.
    if (/^for\s+PS4\b/.test(trimmed)) {
      return { blocked: true, reason: 'PS4 loop variable enables trace-time RCE when `set -x` runs — blocked' };
    }
    const body = trimmed.replace(PREFIX_RE, '');
    const m = VALUE_RE.exec(body);
    if (m && EXPANSION_RE.test(m[1] ?? '')) {
      return { blocked: true, reason: 'PS4 assignment carrying a command substitution/expansion executes at trace time (`set -x`) — code-execution vector' };
    }
  }
  return { blocked: false };
}

// ---------------------------------------------------------------------------
// Git credential theft detector
// ---------------------------------------------------------------------------
// Blocks git commands that can exfiltrate or manipulate stored credentials
// without touching credential files directly:
//   - `git credential fill/approve/reject` — dumps or manipulates stored creds
//   - `git config credential.helper <value>` — replaces the helper with a script
//   - `git -c credential.helper=... <cmd>` — inline credential helper override

export function detectGitCredentialTheft(command: string): { blocked: boolean; reason?: string } {
  const stripped = stripInvisibleUnicode(command);

  // 1. Block `git credential fill/approve/reject` — dumps or manipulates stored credentials
  if (/\bgit\s+credential\s+(fill|approve|reject)\b/i.test(stripped)) {
    const match = stripped.match(/\bgit\s+credential\s+(fill|approve|reject)\b/i);
    return { blocked: true, reason: `git credential ${match![1]} can exfiltrate or manipulate stored credentials` };
  }

  // 2. Block `git -c credential.helper=...` — inline credential helper override
  if (/\bgit\b[^;|&]*\s-c\s+credential\.helper\s*=/.test(stripped)) {
    return { blocked: true, reason: 'git -c credential.helper= can override the credential helper with a malicious script' };
  }

  // 3. Block `git config` that writes to credential.helper
  if (/\bgit\s+config\b/.test(stripped) && /credential\.helper/.test(stripped)) {
    // Read-only invocations: --get, --get-all, --get-regexp, --get-urlmatch, --list, -l
    if (/\s(?:--get|--get-all|--get-regexp|--get-urlmatch|--list|-l)\b/.test(stripped)) {
      return { blocked: false };
    }
    // --unset / --unset-all: modifying credential config
    if (/\s--unset(?:-all)?\b/.test(stripped)) {
      return { blocked: true, reason: 'git config --unset credential.helper can remove credential protection' };
    }
    // Check for a value being set after credential.helper
    // `git config credential.helper` alone is a read (prints current value) — allow
    // `git config credential.helper <value>` is a write — block
    if (/credential\.helper\s+\S/.test(stripped)) {
      return { blocked: true, reason: 'git config credential.helper write can replace the credential helper with a malicious script' };
    }
  }

  // 4. Block GIT_CONFIG env vars targeting credential.helper
  if (/\bGIT_CONFIG_VALUE_\d+\s*=/.test(stripped) && /credential\.helper/.test(stripped)) {
    return { blocked: true, reason: 'GIT_CONFIG env vars can override credential.helper' };
  }

  return { blocked: false };
}

// Git config keys whose VALUE is executed as a shell command by git — setting any
// of these is a code-execution vector (beyond credential.helper, handled above;
// core.hooksPath is handled by detectGitInternalWrite). Matches the reference's
// blanket "NEVER mutate git config" posture for the RCE-bearing keys.
export const GIT_RCE_CONFIG_KEY = '(?:core\\.(?:pager|sshcommand|editor|fsmonitor|askpass)|sequence\\.editor|diff\\.external|uploadpack\\.packobjectshook)';

// `--exec-path[=<dir>]` makes git resolve its `git-*` subcommand binaries from the
// given directory — an attacker-controlled path turns any subsequent subcommand
// into arbitrary code execution. The bare `--exec-path` (query) form is harmless,
// so only block the value-bearing `=<dir>` / ` <dir>` forms.
export const GIT_EXEC_PATH_RE = /\bgit\b[^;|&]*\s--exec-path(?:=|\s+)\S/;

// Git environment variables whose VALUE git later runs as a shell command. Setting
// any of these is an RCE vector equivalent to the corresponding config key but via
// the environment, so it is missed by the `git config` / `git -c` matchers above.
// Scoped to command-valued vars with no legitimate agent override (GIT_PAGER /
// GIT_EDITOR are deliberately excluded — `GIT_EDITOR=true` etc. are common and
// benign — to keep this an additive superset without a false-positive flood).
export const GIT_RCE_ENV_RE =
  /\bGIT_(?:SSH_COMMAND|SSH|EXTERNAL_DIFF|PROXY_COMMAND|SEQUENCE_EDITOR|ASKPASS)\s*=/;

// A git alias whose value begins with `!` is run through the shell — `git config
// alias.x '!cmd'` / `git -c alias.x='!cmd'` is a documented code-execution vector.
export const GIT_ALIAS_RCE_RE = /\balias\.[\w.-]+\s*(?:=|\s)\s*['"]?!/;
