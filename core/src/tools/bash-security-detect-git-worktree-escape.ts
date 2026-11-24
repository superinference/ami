// Split from bash-security.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { win32 as win32Path } from 'node:path';
import { stripInvisibleUnicode } from './bash-security-strip-invisible-unicode';
import { splitShellSegments } from './bash-security-strip-heredoc-bodies';
import { COMMAND_PREFIX_RE } from './bash-security-get-glob-base-directory';

// ---------------------------------------------------------------------------
// Home-directory expansion — normalizes ~, $HOME, ${HOME} in commands so that
// deny rules on home-directory paths (e.g. Read(~/secrets/**)) match even when
// the command spells the path as $HOME/secrets or ~/secrets.
// ---------------------------------------------------------------------------

export function expandHomeDirReferences(command: string, homeDir?: string): string {
  const home = homeDir ?? process.env.HOME;
  if (!home) return command;

  let result = command;

  // 1. Expand ${HOME} (braced form) — only outside quotes
  result = expandOutsideQuotes(result, /\$\{HOME\}/g, home);

  // 2. Expand $HOME followed by / or end-of-word — only outside quotes
  result = expandOutsideQuotes(result, /\$HOME(?=\/|[\s;|&)]|$)/g, home);

  // 3. Expand ~ at the start of a path token (~/...) — only outside quotes
  //    Must NOT expand ~user (tilde with username) — only bare ~/
  result = expandOutsideQuotes(result, /(?<=^|[\s;|&(=])~(?=\/)/g, home);

  return result;
}

// ---------------------------------------------------------------------------
// Assignment-RHS tilde-expansion guard — parity with the reference's
// walkVariableAssignment tilde rejection (utils/bash/ast.ts L1907-1920).
//
// bash tilde-expands inside an assignment VALUE when the `~` is UNQUOTED and sits
// at the start of the value or immediately after a `:` (the PATH-style list
// separator) — `VAR=~/x` → `VAR=/home/user/x`, `VAR=~root/x` → that user's home,
// `PATH=~/bin:~/tools` → both expand. A later use of the variable
// (`cd $VAR`, `cat $VAR`) then resolves to an absolute path the static argv never
// showed: variable-value tracking is out of scope for a stateless validator, so a
// tilde smuggled through an assignment is UNANALYZABLE and must go to manual
// approval rather than be silently auto-run. A QUOTED tilde (`VAR='~/x'`,
// `VAR="~/x"`) is literal to bash (no expansion) and is NOT flagged.
// ---------------------------------------------------------------------------

/** True iff an UNQUOTED `~` in `value` sits at a bash tilde-expansion position
 *  (value start, or immediately after an unquoted `:`). */
export function tildeExpandsInAssignmentValue(value: string): boolean {
  let inS = false, inD = false, esc = false;
  let prev = '';
  for (let i = 0; i < value.length; i++) {
    const c = value[i];
    if (esc) { esc = false; prev = c; continue; }
    if (c === '\\' && !inS) { esc = true; prev = c; continue; }
    if (c === "'" && !inD) { inS = !inS; prev = c; continue; }
    if (c === '"' && !inS) { inD = !inD; prev = c; continue; }
    if (c === '~' && !inS && !inD && (i === 0 || prev === ':')) return true;
    prev = c;
  }
  return false;
}

export const ASSIGNMENT_DECLARING_BUILTIN_RE = /^(?:export|declare|typeset|local|readonly)\s+/;

export function detectAssignmentTildeExpansion(command: string): { blocked: boolean; reason?: string } {
  if (!command.includes('~')) return { blocked: false };
  for (const seg of splitShellSegments(command)) {
    let rest = seg.replace(/^\s+/, '');
    const lead = ASSIGNMENT_DECLARING_BUILTIN_RE.exec(rest);
    if (lead) rest = rest.slice(lead[0].length);
    // Walk the LEADING run of `NAME=VALUE` assignment words only (bash applies a
    // bare assignment prefix to the command's environment); stop at the first
    // non-assignment word so a tilde in a later ordinary argument is not misread.
    for (;;) {
      const m = /^([A-Za-z_]\w*)=/.exec(rest);
      if (!m) break;
      let i = m[0].length;
      let inS = false, inD = false, esc = false;
      let value = '';
      for (; i < rest.length; i++) {
        const c = rest[i];
        if (esc) { value += c; esc = false; continue; }
        if (c === '\\' && !inS) { esc = true; value += c; continue; }
        if (c === "'" && !inD) { inS = !inS; value += c; continue; }
        if (c === '"' && !inS) { inD = !inD; value += c; continue; }
        if (!inS && !inD && /\s/.test(c)) break; // value ends at unquoted whitespace
        value += c;
      }
      if (tildeExpandsInAssignmentValue(value)) {
        return {
          blocked: true,
          reason: `assignment '${m[1]}=' tilde-expands in its value — bash resolves ~ to an absolute home path that a later use (e.g. cd $${m[1]}) hides from static validation (TOCTOU); requires manual approval`,
        };
      }
      rest = rest.slice(i).replace(/^\s+/, '');
    }
  }
  return { blocked: false };
}

// ---------------------------------------------------------------------------
// `file` command arbitrary read — blocks `-m`/`--magic-file` and
// `-f`/`--files-from` flags that let the `file` command read arbitrary paths,
// including sensitive credential files.  Safe `file path` usage is allowed.
// (the reference changelog 2.1.214: "Changed `file` commands using
// `-m`/`--magic-file` or `-f`/`--files-from` to require permission instead
// of being auto-allowed as read-only")
// ---------------------------------------------------------------------------

export function detectUnsafeFileCommand(command: string): boolean {
  const stripped = stripInvisibleUnicode(command);
  if (!/\bfile\b/.test(stripped)) return false;

  const segments = stripped.split(/\s*(?:;|&&|\|\||\|)\s*/);
  for (const seg of segments) {
    const trimmed = seg.trim();
    const base = trimmed.replace(COMMAND_PREFIX_RE, '').trim();
    if (!/^file\b/.test(base)) continue;

    if (/\s-\S*m\b/.test(base) || /\s--magic-file\b/.test(base)) return true;
    if (/\s-\S*f\b/.test(base) || /\s--files-from\b/.test(base)) return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
// Shell startup file write detection — blocks commands that write/append to
// shell startup files (.bashrc, .zshrc, .bash_profile, .zlogin, .zshenv,
// .profile, .bash_login) or git config files (~/.config/git/).
// These files grant code execution on every new shell, making them high-value
// persistence targets.
// (the reference changelog 2.1.160: "Added a prompt before writing to shell
// startup files and `~/.config/git/`")
// ---------------------------------------------------------------------------

export const SHELL_STARTUP_FILES = [
  '.bashrc', '.bash_profile', '.bash_login', '.bash_logout',
  '.zshrc', '.zshenv', '.zprofile', '.zlogin', '.zlogout',
  '.profile', '.login',
];

export const GIT_CONFIG_HOME_RE = /(?:~|\$HOME|\$\{HOME\})\/\.config\/git\//;

export function expandOutsideQuotes(input: string, pattern: RegExp, replacement: string): string {
  const chars = [...input];
  let inSingle = false;
  let inDouble = false;
  const quoteMap: boolean[] = new Array(input.length).fill(false);

  for (let i = 0; i < input.length; i++) {
    const ch = input[i];
    if (ch === '\\' && !inSingle) {
      quoteMap[i] = true;
      if (i + 1 < input.length) {
        quoteMap[i + 1] = true;
        i++;
      }
      continue;
    }
    if (ch === "'" && !inDouble) {
      inSingle = !inSingle;
      quoteMap[i] = true;
      continue;
    }
    if (ch === '"' && !inSingle) {
      inDouble = !inDouble;
      quoteMap[i] = true;
      continue;
    }
    if (inSingle || inDouble) {
      quoteMap[i] = true;
    }
  }

  // Apply replacements only at positions that are outside quotes
  let result = '';
  let lastIndex = 0;
  const re = new RegExp(pattern.source, pattern.flags.includes('g') ? pattern.flags : pattern.flags + 'g');
  let match: RegExpExecArray | null;
  while ((match = re.exec(input)) !== null) {
    if (!quoteMap[match.index]) {
      result += input.slice(lastIndex, match.index) + replacement;
      lastIndex = match.index + match[0].length;
    }
  }
  result += input.slice(lastIndex);
  return result;
}

// ---------------------------------------------------------------------------
// Git worktree escape detector
// ---------------------------------------------------------------------------
// In worktree-isolated sessions, blocks git commands that redirect operations
// outside the worktree boundary using GIT_DIR, GIT_WORK_TREE env vars,
// or --git-dir, --work-tree, -C flags. Safe read-only operations like
// `git -C . status` are allowed when -C targets the current directory.

export function detectGitWorktreeEscape(command: string): { blocked: boolean; reason?: string } {
  if (!command) return { blocked: false };

  const stripped = stripInvisibleUnicode(command);

  // Block GIT_DIR env var — redirects git's object store
  if (/\bGIT_DIR\s*=/.test(stripped)) {
    return { blocked: true, reason: 'GIT_DIR= can redirect git operations outside the worktree' };
  }

  // Block GIT_WORK_TREE env var — redirects the working tree
  if (/\bGIT_WORK_TREE\s*=/.test(stripped)) {
    return { blocked: true, reason: 'GIT_WORK_TREE= can redirect git operations outside the worktree' };
  }

  // Block GIT_CEILING_DIRECTORIES manipulation — can bypass directory ownership checks
  if (/\bGIT_CEILING_DIRECTORIES\s*=/.test(stripped)) {
    return { blocked: true, reason: 'GIT_CEILING_DIRECTORIES= can bypass git directory ownership checks' };
  }

  // Only check git-specific flags if a git command is present
  if (!/\bgit\b/.test(stripped)) return { blocked: false };

  // Block --git-dir flag — redirects git's object store
  if (/\bgit\b[^;|&]*\s--git-dir[\s=]/.test(stripped)) {
    return { blocked: true, reason: '--git-dir can redirect git operations outside the worktree' };
  }

  // Block --work-tree flag — redirects the working tree
  if (/\bgit\b[^;|&]*\s--work-tree[\s=]/.test(stripped)) {
    return { blocked: true, reason: '--work-tree can redirect git operations outside the worktree' };
  }

  // Block git -C with paths that escape — allow -C . and -C ./subdir
  const gitCMatch = stripped.match(/\bgit\s+-C\s+(\S+)/);
  if (gitCMatch) {
    const target = gitCMatch[1]!.replace(/^["']|["']$/g, '');
    if (target !== '.' && !target.startsWith('./') && target !== '..') {
      if (target.startsWith('/') || target.startsWith('..') || target.startsWith('~')) {
        return { blocked: true, reason: `git -C ${target} can redirect git operations outside the worktree` };
      }
    }
  }

  return { blocked: false };
}

// ---------------------------------------------------------------------------
// Git-internal write / bare-repo hook-injection guard
// ---------------------------------------------------------------------------
// Writing into a repository's .git/ metadata directory (hooks, config, objects,
// refs, HEAD, index) — or planting root-level HEAD/objects/refs so git treats
// the cwd as a bare repo — lets a subsequent `git` invocation execute freshly
// written hook scripts, escaping the sandbox. Also blocks core.hooksPath
// overrides which repoint hook execution at an attacker-controlled directory.
//
// NOTE: this is a STATIC guard over the command string. Detecting that the cwd
// is *already* a disguised bare repo (git's is_git_directory() heuristic / a
// TOCTOU probe) requires filesystem access and belongs in the bash tool wiring,
// not this pure validator — that part is intentionally out of scope here.

export const GIT_INTERNAL_PATH_RE =
  /(?:^|\/)\.git\/(?:hooks|config|objects|refs|HEAD|index|info)\b/;

// NTFS 8.3 short-name form of `.git` (GIT~1) used to bypass literal `.git` checks.
export const GIT_INTERNAL_SHORTNAME_RE =
  /(?:^|\/)GIT~1\/(?:hooks|config|objects|refs|HEAD|index|info)\b/i;

// Win32 CreateFileW strips trailing spaces and dots from every path component
// before it reaches the filesystem, so `.git./hooks/pre-commit`, `.git /hooks`,
// and `.git/hooks./pre-commit` all open the *same* real `.git/hooks/pre-commit`
// as the literal form. Our git-internal matcher keys on the literal `.git/`
// component, so those trailing-dot/space variants silently bypass it on Windows.
// Mirror the reference normalizeGitPathArg (PowerShellTool/gitSafety.ts): strip
// trailing spaces then trailing dots from each component, iterating per
// component, but never collapse the special `.`/`..` segments. POSIX treats a
// trailing dot/space as a distinct real directory, so this normalization is
// applied ONLY when validating as win32.
export function normalizeWin32PathComponents(path: string): string {
  return path
    .split('/')
    .map(c => {
      if (c === '') return c;
      let prev;
      do {
        prev = c;
        c = c.replace(/ +$/, '');
        if (c === '.' || c === '..') return c;
        c = c.replace(/\.+$/, '');
      } while (c !== prev);
      return c;
    })
    .join('/');
}

/**
 * Canonicalize a PowerShell/Windows path ARGUMENT to the real filesystem path
 * the shell would open, so a git-internal target cannot dodge the matcher via PS
 * arg idioms that all resolve to the same file. Superset of
 * {@link normalizeWin32PathComponents}: applies the structural strips first, then
 * the NTFS per-component trailing-space/dot strip. Mirrors the reference
 * normalizeGitPathArg (PowerShellTool/gitSafety.ts L48-87). Win32-only — the
 * caller gates on platform. Input is already forward-slash-converted by the
 * caller (`\` → `/`), so the fully-qualified `Microsoft.PowerShell.Core\FileSystem::`
 * provider prefix arrives with `/` separators.
 */
export function normalizePsGitPathArg(arg: string): string {
  let s = arg;
  // (a) colon-bound parameter prefix: a leading PS dash-char (`-`, en/em-dash,
  //     horizontal-bar) or forward-slash (PS 5.1 `/Path:value`) followed by a
  //     `name:value` — extract the colon-bound value (bug #28). A plain absolute
  //     POSIX path (`/etc/hosts`) has no `:` and is left intact.
  if (s.length > 0 && (s[0] === '-' || s[0] === '\u2013' || s[0] === '\u2014' || s[0] === '\u2015' || s[0] === '/')) {
    const colon = s.indexOf(':', 1);
    if (colon > 0) s = s.slice(colon + 1);
  }
  // (b) surrounding quotes and (c) backtick escapes.
  s = s.replace(/^['"]|['"]$/g, '');
  s = s.replace(/`/g, '');
  // (d) PS provider-qualified prefix: `FileSystem::path` and the fully-qualified
  //     `Microsoft.PowerShell.Core\FileSystem::path` (slashes post-conversion).
  // eslint-disable-next-line security/detect-unsafe-regex -- the repeated segment `[A-Za-z0-9_.]+` and its `/` separator are disjoint (class excludes `/`); the group is bounded to {0,3} and anchored to a literal `FileSystem::`, so matching is linear with no ambiguous overlap
  s = s.replace(/^(?:[A-Za-z0-9_.]+\/){0,3}FileSystem::/i, '');
  // (e) drive-relative `C:foo` (NO separator after the colon) is cwd-relative on
  //     that drive → strip the drive. Absolute `C:/foo` (WITH a separator) is
  //     preserved by the negative lookahead for the cwd-resolution pass.
  s = s.replace(/^[A-Za-z]:(?![/\\])/, '');
  return normalizeWin32PathComponents(s);
}

/**
 * Resolve a path that re-enters cwd through the parent (`../<cwd-basename>/…`) to
 * its cwd-relative form. posix normalization preserves a leading `..` with no cwd
 * context, so `../project/HEAD` (cwd=…/project) stays `../project/HEAD` and misses
 * the bare-repo prefix match even though PowerShell resolves it to `HEAD` at
 * runtime (check/use divergence). Iteratively strips `../<cwd-basename>/` pairs
 * (case-insensitive, matching Windows FS semantics) and maps the exact
 * `../<cwd-basename>` to `.`. Mirrors the reference resolveCwdReentry
 * (PowerShellTool/gitSafety.ts L22-38).
 */
export function resolveCwdReentry(normalized: string, cwd: string): string {
  if (!normalized.startsWith('../')) return normalized;
  const parts = cwd.replace(/\\/g, '/').replace(/\/+$/, '').split('/');
  const cwdBase = (parts[parts.length - 1] || '').toLowerCase();
  if (!cwdBase) return normalized;
  const prefix = '../' + cwdBase + '/';
  let s = normalized;
  while (s.toLowerCase().startsWith(prefix)) s = s.slice(prefix.length);
  if (s.toLowerCase() === '../' + cwdBase) return '.';
  return s;
}

/**
 * Resolve a path that ESCAPES cwd (leading `../` or an absolute/drive form)
 * against the real cwd and, if it lands back INSIDE cwd, return the cwd-relative
 * remainder (lower-cased, matching Windows case-insensitivity) for bare-repo
 * prefix matching; return null when it is genuinely external. This is the SOLE
 * guard for the bare-repo HEAD attack expressed as `<cwd>/HEAD`,
 * `..\<cwd-basename>\HEAD`, or `C:\<full-cwd>\HEAD`, because the per-segment
 * `.git` matcher deliberately excludes bare `HEAD`. Mirrors the reference
 * resolveEscapingPathToCwdRelative (PowerShellTool/gitSafety.ts L91-122).
 */
export function resolveEscapingPathToCwdRelative(n: string, cwd: string): string | null {
  const c = cwd.replace(/\//g, '\\');
  const abs = win32Path.resolve(c, n.replace(/\//g, '\\'));
  const cwdWithSep = c.endsWith('\\') ? c : c + '\\';
  const absLower = abs.toLowerCase();
  const cwdLower = c.toLowerCase();
  const cwdSepLower = cwdWithSep.toLowerCase();
  if (absLower === cwdLower) return '.';
  if (!absLower.startsWith(cwdSepLower)) return null;
  return abs.slice(cwdWithSep.length).replace(/\\/g, '/').toLowerCase();
}

/**
 * Resolve a normalized PS path arg against cwd for bare-repo matching: the
 * cwd-reentry form takes precedence (it rewrites to a cwd-relative path), then
 * the escaping/absolute form. Returns the cwd-relative path, or null if the arg
 * is genuinely external to cwd.
 */
export function resolveGitPathAgainstCwd(n: string, cwd: string): string | null {
  const reentered = resolveCwdReentry(n, cwd);
  if (reentered !== n) return reentered.replace(/^\.\//, '');
  return resolveEscapingPathToCwdRelative(n, cwd);
}

/** True when a cwd-resolved path lands on a git-internal metadata name (bare-repo attack). */
export function matchesResolvedGitInternal(n: string): boolean {
  const low = n.toLowerCase();
  if (low === 'head' || low === '.git') return true;
  if (low.startsWith('.git/')) return true;
  if (/^git~\d+(?:\/|$)/.test(low)) return true;
  for (const p of ['objects', 'refs', 'hooks']) {
    if (low === p || low.startsWith(p + '/')) return true;
  }
  return false;
}

/**
 * Credential/secret DIRECTORY components that, as a `cd`/`pushd`/`Set-Location`
 * TARGET, mean the compound has stepped INTO a secret directory — so a relative
 * operand in a later segment (`cat id_rsa`) resolves at runtime to a secret while
 * the permission layer only ever saw the bare `id_rsa` token. These mirror the
 * credential families in CREDENTIAL_PATH_PATTERNS but are matched as a trailing
 * directory component, which the file-oriented isCredentialPath() patterns (which
 * require a `/file` suffix) deliberately do not cover.
 */
export const SENSITIVE_DIR_RE =
  /(^|\/)(?:\.ssh|\.gnupg|\.aws|\.kube|\.docker|\.config\/(?:gcloud|gh|glab-cli|git)|\.ami\/provision)\/?$/;

/**
 * Absolute system directories that hold secrets/host configuration. A `cd` into
 * one of these means later relative operands resolve against a sensitive system
 * location (`cd /etc && cat shadow`). Narrower than SYSTEM_DIR_PREFIXES (which
 * also lists /usr, /var, /opt — ordinary enough to `cd` into) to keep the
 * cwd-desync escalation high-signal.
 */
export const SENSITIVE_SYSTEM_DIRS = ['/etc', '/root', '/proc', '/sys', '/boot'];
