// Split from permissions.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { isGitInvocationReadOnly } from './git-invocation';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { POWERSHELL_ACCEPT_EDITS_WRITE_CMDLETS } from './tools/powershell-security';
import { deobfuscateEnvExpansion } from './tools/command-normalize';
import type { PermissionMode } from './permissions-permission-mode-aliases';
import { HARDLINE_PATTERNS, normalizeForHardline } from './permissions-contains-shell-substitution';

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

/**
 * A single destructive-command signature: a {@link RegExp} and the
 * human-readable {@link warning} shown for it. Mirrors the reference's
 * `DestructivePattern` (utils/.../destructiveCommandWarning.ts, bash variant).
 */
export interface DestructivePattern {
  pattern: RegExp;
  warning: string;
}

/**
 * `git clean` is only destructive when it actually removes files: it requires an
 * `-f`/`--force` and must NOT be a `-n`/`--dry-run` preview (which only lists what
 * WOULD be deleted — read-only). Shared by the informational warning engine AND
 * {@link PermissionManager.classifyBashCommand} so a dry-run is never escalated to a
 * destructive-approval prompt in either path (one canonical regex, no duplication).
 * Mirrors the reference's git-clean negative-lookahead on `-n`/`--dry-run` plus the
 * required `-f` (destructiveCommandWarning.ts L67-71).
 */
export const GIT_CLEAN_FORCE_RE = /\bgit\s+clean\b(?![^;&|\n]*(?:-[a-zA-Z]*n|--dry-run))[^;&|\n]*-[a-zA-Z]*f/;

export function detectCommandChaining(command: string): { chained: boolean; operators: string[]; count: number } {
  const stripped = command.replace(/"[^"]*"|'[^']*'/g, '');
  const operators: string[] = [];
  if (/&&/.test(stripped)) operators.push('&&');
  if (/\|\|/.test(stripped)) operators.push('||');
  if (/;/.test(stripped)) operators.push(';');

  const segments = stripped.split(/\s*(?:&&|\|\||;)\s*/).filter(s => s.trim());
  return { chained: operators.length > 0, operators, count: segments.length };
}

/**
 * Path patterns (checked against the full normalized path) for build config
 * files whose identity depends on directory context, not just the basename.
 */
export const BUILD_CONFIG_PATH_PATTERNS: RegExp[] = [
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

// ---------------------------------------------------------------------------
// Agent-own-config guard — edits to the agent's own settings/config dirs need
// explicit approval even in acceptEdits (prompt-injection / self-reconfig vector)
// ---------------------------------------------------------------------------

/**
 * Config/IDE directory segments that must never be silently auto-edited: the
 * agent's own settings (`.ami`, `.ami-plugin`, `.claude`), IDE config (`.vscode`,
 * `.idea`) and the git metadata dir (`.git`). Writing into these can reconfigure
 * the agent, add hooks/commands/skills, or weaponize IDE tasks. Mirrors the reference's
 * DANGEROUS_DIRECTORIES + isClaudeConfigFilePath guards.
 */
export const AGENT_CONFIG_DIR_SEGMENTS = ['.ami', '.ami-plugin', '.claude', '.vscode', '.idea', '.git'];

// ---------------------------------------------------------------------------
// Suspicious path-pattern detection (canonicalization-bypass vectors)
// ---------------------------------------------------------------------------

/** UNC path (`\\server\share` or `//server/share`) — remote-resource / creds vector. */
export function isUncPath(p: string): boolean {
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
 * tightening of the reference's platform-gated "any colon after the drive letter" rule).
 * Mirrors the reference's hasSuspiciousWindowsPathPattern.
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
 * Mirrors the reference's getPathsForPermissionCheck. Non-existent / unreadable hops stop
 * the walk gracefully and still return what was collected.
 *
 * `precomputedPathsToCheck` — when the caller ALREADY holds the canonical, fully
 * symlink-resolved form of `filePath` (e.g. the output of `fs.realpathSync`), the
 * hop list is trivially those paths and the lstat/readlink chain can be skipped
 * entirely. Passing it returns a copy directly, saving up to `maxDepth` syscalls
 * per inner check. Parity with the reference's `isPathAllowed(…, precomputedPathsToCheck)`
 * canonical fast-path (utils/permissions/pathValidation.ts L139-146). Do NOT pass
 * it for a non-canonical path (a nonexistent target, a path with unresolved
 * symlink components) — parent-directory symlink resolution is still required there.
 */
export function getPathsForPermissionCheck(
  filePath: string,
  maxDepth = 40,
  precomputedPathsToCheck?: readonly string[],
): string[] {
  if (precomputedPathsToCheck) return [...precomputedPathsToCheck];
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

/**
 * Sentinel used by the session-stable base-realpath memo to represent "realpath
 * failed" — the empty string is never a valid absolute realpath, and
 * {@link memoizeWithLRU} cannot store `null`/`undefined` (its `Result` is
 * `NonNullable`). A miss is NEVER retained (see
 * {@link resolveSessionStableBaseRealpath}) so a base created later re-resolves.
 */
export const _REALPATH_MISS = '';

/** System directories that should never be written to. */
export const SYSTEM_DIRECTORIES = [
  '/etc', '/usr', '/bin', '/sbin', '/var',
  '/sys', '/proc', '/dev', '/boot', '/lib', '/lib64',
  '/opt', '/Volumes', '/mnt', '/media', '/run/media',
];

export const WINDOWS_DRIVE_ROOT_REGEX = /^[A-Za-z]:\/?$/;

export const WINDOWS_DRIVE_CHILD_REGEX = /^[A-Za-z]:\/[^/]+$/;

/**
 * Is an ALREADY-RESOLVED path a catastrophic target for a removal (`rm`/`rmdir`)?
 * Generalizes the enumerated system-directory HARDLINE list to EVERY direct child
 * of the filesystem root (so `/srv`, `/root`, `/run` — not just the hardcoded
 * `/etc`/`/usr`/… — are caught) plus the whole-directory wildcard, the root
 * itself, the home directory, and Windows drive roots / their direct children.
 * Pure string predicate (no fs access) so it can run on an unvalidated path.
 *
 * Dangerous when the resolved path is:
 * - the bare wildcard `*` or any path ending in `/*` (wipes a whole directory);
 * - the filesystem root `/`;
 * - a Windows drive root (`C:\`, `D:/`);
 * - the user's home directory;
 * - a direct child of root (`/usr`, `/tmp`, `/opt`, `/srv`, …) — but NOT a
 *   grandchild (`/usr/local` is left to the normal flow);
 * - a direct child of a Windows drive root (`C:\Windows`, `C:\Users`).
 *
 * Mirrors the reference's `isDangerousRemovalPath` (utils/permissions/pathValidation.ts).
 */
export function isDangerousRemovalPath(resolvedPath: string): boolean {
  // Collapse mixed/duplicated separators so `C:\\Windows` (valid in PowerShell)
  // and `//srv` don't slip past the drive-child / root-child checks.
  const forwardSlashed = resolvedPath.replace(/[\\/]+/g, '/');

  if (forwardSlashed === '*' || forwardSlashed.endsWith('/*')) return true;

  const normalizedPath =
    forwardSlashed === '/' ? forwardSlashed : forwardSlashed.replace(/\/$/, '');

  if (normalizedPath === '/') return true;
  if (WINDOWS_DRIVE_ROOT_REGEX.test(normalizedPath)) return true;

  const normalizedHome = os.homedir().replace(/[\\/]+/g, '/');
  if (normalizedPath === normalizedHome) return true;

  // Direct children of root: /usr, /tmp, /etc (but not /usr/local).
  if (path.posix.dirname(normalizedPath) === '/') return true;
  if (WINDOWS_DRIVE_CHILD_REGEX.test(normalizedPath)) return true;

  return false;
}

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
export function findMatchingParen(s: string, start: number): number {
  let depth = 1;
  for (let i = start; i < s.length; i++) {
    if (s[i] === '(') depth++;
    if (s[i] === ')') { depth--; if (depth === 0) return i; }
  }
  return -1;
}

/**
 * Hard cap on how deep the recursive command decomposition
 * ({@link extractBaseCommands} into `$(...)`/backticks/subshells/`-c` payloads and
 * {@link extractCommandSubstitutions}'s nested-substitution walk) may descend
 * before it stops. A maliciously deep nesting chain
 * (`$( $( $( … rm -rf / … ) ) )`) would otherwise recurse unbounded and blow the
 * call stack (RangeError) or hang permission evaluation — a denial-of-service on
 * the safety layer itself. Mirrors the reference's `recursionDepth > 10` boundary in
 * `utils/bash/prefix.ts`. Beyond the cap the decomposition returns what it has
 * collected so far; the width guard ({@link MAX_SUBCOMMANDS_FOR_SECURITY_CHECK})
 * independently fails compound commands closed, so a command too deep to fully
 * analyze still can never be *proven* safe.
 */
export const MAX_COMMAND_PARSE_DEPTH = 10;

/**
 * Introspection accessor: the PowerShell edit-adjacent write CMDLETS a given
 * permission {@link PermissionMode} auto-approves without a prompt, SORTED for
 * stable display — the PowerShell counterpart of {@link getAutoAllowedCommands}
 * (which lists only bash base-commands). Only `acceptEdits` auto-allows a fixed
 * set (the SAME canonical {@link POWERSHELL_ACCEPT_EDITS_WRITE_CMDLETS} the runtime
 * {@link isAcceptEditsPowerShellWrite} gate uses — no duplicate table); every
 * other mode returns an empty list. Surfaced through
 * {@link getDefaultPermissionPatterns} (→ `/permissions defaults`) so a user can
 * see the PowerShell cmdlets acceptEdits waves through, alongside the bash set.
 */
export function getAutoAllowedPowerShellCmdlets(mode: PermissionMode): readonly string[] {
  return mode === 'acceptEdits' ? [...POWERSHELL_ACCEPT_EDITS_WRITE_CMDLETS].sort() : [];
}

/**
 * Canonical, reusable command-identity predicates for the two command roots the
 * compound-command guards recognize specially: a directory change (`cd`) and a
 * `git` invocation. Before this, each guard ({@link isGitReadOnlyCommandSafe},
 * {@link countDirectoryChanges}, {@link commandHasCdWriteDesync},
 * {@link commandInvokesGit}) open-coded its own `cmd === 'cd' || cmd === 'pushd'
 * || …` / `cmd === 'git'` test, and the sets had DRIFTED (one counted `popd` but
 * not `set-location`; the others the reverse) so the same command was recognized
 * as a directory change by one guard and not another. These are the single
 * canonical identity checks every guard now shares, mirroring the reference's injectable
 * `CommandIdentityCheckers` (isNormalizedCdCommand / isNormalizedGitCommand,
 * bashCommandHelpers.ts) so directory-change / git recognition is consistent and
 * independently testable.
 *
 * `isNormalizedCdCommand` recognizes every directory-change verb: the POSIX
 * `cd`/`chdir`/`pushd`/`popd` (lowercase, as a real shell spells them) and the
 * PowerShell `Set-Location` cmdlet (case-insensitive, as PowerShell resolves it).
 */
export function isNormalizedCdCommand(token: string | undefined): boolean {
  if (!token) return false;
  return token === 'cd' || token === 'chdir' || token === 'pushd' || token === 'popd'
    || token.toLowerCase() === 'set-location';
}

/** `isNormalizedGitCommand` recognizes a bare `git` base token (env-var prefixes
 *  and `sudo`/`env` wrappers are peeled by {@link extractBaseCommands} before this
 *  predicate sees the token, so it only needs to match the normalized base word). */
export function isNormalizedGitCommand(token: string | undefined): boolean {
  return token === 'git';
}

/**
 * Whole-command git read-only guard for the bash classifier. Splits a (possibly
 * compound) command into segments, validates every git invocation via
 * isGitInvocationReadOnly, and refuses a compound that changes directory before
 * running git (`cd /attacker && git status`) — a sandbox escape where git runs
 * attacker-planted .git/hooks from the new cwd (the reference's cd+git guard).
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
    if (isNormalizedCdCommand(cmd)) hasCd = true;
    if (isNormalizedGitCommand(cmd)) {
      hasGit = true;
      if (!isGitInvocationReadOnly(tokens.slice(i))) return false;
    }
  }
  if (hasCd && hasGit) return false;
  return true;
}

/**
 * HOST/OWNER/REPO exfiltration guard for `gh` read-only commands. A normal repo
 * spec is `OWNER/REPO` (exactly one `/`). Any token (or `--flag=value` inline
 * value) that names a FOREIGN host — a URL scheme (`https://`, `git://`, …), an
 * SSH-style `git@host:owner/repo`, or a 3+-segment `host/owner/repo` (2+ slashes)
 * — redirects the read at an attacker-chosen remote and must NOT be auto-approved.
 * Mirrors the reference's `ghIsDangerousCallback` (readOnlyCommandValidation.ts).
 * `args` is the token list AFTER the `gh <group> <subcommand>` pair.
 */
export function ghArgsTargetForeignHost(args: string[]): boolean {
  for (const token of args) {
    if (!token) continue;
    // For a flag token, only its inline `=value` can carry a repo spec; a bare
    // flag (its value is the NEXT token) is inspected on that next iteration.
    let value = token;
    if (token.startsWith('-')) {
      const eqIdx = token.indexOf('=');
      if (eqIdx === -1) continue;
      value = token.slice(eqIdx + 1);
      if (!value) continue;
    }
    if (!value.includes('/') && !value.includes('://') && !value.includes('@')) continue;
    if (value.includes('://')) return true; // explicit URL scheme
    if (value.includes('@')) return true;   // ssh-style git@host:owner/repo
    const slashCount = (value.match(/\//g) || []).length;
    if (slashCount >= 2) return true;        // host/owner/repo (3+ segments)
  }
  return false;
}
