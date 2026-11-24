// Split from bash-security.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { posix as posixPath } from 'node:path';
import { normalizeCaseForComparison } from '../utils/path';
import { CHECK_IDS, PASS, extractQuotedContent, fail } from './bash-security-strip-invisible-unicode';
import type { SecurityCheckResult } from './bash-security-strip-invisible-unicode';

/**
 * Walk `command` with BASH quoting semantics and report whether the raw string
 * carries an ODD number of unescaped `"` or `'` (an unterminated quote). Mirrors
 * the reference's `hasMalformedTokens` quote scan (shellQuote.ts): a backslash
 * escapes the next character OUTSIDE single quotes, and nothing is escaped inside
 * single quotes — so a `'` that lives inside `"…"` (and vice-versa) does NOT
 * toggle the opposite quote's balance. This fixes the previous naive
 * `.split(quote).length` counter, which miscounted a quote nested inside the
 * other quote type (e.g. the apostrophe in `echo "it's fine"`).
 */
export function hasUnbalancedQuotes(command: string): boolean {
  let inSingle = false;
  let inDouble = false;
  let doubleCount = 0;
  let singleCount = 0;
  for (let i = 0; i < command.length; i++) {
    const c = command[i];
    if (c === '\\' && !inSingle) {
      i++; // backslash escapes the next char outside single quotes
      continue;
    }
    if (c === '"' && !inSingle) {
      doubleCount++;
      inDouble = !inDouble;
    } else if (c === "'" && !inDouble) {
      singleCount++;
      inSingle = !inSingle;
    }
  }
  return doubleCount % 2 !== 0 || singleCount % 2 !== 0;
}

/**
 * After quote stripping, report the name of any structural delimiter pair
 * (`{}`, `()`, `[]`) that has MORE openers than closers — an unclosed opener.
 * Mirrors the balanced-delimiter arm of the reference's `hasMalformedTokens`
 * (shellQuote.ts L145-161), which rejects tokens with unbalanced braces/parens/
 * brackets as malformed. AMI's `validateBraceExpansion` already blocks the
 * EXCESS-CLOSER direction (`close > open`) for `{}`; this complements it by
 * covering the unclosed-OPENER direction for all three delimiter pairs, which is
 * the injection-relevant case: an unclosed `{` lets bash treat the brace as a
 * literal and run whatever follows an embedded separator
 * (`echo {a; rm -rf /tmp/x` → the `rm` runs), and an unclosed `(`/`[` is a
 * malformed token the shell would re-tokenize unpredictably.
 *
 * Counting is done on the fully-unquoted content so delimiters that are literal
 * data inside quotes (e.g. `awk '{print}'`, `grep '[a]'`) are NOT counted, and a
 * backslash escapes the next character so `echo \{` is a literal brace. Only the
 * opener-excess direction is flagged, so legitimate `case … )` patterns (which
 * have `close > open`) and balanced `[ -f x ]` test brackets / `(subshell)` /
 * `$( … )` / `$(( … ))` never false-positive.
 */
export function unclosedStructuralDelimiter(fullyUnquoted: string): string | null {
  const pairs: [string, string, string][] = [
    ['{', '}', 'brace'],
    ['(', ')', 'parenthesis'],
    ['[', ']', 'bracket'],
  ];
  for (const [open, close, name] of pairs) {
    let openN = 0;
    let closeN = 0;
    for (let i = 0; i < fullyUnquoted.length; i++) {
      const c = fullyUnquoted[i];
      if (c === '\\') { i++; continue; }
      if (c === open) openN++;
      else if (c === close) closeN++;
    }
    if (openN > closeN) return name;
  }
  return null;
}

export function validateMalformedTokenInjection(command: string): SecurityCheckResult {
  // Only a concern when a command separator is present: ambiguous/unbalanced
  // quoting combined with `;`/`&&`/`||`/`|`/`&` is the eval-reparse bypass class
  // (HackerOne). Without a separator an unterminated quote is just a malformed
  // single command the shell will reject, not a hidden-command injection.
  if (!/[;|&]/.test(command)) return PASS;
  if (hasUnbalancedQuotes(command)) {
    return fail(CHECK_IDS.MALFORMED_TOKEN, 'Blocked: potentially malformed quoting with command separators — balance the quotes (every \' and " must be paired) or split the command into separate invocations so no unbalanced quote spans a ;/&&/||/| separator');
  }
  // Quotes balance (checked above), so quoted content strips cleanly; scan the
  // remainder for an unclosed structural delimiter that could hide an injected
  // command after an embedded separator.
  const delim = unclosedStructuralDelimiter(extractQuotedContent(command).fullyUnquoted);
  if (delim !== null) {
    return fail(CHECK_IDS.MALFORMED_TOKEN, `Blocked: unclosed ${delim} with command separators (malformed token may hide an injected command) — close the ${delim} or remove the command separator so the token cannot be re-parsed into a hidden command`);
  }
  return PASS;
}

export const MAX_AUTO_APPROVE_LENGTH = 10_000;

/**
 * Upper bound on the number of shell subcommands (pipeline stages / compound
 * segments) a single command may split into before the full per-segment
 * validator battery is worth running. A maliciously or accidentally wide
 * `a && b && c && …` list otherwise drives tree-sitter parsing plus the ~30
 * regex validators on EVERY segment, starving the event loop at 100% CPU
 * (a ReDoS / DoS vector). Fifty is generous — legitimate commands never split
 * this wide — and above it we fail closed (safe default: reject rather than
 * spend unbounded CPU trying to prove safety). Mirrors the reference's
 * MAX_SUBCOMMANDS_FOR_SECURITY_CHECK.
 */
export const MAX_SUBCOMMANDS_FOR_SECURITY_CHECK = 50;

/**
 * Extract FILE operands from a `jq` argv. The first bare positional is the jq
 * PROGRAM (a filter, never a path); subsequent positionals are JSON input files.
 * `--arg name value` / `--argjson name value` carry two non-path values;
 * `--slurpfile name file` / `--rawfile name file` carry a name then a FILE;
 * `-f`/`--from-file` supplies the program from a file (so the first positional is
 * then a data file) and `--indent` takes a numeric value.
 */
export function extractJqPaths(args: string[]): string[] {
  const NAME_VALUE = new Set(['--arg', '--argjson']);
  const NAME_FILE = new Set(['--slurpfile', '--rawfile']);
  const VALUE1 = new Set(['--indent']);
  const paths: string[] = [];
  let programConsumed = false;
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === '--') { for (let j = i + 1; j < args.length; j++) paths.push(args[j]); break; }
    if (a.startsWith('-') && a.length > 1) {
      if (a.includes('=')) continue;
      if (a === '-f' || a === '--from-file') { programConsumed = true; i++; continue; }
      if (NAME_FILE.has(a)) { if (i + 2 < args.length) paths.push(args[i + 2]); i += 2; continue; }
      if (NAME_VALUE.has(a)) { i += 2; continue; }
      if (VALUE1.has(a)) { i++; continue; }
      continue;
    }
    if (!programConsumed) { programConsumed = true; continue; } // jq filter program
    paths.push(a);
  }
  return paths.filter(Boolean);
}

/**
 * `tr` SET-skipping extractor. `tr SET1 SET2` and `tr -d SET1` take character
 * SETs as operands, NOT file paths (tr only ever reads stdin). Skip SET1 (and
 * SET2 unless `-d`/`--delete` is present, which takes a single SET) so a literal
 * set like `A-Za-z` or `/` is never mistaken for a filesystem path. Parity:
 * the reference PATH_EXTRACTORS.tr. tr has no file operands, so the result is
 * always empty — the skip prevents false positives, not path recovery.
 */
export function extractTrPaths(args: string[]): string[] {
  const hasDelete = args.some(
    a => a === '-d' || a === '--delete' || (a.startsWith('-') && a.length > 1 && a.includes('d')),
  );
  const nonFlags = args.filter(x => !x.startsWith('-'));
  return nonFlags.slice(hasDelete ? 1 : 2);
}

/**
 * `sed` file-operand extractor (script-vs-file aware). sed's grammar is
 * `sed [flags] SCRIPT [files...]` unless the script is supplied via `-e`/`-f`,
 * after which EVERY non-flag operand is a file. Specifically:
 *   - `-f`/`--file SCRIPT_FILE` — the next token is a SCRIPT FILE that is itself
 *     read, so it IS surfaced as a path to validate, and marks the script found;
 *   - `-e`/`--expression EXPR` — the next token is an inline expression (skip it,
 *     never a path), and marks the script found;
 *   - any other combined short flag containing `e`/`f` (e.g. `-ne`, `-nf`) also
 *     marks the script found so the first positional is treated as a FILE;
 *   - once the script is found, all remaining non-flag operands (incl. globs like
 *     `*.log`) are file paths.
 * Replaces the prior naive `!/^s[/|]/` filter which mis-read a `'/addr/p'`-style
 * script as a path. Parity: the reference PATH_EXTRACTORS.sed. (`--` end-of-options
 * is handled upstream by {@link extractCommandPaths}, so it never reaches here.)
 */
export function extractSedPaths(args: string[]): string[] {
  const paths: string[] = [];
  let scriptFound = false;
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg.startsWith('-') && arg.length > 1) {
      if (arg === '-f' || arg === '--file') {
        const scriptFile = args[i + 1];
        if (scriptFile) { paths.push(scriptFile); i++; }
        scriptFound = true;
      } else if (arg === '-e' || arg === '--expression') {
        i++; // next token is an inline expression, never a file
        scriptFound = true;
      } else if (arg.includes('e') || arg.includes('f')) {
        scriptFound = true; // combined short flag carrying e/f (e.g. -ne)
      }
      continue;
    }
    if (!scriptFound) { scriptFound = true; continue; } // first bare operand is the script
    paths.push(arg); // every operand after the script is a file (incl. globs)
  }
  return paths;
}

export function looksLikePath(value: string): boolean {
  return /^[/~.]/.test(value) || value.includes('/');
}

/**
 * Credential / persistence paths that an autonomous agent must never write to or
 * copy out of via bash — the SSH-key / cloud-cred exfil & persistence vector.
 */
export const CREDENTIAL_PATH_PATTERNS: RegExp[] = [
  /(^|\/)\.ssh\//,
  /\.aws\/credentials/,
  /\.aws\/config$/,
  /\.config\/gcloud\//,
  /(^|\/)\.gnupg\//,
  /\.docker\/config\.json/,
  /\.kube\/config/,
  /(^|\/)\.netrc$/,
  /(^|\/)_netrc$/,
  /(^|\/)\.git-credentials$/,
  /\.config\/git\/credentials$/,
  /\.config\/gh\//,
  /\.config\/glab-cli\//,
  /(^|\/)\.npmrc$/,
  /\.claude\/\.credentials\.json$/,
  // AMI provider-credential store: the symmetric key (.credkey) sits beside the
  // ciphertext (credentials.enc); both are owner-readable, so without this guard
  // an agent could `cat` them and self-decrypt every provider key. (C1)
  /(^|\/)\.ami\/provision\//,
  /\/etc\/(passwd|shadow|sudoers)\b/,
];

/**
 * Candidate comparison forms for matching a raw path token against a protected-
 * path pattern set. Returns the separator-normalized token PLUS, when it carries
 * `..`/`.` segments, its lexically-collapsed form — so a traversal that
 * RECONSTRUCTS a protected path (`.aws/x/../credentials`, `.ami/a/../sessions/y`)
 * is caught even though the raw string never contains the target contiguously.
 * Case is folded on case-insensitive filesystems (win32/darwin) via the canonical
 * {@link normalizeCaseForComparison} so `.SSH/ID_RSA` cannot evade the (lowercase)
 * patterns; a no-op on Linux. Parity: the reference normalizes (case-fold + `..`
 * collapse) before protected/sensitive-path matching (utils/memoryFileDetection).
 */
/**
 * POSIX filename-glob metacharacters the shell expands against the filesystem:
 * `*`, `?`, `[`, `]`. Braces are deliberately EXCLUDED — bash brace expansion
 * (`{a,b}`) is a literal set generated WITHOUT touching the filesystem, not a
 * path glob, and is handled by the separate redirect/expansion guards. Parity:
 * the reference's GLOB_PATTERN_REGEX (PowerShellTool/pathValidation.ts) is exactly
 * `/[*?[\]]/` with the same explicit brace exclusion.
 */
export const GLOB_METACHAR_RE = /[*?[\]]/;

/**
 * Directory prefix of a glob path — everything up to and including the last path
 * separator BEFORE the first glob metacharacter. `/etc/[star]/shadow` → `/etc/`;
 * `[star].log` → `.` (no separator precedes the glob); a glob-free path → itself.
 * Mirrors the reference's getGlobBaseDirectory (PowerShellTool/pathValidation.ts):
 * the base is the ONLY part of a glob path that can be statically resolved —
 * anything the glob expands to (matched leaves, and symlinks inside the
 * expansion) can never be validated without expanding the glob at runtime (which
 * races with an attacker creating symlinks post-validation).
 */
export function getGlobBaseDirectory(filePath: string): string {
  const m = filePath.match(GLOB_METACHAR_RE);
  if (!m || m.index === undefined) return filePath;
  const beforeGlob = filePath.slice(0, m.index);
  const lastSep = Math.max(beforeGlob.lastIndexOf('/'), beforeGlob.lastIndexOf('\\'));
  if (lastSep === -1) return '.';
  return beforeGlob.slice(0, lastSep + 1) || '/';
}

export function protectedPathComparands(p: string): string[] {
  const norm = normalizeCaseForComparison(p.replace(/\\/g, '/'));
  const out = new Set<string>([norm]);
  // posix.normalize collapses `.`/`..` lexically; absolute inputs keep their
  // leading `/`, relative inputs stay relative — so a traversal that
  // RECONSTRUCTS a protected path is caught.
  if (/(?:^|\/)\.\.?(?:\/|$)/.test(norm)) out.add(posixPath.normalize(norm));
  // Glob-aware comparands. A glob metacharacter sitting on a DIRECTORY component
  // between a sensitive root and a sensitive leaf — `/etc/*/shadow`,
  // `~/.aws/*/credentials` — breaks a contiguous pattern match even though the
  // operand still resolves to the protected leaf at runtime (verified gap:
  // `cat /etc/*/shadow` previously passed unflagged). The leaf a glob matches
  // (including symlinks inside the expansion) cannot be statically validated
  // (parity: the reference getGlobBaseDirectory, finding #15), so we add two
  // conservative comparands: the glob BASE directory, and the path with every
  // glob-bearing SEGMENT dropped (`/etc/*/shadow` → `/etc/shadow`). This only
  // ADDS matches — it fails safe toward asking and never relaxes an existing
  // verdict.
  if (GLOB_METACHAR_RE.test(norm)) {
    out.add(getGlobBaseDirectory(norm));
    const deglobbed = norm.split('/').filter((seg) => !GLOB_METACHAR_RE.test(seg)).join('/');
    if (deglobbed) out.add(deglobbed);
  }
  return [...out];
}

export function isCredentialPath(p: string): boolean {
  const comparands = protectedPathComparands(p);
  return CREDENTIAL_PATH_PATTERNS.some((re) => comparands.some((c) => re.test(c)));
}

// Mount-point roots: any child path represents an entire mounted volume, so
// rm -rf /Volumes/ExternalDisk or /mnt/c must also be blocked (prefix match).
export const NUCLEAR_RM_MOUNT_PREFIXES = ['/Volumes/', '/mnt/', '/media/', '/run/media/'];

export const HOME_EXPANSIONS = /^(?:~\/?|\$HOME\/?|\$\{HOME\}\/?|\$\(echo\s+~\)\/?|\$\(eval\s+echo\s+~\)\/?|\/home\/\w+\/?)$/;

export const RM_RF_RE = /\brm\b[^;]*(?:-\w*[rf]\w*\s+[^;]*-\w*[rf]\w*|-\w*r\w*f\w*|-\w*f\w*r\w*|--recursive\b[^;]*--force\b|--force\b[^;]*--recursive\b)/;

export const DISK_WIPE_RE = /\b(?:mkfs|mkfs\.\w+|wipefs|shred)\b[^|;]*(?:\/dev\/[a-z]|\/dev\/mapper\/)/;

export const DD_WIPE_RE = /\bdd\b[^|;]*(?:of\s*=\s*\/dev\/(?:sd|hd|vd|nvme|xvd|loop|mapper)|of\s*=\s*\/dev\/(?:zero|null|urandom))/;

export const FORK_BOMB_RE = /:\(\)\s*\{\s*:\s*\|\s*:\s*&\s*\}\s*;?\s*:/;

export const IAC_DESTROY_RE = /\b(?:terraform|tofu)\s+(?:apply\s+-destroy|destroy)\b|\b(?:pulumi|cdktf)\s+destroy\b|\bcdk\s+destroy\b/;

// Windows drive-root and system-child removal targets — defense-in-depth for
// cross-platform payloads (parity: the reference isDangerousRemovalPath Windows
// coverage). Separators are optional because command normalization strips the
// escaping backslash from `C:\Windows` → `C:Windows`; a trailing separator run
// is already collapsed by the caller before matching.
export const WINDOWS_DRIVE_ROOT_RE = /^[a-z]:[\\/]*$/i;

// Prefix match (no trailing boundary): command normalization strips the
// escaping backslashes from `C:\Windows\System32`, merging it into the single
// token `C:WindowsSystem32`, so we match the drive + leading system component.
export const WINDOWS_SYSTEM_CHILD_RE = /^[a-z]:[\\/]*(?:windows|winnt|system32|programdata|users)/i;

// eslint-disable-next-line security/detect-unsafe-regex -- each inner group always advances (greedy \S+ then \s+), linear
export const COMMAND_PREFIX_RE = /^(?:sudo\s+(?:-\S+\s+)*|env\s+(?:-\S+\s+)*(?:\S+=\S+\s+)*|[A-Za-z_]\w*=(?:"[^"]*"|'[^']*'|\S+)\s+|command\s+|\\|\/(?:usr\/)?(?:s?bin)\/|(?:watch|ionice|setsid|timeout|stdbuf|nice|nohup|script)\s+(?:(?:-\S+|[\d.]+[smhd]?)\s+)*)/;

export interface NuclearCheckResult {
  blocked: boolean;
  reason?: string;
  /** For rm commands, a concrete rewrite suggestion for a safer alternative. */
  suggestion?: string;
}

export function extractQuotedArg(s: string): string | null {
  const trimmed = s.trimStart();
  if (trimmed.startsWith('"')) {
    const end = trimmed.indexOf('"', 1);
    if (end !== -1) return trimmed.slice(1, end);
  }
  if (trimmed.startsWith("'")) {
    const end = trimmed.indexOf("'", 1);
    if (end !== -1) return trimmed.slice(1, end);
  }
  if (trimmed.startsWith('$\'')) {
    const end = trimmed.indexOf("'", 2);
    if (end !== -1) return trimmed.slice(2, end);
  }
  // Unquoted: take until whitespace or shell meta
  const unquoted = trimmed.match(/^(\S+)/);
  return unquoted ? unquoted[1] : null;
}

export function stripCommandPrefix(segment: string): string {
  let s = segment.trim();
  let prev = '';
  while (prev !== s) {
    prev = s;
    s = s.replace(COMMAND_PREFIX_RE, '').trim();
  }
  return s;
}

export function stripQuotes(s: string): string {
  return s.replace(/^["']|["']$/g, '');
}

export function extractRmTargets(command: string): string[] {
  const targets: string[] = [];
  const tokens = command.split(/\s+/);
  let pastRm = false;
  for (const tok of tokens) {
    if (!pastRm) {
      if (/\brm$/.test(tok)) pastRm = true;
      continue;
    }
    if (tok.startsWith('-') && !tok.startsWith('/')) continue;
    if (tok === '--') continue;
    targets.push(tok);
  }
  return targets;
}
