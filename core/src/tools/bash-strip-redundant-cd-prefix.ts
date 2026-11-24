// Split from bash.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';
import { windowsPathToPosixPath } from '../utils/windows-paths';
import { coerceNumber } from '../utils/coerce';
import { validateBoundedIntEnvVar } from '../config';

export const MAX_OUTPUT_LENGTH = 200_000;

// Floor for the resolved output cap: the head+tail truncation math in
// truncateBashText/formatOutput needs room for a head slice, a tail slice, and
// the "[truncated …]" banner, so an absurdly small BASH_MAX_OUTPUT_LENGTH is
// clamped up to this minimum rather than producing a degenerate slice.
export const MIN_OUTPUT_LENGTH = 1_000;

// Absolute ceiling for the resolved output cap. A mistaken or hostile
// BASH_MAX_OUTPUT_LENGTH=999999999 would otherwise force the tool to buffer and
// serialize ~1GB of command output per call (memory-exhaustion DoS). Parity with
// the reference's validateBoundedIntEnvVar upper-limit cap: a value above this is
// clamped down to it (and the capped status is debug-logged) rather than honored.
export const DEFAULT_TIMEOUT_MS = 120000;

export const HARD_MAX_TIMEOUT_MS = 3_600_000; // 1 hour

/**
 * Resolve a millisecond env knob to a bounded positive integer, or `undefined`
 * when absent/blank/invalid so the caller applies its own default. Routed through
 * the single canonical {@link validateBoundedIntEnvVar} so every bash timeout
 * knob gets the SAME defensive treatment the rest of the tree uses: NaN /
 * non-positive values are rejected AND a value above `maxMs` is CAPPED (with the
 * capped/invalid status debug-logged) rather than flowing through verbatim — a
 * mistaken or hostile `BASH_MAX_TIMEOUT_MS=999999999` can no longer hang the
 * agent for ~11 days. Parity: the reference's `validateBoundedIntEnvVar`
 * upper-limit cap + invalid/capped reporting (envValidation.ts).
 */
export function parseEnvMs(name: string, maxMs: number): number | undefined {
  const raw = process.env[name];
  if (!raw) return undefined;
  const r = validateBoundedIntEnvVar(name, raw, { default: 0, min: 1, max: maxMs });
  // 'default' (blank) and 'invalid' (NaN / < 1) both mean "let the caller decide".
  return r.status === 'default' || r.status === 'invalid' ? undefined : r.effective;
}

/**
 * Resolve the effective bash timeout. Honors BASH_DEFAULT_TIMEOUT_MS (default
 * when no per-call timeout) and BASH_MAX_TIMEOUT_MS (ceiling), matching the reference
 * Code. A per-call `timeout` still wins but is clamped to the max.
 */
export function resolveBashTimeout(inputTimeout?: unknown): number {
  const def = parseEnvMs('BASH_DEFAULT_TIMEOUT_MS', HARD_MAX_TIMEOUT_MS) ?? DEFAULT_TIMEOUT_MS;
  // [functional parity] The effective ceiling must never be below the configured
  // default. Mirror the reference's getMaxBashTimeoutMs, which floors the max UP to
  // the default via Math.max — a BASH_MAX_TIMEOUT_MS misconfigured below the
  // default is raised to the default rather than silently shrinking every
  // command's timeout below the operator's chosen default.
  const max = Math.max(parseEnvMs('BASH_MAX_TIMEOUT_MS', HARD_MAX_TIMEOUT_MS) ?? HARD_MAX_TIMEOUT_MS, def);
  // [functional parity] semanticNumber coercion: providers routinely serialize a
  // numeric `timeout` as a STRING ("5000"). Coerce it so a numeric string is
  // honored and any non-numeric/garbage shape falls back to the default.
  const requested = coerceNumber(inputTimeout, 0);
  return Math.min(requested > 0 ? requested : def, max);
}

/**
 * Resolve the assistant/detached-mode blocking budget: the wall-clock ceiling
 * after which a still-running FOREGROUND command is auto-moved to the background
 * so the main agent stays responsive. Honors BASH_ASSISTANT_BLOCKING_BUDGET_MS
 * (parity: the reference's ASSISTANT_BLOCKING_BUDGET_MS, advertised at ~15s). Only
 * applied when running in detached/assistant mode; returns `undefined` when the
 * knob is unset so the normal per-call/default timeout governs (preserving AMI's
 * existing behavior unless an operator opts in). A value at/above the effective
 * timeout is a no-op (the timeout already bounds the run).
 */
export function resolveAssistantBlockingBudget(): number | undefined {
  return parseEnvMs('BASH_ASSISTANT_BLOCKING_BUDGET_MS', HARD_MAX_TIMEOUT_MS);
}

export function isBashForkExhaustion(stdout: string, stderr: string): boolean {
  const combined = `${stdout}\n${stderr}`;
  return /fork:\s*Resource temporarily unavailable/i.test(combined)
    || /Cannot fork/i.test(combined)
    || /\bEAGAIN\b/i.test(combined);
}

export const FORK_RETRY_MAX = 5;

export const FORK_RETRY_BASE_MS = 500;

export const DEFAULT_FIND_MAXDEPTH = 20;

export function injectFindMaxdepth(command: string): string {
  if (!/\bfind\b/.test(command)) return command;
  const segments = command.split(/(\s*(?:&&|\|\||;|\|)\s*)/);
  return segments.map(seg => {
    const trimmed = seg.trim();
    const isFindCmd = trimmed.startsWith('find ') || trimmed.startsWith('find\t') || trimmed === 'find' ||
      trimmed.startsWith('sudo find ') || trimmed.startsWith('sudo find\t');
    if (!isFindCmd) return seg;
    if (/-maxdepth\b/.test(trimmed)) return seg;
    // Bounded: reject NaN/non-positive (→ default) and cap the depth so a
    // mistaken/hostile AMI_FIND_MAX_DEPTH=99999999 cannot turn an injected
    // -maxdepth into an unbounded filesystem traversal (resource exhaustion).
    const maxDepth = validateBoundedIntEnvVar('AMI_FIND_MAX_DEPTH', process.env.AMI_FIND_MAX_DEPTH, {
      default: DEFAULT_FIND_MAXDEPTH,
      max: 100,
    }).effective;
    const tokens = trimmed.split(/\s+/);
    let insertIdx = 1;
    for (let i = 1; i < tokens.length; i++) {
      if (tokens[i] === 'sudo') { insertIdx = i + 1; continue; }
      if (!tokens[i].startsWith('-') && tokens[i] !== '(') { insertIdx = i + 1; continue; }
      break;
    }
    tokens.splice(insertIdx, 0, `-maxdepth`, `${maxDepth}`);
    return tokens.join(' ');
  }).join('');
}

/**
 * Strip a redundant leading `cd <cwd> && ` prefix from a command. Models often
 * prefix a command with `cd` to the directory the shell is already in; since the
 * command already executes with `cwd` as its working directory, that prefix is a
 * no-op. Removing it keeps the downstream advisories/caching keyed on the real
 * work (e.g. a `cd <cwd> && git …` won't be mistaken for a relocation) and
 * matches the executed command the model sees. Only the literal current-cwd
 * prefix is stripped — a `cd` into any OTHER directory is preserved.
 *
 * On Windows the session cwd is a native path (`C:\proj`) but a model running
 * under Git Bash routinely emits the POSIX drive form (`cd /c/proj && …`); both
 * variants are stripped. `platform` is injectable so the Windows branch is
 * testable off-Windows.
 * Parity: the reference api.ts BashTool normalizeToolInput (L586-592).
 */
export function stripRedundantCdPrefix(
  command: string,
  cwd: string | undefined,
  platform: NodeJS.Platform = process.platform,
): string {
  if (!cwd) return command;
  // String.replace with a string pattern replaces only the FIRST occurrence and
  // treats the pattern literally (no regex metachar surprises in the path).
  let out = command.replace(`cd ${cwd} && `, '');
  if (platform === 'win32') {
    out = out.replace(`cd ${windowsPathToPosixPath(cwd)} && `, '');
  }
  return out;
}

/**
 * Rewrite Windows CMD-style `>nul` redirects to POSIX `/dev/null`. The model
 * occasionally hallucinates Windows CMD syntax (e.g. `ls 2>nul`) even though the
 * shell is always POSIX; a literal `>nul` makes bash create a file named `nul`,
 * a reserved device name that is extremely hard to delete and breaks `git add .`
 * / `git clone`. Matches `>nul`, `> NUL`, `2>nul`, `&>nul`, `>>nul`
 * (case-insensitive); does NOT match `>null`, `>nul.txt`, or `cat nul.txt`.
 * Parity: the reference utils/bash/shellQuoting.ts rewriteWindowsNullRedirect.
 */
export const NUL_REDIRECT_RE = /(\d?&?>+\s*)[Nn][Uu][Ll](?=\s|$|[|&;)\n])/g;

export function rewriteWindowsNullRedirect(command: string): string {
  return command.replace(NUL_REDIRECT_RE, '$1/dev/null');
}

/**
 * Collapse an over-escaped `find -exec … \\;` terminator to `\;`. Models
 * frequently double-escape the terminator through JSON, emitting
 * `find … -exec rm {} \\;` (literal backslash-backslash-semicolon) which breaks
 * `find` at the shell boundary. Parity: the reference api.ts normalizeToolInput
 * (`normalizedCommand.replace(/\\\\;/g, '\\;')`, L594-595).
 */
export function collapseFindExecEscape(command: string): string {
  return command.replace(/\\\\;/g, '\\;');
}

/**
 * Pre-execution normalization applied to every bash command before security
 * checks and execution: drop a redundant `cd <cwd> && ` prefix (native + Windows
 * POSIX variant), rewrite Windows `>nul` redirects to `/dev/null`, and collapse
 * the double-escaped `find -exec … \\;` terminator. Behavior-preserving
 * transforms that make the command the model actually wrote run correctly on the
 * POSIX shell.
 */
export function normalizeBashInput(command: string, cwd: string | undefined): string {
  let out = stripRedundantCdPrefix(command, cwd);
  out = rewriteWindowsNullRedirect(out);
  out = collapseFindExecEscape(out);
  return out;
}

/**
 * The hardened pwsh flag set (parity: the reference powershell/parser.ts L1171-1179)
 * prepended to a PowerShell invocation: `-NoProfile` skips profile code,
 * `-NonInteractive` avoids prompts/ANSI, `-NoLogo` suppresses the banner. Paired
 * with `-EncodedCommand` (UTF-16LE base64) to run the script without any shell
 * escaping/injection/temp-file issues.
 */
export const HARDENED_PWSH_FLAGS = ['-NoProfile', '-NonInteractive', '-NoLogo'] as const;

/**
 * Encode a PowerShell script as the UTF-16LE base64 string pwsh's `-EncodedCommand`
 * expects (parity: the reference powershell/parser.ts `toUtf16LeBase64`). A base64
 * payload survives any surrounding shell-quoting layer intact.
 */
export function encodeUtf16LeBase64(script: string): string {
  return Buffer.from(script, 'utf16le').toString('base64');
}

// A SIMPLE `pwsh|powershell[.exe] -Command|-c "<script>"` invocation whose single
// quoted script spans to the end of the command. Deliberately conservative: a
// compound/piped command, pre-flag ordering, or an already-`-EncodedCommand` form
// does NOT match, so only the unambiguous common case is rewritten.
export const PWSH_SIMPLE_INVOCATION_RE = /^(\s*)(pwsh|powershell(?:\.exe)?)\s+(?:-Command|-c)\s+(["'])([\s\S]*)\3\s*$/i;

/**
 * Harden a simple PowerShell invocation to pwsh's safe, deterministic form
 * (parity: the reference powershell/parser.ts L1167-1180): rewrite
 * `powershell -Command "<script>"` to
 * `powershell -NoProfile -NonInteractive -NoLogo -EncodedCommand <utf16le-base64>`.
 * The encoded form avoids command-line escaping/injection and interactive-mode
 * ANSI/prompt noise. Non-matching commands (compound, already encoded, other
 * shells) are returned unchanged.
 */
export function rewritePwshInvocation(command: string): string {
  const m = command.match(PWSH_SIMPLE_INVOCATION_RE);
  if (!m) return command;
  const [, lead, exe, , script] = m;
  return `${lead}${exe} ${HARDENED_PWSH_FLAGS.join(' ')} -EncodedCommand ${encodeUtf16LeBase64(script)}`;
}

/**
 * The env gate `AMI_SEARCH_SHADOW` that opts bare `grep`/`find` run through the
 * bash tool into GrepTool-matching default flags (parity: the reference
 * ShellSnapshot.createFindGrepShellIntegration). Default OFF so command behavior is
 * unchanged unless explicitly enabled. Truthy = `1`/`true`/`yes`/`on`.
 */
export const SEARCH_SHADOW_ENV = 'AMI_SEARCH_SHADOW';

export function isSearchShadowEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const v = (env[SEARCH_SHADOW_ENV] ?? '').trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes' || v === 'on';
}

/** VCS metadata dirs excluded from a shadowed `grep` (parity: ShellSnapshot VCS_DIRECTORIES_TO_EXCLUDE). */
export const SEARCH_SHADOW_VCS_DIRS = ['.git', '.svn', '.hg', '.bzr', '.jj', '.sl'] as const;

/**
 * Build the shell preamble that shadows bare `grep`/`find` with GrepTool-matching
 * defaults (parity: the reference ShellSnapshot.createFindGrepShellIntegration — the
 * PORTABLE subset, since AMI ships no embedded bfs/ugrep). First clears any
 * renaming alias (`alias grep=ggrep`) so the function — not the alias — handles the
 * call, then shadows `grep` with the GNU flags GrepTool relies on: `-I` (skip
 * binary files) and `--exclude-dir` for each VCS dir. A later user flag (`-a`,
 * `-E`, `-r`, …) still wins since it appears after these in argv. `find` is left to
 * GNU's default regextype (which already matches GlobTool) — only its alias is
 * cleared, as bfs/ugrep-specific flags don't apply without those binaries.
 */
export function buildSearchShadowPreamble(): string {
  const excludeDirs = SEARCH_SHADOW_VCS_DIRS.map(d => `--exclude-dir=${d}`).join(' ');
  return [
    'unalias grep 2>/dev/null || true',
    'unalias find 2>/dev/null || true',
    `grep() { command grep -I ${excludeDirs} "$@"; }`,
  ].join('\n');
}

/**
 * The env gate `AMI_PLUGIN_HINTS` that opts into surfacing a plugin-install
 * suggestion when a tool emits an `<ami-hint type="plugin" …/>` tag on stderr
 * (parity: the reference's stderr-hint plugin recommendation). Default OFF — the hint
 * TAG is always stripped from output regardless, only the user-facing suggestion is
 * gated. Truthy = `1`/`true`/`yes`/`on`.
 */
export const PLUGIN_HINTS_ENV = 'AMI_PLUGIN_HINTS';

export function isPluginHintsEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const v = (env[PLUGIN_HINTS_ENV] ?? '').trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes' || v === 'on';
}

/**
 * The env gate `AMI_BASH_OUTPUT_FILE_MODE` that opts a BACKGROUND bash command into
 * "file mode": its stdout+stderr are wired STRAIGHT to the task output-file fd so
 * neither stream enters the JS heap/event loop (parity with the reference's TaskOutput
 * `stdoutToFile`, which routes a bash command's output to a file fd so "neither enters
 * JS"). On a very large/fast firehose this keeps V8 out of the hot path — lower CPU/GC
 * and no pipe backpressure — at the cost of the separable in-memory stderr tail (stderr
 * is interleaved in the merged file, exactly as the reference's file mode does). Default OFF
 * (truthy = `1`/`true`/`yes`/`on`) so the normal pipe path — which keeps the distinct
 * `--- stderr ---` task_output section — is unchanged unless an operator opts in.
 */
export const BASH_OUTPUT_FILE_MODE_ENV = 'AMI_BASH_OUTPUT_FILE_MODE';

export function isBashOutputFileModeEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const v = (env[BASH_OUTPUT_FILE_MODE_ENV] ?? '').trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes' || v === 'on';
}

/**
 * Resolve the official-marketplace allowlist (`AMI_PLUGIN_HINT_MARKETPLACES`, a
 * comma list) for the plugin-hint sync gate (parity: the reference's official-marketplace
 * filter). Returns `undefined` when unset — no marketplace restriction is applied.
 */
export const PLUGIN_HINT_MARKETPLACES_ENV = 'AMI_PLUGIN_HINT_MARKETPLACES';

export function resolveOfficialMarketplaces(env: NodeJS.ProcessEnv = process.env): ReadonlySet<string> | undefined {
  const raw = (env[PLUGIN_HINT_MARKETPLACES_ENV] ?? '').trim();
  if (!raw) return undefined;
  const set = new Set(raw.split(',').map(s => s.trim()).filter(Boolean));
  return set.size ? set : undefined;
}

/**
 * Strip leading and trailing lines that contain only whitespace while preserving
 * interior blank lines and in-line indentation. Unlike `String.prototype.trim()`
 * — which also strips meaningful leading indentation of the first content line
 * and trailing spaces of the last — this removes only whole empty boundary lines.
 * Parity: the reference tools/BashTool/utils.ts stripEmptyLines.
 */
export function stripEmptyLines(content: string): string {
  const lines = content.split('\n');
  let start = 0;
  while (start < lines.length && lines[start]?.trim() === '') start++;
  let end = lines.length - 1;
  while (end >= 0 && lines[end]?.trim() === '') end--;
  if (start > end) return '';
  return lines.slice(start, end + 1).join('\n');
}

export const FILE_READ_COMMANDS = /^\s*(?:cat|head|tail|less|more|bat|batcat|grep|egrep|fgrep|sed\s+-n)\b/;

export const GREP_COMMANDS = /^\s*(?:grep|egrep|fgrep)\b/;

/**
 * A single filesystem path a bash read command targets, carrying whether the
 * ORIGINAL token was relative. Relativeness matters for the compound-`cd`
 * read-boundary guard: a relative path after a `cd` in a compound command
 * (`cd ../../other && cat secret`) resolves against the STATIC launch cwd here
 * but at runtime resolves against the NEW cwd, so an "inside workspace" verdict
 * for a relative token can't be trusted and must be re-gated. Absolute tokens are
 * cwd-immune, so they stay trustworthy. Parity: the reference's compoundCommandHasCd
 * forces approval for cwd-relative paths in a cd-bearing compound command.
 */
export interface BashReadPathRef {
  /** The resolved absolute path (relative tokens resolved against `cwd`). */
  resolved: string;
  /** True when the original command token was a relative (non-absolute) path. */
  relative: boolean;
}

/**
 * Extract every filesystem path a bash read command (`cat`/`head`/`tail`/`grep`/
 * `sed -n`/…) targets, together with whether each was a relative token (see
 * {@link BashReadPathRef}). This is the canonical extractor; {@link
 * extractBashReadPaths} is a thin resolved-paths-only wrapper over it.
 */
export function extractBashReadPathRefs(command: string, cwd: string): BashReadPathRef[] {
  const refs: BashReadPathRef[] = [];
  const segments = command.split(/[;&|]+/).map(s => s.trim());
  for (const seg of segments) {
    if (!FILE_READ_COMMANDS.test(seg)) continue;
    const isGrep = GREP_COMMANDS.test(seg);
    const isSed = /^\s*sed\b/.test(seg);
    const rawTokens = seg.match(/[^\s"']+|"[^"]*"|'[^']*'/g) ?? [];
    const tokens = rawTokens.map(t => t.replace(/^["']|["']$/g, ''));
    let patternSkipped = false;
    for (let i = 1; i < tokens.length; i++) {
      const t = tokens[i];
      if (t.startsWith('-')) continue;
      if ((isGrep || isSed) && !patternSkipped) { patternSkipped = true; continue; }
      const relative = !path.isAbsolute(t);
      const resolved = relative ? path.resolve(cwd, t) : t;
      refs.push({ resolved, relative });
    }
  }
  return refs;
}

export function extractBashReadPaths(command: string, cwd: string): string[] {
  return extractBashReadPathRefs(command, cwd).map(r => r.resolved);
}

export function looksLikeHungPrompt(output: string): boolean {
  const lastLine = output.trim().split('\n').pop() ?? '';
  return /\(y\/n\)|\[y\/n\]|\(yes\/no\)|password:|passphrase:|Press Enter|Continue\?|Overwrite\?|Are you sure/i.test(lastLine);
}

/**
 * Classification of a `git commit` invocation. Parity: the reference's
 * gitOperationTracking `CommitKind` distinguishes an amend (history rewrite)
 * from a plain commit, so the two can be surfaced/advised differently. A
 * cherry-pick is a separate git subcommand (not `commit`) and is intentionally
 * NOT classified here — AMI only gates the `commit` subcommand via bash.
 */
export const GIT_COMMIT_KINDS = ['amended', 'committed'] as const;

export type GitCommitKind = (typeof GIT_COMMIT_KINDS)[number];

/**
 * If the first line of a bash command is a `# comment` (not a `#!` shebang),
 * return the comment text with its leading `#`(s) and surrounding whitespace
 * stripped; otherwise `undefined`. Parity: the reference's
 * BashTool/commentLabel `extractBashCommentLabel` — the leading comment a model
 * writes is a human-readable label for the command, used as the tool-use /
 * background-task description before falling back to the raw command text.
 */
export function extractBashCommentLabel(command: string): string | undefined {
  const nl = command.indexOf('\n');
  const firstLine = (nl === -1 ? command : command.slice(0, nl)).trim();
  if (!firstLine.startsWith('#') || firstLine.startsWith('#!')) return undefined;
  return firstLine.replace(/^#+\s*/, '') || undefined;
}
