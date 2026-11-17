import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult, ImageContent, ToolInputSchema } from '../types';
import { detectCommandChaining } from '../permissions';
import { getSessionEnvironmentScript } from '../session-environment';
import { execCommand, SPAWN_FAILURE_EXIT_CODE } from '../utils/shell';
import { windowsPathToPosixPath } from '../utils/windows-paths';
import { coerceBool, coerceNumber } from '../utils/coerce';
import { validateBashSecurity, detectCredentialPathWrite, detectSessionTranscriptWrite, detectDataExfiltration, detectCodeLoadingEnvSet, detectNuclearCommand, detectBashArgv0Assignment, detectBashpidAssignment, sanitizeToolOutput, joinLineContinuations, commandInvokesGit } from './bash-security';
import { shouldUseSandbox, wrapWithSandbox, hostSandboxConfigFromPolicy, isSandboxPlatformEnabled, validateSandboxEnabled, describeSandboxFallbackWarning, type SandboxExemptionOptions } from './bash-sandbox';
import { validateBoundedIntEnvVar } from '../config';
import {
  getDockerAvailability,
  describeDockerUnavailable,
  annotateStderrWithSandboxFailures,
  wrapWithDocker,
  DEFAULT_DOCKER_IMAGE,
} from './docker-sandbox';
import { spillToolText } from './spill-output';
import { compactTestLog, isTestRunnerCommand, looksLikeSuiteLog } from './test-log';
import { isReadOnlyBashCommand as isCacheableReadOnly, BashResultCache } from './bash-cache';
import { compressImageBuffer, detectImageFormatFromBase64, createImageMetadataText, ImageResizeError, IMAGE_TARGET_RAW_SIZE } from './file-read';
import { isBackgroundTasksDisabled, isGitInstructionsDisabled, isHostSandboxDisabled, isSandboxDefaultDeny, isSandboxEscapeDisabled } from './tool-gates';

const MAX_OUTPUT_LENGTH = 200_000;
// Floor for the resolved output cap: the head+tail truncation math in
// truncateBashText/formatOutput needs room for a head slice, a tail slice, and
// the "[truncated …]" banner, so an absurdly small BASH_MAX_OUTPUT_LENGTH is
// clamped up to this minimum rather than producing a degenerate slice.
const MIN_OUTPUT_LENGTH = 1_000;
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
 * [functional parity] Resolve the effective bash output-character cap. Honors
 * BASH_MAX_OUTPUT_LENGTH (parity: Claude Code's BASH_MAX_OUTPUT_LENGTH tuning
 * knob — already registered in env-protection's SAFE_ENV_VARS and merged by the
 * config layer, but previously never READ by the tool, so setting it had no
 * effect). Falls back to the 200k default when unset, non-numeric, or
 * non-positive; a positive value is floored at MIN_OUTPUT_LENGTH so the head+tail
 * truncation math stays valid. Mirrors resolveBashTimeout's env-override pattern.
 */
export function resolveBashMaxOutput(): number {
  const raw = process.env.BASH_MAX_OUTPUT_LENGTH;
  if (!raw) return MAX_OUTPUT_LENGTH;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return MAX_OUTPUT_LENGTH;
  return Math.max(MIN_OUTPUT_LENGTH, n);
}

/**
 * Resolve the effective bash timeout. Honors BASH_DEFAULT_TIMEOUT_MS (default
 * when no per-call timeout) and BASH_MAX_TIMEOUT_MS (ceiling), matching Claude
 * Code. A per-call `timeout` still wins but is clamped to the max.
 */
export function resolveBashTimeout(inputTimeout?: unknown): number {
  const def = parseEnvMs('BASH_DEFAULT_TIMEOUT_MS') ?? DEFAULT_TIMEOUT_MS;
  // [functional parity] The effective ceiling must never be below the configured
  // default. Mirror Claude Code's getMaxBashTimeoutMs, which floors the max UP to
  // the default via Math.max — a BASH_MAX_TIMEOUT_MS misconfigured below the
  // default is raised to the default rather than silently shrinking every
  // command's timeout below the operator's chosen default.
  const max = Math.max(parseEnvMs('BASH_MAX_TIMEOUT_MS') ?? HARD_MAX_TIMEOUT_MS, def);
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
 * (parity: Claude Code's ASSISTANT_BLOCKING_BUDGET_MS, advertised at ~15s). Only
 * applied when running in detached/assistant mode; returns `undefined` when the
 * knob is unset so the normal per-call/default timeout governs (preserving AMI's
 * existing behavior unless an operator opts in). A value at/above the effective
 * timeout is a no-op (the timeout already bounds the run).
 */
export function resolveAssistantBlockingBudget(): number | undefined {
  return parseEnvMs('BASH_ASSISTANT_BLOCKING_BUDGET_MS');
}

/** Ceiling / default for a backgrounded (run_in_background) command's own timeout. */
const MAX_BG_TIMEOUT_MS = 2 * 60 * 60 * 1000;
const DEFAULT_BG_TIMEOUT_MS = 30 * 60 * 1000;

/**
 * Resolve a backgrounded command's timeout (ms). A positive per-call `timeout`
 * wins; otherwise the SUPERINFERENCE_BACKGROUND_COMMAND_TIMEOUT_MS env knob, then
 * the 30-minute default — all clamped to the 2-hour ceiling. The env knob is read
 * via {@link parseEnvMs}, which rejects NaN AND non-positive values (so
 * `=-5`/`=0`/`=abc` fall back to the default): the previous inline
 * `parseInt(raw,10)` idiom let a negative value through, yielding a nonsensical
 * negative timeout (and `=abc` → NaN → `Math.min(NaN, …)` → NaN). Parity: Claude
 * Code's envValidation rejects `isNaN(parsed) || parsed <= 0`.
 */
export function resolveBackgroundTimeoutMs(inputTimeout: unknown): number {
  const coerced = coerceNumber(inputTimeout, 0);
  const requested = coerced > 0 ? coerced : undefined;
  const envMs = parseEnvMs('SUPERINFERENCE_BACKGROUND_COMMAND_TIMEOUT_MS');
  return Math.min(requested ?? envMs ?? DEFAULT_BG_TIMEOUT_MS, MAX_BG_TIMEOUT_MS);
}

let _cachedPermissionManager: InstanceType<typeof import('../permissions').PermissionManager> | null = null;

/**
 * Dependency seams for tests (mirrors the _enterWorktreeHooks pattern). Each
 * default is the real implementation, so production behaviour is unchanged.
 * Tests use node:test `mock.method` on this object to drive otherwise
 * unreachable arms: a fork-storm that never recovers (execCommand + sleep),
 * a command classifier that throws, and a statSync race during a git-checkout
 * discard check.
 */
export const _bashHooks = {
  execCommand: execCommand as typeof execCommand,
  sleep: (ms: number): Promise<void> => new Promise(r => setTimeout(r, ms)),
  classify: (command: string): string => {
    if (!_cachedPermissionManager) {
      const { PermissionManager } = require('../permissions');
      _cachedPermissionManager = new PermissionManager();
    }
    return _cachedPermissionManager!.classifyBashCommand(command);
  },
  existsSync: fs.existsSync as typeof fs.existsSync,
  statSync: fs.statSync as typeof fs.statSync,
  // realpath seam for the working-directory resolver (TOCTOU-safe resolve +
  // delete-recovery). Default is the real fs.realpathSync; tests drive the
  // not-found / recovery-exhausted branches through this seam.
  realpathSync: ((p: fs.PathLike): string => fs.realpathSync(p, 'utf8')) as (p: fs.PathLike) => string,
  // Defense-in-depth exfiltration re-check. validateBashSecurity already runs
  // detectDataExfiltration, so this standalone guard only fires if that upstream
  // check is ever narrowed; the seam lets tests exercise this safety net.
  detectDataExfiltration: detectDataExfiltration as typeof detectDataExfiltration,
  // Sandbox-decision seams: let tests drive the non-container sandbox path
  // (force shouldUseSandbox true, assert wrapWithSandbox is/ISN'T invoked) so the
  // dangerouslyDisableSandbox coercion is red-green verifiable without a real
  // namespace. Defaults are the real implementations — production is unchanged.
  shouldUseSandbox: shouldUseSandbox as typeof shouldUseSandbox,
  wrapWithSandbox: wrapWithSandbox as typeof wrapWithSandbox,
  // Weaker-fallback warning seam: lets tests drive the degraded-isolation advisory
  // (host sandbox fell back to ulimits-only while a carve-out was requested)
  // deterministically without a host that actually lacks `unshare`. Default is the
  // real pure helper — production surfaces the live backend decision.
  describeSandboxFallbackWarning: describeSandboxFallbackWarning as typeof describeSandboxFallbackWarning,
};

function isReadOnlyBashCommand(command: string): boolean {
  try {
    return _bashHooks.classify(command) === 'safe';
  } catch {
    return false;
  }
}

/**
 * Bare-repository attack probe (parity with Claude Code's
 * `isCurrentDirectoryBareGitRepo`). When `cwd` contains bare-repo indicators —
 * a root-level `HEAD` file, an `objects/` directory, or a `refs/` directory —
 * WITHOUT a valid `.git/HEAD`, git's `setup_git_directory()` treats `cwd`
 * itself as a (bare) repository and will execute hook scripts planted in `cwd`.
 * An attacker seeds `hooks/pre-commit` + `objects/` + `HEAD` and deletes
 * `.git/HEAD`, so the next allowed `git` command runs arbitrary code.
 *
 * This is a runtime filesystem probe (not a string check), complementary to
 * {@link detectGitInternalWrite} (which blocks WRITING those paths) and to the
 * sandbox-level `buildBareGitRepoGuard` (which ro-binds markers inside the
 * sandbox): this probe fires before an UNsandboxed git invocation even when the
 * markers already exist. fs access goes through `_bashHooks` so tests can drive
 * it without a real on-disk repo.
 */
export function isCurrentDirectoryBareGitRepo(cwd: string | undefined): boolean {
  if (!cwd) return false;
  const statSync = _bashHooks.statSync;

  // A valid .git (dir with a regular HEAD file, or a gitfile for worktrees/
  // submodules) means git will NOT fall back to treating cwd as a bare repo.
  try {
    const gitStats = statSync(path.join(cwd, '.git'));
    if (gitStats.isFile()) return false; // worktree/submodule gitfile
    if (gitStats.isDirectory()) {
      try {
        if (statSync(path.join(cwd, '.git', 'HEAD')).isFile()) return false;
      } catch {
        // .git/ exists but has no HEAD — fall through to indicator check
      }
    }
  } catch {
    // no .git at all — fall through to indicator check
  }

  // No valid .git/HEAD. Flag if ANY bare-repo indicator exists in cwd.
  try {
    if (statSync(path.join(cwd, 'HEAD')).isFile()) return true;
  } catch { /* no HEAD */ }
  try {
    if (statSync(path.join(cwd, 'objects')).isDirectory()) return true;
  } catch { /* no objects/ */ }
  try {
    if (statSync(path.join(cwd, 'refs')).isDirectory()) return true;
  } catch { /* no refs/ */ }
  return false;
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
  let last = await _bashHooks.execCommand(command, options);
  for (let attempt = 0; attempt < FORK_RETRY_MAX && isBashForkExhaustion(last.stdout, last.stderr); attempt++) {
    const base = FORK_RETRY_BASE_MS * Math.pow(2, attempt);
    const jitter = Math.floor(Math.random() * base);
    await _bashHooks.sleep(base + jitter);
    last = await _bashHooks.execCommand(command, options);
  }
  if (isBashForkExhaustion(last.stdout, last.stderr)) {
    last.stderr += '\n[fork exhaustion: all retries failed — too many concurrent processes. Try closing other sessions.]';
  }
  return last;
}

const DEFAULT_FIND_MAXDEPTH = 20;

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
 * Parity: Claude Code api.ts BashTool normalizeToolInput (L586-592).
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
 * Parity: Claude Code utils/bash/shellQuoting.ts rewriteWindowsNullRedirect.
 */
const NUL_REDIRECT_RE = /(\d?&?>+\s*)[Nn][Uu][Ll](?=\s|$|[|&;)\n])/g;
export function rewriteWindowsNullRedirect(command: string): string {
  return command.replace(NUL_REDIRECT_RE, '$1/dev/null');
}

/**
 * Collapse an over-escaped `find -exec … \\;` terminator to `\;`. Models
 * frequently double-escape the terminator through JSON, emitting
 * `find … -exec rm {} \\;` (literal backslash-backslash-semicolon) which breaks
 * `find` at the shell boundary. Parity: Claude Code api.ts normalizeToolInput
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
 * Resolve the working directory for a bash invocation without a check-then-use
 * (TOCTOU) race, and recover when the directory was deleted out from under the
 * session. Rather than a separate `existsSync` pre-check, it `realpath`s the cwd
 * (canonicalizing symlinks) and acts on the result; if the cwd no longer exists
 * (`ENOENT`/`ENOTDIR`) it falls back to the session's original launch directory
 * (`fallback`, default `process.cwd()`), returning `{ recovered: true }` so the
 * caller can surface the reset. Returns `null` only when neither the cwd nor the
 * fallback can be resolved. Parity: Claude Code Shell.ts `setCwd` (realpath-and-
 * act, no existsSync pre-check) + the delete-recovery fallback to the original
 * cwd.
 */
export function resolveWorkingDirectory(
  cwd: string,
  fallback: string = process.cwd(),
): { dir: string; recovered: boolean } | null {
  try {
    return { dir: _bashHooks.realpathSync(cwd), recovered: false };
  } catch (err) {
    const code = (err as NodeJS.ErrnoException)?.code;
    // A non-existence error is recoverable; any other failure (e.g. EACCES) is
    // surfaced as an unresolved cwd so the caller errors rather than silently
    // relocating the command.
    if (code !== 'ENOENT' && code !== 'ENOTDIR') return null;
  }
  // The cwd is gone — recover to the launch directory if it is a different,
  // still-existing path.
  if (path.resolve(fallback) === path.resolve(cwd)) return null;
  try {
    return { dir: _bashHooks.realpathSync(fallback), recovered: true };
  } catch {
    return null;
  }
}

/**
 * Strip leading and trailing lines that contain only whitespace while preserving
 * interior blank lines and in-line indentation. Unlike `String.prototype.trim()`
 * — which also strips meaningful leading indentation of the first content line
 * and trailing spaces of the last — this removes only whole empty boundary lines.
 * Parity: Claude Code tools/BashTool/utils.ts stripEmptyLines.
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

/**
 * Base commands that normally produce NO stdout on success (`mkdir`, `mv`, …).
 * When a command is composed entirely of these, an empty stdout is expected, so
 * the UI shows a success affordance instead of a misleading "(no output)".
 * Parity: Claude Code BashTool BASH_SILENT_COMMANDS.
 */
const BASH_SILENT_COMMANDS = new Set([
  'mv', 'cp', 'rm', 'mkdir', 'rmdir', 'chmod', 'chown', 'chgrp', 'touch', 'ln',
  'cd', 'export', 'unset', 'wait',
]);

/**
 * Commands that carry no semantic weight; after a `||` they are pure fallbacks
 * (e.g. `rm x || echo "failed"`), so they do not make a silent command noisy.
 * Parity: Claude Code BashTool BASH_SEMANTIC_NEUTRAL_COMMANDS.
 */
const BASH_SEMANTIC_NEUTRAL_COMMANDS = new Set(['echo', 'printf', 'true', 'false', ':']);

/**
 * True if a bash command is expected to produce no stdout on success — used to
 * show a "no output expected" affordance instead of a misleading empty result.
 *
 * Unlike the old leading-command regex (which only inspected the FIRST command
 * and a narrow set), this is compound/pipeline-aware: the command is split on
 * the sequencing operators `&&`, `||`, `;`, `|`, and EVERY non-fallback segment's
 * base command must be in {@link BASH_SILENT_COMMANDS}. A semantic-neutral
 * command following `||` (an error-path fallback like `… || echo failed`) is
 * skipped rather than counted as noisy output. Returns false when any segment is
 * not silent (e.g. `cp a b && ls`, `rm x | tee log`) or when there is no real
 * command at all. Parity: Claude Code BashTool isSilentBashCommand.
 */
export function isSilentBashCommand(command: string): boolean {
  // Split while capturing the sequencing operator before each segment so the
  // `||`-fallback rule can look back at the operator that introduced a segment.
  const parts = command.split(/\s*(&&|\|\||;|\|)\s*/).map(s => s.trim()).filter(Boolean);
  if (parts.length === 0) return false;
  let hasNonFallbackCommand = false;
  let lastOperator: string | null = null;
  for (const part of parts) {
    if (part === '&&' || part === '||' || part === ';' || part === '|') {
      lastOperator = part;
      continue;
    }
    // The base command is the first whitespace-delimited token; a redirect
    // target can never be a base command because it is never segment-leading.
    const baseCommand = part.split(/\s+/)[0];
    if (!baseCommand) continue;
    // A neutral command used as a `||` fallback does not make the command noisy.
    if (lastOperator === '||' && BASH_SEMANTIC_NEUTRAL_COMMANDS.has(baseCommand)) continue;
    hasNonFallbackCommand = true;
    if (!BASH_SILENT_COMMANDS.has(baseCommand)) return false;
  }
  return hasNonFallbackCommand;
}

const FILE_READ_COMMANDS = /^\s*(?:cat|head|tail|less|more|bat|batcat|grep|egrep|fgrep|sed\s+-n)\b/;
const GREP_COMMANDS = /^\s*(?:grep|egrep|fgrep)\b/;

/**
 * A single filesystem path a bash read command targets, carrying whether the
 * ORIGINAL token was relative. Relativeness matters for the compound-`cd`
 * read-boundary guard: a relative path after a `cd` in a compound command
 * (`cd ../../other && cat secret`) resolves against the STATIC launch cwd here
 * but at runtime resolves against the NEW cwd, so an "inside workspace" verdict
 * for a relative token can't be trusted and must be re-gated. Absolute tokens are
 * cwd-immune, so they stay trustworthy. Parity: Claude's compoundCommandHasCd
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

function looksLikeHungPrompt(output: string): boolean {
  const lastLine = output.trim().split('\n').pop() ?? '';
  return /\(y\/n\)|\[y\/n\]|\(yes\/no\)|password:|passphrase:|Press Enter|Continue\?|Overwrite\?|Are you sure/i.test(lastLine);
}

/** Global git options that take a separate value token (so the token after them
 *  is a value, not the subcommand). */
export const GIT_VALUE_OPTS = new Set([
  '-C', '-c', '--git-dir', '--work-tree', '--namespace', '--exec-path',
  '--super-prefix', '--config-env',
]);

/**
 * Classification of a `git commit` invocation. Parity: Claude Code's
 * gitOperationTracking `CommitKind` distinguishes an amend (history rewrite)
 * from a plain commit, so the two can be surfaced/advised differently. A
 * cherry-pick is a separate git subcommand (not `commit`) and is intentionally
 * NOT classified here — AMI only gates the `commit` subcommand via bash.
 */
export type GitCommitKind = 'amended' | 'committed';

/**
 * Classify a bash command that runs `git commit`, or `null` when it does not.
 * Returns `'amended'` when the commit carries `--amend` (or a fused
 * `--amend=…`), else `'committed'`. Shares one linear, ReDoS-free tokenizer
 * with {@link detectGitCommit}: it catches `git -C /x commit`, `git -c k=v
 * commit`, `git --git-dir /x commit`, path-qualified `/usr/bin/git commit`,
 * etc., and the final subcommand must be exactly `commit` (not
 * `commit-graph`/`commit-tree`). The `--amend` scan stops at the next `git`
 * token so a later chained invocation's flags cannot bleed into this one.
 */
export function detectGitCommitKind(command: string): GitCommitKind | null {
  const stripped = command.replace(/"[^"]*"|'[^']*'/g, '');
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
    if (j < tokens.length && tokens[j] === 'commit') {
      // Inspect only THIS commit's own flags for --amend; stop at the next git
      // invocation so `git commit && git log --amend-ish` is not misclassified.
      for (let k = j + 1; k < tokens.length; k++) {
        const t = tokens[k];
        if (t.replace(/^.*\//, '') === 'git') break;
        if (t === '--amend' || t.startsWith('--amend=')) return 'amended';
      }
      return 'committed';
    }
  }
  return null;
}

/**
 * Whether a bash command runs `git commit` (any kind). Thin predicate over
 * {@link detectGitCommitKind} — one canonical tokenizer, no duplication.
 */
export function detectGitCommit(command: string): boolean {
  return detectGitCommitKind(command) !== null;
}

/**
 * If the first line of a bash command is a `# comment` (not a `#!` shebang),
 * return the comment text with its leading `#`(s) and surrounding whitespace
 * stripped; otherwise `undefined`. Parity: Claude Code's
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
    return _bashHooks.existsSync(abs) && _bashHooks.statSync(abs).isFile();
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

/**
 * Non-blocking advisory for ANY force push, independent of branch name.
 * `detectForcePushToProtected` hard-blocks force pushes to protected branches
 * (main/master/…); this surfaces Claude Code's destructiveCommandWarning
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

const SLEEP_SUFFIX_SECONDS: Record<string, number> = { s: 1, m: 60, h: 3600, d: 86400 };

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

/**
 * Leading tokens that must NOT be auto-moved to the background when a foreground
 * command overruns its timeout. A `sleep` that times out is a pointless process
 * to keep alive as a background task — the model wanted a delay, not a
 * long-running job — so it stays in the foreground and surfaces a plain timeout
 * error instead of lingering as a zombie background task. Parity: Claude Code
 * BashTool `DISALLOWED_AUTO_BACKGROUND_COMMANDS` (bashCommandHelpers.ts:219-220).
 */
const DISALLOWED_AUTO_BACKGROUND_COMMANDS = new Set(['sleep']);

/**
 * Whether a timed-out foreground command may be AUTOMATICALLY moved to the
 * background. False when the command's leading token is in
 * {@link DISALLOWED_AUTO_BACKGROUND_COMMANDS} (e.g. `sleep`, `/bin/sleep`,
 * `sleep.exe`), which must surface a timeout error rather than linger as a
 * background task. This gate applies ONLY to automatic backgrounding on timeout;
 * an explicit `run_in_background: true` still honours the request regardless of
 * the command. Parity: Claude Code BashTool `isAutobackgroundingAllowed`
 * (bashCommandHelpers.ts:307-315) gating `shouldAutoBackground`.
 */
export function isAutobackgroundingAllowed(command: string): boolean {
  const leading = command.trim().split(/[\s;&|]+/).filter(Boolean)[0];
  if (!leading) return true;
  return !DISALLOWED_AUTO_BACKGROUND_COMMANDS.has(normalizeBaseCommand(leading));
}

export const SENSITIVE_ENV_SET = new Set([
  'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN',
  'AWS_SECURITY_TOKEN',
  'GITHUB_TOKEN', 'GH_TOKEN', 'GH_ENTERPRISE_TOKEN',
  'GITHUB_ENTERPRISE_TOKEN', 'GITHUB_APP_PRIVATE_KEY',
  'ANTHROPIC_API_KEY', 'ANTHROPIC_AUTH_TOKEN', 'OPENAI_API_KEY',
  'GOOGLE_APPLICATION_CREDENTIALS', 'GOOGLE_API_KEY',
  'AZURE_CLIENT_SECRET', 'AZURE_TENANT_ID',
  'DATABASE_URL', 'REDIS_URL',
  'NPM_TOKEN', 'NODE_AUTH_TOKEN',
  'YARN_NPM_AUTH_TOKEN', 'YARN_NPM_AUTH_IDENT',
  'COREPACK_NPM_TOKEN', 'COREPACK_NPM_PASSWORD', 'COREPACK_NPM_USERNAME',
  'BUN_CONFIG_TOKEN',
  'PYPI_TOKEN',
  'SSH_AUTH_SOCK', 'GIT_ASKPASS',
  // --- Credential-bearing vars whose "secret" word is NOT the last name
  //     segment, so the suffix patterns below cannot catch them. These mirror
  //     the GitHub-Actions subprocess scrub list: a prompt-injection attack in
  //     a bash command (e.g. `echo $ALL_INPUTS`) could otherwise exfiltrate
  //     these tokens. The parent AMI process keeps them (lazy SDK reads); only
  //     spawned child processes are scrubbed.
  'CLAUDE_CODE_OAUTH_TOKEN', 'ANTHROPIC_FOUNDRY_API_KEY',
  'ANTHROPIC_CUSTOM_HEADERS',
  // OTLP exporter headers — documented to carry Authorization=Bearer tokens for
  // monitoring backends; read in-process by the OTEL SDK, never needed by children.
  'OTEL_EXPORTER_OTLP_HEADERS', 'OTEL_EXPORTER_OTLP_LOGS_HEADERS',
  'OTEL_EXPORTER_OTLP_METRICS_HEADERS', 'OTEL_EXPORTER_OTLP_TRACES_HEADERS',
  // Cloud bearer creds whose token word is mid-name.
  'AWS_BEARER_TOKEN_BEDROCK', 'AZURE_CLIENT_CERTIFICATE_PATH',
  // GitHub Actions OIDC — minting an App installation token → repo takeover.
  'ACTIONS_ID_TOKEN_REQUEST_URL', 'ACTIONS_ID_TOKEN_REQUEST_TOKEN',
  // GitHub Actions artifact/cache API — cache poisoning → supply-chain pivot.
  'ACTIONS_RUNTIME_URL', 'ACTIONS_RUNTIME_TOKEN',
  // action-specific duplicates — ALL_INPUTS contains anthropic_api_key as JSON.
  'ALL_INPUTS', 'OVERRIDE_GITHUB_TOKEN', 'DEFAULT_WORKFLOW_TOKEN', 'SSH_SIGNING_KEY',
]);

const SENSITIVE_SUFFIX_PATTERNS = [
  /_KEY$/i, /_SECRET$/i, /_TOKEN$/i, /_PASSWORD$/i,
  /_CREDENTIALS$/i, /_PRIVATE_KEY$/i, /_API_KEY$/i,
  // Any *_HEADERS var — header values commonly carry Authorization=Bearer tokens.
  /_HEADERS$/i,
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

// NOTE: distinct from the exported env-access-guard `isSensitiveEnvVar` (which is a coarse
// substring gate for whether a plugin may READ an env value). THIS policy decides what to
// STRIP from a spawned subprocess's env (exact set + safe-var exclusions + suffix patterns)
// — a different threat model, hence a different name (audit #21: not a duplicate, renamed to
// end the same-name collision).
function isSubprocessSensitiveEnvVar(key: string): boolean {
  // GitHub Actions auto-creates INPUT_<NAME> duplicates of `with:` inputs
  // (e.g. INPUT_ANTHROPIC_API_KEY, INPUT_ALL_INPUTS), duplicating the same
  // secrets under a prefixed name — scrub those exactly as their base var.
  const input = /^INPUT_(.+)$/i.exec(key);
  if (input && isSubprocessSensitiveEnvVar(input[1])) return true;
  if (SENSITIVE_ENV_SET.has(key)) return true;
  if (isSafeEnvVar(key)) return false;
  return SENSITIVE_SUFFIX_PATTERNS.some(p => p.test(key));
}

export function sanitizeEnvForSubprocess(env: Record<string, string | undefined>): Record<string, string | undefined> {
  const out: Record<string, string | undefined> = {};
  for (const key of Object.keys(env)) {
    if (!isSubprocessSensitiveEnvVar(key)) {
      out[key] = env[key];
    }
  }
  return out;
}

/** True if `name` resolves to an executable file on any dir of `pathValue`. */
function isExecutableOnPath(name: string, pathValue: string | undefined): boolean {
  if (!pathValue) return false;
  for (const dir of pathValue.split(path.delimiter)) {
    if (!dir) continue;
    try {
      const p = path.join(dir, name);
      if (fs.statSync(p).isFile()) { fs.accessSync(p, fs.constants.X_OK); return true; }
    } catch { /* not in this dir */ }
  }
  return false;
}

// Cache the resolved shim dir for the process (undefined = not yet computed, null = not applicable).
let cachedPythonShimDir: string | null | undefined;
/** Test seam: forget the memoized python-shim decision. */
export function _resetPythonShimCache(): void { cachedPythonShimDir = undefined; }

/**
 * Many modern distros ship only `python3` (no `python` symlink), so an agent that runs
 * `python foo.py` hits `bash: python: command not found` and the task fails unverified (ui#25).
 * When — and only when — `python` is absent but `python3` is present, materialize a tiny
 * `python` → `python3` shim in a private 0700 temp dir (random name, owned by us, so it is NOT
 * an attacker-plantable PATH-injection vector) and return its path to prepend to PATH. Returns
 * null when a real `python` exists, when there is no `python3` to delegate to, on Windows, or on
 * any fs error. Memoized per process. `pathValue`/`make` are injectable for tests.
 */
export function ensurePythonShimDir(pathValue: string | undefined = process.env.PATH, make = true): string | null {
  if (cachedPythonShimDir !== undefined) return cachedPythonShimDir;
  if (process.platform === 'win32' ||
      isExecutableOnPath('python', pathValue) ||
      !isExecutableOnPath('python3', pathValue)) {
    cachedPythonShimDir = null;
    return null;
  }
  try {
    const dir = make ? fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pyshim-')) : path.join(os.tmpdir(), 'ami-pyshim');
    if (make) {
      const shim = path.join(dir, 'python');
      fs.writeFileSync(shim, '#!/bin/sh\nexec python3 "$@"\n', { mode: 0o755 });
      fs.chmodSync(shim, 0o755); // writeFileSync mode is subject to umask; force exec bits
    }
    cachedPythonShimDir = dir;
  } catch {
    cachedPythonShimDir = null;
  }
  return cachedPythonShimDir;
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
 * override always wins. Parity: Claude Code's upstreamproxy `getUpstreamProxyEnv`
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

function scrubEnv(extra?: Record<string, string>): NodeJS.ProcessEnv {
  const env = sanitizeEnvForSubprocess({ ...process.env }) as NodeJS.ProcessEnv;
  applyCaBundleFanout(env);
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
  delete env.FORCE_COLOR;
  env.NO_COLOR = '1';
  // python → python3 shim: prepend a private shim dir so `python foo.py` works on python3-only
  // hosts instead of failing with "command not found" (issue #25). No-op when a real `python`
  // already exists (shim dir is null).
  const shimDir = ensurePythonShimDir(env.PATH);
  if (shimDir) env.PATH = env.PATH ? shimDir + path.delimiter + env.PATH : shimDir;
  // Active-skill env (e.g. CLAUDE_SKILL_DIR / CLAUDE_PLUGIN_ROOT) so imported Claude-Code skills
  // that shell out via $CLAUDE_SKILL_DIR work (issue #22). Applied last; never stripped.
  if (extra) for (const [k, v] of Object.entries(extra)) env[k] = v;
  return env;
}

// Base description shared by every mode, up to (but not including) the git
// guidance. The git sentence and the background sentence are appended
// conditionally (see buildBashDescription) so an operator can suppress either.
const BASH_DESCRIPTION_BASE =
  'Execute a bash command. Use for running scripts, installing packages, compiling, git operations, and any shell task. '
  + 'IMPORTANT — prefer the dedicated tools over shell equivalents: read files with file_read (NOT cat/head/tail), '
  + 'edit files with file_edit (NOT sed/awk), search with grep/glob (NOT grep/rg/find), and write files with file_write '
  + '(NOT echo >/cat <<EOF). Communicate by writing text directly (NOT echo/printf to the shell). '
  + 'Use absolute paths. Quote paths with spaces. '
  // [functional parity] Affordance hint (Claude prompt.ts:L332): verify the
  // parent directory before creating files/dirs so a typo'd path fails loudly
  // instead of silently writing to the wrong place.
  + 'If a command will create new files or directories, first run `ls` to verify the parent directory exists and is the '
  + 'correct location. '
  + 'The working directory persists between commands, but shell state does NOT: each call runs in a fresh non-interactive '
  + 'shell that is not initialized from your profile (~/.bashrc/.zshrc), so a prior `cd`, exported variables, and your '
  + 'user-defined aliases/functions are unavailable in the next call — use absolute paths and chain dependent commands with &&. '
  // [functional parity] Working-directory stability guidance (Claude prompt.ts:L334):
  // keep the cwd stable across the session — prefer absolute paths and avoid `cd`,
  // using it only when the user explicitly asks.
  + 'Keep your working directory stable: prefer absolute paths and avoid `cd` — only `cd` when the user explicitly asks you to. '
  // [functional parity] Multiple-command guidance (Claude prompt.ts:L297-L302):
  // separate commands with `&&`/`;`, never newlines (newlines are fine only
  // inside quoted strings); run INDEPENDENT commands as parallel tool calls
  // rather than chaining them.
  + 'When issuing multiple commands, separate them with `&&` (stop on failure) or `;` (run regardless); do NOT use newlines '
  + 'to separate commands (newlines are OK inside quoted strings). '
  + 'If several commands are independent, prefer issuing them as parallel bash tool calls in a single message (e.g. run '
  + '`git status` and `git diff` as two parallel calls) instead of chaining with `&&`; chain only commands that genuinely '
  + 'depend on each other. '
  + 'Do not run interactive/blocking commands that wait for input (e.g. `git rebase -i`, `git add -i`, or anything that opens '
  + 'an editor or prompts for a password); they will hang. ';

// Git guidance, suppressed when git instructions are disabled (parity: Claude's
// prompt.ts shouldIncludeGitInstructions()/getCommitAndPRInstructions() gate,
// which drops the git/commit-PR section for deployments that don't want it).
const BASH_DESCRIPTION_GIT =
  'Git: `git commit` via bash is blocked — use the git_commit tool to commit. Never amend/--no-verify/force-push, and never '
  + 'commit unless explicitly asked. ';

// Closing sleep guidance, always present (it is not git- or background-specific).
// [functional parity] Anti-retry-loop / anti-poll guidance (Claude prompt.ts:L310-L328):
// never retry a failing command inside a sleep loop — diagnose the root cause
// instead of polling. (The companion "you'll be notified, don't poll a background
// task" line lives in BASH_DESCRIPTION_BACKGROUND so it is dropped with the
// run_in_background capability when background tasks are disabled.)
const BASH_DESCRIPTION_TAIL =
  'Do not sleep between commands. Do not retry a failing command in a sleep loop — diagnose the root cause instead of polling.';

// Background-task guidance, suppressed when background tasks are disabled so the
// model is never told about a capability that is turned off for this deployment.
const BASH_DESCRIPTION_BACKGROUND =
  ' For intentionally long tasks pass run_in_background: true; a command that overruns its timeout is automatically moved to '
  + 'the background so the session stays responsive. You will be notified when a background command finishes — do not poll it.';

/**
 * [functional parity] Output-truncation threshold surfaced to the model (Claude
 * PowerShellTool/prompt.ts:L121-L125 "If the output exceeds N characters, output
 * will be truncated"). AMI truncates bash output head+tail at the effective cap
 * ({@link resolveBashMaxOutput}, honoring BASH_MAX_OUTPUT_LENGTH), but never told
 * the model, so it could not plan around the limit. Computed at advertise-time so
 * the sentence always reflects the live knob.
 */
function describeOutputTruncation(): string {
  return ` Output longer than ${resolveBashMaxOutput()} characters (BASH_MAX_OUTPUT_LENGTH) is truncated (head + tail) `
    + 'before being returned to you; redirect very large output to a file and inspect it with file_read.';
}

/**
 * Model-facing bash description. When background tasks are disabled the
 * run_in_background guidance is dropped (parity: Claude's prompt.ts suppresses the
 * background usage note behind CLAUDE_CODE_DISABLE_BACKGROUND_TASKS). When git
 * instructions are disabled the git/commit sentence is dropped (parity: Claude's
 * shouldIncludeGitInstructions()/getCommitAndPRInstructions() gate).
 */
export function buildBashDescription(backgroundDisabled: boolean, gitDisabled = false): string {
  const head = BASH_DESCRIPTION_BASE + (gitDisabled ? '' : BASH_DESCRIPTION_GIT) + BASH_DESCRIPTION_TAIL;
  const withBg = backgroundDisabled ? head : head + BASH_DESCRIPTION_BACKGROUND;
  // Always surface the output-truncation threshold so the model can plan around it.
  return withBg + describeOutputTruncation();
}

/**
 * Model-facing bash input schema. When background tasks are disabled the
 * `run_in_background` property is omitted entirely from the advertised schema
 * (parity: Claude's BashTool lazySchema omit behind CLAUDE_CODE_DISABLE_BACKGROUND_TASKS)
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
          // [functional parity] dangerouslyDisableSandbox guardrails (Claude
          // prompt.ts:L228-L257): prefer the sandbox-writable $TMPDIR for temp
          // files, and never weaken the sandbox to reach credential paths.
          'Prefer $TMPDIR (sandbox-writable) over /tmp for temporary files, and never weaken the sandbox to reach ' +
          'credential paths such as ~/.bashrc, ~/.zshrc, ~/.ssh/*, or other secret files. ' +
          // [functional parity] Unsandboxed-override policy gate wording (Claude
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
  }
  // When policy forbids the escape hatch, omit it from the advertised schema so
  // the model cannot even request it (parity: Claude's prompt states the
  // parameter is 'disabled by policy' and drops it). The runtime also ignores a
  // smuggled flag — see the execute() sandbox branch.
  if (sandboxEscapeDisabled) {
    delete schema.properties.dangerouslyDisableSandbox;
  }
  return schema;
}

export const bashTool: ToolDefinition = {
  name: 'bash',
  // Getters so the env knob is read at advertise-time (every access), keeping the
  // schema/description in sync with AMI_DISABLE_BACKGROUND_TASKS without a restart.
  get description(): string {
    return buildBashDescription(isBackgroundTasksDisabled(), isGitInstructionsDisabled());
  },
  get inputSchema(): ToolInputSchema {
    return buildBashInputSchema(isBackgroundTasksDisabled(), isSandboxEscapeDisabled());
  },
  isReadOnly: false,
  isConcurrencySafe: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    let command = input.command as string;
    // Accept `command_description` as alias for `description` — models sometimes use the wrong name
    if (input.command_description && !input.description) {
      input.description = input.command_description;
    }
    const timeout = resolveBashTimeout(input.timeout);

    if (!command || command.trim().length === 0) {
      return { output: 'Error: command must not be empty.', isError: true };
    }

    // Join backslash-newline continuations before all checks so split
    // commands like r\<NL>m can't evade detection.
    command = joinLineContinuations(command);

    // Pre-execution normalization (runs BEFORE every security check so the checks
    // see exactly what will execute): drop a redundant `cd <cwd> && ` prefix,
    // collapse the double-escaped `find -exec \;` terminator, and rewrite
    // Windows `>nul` redirects to `/dev/null`.
    command = normalizeBashInput(command, context.cwd);

    // Nuclear command blocklist — un-bypassable, runs before ALL other checks.
    // No flag, mode, or env var can disable this. Even --yolo cannot override it.
    const nuclear = detectNuclearCommand(command);
    if (nuclear.blocked) {
      const lines = [
        `Error: BLOCKED — ${nuclear.reason}.`,
        `Command: ${command.length > 200 ? command.slice(0, 200) + '...' : command}`,
        '',
        'This command is on the nuclear blocklist and is NEVER allowed, regardless of',
        'permission mode (including --yolo). These are commands that can cause',
        'unrecoverable system-wide damage (filesystem destruction, disk wipe, fork bomb).',
        '',
      ];
      if (nuclear.suggestion) {
        lines.push(`Suggestion: ${nuclear.suggestion}`);
      } else {
        lines.push('If you need to delete files, target a specific directory instead of root paths.');
      }
      return { output: lines.join('\n'), isError: true };
    }

    // TOCTOU-safe working-directory resolution with delete-recovery: realpath the
    // cwd (no separate existsSync pre-check) and, if it was deleted out from under
    // the session, recover to the launch directory rather than failing closed.
    const resolvedCwd = resolveWorkingDirectory(context.cwd);
    if (!resolvedCwd) {
      return {
        output: `Error: working directory "${context.cwd}" does not exist. It may have been deleted or moved. Use /add-dir to set a new working directory.`,
        isError: true,
        // Discriminate a pre-spawn setup failure (the shell never ran) from a
        // command that ran and exited non-zero/timed out, so callers can treat it
        // as a tooling fault rather than a command result. Parity: Claude's
        // dedicated preSpawnError result field (ShellCommand.createFailedCommand).
        metadata: { preSpawnError: 'cwd-missing' },
      };
    }
    let cwdResetNote: string | null = null;
    if (resolvedCwd.dir !== context.cwd) {
      if (resolvedCwd.recovered) {
        cwdResetNote =
          `Shell cwd was reset to ${resolvedCwd.dir} — the previous working directory ` +
          `"${context.cwd}" no longer exists (it may have been deleted or moved).`;
      }
      // Persist the resolved/recovered directory for the rest of this call so
      // path resolution, sandbox config, and the spawn all use it consistently.
      context.cwd = resolvedCwd.dir;
    }

    const commitKind = detectGitCommitKind(command);
    if (commitKind) {
      // Parity: distinguish an amend (history rewrite) from a plain commit so the
      // guidance can call it out specifically (Claude gitOperationTracking CommitKind).
      const amendNote =
        commitKind === 'amended'
          ? '\n\nNote: `git commit --amend` rewrites the last commit; amending via bash is especially not allowed. ' +
            'Make a fresh commit with the git_commit tool instead, and never amend unless the user explicitly asks.'
          : '';
      return {
        output:
          'Error: Use the git_commit tool instead of running git commit via bash. ' +
          'The git_commit tool ensures proper Co-Authored-By attribution is always included.\n\n' +
          'Example: git_commit({ message: "your commit message", files: ["file1.ts", "file2.ts"] })' +
          amendNote,
        isError: true,
      };
    }

    if (context.detachedMode) {
      const discard = detectDetachedGitDiscard(command, context.cwd);
      if (discard) {
        return { output: `Error: ${discard}`, isError: true };
      }
    }

    // Bare-repository attack guard: a git invocation in a cwd that carries
    // bare-repo indicators (HEAD/objects/refs without a valid .git/HEAD) would
    // let git execute attacker-planted hooks from cwd. Block it before running.
    if (commandInvokesGit(command) && isCurrentDirectoryBareGitRepo(context.cwd)) {
      return {
        output:
          'Error: refusing to run git — the working directory has bare-repository indicators ' +
          '(HEAD, objects/, or refs/ present without a valid .git/HEAD). Git would treat this ' +
          'directory as a bare repo and may execute hook scripts planted here. Remove the stray ' +
          'bare-repo files or run git from a legitimate repository.',
        isError: true,
      };
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

    const argv0Check = detectBashArgv0Assignment(command);
    if (argv0Check.blocked) {
      return { output: `Error: ${argv0Check.reason}`, isError: true };
    }

    const bashpidCheck = detectBashpidAssignment(command);
    if (bashpidCheck.blocked) {
      return { output: `Error: ${bashpidCheck.reason}`, isError: true };
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

    const exfil = _bashHooks.detectDataExfiltration(command);
    if (exfil.blocked) {
      return { output: `Error: blocked — ${exfil.reason} (data exfiltration risk)`, isError: true };
    }

    // coerceBool: providers serialize booleans as strings; a naive truthy check
    // would background on the STRING "false". Shared with task.ts.
    // When background tasks are disabled (AMI_DISABLE_BACKGROUND_TASKS), force the
    // command to run in the foreground even if the model still requested a
    // background run — the capability is fully off (parity: Claude runs such
    // commands in the foreground when CLAUDE_CODE_DISABLE_BACKGROUND_TASKS is set).
    const runInBackground = coerceBool(input.run_in_background) && !isBackgroundTasksDisabled();
    const sleepBlock = detectBlockedSleep(command, runInBackground, !!input.timeout);
    if (sleepBlock) return { output: `Error: ${sleepBlock}`, isError: true };

    // Non-blocking advisories derived from the original command string. Computed
    // before injectFindMaxdepth rewrites `command`, appended to the final output
    // so the model sees them (force push → remote-history warning; cd+git → bare
    // repository attack warning). Protected-branch force pushes are already
    // hard-blocked above, so detectForcePushAdvisory only fires for the rest.
    const forcePushAdvisory = detectForcePushAdvisory(command);
    const compoundCdGitAdvisory = detectCompoundCdGit(command);

    command = injectFindMaxdepth(command);

    const readOnly = isReadOnlyBashCommand(command);

    const effectiveCwd = context.cwd;
    if (isCacheableReadOnly(command)) {
      const cached = _bashCache.get(command, effectiveCwd);
      if (cached !== null) return { output: cached, metadata: { readOnly: true, cached: true } };
    }

    // Resolve the isolation backend BEFORE any execution path (foreground or
    // background) so container mode cannot be escaped. In container mode EVERY
    // command runs inside a container whose only writable mount is the project
    // dir; if Docker is unavailable we fail closed and never touch the host.
    // dangerouslyDisableSandbox cannot override enforced container mode.
    //
    // Session environment: prepend the persisted env script (parent-supplied
    // CLAUDE_ENV_FILE/AMI_ENV_FILE + per-hook env fragments) so venv/conda
    // activation and exported vars survive across separate, stateless bash
    // invocations (Claude Code `sessionEnvironment` parity). [FUNCTIONAL CHANGE]
    // The script is folded into the EXECUTED command only; the display `command`
    // is left untouched so transcripts/output stay clean.
    const sessionEnvScript = await getSessionEnvironmentScript(context.sessionId);
    const commandToRun = sessionEnvScript ? `${sessionEnvScript}\n${command}` : command;
    let effectiveCommand: string;
    // Surfaced when the host sandbox degraded to resource-limits-only while a
    // network/filesystem carve-out was explicitly requested (see
    // describeSandboxFallbackWarning) — appended to the command output below so the
    // lost protection is not silent.
    let sandboxFallbackAdvisory: string | null = null;
    // PARITY (sandbox.enabledPlatforms): when the current platform is excluded,
    // the sandbox (container OR host) is disabled entirely and the command runs
    // on the host — undefined=all, []=none, else a membership test. Gates both
    // backends so container mode does not hard-fail on an unsupported platform.
    const platformSandboxEnabled = isSandboxPlatformEnabled(context.sandbox?.enabledPlatforms);
    if (context.sandbox?.mode === 'container' && platformSandboxEnabled) {
      // Pass the abort signal so a user Ctrl-C during a hung daemon probe is
      // honored instead of blocking for the full probe timeout.
      const availability = getDockerAvailability({ signal: context.abortSignal });
      if (!availability.available) {
        return {
          output:
            'Error: SANDBOX REQUIRED — bash is configured to run only inside a container '
            + '(sandbox mode=container) but ' + describeDockerUnavailable(availability) + '. '
            + 'Refusing to run on the host. Change the sandbox policy to run without a container.',
          isError: true,
        };
      }
      effectiveCommand = wrapWithDocker(commandToRun, context.cwd, {
        image: context.sandbox.image || DEFAULT_DOCKER_IMAGE,
        projectDir: context.sandbox.projectDir || context.cwd,
        allowNetwork: context.sandbox.allowNetwork !== false,
        // PARITY: thread the granular filesystem/network carve-outs into the
        // container backend (previously honored only by the unshare backend), so
        // deny-write (config/skill escape guard), deny-read, extra writable mounts
        // (/add-dir, worktree main .git) and the Unix-socket allowlist apply in
        // container mode too.
        denyWritePaths: context.sandbox.filesystem?.denyWrite,
        denyReadPaths: context.sandbox.filesystem?.denyRead,
        allowWritePaths: context.sandbox.filesystem?.allowWrite,
        allowUnixSockets: context.sandbox.network?.allowUnixSockets,
        // PARITY (per-host container egress): blackhole denied hosts + route through
        // the configured host egress proxy (previously honored only by the unshare
        // backend), so container-mode network control is no longer all-or-nothing.
        deniedHosts: context.sandbox.network?.deniedHosts,
        // PARITY (per-domain network allowlist): "network on, but only these
        // domains" — realized by an in-container iptables allowlist in the backend.
        allowedHosts: context.sandbox.network?.allowedHosts,
        httpProxyPort: context.sandbox.network?.httpProxyPort,
        socksProxyPort: context.sandbox.network?.socksProxyPort,
        // PARITY (custom seccomp profile): thread the operator-supplied Docker
        // seccomp JSON profile into the container backend.
        seccompProfilePath: context.sandbox.seccomp?.profilePath,
      });
    } else {
      // coerceBool: providers serialize booleans as strings; a strict `=== true`
      // would silently keep the sandbox on for `dangerouslyDisableSandbox: "true"`.
      // Use the SAME coercion as run_in_background so both boolean flags behave
      // identically for string-serialized input.
      // PARITY (allowUnsandboxedCommands): when the policy sets it to exactly
      // false, the `dangerouslyDisableSandbox` escape hatch is COMPLETELY ignored
      // — every command must run sandboxed (a managed-deployment lockout).
      // The escape hatch is locked out by EITHER the per-config policy
      // (allowUnsandboxedCommands === false) OR the global env knob
      // (AMI_DISABLE_SANDBOX_ESCAPE) — both mirror Claude's
      // areUnsandboxedCommandsAllowed() policy boundary.
      const sandboxEscapeLocked = context.sandbox?.allowUnsandboxedCommands === false || isSandboxEscapeDisabled();
      const disableSandbox = sandboxEscapeLocked ? false : coerceBool(input.dangerouslyDisableSandbox);
      // Global host-sandbox off-switch (parity: isSandboxingEnabled): the env knob
      // or `sandbox.enabled === false` disables the lightweight auto-wrap entirely,
      // so a trigger-pattern match no longer forces the unshare backend on.
      const hostSandboxDisabled = isHostSandboxDisabled() || context.sandbox?.enabled === false;
      // PARITY (default-deny coverage): the AMI_SANDBOX_DEFAULT_DENY gate flips the
      // host auto-sandbox from AMI's narrow trigger allowlist to Claude Code's
      // default-deny model (sandbox every non-exempt command). Threaded alongside
      // the operator exemption list so excludedCommands still carve out exemptions.
      const sandboxExemptionOptions: SandboxExemptionOptions = {
        ...(context.sandboxExcludedCommands ? { excludedCommands: context.sandboxExcludedCommands } : {}),
        ...(isSandboxDefaultDeny() ? { defaultSandbox: true } : {}),
      };
      const useSandbox = platformSandboxEnabled && !disableSandbox && !hostSandboxDisabled && _bashHooks.shouldUseSandbox(command, sandboxExemptionOptions);
      // PARITY (failIfUnavailable / allowUnsandboxedFallback): if the command must
      // be sandboxed but the host backend cannot actually start, a hard gate
      // (`failIfUnavailable`, or `allowUnsandboxedFallback === false`) REFUSES the
      // run instead of silently degrading to the weaker ulimits-only path.
      if (
        useSandbox
        && (context.sandbox?.failIfUnavailable === true || context.sandbox?.allowUnsandboxedFallback === false)
      ) {
        const validation = validateSandboxEnabled({ failIfUnavailable: true });
        if (!validation.valid) {
          return {
            output:
              'Error: SANDBOX REQUIRED — bash is configured to refuse unsandboxed commands '
              + '(sandbox.failIfUnavailable / allowUnsandboxedFallback=false) but the host sandbox '
              + 'cannot start: ' + (validation.error ?? 'unavailable') + '. '
              + 'Refusing to run unsandboxed.',
            isError: true,
          };
        }
      }
      // Thread the policy's network/filesystem carve-outs (deny/allow hosts, unix
      // sockets, proxy ports, deny-read/deny-write with allow-read/allow-write
      // re-opens, CC `~`/`//`/relative path normalization) into
      // the HOST unshare backend too — previously honored only by the container
      // backend (see wrapWithDocker above), so the host sandbox silently ignored
      // every configured restriction. hostSandboxConfigFromPolicy yields {} when
      // no carve-outs are set, preserving the prior defaults.
      const hostCfg = hostSandboxConfigFromPolicy(context.sandbox, context.cwd);
      effectiveCommand = useSandbox
        ? _bashHooks.wrapWithSandbox(commandToRun, context.cwd, hostCfg)
        : commandToRun;
      // When the command IS sandboxed but the host backend silently degraded to
      // the weaker ulimits-only path, surface which REQUESTED carve-outs are now
      // unenforced (null when the full namespace sandbox is in use or no carve-out
      // was requested).
      if (useSandbox) sandboxFallbackAdvisory = _bashHooks.describeSandboxFallbackWarning(hostCfg);
    }

    if (runInBackground && context.processManager) {
      const desc = (input.description as string) || extractBashCommentLabel(command) || command.slice(0, 80);
      // [functional parity] coerce a numeric-string timeout, honor the env knob,
      // and reject NaN/non-positive env values (see resolveBackgroundTimeoutMs).
      const bgTimeoutMs = resolveBackgroundTimeoutMs(input.timeout);
      const taskId = context.processManager.spawn(effectiveCommand, {
        cwd: context.cwd,
        description: desc,
        env: scrubEnv(context.skillEnv),
        timeoutMs: bgTimeoutMs,
        // Record the owning agent so the shell is reaped if that agent exits first
        // (zombie prevention — see ProcessManager.killTasksForAgent).
        agentId: context.agentId,
      });
      const task = context.processManager.get(taskId);
      return {
        output: `[background: true]\nBackground task started.\n  Task ID: ${taskId}\n  PID: ${task?.pid ?? 'unknown'}\n  Command: ${command}\n  Timeout: ${Math.round(bgTimeoutMs / 1000)}s\n\nUse task_output({ task_id: "${taskId}" }) to check status and output.\nUse task_kill({ task_id: "${taskId}" }) to stop it.\nUse task_list() to see all background tasks.`,
        isError: false,
      };
    }

    // Assistant/detached blocking budget: in detached mode an operator-set
    // BASH_ASSISTANT_BLOCKING_BUDGET_MS caps how long a foreground command may
    // block the main agent before it is auto-moved to the background (the command
    // keeps running). Only lowers the foreground ceiling; the normal per-call/
    // default timeout still applies when the knob is unset or already smaller.
    const assistantBudget = context.detachedMode ? resolveAssistantBlockingBudget() : undefined;
    const foregroundTimeout = assistantBudget !== undefined ? Math.min(timeout, assistantBudget) : timeout;

    const result = await execCommandWithForkRetry(effectiveCommand, {
      cwd: context.cwd,
      timeout: foregroundTimeout,
      abortSignal: context.abortSignal,
      env: scrubEnv(context.skillEnv),
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
      // Auto-move a long-running foreground command to the background so the
      // session stays responsive — but NOT for commands that must never be
      // auto-backgrounded (a bare `sleep` that times out is a pointless process
      // to keep alive; it falls through to a plain timeout error below). Parity:
      // Claude Code gates this behind isAutobackgroundingAllowed / shouldAutoBackground.
      if (context.processManager && isAutobackgroundingAllowed(command)) {
        const taskId = context.processManager.spawn(effectiveCommand, {
          cwd: context.cwd,
          description: `[auto-bg] ${extractBashCommentLabel(command)?.slice(0, 60) ?? command.slice(0, 60)}`,
          env: scrubEnv(context.skillEnv),
          agentId: context.agentId,
        });
        const bgTask = context.processManager.get(taskId);
        return {
          output: sanitizeToolOutput(
            formatTimeoutBackgroundOutput(presented.stdout, presented.stderr, foregroundTimeout, taskId, {
              detachedMode: context.detachedMode === true,
              outputPath: bgTask?.outputPath,
            }),
          ),
          // Surface that a long foreground command was auto-backgrounded so the
          // agent loop / introspection can see it happened. In detached/assistant
          // mode this is the "assistant blocking budget" auto-background.
          metadata: {
            backgrounded: true,
            taskId,
            ...(context.detachedMode ? { assistantAutoBackgrounded: true } : {}),
          },
        };
      }
      return {
        output: sanitizeToolOutput(formatOutput(presented.stdout, presented.stderr, null, `Command timed out after ${formatDuration(foregroundTimeout)}.`)),
        isError: true,
      };
    }

    // If a successful command printed a base64 image data URI (a screenshot,
    // chart/plot generator, or `… | base64 --to-data-uri`), surface it as a real
    // image block instead of letting spillToolText truncate the base64 into a
    // corrupt text blob. Emits the same JSON the engine already parses
    // (parseImageToolOutput), so no extra engine wiring is required.
    if (result.exitCode === 0) {
      const imageOutput = await maybeBuildShellImageOutput(presented.stdout);
      if (imageOutput !== null) {
        // NOT run through sanitizeToolOutput: the output is machine-parsed JSON
        // whose base64 payload must stay byte-exact (credential/ANSI redaction
        // would corrupt it). The payload is restricted to base64 chars by the
        // data-URI matcher, so it carries no secrets or escape sequences.
        return { output: imageOutput, isError: false, metadata: { readOnly } };
      }
    }

    // Already folded. Spill only non-suite output that is still over the cap.
    const bashMaxChars = context.outputBudget?.bashMaxChars ?? resolveBashMaxOutput();
    let stdout = spillToolText(context.cwd, 'bash', presented.stdout, bashMaxChars);
    // In container mode, explain a failure that was likely caused by sandbox
    // isolation (network off, or a host path not mounted inside the container) so
    // the model does not misdiagnose a blocked op as a real error.
    const stderr =
      context.sandbox?.mode === 'container'
        ? annotateStderrWithSandboxFailures(command, presented.stderr, {
            allowNetwork: context.sandbox.allowNetwork !== false,
            projectDir: context.sandbox.projectDir || context.cwd,
            ignoreViolations: context.sandbox.ignoreViolations,
          })
        : presented.stderr;

    // Compound/pipeline-aware silent-command detection (parity): every segment
    // must be a known no-output command, so a noisy later segment (`cp a b && ls`)
    // no longer gets a misleading affordance, while a broader silent set
    // (rmdir/chgrp/ln/…) now does.
    const noOutputExpected = isSilentBashCommand(command);
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
    if (forcePushAdvisory) output += `\n\n[Warning: ${forcePushAdvisory}]`;
    if (compoundCdGitAdvisory) output += `\n\n[Warning: ${compoundCdGitAdvisory}]`;
    if (cwdResetNote) output += `\n\n[Warning: ${cwdResetNote}]`;
    if (sandboxFallbackAdvisory) output += `\n\n[Warning: ${sandboxFallbackAdvisory}]`;

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
    output = sanitizeToolOutput(`${output}${exitInfo}`.trim());

    if (!isRealError && isCacheableReadOnly(command)) {
      _bashCache.set(command, effectiveCwd, output);
    } else if (!readOnly && !isRealError) {
      _bashCache.invalidateForCwd(effectiveCwd);
    }

    if (!isRealError && context.filesRead) {
      for (const fp of extractBashReadPaths(command, effectiveCwd)) {
        context.filesRead.add(fp);
      }
    }

    return {
      output,
      isError: isRealError,
      // A SPAWN_FAILURE_EXIT_CODE (126) from the shell layer means the child
      // process could not be started (pre-exec spawn 'error' event), not that a
      // command ran and failed — tag it so callers can distinguish a tooling
      // fault from a genuine non-zero exit. Parity: Claude's preSpawnError field.
      metadata: result.exitCode === SPAWN_FAILURE_EXIT_CODE
        ? { readOnly, preSpawnError: 'spawn-failure' }
        : { readOnly },
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
  // Strip only whole leading/trailing empty lines (a trailing newline from most
  // commands, or a leading blank from a banner) while preserving interior blank
  // lines and indentation, so the model sees clean output without losing layout.
  return {
    stdout: stripEmptyLines(foldOut ? compactTestLog(stdout) : stdout),
    stderr: stripEmptyLines(foldErr ? compactTestLog(stderr) : stderr),
  };
}

/** A bare base64 image data URI occupying the whole of a command's stdout. */
const SHELL_IMAGE_DATA_URI_RE = /^data:(image\/[a-z0-9.+_-]+);base64,([A-Za-z0-9+/=\s]+)$/i;

/** Raw-byte ceiling for a shell-produced image before we refuse to decode it
 *  (parity: Claude Code's MAX_IMAGE_FILE_SIZE = 20MB). Estimated from the base64
 *  length BEFORE allocating the buffer, so a pathological data URI can't OOM us. */
const MAX_SHELL_IMAGE_RAW_BYTES = 20 * 1024 * 1024;

/**
 * When a command's stdout is a single base64 `data:image/...;base64,...` URI
 * (a screenshot / plot / chart-generator / `… | base64 --to-data-uri`), build a
 * real image tool_result instead of a text blob that spillToolText would truncate
 * into a corrupt base64 string. Returns the `{type:'image',source:{type:'base64',…}}`
 * JSON the engine already recognizes (parseImageToolOutput) — so surfacing a
 * shell-produced image needs no new engine wiring. Reuses file_read's
 * compressImageBuffer for the shared size/dimension caps (high-DPI downsample +
 * 5MB API ceiling), matching how file reads encode images. Returns null when
 * stdout is not a bare image data URI (ordinary text output is unchanged).
 * Parity: Claude Code BashTool utils isImageOutput / buildImageToolResult /
 * resizeShellImageOutput.
 */
export async function maybeBuildShellImageOutput(stdout: string): Promise<string | null> {
  const m = stdout.trim().match(SHELL_IMAGE_DATA_URI_RE);
  if (!m) return null;
  const declaredType = m[1].toLowerCase();
  const b64 = m[2].replace(/\s+/g, '');
  if (b64.length === 0) return null;
  // Estimate raw size (base64 inflates by 4/3) before decoding to fail fast on an
  // oversized URI without materializing the buffer.
  if (Math.floor((b64.length * 3) / 4) > MAX_SHELL_IMAGE_RAW_BYTES) {
    return `[Image output omitted: exceeds the ${MAX_SHELL_IMAGE_RAW_BYTES / (1024 * 1024)}MB limit for shell-produced images. Write it to a file and open it with file_read instead.]`;
  }
  const buffer = Buffer.from(b64, 'base64');
  if (buffer.length === 0) return null;
  const sniffed = detectImageFormatFromBase64(b64);
  try {
    const compressed = await compressImageBuffer(buffer, IMAGE_TARGET_RAW_SIZE, sniffed || declaredType);
    const block: ImageContent & { metadata?: string } = {
      type: 'image',
      source: { type: 'base64', media_type: compressed.mediaType, data: compressed.base64 },
    };
    const meta = compressed.dimensions ? createImageMetadataText(compressed.dimensions, 'bash stdout') : null;
    if (meta) block.metadata = meta;
    return JSON.stringify(block);
  } catch (err) {
    if (err instanceof ImageResizeError) {
      return `[Image output could not be prepared: ${err.message}]`;
    }
    return null;
  }
}

const ERROR_HINTS: Array<{ pattern: RegExp; hint: string }> = [
  { pattern: /ENOSPC|No space left on device/i, hint: 'Disk is full. Free up space in the working directory or /tmp. Check usage with `df -h` and find large files with `du -sh * | sort -rh | head`.' },
  { pattern: /Transform failed|SyntaxError:.*Unexpected/i, hint: 'Your code has a syntax error (missing bracket, semicolon, or mismatched quotes). Use file_read to check the file you last edited.' },
  { pattern: /Cannot find module|ERR_MODULE_NOT_FOUND/i, hint: 'Module not found. Check the import path with list_dir or glob, and try running via npm test instead of raw node commands.' },
  { pattern: /ENOENT.*no such file/i, hint: 'File or directory does not exist. Use list_dir to check what files are available.' },
  { pattern: /TypeError:.*is not a function/i, hint: 'A function call is wrong. Use file_read to check the export names and signatures in the imported module.' },
  { pattern: /ReferenceError:.*is not defined/i, hint: 'A variable or import is missing. Check your imports and variable declarations with file_read.' },
  { pattern: /Operation not permitted|\bEPERM\b/i, hint: 'This can be a sandbox restriction — the command ran in an isolated sandbox that blocks writes outside the project, network access, or certain syscalls. If the operation is legitimate, retry the SAME command with dangerouslyDisableSandbox: true.' },
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
  { pattern: /API rate limit exceeded|gh: api rate limit|403.*rate limit/i, hint: 'GitHub API rate limit exceeded. Wait a few minutes before retrying, or authenticate with `gh auth login` for higher limits.' },
  { pattern: /secondary rate limit|abuse detection/i, hint: 'GitHub secondary rate limit triggered. Slow down requests — wait at least 60 seconds before retrying.' },
  { pattern: /missing required scopes|not been granted the required scopes|Resource not accessible by (?:integration|personal access token)|HTTP 403:.*(?:scope|admin)/i, hint: 'GitHub CLI token is missing required scopes. Run `gh auth refresh -h github.com -s repo,workflow` to grant them, or `gh auth login` to re-authenticate.' },
];

export function diagnoseError(output: string): string | null {
  for (const { pattern, hint } of ERROR_HINTS) {
    if (pattern.test(output)) return hint;
  }
  return null;
}

interface ExitCodeInfo {
  message: string;
  isNonError?: boolean;
}

/**
 * Reduce a raw command token to its base command name for exit-code semantics:
 * strip a PowerShell call operator (`&`/`.`) prefix, surrounding quotes, the
 * directory portion of a path (`/usr/bin/grep` → `grep`, Windows `\` too), and a
 * trailing `.exe` (case-insensitive). Without this, a path-qualified or quoted
 * command (`/usr/bin/grep`, `"grep"`, `grep.exe`) would miss its exit-code rule.
 */
export function normalizeBaseCommand(raw: string): string {
  let b = (raw || '').trim();
  b = b.replace(/^[&.]+\s*/, ''); // PowerShell call/dot-source operator
  if (b.length >= 2 && ((b[0] === '"' && b[b.length - 1] === '"') || (b[0] === "'" && b[b.length - 1] === "'"))) {
    b = b.slice(1, -1);
  }
  const slash = Math.max(b.lastIndexOf('/'), b.lastIndexOf('\\'));
  if (slash >= 0) b = b.slice(slash + 1);
  b = b.replace(/\.exe$/i, '');
  return b;
}

export function interpretExitCode(command: string, exitCode: number): ExitCodeInfo | null {
  const parts = command.trim().split(/[|;&]\s*/);
  const lastCmd = parts[parts.length - 1].trim().split(/\s+/)[0];
  const base = normalizeBaseCommand(lastCmd || command.trim().split(/\s+/)[0]);
  // robocopy uses a bitfield exit code: bits 0-2 (values 1/2/4) are informational
  // success states, bit 3 (8) = mismatched/failed files, bit 4 (16) = fatal. So an
  // exit code < 8 is success, not an error.
  if (base.toLowerCase() === 'robocopy' && exitCode >= 0 && exitCode < 8) {
    const notes: string[] = [];
    if (exitCode & 1) notes.push('files copied');
    if (exitCode & 2) notes.push('extra files/dirs present');
    if (exitCode & 4) notes.push('mismatched files/dirs present');
    const detail = notes.length ? notes.join(', ') : 'no files needed copying';
    return { message: `(robocopy success — exit ${exitCode}: ${detail})`, isNonError: true };
  }
  // grep family: grep/egrep/fgrep/rg (POSIX + ripgrep) and findstr (Windows
  // native text search) all exit 1 on no-match and >=2 on a real error.
  const isGrepFamily = base === 'grep' || base === 'egrep' || base === 'fgrep' || base === 'rg' || base.toLowerCase() === 'findstr';
  if (exitCode === 1 && isGrepFamily) return { message: '(no matches found — not an error)', isNonError: true };
  if (exitCode === 1 && base === 'diff') return { message: '(files differ — not an error)', isNonError: true };
  if (exitCode === 1 && base === 'git') {
    const args = command.trim().split(/\s+/).slice(1);
    if (args[0] === 'grep' || args[0] === 'diff') return { message: '(no matches/differences found — not an error)', isNonError: true };
  }
  if (exitCode === 1 && (base === 'test' || base === '[')) return { message: '(condition is false)', isNonError: true };
  if (exitCode === 1 && base === 'find') return { message: '(some paths were inaccessible)', isNonError: true };
  if (exitCode === 2 && isGrepFamily) return { message: '(error in grep pattern or file access)' };
  if (exitCode === 126) return { message: '(permission denied — command not executable)' };
  if (exitCode === 127) return { message: '(command not found)' };
  if (exitCode === 128 + 9) return { message: '(killed by SIGKILL)' };
  if (exitCode === 128 + 15) return { message: '(killed by SIGTERM)' };
  return null;
}

/**
 * Render a millisecond duration as a compact human-readable string for
 * model/user-facing messages (timeouts). Parity: Claude Code
 * ShellCommand `Command timed out after ${formatDuration(timeout)}` — e.g.
 * 120000 → "2m", 90000 → "1m 30s", 3661000 → "1h 1m 1s". As a superset over
 * Claude's whole-second rendering, a sub-second value keeps ms precision
 * ("400ms") rather than collapsing to "0s". Non-finite/negative input falls back
 * to the raw `${ms}ms` form so the message is never silently wrong.
 */
export function formatDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms < 1000) return `${ms}ms`;
  const totalSeconds = Math.floor(ms / 1000);
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const parts: string[] = [];
  if (days) parts.push(`${days}d`);
  if (hours) parts.push(`${hours}h`);
  if (minutes) parts.push(`${minutes}m`);
  if (seconds || parts.length === 0) parts.push(`${seconds}s`);
  return parts.join(' ');
}

export function truncateBashText(text: string, maxLength: number): string {
  if (text.length <= maxLength) {
    return text;
  }
  let half = Math.floor((maxLength - 100) / 2);

  // Avoid splitting a UTF-16 surrogate pair at the head boundary.
  if (half > 0 && half < text.length) {
    const code = text.charCodeAt(half - 1);
    if (code >= 0xD800 && code <= 0xDBFF) half--; // back off lone high surrogate
  }

  // Avoid splitting a UTF-16 surrogate pair at the tail boundary.
  let tailStart = text.length - half;
  if (tailStart > 0 && tailStart < text.length) {
    const code = text.charCodeAt(tailStart);
    if (code >= 0xDC00 && code <= 0xDFFF) tailStart++; // skip lone low surrogate
  }

  return (
    text.slice(0, half) +
    `\n\n... [truncated ${text.length - maxLength + 100} characters] ...\n\n` +
    text.slice(tailStart)
  );
}

function formatOutput(
  stdout: string,
  stderr: string,
  exitCode: number | null,
  extra?: string,
): string {
  const parts: string[] = [];
  // Honor BASH_MAX_OUTPUT_LENGTH (parity knob); defaults to MAX_OUTPUT_LENGTH.
  const maxOutput = resolveBashMaxOutput();

  if (stdout.length > 0) {
    parts.push(truncateBashText(stdout, maxOutput));
  }

  if (stderr.length > 0) {
    const label = stdout.length > 0 ? '\n[stderr]\n' : '';
    const remaining = Math.max(
      1000,
      maxOutput - (parts.join('').length),
    );
    parts.push(label + truncateBashText(stderr, remaining));
  }

  if (extra) {
    parts.push(extra);
  }

  if (exitCode !== null && exitCode !== undefined) {
    parts.push(`Exit code: ${exitCode}`);
  }

  return parts.join('\n').trim() || '(no output)';
}

/**
 * Build the result shown when a command hits its timeout and is auto-moved to
 * the background. The partial stdout/stderr captured before the timeout is
 * surfaced under an "[output so far]" header so the agent loop can act on the
 * in-progress results instead of losing them; when nothing was captured, only
 * the still-running notice is returned.
 */
export function formatTimeoutBackgroundOutput(
  stdout: string,
  stderr: string,
  timeout: number,
  taskId: string,
  opts?: { detachedMode?: boolean; outputPath?: string },
): string {
  let notice =
    `Command timed out after ${formatDuration(timeout)}. Auto-moved to background as task ${taskId}. ` +
    'The command is still running in the background — use task_output to check progress.';
  // Surface where the full output is being written so the model can tail/re-read
  // it (parity: Claude's background notice includes the output file path).
  if (opts?.outputPath) {
    notice += ` Output is being written to: ${opts.outputPath}.`;
  }
  // Assistant/detached-mode coaching: steer the main agent to keep itself
  // responsive rather than blocking on the long task (parity: Claude's
  // assistant-mode auto-background wording).
  if (opts?.detachedMode) {
    notice +=
      ' In assistant mode, delegate long-running work to a subagent or use ' +
      'run_in_background to keep this conversation responsive.';
  }
  // Decide "nothing captured" from the raw streams, not by string-comparing
  // formatOutput's result to its '(no output)' sentinel — a command whose real
  // partial output is literally '(no output)' must still be surfaced.
  if (stdout.trim().length === 0 && stderr.trim().length === 0) return notice;
  return `${notice}\n\n[output so far]\n${formatOutput(stdout, stderr, null)}`;
}
