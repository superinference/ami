import * as fs from 'fs';

import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult, ImageContent, ToolInputSchema } from '../types';
import { detectCommandChaining } from '../permissions';
import { getSessionEnvironmentScript } from '../session-environment';
import { sessionEnvOverrides } from '../session-env-vars';
import { execCommand, resolveTmpBase, SPAWN_FAILURE_EXIT_CODE } from '../utils/shell';
import '../utils/windows-paths';
import { coerceBool, coerceNumber, parseOptionalBool } from '../utils/coerce';
import { validateBashSecurity, detectCredentialPathWrite, detectSessionTranscriptWrite, detectDataExfiltration, detectCodeLoadingEnvSet, detectNuclearCommand, detectBashArgv0Assignment, detectBashpidAssignment, sanitizeToolOutput, joinLineContinuations, commandInvokesGit } from './bash-security';
import { shouldUseSandbox, wrapWithSandbox, hostSandboxConfigFromPolicy, isSandboxPlatformEnabled, validateSandboxEnabled, describeSandboxFallbackWarning, sandboxBashDisplayName, commandWillBeHostSandboxed } from './bash-sandbox';
import { validateBoundedIntEnvVar } from '../config';
import {
  getDockerAvailability,
  describeDockerUnavailable,
  annotateStderrWithSandboxFailures,
  wrapWithDocker,
  DEFAULT_DOCKER_IMAGE,
} from './docker-sandbox';
import { spillToolTextWithMeta } from './spill-output';
import './test-log';
import { isReadOnlyBashCommand as isCacheableReadOnly, BashResultCache } from './bash-cache';
import { classifyBashAccess, describeToolActivity } from './tool-activity-class';
import { compressImageBuffer, detectImageFormatFromBase64, createImageMetadataText, ImageResizeError, IMAGE_TARGET_RAW_SIZE } from './file-read';
import { isBackgroundTasksDisabled, isGitInstructionsDisabled, isSandboxEscapeDisabled } from './tool-gates';
import { parsePluginHintTags, stripPluginHintTags, evaluatePluginHint, formatPluginHintNote, type PluginRecommendationHint } from '../plugins';
import { applySimulatedSedEdit } from './bash-sed-parser';
import '../utils/which';
import './bash/headless-output';
import { FORK_RETRY_BASE_MS, FORK_RETRY_MAX, MAX_OUTPUT_LENGTH, MIN_OUTPUT_LENGTH, buildSearchShadowPreamble, extractBashCommentLabel, extractBashReadPaths, injectFindMaxdepth, isBashForkExhaustion, isBashOutputFileModeEnabled, isPluginHintsEnabled, isSearchShadowEnabled, looksLikeHungPrompt, normalizeBashInput, parseEnvMs, resolveAssistantBlockingBudget, resolveBashTimeout, resolveOfficialMarketplaces, rewritePwshInvocation } from './bash-strip-redundant-cd-prefix';
import type { GitCommitKind } from './bash-strip-redundant-cd-prefix';
import { BASH_DESCRIPTION_TAIL, DETACHED_GIT_DISCARD_MSG, PROXY_FANOUT_GROUPS, SAFE_PREFIX_PATTERNS, SAFE_PREFIX_RE, SENSITIVE_SUFFIX_PATTERNS, SHELL_IMAGE_DATA_URI_RE, applyCaBundleFanout, applyProxyFanout, buildBashInputSchema, detectBlockedSleep, detectCompoundCdGit, detectForcePushAdvisory, detectForcePushToProtected, detectSelfKill, isExecutableOnPath, isSafeEnvVar, isShellMeta, presentCommandOutput, tokenizeShell } from './bash-build-bash-input-schema';
import type { GitInvocation } from './bash-build-bash-input-schema';
import { diagnoseError, formatDuration, interpretExitCode, normalizeBaseCommand, truncateBashText } from './bash-interpret-exit-code';
import type { ExitCodeInfo } from './bash-interpret-exit-code';
export { BASH_OUTPUT_FILE_MODE_ENV, FORK_RETRY_BASE_MS, FORK_RETRY_MAX, GIT_COMMIT_KINDS, HARDENED_PWSH_FLAGS, PLUGIN_HINTS_ENV, PLUGIN_HINT_MARKETPLACES_ENV, SEARCH_SHADOW_ENV, buildSearchShadowPreamble, collapseFindExecEscape, encodeUtf16LeBase64, extractBashCommentLabel, extractBashReadPathRefs, extractBashReadPaths, injectFindMaxdepth, isBashForkExhaustion, isBashOutputFileModeEnabled, isPluginHintsEnabled, isSearchShadowEnabled, normalizeBashInput, resolveAssistantBlockingBudget, resolveBashTimeout, resolveOfficialMarketplaces, rewritePwshInvocation, rewriteWindowsNullRedirect, stripEmptyLines, stripRedundantCdPrefix } from './bash-strip-redundant-cd-prefix';
export type { BashReadPathRef, GitCommitKind } from './bash-strip-redundant-cd-prefix';
export { applyCaBundleFanout, applyProxyFanout, buildBashInputSchema, detectBlockedSleep, detectCompoundCdGit, detectForcePushAdvisory, detectForcePushToProtected, detectSelfKill, parseSleepSeconds } from './bash-build-bash-input-schema';
export { diagnoseError, formatDuration, interpretExitCode, normalizeBaseCommand, truncateBashText } from './bash-interpret-exit-code';
const _bashCache = new BashResultCache();

/**
 * Per-session dedup of surfaced plugin-install hints. A CLI/SDK that emits the
 * SAME `<ami-hint type="plugin" .../>` tag on stderr for EVERY invocation would
 * otherwise re-surface (and, once wired, re-resolve) the identical suggestion on
 * every bash call. This module-level `name@marketplace` set — mutated through the
 * {@link evaluatePluginHint} `seen` gate — bounds a repeated trigger to a single
 * surfaced recommendation for the life of the process, so a thrashing source can't
 * spam the output. Parity: the reference's `triedThisSession` set in
 * `utils/plugins/hintRecommendation.ts` (L83-L96), including its test-only reset.
 */
const _pluginHintSession = new Set<string>();

/**
 * Test-only reset of the per-session plugin-hint dedup set (parity: the reference's
 * `resetHintSessionForTest`, hintRecommendation.ts L94-96), so a test can exercise
 * the first-surface path again without a fresh process.
 */
export function _resetPluginHintSessionForTest(): void {
  _pluginHintSession.clear();
}

/**
 * Enforced upper bound for the bash output cap (parity: the reference's
 * BASH_MAX_OUTPUT_UPPER_LIMIT). AMI keeps its deliberately generous 200k default
 * (a superset of the reference's 30k default) and makes that the enforced ceiling too, so
 * a mistaken or hostile `BASH_MAX_OUTPUT_LENGTH=99999999` cannot flow through and
 * exhaust the context (resource-exhaustion / DoS) — it is capped to this bound.
 */
export const BASH_MAX_OUTPUT_UPPER_LIMIT = MAX_OUTPUT_LENGTH;

/**
 * [functional parity] Resolve the effective bash output-character cap. Honors
 * BASH_MAX_OUTPUT_LENGTH (parity: the reference's BASH_MAX_OUTPUT_LENGTH tuning
 * knob — registered in env-protection's SAFE_ENV_VARS and merged by the config
 * layer). Routes through the canonical {@link validateBoundedIntEnvVar} so the knob
 * is bounded, not just defaulted: absent/invalid/below-MIN_OUTPUT_LENGTH falls back
 * to the 200k default, and a value above {@link BASH_MAX_OUTPUT_UPPER_LIMIT} is
 * capped (previously an absurd value flowed through verbatim — a DoS hole). One
 * implementation of "parse a bounded output limit", shared with the task cap.
 */
export function resolveBashMaxOutput(): number {
  // Routed through the canonical bounded-int helper so an absent/non-positive value
  // falls back to the default AND an out-of-range value is capped to
  // BASH_MAX_OUTPUT_UPPER_LIMIT (DoS/memory-exhaustion guard) instead of forcing the
  // tool to buffer gigabytes — parity with the reference's upper-limit cap (U208).
  // min:1 (not MIN_OUTPUT_LENGTH) so a small-but-positive value is accepted and then
  // clamped UP to the floor via Math.max — whereas 0 / negative / NaN are rejected as
  // invalid and fall back to the default. An over-large value is capped to the upper
  // limit; Math.max then re-asserts the floor (UPPER_LIMIT >= MIN_OUTPUT_LENGTH, so it
  // is a no-op for the cap path). One expression satisfies both the 1000-char floor
  // (bash-tool-parity) and the enforced upper bound (U208).
  const r = validateBoundedIntEnvVar('BASH_MAX_OUTPUT_LENGTH', process.env.BASH_MAX_OUTPUT_LENGTH, {
    default: MAX_OUTPUT_LENGTH,
    min: 1,
    max: BASH_MAX_OUTPUT_UPPER_LIMIT,
  });
  return Math.max(MIN_OUTPUT_LENGTH, r.effective);
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
 * negative timeout (and `=abc` → NaN → `Math.min(NaN, …)` → NaN). Parity: the reference
 * Code's envValidation rejects `isNaN(parsed) || parsed <= 0`.
 */
export function resolveBackgroundTimeoutMs(inputTimeout: unknown): number {
  const coerced = coerceNumber(inputTimeout, 0);
  const requested = coerced > 0 ? coerced : undefined;
  const envMs = parseEnvMs('SUPERINFERENCE_BACKGROUND_COMMAND_TIMEOUT_MS', MAX_BG_TIMEOUT_MS);
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
  // Read back / clean up the shell layer's persisted full-stdout temp file so a
  // large command's complete output is spilled (not just the in-memory head+tail).
  readFileSync: fs.readFileSync as typeof fs.readFileSync,
  unlinkSync: fs.unlinkSync as typeof fs.unlinkSync,
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
 * Bare-repository attack probe (parity with the reference's
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

/**
 * Process the plugin-hint tags a command emitted on stderr (parity: the reference's
 * Bash tool `<claude-code-hint>` handling): ALWAYS strip the internal tags from the
 * model-visible stderr, and — when `AMI_PLUGIN_HINTS` is enabled — run each hint
 * through the synchronous {@link evaluatePluginHint} gate to produce the resolved
 * recommendations and a one-line suggestion note appended to the output. Returns
 * the cleaned stderr plus the surfaced recommendations/note. No network/marketplace
 * lookup is performed here (the expensive resolve is deferred to the async layer).
 */
export function processPluginHints(stderr: string): {
  stderr: string;
  recommendations: PluginRecommendationHint[];
  note: string;
} {
  if (!stderr.includes('<ami-hint')) return { stderr, recommendations: [], note: '' };
  const hints = parsePluginHintTags(stderr);
  const cleaned = stripPluginHintTags(stderr);
  if (!isPluginHintsEnabled()) return { stderr: cleaned, recommendations: [], note: '' };
  const official = resolveOfficialMarketplaces();
  const recommendations: PluginRecommendationHint[] = [];
  let note = '';
  for (const h of hints) {
    const rec = evaluatePluginHint(h, {
      ...(official ? { officialMarketplaces: official } : {}),
      // Session-scoped dedup: a hint already surfaced this process is dropped so a
      // CLI re-emitting the same tag every call cannot re-surface it (anti-thrash).
      seen: _pluginHintSession,
    });
    if (!rec) continue;
    recommendations.push(rec);
    note += `\n\n[Plugin suggestion: ${formatPluginHintNote(rec)}]`;
  }
  return { stderr: cleaned, recommendations, note };
}

/**
 * Resolve the working directory for a bash invocation without a check-then-use
 * (TOCTOU) race, and recover when the directory was deleted out from under the
 * session. Rather than a separate `existsSync` pre-check, it `realpath`s the cwd
 * (canonicalizing symlinks) and acts on the result; if the cwd no longer exists
 * (`ENOENT`/`ENOTDIR`) it falls back to the session's original launch directory
 * (`fallback`, default `process.cwd()`), returning `{ recovered: true }` so the
 * caller can surface the reset. Returns `null` only when neither the cwd nor the
 * fallback can be resolved. Parity: the reference Shell.ts `setCwd` (realpath-and-
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
 * Base commands that normally produce NO stdout on success (`mkdir`, `mv`, …).
 * When a command is composed entirely of these, an empty stdout is expected, so
 * the UI shows a success affordance instead of a misleading "(no output)".
 * Parity: the reference BashTool BASH_SILENT_COMMANDS.
 */
const BASH_SILENT_COMMANDS = new Set([
  'mv', 'cp', 'rm', 'mkdir', 'rmdir', 'chmod', 'chown', 'chgrp', 'touch', 'ln',
  'cd', 'export', 'unset', 'wait',
]);

/**
 * Commands that carry no semantic weight; after a `||` they are pure fallbacks
 * (e.g. `rm x || echo "failed"`), so they do not make a silent command noisy.
 * Parity: the reference BashTool BASH_SEMANTIC_NEUTRAL_COMMANDS.
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
 * command at all. Parity: the reference BashTool isSilentBashCommand.
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

/** Global git options that take a separate value token (so the token after them
 *  is a value, not the subcommand). */
export const GIT_VALUE_OPTS = new Set([
  '-C', '-c', '--git-dir', '--work-tree', '--namespace', '--exec-path',
  '--super-prefix', '--config-env',
]);

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

const STASH_POP_APPLY = new Set(['pop', 'apply']);
const STASH_PUSH_SAVE = new Set(['push', 'save', '']);
const STASH_DROP_CLEAR = new Set(['drop', 'clear']);
const STASH_SUBCOMMANDS = new Set([
  'push', 'save', 'pop', 'apply', 'list', 'show', 'drop', 'clear',
  'branch', 'create', 'store', 'export', 'import',
]);

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

/**
 * Leading tokens that must NOT be auto-moved to the background when a foreground
 * command overruns its timeout. A `sleep` that times out is a pointless process
 * to keep alive as a background task — the model wanted a delay, not a
 * long-running job — so it stays in the foreground and surfaces a plain timeout
 * error instead of lingering as a zombie background task. Parity: the reference
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
 * the command. Parity: the reference BashTool `isAutobackgroundingAllowed`
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

/**
 * Read-only introspection view of the subprocess-env ISOLATION policy applied to
 * every child process spawned by the `bash` tool — i.e. what overriding/stripping
 * AMI injects into spawned shells. Pure (reads the static policy only; no spawn).
 *
 * Parity note: the reference exposes isolation-state getters (e.g. `tmuxSocket.ts`'s
 * `getClaudeTmuxEnv()` / `isSocketInitialized()`) so a caller can inspect what env
 * a child inherits. AMI's isolation is credential-stripping rather than a socket,
 * so this surfaces THAT policy — the exact secret names scrubbed, the suffix
 * patterns, the safe-var exclusions, the INPUT_<NAME> duplicate handling, and the
 * fact that explicit per-session `/env NAME=VALUE` overrides are merged back in —
 * making "what does AMI change about a spawned bash's environment?" answerable via
 * `ami --introspect` without reading source (introspection-on-every-change rule).
 */
export function describeSubprocessEnvIsolation(): {
  /** Exact credential-bearing var names stripped from a spawned child's env. */
  strippedExact: string[];
  /** Suffix regexes (source form) that also mark a var as sensitive → stripped. */
  strippedSuffixPatterns: string[];
  /** Benign vars force-KEPT even if a pattern would otherwise strip them. */
  safeVarPatterns: string[];
  /** `INPUT_<NAME>` (GitHub-Actions) duplicates are scrubbed as their base name. */
  scrubsInputPrefixDuplicates: boolean;
  /** Explicit `/env NAME=VALUE` overrides are merged into the spawned env. */
  sessionEnvOverridesApplied: boolean;
  /** `NODE_EXTRA_CA_CERTS` is fanned out to the per-runtime trust-store vars. */
  caBundleFanoutVars: string[];
  /** Proxy var groups fanned across upper/lower case so all tools share one relay. */
  proxyFanoutVars: string[];
  note: string;
} {
  return {
    strippedExact: [...SENSITIVE_ENV_SET].sort(),
    strippedSuffixPatterns: SENSITIVE_SUFFIX_PATTERNS.map(p => p.source),
    safeVarPatterns: [SAFE_PREFIX_RE.source, ...SAFE_PREFIX_PATTERNS.map(p => p.source)],
    scrubsInputPrefixDuplicates: true,
    sessionEnvOverridesApplied: true,
    caBundleFanoutVars: ['SSL_CERT_FILE', 'REQUESTS_CA_BUNDLE', 'CURL_CA_BUNDLE'],
    proxyFanoutVars: PROXY_FANOUT_GROUPS.flatMap(g => [...g]),
    note: 'the parent AMI process KEEPS these vars (lazy SDK reads); only spawned bash children have them stripped. Explicit /env overrides are then merged in. A single NODE_EXTRA_CA_CERTS / proxy var is fanned out so curl/python/gh share one trust store and relay.',
  };
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
 *
 * The shim directory is created under {@link resolveTmpBase} (which honors
 * `AMI_TMPDIR`/`CLAUDE_CODE_TMPDIR`), NOT the raw `os.tmpdir()`. Under a restrictive
 * sandbox the global tmpdir is not writable — only the sandbox-pinned tmp dir is —
 * so routing the aux-file write through the same base the sandbox exports keeps the
 * shim materializable instead of failing on an un-writable `/tmp` (parity: the reference's
 * PowerShell provider sandbox-tmpdir fallback for provider aux files).
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
    const tmpBase = resolveTmpBase();
    const dir = make ? fs.mkdtempSync(path.join(tmpBase, 'ami-pyshim-')) : path.join(tmpBase, 'ami-pyshim');
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

function scrubEnv(extra?: Record<string, string>): NodeJS.ProcessEnv {
  const env = sanitizeEnvForSubprocess({ ...process.env }) as NodeJS.ProcessEnv;
  applyCaBundleFanout(env);
  applyProxyFanout(env);
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
  // Active-skill env (e.g. CLAUDE_SKILL_DIR / CLAUDE_PLUGIN_ROOT) so imported the reference skills
  // that shell out via $CLAUDE_SKILL_DIR work (issue #22). Applied last; never stripped.
  if (extra) for (const [k, v] of Object.entries(extra)) env[k] = v;
  // Imported-skill helper resolution (issue #33): when a skill is active, PREPEND its
  // `scripts/` dir and the skill dir to PATH — mirroring the python-shim prepend above and
  // honoring issue #33's suggested fix ("put $CLAUDE_SKILL_DIR / the skill's scripts/ dir on
  // PATH"). Imported the reference skills frequently ship a helper under `<skillDir>/scripts/`;
  // without this, invoking it by BARE name (e.g. `impeccable`) from a cwd that is NOT the
  // skill dir failed with "command not found" (the agent's cwd is usually a scratch/project
  // dir). PATH is composed FROM the already-scrubbed base (never REPLACED — putting PATH in
  // skillEnv itself would clobber the base + python shim), so the skill dirs take precedence
  // for bare-name lookups while every system binary still resolves. Set CLAUDE_SKILL_DIR
  // (not SKILL_DIR, which is the same value) is the trigger. REGRESSION LOCK: if a skill is
  // active and `<skillDir>/scripts` is NOT on PATH, imported-skill helpers break again.
  // AMI_SKILL_DIR is the canonical var AMI exports; CLAUDE_SKILL_DIR is the compat alias
  // for imported the reference skills (both point at the same dir).
  const activeSkillDir = extra?.AMI_SKILL_DIR ?? extra?.CLAUDE_SKILL_DIR;
  if (activeSkillDir) {
    const skillPathEntries = [path.join(activeSkillDir, 'scripts'), activeSkillDir].join(path.delimiter);
    env.PATH = env.PATH ? skillPathEntries + path.delimiter + env.PATH : skillPathEntries;
  }
  return env;
}

/**
 * Build the subprocess env for a bash spawn: the scrubbed base env plus the
 * active skill's env AND any session-scoped env vars the user set via `/env`.
 * User-set session vars are merged first and the framework skill env
 * (CLAUDE_SKILL_DIR / CLAUDE_PLUGIN_ROOT) applied last so an operator cannot
 * accidentally clobber the skill wiring. Parity: the reference's shell provider
 * merges `getSessionEnvVars()` into `getEnvironmentOverrides()` for every
 * spawned command (the REPL process itself is never mutated). The lookup is
 * session-scoped via `context.sessionId`.
 */
function buildSubprocessEnv(context: ToolContext): NodeJS.ProcessEnv {
  const overrides = sessionEnvOverrides(context.sessionId);
  const extra = Object.keys(overrides).length > 0
    ? { ...overrides, ...(context.skillEnv ?? {}) }
    : context.skillEnv;
  return scrubEnv(extra);
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
  // [functional parity] Affordance hint (the reference prompt.ts:L332): verify the
  // parent directory before creating files/dirs so a typo'd path fails loudly
  // instead of silently writing to the wrong place.
  + 'If a command will create new files or directories, first run `ls` to verify the parent directory exists and is the '
  + 'correct location. '
  + 'The working directory persists between commands, but shell state does NOT: each call runs in a fresh non-interactive '
  + 'shell that is not initialized from your profile (~/.bashrc/.zshrc), so a prior `cd`, exported variables, and your '
  + 'user-defined aliases/functions are unavailable in the next call — use absolute paths and chain dependent commands with &&. '
  // [functional parity] Working-directory stability guidance (the reference prompt.ts:L334):
  // keep the cwd stable across the session — prefer absolute paths and avoid `cd`,
  // using it only when the user explicitly asks.
  + 'Keep your working directory stable: prefer absolute paths and avoid `cd` — only `cd` when the user explicitly asks you to. '
  // [functional parity] Multiple-command guidance (the reference prompt.ts:L297-L302):
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

// Git guidance, suppressed when git instructions are disabled (parity: the reference's
// prompt.ts shouldIncludeGitInstructions()/getCommitAndPRInstructions() gate,
// which drops the git/commit-PR section for deployments that don't want it).
const BASH_DESCRIPTION_GIT =
  'Git: `git commit` via bash is blocked — use the git_commit tool to commit. Never amend/--no-verify/force-push, and never '
  + 'commit unless explicitly asked. ';

// Background-task guidance, suppressed when background tasks are disabled so the
// model is never told about a capability that is turned off for this deployment.
const BASH_DESCRIPTION_BACKGROUND =
  ' For intentionally long tasks pass run_in_background: true; a command that overruns its timeout is automatically moved to '
  + 'the background so the session stays responsive. You will be notified when a background command finishes — do not poll it.';

/**
 * [functional parity] Output-truncation threshold surfaced to the model (the reference
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
 * run_in_background guidance is dropped (parity: the reference's prompt.ts suppresses the
 * background usage note behind CLAUDE_CODE_DISABLE_BACKGROUND_TASKS). When git
 * instructions are disabled the git/commit sentence is dropped (parity: the reference's
 * shouldIncludeGitInstructions()/getCommitAndPRInstructions() gate).
 */
export function buildBashDescription(backgroundDisabled: boolean, gitDisabled = false): string {
  const head = BASH_DESCRIPTION_BASE + (gitDisabled ? '' : BASH_DESCRIPTION_GIT) + BASH_DESCRIPTION_TAIL;
  const withBg = backgroundDisabled ? head : head + BASH_DESCRIPTION_BACKGROUND;
  // Always surface the output-truncation threshold so the model can plan around it.
  return withBg + describeOutputTruncation();
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

  // Human-readable live-progress phrase (parity: the reference's getActivityDescription):
  // "Running <description|command>". Shown in a sub-agent's task_list progressSummary.
  getActivityDescription(input: Record<string, unknown>): string {
    return describeToolActivity('bash', input) ?? 'Running command';
  },
  // Per-invocation read/search/list classification from the actual command (parity:
  // the reference's isSearchOrReadCommand → isSearchOrReadBashCommand), so a read-only
  // shell command (`cat`, `grep`, `ls`) collapses in live progress while a mutation
  // stays visible.
  isSearchOrReadCommand(input: Record<string, unknown>) {
    return classifyBashAccess(typeof input?.command === 'string' ? input.command : '');
  },
  // The resolved timeout this foreground command will be killed at (parity: the reference's
  // timeoutMs threaded into shell progress/result UI). Surfaced by the executor on a
  // long-running call's live progress so the user can see how close the command is to
  // its timeout boundary. Honors the per-call `timeout` plus BASH_DEFAULT_TIMEOUT_MS /
  // BASH_MAX_TIMEOUT_MS exactly as execute() does, via the shared resolveBashTimeout.
  getProgressBudgetMs(input: Record<string, unknown>): number | undefined {
    return resolveBashTimeout(input?.timeout);
  },

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
        // as a tooling fault rather than a command result. Parity: the reference's
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
      // guidance can call it out specifically (the reference gitOperationTracking CommitKind).
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

    const secCheck = validateBashSecurity(command, { detachedMode: context.detachedMode, cwd: context.cwd });
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

    // parseOptionalBool: providers serialize booleans as strings; a naive truthy
    // check would background on the STRING "false". Strict parse also REJECTS a
    // malformed flag (e.g. "yes"/1) instead of silently treating it as false, so
    // the model learns its arg was wrong (parity: semanticBoolean leaves non-
    // boolean-ish input for schema rejection). Shared with task.ts.
    const ribParsed = parseOptionalBool(input.run_in_background, 'run_in_background');
    if ('error' in ribParsed) {
      return { output: `Error: ${ribParsed.error}`, isError: true };
    }
    // When background tasks are disabled (AMI_DISABLE_BACKGROUND_TASKS), force the
    // command to run in the foreground even if the model still requested a
    // background run — the capability is fully off (parity: the reference runs such
    // commands in the foreground when CLAUDE_CODE_DISABLE_BACKGROUND_TASKS is set).
    const runInBackground = ribParsed.value && !isBackgroundTasksDisabled();
    const sleepBlock = detectBlockedSleep(command, runInBackground, !!input.timeout);
    if (sleepBlock) return { output: `Error: ${sleepBlock}`, isError: true };

    // [FUNCTIONAL CHANGE] Simulated sed in-place edit (_simulatedSedEdit parity).
    // A lone `sed -i 's/…/…/' file` is semantically a FILE EDIT: the permission
    // prompt already renders a before → after diff computed in-memory via
    // applySedSubstitution (common/src/permission-preview.ts). Running the real
    // `sed` binary afterwards could write bytes that differ from that previewed
    // diff (sed-vs-JS-regex divergence). To GUARANTEE the write equals what the
    // user approved, perform the SAME in-memory substitution and write it directly
    // — the identical exactness guarantee as the reference's SedEditPermissionRequest
    // `_simulatedSedEdit` (the approved write uses the JS-computed newContent, not a
    // re-run of sed). Only faithful, single-file, foreground substitutions take this
    // path (applySimulatedSedEdit returns null otherwise → the real binary runs).
    // Container mode is excluded so file writes stay inside the container's isolated
    // mount rather than touching the host directly.
    if (!runInBackground && context.sandbox?.mode !== 'container') {
      const simulated = applySimulatedSedEdit(command, context.cwd);
      if (simulated) {
        // Real `sed -i` prints nothing on success, so the output is empty; the
        // metadata flag makes the simulated path observable (introspection) and
        // mirrors file_edit's `filePath` result key.
        return {
          output: '',
          metadata: { sedSimulatedEdit: true, filePath: simulated.filePath },
        };
      }
    }

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
    // invocations (the reference `sessionEnvironment` parity). [FUNCTIONAL CHANGE]
    // The script is folded into the EXECUTED command only; the display `command`
    // is left untouched so transcripts/output stay clean.
    const sessionEnvScript = await getSessionEnvironmentScript(context.sessionId);
    // Harden a simple PowerShell invocation to the `-NoProfile -NonInteractive
    // -NoLogo -EncodedCommand` form at EXECUTION time only (display `command` stays
    // the readable original). [FUNCTIONAL CHANGE] Parity: the reference pwsh parser.
    const hardenedCommand = rewritePwshInvocation(command);
    // Opt-in (AMI_SEARCH_SHADOW): prepend a preamble that shadows bare grep/find
    // with GrepTool-matching defaults. Folded into the EXECUTED command only, like
    // the session env script, so transcripts/output stay clean. [FUNCTIONAL CHANGE]
    const searchShadow = isSearchShadowEnabled() ? buildSearchShadowPreamble() : '';
    const preamble = [sessionEnvScript, searchShadow].filter(Boolean).join('\n');
    const commandToRun = preamble ? `${preamble}\n${hardenedCommand}` : hardenedCommand;
    let effectiveCommand: string;
    // Surfaced when the host sandbox degraded to resource-limits-only while a
    // network/filesystem carve-out was explicitly requested (see
    // describeSandboxFallbackWarning) — appended to the command output below so the
    // lost protection is not silent.
    let sandboxFallbackAdvisory: string | null = null;
    // PARITY (CLAUDE_CODE_BASH_SANDBOX_SHOW_INDICATOR → SandboxedBash): when the
    // indicator knob is on and the command is actually sandboxed, the user-facing
    // name becomes `SandboxedBash` and a visible `[SandboxedBash]` marker is
    // prefixed to the output. Default OFF → stays `Bash` and output is unchanged.
    let sandboxDisplayName = 'Bash';
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
      // (AMI_DISABLE_SANDBOX_ESCAPE) — both mirror the reference's
      // areUnsandboxedCommandsAllowed() policy boundary.
      const sandboxEscapeLocked = context.sandbox?.allowUnsandboxedCommands === false || isSandboxEscapeDisabled();
      const disableSandbox = sandboxEscapeLocked ? false : coerceBool(input.dangerouslyDisableSandbox);
      // Global host-sandbox off-switch (parity: isSandboxingEnabled): the env knob
      // or `sandbox.enabled === false` disables the lightweight auto-wrap entirely,
      // so a trigger-pattern match no longer forces the unshare backend on. The
      // platform/off-switch/default-deny/exemption gating lives in the SINGLE
      // canonical `commandWillBeHostSandboxed` (shared with the permission layer's
      // autoAllowBashIfSandboxed host-mode path), so the "will this be sandboxed"
      // decision has one home. `disableSandbox` (a per-call input) is handled here.
      const useSandbox = !disableSandbox && commandWillBeHostSandboxed(command, {
        excludedCommands: context.sandboxExcludedCommands,
        enabledPlatforms: context.sandbox?.enabledPlatforms,
        enabled: context.sandbox?.enabled,
        shouldUseSandboxImpl: _bashHooks.shouldUseSandbox,
      });
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
      // PARITY (sandbox indicator): resolve the user-facing name for this run —
      // `SandboxedBash` only when the knob is on AND the command is sandboxed.
      sandboxDisplayName = sandboxBashDisplayName(useSandbox);
    }

    if (runInBackground && context.processManager) {
      const desc = (input.description as string) || extractBashCommentLabel(command) || command.slice(0, 80);
      // [functional parity] coerce a numeric-string timeout, honor the env knob,
      // and reject NaN/non-positive env values (see resolveBackgroundTimeoutMs).
      const bgTimeoutMs = resolveBackgroundTimeoutMs(input.timeout);
      // A model-requested 'monitor' kind opts a streaming command out of the stall
      // watchdog and gives it stream-ended completion wording. Any other value
      // (incl. the default/absent) is a plain 'bash' task. Parity: the reference's
      // BashTaskKind threaded through the spawn input.
      const backgroundKind = input.background_kind === 'monitor' ? 'monitor' as const : undefined;
      const taskId = context.processManager.spawn(effectiveCommand, {
        cwd: context.cwd,
        description: desc,
        env: buildSubprocessEnv(context),
        timeoutMs: bgTimeoutMs,
        ...(backgroundKind ? { kind: backgroundKind } : {}),
        // Record the owning agent so the shell is reaped if that agent exits first
        // (zombie prevention — see ProcessManager.killTasksForAgent).
        agentId: context.agentId,
        // Cooperative cancellation (parity: the reference Shell.ts abortSignal): a
        // turn abort kills the background shell; an abort whose reason is 'interrupt'
        // is a soft interrupt that leaves the shell running to be backgrounded.
        ...(context.abortSignal ? { signal: context.abortSignal } : {}),
        // File mode (AMI_BASH_OUTPUT_FILE_MODE): route stdout+stderr straight to the
        // output fd so the firehose bypasses the JS heap (parity: the reference TaskOutput
        // stdoutToFile). Opt-in; default pipe mode keeps the separable stderr tail.
        ...(isBashOutputFileModeEnabled() ? { fileMode: true } : {}),
      });
      const task = context.processManager.get(taskId);
      return {
        output: `[background: true]\nBackground task started.\n  Task ID: ${taskId}\n  PID: ${task?.pid ?? 'unknown'}\n  Command: ${command}\n  Timeout: ${Math.round(bgTimeoutMs / 1000)}s\n\nUse task_output({ task_id: "${taskId}" }) to check status and output.\nUse task_kill({ task_id: "${taskId}" }) to stop it.\nUse task_list() to see all background tasks.`,
        isError: false,
        // The model EXPLICITLY requested run_in_background — tag it so callers can
        // distinguish a user/model-intended background task from an auto-moved one
        // (the timeout path sets `assistantAutoBackgrounded`). Parity: the reference's
        // ExecResult `backgroundedByUser` vs assistant auto-background discriminator.
        metadata: { backgrounded: true, backgroundedByUser: true, taskId },
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
      env: buildSubprocessEnv(context),
      onData: context.onProgress ? (chunk) => context.onProgress!(chunk) : undefined,
      // Persist the COMPLETE stdout to disk so a large build/test log can be
      // read back in full (including the middle the in-memory head+tail window
      // drops) — see the fullOutputPath handling at the spill site below.
      persistFullOutput: true,
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
      // the reference gates this behind isAutobackgroundingAllowed / shouldAutoBackground.
      if (context.processManager && isAutobackgroundingAllowed(command)) {
        const taskId = context.processManager.spawn(effectiveCommand, {
          cwd: context.cwd,
          description: `[auto-bg] ${extractBashCommentLabel(command)?.slice(0, 60) ?? command.slice(0, 60)}`,
          env: buildSubprocessEnv(context),
          agentId: context.agentId,
          // Honor file mode for auto-backgrounded foreground commands too, so the
          // routing is consistent regardless of how a task entered the background.
          ...(isBashOutputFileModeEnabled() ? { fileMode: true } : {}),
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
    // When the shell layer persisted the COMPLETE stdout to disk (output
    // overflowed the in-memory head+tail window), spill the FULL stdout — not the
    // middle-dropped in-memory copy — so the model can file_read every byte of a
    // large build/test log. Re-fold the raw stdout the same way presentCommandOutput
    // did so the spilled file is consistently formatted. Best-effort: fall back to
    // the in-memory preview if the temp file can't be read, and always remove it.
    let spillStdout = presented.stdout;
    if (result.fullOutputPath) {
      try {
        const rawFull = _bashHooks.readFileSync(result.fullOutputPath, 'utf8');
        spillStdout = presentCommandOutput(command, rawFull, '').stdout;
      } catch { /* fall back to the in-memory preview */ }
      try { _bashHooks.unlinkSync(result.fullOutputPath); } catch { /* already gone */ }
    }
    const spilled = spillToolTextWithMeta(context.cwd, 'bash', spillStdout, bashMaxChars);
    let stdout = spilled.text;
    // In container mode, explain a failure that was likely caused by sandbox
    // isolation (network off, or a host path not mounted inside the container) so
    // the model does not misdiagnose a blocked op as a real error.
    const rawStderr =
      context.sandbox?.mode === 'container'
        ? annotateStderrWithSandboxFailures(command, presented.stderr, {
            allowNetwork: context.sandbox.allowNetwork !== false,
            projectDir: context.sandbox.projectDir || context.cwd,
            ignoreViolations: context.sandbox.ignoreViolations,
          })
        : presented.stderr;
    // Plugin-install hints: a CLI/SDK may emit an `<ami-hint type="plugin" .../>`
    // tag on stderr. The tag is ALWAYS stripped so it never leaks to the model; when
    // AMI_PLUGIN_HINTS is enabled, surface a non-blocking install suggestion + record
    // it in metadata for introspection. Parity: the reference's stderr-hint recommendation.
    const pluginHintResult = processPluginHints(rawStderr);
    const stderr = pluginHintResult.stderr;

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
    if (pluginHintResult.note) output += pluginHintResult.note;
    // PARITY (sandbox indicator): prefix the visible marker when the resolved
    // user-facing name is `SandboxedBash` (the reference's indicator-knob name switch).
    if (sandboxDisplayName !== 'Bash') output = `[${sandboxDisplayName}]\n${output}`;

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
      // fault from a genuine non-zero exit. Parity: the reference's preSpawnError field.
      metadata: {
        ...(result.exitCode === SPAWN_FAILURE_EXIT_CODE
          ? { readOnly, preSpawnError: 'spawn-failure' }
          : { readOnly }),
        ...(pluginHintResult.recommendations.length ? { pluginHints: pluginHintResult.recommendations } : {}),
        // When stdout overflowed the inline cap, the full output was persisted to
        // disk; surface the path + byte size structurally so a programmatic caller
        // need not re-parse the banner. Parity: the reference's ExecResult
        // outputFilePath/outputFileSize (ShellCommand.ts #handleExit).
        ...(spilled.outputFilePath
          ? { outputFilePath: spilled.outputFilePath, outputFileSize: spilled.outputFileSize }
          : {}),
        // Headless terminal fold side-band: present ONLY when the renderer
        // actually collapsed control-heavy output, so an ordinary plain-log
        // command's metadata shape is unchanged. Lets a programmatic caller see
        // that the screen was flattened (and any OSC 7 cwd / OSC title / curses
        // alt-screen a human would have seen). Parity: Orca HeadlessEmulator.
        ...(presented.headless.used
          ? {
              headlessRender: {
                used: true,
                alternateScreen: presented.headless.alternateScreen,
                ...(presented.headless.cwd ? { cwd: presented.headless.cwd } : {}),
                ...(presented.headless.title ? { title: presented.headless.title } : {}),
              },
            }
          : {}),
      },
    };
  },
};

/** Raw-byte ceiling for a shell-produced image before we refuse to decode it
 *  (parity: the reference's MAX_IMAGE_FILE_SIZE = 20MB). Estimated from the base64
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
 * Parity: the reference BashTool utils isImageOutput / buildImageToolResult /
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
  // it (parity: the reference's background notice includes the output file path).
  if (opts?.outputPath) {
    notice += ` Output is being written to: ${opts.outputPath}.`;
  }
  // Assistant/detached-mode coaching: steer the main agent to keep itself
  // responsive rather than blocking on the long task (parity: the reference's
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
