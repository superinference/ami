import * as fs from 'fs';
import * as path from 'path';
import * as child_process from 'child_process';
import * as crypto from 'crypto';
import { ToolContext, ToolDefinition, ToolResult } from '../types';
import { computeDefaultBranch, findRemoteBase, isShallowClone, readWorktreeHeadSha, resolveRef } from '../utils/git';
import { errorMessage } from '../credential-guard';


const SLUG_SEGMENT_RE = /^[a-zA-Z0-9._-]+$/;

/**
 * Non-interactive git environment (parity with the reference's centralized
 * GIT_NO_PROMPT_ENV): applied to EVERY git subprocess so a repo whose remote
 * needs auth can never block the synchronous call on an interactive credential /
 * terminal prompt and hang the engine. Merged OVER the caller's env so a provided
 * env (e.g. a custom GIT_DIR) is preserved while the no-prompt guarantee holds.
 */
const GIT_NO_PROMPT_ENV = { GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '' } as const;

/**
 * Hard upper bound applied to EVERY git subprocess that does not pass a shorter,
 * explicit timeout (parity with the reference's execFileNoThrow 10-minute default). A
 * wedged git process (e.g. a hung network fetch) therefore always terminates
 * instead of hanging the tool — and the engine — indefinitely.
 */
const DEFAULT_GIT_TIMEOUT_MS = 10 * 60 * 1000;

/**
 * Generous default stdout cap applied to EVERY captured git subprocess (parity
 * with the reference's centralized execFileNoThrow `maxBuffer` default). Node's bare
 * `execFileSync` cap is 1 MiB; a repo with very many registered worktrees makes
 * `git worktree list --porcelain` exceed it and throw an opaque ENOBUFS that
 * truncates the capture. Bounding it here (and still handling overflow in the
 * caller's catch) keeps the output buffer uniform instead of crashing.
 */
const DEFAULT_GIT_MAX_BUFFER = 64 * 1024 * 1024;

/** Options for a single no-shell git invocation. */
export interface GitExecOpts {
  cwd: string;
  stdio?: 'ignore' | 'pipe';
  /** Per-call timeout; falls back to {@link DEFAULT_GIT_TIMEOUT_MS} when omitted. */
  timeout?: number;
  env?: NodeJS.ProcessEnv;
  /** Abort signal; a cancelled turn kills the in-flight git process. */
  signal?: AbortSignal;
  /** Per-call stdout cap; falls back to {@link DEFAULT_GIT_MAX_BUFFER} when omitted. */
  maxBuffer?: number;
}

/**
 * Human-readable default worktree name (parity with the reference's EnterWorktreeTool
 * `input.name ?? getPlanSlug()` default — when the model omits `name`, derive a
 * memorable word slug like "graceful-unicorn" instead of an opaque random-hex
 * name, so auto-created worktrees are easy to recognise in `git worktree list`).
 * Retries on collision against the managed `.ami/worktrees/` area so a generated
 * name never clobbers an in-flight worktree; after a bounded number of
 * collisions it appends random hex to guarantee termination.
 */
function defaultWorktreeName(worktreesBase: string): string {
  for (let i = 0; i < 20; i++) {
    const candidate = _enterWorktreeHooks.generateSlug();
    if (!fs.existsSync(path.join(worktreesBase, candidate))) return candidate;
  }
  return `${_enterWorktreeHooks.generateSlug()}-${crypto.randomBytes(2).toString('hex')}`;
}

// Strict-input allow-list (parity with exit_worktree's ALLOWED_KEYS): any key
// outside this set is rejected rather than silently ignored, so a typo'd or
// hallucinated parameter surfaces loudly instead of being dropped.
const ALLOWED_KEYS = new Set(['name', 'path', 'base_ref', 'pr', 'description']);

/**
 * Build the child_process options for a git invocation with the three hardening
 * guarantees applied uniformly (so no call site can forget one):
 *   - a bounded `timeout` (never unbounded) — no git call can hang the engine;
 *   - the non-interactive {@link GIT_NO_PROMPT_ENV} merged over the caller's env —
 *     no git call can block on a credential prompt;
 *   - the caller's abort `signal` — a cancelled turn kills the in-flight process.
 * Exported for direct unit testing of the hardening defaults.
 */
export function buildGitExecOptions(
  opts: GitExecOpts,
): child_process.ExecFileSyncOptionsWithStringEncoding {
  // `signal` is honored by execFileSync at runtime (Node >= 16) but is absent from
  // the @types/node ExecFileSyncOptions shape, so attach it through the spread.
  return {
    cwd: opts.cwd,
    encoding: 'utf-8',
    stdio: opts.stdio === 'ignore' ? 'ignore' : ['ignore', 'pipe', 'pipe'],
    timeout: opts.timeout ?? DEFAULT_GIT_TIMEOUT_MS,
    maxBuffer: opts.maxBuffer ?? DEFAULT_GIT_MAX_BUFFER,
    env: { ...(opts.env ?? process.env), ...GIT_NO_PROMPT_ENV },
    ...(opts.signal ? { signal: opts.signal } : {}),
  } as child_process.ExecFileSyncOptionsWithStringEncoding;
}

/**
 * Run git with NO SHELL (execFile with an argv array) so a branch name, path, or
 * ref can never be interpreted as a shell metacharacter (command-injection
 * immune — parity with worktree-manager's gitExec and the reference's
 * execFileNoThrowWithCwd). Every call is time-bounded, non-interactive, and
 * cancellable via {@link buildGitExecOptions}. Returns trimmed stdout.
 */
function runGit(args: string[], opts: GitExecOpts = { cwd: process.cwd() }): string {
  const out = child_process.execFileSync('git', args, buildGitExecOptions(opts));
  return typeof out === 'string' ? out.trim() : '';
}

/** Shared rejection swallow for fire-and-forget hook promises. Exported for coverage. */
export function swallowHookRejection(): void { /* fire-and-forget */ }

/**
 * Dependency seam for tests (mirrors the _createClient / _lspDiagHooks pattern).
 * All git subprocess calls route through `exec` (an argv array, NO shell) so
 * tests can inject failures (e.g. a corrupted `git worktree list`, an
 * unresolvable HEAD) via `mock.method(_enterWorktreeHooks, 'exec', ...)`. The
 * default implementation is the real no-shell git runner, so production
 * behaviour is unchanged.
 */
export const _enterWorktreeHooks = {
  exec: runGit,
  /** Human-readable default-name generator (parity with the reference's getPlanSlug
   *  default). Delegates to the shared words corpus; a mockable seam property so
   *  tests can force a deterministic / colliding slug without monkeypatching the
   *  frozen `../words` ESM namespace. */
  generateSlug: (): string => require('../words').generateShortWordSlug(),
  /** Post-add setup step (symlink large dirs into the new worktree). Routed
   *  through the seam so tests can inject a post-add failure and exercise the
   *  half-created-worktree teardown path. `dirs` honors the opt-in
   *  `settings.worktree.symlinkDirectories` knob: when undefined the underlying
   *  helper keeps AMI's `['node_modules']` default; an explicit (possibly empty)
   *  list is used verbatim. */
  symlinkLargeDirs: (sourceCwd: string, worktreePath: string, dirs?: string[]): string[] =>
    require('../worktree-manager').symlinkLargeDirectories(sourceCwd, worktreePath, dirs),
  /** Stale-clone diagnostic (FETCH_HEAD age). Seam so tests force the empty catch. */
  isCloneStale: (cwd: string): boolean => require('../worktree-manager').isCloneStale(cwd),
  /** Derive a readable kebab-case worktree/branch slug from a free-form task
   *  description via a deterministic auxiliary model call (parity with the reference's
   *  teleport generateTitleAndBranch). Routed through the seam so tests can inject
   *  a deterministic slug (or a failure) without a live provider. Delegates to the
   *  canonical generateBranchName in title-generator (one branch-naming impl). */
  branchNameFromDescription: (
    description: string,
    provider: import('../types').ProviderConfig,
    signal?: AbortSignal,
  ): Promise<string> =>
    require('../title-generator').generateBranchName(
      description, provider, undefined, undefined, signal,
      // Introspection sink (parity: the reference's logResult): record the structured
      // outcome of the branch-name background query so a failed/aborted/unparseable
      // slug generation is observable instead of silently collapsing to the fallback.
      logBranchNameQueryResult,
    ),
};

/**
 * Log a failed/aborted/unparseable branch-name auxiliary query. Extracted so the
 * error arm is directly unit-testable without a live provider round-trip.
 */
export function logBranchNameQueryResult(r: import('../title-generator').AuxQueryResult): void {
  if (r.type === 'error') {
    require('../logger').log(
      'worktree',
      `branch-name query ${r.reason}${r.error ? `: ${r.error}` : ''}`,
      { source: r.source, model: r.model },
      'warning',
    );
  }
}

/**
 * Resolve the opt-in `settings.worktree.symlinkDirectories` list (parity with
 * the reference's `getInitialSettings().worktree?.symlinkDirectories`). Project config
 * (`<cwd>/.ami/config.json`) takes precedence over the global config
 * (`~/.ami/config.json`); either may configure the list. Returns `undefined` when
 * NEITHER tier configures the key, so the caller keeps AMI's default of symlinking
 * `node_modules`. An explicitly-configured list (including an empty array → symlink
 * nothing) is returned verbatim. Config-read failures fall back to the default.
 */
function resolveSymlinkDirs(cwd: string): string[] | undefined {
  // loadProjectConfig / loadGlobalConfig never throw (tier loader catches I/O).
  const { loadProjectConfig, loadGlobalConfig } = require('../config');
  const proj = loadProjectConfig(cwd)?.worktree?.symlinkDirectories;
  if (Array.isArray(proj)) return proj;
  const glob = loadGlobalConfig()?.worktree?.symlinkDirectories;
  if (Array.isArray(glob)) return glob;
  return undefined;
}

/**
 * Resolve the opt-in `settings.worktree.sparsePaths` cone-mode allow-list (parity
 * with the reference's `getInitialSettings().worktree?.sparsePaths`). Project config
 * (`<cwd>/.ami/config.json`) takes precedence over the global config
 * (`~/.ami/config.json`). Returns `undefined` when NEITHER tier configures a
 * non-empty list, so the caller does a normal full checkout. A configured
 * non-empty list is returned verbatim so only those subtrees materialise.
 * Config-read failures fall back to the full checkout.
 */
function resolveSparsePaths(cwd: string): string[] | undefined {
  // loadProjectConfig / loadGlobalConfig never throw (tier loader catches I/O).
  const { loadProjectConfig, loadGlobalConfig } = require('../config');
  // Project config is authoritative when it DEFINES the key: a project that
  // explicitly configures `sparsePaths` (even as an empty array → "full
  // checkout, ignore any global sparse policy") wins over the global tier.
  const proj = loadProjectConfig(cwd)?.worktree?.sparsePaths;
  if (Array.isArray(proj)) return proj.length > 0 ? proj : undefined;
  const glob = loadGlobalConfig()?.worktree?.sparsePaths;
  if (Array.isArray(glob) && glob.length > 0) return glob;
  return undefined;
}

/**
 * Resolve the opt-in `settings.worktree.tmux` toggle (parity with the reference's
 * `--tmux` worktree mode). Project config (`<cwd>/.ami/config.json`) takes
 * precedence over the global config (`~/.ami/config.json`); either may enable it.
 * Returns true only when a tier sets it to exactly `true`; ABSENT / false / an
 * unreadable config all mean "no tmux session" (the default).
 */
function resolveWorktreeTmux(cwd: string): boolean {
  // loadProjectConfig / loadGlobalConfig never throw (tier loader catches I/O).
  const { loadProjectConfig, loadGlobalConfig } = require('../config');
  const proj = loadProjectConfig(cwd)?.worktree?.tmux;
  if (typeof proj === 'boolean') return proj;
  const glob = loadGlobalConfig()?.worktree?.tmux;
  if (typeof glob === 'boolean') return glob;
  return false;
}

function validateSlug(name: string): boolean {
  if (name.length > 64) return false;
  if (path.isAbsolute(name)) return false;
  const segments = name.split('/');
  for (const seg of segments) {
    if (!seg || seg === '.' || seg === '..') return false;
    if (!SLUG_SEGMENT_RE.test(seg)) return false;
  }
  return true;
}

function getDefaultBranch(cwd: string, signal?: AbortSignal): string {
  // One canonical default-branch algorithm shared with `/commit-push-pr` lives in
  // `computeDefaultBranch`; route it through the mockable `_enterWorktreeHooks.exec`
  // seam so tests keep injecting the symref / rev-parse results they already do.
  // The abort signal is threaded through so a cancelled turn also interrupts the
  // default-branch probe's git calls.
  return computeDefaultBranch(cwd, (args, opts) =>
    _enterWorktreeHooks.exec(args, { cwd: opts.cwd, stdio: opts.stdio, signal }));
}

/**
 * Resolve the canonical MAIN-repo root for `cwd` (parity with the reference's
 * findCanonicalGitRoot): the working-tree directory that owns the shared git
 * common dir. `git rev-parse --path-format=absolute --git-common-dir` resolves to
 * the MAIN repo's `.git` (shared by every linked worktree), so its parent is the
 * main working tree. Computing the worktrees base from THIS root means a worktree
 * created from a SUBDIRECTORY of the repo still lands in the repo root's
 * `.ami/worktrees/` rather than nested under the subdirectory. Falls back to the
 * raw `cwd` when the lookup is unavailable (older git without `--path-format`, or a
 * transient failure) so creation still proceeds.
 */
function resolveMainRepoRoot(cwd: string, signal?: AbortSignal): string {
  try {
    const commonDir = _enterWorktreeHooks.exec(
      ['rev-parse', '--path-format=absolute', '--git-common-dir'],
      { cwd, stdio: 'pipe', signal },
    );
    if (commonDir) return path.dirname(path.resolve(commonDir));
  } catch { /* unresolvable — fall back to the raw cwd */ }
  return cwd;
}

/**
 * Non-git fallback: create (or enter) a worktree by delegating to a configured
 * WorktreeCreate hook so worktree isolation works with VCS systems other than
 * git (parity with the reference's hook-based createWorktreeForSession / setup.ts
 * non-git branch). The hook is responsible for the actual on-disk isolation; we
 * only await it, verify the target materialised, then switch the session into it
 * (recording a `hookBased` worktree session). With no WorktreeCreate hook
 * configured, this returns a loud error that names the cwd and the escape hatch.
 */
async function createViaWorktreeHook(
  context: ToolContext,
  name: string | undefined,
  existingPath: string | undefined,
  baseRefProvided: boolean,
  prProvided: boolean,
): Promise<ToolResult> {
  const cwd = context.cwd || process.cwd();
  const hookCount = context._hookManager?.getSummary?.().worktreeCreate ?? 0;
  if (!context._hookManager || hookCount === 0) {
    return {
      output:
        `Error: not inside a git repository (${cwd}). To use worktree isolation ` +
        'with another VCS, configure WorktreeCreate/WorktreeRemove hooks in settings.',
      isError: true,
    };
  }
  // A git ref cannot be resolved outside git, so base_ref is meaningless here.
  if (baseRefProvided) {
    return { output: 'Error: base_ref requires a git repository (a hook-based worktree cannot resolve a git ref).', isError: true };
  }
  // A pull request is fetched from a git remote, so it cannot seed a non-git,
  // hook-based worktree (parity: the reference's PR path is git-only via `git fetch origin pull/N/head`).
  if (prProvided) {
    return { output: 'Error: pr requires a git repository (a hook-based worktree cannot fetch a pull request).', isError: true };
  }

  // Entering an existing hook-created worktree needs no git: if the directory is
  // present, switch into it; otherwise report it missing.
  if (existingPath) {
    if (!fs.existsSync(existingPath)) {
      return { output: `Error: path "${existingPath}" does not exist.`, isError: true };
    }
    switchHookSession(context, cwd, path.resolve(existingPath), path.basename(existingPath));
    const enteredMessage = `Entered existing worktree at: ${existingPath}. Run exit_worktree when done.`;
    return { output: enteredMessage, metadata: { worktreePath: path.resolve(existingPath), message: enteredMessage } };
  }

  const slug = name || defaultWorktreeName(path.join(cwd, '.ami', 'worktrees'));
  if (!validateSlug(slug)) {
    return { output: `Error: invalid worktree name "${slug}". Use letters, digits, dots, underscores, dashes (max 64 chars per segment).`, isError: true };
  }
  const { flattenSlug } = require('../worktree-manager');
  const flatSlug: string = flattenSlug(slug);
  const worktreeDir = path.join(cwd, '.ami', 'worktrees', flatSlug);
  if (fs.existsSync(worktreeDir)) {
    return { output: `Error: worktree "${flatSlug}" already exists at ${worktreeDir}. Use path to enter an existing worktree.`, isError: true };
  }

  let hookResult: { worktreePath?: string } | void;
  try {
    hookResult = await context._hookManager.executeWorktreeCreate({ name: slug, worktreePath: worktreeDir });
  } catch (e) {
    return { output: `Error: WorktreeCreate hook failed: ${errorMessage(e)}`, isError: true };
  }
  // A provider hook may report the worktree path it actually created (stdout or
  // hookSpecificOutput.worktreePath); use it as the created worktree instead of
  // the default `.ami/worktrees/<slug>` location (the reference parity).
  const effectiveDir = hookResult?.worktreePath ? path.resolve(hookResult.worktreePath) : worktreeDir;
  const effectiveName = hookResult?.worktreePath ? path.basename(effectiveDir) : flatSlug;
  // The hook owns creation; if it did not materialise the directory, fail loud
  // rather than switching the session into a path that does not exist.
  if (!fs.existsSync(effectiveDir)) {
    return { output: `Error: the WorktreeCreate hook did not create ${effectiveDir}.`, isError: true };
  }

  switchHookSession(context, cwd, effectiveDir, effectiveName);
  const createdMessage =
    `Worktree created via WorktreeCreate hook (session switched to it):\n` +
    `  Path: ${effectiveDir}\n` +
    '  Leave with exit_worktree (action:"keep" to preserve, "remove" to delete).';
  return { output: createdMessage, metadata: { worktreePath: effectiveDir, message: createdMessage } };
}

/** Record a hookBased worktree session and switch the context cwd into it,
 *  firing the cwdChanged hook (shared by the create + enter-existing hook paths). */
function switchHookSession(
  context: ToolContext,
  originalCwd: string,
  worktreeDir: string,
  worktreeName: string,
): void {
  const { setWorktreeSession } = require('../worktree-manager');
  setWorktreeSession({
    originalCwd,
    worktreePath: worktreeDir,
    worktreeName,
    worktreeBranch: '',
    hookBased: true,
    createdAt: new Date().toISOString(),
  });
  (context as any)._originalCwd = originalCwd;
  (context as any).cwd = worktreeDir;
  context._hookManager?.executeCwdChanged?.({ oldCwd: originalCwd, newCwd: worktreeDir }).catch(swallowHookRejection);
}

/**
 * Fast-resume an already-existing, registered worktree (parity with the reference's
 * getOrCreateWorktree `existed: true` path): re-establish the worktree session and
 * switch the context cwd into it WITHOUT any fetch or `git worktree add`. The caller
 * has already confirmed the dir is a valid worktree (its HEAD SHA was read directly
 * off disk). Reuses the canonical createWorktreeSession factory (one session path),
 * which no-ops the on-disk add for an existing dir and bumps its mtime so the
 * stale-sweep cannot reap a worktree that is actively being resumed.
 */
function resumeExistingWorktree(
  context: ToolContext,
  repoRoot: string,
  worktreeDir: string,
  flatSlug: string,
  headSha: string,
): ToolResult {
  const { createWorktreeSession, setWorktreeSession } = require('../worktree-manager');
  let symlinkWarnings: string[] = [];
  let session: { worktreePath: string; worktreeBranch: string };
  try {
    session = createWorktreeSession(repoRoot, flatSlug);
    setWorktreeSession(session);
    // Re-ensure the symlinked large dirs exist (idempotent); honors the opt-in
    // settings.worktree.symlinkDirectories knob, undefined keeping AMI's node_modules default.
    symlinkWarnings = _enterWorktreeHooks.symlinkLargeDirs(repoRoot, session.worktreePath, resolveSymlinkDirs(repoRoot));
  } catch (e) {
    // Re-establishing the session for an EXISTING worktree failed. Unlike the create
    // path we do NOT tear the worktree down — it predates this call and may hold the
    // user's uncommitted work; just clear any partial session state and surface the error.
    setWorktreeSession(null);
    return { output: `Error: failed to resume worktree "${flatSlug}": ${errorMessage(e)}`, isError: true };
  }

  const originalCwd = context.cwd;
  if (context.cwd) {
    (context as any)._originalCwd = context.cwd;
    (context as any).cwd = worktreeDir;
  }
  // Fire CwdChanged (parity with the create path) so cwd-scoped lifecycle wiring —
  // the persisted session-env invalidation (clearCwdEnvFiles/invalidateSessionEnvCache)
  // and the file-watcher re-bind — re-evaluates against the resumed worktree's dir.
  context._hookManager?.executeCwdChanged?.({ oldCwd: originalCwd, newCwd: worktreeDir }).catch(swallowHookRejection);

  const warnLines = symlinkWarnings.length ? `\n  Warning: ${symlinkWarnings.join('; ')}.` : '';
  // Report the branch using the SAME `worktree/<slug>` convention the create path
  // stamped onto the actual git branch (createWorktreeSession's session branch uses a
  // different internal naming), so a resume's reported branch matches the live ref.
  const branchName = `worktree/${flatSlug}`;
  const resumedMessage =
    `Resumed existing worktree (session switched to it):\n` +
    `  Path: ${worktreeDir}\n  Branch: ${branchName}\n  HEAD: ${headSha.slice(0, 8)}\n` +
    '  Leave with exit_worktree (action:"keep" to preserve, "remove" to delete).' +
    warnLines;
  return {
    output: resumedMessage,
    // `existed: true` mirrors the reference's getOrCreateWorktree result so a programmatic
    // consumer can tell a resume from a fresh creation.
    metadata: { worktreePath: worktreeDir, worktreeBranch: branchName, message: resumedMessage, existed: true },
  };
}

export const enterWorktreeTool: ToolDefinition = {
  name: 'enter_worktree',
  description:
    'Create or enter a git worktree for isolated file modifications, on a new branch under .ami/worktrees/. ' +
    'WHEN TO USE: isolating risky or parallel changes from the main working tree (a large refactor you may want to throw away, work you want on its own branch). ' +
    'WHEN NOT TO USE: ordinary edits in the current tree, or when the user did not ask for isolation — a worktree adds branch/cwd bookkeeping overhead. ' +
    'Use exit_worktree (action "keep" or "remove") to leave. '
    + 'On session exit while still inside the worktree, the user is prompted to keep or remove it (with a data-loss warning for any uncommitted files / unpushed commits). ' +
    'Requires a git repository; outside one, configure WorktreeCreate/WorktreeRemove hooks for VCS-agnostic isolation. ' +
    'Pass pr (a PR URL or #N) to base the worktree on a pull request. ' +
    'Pass description (a short task summary) instead of name to auto-derive a readable branch name (e.g. "fix-mobile-login-button").',
  inputSchema: {
    type: 'object',
    properties: {
      name: {
        type: 'string',
        description: 'Name for the new worktree. Letters, digits, dots, underscores, dashes; max 64 chars. A random name is generated if omitted.',
      },
      path: {
        type: 'string',
        description: 'Path to an existing worktree to enter instead of creating a new one. Mutually exclusive with name.',
      },
      base_ref: {
        type: 'string',
        description: 'Git ref (branch/tag/commit) to base the new worktree on. Defaults to origin/<default-branch>, then HEAD.',
      },
      pr: {
        type: 'string',
        description: 'Pull request to base the new worktree on — a PR URL (https://<host>/owner/repo/pull/N) or "#N". Fetches origin pull/N/head and bases the worktree on it; names it pr-N if name is omitted. Mutually exclusive with path and base_ref.',
      },
      description: {
        type: 'string',
        description: 'Short task summary used to auto-derive a readable kebab-case branch/worktree name when name is omitted (e.g. "fix the mobile login button" -> "fix-mobile-login-button"). Falls back to a random word slug if generation fails. Mutually exclusive with name, path, and pr.',
      },
    },
  },
  // Structured result shape (parity: the reference's EnterWorktreeTool `outputSchema`
  // returning `{worktreePath, worktreeBranch?, message}`). A frontend / resume
  // path can consume the machine-readable fields carried on `metadata` alongside
  // the human text. Object-root (ToolInputSchema) so it passes the provider's
  // object-root-only exposure guard.
  outputSchema: {
    type: 'object',
    properties: {
      worktreePath: { type: 'string', description: 'Absolute path of the worktree that was created or entered.' },
      worktreeBranch: { type: 'string', description: 'Branch the worktree is on, when known.' },
      tmuxSessionName: { type: 'string', description: 'Name of the detached tmux session created for the worktree, when settings.worktree.tmux is enabled (attach with: tmux attach -t <name>).' },
      message: { type: 'string', description: 'Human-readable summary of what happened.' },
    },
    required: ['worktreePath', 'message'],
  },
  isReadOnly: false,

  // Compact projection fed to the auto-mode security classifier (parity: the reference's
  // EnterWorktreeTool `toAutoClassifierInput(input) => input.name ?? ''`). The
  // auto-approval decision keys on the actual worktree name the model requested
  // rather than the full argument blob; a non-string/omitted name projects to ''
  // (the classifier skips it).
  toAutoClassifierInput: (input) => (typeof input.name === 'string' ? input.name : ''),

  async execute(input, context): Promise<ToolResult> {
    // Strict input: reject unknown keys up front (parity with exit_worktree) so
    // a mistyped parameter can never be silently discarded.
    const unknown = Object.keys(input).filter(k => !ALLOWED_KEYS.has(k));
    if (unknown.length > 0) {
      return { output: `Error: unknown parameter(s): ${unknown.join(', ')}.`, isError: true };
    }

    // Honor cancellation (parity with the reference threading abortSignal into every git
    // exec): short-circuit before doing any git work if the turn is already
    // aborted, and thread `signal` into every git subprocess below so a
    // cancellation kills the in-flight process rather than letting it run on.
    const signal = context.abortSignal;
    if (signal?.aborted) {
      return { output: 'Error: operation aborted before the worktree was created.', isError: true };
    }

    // Trim surrounding whitespace on the name (consistency with base_ref, which
    // is already trimmed below). A whitespace-only name collapses to empty and
    // is then treated as "omitted" → a random name is generated.
    const rawName = input.name;
    const name = typeof rawName === 'string' ? (rawName.trim() || undefined) : (rawName as string | undefined);
    const existingPath = input.path as string | undefined;
    const baseRefProvided = typeof input.base_ref === 'string' && input.base_ref.trim() !== '';
    const rawPr = typeof input.pr === 'string' ? input.pr.trim() : '';
    const prProvided = rawPr !== '';
    const rawDescription = typeof input.description === 'string' ? input.description.trim() : '';
    const descriptionProvided = rawDescription !== '';

    if (name && existingPath) {
      return { output: 'Error: name and path are mutually exclusive.', isError: true };
    }

    // `description` only auto-DERIVES a name when creating a fresh worktree with no
    // explicit name, so it cannot be combined with name (which already names it),
    // path (entering an existing worktree), or pr (which names it pr-N). Reject the
    // combinations loudly rather than silently ignoring the description.
    if (descriptionProvided && name) {
      return { output: 'Error: description and name are mutually exclusive (name already sets the worktree name).', isError: true };
    }
    if (descriptionProvided && existingPath) {
      return { output: 'Error: description cannot be combined with path (description only derives a name when creating a new worktree).', isError: true };
    }
    if (descriptionProvided && prProvided) {
      return { output: 'Error: description and pr are mutually exclusive (a pull request already names the worktree pr-N).', isError: true };
    }

    // base_ref only has meaning when CREATING a new worktree; entering an
    // existing one checks out whatever that worktree already has. Reject the
    // combination instead of silently ignoring base_ref.
    if (existingPath && baseRefProvided) {
      return { output: 'Error: base_ref cannot be combined with path (base_ref only applies when creating a new worktree).', isError: true };
    }

    // A PR reference only makes sense when CREATING a worktree, and it IS the base,
    // so it cannot be combined with an existing path or an explicit base_ref. Reject
    // the combination loudly rather than silently ignoring one input.
    if (prProvided && existingPath) {
      return { output: 'Error: pr cannot be combined with path (pr only applies when creating a new worktree).', isError: true };
    }
    if (prProvided && baseRefProvided) {
      return { output: 'Error: pr and base_ref are mutually exclusive (a pull request already specifies the base).', isError: true };
    }

    // Parse the PR reference up front so an unrecognized value fails before any git
    // work (parity: the reference's parsePRReference accepts a PR URL or "#N").
    let prNumber: number | null = null;
    if (prProvided) {
      const { parsePRReference } = require('../worktree-manager');
      prNumber = parsePRReference(rawPr);
      if (prNumber == null) {
        return { output: `Error: invalid pr "${rawPr}". Use a PR URL (https://<host>/owner/repo/pull/N) or "#N".`, isError: true };
      }
    }

    let insideGit = true;
    try {
      _enterWorktreeHooks.exec(['rev-parse', '--is-inside-work-tree'], { cwd: context.cwd, stdio: 'ignore', signal });
    } catch {
      insideGit = false;
    }

    if (!insideGit) {
      // Outside a git repository, VCS-agnostic isolation is still possible when
      // the operator has configured a WorktreeCreate hook (parity with the reference's
      // createWorktreeForSession hook delegation and setup.ts's non-git
      // fallback): the hook performs the actual VCS-specific worktree creation.
      // With no such hook we fail loud and NAME the cwd so the user knows which
      // directory is not a repo and how to enable the escape hatch.
      return createViaWorktreeHook(context, name, existingPath, baseRefProvided, prProvided);
    }

    // Nested-isolation guard: refuse to create a NEW worktree while the current
    // working directory is already inside a worktree (entering an existing one is
    // still allowed). Detected from the cwd path so it is not sensitive to stale
    // global session state.
    if (!existingPath && (context.cwd || '').includes('.ami/worktrees/')) {
      return { output: 'Error: already inside a worktree. Use exit_worktree before creating another.', isError: true };
    }

    if (existingPath) {
      if (!fs.existsSync(existingPath)) {
        return { output: `Error: path "${existingPath}" does not exist.`, isError: true };
      }
      try {
        // Bound the captured output with a generous maxBuffer (parity with the reference's
        // execFileNoThrow maxBuffer): a repo with very many registered worktrees would
        // otherwise exceed Node's 1 MiB default and throw an opaque ENOBUFS mid-capture.
        const list = _enterWorktreeHooks.exec(['worktree', 'list', '--porcelain'], { cwd: context.cwd, timeout: 5000, signal, maxBuffer: DEFAULT_GIT_MAX_BUFFER });
        // NFC-normalize parsed worktree paths before the identity comparison.
        const worktrees = list.split('\n').filter(l => l.startsWith('worktree ')).map(l => l.slice(9).normalize('NFC'));
        const resolved = path.resolve(existingPath).normalize('NFC');
        const entered = worktrees.find(w => path.resolve(w).normalize('NFC') === resolved);
        if (!entered) {
          // Affordance (parity with "invalid input shows valid options"):
          // enumerate the repo's registered worktrees so the user can correct
          // the path. Uses the shared, sorted (current-first), NFC-normalized
          // listWorktrees enumeration. Best-effort — never blocks the error.
          let avail = '';
          try {
            const { listWorktrees } = require('../worktree-manager');
            const all = listWorktrees(context.cwd) as Array<{ path: string; branch?: string }>;
            if (all.length > 0) {
              avail = '\nAvailable worktrees:\n' +
                all.map(w => `  - ${w.path}${w.branch ? ` (${w.branch})` : ''}`).join('\n');
            }
          } catch { /* affordance is best-effort */ }
          return { output: `Error: "${existingPath}" is not a registered worktree.${avail}`, isError: true };
        }
      } catch (e) {
        // A buffer overflow (ENOBUFS) despite the generous cap means a pathological
        // number of worktrees; surface it as an actionable message rather than the
        // opaque "maxBuffer length exceeded" so the user knows to prune worktrees.
        if ((e as NodeJS.ErrnoException)?.code === 'ENOBUFS') {
          return { output: 'Error: the repository has too many registered worktrees to enumerate; prune stale worktrees (git worktree prune) and retry.', isError: true };
        }
        return { output: `Error listing worktrees: ${errorMessage(e)}`, isError: true };
      }
      // Name the branch the entered worktree is on (parity with the reference's
      // "on branch <branch>" worktree messages). The worktree's live HEAD is the
      // ground truth; best-effort — a detached HEAD or a rev-parse failure simply
      // omits the branch label rather than failing the entry.
      let branchLabel = '';
      let enteredBranch: string | undefined;
      try {
        const head = _enterWorktreeHooks.exec(['rev-parse', '--abbrev-ref', 'HEAD'], { cwd: existingPath, stdio: 'pipe', signal });
        if (head && head !== 'HEAD') { branchLabel = ` on branch ${head}`; enteredBranch = head; }
      } catch { /* detached / unreadable — omit the branch label */ }
      const enteredMessage = `Entered existing worktree at: ${existingPath}${branchLabel}. Run exit_worktree when done.`;
      return {
        output: enteredMessage,
        metadata: {
          worktreePath: path.resolve(existingPath),
          ...(enteredBranch ? { worktreeBranch: enteredBranch } : {}),
          message: enteredMessage,
        },
      };
    }

    // Resolve the canonical MAIN-repo root once, up front: all worktree creation
    // (the worktrees base, the new worktree dir, and every git invocation below)
    // operates against this root so a worktree created from a SUBDIRECTORY of the
    // repo still lands in the repo root's .ami/worktrees/ (parity with the reference's
    // findCanonicalGitRoot). A nested-worktree cwd was already rejected by the
    // guard above, so this only redirects the subdirectory case; when cwd is
    // already the repo root, repoRoot === context.cwd (no behavioral change).
    const repoRoot = resolveMainRepoRoot(context.cwd, signal);

    // Descriptive auto-name (parity with the reference's teleport generateTitleAndBranch):
    // when the model supplies a task `description` (and no explicit name/pr) and a
    // provider is available, derive a readable kebab-case slug from it instead of a
    // random word slug. Any failure (no provider, LLM/parse error) is swallowed so
    // creation still proceeds with the random word-slug default below.
    let derivedName: string | undefined;
    if (descriptionProvided && prNumber == null && context._providerConfig) {
      try {
        const candidate = await _enterWorktreeHooks.branchNameFromDescription(
          rawDescription,
          context._providerConfig,
          context.abortSignal,
        );
        if (candidate && validateSlug(candidate)) derivedName = candidate;
      } catch { /* fall back to the random word slug below */ }
    }

    // Default name: an explicit name wins; otherwise a description-derived slug, then
    // a PR reference names the worktree `pr-<N>` (parity with the reference), falling back to
    // a memorable word slug.
    const slug = name || derivedName || (prNumber != null ? `pr-${prNumber}` : defaultWorktreeName(path.join(repoRoot, '.ami', 'worktrees')));
    if (!validateSlug(slug)) {
      return { output: `Error: invalid worktree name "${slug}". Use letters, digits, dots, underscores, dashes (max 64 chars per segment).`, isError: true };
    }

    const { validateWorktreeSlug, flattenSlug } = require('../worktree-manager');
    const validationError = validateWorktreeSlug(slug);
    if (validationError) return { output: `Error: ${validationError}`, isError: true };

    // Flatten any '/' in the (validated) slug to '+' BEFORE it is used for the
    // on-disk directory or the branch ref. A slug like "user/feature" would
    // otherwise (a) nest the worktree dir at `.ami/worktrees/user/feature`, which
    // lives INSIDE the `user` worktree so `git worktree remove`/cleanup on the
    // parent silently deletes the child's uncommitted work, and (b) create a
    // directory-form ref `worktree/user/feature` that collides (D/F conflict)
    // with the file-form ref `worktree/user` git refuses to hold simultaneously.
    // Flattening both path and branch to a single `user+feature` segment removes
    // both hazards (parity with the reference's worktreePathFor/flattenSlug). The shared
    // flattenSlug helper is reused so there is one canonical flattening.
    const flatSlug: string = flattenSlug(slug);

    const worktreesBase = path.join(repoRoot, '.ami', 'worktrees');
    const worktreeDir = path.join(worktreesBase, flatSlug);

    // Fast-resume (parity with the reference's getOrCreateWorktree existed-path): a named
    // worktree reuses the same on-disk path across invocations, so when the computed
    // dir ALREADY exists and is a valid registered worktree, re-establish its session
    // and switch into it instead of hard-erroring — enter_worktree is idempotent by
    // name. The HEAD SHA is read DIRECTLY off the `.git` pointer (no subprocess, no
    // fetch, no `git worktree add`), skipping ~15-55ms of spawn overhead. This runs
    // BEFORE the stale-sweep and the MAX_WORKTREES ceiling so resuming an existing
    // worktree is never reaped or blocked by the cap.
    if (fs.existsSync(worktreeDir)) {
      const existingHead = readWorktreeHeadSha(worktreeDir);
      if (existingHead) {
        return resumeExistingWorktree(context, repoRoot, worktreeDir, flatSlug, existingHead);
      }
      // The dir exists but is NOT a resumable worktree (never registered, or its
      // `.git` pointer/HEAD is corrupt) — fail loud rather than resuming debris.
      return { output: `Error: "${flatSlug}" exists at ${worktreeDir} but is not a valid git worktree (stale or corrupt). Remove it (or use exit_worktree) before recreating.`, isError: true };
    }

    // Opportunistically clean up stale worktrees (> 30 days, no uncommitted changes)
    if (fs.existsSync(worktreesBase)) {
      const { cleanupStaleWorktrees } = require('../worktree-manager');
      try { cleanupStaleWorktrees(repoRoot); } catch { /* non-critical */ }
    }

    // Enforce the concurrent-worktree ceiling (wires the exported MAX_WORKTREES
    // cap into the creation path) AFTER stale cleanup, so reaped worktrees free
    // up headroom. Refusing here keeps agent worktrees/branches from
    // proliferating without bound and exhausting disk. The count runs over the
    // managed .ami/worktrees area only; if it cannot be read, creation proceeds
    // (fail-open — the cap is a guard rail, not a gate).
    try {
      if (fs.existsSync(worktreesBase)) {
        const { MAX_WORKTREES } = require('../worktree-manager');
        const existingCount = fs.readdirSync(worktreesBase, { withFileTypes: true }).filter(e => e.isDirectory()).length;
        if (existingCount >= MAX_WORKTREES) {
          return {
            output: `Error: worktree limit reached (${existingCount}/${MAX_WORKTREES}). Remove one with exit_worktree (action:"remove") before creating another.`,
            isError: true,
          };
        }
      }
    } catch { /* unreadable worktrees dir — do not block creation */ }

    const defaultBranch = getDefaultBranch(repoRoot, signal);
    const branchName = `worktree/${flatSlug}`;

    const requestedBaseRef = typeof input.base_ref === 'string' ? input.base_ref.trim() : '';
    if (requestedBaseRef) {
      // Explicit base ref: verify it resolves to a commit, else fail loud
      // (do not silently fall back to HEAD and branch from the wrong place).
      try {
        _enterWorktreeHooks.exec(['rev-parse', '--verify', `${requestedBaseRef}^{commit}`], { cwd: repoRoot, stdio: 'ignore', signal });
      } catch {
        // A shallow clone has a truncated history, so a legitimate ref can be
        // genuinely absent locally. Surface that as the likely cause with the
        // concrete recovery command instead of a bare "invalid ref".
        const shallowHint = isShallowClone(repoRoot)
          ? ' This repository is a shallow clone (truncated history), so a valid ref may not be present locally — run `git fetch --unshallow` (or `git fetch --depth=<n>`) to retrieve it.'
          : '';
        return { output: `Error: base_ref "${requestedBaseRef}" is not a valid git ref (branch/tag/commit).${shallowHint}`, isError: true };
      }
    }
    // PR base (parity with the reference's getOrCreateWorktree({prNumber})): fetch the PR
    // head from origin and base the new worktree on FETCH_HEAD. The fetch is the
    // only network step; its failure is surfaced with the same actionable wording
    // the reference uses (the PR may not exist, or there may be no "origin" remote).
    let prBaseRef: string | undefined;
    if (prNumber != null) {
      try {
        _enterWorktreeHooks.exec(['fetch', 'origin', `pull/${prNumber}/head`], { cwd: repoRoot, stdio: 'ignore', signal, timeout: 60000 });
      } catch {
        return { output: `Error: failed to fetch PR #${prNumber}: the PR may not exist or the repository may not have a remote named "origin".`, isError: true };
      }
      prBaseRef = 'FETCH_HEAD';
    }

    // Default base (no explicit base_ref / PR): prefer the current branch's upstream
    // tracking ref (`@{u}`), then origin/HEAD, then the conventional candidate
    // list (findRemoteBase, all local refs — never contacts the network).
    // This matches git's own base-selection priority so a worktree branches from
    // the ref the user is actually tracking rather than a hardcoded origin/main.
    let baseRef: string;
    if (prBaseRef) {
      baseRef = prBaseRef;
    } else if (requestedBaseRef) {
      baseRef = requestedBaseRef;
    } else {
      const localBase = findRemoteBase(repoRoot);
      if (localBase) {
        baseRef = localBase;
      } else {
        // No remote-tracking ref resolves locally — a freshly/stalely cloned repo
        // whose `origin/<default>` isn't present. Read the ref off the filesystem
        // first (no subprocess); only on a MISS fetch `origin/<default>` once so
        // the worktree can still be based on the latest default branch instead of
        // silently basing on HEAD (parity: worktree.ts L283-300 —
        // resolveRef-then-`git fetch origin <default>`). An offline/remoteless
        // fetch failure is swallowed; the rev-parse+HEAD fallback below still fires.
        if (!resolveRef(repoRoot, `refs/remotes/origin/${defaultBranch}`)) {
          try {
            _enterWorktreeHooks.exec(['fetch', 'origin', defaultBranch], { cwd: repoRoot, stdio: 'ignore', signal, timeout: 60000 });
          } catch { /* offline / no origin — HEAD fallback below applies */ }
        }
        baseRef = `origin/${defaultBranch}`;
      }
    }
    // Opt-in sparse-checkout (parity: the reference's getOrCreateWorktree sparsePaths path,
    // worktree.ts L321-366). When configured, the worktree is added with
    // `--no-checkout` so NO files land yet; the cone is then narrowed and HEAD
    // checked out below, so only the listed subtrees materialise.
    const sparsePaths = resolveSparsePaths(repoRoot);
    const addArgs = sparsePaths ? ['worktree', 'add', '--no-checkout'] : ['worktree', 'add'];
    // Re-check cancellation right before the worktree-add — the on-disk creation
    // step. A synchronous execFileSync cannot be interrupted mid-call by the
    // signal (the event loop is blocked for its duration), so an explicit check
    // here is what actually honors a turn cancelled during the preceding
    // base-resolution / fetch steps: we refuse to materialise a worktree rather
    // than pressing on after the user cancelled.
    if (signal?.aborted) {
      return { output: 'Error: operation aborted; worktree not created.', isError: true };
    }
    try {
      try {
        _enterWorktreeHooks.exec(['rev-parse', '--verify', baseRef], { cwd: repoRoot, stdio: 'ignore', signal });
        _enterWorktreeHooks.exec([...addArgs, '-B', branchName, worktreeDir, baseRef], { cwd: repoRoot, stdio: 'ignore', signal });
      } catch {
        if (requestedBaseRef) throw new Error(`could not create worktree from base_ref "${requestedBaseRef}"`);
        _enterWorktreeHooks.exec([...addArgs, '-B', branchName, worktreeDir, 'HEAD'], { cwd: repoRoot, stdio: 'ignore', signal });
      }
    } catch (e) {
      return { output: `Error creating worktree: ${errorMessage(e)}`, isError: true };
    }

    // Narrow the cone + check out HEAD (parity: worktree.ts L336-366). If EITHER
    // step fails after `--no-checkout`, the worktree is registered with an empty
    // working tree; a later fast-resume (rev-parse HEAD) would succeed and present
    // that broken worktree as "resumed". Tear it down (remove --force + drop the
    // branch, matching the post-setup teardown below) before surfacing the error.
    if (sparsePaths) {
      try {
        _enterWorktreeHooks.exec(['sparse-checkout', 'set', '--cone', '--', ...sparsePaths], { cwd: worktreeDir, stdio: 'ignore', signal });
        _enterWorktreeHooks.exec(['checkout', 'HEAD'], { cwd: worktreeDir, stdio: 'ignore', signal });
      } catch (e) {
        try { _enterWorktreeHooks.exec(['worktree', 'remove', '--force', worktreeDir], { cwd: repoRoot, stdio: 'ignore' }); } catch { /* best-effort cleanup */ }
        try { _enterWorktreeHooks.exec(['branch', '-D', branchName], { cwd: repoRoot, stdio: 'ignore' }); } catch { /* best-effort cleanup */ }
        return { output: `Error: sparse-checkout setup failed, rolled back: ${errorMessage(e)}`, isError: true };
      }
    }

    let commitHash: string;
    try {
      commitHash = _enterWorktreeHooks.exec(['rev-parse', '--short', 'HEAD'], { cwd: worktreeDir, signal });
    } catch {
      commitHash = 'unknown';
    }

    const { createWorktreeSession, setWorktreeSession } = require('../worktree-manager');
    // Pass the FLATTENED slug so the session's computed worktree dir matches the
    // one just created on disk (otherwise a '/'-containing slug would make
    // createWorktreeSession look for the raw nested path, miss it, and add a
    // second worktree).
    let session;
    let symlinkWarnings: string[] = [];
    try {
      // Thread the turn's abort signal into worktree creation so a cancelled turn
      // kills the in-flight git worktree-add instead of blocking out the timeout.
      session = createWorktreeSession(repoRoot, flatSlug, { signal });
      // Record whether this worktree is a partial (sparse) checkout so a
      // resume/introspection path can tell an intentional partial tree from a
      // broken empty one (parity: WorktreeSession.usedSparsePaths).
      session.usedSparsePaths = !!sparsePaths;
      setWorktreeSession(session);
      // Honor the opt-in settings.worktree.symlinkDirectories knob; undefined keeps
      // AMI's default of symlinking node_modules. Surface any UNEXPECTED symlink
      // failure (EACCES/EPERM/ENOSYS) rather than letting it fail silently.
      symlinkWarnings = _enterWorktreeHooks.symlinkLargeDirs(repoRoot, session.worktreePath, resolveSymlinkDirs(repoRoot));
    } catch (e) {
      // Post-add setup failed: `git worktree add` already registered the worktree
      // on disk and reset the branch, but the session/symlink wiring is broken.
      // Tear the half-created worktree down (parity with the reference's tearDown in
      // createWorktree — remove --force then drop the branch) so a broken
      // worktree is never left behind to be mistaken for a healthy one on the
      // next fast-resume. Both teardown steps are best-effort; the clearing of
      // any partially-set session state is not.
      try { _enterWorktreeHooks.exec(['worktree', 'remove', '--force', worktreeDir], { cwd: repoRoot, stdio: 'ignore' }); } catch { /* best-effort cleanup */ }
      try { _enterWorktreeHooks.exec(['branch', '-D', branchName], { cwd: repoRoot, stdio: 'ignore' }); } catch { /* best-effort cleanup */ }
      setWorktreeSession(null);
      return { output: `Error: worktree post-setup failed, rolled back: ${errorMessage(e)}`, isError: true };
    }

    // Capture the pre-reassignment cwd so the cwd-changed hook reports the
    // correct oldCwd whether or not context.cwd was set (equivalent to the old
    // `_originalCwd ?? context.cwd`, but without an unreachable fallback arm).
    const originalCwd = context.cwd;
    if (context.cwd) {
      (context as any)._originalCwd = context.cwd;
      (context as any).cwd = worktreeDir;
    }

    if (context._hookManager) {
      context._hookManager.executeWorktreeCreate?.({ name: slug }).catch(swallowHookRejection);
      context._hookManager.executeCwdChanged?.({ oldCwd: originalCwd, newCwd: worktreeDir }).catch(swallowHookRejection);
    }

    // Opt-in tmux session (parity: the reference's `--tmux` worktree mode, setup.ts
    // L220-269). When settings.worktree.tmux is enabled, spawn a DETACHED tmux
    // session rooted at the worktree and surface the copy-paste reattach command.
    // A failure (tmux not installed, spawn error) is NON-fatal: the worktree is
    // already created, so we only append a yellow-equivalent warning line. The
    // session name is recorded on the WorktreeSession so exit_worktree can tear it
    // down on remove / remind how to reattach on keep.
    let tmuxLine = '';
    if (resolveWorktreeTmux(repoRoot)) {
      const { generateTmuxSessionName, createTmuxSessionForWorktree } = require('../worktree-manager');
      const tmuxName: string = generateTmuxSessionName(repoRoot, branchName);
      const tmuxResult = createTmuxSessionForWorktree(tmuxName, worktreeDir) as { created: boolean; error?: string };
      if (tmuxResult.created) {
        session.tmuxSessionName = tmuxName;
        tmuxLine = `\n  Created tmux session: ${tmuxName}\n  To attach: tmux attach -t ${tmuxName}`;
      } else {
        tmuxLine = `\n  Warning: Failed to create tmux session: ${tmuxResult.error || 'unknown error'}`;
      }
    }

    const baseLabel = prNumber != null ? `PR #${prNumber}` : baseRef;
    // Stale-clone diagnostic: when the worktree was based on the default remote
    // ref (no explicit base_ref / PR) and the clone has not fetched recently, warn
    // that the base may be out of date (parity: the reference's FETCH_HEAD staleness
    // signal). Best-effort — a git failure simply omits the hint.
    let staleHint = '';
    try {
      if (!requestedBaseRef && !prBaseRef) {
        if (_enterWorktreeHooks.isCloneStale(repoRoot)) {
          staleHint = `\n  Note: this clone has not fetched in over 7 days — ${baseLabel} may be out of date (run \`git fetch\` to refresh).`;
        }
      }
    } catch { /* best-effort diagnostic */ }
    const warnLines = symlinkWarnings.length
      ? `\n  Warning: ${symlinkWarnings.join('; ')}.`
      : '';
    const createdMessage =
      `Worktree created (session switched to it):\n` +
      `  Path: ${worktreeDir}\n  Branch: ${branchName} (based on ${baseLabel})\n  Base commit: ${commitHash}\n` +
      '  Leave with exit_worktree (action:"keep" to preserve, "remove" to delete).' +
      staleHint + warnLines + tmuxLine;
    return {
      output: createdMessage,
      // Structured fields (parity: the reference's outputSchema result) for programmatic
      // consumers; mirrors the text above.
      metadata: {
        worktreePath: worktreeDir,
        worktreeBranch: branchName,
        ...(session.tmuxSessionName ? { tmuxSessionName: session.tmuxSessionName } : {}),
        message: createdMessage,
      },
    };
  },
};
