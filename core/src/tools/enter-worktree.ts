import * as fs from 'fs';
import * as path from 'path';
import * as child_process from 'child_process';
import * as crypto from 'crypto';
import { ToolContext, ToolDefinition, ToolResult } from '../types';
import { findRemoteBase, isShallowClone } from '../utils/git';

const SLUG_SEGMENT_RE = /^[a-zA-Z0-9._-]+$/;

/**
 * Human-readable default worktree name (parity with Claude's EnterWorktreeTool
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
const ALLOWED_KEYS = new Set(['name', 'path', 'base_ref', 'pr']);

/**
 * Run git with NO SHELL (execFile with an argv array) so a branch name, path, or
 * ref can never be interpreted as a shell metacharacter (command-injection
 * immune — parity with worktree-manager's gitExec and Claude's
 * execFileNoThrowWithCwd). Returns trimmed stdout.
 */
function runGit(
  args: string[],
  opts: { cwd: string; stdio?: 'ignore' | 'pipe'; timeout?: number; env?: NodeJS.ProcessEnv } = { cwd: process.cwd() },
): string {
  const out = child_process.execFileSync('git', args, {
    cwd: opts.cwd,
    encoding: 'utf-8',
    stdio: opts.stdio === 'ignore' ? 'ignore' : ['ignore', 'pipe', 'pipe'],
    timeout: opts.timeout,
    env: opts.env,
  });
  return typeof out === 'string' ? out.trim() : '';
}

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
  /** Human-readable default-name generator (parity with Claude's getPlanSlug
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
  symlinkLargeDirs: (sourceCwd: string, worktreePath: string, dirs?: string[]): void =>
    require('../worktree-manager').symlinkLargeDirectories(sourceCwd, worktreePath, dirs),
};

/**
 * Resolve the opt-in `settings.worktree.symlinkDirectories` list (parity with
 * Claude's `getInitialSettings().worktree?.symlinkDirectories`). Project config
 * (`<cwd>/.ami/config.json`) takes precedence over the global config
 * (`~/.ami/config.json`); either may configure the list. Returns `undefined` when
 * NEITHER tier configures the key, so the caller keeps AMI's default of symlinking
 * `node_modules`. An explicitly-configured list (including an empty array → symlink
 * nothing) is returned verbatim. Config-read failures fall back to the default.
 */
function resolveSymlinkDirs(cwd: string): string[] | undefined {
  try {
    const { loadProjectConfig, loadGlobalConfig } = require('../config');
    const proj = loadProjectConfig(cwd)?.worktree?.symlinkDirectories;
    if (Array.isArray(proj)) return proj;
    const glob = loadGlobalConfig()?.worktree?.symlinkDirectories;
    if (Array.isArray(glob)) return glob;
  } catch { /* unreadable config → fall back to the default */ }
  return undefined;
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

function getDefaultBranch(cwd: string): string {
  // Offline-friendly, NO shell pipe: read the origin/HEAD symref directly
  // instead of `git remote show origin | grep` (a network + shell-pipe call).
  try {
    const sym = _enterWorktreeHooks.exec(['symbolic-ref', '--short', 'refs/remotes/origin/HEAD'], { cwd });
    const match = sym.match(/^origin\/(.+)$/);
    if (match) return match[1];
  } catch { /* fallback */ }
  // Offline candidate probe: check the conventional default branches in order
  // and return the first that actually exists as a local ref. Only when NONE
  // exist do we fall back to 'main' (parity with Claude's computeDefaultBranch,
  // whose final fallback is 'main' — never a 'master' guess for a ref that is
  // not present).
  for (const candidate of ['main', 'master']) {
    try {
      _enterWorktreeHooks.exec(['rev-parse', '--verify', `refs/heads/${candidate}`], { cwd, stdio: 'ignore' });
      return candidate;
    } catch { /* try next candidate */ }
  }
  return 'main';
}

/**
 * Non-git fallback: create (or enter) a worktree by delegating to a configured
 * WorktreeCreate hook so worktree isolation works with VCS systems other than
 * git (parity with Claude's hook-based createWorktreeForSession / setup.ts
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
  // hook-based worktree (parity: Claude's PR path is git-only via `git fetch origin pull/N/head`).
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
    return { output: `Entered existing worktree at: ${existingPath}. Run exit_worktree when done.` };
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

  try {
    await context._hookManager.executeWorktreeCreate({ name: slug, worktreePath: worktreeDir });
  } catch (e) {
    return { output: `Error: WorktreeCreate hook failed: ${e instanceof Error ? e.message : String(e)}`, isError: true };
  }
  // The hook owns creation; if it did not materialise the directory, fail loud
  // rather than switching the session into a path that does not exist.
  if (!fs.existsSync(worktreeDir)) {
    return { output: `Error: the WorktreeCreate hook did not create ${worktreeDir}.`, isError: true };
  }

  switchHookSession(context, cwd, worktreeDir, flatSlug);
  return {
    output:
      `Worktree created via WorktreeCreate hook (session switched to it):\n` +
      `  Path: ${worktreeDir}\n` +
      '  Leave with exit_worktree (action:"keep" to preserve, "remove" to delete).',
  };
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
  context._hookManager?.executeCwdChanged?.({ oldCwd: originalCwd, newCwd: worktreeDir }).catch(() => {});
}

export const enterWorktreeTool: ToolDefinition = {
  name: 'enter_worktree',
  description:
    'Create or enter a git worktree for isolated file modifications, on a new branch under .ami/worktrees/. ' +
    'WHEN TO USE: isolating risky or parallel changes from the main working tree (a large refactor you may want to throw away, work you want on its own branch). ' +
    'WHEN NOT TO USE: ordinary edits in the current tree, or when the user did not ask for isolation — a worktree adds branch/cwd bookkeeping overhead. ' +
    'Use exit_worktree (action "keep" or "remove") to leave. ' +
    'Requires a git repository; outside one, configure WorktreeCreate/WorktreeRemove hooks for VCS-agnostic isolation. ' +
    'Pass pr (a PR URL or #N) to base the worktree on a pull request.',
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
    },
  },
  isReadOnly: false,

  async execute(input, context): Promise<ToolResult> {
    // Strict input: reject unknown keys up front (parity with exit_worktree) so
    // a mistyped parameter can never be silently discarded.
    const unknown = Object.keys(input).filter(k => !ALLOWED_KEYS.has(k));
    if (unknown.length > 0) {
      return { output: `Error: unknown parameter(s): ${unknown.join(', ')}.`, isError: true };
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

    if (name && existingPath) {
      return { output: 'Error: name and path are mutually exclusive.', isError: true };
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
    // work (parity: Claude's parsePRReference accepts a PR URL or "#N").
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
      _enterWorktreeHooks.exec(['rev-parse', '--is-inside-work-tree'], { cwd: context.cwd, stdio: 'ignore' });
    } catch {
      insideGit = false;
    }

    if (!insideGit) {
      // Outside a git repository, VCS-agnostic isolation is still possible when
      // the operator has configured a WorktreeCreate hook (parity with Claude's
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
        const list = _enterWorktreeHooks.exec(['worktree', 'list', '--porcelain'], { cwd: context.cwd, timeout: 5000 });
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
        return { output: `Error listing worktrees: ${e instanceof Error ? e.message : String(e)}`, isError: true };
      }
      // Name the branch the entered worktree is on (parity with Claude's
      // "on branch <branch>" worktree messages). The worktree's live HEAD is the
      // ground truth; best-effort — a detached HEAD or a rev-parse failure simply
      // omits the branch label rather than failing the entry.
      let branchLabel = '';
      try {
        const head = _enterWorktreeHooks.exec(['rev-parse', '--abbrev-ref', 'HEAD'], { cwd: existingPath, stdio: 'pipe' });
        if (head && head !== 'HEAD') branchLabel = ` on branch ${head}`;
      } catch { /* detached / unreadable — omit the branch label */ }
      return {
        output: `Entered existing worktree at: ${existingPath}${branchLabel}. Run exit_worktree when done.`,
      };
    }

    // Default name: an explicit name wins; otherwise a PR reference names the
    // worktree `pr-<N>` (parity with Claude), falling back to a memorable word slug.
    const slug = name || (prNumber != null ? `pr-${prNumber}` : defaultWorktreeName(path.join(context.cwd, '.ami', 'worktrees')));
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
    // both hazards (parity with Claude's worktreePathFor/flattenSlug). The shared
    // flattenSlug helper is reused so there is one canonical flattening.
    const flatSlug: string = flattenSlug(slug);

    // Opportunistically clean up stale worktrees (> 30 days, no uncommitted changes)
    const worktreesBase = path.join(context.cwd, '.ami', 'worktrees');
    if (fs.existsSync(worktreesBase)) {
      const { cleanupStaleWorktrees } = require('../worktree-manager');
      try { cleanupStaleWorktrees(context.cwd); } catch { /* non-critical */ }
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

    const worktreeDir = path.join(context.cwd, '.ami', 'worktrees', flatSlug);
    if (fs.existsSync(worktreeDir)) {
      return { output: `Error: worktree "${flatSlug}" already exists at ${worktreeDir}. Use path to enter an existing worktree.`, isError: true };
    }

    const defaultBranch = getDefaultBranch(context.cwd);
    const branchName = `worktree/${flatSlug}`;

    const gitEnv = { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '' };

    const requestedBaseRef = typeof input.base_ref === 'string' ? input.base_ref.trim() : '';
    if (requestedBaseRef) {
      // Explicit base ref: verify it resolves to a commit, else fail loud
      // (do not silently fall back to HEAD and branch from the wrong place).
      try {
        _enterWorktreeHooks.exec(['rev-parse', '--verify', `${requestedBaseRef}^{commit}`], { cwd: context.cwd, stdio: 'ignore', env: gitEnv });
      } catch {
        // A shallow clone has a truncated history, so a legitimate ref can be
        // genuinely absent locally. Surface that as the likely cause with the
        // concrete recovery command instead of a bare "invalid ref".
        const shallowHint = isShallowClone(context.cwd)
          ? ' This repository is a shallow clone (truncated history), so a valid ref may not be present locally — run `git fetch --unshallow` (or `git fetch --depth=<n>`) to retrieve it.'
          : '';
        return { output: `Error: base_ref "${requestedBaseRef}" is not a valid git ref (branch/tag/commit).${shallowHint}`, isError: true };
      }
    }
    // PR base (parity with Claude's getOrCreateWorktree({prNumber})): fetch the PR
    // head from origin and base the new worktree on FETCH_HEAD. The fetch is the
    // only network step; its failure is surfaced with the same actionable wording
    // Claude uses (the PR may not exist, or there may be no "origin" remote).
    let prBaseRef: string | undefined;
    if (prNumber != null) {
      try {
        _enterWorktreeHooks.exec(['fetch', 'origin', `pull/${prNumber}/head`], { cwd: context.cwd, stdio: 'ignore', env: gitEnv, timeout: 60000 });
      } catch {
        return { output: `Error: failed to fetch PR #${prNumber}: the PR may not exist or the repository may not have a remote named "origin".`, isError: true };
      }
      prBaseRef = 'FETCH_HEAD';
    }

    // Default base (no explicit base_ref / PR): prefer the current branch's upstream
    // tracking ref (`@{u}`), then origin/HEAD, then the conventional candidate
    // list (findRemoteBase, all local refs — never contacts the network),
    // falling back to origin/<default-branch> only when none of those resolve.
    // This matches git's own base-selection priority so a worktree branches from
    // the ref the user is actually tracking rather than a hardcoded origin/main.
    const baseRef = prBaseRef || requestedBaseRef || findRemoteBase(context.cwd) || `origin/${defaultBranch}`;
    try {
      try {
        _enterWorktreeHooks.exec(['rev-parse', '--verify', baseRef], { cwd: context.cwd, stdio: 'ignore', env: gitEnv });
        _enterWorktreeHooks.exec(['worktree', 'add', '-B', branchName, worktreeDir, baseRef], { cwd: context.cwd, stdio: 'ignore', env: gitEnv });
      } catch {
        if (requestedBaseRef) throw new Error(`could not create worktree from base_ref "${requestedBaseRef}"`);
        _enterWorktreeHooks.exec(['worktree', 'add', '-B', branchName, worktreeDir, 'HEAD'], { cwd: context.cwd, stdio: 'ignore', env: gitEnv });
      }
    } catch (e) {
      return { output: `Error creating worktree: ${e instanceof Error ? e.message : String(e)}`, isError: true };
    }

    let commitHash: string;
    try {
      commitHash = _enterWorktreeHooks.exec(['rev-parse', '--short', 'HEAD'], { cwd: worktreeDir });
    } catch {
      commitHash = 'unknown';
    }

    const { createWorktreeSession, setWorktreeSession } = require('../worktree-manager');
    // Pass the FLATTENED slug so the session's computed worktree dir matches the
    // one just created on disk (otherwise a '/'-containing slug would make
    // createWorktreeSession look for the raw nested path, miss it, and add a
    // second worktree).
    let session;
    try {
      session = createWorktreeSession(context.cwd, flatSlug);
      setWorktreeSession(session);
      // Honor the opt-in settings.worktree.symlinkDirectories knob; undefined keeps
      // AMI's default of symlinking node_modules.
      _enterWorktreeHooks.symlinkLargeDirs(context.cwd, session.worktreePath, resolveSymlinkDirs(context.cwd));
    } catch (e) {
      // Post-add setup failed: `git worktree add` already registered the worktree
      // on disk and reset the branch, but the session/symlink wiring is broken.
      // Tear the half-created worktree down (parity with Claude's tearDown in
      // createWorktree — remove --force then drop the branch) so a broken
      // worktree is never left behind to be mistaken for a healthy one on the
      // next fast-resume. Both teardown steps are best-effort; the clearing of
      // any partially-set session state is not.
      try { _enterWorktreeHooks.exec(['worktree', 'remove', '--force', worktreeDir], { cwd: context.cwd, stdio: 'ignore', env: gitEnv }); } catch { /* best-effort cleanup */ }
      try { _enterWorktreeHooks.exec(['branch', '-D', branchName], { cwd: context.cwd, stdio: 'ignore', env: gitEnv }); } catch { /* best-effort cleanup */ }
      setWorktreeSession(null);
      return { output: `Error: worktree post-setup failed, rolled back: ${e instanceof Error ? e.message : String(e)}`, isError: true };
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
      context._hookManager.executeWorktreeCreate?.({ name: slug }).catch(() => {});
      context._hookManager.executeCwdChanged?.({ oldCwd: originalCwd, newCwd: worktreeDir }).catch(() => {});
    }

    const baseLabel = prNumber != null ? `PR #${prNumber}` : baseRef;
    return {
      output:
        `Worktree created (session switched to it):\n` +
        `  Path: ${worktreeDir}\n  Branch: ${branchName} (based on ${baseLabel})\n  Base commit: ${commitHash}\n` +
        '  Leave with exit_worktree (action:"keep" to preserve, "remove" to delete).',
    };
  },
};
