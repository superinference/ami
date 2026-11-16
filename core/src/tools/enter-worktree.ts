import * as fs from 'fs';
import * as path from 'path';
import * as child_process from 'child_process';
import * as crypto from 'crypto';
import { ToolDefinition, ToolResult } from '../types';

const SLUG_SEGMENT_RE = /^[a-zA-Z0-9._-]+$/;

// Strict-input allow-list (parity with exit_worktree's ALLOWED_KEYS): any key
// outside this set is rejected rather than silently ignored, so a typo'd or
// hallucinated parameter surfaces loudly instead of being dropped.
const ALLOWED_KEYS = new Set(['name', 'path', 'base_ref']);

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
};

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

export const enterWorktreeTool: ToolDefinition = {
  name: 'enter_worktree',
  description:
    'Create or enter a git worktree for isolated file modifications, on a new branch under .ami/worktrees/. ' +
    'WHEN TO USE: isolating risky or parallel changes from the main working tree (a large refactor you may want to throw away, work you want on its own branch). ' +
    'WHEN NOT TO USE: ordinary edits in the current tree, or when the user did not ask for isolation — a worktree adds branch/cwd bookkeeping overhead. ' +
    'Use exit_worktree (action "keep" or "remove") to leave. ' +
    'Requires a git repository; outside one, configure WorktreeCreate/WorktreeRemove hooks for VCS-agnostic isolation.',
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

    if (name && existingPath) {
      return { output: 'Error: name and path are mutually exclusive.', isError: true };
    }

    // base_ref only has meaning when CREATING a new worktree; entering an
    // existing one checks out whatever that worktree already has. Reject the
    // combination instead of silently ignoring base_ref.
    if (existingPath && baseRefProvided) {
      return { output: 'Error: base_ref cannot be combined with path (base_ref only applies when creating a new worktree).', isError: true };
    }

    try {
      _enterWorktreeHooks.exec(['rev-parse', '--is-inside-work-tree'], { cwd: context.cwd, stdio: 'ignore' });
    } catch {
      return {
        output: 'Error: not inside a git repository. To use worktree isolation with another VCS, configure WorktreeCreate/WorktreeRemove hooks in settings.',
        isError: true,
      };
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

    const slug = name || `wt-${crypto.randomBytes(4).toString('hex')}`;
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
        return { output: `Error: base_ref "${requestedBaseRef}" is not a valid git ref (branch/tag/commit).`, isError: true };
      }
    }
    try {
      const baseRef = requestedBaseRef || `origin/${defaultBranch}`;
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

    const { createWorktreeSession, setWorktreeSession, symlinkLargeDirectories } = require('../worktree-manager');
    // Pass the FLATTENED slug so the session's computed worktree dir matches the
    // one just created on disk (otherwise a '/'-containing slug would make
    // createWorktreeSession look for the raw nested path, miss it, and add a
    // second worktree).
    const session = createWorktreeSession(context.cwd, flatSlug);
    setWorktreeSession(session);
    symlinkLargeDirectories(context.cwd, session.worktreePath);

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

    const baseLabel = requestedBaseRef || `origin/${defaultBranch}`;
    return {
      output:
        `Worktree created (session switched to it):\n` +
        `  Path: ${worktreeDir}\n  Branch: ${branchName} (based on ${baseLabel})\n  Base commit: ${commitHash}\n` +
        '  Leave with exit_worktree (action:"keep" to preserve, "remove" to delete).',
    };
  },
};
