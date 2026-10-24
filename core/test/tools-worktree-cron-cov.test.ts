import { describe, it, before, after, beforeEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as child_process from 'child_process';

import { enterWorktreeTool, _enterWorktreeHooks } from '../src/tools/enter-worktree';
import {
  createWorktreeSession,
  setWorktreeSession,
  getCurrentWorktreeSession,
  countWorktreeChanges,
  cleanupStaleWorktrees,
  symlinkLargeDirectories,
  copyWorktreeIncludes,
  validateWorktreeSlug,
  worktreeBranchName,
  flattenSlug,
  validateWorktreeGitFile,
  findStaleWorktrees,
} from '../src/worktree-manager';
import { cronCreateTool, resetCronScheduler, getCronScheduler } from '../src/tools/cron-create';
import { cronListTool } from '../src/tools/cron-list';
import { getScheduler, resetScheduler } from '../src/cron';
import {
  planModeTool,
  isPlanModeActive,
  setPlanModeActive,
  getPlanFilePath,
} from '../src/tools/plan-mode';
import {
  gitCommitTool,
  buildFinalMessage,
  CO_AUTHOR_TRAILER,
  TRAILER_REGEX,
  _gitCommitFs,
} from '../src/tools/git-commit';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const GIT_ENV = { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '' };

function git(args: string, cwd: string): string {
  return child_process.execSync(`git ${args}`, { cwd, encoding: 'utf-8', env: GIT_ENV }).toString();
}

function mkTmp(prefix = 'wtcov-'): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

/** Create a git repo with an initial commit so HEAD resolves. */
function initRepoWithCommit(dir: string): void {
  git('init -b main', dir);
  git('config user.email "t@t.com"', dir);
  git('config user.name "Test"', dir);
  git('config commit.gpgsign false', dir);
  fs.writeFileSync(path.join(dir, 'README.md'), '# repo\n');
  git('add README.md', dir);
  git('commit -m init', dir);
}

function ctx(overrides?: Partial<ToolContext> & Record<string, unknown>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

const tmpDirs: string[] = [];
function track(dir: string): string { tmpDirs.push(dir); return dir; }

after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
  setWorktreeSession(null);
  resetScheduler();
});

// ---------------------------------------------------------------------------
// enter_worktree
// ---------------------------------------------------------------------------

describe('enterWorktreeTool.execute', () => {
  it('rejects mutually exclusive name + path', async () => {
    const r = await enterWorktreeTool.execute({ name: 'a', path: '/tmp/x' }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /mutually exclusive/);
  });

  it('errors when not inside a git repository', async () => {
    const dir = track(mkTmp());
    const r = await enterWorktreeTool.execute({ name: 'a' }, ctx({ cwd: dir }));
    assert.equal(r.isError, true);
    assert.match(r.output, /not inside a git repository/);
  });

  it('errors when existing path does not exist', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const r = await enterWorktreeTool.execute(
      { path: path.join(repo, 'does-not-exist') },
      ctx({ cwd: repo }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /does not exist/);
  });

  it('errors when existing path is not a registered worktree', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const plain = path.join(repo, 'plain-dir');
    fs.mkdirSync(plain);
    const r = await enterWorktreeTool.execute({ path: plain }, ctx({ cwd: repo }));
    assert.equal(r.isError, true);
    assert.match(r.output, /not a registered worktree/);
  });

  it('enters an existing registered worktree', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const created = await enterWorktreeTool.execute({ name: 'reg1' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined);
    const wtDir = path.join(repo, '.superinference', 'worktrees', 'reg1');
    const r = await enterWorktreeTool.execute({ path: wtDir }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Entered existing worktree/);
  });

  it('rejects an invalid slug (bad characters)', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const r = await enterWorktreeTool.execute({ name: 'bad$name' }, ctx({ cwd: repo }));
    assert.equal(r.isError, true);
    assert.match(r.output, /invalid worktree name/);
  });

  it('rejects slug variants: too long, absolute, empty/dot segments', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const cases = [
      'x'.repeat(65),   // length > 64
      '/abs/path',      // absolute path
      'a//b',           // empty segment
      'a/./b',          // "." segment
      'a/../b',         // ".." segment
    ];
    for (const name of cases) {
      const r = await enterWorktreeTool.execute({ name }, ctx({ cwd: repo }));
      assert.equal(r.isError, true, `expected error for ${JSON.stringify(name)}`);
      assert.match(r.output, /invalid worktree name/);
    }
  });

  it('rejects a slug that fails worktree-manager validation', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    // "-foo" passes the tool's local validateSlug but fails validateWorktreeSlug
    // (must start with an alphanumeric).
    const r = await enterWorktreeTool.execute({ name: '-foo' }, ctx({ cwd: repo }));
    assert.equal(r.isError, true);
    assert.match(r.output, /Error:/);
  });

  it('creates a worktree (default base ref falls back to HEAD) and runs hooks', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const hookCalls: string[] = [];
    const context = ctx({
      cwd: repo,
      _hookManager: {
        executeWorktreeCreate: async () => { hookCalls.push('create'); },
        executeCwdChanged: async () => { hookCalls.push('cwd'); },
      } as any,
    });
    const r = await enterWorktreeTool.execute({ name: 'feat1' }, context);
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Worktree created/);
    assert.match(r.output, /Branch: worktree\/feat1/);
    assert.ok(fs.existsSync(path.join(repo, '.superinference', 'worktrees', 'feat1')));
    // cwd mutated to the worktree dir
    assert.equal((context as any).cwd, path.join(repo, '.superinference', 'worktrees', 'feat1'));
    await new Promise(res => setTimeout(res, 20));
    assert.deepEqual(hookCalls.sort(), ['create', 'cwd']);
  });

  it('creates a worktree with an explicit valid base_ref', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const r = await enterWorktreeTool.execute(
      { name: 'feat2', base_ref: 'HEAD' },
      ctx({ cwd: repo }),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Worktree created/);
  });

  it('rejects an invalid explicit base_ref', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const r = await enterWorktreeTool.execute(
      { name: 'feat3', base_ref: 'no-such-ref-xyz' },
      ctx({ cwd: repo }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /not a valid git ref/);
  });

  it('fails loudly when a valid base_ref cannot produce a worktree', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    // Check out the branch the tool will try to create so `git worktree add -B`
    // fails (branch already checked out in the main worktree).
    git('checkout -b worktree/clash', repo);
    const r = await enterWorktreeTool.execute(
      { name: 'clash', base_ref: 'HEAD' },
      ctx({ cwd: repo }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /Error creating worktree/);
  });

  it('errors when the target worktree directory already exists', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const wtDir = path.join(repo, '.superinference', 'worktrees', 'dup');
    fs.mkdirSync(wtDir, { recursive: true });
    const r = await enterWorktreeTool.execute({ name: 'dup' }, ctx({ cwd: repo }));
    assert.equal(r.isError, true);
    assert.match(r.output, /already exists/);
  });

  it('generates a random slug when name is omitted', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const r = await enterWorktreeTool.execute({}, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Branch: worktree\/wt-[0-9a-f]{8}/);
  });

  it('resolves the default branch from a configured remote', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    // Create a bare remote and push so `git remote show origin` reports a HEAD branch.
    const bare = track(mkTmp('wtcov-bare-'));
    git('init --bare', bare);
    git(`remote add origin ${JSON.stringify(bare)}`, repo);
    git('push -u origin main', repo);
    git('remote set-head origin main', repo);
    const r = await enterWorktreeTool.execute({ name: 'fromremote' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Worktree created/);
  });

  it('ignores unparseable "git remote show origin" output and falls through to main', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const real = child_process.execSync;
    mock.method(_enterWorktreeHooks, 'execSync', (cmd: string, opts: unknown) => {
      if (typeof cmd === 'string' && cmd.includes('git remote show origin')) {
        // Output that does NOT contain a "HEAD branch:" line → regex match is null.
        return 'unrelated output\n' as unknown as ReturnType<typeof child_process.execSync>;
      }
      return (real as any)(cmd, opts);
    });
    try {
      const r = await enterWorktreeTool.execute({ name: 'nomatch' }, ctx({ cwd: repo }));
      assert.equal(r.isError, undefined);
      assert.match(r.output, /Worktree created/);
    } finally {
      mock.restoreAll();
    }
  });

  it('falls back to the master default branch when there is no main or remote', async () => {
    const repo = track(mkTmp());
    git('init -b trunk', repo);
    git('config user.email "t@t.com"', repo);
    git('config user.name "Test"', repo);
    git('config commit.gpgsign false', repo);
    fs.writeFileSync(path.join(repo, 'f.txt'), 'x');
    git('add f.txt', repo);
    git('commit -m init', repo);
    // No "main" branch, no remote → getDefaultBranch returns "master" → origin/master
    // fails → falls back to HEAD.
    const r = await enterWorktreeTool.execute({ name: 'trunkwt' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Worktree created/);
  });

  it('reports an error when "git worktree list" fails', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const plain = path.join(repo, 'plain2');
    fs.mkdirSync(plain);
    const real = child_process.execSync;
    mock.method(_enterWorktreeHooks, 'execSync', (cmd: string, opts: unknown) => {
      if (typeof cmd === 'string' && cmd.includes('worktree list')) {
        throw new Error('simulated worktree list failure');
      }
      return (real as any)(cmd, opts);
    });
    try {
      const r = await enterWorktreeTool.execute({ path: plain }, ctx({ cwd: repo }));
      assert.equal(r.isError, true);
      assert.match(r.output, /Error listing worktrees/);
    } finally {
      mock.restoreAll();
    }
  });

  it('falls back to "unknown" when the base commit cannot be resolved', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const real = child_process.execSync;
    mock.method(_enterWorktreeHooks, 'execSync', (cmd: string, opts: unknown) => {
      if (cmd === 'git rev-parse --short HEAD') {
        throw new Error('simulated rev-parse failure');
      }
      return (real as any)(cmd, opts);
    });
    try {
      const r = await enterWorktreeTool.execute({ name: 'unkbase' }, ctx({ cwd: repo }));
      assert.equal(r.isError, undefined);
      assert.match(r.output, /Base commit: unknown/);
    } finally {
      mock.restoreAll();
    }
  });

  it('swallows errors from opportunistic stale-worktree cleanup', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    // Make the worktrees base path a FILE: existsSync() is true so cleanup runs,
    // but readdirSync inside cleanupStaleWorktrees throws ENOTDIR — the error must
    // be swallowed (non-critical) and surface only as a later creation failure.
    const si = path.join(repo, '.superinference');
    fs.mkdirSync(si, { recursive: true });
    fs.writeFileSync(path.join(si, 'worktrees'), 'not a directory');
    const r = await enterWorktreeTool.execute({ name: 'afterclean' }, ctx({ cwd: repo }));
    // Creation fails because the worktrees path is a file, but we exercised the
    // cleanup try/catch without crashing.
    assert.equal(r.isError, true);
  });

  it('tolerates a hook manager without the worktree hook methods', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    // _hookManager present but without executeWorktreeCreate/executeCwdChanged:
    // the optional-chaining calls short-circuit without throwing.
    const r = await enterWorktreeTool.execute(
      { name: 'nohooks' },
      ctx({ cwd: repo, _hookManager: {} as any }),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Worktree created/);
  });

  it('swallows rejected worktree-create and cwd-changed hooks', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const context = ctx({
      cwd: repo,
      _hookManager: {
        executeWorktreeCreate: async () => { throw new Error('hook boom'); },
        executeCwdChanged: async () => { throw new Error('hook boom'); },
      } as any,
    });
    const r = await enterWorktreeTool.execute({ name: 'hookfail' }, context);
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Worktree created/);
    // Give the rejected promises a tick so their .catch handlers run.
    await new Promise(res => setTimeout(res, 20));
  });
});

// ---------------------------------------------------------------------------
// worktree-manager
// ---------------------------------------------------------------------------

describe('worktree-manager helpers', () => {
  it('validateWorktreeSlug accepts and rejects', () => {
    assert.equal(validateWorktreeSlug('good-name'), null);
    assert.equal(validateWorktreeSlug('a/b'), null);
    assert.match(String(validateWorktreeSlug('x'.repeat(65))), /64 characters/);
    assert.match(String(validateWorktreeSlug('../evil')), /cannot start/);
    assert.match(String(validateWorktreeSlug('.hidden')), /cannot start/);
    assert.match(String(validateWorktreeSlug('bad$char')), /invalid characters/);
    assert.match(String(validateWorktreeSlug('a/.b')), /Invalid slug segment/);
  });

  it('flattenSlug and worktreeBranchName', () => {
    assert.equal(flattenSlug('a/b/c'), 'a+b+c');
    assert.equal(worktreeBranchName('a/b'), 'worktree-a+b');
  });

  it('createWorktreeSession creates the worktree on disk and records metadata', () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const session = createWorktreeSession(repo, 'sess1');
    assert.equal(session.worktreeName, 'sess1');
    assert.equal(session.worktreeBranch, 'worktree-sess1');
    assert.equal(session.originalBranch, 'main');
    assert.ok(session.originalHeadCommit && session.originalHeadCommit.length > 0);
    assert.ok(fs.existsSync(session.worktreePath));
    assert.ok(typeof session.creationDurationMs === 'number');
    // Session store round trip
    setWorktreeSession(session);
    assert.equal(getCurrentWorktreeSession(), session);
    setWorktreeSession(null);
    assert.equal(getCurrentWorktreeSession(), null);
  });

  it('createWorktreeSession skips disk creation when worktree already exists', () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    createWorktreeSession(repo, 'sess2');
    // Second call: directory already present → no re-add, still returns metadata.
    const again = createWorktreeSession(repo, 'sess2');
    assert.equal(again.worktreeName, 'sess2');
  });

  it('createWorktreeSession tolerates a non-git cwd (rev-parse failures)', () => {
    const dir = track(mkTmp());
    // Not a git repo: the rev-parse calls throw and are swallowed, but then
    // worktree add also fails — createWorktreeSession lets that surface.
    assert.throws(() => createWorktreeSession(dir, 'nope'));
  });

  it('countWorktreeChanges returns null for a non-git path', () => {
    const dir = track(mkTmp());
    assert.equal(countWorktreeChanges(dir, dir), null);
  });

  it('countWorktreeChanges counts uncommitted and unpushed changes', () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    fs.writeFileSync(path.join(repo, 'new.txt'), 'x');
    const changes = countWorktreeChanges(repo, repo);
    assert.ok(changes);
    assert.ok(changes!.uncommitted >= 1);
    // No remote configured → the initial commit counts as unpushed.
    assert.ok(changes!.unpushed >= 1);
  });

  it('countWorktreeChanges handles a detached HEAD', () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const head = git('rev-parse HEAD', repo).trim();
    git(`checkout --detach ${head}`, repo);
    const changes = countWorktreeChanges(repo, repo);
    assert.ok(changes);
    assert.equal(changes!.uncommitted, 0);
  });

  it('cleanupStaleWorktrees returns [] when no worktrees dir exists', () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    assert.deepEqual(cleanupStaleWorktrees(repo), []);
  });

  it('cleanupStaleWorktrees skips non-stale, changed, non-git, and non-dir entries; swallows remove errors', () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const wtBase = path.join(repo, '.superinference', 'worktrees');
    fs.mkdirSync(wtBase, { recursive: true });
    const old = new Date(Date.now() - 60 * 24 * 60 * 60 * 1000); // 60 days ago

    // 1. A stale "empty" git repo (no commits) → counts as {0,0}, but remove
    //    fails because it is not a registered worktree of `repo` → catch arm.
    const emptyRepo = path.join(wtBase, 'stale-empty');
    fs.mkdirSync(emptyRepo);
    git('init', emptyRepo);
    fs.utimesSync(emptyRepo, old, old);

    // 2. A stale git repo with uncommitted changes → skipped.
    const dirtyRepo = path.join(wtBase, 'stale-dirty');
    fs.mkdirSync(dirtyRepo);
    initRepoWithCommit(dirtyRepo);
    fs.writeFileSync(path.join(dirtyRepo, 'dirty.txt'), 'y');
    fs.utimesSync(dirtyRepo, old, old);

    // 3. A stale non-git plain directory → countWorktreeChanges null → skipped.
    const plain = path.join(wtBase, 'stale-plain');
    fs.mkdirSync(plain);
    fs.utimesSync(plain, old, old);

    // 4. A plain file (not a directory) → skipped.
    fs.writeFileSync(path.join(wtBase, 'a-file.txt'), 'z');

    // 5. An ephemeral-named recent dir (halved threshold, but still recent) → skipped.
    const ephemeral = path.join(wtBase, 'wt-12345678');
    fs.mkdirSync(ephemeral);

    // 6. A fresh (non-stale) git repo → skipped by age.
    const fresh = path.join(wtBase, 'fresh');
    fs.mkdirSync(fresh);
    git('init', fresh);

    const cleaned = cleanupStaleWorktrees(repo);
    assert.ok(Array.isArray(cleaned));
    // None of these are removable registered worktrees.
    assert.ok(!cleaned.includes('stale-dirty'));
    assert.ok(!cleaned.includes('stale-plain'));
    assert.ok(!cleaned.includes('fresh'));
  });

  it('cleanupStaleWorktrees returns [] when a missing worktrees dir is given', () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    assert.deepEqual(cleanupStaleWorktrees(repo, 1), []);
  });

  it('symlinkLargeDirectories links existing dirs, skips existing targets and missing sources', () => {
    const src = track(mkTmp());
    const wt = track(mkTmp());
    fs.mkdirSync(path.join(src, 'node_modules'));
    fs.writeFileSync(path.join(src, 'node_modules', 'marker'), '1');

    symlinkLargeDirectories(src, wt);
    const linked = path.join(wt, 'node_modules');
    assert.ok(fs.existsSync(linked));
    assert.ok(fs.lstatSync(linked).isSymbolicLink());

    // Second call: target exists → skipped, no throw.
    symlinkLargeDirectories(src, wt);

    // Missing source dir → skipped.
    symlinkLargeDirectories(src, wt, ['does-not-exist']);
  });

  it('symlinkLargeDirectories swallows symlink errors (unwritable target parent)', () => {
    const src = track(mkTmp());
    fs.mkdirSync(path.join(src, 'node_modules'));
    // Target parent does not exist → symlinkSync throws → caught silently.
    assert.doesNotThrow(() => symlinkLargeDirectories(src, '/nonexistent-parent-xyz/wt'));
  });

  it('copyWorktreeIncludes returns [] when no .worktreeinclude exists', () => {
    const src = track(mkTmp());
    const wt = track(mkTmp());
    assert.deepEqual(copyWorktreeIncludes(src, wt), []);
  });

  it('copyWorktreeIncludes copies files and directories, skipping comments and missing entries', () => {
    const src = track(mkTmp());
    const wt = track(mkTmp());
    fs.writeFileSync(path.join(src, 'config.env'), 'SECRET=1');
    fs.mkdirSync(path.join(src, 'data'));
    fs.writeFileSync(path.join(src, 'data', 'a.txt'), 'a');
    fs.mkdirSync(path.join(src, 'data', 'sub'));
    fs.writeFileSync(path.join(src, 'data', 'sub', 'b.txt'), 'b');
    fs.writeFileSync(
      path.join(src, '.worktreeinclude'),
      '# comment\n\nconfig.env\ndata\nmissing-file.txt\n',
    );

    const copied = copyWorktreeIncludes(src, wt);
    assert.ok(copied.includes('config.env'));
    assert.ok(copied.includes('data'));
    assert.ok(!copied.includes('missing-file.txt'));
    assert.ok(fs.existsSync(path.join(wt, 'config.env')));
    assert.ok(fs.existsSync(path.join(wt, 'data', 'a.txt')));
    assert.ok(fs.existsSync(path.join(wt, 'data', 'sub', 'b.txt')));
  });

  it('copyWorktreeIncludes swallows per-entry copy errors', () => {
    const src = track(mkTmp());
    const wt = track(mkTmp());
    fs.mkdirSync(path.join(src, 'a'));
    fs.writeFileSync(path.join(src, 'a', 'b.txt'), 'x');
    fs.writeFileSync(path.join(src, '.worktreeinclude'), 'a/b.txt\n');
    // Put a FILE where the "a" parent directory needs to be created → mkdirSync throws.
    fs.writeFileSync(path.join(wt, 'a'), 'blocking file');
    const copied = copyWorktreeIncludes(src, wt);
    assert.ok(!copied.includes('a/b.txt'));
  });

  it('cleanupStaleWorktrees swallows statSync errors (broken symlink entry)', () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const wtBase = path.join(repo, '.superinference', 'worktrees');
    fs.mkdirSync(wtBase, { recursive: true });
    // A broken symlink: statSync (follows links) throws ENOENT → outer catch.
    fs.symlinkSync(path.join(repo, 'no-such-target'), path.join(wtBase, 'broken-link'));
    assert.doesNotThrow(() => cleanupStaleWorktrees(repo));
  });

  it('validateWorktreeGitFile validates .git across its shapes', () => {
    // Directory .git → valid (ordinary clone).
    const d1 = track(mkTmp());
    fs.mkdirSync(path.join(d1, '.git'));
    assert.deepEqual(validateWorktreeGitFile(d1), { valid: true });

    // Missing .git → invalid.
    const d2 = track(mkTmp());
    assert.equal(validateWorktreeGitFile(d2).valid, false);
    assert.match(String(validateWorktreeGitFile(d2).error), /missing \.git/);

    // .git is neither file nor directory (symlink) → "not a file or directory".
    const d3 = track(mkTmp());
    fs.symlinkSync(path.join(d3, 'elsewhere'), path.join(d3, '.git'));
    const r3 = validateWorktreeGitFile(d3);
    assert.equal(r3.valid, false);
    assert.match(String(r3.error), /not a file or directory/);

    // .git file without a gitdir: prefix → invalid.
    const d4 = track(mkTmp());
    fs.writeFileSync(path.join(d4, '.git'), 'garbage content');
    assert.match(String(validateWorktreeGitFile(d4).error), /missing gitdir: prefix/);

    // .git file with an empty gitdir path → invalid.
    const d5 = track(mkTmp());
    fs.writeFileSync(path.join(d5, '.git'), 'gitdir:   ');
    assert.match(String(validateWorktreeGitFile(d5).error), /empty gitdir path/);

    // .git file pointing at a missing gitdir target → stale.
    const d6 = track(mkTmp());
    fs.writeFileSync(path.join(d6, '.git'), 'gitdir: /does/not/exist/here');
    assert.match(String(validateWorktreeGitFile(d6).error), /stale worktree/);

    // .git file with a relative gitdir that resolves → valid.
    const d7 = track(mkTmp());
    fs.mkdirSync(path.join(d7, 'realgit'));
    fs.writeFileSync(path.join(d7, '.git'), 'gitdir: ./realgit');
    assert.deepEqual(validateWorktreeGitFile(d7), { valid: true });

    // .git file that cannot be read (no permissions) → "unreadable .git file".
    const d8 = track(mkTmp());
    const gitFile = path.join(d8, '.git');
    fs.writeFileSync(gitFile, 'gitdir: ./x');
    fs.chmodSync(gitFile, 0o000);
    const r8 = validateWorktreeGitFile(d8);
    // Restore perms so cleanup can delete it.
    fs.chmodSync(gitFile, 0o644);
    assert.equal(r8.valid, false);
    assert.match(String(r8.error), /unreadable \.git file/);
  });

  it('findStaleWorktrees finds invalid worktrees and tolerates errors', () => {
    // No worktrees dir → [].
    const empty = track(mkTmp());
    assert.deepEqual(findStaleWorktrees(empty), []);

    // worktrees path is a FILE → readdirSync throws → [].
    const fileBase = track(mkTmp());
    fs.mkdirSync(path.join(fileBase, '.superinference'), { recursive: true });
    fs.writeFileSync(path.join(fileBase, '.superinference', 'worktrees'), 'not a dir');
    assert.deepEqual(findStaleWorktrees(fileBase), []);

    // Mixed entries.
    const repo = track(mkTmp());
    const wtBase = path.join(repo, '.superinference', 'worktrees');
    fs.mkdirSync(wtBase, { recursive: true });
    // valid worktree (directory .git)
    const good = path.join(wtBase, 'good');
    fs.mkdirSync(good);
    fs.mkdirSync(path.join(good, '.git'));
    // stale worktree (missing .git)
    const stale = path.join(wtBase, 'stale');
    fs.mkdirSync(stale);
    // a plain file entry → statSync ok, not a dir → skipped
    fs.writeFileSync(path.join(wtBase, 'afile'), 'x');
    // a broken symlink entry → statSync throws → continue
    fs.symlinkSync(path.join(wtBase, 'nope'), path.join(wtBase, 'brokenlink'));

    const result = findStaleWorktrees(repo);
    assert.ok(result.includes(stale));
    assert.ok(!result.includes(good));
  });
});

// ---------------------------------------------------------------------------
// cron_create / cron_list
// ---------------------------------------------------------------------------

describe('cron tools', () => {
  beforeEach(() => resetScheduler());
  after(() => resetScheduler());

  it('cron_create rejects an invalid cron expression', async () => {
    const dir = track(mkTmp());
    const r = await cronCreateTool.execute({ cron: 'bad', prompt: 'hi' }, ctx({ cwd: dir }));
    assert.equal(r.isError, true);
    assert.match(r.output, /5-field cron/);
  });

  it('cron_create rejects a missing cron expression', async () => {
    const dir = track(mkTmp());
    const r = await cronCreateTool.execute({ prompt: 'hi' } as Record<string, unknown>, ctx({ cwd: dir }));
    assert.equal(r.isError, true);
    assert.match(r.output, /5-field cron/);
  });

  it('cron_create rejects an empty prompt', async () => {
    const dir = track(mkTmp());
    const r = await cronCreateTool.execute({ cron: '* * * * *', prompt: '  ' }, ctx({ cwd: dir }));
    assert.equal(r.isError, true);
    assert.match(r.output, /prompt must not be empty/);
  });

  it('cron_create rejects a missing prompt', async () => {
    const dir = track(mkTmp());
    const r = await cronCreateTool.execute({ cron: '* * * * *' } as Record<string, unknown>, ctx({ cwd: dir }));
    assert.equal(r.isError, true);
    assert.match(r.output, /prompt must not be empty/);
  });

  it('cron_create schedules a recurring durable job', async () => {
    const dir = track(mkTmp());
    resetScheduler();
    const r = await cronCreateTool.execute(
      { cron: '*/5 * * * *', prompt: 'do a thing', recurring: true, durable: true },
      ctx({ cwd: dir }),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Scheduled job cron_/);
    assert.match(r.output, /Recurring: true/);
    assert.match(r.output, /Durable: true/);
    // Persisted to disk because durable.
    assert.ok(fs.existsSync(path.join(dir, '.superinference', 'scheduled_tasks.json')));
  });

  it('cron_create defaults recurring=true and durable=false when omitted', async () => {
    const dir = track(mkTmp());
    resetScheduler();
    const r = await cronCreateTool.execute(
      { cron: '0 0 * * *', prompt: 'nightly' },
      ctx({ cwd: dir }),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Recurring: true/);
    assert.match(r.output, /Durable: false/);
  });

  it('cron_create accepts recurring=false (one-shot)', async () => {
    const dir = track(mkTmp());
    resetScheduler();
    const r = await cronCreateTool.execute(
      { cron: '0 9 * * *', prompt: 'once', recurring: false },
      ctx({ cwd: dir }),
    );
    assert.match(r.output, /Recurring: false/);
  });

  it('cron_create renders "unknown" when the created job has no next run', async () => {
    const dir = track(mkTmp());
    resetScheduler();
    const sched = getScheduler(dir);
    mock.method(sched, 'create', () => ({
      id: 'cron_9_z',
      cron: '*/5 * * * *',
      prompt: 'p',
      recurring: true,
      durable: false,
      createdAt: Date.now(),
      nextRun: undefined,
    }));
    try {
      const r = await cronCreateTool.execute(
        { cron: '*/5 * * * *', prompt: 'p' },
        ctx({ cwd: dir }),
      );
      assert.equal(r.isError, undefined);
      assert.match(r.output, /Next run: unknown/);
    } finally {
      mock.restoreAll();
    }
  });

  it('getCronScheduler / resetCronScheduler deprecated shims work', () => {
    const dir = track(mkTmp());
    resetCronScheduler();
    const s1 = getCronScheduler(dir);
    const s2 = getCronScheduler(dir);
    assert.equal(s1, s2); // same singleton
    resetCronScheduler();
  });

  it('cron_list reports "no jobs" when empty', async () => {
    const dir = track(mkTmp());
    resetScheduler();
    const r = await cronListTool.execute({}, ctx({ cwd: dir }));
    assert.match(r.output, /No scheduled jobs/);
  });

  it('cron_list renders recurring+durable and one-shot+session-only jobs', async () => {
    const dir = track(mkTmp());
    resetScheduler();
    const sched = getScheduler(dir);
    sched.create({ cron: '*/5 * * * *', prompt: 'A', recurring: true, durable: true });
    sched.create({ cron: '0 0 * * *', prompt: 'B'.repeat(200), recurring: false, durable: false });
    const r = await cronListTool.execute({}, ctx({ cwd: dir }));
    assert.match(r.output, /2 scheduled job\(s\)/);
    assert.match(r.output, /recurring, durable/);
    assert.match(r.output, /one-shot, session-only/);
    // Prompt is truncated to 100 chars.
    assert.ok(!r.output.includes('B'.repeat(101)));
  });

  it('cron_list shows "N/A" for a durable job loaded without a nextRun', async () => {
    const dir = track(mkTmp());
    const sidir = path.join(dir, '.superinference');
    fs.mkdirSync(sidir, { recursive: true });
    fs.writeFileSync(
      path.join(sidir, 'scheduled_tasks.json'),
      JSON.stringify([
        { id: 'cron_1_x', cron: '* * * * *', prompt: 'loaded', recurring: true, durable: true, createdAt: Date.now() },
      ]),
    );
    resetScheduler();
    getScheduler(dir); // loads durable jobs from disk (no nextRun)
    const r = await cronListTool.execute({}, ctx({ cwd: dir }));
    assert.match(r.output, /Next: N\/A/);
  });
});

// ---------------------------------------------------------------------------
// plan_mode
// ---------------------------------------------------------------------------

describe('plan_mode tool', () => {
  beforeEach(() => { setPlanModeActive(false); });
  after(() => { setPlanModeActive(false); });

  it('exposes plan-mode state accessors', () => {
    setPlanModeActive(true);
    assert.equal(isPlanModeActive(), true);
    setPlanModeActive(false);
    assert.equal(isPlanModeActive(), false);
    // getPlanFilePath is null outside an active enter.
    assert.equal(getPlanFilePath(), null);
  });

  it('enter creates a plan file and activates plan mode', async () => {
    const dir = track(mkTmp());
    const r = await planModeTool.execute({ action: 'enter' }, ctx({ cwd: dir }));
    assert.equal(r.isError, false);
    assert.match(r.output, /Entered plan mode/);
    assert.equal(isPlanModeActive(), true);
    const planFile = getPlanFilePath();
    assert.ok(planFile && planFile.startsWith(path.join(dir, '.superinference', 'plans')));
    assert.ok(fs.existsSync(path.dirname(planFile!)));
  });

  it('enter tolerates a plan-dir mkdir failure', async () => {
    const dir = track(mkTmp());
    // Make .superinference a FILE so mkdirSync(.superinference/plans) throws.
    fs.writeFileSync(path.join(dir, '.superinference'), 'blocking file');
    const r = await planModeTool.execute({ action: 'enter' }, ctx({ cwd: dir }));
    assert.equal(r.isError, false);
    assert.match(r.output, /Entered plan mode/);
    assert.equal(isPlanModeActive(), true);
  });

  it('enter twice is rejected', async () => {
    const dir = track(mkTmp());
    await planModeTool.execute({ action: 'enter' }, ctx({ cwd: dir }));
    const r = await planModeTool.execute({ action: 'enter' }, ctx({ cwd: dir }));
    assert.equal(r.isError, true);
    assert.match(r.output, /Already in plan mode/);
  });

  it('enter without a context falls back to process.cwd()', async () => {
    const cwd = process.cwd();
    const r = await planModeTool.execute({ action: 'enter' });
    assert.equal(r.isError, false);
    const planFile = getPlanFilePath();
    assert.ok(planFile && planFile.startsWith(path.join(cwd, '.superinference', 'plans')));
    // Clean up the plan file/dir created in the real cwd.
    try { fs.rmSync(path.join(cwd, '.superinference', 'plans'), { recursive: true, force: true }); } catch { /* ok */ }
  });

  it('verify reports "not in plan mode" when inactive', async () => {
    const r = await planModeTool.execute({ action: 'verify' }, ctx());
    assert.equal(r.isError, false);
    assert.match(r.output, /Not in plan mode/);
  });

  it('verify reports an empty plan file', async () => {
    const dir = track(mkTmp());
    await planModeTool.execute({ action: 'enter' }, ctx({ cwd: dir }));
    const r = await planModeTool.execute({ action: 'verify' }, ctx({ cwd: dir }));
    assert.match(r.output, /Plan mode active/);
    assert.match(r.output, /Plan file is empty/);
  });

  it('verify reports plan content when the plan file has content', async () => {
    const dir = track(mkTmp());
    await planModeTool.execute({ action: 'enter' }, ctx({ cwd: dir }));
    fs.writeFileSync(getPlanFilePath()!, '# My Plan\nstep 1\n');
    const r = await planModeTool.execute({ action: 'verify' }, ctx({ cwd: dir }));
    assert.match(r.output, /Plan has content/);
    assert.match(r.output, /Plan length: \d+ chars/);
  });

  it('exit without being in plan mode is rejected', async () => {
    const r = await planModeTool.execute({ action: 'exit' }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /Not in plan mode/);
  });

  it('exit submits plan content read from the plan file', async () => {
    const dir = track(mkTmp());
    await planModeTool.execute({ action: 'enter' }, ctx({ cwd: dir }));
    fs.writeFileSync(getPlanFilePath()!, 'PLAN BODY HERE');
    const r = await planModeTool.execute({ action: 'exit' }, ctx({ cwd: dir }));
    assert.equal(r.isError, false);
    assert.match(r.output, /Exiting plan mode/);
    assert.match(r.output, /PLAN BODY HERE/);
    assert.equal(isPlanModeActive(), false);
    assert.equal(getPlanFilePath(), null);
  });

  it('exit falls back to a placeholder when the plan file is missing', async () => {
    const dir = track(mkTmp());
    await planModeTool.execute({ action: 'enter' }, ctx({ cwd: dir }));
    // Remove the plan file so readFileSync throws inside exit.
    fs.rmSync(getPlanFilePath()!, { force: true });
    const r = await planModeTool.execute({ action: 'exit' }, ctx({ cwd: dir }));
    assert.match(r.output, /No plan file found/);
  });

  it('rejects an unknown action', async () => {
    const r = await planModeTool.execute({ action: 'frobnicate' }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /Unknown action/);
  });
});

// ---------------------------------------------------------------------------
// git_commit
// ---------------------------------------------------------------------------

describe('git_commit tool', () => {
  let savedDisable: string | undefined;
  before(() => { savedDisable = process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR; });
  after(() => {
    if (savedDisable === undefined) delete process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR;
    else process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR = savedDisable;
  });

  it('buildFinalMessage appends the trailer, respects opt-out and existing trailer', () => {
    delete process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR;
    const appended = buildFinalMessage('fix: thing');
    assert.ok(appended.endsWith(CO_AUTHOR_TRAILER));
    assert.ok(TRAILER_REGEX.test(appended));

    // Already has a trailer → not duplicated.
    const withTrailer = `fix: thing\n\n${CO_AUTHOR_TRAILER}`;
    assert.equal(buildFinalMessage(withTrailer), withTrailer.trim());

    // Opt-out env → no trailer.
    process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR = '1';
    assert.equal(buildFinalMessage('fix: thing'), 'fix: thing');
    delete process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR;
  });

  it('rejects an empty commit message', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const r = await gitCommitTool.execute({ message: '   ' }, ctx({ cwd: repo }));
    assert.equal(r.isError, true);
    assert.match(r.output, /must not be empty/);
  });

  it('rejects a flag-like entry in files (injection guard)', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const r = await gitCommitTool.execute(
      { message: 'ok', files: ['--force'] },
      ctx({ cwd: repo }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /must be file paths, not flags/);
  });

  it('ignores an empty files array and non-string entries', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    // Empty array → staging skipped → nothing staged error.
    const r1 = await gitCommitTool.execute({ message: 'ok', files: [] }, ctx({ cwd: repo }));
    assert.equal(r1.isError, true);
    assert.match(r1.output, /nothing staged/);

    // Non-string entries are filtered out before staging.
    fs.writeFileSync(path.join(repo, 'real.txt'), 'data');
    const r2 = await gitCommitTool.execute(
      { message: 'feat: with mixed entries', files: ['real.txt', 123 as unknown as string] },
      ctx({ cwd: repo }),
    );
    assert.equal(r2.isError, false);
  });

  it('errors when staging a nonexistent file', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const r = await gitCommitTool.execute(
      { message: 'ok', files: ['no-such-file.txt'] },
      ctx({ cwd: repo }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /Error staging files/);
  });

  it('stringifies a non-Error thrown while staging', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    fs.writeFileSync(path.join(repo, 's.txt'), 'data');
    mock.method(_gitCommitFs, 'execFileSync', () => { throw 'plain string failure'; });
    try {
      const r = await gitCommitTool.execute(
        { message: 'feat: staging blows up', files: ['s.txt'] },
        ctx({ cwd: repo }),
      );
      assert.equal(r.isError, true);
      assert.match(r.output, /Error staging files: plain string failure/);
    } finally {
      mock.restoreAll();
    }
  });

  it('errors when nothing is staged', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const r = await gitCommitTool.execute({ message: 'nothing here' }, ctx({ cwd: repo }));
    assert.equal(r.isError, true);
    assert.match(r.output, /nothing staged/);
  });

  it('blocks a commit message containing a detected secret', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    fs.writeFileSync(path.join(repo, 'f.txt'), 'content');
    const r = await gitCommitTool.execute(
      { message: 'add key AKIAABCDEFGHIJKLMNOP', files: ['f.txt'] },
      ctx({ cwd: repo }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /Potential secrets detected/);
    // Nothing committed: still only the initial commit.
    assert.equal(git('rev-list --count HEAD', repo).trim(), '1');
  });

  it('commits staged files (bypass variant of the secret guard) with explicit file list', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    fs.writeFileSync(path.join(repo, 'feature.txt'), 'hello');
    const r = await gitCommitTool.execute(
      { message: 'feat: add feature file', files: ['feature.txt'] },
      ctx({ cwd: repo }),
    );
    assert.equal(r.isError, false);
    assert.equal(git('rev-list --count HEAD', repo).trim(), '2');
    // Trailer applied by default.
    const body = git('log -1 --pretty=%B', repo);
    assert.match(body, TRAILER_REGEX);
  });

  it('stages all tracked modifications with files: ["."] (git add -u)', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    // Modify the already-tracked README.
    fs.writeFileSync(path.join(repo, 'README.md'), '# repo changed\n');
    const r = await gitCommitTool.execute(
      { message: 'chore: update readme', files: ['.'] },
      ctx({ cwd: repo }),
    );
    assert.equal(r.isError, false);
    assert.equal(git('rev-list --count HEAD', repo).trim(), '2');
  });

  it('commits whatever is already staged when no files are given', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    fs.writeFileSync(path.join(repo, 'pre.txt'), 'staged');
    git('add pre.txt', repo);
    const r = await gitCommitTool.execute({ message: 'feat: pre-staged' }, ctx({ cwd: repo }));
    assert.equal(r.isError, false);
    assert.equal(git('rev-list --count HEAD', repo).trim(), '2');
  });

  it('stages multiple explicit files via "git add --"', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    fs.writeFileSync(path.join(repo, 'one.txt'), '1');
    fs.writeFileSync(path.join(repo, 'two.txt'), '2');
    const r = await gitCommitTool.execute(
      { message: 'feat: two files', files: ['one.txt', 'two.txt'] },
      ctx({ cwd: repo }),
    );
    assert.equal(r.isError, false);
    assert.equal(git('rev-list --count HEAD', repo).trim(), '2');
  });

  it('still succeeds when temp-file cleanup (unlink) throws', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    fs.writeFileSync(path.join(repo, 'z.txt'), 'data');
    mock.method(_gitCommitFs, 'unlinkSync', () => { throw new Error('simulated unlink failure'); });
    try {
      const r = await gitCommitTool.execute(
        { message: 'feat: cleanup throws', files: ['z.txt'] },
        ctx({ cwd: repo }),
      );
      // Commit still reported as successful despite the swallowed unlink error.
      assert.equal(r.isError, false);
      assert.equal(git('rev-list --count HEAD', repo).trim(), '2');
    } finally {
      mock.restoreAll();
    }
  });

  it('reports git commit failures (pre-commit hook rejects)', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    const hookDir = path.join(repo, '.git', 'hooks');
    fs.mkdirSync(hookDir, { recursive: true });
    const hook = path.join(hookDir, 'pre-commit');
    fs.writeFileSync(hook, '#!/bin/sh\necho "rejected by hook" 1>&2\nexit 1\n');
    fs.chmodSync(hook, 0o755);
    fs.writeFileSync(path.join(repo, 'x.txt'), 'data');
    const r = await gitCommitTool.execute(
      { message: 'feat: should fail', files: ['x.txt'] },
      ctx({ cwd: repo }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /git commit failed/);
  });
});
