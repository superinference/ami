import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { enterWorktreeTool } from '../src/tools/enter-worktree';
import { setWorktreeSession, getCurrentWorktreeSession } from '../src/worktree-manager';
import type { ToolContext } from '../src/types';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, { cwd, encoding: 'utf-8' }).trim();
}

function makeRepo(initArgs: string[] = ['init', '-q']): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ew66-'));
  git(dir, ...initArgs);
  git(dir, 'config', 'user.email', 't@t.com');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  fs.writeFileSync(path.join(dir, 'README.md'), '# r\n');
  git(dir, 'add', 'README.md');
  git(dir, 'commit', '-q', '-m', 'init');
  return dir;
}

const ctx = (over: Partial<ToolContext> = {}) =>
  ({ cwd: process.cwd(), abortSignal: new AbortController().signal, ...over } as ToolContext);

describe('enter_worktree — U066 parity', () => {
  const repos: string[] = [];
  beforeEach(() => { setWorktreeSession(null); });
  afterEach(() => {
    for (const r of repos.splice(0)) fs.rmSync(r, { recursive: true, force: true });
    setWorktreeSession(null);
  });
  const track = (r: string) => { repos.push(r); return r; };

  // --- GAP #4/#5: flatten '/' in the slug for BOTH dir path and branch ------
  // RED (revert the flattenSlug change): name "user/feature" nests the dir at
  //   .ami/worktrees/user/feature AND creates ref worktree/user/feature.
  // GREEN: the slug flattens to user+feature for both, so neither hazard exists.
  it('flattens a "/"-containing name to a single dir segment (no nested worktree dir)', async () => {
    const repo = track(makeRepo());
    const r = await enterWorktreeTool.execute({ name: 'user/feature' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    // Flattened, single-segment dir exists.
    assert.equal(fs.existsSync(path.join(repo, '.ami', 'worktrees', 'user+feature')), true);
    // The dangerous NESTED directory must NOT exist.
    assert.equal(fs.existsSync(path.join(repo, '.ami', 'worktrees', 'user', 'feature')), false);
  });

  it('flattens a "/"-containing name in the branch ref (no D/F ref conflict)', async () => {
    const repo = track(makeRepo());
    const r = await enterWorktreeTool.execute({ name: 'user/feature' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    assert.match(r.output, /worktree\/user\+feature/);
    // The flattened branch exists; the nested directory-form ref does not.
    const branches = git(repo, 'branch', '--format=%(refname:short)').split('\n');
    assert.ok(branches.includes('worktree/user+feature'), `branches: ${branches}`);
    assert.ok(!branches.includes('worktree/user/feature'), `branches: ${branches}`);
    // Creation of a second worktree must not have happened (one managed dir only).
    const managed = fs.readdirSync(path.join(repo, '.ami', 'worktrees'));
    assert.deepEqual(managed, ['user+feature']);
  });

  // --- GAP #59: offline default-branch candidate list, 'main' final fallback
  // RED (revert getDefaultBranch): a repo with neither main nor master nor a
  //   remote resolves the default branch to the non-existent 'master' guess.
  // GREEN: candidates main/master are probed; both absent → final fallback 'main'.
  it('falls back to "main" (not "master") when no main/master/remote exists', async () => {
    const repo = track(makeRepo(['init', '-b', 'trunk', '-q']));
    const r = await enterWorktreeTool.execute({ name: 'trunkwt' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    assert.match(r.output, /based on origin\/main/);
    assert.doesNotMatch(r.output, /based on origin\/master/);
  });

  it('still selects "master" when a local master branch exists', async () => {
    const repo = track(makeRepo(['init', '-b', 'master', '-q']));
    const r = await enterWorktreeTool.execute({ name: 'masterwt' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    assert.match(r.output, /based on origin\/master/);
  });

  // --- GAP #43/#49: name the branch when entering an existing worktree -------
  // RED (revert the branchLabel change): the enter-existing message is only
  //   "Entered existing worktree at: <path>" with no branch.
  // GREEN: the message reports the branch the worktree is on.
  it('reports the branch when entering an existing worktree by path', async () => {
    const repo = track(makeRepo());
    const created = await enterWorktreeTool.execute({ name: 'exist1' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    setWorktreeSession(null);
    const wtPath = path.join(repo, '.ami', 'worktrees', 'exist1');
    const r = await enterWorktreeTool.execute({ path: wtPath }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    assert.match(r.output, /Entered existing worktree/);
    assert.match(r.output, /on branch worktree\/exist1/);
  });

  // --- Non-git VCS-agnostic worktree via WorktreeCreate-hook delegation --------
  // A fresh (non-git) directory so `rev-parse --is-inside-work-tree` fails and
  // the tool takes the non-git branch.
  const nonGitDir = () => track(fs.mkdtempSync(path.join(os.tmpdir(), 'ew66-nogit-')));
  interface FakeHookOpts {
    count?: number;
    onCreate?: (c: { name?: string; worktreePath?: string }) => void | Promise<void>;
    throwOnCreate?: boolean;
    cwdChanged?: (c: { oldCwd: string; newCwd: string }) => void;
  }
  const fakeHooks = (o: FakeHookOpts = {}) =>
    ({
      getSummary: () => ({ worktreeCreate: o.count ?? 1 }),
      executeWorktreeCreate: async (c: { name?: string; worktreePath?: string }) => {
        if (o.throwOnCreate) throw new Error('hook boom');
        if (o.onCreate) await o.onCreate(c);
      },
      executeCwdChanged: async (c: { oldCwd: string; newCwd: string }) => { o.cwdChanged?.(c); },
    }) as unknown as NonNullable<ToolContext['_hookManager']>;

  // RED (revert the non-git hook branch): outside a git repo the tool returned a
  //   generic "not inside a git repository" error WITHOUT the cwd, and never
  //   delegated — so this error never names the directory.
  // GREEN: with no WorktreeCreate hook, the error names the cwd + the escape hatch.
  it('errors (naming the cwd + hook hint) outside git with no WorktreeCreate hook', async () => {
    const dir = nonGitDir();
    const r = await enterWorktreeTool.execute({ name: 'x' }, ctx({ cwd: dir }));
    assert.equal(r.isError, true);
    assert.match(r.output, /not inside a git repository/);
    assert.ok(r.output.includes(dir), `cwd must appear in error: ${r.output}`);
    assert.match(r.output, /WorktreeCreate\/WorktreeRemove hook/);
  });

  it('errors with cwd when a hook manager has zero WorktreeCreate hooks', async () => {
    const dir = nonGitDir();
    const r = await enterWorktreeTool.execute({ name: 'x' }, ctx({ cwd: dir, _hookManager: fakeHooks({ count: 0 }) }));
    assert.equal(r.isError, true);
    assert.ok(r.output.includes(dir), r.output);
  });

  // RED (revert the non-git hook branch): the tool would error instead of
  //   delegating to the configured WorktreeCreate hook.
  // GREEN: the hook creates the dir, the session switches into it (hookBased).
  it('delegates to a WorktreeCreate hook outside git and switches the session', async () => {
    const dir = nonGitDir();
    let created: { name?: string; worktreePath?: string } | undefined;
    let cwdEvt: { oldCwd: string; newCwd: string } | undefined;
    const context = ctx({
      cwd: dir,
      _hookManager: fakeHooks({
        onCreate: c => { created = c; fs.mkdirSync(c.worktreePath!, { recursive: true }); },
        cwdChanged: c => { cwdEvt = c; },
      }),
    });
    const r = await enterWorktreeTool.execute({ name: 'hookwt' }, context);
    assert.equal(r.isError, undefined, r.output);
    assert.match(r.output, /via WorktreeCreate hook/);
    const expectedDir = path.join(dir, '.ami', 'worktrees', 'hookwt');
    assert.equal(created?.name, 'hookwt');
    assert.equal(created?.worktreePath, expectedDir);
    // Session switched into the hook-created worktree, flagged hookBased.
    const sess = getCurrentWorktreeSession();
    assert.equal(sess?.worktreePath, expectedDir);
    assert.equal(sess?.hookBased, true);
    // context cwd re-pointed + cwdChanged hook fired with the right dirs.
    assert.equal((context as unknown as { cwd: string }).cwd, expectedDir);
    assert.equal(cwdEvt?.oldCwd, dir);
    assert.equal(cwdEvt?.newCwd, expectedDir);
  });

  it('errors when the WorktreeCreate hook does not actually create the dir', async () => {
    const dir = nonGitDir();
    const r = await enterWorktreeTool.execute({ name: 'nope' }, ctx({ cwd: dir, _hookManager: fakeHooks({ onCreate: () => {} }) }));
    assert.equal(r.isError, true);
    assert.match(r.output, /did not create/);
    assert.equal(getCurrentWorktreeSession(), null);
  });

  it('surfaces a thrown WorktreeCreate hook as an error (no session switch)', async () => {
    const dir = nonGitDir();
    const r = await enterWorktreeTool.execute({ name: 'boom' }, ctx({ cwd: dir, _hookManager: fakeHooks({ throwOnCreate: true }) }));
    assert.equal(r.isError, true);
    assert.match(r.output, /WorktreeCreate hook failed: hook boom/);
    assert.equal(getCurrentWorktreeSession(), null);
  });

  it('rejects base_ref in the non-git hook path (cannot resolve a git ref)', async () => {
    const dir = nonGitDir();
    const r = await enterWorktreeTool.execute({ name: 'br', base_ref: 'main' }, ctx({ cwd: dir, _hookManager: fakeHooks() }));
    assert.equal(r.isError, true);
    assert.match(r.output, /base_ref requires a git repository/);
  });

  it('enters an existing hook-created worktree by path outside git', async () => {
    const dir = nonGitDir();
    const existing = path.join(dir, 'wt-existing');
    fs.mkdirSync(existing, { recursive: true });
    const context = ctx({ cwd: dir, _hookManager: fakeHooks() });
    const r = await enterWorktreeTool.execute({ path: existing }, context);
    assert.equal(r.isError, undefined, r.output);
    assert.match(r.output, /Entered existing worktree at/);
    assert.equal(getCurrentWorktreeSession()?.worktreePath, path.resolve(existing));
    assert.equal(getCurrentWorktreeSession()?.hookBased, true);
  });

  it('errors when the given existing worktree path does not exist (non-git)', async () => {
    const dir = nonGitDir();
    const r = await enterWorktreeTool.execute({ path: path.join(dir, 'missing') }, ctx({ cwd: dir, _hookManager: fakeHooks() }));
    assert.equal(r.isError, true);
    assert.match(r.output, /does not exist/);
  });

  it('rejects an invalid worktree name in the non-git hook path', async () => {
    const dir = nonGitDir();
    const r = await enterWorktreeTool.execute({ name: 'bad/../x' }, ctx({ cwd: dir, _hookManager: fakeHooks() }));
    assert.equal(r.isError, true);
    assert.match(r.output, /invalid worktree name/);
  });
});
