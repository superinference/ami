import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { enterWorktreeTool } from '../src/tools/enter-worktree';
import { setWorktreeSession } from '../src/worktree-manager';
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
});
