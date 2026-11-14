import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { enterWorktreeTool } from '../src/tools/enter-worktree';
import { exitWorktreeTool } from '../src/tools/exit-worktree';
import { setWorktreeSession, getCurrentWorktreeSession } from '../src/worktree-manager';
import type { ToolContext } from '../src/types';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, { cwd, encoding: 'utf-8' }).trim();
}
function makeRepo(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ee-wt-'));
  git(dir, 'init', '-q');
  git(dir, 'config', 'user.email', 't@t.com');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  fs.writeFileSync(path.join(dir, 'README.md'), '# r\n');
  git(dir, 'add', 'README.md');
  git(dir, 'commit', '-q', '-m', 'init');
  return dir;
}
const ctx = (over: Partial<ToolContext> = {}) => ({ cwd: process.cwd(), abortSignal: new AbortController().signal, ...over } as ToolContext);

describe('enter_worktree parity', () => {
  let repo: string;
  beforeEach(() => { repo = makeRepo(); setWorktreeSession(null); });
  afterEach(() => { fs.rmSync(repo, { recursive: true, force: true }); setWorktreeSession(null); });

  it('description carries when-to-use / when-NOT guidance', () => {
    assert.match(enterWorktreeTool.description, /WHEN TO USE/);
    assert.match(enterWorktreeTool.description, /WHEN NOT TO USE/);
  });

  it('not-a-git-repo error points to the hook escape hatch', async () => {
    const plain = fs.mkdtempSync(path.join(os.tmpdir(), 'ee-plain-'));
    try {
      const r = await enterWorktreeTool.execute({ name: 'x' }, ctx({ cwd: plain }));
      assert.equal(r.isError, true);
      assert.match(r.output, /WorktreeCreate\/WorktreeRemove hooks/);
    } finally {
      fs.rmSync(plain, { recursive: true, force: true });
    }
  });

  it('names the base branch in the success message', async () => {
    const r = await enterWorktreeTool.execute({ name: 'feat1' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined);
    assert.match(r.output, /based on /);
    assert.match(r.output, /exit_worktree/);
  });

  it('refuses to create a nested worktree from inside a worktree cwd', async () => {
    const insideWt = path.join(repo, '.ami', 'worktrees', 'existing');
    fs.mkdirSync(insideWt, { recursive: true });
    const r = await enterWorktreeTool.execute({ name: 'nested' }, ctx({ cwd: insideWt }));
    assert.equal(r.isError, true);
    assert.match(r.output, /already inside a worktree/);
  });
});

describe('exit_worktree parity', () => {
  let repo: string;
  beforeEach(() => { repo = makeRepo(); setWorktreeSession(null); });
  afterEach(() => { fs.rmSync(repo, { recursive: true, force: true }); setWorktreeSession(null); });

  it('rejects unknown parameters', async () => {
    const r = await exitWorktreeTool.execute({ action: 'keep', bogus: 1 } as Record<string, unknown>, ctx({ cwd: repo }));
    assert.equal(r.isError, true);
    assert.match(r.output, /unknown parameter/);
  });

  it('not-a-git-repo error points to the hook escape hatch', async () => {
    const plain = fs.mkdtempSync(path.join(os.tmpdir(), 'ee-plain2-'));
    try {
      const r = await exitWorktreeTool.execute({ action: 'keep' }, ctx({ cwd: plain }));
      assert.equal(r.isError, true);
      assert.match(r.output, /WorktreeCreate\/WorktreeRemove hooks/);
    } finally {
      fs.rmSync(plain, { recursive: true, force: true });
    }
  });

  it('keep message names the branch, a resume hint, and the returned cwd; remove deletes the branch', async () => {
    const created = await enterWorktreeTool.execute({ name: 'keepme' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined);
    const session = getCurrentWorktreeSession()!;
    const wtPath = session.worktreePath;

    const kept = await exitWorktreeTool.execute({ action: 'keep' }, ctx({ cwd: wtPath }));
    assert.match(kept.output, /Worktree kept/);
    assert.match(kept.output, /Resume with: cd /);
    assert.match(kept.output, /Returned to /);
    assert.equal(getCurrentWorktreeSession(), null);

    // Now remove it (session was cleared, so no scope guard); branch should be deleted.
    const branch = session.worktreeBranch;
    const r = await exitWorktreeTool.execute({ action: 'remove', discard_changes: true }, ctx({ cwd: wtPath }));
    assert.match(r.output, /Worktree removed/);
    // The worktree branch must no longer exist.
    const branches = git(repo, 'branch', '--list', branch);
    assert.equal(branches, '');
  });

  it('refuses to remove a worktree the active session does not own (scope guard)', async () => {
    // Create two worktrees; session points at wt-a, we try to exit wt-b.
    await enterWorktreeTool.execute({ name: 'wt-a' }, ctx({ cwd: repo }));
    const sessionA = getCurrentWorktreeSession()!;
    // Manually create a second worktree via git.
    const wtB = path.join(repo, '.ami', 'worktrees', 'wt-b');
    git(repo, 'worktree', 'add', '-q', '-B', 'worktree-wt-b', wtB, 'HEAD');
    // Session still owns wt-a.
    setWorktreeSession(sessionA);
    const r = await exitWorktreeTool.execute({ action: 'remove', discard_changes: true }, ctx({ cwd: wtB }));
    assert.equal(r.isError, false);
    assert.match(r.output, /not created by the active session/);
    assert.equal(fs.existsSync(wtB), true, 'foreign worktree must be untouched');
  });

  it('refuses removal with uncommitted changes and truncates a long file preview', async () => {
    await enterWorktreeTool.execute({ name: 'dirty' }, ctx({ cwd: repo }));
    const wt = getCurrentWorktreeSession()!.worktreePath;
    for (let i = 0; i < 12; i++) fs.writeFileSync(path.join(wt, `f${i}.txt`), 'x');
    const r = await exitWorktreeTool.execute({ action: 'remove' }, ctx({ cwd: wt }));
    assert.equal(r.isError, true);
    assert.match(r.output, /uncommitted file/);
    assert.match(r.output, /and \d+ more/);
    assert.match(r.output, /action:"keep"/);
  });

  it('reports discarded counts on a forced remove of a dirty worktree', async () => {
    await enterWorktreeTool.execute({ name: 'force' }, ctx({ cwd: repo }));
    const wt = getCurrentWorktreeSession()!.worktreePath;
    fs.writeFileSync(path.join(wt, 'scratch.txt'), 'x');
    const r = await exitWorktreeTool.execute({ action: 'remove', discard_changes: true }, ctx({ cwd: wt }));
    assert.match(r.output, /Worktree removed/);
    assert.match(r.output, /discarded/);
  });

  it('no-op outside a worktree path', async () => {
    const r = await exitWorktreeTool.execute({ action: 'remove' }, ctx({ cwd: repo }));
    assert.equal(r.isError, false);
    assert.match(r.output, /No active worktree session/);
  });
});
