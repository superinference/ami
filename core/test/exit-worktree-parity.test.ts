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
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ew-parity-'));
  git(dir, 'init', '-q');
  git(dir, 'config', 'user.email', 't@t.com');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  // Deterministic initial branch name across git versions.
  try { git(dir, 'checkout', '-q', '-B', 'main'); } catch { /* older git */ }
  fs.writeFileSync(path.join(dir, 'README.md'), '# r\n');
  git(dir, 'add', 'README.md');
  git(dir, 'commit', '-q', '-m', 'init');
  return dir;
}

const ctx = (over: Partial<ToolContext> = {}) =>
  ({ cwd: process.cwd(), abortSignal: new AbortController().signal, ...over } as ToolContext);

describe('exit_worktree merge-back parity', () => {
  let repo: string;
  const launchCwd = process.cwd();
  beforeEach(() => { repo = makeRepo(); setWorktreeSession(null); });
  afterEach(() => {
    // A successful merge/remove now restores the process cwd to the repo root
    // (parity: restore the session's working directory). Move the process to a
    // stable dir before deleting the temp repo so a later test's process.cwd()
    // can't land on a just-removed directory.
    try { process.chdir(launchCwd); } catch { /* ignore */ }
    fs.rmSync(repo, { recursive: true, force: true });
    setWorktreeSession(null);
  });

  // ---- GAP 1: action "merge" is a valid action & merges committed work back ----
  it('merges committed worktree work into the original branch, then removes the worktree + branch', async () => {
    const created = await enterWorktreeTool.execute({ name: 'feat-merge' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined);
    const session = getCurrentWorktreeSession()!;
    const wt = session.worktreePath;
    // Ground-truth branch name (the branch git actually created in the worktree),
    // which can differ from session.worktreeBranch.
    const branch = git(wt, 'rev-parse', '--abbrev-ref', 'HEAD');

    // Commit new work inside the worktree.
    fs.writeFileSync(path.join(wt, 'feature.txt'), 'hello\n');
    git(wt, 'add', 'feature.txt');
    git(wt, 'commit', '-q', '-m', 'add feature');

    const r = await exitWorktreeTool.execute({ action: 'merge' }, ctx({ cwd: wt }));
    // RED (pre-fix): action "merge" is rejected with 'action must be "keep" or "remove"'.
    assert.equal(r.isError, undefined, r.output);
    assert.match(r.output, /merged into main/);

    // The committed file must now exist on the original branch in the main checkout.
    assert.equal(fs.existsSync(path.join(repo, 'feature.txt')), true, 'merged file present on main');
    // The worktree branch must be gone.
    assert.equal(git(repo, 'branch', '--list', branch), '');
    // The worktree dir must be removed.
    assert.equal(fs.existsSync(wt), false);
  });

  // ---- GAP 2: merge never silently discards uncommitted work ----
  it('refuses to merge a worktree with uncommitted files unless discard_changes=true, and preserves it', async () => {
    await enterWorktreeTool.execute({ name: 'feat-dirty' }, ctx({ cwd: repo }));
    const wt = getCurrentWorktreeSession()!.worktreePath;
    fs.writeFileSync(path.join(wt, 'scratch.txt'), 'wip\n');

    const r = await exitWorktreeTool.execute({ action: 'merge' }, ctx({ cwd: wt }));
    // RED (pre-fix): no merge action existed, so this path could not guard uncommitted work.
    assert.equal(r.isError, true, r.output);
    assert.match(r.output, /uncommitted file/);
    // Worktree must still be on disk (nothing discarded).
    assert.equal(fs.existsSync(wt), true, 'dirty worktree preserved on refusal');
    assert.equal(fs.existsSync(path.join(wt, 'scratch.txt')), true);
  });

  // ---- GAP 3: a conflicting merge is aborted and the worktree is preserved ----
  it('aborts a conflicting merge, leaves the main checkout clean, and preserves the worktree', async () => {
    const created = await enterWorktreeTool.execute({ name: 'feat-conflict' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined);
    const session = getCurrentWorktreeSession()!;
    const wt = session.worktreePath;
    const branch = git(wt, 'rev-parse', '--abbrev-ref', 'HEAD');

    // Diverge: both sides edit README.md so merge must conflict.
    fs.writeFileSync(path.join(wt, 'README.md'), '# worktree side\n');
    git(wt, 'add', 'README.md');
    git(wt, 'commit', '-q', '-m', 'wt edit');

    fs.writeFileSync(path.join(repo, 'README.md'), '# main side\n');
    git(repo, 'add', 'README.md');
    git(repo, 'commit', '-q', '-m', 'main edit');

    const r = await exitWorktreeTool.execute({ action: 'merge' }, ctx({ cwd: wt }));
    // RED (pre-fix): no merge action, so no conflict-abort behavior at all.
    assert.equal(r.isError, true, r.output);
    assert.match(r.output, /conflict|aborted/i);
    // Worktree preserved.
    assert.equal(fs.existsSync(wt), true, 'worktree preserved after conflict');
    assert.equal(git(repo, 'branch', '--list', branch).includes(branch), true, 'branch preserved after conflict');
    // Main checkout must be clean (merge aborted — no MERGE_HEAD, no staged
    // conflict). Ignore untracked files (the .ami/worktrees/ container is expected).
    assert.equal(git(repo, 'status', '--porcelain', '--untracked-files=no'), '', 'main checkout clean after abort');
    assert.equal(fs.existsSync(path.join(repo, '.git', 'MERGE_HEAD')), false, 'no merge in progress');
  });

  // ---- GAP 4: merge refused when main checkout is not on the original branch ----
  it('refuses to merge when the main checkout is not on the original branch', async () => {
    const created = await enterWorktreeTool.execute({ name: 'feat-wrongbranch' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined);
    const session = getCurrentWorktreeSession()!;
    const wt = session.worktreePath;

    fs.writeFileSync(path.join(wt, 'f.txt'), 'x\n');
    git(wt, 'add', 'f.txt');
    git(wt, 'commit', '-q', '-m', 'wt work');

    // Move the main checkout off the original branch.
    git(repo, 'checkout', '-q', '-b', 'other');

    const r = await exitWorktreeTool.execute({ action: 'merge' }, ctx({ cwd: wt }));
    // RED (pre-fix): no merge action; GREEN: explicit refusal that names the mismatch.
    assert.equal(r.isError, true, r.output);
    assert.match(r.output, /not the original branch|is on "other"/);
    // Worktree preserved — nothing merged, nothing discarded.
    assert.equal(fs.existsSync(wt), true);
  });

  // ---- GAP 5: merge summary reflects merged commits, not discarded ----
  it('reports a positive remove action still works (regression) and merge reports merged commits', async () => {
    const created = await enterWorktreeTool.execute({ name: 'feat-count' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined);
    const session = getCurrentWorktreeSession()!;
    const wt = session.worktreePath;

    fs.writeFileSync(path.join(wt, 'a.txt'), '1\n');
    git(wt, 'add', 'a.txt');
    git(wt, 'commit', '-q', '-m', 'c1');
    fs.writeFileSync(path.join(wt, 'b.txt'), '2\n');
    git(wt, 'add', 'b.txt');
    git(wt, 'commit', '-q', '-m', 'c2');

    const r = await exitWorktreeTool.execute({ action: 'merge' }, ctx({ cwd: wt }));
    assert.equal(r.isError, undefined, r.output);
    // RED (pre-fix): merge did not exist, so the "merged" summary is a pure fix artifact.
    assert.match(r.output, /commits? merged/);
    // Both files are present on the original branch.
    assert.equal(fs.existsSync(path.join(repo, 'a.txt')), true);
    assert.equal(fs.existsSync(path.join(repo, 'b.txt')), true);
  });
});
