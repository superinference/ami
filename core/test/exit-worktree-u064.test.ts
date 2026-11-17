/**
 * U064 — exit_worktree parity hardening.
 *
 * Covers two confirmed parity gaps against Claude's ExitWorktree/
 * WorktreeExitDialog:
 *   1. (G3) Consequence-explaining COMBINED summary before a destructive remove:
 *      when a worktree has BOTH uncommitted files AND unpushed commits, the
 *      refusal names both losses up front in one message (not one-at-a-time).
 *   2. (G2) The exit path's git runner is worktree-manager's hardened `gitExec`
 *      (timeout + GIT_NO_PROMPT_ENV + hooks/fsmonitor/gpg off) — one canonical
 *      hardened git invocation, not a raw unbounded child_process call.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { enterWorktreeTool } from '../src/tools/enter-worktree';
import { exitWorktreeTool, _exitWorktreeHooks } from '../src/tools/exit-worktree';
import { setWorktreeSession, getCurrentWorktreeSession, gitExec } from '../src/worktree-manager';
import type { ToolContext } from '../src/types';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, { cwd, encoding: 'utf-8' }).trim();
}

function makeRepo(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u064-wt-'));
  git(dir, 'init', '-q');
  git(dir, 'config', 'user.email', 't@t.com');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  try { git(dir, 'checkout', '-q', '-B', 'main'); } catch { /* older git */ }
  fs.writeFileSync(path.join(dir, 'README.md'), '# r\n');
  git(dir, 'add', 'README.md');
  git(dir, 'commit', '-q', '-m', 'init');
  return dir;
}

const ctx = (over: Partial<ToolContext> = {}) =>
  ({ cwd: process.cwd(), abortSignal: new AbortController().signal, ...over } as ToolContext);

describe('exit_worktree U064 parity', () => {
  let repo: string;
  const launchCwd = process.cwd();

  beforeEach(() => { repo = makeRepo(); setWorktreeSession(null); });
  afterEach(() => {
    // Always leave the process in a directory that still exists before deleting
    // the temp repo, so a chdir-into-repo test can't strand the suite.
    try { process.chdir(launchCwd); } catch { /* ignore */ }
    fs.rmSync(repo, { recursive: true, force: true });
    setWorktreeSession(null);
  });

  // ---- G3: combined uncommitted + commits consequence summary on remove ----
  it('names BOTH uncommitted files and unpushed commits in one refusal before a remove', async () => {
    const created = await enterWorktreeTool.execute({ name: 'feat-both' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;

    // One committed change (counts as an unpushed commit vs the session base)...
    fs.writeFileSync(path.join(wt, 'committed.txt'), 'c\n');
    git(wt, 'add', 'committed.txt');
    git(wt, 'commit', '-q', '-m', 'work');
    // ...and one still-uncommitted change.
    fs.writeFileSync(path.join(wt, 'dirty.txt'), 'wip\n');

    const r = await exitWorktreeTool.execute({ action: 'remove' }, ctx({ cwd: wt }));
    assert.equal(r.isError, true, r.output);
    // RED (pre-fix): the uncommitted-first refusal returned early and never
    // mentioned the commits, so this substring was absent.
    assert.match(r.output, /uncommitted file/);
    assert.match(r.output, /unpushed commit/);
    // The combined "N and M" phrasing and the permanent-loss consequence.
    assert.match(r.output, /and 1 unpushed commit/);
    assert.match(r.output, /discard this work permanently/);
    assert.match(r.output, /discard_changes=true/);
    // Nothing destroyed on refusal.
    assert.equal(fs.existsSync(wt), true);
  });

  // ---- G2: exit path reuses worktree-manager's single hardened git runner ----
  it('routes every exit-path git call through the hardened gitExec runner', () => {
    // RED (pre-consolidation): _exitWorktreeHooks.exec was a local, unbounded
    // execFileSync wrapper (no timeout / no GIT_NO_PROMPT_ENV / no -c hardening),
    // distinct from worktree-manager's gitExec. Now it IS gitExec.
    assert.strictEqual(_exitWorktreeHooks.exec, gitExec);
  });

  // ---- U057: the remove refusal enumerates at-risk files split tracked/untracked ----
  it('labels tracked modifications vs untracked files in the remove refusal preview', async () => {
    const created = await enterWorktreeTool.execute({ name: 'feat-preview' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;

    // A tracked modification (README.md is committed) and a brand-new untracked file.
    fs.writeFileSync(path.join(wt, 'README.md'), '# changed\n');
    fs.writeFileSync(path.join(wt, 'scratch.txt'), 'temp\n');

    const r = await exitWorktreeTool.execute({ action: 'remove' }, ctx({ cwd: wt }));
    assert.equal(r.isError, true, r.output);
    // RED (pre-U057): the preview dumped raw `git status --porcelain` lines with no
    // tracked/untracked distinction, so these labels were absent.
    assert.match(r.output, /modified: README\.md/);
    assert.match(r.output, /untracked: scratch\.txt/);
    // Nothing destroyed on refusal.
    assert.equal(fs.existsSync(wt), true);
  });
});
