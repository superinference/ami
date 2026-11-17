/**
 * U064 (scope) — exit_worktree session-identity scope guard + reliable cwd restore.
 *
 * Covers confirmed parity gaps against Claude's ExitWorktreeTool.validateInput /
 * prompt.ts "## Scope":
 *
 *   1. Session-ownership is the SOLE entry gate (parity with Claude's
 *      validateInput using getCurrentWorktreeSession()). When no enter_worktree
 *      session is active, the tool is a strict informational NO-OP for every
 *      action — it NEVER mutates the filesystem, even inside a `.ami/worktrees/`
 *      path a restarted/foreign process landed in. Previously AMI gated the
 *      destructive remove purely on the `.ami/worktrees/` path substring and would
 *      happily `git worktree remove --force` a worktree it did not create.
 *   2. Reliable process working-directory restore on REMOVE: the keep branch
 *      chdir'd back to originalCwd but the remove branch only reassigned
 *      context.cwd, leaving the real process cwd pointing at the just-deleted
 *      worktree dir (breaking every subsequent relative-path call). Remove now
 *      restores the process cwd too.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { enterWorktreeTool } from '../src/tools/enter-worktree';
import { exitWorktreeTool, _exitWorktreeHooks } from '../src/tools/exit-worktree';
import { setWorktreeSession, getCurrentWorktreeSession } from '../src/worktree-manager';
import type { ToolContext } from '../src/types';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, { cwd, encoding: 'utf-8' }).trim();
}

function makeRepo(): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'u064scope-')));
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

describe('exit_worktree U064 scope — session-ownership gate + cwd restore', () => {
  let repo: string;
  const launchCwd = process.cwd();
  const origDelay = _exitWorktreeHooks.branchDeleteDelayMs;

  beforeEach(() => { repo = makeRepo(); setWorktreeSession(null); _exitWorktreeHooks.branchDeleteDelayMs = 0; });
  afterEach(() => {
    try { process.chdir(launchCwd); } catch { /* ignore */ }
    fs.rmSync(repo, { recursive: true, force: true });
    setWorktreeSession(null);
    _exitWorktreeHooks.branchDeleteDelayMs = origDelay;
  });

  // ---- 1a. remove with NO active session is a no-op (filesystem unchanged) ----
  it('REMOVE inside a .ami/worktrees path with no active session is an informational no-op', async () => {
    const created = await enterWorktreeTool.execute({ name: 'orphan-rm' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;
    assert.equal(fs.existsSync(wt), true);

    // Simulate a restarted / non-owning process: the worktree dir exists on disk
    // but the in-memory session was never populated by enter_worktree here.
    setWorktreeSession(null);

    const r = await exitWorktreeTool.execute({ action: 'remove', discard_changes: true }, ctx({ cwd: wt }));
    // RED (pre-fix): the destructive remove gated only on the `.ami/worktrees/`
    // substring, so it force-removed the worktree and returned a success message.
    assert.equal(r.isError, false, r.output);
    assert.match(r.output, /no active enter_worktree session/i);
    assert.match(r.output, /No filesystem changes made/);
    assert.equal(fs.existsSync(wt), true, 'worktree dir must be untouched on a no-op');
  });

  // ---- 1b. merge with NO active session is also a no-op ----
  it('MERGE with no active session is an informational no-op', async () => {
    const created = await enterWorktreeTool.execute({ name: 'orphan-merge' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;
    setWorktreeSession(null);

    const r = await exitWorktreeTool.execute({ action: 'merge' }, ctx({ cwd: wt }));
    assert.equal(r.isError, false, r.output);
    assert.match(r.output, /no active enter_worktree session/i);
    assert.equal(fs.existsSync(wt), true, 'worktree preserved');
  });

  // ---- 1c. foreign-session remove (session owns a different worktree) no-op ----
  it('REMOVE is a no-op when the active session owns a DIFFERENT worktree', async () => {
    const created = await enterWorktreeTool.execute({ name: 'owned' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const ownedWt = getCurrentWorktreeSession()!.worktreePath;

    // Create a second, foreign worktree on disk but keep the session pointing at
    // the first one.
    const session = getCurrentWorktreeSession()!;
    const foreignWt = path.join(repo, '.ami', 'worktrees', 'foreign');
    git(repo, 'worktree', 'add', '-q', '-b', 'worktree-foreign', foreignWt);
    setWorktreeSession(session); // still owns `owned`, not `foreign`

    const r = await exitWorktreeTool.execute({ action: 'remove', discard_changes: true }, ctx({ cwd: foreignWt }));
    assert.equal(r.isError, false, r.output);
    assert.match(r.output, /not created by the active session/i);
    assert.equal(fs.existsSync(foreignWt), true, 'foreign worktree untouched');
    assert.equal(fs.existsSync(ownedWt), true, 'owned worktree untouched');
  });

  // ---- 2. remove restores the real process working directory ----
  it('REMOVE restores the process working directory to the original cwd', async () => {
    const created = await enterWorktreeTool.execute({ name: 'chdir-rm' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;

    // Put the real process cwd INSIDE the worktree, as a session physically in the
    // worktree would be; the remove deletes it out from under us.
    process.chdir(wt);

    const r = await exitWorktreeTool.execute({ action: 'remove', discard_changes: true }, ctx({ cwd: wt }));
    assert.equal(r.isError, undefined, r.output);
    assert.equal(fs.existsSync(wt), false, 'worktree dir removed');
    // RED (pre-fix): the remove branch only reassigned context.cwd and never
    // chdir'd, so process.cwd() still pointed at the now-deleted worktree path
    // (or threw ENOENT). GREEN: the process is back in the original repo.
    assert.equal(fs.realpathSync(process.cwd()), fs.realpathSync(repo));
    assert.match(r.output, /Returned to/);
  });
});
