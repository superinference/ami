/**
 * U064 (w83) — concurrency-safe cwd restore on worktree exit.
 *
 * Confirmed parity gap (medium): AMI runs subagents concurrently in ONE process
 * (task.ts), yet exit_worktree unconditionally called `process.chdir(originalCwd)`
 * on keep AND remove. Because `process.chdir` mutates the single GLOBAL process
 * cwd, one agent exiting a worktree shifted `process.cwd()` under every other
 * concurrently-running agent — a cross-agent isolation hazard (the whole point of
 * Claude's per-async-context cwd design, utils/cwd.ts).
 *
 * The fix mirrors Claude's `restoreSessionToOriginalCwd` guard (only restore the
 * working dir it actually changed — `projectRootIsWorktree`): AMI now touches the
 * GLOBAL process cwd ONLY when it is actually stranded inside the worktree being
 * torn down (the foreground/owning session that cd'd in). A concurrent agent whose
 * global cwd is elsewhere is left untouched. The per-call `context.cwd` is always
 * restored regardless, so the owning session still returns correctly.
 *
 * RED (pre-fix): the unconditional process.chdir yanked process.cwd() to the
 * session's originalCwd even when the global cwd was NOT inside the worktree, so
 * the "process cwd unchanged" assertions below failed.
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
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'u064w83-')));
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

const ctx = (over: Partial<ToolContext> = {}): ToolContext =>
  ({ cwd: process.cwd(), abortSignal: new AbortController().signal, ...over } as ToolContext);

describe('exit_worktree U064 w83 — concurrency-safe cwd restore', () => {
  let repo: string;
  let elsewhere: string; // a dir OUTSIDE the worktree, simulating a sibling agent's cwd
  const launchCwd = process.cwd();
  const origDelay = _exitWorktreeHooks.branchDeleteDelayMs;

  beforeEach(() => {
    repo = makeRepo();
    elsewhere = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'u064w83-else-')));
    _exitWorktreeHooks.branchDeleteDelayMs = 0;
    setWorktreeSession(null);
  });
  afterEach(() => {
    try { process.chdir(launchCwd); } catch { /* ignore */ }
    fs.rmSync(repo, { recursive: true, force: true });
    fs.rmSync(elsewhere, { recursive: true, force: true });
    setWorktreeSession(null);
    _exitWorktreeHooks.branchDeleteDelayMs = origDelay;
  });

  it('REMOVE: does NOT move the global process cwd when it is outside the worktree (concurrent agent)', async () => {
    const created = await enterWorktreeTool.execute({ name: 'conc-remove' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const session = getCurrentWorktreeSession()!;
    const wt = session.worktreePath;

    // Simulate a concurrent sibling agent: the GLOBAL process cwd points somewhere
    // unrelated, NOT inside this worktree.
    process.chdir(elsewhere);

    const callCtx = ctx({ cwd: wt });
    const r = await exitWorktreeTool.execute({ action: 'remove', discard_changes: true }, callCtx);
    assert.equal(r.isError, undefined, r.output);
    assert.equal(fs.existsSync(wt), false, 'worktree removed');

    // RED (pre-fix): unconditional process.chdir(originalCwd) moved the global cwd
    // to `repo`, contaminating the sibling agent.
    assert.equal(fs.realpathSync(process.cwd()), fs.realpathSync(elsewhere),
      'global process cwd must be left untouched for concurrent agents');
    // The per-call context cwd is still restored for the owning session.
    assert.equal(path.resolve(callCtx.cwd), path.resolve(session.originalCwd),
      'context.cwd must still be restored to originalCwd');
  });

  it('KEEP: does NOT move the global process cwd when it is outside the worktree (concurrent agent)', async () => {
    const created = await enterWorktreeTool.execute({ name: 'conc-keep' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const session = getCurrentWorktreeSession()!;
    const wt = session.worktreePath;

    process.chdir(elsewhere);

    const callCtx = ctx({ cwd: wt });
    const r = await exitWorktreeTool.execute({ action: 'keep' }, callCtx);
    assert.equal(r.isError, undefined, r.output);

    // RED (pre-fix): keep unconditionally chdir'd to originalCwd.
    assert.equal(fs.realpathSync(process.cwd()), fs.realpathSync(elsewhere),
      'global process cwd must be left untouched on keep for concurrent agents');
    assert.equal(path.resolve(callCtx.cwd), path.resolve(session.originalCwd),
      'context.cwd must still be restored to originalCwd on keep');
  });

  it('REMOVE: STILL rescues the global process cwd when it IS stranded inside the worktree (owning session)', async () => {
    const created = await enterWorktreeTool.execute({ name: 'owner-remove' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const session = getCurrentWorktreeSession()!;
    const wt = session.worktreePath;

    // Owning/foreground session: the global cwd is actually inside the worktree.
    process.chdir(wt);

    const r = await exitWorktreeTool.execute({ action: 'remove', discard_changes: true }, ctx({ cwd: wt }));
    assert.equal(r.isError, undefined, r.output);
    assert.equal(fs.existsSync(wt), false, 'worktree removed');
    // The stranded global cwd (pointing at the now-deleted dir) is rescued to originalCwd.
    assert.equal(fs.realpathSync(process.cwd()), fs.realpathSync(session.originalCwd),
      'global process cwd must be rescued when it was inside the removed worktree');
  });

  it('KEEP: STILL restores the global process cwd when it IS inside the worktree (owning session)', async () => {
    const created = await enterWorktreeTool.execute({ name: 'owner-keep' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const session = getCurrentWorktreeSession()!;
    const wt = session.worktreePath;

    process.chdir(wt);

    const r = await exitWorktreeTool.execute({ action: 'keep' }, ctx({ cwd: wt }));
    assert.equal(r.isError, undefined, r.output);
    assert.equal(fs.realpathSync(process.cwd()), fs.realpathSync(session.originalCwd),
      'global process cwd must be restored when it was inside the kept worktree');
  });
});
