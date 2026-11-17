/**
 * U064 (w72) — exit_worktree parity hardening, additive behaviors.
 *
 * Covers confirmed parity gaps against Claude's ExitWorktree/WorktreeExitDialog
 * that AMI's exit_worktree was missing:
 *
 *   1. CWD-dependent cache invalidation on exit: enter_worktree fires
 *      executeCwdChanged on entry (cwd-reactive cache invalidation), but exit was
 *      asymmetric and never fired it — so system-prompt env sections / session-env
 *      cache populated inside the worktree could leak into the restored session.
 *      Now BOTH keep and remove fire executeCwdChanged(worktreePath → originalCwd).
 *   2. Reliable process working-directory restore on remove: the keep branch did
 *      process.chdir(originalCwd) but the remove branch only reassigned context.cwd,
 *      leaving the real process cwd pointing at the just-deleted worktree dir.
 *      Now remove also restores the process cwd.
 *   3. Lock-release delay before deleting the throwaway worktree branch: Claude
 *      sleeps ~100ms so git releases the worktree lock before `git branch -D`,
 *      avoiding a "cannot delete, locked" race. AMI now does the same (seam-
 *      configurable so tests stay fast).
 *   4. Behavior documentation in the model-facing description (cwd restore, cache
 *      clearing, re-callable).
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
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'u064w72-')));
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

type CwdCall = { oldCwd: string; newCwd: string };
function hookCtx(over: Partial<ToolContext>, calls: CwdCall[]): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _hookManager: {
      executeCwdChanged: async (c: CwdCall) => { calls.push(c); },
      executeWorktreeRemove: async () => {},
    },
    ...over,
  } as unknown as ToolContext;
}

describe('exit_worktree U064 w72 — cwd-cache + chdir + branch lock-delay', () => {
  let repo: string;
  const launchCwd = process.cwd();
  const origDelay = _exitWorktreeHooks.branchDeleteDelayMs;
  const origSleep = _exitWorktreeHooks.sleep;

  beforeEach(() => { repo = makeRepo(); setWorktreeSession(null); });
  afterEach(() => {
    try { process.chdir(launchCwd); } catch { /* ignore */ }
    fs.rmSync(repo, { recursive: true, force: true });
    setWorktreeSession(null);
    _exitWorktreeHooks.branchDeleteDelayMs = origDelay;
    _exitWorktreeHooks.sleep = origSleep;
  });

  it('fires executeCwdChanged(worktreePath → originalCwd) on KEEP', async () => {
    const calls: CwdCall[] = [];
    const created = await enterWorktreeTool.execute({ name: 'keepcache' }, hookCtx({ cwd: repo }, calls));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;
    calls.length = 0; // ignore the enter-time call

    const r = await exitWorktreeTool.execute({ action: 'keep' }, hookCtx({ cwd: wt }, calls));
    assert.equal(r.isError, undefined, r.output);
    // RED (pre-fix): keep never fired executeCwdChanged, so calls stayed empty.
    assert.equal(calls.length, 1, 'keep must fire exactly one executeCwdChanged');
    assert.equal(path.resolve(calls[0].oldCwd), path.resolve(wt));
    assert.equal(path.resolve(calls[0].newCwd), path.resolve(repo));
  });

  it('fires executeCwdChanged(worktreePath → originalCwd) on REMOVE', async () => {
    _exitWorktreeHooks.branchDeleteDelayMs = 0; // keep the test fast
    const calls: CwdCall[] = [];
    const created = await enterWorktreeTool.execute({ name: 'rmcache' }, hookCtx({ cwd: repo }, calls));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;
    calls.length = 0;

    const r = await exitWorktreeTool.execute({ action: 'remove', discard_changes: true }, hookCtx({ cwd: wt }, calls));
    assert.equal(r.isError, undefined, r.output);
    assert.equal(fs.existsSync(wt), false, 'worktree dir removed');
    // RED (pre-fix): remove never fired executeCwdChanged.
    assert.equal(calls.length, 1, 'remove must fire exactly one executeCwdChanged');
    assert.equal(path.resolve(calls[0].oldCwd), path.resolve(wt));
    assert.equal(path.resolve(calls[0].newCwd), path.resolve(repo));
  });

  it('delays (lock-release) before deleting the throwaway worktree branch on REMOVE', async () => {
    const sleeps: number[] = [];
    _exitWorktreeHooks.branchDeleteDelayMs = 42;
    _exitWorktreeHooks.sleep = async (ms: number) => { sleeps.push(ms); };
    const calls: CwdCall[] = [];
    const created = await enterWorktreeTool.execute({ name: 'branchdel' }, hookCtx({ cwd: repo }, calls));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;
    const branch = git(wt, 'rev-parse', '--abbrev-ref', 'HEAD');
    assert.ok(branch && branch !== 'HEAD', 'worktree is on a named branch');

    const r = await exitWorktreeTool.execute({ action: 'remove', discard_changes: true }, hookCtx({ cwd: wt }, calls));
    assert.equal(r.isError, undefined, r.output);
    // RED (pre-fix): no lock-release delay was ever awaited.
    assert.deepEqual(sleeps, [42], 'exactly one lock-release delay before branch -D');
    // The throwaway branch is gone from the main repo ref namespace.
    const branches = git(repo, 'branch', '--list', branch);
    assert.equal(branches, '', `branch ${branch} should be deleted, got: ${branches}`);
  });

  it('documents cwd-restore, cache clearing and re-callable behavior in the description', () => {
    const d = exitWorktreeTool.description;
    // RED (pre-fix): the description had no behavior note about cache clearing.
    assert.match(d, /cache/i);
    assert.match(d, /original working directory/i);
    assert.match(d, /enter_worktree/);
  });
});
