/**
 * U064 (w75) — exit_worktree parity hardening, additive behaviors.
 *
 * Confirmed parity gaps against the reference's ExitWorktree / WorktreeExitDialog that
 * AMI's exit_worktree was missing or only partially covering:
 *
 *   1. SESSION-OWNERSHIP SCOPE GUARD (gaps "no-op safety boundary" / "session
 *      ownership" / "session-scoped no-op" / "non-session no-op"): Claude only
 *      acts on a worktree created by enter_worktree IN THIS SESSION; with no
 *      active session a destructive remove/merge is a clean informational no-op
 *      (filesystem unchanged). AMI previously gated the destructive path purely
 *      on the `.ami/worktrees/` substring, so it would force-remove a leftover
 *      worktree no active session owns. Now remove/merge require an owning
 *      session; otherwise they no-op.
 *   2. RUN `git worktree remove` FROM THE REPO ROOT (gap "run remove from repo
 *      root"): never from inside the directory being deleted.
 *   3. RELIABLY RESTORE THE PROCESS CWD ON REMOVE (gap "reliably restore cwd on
 *      remove"): the remove branch now chdir's the process back to originalCwd,
 *      matching the keep branch, so later relative git/fs ops don't resolve
 *      against a deleted path.
 *   4. STRUCTURED RESULT METADATA (gap "no declared structured outputSchema"):
 *      the declared outputSchema fields (action / originalCwd / worktreePath /
 *      worktreeBranch / discardedFiles / discardedCommits / message) are now
 *      populated on `metadata`, not just the free-text output.
 *   5. GRACEFUL DEGRADED EXIT (gap "graceful degraded-exit messaging"): an
 *      unrecoverable teardown failure returns a non-error "exiting anyway"
 *      result instead of trapping the session.
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
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
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'u064w75-')));
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

describe('exit_worktree U064 w75 — scope guard, repo-root remove, cwd restore, metadata', () => {
  let repo: string;
  const launchCwd = process.cwd();
  const origDelay = _exitWorktreeHooks.branchDeleteDelayMs;

  beforeEach(() => { repo = makeRepo(); setWorktreeSession(null); _exitWorktreeHooks.branchDeleteDelayMs = 0; });
  afterEach(() => {
    mock.restoreAll();
    try { process.chdir(launchCwd); } catch { /* ignore */ }
    try { git(repo, 'worktree', 'prune'); } catch { /* ignore */ }
    fs.rmSync(repo, { recursive: true, force: true });
    setWorktreeSession(null);
    _exitWorktreeHooks.branchDeleteDelayMs = origDelay;
  });

  // ---- 1. session-ownership scope guard on the destructive path ----
  it('no-ops a REMOVE when no active session owns the worktree (filesystem unchanged)', async () => {
    const created = await enterWorktreeTool.execute({ name: 'leftover' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;
    // Simulate a worktree left over from a previous session: clear the session but
    // leave the worktree on disk, then try to remove it from inside.
    setWorktreeSession(null);

    const r = await exitWorktreeTool.execute({ action: 'remove', discard_changes: true }, ctx({ cwd: wt }));
    // RED (pre-fix): AMI force-removed ANY `.ami/worktrees/*` path regardless of
    // session ownership, so the dir was deleted and isError was undefined with a
    // "Worktree removed" message.
    assert.equal(r.isError, false, r.output);
    assert.match(r.output, /No active worktree session created by enter_worktree/);
    assert.equal(fs.existsSync(wt), true, 'un-owned worktree must be left untouched');
  });

  it('no-ops a MERGE when no active session owns the worktree', async () => {
    const created = await enterWorktreeTool.execute({ name: 'leftover-merge' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;
    setWorktreeSession(null);

    const r = await exitWorktreeTool.execute({ action: 'merge' }, ctx({ cwd: wt }));
    assert.equal(r.isError, false, r.output);
    assert.match(r.output, /No active worktree session/);
    assert.equal(fs.existsSync(wt), true, 'un-owned worktree must be left untouched');
  });

  // ---- 2. `git worktree remove` runs from the repo root, not the doomed dir ----
  it('runs `git worktree remove` with cwd = repo root (not inside the worktree)', async () => {
    const created = await enterWorktreeTool.execute({ name: 'reporoot' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;
    const expectedRoot = wt.split('.ami/worktrees/')[0];

    const seen: { args: string[]; cwd: string }[] = [];
    mock.method(_exitWorktreeHooks, 'exec', ((args: string[], opts: { cwd: string }) => {
      seen.push({ args, cwd: opts.cwd });
      return '';
    }) as typeof _exitWorktreeHooks.exec);

    const r = await exitWorktreeTool.execute({ action: 'remove', discard_changes: true }, ctx({ cwd: wt }));
    assert.equal(r.isError, undefined, r.output);
    const removeCall = seen.find(c => c.args[0] === 'worktree' && c.args[1] === 'remove');
    assert.ok(removeCall, 'expected a `git worktree remove` call');
    // RED (pre-fix): the primary removal ran with cwd = worktreePath (the dir being
    // deleted), not the repo root.
    assert.equal(removeCall!.cwd, expectedRoot);
  });

  // ---- 3. process cwd is restored on a real remove ----
  it('restores the PROCESS working directory to originalCwd after a real remove', async () => {
    const created = await enterWorktreeTool.execute({ name: 'cwdrestore' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const session = getCurrentWorktreeSession()!;
    const wt = session.worktreePath;
    // Move the process into the worktree, as a real session would be.
    process.chdir(wt);

    const r = await exitWorktreeTool.execute({ action: 'remove', discard_changes: true }, ctx({ cwd: wt }));
    assert.equal(r.isError, undefined, r.output);
    assert.equal(fs.existsSync(wt), false, 'worktree removed');
    // RED (pre-fix): remove only reassigned context.cwd, leaving process.cwd() at
    // the now-deleted worktree path (process.cwd() would throw / be stale).
    assert.equal(fs.realpathSync(process.cwd()), fs.realpathSync(session.originalCwd));
  });

  // ---- 4. structured result metadata (parity: outputSchema fields populated) ----
  it('populates structured metadata on a forced remove with discard counts', async () => {
    const created = await enterWorktreeTool.execute({ name: 'meta-remove' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const session = getCurrentWorktreeSession()!;
    const wt = session.worktreePath;
    // One committed (unpushed vs base) + one uncommitted change to get non-zero counts.
    fs.writeFileSync(path.join(wt, 'committed.txt'), 'c\n');
    git(wt, 'add', 'committed.txt'); git(wt, 'commit', '-q', '-m', 'work');
    fs.writeFileSync(path.join(wt, 'dirty.txt'), 'wip\n');

    const r = await exitWorktreeTool.execute({ action: 'remove', discard_changes: true }, ctx({ cwd: wt }));
    assert.equal(r.isError, undefined, r.output);
    // RED (pre-fix): remove returned only { output }, metadata was undefined.
    assert.ok(r.metadata, 'remove result must carry structured metadata');
    const m = r.metadata as Record<string, unknown>;
    assert.equal(m.action, 'remove');
    // The worktree dir is gone now — compare resolved path strings, don't lstat it.
    assert.equal(path.resolve(m.worktreePath as string), path.resolve(wt));
    assert.equal(path.resolve(m.originalCwd as string), path.resolve(session.originalCwd));
    assert.equal(m.discardedFiles, 1);
    assert.equal(m.discardedCommits, 1);
    assert.equal(typeof m.message, 'string');
  });

  it('populates structured metadata on keep (nothing discarded)', async () => {
    const created = await enterWorktreeTool.execute({ name: 'meta-keep' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;

    const r = await exitWorktreeTool.execute({ action: 'keep' }, ctx({ cwd: wt }));
    assert.equal(r.isError, undefined, r.output);
    assert.ok(r.metadata, 'keep result must carry structured metadata');
    const m = r.metadata as Record<string, unknown>;
    assert.equal(m.action, 'keep');
    assert.equal(m.discardedFiles, 0);
    assert.equal(m.discardedCommits, 0);
    assert.equal(fs.realpathSync(m.worktreePath as string), fs.realpathSync(wt));
  });

  // ---- 5. graceful degraded exit when teardown is unrecoverable ----
  it('exits anyway (non-error) when both worktree remove and the fallback fail', async () => {
    const created = await enterWorktreeTool.execute({ name: 'softfail' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;

    mock.method(_exitWorktreeHooks, 'exec', ((args: string[]) => {
      if (/worktree remove/.test(args.join(' '))) throw new Error('remove refused');
      throw new Error('prune also failed');
    }) as typeof _exitWorktreeHooks.exec);
    mock.method(_exitWorktreeHooks, 'existsSync', (() => false) as typeof fs.existsSync);

    const r = await exitWorktreeTool.execute({ action: 'remove', discard_changes: true }, ctx({ cwd: wt }));
    // RED (pre-fix): an unrecoverable teardown returned isError:true "Error
    // removing worktree", trapping the session.
    assert.equal(r.isError, undefined, r.output);
    assert.match(r.output, /exiting anyway/);
    assert.equal((r.metadata as { cleanupFailed?: boolean }).cleanupFailed, true);
  });
});
