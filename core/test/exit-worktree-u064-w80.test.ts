/**
 * U064 (w80) — exit_worktree NON-DESTRUCTIVE "stash" action.
 *
 * Confirmed parity gaps against the reference's TeleportStash / stashToCleanState that
 * AMI's exit_worktree was missing: a recoverable "stash changes and continue"
 * affordance as the non-destructive alternative to force-removal.
 *
 *   - RECOVERABLE STASH PATH (gap "Recoverable stash fallback before a destructive
 *     git operation" / "Recoverable 'stash changes and continue' affordance"):
 *     action:"stash" saves the worktree's uncommitted files (tracked + untracked)
 *     to a git stash — which survives the worktree's removal because refs/stash
 *     lives in the shared git dir — then removes the worktree. Nothing is lost;
 *     the work is recoverable from the main checkout with `git stash apply`.
 *   - It refuses on unpushed commits (a stash cannot preserve commits), staying
 *     strictly non-destructive, and fails closed (preserves the worktree) if the
 *     stash itself fails.
 *   - The remove-refusal message now surfaces action:"stash" as the recoverable
 *     alternative, and the schema advertises the action.
 *
 * RED (pre-fix): exit_worktree only knew keep/merge/remove, so action:"stash"
 * returned 'Error: action must be "keep", "merge", or "remove".' and no stash was
 * ever created; the remove-refusal message never mentioned stashing.
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { enterWorktreeTool } from '../src/tools/enter-worktree';
import { exitWorktreeTool, _exitWorktreeHooks } from '../src/tools/exit-worktree';
import { setWorktreeSession, getCurrentWorktreeSession, stashToCleanState } from '../src/worktree-manager';
import type { ToolContext } from '../src/types';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, { cwd, encoding: 'utf-8' }).trim();
}

function makeRepo(): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'u064w80-')));
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

describe('exit_worktree U064 w80 — recoverable "stash" action', () => {
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

  it('advertises the "stash" action in the input + output schema and description', () => {
    const inEnum = (exitWorktreeTool.inputSchema.properties as Record<string, { enum?: string[] }>).action.enum;
    assert.ok(inEnum?.includes('stash'), 'input action enum must include "stash"');
    const outEnum = (exitWorktreeTool.outputSchema?.properties as Record<string, { enum?: string[] }>).action.enum;
    assert.ok(outEnum?.includes('stash'), 'output action enum must include "stash"');
    assert.ok((exitWorktreeTool.outputSchema?.properties as Record<string, unknown>).stashedFiles, 'outputSchema must declare stashedFiles');
    assert.match(exitWorktreeTool.description, /stash/i);
  });

  it('stashes uncommitted (tracked + untracked) work, removes the worktree, and the stash is recoverable', async () => {
    const created = await enterWorktreeTool.execute({ name: 'stashme' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const session = getCurrentWorktreeSession()!;
    const wt = session.worktreePath;
    // A tracked modification + a brand-new untracked file (both must be preserved).
    fs.writeFileSync(path.join(wt, 'README.md'), '# r\nmodified\n');
    fs.writeFileSync(path.join(wt, 'brand-new.txt'), 'wip\n');

    const r = await exitWorktreeTool.execute({ action: 'stash' }, ctx({ cwd: wt }));
    // No discard_changes needed — stash is non-destructive.
    assert.equal(r.isError, undefined, r.output);
    assert.equal(fs.existsSync(wt), false, 'worktree removed after stash');
    assert.match(r.output, /git stash apply/);
    const m = r.metadata as Record<string, unknown>;
    assert.equal(m.action, 'stash');
    assert.equal(m.stashedFiles, 2);
    assert.equal(m.discardedFiles, 0);
    assert.equal(m.discardedCommits, 0);

    // The stash survives worktree removal (refs/stash is shared) and restores both files.
    const stashList = git(repo, 'stash', 'list');
    assert.match(stashList, /exit_worktree auto-stash/, 'a stash entry must exist in the main repo');
    git(repo, 'stash', 'apply');
    const status = git(repo, 'status', '--porcelain');
    assert.match(status, /README\.md/);
    assert.match(status, /brand-new\.txt/);
  });

  it('stash of a clean worktree just removes it (nothing to stash)', async () => {
    const created = await enterWorktreeTool.execute({ name: 'stashclean' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;

    const r = await exitWorktreeTool.execute({ action: 'stash' }, ctx({ cwd: wt }));
    assert.equal(r.isError, undefined, r.output);
    assert.equal(fs.existsSync(wt), false, 'clean worktree removed');
    assert.match(r.output, /no uncommitted changes to stash/);
    assert.equal((r.metadata as Record<string, unknown>).stashedFiles, 0);
    assert.equal(git(repo, 'stash', 'list'), '', 'no stash created for a clean worktree');
  });

  it('REFUSES stash when the worktree has unpushed commits (a stash cannot preserve commits)', async () => {
    const created = await enterWorktreeTool.execute({ name: 'stashcommits' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;
    fs.writeFileSync(path.join(wt, 'committed.txt'), 'c\n');
    git(wt, 'add', 'committed.txt'); git(wt, 'commit', '-q', '-m', 'work');

    const r = await exitWorktreeTool.execute({ action: 'stash' }, ctx({ cwd: wt }));
    assert.equal(r.isError, true, r.output);
    assert.match(r.output, /unpushed commit/);
    assert.match(r.output, /action:"merge"/);
    assert.equal(fs.existsSync(wt), true, 'worktree preserved when stash refused');
    assert.equal(git(repo, 'stash', 'list'), '', 'no stash created on refusal');
  });

  it('fails closed (preserves the worktree) when the stash operation itself fails', async () => {
    const created = await enterWorktreeTool.execute({ name: 'stashfail' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;
    fs.writeFileSync(path.join(wt, 'dirty.txt'), 'wip\n');

    mock.method(_exitWorktreeHooks, 'stash', (() => false) as typeof _exitWorktreeHooks.stash);
    const r = await exitWorktreeTool.execute({ action: 'stash' }, ctx({ cwd: wt }));
    assert.equal(r.isError, true, r.output);
    assert.match(r.output, /failed to stash/i);
    assert.equal(fs.existsSync(wt), true, 'worktree must be intact after a failed stash');
  });

  it('the remove-refusal message offers action:"stash" as the recoverable alternative', async () => {
    const created = await enterWorktreeTool.execute({ name: 'offerstash' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;
    fs.writeFileSync(path.join(wt, 'dirty.txt'), 'wip\n');

    const r = await exitWorktreeTool.execute({ action: 'remove' }, ctx({ cwd: wt }));
    assert.equal(r.isError, true, r.output);
    // RED (pre-fix): the refusal only mentioned discard_changes/merge/keep.
    assert.match(r.output, /action:"stash"/);
  });

  it('exit_worktree remains non-destructive for permission purposes on stash', () => {
    assert.equal(exitWorktreeTool.isDestructive?.({ action: 'stash' }), false);
    assert.equal(exitWorktreeTool.isDestructive?.({ action: 'remove' }), true);
  });

  // Direct unit coverage of the shared helper.
  it('stashToCleanState returns false for a clean tree and true after stashing a dirty one', async () => {
    const created = await enterWorktreeTool.execute({ name: 'helperchk' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    const wt = getCurrentWorktreeSession()!.worktreePath;
    assert.equal(stashToCleanState(wt, 'noop'), false, 'clean tree → nothing to stash');
    fs.writeFileSync(path.join(wt, 'x.txt'), 'y\n');
    assert.equal(stashToCleanState(wt, 'real'), true, 'dirty tree → stashed to clean state');
    assert.equal(git(wt, 'status', '--porcelain'), '', 'tree clean after stash');
  });
});
