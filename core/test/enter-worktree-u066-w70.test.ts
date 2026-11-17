import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { enterWorktreeTool } from '../src/tools/enter-worktree';
import { setWorktreeSession } from '../src/worktree-manager';
import { computeDefaultBranch } from '../src/utils/git';
import { HookManager } from '../src/hooks';
import type { ToolContext } from '../src/types';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, { cwd, encoding: 'utf-8' }).trim();
}

function makeRepo(initArgs: string[] = ['init', '-q']): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ew66w70-'));
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

describe('enter_worktree — U066 w70 parity', () => {
  const repos: string[] = [];
  beforeEach(() => { setWorktreeSession(null); });
  afterEach(() => {
    for (const r of repos.splice(0)) fs.rmSync(r, { recursive: true, force: true });
    setWorktreeSession(null);
  });
  const track = (r: string) => { repos.push(r); return r; };

  // --- GAP: session-exit keep/remove affordance advertised in the description ---
  // RED (revert the description addition): the enter_worktree description never
  //   mentions what happens on session exit while still inside the worktree.
  // GREEN: it advertises the keep/remove prompt + data-loss warning (parity with
  //   Claude's EnterWorktreeTool prompt: "On session exit, if still in the
  //   worktree, the user will be prompted to keep or remove it").
  it('description advertises the session-exit keep/remove affordance', () => {
    const d = enterWorktreeTool.description;
    assert.match(d, /session exit/i);
    assert.match(d, /keep or remove/i);
    assert.match(d, /uncommitted|unpushed/i);
  });

  // --- GAP: computeDefaultBranch is the one shared default-branch algorithm ------
  // getDefaultBranch (consumed by the worktree base selection) must still resolve
  // correctly after being consolidated onto computeDefaultBranch.
  it('bases a worktree on the default branch via the consolidated helper', async () => {
    const repo = track(makeRepo(['init', '-b', 'trunk', '-q']));
    const r = await enterWorktreeTool.execute({ name: 'wt-default' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    // No main/master/remote → final 'main' fallback (not a 'master' guess).
    assert.match(r.output, /based on origin\/main/);
  });

  it('computeDefaultBranch probes main then master, falling back to main', () => {
    // symref present → use it.
    assert.equal(
      computeDefaultBranch('/x', (args) => {
        if (args[0] === 'symbolic-ref') return 'origin/trunk';
        throw new Error('nope');
      }),
      'trunk',
    );
    // No symref, local master exists (main does not) → master.
    assert.equal(
      computeDefaultBranch('/x', (args) => {
        if (args[0] === 'symbolic-ref') throw new Error('no symref');
        if (args[0] === 'rev-parse' && args[2] === 'refs/heads/main') throw new Error('no main');
        if (args[0] === 'rev-parse' && args[2] === 'refs/heads/master') return '';
        throw new Error('nope');
      }),
      'master',
    );
    // Nothing resolves → 'main' fallback (never a bare 'master' guess).
    assert.equal(computeDefaultBranch('/x', () => { throw new Error('all fail'); }), 'main');
  });

  // --- GAP: executeCwdChanged no-ops when the cwd did not actually change --------
  // RED (revert the oldCwd===newCwd guard): a same-dir "change" still fires the
  //   CwdChanged hooks (and clears the session-env cache).
  // GREEN: a same-dir call short-circuits before running any hook.
  it('executeCwdChanged short-circuits when oldCwd === newCwd', async () => {
    const hm = new HookManager();
    let calls = 0;
    hm.onCwdChanged(async () => { calls++; });

    await hm.executeCwdChanged({ oldCwd: '/same', newCwd: '/same' });
    assert.equal(calls, 0, 'same-dir change must not fire CwdChanged hooks');

    await hm.executeCwdChanged({ oldCwd: '/old', newCwd: '/new' });
    assert.equal(calls, 1, 'a real cwd change must fire CwdChanged hooks');
  });
});
