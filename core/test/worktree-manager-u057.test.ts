import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execSync } from 'child_process';

import {
  countWorktreeChanges,
  setWorktreeSession,
  listWorktrees,
} from '../src/worktree-manager';
import { enterWorktreeTool } from '../src/tools/enter-worktree';
import { exitWorktreeTool } from '../src/tools/exit-worktree';
import type { ToolContext } from '../src/types';

const GIT_ENV = {
  GIT_TERMINAL_PROMPT: '0',
  GIT_ASKPASS: '',
  GIT_AUTHOR_NAME: 'Test',
  GIT_AUTHOR_EMAIL: 'test@test.com',
  GIT_COMMITTER_NAME: 'Test',
  GIT_COMMITTER_EMAIL: 'test@test.com',
};

function git(cmd: string, cwd: string): string {
  return execSync(`git ${cmd}`, {
    cwd,
    encoding: 'utf-8',
    env: { ...process.env, ...GIT_ENV },
  }).trim();
}

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

// exit_worktree's remove branch now chdir's the process back to originalCwd
// (parity gap 17); restore to a surviving dir before each temp repo is deleted.
const LAUNCH_CWD = process.cwd();

// ---------------------------------------------------------------------------
// U057 / GAP 14 — precise session-delta commit counting (originalHeadCommit..HEAD)
// ---------------------------------------------------------------------------

describe('U057 GAP14: countWorktreeChanges baseCommit (session-delta counting)', () => {
  let repo: string;

  beforeEach(() => {
    repo = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u057-g14-'));
    git('init -b main', repo);
    fs.writeFileSync(path.join(repo, 'a.txt'), '1');
    git('add a.txt', repo);
    git('commit -m c1', repo);
    fs.writeFileSync(path.join(repo, 'b.txt'), '2');
    git('add b.txt', repo);
    git('commit -m c2', repo);
  });

  afterEach(() => {
    try { git('worktree prune', repo); } catch { /* ignore */ }
    fs.rmSync(repo, { recursive: true, force: true });
    setWorktreeSession(null);
  });

  it('without baseCommit, local-only base commits are over-counted as unpushed (old behavior)', () => {
    const base = git('rev-parse HEAD', repo);
    assert.ok(/^[0-9a-f]{40}$/.test(base));
    const wt = path.join(repo, 'wt');
    git(`worktree add -b feat "${wt}" HEAD`, repo);

    // No new commits in the worktree, but there is NO remote, so the
    // remote-based count attributes the pre-existing base commits here.
    const legacy = countWorktreeChanges(wt, repo);
    assert.ok(legacy !== null);
    assert.ok(legacy!.unpushed > 0, `legacy over-counts (got ${legacy!.unpushed})`);

    git(`worktree remove --force "${wt}"`, repo);
  });

  it('with baseCommit, a worktree with no new commits reports 0 unpushed', () => {
    const base = git('rev-parse HEAD', repo);
    const wt = path.join(repo, 'wt');
    git(`worktree add -b feat "${wt}" HEAD`, repo);

    const precise = countWorktreeChanges(wt, repo, { baseCommit: base });
    assert.ok(precise !== null);
    assert.equal(precise!.unpushed, 0, 'session-delta count must be 0 for no new work');

    git(`worktree remove --force "${wt}"`, repo);
  });

  it('with baseCommit, counts exactly the commits created in the session', () => {
    const base = git('rev-parse HEAD', repo);
    const wt = path.join(repo, 'wt');
    git(`worktree add -b feat "${wt}" HEAD`, repo);
    fs.writeFileSync(path.join(wt, 'c.txt'), '3');
    git('add c.txt', wt);
    git('commit -m c3', wt);
    fs.writeFileSync(path.join(wt, 'd.txt'), '4');
    git('add d.txt', wt);
    git('commit -m c4', wt);

    const precise = countWorktreeChanges(wt, repo, { baseCommit: base });
    assert.ok(precise !== null);
    assert.equal(precise!.unpushed, 2, 'exactly the 2 new session commits');

    git(`worktree remove --force "${wt}"`, repo);
  });

  it('ignores a malformed baseCommit and falls back to the remote-based count', () => {
    const wt = path.join(repo, 'wt');
    git(`worktree add -b feat "${wt}" HEAD`, repo);
    const fallback = countWorktreeChanges(wt, repo, { baseCommit: 'not-a-sha' });
    const legacy = countWorktreeChanges(wt, repo);
    assert.deepEqual(fallback, legacy, 'malformed base must not change the count');
    git(`worktree remove --force "${wt}"`, repo);
  });
});

// ---------------------------------------------------------------------------
// U057 / GAP 14 — E2E through exit_worktree (remove must not refuse when the
// only "unpushed" commits are pre-existing base commits).
// ---------------------------------------------------------------------------

describe('U057 GAP14 E2E: exit_worktree remove uses session-delta counting', () => {
  let repo: string;

  beforeEach(() => {
    repo = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u057-g14e2e-'));
    git('init -b main', repo);
    fs.writeFileSync(path.join(repo, 'a.txt'), '1');
    git('add a.txt', repo);
    git('commit -m c1', repo);
    fs.writeFileSync(path.join(repo, 'b.txt'), '2');
    git('add b.txt', repo);
    git('commit -m c2', repo);
  });

  afterEach(() => {
    try { process.chdir(LAUNCH_CWD); } catch { /* ignore */ }
    try { git('worktree prune', repo); } catch { /* ignore */ }
    fs.rmSync(repo, { recursive: true, force: true });
    setWorktreeSession(null);
  });

  it('removes a clean worktree without demanding discard_changes', async () => {
    const base = git('rev-parse HEAD', repo);
    const wtDir = path.join(repo, '.ami', 'worktrees', 'wt');
    git(`worktree add -b worktree/wt "${wtDir}" HEAD`, repo);

    setWorktreeSession({
      originalCwd: repo,
      worktreePath: wtDir,
      worktreeName: 'wt',
      worktreeBranch: 'worktree/wt',
      originalBranch: 'main',
      originalHeadCommit: base,
      createdAt: new Date().toISOString(),
    });

    const r = await exitWorktreeTool.execute({ action: 'remove' }, ctx({ cwd: wtDir }));
    assert.ok(!r.isError, `remove should succeed, got: ${r.output}`);
    assert.match(r.output, /Worktree removed/);
    assert.equal(fs.existsSync(wtDir), false, 'worktree dir should be gone');
  });
});

// ---------------------------------------------------------------------------
// U057 / GAP 28 — list-worktrees enumeration affordance surfaced by enter_worktree
// ---------------------------------------------------------------------------

describe('U057 GAP28: enter_worktree surfaces available worktrees on an unregistered path', () => {
  let repo: string;

  beforeEach(() => {
    repo = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u057-g28-'));
    git('init -b main', repo);
    fs.writeFileSync(path.join(repo, 'a.txt'), '1');
    git('add a.txt', repo);
    git('commit -m c1', repo);
  });

  afterEach(() => {
    try { git('worktree prune', repo); } catch { /* ignore */ }
    fs.rmSync(repo, { recursive: true, force: true });
    setWorktreeSession(null);
  });

  it('lists the registered worktrees when the given path is not one', async () => {
    const registered = path.join(repo, 'wt-a');
    git(`worktree add -b feat-a "${registered}" HEAD`, repo);

    const plain = path.join(repo, 'plain');
    fs.mkdirSync(plain);

    const r = await enterWorktreeTool.execute({ path: plain }, ctx({ cwd: repo }));
    assert.equal(r.isError, true);
    assert.match(r.output, /not a registered worktree/);
    assert.match(r.output, /Available worktrees/);
    assert.ok(r.output.includes('wt-a'), `should list the registered worktree, got: ${r.output}`);

    git(`worktree remove --force "${registered}"`, repo);
  });

  it('listWorktrees enumerates repo worktrees current-first', () => {
    const registered = path.join(repo, 'wt-b');
    git(`worktree add -b feat-b "${registered}" HEAD`, repo);
    const all = listWorktrees(repo);
    assert.ok(all.length >= 2);
    assert.equal(all[0].isCurrent, true, 'current worktree is first');
    assert.ok(all.some(w => path.resolve(w.path) === fs.realpathSync(registered)
      || w.path.endsWith('wt-b')), 'registered worktree is enumerated');
    git(`worktree remove --force "${registered}"`, repo);
  });
});
