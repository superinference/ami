import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';

import {
  readGitBranchFromDisk,
  createWorktreeSession,
  setWorktreeSession,
  getCurrentWorktreeSession,
  removeSessionWorktreeIfClean,
  listWorktrees,
} from '../src/worktree-manager';
import { removeWorktreeIfClean } from '../src/tools/task';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, {
    cwd,
    encoding: 'utf-8',
    env: {
      ...process.env,
      GIT_TERMINAL_PROMPT: '0',
      GIT_AUTHOR_NAME: 'T', GIT_AUTHOR_EMAIL: 't@t.com',
      GIT_COMMITTER_NAME: 'T', GIT_COMMITTER_EMAIL: 't@t.com',
    },
  }).trim();
}

function makeRepo(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-u057w100-'));
  git(dir, 'init', '-q', '-b', 'main');
  git(dir, 'config', 'commit.gpgsign', 'false');
  fs.writeFileSync(path.join(dir, 'README.md'), '# r\n');
  git(dir, 'add', 'README.md');
  git(dir, 'commit', '-q', '-m', 'init');
  return dir;
}

const tmps: string[] = [];
afterEach(() => {
  setWorktreeSession(null);
  while (tmps.length) { try { fs.rmSync(tmps.pop()!, { recursive: true, force: true }); } catch { /* ignore */ } }
});

// ---------------------------------------------------------------------------
// G8 — readGitBranchFromDisk rejects a tampered/unsafe ref read off disk
// (defense-in-depth: a poisoned .git/HEAD must not reach the journal gitBranch).
// ---------------------------------------------------------------------------

describe('readGitBranchFromDisk — unsafe ref guard (U057-w100)', () => {
  it('returns the branch for a safe nested ref name', () => {
    const repo = makeRepo(); tmps.push(repo);
    git(repo, 'checkout', '-q', '-b', 'feature/x');
    assert.equal(readGitBranchFromDisk(repo), 'feature/x');
  });

  it('returns "" when HEAD points at a ref name with whitespace (tampered)', () => {
    const repo = makeRepo(); tmps.push(repo);
    // Force a read so the git dir is resolved+cached to <repo>/.git, then tamper.
    readGitBranchFromDisk(repo);
    fs.writeFileSync(path.join(repo, '.git', 'HEAD'), 'ref: refs/heads/evil branch\n');
    // RED discriminator: the naive extractor returns the raw unsafe name.
    const head = fs.readFileSync(path.join(repo, '.git', 'HEAD'), 'utf8').trim();
    const naive = /^ref:\s*refs\/heads\/(.+)$/.exec(head)?.[1] ?? '';
    assert.equal(naive, 'evil branch', 'naive extractor leaks the unsafe ref');
    // GREEN: the guarded reader rejects it.
    assert.equal(readGitBranchFromDisk(repo), '');
  });

  it('returns "" when HEAD contains a path-traversal ref (..)', () => {
    const repo = makeRepo(); tmps.push(repo);
    readGitBranchFromDisk(repo);
    fs.writeFileSync(path.join(repo, '.git', 'HEAD'), 'ref: refs/heads/../../etc/passwd\n');
    assert.equal(readGitBranchFromDisk(repo), '');
  });
});

// ---------------------------------------------------------------------------
// G5 — removeSessionWorktreeIfClean: eager, non-age-gated teardown of the
// current session's worktree, fail-closed on any uncommitted/unpushed work.
// ---------------------------------------------------------------------------

describe('removeSessionWorktreeIfClean — interrupt cleanup (U057-w100)', () => {
  it('removes a change-free session worktree and clears currentSession', () => {
    const repo = makeRepo(); tmps.push(repo);
    const session = createWorktreeSession(repo, 'wt-clean');
    setWorktreeSession(session);
    assert.ok(fs.existsSync(session.worktreePath));

    assert.equal(removeSessionWorktreeIfClean(), true);
    assert.equal(fs.existsSync(session.worktreePath), false, 'clean worktree dir removed');
    assert.equal(getCurrentWorktreeSession(), null, 'currentSession cleared after removal');
    // git no longer tracks it as a worktree.
    assert.ok(!listWorktrees(repo).some(w => path.resolve(w.path) === path.resolve(session.worktreePath)));
  });

  it('KEEPS a worktree with an uncommitted (untracked) file — never loses work', () => {
    const repo = makeRepo(); tmps.push(repo);
    const session = createWorktreeSession(repo, 'wt-dirty');
    setWorktreeSession(session);
    fs.writeFileSync(path.join(session.worktreePath, 'scratch.txt'), 'wip\n');

    assert.equal(removeSessionWorktreeIfClean(), false);
    assert.ok(fs.existsSync(session.worktreePath), 'dirty worktree preserved for review');
    assert.ok(getCurrentWorktreeSession(), 'currentSession retained when not removed');
  });

  it('KEEPS a worktree with an unpushed session commit (baseCommit delta)', () => {
    const repo = makeRepo(); tmps.push(repo);
    const session = createWorktreeSession(repo, 'wt-commit');
    setWorktreeSession(session);
    // A new commit in the worktree is unpushed work relative to the recorded base.
    fs.writeFileSync(path.join(session.worktreePath, 'f.txt'), 'x\n');
    git(session.worktreePath, 'add', 'f.txt');
    git(session.worktreePath, 'commit', '-q', '-m', 'work');

    assert.equal(removeSessionWorktreeIfClean(), false);
    assert.ok(fs.existsSync(session.worktreePath), 'worktree with unpushed commit preserved');
  });

  it('returns false and clears a stale session when the worktree dir is already gone', () => {
    const repo = makeRepo(); tmps.push(repo);
    const session = createWorktreeSession(repo, 'wt-gone');
    setWorktreeSession(session);
    git(repo, 'worktree', 'remove', '--force', session.worktreePath);
    fs.rmSync(session.worktreePath, { recursive: true, force: true });

    assert.equal(removeSessionWorktreeIfClean(), false);
    assert.equal(getCurrentWorktreeSession(), null, 'stale currentSession cleared');
  });

  it('no-ops with no active session', () => {
    setWorktreeSession(null);
    assert.equal(removeSessionWorktreeIfClean(), false);
  });
});

// ---------------------------------------------------------------------------
// G7 — removeWorktreeIfClean (task.ts) read-only reads use --no-optional-locks;
// behavior is preserved: clean removed, dirty kept, live-agent kept.
// ---------------------------------------------------------------------------

describe('removeWorktreeIfClean — behavior with --no-optional-locks reads (U057-w100)', () => {
  // A clean worktree is "removable" only when it has no uncommitted changes AND
  // no unpushed commits; with no remote every commit reads as unpushed, so these
  // tests push `main` to a bare origin first (mirrors the real agent flow where
  // the base ref is on a remote).
  function makeRepoWithRemote(): string {
    const repo = makeRepo();
    const bare = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-u057w100-bare-'));
    tmps.push(bare);
    git(bare, 'init', '-q', '--bare', '-b', 'main');
    git(repo, 'remote', 'add', 'origin', bare);
    git(repo, 'push', '-q', 'origin', 'main');
    return repo;
  }

  it('removes a clean agent worktree when no agent is running', async () => {
    const repo = makeRepoWithRemote(); tmps.push(repo);
    const session = createWorktreeSession(repo, 'agent-wt');
    assert.equal(await removeWorktreeIfClean(repo, session.worktreePath, 0), true);
    assert.equal(fs.existsSync(session.worktreePath), false);
  });

  it('does NOT remove a worktree with uncommitted changes', async () => {
    const repo = makeRepoWithRemote(); tmps.push(repo);
    const session = createWorktreeSession(repo, 'agent-dirty');
    fs.writeFileSync(path.join(session.worktreePath, 'wip.txt'), 'x\n');
    assert.equal(await removeWorktreeIfClean(repo, session.worktreePath, 0), false);
    assert.ok(fs.existsSync(session.worktreePath));
  });

  it('does NOT remove a clean worktree while an agent is still running', async () => {
    const repo = makeRepoWithRemote(); tmps.push(repo);
    const session = createWorktreeSession(repo, 'agent-live');
    assert.equal(await removeWorktreeIfClean(repo, session.worktreePath, 1), false);
    assert.ok(fs.existsSync(session.worktreePath));
  });
});
