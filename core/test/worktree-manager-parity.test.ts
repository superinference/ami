import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import {
  isValidGitSha,
  isSafeRefName,
  resolveMainRepoRoot,
  countWorktreeChanges,
  cleanupStaleWorktrees,
  bumpWorktreeMtime,
  listWorktrees,
  symlinkLargeDirectories,
  copyWorktreeIncludes,
  createWorktreeSession,
  setWorktreeSession,
  getCurrentWorktreeSession,
} from '../src/worktree-manager';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, { cwd, encoding: 'utf-8' }).trim();
}
function makeRepo(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-parity-'));
  git(dir, 'init', '-q');
  git(dir, 'config', 'user.email', 't@t.com');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  fs.writeFileSync(path.join(dir, 'README.md'), '# repo\n');
  git(dir, 'add', 'README.md');
  git(dir, 'commit', '-q', '-m', 'init');
  return dir;
}

describe('isValidGitSha', () => {
  it('accepts 40-hex and 64-hex, rejects others', () => {
    assert.equal(isValidGitSha('a'.repeat(40)), true);
    assert.equal(isValidGitSha('0'.repeat(64)), true);
    assert.equal(isValidGitSha('xyz'), false);
    assert.equal(isValidGitSha('A'.repeat(40)), false); // uppercase not hex-lower
    assert.equal(isValidGitSha('a'.repeat(39)), false);
  });
});

describe('isSafeRefName', () => {
  it('accepts normal branch names', () => {
    assert.equal(isSafeRefName('worktree-feature+x'), true);
    assert.equal(isSafeRefName('feature/thing-1'), true);
  });
  it('rejects dangerous or malformed refs', () => {
    assert.equal(isSafeRefName(''), false);
    assert.equal(isSafeRefName('a b'), false);
    assert.equal(isSafeRefName('a..b'), false);
    assert.equal(isSafeRefName('a~b'), false);
    assert.equal(isSafeRefName('-startdash'), false);
    assert.equal(isSafeRefName('/leadslash'), false);
    assert.equal(isSafeRefName('trailslash/'), false);
    assert.equal(isSafeRefName('ends.lock'), false);
    assert.equal(isSafeRefName('has@{reflog'), false);
    assert.equal(isSafeRefName('glob*'), false);
  });
});

describe('resolveMainRepoRoot', () => {
  let repo: string;
  beforeEach(() => { repo = makeRepo(); });
  afterEach(() => { fs.rmSync(repo, { recursive: true, force: true }); });

  it('returns the repo root for a normal repo', () => {
    const root = resolveMainRepoRoot(repo);
    assert.equal(fs.realpathSync(root), fs.realpathSync(repo));
  });
  it('resolves to the MAIN repo root from inside a linked worktree', () => {
    const wtDir = path.join(repo, '.ami', 'worktrees', 'child');
    fs.mkdirSync(path.dirname(wtDir), { recursive: true });
    git(repo, 'worktree', 'add', '-q', '-B', 'child', wtDir, 'HEAD');
    const root = resolveMainRepoRoot(wtDir);
    assert.equal(fs.realpathSync(root), fs.realpathSync(repo));
  });
  it('falls back to cwd for a non-git directory', () => {
    const plain = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-plain-'));
    try {
      assert.equal(resolveMainRepoRoot(plain), plain);
    } finally {
      fs.rmSync(plain, { recursive: true, force: true });
    }
  });
});

describe('countWorktreeChanges', () => {
  let repo: string;
  beforeEach(() => { repo = makeRepo(); });
  afterEach(() => { fs.rmSync(repo, { recursive: true, force: true }); });

  it('returns 0/0 for a clean repo', () => {
    const c = countWorktreeChanges(repo, repo);
    assert.deepEqual(c, { uncommitted: 0, unpushed: 1 }); // the init commit is "unpushed" (no remote)
  });
  it('counts uncommitted files', () => {
    fs.writeFileSync(path.join(repo, 'dirty.txt'), 'x');
    const c = countWorktreeChanges(repo, repo);
    assert.ok(c!.uncommitted >= 1);
  });
  it('ignoreUntracked excludes untracked files', () => {
    fs.writeFileSync(path.join(repo, 'untracked.txt'), 'x');
    const c = countWorktreeChanges(repo, repo, { ignoreUntracked: true });
    assert.equal(c!.uncommitted, 0);
  });
  it('returns null (fail-closed) on a non-git path', () => {
    const plain = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-nogit-'));
    try {
      assert.equal(countWorktreeChanges(plain, plain), null);
    } finally {
      fs.rmSync(plain, { recursive: true, force: true });
    }
  });
});

describe('stale cleanup skips the active session worktree', () => {
  let repo: string;
  beforeEach(() => { repo = makeRepo(); });
  afterEach(() => { fs.rmSync(repo, { recursive: true, force: true }); setWorktreeSession(null); });

  it('never reaps the active worktree even when old and clean', () => {
    const session = createWorktreeSession(repo, 'active-wt');
    setWorktreeSession(session);
    // Backdate its mtime far beyond the threshold.
    const old = Date.now() - 999 * 86400000;
    fs.utimesSync(session.worktreePath, new Date(old), new Date(old));
    const cleaned = cleanupStaleWorktrees(repo, 1);
    assert.ok(!cleaned.includes('active-wt'), 'active worktree must be skipped');
    assert.ok(fs.existsSync(session.worktreePath));
  });
});

describe('bumpWorktreeMtime', () => {
  it('advances the directory mtime', () => {
    const d = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-mtime-'));
    try {
      const old = Date.now() - 100000;
      fs.utimesSync(d, new Date(old), new Date(old));
      bumpWorktreeMtime(d);
      assert.ok(fs.statSync(d).mtimeMs > old);
    } finally {
      fs.rmSync(d, { recursive: true, force: true });
    }
  });
  it('is a no-op on a missing path (no throw)', () => {
    assert.doesNotThrow(() => bumpWorktreeMtime('/no/such/dir/xyz'));
  });
});

describe('listWorktrees', () => {
  let repo: string;
  beforeEach(() => { repo = makeRepo(); });
  afterEach(() => { fs.rmSync(repo, { recursive: true, force: true }); });

  it('enumerates worktrees current-first', () => {
    const wtDir = path.join(repo, '.ami', 'worktrees', 'wt2');
    fs.mkdirSync(path.dirname(wtDir), { recursive: true });
    git(repo, 'worktree', 'add', '-q', '-B', 'wt2', wtDir, 'HEAD');
    const list = listWorktrees(repo);
    assert.ok(list.length >= 2);
    assert.equal(list[0].isCurrent, true);
  });
  it('returns [] for a non-git directory', () => {
    const plain = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-list-plain-'));
    try {
      assert.deepEqual(listWorktrees(plain), []);
    } finally {
      fs.rmSync(plain, { recursive: true, force: true });
    }
  });
});

describe('path-traversal guards', () => {
  it('symlinkLargeDirectories refuses a traversing dir name', () => {
    const src = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-sym-src-'));
    const wt = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-sym-wt-'));
    try {
      // Should not create a symlink that escapes the worktree.
      symlinkLargeDirectories(src, wt, ['../escape']);
      assert.equal(fs.existsSync(path.join(wt, '..', 'escape')), false);
    } finally {
      fs.rmSync(src, { recursive: true, force: true });
      fs.rmSync(wt, { recursive: true, force: true });
    }
  });

  it('copyWorktreeIncludes skips a tracked file and copies an untracked one', () => {
    const repo = makeRepo();
    try {
      // tracked file
      fs.writeFileSync(path.join(repo, 'tracked.txt'), 'T');
      git(repo, 'add', 'tracked.txt');
      git(repo, 'commit', '-q', '-m', 'add tracked');
      // untracked file
      fs.writeFileSync(path.join(repo, 'local.env'), 'SECRET=1');
      fs.writeFileSync(path.join(repo, '.worktreeinclude'), 'tracked.txt\nlocal.env\n../escape\n');
      const wt = path.join(repo, 'wt');
      fs.mkdirSync(wt, { recursive: true });
      const copied = copyWorktreeIncludes(repo, wt);
      assert.ok(copied.includes('local.env'), 'untracked file copied');
      assert.ok(!copied.includes('tracked.txt'), 'tracked file skipped');
      assert.ok(!copied.includes('../escape'), 'escaping entry skipped');
      assert.equal(fs.existsSync(path.join(wt, 'local.env')), true);
    } finally {
      fs.rmSync(repo, { recursive: true, force: true });
    }
  });
});

describe('createWorktreeSession canonical root + teardown', () => {
  let repo: string;
  beforeEach(() => { repo = makeRepo(); });
  afterEach(() => { fs.rmSync(repo, { recursive: true, force: true }); setWorktreeSession(null); });

  it('creates a worktree and records the repo root', () => {
    const s = createWorktreeSession(repo, 'feature-x');
    assert.equal(fs.realpathSync(s.originalCwd), fs.realpathSync(repo));
    assert.ok(fs.existsSync(s.worktreePath));
    assert.equal(s.worktreeBranch, 'worktree-feature-x');
    assert.ok(getCurrentWorktreeSession() === null); // createWorktreeSession does not set the global
  });
});
