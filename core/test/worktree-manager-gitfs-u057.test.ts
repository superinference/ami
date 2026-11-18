import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';

import {
  resolveGitDir,
  readGitBranchFromDisk,
  isClean,
  cleanupStaleWorktrees,
  ONE_DAY_MS,
} from '../src/worktree-manager';

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
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-gitfs-'));
  git(dir, 'init', '-q', '-b', 'main');
  git(dir, 'config', 'commit.gpgsign', 'false');
  fs.writeFileSync(path.join(dir, 'README.md'), '# r\n');
  git(dir, 'add', 'README.md');
  git(dir, 'commit', '-q', '-m', 'init');
  return dir;
}

// ---------------------------------------------------------------------------
// resolveGitDir — worktree/submodule `.git`-file (gitdir:) pointer resolution
// ---------------------------------------------------------------------------

describe('resolveGitDir', () => {
  const tmps: string[] = [];
  afterEach(() => { while (tmps.length) { try { fs.rmSync(tmps.pop()!, { recursive: true, force: true }); } catch { /* ignore */ } } });

  it('returns the .git DIRECTORY for a regular repository', () => {
    const repo = makeRepo(); tmps.push(repo);
    assert.equal(resolveGitDir(repo), path.join(repo, '.git'));
  });

  it('follows the gitdir: pointer when .git is a FILE (linked worktree)', () => {
    const repo = makeRepo(); tmps.push(repo);
    const wt = path.join(repo, '.ami', 'worktrees', 'w1');
    fs.mkdirSync(path.dirname(wt), { recursive: true });
    git(repo, 'worktree', 'add', '-q', wt, 'HEAD');
    // `.git` inside the worktree is a FILE, not a directory.
    assert.ok(fs.lstatSync(path.join(wt, '.git')).isFile());
    const resolved = resolveGitDir(wt);
    assert.ok(resolved, 'expected a resolved git dir for the worktree');
    // The resolved dir is the per-worktree admin dir under the main commondir.
    assert.ok(resolved!.includes(path.join('worktrees', 'w1')), `got ${resolved}`);
    assert.ok(fs.existsSync(path.join(resolved!, 'HEAD')));
  });

  it('returns null for a non-git directory', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-nogit-')); tmps.push(dir);
    assert.equal(resolveGitDir(dir), null);
  });

  it('returns null for a stale gitdir: pointer (target missing)', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-stale-')); tmps.push(dir);
    fs.writeFileSync(path.join(dir, '.git'), 'gitdir: /nonexistent/path/42\n');
    assert.equal(resolveGitDir(dir), null);
  });

  it('returns null for a .git FILE without a gitdir: prefix', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-bad-')); tmps.push(dir);
    fs.writeFileSync(path.join(dir, '.git'), 'garbage\n');
    assert.equal(resolveGitDir(dir), null);
  });

  it('memoizes per start path (cached result survives .git removal)', () => {
    const repo = makeRepo(); tmps.push(repo);
    const first = resolveGitDir(repo);
    // Delete .git; a non-memoized resolver would now return null. The cached
    // value proves the per-path memoization is active (parity: resolveGitDirCache).
    fs.rmSync(path.join(repo, '.git'), { recursive: true, force: true });
    assert.equal(resolveGitDir(repo), first);
  });
});

// ---------------------------------------------------------------------------
// readGitBranchFromDisk — subprocess-free branch read, worktree-correct
// ---------------------------------------------------------------------------

describe('readGitBranchFromDisk', () => {
  const tmps: string[] = [];
  afterEach(() => { while (tmps.length) { try { fs.rmSync(tmps.pop()!, { recursive: true, force: true }); } catch { /* ignore */ } } });

  it('reads the branch of a regular repository', () => {
    const repo = makeRepo(); tmps.push(repo);
    assert.equal(readGitBranchFromDisk(repo), 'main');
  });

  it('reads the real branch INSIDE a linked worktree (the .git-file bug)', () => {
    const repo = makeRepo(); tmps.push(repo);
    const wt = path.join(repo, '.ami', 'worktrees', 'feat');
    fs.mkdirSync(path.dirname(wt), { recursive: true });
    git(repo, 'worktree', 'add', '-q', '-b', 'feature-x', wt, 'HEAD');

    // RED discriminator: the OLD naive reader assumed `.git` is a directory.
    let naive = '';
    try {
      const head = fs.readFileSync(path.join(wt, '.git', 'HEAD'), 'utf8').trim();
      const m = /^ref:\s*refs\/heads\/(.+)$/.exec(head);
      naive = m ? m[1] : '';
    } catch { naive = ''; }
    assert.equal(naive, '', 'naive .git/HEAD read must fail in a worktree (records empty branch)');

    // GREEN: the gitdir-aware reader returns the worktree's actual branch.
    assert.equal(readGitBranchFromDisk(wt), 'feature-x');
  });

  it('returns empty string on a detached HEAD', () => {
    const repo = makeRepo(); tmps.push(repo);
    const sha = git(repo, 'rev-parse', 'HEAD');
    git(repo, 'checkout', '-q', '--detach', sha);
    assert.equal(readGitBranchFromDisk(repo), '');
  });

  it('returns empty string for a non-git directory', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-nb-')); tmps.push(dir);
    assert.equal(readGitBranchFromDisk(dir), '');
  });
});

// ---------------------------------------------------------------------------
// isClean — central cleanliness helper with ignoreUntracked (-uno)
// ---------------------------------------------------------------------------

describe('isClean', () => {
  const tmps: string[] = [];
  afterEach(() => { while (tmps.length) { try { fs.rmSync(tmps.pop()!, { recursive: true, force: true }); } catch { /* ignore */ } } });

  it('is true for a pristine tree', () => {
    const repo = makeRepo(); tmps.push(repo);
    assert.equal(isClean(repo), true);
  });

  it('is false when a tracked file is modified', () => {
    const repo = makeRepo(); tmps.push(repo);
    fs.writeFileSync(path.join(repo, 'README.md'), 'changed\n');
    assert.equal(isClean(repo), false);
    // ignoreUntracked must NOT mask a tracked modification.
    assert.equal(isClean(repo, { ignoreUntracked: true }), false);
  });

  it('treats an untracked-only tree as dirty by default but clean with ignoreUntracked', () => {
    const repo = makeRepo(); tmps.push(repo);
    fs.writeFileSync(path.join(repo, 'new.txt'), 'x\n');
    assert.equal(isClean(repo), false, 'untracked file counts as dirty by default');
    assert.equal(isClean(repo, { ignoreUntracked: true }), true, '-uno ignores the untracked file');
  });

  it('fails closed (false) for a non-git directory', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-ic-')); tmps.push(dir);
    assert.equal(isClean(dir), false);
  });
});

// ---------------------------------------------------------------------------
// cleanupStaleWorktrees — once-per-interval throttle + keep-newest-N retention
// ---------------------------------------------------------------------------

describe('cleanupStaleWorktrees throttle + retention', () => {
  let tmpDir: string;
  beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-throttle-')); });
  afterEach(() => { try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ } });

  it('ONE_DAY_MS is 24h', () => {
    assert.equal(ONE_DAY_MS, 24 * 60 * 60 * 1000);
  });

  const makeAgedDir = (name: string, ageDays: number): string => {
    const base = path.join(tmpDir, '.ami', 'worktrees');
    fs.mkdirSync(base, { recursive: true });
    const d = path.join(base, name);
    fs.mkdirSync(d, { recursive: true });
    const t = new Date(Date.now() - ageDays * ONE_DAY_MS);
    fs.utimesSync(d, t, t);
    return d;
  };

  it('writes a marker when throttled and SKIPS a second call within the interval', () => {
    makeAgedDir('plain', 0); // not aged enough to be reaped; just exercises the sweep
    const markerPath = path.join(tmpDir, '.ami', 'worktrees', '.last-cleanup');
    cleanupStaleWorktrees(tmpDir, 30, { throttle: true });
    assert.ok(fs.existsSync(markerPath), 'throttled sweep must write the marker');
    const firstMtime = fs.statSync(markerPath).mtimeMs;
    // A second throttled call within ONE_DAY_MS must be a no-op (marker untouched).
    const r2 = cleanupStaleWorktrees(tmpDir, 30, { throttle: true });
    assert.deepEqual(r2, { cleaned: [], failed: [] });
    assert.equal(fs.statSync(markerPath).mtimeMs, firstMtime, 'second call must not rewrite the marker');
  });

  it('does NOT write a marker when throttle is off (default behavior unchanged)', () => {
    makeAgedDir('plain', 0);
    cleanupStaleWorktrees(tmpDir);
    assert.equal(fs.existsSync(path.join(tmpDir, '.ami', 'worktrees', '.last-cleanup')), false);
  });

  // Build N real, clean, aged worktrees (0 uncommitted, 0 unpushed) so the sweep
  // would genuinely remove them — then retentionCount must protect the newest.
  const setupRealAgedWorktrees = (names: string[]): void => {
    const remote = path.join(tmpDir, 'remote.git');
    git(tmpDir, 'init', '--bare', '-q', remote);
    git(tmpDir, 'init', '-q', '-b', 'main');
    git(tmpDir, 'config', 'commit.gpgsign', 'false');
    git(tmpDir, 'remote', 'add', 'origin', remote);
    fs.writeFileSync(path.join(tmpDir, 'README.md'), 'init');
    git(tmpDir, 'add', 'README.md');
    git(tmpDir, 'commit', '-q', '-m', 'init');
    git(tmpDir, 'push', '-q', '-u', 'origin', 'HEAD:main');
    const base = path.join(tmpDir, '.ami', 'worktrees');
    fs.mkdirSync(base, { recursive: true });
    names.forEach((name, i) => {
      const wt = path.join(base, name);
      git(tmpDir, 'worktree', 'add', '-q', '--detach', wt, 'HEAD');
      // Age all past the 1-day cutoff; later names are NEWER (smaller age offset).
      const ageMs = (10 - i) * ONE_DAY_MS;
      const t = new Date(Date.now() - ageMs);
      fs.utimesSync(wt, t, t);
    });
  };

  it('keeps the newest-N worktrees regardless of age (retentionCount)', () => {
    setupRealAgedWorktrees(['old-a', 'new-b']); // new-b is the newest
    const result = cleanupStaleWorktrees(tmpDir, 1, { retentionCount: 1 });
    assert.deepEqual(result.failed, []);
    assert.ok(result.cleaned.includes('old-a'), `older worktree should be reaped, got ${JSON.stringify(result)}`);
    assert.ok(!result.cleaned.includes('new-b'), `newest worktree must be retained, got ${JSON.stringify(result)}`);
  });

  it('without retentionCount, an aged clean worktree IS reaped (discriminator)', () => {
    setupRealAgedWorktrees(['old-a', 'new-b']);
    const result = cleanupStaleWorktrees(tmpDir, 1);
    assert.deepEqual(result.failed, []);
    assert.ok(result.cleaned.includes('new-b'), `no retention → newest also reaped, got ${JSON.stringify(result)}`);
  });
});
