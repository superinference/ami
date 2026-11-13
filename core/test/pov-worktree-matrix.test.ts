import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  MAX_WORKTREES,
  validateWorktreeGitFile,
  findStaleWorktrees,
} from '../src/worktree-manager';

// ---------------------------------------------------------------------------
// Temp-dir bookkeeping — every mkdtemp is registered and torn down in after().
// No git invocation anywhere: validateWorktreeGitFile and findStaleWorktrees
// are pure filesystem inspectors, so we build .git pointers by hand and assert
// on the observable classification.
// ---------------------------------------------------------------------------

const tmpDirs: string[] = [];

function mkTmp(prefix = 'pov-wt-'): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  tmpDirs.push(dir);
  return dir;
}

/** Create a worktree-shaped dir with a `.git` file pointing at `gitdir`. */
function mkWorktreeWithGitFile(parent: string, name: string, gitdir: string): string {
  const wt = path.join(parent, name);
  fs.mkdirSync(wt, { recursive: true });
  fs.writeFileSync(path.join(wt, '.git'), `gitdir: ${gitdir}\n`);
  return wt;
}

/** Create a worktree-shaped dir with a real `.git` directory. */
function mkWorktreeWithGitDir(parent: string, name: string): string {
  const wt = path.join(parent, name);
  fs.mkdirSync(path.join(wt, '.git'), { recursive: true });
  return wt;
}

/** Build `<repo>/.ami/worktrees` and return the worktrees dir path. */
function mkWorktreesRoot(repo: string): string {
  const wtRoot = path.join(repo, '.ami', 'worktrees');
  fs.mkdirSync(wtRoot, { recursive: true });
  return wtRoot;
}

afterEach(() => {
  while (tmpDirs.length) {
    const d = tmpDirs.pop()!;
    try {
      fs.rmSync(d, { recursive: true, force: true });
    } catch {}
  }
});

// ---------------------------------------------------------------------------
// MAX_WORKTREES cap
// ---------------------------------------------------------------------------

describe('MAX_WORKTREES cap', () => {
  it('is the exact numeric ceiling 10', () => {
    assert.equal(MAX_WORKTREES, 10);
  });

  it('is a positive integer (a usable cap, not a sentinel)', () => {
    assert.equal(typeof MAX_WORKTREES, 'number');
    assert.ok(Number.isInteger(MAX_WORKTREES));
    assert.ok(MAX_WORKTREES > 0);
  });

  it('classifies a count-vs-cap decision the way a caller would', () => {
    // Model the "at capacity" check a consumer performs.
    const atCap = (existing: number) => existing >= MAX_WORKTREES;
    assert.equal(atCap(MAX_WORKTREES - 1), false);
    assert.equal(atCap(MAX_WORKTREES), true);
    assert.equal(atCap(MAX_WORKTREES + 1), true);
    assert.equal(atCap(0), false);
  });
});

// ---------------------------------------------------------------------------
// validateWorktreeGitFile — valid .git dir / valid .git file pointer
// ---------------------------------------------------------------------------

describe('validateWorktreeGitFile — valid forms', () => {
  it('accepts a real .git directory (no gitdir pointer needed)', () => {
    const repo = mkTmp();
    const wt = mkWorktreeWithGitDir(repo, 'wt');
    const res = validateWorktreeGitFile(wt);
    assert.deepEqual(res, { valid: true });
  });

  it('accepts a .git file with an absolute gitdir that exists', () => {
    const repo = mkTmp();
    const realGitdir = path.join(repo, 'realgitdir');
    fs.mkdirSync(realGitdir, { recursive: true });
    const wt = mkWorktreeWithGitFile(repo, 'wt', realGitdir);
    const res = validateWorktreeGitFile(wt);
    assert.deepEqual(res, { valid: true });
  });

  it('accepts a .git file with a relative gitdir that resolves to an existing target', () => {
    const repo = mkTmp();
    // Build <repo>/.git/worktrees/wt as the target, point the worktree's .git
    // file at it with a relative path from the worktree dir.
    const target = path.join(repo, '.git', 'worktrees', 'wt');
    fs.mkdirSync(target, { recursive: true });
    const wtParent = path.join(repo, 'checkout');
    fs.mkdirSync(wtParent, { recursive: true });
    const rel = path.relative(path.join(wtParent, 'wt'), target);
    const wt = mkWorktreeWithGitFile(wtParent, 'wt', rel);
    const res = validateWorktreeGitFile(wt);
    assert.deepEqual(res, { valid: true });
  });

  it('tolerates surrounding whitespace / trailing newline in the .git file', () => {
    const repo = mkTmp();
    const realGitdir = path.join(repo, 'gd');
    fs.mkdirSync(realGitdir, { recursive: true });
    const wt = path.join(repo, 'wt');
    fs.mkdirSync(wt, { recursive: true });
    fs.writeFileSync(path.join(wt, '.git'), `  gitdir: ${realGitdir}  \n\n`);
    assert.deepEqual(validateWorktreeGitFile(wt), { valid: true });
  });
});

// ---------------------------------------------------------------------------
// validateWorktreeGitFile — malformed / missing / stale / traversal
// ---------------------------------------------------------------------------

describe('validateWorktreeGitFile — invalid forms', () => {
  it('reports missing .git when there is no .git entry at all', () => {
    const repo = mkTmp();
    const wt = path.join(repo, 'bare');
    fs.mkdirSync(wt, { recursive: true });
    const res = validateWorktreeGitFile(wt);
    assert.equal(res.valid, false);
    assert.equal(res.error, 'missing .git');
  });

  it('rejects a .git file lacking the gitdir: prefix', () => {
    const repo = mkTmp();
    const wt = path.join(repo, 'wt');
    fs.mkdirSync(wt, { recursive: true });
    fs.writeFileSync(path.join(wt, '.git'), '/some/path/not/prefixed\n');
    const res = validateWorktreeGitFile(wt);
    assert.equal(res.valid, false);
    assert.match(res.error!, /missing gitdir: prefix/);
  });

  it('rejects a .git file whose gitdir path is empty after the prefix', () => {
    const repo = mkTmp();
    const wt = path.join(repo, 'wt');
    fs.mkdirSync(wt, { recursive: true });
    fs.writeFileSync(path.join(wt, '.git'), 'gitdir:    \n');
    const res = validateWorktreeGitFile(wt);
    assert.equal(res.valid, false);
    assert.match(res.error!, /empty gitdir path/);
  });

  it('reports a stale pointer when the absolute gitdir target does not exist', () => {
    const repo = mkTmp();
    const missing = path.join(repo, 'does', 'not', 'exist');
    const wt = mkWorktreeWithGitFile(repo, 'wt', missing);
    const res = validateWorktreeGitFile(wt);
    assert.equal(res.valid, false);
    assert.match(res.error!, /stale worktree: gitdir target does not exist/);
  });

  it('reports stale for a relative traversal pointer that resolves to nothing', () => {
    const repo = mkTmp();
    const wt = mkWorktreeWithGitFile(repo, 'wt', '../../nonexistent/git/worktrees/wt');
    const res = validateWorktreeGitFile(wt);
    assert.equal(res.valid, false);
    assert.match(res.error!, /stale worktree/);
  });

  it('accepts a traversal pointer when it actually resolves to a real dir (resolution, not rejection, is the contract)', () => {
    const repo = mkTmp();
    const target = path.join(repo, 'escaped-gitdir');
    fs.mkdirSync(target, { recursive: true });
    const wtParent = path.join(repo, 'a', 'b');
    fs.mkdirSync(wtParent, { recursive: true });
    // A relative pointer (with ..) that genuinely resolves back to the target.
    const rel = path.relative(path.join(wtParent, 'wt'), target);
    assert.ok(rel.startsWith('..'), 'fixture should exercise a traversal pointer');
    const wt = mkWorktreeWithGitFile(wtParent, 'wt', rel);
    const res = validateWorktreeGitFile(wt);
    assert.deepEqual(res, { valid: true });
  });

  it('rejects when .git is a symlink (neither a plain file nor a directory)', () => {
    const repo = mkTmp();
    const wt = path.join(repo, 'wt');
    fs.mkdirSync(wt, { recursive: true });
    const linkTarget = path.join(repo, 'elsewhere');
    fs.mkdirSync(linkTarget, { recursive: true });
    try {
      fs.symlinkSync(linkTarget, path.join(wt, '.git'), 'dir');
    } catch {
      return; // platform without symlink perms — skip assertion
    }
    const res = validateWorktreeGitFile(wt);
    assert.equal(res.valid, false);
    assert.equal(res.error, 'not a file or directory');
  });
});

// ---------------------------------------------------------------------------
// findStaleWorktrees — mkdtemp fixture, stale vs active detection
// ---------------------------------------------------------------------------

describe('findStaleWorktrees', () => {
  it('returns [] when the .ami/worktrees dir does not exist', () => {
    const repo = mkTmp();
    assert.deepEqual(findStaleWorktrees(repo), []);
  });

  it('returns [] when the worktrees dir exists but is empty', () => {
    const repo = mkTmp();
    mkWorktreesRoot(repo);
    assert.deepEqual(findStaleWorktrees(repo), []);
  });

  it('does not flag an active worktree (real .git dir)', () => {
    const repo = mkTmp();
    const wtRoot = mkWorktreesRoot(repo);
    mkWorktreeWithGitDir(wtRoot, 'active');
    assert.deepEqual(findStaleWorktrees(repo), []);
  });

  it('does not flag an active worktree whose .git file points to a live gitdir', () => {
    const repo = mkTmp();
    const wtRoot = mkWorktreesRoot(repo);
    const target = path.join(repo, '.git', 'worktrees', 'active');
    fs.mkdirSync(target, { recursive: true });
    mkWorktreeWithGitFile(wtRoot, 'active', target);
    assert.deepEqual(findStaleWorktrees(repo), []);
  });

  it('flags a worktree whose .git pointer target is gone, returning its full path', () => {
    const repo = mkTmp();
    const wtRoot = mkWorktreesRoot(repo);
    const wt = mkWorktreeWithGitFile(wtRoot, 'stale', path.join(repo, 'gone'));
    const result = findStaleWorktrees(repo);
    assert.deepEqual(result, [wt]);
  });

  it('flags a worktree dir with no .git at all', () => {
    const repo = mkTmp();
    const wtRoot = mkWorktreesRoot(repo);
    const wt = path.join(wtRoot, 'nogit');
    fs.mkdirSync(wt, { recursive: true });
    assert.deepEqual(findStaleWorktrees(repo), [wt]);
  });

  it('separates stale from active in a mixed fixture', () => {
    const repo = mkTmp();
    const wtRoot = mkWorktreesRoot(repo);
    // active: real .git dir
    mkWorktreeWithGitDir(wtRoot, 'active-a');
    // active: live pointer
    const liveTarget = path.join(repo, '.git', 'worktrees', 'active-b');
    fs.mkdirSync(liveTarget, { recursive: true });
    mkWorktreeWithGitFile(wtRoot, 'active-b', liveTarget);
    // stale: dead pointer
    const staleDead = mkWorktreeWithGitFile(wtRoot, 'stale-dead', path.join(repo, 'nope'));
    // stale: malformed .git file
    const staleBad = path.join(wtRoot, 'stale-bad');
    fs.mkdirSync(staleBad, { recursive: true });
    fs.writeFileSync(path.join(staleBad, '.git'), 'not a gitdir pointer\n');

    const result = findStaleWorktrees(repo).sort();
    assert.deepEqual(result, [staleBad, staleDead].sort());
  });

  it('ignores plain files sitting in the worktrees dir (only directories are inspected)', () => {
    const repo = mkTmp();
    const wtRoot = mkWorktreesRoot(repo);
    fs.writeFileSync(path.join(wtRoot, 'README.txt'), 'not a worktree');
    // plus one genuinely stale dir to prove the file was skipped, not the scan.
    const wt = mkWorktreeWithGitFile(wtRoot, 'stale', path.join(repo, 'gone'));
    assert.deepEqual(findStaleWorktrees(repo), [wt]);
  });

  it('every returned path is an absolute descendant of the worktrees dir', () => {
    const repo = mkTmp();
    const wtRoot = mkWorktreesRoot(repo);
    mkWorktreeWithGitFile(wtRoot, 's1', path.join(repo, 'x'));
    mkWorktreeWithGitFile(wtRoot, 's2', path.join(repo, 'y'));
    const result = findStaleWorktrees(repo);
    assert.equal(result.length, 2);
    for (const p of result) {
      assert.ok(path.isAbsolute(p));
      assert.equal(path.dirname(p), wtRoot);
    }
  });

  it('agrees with validateWorktreeGitFile for every entry (consistency invariant)', () => {
    const repo = mkTmp();
    const wtRoot = mkWorktreesRoot(repo);
    mkWorktreeWithGitDir(wtRoot, 'good');
    const bad = mkWorktreeWithGitFile(wtRoot, 'bad', path.join(repo, 'gone'));

    const stale = new Set(findStaleWorktrees(repo));
    for (const entry of fs.readdirSync(wtRoot)) {
      const full = path.join(wtRoot, entry);
      const expectedStale = !validateWorktreeGitFile(full).valid;
      assert.equal(stale.has(full), expectedStale, `mismatch for ${entry}`);
    }
    assert.ok(stale.has(bad));
  });
});
