import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';

import {
  validateWorktreeGitFile,
  findStaleWorktrees,
  copyWorktreeIncludes,
} from '../src/worktree-manager';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, { cwd, encoding: 'utf-8' }).trim();
}

function makeRepo(): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'wt-u057h-')));
  git(dir, 'init', '-q');
  git(dir, 'config', 'user.email', 't@t.com');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  fs.writeFileSync(path.join(dir, 'README.md'), '# repo\n');
  git(dir, 'add', 'README.md');
  git(dir, 'commit', '-q', '-m', 'init');
  return dir;
}

/** Resolve the admin gitdir that a worktree's `.git` file points at. */
function adminGitDir(worktreePath: string): string {
  const content = fs.readFileSync(path.join(worktreePath, '.git'), 'utf-8').trim();
  const rel = content.slice('gitdir:'.length).trim();
  return path.isAbsolute(rel) ? rel : path.resolve(worktreePath, rel);
}

// ---------------------------------------------------------------------------
// U057 — validateWorktreeGitFile commondir trust-chain (anti-hijack)
// git.ts resolveCanonicalRoot parity: commondir-child + gitdir back-link checks
// ---------------------------------------------------------------------------
describe('U057: validateWorktreeGitFile commondir trust-chain', () => {
  let repo: string;
  let wt: string;

  beforeEach(() => {
    repo = makeRepo();
    wt = path.join(repo, '.ami', 'worktrees', 'feat');
    fs.mkdirSync(path.dirname(wt), { recursive: true });
    git(repo, 'worktree', 'add', '-q', '-b', 'worktree-feat', wt, 'HEAD');
  });

  afterEach(() => {
    try { git(repo, 'worktree', 'remove', '--force', wt); } catch { /* ignore */ }
    fs.rmSync(repo, { recursive: true, force: true });
  });

  it('accepts a legitimate git-created worktree (commondir + back-link intact)', () => {
    const res = validateWorktreeGitFile(wt);
    assert.equal(res.valid, true, res.error);
  });

  it('rejects a worktree whose gitdir back-link points elsewhere (hijack)', () => {
    // RED on revert: without the back-link check this returns valid.
    const admin = adminGitDir(wt);
    fs.writeFileSync(path.join(admin, 'gitdir'), path.join(repo, 'README.md'));
    const res = validateWorktreeGitFile(wt);
    assert.equal(res.valid, false);
    assert.match(res.error ?? '', /back-link mismatch/);
  });

  it('rejects a worktree whose gitdir is not under <commondir>/worktrees/', () => {
    // Relocate the admin gitdir outside .git/worktrees and repoint the .git file.
    const admin = adminGitDir(wt);
    const evil = path.join(repo, '.git', 'evil');
    fs.renameSync(admin, evil);
    fs.writeFileSync(path.join(wt, '.git'), `gitdir: ${evil}\n`);
    const res = validateWorktreeGitFile(wt);
    assert.equal(res.valid, false);
    assert.match(res.error ?? '', /not under commondir\/worktrees/);
  });

  it('rejects when the gitdir back-link file is missing', () => {
    const admin = adminGitDir(wt);
    fs.rmSync(path.join(admin, 'gitdir'), { force: true });
    const res = validateWorktreeGitFile(wt);
    assert.equal(res.valid, false);
    assert.match(res.error ?? '', /missing gitdir back-link/);
  });

  it('falls through to valid when there is no commondir (submodule-like gitdir)', () => {
    // A .git file pointing at a plain gitdir without a commondir cannot be
    // trust-chain validated; the hardening must not reject it.
    const alt = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'wt-sub-')));
    try {
      const gitdir = path.join(alt, 'realgit');
      fs.mkdirSync(gitdir, { recursive: true });
      fs.writeFileSync(path.join(alt, '.git'), `gitdir: ${gitdir}\n`);
      const res = validateWorktreeGitFile(alt);
      assert.equal(res.valid, true, res.error);
    } finally {
      fs.rmSync(alt, { recursive: true, force: true });
    }
  });

  it('accepts a regular repo whose .git is a directory', () => {
    const res = validateWorktreeGitFile(repo);
    assert.equal(res.valid, true, res.error);
  });

  it('findStaleWorktrees flags a hijacked worktree but not a valid one (consumer wiring)', () => {
    // Valid worktree → not stale.
    assert.deepEqual(findStaleWorktrees(repo), []);
    // Corrupt the back-link → the real consumer now reports it as stale.
    const admin = adminGitDir(wt);
    fs.writeFileSync(path.join(admin, 'gitdir'), path.join(repo, 'README.md'));
    const stale = findStaleWorktrees(repo);
    assert.deepEqual(stale.map(p => fs.realpathSync(p)), [fs.realpathSync(wt)]);
  });
});

// ---------------------------------------------------------------------------
// U057 — copyWorktreeIncludes gitignore-gating (Claude parity)
// Only BOTH-named-AND-gitignored files are carried over; a directory entry
// expands to its gitignored contents.
// ---------------------------------------------------------------------------
describe('U057: copyWorktreeIncludes gitignore-gating', () => {
  let repo: string;

  beforeEach(() => {
    repo = makeRepo();
  });
  afterEach(() => {
    fs.rmSync(repo, { recursive: true, force: true });
  });

  it('copies a gitignored file and skips a plain-untracked file', () => {
    // RED on revert: the pre-gating code copied ANY non-tracked file, so
    // `stray.txt` would appear in the result.
    fs.writeFileSync(path.join(repo, '.gitignore'), 'secret.env\n');
    fs.writeFileSync(path.join(repo, 'secret.env'), 'K=1');
    fs.writeFileSync(path.join(repo, 'stray.txt'), 'S');
    fs.writeFileSync(path.join(repo, '.worktreeinclude'), 'secret.env\nstray.txt\n');
    const wt = path.join(repo, 'wt');
    fs.mkdirSync(wt, { recursive: true });
    const copied = copyWorktreeIncludes(repo, wt);
    assert.deepEqual(copied, ['secret.env']);
    assert.equal(fs.existsSync(path.join(wt, 'secret.env')), true);
    assert.equal(fs.existsSync(path.join(wt, 'stray.txt')), false);
  });

  it('skips a tracked file even when listed', () => {
    fs.writeFileSync(path.join(repo, 'tracked.txt'), 'T');
    git(repo, 'add', 'tracked.txt');
    git(repo, 'commit', '-q', '-m', 'add');
    fs.writeFileSync(path.join(repo, '.worktreeinclude'), 'tracked.txt\n');
    const wt = path.join(repo, 'wt');
    fs.mkdirSync(wt, { recursive: true });
    assert.deepEqual(copyWorktreeIncludes(repo, wt), []);
  });

  it('expands a gitignored directory entry to its ignored files only', () => {
    fs.writeFileSync(path.join(repo, '.gitignore'), 'build/\n');
    fs.mkdirSync(path.join(repo, 'build', 'nested'), { recursive: true });
    fs.writeFileSync(path.join(repo, 'build', 'out.o'), 'o');
    fs.writeFileSync(path.join(repo, 'build', 'nested', 'deep.o'), 'd');
    fs.writeFileSync(path.join(repo, '.worktreeinclude'), 'build\n');
    const wt = path.join(repo, 'wt');
    fs.mkdirSync(wt, { recursive: true });
    const copied = copyWorktreeIncludes(repo, wt);
    assert.ok(copied.includes('build/out.o'));
    assert.ok(copied.includes('build/nested/deep.o'));
    assert.equal(fs.existsSync(path.join(wt, 'build', 'out.o')), true);
    assert.equal(fs.existsSync(path.join(wt, 'build', 'nested', 'deep.o')), true);
  });

  it('falls back to a literal copy outside a git repo', () => {
    const src = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'wt-nogit-src-')));
    const wt = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'wt-nogit-wt-')));
    try {
      fs.writeFileSync(path.join(src, 'local.cfg'), 'x');
      fs.writeFileSync(path.join(src, '.worktreeinclude'), 'local.cfg\n');
      const copied = copyWorktreeIncludes(src, wt);
      assert.deepEqual(copied, ['local.cfg']);
      assert.equal(fs.existsSync(path.join(wt, 'local.cfg')), true);
    } finally {
      fs.rmSync(src, { recursive: true, force: true });
      fs.rmSync(wt, { recursive: true, force: true });
    }
  });
});
