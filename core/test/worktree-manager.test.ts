import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execSync } from 'child_process';

import {
  MAX_WORKTREES,
  GIT_NO_PROMPT_ENV,
  validateWorktreeSlug,
  flattenSlug,
  worktreeBranchName,
  validateWorktreeGitFile,
  findStaleWorktrees,
  getCurrentWorktreeSession,
  setWorktreeSession,
  createWorktreeSession,
  symlinkLargeDirectories,
  copyWorktreeIncludes,
  countWorktreeChanges,
  getWorktreeFileStatus,
  cleanupStaleWorktrees,
  readLastFetchTime,
  isCloneStale,
  STALE_FETCH_WARN_MS,
} from '../src/worktree-manager';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

describe('MAX_WORKTREES', () => {
  it('is 10', () => {
    assert.equal(MAX_WORKTREES, 10);
  });
});

// ---------------------------------------------------------------------------
// validateWorktreeSlug
// ---------------------------------------------------------------------------

describe('validateWorktreeSlug', () => {
  it('accepts valid slug', () => {
    assert.equal(validateWorktreeSlug('agent-abc123f'), null);
  });

  it('accepts slug with hyphens, dots, underscores', () => {
    assert.equal(validateWorktreeSlug('my-worktree_v2.0'), null);
  });

  it('accepts slug with forward slashes', () => {
    assert.equal(validateWorktreeSlug('feature/my-branch'), null);
  });

  it('rejects slug over 64 chars', () => {
    const long = 'a'.repeat(65);
    assert.ok(validateWorktreeSlug(long)?.includes('64'));
  });

  it('rejects slug starting with dot', () => {
    assert.ok(validateWorktreeSlug('.hidden'));
  });

  it('rejects slug starting with slash', () => {
    assert.ok(validateWorktreeSlug('/absolute'));
  });

  it('rejects slug with ..', () => {
    assert.ok(validateWorktreeSlug('a/../b'));
  });

  it('rejects slug with empty segment after slash', () => {
    assert.ok(validateWorktreeSlug('a//b'));
  });

  it('rejects slug starting with number-then-special', () => {
    assert.equal(validateWorktreeSlug('1abc'), null);
  });
});

// ---------------------------------------------------------------------------
// flattenSlug
// ---------------------------------------------------------------------------

describe('flattenSlug', () => {
  it('replaces slashes with +', () => {
    assert.equal(flattenSlug('feature/branch'), 'feature+branch');
  });

  it('preserves slug without slashes', () => {
    assert.equal(flattenSlug('simple'), 'simple');
  });

  it('handles multiple slashes', () => {
    assert.equal(flattenSlug('a/b/c'), 'a+b+c');
  });
});

// ---------------------------------------------------------------------------
// worktreeBranchName
// ---------------------------------------------------------------------------

describe('worktreeBranchName', () => {
  it('prefixes with worktree-', () => {
    assert.equal(worktreeBranchName('my-agent'), 'worktree-my-agent');
  });

  it('flattens slashes', () => {
    assert.equal(worktreeBranchName('feat/x'), 'worktree-feat+x');
  });
});

// ---------------------------------------------------------------------------
// Session getters/setters
// ---------------------------------------------------------------------------

describe('worktree session state', () => {
  afterEach(() => {
    setWorktreeSession(null);
  });

  it('getCurrentWorktreeSession returns null by default', () => {
    setWorktreeSession(null);
    assert.equal(getCurrentWorktreeSession(), null);
  });

  it('setWorktreeSession stores and getCurrentWorktreeSession retrieves', () => {
    const session = {
      originalCwd: '/tmp/test',
      worktreePath: '/tmp/test/.ami/worktrees/x',
      worktreeName: 'x',
      worktreeBranch: 'worktree-x',
      createdAt: new Date().toISOString(),
    };
    setWorktreeSession(session);
    assert.deepEqual(getCurrentWorktreeSession(), session);
  });
});

// ---------------------------------------------------------------------------
// createWorktreeSession — creates the worktree on disk via git worktree add
// ---------------------------------------------------------------------------

describe('createWorktreeSession', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-'));
  });

  afterEach(() => {
    // Clean up worktrees before removing the temp dir
    try {
      execSync('git worktree prune', { cwd: tmpDir, stdio: 'ignore' });
    } catch {}
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns a session with correct paths', () => {
    execSync('git init', { cwd: tmpDir, stdio: 'ignore' });
    execSync('git commit --allow-empty -m "init"', { cwd: tmpDir, stdio: 'ignore', env: { ...process.env, GIT_AUTHOR_NAME: 'Test', GIT_AUTHOR_EMAIL: 'test@test.com', GIT_COMMITTER_NAME: 'Test', GIT_COMMITTER_EMAIL: 'test@test.com' } });

    const session = createWorktreeSession(tmpDir, 'test-agent');
    assert.equal(session.originalCwd, tmpDir);
    assert.ok(session.worktreePath.includes('test-agent'));
    assert.equal(session.worktreeName, 'test-agent');
    assert.equal(session.worktreeBranch, 'worktree-test-agent');
    assert.ok(session.createdAt);
    assert.ok(typeof session.creationDurationMs === 'number');
  });

  it('actually creates the worktree directory on disk (P1-GIT-003 fix)', () => {
    // Set up a real git repo with an initial commit
    execSync('git init', { cwd: tmpDir, stdio: 'ignore' });
    execSync('git commit --allow-empty -m "init"', { cwd: tmpDir, stdio: 'ignore', env: { ...process.env, GIT_AUTHOR_NAME: 'Test', GIT_AUTHOR_EMAIL: 'test@test.com', GIT_COMMITTER_NAME: 'Test', GIT_COMMITTER_EMAIL: 'test@test.com' } });

    const session = createWorktreeSession(tmpDir, 'test-agent');

    // The worktree directory must actually exist on disk
    assert.ok(fs.existsSync(session.worktreePath), 'worktree directory should exist on disk');

    // It must be a valid git worktree (git rev-parse --git-dir should work)
    const gitDir = execSync('git rev-parse --git-dir', {
      cwd: session.worktreePath,
      encoding: 'utf-8',
    }).trim();
    assert.ok(gitDir, 'worktree should have a valid .git reference');

    // Clean up the worktree
    try {
      execSync(`git worktree remove --force "${session.worktreePath}"`, { cwd: tmpDir, stdio: 'ignore' });
    } catch {}
  });

  it('skips creation when worktree directory already exists', () => {
    // Set up a real git repo with an initial commit
    execSync('git init', { cwd: tmpDir, stdio: 'ignore' });
    execSync('git commit --allow-empty -m "init"', { cwd: tmpDir, stdio: 'ignore', env: { ...process.env, GIT_AUTHOR_NAME: 'Test', GIT_AUTHOR_EMAIL: 'test@test.com', GIT_COMMITTER_NAME: 'Test', GIT_COMMITTER_EMAIL: 'test@test.com' } });

    // Create the worktree directory manually (simulating enter-worktree.ts scenario)
    const worktreeDir = path.join(tmpDir, '.ami', 'worktrees', 'pre-existing');
    fs.mkdirSync(worktreeDir, { recursive: true });

    // Should not throw even though the dir exists
    const session = createWorktreeSession(tmpDir, 'pre-existing');
    assert.equal(session.worktreePath, worktreeDir);
    assert.ok(fs.existsSync(worktreeDir));
  });

  it('bumps mtime of an already-existing worktree so stale cleanup cannot reap a reused worktree', () => {
    execSync('git init', { cwd: tmpDir, stdio: 'ignore' });
    execSync('git commit --allow-empty -m "init"', { cwd: tmpDir, stdio: 'ignore', env: { ...process.env, GIT_AUTHOR_NAME: 'Test', GIT_AUTHOR_EMAIL: 'test@test.com', GIT_COMMITTER_NAME: 'Test', GIT_COMMITTER_EMAIL: 'test@test.com' } });

    // Simulate a long-lived worktree created well past the stale-cleanup cutoff.
    const worktreeDir = path.join(tmpDir, '.ami', 'worktrees', 'reused');
    fs.mkdirSync(worktreeDir, { recursive: true });
    const old = new Date(Date.now() - 40 * 24 * 60 * 60 * 1000); // 40 days ago
    fs.utimesSync(worktreeDir, old, old);
    assert.ok(fs.statSync(worktreeDir).mtimeMs < Date.now() - 30 * 24 * 60 * 60 * 1000, 'precondition: mtime is stale');

    // Re-entering the existing worktree must refresh its mtime.
    createWorktreeSession(tmpDir, 'reused');

    const bumped = fs.statSync(worktreeDir).mtimeMs;
    assert.ok(bumped > Date.now() - 60_000, 'mtime must be bumped to ~now on resume');
    assert.ok(bumped > old.getTime(), 'mtime must advance past the stale value');
  });
});

// ---------------------------------------------------------------------------
// validateWorktreeGitFile
// ---------------------------------------------------------------------------

describe('validateWorktreeGitFile', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-git-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns valid:true when .git is a directory', () => {
    fs.mkdirSync(path.join(tmpDir, '.git'), { recursive: true });
    assert.deepEqual(validateWorktreeGitFile(tmpDir), { valid: true });
  });

  it('returns valid:false when .git is missing', () => {
    const result = validateWorktreeGitFile(tmpDir);
    assert.equal(result.valid, false);
    assert.ok(result.error?.includes('missing'));
  });

  it('returns valid:true for .git file with valid gitdir pointing to existing path', () => {
    const gitdirTarget = path.join(tmpDir, 'gitdir-target');
    fs.mkdirSync(gitdirTarget, { recursive: true });
    fs.writeFileSync(path.join(tmpDir, '.git'), `gitdir: ${gitdirTarget}\n`);
    assert.deepEqual(validateWorktreeGitFile(tmpDir), { valid: true });
  });

  it('returns valid:false for .git file with non-existent gitdir target', () => {
    fs.writeFileSync(path.join(tmpDir, '.git'), 'gitdir: /nonexistent/path\n');
    const result = validateWorktreeGitFile(tmpDir);
    assert.equal(result.valid, false);
    assert.ok(result.error?.includes('stale'));
  });

  it('returns valid:false for .git file without gitdir prefix', () => {
    fs.writeFileSync(path.join(tmpDir, '.git'), 'invalid content\n');
    const result = validateWorktreeGitFile(tmpDir);
    assert.equal(result.valid, false);
    assert.ok(result.error?.includes('missing gitdir'));
  });

  it('returns valid:false for .git file with empty gitdir', () => {
    fs.writeFileSync(path.join(tmpDir, '.git'), 'gitdir: \n');
    const result = validateWorktreeGitFile(tmpDir);
    assert.equal(result.valid, false);
    assert.ok(result.error?.includes('empty'));
  });

  it('handles relative gitdir path', () => {
    const subdir = path.join(tmpDir, 'sub');
    fs.mkdirSync(subdir, { recursive: true });
    const gitdir = path.join(tmpDir, 'real-git');
    fs.mkdirSync(gitdir, { recursive: true });
    fs.writeFileSync(path.join(subdir, '.git'), `gitdir: ../real-git\n`);
    assert.deepEqual(validateWorktreeGitFile(subdir), { valid: true });
  });
});

// ---------------------------------------------------------------------------
// findStaleWorktrees
// ---------------------------------------------------------------------------

describe('findStaleWorktrees', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-stale-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns empty for non-existent worktree dir', () => {
    assert.deepEqual(findStaleWorktrees(tmpDir), []);
  });

  it('returns stale worktrees (missing .git)', () => {
    const wtDir = path.join(tmpDir, '.ami', 'worktrees', 'stale-one');
    fs.mkdirSync(wtDir, { recursive: true });
    const stale = findStaleWorktrees(tmpDir);
    assert.equal(stale.length, 1);
    assert.ok(stale[0]!.includes('stale-one'));
  });

  it('excludes valid worktrees', () => {
    const wtDir = path.join(tmpDir, '.ami', 'worktrees', 'valid-one');
    fs.mkdirSync(wtDir, { recursive: true });
    fs.mkdirSync(path.join(wtDir, '.git'), { recursive: true });
    assert.deepEqual(findStaleWorktrees(tmpDir), []);
  });
});

// ---------------------------------------------------------------------------
// symlinkLargeDirectories
// ---------------------------------------------------------------------------

describe('symlinkLargeDirectories', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-sym-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('creates symlink for node_modules', () => {
    const src = path.join(tmpDir, 'source');
    const wt = path.join(tmpDir, 'worktree');
    fs.mkdirSync(path.join(src, 'node_modules'), { recursive: true });
    fs.mkdirSync(wt, { recursive: true });
    symlinkLargeDirectories(src, wt);
    const link = path.join(wt, 'node_modules');
    assert.ok(fs.existsSync(link), 'symlink should exist');
    assert.ok(fs.lstatSync(link).isSymbolicLink(), 'should be a symlink');
  });

  it('skips when source dir does not exist', () => {
    const src = path.join(tmpDir, 'source');
    const wt = path.join(tmpDir, 'worktree');
    fs.mkdirSync(src, { recursive: true });
    fs.mkdirSync(wt, { recursive: true });
    symlinkLargeDirectories(src, wt);
    assert.ok(!fs.existsSync(path.join(wt, 'node_modules')));
  });

  it('skips when target already exists', () => {
    const src = path.join(tmpDir, 'source');
    const wt = path.join(tmpDir, 'worktree');
    fs.mkdirSync(path.join(src, 'node_modules'), { recursive: true });
    fs.mkdirSync(path.join(wt, 'node_modules'), { recursive: true });
    symlinkLargeDirectories(src, wt);
    assert.ok(!fs.lstatSync(path.join(wt, 'node_modules')).isSymbolicLink());
  });
});

// ---------------------------------------------------------------------------
// copyWorktreeIncludes
// ---------------------------------------------------------------------------

describe('copyWorktreeIncludes', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-inc-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns empty when no .worktreeinclude file', () => {
    const src = path.join(tmpDir, 'source');
    const wt = path.join(tmpDir, 'worktree');
    fs.mkdirSync(src, { recursive: true });
    fs.mkdirSync(wt, { recursive: true });
    assert.deepEqual(copyWorktreeIncludes(src, wt), []);
  });

  it('copies files listed in .worktreeinclude', () => {
    const src = path.join(tmpDir, 'source');
    const wt = path.join(tmpDir, 'worktree');
    fs.mkdirSync(src, { recursive: true });
    fs.mkdirSync(wt, { recursive: true });
    fs.writeFileSync(path.join(src, '.env'), 'KEY=val');
    fs.writeFileSync(path.join(src, '.worktreeinclude'), '.env\n');
    const copied = copyWorktreeIncludes(src, wt);
    assert.deepEqual(copied, ['.env']);
    assert.equal(fs.readFileSync(path.join(wt, '.env'), 'utf-8'), 'KEY=val');
  });

  it('skips comments and empty lines', () => {
    const src = path.join(tmpDir, 'source');
    const wt = path.join(tmpDir, 'worktree');
    fs.mkdirSync(src, { recursive: true });
    fs.mkdirSync(wt, { recursive: true });
    fs.writeFileSync(path.join(src, 'a.txt'), 'a');
    fs.writeFileSync(path.join(src, '.worktreeinclude'), '# comment\n\na.txt\n');
    const copied = copyWorktreeIncludes(src, wt);
    assert.deepEqual(copied, ['a.txt']);
  });

  it('skips files that do not exist in source', () => {
    const src = path.join(tmpDir, 'source');
    const wt = path.join(tmpDir, 'worktree');
    fs.mkdirSync(src, { recursive: true });
    fs.mkdirSync(wt, { recursive: true });
    fs.writeFileSync(path.join(src, '.worktreeinclude'), 'missing.txt\n');
    assert.deepEqual(copyWorktreeIncludes(src, wt), []);
  });

  it('copies directories recursively', () => {
    const src = path.join(tmpDir, 'source');
    const wt = path.join(tmpDir, 'worktree');
    fs.mkdirSync(path.join(src, 'config'), { recursive: true });
    fs.mkdirSync(wt, { recursive: true });
    fs.writeFileSync(path.join(src, 'config', 'app.json'), '{}');
    fs.writeFileSync(path.join(src, '.worktreeinclude'), 'config\n');
    const copied = copyWorktreeIncludes(src, wt);
    assert.deepEqual(copied, ['config']);
    assert.ok(fs.existsSync(path.join(wt, 'config', 'app.json')));
  });
});

// ---------------------------------------------------------------------------
// countWorktreeChanges — P4-GIT-004 regression test
// ---------------------------------------------------------------------------

describe('countWorktreeChanges', () => {
  let tmpDir: string;
  const gitEnv = {
    GIT_TERMINAL_PROMPT: '0',
    GIT_ASKPASS: '',
    GIT_AUTHOR_NAME: 'Test',
    GIT_AUTHOR_EMAIL: 'test@test.com',
    GIT_COMMITTER_NAME: 'Test',
    GIT_COMMITTER_EMAIL: 'test@test.com',
  };
  const execOpts = (cwd: string) => ({
    cwd,
    encoding: 'utf-8' as const,
    env: { ...process.env, ...gitEnv },
  });

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-changes-'));
  });

  afterEach(() => {
    try {
      execSync('git worktree prune', execOpts(tmpDir));
    } catch {}
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('detects unpushed commits on a worktree branch with no upstream (P4-GIT-004)', () => {
    execSync('git init', execOpts(tmpDir));
    fs.writeFileSync(path.join(tmpDir, 'README.md'), 'init');
    execSync('git add README.md', execOpts(tmpDir));
    execSync('git commit -m "initial"', execOpts(tmpDir));

    const wtPath = path.join(tmpDir, 'wt');
    execSync(`git worktree add -b test-branch "${wtPath}" HEAD`, execOpts(tmpDir));

    fs.writeFileSync(path.join(wtPath, 'work.txt'), 'agent work');
    execSync('git add work.txt', execOpts(wtPath));
    execSync('git commit -m "agent work"', execOpts(wtPath));

    const result = countWorktreeChanges(wtPath, tmpDir);
    assert.ok(result !== null, 'countWorktreeChanges should not return null');
    assert.equal(result!.uncommitted, 0, 'no uncommitted changes');
    assert.ok(result!.unpushed > 0, `unpushed must be > 0, got ${result!.unpushed}`);

    execSync(`git worktree remove --force "${wtPath}"`, execOpts(tmpDir));
  });

  it('reports uncommitted files correctly', () => {
    execSync('git init', execOpts(tmpDir));
    fs.writeFileSync(path.join(tmpDir, 'README.md'), 'init');
    execSync('git add README.md', execOpts(tmpDir));
    execSync('git commit -m "initial"', execOpts(tmpDir));

    const wtPath = path.join(tmpDir, 'wt');
    execSync(`git worktree add -b test-uncommitted "${wtPath}" HEAD`, execOpts(tmpDir));
    fs.writeFileSync(path.join(wtPath, 'dirty.txt'), 'not committed');

    const result = countWorktreeChanges(wtPath, tmpDir);
    assert.ok(result !== null);
    assert.equal(result!.uncommitted, 1, 'should detect 1 uncommitted file');

    execSync(`git worktree remove --force "${wtPath}"`, execOpts(tmpDir));
  });

  it('returns null on invalid git directory', () => {
    const fakePath = path.join(tmpDir, 'not-a-repo');
    fs.mkdirSync(fakePath, { recursive: true });
    const result = countWorktreeChanges(fakePath, tmpDir);
    assert.equal(result, null, 'should return null for non-git directory');
  });
});

// ---------------------------------------------------------------------------
// CC19: GIT_NO_REPLACE_OBJECTS in worktree git env
// ---------------------------------------------------------------------------

describe('GIT_NO_PROMPT_ENV – CC19 no-replace-objects', () => {
  it('includes GIT_TERMINAL_PROMPT=0', () => {
    assert.equal(GIT_NO_PROMPT_ENV.GIT_TERMINAL_PROMPT, '0');
  });

  it('includes GIT_NO_REPLACE_OBJECTS=1 to prevent replace refs from altering worktree operations', () => {
    assert.equal(
      (GIT_NO_PROMPT_ENV as Record<string, string>).GIT_NO_REPLACE_OBJECTS,
      '1',
      'GIT_NO_PROMPT_ENV should include GIT_NO_REPLACE_OBJECTS=1',
    );
  });
});

// ---------------------------------------------------------------------------
// getWorktreeFileStatus — tracked/untracked dirty-file enumeration
// ---------------------------------------------------------------------------

describe('getWorktreeFileStatus', () => {
  let tmpDir: string;
  const gitEnv = {
    GIT_TERMINAL_PROMPT: '0',
    GIT_ASKPASS: '',
    GIT_AUTHOR_NAME: 'Test',
    GIT_AUTHOR_EMAIL: 'test@test.com',
    GIT_COMMITTER_NAME: 'Test',
    GIT_COMMITTER_EMAIL: 'test@test.com',
  };
  const execOpts = (cwd: string) => ({ cwd, encoding: 'utf-8' as const, env: { ...process.env, ...gitEnv } });

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-fstat-'));
    execSync('git init', execOpts(tmpDir));
    fs.writeFileSync(path.join(tmpDir, 'README.md'), 'init');
    execSync('git add README.md', execOpts(tmpDir));
    execSync('git commit -m initial', execOpts(tmpDir));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('splits tracked modifications from untracked files', () => {
    fs.writeFileSync(path.join(tmpDir, 'README.md'), 'changed'); // tracked modification
    fs.writeFileSync(path.join(tmpDir, 'new.txt'), 'brand new');  // untracked
    const result = getWorktreeFileStatus(tmpDir);
    assert.ok(result !== null);
    assert.deepEqual(result!.tracked, ['README.md']);
    assert.deepEqual(result!.untracked, ['new.txt']);
  });

  it('returns empty arrays for a clean worktree', () => {
    const result = getWorktreeFileStatus(tmpDir);
    assert.deepEqual(result, { tracked: [], untracked: [] });
  });

  it('surfaces the destination path of a rename as tracked', () => {
    execSync('git mv README.md RENAMED.md', execOpts(tmpDir));
    const result = getWorktreeFileStatus(tmpDir);
    assert.ok(result !== null);
    assert.ok(result!.tracked.includes('RENAMED.md'), `tracked should include renamed dest, got ${JSON.stringify(result!.tracked)}`);
  });

  it('returns null for a non-git directory', () => {
    const notRepo = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-nogit-'));
    try {
      assert.equal(getWorktreeFileStatus(notRepo), null);
    } finally {
      fs.rmSync(notRepo, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// cleanupStaleWorktrees — structured {cleaned, failed} accounting
// ---------------------------------------------------------------------------

describe('cleanupStaleWorktrees result accounting', () => {
  let tmpDir: string;
  const isRoot = typeof process.getuid === 'function' && process.getuid() === 0;
  const gitEnv = {
    GIT_TERMINAL_PROMPT: '0',
    GIT_ASKPASS: '',
    GIT_AUTHOR_NAME: 'Test',
    GIT_AUTHOR_EMAIL: 'test@test.com',
    GIT_COMMITTER_NAME: 'Test',
    GIT_COMMITTER_EMAIL: 'test@test.com',
  };
  const execOpts = (cwd: string) => ({ cwd, encoding: 'utf-8' as const, env: { ...process.env, ...gitEnv } });

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-cleanup-'));
  });
  afterEach(() => {
    try { fs.chmodSync(path.join(tmpDir, '.ami', 'worktrees'), 0o755); } catch { /* ignore */ }
    try { execSync('git worktree prune', execOpts(tmpDir)); } catch { /* ignore */ }
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns {cleaned:[], failed:[]} when no worktrees dir exists', () => {
    assert.deepEqual(cleanupStaleWorktrees(tmpDir), { cleaned: [], failed: [] });
  });

  // A clean worktree whose commits are all reachable from a remote (0 unpushed,
  // 0 uncommitted) and aged past the cutoff is removed and reported in `cleaned`.
  const setupStaleCleanWorktree = (): string => {
    const remote = path.join(tmpDir, 'remote.git');
    execSync(`git init --bare "${remote}"`, execOpts(tmpDir));
    execSync('git init', execOpts(tmpDir));
    execSync(`git remote add origin "${remote}"`, execOpts(tmpDir));
    fs.writeFileSync(path.join(tmpDir, 'README.md'), 'init');
    execSync('git add README.md', execOpts(tmpDir));
    execSync('git commit -m initial', execOpts(tmpDir));
    execSync('git push -u origin HEAD:main', execOpts(tmpDir));
    const wtBase = path.join(tmpDir, '.ami', 'worktrees');
    fs.mkdirSync(wtBase, { recursive: true });
    const wt = path.join(wtBase, 'old-wt');
    // Detach at the exact pushed commit so the worktree has 0 unpushed commits.
    execSync(`git worktree add --detach "${wt}" HEAD`, execOpts(tmpDir));
    const old = new Date(Date.now() - 1000 * 60 * 60 * 24 * 365);
    fs.utimesSync(wt, old, old);
    return wt;
  };

  it('removes an aged clean worktree and reports it in cleaned', () => {
    setupStaleCleanWorktree();
    const result = cleanupStaleWorktrees(tmpDir, 1);
    assert.deepEqual(result.failed, []);
    assert.ok(result.cleaned.includes('old-wt'), `expected old-wt cleaned, got ${JSON.stringify(result)}`);
  });

  it('accounts a removal failure in failed[] instead of swallowing it', { skip: isRoot }, () => {
    setupStaleCleanWorktree();
    // Make the worktrees container read-only so git cannot rmdir the entry → the
    // remove throws and must be recorded, not silently dropped.
    fs.chmodSync(path.join(tmpDir, '.ami', 'worktrees'), 0o555);
    const result = cleanupStaleWorktrees(tmpDir, 1);
    assert.equal(result.cleaned.length, 0, 'nothing should be reported as cleaned');
    assert.equal(result.failed.length, 1, `one failure should be accounted, got ${JSON.stringify(result)}`);
    assert.equal(result.failed[0].name, 'old-wt');
    assert.ok(result.failed[0].error.length > 0, 'failure must carry an error message');
  });
});

// ---------------------------------------------------------------------------
// readLastFetchTime / isCloneStale — FETCH_HEAD staleness
// ---------------------------------------------------------------------------

describe('readLastFetchTime / isCloneStale', () => {
  let tmpDir: string;
  const gitEnv = {
    GIT_TERMINAL_PROMPT: '0',
    GIT_ASKPASS: '',
    GIT_AUTHOR_NAME: 'Test',
    GIT_AUTHOR_EMAIL: 'test@test.com',
    GIT_COMMITTER_NAME: 'Test',
    GIT_COMMITTER_EMAIL: 'test@test.com',
  };
  const execOpts = (cwd: string) => ({ cwd, encoding: 'utf-8' as const, env: { ...process.env, ...gitEnv } });

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-fetch-'));
    execSync('git init', execOpts(tmpDir));
    fs.writeFileSync(path.join(tmpDir, 'README.md'), 'init');
    execSync('git add README.md', execOpts(tmpDir));
    execSync('git commit -m initial', execOpts(tmpDir));
  });
  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('7-day window constant matches Claude', () => {
    assert.equal(STALE_FETCH_WARN_MS, 7 * 24 * 60 * 60 * 1000);
  });

  it('returns undefined and reports stale when the repo has never fetched', () => {
    assert.equal(readLastFetchTime(tmpDir), undefined);
    assert.equal(isCloneStale(tmpDir), true);
  });

  it('reads a recent FETCH_HEAD mtime and reports not-stale', () => {
    const gitDir = execSync('git rev-parse --path-format=absolute --git-dir', execOpts(tmpDir)).toString().trim();
    fs.writeFileSync(path.join(gitDir, 'FETCH_HEAD'), 'deadbeef\tbranch main\n');
    const t = readLastFetchTime(tmpDir);
    assert.ok(typeof t === 'number' && t > 0);
    assert.equal(isCloneStale(tmpDir), false);
  });

  it('reports stale when FETCH_HEAD is older than the window', () => {
    const gitDir = execSync('git rev-parse --path-format=absolute --git-dir', execOpts(tmpDir)).toString().trim();
    const fh = path.join(gitDir, 'FETCH_HEAD');
    fs.writeFileSync(fh, 'deadbeef\tbranch main\n');
    const old = new Date(Date.now() - STALE_FETCH_WARN_MS - 60_000);
    fs.utimesSync(fh, old, old);
    assert.equal(isCloneStale(tmpDir), true);
    // A wide window makes the same timestamp fresh again.
    assert.equal(isCloneStale(tmpDir, STALE_FETCH_WARN_MS * 1000), false);
  });

  it('returns undefined for a non-git directory', () => {
    const notRepo = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-fetch-nogit-'));
    try {
      assert.equal(readLastFetchTime(notRepo), undefined);
    } finally {
      fs.rmSync(notRepo, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// symlinkLargeDirectories — warning accounting on unexpected errors
// ---------------------------------------------------------------------------

describe('symlinkLargeDirectories warnings', () => {
  let tmpDir: string;
  const isRoot = typeof process.getuid === 'function' && process.getuid() === 0;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-symwarn-'));
  });
  afterEach(() => {
    try { fs.chmodSync(path.join(tmpDir, 'wt'), 0o755); } catch { /* ignore */ }
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns no warnings on a normal symlink', () => {
    const src = path.join(tmpDir, 'src');
    const wt = path.join(tmpDir, 'wt');
    fs.mkdirSync(path.join(src, 'node_modules'), { recursive: true });
    fs.mkdirSync(wt, { recursive: true });
    assert.deepEqual(symlinkLargeDirectories(src, wt), []);
  });

  it('returns no warnings when the source is missing (benign ENOENT)', () => {
    const src = path.join(tmpDir, 'src');
    const wt = path.join(tmpDir, 'wt');
    fs.mkdirSync(src, { recursive: true });
    fs.mkdirSync(wt, { recursive: true });
    assert.deepEqual(symlinkLargeDirectories(src, wt), []);
  });

  it('records a warning for an unexpected error (EACCES/EPERM)', { skip: isRoot }, () => {
    const src = path.join(tmpDir, 'src');
    const wt = path.join(tmpDir, 'wt');
    fs.mkdirSync(path.join(src, 'node_modules'), { recursive: true });
    fs.mkdirSync(wt, { recursive: true });
    fs.chmodSync(wt, 0o555); // read-only worktree → symlinkSync fails to create entry
    const warnings = symlinkLargeDirectories(src, wt);
    assert.equal(warnings.length, 1, `expected 1 warning, got ${JSON.stringify(warnings)}`);
    assert.ok(/node_modules/.test(warnings[0]));
  });
});
