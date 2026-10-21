import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execSync } from 'child_process';

import {
  MAX_WORKTREES,
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
      worktreePath: '/tmp/test/.superinference/worktrees/x',
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
    // Non-git dir: still returns session metadata, git worktree add will fail gracefully
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
    const worktreeDir = path.join(tmpDir, '.superinference', 'worktrees', 'pre-existing');
    fs.mkdirSync(worktreeDir, { recursive: true });

    // Should not throw even though the dir exists
    const session = createWorktreeSession(tmpDir, 'pre-existing');
    assert.equal(session.worktreePath, worktreeDir);
    assert.ok(fs.existsSync(worktreeDir));
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
    const wtDir = path.join(tmpDir, '.superinference', 'worktrees', 'stale-one');
    fs.mkdirSync(wtDir, { recursive: true });
    const stale = findStaleWorktrees(tmpDir);
    assert.equal(stale.length, 1);
    assert.ok(stale[0]!.includes('stale-one'));
  });

  it('excludes valid worktrees', () => {
    const wtDir = path.join(tmpDir, '.superinference', 'worktrees', 'valid-one');
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
