import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

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
// createWorktreeSession (pure construction — does not call git add worktree)
// ---------------------------------------------------------------------------

describe('createWorktreeSession', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns a session with correct paths', () => {
    const session = createWorktreeSession(tmpDir, 'test-agent');
    assert.equal(session.originalCwd, tmpDir);
    assert.ok(session.worktreePath.includes('test-agent'));
    assert.equal(session.worktreeName, 'test-agent');
    assert.equal(session.worktreeBranch, 'worktree-test-agent');
    assert.ok(session.createdAt);
    assert.ok(typeof session.creationDurationMs === 'number');
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
