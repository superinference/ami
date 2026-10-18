import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  MAX_WORKTREES,
  validateWorktreeGitFile,
  findStaleWorktrees,
} from '../src/worktree-manager';

describe('MAX_WORKTREES', () => {
  it('is 10', () => {
    assert.equal(MAX_WORKTREES, 10);
  });
});

describe('validateWorktreeGitFile', () => {
  const tmpDirs: string[] = [];
  function mkTmp(): string {
    const d = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-safety-'));
    tmpDirs.push(d);
    return d;
  }

  after(() => {
    for (const d of tmpDirs) {
      fs.rmSync(d, { recursive: true, force: true });
    }
  });

  it('returns valid for a .git directory (real repo root)', () => {
    const dir = mkTmp();
    fs.mkdirSync(path.join(dir, '.git'));
    const result = validateWorktreeGitFile(dir);
    assert.equal(result.valid, true);
    assert.equal(result.error, undefined);
  });

  it('returns valid for a .git file pointing to an existing gitdir', () => {
    const dir = mkTmp();
    const gitdirTarget = path.join(mkTmp(), 'worktrees', 'test-wt');
    fs.mkdirSync(gitdirTarget, { recursive: true });
    fs.writeFileSync(path.join(dir, '.git'), `gitdir: ${gitdirTarget}\n`);
    const result = validateWorktreeGitFile(dir);
    assert.equal(result.valid, true);
    assert.equal(result.error, undefined);
  });

  it('returns invalid for missing .git', () => {
    const dir = mkTmp();
    const result = validateWorktreeGitFile(dir);
    assert.equal(result.valid, false);
    assert.equal(result.error, 'missing .git');
  });

  it('returns invalid for stale .git file (gitdir target does not exist)', () => {
    const dir = mkTmp();
    fs.writeFileSync(path.join(dir, '.git'), 'gitdir: /nonexistent/path/worktrees/foo\n');
    const result = validateWorktreeGitFile(dir);
    assert.equal(result.valid, false);
    assert.equal(result.error, 'stale worktree: gitdir target does not exist');
  });

  it('returns invalid for .git file without gitdir: prefix', () => {
    const dir = mkTmp();
    fs.writeFileSync(path.join(dir, '.git'), 'some random content\n');
    const result = validateWorktreeGitFile(dir);
    assert.equal(result.valid, false);
    assert.equal(result.error, 'invalid .git file: missing gitdir: prefix');
  });

  it('returns invalid for .git file with empty gitdir path', () => {
    const dir = mkTmp();
    fs.writeFileSync(path.join(dir, '.git'), 'gitdir:   \n');
    const result = validateWorktreeGitFile(dir);
    assert.equal(result.valid, false);
    assert.equal(result.error, 'invalid .git file: empty gitdir path');
  });

  it('handles relative gitdir path', () => {
    const dir = mkTmp();
    const gitdirTarget = path.join(dir, 'actual-git-dir');
    fs.mkdirSync(gitdirTarget);
    fs.writeFileSync(path.join(dir, '.git'), 'gitdir: ./actual-git-dir\n');
    const result = validateWorktreeGitFile(dir);
    assert.equal(result.valid, true);
  });

  it('returns invalid for nonexistent worktree path', () => {
    const result = validateWorktreeGitFile('/no/such/worktree/path');
    assert.equal(result.valid, false);
    assert.equal(result.error, 'missing .git');
  });

  it('handles .git file with extra whitespace around gitdir path', () => {
    const dir = mkTmp();
    const gitdirTarget = path.join(mkTmp(), 'ws-test');
    fs.mkdirSync(gitdirTarget, { recursive: true });
    fs.writeFileSync(path.join(dir, '.git'), `gitdir:   ${gitdirTarget}   \n`);
    const result = validateWorktreeGitFile(dir);
    assert.equal(result.valid, true);
  });
});

describe('findStaleWorktrees', () => {
  const tmpDirs: string[] = [];
  function mkTmp(): string {
    const d = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-stale-'));
    tmpDirs.push(d);
    return d;
  }

  after(() => {
    for (const d of tmpDirs) {
      fs.rmSync(d, { recursive: true, force: true });
    }
  });

  it('returns empty for nonexistent worktree directory', () => {
    const dir = mkTmp();
    const result = findStaleWorktrees(dir);
    assert.deepEqual(result, []);
  });

  it('returns empty when all worktrees are valid', () => {
    const repo = mkTmp();
    const wtDir = path.join(repo, '.superinference', 'worktrees');
    const wt1 = path.join(wtDir, 'valid-wt');
    fs.mkdirSync(wt1, { recursive: true });
    fs.mkdirSync(path.join(wt1, '.git'));
    const result = findStaleWorktrees(repo);
    assert.deepEqual(result, []);
  });

  it('finds stale worktrees (missing .git)', () => {
    const repo = mkTmp();
    const wtDir = path.join(repo, '.superinference', 'worktrees');
    const staleWt = path.join(wtDir, 'stale-wt');
    fs.mkdirSync(staleWt, { recursive: true });
    const result = findStaleWorktrees(repo);
    assert.equal(result.length, 1);
    assert.equal(result[0], staleWt);
  });

  it('finds stale worktrees (gitdir points to nonexistent dir)', () => {
    const repo = mkTmp();
    const wtDir = path.join(repo, '.superinference', 'worktrees');
    const staleWt = path.join(wtDir, 'stale-gitdir');
    fs.mkdirSync(staleWt, { recursive: true });
    fs.writeFileSync(path.join(staleWt, '.git'), 'gitdir: /nonexistent/place\n');
    const result = findStaleWorktrees(repo);
    assert.equal(result.length, 1);
    assert.equal(result[0], staleWt);
  });

  it('mixes valid and stale worktrees', () => {
    const repo = mkTmp();
    const wtDir = path.join(repo, '.superinference', 'worktrees');
    const validWt = path.join(wtDir, 'valid');
    fs.mkdirSync(validWt, { recursive: true });
    fs.mkdirSync(path.join(validWt, '.git'));

    const staleWt = path.join(wtDir, 'stale');
    fs.mkdirSync(staleWt, { recursive: true });
    fs.writeFileSync(path.join(staleWt, '.git'), 'gitdir: /nowhere\n');

    const result = findStaleWorktrees(repo);
    assert.equal(result.length, 1);
    assert.equal(result[0], staleWt);
  });

  it('skips non-directory entries in worktrees folder', () => {
    const repo = mkTmp();
    const wtDir = path.join(repo, '.superinference', 'worktrees');
    fs.mkdirSync(wtDir, { recursive: true });
    fs.writeFileSync(path.join(wtDir, 'not-a-dir.txt'), 'hello');
    const result = findStaleWorktrees(repo);
    assert.deepEqual(result, []);
  });
});
