/**
 * Unit: detached-HEAD-aware git HEAD readers (worktree-manager).
 *
 * `readGitHeadFromDisk` / `readGitHeadLabelFromDisk` must distinguish a branch
 * from a detached HEAD (and return a SHA-derived label for the detached case)
 * instead of collapsing every non-branch HEAD to '' — parity with Claude's
 * filesystem `readGitHead` (gitFilesystem.ts L149-183). `readGitBranchFromDisk`
 * keeps its '' contract on a detached HEAD by delegating to the structured reader.
 *
 * RED/GREEN: before readGitHeadFromDisk existed, a detached HEAD produced
 * `{type:'none'}` / '' and these detached assertions fail.
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';

import {
  readGitHeadFromDisk,
  readGitHeadLabelFromDisk,
  readGitBranchFromDisk,
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

const dirs: string[] = [];
function makeRepo(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-githead-'));
  dirs.push(dir);
  git(dir, 'init', '-q', '-b', 'main');
  git(dir, 'config', 'commit.gpgsign', 'false');
  fs.writeFileSync(path.join(dir, 'README.md'), '# r\n');
  git(dir, 'add', 'README.md');
  git(dir, 'commit', '-q', '-m', 'init');
  return dir;
}

afterEach(() => {
  while (dirs.length) {
    const d = dirs.pop()!;
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

describe('worktree-manager — detached-HEAD-aware git HEAD readers', () => {
  it('reports a branch HEAD with its name', () => {
    const dir = makeRepo();
    assert.deepEqual(readGitHeadFromDisk(dir), { type: 'branch', branch: 'main' });
    assert.equal(readGitHeadLabelFromDisk(dir), 'main');
    assert.equal(readGitBranchFromDisk(dir), 'main');
  });

  it('reports a detached HEAD with the raw SHA and a HEAD@<short> label', () => {
    const dir = makeRepo();
    const sha = git(dir, 'rev-parse', 'HEAD');
    git(dir, 'checkout', '-q', '--detach', sha);

    const info = readGitHeadFromDisk(dir);
    assert.equal(info.type, 'detached');
    assert.equal((info as { sha: string }).sha, sha);

    // Label degrades to HEAD@<12-char prefix> (parity: computeBranch/computeHead
    // return the SHA rather than '' for a detached HEAD).
    assert.equal(readGitHeadLabelFromDisk(dir), `HEAD@${sha.slice(0, 12)}`);

    // readGitBranchFromDisk preserves its '' contract for a detached HEAD so its
    // existing branch-only consumers are unchanged.
    assert.equal(readGitBranchFromDisk(dir), '');
  });

  it('returns none/empty outside a git repo', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wt-nogit-'));
    dirs.push(dir);
    assert.deepEqual(readGitHeadFromDisk(dir), { type: 'none' });
    assert.equal(readGitHeadLabelFromDisk(dir), '');
    assert.equal(readGitBranchFromDisk(dir), '');
  });

  it('rejects a tampered/unsafe branch ref read off disk (defense-in-depth)', () => {
    const dir = makeRepo();
    const gitDir = path.join(dir, '.git');
    // Write an unsafe ref name that isSafeRefName must reject (space + glob).
    fs.writeFileSync(path.join(gitDir, 'HEAD'), 'ref: refs/heads/bad name*\n');
    assert.deepEqual(readGitHeadFromDisk(dir), { type: 'none' });
    assert.equal(readGitHeadLabelFromDisk(dir), '');
  });

  it('treats a non-SHA, non-ref HEAD as none', () => {
    const dir = makeRepo();
    fs.writeFileSync(path.join(dir, '.git', 'HEAD'), 'garbage-not-a-sha\n');
    assert.deepEqual(readGitHeadFromDisk(dir), { type: 'none' });
  });
});
