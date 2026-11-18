import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import {
  AVG_CHARS_PER_DIFF_LINE,
  getStagedDiffSize,
  isFileStagedForDeletion,
} from '../src/utils/git';

/** Deterministic, prompt-free git invocation inside a throwaway repo. */
function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, {
    cwd,
    encoding: 'utf-8',
    env: { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '', GIT_CONFIG_NOSYSTEM: '1' },
    stdio: ['pipe', 'pipe', 'pipe'],
  }).trim();
}

const dirs: string[] = [];
function mkRepo(): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u244-diffsize-')));
  dirs.push(dir);
  git(dir, 'init', '-q', '-b', 'main');
  git(dir, 'config', 'user.email', 't@t.t');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  return dir;
}

after(() => {
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});

const MISSING_DIR = path.join(os.tmpdir(), 'ami-u244-missing', 'nope', String(Date.now()));

// ---------------------------------------------------------------------------
// AVG_CHARS_PER_DIFF_LINE
// ---------------------------------------------------------------------------

test('AVG_CHARS_PER_DIFF_LINE: the 40 chars-per-line parity constant', () => {
  assert.equal(AVG_CHARS_PER_DIFF_LINE, 40);
});

// ---------------------------------------------------------------------------
// getStagedDiffSize
// ---------------------------------------------------------------------------

test('getStagedDiffSize: 0 when nothing is staged / dir is not a repo', () => {
  const repo = mkRepo();
  assert.equal(getStagedDiffSize(repo), 0, 'empty repo, no staged changes');
  assert.equal(getStagedDiffSize(MISSING_DIR), 0, 'git unavailable -> 0');
});

test('getStagedDiffSize: (insertions + deletions) * 40 for a staged new file', () => {
  const repo = mkRepo();
  // Seed a base commit so --cached has a baseline.
  fs.writeFileSync(path.join(repo, 'base.txt'), 'base\n');
  git(repo, 'add', 'base.txt');
  git(repo, 'commit', '-q', '-m', 'base');

  // Stage a brand-new 3-line file => 3 insertions, 0 deletions.
  fs.writeFileSync(path.join(repo, 'new.txt'), 'one\ntwo\nthree\n');
  git(repo, 'add', 'new.txt');
  assert.equal(getStagedDiffSize(repo), 3 * AVG_CHARS_PER_DIFF_LINE);

  // Narrowing to an unrelated staged path yields 0.
  assert.equal(getStagedDiffSize(repo, 'base.txt'), 0);
  // Narrowing to the staged file yields its own volume.
  assert.equal(getStagedDiffSize(repo, 'new.txt'), 3 * AVG_CHARS_PER_DIFF_LINE);
});

test('getStagedDiffSize: counts both insertions and deletions', () => {
  const repo = mkRepo();
  fs.writeFileSync(path.join(repo, 'f.txt'), 'a\nb\nc\n');
  git(repo, 'add', 'f.txt');
  git(repo, 'commit', '-q', '-m', 'base');

  // Replace all three lines with two new ones => 2 insertions + 3 deletions.
  fs.writeFileSync(path.join(repo, 'f.txt'), 'x\ny\n');
  git(repo, 'add', 'f.txt');
  assert.equal(getStagedDiffSize(repo), (2 + 3) * AVG_CHARS_PER_DIFF_LINE);
});

// ---------------------------------------------------------------------------
// isFileStagedForDeletion
// ---------------------------------------------------------------------------

test('isFileStagedForDeletion: true only for a staged deletion', () => {
  const repo = mkRepo();
  fs.writeFileSync(path.join(repo, 'keep.txt'), 'keep\n');
  fs.writeFileSync(path.join(repo, 'gone.txt'), 'gone\n');
  git(repo, 'add', 'keep.txt', 'gone.txt');
  git(repo, 'commit', '-q', '-m', 'base');

  // Stage a deletion of gone.txt and a modification of keep.txt.
  git(repo, 'rm', '-q', 'gone.txt');
  fs.writeFileSync(path.join(repo, 'keep.txt'), 'changed\n');
  git(repo, 'add', 'keep.txt');

  assert.equal(isFileStagedForDeletion(repo, 'gone.txt'), true);
  assert.equal(isFileStagedForDeletion(repo, 'keep.txt'), false);
});

test('isFileStagedForDeletion: false for an added file, non-repo, or missing stage', () => {
  const repo = mkRepo();
  fs.writeFileSync(path.join(repo, 'base.txt'), 'base\n');
  git(repo, 'add', 'base.txt');
  git(repo, 'commit', '-q', '-m', 'base');

  fs.writeFileSync(path.join(repo, 'added.txt'), 'new\n');
  git(repo, 'add', 'added.txt');
  assert.equal(isFileStagedForDeletion(repo, 'added.txt'), false, 'an added file is A, not D');
  assert.equal(isFileStagedForDeletion(repo, 'base.txt'), false, 'unchanged file has no staged status');
  assert.equal(isFileStagedForDeletion(MISSING_DIR, 'x'), false, 'git unavailable -> false');
});
