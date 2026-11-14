import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execSync } from 'child_process';
import {
  canonicalizePath,
  canonicalGitRoot,
  projectSlug,
} from '../src/reference-journal';

function git(args: string, cwd: string): void {
  execSync(`git ${args}`, { cwd, stdio: 'ignore' });
}

test('canonicalizePath: NFC-normalizes and is pure for non-existent paths', () => {
  // A path that never existed is returned in NFC form, unchanged for ASCII.
  assert.equal(canonicalizePath('/no/such/path/xyz'), '/no/such/path/xyz');
  // NFD input normalizes to NFC (é as e + combining accent -> single codepoint).
  const nfd = '/tmp/café';
  assert.equal(canonicalizePath(nfd), nfd.normalize('NFC'));
  assert.notEqual(canonicalizePath(nfd), nfd); // it actually changed
});

test('canonicalizePath: resolves symlinks of the deepest existing ancestor', () => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-canon-')));
  try {
    const real = path.join(root, 'realdir');
    fs.mkdirSync(real);
    const link = path.join(root, 'link');
    fs.symlinkSync(real, link);
    // A not-yet-existing tail under the symlink still canonicalizes the symlink part.
    assert.equal(canonicalizePath(path.join(link, 'child')), path.join(real, 'child'));
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('canonicalGitRoot: all worktrees + subdirs of one repo resolve to the SAME root', () => {
  const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-root-')));
  try {
    const repo = path.join(base, 'repo');
    fs.mkdirSync(repo);
    git('init -q', repo);
    git('config user.email a@b.c', repo);
    git('config user.name a', repo);
    fs.writeFileSync(path.join(repo, 'f'), 'x');
    git('add -A', repo);
    git('commit -qm init', repo);
    const repoReal = fs.realpathSync(repo);

    // A subdirectory of the repo resolves to the repo root.
    const sub = path.join(repo, 'a', 'b');
    fs.mkdirSync(sub, { recursive: true });
    assert.equal(canonicalGitRoot(sub), repoReal);
    assert.equal(canonicalGitRoot(repo), repoReal);

    // A linked worktree resolves to the MAIN repo root (shared project identity).
    const wt = path.join(base, 'wt');
    git(`worktree add -q ${wt}`, repo);
    assert.equal(canonicalGitRoot(wt), repoReal);

    // Therefore the project slugs match across worktree + subdir + main.
    assert.equal(projectSlug(canonicalGitRoot(wt)), projectSlug(canonicalGitRoot(sub)));
  } finally {
    fs.rmSync(base, { recursive: true, force: true });
  }
});

test('canonicalGitRoot: a non-git directory falls back to canonicalizePath(cwd)', () => {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-nogit-')));
  try {
    assert.equal(canonicalGitRoot(dir), canonicalizePath(dir));
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
