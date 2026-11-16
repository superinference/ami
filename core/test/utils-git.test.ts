import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import * as crypto from 'crypto';
import {
  getGitBranch,
  normalizeGitRemoteUrl,
  getRepoRemoteHash,
  findRemoteBase,
  resolveRef,
  isShallowClone,
  getIsClean,
} from '../src/utils/git';

/** Deterministic, prompt-free git invocation inside a throwaway repo. */
function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, {
    cwd,
    encoding: 'utf-8',
    env: { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '' },
  }).trim();
}

function mkRepo(): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-utilgit-')));
  git(dir, 'init', '-q', '-b', 'main');
  git(dir, 'config', 'user.email', 't@t.t');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  return dir;
}

function commit(dir: string, name: string, body = 'x'): void {
  fs.writeFileSync(path.join(dir, name), body);
  git(dir, 'add', name);
  git(dir, 'commit', '-q', '-m', `add ${name}`);
}

const nonGit = (): string =>
  fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-nongit-')));

// A path that does not exist: spawnSync can't chdir into it, so git is never
// actually spawned (res.error is set) — exercises runGit's "unavailable" arm.
const MISSING_DIR = path.join(os.tmpdir(), 'ami-utilgit-missing', 'nope', String(Date.now()));

// ---------------------------------------------------------------------------
// getGitBranch
// ---------------------------------------------------------------------------

test('getGitBranch: returns the branch for a normal repo, undefined when detached / non-git', () => {
  const repo = mkRepo();
  try {
    commit(repo, 'a');
    assert.equal(getGitBranch(repo), 'main');
    // Detached HEAD -> undefined (abbrev-ref prints "HEAD").
    const sha = git(repo, 'rev-parse', 'HEAD');
    git(repo, 'checkout', '-q', sha);
    assert.equal(getGitBranch(repo), undefined);
  } finally {
    fs.rmSync(repo, { recursive: true, force: true });
  }
  const plain = nonGit();
  try {
    assert.equal(getGitBranch(plain), undefined);
  } finally {
    fs.rmSync(plain, { recursive: true, force: true });
  }
});

test('git helpers degrade safely when the directory cannot be entered (spawn error)', () => {
  // runGit returns null -> each helper takes its "git unavailable" branch.
  assert.equal(getRepoRemoteHash(MISSING_DIR), null);
  assert.equal(findRemoteBase(MISSING_DIR), null);
  assert.equal(isShallowClone(MISSING_DIR), false);
  assert.equal(getIsClean(MISSING_DIR), false);
});

// ---------------------------------------------------------------------------
// normalizeGitRemoteUrl (pure)
// ---------------------------------------------------------------------------

test('normalizeGitRemoteUrl: empty / whitespace / unrecognized -> null', () => {
  assert.equal(normalizeGitRemoteUrl(''), null);
  assert.equal(normalizeGitRemoteUrl('   '), null);
  assert.equal(normalizeGitRemoteUrl('not a url'), null);
  assert.equal(normalizeGitRemoteUrl('ftp://host/owner/repo'), null);
});

test('normalizeGitRemoteUrl: SSH scp-like form', () => {
  assert.equal(normalizeGitRemoteUrl('git@github.com:Owner/Repo.git'), 'github.com/owner/repo');
  assert.equal(normalizeGitRemoteUrl('  git@github.com:owner/repo  '), 'github.com/owner/repo');
});

test('normalizeGitRemoteUrl: HTTPS / ssh:// URL forms strip userinfo + .git', () => {
  assert.equal(normalizeGitRemoteUrl('https://github.com/owner/repo.git'), 'github.com/owner/repo');
  assert.equal(normalizeGitRemoteUrl('ssh://git@github.com/owner/repo'), 'github.com/owner/repo');
  // userinfo (including a password) is dropped from the canonical identity.
  assert.equal(normalizeGitRemoteUrl('https://user:pass@gitlab.com/o/r.git'), 'gitlab.com/o/r');
});

test('normalizeGitRemoteUrl: local git-proxy legacy + enterprise special-casing', () => {
  // Legacy proxy (2 segments) assumes github.com.
  assert.equal(
    normalizeGitRemoteUrl('http://user@127.0.0.1:16583/git/owner/repo'),
    'github.com/owner/repo',
  );
  // Enterprise proxy: first segment contains a dot -> treated as the host.
  assert.equal(
    normalizeGitRemoteUrl('http://127.0.0.1:1/git/ghe.host/owner/repo'),
    'ghe.host/owner/repo',
  );
  // localhost proxy path WITHOUT the /git/ prefix falls through to host/path.
  assert.equal(normalizeGitRemoteUrl('http://localhost:1/owner/repo'), 'localhost:1/owner/repo');
  // IPv6 loopback (`::1`) is also recognized as a local proxy host.
  assert.equal(normalizeGitRemoteUrl('http://user@::1:1/git/owner/repo'), 'github.com/owner/repo');
});

// ---------------------------------------------------------------------------
// getRepoRemoteHash
// ---------------------------------------------------------------------------

test('getRepoRemoteHash: stable across protocol, null without a remote / non-git', () => {
  const repo = mkRepo();
  try {
    commit(repo, 'a');
    // No origin yet.
    assert.equal(getRepoRemoteHash(repo), null);

    git(repo, 'remote', 'add', 'origin', 'git@github.com:owner/repo.git');
    const h = getRepoRemoteHash(repo);
    const expected = crypto.createHash('sha256').update('github.com/owner/repo').digest('hex').slice(0, 16);
    assert.equal(h, expected);
    assert.equal(h!.length, 16);

    // HTTPS clone of the SAME repo yields the SAME hash.
    git(repo, 'remote', 'set-url', 'origin', 'https://github.com/owner/repo.git');
    assert.equal(getRepoRemoteHash(repo), expected);
  } finally {
    fs.rmSync(repo, { recursive: true, force: true });
  }
  const plain = nonGit();
  try {
    assert.equal(getRepoRemoteHash(plain), null);
  } finally {
    fs.rmSync(plain, { recursive: true, force: true });
  }
});

test('getRepoRemoteHash: unparseable remote url -> null', () => {
  const repo = mkRepo();
  try {
    commit(repo, 'a');
    git(repo, 'remote', 'add', 'origin', 'not-a-valid-url');
    assert.equal(getRepoRemoteHash(repo), null);
  } finally {
    fs.rmSync(repo, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// findRemoteBase
// ---------------------------------------------------------------------------

test('findRemoteBase: null when there is no remote at all', () => {
  const repo = mkRepo();
  try {
    commit(repo, 'a');
    assert.equal(findRemoteBase(repo), null);
  } finally {
    fs.rmSync(repo, { recursive: true, force: true });
  }
  const plain = nonGit();
  try {
    assert.equal(findRemoteBase(plain), null);
  } finally {
    fs.rmSync(plain, { recursive: true, force: true });
  }
});

test('findRemoteBase: upstream tracking ref wins, then origin/HEAD, then candidates', () => {
  const upstream = mkRepo();
  const clone = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-clone-')));
  try {
    commit(upstream, 'a');
    git(upstream, 'branch', '-q', 'staging');
    // Clone so the local repo has real origin/* remote refs + origin/HEAD.
    execFileSync('git', ['clone', '-q', upstream, clone], { encoding: 'utf-8' });
    git(clone, 'config', 'user.email', 't@t.t');
    git(clone, 'config', 'user.name', 'T');

    // No upstream set on a fresh local branch -> origin/HEAD resolves the base.
    git(clone, 'checkout', '-q', '-b', 'feature');
    assert.equal(findRemoteBase(clone), 'origin/main');

    // Candidate fallback: remove origin/HEAD so neither @{u} nor it resolve.
    git(clone, 'remote', 'set-head', 'origin', '-d');
    const base = findRemoteBase(clone);
    assert.ok(base === 'origin/main', `expected candidate origin/main, got ${base}`);

    // Custom candidate list is honored.
    assert.equal(findRemoteBase(clone, ['origin/staging']), 'origin/staging');
    assert.equal(findRemoteBase(clone, ['origin/does-not-exist']), null);

    // Set an upstream and confirm it takes priority.
    git(clone, 'branch', '--set-upstream-to=origin/staging', 'feature');
    assert.equal(findRemoteBase(clone), 'origin/staging');
  } finally {
    fs.rmSync(upstream, { recursive: true, force: true });
    fs.rmSync(clone, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// resolveRef (fs-level ref read — no subprocess)
// ---------------------------------------------------------------------------

test('resolveRef: reads loose + packed refs, follows symbolic refs, honors worktree .git file, null when absent/unsafe/non-git', () => {
  const upstream = mkRepo();
  const clone = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-resref-')));
  try {
    commit(upstream, 'a');
    execFileSync('git', ['clone', '-q', upstream, clone], { encoding: 'utf-8' });
    git(clone, 'config', 'user.email', 't@t.t');
    git(clone, 'config', 'user.name', 'T');

    const originSha = git(clone, 'rev-parse', 'refs/remotes/origin/main');
    // Matches what git resolves, whichever store (loose or packed) the clone used.
    assert.equal(resolveRef(clone, 'refs/remotes/origin/main'), originSha);
    // origin/HEAD is a symbolic ref -> followed to the same SHA.
    assert.equal(resolveRef(clone, 'refs/remotes/origin/HEAD'), originSha);

    // Loose-ref path: update-ref writes a loose ref file.
    git(clone, 'update-ref', 'refs/heads/loosebr', originSha);
    assert.equal(resolveRef(clone, 'refs/heads/loosebr'), originSha);

    // packed-refs path: pack everything, loose files are removed.
    git(clone, 'pack-refs', '--all');
    assert.equal(resolveRef(clone, 'refs/remotes/origin/main'), originSha);
    assert.equal(resolveRef(clone, 'refs/heads/loosebr'), originSha);

    // A linked worktree's `.git` is a FILE; refs resolve via commondir.
    const wt = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-resref-wt-')));
    fs.rmSync(wt, { recursive: true, force: true });
    git(clone, 'worktree', 'add', '-q', '--detach', wt);
    assert.ok(fs.statSync(path.join(wt, '.git')).isFile(), '.git should be a file in a linked worktree');
    assert.equal(resolveRef(wt, 'refs/remotes/origin/main'), originSha);

    // Absent ref -> null; unsafe ref (traversal) -> null.
    assert.equal(resolveRef(clone, 'refs/remotes/origin/nope'), null);
    assert.equal(resolveRef(clone, 'refs/../../etc/passwd'), null);
  } finally {
    fs.rmSync(upstream, { recursive: true, force: true });
    fs.rmSync(clone, { recursive: true, force: true });
  }

  // Non-git directory -> null.
  const plain = nonGit();
  try {
    assert.equal(resolveRef(plain, 'refs/heads/main'), null);
  } finally {
    fs.rmSync(plain, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// isShallowClone
// ---------------------------------------------------------------------------

test('isShallowClone: true for a depth-1 clone, false for a full repo / non-git', () => {
  const upstream = mkRepo();
  const full = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-full-')));
  const shallow = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-shallow-')));
  try {
    commit(upstream, 'a');
    commit(upstream, 'b');
    execFileSync('git', ['clone', '-q', upstream, full], { encoding: 'utf-8' });
    execFileSync('git', ['clone', '-q', '--depth', '1', 'file://' + upstream, shallow], {
      encoding: 'utf-8',
    });
    assert.equal(isShallowClone(full), false);
    assert.equal(isShallowClone(shallow), true);
  } finally {
    fs.rmSync(upstream, { recursive: true, force: true });
    fs.rmSync(full, { recursive: true, force: true });
    fs.rmSync(shallow, { recursive: true, force: true });
  }
  const plain = nonGit();
  try {
    assert.equal(isShallowClone(plain), false);
  } finally {
    fs.rmSync(plain, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// getIsClean
// ---------------------------------------------------------------------------

test('getIsClean: clean vs dirty, ignoreUntracked, and non-git', () => {
  const repo = mkRepo();
  try {
    commit(repo, 'a');
    assert.equal(getIsClean(repo), true);

    // A tracked modification is dirty under both modes.
    fs.writeFileSync(path.join(repo, 'a'), 'changed');
    assert.equal(getIsClean(repo), false);
    assert.equal(getIsClean(repo, { ignoreUntracked: true }), false);
    git(repo, 'checkout', '--', 'a');
    assert.equal(getIsClean(repo), true);

    // An untracked file is dirty by default but clean when ignored.
    fs.writeFileSync(path.join(repo, 'untracked'), 'u');
    assert.equal(getIsClean(repo), false);
    assert.equal(getIsClean(repo, { ignoreUntracked: true }), true);
  } finally {
    fs.rmSync(repo, { recursive: true, force: true });
  }
  const plain = nonGit();
  try {
    assert.equal(getIsClean(plain), false);
    assert.equal(getIsClean(plain, { ignoreUntracked: true }), false);
  } finally {
    fs.rmSync(plain, { recursive: true, force: true });
  }
});
