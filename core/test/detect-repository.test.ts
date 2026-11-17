import { test, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';

import {
  parseGitRemote,
  parseGitHubRepository,
  detectCurrentRepository,
  detectCurrentRepositoryWithHost,
  getCachedRepository,
  clearRepositoryCaches,
  type ParsedRepository,
} from '../src/detect-repository';
import { getRemoteOriginUrl } from '../src/utils/git';

// ---------------------------------------------------------------------------
// temp git repo helpers
// ---------------------------------------------------------------------------
const tmpDirs: string[] = [];

function makeRepo(remoteUrl?: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-detect-repo-'));
  tmpDirs.push(dir);
  const run = (args: string[]) =>
    execFileSync('git', args, { cwd: dir, stdio: ['ignore', 'ignore', 'ignore'] });
  run(['init', '-q']);
  if (remoteUrl !== undefined) run(['remote', 'add', 'origin', remoteUrl]);
  return dir;
}

function setRemote(dir: string, url: string): void {
  const hasOrigin =
    execFileSync('git', ['remote'], { cwd: dir, encoding: 'utf-8' })
      .split('\n')
      .includes('origin');
  const args = hasOrigin
    ? ['remote', 'set-url', 'origin', url]
    : ['remote', 'add', 'origin', url];
  execFileSync('git', args, { cwd: dir, stdio: ['ignore', 'ignore', 'ignore'] });
}

afterEach(() => {
  clearRepositoryCaches();
  while (tmpDirs.length) {
    const d = tmpDirs.pop()!;
    try {
      fs.rmSync(d, { recursive: true, force: true });
    } catch {
      /* best effort */
    }
  }
});

// ---------------------------------------------------------------------------
// parseGitRemote
// ---------------------------------------------------------------------------
test('parseGitRemote: SSH scp-style with and without .git', () => {
  assert.deepEqual(parseGitRemote('git@github.com:owner/repo.git'), {
    host: 'github.com',
    owner: 'owner',
    name: 'repo',
  });
  assert.deepEqual(parseGitRemote('git@github.com:owner/repo'), {
    host: 'github.com',
    owner: 'owner',
    name: 'repo',
  });
});

test('parseGitRemote: https/ssh/git URL forms', () => {
  const expected: ParsedRepository = { host: 'github.com', owner: 'owner', name: 'repo' };
  assert.deepEqual(parseGitRemote('https://github.com/owner/repo.git'), expected);
  assert.deepEqual(parseGitRemote('https://github.com/owner/repo'), expected);
  assert.deepEqual(parseGitRemote('ssh://git@github.com/owner/repo.git'), expected);
  assert.deepEqual(parseGitRemote('git://github.com/owner/repo'), expected);
  // leading/trailing whitespace is trimmed
  assert.deepEqual(parseGitRemote('  https://github.com/owner/repo.git\n'), expected);
});

test('parseGitRemote: repo names may contain dots', () => {
  assert.deepEqual(parseGitRemote('https://github.com/owner/cc.kurs.web.git'), {
    host: 'github.com',
    owner: 'owner',
    name: 'cc.kurs.web',
  });
});

test('parseGitRemote: GHE host is preserved (not filtered here)', () => {
  assert.deepEqual(parseGitRemote('git@ghe.corp.com:team/proj.git'), {
    host: 'ghe.corp.com',
    owner: 'team',
    name: 'proj',
  });
});

test('parseGitRemote: HTTPS port preserved, SSH/git port dropped', () => {
  assert.deepEqual(parseGitRemote('https://ghe.corp.com:8443/team/proj.git'), {
    host: 'ghe.corp.com:8443',
    owner: 'team',
    name: 'proj',
  });
  // ssh port is not a usable web port -> dropped
  assert.deepEqual(parseGitRemote('ssh://git@ghe.corp.com:2222/team/proj.git'), {
    host: 'ghe.corp.com',
    owner: 'team',
    name: 'proj',
  });
});

test('parseGitRemote: SSH-config aliases (non-alphabetic TLD) rejected', () => {
  assert.equal(parseGitRemote('git@github.com-work:owner/repo.git'), null);
  assert.equal(parseGitRemote('ssh://git@github.com-work/owner/repo.git'), null);
  // no dot at all -> not a real hostname
  assert.equal(parseGitRemote('git@localhostalias:owner/repo.git'), null);
});

test('parseGitRemote: unrecognized input returns null', () => {
  assert.equal(parseGitRemote(''), null);
  assert.equal(parseGitRemote('not a url'), null);
  assert.equal(parseGitRemote('https://github.com/onlyowner'), null);
});

// ---------------------------------------------------------------------------
// parseGitHubRepository
// ---------------------------------------------------------------------------
test('parseGitHubRepository: URL -> owner/repo (github.com only)', () => {
  assert.equal(parseGitHubRepository('https://github.com/owner/repo.git'), 'owner/repo');
  assert.equal(parseGitHubRepository('git@github.com:owner/repo.git'), 'owner/repo');
  // GHE host filtered out
  assert.equal(parseGitHubRepository('git@ghe.corp.com:team/proj.git'), null);
});

test('parseGitHubRepository: plain owner/repo shorthand accepted', () => {
  assert.equal(parseGitHubRepository('owner/repo'), 'owner/repo');
  assert.equal(parseGitHubRepository('owner/repo.git'), 'owner/repo');
  assert.equal(parseGitHubRepository('  owner/repo  '), 'owner/repo');
});

test('parseGitHubRepository: unrecognized -> null', () => {
  assert.equal(parseGitHubRepository('justaword'), null);
  assert.equal(parseGitHubRepository('a/b/c'), null);
  assert.equal(parseGitHubRepository('ssh://git@host.com/a'), null);
});

// ---------------------------------------------------------------------------
// getRemoteOriginUrl (shared reader added to utils/git)
// ---------------------------------------------------------------------------
test('getRemoteOriginUrl: reads origin, null without remote / non-git', () => {
  const withRemote = makeRepo('https://github.com/acme/widget.git');
  assert.equal(getRemoteOriginUrl(withRemote), 'https://github.com/acme/widget.git');

  const noRemote = makeRepo();
  assert.equal(getRemoteOriginUrl(noRemote), null);

  const nonGit = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-nongit-'));
  tmpDirs.push(nonGit);
  assert.equal(getRemoteOriginUrl(nonGit), null);
});

// ---------------------------------------------------------------------------
// detectCurrentRepositoryWithHost
// ---------------------------------------------------------------------------
test('detectCurrentRepositoryWithHost: resolves {host,owner,name} from origin', () => {
  const dir = makeRepo('git@github.com:acme/widget.git');
  assert.deepEqual(detectCurrentRepositoryWithHost(dir), {
    host: 'github.com',
    owner: 'acme',
    name: 'widget',
  });
});

test('detectCurrentRepositoryWithHost: no remote -> null (negative cached)', () => {
  const dir = makeRepo();
  assert.equal(detectCurrentRepositoryWithHost(dir), null);

  // Add a remote AFTER the negative result was cached; cached null wins until clear.
  setRemote(dir, 'https://github.com/acme/widget.git');
  assert.equal(detectCurrentRepositoryWithHost(dir), null);

  clearRepositoryCaches();
  assert.deepEqual(detectCurrentRepositoryWithHost(dir), {
    host: 'github.com',
    owner: 'acme',
    name: 'widget',
  });
});

test('detectCurrentRepositoryWithHost: positive result is cached until cleared', () => {
  const dir = makeRepo('https://github.com/acme/widget.git');
  const first = detectCurrentRepositoryWithHost(dir);
  assert.equal(first?.name, 'widget');

  // Changing the remote does not change the cached value.
  setRemote(dir, 'https://github.com/acme/renamed.git');
  assert.equal(detectCurrentRepositoryWithHost(dir)?.name, 'widget');

  clearRepositoryCaches();
  assert.equal(detectCurrentRepositoryWithHost(dir)?.name, 'renamed');
});

test('detectCurrentRepositoryWithHost: unparseable remote caches null', () => {
  const dir = makeRepo('file:///some/local/path');
  assert.equal(detectCurrentRepositoryWithHost(dir), null);
});

// ---------------------------------------------------------------------------
// detectCurrentRepository (github.com only)
// ---------------------------------------------------------------------------
test('detectCurrentRepository: github.com -> owner/name, GHE -> null', () => {
  const gh = makeRepo('git@github.com:acme/widget.git');
  assert.equal(detectCurrentRepository(gh), 'acme/widget');

  const ghe = makeRepo('git@ghe.corp.com:team/proj.git');
  assert.equal(detectCurrentRepository(ghe), null);

  const none = makeRepo();
  assert.equal(detectCurrentRepository(none), null);
});

// ---------------------------------------------------------------------------
// getCachedRepository (sync, no spawn)
// ---------------------------------------------------------------------------
test('getCachedRepository: null before populate, owner/name after', () => {
  const dir = makeRepo('https://github.com/acme/widget.git');
  // Not populated yet.
  assert.equal(getCachedRepository(dir), null);
  // Populate the cache.
  detectCurrentRepositoryWithHost(dir);
  assert.equal(getCachedRepository(dir), 'acme/widget');
  // Clearing drops the cached accessor result.
  clearRepositoryCaches();
  assert.equal(getCachedRepository(dir), null);
});

test('getCachedRepository: GHE host filtered even when cached', () => {
  const dir = makeRepo('git@ghe.corp.com:team/proj.git');
  detectCurrentRepositoryWithHost(dir);
  assert.equal(getCachedRepository(dir), null);
});
