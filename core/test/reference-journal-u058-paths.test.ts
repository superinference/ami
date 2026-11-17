/**
 * Claude-parity (U058): additive path/identity knobs on reference-journal —
 *  - getUserBinDir()          (parity: xdg.ts getUserBinDir — ~/.local/bin)
 *  - resolveCacheDir()        (parity: cachePaths.ts envPaths().cache — XDG/platform cache base)
 *  - normalizeGitRemoteUrl()  (parity: git.ts normalizeGitRemoteUrl)
 *  - repoRemoteHash()         (parity: git.ts getRepoRemoteHash — sha256(remote)[:16])
 *  - subagentJournalPathFor() grouping subdir (parity: sessionStorage.ts agentId→subdir)
 *
 * Each test is a discriminator — it fails if the corresponding behavior regresses.
 * Run: cd core/core && node --import tsx --test test/reference-journal-u058-paths.test.ts
 */
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execSync } from 'child_process';
import * as crypto from 'crypto';
import {
  getUserBinDir,
  resolveCacheDir,
  normalizeGitRemoteUrl,
  repoRemoteHash,
  subagentJournalPathFor,
} from '../src/reference-journal';

const tmpdirs: string[] = [];
function mkTmp(prefix: string): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  tmpdirs.push(d);
  return d;
}
afterEach(() => {
  for (const d of tmpdirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
});

const env = (o: Record<string, string | undefined>) => o as unknown as NodeJS.ProcessEnv;

// ---------------------------------------------------------------------------
// getUserBinDir
// ---------------------------------------------------------------------------
describe('getUserBinDir (U058)', () => {
  it('returns ~/.local/bin resolved against HOME', () => {
    assert.equal(getUserBinDir(env({ HOME: '/home/me' })), path.join('/home/me', '.local', 'bin'));
  });
  it('honors an injected homedir seam over HOME', () => {
    assert.equal(getUserBinDir(env({ HOME: '/home/me' }), '/inject'), path.join('/inject', '.local', 'bin'));
  });
});

// ---------------------------------------------------------------------------
// resolveCacheDir
// ---------------------------------------------------------------------------
describe('resolveCacheDir (U058)', () => {
  it('posix: honors XDG_CACHE_HOME when set', () => {
    assert.equal(
      resolveCacheDir(env({ HOME: '/home/me', XDG_CACHE_HOME: '/xdg/cache' }), undefined, 'linux'),
      path.join('/xdg/cache', 'superinference'),
    );
  });
  it('posix: falls back to ~/.cache without XDG_CACHE_HOME', () => {
    assert.equal(
      resolveCacheDir(env({ HOME: '/home/me' }), undefined, 'linux'),
      path.join('/home/me', '.cache', 'superinference'),
    );
  });
  it('darwin: ~/Library/Caches/superinference', () => {
    assert.equal(
      resolveCacheDir(env({ HOME: '/Users/me' }), undefined, 'darwin'),
      path.join('/Users/me', 'Library', 'Caches', 'superinference'),
    );
  });
  it('win32: honors %LOCALAPPDATA%', () => {
    assert.equal(
      resolveCacheDir(env({ HOME: 'C:\\Users\\me', LOCALAPPDATA: 'C:\\Users\\me\\AppData\\Local' }), undefined, 'win32'),
      path.join('C:\\Users\\me\\AppData\\Local', 'superinference', 'Cache'),
    );
  });
  it('win32: falls back to ~/AppData/Local without LOCALAPPDATA', () => {
    assert.equal(
      resolveCacheDir(env({ HOME: '/winhome' }), undefined, 'win32'),
      path.join('/winhome', 'AppData', 'Local', 'superinference', 'Cache'),
    );
  });
  it('is SEPARATE from the data dir (cache knob distinct from config/data)', () => {
    // No override: cache lands under the platform cache base, not under ~/.ami.
    const cache = resolveCacheDir(env({ HOME: '/home/me' }), undefined, 'linux');
    assert.ok(!cache.includes(path.join('home', 'me', '.ami')), 'cache must not be the ~/.ami data dir');
  });
  it('AMI_CACHE_DIR override wins and is tilde-expanded', () => {
    assert.equal(
      resolveCacheDir(env({ HOME: '/home/me', AMI_CACHE_DIR: '~/mycache' }), undefined, 'linux'),
      path.join('/home/me', 'mycache'),
    );
  });
  it('a dangerous AMI_CACHE_DIR override is rejected (falls through to platform base)', () => {
    assert.equal(
      resolveCacheDir(env({ HOME: '/home/me', AMI_CACHE_DIR: '/', XDG_CACHE_HOME: '/xdg' }), undefined, 'linux'),
      path.join('/xdg', 'superinference'),
    );
  });
});

// ---------------------------------------------------------------------------
// normalizeGitRemoteUrl
// ---------------------------------------------------------------------------
describe('normalizeGitRemoteUrl (U058)', () => {
  it('SSH shorthand and HTTPS of the same repo normalize identically', () => {
    const https = normalizeGitRemoteUrl('https://github.com/Owner/Repo.git');
    const ssh = normalizeGitRemoteUrl('git@github.com:Owner/Repo.git');
    assert.equal(https, 'github.com/owner/repo');
    assert.equal(ssh, 'github.com/owner/repo');
    assert.equal(https, ssh);
  });
  it('strips a trailing .git and lower-cases', () => {
    assert.equal(normalizeGitRemoteUrl('https://gitlab.com/A/B'), 'gitlab.com/a/b');
    assert.equal(normalizeGitRemoteUrl('ssh://git@host.example/A/B.git'), 'host.example/a/b');
  });
  it('returns null for an empty or unparseable URL', () => {
    assert.equal(normalizeGitRemoteUrl('   '), null);
    assert.equal(normalizeGitRemoteUrl('not-a-url'), null);
  });
});

// ---------------------------------------------------------------------------
// repoRemoteHash
// ---------------------------------------------------------------------------
function initRepo(dir: string, remote?: string): void {
  execSync('git init -q', { cwd: dir });
  if (remote) execSync(`git remote add origin ${remote}`, { cwd: dir });
}

describe('repoRemoteHash (U058)', () => {
  it('derives a 16-hex sha256 of the normalized remote', () => {
    const d = mkTmp('refjrnl-remote-');
    initRepo(d, 'https://github.com/Owner/Repo.git');
    const h = repoRemoteHash(d);
    const expected = crypto.createHash('sha256').update('github.com/owner/repo').digest('hex').slice(0, 16);
    assert.equal(h, expected);
    assert.match(h!, /^[0-9a-f]{16}$/);
  });
  it('is identical across SSH and HTTPS clones of the same repo', () => {
    const https = mkTmp('refjrnl-https-');
    const ssh = mkTmp('refjrnl-ssh-');
    initRepo(https, 'https://github.com/Owner/Repo.git');
    initRepo(ssh, 'git@github.com:Owner/Repo.git');
    assert.equal(repoRemoteHash(https), repoRemoteHash(ssh));
  });
  it('does not leak the repo name', () => {
    const d = mkTmp('refjrnl-leak-');
    initRepo(d, 'https://github.com/secretorg/secretrepo.git');
    const h = repoRemoteHash(d)!;
    assert.ok(!h.includes('secretrepo'));
    assert.ok(!h.includes('secretorg'));
  });
  it('returns null for a repo with no origin remote', () => {
    const d = mkTmp('refjrnl-noremote-');
    initRepo(d);
    assert.equal(repoRemoteHash(d), null);
  });
  it('returns null outside a git repo', () => {
    const d = mkTmp('refjrnl-norepo-');
    assert.equal(repoRemoteHash(d), null);
  });
});

// ---------------------------------------------------------------------------
// subagentJournalPathFor grouping subdir
// ---------------------------------------------------------------------------
describe('subagentJournalPathFor grouping subdir (U058)', () => {
  const E = env({ SUPERINFERENCE_DATA_DIR: '/data' });
  const CWD = '/home/me/ui';
  const flat = subagentJournalPathFor(CWD, 'ag1', E);

  it('default (no subdir) keeps the flat subagents/agent-<id>.jsonl layout', () => {
    assert.ok(flat.endsWith(path.join('subagents', 'agent-ag1.jsonl')));
  });
  it('nests related subagents under a grouping subdir', () => {
    const grouped = subagentJournalPathFor(CWD, 'ag1', E, 'workflows/run42');
    assert.ok(grouped.endsWith(path.join('subagents', 'workflows', 'run42', 'agent-ag1.jsonl')));
    // same project root, deeper by the two grouping segments
    assert.ok(grouped.startsWith(flat.slice(0, flat.indexOf(path.join('subagents')) + 'subagents'.length)));
  });
  it('a traversal subdir falls back to the flat dir (never escapes subagents/)', () => {
    const p = subagentJournalPathFor(CWD, 'ag1', E, '../../etc');
    assert.equal(p, flat);
  });
  it('an empty/whitespace subdir falls back to the flat dir', () => {
    assert.equal(subagentJournalPathFor(CWD, 'ag1', E, '   '), flat);
  });
});
