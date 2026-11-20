/**
 * Tests for the persistent GitHub-repo -> local-clone-path registry with MRU ordering
 * (parity: the reference's `githubRepoPathMapping.ts`). All state lives in the GLOBAL
 * config (`~/.ami/config.json`); each test points HOME at a throwaway dir and builds
 * real temp git repos so detection/validation exercise actual `git` plumbing.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';

import {
  updateGithubRepoPathMapping,
  getKnownPathsForRepo,
  filterExistingPaths,
  validateRepoAtPath,
  removePathFromRepo,
} from '../src/github-repo-path-mapping';
import {
  saveGlobalConfig,
  loadGlobalConfig,
  validateProjectConfigValues,
  KNOWN_CONFIG_KEYS,
} from '../src/config';
import { canonicalGitRoot } from '../src/reference-journal';
import { clearRepositoryCaches } from '../src/detect-repository';

const tmpDirs: string[] = [];

function makeRepo(remoteUrl?: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-ghrepo-path-'));
  tmpDirs.push(dir);
  const run = (args: string[]) =>
    execFileSync('git', args, { cwd: dir, stdio: ['ignore', 'ignore', 'ignore'] });
  run(['init', '-q']);
  if (remoteUrl !== undefined) run(['remote', 'add', 'origin', remoteUrl]);
  return dir;
}

describe('github repo path mapping (global config, MRU registry)', () => {
  let home: string;
  let prevHome: string | undefined;
  let prevUserProfile: string | undefined;

  beforeEach(() => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-ghrepo-home-'));
    prevHome = process.env.HOME;
    prevUserProfile = process.env.USERPROFILE;
    process.env.HOME = home;
    delete process.env.USERPROFILE;
  });

  afterEach(() => {
    if (prevHome !== undefined) process.env.HOME = prevHome; else delete process.env.HOME;
    if (prevUserProfile !== undefined) process.env.USERPROFILE = prevUserProfile; else delete process.env.USERPROFILE;
    clearRepositoryCaches();
    fs.rmSync(home, { recursive: true, force: true });
    while (tmpDirs.length) {
      const d = tmpDirs.pop()!;
      try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
    }
  });

  // --- getKnownPathsForRepo ------------------------------------------------
  it('getKnownPathsForRepo returns [] when nothing is recorded', () => {
    assert.deepEqual(getKnownPathsForRepo('acme/widget'), []);
  });

  // --- updateGithubRepoPathMapping -----------------------------------------
  it('records the canonical git root for the current github repo', async () => {
    const repo = makeRepo('git@github.com:acme/widget.git');
    await updateGithubRepoPathMapping(repo);
    assert.deepEqual(getKnownPathsForRepo('acme/widget'), [canonicalGitRoot(repo)]);
  });

  it('promotes the most-recently-used clone to the front (MRU)', async () => {
    const repoA = makeRepo('git@github.com:acme/widget.git');
    const repoB = makeRepo('https://github.com/acme/widget.git');
    const a = canonicalGitRoot(repoA);
    const b = canonicalGitRoot(repoB);

    await updateGithubRepoPathMapping(repoA);
    assert.deepEqual(getKnownPathsForRepo('acme/widget'), [a]);

    await updateGithubRepoPathMapping(repoB);
    assert.deepEqual(getKnownPathsForRepo('acme/widget'), [b, a]);

    // Re-using repoA promotes it back to the front without duplicating it.
    await updateGithubRepoPathMapping(repoA);
    assert.deepEqual(getKnownPathsForRepo('acme/widget'), [a, b]);
  });

  it('short-circuits (no write) when the path is already at the front', async () => {
    const repo = makeRepo('git@github.com:acme/widget.git');
    await updateGithubRepoPathMapping(repo);
    const before = JSON.stringify(loadGlobalConfig()?.githubRepoPaths);
    await updateGithubRepoPathMapping(repo);
    const after = JSON.stringify(loadGlobalConfig()?.githubRepoPaths);
    assert.equal(after, before);
    assert.deepEqual(getKnownPathsForRepo('acme/widget'), [canonicalGitRoot(repo)]);
  });

  it('keys the registry case-insensitively', async () => {
    const repo = makeRepo('git@github.com:Acme/Widget.git');
    await updateGithubRepoPathMapping(repo);
    // Stored under the lowercased key, readable under any case.
    assert.deepEqual(getKnownPathsForRepo('acme/widget'), [canonicalGitRoot(repo)]);
    assert.deepEqual(getKnownPathsForRepo('ACME/WIDGET'), [canonicalGitRoot(repo)]);
    assert.ok(loadGlobalConfig()?.githubRepoPaths?.['acme/widget']);
  });

  it('no-ops when cwd has no origin remote', async () => {
    const repo = makeRepo();
    await updateGithubRepoPathMapping(repo);
    assert.equal(loadGlobalConfig()?.githubRepoPaths, undefined);
  });

  it('no-ops when the repo is not hosted on github.com', async () => {
    const repo = makeRepo('git@gitlab.com:acme/widget.git');
    await updateGithubRepoPathMapping(repo);
    assert.equal(loadGlobalConfig()?.githubRepoPaths, undefined);
  });

  // --- filterExistingPaths -------------------------------------------------
  it('filterExistingPaths keeps only existing paths, preserving order', async () => {
    const a = makeRepo('git@github.com:acme/widget.git');
    const b = makeRepo('git@github.com:acme/widget.git');
    const missing = path.join(os.tmpdir(), 'ami-ghrepo-does-not-exist-xyz');
    const result = await filterExistingPaths([a, missing, b]);
    assert.deepEqual(result, [a, b]);
  });

  it('filterExistingPaths returns [] for an all-missing list', async () => {
    const result = await filterExistingPaths(['/no/such/a', '/no/such/b']);
    assert.deepEqual(result, []);
  });

  // --- validateRepoAtPath --------------------------------------------------
  it('validateRepoAtPath confirms a matching repo (case-insensitive)', async () => {
    const repo = makeRepo('git@github.com:acme/widget.git');
    assert.equal(await validateRepoAtPath(repo, 'acme/widget'), true);
    assert.equal(await validateRepoAtPath(repo, 'ACME/WIDGET'), true);
  });

  it('validateRepoAtPath rejects a different repo', async () => {
    const repo = makeRepo('git@github.com:acme/widget.git');
    assert.equal(await validateRepoAtPath(repo, 'acme/other'), false);
  });

  it('validateRepoAtPath rejects a path with no origin remote', async () => {
    const repo = makeRepo();
    assert.equal(await validateRepoAtPath(repo, 'acme/widget'), false);
  });

  it('validateRepoAtPath rejects a non-github remote (unparseable owner/repo)', async () => {
    const repo = makeRepo('git@gitlab.com:acme/widget.git');
    assert.equal(await validateRepoAtPath(repo, 'acme/widget'), false);
  });

  // --- removePathFromRepo --------------------------------------------------
  it('removePathFromRepo prunes one path and keeps the rest', () => {
    saveGlobalConfig(cur => ({ ...cur, githubRepoPaths: { 'acme/widget': ['/p1', '/p2'] } }));
    removePathFromRepo('acme/widget', '/p1');
    assert.deepEqual(getKnownPathsForRepo('acme/widget'), ['/p2']);
  });

  it('removePathFromRepo deletes the repo key when the list empties', () => {
    saveGlobalConfig(cur => ({ ...cur, githubRepoPaths: { 'acme/widget': ['/p1'], 'acme/other': ['/q1'] } }));
    removePathFromRepo('acme/widget', '/p1');
    assert.deepEqual(getKnownPathsForRepo('acme/widget'), []);
    // The key is removed entirely, not left as an empty array; siblings are untouched.
    const map = loadGlobalConfig()?.githubRepoPaths ?? {};
    assert.equal('acme/widget' in map, false);
    assert.deepEqual(map['acme/other'], ['/q1']);
  });

  it('removePathFromRepo is a no-op when the path is not tracked', () => {
    saveGlobalConfig(cur => ({ ...cur, githubRepoPaths: { 'acme/widget': ['/p1'] } }));
    const before = JSON.stringify(loadGlobalConfig()?.githubRepoPaths);
    removePathFromRepo('acme/widget', '/not-there');
    assert.equal(JSON.stringify(loadGlobalConfig()?.githubRepoPaths), before);
  });

  it('removePathFromRepo matches the repo key case-insensitively', () => {
    saveGlobalConfig(cur => ({ ...cur, githubRepoPaths: { 'acme/widget': ['/p1', '/p2'] } }));
    removePathFromRepo('ACME/WIDGET', '/p1');
    assert.deepEqual(getKnownPathsForRepo('acme/widget'), ['/p2']);
  });

  // --- config schema -------------------------------------------------------
  it('githubRepoPaths is a recognized config key', () => {
    assert.ok(KNOWN_CONFIG_KEYS.has('githubRepoPaths'));
  });

  it('validateProjectConfigValues flags a non-array githubRepoPaths entry', () => {
    const issues = validateProjectConfigValues({
      githubRepoPaths: { 'acme/widget': 'not-an-array' },
    } as never);
    assert.ok(issues.some(i => i.field === 'githubRepoPaths.acme/widget'));
  });

  it('validateProjectConfigValues accepts a well-formed githubRepoPaths map', () => {
    const issues = validateProjectConfigValues({
      githubRepoPaths: { 'acme/widget': ['/p1', '/p2'] },
    } as never);
    assert.equal(issues.some(i => i.field.startsWith('githubRepoPaths')), false);
  });
});
