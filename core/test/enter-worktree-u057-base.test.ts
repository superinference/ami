import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as child_process from 'child_process';

import { enterWorktreeTool } from '../src/tools/enter-worktree';
import { setWorktreeSession } from '../src/worktree-manager';
import { findRemoteBase, isShallowClone } from '../src/utils/git';
import type { ToolContext } from '../src/types';

// U057 — base-ref resolution hardening for enter_worktree. These exercise two
// previously-unwired core/git helpers that the worktree subsystem must consume:
//   1. isShallowClone  → explain a resolvable-looking base_ref that is missing
//      because the clone's history is truncated (shallow-clone edge-case
//      fallback, parity with Claude's shallow detection).
//   2. findRemoteBase  → default worktree base follows git's own priority
//      (upstream @{u} → origin/HEAD → candidate list) instead of a hardcoded
//      origin/<default>.

const GIT_ENV = { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '' };

function git(args: string, cwd: string): string {
  return child_process.execSync(`git ${args}`, { cwd, encoding: 'utf-8', env: GIT_ENV }).toString();
}

function mkTmp(prefix = 'wt-u057-'): string {
  return fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), prefix)));
}

function initRepoWithCommit(dir: string): void {
  git('init -b main', dir);
  git('config user.email "t@t.com"', dir);
  git('config user.name "Test"', dir);
  git('config commit.gpgsign false', dir);
  fs.writeFileSync(path.join(dir, 'README.md'), '# repo\n');
  git('add README.md', dir);
  git('commit -m init', dir);
}

function ctx(overrides?: Partial<ToolContext> & Record<string, unknown>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

const tmpDirs: string[] = [];
function track(dir: string): string { tmpDirs.push(dir); return dir; }

after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
  setWorktreeSession(null);
});

describe('U057: shallow-clone edge-case fallback on an unresolvable base_ref', () => {
  it('appends a shallow-clone hint (and the fetch --unshallow command) when the ref is missing from a truncated history', () => {
    // Build an upstream with TWO commits, then make a depth-1 (shallow) clone so
    // the first commit's sha is genuinely absent from the clone's history.
    const upstream = track(mkTmp('wt-u057-up-'));
    initRepoWithCommit(upstream);
    const firstSha = git('rev-parse HEAD', upstream).trim();
    fs.writeFileSync(path.join(upstream, 'second.txt'), 'two\n');
    git('add second.txt', upstream);
    git('commit -m second', upstream);

    const shallow = track(mkTmp('wt-u057-shallow-'));
    child_process.execFileSync('git', ['clone', '-q', '--depth', '1', 'file://' + upstream, shallow], {
      env: GIT_ENV,
      stdio: 'ignore',
    });
    git('config user.email "t@t.com"', shallow);
    git('config user.name "Test"', shallow);

    assert.equal(isShallowClone(shallow), true, 'precondition: clone must be shallow');

    return enterWorktreeTool
      .execute({ name: 'wt-shallow', base_ref: firstSha }, ctx({ cwd: shallow }))
      .then((r) => {
        assert.equal(r.isError, true);
        assert.match(r.output, /not a valid git ref/);
        // The discriminating assertion (fails on fix-revert): without the
        // isShallowClone wiring the message has no shallow-clone explanation.
        assert.match(r.output, /shallow clone/);
        assert.match(r.output, /git fetch --unshallow/);
      });
  });

  it('does NOT add the shallow hint for an unresolvable ref in a FULL clone', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    assert.equal(isShallowClone(repo), false);
    const r = await enterWorktreeTool.execute(
      { name: 'wt-full-badref', base_ref: 'nonexistent-ref-xyz' },
      ctx({ cwd: repo }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /not a valid git ref/);
    assert.doesNotMatch(r.output, /shallow clone/);
  });
});

describe('U057: default worktree base follows findRemoteBase priority (upstream @{u})', () => {
  it('branches a worktree from the current branch upstream ref, not a hardcoded origin/<default>', async () => {
    // A repo whose checked-out branch tracks origin/release (NOT main): the
    // default base must be the tracking ref origin/release.
    const bare = track(mkTmp('wt-u057-bare-'));
    git('init --bare -b main', bare);

    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    git(`remote add origin ${JSON.stringify(bare)}`, repo);
    // Create a distinct release branch and publish both.
    git('push -u origin main', repo);
    git('checkout -b release', repo);
    fs.writeFileSync(path.join(repo, 'rel.txt'), 'rel\n');
    git('add rel.txt', repo);
    git('commit -m rel', repo);
    git('push -u origin release', repo);

    // HEAD is on release, tracking origin/release. findRemoteBase must return it.
    assert.equal(findRemoteBase(repo), 'origin/release');

    const r = await enterWorktreeTool.execute({ name: 'wt-from-upstream' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Worktree created/);
    // Discriminating assertion (fails on fix-revert): the old code hardcoded the
    // label to origin/<defaultBranch> (origin/main here), never the upstream.
    assert.match(r.output, /based on origin\/release/);
  });

  it('still falls back to origin/<default-branch> when no upstream/remote resolves', async () => {
    const repo = track(mkTmp());
    initRepoWithCommit(repo);
    assert.equal(findRemoteBase(repo), null);
    const r = await enterWorktreeTool.execute({ name: 'wt-no-remote' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined);
    assert.match(r.output, /based on origin\/main/);
  });
});
