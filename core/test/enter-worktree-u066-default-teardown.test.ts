import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { enterWorktreeTool, _enterWorktreeHooks } from '../src/tools/enter-worktree';
import { setWorktreeSession, getCurrentWorktreeSession } from '../src/worktree-manager';
import type { ToolContext } from '../src/types';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, { cwd, encoding: 'utf-8' }).trim();
}

function makeRepo(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ew66dt-'));
  git(dir, 'init', '-q');
  git(dir, 'config', 'user.email', 't@t.com');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  fs.writeFileSync(path.join(dir, 'README.md'), '# r\n');
  git(dir, 'add', 'README.md');
  git(dir, 'commit', '-q', '-m', 'init');
  return dir;
}

const ctx = (over: Partial<ToolContext> = {}) =>
  ({ cwd: process.cwd(), abortSignal: new AbortController().signal, ...over } as ToolContext);

describe('enter_worktree — U066 default-name + post-add teardown', () => {
  const repos: string[] = [];
  beforeEach(() => { setWorktreeSession(null); });
  afterEach(() => {
    mock.restoreAll();
    for (const r of repos.splice(0)) fs.rmSync(r, { recursive: true, force: true });
    setWorktreeSession(null);
  });
  const track = (r: string) => { repos.push(r); return r; };

  // --- GAP: default worktree name derived from a human-readable word slug ----
  // RED (revert defaultWorktreeName → `wt-<hex>`): the branch is `worktree/wt-…`,
  // which does NOT match the adjective-noun word-slug shape asserted below.
  it('omitted name → memorable adjective-noun word slug, not opaque hex', async () => {
    const dir = track(makeRepo());
    const res = await enterWorktreeTool.execute({}, ctx({ cwd: dir }));
    assert.equal(res.isError, undefined);
    const m = res.output.match(/Branch: worktree\/([^\s]+)/);
    assert.ok(m, `expected a Branch line, got:\n${res.output}`);
    const slug = m![1];
    assert.match(slug, /^[a-z]+-[a-z]+$/, `expected adjective-noun slug, got "${slug}"`);
    assert.doesNotMatch(slug, /^wt-/, 'must not fall back to the opaque wt-<hex> name');
    // The named-import default actually routes through the shared words corpus.
    assert.ok(fs.existsSync(path.join(dir, '.ami', 'worktrees', slug)));
  });

  // --- GAP: collision retry → bounded fallback suffix ------------------------
  // With a generator that always returns the SAME (already-taken) slug, the
  // retry loop exhausts and appends random hex so creation still succeeds.
  it('collision on generated name → bounded hex-suffixed fallback', async () => {
    const dir = track(makeRepo());
    fs.mkdirSync(path.join(dir, '.ami', 'worktrees', 'fixed-slug'), { recursive: true });
    mock.method(_enterWorktreeHooks, 'generateSlug', () => 'fixed-slug');
    const res = await enterWorktreeTool.execute({}, ctx({ cwd: dir }));
    assert.equal(res.isError, undefined, res.output);
    const m = res.output.match(/Branch: worktree\/([^\s]+)/);
    assert.ok(m);
    assert.match(m![1], /^fixed-slug-[0-9a-f]{4}$/, `expected hex-suffixed fallback, got "${m![1]}"`);
  });

  // --- GAP: tear down a half-created worktree when post-add setup fails ------
  // RED (remove the try/catch teardown): `git worktree add` already ran, so the
  // worktree dir + branch leak and the tool returns success despite broken wiring.
  it('post-add setup failure rolls back the worktree (remove + branch -D)', async () => {
    const dir = track(makeRepo());
    mock.method(_enterWorktreeHooks, 'symlinkLargeDirs', () => { throw new Error('symlink boom'); });
    const res = await enterWorktreeTool.execute({ name: 'doomed' }, ctx({ cwd: dir }));
    assert.equal(res.isError, true);
    assert.match(res.output, /rolled back/);
    assert.match(res.output, /symlink boom/);
    // Worktree dir removed from disk.
    assert.equal(fs.existsSync(path.join(dir, '.ami', 'worktrees', 'doomed')), false);
    // No longer registered with git.
    const list = git(dir, 'worktree', 'list', '--porcelain');
    assert.doesNotMatch(list, /doomed/);
    // Branch dropped.
    const branches = git(dir, 'branch', '--list', 'worktree/doomed');
    assert.equal(branches, '');
    // Session state cleared.
    assert.equal(getCurrentWorktreeSession(), null);
  });
});
