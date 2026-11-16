import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { enterWorktreeTool, _enterWorktreeHooks } from '../src/tools/enter-worktree';
import { setWorktreeSession } from '../src/worktree-manager';
import type { ToolContext, ProviderConfig } from '../src/types';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, {
    cwd,
    encoding: 'utf-8',
    env: { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '' },
  }).trim();
}

function makeRepo(): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ew-desc-')));
  git(dir, 'init', '-q', '-b', 'main');
  git(dir, 'config', 'user.email', 't@t.com');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  fs.writeFileSync(path.join(dir, 'README.md'), '# r\n');
  git(dir, 'add', '-A');
  git(dir, 'commit', '-q', '-m', 'c1');
  return dir;
}

const provider: ProviderConfig = { baseUrl: 'http://x/v1', apiKey: 'k', model: 'm', maxTokens: 50 };

const ctx = (over: Partial<ToolContext> = {}) =>
  ({ cwd: process.cwd(), abortSignal: new AbortController().signal, ...over } as ToolContext);

describe('enter_worktree — U066 description-derived branch name', () => {
  const dirs: string[] = [];
  const track = (d: string) => { dirs.push(d); return d; };
  beforeEach(() => { setWorktreeSession(null); });
  afterEach(() => {
    mock.reset();
    for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
    setWorktreeSession(null);
  });

  it('derives a readable branch/worktree name from `description` (provider present)', async () => {
    const repo = track(makeRepo());
    const seam = mock.method(_enterWorktreeHooks, 'branchNameFromDescription', async () => 'fix-mobile-login-button');
    const res = await enterWorktreeTool.execute(
      { description: 'fix the mobile login button' },
      ctx({ cwd: repo, _providerConfig: provider }),
    );
    assert.equal(res.isError, undefined, res.output);
    assert.equal(seam.mock.callCount(), 1);
    // The derived slug drives both the on-disk worktree dir and the branch.
    assert.match(res.output, /worktree\/fix-mobile-login-button/);
    assert.equal((res.metadata as any).worktreeBranch, 'worktree/fix-mobile-login-button');
    assert.ok(fs.existsSync(path.join(repo, '.ami', 'worktrees', 'fix-mobile-login-button')));
  });

  it('passes the trimmed description + provider + abort signal to the generator seam', async () => {
    const repo = track(makeRepo());
    const seen: any[] = [];
    mock.method(_enterWorktreeHooks, 'branchNameFromDescription', async (...a: any[]) => { seen.push(a); return 'add-oauth'; });
    const signal = new AbortController().signal;
    await enterWorktreeTool.execute(
      { description: '  add oauth login  ' },
      ctx({ cwd: repo, _providerConfig: provider, abortSignal: signal }),
    );
    assert.equal(seen[0][0], 'add oauth login');
    assert.equal(seen[0][1], provider);
    assert.equal(seen[0][2], signal);
  });

  it('falls back to a random word slug when generation yields an invalid slug', async () => {
    const repo = track(makeRepo());
    // An unusable slug (invalid chars) must not be used as the name.
    mock.method(_enterWorktreeHooks, 'branchNameFromDescription', async () => 'has spaces!!');
    const res = await enterWorktreeTool.execute(
      { description: 'something' },
      ctx({ cwd: repo, _providerConfig: provider }),
    );
    assert.equal(res.isError, undefined, res.output);
    assert.doesNotMatch(res.output, /has spaces/);
    assert.match(res.output, /Branch: worktree\//);
  });

  it('swallows a generator throw and still creates the worktree with the default slug', async () => {
    const repo = track(makeRepo());
    mock.method(_enterWorktreeHooks, 'branchNameFromDescription', async () => { throw new Error('llm down'); });
    const res = await enterWorktreeTool.execute(
      { description: 'whatever' },
      ctx({ cwd: repo, _providerConfig: provider }),
    );
    assert.equal(res.isError, undefined, res.output);
    assert.match(res.output, /Branch: worktree\//);
  });

  it('does NOT call the generator (uses the word slug) when no provider is configured', async () => {
    const repo = track(makeRepo());
    const seam = mock.method(_enterWorktreeHooks, 'branchNameFromDescription', async () => 'should-not-be-used');
    const res = await enterWorktreeTool.execute({ description: 'no provider here' }, ctx({ cwd: repo }));
    assert.equal(res.isError, undefined, res.output);
    assert.equal(seam.mock.callCount(), 0);
    assert.doesNotMatch(res.output, /should-not-be-used/);
  });

  it('rejects description combined with name / path / pr', async () => {
    const repo = track(makeRepo());
    const a = await enterWorktreeTool.execute({ description: 'x', name: 'y' }, ctx({ cwd: repo }));
    assert.equal(a.isError, true);
    assert.match(a.output, /description and name are mutually exclusive/);
    const b = await enterWorktreeTool.execute({ description: 'x', path: '/tmp/zzz' }, ctx({ cwd: repo }));
    assert.equal(b.isError, true);
    assert.match(b.output, /description cannot be combined with path/);
    const c = await enterWorktreeTool.execute({ description: 'x', pr: '#5' }, ctx({ cwd: repo }));
    assert.equal(c.isError, true);
    assert.match(c.output, /description and pr are mutually exclusive/);
  });

  it('accepts description as a known parameter (not rejected as unknown)', async () => {
    const repo = track(makeRepo());
    mock.method(_enterWorktreeHooks, 'branchNameFromDescription', async () => 'ok-slug');
    const res = await enterWorktreeTool.execute({ description: 'do a thing' }, ctx({ cwd: repo, _providerConfig: provider }));
    assert.doesNotMatch(res.output ?? '', /unknown parameter/);
  });
});
