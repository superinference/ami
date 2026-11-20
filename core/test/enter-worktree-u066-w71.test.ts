import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import {
  enterWorktreeTool,
  _enterWorktreeHooks,
  buildGitExecOptions,
} from '../src/tools/enter-worktree';
import { setWorktreeSession } from '../src/worktree-manager';
import type { ToolContext } from '../src/types';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, {
    cwd,
    encoding: 'utf-8',
    env: { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '' },
  }).trim();
}

function makeRepo(initArgs: string[] = ['init', '-b', 'main', '-q']): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ew66w71-'));
  git(dir, ...initArgs);
  git(dir, 'config', 'user.email', 't@t.com');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  fs.writeFileSync(path.join(dir, 'README.md'), '# r\n');
  fs.mkdirSync(path.join(dir, 'sub', 'nested'), { recursive: true });
  fs.writeFileSync(path.join(dir, 'sub', 'nested', 'f.txt'), 'x\n');
  git(dir, 'add', '-A');
  git(dir, 'commit', '-q', '-m', 'init');
  return dir;
}

const ctx = (over: Partial<ToolContext> = {}) =>
  ({ cwd: process.cwd(), abortSignal: new AbortController().signal, ...over } as ToolContext);

describe('enter_worktree — U066 w71 git-hardening parity', () => {
  const repos: string[] = [];
  beforeEach(() => { setWorktreeSession(null); });
  afterEach(() => {
    mock.reset();
    for (const r of repos.splice(0)) fs.rmSync(r, { recursive: true, force: true });
    setWorktreeSession(null);
  });
  const track = (r: string) => { repos.push(r); return r; };

  // ======================================================================
  // GAP (high): Timeout bound on EVERY git subprocess.
  //   RED (revert `timeout: opts.timeout ?? DEFAULT_GIT_TIMEOUT_MS` to
  //   `timeout: opts.timeout`): a call with no explicit timeout leaves the
  //   subprocess unbounded (timeout === undefined) so a wedged git can hang.
  //   GREEN: buildGitExecOptions always supplies a positive bounded timeout.
  // ======================================================================
  it('applies a bounded default timeout when none is given', () => {
    const o = buildGitExecOptions({ cwd: '/x' });
    assert.equal(typeof o.timeout, 'number');
    assert.ok((o.timeout as number) > 0, 'default timeout must be a positive bound');
  });

  it('honors an explicit (shorter) per-call timeout', () => {
    const o = buildGitExecOptions({ cwd: '/x', timeout: 5000 });
    assert.equal(o.timeout, 5000);
  });

  // ======================================================================
  // GAP (high): Prevent credential-prompt hang — non-interactive env on
  //   EVERY git call (centralized GIT_NO_PROMPT_ENV).
  //   RED (revert the `...GIT_NO_PROMPT_ENV` merge): a call with no env (or a
  //   custom env) no longer forces GIT_TERMINAL_PROMPT=0 / GIT_ASKPASS='', so
  //   git can block on an interactive credential prompt.
  //   GREEN: the no-prompt env is merged into every invocation.
  // ======================================================================
  it('forces the non-interactive no-prompt env even when no env is passed', () => {
    const o = buildGitExecOptions({ cwd: '/x' });
    const env = o.env as NodeJS.ProcessEnv;
    assert.equal(env.GIT_TERMINAL_PROMPT, '0');
    assert.equal(env.GIT_ASKPASS, '');
  });

  it('merges the no-prompt env OVER a caller-provided env (preserving other keys)', () => {
    const o = buildGitExecOptions({ cwd: '/x', env: { CUSTOM_KEY: 'v', GIT_TERMINAL_PROMPT: '1' } });
    const env = o.env as NodeJS.ProcessEnv;
    assert.equal(env.CUSTOM_KEY, 'v', 'caller keys are preserved');
    assert.equal(env.GIT_TERMINAL_PROMPT, '0', 'no-prompt wins over a caller override');
    assert.equal(env.GIT_ASKPASS, '');
  });

  // ======================================================================
  // GAP (medium): Honor cancellation / abort signal.
  //   RED (revert the `if (signal?.aborted)` upfront guard): an already-aborted
  //   turn still proceeds to create a worktree.
  //   GREEN: a pre-aborted signal short-circuits with an error, no worktree.
  // ======================================================================
  it('threads the abort signal into the git exec options', () => {
    const ac = new AbortController();
    const o = buildGitExecOptions({ cwd: '/x', signal: ac.signal });
    assert.equal((o as { signal?: AbortSignal }).signal, ac.signal);
  });

  it('short-circuits up front when already aborted (no git subprocess runs)', async () => {
    const repo = track(makeRepo());
    const ac = new AbortController();
    ac.abort();
    // Spy: the upfront guard must return BEFORE any git invocation.
    let execCalls = 0;
    const orig = _enterWorktreeHooks.exec;
    mock.method(_enterWorktreeHooks, 'exec', (args: string[], opts: Parameters<typeof orig>[1]) => {
      execCalls++;
      return orig(args, opts);
    });
    const r = await enterWorktreeTool.execute({ name: 'should-not-exist' }, ctx({ cwd: repo, abortSignal: ac.signal }));
    assert.equal(r.isError, true);
    assert.match(r.output, /abort/i);
    assert.equal(execCalls, 0, 'no git subprocess may run once the turn is already aborted');
    assert.equal(fs.existsSync(path.join(repo, '.ami', 'worktrees', 'should-not-exist')), false);
  });

  it('aborts before the worktree-add git step (no on-disk worktree)', async () => {
    const repo = track(makeRepo());
    const ac = new AbortController();
    // Abort partway through: once the first git probe runs, trip the signal so the
    // re-check right before `git worktree add` short-circuits creation.
    const orig = _enterWorktreeHooks.exec;
    mock.method(_enterWorktreeHooks, 'exec', (args: string[], opts: Parameters<typeof orig>[1]) => {
      if (args[0] === 'rev-parse' && args.includes('--is-inside-work-tree')) ac.abort();
      return orig(args, opts);
    });
    const r = await enterWorktreeTool.execute({ name: 'wt-abort' }, ctx({ cwd: repo, abortSignal: ac.signal }));
    assert.equal(r.isError, true);
    assert.match(r.output, /abort/i);
    assert.equal(fs.existsSync(path.join(repo, '.ami', 'worktrees', 'wt-abort')), false);
  });

  // ======================================================================
  // GAP (medium): Resolve to canonical main-repo root before creating a
  //   worktree (parity with the reference's findCanonicalGitRoot).
  //   RED (revert `const repoRoot = resolveMainRepoRoot(...)` and the
  //   context.cwd->repoRoot swaps): a worktree created while cwd is a
  //   SUBDIRECTORY of the repo lands under the subdirectory, not the repo root.
  //   GREEN: the worktree lands in the repo root's .ami/worktrees/.
  // ======================================================================
  it('creates the worktree under the repo ROOT when invoked from a subdirectory', async () => {
    const repo = track(makeRepo());
    const subdir = path.join(repo, 'sub', 'nested');
    const r = await enterWorktreeTool.execute({ name: 'from-sub' }, ctx({ cwd: subdir }));
    assert.equal(r.isError, undefined, r.output);
    const atRoot = path.join(repo, '.ami', 'worktrees', 'from-sub');
    const atSub = path.join(subdir, '.ami', 'worktrees', 'from-sub');
    assert.equal(fs.existsSync(atRoot), true, 'worktree must land at the repo root');
    assert.equal(fs.existsSync(atSub), false, 'worktree must NOT land under the subdirectory');
    assert.match(r.output, new RegExp(atRoot.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
    // The reported metadata path is the canonical repo-root path.
    assert.equal((r.metadata as { worktreePath?: string }).worktreePath, atRoot);
  });

  it('still creates at the repo root when invoked from the root itself (no regression)', async () => {
    const repo = track(makeRepo());
    const r = await enterWorktreeTool.execute({ name: 'from-root' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    assert.equal(fs.existsSync(path.join(repo, '.ami', 'worktrees', 'from-root')), true);
  });

  it('falls back to the raw cwd when the canonical-root lookup fails', async () => {
    const repo = track(makeRepo());
    const orig = _enterWorktreeHooks.exec;
    mock.method(_enterWorktreeHooks, 'exec', (args: string[], opts: Parameters<typeof orig>[1]) => {
      // Simulate an older git that lacks --path-format: the common-dir probe throws.
      if (args.includes('--git-common-dir')) throw new Error('unknown option: --path-format');
      return orig(args, opts);
    });
    const r = await enterWorktreeTool.execute({ name: 'wt-fallback' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    // Fell back to cwd (the repo root here) → still created successfully.
    assert.equal(fs.existsSync(path.join(repo, '.ami', 'worktrees', 'wt-fallback')), true);
  });
});
