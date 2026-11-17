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
import { readWorktreeHeadSha } from '../src/utils/git';
import { setWorktreeSession, getCurrentWorktreeSession } from '../src/worktree-manager';
import type { ToolContext } from '../src/types';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, {
    cwd,
    encoding: 'utf-8',
    env: { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '' },
  }).trim();
}

function makeRepo(): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ew66w96-')));
  git(dir, 'init', '-b', 'main', '-q');
  git(dir, 'config', 'user.email', 't@t.com');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  fs.writeFileSync(path.join(dir, 'README.md'), '# r\n');
  git(dir, 'add', '-A');
  git(dir, 'commit', '-q', '-m', 'init');
  return dir;
}

const ctx = (over: Partial<ToolContext> = {}) =>
  ({ cwd: process.cwd(), abortSignal: new AbortController().signal, ...over } as ToolContext);

describe('enter_worktree — U066 w96 (maxBuffer bound + fast-resume)', () => {
  const repos: string[] = [];
  beforeEach(() => { setWorktreeSession(null); });
  afterEach(() => {
    mock.reset();
    for (const r of repos.splice(0)) fs.rmSync(r, { recursive: true, force: true });
    setWorktreeSession(null);
  });
  const track = (r: string) => { repos.push(r); return r; };

  // ======================================================================
  // GAP: Bounded output buffer for `git worktree list --porcelain`.
  //   RED (revert `maxBuffer: opts.maxBuffer ?? DEFAULT_GIT_MAX_BUFFER`):
  //   buildGitExecOptions no longer sets a maxBuffer, so a very large worktree
  //   list throws an opaque ENOBUFS at Node's 1 MiB default.
  //   GREEN: a generous default bound is applied to every captured git call.
  // ======================================================================
  it('applies a bounded (generous) default maxBuffer when none is given', () => {
    const o = buildGitExecOptions({ cwd: '/x' }) as { maxBuffer?: number };
    assert.equal(typeof o.maxBuffer, 'number');
    // Must exceed Node's 1 MiB default so large worktree lists do not ENOBUFS.
    assert.ok((o.maxBuffer as number) > 1024 * 1024, 'default maxBuffer must exceed 1 MiB');
  });

  it('honors an explicit per-call maxBuffer', () => {
    const o = buildGitExecOptions({ cwd: '/x', maxBuffer: 4096 }) as { maxBuffer?: number };
    assert.equal(o.maxBuffer, 4096);
  });

  it('passes an explicit maxBuffer to the worktree-list enumeration', async () => {
    const repo = track(makeRepo());
    const wtDir = path.join(repo, '.ami', 'worktrees', 'listed');
    git(repo, 'worktree', 'add', '-q', wtDir, 'HEAD');
    let listOpts: { maxBuffer?: number } | undefined;
    const orig = _enterWorktreeHooks.exec;
    mock.method(_enterWorktreeHooks, 'exec', (args: string[], opts: Parameters<typeof orig>[1]) => {
      if (args[0] === 'worktree' && args[1] === 'list') listOpts = opts as { maxBuffer?: number };
      return orig(args, opts);
    });
    const r = await enterWorktreeTool.execute({ path: wtDir }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    assert.ok(listOpts, 'the worktree-list call must have run');
    assert.equal(typeof listOpts!.maxBuffer, 'number');
    assert.ok((listOpts!.maxBuffer as number) > 1024 * 1024);
  });

  it('turns a worktree-list ENOBUFS overflow into an actionable prune message', async () => {
    const repo = track(makeRepo());
    const wtDir = path.join(repo, '.ami', 'worktrees', 'existing');
    git(repo, 'worktree', 'add', '-q', wtDir, 'HEAD');
    const orig = _enterWorktreeHooks.exec;
    mock.method(_enterWorktreeHooks, 'exec', (args: string[], opts: Parameters<typeof orig>[1]) => {
      if (args[0] === 'worktree' && args[1] === 'list') {
        const e = new Error('stdout maxBuffer length exceeded') as NodeJS.ErrnoException;
        e.code = 'ENOBUFS';
        throw e;
      }
      return orig(args, opts);
    });
    const r = await enterWorktreeTool.execute({ path: wtDir }, ctx({ cwd: repo }));
    assert.equal(r.isError, true);
    assert.match(r.output, /too many registered worktrees/i);
    assert.match(r.output, /prune/i);
  });

  // ======================================================================
  // GAP: readWorktreeHeadSha — direct, subprocess-free HEAD read.
  // ======================================================================
  it('readWorktreeHeadSha returns the HEAD SHA for a real linked worktree', () => {
    const repo = track(makeRepo());
    const wtDir = path.join(repo, '.ami', 'worktrees', 'probe');
    git(repo, 'worktree', 'add', '-q', wtDir, 'HEAD');
    const sha = readWorktreeHeadSha(wtDir);
    assert.ok(sha && /^[0-9a-f]{40}$/i.test(sha), `expected a SHA, got ${sha}`);
    assert.equal(sha, git(wtDir, 'rev-parse', 'HEAD'));
  });

  it('readWorktreeHeadSha returns null for a non-worktree directory and a missing path', () => {
    const repo = track(makeRepo());
    // A plain dir with no `.git` pointer file.
    const plain = path.join(repo, '.ami', 'worktrees', 'plain');
    fs.mkdirSync(plain, { recursive: true });
    assert.equal(readWorktreeHeadSha(plain), null);
    assert.equal(readWorktreeHeadSha(path.join(repo, 'does-not-exist')), null);
    // The main repo's `.git` is a directory, not a `gitdir:` pointer → null.
    assert.equal(readWorktreeHeadSha(repo), null);
  });

  // ======================================================================
  // GAP: Fast-resume an existing named worktree instead of hard-erroring.
  //   RED (revert the fast-resume block to the old
  //   `return { output: 'Error: worktree "..." already exists', isError: true }`):
  //   re-entering by name errors instead of resuming.
  //   GREEN: enter_worktree is idempotent by name — it resumes the session.
  // ======================================================================
  it('resumes an existing worktree by name instead of erroring (idempotent)', async () => {
    const repo = track(makeRepo());
    const first = await enterWorktreeTool.execute({ name: 'resume-me' }, ctx({ cwd: repo }));
    assert.equal(first.isError, undefined, first.output);
    const wtDir = path.join(repo, '.ami', 'worktrees', 'resume-me');
    assert.equal(fs.existsSync(wtDir), true);
    setWorktreeSession(null);

    // Second call, same name, from the repo root — must RESUME, not error.
    let sawAdd = false;
    let sawFetch = false;
    const orig = _enterWorktreeHooks.exec;
    mock.method(_enterWorktreeHooks, 'exec', (args: string[], opts: Parameters<typeof orig>[1]) => {
      if (args[0] === 'worktree' && args[1] === 'add') sawAdd = true;
      if (args[0] === 'fetch') sawFetch = true;
      return orig(args, opts);
    });
    const second = await enterWorktreeTool.execute({ name: 'resume-me' }, ctx({ cwd: repo }));
    assert.equal(second.isError, undefined, second.output);
    assert.match(second.output, /resumed existing worktree/i);
    assert.equal((second.metadata as { existed?: boolean }).existed, true);
    assert.equal((second.metadata as { worktreePath?: string }).worktreePath, wtDir);
    assert.equal((second.metadata as { worktreeBranch?: string }).worktreeBranch, 'worktree/resume-me');
    assert.equal(sawAdd, false, 'resume must NOT run `git worktree add`');
    assert.equal(sawFetch, false, 'resume must NOT fetch');
  });

  it('resume re-establishes the session and switches the context cwd', async () => {
    const repo = track(makeRepo());
    const first = await enterWorktreeTool.execute({ name: 'sw' }, ctx({ cwd: repo }));
    assert.equal(first.isError, undefined, first.output);
    setWorktreeSession(null);
    const wtDir = path.join(repo, '.ami', 'worktrees', 'sw');
    const c = ctx({ cwd: repo });
    const second = await enterWorktreeTool.execute({ name: 'sw' }, c);
    assert.equal(second.isError, undefined, second.output);
    assert.equal((c as unknown as { cwd: string }).cwd, wtDir, 'context cwd must switch into the resumed worktree');
    const session = getCurrentWorktreeSession();
    assert.ok(session, 'a worktree session must be re-established on resume');
    assert.equal(session!.worktreePath, wtDir);
  });

  it('errors (does not resume) when the dir exists but is not a valid worktree', async () => {
    const repo = track(makeRepo());
    // A debris directory at the computed worktree path with no `.git` pointer.
    const wtDir = path.join(repo, '.ami', 'worktrees', 'debris');
    fs.mkdirSync(wtDir, { recursive: true });
    fs.writeFileSync(path.join(wtDir, 'leftover.txt'), 'x\n');
    const r = await enterWorktreeTool.execute({ name: 'debris' }, ctx({ cwd: repo }));
    assert.equal(r.isError, true);
    assert.match(r.output, /not a valid git worktree/i);
  });

  it('a resume failure clears session state without tearing the worktree down', async () => {
    const repo = track(makeRepo());
    const first = await enterWorktreeTool.execute({ name: 'keep-safe' }, ctx({ cwd: repo }));
    assert.equal(first.isError, undefined, first.output);
    setWorktreeSession(null);
    const wtDir = path.join(repo, '.ami', 'worktrees', 'keep-safe');
    // Force the post-resume setup (symlink step) to blow up.
    mock.method(_enterWorktreeHooks, 'symlinkLargeDirs', () => { throw new Error('boom'); });
    const r = await enterWorktreeTool.execute({ name: 'keep-safe' }, ctx({ cwd: repo }));
    assert.equal(r.isError, true);
    assert.match(r.output, /failed to resume/i);
    // The pre-existing worktree must NOT have been removed by the failed resume.
    assert.equal(fs.existsSync(wtDir), true, 'a failed resume must not delete an existing worktree');
    assert.equal(getCurrentWorktreeSession(), null, 'partial session state must be cleared');
  });
});
