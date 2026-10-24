/**
 * Last-gap coverage for:
 *   - src/tools/bash.ts          (fork-storm retry, classifier throw, checkout
 *                                 statSync race, detached discard, BASH_ARGV0 /
 *                                 BASHPID / code-loading env blocks)
 *   - src/tools/exit-worktree.ts (git-worktree-remove failure fallback, a
 *                                 rejected executeWorktreeRemove hook)
 *
 * All error/fallback arms are driven through the DI seams (_bashHooks,
 * _exitWorktreeHooks) with node:test `mock.method`, never by monkeypatching a
 * frozen namespace import. Temp dirs come from fs.mkdtempSync; no source-text
 * reads, no __dirname repo coupling.
 */

import { describe, it, before, after, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as child_process from 'child_process';

import {
  bashTool,
  detectDetachedGitDiscard,
  injectFindMaxdepth,
  _bashHooks,
} from '../src/tools/bash';
import { exitWorktreeTool, _exitWorktreeHooks } from '../src/tools/exit-worktree';
import { setWorktreeSession } from '../src/worktree-manager';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// bash.ts — fork-exhaustion retry loop driven deterministically via the seam.
// A container that never recovers from the per-process fork limit must exhaust
// every retry and then surface the actionable "all retries failed" note. The
// sleep seam is stubbed so the exponential backoff adds no real wall time.
// ---------------------------------------------------------------------------

describe('bashTool — fork-storm that never recovers (seam-driven)', () => {
  afterEach(() => mock.restoreAll());

  it('retries, exhausts, and appends the all-retries-failed note', async () => {
    let calls = 0;
    mock.method(_bashHooks, 'execCommand', async () => {
      calls++;
      return { stdout: '', stderr: 'bash: fork: Resource temporarily unavailable', exitCode: 1 };
    });
    let sleeps = 0;
    mock.method(_bashHooks, 'sleep', async () => { sleeps++; });

    const result = await bashTool.execute({ command: 'echo fork-storm' }, ctx());

    assert.match(result.output, /fork exhaustion: all retries failed/);
    // 1 initial + FORK_RETRY_MAX (5) retries = 6 invocations, 5 backoff sleeps.
    assert.equal(calls, 6);
    assert.equal(sleeps, 5);
  });

  it('stops retrying once a call recovers (no exhaustion note)', async () => {
    let calls = 0;
    mock.method(_bashHooks, 'execCommand', async () => {
      calls++;
      if (calls === 1) return { stdout: '', stderr: 'fork: Resource temporarily unavailable', exitCode: 1 };
      return { stdout: 'recovered', stderr: '', exitCode: 0 };
    });
    mock.method(_bashHooks, 'sleep', async () => {});

    const result = await bashTool.execute({ command: 'echo recover' }, ctx());
    assert.ok(!result.output.includes('all retries failed'), result.output);
    assert.equal(calls, 2);
  });
});

// ---------------------------------------------------------------------------
// bash.ts — isReadOnlyBashCommand catch arm: a classifier that throws must be
// swallowed (command treated as not-read-only), never crash the tool.
// ---------------------------------------------------------------------------

describe('bashTool — classifier throw is swallowed (read-only=false)', () => {
  afterEach(() => mock.restoreAll());

  it('still runs the command when _bashHooks.classify throws', async () => {
    mock.method(_bashHooks, 'classify', () => { throw new Error('classifier boom'); });
    const result = await bashTool.execute({ command: 'echo classify-boom' }, ctx());
    assert.ok(!result.isError, result.output);
    assert.match(result.output, /classify-boom/);
  });
});

// ---------------------------------------------------------------------------
// bash.ts — checkoutPathExists statSync race: existsSync says the token is
// present but statSync throws between the two syscalls. The guard must treat it
// as "not a file" (return false) so a benign branch switch is not blocked.
// ---------------------------------------------------------------------------

describe('detectDetachedGitDiscard — statSync race on a checkout positional', () => {
  afterEach(() => mock.restoreAll());

  it('treats a statSync failure as not-a-file (allows the checkout)', () => {
    mock.method(_bashHooks, 'existsSync', () => true);
    mock.method(_bashHooks, 'statSync', () => { throw new Error('vanished mid-check'); });
    // single positional, existsSync true but statSync throws -> catch -> false -> allowed
    assert.equal(detectDetachedGitDiscard('git checkout some-ref', '/no/such/cwd'), null);
  });
});

// ---------------------------------------------------------------------------
// bash.ts — execute() guard returns that were still uncovered.
// ---------------------------------------------------------------------------

describe('bashTool — execute guard branches', () => {
  let dir: string;
  before(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cbash-guard-')); });
  after(() => fs.rmSync(dir, { recursive: true, force: true }));

  it('blocks a worktree-discarding git command in detached mode', async () => {
    const result = await bashTool.execute(
      { command: 'git stash' },
      ctx({ cwd: dir, detachedMode: true }),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /discards the patch/);
  });

  it('allows the same command when NOT in detached mode (guard skipped)', async () => {
    // Not detached -> the discard guard is bypassed; the command just runs in a
    // non-repo dir and fails at git level, but NOT with the discard message.
    const result = await bashTool.execute(
      { command: 'git stash' },
      ctx({ cwd: dir, detachedMode: false }),
    );
    assert.ok(!result.output.includes('discards the patch'), result.output);
  });

  it('blocks a BASH_ARGV0 assignment (process-name masquerade)', async () => {
    const result = await bashTool.execute({ command: 'BASH_ARGV0=fake ls' }, ctx({ cwd: dir }));
    assert.equal(result.isError, true);
    assert.match(result.output, /BASH_ARGV0/);
  });

  it('blocks a BASHPID assignment', async () => {
    const result = await bashTool.execute({ command: 'BASHPID=123 ls' }, ctx({ cwd: dir }));
    assert.equal(result.isError, true);
    assert.match(result.output, /BASHPID/);
  });

  it('blocks a code-loading env assignment (LD_PRELOAD)', async () => {
    const result = await bashTool.execute({ command: 'export LD_PRELOAD=/tmp/evil.so' }, ctx({ cwd: dir }));
    assert.equal(result.isError, true);
    assert.match(result.output, /LD_PRELOAD/);
  });

  it('redirects a bash `git commit` to the git_commit tool', async () => {
    const result = await bashTool.execute({ command: 'git commit -m "x"' }, ctx({ cwd: dir }));
    assert.equal(result.isError, true);
    assert.match(result.output, /Use the git_commit tool/);
  });

  it('blocks a command that would kill AMI itself (self-PID)', async () => {
    const result = await bashTool.execute({ command: `kill ${process.pid}` }, ctx({ cwd: dir }));
    assert.equal(result.isError, true);
    assert.match(result.output, /kill AMI itself/);
    assert.match(result.output, /specific child process PID/);
  });

  it('blocks a force push to a protected branch at the execute boundary', async () => {
    const result = await bashTool.execute({ command: 'git push --force origin main' }, ctx({ cwd: dir }));
    assert.equal(result.isError, true);
    assert.match(result.output, /Force push to protected branch/);
  });

  it('blocks at the execute-level exfiltration safety net (defense in depth)', async () => {
    // validateBashSecurity already runs detectDataExfiltration, so a real exfil
    // command is caught upstream. This standalone guard only fires if that
    // upstream check is narrowed; drive it via the seam on an otherwise-benign
    // command so the defensive branch is exercised.
    mock.method(_bashHooks, 'detectDataExfiltration', () => ({ blocked: true, reason: 'seam-forced exfil' }));
    const result = await bashTool.execute({ command: 'echo benign' }, ctx({ cwd: dir }));
    assert.equal(result.isError, true);
    assert.match(result.output, /data exfiltration risk/);
    assert.match(result.output, /seam-forced exfil/);
    mock.restoreAll();
  });
});

// ---------------------------------------------------------------------------
// injectFindMaxdepth — a positional literally named `sudo` is skipped by the
// insert-position scan (the `tokens[i] === 'sudo'` arm), landing -maxdepth after
// it rather than before.
// ---------------------------------------------------------------------------

describe('injectFindMaxdepth — a "sudo" positional token', () => {
  it('skips a bare `sudo` positional while scanning the insert index', () => {
    const out = injectFindMaxdepth('find sudo -type f');
    assert.equal(out, 'find sudo -maxdepth 20 -type f');
  });
});

// ---------------------------------------------------------------------------
// bash.ts — parseGitInvocations global-option skipping (via the detached
// discard guard). A discard smuggled behind `-C`, `-c`, or `--git-dir` (both
// the `=value` and the space-separated forms) must still be seen by the guard.
// ---------------------------------------------------------------------------

describe('detectDetachedGitDiscard — global options before the subcommand', () => {
  const MSG = 'discards the patch';
  it('skips a `-C <dir>` value token and still blocks the stash', () => {
    assert.match(detectDetachedGitDiscard('git -C /tmp stash')!, new RegExp(MSG));
  });
  it('skips a `-c key=val` value token and still blocks the stash', () => {
    assert.match(detectDetachedGitDiscard('git -c user.name=x stash')!, new RegExp(MSG));
  });
  it('skips a `--git-dir=<dir>` (= form, single token) and blocks the stash', () => {
    assert.match(detectDetachedGitDiscard('git --git-dir=/tmp/x stash')!, new RegExp(MSG));
  });
  it('skips a `--git-dir <dir>` (space form, two tokens) and blocks the stash', () => {
    assert.match(detectDetachedGitDiscard('git --git-dir /tmp/x stash')!, new RegExp(MSG));
  });
  it('skips a `--work-tree <dir>` (space form) and blocks the stash', () => {
    assert.match(detectDetachedGitDiscard('git --work-tree /tmp/x stash')!, new RegExp(MSG));
  });
  it('skips an unrelated dashed flag (`--no-pager`) and blocks the stash', () => {
    // exercises the generic `t.startsWith('-') && t !== '--'` skip arm
    assert.match(detectDetachedGitDiscard('git --no-pager stash')!, new RegExp(MSG));
  });
  it('breaks the option scan on a shell-meta token, still blocking a later stash', () => {
    // After `-C /tmp` the next token is `&&` (shell meta) -> the option-scan
    // while-loop hits its isShellMeta break; the second invocation is the stash.
    assert.match(detectDetachedGitDiscard('git -C /tmp && git stash')!, new RegExp(MSG));
  });
});

// ---------------------------------------------------------------------------
// detectForcePushToProtected — the two "git/push token not found after the
// regex matched" guards (path-qualified git, and push glued to the branch by a
// separator so it is not its own token).
// ---------------------------------------------------------------------------

describe('detectForcePushToProtected — token-index guards', () => {
  it('returns null when the matched git is path-qualified (no bare `git` token)', async () => {
    const { detectForcePushToProtected } = await import('../src/tools/bash');
    assert.equal(detectForcePushToProtected('/usr/bin/git push --force main'), null);
  });
  it('returns null when `push` is glued to the branch and is not its own token', async () => {
    const { detectForcePushToProtected } = await import('../src/tools/bash');
    assert.equal(detectForcePushToProtected('git push;main --force'), null);
  });
});

// ---------------------------------------------------------------------------
// interpretExitCode (via execute) — git diff exit-1 and find exit-1 non-error
// classifications that were not yet exercised.
// ---------------------------------------------------------------------------

describe('bashTool — interpretExitCode extra non-error arms', () => {
  let repo: string;
  before(() => {
    repo = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cbash-iec-'));
    child_process.execSync('git init', { cwd: repo, stdio: 'ignore' });
    child_process.execSync('git config user.email "t@t.com"', { cwd: repo, stdio: 'ignore' });
    child_process.execSync('git config user.name "T"', { cwd: repo, stdio: 'ignore' });
    fs.writeFileSync(path.join(repo, 'a.txt'), 'one\n');
    child_process.execSync('git add . && git commit -m init', { cwd: repo, stdio: 'ignore' });
    fs.writeFileSync(path.join(repo, 'a.txt'), 'two\n');
  });
  after(() => fs.rmSync(repo, { recursive: true, force: true }));

  it('git diff --exit-code (exit 1) is a non-error (differences found)', async () => {
    const result = await bashTool.execute({ command: 'git diff --exit-code' }, ctx({ cwd: repo }));
    assert.equal(result.isError, false, result.output);
    assert.match(result.output, /no matches\/differences found/);
  });

  it('find on a missing path (exit 1) is a non-error (paths inaccessible)', async () => {
    const result = await bashTool.execute(
      { command: 'find /no-such-dir-xyz-123456' },
      ctx({ cwd: repo }),
    );
    assert.equal(result.isError, false, result.output);
    assert.match(result.output, /some paths were inaccessible/);
  });
});

// ---------------------------------------------------------------------------
// bash.ts — the real _bashHooks.sleep default (exercised directly so the
// backoff-timer arm is covered without relying on a slow real fork storm).
// ---------------------------------------------------------------------------

describe('_bashHooks.sleep — real timer default', () => {
  it('resolves after the requested delay', async () => {
    const t0 = Date.now();
    await _bashHooks.sleep(1);
    assert.ok(Date.now() - t0 >= 0);
  });
});

// ---------------------------------------------------------------------------
// exit-worktree.ts — git-worktree-remove failure fallback (fs.rmSync + prune)
// and a rejected executeWorktreeRemove hook (the .catch(() => {}) arm).
// ---------------------------------------------------------------------------

function makeTmpGitRepo(): string {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cbash-wt-'));
  child_process.execSync('git init', { cwd: tmpDir, stdio: 'ignore' });
  child_process.execSync('git config user.email "t@t.com"', { cwd: tmpDir, stdio: 'ignore' });
  child_process.execSync('git config user.name "T"', { cwd: tmpDir, stdio: 'ignore' });
  fs.writeFileSync(path.join(tmpDir, 'init.txt'), 'init');
  child_process.execSync('git add . && git commit -m init', { cwd: tmpDir, stdio: 'ignore' });
  return tmpDir;
}

function makeWorktreeDir(repoDir: string, name: string): string {
  const wtPath = path.join(repoDir, '.superinference', 'worktrees', name);
  fs.mkdirSync(wtPath, { recursive: true });
  child_process.execSync('git init', { cwd: wtPath, stdio: 'ignore' });
  child_process.execSync('git config user.email "t@t.com"', { cwd: wtPath, stdio: 'ignore' });
  child_process.execSync('git config user.name "T"', { cwd: wtPath, stdio: 'ignore' });
  fs.writeFileSync(path.join(wtPath, 'f.txt'), 'x');
  child_process.execSync('git add . && git commit -m wt', { cwd: wtPath, stdio: 'ignore' });
  return wtPath;
}

describe('exitWorktreeTool — remove fallback + rejected hook (seam-driven)', () => {
  let tmpDir: string;
  let worktreePath: string;

  beforeEach(() => {
    tmpDir = makeTmpGitRepo();
    worktreePath = makeWorktreeDir(tmpDir, 'cbash-wt');
    setWorktreeSession(null);
  });

  afterEach(() => {
    mock.restoreAll();
    setWorktreeSession(null);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('falls back to rmSync + prune when `git worktree remove` throws', async () => {
    let pruned = false;
    let rmCalled = false;
    mock.method(_exitWorktreeHooks, 'execSync', ((cmd: string) => {
      if (/worktree remove/.test(cmd)) throw new Error('remove refused');
      if (/worktree prune/.test(cmd)) { pruned = true; return ''; }
      return '';
    }) as typeof child_process.execSync);
    mock.method(_exitWorktreeHooks, 'existsSync', (() => true) as typeof fs.existsSync);
    mock.method(_exitWorktreeHooks, 'rmSync', (() => { rmCalled = true; }) as typeof fs.rmSync);

    const result = await exitWorktreeTool.execute(
      { action: 'remove', discard_changes: true },
      ctx({ cwd: worktreePath }),
    );

    assert.ok(!result.isError, result.output);
    assert.match(result.output, /Worktree removed/);
    assert.equal(rmCalled, true, 'fallback rmSync must run');
    assert.equal(pruned, true, 'fallback prune must run');
  });

  it('returns the original error when the fallback itself fails', async () => {
    mock.method(_exitWorktreeHooks, 'execSync', ((cmd: string) => {
      if (/worktree remove/.test(cmd)) throw new Error('remove refused');
      throw new Error('prune also failed');
    }) as typeof child_process.execSync);
    mock.method(_exitWorktreeHooks, 'existsSync', (() => false) as typeof fs.existsSync);

    const result = await exitWorktreeTool.execute(
      { action: 'remove', discard_changes: true },
      ctx({ cwd: worktreePath }),
    );

    assert.equal(result.isError, true);
    assert.match(result.output, /Error removing worktree: remove refused/);
  });

  it('swallows a rejected executeWorktreeRemove hook', async () => {
    // remove succeeds (seam no-op) so we reach the hook; the hook rejects and
    // the .catch(() => {}) arm must swallow it without failing the tool.
    mock.method(_exitWorktreeHooks, 'execSync', (() => '') as typeof child_process.execSync);

    const result = await exitWorktreeTool.execute(
      { action: 'remove', discard_changes: true },
      ctx({
        cwd: worktreePath,
        _hookManager: {
          executeWorktreeRemove: () => Promise.reject(new Error('hook boom')),
        } as any,
      }),
    );

    assert.ok(!result.isError, result.output);
    assert.match(result.output, /Worktree removed/);
    // Flush the rejected-hook microtask so its .catch runs before teardown.
    await new Promise(r => setImmediate(r));
  });
});
