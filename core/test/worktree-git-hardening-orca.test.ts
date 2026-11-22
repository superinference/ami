import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';

import {
  classifyGitCommand,
  gitTimeoutForClass,
  findGitSubcommandIndex,
  GIT_READ_CMD_TIMEOUT_MS,
  isGitLockContentionFailure,
  GIT_LOCK_CONTENTION_RETRY_DELAYS_MS,
  resolveFetchHeadLockKey,
  resolveCommonGitDir,
  withFetchHeadLock,
  retryOnGitLockContention,
  gitExec,
} from '../src/worktree-manager';

// ---------------------------------------------------------------------------
// Command classification (read / network / other)
// ---------------------------------------------------------------------------

describe('classifyGitCommand', () => {
  it('classifies remote-transfer subcommands as network', () => {
    for (const sub of ['fetch', 'pull', 'push', 'clone', 'ls-remote']) {
      assert.equal(classifyGitCommand([sub]), 'network', sub);
    }
    assert.equal(classifyGitCommand(['submodule', 'update', '--init']), 'network');
    assert.equal(classifyGitCommand(['remote', 'update']), 'network');
  });

  it('classifies read subcommands as read, honoring leading global options', () => {
    assert.equal(classifyGitCommand(['status', '--porcelain']), 'read');
    assert.equal(classifyGitCommand(['rev-list', '--count', 'HEAD']), 'read');
    assert.equal(classifyGitCommand(['-C', '/tmp/repo', 'rev-parse', 'HEAD']), 'read');
    assert.equal(classifyGitCommand(['-c', 'k=v', 'log']), 'read');
    // branch/config/worktree/remote only read in their listing forms.
    assert.equal(classifyGitCommand(['branch', '--list']), 'read');
    assert.equal(classifyGitCommand(['worktree', 'list', '--porcelain']), 'read');
    assert.equal(classifyGitCommand(['remote', 'get-url', 'origin']), 'read');
    assert.equal(classifyGitCommand(['symbolic-ref', '-q', 'HEAD']), 'read');
  });

  it('classifies mutations / unknowns as other', () => {
    assert.equal(classifyGitCommand(['worktree', 'add', '-B', 'x', '/p', 'HEAD']), 'other');
    assert.equal(classifyGitCommand(['worktree', 'remove', '--force', '/p']), 'other');
    assert.equal(classifyGitCommand(['branch', '-D', 'feature']), 'other');
    assert.equal(classifyGitCommand(['symbolic-ref', 'HEAD', 'refs/heads/x']), 'other');
    assert.equal(classifyGitCommand(['commit', '-m', 'x']), 'other');
    assert.equal(classifyGitCommand([]), 'other');
    assert.equal(classifyGitCommand(['-c', 'k=v']), 'other'); // only global opts, no subcommand
  });

  it('findGitSubcommandIndex skips global option values', () => {
    assert.equal(findGitSubcommandIndex(['-C', '/r', 'status']), 2);
    assert.equal(findGitSubcommandIndex(['--git-dir', '/g', 'fetch']), 2);
    assert.equal(findGitSubcommandIndex(['-p', 'status']), 1); // bare flag, no value
    assert.equal(findGitSubcommandIndex(['-c', 'k=v']), -1);
  });
});

describe('gitTimeoutForClass', () => {
  it('leaves network UNCAPPED (0), caps read at 120s, keeps other tight', () => {
    assert.equal(gitTimeoutForClass('network'), 0);
    assert.equal(gitTimeoutForClass('read'), GIT_READ_CMD_TIMEOUT_MS);
    assert.equal(GIT_READ_CMD_TIMEOUT_MS, 120_000);
    assert.equal(gitTimeoutForClass('other'), 15_000);
    // The whole point: a network op is NOT killed by the flat 15s read/other cap.
    assert.notEqual(gitTimeoutForClass('network'), 15_000);
  });
});

// ---------------------------------------------------------------------------
// Lock-contention classification
// ---------------------------------------------------------------------------

describe('isGitLockContentionFailure', () => {
  it('matches only transient *.lock File-exists failures', () => {
    assert.equal(
      isGitLockContentionFailure({ stderr: "fatal: Unable to create '/r/.git/index.lock': File exists" }),
      true,
    );
    assert.equal(
      isGitLockContentionFailure(new Error("error: could not lock config file /r/.git/config: File exists")),
      true,
    );
    assert.equal(
      isGitLockContentionFailure({ stderr: "cannot lock ref 'refs/heads/x': Unable to create '/r/.git/refs/heads/x.lock': File exists" }),
      true,
    );
  });

  it('does NOT match a CAS ref mismatch or an unrelated failure (never masks a real error)', () => {
    assert.equal(
      isGitLockContentionFailure({ stderr: "cannot lock ref 'refs/heads/x': is at abc but expected def" }),
      false,
    );
    assert.equal(isGitLockContentionFailure({ stderr: 'fatal: not a git repository' }), false);
    assert.equal(isGitLockContentionFailure(undefined), false);
    assert.equal(isGitLockContentionFailure('plain string'), false);
  });

  it('exposes a bounded backoff schedule', () => {
    assert.deepEqual([...GIT_LOCK_CONTENTION_RETRY_DELAYS_MS], [50, 150, 400]);
  });
});

describe('retryOnGitLockContention', () => {
  const lockErr = () => new Error("fatal: Unable to create '/r/.git/index.lock': File exists");

  it('retries past transient lock failures then returns the success value', () => {
    let calls = 0;
    const out = retryOnGitLockContention(() => {
      calls += 1;
      if (calls < 3) throw lockErr();
      return 'ok';
    }, [1, 1, 1]); // tiny delays for a fast test
    assert.equal(out, 'ok');
    assert.equal(calls, 3);
  });

  it('throws a non-transient failure WITHOUT retrying', () => {
    let calls = 0;
    assert.throws(() => retryOnGitLockContention(() => {
      calls += 1;
      throw new Error('fatal: bad object');
    }, [1, 1, 1]));
    assert.equal(calls, 1, 'no retry on a non-transient error');
  });

  it('gives up after exhausting the delays, rethrowing the last lock failure', () => {
    let calls = 0;
    assert.throws(() => retryOnGitLockContention(() => {
      calls += 1;
      throw lockErr();
    }, [1, 1]), /File exists/);
    assert.equal(calls, 3, 'initial try + 2 retries');
  });

  it('bails out of the backoff when shouldAbort reports cancellation', () => {
    let calls = 0;
    assert.throws(() => retryOnGitLockContention(() => {
      calls += 1;
      throw lockErr();
    }, [1, 1, 1], () => true), /aborted/);
    assert.equal(calls, 1);
  });
});

// ---------------------------------------------------------------------------
// FETCH_HEAD lane resolution (per common-dir)
// ---------------------------------------------------------------------------

describe('resolveFetchHeadLockKey + resolveCommonGitDir', () => {
  let main: string;
  let wt: string;

  beforeEach(() => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-fhlock-'));
    main = path.join(base, 'main');
    fs.mkdirSync(main);
    const run = (args: string[], cwd: string) =>
      execFileSync('git', args, { cwd, stdio: 'ignore', env: { ...process.env, GIT_TERMINAL_PROMPT: '0' } });
    run(['init', '-q'], main);
    run(['config', 'user.email', 't@t'], main);
    run(['config', 'user.name', 't'], main);
    fs.writeFileSync(path.join(main, 'f'), 'x');
    run(['add', '-A'], main);
    run(['commit', '-qm', 'init'], main);
    wt = path.join(base, 'wt');
    run(['worktree', 'add', '-q', wt, 'HEAD'], main);
  });

  afterEach(() => {
    try { fs.rmSync(path.dirname(main), { recursive: true, force: true }); } catch { /* ignore */ }
  });

  it('a linked worktree and the main checkout resolve to the SAME common-dir FETCH_HEAD key', () => {
    const fromMain = resolveFetchHeadLockKey(['fetch', 'origin'], main);
    const fromWt = resolveFetchHeadLockKey(['fetch', 'origin'], wt);
    assert.ok(fromMain, 'main resolves a key');
    assert.ok(fromWt, 'worktree resolves a key');
    // Both must serialize on ONE lane so sibling fetches cannot race. The FETCH_HEAD
    // file need not exist yet (nothing has fetched), so compare the keys directly.
    assert.equal(fromMain, fromWt);
    assert.ok(fromMain!.endsWith('FETCH_HEAD'));
    // The common dir is the main repo's .git, shared by both.
    assert.equal(fs.realpathSync(resolveCommonGitDir(wt)!), fs.realpathSync(resolveCommonGitDir(main)!));
  });

  it('returns null for commands that do not write a shared FETCH_HEAD', () => {
    assert.equal(resolveFetchHeadLockKey(['status'], main), null);
    assert.equal(resolveFetchHeadLockKey(['push', 'origin', 'main'], main), null);
    assert.equal(resolveFetchHeadLockKey(['fetch', '--no-write-fetch-head', 'origin'], main), null);
    // ...but an explicit tracking-ref update still needs the lane.
    assert.ok(resolveFetchHeadLockKey(['fetch', '--no-write-fetch-head', 'origin', '+refs/heads/*:refs/remotes/origin/*'], main));
    assert.equal(resolveFetchHeadLockKey(['fetch'], path.join(os.tmpdir(), 'not-a-repo-xyz')), null);
  });

  it('honors -C / --git-dir relocation before the subcommand', () => {
    const expected = resolveFetchHeadLockKey(['fetch', 'origin'], main);
    assert.ok(expected);
    // Spaced -C <dir>
    assert.equal(resolveFetchHeadLockKey(['-C', main, 'fetch', 'origin'], path.dirname(main)), expected);
    // Glued -Cdir
    assert.equal(resolveFetchHeadLockKey([`-C${main}`, 'fetch', 'origin'], path.dirname(main)), expected);
    // --git-dir <path> and --git-dir=path (explicit common dir)
    const gitDir = fs.realpathSync(path.join(main, '.git'));
    assert.equal(
      resolveFetchHeadLockKey(['--git-dir', gitDir, 'fetch', 'origin'], path.dirname(main)),
      path.join(gitDir, 'FETCH_HEAD'),
    );
    assert.equal(
      resolveFetchHeadLockKey([`--git-dir=${gitDir}`, 'fetch', 'origin'], path.dirname(main)),
      path.join(gitDir, 'FETCH_HEAD'),
    );
    // A valued global option before the subcommand is skipped (consumes next argv).
    assert.equal(
      resolveFetchHeadLockKey(['-c', 'advice.detachedHead=false', 'fetch', 'origin'], main),
      expected,
    );
  });
});

// ---------------------------------------------------------------------------
// withFetchHeadLock (cross-process advisory lane)
// ---------------------------------------------------------------------------

describe('withFetchHeadLock', () => {
  let dir: string;
  let key: string;
  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-wfhl-'));
    key = path.join(dir, 'FETCH_HEAD');
  });
  afterEach(() => { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ } });

  it('creates the lockfile while running and removes it afterward', () => {
    const lockPath = `${key}.ami-lock`;
    let sawLock = false;
    const result = withFetchHeadLock(key, () => {
      sawLock = fs.existsSync(lockPath);
      return 42;
    });
    assert.equal(result, 42);
    assert.equal(sawLock, true, 'lock held during run');
    assert.equal(fs.existsSync(lockPath), false, 'lock released after run');
  });

  it('removes the lockfile even when the body throws', () => {
    const lockPath = `${key}.ami-lock`;
    assert.throws(() => withFetchHeadLock(key, () => { throw new Error('boom'); }), /boom/);
    assert.equal(fs.existsSync(lockPath), false);
  });

  it('steals an ABANDONED (stale) lockfile instead of waiting forever', () => {
    const lockPath = `${key}.ami-lock`;
    fs.writeFileSync(lockPath, '');
    // Age the lock past the stale window.
    const old = Date.now() - 10 * 60_000;
    fs.utimesSync(lockPath, new Date(old), new Date(old));
    let ran = false;
    withFetchHeadLock(key, () => { ran = true; });
    assert.equal(ran, true, 'stale lock was stolen and body ran');
  });

  it('runs unlocked (never throws) when the lock dir does not exist', () => {
    const missing = path.join(dir, 'nope', 'FETCH_HEAD');
    let ran = false;
    withFetchHeadLock(missing, () => { ran = true; });
    assert.equal(ran, true);
  });
});

// ---------------------------------------------------------------------------
// gitExec integration: lock-contention retry succeeds; non-transient throws
// ---------------------------------------------------------------------------

describe('gitExec command-class timeout + retry (integration)', () => {
  let repo: string;
  beforeEach(() => {
    repo = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-gitexec-'));
    const run = (args: string[]) => execFileSync('git', args, { cwd: repo, stdio: 'ignore', env: { ...process.env, GIT_TERMINAL_PROMPT: '0' } });
    run(['init', '-q']);
    run(['config', 'user.email', 't@t']);
    run(['config', 'user.name', 't']);
    fs.writeFileSync(path.join(repo, 'f'), 'x');
    run(['add', '-A']);
    run(['commit', '-qm', 'init']);
  });
  afterEach(() => { try { fs.rmSync(repo, { recursive: true, force: true }); } catch { /* ignore */ } });

  it('a read command still succeeds under the read ceiling', () => {
    const out = gitExec(['rev-parse', '--abbrev-ref', 'HEAD'], { cwd: repo });
    assert.match(out, /\S/);
  });

  it('does NOT retry a non-transient failure (throws immediately)', () => {
    const start = Date.now();
    assert.throws(() => gitExec(['rev-parse', 'does-not-exist-ref'], { cwd: repo }));
    // No backoff sleeps were spent (would be ≥600ms if it wrongly retried all delays).
    assert.ok(Date.now() - start < 500, 'failed fast without spurious retry backoff');
  });
});
