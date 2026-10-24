import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as childProcess from 'child_process';
import {
  bashTool,
  injectFindMaxdepth,
  detectDetachedGitDiscard,
  detectForcePushToProtected,
} from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// injectFindMaxdepth — sudo + paren + tab branches
// Each asserts the -maxdepth guard is injected at the correct position so a
// `find` can never recurse unbounded, even behind sudo or a grouped expression.
// ---------------------------------------------------------------------------

describe('injectFindMaxdepth — sudo / paren / tab / exact', () => {
  it('injects after a `sudo find <path>` prefix (skips sudo and the path)', () => {
    const out = injectFindMaxdepth('sudo find /tmp -type f');
    // maxdepth must land before the first flag, after sudo + find + path.
    assert.match(out, /^sudo find \/tmp -maxdepth 20 -type f$/);
  });

  it('injects for a tab-separated `sudo find\\t` invocation', () => {
    const out = injectFindMaxdepth('sudo find\t/var -type d');
    assert.match(out, /-maxdepth 20/);
    // still a sudo find, not mangled into something else
    assert.ok(out.startsWith('sudo find'));
  });

  it('injects for a bare tab-separated `find\\t` invocation', () => {
    const out = injectFindMaxdepth('find\t. -name x');
    assert.match(out, /-maxdepth 20/);
  });

  it('injects for the exact one-token `find` command', () => {
    const out = injectFindMaxdepth('find');
    assert.equal(out, 'find -maxdepth 20');
  });

  it('inserts -maxdepth before a `(` grouped expression (not inside it)', () => {
    const out = injectFindMaxdepth('find . ( -name a -o -name b )');
    // the paren token must STOP the positional scan: maxdepth lands before `(`
    assert.match(out, /^find \. -maxdepth 20 \( -name a -o -name b \)$/);
  });

  it('falls back to the default depth when AMI_FIND_MAX_DEPTH is non-numeric', () => {
    const orig = process.env.AMI_FIND_MAX_DEPTH;
    process.env.AMI_FIND_MAX_DEPTH = 'not-a-number';
    try {
      assert.match(injectFindMaxdepth('find . -type f'), /-maxdepth 20/);
    } finally {
      if (orig !== undefined) process.env.AMI_FIND_MAX_DEPTH = orig;
      else delete process.env.AMI_FIND_MAX_DEPTH;
    }
  });

  it('skips a non-find segment in a chain (returns it untouched)', () => {
    // `echo` segment must pass through isFindCmd=false; find segment gets depth.
    const out = injectFindMaxdepth('echo hi ; find . -type f');
    assert.match(out, /echo hi/);
    assert.match(out, /-maxdepth 20/);
  });
});

// ---------------------------------------------------------------------------
// detectDetachedGitDiscard — checkout of an existing DIRECTORY positional
// A directory token is not a worktree file, so a single-positional checkout of
// it is a branch/ref switch and must be allowed; only a real tracked FILE is a
// discard. This pins the fs.statSync(...).isFile() === false branch.
// ---------------------------------------------------------------------------

describe('detectDetachedGitDiscard — directory vs file positional', () => {
  let dir: string;
  before(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wave1-co-')); });
  after(() => fs.rmSync(dir, { recursive: true, force: true }));

  it('allows `git checkout <dir>` when the single positional is a directory', () => {
    const sub = path.join(dir, 'subdir');
    fs.mkdirSync(sub);
    // positional resolves to an existing path that is NOT a file → allowed
    assert.equal(detectDetachedGitDiscard('git checkout subdir', dir), null);
  });

  it('still blocks `git checkout <file>` when the positional is a real file', () => {
    fs.writeFileSync(path.join(dir, 'real.ts'), 'export const x = 1;\n');
    assert.ok(detectDetachedGitDiscard('git checkout real.ts', dir)?.includes('not allowed'));
  });

  it('allows `git checkout <missing>` (existsSync false → branch switch)', () => {
    assert.equal(detectDetachedGitDiscard('git checkout totally-missing-ref', dir), null);
  });

  it('returns null when checkoutPathExists hits a statSync error path (cwd only)', () => {
    // No cwd → the fs.statSync branch is never reached; single positional treated as branch.
    assert.equal(detectDetachedGitDiscard('git checkout some-branch'), null);
  });
});

// ---------------------------------------------------------------------------
// detectForcePushToProtected — short-circuit guards
// ---------------------------------------------------------------------------

describe('detectForcePushToProtected — guard short-circuits', () => {
  it('returns null when there is no `git push` at all', () => {
    assert.equal(detectForcePushToProtected('rm -rf build --force'), null);
  });

  it('returns null for a force push to a NON-protected branch', () => {
    assert.equal(detectForcePushToProtected('git push --force origin my-feature'), null);
  });

  it('blocks a force push to a protected branch (develop)', () => {
    const r = detectForcePushToProtected('git push --force origin develop');
    assert.ok(r && r.includes('develop'));
  });

  it('does not treat an `echo` of a force-push string as a real push', () => {
    assert.equal(detectForcePushToProtected('echo git push --force main'), null);
  });

  it('returns null for a plain push without a force flag', () => {
    assert.equal(detectForcePushToProtected('git push origin main'), null);
  });
});

// ---------------------------------------------------------------------------
// bashTool.execute — read-only result cache (set then hit)
// First run populates the cache (isCacheableReadOnly + success); the second
// returns the cached output with metadata.cached === true without re-executing.
// ---------------------------------------------------------------------------

describe('bashTool – read-only result cache', () => {
  it('caches a read-only command and serves the second call from cache', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wave1-cache-'));
    try {
      const cmd = 'echo si-wave1-cache-token-UNIQUE';
      const first = await bashTool.execute({ command: cmd }, ctx({ cwd: dir }));
      assert.ok(!first.isError);
      assert.ok(first.output.includes('si-wave1-cache-token-UNIQUE'));
      // first call is not served from cache
      assert.notEqual((first.metadata as any)?.cached, true);

      const second = await bashTool.execute({ command: cmd }, ctx({ cwd: dir }));
      assert.equal((second.metadata as any)?.cached, true, 'second call must hit cache');
      assert.equal((second.metadata as any)?.readOnly, true);
      assert.equal(second.output, first.output);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('invalidates the cwd cache after a non-read-only write command', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wave1-inval-'));
    try {
      const readCmd = 'echo si-wave1-inval-token';
      // seed the cache
      await bashTool.execute({ command: readCmd }, ctx({ cwd: dir }));
      const cachedHit = await bashTool.execute({ command: readCmd }, ctx({ cwd: dir }));
      assert.equal((cachedHit.metadata as any)?.cached, true);

      // a write command (touch) is not read-only → invalidates the cwd cache
      const write = await bashTool.execute(
        { command: `touch ${path.join(dir, 'made.txt')}` },
        ctx({ cwd: dir }),
      );
      assert.ok(!write.isError);
      // "no output expected" sentinel confirms the noOutputExpected branch ran
      assert.ok(write.output.includes('no output expected'), write.output);
      assert.equal(fs.existsSync(path.join(dir, 'made.txt')), true);

      // next read must re-execute (cache was invalidated), so not cached
      const afterInval = await bashTool.execute({ command: readCmd }, ctx({ cwd: dir }));
      assert.notEqual((afterInval.metadata as any)?.cached, true, 'cache must be invalidated after write');
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// bashTool.execute — timeout disposition branches
// ---------------------------------------------------------------------------

describe('bashTool – timeout disposition', () => {
  it('returns a plain timeout error when no processManager is present', async () => {
    const result = await bashTool.execute(
      { command: 'sleep 30', timeout: 400 },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /timed out after 400ms/);
    assert.ok(!result.output.includes('Auto-moved'));
  });

  it('auto-moves a timed-out command to the background when a processManager exists', async () => {
    let spawned = '';
    const pm = {
      spawn: (_cmd: string, opts: any) => {
        spawned = opts.description;
        return 'auto-bg-task-1';
      },
      get: () => ({ pid: 999 }),
    };
    const result = await bashTool.execute(
      { command: 'sleep 30', timeout: 400 },
      ctx({ processManager: pm } as any),
    );
    assert.ok(!result.isError, `auto-bg return is not an error: ${result.output}`);
    assert.match(result.output, /Auto-moved to background as task auto-bg-task-1/);
    assert.ok(spawned.startsWith('[auto-bg]'), `expected auto-bg description, got: ${spawned}`);
  });

  it('detects an interactive-prompt hang and suggests a non-interactive rerun', async () => {
    // Emit a y/n prompt then block: the command never exits, so it times out
    // with the prompt as the last line → hung-prompt branch.
    const result = await bashTool.execute(
      { command: `printf 'Overwrite? (y/n) '; sleep 30`, timeout: 600 },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /waiting for interactive input/);
    assert.match(result.output, /echo y \||yes \||run_in_background/);
  });
});

// ---------------------------------------------------------------------------
// WAVE 2 — remaining reachable branches
// ---------------------------------------------------------------------------

// tokenizeShell (via parseGitInvocations/detectDetachedGitDiscard): the guard
// must survive every shell separator a bash user can type, so an attacker
// cannot smuggle a discard past the tokenizer with an unusual operator.
describe('detectDetachedGitDiscard — tokenizer separator coverage', () => {
  const MSG = 'not allowed in non-interactive eval';
  it('sees a tab-separated `git\\tstash`', () => {
    assert.ok(detectDetachedGitDiscard('git\tstash')?.includes(MSG));
  });
  it('treats `|&` as a boundary and still blocks the leading stash', () => {
    assert.ok(detectDetachedGitDiscard('git stash |& cat')?.includes(MSG));
  });
  it('treats a lone `&` (background) as a boundary and blocks the stash', () => {
    assert.ok(detectDetachedGitDiscard('git stash & echo bg')?.includes(MSG));
  });
  it('handles parenthesised grouping `( git stash )`', () => {
    assert.ok(detectDetachedGitDiscard('( git stash )')?.includes(MSG));
  });
  it('treats a carriage return as a boundary (benign checkout not blocked)', () => {
    assert.equal(detectDetachedGitDiscard('git checkout main\r\necho done'), null);
  });
  it('allows a git invocation with options but no subcommand', () => {
    // `git -C /tmp` has no subcommand → sub === '' → nothing to block
    assert.equal(detectDetachedGitDiscard('git -C /tmp'), null);
  });
});

// extractBashReadPaths: a grep with MULTIPLE file operands must track every
// file after the pattern (patternSkipped already true on the 2nd+ operand).
describe('extractBashReadPaths — multi-file grep operand', () => {
  it('tracks every file after the grep pattern', async () => {
    const { extractBashReadPaths } = await import('../src/tools/bash');
    const paths = extractBashReadPaths('grep -n TODO /a.ts /b.ts /c.ts', '/cwd');
    assert.deepEqual(paths, ['/a.ts', '/b.ts', '/c.ts']);
  });
});

// detectGitCommit: a git invocation ending in a global option with NO
// subcommand must not be mis-read as a commit (out-of-bounds subcommand index).
describe('detectGitCommit — no-subcommand invocation', () => {
  it('does not flag `git -C /tmp` (no subcommand token)', async () => {
    const { detectGitCommit } = await import('../src/tools/bash');
    assert.equal(detectGitCommit('git -C /tmp'), false);
  });
});

// bashTool.execute — sandbox opt-out + non-error exit-code interpretation.
describe('bashTool – sandbox opt-out and non-error exit codes', () => {
  it('runs without the sandbox when dangerouslyDisableSandbox is true', async () => {
    const result = await bashTool.execute(
      { command: 'echo sandbox-off', dangerouslyDisableSandbox: true },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('sandbox-off'));
  });

  it('treats a no-match grep (exit 1) as a non-error with an explanatory note', async () => {
    const result = await bashTool.execute(
      { command: 'grep zzz-no-such-token /dev/null' },
      ctx(),
    );
    // grep exit 1 = "no matches", which interpretExitCode marks non-error
    assert.equal(result.isError, false, `grep no-match must not be a hard error: ${result.output}`);
    assert.match(result.output, /no matches found/);
  });

  it('treats a false `test` condition (exit 1) as a non-error', async () => {
    const result = await bashTool.execute(
      { command: 'test 1 -eq 2' },
      ctx(),
    );
    assert.equal(result.isError, false);
    assert.match(result.output, /condition is false/);
  });
});

// ---------------------------------------------------------------------------
// WAVE 3 — interpretExitCode battery (every non-error / diagnostic exit code)
// These pin the exit-code interpreter: a tool caller must be able to tell a
// benign non-zero (grep no-match, false condition, files differ) from a real
// failure, and get the right diagnostic for 126/127/signal deaths.
// ---------------------------------------------------------------------------

describe('bashTool – interpretExitCode battery', () => {
  it('diff exit 1 = "files differ" (non-error)', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wave3-diff-'));
    try {
      fs.writeFileSync(path.join(dir, 'a'), 'a\n');
      fs.writeFileSync(path.join(dir, 'b'), 'b\n');
      const result = await bashTool.execute(
        { command: 'diff a b' },
        ctx({ cwd: dir }),
      );
      assert.equal(result.isError, false, result.output);
      assert.match(result.output, /files differ/);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('grep exit 2 (bad pattern) = diagnostic, treated as error', async () => {
    const result = await bashTool.execute(
      { command: `grep '[' /dev/null` },
      ctx(),
    );
    assert.match(result.output, /error in grep pattern/);
  });

  it('exit 126 = permission denied / not executable', async () => {
    const result = await bashTool.execute({ command: 'sh -c "exit 126"' }, ctx());
    assert.equal(result.isError, true);
    assert.match(result.output, /permission denied — command not executable/);
  });

  it('exit 127 = command not found', async () => {
    const result = await bashTool.execute({ command: 'this-command-does-not-exist-xyz' }, ctx());
    assert.equal(result.isError, true);
    assert.match(result.output, /command not found/);
  });

  it('exit 137 (128+9) = killed by SIGKILL', async () => {
    const result = await bashTool.execute({ command: 'sh -c "exit 137"' }, ctx());
    assert.match(result.output, /killed by SIGKILL/);
  });

  it('exit 143 (128+15) = killed by SIGTERM', async () => {
    const result = await bashTool.execute({ command: 'sh -c "exit 143"' }, ctx());
    assert.match(result.output, /killed by SIGTERM/);
  });
});

describe('bashTool – git grep/diff exit 1 is a non-error', () => {
  let repo: string;
  before(() => {
    repo = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wave3-git-'));
    childProcess.execSync('git init', { cwd: repo, stdio: 'pipe' });
    childProcess.execSync('git config user.name T', { cwd: repo, stdio: 'pipe' });
    childProcess.execSync('git config user.email t@t.com', { cwd: repo, stdio: 'pipe' });
    fs.writeFileSync(path.join(repo, 'a.txt'), 'hello\n');
    childProcess.execSync('git add a.txt && git -c user.name=T -c user.email=t@t.com commit -m init', { cwd: repo, stdio: 'pipe' });
  });
  after(() => fs.rmSync(repo, { recursive: true, force: true }));

  it('git grep with no match (exit 1) is not a hard error', async () => {
    const result = await bashTool.execute(
      { command: 'git grep zzz-nomatch-token' },
      ctx({ cwd: repo }),
    );
    assert.equal(result.isError, false, result.output);
    assert.match(result.output, /no matches\/differences found/);
  });
});

// ---------------------------------------------------------------------------
// WAVE 3 — fork-exhaustion retry (execCommandWithForkRetry)
// A container that hits the per-process fork limit emits EAGAIN; the tool must
// retry and, if it never recovers, surface an actionable exhaustion note rather
// than a bare failure. Driven through execute() with a command whose stderr
// mimics the kernel's fork-exhaustion message.
// ---------------------------------------------------------------------------

describe('bashTool – fork-exhaustion retry note', () => {
  it('appends the exhaustion note after retries fail', async () => {
    const result = await bashTool.execute(
      { command: `printf 'bash: fork: Resource temporarily unavailable\\n' >&2; exit 1` },
      ctx(),
    );
    assert.match(result.output, /fork exhaustion: all retries failed/);
  });
});

// ---------------------------------------------------------------------------
// WAVE 4 — parseEnvMs non-positive env values + tokenizer edges
// ---------------------------------------------------------------------------

describe('resolveBashTimeout — non-positive env values fall back to defaults', () => {
  const save = { d: process.env.BASH_DEFAULT_TIMEOUT_MS, m: process.env.BASH_MAX_TIMEOUT_MS };
  const restore = () => {
    if (save.d === undefined) delete process.env.BASH_DEFAULT_TIMEOUT_MS; else process.env.BASH_DEFAULT_TIMEOUT_MS = save.d;
    if (save.m === undefined) delete process.env.BASH_MAX_TIMEOUT_MS; else process.env.BASH_MAX_TIMEOUT_MS = save.m;
  };

  it('ignores a zero BASH_DEFAULT_TIMEOUT_MS (n > 0 is false → default 120000)', async () => {
    const { resolveBashTimeout } = await import('../src/tools/bash');
    delete process.env.BASH_MAX_TIMEOUT_MS;
    process.env.BASH_DEFAULT_TIMEOUT_MS = '0';
    try { assert.equal(resolveBashTimeout(undefined), 120000); } finally { restore(); }
  });

  it('ignores a negative BASH_MAX_TIMEOUT_MS (n > 0 is false → hard max ceiling)', async () => {
    const { resolveBashTimeout } = await import('../src/tools/bash');
    delete process.env.BASH_DEFAULT_TIMEOUT_MS;
    process.env.BASH_MAX_TIMEOUT_MS = '-5';
    try {
      // per-call 99999 is kept because the (invalid) max falls back to 1h ceiling
      assert.equal(resolveBashTimeout(99999), 99999);
    } finally { restore(); }
  });
});

describe('detectDetachedGitDiscard — more tokenizer edges', () => {
  const MSG = 'not allowed in non-interactive eval';
  it('handles a trailing bare newline after a discard', () => {
    assert.ok(detectDetachedGitDiscard('git stash\n')?.includes(MSG));
  });
  it('keeps quoted content as a single token (echo of a discard is inert)', () => {
    assert.equal(detectDetachedGitDiscard('echo "please run git reset --hard now"'), null);
  });
  it('allows a single-pipe read-only git stash list', () => {
    assert.equal(detectDetachedGitDiscard('git stash list | grep foo'), null);
  });
});
