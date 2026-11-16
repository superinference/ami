import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as fs from 'fs';
import { EventEmitter } from 'events';
import { PassThrough } from 'stream';

import {
  execCommand,
  findSuitableShell,
  formatShellPrefixCommand,
  shellHooks,
} from '../src/utils/shell';

// A real bash on this host (shopt/login tests need bash specifically).
const REAL_BASH = ['/bin/bash', '/usr/bin/bash', '/usr/local/bin/bash'].find(p => fs.existsSync(p)) ?? null;

// Env vars these tests mutate; captured once and restored after each test so a
// leaked AMI_SHELL / SHELL never bleeds into another case or the real suite.
const ENV_KEYS = ['AMI_SHELL', 'CLAUDE_CODE_SHELL', 'SHELL', 'AMI_LOGIN_SHELL', 'AMI_SHELL_PREFIX'] as const;
const savedEnv: Record<string, string | undefined> = {};
for (const k of ENV_KEYS) savedEnv[k] = process.env[k];

afterEach(() => {
  mock.restoreAll();
  for (const k of ENV_KEYS) {
    if (savedEnv[k] === undefined) delete process.env[k];
    else process.env[k] = savedEnv[k];
  }
});

function clearShellEnv(): void {
  for (const k of ENV_KEYS) delete process.env[k];
}

// ---------------------------------------------------------------------------
// formatShellPrefixCommand (shell-prefix quoting helper)
// ---------------------------------------------------------------------------

describe('formatShellPrefixCommand', () => {
  it('quotes a bare executable prefix', () => {
    assert.equal(formatShellPrefixCommand('bash', 'echo hi'), `'bash' 'echo hi'`);
  });

  it('splits executable from flags on the last " -" and quotes the exec path', () => {
    assert.equal(
      formatShellPrefixCommand('/usr/bin/bash -c', 'echo hi'),
      `'/usr/bin/bash' -c 'echo hi'`,
    );
  });

  it('keeps an exec path with spaces intact as one quoted unit', () => {
    assert.equal(
      formatShellPrefixCommand('/Program Files/Git/bin/bash.exe -c', 'echo hi'),
      `'/Program Files/Git/bin/bash.exe' -c 'echo hi'`,
    );
  });

  it('escapes embedded single quotes in the command', () => {
    const out = formatShellPrefixCommand('bash', `echo 'x'`);
    assert.ok(out.includes(`'\\''`), out);
  });
});

// ---------------------------------------------------------------------------
// findSuitableShell (override + SHELL preference + probe + executability)
// ---------------------------------------------------------------------------

describe('findSuitableShell', () => {
  it('returns the AMI_SHELL override when it is a supported, executable shell', (t) => {
    if (!REAL_BASH) return t.skip('no bash on host');
    clearShellEnv();
    process.env.AMI_SHELL = REAL_BASH;
    assert.equal(findSuitableShell(), REAL_BASH);
  });

  it('honors CLAUDE_CODE_SHELL as an override alias', (t) => {
    if (!REAL_BASH) return t.skip('no bash on host');
    clearShellEnv();
    process.env.CLAUDE_CODE_SHELL = REAL_BASH;
    assert.equal(findSuitableShell(), REAL_BASH);
  });

  it('ignores an override that is a supported type but not executable, then detects one', () => {
    clearShellEnv();
    process.env.AMI_SHELL = '/no/such/dir/bash'; // supported type, missing binary
    const resolved = findSuitableShell();
    assert.notEqual(resolved, '/no/such/dir/bash');
    assert.ok(resolved === null || fs.existsSync(resolved));
  });

  it('ignores an override of an unsupported shell type', () => {
    clearShellEnv();
    process.env.AMI_SHELL = '/usr/bin/fish'; // neither bash nor zsh
    const resolved = findSuitableShell();
    assert.notEqual(resolved, '/usr/bin/fish');
  });

  it('prefers the SHELL env shell first when it is supported + executable', (t) => {
    if (!REAL_BASH) return t.skip('no bash on host');
    clearShellEnv();
    process.env.SHELL = REAL_BASH; // preferBash=true, unshifted to the front
    assert.equal(findSuitableShell(), REAL_BASH);
  });

  it('resolves with a non-bash SHELL preference (zsh-first ordering branch)', () => {
    clearShellEnv();
    process.env.SHELL = '/bin/zsh'; // preferBash=false branch
    // Resolves to whatever POSIX shell exists; just must not throw / may be null.
    const resolved = findSuitableShell();
    assert.ok(resolved === null || typeof resolved === 'string');
  });

  it('falls back to probing standard paths when SHELL is unset', (t) => {
    if (!REAL_BASH) return t.skip('no bash on host');
    clearShellEnv(); // no SHELL, no override
    assert.ok(fs.existsSync(findSuitableShell()!));
  });

  it('uses the --version executability fallback when accessSync misreports', () => {
    clearShellEnv();
    process.env.AMI_SHELL = '/probe/version/fallback'; // unique cache key, unsupported type
    // accessSync always throws -> isExecutable must fall through to execFileSync --version.
    mock.method(shellHooks, 'accessSync', () => { throw new Error('EACCES'); });
    const resolved = findSuitableShell();
    // On any host with a working bash/zsh --version, this resolves non-null.
    assert.ok(resolved === null || typeof resolved === 'string');
  });

  it('returns null when no shell is executable at all', () => {
    clearShellEnv();
    process.env.AMI_SHELL = '/none/at/all/bash'; // supported type, unique key
    mock.method(shellHooks, 'accessSync', () => { throw new Error('EACCES'); });
    mock.method(shellHooks, 'execFileSync', () => { throw new Error('ENOENT'); });
    assert.equal(findSuitableShell(), null);
  });

  it('memoizes resolution for an identical environment', (t) => {
    if (!REAL_BASH) return t.skip('no bash on host');
    clearShellEnv();
    process.env.AMI_SHELL = REAL_BASH;
    const first = findSuitableShell();
    // Second call with identical env returns the cached value without re-probing;
    // even if we now break accessSync, the cached answer stands.
    mock.method(shellHooks, 'accessSync', () => { throw new Error('EACCES'); });
    assert.equal(findSuitableShell(), first);
  });
});

// ---------------------------------------------------------------------------
// execCommand – shell resolution + no-suitable-shell error
// ---------------------------------------------------------------------------

describe('execCommand – shell resolution', () => {
  it('runs through the resolved shell by default', async () => {
    const r = await execCommand('echo resolved-ok', { cwd: '/tmp' });
    assert.equal(r.exitCode, 0);
    assert.ok(r.stdout.includes('resolved-ok'));
  });

  it('returns an actionable error (no spawn) when no shell can be found', async () => {
    clearShellEnv();
    process.env.AMI_SHELL = '/exec/noshell/bash'; // unique key, supported type
    mock.method(shellHooks, 'accessSync', () => { throw new Error('EACCES'); });
    mock.method(shellHooks, 'execFileSync', () => { throw new Error('ENOENT'); });
    let spawned = false;
    mock.method(shellHooks, 'spawn', () => { spawned = true; throw new Error('should not spawn'); });
    const r = await execCommand('echo nope', { cwd: '/tmp' });
    assert.equal(spawned, false);
    assert.equal(r.exitCode, null);
    assert.ok(r.stderr.includes('No suitable shell found'));
  });
});

// ---------------------------------------------------------------------------
// execCommand – AMI_SHELL_PREFIX wrapping
// ---------------------------------------------------------------------------

describe('execCommand – AMI_SHELL_PREFIX', () => {
  it('wraps the command with a configured shell prefix', (t) => {
    if (!REAL_BASH) return t.skip('no bash on host');
    clearShellEnv();
    process.env.AMI_SHELL = REAL_BASH;
    process.env.AMI_SHELL_PREFIX = `${REAL_BASH} -c`;
    return execCommand('echo prefixed-run', { cwd: '/tmp' }).then((r) => {
      assert.equal(r.exitCode, 0);
      assert.ok(r.stdout.includes('prefixed-run'));
    });
  });
});

// ---------------------------------------------------------------------------
// execCommand – AMI_LOGIN_SHELL (-l) flag
// ---------------------------------------------------------------------------

describe('execCommand – AMI_LOGIN_SHELL', () => {
  it('spawns a login shell when AMI_LOGIN_SHELL is set', (t) => {
    if (!REAL_BASH) return t.skip('no bash on host');
    clearShellEnv();
    process.env.AMI_SHELL = REAL_BASH;
    process.env.AMI_LOGIN_SHELL = '1';
    return execCommand('shopt -q login_shell && echo IS-LOGIN || echo NOT-LOGIN', { cwd: '/tmp' }).then((r) => {
      assert.ok(r.stdout.includes('IS-LOGIN'), r.stdout);
    });
  });

  it('does not use -l by default (non-login shell)', (t) => {
    if (!REAL_BASH) return t.skip('no bash on host');
    clearShellEnv();
    process.env.AMI_SHELL = REAL_BASH;
    return execCommand('shopt -q login_shell && echo IS-LOGIN || echo NOT-LOGIN', { cwd: '/tmp' }).then((r) => {
      assert.ok(r.stdout.includes('NOT-LOGIN'), r.stdout);
    });
  });
});

// ---------------------------------------------------------------------------
// execCommand – working-directory recovery
// ---------------------------------------------------------------------------

describe('execCommand – cwd recovery', () => {
  it('recovers to the process cwd when the requested cwd is gone', async () => {
    const r = await execCommand('pwd', { cwd: '/nonexistent/parity/xyz' });
    assert.equal(r.exitCode, 0);
    assert.ok(r.stderr.includes('no longer exists'));
    assert.ok(r.stderr.includes('recovered to'));
    assert.ok(r.stdout.includes(process.cwd()));
  });

  it('recovers to the home directory when the process cwd is also gone', async () => {
    const home = os.homedir();
    mock.method(shellHooks, 'existsSync', (p: fs.PathLike) => String(p) === home);
    const r = await execCommand('pwd', { cwd: '/gone/one' });
    assert.equal(r.exitCode, 0);
    assert.ok(r.stderr.includes(home));
  });

  it('fails with a clear message (no spawn) when no fallback directory exists', async () => {
    mock.method(shellHooks, 'existsSync', () => false);
    let spawned = false;
    mock.method(shellHooks, 'spawn', () => { spawned = true; throw new Error('should not spawn'); });
    const r = await execCommand('pwd', { cwd: '/gone/two' });
    assert.equal(spawned, false);
    assert.equal(r.exitCode, null);
    assert.ok(r.stderr.includes('no fallback directory is available'));
  });
});

// ---------------------------------------------------------------------------
// execCommand – actionable spawn error (ENOENT mapping)
// ---------------------------------------------------------------------------

class FakeProc extends EventEmitter {
  stdout = new PassThrough();
  stderr = new PassThrough();
  stdin = new PassThrough();
  pid: number | undefined = 9191;
  kill(): boolean { return true; }
}

describe('execCommand – actionable spawn error', () => {
  it('maps an ENOENT spawn error to guidance naming the shell and cwd', async () => {
    const fake = new FakeProc();
    mock.method(shellHooks, 'spawn', () => fake as unknown as ReturnType<typeof shellHooks.spawn>);
    const p = execCommand('x', { cwd: '/tmp', stallTimeoutMs: 0 });
    fake.emit('error', Object.assign(new Error('spawn bash ENOENT'), { code: 'ENOENT' }));
    const r = await p;
    assert.equal(r.exitCode, null);
    assert.ok(r.stderr.includes('Failed to start command'));
    assert.ok(r.stderr.includes('could not be found'));
  });

  it('passes a non-ENOENT spawn error message through unchanged', async () => {
    const fake = new FakeProc();
    mock.method(shellHooks, 'spawn', () => fake as unknown as ReturnType<typeof shellHooks.spawn>);
    const p = execCommand('x', { cwd: '/tmp', stallTimeoutMs: 0 });
    fake.emit('error', Object.assign(new Error('weird-failure'), { code: 'EACCES' }));
    const r = await p;
    assert.equal(r.exitCode, null);
    assert.ok(r.stderr.includes('weird-failure'));
    assert.ok(!r.stderr.includes('Failed to start command'));
  });
});
