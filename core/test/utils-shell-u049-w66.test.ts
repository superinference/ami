import { describe, it, mock, afterEach, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as path from 'path';
import { EventEmitter } from 'events';
import { PassThrough } from 'stream';

import {
  execCommand,
  shellHooks,
  SHELL_TYPES,
  DEFAULT_SHELL_TYPE,
  resolveDefaultShell,
  buildPowerShellArgs,
  withPowerShellExitCode,
  findPowerShell,
  getCachedPowerShellPath,
  resetPowerShellCache,
  SPAWN_FAILURE_EXIT_CODE,
} from '../src/utils/shell';

// Deterministic fake child process: the test drives stdout/stderr and emits
// 'close' manually to settle execCommand without spawning a real subprocess.
class FakeProc extends EventEmitter {
  stdout = new PassThrough();
  stderr = new PassThrough();
  stdin = new PassThrough();
  pid: number | undefined = 6262;
  kill(): boolean { return true; }
}

const PS_ENV_KEYS = ['AMI_DEFAULT_SHELL', 'CLAUDE_CODE_SHELL_TYPE', 'AMI_POWERSHELL_PATH', 'CLAUDE_CODE_POWERSHELL_PATH'];

beforeEach(() => {
  for (const k of PS_ENV_KEYS) delete process.env[k];
  resetPowerShellCache();
});

afterEach(() => {
  mock.restoreAll();
  for (const k of PS_ENV_KEYS) delete process.env[k];
  resetPowerShellCache();
});

// ---------------------------------------------------------------------------
// SHELL_TYPES enum / ShellType config + safe default resolution
// ---------------------------------------------------------------------------
describe('cross-shell: SHELL_TYPES + resolveDefaultShell', () => {
  it('SHELL_TYPES lists bash and powershell as first-class shell families', () => {
    assert.deepEqual([...SHELL_TYPES], ['bash', 'powershell']);
    assert.equal(DEFAULT_SHELL_TYPE, 'bash');
  });

  it('defaults to bash when no env knob is set (never auto-flips)', () => {
    assert.equal(resolveDefaultShell(), 'bash');
  });

  it('honors AMI_DEFAULT_SHELL=powershell', () => {
    process.env.AMI_DEFAULT_SHELL = 'PowerShell';
    assert.equal(resolveDefaultShell(), 'powershell');
  });

  it('accepts CLAUDE_CODE_SHELL_TYPE as a parity fallback', () => {
    process.env.CLAUDE_CODE_SHELL_TYPE = 'powershell';
    assert.equal(resolveDefaultShell(), 'powershell');
  });

  it('safely falls back to bash for an unrecognized value', () => {
    process.env.AMI_DEFAULT_SHELL = 'fish';
    assert.equal(resolveDefaultShell(), 'bash');
  });
});

// ---------------------------------------------------------------------------
// PowerShell invocation hardening + exit-code normalization
// ---------------------------------------------------------------------------
describe('cross-shell: buildPowerShellArgs + withPowerShellExitCode', () => {
  it('builds hardened, non-interactive PowerShell args', () => {
    assert.deepEqual(buildPowerShellArgs('Get-ChildItem'), [
      '-NoProfile', '-NonInteractive', '-Command', 'Get-ChildItem',
    ]);
  });

  it('normalizes the exit code preferring $LASTEXITCODE then $?', () => {
    const wrapped = withPowerShellExitCode('git push');
    assert.ok(wrapped.startsWith('git push\n'));
    assert.match(wrapped, /\$null -ne \$LASTEXITCODE/);
    assert.match(wrapped, /elseif \(\$\?\) \{ 0 \} else \{ 1 \}/);
    assert.ok(wrapped.trimEnd().endsWith('exit $_ec'));
  });
});

// ---------------------------------------------------------------------------
// PowerShell detection (pwsh preference, snap avoidance, cwd-shadow guard,
// memoization + reset, explicit override)
// ---------------------------------------------------------------------------
describe('cross-shell: findPowerShell / getCachedPowerShellPath', () => {
  function mockWhich(map: Record<string, string | undefined>): void {
    mock.method(shellHooks, 'execFileSync', ((file: string, args: string[]) => {
      // whichSync calls which/where.exe <cmd>
      const cmd = args[0];
      const hit = map[cmd];
      if (hit === undefined) throw new Error('not found');
      return `${hit}\n`;
    }) as typeof shellHooks.execFileSync);
  }

  it('prefers pwsh over powershell', () => {
    mock.method(shellHooks, 'platform', () => 'darwin');
    mockWhich({ pwsh: '/usr/local/bin/pwsh', powershell: '/usr/local/bin/powershell' });
    assert.equal(findPowerShell(), '/usr/local/bin/pwsh');
  });

  it('falls back to powershell (5.1) when pwsh is absent', () => {
    mock.method(shellHooks, 'platform', () => 'win32');
    mockWhich({ powershell: 'C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe' });
    assert.equal(findPowerShell(), 'C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe');
  });

  it('returns null when neither pwsh nor powershell is on PATH', () => {
    mock.method(shellHooks, 'platform', () => 'linux');
    mockWhich({});
    assert.equal(findPowerShell(), null);
  });

  it('avoids a snap launcher by probing the direct install path on linux', () => {
    mock.method(shellHooks, 'platform', () => 'linux');
    mockWhich({ pwsh: '/snap/bin/pwsh' });
    mock.method(shellHooks, 'existsSync', ((p: string) => p === '/opt/microsoft/powershell/7/pwsh') as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'realpathSync', ((p: string) => p) as typeof shellHooks.realpathSync);
    assert.equal(findPowerShell(), '/opt/microsoft/powershell/7/pwsh');
  });

  it('follows a symlinked snap launcher via realpath and still avoids it', () => {
    mock.method(shellHooks, 'platform', () => 'linux');
    mockWhich({ pwsh: '/usr/bin/pwsh' });
    mock.method(shellHooks, 'realpathSync', ((p: string) => p === '/usr/bin/pwsh' ? '/snap/bin/pwsh' : p) as typeof shellHooks.realpathSync);
    mock.method(shellHooks, 'existsSync', ((p: string) => p === '/opt/microsoft/powershell/7/pwsh') as typeof shellHooks.existsSync);
    assert.equal(findPowerShell(), '/opt/microsoft/powershell/7/pwsh');
  });

  it('rejects a pwsh planted in the current working directory (cwd-shadow guard)', () => {
    mock.method(shellHooks, 'platform', () => 'linux');
    const planted = path.join(process.cwd(), 'pwsh');
    mockWhich({ pwsh: planted }); // powershell absent → overall null
    assert.equal(findPowerShell(), null);
  });

  it('honors an explicit AMI_POWERSHELL_PATH override without probing PATH', () => {
    process.env.AMI_POWERSHELL_PATH = '/custom/pwsh';
    const spy = mock.method(shellHooks, 'execFileSync', (() => { throw new Error('should not probe'); }) as typeof shellHooks.execFileSync);
    assert.equal(findPowerShell(), '/custom/pwsh');
    assert.equal(spy.mock.callCount(), 0);
  });

  it('memoizes detection and re-resolves after resetPowerShellCache', () => {
    mock.method(shellHooks, 'platform', () => 'darwin');
    let calls = 0;
    mock.method(shellHooks, 'execFileSync', ((_f: string, args: string[]) => {
      if (args[0] === 'pwsh') { calls++; return '/usr/local/bin/pwsh\n'; }
      throw new Error('not found');
    }) as typeof shellHooks.execFileSync);
    assert.equal(getCachedPowerShellPath(), '/usr/local/bin/pwsh');
    assert.equal(getCachedPowerShellPath(), '/usr/local/bin/pwsh');
    assert.equal(calls, 1, 'detection should run once while cached');
    resetPowerShellCache();
    assert.equal(getCachedPowerShellPath(), '/usr/local/bin/pwsh');
    assert.equal(calls, 2, 'reset should force re-detection');
  });
});

// ---------------------------------------------------------------------------
// execCommand wiring: shellType / shellPath → PowerShell spawn
// ---------------------------------------------------------------------------
describe('execCommand: PowerShell shell type', () => {
  function spawnCapture(fake: FakeProc): { shell?: string; args?: string[] } {
    const captured: { shell?: string; args?: string[] } = {};
    mock.method(shellHooks, 'spawn', ((cmd: string, args: string[]) => {
      captured.shell = cmd;
      captured.args = args;
      return fake as unknown as ReturnType<typeof shellHooks.spawn>;
    }) as typeof shellHooks.spawn);
    return captured;
  }

  it('spawns the resolved pwsh with hardened flags + normalized command when shellType=powershell', async () => {
    const fake = new FakeProc();
    const cap = spawnCapture(fake);
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellType: 'powershell', shellPath: '/usr/bin/pwsh' });
    fake.emit('close', 0, null);
    await p;
    assert.equal(cap.shell, '/usr/bin/pwsh');
    assert.deepEqual(cap.args, buildPowerShellArgs(withPowerShellExitCode('echo hi')));
  });

  it('selects PowerShell via the AMI_DEFAULT_SHELL env knob (no explicit shellType)', async () => {
    process.env.AMI_DEFAULT_SHELL = 'powershell';
    process.env.AMI_POWERSHELL_PATH = '/opt/pwsh';
    const fake = new FakeProc();
    const cap = spawnCapture(fake);
    const p = execCommand('whoami', { cwd: os.tmpdir(), stallTimeoutMs: 0 });
    fake.emit('close', 0, null);
    await p;
    assert.equal(cap.shell, '/opt/pwsh');
    assert.equal(cap.args?.[0], '-NoProfile');
  });

  it('surfaces a launch failure (exit 126) when no PowerShell can be found', async () => {
    mock.method(shellHooks, 'execFileSync', (() => { throw new Error('not found'); }) as typeof shellHooks.execFileSync);
    const res = await execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellType: 'powershell' });
    assert.equal(res.exitCode, SPAWN_FAILURE_EXIT_CODE);
    assert.match(res.stderr, /No PowerShell found/);
  });

  it('does NOT apply the Windows >nul rewrite on the PowerShell path', async () => {
    const fake = new FakeProc();
    const cap = spawnCapture(fake);
    const p = execCommand('cmd 2>nul', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellType: 'powershell', shellPath: '/usr/bin/pwsh' });
    fake.emit('close', 0, null);
    await p;
    assert.ok(cap.args?.[3]?.startsWith('cmd 2>nul'), 'nul must be preserved for PowerShell');
    assert.ok(!cap.args?.[3]?.includes('/dev/null'));
  });
});

// ---------------------------------------------------------------------------
// execCommand: configurable shellPath for the bash family
// ---------------------------------------------------------------------------
describe('execCommand: configurable shellPath (bash family)', () => {
  it('spawns the caller-provided bash shellPath instead of auto-resolving', async () => {
    mock.method(shellHooks, 'platform', () => 'linux');
    const fake = new FakeProc();
    let captured: { shell?: string; args?: string[] } = {};
    mock.method(shellHooks, 'spawn', ((cmd: string, args: string[]) => {
      captured = { shell: cmd, args };
      return fake as unknown as ReturnType<typeof shellHooks.spawn>;
    }) as typeof shellHooks.spawn);
    const p = execCommand('ls', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellType: 'bash', shellPath: '/custom/bin/bash' });
    fake.emit('close', 0, null);
    await p;
    assert.equal(captured.shell, '/custom/bin/bash');
    assert.ok(captured.args?.includes('-c'));
    assert.equal(captured.args?.[captured.args.length - 1], 'ls');
  });
});
