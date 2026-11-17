/**
 * U049 (w83) — additive shell-util capabilities for core/core/src/utils/shell.ts.
 *
 * Parity gaps covered (all verified=confirmed):
 *  - Shared never-throw file+args exec wrapper `execFileNoThrow` with a per-call
 *    `shell?: boolean | string` toggle and cross-platform Windows `.bat`/`.cmd`
 *    handling + argument escaping (parity with Claude Code's execFileNoThrow.ts:
 *    "By using execa, Windows automatically gets shell escaping + BAT / CMD
 *    handling"). Routed into workspace-indexer's `git ls-files` discovery as the
 *    real consumer so a missing/failed git never throws.
 *  - Shell-override introspection/feedback: resolveShell emits "Using shell
 *    override: <path>" on success and a "falling back to detection" notice when
 *    the AMI_SHELL / CLAUDE_CODE_SHELL override is rejected (parity with Claude
 *    Code's Shell.ts override logging), through the opt-in onShellDebug surface.
 */
import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as child_process from 'child_process';

import {
  execFileNoThrow,
  quoteWindowsShellArg,
  findSuitableShell,
  shellHooks,
  DEFAULT_EXEC_FILE_TIMEOUT_MS,
  DEFAULT_EXEC_FILE_MAX_BUFFER,
  type ExecFileNoThrowResult,
} from '../src/utils/shell';

const NODE = process.execPath;

afterEach(() => {
  mock.restoreAll();
});

describe('execFileNoThrow — never-throw file+args exec (real subprocess)', () => {
  it('resolves code 0 with captured stdout on success', async () => {
    const r = await execFileNoThrow(NODE, ['-e', 'process.stdout.write("hi there")']);
    assert.equal(r.code, 0);
    assert.equal(r.stdout, 'hi there');
    assert.equal(r.error, undefined);
  });

  it('maps a non-zero exit to a result (never throws) and preserves output by default', async () => {
    const r = await execFileNoThrow(NODE, ['-e', 'process.stderr.write("boom");process.exit(3)']);
    assert.equal(r.code, 3);
    assert.match(r.stderr, /boom/);
    assert.ok(r.error, 'a non-zero exit carries a human-readable error');
  });

  it('drops output on a non-zero exit when preserveOutputOnError is false', async () => {
    const r = await execFileNoThrow(
      NODE,
      ['-e', 'process.stdout.write("keep?");process.stderr.write("drop?");process.exit(7)'],
      { preserveOutputOnError: false },
    );
    assert.equal(r.code, 7);
    assert.equal(r.stdout, '');
    assert.equal(r.stderr, '');
    assert.equal(r.error, undefined);
  });

  it('never throws when the binary does not exist (ENOENT → code 1 + error)', async () => {
    const r = await execFileNoThrow('this-binary-does-not-exist-ami-u049', ['--version']);
    assert.equal(r.code, 1);
    assert.ok(r.error && r.error.length > 0);
  });

  it('short-circuits an already-aborted signal with error "Aborted"', async () => {
    const r = await execFileNoThrow(NODE, ['-e', 'process.exit(0)'], {
      abortSignal: AbortSignal.abort(),
    });
    assert.equal(r.code, 1);
    assert.equal(r.error, 'Aborted');
  });

  it('kills an in-flight command when the abort signal fires', async () => {
    const ac = new AbortController();
    const p = execFileNoThrow(NODE, ['-e', 'setTimeout(() => {}, 10000)'], {
      abortSignal: ac.signal,
    });
    setTimeout(() => ac.abort(), 50);
    const r = await p;
    assert.notEqual(r.code, 0);
    assert.ok(r.error);
  });

  it('enforces the per-call timeout (never hangs)', async () => {
    const r = await execFileNoThrow(NODE, ['-e', 'setTimeout(() => {}, 10000)'], {
      timeout: 100,
    });
    assert.notEqual(r.code, 0);
    assert.ok(r.error);
  });

  it('feeds the input payload to the child stdin', async () => {
    const r = await execFileNoThrow(
      NODE,
      ['-e', "let d='';process.stdin.on('data',c=>d+=c);process.stdin.on('end',()=>process.stdout.write(d.toUpperCase()))"],
      { input: 'abc' },
    );
    assert.equal(r.code, 0);
    assert.equal(r.stdout, 'ABC');
  });

  it('runs through a POSIX shell when shell:true, escaping args safely', async (t) => {
    if (process.platform === 'win32') { t.skip('POSIX shell path'); return; }
    const r = await execFileNoThrow(NODE, ['-e', 'process.stdout.write("via shell")'], {
      shell: true,
    });
    assert.equal(r.code, 0);
    assert.equal(r.stdout, 'via shell');
  });
});

describe('execFileNoThrow — Windows .bat/.cmd auto-shell (mocked platform)', () => {
  it('auto-enables the shell for a .cmd target and escapes the argv into one command', async () => {
    mock.method(shellHooks, 'platform', () => 'win32' as NodeJS.Platform);
    let capturedTarget = '';
    let capturedArgs: readonly string[] = ['unset'];
    let capturedShell: unknown = 'unset';
    mock.method(
      shellHooks,
      'execFile',
      ((target: string, args: readonly string[], opts: Record<string, unknown>, cb: (e: null, o: string, e2: string) => void) => {
        capturedTarget = target;
        capturedArgs = args;
        capturedShell = opts.shell;
        cb(null, 'ok', '');
        return { stdin: null } as unknown as child_process.ChildProcess;
      }) as unknown as typeof shellHooks.execFile,
    );
    const r = await execFileNoThrow('tool.cmd', ['a b', 'plain'], {});
    assert.equal(r.code, 0);
    assert.equal(r.stdout, 'ok');
    assert.equal(capturedShell, true, '.cmd target must auto-run through a shell');
    assert.deepEqual(capturedArgs, [], 'args are folded into the command string');
    assert.equal(capturedTarget, 'tool.cmd "a b" plain');
  });

  it('respects an explicit shell:false on Windows (no auto-shell, argv passed through)', async () => {
    mock.method(shellHooks, 'platform', () => 'win32' as NodeJS.Platform);
    let capturedShell: unknown = 'unset';
    let capturedArgs: readonly string[] = ['unset'];
    mock.method(
      shellHooks,
      'execFile',
      ((target: string, args: readonly string[], opts: Record<string, unknown>, cb: (e: null, o: string, e2: string) => void) => {
        capturedShell = opts.shell;
        capturedArgs = args;
        cb(null, '', '');
        return { stdin: null } as unknown as child_process.ChildProcess;
      }) as unknown as typeof shellHooks.execFile,
    );
    await execFileNoThrow('tool.cmd', ['x'], { shell: false });
    assert.equal(capturedShell, false, 'explicit shell:false overrides the .cmd auto-shell');
    assert.deepEqual(capturedArgs, ['x'], 'argv is passed verbatim when no shell');
  });
});

describe('quoteWindowsShellArg', () => {
  it('quotes empty, whitespace, and metacharacter args; leaves plain tokens alone', () => {
    assert.equal(quoteWindowsShellArg(''), '""');
    assert.equal(quoteWindowsShellArg('plain'), 'plain');
    assert.equal(quoteWindowsShellArg('a b'), '"a b"');
    assert.equal(quoteWindowsShellArg('a&b'), '"a&b"');
    assert.equal(quoteWindowsShellArg('say "hi"'), '"say \\"hi\\""');
  });
});

describe('execFileNoThrow — exported defaults', () => {
  it('exposes the reference-parity default timeout (10 min) and maxBuffer (10MB)', () => {
    assert.equal(DEFAULT_EXEC_FILE_TIMEOUT_MS, 10 * 60 * 1000);
    assert.equal(DEFAULT_EXEC_FILE_MAX_BUFFER, 10 * 1024 * 1024);
  });

  it('result shape has stdout/stderr/code (error optional)', () => {
    const r: ExecFileNoThrowResult = { stdout: '', stderr: '', code: 0 };
    assert.equal(r.code, 0);
  });
});

describe('shell-override introspection (resolveShell → onShellDebug)', () => {
  const savedAmi = process.env.AMI_SHELL;
  const savedClaude = process.env.CLAUDE_CODE_SHELL;
  const savedShell = process.env.SHELL;

  afterEach(() => {
    if (savedAmi === undefined) delete process.env.AMI_SHELL; else process.env.AMI_SHELL = savedAmi;
    if (savedClaude === undefined) delete process.env.CLAUDE_CODE_SHELL; else process.env.CLAUDE_CODE_SHELL = savedClaude;
    if (savedShell === undefined) delete process.env.SHELL; else process.env.SHELL = savedShell;
  });

  it('emits "Using shell override" when the AMI_SHELL override is executable', () => {
    // Unique path → unique resolution-cache key (not a cached prior result).
    const override = `/opt/u049w83/bin/bash-${Date.now()}`;
    process.env.AMI_SHELL = override;
    delete process.env.CLAUDE_CODE_SHELL;
    const msgs: string[] = [];
    mock.method(shellHooks, 'onShellDebug', (m: string) => { msgs.push(m); });
    // Make the override pass the executability probe.
    mock.method(shellHooks, 'accessSync', () => undefined);
    const resolved = findSuitableShell();
    assert.equal(resolved, override);
    assert.ok(msgs.some(m => m === `Using shell override: ${override}`), msgs.join(' | '));
  });

  it('emits a "falling back to detection" notice when the override is not runnable', () => {
    const override = `/opt/u049w83/bin/bash-reject-${Date.now()}`;
    process.env.AMI_SHELL = override;
    delete process.env.CLAUDE_CODE_SHELL;
    delete process.env.SHELL;
    const msgs: string[] = [];
    mock.method(shellHooks, 'onShellDebug', (m: string) => { msgs.push(m); });
    // Override fails BOTH the X_OK check and the --version fallback → rejected.
    mock.method(shellHooks, 'accessSync', () => { throw new Error('no access'); });
    mock.method(shellHooks, 'execFileSync', () => { throw new Error('no run'); });
    findSuitableShell();
    assert.ok(
      msgs.some(m => m === `Shell override "${override}" is not a valid bash/zsh path, falling back to detection`),
      msgs.join(' | '),
    );
  });
});
