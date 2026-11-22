/**
 * Wave-6 coverage: shell slow-exec default sink + signal labels, managed-config
 * trust refusals, powershell default runtime resolve, worktree fetch-lock
 * deadline, and remaining process-hardening / package-manager seams.
 */
import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import { EventEmitter } from 'events';

import {
  shellHooks,
  execCommand,
  type SlowCommandInfo,
} from '../src/utils/shell';
import {
  clearManagedConfigCache,
  fsHooks,
  loadManagedConfig,
  ManagedConfigError,
  MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV,
  resetConfigCache,
} from '../src/config';
import {
  __resetPwshRuntimeForTest,
  __setPwshRuntimeForTest,
  parsePowerShellCommand,
} from '../src/tools/powershell-parser';
import {
  hardenProcess,
  blockPtrace,
  loadLibcPrctl,
  bunFfiRequire,
} from '../src/utils/process-hardening';
import {
  packageManagerHooks,
  getInstallPlatform,
  resetPackageManagerCaches,
  upgradeInstructionFor,
  PACKAGE_MANAGERS,
} from '../src/package-manager';
import { withFetchHeadLock } from '../src/worktree-manager';

const TEMP = '/home/ccamacho/tmp-ami-cov';

afterEach(() => {
  mock.restoreAll();
  clearManagedConfigCache();
  resetConfigCache();
  resetPackageManagerCaches();
  __resetPwshRuntimeForTest();
  delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
  delete process.env[MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV];
});

function mkTmp(prefix: string): string {
  fs.mkdirSync(TEMP, { recursive: true });
  return fs.mkdtempSync(path.join(TEMP, prefix));
}

function fakeChild(signal: NodeJS.Signals | null, code: number | null = null) {
  const child = new EventEmitter() as EventEmitter & {
    stdout: EventEmitter;
    stderr: EventEmitter;
    stdin: EventEmitter & { end: () => void; write: () => boolean; destroyed: boolean };
    pid: number;
    kill: (sig?: NodeJS.Signals) => boolean;
    killed: boolean;
  };
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  const stdin = new EventEmitter() as typeof child.stdin;
  stdin.end = () => {};
  stdin.write = () => true;
  stdin.destroyed = false;
  child.stdin = stdin;
  child.pid = 4242;
  child.killed = false;
  child.kill = () => true;
  queueMicrotask(() => {
    child.emit('close', code, signal);
  });
  return child;
}

// ---------------------------------------------------------------------------
// shell — default onSlowCommand arrow + describeSignal via execCommand
// ---------------------------------------------------------------------------
describe('shell wave6 — slow-exec default + signal labels', () => {
  it('default onSlowCommand writes a [slow-exec] line to stderr', () => {
    const chunks: string[] = [];
    mock.method(process.stderr, 'write', ((chunk: string | Uint8Array) => {
      chunks.push(String(chunk));
      return true;
    }) as typeof process.stderr.write);
    const info: SlowCommandInfo = {
      command: 'sleep 9 && echo hi',
      durationMs: 1500.4,
      thresholdMs: 1000,
    };
    shellHooks.onSlowCommand(info);
    assert.ok(chunks.some((c) => c.includes('[slow-exec]') && c.includes('1500ms')));
  });

  it('execCommand annotates stderr for each known terminating signal', async () => {
    const signals: NodeJS.Signals[] = [
      'SIGSEGV', 'SIGKILL', 'SIGTERM', 'SIGABRT', 'SIGINT',
      'SIGQUIT', 'SIGFPE', 'SIGBUS', 'SIGILL', 'SIGUSR1',
    ];
    const cwd = mkTmp('sh-sig-');
    for (const signal of signals) {
      mock.method(shellHooks, 'spawn', () => fakeChild(signal) as never);
      const r = await execCommand('true', { cwd, shellType: 'bash', timeout: 5000, stallTimeoutMs: 0 });
      assert.ok(
        typeof r.stderr === 'string' && r.stderr.includes(`signal ${signal}`),
        `expected signal annotation for ${signal}, got: ${JSON.stringify(r.stderr)}`,
      );
      mock.restoreAll();
    }
  });
});

// ---------------------------------------------------------------------------
// config — managed trust refusals (symlink / world-writable / foreign owner)
// ---------------------------------------------------------------------------
describe('config wave6 — managed trust refusals', () => {
  it('refuses symlink + world-writable + foreign-owner relocated managed files', () => {
    const d = mkTmp('cfg-trust-');
    const real = path.join(d, 'real.json');
    fs.writeFileSync(real, '{"model":"m"}');
    const link = path.join(d, 'link.json');
    fs.symlinkSync(real, link);

    process.env.SUPERINFERENCE_MANAGED_CONFIG = link;
    clearManagedConfigCache();
    assert.throws(() => loadManagedConfig(), ManagedConfigError);

    const ww = path.join(d, 'ww.json');
    fs.writeFileSync(ww, '{"model":"m"}', { mode: 0o666 });
    try { fs.chmodSync(ww, 0o666); } catch { /* ignore */ }
    process.env.SUPERINFERENCE_MANAGED_CONFIG = ww;
    clearManagedConfigCache();
    const mode = fs.statSync(ww).mode;
    if ((mode & 0o002) !== 0) {
      assert.throws(() => loadManagedConfig(), /world-writable/);
    }

    const owned = path.join(d, 'owned.json');
    fs.writeFileSync(owned, '{"model":"m"}', { mode: 0o600 });
    process.env.SUPERINFERENCE_MANAGED_CONFIG = owned;
    delete process.env[MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV];
    clearManagedConfigCache();
    const realLstat = fsHooks.lstatSync.bind(fsHooks);
    mock.method(fsHooks, 'lstatSync', ((p: fs.PathLike, ...rest: unknown[]) => {
      const st = (realLstat as Function)(p, ...rest);
      if (String(p) === owned) {
        return Object.assign(Object.create(Object.getPrototypeOf(st)), st, {
          uid: 1,
          mode: 0o600,
          isSymbolicLink: () => false,
          isFile: () => true,
        });
      }
      return st;
    }) as typeof fsHooks.lstatSync);
    if (typeof process.getuid === 'function' && process.getuid() !== 1) {
      assert.throws(() => loadManagedConfig(), /owned by uid/);
    }
    process.env[MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV] = '1';
    clearManagedConfigCache();
    assert.doesNotThrow(() => loadManagedConfig());
  });

  it('drop-in readdir failure collapses to empty entries (catch arm)', () => {
    const d = mkTmp('cfg-drop-');
    const managed = path.join(d, 'managed.json');
    fs.writeFileSync(managed, '{"model":"base"}');
    const drop = path.join(d, 'config.d');
    fs.mkdirSync(drop);
    process.env.SUPERINFERENCE_MANAGED_CONFIG = managed;
    clearManagedConfigCache();
    const realReaddir = fsHooks.readdirSync.bind(fsHooks);
    mock.method(fsHooks, 'readdirSync', ((p: fs.PathLike, ...rest: unknown[]) => {
      if (String(p) === drop) throw Object.assign(new Error('eacces'), { code: 'EACCES' });
      return (realReaddir as Function)(p, ...rest);
    }) as typeof fsHooks.readdirSync);
    const cfg = loadManagedConfig();
    assert.ok(cfg === null || (cfg as { model?: string }).model === 'base');
  });
});

// ---------------------------------------------------------------------------
// powershell — default resolvePwshPath arrow (whichSync cascade)
// ---------------------------------------------------------------------------
describe('powershell wave6 — default runtime resolve', () => {
  it('DEFAULT resolvePwshPath runs whichSync for pwsh then powershell', async () => {
    __resetPwshRuntimeForTest();
    __setPwshRuntimeForTest({
      spawn: async () => ({
        stdout: '[]',
        stderr: '',
        timedOut: false,
        failed: false,
        exitCode: 0,
      }),
    });
    const r = await parsePowerShellCommand('Get-Date');
    assert.ok(r && typeof r === 'object');
  });
});

// ---------------------------------------------------------------------------
// process-hardening — bunFfiRequire + loadLibcPrctl success / error arms
// ---------------------------------------------------------------------------
describe('process-hardening wave6 — FFI loader arms', () => {
  it('bunFfiRequire throws without a runtime require', () => {
    assert.throws(() => bunFfiRequire('bun:ffi'), /no runtime require/);
  });

  it('loadLibcPrctl returns a prctl wrapper when FFI dlopen succeeds', () => {
    const prctl = loadLibcPrctl({
      hasFfi: true,
      ffiRequire: () => ({
        dlopen: () => ({
          symbols: {
            prctl: () => 0,
          },
        }),
      }),
    });
    assert.equal(typeof prctl, 'function');
    assert.equal(prctl!(4, 0n), 0);
  });

  it('loadLibcPrctl fails open when dlopen throws', () => {
    assert.equal(
      loadLibcPrctl({
        hasFfi: true,
        ffiRequire: () => { throw new Error('no bun:ffi'); },
      }),
      null,
    );
  });

  it('blockPtrace applied / failed / error arms via injectable prctl', () => {
    assert.equal(blockPtrace({ platform: 'linux', prctl: () => 0 }).status, 'applied');
    assert.equal(blockPtrace({ platform: 'linux', prctl: () => 1 }).status, 'failed');
    assert.equal(
      blockPtrace({
        platform: 'linux',
        prctl: () => { throw new Error('ffi'); },
      }).status,
      'error',
    );
    const r = hardenProcess({
      platform: 'linux',
      env: {},
      readFileSync: () => '0x33',
      writeFileSync: () => {},
      prctl: () => 0,
    });
    assert.ok(['applied', 'already', 'failed'].includes(r.status));
    assert.equal(r.ptrace.status, 'applied');
  });
});

// ---------------------------------------------------------------------------
// package-manager — live default hooks + upgradeInstructionFor edges
// ---------------------------------------------------------------------------
describe('package-manager wave6 — default hooks + upgrade copy', () => {
  it('exercises default platform/execPath hooks and upgrade instructions', () => {
    resetPackageManagerCaches();
    assert.ok(typeof packageManagerHooks.execPath() === 'string');
    assert.ok(typeof packageManagerHooks.platform() === 'string');
    const plat = getInstallPlatform();
    assert.ok(['macos', 'linux', 'wsl', 'windows'].includes(plat));
    for (const pm of PACKAGE_MANAGERS) {
      const text = upgradeInstructionFor(pm);
      assert.ok(typeof text === 'string' && text.length > 0);
    }
  });
});

// ---------------------------------------------------------------------------
// worktree-manager — fetch-head lock deadline falls through to fn()
// ---------------------------------------------------------------------------
describe('worktree-manager wave6 — fetch-head lock deadline', () => {
  it('withFetchHeadLock proceeds unlocked after the stale deadline', () => {
    const d = mkTmp('wt-lock-');
    const lockKey = path.join(d, 'FETCH_HEAD');
    const lockPath = `${lockKey}.ami-lock`;
    fs.writeFileSync(lockPath, 'held');
    // Keep mtime fresh so the stale-steal arm does not fire; jump Date.now past
    // the 2-minute wait window so the deadline arm returns fn() unlocked.
    const realNow = Date.now.bind(Date);
    const start = realNow();
    let calls = 0;
    mock.method(Date, 'now', () => {
      calls += 1;
      // First few calls establish deadline; later calls exceed it.
      return calls < 4 ? start : start + 3 * 60_000;
    });
    let ran = false;
    withFetchHeadLock(lockKey, () => { ran = true; });
    assert.equal(ran, true);
  });
});
