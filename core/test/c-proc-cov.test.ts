import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as child_process from 'child_process';
import {
  ProcessManager,
  osHooks,
  procHooks,
  forceKillAfterTimeout,
  isProcessAlive,
} from '../src/process-manager';
import { getEffectiveTier } from '../src/settings-guard';
import { ToolConfirmationService } from '../src/tool-confirmation';

// ---------------------------------------------------------------------------
// Final branch-coverage closure for process-manager.ts / settings-guard.ts /
// tool-confirmation.ts.
//
// All behaviour is driven through the real exported surface (plus the osHooks
// DI seam for the Windows-only paths). No source text is read, no frozen
// namespace import is monkeypatched. Temp dirs come from fs.mkdtempSync.
// ---------------------------------------------------------------------------

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-cproc-'));
}

function waitForComplete(pm: ProcessManager, taskId: string, timeoutMs = 8000): Promise<any> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timed out waiting for ${taskId}`)), timeoutMs);
    pm.on('complete', function handler(event: any) {
      if (event.taskId === taskId) {
        clearTimeout(timer);
        pm.off('complete', handler);
        resolve(event);
      }
    });
  });
}

describe('process-manager — Windows spawn/kill branches (osHooks seam)', () => {
  const dirs: string[] = [];
  const pms: ProcessManager[] = [];
  after(async () => {
    osHooks.platform = () => os.platform();
    for (const pm of pms) { try { pm.cleanup(); } catch { /* ignore */ } }
    await new Promise(r => setTimeout(r, 100));
    for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
  });
  function mk(): ProcessManager {
    const d = makeTmpDir();
    dirs.push(d);
    const pm = new ProcessManager(d);
    pms.push(pm);
    return pm;
  }

  it('spawn uses the cmd/[/c] shell form when the platform reports win32', async () => {
    const pm = mk();
    const real = osHooks.platform;
    let id: string;
    try {
      osHooks.platform = () => 'win32';
      // On a non-Windows host "cmd" does not exist, so the child emits ENOENT —
      // but only after spawn() chose the win32 shell + `/c` argument form, which
      // is exactly the branch under test.
      id = pm.spawn('echo win-branch', { cwd: (pm as any).tasksDir });
    } finally {
      osHooks.platform = real;
    }
    const event = await waitForComplete(pm, id!);
    assert.equal(event.exitCode, null, 'cmd spawn fails on non-Windows -> error path');
    assert.equal(pm.get(id!)!.status, 'failed');
  });

  it('kill takes the win32 taskkill path and falls back when taskkill is absent', async () => {
    const pm = mk();
    const id = pm.spawn('sleep 30', { cwd: (pm as any).tasksDir });
    await new Promise(r => setTimeout(r, 150));
    const real = osHooks.platform;
    let killed: boolean;
    try {
      osHooks.platform = () => 'win32';
      // taskkill is not on PATH on Linux -> execSync throws -> catch -> proc.kill.
      killed = pm.kill(id);
    } finally {
      osHooks.platform = real;
    }
    assert.equal(killed, true);
    assert.equal(pm.get(id)!.status, 'killed');
    // Await the child's close so its stall-check interval is cleared and the
    // worker can exit cleanly (no lingering handle -> no force-exit truncation).
    await waitForComplete(pm, id);
  });
});

describe('process-manager — defensive catch / boundary branches', () => {
  const dirs: string[] = [];
  const pms: ProcessManager[] = [];
  after(async () => {
    for (const pm of pms) { try { pm.cleanup(); } catch { /* ignore */ } }
    await new Promise(r => setTimeout(r, 100));
    for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
  });
  function mk(): ProcessManager {
    const d = makeTmpDir();
    dirs.push(d);
    const pm = new ProcessManager(d);
    pms.push(pm);
    return pm;
  }

  it('swallows the write error in the process-error handler when the fd is closed', async () => {
    const pm = mk();
    const badCwd = path.join((pm as any).tasksDir, 'no', 'such', 'dir');
    const id = pm.spawn('echo never', { cwd: badCwd });
    // Close the backing fd before the asynchronous ENOENT error arrives, so the
    // error handler's fs.writeSync throws and hits its defensive catch.
    fs.closeSync((pm as any).processes.get(id).outputFd);
    const event = await waitForComplete(pm, id);
    assert.equal(event.exitCode, null);
    assert.equal(pm.get(id)!.status, 'failed');
  });

  it('inner kill catch: both the group kill and the proc.kill fallback throw', () => {
    const pm = mk();
    (pm as any).processes.set('dbl-fail', {
      taskId: 'dbl-fail',
      pid: 2147483646,
      command: 'x',
      description: 'x',
      status: 'running',
      exitCode: null,
      outputPath: '',
      startTime: Date.now(),
      proc: { pid: 2147483646, kill: () => { throw new Error('kill boom'); }, stdout: null, stderr: null, stdin: null },
      outputFd: null,
      bytesWritten: 0,
    });
    // process.kill(-pid) throws (invalid) -> outer catch -> proc.kill throws -> inner catch.
    assert.equal(pm.kill('dbl-fail'), true);
    assert.equal(pm.get('dbl-fail')!.status, 'killed');
  });

  it('destroyStreams swallows a throwing stream.destroy()', () => {
    const pm = mk();
    let destroyCalled = false;
    (pm as any).processes.set('bad-stream', {
      taskId: 'bad-stream',
      pid: 2147483646,
      command: 'x',
      description: 'x',
      status: 'running',
      exitCode: null,
      outputPath: '',
      startTime: Date.now(),
      proc: {
        pid: 2147483646,
        kill: () => {},
        stdout: { destroyed: false, destroy: () => { destroyCalled = true; throw new Error('destroy boom'); } },
        stderr: null,
        stdin: null,
      },
      outputFd: null,
      bytesWritten: 0,
    });
    assert.equal(pm.kill('bad-stream'), true);
    assert.equal(destroyCalled, true, 'stream.destroy must have been attempted');
    assert.equal(pm.get('bad-stream')!.status, 'killed');
  });
});

describe('process-manager — forceKillAfterTimeout escalation (procHooks seam)', () => {
  const realIsAlive = procHooks.isAlive;
  const realKill = procHooks.kill;
  after(() => { procHooks.isAlive = realIsAlive; procHooks.kill = realKill; });

  it('real seam still kills a genuinely running process end-to-end', async () => {
    const child = child_process.spawn('sleep', ['30'], { stdio: 'ignore' });
    const pid = child.pid!;
    assert.equal(isProcessAlive(pid), true);
    assert.equal(await forceKillAfterTimeout(pid, 200), true);
    await new Promise(r => setTimeout(r, 100));
    assert.equal(isProcessAlive(pid), false);
  });

  it('returns true via the post-loop check when the process dies after the grace window', async () => {
    // Alive at the entry + SIGTERM checks, dead at the post-loop check: this is
    // the `if (!procHooks.isAlive(pid)) return true` arm after the while loop.
    // timeoutMs=0 skips the loop body, so isAlive is consulted twice.
    const alive = [true, false];
    procHooks.isAlive = () => alive.shift() ?? false;
    procHooks.kill = () => { /* SIGTERM no-op */ };
    assert.equal(await forceKillAfterTimeout(123456, 0), true);
  });

  it('escalates to SIGKILL and swallows its throw when the kill races a death', async () => {
    // Alive throughout (entry + post-loop) -> reaches SIGKILL; the kill throws
    // (the "already dead" race) and is swallowed, still returning true.
    procHooks.isAlive = () => true;
    let sigkillAttempted = false;
    procHooks.kill = (_pid, signal) => {
      if (signal === 'SIGKILL') { sigkillAttempted = true; throw new Error('ESRCH'); }
    };
    assert.equal(await forceKillAfterTimeout(123456, 0), true);
    assert.equal(sigkillAttempted, true, 'SIGKILL branch must be reached');
  });

  it('returns false when the SIGTERM signal itself throws', async () => {
    procHooks.isAlive = () => true;
    procHooks.kill = (_pid, signal) => { if (signal === 'SIGTERM') throw new Error('EPERM'); };
    assert.equal(await forceKillAfterTimeout(123456, 0), false);
  });
});

describe('process-manager — looksLikePrompt after refactor', () => {
  it('handles empty and prompt-like last lines without a nullish fallback', () => {
    const d = makeTmpDir();
    try {
      const pm = new ProcessManager(d);
      const probe = (t: string): boolean => (pm as any).looksLikePrompt(t);
      assert.equal(probe(''), false);
      assert.equal(probe('   '), false);
      assert.equal(probe('all done'), false);
      assert.equal(probe('Overwrite? '), true);
      pm.cleanup();
    } finally {
      fs.rmSync(d, { recursive: true, force: true });
    }
  });
});

describe('settings-guard — getEffectiveTier after seed refactor', () => {
  it('returns null for an empty source list', () => {
    assert.equal(getEffectiveTier('customKey', []), null);
    assert.equal(getEffectiveTier('permissionMode', []), null);
  });

  it('tier-locked: lowest-priority-number source wins regardless of order', () => {
    // true arm (replace) then later a worse source (false arm, keep).
    assert.equal(getEffectiveTier('maxTurns', [{ tier: 'cli' }, { tier: 'global' }, { tier: 'local' }]), 'global');
    assert.equal(getEffectiveTier('maxTurns', [{ tier: 'managed' }]), 'managed');
  });

  it('normal key: highest-priority-number source wins regardless of order', () => {
    assert.equal(getEffectiveTier('customKey', [{ tier: 'managed' }, { tier: 'cli' }, { tier: 'project' }]), 'cli');
    assert.equal(getEffectiveTier('customKey', [{ tier: 'global' }]), 'global');
  });
});

describe('tool-confirmation — deepSortKeys array branch', () => {
  it('normalises array-valued params so key order does not change the hash', () => {
    const svc = new ToolConfirmationService();
    // An array value drives deepSortKeys' Array.isArray(obj) -> obj.map branch.
    svc.approve('bash', { argv: ['one', 'two'], nested: { b: 1, a: 2 } }, 'session');
    assert.equal(svc.isApproved('bash', { nested: { a: 2, b: 1 }, argv: ['one', 'two'] }), true);
    // A different array value must NOT match.
    assert.equal(svc.isApproved('bash', { argv: ['two', 'one'], nested: { a: 2, b: 1 } }), false);
  });
});
