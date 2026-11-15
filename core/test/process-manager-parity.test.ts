import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  ProcessManager,
  isProcessAlive,
  generateTaskId,
  openTaskOutputWrite,
  openTaskOutputRead,
} from '../src/process-manager';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-parity-'));
}

function waitForComplete(pm: ProcessManager, taskId: string, timeoutMs = 5000): Promise<any> {
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

async function cleanupPm(pm: ProcessManager | null): Promise<void> {
  if (!pm) return;
  pm.cleanup();
  await new Promise(r => setTimeout(r, 100));
}

// ---------------------------------------------------------------------------
// generateTaskId — high-entropy, prefixed, symlink-attack-resistant ids
// ---------------------------------------------------------------------------

describe('generateTaskId', () => {
  it('produces <prefix>-<8 chars over a 36-symbol alphabet>', () => {
    const id = generateTaskId('bg');
    assert.match(id, /^bg-[0-9a-z]{8}$/);
  });

  it('honors the prefix argument', () => {
    assert.match(generateTaskId('agent'), /^agent-[0-9a-z]{8}$/);
  });

  it('is effectively unique across many draws', () => {
    const seen = new Set<string>();
    for (let i = 0; i < 2000; i++) seen.add(generateTaskId('bg'));
    assert.equal(seen.size, 2000);
  });
});

// ---------------------------------------------------------------------------
// Symlink hardening — O_EXCL / O_NOFOLLOW on task output open
// ---------------------------------------------------------------------------

describe('openTaskOutputWrite — symlink hardening', () => {
  let tmpDir: string;
  afterEach(() => { if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('opens a fresh path for writing and the bytes land in the real file', () => {
    tmpDir = makeTmpDir();
    const p = path.join(tmpDir, 'fresh.output');
    const fd = openTaskOutputWrite(p);
    try { fs.writeSync(fd, Buffer.from('hello')); } finally { fs.closeSync(fd); }
    assert.equal(fs.readFileSync(p, 'utf-8'), 'hello');
  });

  it('refuses to open through a planted symlink (never clobbers the target)', () => {
    tmpDir = makeTmpDir();
    const secret = path.join(tmpDir, 'secret.txt');
    fs.writeFileSync(secret, 'TOP-SECRET');
    const link = path.join(tmpDir, 'bg-attack.output');
    fs.symlinkSync(secret, link);
    // O_EXCL (path already exists as a symlink) + O_NOFOLLOW both defeat this.
    assert.throws(() => openTaskOutputWrite(link));
    // The target was NOT truncated or overwritten.
    assert.equal(fs.readFileSync(secret, 'utf-8'), 'TOP-SECRET');
  });
});

describe('openTaskOutputRead — symlink hardening', () => {
  let tmpDir: string;
  afterEach(() => { if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('reads a real file', () => {
    tmpDir = makeTmpDir();
    const p = path.join(tmpDir, 'real.output');
    fs.writeFileSync(p, 'content');
    const fd = openTaskOutputRead(p);
    try {
      const buf = Buffer.allocUnsafe(7);
      const n = fs.readSync(fd, buf, 0, 7, 0);
      assert.equal(buf.subarray(0, n).toString('utf-8'), 'content');
    } finally { fs.closeSync(fd); }
  });

  it('refuses to follow a symlink on read', () => {
    tmpDir = makeTmpDir();
    const secret = path.join(tmpDir, 'secret.txt');
    fs.writeFileSync(secret, 'TOP-SECRET');
    const link = path.join(tmpDir, 'link.output');
    fs.symlinkSync(secret, link);
    assert.throws(() => openTaskOutputRead(link));
  });
});

// ---------------------------------------------------------------------------
// isProcessAlive — pid<=1 guard
// ---------------------------------------------------------------------------

describe('isProcessAlive — pid<=1 guard', () => {
  it('returns false for pid 0 (whole current process group)', () => {
    assert.equal(isProcessAlive(0), false);
  });
  it('returns false for pid 1 (init)', () => {
    assert.equal(isProcessAlive(1), false);
  });
  it('returns false for a negative pid', () => {
    assert.equal(isProcessAlive(-1), false);
  });
});

// ---------------------------------------------------------------------------
// endTime — frozen, clamped elapsed time
// ---------------------------------------------------------------------------

describe('ProcessManager – endTime / frozen elapsed', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('records endTime on completion and freezes elapsed time', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo done', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const task = pm.get(id);
    assert.ok(typeof task!.endTime === 'number', 'endTime must be set on terminal');
    const first = pm.getOutput(id)!.elapsedMs;
    await new Promise(r => setTimeout(r, 120));
    const second = pm.getOutput(id)!.elapsedMs;
    assert.equal(first, second, 'elapsed must freeze once terminal');
  });

  it('records endTime on kill', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('sleep 60', { cwd: tmpDir });
    await new Promise(r => setTimeout(r, 80));
    pm.kill(id);
    assert.ok(typeof pm.get(id)!.endTime === 'number');
  });

  it('clamps elapsed to 0 when endTime precedes startTime (clock skew)', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo x', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const entry = (pm as any).processes.get(id);
    entry.endTime = entry.startTime - 5000;
    assert.equal(pm.getOutput(id)!.elapsedMs, 0);
  });
});

// ---------------------------------------------------------------------------
// getOutput — bounded read + truncation indicator
// ---------------------------------------------------------------------------

describe('ProcessManager – getOutput truncation indicator', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('reports bytesTotal and truncated=false for a fully-shown small file', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo small', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const r = pm.getOutput(id, 0)!;
    assert.ok(r.bytesTotal > 0);
    assert.equal(r.truncated, false);
  });

  it('flags truncated=true when more lines exist than the tail shows', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('for i in $(seq 1 20); do echo "line$i"; done', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const r = pm.getOutput(id, 5)!;
    assert.equal(r.truncated, true);
    assert.ok(r.output.includes('line20'));
    assert.ok(!r.output.includes('line1\n'));
  });
});

// ---------------------------------------------------------------------------
// Stall watchdog — notify instead of silent kill
// ---------------------------------------------------------------------------

describe('ProcessManager – stall notify (not kill)', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('writes remediation into output and leaves the task running (once)', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn("printf 'Overwrite? ' && sleep 5", { cwd: tmpDir });
    await new Promise(r => setTimeout(r, 400));
    const notified = (pm as any).notifyStalledOnPrompt(id);
    assert.equal(notified, true);
    const out = pm.getOutput(id, 0)!;
    assert.ok(out.output.includes('waiting for interactive input'), out.output);
    assert.ok(out.output.includes('echo y | command'));
    assert.equal(pm.get(id)!.status, 'running', 'stall must NOT kill the task');
    // Dedup: a second call is a no-op.
    assert.equal((pm as any).notifyStalledOnPrompt(id), false);
    pm.kill(id);
  });

  it('does nothing when the tail is not prompt-like', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn("echo progressing && sleep 5", { cwd: tmpDir });
    await new Promise(r => setTimeout(r, 300));
    assert.equal((pm as any).notifyStalledOnPrompt(id), false);
    pm.kill(id);
  });

  it('returns false for an unknown task', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    assert.equal((pm as any).notifyStalledOnPrompt('bg-nope'), false);
  });

  it('returns false for an entry with no output fd (registered agent)', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.registerAgent('agent-x', {
      command: '[agent] x', description: 'x',
      outputPath: path.join(tmpDir, 'a.out'), abortFn: () => {},
    });
    assert.equal((pm as any).notifyStalledOnPrompt('agent-x'), false);
  });
});

// ---------------------------------------------------------------------------
// updateEntry — terminal status is final (killed is never clobbered)
// ---------------------------------------------------------------------------

describe('ProcessManager – updateEntry terminal guard', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('does not let a late completion clobber a killed agent', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.registerAgent('agent-k', {
      command: '[agent] k', description: 'k',
      outputPath: path.join(tmpDir, 'k.out'), abortFn: () => {},
    });
    assert.equal(pm.kill('agent-k'), true);
    assert.equal(pm.get('agent-k')!.status, 'killed');
    // The subagent loop's finally-block update must NOT flip killed -> completed.
    pm.updateEntry('agent-k', { status: 'completed', exitCode: 0 });
    assert.equal(pm.get('agent-k')!.status, 'killed');
  });

  it('sets a terminal status + endTime on a running agent', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.registerAgent('agent-c', {
      command: '[agent] c', description: 'c',
      outputPath: path.join(tmpDir, 'c.out'), abortFn: () => {},
    });
    pm.updateEntry('agent-c', { status: 'completed' });
    const t = pm.get('agent-c')!;
    assert.equal(t.status, 'completed');
    assert.ok(typeof t.endTime === 'number');
  });

  it('is a no-op for an unknown task id', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    assert.doesNotThrow(() => pm.updateEntry('bg-nope', { status: 'completed' }));
  });
});

// ---------------------------------------------------------------------------
// getOutput — symlinked registered output path is not followed
// ---------------------------------------------------------------------------

describe('ProcessManager – getOutput symlink safety', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('does not leak a symlinked target through getOutput', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const secret = path.join(tmpDir, 'secret.txt');
    fs.writeFileSync(secret, 'TOP-SECRET');
    const link = path.join(tmpDir, 'planted.output');
    fs.symlinkSync(secret, link);
    pm.registerAgent('agent-s', {
      command: '[agent] s', description: 's', outputPath: link, abortFn: () => {},
    });
    const r = pm.getOutput('agent-s', 0)!;
    assert.ok(!r.output.includes('TOP-SECRET'), 'symlinked target must not be read');
  });
});

// ---------------------------------------------------------------------------
// list() — running-first, newest-first ordering
// ---------------------------------------------------------------------------

describe('ProcessManager – list() ordering', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('sorts running tasks first, then newest-first', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const done = pm.spawn('echo first', { cwd: tmpDir });
    await waitForComplete(pm, done);
    await new Promise(r => setTimeout(r, 5));
    const runOld = pm.spawn('sleep 60', { cwd: tmpDir });
    await new Promise(r => setTimeout(r, 5));
    const runNew = pm.spawn('sleep 60', { cwd: tmpDir });
    const ids = pm.list().map(t => t.taskId);
    // Both running tasks precede the completed one...
    assert.ok(ids.indexOf(runOld) < ids.indexOf(done));
    assert.ok(ids.indexOf(runNew) < ids.indexOf(done));
    // ...and the newest running task comes first.
    assert.ok(ids.indexOf(runNew) < ids.indexOf(runOld));
    pm.kill(runOld);
    pm.kill(runNew);
  });
});
