import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ProcessManager, generateTaskId } from '../src/process-manager';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-u031-'));
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
// GAP U031-A: High-entropy, symlink-attack-resistant task IDs (randomBytes(8))
// ---------------------------------------------------------------------------
describe('ProcessManager – high-entropy task IDs', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('generateTaskId draws 8 random bytes (16 hex chars) behind a type prefix', () => {
    const id = generateTaskId('bg');
    assert.match(id, /^bg-[0-9a-f]{16}$/);
    // High collision-resistance: many draws are all distinct.
    const seen = new Set<string>();
    for (let i = 0; i < 1000; i++) seen.add(generateTaskId('bg'));
    assert.equal(seen.size, 1000);
  });

  it('spawn() ids use the 16-hex high-entropy form', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo hi', { cwd: tmpDir });
    assert.match(id, /^bg-[0-9a-f]{16}$/);
  });

  it('monitorMcp ids are randomised (not a bare predictable timestamp)', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const a = pm.monitorMcp('srv', async () => true, 60_000);
    const b = pm.monitorMcp('srv', async () => true, 60_000);
    assert.ok(a.startsWith('monitor-srv-'));
    assert.notEqual(a, b);
    assert.match(a, /^monitor-srv-[0-9a-f]{16}$/);
  });
});

// ---------------------------------------------------------------------------
// GAP U031-B: unref() the observational timers so they never block exit
// ---------------------------------------------------------------------------
describe('ProcessManager – observational timers are unref\'d', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('spawn stall-check interval and timeout timer are unref\'d', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const origSI = global.setInterval;
    const origST = global.setTimeout;
    const intervals: any[] = [];
    const timeouts: any[] = [];
    (global as any).setInterval = (...args: any[]) => { const t = (origSI as any)(...args); intervals.push(t); return t; };
    (global as any).setTimeout = (...args: any[]) => { const t = (origST as any)(...args); timeouts.push(t); return t; };
    try {
      pm.spawn('sleep 5', { cwd: tmpDir, timeoutMs: 60_000 });
    } finally {
      (global as any).setInterval = origSI;
      (global as any).setTimeout = origST;
    }
    assert.ok(intervals.length >= 1, 'expected a stall-check interval');
    for (const t of intervals) assert.equal(t.hasRef(), false, 'stall interval must be unref\'d');
    // The timeoutMs watchdog created synchronously in spawn() must be unref'd too.
    assert.ok(timeouts.length >= 1, 'expected the timeout watchdog');
    for (const t of timeouts) assert.equal(t.hasRef(), false, 'bg timeout must be unref\'d');
  });

  it('monitorMcp interval is unref\'d', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const origSI = global.setInterval;
    const intervals: any[] = [];
    (global as any).setInterval = (...args: any[]) => { const t = (origSI as any)(...args); intervals.push(t); return t; };
    try {
      pm.monitorMcp('srv', async () => true, 60_000);
    } finally {
      (global as any).setInterval = origSI;
    }
    assert.ok(intervals.length >= 1);
    for (const t of intervals) assert.equal(t.hasRef(), false);
  });
});

// ---------------------------------------------------------------------------
// GAP U031-C: looksLikePrompt — directed questions + "Press any key"
// ---------------------------------------------------------------------------
describe('ProcessManager – looksLikePrompt directed-question patterns', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('detects directed questions and "Press any key" without an explicit (y/n)', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const probe = (t: string): boolean => (pm as any).looksLikePrompt(t);
    assert.equal(probe('Do you want to continue? '), true);
    assert.equal(probe('Would you like to proceed? '), true);
    assert.equal(probe('Shall I overwrite the file? '), true);
    assert.equal(probe('Are you sure? '), true);
    assert.equal(probe('Ready to deploy? '), true);
    assert.equal(probe('Press any key to continue'), true);
    // Regression: existing matchers + plain output still behave.
    assert.equal(probe('Proceed? (y/n)'), true);
    assert.equal(probe('Enter your password:'), true);
    assert.equal(probe('Press Enter to continue'), true);
    assert.equal(probe('build finished successfully'), false);
    assert.equal(probe('Downloading do you know files'), false);
  });
});

// ---------------------------------------------------------------------------
// GAP U031-D: diagnostic (not silent empty) when a task output file can't be read
// ---------------------------------------------------------------------------
describe('ProcessManager – diagnostic on output-file read failure', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('surfaces a diagnostic with the errno when a spawned task file is deleted mid-run', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo hello', { cwd: tmpDir });
    await waitForComplete(pm, id);
    // Another process in the shared project dir deletes the output file.
    const entry = pm.get(id)!;
    fs.rmSync(entry.outputPath, { force: true });
    const result = pm.getOutput(id);
    assert.ok(result);
    assert.match(result!.output, /task output unavailable/i);
    assert.match(result!.output, /ENOENT/);
    assert.ok(result!.output.includes(entry.outputPath));
  });

  it('keeps an empty (not diagnostic) read for a registered agent whose file does not exist yet', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.registerAgent('agent-x', {
      command: '[agent] task',
      description: 'd',
      outputPath: path.join(tmpDir, 'never-written.out'),
      abortFn: () => {},
    });
    const result = pm.getOutput('agent-x');
    assert.ok(result);
    assert.equal(result!.output, '');
  });
});
