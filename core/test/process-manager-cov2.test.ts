import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ProcessManager } from '../src/process-manager';

// ---------------------------------------------------------------------------
// ESM-safe branch coverage for process-manager.ts.
//
// These tests exercise behaviour only — no reading of source text, no
// monkeypatching of imported namespace bindings. Every process is driven
// through the real ProcessManager with real child processes and real temp
// files so each assertion is red-green meaningful.
// ---------------------------------------------------------------------------

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-cov2-'));
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

async function cleanupPm(pm: ProcessManager | null): Promise<void> {
  if (!pm) return;
  pm.cleanup();
  await new Promise(r => setTimeout(r, 100));
}

// ---------------------------------------------------------------------------
// spawn: custom env branch (`...(opts.env ? { env } : {})`)
// ---------------------------------------------------------------------------

describe('ProcessManager – spawn with custom env', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('passes a provided env to the child process', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo marker=$COV2_VAR', {
      cwd: tmpDir,
      env: { ...process.env, COV2_VAR: 'cov2value' },
    });
    await waitForComplete(pm, id);
    const result = pm.getOutput(id);
    // Discriminating: without the env-spread branch the var would be empty.
    assert.ok(result!.output.includes('marker=cov2value'));
  });
});

// ---------------------------------------------------------------------------
// spawn: proc.on('error') path — spawn into a nonexistent cwd emits ENOENT
// ---------------------------------------------------------------------------

describe('ProcessManager – spawn error path', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('marks task failed and records the error when the child cannot spawn', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const badCwd = path.join(tmpDir, 'does', 'not', 'exist');
    const id = pm.spawn('echo never-runs', { cwd: badCwd });
    const event = await waitForComplete(pm, id);
    // error handler emits complete with a null exit code.
    assert.equal(event.exitCode, null);
    const task = pm.get(id);
    assert.equal(task!.status, 'failed');
    const result = pm.getOutput(id);
    assert.ok(result!.output.includes('process error'), 'error message must be written to output');
  });
});

// ---------------------------------------------------------------------------
// spawn: writeChunk 5MB disk-cap truncation
//   covers: subarray truncation, the cap-reached message, and the early
//   `bytesWritten >= MAX_OUTPUT_BYTES` return for later chunks.
// ---------------------------------------------------------------------------

describe('ProcessManager – output disk cap', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('truncates output at the 5MB cap and records the truncation notice', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    // Emit ~6MB so the writer crosses the 5MB cap mid-stream.
    const id = pm.spawn('head -c 6291456 /dev/zero', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const size = pm.getOutputSize(id);
    // Byte accounting stops exactly at the 5MB cap.
    assert.equal(size, 5 * 1024 * 1024);
    const result = pm.getOutput(id, 0);
    assert.ok(result!.output.includes('output truncated'), 'truncation notice must be appended');
  });
});

// ---------------------------------------------------------------------------
// getOutput: readFileSync catch branch (output file deleted)
// ---------------------------------------------------------------------------

describe('ProcessManager – getOutput missing file', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns empty output (not null) when the backing file is gone', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo gone-soon', { cwd: tmpDir });
    await waitForComplete(pm, id);
    fs.rmSync(pm.get(id)!.outputPath, { force: true });
    const result = pm.getOutput(id);
    assert.ok(result, 'task still tracked, so result object is returned');
    assert.equal(result!.output, '');
    assert.equal(result!.status, 'completed');
  });
});

// ---------------------------------------------------------------------------
// looksLikePrompt: both regex branches (prompt-like vs normal last line)
// ---------------------------------------------------------------------------

describe('ProcessManager – looksLikePrompt', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('detects interactive prompts and rejects normal output', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const probe = (text: string): boolean => (pm as any).looksLikePrompt(text);
    assert.equal(probe('Proceed? (y/n)'), true);
    assert.equal(probe('install?\nContinue? '), true);
    assert.equal(probe('Enter your password:'), true);
    assert.equal(probe('build finished successfully'), false);
    assert.equal(probe(''), false);
  });
});

// ---------------------------------------------------------------------------
// runningAgentCount: only running [agent] entries count
// ---------------------------------------------------------------------------

describe('ProcessManager – runningAgentCount', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('counts only running agent entries', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    assert.equal(pm.runningAgentCount(), 0);

    pm.registerAgent('agent-1', {
      command: '[agent] research task',
      description: 'r',
      outputPath: path.join(tmpDir, 'a1.out'),
      abortFn: () => {},
    });
    pm.registerAgent('agent-2', {
      command: '[agent] second task',
      description: 's',
      outputPath: path.join(tmpDir, 'a2.out'),
      abortFn: () => {},
    });
    // A non-agent entry must not be counted.
    pm.registerAgent('plain-1', {
      command: 'echo not-an-agent',
      description: 'p',
      outputPath: path.join(tmpDir, 'p1.out'),
      abortFn: () => {},
    });

    assert.equal(pm.runningAgentCount(), 2);

    // Completing one agent drops it from the count.
    pm.updateEntry('agent-1', { status: 'completed' });
    assert.equal(pm.runningAgentCount(), 1);
  });
});

// ---------------------------------------------------------------------------
// registerAgent: entry shape + tokenCount initialisation
// ---------------------------------------------------------------------------

describe('ProcessManager – registerAgent', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('creates a running agent entry with a zeroed token count', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.registerAgent('agent-x', {
      command: '[agent] work',
      description: 'desc',
      outputPath: path.join(tmpDir, 'x.out'),
      abortFn: () => {},
    });
    const task = pm.get('agent-x');
    assert.ok(task);
    assert.equal(task!.status, 'running');
    assert.equal(task!.tokenCount, 0);
    assert.equal(task!.exitCode, null);
    assert.equal(task!.pid, process.pid);
    assert.equal(task!.description, 'desc');
  });

  it('kill() invokes the agent abort function and marks it killed', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    let aborted = false;
    pm.registerAgent('agent-k', {
      command: '[agent] abortable',
      description: 'desc',
      outputPath: path.join(tmpDir, 'k.out'),
      abortFn: () => { aborted = true; },
    });
    const killed = pm.kill('agent-k');
    assert.equal(killed, true);
    assert.equal(aborted, true, 'abortFn (proc.kill) must be called');
    assert.equal(pm.get('agent-k')!.status, 'killed');
  });
});

// ---------------------------------------------------------------------------
// updateEntry: every conditional branch
// ---------------------------------------------------------------------------

describe('ProcessManager – updateEntry', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('is a no-op for an unknown task id', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    assert.doesNotThrow(() => pm.updateEntry('nope', { status: 'completed' }));
    assert.equal(pm.get('nope'), null);
  });

  it('updates status, exitCode and tokenCount independently', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.registerAgent('agent-u', {
      command: '[agent] u',
      description: 'u',
      outputPath: path.join(tmpDir, 'u.out'),
      abortFn: () => {},
    });

    // tokenCount only — status/exitCode must stay as registered.
    pm.updateEntry('agent-u', { tokenCount: 1234 });
    let t = pm.get('agent-u')!;
    assert.equal(t.tokenCount, 1234);
    assert.equal(t.status, 'running');
    assert.equal(t.exitCode, null);

    // status + exitCode together.
    pm.updateEntry('agent-u', { status: 'completed', exitCode: 0 });
    t = pm.get('agent-u')!;
    assert.equal(t.status, 'completed');
    assert.equal(t.exitCode, 0);
    assert.equal(t.tokenCount, 1234, 'omitted field is preserved');

    // empty update changes nothing.
    pm.updateEntry('agent-u', {});
    t = pm.get('agent-u')!;
    assert.equal(t.status, 'completed');
    assert.equal(t.exitCode, 0);
    assert.equal(t.tokenCount, 1234);
  });
});
