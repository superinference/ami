import { describe, it, afterEach, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  ProcessManager,
  formatBackgroundOutputDeltaNote,
  OUTPUT_DELTA_MAX_BYTES,
  type OutputDelta,
} from '../src/process-manager';

// ---------------------------------------------------------------------------
// U031 / w113: incremental OUTPUT delta reporting of still-running background
// tasks.
//
// Parity with Claude Code's LocalShellTaskState `lastReportedTotalLines` delta
// cursor: a long-running background shell's NEW output is surfaced to the model
// incrementally (periodic progress attachments) instead of the model learning
// nothing until the task completes. AMI had the primitive (getOutputTail) but it
// was UNWIRED — no per-task reported-offset cursor and no engine injection.
// reportOutputDelta / drainRunningOutputDeltas add that cursor + batch drain, and
// formatBackgroundOutputDeltaNote is the single model-facing wording source.
//
// Red-green: reverting any of these methods/helpers removes the symbol the cases
// below call, failing the suite.
// ---------------------------------------------------------------------------

const tmpDirs: string[] = [];
function makeTmpDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-w113-'));
  tmpDirs.push(d);
  return d;
}

function waitForComplete(pm: ProcessManager, taskId: string, timeoutMs = 5000): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timed out waiting for ${taskId}`)), timeoutMs);
    pm.on('complete', function handler(event: { taskId: string }) {
      if (event.taskId === taskId) {
        clearTimeout(timer);
        pm.off('complete', handler);
        resolve(event);
      }
    });
  });
}

/** Poll until the running task's output file contains `needle` (or time out). */
async function waitForOutput(pm: ProcessManager, taskId: string, needle: string, timeoutMs = 4000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const out = pm.getOutput(taskId, 0);
    if (out && out.output.includes(needle)) return;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error(`Timed out waiting for output "${needle}" on ${taskId}`);
}

after(() => {
  for (const d of tmpDirs) fs.rmSync(d, { recursive: true, force: true });
});

describe('ProcessManager – incremental output deltas (reportOutputDelta)', () => {
  let pm: ProcessManager | null = null;
  afterEach(async () => { pm?.cleanup(); await new Promise((r) => setTimeout(r, 80)); pm = null; });

  it('first report returns the full appended output and advances the offset', async () => {
    const dir = makeTmpDir();
    pm = new ProcessManager(dir);
    const id = pm.spawn("printf 'hello\\nworld\\n'", { cwd: dir });
    await waitForComplete(pm, id);

    const first = pm.reportOutputDelta(id);
    assert.ok(first, 'expected a delta on the first report');
    assert.match(first!.output, /hello/);
    assert.match(first!.output, /world/);
    assert.equal(first!.reset, false);
    assert.equal(first!.bytesOmitted, 0);
    assert.equal(first!.taskId, id);
    assert.equal(first!.kind, 'bash');
  });

  it('a second report with no new output returns undefined (nothing re-reported)', async () => {
    const dir = makeTmpDir();
    pm = new ProcessManager(dir);
    const id = pm.spawn("printf 'once\\n'", { cwd: dir });
    await waitForComplete(pm, id);

    assert.ok(pm.reportOutputDelta(id), 'first report yields the output');
    assert.equal(pm.reportOutputDelta(id), undefined, 'second report has nothing new');
  });

  it('caps an over-cap delta to the last maxBytes and counts the dropped prefix', async () => {
    const dir = makeTmpDir();
    pm = new ProcessManager(dir);
    // Produce well over the tiny cap used below.
    const id = pm.spawn("for i in $(seq 1 200); do echo LINE$i; done", { cwd: dir });
    await waitForComplete(pm, id);

    const delta = pm.reportOutputDelta(id, 64)!;
    assert.ok(delta, 'expected a delta');
    assert.ok(Buffer.byteLength(delta.output, 'utf-8') <= 64, 'output trimmed to the cap');
    assert.ok(delta.bytesOmitted > 0, 'the dropped prefix is counted');
    // The TAIL is kept, so the last line survives and an early line is dropped.
    assert.match(delta.output, /LINE200/);
    assert.doesNotMatch(delta.output, /LINE1\b/);
  });

  it('returns undefined for an unknown task', async () => {
    const dir = makeTmpDir();
    pm = new ProcessManager(dir);
    assert.equal(pm.reportOutputDelta('does-not-exist'), undefined);
  });

  it('returns undefined for a task with no output file (abort-before-spawn stub)', async () => {
    const dir = makeTmpDir();
    pm = new ProcessManager(dir);
    const ctl = new AbortController();
    ctl.abort();
    const id = pm.spawn('echo never', { cwd: dir, signal: ctl.signal });
    // The aborted stub has an empty outputPath, so there is no file to tail.
    assert.equal(pm.reportOutputDelta(id), undefined);
  });
});

describe('ProcessManager – drainRunningOutputDeltas (batch)', () => {
  let pm: ProcessManager | null = null;
  afterEach(async () => { pm?.cleanup(); await new Promise((r) => setTimeout(r, 80)); pm = null; });

  it('surfaces new output of a still-running task, then nothing until more arrives', async () => {
    const dir = makeTmpDir();
    pm = new ProcessManager(dir);
    const id = pm.spawn("printf 'progress-1\\n'; sleep 1.5", { cwd: dir });
    await waitForOutput(pm, id, 'progress-1');

    const first = pm.drainRunningOutputDeltas();
    assert.equal(first.length, 1, 'one running task produced new output');
    assert.equal(first[0].taskId, id);
    assert.match(first[0].output, /progress-1/);

    // Immediately draining again yields nothing — the offset advanced.
    const second = pm.drainRunningOutputDeltas();
    assert.equal(second.length, 0, 'no new output since the last drain');

    pm.kill(id);
  });

  it('excludes TERMINAL tasks — a completed task is not re-surfaced as a running delta', async () => {
    const dir = makeTmpDir();
    pm = new ProcessManager(dir);
    const id = pm.spawn("printf 'done\\n'", { cwd: dir });
    await waitForComplete(pm, id);
    assert.equal(pm.drainRunningOutputDeltas().length, 0, 'a finished task is not a running delta');
  });

  it('excludes in-process agents (no spooled shell output)', async () => {
    const dir = makeTmpDir();
    pm = new ProcessManager(dir);
    pm.registerAgent('agent-1', {
      command: '[agent] do work',
      description: 'worker',
      outputPath: path.join(dir, 'agent-1.output'),
      abortFn: () => { /* noop */ },
    });
    // Even if an agent output file exists, registered agents are skipped.
    fs.writeFileSync(path.join(dir, 'agent-1.output'), 'agent chatter\n');
    assert.equal(pm.drainRunningOutputDeltas().length, 0, 'registered agents are excluded');
    pm.kill('agent-1');
  });
});

describe('formatBackgroundOutputDeltaNote (wording)', () => {
  const base: OutputDelta = { taskId: 'bg-1', description: 'build', kind: 'bash', output: 'compiling...\n\n', reset: false, bytesOmitted: 0 };

  it('renders the task id, description, and trailing-newline-trimmed output', () => {
    const note = formatBackgroundOutputDeltaNote(base);
    assert.match(note, /Background task "bg-1" \(build\) new output:/);
    assert.match(note, /compiling\.\.\.\]$/, 'trailing newlines are trimmed before the close bracket');
  });

  it('flags a reset (rotated/truncated) read', () => {
    const note = formatBackgroundOutputDeltaNote({ ...base, reset: true });
    assert.match(note, /truncated\/rotated/);
  });

  it('flags an omitted over-cap prefix with a human-readable byte cap', () => {
    const note = formatBackgroundOutputDeltaNote({ ...base, bytesOmitted: 2048 });
    assert.match(note, /earlier 2KB of this delta omitted/);
  });

  it('exports a positive default byte cap', () => {
    assert.ok(OUTPUT_DELTA_MAX_BYTES > 0);
  });
});
