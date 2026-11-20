import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  ProcessManager,
  STALL_THRESHOLD_MS,
  buildCompletionSummary,
} from '../src/process-manager';

// ---------------------------------------------------------------------------
// U030 — per-task 'kind' (BashTaskKind: 'bash' | 'monitor') on a spawned
// background shell. Parity: the reference's LocalShellTask kind that (a) disables
// the stall watchdog for a streaming/monitor command and (b) words its
// completion as a stream ending rather than a command passing/failing.
// ---------------------------------------------------------------------------

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'pm-monitor-'));
}

async function cleanupPm(pm: ProcessManager | undefined): Promise<void> {
  if (!pm) return;
  try { pm.cleanup(); } catch { /* ignore */ }
  await new Promise((r) => setTimeout(r, 30));
}

function waitFor(cond: () => boolean, timeoutMs = 4000): Promise<void> {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const tick = () => {
      if (cond()) return resolve();
      if (Date.now() - start > timeoutMs) return reject(new Error('waitFor timed out'));
      setTimeout(tick, 20);
    };
    tick();
  });
}

describe('buildCompletionSummary — monitor kind wording', () => {
  it('words a monitor task as a stream ending/stopping, not pass/fail', () => {
    assert.equal(buildCompletionSummary('completed', 0, 'monitor'), 'monitor stream ended');
    assert.equal(buildCompletionSummary('killed', null, 'monitor'), 'monitor stream stopped');
    assert.equal(
      buildCompletionSummary('failed', 1, 'monitor'),
      'monitor stream ended with exit code 1',
    );
    assert.equal(buildCompletionSummary('failed', null, 'monitor'), 'monitor stream ended');
  });

  it('keeps the original pass/fail wording for a plain bash task', () => {
    assert.equal(buildCompletionSummary('completed', 0), 'completed (exit code: 0)');
    assert.equal(buildCompletionSummary('failed', 1), 'failed with exit code 1');
    assert.equal(buildCompletionSummary('killed', null), 'was stopped');
    // Default kind ('bash') is identical to omitting it.
    assert.equal(buildCompletionSummary('completed', 0, 'bash'), 'completed (exit code: 0)');
  });
});

describe('ProcessManager.spawn — kind', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('defaults a spawned task to kind "bash"', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo hi', { cwd: tmpDir });
    assert.equal(pm.taskKind(id), 'bash');
  });

  it('records an explicit monitor kind on the spawned task', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('sleep 30', { cwd: tmpDir, kind: 'monitor' });
    assert.equal(pm.taskKind(id), 'monitor');
  });

  it('DISABLES the stall watchdog for a monitor task (no stalled event, no pause, no hint)', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    // Same prompt-like-tail-then-block command the bash stall test uses, but spawned
    // as a monitor: the watchdog must NOT fire even once past the threshold.
    const id = pm.spawn("printf 'Continue? (y/n) '; sleep 30", { cwd: tmpDir, kind: 'monitor' });
    await waitFor(() => pm.getOutputSize(id) > 0);

    const stalledEvents: unknown[] = [];
    pm.on('stalled', (e) => stalledEvents.push(e));

    const t0 = Date.now();
    let last = 0;
    last = (pm as unknown as { stallTick(id: string, last: number, now: number): number }).stallTick(id, last, t0);
    last = (pm as unknown as { stallTick(id: string, last: number, now: number): number }).stallTick(id, last, t0 + STALL_THRESHOLD_MS + 1);
    (pm as unknown as { stallTick(id: string, last: number, now: number): number }).stallTick(id, last, t0 + STALL_THRESHOLD_MS + 2);

    assert.equal(stalledEvents.length, 0, 'a monitor task is never flagged as stalled');
    assert.equal(pm.getStatus(id), 'running', 'monitor task stays running');
    const out = pm.getOutput(id, 100)!.output;
    assert.doesNotMatch(out, /interactive input/i, 'no stall hint written for a monitor task');
    const entry = (pm as unknown as { processes: Map<string, { _pausedAt?: number }> }).processes.get(id)!;
    assert.equal(entry._pausedAt, undefined, 'monitor task clock is never paused by the watchdog');
  });

  it('STILL arms the stall watchdog for a default (bash) task', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn("printf 'Continue? (y/n) '; sleep 30", { cwd: tmpDir });
    await waitFor(() => pm.getOutputSize(id) > 0);

    const stalledEvents: unknown[] = [];
    pm.on('stalled', (e) => stalledEvents.push(e));

    const t0 = Date.now();
    let last = 0;
    last = (pm as unknown as { stallTick(id: string, last: number, now: number): number }).stallTick(id, last, t0);
    (pm as unknown as { stallTick(id: string, last: number, now: number): number }).stallTick(id, last, t0 + STALL_THRESHOLD_MS + 1);

    assert.equal(stalledEvents.length, 1, 'a plain bash task is still watched and flagged');
  });
});
