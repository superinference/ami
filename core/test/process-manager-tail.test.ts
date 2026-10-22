import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ProcessManager } from '../src/process-manager';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-tail-test-'));
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

describe('ProcessManager – getOutputTail (incremental tailing)', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns null for unknown task ID', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    assert.equal(pm.getOutputTail('bg-nope', 0), null);
  });

  it('reads only the bytes appended since the previous offset', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('printf part1', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const outputPath = pm.get(id)!.outputPath;

    const first = pm.getOutputTail(id, 0);
    assert.ok(first);
    assert.equal(first!.reset, false);
    assert.ok(first!.output.includes('part1'));
    assert.equal(first!.offset, fs.statSync(outputPath).size);

    // Simulate further streamed output appended to the task file.
    fs.appendFileSync(outputPath, 'part2\n');

    const second = pm.getOutputTail(id, first!.offset);
    assert.ok(second);
    // Discriminating: a full re-read would still contain 'part1'.
    assert.equal(second!.output, 'part2\n');
    assert.ok(!second!.output.includes('part1'));
    assert.equal(second!.offset, fs.statSync(outputPath).size);
  });

  it('returns an empty chunk when the offset is already at EOF', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('printf hello', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const size = fs.statSync(pm.get(id)!.outputPath).size;

    const tail = pm.getOutputTail(id, size);
    assert.ok(tail);
    assert.equal(tail!.output, '');
    assert.equal(tail!.offset, size);
    assert.equal(tail!.reset, false);
  });

  it('signals reset and rebuilds from the start when the file shrank', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('printf shortdata', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const size = fs.statSync(pm.get(id)!.outputPath).size;

    // Caller's cursor is past the current end (file was truncated/rotated).
    const tail = pm.getOutputTail(id, size + 1000);
    assert.ok(tail);
    assert.equal(tail!.reset, true);
    assert.ok(tail!.output.includes('shortdata'));
    assert.equal(tail!.offset, size);
  });

  it('treats a negative offset as a reset', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('printf negcase', { cwd: tmpDir });
    await waitForComplete(pm, id);

    const tail = pm.getOutputTail(id, -5);
    assert.ok(tail);
    assert.equal(tail!.reset, true);
    assert.ok(tail!.output.includes('negcase'));
  });

  it('returns an empty chunk when the output file is missing', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('printf willvanish', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const outputPath = pm.get(id)!.outputPath;
    fs.rmSync(outputPath, { force: true });

    const tail = pm.getOutputTail(id, 0);
    assert.ok(tail);
    assert.equal(tail!.output, '');
    assert.equal(tail!.offset, 0);
    assert.equal(tail!.reset, false);
  });
});
