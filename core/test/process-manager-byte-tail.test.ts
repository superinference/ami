import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ProcessManager } from '../src/process-manager';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-bytetail-'));
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
// GAP U031: byte-bounded tail read of task output + total-line extrapolation.
// Claude Code's getTaskOutput/tailFile reads only the last maxBytes of the log
// (bounded memory/IO) and reports bytesTotal + an extrapolated line count so the
// model knows how much earlier output was elided. AMI previously read the WHOLE
// file into memory on every poll and reported no total-size / line signal.
// ---------------------------------------------------------------------------
describe('ProcessManager – byte-bounded tail read', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('reads only the last maxReadBytes and reports bytesTotal/bytesOmitted + an extrapolated line count', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn("printf 'line1\\nline2\\nline3\\nline4\\nline5\\n'", { cwd: tmpDir });
    await waitForComplete(pm, id);

    const fullSize = fs.statSync(pm.get(id)!.outputPath).size;
    assert.ok(fullSize >= 30, 'sanity: output file has the expected bytes');

    // A tight byte window forces the tail read to skip earlier output.
    const windowed = pm.getOutput(id, 0, undefined, 12)!;
    assert.equal(windowed.bytesTotal, fullSize, 'bytesTotal reflects the whole on-disk size');
    assert.equal(windowed.bytesOmitted, fullSize - 12, 'bytesOmitted is the skipped prefix');
    assert.ok(windowed.bytesOmitted! > 0, 'some earlier output was omitted');
    // Only the last 12 bytes were read, so the returned body is short.
    assert.ok(Buffer.byteLength(windowed.output, 'utf-8') <= 12, 'only the tail window was read');
    // Line count is extrapolated from the sample and is a positive integer.
    assert.equal(typeof windowed.totalLinesEstimate, 'number');
    assert.ok(windowed.totalLinesEstimate! >= 1);
    assert.ok(Number.isInteger(windowed.totalLinesEstimate!));
  });

  it('reads the whole file (bytesOmitted 0, exact line count) when the window exceeds the file', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn("printf 'a\\nb\\nc\\n'", { cwd: tmpDir });
    await waitForComplete(pm, id);
    const fullSize = fs.statSync(pm.get(id)!.outputPath).size;

    const whole = pm.getOutput(id, 0, undefined, 1_000_000)!;
    assert.equal(whole.bytesTotal, fullSize);
    assert.equal(whole.bytesOmitted, 0, 'nothing omitted when the window covers the file');
    // "a\nb\nc\n".split('\n') => ['a','b','c',''] => 4
    assert.equal(whole.totalLinesEstimate, 4, 'exact line count when the whole file was read');
    assert.ok(whole.output.includes('a') && whole.output.includes('c'));
  });

  it('reports size fields on the default (no byte ceiling) whole-file path too', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn("printf 'x\\ny\\n'", { cwd: tmpDir });
    await waitForComplete(pm, id);
    const fullSize = fs.statSync(pm.get(id)!.outputPath).size;

    const res = pm.getOutput(id, 0)!;
    assert.equal(res.bytesTotal, fullSize);
    assert.equal(res.bytesOmitted, 0);
    assert.equal(res.totalLinesEstimate, 3); // 'x\ny\n' -> ['x','y',''] -> 3
  });

  it('leaves size fields undefined when the output file is unreadable', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo gone', { cwd: tmpDir });
    await waitForComplete(pm, id);
    fs.rmSync(pm.get(id)!.outputPath, { force: true });

    const res = pm.getOutput(id, 0, undefined, 1024)!;
    assert.equal(res.bytesTotal, undefined);
    assert.equal(res.bytesOmitted, undefined);
    assert.equal(res.totalLinesEstimate, undefined);
    assert.match(res.output, /task output unavailable/i);
  });

  it('extrapolates a larger total-line estimate than the sample when output is omitted', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    // 20 single-char lines ("r\n" each = 2 bytes) => 40 bytes.
    const id = pm.spawn("for i in $(seq 1 20); do echo r; done", { cwd: tmpDir });
    await waitForComplete(pm, id);
    const fullSize = fs.statSync(pm.get(id)!.outputPath).size;
    assert.ok(fullSize >= 40);

    // Read ~a quarter of the file: estimate should scale up toward the real count.
    const windowed = pm.getOutput(id, 0, undefined, Math.ceil(fullSize / 4))!;
    assert.ok(windowed.bytesOmitted! > 0);
    const sampleLines = windowed.output === '' ? 0 : windowed.output.split('\n').length;
    assert.ok(windowed.totalLinesEstimate! >= sampleLines, 'estimate is at least the sampled line count');
    assert.ok(windowed.totalLinesEstimate! > 1, 'estimate scales beyond a single line');
  });
});
