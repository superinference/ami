/**
 * U031 — Bash stdout/stderr routed directly to a file fd, bypassing the JS heap
 * ("file mode"). Parity with the reference's TaskOutput stdoutToFile (L21-36): a bash
 * command's stdout AND stderr go STRAIGHT to the output-file fd so "neither enters
 * JS". AMI previously always piped every byte through JS ('data' listeners +
 * writeChunk). This adds an opt-in file mode where ProcessManager.spawn wires
 * `stdio: ['ignore', fd, fd]`, derives size by polling the file, enforces the runaway
 * byte-cap from that poll, and leaves stderr interleaved in the merged file.
 *
 * RED-GREEN discriminators:
 *   - spawn({fileMode:true}) ⇒ the child has NO parent-side stdout/stderr stream
 *     (proc.stdout === null). Before the fix spawn ignored fileMode and still piped,
 *     so proc.stdout was a readable stream (not null) → the assertion fails.
 *   - getOutputSize reads the on-disk size in file mode (bytesWritten stays 0).
 *   - the poll-driven cap kills a runaway file-mode task.
 *   - bash.ts threads fileMode when AMI_BASH_OUTPUT_FILE_MODE is truthy.
 */
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { ProcessManager } from '../src/process-manager';
import {
  bashTool,
  isBashOutputFileModeEnabled,
  BASH_OUTPUT_FILE_MODE_ENV,
} from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'pm-u031fm-'));
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

let pm: ProcessManager | undefined;
let tmp: string;
afterEach(async () => {
  await cleanupPm(pm);
  pm = undefined;
  if (tmp) fs.rmSync(tmp, { recursive: true, force: true });
});

describe('U031 file mode — stdio wired straight to the fd (bypasses JS)', () => {
  it('spawn({fileMode:true}) leaves the parent with NO stdout/stderr pipe', () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    const id = pm.spawn('sleep 30', { cwd: tmp, fileMode: true });
    const internal = (pm as unknown as { processes: Map<string, { proc: { stdout: unknown; stderr: unknown } }> }).processes.get(id)!;
    assert.equal(internal.proc.stdout, null, 'file mode: stdout is the fd, not a JS pipe');
    assert.equal(internal.proc.stderr, null, 'file mode: stderr is the fd, not a JS pipe');
    pm.kill(id);
  });

  it('default (pipe) mode still exposes the JS stdout/stderr pipes', () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    const id = pm.spawn('sleep 30', { cwd: tmp });
    const internal = (pm as unknown as { processes: Map<string, { proc: { stdout: unknown; stderr: unknown } }> }).processes.get(id)!;
    assert.notEqual(internal.proc.stdout, null, 'pipe mode: stdout stream is present');
    assert.notEqual(internal.proc.stderr, null, 'pipe mode: stderr stream is present');
    pm.kill(id);
  });

  it('routes both stdout and stderr into the merged output file', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    const id = pm.spawn('echo to-stdout; echo to-stderr 1>&2', { cwd: tmp, fileMode: true });
    await waitFor(() => pm!.getStatus(id) !== 'running');
    const merged = pm.getOutput(id, 100)!.output;
    assert.match(merged, /to-stdout/, 'stdout reached the file');
    assert.match(merged, /to-stderr/, 'stderr is interleaved into the merged file');
  });

  it('getStderr returns "" in file mode (stderr interleaved, not separately captured)', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    const id = pm.spawn('echo out; echo err 1>&2', { cwd: tmp, fileMode: true });
    await waitFor(() => pm!.getStatus(id) !== 'running');
    assert.equal(pm.getStderr(id), '', 'no separable stderr tail in file mode (parity with the reference getStderr()="")');
  });

  it('getOutputSize reflects the on-disk size while bytesWritten stays 0 (output bypassed JS)', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    const id = pm.spawn("printf 'abcdefghij'; sleep 30", { cwd: tmp, fileMode: true });
    await waitFor(() => pm!.getOutputSize(id) >= 10);
    assert.ok(pm.getOutputSize(id) >= 10, 'size is polled from the file');
    const internal = (pm as unknown as { processes: Map<string, { bytesWritten: number }> }).processes.get(id)!;
    assert.equal(internal.bytesWritten, 0, 'writeChunk never ran, so the JS byte counter is 0');
    pm.kill(id);
  });
});

describe('U031 file mode — poll-driven runaway byte-cap', () => {
  it('kills a file-mode task once the on-disk output reaches the cap', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    pm.maxOutputBytes = 10;
    const id = pm.spawn("printf 'aaaaaaaaaaaaaaaaaaaa'; sleep 30", { cwd: tmp, fileMode: true });
    await waitFor(() => pm!.getOutputSize(id) >= 10);
    const killed = (pm as unknown as { enforceFileModeOutputCap: (id: string) => boolean }).enforceFileModeOutputCap(id);
    assert.equal(killed, true, 'the poll killed the over-cap task');
    assert.equal(pm.getStatus(id), 'killed');
    const merged = pm.getOutput(id, 100)!.output;
    assert.match(merged, /output exceeded 10B cap — task killed/, 'the cap diagnostic was written to the file');
  });

  it('the cap poll is a no-op for a pipe-mode / terminal / unknown task', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    const force = (pm as unknown as { enforceFileModeOutputCap: (id: string) => boolean }).enforceFileModeOutputCap.bind(pm);
    assert.equal(force('nope'), false, 'unknown id');
    const pipe = pm.spawn('sleep 30', { cwd: tmp });
    assert.equal(force(pipe), false, 'pipe-mode task is not subject to the file-mode cap poll');
    pm.kill(pipe);
    assert.equal(force(pipe), false, 'terminal task');
  });
});

describe('U031 file mode — bash tool wiring (AMI_BASH_OUTPUT_FILE_MODE)', () => {
  it('isBashOutputFileModeEnabled parses truthy/falsy env values', () => {
    for (const v of ['1', 'true', 'YES', 'on', ' On ']) {
      assert.equal(isBashOutputFileModeEnabled({ [BASH_OUTPUT_FILE_MODE_ENV]: v } as NodeJS.ProcessEnv), true, v);
    }
    for (const v of ['', '0', 'false', 'no', 'off', 'nope']) {
      assert.equal(isBashOutputFileModeEnabled({ [BASH_OUTPUT_FILE_MODE_ENV]: v } as NodeJS.ProcessEnv), false, v);
    }
    assert.equal(isBashOutputFileModeEnabled({} as NodeJS.ProcessEnv), false, 'unset');
  });

  it('threads fileMode:true into spawn ONLY when the env gate is on', async () => {
    let opts: Record<string, unknown> | undefined;
    const pmFake = {
      spawn: (_cmd: string, o: Record<string, unknown>) => { opts = o; return 'task-x'; },
      get: () => ({ pid: 1, outputPath: '/tmp/task-x.output' }),
    };
    const ctx = { cwd: process.cwd(), abortSignal: new AbortController().signal, processManager: pmFake } as unknown as ToolContext;

    const orig = process.env[BASH_OUTPUT_FILE_MODE_ENV];
    try {
      delete process.env[BASH_OUTPUT_FILE_MODE_ENV];
      await bashTool.execute({ command: 'npm run build', run_in_background: true }, ctx);
      assert.equal('fileMode' in (opts ?? {}), false, 'no fileMode key when the gate is off');

      process.env[BASH_OUTPUT_FILE_MODE_ENV] = '1';
      await bashTool.execute({ command: 'npm run build', run_in_background: true }, ctx);
      assert.equal(opts?.fileMode, true, 'fileMode threaded when the gate is on');
    } finally {
      if (orig === undefined) delete process.env[BASH_OUTPUT_FILE_MODE_ENV];
      else process.env[BASH_OUTPUT_FILE_MODE_ENV] = orig;
    }
  });

  it('E2E: a real background bash run in file mode writes its output to the fd-backed file', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    const ctx = { cwd: tmp, abortSignal: new AbortController().signal, processManager: pm } as unknown as ToolContext;
    const orig = process.env[BASH_OUTPUT_FILE_MODE_ENV];
    let taskId = '';
    try {
      process.env[BASH_OUTPUT_FILE_MODE_ENV] = '1';
      const res = await bashTool.execute({ command: 'echo file-mode-e2e', run_in_background: true }, ctx);
      assert.equal(res.isError, false);
      const m = res.output.match(/Task ID: (\S+)/);
      assert.ok(m, 'a task id was returned');
      taskId = m![1];
    } finally {
      if (orig === undefined) delete process.env[BASH_OUTPUT_FILE_MODE_ENV];
      else process.env[BASH_OUTPUT_FILE_MODE_ENV] = orig;
    }
    await waitFor(() => pm!.getStatus(taskId) !== 'running');
    // The task really ran in file mode (no JS stderr tail) and its output is on disk.
    const internal = (pm as unknown as { processes: Map<string, { _fileMode?: boolean }> }).processes.get(taskId)!;
    assert.equal(internal._fileMode, true, 'the real bash run used file mode');
    assert.match(pm.getOutput(taskId, 100)!.output, /file-mode-e2e/, 'output reached the fd-backed file');
  });
});
