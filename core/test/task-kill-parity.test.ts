import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { taskKillTool } from '../src/tools/task-kill';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext } from '../src/types';

/**
 * Parity tests for task_kill / TaskStop. Each test targets a specific additive
 * parity gap and is RED-GREEN validated against the pre-change tool:
 *
 *  - argument validation: empty/non-string task_id, non-boolean cleanup → [code: invalid_input]
 *  - idempotent already-terminal stop with no "exit code: null" noise
 *  - race-safe idempotency: kill()→false but status now terminal → success, not kill_failed
 *  - cleanup:true reaping (removeTask) with real + mock managers
 *
 * Mock tests pin the branch logic deterministically; real-child tests exercise the
 * genuine ProcessManager kill/cleanup path with short-lived children that are
 * always torn down.
 */

function mockCtx(pm: Partial<ProcessManager>): ToolContext {
  return { cwd: '/tmp', processManager: pm as ProcessManager } as unknown as ToolContext;
}

describe('taskKillTool — argument validation (parity)', () => {
  it('rejects an empty-string task_id with invalid_input', async () => {
    const ctx = mockCtx({ get: () => null as any, kill: () => false });
    const r = await taskKillTool.execute!({ task_id: '' }, ctx);
    assert.ok(r.isError);
    assert.match(r.output, /must be a non-empty string/);
    assert.match(r.output, /\[code: invalid_input\]/);
  });

  it('rejects a whitespace-only task_id with invalid_input', async () => {
    const ctx = mockCtx({ get: () => null as any, kill: () => false });
    const r = await taskKillTool.execute!({ task_id: '   ' }, ctx);
    assert.ok(r.isError);
    assert.match(r.output, /\[code: invalid_input\]/);
  });

  it('rejects a non-string task_id with invalid_input', async () => {
    const ctx = mockCtx({ get: () => null as any, kill: () => false });
    const r = await taskKillTool.execute!({ task_id: 123 as unknown as string }, ctx);
    assert.ok(r.isError);
    assert.match(r.output, /must be a non-empty string/);
  });

  it('rejects a non-boolean cleanup with invalid_input', async () => {
    const ctx = mockCtx({ get: () => ({ status: 'running', pid: 1 } as any), kill: () => true });
    const r = await taskKillTool.execute!({ task_id: 'x', cleanup: 'yes' as unknown as boolean }, ctx);
    assert.ok(r.isError);
    assert.match(r.output, /"cleanup" must be a boolean/);
    assert.match(r.output, /\[code: invalid_input\]/);
  });

  it('accepts cleanup as a real boolean', async () => {
    let removed = false;
    const ctx = mockCtx({
      get: () => ({ status: 'running', pid: 1 } as any),
      kill: () => true,
      removeTask: () => { removed = true; return true; },
    });
    const r = await taskKillTool.execute!({ task_id: 'x', cleanup: true }, ctx);
    assert.ok(!r.isError);
    assert.ok(removed);
  });

  it('still rejects unknown keys and lists cleanup as allowed', async () => {
    const ctx = mockCtx({ get: () => null as any, kill: () => false });
    const r = await taskKillTool.execute!({ task_id: 'x', bogus: 1 }, ctx);
    assert.ok(r.isError);
    assert.match(r.output, /unknown input key/);
    assert.match(r.output, /task_id, cleanup/);
  });
});

describe('taskKillTool — idempotent terminal handling (parity)', () => {
  it('reports an already-killed task without "exit code: null" noise', async () => {
    const ctx = mockCtx({
      get: () => ({ status: 'killed', exitCode: null, pid: 42 } as any),
      kill: () => false,
    });
    const r = await taskKillTool.execute!({ task_id: 't' }, ctx);
    assert.ok(!r.isError);
    assert.match(r.output, /already killed/);
    assert.doesNotMatch(r.output, /exit code: null/);
    assert.doesNotMatch(r.output, /exit code/);
  });

  it('still shows a known exit code for a completed task', async () => {
    const ctx = mockCtx({
      get: () => ({ status: 'completed', exitCode: 0, pid: 7 } as any),
      kill: () => false,
    });
    const r = await taskKillTool.execute!({ task_id: 't' }, ctx);
    assert.ok(!r.isError);
    assert.match(r.output, /already completed \(exit code: 0\)/);
  });

  it('treats undefined exitCode as unknown (no exit-code suffix)', async () => {
    const ctx = mockCtx({
      get: () => ({ status: 'failed', pid: 9 } as any), // exitCode undefined
      kill: () => false,
    });
    const r = await taskKillTool.execute!({ task_id: 't' }, ctx);
    assert.ok(!r.isError);
    assert.match(r.output, /already failed\./);
    assert.doesNotMatch(r.output, /exit code/);
  });
});

describe('taskKillTool — race-safe idempotency (parity)', () => {
  it('kill()→false but status now terminal is reported as success', async () => {
    const ctx = mockCtx({
      get: () => ({ status: 'running', pid: 100, command: 'sleep 999' } as any),
      kill: () => false,
      getStatus: () => 'completed' as any,
    });
    const r = await taskKillTool.execute!({ task_id: 'raced' }, ctx);
    assert.ok(!r.isError, 'a task that finished during the stop must not be an error');
    assert.match(r.output, /had already completed/);
    assert.match(r.output, /sleep 999/);
  });

  it('kill()→false and status still running is a genuine kill_failed', async () => {
    const ctx = mockCtx({
      get: () => ({ status: 'running', pid: 100 } as any),
      kill: () => false,
      getStatus: () => 'running' as any,
    });
    const r = await taskKillTool.execute!({ task_id: 'stuck' }, ctx);
    assert.ok(r.isError);
    assert.match(r.output, /\[code: kill_failed\]/);
  });

  it('kill()→false with no getStatus method falls back to kill_failed', async () => {
    const ctx = mockCtx({
      get: () => ({ status: 'running', pid: 100 } as any),
      kill: () => false,
    });
    const r = await taskKillTool.execute!({ task_id: 'stuck' }, ctx);
    assert.ok(r.isError);
    assert.match(r.output, /\[code: kill_failed\]/);
  });
});

describe('taskKillTool — cleanup reaping (parity, mock)', () => {
  it('cleanup:true after a successful kill reaps the task', async () => {
    const calls: string[] = [];
    const ctx = mockCtx({
      get: () => ({ status: 'running', pid: 1 } as any),
      kill: () => true,
      removeTask: (id: string) => { calls.push(id); return true; },
    });
    const r = await taskKillTool.execute!({ task_id: 'bg-1', cleanup: true }, ctx);
    assert.ok(!r.isError);
    assert.match(r.output, /killed successfully/);
    assert.match(r.output, /Task reaped/);
    assert.deepEqual(calls, ['bg-1']);
  });

  it('cleanup:true when removeTask returns false says cleanup skipped', async () => {
    const ctx = mockCtx({
      get: () => ({ status: 'running', pid: 1 } as any),
      kill: () => true,
      removeTask: () => false,
    });
    const r = await taskKillTool.execute!({ task_id: 'bg-2', cleanup: true }, ctx);
    assert.ok(!r.isError);
    assert.match(r.output, /cleanup skipped/);
  });

  it('cleanup:true with no removeTask method is a silent no-op', async () => {
    const ctx = mockCtx({
      get: () => ({ status: 'running', pid: 1 } as any),
      kill: () => true,
    });
    const r = await taskKillTool.execute!({ task_id: 'bg-3', cleanup: true }, ctx);
    assert.ok(!r.isError);
    assert.doesNotMatch(r.output, /reaped|cleanup skipped/);
  });

  it('cleanup default (omitted) does not reap', async () => {
    let called = false;
    const ctx = mockCtx({
      get: () => ({ status: 'running', pid: 1 } as any),
      kill: () => true,
      removeTask: () => { called = true; return true; },
    });
    const r = await taskKillTool.execute!({ task_id: 'bg-4' }, ctx);
    assert.ok(!r.isError);
    assert.ok(!called);
    assert.doesNotMatch(r.output, /reaped/);
  });

  it('cleanup:true also reaps an already-terminal task', async () => {
    const calls: string[] = [];
    const ctx = mockCtx({
      get: () => ({ status: 'killed', exitCode: null, pid: 1 } as any),
      kill: () => false,
      removeTask: (id: string) => { calls.push(id); return true; },
    });
    const r = await taskKillTool.execute!({ task_id: 'bg-5', cleanup: true }, ctx);
    assert.ok(!r.isError);
    assert.match(r.output, /already killed/);
    assert.match(r.output, /Task reaped/);
    assert.deepEqual(calls, ['bg-5']);
  });

  it('cleanup:true reaps on the race-safe terminal path too', async () => {
    const calls: string[] = [];
    const ctx = mockCtx({
      get: () => ({ status: 'running', pid: 1 } as any),
      kill: () => false,
      getStatus: () => 'completed' as any,
      removeTask: (id: string) => { calls.push(id); return true; },
    });
    const r = await taskKillTool.execute!({ task_id: 'bg-6', cleanup: true }, ctx);
    assert.ok(!r.isError);
    assert.match(r.output, /had already completed/);
    assert.match(r.output, /Task reaped/);
    assert.deepEqual(calls, ['bg-6']);
  });
});

describe('taskKillTool — real ProcessManager (parity, short-lived children)', () => {
  let dataDir: string;
  let prevEnv: string | undefined;
  let pm: ProcessManager;

  before(() => {
    prevEnv = process.env.SUPERINFERENCE_DATA_DIR;
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'task-kill-parity-'));
    process.env.SUPERINFERENCE_DATA_DIR = dataDir;
    pm = new ProcessManager(dataDir);
  });

  after(() => {
    try { pm.cleanup(); } catch { /* best-effort */ }
    if (prevEnv === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
    else process.env.SUPERINFERENCE_DATA_DIR = prevEnv;
    try { fs.rmSync(dataDir, { recursive: true, force: true }); } catch { /* best-effort */ }
  });

  it('kills a real long-running child and a second stop is idempotent (no exit-code noise)', async () => {
    const taskId = pm.spawn('sleep 30', { cwd: dataDir });
    const ctx = { cwd: dataDir, processManager: pm } as unknown as ToolContext;

    const first = await taskKillTool.execute!({ task_id: taskId }, ctx);
    assert.ok(!first.isError, first.output);
    assert.match(first.output, /killed successfully/);

    // Second stop: task is now terminal ('killed', exitCode null) — idempotent success.
    const second = await taskKillTool.execute!({ task_id: taskId }, ctx);
    assert.ok(!second.isError);
    assert.match(second.output, /already killed/);
    assert.doesNotMatch(second.output, /exit code: null/);
  });

  it('cleanup:true removes a killed real task from the table and deletes its output file', async () => {
    const taskId = pm.spawn('sleep 30', { cwd: dataDir });
    const outputPath = pm.get(taskId)!.outputPath;
    assert.ok(fs.existsSync(outputPath));
    const ctx = { cwd: dataDir, processManager: pm } as unknown as ToolContext;

    const r = await taskKillTool.execute!({ task_id: taskId, cleanup: true }, ctx);
    assert.ok(!r.isError, r.output);
    assert.match(r.output, /killed successfully/);
    assert.match(r.output, /Task reaped/);

    // Reaped: no longer tracked, output file gone.
    assert.equal(pm.get(taskId), null);
    assert.ok(!fs.existsSync(outputPath));
  });

  it('reports not_found for an unknown id against a real manager', async () => {
    const ctx = { cwd: dataDir, processManager: pm } as unknown as ToolContext;
    const r = await taskKillTool.execute!({ task_id: 'bg-doesnotexist' }, ctx);
    assert.ok(r.isError);
    assert.match(r.output, /\[code: not_found\]/);
  });
});
