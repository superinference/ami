import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { taskKillTool, type TaskKillMetadata } from '../src/tools/task-kill';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext } from '../src/types';

/**
 * PARITY structured-result tests for task_kill / TaskStop. Each targets an
 * additive parity gap and is RED-GREEN validated:
 *
 *  - isConcurrencySafe: true (TaskStop declared concurrency-safe)
 *  - machine-readable `metadata.reason` discriminator on every branch
 *  - structured `metadata.taskType` (agent/bash) + full `metadata.command`
 *  - width/line-bounded command preview in the human-readable output
 */

function mockCtx(pm: Partial<ProcessManager>): ToolContext {
  return { cwd: '/tmp', processManager: pm as ProcessManager } as unknown as ToolContext;
}

function md(r: { metadata?: Record<string, unknown> }): TaskKillMetadata {
  assert.ok(r.metadata, 'result must carry structured metadata');
  return r.metadata as unknown as TaskKillMetadata;
}

describe('taskKillTool — concurrency-safety (parity)', () => {
  it('declares itself concurrency-safe', () => {
    // RED on revert: isConcurrencySafe is undefined before the change.
    assert.equal(taskKillTool.isConcurrencySafe, true);
  });
});

describe('taskKillTool — structured metadata reasons (parity)', () => {
  it('invalid_input on unknown keys', async () => {
    const ctx = mockCtx({ get: () => null as any, kill: () => false });
    const r = await taskKillTool.execute!({ task_id: 'x', bogus: 1 }, ctx);
    assert.equal(md(r).reason, 'invalid_input');
  });

  it('invalid_input on empty task_id', async () => {
    const ctx = mockCtx({ get: () => null as any, kill: () => false });
    const r = await taskKillTool.execute!({ task_id: '' }, ctx);
    assert.equal(md(r).reason, 'invalid_input');
  });

  it('invalid_input on non-boolean cleanup carries the taskId', async () => {
    const ctx = mockCtx({ get: () => ({ status: 'running', pid: 1 } as any), kill: () => true });
    const r = await taskKillTool.execute!({ task_id: 'x', cleanup: 'yes' as unknown as boolean }, ctx);
    assert.equal(md(r).reason, 'invalid_input');
    assert.equal(md(r).taskId, 'x');
  });

  it('manager_unavailable when no process manager', async () => {
    const r = await taskKillTool.execute!({ task_id: 'x' }, { cwd: '/tmp' } as ToolContext);
    assert.equal(md(r).reason, 'manager_unavailable');
    assert.match(r.output, /\[code: manager_unavailable\]/);
  });

  it('not_found for an unknown id, with the id echoed in metadata', async () => {
    const ctx = mockCtx({ get: () => null as any, kill: () => false });
    const r = await taskKillTool.execute!({ task_id: 'nope' }, ctx);
    assert.equal(md(r).reason, 'not_found');
    assert.equal(md(r).taskId, 'nope');
  });

  it('killed: success carries reason, command and pid', async () => {
    const ctx = mockCtx({
      get: () => ({ status: 'running', pid: 4242, command: 'sleep 999' } as any),
      kill: () => true,
    });
    const r = await taskKillTool.execute!({ task_id: 't' }, ctx);
    assert.ok(!r.isError);
    const m = md(r);
    assert.equal(m.reason, 'killed');
    assert.equal(m.command, 'sleep 999');
    assert.equal(m.pid, 4242);
  });

  it('already_terminal: idempotent stop carries status + exitCode', async () => {
    const ctx = mockCtx({
      get: () => ({ status: 'completed', exitCode: 0, pid: 7, command: 'build' } as any),
      kill: () => false,
    });
    const r = await taskKillTool.execute!({ task_id: 't' }, ctx);
    assert.ok(!r.isError);
    const m = md(r);
    assert.equal(m.reason, 'already_terminal');
    assert.equal(m.status, 'completed');
    assert.equal(m.exitCode, 0);
    assert.equal(m.command, 'build');
  });

  it('already_terminal on the race-safe path (kill→false but now terminal)', async () => {
    const ctx = mockCtx({
      get: () => ({ status: 'running', pid: 100, command: 'sleep 999' } as any),
      kill: () => false,
      getStatus: () => 'completed' as any,
    });
    const r = await taskKillTool.execute!({ task_id: 'raced' }, ctx);
    assert.ok(!r.isError);
    const m = md(r);
    assert.equal(m.reason, 'already_terminal');
    assert.equal(m.status, 'completed');
  });

  it('kill_failed when the task refuses to die', async () => {
    const ctx = mockCtx({
      get: () => ({ status: 'running', pid: 100, command: 'stuck' } as any),
      kill: () => false,
      getStatus: () => 'running' as any,
    });
    const r = await taskKillTool.execute!({ task_id: 'stuck' }, ctx);
    assert.ok(r.isError);
    const m = md(r);
    assert.equal(m.reason, 'kill_failed');
    assert.equal(m.command, 'stuck');
  });
});

describe('taskKillTool — bounded command preview (parity)', () => {
  it('truncates a huge multi-line command in the human output but keeps the full command in metadata', async () => {
    const huge = 'line1\nline2\nline3\n' + 'z'.repeat(400);
    const ctx = mockCtx({
      get: () => ({ status: 'running', pid: 1, command: huge } as any),
      kill: () => true,
    });
    const r = await taskKillTool.execute!({ task_id: 't' }, ctx);
    assert.ok(!r.isError);
    // Output preview is bounded and ends with an ellipsis marker.
    assert.ok(r.output.includes('line1\nline2'));
    assert.ok(!r.output.includes('line3'));
    assert.match(r.output, /…\) killed successfully/);
    // Full command preserved for programmatic consumers.
    assert.equal(md(r).command, huge);
  });

  it('a short command has no ellipsis marker', async () => {
    const ctx = mockCtx({
      get: () => ({ status: 'running', pid: 1, command: 'echo hi' } as any),
      kill: () => true,
    });
    const r = await taskKillTool.execute!({ task_id: 't' }, ctx);
    assert.match(r.output, /\(echo hi\) killed successfully/);
  });
});

describe('taskKillTool + ProcessManager.taskType (integration, real manager)', () => {
  let dataDir: string;
  let prevEnv: string | undefined;
  let pm: ProcessManager;

  before(() => {
    prevEnv = process.env.SUPERINFERENCE_DATA_DIR;
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'task-kill-structured-'));
    process.env.SUPERINFERENCE_DATA_DIR = dataDir;
    pm = new ProcessManager(dataDir);
  });

  after(() => {
    try { pm.cleanup(); } catch { /* best-effort */ }
    if (prevEnv === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
    else process.env.SUPERINFERENCE_DATA_DIR = prevEnv;
    try { fs.rmSync(dataDir, { recursive: true, force: true }); } catch { /* best-effort */ }
  });

  it('taskType() returns undefined for an unknown id', () => {
    assert.equal(pm.taskType('does-not-exist'), undefined);
  });

  it('a spawned bash task is classified as bash and surfaced in kill metadata', async () => {
    const taskId = pm.spawn('sleep 30', { cwd: dataDir });
    assert.equal(pm.taskType(taskId), 'bash');
    const ctx = { cwd: dataDir, processManager: pm } as unknown as ToolContext;
    const r = await taskKillTool.execute!({ task_id: taskId }, ctx);
    assert.ok(!r.isError, r.output);
    assert.equal(md(r).taskType, 'bash');
    assert.equal(md(r).reason, 'killed');
  });

  it('a registered agent is classified as agent and surfaced in kill metadata', async () => {
    let aborted = false;
    const taskId = 'agent-1';
    pm.registerAgent(taskId, {
      command: 'research task',
      description: 'deep research',
      outputPath: path.join(dataDir, 'agent-1.out'),
      abortFn: () => { aborted = true; },
    });
    assert.equal(pm.taskType(taskId), 'agent');
    const ctx = { cwd: dataDir, processManager: pm } as unknown as ToolContext;
    const r = await taskKillTool.execute!({ task_id: taskId }, ctx);
    assert.ok(!r.isError, r.output);
    assert.ok(aborted, 'registered agent kill must invoke the abort callback');
    assert.equal(md(r).taskType, 'agent');
  });
});
