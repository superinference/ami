import { describe, it, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { taskKillTool, GRACEFUL_SHUTDOWN_WAIT_MS } from '../src/tools/task-kill';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U031 / w79: `graceful` option on task_kill — cooperative graceful shutdown of
// a running in-process agent (requestShutdown + event-driven waitForIdle), as a
// third tier between soft-interrupt and hard-kill. Parity with Claude Code's
// requestTeammateShutdown + leader-wait on onIdleCallbacks.
// ---------------------------------------------------------------------------

const tmpDirs: string[] = [];
function makeTmpDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-tk-graceful-'));
  tmpDirs.push(d);
  return d;
}
function makePm(): ProcessManager { return new ProcessManager(makeTmpDir()); }
function registerAgent(pm: ProcessManager, id: string): void {
  pm.registerAgent(id, {
    command: `[agent] ${id}`,
    description: id,
    outputPath: path.join(makeTmpDir(), `${id}.out`),
    abortFn: () => { /* no-op */ },
  });
}
after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best-effort */ } }
});

describe('task_kill graceful shutdown', () => {
  it('rejects a non-boolean graceful', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    const ctx = { cwd: '/tmp', processManager: pm } as unknown as ToolContext;
    const r = await taskKillTool.execute!({ task_id: 'a1', graceful: 'yes' }, ctx);
    assert.ok(r.isError);
    assert.equal((r.metadata as any).reason, 'invalid_input');
  });

  it('rejects graceful + interrupt together (mutually exclusive)', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    const ctx = { cwd: '/tmp', processManager: pm } as unknown as ToolContext;
    const r = await taskKillTool.execute!({ task_id: 'a1', graceful: true, interrupt: true }, ctx);
    assert.ok(r.isError);
    assert.equal((r.metadata as any).reason, 'invalid_input');
    assert.ok(r.output.includes('mutually exclusive'));
  });

  it('refuses graceful on a bash task (no turn concept)', async () => {
    const pm = makePm();
    const bashId = pm.spawn('echo hi', { cwd: makeTmpDir() });
    const ctx = { cwd: '/tmp', processManager: pm } as unknown as ToolContext;
    const r = await taskKillTool.execute!({ task_id: bashId, graceful: true }, ctx);
    assert.ok(r.isError);
    assert.equal((r.metadata as any).reason, 'shutdown_unsupported');
    pm.cleanup();
  });

  it('requests shutdown and reports the terminal status once the agent goes idle', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    const ctx = { cwd: '/tmp', processManager: pm } as unknown as ToolContext;
    const pending = taskKillTool.execute!({ task_id: 'a1', graceful: true }, ctx);
    // The flag is set synchronously before the wait begins.
    assert.equal(pm.isShutdownRequested('a1'), true);
    // The agent finishes its turn and goes terminal → the wait resolves.
    setTimeout(() => pm.updateEntry('a1', { status: 'completed' }), 10);
    const r = await pending;
    assert.ok(!r.isError);
    assert.equal((r.metadata as any).reason, 'shutdown_requested');
    assert.equal((r.metadata as any).taskType, 'agent');
    assert.equal((r.metadata as any).status, 'completed');
    assert.ok(r.output.includes('gracefully shut down'));
  });

  it('reports "will stop after its current turn" when the agent is still running after the wait', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    assert.ok(GRACEFUL_SHUTDOWN_WAIT_MS > 0);
    // Stub whose waitForIdle resolves promptly but the agent is still running
    // afterwards (e.g. a long turn that outlasted the bounded wait), exercising the
    // "will stop after its current turn" message branch.
    const stub = {
      get: (id: string) => pm.get(id),
      taskKind: () => 'agent' as const,
      requestShutdown: (id: string) => pm.requestShutdown(id),
      waitForIdle: () => Promise.resolve(),
      getStatus: () => 'running' as const,
    };
    const ctx = { cwd: '/tmp', processManager: stub } as unknown as ToolContext;
    const r = await taskKillTool.execute!({ task_id: 'a1', graceful: true }, ctx);
    assert.ok(!r.isError);
    assert.equal((r.metadata as any).reason, 'shutdown_requested');
    assert.ok(r.output.includes('will stop after its current turn'));
  });

  it('fails cleanly when requestShutdown is refused by the manager', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    const stub = {
      get: (id: string) => pm.get(id),
      taskKind: () => 'agent' as const,
      requestShutdown: () => false,
    };
    const ctx = { cwd: '/tmp', processManager: stub } as unknown as ToolContext;
    const r = await taskKillTool.execute!({ task_id: 'a1', graceful: true }, ctx);
    assert.ok(r.isError);
    assert.equal((r.metadata as any).reason, 'kill_failed');
  });
});
