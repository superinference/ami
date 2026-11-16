import { describe, it, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  ProcessManager,
  TASK_KINDS,
  type KilledTaskEvent,
  type TaskKind,
} from '../src/process-manager';
import { shellHooks } from '../src/utils/shell';
import { taskKillTool, type TaskKillMetadata } from '../src/tools/task-kill';
import type { ToolContext } from '../src/types';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'pm-w67-'));
}

async function cleanupPm(pm: ProcessManager | undefined): Promise<void> {
  if (!pm) return;
  try { pm.cleanup(); } catch { /* ignore */ }
  await new Promise((r) => setTimeout(r, 30));
}

function waitFor(cond: () => boolean, timeoutMs = 5000): Promise<void> {
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

// ---------------------------------------------------------------------------
// GAP: Robust process-TREE kill (parity with Claude Code's ShellCommand #doKill
// → treeKill). ProcessManager.kill() previously signalled ONLY the child's
// process GROUP (`process.kill(-pid)`), so a descendant that escaped the group
// via setsid()/double-fork survived the stop. It must now reuse the shared
// killTree() helper, which signals every descendant pid individually AND sweeps
// the group — while never touching an unrelated pid.
// ---------------------------------------------------------------------------
describe('ProcessManager.kill – robust process-tree kill', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('kills every descendant pid (setsid escapee), not just the group — RED without tree-kill', async (t) => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const taskId = pm.spawn('sleep 5', { cwd: tmpDir });
    const rootPid = pm.get(taskId)!.pid;
    // A descendant pid that has "escaped" the process group — it will NOT be
    // reached by a bare `process.kill(-rootPid)` group signal. Use a value that
    // does not exist so the delegated real kill is a harmless ESRCH no-op.
    const escapee = 2_000_000_000;

    t.mock.method(shellHooks, 'platform', () => 'linux' as NodeJS.Platform);
    t.mock.method(
      shellHooks,
      'execSync',
      (() => `  ${rootPid}     1\n  ${escapee}  ${rootPid}\n  999999999     1\n`) as typeof shellHooks.execSync,
    );
    const killed: number[] = [];
    t.mock.method(shellHooks, 'kill', ((pid: number, sig: NodeJS.Signals) => {
      killed.push(pid);
      try { process.kill(pid, sig); } catch { /* fake/escapee pids ESRCH */ }
    }) as typeof shellHooks.kill);

    const ok = pm.kill(taskId, { suppressCompletion: true });
    assert.equal(ok, true);
    // The whole tree is signalled individually, the group is swept, and the
    // unrelated pid is spared.
    assert.ok(killed.includes(rootPid), 'root pid killed');
    assert.ok(killed.includes(escapee), 'setsid escapee killed (tree-kill) — group-only kill would miss this');
    assert.ok(killed.includes(-rootPid), 'process group swept');
    assert.ok(!killed.includes(999999999), 'unrelated pid untouched');
  });
});

// ---------------------------------------------------------------------------
// GAP: task-KIND discriminator (parity with Claude's BackgroundTaskState `type`
// union + LocalShellTask `kind`). Every tracked entry carries a `kind`
// (bash | monitor | agent) so consumers branch on it instead of brittle
// command-string heuristics, and taskType() no longer MISCLASSIFIES a monitor
// as 'bash'.
// ---------------------------------------------------------------------------
describe('ProcessManager – task-kind discriminator', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('TASK_KINDS enumerates the three kinds', () => {
    assert.deepEqual([...TASK_KINDS], ['bash', 'monitor', 'agent']);
  });

  it('spawn → kind "bash"; list()/get() surface it', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const taskId = pm.spawn('echo hi', { cwd: tmpDir });
    assert.equal(pm.taskKind(taskId), 'bash');
    assert.equal(pm.taskType(taskId), 'bash');
    assert.equal(pm.get(taskId)!.kind, 'bash');
    assert.equal(pm.list().find((t) => t.taskId === taskId)!.kind, 'bash');
  });

  it('registerAgent → kind "agent"', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.registerAgent('agent-k', {
      command: '[agent] worker',
      description: 'worker',
      outputPath: path.join(tmpDir, 'a.out'),
      abortFn: () => {},
    });
    assert.equal(pm.taskKind('agent-k'), 'agent');
    assert.equal(pm.taskType('agent-k'), 'agent');
    assert.equal(pm.get('agent-k')!.kind, 'agent');
  });

  it('monitorMcp → kind "monitor"; taskType() returns undefined (NOT misclassified as bash)', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const taskId = pm.monitorMcp('srv', async () => true, 30_000);
    assert.equal(pm.taskKind(taskId), 'monitor');
    // RED without the fix: taskType ran the non-registered branch → 'bash'.
    assert.equal(pm.taskType(taskId), undefined, 'a monitor has no agent/bash task_type');
  });

  it('taskKind / taskType are undefined for an untracked id', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    assert.equal(pm.taskKind('nope'), undefined);
    assert.equal(pm.taskType('nope'), undefined);
  });
});

// ---------------------------------------------------------------------------
// GAP: reason-bearing kill (parity with Claude's killShellTasksForAgent debug
// line). The emitted 'killed' event carries WHY the task was reaped, and
// killTasksForAgent supplies `agent <id> exiting`. The taskType on the event is
// derived from the authoritative kind (a monitor → undefined).
// ---------------------------------------------------------------------------
describe('ProcessManager – reason-bearing killed event', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('killTasksForAgent emits a killed event with reason "agent <id> exiting"', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const events: KilledTaskEvent[] = [];
    pm.on('killed', (e: KilledTaskEvent) => events.push(e));
    const taskId = pm.spawn('sleep 5', { cwd: tmpDir, agentId: 'A1' });
    await waitFor(() => pm.getStatus(taskId) === 'running');

    const n = pm.killTasksForAgent('A1');
    assert.equal(n, 1);
    const ev = events.find((e) => e.taskId === taskId);
    assert.ok(ev, 'killed event emitted');
    assert.equal(ev!.reason, 'agent A1 exiting');
    assert.equal(ev!.taskType, 'bash');
    // Internal reap (no suppressCompletion) → not suppressed, so completion still surfaces.
    assert.equal(ev!.suppressed, false);
  });

  it('a plain kill carries no reason; an explicit reason is passed through', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const events: KilledTaskEvent[] = [];
    pm.on('killed', (e: KilledTaskEvent) => events.push(e));
    pm.registerAgent('agent-r', {
      command: '[agent] r',
      description: 'r',
      outputPath: path.join(tmpDir, 'r.out'),
      abortFn: () => {},
    });
    pm.kill('agent-r');
    assert.equal(events[0].reason, undefined);

    pm.registerAgent('agent-r2', {
      command: '[agent] r2',
      description: 'r2',
      outputPath: path.join(tmpDir, 'r2.out'),
      abortFn: () => {},
    });
    pm.kill('agent-r2', { reason: 'custom cause' });
    assert.equal(events[1].reason, 'custom cause');
  });

  it('a killed MONITOR event reports taskType undefined (kind-derived)', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const events: KilledTaskEvent[] = [];
    pm.on('killed', (e: KilledTaskEvent) => events.push(e));
    const taskId = pm.monitorMcp('srv', async () => true, 30_000);
    pm.kill(taskId);
    const ev = events.find((e) => e.taskId === taskId)!;
    assert.equal(ev.taskType, undefined, 'monitor kill → no agent/bash task_type');
  });
});

// ---------------------------------------------------------------------------
// FUNCTIONAL (task_kill tool): the structured metadata now carries the full
// task `kind` (bash | monitor | agent) alongside the agent/bash `taskType`.
// ---------------------------------------------------------------------------
describe('taskKillTool – kind in structured metadata', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  function md(r: { metadata?: Record<string, unknown> }): TaskKillMetadata {
    assert.ok(r.metadata, 'result must carry metadata');
    return r.metadata as unknown as TaskKillMetadata;
  }

  it('surfaces kind:"agent" when stopping a registered agent', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.registerAgent('agent-x', {
      command: '[agent] x',
      description: 'x',
      outputPath: path.join(tmpDir, 'x.out'),
      abortFn: () => {},
    });
    const ctx = { cwd: tmpDir, processManager: pm } as unknown as ToolContext;
    const r = await taskKillTool.execute!({ task_id: 'agent-x' }, ctx);
    assert.equal(r.isError, false);
    const kind: TaskKind | undefined = md(r).kind;
    assert.equal(kind, 'agent');
    assert.equal(md(r).taskType, 'agent');
  });

  it('surfaces kind:"bash" when stopping a background shell', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const taskId = pm.spawn('sleep 5', { cwd: tmpDir });
    await waitFor(() => pm.getStatus(taskId) === 'running');
    const ctx = { cwd: tmpDir, processManager: pm } as unknown as ToolContext;
    const r = await taskKillTool.execute!({ task_id: taskId }, ctx);
    assert.equal(r.isError, false);
    assert.equal(md(r).kind, 'bash');
  });
});
