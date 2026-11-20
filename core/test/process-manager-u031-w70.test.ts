import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ProcessManager, type KilledTaskEvent } from '../src/process-manager';
import {
  taskListTool,
  summarizeTaskKinds,
  formatKindSummary,
} from '../src/tools/task-list';
import type { ToolContext } from '../src/types';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-u031-w70-'));
}

function waitFor(pred: () => boolean, timeoutMs = 5000): Promise<void> {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const tick = () => {
      if (pred()) return resolve();
      if (Date.now() - start > timeoutMs) return reject(new Error('waitFor timed out'));
      setTimeout(tick, 15);
    };
    tick();
  });
}

function ctxFor(pm: ProcessManager): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    processManager: pm,
  } as unknown as ToolContext;
}

// ---------------------------------------------------------------------------
// Feature 1 — GAP: reason-bearing reap at session shutdown.
//   Claude's killShellTasksForAgent logs a context-rich reason for every task it
//   reaps ("killing orphaned shell task X (agent Y exiting)"). AMI's session-end
//   cleanup() previously killed every running task SILENTLY — the emitted 'killed'
//   event (consumed by engine.ts → analytics `background_task_killed`) carried no
//   reason, so a developer tracing analytics could not tell a session-end reap
//   from a user kill. cleanup() now passes an explicit 'session exiting' reason.
// ---------------------------------------------------------------------------

describe('ProcessManager.cleanup — reason-bearing reap (GAP: silent shutdown reap)', () => {
  let tmp: string | null = null;
  afterEach(() => {
    if (tmp) { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } tmp = null; }
  });

  it("emits a 'killed' event carrying reason 'session exiting' for each reaped running task", async () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    const events: KilledTaskEvent[] = [];
    pm.on('killed', (e: KilledTaskEvent) => events.push(e));

    const taskId = pm.spawn('sleep 60', { cwd: tmp, description: 'long sleeper' });
    await waitFor(() => pm.getStatus(taskId) === 'running');

    pm.cleanup();

    // cleanup() ran the reap; the killed event for the reaped task must carry the
    // shutdown reason (undefined before the fix → RED on revert).
    const reaped = events.find((e) => e.taskId === taskId);
    assert.ok(reaped, 'expected a killed event for the reaped task');
    assert.equal(reaped!.reason, 'session exiting');
    await new Promise((r) => setTimeout(r, 100));
  });

  it("does not synthesise a reason for an EXPLICIT task_kill (reason stays undefined)", async () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    const events: KilledTaskEvent[] = [];
    pm.on('killed', (e: KilledTaskEvent) => events.push(e));

    const taskId = pm.spawn('sleep 60', { cwd: tmp, description: 'sleeper' });
    await waitFor(() => pm.getStatus(taskId) === 'running');

    // A plain model/user stop carries no reason — the shutdown reason must be
    // scoped to cleanup(), not applied to every kill.
    pm.kill(taskId, { suppressCompletion: true });
    const killed = events.find((e) => e.taskId === taskId);
    assert.ok(killed);
    assert.equal(killed!.reason, undefined);
    pm.cleanup();
    await new Promise((r) => setTimeout(r, 100));
  });
});

// ---------------------------------------------------------------------------
// Feature 2 — GAP: per-type pill-label counts from the authoritative `kind`
//   discriminator (not brittle command-string heuristics). Mirrors the reference's
//   getPillLabel splitting "1 shell, 1 monitor". task_list now renders a "By
//   type:" line driven by the task `kind` field, and surfaces the count of running
//   internal MCP monitors (still hidden from the per-task listing).
// ---------------------------------------------------------------------------

describe('summarizeTaskKinds / formatKindSummary (GAP: per-type labels)', () => {
  it('counts tasks by their authoritative kind discriminator', () => {
    const counts = summarizeTaskKinds([
      { kind: 'bash' }, { kind: 'bash' }, { kind: 'agent' }, { kind: 'monitor' },
    ]);
    assert.deepEqual(counts, { bash: 2, monitor: 1, agent: 1 });
  });

  it('ignores entries whose kind is missing/unknown (old mock stubs)', () => {
    const counts = summarizeTaskKinds([
      { kind: undefined as any }, { kind: 'bash' },
    ]);
    assert.deepEqual(counts, { bash: 1, monitor: 0, agent: 0 });
  });

  it('pluralises each kind the Claude getPillLabel way', () => {
    assert.equal(formatKindSummary({ bash: 1, monitor: 0, agent: 0 }), '1 shell');
    assert.equal(formatKindSummary({ bash: 2, monitor: 0, agent: 0 }), '2 shells');
    assert.equal(formatKindSummary({ bash: 2, monitor: 1, agent: 3 }), '2 shells, 3 agents, 1 monitor');
    assert.equal(formatKindSummary({ bash: 0, monitor: 2, agent: 0 }), '2 monitors');
    assert.equal(formatKindSummary({ bash: 0, monitor: 0, agent: 0 }), '');
  });
});

describe('task_list — By-type summary line (real ProcessManager)', () => {
  let tmp: string | null = null;
  afterEach(() => {
    if (tmp) { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } tmp = null; }
  });

  it('renders a By-type line with shells and agents from the kind field', async () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    const shellId = pm.spawn('sleep 60', { cwd: tmp, description: 'a shell' });
    pm.registerAgent('bg-agent-1', {
      command: '[agent] builder',
      description: 'Background agent: builder',
      outputPath: path.join(tmp, 'bg-agent-1.output'),
      abortFn: () => { /* noop */ },
    });
    await waitFor(() => pm.getStatus(shellId) === 'running');

    const r = await taskListTool.execute({}, ctxFor(pm));
    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('By type:'), r.output);
    assert.ok(r.output.includes('1 shell'), r.output);
    assert.ok(r.output.includes('1 agent'), r.output);
    pm.cleanup();
    await new Promise((res) => setTimeout(res, 100));
  });

  it('surfaces the running-monitor count even though monitors are not listed', async () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    const shellId = pm.spawn('sleep 60', { cwd: tmp, description: 'a shell' });
    // A monitor that stays healthy forever (interval never fires in the test window).
    pm.monitorMcp('srv-xyz', async () => true, 60_000);
    await waitFor(() => pm.getStatus(shellId) === 'running');

    const r = await taskListTool.execute({}, ctxFor(pm));
    // Monitors are intentionally excluded from the per-task listing...
    assert.ok(!r.output.includes('MCP monitor: srv-xyz'), r.output);
    // ...but their running count is surfaced in the By-type summary.
    assert.ok(r.output.includes('By type:'), r.output);
    assert.ok(r.output.includes('1 monitor'), r.output);
    pm.cleanup();
    await new Promise((res) => setTimeout(res, 100));
  });
});
