import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  taskListTool,
  isAgentTask,
  isMonitorTask,
  agentDisplayName,
  formatTokenCount,
  formatElapsed,
  formatTaskStatus,
  summarizeTasks,
} from '../src/tools/task-list';
import type { ToolContext } from '../src/types';
import type { BackgroundProcess } from '../src/process-manager';

// --------------------------------------------------------------------------
// Deterministic rendering harness: the tool only reads processManager.list(),
// so a stub returning crafted BackgroundProcess arrays exercises every render
// path without racing a real child process.
// --------------------------------------------------------------------------
let idSeq = 0;
function mkTask(over: Partial<BackgroundProcess> = {}): BackgroundProcess {
  idSeq++;
  return {
    taskId: over.taskId ?? `bg-${idSeq.toString(16).padStart(8, '0')}`,
    pid: over.pid ?? 1000 + idSeq,
    command: over.command ?? 'sleep 60',
    description: over.description ?? 'a shell task',
    status: over.status ?? 'running',
    exitCode: over.exitCode ?? null,
    outputPath: over.outputPath ?? '/tmp/out',
    startTime: over.startTime ?? Date.now(),
    ...(over.tokenCount !== undefined ? { tokenCount: over.tokenCount } : {}),
  };
}

function ctx(tasks: BackgroundProcess[] | null): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    processManager: tasks === null ? undefined : ({ list: () => tasks } as any),
  } as ToolContext;
}

describe('task_list parity — backward compatibility (regression guards)', () => {
  it('errors when processManager is unavailable', async () => {
    const r = await taskListTool.execute({}, ctx(null));
    assert.ok(r.isError);
    assert.ok(r.output.includes('not available'));
  });

  it('reports empty state', async () => {
    const r = await taskListTool.execute({}, ctx([]));
    assert.ok(!r.isError);
    assert.ok(r.output.includes('No background tasks.'));
  });

  it('keeps the "Background tasks (N)" header and per-task fields', async () => {
    const t = mkTask({ description: 'Long sleep', status: 'running' });
    const r = await taskListTool.execute({}, ctx([t]));
    assert.ok(r.output.includes('Background tasks (1)'));
    assert.ok(r.output.includes(t.taskId));
    assert.ok(r.output.includes('running'));
    assert.ok(r.output.includes('Long sleep'));
  });
});

describe('task_list parity — additive supersets (RED without the change)', () => {
  // Gap: at-a-glance status breakdown. Old output had no summary line.
  it('includes a status summary line', async () => {
    const tasks = [
      mkTask({ status: 'running' }),
      mkTask({ status: 'completed', exitCode: 0 }),
      mkTask({ status: 'failed', exitCode: 2 }),
    ];
    const r = await taskListTool.execute({}, ctx(tasks));
    assert.ok(r.output.includes('1 running'), r.output);
    assert.ok(r.output.includes('1 completed'), r.output);
    assert.ok(r.output.includes('1 failed'), r.output);
  });

  // Gap: subagents are rendered with an addressable name, not just a description.
  it('renders agent tasks with an addressable name', async () => {
    const agent = mkTask({ command: '[agent] security-review', description: 'Background agent: security-review' });
    const r = await taskListTool.execute({}, ctx([agent]));
    assert.ok(r.output.includes('agent "security-review"'), r.output);
  });

  // Gap: token usage for background agents was tracked but never displayed.
  it('shows token usage for agents', async () => {
    const agent = mkTask({ command: '[agent] builder', tokenCount: 1500 });
    const r = await taskListTool.execute({}, ctx([agent]));
    assert.ok(r.output.includes('1.5k tok'), r.output);
  });

  // Gap: completed/failed with unknown exit rendered meaningless "(exit null)".
  it('does not print "(exit null)" for a completed task with unknown exit', async () => {
    const t = mkTask({ status: 'completed', exitCode: null, description: 'done' });
    const r = await taskListTool.execute({}, ctx([t]));
    assert.ok(!r.output.includes('exit null'), r.output);
    assert.ok(r.output.includes('✓ completed'), r.output);
  });

  // Gap: status filter input was ignored (empty schema).
  it('filters by status and annotates the header with "of"', async () => {
    const tasks = [
      mkTask({ status: 'running', description: 'alive' }),
      mkTask({ status: 'completed', exitCode: 0, description: 'done' }),
    ];
    const r = await taskListTool.execute({ status: 'running' }, ctx(tasks));
    assert.ok(r.output.includes(tasks[0].taskId), r.output);
    assert.ok(!r.output.includes(tasks[1].taskId), r.output);
    assert.ok(r.output.includes('(1 of 2)'), r.output);
  });

  // Gap: type filter input was ignored.
  it('filters by type=agent', async () => {
    const agent = mkTask({ command: '[agent] reviewer' });
    const shell = mkTask({ command: 'npm test', description: 'tests' });
    const r = await taskListTool.execute({ type: 'agent' }, ctx([agent, shell]));
    assert.ok(r.output.includes(agent.taskId), r.output);
    assert.ok(!r.output.includes(shell.taskId), r.output);
  });

  it('filters by type=task (excludes agents)', async () => {
    const agent = mkTask({ command: '[agent] reviewer' });
    const shell = mkTask({ command: 'npm test', description: 'tests' });
    const r = await taskListTool.execute({ type: 'task' }, ctx([agent, shell]));
    assert.ok(!r.output.includes(agent.taskId), r.output);
    assert.ok(r.output.includes(shell.taskId), r.output);
  });

  // Gap: internal MCP monitors leaked into the user-facing listing.
  it('excludes internal MCP monitor entries', async () => {
    const monitor = mkTask({ taskId: 'monitor-srv-1', command: 'monitor:srv', description: 'MCP monitor: srv', pid: 0 });
    const real = mkTask({ description: 'real work' });
    const r = await taskListTool.execute({}, ctx([monitor, real]));
    assert.ok(!r.output.includes('monitor-srv-1'), r.output);
    assert.ok(!r.output.includes('MCP monitor'), r.output);
    assert.ok(r.output.includes(real.taskId), r.output);
    assert.ok(r.output.includes('Background tasks (1)'), r.output);
  });

  // Gap: insertion order surfaced finished work above live work.
  it('sorts running tasks ahead of terminal ones', async () => {
    const done = mkTask({ status: 'completed', exitCode: 0, startTime: 2000, description: 'finished' });
    const live = mkTask({ status: 'running', startTime: 1000, description: 'still going' });
    const r = await taskListTool.execute({}, ctx([done, live]));
    assert.ok(r.output.indexOf(live.taskId) < r.output.indexOf(done.taskId), r.output);
  });

  // Gap: duplicate agent names are ambiguous to address — flag them.
  it('flags ambiguous duplicate agent names', async () => {
    const a = mkTask({ command: '[agent] build' });
    const b = mkTask({ command: '[agent] build' });
    const r = await taskListTool.execute({}, ctx([a, b]));
    assert.ok(r.output.includes('ambiguous'), r.output);
    assert.ok(r.output.includes(a.taskId) && r.output.includes(b.taskId), r.output);
  });
});

describe('task_list parity — filter edge cases', () => {
  it('reports when a filter matches nothing', async () => {
    const r = await taskListTool.execute({ status: 'killed' }, ctx([mkTask({ status: 'running' })]));
    assert.ok(!r.isError);
    assert.ok(r.output.includes('No background tasks match filter (status=killed)'), r.output);
  });

  it('combines status and type in the no-match message', async () => {
    const r = await taskListTool.execute({ status: 'killed', type: 'agent' }, ctx([mkTask({ status: 'running' })]));
    assert.ok(r.output.includes('status=killed, type=agent'), r.output);
  });

  it('empty after excluding monitors reports base empty state', async () => {
    const monitor = mkTask({ command: 'monitor:only', description: 'MCP monitor: only' });
    const r = await taskListTool.execute({}, ctx([monitor]));
    assert.ok(r.output.includes('No background tasks.'), r.output);
  });

  it('agents with zero/undefined tokenCount omit the token tag', async () => {
    const zero = mkTask({ command: '[agent] a', tokenCount: 0 });
    const none = mkTask({ command: '[agent] b' });
    const r = await taskListTool.execute({ type: 'agent' }, ctx([zero, none]));
    assert.ok(!r.output.includes('tok'), r.output);
  });

  it('truncates a very large listing', async () => {
    const many = Array.from({ length: 500 }, (_, i) =>
      mkTask({ description: 'x'.repeat(120), taskId: `bg-big-${i}` }),
    );
    const r = await taskListTool.execute({}, ctx(many));
    assert.ok(r.output.includes('truncated at 30000 chars'), r.output.slice(-200));
  });
});

describe('task_list parity — exported helpers', () => {
  it('isAgentTask / isMonitorTask discriminate command kinds', () => {
    assert.equal(isAgentTask('[agent] x'), true);
    assert.equal(isAgentTask('npm test'), false);
    assert.equal(isAgentTask(undefined), false);
    assert.equal(isMonitorTask('monitor:srv'), true);
    assert.equal(isMonitorTask('npm test'), false);
    assert.equal(isMonitorTask(undefined), false);
  });

  it('agentDisplayName returns label, then description, then command', () => {
    assert.equal(agentDisplayName({ command: '[agent] reviewer', description: 'd' }), 'reviewer');
    assert.equal(agentDisplayName({ command: '[agent] ', description: 'desc' }), 'desc');
    assert.equal(agentDisplayName({ command: 'raw', description: '' }), 'raw');
  });

  it('formatTokenCount renders compact units', () => {
    assert.equal(formatTokenCount(500), '500 tok');
    assert.equal(formatTokenCount(1500), '1.5k tok');
  });

  it('formatElapsed renders ms / s / m', () => {
    assert.equal(formatElapsed(850), '850ms');
    assert.equal(formatElapsed(42000), '42s');
    assert.equal(formatElapsed(210000), '3.5m');
  });

  it('formatTaskStatus covers every status + null-exit', () => {
    assert.equal(formatTaskStatus({ status: 'running', exitCode: null }), '▶ running');
    assert.equal(formatTaskStatus({ status: 'killed', exitCode: null }), '✗ killed');
    assert.equal(formatTaskStatus({ status: 'completed', exitCode: null }), '✓ completed');
    assert.equal(formatTaskStatus({ status: 'completed', exitCode: 0 }), '✓ completed (exit 0)');
    assert.equal(formatTaskStatus({ status: 'failed', exitCode: 2 }), '✗ failed (exit 2)');
    // unknown status → fallback icon + no exit when exit is null
    assert.equal(formatTaskStatus({ status: 'weird' as any, exitCode: null }), '• weird');
    assert.equal(formatTaskStatus({ status: 'weird' as any, exitCode: 7 }), '• weird (exit 7)');
  });

  it('summarizeTasks counts known statuses and ignores unknown', () => {
    const counts = summarizeTasks([
      { status: 'running' }, { status: 'running' },
      { status: 'completed' }, { status: 'weird' as any },
    ]);
    assert.equal(counts.running, 2);
    assert.equal(counts.completed, 1);
    assert.equal(counts.failed, 0);
    assert.equal(counts.killed, 0);
  });
});

describe('task_list parity — frozen elapsed for terminal tasks (endTime)', () => {
  it('renders a terminal task\'s duration from endTime, not the growing wall clock', async () => {
    const now = Date.now();
    const task: BackgroundProcess = {
      taskId: 'bg-frozen01',
      pid: 4242,
      command: 'echo done',
      description: 'quick echo',
      status: 'completed',
      exitCode: 0,
      outputPath: '/tmp/out',
      startTime: now - 100_000, // started 100s ago
      endTime: now - 70_000,    // but finished after only 30s
    };
    const r = await taskListTool.execute({}, ctx([task]));
    assert.ok(!r.isError);
    // 30s elapsed should render as "30s"; the un-frozen value (~100s) would be "1.7m".
    assert.ok(r.output.includes('30s'), `expected frozen 30s duration, got: ${r.output}`);
    assert.ok(!r.output.includes('1.7m'), 'must not report the ever-growing wall-clock duration');
  });
});
