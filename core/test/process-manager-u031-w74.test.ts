import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  ProcessManager,
  formatUsageClause,
  formatBackgroundCompletionNote,
  type CompletedTaskEvent,
} from '../src/process-manager';
import { taskOutputTool } from '../src/tools/task-output';
import { taskListTool, countDistinctTeams } from '../src/tools/task-list';
import type { BackgroundProcess } from '../src/process-manager';
import type { ToolContext } from '../src/types';

const baseEvent: CompletedTaskEvent = {
  taskId: 'bg-1',
  exitCode: 0,
  command: 'echo hi',
  description: 'echo hi',
  outputPath: '/tmp/x.output',
};

// --------------------------------------------------------------------------
// #24 — usage clause: duration + total_tokens + tool_uses
// --------------------------------------------------------------------------
describe('U031-w74 formatUsageClause — tokens + tool uses', () => {
  it('keeps the duration-only clause unchanged (backward compat)', () => {
    assert.equal(formatUsageClause({ ...baseEvent, durationMs: 3210 }), ' Usage: ran 3.2s.');
    assert.equal(formatUsageClause({ ...baseEvent, durationMs: 0 }), ' Usage: ran 0.0s.');
  });

  it('returns empty string when nothing is known', () => {
    assert.equal(formatUsageClause(baseEvent), '');
  });

  it('omits a negative (skewed) duration', () => {
    assert.equal(formatUsageClause({ ...baseEvent, durationMs: -5 }), '');
  });

  it('renders duration + tokens + tool uses together', () => {
    assert.equal(
      formatUsageClause({ ...baseEvent, durationMs: 3210, tokenCount: 1500, toolCount: 4 }),
      ' Usage: ran 3.2s, 1500 tokens, 4 tool uses.',
    );
  });

  it('uses singular nouns for a count of 1', () => {
    assert.equal(
      formatUsageClause({ ...baseEvent, durationMs: 1000, tokenCount: 1, toolCount: 1 }),
      ' Usage: ran 1.0s, 1 token, 1 tool use.',
    );
  });

  it('renders tokens even when the duration is unknown', () => {
    assert.equal(
      formatUsageClause({ ...baseEvent, tokenCount: 42 }),
      ' Usage: 42 tokens.',
    );
  });

  it('omits zero token/tool counts (a bash shell reports neither)', () => {
    assert.equal(
      formatUsageClause({ ...baseEvent, durationMs: 2000, tokenCount: 0, toolCount: 0 }),
      ' Usage: ran 2.0s.',
    );
  });

  it('the completion note embeds the full usage clause', () => {
    const note = formatBackgroundCompletionNote({
      ...baseEvent, status: 'completed', exitCode: 0, durationMs: 1500, tokenCount: 2000, toolCount: 3,
    });
    assert.ok(note.includes('Usage: ran 1.5s, 2000 tokens, 3 tool uses.'), note);
  });
});

// --------------------------------------------------------------------------
// #24 — emitCompletion routes an in-process agent through the durable buffer,
// enriched with duration + result token/tool totals.
// --------------------------------------------------------------------------
describe('U031-w74 emitCompletion — buffered + enriched agent completion', () => {
  let pm: ProcessManager;
  let tmp: string;
  beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-pm-w74-')); pm = new ProcessManager(tmp); });
  afterEach(() => { try { pm.cleanup(); } catch { /* */ } fs.rmSync(tmp, { recursive: true, force: true }); });

  it('buffers an agent completion carrying token/tool usage and duration', () => {
    const id = 'agent-usage';
    pm.registerAgent(id, {
      command: '[agent] reviewer',
      description: 'Background agent: reviewer',
      outputPath: path.join(tmp, `${id}.output`),
      abortFn: () => {},
    });
    // Final-result stats recorded at the terminal transition (as task.ts does).
    pm.updateEntry(id, { status: 'completed', exitCode: 0, resultToolCount: 7, resultTokenCount: 3400 });
    pm.emitCompletion({ taskId: id, exitCode: 0, command: '[agent] reviewer', description: 'Background agent: reviewer', outputPath: path.join(tmp, `${id}.output`), status: 'completed' });

    const drained = pm.drainCompletedTasks();
    const ev = drained.find(e => e.taskId === id);
    assert.ok(ev, 'agent completion must be buffered via emitCompletion');
    assert.equal(ev!.tokenCount, 3400);
    assert.equal(ev!.toolCount, 7);
    assert.equal(ev!.status, 'completed');
    assert.equal(typeof ev!.durationMs, 'number');
    assert.ok(ev!.summary!.length > 0);
    const note = formatBackgroundCompletionNote(ev!);
    assert.ok(note.includes('3400 tokens'), note);
    assert.ok(note.includes('7 tool uses'), note);
  });

  it('falls back to the live counters when no result totals were recorded', () => {
    const id = 'agent-live';
    pm.registerAgent(id, { command: '[agent] a', description: 'd', outputPath: path.join(tmp, `${id}.output`), abortFn: () => {} });
    pm.updateEntry(id, { tokenCount: 500, toolCount: 2 });
    pm.updateEntry(id, { status: 'completed', exitCode: 0 });
    pm.emitCompletion({ taskId: id, exitCode: 0, command: '[agent] a', description: 'd', outputPath: '', status: 'completed' });
    const ev = pm.drainCompletedTasks().find(e => e.taskId === id)!;
    assert.equal(ev.tokenCount, 500);
    assert.equal(ev.toolCount, 2);
  });

  it('suppresses a completion for an already-notified (task_output-read) agent', () => {
    const id = 'agent-read';
    pm.registerAgent(id, { command: '[agent] a', description: 'd', outputPath: path.join(tmp, `${id}.output`), abortFn: () => {} });
    pm.updateEntry(id, { status: 'completed', exitCode: 0, resultTokenCount: 100 });
    assert.equal(pm.markNotified(id), true);
    pm.emitCompletion({ taskId: id, exitCode: 0, command: '[agent] a', description: 'd', outputPath: '', status: 'completed' });
    assert.deepEqual(pm.drainCompletedTasks(), [], 'a notified agent completion is not re-surfaced');
  });

  it('a real bash task completion has no token/tool usage (duration-only clause)', async () => {
    const id = pm.spawn('echo done', { cwd: tmp, description: 'echo' });
    const start = Date.now();
    while (Date.now() - start < 5000) {
      if (pm.getOutput(id)?.status !== 'running') break;
      await new Promise(r => setTimeout(r, 20));
    }
    const ev = pm.drainCompletedTasks().find(e => e.taskId === id)!;
    assert.equal(ev.tokenCount, undefined);
    assert.equal(ev.toolCount, undefined);
    assert.ok(!formatUsageClause(ev).includes('token'));
  });
});

// --------------------------------------------------------------------------
// #36 — resumed marker; #31 — teamName field on registerAgent.
// --------------------------------------------------------------------------
describe('U031-w74 registerAgent — resumed + teamName introspection fields', () => {
  let pm: ProcessManager;
  let tmp: string;
  beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-pm-w74b-')); pm = new ProcessManager(tmp); });
  afterEach(() => { try { pm.cleanup(); } catch { /* */ } fs.rmSync(tmp, { recursive: true, force: true }); });

  it('records resumed + teamName and exposes them via get()/list()', () => {
    pm.registerAgent('agent-r', {
      command: '[agent] r', description: 'd', outputPath: path.join(tmp, 'r.output'),
      abortFn: () => {}, resumed: true, teamName: 'alpha',
    });
    const snap = pm.get('agent-r');
    assert.equal(snap!.resumed, true);
    assert.equal(snap!.teamName, 'alpha');
    assert.equal(pm.list().find(t => t.taskId === 'agent-r')!.teamName, 'alpha');
  });

  it('a fresh spawn leaves resumed/teamName undefined', () => {
    pm.registerAgent('agent-f', { command: '[agent] f', description: 'd', outputPath: path.join(tmp, 'f.output'), abortFn: () => {} });
    const snap = pm.get('agent-f');
    assert.equal(snap!.resumed, undefined);
    assert.equal(snap!.teamName, undefined);
  });
});

// --------------------------------------------------------------------------
// #36 consumer — task_output surfaces "Invocation: resumed".
// --------------------------------------------------------------------------
describe('U031-w74 task_output — resumed invocation marker', () => {
  let pm: ProcessManager;
  let tmp: string;
  beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-pm-w74c-')); pm = new ProcessManager(tmp); });
  afterEach(() => { try { pm.cleanup(); } catch { /* */ } fs.rmSync(tmp, { recursive: true, force: true }); });

  function ctx(): ToolContext {
    return { cwd: tmp, abortSignal: new AbortController().signal, processManager: pm } as unknown as ToolContext;
  }

  it('marks a resumed agent', async () => {
    const out = path.join(tmp, 'ra.output');
    fs.writeFileSync(out, 'work\n');
    pm.registerAgent('agent-ra', { command: '[agent] ra', description: 'd', outputPath: out, abortFn: () => {}, resumed: true });
    const r = await taskOutputTool.execute({ task_id: 'agent-ra', wait: false }, ctx());
    assert.ok(r.output.includes('Invocation: resumed'), r.output);
  });

  it('omits the marker for a fresh spawn', async () => {
    const out = path.join(tmp, 'fa.output');
    fs.writeFileSync(out, 'work\n');
    pm.registerAgent('agent-fa', { command: '[agent] fa', description: 'd', outputPath: out, abortFn: () => {} });
    const r = await taskOutputTool.execute({ task_id: 'agent-fa', wait: false }, ctx());
    assert.ok(!r.output.includes('Invocation: resumed'), r.output);
  });
});

// --------------------------------------------------------------------------
// #31 consumer — countDistinctTeams + task_list "N teams" tally.
// --------------------------------------------------------------------------
describe('U031-w74 task_list — distinct-team tally', () => {
  function mkAgent(over: Partial<BackgroundProcess>): BackgroundProcess {
    return {
      taskId: over.taskId!, pid: 1, command: over.command ?? '[agent] x', description: over.description ?? 'd',
      status: over.status ?? 'running', kind: 'agent', exitCode: null, outputPath: '/tmp/o', startTime: Date.now(),
      ...(over.teamName !== undefined ? { teamName: over.teamName } : {}),
    };
  }
  function ctx(tasks: BackgroundProcess[]): ToolContext {
    return { cwd: '/tmp', abortSignal: new AbortController().signal, processManager: { list: () => tasks } as any } as ToolContext;
  }

  it('countDistinctTeams counts distinct non-empty names', () => {
    assert.equal(countDistinctTeams([{ teamName: 'a' }, { teamName: 'a' }, { teamName: 'b' }, {}]), 2);
    assert.equal(countDistinctTeams([{}, {}]), 0);
  });

  it('renders "N teams" on the by-type line when agents carry teams', async () => {
    const tasks = [
      mkAgent({ taskId: 'agent-1', command: '[agent] one', teamName: 'alpha' }),
      mkAgent({ taskId: 'agent-2', command: '[agent] two', teamName: 'beta' }),
    ];
    const r = await taskListTool.execute({}, ctx(tasks));
    assert.ok(r.output.includes('By type:'), r.output);
    assert.ok(r.output.includes('2 teams'), r.output);
  });

  it('uses the singular "1 team" and omits teams when none are set', async () => {
    const one = await taskListTool.execute({}, ctx([mkAgent({ taskId: 'agent-3', command: '[agent] a', teamName: 'solo' })]));
    assert.ok(one.output.includes('1 team'), one.output);
    const none = await taskListTool.execute({}, ctx([mkAgent({ taskId: 'agent-4', command: '[agent] b' })]));
    assert.ok(!none.output.includes('team'), none.output);
  });
});
