import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ProcessManager, PROGRESS_IDLE_MS } from '../src/process-manager';
import { taskListTool } from '../src/tools/task-list';
import type { ToolContext } from '../src/types';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-u031-w73-'));
}

function ctxFor(pm: ProcessManager): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    processManager: pm,
  } as unknown as ToolContext;
}

function registerAgentEntry(pm: ProcessManager, id: string, tmp: string, label = 'builder'): void {
  pm.registerAgent(id, {
    command: `[agent] ${label}`,
    description: `Background agent: ${label}`,
    outputPath: path.join(tmp, `${id}.output`),
    abortFn: () => { /* noop */ },
  });
}

// ---------------------------------------------------------------------------
// Feature 1 — GAP (AsyncAgentDetailDialog L100-101): a background agent had no
//   separate FINAL-result token/tool totals distinct from the LIVE progress
//   counters. Claude Code shows a finished agent's `result` stats separately from
//   the in-flight `progress` stats. updateEntry now records resultTokenCount /
//   resultToolCount and task_list prefers them for a terminal agent (fallback to
//   the live counter when a legacy producer never set them).
// ---------------------------------------------------------------------------
describe('ProcessManager — result-vs-live stats (GAP: no final-result totals)', () => {
  let tmp: string | null = null;
  afterEach(() => {
    if (tmp) { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } tmp = null; }
  });

  it('updateEntry records resultTokenCount / resultToolCount on the entry snapshot', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'r1', tmp);
    assert.equal(pm.get('r1')?.resultTokenCount, undefined);
    assert.equal(pm.get('r1')?.resultToolCount, undefined);
    pm.updateEntry('r1', { status: 'completed', exitCode: 0, resultTokenCount: 2000, resultToolCount: 4 });
    const snap = pm.get('r1');
    assert.equal(snap?.resultTokenCount, 2000);
    assert.equal(snap?.resultToolCount, 4);
    // list() exposes them too (not stripped as internal).
    const listed = pm.list().find(t => t.taskId === 'r1');
    assert.equal(listed?.resultTokenCount, 2000);
    assert.equal(listed?.resultToolCount, 4);
    pm.cleanup();
  });

  it('task_list prefers the result totals over the stale live counter for a terminal agent', async () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'done', tmp, 'finisher');
    // Live counters lagged the true totals; the terminal transition records the
    // authoritative result totals.
    pm.updateEntry('done', { tokenCount: 1500, toolCount: 2 });
    pm.updateEntry('done', { status: 'completed', exitCode: 0, resultTokenCount: 2000, resultToolCount: 5 });
    const r = await taskListTool.execute({}, ctxFor(pm));
    assert.ok(!r.isError, r.output);
    assert.match(r.output, /2\.0k tok/);     // result total, not the live 1.5k
    assert.doesNotMatch(r.output, /1\.5k tok/);
    assert.match(r.output, /5 tools/);        // result total, not the live 2
    pm.cleanup();
  });

  it('task_list falls back to the live counter for a terminal agent with no result totals', async () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'legacy', tmp, 'legacy');
    // Legacy producer: only live counters, terminal via status (no result totals).
    pm.updateEntry('legacy', { tokenCount: 1500, toolCount: 2 });
    pm.updateEntry('legacy', { status: 'completed', exitCode: 0 });
    const r = await taskListTool.execute({}, ctxFor(pm));
    assert.match(r.output, /1\.5k tok/);
    assert.match(r.output, /2 tools/);
    pm.cleanup();
  });

  it('a RUNNING agent still shows the live counter, not the result total', async () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'live', tmp, 'live');
    pm.updateEntry('live', { tokenCount: 1500, toolCount: 2, resultTokenCount: 9000, resultToolCount: 99 });
    const r = await taskListTool.execute({}, ctxFor(pm));
    assert.match(r.output, /1\.5k tok/);
    assert.match(r.output, /2 tools/);
    assert.doesNotMatch(r.output, /9\.0k tok/);
    pm.cleanup();
  });
});

// ---------------------------------------------------------------------------
// Feature 2 — GAP (CoordinatorAgentStatus L168-183): a background agent had no
//   live progress SUMMARY (what it is doing now) nor an activity-direction
//   indicator. Claude Code's progress.summary takes precedence over the static
//   description, and progress.lastActivity drives an idle/active indicator.
//   updateEntry now records progressSummary and stamps lastActivity on any live
//   progress signal; task_list surfaces both for a running agent.
// ---------------------------------------------------------------------------
describe('ProcessManager — progressSummary + lastActivity (GAP: no live activity)', () => {
  let tmp: string | null = null;
  afterEach(() => {
    if (tmp) { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } tmp = null; }
  });

  it('updateEntry records progressSummary and stamps lastActivity on a progress signal', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'p1', tmp);
    assert.equal(pm.get('p1')?.progressSummary, undefined);
    assert.equal(pm.get('p1')?.lastActivity, undefined);
    const before = Date.now();
    pm.updateEntry('p1', { toolCount: 1, progressSummary: 'bash: npm test' });
    const snap = pm.get('p1');
    assert.equal(snap?.progressSummary, 'bash: npm test');
    assert.ok(typeof snap?.lastActivity === 'number' && snap.lastActivity >= before);
    pm.cleanup();
  });

  it('a status/exitCode-only update does NOT stamp lastActivity (not progress)', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'p2', tmp);
    pm.updateEntry('p2', { status: 'running', exitCode: null });
    assert.equal(pm.get('p2')?.lastActivity, undefined);
    pm.cleanup();
  });

  it('a token-only update stamps lastActivity (live token progress counts)', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'p3', tmp);
    pm.updateEntry('p3', { tokenCount: 500 });
    assert.ok(typeof pm.get('p3')?.lastActivity === 'number');
    pm.cleanup();
  });

  it('task_list surfaces the progress summary (precedence over description) for a running agent', async () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'act', tmp, 'crawler');
    pm.updateEntry('act', { toolCount: 1, progressSummary: 'bash: npm run build' });
    const r = await taskListTool.execute({}, ctxFor(pm));
    assert.ok(!r.isError, r.output);
    assert.match(r.output, /— bash: npm run build/);
    pm.cleanup();
  });

  it('task_list flags an agent idle once no progress has arrived for PROGRESS_IDLE_MS', async () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'idle', tmp, 'sleeper');
    // Drive the last-activity stamp into the past so the idle branch fires without
    // waiting out the real threshold (Date.now is what updateEntry stamps).
    const realNow = Date.now;
    Date.now = () => realNow() - (PROGRESS_IDLE_MS + 5_000);
    try {
      pm.updateEntry('idle', { toolCount: 1 });
    } finally {
      Date.now = realNow;
    }
    const r = await taskListTool.execute({}, ctxFor(pm));
    assert.match(r.output, /\[idle \d+s\]/);
    pm.cleanup();
  });

  it('task_list shows NO idle flag for a freshly-active agent', async () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'fresh', tmp, 'worker');
    pm.updateEntry('fresh', { toolCount: 1, progressSummary: 'grep: foo' });
    const r = await taskListTool.execute({}, ctxFor(pm));
    assert.doesNotMatch(r.output, /\[idle/);
    pm.cleanup();
  });

  it('task_list shows neither summary nor idle flag for a TERMINAL agent', async () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'fin', tmp, 'done');
    const realNow = Date.now;
    Date.now = () => realNow() - (PROGRESS_IDLE_MS + 5_000);
    try {
      pm.updateEntry('fin', { toolCount: 1, progressSummary: 'bash: old' });
    } finally {
      Date.now = realNow;
    }
    pm.updateEntry('fin', { status: 'completed', exitCode: 0 });
    const r = await taskListTool.execute({}, ctxFor(pm));
    assert.doesNotMatch(r.output, /— bash: old/);
    assert.doesNotMatch(r.output, /\[idle/);
    pm.cleanup();
  });
});
