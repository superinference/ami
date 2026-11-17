import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ProcessManager, type ProgressDelta } from '../src/process-manager';
import { taskListTool } from '../src/tools/task-list';
import type { ToolContext } from '../src/types';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-u031-w72-'));
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
// Feature 1 — GAP: a background AGENT had no live tool-call progress counter.
//   Claude Code exposes task.progress.toolCount (alongside tokenCount). AMI only
//   tracked tokenCount; updateEntry now accepts toolCount and task_list renders it.
// ---------------------------------------------------------------------------
describe('ProcessManager — toolCount progress field (GAP: no tool-call counter)', () => {
  let tmp: string | null = null;
  afterEach(() => {
    if (tmp) { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } tmp = null; }
  });

  it('updateEntry records toolCount on the agent entry', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'agent-tc', tmp);
    // Baseline: a fresh agent has no tool count yet.
    assert.equal(pm.list().find(t => t.taskId === 'agent-tc')?.toolCount, undefined);
    pm.updateEntry('agent-tc', { toolCount: 3 });
    assert.equal(pm.list().find(t => t.taskId === 'agent-tc')?.toolCount, 3);
    pm.updateEntry('agent-tc', { toolCount: 7 });
    assert.equal(pm.list().find(t => t.taskId === 'agent-tc')?.toolCount, 7);
    pm.cleanup();
  });

  it('updateEntry on an unknown task is a no-op (no throw)', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    assert.doesNotThrow(() => pm.updateEntry('nope', { toolCount: 5 }));
    pm.cleanup();
  });

  it('task_list renders the tool-call count for a running agent (singular/plural)', async () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'agent-one', tmp, 'one');
    registerAgentEntry(pm, 'agent-many', tmp, 'many');
    pm.updateEntry('agent-one', { toolCount: 1 });
    pm.updateEntry('agent-many', { toolCount: 5 });

    const r = await taskListTool.execute({}, ctxFor(pm));
    assert.ok(!r.isError, r.output);
    assert.match(r.output, /1 tool\b/);
    assert.match(r.output, /5 tools\b/);
    pm.cleanup();
  });

  it('task_list omits the tool tag when toolCount is zero/undefined', async () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'agent-zero', tmp, 'zero');
    pm.updateEntry('agent-zero', { toolCount: 0 });
    const r = await taskListTool.execute({}, ctxFor(pm));
    assert.doesNotMatch(r.output, /\b0 tools?\b/);
    pm.cleanup();
  });
});

// ---------------------------------------------------------------------------
// Feature 2 — GAP: no incremental progress delta (lastReportedToolCount /
//   lastReportedTokenCount). Claude caches the last-reported counts so a
//   notification reports only the CHANGE. reportProgressDelta implements this.
// ---------------------------------------------------------------------------
describe('ProcessManager.reportProgressDelta (GAP: no incremental progress delta)', () => {
  let tmp: string | null = null;
  afterEach(() => {
    if (tmp) { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } tmp = null; }
  });

  it('first report establishes a baseline with zero deltas', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'a1', tmp);
    pm.updateEntry('a1', { toolCount: 4, tokenCount: 1000 });
    const d = pm.reportProgressDelta('a1') as ProgressDelta;
    assert.equal(d.firstReport, true);
    assert.equal(d.toolCountDelta, 0);
    assert.equal(d.tokenCountDelta, 0);
    pm.cleanup();
  });

  it('subsequent reports return the change since the previous report', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'a2', tmp);
    pm.updateEntry('a2', { toolCount: 2, tokenCount: 500 });
    pm.reportProgressDelta('a2'); // baseline
    pm.updateEntry('a2', { toolCount: 5, tokenCount: 1700 });
    const d = pm.reportProgressDelta('a2') as ProgressDelta;
    assert.equal(d.firstReport, false);
    assert.equal(d.toolCountDelta, 3);
    assert.equal(d.tokenCountDelta, 1200);
    // No further change ⇒ next report is all zeros but still not a first report.
    const d2 = pm.reportProgressDelta('a2') as ProgressDelta;
    assert.equal(d2.firstReport, false);
    assert.equal(d2.toolCountDelta, 0);
    assert.equal(d2.tokenCountDelta, 0);
    pm.cleanup();
  });

  it('clamps a decreasing count to a zero delta (never negative)', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'a3', tmp);
    pm.updateEntry('a3', { toolCount: 9, tokenCount: 9000 });
    pm.reportProgressDelta('a3');
    pm.updateEntry('a3', { toolCount: 1, tokenCount: 10 });
    const d = pm.reportProgressDelta('a3') as ProgressDelta;
    assert.equal(d.toolCountDelta, 0);
    assert.equal(d.tokenCountDelta, 0);
    pm.cleanup();
  });

  it('returns undefined for an unknown task', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    assert.equal(pm.reportProgressDelta('ghost'), undefined);
    pm.cleanup();
  });
});

// ---------------------------------------------------------------------------
// E2E — the delta surfaces through the real task_list tool across two listings.
// ---------------------------------------------------------------------------
describe('task_list — incremental "since last check" delta (real ProcessManager, E2E)', () => {
  let tmp: string | null = null;
  afterEach(() => {
    if (tmp) { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } tmp = null; }
  });

  it('shows no delta on the first listing, then the change on the second', async () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'bg', tmp, 'crawler');
    pm.updateEntry('bg', { toolCount: 2, tokenCount: 800 });

    // First listing: baseline — no "since last check" clause.
    const r1 = await taskListTool.execute({}, ctxFor(pm));
    assert.ok(!r1.isError, r1.output);
    assert.doesNotMatch(r1.output, /since last check/);

    // Agent did more work.
    pm.updateEntry('bg', { toolCount: 6, tokenCount: 2300 });

    // Second listing: the delta is surfaced.
    const r2 = await taskListTool.execute({}, ctxFor(pm));
    assert.match(r2.output, /\+4 tools/);
    assert.match(r2.output, /since last check/);
    assert.match(r2.output, /\+1\.5k tok/);

    // Third listing with no further work ⇒ delta clause disappears again.
    const r3 = await taskListTool.execute({}, ctxFor(pm));
    assert.doesNotMatch(r3.output, /since last check/);
    pm.cleanup();
  });

  it('a terminal (non-running) agent does not latch a delta baseline', async () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerAgentEntry(pm, 'done', tmp, 'finisher');
    pm.updateEntry('done', { toolCount: 3, tokenCount: 400, status: 'completed', exitCode: 0 });
    // Completed agents are listed but the delta clause only applies to running ones.
    const r = await taskListTool.execute({}, ctxFor(pm));
    assert.doesNotMatch(r.output, /since last check/);
    pm.cleanup();
  });
});
