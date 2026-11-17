import { describe, it, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { taskKillTool } from '../src/tools/task-kill';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U134 — per-agent workflow recovery via task_kill `retry` / `skip`.
//
// Parity with the reference workflow detail actions onRetryAgent / onSkipAgent
// (retryWorkflowAgent / skipWorkflowAgent): recover ONE wedged agent in a still-
// running workflow without aborting the whole run. task_kill retry/skip delegate
// to ProcessManager.retryAgentTask / skipAgentTask, which invoke the per-agent
// recovery callbacks the workflow tool registered at registerAgent time.
// ---------------------------------------------------------------------------

const tmpDirs: string[] = [];
function makeTmpDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-tk-recover-'));
  tmpDirs.push(d);
  return d;
}
function makePm(): ProcessManager { return new ProcessManager(makeTmpDir()); }
function registerAgent(
  pm: ProcessManager,
  id: string,
  cbs?: { retryFn?: () => void; skipFn?: () => void },
): void {
  pm.registerAgent(id, {
    command: `[agent] ${id}`,
    description: id,
    outputPath: path.join(makeTmpDir(), `${id}.out`),
    abortFn: () => { /* no-op */ },
    ...cbs,
  });
}
after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best-effort */ } }
});

describe('ProcessManager.retryAgentTask / skipAgentTask', () => {
  it('retryAgentTask invokes the registered retry callback for a running agent', () => {
    const pm = makePm();
    let retried = 0;
    registerAgent(pm, 'a1', { retryFn: () => { retried++; } });
    assert.equal(pm.retryAgentTask('a1'), true);
    assert.equal(retried, 1);
  });

  it('skipAgentTask invokes the registered skip callback for a running agent', () => {
    const pm = makePm();
    let skipped = 0;
    registerAgent(pm, 'a1', { skipFn: () => { skipped++; } });
    assert.equal(pm.skipAgentTask('a1'), true);
    assert.equal(skipped, 1);
  });

  it('returns false for an unknown task, or an agent with no callback', () => {
    const pm = makePm();
    assert.equal(pm.retryAgentTask('nope'), false);
    assert.equal(pm.skipAgentTask('nope'), false);
    registerAgent(pm, 'a2'); // no retry/skip callbacks
    assert.equal(pm.retryAgentTask('a2'), false);
    assert.equal(pm.skipAgentTask('a2'), false);
  });

  it('a throwing recovery callback is swallowed (best-effort) and still reports success', () => {
    const pm = makePm();
    registerAgent(pm, 'a3', { retryFn: () => { throw new Error('boom'); }, skipFn: () => { throw new Error('boom'); } });
    assert.equal(pm.retryAgentTask('a3'), true);
    assert.equal(pm.skipAgentTask('a3'), true);
  });

  it('emits agent-retry-requested / agent-skip-requested events', () => {
    const pm = makePm();
    const events: string[] = [];
    pm.on('agent-retry-requested', (e: { taskId: string }) => events.push(`retry:${e.taskId}`));
    pm.on('agent-skip-requested', (e: { taskId: string }) => events.push(`skip:${e.taskId}`));
    registerAgent(pm, 'a4', { retryFn: () => {}, skipFn: () => {} });
    pm.retryAgentTask('a4');
    pm.skipAgentTask('a4');
    assert.deepEqual(events, ['retry:a4', 'skip:a4']);
  });

  it('does not serialize the internal recovery callbacks in task snapshots', () => {
    const pm = makePm();
    registerAgent(pm, 'a5', { retryFn: () => {}, skipFn: () => {} });
    const snap = pm.get('a5') as Record<string, unknown>;
    assert.equal(snap._retryFn, undefined);
    assert.equal(snap._skipFn, undefined);
  });
});

describe('task_kill retry / skip options', () => {
  function ctx(pm: ProcessManager): ToolContext {
    return { cwd: '/tmp', processManager: pm } as unknown as ToolContext;
  }

  it('retry re-runs ONE running workflow agent and reports `retried`', async () => {
    const pm = makePm();
    let retried = 0;
    registerAgent(pm, 'a1', { retryFn: () => { retried++; } });
    const r = await taskKillTool.execute!({ task_id: 'a1', retry: true }, ctx(pm));
    assert.equal(r.isError, false);
    assert.equal((r.metadata as any).reason, 'retried');
    assert.equal((r.metadata as any).taskType, 'agent');
    assert.equal(retried, 1);
  });

  it('skip settles ONE running workflow agent and reports `skipped`', async () => {
    const pm = makePm();
    let skipped = 0;
    registerAgent(pm, 'a1', { skipFn: () => { skipped++; } });
    const r = await taskKillTool.execute!({ task_id: 'a1', skip: true }, ctx(pm));
    assert.equal(r.isError, false);
    assert.equal((r.metadata as any).reason, 'skipped');
    assert.equal(skipped, 1);
  });

  it('retry on an agent without a retry callback is refused with retry_unsupported', async () => {
    const pm = makePm();
    registerAgent(pm, 'a2'); // no callbacks
    const r = await taskKillTool.execute!({ task_id: 'a2', retry: true }, ctx(pm));
    assert.ok(r.isError);
    assert.equal((r.metadata as any).reason, 'retry_unsupported');
  });

  it('skip on an agent without a skip callback is refused with skip_unsupported', async () => {
    const pm = makePm();
    registerAgent(pm, 'a2');
    const r = await taskKillTool.execute!({ task_id: 'a2', skip: true }, ctx(pm));
    assert.ok(r.isError);
    assert.equal((r.metadata as any).reason, 'skip_unsupported');
  });

  it('rejects a non-boolean retry / skip', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1', { retryFn: () => {}, skipFn: () => {} });
    const r1 = await taskKillTool.execute!({ task_id: 'a1', retry: 'yes' }, ctx(pm));
    assert.ok(r1.isError);
    assert.equal((r1.metadata as any).reason, 'invalid_input');
    const r2 = await taskKillTool.execute!({ task_id: 'a1', skip: 1 }, ctx(pm));
    assert.ok(r2.isError);
    assert.equal((r2.metadata as any).reason, 'invalid_input');
  });

  it('retry + skip (and other per-agent actions) are mutually exclusive', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1', { retryFn: () => {}, skipFn: () => {} });
    const r = await taskKillTool.execute!({ task_id: 'a1', retry: true, skip: true }, ctx(pm));
    assert.ok(r.isError);
    assert.equal((r.metadata as any).reason, 'invalid_input');
    assert.ok(r.output.includes('mutually exclusive'));
  });

  it('retry/skip appear in the list of allowed keys on unknown-input rejection', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1', { retryFn: () => {}, skipFn: () => {} });
    const r = await taskKillTool.execute!({ task_id: 'a1', bogus: true }, ctx(pm));
    assert.ok(r.isError);
    assert.match(r.output, /retry, skip/);
  });
});
