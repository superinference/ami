import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { workflowTool } from '../src/tools/workflow';
import { projectStateDir } from '../src/reference-journal';
import { DISABLE_BACKGROUND_TASKS_ENV } from '../src/tools/tool-gates';

// U1 — background workflow execution. The workflow tool runs DETACHED in the
// background by default (parity with the reference's "Running in background ·
// /flows to monitor") and returns a task id immediately; the whole run is ONE
// process-manager entry of kind 'workflow' and its completion routes through the
// SHARED emitCompletion path so a <task-notification> surfaces for free.
//
// RED/GREEN: reverting the background branch in workflow.ts (so execute() always
// awaits executeWorkflow and returns "Workflow '...' completed") makes every
// "returns immediately in the background" / kind:'workflow' / emitCompletion
// assertion below fail — proving these discriminate the fix from the old blocking
// behavior.

/** Fake ProcessManager capturing registerAgent / updateEntry / emitCompletion.
 *  Unlike the cov100 fake it does NOT auto-invoke abortFn, so a backgrounded run
 *  is left to complete on its own unless a test explicitly aborts it. */
function makeProcessManager() {
  const calls = {
    flows: [] as string[],
    registered: [] as Array<{ taskId: string; opts: any }>,
    updates: [] as Array<{ taskId: string; updates: any }>,
    completions: [] as any[],
  };
  let flowCounter = 0;
  const pm: any = {
    flowManager: {
      createFlow(name: string) { const id = `flow-${++flowCounter}-${name}`; calls.flows.push(id); return id; },
      addAgent(_flowId: string, _taskId: string) { /* captured via registered */ },
    },
    registerAgent(taskId: string, opts: any) { calls.registered.push({ taskId, opts }); },
    updateEntry(taskId: string, updates: any) { calls.updates.push({ taskId, updates }); },
    emitCompletion(payload: any) { calls.completions.push(payload); },
  };
  return { pm, calls };
}

/** A sub-engine factory whose submit streams one text delta then completes. */
function instantEngineFactory() {
  return (_config: any) => ({
    async *submit(_prompt: string) {
      yield { type: 'text_delta', text: 'done' } as any;
    },
    shutdown() { /* noop */ },
  });
}

/** A sub-engine factory that HANGS until its config.abortController aborts, so a
 *  test can drive the run to a 'killed' terminal state via the registered abortFn. */
function hangingEngineFactory() {
  return (config: any) => ({
    // eslint-disable-next-line require-yield
    async *submit(_prompt: string) {
      await new Promise<void>((_resolve, reject) => {
        const sig: AbortSignal | undefined = config?.abortController?.signal;
        if (sig?.aborted) return reject(new Error('aborted'));
        sig?.addEventListener('abort', () => reject(new Error('aborted')), { once: true });
      });
    },
    shutdown() { /* noop */ },
  });
}

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error('waitFor timed out');
    await new Promise(r => setTimeout(r, 5));
  }
}

describe('workflowTool — background deploy (U1)', () => {
  const dirs: string[] = [];
  afterEach(() => {
    for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
    delete process.env[DISABLE_BACKGROUND_TASKS_ENV];
  });
  const mkcwd = () => { const d = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wf-bg-')); dirs.push(d); return d; };

  const baseCtx = (cwd: string, pm: any, factory: any) => ({
    cwd,
    abortSignal: new AbortController().signal,
    processManager: pm,
    _providerConfig: { provider: 'test' },
    _engineFactory: factory,
  }) as any;

  it('backgrounds by DEFAULT: returns a task id immediately and registers a kind:"workflow" entry', async () => {
    const cwd = mkcwd();
    const { pm, calls } = makeProcessManager();
    const script = `export const meta = { name: 'bg-wf', description: 'd' }\nreturn await agent('step', { label: 'S' })`;

    const r = await workflowTool.execute!({ script }, baseCtx(cwd, pm, instantEngineFactory()));

    // Returned IMMEDIATELY with the background affordance (not the completed block).
    assert.equal(r.isError, false);
    assert.match(r.output, /started in the background/);
    assert.match(r.output, /Task id: workflow-[0-9a-f]{16}/);
    assert.match(r.output, /\/flows to (monitor|watch|manage)|Monitor it live with \/flows/);
    assert.doesNotMatch(r.output, /Workflow 'bg-wf' completed/);

    // ONE workflow-level entry registered (distinct from its agent children).
    const wfEntry = calls.registered.find(e => e.opts.kind === 'workflow');
    assert.ok(wfEntry, 'a kind:"workflow" entry must be registered');
    assert.match(wfEntry!.taskId, /^workflow-[0-9a-f]{16}$/);
    assert.match(wfEntry!.opts.command, /\[workflow\] bg-wf/);
    assert.equal(typeof wfEntry!.opts.abortFn, 'function');

    // The detached run completes and routes its completion through emitCompletion.
    await waitFor(() => calls.completions.length > 0);
    const comp = calls.completions[0];
    assert.equal(comp.taskId, wfEntry!.taskId);
    assert.equal(comp.status, 'completed');
    assert.equal(comp.exitCode, 0);
    // The terminal updateEntry marked the workflow entry completed.
    const finalUpd = calls.updates.filter(u => u.taskId === wfEntry!.taskId).pop();
    assert.equal(finalUpd?.updates.status, 'completed');
  });

  it('run_in_background:false runs INLINE and returns the full completed result (no workflow entry)', async () => {
    const cwd = mkcwd();
    const { pm, calls } = makeProcessManager();
    const script = `export const meta = { name: 'fg-wf', description: 'd' }\nreturn 7`;

    const r = await workflowTool.execute!({ script, run_in_background: false }, baseCtx(cwd, pm, instantEngineFactory()));

    assert.equal(r.isError, false);
    assert.match(r.output, /Workflow 'fg-wf' completed/);
    assert.match(r.output, /Result: 7/);
    // No background workflow-kind entry, no emitCompletion on the inline path.
    assert.equal(calls.registered.filter(e => e.opts.kind === 'workflow').length, 0);
    assert.equal(calls.completions.length, 0);
  });

  it('with NO process manager, runs inline even by default (nothing to track)', async () => {
    const cwd = mkcwd();
    const script = `export const meta = { name: 'nopm', description: 'd' }\nreturn 1`;
    const ctx: any = { cwd, abortSignal: new AbortController().signal, _providerConfig: { provider: 'test' }, _engineFactory: instantEngineFactory() };

    const r = await workflowTool.execute!({ script }, ctx);
    assert.equal(r.isError, false);
    assert.match(r.output, /Workflow 'nopm' completed/);
    assert.doesNotMatch(r.output, /started in the background/);
  });

  it('honors the global background-disable gate', async () => {
    const cwd = mkcwd();
    const { pm, calls } = makeProcessManager();
    process.env[DISABLE_BACKGROUND_TASKS_ENV] = '1';
    const script = `export const meta = { name: 'gated', description: 'd' }\nreturn 2`;

    // An EXPLICIT background request is an error when background is disabled.
    const explicit = await workflowTool.execute!({ script, run_in_background: true }, baseCtx(cwd, pm, instantEngineFactory()));
    assert.equal(explicit.isError, true);
    assert.match(explicit.output, /background workflows are disabled/);

    // A DEFAULTED request silently falls back to inline (no error, no bg entry).
    const defaulted = await workflowTool.execute!({ script }, baseCtx(cwd, pm, instantEngineFactory()));
    assert.equal(defaulted.isError, false);
    assert.match(defaulted.output, /Workflow 'gated' completed/);
    assert.equal(calls.registered.filter(e => e.opts.kind === 'workflow').length, 0);
  });

  it('a background run aborted via its registered abortFn terminates as "killed"', async () => {
    const cwd = mkcwd();
    const { pm, calls } = makeProcessManager();
    const script = `export const meta = { name: 'kill-wf', description: 'd' }\nreturn await agent('hang', { label: 'H' })`;

    const r = await workflowTool.execute!({ script }, baseCtx(cwd, pm, hangingEngineFactory()));
    assert.match(r.output, /started in the background/);

    const wfEntry = calls.registered.find(e => e.opts.kind === 'workflow');
    assert.ok(wfEntry);
    // Stop it the way /flows stop / task_kill does — via the registered abortFn.
    wfEntry!.opts.abortFn();

    await waitFor(() => calls.completions.length > 0);
    assert.equal(calls.completions[0].status, 'killed');
    assert.equal(calls.completions[0].exitCode, 1);
  });

  it('writes the run output to the workflow task output file', async () => {
    const cwd = mkcwd();
    const { pm, calls } = makeProcessManager();
    const script = `export const meta = { name: 'outfile', description: 'd' }\nreturn 42`;

    const r = await workflowTool.execute!({ script }, baseCtx(cwd, pm, instantEngineFactory()));
    const wfEntry = calls.registered.find(e => e.opts.kind === 'workflow')!;
    await waitFor(() => calls.completions.length > 0);

    const outPath = path.join(projectStateDir(cwd, 'tasks'), `${wfEntry.taskId}.output`);
    assert.ok(fs.existsSync(outPath));
    assert.match(fs.readFileSync(outPath, 'utf-8'), /Workflow 'outfile' completed[\s\S]*Result: 42/);
    void r;
  });
});
