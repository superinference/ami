import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  ProcessManager,
  resolveSpawnCwd,
  cwdHooks,
  STDERR_TAIL_CAP,
  TASK_ATTENTION_STATES,
  STALL_THRESHOLD_MS,
} from '../src/process-manager';
import { taskKillTool } from '../src/tools/task-kill';
import { taskOutputTool } from '../src/tools/task-output';
import { taskListTool } from '../src/tools/task-list';
import type { ToolContext } from '../src/types';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'pm-u031w77-'));
}

async function cleanupPm(pm: ProcessManager | undefined): Promise<void> {
  if (!pm) return;
  try { pm.cleanup(); } catch { /* ignore */ }
  await new Promise((r) => setTimeout(r, 30));
}

function waitFor(cond: () => boolean, timeoutMs = 4000): Promise<void> {
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

function toolCtx(pm: ProcessManager, tmp: string): ToolContext {
  return {
    cwd: tmp,
    abortSignal: new AbortController().signal,
    processManager: pm,
  } as unknown as ToolContext;
}

let pm: ProcessManager | undefined;
let tmp: string;
afterEach(async () => {
  await cleanupPm(pm);
  pm = undefined;
  if (tmp) fs.rmSync(tmp, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Gap #9 — CWD existence validation + recovery before spawn
// ---------------------------------------------------------------------------
describe('U031-w77 resolveSpawnCwd — deleted-cwd recovery', () => {
  it('keeps an existing requested dir (no recovery)', () => {
    tmp = makeTmpDir();
    assert.deepEqual(resolveSpawnCwd(tmp, tmp), { cwd: tmp, recovered: false });
  });

  it('recovers to the fallback root when the requested dir is gone', () => {
    tmp = makeTmpDir();
    const gone = path.join(tmp, 'deleted-subdir');
    const r = resolveSpawnCwd(gone, tmp);
    assert.equal(r.cwd, tmp);
    assert.equal(r.recovered, true);
    assert.equal(r.error, undefined);
  });

  it('errors when BOTH the requested dir and the fallback are gone', () => {
    const gone1 = path.join(os.tmpdir(), 'pm-u031w77-nope-1');
    const gone2 = path.join(os.tmpdir(), 'pm-u031w77-nope-2');
    const r = resolveSpawnCwd(gone1, gone2);
    assert.ok(r.error);
    assert.match(r.error!, /no longer exists/);
  });

  it('spawn short-circuits to failed (no child) when cwd + fallback are both gone', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    const orig = cwdHooks.realpathSync;
    const completed: any[] = [];
    pm.on('complete', (e) => completed.push(e));
    cwdHooks.realpathSync = () => { throw Object.assign(new Error('ENOENT'), { code: 'ENOENT' }); };
    let id: string;
    try {
      id = pm.spawn('echo should-not-run', { cwd: path.join(tmp, 'gone') });
    } finally {
      cwdHooks.realpathSync = orig;
    }
    const task = pm.get(id)!;
    assert.equal(task.status, 'failed');
    assert.equal(task.pid, 0, 'no child process was forked');
    assert.match(task.error ?? '', /no longer exists/);
    assert.equal(completed.length, 1, 'a completion is still surfaced for the refused spawn');
    assert.equal(completed[0].status, 'failed');
  });
});

// ---------------------------------------------------------------------------
// Gap #8 — AbortSignal-based cancellation (interrupt vs abort distinction)
// ---------------------------------------------------------------------------
describe('U031-w77 spawn — AbortSignal cancellation', () => {
  it('short-circuits (no fork, no output file) when the signal is already aborted', () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    const ac = new AbortController();
    ac.abort();
    const completed: any[] = [];
    pm.on('complete', (e) => completed.push(e));
    const id = pm.spawn('echo nope', { cwd: tmp, signal: ac.signal });
    const task = pm.get(id)!;
    assert.equal(task.status, 'killed');
    assert.equal(task.pid, 0);
    // No output file created (abort-before-spawn pays no fs cost).
    assert.equal(fs.existsSync(path.join(tmp, `${id}.output`)), false);
    assert.equal(completed.length, 1);
    assert.equal(completed[0].status, 'killed');
  });

  it('a REAL abort kills the running task', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    const ac = new AbortController();
    const id = pm.spawn('sleep 30', { cwd: tmp, signal: ac.signal });
    assert.equal(pm.getStatus(id), 'running');
    ac.abort();
    await waitFor(() => pm.getStatus(id) === 'killed');
    assert.equal(pm.getStatus(id), 'killed');
  });

  it("an abort whose reason is 'interrupt' does NOT kill the task (soft interrupt)", async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    const ac = new AbortController();
    const id = pm.spawn('sleep 30', { cwd: tmp, signal: ac.signal });
    await waitFor(() => pm.getStatus(id) === 'running');
    ac.abort('interrupt');
    // Give the listener a tick; it must NOT have flipped the task to killed.
    await new Promise((r) => setTimeout(r, 60));
    assert.equal(pm.getStatus(id), 'running', 'interrupt reason preserves the process');
    pm.kill(id); // clean up the real child
  });
});

// ---------------------------------------------------------------------------
// Gap #12 — stderr surfaced distinctly from stdout
// ---------------------------------------------------------------------------
describe('U031-w77 getStderr — distinct stderr capture', () => {
  it('captures stderr separately from the merged output', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    const id = pm.spawn("echo to-stdout; echo to-stderr 1>&2", { cwd: tmp });
    await waitFor(() => pm.getStatus(id) !== 'running');
    const merged = pm.getOutput(id, 100)!.output;
    const stderr = pm.getStderr(id);
    assert.match(merged, /to-stdout/);
    assert.match(merged, /to-stderr/, 'merged file still contains stderr (back-compat)');
    assert.match(stderr, /to-stderr/, 'stderr channel has the stderr line');
    assert.doesNotMatch(stderr, /to-stdout/, 'stderr channel does NOT contain stdout');
  });

  it('returns empty string for an unknown task / a task with no stderr', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    assert.equal(pm.getStderr('nope'), '');
    const id = pm.spawn('echo only-stdout', { cwd: tmp });
    await waitFor(() => pm.getStatus(id) !== 'running');
    assert.equal(pm.getStderr(id), '');
  });

  it('the retained stderr tail is bounded by STDERR_TAIL_CAP', () => {
    assert.ok(STDERR_TAIL_CAP > 0 && STDERR_TAIL_CAP <= 1024 * 1024);
  });
});

// ---------------------------------------------------------------------------
// Gaps #1/#2/#3 — two-tier agent interrupt (abort current turn vs kill)
// ---------------------------------------------------------------------------
describe('U031-w77 interruptAgentTurn — soft interrupt keeps the agent alive', () => {
  it('invokes the abort callback but leaves the agent running + emits interrupted', () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    let aborts = 0;
    const events: any[] = [];
    pm.on('interrupted', (e) => events.push(e));
    pm.registerAgent('agent-i', {
      command: '[agent] i', description: 'worker', outputPath: path.join(tmp, 'i.out'),
      abortFn: () => { aborts++; },
    });
    const ok = pm.interruptAgentTurn('agent-i');
    assert.equal(ok, true);
    assert.equal(aborts, 1, 'the per-turn abort callback fired');
    assert.equal(pm.getStatus('agent-i'), 'running', 'agent stays alive (not killed)');
    assert.equal(events.length, 1);
    assert.equal(events[0].taskId, 'agent-i');
  });

  it('refuses for a bash task, a terminal agent, and an unknown id', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    assert.equal(pm.interruptAgentTurn('missing'), false);
    const bash = pm.spawn('sleep 30', { cwd: tmp });
    assert.equal(pm.interruptAgentTurn(bash), false, 'a bash shell has no turn to interrupt');
    pm.kill(bash);
    pm.registerAgent('agent-done', {
      command: '[agent] d', description: 'd', outputPath: path.join(tmp, 'd.out'), abortFn: () => {},
    });
    pm.updateEntry('agent-done', { status: 'completed' });
    assert.equal(pm.interruptAgentTurn('agent-done'), false, 'a terminal agent cannot be interrupted');
  });
});

// ---------------------------------------------------------------------------
// Gap #18 — AI summary precedence + clobber-preserving merge
// ---------------------------------------------------------------------------
describe('U031-w77 effectiveSummary — AI summary precedence / preservation', () => {
  it('prefers the AI summary over the raw progress tick, and a raw tick never clobbers it', () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    pm.registerAgent('agent-s', {
      command: '[agent] s', description: 's', outputPath: path.join(tmp, 's.out'), abortFn: () => {},
    });
    pm.updateEntry('agent-s', { progressSummary: 'bash: npm test' });
    assert.equal(pm.effectiveSummary('agent-s'), 'bash: npm test', 'falls back to the raw summary');
    pm.updateEntry('agent-s', { agentSummary: 'Investigating the failing auth test.' });
    assert.equal(pm.effectiveSummary('agent-s'), 'Investigating the failing auth test.');
    // A later RAW progress tick must NOT clobber the authoritative AI summary.
    pm.updateEntry('agent-s', { progressSummary: 'bash: npm run build' });
    assert.equal(pm.effectiveSummary('agent-s'), 'Investigating the failing auth test.', 'AI summary preserved');
  });

  it('returns undefined for an unknown task', () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    assert.equal(pm.effectiveSummary('nope'), undefined);
  });
});

// ---------------------------------------------------------------------------
// Gaps #5/#20 — per-task attention state (needs_input / plan_ready)
// ---------------------------------------------------------------------------
describe('U031-w77 attentionState — needs_input / plan_ready', () => {
  it('updateEntry stores a valid attention state and ignores a bogus one', () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    pm.registerAgent('agent-a', {
      command: '[agent] a', description: 'a', outputPath: path.join(tmp, 'a.out'), abortFn: () => {},
    });
    pm.updateEntry('agent-a', { attentionState: 'plan_ready' });
    assert.equal(pm.get('agent-a')!.attentionState, 'plan_ready');
    pm.updateEntry('agent-a', { attentionState: 'bogus' as any });
    assert.equal(pm.get('agent-a')!.attentionState, 'plan_ready', 'invalid value ignored');
    assert.ok(TASK_ATTENTION_STATES.includes('needs_input'));
  });

  it('the stall watchdog flags a prompt-blocked task as needs_input', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    const id = pm.spawn("printf 'Continue? (y/n) '; sleep 30", { cwd: tmp });
    await waitFor(() => pm.getOutputSize(id) > 0);
    const t0 = Date.now();
    let last = 0;
    last = (pm as any).stallTick(id, last, t0);
    last = (pm as any).stallTick(id, last, t0 + STALL_THRESHOLD_MS + 1);
    assert.equal(pm.get(id)!.attentionState, 'needs_input', 'prompt-blocked task needs input');
    pm.kill(id);
  });
});

// ---------------------------------------------------------------------------
// E2E — task_kill interrupt option (tool-level wiring)
// ---------------------------------------------------------------------------
describe('U031-w77 task_kill — interrupt option (end-to-end)', () => {
  it('soft-interrupts a running agent via the tool', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    let aborted = false;
    pm.registerAgent('agent-k', {
      command: '[agent] k', description: 'k', outputPath: path.join(tmp, 'k.out'),
      abortFn: () => { aborted = true; },
    });
    const res = await taskKillTool.execute!({ task_id: 'agent-k', interrupt: true }, toolCtx(pm, tmp));
    assert.equal(res.isError, false);
    assert.match(res.output, /interrupted/i);
    assert.equal((res.metadata as any)?.reason, 'interrupted');
    assert.equal(aborted, true);
    assert.equal(pm.getStatus('agent-k'), 'running', 'agent kept alive');
  });

  it('refuses interrupt for a non-agent task with interrupt_unsupported', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    const id = pm.spawn('sleep 30', { cwd: tmp });
    const res = await taskKillTool.execute!({ task_id: id, interrupt: true }, toolCtx(pm, tmp));
    assert.equal(res.isError, true);
    assert.match(res.output, /interrupt_unsupported/);
    assert.equal((res.metadata as any)?.reason, 'interrupt_unsupported');
    pm.kill(id);
  });

  it('rejects a non-boolean interrupt argument', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    pm.registerAgent('agent-b', {
      command: '[agent] b', description: 'b', outputPath: path.join(tmp, 'b.out'), abortFn: () => {},
    });
    const res = await taskKillTool.execute!({ task_id: 'agent-b', interrupt: 'yes' as any }, toolCtx(pm, tmp));
    assert.equal(res.isError, true);
    assert.match(res.output, /invalid_input/);
  });
});

// ---------------------------------------------------------------------------
// E2E — task_output stderr section + task_list attention/summary render
// ---------------------------------------------------------------------------
describe('U031-w77 task_output / task_list — stderr + attention rendering', () => {
  it('task_output renders a distinct --- stderr --- section', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    const id = pm.spawn("echo on-stdout; echo on-stderr 1>&2", { cwd: tmp });
    await waitFor(() => pm.getStatus(id) !== 'running');
    const res = await taskOutputTool.execute!({ task_id: id }, toolCtx(pm, tmp));
    assert.match(res.output, /--- stderr ---/);
    assert.match(res.output, /on-stderr/);
  });

  it('task_list prefers the AI summary and shows the attention badge', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    pm.registerAgent('agent-l', {
      command: '[agent] worker', description: 'worker', outputPath: path.join(tmp, 'l.out'), abortFn: () => {},
    });
    pm.updateEntry('agent-l', {
      progressSummary: 'bash: raw tick',
      agentSummary: 'Refactoring the parser.',
      attentionState: 'needs_input',
    });
    const res = await taskListTool.execute!({}, toolCtx(pm, tmp));
    assert.match(res.output, /Refactoring the parser\./, 'AI summary surfaced');
    assert.doesNotMatch(res.output, /raw tick/, 'raw tick not surfaced when an AI summary exists');
    assert.match(res.output, /\[needs input\]/, 'attention badge rendered');
  });
});
