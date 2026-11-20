import { describe, it, afterEach, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  ProcessManager,
  pmTraceHooks,
  type PmLifecyclePhase,
} from '../src/process-manager';

// ---------------------------------------------------------------------------
// U031 / w79: cooperative graceful-shutdown flag, idle lifecycle (event-driven
// waitForIdle / onIdleCallbacks), and the gated per-transition lifecycle trace.
// Parity with the reference's InProcessTeammateTask shutdownRequested / isIdle /
// onIdleCallbacks and InProcessBackend's gated lifecycle trace.
// Red-green: reverting requestShutdown / markIdle / waitForIdle / traceLifecycle
// fails the matching cases below.
// ---------------------------------------------------------------------------

const tmpDirs: string[] = [];
function makeTmpDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-w79-'));
  tmpDirs.push(d);
  return d;
}
function makePm(): ProcessManager {
  return new ProcessManager(makeTmpDir());
}
function registerAgent(pm: ProcessManager, id: string, description = id): void {
  pm.registerAgent(id, {
    command: `[agent] ${description}`,
    description,
    outputPath: path.join(makeTmpDir(), `${id}.out`),
    abortFn: () => { /* no-op abort */ },
  });
}

afterEach(() => { pmTraceHooks.onLifecycle = () => { /* reset to default no-op */ }; });
after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best-effort */ } }
});

describe('ProcessManager.requestShutdown / isShutdownRequested', () => {
  it('sets the cooperative flag for a running agent and is idempotent', () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    assert.equal(pm.isShutdownRequested('a1'), false);
    assert.equal(pm.requestShutdown('a1'), true);
    assert.equal(pm.isShutdownRequested('a1'), true);
    // Idempotent: a second request before the agent acts is a no-op.
    assert.equal(pm.requestShutdown('a1'), false);
    // Surfaced on the public snapshot for task_list rendering.
    assert.equal(pm.get('a1')?.shutdownRequested, true);
  });

  it('refuses an unknown task, a non-running agent, and a bash task', () => {
    const pm = makePm();
    assert.equal(pm.requestShutdown('nope'), false);
    assert.equal(pm.isShutdownRequested('nope'), false);
    // A terminated agent is not running.
    registerAgent(pm, 'a2');
    pm.kill('a2');
    assert.equal(pm.requestShutdown('a2'), false);
    // A bash task has no turn concept.
    const bashId = pm.spawn('echo hi', { cwd: makeTmpDir() });
    assert.equal(pm.requestShutdown(bashId), false);
    pm.cleanup();
  });

  it('emits a shutdown-requested event', () => {
    const pm = makePm();
    registerAgent(pm, 'a3');
    let seen: { taskId: string } | undefined;
    pm.on('shutdown-requested', (e) => { seen = e; });
    pm.requestShutdown('a3');
    assert.equal(seen?.taskId, 'a3');
  });
});

describe('ProcessManager.markIdle / isIdle / waitForIdle', () => {
  it('toggles the idle flag for a running agent and surfaces it', () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    assert.equal(pm.isIdle('a1'), false);
    assert.equal(pm.markIdle('a1', true), true);
    assert.equal(pm.isIdle('a1'), true);
    assert.equal(pm.get('a1')?.idle, true);
    // No-op when already at the requested value.
    assert.equal(pm.markIdle('a1', true), false);
    // Back to busy.
    assert.equal(pm.markIdle('a1', false), true);
    assert.equal(pm.isIdle('a1'), false);
  });

  it('refuses markIdle for unknown / non-agent / terminal tasks', () => {
    const pm = makePm();
    assert.equal(pm.markIdle('nope', true), false);
    const bashId = pm.spawn('echo hi', { cwd: makeTmpDir() });
    assert.equal(pm.markIdle(bashId, true), false);
    registerAgent(pm, 'a2');
    pm.kill('a2');
    assert.equal(pm.markIdle('a2', true), false);
    pm.cleanup();
  });

  it('waitForIdle resolves immediately for unknown, terminal, and already-idle tasks', async () => {
    const pm = makePm();
    await assert.doesNotReject(pm.waitForIdle('nope'));
    registerAgent(pm, 'a1');
    pm.markIdle('a1', true);
    await assert.doesNotReject(pm.waitForIdle('a1')); // already idle
    registerAgent(pm, 'a2');
    pm.kill('a2');
    await assert.doesNotReject(pm.waitForIdle('a2')); // terminal
  });

  it('waitForIdle resolves event-driven on the next markIdle(true) — no polling', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    let resolved = false;
    const p = pm.waitForIdle('a1').then(() => { resolved = true; });
    // Not yet idle → still pending.
    await new Promise((r) => setTimeout(r, 10));
    assert.equal(resolved, false);
    pm.markIdle('a1', true);
    await p;
    assert.equal(resolved, true);
  });

  it('waitForIdle is woken by a kill (a killed agent is idle forever)', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    const p = pm.waitForIdle('a1');
    pm.kill('a1');
    await assert.doesNotReject(p);
  });

  it('waitForIdle is woken by a terminal updateEntry transition', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    const p = pm.waitForIdle('a1');
    pm.updateEntry('a1', { status: 'completed' });
    await assert.doesNotReject(p);
  });

  it('clears waiters after firing so a second idle transition has none pending', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    let count = 0;
    const p = pm.waitForIdle('a1').then(() => { count++; });
    pm.markIdle('a1', true);
    await p;
    assert.equal(count, 1);
    // Toggle busy then idle again — the already-resolved waiter is not re-fired.
    pm.markIdle('a1', false);
    pm.markIdle('a1', true);
    await new Promise((r) => setTimeout(r, 10));
    assert.equal(count, 1);
  });
});

describe('ProcessManager gated lifecycle trace (pmTraceHooks)', () => {
  it('emits a trace on register / idle / shutdown / interrupt / kill', () => {
    const pm = makePm();
    const phases: PmLifecyclePhase[] = [];
    pmTraceHooks.onLifecycle = (phase) => { phases.push(phase); };
    registerAgent(pm, 'a1');
    pm.markIdle('a1', true);
    pm.requestShutdown('a1');
    pm.interruptAgentTurn('a1');
    pm.kill('a1');
    assert.ok(phases.includes('register'), 'register traced');
    assert.ok(phases.includes('idle'), 'idle traced');
    assert.ok(phases.includes('shutdown'), 'shutdown traced');
    assert.ok(phases.includes('interrupt'), 'interrupt traced');
    assert.ok(phases.includes('kill'), 'kill traced');
  });

  it('emits spawn + start traces for a pending→running agent', () => {
    const pm = makePm();
    const phases: PmLifecyclePhase[] = [];
    pmTraceHooks.onLifecycle = (phase) => { phases.push(phase); };
    const bashId = pm.spawn('echo hi', { cwd: makeTmpDir() });
    assert.ok(phases.includes('spawn'), 'spawn traced');
    assert.ok(typeof bashId === 'string');
    pm.registerAgent('p1', { command: '[agent] p', description: 'p', outputPath: path.join(makeTmpDir(), 'p.out'), abortFn: () => {}, pending: true });
    assert.equal(pm.markRunning('p1'), true);
    assert.ok(phases.includes('start'), 'start traced');
    pm.cleanup();
  });

  it('a throwing trace sink never breaks a transition', () => {
    const pm = makePm();
    pmTraceHooks.onLifecycle = () => { throw new Error('sink boom'); };
    assert.doesNotThrow(() => registerAgent(pm, 'a1'));
    assert.doesNotThrow(() => pm.kill('a1'));
  });

  it('defaults to a no-op sink (no throw with no listener)', () => {
    const pm = makePm();
    assert.doesNotThrow(() => registerAgent(pm, 'a1'));
  });
});
