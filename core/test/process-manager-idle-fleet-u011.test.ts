import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ProcessManager } from '../src/process-manager';

// ---------------------------------------------------------------------------
// U011 — teammate idle-fleet coordination (parity: Claude Code's
// hasWorkingInProcessTeammates / waitForTeammatesToBecomeIdle, teammate.ts
// L205-292, and the TeamsDialog 'p' prune-idle handler, TeamsDialog.tsx L207-217).
//   - hasWorkingAgents(): running AND not idle (the 'alive but busy' distinction)
//   - waitForAllAgentsIdle(): resolve once every working agent goes idle/terminal
//   - pruneIdleAgents(): bulk-stop every idle teammate, leave busy ones alone
// Red-green: reverting any of the three methods fails the matching case here.
// ---------------------------------------------------------------------------

const tmpDirs: string[] = [];
function makeTmpDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-idlefleet-'));
  tmpDirs.push(d);
  return d;
}
function makePm(): ProcessManager {
  return new ProcessManager(makeTmpDir());
}
function registerAgent(pm: ProcessManager, id: string): void {
  pm.registerAgent(id, {
    command: `[agent] ${id}`,
    description: id,
    outputPath: path.join(makeTmpDir(), `${id}.out`),
    abortFn: () => { /* no-op */ },
  });
}

after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best-effort */ } }
});

describe('ProcessManager.hasWorkingAgents', () => {
  it('is false when there are no agents', () => {
    const pm = makePm();
    assert.equal(pm.hasWorkingAgents(), false);
    pm.cleanup();
  });

  it('is true for a freshly-registered (running, not idle) agent', () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    assert.equal(pm.hasWorkingAgents(), true);
    pm.cleanup();
  });

  it('flips to false once the only running agent is marked idle', () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    assert.equal(pm.markIdle('a1', true), true);
    assert.equal(pm.hasWorkingAgents(), false);
    // runningAgentCount still counts it as alive — the 'alive vs busy' distinction.
    assert.equal(pm.runningAgentCount(), 1);
    pm.cleanup();
  });

  it('is true while ANY agent is still working, even if another is idle', () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    registerAgent(pm, 'a2');
    pm.markIdle('a1', true);
    assert.equal(pm.hasWorkingAgents(), true);
    pm.markIdle('a2', true);
    assert.equal(pm.hasWorkingAgents(), false);
    pm.cleanup();
  });

  it('does not count a background bash shell as a working agent', async () => {
    const pm = makePm();
    const bashId = pm.spawn('sleep 5', { cwd: makeTmpDir() });
    assert.equal(pm.hasWorkingAgents(), false);
    pm.kill(bashId);
    pm.cleanup();
    await new Promise(r => setTimeout(r, 30));
  });
});

describe('ProcessManager.waitForAllAgentsIdle', () => {
  it('resolves immediately when no agent is working', async () => {
    const pm = makePm();
    await assert.doesNotReject(pm.waitForAllAgentsIdle());
    pm.cleanup();
  });

  it('resolves only after every working agent goes idle (event-driven, no polling)', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    registerAgent(pm, 'a2');
    let resolved = false;
    const p = pm.waitForAllAgentsIdle().then(() => { resolved = true; });
    // Still pending — both agents are busy.
    await new Promise(r => setTimeout(r, 10));
    assert.equal(resolved, false);
    pm.markIdle('a1', true);
    await new Promise(r => setTimeout(r, 10));
    assert.equal(resolved, false); // a2 still working
    pm.markIdle('a2', true);
    await p;
    assert.equal(resolved, true);
    pm.cleanup();
  });

  it('resolves when a working agent transitions to terminal (killed), not only idle', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    const p = pm.waitForAllAgentsIdle();
    pm.kill('a1');
    await assert.doesNotReject(p);
    pm.cleanup();
  });

  it('honors the timeout cap when an agent never goes idle', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    const start = Date.now();
    await pm.waitForAllAgentsIdle(40);
    assert.ok(Date.now() - start >= 30, 'waited at least ~the cap');
    // The agent is still working — the wait returned on the cap, not on an idle.
    assert.equal(pm.hasWorkingAgents(), true);
    pm.cleanup();
  });
});

describe('ProcessManager.pruneIdleAgents', () => {
  it('returns an empty list when no agent is idle', () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    assert.deepEqual(pm.pruneIdleAgents(), []);
    assert.equal(pm.getStatus('a1'), 'running');
    pm.cleanup();
  });

  it('kills every idle agent and leaves busy agents running', () => {
    const pm = makePm();
    registerAgent(pm, 'idle-1');
    registerAgent(pm, 'idle-2');
    registerAgent(pm, 'busy-1');
    pm.markIdle('idle-1', true);
    pm.markIdle('idle-2', true);

    const pruned = pm.pruneIdleAgents();
    assert.equal(pruned.length, 2);
    assert.deepEqual(pruned.map(p => p.taskId).sort(), ['idle-1', 'idle-2']);
    assert.equal(pm.getStatus('idle-1'), 'killed');
    assert.equal(pm.getStatus('idle-2'), 'killed');
    assert.equal(pm.getStatus('busy-1'), 'running');
    pm.cleanup();
  });

  it('does not touch background bash shells even when no agent work is in flight', async () => {
    const pm = makePm();
    const bashId = pm.spawn('sleep 5', { cwd: makeTmpDir() });
    assert.deepEqual(pm.pruneIdleAgents(), []);
    assert.equal(pm.getStatus(bashId), 'running');
    pm.kill(bashId);
    pm.cleanup();
    await new Promise(r => setTimeout(r, 30));
  });
});
