import { describe, it, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ProcessManager, formatStoppedByUserNotification } from '../src/process-manager';

// ---------------------------------------------------------------------------
// Parity: single-shot stop of ALL running background agents (ESC / kill-all)
// + ONE aggregate "stopped by the user" model-facing notification. Mirrors
// Claude Code's useCancelRequest.killAllAgentsAndNotify (filters to local_agent
// running tasks, kills them all, enqueues a single summary).
// Red-green: reverting killAllAgents()/formatStoppedByUserNotification fails here.
// ---------------------------------------------------------------------------

const tmpDirs: string[] = [];
function makeTmpDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-killall-'));
  tmpDirs.push(d);
  return d;
}

function makePm(): ProcessManager {
  return new ProcessManager(makeTmpDir());
}

afterEach(() => { /* nothing persistent between cases beyond tmp dirs */ });
after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best-effort */ } }
});

describe('formatStoppedByUserNotification', () => {
  it('returns null for an empty list', () => {
    assert.equal(formatStoppedByUserNotification([]), null);
  });

  it('uses the singular wording for exactly one agent', () => {
    assert.equal(
      formatStoppedByUserNotification(['build the app']),
      'Background agent "build the app" was stopped by the user.',
    );
  });

  it('uses the aggregate wording with a quoted, comma-joined list for many', () => {
    assert.equal(
      formatStoppedByUserNotification(['alpha', 'beta', 'gamma']),
      '3 background agents were stopped by the user: "alpha", "beta", "gamma".',
    );
  });
});

describe('ProcessManager.killAllAgents', () => {
  function registerAgent(pm: ProcessManager, id: string, description: string): void {
    pm.registerAgent(id, {
      command: `[agent] ${description}`,
      description,
      outputPath: path.join(makeTmpDir(), `${id}.out`),
      abortFn: () => { /* no-op abort */ },
    });
  }

  it('kills every running agent and returns a single aggregate notification', () => {
    const pm = makePm();
    registerAgent(pm, 'a1', 'first');
    registerAgent(pm, 'a2', 'second');

    const res = pm.killAllAgents();
    assert.equal(res.stopped.length, 2);
    assert.equal(pm.getStatus('a1'), 'killed');
    assert.equal(pm.getStatus('a2'), 'killed');
    assert.equal(
      res.notification,
      '2 background agents were stopped by the user: "first", "second".',
    );
    pm.cleanup();
  });

  it('returns null notification + empty list when no agent is running', () => {
    const pm = makePm();
    const res = pm.killAllAgents();
    assert.deepEqual(res.stopped, []);
    assert.equal(res.notification, null);
    pm.cleanup();
  });

  it('invokes each agent abort callback exactly once', () => {
    const pm = makePm();
    let aborts = 0;
    pm.registerAgent('a1', {
      command: '[agent] work',
      description: 'work',
      outputPath: path.join(makeTmpDir(), 'a1.out'),
      abortFn: () => { aborts++; },
    });
    const res = pm.killAllAgents();
    assert.equal(aborts, 1);
    assert.equal(res.stopped.length, 1);
    // A second kill-all is a no-op: the agent is already terminal.
    assert.deepEqual(pm.killAllAgents().stopped, []);
    assert.equal(aborts, 1);
    pm.cleanup();
  });

  it('does NOT kill background bash shells or MCP monitors (agent-type filter)', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1', 'the agent');
    // A real background bash shell (taskType 'bash').
    const bashId = pm.spawn('sleep 5', { cwd: makeTmpDir() });
    // An MCP monitor (not an agent, not a bash shell).
    const monId = pm.monitorMcp('srv', async () => true, 60_000);

    const res = pm.killAllAgents();
    assert.equal(res.stopped.length, 1);
    assert.equal(res.stopped[0].taskId, 'a1');
    assert.equal(pm.getStatus('a1'), 'killed');
    assert.equal(pm.getStatus(bashId), 'running');
    assert.equal(pm.getStatus(monId), 'running');

    pm.cleanup();
    await new Promise(r => setTimeout(r, 50));
  });

  it('freezes elapsed time (endTime) for killed agents', () => {
    const pm = makePm();
    registerAgent(pm, 'a1', 'work');
    pm.killAllAgents();
    const snap = pm.get('a1');
    assert.ok(snap);
    assert.equal(typeof snap!.endTime, 'number');
    pm.cleanup();
  });
});
