import { describe, it, beforeEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  taskTool,
  uniqueAgentName,
  getAgentByName,
  getAgentNameById,
  resetAgentRegistry,
} from '../src/tools/task';
import { ProcessManager, isTerminalTaskStatus } from '../src/process-manager';
import type { ToolContext } from '../src/types';

// ===========================================================================
// U011-w96 — status-aware agent-name reclamation (gaps #13/#14)
//
// A name whose holder has TERMINATED (completed/failed/killed) is reclaimed for
// the new live agent instead of being pushed to a suffixed alias, so a message /
// resume aimed at the canonical name routes to the running instance — not the dead
// one (parity: the reference's findTeammateTaskByAgentId preferring the RUNNING task
// over a stale killed/completed one, InProcessTeammateTask.tsx:L92-108).
//
// Red-green: reverting the `isTerminal?.(existing) -> return name` reclamation in
// uniqueAgentName (and the probe threaded at the spawn site) makes the reclaim
// cases fall back to suffixing and these tests fail.
// ===========================================================================

const tmpDirs: string[] = [];
function makeTmpDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-reclaim-'));
  tmpDirs.push(d);
  return d;
}
after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best-effort */ } }
});

async function spawnNamed(
  name: string,
  cwd: string,
  pm?: ProcessManager,
): Promise<ToolResultLike> {
  async function* fakeSubmit() {
    yield { type: 'text_delta', text: 'ok' } as never;
  }
  const res = await taskTool.execute(
    { prompt: 'do a thing', mode: 'explore', name },
    {
      cwd,
      abortSignal: new AbortController().signal,
      processManager: pm,
      _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
      _hookManager: ({
        executePreToolUse: async () => ({ action: 'allow' }),
        executeSubagentStart: async () => {},
        executeSubagentStop: async () => {},
      } as unknown as ToolContext['_hookManager']),
      _engineFactory: () => ({ submit: fakeSubmit }),
    } as unknown as ToolContext,
  );
  return res as ToolResultLike;
}

interface ToolResultLike { isError?: boolean; output: string }

describe('U011-w96 — uniqueAgentName status-aware reclamation', () => {
  beforeEach(() => resetAgentRegistry());

  it('reclaims the base name verbatim when the current holder is terminal', async () => {
    const cwd = makeTmpDir();
    await spawnNamed('scout', cwd);
    const idA = getAgentByName('scout');
    assert.ok(idA, 'first scout registered under the base name');
    // Holder is dead -> the base name is reclaimed for the new agent.
    assert.equal(uniqueAgentName('scout', id => id === idA), 'scout');
    // Holder is alive -> the respawn is pushed to a suffixed alias.
    assert.equal(uniqueAgentName('scout', () => false), 'scout-2');
  });

  it('reclaims a terminal SUFFIXED slot when the base name holder is still live', async () => {
    const cwd = makeTmpDir();
    await spawnNamed('w', cwd);          // w   -> idA
    await spawnNamed('w', cwd, undefined); // w-2 -> idB (no probe => suffix)
    const idA = getAgentByName('w');
    const idB = getAgentByName('w-2');
    assert.ok(idA && idB && idA !== idB);
    // Base (idA) alive, suffixed (idB) dead -> reclaim the lowest dead slot (w-2).
    assert.equal(uniqueAgentName('w', id => id === idB), 'w-2');
    // Both alive -> skip to the next free suffix.
    assert.equal(uniqueAgentName('w', () => false), 'w-3');
  });

  it('falls back to pure suffixing when no status probe is supplied (back-compat)', async () => {
    const cwd = makeTmpDir();
    await spawnNamed('agent', cwd);
    // Without a probe the reclamation cannot prove the holder is dead -> suffix.
    assert.equal(uniqueAgentName('agent'), 'agent-2');
  });
});

describe('U011-w96 — kill+respawn reclaims the canonical address end to end', () => {
  beforeEach(() => resetAgentRegistry());

  it('a respawn of a KILLED named agent takes the base name (not a -2 alias)', async () => {
    const cwd = makeTmpDir();
    const pm = new ProcessManager(cwd);

    await spawnNamed('scout', cwd, pm);
    const idA = getAgentByName('scout');
    assert.ok(idA, 'first scout addressable by its base name');

    // Simulate the first agent being killed: its process-manager entry goes terminal.
    pm.registerAgent(idA!, {
      command: '[agent] scout',
      description: 'scout',
      outputPath: path.join(cwd, 'scout.out'),
      abortFn: () => { /* no-op */ },
    });
    pm.updateEntry(idA!, { status: 'killed', exitCode: 1 });
    assert.ok(isTerminalTaskStatus(pm.getStatus(idA!)), 'first scout is terminal');

    // Respawn with the SAME name while the dead one lingers in the registry.
    await spawnNamed('scout', cwd, pm);
    const idB = getAgentByName('scout');

    assert.ok(idB, 'scout still addressable after respawn');
    assert.notEqual(idB, idA, 'the base name now points at the LIVE respawned agent');
    assert.equal(getAgentByName('scout-2'), undefined, 'no suffixed alias was created');
    // The dead agent lost the canonical reverse-lookup; only the live one holds it.
    assert.equal(getAgentNameById(idB!), 'scout');
    assert.notEqual(getAgentNameById(idA!), 'scout');
  });

  it('a respawn of a still-RUNNING named agent is suffixed (no reclamation)', async () => {
    const cwd = makeTmpDir();
    const pm = new ProcessManager(cwd);

    await spawnNamed('builder', cwd, pm);
    const idA = getAgentByName('builder');
    assert.ok(idA);
    pm.registerAgent(idA!, {
      command: '[agent] builder',
      description: 'builder',
      outputPath: path.join(cwd, 'builder.out'),
      abortFn: () => { /* no-op */ },
    });
    // Leave it RUNNING (default status) — not terminal.
    assert.ok(!isTerminalTaskStatus(pm.getStatus(idA!)), 'first builder still running');

    await spawnNamed('builder', cwd, pm);
    assert.equal(getAgentByName('builder'), idA, 'live agent keeps the base name');
    assert.ok(getAgentByName('builder-2'), 'respawn got a suffixed alias');
  });
});
