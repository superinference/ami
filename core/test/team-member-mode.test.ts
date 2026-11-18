import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  teamCreateTool,
  getCurrentTeam,
  resetTeam,
  registerTeamMember,
  setMemberMode,
  cycleMemberMode,
  getNextPermissionMode,
} from '../src/tools/team';
import { PERMISSION_MODES } from '../src/permissions';
import type { ToolContext } from '../src/types';

let tmp: string;
function ctx(): ToolContext {
  return { cwd: tmp, sessionId: 'sess-1', abortSignal: new AbortController().signal } as ToolContext;
}

/**
 * persistTeam flushes fire-and-forget under a cross-process file lock, so the
 * on-disk write lands asynchronously. Poll the team file until `pred` holds
 * (bounded) instead of a fixed sleep, so the assertion is robust to lock-acquire
 * latency under heavy parallel test load rather than racing a single 50ms window.
 */
async function readTeamWhen(
  teamFile: string,
  pred: (raw: { members: { name: string; permissionMode?: string }[] }) => boolean,
): Promise<{ members: { name: string; permissionMode?: string }[] }> {
  for (let i = 0; i < 100; i++) {
    try {
      const raw = JSON.parse(fs.readFileSync(teamFile, 'utf-8'));
      if (pred(raw)) return raw;
    } catch { /* not written / mid-write yet — retry */ }
    await new Promise(r => setTimeout(r, 20));
  }
  return JSON.parse(fs.readFileSync(teamFile, 'utf-8'));
}

describe('per-teammate permission mode (gap: setMemberMode + getNextPermissionMode cycle)', () => {
  beforeEach(() => { resetTeam(); tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-mode-')); });
  afterEach(() => { resetTeam(); try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } });

  it('getNextPermissionMode cycles through every mode and wraps around', () => {
    let m: string | undefined = PERMISSION_MODES[0];
    const seen: string[] = [];
    for (let i = 0; i < PERMISSION_MODES.length; i++) { seen.push(m!); m = getNextPermissionMode(m); }
    assert.deepEqual([...seen].sort(), [...PERMISSION_MODES].sort());
    assert.equal(getNextPermissionMode(PERMISSION_MODES[PERMISSION_MODES.length - 1]), PERMISSION_MODES[0]);
  });

  it('getNextPermissionMode on an unknown value starts the cycle at the first mode', () => {
    assert.equal(getNextPermissionMode('bogus'), PERMISSION_MODES[0]);
    assert.equal(getNextPermissionMode(undefined), PERMISSION_MODES[0]);
  });

  it('setMemberMode sets and persists a single teammate mode', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'worker', agentId: 'a-1' });
    const applied = setMemberMode('worker', 'auto-allow');
    assert.equal(applied, 'auto-allow');
    const team = getCurrentTeam()!;
    assert.equal(team.members.find(m => m.name === 'worker')!.permissionMode, 'auto-allow');
    // Persisted to disk (persistTeam flushes fire-and-forget under a file lock);
    // poll the team file until the mode lands rather than racing a fixed window.
    const teamFile = path.join(tmp, '.ami', 'teams', 'sq', 'team.json');
    const raw = await readTeamWhen(teamFile, (r) => r.members.find((m) => m.name === 'worker')?.permissionMode === 'auto-allow');
    assert.equal(raw.members.find((m) => m.name === 'worker')!.permissionMode, 'auto-allow');
  });

  it('setMemberMode rejects an invalid mode and an unknown member', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'worker', agentId: 'a-1' });
    assert.equal(setMemberMode('worker', 'nonsense'), null);
    assert.equal(setMemberMode('ghost', 'ask'), null);
  });

  it('setMemberMode / cycleMemberMode are no-ops without an active team', () => {
    assert.equal(setMemberMode('x', 'ask'), null);
    assert.equal(cycleMemberMode('x'), null);
  });

  it('cycleMemberMode advances one teammate to the next mode', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'worker', agentId: 'a-1', permissionMode: 'ask' });
    const next = cycleMemberMode('worker');
    assert.equal(next, getNextPermissionMode('ask'));
    assert.equal(getCurrentTeam()!.members.find(m => m.name === 'worker')!.permissionMode, next!);
  });

  it('cycleMemberMode returns null for an unknown member', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    assert.equal(cycleMemberMode('ghost'), null);
  });
});
