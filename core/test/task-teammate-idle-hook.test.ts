import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { markTeammateIdle } from '../src/tools/task';
import { teamCreateTool, getCurrentTeam, resetTeam, registerTeamMember } from '../src/tools/team';
import { HookManager, type TeammateContext } from '../src/hooks';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U007 (w65) — end-to-end wiring: a finished teammate fires TeammateIdle.
// markTeammateIdle is the exact helper the subagent-finish paths in task.ts
// call; this drives it through a real team + real HookManager.
// ---------------------------------------------------------------------------

let tmp: string;

describe('U007/w65 — markTeammateIdle fires the TeammateIdle hook', () => {
  beforeEach(() => { resetTeam(); tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-ti-e2e-')); });
  afterEach(() => { resetTeam(); try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } });

  function ctx(hm?: HookManager): ToolContext {
    return {
      cwd: tmp,
      sessionId: 'sess-1',
      abortSignal: new AbortController().signal,
      _hookManager: hm,
    } as unknown as ToolContext;
  }

  it('marks the teammate idle AND fires TeammateIdle with teammate/team identity', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'worker', agentId: 'a-1' });

    const hm = new HookManager();
    const fired: TeammateContext[] = [];
    hm.onTeammateIdle(async (c) => { fired.push(c); });

    markTeammateIdle(ctx(hm), 'a-1');
    // Let the fire-and-forget hook settle.
    await new Promise((r) => setTimeout(r, 10));

    const member = getCurrentTeam()!.members.find((m) => m.agentId === 'a-1')!;
    assert.equal(member.isActive, false, 'teammate marked idle');
    assert.equal(fired.length, 1, 'TeammateIdle hook fired exactly once');
    assert.deepEqual(fired[0], { teammateName: 'worker', teamName: 'sq' });
  });

  it('does not fire TeammateIdle when the id is not a team member', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    const hm = new HookManager();
    let count = 0;
    hm.onTeammateIdle(async () => { count++; });

    markTeammateIdle(ctx(hm), 'ghost');
    await new Promise((r) => setTimeout(r, 10));
    assert.equal(count, 0, 'no TeammateIdle for an unknown teammate');
  });

  it('is a no-op (no throw) when no hook manager is present', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'worker', agentId: 'a-1' });
    assert.doesNotThrow(() => markTeammateIdle(ctx(undefined), 'a-1'));
    const member = getCurrentTeam()!.members.find((m) => m.agentId === 'a-1')!;
    assert.equal(member.isActive, false, 'still marks idle even without a hook manager');
  });
});
