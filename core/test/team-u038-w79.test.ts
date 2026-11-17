import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  teamCreateTool,
  resetTeam,
  getCurrentTeam,
  isTaskListVisibleForSelf,
  getSelfRole,
  setDynamicTeamContext,
  clearDynamicTeamContext,
} from '../src/tools/team';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U038 (w79) — team-role task-list visibility gate.
// Parity: Claude Code's useTasksV2 hook gates the persistent shared task-list
// panel with `!teamContext || isTeamLead(teamContext)` — inside a team only the
// LEAD sees the shared list; a non-lead teammate suppresses it. AMI's
// isTaskListVisibleForSelf() is the core predicate that drives the same gate.
// ---------------------------------------------------------------------------

let tmp: string;

function ctx(sessionId = 'sess-vis'): ToolContext {
  return { cwd: tmp, sessionId, abortSignal: new AbortController().signal } as ToolContext;
}

describe('U038 w79 — isTaskListVisibleForSelf (team-lead task-list visibility gate)', () => {
  beforeEach(() => { resetTeam(); clearDynamicTeamContext(); tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-tlvis-')); });
  afterEach(() => { resetTeam(); clearDynamicTeamContext(); try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } });

  it('a standalone (non-team) session always sees the task-list panel', () => {
    assert.equal(getSelfRole(), 'none');
    assert.equal(isTaskListVisibleForSelf(), true);
  });

  it('the team LEAD sees the shared task-list panel', async () => {
    const res = await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(getCurrentTeam());
    // The creating session is bound as the lead.
    assert.equal(getSelfRole(), 'lead');
    assert.equal(isTaskListVisibleForSelf(), true);
  });

  it('a non-lead teammate suppresses the shared task-list panel', () => {
    // A spawned/joined teammate installs a runtime team context whose agentId is
    // NOT the lead's — it resolves to the 'teammate' role.
    setDynamicTeamContext({
      agentId: 'worker@sq',
      agentName: 'worker',
      teamName: 'sq',
      planModeRequired: false,
    });
    assert.equal(getSelfRole(), 'teammate');
    assert.equal(isTaskListVisibleForSelf(), false);
  });

  it('the gate follows the role: clearing the teammate context restores visibility', () => {
    setDynamicTeamContext({ agentId: 'worker@sq', agentName: 'worker', teamName: 'sq', planModeRequired: false });
    assert.equal(isTaskListVisibleForSelf(), false);
    clearDynamicTeamContext();
    assert.equal(getSelfRole(), 'none');
    assert.equal(isTaskListVisibleForSelf(), true);
  });
});
