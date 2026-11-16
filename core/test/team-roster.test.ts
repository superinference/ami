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
  setTeamMemberActive,
  setMemberMode,
  setMultipleMemberModes,
  cycleAllMemberModes,
  getNextPermissionMode,
  removeTeamMember,
  getTeammateStatuses,
  getTeamSummary,
  readTeamFile,
} from '../src/tools/team';
import { TEAM_LEAD_NAME } from '../src/tools/send-message';
import { normalizePermissionMode } from '../src/permissions';
import type { ToolContext } from '../src/types';

let tmp: string;
function ctx(): ToolContext {
  return { cwd: tmp, sessionId: 'sess-1', abortSignal: new AbortController().signal } as ToolContext;
}
function teamJson(): Record<string, unknown> {
  return JSON.parse(fs.readFileSync(path.join(tmp, '.ami', 'teams', 'sq', 'team.json'), 'utf-8'));
}
function settle(): Promise<void> { return new Promise(r => setTimeout(r, 60)); }

describe('team roster: batch modes, cycle-all, status discovery, summary, removal', () => {
  beforeEach(() => { resetTeam(); tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-roster-')); });
  afterEach(() => { resetTeam(); try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } });

  // ── Gap: setMultipleMemberModes (atomic batch, Map lookup, anyChanged) ──────
  it('setMultipleMemberModes applies many modes in one atomic write and returns change count', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'a', agentId: 'id-a', permissionMode: 'ask' });
    registerTeamMember({ name: 'b', agentId: 'id-b', permissionMode: 'ask' });
    const changed = setMultipleMemberModes([
      { nameOrId: 'a', mode: 'auto-allow' },
      { nameOrId: 'id-b', mode: 'plan' }, // keyed by agentId
    ]);
    assert.equal(changed, 2);
    const team = getCurrentTeam()!;
    assert.equal(team.members.find(m => m.name === 'a')!.permissionMode, 'auto-allow');
    assert.equal(team.members.find(m => m.name === 'b')!.permissionMode, 'plan');
    await settle();
    const raw = teamJson();
    const members = raw.members as Array<{ name: string; permissionMode: string }>;
    assert.equal(members.find(m => m.name === 'a')!.permissionMode, 'auto-allow');
    assert.equal(members.find(m => m.name === 'b')!.permissionMode, 'plan');
  });

  it('setMultipleMemberModes write-if-changed: returns 0 and skips when nothing changes', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'a', agentId: 'id-a', permissionMode: 'ask' });
    assert.equal(setMultipleMemberModes([{ nameOrId: 'a', mode: 'ask' }]), 0);
    // invalid modes are skipped, not fatal
    assert.equal(setMultipleMemberModes([{ nameOrId: 'a', mode: 'bogus' }]), 0);
    assert.equal(getCurrentTeam()!.members.find(m => m.name === 'a')!.permissionMode, 'ask');
  });

  it('setMultipleMemberModes is a no-op (0) without an active team', () => {
    assert.equal(setMultipleMemberModes([{ nameOrId: 'a', mode: 'ask' }]), 0);
  });

  // ── Gap: cycleAllMemberModes (reset-to-default-when-divergent) ──────────────
  it('cycleAllMemberModes resets all to default when modes diverge', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'a', agentId: 'id-a', permissionMode: 'ask' });
    registerTeamMember({ name: 'b', agentId: 'id-b', permissionMode: 'plan' });
    const target = cycleAllMemberModes();
    const resetMode = normalizePermissionMode('default')!;
    assert.equal(target, resetMode);
    const team = getCurrentTeam()!;
    assert.equal(team.members.find(m => m.name === 'a')!.permissionMode, resetMode);
    assert.equal(team.members.find(m => m.name === 'b')!.permissionMode, resetMode);
  });

  it('cycleAllMemberModes advances all to the next mode when they already agree', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'a', agentId: 'id-a', permissionMode: 'ask' });
    registerTeamMember({ name: 'b', agentId: 'id-b', permissionMode: 'ask' });
    const target = cycleAllMemberModes();
    assert.equal(target, getNextPermissionMode('ask'));
    const team = getCurrentTeam()!;
    assert.equal(team.members.find(m => m.name === 'a')!.permissionMode, target!);
    assert.equal(team.members.find(m => m.name === 'b')!.permissionMode, target!);
  });

  it('cycleAllMemberModes excludes the lead and returns null when there are no teammates', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    // only the lead is present
    assert.equal(cycleAllMemberModes(), null);
    // lead mode is never cycled
    assert.equal(getCurrentTeam()!.members.find(m => m.name === TEAM_LEAD_NAME)!.permissionMode, undefined);
  });

  it('cycleAllMemberModes is null without an active team', () => {
    assert.equal(cycleAllMemberModes(), null);
  });

  // ── Gap: getTeammateStatuses (rich status, lead-exclusion, isActive default) ─
  it('getTeammateStatuses projects non-lead members with running/idle derived from isActive', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'a', agentId: 'id-a', agentType: 'coder', model: 'opus', cwd: '/work', permissionMode: 'ask' });
    registerTeamMember({ name: 'b', agentId: 'id-b' }); // isActive undefined -> defaults running
    setTeamMemberActive('a', false); // idle, stamps idleSince
    const statuses = getTeammateStatuses();
    assert.equal(statuses.length, 2); // lead excluded
    assert.ok(!statuses.some(s => s.name === TEAM_LEAD_NAME));
    const a = statuses.find(s => s.name === 'a')!;
    assert.equal(a.status, 'idle');
    assert.equal(a.agentType, 'coder');
    assert.equal(a.model, 'opus');
    assert.equal(a.cwd, '/work');
    assert.ok(typeof a.idleSince === 'string');
    const b = statuses.find(s => s.name === 'b')!;
    assert.equal(b.status, 'running'); // undefined isActive => running
    assert.equal(b.idleSince, undefined);
  });

  it('getTeammateStatuses can project a disk-loaded team file and returns [] when no team', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'a', agentId: 'id-a' });
    await settle();
    const loaded = readTeamFile(path.join(tmp, '.ami', 'teams', 'sq'))!;
    resetTeam();
    assert.deepEqual(getTeammateStatuses(), []); // no active team
    const statuses = getTeammateStatuses(loaded);
    assert.equal(statuses.length, 1);
    assert.equal(statuses[0].name, 'a');
  });

  // ── Gap: getTeamSummary (memberCount/runningCount/idleCount, lead excluded) ──
  it('getTeamSummary aggregates counts excluding the lead', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'a', agentId: 'id-a' });
    registerTeamMember({ name: 'b', agentId: 'id-b' });
    registerTeamMember({ name: 'c', agentId: 'id-c' });
    setTeamMemberActive('c', false);
    const summary = getTeamSummary()!;
    assert.equal(summary.name, 'sq');
    assert.equal(summary.memberCount, 3); // lead not counted
    assert.equal(summary.runningCount, 2);
    assert.equal(summary.idleCount, 1);
  });

  it('getTeamSummary is null without a team', () => {
    assert.equal(getTeamSummary(), null);
  });

  // ── Gap: removeTeamMember (by name OR agentId; lead protected) ──────────────
  it('removeTeamMember removes by name and by agentId, persisting the change', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'a', agentId: 'id-a' });
    registerTeamMember({ name: 'b', agentId: 'id-b' });
    const byName = removeTeamMember('a');
    assert.equal(byName!.name, 'a');
    const byId = removeTeamMember('id-b');
    assert.equal(byId!.name, 'b');
    const team = getCurrentTeam()!;
    assert.equal(team.members.length, 1); // only lead remains
    assert.equal(team.members[0].name, TEAM_LEAD_NAME);
    await settle();
    const members = teamJson().members as Array<{ name: string }>;
    assert.equal(members.length, 1);
    assert.equal(members[0].name, TEAM_LEAD_NAME);
  });

  it('removeTeamMember never removes the lead and returns null for unknowns/no team', async () => {
    assert.equal(removeTeamMember('x'), null); // no team
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    assert.equal(removeTeamMember(TEAM_LEAD_NAME), null); // lead protected
    assert.equal(removeTeamMember('ghost'), null);
    assert.equal(getCurrentTeam()!.members.length, 1);
  });

  // ── Guard regression: setTeamMemberActive write-if-changed + idleSince ──────
  it('setTeamMemberActive stamps idleSince on going idle and clears it on reactivation', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'a', agentId: 'id-a' }); // isActive true
    setTeamMemberActive('a', false);
    const m = () => getCurrentTeam()!.members.find(x => x.name === 'a')!;
    assert.equal(m().isActive, false);
    assert.ok(typeof m().idleSince === 'string');
    setTeamMemberActive('a', true);
    assert.equal(m().isActive, true);
    assert.equal(m().idleSince, undefined);
  });
});
