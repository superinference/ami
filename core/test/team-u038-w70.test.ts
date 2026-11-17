import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  teamCreateTool,
  resetTeam,
  getCurrentTeam,
  registerTeamMember,
  getTeammateStatuses,
  readTeamFile,
  formatAgentId,
  // U038-w70 additions: hidden-pane roster + pane-keyed removal + subscriptions
  removeMemberByPaneId,
  setMemberPaneHidden,
  removeTeamMember,
} from '../src/tools/team';
import { resetTaskState } from '../src/tools/task-tracker';
import { TEAM_LEAD_NAME } from '../src/tools/send-message';
import { __resetLoggerForTest } from '../src/logger';
import type { ToolContext } from '../src/types';

let tmp: string;
let prevDataDir: string | undefined;

function ctx(sessionId = 'sess-w70'): ToolContext {
  return { cwd: tmp, sessionId, abortSignal: new AbortController().signal } as unknown as ToolContext;
}

function teamDir(name: string): string {
  return path.join(tmp, '.ami', 'teams', name);
}

describe('U038-w70 team.ts hidden-pane roster + pane-keyed removal + subscriptions', () => {
  beforeEach(() => {
    resetTeam();
    resetTaskState();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-w70-'));
    prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = path.join(tmp, '.data');
  });
  afterEach(() => {
    resetTeam();
    resetTaskState();
    if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR; else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
    __resetLoggerForTest();
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  // --- isHidden derivation in getTeammateStatuses (parity: hiddenPaneIds Set) ---
  describe('getTeammateStatuses isHidden derivation', () => {
    it('derives isHidden from the team hiddenPaneIds set and surfaces tmuxPaneId', async () => {
      await teamCreateTool.execute({ team_name: 'alpha' }, ctx());
      registerTeamMember({ name: 'w1', agentId: formatAgentId('w1', 'alpha'), tmuxPaneId: 'pane-1', backendType: 'tmux' });
      registerTeamMember({ name: 'w2', agentId: formatAgentId('w2', 'alpha'), tmuxPaneId: 'pane-2', backendType: 'tmux' });
      // Hide pane-1.
      assert.equal(setMemberPaneHidden('pane-1', true), true);

      const statuses = getTeammateStatuses();
      const w1 = statuses.find(s => s.name === 'w1')!;
      const w2 = statuses.find(s => s.name === 'w2')!;
      assert.equal(w1.tmuxPaneId, 'pane-1');
      assert.equal(w1.isHidden, true, 'w1 is in the hidden set');
      assert.equal(w2.tmuxPaneId, 'pane-2');
      assert.equal(w2.isHidden, false, 'w2 is not hidden');
    });

    it('a paneless (in-process) teammate is never hidden even if its name collides with a hidden pane', async () => {
      await teamCreateTool.execute({ team_name: 'beta' }, ctx());
      registerTeamMember({ name: 'ip', agentId: formatAgentId('ip', 'beta') }); // no tmuxPaneId
      setMemberPaneHidden('ip', true); // a stray pane id that no member actually owns
      const [status] = getTeammateStatuses();
      assert.equal(status.tmuxPaneId, undefined);
      assert.equal(status.isHidden, false);
    });
  });

  // --- setMemberPaneHidden (parity: hiddenPaneIds toggle) ----------------------
  describe('setMemberPaneHidden', () => {
    it('adds, is idempotent, and clears a pane in the hidden set with write-if-changed', async () => {
      await teamCreateTool.execute({ team_name: 'gamma' }, ctx());
      assert.equal(setMemberPaneHidden('p9', true), true, 'first hide changes the set');
      assert.deepEqual(getCurrentTeam()!.hiddenPaneIds, ['p9']);
      assert.equal(setMemberPaneHidden('p9', true), false, 'hiding an already-hidden pane is a no-op');
      assert.equal(setMemberPaneHidden('p9', false), true, 'unhide changes the set');
      // Last entry removed => normalized to undefined, not an empty array.
      assert.equal(getCurrentTeam()!.hiddenPaneIds, undefined);
      assert.equal(setMemberPaneHidden('p9', false), false, 'unhiding a non-hidden pane is a no-op');
    });

    it('is a no-op with no active team or an empty pane id', () => {
      assert.equal(setMemberPaneHidden('x', true), false);
    });
  });

  // --- removeMemberByPaneId (parity: removeMemberFromTeam by pane) -------------
  describe('removeMemberByPaneId', () => {
    it('removes the member owning a pane and scrubs that pane from hiddenPaneIds', async () => {
      await teamCreateTool.execute({ team_name: 'delta' }, ctx());
      registerTeamMember({ name: 'w1', agentId: formatAgentId('w1', 'delta'), tmuxPaneId: 'pane-a' });
      setMemberPaneHidden('pane-a', true);
      assert.deepEqual(getCurrentTeam()!.hiddenPaneIds, ['pane-a']);

      const removed = removeMemberByPaneId('pane-a');
      assert.equal(removed?.name, 'w1');
      assert.equal(getCurrentTeam()!.members.find(m => m.name === 'w1'), undefined, 'member gone');
      assert.equal(getCurrentTeam()!.hiddenPaneIds, undefined, 'dangling hidden pane scrubbed');
    });

    it('never removes the lead and returns null for an unknown pane / no team', async () => {
      assert.equal(removeMemberByPaneId('nope'), null, 'no team');
      await teamCreateTool.execute({ team_name: 'epsilon' }, ctx());
      // Give the lead a pane, then try to remove it by pane — must refuse.
      const lead = getCurrentTeam()!.members.find(m => m.name === TEAM_LEAD_NAME)!;
      lead.tmuxPaneId = 'lead-pane';
      assert.equal(removeMemberByPaneId('lead-pane'), null, 'lead not removable by pane');
      assert.equal(removeMemberByPaneId('ghost'), null, 'unknown pane');
    });
  });

  // --- removeTeamMember also scrubs a hidden pane -----------------------------
  describe('removeTeamMember hidden-pane scrub', () => {
    it('scrubs the removed member\'s pane from hiddenPaneIds', async () => {
      await teamCreateTool.execute({ team_name: 'zeta' }, ctx());
      registerTeamMember({ name: 'w1', agentId: formatAgentId('w1', 'zeta'), tmuxPaneId: 'z-pane' });
      setMemberPaneHidden('z-pane', true);
      const removed = removeTeamMember('w1');
      assert.equal(removed?.name, 'w1');
      assert.equal(getCurrentTeam()!.hiddenPaneIds, undefined, 'pane scrubbed on name-keyed removal');
    });
  });

  // --- readTeamFile boundary validation of new persisted fields ---------------
  describe('readTeamFile validation of hiddenPaneIds / tmuxPaneId / subscriptions', () => {
    it('coerces a non-array hiddenPaneIds and malformed member fields to undefined', async () => {
      await teamCreateTool.execute({ team_name: 'eta' }, ctx());
      const dir = teamDir('eta');
      const raw = JSON.parse(fs.readFileSync(path.join(dir, 'team.json'), 'utf8'));
      raw.hiddenPaneIds = 'not-an-array';
      raw.members.push({
        name: 'bad', agentId: 'bad@eta', joinedAt: new Date().toISOString(),
        tmuxPaneId: 123, subscriptions: ['ok', 7],
      });
      fs.writeFileSync(path.join(dir, 'team.json'), JSON.stringify(raw));

      const team = readTeamFile(dir)!;
      assert.equal(team.hiddenPaneIds, undefined, 'bad hiddenPaneIds coerced away');
      const bad = team.members.find(m => m.name === 'bad')!;
      assert.equal(bad.tmuxPaneId, undefined, 'non-string pane id coerced away');
      assert.equal(bad.subscriptions, undefined, 'subscriptions with a non-string entry coerced away');
    });

    it('keeps a valid hiddenPaneIds array and valid member fields', async () => {
      await teamCreateTool.execute({ team_name: 'theta' }, ctx());
      const dir = teamDir('theta');
      const raw = JSON.parse(fs.readFileSync(path.join(dir, 'team.json'), 'utf8'));
      raw.hiddenPaneIds = ['p1', 'p2'];
      raw.members.push({
        name: 'good', agentId: 'good@theta', joinedAt: new Date().toISOString(),
        tmuxPaneId: 'p1', subscriptions: ['build', 'deploy'],
      });
      fs.writeFileSync(path.join(dir, 'team.json'), JSON.stringify(raw));

      const team = readTeamFile(dir)!;
      assert.deepEqual(team.hiddenPaneIds, ['p1', 'p2']);
      const good = team.members.find(m => m.name === 'good')!;
      assert.equal(good.tmuxPaneId, 'p1');
      assert.deepEqual(good.subscriptions, ['build', 'deploy']);
      // And projection: a member on a hidden pane reads isHidden true off disk.
      const status = getTeammateStatuses(team).find(s => s.name === 'good')!;
      assert.equal(status.isHidden, true);
    });
  });

  // --- E2E: subscriptions declared at team_create persist to team.json ---------
  describe('team_create subscriptions roster knob (parity: per-member subscriptions)', () => {
    it('parses, persists, and reads back per-member subscriptions', async () => {
      const res = await teamCreateTool.execute({
        team_name: 'iota',
        members: [
          { name: 'builder', subscriptions: ['build', 'ci'] },
          { name: 'noisy', subscriptions: ['', 42, 'deploy'] }, // only 'deploy' survives
          { name: 'bare' }, // no subscriptions => undefined
        ],
      }, ctx());
      assert.equal(res.isError, undefined);

      // In-memory.
      const mem = getCurrentTeam()!;
      assert.deepEqual(mem.members.find(m => m.name === 'builder')!.subscriptions, ['build', 'ci']);
      assert.deepEqual(mem.members.find(m => m.name === 'noisy')!.subscriptions, ['deploy']);
      assert.equal(mem.members.find(m => m.name === 'bare')!.subscriptions, undefined);

      // Round-trips through disk.
      const onDisk = readTeamFile(teamDir('iota'))!;
      assert.deepEqual(onDisk.members.find(m => m.name === 'builder')!.subscriptions, ['build', 'ci']);
      assert.deepEqual(onDisk.members.find(m => m.name === 'noisy')!.subscriptions, ['deploy']);
    });
  });
});
