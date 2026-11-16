import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  teamCreateTool,
  teamDeleteTool,
  getCurrentTeam,
  resetTeam,
  registerTeamMember,
  setTeamMemberActive,
  readTeamFile,
  findTeamMemberByName,
  loadTeamFromDisk,
  restoreTeam,
} from '../src/tools/team';
import {
  sendMessageTool,
  getMailbox,
  resetAllMailboxes,
} from '../src/tools/send-message';
import type { ToolContext } from '../src/types';

let tmp: string;
function ctx(sessionId = 'sess-1'): ToolContext {
  return { cwd: tmp, sessionId, abortSignal: new AbortController().signal } as ToolContext;
}
function teamDir(name: string): string {
  return path.join(tmp, '.ami', 'teams', name);
}
/**
 * Wait until a predicate holds. The team file is persisted asynchronously (under a
 * cross-process lock), so a test that reads it back immediately after a mutation
 * must poll for the write to land rather than assume it is synchronous.
 */
async function waitFor(pred: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error('waitFor: predicate never became true');
    await new Promise(r => setTimeout(r, 5));
  }
}

describe('team reconnection + disband inbox flush', () => {
  beforeEach(() => {
    resetTeam();
    resetAllMailboxes();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-reconnect-'));
  });
  afterEach(() => {
    resetTeam();
    resetAllMailboxes();
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  // --- readTeamFile: graceful fallback (missing/corrupt/wrong-shape) ---------
  describe('readTeamFile', () => {
    it('returns null for a missing team file (no throw)', () => {
      assert.equal(readTeamFile(teamDir('nope')), null);
    });

    it('returns null for corrupt JSON', () => {
      const dir = teamDir('corrupt');
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, 'team.json'), '{ this is not json');
      assert.equal(readTeamFile(dir), null);
    });

    it('returns null for structurally invalid JSON (missing required fields)', () => {
      const dir = teamDir('wrongshape');
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, 'team.json'), JSON.stringify({ name: 'x' }));
      assert.equal(readTeamFile(dir), null);
    });

    it('parses a valid persisted team file', async () => {
      await teamCreateTool.execute({ team_name: 'alpha' }, ctx());
      await waitFor(() => !!readTeamFile(teamDir('alpha')));
      const t = readTeamFile(teamDir('alpha'));
      assert.ok(t);
      assert.equal(t.name, 'alpha');
      assert.equal(t.leadAgentId, 'lead-alpha');
      assert.ok(Array.isArray(t.members));
    });
  });

  // --- findTeamMemberByName: recovery with removed-member branch -------------
  describe('findTeamMemberByName', () => {
    it('finds an existing member by name', async () => {
      await teamCreateTool.execute({ team_name: 'beta' }, ctx());
      registerTeamMember({ name: 'worker', agentId: 'ag-1' });
      const team = getCurrentTeam()!;
      const m = findTeamMemberByName(team, 'worker');
      assert.ok(m);
      assert.equal(m.agentId, 'ag-1');
    });

    it('returns null for a removed/absent member', async () => {
      await teamCreateTool.execute({ team_name: 'beta' }, ctx());
      const team = getCurrentTeam()!;
      assert.equal(findTeamMemberByName(team, 'ghost'), null);
    });
  });

  // --- loadTeamFromDisk + restoreTeam ---------------------------------------
  describe('loadTeamFromDisk / restoreTeam', () => {
    it('returns null when no team exists on disk', () => {
      assert.equal(loadTeamFromDisk(tmp, 'absent'), null);
    });

    it('loads a persisted team and restoreTeam rehydrates the singleton', async () => {
      await teamCreateTool.execute({ team_name: 'gamma' }, ctx());
      await waitFor(() => !!readTeamFile(teamDir('gamma')));
      resetTeam(); // simulate a process restart (in-memory state lost)
      assert.equal(getCurrentTeam(), null);

      const loaded = loadTeamFromDisk(tmp, 'gamma');
      assert.ok(loaded);
      assert.equal(loaded.team.name, 'gamma');

      restoreTeam(loaded.team, loaded.dir);
      assert.equal(getCurrentTeam()!.name, 'gamma');
    });
  });

  // --- team_create resumed-session reconnection (E2E through the tool) -------
  describe('team_create resumed-session reconnection', () => {
    it('reconnects in place when the same session recreates an existing team', async () => {
      await teamCreateTool.execute({ team_name: 'squad' }, ctx('sess-A'));
      registerTeamMember({ name: 'worker', agentId: 'ag-w' });
      const firstDir = teamDir('squad');
      assert.ok(fs.existsSync(firstDir));
      // The persist is async (locked write); wait for the member to land on disk.
      await waitFor(() => !!readTeamFile(firstDir)?.members.some(m => m.name === 'worker'));

      resetTeam(); // process restart, same session resumes

      const r = await teamCreateTool.execute({ team_name: 'squad' }, ctx('sess-A'));
      assert.equal(r.isError, undefined);
      assert.match(r.output, /Reconnected to existing team "squad"/);
      // No renamed duplicate directory was created.
      assert.equal(fs.existsSync(teamDir('squad-2')), false);
      // The restored team retains the previously-registered member.
      const team = getCurrentTeam()!;
      assert.equal(team.name, 'squad');
      assert.ok(findTeamMemberByName(team, 'worker'));
    });

    it('does NOT reconnect a foreign team (different session) — falls back to unique name', async () => {
      await teamCreateTool.execute({ team_name: 'squad' }, ctx('sess-A'));
      resetTeam();

      const r = await teamCreateTool.execute({ team_name: 'squad' }, ctx('sess-B'));
      assert.equal(r.isError, undefined);
      assert.doesNotMatch(r.output, /Reconnected/);
      assert.match(r.output, /renamed from "squad"/);
      assert.ok(fs.existsSync(teamDir('squad-2')));
      assert.equal(getCurrentTeam()!.name, 'squad-2');
    });

    it('does NOT reconnect when the on-disk team file is corrupt — falls back to unique name', async () => {
      await teamCreateTool.execute({ team_name: 'squad' }, ctx('sess-A'));
      resetTeam();
      // Corrupt the persisted file so readTeamFile returns null.
      fs.writeFileSync(path.join(teamDir('squad'), 'team.json'), 'not json at all');

      const r = await teamCreateTool.execute({ team_name: 'squad' }, ctx('sess-A'));
      assert.equal(r.isError, undefined);
      assert.doesNotMatch(r.output, /Reconnected/);
      assert.ok(fs.existsSync(teamDir('squad-2')));
    });
  });

  // --- team_delete flushes queued inbox messages (E2E) ----------------------
  describe('team_delete inbox flush', () => {
    it('clears queued mailbox messages for every member on disband', async () => {
      await teamCreateTool.execute({ team_name: 'crew' }, ctx());
      // Register a teammate and queue a message for it via the real send path.
      registerTeamMember({ name: 'worker', agentId: 'ag-x' });
      const send = await sendMessageTool.execute(
        { to: 'worker', content: 'hello teammate' },
        ctx(),
      );
      assert.equal(send.isError, undefined);
      assert.ok(getMailbox('worker').length > 0, 'precondition: mailbox has a queued message');

      // Mark the teammate idle so the active-member guard permits disband.
      setTeamMemberActive('worker', false);
      const del = await teamDeleteTool.execute({}, ctx());
      assert.equal(del.isError, undefined);

      // The teammate's queued inbox was flushed on disband.
      assert.equal(getMailbox('worker').length, 0);
    });
  });
});
