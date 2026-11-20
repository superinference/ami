import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  teamCreateTool,
  resetTeam,
  getTeamsRoot,
  getTeamDir,
  getTeamFilePath,
  getTeamScratchpadDir,
  loadTeamFromDisk,
  loadTeamFromSession,
  registerTeamForSessionCleanup,
  cleanupSessionTeams,
  type TeamFile,
} from '../src/tools/team';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U038 (w98) — re-sanitization of a team name at the READ/RESUME boundary.
//
// Parity: the reference (utils/swarm/teamHelpers.ts L100-123) turns a team name
// into a path through a SINGLE canonical helper — getTeamDir(name) =
// join(getTeamsDir(), sanitizeName(name)) and getTeamFilePath = join(getTeamDir,
// config.json) — so readTeamFile / getTeamFilePath apply sanitizeName on EVERY
// access, including the resume path (reconnection.ts L36-49 reads a team name that
// came from resumed transcript data, i.e. untrusted persisted state).
//
// Previously AMI validated a team name only on the WRITE path (team_create) and
// joined it verbatim on READ (loadTeamFromDisk did
// path.join(cwd, '.ami', 'teams', name)), so a traversal name rehydrated from a
// persisted transcript/team file could escape the teams root. These tests lock in
// the canonical getTeamsRoot/getTeamDir/getTeamFilePath helpers and prove every
// read/resume/cleanup path routes through them.
// ---------------------------------------------------------------------------

let tmp: string;

function ctx(sessionId = 'sess-w98'): ToolContext {
  return { cwd: tmp, sessionId, abortSignal: new AbortController().signal } as ToolContext;
}

function validTeamFile(name: string): TeamFile {
  return {
    name,
    leadAgentId: `lead-${name}`,
    members: [],
    createdAt: new Date().toISOString(),
  };
}

describe('U038 w98 — canonical team-name→path helpers (read-boundary sanitization)', () => {
  beforeEach(() => {
    resetTeam();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w98-'));
  });
  afterEach(() => {
    resetTeam();
    cleanupSessionTeams(tmp); // drain any registered names so state never leaks across tests
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  it('getTeamsRoot is the single <cwd>/.ami/teams spelling', () => {
    assert.equal(getTeamsRoot(tmp), path.join(tmp, '.ami', 'teams'));
  });

  it('getTeamDir preserves an already-valid name verbatim (AMI superset: case/underscore kept)', () => {
    assert.equal(getTeamDir(tmp, 'My_Team-1'), path.join(getTeamsRoot(tmp), 'My_Team-1'));
  });

  it('getTeamDir contains a traversal name inside the teams root (never escapes)', () => {
    const dir = getTeamDir(tmp, '../../EVIL');
    assert.ok(dir, 'expected a non-null contained dir');
    // The slug is the only path component under the teams root.
    assert.equal(path.dirname(dir!), getTeamsRoot(tmp));
    // No parent-dir segment survives.
    assert.ok(!dir!.split(path.sep).includes('..'));
    assert.ok(dir!.startsWith(getTeamsRoot(tmp) + path.sep));
  });

  it('getTeamDir / getTeamFilePath return null for a name that normalizes to empty', () => {
    assert.equal(getTeamDir(tmp, '...'), null);
    assert.equal(getTeamDir(tmp, ''), null);
    assert.equal(getTeamFilePath(tmp, '...'), null);
  });

  it('getTeamFilePath derives <teamDir>/team.json through the canonical dir', () => {
    assert.equal(
      getTeamFilePath(tmp, 'alpha'),
      path.join(getTeamsRoot(tmp), 'alpha', 'team.json'),
    );
  });

  it('REPRODUCER: loadTeamFromDisk re-sanitizes the name and cannot read a file outside the teams root', () => {
    // Plant a valid team file OUTSIDE the teams root, at the location a RAW join of
    // the traversal name would reach: path.join(cwd, '.ami', 'teams', '../../EVIL')
    // === path.join(cwd, 'EVIL').
    const escapeDir = path.join(tmp, 'EVIL');
    fs.mkdirSync(escapeDir, { recursive: true });
    fs.writeFileSync(path.join(escapeDir, 'team.json'), JSON.stringify(validTeamFile('EVIL')));

    // GREEN (fixed): the read boundary sanitizes '../../EVIL' -> 'EVIL' UNDER the
    // teams root, where no file exists, so the escape file is NOT loaded.
    // RED (pre-fix): loadTeamFromDisk joined the name verbatim and would return the
    // planted escape team.
    const loaded = loadTeamFromDisk(tmp, '../../EVIL');
    assert.equal(loaded, null, 'traversal name must not read a file outside the teams root');

    // An un-sanitizable name degrades to null rather than throwing.
    assert.equal(loadTeamFromDisk(tmp, '...'), null);
  });

  it('REPRODUCER: loadTeamFromSession (resume path) routes through the sanitizing read boundary', () => {
    const escapeDir = path.join(tmp, 'EVIL');
    fs.mkdirSync(escapeDir, { recursive: true });
    fs.writeFileSync(path.join(escapeDir, 'team.json'), JSON.stringify(validTeamFile('EVIL')));
    // teamName here models an untrusted value rehydrated from a persisted transcript.
    const restored = loadTeamFromSession(tmp, '../../EVIL', 'lead');
    assert.equal(restored, null, 'resume must not rehydrate a team from outside the teams root');
  });

  it('loadTeamFromDisk round-trips a legitimately-created team through the canonical dir', async () => {
    const res = await teamCreateTool.execute({ team_name: 'My Team!' }, ctx());
    assert.equal(res.isError, undefined);
    // team_create normalizes "My Team!" -> "My-Team"; the read boundary resolves it.
    const loaded = loadTeamFromDisk(tmp, 'My-Team');
    assert.ok(loaded, 'expected the created team to be loadable by its sanitized name');
    assert.equal(loaded!.team.name, 'My-Team');
    assert.equal(loaded!.dir, getTeamDir(tmp, 'My-Team'));
    resetTeam();
  });

  it('getTeamScratchpadDir contains an unsafe team name and nulls an un-sanitizable one', () => {
    // An unsafe name on an (explicit) team file is contained under the teams root.
    const contained = getTeamScratchpadDir(tmp, validTeamFile('../evil'));
    assert.ok(contained);
    assert.ok(contained!.startsWith(getTeamsRoot(tmp) + path.sep));
    assert.ok(!contained!.split(path.sep).includes('..'));
    assert.equal(path.basename(contained!), 'memory');
    // A name that normalizes to empty yields null (no path built).
    assert.equal(getTeamScratchpadDir(tmp, validTeamFile('...')), null);
    // No current team -> null.
    resetTeam();
    assert.equal(getTeamScratchpadDir(tmp, null), null);
  });

  it('REPRODUCER: cleanupSessionTeams cannot rm a directory outside the teams root', () => {
    // Plant a directory at the escape location the raw join would reach and mark a
    // traversal name for session cleanup.
    const escapeDir = path.join(tmp, 'EVIL2');
    fs.mkdirSync(escapeDir, { recursive: true });
    fs.writeFileSync(path.join(escapeDir, 'keep.txt'), 'important');
    registerTeamForSessionCleanup('../../EVIL2');
    // Also register an un-sanitizable name to exercise the skip branch.
    registerTeamForSessionCleanup('...');

    const removed = cleanupSessionTeams(tmp);
    // GREEN: nothing outside the teams root is removed (the sanitized dir does not
    // exist); RED (pre-fix): the raw join would rmSync(cwd/EVIL2), deleting keep.txt.
    assert.ok(fs.existsSync(path.join(escapeDir, 'keep.txt')), 'cleanup must not escape the teams root');
    assert.deepEqual(removed, [], 'no contained team dir existed, so nothing is reported removed');
  });
});
