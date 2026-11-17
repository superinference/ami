import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  teamCreateTool,
  resetTeam,
  getCurrentTeam,
  setDynamicTeamContext,
  clearDynamicTeamContext,
  getSelfPlanModeRequired,
  getSelfIdentity,
  readTeamFile,
} from '../src/tools/team';
import {
  createTeammateContext,
  runWithTeammateContext,
} from '../src/tools/teammate-context';
import { resetTaskState } from '../src/tools/task-tracker';
import { __resetLoggerForTest } from '../src/logger';
import type { ToolContext } from '../src/types';

let tmp: string;
let prevDataDir: string | undefined;
let prevDebugFile: string | undefined;

function ctx(sessionId = 'sess-w71'): ToolContext {
  return { cwd: tmp, sessionId, abortSignal: new AbortController().signal } as unknown as ToolContext;
}

function teamDir(name: string): string {
  return path.join(tmp, '.ami', 'teams', name);
}

// Capture everything the core logger writes during fn() (parity-trail assertions),
// using the same AMI_DEBUG_FILE seam the rest of the team suite uses.
function captureLogs(fn: () => void): string {
  const logFile = path.join(tmp, 'debug.log');
  prevDebugFile = process.env.AMI_DEBUG_FILE;
  process.env.AMI_DEBUG_FILE = logFile;
  __resetLoggerForTest();
  try {
    fn();
  } finally {
    if (prevDebugFile === undefined) delete process.env.AMI_DEBUG_FILE; else process.env.AMI_DEBUG_FILE = prevDebugFile;
  }
  try { return fs.readFileSync(logFile, 'utf8'); } catch { return ''; }
}

const PLAN_ENV = 'AMI_TEAMMATE_PLAN_MODE_REQUIRED';

describe('U038-w71 team.ts plan-mode env fallback + resilient readTeamFile logging + addressing rule', () => {
  let savedPlanEnv: string | undefined;

  beforeEach(() => {
    resetTeam();
    resetTaskState();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-w71-'));
    prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = path.join(tmp, '.data');
    savedPlanEnv = process.env[PLAN_ENV];
    delete process.env[PLAN_ENV];
  });
  afterEach(() => {
    resetTeam();
    resetTaskState();
    if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR; else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
    if (savedPlanEnv === undefined) delete process.env[PLAN_ENV]; else process.env[PLAN_ENV] = savedPlanEnv;
    __resetLoggerForTest();
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  // --- Gap: per-teammate plan-mode-required gating WITH env fallback -----------
  describe('getSelfPlanModeRequired env fallback (AMI_TEAMMATE_PLAN_MODE_REQUIRED)', () => {
    it('reads a truthy env var when no ALS/dynamic flag is bound', () => {
      for (const truthy of ['1', 'true', 'TRUE', 'yes', 'on']) {
        process.env[PLAN_ENV] = truthy;
        assert.equal(getSelfPlanModeRequired(), true, `expected true for ${truthy}`);
      }
    });

    it('treats an unrecognized / empty env value as false', () => {
      for (const falsey of ['0', 'false', 'no', 'off', '', 'maybe']) {
        process.env[PLAN_ENV] = falsey;
        assert.equal(getSelfPlanModeRequired(), false, `expected false for "${falsey}"`);
      }
    });

    it('defaults to false when the env var is unset', () => {
      delete process.env[PLAN_ENV];
      assert.equal(getSelfPlanModeRequired(), false);
    });

    it('a bound dynamic-context flag takes precedence over the env fallback', () => {
      process.env[PLAN_ENV] = 'true';
      setDynamicTeamContext({ agentId: 'w@t', agentName: 'w', teamName: 't', planModeRequired: false });
      assert.equal(getSelfPlanModeRequired(), false, 'dynamic false must win over env true');
      clearDynamicTeamContext();
      // With dynamic cleared, the env fallback applies again.
      assert.equal(getSelfPlanModeRequired(), true);
    });

    it('an in-process ALS flag takes precedence over the env fallback', () => {
      process.env[PLAN_ENV] = 'true';
      const als = createTeammateContext({ agentId: 'als@t', agentName: 'als', teamName: 't', planModeRequired: false });
      runWithTeammateContext(als, () => {
        assert.equal(getSelfPlanModeRequired(), false, 'ALS false must win over env true');
      });
      // Outside the ALS scope the env fallback applies.
      assert.equal(getSelfPlanModeRequired(), true);
    });

    it('surfaces through the getSelfIdentity aggregate', () => {
      process.env[PLAN_ENV] = 'yes';
      assert.equal(getSelfIdentity().planModeRequired, true);
    });
  });

  // --- Gap: resilient team-file read, ENOENT vs corruption distinction ---------
  describe('readTeamFile ENOENT-vs-corruption diagnostic distinction', () => {
    it('ENOENT (no file) degrades to null WITHOUT a diagnostic', () => {
      let result: ReturnType<typeof readTeamFile> = {} as never;
      const logs = captureLogs(() => { result = readTeamFile(teamDir('nope')); });
      assert.equal(result, null);
      assert.ok(!logs.includes('readTeamFile:'), `ENOENT must not log a readTeamFile diagnostic:\n${logs}`);
    });

    it('corrupt JSON degrades to null AND logs a distinct corruption diagnostic', () => {
      const dir = teamDir('corrupt');
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, 'team.json'), '{ this is not json');
      let result: ReturnType<typeof readTeamFile> = {} as never;
      const logs = captureLogs(() => { result = readTeamFile(dir); });
      assert.equal(result, null);
      assert.ok(logs.includes('corrupt JSON'), `expected a corrupt-JSON diagnostic:\n${logs}`);
    });

    it('a non-object JSON value degrades to null with a diagnostic', () => {
      const dir = teamDir('scalar');
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, 'team.json'), '42');
      let result: ReturnType<typeof readTeamFile> = {} as never;
      const logs = captureLogs(() => { result = readTeamFile(dir); });
      assert.equal(result, null);
      assert.ok(logs.includes('non-object team file'), `expected a non-object diagnostic:\n${logs}`);
    });

    it('a well-formed-JSON-but-wrong-shape file degrades to null with a shape diagnostic', () => {
      const dir = teamDir('wrongshape');
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, 'team.json'), JSON.stringify({ name: 'x' })); // no leadAgentId/members
      let result: ReturnType<typeof readTeamFile> = {} as never;
      const logs = captureLogs(() => { result = readTeamFile(dir); });
      assert.equal(result, null);
      assert.ok(logs.includes('invalid team file shape'), `expected a shape diagnostic:\n${logs}`);
    });

    it('a valid team file still reads back cleanly with no diagnostic', async () => {
      await teamCreateTool.execute({ team_name: 'alpha' }, ctx());
      const dir = teamDir('alpha');
      let result: ReturnType<typeof readTeamFile> = null;
      const logs = captureLogs(() => { result = readTeamFile(dir); });
      assert.ok(result && (result as { name: string }).name === 'alpha');
      assert.ok(!logs.includes('readTeamFile:'), `valid read must not log a degradation diagnostic:\n${logs}`);
    });
  });

  // --- Gap: member-discovery + "always refer to teammates by NAME" rule --------
  describe('team_create model-facing addressing rule', () => {
    it('documents the always-by-NAME addressing rule and member discovery', () => {
      const d = teamCreateTool.description ?? '';
      assert.ok(/refer to a teammate by its human NAME/i.test(d), 'missing the by-NAME addressing rule');
      assert.ok(/never by its internal agentId/i.test(d), 'missing the never-by-agentId guidance');
      assert.ok(/members/i.test(d) && /discover/i.test(d), 'missing member-discovery guidance');
    });

    it('a created team records the discoverable member roster (name/agentId) the rule references', async () => {
      await teamCreateTool.execute(
        { team_name: 'beta', members: ['researcher', 'builder'] },
        ctx(),
      );
      const team = getCurrentTeam()!;
      const names = team.members.map(m => m.name).sort();
      assert.ok(names.includes('researcher') && names.includes('builder'));
      // Every member carries both the human name and an internal agentId, so a
      // teammate can be addressed by NAME while the id stays available internally.
      for (const m of team.members) {
        assert.equal(typeof m.name, 'string');
        assert.equal(typeof m.agentId, 'string');
      }
    });
  });
});
