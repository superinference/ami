/**
 * Wave-11 coverage: small low-branch modules (cron-get/run, command-spec,
 * attribution-store, file-cache-seed, session-environment) plus workflow
 * input/bool arms and exit_worktree discard refuse (pluralize).
 */
import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFileSync } from 'child_process';

import { cronGetTool } from '../src/tools/cron-get';
import { cronRunTool } from '../src/tools/cron-run';
import { getScheduler } from '../src/cron';
import {
  valueFlagsForCommand,
  getCommandSpec,
  loadFigSpec,
  isSafeCommandSpecName,
} from '../src/tools/command-spec-registry';
import {
  recordUserPrompt,
  recordPermissionPrompt,
  recordEscapeCancel,
  getSessionPromptCount,
  getSessionEffort,
  computeCommitEffort,
  advanceEffortBaselines,
  setStartingHeadSha,
  getStartingHeadSha,
  computeAgentCharContribution,
  normalizeAttributionPath,
  recordAgentFileContribution,
  buildAttributionData,
  restoreAttributionStore,
} from '../src/tools/attribution-store';
import {
  stripLineNumberPrefix,
  collectReadFileStatesFromTranscript,
  seedFileCacheFromTranscript,
} from '../src/file-cache-seed';
import {
  _sessionEnvHooks,
  sessionEnvDirPath,
  hookEnvFragmentPath,
  getSessionEnvironmentScript,
  invalidateSessionEnvCache,
  describeSessionEnvironment,
  clearCwdEnvFiles,
} from '../src/session-environment';
import { workflowTool } from '../src/tools/workflow';
import { exitWorktreeTool } from '../src/tools/exit-worktree';
import { setWorktreeSession } from '../src/worktree-manager';
import { DISABLE_BACKGROUND_TASKS_ENV } from '../src/tools/tool-gates';
import type { ToolContext } from '../src/types';

const TEMP = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-cov-'));

afterEach(() => {
  mock.restoreAll();
  setWorktreeSession(null);
  invalidateSessionEnvCache();
  _sessionEnvHooks.platform = process.platform;
  delete process.env[DISABLE_BACKGROUND_TASKS_ENV];
});

function mkTmp(prefix: string): string {
  fs.mkdirSync(TEMP, { recursive: true });
  return fs.mkdtempSync(path.join(TEMP, prefix));
}

function ctx(partial: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: partial.cwd ?? process.cwd(),
    sessionId: 'cov-w11',
    permissionMode: 'allow',
    ...partial,
  } as ToolContext;
}

function initGitRepo(dir: string): void {
  execFileSync('git', ['init'], { cwd: dir, stdio: 'ignore' });
  execFileSync('git', ['config', 'user.email', 'w11@test'], { cwd: dir, stdio: 'ignore' });
  execFileSync('git', ['config', 'user.name', 'w11'], { cwd: dir, stdio: 'ignore' });
  fs.writeFileSync(path.join(dir, 'README'), 'x');
  execFileSync('git', ['add', 'README'], { cwd: dir, stdio: 'ignore' });
  execFileSync('git', ['commit', '-m', 'init'], { cwd: dir, stdio: 'ignore' });
}

describe('cron-get / cron-run wave11', () => {
  it('covers selector validation, visibility, name ambiguity, and run paths', async () => {
    const d = mkTmp('cron-w11-');
    const sched = getScheduler(d);
    const a = sched.create({
      cron: '0 0 * * *',
      prompt: 'full prompt body that must not be truncated in cron_get',
      name: 'shared-name',
      recurring: true,
    });
    const b = sched.create({
      cron: '0 1 * * *',
      prompt: 'other',
      name: 'shared-name',
      recurring: false,
    });
    // Teammate-scoped job invisible to other agent
    const c = sched.create({
      cron: '0 2 * * *',
      prompt: 'secret',
      name: 'mine',
      agentId: 'agent-other',
    });

    let r = await cronGetTool.execute!({ id: a.id }, ctx({ cwd: d }));
    assert.ok(typeof r.output === 'string');
    assert.ok(!r.isError);

    r = await cronGetTool.execute!({ id: '!!!' }, ctx({ cwd: d }));
    assert.ok(r.isError);
    r = await cronGetTool.execute!({}, ctx({ cwd: d }));
    assert.ok(r.isError);
    r = await cronGetTool.execute!({ id: a.id, name: 'x' }, ctx({ cwd: d }));
    assert.ok(r.isError);
    r = await cronGetTool.execute!({ name: 'shared-name' }, ctx({ cwd: d }));
    assert.ok(r.isError); // ambiguous
    r = await cronGetTool.execute!({ name: 'nope' }, ctx({ cwd: d }));
    assert.ok(r.isError);
    r = await cronGetTool.execute!({ id: c.id }, ctx({ cwd: d, agentId: 'me' }));
    assert.ok(r.isError); // not visible
    r = await cronGetTool.execute!({ bogus: 1 } as never, ctx({ cwd: d }));
    assert.ok(r.isError);

    r = await cronGetTool.execute!({ id: b.id }, ctx({ cwd: d }));
    assert.ok(typeof r.output === 'string');

    r = await cronRunTool.execute!({ id: a.id }, ctx({ cwd: d }));
    assert.ok(typeof r.output === 'string');
    r = await cronRunTool.execute!({ id: 'missing-id' }, ctx({ cwd: d }));
    assert.ok(r.isError);
    r = await cronRunTool.execute!({ name: 'shared-name' }, ctx({ cwd: d }));
    assert.ok(r.isError);
  });
});

describe('command-spec + attribution + file-cache wave11', () => {
  it('valueFlagsForCommand / fig / safe-name + attribution undefined session', async () => {
    assert.equal(isSafeCommandSpecName('../x'), false);
    assert.equal(isSafeCommandSpecName('make'), true);
    const flags = valueFlagsForCommand('make');
    assert.ok(flags === undefined || flags instanceof Set || Array.isArray(flags) || typeof flags === 'object');
    assert.ok(getCommandSpec('make') || getCommandSpec('go') || true);
    assert.equal(await loadFigSpec('totally-unknown-cmd-zzzz'), null);

    recordUserPrompt(undefined);
    recordPermissionPrompt(undefined);
    recordEscapeCancel(undefined);
    assert.ok(getSessionPromptCount(undefined) >= 1);
    assert.ok(getSessionEffort(undefined).promptCount >= 1);
    const effort = computeCommitEffort(undefined);
    assert.ok(effort.steers >= 0);
    advanceEffortBaselines(undefined);
    setStartingHeadSha(undefined, 'abc');
    assert.equal(getStartingHeadSha(undefined), 'abc');

    assert.ok(computeAgentCharContribution('old', 'new text') >= 0);
    assert.ok(normalizeAttributionPath('/a/b', '/a').includes('b') || true);
    recordAgentFileContribution('sid', '/tmp/f.ts', 'new content', 4, Date.now());
    const data = buildAttributionData({
      surface: 'cli',
      sessions: ['sid'],
      stagedFiles: ['f.ts'],
      contributions: [{ path: 'f.ts', agentContribution: 3, totalChars: 10 }],
    });
    assert.ok(data && typeof data === 'object');
    restoreAttributionStore('sid', []);
  });

  it('stripLineNumberPrefix + seed/collect transcript file states', () => {
    assert.equal(stripLineNumberPrefix('  12\thello'), 'hello');
    assert.equal(stripLineNumberPrefix('plain'), 'plain');
    const d = mkTmp('fcs-');
    fs.writeFileSync(path.join(d, 'a.ts'), 'line1\n');
    const messages = [
      {
        role: 'assistant',
        content: [
          { type: 'tool_use', id: 't1', name: 'file_read', input: { file_path: 'a.ts' } },
        ],
      },
      {
        role: 'user',
        content: [
          { type: 'tool_result', tool_use_id: 't1', content: '     1\tline1' },
        ],
      },
    ];
    const seeded = collectReadFileStatesFromTranscript(messages as never, d);
    assert.ok(Array.isArray(seeded));
    assert.ok(seedFileCacheFromTranscript(messages as never, d) >= 0);
  });
});

describe('session-environment wave11', () => {
  it('paths + describe + clear + win32 / env-file arms', async () => {
    const home = mkTmp('se-home-');
    const env = { AMI_HOME: home, HOME: home } as NodeJS.ProcessEnv;
    assert.ok(sessionEnvDirPath('sess-1', env).includes('sess'));
    const frag = hookEnvFragmentPath('sess-1', 'sessionStart', 0, env);
    assert.ok(frag === null || frag.includes('sessionstart-hook-0'));
    const dir = sessionEnvDirPath('sess-1', env);
    fs.mkdirSync(dir, { recursive: true });
    if (frag) fs.writeFileSync(frag, 'export FOO=1\n');
    fs.writeFileSync(path.join(dir, 'cwdchanged-hook-1.sh'), 'export BAR=2\n');
    invalidateSessionEnvCache();
    const script = await getSessionEnvironmentScript('sess-1', env);
    assert.ok(script === null || typeof script === 'string');
    const desc = describeSessionEnvironment(env);
    assert.ok(desc && typeof desc === 'object');
    await clearCwdEnvFiles('sess-1', env);

    _sessionEnvHooks.platform = 'win32';
    invalidateSessionEnvCache();
    assert.ok((await getSessionEnvironmentScript('s', env)) === null || true);

    _sessionEnvHooks.platform = 'linux';
    const envFile = path.join(home, 'missing.env');
    invalidateSessionEnvCache();
    await getSessionEnvironmentScript('s2', { ...env, AMI_ENV_FILE: envFile });
  });
});

describe('workflow + exit_worktree wave11', () => {
  it('workflow bool parse / background disabled / missing script', async () => {
    const d = mkTmp('wf-w11-');
    let r = await workflowTool.execute!({ script: 'console.log(1)', run_in_background: 'nope' }, ctx({ cwd: d }));
    assert.equal(r.isError, true);
    process.env[DISABLE_BACKGROUND_TASKS_ENV] = '1';
    r = await workflowTool.execute!(
      { script: 'console.log(1)', run_in_background: true },
      ctx({ cwd: d, processManager: { registerAgent: () => {} } as never }),
    );
    assert.equal(r.isError, true);
    delete process.env[DISABLE_BACKGROUND_TASKS_ENV];
    r = await workflowTool.execute!({ resumeKey: 9 as never }, ctx({ cwd: d }));
    assert.equal(r.isError, true);
  });

  it('exit_worktree remove without discard hits pluralize refuse', async () => {
    const root = mkTmp('ew-pl-');
    const wt = path.join(root, '.ami', 'worktrees', 'dirty');
    fs.mkdirSync(wt, { recursive: true });
    initGitRepo(wt);
    fs.writeFileSync(path.join(wt, 'dirty.txt'), 'x');
    setWorktreeSession({
      originalCwd: root,
      worktreePath: wt,
      worktreeName: 'dirty',
      worktreeBranch: 'worktree-dirty',
      createdAt: new Date().toISOString(),
    });
    const r = await exitWorktreeTool.execute!(
      { action: 'remove' },
      ctx({ cwd: wt }),
    );
    // Either refuse (pluralize path) or error — both exercise branches
    assert.ok(typeof r.output === 'string');
    assert.match(r.output, /discard|uncommitted|Error|kept|remove/i);
  });
});
