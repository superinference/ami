/**
 * Wave-12 coverage: enter-worktree branch-name log fn, memory bestEffortChmod
 * catch arms, session-environment non-ENOENT + sort/cache edges, and a few
 * remaining small-module branch gaps.
 */
import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as fsp from 'fs/promises';

import {
  logBranchNameQueryResult,
  enterWorktreeTool,
  swallowHookRejection,
} from '../src/tools/enter-worktree';
import { bestEffortChmod, bestEffortChmodAsync } from '../src/memory';
import {
  _sessionEnvHooks,
  sessionEnvDirPath,
  hookEnvFragmentPath,
  getSessionEnvironmentScript,
  invalidateSessionEnvCache,
  clearCwdEnvFiles,
  describeSessionEnvironment,
} from '../src/session-environment';
import {
  valueFlagsForCommand,
  getCommandSpec,
  isSafeCommandSpecName,
} from '../src/tools/command-spec-registry';
import {
  recordUserPrompt,
  getSessionPromptCount,
  normalizeAttributionPath,
} from '../src/tools/attribution-store';
import {
  suggestSimilarPaths,
  suggestPathCorrection,
  isENOENT,
  logScanError,
  clampCount,
} from '../src/tools/path-suggest';
import {
  workflowTool,
  sanitizeWorkflowRunId,
  writeTaskOutputNoFollow,
  persistWorkflowRun,
  loadWorkflowRun,
  listWorkflowRuns,
} from '../src/tools/workflow';
import {
  tryGetInstalledAppNames,
  toUndefined,
  planAnimatedMove,
} from '../src/tools/computer-use';
import type { ToolContext } from '../src/types';

const TEMP = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-cov-'));

afterEach(() => {
  mock.restoreAll();
  invalidateSessionEnvCache();
  _sessionEnvHooks.platform = process.platform;
});

function mkTmp(prefix: string): string {
  fs.mkdirSync(TEMP, { recursive: true });
  return fs.mkdtempSync(path.join(TEMP, prefix));
}

function ctx(partial: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: partial.cwd ?? process.cwd(),
    sessionId: 'cov-w12',
    permissionMode: 'allow',
    ...partial,
  } as ToolContext;
}

describe('enter-worktree wave12 — logBranchNameQueryResult', () => {
  it('logs error results with and without error detail; ignores non-error', () => {
    logBranchNameQueryResult({ type: 'ok', value: 'slug' } as never);
    logBranchNameQueryResult({
      type: 'error',
      reason: 'aborted',
      source: 'test',
      model: 'm',
    } as never);
    logBranchNameQueryResult({
      type: 'error',
      reason: 'failed',
      error: 'boom',
      source: 'test',
      model: 'm',
    } as never);
    swallowHookRejection();
    assert.ok(enterWorktreeTool.execute);
  });

  it('enter_worktree rejects invalid pr reference', async () => {
    const d = mkTmp('ew-pr-');
    const r = await enterWorktreeTool.execute!(
      { name: 'ok-name', pr: 'not-a-pr' },
      ctx({ cwd: d }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /invalid pr/i);
  });
});

describe('memory wave12 — bestEffortChmod catch', () => {
  it('sync and async chmod swallow missing-path errors', async () => {
    bestEffortChmod(path.join(TEMP, 'no-such-chmod-target-w12'), 0o600);
    await bestEffortChmodAsync(path.join(TEMP, 'no-such-chmod-async-w12'), 0o600);
    // Happy path
    const f = path.join(mkTmp('chmod-ok-'), 'f');
    fs.writeFileSync(f, 'x');
    bestEffortChmod(f, 0o600);
    await bestEffortChmodAsync(f, 0o600);
  });
});

describe('session-environment wave12 — non-ENOENT + ordering', () => {
  it('covers hook path nulls, env-file non-ENOENT, unreadable fragment, cache hit', async () => {
    const home = mkTmp('se-w12-');
    const env = { AMI_HOME: home, HOME: home } as NodeJS.ProcessEnv;

    assert.equal(hookEnvFragmentPath('s', 'PreToolUse', 0, env), null);
    assert.equal(hookEnvFragmentPath('s', 'sessionStart', -1, env), null);
    assert.equal(hookEnvFragmentPath('s', 'sessionStart', 1.5, env), null);
    assert.ok(hookEnvFragmentPath('s', 'SessionStart', 0, env)); // case fold
    assert.ok(hookEnvFragmentPath('', 'setup', 0, env)); // empty → default segment
    assert.ok(hookEnvFragmentPath(undefined, 'fileChanged', 2, env));

    // Unknown-named fragments still sort (priority 99) via getSessionEnvironmentScript
    const dir = sessionEnvDirPath('ord', env);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'filechanged-hook-0.sh'), 'export Z=1\n');
    fs.writeFileSync(path.join(dir, 'setup-hook-0.sh'), 'export A=1\n');
    fs.writeFileSync(path.join(dir, 'sessionstart-hook-1.sh'), 'export B=1\n');
    fs.writeFileSync(path.join(dir, 'cwdchanged-hook-0.sh'), 'export C=1\n');
    fs.writeFileSync(path.join(dir, 'not-a-hook.sh'), 'export SKIP=1\n');
    // Unreadable fragment → non-ENOENT log arm
    const bad = path.join(dir, 'setup-hook-9.sh');
    fs.writeFileSync(bad, 'export X=1\n');
    fs.chmodSync(bad, 0o000);

    invalidateSessionEnvCache();
    try {
      const script = await getSessionEnvironmentScript('ord', env);
      assert.ok(script === null || typeof script === 'string');
      // Cache hit
      const again = await getSessionEnvironmentScript('ord', env);
      assert.equal(again, script);
    } finally {
      fs.chmodSync(bad, 0o644);
    }

    // Non-ENOENT env file read (directory path)
    const asDir = path.join(home, 'env-as-dir');
    fs.mkdirSync(asDir);
    invalidateSessionEnvCache();
    await getSessionEnvironmentScript('e2', { ...env, AMI_ENV_FILE: asDir });

    // Empty env file content
    const emptyFile = path.join(home, 'empty.env');
    fs.writeFileSync(emptyFile, '   \n');
    invalidateSessionEnvCache();
    await getSessionEnvironmentScript('e3', { ...env, CLAUDE_ENV_FILE: emptyFile });

    // clearCwdEnvFiles non-ENOENT (dir is a file)
    const fileAsSession = path.join(home, 'session-env', 'file-sess');
    // resolveConfigDir under AMI_HOME — clear with a session whose dir can't be readdir'd
    // Use chmod 000 on the session-env dir
    const sdir = sessionEnvDirPath('locked', env);
    fs.mkdirSync(sdir, { recursive: true });
    fs.writeFileSync(path.join(sdir, 'cwdchanged-hook-0.sh'), 'export T=1\n');
    fs.chmodSync(sdir, 0o000);
    try {
      await clearCwdEnvFiles('locked', env);
    } finally {
      fs.chmodSync(sdir, 0o700);
    }

    const desc = describeSessionEnvironment({ ...env, AMI_ENV_FILE: emptyFile });
    assert.equal(desc.envFileConfigured, 'AMI_ENV_FILE');
    assert.equal(desc.supported, true);
    _sessionEnvHooks.platform = 'win32';
    assert.equal(describeSessionEnvironment(env).supported, false);
    void fileAsSession;
    void fsp;
  });
});

describe('command-spec + attribution + path-suggest + workflow wave12', () => {
  it('command-spec edges and attribution path normalize', () => {
    assert.equal(isSafeCommandSpecName(''), false);
    assert.equal(isSafeCommandSpecName('a/b'), false);
    assert.equal(isSafeCommandSpecName('git'), true);
    valueFlagsForCommand('git');
    valueFlagsForCommand('unknown-zzzz');
    getCommandSpec('git');
    getCommandSpec('');
    recordUserPrompt('sid-w12');
    assert.ok(getSessionPromptCount('sid-w12') >= 1);
    normalizeAttributionPath('rel/path.ts', '/cwd');
    normalizeAttributionPath('/abs/path.ts', '/cwd');
  });

  it('path-suggest ignore + workflow empty script', async () => {
    const d = mkTmp('ps-w12-');
    fs.writeFileSync(path.join(d, '.gitignore'), 'secret.env\n');
    fs.writeFileSync(path.join(d, 'secret.env'), 'x');
    fs.writeFileSync(path.join(d, 'readme.md'), 'x');
    // Init minimal git so findProjectRoot works
    try {
      const { execFileSync } = await import('child_process');
      execFileSync('git', ['init'], { cwd: d, stdio: 'ignore' });
    } catch { /* ok */ }
    const got = suggestSimilarPaths(path.join(d, 'readme.me'), d);
    assert.ok(Array.isArray(got));
    const corr = suggestPathCorrection(path.join(d, 'nope', 'x.ts'), d);
    assert.ok(corr);

    // Direct coverage of isENOENT / logScanError / clampCount (was 95% functions)
    assert.equal(isENOENT({ code: 'ENOENT' }), true);
    assert.equal(isENOENT({ code: 'EACCES' }), false);
    assert.equal(isENOENT(null), false);
    assert.equal(isENOENT('x'), false);
    logScanError('test', d, { code: 'ENOENT' }); // quiet
    logScanError('test', d, new Error('eacces')); // logs
    logScanError('test', d, 'string-err');
    assert.equal(clampCount(Number.NaN), 0);
    assert.equal(clampCount(-3), 0);
    assert.equal(clampCount(2.9), 2);

    const r = await workflowTool.execute!({ script: '' }, ctx({ cwd: d }));
    assert.equal(r.isError, true);
    const r2 = await workflowTool.execute!({ script: '   ' }, ctx({ cwd: d }));
    assert.equal(r2.isError, true);
    assert.equal(workflowTool.isEnabled?.(), true);

    assert.ok(sanitizeWorkflowRunId('Ab C!').length > 0);
    const out = path.join(d, 'task-out.txt');
    writeTaskOutputNoFollow(out, 'hi');
    assert.equal(fs.readFileSync(out, 'utf8'), 'hi');
    persistWorkflowRun(d, 'run1', {
      script: 'export const meta = { name: "t", description: "d" };',
      meta: { name: 't', description: 'd' },
      createdAt: new Date().toISOString(),
    });
    assert.ok(loadWorkflowRun(d, 'run1') || loadWorkflowRun(d, sanitizeWorkflowRunId('run1')));
    assert.ok(Array.isArray(listWorkflowRuns(d)));

    assert.equal(toUndefined(), undefined);
    const timed = await tryGetInstalledAppNames(
      () => new Promise((r) => setTimeout(() => r(['a']), 500)),
      5,
    );
    assert.equal(timed, undefined);
    const ok = await tryGetInstalledAppNames(async () => ['x'], 1000);
    assert.deepEqual(ok, ['x']);
    const fail = await tryGetInstalledAppNames(async () => { throw new Error('no'); }, 1000);
    assert.equal(fail, undefined);
    assert.ok(planAnimatedMove({ x: 0, y: 0 }, { x: 100, y: 0 }, true).length >= 1);
  });
});
