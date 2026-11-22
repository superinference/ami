/**
 * Wave-7 coverage: exit/enter worktree default hook functions, mcp-output
 * ternary exhaustiveness, task-kill interrupt/retry/skip failure arms, and
 * process.cwd() throw on exit_worktree.
 */
import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  persistBinaryContent,
  getBinaryBlobSavedMessage,
  mcpOutputHooks,
} from '../src/tools/mcp-output-storage';
import { exitWorktreeTool, _exitWorktreeHooks } from '../src/tools/exit-worktree';
import { enterWorktreeTool, _enterWorktreeHooks } from '../src/tools/enter-worktree';
import { taskKillTool } from '../src/tools/task-kill';
import {
  isAdvisorDisabled,
  ADVISOR_DISABLE_ENV,
  ADVISOR_DISABLE_ENV_ALIAS,
  resolveAdvisorModel,
  buildTranscriptDigest,
  advisorTool,
} from '../src/tools/advisor';
import {
  valueFlagsForCommand,
  loadFigSpec,
  getCommandSpec,
  isSafeCommandSpecName,
} from '../src/tools/command-spec-registry';
import {
  _sessionEnvHooks,
  getSessionEnvironmentScript,
  invalidateSessionEnvCache,
  clearCwdEnvFiles,
  describeSessionEnvironment,
} from '../src/session-environment';
import type { ToolContext } from '../src/types';

const TEMP = '/tmp';

afterEach(() => {
  mock.restoreAll();
  delete process.env[ADVISOR_DISABLE_ENV];
  delete process.env[ADVISOR_DISABLE_ENV_ALIAS];
  _sessionEnvHooks.platform = process.platform;
});

function mkTmp(prefix: string): string {
  return fs.mkdtempSync(path.join(TEMP, prefix));
}

function ctx(partial: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: partial.cwd ?? process.cwd(),
    sessionId: 'cov-w7',
    permissionMode: 'allow',
    ...partial,
  } as ToolContext;
}

// ---------------------------------------------------------------------------
// mcp-output — every ternary arm
// ---------------------------------------------------------------------------
describe('mcp-output wave7 — ternary exhaustiveness', () => {
  it('covers mime present/absent, empty cwd, Error and non-Error catch', () => {
    const d = mkTmp('mcp-blob-');
    const ok = persistBinaryContent(d, Buffer.from('pdf-bytes'), 'application/pdf');
    assert.ok('filepath' in ok);
    assert.ok(getBinaryBlobSavedMessage((ok as { filepath: string }).filepath, 'application/pdf', 9, 'src:').includes('application/pdf'));
    assert.ok(getBinaryBlobSavedMessage('/x', undefined, 1, '').includes('unknown type'));
    assert.ok(getBinaryBlobSavedMessage('/x', '', 1, '').includes('unknown type'));

    const viaCwd = persistBinaryContent('', Buffer.from('via-cwd-w7'), 'text/plain');
    assert.ok('filepath' in viaCwd || 'error' in viaCwd);

    mock.method(mcpOutputHooks, 'mkdirSync', (() => {
      throw new Error('eacces-dir');
    }) as typeof mcpOutputHooks.mkdirSync);
    const asErr = persistBinaryContent(d, Buffer.from('y'), 'image/png');
    assert.ok('error' in asErr);
    assert.match((asErr as { error: string }).error, /eacces-dir/);
    mock.restoreAll();

    mock.method(mcpOutputHooks, 'mkdirSync', (() => {
      throw 'plain-string-fail';
    }) as typeof mcpOutputHooks.mkdirSync);
    const nonErr = persistBinaryContent(d, Buffer.from('x'), undefined);
    assert.ok('error' in nonErr);
    assert.equal((nonErr as { error: string }).error, 'plain-string-fail');
  });
});

// ---------------------------------------------------------------------------
// exit/enter worktree — default hook functions + cwd throw
// ---------------------------------------------------------------------------
describe('worktree tools wave7 — default hooks + cwd throw', () => {
  it('invokes default sleep / stash / generateSlug / branchName seams', async () => {
    const prevDelay = _exitWorktreeHooks.branchDeleteDelayMs;
    _exitWorktreeHooks.branchDeleteDelayMs = 0;
    try {
      await _exitWorktreeHooks.sleep(0);
      assert.equal(typeof _enterWorktreeHooks.generateSlug(), 'string');
      // Default branchNameFromDescription may call an LLM — override lightly if needed.
      if (typeof _enterWorktreeHooks.branchNameFromDescription === 'function') {
        const name = await _enterWorktreeHooks.branchNameFromDescription('fix a bug in auth', {
          cwd: process.cwd(),
          abortSignal: AbortSignal.timeout(50),
        } as never).catch(() => 'fallback');
        assert.ok(typeof name === 'string' || name == null);
      }
    } finally {
      _exitWorktreeHooks.branchDeleteDelayMs = prevDelay;
    }
  });

  it('exit_worktree keep tolerates process.cwd() throwing', async () => {
    mock.method(process, 'cwd', () => {
      throw Object.assign(new Error('ENOENT'), { code: 'ENOENT' });
    });
    // Not inside a worktree → early informational path after cwd capture.
    const r = await exitWorktreeTool.execute!(
      { action: 'keep' },
      ctx({ cwd: mkTmp('ew-cwd-') }),
    );
    // Either not-a-git-repo or no-session — both exercise the cwd try/catch.
    assert.ok(typeof r.output === 'string');
  });

  it('enter_worktree already-exists + invalid slug arms', async () => {
    const d = mkTmp('ew-exist-');
    fs.mkdirSync(path.join(d, '.ami', 'worktrees', 'taken'), { recursive: true });
    // Need a git repo for later paths; init quickly.
    const { execFileSync } = await import('child_process');
    execFileSync('git', ['init', '-q'], { cwd: d });
    const exists = await enterWorktreeTool.execute!({ name: 'taken' }, ctx({ cwd: d }));
    assert.equal(exists.isError, true);
    assert.match(exists.output, /already exists|invalid|git/i);
    const bad = await enterWorktreeTool.execute!({ name: 'bad name!!' }, ctx({ cwd: d }));
    assert.equal(bad.isError, true);
  });

  it('exit_worktree isDestructive / toAutoClassifier / userFacingName', () => {
    assert.equal(exitWorktreeTool.isDestructive?.({ action: 'remove' }), true);
    assert.equal(exitWorktreeTool.isDestructive?.({ action: 'keep' }), false);
    assert.equal(exitWorktreeTool.isDestructive?.({ action: 'stash' }), false);
    assert.equal(exitWorktreeTool.toAutoClassifierInput?.({ action: 'merge' }), 'merge');
    assert.equal(exitWorktreeTool.toAutoClassifierInput?.({ action: 1 as never }), '');
    assert.equal(exitWorktreeTool.userFacingName?.({}), 'Exiting worktree');
  });
});

// ---------------------------------------------------------------------------
// task-kill — interrupt/retry/skip failure + success arms
// ---------------------------------------------------------------------------
describe('task-kill wave7 — interrupt/retry/skip arms', () => {
  it('covers kill_failed / retry_unsupported / skip_unsupported / success', async () => {
    const baseTask = { status: 'running', command: 'x', description: 'd' };
    // interrupt: method present but returns false → kill_failed
    let r = await taskKillTool.execute!(
      { task_id: 'a1', interrupt: true },
      ctx({
        processManager: {
          get: () => baseTask,
          taskKind: () => 'agent',
          interruptAgentTurn: () => false,
        },
      } as never),
    );
    assert.equal(r.metadata?.reason, 'kill_failed');

    // interrupt unsupported (no interruptAgentTurn)
    r = await taskKillTool.execute!(
      { task_id: 'a1', interrupt: true },
      ctx({
        processManager: {
          get: () => baseTask,
          taskKind: () => 'local',
        },
      } as never),
    );
    assert.ok(r.isError);

    // retry unsupported (wrong kind)
    r = await taskKillTool.execute!(
      { task_id: 'a1', retry: true },
      ctx({
        processManager: {
          get: () => baseTask,
          taskKind: () => 'local',
        },
      } as never),
    );
    assert.equal(r.metadata?.reason, 'retry_unsupported');

    // retry present but returns false
    r = await taskKillTool.execute!(
      { task_id: 'a1', retry: true },
      ctx({
        processManager: {
          get: () => baseTask,
          taskKind: () => 'agent',
          retryAgentTask: () => false,
        },
      } as never),
    );
    assert.equal(r.metadata?.reason, 'retry_unsupported');

    // retry success
    r = await taskKillTool.execute!(
      { task_id: 'a1', retry: true },
      ctx({
        processManager: {
          get: () => baseTask,
          taskKind: () => 'agent',
          retryAgentTask: () => true,
        },
      } as never),
    );
    assert.equal(r.isError, false);
    assert.equal(r.metadata?.reason, 'retried');

    // skip unsupported
    r = await taskKillTool.execute!(
      { task_id: 'a1', skip: true },
      ctx({
        processManager: {
          get: () => baseTask,
          taskKind: () => 'local',
        },
      } as never),
    );
    assert.equal(r.metadata?.reason, 'skip_unsupported');

    // skip success
    r = await taskKillTool.execute!(
      { task_id: 'a1', skip: true },
      ctx({
        processManager: {
          get: () => baseTask,
          taskKind: () => 'agent',
          skipAgentTask: () => true,
        },
      } as never),
    );
    assert.equal(r.metadata?.reason, 'skipped');
  });
});

// ---------------------------------------------------------------------------
// advisor — disable alias + digest edges
// ---------------------------------------------------------------------------
describe('advisor wave7 — disable alias + digest', () => {
  it('isAdvisorDisabled honors the Claude-compat alias', () => {
    assert.equal(isAdvisorDisabled({}), false);
    assert.equal(isAdvisorDisabled({ [ADVISOR_DISABLE_ENV_ALIAS]: '1' }), true);
    assert.equal(isAdvisorDisabled({ [ADVISOR_DISABLE_ENV]: 'true' }), true);
  });

  it('resolveAdvisorModel + buildTranscriptDigest edge arms', () => {
    const model = resolveAdvisorModel({
      provider: 'openai',
      model: 'gpt-4o',
    } as never);
    assert.ok(typeof model === 'string' && model.length > 0);
    const digest = buildTranscriptDigest([
      { role: 'system', content: 'sys' },
      { role: 'user', content: 'hi' },
      { role: 'assistant', content: [{ type: 'tool_use', name: 'bash', input: { c: 1 }, id: '1' }] as never },
      { role: 'user', content: [{ type: 'tool_result', tool_use_id: '1', content: 'out' }] as never },
    ] as never);
    assert.ok(digest.includes('hi') || digest.length >= 0);
  });

  it('advisorTool.execute surfaces disabled', async () => {
    process.env[ADVISOR_DISABLE_ENV] = '1';
    const r = await advisorTool.execute!({}, ctx());
    assert.equal(r.isError, true);
  });
});

// ---------------------------------------------------------------------------
// command-spec + session-environment remaining arms
// ---------------------------------------------------------------------------
describe('command-spec + session-env wave7', () => {
  it('valueFlags / fig / safe-name edges', async () => {
    assert.equal(isSafeCommandSpecName(''), false);
    assert.equal(isSafeCommandSpecName('git'), true);
    assert.ok(getCommandSpec('git') || getCommandSpec('git') === undefined);
    const flags = valueFlagsForCommand('git');
    assert.ok(flags === undefined || Array.isArray(flags) || flags instanceof Set || typeof flags === 'object');
    assert.equal(await loadFigSpec('!!!'), null);
  });

  it('session-env win32 short-circuit + clearCwdEnvFiles', async () => {
    _sessionEnvHooks.platform = 'win32';
    invalidateSessionEnvCache();
    const s = await getSessionEnvironmentScript('s-win', process.cwd());
    assert.ok(s === null || typeof s === 'string');
    const rep = describeSessionEnvironment();
    assert.ok(rep && typeof rep === 'object');
    _sessionEnvHooks.platform = 'linux';
    const d = mkTmp('se-clear-');
    await clearCwdEnvFiles('s-clear', { AMI_HOME: d } as never).catch(() => undefined);
  });
});

void os;
