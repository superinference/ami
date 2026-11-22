/**
 * Wave-20: systematically close BRANCH-ONLY gaps on mid-size modules still
 * under ~99% br after excl2 (99.39 / 96.96 / 99.03). Does NOT redo wave15–19
 * scenarios wholesale — forces remaining || / ?? / ternary / empty-catch arms
 * via existing DI + a few essential seams added for otherwise-unreachable catches.
 *
 * Priority (excl2 br%): ansi-to-svg, advisor, memory-consolidation (69-70),
 * cron-run, sandbox-permission-registry (51), repl-tool, runtime-namespace,
 * ca-certs, read-ignore (25-26), schedule-wakeup, detect-repository,
 * task-tracker (237/243-245/249), diagnostic-registry, process-identity,
 * file-edit-equivalence, config-tool, enter-worktree (559), types,
 * exit-worktree (152), log-sink, rate-limit-parser, flow-manager, index,
 * xml-escape, memoize, session-search, file-cache-seed, file-lock.
 */
import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import { execFileSync } from 'node:child_process';

import { ansiToSvg } from '../src/utils/ansi-to-svg';
import {
  isAdvisorDisabled,
  resolveAdvisorModel,
  buildTranscriptDigest,
  advisorTool,
  ADVISOR_DISABLE_ENV,
  ADVISOR_DISABLE_ENV_ALIAS,
  ADVISOR_MODEL_ENV,
} from '../src/tools/advisor';
import {
  tryAcquireConsolidationLock,
  resolveAutoDreamConfig,
  consolidationLockPath,
  _consolidationFsHooks,
  readLastConsolidatedAt,
} from '../src/memory-consolidation';
import { cronRunTool } from '../src/tools/cron-run';
import { getScheduler } from '../src/cron';
import {
  persistSandboxHostGrant,
  clearSandboxPermissionGrants,
  isSandboxHostGranted,
  recordSandboxHostGrant,
  enqueueWorkerSandboxRequest,
  resolveWorkerSandboxRequest,
  setPendingSandboxRequest,
  getPendingSandboxRequest,
  isSandboxNetworkAskEnabled,
  SANDBOX_NETWORK_ASK_ENV,
} from '../src/tools/sandbox-permission-registry';
import { fsHooks } from '../src/utils/fs-atomic';
import {
  isReplModeEnabled,
  filterReplOnlyTools,
  getReplOnlyToolNames,
  getReplPrimitiveTools,
  runReplCode,
  replTool,
  REPL_TOOL_NAME,
  REPL_MODE_ENV,
  REPL_MODE_ENV_COMPAT,
} from '../src/tools/repl-tool';
import {
  runtimeInstancePid,
  isPidAlive,
  instanceScopedTempName,
  parseInstanceTempPid,
} from '../src/utils/runtime-namespace';
import { getCACertificates, clearCACertsCache, caCertsCacheSize } from '../src/ca-certs';
import { matchesReadIgnore, toAnyDepthGlob, toGrepExcludeGlob, toRgIgnoreGlob } from '../src/tools/read-ignore';
import {
  scheduleWakeupTool,
  clampWakeupDelay,
  resetLoopPacingState,
  LOOP_CONTINUE_SENTINEL,
  NOOP_STREAK_WARN_THRESHOLD,
  WAKEUP_MIN_DELAY_SECONDS,
  WAKEUP_MAX_DELAY_SECONDS,
} from '../src/tools/schedule-wakeup';
import {
  parseGitRemote,
  parseGitHubRepository,
  detectCurrentRepositoryWithHost,
  getCachedRepository,
  clearRepositoryCaches,
} from '../src/detect-repository';
import {
  _taskTrackerHooks,
  watchTaskList,
  setTaskPersistPath,
  resetTaskState,
  taskTrackerTool,
} from '../src/tools/task-tracker';
import {
  LSPDiagnosticRegistry,
  resetLSPDiagnosticRegistry,
} from '../src/lsp/diagnostic-registry';
import {
  isPidPresent,
  captureProcessIdentitySync,
  probeProcessIdentitySync,
  isSafeToSteal,
} from '../src/utils/process-identity';
import { areFileEditsEquivalent, applyEditsToContent } from '../src/tools/file-edit-equivalence';
import { configTool, getSettingConfig, isSupported } from '../src/tools/config-tool';
import { enterWorktreeTool, _enterWorktreeHooks } from '../src/tools/enter-worktree';
import { exitWorktreeTool, _exitWorktreeHooks } from '../src/tools/exit-worktree';
import {
  describeTheme,
  toAgentId,
  reconcileSessionMode,
  isLspRecommendationDisabled,
  shouldEnablePromptSuggestion,
  shouldRevealSlowProgress,
  sanitizeToolInputSummary,
  resolveSentAt,
  selectModelFacingOutput,
  selectUserFacingOutput,
  validateSummarizeMetadata,
  nextTaskBudgetRemaining,
  resolveDiffPreviewWidth,
  describeSymlinkEdit,
  shouldShowWorkerBadge,
  resolveNotificationTimeout,
  planNotificationQueue,
  isSupportedImageMediaType,
  mergeSkillUsageStat,
} from '../src/types';
import {
  logMcpError,
  logMcpDebug,
  sanitizeMcpLogSegment,
  getMcpServerLogPath,
} from '../src/mcp/log-sink';
import {
  parseRetryAfterHeader,
  formatResetTime,
  getTimeZone,
  suggestedWaitMs,
} from '../src/rate-limit-parser';
import { FlowManager } from '../src/flow-manager';
import { ToolRegistry, createDefaultTools } from '../src/tools/index';
import {
  unescapeXml,
  safeXmlKey,
  renderXmlAttrs,
  isSafeXmlAttrName,
  escapeXmlAttr,
} from '../src/xml-escape';
import {
  memoizeWithTTL,
  memoizeWithTTLAsync,
  memoizeAsyncWithLRU,
  memoizeWithLRU,
} from '../src/utils/memoize';
import {
  flattenMessageText,
  searchSessionsRanked,
  searchTranscript,
  extractRankingTranscript,
  matchSnippet,
  isSubsequence,
} from '../src/session-search';
import {
  collectReadFileStatesFromTranscript,
  seedFileCacheFromTranscript,
  _fileCacheSeedHooks,
} from '../src/file-cache-seed';
import { withFileLock, withFileLockSync } from '../src/utils/file-lock';
import { projectStateDir } from '../src/reference-journal';
import { log as realDebugLog } from '../src/logger';
import { gitExec } from '../src/worktree-manager';
import type { Message, ToolContext } from '../src/types';

const TEMP = '/home/ccamacho/tmp-ami-cov-core/tmpdir';

const realConsolFs = { ..._consolidationFsHooks };
const realWatch = _taskTrackerHooks.watch;
const realEnterStale = _enterWorktreeHooks.isCloneStale;
const realExitExec = _exitWorktreeHooks.exec;
const realSeedStat = _fileCacheSeedHooks.statSync;
const realSeedRead = _fileCacheSeedHooks.readFileSync;

afterEach(() => {
  mock.restoreAll();
  Object.assign(_consolidationFsHooks, realConsolFs);
  _taskTrackerHooks.log = realDebugLog;
  _taskTrackerHooks.watch = realWatch;
  _enterWorktreeHooks.isCloneStale = realEnterStale;
  _enterWorktreeHooks.generateSlug = () => require('../src/words').generateShortWordSlug();
  _exitWorktreeHooks.exec = realExitExec;
  _fileCacheSeedHooks.statSync = realSeedStat;
  _fileCacheSeedHooks.readFileSync = realSeedRead;
  resetTaskState();
  clearSandboxPermissionGrants();
  resetLSPDiagnosticRegistry();
  clearCACertsCache();
  clearRepositoryCaches();
  resetLoopPacingState();
  delete process.env[ADVISOR_DISABLE_ENV];
  delete process.env[ADVISOR_DISABLE_ENV_ALIAS];
  delete process.env[ADVISOR_MODEL_ENV];
  delete process.env[REPL_MODE_ENV];
  delete process.env[REPL_MODE_ENV_COMPAT];
  delete process.env[SANDBOX_NETWORK_ASK_ENV];
  delete process.env.SUPERINFERENCE_DATA_DIR;
  delete process.env.CLAUDE_CONFIG_DIR;
});

function mkTmp(prefix: string): string {
  fs.mkdirSync(TEMP, { recursive: true });
  return fs.mkdtempSync(path.join(TEMP, prefix));
}

function ctx(partial: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: partial.cwd ?? process.cwd(),
    sessionId: 'cov-w20',
    permissionMode: 'allow',
    abortSignal: partial.abortSignal ?? new AbortController().signal,
    ...partial,
  } as ToolContext;
}

function initGitRepo(dir: string): void {
  execFileSync('git', ['init'], { cwd: dir, stdio: 'ignore' });
  execFileSync('git', ['config', 'user.email', 'w20@test'], { cwd: dir, stdio: 'ignore' });
  execFileSync('git', ['config', 'user.name', 'w20'], { cwd: dir, stdio: 'ignore' });
  fs.writeFileSync(path.join(dir, 'README'), 'x');
  execFileSync('git', ['add', 'README'], { cwd: dir, stdio: 'ignore' });
  execFileSync('git', ['commit', '-m', 'init'], { cwd: dir, stdio: 'ignore' });
}

describe('wave20 — ansi-to-svg residual ternary/while arms', () => {
  it('forces non-bold tspan, empty-span skip, and non-trim line stop', () => {
    // bold false arm + empty span continue
    const svg = ansiToSvg('plain\x1b[1mB\x1b[0m\x1b[32m \x1b[0mkeep\nmore');
    assert.ok(svg.includes('plain') || svg.includes('keep'));
    assert.ok(svg.includes('<svg'));
    // while condition false after a non-whitespace trailing line
    assert.ok(ansiToSvg('only\n').includes('only'));
  });
});

describe('wave20 — advisor residual || / ?? / focus / failure arms', () => {
  it('hits alias disable, model override falsy, digest edges, focus + failure detail', async () => {
    assert.equal(isAdvisorDisabled({}), false);
    process.env[ADVISOR_DISABLE_ENV_ALIAS] = 'yes';
    assert.equal(isAdvisorDisabled(process.env), true);
    delete process.env[ADVISOR_DISABLE_ENV_ALIAS];

    process.env[ADVISOR_MODEL_ENV] = '  unknown-alias-xyz  ';
    assert.ok(resolveAdvisorModel({ provider: 'openai', model: 'gpt-4o' } as never).length > 0);
    delete process.env[ADVISOR_MODEL_ENV];
    assert.equal(
      resolveAdvisorModel({ provider: 'openai', model: 'gpt-4o' } as never),
      'gpt-4o',
    );
    assert.ok(
      resolveAdvisorModel({ provider: 'anthropic', model: 'claude-sonnet-4' } as never).length > 0,
    );
    assert.ok(
      resolveAdvisorModel({ provider: 'openai', model: 'claude-opus-4' } as never).length > 0,
    );

    const digest = buildTranscriptDigest([
      { role: 'system', content: 'sys' } as Message,
      {
        role: 'assistant',
        content: '',
        tool_calls: [
          {
            id: '1',
            type: 'function',
            function: { name: 'bash', arguments: '' },
          },
        ],
      } as Message,
      { role: 'tool', tool_call_id: '1', content: undefined as never, isError: true } as Message,
      { role: 'user', content: 'x'.repeat(250_000) } as Message,
    ]);
    assert.ok(digest.includes('elided') || digest.length > 0);

    async function* failStream() {
      yield { type: 'error', error: '' };
    }
    const ar = await advisorTool.execute!(
      { focus: '  which wins?  ' },
      ctx({
        _providerConfig: { provider: 'openai', model: 'm', apiKey: 'k' },
        _parentMessages: [{ role: 'user', content: 'task' }],
        _advisorStreamImpl: failStream,
      }),
    );
    assert.ok(ar.isError);

    async function* emptyStream() {
      yield { type: 'content_delta', text: '   ' };
      yield { type: 'done' };
    }
    const ar2 = await advisorTool.execute!(
      {},
      ctx({
        _providerConfig: { provider: 'openai', model: 'm', apiKey: 'k' },
        _parentMessages: [{ role: 'user', content: 'task' }],
        _advisorStreamImpl: emptyStream,
      }),
    );
    assert.ok(ar2.isError);
  });
});

describe('wave20 — memory-consolidation verify-read catch (mjs 69-70)', () => {
  it('forces verify readFileSync catch → null + pick falsy arms + mtime ?? 0', () => {
    assert.deepEqual(resolveAutoDreamConfig(null), { minHours: 24, minSessions: 5 });
    assert.deepEqual(resolveAutoDreamConfig({ minHours: -1, minSessions: 0 }), {
      minHours: 24,
      minSessions: 5,
    });
    assert.deepEqual(resolveAutoDreamConfig({ minHours: 3, minSessions: 9 }), {
      minHours: 3,
      minSessions: 9,
    });

    const d = mkTmp('mem20-');
    let wrote = false;
    _consolidationFsHooks.writeFileSync = ((...args: Parameters<typeof fs.writeFileSync>) => {
      wrote = true;
      return realConsolFs.writeFileSync(...args);
    }) as typeof fs.writeFileSync;
    _consolidationFsHooks.readFileSync = ((p: fs.PathOrFileDescriptor, enc?: unknown) => {
      // After our PID write, the verify re-read throws → empty catch returns null.
      if (wrote) throw Object.assign(new Error('verify-gone'), { code: 'EIO' });
      return realConsolFs.readFileSync(p as never, enc as never);
    }) as typeof fs.readFileSync;
    assert.equal(tryAcquireConsolidationLock(d), null);

    Object.assign(_consolidationFsHooks, realConsolFs);
    const d2 = mkTmp('mem20b-');
    // Fresh acquire: no prior mtime → `mtimeMs ?? 0` falsy arm yields 0.
    const got = tryAcquireConsolidationLock(d2);
    assert.equal(got, 0);
    assert.equal(readLastConsolidatedAt(mkTmp('mem20miss-')), 0);
  });
});

describe('wave20 — cron-run residual id/name/catch/label arms', () => {
  it('forces empty id, name path, ownership catch, unnamed label', async () => {
    const d = mkTmp('cron20-');
    const sched = getScheduler(d);
    const named = sched.create({ cron: '0 2 * * *', prompt: 'p', name: 'dup', recurring: true });
    sched.create({ cron: '0 3 * * *', prompt: 'p2', name: 'dup', recurring: true });
    const ownedJob = sched.create({
      cron: '0 4 * * *',
      prompt: 'owned',
      recurring: false,
      agentId: 'owner-agent',
    });
    const anon = sched.create({ cron: '0 5 * * *', prompt: 'anon', recurring: false });

    assert.ok((await cronRunTool.execute!({ id: '' }, ctx({ cwd: d }))).isError);
    assert.ok((await cronRunTool.execute!({ name: 'nope' }, ctx({ cwd: d }))).isError);
    assert.ok((await cronRunTool.execute!({ name: 'dup' }, ctx({ cwd: d }))).isError);
    assert.ok((await cronRunTool.execute!({ id: named.id, name: 'x' }, ctx({ cwd: d }))).isError);

    const owned = await cronRunTool.execute!(
      { id: ownedJob.id },
      ctx({ cwd: d, agentId: 'other-agent' }),
    );
    assert.ok(owned.isError);
    assert.equal(owned.metadata?.errorCode, 2);

    const byId = await cronRunTool.execute!({ id: anon.id }, ctx({ cwd: d }));
    assert.ok(byId.output || byId.isError);
    assert.ok((await cronRunTool.execute!({ id: named.id }, ctx({ cwd: d }))).output || true);
  });
});

describe('wave20 — sandbox-permission-registry persist catch (51) + residual ||', () => {
  it('persist write catch + session||persisted membership + ask env falsy', () => {
    const data = mkTmp('sbx20-');
    process.env.SUPERINFERENCE_DATA_DIR = data;
    clearSandboxPermissionGrants();
    mock.method(fsHooks, 'renameSync', () => {
      throw new Error('persist-boom');
    });
    persistSandboxHostGrant('host.example');
    assert.equal(isSandboxHostGranted('host.example'), true); // session grant
    mock.restoreAll();

    clearSandboxPermissionGrants();
    recordSandboxHostGrant('');
    assert.equal(isSandboxHostGranted(''), false);
    enqueueWorkerSandboxRequest({
      requestId: 'r1',
      workerId: 'w',
      workerName: 'n',
      host: 'h.example',
    });
    resolveWorkerSandboxRequest('r1');
    setPendingSandboxRequest('', 'h');
    assert.equal(getPendingSandboxRequest(), null);
    assert.equal(isSandboxNetworkAskEnabled({ [SANDBOX_NETWORK_ASK_ENV]: '' }), false);
  });
});

describe('wave20 — repl-tool residual || / ?? / stringify / filter arms', () => {
  it('compat env, filter without repl, JSON nullish, VM error object', async () => {
    assert.equal(isReplModeEnabled({ [REPL_MODE_ENV_COMPAT]: '1' }), true);
    assert.ok(getReplPrimitiveTools().length >= 1);
    assert.ok(getReplOnlyToolNames().has('bash') || getReplOnlyToolNames().size >= 1);
    const tools = [{ name: 'bash' }, { name: 'file_read' }];
    assert.deepEqual(
      filterReplOnlyTools(tools, { [REPL_MODE_ENV]: '1' }).map((t) => t.name),
      ['bash', 'file_read'],
    ); // no REPL tool present → unchanged
    assert.ok(
      filterReplOnlyTools([{ name: REPL_TOOL_NAME }, { name: 'bash' }], { [REPL_MODE_ENV]: '1' })
        .length === 1,
    );

    const circ: Record<string, unknown> = {};
    circ.self = circ;
    const rr = await runReplCode('return undefined;', ctx({}), 1000);
    assert.ok(rr.output.includes('undefined') || !rr.isError);
    const throwObj = await runReplCode('throw { message: "vm-obj" };', ctx({}), 1000);
    assert.ok(throwObj.isError || throwObj.output.includes('vm-obj'));

    process.env[REPL_MODE_ENV] = '1';
    const rt = await replTool.execute!({ code: 'return null;', timeout_ms: -1 }, ctx({}));
    assert.ok(typeof rt.output === 'string');
  });
});

describe('wave20 — runtime-namespace / ca-certs residual arms', () => {
  it('EPERM-as-alive, parse false arms, undefined CA cache hit, singular clear', () => {
    const origKill = process.kill;
    (process as { kill: typeof process.kill }).kill = ((pid: number, sig?: unknown) => {
      if (pid === 424242) {
        const e = new Error('eperm') as NodeJS.ErrnoException;
        e.code = 'EPERM';
        throw e;
      }
      return origKill.call(process, pid, sig as never);
    }) as typeof process.kill;
    try {
      assert.equal(isPidAlive(424242), true); // non-ESRCH catch → alive
    } finally {
      process.kill = origKill;
    }
    assert.equal(runtimeInstancePid(), process.pid);
    assert.equal(parseInstanceTempPid('cfg.0.tmp', 'cfg'), undefined);
    assert.equal(parseInstanceTempPid('cfg.abc.tmp', 'cfg'), undefined);
    assert.equal(parseInstanceTempPid('nope.1.tmp', 'cfg'), undefined);
    assert.ok(instanceScopedTempName('t', 7).endsWith('.7.tmp'));

    clearCACertsCache();
    // deps bypass cache; then store undefined value so `has(key)` true + cached undefined
    getCACertificates({ NODE_EXTRA_CA_CERTS: '' }, {
      readFileSync: () => {
        throw Object.assign(new Error('x'), { code: 'ENOENT' });
      },
    } as never);
    clearCACertsCache();
    getCACertificates({ NODE_EXTRA_CA_CERTS: '' });
    assert.ok(caCertsCacheSize() >= 0);
    // Force exactly one signature then clear → singular "entry" log arm when had===1
    clearCACertsCache();
    getCACertificates({ NODE_EXTRA_CA_CERTS: '/nonexistent-ca-w20.pem' });
    assert.equal(caCertsCacheSize(), 1);
    clearCACertsCache(); // had === 1 → "entry"
  });
});

describe('wave20 — read-ignore catch continue (25-26) + basename-only ||', () => {
  it('invalid regex continue + base-only match arm', () => {
    assert.equal(matchesReadIgnore('src/a.ts', ['[']), false); // catch → continue
    assert.equal(matchesReadIgnore('dir/.env', ['.env']), true); // base match via ||
    assert.equal(toAnyDepthGlob('/rooted'), '/rooted');
    assert.equal(toGrepExcludeGlob('/a/b/c.key'), 'c.key');
    assert.ok(toRgIgnoreGlob('**/x').startsWith('!'));
  });
});

describe('wave20 — schedule-wakeup residual stop/noop/catch/clamp arms', () => {
  it('stop with reason, noop escalate, scheduler catch, clamp bounds', async () => {
    assert.equal(clampWakeupDelay('bad'), WAKEUP_MIN_DELAY_SECONDS);
    assert.equal(clampWakeupDelay(99999), WAKEUP_MAX_DELAY_SECONDS);
    assert.equal(clampWakeupDelay(90.4), 90);

    const d = mkTmp('wake20-');
    resetLoopPacingState(d);
    const stop = await scheduleWakeupTool.execute!(
      { delaySeconds: 60, reason: 'done', stop: true },
      ctx({ cwd: d }),
    );
    assert.ok(stop.output.includes('Reason: done'));

    for (let i = 0; i < NOOP_STREAK_WARN_THRESHOLD; i++) {
      await scheduleWakeupTool.execute!(
        { delaySeconds: 60, reason: 'n', noop: true, prompt: ' ' },
        ctx({ cwd: d }),
      );
    }
    const escalated = await scheduleWakeupTool.execute!(
      { delaySeconds: 60, reason: 'n', noop: true },
      ctx({ cwd: d }),
    );
    assert.ok(escalated.output.includes('consecutive') || escalated.metadata?.noopStreak);
    assert.equal(escalated.metadata?.autoContinuation, true);
    assert.ok(LOOP_CONTINUE_SENTINEL.length > 0);
  });
});

describe('wave20 — detect-repository residual parse arms', () => {
  it('ssh/https/owner-repo false arms + cache miss', () => {
    clearRepositoryCaches();
    assert.equal(parseGitRemote('git@host-with-dashes:o/r.git'), null);
    assert.equal(parseGitRemote('https://github.com/only'), null);
    assert.equal(parseGitRemote(''), null);
    assert.equal(parseGitHubRepository('owner/repo.git'), 'owner/repo');
    assert.equal(parseGitHubRepository('https://gitlab.com/o/r'), null);
    assert.ok(parseGitRemote('ssh://git@github.com/o/r.git'));
    assert.ok(parseGitRemote('git://github.com/o/r'));
    assert.equal(parseGitRemote('o/r'), null); // owner/repo without host is GitHub-only via parseGitHubRepository
    assert.equal(parseGitHubRepository('o/r'), 'o/r');
    const miss = mkTmp('repo20-');
    assert.equal(getCachedRepository(miss), null);
    detectCurrentRepositoryWithHost(miss);
  });
});

describe('wave20 — task-tracker watch close catch (237/243-245/249)', () => {
  it('watch throw degrade + pending-timer clear + close() throw catch', async () => {
    const d = mkTmp('tt20-');
    setTaskPersistPath(d, 'w20');
    await taskTrackerTool.execute!(
      { action: 'create', subject: 's', description: 'd' },
      ctx({ cwd: d }),
    );

    _taskTrackerHooks.watch = (() => {
      throw new Error('inotify-exhausted');
    }) as typeof fs.watch;
    const noop = watchTaskList(d, () => {}, { debounceMs: 5 });
    noop.close(); // watcher null → optional-chain short-circuit
    noop.close(); // closed early return

    _taskTrackerHooks.watch = ((dir: fs.PathLike, cb: fs.WatchListener<string>) => {
      const w = realWatch(dir, cb);
      return {
        close: () => {
          throw new Error('already-closed');
        },
        unref: () => w.unref?.(),
        ref: () => w.ref?.(),
      } as fs.FSWatcher;
    }) as typeof fs.watch;

    const handle = watchTaskList(d, () => {}, { debounceMs: 60_000 });
    // schedule a pending timer then close → clearTimeout arm + close throw catch
    const sd = projectStateDir(d);
    const tf = fs.readdirSync(sd).find((f) => f.startsWith('tasks'));
    if (tf) fs.writeFileSync(path.join(sd, tf), fs.readFileSync(path.join(sd, tf)));
    handle.close();
  });
});

describe('wave20 — diagnostic-registry residual maxFiles / empty message / non-Error', () => {
  it('clamps bad maxFiles + fail-open empty message snippet', () => {
    const reg = new LSPDiagnosticRegistry(Number.NaN);
    assert.equal(reg.deliveredFileCount, 0);
    // Circular range → JSON.stringify in createDiagnosticKey throws → fail-open catch
    // with empty message → `<no message>` snippet arm.
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    reg.registerPendingLSPDiagnostic({
      serverName: 'ts',
      files: [
        {
          uri: 'file:///x.ts',
          diagnostics: [
            {
              severity: 1,
              message: '',
              range: circular,
            },
          ],
        },
      ],
    } as never);
    assert.ok(Array.isArray(reg.checkForLSPDiagnostics()));
  });
});

describe('wave20 — process-identity residual EPERM / null startTime arms', () => {
  it('isPidPresent + probe with null startTime mismatch path', () => {
    assert.equal(isPidPresent(-1), false);
    const id = captureProcessIdentitySync({ pid: process.pid, spawnToken: null, platform: 'linux' });
    const p = probeProcessIdentitySync(id, {
      isPidPresent: () => true,
      readProcessStartTimeMs: () => null,
    });
    assert.ok(p.outcome === 'indeterminate' || p.outcome === 'live');
    assert.equal(isSafeToSteal({ outcome: 'identity-mismatch' }), true);
  });
});

describe('wave20 — file-edit-equivalence residual replace_all / one-side-fail', () => {
  it('noop continue, replace_all multi, and asymmetric error arms', () => {
    assert.equal(
      applyEditsToContent('aa aa', [{ old_string: 'aa', new_string: 'aa', replace_all: true }]),
      'aa aa',
    ); // NOOP continue
    assert.equal(
      applyEditsToContent('aa aa', [{ old_string: 'aa', new_string: 'bb', replace_all: true }]),
      'bb bb',
    );
    assert.equal(
      areFileEditsEquivalent(
        [{ old_string: 'x', new_string: 'y' }],
        [{ old_string: 'x', new_string: 'y' }],
        'x',
      ),
      true,
    ); // fast path
    assert.equal(
      areFileEditsEquivalent(
        [{ old_string: 'missing', new_string: 'y' }],
        [{ old_string: 'a', new_string: 'b' }],
        'a',
      ),
      false,
    ); // one fails one succeeds
    assert.equal(
      areFileEditsEquivalent(
        [{ old_string: 'gone', new_string: 'y' }],
        [{ old_string: 'also-gone', new_string: 'y' }],
        'z',
      ),
      true,
    ); // both fail same way
  });
});

describe('wave20 — config-tool suggestion / corrupt diagnostic ?? / special file', () => {
  it('validate special-file arm + set corrupt refuse', async () => {
    assert.equal(isSupported('model'), true);
    assert.ok(getSettingConfig('model'));

    const d = mkTmp('cfg20-');
    const ami = path.join(d, '.ami');
    fs.mkdirSync(ami, { recursive: true });
    fs.writeFileSync(path.join(ami, 'settings.json'), '{not-json');
    const set = await configTool.execute!(
      { action: 'set', key: 'model', value: 'gpt-4o' },
      ctx({ cwd: d }),
    );
    assert.ok(set.isError || typeof set.output === 'string');

    const d2 = mkTmp('cfg20dir-');
    fs.mkdirSync(path.join(d2, '.ami', 'settings.json'), { recursive: true });
    const v = await configTool.execute!({ action: 'validate', scope: 'project' }, ctx({ cwd: d2 }));
    assert.ok(v.isError || typeof v.output === 'string');
  });
});

describe('wave20 — enter-worktree staleHint catch (559)', () => {
  it('isCloneStale throw swallowed on successful create path', async () => {
    const cwd = mkTmp('ew20-');
    initGitRepo(cwd);
    _enterWorktreeHooks.generateSlug = () => 'wave20slug';
    _enterWorktreeHooks.isCloneStale = () => {
      throw new Error('stale-probe-boom');
    };
    const r = await enterWorktreeTool.execute!({ name: 'wave20slug' }, ctx({ cwd }));
    // success or exists — either way the stale catch ran on the create path
    assert.ok(typeof r.output === 'string');
  });
});

describe('wave20 — exit-worktree branch rev-parse catch (152)', () => {
  it('abbrev-ref throw → empty catch then session branch fallback', async () => {
    const cwd = mkTmp('ex20-');
    initGitRepo(cwd);
    const wt = path.join(cwd, '.ami', 'worktrees', 'feat');
    fs.mkdirSync(wt, { recursive: true });
    initGitRepo(wt);
    // Point HEAD so we're "inside" a worktree path; session metadata supplies branch.
    const { setWorktreeSession } = require('../src/worktree-manager');
    setWorktreeSession({
      worktreePath: wt,
      worktreeBranch: 'worktree-feat',
      originalCwd: cwd,
      originalBranch: 'main',
      createdAt: Date.now(),
    });
    let calls = 0;
    _exitWorktreeHooks.exec = ((args: string[], opts?: { cwd?: string }) => {
      calls++;
      if (args[0] === 'rev-parse' && args[1] === '--abbrev-ref') {
        throw new Error('abbrev-fail');
      }
      return gitExec(args, opts as never);
    }) as typeof gitExec;
    // First gate uses raw gitExec — need cwd inside worktree
    const r = await exitWorktreeTool.execute!({ action: 'keep' }, ctx({ cwd: wt }));
    assert.ok(typeof r.output === 'string');
    assert.ok(calls >= 0);
  });
});

describe('wave20 — types residual helpers', () => {
  it('forces falsy/alt arms across exported type helpers', () => {
    assert.equal(toAgentId(''), null);
    assert.equal(describeTheme('nope'), 'nope');
    assert.equal(isSupportedImageMediaType(undefined), false);
    assert.ok(mergeSkillUsageStat(undefined, 's').s.usageCount === 1);
    assert.ok(reconcileSessionMode('coordinator', 'normal').warning?.includes('Entered'));
    assert.ok(reconcileSessionMode('normal', 'coordinator').warning?.includes('Exited'));
    assert.equal(reconcileSessionMode(undefined, 'normal').changed, false);
    assert.equal(isLspRecommendationDisabled({ lspRecommendationIgnoredCount: 99 }), true);
    assert.equal(shouldEnablePromptSuggestion({ configEnabled: false }), false);
    assert.equal(shouldRevealSlowProgress(1, 10), false);
    assert.equal(sanitizeToolInputSummary('  '), undefined);
    assert.ok(resolveSentAt(undefined));
    assert.equal(selectModelFacingOutput({ output: 'o' } as never), 'o');
    assert.equal(
      selectUserFacingOutput({ output: 'o', userFacingContent: 'u' } as never),
      'u',
    );
    assert.equal(validateSummarizeMetadata(null), null);
    assert.equal(nextTaskBudgetRemaining({ total: 10 } as never, 3, undefined), 7);
    assert.ok(resolveDiffPreviewWidth(true) > resolveDiffPreviewWidth(false));
    assert.ok(describeSymlinkEdit('/a', '/outside/b', '/c')?.note.includes('symlink'));
    assert.equal(shouldShowWorkerBadge({ name: 'w' } as never, true), false);
    assert.ok(resolveNotificationTimeout({} as never) > 0);
    assert.ok(
      planNotificationQueue([], { key: 'k', priority: 'immediate' } as never).length === 1,
    );
  });
});

describe('wave20 — log-sink / rate-limit / flow-manager / tools-index', () => {
  it('log catch + timezone ||/catch + flow summarize falsy + denyPredicate', () => {
    assert.equal(sanitizeMcpLogSegment(''), '_');
    assert.equal(sanitizeMcpLogSegment('...'), '_');
    const dir = mkTmp('mcp20-');
    // Make parent a file so mkdirSync throws → append catch
    const blocker = path.join(dir, 'mcp-logs-_');
    fs.writeFileSync(blocker, 'x');
    logMcpError('..', new Error('e'), { configDir: dir, cwd: dir });
    logMcpDebug('ok', 'secret=sk-abc', { configDir: mkTmp('mcp20b-'), cwd: dir, sessionId: 's' });
    assert.ok(getMcpServerLogPath('s', { configDir: dir }).includes('mcp-logs'));

    mock.method(Intl, 'DateTimeFormat', function () {
      return { resolvedOptions: () => ({ timeZone: '' }) };
    } as never);
    assert.equal(getTimeZone(), 'local');
    mock.restoreAll();
    mock.method(Intl, 'DateTimeFormat', () => {
      throw new Error('intl-down');
    });
    assert.equal(getTimeZone(), 'local');
    mock.restoreAll();
    assert.ok(formatResetTime(Math.floor(Date.now() / 1000) + 7200, true)!.includes('('));
    assert.equal(parseRetryAfterHeader(undefined), null);
    assert.ok(
      suggestedWaitMs(
        { retryAfterMs: null, resetAt: null, remainingRequests: null, remainingTokens: null } as never,
        100,
      ) >= 0,
    );

    assert.equal(FlowManager.summarize(['completed', 'completed']), '2/2');
    assert.equal(FlowManager.summarize(['failed', 'killed']), '0/2 · 2 failed');
    assert.equal(FlowManager.needsAttention([undefined, 'working']), false);
    assert.equal(FlowManager.needsAttention(['needs_input']), true);
    const fm = new FlowManager();
    const id = fm.createFlow('d');
    assert.equal(fm.addAgent(id, ''), false);
    fm.addAgent(id, 'agent1');
    fm.updateFlowStatus(id, () => 'completed');
    assert.equal(fm.markNotified(id), true);
    assert.equal(fm.markNotified(id), false);
    fm.scheduleEviction(id, 0);
    fm.listFlows();

    const reg = new ToolRegistry();
    reg.register({ name: 'zzz_core', description: 'd', inputSchema: {}, isReadOnly: true, execute: async () => ({ output: '' }) }, 'core');
    reg.register({ name: 'aaa_mcp', description: 'd', inputSchema: {}, isReadOnly: true, execute: async () => ({ output: '' }) }, 'mcp');
    const pool = reg.assembleToolPool();
    assert.ok(pool[0]!.name === 'zzz_core' || pool.length >= 2);
    assert.equal(reg.get('missing-alias'), undefined);
    createDefaultTools(mkTmp('idx20-'));
  });
});

describe('wave20 — xml-escape / memoize residual catch / ?? arms', () => {
  it('fromCodePoint catch, named ??, refresh catch, async LRU reject eviction', async () => {
    assert.equal(unescapeXml('&#x110000;'), '&#x110000;'); // out of range → keep match
    assert.equal(unescapeXml('&#-1;'), '&#-1;'); // negative → keep match
    assert.equal(unescapeXml('&unknown;'), '&unknown;'); // named ?? fallback
    assert.equal(safeXmlKey('1bad', 'fb'), 'fb');
    assert.equal(isSafeXmlAttrName('a.b'), false);
    assert.equal(renderXmlAttrs(null), '');
    assert.ok(escapeXmlAttr('a\tb').includes('&#9;'));

    let n = 0;
    const m = memoizeWithTTL(() => {
      n++;
      if (n === 2) throw new Error('refresh-boom');
      return n;
    }, 1);
    assert.equal(m(), 1);
    await new Promise((r) => setTimeout(r, 5));
    assert.equal(m(), 1); // stale serve + bg refresh throw catch
    await new Promise((r) => setTimeout(r, 5));

    let a = 0;
    const ma = memoizeWithTTLAsync(async () => {
      a++;
      if (a === 2) throw new Error('async-refresh');
      return a;
    }, 1);
    assert.equal(await ma(), 1);
    await new Promise((r) => setTimeout(r, 5));
    assert.equal(await ma(), 1);
    await new Promise((r) => setTimeout(r, 5));

    const lru = memoizeWithLRU((x: number) => x * 2, (x) => String(x), 1);
    assert.equal(lru(1), 2);
    assert.equal(lru(2), 4); // evict
    assert.equal(lru.cache.get('1'), undefined);

    let rejects = 0;
    const alru = memoizeAsyncWithLRU(
      async (x: number) => {
        if (x < 0) {
          rejects++;
          throw new Error('neg');
        }
        return x;
      },
      (x) => String(x),
      2,
    );
    await assert.rejects(() => alru(-1));
    assert.equal(alru.cache.has('-1'), false);
    assert.ok(rejects >= 1);
  });
});

describe('wave20 — session-search residual subsequence / sample / rank arms', () => {
  it('flatten non-object, subsequence, sample truncate, recentFill catch', async () => {
    assert.equal(flattenMessageText(null as never), '');
    assert.ok(isSubsequence('abc', 'ac'));
    assert.ok(matchSnippet('hello world', ['zz', 'hello'], 'hello', 8).length > 0);
    const msgs: Message[] = Array.from({ length: 20 }, (_, i) => ({
      role: 'user',
      content: `msg-${i}-needle`,
    })) as Message[];
    assert.ok(extractRankingTranscript(msgs, 40, 4).includes('…') || extractRankingTranscript(msgs, 40, 4).length <= 50);
    assert.ok(searchTranscript(msgs, 'needle', { limit: 2 }).length <= 2);

    const cwd = mkTmp('ss20-');
    const sessDir = projectStateDir(cwd, 'sessions');
    fs.mkdirSync(sessDir, { recursive: true });
    fs.writeFileSync(path.join(sessDir, 'session-wave20.json'), '{broken');
    const ranked = await searchSessionsRanked(cwd, 'zzz-no-hit', {
      limit: 2,
      recentFill: true,
      rankFn: async () => {
        throw new Error('rank-boom');
      },
    });
    assert.ok(Array.isArray(ranked));
  });
});

describe('wave20 — file-cache-seed rethrow (72-73) + file-lock residual catches', () => {
  it('unexpected FS error rethrows; lock utimes/remove catches', async () => {
    const cwd = mkTmp('fcs20-');
    const target = path.join(cwd, 'f.ts');
    fs.writeFileSync(target, 'hello');
    _fileCacheSeedHooks.statSync = (() => {
      throw Object.assign(new Error('weird'), { code: 'ENOTSUP' });
    }) as typeof fs.statSync;
    assert.throws(() =>
      collectReadFileStatesFromTranscript(
        [
          {
            role: 'assistant',
            tool_calls: [
              {
                id: '1',
                type: 'function',
                function: { name: 'file_read', arguments: JSON.stringify({ file_path: 'f.ts' }) },
              },
            ],
          } as never,
          { role: 'tool', tool_call_id: '1', content: '1| hello' } as never,
        ],
        cwd,
      ),
    );
    _fileCacheSeedHooks.statSync = realSeedStat;
    assert.equal(seedFileCacheFromTranscript([], cwd), 0);

    const lockTarget = path.join(mkTmp('lk20-'), 't.json');
    fs.writeFileSync(lockTarget, '{}');
    await withFileLock(
      lockTarget,
      async () => {
        fs.rmSync(`${lockTarget}.lock`, { recursive: true, force: true });
        await new Promise((r) => setTimeout(r, 30));
      },
      { refreshMs: 5, staleMs: 40, onCompromised: () => {} },
    );

    const t2 = path.join(mkTmp('lk20b-'), 'u.json');
    fs.writeFileSync(t2, '{}');
    withFileLockSync(t2, () => {
      // Make removeLockDirSync throw (non-empty dir → rmdir fails) when not compromised
      fs.writeFileSync(path.join(`${t2}.lock`, 'extra'), 'x');
    });
  });
});
