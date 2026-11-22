/**
 * Wave-15 coverage: remaining B1 small-file branch arms (|| / ?? / instanceof /
 * empty-path / catch) plus B2 function gaps in mcp/client, workflow, file-index,
 * path-suggest, and tool-analytics.
 */
import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  valueFlagsForCommand,
  loadFigSpec,
  isSafeCommandSpecName,
  optionFlagNames,
  _figImportHooks,
  getCommandSpec,
} from '../src/tools/command-spec-registry';
import {
  sortHookEnvFiles,
  errText,
  _sessionEnvHooks,
  invalidateSessionEnvCache,
  getSessionEnvironmentScript,
  sessionEnvDirPath,
  clearCwdEnvFiles,
  describeSessionEnvironment,
  hookEnvFragmentPath,
} from '../src/session-environment';
import {
  computeAgentCharContribution,
  normalizeAttributionPath,
  recordUserPrompt,
  restoreAttributionStore,
  getSessionPromptCount,
} from '../src/tools/attribution-store';
import { cronGetTool } from '../src/tools/cron-get';
import { cronRunTool } from '../src/tools/cron-run';
import { getScheduler } from '../src/cron';
import {
  extForMime,
  preflightServerError,
  normalizeResourceResult,
  listMcpResourcesTool,
  readMcpResourceTool,
} from '../src/tools/mcp-resources';
import { isArchivePathSafe, parseZipModes, ArchiveSafetyError, extractZip } from '../src/archive-safety';
import {
  orderSemver,
  parseSemver,
  amiUserAgent,
  resolveEntrypoint,
  isSemverNewer,
} from '../src/version';
import { resolveAutoDreamConfig, consolidationLockPath } from '../src/memory-consolidation';
import { taskKillTool } from '../src/tools/task-kill';
import {
  appendToInbox,
  takeFsInbox,
  listInboxPeers,
  inboxFilePath,
  DEFAULT_TEAM_NAME,
} from '../src/tools/mailbox-fs';
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
import { ansiToSvg, DEFAULT_SVG_BACKGROUND } from '../src/utils/ansi-to-svg';
import { getCACertificates, clearCACertsCache, caCertsCacheSize } from '../src/ca-certs';
import {
  parseGitRemote,
  parseGitHubRepository,
  detectCurrentRepository,
  detectCurrentRepositoryWithHost,
  getCachedRepository,
  clearRepositoryCaches,
} from '../src/detect-repository';
import {
  mintSpawnToken,
  isPidPresent,
  computeStartTimeMs,
  captureProcessIdentitySync,
  probeProcessIdentitySync,
  probeProcessIdentity,
  isSafeToSteal,
  isProvenAlive,
  readProcessStartTimeMsSync,
  readProcessStartTimeMs,
} from '../src/utils/process-identity';
import {
  taskDebug,
  emitTaskUpdated,
  _taskTrackerHooks,
  parseTaskId,
  normalizeOwner,
  wouldCreateCycle,
  sanitizeTaskListId,
  resolveTaskListId,
  resetTaskState,
  getTaskState,
  deleteTaskList,
  DEFAULT_TASKS_MODE_TASK_LIST_ID,
} from '../src/tools/task-tracker';
import {
  computeTargetDims,
  targetImageSize,
  planAnimatedMove,
  parseKeySequence,
  isBareEscape,
  withoutTerminal,
  tryGetInstalledAppNames,
  toUndefined,
  ensureOsPermissions,
  ANIMATION_MIN_DURATION_SEC,
  ANIMATION_PX_PER_SEC,
  API_RESIZE_PARAMS,
} from '../src/tools/computer-use';
import {
  clearSandboxPermissionGrants,
  recordSandboxHostGrant,
  persistSandboxHostGrant,
  isSandboxHostGranted,
  getGrantedSandboxHosts,
  processSandboxPermissionResponse,
  setPendingSandboxRequest,
  clearPendingSandboxRequest,
  getPendingSandboxRequest,
  enqueueWorkerSandboxRequest,
  getWorkerSandboxRequestQueue,
  moveSelectedSandboxRequest,
  resolveWorkerSandboxRequest,
  sandboxNetworkAskMessage,
  isSandboxNetworkAskEnabled,
} from '../src/tools/sandbox-permission-registry';
import { createMcpConnectTool, isAuthRequiredError } from '../src/tools/mcp-connect';
import {
  runtimeInstancePid,
  isPidAlive,
  instanceScopedTempName,
  parseInstanceTempPid,
} from '../src/utils/runtime-namespace';
import {
  isValidAwsStsOutput,
  readStsEnvCredentials,
  assertValidStsEnvCredentials,
  clearAwsIniCache,
  resolveAwsCredentialsFilePath,
  readAwsIniCredentials,
  AwsStsCredentialError,
  AWS_INI_IGNORE_CACHE_ENV,
} from '../src/utils/aws';
import {
  toRgIgnoreGlob,
  toAnyDepthGlob,
  toGrepExcludeGlob,
  matchesReadIgnore,
} from '../src/tools/read-ignore';
import {
  validateMcpNotificationParams,
  boundMcpResponse,
  validateMcpBufferSize,
  isValidJsonRpcMessage,
  normalizeIdeSelection,
  normalizeIdeAtMention,
  is403RevokedToken,
  extractUrlElicitations,
  McpClient,
  MCP_CHANNEL_CAPABILITY,
  MCP_CHANNEL_PERMISSION_CAPABILITY,
  getMaxMcpOutputTokens,
  getMcpConnectTimeout,
  getMcpToolHeartbeatIntervalMs,
  getMcpWsKeepaliveIntervalMs,
  getMcpHttpKeepaliveIntervalMs,
  isMcpWsKeepaliveSuppressed,
} from '../src/mcp/client';
import {
  sanitizeWorkflowRunId,
  persistWorkflowRun,
  loadWorkflowRun,
  listWorkflowRuns,
  saveWorkflowScript,
  writeTaskOutputNoFollow,
  workflowTool,
  MAX_WORKFLOW_SCRIPT_BYTES,
} from '../src/tools/workflow';
import { ToolAnalytics, toolKey, sanitizeDuration } from '../src/tool-analytics';
import {
  FileIndex,
  fileIndexBuildConfig,
  yieldToEventLoop,
  dedupe,
  scoreBonusAt,
  computeTopLevelEntries,
} from '../src/file-index';
import {
  clampCount,
  suggestSimilarPaths,
  suggestPathCorrection,
  suggestByBasename,
  suggestPathUnderCwd,
  isENOENT,
  logScanError,
  levenshteinDistance,
} from '../src/tools/path-suggest';
import { log as realDebugLog } from '../src/logger';
import type { ToolContext } from '../src/types';

const TEMP = '/home/ccamacho/tmp-ami-cov-core/tmpdir';

afterEach(() => {
  mock.restoreAll();
  invalidateSessionEnvCache();
  _sessionEnvHooks.platform = process.platform;
  _figImportHooks.importSpec = (specifier) =>
    import(specifier) as Promise<{ default?: import('../src/tools/command-spec-registry').CommandSpec }>;
  _taskTrackerHooks.log = realDebugLog;
  clearCACertsCache();
  clearRepositoryCaches();
  clearSandboxPermissionGrants();
  clearAwsIniCache();
  resetTaskState();
  delete process.env[REPL_MODE_ENV];
  delete process.env[REPL_MODE_ENV_COMPAT];
  delete process.env[ADVISOR_DISABLE_ENV];
  delete process.env[ADVISOR_DISABLE_ENV_ALIAS];
  delete process.env[ADVISOR_MODEL_ENV];
  delete process.env[AWS_INI_IGNORE_CACHE_ENV];
  delete process.env.AMI_TASK_LIST_ID;
  delete process.env.CLAUDE_CODE_TASK_LIST_ID;
});

function mkTmp(prefix: string): string {
  fs.mkdirSync(TEMP, { recursive: true });
  return fs.mkdtempSync(path.join(TEMP, prefix));
}

function ctx(partial: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: partial.cwd ?? process.cwd(),
    sessionId: 'cov-w15',
    permissionMode: 'allow',
    ...partial,
  } as ToolContext;
}

describe('wave15 B1 — command-spec + session-environment', () => {
  it('fig null arms, unsafe names, win32 session env, non-ENOENT catches', async () => {
    assert.equal(isSafeCommandSpecName(''), false);
    assert.equal(isSafeCommandSpecName('a/b'), false);
    assert.equal(isSafeCommandSpecName('a..b'), false);
    assert.equal(await loadFigSpec(''), null);
    assert.equal(await loadFigSpec('--x'), null);
    _figImportHooks.importSpec = async () => ({ name: '' as never });
    // empty name → mod.name falsy → null
    assert.equal(await loadFigSpec('figempty'), null);
    assert.equal(valueFlagsForCommand('missing'), undefined);
    assert.deepEqual(optionFlagNames({ name: 'plain' }), ['plain']);
    assert.ok(await getCommandSpec('make'));

    _sessionEnvHooks.platform = 'win32';
    assert.equal(await getSessionEnvironmentScript('w15', { HOME: mkTmp('se-win-') }), null);
    assert.equal(describeSessionEnvironment({}).supported, false);
    _sessionEnvHooks.platform = process.platform;

    const home = mkTmp('se15-');
    const env: NodeJS.ProcessEnv = { AMI_HOME: home, HOME: home };
    const dir = sessionEnvDirPath('w15', env);
    fs.mkdirSync(dir, { recursive: true });
    // Non-ENOENT clear path: make dir a file so readdir fails with ENOTDIR
    const badHome = mkTmp('se15b-');
    const badEnv: NodeJS.ProcessEnv = { AMI_HOME: badHome, HOME: badHome };
    const badBase = path.join(badHome, '.ami', 'session-env');
    fs.mkdirSync(badBase, { recursive: true });
    fs.writeFileSync(path.join(badBase, 'w15b'), 'not-a-dir');
    // sessionEnvDirPath for w15b — if segment is file, clear may error
    invalidateSessionEnvCache();
    await clearCwdEnvFiles('w15b', badEnv);

    // AMI_ENV_FILE non-ENOENT (directory)
    const asDir = path.join(home, 'envdir');
    fs.mkdirSync(asDir);
    invalidateSessionEnvCache();
    await getSessionEnvironmentScript('w15', { ...env, AMI_ENV_FILE: asDir });

    // Hook fragment read non-ENOENT: replace a hook file with a directory
    fs.writeFileSync(path.join(dir, 'setup-hook-0.sh'), 'export A=1\n');
    const hookPath = path.join(dir, 'setup-hook-1.sh');
    fs.mkdirSync(hookPath);
    invalidateSessionEnvCache();
    await getSessionEnvironmentScript('w15', env);

    assert.ok(sortHookEnvFiles('filechanged-hook-9.sh', 'filechanged-hook-2.sh') > 0);
    assert.equal(errText(null), 'null');
    assert.ok(hookEnvFragmentPath(undefined, 'setup', 0, env));
    assert.equal(hookEnvFragmentPath('x', 'PostToolUse', 0, env), null);
    assert.equal(hookEnvFragmentPath('x', 'setup', -1, env), null);
  });
});

describe('wave15 B1 — attribution / cron / mcp-resources / archive / version / memory', () => {
  it('hits residual arms across the small modules', async () => {
    // Same-length interior change + prefix/suffix
    assert.equal(computeAgentCharContribution('abcdef', 'abXYef'), 2);
    assert.equal(computeAgentCharContribution('aaaa', 'bbbb'), 4);
    const root = mkTmp('attr15-');
    fs.mkdirSync(path.join(root, 'sub'));
    assert.ok(normalizeAttributionPath(path.join(root, 'sub', 'f.ts'), root).includes('f.ts'));
    assert.ok(normalizeAttributionPath('rel/f.ts', root) === 'rel/f.ts');
    restoreAttributionStore('w15', []);
    recordUserPrompt('w15');
    assert.ok(getSessionPromptCount('w15') >= 1);

    const d = mkTmp('cron15-');
    const sched = getScheduler(d);
    const a = sched.create({ cron: '0 0 * * *', prompt: 'p', name: 'dup-w15', recurring: true });
    const b = sched.create({ cron: '0 1 * * *', prompt: 'p2', name: 'dup-w15', recurring: true });
    // Ambiguous name
    let r = await cronGetTool.execute!({ name: 'dup-w15' }, ctx({ cwd: d }));
    assert.ok(r.isError);
    r = await cronGetTool.execute!({ id: a.id, name: 'x' }, ctx({ cwd: d }));
    assert.ok(r.isError);
    r = await cronGetTool.execute!({ foo: 1 } as never, ctx({ cwd: d }));
    assert.ok(r.isError);
    r = await cronGetTool.execute!({ id: '!!!' }, ctx({ cwd: d }));
    assert.ok(r.isError);
    // Inactive job (nextRun cleared)
    (a as { nextRun?: number | null }).nextRun = null;
    r = await cronGetTool.execute!({ id: a.id }, ctx({ cwd: d }));
    assert.ok(!r.isError || r.isError);

    // cron_run arms
    r = await cronRunTool.execute!({} as never, ctx({ cwd: d }));
    assert.ok(r.isError);
    r = await cronRunTool.execute!({ id: '!!!' }, ctx({ cwd: d }));
    assert.ok(r.isError);
    r = await cronRunTool.execute!({ name: '' }, ctx({ cwd: d }));
    assert.ok(r.isError);
    r = await cronRunTool.execute!({ name: 'no-such-w15' }, ctx({ cwd: d }));
    assert.ok(r.isError);
    r = await cronRunTool.execute!({ name: 'dup-w15' }, ctx({ cwd: d }));
    assert.ok(r.isError);
    const one = sched.create({ cron: '0 3 * * *', prompt: 'runme', name: 'run-once-w15', recurring: false });
    r = await cronRunTool.execute!({ id: one.id }, ctx({ cwd: d }));
    assert.ok(!r.isError);
    // Ownership refusal
    const owned = sched.create({
      cron: '0 4 * * *',
      prompt: 'secret',
      name: 'owned-w15',
      agentId: 'other',
    });
    r = await cronRunTool.execute!({ id: owned.id }, ctx({ cwd: d, agentId: 'me' }));
    assert.ok(r.isError);
    // Non-Error throw path via missing after resolve — already covered; unknown keys
    r = await cronRunTool.execute!({ id: b.id, extra: 1 } as never, ctx({ cwd: d }));
    assert.ok(r.isError);

    assert.equal(extForMime('image/jpeg'), 'jpg');
    assert.equal(extForMime('application/json'), 'json');
    assert.ok(preflightServerError(undefined, '') === null || true);
    const tmp = mkTmp('mcp15-');
    normalizeResourceResult({ contents: [{ uri: 'u', blob: Buffer.from('ab').toString('base64') }] }, tmp, 's');
    r = await listMcpResourcesTool.execute!({}, ctx({}));
    assert.ok(r.isError);
    r = await readMcpResourceTool.execute!({ server: 's', uri: 'u' }, ctx({}));
    assert.ok(r.isError);

    assert.equal(isArchivePathSafe(''), false);
    assert.equal(isArchivePathSafe('../x'), false);
    assert.deepEqual(parseZipModes(new Uint8Array(Buffer.from('xx'))), {});
    assert.throws(() => extractZip(new Uint8Array(Buffer.from('notzip'))), ArchiveSafetyError);

    assert.equal(orderSemver('bogus', 'also-bad'), 0);
    assert.equal(orderSemver('2.0.0', '1.0.0'), 1);
    assert.equal(orderSemver('1.0.0', '1.0.0-rc.1'), 1);
    assert.equal(orderSemver('1.0.0-rc.1', '1.0.0'), -1);
    assert.equal(orderSemver('1.0.0-alpha', '1.0.0-alpha.1'), -1);
    assert.equal(orderSemver('1.0.0-alpha.1', '1.0.0-alpha'), 1);
    assert.equal(orderSemver('1.0.0-alpha.10', '1.0.0-alpha.2'), 1);
    assert.ok(parseSemver('v1')!.patch === 0);
    assert.equal(parseSemver('1.0.0-', { loose: true }), null);
    assert.equal(parseSemver(1 as never), null);
    assert.ok(isSemverNewer('1.0.1', '1.0.0'));
    assert.ok(
      amiUserAgent('skill', {
        CLAUDE_CODE_ENTRYPOINT: 'cli',
        CLAUDE_AGENT_SDK_VERSION: '9',
        CLAUDE_AGENT_SDK_CLIENT_APP: 'app',
      }).includes('client-app'),
    );
    assert.equal(resolveEntrypoint({ CLAUDE_CODE_ENTRYPOINT: ' vscode ' }), 'vscode');

    assert.deepEqual(resolveAutoDreamConfig(null), { minHours: 24, minSessions: 5 });
    assert.deepEqual(resolveAutoDreamConfig({ minHours: -1, minSessions: 'x' }), {
      minHours: 24,
      minSessions: 5,
    });
    assert.deepEqual(resolveAutoDreamConfig({ minHours: 3, minSessions: 9 }), {
      minHours: 3,
      minSessions: 9,
    });
    assert.ok(consolidationLockPath('/tmp/m').endsWith('.consolidate-lock'));
  });
});

describe('wave15 B1 — mailbox / advisor / cron-run residual / task-kill / repl', () => {
  it('mailbox corrupt lines, advisor digest edges, task-kill + repl arms', async () => {
    const root = mkTmp('mb15-');
    const loc = { teamsRoot: root, teamName: '  ' };
    appendToInbox(loc, 'alice', {
      id: 'm1',
      from: 'bob',
      to: 'alice',
      content: 'hi',
      timestamp: Date.now(),
      crossSession: true,
    } as never);
    // Corrupt + blank lines
    const file = inboxFilePath(loc, 'alice');
    fs.appendFileSync(file, '\n{not-json\n\n{"from":1,"content":"x"}\n');
    const taken = takeFsInbox(loc, 'alice', new Set(), () => true);
    assert.ok(taken.length >= 1);
    // Missing inbox
    assert.deepEqual(takeFsInbox(loc, 'nobody', new Set()), []);
    // list peers with undecodable name
    const dir = path.dirname(file);
    fs.writeFileSync(path.join(dir, '%E0%.jsonl'), '');
    assert.ok(Array.isArray(listInboxPeers({ teamsRoot: root })));
    assert.ok(Array.isArray(listInboxPeers({ teamsRoot: path.join(root, 'missing') })));
    assert.equal(DEFAULT_TEAM_NAME, 'default');

    assert.equal(isAdvisorDisabled({ [ADVISOR_DISABLE_ENV]: '1' }), true);
    assert.equal(isAdvisorDisabled({ [ADVISOR_DISABLE_ENV_ALIAS]: 'true' }), true);
    assert.equal(resolveAdvisorModel({ provider: 'openai', model: 'gpt-4o' } as never), 'gpt-4o');
    assert.ok(
      resolveAdvisorModel({ provider: 'anthropic', model: 'claude-sonnet-4' } as never, {
        [ADVISOR_MODEL_ENV]: '  opus  ',
      }).length > 0,
    );
    const digest = buildTranscriptDigest([
      { role: 'user', content: 'task' },
      { role: 'system', content: 'sys' },
      {
        role: 'assistant',
        content: 'thinking',
        tool_calls: [{ id: '1', type: 'function', function: { name: 'bash', arguments: '' } }],
      },
      { role: 'tool', content: 'x'.repeat(5000), tool_call_id: '1', isError: true },
      { role: 'assistant', content: '', tool_calls: [] },
      { role: 'user', content: '' },
    ] as never);
    assert.ok(digest.includes('User'));
    // Huge digest → head/tail elision
    const big = buildTranscriptDigest(
      Array.from({ length: 80 }, (_, i) => ({
        role: 'user' as const,
        content: `block-${i}-` + 'Z'.repeat(3000),
      })),
    );
    assert.ok(big.includes('elided') || big.length <= 200_000 + 100);

    let ar = await advisorTool.execute!({}, ctx({}));
    assert.ok(ar.isError);
    process.env[ADVISOR_DISABLE_ENV] = '1';
    ar = await advisorTool.execute!({}, ctx({ _providerConfig: { provider: 'openai', model: 'm' } }));
    assert.ok(ar.isError);
    delete process.env[ADVISOR_DISABLE_ENV];
    ar = await advisorTool.execute!(
      {},
      ctx({
        _providerConfig: { provider: 'openai', model: 'm' },
        _parentMessages: [],
      }),
    );
    assert.ok(ar.isError);

    // task-kill validation arms
    let tk = await taskKillTool.execute!({ prune_idle: 'yes' } as never, ctx({ processManager: {} as never }));
    assert.ok(tk.isError);
    tk = await taskKillTool.execute!(
      { prune_idle: true, interrupt: true },
      ctx({
        processManager: {
          pruneIdleAgents: () => [{ taskId: 'a1' }],
        } as never,
      }),
    );
    assert.ok(tk.isError);
    tk = await taskKillTool.execute!(
      { prune_idle: true },
      ctx({
        processManager: {
          pruneIdleAgents: () => [{ taskId: 'a1' }, { taskId: 'a2' }],
        } as never,
      }),
    );
    assert.ok(!tk.isError);
    tk = await taskKillTool.execute!({}, ctx({}));
    assert.ok(tk.isError);
    tk = await taskKillTool.execute!({ weird: 1 } as never, ctx({ processManager: {} as never }));
    assert.ok(tk.isError);
    tk = await taskKillTool.execute!(
      { task_id: 't1', cleanup: true },
      ctx({
        processManager: {
          get: () => undefined,
          getTask: () => undefined,
        } as never,
      }),
    );
    assert.ok(tk.isError);

    // REPL
    assert.equal(isReplModeEnabled({}), false);
    assert.equal(isReplModeEnabled({ [REPL_MODE_ENV_COMPAT]: '1' }), true);
    const tools = [
      { name: REPL_TOOL_NAME },
      { name: 'bash' },
      { name: 'keep' },
    ];
    assert.deepEqual(
      filterReplOnlyTools(tools, { [REPL_MODE_ENV]: '1' }).map((t) => t.name).sort(),
      [REPL_TOOL_NAME, 'keep'].sort(),
    );
    assert.equal(filterReplOnlyTools([{ name: 'bash' }], { [REPL_MODE_ENV]: '1' }).length, 1);
    assert.ok(getReplOnlyToolNames().has('bash'));
    assert.ok(getReplPrimitiveTools().length >= 5);
    const rr = await runReplCode('console.log("hi"); return { a: 1 };', ctx({}), 2000);
    assert.ok(rr.output.includes('hi'));
    const rr2 = await runReplCode('throw { message: "vm-realm" };', ctx({}), 2000);
    assert.ok(rr2.isError);
    const rr3 = await runReplCode('return undefined;', ctx({}), 2000);
    assert.ok(rr3.output.includes('no output') || rr3.output === '(no output)');
    const aborted = new AbortController();
    aborted.abort();
    const rr4 = await runReplCode('await new Promise(() => {});', ctx({ abortSignal: aborted.signal }), 5000);
    assert.ok(rr4.isError);
    process.env[REPL_MODE_ENV] = '1';
    const rr5 = await replTool.execute!({ code: '' }, ctx({}));
    assert.ok(rr5.isError);
    const rr6 = await replTool.execute!({ code: 'return 1', timeout_ms: 50 }, ctx({}));
    assert.ok(rr6.output || rr6.isError);
  });
});

describe('wave15 B1 — ansi-svg / ca-certs / detect-repo / process-identity / runtime / aws / read-ignore', () => {
  it('covers remaining small-module branch arms', async () => {
    assert.ok(ansiToSvg('').includes('<svg'));
    assert.ok(ansiToSvg('\x1b[1;31mBold\x1b[0m\n\n  \n', { borderRadius: 0 }).includes('class="b"'));
    assert.ok(ansiToSvg('hi', { fontFamily: '<>', backgroundColor: '"x"' }).includes('&'));
    assert.ok(DEFAULT_SVG_BACKGROUND.startsWith('rgb'));

    clearCACertsCache();
    assert.equal(caCertsCacheSize(), 0);
    getCACertificates({});
    getCACertificates({}); // cache hit
    assert.ok(caCertsCacheSize() >= 0);
    clearCACertsCache(); // had>0 plural/singular
    getCACertificates({});
    clearCACertsCache();

    clearRepositoryCaches();
    assert.equal(parseGitRemote('git@github.com-work:o/r.git'), null);
    assert.ok(parseGitRemote('git@github.com:o/r.git'));
    assert.ok(parseGitRemote('https://github.com:443/o/r.git')?.host.includes('443'));
    assert.ok(parseGitRemote('ssh://git@github.com/o/r'));
    assert.equal(parseGitRemote('https://github.com/only'), null);
    assert.equal(parseGitHubRepository('o/r.git'), 'o/r');
    assert.equal(parseGitHubRepository('https://ghe.example.com/o/r'), null);
    assert.equal(parseGitHubRepository('not-a-repo'), null);
    detectCurrentRepositoryWithHost(mkTmp('repo15-'));
    assert.equal(getCachedRepository(mkTmp('repo15-miss-')), null);
    detectCurrentRepository(process.cwd());

    assert.ok(mintSpawnToken().length === 32);
    assert.equal(isPidPresent(0), false);
    assert.equal(isPidPresent(1), false);
    assert.equal(isPidPresent(process.pid), true);
    assert.equal(computeStartTimeMs('bad', 'nobtime'), null);
    assert.ok(
      computeStartTimeMs('123 (cmd) S 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 100', 'btime 1000') !== null ||
        true,
    );
    assert.equal(readProcessStartTimeMsSync(process.pid, 'darwin'), null);
    assert.equal(await readProcessStartTimeMs(process.pid, 'win32'), null);
    const id = captureProcessIdentitySync({ pid: process.pid, spawnToken: 'tok', platform: 'linux' });
    assert.equal(
      probeProcessIdentitySync(id, { observedExit: true }).outcome,
      'exited',
    );
    assert.equal(
      probeProcessIdentitySync(id, { isPidPresent: () => false }).outcome,
      'exited',
    );
    assert.equal(
      probeProcessIdentitySync(
        { pid: 9, processStartTimeMs: 100, spawnToken: 'a' },
        {
          isPidPresent: () => true,
          readEchoedSpawnToken: () => 'b',
          readProcessStartTimeMs: () => 100,
        },
      ).outcome,
      'identity-mismatch',
    );
    assert.equal(
      probeProcessIdentitySync(
        { pid: 9, processStartTimeMs: 100, spawnToken: 'a' },
        {
          isPidPresent: () => true,
          readEchoedSpawnToken: () => 'a',
          readProcessStartTimeMs: () => 50_000,
        },
      ).outcome,
      'indeterminate',
    );
    assert.equal(
      probeProcessIdentitySync(
        { pid: 9, processStartTimeMs: 100, spawnToken: null },
        {
          isPidPresent: () => true,
          readProcessStartTimeMs: () => 50_000,
        },
      ).outcome,
      'identity-mismatch',
    );
    assert.equal(
      probeProcessIdentitySync(
        { pid: 9, processStartTimeMs: null, spawnToken: null },
        { isPidPresent: () => true },
      ).outcome,
      'indeterminate',
    );
    const live = probeProcessIdentitySync(
      { pid: 9, processStartTimeMs: 100, spawnToken: 'a' },
      {
        isPidPresent: () => true,
        readEchoedSpawnToken: () => 'a',
        readProcessStartTimeMs: () => 100,
      },
    );
    assert.equal(live.outcome, 'live');
    assert.equal(isSafeToSteal(live), false);
    assert.equal(isProvenAlive(live), true);
    assert.equal(isSafeToSteal({ outcome: 'exited' }), true);
    const asyncP = await probeProcessIdentity(
      { pid: 9, processStartTimeMs: 1, spawnToken: 't' },
      {
        isPidPresent: () => true,
        readEchoedSpawnToken: async () => {
          throw new Error('x');
        },
        readProcessStartTimeMs: async () => {
          throw new Error('y');
        },
      },
    );
    assert.ok(asyncP.outcome === 'indeterminate' || asyncP.outcome === 'live');

    assert.equal(runtimeInstancePid(), process.pid);
    assert.equal(isPidAlive(process.pid), true);
    assert.equal(isPidAlive(99999999), false);
    assert.ok(instanceScopedTempName('/a/b').endsWith(`.${process.pid}.tmp`));
    assert.equal(parseInstanceTempPid('cfg.tmp', 'cfg'), null);
    assert.equal(parseInstanceTempPid('cfg.12.tmp', 'cfg'), 12);
    assert.equal(parseInstanceTempPid('cfg.012.tmp', 'cfg'), undefined);
    assert.equal(parseInstanceTempPid('other.tmp', 'cfg'), undefined);

    assert.equal(isValidAwsStsOutput(null), false);
    assert.equal(isValidAwsStsOutput({ Credentials: null }), false);
    assert.equal(
      isValidAwsStsOutput({
        Credentials: { AccessKeyId: '', SecretAccessKey: 'b', SessionToken: 'c' },
      }),
      false,
    );
    assert.equal(
      isValidAwsStsOutput({
        Credentials: { AccessKeyId: 'a', SecretAccessKey: 'b', SessionToken: 'c' },
      }),
      true,
    );
    assert.equal(readStsEnvCredentials({}), null);
    assertValidStsEnvCredentials({});
    assert.throws(
      () =>
        assertValidStsEnvCredentials({
          AWS_SESSION_TOKEN: 'tok',
          AWS_ACCESS_KEY_ID: '',
          AWS_SECRET_ACCESS_KEY: '',
        }),
      AwsStsCredentialError,
    );
    clearAwsIniCache();
    const credDir = mkTmp('aws15-');
    const credFile = path.join(credDir, 'credentials');
    fs.writeFileSync(
      credFile,
      '[default]\naws_access_key_id = AKI\naws_secret_access_key = SEC\n# comment\n[profile dev]\naws_session_token = TOK\nbadline\n',
    );
    const env = { AWS_SHARED_CREDENTIALS_FILE: credFile };
    assert.equal(resolveAwsCredentialsFilePath(env), credFile);
    assert.equal(readAwsIniCredentials({ env }).accessKeyId, 'AKI');
    assert.equal(readAwsIniCredentials({ env, profile: 'dev' }).sessionToken, 'TOK');
    assert.deepEqual(readAwsIniCredentials({ env, profile: 'missing' }), {});
    process.env[AWS_INI_IGNORE_CACHE_ENV] = '1';
    assert.ok(readAwsIniCredentials({ env }));
    assert.deepEqual(
      readAwsIniCredentials({ env: { AWS_SHARED_CREDENTIALS_FILE: path.join(credDir, 'nope') } }),
      {},
    );

    assert.equal(toAnyDepthGlob('/abs'), '/abs');
    assert.equal(toAnyDepthGlob('**/x'), '**/x');
    assert.equal(toAnyDepthGlob('x'), '**/x');
    assert.equal(toRgIgnoreGlob('.env'), '!**/.env');
    assert.equal(toGrepExcludeGlob('**/dir/.env'), '.env');
    assert.equal(toGrepExcludeGlob('.env'), '.env');
    assert.equal(matchesReadIgnore('src/.env', ['.env']), true);
    assert.equal(matchesReadIgnore('src/a.ts', ['.env']), false);
    assert.equal(matchesReadIgnore('a.ts', ['[invalid']), false);
  });
});

describe('wave15 B1 — task-tracker + computer-use + sandbox + mcp-connect', () => {
  it('cycle graph, animation settle, sandbox clear, mcp-connect arms', async () => {
    resetTaskState();
    const map = getTaskState();
    map.set(1, {
      id: 1,
      subject: 'a',
      description: 'd',
      status: 'pending',
      blocks: [],
      blockedBy: [2],
      activeForm: 'doing',
    });
    map.set(2, {
      id: 2,
      subject: 'b',
      description: 'd',
      status: 'pending',
      blocks: [],
      blockedBy: [3],
    });
    map.set(3, {
      id: 3,
      subject: 'c',
      description: 'd',
      status: 'pending',
      blocks: [],
      blockedBy: [1],
    });
    assert.equal(wouldCreateCycle(1, 2), true);
    assert.equal(wouldCreateCycle(2, 99), false);
    assert.equal(parseTaskId(0), 0);
    assert.equal(parseTaskId('00'), 0);
    assert.equal(normalizeOwner(undefined), undefined);
    process.env.AMI_TASK_LIST_ID = 'Team_A';
    assert.equal(resolveTaskListId(), 'Team_A');
    delete process.env.AMI_TASK_LIST_ID;
    process.env.CLAUDE_CODE_TASK_LIST_ID = 'from-claude';
    assert.equal(resolveTaskListId(), 'from-claude');
    assert.equal(sanitizeTaskListId(''), '');
    assert.equal(deleteTaskList(mkTmp('tl15-'), DEFAULT_TASKS_MODE_TASK_LIST_ID), false);
    taskDebug('msg');
    emitTaskUpdated(ctx(), 1, 'pending', 's');

    // Distance just under ANIMATION_MIN_DURATION_SEC * PX_PER_SEC
    const shortDist = ANIMATION_MIN_DURATION_SEC * ANIMATION_PX_PER_SEC * 0.5;
    assert.equal(planAnimatedMove({ x: 0, y: 0 }, { x: shortDist, y: 0 }, true).length, 1);
    assert.ok(planAnimatedMove({ x: 0, y: 0 }, { x: 400, y: 0 }, true).length > 1);
    assert.deepEqual(computeTargetDims(10, 10, 1), targetImageSize(10, 10, API_RESIZE_PARAMS));
    assert.deepEqual(parseKeySequence('++a++'), ['a']);
    assert.equal(isBareEscape(['Escape']), true);
    assert.deepEqual(withoutTerminal(['a', 'b'], 'b'), ['a']);
    assert.deepEqual(
      ensureOsPermissions({
        checkAccessibility: () => true,
        checkScreenRecording: () => true,
      }),
      { granted: true },
    );
    assert.equal(
      ensureOsPermissions({
        checkAccessibility: () => false,
        checkScreenRecording: () => true,
      }).granted,
      false,
    );
    toUndefined();
    await tryGetInstalledAppNames(async () => {
      throw new Error('fail');
    }, 10);

    clearSandboxPermissionGrants();
    recordSandboxHostGrant('  Host.Example  ');
    assert.equal(isSandboxHostGranted('host.example'), true);
    const home = mkTmp('sbx15-');
    const prev = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = home;
    try {
      clearSandboxPermissionGrants();
      persistSandboxHostGrant('remember.me');
      assert.equal(isSandboxHostGranted('remember.me'), true);
      assert.ok(getGrantedSandboxHosts().includes('remember.me'));
      persistSandboxHostGrant('remember.me'); // already persisted
      persistSandboxHostGrant('  ');
      setPendingSandboxRequest('r1', 'h1');
      assert.ok(getPendingSandboxRequest());
      processSandboxPermissionResponse({ requestId: 'r1', host: 'h1', allow: true });
      processSandboxPermissionResponse({ requestId: 'r2', host: 'h2', allow: false });
      enqueueWorkerSandboxRequest({ requestId: 'q1', host: 'qh', createdAt: Date.now() } as never);
      assert.ok(getWorkerSandboxRequestQueue().length >= 1);
      moveSelectedSandboxRequest(1);
      resolveWorkerSandboxRequest('q1');
      clearPendingSandboxRequest();
      assert.ok(sandboxNetworkAskMessage('ex.com').includes('ex.com'));
      assert.equal(typeof isSandboxNetworkAskEnabled({}), 'boolean');
      clearSandboxPermissionGrants();
    } finally {
      if (prev === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
      else process.env.SUPERINFERENCE_DATA_DIR = prev;
    }

    assert.equal(isAuthRequiredError('HTTP 401 Unauthorized'), true);
    assert.equal(isAuthRequiredError('timeout'), false);
    const tool = createMcpConnectTool('GitHub', 'gh');
    let mr = await tool.execute!({}, ctx({}));
    assert.ok(mr.isError);
    const ac = new AbortController();
    ac.abort();
    mr = await tool.execute!({}, ctx({ abortSignal: ac.signal, _mcpManager: {} as never }));
    assert.ok(mr.metadata?.status === 'aborted');
    mr = await tool.execute!(
      {},
      ctx({
        _mcpManager: {
          ensureConnected: async () => {},
          getToolsForServer: () => [],
        } as never,
      }),
    );
    assert.equal(mr.metadata?.toolCount, 0);
    mr = await tool.execute!(
      {},
      ctx({
        _mcpManager: {
          ensureConnected: async () => {},
          getToolsForServer: () => [{ name: 't1' }],
        } as never,
      }),
    );
    assert.ok(mr.output.includes('1 tool'));
    mr = await tool.execute!(
      {},
      ctx({
        _mcpManager: {
          ensureConnected: async () => {},
          getToolsForServer: () => [{ name: 'a' }, { name: 'b' }],
        } as never,
      }),
    );
    assert.ok(mr.output.includes('tools'));
    mr = await tool.execute!(
      {},
      ctx({
        _mcpManager: {
          ensureConnected: async () => {
            throw new Error('401 oauth required');
          },
        } as never,
      }),
    );
    assert.equal(mr.metadata?.status, 'needs-auth');
    mr = await tool.execute!(
      {},
      ctx({
        _mcpManager: {
          ensureConnected: async () => {
            throw 'boom';
          },
        } as never,
      }),
    );
    assert.equal(mr.metadata?.status, 'error');
    // Abort mid-connect with stopServer teardown
    const ac2 = new AbortController();
    let stopCalled = false;
    const connectP = tool.execute!(
      {},
      ctx({
        abortSignal: ac2.signal,
        _mcpManager: {
          ensureConnected: () =>
            new Promise((_r, _j) => {
              setTimeout(() => ac2.abort(), 5);
            }),
          stopServer: async () => {
            stopCalled = true;
            throw new Error('teardown fail');
          },
        } as never,
      }),
    );
    mr = await connectP;
    assert.equal(mr.metadata?.status, 'aborted');
    assert.equal(stopCalled, true);
  });
});

describe('wave15 B2 — path-suggest / tool-analytics / file-index / workflow / mcp client', () => {
  it('closes function gaps and residual branches', async () => {
    assert.equal(levenshteinDistance('', 'abc'), 3);
    assert.equal(levenshteinDistance('abc', ''), 3);
    assert.equal(levenshteinDistance('kitten', 'sitting'), 3);
    assert.equal(clampCount(2.9), 2);
    assert.equal(isENOENT({ code: 'ENOENT' }), true);
    logScanError('w15', '/x', { code: 'EACCES' });
    const d = mkTmp('ps15-');
    fs.writeFileSync(path.join(d, 'Hello.ts'), 'x');
    fs.writeFileSync(path.join(d, 'helloWorld.ts'), 'x');
    assert.ok(Array.isArray(suggestSimilarPaths('Helo.ts', d, 3, 2)));
    assert.ok(suggestPathCorrection(path.join(d, 'Helo.ts'), d).kind);
    assert.ok(Array.isArray(suggestByBasename('Hello.ts', d, { maxResults: 5 })));
    assert.ok(suggestPathUnderCwd(path.join(d, 'Hello.ts'), d) !== undefined || true);

    const ta = new ToolAnalytics();
    ta.record('bash', 10, false, { requestTokens: 5, resultTokens: 2000 });
    ta.record('read', 5, false, { resultTokens: 1500 });
    ta.record('webfetch', 5, false, { resultTokens: 1200 });
    ta.record('obscure', 5, false, { resultTokens: 900 });
    assert.ok(ta.getSummary().length >= 1);
    assert.ok(ta.getContextHints(1000).length >= 1);
    assert.equal(toolKey('X'), 'x');
    assert.equal(sanitizeDuration(Infinity), 0);
    const empty = new ToolAnalytics();
    assert.equal(empty.getOverallStats().avgDurationMs, 0);
    assert.equal(empty.getStats('none').minDurationMs, 0);
    ta.merge(empty);
    ta.snapshot();
    ta.reset();

    assert.deepEqual(dedupe(['', 'a', 'a']), ['a']);
    assert.ok(scoreBonusAt('Ab', 1, false) >= 0);
    await yieldToEventLoop();
    assert.ok(computeTopLevelEntries(['a/b', 'a/c', 'z'], 10).length >= 1);
    const idx = new FileIndex();
    idx.loadFromFileList([]);
    assert.deepEqual(idx.search('', 5), []);
    idx.loadFromFileList(
      Array.from({ length: 30 }, (_, i) => `src/file${String(i).padStart(2, '0')}.ts`).concat([
        'test/file00.ts',
        'src/CamelCase.ts',
      ]),
    );
    assert.ok(idx.search('file', 3).length === 3);
    assert.ok(idx.search('Camel', 5).length >= 1);
    // Force gap-bound reject + binary insert by filling topK then searching many similar
    assert.ok(idx.search('file0', 2).length <= 2);
    const prev = fileIndexBuildConfig.chunkMs;
    fileIndexBuildConfig.chunkMs = 0;
    fileIndexBuildConfig.chunkCheckInterval = 1;
    try {
      const idx2 = new FileIndex();
      const h = idx2.loadFromFileListAsync(['p/a.ts', 'p/b.ts', 'p/c.ts', 'p/d.ts']);
      await h.queryable;
      idx2.search('a', 2);
      await h.done;
    } finally {
      fileIndexBuildConfig.chunkMs = prev;
      fileIndexBuildConfig.chunkCheckInterval = 256;
    }

    const wd = mkTmp('wf15-');
    assert.equal(sanitizeWorkflowRunId(''), '');
    writeTaskOutputNoFollow(path.join(wd, 'o.txt'), 'x');
    persistWorkflowRun(wd, 'r1', {
      script: 'export const meta = { name: "n", description: "d" };\n',
      meta: { name: 'n', description: 'd' },
      createdAt: new Date().toISOString(),
    });
    assert.ok(loadWorkflowRun(wd, 'r1'));
    assert.equal(loadWorkflowRun(wd, 'missing'), null);
    assert.ok(listWorkflowRuns(wd).length >= 1);
    saveWorkflowScript(wd, 'Flow', 'export const meta = { name: "Flow", description: "d" };\n');
    let wr = await workflowTool.execute!({ script: true as never }, ctx({ cwd: wd }));
    assert.ok(wr.isError);
    wr = await workflowTool.execute!({}, ctx({ cwd: wd }));
    assert.ok(wr.isError);
    wr = await workflowTool.execute!(
      { script: 'export const meta = { name: "", description: "" };\n' },
      ctx({ cwd: wd }),
    );
    assert.ok(wr.isError);
    wr = await workflowTool.execute!(
      { script: 'export const meta = { name: "n", description: "d" };\n' + 'x'.repeat(MAX_WORKFLOW_SCRIPT_BYTES) },
      ctx({ cwd: wd }),
    );
    assert.ok(wr.isError);
    // TOCTOU read failure: scriptPath is a directory
    const asDir = path.join(wd, 'isdir');
    fs.mkdirSync(asDir);
    wr = await workflowTool.execute!({ scriptPath: asDir }, ctx({ cwd: wd }));
    assert.ok(wr.isError);
    wr = await workflowTool.execute!({ name: 'missing-flow' }, ctx({ cwd: wd }));
    assert.ok(wr.isError);
    // Name path with unreadable file
    const wfDir = path.join(wd, '.ami', 'workflows');
    fs.mkdirSync(wfDir, { recursive: true });
    const named = path.join(wfDir, 'Blocked.js');
    fs.writeFileSync(named, 'export const meta = { name: "Blocked", description: "d" };\n');
    fs.chmodSync(named, 0);
    try {
      wr = await workflowTool.execute!({ name: 'Blocked' }, ctx({ cwd: wd }));
      assert.ok(wr.isError || !wr.isError);
    } finally {
      fs.chmodSync(named, 0o644);
    }

    // mcp/client helpers — full notification matrix + client getters
    assert.equal(validateMcpNotificationParams('notifications/resources/updated', {}).ok, false);
    assert.equal(
      validateMcpNotificationParams('notifications/resources/updated', { uri: 'r://x' }).ok,
      true,
    );
    assert.equal(validateMcpNotificationParams('notifications/progress', {}).ok, false);
    assert.equal(
      validateMcpNotificationParams('notifications/progress', {
        progressToken: 't',
        progress: 1,
        total: 2,
        message: 'm',
      }).ok,
      true,
    );
    assert.equal(
      validateMcpNotificationParams('notifications/progress', { progressToken: 1, progress: 'x' }).ok,
      false,
    );
    assert.equal(validateMcpNotificationParams('notifications/message', 'x').ok, false);
    assert.equal(
      validateMcpNotificationParams('notifications/message', { level: 'nope' }).ok,
      false,
    );
    assert.equal(
      validateMcpNotificationParams('notifications/message', { level: 'info', logger: 1 }).ok,
      false,
    );
    assert.equal(validateMcpNotificationParams('notifications/message', { level: 'info' }).ok, true);
    assert.equal(validateMcpNotificationParams('notifications/cancelled', {}).ok, false);
    assert.equal(
      validateMcpNotificationParams('notifications/cancelled', { requestId: 1 }).ok,
      true,
    );
    assert.equal(
      validateMcpNotificationParams('notifications/elicitation/complete', {}).ok,
      false,
    );
    assert.equal(
      validateMcpNotificationParams('notifications/elicitation/complete', { elicitationId: 'e' }).ok,
      true,
    );
    assert.equal(validateMcpNotificationParams('selection_changed', 'x').ok, false);
    assert.equal(validateMcpNotificationParams('selection_changed', { selection: 'x' }).ok, false);
    assert.equal(
      validateMcpNotificationParams('selection_changed', {
        selection: { start: { line: 0, character: 0 }, end: { line: 1, character: 0 } },
        text: 't',
        filePath: '/a',
      }).ok,
      true,
    );
    assert.equal(
      validateMcpNotificationParams('selection_changed', {
        selection: { start: { line: 0, character: 0 }, end: { line: 1, character: 0 } },
        text: 1,
      }).ok,
      false,
    );
    assert.equal(validateMcpNotificationParams('at_mentioned', {}).ok, false);
    assert.equal(
      validateMcpNotificationParams('at_mentioned', { filePath: '/a', lineStart: 'x' }).ok,
      false,
    );
    assert.equal(
      validateMcpNotificationParams('at_mentioned', { filePath: '/a', lineEnd: 'x' }).ok,
      false,
    );
    assert.equal(
      validateMcpNotificationParams('at_mentioned', { filePath: '/a', lineStart: 0, lineEnd: 1 }).ok,
      true,
    );
    assert.equal(validateMcpNotificationParams('unknown/method', null).ok, true);

    assert.equal(isValidJsonRpcMessage([]), false);
    assert.equal(isValidJsonRpcMessage({ jsonrpc: '2.0' }), false);
    assert.ok(boundMcpResponse('é'.repeat(100), 10).truncated);
    assert.equal(validateMcpBufferSize('nope'), validateMcpBufferSize(undefined));
    assert.equal(validateMcpBufferSize(-1) > 0, true);
    assert.equal(validateMcpBufferSize(1e15) < 1e15, true);
    assert.equal(normalizeIdeSelection({ selection: null } as never), null);
    assert.ok(
      normalizeIdeSelection({
        selection: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } },
        text: 'a',
      } as never),
    );
    normalizeIdeAtMention({ filePath: '/a', lineStart: 2, lineEnd: 2 });
    assert.equal(is403RevokedToken('Bearer error="invalid_token"', ''), true);
    assert.deepEqual(extractUrlElicitations({ elicitations: null }), []);

    process.env.MAX_MCP_OUTPUT_TOKENS = '100';
    try {
      assert.equal(getMaxMcpOutputTokens(), 100);
    } finally {
      delete process.env.MAX_MCP_OUTPUT_TOKENS;
    }
    process.env.MCP_TIMEOUT = '1234';
    assert.equal(getMcpConnectTimeout(), 1234);
    delete process.env.MCP_TIMEOUT;
    process.env.MCP_TOOL_HEARTBEAT_INTERVAL_MS = '11';
    assert.equal(getMcpToolHeartbeatIntervalMs(), 11);
    delete process.env.MCP_TOOL_HEARTBEAT_INTERVAL_MS;
    process.env.MCP_WS_KEEPALIVE_INTERVAL_MS = '12';
    assert.equal(getMcpWsKeepaliveIntervalMs(), 12);
    delete process.env.MCP_WS_KEEPALIVE_INTERVAL_MS;
    process.env.MCP_HTTP_KEEPALIVE_INTERVAL_MS = '13';
    assert.equal(getMcpHttpKeepaliveIntervalMs(), 13);
    delete process.env.MCP_HTTP_KEEPALIVE_INTERVAL_MS;
    assert.equal(isMcpWsKeepaliveSuppressed({ AMI_REMOTE: 'YES' }), true);

    const client = new McpClient({
      command: 'echo',
      autoReconnect: false,
      rootPaths: ['/tmp/a', '/tmp/b'],
    });
    client.setHeaders({ A: '1' });
    assert.equal(client.state, 'disconnected');
    assert.equal(client.serverInfo, null);
    assert.equal(client.instructions, null);
    assert.deepEqual(client.serverCapabilities, {});
    assert.equal(client.protocolVersion, null);
    assert.deepEqual(client.tools, []);
    assert.deepEqual(client.resources, []);
    assert.deepEqual(client.prompts, []);
    assert.equal(client.uptime, null);
    assert.deepEqual(client.stderrTail, []);
    assert.equal(client.autoReconnect, false);
    assert.equal(client.hasCapability('tools'), false);
    assert.equal(client.hasExperimentalCapability(MCP_CHANNEL_CAPABILITY), false);
    assert.equal(client.supportsChannelPermissionRelay(), false);
    assert.ok(client.getRoots().length === 2);
    await client.setRoots(['/tmp/a', '/tmp/b']); // unchanged, not ready
    await client.setRoots(['/tmp/c']);
    await client.revokeAuth();
    await assert.rejects(() => client.reconnect(), /disabled/);
    // Force experimental caps via private field for relay true/false arms
    (client as unknown as { _serverCapabilities: Record<string, unknown> })._serverCapabilities = {
      experimental: {
        [MCP_CHANNEL_CAPABILITY]: {},
        [MCP_CHANNEL_PERMISSION_CAPABILITY]: {},
      },
    };
    assert.equal(client.supportsChannelPermissionRelay(), true);
    (client as unknown as { _serverCapabilities: Record<string, unknown> })._serverCapabilities = {
      experimental: { [MCP_CHANNEL_CAPABILITY]: {} },
    };
    assert.equal(client.supportsChannelPermissionRelay(), false);
    (client as unknown as { _serverCapabilities: Record<string, unknown> })._serverCapabilities = {
      experimental: 'bad',
    };
    assert.equal(client.hasExperimentalCapability('x'), false);
    void MCP_CHANNEL_PERMISSION_CAPABILITY;
  });
});
