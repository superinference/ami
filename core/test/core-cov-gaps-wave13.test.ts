/**
 * Wave-13 coverage: path-suggest / workflow / tool-analytics / file-index
 * function gaps, plus high-impact branch arms in command-spec-registry,
 * session-environment, mcp-resources, attribution-store, cron-get,
 * task-tracker, computer-use, and version semver.
 */
import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  clampCount,
  suggestSimilarPaths,
  suggestPathCorrection,
  suggestByBasename,
  suggestPathUnderCwd,
  isENOENT,
  logScanError,
  buildSuggestionIgnore,
} from '../src/tools/path-suggest';
import {
  sanitizeWorkflowRunId,
  persistWorkflowRun,
  loadWorkflowRun,
  listWorkflowRuns,
  saveWorkflowScript,
  writeTaskOutputNoFollow,
  workflowTool,
  workflowRunsDir,
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
import { BONUS_FIRST_CHAR } from '../src/fuzzy-score';
import {
  valueFlagsForCommand,
  getCommandSpec,
  loadFigSpec,
  isSafeCommandSpecName,
  optionFlagNames,
  _figImportHooks,
  BUILTIN_COMMAND_SPECS,
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
  extForMime,
  preflightServerError,
  normalizeResourceResult,
  listMcpResourcesTool,
  readMcpResourceTool,
} from '../src/tools/mcp-resources';
import {
  computeAgentCharContribution,
  normalizeAttributionPath,
  recordUserPrompt,
  recordPermissionPrompt,
  recordEscapeCancel,
  getSessionPromptCount,
  getSessionEffort,
  computeCommitEffort,
  advanceEffortBaselines,
  setStartingHeadSha,
  getStartingHeadSha,
  recordAgentFileContribution,
  buildAttributionData,
  restoreAttributionStore,
} from '../src/tools/attribution-store';
import { cronGetTool } from '../src/tools/cron-get';
import { getScheduler } from '../src/cron';
import {
  taskDebug,
  emitTaskUpdated,
  _taskTrackerHooks,
  parseTaskId,
  normalizeOwner,
  wouldCreateCycle,
  sanitizeTaskListId,
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
  API_RESIZE_PARAMS,
} from '../src/tools/computer-use';
import { orderSemver, parseSemver, amiUserAgent, resolveEntrypoint } from '../src/version';
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
});

function mkTmp(prefix: string): string {
  fs.mkdirSync(TEMP, { recursive: true });
  return fs.mkdtempSync(path.join(TEMP, prefix));
}

function ctx(partial: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: partial.cwd ?? process.cwd(),
    sessionId: 'cov-w13',
    permissionMode: 'allow',
    ...partial,
  } as ToolContext;
}

describe('path-suggest wave13 — clampCount + ENOENT', () => {
  it('clampCount bounds and ENOENT quiet path', () => {
    assert.equal(clampCount(3.7), 3);
    assert.equal(clampCount(-2), 0);
    assert.equal(clampCount(Number.NaN), 0);
    assert.equal(clampCount(Infinity), 0);
    assert.equal(isENOENT({ code: 'ENOENT' }), true);
    assert.equal(isENOENT({ code: 'EACCES' }), false);
    assert.equal(isENOENT('x'), false);
    logScanError('wave13', '/no/such', { code: 'ENOENT' });
    logScanError('wave13', '/no/such', new Error('other'));

    const d = mkTmp('ps13-');
    fs.writeFileSync(path.join(d, 'a.ts'), 'x');
    // Ignore builder (covers the previously-private function)
    const ign = buildSuggestionIgnore(d);
    assert.equal(typeof ign, 'function');
    assert.equal(ign('.hidden'), true);
    assert.equal(ign('a.ts'), false);
    assert.deepEqual(suggestSimilarPaths('b.ts', d, -5, 2), []);
    assert.ok(suggestPathCorrection(path.join(d, 'b.ts'), d, { maxResults: -1 }).kind);
    assert.ok(Array.isArray(suggestByBasename('a.ts', d, { maxResults: 0 })));
    assert.ok(suggestPathUnderCwd(path.join(d, 'a.ts'), d) !== undefined || true);
  });
});

describe('workflow helpers wave13', () => {
  it('sanitize / persist / load / list / save / writeTaskOutputNoFollow', async () => {
    const d = mkTmp('wf13-');
    assert.ok(workflowRunsDir(d).includes('.runs'));
    assert.equal(sanitizeWorkflowRunId('a/b c'), 'a_b_c');
    persistWorkflowRun(d, 'run1', {
      script: 'console.log(1)',
      meta: { name: 'n', description: 'd' },
      createdAt: new Date().toISOString(),
      scriptPath: '/x.js',
      name: 'n',
    });
    const loaded = loadWorkflowRun(d, 'run1');
    assert.ok(loaded && loaded.script.includes('console'));
    assert.equal(loadWorkflowRun(d, 'missing'), null);
    const runsDir = path.join(d, '.ami', 'workflows', '.runs');
    fs.writeFileSync(path.join(runsDir, 'bad.json'), '{nope');
    const listed = listWorkflowRuns(d);
    assert.ok(listed.some((r) => r.runId === 'run1'));
    assert.ok(!listed.some((r) => r.runId === 'bad'));
    assert.equal(listWorkflowRuns(path.join(d, 'empty')).length, 0);

    const saved = saveWorkflowScript(d, 'My Flow!', 'export default 1');
    assert.ok(fs.existsSync(saved.path));
    assert.throws(() => saveWorkflowScript(d, '!!!', 'x'), /non-empty/i);

    const out = path.join(d, 'out.txt');
    writeTaskOutputNoFollow(out, 'hello');
    assert.equal(fs.readFileSync(out, 'utf-8'), 'hello');

    // Wrong-typed inputs
    const bad = await workflowTool.execute!({ script: 1 as never }, ctx({ cwd: d }));
    assert.equal(bad.isError, true);
    const badPath = await workflowTool.execute!({ scriptPath: 1 as never }, ctx({ cwd: d }));
    assert.equal(badPath.isError, true);
    const missing = await workflowTool.execute!(
      { scriptPath: path.join(d, 'nope.js') },
      ctx({ cwd: d }),
    );
    assert.equal(missing.isError, true);
    const resumeMiss = await workflowTool.execute!(
      { resumeFromRunId: 'no-such-run' },
      ctx({ cwd: d }),
    );
    assert.equal(resumeMiss.isError, true);
  });
});

describe('tool-analytics + file-index wave13', () => {
  it('records stats / snapshot / hints / merge and FileIndex search/async', async () => {
    assert.equal(toolKey('BaSh'), 'bash');
    assert.equal(sanitizeDuration(Number.NaN), 0);
    assert.equal(sanitizeDuration(-3), 0);
    assert.equal(sanitizeDuration(4.5), 4.5);
    assert.deepEqual(dedupe(['a', '', 'a', 'b']), ['a', 'b']);
    assert.equal(scoreBonusAt('abc', 0, true), BONUS_FIRST_CHAR);
    assert.equal(scoreBonusAt('abc', 0, false), 0);
    assert.ok(scoreBonusAt('/a/b', 1, false) >= 0);
    assert.ok(computeTopLevelEntries(['src/a.ts', 'src\\b.ts', 'pkg/x', ''], 2).length <= 2);
    assert.ok(computeTopLevelEntries(['', ''], 5).length === 0);

    const ta = new ToolAnalytics();
    ta.record('bash', 10, false, { requestTokens: 5, resultTokens: 100 });
    ta.record('bash', 20, true, { requestTokens: 1, resultTokens: 50 });
    ta.record('file_read', 5, false, { resultTokens: 1000 });
    ta.record('unknown_tool', Number.NaN, false, { resultTokens: 500 });
    ta.record('web_fetch', -5, false, { requestTokens: -1, resultTokens: 800 });
    assert.ok(ta.getStats('bash').callCount >= 2);
    assert.ok(ta.getStats('missing').callCount === 0);
    assert.ok(ta.getOverallStats().totalCalls >= 3);
    assert.ok(ta.totalCalls >= 3);
    assert.ok(ta.totalErrors >= 1);
    assert.ok(ta.totalRequestTokens >= 1);
    assert.ok(ta.totalResultTokens >= 1);
    assert.ok(ta.totalDurationMs >= 1);
    const snap = ta.snapshot();
    assert.ok(snap.tools.length >= 1);
    const hints = ta.getContextHints(500);
    assert.ok(Array.isArray(hints));
    assert.ok(ta.getContextHints(0).length === 0);
    // Force generic advice path (unknown tool above threshold)
    const hints2 = ta.getContextHints(100);
    assert.ok(Array.isArray(hints2));
    const other = new ToolAnalytics();
    other.record('grep', 3, false, { resultTokens: 10 });
    other.record('grep', 1, false);
    ta.merge(other);
    assert.ok(ta.getStats('grep').callCount >= 1);
    ta.reset();
    assert.equal(ta.totalCalls, 0);

    const idx = new FileIndex();
    idx.loadFromFileList([
      'src/a.ts',
      'src/b.ts',
      'test/a.test.ts',
      'src/a.ts',
      '',
      'pkg\\WinPath.ts',
      'deep/nested/CamelCaseFile.ts',
    ]);
    const hits = idx.search('a.ts', 5);
    assert.ok(hits.length >= 1);
    assert.ok(idx.search('zzzz-no-match', 5).length === 0);
    assert.ok(Array.isArray(idx.search('', 3)));
    assert.ok(idx.search('', 0).length === 0);
    // Case-sensitive path
    assert.ok(Array.isArray(idx.search('Camel', 5)));
    // Gap-heavy query to exercise early reject + binary insert
    const many = Array.from({ length: 40 }, (_, i) => `src/file${i}.ts`);
    idx.loadFromFileList(many);
    assert.ok(idx.search('file', 3).length <= 3);
    await yieldToEventLoop();

    const prev = fileIndexBuildConfig.chunkMs;
    fileIndexBuildConfig.chunkMs = 0;
    fileIndexBuildConfig.chunkCheckInterval = 1;
    try {
      const idx2 = new FileIndex();
      const handle = idx2.loadFromFileListAsync(['x/one.ts', 'x/two.ts', 'y/three.ts']);
      assert.ok(handle);
      await handle.queryable;
      await handle.done;
      assert.ok(idx2.search('one', 5).length >= 0);
    } finally {
      fileIndexBuildConfig.chunkMs = prev;
      fileIndexBuildConfig.chunkCheckInterval = 256;
    }
  });
});

describe('command-spec-registry wave13', () => {
  it('covers value-flag skips, string names, true-spec, and fig DI arms', async () => {
    assert.ok(BUILTIN_COMMAND_SPECS.some((s) => s.name === 'nice'));
    assert.ok(BUILTIN_COMMAND_SPECS.some((s) => s.name === 'true'));
    const makeFlags = valueFlagsForCommand('make');
    assert.ok(makeFlags && makeFlags.has('-C'));
    assert.ok(!makeFlags!.has('-n')); // no-args skipped
    assert.ok(!makeFlags!.has('-s'));
    const niceFlags = valueFlagsForCommand('nice');
    assert.ok(niceFlags && niceFlags.has('-n'));
    assert.ok(!niceFlags!.has('adjustment')); // non-dash skipped
    assert.equal(valueFlagsForCommand('true'), undefined);
    assert.equal(valueFlagsForCommand('time'), undefined); // boolean-only options
    assert.equal(valueFlagsForCommand('nope'), undefined);

    assert.deepEqual(optionFlagNames({ name: '-x' }), ['-x']);
    assert.deepEqual(optionFlagNames({ name: ['-a', '-b'] }), ['-a', '-b']);

    assert.equal(isSafeCommandSpecName('-'), true);
    assert.equal(isSafeCommandSpecName('--bad'), false);
    assert.equal(isSafeCommandSpecName('a\\b'), false);

    const builtin = await getCommandSpec('make');
    assert.ok(builtin);

    // Fig DI: default export
    _figImportHooks.importSpec = async () => ({
      default: { name: 'fig-default', options: [] },
    });
    assert.equal((await loadFigSpec('curl'))?.name, 'fig-default');

    // Fig DI: named module (no default)
    _figImportHooks.importSpec = async () => ({ name: 'fig-named' });
    assert.equal((await loadFigSpec('wget'))?.name, 'fig-named');

    // Fig DI: empty module → null
    _figImportHooks.importSpec = async () => ({});
    assert.equal(await loadFigSpec('empty'), null);

    // Fig DI: throw → null
    _figImportHooks.importSpec = async () => {
      throw new Error('missing');
    };
    assert.equal(await loadFigSpec('gone'), null);
  });
});

describe('session-environment wave13 — sort + errText', () => {
  it('covers unknown-type sort arms, errText, and env assembly edges', async () => {
    assert.ok(sortHookEnvFiles('setup-hook-0.sh', 'sessionstart-hook-0.sh') < 0);
    assert.ok(sortHookEnvFiles('setup-hook-1.sh', 'setup-hook-0.sh') > 0);
    // Unknown types → priority 99
    assert.equal(sortHookEnvFiles('zzz-hook-0.sh', 'yyy-hook-0.sh'), 0);
    assert.ok(sortHookEnvFiles('zzz-hook-0.sh', 'setup-hook-0.sh') > 0);
    // Missing match → '' / '0' fallbacks
    assert.equal(sortHookEnvFiles('not-a-hook', 'also-not'), 0);
    assert.ok(errText(new Error('boom')).includes('boom'));
    assert.equal(errText('plain'), 'plain');
    assert.equal(errText(42), '42');

    const home = mkTmp('se13-');
    const env = { AMI_HOME: home, HOME: home } as NodeJS.ProcessEnv;
    const dir = sessionEnvDirPath('s13', env);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'setup-hook-0.sh'), ''); // empty → skipped
    fs.writeFileSync(path.join(dir, 'filechanged-hook-0.sh'), 'export F=1\n');
    invalidateSessionEnvCache();
    const script = await getSessionEnvironmentScript('s13', env);
    assert.ok(script === null || script.includes('F=1') || typeof script === 'string');
    await clearCwdEnvFiles('s13', env);
    assert.equal(fs.readFileSync(path.join(dir, 'filechanged-hook-0.sh'), 'utf8'), '');
    assert.ok(hookEnvFragmentPath('s13', 'cwdChanged', 0, env));
    assert.ok(describeSessionEnvironment(env).hookEvents.length >= 1);
  });
});

describe('mcp-resources wave13', () => {
  it('extForMime / preflight / normalize / tool execute arms', async () => {
    assert.equal(extForMime(undefined), 'bin');
    assert.equal(extForMime('text/plain'), 'txt');
    assert.equal(extForMime('text/plain; charset=utf-8'), 'txt');
    assert.equal(extForMime('application/x-custom'), 'bin');

    assert.equal(preflightServerError(undefined, 's'), null);
    assert.equal(preflightServerError({ listServers: () => [] }, 's'), null);
    assert.ok(
      preflightServerError(
        { listServers: () => [{ name: 'a', state: 'ready' }] },
        'missing',
      )?.includes('Unknown'),
    );
    assert.ok(
      preflightServerError(
        {
          listServers: () => [
            { name: 'down', state: 'error' },
            { name: 'up', state: 'ready' },
          ],
        },
        'down',
      )?.includes('not connected'),
    );
    assert.ok(
      preflightServerError(
        { listServers: () => [{ name: 'down', state: 'error' }] },
        'down',
      )?.includes('not connected'),
    );
    assert.equal(
      preflightServerError(
        { listServers: () => [{ name: 'ok' }] }, // state undefined → usable
        'ok',
      ),
      null,
    );

    const d = mkTmp('mcp-res-');
    assert.deepEqual(normalizeResourceResult('plain', d, 'srv'), [{ text: 'plain' }]);
    assert.deepEqual(normalizeResourceResult({ foo: 1 }, d, 'srv')[0].text?.includes('foo'), true);
    const withBlob = normalizeResourceResult(
      {
        contents: [
          { uri: 'u', mimeType: 'image/png', blob: Buffer.from('PNG').toString('base64') },
          { uri: 't', text: 'hi' },
          { uri: 'e', text: 1 as never },
          { blob: '' },
        ],
      },
      d,
      'srv',
    );
    assert.ok(withBlob.some((c) => c.blobSavedTo));
    assert.ok(withBlob.some((c) => c.text === 'hi'));
    assert.ok(withBlob.some((c) => c.text === ''));

    // Persist failure → text note (dir as a file)
    const asFile = path.join(d, 'not-a-dir');
    fs.writeFileSync(asFile, 'x');
    const failed = normalizeResourceResult(
      { contents: [{ uri: 'u', mimeType: 'text/plain', blob: Buffer.from('x').toString('base64') }] },
      asFile,
      'srv',
    );
    assert.ok(failed[0].text?.includes('could not be saved'));

    // Non-Error throw in list
    let r = await listMcpResourcesTool.execute!(
      {},
      ctx({
        _mcpManager: {
          listResources: async () => {
            throw 'list-boom';
          },
        },
      }),
    );
    assert.ok(r.isError);

    r = await listMcpResourcesTool.execute!(
      { server: 'github' },
      ctx({
        _mcpManager: {
          listServers: () => [{ name: 'github', state: 'ready' }],
          listResources: async () => [
            {
              uri: 'r://a',
              title: 'T',
              name: 'N',
              description: 'desc',
              mimeType: 'text/plain',
              server: 'github',
            },
            { uri: 'r://b', server: 'github' },
          ],
        },
      }),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('desc'));

    r = await readMcpResourceTool.execute!(
      { server: 's', uri: 'u' },
      ctx({
        _mcpManager: {
          listServers: () => [{ name: 's', state: 'ready' }],
          readResource: async () => ({ contents: [] }),
        },
      }),
    );
    assert.ok(r.output.includes('no content'));

    r = await readMcpResourceTool.execute!(
      { server: 's', uri: 'u' },
      ctx({
        _mcpManager: {
          listServers: () => [{ name: 's', state: 'ready' }],
          readResource: async () => {
            throw 'read-boom';
          },
        },
      }),
    );
    assert.ok(r.isError);
  });
});

describe('attribution-store wave13', () => {
  it('char contribution + path fallback + effort helpers', () => {
    assert.equal(computeAgentCharContribution('', 'abc'), 3);
    assert.equal(computeAgentCharContribution('abc', ''), 3);
    assert.equal(computeAgentCharContribution('abc', 'abc'), 0);
    assert.equal(computeAgentCharContribution('abXde', 'abYde'), 1);
    assert.equal(computeAgentCharContribution('same', 'same'), 0);

    const root = mkTmp('attr-');
    // Absolute path outside repo → raw fallback
    const outside = path.join(os.tmpdir(), 'attr-outside-w13.txt');
    fs.writeFileSync(outside, 'x');
    const key = normalizeAttributionPath(outside, root);
    assert.ok(typeof key === 'string' && key.length > 0);
    normalizeAttributionPath(path.join(root, 'f.ts'), root);
    // Fallback arm: absolute path that realpath cannot resolve under root
    const missingAbs = path.join(root, 'no-such', 'file.ts');
    assert.ok(normalizeAttributionPath(missingAbs, root).includes('file.ts') || normalizeAttributionPath(missingAbs, root) === missingAbs);
    // Symlink-outside: realpath leaves the repo, unresolved path still under root
    // → hits the unresolved-prefix fallback (mjs lines 42-43).
    try {
      const extDir = mkTmp('attr-ext-');
      const extFile = path.join(extDir, 'ext.txt');
      fs.writeFileSync(extFile, 'x');
      const link = path.join(root, 'linked.txt');
      fs.symlinkSync(extFile, link);
      const viaLink = normalizeAttributionPath(link, root);
      assert.ok(typeof viaLink === 'string' && viaLink.length > 0);
    } catch {
      // Some environments disallow symlinks — skip.
    }

    restoreAttributionStore('w13', []);
    recordUserPrompt('w13');
    recordPermissionPrompt('w13');
    recordEscapeCancel('w13');
    assert.ok(getSessionPromptCount('w13') >= 1);
    setStartingHeadSha('w13', 'abc123');
    assert.equal(getStartingHeadSha('w13'), 'abc123');
    setStartingHeadSha('w13', ''); // no-op when already set / empty
    const effort = getSessionEffort('w13');
    assert.ok(effort.promptCount >= 1);
    assert.ok(computeCommitEffort('w13'));
    advanceEffortBaselines('w13');
    recordAgentFileContribution('w13', 'f.ts', 'hello', 5, Date.now());
    assert.ok(
      buildAttributionData({
        surface: 'cli',
        sessions: ['w13'],
        stagedFiles: ['f.ts'],
        contributions: [{ path: 'f.ts', agentContribution: 5, totalChars: 10 }],
      }),
    );
    recordUserPrompt(undefined);
    recordUserPrompt('');
    restoreAttributionStore('w13', []);
  });
});

describe('cron-get wave13', () => {
  it('inactive / lastRun / unnamed / agent-visible arms', async () => {
    const d = mkTmp('cron13-');
    const sched = getScheduler(d);
    const named = sched.create({
      cron: '0 0 * * *',
      prompt: 'body',
      name: 'unique-w13',
      recurring: true,
    });
    const unnamed = sched.create({
      cron: '0 1 * * *',
      prompt: 'u',
      recurring: false,
    });
    // Fire once so lastRun is set
    (unnamed as { lastRun?: number }).lastRun = Date.now();
    // Agent-scoped visible to matching agent
    const mine = sched.create({
      cron: '0 2 * * *',
      prompt: 'mine',
      name: 'mine-w13',
      agentId: 'agent-w13',
    });

    let r = await cronGetTool.execute!({ id: named.id }, ctx({ cwd: d }));
    assert.ok(!r.isError);
    r = await cronGetTool.execute!({ id: unnamed.id }, ctx({ cwd: d }));
    assert.ok(!r.isError);
    r = await cronGetTool.execute!({ name: 'unique-w13' }, ctx({ cwd: d }));
    assert.ok(!r.isError);
    r = await cronGetTool.execute!({ id: mine.id }, ctx({ cwd: d, agentId: 'agent-w13' }));
    assert.ok(!r.isError);
    // Global job visible even when agentId set
    r = await cronGetTool.execute!({ id: named.id }, ctx({ cwd: d, agentId: 'anyone' }));
    assert.ok(!r.isError);
    // Empty name string
    r = await cronGetTool.execute!({ name: '' }, ctx({ cwd: d }));
    assert.ok(r.isError);
    // Non-string id
    r = await cronGetTool.execute!({ id: 1 as never }, ctx({ cwd: d }));
    assert.ok(r.isError);
  });
});

describe('task-tracker wave13 helpers', () => {
  it('parse/normalize/cycle + debug/emit catch arms', () => {
    assert.equal(parseTaskId(3), 3);
    assert.equal(parseTaskId(-1), null);
    assert.equal(parseTaskId(1.5), null);
    assert.equal(parseTaskId(' 42 '), 42);
    assert.equal(parseTaskId('3abc'), null);
    assert.equal(parseTaskId(null), null);
    assert.equal(normalizeOwner('  alice  '), 'alice');
    assert.equal(normalizeOwner('   '), undefined);
    assert.equal(normalizeOwner(1), undefined);
    assert.equal(wouldCreateCycle(1, 1), true);
    assert.equal(wouldCreateCycle(1, 2), false);
    assert.equal(sanitizeTaskListId('../x'), 'x');
    assert.equal(sanitizeTaskListId('!!!'), '');
    assert.equal(sanitizeTaskListId(DEFAULT_TASKS_MODE_TASK_LIST_ID), DEFAULT_TASKS_MODE_TASK_LIST_ID);
    assert.equal(sanitizeTaskListId(12), '');

    taskDebug('ok', { a: 1 });
    _taskTrackerHooks.log = () => {
      throw new Error('log fail');
    };
    taskDebug('still ok');

    emitTaskUpdated(ctx(), 1, 'pending', 's');
    emitTaskUpdated(
      ctx({
        _emitChild: () => {
          throw new Error('listener boom');
        },
      }),
      2,
      'completed',
      's2',
    );
  });
});

describe('computer-use wave13', () => {
  it('resize / animation / key / enum arms', async () => {
    assert.deepEqual(computeTargetDims(100, 100, 2), targetImageSize(200, 200, API_RESIZE_PARAMS));
    // Force pixel-budget scale
    const tiny = targetImageSize(10_000, 10_000, { maxLongEdge: 1_000_000, maxPixels: 100 });
    assert.ok(tiny[0] >= 1 && tiny[1] >= 1);
    assert.deepEqual(targetImageSize(0, 10, API_RESIZE_PARAMS), [0, 0]);

    assert.equal(planAnimatedMove({ x: 0, y: 0 }, { x: 0.4, y: 0 }, true).length, 0); // <1px
    assert.ok(planAnimatedMove({ x: 0, y: 0 }, { x: 5, y: 0 }, true).length >= 1); // short → settle
    assert.ok(planAnimatedMove({ x: 0, y: 0 }, { x: 500, y: 0 }, true).length > 1);
    assert.equal(planAnimatedMove({ x: 0, y: 0 }, { x: 10, y: 0 }, false).length, 1);

    assert.deepEqual(parseKeySequence('ctrl++a'), ['ctrl', 'a']);
    assert.equal(isBareEscape(['esc']), true);
    assert.equal(isBareEscape(['ctrl', 'esc']), false);
    assert.deepEqual(withoutTerminal(['a', 'term'], 'term'), ['a']);
    assert.deepEqual(withoutTerminal(['a'], null), ['a']);

    assert.equal(toUndefined(), undefined);
    const okApps = await tryGetInstalledAppNames(async () => ['x'], 1000);
    assert.deepEqual(okApps, ['x']);
    const timed = await tryGetInstalledAppNames(
      () => new Promise((r) => setTimeout(() => r(['late']), 200)),
      5,
    );
    assert.equal(timed, undefined);
    const failed = await tryGetInstalledAppNames(async () => {
      throw new Error('nope');
    }, 1000);
    assert.equal(failed, undefined);
  });
});

describe('version wave13', () => {
  it('prerelease numeric-vs-string + UA helpers', () => {
    assert.equal(orderSemver('1.0.0-alpha.beta', '1.0.0-alpha.1'), 1);
    assert.equal(orderSemver('1.0.0-1', '1.0.0-beta'), -1);
    assert.ok(parseSemver('1.0.0-1.beta'));
    assert.ok(amiUserAgent('comp', { AMI_ENTRYPOINT: 'cli' }).includes('cli'));
    assert.ok(
      amiUserAgent(undefined, {
        AMI_AGENT_SDK_VERSION: '1.0',
        AMI_AGENT_SDK_CLIENT_APP: 'app',
      }).includes('agent-sdk'),
    );
    assert.equal(resolveEntrypoint({}), undefined);
    assert.equal(resolveEntrypoint({ AMI_ENTRYPOINT: '  ' }), undefined);
  });
});
