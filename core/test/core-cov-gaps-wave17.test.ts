/**
 * Wave-17 coverage: residual || / ?? / catch / instanceof arms on the highest-
 * drag small modules left after wave15+16 (final18 baseline). Does NOT redo
 * those waves — only fills the leftover branch/line gaps.
 *
 * Priority: command-spec-registry, session-environment, attribution-store
 * (42-43), cron-get, mcp-resources, archive-safety (43 / 138-139), task-kill,
 * mailbox-fs (78 / 97), treeify (19-21), process-hardening — plus a few easy
 * shell / worktree arms when cheap.
 */
import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { deflateRawSync } from 'node:zlib';

import {
  valueFlagsForCommand,
  loadFigSpec,
  isSafeCommandSpecName,
  optionFlagNames,
  _figImportHooks,
  getCommandSpec,
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
  computeAgentCharContribution,
  normalizeAttributionPath,
  recordUserPrompt,
  recordPermissionPrompt,
  restoreAttributionStore,
  getSessionPromptCount,
  buildAttributionData,
  buildAttributionSnapshot,
  setStartingHeadSha,
  getStartingHeadSha,
  advanceEffortBaselines,
  computeCommitEffort,
} from '../src/tools/attribution-store';
import { cronGetTool } from '../src/tools/cron-get';
import { getScheduler } from '../src/cron';
import {
  extForMime,
  preflightServerError,
  normalizeResourceResult,
  listMcpResourcesTool,
  readMcpResourceTool,
} from '../src/tools/mcp-resources';
import { isArchivePathSafe, parseZipModes, ArchiveSafetyError, extractZip } from '../src/archive-safety';
import { taskKillTool } from '../src/tools/task-kill';
import {
  appendToInbox,
  takeFsInbox,
  listInboxPeers,
  inboxFilePath,
  inboxDir,
  _mailboxFsHooks,
} from '../src/tools/mailbox-fs';
import { treeify, _treeifyHooks, TREE_COLOR_ROLES } from '../src/utils/treeify';
import {
  hardenProcess,
  describeProcessHardening,
  processHardeningDisabled,
  blockPtrace,
  loadLibcPrctl,
  bunFfiRequire,
  PROCESS_HARDENING_DISABLE_ENV,
  COREDUMP_FILTER_PATH,
  PR_SET_DUMPABLE,
} from '../src/utils/process-hardening';
import {
  shellHooks,
  findPowerShell,
  resetPowerShellCache,
} from '../src/utils/shell';
import {
  withFetchHeadLock,
  _fetchHeadLockHooks,
  generateTmuxSessionName,
  parsePRReference,
} from '../src/worktree-manager';
import type { ToolContext } from '../src/types';

const TEMP = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-cov-'));

afterEach(() => {
  mock.restoreAll();
  invalidateSessionEnvCache();
  _sessionEnvHooks.platform = process.platform;
  _figImportHooks.importSpec = (specifier) =>
    import(specifier) as Promise<{ default?: import('../src/tools/command-spec-registry').CommandSpec }>;
  _treeifyHooks.preVisited = [];
  _mailboxFsHooks.unlinkSync = (p) => {
    fs.unlinkSync(p);
  };
  delete process.env[PROCESS_HARDENING_DISABLE_ENV];
  resetPowerShellCache();
  _fetchHeadLockHooks.openSync = fs.openSync;
  _fetchHeadLockHooks.closeSync = fs.closeSync;
  _fetchHeadLockHooks.unlinkSync = fs.unlinkSync;
});

function mkTmp(prefix: string): string {
  fs.mkdirSync(TEMP, { recursive: true });
  return fs.mkdtempSync(path.join(TEMP, prefix));
}

function ctx(partial: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: partial.cwd ?? process.cwd(),
    sessionId: 'cov-w17',
    permissionMode: 'allow',
    ...partial,
  } as ToolContext;
}

describe('wave17 — command-spec-registry residual', () => {
  it('hits time empty-flags, fig default/name matrix, safe-name || arms, non-builtin get', async () => {
    assert.equal(valueFlagsForCommand('time'), undefined);
    assert.ok(BUILTIN_COMMAND_SPECS.some((s) => s.name === 'time'));

    assert.deepEqual(optionFlagNames({ name: ['-a', '-b'] }), ['-a', '-b']);
    assert.deepEqual(optionFlagNames({ name: '-s' }), ['-s']);

    assert.equal(isSafeCommandSpecName('a/b'), false);
    assert.equal(isSafeCommandSpecName('a\\b'), false);
    assert.equal(isSafeCommandSpecName('-'), true);
    assert.equal(isSafeCommandSpecName('-x'), false);
    assert.equal(isSafeCommandSpecName('a..b'), false);

    _figImportHooks.importSpec = async () => ({ default: { name: 'from-default' } });
    assert.equal((await loadFigSpec('figdef'))?.name, 'from-default');
    _figImportHooks.importSpec = async () => ({ name: 'from-named' });
    assert.equal((await loadFigSpec('figname'))?.name, 'from-named');
    _figImportHooks.importSpec = async () => ({ default: undefined, name: '' });
    assert.equal(await loadFigSpec('fignull'), null);
    _figImportHooks.importSpec = async () => {
      throw new Error('missing-pkg');
    };
    assert.equal(await loadFigSpec('figmiss'), null);
    assert.equal(await loadFigSpec('-'), null);

    _figImportHooks.importSpec = async () => ({ default: { name: 'fig-only' } });
    assert.equal((await getCommandSpec('w17-fig-only-cmd'))?.name, 'fig-only');
  });
});

describe('wave17 — session-environment residual', () => {
  it('cache hit, CLAUDE_ENV_FILE, empty trim, unknown sort ??99, describe configured', async () => {
    _sessionEnvHooks.platform = process.platform === 'win32' ? 'linux' : process.platform;
    const home = mkTmp('se17-');
    const env: NodeJS.ProcessEnv = { AMI_HOME: home, HOME: home };
    const dir = sessionEnvDirPath('w17', env);
    fs.mkdirSync(dir, { recursive: true });

    const emptyEnv = path.join(home, 'empty.env');
    fs.writeFileSync(emptyEnv, '   \n  ');
    invalidateSessionEnvCache();
    assert.equal(await getSessionEnvironmentScript('w17', { ...env, CLAUDE_ENV_FILE: emptyEnv }), null);

    const realEnv = path.join(home, 'real.env');
    fs.writeFileSync(realEnv, 'export W17=1\n');
    fs.writeFileSync(path.join(dir, 'setup-hook-0.sh'), 'export H=1\n');
    fs.writeFileSync(path.join(dir, 'cwdchanged-hook-0.sh'), 'export C=1\n');
    invalidateSessionEnvCache();
    const script = await getSessionEnvironmentScript('w17', { ...env, AMI_ENV_FILE: realEnv });
    assert.ok(script && script.includes('W17') && script.includes('H=1'));

    const cached = await getSessionEnvironmentScript('w17', { ...env, AMI_ENV_FILE: realEnv });
    assert.equal(cached, script);

    await clearCwdEnvFiles('w17', env);
    assert.equal(fs.readFileSync(path.join(dir, 'cwdchanged-hook-0.sh'), 'utf8'), '');
    assert.ok(fs.readFileSync(path.join(dir, 'setup-hook-0.sh'), 'utf8').includes('H=1'));

    assert.ok(sortHookEnvFiles('zzz-hook-1.sh', 'setup-hook-0.sh') !== 0);
    assert.equal(errText(new Error('boom')), 'boom');
    assert.equal(errText(42), '42');

    const d = describeSessionEnvironment({ AMI_ENV_FILE: ' /tmp/x ' });
    assert.equal(d.envFileConfigured, 'AMI_ENV_FILE');
    assert.ok(d.supported);
    _sessionEnvHooks.platform = 'win32';
    assert.equal(describeSessionEnvironment({}).supported, false);
    assert.equal(await getSessionEnvironmentScript('w17', env), null);
    _sessionEnvHooks.platform = process.platform;

    assert.ok(sessionEnvDirPath('   ', env).endsWith(`${path.sep}default`));
    assert.equal(hookEnvFragmentPath('w17', 'PostToolUse', 0, env), null);
    assert.ok(hookEnvFragmentPath('w17', 'SessionStart', 0, env));
  });
});

describe('wave17 — attribution-store lines 42-43 + effort arms', () => {
  it('fallback relative when realpath escapes repo but raw path is under root', () => {
    const root = mkTmp('attr17-');
    const outside = mkTmp('attr17-out-');
    const target = path.join(outside, 'escaped.ts');
    fs.writeFileSync(target, 'x');
    const link = path.join(root, 'escape-link');
    let linked = false;
    try {
      fs.symlinkSync(target, link);
      linked = true;
    } catch {
      /* FS may disallow symlinks */
    }
    if (linked) {
      assert.equal(normalizeAttributionPath(link, root), 'escape-link');
    } else {
      assert.ok(normalizeAttributionPath(path.join(root, 'missing.ts'), root).includes('missing'));
      assert.equal(normalizeAttributionPath('/elsewhere/x.ts', root), '/elsewhere/x.ts');
    }

    assert.equal(computeAgentCharContribution('abcXYZ', 'abcXYZ'), 0);
    assert.equal(computeAgentCharContribution('abc', 'abcDEF'), 3);

    const sid = 'w17-attr';
    recordUserPrompt(sid);
    recordPermissionPrompt(sid);
    setStartingHeadSha(sid, 'abc123');
    assert.equal(getStartingHeadSha(sid), 'abc123');
    setStartingHeadSha(sid, 'other');
    assert.equal(getStartingHeadSha(sid), 'abc123');
    assert.ok(computeCommitEffort(sid).steers >= 1);
    advanceEffortBaselines(sid);
    assert.equal(computeCommitEffort(sid).steers, 0);

    const data = buildAttributionData({
      surface: 'cli',
      sessions: [sid],
      stagedFiles: ['a.ts', 'package-lock.json'],
      contributions: [
        { path: 'a.ts', agentContribution: 10, totalChars: 20 },
        { path: 'package-lock.json', agentContribution: 5, totalChars: 5 },
      ],
    });
    assert.ok(data.summary.agentChars >= 0);
    assert.ok(Array.isArray(data.excludedGenerated));

    const snap = buildAttributionSnapshot(sid, 'msg1', 'cli');
    assert.equal(restoreAttributionStore(sid, [snap]), true);
    assert.ok(getSessionPromptCount(sid) >= 0);
    assert.equal(
      restoreAttributionStore('w17-sparse', [
        {
          type: 'attribution-snapshot',
          messageId: 'm',
          surface: 'cli',
          fileStates: {},
        } as never,
      ]),
      true,
    );
  });
});

describe('wave17 — cron-get residual', () => {
  it('ownership hide, name miss, active+lastRun+name success, inactive no last', async () => {
    const d = mkTmp('cron17-');
    const sched = getScheduler(d);
    const owned = sched.create({
      cron: '0 5 * * *',
      prompt: 'secret-prompt-full',
      name: 'owned-w17',
      agentId: 'alice',
      recurring: true,
    });
    let r = await cronGetTool.execute!({ id: owned.id }, ctx({ cwd: d, agentId: 'bob' }));
    assert.ok(r.isError);
    assert.match(r.output, /not found/);

    r = await cronGetTool.execute!({ id: owned.id }, ctx({ cwd: d, agentId: 'alice' }));
    assert.ok(!r.isError);
    assert.ok(r.output.includes('secret-prompt-full'));
    assert.equal(r.metadata?.name, 'owned-w17');

    r = await cronGetTool.execute!({ id: owned.id }, ctx({ cwd: d }));
    assert.ok(!r.isError);

    r = await cronGetTool.execute!({ name: 'no-such-w17' }, ctx({ cwd: d }));
    assert.ok(r.isError);

    r = await cronGetTool.execute!({ name: '' }, ctx({ cwd: d }));
    assert.ok(r.isError);

    const one = sched.create({
      cron: '0 6 * * *',
      prompt: 'once',
      name: 'once-w17',
      recurring: false,
    });
    (one as { nextRun?: number | null }).nextRun = null;
    (one as { lastRun?: number }).lastRun = Date.now() - 1000;
    r = await cronGetTool.execute!({ id: one.id }, ctx({ cwd: d }));
    assert.ok(!r.isError);
    assert.equal(r.metadata?.active, false);
    assert.ok(typeof r.metadata?.lastRun === 'number');

    const anon = sched.create({ cron: '0 7 * * *', prompt: 'anon', recurring: true });
    r = await cronGetTool.execute!({ id: anon.id }, ctx({ cwd: d }));
    assert.ok(!r.isError);
    assert.equal(r.metadata?.name, undefined);
  });
});

describe('wave17 — mcp-resources residual', () => {
  it('mime/preflight/list/read/normalize catch arms', async () => {
    assert.equal(extForMime(undefined), 'bin');
    assert.equal(extForMime('image/png; charset=utf-8'), 'png');
    assert.equal(extForMime('application/x-unknown'), 'bin');

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
        { listServers: () => [{ name: 'solo', state: 'connecting' }] },
        'solo',
      )?.includes('not connected'),
    );
    assert.equal(
      preflightServerError({ listServers: () => [{ name: 'ok', state: 'ready' }] }, 'ok'),
      null,
    );
    assert.equal(
      preflightServerError({ listServers: () => [{ name: 'nostate' }] }, 'nostate'),
      null,
    );

    const tmp = mkTmp('mcp17-');
    assert.ok(normalizeResourceResult({ foo: 1 }, tmp, 's')[0]?.text);
    assert.ok(normalizeResourceResult('plain', tmp, 's')[0]?.text === 'plain');
    const blob = Buffer.from('hello-blob').toString('base64');
    const saved = normalizeResourceResult(
      { contents: [{ uri: 'u://x', mimeType: 'text/plain', blob }] },
      tmp,
      'srv',
    );
    assert.ok(saved[0]?.blobSavedTo);
    const asFile = path.join(tmp, 'notdir');
    fs.writeFileSync(asFile, 'x');
    const failed = normalizeResourceResult(
      { contents: [{ uri: 'u://y', mimeType: 'image/png', blob }] },
      asFile,
      'srv',
    );
    assert.ok(failed[0]?.text?.includes('could not be saved'));
    assert.equal(
      normalizeResourceResult({ contents: [{ uri: 'u', text: 1 as never }] }, tmp, 's')[0]?.text,
      '',
    );

    let r = await listMcpResourcesTool.execute!({}, ctx({}));
    assert.ok(r.isError);

    r = await listMcpResourcesTool.execute!(
      { server: 'nope' },
      ctx({
        _mcpManager: {
          listServers: () => [{ name: 'only', state: 'ready' }],
          listResources: async () => [],
        } as never,
      }),
    );
    assert.ok(r.isError);

    r = await listMcpResourcesTool.execute!(
      {},
      ctx({
        _mcpManager: {
          listResources: async () => [],
        } as never,
      }),
    );
    assert.ok(r.output.includes('No MCP resources'));

    r = await listMcpResourcesTool.execute!(
      {},
      ctx({
        _mcpManager: {
          listResources: async () => [
            {
              uri: 'res://a',
              name: 'A',
              title: 'Title',
              mimeType: 'text/plain',
              description: 'desc',
              server: 's1',
            },
          ],
        } as never,
      }),
    );
    assert.ok(r.output.includes('res://a'));
    assert.ok(r.output.includes('[s1]'));

    r = await listMcpResourcesTool.execute!(
      { server: 's1' },
      ctx({
        _mcpManager: {
          listServers: () => [{ name: 's1', state: 'ready' }],
          listResources: async () => [{ uri: 'res://b', name: 'B' }],
        } as never,
      }),
    );
    assert.ok(r.output.includes('res://b'));

    r = await listMcpResourcesTool.execute!(
      {},
      ctx({
        _mcpManager: {
          listResources: async () => {
            throw new Error('list-boom');
          },
        } as never,
      }),
    );
    assert.ok(r.isError && r.output.includes('list-boom'));
    r = await listMcpResourcesTool.execute!(
      {},
      ctx({
        _mcpManager: {
          listResources: async () => {
            throw 'string-err';
          },
        } as never,
      }),
    );
    assert.ok(r.isError && r.output.includes('string-err'));

    r = await readMcpResourceTool.execute!({ server: 's', uri: 'u' }, ctx({}));
    assert.ok(r.isError);

    r = await readMcpResourceTool.execute!(
      { server: 's', uri: 'u' },
      ctx({
        _mcpManager: {
          listServers: () => [{ name: 's', state: 'ready' }],
          readResource: async () => ({ contents: [] }),
        } as never,
      }),
    );
    assert.ok(r.output.includes('no content'));

    r = await readMcpResourceTool.execute!(
      { server: 's', uri: 'u' },
      ctx({
        _mcpManager: {
          listServers: () => [{ name: 's', state: 'ready' }],
          readResource: async () => ({ contents: [{ uri: 'u', text: 'body' }] }),
        } as never,
      }),
    );
    assert.ok(r.output.includes('body'));

    r = await readMcpResourceTool.execute!(
      { server: 's', uri: 'u' },
      ctx({
        _mcpManager: {
          listServers: () => [{ name: 's', state: 'ready' }],
          readResource: async () => {
            throw 99;
          },
        } as never,
      }),
    );
    assert.ok(r.isError && r.output.includes('99'));
  });
});

describe('wave17 — archive-safety lines 43 + 138-139', () => {
  it('findEocd fallthrough + malformed local header throw', () => {
    assert.deepEqual(parseZipModes(new Uint8Array(Buffer.alloc(30, 0x41))), {});
    assert.deepEqual(parseZipModes(new Uint8Array(Buffer.from('x'.repeat(40)))), {});

    const name = Buffer.from('bad.txt');
    const central = Buffer.alloc(46 + name.length);
    central.writeUInt32LE(0x02014b50, 0);
    central.writeUInt16LE(3 << 8, 4);
    central.writeUInt16LE(0, 10);
    central.writeUInt32LE(0, 20);
    central.writeUInt32LE(0, 24);
    central.writeUInt16LE(name.length, 28);
    central.writeUInt32LE(0, 42);
    name.copy(central, 46);
    const eocd = Buffer.alloc(22);
    eocd.writeUInt32LE(0x06054b50, 0);
    eocd.writeUInt16LE(1, 8);
    eocd.writeUInt16LE(1, 10);
    eocd.writeUInt32LE(central.length, 12);
    eocd.writeUInt32LE(0, 16);
    const zip = Buffer.concat([central, eocd]);
    assert.throws(() => extractZip(new Uint8Array(zip)), /Malformed local header/);

    const central2 = Buffer.from(central);
    central2.writeUInt32LE(999999, 42);
    const eocd2 = Buffer.alloc(22);
    eocd2.writeUInt32LE(0x06054b50, 0);
    eocd2.writeUInt16LE(1, 8);
    eocd2.writeUInt16LE(1, 10);
    eocd2.writeUInt32LE(central2.length, 12);
    eocd2.writeUInt32LE(5, 16);
    const zip3 = Buffer.concat([Buffer.alloc(5, 0x11), central2, eocd2]);
    assert.throws(() => extractZip(new Uint8Array(zip3)), ArchiveSafetyError);

    const raw = Buffer.from('wave17-deflate');
    const compressed = deflateRawSync(raw);
    const name2 = Buffer.from('ok.txt');
    const local = Buffer.alloc(30 + name2.length + compressed.length);
    local.writeUInt32LE(0x04034b50, 0);
    local.writeUInt16LE(8, 8);
    local.writeUInt32LE(compressed.length, 18);
    local.writeUInt32LE(raw.length, 22);
    local.writeUInt16LE(name2.length, 26);
    name2.copy(local, 30);
    compressed.copy(local, 30 + name2.length);
    const cen = Buffer.alloc(46 + name2.length);
    cen.writeUInt32LE(0x02014b50, 0);
    cen.writeUInt16LE((3 << 8) | 20, 4);
    cen.writeUInt16LE(8, 10);
    cen.writeUInt32LE(compressed.length, 20);
    cen.writeUInt32LE(raw.length, 24);
    cen.writeUInt16LE(name2.length, 28);
    cen.writeUInt32LE((0o100644 << 16) >>> 0, 38);
    cen.writeUInt32LE(0, 42);
    name2.copy(cen, 46);
    const eo = Buffer.alloc(22);
    eo.writeUInt32LE(0x06054b50, 0);
    eo.writeUInt16LE(1, 8);
    eo.writeUInt16LE(1, 10);
    eo.writeUInt32LE(cen.length, 12);
    eo.writeUInt32LE(local.length, 16);
    const good = Buffer.concat([local, cen, eo]);
    const res = extractZip(new Uint8Array(good));
    assert.deepEqual(Buffer.from(res.files['ok.txt']!.data), raw);

    assert.equal(isArchivePathSafe('ok/a.txt'), true);
    assert.equal(isArchivePathSafe('/abs'), false);
  });
});

describe('wave17 — task-kill residual', () => {
  it('shell_id, graceful/interrupt/retry/skip, cleanup, race terminal', async () => {
    const baseTask = {
      id: 't1',
      status: 'running' as const,
      pid: 4242,
      command: 'sleep 99',
      description: '',
      exitCode: null as number | null,
    };

    let r = await taskKillTool.execute!(
      { shell_id: 't1' },
      ctx({
        processManager: {
          get: () => ({ ...baseTask, status: 'completed', exitCode: 0 }),
          kill: () => false,
          removeTask: () => true,
        } as never,
      }),
    );
    assert.ok(!r.isError);
    assert.equal(r.metadata?.reason, 'already_terminal');

    r = await taskKillTool.execute!(
      { task_id: 't1', cleanup: true },
      ctx({
        processManager: {
          get: () => ({ ...baseTask, status: 'killed', exitCode: null }),
          removeTask: () => false,
          taskType: () => 'bash',
          taskKind: () => 'bash',
        } as never,
      }),
    );
    assert.ok(!r.isError);

    r = await taskKillTool.execute!(
      { task_id: 'ag1', graceful: true },
      ctx({
        processManager: {
          get: () => ({ ...baseTask, id: 'ag1', command: '', description: 'agent-a' }),
          taskKind: () => 'agent',
          requestShutdown: () => true,
          waitForIdle: async () => {},
          getStatus: () => 'running',
        } as never,
      }),
    );
    assert.ok(!r.isError);
    assert.equal(r.metadata?.reason, 'shutdown_requested');

    r = await taskKillTool.execute!(
      { task_id: 'ag2', graceful: true },
      ctx({
        processManager: {
          get: () => ({ ...baseTask, id: 'ag2' }),
          taskKind: () => 'agent',
          requestShutdown: () => true,
          waitForIdle: async () => {},
          getStatus: () => 'completed',
        } as never,
      }),
    );
    assert.ok(r.output.includes('gracefully'));

    r = await taskKillTool.execute!(
      { task_id: 'ag3', graceful: true },
      ctx({
        processManager: {
          get: () => ({ ...baseTask, id: 'ag3' }),
          taskKind: () => 'agent',
          requestShutdown: () => false,
        } as never,
      }),
    );
    assert.equal(r.metadata?.reason, 'kill_failed');

    r = await taskKillTool.execute!(
      { task_id: 'ag4', interrupt: true },
      ctx({
        processManager: {
          get: () => ({ ...baseTask, id: 'ag4' }),
          taskKind: () => 'agent',
          interruptAgentTurn: () => true,
        } as never,
      }),
    );
    assert.equal(r.metadata?.reason, 'interrupted');
    r = await taskKillTool.execute!(
      { task_id: 'ag5', interrupt: true },
      ctx({
        processManager: {
          get: () => ({ ...baseTask, id: 'ag5' }),
          taskKind: () => 'agent',
          interruptAgentTurn: () => false,
        } as never,
      }),
    );
    assert.equal(r.metadata?.reason, 'kill_failed');

    r = await taskKillTool.execute!(
      { task_id: 'ag6', retry: true },
      ctx({
        processManager: {
          get: () => ({ ...baseTask, id: 'ag6' }),
          taskKind: () => 'agent',
          retryAgentTask: () => true,
        } as never,
      }),
    );
    assert.equal(r.metadata?.reason, 'retried');
    r = await taskKillTool.execute!(
      { task_id: 'ag7', retry: true },
      ctx({
        processManager: {
          get: () => ({ ...baseTask, id: 'ag7' }),
          taskKind: () => 'agent',
          retryAgentTask: () => false,
        } as never,
      }),
    );
    assert.equal(r.metadata?.reason, 'retry_unsupported');

    r = await taskKillTool.execute!(
      { task_id: 'ag8', skip: true },
      ctx({
        processManager: {
          get: () => ({ ...baseTask, id: 'ag8' }),
          taskKind: () => 'agent',
          skipAgentTask: () => true,
        } as never,
      }),
    );
    assert.equal(r.metadata?.reason, 'skipped');
    r = await taskKillTool.execute!(
      { task_id: 'ag9', skip: true },
      ctx({
        processManager: {
          get: () => ({ ...baseTask, id: 'ag9' }),
          taskKind: () => 'agent',
          skipAgentTask: () => false,
        } as never,
      }),
    );
    assert.equal(r.metadata?.reason, 'skip_unsupported');

    r = await taskKillTool.execute!(
      { task_id: 't', interrupt: true, graceful: true },
      ctx({ processManager: { get: () => baseTask } as never }),
    );
    assert.equal(r.metadata?.reason, 'invalid_input');

    for (const bad of [
      { task_id: 't', cleanup: 'yes' as never },
      { task_id: 't', graceful: 1 as never },
      { task_id: 't', retry: 'x' as never },
      { task_id: 't', skip: 'x' as never },
      { task_id: 't', interrupt: 'x' as never },
    ]) {
      r = await taskKillTool.execute!(bad, ctx({ processManager: { get: () => baseTask } as never }));
      assert.ok(r.isError);
    }

    r = await taskKillTool.execute!(
      { task_id: 't1', cleanup: true },
      ctx({
        processManager: {
          get: () => ({ ...baseTask }),
          kill: () => true,
          taskType: () => 'bash',
          taskKind: () => 'bash',
        } as never,
      }),
    );
    assert.equal(r.metadata?.reason, 'killed');

    r = await taskKillTool.execute!(
      { task_id: 't1' },
      ctx({
        processManager: {
          get: () => ({ ...baseTask }),
          kill: () => false,
          getStatus: () => 'completed',
          taskKind: () => 'bash',
        } as never,
      }),
    );
    assert.equal(r.metadata?.reason, 'already_terminal');

    r = await taskKillTool.execute!(
      { task_id: 't1' },
      ctx({
        processManager: {
          get: () => ({ ...baseTask }),
          kill: () => false,
          getStatus: () => 'running',
        } as never,
      }),
    );
    assert.equal(r.metadata?.reason, 'kill_failed');

    r = await taskKillTool.execute!(
      { prune_idle: true },
      ctx({
        processManager: {
          pruneIdleAgents: () => [],
        } as never,
      }),
    );
    assert.equal(r.metadata?.reason, 'pruned_idle');
    assert.equal(r.metadata?.prunedCount, 0);

    r = await taskKillTool.execute!(
      { task_id: 'sh1', graceful: true },
      ctx({
        processManager: {
          get: () => ({ ...baseTask, id: 'sh1' }),
          taskKind: () => 'bash',
        } as never,
      }),
    );
    assert.equal(r.metadata?.reason, 'shutdown_unsupported');
  });
});

describe('wave17 — mailbox-fs lines 78 + 97', () => {
  it('unlinkSync catch + decodeURIComponent catch', () => {
    const root = mkTmp('mb17-');
    const loc = { teamsRoot: root, teamName: 't17' };
    appendToInbox(loc, 'agent', {
      id: 'm1',
      from: 'x',
      to: 'agent',
      content: 'hi',
      timestamp: Date.now(),
    });
    let unlinkThrew = false;
    _mailboxFsHooks.unlinkSync = () => {
      unlinkThrew = true;
      throw new Error('unlink-race');
    };
    const taken = takeFsInbox(loc, 'agent', new Set());
    assert.ok(taken.length >= 1);
    assert.equal(unlinkThrew, true);
    _mailboxFsHooks.unlinkSync = (p) => {
      fs.unlinkSync(p);
    };

    const dir = inboxDir(loc);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, '%.jsonl'), '');
    fs.writeFileSync(path.join(dir, '%E0%.jsonl'), '');
    fs.writeFileSync(path.join(dir, 'ok.jsonl'), '');
    const peers = listInboxPeers(loc);
    assert.ok(Array.isArray(peers));
    assert.ok(!peers.includes('%'));
    assert.ok(inboxFilePath(loc, 'a@b').includes(encodeURIComponent('a@b')));
  });
});

describe('wave17 — treeify lines 19-21 + separator arms', () => {
  it('forces node-entry Circular via preVisited hook', () => {
    const obj = { a: 1 };
    _treeifyHooks.preVisited = [obj];
    const out = treeify(obj);
    assert.ok(out.includes('[Circular]'));

    const blank = treeify({ ' ': 'v' }, { colorize: () => '' });
    assert.equal(typeof blank, 'string');

    assert.ok(
      treeify({ k: 'v', fn: (() => 1) as never }, { showValues: false, hideFunctions: true }).includes(
        'k',
      ),
    );
    assert.ok(TREE_COLOR_ROLES.includes('key'));
  });
});

describe('wave17 — process-hardening residual', () => {
  it('ptrace applied/failed/error + describe note arms + bunFfiRequire', () => {
    assert.equal(processHardeningDisabled({ [PROCESS_HARDENING_DISABLE_ENV]: 'YES' }), true);

    const applied = hardenProcess({
      platform: 'linux',
      env: {},
      readFileSync: () => '33',
      writeFileSync: () => {},
      prctl: () => 0,
      log: () => {},
    });
    assert.equal(applied.status, 'applied');
    assert.equal(applied.ptrace.status, 'applied');

    assert.equal(
      blockPtrace({ platform: 'linux', env: {}, prctl: () => 1, log: () => {} }).status,
      'failed',
    );
    assert.equal(
      blockPtrace({
        platform: 'linux',
        env: {},
        prctl: () => {
          throw new Error('ffi-boom');
        },
        log: () => {},
      }).status,
      'error',
    );
    assert.equal(
      blockPtrace({
        platform: 'linux',
        env: {},
        prctl: () => {
          throw 'non-error';
        },
        log: () => {},
      }).status,
      'error',
    );

    const failedWrite = hardenProcess({
      platform: 'linux',
      env: {},
      readFileSync: () => '1',
      writeFileSync: () => {
        throw new Error('ro');
      },
      prctl: null,
      log: () => {},
    });
    assert.equal(failedWrite.status, 'failed');

    const unread = hardenProcess({
      platform: 'linux',
      env: {},
      readFileSync: () => {
        throw new Error('enoent');
      },
      prctl: null,
      log: () => {},
    });
    assert.equal(unread.status, 'unsupported');

    assert.equal(loadLibcPrctl({ hasFfi: false }), null);
    assert.equal(
      loadLibcPrctl({
        hasFfi: true,
        ffiRequire: () => {
          throw new Error('no-ffi');
        },
      }),
      null,
    );
    assert.equal(
      loadLibcPrctl({
        hasFfi: true,
        ffiRequire: () => ({
          dlopen: () => ({
            symbols: {
              prctl: () => 0,
            },
          }),
        }),
      })?.(PR_SET_DUMPABLE, 0n),
      0,
    );

    assert.throws(() => bunFfiRequire('bun:ffi'));

    const desc = describeProcessHardening({ platform: 'linux', env: {} });
    assert.ok(desc.supported);
    assert.ok(desc.ptrace.note.length > 0);
    assert.equal(describeProcessHardening({ platform: 'darwin', env: {} }).supported, false);
    assert.ok(COREDUMP_FILTER_PATH.includes('coredump_filter'));
  });
});

describe('wave17 — shell realpathSafe catch + worktree keep-alive', () => {
  it('findPowerShell snap probe + realpathSafe fallback + lock helpers', () => {
    resetPowerShellCache();
    mock.method(shellHooks, 'platform', () => 'linux');
    mock.method(shellHooks, 'execFileSync', () => '/snap/bin/pwsh\n');
    mock.method(shellHooks, 'realpathSync', ((p: fs.PathLike) => {
      if (String(p).includes('snap')) return String(p);
      throw new Error('realpath-fail');
    }) as typeof fs.realpathSync);
    mock.method(shellHooks, 'existsSync', (p: fs.PathLike) => String(p) === '/usr/bin/pwsh');
    const found = findPowerShell();
    assert.ok(found === '/usr/bin/pwsh' || found === '/snap/bin/pwsh' || found === null);

    assert.ok(generateTmuxSessionName('/a/b.c', 'feat/x').length > 0);
    assert.equal(parsePRReference('#42'), 42);
    assert.equal(parsePRReference('nope'), null);

    const d = mkTmp('wt17-');
    const key = path.join(d, 'FETCH_HEAD');
    let ran = false;
    withFetchHeadLock(key, () => {
      ran = true;
      return 7;
    });
    assert.equal(ran, true);
  });
});
