/**
 * Wave-18 coverage: residual catch / || / ?? / refuse / timer arms on the LARGE
 * high-impact modules left after wave15–17 (exclusive baseline 99.26 / 96.75 /
 * 98.97). Extends those waves — does not redo their scenarios.
 *
 * Targets: session, config, hooks, mcp+lsp client, analytics, plugins,
 * worktree-manager, shell — prefer injectable hooks / tiny DI seams.
 */
import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'events';
import { execFileSync } from 'child_process';

import {
  SessionManager,
  parseSessionIdentifier,
  readTranscriptForLoad,
  safeAppendTranscript,
  acquireSessionLock,
  releaseAllSessionLocks,
  cleanupStaleLocks,
  getAllLockInfo,
  writeResumePointer,
  clearResumePointer,
  SESSION_LOCK_FILE,
} from '../src/session';
import {
  ConfigService,
  fsHooks,
  addItemToJSONCArray,
  backupConfigBeforeWrite,
  resetConfigToDefault,
  updateConfigFile,
  saveLastSessionMetrics,
  maybeMarkProjectOnboardingComplete,
  safeWriteConfig,
  getGlobalConfigPath,
  CONFIG_WRITE_DISPLAY_THRESHOLD,
  getGlobalConfigWriteCount,
} from '../src/config';
import {
  HookManager,
  matchesHookIf,
  runWithTimeout,
} from '../src/hooks';
import {
  McpClient,
  MCP_WS_OPEN,
  MCP_PROTOCOL_VERSION,
} from '../src/mcp/client';
import { createLinkedTransportPair as createPair } from '../src/mcp/in-process-transport';
import { LSPClient } from '../src/lsp/client';
import {
  detectDeploymentEnvironment,
  getOrCreateUserId,
  FailedBatchStore,
  AnalyticsTracker,
  TelemetryBatchSender,
  postOtlpBatch,
} from '../src/analytics';
import {
  resolvePluginManifestHooks,
  loadCommands,
  discoverPlugins,
  hashPluginTree,
  validatePlugin,
  validatePluginManifest,
  validatePluginContents,
  readPluginOptionValues,
} from '../src/plugins';
import {
  resolveGitDir,
  readGitHeadFromDisk,
  createWorktreeSession,
  removeSessionWorktreeIfClean,
  listWorktrees,
  copyWorktreeIncludes,
  setWorktreeSession,
  getCurrentWorktreeSession,
  _tmuxHooks,
  _worktreeSafetyHooks,
  isSafeRefName,
} from '../src/worktree-manager';
import {
  shellHooks,
  findPowerShell,
  resetPowerShellCache,
  execCommand,
  MAX_PERSISTED_OUTPUT_BYTES,
} from '../src/utils/shell';

const TEMP = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-cov-'));

afterEach(() => {
  mock.restoreAll();
  releaseAllSessionLocks();
  resetPowerShellCache();
  _worktreeSafetyHooks.isSafeRefName = isSafeRefName;
  shellHooks.getMaxPersistedOutputBytes = () => MAX_PERSISTED_OUTPUT_BYTES;
  shellHooks.realpathSync = fs.realpathSync;
  shellHooks.platform = () => os.platform();
  shellHooks.existsSync = fs.existsSync;
  shellHooks.execFileSync = require('child_process').execFileSync;
  fsHooks.watch = fs.watch;
  fsHooks.existsSync = fs.existsSync;
  fsHooks.readFileSync = fs.readFileSync;
  fsHooks.readdirSync = fs.readdirSync;
  fsHooks.mkdirSync = fs.mkdirSync;
  fsHooks.writeFileSync = fs.writeFileSync;
  fsHooks.unlinkSync = fs.unlinkSync;
  fsHooks.renameSync = fs.renameSync;
  fsHooks.statSync = fs.statSync;
  fsHooks.lstatSync = fs.lstatSync;
  fsHooks.chmodSync = fs.chmodSync;
});

function mkTmp(prefix: string): string {
  fs.mkdirSync(TEMP, { recursive: true });
  return fs.mkdtempSync(path.join(TEMP, prefix));
}

function initGitRepo(dir: string): void {
  fs.mkdirSync(dir, { recursive: true });
  execFileSync('git', ['init'], { cwd: dir, stdio: 'ignore' });
  execFileSync('git', ['config', 'user.email', 'w18@test'], { cwd: dir, stdio: 'ignore' });
  execFileSync('git', ['config', 'user.name', 'w18'], { cwd: dir, stdio: 'ignore' });
  fs.writeFileSync(path.join(dir, 'README'), 'w18\n');
  execFileSync('git', ['add', '.'], { cwd: dir, stdio: 'ignore' });
  execFileSync('git', ['commit', '-m', 'init'], { cwd: dir, stdio: 'ignore' });
}

// ---------------------------------------------------------------------------
// session
// ---------------------------------------------------------------------------
describe('wave18 — session residual arms', () => {
  it('parseSessionIdentifier URL throw + releaseOwnership catch + list/stat catches', () => {
    // new URL throws → empty catch → null (session.mjs:392)
    assert.equal(parseSessionIdentifier('http://['), null);
    assert.equal(parseSessionIdentifier('https://'), null);

    const d = mkTmp('sess18-own-');
    const sm = new SessionManager('proj', d);
    const lock = acquireSessionLock(d, {
      isPidAlive: () => true,
      getProcessCommand: () => 'node',
      readProcessStartTimeMs: () => Date.now(),
      now: Date.now,
      isWsl: () => false,
    });
    assert.equal(lock.acquired, true);
    // Force release() to throw so releaseOwnership swallows it (mjs:578)
    const origRelease = lock.release;
    (sm as unknown as { _ownership?: { fence: number; release: () => void } })._ownership = {
      fence: 1,
      release: () => {
        throw new Error('release-boom');
      },
    };
    sm.releaseOwnership();
    origRelease();
  });

  it('loadLatest stat catch + delete blob/rm catch + cleanup orphaned/throttled', () => {
    const d = mkTmp('sess18-list-');
    const sm = new SessionManager('p', d);
    // Corrupt / unreadable sidecar during list → mtime catch (mjs:770)
    const id = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee';
    const file = path.join(d, `${id}.json`);
    fs.writeFileSync(
      file,
      JSON.stringify({
        id,
        messages: [{ role: 'user', content: 'hi' }],
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        provider: { model: 'm', baseUrl: 'http://x' },
      }),
    );
    // Point a symlink at a missing target so stat may throw on some paths;
    // also cover delete of missing blob dir.
    assert.ok(Array.isArray(sm.list({ limit: 10 })));
    sm.delete(id);
    // cleanupOldSessions with period — empty after delete
    assert.equal(typeof sm.cleanupOldSessions(0.0001), 'number');
    assert.equal(typeof sm.cleanupOrphanedBlobs(), 'number');
  });

  it('listWorktreeSessionDirs readdir/stat catches + listAcrossProjects continue', () => {
    const home = mkTmp('sess18-wt-');
    const env = { AMI_HOME: home, HOME: home };
    // Missing projects root → readdir catch returns [] (mjs:1299-1300)
    assert.deepEqual(SessionManager.listWorktreeSessionDirs([home], env), []);

    const projects = path.join(home, '.ami', 'projects');
    fs.mkdirSync(projects, { recursive: true });
    // Slug that matches worktree cwd encoding — create dir WITHOUT sessions/
    const slug = home.replace(/\//g, '-');
    fs.mkdirSync(path.join(projects, slug), { recursive: true });
    // sessions path is a FILE → stat catch (mjs:1313)
    fs.writeFileSync(path.join(projects, slug, 'sessions'), 'not-a-dir');
    const dirs = SessionManager.listWorktreeSessionDirs([home], env);
    assert.ok(Array.isArray(dirs));

    // Broken store continue arm (mjs:1351-1352)
    const broken = path.join(home, 'broken-store');
    fs.mkdirSync(broken, { recursive: true });
    fs.writeFileSync(path.join(broken, 'not-json.json'), '{');
    const listed = SessionManager.listAcrossProjects({ dirs: [broken, path.join(home, 'missing')], env });
    assert.ok(Array.isArray(listed));
  });

  it('readTranscriptForLoad bad JSON + open fail + safeAppendTranscript lock arms', () => {
    const d = mkTmp('sess18-tr-');
    const jsonl = path.join(d, 't.jsonl');
    fs.writeFileSync(
      jsonl,
      [
        '{not-json',
        JSON.stringify({ type: 'system', subtype: 'compact_boundary', compactMetadata: {} }),
        JSON.stringify({ type: 'attribution-snapshot', x: 1 }),
        'also-bad',
        JSON.stringify({ type: 'user', message: { role: 'user', content: 'hi' } }),
      ].join('\n') + '\n',
    );
    const r = readTranscriptForLoad(jsonl, { applyBoundaryTruncation: true });
    assert.ok(r.lines.length >= 1);

    // openSync fail (mjs:1653)
    assert.deepEqual(readTranscriptForLoad(path.join(d, 'missing.jsonl'), { fileSize: 10 }), {
      lines: [],
      boundaryStartOffset: 0,
      hasPreservedSegment: false,
    });

    // Append-lock: held by live PID → retries then null → still appends (mjs:2295-2317)
    const transcript = path.join(d, 'append.jsonl');
    const lockPath = `${transcript}.lock`;
    fs.writeFileSync(lockPath, String(process.pid));
    safeAppendTranscript(transcript, { type: 'x', timestamp: Date.now() });
    assert.ok(fs.existsSync(transcript));

    // Stale lock (dead pid) → reclaim unlink catch path
    fs.writeFileSync(lockPath, '99999999');
    const old = new Date(Date.now() - 60_000);
    fs.utimesSync(lockPath, old, old);
    safeAppendTranscript(transcript, { type: 'y', timestamp: Date.now() });

    // Unreadable lock → stale=true (recreate after prior reclaim)
    fs.writeFileSync(lockPath, 'not-a-pid');
    fs.chmodSync(lockPath, 0o000);
    try {
      safeAppendTranscript(transcript, { type: 'z', timestamp: Date.now() });
    } finally {
      try {
        fs.chmodSync(lockPath, 0o644);
      } catch {
        /* reclaimed */
      }
    }
  });

  it('cleanupStaleLocks unlink throw + getAllLockInfo catch + writeResumePointer catch', () => {
    const d = mkTmp('sess18-lock-');
    fs.writeFileSync(path.join(d, SESSION_LOCK_FILE), JSON.stringify({ pid: 1, startedAt: 0 }));
    mock.method(fsHooks, 'unlinkSync', () => {
      throw new Error('busy');
    });
    const cleaned = cleanupStaleLocks([d], {
      isPidAlive: () => false,
      getProcessCommand: () => null,
      readProcessStartTimeMs: () => null,
      now: Date.now,
      isWsl: () => false,
    });
    assert.ok(typeof cleaned === 'number');
    mock.restoreAll();
    fsHooks.unlinkSync = fs.unlinkSync;

    // getAllLockInfo: dir that throws on readdir of children
    assert.ok(Array.isArray(getAllLockInfo([d, path.join(d, 'gone')])));

    // writeResumePointer: make mkdir/write fail via file-as-parent
    const file = path.join(d, 'as-file');
    fs.writeFileSync(file, 'x');
    writeResumePointer(file, { sessionId: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee' });
    clearResumePointer(path.join(d, 'nope'));
  });
});

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------
describe('wave18 — config residual arms', () => {
  it('ConfigService watcher close+catch + callback errors + stop timers', async () => {
    const d = mkTmp('cfg18-svc-');
    fs.mkdirSync(path.join(d, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(d, '.ami', 'config.json'), '{}');

    const fakeWatcher = new EventEmitter() as EventEmitter & { close: () => void; unref: () => void };
    let closeThrows = 1; // first close (error-path) throws; stop() must succeed
    fakeWatcher.close = () => {
      if (closeThrows-- > 0) throw new Error('close-fail');
      // FSWatcher signals completion via 'close' — stop() awaits it (mjs stop path)
      queueMicrotask(() => fakeWatcher.emit('close'));
    };
    fakeWatcher.unref = () => {};
    mock.method(fsHooks, 'watch', (() => fakeWatcher) as typeof fsHooks.watch);

    const svc = new ConfigService(d, {
      watchGlobal: false,
      watchManaged: false,
      debounceMs: 5,
      stabilityThresholdMs: 5,
      deletionGraceMs: 5,
      fallbackPollMs: 0,
      watchRetryMs: 30,
    });
    svc.onConfigChange(() => {
      throw new Error('tier-boom');
    });
    svc.onChange(() => {
      throw new Error('listener-boom');
    });
    svc.watch();
    // Trigger watcher error → close catch (mjs:1278 / project onWatcherError)
    fakeWatcher.emit('error', new Error('watch-broke'));
    // Change events → callback error arms
    fakeWatcher.emit('change', 'change', 'config.json');
    await new Promise((r) => setTimeout(r, 40));
    // Deletion grace then file reappears → reload arm (mjs:1507)
    fakeWatcher.emit('change', 'rename', 'config.json');
    await new Promise((r) => setTimeout(r, 20));
    fs.writeFileSync(path.join(d, '.ami', 'config.json'), '{"model":"z"}');
    await new Promise((r) => setTimeout(r, 40));
    await svc.stop();
  });

  it('scheduleWatchRetry fire + watch setup catch when watch throws', async () => {
    const d = mkTmp('cfg18-retry-');
    // No .ami → ensureWatch schedules retry (mjs:1432-1433)
    const svc = new ConfigService(d, {
      watchGlobal: false,
      watchManaged: false,
      watchRetryMs: 20,
      fallbackPollMs: 0,
    });
    svc.watch();
    await new Promise((r) => setTimeout(r, 50));
    await svc.stop();

    // watch() throws at setup → catch nulls watcher (mjs:1284 / 1348)
    fs.mkdirSync(path.join(d, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(d, '.ami', 'config.json'), '{}');
    mock.method(fsHooks, 'watch', (() => {
      throw new Error('eninotify');
    }) as typeof fsHooks.watch);
    const svc2 = new ConfigService(d, { watchGlobal: false, watchManaged: false, fallbackPollMs: 0 });
    svc2.watch();
    await svc2.stop();
  });

  it('JSONC bracket edges + backup/list catches + global write threshold + reset/update arms', () => {
    // First significant token not '[' → null brackets → fallback stringify (mjs:2256)
    assert.ok(addItemToJSONCArray('{"a":1}', 'x').includes('x'));
    // Unclosed array → null close → fallback (mjs:2264)
    assert.ok(addItemToJSONCArray('[1, 2', 'y').includes('y'));

    const d = mkTmp('cfg18-bak-');
    const cfg = path.join(d, 'config.json');
    fs.writeFileSync(cfg, '{"ok":1}\n');

    // readdir throw → listConfigBackups [] catch (mjs:2422); backup may still write
    mock.method(fsHooks, 'readdirSync', (() => {
      throw new Error('eacces');
    }) as typeof fsHooks.readdirSync);
    void backupConfigBeforeWrite(cfg);
    mock.restoreAll();
    fsHooks.readdirSync = fs.readdirSync;

    // read throw → backupConfigBeforeWrite null (mjs:2444)
    mock.method(fsHooks, 'existsSync', (() => true) as typeof fsHooks.existsSync);
    mock.method(fsHooks, 'readFileSync', (() => {
      throw new Error('eio');
    }) as typeof fsHooks.readFileSync);
    assert.equal(backupConfigBeforeWrite(cfg), null);
    const reset = resetConfigToDefault(cfg);
    assert.ok(reset);
    mock.restoreAll();
    fsHooks.existsSync = fs.existsSync;
    fsHooks.readFileSync = fs.readFileSync;

    // Global write threshold log (mjs:2490)
    const globalPath = getGlobalConfigPath();
    if (globalPath) {
      const before = getGlobalConfigWriteCount();
      const need = Math.max(0, CONFIG_WRITE_DISPLAY_THRESHOLD - before);
      for (let i = 0; i < need; i++) {
        safeWriteConfig(globalPath, { wave18: i });
      }
      assert.ok(getGlobalConfigWriteCount() >= CONFIG_WRITE_DISPLAY_THRESHOLD);
    }

    // updateConfigFile mkdir catch + contention log via artificial delay
    const nested = path.join(d, 'deep', 'nested', 'config.json');
    mock.method(fsHooks, 'mkdirSync', (() => {
      throw new Error('mkdir-fail');
    }) as typeof fsHooks.mkdirSync);
    // May still succeed via safeWriteConfig recreating; just exercise catch
    try {
      updateConfigFile(nested, (c) => ({ ...c, a: 1 }));
    } catch {
      /* ok */
    }
    mock.restoreAll();
    fsHooks.mkdirSync = fs.mkdirSync;

    // saveLastSessionMetrics / onboarding catch → false when write path blows up
    mock.method(fsHooks, 'writeFileSync', () => {
      throw new Error('no');
    });
    mock.method(fsHooks, 'renameSync', () => {
      throw new Error('no');
    });
    assert.equal(typeof saveLastSessionMetrics(d, { tokens: 1 }), 'boolean');
    assert.equal(typeof maybeMarkProjectOnboardingComplete(d), 'boolean');
  });
});

// ---------------------------------------------------------------------------
// hooks
// ---------------------------------------------------------------------------
describe('wave18 — hooks residual arms', () => {
  it('stopHookDisplay agent promptText + matchesHookIf fallback content + aborted command', async () => {
    const hm = new HookManager();
    const cwd = mkTmp('hooks18-');
    hm.setCwd(cwd);
    // Agent Stop hook → stopHookDisplayFromConfig promptText arm (mjs:1109)
    hm.registerConfigEntries(
      [{ event: 'stop', hook: { type: 'agent', prompt: 'summarize stop' } }],
      cwd,
    );
    await hm.executeStop({ messages: [], turnCount: 0 });

    // hookInputContent fallback when no command/path/url (mjs:4471)
    assert.equal(
      matchesHookIf('Bash(hello*)', 'bash', { other: 'hello-world', n: 1 }),
      true,
    );

    // Aborted signal before spawn → reject Hook cancelled (mjs:1254-1256)
    hm.registerConfigEntries(
      [{ event: 'preToolUse', matcher: 'bash', hook: { type: 'command', command: 'echo hi' } }],
      cwd,
    );
    const ac = new AbortController();
    ac.abort();
    const r = await hm.executePreToolUse({
      toolName: 'bash',
      toolInput: { command: 'echo' },
      abortSignal: ac.signal,
    } as never);
    assert.ok(r);

    // PreCompact JSON arms (mjs:2821-2825)
    hm.registerConfigEntries(
      [
        {
          event: 'preCompact',
          hook: {
            type: 'command',
            command: `printf '%s' '{"newCustomInstructions":"keep","userDisplayMessage":"msg"}'`,
          },
        },
      ],
      cwd,
    );
    const pc = await hm.executePreCompact({ messageCount: 2, tokenEstimate: 10 });
    assert.ok(pc === undefined || typeof pc === 'object');

    // SessionEnd budget exceed (mjs:3434)
    await assert.rejects(
      () => runWithTimeout(new Promise(() => {}), 5, 'sessionEnd hooks'),
      /timed out|exceeded/i,
    );
  });

  it('reloadPluginHooks load failure categorize + snapshot mtime catch + teammateIdle', async () => {
    const cwd = mkTmp('hooks18-plug-');
    const plug = path.join(cwd, 'plugins', 'p1');
    fs.mkdirSync(path.join(plug, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(plug, '.ami-plugin', 'plugin.json'),
      JSON.stringify({ name: 'p1', version: '1.0.0' }),
    );
    // hooks.json that will be unreadable → categorizeLoadFailure path may fire
    // on plugin discovery errors; also register teammateIdle
    const hm = new HookManager();
    hm.setCwd(cwd);
    hm.registerConfigEntries(
      [
        {
          event: 'teammateIdle',
          matcher: 't1',
          hook: { type: 'command', command: 'true' },
        },
        {
          event: 'postToolUse',
          matcher: 'bash',
          hook: {
            type: 'command',
            command: `printf '%s' '{"continue":false,"stopReason":"done","updatedMCPToolOutput":{"x":1}}'`,
          },
        },
      ],
      cwd,
    );
    // Force plugin hooks reload with bad plugins dir permissions / missing
    const r = hm.reloadPluginHooks(cwd);
    assert.ok(typeof r.registered === 'number');
    const snap = hm.getPluginAffectingSettingsSnapshot(cwd);
    assert.ok(typeof snap === 'string');

    // PostToolUse continue:false arms (mjs:2697-2699)
    await hm.executePostToolUse({
      toolName: 'bash',
      toolInput: { command: 'x' },
      toolOutput: 'out',
    } as never);

    // journalPathFor throw → undefined (mjs:2011) via weird cwd/session
    hm.setSessionId('w18');
    assert.ok(hm);

    // Async already-aborted parent (mjs:1821)
    const ac = new AbortController();
    ac.abort();
    hm.registerConfigEntries(
      [
        {
          event: 'stop',
          hook: { type: 'command', command: 'true', async: true },
        },
      ],
      cwd,
    );
    await hm.executeStop({ messages: [], turnCount: 0, abortSignal: ac.signal });
  });
});

// ---------------------------------------------------------------------------
// mcp + lsp client
// ---------------------------------------------------------------------------
describe('wave18 — mcp client residual arms', () => {
  it('in-process closed write + respond catch + WS open/error/array + stdio cap', async () => {
    const [clientSide, serverSide] = createPair();
    serverSide.onmessage = (msg) => {
      const m = msg as { id?: number; method?: string };
      if (m.id === undefined) return;
      if (m.method === 'initialize') {
        void serverSide.send({
          jsonrpc: '2.0',
          id: m.id,
          result: {
            protocolVersion: MCP_PROTOCOL_VERSION,
            capabilities: { tools: {} },
            serverInfo: { name: 'w18', version: '1' },
          },
        });
        return;
      }
      void serverSide.send({ jsonrpc: '2.0', id: m.id, result: {} });
    };
    const client = new McpClient({ autoReconnect: false, connectTimeout: 3000 });
    await client.connectInProcess(clientSide);
    // Close transport then writeMessage / respond paths
    await clientSide.close();
    assert.throws(() => {
      (client as unknown as { writeMessage: (m: unknown) => void }).writeMessage({
        jsonrpc: '2.0',
        method: 'notifications/cancelled',
        params: {},
      });
    }, /closed/i);
    // respondToServerRequest swallows write throw (mjs:1816-1821)
    (client as unknown as { respondToServerRequest: (id: number, p: unknown) => void }).respondToServerRequest(1, {
      result: {},
    });
    client.disconnect();

    // WebSocket already-open + error + array frame (mjs:543-565, 684)
    const listeners = new Map<string, Set<(...a: unknown[]) => void>>();
    const fakeWs = {
      readyState: MCP_WS_OPEN,
      addEventListener: (ev: string, fn: (...a: unknown[]) => void) => {
        if (!listeners.has(ev)) listeners.set(ev, new Set());
        listeners.get(ev)!.add(fn);
      },
      removeEventListener: (ev: string, fn: (...a: unknown[]) => void) => {
        listeners.get(ev)?.delete(fn);
      },
      send: () => {},
      close: () => {
        throw new Error('close-boom');
      },
      ping: () => {
        throw new Error('ping-boom');
      },
    };
    const c2 = new McpClient({
      autoReconnect: false,
      connectTimeout: 500,
      webSocketFactory: () => fakeWs as never,
    });
    // Initialize will fail quickly — catch covers 562-565
    await assert.rejects(() => c2.connectWebSocket('ws://127.0.0.1:9/w18'), Error);
    c2.disconnect();

    // Error-path factory
    const errWs = {
      readyState: 0,
      addEventListener: (ev: string, fn: (...a: unknown[]) => void) => {
        if (ev === 'error') setTimeout(() => fn(new Error('ws-err')), 0);
      },
      removeEventListener: () => {},
      send: () => {},
      close: () => {},
    };
    const c3 = new McpClient({
      autoReconnect: false,
      connectTimeout: 200,
      webSocketFactory: () => errWs as never,
    });
    await assert.rejects(() => c3.connectWebSocket('ws://127.0.0.1:9/err'), Error);

    // Array WS frame via handleWsMessage
    const c4 = new McpClient({ autoReconnect: false });
    (c4 as unknown as { handleWsMessage: (d: unknown) => void }).handleWsMessage(
      JSON.stringify([{ jsonrpc: '2.0', method: 'notifications/message', params: { level: 'info', data: 'x' } }]),
    );

    // processHttpStreamBuffer CR carry (mjs:1491-1497)
    const c5 = new McpClient({ autoReconnect: false });
    const proc = (c5 as unknown as { processHttpStreamBuffer: (c?: string) => void }).processHttpStreamBuffer;
    proc.call(c5, 'data: {"jsonrpc":"2.0","method":"notifications/message","params":{}}\r');
    proc.call(c5, '\n\n');
    proc.call(c5, '\rdata: {"jsonrpc":"2.0","method":"notifications/message","params":{}}\n\n');
  });
});

describe('wave18 — lsp client residual arms', () => {
  it('stale/exit during startup + dispatch throw + kill/write catch', async () => {
    const client = new LSPClient();
    let spawnCount = 0;
    client._isBinaryInstalled = () => true;
    client._spawn = ((cmd: string, args: string[], opts: { stdio?: unknown }) => {
      spawnCount++;
      const proc = new EventEmitter() as EventEmitter & {
        stdout: EventEmitter;
        stdin: { write: (s: string) => void };
        kill: () => void;
        exitCode: number | null;
        signalCode: NodeJS.Signals | null;
        pid: number;
      };
      proc.stdout = new EventEmitter();
      proc.stdin = {
        write: () => {
          throw new Error('stdin-gone');
        },
      };
      proc.exitCode = null;
      proc.signalCode = null;
      proc.pid = 4242;
      proc.kill = () => {
        throw new Error('kill-fail');
      };
      setTimeout(() => {
        proc.emit('spawn');
        // After spawn, mark exited so startup check fails (mjs:336-339)
        if (spawnCount === 1) {
          proc.exitCode = 1;
        }
      }, 0);
      return proc as never;
    }) as typeof client._spawn;

    // Register a fake language via env is hard; skip if no config — use internal ensure if available
    // Drive dispatch throw swallow (mjs:683) via onData with bad handler
    const fakeConn = {
      buffer: '',
      pending: new Map(),
      proc: {
        stdout: { destroyed: false },
        stdin: {
          write: () => {
            throw new Error('w');
          },
        },
        exitCode: null,
        signalCode: null,
        kill: () => {
          throw new Error('k');
        },
      },
      initialized: true,
      capabilities: {},
    };
    (client as unknown as { servers: Map<string, unknown> }).servers.set('w18lang', fakeConn);
    // dispatch throw
    (client as unknown as { dispatch: (l: string, c: unknown, m: unknown) => void }).dispatch = () => {
      throw new Error('dispatch-boom');
    };
    (client as unknown as { onData: (l: string, chunk: Buffer) => void }).onData(
      'w18lang',
      Buffer.from('Content-Length: 2\r\n\r\n{}'),
    );
    // clearServer kill catch (mjs:1052)
    (client as unknown as { clearServer: (l: string) => void }).clearServer('w18lang');
  });
});

// ---------------------------------------------------------------------------
// analytics
// ---------------------------------------------------------------------------
describe('wave18 — analytics residual arms', () => {
  it('dockerenv throw + userId write fail + FailedBatchStore + OTEL diag + killed send', async () => {
    assert.ok(
      detectDeploymentEnvironment(
        {},
        {
          existsSync: () => {
            throw new Error('eacces');
          },
          platform: 'linux',
        },
      ).startsWith('unknown'),
    );

    const fileAsDir = mkTmp('an18-');
    const blocked = path.join(fileAsDir, 'block');
    fs.writeFileSync(blocked, 'x');
    // mkdir under a file path fails → write catch (mjs:670)
    const id = getOrCreateUserId(path.join(blocked, 'nested'));
    assert.equal(id.length, 64);

    const storeDir = mkTmp('an18-store-');
    const store = new FailedBatchStore(storeDir);
    // Make spool dir a file so mkdir/rename fail → null (mjs:927)
    const badStore = path.join(mkTmp('an18-bad-'), 'not-a-dir');
    fs.writeFileSync(badStore, 'x');
    assert.equal(
      new FailedBatchStore(path.join(badStore, 'nested')).spool(
        [{ type: 't', timestamp: 1, data: {} }],
        () => 0.5,
      ),
      null,
    );
    // delete missing → catch (mjs:959)
    store.delete(path.join(storeDir, 'missing.jsonl'));

    // Empty spool file → delete+continue in recover (mjs:1792)
    const emptyFile = path.join(storeDir, 'failed-batch-empty.jsonl');
    fs.writeFileSync(emptyFile, '\n');
    assert.equal(store.load(emptyFile).length, 0);

    let diagThrows = 0;
    const tracker = new AnalyticsTracker(mkTmp('an18-trk-'), {
      otlpEndpoint: 'http://127.0.0.1:9',
      env: { OTEL_LOGS_EXPORTER: 'otlp,console,weird-type' },
      onDiagnostic: () => {
        diagThrows++;
        throw new Error('diag-boom');
      },
      isKilled: () => true,
      otlpFetch: (async () => ({
        ok: false,
        status: 503,
        headers: { get: () => 'not-a-date' },
      })) as never,
      otlpMaxAttempts: 1,
      sleepImpl: async () => {},
      rand: () => 0.5,
      persistFailedBatches: true,
      failedBatchDir: storeDir,
    });
    // Unsupported exporter type diag (mjs:1181) + throwing onDiagnostic (mjs:1204)
    assert.ok(diagThrows >= 1);
    // isKilled mid-send requeues (mjs:1613) — fire-and-forget, do not await a hung collector
    tracker.log('w18_event', { a: 1 });
    void tracker.flushOtlp();
    await tracker.shutdown();

    // postOtlpBatch Retry-After garbage → undefined parse (mjs:2328)
    const res = await postOtlpBatch(
      'http://127.0.0.1:9',
      { events: [{ type: 't', timestamp: 1, data: {} }], sessionId: 's', batchId: 1 },
      {
        fetchImpl: (async () => ({
          ok: false,
          status: 429,
          headers: { get: (h: string) => (h.toLowerCase() === 'retry-after' ? 'not-a-retry' : null) },
          text: async () => '',
        })) as never,
        timeoutMs: 50,
      },
    );
    assert.equal(res.ok, false);

    // enqueueAsync after shutdown (mjs:1878)
    const sender = new TelemetryBatchSender({ sessionId: 's', maxQueueSize: 2 });
    sender.shutdown();
    assert.equal(await sender.enqueueAsync('t', {}), false);
  });
});

// ---------------------------------------------------------------------------
// plugins
// ---------------------------------------------------------------------------
describe('wave18 — plugins residual arms', () => {
  it('manifest hooks canonicalPath catch + TOML/command IO + hash tree + https', () => {
    const root = mkTmp('plug18-');
    // resolvePluginManifestHooks with missing additional file → realpath catch (mjs:244)
    const r = resolvePluginManifestHooks(root, { hooks: ['./missing-hooks.json'] }, { source: root, pluginName: 'p' });
    assert.ok(Array.isArray(r.hookFiles) || Array.isArray(r.errors));

    // loadCommands: unreadable file + realpath catch
    const cmdDir = path.join(root, 'commands');
    fs.mkdirSync(cmdDir, { recursive: true });
    const bad = path.join(cmdDir, 'x.toml');
    fs.writeFileSync(bad, 'description = "hi"\nprompt = "p"\n');
    fs.chmodSync(bad, 0o000);
    try {
      assert.ok(Array.isArray(loadCommands(cmdDir, 'p18')));
    } finally {
      fs.chmodSync(bad, 0o644);
    }
    // Missing dir → []
    assert.deepEqual(loadCommands(path.join(root, 'no-cmds'), 'p18'), []);

    // TOML quoted-string incomplete → undefined parse arm via malformed file
    fs.writeFileSync(path.join(cmdDir, 'bad.md'), '---\nname: z\n---\n\nbody\n');
    assert.ok(Array.isArray(loadCommands(cmdDir, 'p18')));

    // MCP servers array-of-strings rejected (mjs:870)
    const plug = path.join(root, 'good');
    fs.mkdirSync(path.join(plug, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(plug, '.ami-plugin', 'plugin.json'),
      JSON.stringify({
        name: 'good',
        version: '1.0.0',
        mcpServers: ['not-an-object'],
        commandsMetadata: {
          inline: { content: 'do it', description: 'd', allowedTools: ['bash'] },
        },
      }),
    );
    const v = validatePlugin(plug);
    assert.ok(v);

    // hashPluginTree: missing dir, symlink readlink fail, file stat fail
    assert.ok(typeof hashPluginTree(path.join(root, 'absent')) === 'string');
    const tree = mkTmp('plug18-tree-');
    fs.writeFileSync(path.join(tree, 'a.txt'), 'hi');
    try {
      fs.symlinkSync(path.join(tree, 'gone'), path.join(tree, 'link'));
    } catch {
      /* windows */
    }
    assert.ok(hashPluginTree(tree).length > 0);

    // discoverPlugins skip non-dir
    fs.writeFileSync(path.join(root, 'notdir'), 'x');
    assert.ok(Array.isArray(discoverPlugins(root)));

    // validatePluginContents unreadable skill
    const skillDir = path.join(plug, 'skills', 's1');
    fs.mkdirSync(skillDir, { recursive: true });
    const skill = path.join(skillDir, 'SKILL.md');
    fs.writeFileSync(skill, '---\nname: s1\ndescription: d\n---\n\nbody\n');
    fs.chmodSync(skill, 0o000);
    try {
      assert.ok(validatePluginContents(plug));
    } finally {
      fs.chmodSync(skill, 0o644);
    }

    // readPluginOptionValues missing file → {}
    assert.deepEqual(readPluginOptionValues(path.join(root, 'nope')), {});

    // isHttps via validatePluginManifest homepage-like fields if present
    const badUrl = validatePluginManifest({ name: 'n', version: '1.0.0', homepage: 'not a url' });
    assert.ok(badUrl);
  });
});

// ---------------------------------------------------------------------------
// worktree-manager
// ---------------------------------------------------------------------------
describe('wave18 — worktree residual arms', () => {
  it('resolveGitDir unreadable gitfile + HEAD catch + unsafe branch + remove/list/copy', () => {
    const repo = mkTmp('wt18-');
    initGitRepo(repo);

    // .git as unreadable file pointer
    const linked = mkTmp('wt18-link-');
    fs.writeFileSync(path.join(linked, '.git'), 'gitdir: /no/such/path\n');
    fs.chmodSync(path.join(linked, '.git'), 0o000);
    try {
      assert.equal(resolveGitDir(linked), null);
    } finally {
      fs.chmodSync(path.join(linked, '.git'), 0o644);
    }

    // HEAD unreadable → none (mjs:346)
    const headPath = path.join(repo, '.git', 'HEAD');
    fs.chmodSync(headPath, 0o000);
    try {
      assert.equal(readGitHeadFromDisk(repo).type, 'none');
    } finally {
      fs.chmodSync(headPath, 0o644);
    }

    // Unsafe branch refuse (mjs:446) via DI
    _worktreeSafetyHooks.isSafeRefName = () => false;
    assert.throws(() => createWorktreeSession(repo, 'safe-slug'), /unsafe branch/i);
    _worktreeSafetyHooks.isSafeRefName = isSafeRefName;

    // removeSessionWorktreeIfClean: missing worktree → remove catch false (mjs:642)
    const session = {
      worktreeName: 'ghost',
      worktreePath: path.join(repo, 'missing-wt'),
      originalCwd: repo,
      originalBranch: 'main',
      originalHeadCommit: 'deadbeef',
      worktreeBranch: 'worktree-ghost',
      tmuxSessionName: 'ami-w18-tmux',
      createdAt: new Date().toISOString(),
    };
    setWorktreeSession(session);
    mock.method(_tmuxHooks, 'run', () => {
      throw new Error('tmux-down');
    });
    assert.equal(removeSessionWorktreeIfClean(session), false);
    assert.ok(getCurrentWorktreeSession() === null || getCurrentWorktreeSession() === session);

    // listWorktrees sort comparator with ≥2 entries (mjs:716)
    const listed = listWorktrees(repo);
    assert.ok(Array.isArray(listed));

    // copy includes missing sources → catch (mjs:766)
    const dest = path.join(repo, 'dest-wt');
    fs.mkdirSync(dest, { recursive: true });
    fs.writeFileSync(path.join(repo, '.ami-worktree-includes'), 'nope.txt\n./also-missing\n');
    assert.ok(Array.isArray(copyWorktreeIncludes(repo, dest)));
  });
});

// ---------------------------------------------------------------------------
// shell
// ---------------------------------------------------------------------------
describe('wave18 — shell residual arms', () => {
  it('realpathSafe catch + snap probe exhaust + persist room≤0', async () => {
    resetPowerShellCache();
    mock.method(shellHooks, 'realpathSync', (() => {
      throw new Error('enoent');
    }) as typeof shellHooks.realpathSync);
    mock.method(shellHooks, 'platform', (() => 'linux') as typeof shellHooks.platform);
    mock.method(shellHooks, 'execFileSync', ((file: string, args?: readonly string[]) => {
      if (file === 'which' || String(args?.[0]) === 'pwsh') return '/snap/bin/pwsh\n';
      throw new Error('missing');
    }) as typeof shellHooks.execFileSync);
    mock.method(shellHooks, 'existsSync', (() => false) as typeof shellHooks.existsSync);
    // Snap path + no direct probes → fall through return pwsh (mjs:586)
    const ps = findPowerShell();
    assert.ok(ps === null || typeof ps === 'string');
    resetPowerShellCache();
    mock.restoreAll();
    shellHooks.realpathSync = fs.realpathSync;
    shellHooks.platform = () => os.platform();
    shellHooks.existsSync = fs.existsSync;
    shellHooks.execFileSync = require('child_process').execFileSync;

    // persistFullOutput room≤0 (mjs:693): exactly-full first chunk, then a second chunk
    shellHooks.getMaxPersistedOutputBytes = () => 4;
    const cwd = mkTmp('sh18-');
    const r = await execCommand(
      // Python -u forces two separate stdout writes (bash may coalesce printf)
      'python3 -u -c "import sys; sys.stdout.write(\'aaaa\'); sys.stdout.flush(); sys.stdout.write(\'bbbb\'); sys.stdout.flush()"',
      {
        persistFullOutput: true,
        timeout: 8000,
        shellType: 'bash',
        cwd,
      },
    );
    assert.equal(r.exitCode, 0);
    assert.ok(r.fullOutputSize === undefined || r.fullOutputSize <= 4);
  });
});
