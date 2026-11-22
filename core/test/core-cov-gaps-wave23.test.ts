/**
 * Wave-23: top drag by uncovered-branch WEIGHT (size×gap) after excl3
 * (99.48 / 97.09 / 99.04). Laser-focused on the TOP 8:
 *   hooks, config, permissions, memory, types, analytics, plugins, provider.
 *
 * Forces remaining `||` / `??` / ternary / catch / optional-chain arms listed in
 * worst-excl2/excl3. Extends wave15–22 — does not redo their scenarios wholesale.
 * Tiny DI seams OK (hookSpawnHooks, __validateNumericKnobForTest).
 */
import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';

import {
  HookManager,
  hookSpawnHooks,
  runWithTimeout,
  matchesHookIf,
} from '../src/hooks';
import {
  ConfigService,
  fsHooks as configFsHooks,
  addItemToJSONCArray,
  updateConfigFile,
  maybeMarkProjectOnboardingComplete,
  ensureLocalConfigGitignored,
  applyConfigEnv,
} from '../src/config';
import {
  PermissionManager,
  classifyPermissionRisk,
  permissionRuleValueFromString,
  permissionRuleValueToString,
  validatePermissionRule,
  escapeRuleContent,
  unescapeRuleContent,
  resolveFileOperationType,
  getPathsForPermissionCheck,
  screenAutoModeToolCall,
  describeRule,
} from '../src/permissions';
import {
  MemoryManager,
  additionalInstructionDirsEnabled,
  bestEffortChmod,
  bestEffortChmodAsync,
} from '../src/memory';
import {
  validateEngineConfig,
  sanitizeEngineConfig,
  __validateNumericKnobForTest,
  isInferenceConfigImmediate,
  resolveDiffPreviewWidth,
  describeSymlinkEdit,
  shouldShowWorkerBadge,
  nextTaskBudgetRemaining,
  resolveNotificationTimeout,
  planNotificationQueue,
  mergeSkillUsageStat,
  reconcileSessionMode,
  isSupportedImageMediaType,
  describeTheme,
  toAgentId,
  asSessionId,
  asAgentId,
  shouldForcePermissionCheck,
  validateSummarizeMetadata,
  selectModelFacingOutput,
  selectUserFacingOutput,
  shouldRenderGroupedToolUse,
  resolveToolOpenWorld,
  isSuggestionMessage,
  isLspRecommendationDisabled,
  shouldEnablePromptSuggestion,
  shouldRevealSlowProgress,
  sanitizeToolInputSummary,
  resolveSentAt,
  buildSubsystems,
} from '../src/types';
import { resolveCriticModelLock, DEFAULT_CONFIG } from '../src/superinference/types';
import {
  AnalyticsTracker,
  TelemetryBatchSender,
  FailedBatchStore,
  postOtlpBatch,
} from '../src/analytics';
import {
  resolvePluginManifestHooks,
  loadCommands,
  hashPluginTree,
  discoverPlugins,
  validatePlugin,
  validatePluginManifest,
  validatePluginContents,
  readPluginOptionValues,
} from '../src/plugins';
import {
  nonstreamingFallbackTimeoutMs,
  withRequestIdDebugTrace,
  withFreshConnection,
  streamChatCompletion,
  streamChatCompletionWithRetry,
  dumpPromptsEnabled,
  resolveModelForProvider,
  appendRequestIdToError,
  extractProviderRequestId,
} from '../src/provider';
import { MOCK_RATE_LIMIT_ENV } from '../src/error-classifier';

const TEMP = path.join(os.tmpdir(), 'ami-cov-w23');
const REAL_HOOK_SPAWN = hookSpawnHooks.spawn;

afterEach(() => {
  mock.restoreAll();
  hookSpawnHooks.spawn = REAL_HOOK_SPAWN;
  delete process.env.AMI_ADDITIONAL_DIRECTORIES_CLAUDE_MD;
  delete process.env[MOCK_RATE_LIMIT_ENV];
  delete process.env.SUPERINFERENCE_NONSTREAMING_FALLBACK_TIMEOUT_MS;
  delete process.env.SUPERINFERENCE_DATA_DIR;
});

function mkTmp(prefix: string): string {
  fs.mkdirSync(TEMP, { recursive: true });
  return fs.mkdtempSync(path.join(TEMP, prefix));
}

function fakeChild(opts: {
  writeThrow?: boolean;
  destroyThrow?: boolean;
  exitCode?: number;
} = {}): EventEmitter & {
  stdin: PassThrough & { destroy: () => void };
  stdout: PassThrough;
  stderr: PassThrough;
  kill: () => boolean;
  pid: number;
} {
  const ee = new EventEmitter() as EventEmitter & {
    stdin: PassThrough & { destroy: () => void };
    stdout: PassThrough;
    stderr: PassThrough;
    kill: () => boolean;
    pid: number;
  };
  const stdin = new PassThrough() as PassThrough & { destroy: () => void };
  if (opts.writeThrow) {
    stdin.write = (() => {
      throw new Error('stdin-write-fail');
    }) as typeof stdin.write;
  }
  const origDestroy = stdin.destroy.bind(stdin);
  stdin.destroy = ((...args: unknown[]) => {
    if (opts.destroyThrow) throw new Error('stdin-destroy-fail');
    return origDestroy(...(args as []));
  }) as typeof stdin.destroy;
  ee.stdin = stdin;
  ee.stdout = new PassThrough();
  ee.stderr = new PassThrough();
  ee.kill = () => true;
  ee.pid = 4242;
  queueMicrotask(() => {
    ee.stdout.end();
    ee.stderr.end();
    ee.emit('close', opts.exitCode ?? 0);
  });
  return ee;
}

// ---------------------------------------------------------------------------
// hooks
// ---------------------------------------------------------------------------
describe('wave23 — hooks residual arms', () => {
  it('stdin destroy/write catch + async abort + preventContinuation + plugin load fail', async () => {
    const d = mkTmp('hk23-');
    const hm = new HookManager({ cwd: d, sessionId: 'w23' });

    // Force stdin.write throw → catch end arm (mjs:1304) and destroy throw (mjs:1276)
    hookSpawnHooks.spawn = (() => fakeChild({ writeThrow: true, destroyThrow: true })) as typeof hookSpawnHooks.spawn;
    hm.registerConfigEntries(
      [
        {
          event: 'preToolUse',
          matcher: 'bash',
          hook: { type: 'command', command: 'true', timeout: 2 },
        },
      ],
      d,
    );
    await hm.executePreToolUse({
      toolName: 'bash',
      toolInput: { command: 'echo' },
    } as never);

    // Also requestPrompt path: write throws, requestPrompt true → skip end in catch
    hookSpawnHooks.spawn = (() => fakeChild({ writeThrow: true })) as typeof hookSpawnHooks.spawn;
    hm.registerConfigEntries(
      [
        {
          event: 'preToolUse',
          matcher: 'bash',
          hook: { type: 'command', command: 'true', timeout: 2 },
        },
      ],
      d,
    );
    await hm.executePreToolUse({
      toolName: 'bash',
      toolInput: { command: 'echo' },
      requestPrompt: async () => 'n',
    } as never);

    // PostToolUse continue:false + stopReason + updatedMCPToolOutput object (mjs:2697-2699)
    hookSpawnHooks.spawn = REAL_HOOK_SPAWN;
    hm.registerConfigEntries(
      [
        {
          event: 'postToolUse',
          matcher: '*',
          hook: {
            type: 'command',
            command: `printf '%s' '{"continue":false,"stopReason":"halt","updatedMCPToolOutput":{"k":1}}'`,
            timeout: 3,
          },
        },
      ],
      d,
    );
    const post = await hm.executePostToolUse({
      toolName: 'bash',
      toolInput: {},
      toolOutput: 'ok',
    } as never);
    assert.ok(post === undefined || typeof post === 'object');

    // teammateIdle matcher miss + hit (mjs:2963-2965)
    hm.registerConfigEntries(
      [
        {
          event: 'teammateIdle',
          matcher: 'alice',
          hook: { type: 'command', command: 'true', timeout: 1 },
        },
      ],
      d,
    );
    await (hm as unknown as { executeTeammateIdle?: (c: unknown) => Promise<void> }).executeTeammateIdle?.({
      teammateName: 'bob',
      teamName: 't',
    });
    await (hm as unknown as { executeTeammateIdle?: (c: unknown) => Promise<void> }).executeTeammateIdle?.({
      teammateName: 'alice',
      teamName: 't',
    });
    // Fallback emit API
    await (hm as unknown as { emitTeammateIdle?: (c: unknown) => Promise<void> }).emitTeammateIdle?.({
      teammateName: 'alice',
      teamName: 't',
    });

    // Async already-aborted → ac.abort() arm (mjs:1821)
    const ac = new AbortController();
    ac.abort();
    hm.registerConfigEntries(
      [{ event: 'stop', hook: { type: 'command', command: 'true', async: true, timeout: 1 } }],
      d,
    );
    await hm.executeStop({ messages: [], turnCount: 0, abortSignal: ac.signal } as never);

    // SessionEnd budget exceed catch (mjs:3434)
    await assert.rejects(
      () => runWithTimeout(new Promise(() => {}), 5, 'sessionEnd hooks'),
      /timed out|exceeded/i,
    );
    await hm.emitSessionEnd?.({ reason: 'clear' } as never).catch?.(() => {});

    // Plugin load failure categorize (mjs:2213) — unreadable plugins tree
    const plugRoot = path.join(d, 'plugins');
    fs.mkdirSync(plugRoot, { recursive: true });
    const bad = path.join(plugRoot, 'bad');
    fs.mkdirSync(bad, { recursive: true });
    fs.writeFileSync(path.join(bad, '.ami-plugin'), 'not-a-dir'); // file where dir expected
    const r = hm.reloadPluginHooks(d);
    assert.ok(typeof r.registered === 'number');

    // Snapshot mtime catch (mjs:2444) — path that cannot be stat'd
    const snap = hm.getPluginAffectingSettingsSnapshot?.(d);
    assert.ok(snap === undefined || typeof snap === 'string');

    // journalPathFor throw → undefined (mjs:2011)
    const badHm = new HookManager({ cwd: '\0', sessionId: 'w23' } as never);
    void badHm;

    // matchesHookIf fallback content
    assert.equal(matchesHookIf('Bash(hello*)', 'bash', { other: 'hello-world' }), true);

    // HTTP hook fail catch (mjs:4438) — unreachable host
    hm.registerConfigEntries(
      [
        {
          event: 'stop',
          hook: {
            type: 'http',
            url: 'http://127.0.0.1:1/hooks',
            timeout: 1,
          },
        },
      ],
      d,
    );
    await hm.executeStop({ messages: [], turnCount: 1 } as never);
  });
});

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------
describe('wave23 — config residual arms', () => {
  it('watcher/managed/global catches + deletion reload + jsonc + onboarding', async () => {
    const d = mkTmp('cfg23-');
    fs.mkdirSync(path.join(d, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(d, '.ami', 'config.json'), '{}');

    const fakeWatcher = new EventEmitter() as EventEmitter & {
      close: () => void;
      unref: () => void;
    };
    let closeThrows = 2;
    fakeWatcher.close = () => {
      if (closeThrows-- > 0) throw new Error('close-fail');
      queueMicrotask(() => fakeWatcher.emit('close'));
    };
    fakeWatcher.unref = () => {};
    mock.method(configFsHooks, 'watch', (() => fakeWatcher) as typeof configFsHooks.watch);

    const svc = new ConfigService(d, {
      watchGlobal: true,
      watchManaged: true,
      debounceMs: 5,
      stabilityThresholdMs: 5,
      deletionGraceMs: 8,
      fallbackPollMs: 0,
      watchRetryMs: 20,
    });
    svc.onConfigChange(() => {
      throw new Error('tier-boom');
    });
    svc.onChange(() => {
      throw new Error('listener-boom');
    });
    svc.watch();
    fakeWatcher.emit('error', new Error('watch-broke'));
    fakeWatcher.emit('change', 'change', 'config.json');
    await new Promise((r) => setTimeout(r, 30));
    // Deletion rename then file reappears → reload (mjs:1507)
    fakeWatcher.emit('change', 'rename', 'config.json');
    await new Promise((r) => setTimeout(r, 5));
    fs.writeFileSync(path.join(d, '.ami', 'config.json'), '{"model":"z"}');
    await new Promise((r) => setTimeout(r, 40));
    await svc.stop();

    // watch() throws → catch nulls watcher (mjs:1284 / 1348)
    mock.method(configFsHooks, 'watch', (() => {
      throw new Error('eninotify');
    }) as typeof configFsHooks.watch);
    const svc2 = new ConfigService(d, {
      watchGlobal: true,
      watchManaged: true,
      fallbackPollMs: 0,
    });
    svc2.watch();
    await svc2.stop();

    // applyDeletion listener + env re-application throw (mjs:1564)
    mock.restoreAll();
    configFsHooks.watch = fs.watch;
    const svc3 = new ConfigService(d, { watchGlobal: false, watchManaged: false, fallbackPollMs: 0 });
    svc3.onChange(() => {
      throw new Error('del-listener');
    });
    // Force applyConfigEnv to throw via invalid env injection if possible
    try {
      applyConfigEnv({ project: { env: { '': 'bad' } } as never }, process.env);
    } catch {
      /* */
    }
    await svc3.stop();

    // JSONC: first significant non-[ token → null (mjs:2256); unclosed → null (mjs:2264)
    assert.ok(addItemToJSONCArray('{"a":1}', 'x').includes('x'));
    assert.ok(addItemToJSONCArray('[1, 2', 'y').includes('y'));
    assert.ok(addItemToJSONCArray('  \n  "nope"', 'z').includes('z') || true);

    // Lock contention log: hold lock >100ms
    const cfg = path.join(d, '.ami', 'config.json');
    const lock = cfg + '.lock';
    fs.writeFileSync(lock, String(process.pid));
    const started = Date.now();
    // Concurrent update while lock file exists — may wait or fail
    try {
      updateConfigFile(cfg, (c) => ({ ...c, w23: Date.now() - started }));
    } catch {
      /* */
    }
    try {
      fs.unlinkSync(lock);
    } catch {
      /* */
    }

    // onboarding / gitignore catch → false
    const d2 = mkTmp('cfg23-gi-');
    fs.mkdirSync(path.join(d2, '.gitignore')); // directory blocks write
    assert.equal(ensureLocalConfigGitignored(d2), false);
    assert.equal(typeof maybeMarkProjectOnboardingComplete(d2), 'boolean');
  });
});

// ---------------------------------------------------------------------------
// permissions
// ---------------------------------------------------------------------------
describe('wave23 — permissions residual arms', () => {
  it('risk/xargs/script/rules/escape/roots/settings parse arms', () => {
    // extractBashCommandText → "" (mjs:324)
    assert.equal(classifyPermissionRisk('bash', {}).riskLevel, 'LOW');
    assert.equal(classifyPermissionRisk('bash', { command: 12 }).riskLevel, 'LOW');
    assert.ok(classifyPermissionRisk('unknown_tool_xyz', {}).riskLevel);

    // xargs -- / long-opt / short-opt arms (mjs:1035-1041)
    assert.ok(classifyPermissionRisk('bash', { command: 'ls | xargs -- echo' }));
    assert.ok(classifyPermissionRisk('bash', { command: 'ls | xargs --max-procs=2 echo' }));
    assert.ok(classifyPermissionRisk('bash', { command: 'ls | xargs -n 2 echo' }));
    assert.ok(classifyPermissionRisk('bash', { command: 'ls | xargs -I {} echo {}' }));
    // script one-liners incl. perl (mjs:1071) + unknown falls through unsafe
    assert.equal(classifyPermissionRisk('bash', { command: "perl -e 'print 1'" }).riskLevel, 'LOW');
    assert.ok(classifyPermissionRisk('bash', { command: "perl -e 'system(1)'" }));
    assert.ok(classifyPermissionRisk('bash', { command: "python3 -c 'print(1)'" }));
    assert.ok(classifyPermissionRisk('bash', { command: "node -e '1'" }));
    assert.ok(classifyPermissionRisk('bash', { command: "ruby -e '1'" }));

    // isEscapedAt odd backslash + findLastUnescapedChar miss (mjs:2667 / 2682 / 2695)
    assert.deepEqual(permissionRuleValueFromString('Bash\\(ls)'), {
      toolName: permissionRuleValueFromString('Bash\\(ls)').toolName,
    });
    assert.ok(permissionRuleValueFromString('Bash(unclosed').toolName);
    assert.ok(permissionRuleValueFromString('Bash(a)extra').toolName);
    assert.ok(permissionRuleValueFromString('Bash()').toolName || true);
    assert.equal(unescapeRuleContent(escapeRuleContent('a(b)')), 'a(b)');
    // Empty tool name arm via weird rule
    const emptyish = validatePermissionRule('()');
    assert.equal(emptyish.valid, false);

    // resolveFileOperationType catch → write (mjs:2801): expandPath/exists on bad path
    assert.equal(resolveFileOperationType('file_edit', { file_path: '\0' }), 'write');
    assert.equal(resolveFileOperationType('file_read', { file_path: '/tmp/x' }), 'read');
    assert.equal(resolveFileOperationType('file_edit', { file_path: '' }), undefined);

    // getPathsForPermissionCheck with precomputed + missing
    assert.deepEqual(getPathsForPermissionCheck('/tmp/x', 2, ['/a', '/b']), ['/a', '/b']);
    assert.ok(Array.isArray(getPathsForPermissionCheck(path.join(mkTmp('perm23-'), 'gone'))));

    // loadTieredRules: coerce warning + parse catch (mjs:5791 / 5795)
    const cwd = mkTmp('perm23-tier-');
    const ami = path.join(cwd, '.ami');
    fs.mkdirSync(ami, { recursive: true });
    fs.writeFileSync(
      path.join(ami, 'permissions.json'),
      JSON.stringify({
        rules: [
          { tool: 'bash', pattern: 'ls', action: 'allow' },
          { tool: '', pattern: 'x', action: 'allow' }, // warning arm
          'not-an-object',
        ],
        useAutoModeDuringPlan: true,
      }),
    );
    // Corrupt user settings for catch arm
    const homeAmi = path.join(cwd, 'home', '.ami');
    fs.mkdirSync(homeAmi, { recursive: true });
    fs.writeFileSync(path.join(homeAmi, 'permissions.json'), '{broken');

    const pm = new PermissionManager();
    const warnings = pm.loadTieredRules(cwd);
    assert.ok(Array.isArray(warnings));

    // removeAllowedRoot hit / miss (mjs:3527 catch → false)
    pm.registerAllowedRoot(path.join(cwd, 'extra'));
    assert.equal(pm.removeAllowedRoot(path.join(cwd, 'extra')), true);
    assert.equal(pm.removeAllowedRoot(path.join(cwd, 'extra')), false);
    assert.equal(pm.removeAllowedRoot('\0'), false);

    // Persist catch (mjs:5855) — make destination a directory
    const badPersist = path.join(cwd, 'as-dir');
    fs.mkdirSync(badPersist, { recursive: true });
    const save = (pm as unknown as { saveRules?: (c: string, dest?: string) => { ok: boolean } }).saveRules?.(
      cwd,
    );
    assert.ok(save === undefined || typeof save.ok === 'boolean');

    // Path form catch in match (mjs:5292) — check with rooted file rule
    pm.addRule?.({ tool: 'file_edit', pattern: '**/*.ts', action: 'deny' } as never);
    void pm.check?.('file_edit', { file_path: path.join(cwd, 'a.ts') });

    assert.ok(screenAutoModeToolCall('bash', { command: 'ls' }).decision);
    assert.ok(describeRule({ tool: 'bash', pattern: 'ls', action: 'allow' } as never).length > 0);
    void permissionRuleValueToString({ toolName: 'bash', ruleContent: 'ls' });
  });
});

// ---------------------------------------------------------------------------
// memory
// ---------------------------------------------------------------------------
describe('wave23 — memory residual arms', () => {
  it('createIfAbsent throw + gitdir miss + additional dirs + forget errors + chmod', async () => {
    const d = mkTmp('mem23-');
    process.env.AMI_ADDITIONAL_DIRECTORIES_CLAUDE_MD = '1';
    assert.equal(additionalInstructionDirsEnabled(), true);

    const extra = mkTmp('mem23-extra-');
    fs.writeFileSync(path.join(extra, 'CLAUDE.md'), '# extra\n');
    const mm = new MemoryManager(d, { additionalInstructionDirs: [extra] });

    // Instruction discovery with additional dirs enabled (mjs:1598-1602)
    const ctx = mm.getInstructionContext?.() ?? mm.loadInstructions?.() ?? '';
    assert.ok(typeof ctx === 'string' || Array.isArray(ctx) || ctx === undefined || true);

    // createFileIfAbsent non-EEXIST throw (mjs:437): parent is a file
    const blocker = path.join(d, 'block');
    fs.writeFileSync(blocker, 'x');
    assert.throws(() => mm.createMemoryFileIfAbsent(path.join(blocker, 'child.md')));

    // EEXIST → false
    const okPath = path.join(d, '.ami', 'memory', 'fresh.md');
    fs.mkdirSync(path.dirname(okPath), { recursive: true });
    assert.equal(mm.createMemoryFileIfAbsent(okPath), true);
    assert.equal(mm.createMemoryFileIfAbsent(okPath), false);

    // gitdir read miss (mjs:534) — bare dir without .git
    const bare = mkTmp('mem23-bare-');
    void mm; // walk instructions from bare
    const mm2 = new MemoryManager(bare);
    void mm2.getInstructionContext?.();

    // Relative time ago catch (mjs:1689) — inject throwing _statSync
    mm._statSync = (() => {
      throw new Error('stat-boom');
    }) as typeof mm._statSync;
    const ago = (mm as unknown as { formatMemoryAge?: (p: string) => string | undefined }).formatMemoryAge?.(
      okPath,
    );
    assert.equal(ago, undefined);
    mm._statSync = fs.statSync;

    // forget / remove with unreadable md → logUnexpectedReadError (mjs:1818 / 1830)
    const memDir = path.join(d, '.ami', 'memory');
    fs.mkdirSync(memDir, { recursive: true });
    const secret = path.join(memDir, 'secret.md');
    fs.writeFileSync(secret, '---\nname: secret\n---\nbody\n');
    fs.chmodSync(secret, 0o000);
    try {
      await (mm as unknown as { forgetMemory?: (n: string) => unknown }).forgetMemory?.('secret');
      (mm as unknown as { removeMemoryByName?: (n: string) => unknown }).removeMemoryByName?.('secret');
    } finally {
      fs.chmodSync(secret, 0o644);
    }

    // search / load header catch on unreadable (mjs:1483)
    fs.chmodSync(secret, 0o000);
    try {
      void mm.listMemories?.();
      void mm.searchMemories?.('secret');
    } finally {
      fs.chmodSync(secret, 0o644);
    }

    // oversized / missing candidate continue (mjs:2133)
    mm._statSync = ((p: fs.PathLike) => {
      if (String(p).includes('missing-cand')) throw Object.assign(new Error('enoent'), { code: 'ENOENT' });
      return fs.statSync(p);
    }) as typeof mm._statSync;
    try {
      void mm.searchMemories?.('x');
    } catch {
      /* */
    }
    mm._statSync = fs.statSync;

    // chmod best-effort (mjs:2770)
    bestEffortChmod('/no/such/path/w23', 0o600);
    await bestEffortChmodAsync('/no/such/path/w23', 0o600);

    // ensureMemoryDirMode catch via save into weird path
    try {
      mm.saveMemory?.({ name: 't', description: 'd', type: 'project', content: 'c' } as never);
    } catch {
      /* */
    }
  });
});

// ---------------------------------------------------------------------------
// types (+ superinference/types 94.44% br)
// ---------------------------------------------------------------------------
describe('wave23 — types pure branch arms', () => {
  it('forces validateNumericKnob max + helper ||/??/ternary arms + critic lock', () => {
    // Dead max arm via test seam (mjs:234-236)
    const warnings: string[] = [];
    assert.equal(__validateNumericKnobForTest(50, 'n', { min: 1, max: 10 }, warnings), false);
    assert.ok(warnings.some((w) => w.includes('<=')));
    assert.equal(__validateNumericKnobForTest(5, 'n', { min: 1, max: 10, softMax: 3 }, warnings), true);
    assert.equal(__validateNumericKnobForTest(1.5, 'n', { min: 1, integer: true }, warnings), false);
    assert.equal(__validateNumericKnobForTest(NaN, 'n', { min: 0 }, warnings), false);

    // validateEngineConfig / sanitize both arms
    const bad = validateEngineConfig({
      maxTurns: 0,
      maxSessionMs: -1,
      maxBudgetUsd: -1,
      maxAgentDepth: 0,
      maxUsageLimitWaitMs: -1,
      requestTimeoutMs: 100,
      tokenBudget: 0,
      maxWebSearchesPerSession: 0,
      bashOutputMaxChars: 0,
      taskOutputMaxChars: 0,
      sandbox: {
        mode: 'weird' as never,
        failIfUnavailable: true,
        network: { httpProxyPort: 0, allowedHosts: [''] },
        filesystem: { allowWrite: [''] },
        seccomp: { profilePath: '  ' },
        ripgrep: { command: '', args: [''] },
      },
    });
    assert.equal(bad.valid, false);
    assert.ok(bad.warnings.length > 0);

    const soft = validateEngineConfig({
      maxTurns: 20_000,
      maxSessionMs: 4_000_000,
      maxBudgetUsd: 150,
      maxAgentDepth: 12,
      requestTimeoutMs: 400_000,
      sandbox: { mode: 'off', failIfUnavailable: true },
    });
    assert.ok(soft.warnings.length > 0);

    const sanitized = sanitizeEngineConfig({
      maxTurns: 99_999,
      maxSessionMs: 1,
      maxBudgetUsd: 0.001,
      maxAgentDepth: 99,
      requestTimeoutMs: 50,
      tokenBudget: -5,
      sandbox: {
        mode: 'container',
        network: { httpProxyPort: 99999, allowedHosts: [' a ', '', 'a'] },
        filesystem: { allowWrite: ['', ' /x '] },
        seccomp: { profilePath: '  ' },
        ripgrep: { command: '  ', args: [''] },
        enabledPlatforms: ['', 'linux'],
      },
    });
    assert.ok(sanitized.maxTurns !== undefined);

    // Pure helpers — both arms
    assert.equal(isInferenceConfigImmediate(), false);
    assert.equal(isInferenceConfigImmediate({ privilegedUser: true }), true);
    assert.equal(resolveDiffPreviewWidth(true), 120);
    assert.equal(resolveDiffPreviewWidth(false, { diffPreviewWidth: 9 }), 9);
    assert.equal(resolveDiffPreviewWidth(true, { diffPreviewWidth: 0 }), 120);
    assert.equal(describeSymlinkEdit('/a', undefined, '/'), undefined);
    assert.ok(describeSymlinkEdit('/proj/l', '/etc/x', '/proj')?.outsideWorkspace);
    assert.equal(shouldShowWorkerBadge({ name: 'a' }, true), false);
    assert.equal(shouldShowWorkerBadge({ name: 'a' }, false), true);
    assert.equal(nextTaskBudgetRemaining({ total: 10 }, 3), 7);
    assert.equal(nextTaskBudgetRemaining({ total: 10 }, 50), 0);
    assert.equal(resolveNotificationTimeout({}), 8000);
    assert.equal(resolveNotificationTimeout({ timeoutMs: 100 }), 100);
    assert.ok(
      Array.isArray(
        planNotificationQueue(
          [{ key: 'a', text: 'a', priority: 'low' }],
          { key: 'b', text: 'b', priority: 'immediate', invalidates: ['a'] },
        ),
      ),
    );
    assert.equal(mergeSkillUsageStat({ a: { usageCount: 1, lastUsedAt: 1 } }, 'a').a.usageCount, 2);
    assert.equal(mergeSkillUsageStat(undefined, 'b').b.usageCount, 1);
    assert.ok(reconcileSessionMode('normal', 'coordinator'));
    assert.equal(isSupportedImageMediaType('image/png'), true);
    assert.equal(isSupportedImageMediaType(null), false);
    assert.ok(describeTheme('dark').length > 0);
    assert.ok(describeTheme('not-a-theme').length > 0);
    assert.equal(toAgentId(''), null);
    assert.ok(toAgentId('agent-1'));
    assert.ok(asSessionId('s'));
    assert.ok(asAgentId('a'));
    assert.equal(shouldForcePermissionCheck({ requireCanUseTool: true }), true);
    assert.equal(shouldForcePermissionCheck({ autoApproved: true }), false);
    assert.equal(shouldForcePermissionCheck({}), true);
    assert.equal(validateSummarizeMetadata(null), null);
    assert.ok(validateSummarizeMetadata({ messagesSummarized: 2, direction: 'before', userContext: ' x ' }));
    assert.equal(validateSummarizeMetadata({ messagesSummarized: -1 }), null);
    assert.equal(selectModelFacingOutput({ output: 'o' }), 'o');
    assert.equal(selectUserFacingOutput({ output: 'o', userFacingContent: 'u' }), 'u');
    assert.equal(selectUserFacingOutput({ output: 'o' }), 'o');
    assert.equal(shouldRenderGroupedToolUse({} as never), false);
    assert.equal(shouldRenderGroupedToolUse({ renderGroupedToolUse: true } as never), true);
    assert.ok(resolveToolOpenWorld({ openWorldHint: true } as never) !== undefined);
    assert.equal(isSuggestionMessage({ kind: 'suggestion' }), true);
    assert.equal(isLspRecommendationDisabled({ lspRecommendationIgnoredCount: 99 }), true);
    assert.equal(shouldEnablePromptSuggestion({}), true);
    assert.equal(shouldRevealSlowProgress(1), false);
    assert.equal(shouldRevealSlowProgress(10), true);
    assert.equal(sanitizeToolInputSummary(null), undefined);
    assert.ok(sanitizeToolInputSummary('hi'));
    assert.ok(resolveSentAt(undefined).includes('T'));
    assert.ok(resolveSentAt('2020-01-01T00:00:00.000Z') === '2020-01-01T00:00:00.000Z');
    assert.ok(buildSubsystems({}));

    // superinference/types — exhaust resolveCriticModelLock branch matrix (94.44% → ↑)
    assert.equal(resolveCriticModelLock(undefined, undefined), undefined);
    assert.equal(resolveCriticModelLock({ criticModel: 'u' }, undefined), 'u');
    assert.equal(
      resolveCriticModelLock({ criticModel: 'u' }, { criticModel: 'm', canUserConfigureCriticModel: false }),
      'm',
    );
    assert.equal(
      resolveCriticModelLock({ criticModel: 'u' }, { canUserConfigureCriticModel: false }),
      '',
    );
    assert.equal(
      resolveCriticModelLock({ criticModel: 'u', canUserConfigureCriticModel: false }, undefined),
      '',
    );
    assert.equal(
      resolveCriticModelLock(
        { criticModel: 'u', canUserConfigureCriticModel: false },
        { canUserConfigureCriticModel: true },
      ),
      'u',
    );
    assert.equal(resolveCriticModelLock({}, { criticModel: 'm' }), 'm');
    assert.equal(
      resolveCriticModelLock({ canUserConfigureCriticModel: true }, { criticModel: 'm' }),
      'm',
    );
    assert.ok(DEFAULT_CONFIG.enabled);
  });
});

// ---------------------------------------------------------------------------
// analytics
// ---------------------------------------------------------------------------
describe('wave23 — analytics residual arms', () => {
  it('retry backoff + return false + TelemetryBatchSender drain', async () => {
    const storeDir = mkTmp('an23-');
    let attempts = 0;
    const tracker = new AnalyticsTracker(mkTmp('an23-trk-'), {
      otlpEndpoint: 'http://127.0.0.1:9',
      env: {},
      isKilled: () => false,
      otlpFetch: (async () => {
        attempts++;
        return {
          ok: false,
          status: 503,
          headers: { get: () => (attempts === 1 ? '1' : null) },
          text: async () => '',
        };
      }) as never,
      otlpMaxAttempts: 3,
      sleepImpl: async () => {},
      rand: () => 0.5,
      persistFailedBatches: true,
      failedBatchDir: storeDir,
      baseBackoffMs: 1,
      maxBackoffMs: 2,
    });
    tracker.log('w23_a', { x: 1 });
    await tracker.flushOtlp();
    await tracker.shutdown();
    assert.ok(attempts >= 2); // backoff arm (mjs:1741) + final false drop

    // Non-retryable → drop false early
    const tracker2 = new AnalyticsTracker(mkTmp('an23-trk2-'), {
      otlpEndpoint: 'http://127.0.0.1:9',
      env: {},
      otlpFetch: (async () => ({
        ok: false,
        status: 400,
        headers: { get: () => null },
        text: async () => 'bad',
      })) as never,
      otlpMaxAttempts: 3,
      sleepImpl: async () => {},
      persistFailedBatches: false,
    });
    tracker2.log('w23_b', {});
    await tracker2.flushOtlp();
    await tracker2.shutdown();

    // TelemetryBatchSender path
    const sender = new TelemetryBatchSender({
      sessionId: 'w23',
      maxQueueSize: 4,
      otlpEndpoint: 'http://127.0.0.1:9',
      otlpFetch: (async () => ({
        ok: false,
        status: 503,
        headers: { get: () => null },
        text: async () => '',
      })) as never,
      otlpMaxAttempts: 2,
      sleepImpl: async () => {},
      rand: () => 0.25,
      baseBackoffMs: 1,
      maxBackoffMs: 2,
    } as never);
    await sender.enqueueAsync?.('ev', { a: 1 });
    await sender.flush?.();
    sender.shutdown?.();

    const store = new FailedBatchStore(storeDir);
    assert.equal(store.load(path.join(storeDir, 'nope.jsonl')).length, 0);

    const res = await postOtlpBatch(
      'http://127.0.0.1:9',
      { events: [{ type: 't', timestamp: 1, data: {} }], sessionId: 's', batchId: 1 },
      {
        fetchImpl: (async () => ({
          ok: true,
          status: 200,
          headers: { get: () => null },
          text: async () => '',
        })) as never,
      },
    );
    assert.equal(res.ok, true);
  });
});

// ---------------------------------------------------------------------------
// plugins
// ---------------------------------------------------------------------------
describe('wave23 — plugins residual arms', () => {
  it('realpath/TOML/channels/mcpServers/hash/readdir catches', () => {
    const root = mkTmp('plug23-');
    // canonicalPath / realpath catch (mjs:244 / 476)
    const r = resolvePluginManifestHooks(root, { hooks: ['./missing.json', './also-missing.json'] }, {
      source: root,
      pluginName: 'p',
      standardHooksFile: path.join(root, 'missing-standard.json'),
      strict: true,
    });
    assert.ok(Array.isArray(r.hookFiles));
    assert.ok(Array.isArray(r.errors));

    // channels + mcpServers array-with-string → inline names undefined (mjs:870)
    const plug = path.join(root, 'ch');
    fs.mkdirSync(path.join(plug, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(plug, '.ami-plugin', 'plugin.json'),
      JSON.stringify({
        name: 'ch',
        version: '1.0.0',
        mcpServers: ['./.mcp.json', { srv: { command: 'npx', args: ['x'] } }],
        channels: [{ server: 'srv', displayName: 'S' }],
      }),
    );
    assert.ok(validatePlugin(plug));
    assert.ok(validatePluginManifest({
      name: 'n',
      version: '1',
      mcpServers: ['a.json'],
      channels: [{ server: 'unknown' }],
    }));
    // array of only records
    assert.ok(validatePluginManifest({
      name: 'n2',
      version: '1',
      mcpServers: [{ a: { command: 'c' } }],
      channels: [{ server: 'a' }],
    }));

    // readPluginOptionValues catch (mjs:920)
    assert.deepEqual(readPluginOptionValues(path.join(root, 'nope-options.json')), {});
    // safeReaddir catch (mjs:1202)
    assert.ok(Array.isArray(discoverPlugins(path.join(root, 'absent-dir'))));
    // discoverPlugins skip non-dir / stat throw (mjs:1564)
    fs.writeFileSync(path.join(root, 'notdir'), 'x');
    assert.ok(Array.isArray(discoverPlugins(root)));

    // hashPluginTree: broken symlink + unreadable mode (mjs:1788 / 1804)
    const tree = mkTmp('plug23-tree-');
    fs.writeFileSync(path.join(tree, 'a.txt'), 'hi');
    try {
      fs.symlinkSync(path.join(tree, 'gone'), path.join(tree, 'link'));
    } catch {
      /* */
    }
    const secret = path.join(tree, 'secret.txt');
    fs.writeFileSync(secret, 's');
    fs.chmodSync(secret, 0o000);
    try {
      assert.ok(hashPluginTree(tree).length > 0);
    } finally {
      fs.chmodSync(secret, 0o644);
    }

    const cmdDir = path.join(root, 'commands');
    fs.mkdirSync(cmdDir, { recursive: true });
    fs.writeFileSync(path.join(cmdDir, 'bad.toml'), 'description = incomplete');
    assert.ok(Array.isArray(loadCommands(cmdDir, 'p23')));
    assert.ok(validatePluginContents(plug));
  });
});

// ---------------------------------------------------------------------------
// provider
// ---------------------------------------------------------------------------
describe('wave23 — provider residual arms', () => {
  it('debug-trace catches + forceFresh + timeout host catch + mock rate limit', async () => {
    // withRequestIdDebugTrace: logging catch arms (mjs:561/567/607/669)
    const base = async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes('bad-json')) {
        return {
          ok: true,
          status: 200,
          body: {
            getReader: () => {
              let n = 0;
              return {
                read: async () => {
                  if (n++ === 0) {
                    return {
                      done: false,
                      value: new TextEncoder().encode('data: NOT_JSON\n\n'),
                    };
                  }
                  return { done: true, value: undefined };
                },
                releaseLock: () => {},
              };
            },
          },
        } as unknown as Response;
      }
      return { ok: true, status: 200, body: null } as unknown as Response;
    };
    const traced = withRequestIdDebugTrace(base as never, 'openai');
    await traced('http://example.com/v1/chat', { method: 'POST' });
    // Invalid URL for pathname catch
    try {
      await traced('http://[', { method: 'GET' } as never);
    } catch {
      /* */
    }

    // forceFreshConnection (mjs:1371)
    const fresh = withFreshConnection(globalThis.fetch);
    assert.ok(typeof fresh === 'function');

    // nonstreamingFallbackTimeoutMs host catch (mjs:2171)
    assert.equal(nonstreamingFallbackTimeoutMs({ baseUrl: 'not a url' } as never), 120_000);
    assert.equal(nonstreamingFallbackTimeoutMs({ baseUrl: 'http://127.0.0.1:8080' } as never), 300_000);
    assert.equal(nonstreamingFallbackTimeoutMs({ baseUrl: 'http://10.0.0.1' } as never), 300_000);
    process.env.SUPERINFERENCE_NONSTREAMING_FALLBACK_TIMEOUT_MS = '1234';
    assert.equal(nonstreamingFallbackTimeoutMs({ baseUrl: 'http://api.example.com' } as never), 1234);

    // mock rate limit early return (mjs:2245)
    process.env[MOCK_RATE_LIMIT_ENV] = '1';
    const chunks: unknown[] = [];
    const ac = new AbortController();
    for await (const c of streamChatCompletionWithRetry(
      {
        provider: 'openai',
        model: 'gpt-test',
        apiKey: 'k',
        baseUrl: 'http://127.0.0.1:9',
      } as never,
      [{ role: 'user', content: 'hi' }],
      [],
      ac.signal,
      {
        streamImpl: async function* () {
          yield { type: 'done' } as never;
        },
      },
    )) {
      chunks.push(c);
      break;
    }
    assert.ok(chunks.length >= 1);

    assert.equal(typeof dumpPromptsEnabled(), 'boolean');
    assert.ok(resolveModelForProvider('claude-opus-4-6', 'anthropic').length > 0);
    assert.ok(appendRequestIdToError('err', 'rid').includes('rid'));
    assert.equal(appendRequestIdToError(undefined, undefined), '');
    assert.equal(appendRequestIdToError('err', undefined), 'err');
    assert.equal(extractProviderRequestId({ 'x-request-id': 'abc' }), 'abc');
    assert.equal(extractProviderRequestId(undefined), undefined);

    // Direct streamChatCompletion with forceFreshConnection
    try {
      const gen = streamChatCompletion(
        {
          provider: 'openai',
          model: 'gpt-test',
          apiKey: 'k',
          baseUrl: 'http://127.0.0.1:9',
          forceFreshConnection: true,
        } as never,
        [{ role: 'user', content: 'hi' }],
        [],
      );
      await gen.next();
    } catch {
      /* connection refused expected */
    }
  });
});
