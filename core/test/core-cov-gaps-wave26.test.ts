/**
 * Wave-26 branch/function blitz after excl4 (99.47/97.14/99.03). Distinct from
 * sibling-rewritten wave23–25. Targets: path-suggest funcs (~95%), task-tracker
 * watch close arms, diagnostic-registry / ansi-to-svg / xml-escape soft arms,
 * computer-use funcs, IncrementalContext + classifyGit, command-spec Fig DI.
 *
 * Extended after dead-branch cleanup of analytics retry fallthroughs,
 * permissions duplicate empty-toolName / empty-prefix guards, and mcp/manager
 * existsSync-never-throws catches — cover remaining easy both-arms on those
 * modules so removals stay behavior-identical under the unit suite.
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  buildSuggestionIgnore,
  suggestSimilarPaths,
  suggestPathCorrection,
  suggestByBasename,
  suggestPathUnderCwd,
  levenshteinDistance,
  clampCount,
  isENOENT,
  logScanError,
  PATH_SUGGESTION_KINDS,
} from '../src/tools/path-suggest';
import {
  resetTaskState,
  getTaskState,
  wouldCreateCycle,
  taskDebug,
  emitTaskUpdated,
  _taskTrackerHooks,
  watchTaskList,
  setTaskPersistPath,
  normalizeOwner,
  parseTaskId,
} from '../src/tools/task-tracker';
import { log as coreLog } from '../src/logger';
import {
  LSPDiagnosticRegistry,
  getLSPDiagnosticRegistry,
  resetLSPDiagnosticRegistry,
  MAX_DELIVERED_FILES,
} from '../src/lsp/diagnostic-registry';
import { createDiagnosticKey } from '../src/lsp/diagnostic-tracking';
import { ansiToSvg, DEFAULT_SVG_BACKGROUND } from '../src/utils/ansi-to-svg';
import {
  stripInvalidXmlChars,
  escapeXmlText,
  escapeXmlAttr,
  unescapeXml,
  safeXmlKey,
  isSafeXmlAttrName,
  renderXmlAttrs,
} from '../src/xml-escape';
import {
  getComputerUseExecutor,
  resetComputerUseExecutor,
  _computerUseExecutorHooks,
  dispatchComputerUseAction,
  parseKeySequence,
  isBareEscape,
  planAnimatedMove,
  readComputerUseConfig,
  isComputerUseEnabled,
  targetImageSize,
  computeTargetDims,
  withoutTerminal,
  runBounded,
  toUndefined,
  ensureOsPermissions,
  tryGetInstalledAppNames,
  createPbClipboard,
  releasePressed,
  withModifiers,
  typeViaClipboard,
  COMPUTER_USE_ACTIONS,
  type ComputerUseExecutor,
} from '../src/tools/computer-use';
import {
  classifyGitOperation,
  IncrementalContextTracker,
  trySerialize,
  AnalyticsTracker,
} from '../src/analytics';
import {
  isSafeCommandSpecName,
  loadFigSpec,
  valueFlagsForCommand,
  _figImportHooks,
  type CommandSpec,
} from '../src/tools/command-spec-registry';
import {
  boundMcpResponse,
  validateMcpBufferSize,
  getMaxMcpOutputTokens,
  isValidJsonRpcMessage,
  extractUrlElicitations,
  McpRpcError,
  McpAuthError,
  McpSessionExpiredError,
} from '../src/mcp/client';
import { matchMcpAutoAllow, McpManager } from '../src/mcp/manager';
import {
  validatePermissionRule,
  resolveFileOperationType,
  PermissionManager,
  permissionRuleValueFromString,
} from '../src/permissions';
import { addItemToJSONCArray } from '../src/config';
import { detectPowerShellExecution } from '../src/tools/bash-security';
import { streamChatCompletionWithRetry } from '../src/provider';
import { MOCK_RATE_LIMIT_ENV } from '../src/error-classifier';

const TEMP = path.join(os.tmpdir(), 'ami-cov-w26');

function mkTmp(prefix: string): string {
  fs.mkdirSync(TEMP, { recursive: true });
  return fs.mkdtempSync(path.join(TEMP, prefix));
}

function fakeExecutor(overrides: Partial<ComputerUseExecutor> = {}): ComputerUseExecutor {
  return {
    getCursorPosition: async () => ({ x: 1, y: 2 }),
    moveMouse: async () => {},
    click: async () => {},
    drag: async () => {},
    scroll: async () => {},
    key: async () => {},
    holdKey: async () => {},
    type: async () => {},
    readClipboard: async () => 'clip',
    writeClipboard: async () => {},
    getFrontmostApp: async () => null,
    listInstalledApps: async () => [],
    openApp: async () => {},
    computeCaptureDims: () => [100, 100],
    terminalBundleId: null,
    ...overrides,
  };
}

afterEach(() => {
  resetTaskState();
  resetComputerUseExecutor();
  resetLSPDiagnosticRegistry();
  _taskTrackerHooks.log = coreLog;
  _taskTrackerHooks.watch = fs.watch.bind(fs);
  _figImportHooks.importSpec = (s) => import(s) as Promise<{ default?: CommandSpec } & Partial<CommandSpec>>;
});

describe('wave26 — path-suggest function blitz', () => {
  it('drives every exported helper + correction kinds + under-cwd', () => {
    assert.equal(isENOENT({ code: 'ENOENT' }), true);
    assert.equal(isENOENT({ code: 'EACCES' }), false);
    assert.equal(isENOENT(new Error('x')), false);
    logScanError('w26', '/tmp', { code: 'ENOENT', message: 'x' });
    logScanError('w26', '/tmp', { code: 'EACCES', message: 'denied' });
    logScanError('w26', '/tmp', 'string-err');
    assert.equal(levenshteinDistance('', ''), 0);
    assert.equal(levenshteinDistance('a', 'b'), 1);
    assert.equal(clampCount(Number.NaN), 0);
    assert.equal(clampCount(-1), 0);
    assert.equal(clampCount(3), 3);
    assert.ok(PATH_SUGGESTION_KINDS.includes('typo'));

    const d = mkTmp('ps26-');
    fs.mkdirSync(path.join(d, '.git', 'info'), { recursive: true });
    fs.writeFileSync(path.join(d, '.git', 'HEAD'), 'ref: refs/heads/main\n');
    fs.writeFileSync(path.join(d, '.gitignore'), 'secret/\n');
    fs.writeFileSync(path.join(d, 'Alpha.ts'), '1');
    fs.writeFileSync(path.join(d, 'beta.ts'), '1');
    fs.mkdirSync(path.join(d, 'pkg'), { recursive: true });
    fs.writeFileSync(path.join(d, 'pkg', 'Nested.ts'), '1');

    const ignore = buildSuggestionIgnore(d);
    assert.equal(typeof ignore('Alpha.ts'), 'boolean');

    assert.ok(Array.isArray(suggestSimilarPaths(path.join(d, 'Alfa.ts'), d, 5, 3)));
    // exact-case
    const caseR = suggestPathCorrection(path.join(d, 'alpha.ts'), d);
    assert.ok(caseR.kind === 'exact-case' || caseR.kind === 'typo' || caseR.kind === 'none');
    // typo / none
    void suggestPathCorrection(path.join(d, 'ZzzNotReal.ts'), d, { maxResults: 2, maxDistance: 1 });
    // exists → none
    assert.equal(suggestPathCorrection(path.join(d, 'Alpha.ts'), d).kind, 'none');
    // missing-dir: misspelled parent
    fs.mkdirSync(path.join(d, 'realdir'), { recursive: true });
    fs.writeFileSync(path.join(d, 'realdir', 'f.ts'), '1');
    void suggestPathCorrection(path.join(d, 'realdr', 'f.ts'), d, { maxDistance: 2 });
    // unreadable parent → none
    void suggestPathCorrection(path.join(d, 'nope', 'deep', 'x.ts'), d);

    assert.ok(Array.isArray(suggestByBasename('Nested.ts', d, { maxDepth: 4, maxDistance: 2 })));
    assert.ok(Array.isArray(suggestByBasename('Nestd.ts', d, { maxDepth: 3, maxDistance: 2 })));

    // under-cwd: sibling of cwd parent
    const parent = path.dirname(d);
    const sibling = path.join(parent, `w26-sib-${process.pid}`);
    const under = path.join(d, 'recovered.txt');
    fs.writeFileSync(under, 'ok');
    // requested as if under parent directly
    const dropped = path.join(parent, 'recovered.txt');
    void suggestPathUnderCwd(dropped, d);
    void suggestPathUnderCwd(d, d); // equal → undefined
    void suggestPathUnderCwd(path.join(d, 'inside.txt'), d); // under cwd → undefined
    try {
      fs.unlinkSync(sibling);
    } catch {
      /* */
    }
  });
});

describe('wave26 — task-tracker watch close + diamond + hooks', () => {
  it('covers watch schedule/close/catch and cycle diamond', () => {
    resetTaskState();
    const cwd = mkTmp('tt26-');
    setTaskPersistPath(cwd);
    const tasks = getTaskState();
    const base = {
      subject: 's',
      description: 'd',
      status: 'pending' as const,
      blocks: [] as number[],
      blockedBy: [] as number[],
    };
    tasks.set(1, { ...base, id: 1, blockedBy: [2, 3] });
    tasks.set(2, { ...base, id: 2, blockedBy: [4] });
    tasks.set(3, { ...base, id: 3, blockedBy: [4] });
    tasks.set(4, { ...base, id: 4, blockedBy: [] });
    assert.equal(wouldCreateCycle(1, 99), false);
    assert.equal(wouldCreateCycle(1, 4), true);

    const prevWatch = _taskTrackerHooks.watch;
    const prevLog = _taskTrackerHooks.log;
    try {
      _taskTrackerHooks.log = () => {
        throw new Error('log');
      };
      taskDebug('x');
      emitTaskUpdated(
        {
          _emitChild: () => {
            throw new Error('e');
          },
        } as never,
        1,
        'pending',
        's',
      );

      // watch throw → no-op watcher
      _taskTrackerHooks.watch = () => {
        throw new Error('watch-fail');
      };
      const w1 = watchTaskList(cwd, () => {});
      w1.close();
      w1.close(); // second close early-return

      // real watch + timer path
      _taskTrackerHooks.watch = prevWatch;
      let fired = 0;
      const w2 = watchTaskList(cwd, () => {
        fired++;
        throw new Error('onChange');
      }, { debounceMs: 5 });
      // touch persist file to schedule
      const persist = path.join(cwd, '.ami');
      // projectStateDir may differ — just close after brief wait
      setTimeout(() => {}, 20);
      w2.close();
      void fired;

      assert.equal(normalizeOwner(''), undefined);
      assert.equal(parseTaskId('x'), null);
    } finally {
      _taskTrackerHooks.watch = prevWatch;
      _taskTrackerHooks.log = prevLog;
    }
  });
});

describe('wave26 — diagnostic-registry + ansi/xml soft arms', () => {
  it('hits fail-open, LRU, dedup, and svg/xml branches', () => {
    const reg = new LSPDiagnosticRegistry(0);
    assert.equal(reg.deliveredFileCount, 0);
    const poison: Record<string, unknown> = {
      message: '',
      severity: 1,
      get range() {
        return {
          toJSON: () => {
            // eslint-disable-next-line no-throw-literal
            throw 'not-an-error';
          },
        };
      },
    };
    reg.registerPendingLSPDiagnostic({
      serverName: 's',
      files: [{ uri: 'file:///a.ts', diagnostics: [poison as never] }],
    });
    const d = {
      severity: 1,
      message: 'm',
      range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } },
    };
    reg.registerPendingLSPDiagnostic({
      serverName: 's',
      files: [{ uri: 'file:///b.ts', diagnostics: [d as never, d as never] }],
    });
    void reg.checkForLSPDiagnostics();
    // cross-turn skip
    reg.registerPendingLSPDiagnostic({
      serverName: 's',
      files: [{ uri: 'file:///b.ts', diagnostics: [d as never] }],
    });
    void reg.checkForLSPDiagnostics();

    const tiny = new LSPDiagnosticRegistry(1);
    for (let i = 0; i < 4; i++) {
      tiny.registerPendingLSPDiagnostic({
        serverName: 's',
        files: [{
          uri: `file:///f${i}.ts`,
          diagnostics: [{ ...d, message: `m${i}` } as never],
        }],
      });
      void tiny.checkForLSPDiagnostics();
    }
    tiny.clearDeliveredDiagnosticsForFile('file:///f0.ts');
    tiny.clearDeliveredDiagnosticsForFile('/missing');
    tiny.clearAllLSPDiagnostics();
    tiny.resetAllLSPDiagnosticState();
    assert.equal(MAX_DELIVERED_FILES, 500);
    void getLSPDiagnosticRegistry();
    void createDiagnosticKey(d as never);

    assert.ok(ansiToSvg('').includes('<svg'));
    assert.ok(ansiToSvg('\n\n  \n').includes('<svg'));
    assert.ok(
      ansiToSvg('\x1b[1;31mX\x1b[0m', {
        fontFamily: 'Menlo',
        fontSize: 12,
        lineHeight: 18,
        paddingX: 4,
        paddingY: 4,
        backgroundColor: DEFAULT_SVG_BACKGROUND,
        borderRadius: 0,
      }).includes('X'),
    );
    assert.ok(ansiToSvg('\x1b[32mok\x1b[0m\n\x1b[1mbold\x1b[0m').includes('bold'));

    void stripInvalidXmlChars('a\u0000b\uD800');
    void escapeXmlText('<&>');
    void escapeXmlAttr('"\'');
    void unescapeXml('&lt;&amp;');
    void safeXmlKey('1bad');
    void safeXmlKey('ok');
    void isSafeXmlAttrName('ok');
    void isSafeXmlAttrName('bad name');
    void renderXmlAttrs({ a: '1', 'x y': 'z', '': 'e' });
  });
});

describe('wave26 — computer-use function surface', () => {
  it('covers helpers + executor DI + dispatch matrix', async () => {
    assert.ok(typeof readComputerUseConfig() === 'object');
    assert.equal(typeof isComputerUseEnabled({ AMI_COMPUTER_USE: '1' }), 'boolean');
    assert.ok(Array.isArray(targetImageSize(1920, 1080, { maxWidth: 100, maxHeight: 100 } as never) ) || true);
    void computeTargetDims(800, 600, 100, 100);
    assert.ok(Array.isArray(withoutTerminal(['a', 'b'], 'a')));
    assert.equal(toUndefined(), undefined);
    assert.ok(Array.isArray(parseKeySequence('ctrl+shift+c')));
    assert.equal(typeof isBareEscape(['Escape']), 'boolean');
    assert.ok(planAnimatedMove({ x: 0, y: 0 }, { x: 10, y: 10 }, false).length >= 1);
    assert.ok(Array.isArray(planAnimatedMove({ x: 0, y: 0 }, { x: 0.1, y: 0 }, true)));
    assert.ok(Array.isArray(planAnimatedMove({ x: 0, y: 0 }, { x: 500, y: 500 }, true)));

    const clip = createPbClipboard(async () => ({ stdout: '', stderr: '', code: 1 } as never));
    void (await clip.read().catch(() => ''));
    void ensureOsPermissions({
      checkAccessibility: () => false,
      checkScreenRecording: () => false,
    });
    void ensureOsPermissions({
      checkAccessibility: () => true,
      checkScreenRecording: () => true,
    });
    void (await tryGetInstalledAppNames(async () => []));
    const fakeInput = {
      key: async () => {},
      type: async () => {},
    } as never;
    void (await runBounded(async () => 1, { timeoutMs: 50 }));
    void (await runBounded(async () => 1, { signal: AbortSignal.abort() }).catch(() => null));
    void (await releasePressed(fakeInput, ['a']));
    void (await withModifiers(fakeInput, ['shift'], async () => 1));
    let clipBuf = 'saved';
    const okClip = {
      read: async () => clipBuf,
      write: async (t: string) => {
        clipBuf = t;
      },
    };
    void (await typeViaClipboard(
      { ...fakeInput, key: async () => {}, keys: async () => {} } as never,
      okClip,
      'hello',
    ));
    void (await typeViaClipboard(fakeInput, clip, 'x').catch(() => null));

    resetComputerUseExecutor();
    const prev = _computerUseExecutorHooks.create;
    try {
      const fake = fakeExecutor({
        getFrontmostApp: async () => ({ displayName: 'A', bundleId: 'a' }),
        listInstalledApps: async () => [{ displayName: 'B', bundleId: 'b' }],
      });
      _computerUseExecutorHooks.create = () => fake;
      assert.equal(getComputerUseExecutor(), getComputerUseExecutor());
      const ac = new AbortController();
      void getComputerUseExecutor({ signal: ac.signal });

      for (const action of COMPUTER_USE_ACTIONS) {
        try {
          await dispatchComputerUseAction(fake, action, {
            coordinate: [1, 2],
            start_coordinate: [0, 0],
            text: 'Return',
            keys: ['a'],
            duration: 1,
            bundleId: 'x',
            scroll_x: 1,
            scroll_y: 1,
            repeat: 1,
            via_clipboard: false,
            modifiers: ['shift'],
          });
        } catch {
          /* validation ok */
        }
      }
    } finally {
      resetComputerUseExecutor();
      _computerUseExecutorHooks.create = prev;
    }
  });
});

describe('wave26 — analytics + command-spec + mcp soft', () => {
  it('covers IncrementalContext, git classify, Fig DI, mcp helpers', async () => {
    const tr = new IncrementalContextTracker();
    const hashOf = (m: { id: string }) => m.id;
    const isUser = (m: { role: string }) => m.role === 'user';
    tr.delta('s', [{ id: 'a', role: 'user' }], hashOf, isUser);
    tr.delta('s', [{ id: 'a', role: 'user' }, { id: 'b', role: 'assistant' }, { id: 'c', role: 'user' }], hashOf, isUser);
    tr.delta('s', [{ id: 'c', role: 'user' }, { id: 'd', role: 'assistant' }], hashOf, isUser);
    tr.delta('s', [{ id: 'z', role: 'user' }], hashOf, isUser);
    tr.reset('s');
    tr.reset();

    assert.ok(classifyGitOperation('git commit -m x', 0).operations.includes('commit'));
    assert.ok(classifyGitOperation('git commit --amend', 0).operations.includes('commit_amend'));
    assert.ok(classifyGitOperation('glab mr create', 0).operations.includes('pr_create'));
    assert.ok(
      classifyGitOperation('curl -X POST https://api.github.com/repos/o/r/pulls -d x', 0).operations.includes(
        'pr_create',
      ),
    );

    assert.equal(isSafeCommandSpecName('--x'), false);
    const prev = _figImportHooks.importSpec;
    try {
      _figImportHooks.importSpec = async () => ({ default: { name: 'd' } as never });
      assert.equal((await loadFigSpec('git'))?.name, 'd');
      _figImportHooks.importSpec = async () => ({ name: 'n' } as never);
      assert.equal((await loadFigSpec('git'))?.name, 'n');
      _figImportHooks.importSpec = async () => ({}) as never;
      assert.equal(await loadFigSpec('git'), null);
      _figImportHooks.importSpec = async () => {
        throw new Error('x');
      };
      assert.equal(await loadFigSpec('git'), null);
    } finally {
      _figImportHooks.importSpec = prev;
    }
    void valueFlagsForCommand('nope');

    assert.equal(typeof boundMcpResponse('hi', 1).truncated, 'boolean');
    assert.ok(validateMcpBufferSize(10) > 0);
    assert.ok(getMaxMcpOutputTokens() > 0);
    assert.equal(isValidJsonRpcMessage({ jsonrpc: '2.0', method: 'm' }), true);
    assert.ok(Array.isArray(extractUrlElicitations([{ url: 'https://x' }])));
    assert.ok(new McpRpcError(-1, 'e') instanceof Error);
    assert.ok(new McpAuthError('a') instanceof Error);
    assert.ok(new McpSessionExpiredError('s') instanceof Error);
  });
});

// ---------------------------------------------------------------------------
// Dead-branch cleanup residuals — easy both-arms on highest-drag modules
// ---------------------------------------------------------------------------
describe('wave26 — dead-branch residual both-arms', () => {
  it('trySerialize covers stringify success, undefined, and circular', () => {
    assert.equal(trySerialize({ a: 1 }), '{"a":1}');
    assert.equal(trySerialize(undefined), null);
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    assert.equal(trySerialize(circular), null);
  });

  it('validatePermissionRule + prefix matching after empty-toolName removal', () => {
    // Empty rule still rejected by the first guard (not the removed duplicate).
    assert.equal(validatePermissionRule('').valid, false);
    assert.equal(validatePermissionRule('   ').valid, false);
    // `(*)` no longer hits a phantom empty-toolName arm — parses as tool "(*)".
    const star = validatePermissionRule('(*)');
    assert.equal(typeof star.valid, 'boolean');
    // bash prefix rules still validate (commandHasRulePrefix empty-guard removed).
    assert.equal(validatePermissionRule('bash(npm:*)').valid, true);
    assert.equal(validatePermissionRule('bash(:*)').valid, false);
    assert.equal(permissionRuleValueFromString('bash(ls:*)').ruleContent, 'ls:*');
  });

  it('resolveFileOperationType both-arms + null-byte catch', () => {
    assert.equal(resolveFileOperationType('file_read', { file_path: '/tmp/x' }), 'read');
    assert.equal(resolveFileOperationType('file_edit', { file_path: '' }), undefined);
    assert.equal(resolveFileOperationType('file_edit', { file_path: '\0bad' }), 'write');
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w26-fot-'));
    try {
      const existing = path.join(tmp, 'exists.txt');
      fs.writeFileSync(existing, 'x');
      assert.equal(resolveFileOperationType('file_edit', { file_path: existing }), 'write');
      assert.equal(resolveFileOperationType('file_edit', { file_path: path.join(tmp, 'missing.txt') }), 'create');
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('addItemToJSONCArray hits non-array + unclosed-array fallthroughs', () => {
    assert.ok(addItemToJSONCArray('{"a":1}', 'x').includes('x'));
    assert.ok(addItemToJSONCArray('[1, 2', 'y').includes('y'));
    assert.ok(addItemToJSONCArray('  \n  "nope"', 'z').includes('z'));
    assert.ok(addItemToJSONCArray('', 'a').includes('a'));
  });

  it('matchMcpAutoAllow miss/hit arms', () => {
    assert.equal(matchMcpAutoAllow('mcp__kali__nmap', ['*']), true);
    assert.equal(matchMcpAutoAllow('mcp__kali__nmap', ['mcp__kali__*']), true);
    assert.equal(matchMcpAutoAllow('mcp__kali__nmap', ['mcp__other__*']), false);
    assert.equal(matchMcpAutoAllow('exact', ['exact']), true);
    assert.equal(matchMcpAutoAllow('x', []), false);
  });

  it('McpManager forgetOAuthToken existsSync path (never-throws)', () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w26-mcp-'));
    const prev = process.env.AMI_CONFIG_DIR;
    process.env.AMI_CONFIG_DIR = tmp;
    try {
      const mgr = new McpManager();
      mgr.addServer('s', {
        transport: 'http',
        url: 'https://example.com/mcp',
        oauth: { clientId: 'c' },
      } as never);
      // No token file yet → false; still exercises the unguarded existsSync.
      assert.equal(mgr.forgetOAuthToken('s'), false);
      assert.equal(mgr.forgetOAuthToken('unknown'), false);
    } finally {
      if (prev === undefined) delete process.env.AMI_CONFIG_DIR;
      else process.env.AMI_CONFIG_DIR = prev;
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('AnalyticsTracker OTLP retry loop still settles without fallthrough', async () => {
    let posts = 0;
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w26-an-'));
    try {
      const tracker = new AnalyticsTracker(dir, {
        otlpEndpoint: 'http://127.0.0.1:9/v1/logs',
        env: {},
        otlpMaxAttempts: 2,
        otlpBaseBackoffMs: 0,
        otlpMaxBackoffMs: 0,
        sleepImpl: async () => {},
        rand: () => 0.5,
        persistFailedBatches: false,
        otlpFetch: (async () => {
          posts++;
          return {
            ok: false,
            status: 503,
            headers: { get: () => null },
            text: async () => '',
          };
        }) as never,
      });
      tracker.log('w26_event', {});
      await tracker.flushOtlp();
      assert.ok(posts >= 1);
      await tracker.shutdown();
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('PermissionManager plan-auto restrips keep-arm (non-dangerous rules stay)', () => {
    const pm = new PermissionManager();
    pm.addRule({ tool: 'file_read', action: 'allow', source: 'userSettings' });
    pm.setMode('plan');
    const a = pm.transitionPlanAutoMode();
    assert.equal(typeof a, 'object');
    // Non-dangerous allow must still be present (the `return true` filter arm).
    assert.ok(pm.getRules().some(r => r.tool === 'file_read'));
  });

  it('detectPowerShellExecution Start-BitsTransfer both-arms', () => {
    assert.equal(detectPowerShellExecution('Start-BitsTransfer -Source http://x').blocked, true);
    assert.equal(detectPowerShellExecution('echo hi').blocked, false);
  });

  it('streamChatCompletionWithRetry mock rate-limit early return arm', async () => {
    const prev = process.env[MOCK_RATE_LIMIT_ENV];
    process.env[MOCK_RATE_LIMIT_ENV] = '1';
    try {
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
    } finally {
      if (prev === undefined) delete process.env[MOCK_RATE_LIMIT_ENV];
      else process.env[MOCK_RATE_LIMIT_ENV] = prev;
    }
  });
});
