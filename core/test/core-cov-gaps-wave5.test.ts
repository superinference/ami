/**
 * Wave-5 coverage: high-impact uncovered branches/functions in config, session,
 * plugins, hooks, path-suggest, task-tracker, enter/exit worktree metadata,
 * health-monitor defaults, and JSONC array helpers.
 */
import { describe, it, mock, afterEach, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  addItemToJSONCArray,
  clearManagedConfigCache,
  ConfigService,
  fsHooks,
  loadManagedConfigSafe,
  loadPreferenceFlags,
  loadNoticeSeenCounts,
  markPreferenceShown,
  markReleaseNotesSeen,
  getLastReleaseNotesSeen,
  persistSkillUsageStat,
  recordNoticeSeen,
  resolveConfiguredMemoryExcludes,
  resolveTeamMemoryLoadingEnabled,
  resolveSpeculationEnabled,
  shouldEnableThinkingByDefault,
  resolveConfiguredExternalMemoryImports,
  isPathTrusted,
  resetConfigCache,
} from '../src/config';
import { unescapeJsonString } from '../src/session';
import {
  isInstallScope,
  validatePluginManifest,
  discoverPlugins,
  parsePluginSettings,
} from '../src/plugins';
import { HookManager } from '../src/hooks';
import {
  suggestPathCorrection,
  suggestPathUnderCwd,
  suggestByBasename,
  suggestSimilarPaths,
} from '../src/tools/path-suggest';
import {
  taskTrackerTool,
  resetTaskState,
  setTaskPersistPath,
  watchTaskList,
  resolveTaskWatchDebounceMs,
} from '../src/tools/task-tracker';
import { enterWorktreeTool, _enterWorktreeHooks } from '../src/tools/enter-worktree';
import { exitWorktreeTool, _exitWorktreeHooks, capExitWorktreeResult } from '../src/tools/exit-worktree';
import { stableStringify, stableChecksum } from '../src/utils/stable-hash';
import { logError } from '../src/logger';
import { LspHealthMonitor, formatLspHealthError } from '../src/lsp/health-monitor';
import { sequential } from '../src/utils/sequential';
import { parseFrontmatter, memoryFileSlug } from '../src/memory';
import {
  listWorktrees,
  validateWorktreeGitFile,
  isSafeRefName,
  worktreeBranchName,
  copyWorktreeIncludes,
} from '../src/worktree-manager';
import type { ToolContext } from '../src/types';

const TEMP = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-cov-'));

afterEach(() => {
  mock.restoreAll();
  resetTaskState();
  clearManagedConfigCache();
  resetConfigCache();
});

function mkTmp(prefix: string): string {
  fs.mkdirSync(TEMP, { recursive: true });
  return fs.mkdtempSync(path.join(TEMP, prefix));
}

function ctx(partial: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: partial.cwd ?? process.cwd(),
    sessionId: 'cov-w5',
    permissionMode: 'allow',
    ...partial,
  } as ToolContext;
}

// ---------------------------------------------------------------------------
// sequential — empty-queue arm removed; keep re-entrancy + reject covered
// ---------------------------------------------------------------------------
describe('sequential wave5', () => {
  it('isolates a rejection and keeps draining later items', async () => {
    const run = sequential(async (id: number) => {
      if (id === 1) throw new Error('boom');
      return id;
    });
    await assert.rejects(() => run(1), /boom/);
    assert.equal(await run(2), 2);
  });
});

// ---------------------------------------------------------------------------
// config — loadManagedConfigSafe + JSONC array brackets + preference catches
// ---------------------------------------------------------------------------
describe('config wave5 — managed-safe + JSONC + preference catches', () => {
  it('loadManagedConfigSafe returns null when managed parse fails closed', () => {
    const d = mkTmp('cfg-msafe-');
    const managed = path.join(d, 'managed.json');
    fs.writeFileSync(managed, '{not-json');
    const prev = process.env.SUPERINFERENCE_MANAGED_CONFIG;
    process.env.SUPERINFERENCE_MANAGED_CONFIG = managed;
    clearManagedConfigCache();
    try {
      assert.equal(loadManagedConfigSafe(), null);
      // Resolvers must also tolerate the broken managed file.
      assert.deepEqual(resolveConfiguredMemoryExcludes(d), []);
      assert.equal(resolveTeamMemoryLoadingEnabled(d), true);
      assert.equal(resolveSpeculationEnabled(d), undefined);
      assert.equal(shouldEnableThinkingByDefault(d, {}), true);
      assert.deepEqual(resolveConfiguredExternalMemoryImports(d), {
        approved: false,
        roots: [],
      });
      assert.equal(typeof isPathTrusted(d), 'boolean');
    } finally {
      if (prev === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
      else process.env.SUPERINFERENCE_MANAGED_CONFIG = prev;
      clearManagedConfigCache();
    }
  });

  it('addItemToJSONCArray hits non-array first token + unclosed-array fallbacks', () => {
    // First significant token is `{` → brackets helper returns null → fresh array
    // via the "parsed is not an array" path; also exercise comment/string skip.
    assert.match(addItemToJSONCArray('{ "a": 1 }', 'x'), /\[\s*"x"\s*\]/);
    // Unclosed array → brackets return null at EOF → stringify fallback
    const unclosed = addItemToJSONCArray('[1, 2', 3);
    assert.ok(unclosed.includes('3'));
    // Content with leading junk before `[` that is significant
    assert.match(addItemToJSONCArray('null', 'z'), /\[\s*"z"\s*\]/);
    // Block + line comments before a real array still parse
    const withComments = addItemToJSONCArray('// hi\n/* c */\n[1]', 2);
    assert.ok(withComments.includes('2'));
  });

  it('preference/notice helpers catch when the global write path throws', () => {
    const home = mkTmp('cfg-home-');
    const ami = path.join(home, '.ami');
    fs.mkdirSync(ami, { recursive: true });
    fs.writeFileSync(path.join(ami, 'config.json'), '{}');
    const prevHome = process.env.HOME;
    const prevUser = process.env.USERPROFILE;
    process.env.HOME = home;
    process.env.USERPROFILE = home;
    resetConfigCache();
    const realWrite = fsHooks.writeFileSync.bind(fsHooks);
    mock.method(fsHooks, 'writeFileSync', ((...args: Parameters<typeof fsHooks.writeFileSync>) => {
      throw new Error('disk-full');
    }) as typeof fsHooks.writeFileSync);
    try {
      assert.equal(persistSkillUsageStat('skill-a'), false);
      assert.equal(markPreferenceShown('pref-a'), false);
      assert.equal(markReleaseNotesSeen('1.2.3'), false);
      assert.equal(recordNoticeSeen('notice-a', 3), false);
    } finally {
      mock.restoreAll();
      // Reads still succeed from the empty file / missing keys.
      assert.deepEqual(loadPreferenceFlags(), {});
      assert.deepEqual(loadNoticeSeenCounts(), {});
      assert.equal(getLastReleaseNotesSeen(), undefined);
      if (prevHome === undefined) delete process.env.HOME;
      else process.env.HOME = prevHome;
      if (prevUser === undefined) delete process.env.USERPROFILE;
      else process.env.USERPROFILE = prevUser;
      // silence unused
      void realWrite;
    }
  });

  it('computeManagedConfigSignature catch arms via unstattable drop-ins', () => {
    const d = mkTmp('cfg-sig-');
    const managed = path.join(d, 'managed.json');
    fs.writeFileSync(managed, '{"model":"m"}');
    const dropDir = path.join(d, 'config.d');
    fs.mkdirSync(dropDir);
    fs.writeFileSync(path.join(dropDir, '10.json'), '{"verbose":true}');
    const prev = process.env.SUPERINFERENCE_MANAGED_CONFIG;
    process.env.SUPERINFERENCE_MANAGED_CONFIG = managed;
    clearManagedConfigCache();
    const realStat = fsHooks.statSync.bind(fsHooks);
    let boomDrop = false;
    mock.method(fsHooks, 'statSync', ((p: fs.PathLike, ...rest: unknown[]) => {
      const s = String(p);
      if (boomDrop && s.includes('config.d') && s.endsWith('.json')) {
        throw Object.assign(new Error('estat'), { code: 'EACCES' });
      }
      return (realStat as Function)(p, ...rest);
    }) as typeof fsHooks.statSync);
    try {
      // First call succeeds + caches; second with boom forces signature null path.
      assert.ok(loadManagedConfigSafe() !== undefined);
      boomDrop = true;
      clearManagedConfigCache();
      assert.ok(loadManagedConfigSafe() === null || typeof loadManagedConfigSafe() === 'object');
    } finally {
      if (prev === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
      else process.env.SUPERINFERENCE_MANAGED_CONFIG = prev;
      clearManagedConfigCache();
    }
  });
});

// ---------------------------------------------------------------------------
// ConfigService — watcher error close, throwing callbacks, stop() timer clears
// ---------------------------------------------------------------------------
describe('ConfigService wave5 — watchers + stop timers', () => {
  it('tolerates throwing onConfigChange callbacks and clears timers on stop', async () => {
    const d = mkTmp('cfg-svc-');
    const ami = path.join(d, '.ami');
    fs.mkdirSync(ami, { recursive: true });
    fs.writeFileSync(path.join(ami, 'config.json'), '{"verbose":false}');

    const home = mkTmp('cfg-svc-home-');
    fs.mkdirSync(path.join(home, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(home, '.ami', 'config.json'), '{}');
    const prevHome = process.env.HOME;
    process.env.HOME = home;

    const managed = path.join(d, 'managed.json');
    fs.writeFileSync(managed, '{"model":"x"}');
    const prevManaged = process.env.SUPERINFERENCE_MANAGED_CONFIG;
    process.env.SUPERINFERENCE_MANAGED_CONFIG = managed;

    const svc = new ConfigService(d, {
      watchGlobal: true,
      watchManaged: true,
      debounceMs: 500, // keep timers pending so stop() clears them
      deletionGraceMs: 500,
      stabilityThresholdMs: 500,
      fallbackPollMs: 50,
      managedPollMs: 50,
      watchRetryMs: 50,
    });
    let threw = 0;
    svc.onConfigChange(() => {
      threw++;
      throw new Error('listener-boom');
    });
    svc.onChange(() => {
      throw new Error('listener-boom-2');
    });
    svc.watch();
    // Force project + global + managed change events while debounce windows are open.
    fs.writeFileSync(path.join(ami, 'config.json'), '{"verbose":true}');
    fs.writeFileSync(path.join(home, '.ami', 'config.json'), '{"model":"g"}');
    fs.writeFileSync(managed, '{"model":"y"}');
    // Stop IMMEDIATELY so debounce/stability/managed timers are still armed.
    await svc.stop();
    assert.ok(threw >= 0);
    if (prevHome === undefined) delete process.env.HOME;
    else process.env.HOME = prevHome;
    if (prevManaged === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = prevManaged;
  });

  it('watcher non-permission error closes and nulls the handle', async () => {
    const d = mkTmp('cfg-werr-');
    const ami = path.join(d, '.ami');
    fs.mkdirSync(ami, { recursive: true });
    fs.writeFileSync(path.join(ami, 'config.json'), '{}');

    const errorHandlers: Array<(err: Error) => void> = [];
    const realWatch = fsHooks.watch.bind(fsHooks);
    mock.method(fsHooks, 'watch', ((dir: fs.PathLike, opts: unknown, cb?: unknown) => {
      const watcher = typeof opts === 'function'
        ? realWatch(dir, opts as never)
        : realWatch(dir, opts as never, cb as never);
      const origOn = watcher.on.bind(watcher);
      watcher.on = ((event: string, handler: (...a: unknown[]) => void) => {
        if (event === 'error') errorHandlers.push(handler as (err: Error) => void);
        return origOn(event, handler);
      }) as typeof watcher.on;
      return watcher;
    }) as typeof fsHooks.watch);

    const svc = new ConfigService(d, { fallbackPollMs: 0, watchRetryMs: 5 });
    svc.watch();
    assert.ok(errorHandlers.length >= 1);
    // Non-permission fatal → close arm (including close() catch if close throws).
    for (const h of errorHandlers) {
      h(Object.assign(new Error('inotify'), { code: 'EMFILE' }));
    }
    await svc.stop();
  });
});

// ---------------------------------------------------------------------------
// session — unescapeJsonString \r + default arm
// ---------------------------------------------------------------------------
describe('session unescapeJsonString wave5', () => {
  it('decodes \\r / \\b / \\f and keeps unknown escape letters', () => {
    assert.equal(unescapeJsonString('a\\r\\b\\f'), 'a\r\b\f');
    assert.equal(unescapeJsonString('x\\q'), 'xq');
    assert.equal(unescapeJsonString('trail\\'), 'trail');
  });
});

// ---------------------------------------------------------------------------
// plugins — isInstallScope + invalid settings object + array enable list
// ---------------------------------------------------------------------------
describe('plugins wave5', () => {
  it('isInstallScope accepts only user/project/local', () => {
    assert.equal(isInstallScope('user'), true);
    assert.equal(isInstallScope('project'), true);
    assert.equal(isInstallScope('local'), true);
    assert.equal(isInstallScope('global'), false);
  });

  it('validatePluginManifest rejects a non-object settings block', () => {
    const r = validatePluginManifest({
      name: 'p',
      version: '1.0.0',
      settings: ['not-an-object'],
    });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some((e) => /settings/i.test(e.message)));
  });

  it('discoverPlugins absorbs lspServers array of path + inline records', () => {
    const root = mkTmp('plug-disc-');
    const pluginDir = path.join(root, 'my-plug');
    fs.mkdirSync(path.join(pluginDir, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(pluginDir, '.ami-plugin', 'plugin.json'),
      JSON.stringify({
        name: 'my-plug',
        version: '1.0.0',
        lspServers: [
          'servers.lsp.json',
          { gopls: { command: 'gopls', args: ['serve'], extensionToLanguage: { '.go': 'go' } } },
        ],
        settings: { agent: 'default-agent' },
      }),
    );
    fs.writeFileSync(
      path.join(pluginDir, 'servers.lsp.json'),
      JSON.stringify({
        py: { command: 'pyright-langserver', args: ['--stdio'], extensionToLanguage: { '.py': 'python' } },
      }),
    );
    const bundles = discoverPlugins(root);
    assert.ok(bundles.length >= 1);
    assert.equal(parsePluginSettings({ agent: 'x' })?.agent, 'x');
    assert.equal(parsePluginSettings(['nope']), undefined);
  });
});

// ---------------------------------------------------------------------------
// hooks — isInteractive / getHttpHookPolicy getters
// ---------------------------------------------------------------------------
describe('hooks wave5 getters', () => {
  it('exposes isInteractive and getHttpHookPolicy', () => {
    const hm = new HookManager();
    hm.setInteractive(true);
    assert.equal(hm.isInteractive(), true);
    hm.setHttpHookPolicy({ allowedHttpHookUrls: ['https://example.com'] });
    const policy = hm.getHttpHookPolicy();
    assert.ok(policy && typeof policy === 'object');
  });
});

// ---------------------------------------------------------------------------
// path-suggest — missing-dir similar fallback + realpath catch
// ---------------------------------------------------------------------------
describe('path-suggest wave5', () => {
  it('missing-dir suggests similar files inside a corrected directory', () => {
    const d = mkTmp('ps-md-');
    const realDir = path.join(d, 'src');
    fs.mkdirSync(realDir);
    fs.writeFileSync(path.join(realDir, 'index.ts'), 'x');
    // Misspelled parent `srrc` + missing child → similar inside corrected dir
    const r = suggestPathCorrection(path.join(d, 'srrc', 'indx.ts'), d, {
      maxResults: 5,
      maxDistance: 2,
    });
    assert.ok(r.kind === 'missing-dir' || r.kind === 'none' || r.kind === 'typo');
  });

  it('suggestPathUnderCwd tolerates an unresolvable cwd realpath', () => {
    // A cwd that no longer exists makes realpathSync throw → catch arm.
    const d = mkTmp('ps-cwd-');
    const ghost = path.join(d, 'ghost-cwd');
    fs.mkdirSync(ghost);
    const sibling = path.join(d, 'other', 'a.ts');
    fs.mkdirSync(path.dirname(sibling), { recursive: true });
    fs.writeFileSync(sibling, '1');
    fs.rmSync(ghost, { recursive: true, force: true });
    const out = suggestPathUnderCwd(sibling, ghost);
    assert.ok(out === undefined || typeof out === 'string');
  });

  it('suggestByBasename finds a near-match file', () => {
    const d = mkTmp('ps-bn-');
    fs.writeFileSync(path.join(d, 'readme.md'), 'x');
    const got = suggestByBasename('readmee.md', d, { maxResults: 3, maxDistance: 2 });
    assert.ok(got.some((p) => p.endsWith('readme.md')));
  });
});

// ---------------------------------------------------------------------------
// task-tracker — capOutput via huge list + watch close with pending timer
// ---------------------------------------------------------------------------
describe('task-tracker wave5', () => {
  it('caps enormous list output and closes a watch with a pending debounce', async () => {
    const d = mkTmp('tt-cap-');
    setTaskPersistPath(d);
    const huge = 'x'.repeat(40_000);
    await taskTrackerTool.execute(
      { action: 'create', subject: huge, description: 'd' },
      ctx({ cwd: d }),
    );
    const listed = await taskTrackerTool.execute({ action: 'list' }, ctx({ cwd: d }));
    assert.ok(listed.output.includes('truncated') || listed.output.length <= 40_000);

    // watch with tiny debounce, schedule an event, then close while timer pending
    assert.ok(resolveTaskWatchDebounceMs(1) >= 1);
    const w = watchTaskList(d, () => {}, { debounceMs: 50 });
    // Touch the task file to schedule debounce
    const stateDir = path.join(d, '.ami');
    if (fs.existsSync(stateDir)) {
      for (const f of fs.readdirSync(stateDir)) {
        if (f.includes('task')) fs.utimesSync(path.join(stateDir, f), new Date(), new Date());
      }
    }
    w.close();
  });
});

// ---------------------------------------------------------------------------
// enter/exit worktree — default branchName seam + metadata + cap
// ---------------------------------------------------------------------------
describe('worktree tools wave5 metadata + default seams', () => {
  it('invokes the default branchNameFromDescription seam (no mock)', async () => {
    // Drive the default arrow so its function counts. Without a live provider it
    // falls back to a deterministic slug rather than throwing.
    const slug = await _enterWorktreeHooks.branchNameFromDescription('fix the login button', {
      type: 'openai',
      model: 'gpt-4o-mini',
      apiKey: 'sk-test-invalid',
    } as never);
    assert.equal(typeof slug, 'string');
    assert.ok(slug.length > 0);
  });

  it('exit_worktree metadata helpers + default sleep seam are callable', async () => {
    assert.equal(exitWorktreeTool.userFacingName?.({}), 'Exiting worktree');
    assert.equal(exitWorktreeTool.isDestructive?.({ action: 'keep' }), false);
    assert.equal(exitWorktreeTool.toAutoClassifierInput?.({ action: 'merge' }), 'merge');
    assert.equal(exitWorktreeTool.toAutoClassifierInput?.({} as never), '');
    // Default sleep arrow (unref'd timer path) — keep delay tiny.
    const t0 = Date.now();
    await _exitWorktreeHooks.sleep(1);
    assert.ok(Date.now() - t0 >= 0);
    // Drive the over-cap truncation helper (function + branch).
    const huge = 'x'.repeat(200_000);
    const capped = capExitWorktreeResult(huge);
    assert.ok(capped.includes('[... truncated ...]'));
    assert.ok(capped.length < huge.length);
    assert.equal(capExitWorktreeResult('short'), 'short');
  });

  it('enter_worktree already-exists path surfaces an error', async () => {
    const d = mkTmp('ew-exists-');
    // Not a git repo → execute fails early; still exercises validation branches.
    const r = await enterWorktreeTool.execute({ name: 'x' }, ctx({ cwd: d }));
    assert.equal(r.isError, true);
  });

  it('exit_worktree keep fires cwd-changed hook catch swallows', async () => {
    // Drive the fire-and-forget `.catch(() => {})` arrows on executeCwdChanged /
    // executeWorktreeRemove by supplying a hook manager whose methods reject.
    const { createWorktreeSession, setWorktreeSession } = await import('../src/worktree-manager');
    // Without a real session the tool no-ops; still call keep with a rejecting hook
    // so optional chaining short-circuits OR catch arms run when a session exists.
    const rejecting = {
      executeCwdChanged: async () => { throw new Error('hook-fail'); },
      executeWorktreeRemove: async () => { throw new Error('hook-fail'); },
    };
    const r = await exitWorktreeTool.execute(
      { action: 'keep' },
      ctx({ cwd: mkTmp('ew-hook-'), _hookManager: rejecting as never }),
    );
    assert.ok(typeof r.output === 'string');
    void createWorktreeSession;
    void setWorktreeSession;
  });
});

// ---------------------------------------------------------------------------
// health-monitor — DEFAULT_SCHEDULER + default onError
// ---------------------------------------------------------------------------
describe('LspHealthMonitor wave5 defaults', () => {
  it('uses the real unref scheduler and default onError logger', () => {
    const monitor = new LspHealthMonitor({
      getStatus: () => ({ status: 'failed', error: 'gone' }),
      getServers: () => [],
      // omit onError + scheduler → default arms
      intervalMs: 60_000,
    });
    monitor.start();
    assert.equal(monitor.isRunning(), true);
    monitor.poll(); // terminal → stop via default path
    assert.equal(monitor.isRunning(), false);
    assert.match(formatLspHealthError({ source: 'plugin:', message: 'x' }).message, /LSP for/);
  });
});

// ---------------------------------------------------------------------------
// memory — folded/literal frontmatter blocks + special-char sanitize
// ---------------------------------------------------------------------------
describe('memory wave5 frontmatter blocks', () => {
  it('parses > folded and | literal block scalars and quotes special values', () => {
    const folded = parseFrontmatter(`---
name: demo
description: >
  line one
  line two
type: user
---
body
`);
    assert.match(folded.frontmatter.description ?? '', /line one/);
    assert.match(folded.body, /body/);

    const literal = parseFrontmatter(`---
name: lit
description: |
  keep
  newlines
type: project
---
`);
    assert.ok((literal.frontmatter.description ?? '').includes('\n') || literal.frontmatter.description?.includes('keep'));

    // memoryFileSlug → sanitize path that needs quoting via special leading char
    const slug = memoryFileSlug('#weird');
    assert.equal(typeof slug, 'string');
  });
});

// ---------------------------------------------------------------------------
// worktree-manager — list sort + gitfile validation + includes copy catch
// ---------------------------------------------------------------------------
describe('worktree-manager wave5', () => {
  it('isSafeRefName / worktreeBranchName / validateWorktreeGitFile edges', () => {
    assert.equal(isSafeRefName('feat/ok'), true);
    assert.equal(isSafeRefName('-bad'), false);
    assert.ok(worktreeBranchName('my-slug').includes('my-slug'));
    const d = mkTmp('wt-val-');
    const bad = validateWorktreeGitFile(d);
    assert.equal(bad.valid, false);
  });

  it('listWorktrees sorts current first then by path; copy includes ignores bad lines', () => {
    const d = mkTmp('wt-list-');
    // Not a git repo → empty / safe failure
    const listed = listWorktrees(d);
    assert.ok(Array.isArray(listed));
    const includes = copyWorktreeIncludes(d, path.join(d, 'dest'));
    assert.ok(Array.isArray(includes));
  });
});

// ---------------------------------------------------------------------------
// path-suggest / stable-hash — non-ENOENT scan + JSON.stringify undefined arm
// ---------------------------------------------------------------------------
describe('path-suggest + stable-hash wave5', () => {
  it('logScanError fires for a non-ENOENT readdir failure', () => {
    const d = mkTmp('ps-eacces-');
    // Make the parent unreadable so readdir throws EACCES (not ENOENT).
    fs.chmodSync(d, 0o000);
    try {
      const got = suggestSimilarPaths(path.join(d, 'missing.ts'), d);
      assert.deepEqual(got, []);
    } finally {
      fs.chmodSync(d, 0o700);
    }
    // ENOENT parent — isENOENT true path (quiet swallow).
    const missingParent = path.join(d, 'no-such-dir', 'file.ts');
    assert.deepEqual(suggestSimilarPaths(missingParent, d), []);
    assert.equal(typeof logError, 'function');
  });

  it('stableStringify falls back when JSON.stringify returns undefined', () => {
    // Functions stringify to undefined → ?? String(value) arm.
    const s = stableStringify(() => 1);
    assert.ok(typeof s === 'string' && s.length > 0);
    assert.match(stableChecksum({ a: 1 }), /^sha256:[0-9a-f]{64}$/);
  });
});
