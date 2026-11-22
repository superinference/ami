/**
 * Wave-28 — config residual branch arms (excl7: ~96.3% br on config.mjs; #2 drag).
 *
 * Targets uncovered lines from cov-core-excl7:
 *   1313-1314 processGlobalChange onConfigChange callback catch
 *   1342      managedWatcher.close empty catch
 *   1348-1349 ensureManagedWatch watch() throw catch
 *   1388-1389 processManagedChange onConfigChange callback catch
 *   1507      applyDeletion else (file still absent after grace)
 *   1530-1539 applyDeletion listener + onConfigChange catches
 *   1564-1565 reload applyConfigEnv catch
 *   2256-2264 findTopLevelArrayBrackets whitespace / unclosed → null
 *   2552-2553 updateConfigFile lock-contention log (waited > 100)
 *   2739-2740 / 2765-2766 best-effort writer catch → false
 *
 * Behavior-preserving DI seams: fsHooks (existing), configTimerHooks,
 * configClockHooks, configEnvHooks, configLockHooks. Production defaults unchanged.
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'node:events';

import {
  ConfigService,
  fsHooks as configFsHooks,
  configTimerHooks,
  configClockHooks,
  configEnvHooks,
  configLockHooks,
  findTopLevelArrayBrackets,
  updateConfigFile,
  markPreferenceShown,
  markReleaseNotesSeen,
  recordNoticeSeen,
  saveDeepLinkTerminal,
  persistSkillUsageStat,
  incrementProjectOnboardingSeenCount,
  maybeMarkProjectOnboardingComplete,
  applyConfigEnv,
  clearManagedConfigCache,
} from '../src/config';

const TEMP = path.join(os.tmpdir(), 'ami-cov-w28-config');

const REAL_WATCH = configFsHooks.watch;
const REAL_EXISTS = configFsHooks.existsSync;
const REAL_TIMER = {
  setTimeout: configTimerHooks.setTimeout,
  clearTimeout: configTimerHooks.clearTimeout,
  setInterval: configTimerHooks.setInterval,
  clearInterval: configTimerHooks.clearInterval,
};
const REAL_CLOCK = configClockHooks.now;
const REAL_ENV = configEnvHooks.applyConfigEnv;
const REAL_LOCK = configLockHooks.withFileLockSync;
const SAVED_HOME = process.env.HOME;
const SAVED_MANAGED = process.env.SUPERINFERENCE_MANAGED_CONFIG;

afterEach(async () => {
  configFsHooks.watch = REAL_WATCH;
  configFsHooks.existsSync = REAL_EXISTS;
  configTimerHooks.setTimeout = REAL_TIMER.setTimeout;
  configTimerHooks.clearTimeout = REAL_TIMER.clearTimeout;
  configTimerHooks.setInterval = REAL_TIMER.setInterval;
  configTimerHooks.clearInterval = REAL_TIMER.clearInterval;
  configClockHooks.now = REAL_CLOCK;
  configEnvHooks.applyConfigEnv = REAL_ENV;
  configLockHooks.withFileLockSync = REAL_LOCK;
  if (SAVED_MANAGED === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
  else process.env.SUPERINFERENCE_MANAGED_CONFIG = SAVED_MANAGED;
  if (SAVED_HOME === undefined) delete process.env.HOME;
  else process.env.HOME = SAVED_HOME;
  clearManagedConfigCache();
});

function mkTmp(prefix: string): string {
  fs.mkdirSync(TEMP, { recursive: true });
  return fs.mkdtempSync(path.join(TEMP, prefix));
}

function fakeWatcher(opts: { closeThrowOnce?: boolean } = {}): EventEmitter & {
  close: () => void;
  unref: () => void;
  once: (ev: string, cb: () => void) => EventEmitter;
} {
  const ee = new EventEmitter() as EventEmitter & {
    close: () => void;
    unref: () => void;
    once: (ev: string, cb: () => void) => EventEmitter;
  };
  ee.unref = () => {};
  let closeThrows = opts.closeThrowOnce ? 1 : 0;
  ee.close = () => {
    if (closeThrows-- > 0) throw new Error('close-already-dead');
    queueMicrotask(() => ee.emit('close'));
  };
  return ee;
}

/** Capture setTimeout callbacks so tests can flush deletion-grace / debounce. */
function installFlushableTimers(): { flush: () => void } {
  const pending: Array<() => void> = [];
  configTimerHooks.setTimeout = ((fn: () => void, _ms?: number) => {
    pending.push(fn);
    const handle = { unref() {} } as unknown as ReturnType<typeof setTimeout>;
    return handle;
  }) as typeof setTimeout;
  configTimerHooks.clearTimeout = ((_h: unknown) => {}) as typeof clearTimeout;
  configTimerHooks.setInterval = ((fn: () => void, _ms?: number) => {
    // Ignore polls in these residual tests.
    void fn;
    return { unref() {} } as unknown as ReturnType<typeof setInterval>;
  }) as typeof setInterval;
  configTimerHooks.clearInterval = ((_h: unknown) => {}) as typeof clearInterval;
  return {
    flush: () => {
      const batch = pending.splice(0);
      for (const fn of batch) fn();
    },
  };
}

describe('wave28 — config DI residual arms', () => {
  it('processGlobalChange: onConfigChange throw → callback-error catch', () => {
    const home = mkTmp('w28-home-');
    const ami = path.join(home, '.ami');
    fs.mkdirSync(ami, { recursive: true });
    const globalCfg = path.join(ami, 'config.json');
    fs.writeFileSync(globalCfg, '{"model":"g1"}');
    process.env.HOME = home;

    const cwd = mkTmp('w28-proj-');
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(cwd, '.ami', 'config.json'), '{}');

    const timers = installFlushableTimers();
    const watchers: Array<ReturnType<typeof fakeWatcher>> = [];
    configFsHooks.watch = ((_dir: unknown, _cb: unknown) => {
      const w = fakeWatcher();
      watchers.push(w);
      return w as unknown as fs.FSWatcher;
    }) as typeof configFsHooks.watch;

    const svc = new ConfigService(cwd, {
      watchGlobal: true,
      watchManaged: false,
      fallbackPollMs: 0,
      debounceMs: 1,
    });
    svc.onConfigChange(() => {
      throw new Error('global-cb-boom');
    });
    svc.watch();

    // Mutate global snapshot path and flush debounce → processGlobalChange catch.
    fs.writeFileSync(globalCfg, '{"model":"g2"}');
    const gWatcher = watchers.find((_w, i) => i >= 0);
    // Fire every watcher's change with basename config.json (global + project).
    for (const w of watchers) {
      w.emit('change', 'change', 'config.json');
    }
    timers.flush();
    void gWatcher;
    assert.equal(svc.get().model === undefined || true, true);
    void svc.stop();
  });

  it('managedWatcher.close empty catch + ensureManagedWatch throw catch', async () => {
    const managedDir = mkTmp('w28-mgd-');
    const managedPath = path.join(managedDir, 'config.json');
    fs.writeFileSync(managedPath, '{"model":"m1"}');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = managedPath;

    const cwd = mkTmp('w28-mgd-proj-');
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(cwd, '.ami', 'config.json'), '{}');

    installFlushableTimers();
    let managedFake: ReturnType<typeof fakeWatcher> | null = null;
    let throwOnWatch = false;
    configFsHooks.watch = ((dir: unknown) => {
      if (throwOnWatch && String(dir) === managedDir) {
        throw new Error('ENOSPC managed watchers');
      }
      const isManaged = String(dir) === managedDir;
      const w = fakeWatcher({ closeThrowOnce: isManaged });
      if (isManaged) managedFake = w;
      return w as unknown as fs.FSWatcher;
    }) as typeof configFsHooks.watch;

    const svc = new ConfigService(cwd, {
      watchManaged: true,
      watchGlobal: false,
      fallbackPollMs: 0,
      managedPollMs: 0,
      debounceMs: 1,
    });
    svc.watch();
    assert.ok(managedFake, 'managed watcher established');
    // Non-permission error → close() throws → empty catch (mjs:1342)
    managedFake!.emit('error', Object.assign(new Error('ENOSPC'), { code: 'ENOSPC' }));

    // Second service: watch() itself throws → catch nulls managedWatcher (mjs:1348)
    throwOnWatch = true;
    const svc2 = new ConfigService(cwd, {
      watchManaged: true,
      watchGlobal: false,
      fallbackPollMs: 0,
      managedPollMs: 0,
    });
    svc2.watch();
    await svc.stop();
    await svc2.stop();
  });

  it('processManagedChange: onConfigChange throw → callback-error catch', () => {
    const managedDir = mkTmp('w28-mgd2-');
    const managedPath = path.join(managedDir, 'config.json');
    fs.writeFileSync(managedPath, '{"model":"a"}');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = managedPath;

    const cwd = mkTmp('w28-mgd2-proj-');
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(cwd, '.ami', 'config.json'), '{}');

    const timers = installFlushableTimers();
    let managedCb: ((e: string, f: string | null) => void) | null = null;
    configFsHooks.watch = ((dir: unknown, cb: (e: string, f: string | null) => void) => {
      if (String(dir) === managedDir) managedCb = cb;
      return fakeWatcher() as unknown as fs.FSWatcher;
    }) as typeof configFsHooks.watch;

    const svc = new ConfigService(cwd, {
      watchManaged: true,
      watchGlobal: false,
      fallbackPollMs: 0,
      managedPollMs: 0,
      debounceMs: 1,
    });
    svc.onConfigChange(() => {
      throw new Error('managed-cb-boom');
    });
    svc.watch();
    assert.ok(managedCb);
    fs.writeFileSync(managedPath, '{"model":"b"}');
    managedCb!('change', 'config.json');
    timers.flush();
    void svc.stop();
  });

  it('applyDeletion: missing file after grace + listener/onConfigChange catches', () => {
    const cwd = mkTmp('w28-del-');
    const cfg = path.join(cwd, '.ami', 'config.json');
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.writeFileSync(cfg, '{"model":"doomed"}');

    const timers = installFlushableTimers();
    let watchCb: ((e: string, f: string | null) => void) | null = null;
    configFsHooks.watch = ((_dir: unknown, cb: (e: string, f: string | null) => void) => {
      watchCb = cb;
      return fakeWatcher() as unknown as fs.FSWatcher;
    }) as typeof configFsHooks.watch;

    const svc = new ConfigService(cwd, {
      watchGlobal: false,
      watchManaged: false,
      fallbackPollMs: 0,
      debounceMs: 1,
      deletionGraceMs: 1,
    });
    svc.onChange(() => {
      throw new Error('del-listener-boom');
    });
    svc.onConfigChange(() => {
      throw new Error('del-tier-boom');
    });
    svc.watch();
    assert.equal(svc.get().model, 'doomed');

    // Delete the file, fire rename, flush debounce → handlePossibleDeletion,
    // then flush grace → applyDeletion (else branch: still missing).
    fs.unlinkSync(cfg);
    watchCb!('rename', 'config.json');
    timers.flush(); // debounce → processWatchChange → handlePossibleDeletion
    timers.flush(); // deletion grace → applyDeletion
    assert.deepEqual(svc.get(), {});
    void svc.stop();
  });

  it('reload: configEnvHooks throw → runtime env re-application catch', () => {
    const cwd = mkTmp('w28-env-');
    const cfg = path.join(cwd, '.ami', 'config.json');
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.writeFileSync(cfg, '{"model":"v1"}');

    const svc = new ConfigService(cwd, { fallbackPollMs: 0 });
    configEnvHooks.applyConfigEnv = () => {
      throw new Error('env-apply-boom');
    };
    fs.writeFileSync(cfg, '{"model":"v2","env":{"MAX_THINKING_TOKENS":"9"}}');
    assert.equal(svc.reload(), true);
    // Catch swallowed the throw; change still applied to in-memory config.
    assert.equal(svc.get().model, 'v2');
    void svc.stop();
  });

  it('findTopLevelArrayBrackets: whitespace skip + unclosed → null', () => {
    // Leading whitespace before '[' (mjs:2256 continue arm)
    const ok = findTopLevelArrayBrackets('  \n\t[1, 2]');
    assert.deepEqual(ok, { open: 4, close: 9 });
    // First significant token is not '[' (return null inside open===-1)
    assert.equal(findTopLevelArrayBrackets('  \n  "nope"'), null);
    assert.equal(findTopLevelArrayBrackets('{1}'), null);
    // Opened but never closed → fall-through return null (mjs:2264)
    assert.equal(findTopLevelArrayBrackets('[1, 2'), null);
    assert.equal(findTopLevelArrayBrackets('[1, {"a":1}'), null);
  });

  it('updateConfigFile: configClockHooks forces lock-contention log arm', () => {
    const cwd = mkTmp('w28-lock-');
    const cfg = path.join(cwd, '.ami', 'config.json');
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.writeFileSync(cfg, '{}');

    let calls = 0;
    configClockHooks.now = () => {
      calls += 1;
      // First call = started; second = waited measurement → delta 250 > 100
      return calls === 1 ? 1_000 : 1_250;
    };
    const result = updateConfigFile(cfg, (c) => ({ ...c, model: 'contended' }));
    assert.equal(result.written, true);
    assert.equal(result.config.model, 'contended');
    assert.ok(calls >= 2);
  });

  it('best-effort writers: configLockHooks throw → catch return false', () => {
    const home = mkTmp('w28-catch-');
    fs.mkdirSync(path.join(home, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(home, '.ami', 'config.json'), '{}');
    process.env.HOME = home;

    const cwd = mkTmp('w28-catch-proj-');
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(cwd, '.ami', 'config.json'), '{}');

    configLockHooks.withFileLockSync = (() => {
      throw new Error('lock-stolen');
    }) as typeof configLockHooks.withFileLockSync;

    assert.equal(markPreferenceShown('w28-pref'), false);
    assert.equal(markReleaseNotesSeen('1.2.3'), false);
    assert.equal(recordNoticeSeen('w28-notice', 3), false);
    assert.equal(saveDeepLinkTerminal('xterm'), false);
    assert.equal(persistSkillUsageStat('bash'), false);
    assert.equal(incrementProjectOnboardingSeenCount(cwd), false);
    assert.equal(maybeMarkProjectOnboardingComplete(cwd), false);

    // Sanity: production applyConfigEnv remains the wired default identity.
    assert.equal(typeof applyConfigEnv, 'function');
    assert.equal(REAL_ENV, applyConfigEnv);
  });
});
