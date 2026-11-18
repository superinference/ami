// U010 / w116 — ConfigService watcher per-path permission-error tolerance.
//
// Parity: Claude Code's changeDetector chokidar `ignorePermissionErrors: true`.
// A per-path EACCES/EPERM (an unreadable sibling/drop-in in the watched directory)
// must NOT tear the whole watcher down — the handle still covers the rest of the dir,
// so reacting by close()+re-establish would churn (and, on a persistently-unreadable
// path, loop) while dropping live reload for every other file. A non-permission error
// (inotify exhaustion / a dead handle) stays fatal and re-establishes as before.
//
// The tests drive the real watcher `error` dispatcher by capturing every error handler
// registered on an injected FSWatcher (one per watched tier), so behavior is checked
// deterministically without needing a real unreadable file.

import { describe, it, mock, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  ConfigService,
  fsHooks as configFsHooks,
  isWatcherPermissionError,
} from '../src/config';

type ErrHandler = (err?: unknown) => void;

/**
 * Mount a ConfigService and capture, per watched directory, the `error` handler it
 * registers on the injected fake FSWatcher plus whether that watcher was closed.
 */
function withCapturedWatchers(svc: ConfigService): {
  handlers: Array<{ dir: string; fire: ErrHandler; closed: () => boolean }>;
} {
  const handlers: Array<{ dir: string; fire: ErrHandler; closed: () => boolean }> = [];
  const m = mock.method(
    configFsHooks,
    'watch',
    (dir: unknown, _cb: (e: string, f: string | null) => void) => {
      let errorHandler: ErrHandler | null = null;
      let closed = false;
      const fake = {
        unref() {},
        on(ev: string, h: ErrHandler) { if (ev === 'error') errorHandler = h; },
        close() { closed = true; },
      } as unknown as fs.FSWatcher;
      handlers.push({
        dir: String(dir),
        fire: (err?: unknown) => { assert.ok(errorHandler, `error handler registered for ${String(dir)}`); errorHandler!(err); },
        closed: () => closed,
      });
      return fake;
    },
  );
  svc.watch();
  m.mock.restore();
  return { handlers };
}

describe('isWatcherPermissionError (U010/w116)', () => {
  it('is true for EACCES and EPERM', () => {
    assert.equal(isWatcherPermissionError(Object.assign(new Error('x'), { code: 'EACCES' })), true);
    assert.equal(isWatcherPermissionError(Object.assign(new Error('x'), { code: 'EPERM' })), true);
  });
  it('is false for other codes / absent / non-error values', () => {
    assert.equal(isWatcherPermissionError(Object.assign(new Error('x'), { code: 'ENOSPC' })), false);
    assert.equal(isWatcherPermissionError(new Error('no code')), false);
    assert.equal(isWatcherPermissionError(undefined), false);
    assert.equal(isWatcherPermissionError(null), false);
    assert.equal(isWatcherPermissionError('EACCES'), false);
  });
});

describe('ConfigService watcher permission-error tolerance (U010/w116)', () => {
  const made: string[] = [];
  const savedEnv: Record<string, string | undefined> = {};
  function setEnv(k: string, v: string | undefined): void {
    if (!(k in savedEnv)) savedEnv[k] = process.env[k];
    if (v === undefined) delete process.env[k]; else process.env[k] = v;
  }
  function tmp(prefix: string): string {
    const d = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
    made.push(d);
    return d;
  }
  afterEach(() => {
    for (const [k, v] of Object.entries(savedEnv)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
    for (const k of Object.keys(savedEnv)) delete savedEnv[k];
    for (const d of made.splice(0)) fs.rmSync(d, { recursive: true, force: true });
  });

  it('keeps the project watcher open on a per-path EACCES (no close, no retry)', () => {
    const root = tmp('u010-w116-proj-');
    fs.mkdirSync(path.join(root, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(root, '.ami', 'config.json'), '{}');
    const svc = new ConfigService(root, { fallbackPollMs: 0, watchRetryMs: 5 });
    const { handlers } = withCapturedWatchers(svc);
    const proj = handlers[0];

    proj.fire(Object.assign(new Error('EACCES, watch'), { code: 'EACCES' }));

    assert.equal(proj.closed(), false, 'permission error must NOT close the watcher');
    assert.notEqual((svc as unknown as { watcher: unknown }).watcher, null, 'watcher kept alive');
    assert.equal((svc as unknown as { retryTimer: unknown }).retryTimer, null, 'no re-establish retry scheduled');
    svc.stop();
  });

  it('still closes + reschedules the project watcher on a non-permission error', () => {
    const root = tmp('u010-w116-proj2-');
    fs.mkdirSync(path.join(root, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(root, '.ami', 'config.json'), '{}');
    const svc = new ConfigService(root, { fallbackPollMs: 0, watchRetryMs: 5 });
    const { handlers } = withCapturedWatchers(svc);
    const proj = handlers[0];

    proj.fire(Object.assign(new Error('ENOSPC, inotify'), { code: 'ENOSPC' }));

    assert.equal(proj.closed(), true, 'a fatal error closes the dead watcher');
    assert.equal((svc as unknown as { watcher: unknown }).watcher, null, 'watcher nulled pending retry');
    assert.ok((svc as unknown as { retryTimer: unknown }).retryTimer, 'a re-establish retry is scheduled');
    svc.stop();
  });

  it('tolerates a per-path EACCES on the GLOBAL watcher without dropping it', () => {
    const home = tmp('u010-w116-home-');
    fs.mkdirSync(path.join(home, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(home, '.ami', 'config.json'), '{}');
    setEnv('HOME', home);
    setEnv('USERPROFILE', home);
    const proj = tmp('u010-w116-gproj-');
    fs.mkdirSync(path.join(proj, '.ami'), { recursive: true });
    const svc = new ConfigService(proj, { watchGlobal: true, fallbackPollMs: 0 });
    const { handlers } = withCapturedWatchers(svc);
    const globalW = handlers.find(h => h.dir === path.join(home, '.ami'));
    assert.ok(globalW, 'a global-tier watcher was established');

    globalW!.fire(Object.assign(new Error('EPERM, watch'), { code: 'EPERM' }));
    assert.equal(globalW!.closed(), false, 'permission error must NOT close the global watcher');
    assert.notEqual((svc as unknown as { globalWatcher: unknown }).globalWatcher, null, 'global watcher kept alive');

    // A fatal error still drops it.
    globalW!.fire(Object.assign(new Error('EIO'), { code: 'EIO' }));
    assert.equal(globalW!.closed(), true, 'a fatal error closes the global watcher');
    svc.stop();
  });

  it('tolerates a per-path EACCES on the MANAGED watcher without dropping it', () => {
    const base = tmp('u010-w116-managed-');
    const managedPath = path.join(base, 'config.json');
    fs.writeFileSync(managedPath, '{}');
    setEnv('SUPERINFERENCE_MANAGED_CONFIG', managedPath);
    const proj = tmp('u010-w116-mproj-');
    fs.mkdirSync(path.join(proj, '.ami'), { recursive: true });
    const svc = new ConfigService(proj, { watchManaged: true, fallbackPollMs: 0, managedPollMs: 0 });
    const { handlers } = withCapturedWatchers(svc);
    const managedW = handlers.find(h => h.dir === base);
    assert.ok(managedW, 'a managed-tier watcher was established');

    managedW!.fire(Object.assign(new Error('EACCES, watch'), { code: 'EACCES' }));
    assert.equal(managedW!.closed(), false, 'permission error must NOT close the managed watcher');
    assert.notEqual((svc as unknown as { managedWatcher: unknown }).managedWatcher, null, 'managed watcher kept alive');

    managedW!.fire(Object.assign(new Error('EIO'), { code: 'EIO' }));
    assert.equal(managedW!.closed(), true, 'a fatal error closes the managed watcher');
    svc.stop();
  });
});
