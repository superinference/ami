/**
 * ConfigService.stop() must survive a watcher whose close() throws.
 *
 * Every other close site in ConfigService treats a throwing close() as "already dead".
 * stop() used to call close() unguarded: one throwing watcher rejected stop(), left the
 * remaining watchers open and the service still marked started. (Seen in CI as a timing-
 * dependent `Error: close-fail` from a coverage test whose earlier error paths had not yet
 * consumed the fake's throws.) A naive try/catch would instead HANG, awaiting a 'close'
 * event a dead watcher never emits. This pins both halves deterministically.
 *
 * Run: cd core/core && node --import tsx --test test/config-service-stop-close-throws.test.ts
 */
import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { EventEmitter } from 'node:events';
import { ConfigService, fsHooks as configFsHooks } from '../src/config';

type FakeWatcher = EventEmitter & { close: () => void; unref: () => void; closeCalls: number };

function fakeWatcher(throwsOnClose: boolean): FakeWatcher {
  const w = new EventEmitter() as FakeWatcher;
  w.closeCalls = 0;
  w.close = () => {
    w.closeCalls++;
    if (throwsOnClose) throw new Error('close-fail');
    queueMicrotask(() => w.emit('close'));
  };
  w.unref = () => {};
  return w;
}

describe('ConfigService.stop() with a throwing watcher close()', () => {
  const dirs: string[] = [];
  afterEach(() => {
    mock.restoreAll();
    for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
  });

  it('resolves (no throw, no hang), closes every watcher, and resets started', async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-stop-'));
    dirs.push(cwd);
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(cwd, '.ami', 'config.json'), '{}');

    const created: FakeWatcher[] = [];
    // The FIRST watcher created is dead: its close() always throws.
    mock.method(configFsHooks, 'watch', (() => {
      const w = fakeWatcher(created.length === 0);
      created.push(w);
      return w;
    }) as unknown as typeof configFsHooks.watch);

    const svc = new ConfigService(cwd, { watchGlobal: false, watchManaged: false, fallbackPollMs: 0 });
    svc.watch();
    assert.ok(created.length >= 1, 'a watcher was established');

    const outcome = await Promise.race([
      svc.stop().then(() => 'resolved', (e: unknown) => `rejected: ${(e as Error).message}`),
      new Promise<string>((r) => setTimeout(() => r('hung'), 1000)),
    ]);
    assert.equal(outcome, 'resolved', 'stop() must neither reject nor hang on a dead watcher');
    for (const w of created) assert.equal(w.closeCalls, 1, 'every watcher was closed exactly once');

    // started was reset: watching again establishes a fresh watcher.
    const before = created.length;
    svc.watch();
    assert.ok(created.length > before, 'stop() reset started, so watch() re-establishes');
    await svc.stop();
  });
});
