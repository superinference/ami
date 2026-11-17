// U395 — end-to-end watcher echo suppression for the settings/config file.
//
// The internal-write registry (markInternalWrite / consumeInternalWrite /
// INTERNAL_WRITE_WINDOW_MS in config.ts) exists to stop AMI's OWN config writes
// from re-triggering the ConfigService file watcher — a self-feedback loop in
// which a `/config` change (persisted via safeWriteConfig) fires the watcher,
// which reloads and invokes the onChange / onConfigChange hooks as if an external
// editor had changed the file (parity: Claude Code's changeDetector
// markInternalWrite / consumeInternalWrite).
//
// The primitive-level behavior is covered in config-file-safety.test.ts. This
// file drives the capability END TO END through a real ConfigService watcher:
// it proves a marked self-write is SWALLOWED at the watcher (no reload, no
// listener fire) while an UNMARKED external write still fires — the control that
// discriminates the suppression from a watcher that simply never fires.

import { describe, it, after, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  ConfigService,
  fsHooks as configFsHooks,
  safeWriteConfig,
  markInternalWrite,
} from '../src/config';

const delay = (ms: number): Promise<void> => new Promise(r => setTimeout(r, ms));

/** Mount a ConfigService with a deterministically captured fs.watch callback. */
function withCapturedWatch(svc: ConfigService): {
  cb: (eventType: string, filename: string | null) => void;
} {
  let captured: ((e: string, f: string | null) => void) | null = null;
  // No `once` method: ConfigService.stop() only awaits a watcher 'close' event when
  // `typeof w.once === 'function'`, so a minimal fake without it resolves synchronously.
  const fake = {
    unref() {},
    on() {},
    close() {},
  } as unknown as fs.FSWatcher;
  const m = mock.method(
    configFsHooks,
    'watch',
    (_d: unknown, cb: (e: string, f: string | null) => void) => {
      captured = cb;
      return fake;
    },
  );
  svc.watch();
  m.mock.restore();
  assert.ok(captured, 'watch callback should have been captured');
  return { cb: captured as unknown as (e: string, f: string | null) => void };
}

describe('internal-write echo suppression — end to end (U395)', () => {
  const tmpDirs: string[] = [];
  function setup(initial: Record<string, unknown>): { dir: string; cfgPath: string } {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u395-echo-'));
    tmpDirs.push(dir);
    const cfgDir = path.join(dir, '.ami');
    fs.mkdirSync(cfgDir, { recursive: true });
    const cfgPath = path.join(cfgDir, 'config.json');
    fs.writeFileSync(cfgPath, JSON.stringify(initial));
    return { dir, cfgPath };
  }
  after(() => {
    for (const d of tmpDirs) fs.rmSync(d, { recursive: true, force: true });
  });

  it('swallows AMI\'s own marked write — no reload, no onChange / onConfigChange', async () => {
    const { dir, cfgPath } = setup({ model: 'before' });
    const svc = new ConfigService(dir, { debounceMs: 1, fallbackPollMs: 0 });
    let changes = 0;
    let notifications = 0;
    svc.onChange(() => { changes++; });
    svc.onConfigChange(() => { notifications++; });
    const { cb } = withCapturedWatch(svc);

    // Simulate AMI persisting a `/config` edit: the new bytes land on disk AND the
    // write is attributed to AMI (exactly what safeWriteConfig does internally).
    fs.writeFileSync(cfgPath, JSON.stringify({ model: 'after' }));
    markInternalWrite(cfgPath);
    cb('change', 'config.json');

    // Wait well past the debounce window: the watcher must have short-circuited.
    await delay(30);

    assert.equal(changes, 0, 'self-write must not fire onChange listeners');
    assert.equal(notifications, 0, 'self-write must not fire onConfigChange hooks');
    assert.equal(svc.get().model, 'before', 'in-memory config untouched (reload skipped)');
    await svc.stop();
  });

  it('still fires onChange for an UNMARKED external write (discriminating control)', async () => {
    const { dir, cfgPath } = setup({ model: 'before' });
    const svc = new ConfigService(dir, { debounceMs: 1, fallbackPollMs: 0 });
    let changes = 0;
    let notifications = 0;
    let last: { model?: string } = {};
    svc.onChange((n) => { changes++; last = n; });
    svc.onConfigChange(() => { notifications++; });
    const { cb } = withCapturedWatch(svc);

    // An external editor changes the file — NO internal-write mark.
    fs.writeFileSync(cfgPath, JSON.stringify({ model: 'external' }));
    cb('change', 'config.json');
    for (let i = 0; i < 40 && last.model !== 'external'; i++) await delay(10);

    assert.equal(last.model, 'external', 'external edit reloaded');
    assert.equal(changes, 1, 'external edit fires onChange exactly once');
    assert.equal(notifications, 1, 'external edit fires onConfigChange exactly once');
    await svc.stop();
  });

  it('safeWriteConfig\'s own mark is swallowed by the watcher (real production path)', async () => {
    const { dir, cfgPath } = setup({ model: 'v0' });
    const svc = new ConfigService(dir, { debounceMs: 1, fallbackPollMs: 0 });
    let changes = 0;
    svc.onChange(() => { changes++; });
    const { cb } = withCapturedWatch(svc);

    // The REAL persist path a `/config` command uses — marks the write itself.
    safeWriteConfig(cfgPath, { model: 'v1' });
    cb('change', 'config.json');
    await delay(30);

    assert.equal(changes, 0, 'safeWriteConfig echo swallowed — no listener fire');
    assert.equal(svc.get().model, 'v0', 'reload skipped on AMI\'s own write');
    await svc.stop();
  });

  it('only the first watcher event after a mark is swallowed; a later external edit fires', async () => {
    const { dir, cfgPath } = setup({ model: 'v0' });
    const svc = new ConfigService(dir, { debounceMs: 1, fallbackPollMs: 0 });
    let changes = 0;
    let last: { model?: string } = {};
    svc.onChange((n) => { changes++; last = n; });
    const { cb } = withCapturedWatch(svc);

    // Self-write: marked, swallowed.
    safeWriteConfig(cfgPath, { model: 'self' });
    cb('change', 'config.json');
    await delay(20);
    assert.equal(changes, 0, 'self-write swallowed');

    // A subsequent genuine external edit (mark already consumed) must fire.
    fs.writeFileSync(cfgPath, JSON.stringify({ model: 'later' }));
    cb('change', 'config.json');
    for (let i = 0; i < 40 && last.model !== 'later'; i++) await delay(10);

    assert.equal(last.model, 'later', 'external edit after a consumed mark reloads');
    assert.equal(changes, 1, 'exactly the external edit fired');
    await svc.stop();
  });
});
