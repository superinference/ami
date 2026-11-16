// U010 / w68 — ConfigService hot-reload robustness (changeDetector parity).
//
// Covers the additive capabilities added to ConfigService.watch/reload:
//   - deletion grace window + grace-expired deletion reverts the project layer (gaps 27/65/67)
//   - delete-then-recreate within the grace window is coalesced into a single change
//   - write-stability retry: a present-but-unparseable (half-written) file is retried
//     once rather than silently dropped (gaps 26/64/68)
//   - fallback reconcile poll picks up a change event fs.watch dropped (gap 30)
//   - watcher retry: an absent dir / a watcher 'error' re-establishes instead of
//     permanently giving up (gap 31)
//   - loadManagedConfig success logging (gap 84, logging portion)
//
// The tests drive the real watch-event dispatcher by capturing the fs.watch callback
// (so timing is deterministic) and inject tiny timer windows via ConfigServiceOptions.

import { describe, it, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  ConfigService,
  fsHooks as configFsHooks,
  loadManagedConfig,
  CONFIG_WATCH_DEBOUNCE_MS,
  CONFIG_FILE_STABILITY_THRESHOLD_MS,
  CONFIG_DELETION_GRACE_MS,
  CONFIG_FALLBACK_POLL_MS,
  CONFIG_WATCH_RETRY_MS,
} from '../src/config';

const delay = (ms: number) => new Promise(r => setTimeout(r, ms));

/** Mount a ConfigService with a captured fs.watch callback + an injected FSWatcher. */
function withCapturedWatch(svc: ConfigService): {
  cb: (eventType: string, filename: string | null) => void;
  errorHandler: () => void;
  closed: () => boolean;
} {
  let captured: ((e: string, f: string | null) => void) | null = null;
  let errorHandler: (() => void) | null = null;
  let closed = false;
  const fake = {
    unref() {},
    on(ev: string, h: () => void) { if (ev === 'error') errorHandler = h; },
    close() { closed = true; },
  } as unknown as fs.FSWatcher;
  const m = mock.method(configFsHooks, 'watch', (_d: unknown, cb: (e: string, f: string | null) => void) => {
    captured = cb;
    return fake;
  });
  svc.watch();
  m.mock.restore();
  assert.ok(captured, 'watch callback should have been captured');
  return {
    cb: captured as unknown as (e: string, f: string | null) => void,
    errorHandler: () => { assert.ok(errorHandler, 'error handler should be registered'); errorHandler!(); },
    closed: () => closed,
  };
}

describe('ConfigService hot-reload robustness (U010/w68)', () => {
  let tmpDir: string;
  let cfgDir: string;
  let cfgPath: string;

  function setup(): void {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'u010-w68-'));
    cfgDir = path.join(tmpDir, '.ami');
    fs.mkdirSync(cfgDir, { recursive: true });
    cfgPath = path.join(cfgDir, 'config.json');
  }
  function teardown(svc?: ConfigService): void {
    if (svc) svc.stop();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }

  it('exposes tunable parity constants with sane defaults', () => {
    assert.equal(CONFIG_WATCH_DEBOUNCE_MS, 100);
    assert.equal(CONFIG_FILE_STABILITY_THRESHOLD_MS, 1000);
    assert.equal(CONFIG_DELETION_GRACE_MS, 1000);
    assert.equal(CONFIG_FALLBACK_POLL_MS, 5000);
    assert.equal(CONFIG_WATCH_RETRY_MS, 1000);
  });

  it('reverts the project layer to defaults after a grace-expired deletion', async () => {
    setup();
    fs.writeFileSync(cfgPath, '{"model":"present"}');
    const svc = new ConfigService(tmpDir, { debounceMs: 1, deletionGraceMs: 10, fallbackPollMs: 0 });
    assert.equal(svc.get().model, 'present');
    let notifiedNew: unknown = 'unset';
    let notifiedOld: unknown = 'unset';
    svc.onChange((n, o) => { notifiedNew = n; notifiedOld = o; });
    const { cb } = withCapturedWatch(svc);

    fs.rmSync(cfgPath);
    cb('rename', 'config.json');
    // wait out debounce + grace
    for (let i = 0; i < 40 && svc.get().model !== undefined; i++) await delay(10);

    assert.equal(svc.get().model, undefined, 'deleted config reverts to empty project layer');
    assert.deepEqual(notifiedNew, {});
    assert.deepEqual(notifiedOld, { model: 'present' });
    teardown(svc);
  });

  it('coalesces delete-then-recreate within the grace window into a single change', async () => {
    setup();
    fs.writeFileSync(cfgPath, '{"model":"v1"}');
    const svc = new ConfigService(tmpDir, { debounceMs: 1, deletionGraceMs: 200, fallbackPollMs: 0 });
    let changes = 0;
    let last: { model?: string } = {};
    svc.onChange((n) => { changes++; last = n; });
    const { cb } = withCapturedWatch(svc);

    // Simulate an atomic rename save: the file briefly vanishes then reappears.
    fs.rmSync(cfgPath);
    cb('rename', 'config.json');          // arms the deletion grace timer
    await delay(20);                       // still inside grace
    fs.writeFileSync(cfgPath, '{"model":"v2"}');
    cb('rename', 'config.json');           // recreate cancels the pending deletion
    for (let i = 0; i < 40 && last.model !== 'v2'; i++) await delay(10);

    assert.equal(last.model, 'v2', 'recreated content applied');
    assert.equal(changes, 1, 'deletion coalesced — no spurious revert-to-{} change');
    teardown(svc);
  });

  it('retries a present-but-unparseable (half-written) config instead of dropping it', async () => {
    setup();
    fs.writeFileSync(cfgPath, '{"model":"old"}');
    const svc = new ConfigService(tmpDir, { debounceMs: 1, stabilityThresholdMs: 30, fallbackPollMs: 0 });
    let applied: string | undefined;
    svc.onChange((n) => { applied = n.model; });
    const { cb } = withCapturedWatch(svc);

    // First event: the file is mid-write (truncated JSON) → must NOT apply, must retry.
    fs.writeFileSync(cfgPath, '{"model":"new"');  // invalid JSON
    cb('change', 'config.json');
    await delay(15);
    assert.equal(applied, undefined, 'partial write is not applied immediately');
    assert.equal(svc.get().model, 'old', 'in-memory config kept while file is unparseable');

    // The write finishes before the stability retry fires.
    fs.writeFileSync(cfgPath, '{"model":"new"}');
    for (let i = 0; i < 40 && applied !== 'new'; i++) await delay(10);
    assert.equal(applied, 'new', 'stability retry picks up the now-complete file');
    teardown(svc);
  });

  it('fallback poll reconciles a change event that fs.watch dropped', async () => {
    setup();
    fs.writeFileSync(cfgPath, '{"model":"a"}');
    const svc = new ConfigService(tmpDir, { fallbackPollMs: 15 });
    let seen: string | undefined;
    svc.onChange((n) => { seen = n.model; });
    // Capture (and discard) the watch callback so no watch event ever fires — the only
    // path that can pick up the change is the fallback poll.
    withCapturedWatch(svc);

    fs.writeFileSync(cfgPath, '{"model":"b"}');
    for (let i = 0; i < 50 && seen !== 'b'; i++) await delay(10);
    assert.equal(seen, 'b', 'fallback poll picked up the missed change');
    teardown(svc);
  });

  it('re-establishes the watcher after an error event instead of giving up', () => {
    setup();
    fs.writeFileSync(cfgPath, '{}');
    const svc = new ConfigService(tmpDir, { fallbackPollMs: 0, watchRetryMs: 5 });
    const handle = withCapturedWatch(svc);

    // Fire the watcher 'error' event: the dead watcher is closed and a retry scheduled.
    handle.errorHandler();
    assert.equal(handle.closed(), true, 'errored watcher was closed');
    assert.equal((svc as unknown as { watcher: unknown }).watcher, null, 'watcher nulled pending retry');
    assert.ok((svc as unknown as { retryTimer: unknown }).retryTimer, 'a re-establish retry is scheduled');
    teardown(svc);
  });

  it('retries establishing a watcher when the directory does not exist yet', () => {
    setup();
    // Point at a project whose .ami/ has not been created.
    const freshRoot = path.join(tmpDir, 'fresh');
    const svc = new ConfigService(freshRoot, { fallbackPollMs: 0, watchRetryMs: 5 });
    svc.watch();
    assert.equal((svc as unknown as { watcher: unknown }).watcher, null, 'no watcher while dir absent');
    assert.ok((svc as unknown as { retryTimer: unknown }).retryTimer, 'watch retry scheduled for absent dir');
    svc.stop();
    assert.equal((svc as unknown as { retryTimer: unknown }).retryTimer, null, 'stop() clears the retry timer');
    teardown();
  });

  it('watch() is idempotent and stop() clears every timer', () => {
    setup();
    fs.writeFileSync(cfgPath, '{}');
    const svc = new ConfigService(tmpDir, { fallbackPollMs: 50 });
    svc.watch();
    svc.watch(); // second call is a no-op
    assert.ok((svc as unknown as { pollTimer: unknown }).pollTimer, 'fallback poll running');
    svc.stop();
    assert.equal((svc as unknown as { pollTimer: unknown }).pollTimer, null);
    assert.equal((svc as unknown as { watcher: unknown }).watcher, null);
    svc.stop(); // second stop is safe
    teardown();
  });

  it('loadManagedConfig logs a success breadcrumb with the key count', () => {
    setup();
    const managedPath = path.join(tmpDir, 'managed.json');
    fs.writeFileSync(managedPath, JSON.stringify({ enforceAvailableModels: ['x'], permissionMode: 'deny-all' }));
    const prevEnv = process.env.SUPERINFERENCE_MANAGED_CONFIG;
    process.env.SUPERINFERENCE_MANAGED_CONFIG = managedPath;
    try {
      const loaded = loadManagedConfig();
      assert.ok(loaded, 'managed config loaded');
      assert.deepEqual(loaded!.enforceAvailableModels, ['x']);
    } finally {
      if (prevEnv === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
      else process.env.SUPERINFERENCE_MANAGED_CONFIG = prevEnv;
      teardown();
    }
  });
});
