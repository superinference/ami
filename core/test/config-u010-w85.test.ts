// U010 / w85 — two additive config.ts parity features:
//
//  1. ConfigService.stop() is now AWAIT-able: it returns a Promise that resolves only
//     once every live fs.FSWatcher has emitted its 'close' event, so a test/shutdown can
//     await the watcher being FULLY stopped before deleting the watched directory (parity:
//     Claude Code's changeDetector.dispose() returning the watcher close() promise — the
//     documented teardown ENOENT race). The synchronous teardown still runs before the
//     promise is returned, so fire-and-forget callers are unaffected.
//
//  2. statusLine `{ type: 'command', command }` Claude-Code drop-in shape: a settings.json
//     copied verbatim from the reference (which ALWAYS writes the `type` discriminator) must
//     parse unchanged — `type` is accepted-and-ignored, AMI renders the `command`. A
//     non-string `type` is surfaced as an invalid-value diagnostic (the command still loads).

import { describe, it, mock } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  ConfigService,
  fsHooks as configFsHooks,
  normalizeStatusLine,
  validateProjectConfigValues,
  loadConfigFileChecked,
  findUnknownConfigKeys,
  type ProjectConfig,
} from '../src/config';

/** A close-signalling FSWatcher fake: emits 'close' when close() is called (like Node's). */
class FakeWatcher extends EventEmitter {
  closed = false;
  unref(): this { return this; }
  close(): void { this.closed = true; this.emit('close'); }
}

// ---------------------------------------------------------------------------
// Feature 1 — await-able ConfigService.stop() teardown boundary
// ---------------------------------------------------------------------------

describe('ConfigService.stop() await-able teardown (U010/w85)', () => {
  let tmpDir: string;
  function setup(): void {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'u010-w85-'));
    fs.mkdirSync(path.join(tmpDir, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(tmpDir, '.ami', 'config.json'), '{"model":"m"}');
  }
  function cleanup(): void {
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ }
  }

  it('resolves only after the watcher emits close, and tears down synchronously first', async () => {
    setup();
    const watchers: FakeWatcher[] = [];
    const m = mock.method(configFsHooks, 'watch', () => {
      const w = new FakeWatcher();
      watchers.push(w);
      return w as unknown as fs.FSWatcher;
    });
    const svc = new ConfigService(tmpDir, { fallbackPollMs: 0 });
    svc.watch();
    m.mock.restore();
    assert.equal(watchers.length, 1, 'one project watcher created');

    const p = svc.stop();
    assert.ok(typeof (p as Promise<void>).then === 'function', 'stop() returns a Promise');
    // Synchronous teardown happened BEFORE the promise resolves.
    assert.equal((svc as unknown as { watcher: unknown }).watcher, null, 'watcher nulled synchronously');
    assert.equal((svc as unknown as { started: boolean }).started, false, 'marked stopped synchronously');
    assert.equal(watchers[0]!.closed, true, 'watcher.close() called synchronously');

    await p; // resolves once the 'close' event fired
    cleanup();
  });

  it('awaits BOTH the project and global watchers when watchGlobal is on', async () => {
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'u010-w85-home-'));
    const prevHome = process.env.HOME;
    const prevUserProfile = process.env.USERPROFILE;
    process.env.HOME = home;
    delete process.env.USERPROFILE;
    fs.mkdirSync(path.join(home, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(home, '.ami', 'config.json'), '{"model":"g"}');
    setup();
    const watchers: FakeWatcher[] = [];
    const m = mock.method(configFsHooks, 'watch', () => {
      const w = new FakeWatcher();
      watchers.push(w);
      return w as unknown as fs.FSWatcher;
    });
    const svc = new ConfigService(tmpDir, { watchGlobal: true, fallbackPollMs: 0 });
    svc.watch();
    m.mock.restore();
    assert.equal(watchers.length, 2, 'project + global watchers created');

    let resolved = false;
    const p = svc.stop().then(() => { resolved = true; });
    await p;
    assert.equal(resolved, true, 'stop() resolved after both watchers closed');
    assert.ok(watchers.every(w => w.closed), 'both watchers closed');

    if (prevHome === undefined) delete process.env.HOME; else process.env.HOME = prevHome;
    if (prevUserProfile === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = prevUserProfile;
    try { fs.rmSync(home, { recursive: true, force: true }); } catch { /* ignore */ }
    cleanup();
  });

  it('resolves immediately when the service was never watched', async () => {
    setup();
    const svc = new ConfigService(tmpDir, { fallbackPollMs: 0 });
    await svc.stop(); // no watcher armed — nothing to await
    cleanup();
  });

  it('a second stop() is safe and resolves (idempotent teardown)', async () => {
    setup();
    const m = mock.method(configFsHooks, 'watch', () => new FakeWatcher() as unknown as fs.FSWatcher);
    const svc = new ConfigService(tmpDir, { fallbackPollMs: 0 });
    svc.watch();
    m.mock.restore();
    await svc.stop();
    await svc.stop(); // no watchers left — resolves at once
    cleanup();
  });

  it('tolerates a watcher fake without once() (resolves synchronously)', async () => {
    setup();
    // The minimal fake used by other suites has no `once` — stop() must not throw and
    // must still resolve (nothing to await on it).
    const fake = { unref() {}, on() {}, close() {} } as unknown as fs.FSWatcher;
    const m = mock.method(configFsHooks, 'watch', () => fake);
    const svc = new ConfigService(tmpDir, { fallbackPollMs: 0 });
    svc.watch();
    m.mock.restore();
    await svc.stop();
    assert.equal((svc as unknown as { watcher: unknown }).watcher, null);
    cleanup();
  });
});

// ---------------------------------------------------------------------------
// Feature 2 — statusLine `{ type: 'command', command }` Claude-Code drop-in shape
// ---------------------------------------------------------------------------

describe('statusLine type discriminator drop-in compat (U010/w85)', () => {
  let tmpDir: string;
  function writeConfig(body: string): string {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'u010-w85-sl-'));
    const p = path.join(tmpDir, 'config.json');
    fs.writeFileSync(p, body);
    return p;
  }
  function cleanup(): void {
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ }
  }

  it('loads a verbatim Claude-Code { type:"command", command } block with no diagnostic', () => {
    const p = writeConfig('{"statusLine":{"type":"command","command":"statusline.sh"}}');
    const { config, diagnostic } = loadConfigFileChecked(p);
    assert.equal(diagnostic, undefined, 'CC drop-in statusLine must not be flagged');
    assert.ok(config, 'config loaded');
    assert.deepEqual(normalizeStatusLine(config!.statusLine), { command: 'statusline.sh' });
    cleanup();
  });

  it('honors padding alongside the type discriminator', () => {
    const p = writeConfig('{"statusLine":{"type":"command","command":"s.sh","padding":2}}');
    const { diagnostic, config } = loadConfigFileChecked(p);
    assert.equal(diagnostic, undefined);
    assert.deepEqual(normalizeStatusLine(config!.statusLine), { command: 's.sh', padding: 2 });
    cleanup();
  });

  it('does not surface the nested `type` key as an unknown-config-key typo', () => {
    // `type` lives on the statusLine object, not the top level — it must never be
    // mistaken for an unrecognized top-level config key.
    assert.deepEqual(findUnknownConfigKeys({ statusLine: { type: 'command', command: 'x' } } as ProjectConfig), []);
  });

  it('ignores an unrecognized future type string (forward-compat), still rendering command', () => {
    assert.deepEqual(normalizeStatusLine({ type: 'future-kind', command: 'x' }), { command: 'x' });
  });

  it('surfaces a non-string `type` as an invalid-value issue while the command still loads', () => {
    const issues = validateProjectConfigValues({ statusLine: { type: 123, command: 'x' } } as unknown as ProjectConfig);
    const typeIssue = issues.find(i => i.field === 'statusLine.type');
    assert.ok(typeIssue, 'a numeric statusLine.type is flagged');
    assert.match(typeIssue!.message, /must be a string/);
    // The valid command is NOT flagged.
    assert.equal(issues.find(i => i.field === 'statusLine.command'), undefined);
  });
});
