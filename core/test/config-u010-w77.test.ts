// U010 / w77 — two additive config.ts parity features:
//
//  1. statusLine `{ command, padding }` object form (parity: Claude Code's
//     `settings.statusLine` object). A ported CC settings file using the object shape
//     must PARSE (not be rejected as "must be a string"), and `normalizeStatusLine`
//     collapses both shapes into one `{ command?, padding? }` result.
//
//  2. ConfigService global-tier watch + real `source` discrimination (parity: Claude's
//     useSettingsChange threading the tier `source` and watching every tier). With
//     `watchGlobal`, a runtime edit to `~/.ami/config.json` fires onConfigChange with
//     `source: 'global'` (vs the project tier's `'project'`); default-off keeps the
//     project-only behavior.

import { describe, it, mock, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  ConfigService,
  fsHooks as configFsHooks,
  normalizeStatusLine,
  validateProjectConfigValues,
  loadConfigFileChecked,
  KNOWN_CONFIG_KEYS,
  type ProjectConfig,
} from '../src/config';

const delay = (ms: number) => new Promise(r => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// Feature 1 — statusLine object form
// ---------------------------------------------------------------------------

describe('statusLine { command, padding } object form (U010/w77)', () => {
  it('normalizeStatusLine collapses the bare-string shape', () => {
    assert.deepEqual(normalizeStatusLine('mystatus.sh'), { command: 'mystatus.sh' });
    assert.deepEqual(normalizeStatusLine('  trimmed.sh  '), { command: 'trimmed.sh' });
  });

  it('normalizeStatusLine collapses the object shape with padding', () => {
    assert.deepEqual(normalizeStatusLine({ command: 'a.sh', padding: 3 }), { command: 'a.sh', padding: 3 });
    assert.deepEqual(normalizeStatusLine({ command: 'a.sh' }), { command: 'a.sh' });
  });

  it('normalizeStatusLine treats blank / absent / non-positive padding as unset', () => {
    assert.deepEqual(normalizeStatusLine(undefined), {});
    assert.deepEqual(normalizeStatusLine('   '), {});
    assert.deepEqual(normalizeStatusLine({ command: '   ' }), {});
    // A non-positive / non-finite padding is dropped so the env/default fallback wins.
    assert.deepEqual(normalizeStatusLine({ command: 'a.sh', padding: 0 }), { command: 'a.sh' });
    assert.deepEqual(normalizeStatusLine({ command: 'a.sh', padding: -5 }), { command: 'a.sh' });
    assert.deepEqual(normalizeStatusLine({ command: 'a.sh', padding: Infinity }), { command: 'a.sh' });
  });

  it('validateProjectConfigValues ACCEPTS both statusLine shapes', () => {
    assert.deepEqual(validateProjectConfigValues({ statusLine: 'cmd.sh' }), []);
    assert.deepEqual(validateProjectConfigValues({ statusLine: { command: 'cmd.sh', padding: 2 } }), []);
    assert.deepEqual(validateProjectConfigValues({ statusLine: { command: 'cmd.sh' } }), []);
  });

  it('validateProjectConfigValues FLAGS a malformed statusLine', () => {
    // A non-string / non-object value.
    const asArray = validateProjectConfigValues({ statusLine: ['x'] as unknown as string });
    assert.equal(asArray.length, 1);
    assert.equal(asArray[0]!.field, 'statusLine');
    assert.match(asArray[0]!.message, /string or a \{ command, padding \} object/);

    // Object with a non-string command.
    const badCmd = validateProjectConfigValues({ statusLine: { command: 5 } as unknown as { command: string } });
    assert.ok(badCmd.some(i => i.field === 'statusLine.command'));

    // Object with a non-number padding.
    const badPad = validateProjectConfigValues({ statusLine: { command: 'a.sh', padding: 'wide' } as unknown as { command: string; padding: number } });
    assert.ok(badPad.some(i => i.field === 'statusLine.padding'));
  });

  it('statusLine is a recognized config key (no spurious unknown-key warning)', () => {
    assert.ok(KNOWN_CONFIG_KEYS.has('statusLine'));
  });

  it('loadConfigFileChecked loads an object-form statusLine config cleanly (RED before fix)', () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'u010-w77-sl-'));
    try {
      const p = path.join(tmp, 'config.json');
      fs.writeFileSync(p, JSON.stringify({ statusLine: { command: 'mystatus.sh', padding: 2 } }));
      const { config, diagnostic } = loadConfigFileChecked(p);
      // Before the fix statusLine was typed string-only, so the object form produced an
      // `invalid-value` diagnostic and would not load — this is the discriminating check.
      assert.equal(diagnostic, undefined, 'object-form statusLine must not be a diagnostic');
      assert.deepEqual((config as ProjectConfig).statusLine, { command: 'mystatus.sh', padding: 2 });
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Feature 2 — ConfigService global-tier watch + real source discrimination
// ---------------------------------------------------------------------------

/**
 * Mount a ConfigService, capturing each `fs.watch` callback keyed by the WATCHED DIR so
 * the project-tier and global-tier watchers can be driven independently and deterministically.
 */
function withCapturedWatches(svc: ConfigService): {
  fire: (dir: string, filename: string) => void;
  dirs: string[];
} {
  const byDir = new Map<string, (e: string, f: string | null) => void>();
  const fake = { unref() {}, on() {}, close() {} } as unknown as fs.FSWatcher;
  const m = mock.method(configFsHooks, 'watch', (dir: unknown, cb: (e: string, f: string | null) => void) => {
    byDir.set(String(dir), cb);
    return fake;
  });
  svc.watch();
  m.mock.restore();
  return {
    fire: (dir, filename) => {
      const cb = byDir.get(dir);
      assert.ok(cb, `no watcher registered for ${dir}`);
      cb!('change', filename);
    },
    dirs: [...byDir.keys()],
  };
}

describe('ConfigService global-tier watch + source (U010/w77)', () => {
  let tmpDir: string;
  let tmpHome: string;
  let prevHome: string | undefined;
  let prevUserProfile: string | undefined;

  function setup(globalContent?: string): { cwd: string; globalDir: string; globalPath: string } {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'u010-w77-cwd-'));
    tmpHome = fs.mkdtempSync(path.join(os.tmpdir(), 'u010-w77-home-'));
    prevHome = process.env.HOME;
    prevUserProfile = process.env.USERPROFILE;
    process.env.HOME = tmpHome;
    delete process.env.USERPROFILE;
    const cfgDir = path.join(tmpDir, '.ami');
    fs.mkdirSync(cfgDir, { recursive: true });
    fs.writeFileSync(path.join(cfgDir, 'config.json'), '{"model":"proj"}');
    const globalDir = path.join(tmpHome, '.ami');
    fs.mkdirSync(globalDir, { recursive: true });
    const globalPath = path.join(globalDir, 'config.json');
    if (globalContent !== undefined) fs.writeFileSync(globalPath, globalContent);
    return { cwd: tmpDir, globalDir, globalPath };
  }

  afterEach(() => {
    if (prevHome === undefined) delete process.env.HOME; else process.env.HOME = prevHome;
    if (prevUserProfile === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = prevUserProfile;
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ }
    try { fs.rmSync(tmpHome, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  it('fires onConfigChange with source "global" on a global-tier edit', async () => {
    const { cwd, globalDir, globalPath } = setup('{"model":"g1"}');
    const svc = new ConfigService(cwd, { watchGlobal: true, debounceMs: 1, fallbackPollMs: 0 });
    const events: Array<{ source: string; path: string }> = [];
    svc.onConfigChange((source, filePath) => { events.push({ source, path: filePath }); });
    const { fire, dirs } = withCapturedWatches(svc);
    assert.ok(dirs.includes(globalDir), 'global dir is watched when watchGlobal is on');

    fs.writeFileSync(globalPath, '{"model":"g2"}');
    fire(globalDir, 'config.json');
    for (let i = 0; i < 40 && events.length === 0; i++) await delay(10);

    assert.equal(events.length, 1, 'exactly one global change fired');
    assert.equal(events[0]!.source, 'global');
    assert.equal(events[0]!.path, globalPath);
    svc.stop();
  });

  it('a no-op global watcher echo (unchanged content) fires nothing', async () => {
    const { cwd, globalDir } = setup('{"model":"g1"}');
    const svc = new ConfigService(cwd, { watchGlobal: true, debounceMs: 1, fallbackPollMs: 0 });
    const events: string[] = [];
    svc.onConfigChange((source) => { events.push(source); });
    const { fire } = withCapturedWatches(svc);

    // Content is unchanged since construction → snapshot matches → no notification.
    fire(globalDir, 'config.json');
    await delay(30);
    assert.deepEqual(events, [], 'unchanged global content must not fire a change');
    svc.stop();
  });

  it('project-tier edits still fire source "project"', async () => {
    const { cwd } = setup('{"model":"g1"}');
    const projDir = path.join(cwd, '.ami');
    const svc = new ConfigService(cwd, { watchGlobal: true, debounceMs: 1, fallbackPollMs: 0 });
    const events: string[] = [];
    svc.onConfigChange((source) => { events.push(source); });
    const { fire } = withCapturedWatches(svc);

    fs.writeFileSync(path.join(projDir, 'config.json'), '{"model":"proj2"}');
    fire(projDir, 'config.json');
    for (let i = 0; i < 40 && events.length === 0; i++) await delay(10);

    assert.deepEqual(events, ['project']);
    svc.stop();
  });

  it('default (watchGlobal off) registers NO global watcher', () => {
    const { cwd, globalDir } = setup('{"model":"g1"}');
    const svc = new ConfigService(cwd, { fallbackPollMs: 0 });
    const { dirs } = withCapturedWatches(svc);
    assert.ok(!dirs.includes(globalDir), 'no global watcher without watchGlobal');
    assert.equal((svc as unknown as { globalConfigPath: unknown }).globalConfigPath, null);
    svc.stop();
  });

  it('stop() tears down the global watcher and debounce timer', () => {
    const { cwd, globalDir } = setup('{"model":"g1"}');
    const svc = new ConfigService(cwd, { watchGlobal: true, debounceMs: 1000, fallbackPollMs: 0 });
    svc.onConfigChange(() => {});
    const { fire } = withCapturedWatches(svc);
    // Arm the global debounce timer (write new content so the event is not a no-op path).
    fs.writeFileSync(path.join(globalDir, 'config.json'), '{"model":"g2"}');
    fire(globalDir, 'config.json');
    assert.ok((svc as unknown as { globalDebounceTimer: unknown }).globalDebounceTimer, 'global debounce armed');
    svc.stop();
    assert.equal((svc as unknown as { globalWatcher: unknown }).globalWatcher, null);
    assert.equal((svc as unknown as { globalDebounceTimer: unknown }).globalDebounceTimer, null);
  });
});
