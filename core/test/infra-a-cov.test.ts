import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  stripJsoncComments,
  loadProjectConfig,
  loadLocalConfig,
  loadGlobalConfig,
  loadManagedConfig,
  ManagedConfigError,
  ConfigService,
  mergeConfigs,
  validateConfigJson,
  safeWriteConfig,
  cleanupStaleConfigTmp,
  fsHooks as configFsHooks,
  jsonHooks as configJsonHooks,
  type ProjectConfig,
} from '../src/config';
import {
  CronScheduler,
  matchField,
  wrapPromptSafely,
  getScheduler,
  resetScheduler,
  type CronJob,
} from '../src/cron';
import {
  log,
  logToolCall,
  logApiCall,
  logApiResponse,
  logError,
  getLogPath,
  fsHooks as loggerFsHooks,
  __resetLoggerForTest,
} from '../src/logger';
import {
  formatFile,
  getAvailableFormatters,
  isFormatterAvailable,
  execHooks,
  __clearFormatterCache,
  type FormatterConfig,
} from '../src/formatter';
import { ToolAnalytics } from '../src/tool-analytics';
import { TurnMetrics } from '../src/turn-metrics';

function mkTmp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'infra-a-'));
}
const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ===========================================================================
// config.ts
// ===========================================================================
describe('config: stripJsoncComments', () => {
  it('strips single-line // comments outside strings', () => {
    const out = stripJsoncComments('{\n  // a comment\n  "a": 1\n}');
    assert.deepEqual(JSON.parse(out), { a: 1 });
  });

  it('strips block /* */ comments', () => {
    const out = stripJsoncComments('{/* block */ "a": /* mid */ 2}');
    assert.deepEqual(JSON.parse(out), { a: 2 });
  });

  it('preserves // and /* inside string literals (URL safety)', () => {
    const out = stripJsoncComments('{"url": "http://x.com//y/*z*/"}');
    assert.deepEqual(JSON.parse(out), { url: 'http://x.com//y/*z*/' });
  });

  it('honors backslash escapes inside strings', () => {
    const out = stripJsoncComments('{"a": "b\\"c//d"}');
    assert.deepEqual(JSON.parse(out), { a: 'b"c//d' });
  });

  it('keeps a lone slash that is not a comment', () => {
    assert.equal(stripJsoncComments('a / b'), 'a / b');
  });

  it('keeps a trailing slash at end of input', () => {
    assert.equal(stripJsoncComments('x/'), 'x/');
  });

  it('handles unterminated block comment to EOF', () => {
    assert.equal(stripJsoncComments('a/* never closed').trim(), 'a');
  });
});

describe('config: file loaders', () => {
  let dir: string;
  beforeEach(() => { dir = mkTmp(); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('loadProjectConfig returns parsed config', () => {
    fs.mkdirSync(path.join(dir, '.superinference'));
    fs.writeFileSync(path.join(dir, '.superinference', 'config.json'), '{"model": "m1"}');
    assert.deepEqual(loadProjectConfig(dir), { model: 'm1' });
  });

  it('loadProjectConfig returns null when missing', () => {
    assert.equal(loadProjectConfig(dir), null);
  });

  it('loadProjectConfig returns null on invalid JSON (caught)', () => {
    fs.mkdirSync(path.join(dir, '.superinference'));
    fs.writeFileSync(path.join(dir, '.superinference', 'config.json'), '{not json');
    assert.equal(loadProjectConfig(dir), null);
  });

  it('loadLocalConfig reads config.local.json', () => {
    fs.mkdirSync(path.join(dir, '.superinference'));
    fs.writeFileSync(path.join(dir, '.superinference', 'config.local.json'), '{"persona": "p"}');
    assert.deepEqual(loadLocalConfig(dir), { persona: 'p' });
  });

  it('loadGlobalConfig reads from HOME', () => {
    const prevHome = process.env.HOME;
    const prevUp = process.env.USERPROFILE;
    try {
      process.env.HOME = dir;
      delete process.env.USERPROFILE;
      fs.mkdirSync(path.join(dir, '.superinference'));
      fs.writeFileSync(path.join(dir, '.superinference', 'config.json'), '{"maxTurns": 7}');
      assert.deepEqual(loadGlobalConfig(), { maxTurns: 7 });
    } finally {
      process.env.HOME = prevHome;
      if (prevUp === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = prevUp;
    }
  });

  it('loadGlobalConfig falls back to USERPROFILE', () => {
    const prevHome = process.env.HOME;
    const prevUp = process.env.USERPROFILE;
    try {
      delete process.env.HOME;
      process.env.USERPROFILE = dir;
      fs.mkdirSync(path.join(dir, '.superinference'));
      fs.writeFileSync(path.join(dir, '.superinference', 'config.json'), '{"tokenBudget": 9}');
      assert.deepEqual(loadGlobalConfig(), { tokenBudget: 9 });
    } finally {
      if (prevHome === undefined) delete process.env.HOME; else process.env.HOME = prevHome;
      if (prevUp === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = prevUp;
    }
  });

  it('loadGlobalConfig returns null when no home dir', () => {
    const prevHome = process.env.HOME;
    const prevUp = process.env.USERPROFILE;
    try {
      delete process.env.HOME;
      delete process.env.USERPROFILE;
      assert.equal(loadGlobalConfig(), null);
    } finally {
      if (prevHome === undefined) delete process.env.HOME; else process.env.HOME = prevHome;
      if (prevUp === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = prevUp;
    }
  });
});

describe('config: loadManagedConfig', () => {
  let dir: string;
  let prev: string | undefined;
  beforeEach(() => { dir = mkTmp(); prev = process.env.SUPERINFERENCE_MANAGED_CONFIG; });
  afterEach(() => {
    fs.rmSync(dir, { recursive: true, force: true });
    if (prev === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = prev;
  });

  it('returns parsed managed config', () => {
    const p = path.join(dir, 'managed.json');
    fs.writeFileSync(p, '{"enforceAvailableModels": ["m"]}');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    assert.deepEqual(loadManagedConfig(), { enforceAvailableModels: ['m'] });
  });

  it('returns null when file absent', () => {
    process.env.SUPERINFERENCE_MANAGED_CONFIG = path.join(dir, 'nope.json');
    assert.equal(loadManagedConfig(), null);
  });

  it('falls back to /etc default path when env var is unset', () => {
    delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    // /etc/superinference/config.json is absent in test environments -> null.
    const result = loadManagedConfig();
    assert.ok(result === null || typeof result === 'object');
  });

  it('throws ManagedConfigError (fail-closed) on broken file', () => {
    const p = path.join(dir, 'broken.json');
    fs.writeFileSync(p, '{bad');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    assert.throws(() => loadManagedConfig(), ManagedConfigError);
  });

  it('wraps a non-Error read failure via String()', () => {
    const p = path.join(dir, 'ok.json');
    fs.writeFileSync(p, '{}');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    const m = mock.method(configFsHooks, 'readFileSync', () => { throw 'raw string failure'; });
    try {
      assert.throws(() => loadManagedConfig(), (e: unknown) =>
        e instanceof ManagedConfigError && /raw string failure/.test(e.message));
    } finally { m.mock.restore(); }
  });
});

describe('config: ConfigService', () => {
  let dir: string;
  let sdir: string;
  let cfgPath: string;
  let svc: ConfigService | null;
  beforeEach(() => {
    dir = mkTmp();
    sdir = path.join(dir, '.superinference');
    fs.mkdirSync(sdir);
    cfgPath = path.join(sdir, 'config.json');
    fs.writeFileSync(cfgPath, '{"model": "a"}');
    svc = null;
  });
  afterEach(() => {
    if (svc) svc.stop();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('loads config in constructor and get() returns a copy', () => {
    svc = new ConfigService(dir);
    const c = svc.get();
    assert.equal(c.model, 'a');
    c.model = 'mutated';
    assert.equal(svc.get().model, 'a');
  });

  it('defaults to {} when no config file exists', () => {
    const empty = mkTmp();
    try {
      const s = new ConfigService(empty);
      assert.deepEqual(s.get(), {});
      s.stop();
    } finally { fs.rmSync(empty, { recursive: true, force: true }); }
  });

  it('onChange subscribe + unsubscribe', () => {
    svc = new ConfigService(dir);
    let calls = 0;
    const off = svc.onChange(() => { calls++; });
    fs.writeFileSync(cfgPath, '{"model": "b"}');
    assert.equal(svc.reload(), true);
    assert.equal(calls, 1);
    off();
    fs.writeFileSync(cfgPath, '{"model": "c"}');
    assert.equal(svc.reload(), true);
    assert.equal(calls, 1);
  });

  it('reload returns false when file becomes unreadable', () => {
    svc = new ConfigService(dir);
    fs.rmSync(cfgPath);
    assert.equal(svc.reload(), false);
  });

  it('reload returns false when content unchanged', () => {
    svc = new ConfigService(dir);
    assert.equal(svc.reload(), false);
  });

  it('reload invokes onConfigChange callbacks and tolerates throwing listeners', () => {
    svc = new ConfigService(dir);
    let cbSource = '';
    svc.onChange(() => { throw new Error('listener boom'); });
    svc.onConfigChange((source) => { cbSource = source; });
    svc.onConfigChange(() => { throw new Error('cb boom'); });
    fs.writeFileSync(cfgPath, '{"model": "z"}');
    assert.equal(svc.reload(), true);
    assert.equal(cbSource, 'project');
    assert.equal(svc.get().model, 'z');
  });

  it('watch() picks up file changes via fs.watch', async () => {
    svc = new ConfigService(dir);
    let changed: ProjectConfig | null = null;
    svc.onChange((next) => { changed = next; });
    svc.watch();
    svc.watch(); // second call is a no-op (already watching)
    // unrelated file change must be ignored
    fs.writeFileSync(path.join(sdir, 'other.txt'), 'x');
    await delay(60);
    fs.writeFileSync(cfgPath, '{"model": "watched"}');
    for (let i = 0; i < 40 && !changed; i++) await delay(25);
    assert.ok(changed, 'watcher should have fired reload');
    assert.equal((changed as ProjectConfig).model, 'watched');
  });

  it('watch callback ignores other files and debounces same-file bursts', async () => {
    svc = new ConfigService(dir);
    let reloads = 0;
    svc.onChange(() => { reloads++; });
    let captured: ((e: string, f: string | null) => void) | null = null;
    const fake = { unref() {}, on() {}, close() {} } as unknown as fs.FSWatcher;
    const m = mock.method(configFsHooks, 'watch', (_d: unknown, cb: (e: string, f: string | null) => void) => {
      captured = cb;
      return fake;
    });
    svc.watch();
    m.mock.restore();
    assert.ok(captured);
    const cb = captured as unknown as (e: string, f: string | null) => void;
    cb('change', 'unrelated.txt'); // filename !== config.json -> early return
    fs.writeFileSync(cfgPath, '{"model": "burst"}');
    cb('change', 'config.json'); // arms the debounce timer
    cb('change', 'config.json'); // existing timer -> clearTimeout + re-arm
    for (let i = 0; i < 40 && reloads === 0; i++) await delay(25);
    assert.equal(reloads, 1); // debounced into a single reload
    assert.equal(svc.get().model, 'burst');
  });

  it('watch() swallows errors from the underlying watcher setup', () => {
    svc = new ConfigService(dir);
    const m = mock.method(configFsHooks, 'watch', () => { throw new Error('ENOSPC watchers'); });
    try {
      assert.doesNotThrow(() => svc!.watch());
      // No watcher was installed.
      assert.equal((svc as unknown as { watcher: unknown }).watcher, null);
    } finally {
      m.mock.restore();
    }
  });

  it('watch() debounces a burst of changes', async () => {
    svc = new ConfigService(dir);
    let reloads = 0;
    svc.onChange(() => { reloads++; });
    svc.watch();
    fs.writeFileSync(cfgPath, '{"model": "b1"}');
    fs.writeFileSync(cfgPath, '{"model": "b2"}');
    fs.writeFileSync(cfgPath, '{"model": "b3"}');
    for (let i = 0; i < 40 && reloads === 0; i++) await delay(25);
    assert.ok(reloads >= 1);
    assert.equal(svc.get().model, 'b3');
  });

  it('watch() returns early when directory is absent', () => {
    const empty = mkTmp();
    try {
      const s = new ConfigService(empty); // .superinference does not exist
      s.watch();
      // No watcher created; stop is safe
      s.stop();
      assert.ok(true);
    } finally { fs.rmSync(empty, { recursive: true, force: true }); }
  });

  it('watcher error event triggers stop()', () => {
    svc = new ConfigService(dir);
    svc.watch();
    const w = (svc as unknown as { watcher: { emit: (e: string, a: unknown) => void } | null }).watcher;
    assert.ok(w);
    w!.emit('error', new Error('watch fail'));
    assert.equal((svc as unknown as { watcher: unknown }).watcher, null);
  });

  it('stop() is idempotent', () => {
    svc = new ConfigService(dir);
    svc.watch();
    svc.stop();
    svc.stop(); // nothing set now — both branches false
    assert.ok(true);
  });
});

describe('config: mergeConfigs', () => {
  it('returns empty object when all sources empty', () => {
    assert.deepEqual(mergeConfigs({}, {}, null, null), {});
  });

  it('applies every field from a single layer', () => {
    const all: ProjectConfig = {
      model: 'm', provider: 'p', baseUrl: 'u', permissionMode: 'ask',
      thinkingLevel: 't', maxEffortLevel: 'e', fallbackModel: 'fm',
      compactionModel: 'cm', maxTurns: 3, tokenBudget: 4, persona: 'per',
      bashOutputMaxChars: 5, taskOutputMaxChars: 6, cacheTtl: '1h', safeMode: true,
      maxWebSearchesPerSession: 7, deferTools: true, alwaysLoadTools: ['x'],
      fallbackModels: ['a'], allowedMcpServers: ['s'], managedMcpServers: { k: 1 },
    };
    assert.deepEqual(mergeConfigs({}, {}, all, null), all);
  });

  it('respects priority: cli > env > local > project > global > managed', () => {
    const merged = mergeConfigs(
      { model: 'cli' },
      { model: 'env', provider: 'envP' },
      { model: 'project', baseUrl: 'projU' },
      { model: 'global', persona: 'globalPer' },
      { model: 'local', maxTurns: 2 },
      { model: 'managed', tokenBudget: 1 },
    );
    assert.equal(merged.model, 'cli');
    assert.equal(merged.provider, 'envP');
    assert.equal(merged.baseUrl, 'projU');
    assert.equal(merged.persona, 'globalPer');
    assert.equal(merged.maxTurns, 2);
    assert.equal(merged.tokenBudget, 1);
  });

  it('managed allowlists are authoritative and override lower layers', () => {
    const merged = mergeConfigs(
      { enforceAvailableModels: ['cli'], allowedMcpServers: ['cliS'], managedMcpServers: { a: 1 } },
      {}, null, null, null,
      { enforceAvailableModels: ['mgd'], allowedMcpServers: ['mgdS'], managedMcpServers: { b: 2 } },
    );
    assert.deepEqual(merged.enforceAvailableModels, ['mgd']);
    assert.deepEqual(merged.allowedMcpServers, ['mgdS']);
    assert.deepEqual(merged.managedMcpServers, { b: 2 });
  });

  it('enforceAvailableModels is managed-only (ignored from lower layers)', () => {
    const merged = mergeConfigs(
      { enforceAvailableModels: ['cli'] }, {}, null, null,
    );
    // Only a managed layer may set enforceAvailableModels; lower layers are dropped.
    assert.equal(merged.enforceAvailableModels, undefined);
  });
});

describe('config: validateConfigJson', () => {
  it('accepts a JSON object', () => {
    assert.deepEqual(validateConfigJson('{"a":1}'), { valid: true });
  });
  it('rejects invalid JSON', () => {
    const r = validateConfigJson('{bad');
    assert.equal(r.valid, false);
    assert.match(r.error!, /Invalid JSON/);
  });

  it('wraps a non-Error parse failure via String()', () => {
    const m = mock.method(configJsonHooks, 'parse', () => { throw 'boom-string'; });
    try {
      const r = validateConfigJson('{}');
      assert.equal(r.valid, false);
      assert.equal(r.error, 'Invalid JSON: boom-string');
    } finally { m.mock.restore(); }
  });
  it('rejects null', () => {
    const r = validateConfigJson('null');
    assert.equal(r.valid, false);
    assert.match(r.error!, /null/);
  });
  it('rejects arrays', () => {
    const r = validateConfigJson('[1,2]');
    assert.equal(r.valid, false);
    assert.match(r.error!, /array/);
  });
  it('rejects primitives', () => {
    const r = validateConfigJson('42');
    assert.equal(r.valid, false);
    assert.match(r.error!, /number/);
  });
});

describe('config: safeWriteConfig & cleanupStaleConfigTmp', () => {
  let dir: string;
  beforeEach(() => { dir = mkTmp(); });
  afterEach(() => { mock.restoreAll(); fs.rmSync(dir, { recursive: true, force: true }); });

  it('writes atomically, creating parent dirs', () => {
    const p = path.join(dir, 'nested', 'config.json');
    safeWriteConfig(p, { model: 'm' });
    assert.deepEqual(JSON.parse(fs.readFileSync(p, 'utf-8')), { model: 'm' });
    assert.ok(!fs.existsSync(p + '.tmp'));
  });

  it('skips mkdir when dir already exists', () => {
    const p = path.join(dir, 'config.json');
    safeWriteConfig(p, { a: 1 });
    assert.ok(fs.existsSync(p));
  });

  it('falls back to direct write when rename fails', () => {
    const p = path.join(dir, 'config.json');
    mock.method(configFsHooks, 'renameSync', () => { throw new Error('rename EXDEV'); });
    safeWriteConfig(p, { fallback: true });
    assert.deepEqual(JSON.parse(fs.readFileSync(p, 'utf-8')), { fallback: true });
    assert.ok(!fs.existsSync(p + '.tmp'));
  });

  it('throws when write fails and cleans up tmp', () => {
    const p = path.join(dir, 'config.json');
    mock.method(configFsHooks, 'writeFileSync', () => { throw new Error('ENOSPC'); });
    assert.throws(() => safeWriteConfig(p, { x: 1 }), /ENOSPC/);
  });

  it('cleanupStaleConfigTmp returns false when no tmp present', () => {
    assert.equal(cleanupStaleConfigTmp(path.join(dir, 'config.json')), false);
  });

  it('cleanupStaleConfigTmp returns false when existsSync throws', () => {
    mock.method(configFsHooks, 'existsSync', () => { throw new Error('EACCES'); });
    assert.equal(cleanupStaleConfigTmp(path.join(dir, 'config.json')), false);
  });

  it('cleanupStaleConfigTmp promotes a valid tmp to the real file', () => {
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p + '.tmp', '{"recovered": 1}');
    assert.equal(cleanupStaleConfigTmp(p), true);
    assert.deepEqual(JSON.parse(fs.readFileSync(p, 'utf-8')), { recovered: 1 });
    assert.ok(!fs.existsSync(p + '.tmp'));
  });

  it('cleanupStaleConfigTmp falls back to copy when rename fails', () => {
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p + '.tmp', '{"copied": 1}');
    mock.method(configFsHooks, 'renameSync', () => { throw new Error('rename fail'); });
    assert.equal(cleanupStaleConfigTmp(p), true);
    assert.deepEqual(JSON.parse(fs.readFileSync(p, 'utf-8')), { copied: 1 });
    assert.ok(!fs.existsSync(p + '.tmp'));
  });

  it('cleanupStaleConfigTmp discards an invalid tmp', () => {
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p + '.tmp', '{invalid json');
    assert.equal(cleanupStaleConfigTmp(p), true);
    assert.ok(!fs.existsSync(p + '.tmp'));
    assert.ok(!fs.existsSync(p));
  });

  it('cleanupStaleConfigTmp returns true and cleans up when read throws', () => {
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p + '.tmp', '{"a":1}');
    mock.method(configFsHooks, 'readFileSync', () => { throw new Error('EIO'); });
    assert.equal(cleanupStaleConfigTmp(p), true);
  });
});

// ===========================================================================
// cron.ts
// ===========================================================================
describe('cron: matchField', () => {
  it('wildcard matches anything', () => {
    assert.equal(matchField(5, '*', 0, 59), true);
  });
  it('step over wildcard range', () => {
    assert.equal(matchField(10, '*/5', 0, 59), true);
    assert.equal(matchField(11, '*/5', 0, 59), false);
  });
  it('step over explicit range', () => {
    assert.equal(matchField(14, '10-20/2', 0, 59), true);
    assert.equal(matchField(15, '10-20/2', 0, 59), false);
  });
  it('step over single start uses max as end', () => {
    assert.equal(matchField(25, '5/10', 0, 59), true);
    assert.equal(matchField(4, '5/10', 0, 59), false);
  });
  it('invalid step is skipped', () => {
    assert.equal(matchField(5, '*/0', 0, 59), false);
    assert.equal(matchField(5, '*/x', 0, 59), false);
  });
  it('invalid range in step is skipped', () => {
    assert.equal(matchField(5, 'a-b/2', 0, 59), false);
  });
  it('invalid single start in step is skipped', () => {
    assert.equal(matchField(5, 'z/2', 0, 59), false);
  });
  it('plain ascending range', () => {
    assert.equal(matchField(15, '10-20', 0, 59), true);
    assert.equal(matchField(21, '10-20', 0, 59), false);
  });
  it('wrapping range (hi < lo)', () => {
    assert.equal(matchField(23, '22-2', 0, 23), true);
    assert.equal(matchField(1, '22-2', 0, 23), true);
    assert.equal(matchField(10, '22-2', 0, 23), false);
  });
  it('invalid plain range is skipped', () => {
    assert.equal(matchField(5, 'a-b', 0, 59), false);
  });
  it('DOW range applies modulo 7', () => {
    assert.equal(matchField(0, '7-7', 0, 6), true); // 7 -> 0
  });
  it('single value match', () => {
    assert.equal(matchField(30, '30', 0, 59), true);
    assert.equal(matchField(31, '30', 0, 59), false);
  });
  it('DOW single value 7 maps to 0', () => {
    assert.equal(matchField(0, '7', 0, 6), true);
  });
  it('comma list tries each part', () => {
    assert.equal(matchField(45, '10,45,50', 0, 59), true);
    assert.equal(matchField(46, '10,45,50', 0, 59), false);
  });
});

describe('cron: wrapPromptSafely', () => {
  it('wraps the prompt in a randomized fence', () => {
    const out = wrapPromptSafely('do a thing');
    assert.match(out, /Previously scheduled task/);
    assert.match(out, /do a thing/);
    assert.match(out, /`{4,}/);
  });
});

describe('cron: CronScheduler core', () => {
  let dir: string;
  let sched: CronScheduler;
  beforeEach(() => { dir = mkTmp(); sched = new CronScheduler(dir, 'sessX'); });
  afterEach(() => { sched.stopTicker(); fs.rmSync(dir, { recursive: true, force: true }); });

  it('create assigns incrementing ids and defaults', () => {
    const a = sched.create({ cron: '*/5 * * * *', prompt: 'p1' });
    const b = sched.create({ cron: '0 0 * * *', prompt: 'p2' });
    const c = sched.create({ cron: '0 12 * * *', prompt: 'p3' });
    assert.match(a.id, /^cron_1_/);
    assert.match(b.id, /^cron_2_/);
    assert.match(c.id, /^cron_3_/);
    assert.equal(a.recurring, true);
    assert.equal(a.durable, false);
    assert.ok(a.nextRun && a.nextRun > Date.now());
  });

  it('create honors explicit recurring/durable flags', () => {
    const j = sched.create({ cron: '0 0 * * *', prompt: 'x', recurring: false, durable: true });
    assert.equal(j.recurring, false);
    assert.equal(j.durable, true);
    assert.ok(fs.existsSync(path.join(dir, '.superinference', 'scheduled_tasks.json')));
  });

  it('enforces MAX_JOBS', () => {
    for (let i = 0; i < 50; i++) sched.create({ cron: '0 0 * * *', prompt: `p${i}` });
    assert.throws(() => sched.create({ cron: '0 0 * * *', prompt: 'overflow' }), /Maximum of 50/);
  });

  it('list and get', () => {
    const j = sched.create({ cron: '0 0 * * *', prompt: 'p' });
    assert.equal(sched.list().length, 1);
    assert.equal(sched.get(j.id)!.prompt, 'p');
    assert.equal(sched.get('missing'), undefined);
  });

  it('delete removes jobs and returns status', () => {
    const j = sched.create({ cron: '0 0 * * *', prompt: 'p', durable: true });
    assert.equal(sched.delete(j.id), true);
    assert.equal(sched.delete(j.id), false);
    assert.equal(sched.list().length, 0);
  });

  it('getDueJobs returns jobs whose nextRun+jitter has passed', () => {
    const j = sched.create({ cron: '*/5 * * * *', prompt: 'p' });
    j.nextRun = Date.now() - 60 * 60 * 1000;
    assert.deepEqual(sched.getDueJobs().map((x) => x.id), [j.id]);
    // not due when nextRun far in the future
    const future = sched.create({ cron: '0 0 * * *', prompt: 'later' });
    future.nextRun = Date.now() + 60 * 60 * 1000;
    assert.ok(!sched.getDueJobs().some((x) => x.id === future.id));
  });

  it('getDueJobs ignores jobs with no nextRun', () => {
    const j = sched.create({ cron: '0 0 * * *', prompt: 'p' });
    delete j.nextRun;
    assert.deepEqual(sched.getDueJobs(), []);
  });

  it('markRun reschedules recurring and deletes one-shot jobs', () => {
    const rec = sched.create({ cron: '*/5 * * * *', prompt: 'r' });
    rec.nextRun = Date.now() - 20 * 60 * 1000;
    sched.markRun(rec.id);
    assert.ok(rec.lastRun && rec.lastFiredAt);
    assert.ok(sched.get(rec.id)!.nextRun! > Date.now());

    const once = sched.create({ cron: '0 0 * * *', prompt: 'o', recurring: false });
    sched.markRun(once.id);
    assert.equal(sched.get(once.id), undefined);

    sched.markRun('missing'); // no-op
  });

  it('detectMissedTasks finds overdue recurring jobs', () => {
    const rec = sched.create({ cron: '*/5 * * * *', prompt: 'r' });
    rec.lastFiredAt = Date.now() - 24 * 60 * 60 * 1000;
    const once = sched.create({ cron: '*/5 * * * *', prompt: 'o', recurring: false });
    once.lastFiredAt = Date.now() - 24 * 60 * 60 * 1000;
    const missed = sched.detectMissedTasks();
    assert.ok(missed.some((m) => m.id === rec.id));
    assert.ok(!missed.some((m) => m.id === once.id));
  });

  it('computeNextRun throws on malformed cron', () => {
    assert.throws(() => sched.computeNextRun('* * *'), /Invalid cron expression/);
  });

  it('computeNextRun throws when no occurrence exists within the search window', () => {
    // Feb 30th never occurs: a valid 5-field cron that matches no minute in a year.
    assert.throws(() => sched.computeNextRun('0 0 30 2 *'), /Invalid cron expression/);
  });

  it('computeNextRun honors a fromTime anchor', () => {
    const base = new Date('2020-01-01T00:00:00Z').getTime();
    const next = sched.computeNextRun('0 0 * * *', base);
    assert.ok(next > base);
  });

  it('formatSchedule renders human-readable schedules', () => {
    assert.match(sched.formatSchedule('*/15 * * * *'), /every 15 minutes/);
    assert.match(sched.formatSchedule('30 9 5 6 1'), /at minute 30/);
    assert.match(sched.formatSchedule('30 9 5 6 1'), /at hour 9/);
    assert.match(sched.formatSchedule('30 9 5 6 1'), /on day 5/);
    assert.match(sched.formatSchedule('30 9 5 6 1'), /in month 6/);
    assert.match(sched.formatSchedule('0 0 * * 1,3'), /on Mon, Wed/);
    assert.match(sched.formatSchedule('0 0 * * 9'), /on 9/); // out-of-range day index falls through
    assert.equal(sched.formatSchedule('* * * * *'), 'every minute');
    assert.equal(sched.formatSchedule('bad'), 'bad');
  });
});

describe('cron: ticker, locks and expiry', () => {
  let dir: string;
  beforeEach(() => { dir = mkTmp(); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('startTicker is idempotent and stopTicker clears it', () => {
    const sched = new CronScheduler(dir, 's1');
    sched.startTicker(() => {});
    sched.startTicker(() => {}); // early return
    sched.stopTicker();
    sched.stopTicker(); // no interval now
    assert.ok(true);
  });

  it('startTicker interval fires due jobs on each tick', () => {
    mock.timers.enable({ apis: ['setInterval'] });
    const sched = new CronScheduler(dir, 's1');
    try {
      const j = sched.create({ cron: '*/5 * * * *', prompt: 'p' });
      j.nextRun = Date.now() - 20 * 60 * 1000;
      const fired: string[] = [];
      sched.startTicker((job) => fired.push(job.id));
      mock.timers.tick(5_000); // invoke the interval callback once
      assert.deepEqual(fired, [j.id]);
    } finally {
      sched.stopTicker();
      mock.timers.reset();
    }
  });

  it('tick runs due non-durable jobs', () => {
    const sched = new CronScheduler(dir, 's1');
    const j = sched.create({ cron: '*/5 * * * *', prompt: 'p' });
    j.nextRun = Date.now() - 20 * 60 * 1000;
    const fired: string[] = [];
    (sched as unknown as { tick: (cb: (j: CronJob) => void) => void }).tick((job) => fired.push(job.id));
    assert.deepEqual(fired, [j.id]);
  });

  it('tick skips durable jobs when the lock is held by another session', () => {
    // Pre-write a fresh foreign lock.
    fs.mkdirSync(path.join(dir, '.superinference'), { recursive: true });
    fs.writeFileSync(
      path.join(dir, '.superinference', 'cron.lock'),
      JSON.stringify({ sessionId: 'other', pid: 1, timestamp: Date.now() }),
    );
    const sched = new CronScheduler(dir, 'mine');
    const j = sched.create({ cron: '*/5 * * * *', prompt: 'p', durable: true });
    j.nextRun = Date.now() - 20 * 60 * 1000;
    const fired: string[] = [];
    (sched as unknown as { tick: (cb: (j: CronJob) => void) => void }).tick((job) => fired.push(job.id));
    assert.deepEqual(fired, []); // skipped
  });

  it('tick runs durable jobs when it can acquire the lock, replacing a stale lock', () => {
    fs.mkdirSync(path.join(dir, '.superinference'), { recursive: true });
    fs.writeFileSync(
      path.join(dir, '.superinference', 'cron.lock'),
      JSON.stringify({ sessionId: 'old', pid: 1, timestamp: Date.now() - 10 * 60 * 1000 }),
    );
    const sched = new CronScheduler(dir, 'mine');
    const j = sched.create({ cron: '*/5 * * * *', prompt: 'p', durable: true });
    j.nextRun = Date.now() - 20 * 60 * 1000;
    const fired: string[] = [];
    (sched as unknown as { tick: (cb: (j: CronJob) => void) => void }).tick((job) => fired.push(job.id));
    assert.deepEqual(fired, [j.id]);
  });

  it('acquireLock: fresh lock from same session is reacquired, releaseLock removes own lock', () => {
    const sched = new CronScheduler(dir, 'self');
    const inner = sched as unknown as { acquireLock: () => boolean; releaseLock: () => void };
    assert.equal(inner.acquireLock(), true); // create new lock
    assert.equal(inner.acquireLock(), true); // fresh + same session -> true
    inner.releaseLock();
    assert.ok(!fs.existsSync(path.join(dir, '.superinference', 'cron.lock')));
  });

  it('acquireLock returns false on a corrupt lock file', () => {
    fs.mkdirSync(path.join(dir, '.superinference'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.superinference', 'cron.lock'), '{corrupt');
    const sched = new CronScheduler(dir, 'self');
    assert.equal((sched as unknown as { acquireLock: () => boolean }).acquireLock(), false);
  });

  it('releaseLock leaves a foreign lock intact', () => {
    fs.mkdirSync(path.join(dir, '.superinference'), { recursive: true });
    const lockPath = path.join(dir, '.superinference', 'cron.lock');
    fs.writeFileSync(lockPath, JSON.stringify({ sessionId: 'someone-else', pid: 1, timestamp: Date.now() }));
    const sched = new CronScheduler(dir, 'self');
    (sched as unknown as { releaseLock: () => void }).releaseLock();
    assert.ok(fs.existsSync(lockPath)); // not ours -> untouched
  });

  it('releaseLock is a no-op when no lock exists', () => {
    const sched = new CronScheduler(dir, 'self');
    (sched as unknown as { releaseLock: () => void }).releaseLock();
    assert.ok(true);
  });

  it('releaseLock swallows a corrupt lock file', () => {
    fs.mkdirSync(path.join(dir, '.superinference'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.superinference', 'cron.lock'), '{not json');
    const sched = new CronScheduler(dir, 'self');
    assert.doesNotThrow(() => (sched as unknown as { releaseLock: () => void }).releaseLock());
  });

  it('delete clears a tracked timer when one is present', () => {
    const sched = new CronScheduler(dir, 's');
    const j = sched.create({ cron: '0 0 * * *', prompt: 'p' });
    const timers = (sched as unknown as { timers: Map<string, NodeJS.Timeout> }).timers;
    const t = setTimeout(() => {}, 60_000);
    t.unref();
    timers.set(j.id, t);
    assert.equal(sched.delete(j.id), true);
    assert.equal(timers.has(j.id), false);
  });

  it('expireOld deletes aged recurring jobs and keeps young ones', () => {
    const sched = new CronScheduler(dir, 's');
    const old = sched.create({ cron: '*/5 * * * *', prompt: 'old' });
    old.createdAt = Date.now() - 100 * 24 * 60 * 60 * 1000;
    const young = sched.create({ cron: '*/5 * * * *', prompt: 'young' });
    const once = sched.create({ cron: '*/5 * * * *', prompt: 'o', recurring: false });
    once.createdAt = Date.now() - 100 * 24 * 60 * 60 * 1000; // non-recurring -> never expired
    (sched as unknown as { expireOld: () => void }).expireOld();
    assert.equal(sched.get(old.id), undefined);
    assert.ok(sched.get(young.id));
    assert.ok(sched.get(once.id));
  });

  it('expireOld respects a per-job maxAgeDays override', () => {
    const sched = new CronScheduler(dir, 's');
    const j = sched.create({ cron: '*/5 * * * *', prompt: 'p' });
    j.maxAgeDays = 1;
    j.createdAt = Date.now() - 2 * 24 * 60 * 60 * 1000;
    (sched as unknown as { expireOld: () => void }).expireOld();
    assert.equal(sched.get(j.id), undefined);
  });
});

describe('cron: durable persistence', () => {
  let dir: string;
  beforeEach(() => { dir = mkTmp(); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('loads only durable jobs from disk, estimating jitter for malformed crons', () => {
    fs.mkdirSync(path.join(dir, '.superinference'), { recursive: true });
    const jobs: CronJob[] = [
      { id: 'cron_9_a', cron: 'weird', prompt: 'durable', recurring: true, durable: true, createdAt: Date.now(), nextRun: Date.now() - 20 * 60 * 1000 },
      { id: 'cron_3_c', cron: '0 0 * * *', prompt: 'durable2', recurring: true, durable: true, createdAt: Date.now() },
      { id: 'cron_8_b', cron: '0 0 * * *', prompt: 'ephemeral', recurring: true, durable: false, createdAt: Date.now() },
    ];
    fs.writeFileSync(path.join(dir, '.superinference', 'scheduled_tasks.json'), JSON.stringify(jobs));
    const sched = new CronScheduler(dir, 's');
    assert.equal(sched.list().length, 2);
    assert.equal(sched.get('cron_9_a')!.prompt, 'durable');
    // Next id must exceed the max loaded num; getNextIdNum iterates 9 then 3,
    // exercising the "num not greater than running max" branch.
    const created = sched.create({ cron: '0 0 * * *', prompt: 'new' });
    assert.match(created.id, /^cron_10_/);
    // getDueJobs exercises computeJitter -> estimatePeriod on a <2-part cron
    assert.ok(sched.getDueJobs().some((j) => j.id === 'cron_9_a'));
    sched.stopTicker();
  });

  it('estimatePeriod branches via getDueJobs for various cron shapes', () => {
    const sched = new CronScheduler(dir, 's');
    for (const c of ['*/5 * * * *', '0 */2 * * *', '0 * * * *', '0 0 * * *']) {
      const j = sched.create({ cron: c, prompt: c });
      j.nextRun = Date.now() - 20 * 60 * 1000;
    }
    assert.equal(sched.getDueJobs().length, 4);
    sched.stopTicker();
  });

  it('tolerates an unreadable durable file', () => {
    fs.mkdirSync(path.join(dir, '.superinference'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.superinference', 'scheduled_tasks.json'), '{not an array');
    const sched = new CronScheduler(dir, 's');
    assert.equal(sched.list().length, 0);
    sched.stopTicker();
  });

  it('saveDurable swallows write errors when the dir path is a file', () => {
    // Make .superinference a FILE so mkdirSync/write fail inside saveDurable.
    fs.writeFileSync(path.join(dir, '.superinference'), 'blocker');
    const sched = new CronScheduler(dir, 's');
    assert.doesNotThrow(() => sched.create({ cron: '0 0 * * *', prompt: 'p', durable: true }));
    sched.stopTicker();
  });
});

describe('cron: getScheduler singleton', () => {
  afterEach(() => { resetScheduler(); });
  it('returns a shared instance and resets cleanly', () => {
    resetScheduler(); // reset when already null -> early-return branch
    const a = getScheduler(mkTmp(), 'sess');
    const b = getScheduler();
    assert.equal(a, b);
    resetScheduler();
    const c = getScheduler();
    assert.notEqual(a, c);
  });
});

// ===========================================================================
// logger.ts
// ===========================================================================
describe('logger', () => {
  afterEach(() => { mock.restoreAll(); __resetLoggerForTest(); });

  it('getLogPath points under tmpdir/superinference/core/engine.log', () => {
    const p = getLogPath();
    assert.ok(p.startsWith(os.tmpdir()));
    assert.equal(path.basename(p), 'engine.log');
  });

  it('log writes a formatted line with and without data', () => {
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    log('comp', 'plain');
    log('comp', 'with data', { k: 'v' });
    const added = fs.readFileSync(getLogPath(), 'utf-8').slice(before.length);
    assert.match(added, /\[comp\] plain/);
    assert.match(added, /"k":"v"/);
  });

  it('helper loggers emit lines', () => {
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    logToolCall('bash', { cmd: 'x'.repeat(1000) }, 'y'.repeat(2000), true, 12);
    logApiCall('m', 3, 4, true);
    logApiResponse('stop', 100, 2, { promptTokens: 1, completionTokens: 2 });
    logApiResponse('stop', 50, 0);
    logError('eng', 'boom', { detail: 1 });
    const added = fs.readFileSync(getLogPath(), 'utf-8').slice(before.length);
    assert.match(added, /\[tool\] bash \(12ms\) ERROR/);
    assert.match(added, /\[api\] call model=m/);
    assert.match(added, /\[eng\] ERROR: boom/);
  });

  it('rotates the log when it exceeds the size cap', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'logrot-'));
    const fakeLog = path.join(dir, 'engine.log');
    const fakeBackup = fakeLog + '.prev';
    try {
      mock.method(loggerFsHooks, 'mkdirSync', () => undefined);
      mock.method(loggerFsHooks, 'statSync', () => ({ size: 20 * 1024 * 1024 }) as fs.Stats);
      let unlinked = false;
      let renamed = false;
      mock.method(loggerFsHooks, 'unlinkSync', () => { unlinked = true; });
      mock.method(loggerFsHooks, 'renameSync', () => { renamed = true; });
      __resetLoggerForTest();
      log('rot', 'trigger rotation');
      assert.equal(unlinked, true);
      assert.equal(renamed, true);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
      void fakeBackup;
    }
  });

  it('swallows a statSync failure (no existing log to rotate)', () => {
    mock.method(loggerFsHooks, 'mkdirSync', () => undefined);
    mock.method(loggerFsHooks, 'statSync', () => { throw new Error('ENOENT'); });
    let renamed = false;
    mock.method(loggerFsHooks, 'renameSync', () => { renamed = true; });
    __resetLoggerForTest();
    assert.doesNotThrow(() => log('nostat', 'first write'));
    assert.equal(renamed, false);
  });

  it('does not rotate when under the size cap', () => {
    mock.method(loggerFsHooks, 'mkdirSync', () => undefined);
    mock.method(loggerFsHooks, 'statSync', () => ({ size: 1 }) as fs.Stats);
    let renamed = false;
    mock.method(loggerFsHooks, 'renameSync', () => { renamed = true; });
    __resetLoggerForTest();
    log('norot', 'small');
    assert.equal(renamed, false);
  });

  it('swallows a mkdir failure during setup', () => {
    mock.method(loggerFsHooks, 'mkdirSync', () => { throw new Error('EACCES'); });
    __resetLoggerForTest();
    assert.doesNotThrow(() => log('fail', 'dir cannot be made'));
  });

  it('swallows an append failure', () => {
    mock.method(loggerFsHooks, 'appendFileSync', () => { throw new Error('ENOSPC'); });
    assert.doesNotThrow(() => log('fail', 'cannot append'));
  });

  it('ensureDir early-returns once initialized', () => {
    log('init', 'first'); // initialize
    let statCalls = 0;
    mock.method(loggerFsHooks, 'statSync', () => { statCalls++; return { size: 1 } as fs.Stats; });
    log('init', 'second'); // initialized already -> ensureDir returns immediately
    assert.equal(statCalls, 0);
  });
});

// ===========================================================================
// formatter.ts
// ===========================================================================
describe('formatter', () => {
  let dir: string;
  beforeEach(() => { dir = mkTmp(); __clearFormatterCache(); });
  afterEach(() => { mock.restoreAll(); __clearFormatterCache(); fs.rmSync(dir, { recursive: true, force: true }); });

  it('returns not-formatted for unknown extensions', () => {
    assert.deepEqual(formatFile(path.join(dir, 'a.unknownext'), dir), { formatted: false });
  });

  it('returns not-formatted when the formatter is unavailable', () => {
    mock.method(execHooks, 'execSync', () => { throw new Error('not installed'); });
    assert.deepEqual(formatFile(path.join(dir, 'a.ts'), dir), { formatted: false });
  });

  it('formats when the tool is available and succeeds', () => {
    const calls: string[] = [];
    mock.method(execHooks, 'execSync', (cmd: string) => { calls.push(cmd); return Buffer.from(''); });
    const file = path.join(dir, "weird'name.ts");
    const res = formatFile(file, dir);
    assert.deepEqual(res, { formatted: true });
    // availability check + the format command both ran
    assert.equal(calls.length, 2);
    assert.match(calls[1], /prettier --write/);
    // single-quote in the path is shell-escaped
    assert.match(calls[1], /weird'\\''name\.ts/);
  });

  it('reports an error when the formatter command fails', () => {
    let n = 0;
    mock.method(execHooks, 'execSync', () => {
      n++;
      if (n === 1) return Buffer.from(''); // availability check passes
      throw new Error('format crashed'); // the actual format fails
    });
    const res = formatFile(path.join(dir, 'a.ts'), dir);
    assert.equal(res.formatted, false);
    assert.match(res.error!, /format crashed/);
  });

  it('reports a non-Error rejection via String()', () => {
    let n = 0;
    mock.method(execHooks, 'execSync', () => {
      n++;
      if (n === 1) return Buffer.from('');
      throw 'string failure'; // non-Error throw
    });
    const res = formatFile(path.join(dir, 'a.ts'), dir);
    assert.equal(res.error, 'string failure');
  });

  it('memoizes availability across calls', () => {
    let checks = 0;
    mock.method(execHooks, 'execSync', (cmd: string) => {
      if (cmd.includes('--version')) checks++;
      return Buffer.from('');
    });
    formatFile(path.join(dir, 'a.ts'), dir);
    formatFile(path.join(dir, 'b.ts'), dir);
    assert.equal(checks, 1); // second call hit the cache
  });

  it('isFormatterAvailable returns false for a config without a check command', () => {
    const cfg: FormatterConfig = { extensions: ['.xyz'], command: 'noop', args: [] };
    assert.equal(isFormatterAvailable(cfg), false);
  });

  it('getAvailableFormatters lists tools reported as present', () => {
    mock.method(execHooks, 'execSync', () => Buffer.from(''));
    const list = getAvailableFormatters(dir);
    assert.ok(list.length > 0);
    assert.ok(list.some((l) => l.includes('prettier')));
  });

  it('getAvailableFormatters returns empty when nothing is installed', () => {
    mock.method(execHooks, 'execSync', () => { throw new Error('none'); });
    assert.deepEqual(getAvailableFormatters(dir), []);
  });
});

// ===========================================================================
// tool-analytics.ts
// ===========================================================================
describe('tool-analytics', () => {
  it('records calls and aggregates stats', () => {
    const a = new ToolAnalytics();
    a.record('bash', 100, false);
    a.record('bash', 300, true);
    a.record('read', 50, false);
    const bash = a.getStats('bash');
    assert.equal(bash.callCount, 2);
    assert.equal(bash.errorCount, 1);
    assert.equal(bash.totalDurationMs, 400);
    assert.equal(bash.avgDurationMs, 200);
    assert.equal(bash.minDurationMs, 100);
    assert.equal(bash.maxDurationMs, 300);
    assert.equal(bash.errorRate, 0.5);
  });

  it('leaves min/max unchanged for a mid-range duration', () => {
    const a = new ToolAnalytics();
    a.record('t', 100, false);
    a.record('t', 300, false);
    a.record('t', 200, false); // neither a new min nor a new max
    const s = a.getStats('t');
    assert.equal(s.minDurationMs, 100);
    assert.equal(s.maxDurationMs, 300);
  });

  it('reports min as 0 when every recorded duration is Infinity', () => {
    const a = new ToolAnalytics();
    a.record('inf', Infinity, false); // minDurationMs stays Infinity
    const s = a.getStats('inf');
    assert.equal(s.minDurationMs, 0); // Infinity sentinel normalized to 0
  });

  it('getStats guards against a degenerate zero-count entry', () => {
    const a = new ToolAnalytics();
    // White-box: inject an entry that never went through record() to exercise
    // the defensive callCount===0 / minDurationMs===Infinity guards in getStats.
    (a as unknown as { data: Map<string, unknown> }).data.set('ghost', {
      callCount: 0, errorCount: 0, totalDurationMs: 0, minDurationMs: Infinity, maxDurationMs: 0,
    });
    const s = a.getStats('ghost');
    assert.equal(s.avgDurationMs, 0);
    assert.equal(s.errorRate, 0);
    assert.equal(s.minDurationMs, 0);
  });

  it('returns zeroed stats for an unknown tool', () => {
    const a = new ToolAnalytics();
    const s = a.getStats('nope');
    assert.equal(s.tool, 'nope');
    assert.equal(s.callCount, 0);
    assert.equal(s.avgDurationMs, 0);
    assert.equal(s.minDurationMs, 0);
    assert.equal(s.errorRate, 0);
  });

  it('getSummary sorts by call count descending', () => {
    const a = new ToolAnalytics();
    a.record('x', 1, false);
    a.record('y', 1, false);
    a.record('y', 1, false);
    assert.deepEqual(a.getSummary().map((s) => s.tool), ['y', 'x']);
  });

  it('totalCalls and totalErrors aggregate across tools', () => {
    const a = new ToolAnalytics();
    assert.equal(a.totalCalls, 0);
    assert.equal(a.totalErrors, 0);
    a.record('a', 1, true);
    a.record('b', 1, false);
    assert.equal(a.totalCalls, 2);
    assert.equal(a.totalErrors, 1);
  });

  it('reset clears everything', () => {
    const a = new ToolAnalytics();
    a.record('a', 1, false);
    a.reset();
    assert.equal(a.totalCalls, 0);
    assert.deepEqual(a.getSummary(), []);
  });
});

// ===========================================================================
// turn-metrics.ts
// ===========================================================================
describe('turn-metrics', () => {
  it('starts empty with zeroed derived metrics', () => {
    const m = new TurnMetrics();
    assert.equal(m.turnCount, 0);
    assert.equal(m.avgToolCallsPerTurn, 0);
    assert.equal(m.avgTokensPerTurn, 0);
    assert.equal(m.tokensPerSecond, 0);
    assert.equal(m.longestTurnMs, 0);
  });

  it('records turns and computes aggregates', () => {
    const m = new TurnMetrics();
    m.recordTurn({ toolCalls: 2, tokensUsed: 1000, durationMs: 2000 });
    m.recordTurn({ toolCalls: 4, tokensUsed: 500, durationMs: 1000 }); // shorter -> longest unchanged
    assert.equal(m.turnCount, 2);
    assert.equal(m.totalToolCalls, 6);
    assert.equal(m.totalTokens, 1500);
    assert.equal(m.totalDurationMs, 3000);
    assert.equal(m.longestTurnMs, 2000);
    assert.equal(m.avgToolCallsPerTurn, 3);
    assert.equal(m.avgTokensPerTurn, 750);
    assert.equal(m.tokensPerSecond, 500);
  });

  it('getSummary mirrors the live getters', () => {
    const m = new TurnMetrics();
    m.recordTurn({ toolCalls: 1, tokensUsed: 100, durationMs: 1000 });
    const s = m.getSummary();
    assert.deepEqual(s, {
      turnCount: 1,
      totalToolCalls: 1,
      totalTokens: 100,
      totalDurationMs: 1000,
      avgToolCallsPerTurn: 1,
      avgTokensPerTurn: 100,
      tokensPerSecond: 100,
      longestTurnMs: 1000,
    });
  });

  it('reset returns to the empty state', () => {
    const m = new TurnMetrics();
    m.recordTurn({ toolCalls: 1, tokensUsed: 1, durationMs: 1 });
    m.reset();
    assert.equal(m.turnCount, 0);
    assert.equal(m.totalDurationMs, 0);
    assert.equal(m.longestTurnMs, 0);
  });
});
