/**
 * U010 / w111 — managed-config result cache (parity: Claude Code's `mdmCache` behind
 * the explicit `clearMdmSettingsCache` clear hook).
 *
 * `loadManagedConfig()` is re-invoked on EVERY config-waterfall merge (getMergedConfig,
 * the detached config path, the /doctor sweep, the managed-tier change detector). Before
 * w111 each call re-ran the full existsSync + readFileSync + parseJsonc chain across the
 * managed source(s). This memoizes the parsed result behind a cheap file-identity
 * signature, so a repeat read of an unchanged policy is a memory hit, while ANY on-disk
 * change (mtime/size) or an explicit resetConfigCache()/clearManagedConfigCache() re-reads
 * fresh — never a stale snapshot.
 *
 * RED (pre-feature): loadManagedConfig re-read the file on every call, so the
 * "second identical load performs no readFileSync" assertion FAILS and
 * clearManagedConfigCache does not exist.
 *
 * SAFE under fs mocks: the signature needs a real statSync on each present source; when
 * that stat cannot be taken (existsSync/readFileSync stubbed but statSync not), the read
 * is treated as uncacheable and always runs fresh — proven by the final test.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mock } from 'node:test';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  loadManagedConfig,
  clearManagedConfigCache,
  resetConfigCache,
  fsHooks,
  type ProjectConfig,
} from '../src/config';

describe('U010/w111 — loadManagedConfig result cache', () => {
  let tmpDir: string;
  let managedPath: string;
  let prevEnv: string | undefined;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w111-managed-'));
    managedPath = path.join(tmpDir, 'managed-config.json');
    prevEnv = process.env.SUPERINFERENCE_MANAGED_CONFIG;
    process.env.SUPERINFERENCE_MANAGED_CONFIG = managedPath;
    // Start from a clean module-global cache so an earlier read cannot leak in.
    resetConfigCache();
  });

  afterEach(() => {
    mock.restoreAll();
    if (prevEnv === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = prevEnv;
    resetConfigCache();
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* best effort */ }
  });

  /** Spy that counts real readFileSync calls for the managed path (passes through). */
  function spyReads(): () => number {
    const real = fs.readFileSync;
    let count = 0;
    mock.method(fsHooks, 'readFileSync', ((p: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
      if (String(p) === managedPath) count += 1;
      return (real as (...a: unknown[]) => unknown)(p, ...rest);
    }) as typeof fs.readFileSync);
    return () => count;
  }

  it('serves a second unchanged load from the cache (no re-read)', () => {
    fs.writeFileSync(managedPath, JSON.stringify({ model: 'cached-model' }), { mode: 0o600 });
    const reads = spyReads();

    const first = loadManagedConfig() as ProjectConfig;
    assert.equal(first.model, 'cached-model');
    assert.equal(reads(), 1, 'first load reads the file once');

    const second = loadManagedConfig() as ProjectConfig;
    assert.equal(second.model, 'cached-model');
    // RED without the cache: this is 2 (the file is re-read every call).
    assert.equal(reads(), 1, 'second identical load is a cache hit — no new readFileSync');
  });

  it('re-reads when the file identity (mtime/size) changes', () => {
    fs.writeFileSync(managedPath, JSON.stringify({ model: 'v1' }), { mode: 0o600 });
    const reads = spyReads();
    assert.equal((loadManagedConfig() as ProjectConfig).model, 'v1');
    assert.equal(reads(), 1);

    // Change content (different size) + bump mtime so the identity signature misses.
    fs.writeFileSync(managedPath, JSON.stringify({ model: 'v2-changed' }), { mode: 0o600 });
    const future = new Date(Date.now() + 5000);
    fs.utimesSync(managedPath, future, future);

    assert.equal((loadManagedConfig() as ProjectConfig).model, 'v2-changed', 'picks up the on-disk change');
    assert.equal(reads(), 2, 'a changed file is re-read');
  });

  it('re-reads after an explicit clearManagedConfigCache()', () => {
    fs.writeFileSync(managedPath, JSON.stringify({ model: 'x' }), { mode: 0o600 });
    const reads = spyReads();
    loadManagedConfig();
    assert.equal(reads(), 1);
    loadManagedConfig();
    assert.equal(reads(), 1, 'cached');

    clearManagedConfigCache();
    loadManagedConfig();
    assert.equal(reads(), 2, 'explicit clear forces a fresh read even when the file is unchanged');
  });

  it('resetConfigCache() fans out to the managed-config cache', () => {
    fs.writeFileSync(managedPath, JSON.stringify({ model: 'y' }), { mode: 0o600 });
    const reads = spyReads();
    loadManagedConfig();
    assert.equal(reads(), 1);

    resetConfigCache();
    loadManagedConfig();
    assert.equal(reads(), 2, 'resetConfigCache clears the managed memo too');
  });

  it('returns a defensive clone so a caller cannot poison the cache', () => {
    fs.writeFileSync(managedPath, JSON.stringify({ model: 'pristine', env: { A: '1' } }), { mode: 0o600 });
    const first = loadManagedConfig() as ProjectConfig;
    // Mutate the returned object — a shared cache reference would be corrupted.
    (first as { model?: string }).model = 'MUTATED';
    (first.env as Record<string, string>).A = 'MUTATED';

    const second = loadManagedConfig() as ProjectConfig;
    assert.equal(second.model, 'pristine', 'cache is isolated from caller mutation');
    assert.equal((second.env as Record<string, string>).A, '1');
  });

  it('never caches an uncacheable read (present source cannot be stat\'d)', () => {
    // Simulate the fs-mocked test path: the file "exists" and reads, but statSync
    // throws (not stubbed) — the read must run fresh every time, never cached.
    mock.method(fsHooks, 'existsSync', ((p: fs.PathLike) => String(p) === managedPath) as typeof fs.existsSync);
    mock.method(fsHooks, 'lstatSync', (() => ({ isSymbolicLink: () => false, mode: 0o600 })) as unknown as typeof fs.lstatSync);
    mock.method(fsHooks, 'statSync', (() => { throw Object.assign(new Error('ENOENT'), { code: 'ENOENT' }); }) as typeof fs.statSync);
    mock.method(fsHooks, 'readdirSync', (() => { throw new Error('no dir'); }) as typeof fs.readdirSync);
    let count = 0;
    mock.method(fsHooks, 'readFileSync', ((p: fs.PathOrFileDescriptor) => {
      if (String(p) === managedPath) count += 1;
      return JSON.stringify({ model: 'mocked' });
    }) as typeof fs.readFileSync);

    assert.equal((loadManagedConfig() as ProjectConfig).model, 'mocked');
    assert.equal((loadManagedConfig() as ProjectConfig).model, 'mocked');
    assert.equal(count, 2, 'an uncacheable (mocked) read is never served from the cache');
  });
});
