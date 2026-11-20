/**
 * U010 / w110 — config read-cache control (parity: the reference's settings cache).
 *
 * Three Claude-Code parity caching capabilities, all correctness-preserving and
 * tied to ONE invalidation point:
 *
 *  - Memoized managed-config DEVICE-DEFAULT path resolution (parity:
 *    managedPath.ts getManagedFilePath wrapped in lodash memoize). The platform
 *    switch is computed once per platform instead of on every call, while an
 *    explicit SUPERINFERENCE_MANAGED_CONFIG override still wins and a forced
 *    different process.platform still resolves correctly (keyed by platform).
 *
 *  - An in-memory, file-identity-keyed (mtime+size) cache for the per-tier config
 *    loaders (project / local / global) with NEGATIVE caching of absent files, so a
 *    repeated load of an unchanged file is a pure memory hit (no readFileSync) while
 *    any content change — our OWN write or another instance's — changes the identity
 *    and re-reads (parity: config.ts in-memory global-config cache + settingsCache's
 *    miss/negative distinction).
 *
 *  - A single resetConfigCache() invalidation point wired into every config write
 *    (safeWriteConfig), mirroring the reference's resetSettingsCache() fired on
 *    write / --add-dir / plugin-init / hooks-refresh.
 *
 * RED (pre-feature): loadGlobalConfig re-read the file on every call (no cache), so
 * the "second identical load performs no readFileSync" assertion FAILS, and
 * resetConfigCache / the memoized default-path resolver do not exist.
 */
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  loadGlobalConfig,
  loadProjectConfig,
  getManagedConfigPath,
  safeWriteConfig,
  resetConfigCache,
  fsHooks,
} from '../src/config';

/** Run `fn` with `process.platform` temporarily forced to `platform`. */
function withPlatform(platform: NodeJS.Platform, fn: () => void): void {
  const orig = Object.getOwnPropertyDescriptor(process, 'platform')!;
  Object.defineProperty(process, 'platform', { value: platform, configurable: true });
  try {
    fn();
  } finally {
    Object.defineProperty(process, 'platform', orig);
  }
}

function mkTemp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-u010-w110-'));
}

const origHome = process.env.HOME;
const origUserProfile = process.env.USERPROFILE;
const origManaged = process.env.SUPERINFERENCE_MANAGED_CONFIG;
const origProgramData = process.env.PROGRAMDATA;
const origReadFileSync = fsHooks.readFileSync;

afterEach(() => {
  fsHooks.readFileSync = origReadFileSync;
  resetConfigCache();
  if (origHome === undefined) delete process.env.HOME; else process.env.HOME = origHome;
  if (origUserProfile === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = origUserProfile;
  if (origManaged === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG; else process.env.SUPERINFERENCE_MANAGED_CONFIG = origManaged;
  if (origProgramData === undefined) delete process.env.PROGRAMDATA; else process.env.PROGRAMDATA = origProgramData;
});

/** Count readFileSync calls while running `fn`. */
function countReads(fn: () => void): number {
  let n = 0;
  fsHooks.readFileSync = ((...args: Parameters<typeof origReadFileSync>) => {
    n++;
    return (origReadFileSync as (...a: unknown[]) => unknown)(...args);
  }) as typeof fsHooks.readFileSync;
  try {
    fn();
  } finally {
    fsHooks.readFileSync = origReadFileSync;
  }
  return n;
}

describe('U010/w110 — memoized managed-config default path (gap #4)', () => {
  it('resolves the OS-conventional device path per platform (memoized but platform-keyed)', () => {
    delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    resetConfigCache();
    withPlatform('linux', () => {
      assert.equal(getManagedConfigPath(), '/etc/superinference/config.json');
      // Second call on the same platform returns the identical memoized value.
      assert.equal(getManagedConfigPath(), '/etc/superinference/config.json');
    });
    // Flipping the platform must NOT return a stale first-platform value.
    withPlatform('darwin', () => {
      assert.equal(getManagedConfigPath(), '/Library/Application Support/SuperInference/config.json');
    });
    withPlatform('win32', () => {
      process.env.PROGRAMDATA = 'D:\\PD';
      assert.equal(getManagedConfigPath(), path.join('D:\\PD', 'SuperInference', 'config.json'));
    });
  });

  it('an explicit SUPERINFERENCE_MANAGED_CONFIG override still wins over the memo', () => {
    resetConfigCache();
    withPlatform('linux', () => {
      assert.equal(getManagedConfigPath(), '/etc/superinference/config.json');
      process.env.SUPERINFERENCE_MANAGED_CONFIG = '/tmp/override/config.json';
      assert.equal(getManagedConfigPath(), '/tmp/override/config.json');
    });
  });
});

describe('U010/w110 — per-tier config read cache (gaps #2 + #6)', () => {
  it('serves an unchanged global config from memory (no second readFileSync)', () => {
    const home = mkTemp();
    process.env.HOME = home;
    delete process.env.USERPROFILE;
    fs.mkdirSync(path.join(home, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(home, '.ami', 'config.json'), JSON.stringify({ maxTurns: 7 }));
    resetConfigCache();

    const firstReads = countReads(() => {
      assert.equal(loadGlobalConfig()?.maxTurns, 7);
    });
    assert.ok(firstReads >= 1, 'first load must hit disk');

    const secondReads = countReads(() => {
      assert.equal(loadGlobalConfig()?.maxTurns, 7);
    });
    assert.equal(secondReads, 0, 'second identical load must be a pure memory hit');
  });

  it('re-reads when the file content changes (identity miss)', () => {
    const home = mkTemp();
    process.env.HOME = home;
    delete process.env.USERPROFILE;
    fs.mkdirSync(path.join(home, '.ami'), { recursive: true });
    const p = path.join(home, '.ami', 'config.json');
    fs.writeFileSync(p, JSON.stringify({ maxTurns: 1 }));
    resetConfigCache();
    assert.equal(loadGlobalConfig()?.maxTurns, 1);
    // Rewrite with a DIFFERENT size so mtime+size identity changes even at coarse mtime.
    fs.writeFileSync(p, JSON.stringify({ maxTurns: 222222 }));
    assert.equal(loadGlobalConfig()?.maxTurns, 222222);
  });

  it('negative-caches an absent global config (no readFileSync on repeat)', () => {
    const home = mkTemp();
    process.env.HOME = home;
    delete process.env.USERPROFILE;
    resetConfigCache();
    assert.equal(loadGlobalConfig(), null);
    const reads = countReads(() => {
      assert.equal(loadGlobalConfig(), null);
    });
    assert.equal(reads, 0, 'repeat absent-load must be a negative-cache memory hit');
  });

  it('returns an isolated clone — mutating the result cannot poison the cache', () => {
    const home = mkTemp();
    process.env.HOME = home;
    delete process.env.USERPROFILE;
    fs.mkdirSync(path.join(home, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(home, '.ami', 'config.json'), JSON.stringify({ maxTurns: 3 }));
    resetConfigCache();
    const first = loadGlobalConfig()!;
    (first as { maxTurns?: number }).maxTurns = 999;
    assert.equal(loadGlobalConfig()?.maxTurns, 3, 'cache must not be mutated by a caller');
  });

  it('resetConfigCache() forces the next load to re-read from disk', () => {
    const home = mkTemp();
    process.env.HOME = home;
    delete process.env.USERPROFILE;
    fs.mkdirSync(path.join(home, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(home, '.ami', 'config.json'), JSON.stringify({ maxTurns: 5 }));
    resetConfigCache();
    loadGlobalConfig();
    resetConfigCache();
    const reads = countReads(() => loadGlobalConfig());
    assert.ok(reads >= 1, 'after reset the cache is cold and must re-read');
  });

  it('safeWriteConfig invalidates the cache so a follow-up load sees the new value', () => {
    const home = mkTemp();
    process.env.HOME = home;
    delete process.env.USERPROFILE;
    const p = path.join(home, '.ami', 'config.json');
    fs.mkdirSync(path.join(home, '.ami'), { recursive: true });
    fs.writeFileSync(p, JSON.stringify({ maxTurns: 1 }));
    resetConfigCache();
    assert.equal(loadGlobalConfig()?.maxTurns, 1); // warm the cache
    safeWriteConfig(p, { maxTurns: 42 });
    assert.equal(loadGlobalConfig()?.maxTurns, 42, 'write must invalidate the read cache');
  });

  it('also caches the project tier (unchanged project config → no re-read)', () => {
    const cwd = mkTemp();
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(cwd, '.ami', 'config.json'), JSON.stringify({ maxTurns: 9 }));
    resetConfigCache();
    assert.equal(loadProjectConfig(cwd)?.maxTurns, 9);
    const reads = countReads(() => {
      assert.equal(loadProjectConfig(cwd)?.maxTurns, 9);
    });
    assert.equal(reads, 0, 'second identical project load must be a memory hit');
  });
});
