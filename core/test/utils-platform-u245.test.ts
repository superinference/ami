import { describe, it, afterEach, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  isMuslEnvironment,
  isNpmFromWindowsPath,
  __resetPlatformCacheForTest,
} from '../src/utils/platform';
import { isWSL } from '../src/utils/env';

// ---------------------------------------------------------------------------
// U245 — platform detection parity with Claude Code's utils/platform.ts,
// utils/env.ts, utils/envDynamic.ts:
//   - isMuslEnvironment() (musl vs glibc via the arch-specific loader stat)
//   - isNpmFromWindowsPath() (WSL npm resolving onto /mnt/c)
//   - isWSL()'s resilient WSLInterop binfmt file probe
// ---------------------------------------------------------------------------

describe('isMuslEnvironment — musl vs glibc detection', () => {
  beforeEach(() => __resetPlatformCacheForTest());
  afterEach(() => __resetPlatformCacheForTest());

  it('returns false on a non-Linux host regardless of the probe', () => {
    assert.equal(
      isMuslEnvironment({ platform: 'darwin', fileExists: () => true }),
      false,
    );
    assert.equal(
      isMuslEnvironment({ platform: 'win32', fileExists: () => true }),
      false,
    );
  });

  it('reports musl when the arch-specific musl loader exists', () => {
    const seen: string[] = [];
    const probe = (p: string) => { seen.push(p); return true; };
    assert.equal(isMuslEnvironment({ platform: 'linux', arch: 'x64', fileExists: probe }), true);
    assert.deepEqual(seen, ['/lib/libc.musl-x86_64.so.1']);
  });

  it('normalizes arch ids (x64→x86_64, arm64→aarch64) and passes others through', () => {
    const paths: string[] = [];
    const probe = (p: string) => { paths.push(p); return false; };
    isMuslEnvironment({ platform: 'linux', arch: 'x64', fileExists: probe });
    isMuslEnvironment({ platform: 'linux', arch: 'arm64', fileExists: probe });
    isMuslEnvironment({ platform: 'linux', arch: 'ppc64', fileExists: probe });
    assert.deepEqual(paths, [
      '/lib/libc.musl-x86_64.so.1',
      '/lib/libc.musl-aarch64.so.1',
      '/lib/libc.musl-ppc64.so.1',
    ]);
  });

  it('degrades to glibc (false) when the loader is absent', () => {
    assert.equal(isMuslEnvironment({ platform: 'linux', arch: 'x64', fileExists: () => false }), false);
  });

  it('memoizes the zero-options production path', () => {
    const first = isMuslEnvironment();
    const second = isMuslEnvironment();
    assert.equal(first, second);
    assert.equal(typeof first, 'boolean');
  });
});

describe('isNpmFromWindowsPath — WSL cross-filesystem npm', () => {
  beforeEach(() => __resetPlatformCacheForTest());
  afterEach(() => __resetPlatformCacheForTest());

  it('is false when not in WSL, without resolving npm at all', () => {
    let resolved = false;
    const r = isNpmFromWindowsPath({
      isWsl: () => false,
      resolve: () => { resolved = true; return '/mnt/c/npm'; },
    });
    assert.equal(r, false);
    assert.equal(resolved, false, 'must not resolve npm off-WSL');
  });

  it('is true in WSL when npm resolves onto /mnt/c', () => {
    assert.equal(
      isNpmFromWindowsPath({ isWsl: () => true, resolve: () => '/mnt/c/Program Files/nodejs/npm' }),
      true,
    );
  });

  it('is false in WSL when npm lives on the Linux filesystem', () => {
    assert.equal(
      isNpmFromWindowsPath({ isWsl: () => true, resolve: () => '/usr/bin/npm' }),
      false,
    );
  });

  it('is false in WSL when npm cannot be resolved', () => {
    assert.equal(isNpmFromWindowsPath({ isWsl: () => true, resolve: () => null }), false);
  });

  it('swallows a throwing resolver and reports false', () => {
    assert.equal(
      isNpmFromWindowsPath({ isWsl: () => true, resolve: () => { throw new Error('boom'); } }),
      false,
    );
  });

  it('memoizes the zero-options production path', () => {
    const first = isNpmFromWindowsPath();
    const second = isNpmFromWindowsPath();
    assert.equal(first, second);
    assert.equal(typeof first, 'boolean');
  });
});

describe('isWSL — resilient WSLInterop binfmt file probe', () => {
  it('detects WSL via the binfmt interop marker even without env vars', () => {
    const seen: string[] = [];
    const probe = (p: string) => { seen.push(p); return true; };
    assert.equal(isWSL({} as NodeJS.ProcessEnv, probe), true);
    assert.deepEqual(seen, ['/proc/sys/fs/binfmt_misc/WSLInterop']);
  });

  it('does not probe the file when an interop env var is already present', () => {
    let probed = false;
    const probe = (_p: string) => { probed = true; return false; };
    assert.equal(isWSL({ WSL_DISTRO_NAME: 'Ubuntu' } as NodeJS.ProcessEnv, probe), true);
    assert.equal(probed, false);
  });

  it('falls through to the kernel-release check when the marker is absent', () => {
    // No env vars, marker absent → result is whatever os.release() reports; must be a boolean.
    assert.equal(typeof isWSL({} as NodeJS.ProcessEnv, () => false), 'boolean');
  });

  it('tolerates a throwing file probe and continues to the kernel check', () => {
    assert.equal(
      typeof isWSL({} as NodeJS.ProcessEnv, () => { throw new Error('eacces'); }),
      'boolean',
    );
  });
});
