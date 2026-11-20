import { describe, it, afterEach, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { join } from 'node:path';
import { homedir } from 'node:os';

import {
  getPlatform,
  getSystemDirectories,
  __resetPlatformCacheForTest,
  type Platform,
} from '../src/utils/platform';

// ---------------------------------------------------------------------------
// U425 — canonical typed host-platform detection (parity with the reference's
// utils/platform.ts + utils/systemDirectories.ts): the five-member Platform enum
// incl. a distinct `wsl` and an observable `unknown` (one-shot debug log), plus
// XDG-/USERPROFILE-aware system-directory resolution.
// ---------------------------------------------------------------------------

describe('getPlatform — typed Platform enum incl. wsl + unknown', () => {
  beforeEach(() => __resetPlatformCacheForTest());
  afterEach(() => __resetPlatformCacheForTest());

  it('maps the three OS families to the typed enum', () => {
    assert.equal(getPlatform({ platform: 'darwin' }), 'macos');
    assert.equal(getPlatform({ platform: 'win32' }), 'windows');
    assert.equal(getPlatform({ platform: 'linux', isWsl: () => false }), 'linux');
  });

  it('distinguishes wsl from native linux via the injected probe', () => {
    assert.equal(getPlatform({ platform: 'linux', isWsl: () => true }), 'wsl');
    assert.equal(getPlatform({ platform: 'linux', isWsl: () => false }), 'linux');
  });

  it('threads the env through to the WSL probe', () => {
    let seen: NodeJS.ProcessEnv | undefined = {};
    const env = { WSL_DISTRO_NAME: 'Ubuntu' } as unknown as NodeJS.ProcessEnv;
    getPlatform({ platform: 'linux', env, isWsl: (e) => { seen = e; return false; } });
    assert.equal(seen, env);
  });

  it('classifies a long-tail host as unknown and logs ONCE per distinct id', () => {
    const logged: Array<{ msg: string; data?: Record<string, unknown> }> = [];
    const log = (_c: string, msg: string, data?: Record<string, unknown>) => { logged.push({ msg, data }); };
    assert.equal(getPlatform({ platform: 'freebsd', log }), 'unknown');
    assert.equal(getPlatform({ platform: 'freebsd', log }), 'unknown'); // same id again
    assert.equal(getPlatform({ platform: 'sunos', log }), 'unknown'); // different id
    assert.equal(logged.length, 2, 'one log per distinct unknown id');
    assert.match(logged[0].msg, /Unknown platform detected/);
    assert.equal(logged[0].data?.platform, 'freebsd');
    assert.equal(logged[1].data?.platform, 'sunos');
  });

  it('memoizes the zero-options production path', () => {
    const first = getPlatform();
    const second = getPlatform();
    assert.equal(first, second);
    assert.ok((['macos', 'windows', 'wsl', 'linux', 'unknown'] as Platform[]).includes(first));
  });

  it('defaults the platform arg to process.platform when none is given', () => {
    // On the CI/dev host this is a classifiable OS, never unknown.
    const expected: Record<string, Platform | undefined> = { darwin: 'macos', win32: 'windows' };
    const p = getPlatform();
    if (process.platform === 'darwin') assert.equal(p, expected.darwin);
    else if (process.platform === 'win32') assert.equal(p, expected.windows);
    else if (process.platform === 'linux') assert.ok(p === 'linux' || p === 'wsl');
  });
});

describe('getSystemDirectories — XDG/USERPROFILE-aware resolution', () => {
  afterEach(() => __resetPlatformCacheForTest());

  it('windows honors USERPROFILE for localized folder names', () => {
    const dirs = getSystemDirectories({
      platform: 'windows',
      homedir: '/home/u',
      env: { USERPROFILE: 'C:/Users/José' } as unknown as NodeJS.ProcessEnv,
    });
    assert.equal(dirs.HOME, '/home/u');
    assert.equal(dirs.DESKTOP, join('C:/Users/José', 'Desktop'));
    assert.equal(dirs.DOCUMENTS, join('C:/Users/José', 'Documents'));
  });

  it('windows falls back to homedir when USERPROFILE is unset', () => {
    const dirs = getSystemDirectories({ platform: 'windows', homedir: '/h', env: {} });
    assert.equal(dirs.DESKTOP, join('/h', 'Desktop'));
  });

  it('linux and wsl honor the XDG base directory spec', () => {
    const env = {
      XDG_DESKTOP_DIR: '/x/desk',
      XDG_DOCUMENTS_DIR: '/x/docs',
      XDG_DOWNLOAD_DIR: '/x/dl',
    } as unknown as NodeJS.ProcessEnv;
    for (const platform of ['linux', 'wsl'] as const) {
      const dirs = getSystemDirectories({ platform, homedir: '/home/u', env });
      assert.equal(dirs.DESKTOP, '/x/desk');
      assert.equal(dirs.DOCUMENTS, '/x/docs');
      assert.equal(dirs.DOWNLOADS, '/x/dl');
      assert.equal(dirs.HOME, '/home/u');
    }
  });

  it('linux without XDG uses the default ~/Desktop layout', () => {
    const dirs = getSystemDirectories({ platform: 'linux', homedir: '/home/u', env: {} });
    assert.equal(dirs.DESKTOP, join('/home/u', 'Desktop'));
    assert.equal(dirs.DOWNLOADS, join('/home/u', 'Downloads'));
  });

  it('macos uses standard paths and does NOT log', () => {
    let logged = 0;
    const dirs = getSystemDirectories({ platform: 'macos', homedir: '/Users/u', env: { XDG_DESKTOP_DIR: '/ignored' } as unknown as NodeJS.ProcessEnv, log: () => { logged++; } });
    assert.equal(dirs.DESKTOP, join('/Users/u', 'Desktop'));
    assert.equal(logged, 0);
  });

  it('unknown platform falls back to safe defaults AND emits a debug log', () => {
    const logged: string[] = [];
    const dirs = getSystemDirectories({ platform: 'unknown', homedir: '/h', env: {}, log: (_c, m) => logged.push(m) });
    assert.equal(dirs.DESKTOP, join('/h', 'Desktop'));
    assert.equal(dirs.DOCUMENTS, join('/h', 'Documents'));
    assert.equal(dirs.DOWNLOADS, join('/h', 'Downloads'));
    assert.equal(logged.length, 1);
    assert.match(logged[0], /Unknown platform detected, using default paths/);
  });

  it('defaults homedir to os.homedir() and platform to getPlatform()', () => {
    const dirs = getSystemDirectories();
    assert.equal(dirs.HOME, homedir());
    assert.equal(typeof dirs.DESKTOP, 'string');
  });
});
