import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { join } from 'node:path';
import {
  getSystemDirectories,
  defaultSystemDirectoryPlatform,
} from '../src/utils/system-directories';

const HOME = join('/home', 'tester');

describe('getSystemDirectories', () => {
  it('macOS uses the standard ~/Desktop etc. defaults', () => {
    const d = getSystemDirectories({ platform: 'macos', homedir: HOME, env: {} });
    assert.deepEqual(d, {
      HOME,
      DESKTOP: join(HOME, 'Desktop'),
      DOCUMENTS: join(HOME, 'Documents'),
      DOWNLOADS: join(HOME, 'Downloads'),
    });
  });

  it('unknown platform falls back to the standard defaults', () => {
    const d = getSystemDirectories({ platform: 'unknown', homedir: HOME, env: {} });
    assert.equal(d.DESKTOP, join(HOME, 'Desktop'));
    assert.equal(d.DOCUMENTS, join(HOME, 'Documents'));
    assert.equal(d.DOWNLOADS, join(HOME, 'Downloads'));
  });

  it('Windows prefers USERPROFILE for localized/relocated folders', () => {
    const profile = 'C:\\Users\\Tëster';
    const d = getSystemDirectories({
      platform: 'windows',
      homedir: HOME,
      env: { USERPROFILE: profile },
    });
    assert.equal(d.HOME, HOME);
    assert.equal(d.DESKTOP, join(profile, 'Desktop'));
    assert.equal(d.DOCUMENTS, join(profile, 'Documents'));
    assert.equal(d.DOWNLOADS, join(profile, 'Downloads'));
  });

  it('Windows falls back to homedir when USERPROFILE is unset', () => {
    const d = getSystemDirectories({ platform: 'windows', homedir: HOME, env: {} });
    assert.equal(d.DESKTOP, join(HOME, 'Desktop'));
  });

  it('Linux honors XDG_*_DIR before the defaults', () => {
    const d = getSystemDirectories({
      platform: 'linux',
      homedir: HOME,
      env: {
        XDG_DESKTOP_DIR: '/mnt/desk',
        XDG_DOCUMENTS_DIR: '/mnt/docs',
        XDG_DOWNLOAD_DIR: '/mnt/dl',
      },
    });
    assert.equal(d.DESKTOP, '/mnt/desk');
    assert.equal(d.DOCUMENTS, '/mnt/docs');
    assert.equal(d.DOWNLOADS, '/mnt/dl');
  });

  it('Linux falls back to ~/ defaults when XDG vars are absent', () => {
    const d = getSystemDirectories({ platform: 'linux', homedir: HOME, env: {} });
    assert.equal(d.DESKTOP, join(HOME, 'Desktop'));
    assert.equal(d.DOWNLOADS, join(HOME, 'Downloads'));
  });

  it('WSL uses the same XDG resolution as Linux', () => {
    const d = getSystemDirectories({
      platform: 'wsl',
      homedir: HOME,
      env: { XDG_DOWNLOAD_DIR: '/mnt/c/Downloads' },
    });
    assert.equal(d.DOWNLOADS, '/mnt/c/Downloads');
    assert.equal(d.DESKTOP, join(HOME, 'Desktop'));
  });

  it('Windows does NOT apply XDG vars (platform isolation)', () => {
    const d = getSystemDirectories({
      platform: 'windows',
      homedir: HOME,
      env: { USERPROFILE: HOME, XDG_DOWNLOAD_DIR: '/should/not/apply' },
    });
    assert.equal(d.DOWNLOADS, join(HOME, 'Downloads'));
  });

  it('result is index-signature compatible with Record<string,string>', () => {
    const d = getSystemDirectories({ platform: 'macos', homedir: HOME, env: {} });
    const record: Record<string, string> = d;
    assert.equal(record.HOME, HOME);
  });
});

describe('defaultSystemDirectoryPlatform', () => {
  it('maps darwin -> macos, win32 -> windows', () => {
    assert.equal(defaultSystemDirectoryPlatform('darwin', {}), 'macos');
    assert.equal(defaultSystemDirectoryPlatform('win32', {}), 'windows');
  });

  it('plain linux without WSL markers -> linux', () => {
    assert.equal(defaultSystemDirectoryPlatform('linux', {}), 'linux');
  });

  it('linux with WSL_DISTRO_NAME -> wsl', () => {
    assert.equal(
      defaultSystemDirectoryPlatform('linux', { WSL_DISTRO_NAME: 'Ubuntu' }),
      'wsl',
    );
  });

  it('linux with WSL_INTEROP -> wsl', () => {
    assert.equal(
      defaultSystemDirectoryPlatform('linux', { WSL_INTEROP: '/run/WSL/1_interop' }),
      'wsl',
    );
  });

  it('anything else -> unknown', () => {
    assert.equal(defaultSystemDirectoryPlatform('freebsd' as NodeJS.Platform, {}), 'unknown');
  });

  it('getSystemDirectories without an explicit platform uses the default resolver', () => {
    // No platform option => resolves from process.platform; on the CI linux host
    // with no WSL markers this takes the linux/XDG branch.
    const d = getSystemDirectories({
      homedir: HOME,
      env: { XDG_DESKTOP_DIR: '/x/desk' },
    });
    assert.equal(d.HOME, HOME);
    // On linux the XDG override is honored; on macOS it is ignored — assert the
    // HOME anchor which holds on every host, plus that a string path is produced.
    assert.equal(typeof d.DOWNLOADS, 'string');
  });
});
