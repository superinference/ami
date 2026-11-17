/**
 * Native-installer (system package-manager) detection — unit + integration tests.
 *
 * Covers the exec-path classifiers (mise/asdf/homebrew/winget), the
 * `/etc/os-release` parse + distro-family gating, the Linux DB-ownership probes,
 * the `getPackageManager` cascade, and the per-manager upgrade instruction.
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  getInstallPlatform,
  getOsRelease,
  isDistroFamily,
  detectMise,
  detectAsdf,
  detectHomebrew,
  detectWinget,
  detectPacman,
  detectDeb,
  detectRpm,
  detectApk,
  getPackageManager,
  upgradeInstructionFor,
  resetPackageManagerCaches,
  packageManagerHooks,
  type PackageManager,
} from '../src/index';
import type { ExecFileNoThrowResult } from '../src/utils/shell';

type ExecResult = ExecFileNoThrowResult;

function ok(stdout: string): ExecResult {
  return { stdout, stderr: '', code: 0 };
}
function fail(): ExecResult {
  return { stdout: '', stderr: 'not found', code: 1 };
}

/** Seed all hooks to a known baseline (linux, no match, failing probes). */
function seed(opts: {
  platform?: 'macos' | 'linux' | 'wsl' | 'windows';
  execPath?: string;
  osRelease?: string | Error;
  exec?: (file: string, args: string[]) => ExecResult;
} = {}): void {
  mock.method(packageManagerHooks, 'platform', () => opts.platform ?? 'linux');
  mock.method(packageManagerHooks, 'execPath', () => opts.execPath ?? '/usr/bin/ami');
  mock.method(packageManagerHooks, 'readOsRelease', async () => {
    if (opts.osRelease instanceof Error) throw opts.osRelease;
    if (opts.osRelease === undefined) throw new Error('ENOENT');
    return opts.osRelease;
  });
  mock.method(packageManagerHooks, 'execFile', async (file: string, args: string[]) =>
    opts.exec ? opts.exec(file, args) : fail(),
  );
}

describe('package-manager: getInstallPlatform', () => {
  it('maps darwin → macos and win32 → windows', () => {
    assert.equal(getInstallPlatform('darwin', {}), 'macos');
    assert.equal(getInstallPlatform('win32', {}), 'windows');
  });
  it('maps linux without WSL markers → linux', () => {
    assert.equal(getInstallPlatform('linux', {}), 'linux');
  });
  it('maps linux WITH a WSL marker → wsl', () => {
    assert.equal(getInstallPlatform('linux', { WSL_DISTRO_NAME: 'Ubuntu' }), 'wsl');
    assert.equal(getInstallPlatform('linux', { WSL_INTEROP: '/run/x' }), 'wsl');
  });
  it('defaults to live process.platform when no arg is given', () => {
    assert.ok(['macos', 'linux', 'wsl', 'windows'].includes(getInstallPlatform()));
  });
});

describe('package-manager: getOsRelease + isDistroFamily', () => {
  beforeEach(() => resetPackageManagerCaches());
  afterEach(() => { mock.reset(); resetPackageManagerCaches(); });

  it('parses ID and ID_LIKE (quote-stripped, space-split)', async () => {
    seed({ osRelease: 'NAME="Ubuntu"\nID=ubuntu\nID_LIKE="debian gnu"\n' });
    const r = await getOsRelease();
    assert.deepEqual(r, { id: 'ubuntu', idLike: ['debian', 'gnu'] });
  });
  it('defaults missing ID/ID_LIKE to empty/[]', async () => {
    seed({ osRelease: 'NAME="Weird"\n' });
    assert.deepEqual(await getOsRelease(), { id: '', idLike: [] });
  });
  it('returns null when the file is unreadable', async () => {
    seed({ osRelease: new Error('ENOENT') });
    assert.equal(await getOsRelease(), null);
  });
  it('memoizes: a second call does not re-read the file', async () => {
    seed({ osRelease: 'ID=fedora\n' });
    await getOsRelease();
    const calls = (packageManagerHooks.readOsRelease as unknown as { mock: { calls: unknown[] } }).mock.calls.length;
    await getOsRelease();
    assert.equal((packageManagerHooks.readOsRelease as unknown as { mock: { calls: unknown[] } }).mock.calls.length, calls);
  });
  it('isDistroFamily matches by id OR idLike, else false', () => {
    assert.equal(isDistroFamily({ id: 'arch', idLike: [] }, ['arch']), true);
    assert.equal(isDistroFamily({ id: 'ubuntu', idLike: ['debian'] }, ['debian']), true);
    assert.equal(isDistroFamily({ id: 'fedora', idLike: [] }, ['debian']), false);
  });
});

describe('package-manager: exec-path classifiers', () => {
  afterEach(() => { mock.reset(); resetPackageManagerCaches(); });

  it('detectMise matches a mise installs dir, else false', () => {
    seed({ execPath: '/home/u/.local/share/mise/installs/ami/1.0/bin/ami' });
    assert.equal(detectMise(), true);
    mock.reset();
    seed({ execPath: '/usr/bin/ami' });
    assert.equal(detectMise(), false);
  });
  it('detectAsdf matches an asdf installs dir (with or without dot), else false', () => {
    seed({ execPath: '/home/u/.asdf/installs/ami/1.0/bin/ami' });
    assert.equal(detectAsdf(), true);
    mock.reset();
    seed({ execPath: '/usr/bin/ami' });
    assert.equal(detectAsdf(), false);
  });
  it('detectHomebrew matches /Caskroom/ on macos/linux/wsl but NOT windows', () => {
    seed({ platform: 'macos', execPath: '/opt/homebrew/Caskroom/superinference/1.0/ami' });
    assert.equal(detectHomebrew(), true);
    mock.reset();
    // npm-global under Homebrew node must NOT be reported as a cask
    seed({ platform: 'macos', execPath: '/opt/homebrew/lib/node_modules/ami/bin/ami' });
    assert.equal(detectHomebrew(), false);
    mock.reset();
    seed({ platform: 'windows', execPath: '/Caskroom/ami' });
    assert.equal(detectHomebrew(), false);
  });
  it('detectWinget matches WinGet Packages/Links on windows only', () => {
    seed({ platform: 'windows', execPath: 'C:/Users/u/AppData/Local/Microsoft/WinGet/Packages/ami/ami.exe' });
    assert.equal(detectWinget(), true);
    mock.reset();
    seed({ platform: 'windows', execPath: 'C:/Users/u/AppData/Local/Microsoft/WinGet/Links/ami.exe' });
    assert.equal(detectWinget(), true);
    mock.reset();
    seed({ platform: 'windows', execPath: 'C:/other/ami.exe' });
    assert.equal(detectWinget(), false);
    mock.reset();
    seed({ platform: 'linux', execPath: 'C:/Microsoft/WinGet/Packages/ami.exe' });
    assert.equal(detectWinget(), false);
  });
});

describe('package-manager: Linux DB-ownership probes', () => {
  beforeEach(() => resetPackageManagerCaches());
  afterEach(() => { mock.reset(); resetPackageManagerCaches(); });

  it('detectDeb: owned on debian family returns true', async () => {
    seed({ osRelease: 'ID=ubuntu\nID_LIKE=debian\n', exec: () => ok('ami: /usr/bin/ami') });
    assert.equal(await detectDeb(), true);
  });
  it('detectDeb: wrong distro family short-circuits (no exec)', async () => {
    seed({ osRelease: 'ID=fedora\n', exec: () => ok('x') });
    assert.equal(await detectDeb(), false);
    assert.equal((packageManagerHooks.execFile as unknown as { mock: { calls: unknown[] } }).mock.calls.length, 0);
  });
  it('detectDeb: unreadable os-release falls through to the exec', async () => {
    seed({ osRelease: new Error('ENOENT'), exec: () => ok('ami: /usr/bin/ami') });
    assert.equal(await detectDeb(), true);
  });
  it('detectDeb: non-linux platform returns false', async () => {
    seed({ platform: 'macos', exec: () => ok('x') });
    assert.equal(await detectDeb(), false);
  });
  it('detectDeb: clean exit but EMPTY stdout returns false', async () => {
    seed({ osRelease: 'ID=debian\n', exec: () => ok('') });
    assert.equal(await detectDeb(), false);
  });
  it('detectRpm/detectApk/detectPacman gate on their families and query their DB', async () => {
    seed({ osRelease: 'ID=fedora\n', exec: (f) => (f === 'rpm' ? ok('ami-1.0') : fail()) });
    assert.equal(await detectRpm(), true);
    mock.reset(); resetPackageManagerCaches();
    seed({ osRelease: 'ID=alpine\n', exec: (f, a) => (f === 'apk' && a[0] === 'info' ? ok('ami') : fail()) });
    assert.equal(await detectApk(), true);
    mock.reset(); resetPackageManagerCaches();
    seed({ osRelease: 'ID=arch\n', exec: (f, a) => (f === 'pacman' && a[0] === '-Qo' ? ok('ami') : fail()) });
    assert.equal(await detectPacman(), true);
  });
});

describe('package-manager: getPackageManager cascade', () => {
  beforeEach(() => resetPackageManagerCaches());
  afterEach(() => { mock.reset(); resetPackageManagerCaches(); });

  const cases: Array<{ name: PackageManager; setup: () => void }> = [
    { name: 'homebrew', setup: () => seed({ platform: 'macos', execPath: '/opt/homebrew/Caskroom/ami/ami' }) },
    { name: 'winget', setup: () => seed({ platform: 'windows', execPath: 'C:/Microsoft/WinGet/Packages/ami.exe' }) },
    { name: 'mise', setup: () => seed({ execPath: '/u/.local/share/mise/installs/ami/1/ami' }) },
    { name: 'asdf', setup: () => seed({ execPath: '/u/.asdf/installs/ami/1/ami' }) },
    { name: 'pacman', setup: () => seed({ osRelease: 'ID=arch\n', exec: (f) => (f === 'pacman' ? ok('ami') : fail()) }) },
    { name: 'apk', setup: () => seed({ osRelease: 'ID=alpine\n', exec: (f) => (f === 'apk' ? ok('ami') : fail()) }) },
    { name: 'deb', setup: () => seed({ osRelease: 'ID=debian\n', exec: (f) => (f === 'dpkg' ? ok('ami') : fail()) }) },
    { name: 'rpm', setup: () => seed({ osRelease: 'ID=fedora\n', exec: (f) => (f === 'rpm' ? ok('ami') : fail()) }) },
    { name: 'unknown', setup: () => seed({ osRelease: 'ID=void\n' }) },
  ];

  for (const c of cases) {
    it(`classifies as ${c.name}`, async () => {
      c.setup();
      assert.equal(await getPackageManager(), c.name);
    });
    it(`memoizes the ${c.name} result`, async () => {
      c.setup();
      const first = await getPackageManager();
      assert.equal(await getPackageManager(), first);
    });
  }
});

describe('package-manager: upgradeInstructionFor', () => {
  it('returns a non-empty command for every manager', () => {
    const all: PackageManager[] = ['homebrew', 'winget', 'pacman', 'deb', 'rpm', 'apk', 'mise', 'asdf', 'unknown'];
    for (const pm of all) {
      assert.ok(upgradeInstructionFor(pm).length > 0, `${pm} must map to a command`);
    }
  });
  it('maps each manager to its native upgrade tool', () => {
    assert.match(upgradeInstructionFor('homebrew'), /brew upgrade/);
    assert.match(upgradeInstructionFor('winget'), /winget upgrade/);
    assert.match(upgradeInstructionFor('pacman'), /pacman -Syu/);
    assert.match(upgradeInstructionFor('deb'), /apt-get/);
    assert.match(upgradeInstructionFor('rpm'), /dnf upgrade/);
    assert.match(upgradeInstructionFor('apk'), /apk upgrade/);
    assert.match(upgradeInstructionFor('mise'), /mise upgrade/);
    assert.match(upgradeInstructionFor('asdf'), /asdf install/);
    assert.match(upgradeInstructionFor('unknown'), /install\.sh/);
  });
});
