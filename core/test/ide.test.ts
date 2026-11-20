/**
 * Unit tests for the JetBrains IDE detection building blocks (core/src/ide.ts).
 *
 * Mirrors the reference's `src/utils/jetbrains.ts` — the `ideNameToDirMap` config
 * table (15 JetBrains IDEs) and `buildCommonPluginDirectoryPaths` platform-aware
 * path resolution with APPDATA/LOCALAPPDATA env fallbacks and XDG-style Linux
 * defaults. The resolver is pure and context-injected, so every platform branch
 * is exercised deterministically regardless of the host OS.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { join } from 'node:path';

import {
  JETBRAINS_IDE_DIR_MAP,
  SUPPORTED_JETBRAINS_IDES,
  isSupportedJetBrainsIde,
  getJetBrainsIdeDirPatterns,
  defaultIdeDirectoryContext,
  resolveJetBrainsConfigDirectories,
  type IdeDirectoryContext,
} from '../src/ide';

const HOME = '/home/dev';
const ctx = (platform: NodeJS.Platform, env: Record<string, string | undefined> = {}): IdeDirectoryContext => ({
  platform,
  homeDir: HOME,
  env,
});

describe('JETBRAINS_IDE_DIR_MAP config table', () => {
  it('enumerates exactly the 15 supported JetBrains IDEs', () => {
    assert.equal(SUPPORTED_JETBRAINS_IDES.length, 15);
    assert.deepEqual(
      [...SUPPORTED_JETBRAINS_IDES].sort(),
      [
        'androidstudio', 'appcode', 'aqua', 'clion', 'datagrip', 'dataspell',
        'fleet', 'gateway', 'goland', 'intellij', 'phpstorm', 'pycharm',
        'rider', 'rubymine', 'webstorm',
      ],
    );
  });

  it('maps IntelliJ to both commercial and community directory patterns', () => {
    assert.deepEqual(JETBRAINS_IDE_DIR_MAP.intellij, ['IntelliJIdea', 'IdeaIC']);
  });

  it('maps single-pattern IDEs to their canonical directory name', () => {
    assert.deepEqual(JETBRAINS_IDE_DIR_MAP.pycharm, ['PyCharm']);
    assert.deepEqual(JETBRAINS_IDE_DIR_MAP.androidstudio, ['AndroidStudio']);
  });

  it('is frozen (config table cannot be mutated at runtime)', () => {
    assert.ok(Object.isFrozen(JETBRAINS_IDE_DIR_MAP));
    assert.ok(Object.isFrozen(SUPPORTED_JETBRAINS_IDES));
  });
});

describe('isSupportedJetBrainsIde', () => {
  it('recognizes known identifiers case-insensitively', () => {
    assert.equal(isSupportedJetBrainsIde('pycharm'), true);
    assert.equal(isSupportedJetBrainsIde('PyCharm'), true);
    assert.equal(isSupportedJetBrainsIde('INTELLIJ'), true);
  });

  it('rejects unknown identifiers', () => {
    assert.equal(isSupportedJetBrainsIde('vscode'), false);
    assert.equal(isSupportedJetBrainsIde('emacs'), false);
    assert.equal(isSupportedJetBrainsIde(''), false);
  });
});

describe('getJetBrainsIdeDirPatterns', () => {
  it('returns patterns for a known IDE (case-insensitive)', () => {
    assert.deepEqual(getJetBrainsIdeDirPatterns('WebStorm'), ['WebStorm']);
    assert.deepEqual(getJetBrainsIdeDirPatterns('intellij'), ['IntelliJIdea', 'IdeaIC']);
  });

  it('returns an empty array for an unknown IDE', () => {
    assert.deepEqual(getJetBrainsIdeDirPatterns('sublime'), []);
  });
});

describe('resolveJetBrainsConfigDirectories', () => {
  it('returns [] for an unknown IDE', () => {
    assert.deepEqual(resolveJetBrainsConfigDirectories('notanide', ctx('linux')), []);
  });

  it('returns [] for an unsupported platform', () => {
    assert.deepEqual(resolveJetBrainsConfigDirectories('pycharm', ctx('aix')), []);
  });

  describe('darwin', () => {
    it('resolves the JetBrains + Application Support base dirs', () => {
      assert.deepEqual(resolveJetBrainsConfigDirectories('pycharm', ctx('darwin')), [
        join(HOME, 'Library', 'Application Support', 'JetBrains'),
        join(HOME, 'Library', 'Application Support'),
      ]);
    });

    it('adds the Google base dir for Android Studio only', () => {
      assert.deepEqual(resolveJetBrainsConfigDirectories('androidstudio', ctx('darwin')), [
        join(HOME, 'Library', 'Application Support', 'JetBrains'),
        join(HOME, 'Library', 'Application Support'),
        join(HOME, 'Library', 'Application Support', 'Google'),
      ]);
    });
  });

  describe('win32', () => {
    it('uses APPDATA/LOCALAPPDATA env vars when present', () => {
      const env = { APPDATA: 'C:\\Roaming', LOCALAPPDATA: 'C:\\Local' };
      assert.deepEqual(resolveJetBrainsConfigDirectories('goland', ctx('win32', env)), [
        join('C:\\Roaming', 'JetBrains'),
        join('C:\\Local', 'JetBrains'),
        'C:\\Roaming',
      ]);
    });

    it('falls back to homeDir-derived AppData paths when env vars are absent', () => {
      assert.deepEqual(resolveJetBrainsConfigDirectories('goland', ctx('win32')), [
        join(join(HOME, 'AppData', 'Roaming'), 'JetBrains'),
        join(join(HOME, 'AppData', 'Local'), 'JetBrains'),
        join(HOME, 'AppData', 'Roaming'),
      ]);
    });

    it('adds the local Google dir for Android Studio', () => {
      const dirs = resolveJetBrainsConfigDirectories('androidstudio', ctx('win32', { LOCALAPPDATA: 'C:\\Local' }));
      assert.ok(dirs.includes(join('C:\\Local', 'Google')));
    });
  });

  describe('linux', () => {
    it('resolves XDG-style config + data dirs plus per-pattern dotdirs', () => {
      assert.deepEqual(resolveJetBrainsConfigDirectories('intellij', ctx('linux')), [
        join(HOME, '.config', 'JetBrains'),
        join(HOME, '.local', 'share', 'JetBrains'),
        join(HOME, '.IntelliJIdea'),
        join(HOME, '.IdeaIC'),
      ]);
    });

    it('adds the Google config dir for Android Studio', () => {
      assert.deepEqual(resolveJetBrainsConfigDirectories('androidstudio', ctx('linux')), [
        join(HOME, '.config', 'JetBrains'),
        join(HOME, '.local', 'share', 'JetBrains'),
        join(HOME, '.AndroidStudio'),
        join(HOME, '.config', 'Google'),
      ]);
    });
  });

  it('defaults the context to live runtime values when omitted', () => {
    // Should not throw and should return an array (content depends on the host OS).
    const dirs = resolveJetBrainsConfigDirectories('pycharm');
    assert.ok(Array.isArray(dirs));
  });
});

describe('defaultIdeDirectoryContext', () => {
  it('reports the live platform, home dir, and process env', () => {
    const live = defaultIdeDirectoryContext();
    assert.equal(live.platform, process.platform);
    assert.equal(typeof live.homeDir, 'string');
    assert.ok(live.homeDir.length > 0);
    assert.equal(live.env, process.env);
  });
});
