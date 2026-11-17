import { describe, it, afterEach, beforeEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import {
  MACOS_MANAGED_PREFERENCE_DOMAIN,
  WINDOWS_REGISTRY_KEY_PATH_HKLM,
  WINDOWS_REGISTRY_KEY_PATH_HKCU,
  WINDOWS_REGISTRY_VALUE_NAME,
  PLUTIL_PATH,
  PLUTIL_ARGS_PREFIX,
  MDM_SUBPROCESS_TIMEOUT_MS,
  getMacOSManagedPlistPaths,
  parseRegQueryStdout,
  readNativeManagedPolicy,
  execHooks,
} from '../src/managed-policy-source';
import {
  loadManagedConfig,
  mergeConfigs,
  validateProjectConfigValues,
  findUnknownConfigKeys,
  KNOWN_CONFIG_KEYS,
  fsHooks,
  type ProjectConfig,
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

// ---------------------------------------------------------------------------
// U010 — native OS managed-policy source (gaps 1-4): constants + plist paths +
// reg-query parsing + timeout-bounded, never-throwing acquisition.
// ---------------------------------------------------------------------------

describe('managed-policy-source — constants (gap 1)', () => {
  it('pins the Windows Group-Policy key paths under SOFTWARE\\Policies (WOW64-shared, no redirection)', () => {
    assert.equal(WINDOWS_REGISTRY_KEY_PATH_HKLM, 'HKLM\\SOFTWARE\\Policies\\SuperInference');
    assert.equal(WINDOWS_REGISTRY_KEY_PATH_HKCU, 'HKCU\\SOFTWARE\\Policies\\SuperInference');
    assert.equal(WINDOWS_REGISTRY_VALUE_NAME, 'Settings');
  });
  it('pins the macOS plutil binary + JSON-to-stdout args + a bounded timeout', () => {
    assert.equal(PLUTIL_PATH, '/usr/bin/plutil');
    assert.deepEqual([...PLUTIL_ARGS_PREFIX], ['-convert', 'json', '-o', '-', '--']);
    assert.equal(MDM_SUBPROCESS_TIMEOUT_MS, 5000);
    assert.equal(MACOS_MANAGED_PREFERENCE_DOMAIN, 'org.superinference.ami');
  });
});

describe('getMacOSManagedPlistPaths (gap 3) — per-user before device', () => {
  it('orders the per-user managed plist BEFORE the device-level one when a username resolves', () => {
    const paths = getMacOSManagedPlistPaths();
    const username = os.userInfo().username;
    const device = `/Library/Managed Preferences/${MACOS_MANAGED_PREFERENCE_DOMAIN}.plist`;
    assert.equal(paths[paths.length - 1]!.path, device, 'device plist is the last candidate');
    if (username) {
      assert.equal(
        paths[0]!.path,
        `/Library/Managed Preferences/${username}/${MACOS_MANAGED_PREFERENCE_DOMAIN}.plist`,
      );
      assert.equal(paths.length, 2);
    }
  });
});

describe('parseRegQueryStdout (gap 3) — extract the Settings value', () => {
  it('parses a REG_SZ Settings line', () => {
    const out = [
      '',
      'HKEY_LOCAL_MACHINE\\SOFTWARE\\Policies\\SuperInference',
      '    Settings    REG_SZ    {"permissionMode":"deny-all"}',
      '',
    ].join('\r\n');
    assert.equal(parseRegQueryStdout(out), '{"permissionMode":"deny-all"}');
  });
  it('parses a REG_EXPAND_SZ value case-insensitively', () => {
    const out = '    settings    reg_expand_sz    {"a":1}';
    assert.equal(parseRegQueryStdout(out, 'Settings'), '{"a":1}');
  });
  it('returns null when the value name is absent', () => {
    assert.equal(parseRegQueryStdout('    Other    REG_SZ    x'), null);
  });
  it('honors a custom value name and escapes regex metachars in it', () => {
    assert.equal(parseRegQueryStdout('    My.Val    REG_SZ    hi', 'My.Val'), 'hi');
    assert.equal(parseRegQueryStdout('    MyXVal    REG_SZ    hi', 'My.Val'), null);
  });
});

describe('readNativeManagedPolicy (gaps 2-4) — platform-aware, bounded, never-throwing', () => {
  afterEach(() => mock.restoreAll());

  it('returns null on Linux (no native policy store, no subprocess spawned)', () => {
    const exec = mock.method(execHooks, 'execFileSync', (() => {
      throw new Error('should not spawn on linux');
    }) as typeof execHooks.execFileSync);
    withPlatform('linux', () => {
      assert.equal(readNativeManagedPolicy(), null);
    });
    assert.equal(exec.mock.callCount(), 0);
  });

  it('darwin: reads + converts the first EXISTING plist via plutil (device fallback)', () => {
    const device = `/Library/Managed Preferences/${MACOS_MANAGED_PREFERENCE_DOMAIN}.plist`;
    mock.method(execHooks, 'existsSync', ((p: fs.PathLike) => String(p) === device) as typeof fs.existsSync);
    const exec = mock.method(execHooks, 'execFileSync', ((cmd: string, args: string[]) => {
      assert.equal(cmd, PLUTIL_PATH);
      assert.ok(args.includes(device));
      return '{"model":"from-plist"}\n';
    }) as typeof execHooks.execFileSync);
    withPlatform('darwin', () => {
      const res = readNativeManagedPolicy();
      assert.deepEqual(res, { raw: '{"model":"from-plist"}', source: `device managed preferences (${device})` });
    });
    assert.equal(exec.mock.callCount(), 1);
  });

  it('darwin: skips the plutil spawn entirely when no plist file exists', () => {
    mock.method(execHooks, 'existsSync', (() => false) as typeof fs.existsSync);
    const exec = mock.method(execHooks, 'execFileSync', (() => '{}') as typeof execHooks.execFileSync);
    withPlatform('darwin', () => {
      assert.equal(readNativeManagedPolicy(), null);
    });
    assert.equal(exec.mock.callCount(), 0);
  });

  it('darwin: a plutil failure (timeout/error) degrades to null, never throws', () => {
    mock.method(execHooks, 'existsSync', (() => true) as typeof fs.existsSync);
    mock.method(execHooks, 'execFileSync', (() => {
      const err = new Error('timed out') as NodeJS.ErrnoException;
      err.code = 'ETIMEDOUT';
      throw err;
    }) as typeof execHooks.execFileSync);
    withPlatform('darwin', () => {
      assert.doesNotThrow(() => assert.equal(readNativeManagedPolicy(), null));
    });
  });

  it('win32: reads HKLM first via reg query and parses the Settings JSON', () => {
    const exec = mock.method(execHooks, 'execFileSync', ((_cmd: string, args: string[]) => {
      if (args.includes(WINDOWS_REGISTRY_KEY_PATH_HKLM)) {
        return `\r\n${WINDOWS_REGISTRY_KEY_PATH_HKLM}\r\n    Settings    REG_SZ    {"safeMode":true}\r\n`;
      }
      return '';
    }) as typeof execHooks.execFileSync);
    withPlatform('win32', () => {
      const res = readNativeManagedPolicy();
      assert.deepEqual(res, {
        raw: '{"safeMode":true}',
        source: `${WINDOWS_REGISTRY_KEY_PATH_HKLM}\\${WINDOWS_REGISTRY_VALUE_NAME}`,
      });
    });
    assert.equal(exec.mock.callCount(), 1, 'HKLM hit short-circuits HKCU');
  });

  it('win32: falls through to HKCU when HKLM has no value', () => {
    mock.method(execHooks, 'execFileSync', ((_cmd: string, args: string[]) => {
      if (args.includes(WINDOWS_REGISTRY_KEY_PATH_HKCU)) {
        return `    Settings    REG_SZ    {"model":"hkcu"}`;
      }
      return ''; // HKLM empty
    }) as typeof execHooks.execFileSync);
    withPlatform('win32', () => {
      const res = readNativeManagedPolicy();
      assert.equal(res?.raw, '{"model":"hkcu"}');
      assert.equal(res?.source, `${WINDOWS_REGISTRY_KEY_PATH_HKCU}\\${WINDOWS_REGISTRY_VALUE_NAME}`);
    });
  });

  it('win32: returns null when neither key holds a value (reg exits non-zero → throws → caught)', () => {
    mock.method(execHooks, 'execFileSync', (() => {
      const err = new Error('key not found') as NodeJS.ErrnoException;
      err.code = '1';
      throw err;
    }) as typeof execHooks.execFileSync);
    withPlatform('win32', () => {
      assert.equal(readNativeManagedPolicy(), null);
    });
  });
});

// ---------------------------------------------------------------------------
// U010 — loadManagedConfig consults the native OS policy (end-to-end wiring)
// ---------------------------------------------------------------------------

describe('loadManagedConfig — native OS policy integration (gaps 2-4)', () => {
  let prevEnv: string | undefined;
  beforeEach(() => {
    prevEnv = process.env.SUPERINFERENCE_MANAGED_CONFIG;
    delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
  });
  afterEach(() => {
    mock.restoreAll();
    if (prevEnv === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = prevEnv;
  });

  it('applies native Windows policy when no file-based managed config exists', () => {
    mock.method(fsHooks, 'existsSync', (() => false) as typeof fs.existsSync);
    mock.method(execHooks, 'execFileSync', ((_c: string, args: string[]) =>
      args.includes(WINDOWS_REGISTRY_KEY_PATH_HKLM)
        ? `    Settings    REG_SZ    {"permissionMode":"deny-all","safeMode":true}`
        : '') as typeof execHooks.execFileSync);
    withPlatform('win32', () => {
      const cfg = loadManagedConfig();
      assert.equal(cfg?.permissionMode, 'deny-all');
      assert.equal(cfg?.safeMode, true);
    });
  });

  it('native OS policy WINS over a file-based managed config on a conflicting key, keeping non-conflicting file keys', () => {
    const managedPath = '/custom/managed.json';
    process.env.SUPERINFERENCE_MANAGED_CONFIG = managedPath;
    mock.method(fsHooks, 'existsSync', ((p: fs.PathLike) => String(p) === managedPath) as typeof fs.existsSync);
    mock.method(fsHooks, 'lstatSync', (() => ({
      isSymbolicLink: () => false,
      mode: 0o644,
    })) as unknown as typeof fs.lstatSync);
    mock.method(fsHooks, 'readFileSync', (() => '{"model":"file-model","maxTurns":5}') as typeof fs.readFileSync);
    mock.method(execHooks, 'execFileSync', ((_c: string, args: string[]) =>
      args.includes(WINDOWS_REGISTRY_KEY_PATH_HKLM)
        ? `    Settings    REG_SZ    {"model":"native-model"}`
        : '') as typeof execHooks.execFileSync);
    withPlatform('win32', () => {
      const cfg = loadManagedConfig();
      assert.equal(cfg?.model, 'native-model', 'native policy overrides the file value');
      assert.equal(cfg?.maxTurns, 5, 'non-conflicting file key is preserved');
    });
  });

  it('a malformed native payload (not JSON / not an object) is ignored, not fatal', () => {
    mock.method(fsHooks, 'existsSync', (() => false) as typeof fs.existsSync);
    mock.method(execHooks, 'execFileSync', ((_c: string, args: string[]) =>
      args.includes(WINDOWS_REGISTRY_KEY_PATH_HKLM)
        ? `    Settings    REG_SZ    not-valid-json{`
        : '') as typeof execHooks.execFileSync);
    withPlatform('win32', () => {
      assert.doesNotThrow(() => {
        assert.equal(loadManagedConfig(), null);
      });
    });
  });

  it('a native payload that is a JSON array (not an object) is ignored', () => {
    mock.method(fsHooks, 'existsSync', (() => false) as typeof fs.existsSync);
    mock.method(execHooks, 'execFileSync', ((_c: string, args: string[]) =>
      args.includes(WINDOWS_REGISTRY_KEY_PATH_HKLM)
        ? `    Settings    REG_SZ    [1,2,3]`
        : '') as typeof execHooks.execFileSync);
    withPlatform('win32', () => {
      assert.equal(loadManagedConfig(), null);
    });
  });
});

// ---------------------------------------------------------------------------
// U010 — autoUpdatesChannel + minimumVersion recognized config knobs (gap 14)
// ---------------------------------------------------------------------------

describe('update-channel knobs (gap 14)', () => {
  it('autoUpdatesChannel + minimumVersion are recognized keys (not flagged unknown)', () => {
    assert.ok(KNOWN_CONFIG_KEYS.has('autoUpdatesChannel'));
    assert.ok(KNOWN_CONFIG_KEYS.has('minimumVersion'));
    const unknown = findUnknownConfigKeys({ autoUpdatesChannel: 'stable', minimumVersion: '1.2.3' } as ProjectConfig);
    assert.deepEqual(unknown, []);
  });

  it('mergeConfigs carries both across layers (project value wins over global)', () => {
    const merged = mergeConfigs(
      {}, // cliArgs
      {}, // envVars
      { autoUpdatesChannel: 'stable' }, // projectConfig (higher precedence)
      { autoUpdatesChannel: 'latest', minimumVersion: '1.0.0' }, // globalConfig (lower)
    );
    assert.equal(merged.autoUpdatesChannel, 'stable');
    assert.equal(merged.minimumVersion, '1.0.0');
  });

  it('validateProjectConfigValues rejects a bad autoUpdatesChannel enum but accepts a valid one', () => {
    const bad = validateProjectConfigValues({ autoUpdatesChannel: 'nightly' as unknown as 'latest' });
    assert.ok(bad.some((i) => i.field === 'autoUpdatesChannel'));
    const ok = validateProjectConfigValues({ autoUpdatesChannel: 'stable', minimumVersion: '2.0.0' });
    assert.equal(ok.length, 0);
  });

  it('validateProjectConfigValues rejects a non-string minimumVersion', () => {
    const issues = validateProjectConfigValues({ minimumVersion: 42 as unknown as string });
    assert.ok(issues.some((i) => i.field === 'minimumVersion'));
  });
});
