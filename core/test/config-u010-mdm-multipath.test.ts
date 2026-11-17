import { describe, it, afterEach, beforeEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  getManagedConfigPath,
  getManagedConfigPaths,
  loadManagedConfig,
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

/** The per-user macOS managed-preferences candidate for the current test user. */
function macUserCandidate(): string {
  const username = os.userInfo().username;
  return `/Library/Managed Preferences/${username}/SuperInference/config.json`;
}

const MAC_DEVICE = '/Library/Managed Preferences/SuperInference/config.json';
const MAC_LEGACY = '/Library/Application Support/SuperInference/config.json';

// ---------------------------------------------------------------------------
// U010 — priority-ordered multi-path managed-config resolution
// (parity: Claude Code getMacOSPlistPaths per-user > device-level ordering)
// ---------------------------------------------------------------------------

describe('getManagedConfigPaths — ordered candidate list', () => {
  let prevEnv: string | undefined;
  let prevProgramData: string | undefined;
  beforeEach(() => {
    prevEnv = process.env.SUPERINFERENCE_MANAGED_CONFIG;
    prevProgramData = process.env.PROGRAMDATA;
    delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
  });
  afterEach(() => {
    if (prevEnv === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = prevEnv;
    if (prevProgramData === undefined) delete process.env.PROGRAMDATA;
    else process.env.PROGRAMDATA = prevProgramData;
  });

  it('returns the single /etc path on Linux (one tier, no precedence to layer)', () => {
    withPlatform('linux', () => {
      assert.deepEqual(getManagedConfigPaths(), ['/etc/superinference/config.json']);
    });
  });

  it('returns the single ProgramData path on Windows', () => {
    process.env.PROGRAMDATA = 'D:\\PData';
    withPlatform('win32', () => {
      assert.deepEqual(getManagedConfigPaths(), [
        path.join('D:\\PData', 'SuperInference', 'config.json'),
      ]);
    });
  });

  it('orders per-user BEFORE device-level BEFORE the legacy default on darwin', () => {
    withPlatform('darwin', () => {
      const paths = getManagedConfigPaths();
      assert.deepEqual(paths, [macUserCandidate(), MAC_DEVICE, MAC_LEGACY]);
    });
  });

  it('keeps getManagedConfigPath (device default) as the LAST darwin candidate (additive superset)', () => {
    withPlatform('darwin', () => {
      const paths = getManagedConfigPaths();
      assert.equal(paths[paths.length - 1], getManagedConfigPath());
    });
  });

  it('never carries a duplicate candidate', () => {
    withPlatform('darwin', () => {
      const paths = getManagedConfigPaths();
      assert.equal(new Set(paths).size, paths.length);
    });
  });

  it('SUPERINFERENCE_MANAGED_CONFIG override is the single authoritative candidate on every platform', () => {
    process.env.SUPERINFERENCE_MANAGED_CONFIG = '/custom/managed.json';
    for (const p of ['linux', 'darwin', 'win32'] as NodeJS.Platform[]) {
      withPlatform(p, () => {
        assert.deepEqual(getManagedConfigPaths(), ['/custom/managed.json']);
      });
    }
  });
});

// ---------------------------------------------------------------------------
// U010 — loadManagedConfig reads the first-present candidate (first-wins)
// ---------------------------------------------------------------------------

describe('loadManagedConfig — first-present candidate selection', () => {
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

  /** Seed the fsHooks seam so `present` paths read `contentByPath[path]`. */
  function seedFs(present: Set<string>, contentByPath: Record<string, string>): void {
    mock.method(fsHooks, 'existsSync', ((p: fs.PathLike) => present.has(String(p))) as typeof fs.existsSync);
    mock.method(fsHooks, 'lstatSync', (() => ({
      isSymbolicLink: () => false,
      mode: 0o600,
    })) as unknown as typeof fs.lstatSync);
    mock.method(fsHooks, 'readFileSync', ((p: fs.PathOrFileDescriptor) => {
      const key = String(p);
      if (key in contentByPath) return contentByPath[key];
      throw Object.assign(new Error('ENOENT'), { code: 'ENOENT' });
    }) as typeof fs.readFileSync);
    // No drop-ins in these scenarios.
    mock.method(fsHooks, 'readdirSync', (() => { throw new Error('no dir'); }) as typeof fs.readdirSync);
  }

  it('reads the per-user source when it exists (highest priority)', () => {
    withPlatform('darwin', () => {
      const user = macUserCandidate();
      seedFs(
        new Set([user, MAC_DEVICE, MAC_LEGACY]),
        {
          [user]: JSON.stringify({ model: 'from-per-user' }),
          [MAC_DEVICE]: JSON.stringify({ model: 'from-device' }),
          [MAC_LEGACY]: JSON.stringify({ model: 'from-legacy' }),
        },
      );
      const cfg = loadManagedConfig() as ProjectConfig;
      assert.equal(cfg.model, 'from-per-user');
    });
  });

  it('falls through to the device-level source when per-user is absent', () => {
    withPlatform('darwin', () => {
      seedFs(
        new Set([MAC_DEVICE, MAC_LEGACY]),
        {
          [MAC_DEVICE]: JSON.stringify({ model: 'from-device' }),
          [MAC_LEGACY]: JSON.stringify({ model: 'from-legacy' }),
        },
      );
      const cfg = loadManagedConfig() as ProjectConfig;
      // RED without the multi-path change: old loadManagedConfig only read the
      // Application Support path, which exists here too — so this case alone does
      // not discriminate. The per-user-only case below is the discriminating one.
      assert.equal(cfg.model, 'from-device');
    });
  });

  it('reads the per-user source even when the legacy default is ABSENT (discriminates multi-path)', () => {
    withPlatform('darwin', () => {
      const user = macUserCandidate();
      // Only the per-user candidate exists; the legacy Application Support default
      // does NOT. Pre-change code read only the legacy path → null; the multi-path
      // resolver finds the per-user source.
      seedFs(new Set([user]), { [user]: JSON.stringify({ model: 'per-user-only' }) });
      const cfg = loadManagedConfig() as ProjectConfig;
      assert.equal(cfg.model, 'per-user-only');
    });
  });

  it('returns null when no candidate exists', () => {
    withPlatform('darwin', () => {
      seedFs(new Set(), {});
      assert.equal(loadManagedConfig(), null);
    });
  });

  it('honors the env override as the sole candidate (Linux)', () => {
    process.env.SUPERINFERENCE_MANAGED_CONFIG = '/custom/managed.json';
    withPlatform('linux', () => {
      seedFs(new Set(['/custom/managed.json']), {
        '/custom/managed.json': JSON.stringify({ model: 'overridden' }),
      });
      const cfg = loadManagedConfig() as ProjectConfig;
      assert.equal(cfg.model, 'overridden');
    });
  });
});
