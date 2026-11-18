// U010 / w108 — managed-config resilience + project reload introspection.
//
// Covers three additive parity capabilities added to config.ts:
//   - loadManagedConfig empty-source FALL-THROUGH: a present-but-empty higher-priority
//     managed source does NOT shadow a populated lower-priority one (parity: Claude's
//     consumeRawReadResult first-non-empty-source-wins chain).
//   - managed `config.d/*.json` drop-in PER-FILE fault isolation: one malformed/unreadable
//     drop-in is skipped (logged) while the base file + remaining drop-ins still apply
//     (parity: Claude's managed-settings.d per-file try/catch skip). The BASE file still
//     fails closed.
//   - ConfigService.reload() introspection breadcrumb: a project-tier change emits a
//     "changed; reapplying (source: project)" debug line, matching the global/managed
//     tiers' change logs (parity: Claude's logForDebugging settings-change line).

import { describe, it, afterEach, beforeEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  loadManagedConfig,
  ConfigService,
  fsHooks,
  type ProjectConfig,
} from '../src/config';
import { execHooks } from '../src/managed-policy-source';

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

const macUserCandidate = (): string =>
  `/Library/Managed Preferences/${os.userInfo().username}/SuperInference/config.json`;
const MAC_DEVICE = '/Library/Managed Preferences/SuperInference/config.json';
const MAC_LEGACY = '/Library/Application Support/SuperInference/config.json';

/**
 * Seed the config fsHooks seam so only `present` paths exist and read back
 * `contentByPath[path]`. `dirEntries` maps a `config.d` dir path to its listing.
 * Native OS policy is disabled (execHooks.existsSync → false) so these tests
 * exercise only the file-based managed layer.
 */
function seedFs(
  present: Set<string>,
  contentByPath: Record<string, string>,
  dirEntries: Record<string, string[]> = {},
): void {
  mock.method(execHooks, 'existsSync', (() => false) as typeof fs.existsSync);
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
  mock.method(fsHooks, 'readdirSync', ((p: fs.PathLike) => {
    const key = String(p);
    if (key in dirEntries) return dirEntries[key] as unknown as fs.Dirent[];
    throw Object.assign(new Error('ENOTDIR'), { code: 'ENOTDIR' });
  }) as typeof fs.readdirSync);
}

describe('U010/w108 — loadManagedConfig empty-source fall-through', () => {
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

  it('falls through a present-but-EMPTY higher-priority source to a populated lower one', () => {
    withPlatform('darwin', () => {
      const user = macUserCandidate();
      // Per-user source is present but empty `{}`; device-level has real policy.
      // RED (first-PRESENT selection): picked the empty per-user source → model undefined.
      // GREEN (first-NON-EMPTY selection): falls through to the populated device source.
      seedFs(
        new Set([user, MAC_DEVICE]),
        {
          [user]: JSON.stringify({}),
          [MAC_DEVICE]: JSON.stringify({ model: 'from-device' }),
        },
      );
      const cfg = loadManagedConfig() as ProjectConfig;
      assert.equal(cfg.model, 'from-device');
    });
  });

  it('keeps a populated higher-priority source (no spurious fall-through)', () => {
    withPlatform('darwin', () => {
      const user = macUserCandidate();
      seedFs(
        new Set([user, MAC_DEVICE]),
        {
          [user]: JSON.stringify({ model: 'from-per-user' }),
          [MAC_DEVICE]: JSON.stringify({ model: 'from-device' }),
        },
      );
      assert.equal((loadManagedConfig() as ProjectConfig).model, 'from-per-user');
    });
  });

  it('returns an empty base when every present source is empty (drop-ins still layer on it)', () => {
    withPlatform('darwin', () => {
      const user = macUserCandidate();
      // Only empty sources exist; the highest-priority present one is the base, and
      // ITS config.d drop-in still applies on top.
      seedFs(
        new Set([user, MAC_DEVICE, `/Library/Managed Preferences/${os.userInfo().username}/SuperInference/config.d`]),
        {
          [user]: JSON.stringify({}),
          [MAC_DEVICE]: JSON.stringify({}),
          [`/Library/Managed Preferences/${os.userInfo().username}/SuperInference/config.d/10-team.json`]: JSON.stringify({ model: 'from-dropin' }),
        },
        {
          [`/Library/Managed Preferences/${os.userInfo().username}/SuperInference/config.d`]: ['10-team.json'],
        },
      );
      assert.equal((loadManagedConfig() as ProjectConfig).model, 'from-dropin');
    });
  });

  it('returns null when no managed source exists', () => {
    withPlatform('darwin', () => {
      seedFs(new Set(), {});
      assert.equal(loadManagedConfig(), null);
    });
  });
});

describe('U010/w108 — managed config.d drop-in per-file fault isolation', () => {
  let prevEnv: string | undefined;
  beforeEach(() => {
    prevEnv = process.env.SUPERINFERENCE_MANAGED_CONFIG;
    process.env.SUPERINFERENCE_MANAGED_CONFIG = '/etc/fixture/config.json';
  });
  afterEach(() => {
    mock.restoreAll();
    if (prevEnv === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = prevEnv;
  });

  it('skips a malformed drop-in while keeping the base + the well-formed drop-ins', () => {
    const base = '/etc/fixture/config.json';
    const dropDir = '/etc/fixture/config.d';
    seedFs(
      new Set([base, dropDir]),
      {
        [base]: JSON.stringify({ model: 'base-model', maxTurns: 10 }),
        [`${dropDir}/10-ok.json`]: JSON.stringify({ maxTurns: 20 }),
        [`${dropDir}/20-broken.json`]: '{ this is not json',
        [`${dropDir}/30-ok.json`]: JSON.stringify({ permissionMode: 'auto-allow' }),
      },
      { [dropDir]: ['10-ok.json', '20-broken.json', '30-ok.json'] },
    );
    // RED (parseManagedFile throwing out of the loop): the whole load throws on the
    // malformed drop-in. GREEN (per-file skip): the broken file is skipped and the
    // valid base + remaining drop-ins merge (last well-formed wins per field).
    const cfg = loadManagedConfig() as ProjectConfig;
    assert.equal(cfg.model, 'base-model');
    assert.equal(cfg.maxTurns, 20);
    assert.equal(cfg.permissionMode, 'auto-allow');
  });

  it('still FAILS CLOSED on a malformed BASE managed file (base is the authoritative policy)', () => {
    const base = '/etc/fixture/config.json';
    seedFs(new Set([base]), { [base]: '{ broken base' });
    assert.throws(() => loadManagedConfig(), /failed to parse/);
  });
});

describe('U010/w108 — ConfigService.reload() project-change introspection breadcrumb', () => {
  let dir: string;
  let prevStderr: string | undefined;
  let prevLevel: string | undefined;
  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-u010-w108-'));
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.ami', 'config.json'), JSON.stringify({ model: 'a' }));
    prevStderr = process.env.AMI_DEBUG_TO_STDERR;
    prevLevel = process.env.AMI_DEBUG_LOG_LEVEL;
    // Route coreLog to stderr (bypassing the NODE_ENV=test disk gate) at debug level so
    // the breadcrumb is observable via a mockable process.stderr.write.
    process.env.AMI_DEBUG_TO_STDERR = '1';
    process.env.AMI_DEBUG_LOG_LEVEL = 'debug';
  });
  afterEach(() => {
    mock.restoreAll();
    fs.rmSync(dir, { recursive: true, force: true });
    if (prevStderr === undefined) delete process.env.AMI_DEBUG_TO_STDERR; else process.env.AMI_DEBUG_TO_STDERR = prevStderr;
    if (prevLevel === undefined) delete process.env.AMI_DEBUG_LOG_LEVEL; else process.env.AMI_DEBUG_LOG_LEVEL = prevLevel;
  });

  it('emits a "reapplying (source: project)" breadcrumb when the project config changes', () => {
    const svc = new ConfigService(dir);
    const lines: string[] = [];
    mock.method(process.stderr, 'write', ((chunk: string | Uint8Array) => { lines.push(String(chunk)); return true; }) as typeof process.stderr.write);
    // Mutate the file and reload: the content differs, so the change branch runs.
    fs.writeFileSync(path.join(dir, '.ami', 'config.json'), JSON.stringify({ model: 'b' }));
    const changed = svc.reload();
    assert.equal(changed, true);
    const breadcrumb = lines.find(l => l.includes('changed; reapplying (source: project)'));
    assert.ok(breadcrumb, `expected a project-change breadcrumb; got:\n${lines.join('')}`);
  });

  it('does NOT emit the breadcrumb when the reload is a no-op (content unchanged)', () => {
    const svc = new ConfigService(dir);
    const lines: string[] = [];
    mock.method(process.stderr, 'write', ((chunk: string | Uint8Array) => { lines.push(String(chunk)); return true; }) as typeof process.stderr.write);
    // Rewrite identical content: no logical change → no breadcrumb, reload() → false.
    fs.writeFileSync(path.join(dir, '.ami', 'config.json'), JSON.stringify({ model: 'a' }));
    assert.equal(svc.reload(), false);
    assert.equal(lines.some(l => l.includes('reapplying (source: project)')), false);
  });
});
