/**
 * U010 — managed-config OVERRIDE trust boundary.
 *
 * Parity: Claude Code's managed/enterprise config is sourced from a FIXED admin-only OS
 * location the end user cannot relocate (mdm/settings.ts). AMI additionally honors a
 * user-settable SUPERINFERENCE_MANAGED_CONFIG override for testing / non-standard
 * deployments — a relocation hole: an unprivileged user could point the authoritative
 * managed-policy source at a file a DIFFERENT unprivileged principal controls and have the
 * victim/root session honor injected policy.
 *
 * This hardening refuses a RELOCATED (override) managed file owned by NEITHER root (uid 0)
 * NOR the current euid, unless the explicit MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV opt-out seam
 * is engaged. The existing base checks (symlink, world-writable) are unchanged, and a
 * self-owned group-writable (umask-002, 0o664) override is still accepted — matching the
 * pre-existing deliberate group-write tolerance.
 *
 * RED (pre-fix): a foreign-owned override loaded silently. GREEN: it fails closed.
 */
import { describe, it, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  loadManagedConfig,
  collectConfigDiagnostics,
  ManagedConfigError,
  MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV,
  fsHooks,
} from '../src/config';

function mkTemp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-u010-ovr-'));
}

/** A minimal fs.Stats-like object for lstatSync mocking. */
function fakeStats(opts: { symlink?: boolean; mode?: number; uid?: number }): fs.Stats {
  return {
    isSymbolicLink: () => opts.symlink === true,
    mode: opts.mode ?? 0o644,
    uid: opts.uid ?? 0,
  } as unknown as fs.Stats;
}

describe('U010 — managed-config override trust boundary', () => {
  const savedOverride = process.env.SUPERINFERENCE_MANAGED_CONFIG;
  const savedSeam = process.env[MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV];

  afterEach(() => {
    if (savedOverride === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = savedOverride;
    if (savedSeam === undefined) delete process.env[MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV];
    else process.env[MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV] = savedSeam;
    mock.restoreAll();
  });

  it('exports the opt-out seam env-var name', () => {
    assert.equal(MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV, 'SUPERINFERENCE_MANAGED_CONFIG_ALLOW_UNTRUSTED');
  });

  it('accepts a self-owned override file (regression: legitimate relocation still works)', () => {
    const p = path.join(mkTemp(), 'managed.json');
    fs.writeFileSync(p, JSON.stringify({ model: 'mine' }), 'utf-8');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    assert.equal(loadManagedConfig()!.model, 'mine');
  });

  it('accepts a self-owned GROUP-WRITABLE (0o664, umask-002) override (regression)', () => {
    const p = path.join(mkTemp(), 'managed.json');
    fs.writeFileSync(p, JSON.stringify({ model: 'grp' }), 'utf-8');
    fs.chmodSync(p, 0o664);
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    assert.equal(loadManagedConfig()!.model, 'grp');
  });

  it('FAILS CLOSED on an override owned by a foreign (non-root, non-self) uid', () => {
    const p = path.join(mkTemp(), 'managed.json');
    fs.writeFileSync(p, JSON.stringify({ model: 'injected' }), 'utf-8');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    // A file owned by neither root nor the current user is only reproducible without
    // root privileges by mocking the stat seam. mode 0o644, uid = a foreign principal.
    mock.method(fsHooks, 'lstatSync', () => fakeStats({ mode: 0o644, uid: 999999 }));
    // RED (pre-fix): parseManagedFile trusted any non-world-writable override → 'injected'.
    assert.throws(() => loadManagedConfig(), (err: unknown) => {
      assert.ok(err instanceof ManagedConfigError);
      assert.match((err as Error).message, /owned by uid 999999/);
      assert.match((err as Error).message, /neither root nor the current user/);
      return true;
    });
  });

  it('surfaces the foreign-owner refusal as a /doctor diagnostic', () => {
    const cwd = mkTemp();
    const p = path.join(mkTemp(), 'managed.json');
    fs.writeFileSync(p, JSON.stringify({ model: 'injected' }), 'utf-8');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    mock.method(fsHooks, 'lstatSync', () => fakeStats({ mode: 0o644, uid: 999999 }));
    const diags = collectConfigDiagnostics(cwd);
    const d = diags.find(x => x.path === p);
    assert.ok(d, 'expected a managed-policy diagnostic');
    assert.equal(d!.kind, 'unreadable');
    assert.match(d!.message, /owned by uid 999999/);
  });

  it('opt-out seam (=1) relaxes the foreign-owner refusal', () => {
    const p = path.join(mkTemp(), 'managed.json');
    fs.writeFileSync(p, JSON.stringify({ model: 'allowed' }), 'utf-8');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    process.env[MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV] = '1';
    mock.method(fsHooks, 'lstatSync', () => fakeStats({ mode: 0o644, uid: 999999 }));
    assert.equal(loadManagedConfig()!.model, 'allowed');
  });

  it('opt-out seam (=true) also relaxes the refusal', () => {
    const p = path.join(mkTemp(), 'managed.json');
    fs.writeFileSync(p, JSON.stringify({ model: 'allowed2' }), 'utf-8');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    process.env[MANAGED_CONFIG_ALLOW_UNTRUSTED_ENV] = 'true';
    mock.method(fsHooks, 'lstatSync', () => fakeStats({ mode: 0o644, uid: 999999 }));
    assert.equal(loadManagedConfig()!.model, 'allowed2');
  });

  it('still FAILS CLOSED on a world-writable override before the ownership check', () => {
    const p = path.join(mkTemp(), 'managed.json');
    fs.writeFileSync(p, JSON.stringify({ model: 'm' }), 'utf-8');
    fs.chmodSync(p, 0o666);
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    assert.throws(() => loadManagedConfig(), (err: unknown) => {
      assert.ok(err instanceof ManagedConfigError);
      assert.match((err as Error).message, /world-writable/);
      return true;
    });
  });

  it('also refuses a foreign-owned RELOCATED drop-in file', () => {
    const dir = mkTemp();
    const base = path.join(dir, 'config.json');
    fs.writeFileSync(base, JSON.stringify({ model: 'base' }), 'utf-8');
    const dropInDir = path.join(dir, 'config.d');
    fs.mkdirSync(dropInDir);
    const drop = path.join(dropInDir, '10-injected.json');
    fs.writeFileSync(drop, JSON.stringify({ model: 'injected' }), 'utf-8');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = base;
    // Only the drop-in reports a foreign owner; the base stays self-owned.
    const realLstat = fs.lstatSync;
    mock.method(fsHooks, 'lstatSync', (fp: string) =>
      fp === drop ? fakeStats({ mode: 0o644, uid: 999999 }) : realLstat(fp),
    );
    assert.throws(() => loadManagedConfig(), ManagedConfigError);
  });

  it('on a platform without process.getuid (Windows), skips the ownership check', () => {
    const p = path.join(mkTemp(), 'managed.json');
    fs.writeFileSync(p, JSON.stringify({ model: 'win' }), 'utf-8');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    // Report a foreign owner, but with no getuid the POSIX checks are skipped entirely.
    mock.method(fsHooks, 'lstatSync', () => fakeStats({ mode: 0o644, uid: 999999 }));
    const origGetuid = process.getuid;
    (process as { getuid?: unknown }).getuid = undefined;
    try {
      assert.equal(loadManagedConfig()!.model, 'win');
    } finally {
      (process as { getuid?: unknown }).getuid = origGetuid;
    }
  });

  it('the DEVICE-DEFAULT (non-override) managed path is NOT subject to the ownership check', () => {
    // No override → relocatable=false. Simulate an existing, foreign-owned device-default
    // managed file and confirm the strict ownership refusal does NOT fire for it.
    delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    const realExists = fs.existsSync;
    const realRead = fs.readFileSync;
    mock.method(fsHooks, 'existsSync', (fp: string) =>
      fp.endsWith('config.json') && fp.includes('superinference') ? true : realExists(fp),
    );
    mock.method(fsHooks, 'lstatSync', () => fakeStats({ mode: 0o644, uid: 999999 }));
    mock.method(fsHooks, 'readFileSync', (fp: string, enc?: unknown) =>
      fp.endsWith('config.json') && fp.includes('superinference')
        ? JSON.stringify({ model: 'device' })
        : (realRead as (p: string, e?: unknown) => string)(fp, enc),
    );
    // readdirSync for the config.d dir may be invoked; keep it empty/harmless.
    mock.method(fsHooks, 'readdirSync', () => [] as unknown as fs.Dirent[]);
    const cfg = loadManagedConfig();
    assert.equal(cfg?.model, 'device', 'a foreign-owned device-default path is trusted (not relocatable)');
  });
});
